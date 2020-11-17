import os
import random
from asyncio import Future, ensure_future, get_event_loop, sleep
from base64 import b64decode
from binascii import hexlify, unhexlify
from functools import wraps
from math import ceil
from typing import Dict, List, Optional, Tuple

from ipv8.community import Community, lazy_wrapper
from ipv8.dht import DHTError
from ipv8.messaging.payload import IntroductionRequestPayload, IntroductionResponsePayload
from ipv8.messaging.payload_headers import BinMemberAuthenticationPayload
from ipv8.messaging.payload_headers import GlobalTimeDistributionPayload
from ipv8.peer import Peer
from ipv8.requestcache import RequestCache
from ipv8.util import succeed

from anydex.core import DeclineMatchReason, DeclinedTradeReason, MAX_ORDER_TIMEOUT, CONVERSION_RATES
from anydex.core.assetamount import AssetAmount
from anydex.core.assetpair import AssetPair
from anydex.core.block import MarketBlock
from anydex.core.bloomfilter import BloomFilter
from anydex.core.cache import MatchCache, OrderStatusRequestCache, PingRequestCache, ProposedTradeRequestCache,\
    PublicKeyRequestCache
from anydex.core.database import MarketDB
from anydex.core.defs import *
from anydex.core.matching_engine import MatchingEngine, PriceTimeStrategy
from anydex.core.message import TraderId
from anydex.core.order import Order, OrderId, OrderNumber
from anydex.core.order_manager import OrderManager
from anydex.core.order_repository import DatabaseOrderRepository, MemoryOrderRepository
from anydex.core.orderbook import DatabaseOrderBook, OrderBook
from anydex.core.payload import DeclineMatchPayload, DeclineTradePayload, InfoPayload, MatchPayload,\
    OrderStatusRequestPayload, OrderStatusResponsePayload, OrderbookSyncPayload, PingPongPayload, PublicKeyPayload,\
    TradePayload
from anydex.core.payment import Payment
from anydex.core.payment_id import PaymentId
from anydex.core.price import Price
from anydex.core.settings import MarketSettings
from anydex.core.tick import Ask, Bid, Tick
from anydex.core.tickentry import TickEntry
from anydex.core.timeout import Timeout
from anydex.core.timestamp import Timestamp
from anydex.core.trade import AcceptedTrade, CounterTrade, DeclinedTrade, ProposedTrade, Trade
from anydex.core.transaction import Transaction, TransactionId
from anydex.core.transaction_manager import TransactionManager
from anydex.core.transaction_repository import DatabaseTransactionRepository,\
    MemoryTransactionRepository
from anydex.core.wallet_address import WalletAddress
from anydex.trustchain.listener import BlockListener
from anydex.trustchain.payload import HalfBlockBroadcastPayload, HalfBlockPairBroadcastPayload, HalfBlockPairPayload
from anydex.wallet.tc_wallet import TrustchainWallet


def synchronized(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        with self.trustchain.receive_block_lock:
            return f(self, *args, **kwargs)
    return wrapper


class MarketCommunity(Community, BlockListener):
    """
    Community for general asset trading.
    """
    community_id = unhexlify('98c1f6342f30528ada9647197f0503d48db9c2fb')
    PROTOCOL_VERSION = 4
    BLOCK_CLASS = MarketBlock
    DB_NAME = 'market'

    def __init__(self, *args, **kwargs):
        self.is_matchmaker = kwargs.pop('is_matchmaker', True)
        self.wallets = kwargs.pop('wallets', {})
        self.trustchain = kwargs.pop('trustchain', None)
        if self.trustchain:
            self.initialize_trustchain()
        self.record_transactions = kwargs.pop('record_transactions', False)
        self.dht = kwargs.pop('dht', None)
        self.use_database = kwargs.pop('use_database', True)
        self.settings = kwargs.pop('settings', MarketSettings())
        self.fixed_broadcast_set = []  # Optional list of fixed peers that will receive market messages

        db_working_dir = kwargs.pop('working_directory', '')

        Community.__init__(self, *args, **kwargs)
        BlockListener.__init__(self)

        self._use_main_thread = True  # Market community is unable to deal with thread pool message processing yet
        self.mid = self.my_peer.mid
        self.mid_register = {}
        self.pk_register = {}
        self.order_book = None
        self.market_database = MarketDB(db_working_dir, self.DB_NAME)
        self.matching_engine = None
        self.transaction_manager = None
        self.use_local_address = False
        self.matching_enabled = True
        self.use_incremental_payments = False
        self.matchmakers = set()
        self.request_cache = RequestCache()
        self.cancelled_orders = set()  # Keep track of cancelled orders so we don't add them again to the orderbook.
        self.sent_matches = set()

        if self.use_database:
            order_repository = DatabaseOrderRepository(self.mid, self.market_database)
            transaction_repository = DatabaseTransactionRepository(self.mid, self.market_database)
        else:
            order_repository = MemoryOrderRepository(self.mid)
            transaction_repository = MemoryTransactionRepository(self.mid)

        self.order_manager = OrderManager(order_repository)
        self.transaction_manager = TransactionManager(transaction_repository)

        if self.is_matchmaker:
            self.enable_matchmaker()

        # Register messages
        self.decode_map[MSG_MATCH] = self.received_match
        self.decode_map[MSG_MATCH_DECLINE] = self.received_decline_match
        self.decode_map[MSG_PROPOSED_TRADE] = self.received_proposed_trade
        self.decode_map[MSG_DECLINED_TRADE] = self.received_decline_trade
        self.decode_map[MSG_COUNTER_TRADE] = self.received_counter_trade
        self.decode_map[MSG_ACCEPT_TRADE] = self.received_accept_trade
        self.decode_map[MSG_ORDER_QUERY] = self.received_order_status_request
        self.decode_map[MSG_ORDER_RESPONSE] = self.received_order_status
        self.decode_map[MSG_BOOK_SYNC] = self.received_orderbook_sync
        self.decode_map[MSG_PING] = self.received_ping
        self.decode_map[MSG_PONG] = self.received_pong
        self.decode_map[MSG_MATCH_DONE] = self.received_matched_tx_complete
        self.decode_map[MSG_PK_QUERY] = self.received_trader_pk_request
        self.decode_map[MSG_PK_RESPONSE] = self.received_trader_pk_response

        self.logger.info("Market community initialized with mid %s", hexlify(self.mid).decode())

    def initialize_trustchain(self) -> None:
        market_block_types = [b'ask', b'bid', b'cancel_order', b'tx_init', b'tx_payment', b'tx_done']
        self.trustchain.settings.block_types_bc_disabled |= set(market_block_types)
        self.trustchain.add_listener(self, market_block_types)

    async def get_address_for_trader(self, trader_id: TraderId) -> Optional[Tuple]:
        """
        Fetch the address for a trader.
        If not available in the local storage, perform a DHT request to fetch the address of the peer with a
        specified trader ID.
        """
        if bytes(trader_id) == self.mid:
            return self.get_ipv8_address()
        address = self.lookup_ip(trader_id)
        if address:
            return address

        self.logger.info("Address for trader %s not found locally, doing DHT request", trader_id.as_hex())

        if not self.dht:
            raise RuntimeError("DHT not available")

        try:
            peers = await self.dht.connect_peer(bytes(trader_id))
        except DHTError:
            self._logger.warning("Unable to get address for trader %s", trader_id.as_hex())
            return

        if peers:
            self.update_ip(trader_id, peers[0].address)
            return peers[0].address

    def get_peer_from_mid(self, peer_mid: bytes) -> Optional[Peer]:
        """
        Find a peer by mid.
        """
        peers = self.network.verified_peers
        matches = [p for p in peers if p.mid == peer_mid]
        return matches[0] if matches else None

    async def should_sign(self, block: MarketBlock) -> bool:
        """
        Check whether we should sign the incoming block.
        """
        tx = block.transaction
        if block.type == b"tx_payment":
            txid = TransactionId(unhexlify(tx["payment"]["transaction_id"]))
            transaction = self.transaction_manager.find_by_id(txid)
            if not transaction or not block.is_valid_tx_payment_block():
                return False

            if not transaction.trading_peer:
                transaction.trading_peer = self.get_peer_from_mid(bytes(transaction.partner_order_id.trader_id))

            # Start polling for the payment
            asset_id = tx["payment"]["transferred"]["type"]
            if asset_id not in self.wallets or not self.wallets[asset_id].created:
                self.logger.warning("Wallet for asset %s not found - not signing payment message", asset_id)
                return False

            wallet = self.wallets[asset_id]
            payment = Payment.from_block(block)
            await wallet.monitor_transaction(payment.payment_id.payment_id)
            transaction.add_payment(payment)
            self.transaction_manager.transaction_repository.update(transaction)

            order = self.order_manager.order_repository.find_by_id(transaction.order_id)
            order.add_trade(transaction.partner_order_id, payment.transferred_assets)
            self.order_manager.order_repository.update(order)

            return True

        elif block.type == b"tx_init":
            return block.is_valid_tx_init_done_block()
        elif block.type == b"tx_done":
            txid = TransactionId(unhexlify(tx["tx"]["transaction_id"]))
            transaction = self.transaction_manager.find_by_id(txid)
            return transaction and block.is_valid_tx_init_done_block()

        return False  # Unknown block type

    def on_counter_signed_block(self, block: MarketBlock) -> None:
        if block.type == b"tx_payment":
            # Send the next payment, if we are not done yet
            txid = TransactionId(unhexlify(block.transaction["payment"]["transaction_id"]))
            transaction = self.transaction_manager.find_by_id(txid)
            if not transaction or not block.is_valid_tx_payment_block():
                return

            if not transaction.is_payment_complete():
                transaction.trading_peer = Peer(block.public_key,
                                                address=self.lookup_ip(transaction.partner_order_id.trader_id))
                #self.register_anonymous_task('send_payment_%s' % id(transaction), self.send_payment, transaction)
                with open(os.path.join(self.data_dir, "fraud.txt"), "a") as fraud_file:
                    stolen_assets = block.transaction["payment"]["transferred"]
                    counterparty_id = transaction.partner_order_id.trader_id.as_hex()[-8:]
                    self.logger.error("COMMIT FRAUD with party %s: %s" % (counterparty_id, stolen_assets))
                    loop = get_event_loop()
                    fraud_file.write("%s,%s,%f,%d,%s\n" % (self.peer_id, counterparty_id, loop.time(), stolen_assets["amount"], stolen_assets["type"]))

        if block.type == b"tx_init":
            # Create a transaction, based on the information in the block
            if not self.transaction_manager.find_by_id(TransactionId(block.hash)):
                tx = block.transaction
                order_id = OrderId(TraderId(unhexlify(tx["tx"]["partner_trader_id"])),
                                   OrderNumber(tx["tx"]["partner_order_number"]))
                order = self.order_manager.order_repository.find_by_id(order_id)
                if not order:
                    return
                incoming_address, outgoing_address = self.get_order_addresses(order)
                transaction = Transaction.from_tx_init_block(block)
                transaction.incoming_address = incoming_address
                transaction.outgoing_address = outgoing_address
                transaction.partner_incoming_address = WalletAddress(block.transaction["wallets"]["incoming"])
                transaction.partner_outgoing_address = WalletAddress(block.transaction["wallets"]["outgoing"])

                transaction.trading_peer = Peer(block.public_key,
                                                address=self.lookup_ip(transaction.partner_order_id.trader_id))
                self.transaction_manager.transaction_repository.add(transaction)

    def get_counter_tx(self, block: MarketBlock) -> Dict:
        """
        Return the counter tx, with information on the number of ongoing trades.
        """
        tx = block.transaction.copy()

        entrusted_assets = self.get_entrusted_assets()
        if block.type == b"tx_init":
            # Replace wallet addresses
            order_id = OrderId(TraderId(unhexlify(tx["tx"]["partner_trader_id"])),
                               OrderNumber(tx["tx"]["partner_order_number"]))
            order = self.order_manager.order_repository.find_by_id(order_id)
            incoming_address, outgoing_address = self.get_order_addresses(order)
            tx["wallets"]["incoming"] = incoming_address.address
            tx["wallets"]["outgoing"] = outgoing_address.address

            # We have to manually add the assets of the upcoming trade since the transaction will only be created
            # after counter-signing the incoming tx_init block.
            incoming_assets = tx["tx"]["assets"]["second"] if order.is_ask() else tx["tx"]["assets"]["first"]
            incoming_assets = AssetAmount(ceil(incoming_assets["amount"] / tx["tx"]["payments"]), incoming_assets["type"])
            if incoming_assets.asset_id not in entrusted_assets:
                entrusted_assets[incoming_assets.asset_id] = 0
            entrusted_assets[incoming_assets.asset_id] += incoming_assets.amount

        tx["entrusted"] = entrusted_assets

        return tx

    def get_entrusted_assets(self) -> Dict:
        """
        Return a dictionary with the assets that this user is currently entrusted with, in USD.
        """
        entrusted_assets = {}
        for transaction in self.transaction_manager.find_all():
            if transaction.is_risky:
                # Find out which assets we will receive
                tx_order = self.order_manager.order_repository.find_by_id(transaction.order_id)
                incoming_assets = transaction.assets.second if tx_order.is_ask() else transaction.assets.first
                incoming_assets = AssetAmount(ceil(incoming_assets.amount / transaction.num_payments), incoming_assets.asset_id)
                if incoming_assets.asset_id not in entrusted_assets:
                    entrusted_assets[incoming_assets.asset_id] = 0
                entrusted_assets[incoming_assets.asset_id] += incoming_assets.amount

        return entrusted_assets

    def enable_matchmaker(self) -> None:
        """
        Enable this node to be a matchmaker
        """
        if self.use_database:
            self.order_book = DatabaseOrderBook(self.market_database)
            self.order_book.restore_from_database()
        else:
            self.order_book = OrderBook()
        self.matching_engine = MatchingEngine(PriceTimeStrategy(self.order_book))
        self.is_matchmaker = True

    def disable_matchmaker(self) -> None:
        """
        Disable the matchmaker status of this node
        """
        self.order_book = None
        self.matching_engine = None
        self.is_matchmaker = False

    def create_introduction_request(self, socket_address: Tuple, extra_bytes: bytes = b'', new_style: bool = False):
        extra_payload = InfoPayload(TraderId(self.mid), Timestamp.now(), self.is_matchmaker)
        extra_bytes = self.serializer.pack_serializable(extra_payload)
        return super(MarketCommunity, self).create_introduction_request(socket_address, extra_bytes)

    def create_introduction_response(self, lan_socket_address: Tuple, socket_address: Tuple, identifier: bytes,
                                     introduction=None, extra_bytes: bytes = b'', new_style: bool = False):
        extra_payload = InfoPayload(TraderId(self.mid), Timestamp.now(), self.is_matchmaker)
        extra_bytes = self.serializer.pack_serializable(extra_payload)
        return super(MarketCommunity, self).create_introduction_response(lan_socket_address, socket_address,
                                                                         identifier, introduction, extra_bytes)

    def parse_extra_bytes(self, extra_bytes: bytes, peer: Peer) -> None:
        if not extra_bytes:
            return

        payload = self.serializer.unpack_serializable(InfoPayload, extra_bytes)[0]
        self.update_ip(payload.trader_id, peer.address)

        if payload.is_matchmaker:
            self.add_matchmaker(peer)

    def introduction_request_callback(self, peer: Peer, _, payload: IntroductionRequestPayload) -> None:
        if self.is_matchmaker and peer.address not in self.network.blacklist:
            self.send_orderbook_sync(peer)
        self.parse_extra_bytes(payload.extra_bytes, peer)

    def introduction_response_callback(self, peer, dist, payload: IntroductionResponsePayload) -> None:
        if self.is_matchmaker and peer.address not in self.network.blacklist:
            self.send_orderbook_sync(peer)
        self.parse_extra_bytes(payload.extra_bytes, peer)

    def send_orderbook_sync(self, peer: Peer) -> None:
        """
        Send an orderbook sync message to a specific peer.
        """
        self.logger.debug("Sending orderbook sync to peer %s", peer)
        bloomfilter = self.get_orders_bloomfilter()
        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = OrderbookSyncPayload(TraderId(self.mid), Timestamp.now(), bloomfilter)

        packet = self._ez_pack(self._prefix, MSG_BOOK_SYNC, [auth, payload])
        self.endpoint.send(peer.address, packet)

    def get_orders_bloomfilter(self) -> BloomFilter:
        order_ids = [bytes(order_id) for order_id in self.order_book.get_order_ids()]
        orders_bloom_filter = BloomFilter(0.005, max(len(order_ids), 1), prefix=b' ')
        if order_ids:
            orders_bloom_filter.add_keys(order_ids)
        return orders_bloom_filter

    async def unload(self) -> None:
        # Clear match caches
        for match_cache in self.get_match_caches():
            if match_cache.schedule_task:
                match_cache.schedule_task.cancel()
            if match_cache.schedule_propose:
                match_cache.schedule_propose.cancel()

        self.request_cache.clear()

        # Save the ticks to the database
        if self.is_matchmaker:
            if self.use_database:
                self.order_book.save_to_database()
            await self.order_book.shutdown_task_manager()
        self.market_database.close()
        await super(MarketCommunity, self).unload()

    def get_ipv8_address(self) -> Tuple:
        """
        Returns the address of the IPV8 instance. This method is here to make the experiments on the DAS5 succeed;
        direct messaging is not possible there with a wan address so we are using the local address instead.
        """
        return self.my_estimated_lan if self.use_local_address else self.my_estimated_wan

    def get_order_addresses(self, order: Order) -> Tuple[WalletAddress, WalletAddress]:
        """
        Return a tuple of incoming and outgoing payment address of an order.
        """
        if order.is_ask():
            return (WalletAddress(self.wallets[order.assets.second.asset_id].get_address()),
                    WalletAddress(self.wallets[order.assets.first.asset_id].get_address()))
        else:
            return (WalletAddress(self.wallets[order.assets.first.asset_id].get_address()),
                    WalletAddress(self.wallets[order.assets.second.asset_id].get_address()))

    def match_order_ids(self, order_ids: List[OrderId]) -> None:
        """
        Attempt to match the ticks with the provided order ids
        :param order_ids: The order ids to match
        """
        for order_id in order_ids:
            if self.order_book.tick_exists(order_id):
                self.match(self.order_book.get_tick(order_id))

    def match(self, tick: Tick) -> int:
        """
        Try to find a match for a specific tick and send proposed trade messages if there is a match
        :param tick: The tick to find matches for
        :return The number of matches found
        """
        if not self.matching_enabled:
            return 0

        order_tick_entry = self.order_book.get_tick(tick.order_id)
        if tick.assets.first.amount - tick.traded <= 0:
            self.logger.debug("Tick %s does not have any quantity to match!", tick.order_id)
            return 0

        matched_ticks = self.matching_engine.match(order_tick_entry)
        self.send_match_messages(matched_ticks, tick.order_id)
        return len(matched_ticks)

    def lookup_ip(self, trader_id: TraderId) -> Optional[Tuple]:
        """
        Lookup the ip for the public key to send a message to a specific node

        :param trader_id: The public key of the node to send to
        :return: The ip and port tuple: (<ip>, <port>)
        """
        return self.mid_register.get(trader_id)

    def update_ip(self, trader_id: TraderId, ip: Tuple) -> None:
        """
        Update the public key to ip mapping

        :param trader_id: The public key of the node
        :param ip: The ip and port of the node
        """
        self.logger.debug("Updating ip of trader %s to (%s, %s)", trader_id.as_hex(), ip[0], ip[1])
        self.mid_register[trader_id] = ip

    def process_tick_block(self, block: MarketBlock) -> None:
        """
        Process a TradeChain block containing a tick, only if we have a verified order.
        :param block: The TradeChain block containing the tick
        """
        if not block.is_valid_tick_block():
            self._logger.warning("Invalid tick block received!")
            return

        tick = Ask.from_block(block) if block.type == b'ask' else Bid.from_block(block)
        ensure_future(self.on_tick(tick))

    def process_tx_init_block(self, block: MarketBlock) -> None:
        """
        Process a TrustChain block containing a transaction initialisation
        :param block: The TrustChain block containing the transaction initialisation
        """
        if not block.is_valid_tx_init_done_block():
            self._logger.warning("Invalid tx_init block received! %s" % block.transaction)
            return

        if self.is_matchmaker:
            tx_dict = block.transaction
            order_id1 = OrderId(TraderId(unhexlify(tx_dict["tx"]["trader_id"])),
                                OrderNumber(tx_dict["tx"]["order_number"]))
            order_id2 = OrderId(TraderId(unhexlify(tx_dict["tx"]["partner_trader_id"])),
                                OrderNumber(tx_dict["tx"]["partner_order_number"]))
            self.match_order_ids([order_id1, order_id2])

    async def process_tx_payment_block(self, block: MarketBlock) -> None:
        """
        Process a TrustChain block containing a payment.
        :param block: The TrustChain block containing the payment info.
        """
        if block.link_public_key == self.my_peer.public_key.key_to_bin():
            transaction_id = TransactionId(unhexlify(block.transaction["payment"]["transaction_id"]))
            transaction = self.transaction_manager.find_by_id(transaction_id)
            if not transaction:
                self.logger.warning("Could not find transaction %s associated for signed payment block %s",
                                    transaction_id.as_hex(), block)
                return

            if not transaction.trading_peer:
                transaction.trading_peer = self.get_peer_from_mid(bytes(transaction.partner_order_id.trader_id))

            if transaction.is_payment_complete():
                order = self.order_manager.order_repository.find_by_id(transaction.order_id)

                other_order_dict = await self.send_order_status_request(transaction.partner_order_id)
                my_order_dict = order.to_status_dictionary()

                if order.is_ask():
                    ask_order_dict = my_order_dict
                    bid_order_dict = other_order_dict
                else:
                    ask_order_dict = other_order_dict
                    bid_order_dict = my_order_dict

                tx_done_block = await self.create_new_tx_done_block(transaction.trading_peer, ask_order_dict, bid_order_dict, transaction)
                self.send_matched_transaction_completed(transaction, tx_done_block)

    def process_tx_done_block(self, block: MarketBlock) -> None:
        """
        Process a TrustChain block containing a transaction completion
        :param block: The TradeChain block containing the transaction completion
        """
        if not block.is_valid_tx_init_done_block():
            self._logger.warning("Invalid tx_done block received!")
            return

        if block.link_public_key == self.my_peer.public_key.key_to_bin():
            # If we have signed an incoming tx_done block, notify the matchmaker about this
            transaction_id = TransactionId(unhexlify(block.transaction["tx"]["transaction_id"]))
            transaction = self.transaction_manager.find_by_id(transaction_id)
            if transaction:
                self.send_matched_transaction_completed(transaction, block)
        elif self.is_matchmaker:
            tx_dict = block.transaction
            transferred_quantity = tx_dict["tx"]["transferred"]["first"]["amount"]
            self.order_book.update_ticks(tx_dict["ask"], tx_dict["bid"], transferred_quantity)
            ask_order_id = OrderId(TraderId(unhexlify(tx_dict["ask"]["trader_id"])),
                                   OrderNumber(tx_dict["ask"]["order_number"]))
            bid_order_id = OrderId(TraderId(unhexlify(tx_dict["bid"]["trader_id"])),
                                   OrderNumber(tx_dict["bid"]["order_number"]))
            self.match_order_ids([ask_order_id, bid_order_id])

    def process_cancel_order_block(self, block: MarketBlock) -> None:
        """
        Process a TradeChain block containing a order cancellation
        :param block: The TradeChain block containing the order cancellation
        """
        if not block.is_valid_cancel_block():
            self._logger.warning("Invalid cancel block received!")
            return

        order_id = OrderId(TraderId(unhexlify(block.transaction["trader_id"])),
                           OrderNumber(block.transaction["order_number"]))
        if self.is_matchmaker and self.order_book.tick_exists(order_id):
            self.order_book.remove_tick(order_id)
            self.cancelled_orders.add(order_id)

    @lazy_wrapper(OrderbookSyncPayload)
    def received_orderbook_sync(self, peer: Peer, payload: OrderbookSyncPayload) -> None:
        if not self.is_matchmaker:
            return

        ticks = []
        for order_id in self.order_book.get_order_ids():
            if bytes(order_id) not in payload.bloomfilter:
                is_ask = self.order_book.ask_exists(order_id)
                entry = self.order_book.get_ask(order_id) if is_ask else self.order_book.get_bid(order_id)
                ticks.append(entry)

        for entry in random.sample(ticks, min(len(ticks), self.settings.num_order_sync)):
            # Send the block pair associated with this tick
            tick_block = self.trustchain.persistence.get_block_with_hash(entry.tick.block_hash)
            if tick_block:
                self.trustchain.send_block(tick_block, address=peer.address)

    def ping_peer(self, peer: Peer) -> Future:
        """
        Ping a specific peer. Return a deferred that fires with a boolean value whether the peer responded within time.
        """
        future = Future()
        cache = PingRequestCache(self, future)
        self.request_cache.add(cache)
        self.send_ping(peer, cache.number)
        return future

    def send_ping(self, peer: Peer, identifier: int) -> None:
        """
        Send a ping message with an identifier to a specific peer.
        """
        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = PingPongPayload(TraderId(self.mid), Timestamp.now(), identifier)

        packet = self._ez_pack(self._prefix, MSG_PING, [auth, payload])
        self.endpoint.send(peer.address, packet)

    @lazy_wrapper(PingPongPayload)
    def received_ping(self, peer: Peer, payload: PingPongPayload) -> None:
        self.send_pong(peer, payload.identifier)

    def send_pong(self, peer: Peer, identifier: int) -> None:
        """
        Send a pong message with an identifier to a specific peer.
        """
        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = PingPongPayload(TraderId(self.mid), Timestamp.now(), identifier)

        packet = self._ez_pack(self._prefix, MSG_PONG, [auth, payload])
        self.endpoint.send(peer.address, packet)

    @lazy_wrapper(PingPongPayload)
    def received_pong(self, _, payload: PingPongPayload) -> None:
        if not self.request_cache.has("ping", payload.identifier):
            self.logger.warning("ping cache with id %s not found", payload.identifier)
            return

        cache = self.request_cache.pop("ping", payload.identifier)
        cache.request_future.set_result(True)

    def verify_offer_creation(self, assets: AssetPair, timeout: int) -> None:
        """
        Verify whether we are creating a valid order.
        This method raises a RuntimeError if the created order is not valid.
        """
        if assets.first.asset_id == assets.second.asset_id:
            raise RuntimeError("You cannot trade between the same wallet")

        if assets.first.asset_id not in self.wallets or not self.wallets[assets.first.asset_id].created:
            raise RuntimeError("Please create a %s wallet first" % assets.first.asset_id)

        if assets.second.asset_id not in self.wallets or not self.wallets[assets.second.asset_id].created:
            raise RuntimeError("Please create a %s wallet first" % assets.second.asset_id)

        asset1_min_unit = self.wallets[assets.first.asset_id].min_unit()
        if assets.first.amount < asset1_min_unit:
            raise RuntimeError("The assets to trade should be higher than or equal to the min unit of this asset (%s)."
                               % assets.first)

        asset2_min_unit = self.wallets[assets.second.asset_id].min_unit()
        if assets.second.amount < asset2_min_unit:
            raise RuntimeError("The assets to trade should be higher than or equal to the min unit of this asset (%s)."
                               % assets.second)

        if timeout < 0:
            raise RuntimeError("The timeout for this order should be positive")

        if timeout > MAX_ORDER_TIMEOUT:
            raise RuntimeError("The timeout for this order should be less than a day")

    async def create_ask(self, assets: AssetPair, timeout: int) -> Order:
        """
        Create an ask order (sell order)

        :param assets: The assets to exchange
        :param timeout: The timeout of the order, when does the order need to be timed out
        :return: The created order
        """
        self.verify_offer_creation(assets, timeout)

        # Create the order
        order = self.order_manager.create_ask_order(assets, Timeout(timeout))
        order.set_verified()
        self.order_manager.order_repository.update(order)

        # Create the tick
        tick = Tick.from_order(order)

        block = await self.create_new_tick_block(tick)
        self.logger.info("Ask created with asset pair %s", assets)
        order.broadcast_peers = self.broadcast_block(block)
        if self.is_matchmaker:
            tick.block_hash = block.hash
            # Search for matches
            self.order_book.insert_ask(tick)
            self.match(tick)
        return order

    async def create_bid(self, assets: AssetPair, timeout: int) -> Order:
        """
        Create an ask order (sell order)

        :param assets: The assets to exchange
        :param timeout: The timeout of the order, when does the order need to be timed out
        :return: The created order
        """
        self.verify_offer_creation(assets, timeout)

        # Create the order
        order = self.order_manager.create_bid_order(assets, Timeout(timeout))
        order.set_verified()
        self.order_manager.order_repository.update(order)

        # Create the tick
        tick = Tick.from_order(order)

        block = await self.create_new_tick_block(tick)
        self.logger.info("Bid created with asset pair %s", assets)
        order.broadcast_peers = self.broadcast_block(block)
        if self.is_matchmaker:
            tick.block_hash = block.hash
            # Search for matches
            self.order_book.insert_bid(tick)
            self.match(tick)
        return order

    def broadcast_block(self, block: MarketBlock) -> List[Peer]:
        """
        Broadcast a block with market information to matchmakers.
        :param block: The block to broadcast.
        :return The peers this block was sent to.
        """
        global_time = self.claim_global_time()
        dist = GlobalTimeDistributionPayload(global_time)
        payload = HalfBlockBroadcastPayload.from_half_block(block, self.settings.ttl)
        packet = self._ez_pack(self.trustchain._prefix, 5, [dist, payload], False)
        if self.fixed_broadcast_set:
            broadcast_peers = self.fixed_broadcast_set
        else:
            broadcast_peers = random.sample(self.matchmakers, min(len(self.matchmakers), self.settings.fanout))

        for peer in broadcast_peers:
            self.endpoint.send(peer.address, packet)
        self.trustchain.relayed_broadcasts.add(block.block_id)

        return broadcast_peers

    def broadcast_block_pair(self, block1: MarketBlock, block2: MarketBlock) -> List[Peer]:
        """
        Broadcast a block with market information to matchmakers.
        :param block1: The first part of the block pair to broadcast.
        :param block2: The second part of the block pair to broadcast.
        :return The peers this block was sent to.
        """
        global_time = self.claim_global_time()
        dist = GlobalTimeDistributionPayload(global_time)
        payload = HalfBlockPairBroadcastPayload.from_half_blocks(block1, block2, self.settings.ttl)
        packet = self._ez_pack(self.trustchain._prefix, 6, [dist, payload], False)
        if self.fixed_broadcast_set:
            broadcast_peers = self.fixed_broadcast_set
        else:
            broadcast_peers = random.sample(self.matchmakers, min(len(self.matchmakers), self.settings.fanout))

        for peer in broadcast_peers:
            self.endpoint.send(peer.address, packet)
        self.trustchain.relayed_broadcasts.add(block1.block_id)

        return broadcast_peers

    def received_block(self, block: MarketBlock) -> None:
        """
        We received a block for the market community.
        Process it accordingly, after checking the version number first.
        """
        if block.transaction.get("version") != self.PROTOCOL_VERSION:
            return

        if block.type in (b"ask", b"bid") and block.public_key != self.trustchain.my_peer.public_key.key_to_bin():
            self.process_tick_block(block)
        elif block.type == b"tx_init":
            self.process_tx_init_block(block)
        elif block.type == b"tx_payment":
            ensure_future(self.process_tx_payment_block(block))
        elif block.type == b"tx_done":
            self.process_tx_done_block(block)
        elif block.type == b"cancel_order":
            self.process_cancel_order_block(block)

    def add_matchmaker(self, matchmaker: Peer) -> None:
        """
        Add a matchmaker to the set of known matchmakers. Also check whether there are pending deferreds.
        """
        if matchmaker.public_key.key_to_bin() == self.my_peer.public_key.key_to_bin():
            return

        self.matchmakers.add(matchmaker)

    @synchronized
    async def create_new_tick_block(self, tick: Tick) -> MarketBlock:
        """
        Create a block on TradeChain defining a new tick (either ask or bid).

        :param tick: The tick we want to persist to the TradeChain.
        :return: A MarketBlock with the order details.
        """
        block_type = b'ask' if tick.is_ask() else b'bid'
        tx_dict = {
            "tick": tick.to_block_dict(),
            "entrusted": self.get_entrusted_assets(),
            "version": self.PROTOCOL_VERSION
        }
        blocks = await self.trustchain.create_source_block(block_type=block_type, transaction=tx_dict)
        return blocks[0]

    @synchronized
    async def create_new_cancel_order_block(self, order: Order) -> MarketBlock:
        """
        Create a block on TradeChain defining a cancellation of an order.
        :param order: The tick order to cancel
        """
        tx_dict = {
            "trader_id": order.order_id.trader_id.as_hex(),
            "order_number": int(order.order_id.order_number),
            "entrusted": self.get_entrusted_assets(),
            "version": self.PROTOCOL_VERSION
        }
        blocks = await self.trustchain.create_source_block(block_type=b'cancel_order', transaction=tx_dict)
        return blocks[0]

    @synchronized
    async def create_new_tx_init_block(self, peer: Peer, accepted_trade: AcceptedTrade) -> Transaction:
        """
        Create a block on TradeChain defining initiation of a transaction.

        :param: peer: The peer to send the block to
        :param: ask_order_dict: A dictionary containing the status of the ask order
        :param: bid_order_dict: A dictionary containing the status of the bid order
        :param accepted_trade: Details on the accepted trade
        """
        order = self.order_manager.order_repository.find_by_id(accepted_trade.recipient_order_id)
        incoming_address, outgoing_address = self.get_order_addresses(order)
        tx_dict = {
            "tx": accepted_trade.to_block_dictionary(),
            "wallets": {
                "incoming": incoming_address.address,
                "outgoing": outgoing_address.address
            },
            "entrusted": self.get_entrusted_assets(),
            "version": self.PROTOCOL_VERSION
        }
        blocks = await self.trustchain.sign_block(peer, peer.public_key.key_to_bin(),
                                                  block_type=b'tx_init', transaction=tx_dict)

        transaction_id = TransactionId(blocks[1].hash)
        transaction = Transaction.from_accepted_trade(accepted_trade, transaction_id)
        transaction.trading_peer = peer

        transaction.incoming_address = incoming_address
        transaction.outgoing_address = outgoing_address
        transaction.partner_incoming_address = WalletAddress(blocks[0].transaction["wallets"]["incoming"])
        transaction.partner_outgoing_address = WalletAddress(blocks[0].transaction["wallets"]["outgoing"])

        self.transaction_manager.transaction_repository.add(transaction)
        return transaction

    @synchronized
    async def create_new_tx_payment_block(self, peer: Peer, payment: Payment) -> MarketBlock:
        """
        Create a block on TrustChain with information about a specific payment.

        :param peer: The peer that we did this transaction with
        :param payment: The payment to record
        :return: A deferred that fires when the transaction counterparty has signed and returned the block.
        """
        tx_dict = {
            "payment": payment.to_dictionary(),
            "entrusted": self.get_entrusted_assets(),
            "version": self.PROTOCOL_VERSION
        }
        blocks = await self.trustchain.sign_block(peer, peer.public_key.key_to_bin(),
                                                  block_type=b'tx_payment', transaction=tx_dict)
        return blocks[0]

    @synchronized
    async def create_new_tx_done_block(self, peer: Peer, ask_order_dict: Dict, bid_order_dict: Dict,
                                       transaction: Transaction) -> MarketBlock:
        """
        Create a block on TrustChain defining completion of a transaction.

        :param: peer: The peer to send the block to
        :param: ask_order_dict: A dictionary containing the status of the ask order
        :param: bid_order_dict: A dictionary containing the status of the bid order
        :param transaction: The transaction that has been completed
        :return: A deferred that fires when the transaction counterparty has signed and returned the block.
        """
        tx_dict = {
            "ask": ask_order_dict,
            "bid": bid_order_dict,
            "tx": transaction.to_block_dictionary(),
            "entrusted": self.get_entrusted_assets(),
            "version": self.PROTOCOL_VERSION
        }
        blocks = await self.trustchain.sign_block(peer, peer.public_key.key_to_bin(),
                                                  block_type=b'tx_done', transaction=tx_dict)
        return blocks[0]

    async def on_tick(self, tick: Tick) -> None:
        """
        Process an incoming tick.
        :param tick: the received tick to process
        """
        self.logger.debug("%s received from trader %s, asset pair: %s", type(tick),
                          tick.order_id.trader_id.as_hex(), tick.assets)

        if self.is_matchmaker:
            insert_method = self.order_book.insert_ask if isinstance(tick, Ask) else self.order_book.insert_bid

            if not self.order_book.tick_exists(tick.order_id) and tick.order_id not in self.cancelled_orders:
                self.logger.info("Inserting tick %s from %s, asset pair: %s", tick, tick.order_id, tick.assets)
                insert_method(tick)

                if self.order_book.tick_exists(tick.order_id):
                    # Check for new matches against the orders of this node
                    for order in self.order_manager.order_repository.find_all():
                        order_tick_entry = self.order_book.get_tick(order.order_id)
                        if not order.is_valid() or not order_tick_entry:
                            continue

                        self.match(order_tick_entry.tick)

                    # Only after we have matched our own orders, do the matching with other ticks if necessary
                    self.match(tick)

    def send_match_messages(self, matching_ticks: List[TickEntry], order_id: OrderId) -> None:
        for tick_entry in matching_ticks:
            self.send_match_message(tick_entry.tick, order_id)

    def send_match_message(self, tick: Tick, recipient_order_id: OrderId) -> None:
        """
        Send a match message to a specific node
        :param tick: The matched tick
        :param recipient_order_id: The order id of the recipient, matching the tick
        """
        if (recipient_order_id, tick.order_id) in self.sent_matches:
            return
        self.sent_matches.add((recipient_order_id, tick.order_id))

        payload_tup = tick.to_network()

        # Add recipient order number, matched quantity, trader ID of the matched person, our own trader ID and match ID
        my_id = TraderId(self.mid)
        payload_tup += (recipient_order_id.order_number, tick.order_id.trader_id, my_id)

        async def get_address():
            try:
                address = await self.get_address_for_trader(recipient_order_id.trader_id)
            except RuntimeError:
                address = None

            if not address:
                return

            self.logger.info("Sending match message for order id %s and tick order id %s to trader %s",
                             str(recipient_order_id), str(tick.order_id), recipient_order_id.trader_id.as_hex())

            auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
            payload = MatchPayload(*payload_tup)

            packet = self._ez_pack(self._prefix, MSG_MATCH, [auth, payload])
            self.endpoint.send(address, packet)

        self.register_task('get_address_for_trader_%s-%s' % (recipient_order_id, tick.order_id), get_address,
                           delay=random.uniform(0, self.settings.match_send_interval))

    @lazy_wrapper(MatchPayload)
    def received_match(self, peer: Peer, payload: MatchPayload) -> None:
        """
        We received a match message from a matchmaker.
        """
        self.logger.info("We received a match message from %s for order %s.%s (matched against %s.%s)",
                         payload.matchmaker_trader_id.as_hex(), TraderId(self.mid).as_hex(),
                         payload.recipient_order_number, payload.trader_id.as_hex(), payload.order_number)

        # We got a match, check whether we can respond to this match
        self.update_ip(payload.matchmaker_trader_id, peer.address)
        self.add_matchmaker(peer)

        self.process_match_payload(payload)

    def process_match_payload(self, payload: MatchPayload) -> None:
        """
        Process a match payload.
        """
        order_id = OrderId(TraderId(self.mid), payload.recipient_order_number)
        order = self.order_manager.order_repository.find_by_id(order_id)
        if not order:
            self.logger.warning("Cannot find order %s in order repository!", order_id)
            return

        if order.status != "open":
            # Send a declined match back so the matchmaker removes the order from their book
            decline_reason = DeclineMatchReason.ORDER_COMPLETED if order.status != "open" \
                else DeclineMatchReason.OTHER

            other_order_id = OrderId(payload.match_trader_id, payload.recipient_order_number)
            self.send_decline_match_message(order, other_order_id, payload.matchmaker_trader_id, decline_reason)
            return

        cache = self.request_cache.get("match", int(payload.recipient_order_number))
        if not cache:
            cache = MatchCache(self, order)
            self.request_cache.add(cache)

        # Add the match to the cache and process it
        cache.add_match(payload)

    async def entrust_prospective_counterparty(self, order: Order, trader_id: TraderId, assets: AssetPair) -> Tuple[bool, int]:
        """
        Check the entrust limits of the counterparty.
        Return whether we want to trade with the counterparty and if so, how many payments would be required.
        """
        self.logger.info("Checking entrust values for trader %s", trader_id)
        address = await self.get_address_for_trader(trader_id)
        if not address:
            self.logger.info("Entrust policy is unable to determine address of trader %s", trader_id.as_hex())
            return False, 0

        # Wait a small, random period to avoid getting the non-latest state
        await sleep(random.random(), loop=get_event_loop())

        # Get the public key of the peer
        peer_pk = await self.send_trader_pk_request(trader_id)
        peer_pk = peer_pk.key_to_bin()
        peer = Peer(peer_pk, address=address)

        blocks = await self.trustchain.send_crawl_request(peer, peer_pk, -1, -1)
        if not blocks:
            self.logger.info("Counterparty did not send blocks, failing entrust policy")
            return False, 0

        block = blocks[0] if (blocks[0].public_key == peer_pk or len(blocks) == 1) else blocks[1]  # Get the right block
        if block.type not in [b"ask", b"bid", b"cancel_order", b"tx_init", b"tx_payment", b"tx_done"]:
            self.logger.info("Unknown last block type %s, not trading with this counterparty", block.type)
            return False, 0

        # We now compute the total entrusted value by that party and check if we can still trade within the desired window.
        entrusted = 0
        for asset_id, asset_amount in block.transaction["entrusted"].items():
            if asset_id not in CONVERSION_RATES:
                raise RuntimeError("Conversion rate for asset %s not found!" % asset_id)
            entrusted += (asset_amount / 10 ** CONVERSION_RATES[asset_id][0] * CONVERSION_RATES[asset_id][1])

        trading_residue = self.settings.entrust_limit - entrusted

        outgoing_assets = assets.first if order.is_ask() else assets.second
        if outgoing_assets.asset_id not in CONVERSION_RATES:
            raise RuntimeError("Conversion rate for asset %s not found!" % outgoing_assets.asset_id)
        value_to_trade = outgoing_assets.amount/10 ** CONVERSION_RATES[outgoing_assets.asset_id][0] * CONVERSION_RATES[outgoing_assets.asset_id][1]

        if trading_residue <= 0:
            self.logger.info("Will NOT trade with trader %s (trading residue negative or zero)", trader_id.as_hex())
            return False, 0

        # There is some residue left. Compute the number of incremental payments required for the trade to remain under
        # the residue.
        payments_required = ceil(value_to_trade / trading_residue)
        if payments_required > self.settings.max_payments_per_trade:
            self.logger.info("Will NOT trade with trader %s, too much payments required (%d)",
                             trader_id.as_hex(), payments_required)
            return False, 0

        self.logger.info("Will trade with trader %s (residue: %d, value to trade: %d, payments: %d)",
                         trader_id.as_hex(), trading_residue, value_to_trade, payments_required)
        return True, payments_required

    async def accept_match_and_propose(self, order: Order, other_order_id: OrderId, other_price: Price,
                                       other_quantity: int, propose_quantity=None, should_reserve=True) -> None:
        """
        Accept an incoming match payload and propose a trade to the counterparty
        """
        if should_reserve:
            if order.available_quantity == 0:
                self.logger.info("No available quantity for order %s - not sending outgoing proposal", order.order_id)

                # Notify the match cache
                cache = self.request_cache.get("match", int(order.order_id.order_number))
                if cache:
                    cache.received_decline_trade(other_order_id, DeclinedTradeReason.NO_AVAILABLE_QUANTITY)
                return

            # Pre-actively reserve the available quantity in the order
            propose_quantity = min(order.available_quantity, other_quantity)
            order.reserve_quantity_for_tick(other_order_id, propose_quantity)
            self.order_manager.order_repository.update(order)

        # If we are going to send out an incoming proposal that will be accepted, we will become the risktaker in
        # the upcoming trade (since we have to make the first payment). Therefore, carefully check the entrust limit of
        # the counterparty.
        do_entrust = True
        num_payments = self.settings.default_payments_per_trade
        if self.settings.entrust_limit != -1:
            propose_quantity_scaled = AssetPair.from_price(other_price, propose_quantity)
            if propose_quantity_scaled.second.amount == 0:
                propose_quantity_scaled.second = AssetAmount(1, propose_quantity_scaled.second.asset_id)
            do_entrust, num_payments = await self.entrust_prospective_counterparty(order, other_order_id.trader_id, propose_quantity_scaled)

        decline_reason = DeclinedTradeReason.ALREADY_TRADING
        if not do_entrust:
            # Release the quantity again
            order.release_quantity_for_tick(other_order_id, propose_quantity)
            self.order_manager.order_repository.update(order)

            # Notify the match cache
            cache = self.request_cache.get("match", int(order.order_id.order_number))
            if cache:
                cache.received_decline_trade(other_order_id, decline_reason)
            return

        # Otherwise, propose!
        await self.propose_trade(order, other_order_id, propose_quantity, other_price, num_payments)

    async def propose_trade(self, order: Order, other_order_id: OrderId, propose_quantity: int, other_price: Price,
                            num_payments: int) -> None:
        propose_quantity_scaled = AssetPair.from_price(other_price, propose_quantity)
        if propose_quantity_scaled.second.amount == 0:
            propose_quantity_scaled.second = AssetAmount(1, propose_quantity_scaled.second.asset_id)

        propose_trade = Trade.propose(
            TraderId(self.mid),
            order.order_id,
            other_order_id,
            propose_quantity_scaled,
            num_payments,
            Timestamp.now()
        )

        # Fetch the address of the target peer (we are not guaranteed to know it at this point since we might have
        # received the order indirectly)
        try:
            address = await self.get_address_for_trader(propose_trade.recipient_order_id.trader_id)
        except RuntimeError:
            address = None

        if address:
            self.send_proposed_trade(propose_trade, address)
        else:
            order.release_quantity_for_tick(other_order_id, propose_quantity)

            # Notify the match cache
            cache = self.request_cache.get("match", int(order.order_id.order_number))
            if cache:
                cache.received_decline_trade(other_order_id, DeclinedTradeReason.ADDRESS_LOOKUP_FAIL)

    def send_decline_match_message(self, order: Order, other_order_id: OrderId,
                                   matchmaker_trader_id: TraderId, decline_reason: DeclineMatchReason) -> None:
        address = self.lookup_ip(matchmaker_trader_id)

        self.logger.info("Sending decline match message for order %s to trader %s (ip: %s, port: %s)",
                         order.order_id, matchmaker_trader_id.as_hex(), *address)

        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = (TraderId(self.mid), Timestamp.now(), order.order_id.order_number, other_order_id, decline_reason)
        payload = DeclineMatchPayload(*payload)

        packet = self._ez_pack(self._prefix, MSG_MATCH_DECLINE, [auth, payload])
        self.endpoint.send(address, packet)

    @lazy_wrapper(DeclineMatchPayload)
    def received_decline_match(self, _, payload: DeclineMatchPayload) -> None:
        order_id = OrderId(payload.trader_id, payload.order_number)
        matched_order_id = payload.other_order_id
        self.logger.info("Received decline-match message for tick %s matched with %s, reason %s", order_id,
                         matched_order_id, payload.decline_reason)

        # It could be that one or both matched tick(s) have already been removed from the order book by a
        # tx_done block. We have to account for that and act accordingly.
        tick_entry = self.order_book.get_tick(order_id)
        matched_tick_entry = self.order_book.get_tick(matched_order_id)

        if tick_entry and matched_tick_entry:
            tick_entry.block_for_matching(matched_tick_entry.order_id)
            matched_tick_entry.block_for_matching(tick_entry.order_id)

        if matched_tick_entry and (payload.decline_reason == DeclineMatchReason.OTHER_ORDER_COMPLETED or
                                   payload.decline_reason == DeclineMatchReason.OTHER_ORDER_CANCELLED):
            self.order_book.remove_tick(matched_tick_entry.order_id)
            self.order_book.completed_orders.add(matched_tick_entry.order_id)
            self.on_order_completed(matched_tick_entry.order_id)

        if payload.decline_reason == DeclineMatchReason.ORDER_COMPLETED and tick_entry:
            self.order_book.remove_tick(tick_entry.order_id)
            self.order_book.completed_orders.add(tick_entry.order_id)
        elif tick_entry:
            # Search for a new match
            self.match(tick_entry.tick)

    async def cancel_order(self, order_id: OrderId, broadcast: bool = True) -> None:
        order = self.order_manager.order_repository.find_by_id(order_id)
        if order and (order.status == "open" or order.status == "unverified"):
            self.order_manager.cancel_order(order_id)

            if self.is_matchmaker:
                self.order_book.remove_tick(order_id)

            if order.verified:
                block = await self.create_new_cancel_order_block(order)
                if broadcast:
                    self.broadcast_block(block)

    def on_order_completed(self, order_id: OrderId) -> None:
        """
        An order has been completed. Update the match caches accordingly
        """
        for cache in self.get_match_caches():
            cache.remove_order(order_id)

    # Proposed trade
    def send_proposed_trade(self, proposed_trade: ProposedTrade, address: Tuple) -> None:
        payload = proposed_trade.to_network()

        self.request_cache.add(ProposedTradeRequestCache(self, proposed_trade))

        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = TradePayload(*payload)

        self.logger.debug("Sending proposed trade with own order id %s and other order id %s to trader "
                          "%s, asset pair %s", str(proposed_trade.order_id),
                          str(proposed_trade.recipient_order_id), proposed_trade.recipient_order_id.trader_id.as_hex(),
                          proposed_trade.assets)

        packet = self._ez_pack(self._prefix, MSG_PROPOSED_TRADE, [auth, payload])
        self.endpoint.send(address, packet)

    def check_trade_payload_validity(self, payload: TradePayload) -> Tuple[bool, str]:
        if bytes(payload.recipient_order_id.trader_id) != self.mid:
            return False, "this payload is not meant for this node"

        if not self.order_manager.order_repository.find_by_id(payload.recipient_order_id):
            return False, "order does not exist"

        return True, ""

    def get_outstanding_proposals(self, order_id: OrderId, partner_order_id: OrderId) -> List[Tuple[int, RequestCache]]:
        return [(proposal_id, cache) for proposal_id, cache in self.request_cache._identifiers.items()
                if isinstance(cache, ProposedTradeRequestCache)
                and cache.proposed_trade.order_id == order_id
                and cache.proposed_trade.recipient_order_id == partner_order_id]

    def get_match_caches(self) -> List[MatchCache]:
        """
        Return all match caches.
        """
        return [cache for cache in self.request_cache._identifiers.values() if isinstance(cache, MatchCache)]

    @lazy_wrapper(TradePayload)
    async def received_proposed_trade(self, peer: Peer, payload: TradePayload) -> None:
        validation = self.check_trade_payload_validity(payload)
        if not validation[0]:
            self.logger.warning("Validation of proposed trade payload failed: %s", validation[1])
            return

        proposed_trade = ProposedTrade.from_network(payload)

        self.logger.debug("Proposed trade received from trader %s for order %s",
                          proposed_trade.trader_id.as_hex(), str(proposed_trade.recipient_order_id))

        # Update the known IP address of the sender of this proposed trade
        self.update_ip(proposed_trade.trader_id, peer.address)

        order = self.order_manager.order_repository.find_by_id(proposed_trade.recipient_order_id)

        # We can have a race condition where an ask/bid is created simultaneously on two different nodes.
        # In this case, both nodes first send a proposed trade and then receive a proposed trade from the other
        # node. To counter this, we have the following check.
        outstanding_proposals = self.get_outstanding_proposals(order.order_id, proposed_trade.order_id)
        if outstanding_proposals:
            # Discard current outstanding proposed trade and continue
            for proposal_id, _ in outstanding_proposals:
                request = self.request_cache.get("proposed-trade", int(proposal_id.split(':')[1]))
                eq_and_ask = order.assets.first.amount == request.proposed_trade.assets.first.amount and order.is_ask()
                have_largest_order = order.assets.first.amount > request.proposed_trade.assets.first.amount
                if eq_and_ask or have_largest_order:
                    self.logger.info("Discarding current outstanding proposals for order %s", proposed_trade.order_id)
                    self.request_cache.pop("proposed-trade", int(proposal_id.split(':')[1]))
                    request.on_timeout()

        # if order.available_quantity == 0:
        #     # No quantity available in this order, decline
        #     decline_reason = DeclinedTradeReason.ORDER_COMPLETED if order.status == "completed" else DeclinedTradeReason.ORDER_RESERVED
        #     declined_trade = Trade.decline(TraderId(self.mid), Timestamp.now(), proposed_trade, decline_reason)
        #     self.send_decline_trade(declined_trade)
        #     return
        #
        # # Pre-actively reserve quantity in the order
        # quantity_in_propose = proposed_trade.assets.first.amount
        # should_counter = quantity_in_propose > order.available_quantity
        # reserve_quantity = min(quantity_in_propose, order.available_quantity)
        # order.reserve_quantity_for_tick(proposed_trade.order_id, reserve_quantity)
        # self.order_manager.order_repository.update(order)
        #
        # result = await self.should_accept_propose_trade(proposed_trade, order)
        # should_trade, decline_reason = result
        # if not should_trade:
        #     self.decline_proposed_trade(order, proposed_trade, decline_reason, reserve_quantity)
        # else:
        #     if not should_counter:  # Enough quantity left
        #         self.accept_proposed_trade(proposed_trade)
        #     else:
        #         # Not all quantity can be traded, so we are going to make a counter trade.
        #         # This will mean that we will become the risky party, so we have to verify the entrust limits.
        #         do_entrust = True
        #         num_payments = 1
        #         if self.settings.entrust_limit != -1:
        #             do_entrust, num_payments = await self.entrust_prospective_counterparty(order,
        #                                                                                    proposed_trade.order_id.trader_id,
        #                                                                                    reserve_quantity)
        #             if num_payments >= self.settings.max_payments_per_trade:
        #                 # The resulting trade would result in too many payments - decline.
        #                 self.decline_proposed_trade(order, proposed_trade, decline_reason, reserve_quantity)
        #
        #         if do_entrust:
        #             new_pair = order.assets.proportional_downscale(first=reserve_quantity)
        #             counter_trade = Trade.counter(TraderId(self.mid), new_pair, num_payments, Timestamp.now(), proposed_trade)
        #             self.logger.debug("Counter trade made with asset pair %s for proposed trade", counter_trade.assets)
        #             self.send_counter_trade(counter_trade)
        #         else:
        #             self.decline_proposed_trade(order, proposed_trade, decline_reason, reserve_quantity)
        self.accept_proposed_trade(proposed_trade)

    def decline_proposed_trade(self, order, proposed_trade, decline_reason, reserve_quantity):
        declined_trade = Trade.decline(TraderId(self.mid), Timestamp.now(), proposed_trade, decline_reason)
        self.logger.debug("Declined trade made for order id: %s and id: %s "
                          "(valid? %s, available quantity of order: %s, reserved: %s, traded: %s), reason: %s",
                          str(declined_trade.order_id), str(declined_trade.recipient_order_id),
                          order.is_valid(), order.available_quantity, order.reserved_quantity,
                          order.traded_quantity, decline_reason)
        self.send_decline_trade(declined_trade)
        order.release_quantity_for_tick(proposed_trade.order_id, reserve_quantity)
        self.order_manager.order_repository.update(order)

    async def should_accept_propose_trade(self, proposed_trade: ProposedTrade, my_order: Order) -> Tuple[bool, Optional[DeclinedTradeReason]]:
        # First, check some basic conditions
        should_trade = False
        decline_reason = DeclinedTradeReason.OTHER
        if not my_order.is_valid:
            decline_reason = DeclinedTradeReason.ORDER_INVALID
        elif my_order.status == "expired":
            decline_reason = DeclinedTradeReason.ORDER_EXPIRED
        elif my_order.status == "cancelled":
            decline_reason = DeclinedTradeReason.ORDER_CANCELLED
        elif not my_order.has_acceptable_price(proposed_trade.assets):
            self.logger.info("Unacceptable price for order %s - %s vs %s", my_order.order_id,
                             proposed_trade.assets, my_order.assets)
            decline_reason = DeclinedTradeReason.UNACCEPTABLE_PRICE
        else:
            should_trade = True

        if not should_trade:
            return False, decline_reason

        return True, None

    def send_decline_trade(self, declined_trade: DeclinedTrade) -> None:
        payload = declined_trade.to_network()

        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = DeclineTradePayload(*payload)

        packet = self._ez_pack(self._prefix, MSG_DECLINED_TRADE, [auth, payload])
        self.endpoint.send(self.lookup_ip(declined_trade.recipient_order_id.trader_id), packet)

    @lazy_wrapper(DeclineTradePayload)
    def received_decline_trade(self, _, payload: DeclineTradePayload) -> None:
        validation = self.check_trade_payload_validity(payload)
        if not validation[0]:
            self.logger.warning("Validation of decline trade payload failed: %s", validation[1])
            return

        declined_trade = DeclinedTrade.from_network(payload)

        if not self.request_cache.has("proposed-trade", declined_trade.proposal_id):
            self.logger.warning("declined trade cache with id %s not found", declined_trade.proposal_id)
            return

        request = self.request_cache.pop("proposed-trade", declined_trade.proposal_id)

        order = self.order_manager.order_repository.find_by_id(declined_trade.recipient_order_id)
        proposed_assets = request.proposed_trade.assets
        proposed_owned = proposed_assets.first.amount
        order.release_quantity_for_tick(declined_trade.order_id, proposed_owned)
        self.order_manager.order_repository.update(order)

        # Just remove the tick with the order id of the other party and try to find a new match
        self.logger.debug("Received decline trade (proposal id: %d, reason: %d)",
                          declined_trade.proposal_id, declined_trade.decline_reason)

        other_order_id = OrderId(payload.trader_id, payload.order_number)

        # Update the cache which will inform the related matchmakers
        cache = self.request_cache.get("match", int(order.order_id.order_number))
        if cache:
            cache.received_decline_trade(other_order_id, payload.decline_reason)

        # We want to remove this order from all the other caches too if the order is completed or cancelled
        if payload.decline_reason == DeclinedTradeReason.ORDER_COMPLETED or payload.decline_reason == DeclinedTradeReason.ORDER_CANCELLED:
            for cache in self.get_match_caches():
                cache.remove_order(other_order_id)

    # Counter trade
    def send_counter_trade(self, counter_trade: CounterTrade) -> None:
        payload = counter_trade.to_network()

        self.request_cache.add(ProposedTradeRequestCache(self, counter_trade))

        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = TradePayload(*payload)

        packet = self._ez_pack(self._prefix, MSG_COUNTER_TRADE, [auth, payload])
        self.endpoint.send(self.lookup_ip(counter_trade.recipient_order_id.trader_id), packet)

    @lazy_wrapper(TradePayload)
    def received_counter_trade(self, _, payload: TradePayload) -> None:
        validation = self.check_trade_payload_validity(payload)
        if not validation[0]:
            self.logger.warning("Validation of counter trade payload failed: %s", validation[1])
            return

        counter_trade = CounterTrade.from_network(payload)

        if not self.request_cache.has("proposed-trade", counter_trade.proposal_id):
            self.logger.warning("proposed trade cache with id %s not found", counter_trade.proposal_id)
            return

        request = self.request_cache.pop("proposed-trade", counter_trade.proposal_id)

        order = self.order_manager.order_repository.find_by_id(counter_trade.recipient_order_id)
        self.logger.info("Received counter trade for order %s (quantity: %d)", order.order_id,
                         counter_trade.assets.first.amount)
        should_decline = True
        decline_reason = 0
        if not order.is_valid:
            decline_reason = DeclinedTradeReason.ORDER_INVALID
        elif not order.has_acceptable_price(counter_trade.assets):
            self.logger.info("Unacceptable price for order %s - %s vs %s", order.order_id,
                             counter_trade.assets, order.assets)
            decline_reason = DeclinedTradeReason.UNACCEPTABLE_PRICE
        else:
            should_decline = False

        if should_decline:
            declined_trade = Trade.decline(TraderId(self.mid), Timestamp.now(), counter_trade, decline_reason)
            self.logger.debug("Declined trade made for order id: %s and id: %s ",
                              str(declined_trade.order_id), str(declined_trade.recipient_order_id))
            self.send_decline_trade(declined_trade)

            # Release the quantity from the tick
            proposed_assets = request.proposed_trade.assets
            proposed_owned = proposed_assets.first.amount

            order.release_quantity_for_tick(declined_trade.recipient_order_id, proposed_owned)
            self.order_manager.order_repository.update(order)
        else:
            proposed_assets = request.proposed_trade.assets
            proposed_owned = proposed_assets.first.amount
            counter_assets = counter_trade.assets
            counter_owned = counter_assets.first.amount

            order.release_quantity_for_tick(counter_trade.order_id, proposed_owned)
            order.reserve_quantity_for_tick(counter_trade.order_id, counter_owned)
            self.order_manager.order_repository.update(order)
            self.accept_proposed_trade(counter_trade)

    def accept_proposed_trade(self, proposed_trade: ProposedTrade) -> None:
        accepted_trade = Trade.accept(TraderId(self.mid), Timestamp.now(), proposed_trade)
        payload = accepted_trade.to_network()

        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = TradePayload(*payload)

        packet = self._ez_pack(self._prefix, MSG_ACCEPT_TRADE, [auth, payload])
        self.endpoint.send(self.lookup_ip(proposed_trade.order_id.trader_id), packet)

    @lazy_wrapper(TradePayload)
    async def received_accept_trade(self, peer: Peer, payload: TradePayload) -> None:
        accepted_trade = AcceptedTrade.from_network(payload)

        if not self.request_cache.has("proposed-trade", accepted_trade.proposal_id):
            self.logger.warning("No proposed-trade cache found for proposal id %d", accepted_trade.proposal_id)
            return

        self.request_cache.pop("proposed-trade", accepted_trade.proposal_id)

        order = self.order_manager.order_repository.find_by_id(accepted_trade.recipient_order_id)
        if not order:
            return

        # Create a tx_init block to capture that we are going to initiate a transaction
        transaction = await self.create_new_tx_init_block(peer, accepted_trade)
        self.logger.info("Transaction %s started - initiating payments", transaction.transaction_id.as_hex())
        self.register_anonymous_task('send_payment_%s' % id(transaction), self.send_payment, transaction)
        self.transaction_manager.transaction_repository.update(transaction)

    def send_order_status_request(self, order_id: OrderId) -> Future:
        self.logger.debug("Sending order status request to trader %s (number: %d)",
                          order_id.trader_id.as_hex(), order_id.order_number)

        request_future = Future()
        cache = self.request_cache.add(OrderStatusRequestCache(self, request_future))

        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = OrderStatusRequestPayload(TraderId(self.mid), Timestamp.now(), order_id, cache.number)

        packet = self._ez_pack(self._prefix, MSG_ORDER_QUERY, [auth, payload])
        self.endpoint.send(self.lookup_ip(order_id.trader_id), packet)

        return request_future

    @lazy_wrapper(OrderStatusRequestPayload)
    def received_order_status_request(self, peer: Peer, payload: OrderStatusRequestPayload) -> None:
        order = self.order_manager.order_repository.find_by_id(payload.order_id)

        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())

        order_payload = list(order.to_network())
        order_payload.append(payload.identifier)
        new_payload = OrderStatusResponsePayload(*order_payload)

        packet = self._ez_pack(self._prefix, MSG_ORDER_RESPONSE, [auth, new_payload])
        self.endpoint.send(peer.address, packet)

    @lazy_wrapper(OrderStatusResponsePayload)
    def received_order_status(self, _, payload: OrderStatusResponsePayload) -> None:
        request = self.request_cache.pop("order-status-request", payload.identifier)

        # Convert the order status to a dictionary that is saved on TradeChain
        order_dict = {
            "trader_id": payload.trader_id.as_hex(),
            "order_number": int(payload.order_number),
            "assets": payload.assets.to_dictionary(),
            "traded": payload.traded,
            "timeout": int(payload.timeout),
            "timestamp": int(payload.timestamp),
        }

        request.request_future.set_result(order_dict)

    async def send_payment(self, transaction: Transaction) -> None:
        order = self.order_manager.order_repository.find_by_id(transaction.order_id)

        transfer_amount = transaction.next_payment(order.is_ask())
        asset_id = transfer_amount.asset_id

        wallet = self.wallets[asset_id]
        if not wallet or not wallet.created:
            raise RuntimeError("No %s wallet present" % asset_id)

        # While this conditional is not very pretty, the alternative is to move all this logic to the wallet which
        # requires the wallet to know about transactions, the market community and IPv8.
        if isinstance(wallet, TrustchainWallet):
            peer = Peer(b64decode(str(transaction.partner_incoming_address)),
                        address=self.lookup_ip(transaction.partner_order_id.trader_id))
            transfer_coro = wallet.transfer(transfer_amount.amount, peer)
        else:
            transfer_coro = wallet.transfer(transfer_amount.amount, str(transaction.partner_incoming_address))

        try:
            txid = await transfer_coro
        except Exception as e:
            # When a payment fails, log the error.
            self.logger.error("Payment of %s to %s failed: (%s) %s", transfer_amount,
                              str(transaction.partner_incoming_address), type(e), e)
            return

        payment = Payment(TraderId(self.mid), transaction.transaction_id, transfer_amount,
                          wallet.get_address(), str(transaction.partner_incoming_address), PaymentId(txid),
                          Timestamp.now())

        # Add it to the transaction
        transaction.add_payment(payment)
        self.transaction_manager.transaction_repository.update(transaction)

        order.add_trade(transaction.partner_order_id, payment.transferred_assets)
        self.order_manager.order_repository.update(order)

        await self.create_new_tx_payment_block(transaction.trading_peer, payment)
        self.logger.info("Payment with id %s acknowledged by counterparty!", payment.payment_id)

    def send_matched_transaction_completed(self, transaction: Transaction, block: MarketBlock) -> None:
        """
        Let the matchmaker know that the transaction has been completed.
        :param transaction: The completed transaction.
        :param block: The block created by this peer defining the transaction.
        """
        cache = self.request_cache.get("match", int(transaction.order_id.order_number))
        if cache and cache.order.status != "open":
            # Remove the match request cache
            self.request_cache.pop("match", int(transaction.order_id.order_number))
        elif cache:
            cache.did_trade(transaction, block)

    def received_matched_tx_complete(self, _, data: bytes) -> None:
        self.logger.debug("Received transaction-completed message as a matchmaker")
        if not self.is_matchmaker:
            return

        _, payload = self._ez_unpack_noauth(HalfBlockPairPayload, data)
        block1, block2 = self.trustchain.get_block_class(payload.type1).from_pair_payload(payload, self.serializer)
        self.trustchain.validate_persist_block(block1)
        self.trustchain.validate_persist_block(block2)

        # Update ticks in order book, release the reserved quantity and find a new match
        tx_dict = block1.transaction
        quantity = tx_dict["tx"]["transferred"]["first"]["amount"]
        self.order_book.update_ticks(tx_dict["ask"], tx_dict["bid"], quantity)
        ask_order_id = OrderId(TraderId(unhexlify(tx_dict["ask"]["trader_id"])),
                               OrderNumber(tx_dict["ask"]["order_number"]))
        bid_order_id = OrderId(TraderId(unhexlify(tx_dict["bid"]["trader_id"])),
                               OrderNumber(tx_dict["bid"]["order_number"]))
        self.match_order_ids([ask_order_id, bid_order_id])

        # Broadcast the pair of blocks
        self.broadcast_block_pair(block1, block2)

        order_id = OrderId(TraderId(unhexlify(tx_dict["tx"]["trader_id"])), OrderNumber(tx_dict["tx"]["order_number"]))
        tick_entry_sender = self.order_book.get_tick(order_id)
        if tick_entry_sender:
            self.match(tick_entry_sender.tick)

    def send_trader_pk_request(self, trader_id: TraderId) -> Future:
        if trader_id in self.pk_register:
            return succeed(self.pk_register[trader_id])

        self.logger.debug("Sending public key status request to trader %s", trader_id.as_hex())

        request_future = Future()
        cache = self.request_cache.add(PublicKeyRequestCache(self, trader_id, request_future))

        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        payload = PublicKeyPayload(TraderId(self.mid), Timestamp.now(), cache.number)

        packet = self._ez_pack(self._prefix, MSG_PK_QUERY, [auth, payload])
        self.endpoint.send(self.lookup_ip(trader_id), packet)

        return request_future

    @lazy_wrapper(PublicKeyPayload)
    def received_trader_pk_request(self, peer: Peer, payload: PublicKeyPayload) -> None:
        auth = BinMemberAuthenticationPayload(self.my_peer.public_key.key_to_bin())
        new_payload = PublicKeyPayload(TraderId(self.mid), Timestamp.now(), payload.identifier)

        packet = self._ez_pack(self._prefix, MSG_PK_RESPONSE, [auth, new_payload])
        self.endpoint.send(peer.address, packet)

    @lazy_wrapper(PublicKeyPayload)
    def received_trader_pk_response(self, peer: Peer, payload: PublicKeyPayload) -> None:
        request = self.request_cache.pop("pk-request", payload.identifier)
        self.pk_register[request.trader_id] = peer.public_key
        request.request_future.set_result(peer.public_key)


class MarketTestnetCommunity(MarketCommunity):
    """
    This community defines a testnet for the market.
    """
    community_id = unhexlify('26679385c8aeba3cce644f9e4c6d24d322a771ea')
    DB_NAME = 'market_testnet'
