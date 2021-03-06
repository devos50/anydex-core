from __future__ import absolute_import

from nose.tools import raises

from twisted.internet.defer import fail, inlineCallbacks
from twisted.python.failure import Failure

from anydex.test.util import MockObject, trial_timeout
from anydex.wallet.dummy_wallet import DummyWallet1, DummyWallet2
from anydex.wallet.tc_wallet import TrustchainWallet
from anydex.core.block import MarketBlock
from anydex.core.community import MarketCommunity
from anydex.core.assetamount import AssetAmount
from anydex.core.assetpair import AssetPair
from anydex.core.message import TraderId
from anydex.core.order import Order, OrderId, OrderNumber
from anydex.core.tick import Ask, Bid
from anydex.core.timeout import Timeout
from anydex.core.timestamp import Timestamp
from anydex.core.transaction import Transaction, TransactionId, TransactionNumber
from ipv8.test.base import TestBase
from ipv8.test.mocking.ipv8 import MockIPv8


class TestMarketCommunityBase(TestBase):
    __testing__ = False
    NUM_NODES = 2

    def setUp(self):
        super(TestMarketCommunityBase, self).setUp()
        self.initialize(MarketCommunity, self.NUM_NODES)
        for node in self.nodes:
            node.overlay._use_main_thread = True

    def create_node(self):
        dum1_wallet = DummyWallet1()
        dum2_wallet = DummyWallet2()
        dum1_wallet.MONITOR_DELAY = 0
        dum2_wallet.MONITOR_DELAY = 0

        wallets = {'DUM1': dum1_wallet, 'DUM2': dum2_wallet}

        mock_ipv8 = MockIPv8(u"curve25519", MarketCommunity, create_trustchain=True, create_dht=True,
                             is_matchmaker=True, wallets=wallets, use_database=False, working_directory=u":memory:")
        tc_wallet = TrustchainWallet(mock_ipv8.trustchain)
        mock_ipv8.overlay.wallets['MB'] = tc_wallet

        return mock_ipv8


class TestMarketCommunity(TestMarketCommunityBase):
    __testing__ = True
    NUM_NODES = 3

    def setUp(self):
        super(TestMarketCommunity, self).setUp()

        self.nodes[0].overlay.disable_matchmaker()
        self.nodes[1].overlay.disable_matchmaker()

    @trial_timeout(2)
    @inlineCallbacks
    def test_create_ask(self):
        """
        Test creating an ask and sending it to others
        """
        yield self.introduce_nodes()

        yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)

        yield self.sleep(0.5)

        orders = list(self.nodes[0].overlay.order_manager.order_repository.find_all())
        self.assertTrue(orders)
        self.assertTrue(orders[0].verified)
        self.assertTrue(orders[0].is_ask())
        self.assertEqual(len(self.nodes[2].overlay.order_book.asks), 1)

    @trial_timeout(2)
    @inlineCallbacks
    def test_create_bid(self):
        """
        Test creating a bid and sending it to others
        """
        yield self.introduce_nodes()

        yield self.nodes[0].overlay.create_bid(AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)

        yield self.sleep(0.5)

        orders = list(self.nodes[0].overlay.order_manager.order_repository.find_all())
        self.assertTrue(orders)
        self.assertTrue(orders[0].verified)
        self.assertFalse(orders[0].is_ask())
        self.assertEqual(len(self.nodes[2].overlay.order_book.bids), 1)

    def test_create_invalid_ask_bid(self):
        """
        Test creating an invalid ask/bid with invalid asset pairs.
        """
        invalid_pair = AssetPair(AssetAmount(1, 'DUM2'), AssetAmount(2, 'DUM2'))
        self.assertRaises(RuntimeError, self.nodes[0].overlay.create_ask, invalid_pair, 3600)
        self.assertRaises(RuntimeError, self.nodes[0].overlay.create_bid, invalid_pair, 3600)

    @trial_timeout(2)
    @inlineCallbacks
    def test_decline_trade(self):
        """
        Test declining a trade
        """
        yield self.introduce_nodes()

        order = yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)
        order._traded_quantity = 1  # So it looks like this order has already been fulfilled

        yield self.sleep(0.5)

        self.assertEqual(len(self.nodes[2].overlay.order_book.asks), 1)
        self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)

        yield self.sleep(0.5)

        # The ask should be removed since this node thinks the order is already completed
        self.assertEqual(len(self.nodes[2].overlay.order_book.asks), 0)

    @trial_timeout(3)
    @inlineCallbacks
    def test_decline_trade_cancel(self):
        """
        Test whether a cancelled order is correctly declined when negotiating
        """
        yield self.introduce_nodes()

        order = yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)
        yield self.nodes[0].overlay.cancel_order(order.order_id, broadcast=False)

        self.assertEqual(order.status, "cancelled")

        yield self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)

        yield self.sleep(1)

        # No trade should have been made
        self.assertFalse(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertFalse(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertEqual(len(self.nodes[2].overlay.order_book.asks), 0)

    @trial_timeout(2)
    @inlineCallbacks
    def test_decline_match_cancel(self):
        """
        Test whether an order is removed when the matched order is cancelled
        """
        yield self.introduce_nodes()

        yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)
        yield self.sleep(0.5)

        order = yield self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)
        self.nodes[1].overlay.cancel_order(order.order_id, broadcast=False)  # Immediately cancel it

        yield self.sleep(0.5)

        # No trade should have been made
        self.assertFalse(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertFalse(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertEqual(len(self.nodes[2].overlay.order_book.bids), 0)

    @trial_timeout(2)
    @inlineCallbacks
    def test_counter_trade(self):
        """
        Test making a counter trade
        """
        yield self.introduce_nodes()

        order = yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)
        order._traded_quantity = 1  # Partially fulfill this order

        yield self.sleep(0.5)  # Give it some time to complete the trade

        self.assertEqual(len(self.nodes[2].overlay.order_book.asks), 1)
        self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)

        yield self.sleep(0.5)

        self.assertTrue(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertTrue(list(self.nodes[1].overlay.transaction_manager.find_all()))

    @trial_timeout(3)
    @inlineCallbacks
    def test_completed_trade(self):
        """
        Test whether a completed trade is removed from the orderbook of a matchmaker
        """
        yield self.introduce_nodes()

        yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)

        yield self.sleep(0.5)  # Give it some time to disseminate

        self.assertEqual(len(self.nodes[2].overlay.order_book.asks), 1)
        order = yield self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)
        order._traded_quantity = 2  # Fulfill this order

        yield self.sleep(0.5)

        # The matchmaker should have removed this order from the orderbook
        self.assertFalse(self.nodes[2].overlay.order_book.tick_exists(order.order_id))

    @trial_timeout(3)
    @inlineCallbacks
    def test_other_completed_trade(self):
        """
        Test whether a completed trade of a counterparty is removed from the orderbook of a matchmaker
        """
        yield self.introduce_nodes()

        order = yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)

        yield self.sleep(0.5)  # Give it some time to disseminate

        order._traded_quantity = 2  # Fulfill this order
        self.assertEqual(len(self.nodes[2].overlay.order_book.asks), 1)
        self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)

        yield self.sleep(1)

        # Matchmaker should have removed this order from the orderbook
        self.assertFalse(self.nodes[2].overlay.order_book.tick_exists(order.order_id))

    @trial_timeout(3)
    @inlineCallbacks
    def test_e2e_trade(self):
        """
        Test trading dummy tokens against bandwidth tokens between two persons, with a matchmaker
        """
        yield self.introduce_nodes()

        yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(50, 'DUM1'), AssetAmount(50, 'MB')), 3600)
        yield self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(50, 'DUM1'), AssetAmount(50, 'MB')), 3600)

        yield self.sleep(0.5)  # Give it some time to complete the trade

        # Verify that the trade has been made
        self.assertTrue(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertTrue(list(self.nodes[1].overlay.transaction_manager.find_all()))

        balance1 = yield self.nodes[0].overlay.wallets['DUM1'].get_balance()
        balance2 = yield self.nodes[0].overlay.wallets['MB'].get_balance()
        self.assertEqual(balance1['available'], 950)
        self.assertEqual(balance2['available'], 50)

        balance1 = yield self.nodes[1].overlay.wallets['DUM1'].get_balance()
        balance2 = yield self.nodes[1].overlay.wallets['MB'].get_balance()
        self.assertEqual(balance1['available'], 1050)
        self.assertEqual(balance2['available'], -50)

    @trial_timeout(2)
    @inlineCallbacks
    def test_e2e_trade_dht(self):
        """
        Test a full trade with (dummy assets), where both traders are not connected to each other
        """
        yield self.introduce_nodes()

        for node in self.nodes:
            for other in self.nodes:
                if other != node:
                    node.dht.walk_to(other.endpoint.wan_address)
        yield self.deliver_messages()

        # Remove the address from the mid registry from the trading peers
        self.nodes[0].overlay.mid_register.pop(TraderId(self.nodes[1].overlay.mid))
        self.nodes[1].overlay.mid_register.pop(TraderId(self.nodes[0].overlay.mid))

        for node in self.nodes:
            node.dht.store_peer()
        yield self.deliver_messages()

        yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600)
        yield self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600)

        yield self.sleep(0.5)

        # Verify that the trade has been made
        self.assertTrue(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertTrue(list(self.nodes[1].overlay.transaction_manager.find_all()))

    @inlineCallbacks
    def test_cancel(self):
        """
        Test cancelling an order
        """
        yield self.introduce_nodes()

        ask_order = yield self.nodes[0].overlay.create_ask(
            AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)

        self.nodes[0].overlay.cancel_order(ask_order.order_id)

        yield self.sleep(0.5)

        self.assertTrue(self.nodes[0].overlay.order_manager.order_repository.find_by_id(ask_order.order_id).cancelled)

    @trial_timeout(2)
    @inlineCallbacks
    def test_failing_payment(self):
        """
        Test trading between two persons when a payment fails
        """
        yield self.introduce_nodes()

        for node_nr in [0, 1]:
            self.nodes[node_nr].overlay.wallets['DUM1'].transfer = lambda *_: fail(RuntimeError("oops"))
            self.nodes[node_nr].overlay.wallets['DUM2'].transfer = lambda *_: fail(RuntimeError("oops"))

        yield self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)
        yield self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)

        yield self.sleep(0.5)

        self.assertEqual(list(self.nodes[0].overlay.transaction_manager.find_all())[0].status, "error")
        self.assertEqual(list(self.nodes[1].overlay.transaction_manager.find_all())[0].status, "error")

    @trial_timeout(3)
    @inlineCallbacks
    def test_proposed_trade_timeout(self):
        """
        Test whether we unreserve the quantity if a proposed trade timeouts
        """
        yield self.introduce_nodes()

        self.nodes[0].overlay.decode_map[chr(10)] = lambda *_: None

        ask_order = yield self.nodes[0].overlay.create_ask(
            AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)
        bid_order = yield self.nodes[1].overlay.create_bid(
            AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)

        yield self.sleep(0.5)

        outstanding = self.nodes[1].overlay.get_outstanding_proposals(bid_order.order_id, ask_order.order_id)
        self.assertTrue(outstanding)
        outstanding[0][1].on_timeout()

        yield self.sleep(0.5)

        self.assertEqual(ask_order.reserved_quantity, 0)
        self.assertEqual(bid_order.reserved_quantity, 0)

    @trial_timeout(3)
    @inlineCallbacks
    def test_address_resolv_fail(self):
        """
        Test whether an order is unreserved when address resolution fails
        """
        yield self.introduce_nodes()

        # Clean the mid register of node 1 and make sure DHT peer connection fails
        self.nodes[1].overlay.mid_register = {}
        self.nodes[1].overlay.dht = MockObject()
        self.nodes[1].overlay.dht.connect_peer = lambda *_: fail(Failure(RuntimeError()))

        ask_order = yield self.nodes[0].overlay.create_ask(
            AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)
        bid_order = yield self.nodes[1].overlay.create_bid(
            AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)

        yield self.sleep(0.5)

        self.assertEqual(ask_order.reserved_quantity, 0)
        self.assertEqual(bid_order.reserved_quantity, 0)

    @trial_timeout(4)
    @inlineCallbacks
    def test_orderbook_sync(self):
        """
        Test whether orderbooks are synchronized with a new node
        """
        yield self.introduce_nodes()

        ask_order = yield self.nodes[0].overlay.create_ask(
            AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)
        bid_order = yield self.nodes[1].overlay.create_bid(
            AssetPair(AssetAmount(1, 'DUM1'), AssetAmount(1, 'DUM2')), 3600)

        yield self.deliver_messages(timeout=.5)

        # Add a node that crawls the matchmaker
        self.add_node_to_experiment(self.create_node())
        self.nodes[3].discovery.take_step()
        yield self.deliver_messages(timeout=.5)
        yield self.sleep(0.2)  # For processing the tick blocks

        self.assertTrue(self.nodes[3].overlay.order_book.get_tick(ask_order.order_id))
        self.assertTrue(self.nodes[3].overlay.order_book.get_tick(bid_order.order_id))

        # Add another node that crawls our newest node
        self.add_node_to_experiment(self.create_node())
        self.nodes[4].overlay.send_orderbook_sync(self.nodes[3].overlay.my_peer)
        yield self.deliver_messages(timeout=.5)
        yield self.sleep(0.2)  # For processing the tick blocks

        self.assertTrue(self.nodes[4].overlay.order_book.get_tick(ask_order.order_id))
        self.assertTrue(self.nodes[4].overlay.order_book.get_tick(bid_order.order_id))

    @trial_timeout(4)
    @inlineCallbacks
    def test_partial_trade(self):
        """
        Test a partial trade between two nodes with a matchmaker
        """
        yield self.introduce_nodes()

        yield self.nodes[0].overlay.create_ask(
            AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600)
        yield self.nodes[1].overlay.create_bid(
            AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)

        yield self.sleep(0.5)

        # Verify that the trade has been made
        self.assertTrue(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertTrue(list(self.nodes[1].overlay.transaction_manager.find_all()))

        yield self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(8, 'DUM1'), AssetAmount(8, 'DUM2')), 3600)

        yield self.sleep(1)

        # Verify that the trade has been made
        self.assertEqual(len(list(self.nodes[0].overlay.transaction_manager.find_all())), 2)
        self.assertEqual(len(list(self.nodes[1].overlay.transaction_manager.find_all())), 2)


class TestMarketCommunityTwoNodes(TestMarketCommunityBase):
    __testing__ = True

    @trial_timeout(2)
    @inlineCallbacks
    def test_e2e_trade(self):
        """
        Test a direct trade between two nodes
        """
        yield self.introduce_nodes()

        self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(13, 'DUM2')), 3600)
        self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(13, 'DUM2')), 3600)

        yield self.sleep(0.5)

        # Verify that the trade has been made
        self.assertTrue(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertTrue(list(self.nodes[1].overlay.transaction_manager.find_all()))

        balance1 = yield self.nodes[0].overlay.wallets['DUM1'].get_balance()
        balance2 = yield self.nodes[0].overlay.wallets['DUM2'].get_balance()
        self.assertEqual(balance1['available'], 990)
        self.assertEqual(balance2['available'], 10013)

        balance1 = yield self.nodes[1].overlay.wallets['DUM1'].get_balance()
        balance2 = yield self.nodes[1].overlay.wallets['DUM2'].get_balance()
        self.assertEqual(balance1['available'], 1010)
        self.assertEqual(balance2['available'], 9987)

    @trial_timeout(2)
    @inlineCallbacks
    def test_partial_trade(self):
        """
        Test a partial trade between two nodes
        """
        yield self.introduce_nodes()

        self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600)
        self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(2, 'DUM1'), AssetAmount(2, 'DUM2')), 3600)

        yield self.sleep(0.5)

        # Verify that the trade has been made
        transactions1 = list(self.nodes[0].overlay.transaction_manager.find_all())
        transactions2 = list(self.nodes[1].overlay.transaction_manager.find_all())
        self.assertEqual(len(transactions1), 1)
        self.assertEqual(len(transactions1[0].payments), 2)
        self.assertEqual(len(transactions2), 1)
        self.assertEqual(len(transactions2[0].payments), 2)

        yield self.nodes[1].overlay.create_bid(AssetPair(AssetAmount(8, 'DUM1'), AssetAmount(8, 'DUM2')), 3600)

        yield self.sleep(1)

        # Verify that the trade has been made
        self.assertEqual(len(list(self.nodes[0].overlay.transaction_manager.find_all())), 2)
        self.assertEqual(len(list(self.nodes[1].overlay.transaction_manager.find_all())), 2)

        for node_nr in [0, 1]:
            self.assertEqual(len(self.nodes[node_nr].overlay.order_book.asks), 0)
            self.assertEqual(len(self.nodes[node_nr].overlay.order_book.bids), 0)

    @inlineCallbacks
    def test_ping_pong(self):
        """
        Test the ping/pong mechanism of the market
        """
        yield self.nodes[0].overlay.ping_peer(self.nodes[1].overlay.my_peer)


class TestMarketCommunityFourNodes(TestMarketCommunityBase):
    __testing__ = True
    NUM_NODES = 5

    def setUp(self):
        super(TestMarketCommunityFourNodes, self).setUp()

        self.nodes[0].overlay.disable_matchmaker()
        self.nodes[1].overlay.disable_matchmaker()
        self.nodes[2].overlay.disable_matchmaker()

    @trial_timeout(2)
    @inlineCallbacks
    def test_partial_match(self):
        """
        Test matchmaking with partial orders
        """
        yield self.introduce_nodes()

        self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(5, 'DUM1'), AssetAmount(5, 'DUM2')), 3600)
        self.nodes[1].overlay.create_ask(AssetPair(AssetAmount(5, 'DUM1'), AssetAmount(5, 'DUM2')), 3600)

        yield self.sleep(0.5)

        self.nodes[2].overlay.create_bid(AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600)

        yield self.sleep(0.5)

        # Verify that the trade has been made
        self.assertEqual(len(list(self.nodes[0].overlay.transaction_manager.find_all())), 1)
        self.assertEqual(len(list(self.nodes[1].overlay.transaction_manager.find_all())), 1)
        self.assertEqual(len(list(self.nodes[2].overlay.transaction_manager.find_all())), 2)

    @inlineCallbacks
    def match_window_impl(self, test_ask):
        yield self.introduce_nodes()

        self.nodes[2].overlay.settings.match_window = 0.5  # Wait 1 sec before accepting (the best) match

        if test_ask:
            order1 = yield self.nodes[1].overlay.create_bid(
                AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600)
            order2 = yield self.nodes[0].overlay.create_bid(
                AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(20, 'DUM2')), 3600)
        else:
            order1 = yield self.nodes[0].overlay.create_ask(
                AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600)
            order2 = yield self.nodes[1].overlay.create_ask(
                AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(20, 'DUM2')), 3600)

        yield self.sleep(0.2)

        # Make sure that the two matchmaker match different orders
        order1_tick = self.nodes[3].overlay.order_book.get_tick(order1.order_id)
        order2_tick = self.nodes[4].overlay.order_book.get_tick(order2.order_id)
        order1_tick.available_for_matching = 0
        order2_tick.available_for_matching = 0

        if test_ask:
            yield self.nodes[2].overlay.create_ask(AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(20, 'DUM2')), 3600)
        else:
            yield self.nodes[2].overlay.create_bid(AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(20, 'DUM2')), 3600)

        yield self.sleep(1)

        # Verify that the trade has been made
        self.assertTrue(list(self.nodes[0].overlay.transaction_manager.find_all()))
        self.assertTrue(list(self.nodes[2].overlay.transaction_manager.find_all()))

    @trial_timeout(4)
    @inlineCallbacks
    def test_match_window_bid(self):
        """
        Test the match window when one is matching a new bid
        """
        yield self.match_window_impl(False)

    @trial_timeout(4)
    @inlineCallbacks
    def test_match_window_ask(self):
        """
        Test the match window when one is matching a new ask
        """
        yield self.match_window_impl(True)

    @trial_timeout(4)
    @inlineCallbacks
    def test_match_window_multiple(self):
        """
        Test whether multiple ask orders in the matching window will get matched
        """
        yield self.introduce_nodes()

        self.nodes[2].overlay.settings.match_window = 0.5  # Wait 1 sec before accepting (the best) match

        order1 = yield self.nodes[0].overlay.create_bid(
            AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600)
        order2 = yield self.nodes[1].overlay.create_bid(
            AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600)

        yield self.sleep(0.3)

        # Make sure that the two matchmaker match different orders
        order1_tick = self.nodes[3].overlay.order_book.get_tick(order1.order_id)
        order2_tick = self.nodes[4].overlay.order_book.get_tick(order2.order_id)
        order1_tick.available_for_matching = 0
        order2_tick.available_for_matching = 0

        yield self.nodes[2].overlay.create_ask(AssetPair(AssetAmount(20, 'DUM1'), AssetAmount(20, 'DUM2')), 3600)

        yield self.sleep(1.5)

        # Verify that the trade has been made
        self.assertEqual(len(list(self.nodes[0].overlay.transaction_manager.find_all())), 1)
        self.assertEqual(len(list(self.nodes[1].overlay.transaction_manager.find_all())), 1)
        self.assertEqual(len(list(self.nodes[2].overlay.transaction_manager.find_all())), 2)


class TestMarketCommunitySingle(TestMarketCommunityBase):
    __testing__ = True
    NUM_NODES = 1

    @staticmethod
    def get_tick_block(return_ask, pair):
        tick_cls = Ask if return_ask else Bid
        ask = tick_cls(OrderId(TraderId(b'0' * 20), OrderNumber(1)), pair, Timeout(3600), Timestamp.now(), return_ask)
        ask_tx = ask.to_block_dict()
        ask_tx["address"], ask_tx["port"] = "127.0.0.1", 1337
        tick_block = MarketBlock()
        tick_block.type = b'ask' if return_ask else b'bid'
        tick_block.transaction = {'tick': ask_tx, 'version': MarketCommunity.PROTOCOL_VERSION}
        return tick_block

    @staticmethod
    def get_tx_done_block(ask_amount, bid_amount, traded_amount, ask_total_traded, bid_total_traded):
        ask_pair = AssetPair(AssetAmount(ask_amount, 'BTC'), AssetAmount(ask_amount, 'MB'))
        bid_pair = AssetPair(AssetAmount(bid_amount, 'BTC'), AssetAmount(bid_amount, 'MB'))
        ask = Order(OrderId(TraderId(b'0' * 20), OrderNumber(1)), ask_pair, Timeout(3600), Timestamp.now(), True)
        ask._traded_quantity = ask_total_traded
        bid = Order(OrderId(TraderId(b'1' * 20), OrderNumber(1)), bid_pair, Timeout(3600), Timestamp.now(), False)
        bid._traded_quantity = bid_total_traded
        tx = Transaction(TransactionId(TraderId(b'0' * 20), TransactionNumber(1)),
                         AssetPair(AssetAmount(traded_amount, 'BTC'), AssetAmount(traded_amount, 'MB')),
                         OrderId(TraderId(b'0' * 20), OrderNumber(1)),
                         OrderId(TraderId(b'1' * 20), OrderNumber(1)), Timestamp(0))
        tx.transferred_assets.first += AssetAmount(traded_amount, 'BTC')
        tx.transferred_assets.second += AssetAmount(traded_amount, 'MB')
        tx_done_block = MarketBlock()
        tx_done_block.type = b'tx_done'
        tx_done_block.transaction = {
            'ask': ask.to_status_dictionary(),
            'bid': bid.to_status_dictionary(),
            'tx': tx.to_dictionary(),
            'version': MarketCommunity.PROTOCOL_VERSION
        }
        tx_done_block.transaction['ask']['address'], tx_done_block.transaction['ask']['port'] = "1.1.1.1", 1234
        tx_done_block.transaction['bid']['address'], tx_done_block.transaction['bid']['port'] = "1.1.1.1", 1234
        return tx_done_block

    def test_insert_ask_bid(self):
        """
        Test whether an ask is successfully inserted when a tick block is received
        """
        ask = TestMarketCommunitySingle.get_tick_block(True, AssetPair(AssetAmount(30, 'BTC'), AssetAmount(30, 'MB')))
        bid = TestMarketCommunitySingle.get_tick_block(False, AssetPair(AssetAmount(30, 'BTC'), AssetAmount(29, 'MB')))
        bid.transaction["tick"]["order_number"] = 2  # To give it a different order number

        self.nodes[0].overlay.trustchain.persistence.get_linked = lambda _: True
        self.nodes[0].overlay.received_block(ask)
        self.nodes[0].overlay.received_block(bid)
        self.assertEqual(len(self.nodes[0].overlay.order_book.asks), 1)
        self.assertEqual(len(self.nodes[0].overlay.order_book.bids), 1)

    def test_tx_done_block_new(self):
        """
        Test whether receiving a tx_done block, update the entries in the order book correctly
        """
        tx_done = TestMarketCommunitySingle.get_tx_done_block(10, 3, 3, 3, 3)
        self.nodes[0].overlay.received_block(tx_done)
        self.assertEqual(len(self.nodes[0].overlay.order_book.asks), 1)
        self.assertEqual(len(self.nodes[0].overlay.order_book.bids), 0)

    def test_tx_done_block_asc(self):
        """
        Test whether receiving multiple tx_done blocks, update the entries in the order book correctly
        """
        tx_done = TestMarketCommunitySingle.get_tx_done_block(10, 3, 3, 3, 3)
        self.nodes[0].overlay.received_block(tx_done)
        tx_done = TestMarketCommunitySingle.get_tx_done_block(10, 7, 7, 10, 7)
        self.nodes[0].overlay.received_block(tx_done)
        self.assertEqual(len(self.nodes[0].overlay.order_book.asks), 0)
        self.assertEqual(len(self.nodes[0].overlay.order_book.bids), 0)

    def test_tx_done_block_desc(self):
        """
        Test whether receiving multiple tx_done blocks, update the entries in the order book correctly
        """
        tx_done = TestMarketCommunitySingle.get_tx_done_block(10, 7, 7, 10, 7)
        self.nodes[0].overlay.received_block(tx_done)
        tx_done = TestMarketCommunitySingle.get_tx_done_block(10, 3, 3, 3, 3)
        self.nodes[0].overlay.received_block(tx_done)
        self.assertEqual(len(self.nodes[0].overlay.order_book.asks), 0)
        self.assertEqual(len(self.nodes[0].overlay.order_book.bids), 0)

    @raises(RuntimeError)
    def test_order_invalid_timeout(self):
        """
        Test whether we cannot create an order with an invalid timeout
        """
        self.nodes[0].overlay.create_ask(AssetPair(AssetAmount(10, 'DUM1'), AssetAmount(10, 'DUM2')), 3600 * 1000)
