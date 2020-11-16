import asyncio
import os
import shutil
import time
from asyncio import ensure_future
from binascii import hexlify

import yappi

from anydex.simulation.discrete_loop import EventSimulatorLoop
from anydex.simulation.ipv8 import SimulatedIPv8
from anydex.core import CONVERSION_RATES
from anydex.core.assetamount import AssetAmount
from anydex.core.assetpair import AssetPair
from anydex.core.message import TraderId
from anydex.simulation.scenario import Scenario
from anydex.simulation.wallet import SimulationWallet


class AnyDexSimulation:
    """
    Represents a simulation with some settings.
    """

    def __init__(self, settings):
        self.settings = settings
        self.nodes = []
        self.data_dir = os.path.join("data", "n_%d" % (self.settings.peers,))
        self.scenario = None
        self.orders = {}

        self.loop = EventSimulatorLoop()
        asyncio.set_event_loop(self.loop)

        # Prepare the scenario file, if set
        if settings.scenario_file:
            self.scenario = Scenario(settings.scenario_file)
            self.scenario.parse()
            self.settings.peers = len(self.scenario.unique_users) + 1  # The last peer is the matchmaker
            self.data_dir = os.path.join("data", "n_%d" % (self.settings.peers,))  # Reset the data dir

    def start_ipv8_nodes(self):
        for peer_ind in range(self.settings.peers):
            if peer_ind % 100 == 0:
                print("Created %d peers..." % peer_ind)

            is_matchmaker = (peer_ind == self.settings.peers - 1)

            peer_id = ""
            if peer_ind in self.scenario.index_to_user_map:
                peer_id = self.scenario.index_to_user_map[peer_ind]

            ipv8 = SimulatedIPv8(self.settings, self.data_dir, is_matchmaker, peer_id)
            self.nodes.append(ipv8)

    def create_ask(self, node, order):
        def on_created(o):
            self.orders[order.id] = o

        if order.asset1_type not in CONVERSION_RATES:
            print("Not creating ask due to missing conversion rate for asset %s!" % order.asset1_type)
            return
        if order.asset2_type not in CONVERSION_RATES:
            print("Not creating ask due to missing conversion rate for asset %s!" % order.asset2_type)
            return

        pair = AssetPair(AssetAmount(order.asset1_amount, order.asset1_type), AssetAmount(order.asset2_amount, order.asset2_type))
        ensure_future(node.overlay.create_ask(pair, order.timeout // 1000)).add_done_callback(on_created)
        print("%d - Created ask" % self.loop.time())

    def create_bid(self, node, order):
        def on_created(o):
            self.orders[order.id] = o

        if order.asset1_type not in CONVERSION_RATES:
            print("Not creating bid due to missing conversion rate for asset %s!" % order.asset1_type)
            return
        if order.asset2_type not in CONVERSION_RATES:
            print("Not creating bid due to missing conversion rate for asset %s!" % order.asset2_type)
            return

        pair = AssetPair(AssetAmount(order.asset1_amount, order.asset1_type), AssetAmount(order.asset2_amount, order.asset2_type))
        ensure_future(node.overlay.create_bid(pair, order.timeout // 1000)).add_done_callback(on_created)
        print("%d - Created bid" % self.loop.time())

    def cancel_order(self, node, order):
        print("%d - Cancelling order" % self.loop.time())
        if order.id not in self.orders:
            return
        ensure_future(node.overlay.cancel_order(self.orders[order.id]))

    def create_wallet_if_required(self, node, order):
        if order.asset1_type not in node.overlay.wallets:
            node.overlay.wallets[order.asset1_type] = SimulationWallet(order.asset1_type)
        if order.asset2_type not in node.overlay.wallets:
            node.overlay.wallets[order.asset2_type] = SimulationWallet(order.asset2_type)

    def schedule_scenario_actions(self):
        """
        Schedule all the actions in the scenario in the reactor.
        """
        for order in self.scenario.orders:
            node = self.nodes[self.scenario.user_to_index_map[order.user_id]]
            if order.type == "cancel":
                self.loop.call_at(order.timestamp // 1000, self.cancel_order, node, order)
            elif order.type == "ask":
                self.create_wallet_if_required(node, order)
                self.loop.call_at(order.timestamp // 1000, self.create_ask, node, order)
            elif order.type == "bid":
                self.create_wallet_if_required(node, order)
                self.loop.call_at(order.timestamp // 1000, self.create_bid, node, order)

    def setup_directories(self):
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)

    def ipv8_discover_peers(self):
        # Let nodes discover each other
        for node_a in self.nodes:
            for node_b in self.nodes:
                if node_a == node_b:
                    continue

                node_a.network.verified_peers.add(node_b.my_peer)
                node_a.network.reverse_ip_lookup[node_b.my_peer.address] = node_b
                node_b.network.reverse_ip_lookup[node_a.my_peer.address] = node_a
                node_a.network.discover_services(node_b.my_peer, [node_a.overlay.community_id, ])
                node_a.overlay.update_ip(TraderId(node_b.overlay.mid), node_b.my_peer.address)

            # If this node is a matchmaker, introduce it to everyone
            if node_a.overlay.is_matchmaker:
                for node_b in self.nodes:
                    if node_a == node_b:
                        continue
                    node_b.overlay.add_matchmaker(node_a.my_peer)

    def write_bandwidth_statistics(self):
        # Write bandwidth statistics
        total_bytes_up = 0
        total_bytes_down = 0
        with open(os.path.join(self.data_dir, "bandwidth.csv"), "w") as bw_file:
            bw_file.write("public_key,bytes_up,bytes_down\n")
            for node in self.nodes:
                total_bytes_up += node.endpoint.bytes_up
                total_bytes_down += node.endpoint.bytes_down
                bw_file.write("%s,%d,%d\n" % (
                    hexlify(node.my_peer.public_key.key_to_bin()).decode(), node.endpoint.bytes_up,
                    node.endpoint.bytes_down))

        with open(os.path.join(self.data_dir, "total_bandwidth.csv"), "w") as bw_file:
            bw_file.write("%d,%d" % (total_bytes_up, total_bytes_down))

    def get_bandwidth_stats(self):
        total_bytes_up = 0
        total_bytes_down = 0
        for node in self.nodes:
            total_bytes_up += node.endpoint.bytes_up
            total_bytes_down += node.endpoint.bytes_down

        return total_bytes_up, total_bytes_down

    def get_node_index_of_trader(self, trader_id):
        for index, node in enumerate(self.nodes):
            if node.overlay.mid == bytes(trader_id):
                return index
        return -1

    def collect_simulation_results(self):
        self.write_bandwidth_statistics()

        # Write away the order books
        order_books_dir = os.path.join(self.data_dir, "order_books")
        os.makedirs(order_books_dir, exist_ok=True)
        for index, node in enumerate(self.nodes):
            if node.overlay.is_matchmaker:
                with open(os.path.join(order_books_dir, "%d.txt" % index), "w") as ob_file:
                    ob_file.write(str(node.overlay.order_book))

        # Write away all the transactions
        transactions = []
        for index, node in enumerate(self.nodes):
            for transaction in node.overlay.transaction_manager.find_all():
                partner_peer_id = self.get_node_index_of_trader(transaction.partner_order_id.trader_id)
                if partner_peer_id < index:
                    transactions.append((int(transaction.timestamp) / 1000.0,
                                transaction.transferred_assets.first.amount,
                                transaction.transferred_assets.second.amount,
                                transaction.num_payments, index, partner_peer_id))

        transactions = sorted(transactions, key=lambda x: x[0])

        with open(os.path.join(self.data_dir, 'transactions.log'), 'w') as transactions_file:
            transactions_file.write("time,assets1,assets2,payments,peer1,peer2\n")
            for transaction in transactions:
                transactions_file.write("%s,%s,%s,%s,%s,%s\n" % transaction)

        # Write orders
        with open(os.path.join(self.data_dir, 'orders.log'), 'w') as orders_file:
            orders_file.write("time,id,peer,is_ask,completed,price,quantity,reserved_quantity,traded_quantity,completed_time\n")
            for index, node in enumerate(self.nodes):
                for order in node.overlay.order_manager.order_repository.find_all():
                    order_data = (int(order.timestamp) / 1000.0, order.order_id, index,
                                  'ask' if order.is_ask() else 'bid',
                                  'complete' if order.is_complete() else 'incomplete',
                                  order.assets.first.amount, order.assets.second.amount, order.reserved_quantity,
                                  order.traded_quantity,
                                  (int(order.completed_timestamp) / 1000.0) if order.is_complete() else '-1')
                    orders_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % order_data)

        # Write items in the matching queue
        with open(os.path.join(self.data_dir, 'match_queue.txt'), 'w') as queue_file:
            queue_file.write("peer_id,order_id,retries,price,other_order_id\n")
            for index, node in enumerate(self.nodes):
                for match_cache in node.overlay.get_match_caches():
                    for retries, price, other_order_id, quantity in match_cache.queue.queue:
                        queue_file.write("%d,%s,%d,%s,%s,%d\n" % (index, match_cache.order.order_id, retries, price, other_order_id, quantity))

    async def start_simulation(self, run_yappi=False):
        print("Starting simulation with %d peers..." % self.settings.peers)

        if run_yappi:
            yappi.start(builtins=True)

        start_time = time.time()
        await asyncio.sleep(3894441 + 3600)
        print("Simulation took %f seconds" % (time.time() - start_time))

        self.collect_simulation_results()

        if run_yappi:
            yappi.stop()
            yappi_stats = yappi.get_func_stats()
            yappi_stats.sort("tsub")
            yappi_stats.save("yappi.stats", type='callgrind')

        self.loop.stop()

    async def run(self, yappi=False):
        self.setup_directories()
        self.start_ipv8_nodes()
        self.ipv8_discover_peers()
        if self.settings.scenario_file:
            self.schedule_scenario_actions()

        await self.start_simulation(run_yappi=yappi)
