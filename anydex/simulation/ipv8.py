from anydex.simulation.community import SimulatedMarketCommunity
from anydex.simulation.endpoint import PySimEndpoint
from anydex.simulation.trustchain_memory_database import TrustchainMemoryDatabase
from anydex.trustchain.community import TrustChainCommunity
from anydex.trustchain.settings import TrustChainSettings
from anydex.wallet.dummy_wallet import DummyWallet1, DummyWallet2

from ipv8.keyvault.crypto import default_eccrypto
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network


class SimulatedIPv8(object):

    def __init__(self, sim_settings, data_dir, is_matchmaker, peer_id):
        keypair = default_eccrypto.generate_key("curve25519")
        self.network = Network()

        self.endpoint = PySimEndpoint(sim_settings)
        self.endpoint.open()

        self.my_peer = Peer(keypair, self.endpoint.wan_address)

        database = TrustchainMemoryDatabase()
        settings = TrustChainSettings()
        wallets = {}

        dummy_wallet1 = DummyWallet1()
        wallets[dummy_wallet1.get_identifier()] = dummy_wallet1

        dummy_wallet2 = DummyWallet2()
        wallets[dummy_wallet2.get_identifier()] = dummy_wallet2

        self.trustchain = TrustChainCommunity(self.my_peer, self.endpoint, self.network, persistence=database, settings=settings)
        self.overlay = SimulatedMarketCommunity(self.my_peer, self.endpoint, self.network, use_database=False,
                                                sim_settings=sim_settings, trustchain=self.trustchain,
                                                is_matchmaker=is_matchmaker, wallets=wallets, data_dir=data_dir, peer_id=peer_id)
        self.overlay.settings.match_window = 2
        self.overlay.settings.entrust_limit = sim_settings.entrust_limit
