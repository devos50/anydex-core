import os
from binascii import hexlify

from anydex.core.community import MarketCommunity
from anydex.simulation.logger import setup_logger


class SimulatedMarketCommunity(MarketCommunity):

    def __init__(self, *args, **kwargs):
        self.sim_settings = kwargs.pop('sim_settings')
        self.data_dir = kwargs.pop('data_dir', None)
        MarketCommunity.__init__(self, *args, **kwargs)

        if self.data_dir:
            logs_dir = os.path.join(self.data_dir, "logs")
            os.makedirs(logs_dir, exist_ok=True)
            peer_mid = hexlify(self.mid).decode()[-8:]
            log_file = os.path.join(logs_dir, "%s.log" % peer_mid)
            self.trustchain.logger = setup_logger(peer_mid, self.trustchain.__class__.__name__, log_file)
            self.logger = setup_logger(peer_mid, self.__class__.__name__, log_file)
            for policy in self.clearing_policies:
                policy.logger = self.logger

    def get_ipv8_address(self):
        return self.endpoint.wan_address
