from anydex.wallet.dummy_wallet import BaseDummyWallet


class SimulationWallet(BaseDummyWallet):

    def __init__(self, name):
        super().__init__()
        self.name = name
        self.balance = 10000000000000

    def get_name(self):
        return self.name

    def get_identifier(self):
        return self.name

    def precision(self):
        return 1
