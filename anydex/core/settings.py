class MarketSettings(object):
    """
    Object that defines various settings for the market.
    """
    def __init__(self):
        self.ttl = 1
        self.fanout = 20
        self.match_window = 0           # How much time we wait before accepting a specific match
        self.match_send_interval = 0    # How long we should wait with sending a match message (to avoid overloading a peer)
        self.num_order_sync = 10        # How many orders to sync at most
        self.match_process_batch_size = 20  # How many match items we process in one batch
        self.entrust_limit = -1        # How much money we entrust to others (in USD)
        self.max_payments_per_trade = 20  # The maximum number of payments we allow per trade
