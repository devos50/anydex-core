class MarketSettings(object):
    """
    Object that defines various settings for the market.
    """
    def __init__(self):
        self.ttl = 1
        self.fanout = 20
        self.match_window = 0         # How much time we wait before accepting a specific match
        self.match_send_interval = 0  # How long we should wait with sending a match message (to avoid overloading a peer)
        self.sync_interval = 30       # Synchronization interval
        self.num_order_sync = 10      # How many orders to sync at most
