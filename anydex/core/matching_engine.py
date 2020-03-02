import logging
from abc import ABCMeta, abstractmethod
from math import radians, sin, cos, asin, sqrt
from time import time


class MatchingStrategy(object):
    """Matching strategy base class"""
    __metaclass__ = ABCMeta

    def __init__(self, order_book):
        """
        :param order_book: The order book to search in
        :type order_book: OrderBook
        """
        super(MatchingStrategy, self).__init__()
        self._logger = logging.getLogger(self.__class__.__name__)

        self.order_book = order_book

    @abstractmethod
    def match(self, order_id, latitude, longitude, is_ask):
        """
        :param order_id: The order id of the tick to match
        :param latitude: The latitude of the tick
        :param longitude: The longitude of the tick
        :param is_ask: Whether the object we want to match is an ask
        :type order_id: OrderId
        :type latitude: float
        :type longitude: float
        :type is_ask: Bool
        :return: A list of tuples containing the ticks and the matched quantity
        :rtype: [(str, TickEntry)]
        """
        return


class TaxiStrategy(MatchingStrategy):
    """
    Matching strategy based on euclidean distance
    """

    def haversine(self, lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 6371 # Radius of earth in kilometers. Use 3956 for miles
        return c * r

    def match(self, order_id, latitude, longitude, is_ask):
        if latitude < -90 or latitude > 90 or longitude < -180 or longitude > 180:
            return []

        return self.match_ask(latitude, longitude, order_id) if is_ask else self.match_bid(latitude, longitude, order_id)

    def match_ask(self, latitude, longitude, order_id):
        if not self.order_book._bids:
            return []

        min_tick = None
        min_distance = 100000000
        for tick_entry in self.order_book._bids.values():
            if tick_entry.is_blocked_for_matching(order_id):
                continue

            distance = self.haversine(longitude, latitude, tick_entry.tick.longitude, tick_entry.tick.latitude)
            if distance < min_distance:
                min_tick = tick_entry
                min_distance = distance

        if min_tick:
            return [min_tick]
        return []

    def match_bid(self, latitude, longitude, order_id):
        if not self.order_book._asks:
            return []

        min_tick = None
        min_distance = 100000000
        for tick_entry in self.order_book._asks.values():
            if tick_entry.is_blocked_for_matching(order_id):
                continue

            distance = self.haversine(longitude, latitude, tick_entry.tick.longitude, tick_entry.tick.latitude)
            if distance < min_distance:
                min_tick = tick_entry
                min_distance = distance

        if min_tick:
            return [min_tick]
        return []


class PriceTimeStrategy(MatchingStrategy):
    """Strategy that uses the price time method for picking ticks"""

    def match(self, order_id, price, quantity, is_ask):
        """
        :param order_id: The order id of the tick to match
        :param price: The price to match against
        :param quantity: The quantity that should be matched
        :param is_ask: Whether the object we want to match is an ask
        :type order_id: OrderId
        :type price: Price
        :type quantity: int
        :type is_ask: Bool
        :return: A list of tuples containing the ticks and the matched quantity
        :rtype: [(str, TickEntry, Quantity)]
        """
        matched_ticks = []
        quantity_to_match = quantity

        # First check whether we can match our order at all in the order book
        if is_ask:
            bid_price = self.order_book.get_bid_price(price.num_type, price.denom_type)
            if not bid_price or price > bid_price:
                return []
        if not is_ask:
            ask_price = self.order_book.get_ask_price(price.num_type, price.denom_type)
            if not ask_price or price < ask_price:
                return []

        # Next, check whether we have a price level we can start our match search from
        if is_ask:
            price_level = self.order_book.get_bid_price_level(price.num_type, price.denom_type)
        else:
            price_level = self.order_book.get_ask_price_level(price.num_type, price.denom_type)

        if not price_level:
            return []

        cur_tick_entry = price_level.first_tick
        cur_price_level_price = price_level.price

        # We now start to iterate through price levels and tick entries and match on the fly
        while cur_tick_entry and quantity_to_match > 0:
            if cur_tick_entry.is_blocked_for_matching(order_id) or \
                            order_id.trader_id == cur_tick_entry.order_id.trader_id:
                cur_tick_entry = cur_tick_entry.next_tick
                continue

            quantity_matched = min(quantity_to_match, cur_tick_entry.available_for_matching)
            if quantity_matched > 0:
                matched_ticks.append(cur_tick_entry)
                quantity_to_match -= quantity_matched

            cur_tick_entry = cur_tick_entry.next_tick
            if not cur_tick_entry:
                # We probably reached the end of a price level, check whether we have a next price level
                try:
                    # Get the next price level
                    if is_ask:
                        next_price_level = self.order_book.bids.\
                            get_price_level_list(price.num_type, price.denom_type).prev_item(cur_price_level_price)
                    else:
                        next_price_level = self.order_book.asks.\
                            get_price_level_list(price.num_type, price.denom_type).succ_item(cur_price_level_price)
                    cur_price_level_price = next_price_level.price
                except IndexError:
                    break

                if (is_ask and price > cur_price_level_price) or (not is_ask and price < cur_price_level_price):
                    # The price of this price level is too high/low
                    break

                cur_tick_entry = next_price_level.first_tick

        return matched_ticks


class MatchingEngine(object):
    """Matches ticks and orders to the order book"""

    def __init__(self, matching_strategy):
        """
        :param matching_strategy: The strategy to use
        :type matching_strategy: MatchingStrategy
        """
        super(MatchingEngine, self).__init__()
        self._logger = logging.getLogger(self.__class__.__name__)

        self.matching_strategy = matching_strategy

    def match(self, tick_entry):
        """
        :param tick_entry: The TickEntry that should be matched
        :type tick_entry: TickEntry
        :return: A list of tuples containing a random match id, ticks and the matched quantity
        :rtype: [(str, TickEntry)]
        """
        now = time()

        matched_ticks = self.matching_strategy.match(tick_entry.order_id,
                                                     tick_entry.latitude,
                                                     tick_entry.longitude,
                                                     tick_entry.tick.is_ask())

        diff = time() - now
        self._logger.debug("Matching engine completed in %.2f seconds", diff)
        return matched_ticks
