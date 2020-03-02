import logging

from anydex.core.order import Order
from anydex.core.order_repository import OrderRepository
from anydex.core.timestamp import Timestamp


class OrderManager(object):
    """Provides an interface to the user to manage the users orders"""

    def __init__(self, order_repository):
        """
        :type order_repository: OrderRepository
        """
        super(OrderManager, self).__init__()
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info("Market order manager initialized")

        self.order_repository = order_repository

    def create_ride_offer(self, latitude, longitude, timeout):
        """
        Create an ask order (sell order)

        :param latitude: The latitude of the taxi driver
        :param longitude: The longitude of the taxi driver
        :param timeout: The timeout of the order, when does the order need to be timed out
        :type latitude: float
        :type longitude: float
        :type timeout: Timeout
        :return: The order that is created
        :rtype: Order
        """
        order = Order(self.order_repository.next_identity(), latitude, longitude, timeout, Timestamp.now(), True)
        self.order_repository.add(order)

        self._logger.info("Ride offer created with id: " + str(order.order_id))

        return order

    def create_ride_request(self, latitude, longitude, timeout):
        """
        Create a bid order (buy order)

        :param latitude: The latitude of the taxi requester
        :param longitude: The longitude of the taxi requester
        :param timeout: The timeout of the order, when does the order need to be timed out
        :type latitude: float
        :type longitude: float
        :type timeout: Timeout
        :return: The order that is created
        :rtype: Order
        """
        order = Order(self.order_repository.next_identity(), latitude, longitude, timeout, Timestamp.now(), False)
        self.order_repository.add(order)

        self._logger.info("Bid order created with id: " + str(order.order_id))

        return order

    def cancel_order(self, order_id):
        """
        Cancel an order that was created by the user.
        :return: The order that is created
        :rtype: Order
        """
        order = self.order_repository.find_by_id(order_id)

        if order:
            order.cancel()
            self.order_repository.update(order)

        self._logger.info("Order cancelled with id: " + str(order_id))
