from __future__ import absolute_import

from ipv8.REST.base_endpoint import BaseEndpoint
from ipv8.REST.root_endpoint import RootEndpoint as IPv8RootEndpoint

from anydex.restapi.asks_bids_endpoint import AsksEndpoint, BidsEndpoint
from anydex.restapi.base_market_endpoint import BaseMarketEndpoint
from anydex.restapi.matchmakers_endpoint import MatchmakersEndpoint
from anydex.restapi.orders_endpoint import OrdersEndpoint
from anydex.restapi.state_endpoint import StateEndpoint
from anydex.restapi.transactions_endpoint import TransactionsEndpoint


class RootEndpoint(BaseEndpoint):
    """
    The root endpoint of the HTTP API is the root resource in the request tree.
    It will dispatch requests regarding torrents, channels, settings etc to the right child endpoint.
    """

    def __init__(self, session):
        """
        During the initialization of the REST API, we only start the event sockets and the state endpoint.
        We enable the other endpoints after completing the starting procedure.
        """
        super(RootEndpoint, self).__init__()
        self.session = session

        child_handler_dict = {
            b"asks": AsksEndpoint,
            b"bids": BidsEndpoint,
            b"transactions": TransactionsEndpoint,
            b"orders": OrdersEndpoint,
            b"matchmakers": MatchmakersEndpoint,
            b"state": StateEndpoint,
            b"ipv8": IPv8RootEndpoint,
            b"connect": ConnectEndpoint,
        }
        for path, child_cls in child_handler_dict.items():
            self.putChild(path, child_cls(self.session))


class ConnectEndpoint(BaseMarketEndpoint):

    def render_GET(self, request):
        print(request.args)
        ip_addr = request.args[b"ip"][0]
        port = int(request.args[b"port"][0])

        market_community = self.get_market_community()
        market_community.create_introduction_request((ip_addr, port))
        market_community.trustchain.create_introduction_request((ip_addr, port))
        return b''