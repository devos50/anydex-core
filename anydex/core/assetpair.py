# pylint: disable=long-builtin,redefined-builtin

from anydex.core.assetamount import AssetAmount
from anydex.core.price import Price

try:
    long
except NameError:
    long = int


class AssetPair(object):
    """
    An asset pair represents a pair of specific amounts of assets, i.e. 10 BTC - 20 MB.
    It is used when dealing with orders in the market.
    """

    def __init__(self, first, second):
        if first.asset_id > second.asset_id:
            raise ValueError("Asset %s must be smaller than %s" % (first, second))

        self.first = first
        self.second = second

    def __eq__(self, other):
        if not isinstance(other, AssetPair):
            return NotImplemented
        else:
            return self.first == other.first and self.second == other.second

    def to_dictionary(self):
        return {
            "first": self.first.to_dictionary(),
            "second": self.second.to_dictionary()
        }

    @classmethod
    def from_dictionary(cls, dictionary):
        return cls(AssetAmount(dictionary["first"]["amount"], dictionary["first"]["type"]),
                   AssetAmount(dictionary["second"]["amount"], dictionary["second"]["type"]))

    @property
    def price(self):
        """
        Return a Price object of this asset pair, which expresses the second asset into the first asset.
        """
        return Price(self.second.amount, self.first.amount, self.second.asset_id, self.first.asset_id)

    @staticmethod
    def from_price(price, first_amount):
        return AssetPair(AssetAmount(first_amount, price.denom_type), AssetAmount(int(price.frac * first_amount), price.num_type))

    def proportional_downscale(self, first=None, second=None):
        """
        This method constructs a new AssetPair where the ratio between the first/second asset is preserved.
        One should specify a new amount for the first asset.
        For instance, if we have an asset pair (4 BTC, 8 MB), the price is 8/4 = 2 MB/BTC.
        If we now change the amount of the first asset from 4 BTC to 1 BTC, the new AssetPair becomes (1 BTC, 2 MB).
        Likewise, if the second asset is changed to 4, the new AssetPair becomes (2 BTC, 4 MB)
        """
        if first:
            return AssetPair(AssetAmount(first, self.first.asset_id),
                             AssetAmount(long(self.price.amount * first), self.second.asset_id))
        elif second:
            return AssetPair(AssetAmount(long(second / self.price.amount), self.first.asset_id),
                             AssetAmount(second, self.second.asset_id))
        else:
            raise ValueError("No first/second provided in proportional downscale!")

    def __str__(self):
        return "%s %s" % (self.first, self.second)
