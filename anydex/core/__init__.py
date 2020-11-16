class DeclinedTradeReason(object):
    ORDER_COMPLETED = 0
    ORDER_EXPIRED = 1
    ORDER_RESERVED = 2
    ORDER_INVALID = 3
    ORDER_CANCELLED = 4
    UNACCEPTABLE_PRICE = 5
    ADDRESS_LOOKUP_FAIL = 6
    NO_AVAILABLE_QUANTITY = 7
    ALREADY_TRADING = 8
    OTHER = 9


class DeclineMatchReason(object):
    ORDER_COMPLETED = 0
    OTHER_ORDER_COMPLETED = 1
    OTHER_ORDER_CANCELLED = 2
    OTHER = 3


MAX_ORDER_TIMEOUT = 3600 * 24 * 365 * 10

VERSION = 0.1


# Conversion rates
CONVERSION_RATES = {
    "MB": (1, 1),
    "DUM1": (0, 1),
    "DUM2": (0, 1),
}