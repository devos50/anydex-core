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

BTS_USD = 0.01899240

# Conversion rates
CONVERSION_RATES = {
    "MB": (1, 1),
    "DUM1": (0, 1),
    "DUM2": (0, 1),
    "1.3.0": (5, BTS_USD),
    "1.3.121": (4, 1),
    "1.3.113": (4, 4.46 * BTS_USD),
    "1.3.350": (8, 400000 * BTS_USD),
    "1.3.103": (8, 113634 * BTS_USD),
    "1.3.422": (6, 0.0035 * BTS_USD),
    "1.3.105": (4, 79.8 * BTS_USD),
    "1.3.136": (0, 0.1 * BTS_USD),
    "1.3.590": (4, 0.0002 * BTS_USD),
    "1.3.562": (4, 0.721 * BTS_USD),
    "1.3.569": (8, 142852 * BTS_USD),
    "1.3.541": (4, 37 * BTS_USD),
    "1.3.120": (4, 75.7 * BTS_USD),
    "1.3.472": (6, 9000 * BTS_USD),
    "1.3.599": (4, 0.55 * BTS_USD),
    "1.3.578": (8, 0.003 * BTS_USD),
    "1.3.106": (6, 9622 * BTS_USD),
    "1.3.666": (4, 10 * BTS_USD),
    "1.3.138": (2, 0.00000001 * BTS_USD),
    "1.3.660": (1, 0.25 * BTS_USD),
    "1.3.577": (8, 600 * BTS_USD),
    "1.3.397": (6, 0.0748 * BTS_USD),
    "1.3.658": (4, 0.01 * BTS_USD),
    "1.3.616": (6, 1000 * BTS_USD),
    "1.3.556": (0, 49 * BTS_USD),
    "1.3.614": (6, 1 * BTS_USD),
    "1.3.861": (8, 447501 * BTS_USD),
    "1.3.552": (2, 0.048 * BTS_USD),
    "1.3.592": (8, 104000 * BTS_USD)
}