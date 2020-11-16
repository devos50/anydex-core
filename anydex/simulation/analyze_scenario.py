from anydex.core import CONVERSION_RATES
from anydex.simulation.scenario_order import Order

SCENARIO_FILE = "../../data/scenario.txt"
missing = {}

with open(SCENARIO_FILE) as scenario_file:
    for scenario_line in scenario_file:
        order = Order.from_line(scenario_line)
        if order.type != "ask" and order.type != "bid":
            continue

        if order.asset1_type not in CONVERSION_RATES:
            if order.asset1_type not in missing:
                missing[order.asset1_type] = 0
            missing[order.asset1_type] += 1

        if order.asset2_type not in CONVERSION_RATES:
            if order.asset2_type not in missing:
                missing[order.asset2_type] = 0
            missing[order.asset2_type] += 1


for asset_id, times in missing.items():
    print("Asset %s, missing %d times" % (asset_id, times))
