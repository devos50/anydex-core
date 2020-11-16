import glob

from anydex.core import CONVERSION_RATES
from anydex.simulation.scenario_order import Order

SCENARIOS_DIR = "../../data/scenarios/"
missing = {}
asset_ids = set()

for scenario_path in glob.iglob(SCENARIOS_DIR + "*.txt"):
    with open(scenario_path) as scenario_file:
        for scenario_line in scenario_file:
            order = Order.from_line(scenario_line)
            if order.type != "ask" and order.type != "bid":
                continue

            asset_ids.add(order.asset1_type)
            asset_ids.add(order.asset2_type)

            if order.asset1_type not in CONVERSION_RATES:
                if order.asset1_type not in missing:
                    missing[order.asset1_type] = 0
                missing[order.asset1_type] += 1

            if order.asset2_type not in CONVERSION_RATES:
                if order.asset2_type not in missing:
                    missing[order.asset2_type] = 0
                missing[order.asset2_type] += 1

    print("Parsed scenario %s!" % scenario_path)

missing_list = []
for asset_id, times in missing.items():
    missing_list.append((asset_id, times))
missing_list = sorted(missing_list, key=lambda item: item[1])

print("Unique assets: %d" % len(asset_ids))
print("Missing assets: %d" % len(missing))

with open("missing_assets.txt", "w") as missing_assets_file:
    for asset_id, times in missing_list:
        missing_assets_file.write("%s\n" % asset_id)
        print("Asset %s, missing %d times" % (asset_id, times))
