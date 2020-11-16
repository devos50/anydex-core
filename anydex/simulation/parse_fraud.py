"""
Parse all fraud instances
"""
import os

FRAUD_FILE = "../../data/n_3/fraud.txt"
BTS_USD = 0.01899240

asset_info = {  # (precision, USD price)
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
    "1.3.861": (8, 447501 * BTS_USD)
}
stolen = {}
stolen_times = {}

if not os.path.exists(FRAUD_FILE):
    print("Fraud file %s does not exist!" % FRAUD_FILE)


with open(FRAUD_FILE, "r") as fraud_file:
    for line in fraud_file.readlines():
        if not line:
            continue

        parts = line.strip().split(",")
        peer = parts[0]
        fraud_time = float(parts[1])
        amount = int(parts[2])
        asset = parts[3]

        if asset not in stolen:
            stolen[asset] = 0
        if asset not in stolen_times:
            stolen_times[asset] = 0
        stolen[asset] += amount
        stolen_times[asset] += 1

# Determine total money lost
fraud = 0
for asset in stolen:
    if asset not in asset_info:
        print("Asset %s not in asset info! (stolen: %d)" % (asset, stolen_times[asset]))
        continue
    precision = asset_info[asset][0]
    usd_price = asset_info[asset][1]
    asset_fraud = stolen[asset] / 10**precision * usd_price
    fraud += asset_fraud

print(fraud)
