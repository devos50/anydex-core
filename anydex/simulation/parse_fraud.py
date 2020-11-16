"""
Parse all fraud instances
"""
import os

from anydex.core import CONVERSION_RATES

FRAUD_FILE = "../../data/n_677/fraud.txt"
BTS_USD = 0.01899240

stolen_per_user = {}
stolen_times = {}

if not os.path.exists(FRAUD_FILE):
    print("Fraud file %s does not exist!" % FRAUD_FILE)


total_fraud = 0
with open(FRAUD_FILE, "r") as fraud_file:
    for line in fraud_file.readlines():
        if not line:
            continue

        parts = line.strip().split(",")
        peer = parts[0]
        fraud_time = float(parts[1])
        amount = int(parts[2])
        asset = parts[3]

        if asset not in stolen_times:
            stolen_times[asset] = 0
        stolen_times[asset] += 1

        precision = CONVERSION_RATES[asset][0]
        usd_price = CONVERSION_RATES[asset][1]
        asset_fraud = amount / 10 ** precision * usd_price
        total_fraud += asset_fraud

        if peer not in stolen_per_user:
            stolen_per_user[peer] = 0
        stolen_per_user[peer] += asset_fraud

for asset in stolen_times:
    if asset not in CONVERSION_RATES:
        print("Asset %s not in asset info! (stolen: %d)" % (asset, stolen_times[asset]))
        continue

print("Total fraud: %f" % total_fraud)

with open("fraud_per_user.csv", "w") as fraud_file:
    fraud_file.write("peer,stolen\n")
    max_stolen_per_user = 0
    for peer, stolen_value in stolen_per_user.items():
        print("%s => %f" % (peer, stolen_value))
        fraud_file.write("%s,%f\n" % (peer, stolen_value))
        if stolen_value > max_stolen_per_user:
            max_stolen_per_user = stolen_value

print("Max per user: %f" % max_stolen_per_user)
