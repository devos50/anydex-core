"""
Parse all fraud instances
"""
import os

from anydex.core import CONVERSION_RATES

USERS = 1162
BTS_USD = 0.01899240

fraud_file = open("gained_per_user.csv", "w")
fraud_file.write("strategy,peer_id,peer,stolen\n")

losses_file = open("losses_per_user.csv", "w")
losses_file.write("strategy,peer_id,peer,lost\n")

for strategy in [0, 1, 2, 3]:
    total_fraud = 0
    strategy_str = "none"
    if strategy == 1:
        strategy_str = "INC_SET(2)"
    elif strategy == 2:
        strategy_str = "RESTRICT"
    elif strategy == 3:
        strategy_str = "RESTRICT+INC_SET"

    stolen_per_user = {}
    user_losses = {}

    fraud_file_path = "../../data/n_%d_s_%d/fraud.txt" % (USERS, strategy)
    if not os.path.exists(fraud_file_path):
        print("Skipping analysis of strategy %d, files not found!" % strategy)
        continue

    with open(fraud_file_path, "r") as input_fraud_file:
        for line in input_fraud_file.readlines():
            if not line:
                continue

            parts = line.strip().split(",")
            peer = parts[0]
            scammed_peer = parts[1]
            fraud_time = float(parts[2])
            amount = int(parts[3])
            asset = parts[4]

            precision = CONVERSION_RATES[asset][0]
            usd_price = CONVERSION_RATES[asset][1]
            asset_fraud = amount / 10 ** precision * usd_price
            total_fraud += asset_fraud

            if peer not in stolen_per_user:
                stolen_per_user[peer] = 0
            stolen_per_user[peer] += asset_fraud

            if scammed_peer not in user_losses:
                user_losses[scammed_peer] = 0
            user_losses[scammed_peer] += asset_fraud

    # Write the fraud gains
    frauds_sorted = []
    for peer, stolen_value in stolen_per_user.items():
        frauds_sorted.append((peer, stolen_value))
    frauds_sorted = sorted(frauds_sorted, key=lambda x: x[1], reverse=True)

    for index, info in enumerate(frauds_sorted):
        peer, stolen_value = info
        fraud_file.write("%s,%d,%s,%f\n" % (strategy_str, index + 1, peer, stolen_value))

    # Write the losses
    losses_sorted = []
    for peer, lost_value in user_losses.items():
        losses_sorted.append((peer, lost_value))
    losses_sorted = sorted(losses_sorted, key=lambda x: x[1], reverse=True)

    for index, info in enumerate(losses_sorted):
        peer, lost_value = info
        losses_file.write("%s,%d,%s,%f\n" % (strategy_str, index + 1, peer, lost_value))

    print("Total fraud in strategy %d: %f" % (strategy, total_fraud))

fraud_file.close()
losses_file.close()
