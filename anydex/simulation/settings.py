from dataclasses import dataclass


@dataclass
class SimulationSettings:
    peers = 100
    matchmakers = 1
    max_duration = 60
    entrust_limit = 100
    scenario_file = None

    # 0 = no risk mitigation strategy and no incremental settlement
    # 1 = incremental settlement (2)
    # 2 = restrict, no incremental settlement
    # 3 = restrict + incremental settlement
    strategy = 0
