from dataclasses import dataclass


@dataclass
class SimulationSettings:
    peers = 100
    matchmakers = 1
    max_duration = 60
    entrust_limit = -1
    scenario_file = None
