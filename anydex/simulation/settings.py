from dataclasses import dataclass


@dataclass
class SimulationSettings:
    peers = 100
    matchmakers = 1
    max_duration = 60
    scenario_file = None
