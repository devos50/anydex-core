import argparse
import sys
from asyncio import ensure_future

from anydex.simulation.settings import SimulationSettings
from anydex.simulation.simulation import AnyDexSimulation

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TrustChain simulator')
    parser.add_argument('--peers', '-p', default=3, type=int, help='The number of peers')
    parser.add_argument('--scenario', '-s', default=None, type=str, help='The scenario file to run')
    parser.add_argument('--yappi', '-y', action='store_const', default=False, const=True, help='Run the Yappi profiler')

    args = parser.parse_args(sys.argv[1:])

    sim_settings = SimulationSettings()
    sim_settings.peers = args.peers
    sim_settings.scenario_file = "data/scenario.txt"
    simulation = AnyDexSimulation(sim_settings)
    ensure_future(simulation.run(yappi=args.yappi))

    simulation.loop.run_forever()
