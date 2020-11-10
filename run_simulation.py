import argparse
import sys

from simpy import Environment

from anydex.simulation.settings import SimulationSettings
from anydex.simulation.simulation import AnyDexSimulation

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TrustChain simulator')
    parser.add_argument('--peers', '-p', default=2, type=int, help='The number of peers')
    parser.add_argument('--yappi', '-y', action='store_const', default=False, const=True, help='Run the Yappi profiler')

    args = parser.parse_args(sys.argv[1:])
    env = Environment()

    sim_settings = SimulationSettings()
    sim_settings.peers = args.peers
    simulation = AnyDexSimulation(sim_settings, env)
    simulation.run(yappi=args.yappi)

    simulation.loop.run_forever()
