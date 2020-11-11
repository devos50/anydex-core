from anydex.simulation.scenario_order import Order


class Scenario:

    def __init__(self, scenario_file_path):
        self.scenario_file_path = scenario_file_path
        self.orders = []
        self.unique_users = set()
        self.user_to_index_map = {}

    def parse(self):
        """
        Read the scenario file and schedule the events.
        """
        print("Parsing scenario %s!" % self.scenario_file_path)
        with open(self.scenario_file_path) as scenario_file:
            for scenario_line in scenario_file:
                order = Order.from_line(scenario_line)
                self.orders.append(order)
                self.unique_users.add(order.user_id)
        print("Parsing scenario done! Users: %d" % len(self.unique_users))

        for index, user_id in enumerate(list(self.unique_users)):
            self.user_to_index_map[user_id] = index
