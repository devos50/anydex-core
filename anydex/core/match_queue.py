from functools import cmp_to_key


class MatchPriorityQueue(object):
    """
    This priority queue keeps track of incoming match message for a specific order.
    """
    def __init__(self, order):
        self.order = order
        self.queue = []

    def __str__(self):
        return ' '.join([str(i) for i in self.queue])

    def is_empty(self):
        return len(self.queue) == []

    def contains_order(self, order_id):
        for _, _, other_order_id in self.queue:
            if other_order_id == order_id:
                return True
        return False

    def insert(self, retries, distance, order_id):
        self.queue.append((retries, distance, order_id))
        self.queue = sorted(self.queue)

    def delete(self):
        if not self.queue:
            return None

        return self.queue.pop(0)
