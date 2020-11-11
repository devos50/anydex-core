class Order(object):
    """
    Class that represents an order.
    """
    def __init__(self, height, id, type, asset1_amount, asset1_type, asset2_amount, asset2_type, user_id, timeout, timestamp):
        self.height = height
        self.id = id
        self.type = type
        self.asset1_amount = asset1_amount
        self.asset1_type = asset1_type
        self.asset2_amount = asset2_amount
        self.asset2_type = asset2_type
        self.user_id = user_id
        self.timeout = timeout
        self.timestamp = timestamp

    @classmethod
    def from_line(cls, line):
        parts = line.split(',')
        if len(parts) != 10:
            return None

        height = int(parts[0])
        id = parts[1]
        type = parts[2]
        asset1_amount = int(parts[3])
        asset1_type = parts[4]
        asset2_amount = int(parts[5])
        asset2_type = parts[6]
        user_id = parts[7]
        timeout = int(parts[8])
        timestamp = int(parts[9])

        return Order(height, id, type, asset1_amount, asset1_type, asset2_amount, asset2_type, user_id, timeout, timestamp)
