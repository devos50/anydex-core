import random
from asyncio import Queue, ensure_future, sleep

from ipv8.test.mocking.endpoint import AutoMockEndpoint, internet


class PySimEndpoint(AutoMockEndpoint):

    def __init__(self, settings):
        super().__init__()
        self.msg_queue = Queue()
        self.bytes_up = 0
        self.bytes_down = 0
        self.settings = settings
        self.send_fail_probability = 0
        self.overlay = None
        ensure_future(self.process_messages())

    async def process_messages(self):
        while True:
            from_address, packet = await self.msg_queue.get()
            self.bytes_down += len(packet)
            self.notify_listeners((from_address, packet))

    async def delayed_send(self, delay, socket_address, packet):
        await sleep(delay)
        if socket_address in internet:
            # For the unit tests we handle messages in separate asyncio tasks to prevent infinite recursion.
            ep = internet[socket_address]
            self.bytes_up += len(packet)
            if random.random() > self.send_fail_probability:
                ep.msg_queue.put_nowait((self.wan_address, packet))
        else:
            raise AssertionError("Received data from unregistered address %s" % repr(socket_address))

    def send(self, socket_address, packet):
        delay = random.random() * 0.1
        ensure_future(self.delayed_send(delay, socket_address, packet))
