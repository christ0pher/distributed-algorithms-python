import asyncio
from random import randint

import zmq
import zmq.asyncio

from distributed_algorithms.wave_algorithm import Wave

CONTROLLER = "controller"

__author__ = 'christopher@levire.com'

zmq.asyncio.install()

ctx = zmq.asyncio.Context()

_OPERATORS = ["ADD", "MUL", "DIV", "SUB"]


class Controller:

    def __init__(self):
        self.zmocket = ctx.socket(zmq.PUSH)
        self.zmocket.bind("tcp://127.0.0.1:6667")

        self.wave_zmocket = ctx.socket(zmq.ROUTER)
        self.wave_zmocket.setsockopt(zmq.IDENTITY, CONTROLLER.encode())
        self.wave_zmocket.bind("tcp://127.0.0.1:6672")

        self.wave = Wave(self.wave_zmocket, controller=True, name=CONTROLLER)

    async def publish_work(self):

        while True:

            await asyncio.sleep(0.5)
            await self.wave.run_wave()

if __name__ == "__main__":

    controller = Controller()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(controller.publish_work())
    loop.close()

