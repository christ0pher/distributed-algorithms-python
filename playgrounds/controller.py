from random import randint

import zmq.asyncio
import zmq
import asyncio

__author__ = 'christopher@levire.com'

zmq.asyncio.install()

ctx = zmq.asyncio.Context()

_OPERATORS = ["ADD", "MUL", "DIV", "SUB"]


class Controller:

    def __init__(self):
        self.zmocket = ctx.socket(zmq.PUSH)
        self.zmocket.bind("tcp://127.0.0.1:6667")

        self.wave_zmocket = ctx.socket(zmq.ROUTER)
        self.wave_zmocket.setsockopt(zmq.IDENTITY, "controller".encode())
        self.wave_zmocket.bind("tcp://127.0.0.1:6672")

    async def publish_work(self):
        i = 0
        while True:
            work_unit = {
                "operator": _OPERATORS[randint(0, 3)],
                "operands": [randint(1, 10), randint(1, 3)]
            }
            await self.zmocket.send_json(work_unit)

            print("Work sent to workers "+str(i)+": "+str(work_unit))

            await asyncio.sleep(0.1)
            i += 1

if __name__ == "__main__":

    controller = Controller()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(controller.publish_work())
    loop.close()

