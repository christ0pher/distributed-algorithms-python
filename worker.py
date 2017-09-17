import asyncio
import json
import sys

import zmq
import zmq.asyncio

from distributed_algorithms.wave_algorithm import Wave

__author__ = 'christopher@levire.com'


zmq.asyncio.install()

ctx = zmq.asyncio.Context()


class Worker:

    def __init__(self, name):
        self.zmocket = ctx.socket(zmq.PULL)
        self.zmocket.connect("tcp://127.0.0.1:6667")

        self.gatherer_zmocket = ctx.socket(zmq.ROUTER)
        self.gatherer_zmocket.connect("tcp://127.0.0.1:6666")
        self.gatherer_zmocket.connect("tcp://127.0.0.1:6665")

        self.wave_zmocket = ctx.socket(zmq.ROUTER)
        self.wave_zmocket.setsockopt(zmq.IDENTITY, name.encode())
        self.wave_zmocket.connect("tcp://127.0.0.1:6670")  # gatherer 1
        self.wave_zmocket.connect("tcp://127.0.0.1:6671")  # gatherer 2
        self.wave_zmocket.connect("tcp://127.0.0.1:6672")  # controller

        self.name = name

        self.wave = Wave(self.wave_zmocket, controller=False, name=self.name)
        # SERVICE DISCOVERY SHIT DOING LOCAL :D NO SOUNDS ;)
        self.wave.update_neighbors([])

    async def pulling_work(self):
        await asyncio.sleep(0.5)
        self.wave_zmocket.send_multipart(["controller".encode(),
                                            json.dumps({"type": "wave_init", "sender": self.name}).encode()])
        while True:
            await asyncio.sleep(0.5)
            await self.wave.run_wave()


if __name__ == "__main__":

    worker_name = sys.argv[1]

    worker = Worker(worker_name)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.pulling_work())
    loop.close()
