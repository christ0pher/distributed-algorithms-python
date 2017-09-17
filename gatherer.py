import asyncio
import sys

import zmq
import zmq.asyncio

from distributed_algorithms.wave_algorithm import Wave

__author__ = 'christopher@levire.com'


zmq.asyncio.install()

ctx = zmq.asyncio.Context()


class Gatherer:

    def __init__(self, name, port, wave_port):
        self.zmocket = ctx.socket(zmq.ROUTER)
        self.zmocket.setsockopt(zmq.IDENTITY, name.encode())
        self.zmocket.bind("tcp://127.0.0.1:"+str(port))

        self.wave_zmocket = ctx.socket(zmq.ROUTER)
        self.wave_zmocket.setsockopt(zmq.IDENTITY, name.encode())
        self.wave_zmocket.bind("tcp://127.0.0.1:"+str(wave_port))

        self.name = name
        self.wave = Wave(self.wave_zmocket, controller=False, name=self.name)

    async def gather_results(self):
        while True:
            await asyncio.sleep(0.5)
            await self.wave.run_wave()


if __name__ == "__main__":

    port = sys.argv[1]
    name = sys.argv[2]
    wave_port = sys.argv[3]

    gatherer = Gatherer(name, port, wave_port)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(gatherer.gather_results())
    loop.close()
