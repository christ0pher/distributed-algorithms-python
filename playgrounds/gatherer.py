import zmq.asyncio
import zmq
import asyncio
import sys

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

    async def gather_results(self):
        i = 0
        while True:
            try:
                work_unit = await self.zmocket.recv_json(flags=zmq.NOBLOCK)
                i += 1
                print("Received workunit: "+str(i) + "Unit: "+str(work_unit))
            except:
                print("Nothing to gather")
                await asyncio.sleep(0.1)

if __name__ == "__main__":

    port = sys.argv[1]
    name = sys.argv[2]
    wave_port = sys.argv[3]

    gatherer = Gatherer(name, port, wave_port)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(gatherer.gather_results())
    loop.close()
