import zmq.asyncio
import zmq
import asyncio

__author__ = 'christopher@levire.com'


zmq.asyncio.install()

ctx = zmq.asyncio.Context()


class Gatherer:

    def __init__(self):
        self.zmocket = ctx.socket(zmq.ROUTER)
        self.zmocket.setsockopt(zmq.IDENTITY, "gatherer1".encode())
        self.zmocket.bind("tcp://127.0.0.1:6666")

    async def gather_results(self):
        i = 0
        while True:
            try:
                work_unit = await self.zmocket.recv_json(flags=zmq.NOBLOCK)
                i += 1
                print("Received workunit: "+str(i) + "Unit: "+str(work_unit))
            except:
                print("Nothing to gather")
                await asyncio.sleep(0.5)

if __name__ == "__main__":

    gatherer = Gatherer()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(gatherer.gather_results())
    loop.close()
