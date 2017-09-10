import json
import sys

import zmq.asyncio
import zmq
import asyncio

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

    async def pulling_work(self):
        while True:
            try:
                work_unit = await self.zmocket.recv_json(flags=zmq.NOBLOCK)
                print("Worker-"+self.name+" received work: "+str(work_unit))
                operator = work_unit["operator"]
                result = 0
                if operator == "ADD":
                    result = work_unit["operands"][0] + work_unit["operands"][1]
                elif operator == "MUL":
                    result = work_unit["operands"][0] * work_unit["operands"][1]
                elif operator == "DIV":
                    result = int(work_unit["operands"][0] / work_unit["operands"][1])
                else:
                    result = work_unit["operands"][0] - work_unit["operands"][1]

                gatherer = "gatherer_odd"
                if result % 2 == 0:
                    gatherer = "gatherer_even"

                await self.gatherer_zmocket.send_multipart([gatherer.encode(), json.dumps({"result": result,
                                                   "work_unit": work_unit,
                                                   "worker": self.name}).encode()])
            except:
                print("No work there: "+self.name)
                await asyncio.sleep(0.1)


if __name__ == "__main__":

    worker_name = sys.argv[1]

    worker = Worker(worker_name)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.pulling_work())
    loop.close()
