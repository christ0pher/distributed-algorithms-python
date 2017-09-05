import json

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
                await self.gatherer_zmocket.send_multipart(["gatherer1".encode(), json.dumps({"result": result,
                                                       "work_unit": work_unit,
                                                       "worker": self.name}).encode()])
            except:
                print("No work there: "+self.name)
                await asyncio.sleep(0.5)


if __name__ == "__main__":

    worker_ports = [42421, 42422, 42423]
    workers = []
    for worker_port in worker_ports:
        worker = Worker("worker-"+str(worker_port))
        workers.append(worker)

    for worker in workers:
        asyncio.ensure_future(worker.pulling_work())

    loop = asyncio.get_event_loop()
    loop.run_forever()
    loop.close()