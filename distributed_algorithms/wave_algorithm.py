import datetime
import json

__author__ = 'christopher@levire.com'

import zmq


# Controller sends wave message
'''{
    "sender": "process",
    "type": ["wave_request", "wave_response", "wave_init"],
    "parent": "process",
    "responses": ["responses"]  # only add responses when name==parent
}'''
# relay wave message to neighbors
# gather results from all neighbors
# if all results gathered, send response to parent


class Wave:

    def __init__(self, wave_zmocket, controller, name):

        self.wave_zmocket = wave_zmocket
        self.neighbors = []
        self.controller = controller
        self.wave_request = {}
        self.wave_responses = []
        self.last_wave = datetime.datetime.utcnow()
        self.parent = None
        self.name = name
        self.wave_init = False

    async def run_wave(self):

        try:
            msg = await self.wave_zmocket.recv_json(flags=zmq.NOBLOCK)
            send_msg, receiver, msg = self.handle_wave_message(msg)
            if send_msg:
                await self.wave_zmocket.send_multipart([receiver.encode(),
                                                        json.dumps(msg).encode()])
        except Exception as e:
            print("Error: "+str(e))

        if datetime.datetime.utcnow() - self.last_wave > datetime.timedelta(seconds=3) \
                and self.controller:
            self.init_wave()
            try:
                self.send_wave_request()
            except Exception as e:
                print("Error sending: "+str(e))

    def handle_wave_message(self, msg):
        sender = msg["sender"]

        if msg["type"] == "wave_init":
            self.neighbors.append(sender)
        if msg["type"] == "wave_response":
            self.wave_request[sender] = True
            self.wave_responses.append(msg)

            all_neighbors = True
            for neighbor in self.neighbors:
                all_neighbors &= self.wave_request[neighbor]
            if all_neighbors and not self.controller:
                response = {
                    "sender": self.name,
                    "type": "wave_response",
                    "parent": self.parent,
                    "responses": self.wave_responses
                }
                return True, self.parent, response

            elif all_neighbors and self.controller:
                print("WAVE IS DONE")
                print(json.dumps(self.wave_responses, indent=4, sort_keys=True))
        if msg["type"] == "wave_request":
            self.init_wave()
            self.parent = sender
            if len(self.neighbors) != 0:
                self.send_wave_request()
            if len(self.neighbors) == 0:
                response = {
                    "sender": self.name,
                    "type": "wave_response",
                    "parent": self.parent,
                    "responses": self.wave_responses
                }
                return True, self.parent, response

        return False, _, _

    def send_wave_request(self):
        for neighbor in self.neighbors:
            request = {
                "type": "wave_request",
                "sender": self.name
            }
            self.wave_zmocket.send_multipart([neighbor.encode(),
                                                    json.dumps(request).encode()])

    def init_wave(self):
        print("INITIALIZING WAVE")
        self.wave_responses = {}
        self.wave_request = {}
        self.last_wave = datetime.datetime.utcnow()

    def update_neighbors(self, neighbors):
        self.neighbors = neighbors



