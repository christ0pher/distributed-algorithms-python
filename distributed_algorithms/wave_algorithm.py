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
        self.wave_responses = []
        self.last_wave = datetime.datetime.utcnow()
        self.parent = None
        self.name = name
        self.wave_init = False
        self.wave_request = {}

    async def run_wave(self):

        try:
            msg = await self.wave_zmocket.recv_json(flags=zmq.NOBLOCK)
            print("%s received the message %s" % (self.name, str(msg)))
            send_msg, message_list = self.handle_wave_message(msg)

            await self.snd_msg(message_list, send_msg)
        except Exception as e:
            print("Error: "+str(e))

        if datetime.datetime.utcnow() - self.last_wave > datetime.timedelta(seconds=10) \
                and self.controller:
            self.init_wave()
            send_msg, message_list = self.mk_wave_request()

            await self.snd_msg(message_list, send_msg)

    async def snd_msg(self, message_list, send_msg):
        if send_msg:
            print(str(message_list))
            for msg in message_list:
                print("Sending message %s" % str(msg))
                await self.wave_zmocket.send_multipart([msg["receiver"].encode(),
                                                        json.dumps(msg["message"]).encode()])

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
                return True, [{"receiver": self.parent, "message": response}]

            elif all_neighbors and self.controller:
                print("WAVE IS DONE")
                print(json.dumps(self.wave_responses, indent=4, sort_keys=True))
        if msg["type"] == "wave_request":
            self.init_wave()
            self.parent = sender
            if len(self.neighbors) != 0:
                return self.mk_wave_request()
            if len(self.neighbors) == 0:
                response = {
                    "sender": self.name,
                    "type": "wave_response",
                    "parent": self.parent,
                    "responses": self.wave_responses
                }
                return True, [{"receiver": self.parent, "message": response}]

        return False, None

    def mk_wave_request(self):
        request = {
            "type": "wave_request",
            "sender": self.name
        }
        receivers = []
        for neighbor in self.neighbors:
            receivers.append({"receiver": neighbor, "message": request})
            self.wave_request[neighbor] = False
        return True, receivers

    def init_wave(self):
        print("INITIALIZING WAVE")
        self.wave_responses = []
        self.wave_request = {}
        self.last_wave = datetime.datetime.utcnow()

    def update_neighbors(self, neighbors):
        self.neighbors = neighbors



