import zmq
import sys
from multiprocessing import Process
import consul
import json


class Server:
    def __init__(self, port):
        self.di = {}
        self.di["collection"] = []
        self.port = port

    def server_connect(self):
        context = zmq.Context()
        consumer = context.socket(zmq.REP)
        consumer.bind(f"tcp://127.0.0.1:{self.port}")
        while True:
            raw = consumer.recv_json()
            # print(raw)
            # raw example: Server_port=2005:key=key-1,value=value-1, op=PUT
            op = raw['op']
            key = 'no value'
            value = 'no value'
            if (op == 'PUT'):
                key = raw['key']
                value = raw['value']
                self.put(key, value)
                res = 'success'
            if (op == 'GET_ONE'):
                key = raw['key']
                res = self.get_key(key)
            if (op == 'GET_ALL'):
                res = self.get_all()
            if (op == 'DE_REG'):
                self.deregister(self.port)
                res = 'success'

            print(f"Server_port={self.port}:key={key},value={value}, op={op}")
            consumer.send_json(res)

    def register(self):
        c = consul.Consul()
        server_name = "server-" + str(self.port)
        service_id = "serverid-" + str(self.port)
        c.agent.service.register(
            service_id=service_id, name=server_name, port=self.port)

    def deregister(self, port):
        service_id = "serverid-" + str(port)
        c = consul.Consul()
        c.agent.service.deregister(service_id)

    def get_all(self):
        return self.di

    def get_key(self, key):
        data_arr = self.di["collection"]
        value = 'no value'
        for i in range(len(data_arr)):
            if data_arr[i]["key"] == key:
                value = data_arr[i]['value']

        res = {
            'key': key,
            'value': value
        }
        return res

    def put(self, key, value):
        collection_list = self.di["collection"]
        for data_di in collection_list:
            if data_di["key"] == key:
                return self.di["collection"]
        else:
            new_pair = {
                'key': key,
                'value': value
            }
            res = self.di["collection"].append(new_pair)
            return res


if __name__ == "__main__":

    if len(sys.argv) > 1:
        server_port = int(sys.argv[1])
        server = Server(server_port)
        server.register()
    print(f"Starting a server at:{server_port}...")
    Process(target=server.server_connect, args=()).start()
