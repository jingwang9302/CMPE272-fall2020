import zmq
import time
import sys
from itertools import cycle
from consistent_hashing import ConsistentHashing
from hrw import HWR
import consul


# def create_clients(servers):
#     producers = {}  # eg, 'tcp://127.0.0.1:{port}': producer_conn
#     context = zmq.Context()
#     for server in servers:
#         print(f"Creating a server connection to {server}...")
#         producer_conn = context.socket(zmq.REQ)
#         producer_conn.connect(server)
#         producers[server] = producer_conn
#     return producers


# def generate_data_round_robin(servers):
#     print("Starting...")
#     producers = create_clients(servers)
#     pool = cycle(producers.values())
#     for num in range(10):
#         data = {'key': f'key-{num}', 'value': f'value-{num}'}
#         print(f"Sending data:{data}")
#         next(pool).send_json(data)
#         time.sleep(1)
#     print("Done")


# def generate_data_hrw_hashing(servers):
#     print("Starting...")
#     # TODO
#     producers = create_clients(servers)
#     hrw_hashing = HWR(servers, seed=31)
#     for num in range(10):
#         data = {'key': f'key-{num}', 'value': f'value-{num}'}
#         print(f"Sending data:{data}")
#         producers[hrw_hashing.find_leader_node(num)].send_json(data)
#         time.sleep(1)
#     print("Done")


class Client:
    def __init__(self):
        self.servers = []  # eg. 'tcp://127.0.0.1:{port}'
        self.c = consul.Consul()
        # http://localhost:8500/v1/agent/services/的data
        serversDict = self.c.agent.services()

        for server in serversDict:
            each_server = serversDict[server]
            port = each_server['Port']
            self.servers.append(f'tcp://127.0.0.1:{port}')

        self.producers = self.create_clients(self.servers)
        self.hashing_ring = ConsistentHashing(self.servers)

    def create_clients(self, servers):
        producers = {}  # eg, 'tcp://127.0.0.1:{port}': producer_conn
        context = zmq.Context()
        for server in servers:
            print(f"Creating a server connection to {server}...")
            producer_conn = context.socket(zmq.REQ)
            producer_conn.connect(server)
            producers[server] = producer_conn
        return producers

    def put_data(self, params):
        # params = {'key': f'key-{num}', 'value': f'value-{num}', 'op': 'PUT'}
        print(f"Sending data:{params}")
        target_url = self.hashing_ring.get_node(params['key'])[0]
        target_producer_conn = self.producers[target_url]
        target_producer_conn.send_json(params)
        res = target_producer_conn.recv()
        print(f"received res: {res}")
        return res

    def get_one_data(self, params):
        # params = {'key': f'key-{num}', 'value': '', 'op': 'GET_ONE'}
        target_url = self.hashing_ring.get_node(params['key'])[0]
        target_producer_conn = self.producers[target_url]
        target_producer_conn.send_json(params)
        res = target_producer_conn.recv()
        print(f"received res: {res}")
        return res

    def get_all_data_from_producer(self, producer):
        params = {'op': 'GET_ALL'}
        producer.send_json(params)
        data = producer.recv()
        return data

    def get_all_data(self):
        res = []
        for url in self.producers:
            producer_conn = self.producers[url]
            data = self.get_all_data_from_producer(producer_conn)
            res.append(data)
        return res

    def deregister_node(self, port):
        producer_conn = self.producers[f"tcp://127.0.0.1:{port}"]
        params = {'op': 'DE_REG', 'value': '', 'key': ''}
        producer_conn.send_json(params)
        res = producer_conn.recv()
        print("deregister success")

    def delete_node(self, port):
        producer_conn = self.producers[f"tcp://127.0.0.1:{port}"]
        server_data = self.get_all_data_from_producer(producer_conn)
        # save all data to other server
        self.hashing_ring.remove_node(f"tcp://127.0.0.1:{port}")
        print(server_data)
        # for data in server_data:
        #     print(data)
        # self.put_data(
        #     {'key': data[key], 'value': data, 'op': 'PUT'})

        # if removing the last server, assign data to the first server
        # if current_server_position + 1 == len(self.servers):
        #     self.put_data(0)

        # else:
        #     self.put_data(current_server_position+1)
        # next_server = self.hashing_ring.sorted_keys
        # self.deregister_node()


if __name__ == "__main__":

    client = Client()

    # Put data
    for num in range(10):
        params = {'key': f'key-{num}', 'value': f'value-{num}', 'op': 'PUT'}
        client.put_data(params)
        time.sleep(1)
    # Get all
    print(client.get_all_data())
    # Get one data

    # producer_conn = self.producers["tcp://127.0.0.1:2001"]
    # res = client.get_all_data_from_producer(producer_conn)
    # print(res)

    # client.delete_node(2001)


# servers = []  # eg. 'tcp://127.0.0.1:{port}'
    # c = consul.Consul()
    # serversDict = c.agent.services()  # http://localhost:8500/v1/agent/services/的data

    # for server in serversDict:
    #     each_server = serversDict[server]
    #     port = each_server['Port']
    #     servers.append(f'tcp://127.0.0.1:{port}')
    # print(serversDict)
    # print("Servers:", servers)
    # generate_data_round_robin(servers)
    # generate_data_consistent_hashing(servers)
    # generate_data_hrw_hashing(servers)
