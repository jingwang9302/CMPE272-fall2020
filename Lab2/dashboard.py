import time
import zmq
import json


def result_collector():
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    results_receiver.bind("tcp://127.0.0.1:5557")
    outfile = open("output.txt", "w")
    for x in range(10):
        result = results_receiver.recv_json()
        json.dump(result, outfile)
        print(result)


result_collector()
