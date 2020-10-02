import time
import zmq
import random


def generator():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://127.0.0.1:5555")

    for x in range(10):
        num = random.randrange(1, 10001)
        work_message = {"num": num}
        socket.send_json(work_message)
        time.sleep(1)
        print(work_message)


generator()

