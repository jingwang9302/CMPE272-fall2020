import time, zmq, random


def worker():
    worker_id = random.randrange(1, 100)
    print("I am worker#%s" % (worker_id))
    context = zmq.Context()

    worker_receiver = context.socket(zmq.PULL)
    worker_receiver.connect("tcp://127.0.0.1:5555")

    worker_sender = context.socket(zmq.PUSH)
    worker_sender.connect("tcp://127.0.0.1:5557")

    while True:
        work = worker_receiver.recv_json()
        data = work["num"]
        squared_num = float(data) ** 0.5
        result = {"worker": worker_id, "num": squared_num}
        worker_sender.send_json(result)

        print(result)


worker()

