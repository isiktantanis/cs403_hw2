import time
from multiprocessing import Process
import zmq
import os


class MapReduce:
    def __init__(self, num_workers: int):
        self.num_workers = num_workers

    def Map(self, map_input):
        raise NotImplementedError("Map function is not implemented")

    def Reduce(self, reduce_input):
        raise NotImplementedError("Reduce function is not implemented")

    def _Producer(self, producer_input):
        # print(f"Producer start, {os.getpid()}")
        line_per_worker = len(producer_input) // self.num_workers
        extra_lines = len(producer_input) % self.num_workers
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.bind("tcp://127.0.0.1:5558")
        current = 0
        for i in range(self.num_workers):
            end = current + line_per_worker + int(i < extra_lines)
            msg = {"i": i, "data": producer_input[current:end]}
            current = end
            # print(f"sending {msg}")
            time.sleep(0.01)
            socket.send_json(msg)

        # print(f"Producer end, {os.getpid()}")

    def _Consumer(self):
        # print(f"Consumer start, {os.getpid()}")
        context = zmq.Context()
        receiver = context.socket(zmq.PULL)
        receiver.connect("tcp://127.0.0.1:5558")

        piece = receiver.recv_json()
        # print(f"{os.getpid()} received {piece}")
        partial_result = self.Map(piece["data"])

        sender = context.socket(zmq.PUSH)
        sender.connect("tcp://127.0.0.1:5559")
        sender.send_json({"i": piece["i"], "data": partial_result})
        # print(f"Consumer end, {os.getpid()}")

    def _ResultCollector(self):
        # print(f"ResultCollector start, {os.getpid()}")
        context = zmq.Context()
        receiver = context.socket(zmq.PULL)
        receiver.bind("tcp://127.0.0.1:5559")

        partial_results = [None] * self.num_workers
        for i in range(self.num_workers):
            piece = receiver.recv_json()
            # print(f"collector received {piece}")
            partial_results[piece["i"]] = piece["data"]
        # print("partial_results", partial_results)

        reduced_result = self.Reduce(partial_results)
        with open("results.txt", "w") as f:
            f.write(str(reduced_result))

        # print(f"ResultCollector end, {os.getpid()}")

    def start(self, filename: str):
        f = open(filename, "r")
        lines = f.readlines()
        f.close()

        producer = Process(
            target=self._Producer,
            kwargs={"producer_input": lines},
        )

        consumers = []
        for _ in range(self.num_workers):
            consumer = Process(
                target=self._Consumer,
                args=(),
            )
            consumers.append(consumer)
            consumer.start()

        result_collector = Process(
            target=self._ResultCollector,
            args=(),
        )

        producer.start()
        result_collector.start()
        producer.join()

        for consumer in consumers:
            consumer.join()

        result_collector.join()
