from multiprocessing import Process, Value, Array
import zmq


class MapReduce:
    def __init__(self, num_workers: int):
        self.num_workers = num_workers

    def Map(self, map_input):
        raise NotImplementedError("Map function is not implemented")

    def Reduce(self, reduce_input):
        raise NotImplementedError("Reduce function is not implemented")

    def _Producer(self, producer_input):
        line_per_worker = len(producer_input) // self.num_workers
        extra_lines = len(producer_input) % self.num_workers
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.bind("tcp://127.0.0.1:5558")

        for i in range(self.num_workers):
            print(i)
            start = i * line_per_worker + int(i < extra_lines)
            end = start + line_per_worker + int(i < extra_lines)
            socket.send_json({"i": i, "data": producer_input[start:end]})
        print("producer in end")

    def _Consumer(self, consumer_input):
        context = zmq.Context()
        receiver = context.socket(zmq.PULL)
        receiver.connect("tcp://127.0.0.1:5558")

        piece = receiver.recv_json()
        partial_result = self.Map(piece["data"])

        sender = context.socket(zmq.PUSH)
        sender.bind("tcp://127.0.0.1:5559")
        sender.send_json({"i": piece["i"], "data": partial_result})

    def _ResultCollector(self):
        context = zmq.Context()
        receiver = context.socket(zmq.PULL)
        receiver.connect("tcp://127.0.0.1:5559")

        partial_results = [None * self.num_workers]
        for i in range(self.num_workers):
            piece = receiver.recv_json()
            partial_results[piece["i"]] = piece["data"]

        reduced_result = self.Reduce(partial_results)
        with open("results.txt", "w") as f:
            f.write(reduced_result)

    def start(self, filename: str):
        # generate num_workers amount of consumer processes
        f = open(filename, "r")
        lines = f.readlines()
        f.close()
        # generate producer process
        producer = Process(
            target=self._Producer,
            kwargs={"producer_input": lines},
        )
        print("prod start")
        producer.start()
        producer.join()
        print("prod end")

        for _ in range(self.num_workers):
            consumer = Process(
                target=self._Consumer,
                args=(),
            )
            consumer.start()
            consumer.join()

        result_collector = Process(
            target=self._ResultCollector,
            args=(),
        )
        result_collector.start()
        result_collector.join()
