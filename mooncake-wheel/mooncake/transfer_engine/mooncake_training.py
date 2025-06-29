import torch
import zmq
import time
from threading import Thread
from queue import Queue
from new_tensor import get_dtype_str

from transfer_engine import MooncakeTransferEngine

class MooncakeTraining(Thread):
    def __init__(self, config: dict):
        super().__init__()
        self._stop = False
        self.config = config
        self._queue = Queue(maxsize=config["send_bulk"])
        self.engine = MooncakeTransferEngine(
            hostname = config["training_ip"],
            gpu_id = config["training_gpu_id"],  # Using GPU
            ib_device = None  # No specific IB device
        )
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REP)
        self._socket.bind(f"tcp://*:{config['port']}")
        self._report = 0
        self.start()

    def __del__(self):
        self._socket.close()
        self._context.destroy()
            

    def reg_tensor(self, name: str, tensor: torch.Tensor):
        self._queue.put((name, tensor))

    def run(self):
        while True:
            name, tensor = self._queue.get()
            if name is None:
                break
            ret = self._socket.recv_json()  # get req.
            session_id = ret["session_id"]
            self._socket.send_json([name, tensor.shape, get_dtype_str(tensor.dtype)])
            ptr, size = tensor.data_ptr(), tensor.numel() * tensor.element_size()
            self.engine.register(ptr, size)
            ret = self._socket.recv_json()
            t1 = time.time()
            self.engine.transfer_sync(session_id, ptr, ret["ptr"], size)
            t2 = time.time()
            self._report += t2 - t1
            self._socket.send_json(ret)
            self.engine.deregister(ptr)

    def report(self):
        return self._report
    