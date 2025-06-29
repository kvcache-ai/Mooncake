import torch
import zmq

from transfer_engine import MooncakeTransferEngine
from new_tensor import create_tensor

class MooncakeInference:
    def __init__(self, config: dict):
        self.config = config
        self._engine = MooncakeTransferEngine(
            hostname = config["inference_ip"],
            gpu_id = config["inference_gpu_id"],  # Using GPU
            ib_device = None  # No specific IB device
        )
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect(f"tcp://{config['training_ip']}:{config['port']}")
    
    def __del__(self):
        self._socket.close()
        self._context.destroy()

    def recv_tensor(self) -> dict:
        self._socket.send_json({"session_id": self._engine.get_session_id()})
        ret = self._socket.recv_json()  # ["name", "shape", "dtype"]
        tensor = create_tensor(ret[1], ret[2], self.config["inference_gpu_id"])
        self._engine.register(tensor.data_ptr(), tensor.numel() * tensor.element_size())
        self._socket.send_json({"ptr": tensor.data_ptr()})
        self._socket.recv_json()
        self._engine.deregister(tensor.data_ptr())
        return {ret[0]: tensor}