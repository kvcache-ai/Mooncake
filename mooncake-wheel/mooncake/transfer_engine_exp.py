import numpy as np
import torch
import pickle
import zmq
from typing import Dict, Any, Union, Optional, cast
from dataclasses import dataclass
from mooncake.engine import TransferEngine

@dataclass
class TransferConfig:
    """Transfer configuration"""
    hostname: str
    port: int = 5555
    zmq_host: str = "localhost"

class SimpleTransfer:
    """Simple {str, Tensor} transfer class"""
    
    def __init__(self, config: TransferConfig):
        self.config = config
        self.engine = None
        self.session_id = None
        self.remote_session_id = None
        self._init_engine()
    
    def _init_engine(self):
        """Init TransferEngine"""
        
        self.engine = TransferEngine()
        ret = self.engine.initialize(
            self.config.hostname,
            "P2PHANDSHAKE",
            "rdma",
            ""
        )
        if ret != 0:
            raise RuntimeError("TransferEngine initialization failed")
        
        self.session_id = f"{self.config.hostname}:{self.engine.get_rpc_port()}"
    
    def _serialize(self, data: Dict[str, Union[str, torch.Tensor]]) -> tuple[np.ndarray, int]:
        """Serialize data"""
        # Convert tensor to numpy array
        serializable = {}
        for k, v in data.items():
            if isinstance(v, torch.Tensor):
                serializable[k] = v.detach().cpu().numpy()
            else:
                serializable[k] = v
        
        # Use pickle for serialization
        bytes_data = pickle.dumps(serializable)
        buffer = np.frombuffer(bytes_data, dtype=np.uint8)
        return buffer, len(bytes_data)
    
    def _deserialize(self, buffer: np.ndarray, size: int) -> Dict[str, Union[str, torch.Tensor]]:
        """Deserialize data"""
        bytes_data = buffer[:size].tobytes()
        data = pickle.loads(bytes_data)
        
        # Convert numpy array to tensor
        result = {}
        for k, v in data.items():
            if isinstance(v, np.ndarray):
                result[k] = torch.from_numpy(v)
            else:
                result[k] = v
        return result

class Sender(SimpleTransfer):
    """Sender class"""
    
    def __init__(self, config: TransferConfig):
        super().__init__(config)
        self.zmq = zmq.Context()
        # Create sockets once - sender connects to receiver's ZMQ host
        self.size_socket = self.zmq.socket(zmq.PUSH)
        self.size_socket.connect(f"tcp://{self.config.zmq_host}:{self.config.port}")
        
        self.info_socket = self.zmq.socket(zmq.PULL)
        self.info_socket.bind(f"tcp://*:{self.config.port + 1}")
        
        self.complete_socket = self.zmq.socket(zmq.PUSH)
        self.complete_socket.connect(f"tcp://{self.config.zmq_host}:{self.config.port + 2}")
    
    def send_dict(self, data: Dict[str, Union[str, torch.Tensor]], receiver_session_id: str) -> bool:
        """Send data dictionary"""
        if self.engine is None:
            raise RuntimeError("Engine not initialized")
        
        # Serialize
        buffer, size = self._serialize(data)
        
        # Register memory
        ptr = buffer.ctypes.data
        ret = self.engine.register_memory(ptr, size)
        if ret != 0:
            raise RuntimeError("Memory registration failed")
        
        try:
            # Send data size via ZMQ
            self._send_size_via_zmq(size)
            
            # Wait for receiver info
            receiver_info = self._wait_receiver_info()
            
            # Transfer data
            ret = self.engine.transfer_sync_write(
                receiver_info["session_id"],
                ptr,
                receiver_info["ptr"],
                size
            )
            
            # Send transfer completion notification
            self._send_transfer_complete()
            
            return ret >= 0
            
        finally:
            self.engine.unregister_memory(ptr)
    
    def send(self, key: str, value: Union[str, torch.Tensor], receiver_session_id: str) -> bool:
        """Send a single key-value pair"""
        data = {key: value}
        return self.send_dict(data, receiver_session_id)
    
    def _send_size_via_zmq(self, size: int):
        """Send data size via ZMQ"""
        self.size_socket.send_json({"data_size": size})
    
    def _wait_receiver_info(self) -> Dict[str, Any]:
        """Wait for receiver info"""
        info = cast(Dict[str, Any], self.info_socket.recv_json())
        return info
    
    def _send_transfer_complete(self):
        """Send transfer completion notification"""
        self.complete_socket.send_json({"status": "complete"})
    
    def __del__(self):
        """Cleanup"""
        if hasattr(self, 'size_socket'):
            self.size_socket.close()
        if hasattr(self, 'info_socket'):
            self.info_socket.close()
        if hasattr(self, 'complete_socket'):
            self.complete_socket.close()
        if hasattr(self, 'zmq'):
            self.zmq.term()

class Receiver(SimpleTransfer):
    """Receiver class"""
    
    def __init__(self, config: TransferConfig):
        super().__init__(config)
        self.buffer = None
        self.ptr = None
        self.buffer_size = 0
        self.zmq = zmq.Context()
        # Create sockets once - receiver binds to its own ports and connects to sender's ZMQ host
        self.size_socket = self.zmq.socket(zmq.PULL)
        self.size_socket.bind(f"tcp://*:{self.config.port}")
        
        self.info_socket = self.zmq.socket(zmq.PUSH)
        self.info_socket.connect(f"tcp://{self.config.zmq_host}:{self.config.port + 1}")
        
        self.complete_socket = self.zmq.socket(zmq.PULL)
        self.complete_socket.bind(f"tcp://*:{self.config.port + 2}")
    
    def recv_dict(self) -> Optional[Dict[str, Union[str, torch.Tensor]]]:
        """Receive data dictionary"""
        # Receive data size via ZMQ
        data_size = self._recv_size_via_zmq()
        
        # Dynamically allocate receive buffer
        self._allocate_buffer(data_size)
        
        # Register buffer
        self._register_buffer()
        
        # Send buffer info to sender
        self._send_buffer_info()
        
        # Wait for transfer completion notification
        self._wait_transfer_complete()
        
        # Deserialize
        if self.buffer is not None:
            return self._deserialize(self.buffer, data_size)
        else:
            raise RuntimeError("Buffer not allocated")
    
    def recv(self) -> tuple[str, Union[str, torch.Tensor]]:
        """Receive a single key-value pair"""
        data = self.recv_dict()
        if data is None or len(data) == 0:
            raise RuntimeError("No data received")
        
        # Return the first (and only) key-value pair
        key, value = next(iter(data.items()))
        return key, value
    
    def _recv_size_via_zmq(self) -> int:
        """Receive data size via ZMQ"""
        info = cast(Dict[str, Any], self.size_socket.recv_json())
        return info["data_size"]
    
    def _allocate_buffer(self, size: int):
        """Dynamically allocate receive buffer"""
        self.buffer_size = size
        self.buffer = np.zeros(size, dtype=np.uint8)
        self.ptr = self.buffer.ctypes.data
        print(f"Allocated buffer of size: {size} bytes")
    
    def _register_buffer(self):
        """Register receive buffer"""
        if self.engine is None:
            raise RuntimeError("Engine not initialized")
        
        if self.buffer is None:
            raise RuntimeError("Buffer not allocated")
        
        ret = self.engine.register_memory(self.ptr, self.buffer_size)
        if ret != 0:
            raise RuntimeError("Buffer registration failed")
    
    def _send_buffer_info(self):
        """Send buffer info"""
        self.info_socket.send_json({
            "session_id": self.session_id,
            "ptr": self.ptr,
            "len": self.buffer_size
        })
    
    def _wait_transfer_complete(self):
        """Wait for transfer completion notification"""
        notification = cast(Dict[str, Any], self.complete_socket.recv_json())
        
        if notification.get("status") == "complete":
            print("Transfer completed successfully")
        else:
            raise RuntimeError("Unexpected transfer notification")
    
    def __del__(self):
        """Cleanup"""
        if hasattr(self, 'engine') and self.engine and self.ptr:
            self.engine.unregister_memory(self.ptr)
        if hasattr(self, 'size_socket'):
            self.size_socket.close()
        if hasattr(self, 'info_socket'):
            self.info_socket.close()
        if hasattr(self, 'complete_socket'):
            self.complete_socket.close()
        if hasattr(self, 'zmq'):
            self.zmq.term()

# Convenience functions
def send(data: Dict[str, Union[str, torch.Tensor]], 
         hostname: str,
         receiver_session_id: Optional[str] = None) -> bool:
    """Convenience function to send data"""
    if receiver_session_id is None:
        raise ValueError("receiver_session_id is required")
    
    config = TransferConfig(hostname)
    sender = Sender(config)
    return sender.send_dict(data, receiver_session_id)

def recv(hostname: str) -> Optional[Dict[str, Union[str, torch.Tensor]]]:
    """Convenience function to receive data"""
    config = TransferConfig(hostname)
    receiver = Receiver(config)
    return receiver.recv_dict() 
