import os
import socket
import unittest
from unittest import mock
import torch

def get_ip() -> str:
    try:
        # try to get ip from network interface
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return socket.gethostbyname(socket.gethostname())

class TestTransferOnCuda(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Safely set environment variables only for this test class
        cls.env_patcher = mock.patch.dict(os.environ, {
            "MC_USE_IPV6": "1", 
            "MC_GID_INDEX": "3"
        })
        cls.env_patcher.start()
        from mooncake.engine import TransferEngine

        cls.engine = TransferEngine()
        cls.local_hostname = get_ip()
        # Initialize engine
        # In this test environment, we use local hostname for both initiator and target
        ret = cls.engine.initialize(cls.local_hostname, "P2PHANDSHAKE", "rdma", "mlx5_bond_0")
        if ret != 0:
            raise unittest.SkipTest(f"Failed to initialize engine (code {ret}). Possibly no RDMA device.")
        
        cls.rpc_port = cls.engine.get_rpc_port()
        if ":" in cls.local_hostname:
            cls.target_name = f"[{cls.local_hostname}]:{cls.rpc_port}"
        else:
            cls.target_name = f"{cls.local_hostname}:{cls.rpc_port}"

        cls.device = torch.device("cuda:0")
        if not torch.cuda.is_available():
            raise unittest.SkipTest("CUDA not available")

        # 64MB tensor
        cls.N = 1024 * 1024 * 64
        cls.src_tensor = torch.ones(cls.N, dtype=torch.uint8, device=cls.device)
        cls.dst_tensor = torch.zeros(cls.N, dtype=torch.uint8, device=cls.device)

        # Register Memory
        ret1 = cls.engine.register_memory(cls.src_tensor.data_ptr(), cls.src_tensor.nbytes)
        ret2 = cls.engine.register_memory(cls.dst_tensor.data_ptr(), cls.dst_tensor.nbytes)
        if ret1 != 0 or ret2 != 0:
            raise RuntimeError("Failed to register memory for RDMA")

        cls.copy_stream = torch.cuda.Stream(cls.device)
        
        # Tensors for overlapping computation
        cls.a = torch.randn(4096, 4096, device=cls.device)
        cls.b = torch.randn(4096, 4096, device=cls.device)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'engine'):
            if hasattr(cls, 'src_tensor'):
                cls.engine.unregister_memory(cls.src_tensor.data_ptr())
            if hasattr(cls, 'dst_tensor'):
                cls.engine.unregister_memory(cls.dst_tensor.data_ptr())
        
        # Stop the environment patcher
        if hasattr(cls, 'env_patcher'):
            cls.env_patcher.stop()

    def test_transfer_write_on_cuda(self):
        """Test single transfer write (src -> dst)."""
        for i in range(10):
            self.src_tensor.fill_(i)
            self.dst_tensor.zero_()
            
            event = torch.cuda.default_stream().record_event()
            self.copy_stream.wait_event(event)
            self.engine.transfer_write_on_cuda(
                self.target_name,
                self.src_tensor.data_ptr(),
                self.dst_tensor.data_ptr(),
                self.src_tensor.nbytes,
                self.copy_stream.cuda_stream,
            )
            event = self.copy_stream.record_event()
            
            # Concurrent computation
            _ = torch.matmul(self.a, self.b)
            
            torch.cuda.default_stream().wait_event(event)
            self.assertTrue(torch.all(self.dst_tensor == i), f"Write verification failed at iter {i}")

    def test_transfer_read_on_cuda(self):
        """Test single transfer read (dst -> src)."""
        for i in range(10):
            val = i + 50
            # Setup remote (dst) with data
            self.dst_tensor.fill_(val)
            self.src_tensor.zero_()
            
            event = torch.cuda.default_stream().record_event()
            self.copy_stream.wait_event(event)
            self.engine.transfer_read_on_cuda(
                self.target_name,
                self.src_tensor.data_ptr(), # local
                self.dst_tensor.data_ptr(), # remote
                self.src_tensor.nbytes,
                self.copy_stream.cuda_stream,
            )
            event = self.copy_stream.record_event()
            
            # Concurrent computation
            _ = torch.matmul(self.a, self.b)
            
            torch.cuda.default_stream().wait_event(event)
            self.assertTrue(torch.all(self.src_tensor == val), f"Read verification failed at iter {i}")

    def test_batch_transfer_write_on_cuda(self):
        """Test batched transfer write (src chunks -> dst chunks)."""
        chunk_size = self.N // 4
        src_ptrs = [self.src_tensor.data_ptr() + j * chunk_size for j in range(4)]
        dst_ptrs = [self.dst_tensor.data_ptr() + j * chunk_size for j in range(4)]
        lengths = [chunk_size] * 4

        for i in range(10):
            val = i + 100
            self.src_tensor.fill_(val)
            self.dst_tensor.zero_()
            
            event = torch.cuda.default_stream().record_event()
            self.copy_stream.wait_event(event)
            self.engine.batch_transfer_write_on_cuda(
                self.target_name, src_ptrs, dst_ptrs, lengths, self.copy_stream.cuda_stream
            )
            event = self.copy_stream.record_event()
            
            # Concurrent computation
            _ = torch.matmul(self.a, self.b)
            
            torch.cuda.default_stream().wait_event(event)
            self.assertTrue(torch.all(self.dst_tensor == val), f"Batch write verification failed at iter {i}")

    def test_batch_transfer_read_on_cuda(self):
        """Test batched transfer read (dst chunks -> src chunks)."""
        chunk_size = self.N // 4
        src_ptrs = [self.src_tensor.data_ptr() + j * chunk_size for j in range(4)]
        dst_ptrs = [self.dst_tensor.data_ptr() + j * chunk_size for j in range(4)]
        lengths = [chunk_size] * 4

        for i in range(10):
            val = i + 200
            # Setup remote (dst) with data
            self.dst_tensor.fill_(val)
            self.src_tensor.zero_()
            
            event = torch.cuda.default_stream().record_event()
            self.copy_stream.wait_event(event)
            self.engine.batch_transfer_read_on_cuda(
                self.target_name, src_ptrs, dst_ptrs, lengths, self.copy_stream.cuda_stream
            )
            event = self.copy_stream.record_event()
            
            # Concurrent computation
            _ = torch.matmul(self.a, self.b)
            
            torch.cuda.default_stream().wait_event(event)
            self.assertTrue(torch.all(self.src_tensor == val), f"Batch read verification failed at iter {i}")

if __name__ == "__main__":
    unittest.main()
