import socket
import unittest

import torch


def get_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return socket.gethostbyname(socket.gethostname())


class TestTransferOnHip(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from mooncake.engine import TransferEngine

        if not torch.cuda.is_available():
            raise unittest.SkipTest("ROCm device not available")
        if not getattr(torch.version, "hip", None):
            raise unittest.SkipTest("PyTorch is not built with HIP support")

        cls.engine = TransferEngine()
        cls.local_hostname = get_ip()
        ret = cls.engine.initialize(cls.local_hostname, "P2PHANDSHAKE", "hip", "")
        if ret != 0:
            raise unittest.SkipTest(f"Failed to initialize HIP transport (code {ret})")

        if ":" in cls.local_hostname:
            cls.target_name = f"[{cls.local_hostname}]:{cls.engine.get_rpc_port()}"
        else:
            cls.target_name = f"{cls.local_hostname}:{cls.engine.get_rpc_port()}"

        cls.device = torch.device("cuda:0")
        cls.numel = 1024 * 1024
        cls.src_tensor = torch.ones(cls.numel, dtype=torch.uint8, device=cls.device)
        cls.dst_tensor = torch.zeros(cls.numel, dtype=torch.uint8, device=cls.device)
        cls.nbytes = cls.src_tensor.numel() * cls.src_tensor.element_size()

        ret1 = cls.engine.register_memory(cls.src_tensor.data_ptr(), cls.nbytes)
        ret2 = cls.engine.register_memory(cls.dst_tensor.data_ptr(), cls.nbytes)
        if ret1 != 0 or ret2 != 0:
            raise RuntimeError("Failed to register HIP buffers")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "engine"):
            if hasattr(cls, "src_tensor"):
                cls.engine.unregister_memory(cls.src_tensor.data_ptr())
            if hasattr(cls, "dst_tensor"):
                cls.engine.unregister_memory(cls.dst_tensor.data_ptr())

    def test_transfer_sync_write(self):
        self.src_tensor.fill_(123)
        self.dst_tensor.zero_()

        ret = self.engine.transfer_sync_write(
            self.target_name,
            self.src_tensor.data_ptr(),
            self.dst_tensor.data_ptr(),
            self.nbytes,
        )
        self.assertEqual(ret, 0)
        torch.cuda.synchronize()
        self.assertTrue(torch.equal(self.src_tensor, self.dst_tensor))

    def test_transfer_sync_read(self):
        self.src_tensor.zero_()
        self.dst_tensor.fill_(77)

        ret = self.engine.transfer_sync_read(
            self.target_name,
            self.src_tensor.data_ptr(),
            self.dst_tensor.data_ptr(),
            self.nbytes,
        )
        self.assertEqual(ret, 0)
        torch.cuda.synchronize()
        self.assertTrue(torch.all(self.src_tensor == 77))


if __name__ == "__main__":
    unittest.main()
