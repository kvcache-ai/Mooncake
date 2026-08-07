import ctypes
import gc
from concurrent.futures import ThreadPoolExecutor
from ctypes.util import find_library
import unittest

try:
    import torch
except ImportError:
    torch = None

try:
    from mooncake import engine as mooncake_engine
except ImportError:
    mooncake_engine = None


BUFFER_BYTES = 1 << 20
TRANSFER_BYTES = 256 << 10


class CudaRuntime:
    CUDA_MEMCPY_DEVICE_TO_HOST = 2

    def __init__(self):
        self.lib = ctypes.CDLL(find_library("cudart") or "libcudart.so")
        self.lib.cudaSetDevice.argtypes = [ctypes.c_int]
        self.lib.cudaMemset.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_size_t]
        self.lib.cudaMemcpy.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_int,
        ]

    def check(self, result, operation):
        if result != 0:
            raise RuntimeError(f"{operation} failed with CUDA error {result}")

    def set_device(self, device):
        self.check(self.lib.cudaSetDevice(device), "cudaSetDevice")

    def memset(self, buffer, value, length):
        self.check(self.lib.cudaMemset(buffer, value, length), "cudaMemset")

    def read(self, buffer, length):
        result = (ctypes.c_ubyte * length)()
        self.check(
            self.lib.cudaMemcpy(
                result, buffer, length, self.CUDA_MEMCPY_DEVICE_TO_HOST
            ),
            "cudaMemcpy",
        )
        return bytes(result)


class NcclRuntime:
    def __init__(self):
        self.lib = ctypes.CDLL(find_library("nccl") or "libnccl.so")
        self.lib.ncclMemAlloc.argtypes = [
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_size_t,
        ]
        self.lib.ncclMemAlloc.restype = ctypes.c_int
        self.lib.ncclMemFree.argtypes = [ctypes.c_void_p]
        self.lib.ncclMemFree.restype = ctypes.c_int

    def allocate(self, length):
        buffer = ctypes.c_void_p()
        result = self.lib.ncclMemAlloc(ctypes.byref(buffer), length)
        if result != 0 or not buffer.value:
            raise RuntimeError(f"ncclMemAlloc failed with NCCL error {result}")
        return buffer

    def free(self, buffer):
        result = self.lib.ncclMemFree(buffer)
        if result != 0:
            raise RuntimeError(f"ncclMemFree failed with NCCL error {result}")


NCCL_HOST_RUNTIME_AVAILABLE = (
    mooncake_engine is not None
    and torch is not None
    and getattr(mooncake_engine, "SUPPORT_NCCL_HOST", False)
    and mooncake_engine.is_nccl_host_runtime_available()
)


@unittest.skipUnless(
    NCCL_HOST_RUNTIME_AVAILABLE,
    "NCCL host transport requires a CUDA-enabled Mooncake wheel with PyTorch",
)
class TestNcclHostTransport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not torch.cuda.is_available() or torch.cuda.device_count() < 2:
            raise unittest.SkipTest("NCCL host transport requires two CUDA GPUs")
        cls.cuda = CudaRuntime()
        cls.nccl = NcclRuntime()

    def test_write_and_read_rejection(self):
        engines = []
        buffers = []
        try:
            for device in range(2):
                self.cuda.set_device(device)
                buffer = self.nccl.allocate(BUFFER_BYTES)
                self.cuda.memset(buffer, 0, BUFFER_BYTES)

                engine = mooncake_engine.TransferEngine()
                self.assertEqual(
                    engine.initialize("127.0.0.1", "P2PHANDSHAKE", "nccl", ""),
                    0,
                )
                self.assertEqual(
                    engine.register_memory(
                        buffer.value, BUFFER_BYTES, f"cuda:{device}"
                    ),
                    0,
                )
                engines.append(engine)
                buffers.append(buffer)

            peer = engines[1].get_local_ip_and_port()
            self.assertTrue(peer)
            self.assertEqual(
                engines[0].transfer_sync_read(
                    peer, buffers[0].value, buffers[1].value, TRANSFER_BYTES
                ),
                -1,
            )

            self.cuda.set_device(0)
            self.cuda.memset(buffers[0], 0x5A, TRANSFER_BYTES)
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(
                    executor.map(
                        lambda _: engines[0].transfer_sync_write(
                            peer,
                            buffers[0].value,
                            buffers[1].value,
                            TRANSFER_BYTES,
                        ),
                        range(4),
                    )
                )
            self.assertEqual(results, [0] * 4)

            self.cuda.set_device(1)
            self.assertEqual(
                self.cuda.read(buffers[1], TRANSFER_BYTES),
                b"\x5A" * TRANSFER_BYTES,
            )
        finally:
            engines.clear()
            gc.collect()
            for device, buffer in enumerate(buffers):
                self.cuda.set_device(device)
                self.nccl.free(buffer)


if __name__ == "__main__":
    unittest.main()
