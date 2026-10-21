"""RealClient tensor read regressions; requires a running Mooncake master."""

import os
import time
import unittest
import uuid

try:
    import torch
except ImportError:
    torch = None

from mooncake.store import MooncakeDistributedStore


class TensorAfterMetadata:
    """Run a callback when the binding inspects the destination after its read."""

    def __init__(self, tensor, callback):
        self.tensor = tensor
        self.callback = callback

    def is_contiguous(self):
        callback, self.callback = self.callback, None
        if callback is not None:
            callback()
        return self.tensor.is_contiguous()

    def __getattr__(self, name):
        return getattr(self.tensor, name)


@unittest.skipIf(torch is None, "PyTorch is unavailable")
class TensorReadSnapshotTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        rc = cls.store.setup(
            os.getenv("LOCAL_HOSTNAME", "localhost"),
            os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
            32 * 1024 * 1024,
            16 * 1024 * 1024,
            os.getenv("PROTOCOL", "tcp"),
            os.getenv("DEVICE_NAME", ""),
            os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
        )
        if rc != 0:
            raise RuntimeError(f"Store setup failed: {rc}")

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def test_upsert_between_metadata_and_payload(self):
        devices = ["cpu"] + (["cuda"] if torch.cuda.is_available() else [])
        for device in devices:
            for batched in (False, True):
                with self.subTest(device=device, batched=batched):
                    key = f"tensor_snapshot_{uuid.uuid4().hex}"
                    original = torch.arange(8, dtype=torch.float32)
                    replacement = original + 100
                    destination = torch.full_like(original, -1, device=device)
                    size = destination.numel() * destination.element_size()
                    self.assertEqual(self.store.put_tensor(key, original), 0)
                    self.assertEqual(
                        self.store.register_buffer(destination.data_ptr(), size), 0
                    )
                    upserts = []

                    def upsert():
                        upserts.append(self.store.upsert_tensor(key, replacement))

                    proxy = TensorAfterMetadata(destination, upsert)
                    try:
                        if batched:
                            result = self.store.batch_get_tensor_into_cuda(
                                [key], [proxy]
                            )
                            self.assertEqual(result, [size])
                        else:
                            result = self.store.get_tensor_into_cuda(key, proxy)
                            self.assertEqual(result, size)
                        self.assertEqual(upserts, [0])
                        self.assertTrue(torch.equal(destination.cpu(), original))
                        self.assertTrue(
                            torch.equal(self.store.get_tensor(key), replacement)
                        )
                    finally:
                        self.store.unregister_buffer(destination.data_ptr())

    def test_batch_preserves_missing_key_error_and_valid_item(self):
        key = f"tensor_snapshot_{uuid.uuid4().hex}"
        original = torch.arange(8, dtype=torch.float32)
        destinations = [torch.empty_like(original), torch.empty_like(original)]
        size = original.numel() * original.element_size()
        self.assertEqual(self.store.put_tensor(key, original), 0)
        for destination in destinations:
            self.assertEqual(
                self.store.register_buffer(destination.data_ptr(), size), 0
            )
        try:
            result = self.store.batch_get_tensor_into_cuda(
                [key + "_missing", key], destinations
            )
            # OBJECT_NOT_FOUND from the metadata query must survive the wrapper.
            self.assertEqual(result, [-704, size])
            self.assertTrue(torch.equal(destinations[1], original))
        finally:
            for destination in destinations:
                self.store.unregister_buffer(destination.data_ptr())

    def test_expired_payload_snapshot_preserves_lease_error(self):
        key = f"tensor_snapshot_{uuid.uuid4().hex}"
        original = torch.arange(8, dtype=torch.float32)
        destination = torch.empty_like(original)
        size = original.numel() * original.element_size()
        self.assertEqual(self.store.put_tensor(key, original), 0)
        self.assertEqual(self.store.register_buffer(destination.data_ptr(), size), 0)
        # Match the master's configured TTL, as in test_put_get_tensor.py.
        ttl_seconds = int(os.getenv("DEFAULT_KV_LEASE_TTL", "5000")) / 1000
        proxy = TensorAfterMetadata(destination, lambda: time.sleep(ttl_seconds + 0.1))
        try:
            self.assertEqual(self.store.get_tensor_into_cuda(key, proxy), -707)
        finally:
            self.store.unregister_buffer(destination.data_ptr())


if __name__ == "__main__":
    unittest.main()
