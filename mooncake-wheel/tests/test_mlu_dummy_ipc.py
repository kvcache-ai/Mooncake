"""MLU IPC tests against a separately launched mooncake_client service.

Set MC_MLU_REAL_CLIENT=127.0.0.1:50052 and MC_STORE_TEST_REQUIRE_MLU=1.
Both processes need access to the same devices and /dev/shm namespace.
"""

import gc
import os
import struct
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

from test_mlu_tensor import load_mlu, registered, tensor_bytes


class TestMluDummyIpc(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from mooncake.store import MooncakeDistributedStore

        cls.torch = load_mlu()
        cls.torch.set_num_threads(1)
        cls.store = MooncakeDistributedStore()
        result = cls.store.setup_dummy(
            0, 8 << 20, os.getenv("MC_MLU_REAL_CLIENT", "127.0.0.1:50052")
        )
        if result:
            cls.store.close()
            raise RuntimeError(f"setup_dummy failed: {result}")

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        self.keys = []

    def tearDown(self):
        for key in self.keys:
            self.store.remove(key, force=True)

    def key(self):
        key = f"mlu_dummy_ipc_{uuid.uuid4().hex}"
        self.keys.append(key)
        return key

    def assertTensor(self, actual, expected):
        self.assertTrue(self.torch.equal(actual.cpu(), expected))

    def test_slices_shared_allocation_and_upsert(self):
        torch = self.torch
        expected = torch.arange(8192, dtype=torch.int32).reshape(2, 4096)
        allocation = expected.to("mlu")
        sources = list(allocation.unbind())
        keys = [self.key(), self.key()]
        self.assertEqual(self.store.batch_put_tensor(keys, sources), [0, 0])
        allocation.add_(7)
        self.assertEqual(self.store.batch_upsert_tensor(keys, sources), [0, 0])
        del sources, allocation
        gc.collect()
        torch.mlu.empty_cache()
        destinations = [
            torch.empty(4096, dtype=torch.int32, device="mlu") for _ in keys
        ]
        # Tensor IPC imports do not require TE registration of the destination.
        self.assertEqual(
            self.store.batch_get_tensor_into_cuda(keys, destinations), [16384, 16384]
        )
        for i, destination in enumerate(destinations):
            self.assertTensor(destination, expected[i] + 7)

    def test_busy_current_stream_and_noncontiguous_source(self):
        torch = self.torch
        stream = torch.mlu.Stream()
        for strided in (False, True):
            with self.subTest(strided=strided), torch.mlu.stream(stream):
                source = torch.zeros((256, 256), dtype=torch.int32, device="mlu")
                torch.mlu._sleep(100_000_000)
                source.fill_(37)
                key = self.key()
                if strided:
                    source = source.t()
                self.assertEqual(self.store.put_tensor(key, source), 0)
                destination = torch.empty((256, 256), dtype=torch.int32, device="mlu")
                torch.mlu._sleep(100_000_000)
                destination.fill_(-1)
                self.assertEqual(
                    self.store.get_tensor_into_cuda(key, destination),
                    tensor_bytes(source),
                )
                self.assertTensor(
                    destination, torch.full((256, 256), 37, dtype=torch.int32)
                )

    def test_raw_registered_pointer_capacity_and_bounds(self):
        torch = self.torch
        expected = torch.arange(4096, dtype=torch.int32)
        source = expected.to("mlu")
        destination = torch.full((4098,), -1, dtype=torch.int32, device="mlu")
        torch.mlu.synchronize()
        key = self.key()
        with registered(self.store, source, destination):
            self.assertEqual(
                self.store.put_from(key, source.data_ptr(), tensor_bytes(source)), 0
            )
            # Capacity may exceed the stored value; preserve the trailing bytes.
            self.assertEqual(
                self.store.get_into(
                    key, destination.data_ptr(), tensor_bytes(destination)
                ),
                tensor_bytes(source),
            )
            self.assertTensor(destination[:4096], expected)
            self.assertTensor(
                destination[4096:], torch.full((2,), -1, dtype=torch.int32)
            )
            self.assertLess(
                self.store.put_from(
                    self.key(), source.data_ptr() + 4, tensor_bytes(source)
                ),
                0,
            )
        self.assertLess(
            self.store.get_into(key, destination.data_ptr(), tensor_bytes(destination)),
            0,
        )

    def test_mixed_fallback_and_partial_read_errors(self):
        torch = self.torch
        cpu = torch.arange(4096, dtype=torch.int32)
        source = cpu.to("mlu")
        torch.mlu.synchronize()
        keys = [self.key(), self.key()]
        self.assertEqual(self.store.batch_put_tensor(keys, [cpu, source]), [0, 0])
        targets = [
            torch.full((4096,), -1, dtype=torch.int32, device="mlu") for _ in range(3)
        ]
        targets[2] = targets[2][:-1]
        result = self.store.batch_get_tensor_into_cuda(
            [keys[0], self.key(), keys[1]], targets
        )
        self.assertEqual(result[0], 16384)
        self.assertLess(result[1], 0)
        self.assertLess(result[2], 0)
        self.assertTensor(targets[0], cpu)
        self.assertTensor(targets[1], torch.full((4096,), -1, dtype=torch.int32))
        self.assertTensor(targets[2], torch.full((4095,), -1, dtype=torch.int32))

    def test_concurrent_devices_and_repeated_imports(self):
        torch = self.torch
        keys = [self.key() for _ in range(4)]

        def worker(i):
            with torch.mlu.device(i % min(torch.mlu.device_count(), 2)):
                source = torch.full((65536,), i + 1, dtype=torch.int32, device="mlu")
                target = torch.empty_like(source)
                for _ in range(5):
                    self.assertEqual(self.store.upsert_tensor(keys[i], source), 0)
                    self.assertEqual(
                        self.store.get_tensor_into_cuda(keys[i], target), 262144
                    )
                    self.assertTensor(
                        target, torch.full((65536,), i + 1, dtype=torch.int32)
                    )

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(worker, range(4)))

    def test_raw_batch_capacity_duplicates_and_partial_errors(self):
        torch = self.torch
        size = 4096
        arena = torch.full((7, size + 8), 165, dtype=torch.uint8, device="mlu")
        arena[0, :size].fill_(11)
        arena[1, :size].fill_(22)
        torch.mlu.synchronize()
        keys = [self.key(), self.key()]
        with registered(self.store, arena):
            self.assertEqual(
                self.store.batch_put_from(
                    keys, [arena[i].data_ptr() for i in range(2)], [size] * 2
                ),
                [0, 0],
            )
            results = self.store.batch_get_into(
                [keys[0], keys[1], keys[0], self.key(), keys[1]],
                [arena[i].data_ptr() for i in range(2, 7)],
                [size + 8, size, size, size, size - 1],
            )
            self.assertEqual(results[:3], [size] * 3)
            self.assertTrue(all(result < 0 for result in results[3:]))
            self.assertEqual(self.store.batch_get_into([], [], []), [])
        actual = arena.cpu()
        for i, value in [(2, 11), (3, 22), (4, 11)]:
            self.assertTrue(bool(torch.all(actual[i, :size] == value)))
            self.assertTrue(bool(torch.all(actual[i, size:] == 165)))
        self.assertTrue(bool(torch.all(actual[5:] == 165)))

    def test_raw_batch_larger_than_staging_pool(self):
        # Run the remote-provider suite with a 4 MiB service pool to exercise
        # deferred reads; the eight-object batch is larger than that pool.
        torch = self.torch
        size = 1 << 20
        arena = torch.empty((2, 8, size), dtype=torch.uint8, device="mlu")
        for i in range(8):
            arena[0, i].fill_(i + 1)
        arena[1].zero_()
        torch.mlu.synchronize()
        keys = [self.key() for _ in range(8)]
        with registered(self.store, arena):
            for i, key in enumerate(keys):
                self.assertEqual(
                    self.store.put_from(key, arena[0, i].data_ptr(), size), 0
                )
            self.assertEqual(
                self.store.batch_get_into(
                    keys, [arena[1, i].data_ptr() for i in range(8)], [size] * 8
                ),
                [size] * 8,
            )
        self.assertTensor(arena[1], arena[0].cpu())

    def test_concurrent_batch_upsert_metadata_lifetime(self):
        torch = self.torch
        keys = [[self.key() for _ in range(4)] for _ in range(8)]
        barrier = threading.Barrier(8, timeout=30)

        def worker(i):
            with torch.mlu.device(i % min(torch.mlu.device_count(), 2)):
                allocation = torch.empty((4, 4096), dtype=torch.int32, device="mlu")
                sources = [
                    allocation[j].reshape((64, 64) if j % 2 else (4096,))
                    for j in range(4)
                ]
                barrier.wait()
                for iteration in range(10):
                    value = 100 * i + iteration
                    allocation.fill_(value)
                    self.assertEqual(
                        self.store.batch_upsert_tensor(keys[i], sources), [0] * 4
                    )
                    for j, key in enumerate(keys[i]):
                        actual = self.store.get_tensor(key)
                        self.assertEqual(actual.shape, sources[j].shape)
                        self.assertEqual(actual.dtype, torch.int32)
                        self.assertTrue(bool(torch.all(actual == value)))

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(8)))

    def test_padded_tensor_header_and_staging_interoperability(self):
        torch = self.torch
        expected = torch.arange(4096, dtype=torch.int32).reshape(64, 64)
        source = expected.to("mlu")
        key = self.key()
        self.assertEqual(self.store.put_tensor(key, source), 0)
        encoded = self.store.get(key)
        data_offset = struct.unpack_from("<Q", encoded, 24)[0]
        self.assertEqual(data_offset % 512, 0)
        self.assertEqual(len(encoded), data_offset + tensor_bytes(source))
        self.assertEqual(encoded[data_offset:], expected.numpy().tobytes())
        self.assertTensor(self.store.get_tensor(key), expected)
        target = torch.empty_like(source)
        self.assertEqual(
            self.store.get_tensor_into_cuda(key, target), tensor_bytes(source)
        )
        self.assertTensor(target, expected)
        # CPU staging writes use the original compact header. Switching paths
        # for an existing key must preserve values, shapes and offset parsing.
        self.assertEqual(self.store.upsert_tensor(key, expected + 1), 0)
        self.assertTensor(self.store.get_tensor(key), expected + 1)
        self.assertEqual(self.store.upsert_tensor(key, source + 2), 0)
        self.assertTensor(self.store.get_tensor(key), expected + 2)


if __name__ == "__main__":
    unittest.main()
