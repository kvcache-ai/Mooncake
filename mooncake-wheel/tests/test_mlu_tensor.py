"""Real torch_mlu / C++ Store integration tests.

Build Mooncake with USE_MLU=ON and WITH_STORE=ON. Set MASTER_SERVER,
LOCAL_HOSTNAME, PROTOCOL and DEVICE_NAME as in test_put_get_tensor.py.
MC_MLU_TEST_SEGMENT_SIZE=0 makes every replica remote (a separate provider
must already be running). MC_STORE_TEST_REQUIRE_MLU=1 turns missing MLU
support into a failure instead of a skip. Run with unittest or pytest.
The caller must finish producing a tensor before handing its pointer to Store.
"""

import contextlib
import gc
import os
import subprocess
import sys
import unittest
import uuid
import weakref
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def load_mlu():
    try:
        import torch
        import torch_mlu  # noqa: F401

        if not torch.mlu.is_available():
            raise RuntimeError("torch_mlu reports no available MLU")
        return torch
    except Exception as exc:
        message = f"Real torch_mlu is required: {exc}"
        if os.getenv("MC_STORE_TEST_REQUIRE_MLU") == "1":
            raise RuntimeError(message) from exc
        raise unittest.SkipTest(message) from exc


def make_store(segment_size=None):
    from mooncake.store import MooncakeDistributedStore

    if segment_size is None:
        segment_size = int(os.getenv("MC_MLU_TEST_SEGMENT_SIZE", str(128 << 20)))
    store = MooncakeDistributedStore()
    result = store.setup(
        os.getenv("LOCAL_HOSTNAME", "127.0.0.1:0"),
        os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        segment_size,
        int(os.getenv("MC_MLU_TEST_LOCAL_BUFFER_SIZE", str(32 << 20))),
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )
    if result != 0:
        store.close()
        raise RuntimeError(f"Store setup failed: {result}")
    return store


def tensor_bytes(tensor):
    return tensor.numel() * tensor.element_size()


@contextlib.contextmanager
def registered(store, *tensors):
    """Keep Tensor owners alive until all registered pointers are removed."""
    pointers = []
    try:
        for tensor in tensors:
            size = tensor_bytes(tensor)
            if size:
                pointer = tensor.data_ptr()
                result = store.register_buffer(pointer, size)
                if result != 0:
                    raise RuntimeError(f"register_buffer failed: {result}")
                pointers.append(pointer)
        yield
    finally:
        errors = [store.unregister_buffer(pointer) for pointer in reversed(pointers)]
        if any(result != 0 for result in errors):
            raise RuntimeError(f"unregister_buffer failed: {errors}")


def expected_tensor(torch, size=4096, offset=0):
    return ((torch.arange(size, dtype=torch.int32) + offset) % 251).to(torch.uint8)


def process_roundtrip(mode, prefix):
    """Writer exits before a fresh reader is started by the parent test."""
    torch = load_mlu()
    torch.set_num_threads(1)
    store = make_store(0)
    try:
        keys = [f"{prefix}_{index}" for index in range(4)]
        expected = [expected_tensor(torch, (1 << 20) + 17, i) for i in range(4)]
        tensors = [value.to("mlu") for value in expected]
        torch.mlu.synchronize()
        if mode == "write":
            assert store.batch_put_tensor(keys, tensors) == [0] * len(keys)
        else:
            for tensor in tensors:
                tensor.zero_()
            torch.mlu.synchronize()
            with registered(store, *tensors):
                assert store.batch_get_tensor_into_cuda(keys, tensors) == [
                    tensor_bytes(value) for value in expected
                ]
                torch.mlu.synchronize()
                assert all(torch.equal(a.cpu(), b) for a, b in zip(tensors, expected))
    finally:
        store.close()
    print(f"MLU_PROCESS_{mode.upper()}_OK", flush=True)


class TestMluTensorStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.torch = load_mlu()
        cls.torch.set_num_threads(1)
        cls.store = make_store()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        self.prefix = f"mlu_tensor_{uuid.uuid4().hex}"
        self.keys = []
        self.torch.mlu.set_device(0)

    def tearDown(self):
        for key in self.keys:
            self.store.remove(key, force=True)

    def key(self, suffix):
        key = f"{self.prefix}_{suffix}"
        self.keys.append(key)
        return key

    def assertTensorEqual(self, actual, expected):
        actual = actual.cpu()
        self.assertEqual(actual.dtype, expected.dtype)
        self.assertEqual(tuple(actual.shape), tuple(expected.shape))
        self.assertTrue(self.torch.equal(actual, expected))

    def test_dtypes_and_metadata(self):
        torch = self.torch
        for dtype in (torch.float32, torch.float16, torch.bfloat16, torch.uint8):
            with self.subTest(dtype=dtype):
                expected = expected_tensor(torch, 4096).reshape(16, 16, 16).to(dtype)
                source = expected.to("mlu")
                self.assertEqual(source.device.type, "mlu")
                torch.mlu.synchronize()
                key = self.key(str(dtype))
                self.assertEqual(self.store.put_tensor(key, source), 0)
                self.assertTensorEqual(self.store.get_tensor(key), expected)
                destination = torch.empty_like(source)
                with registered(self.store, destination):
                    self.assertEqual(
                        self.store.get_tensor_into_cuda(key, destination),
                        tensor_bytes(source),
                    )
                    torch.mlu.synchronize()
                    self.assertTensorEqual(destination, expected)

    def test_batch_mixed_devices_and_upsert(self):
        torch = self.torch
        expected = [expected_tensor(torch, 8192 + i, i) for i in range(6)]
        sources = [
            value.to("mlu") if i % 2 else value for i, value in enumerate(expected)
        ]
        keys = [self.key(str(i)) for i in range(len(sources))]
        torch.mlu.synchronize()
        self.assertEqual(self.store.batch_put_tensor(keys, sources), [0] * len(keys))
        expected = [value + 1 for value in expected]
        sources = [
            value.to("mlu") if i % 2 else value for i, value in enumerate(expected)
        ]
        torch.mlu.synchronize()
        self.assertEqual(self.store.batch_upsert_tensor(keys, sources), [0] * len(keys))
        destinations = [torch.empty_like(value, device="mlu") for value in expected]
        with registered(self.store, *destinations):
            self.assertEqual(
                self.store.batch_get_tensor_into_cuda(keys, destinations),
                [tensor_bytes(value) for value in expected],
            )
            torch.mlu.synchronize()
            for actual, value in zip(destinations, expected):
                self.assertTensorEqual(actual, value)

    def test_release_source_and_reload(self):
        torch = self.torch
        expected = expected_tensor(torch, (4 << 20) + 17)
        key = self.key("released")
        gc.collect()
        torch.mlu.synchronize()
        before = torch.mlu.memory_allocated()
        source = expected.to("mlu")
        torch.mlu.synchronize()
        source_ref = weakref.ref(source)
        self.assertEqual(self.store.put_tensor(key, source), 0)
        del source
        gc.collect()
        torch.mlu.synchronize()
        self.assertIsNone(source_ref())
        self.assertEqual(torch.mlu.memory_allocated(), before)
        torch.mlu.empty_cache()
        destination = torch.full(
            expected.shape, 255, dtype=expected.dtype, device="mlu"
        )
        torch.mlu.synchronize()
        with registered(self.store, destination):
            self.assertEqual(
                self.store.get_tensor_into_cuda(key, destination), expected.numel()
            )
            torch.mlu.synchronize()
            self.assertTensorEqual(destination, expected)

    def test_pytorch_pinned_host_buffers(self):
        torch = self.torch
        expected = expected_tensor(torch, (1 << 20) + 17)
        source = expected.pin_memory(device="mlu")
        host_destination = torch.empty_like(expected).pin_memory(device="mlu")
        device_destination = torch.empty_like(source, device="mlu")
        self.assertTrue(source.is_pinned(device="mlu"))
        self.assertTrue(host_destination.is_pinned(device="mlu"))
        key = self.key("pinned")
        with registered(self.store, source, host_destination, device_destination):
            self.assertEqual(
                self.store.put_from(key, source.data_ptr(), source.numel()), 0
            )
            self.assertEqual(
                self.store.get_into(key, device_destination.data_ptr(), source.numel()),
                source.numel(),
            )
            torch.mlu.synchronize()
            self.assertTensorEqual(device_destination, expected)
            self.assertEqual(
                self.store.get_into(key, host_destination.data_ptr(), source.numel()),
                source.numel(),
            )
            self.assertTensorEqual(host_destination, expected)

    def test_registered_multi_buffers_and_offsets(self):
        torch = self.torch
        expected = [expected_tensor(torch, 8192 + i * 17, i) for i in range(3)]
        # A sliced allocation tests recognition of a non-base device pointer.
        owners = [
            torch.full((value.numel() + 64,), 255, dtype=torch.uint8, device="mlu")
            for value in expected
        ]
        sources = [owner[32:-32] for owner in owners]
        for source, value in zip(sources, expected):
            source.copy_(value)
        destinations = [torch.empty_like(source) for source in sources]
        sizes = [value.numel() for value in expected]
        key = self.key("parts")
        torch.mlu.synchronize()
        with registered(self.store, *sources, *destinations):
            self.assertEqual(
                self.store.batch_put_from_multi_buffers(
                    [key], [[value.data_ptr() for value in sources]], [sizes]
                ),
                [0],
            )
            results = self.store.batch_get_into_multi_buffers(
                [key], [[value.data_ptr() for value in destinations]], [sizes]
            )
            self.assertEqual(results, [sum(sizes)])
            torch.mlu.synchronize()
            for actual, value in zip(destinations, expected):
                self.assertTensorEqual(actual, value)
        self.assertEqual(
            self.store.get(key), b"".join(value.numpy().tobytes() for value in expected)
        )

    def test_noncontiguous_source_and_destination_validation(self):
        torch = self.torch
        expected = expected_tensor(torch, 4096).reshape(64, 64).t().contiguous()
        source = expected.t().contiguous().to("mlu").t()
        self.assertFalse(source.is_contiguous())
        torch.mlu.synchronize()
        key = self.key("strided")
        self.assertEqual(self.store.put_tensor(key, source), 0)
        self.assertTensorEqual(self.store.get_tensor(key), expected)
        destination = torch.full((64, 64), 255, dtype=torch.uint8, device="mlu")
        torch.mlu.synchronize()
        with registered(self.store, destination):
            self.assertLess(self.store.get_tensor_into_cuda(key, destination.t()), 0)
            torch.mlu.synchronize()
            self.assertTrue(torch.all(destination.cpu() == 255))
            self.assertEqual(self.store.get_tensor_into_cuda(key, destination), 4096)
            torch.mlu.synchronize()
            self.assertTensorEqual(destination, expected)

    def test_batch_errors_preserve_other_results(self):
        torch = self.torch
        expected = expected_tensor(torch)
        source = expected.to("mlu")
        key = self.key("present")
        missing = self.key("missing")
        torch.mlu.synchronize()
        self.assertEqual(self.store.put_tensor(key, source), 0)
        destinations = [
            torch.full((4096,), 255, dtype=torch.uint8, device="mlu") for _ in range(3)
        ]
        destinations[2] = torch.full((4095,), 255, dtype=torch.uint8, device="mlu")
        torch.mlu.synchronize()
        with registered(self.store, *destinations):
            result = self.store.batch_get_tensor_into_cuda(
                [key, missing, key], destinations
            )
            self.assertEqual(result[0], 4096)
            self.assertLess(result[1], 0)
            self.assertLess(result[2], 0)
            torch.mlu.synchronize()
            self.assertTensorEqual(destinations[0], expected)
            for destination in destinations[1:]:
                self.assertTrue(torch.all(destination.cpu() == 255))
            self.assertEqual(
                self.store.get_tensor_into_cuda(key, destinations[1]), 4096
            )
            torch.mlu.synchronize()
            self.assertTensorEqual(destinations[1], expected)

    def test_nondefault_stream_handoff(self):
        torch = self.torch
        expected = expected_tensor(torch, 1 << 20)
        stream = torch.mlu.Stream()
        ready = torch.mlu.Event()
        with torch.mlu.stream(stream):
            source = expected.to("mlu")
            source.add_(1)
            ready.record()
        ready.synchronize()
        key = self.key("stream")
        self.assertEqual(self.store.put_tensor(key, source), 0)
        destination = torch.empty_like(source)
        with registered(self.store, destination):
            self.assertEqual(
                self.store.get_tensor_into_cuda(key, destination), source.numel()
            )
            with torch.mlu.stream(stream):
                destination.add_(1)
            stream.synchronize()
            self.assertTensorEqual(destination, expected + 2)

    def test_concurrent_threads_and_devices(self):
        torch = self.torch
        devices = min(torch.mlu.device_count(), 2)
        if os.getenv("MC_MLU_TEST_REQUIRE_TWO_DEVICES") == "1":
            self.assertEqual(devices, 2)
        keys = [self.key(f"thread_{i}") for i in range(4)]

        def worker(index):
            with torch.mlu.device(index % devices):
                expected = expected_tensor(torch, 65536, index)
                source = expected.to("mlu")
                destination = torch.empty_like(source)
                torch.mlu.synchronize()
                with registered(self.store, destination):
                    for _ in range(10):
                        self.assertEqual(
                            self.store.upsert_tensor(keys[index], source), 0
                        )
                        self.assertEqual(
                            self.store.get_tensor_into_cuda(keys[index], destination),
                            65536,
                        )
                        torch.mlu.synchronize()
                        self.assertTensorEqual(destination, expected)

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(worker, range(4)))

    def test_writer_exit_and_fresh_reader(self):
        prefix = self.key("process")
        self.keys.extend(f"{prefix}_{index}" for index in range(4))
        # This test's parent owns a segment in local mode; the writer never
        # owns a replica, so its exit must not remove the stored payload.
        for mode in ("write", "read", "read", "read"):
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--process",
                    mode,
                    prefix,
                ],
                capture_output=True,
                text=True,
                timeout=90,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn(f"MLU_PROCESS_{mode.upper()}_OK", result.stdout)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--process":
        process_roundtrip(sys.argv[2], sys.argv[3])
    else:
        unittest.main()
