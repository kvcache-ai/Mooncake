"""
Python unit tests for AsyncGetContext pybind11 bindings.
Requires: master running or InProcMaster (we use the store setup flow).
Run from build/mooncake-integration/ directory.
"""
import sys
import os
import ctypes
import unittest

# Add build path to import store module
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
    "../../build/mooncake-integration"))

from store import MooncakeDistributedStore, AsyncGetContext, ReplicateConfig


class AsyncGetContextPythonTest(unittest.TestCase):
    """Test AsyncGetContext via Python pybind11 bindings."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        protocol = os.environ.get("PROTOCOL", "tcp")
        device_name = os.environ.get("DEVICE_NAME", "")
        master_addr = os.environ.get("MASTER_ADDR", "")

        if not master_addr:
            # Skip if no master available
            raise unittest.SkipTest("MASTER_ADDR not set, skipping Python async tests")

        rc = cls.store.setup(
            local_hostname="localhost:17850",
            metadata_server="P2PHANDSHAKE",
            global_segment_size=16 * 1024 * 1024,
            local_buffer_size=16 * 1024 * 1024,
            protocol=protocol,
            rdma_devices=device_name,
            master_server_addr=master_addr,
        )
        assert rc == 0, f"Store setup failed with rc={rc}"

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "store") and cls.store:
            cls.store.close()

    def _put_key(self, key: str, data: bytes):
        config = ReplicateConfig()
        config.replica_num = 1
        rc = self.store.put(key, data, config)
        self.assertEqual(rc, 0, f"Put failed for key {key}")

    def _alloc_registered_buffer(self, size: int):
        buf = ctypes.create_string_buffer(size)
        ptr = ctypes.addressof(buf)
        rc = self.store.register_buffer(ptr, size)
        self.assertEqual(rc, 0, "register_buffer failed")
        return buf, ptr

    def _unregister_buffer(self, ptr):
        self.store.unregister_buffer(ptr)

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_create_context(self):
        ctx = self.store.create_async_context(4)
        self.assertIsNotNone(ctx)
        self.assertEqual(ctx.in_flight, 0)

    def test_invalid_token_value(self):
        self.assertEqual(AsyncGetContext.INVALID_TOKEN, 2**64 - 1)

    def test_submit_and_wait_any(self):
        key = "py_async_test_1"
        data = b"hello from python async"
        self._put_key(key, data)

        buf, ptr = self._alloc_registered_buffer(1024)
        ctx = self.store.create_async_context(4)

        token = ctx.submit([key], [ptr], [1024])
        self.assertNotEqual(token, AsyncGetContext.INVALID_TOKEN)
        self.assertEqual(ctx.in_flight, 1)

        ret_token, results = ctx.wait_any()
        self.assertEqual(ret_token, token)
        self.assertEqual(ctx.in_flight, 0)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], 0)  # success

        # Verify data
        self.assertEqual(buf.raw[:len(data)], data)
        self._unregister_buffer(ptr)

    def test_multiple_concurrent(self):
        num_ops = 3
        keys = []
        datas = []
        for i in range(num_ops):
            k = f"py_async_concurrent_{i}"
            d = f"concurrent_data_{i}".encode()
            self._put_key(k, d)
            keys.append(k)
            datas.append(d)

        ctx = self.store.create_async_context(4)
        bufs = []
        ptrs = []
        token_map = {}

        for i in range(num_ops):
            buf, ptr = self._alloc_registered_buffer(1024)
            bufs.append(buf)
            ptrs.append(ptr)
            token = ctx.submit([keys[i]], [ptr], [1024])
            self.assertNotEqual(token, AsyncGetContext.INVALID_TOKEN)
            token_map[token] = i

        self.assertEqual(ctx.in_flight, num_ops)

        for _ in range(num_ops):
            ret_token, results = ctx.wait_any()
            self.assertIn(ret_token, token_map)
            idx = token_map[ret_token]
            self.assertEqual(results[0], 0)
            self.assertEqual(bufs[idx].raw[:len(datas[idx])], datas[idx])

        self.assertEqual(ctx.in_flight, 0)
        for ptr in ptrs:
            self._unregister_buffer(ptr)

    def test_max_concurrency(self):
        for i in range(3):
            self._put_key(f"py_async_max_{i}", f"data_{i}".encode())

        ctx = self.store.create_async_context(2)
        bufs = []
        ptrs = []
        for i in range(3):
            buf, ptr = self._alloc_registered_buffer(1024)
            bufs.append(buf)
            ptrs.append(ptr)

        t1 = ctx.submit(["py_async_max_0"], [ptrs[0]], [1024])
        self.assertNotEqual(t1, AsyncGetContext.INVALID_TOKEN)
        t2 = ctx.submit(["py_async_max_1"], [ptrs[1]], [1024])
        self.assertNotEqual(t2, AsyncGetContext.INVALID_TOKEN)

        # Third should fail (full)
        t3 = ctx.submit(["py_async_max_2"], [ptrs[2]], [1024])
        self.assertEqual(t3, AsyncGetContext.INVALID_TOKEN)

        # Drain one
        ctx.wait_any()

        # Now should succeed
        t4 = ctx.submit(["py_async_max_2"], [ptrs[2]], [1024])
        self.assertNotEqual(t4, AsyncGetContext.INVALID_TOKEN)

        # Drain remaining
        ctx.wait_any()
        ctx.wait_any()
        self.assertEqual(ctx.in_flight, 0)

        for ptr in ptrs:
            self._unregister_buffer(ptr)

    def test_nonexistent_key_error_via_wait(self):
        buf, ptr = self._alloc_registered_buffer(1024)
        ctx = self.store.create_async_context(4)

        token = ctx.submit(["no_such_key_python"], [ptr], [1024])
        # H2 fix: returns valid token, errors in wait_any
        self.assertNotEqual(token, AsyncGetContext.INVALID_TOKEN)

        ret_token, results = ctx.wait_any()
        self.assertEqual(ret_token, token)
        self.assertEqual(results[0], -1)  # error

        self._unregister_buffer(ptr)

    def test_destroy_context_with_inflight(self):
        key = "py_async_destroy"
        self._put_key(key, b"destroy_test")

        buf, ptr = self._alloc_registered_buffer(1024)
        ctx = self.store.create_async_context(4)
        token = ctx.submit([key], [ptr], [1024])
        self.assertNotEqual(token, AsyncGetContext.INVALID_TOKEN)

        # Destroy without wait_any - should not hang or crash
        del ctx

        self._unregister_buffer(ptr)


if __name__ == "__main__":
    unittest.main()
