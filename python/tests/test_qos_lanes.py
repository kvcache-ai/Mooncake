# SPDX-License-Identifier: Apache-2.0
"""CPU lane lifetime contracts using controlled engines, never hardware QoS."""

import ctypes
import importlib.util
import json
import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch


def module(name):
    path = Path(__file__).resolve().parents[1] / "mooncake" / (name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


STORE = module("qos_lane")
PD = module("qos_pd_lane")


class Engine:
    def __init__(self, port):
        self.port = port
        self.regions = {}
        self.reg_rc = self.unreg_rc = 0
        self.on_read = lambda *args: 0

    def initialize_with_ascend_resource_config(self, host, metadata, device, resource):
        self.config = json.loads(resource)
        return 0

    def get_rpc_port(self):
        return self.port

    def get_engine(self):
        return self

    def register_memory(self, ptr, size):
        if not self.reg_rc:
            self.regions[ptr] = size
        return self.reg_rc

    def unregister_memory(self, ptr):
        if not self.unreg_rc:
            self.regions.pop(ptr, None)
        return self.unreg_rc

    def batch_transfer_sync_read(self, *args):
        return self.on_read(*args)


class Store:
    def __init__(self, data):
        self.data = data
        self.closed = False

    def setup(self, **kwargs):
        self.kwargs = kwargs
        return 0

    def close(self):
        self.closed = True
        return 0

    def batch_put_from_multi_buffers(self, keys, addrs, sizes, replicate):
        for key, addresses, lengths in zip(keys, addrs, sizes):
            self.data[key] = b"".join(
                ctypes.string_at(a, n) for a, n in zip(addresses, lengths)
            )
        return [0] * len(keys)

    def batch_get_into_multi_buffers(self, keys, addrs, sizes):
        for key, addresses, lengths in zip(keys, addrs, sizes):
            offset = 0
            for addr, size in zip(addresses, lengths):
                ctypes.memmove(addr, self.data[key][offset : offset + size], size)
                offset += size
        return [sum(parts) for parts in sizes]


class LaneContracts(unittest.TestCase):
    def setUp(self):
        self.engines, self.stores, self.data = [], [], {}
        env = patch.dict(os.environ, {"ASCEND_ENABLE_USE_FABRIC_MEM": "0"})
        env.start()
        self.addCleanup(env.stop)

    def engine(self):
        engine = Engine(23000 + len(self.engines))
        self.engines.append(engine)
        return engine

    def store(self):
        store = Store(self.data)
        self.stores.append(store)
        return store

    def pool(self, store=False, contribute=True):
        if store:
            result = STORE.QosStorePool(
                qos_values=[7, 0, 3, 3],
                default_qos=3,
                resource_config={},
                setup_kwargs=dict(
                    protocol="ascend",
                    local_hostname="localhost",
                    tenant_id="team-a",
                    global_segment_size=4096 if contribute else 0,
                    local_buffer_size=512 if contribute else 0,
                ),
                engine_factory=self.engine,
                store_factory=self.store,
            )
        else:
            result = PD.QosPDPool("localhost", "", [7, 0, 3, 3], 3, {}, self.engine)
        self.addCleanup(result.close)
        return result

    def test_tenant_capacity_and_no_environment_mutation(self):
        before = dict(os.environ)
        pool = self.pool(store=True)
        self.assertEqual(len(self.engines), 3)
        self.assertEqual([s.kwargs["tenant_id"] for s in self.stores], ["team-a"] * 3)
        self.assertEqual(
            [s.kwargs["global_segment_size"] for s in self.stores], [0, 4096, 0]
        )
        self.assertEqual(
            [s.kwargs["local_buffer_size"] for s in self.stores], [0, 512, 0]
        )
        self.assertIs(pool.default_store, self.stores[1])
        self.assertEqual(dict(os.environ), before)

    def test_noncontributing_client_capacity_remains_zero(self):
        self.pool(store=True, contribute=False)
        self.assertTrue(
            all(
                s.kwargs["global_segment_size"] == s.kwargs["local_buffer_size"] == 0
                for s in self.stores
            )
        )

    def test_all_nine_cross_lane_combinations_bytes_and_guards(self):
        pool = self.pool(store=True)
        source = ctypes.create_string_buffer(bytes(range(64)))
        target = ctypes.create_string_buffer(b"x" * 65)
        src, dst = ctypes.addressof(source), ctypes.addressof(target)
        pool.register_buffers([src, dst], [65, 66])
        for put in [0, 3, 7]:
            for get in [0, 3, 7]:
                with self.subTest(put=put, get=get):
                    ctypes.memset(dst, ord("x"), 66)
                    pool.transfer(
                        put, "put", ["same-key"], [[src + 1, src + 17]], [[7, 19]]
                    )
                    pool.transfer(
                        get, "get", ["same-key"], [[dst + 1, dst + 17]], [[7, 19]]
                    )
                    self.assertEqual(target.raw[1:8], source.raw[1:8])
                    self.assertEqual(target.raw[17:36], source.raw[17:36])
                    self.assertEqual(
                        target.raw[:1] + target.raw[8:17] + target.raw[36:], b"x" * 40
                    )
        self.assertEqual(list(self.data), ["same-key"])

    def test_registration_failure_each_lane_preserves_existing(self):
        for failed in range(3):
            pool = self.pool()
            engines = self.engines[-3:]
            pool.register_buffers([100], [10])
            engines[failed].reg_rc = -1
            with self.assertRaises(RuntimeError):
                pool.register_buffers([200], [10])
            self.assertTrue(all(e.regions == {100: 10} for e in engines))
            engines[failed].reg_rc = 0
            pool.close()

    def test_rollback_failure_poisoning_then_cleanup(self):
        pool = self.pool()
        self.engines[1].reg_rc = -1
        self.engines[0].unreg_rc = -1
        with self.assertRaisesRegex(RuntimeError, "broken"):
            pool.register_buffers([100], [10])
        with self.assertRaisesRegex(RuntimeError, "broken"):
            pool.read(0, "peer:1", [100], [200], [1])
        self.engines[0].unreg_rc = 0
        pool.close()
        self.assertTrue(all(not e.regions for e in self.engines))

    def test_overlap_overflow_and_size_change(self):
        pool = self.pool()
        pool.register_buffers([100], [10])
        pool.register_buffers([100], [10])
        for ptr, size in [(100, 11), (101, 2), ((1 << 64) - 1, 2), (True, 1)]:
            with self.assertRaises(ValueError):
                pool.register_buffers([ptr], [size])
        self.assertTrue(all(e.regions == {100: 10} for e in self.engines))

    def test_cross_lane_progress_and_close_waits_for_native_return(self):
        pool = self.pool()
        pool.register_buffers([100], [10])
        entered, release, other = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )

        def blocked(*args):
            entered.set()
            if not release.wait(2):
                raise TimeoutError("fixture release missing")
            return 0

        self.engines[0].on_read = blocked
        self.engines[1].on_read = lambda *a: other.set() or 0
        with ThreadPoolExecutor(max_workers=3) as executor:
            first = executor.submit(pool.read, 0, "peer:1", [100], [200], [1])
            try:
                self.assertTrue(entered.wait(1))
                second = executor.submit(pool.read, 3, "peer:3", [100], [200], [1])
                self.assertTrue(other.wait(1))
                second.result(1)
                closing = executor.submit(pool.close)
                self.assertFalse(closing.done())
                self.assertTrue(all(e.regions for e in self.engines))
            finally:
                release.set()
            first.result(1)
            closing.result(1)
        self.assertTrue(all(not e.regions for e in self.engines))

    def test_same_lane_serializes_calls(self):
        pool = self.pool()
        pool.register_buffers([100], [10])
        entered, release, second_started = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        calls = []

        def blocked(*args):
            calls.append(1)
            entered.set()
            if not release.wait(2):
                raise TimeoutError("fixture release missing")
            return 0

        self.engines[0].on_read = blocked

        def second():
            second_started.set()
            return pool.read(0, "peer:1", [100], [200], [1])

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(pool.read, 0, "peer:1", [100], [200], [1])
            try:
                self.assertTrue(entered.wait(1))
                future = executor.submit(second)
                self.assertTrue(second_started.wait(1))
                self.assertEqual(calls, [1])
            finally:
                release.set()
            first.result(1)
            future.result(1)
        self.assertEqual(calls, [1, 1])

    def test_missing_native_method_never_falls_back(self):
        with self.assertRaisesRegex(RuntimeError, "patch missing"):
            PD.QosPDPool("host", "", [0, 3, 7], 0, {}, lambda: object())

    def test_close_idempotent_and_no_use_after_close(self):
        pool = self.pool()
        pool.register_buffers([100], [10])
        pool.close()
        pool.close()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            pool.read(0, "peer:1", [100], [200], [1])


if __name__ == "__main__":
    unittest.main()
