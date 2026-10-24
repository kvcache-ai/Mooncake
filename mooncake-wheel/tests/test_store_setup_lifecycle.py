#!/usr/bin/env python3
"""Setup lifecycle of the MooncakeDistributedStore Python binding (#4294).

A store object holds at most one native client. setup() (positional or config
dict) and setup_dummy() are rejected with INVALID_PARAMS until close() releases
it, a failed setup leaves the store uninitialized so the caller can retry, and
close() allows a fresh setup.

Real clients use an in-process master, so no mooncake_master is needed. The
successful setup_dummy() cases need a running mooncake_client: set
MOONCAKE_DUMMY_SERVER_ADDRESS (e.g. 127.0.0.1:50052) to run them.
"""

import os
import time
import unittest

from mooncake.store import MooncakeDistributedStore

INVALID_PARAMS = -600
HC_NOT_INITIALIZED = 1
SEGMENT_SIZE = 16 * 1024 * 1024
LOCAL_BUFFER_SIZE = 16 * 1024 * 1024
# Nothing listens on this port, so setup_dummy() fails to connect.
UNREACHABLE_DUMMY_SERVER = "127.0.0.1:1"
DUMMY_SERVER = os.getenv("MOONCAKE_DUMMY_SERVER_ADDRESS")


def setup_positional(store):
    return store.setup(
        "localhost",
        "P2PHANDSHAKE",
        SEGMENT_SIZE,
        LOCAL_BUFFER_SIZE,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        "",
        enable_embedded_master=True,
    )


def setup_config(store, **overrides):
    config = {
        "local_hostname": "localhost",
        "metadata_server": "P2PHANDSHAKE",
        "global_segment_size": SEGMENT_SIZE,
        "local_buffer_size": LOCAL_BUFFER_SIZE,
        "protocol": os.getenv("PROTOCOL", "tcp"),
        "rdma_devices": os.getenv("DEVICE_NAME", ""),
        "enable_embedded_master": True,
    }
    config.update(overrides)
    return store.setup(config)


def setup_dummy(store, server_address):
    return store.setup_dummy(SEGMENT_SIZE, LOCAL_BUFFER_SIZE, server_address)


class TestStoreSetupLifecycle(unittest.TestCase):
    def setUp(self):
        self.store = MooncakeDistributedStore()
        self.addCleanup(self.store.close)

    def assert_usable(self):
        key = f"setup_lifecycle_{os.getpid()}_{time.time_ns()}"
        self.assertEqual(self.store.put(key, b"payload"), 0)
        self.assertEqual(self.store.get(key), b"payload")

    def test_repeated_setup_is_rejected_until_close(self):
        self.assertEqual(setup_positional(self.store), 0)
        # Each client has its own in-process master, so a replaced client
        # would lose this key.
        sentinel = f"setup_lifecycle_sentinel_{os.getpid()}_{time.time_ns()}"
        self.assertEqual(self.store.put(sentinel, b"kept"), 0)
        repeats = {
            "positional": setup_positional,
            "config": setup_config,
            "dummy": lambda s: setup_dummy(s, UNREACHABLE_DUMMY_SERVER),
        }
        for name, repeat in repeats.items():
            with self.subTest(repeat=name):
                self.assertEqual(repeat(self.store), INVALID_PARAMS)
                # The active client is left untouched.
                self.assertEqual(self.store.get(sentinel), b"kept")
                self.assert_usable()

        self.assertEqual(self.store.close(), 0)
        self.assertEqual(self.store.health_check(), HC_NOT_INITIALIZED)
        self.assertEqual(setup_config(self.store), 0)
        self.assert_usable()

    def test_failed_setup_can_be_retried(self):
        failures = {
            # Rejected while parsing the config, before any client state.
            "early": {"global_segment_size": 1},
            # Rejected after the master, client and segment are set up, so
            # the partially built client must be torn down.
            "late": {"enable_client_http_server": True, "client_http_port": 70000},
        }
        for name, overrides in failures.items():
            with self.subTest(failure=name):
                self.assertNotEqual(setup_config(self.store, **overrides), 0)
                self.assertEqual(self.store.health_check(), HC_NOT_INITIALIZED)
                self.assertEqual(setup_config(self.store), 0)
                self.assert_usable()
                self.assertEqual(self.store.close(), 0)

    def test_exception_during_setup_leaves_store_uninitialized(self):
        with self.assertRaises(RuntimeError):
            self.store.setup(
                "localhost",
                "P2PHANDSHAKE",
                SEGMENT_SIZE,
                LOCAL_BUFFER_SIZE,
                "tcp",
                "",
                "",
                engine=object(),
                enable_embedded_master=True,
            )
        self.assertEqual(self.store.health_check(), HC_NOT_INITIALIZED)
        self.assertEqual(setup_positional(self.store), 0)
        self.assert_usable()

    def test_close_allows_a_fresh_setup(self):
        self.assertEqual(self.store.close(), 0)
        self.assertEqual(setup_positional(self.store), 0)
        self.assertEqual(self.store.close(), 0)
        self.assertEqual(self.store.close(), 0)
        self.assertEqual(self.store.health_check(), HC_NOT_INITIALIZED)
        self.assertEqual(setup_positional(self.store), 0)
        self.assert_usable()

    def test_failed_dummy_setup_leaves_store_uninitialized(self):
        self.assertNotEqual(setup_dummy(self.store, UNREACHABLE_DUMMY_SERVER), 0)
        self.assertEqual(self.store.health_check(), HC_NOT_INITIALIZED)
        # A real client can follow the failed dummy setup.
        self.assertEqual(setup_positional(self.store), 0)
        self.assert_usable()

    @unittest.skipUnless(
        DUMMY_SERVER,
        "set MOONCAKE_DUMMY_SERVER_ADDRESS to a running mooncake_client",
    )
    def test_dummy_and_real_transitions(self):
        self.assertEqual(setup_dummy(self.store, DUMMY_SERVER), 0)
        self.assertEqual(setup_positional(self.store), INVALID_PARAMS)
        self.assertEqual(setup_dummy(self.store, DUMMY_SERVER), INVALID_PARAMS)
        self.assert_usable()

        self.assertEqual(self.store.close(), 0)
        self.assertEqual(setup_positional(self.store), 0)
        self.assertEqual(setup_dummy(self.store, DUMMY_SERVER), INVALID_PARAMS)
        self.assert_usable()

        self.assertEqual(self.store.close(), 0)
        self.assertEqual(setup_dummy(self.store, DUMMY_SERVER), 0)
        self.assert_usable()


if __name__ == "__main__":
    unittest.main()
