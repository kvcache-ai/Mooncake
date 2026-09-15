import os
import unittest

from mooncake.store import MooncakeDistributedStore


def store_config(global_segment_size: str = "16MB") -> dict[str, object]:
    return {
        "local_hostname": os.getenv("LOCAL_HOSTNAME", "localhost"),
        "metadata_server": os.getenv(
            "MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata"
        ),
        "master_server_addr": os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
        "protocol": os.getenv("PROTOCOL", "tcp"),
        "rdma_devices": os.getenv("DEVICE_NAME", ""),
        "global_segment_size": global_segment_size,
        "local_buffer_size": "16MB",
    }


class TestStoreSetupLifecycle(unittest.TestCase):
    def test_repeated_setup_requires_successful_close(self):
        store = MooncakeDistributedStore()
        self.addCleanup(store.close)

        self.assertEqual(store.setup(store_config()), 0)
        self.assertNotEqual(store.setup(store_config()), 0)
        self.assertNotEqual(
            store.setup_dummy(16 * 1024**2, 16 * 1024**2, "localhost:12345"),
            0,
        )
        self.assertEqual(store.health_check(), 0)

        self.assertEqual(store.close(), 0)
        self.assertEqual(store.setup(store_config()), 0)

    def test_failed_setup_must_be_closed_before_retry(self):
        store = MooncakeDistributedStore()
        self.addCleanup(store.close)

        self.assertNotEqual(store.setup(store_config("50%")), 0)
        self.assertNotEqual(store.setup(store_config()), 0)
        self.assertEqual(store.close(), 0)
        self.assertEqual(store.setup(store_config()), 0)


if __name__ == "__main__":
    unittest.main()
