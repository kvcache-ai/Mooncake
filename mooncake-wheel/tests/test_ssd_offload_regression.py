import hashlib
import os
import time
import unittest

from mooncake.store import MooncakeDistributedStore


VALUE_SIZE = 1024 * 1024
OBJECT_COUNT = 768
GLOBAL_SEGMENT_SIZE = 256 * 1024 * 1024
LOCAL_BUFFER_SIZE = 128 * 1024 * 1024
PUT_TIMEOUT_SECONDS = 120


def make_value(key):
    digest = hashlib.sha256(key.encode()).digest()
    return (digest * (VALUE_SIZE // len(digest) + 1))[:VALUE_SIZE]


class TestSsdOffloadRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        result = cls.store.setup(
            os.getenv("LOCAL_HOSTNAME", "localhost"),
            os.getenv("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata"),
            GLOBAL_SEGMENT_SIZE,
            LOCAL_BUFFER_SIZE,
            os.getenv("PROTOCOL", "tcp"),
            os.getenv("DEVICE_NAME", "eth0"),
            os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
            None,
            True,
            os.environ["MOONCAKE_OFFLOAD_FILE_STORAGE_PATH"],
        )
        if result != 0:
            raise RuntimeError(f"Failed to setup store client: {result}")

    def test_values_survive_ssd_offload_on_eviction(self):
        keys = [f"ssd-offload-regression-{index}" for index in range(OBJECT_COUNT)]

        for key in keys:
            deadline = time.monotonic() + PUT_TIMEOUT_SECONDS
            value = make_value(key)
            while True:
                result = self.store.put(key, value)
                if result == 0:
                    break
                if result != -200 or time.monotonic() >= deadline:
                    self.fail(f"Put failed for {key}: {result}")
                time.sleep(0.01)

        time.sleep(5)

        for key in keys:
            self.assertEqual(
                self.store.get(key),
                make_value(key),
                f"SSD offload returned invalid data for {key}",
            )

        for key in keys:
            self.store.remove(key)


if __name__ == "__main__":
    unittest.main()
