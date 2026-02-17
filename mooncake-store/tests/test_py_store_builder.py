import os
import sys
import time
import subprocess
import shutil
import unittest
from pathlib import Path

# Ensure we can import the built pybind module 'store'
# It is produced at build/mooncake-integration/store*.so
REPO_ROOT = Path(__file__).resolve().parents[2]
# Prefer system libstdc++ to avoid GLIBCXX mismatches with conda envs
os.environ["LD_LIBRARY_PATH"] = \
    "/usr/lib/x86_64-linux-gnu:" + os.environ.get("LD_LIBRARY_PATH", "")
BUILD_INTEGRATION_DIR = REPO_ROOT / "build" / "mooncake-integration"
if BUILD_INTEGRATION_DIR.exists():
    sys.path.insert(0, str(BUILD_INTEGRATION_DIR))

STORE_IMPORT_ERROR = None
store = None
try:
    import store  # pybind module built from mooncake-integration
except Exception as e:
    STORE_IMPORT_ERROR = (
        f"Cannot import 'store' from {BUILD_INTEGRATION_DIR}: {e}. "
        f"LD_LIBRARY_PATH={os.environ.get('LD_LIBRARY_PATH','')}")


@unittest.skipIf(STORE_IMPORT_ERROR is not None, STORE_IMPORT_ERROR)
class StoreBuilderE2ETest(unittest.TestCase):
    master_proc = None
    metadata_url = "http://127.0.0.1:8080/metadata"

    @classmethod
    def setUpClass(cls):
        if shutil.which("mooncake_master") is None:
            raise unittest.SkipTest("'mooncake_master' not found in PATH")
        # Launch master with HTTP metadata server
        # Use a small lease TTL to keep tests quick/consistent
        cmd = [
            "mooncake_master",
            "--default_kv_lease_ttl=500",
            "--enable_http_metadata_server=true",
        ]
        cls.master_proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        # Give master a moment to start
        time.sleep(1.0)

    @classmethod
    def tearDownClass(cls):
        if cls.master_proc is not None:
            try:
                cls.master_proc.terminate()
                cls.master_proc.wait(timeout=5)
            except Exception:
                cls.master_proc.kill()

    def _basic_kv_cycle(self, st):
        key = "py_builder_test_key"
        value = b"hello_builder"

        # Put
        rc = st.put(key, value)
        self.assertEqual(rc, 0, f"put failed: rc={rc}")

        # Existence
        exist = st.is_exist(key)
        self.assertEqual(exist, 1, f"is_exist unexpected: {exist}")

        # Size
        size = st.get_size(key)
        self.assertEqual(size, len(value), f"get_size unexpected: {size}")

        # Get
        got = st.get(key)
        self.assertIsInstance(got, (bytes, bytearray))
        self.assertEqual(bytes(got), value)

        # Remove
        rc = st.remove(key)
        self.assertEqual(rc, 0, f"remove failed: rc={rc}")

        # Verify non-existence
        exist = st.is_exist(key)
        self.assertIn(exist, (0, -1))  # -1 may appear if metadata not yet visible

        # Close
        st.close()

    def test_builder_with_all_parameters(self):
        # Build store with all supported parameters explicitly specified
        st = (
            store.builder()
            .local_hostname("localhost")
            .metadata_server(self.metadata_url)
            .global_segment_size(16 * 1024 * 1024)
            .local_buffer_size(16 * 1024 * 1024)
            .protocol("tcp")
            .rdma_devices("")
            .master_server_addr("127.0.0.1:50051")
            .engine(None)
            .build()
        )

        # Sanity check hostname
        host = st.get_hostname()
        self.assertTrue(isinstance(host, str) and len(host) > 0)

        # Full KV cycle
        self._basic_kv_cycle(st)

    def test_builder_minimal_parameters(self):
        # Only set the required ones; other defaults should apply
        st = (
            store.builder()
            .local_hostname("localhost")
            .metadata_server(self.metadata_url)
            .build()
        )
        self._basic_kv_cycle(st)

    def test_builder_zero_sizes(self):
        # Zero global_segment_size and local_buffer_size should be supported
        st = (
            store.builder()
            .local_hostname("localhost")
            .metadata_server(self.metadata_url)
            .global_segment_size(0)
            .local_buffer_size(0)
            .protocol("tcp")
            .build()
        )
        self._basic_kv_cycle(st)


if __name__ == "__main__":
    unittest.main(verbosity=2)
