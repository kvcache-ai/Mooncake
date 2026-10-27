import unittest
import os
import time
import argparse
import sys
from mooncake.store import MooncakeDistributedStore

# The lease time of the kv object, should be set equal to
# the master's value.
DEFAULT_DEFAULT_KV_LEASE_TTL = 5000 # 5000 milliseconds
# Use environment variable if set, otherwise use default
default_kv_lease_ttl = int(os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL))

# Parse command line arguments for client ID
parser = argparse.ArgumentParser()
parser.add_argument('--client-id', type=str, default='default', help='Client ID for this instance')
args, unknown = parser.parse_known_args()
CLIENT_ID = args.client_id


def get_client(store, local_buffer_size_param=None):
    """Initialize and setup the distributed store client."""
    mem_pool_size = 3200 * 1024 * 1024  # 3200 MB
    local_buffer_size = (
        local_buffer_size_param if local_buffer_size_param is not None
        else 512 * 1024 * 1024  # 512 MB
    )
    real_client_address = "127.0.0.1:50052"

    retcode = store.setup_dummy(
        mem_pool_size,
        local_buffer_size,
        real_client_address
    )

    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")

class TestMultiDummyClients(unittest.TestCase):
    """Test class for multi dummy clients operations."""

    @classmethod
    def setUpClass(cls):
        """Initialize the store once for all tests."""
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)
        cls.client_id = CLIENT_ID

    def test_client_interaction(self):
        """Test basic Put/Get/Exist operations between multi dummy clients."""
        test_data = b"Hello, World!"
        key1 = f"test_multi_dummy_client_interaction_client1"
        key2 = f"test_multi_dummy_client_interaction_client2"

        # Test Put/Get/Remove interaction
        if self.client_id == 'client1':
            self.assertEqual(self.store.put(key1, test_data), 0)
            start_time = time.time()
            timeout = 10  # 10 seconds timeout
            while True:
                time.sleep(0.1)
                if self.store.is_exist(key2):
                    # Check key2 exists
                    self.assertEqual(self.store.get_size(key2), len(test_data))
                    self.assertEqual(self.store.get(key2), test_data)
                    # Put again with the same key, should succeeded
                    self.assertEqual(self.store.put(key2, test_data), 0)
                    # Remove key2
                    time.sleep(default_kv_lease_ttl / 1000)
                    self.assertEqual(self.store.remove(key2), 0)
                    break
                if time.time() - start_time > timeout:
                    raise RuntimeError("timeout waiting for key2 to appear")
        else:
            start_time = time.time()
            timeout = 10  # 10 seconds timeout
            while True:
                time.sleep(0.1)
                if self.store.is_exist(key1):
                    # Check key1 exists
                    self.assertEqual(self.store.get_size(key1), len(test_data))
                    self.assertEqual(self.store.get(key1), test_data)
                    # Put again with the same key, should succeeded
                    self.assertEqual(self.store.put(key1, test_data), 0)
                    # Remove key1
                    time.sleep(default_kv_lease_ttl / 1000)
                    self.assertEqual(self.store.remove(key1), 0)
                    # Put key2
                    self.assertEqual(self.store.put(key2, test_data), 0)
                    break
                if time.time() - start_time > timeout:
                    raise RuntimeError("timeout waiting for key1 to appear")

if __name__ == '__main__':
    # Show which test is running; stop on first failure
    sys.argv = ['test_multi_dummy_clients.py'] + unknown
    unittest.main(verbosity=2, failfast=True)