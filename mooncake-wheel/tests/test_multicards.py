import os
import unittest

from mooncake.store import MooncakeDistributedStore

class TestMultiCards(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize the store once for all tests."""
        cls.store = MooncakeDistributedStore()
        protocol = os.getenv("PROTOCOL", "rdma")
        # get device from auto discovery
        device_name = os.getenv("DEVICE_NAME", "")
        # Changed to the local hostname (retrieved by `ip addr`) accordingly
        local_hostname = os.getenv("LOCAL_HOSTNAME", "127.0.0.1")
        metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
        global_segment_size = 3200 * 1024 * 1024
        local_buffer_size = 512 * 1024 * 1024
        master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")
        retcode = cls.store.setup(local_hostname,
                                  metadata_server,
                                  global_segment_size,
                                  local_buffer_size,
                                  protocol,
                                  device_name,
                                  master_server_address)
        
        if retcode:
            raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")
    
    def test_two_cards_send_to_each_other(self):
        """To verify if one card can work correctly as both receiver and sender at same time
        inside same process with multicards configured via auto discovery.
        """
        index = 0
        # make value length just equal to 2 * (slice size)
        # so that two cards send data to each other at almost same time.
        value_length = 65536 * 2
        max_requests = 10
        value = os.urandom(value_length)
        while index < max_requests:
            key = "k_" + str(index)
            value = bytes([index % 256] * value_length)
            retcode = self.store.put(key, value)
            if retcode:
                raise RuntimeError("put failed, key", key)
            index += 1
            print("completed", index, "entries")  

        #remove all keys
        index = 0
        while index < max_requests:
            key = "k_" + str(index)
            self.store.remove(key)
            index = index + 1

if __name__ == '__main__':
    unittest.main()