import os
import time
import random
from mooncake.store import MooncakeDistributedStore

# How to test
# 1. Config the following parameters in `setup`, notice all IP addresses
# 2. Run `ROLE=prefill python3 ./stress_cluster_benchmark.py` in one terminal
# 2. Run `ROLE=decode python3 ./stress_cluster_benchmark.py` in another terminal

class TestInstance:
    def setup(cls):
        cls.store = MooncakeDistributedStore()
        protocol = os.getenv("PROTOCOL", "tcp")
        device_name = os.getenv("DEVICE_NAME", "ibp6s0")
        # Changed to the local hostname (retrieved by `ip addr`) accordingly
        local_hostname = os.getenv("LOCAL_HOSTNAME", "10.1.101.3")
        metadata_server = os.getenv("METADATA_ADDR", "10.1.101.3:2379")
        global_segment_size = 3200 * 1024 * 1024
        local_buffer_size = 512 * 1024 * 1024
        master_server_address = os.getenv("MASTER_SERVER", "10.1.101.3:50051")
        cls.value_length = 1 * 1024 * 1024
        cls.max_requests = 10000
        storage_root_path = os.getenv("STORAGE_ROOT_PATH", "")
        retcode = cls.store.setup(local_hostname, 
                                  metadata_server, 
                                  global_segment_size,
                                  local_buffer_size, 
                                  protocol, 
                                  device_name,
                                  master_server_address,
                                  storage_root_path)
        if retcode:
            exit(1)
        time.sleep(1)  # Give some time for initialization
    
    def prefill(cls):
        index = 0
        value = os.urandom(cls.value_length)
        while index < cls.max_requests:
            key = "k_" + str(index)
            value = bytes([index % 256] * cls.value_length)
            retcode = cls.store.put(key, value)
            if retcode:
                print("WARNING: put failed, key", key)
            if random.randint(0, 100) < 98:
                index = index + 1
            if index % 500 == 0:
                print("completed", index, "entries");
        time.sleep(20) # wait for decode
    
    def decode(cls):
        index = 0
        while index < cls.max_requests:
            key = "k_" + str(index)
            value = cls.store.get(key)
            if len(value) == 0:
                print("WARNING: get failed, key", key)
            else:
                expected_value = bytes([index % 256] * cls.value_length)
                if value != expected_value:
                    print("WARNING: get data corrupted, key", key)
            index = index + 1
            if index % 500 == 0:
                print("completed", index, "entries");
        time.sleep(20) # wait for decode


if __name__ == '__main__':
    tester = TestInstance()
    tester.setup()
    if os.getenv("ROLE", "prefill") == "decode":
        tester.decode()
    else:
        tester.prefill()
