import os
import time
from mooncake_vllm_adaptor import MooncakeDistributedStore

def startProvider():
    # Initialize the store
    store = MooncakeDistributedStore()
    # Use TCP protocol by default for testing
    protocol = os.getenv("PROTOCOL", "rdma")
    device_name = os.getenv("DEVICE_NAME", "ibp6s0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "10.1.100.3:12355")
    metadata_server = os.getenv("METADATA_ADDR", "10.1.101.3:2379")
    global_segment_size = 3200 * 1024 * 1024
    local_buffer_size = 512 * 1024 * 1024
    master_server_address = os.getenv("MASTER_SERVER", "10.1.101.3:50051")
    retcode = store.setup(local_hostname, 
                          metadata_server, 
                          global_segment_size,
                          local_buffer_size, 
                          protocol, 
                          device_name,
                          master_server_address)
    if retcode:
        exit(1)
    time.sleep(100)  # Give some time for initialization


if __name__ == '__main__':
    startProvider()
