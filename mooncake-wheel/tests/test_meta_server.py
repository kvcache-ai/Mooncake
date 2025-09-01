import os, sys, random
import time
import threading

from mooncake.store import MooncakeDistributedStore

# how to test: start mooncake_master and http_metadata_server.
# Default is tcp, if you want to use rdma, should set MC_RPC_PROTOCOL=rdma and DEVICE_NAME=rdma_xxx at first.
# ./mooncake_master
# python mooncake-wheel/mooncake/http_metadata_server.py --port 8080
# python mooncake-wheel/tests/test_meta_server.py 127.0.0.1 8

import mooncake
print(mooncake.__file__)

def test_worker(store, num_req):
  for i in range(num_req):
    store.is_exist(str(random.randint(0, 1000000)))
        
master_host = "master.mooncake.dc" if len(sys.argv) < 2 else sys.argv[1]
num_thread = 1 if len(sys.argv) < 3 else int(sys.argv[2])
# Initialize the store
store = MooncakeDistributedStore()
# Use TCP protocol by default for testing, also support rdma
protocol = os.getenv("MC_RPC_PROTOCOL", "tcp")
device_name = os.getenv("DEVICE_NAME", "eth0")
local_hostname = os.getenv("LOCAL_HOSTNAME", "127.0.0.1")
metadata_server = os.getenv("METADATA_ADDR", f"http://{master_host}:8080/metadata")
global_segment_size = 0
local_buffer_size = 512 * 1024 * 1024
master_server_address = os.getenv("MASTER_SERVER", f"{master_host}:50051")
value_length = 1 * 1024 * 1024
retcode = store.setup(local_hostname,
                      metadata_server,
                      global_segment_size,
                      local_buffer_size,
                      protocol,
                      device_name,
                      master_server_address)
if retcode:
    exit(1)
time.sleep(1)  # Give some time for initialization
start_time = time.perf_counter()
threads = []
for _ in range(num_thread):
  thread = threading.Thread(target=test_worker, args=(store, 15000))
  thread.start()
  threads.append(thread)
for thread in threads:
  thread.join()
end_time = time.perf_counter()
total_time = end_time - start_time if end_time > start_time else 0
ops_per_second = 15000 * num_thread / total_time if total_time > 0 else 0
print(f"total time: {total_time} s, QPS: {ops_per_second:.2f}")