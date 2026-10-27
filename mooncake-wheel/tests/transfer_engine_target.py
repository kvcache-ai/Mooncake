import os
from mooncake.engine import TransferEngine

target_server_name = os.getenv("TARGET_SERVER_NAME", "127.0.0.1:12345")
initiator_server_name = os.getenv("INITIATOR_SERVER_NAME", "127.0.0.1:12347")
metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
protocol = os.getenv("PROTOCOL", "tcp")  # Protocol type: "rdma" or "tcp"

DEFAULT_BUFFER_CAPACITY = 64 * 1024 * 1024

target = TransferEngine()
ret = target.initialize(target_server_name, metadata_server, protocol, "")
if ret != 0:
    raise RuntimeError(f"Target initialization failed with code {ret}")

buffer_addr = target.get_first_buffer_address(target_server_name)
if buffer_addr == 0:
    # No buffer registered, allocate a managed buffer as fallback
    buffer_addr = target.allocate_managed_buffer(DEFAULT_BUFFER_CAPACITY)
    if buffer_addr == 0:
        raise RuntimeError("Failed to allocate fallback buffer for target")
    print(f"Target allocated fallback buffer at {buffer_addr}")

print(f"Target server ready at {target_server_name}")

while True:
    pass
