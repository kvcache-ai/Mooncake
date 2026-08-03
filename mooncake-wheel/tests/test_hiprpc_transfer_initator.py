# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

import torch
import zmq
from mooncake.engine import TransferEngine
import os
from mooncake.allocator import NVLinkAllocator

# use cross-node Hygon HCU fabric transport, need hardware support
os.environ["MC_FORCE_MNNVL"] = "1" # or os.environ["MC_FORCE_HIP"] = "1"
os.environ["MC_USE_HIP_IPC"] = "0"

def main():
    torch.cuda.set_device(1) # Use GPU 1 for client
    allocator = NVLinkAllocator.get_allocator(1)
    custom_mem_pool = torch.cuda.MemPool(allocator.allocator())
    # Initialize ZMQ context and socket
    context = zmq.Context()
    recv_socket = context.socket(zmq.PULL)
    recv_socket.connect(f"tcp://172.17.113.140:5555")

    send_socket = context.socket(zmq.PUSH)
    send_socket.connect(f"tcp://172.17.113.140:5556")

    # Wait for buffer info from server
    print("Waiting for server buffer information...")
    buffer_info = recv_socket.recv_json()
    server_session_id = buffer_info["session_id"]
    server_ptr = buffer_info["ptr"]
    server_len = buffer_info["len"]
    print(f"Received server info - Session ID: {server_session_id}")
    print(f"Server buffer address: {server_ptr}, length: {server_len}")

    # Initialize client engine
    HOSTNAME = "172.17.112.120" # localhost for simple demo, replace with actual IP if needed
    METADATA_SERVER = "P2PHANDSHAKE" # [ETCD_SERVER_URL, P2PHANDSHAKE, ...]
    PROTOCOL = "rdma" # [rdma, tcp, ...]
    DEVICE_NAME = "" # auto discovery if empty

    client_engine = TransferEngine()
    client_engine.initialize(
        HOSTNAME,
        METADATA_SERVER,
        PROTOCOL,
        DEVICE_NAME
    )
    session_id = f"{HOSTNAME}:{client_engine.get_rpc_port()}"

    with torch.cuda.use_mem_pool(custom_mem_pool):
        # Allocate and initialize client buffer (1MB)
        client_buffer = torch.full((1024 * 1024,), 92, dtype=torch.uint8, device="cuda:1")
        client_ptr = client_buffer.data_ptr()
        client_len = client_buffer.nbytes

    torch.cuda.synchronize(1) # Ensure data is ready before transfer

    # Register memory with Mooncake
    ret_value = client_engine.register_memory(client_ptr, client_len)
    if ret_value != 0:
        print("Mooncake memory registration failed.")
        raise RuntimeError("Mooncake memory registration failed.")

    print(f"Client initialized with session ID: {session_id}")

    # Transfer data from client to server
    print("Transferring data to server...")
    ret = client_engine.transfer_sync_write(
        server_session_id,
        client_ptr,
        server_ptr,
        min(client_len, server_len)  # Transfer minimum of both lengths
    )

    if ret >= 0:
        print("Transfer successful!")
    else:
        print("Transfer failed!")

    # notify server after transfer is complete
    send_socket.send_json({"status": "transfer_complete"})
    # Cleanup
    ret_value = client_engine.unregister_memory(client_ptr)
    if ret_value != 0:
        print("Mooncake memory deregistration failed.")
        raise RuntimeError("Mooncake memory deregistration failed.")

    send_socket.close()
    recv_socket.close()
    context.term()

if __name__ == "__main__":
    main()
