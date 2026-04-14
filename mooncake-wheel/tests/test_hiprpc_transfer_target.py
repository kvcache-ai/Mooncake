import zmq
import torch
from mooncake.engine import TransferEngine
import os
from mooncake.allocator import NVLinkAllocator

# use cross-node Hygon HCU fabric transport, need hardware support
os.environ["MC_FORCE_MNNVL"] = "1" # or os.environ["MC_FORCE_HIP"] = "1"
os.environ["MC_USE_HIP_IPC"] = "0"

def main():
    torch.cuda.set_device(0) # Use GPU 0 for server
    allocator = NVLinkAllocator.get_allocator(0)
    custom_mem_pool = torch.cuda.MemPool(allocator.allocator())
    # Initialize ZMQ context and socket
    context = zmq.Context()
    send_socket = context.socket(zmq.PUSH)
    send_socket.bind("tcp://*:5555")  # Bind to port 5555 for buffer info

    recv_socket = context.socket(zmq.PULL)
    recv_socket.bind("tcp://*:5556")  # Bind to port 5556

    HOSTNAME = "172.17.113.140" # localhost for simple demo, replace with actual IP if needed
    METADATA_SERVER = "P2PHANDSHAKE" # [ETCD_SERVER_URL, P2PHANDSHAKE, ...]
    PROTOCOL = "rdma" # [rdma, tcp, ...]
    DEVICE_NAME = "" # auto discovery if empty

    # Initialize server engine
    server_engine = TransferEngine()
    ret_value = server_engine.initialize(
        HOSTNAME,
        METADATA_SERVER,
        PROTOCOL,
        DEVICE_NAME
    )
    assert ret_value == 0, "Failed to initialize Mooncake server engine."
    session_id = f"{HOSTNAME}:{server_engine.get_rpc_port()}"

    with torch.cuda.use_mem_pool(custom_mem_pool):
        # Allocate memory on server side (1MB buffer)
        server_buffer = torch.full((1024 * 1024,), 77, dtype=torch.uint8, device="cuda:0")
        server_ptr = server_buffer.data_ptr()
        server_len = server_buffer.nbytes

    torch.cuda.synchronize(0)  # Ensure memory is allocated and initialized before registration
    # Register memory with Mooncake
    ret_value = server_engine.register_memory(server_ptr, server_len)
    if ret_value != 0:
        print("Mooncake memory registration failed.")
        raise RuntimeError("Mooncake memory registration failed.")

    print(f"Server initialized with session ID: {session_id}")
    print(f"Server buffer address: {server_ptr}, length: {server_len}")

    # Send buffer info to client
    buffer_info = {
        "session_id": session_id,
        "ptr": server_ptr,
        "len": server_len
    }
    send_socket.send_json(buffer_info)
    print("Buffer information sent to client")

    # Keep server running
    transfer_status = recv_socket.recv_json()
    if transfer_status.get("status") == "transfer_complete":
        print("Data transfer from client to server completed.")

    expect_val = 92
    is_correct = torch.all(server_buffer == expect_val).item()
    if is_correct:
        print("Data verification successful! All values are correct.")
    else:
        print("Data verification failed! Buffer values do not match expected value.")
    # Cleanup
    ret_value = server_engine.unregister_memory(server_ptr)
    if ret_value != 0:
        print("Mooncake memory deregistration failed.")
        raise RuntimeError("Mooncake memory deregistration failed.")

    send_socket.close()
    recv_socket.close()
    context.term()

if __name__ == "__main__":
    main()
