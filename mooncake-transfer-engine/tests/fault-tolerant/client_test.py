import numpy as np
import zmq
from transfer_engine import MooncakeTransferEngine

def main():
    # Initialize ZMQ context and socket
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    # server_hostname = input("Enter server hostname (without port): ")
    socket.connect(f"tcp://localhost:5555")
    
    # Wait for buffer info from server
    print("Waiting for server buffer information...")
    buffer_info = socket.recv_json()
    server_session_id = buffer_info["session_id"]
    server_ptr = buffer_info["ptr"]
    server_len = buffer_info["len"]
    print(f"Received server info - Session ID: {server_session_id}")
    print(f"Server buffer address: {server_ptr}, length: {server_len}")
    
    # Initialize client engine
    client_engine = MooncakeTransferEngine(
        hostname="localhost:10011",
        gpu_id=0,  # Using GPU 0
        ib_device=None  # No specific IB device
    )
    
    # Allocate and initialize client buffer (1MB)
    client_buffer = np.ones(1024 * 1024, dtype=np.uint8)  # Fill with ones
    client_ptr = client_buffer.ctypes.data
    client_len = client_buffer.nbytes
    
    # Register memory with Mooncake
    client_engine.register(client_ptr, client_len)
    print(f"Client initialized with session ID: {client_engine.get_session_id()}")
    
    # Transfer data from client to server
    print("Transferring data to server...")
    while True:
        ret = client_engine.transfer_sync(
            server_session_id,
            client_ptr,
            server_ptr,
            min(client_len, server_len)  # Transfer minimum of both lengths
        )
    
        if ret >= 0:
            print("Transfer successful!")
        else:
            print("Transfer failed!")
    
    # Cleanup
    client_engine.deregister(client_ptr)
    socket.close()
    context.term()

if __name__ == "__main__":
    main() 