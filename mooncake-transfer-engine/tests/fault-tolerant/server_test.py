import numpy as np
import zmq
from transfer_engine import MooncakeTransferEngine

def main():
    # Initialize ZMQ context and socket
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5555")  # Bind to port 5555 for buffer info
    
    # Initialize server engine
    server_engine = MooncakeTransferEngine(
        hostname="localhost:10010",
        gpu_id=0,  # Using GPU 0
        ib_device=None  # No specific IB device
    )
    
    # Allocate memory on server side (1MB buffer)
    server_buffer = np.zeros(1024 * 1024, dtype=np.uint8)
    server_ptr = server_buffer.ctypes.data
    server_len = server_buffer.nbytes
    
    # Register memory with Mooncake
    server_engine.register(server_ptr, server_len)
    print(f"Server initialized with session ID: {server_engine.get_session_id()}")
    print(f"Server buffer address: {server_ptr}, length: {server_len}")
    
    # Send buffer info to client
    buffer_info = {
        "session_id": server_engine.get_session_id(),
        "ptr": server_ptr,
        "len": server_len
    }
    socket.send_json(buffer_info)
    print("Buffer information sent to client")
    
    # Keep server running
    try:
        while True:
            input("Press Enter to exit...")
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        # Cleanup
        server_engine.deregister(server_ptr)
        socket.close()
        context.term()

if __name__ == "__main__":
    main() 