# Quick Start

This document describes how to quickly start using Mooncake Transfer Engine and Mooncake Store.

## Installation

Install the Mooncake Transfer Engine package from PyPI, which includes both Mooncake Transfer Engine and Mooncake Store Python bindings:

**For CUDA-enabled systems:**
```bash
pip install mooncake-transfer-engine
```
📦 **Package Details**: [https://pypi.org/project/mooncake-transfer-engine/](https://pypi.org/project/mooncake-transfer-engine/)

**For non-CUDA systems:**
```bash
pip install mooncake-transfer-engine-non-cuda
```
📦 **Package Details**: [https://pypi.org/project/mooncake-transfer-engine-non-cuda/](https://pypi.org/project/mooncake-transfer-engine-non-cuda/)

> **Note**: The CUDA version includes Mooncake-EP and GPU topology detection, requiring CUDA 12.1+. The non-CUDA version is for environments without CUDA dependencies.

## Transfer Engine Quick Start

> **Note**: When using RDMA protocol, you may need to run with `sudo` for proper permissions.

### Start Transfer Engine Receiver (Server)

```python

import numpy as np
import zmq
from mooncake.engine import TransferEngine

def main():
    # Initialize ZMQ context and socket
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5555")  # Bind to port 5555 for buffer info

    HOSTNAME = "localhost" # localhost for simple demo
    METADATA_SERVER = "P2PHANDSHAKE" # [ETCD_SERVER_URL, P2PHANDSHAKE, ...]
    PROTOCOL = "rdma" # [rdma, tcp, ...]
    DEVICE_NAME = "" # auto discovery if empty
    
    # Initialize server engine
    server_engine = TransferEngine()
    server_engine.initialize(
        HOSTNAME,
        METADATA_SERVER,
        PROTOCOL,
        DEVICE_NAME
    )
    session_id = f"{HOSTNAME}:{server_engine.get_rpc_port()}"
    
    # Allocate memory on server side (1MB buffer)
    server_buffer = np.zeros(1024 * 1024, dtype=np.uint8)
    server_ptr = server_buffer.ctypes.data
    server_len = server_buffer.nbytes
    
    # Register memory with Mooncake
    if PROTOCOL == "rdma":
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
    socket.send_json(buffer_info)
    print("Buffer information sent to client")
    
    # Keep server running
    try:
        while True:
            input("Press Ctrl+C to exit...")
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        # Cleanup
        if PROTOCOL == "rdma":
            ret_value = server_engine.unregister_memory(server_ptr)
            if ret_value != 0:
                print("Mooncake memory deregistration failed.")
                raise RuntimeError("Mooncake memory deregistration failed.")

        socket.close()
        context.term()

if __name__ == "__main__":
    main() 
 
```

### Start Transfer Engine Sender (Client)

```python


import numpy as np
import zmq
from mooncake.engine import TransferEngine

def main():
    # Initialize ZMQ context and socket
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
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
    HOSTNAME = "localhost" # localhost for simple demo
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
    
    # Allocate and initialize client buffer (1MB)
    client_buffer = np.ones(1024 * 1024, dtype=np.uint8)  # Fill with ones
    client_ptr = client_buffer.ctypes.data
    client_len = client_buffer.nbytes
    
    # Register memory with Mooncake
    if PROTOCOL == "rdma":
        ret_value = client_engine.register_memory(client_ptr, client_len)
    if ret_value != 0:
        print("Mooncake memory registration failed.")
        raise RuntimeError("Mooncake memory registration failed.")

    print(f"Client initialized with session ID: {session_id}")

    # Transfer data from client to server
    print("Transferring data to server...")
    for _ in range(10):
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
    
    # Cleanup
    if PROTOCOL == "rdma":
        ret_value = client_engine.unregister_memory(client_ptr)
        if ret_value != 0:
            print("Mooncake memory deregistration failed.")
            raise RuntimeError("Mooncake memory deregistration failed.")

    socket.close()
    context.term()

if __name__ == "__main__":
    main() 

```

### More Examples and Documentation

Please refer to the [Transfer Engine Python API](../python-api-reference/transfer-engine.md) and [Transfer Engine](../design/transfer-engine/index.md) for more examples and documentation.

## Mooncake Store Quick Start

### Start Master (with HTTP enabled)

Enable the built-in HTTP metadata server when starting the master:

```bash
mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080
```
This exposes the metadata endpoint at `http://<host>:<port>/metadata`.

### Hello World Example

```python
from mooncake.store import MooncakeDistributedStore

# 1. Create store instance
store = MooncakeDistributedStore()

# 2. Setup with all required parameters
store.setup(
    "localhost",           # Your node's address
    "http://localhost:8080/metadata",    # HTTP metadata server
    512*1024*1024,          # 512MB segment size
    128*1024*1024,          # 128MB local buffer
    "tcp",                  # Use TCP (RDMA for high performance)
    "",                      # Leave empty; Mooncake auto-picks RDMA devices when needed
    "localhost:50051"        # Master service
)

# 3. Store data
store.put("hello_key", b"Hello, Mooncake Store!")

# 4. Retrieve data
data = store.get("hello_key")
print(data.decode())  # Output: Hello, Mooncake Store!

# 5. Clean up
store.close()
```

### More Examples and Documentation

Please refer to the [Mooncake Store Python API](../python-api-reference/mooncake-store.md), [Mooncake Store](../design/mooncake-store.md) and [Mooncake Store Deployment & Operations Guide](../deployment/mooncake-store-deployment-guide.md) for more examples and documentation.