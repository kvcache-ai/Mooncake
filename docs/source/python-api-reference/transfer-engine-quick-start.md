# Transfer Engine Developer Quick Start

This guide is for developers who need to call Transfer Engine APIs directly.
Most vLLM and SGLang users should configure Mooncake through their serving
framework instead of using this low-level API.

The example below shows a minimal two-process Transfer Engine transfer over TCP.
It uses only the Python standard library plus the Mooncake package.

## Installation

Install the Mooncake Transfer Engine package from PyPI:

**For CUDA-enabled systems:**

```bash
pip install mooncake-transfer-engine
```

Package details: [mooncake-transfer-engine](https://pypi.org/project/mooncake-transfer-engine/)

**For non-CUDA systems:**

```bash
pip install mooncake-transfer-engine-non-cuda
```

Package details: [mooncake-transfer-engine-non-cuda](https://pypi.org/project/mooncake-transfer-engine-non-cuda/)

> **Note**: The CUDA version includes Mooncake-EP and GPU topology detection,
> requiring CUDA 12.1+. The non-CUDA version is for environments without CUDA
> dependencies, but it still requires the system runtime libraries used by the
> transfer stack. On Ubuntu, install them with:
>
> ```bash
> sudo apt-get update && sudo apt-get install -y libcurl4 libibverbs1 rdma-core librdmacm1 libnuma1 liburing2
> ```

> **Note**: When using RDMA, you may need to run with `sudo` or configure the
> required device permissions.

## Start the Receiver

Save the following as `receiver.py`, then run it in the first terminal:

```python
import ctypes
import json
import socket

from mooncake.engine import TransferEngine


HOSTNAME = "localhost"
METADATA_SERVER = "P2PHANDSHAKE"
PROTOCOL = "tcp"
DEVICE_NAME = ""
BUFFER_SIZE = 1024 * 1024


def main():
    engine = TransferEngine()
    engine.initialize(HOSTNAME, METADATA_SERVER, PROTOCOL, DEVICE_NAME)
    session_id = f"{HOSTNAME}:{engine.get_rpc_port()}"

    server_buffer = (ctypes.c_uint8 * BUFFER_SIZE)()
    server_ptr = ctypes.addressof(server_buffer)
    server_len = ctypes.sizeof(server_buffer)

    ret = engine.register_memory(server_ptr, server_len)
    if ret != 0:
        raise RuntimeError("Mooncake memory registration failed.")

    print(f"Receiver session ID: {session_id}")
    print(f"Receiver buffer address: {server_ptr}, length: {server_len}")

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("0.0.0.0", 5555))
    listener.listen(1)

    try:
        print("Waiting for sender to connect on port 5555...")
        conn, _ = listener.accept()
        with conn:
            payload = {
                "session_id": session_id,
                "ptr": server_ptr,
                "len": server_len,
            }
            conn.sendall(json.dumps(payload).encode("utf-8") + b"\n")
            print("Buffer information sent to sender.")

        input("Press Enter after the sender finishes...")
        print(f"First byte in receiver buffer: {server_buffer[0]}")
    finally:
        ret = engine.unregister_memory(server_ptr)
        if ret != 0:
            raise RuntimeError("Mooncake memory deregistration failed.")
        listener.close()


if __name__ == "__main__":
    main()
```

## Start the Sender

Save the following as `sender.py`, then run it in a second terminal:

```python
import ctypes
import json
import socket

from mooncake.engine import TransferEngine


HOSTNAME = "localhost"
METADATA_SERVER = "P2PHANDSHAKE"
PROTOCOL = "tcp"
DEVICE_NAME = ""
BUFFER_SIZE = 1024 * 1024


def recv_json_line(sock):
    chunks = []
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        chunks.append(chunk)
        if b"\n" in chunk:
            break
    return json.loads(b"".join(chunks).split(b"\n", 1)[0].decode("utf-8"))


def main():
    with socket.create_connection(("localhost", 5555)) as sock:
        buffer_info = recv_json_line(sock)

    server_session_id = buffer_info["session_id"]
    server_ptr = buffer_info["ptr"]
    server_len = buffer_info["len"]
    print(f"Receiver session ID: {server_session_id}")
    print(f"Receiver buffer address: {server_ptr}, length: {server_len}")

    engine = TransferEngine()
    engine.initialize(HOSTNAME, METADATA_SERVER, PROTOCOL, DEVICE_NAME)
    session_id = f"{HOSTNAME}:{engine.get_rpc_port()}"

    client_buffer = (ctypes.c_uint8 * BUFFER_SIZE)()
    client_ptr = ctypes.addressof(client_buffer)
    client_len = ctypes.sizeof(client_buffer)
    ctypes.memset(client_ptr, 1, client_len)

    ret = engine.register_memory(client_ptr, client_len)
    if ret != 0:
        raise RuntimeError("Mooncake memory registration failed.")

    try:
        print(f"Sender session ID: {session_id}")
        print("Transferring data to receiver...")
        ret = engine.transfer_sync_write(
            server_session_id,
            client_ptr,
            server_ptr,
            min(client_len, server_len),
        )
        if ret < 0:
            raise RuntimeError("Transfer failed.")
        print("Transfer successful.")
    finally:
        ret = engine.unregister_memory(client_ptr)
        if ret != 0:
            raise RuntimeError("Mooncake memory deregistration failed.")


if __name__ == "__main__":
    main()
```

For more Python APIs, see [Transfer Engine Python API](transfer-engine.md).
