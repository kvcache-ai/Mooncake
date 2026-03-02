# ZMQ Communicator

## Overview

ZMQ Communicator is a ZeroMQ-style communication component implemented on the Mooncake Transfer Engine's RPC communication infrastructure, supporting common message passing patterns.

## Supported Patterns

| Pattern | Direction | Sync/Async | Load Balance | Use Case |
|---------|-----------|------------|--------------|----------|
| REQ/REP | Bidirectional | Synchronous | ❌ | RPC, Command-Response |
| PUB/SUB | Unidirectional | Asynchronous | ❌ | Broadcasting, Logging |
| PUSH/PULL | Unidirectional | Asynchronous | ✅ Round-robin | Task Distribution |
| PAIR | Bidirectional | Asynchronous | ❌ | 1-to-1 Communication |

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    ZmqCommunicator                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Socket Mgmt  │  │ RPC Pools    │  │ RPC Servers  │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────┬────────────────────────────────────────┘
                     │
     ┌───────────────┼───────────────┐
     │               │               │               │
┌────▼────┐   ┌─────▼──────┐  ┌────▼────┐    ┌────▼────┐
│ REQ/REP │   │  PUB/SUB   │  │PUSH/PULL│    │  PAIR   │
│ Pattern │   │  Pattern   │  │ Pattern │    │ Pattern │
└─────────┘   └────────────┘  └─────────┘    └─────────┘
     │               │               │               │
     └───────────────┴───────────────┴───────────────┘
                     │
          ┌──────────▼──────────┐
          │   coro_rpc Layer    │
          │  (yalantinglibs)    │
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │   async_simple      │
          │   (Coroutines)      │
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │  TCP / RDMA         │
          └─────────────────────┘
```

### Key Design Features

1. **Zero-copy Transfer**: Uses `coro_rpc` attachment mechanism for large data
2. **Pattern-based Design**: Abstract `BasePattern` with concrete implementations
3. **Async Coroutines**: Non-blocking operations with `async_simple`
4. **Connection Pooling**: Reuses RPC connections for efficiency
5. **RDMA Support**: Configurable via `MC_RPC_PROTOCOL` environment variable

## Implementation

### File Structure

```
mooncake-transfer-engine/src/transport/zmq_communicator/
├── zmq_types.h              # Data structures and enums
├── message_codec.{h,cpp}    # Message encoding/decoding
├── base_pattern.h           # Abstract pattern interface
├── req_rep_pattern.{h,cpp}  # REQ/REP implementation
├── pub_sub_pattern.{h,cpp}  # PUB/SUB implementation
├── push_pull_pattern.{h,cpp}# PUSH/PULL implementation
├── pair_pattern.{h,cpp}     # PAIR implementation
├── zmq_communicator.{h,cpp} # Main communicator class
└── zmq_interface.{h,cpp}    # Python bindings (optional)
```

### Test Location

- **Test File**: `mooncake-transfer-engine/tests/zmq_communicator_test.cpp`
- **Python Example**: `mooncake-transfer-engine/tests/zmq_communicator_example.py`
- **Framework**: Google Test (gtest) for C++, Python script for integration testing

## Building and Testing

### Build option: `MOONCAKE_BUILD_ZMQ_COMMUNICATOR`

ZMQ Communicator is built by default. To exclude it from the build (for example when libzmq is not installed or ZMQ support is not required), set the environment variable **`MOONCAKE_BUILD_ZMQ_COMMUNICATOR`** to `0`, `OFF`, `NO`, or `FALSE` before running CMake:

```bash
MOONCAKE_BUILD_ZMQ_COMMUNICATOR=0 cmake -B build
cmake --build build
```

When disabled, the ZMQ Communicator library, its unit test (`zmq_communicator_test`), and the ZMQ Python bindings in the `engine` module are not built. When enabled (default), all of them are included.

### Build

```bash
cd /root/Mooncake/build
cmake ..
make zmq_communicator_test -j4
```

### Run Tests

```bash
# Direct execution
./mooncake-transfer-engine/tests/zmq_communicator_test

# Using CTest
ctest -R zmq_communicator_test -V
```

### Test Coverage

- Message encoding/decoding (data and tensor)
- Socket operations (create, bind, connect, close)
- All communication patterns (REQ/REP, PUB/SUB, PUSH/PULL, PAIR)
- Callbacks and subscriptions
- Large tensor transfer (10 MB, ~2000 MB/s throughput)

## Advanced Features

### Socket Options

ZMQ Communicator supports various socket options to fine-tune behavior:

| Option | Description | Default |
|--------|-------------|---------|
| RCVTIMEO | Receive timeout (ms) | 5000 |
| SNDTIMEO | Send timeout (ms) | 5000 |
| SNDHWM | Send high water mark | 1000 |
| RCVHWM | Receive high water mark | 1000 |
| LINGER | Linger period for shutdown (ms) | 1000 |
| RECONNECT_IVL | Reconnection interval (ms) | 100 |
| RCVBUF | Receive buffer size (bytes) | 65536 |
| SNDBUF | Send buffer size (bytes) | 65536 |
| ROUTING_ID | Socket routing identity | - |

```cpp
// Set socket options
comm.setSocketOption(socket_id, ZmqSocketOption::RCVTIMEO, 10000);
comm.setSocketOption(socket_id, ZmqSocketOption::SNDHWM, 2000);

// Get socket options
int64_t timeout;
comm.getSocketOption(socket_id, ZmqSocketOption::RCVTIMEO, timeout);
```

### Connection Management

Enhanced connection management with unbind and disconnect:

```cpp
// Bind and unbind
comm.bind(socket_id, "127.0.0.1:5555");
comm.unbind(socket_id, "127.0.0.1:5555");

// Connect and disconnect
comm.connect(socket_id, "127.0.0.1:5556");
comm.disconnect(socket_id, "127.0.0.1:5556");

// Query socket state
bool is_bound = comm.isBound(socket_id);
bool is_connected = comm.isConnected(socket_id);
auto endpoints = comm.getConnectedEndpoints(socket_id);
auto bound_ep = comm.getBoundEndpoint(socket_id);
auto socket_type = comm.getSocketType(socket_id);
```

### Routing Identity

Set custom routing identities for advanced routing patterns:

```cpp
// Set routing ID
comm.setRoutingId(socket_id, "worker-1");
auto routing_id = comm.getRoutingId(socket_id);
```

### Python Object Serialization

Send and receive Python objects using pickle serialization (ZMQ-compatible). For
security, deserialization is disabled by default and uses a safe unpickler that
rejects globals unless explicitly overridden. This feature is only safe in
strictly trusted environments; do not expose pyobj sockets to untrusted
networks without strong authentication/integrity protections.

```python
# Send Python objects
data = {"name": "Alice", "age": 30, "skills": ["Python", "C++"]}
zmq.send_pyobj(pub, data, "user.info")

# Receive Python objects
def on_pyobj(msg):
    obj = msg['obj']        # Deserialized Python object
    topic = msg['topic']    # Topic string
    source = msg['source']  # Source address
    print(f"Received: {obj}")

zmq.set_pyobj_receive_callback(sub, on_pyobj)
```

Supported by default (safe unpickler):
- Basic types: int, float, str, bool, None
- Containers: list, tuple, dict, set

Unsafe/legacy mode (allows globals, **RCE risk**):
- Custom classes (defined at module level)
- NumPy arrays (if installed)

**Async version:**
```python
import asyncio
loop = asyncio.get_event_loop()
future = zmq.send_pyobj_async(pub, data, loop, "topic")
result = await future
```

**Security notes:**
- Enable deserialization explicitly with `MOONCAKE_ALLOW_PICKLE=1` (or
  `MC_ALLOW_PICKLE=1`).
- To allow full pickle globals, also set `MOONCAKE_ALLOW_UNSAFE_PICKLE=1` (or
  `MC_ALLOW_UNSAFE_PICKLE=1`). Only use this with trusted sources.

### Multipart Messages

Send and receive messages composed of multiple frames (ZMQ-compatible):

```python
# Send multipart message
frames = [
    b"task-123",           # Frame 1: Task ID
    b"process_image",      # Frame 2: Task type
    b"\x00\x01\x02\x03"    # Frame 3: Binary data
]
zmq.send_multipart(push, frames)

# Receive multipart message (callback mode)
def on_multipart(msg):
    frames = msg['frames']  # List of bytes
    task_id = frames[0].decode()
    task_type = frames[1].decode()
    data = frames[2]
    print(f"Task {task_id}: {task_type}, data length: {len(data)}")

zmq.set_multipart_receive_callback(pull, on_multipart)
```

**Use cases:**
- Structured messages with headers and body
- Task distribution with metadata
- Binary data with JSON metadata
- Multi-frame protocol implementations

**Async version:**
```python
future = zmq.send_multipart_async(push, frames, loop, "topic")
result = await future
```

### JSON Messages

Send and receive JSON-encoded messages (ZMQ-compatible):

```python
# Send JSON
data = {"user": "Alice", "age": 30, "role": "admin"}
zmq.send_json(pub, data, "user.info")

# Receive JSON (callback mode)
def on_json(msg):
    obj = msg['obj']        # Deserialized JSON object
    topic = msg['topic']    # Topic string
    print(f"Received: {obj}")

zmq.set_json_receive_callback(sub, on_json)

# Receive JSON (polling mode)
zmq.set_polling_mode(sub, True)
msg = zmq.recv_json(sub)  # Blocks until message arrives
obj = msg['obj']
```

**Use cases:**
- Cross-language communication (JSON is language-agnostic)
- REST API integration and web services
- Configuration data and structured messages
- Human-readable data interchange

**Comparison with send_pyobj:**
- **JSON**: Portable, slower, limited to JSON-serializable types
- **Pickle (pyobj)**: Python-only, faster, supports all picklable Python objects

**Async version:**
```python
future = zmq.send_json_async(pub, data, loop, "topic")
result = await future
```

### String Messages

Send and receive UTF-8 encoded strings (ZMQ-compatible):

```python
# Send string (UTF-8 by default)
zmq.send_string(push, "Hello, World!")
zmq.send_string(push, "中文消息")  # Unicode support

# Receive string (callback mode)
def on_string(msg):
    text = msg['string']    # Decoded string
    print(f"Received: {text}")

zmq.set_string_receive_callback(pull, on_string)

# Receive string (polling mode)
zmq.set_polling_mode(pull, True)
msg = zmq.recv_string(pull)
text = msg['string']

# Custom encoding
zmq.send_string(push, "Héllo", encoding="latin-1")
zmq.set_string_receive_callback(pull, on_string, encoding="latin-1")
```

**Use cases:**
- Log streaming and text messages
- Command and control messages
- Multilingual text communication
- Chat applications and notifications

**Encoding options:**
- `utf-8` (default) - Universal Unicode support
- `latin-1`, `gbk`, `iso-8859-1` - Legacy encodings
- Any Python-supported encoding

**Async version:**
```python
future = zmq.send_string_async(push, "text", loop, "topic")
result = await future
```

### Blocking Receive Methods (Polling Mode)

In addition to callback-based receiving, ZMQ Communicator now supports **blocking recv methods** similar to PyZMQ, enabling polling-style message reception:

```python
# Enable polling mode for a socket
zmq.set_polling_mode(socket_id, True)

# Blocking receive (waits until message arrives or timeout)
msg = zmq.recv(socket_id)
print(f"Received: {msg['data']}, from: {msg['source']}, topic: {msg['topic']}")

# Non-blocking receive (DONTWAIT flag = 1)
try:
    msg = zmq.recv(socket_id, flags=1)
except RuntimeError:
    print("No message available")
```

**Supported recv methods:**

1. **`recv(socket_id, flags=0)`** - Receive general data
   ```python
   msg = zmq.recv(sub_socket)
   data = msg['data']  # bytes
   ```

2. **`recv_pyobj(socket_id, flags=0)`** - Receive Python object
   ```python
   msg = zmq.recv_pyobj(sub_socket)
   obj = msg['obj']  # Deserialized Python object
   ```

3. **`recv_multipart(socket_id, flags=0)`** - Receive multipart message
   ```python
   msg = zmq.recv_multipart(pull_socket)
   frames = msg['frames']  # List of bytes
   ```

4. **`recv_tensor(socket_id, flags=0)`** - Receive tensor metadata
   ```python
   msg = zmq.recv_tensor(socket_id)
   shape = msg['shape']
   dtype = msg['dtype']
   ```

**Async versions:**
```python
import asyncio
loop = asyncio.get_event_loop()

# Async receive
msg = await zmq.recv_async(socket_id, loop)
msg = await zmq.recv_pyobj_async(socket_id, loop)
msg = await zmq.recv_multipart_async(socket_id, loop)
msg = await zmq.recv_tensor_async(socket_id, loop)
```

**Flags:**
- `0` - Blocking mode (default, waits for message or timeout)
- `1` - Non-blocking mode (DONTWAIT, returns immediately if no message)

**Timeout:**
- Controlled by `ZmqSocketOption.RCVTIMEO` (default: 5000ms)
- Throws `RuntimeError` on timeout

**Example - PUB/SUB with polling:**
```python
# Publisher
pub = zmq.create_socket(ZmqSocketType.PUB)
zmq.bind(pub, "0.0.0.0:5556")
zmq.start_server(pub)
zmq.publish(pub, "topic", b"Hello")

# Subscriber with polling mode
sub = zmq.create_socket(ZmqSocketType.SUB)
zmq.connect(sub, "127.0.0.1:5556")
zmq.subscribe(sub, "topic")

# Enable polling mode (required for recv)
zmq.set_polling_mode(sub, True)

# Blocking receive (like PyZMQ)
msg = zmq.recv(sub)  # Blocks until message arrives
print(f"Received: {msg['data']}")
```

**Key differences from callback mode:**
- **Callback mode**: Asynchronous, callback function is invoked when message arrives
- **Polling mode**: Synchronous, `recv()` blocks until message arrives (or timeout)
- Both modes can be used on different sockets simultaneously
- Cannot mix both modes on the same socket

## Usage

### C++ Example

```cpp
#include "transport/zmq_communicator/zmq_communicator.h"

// Initialize communicator
mooncake::ZmqCommunicator comm;
mooncake::ZmqConfig config;
config.pool_size = 10;
comm.initialize(config);

// REQ/REP Pattern
int req = comm.createSocket(mooncake::ZmqSocketType::REQ);
int rep = comm.createSocket(mooncake::ZmqSocketType::REP);

comm.bind(rep, "127.0.0.1:5555");
comm.connect(req, "127.0.0.1:5555");

// Send data
const char* data = "Hello";
auto result = async_simple::coro::syncAwait(
    comm.sendDataAsync(req, data, strlen(data))
);

// PUB/SUB Pattern
int pub = comm.createSocket(mooncake::ZmqSocketType::PUB);
int sub = comm.createSocket(mooncake::ZmqSocketType::SUB);

comm.bind(pub, "127.0.0.1:5556");
comm.connect(sub, "127.0.0.1:5556");
comm.subscribe(sub, "topic.");

// Set callback
comm.setReceiveCallback(sub, 
    [](std::string_view source, std::string_view data, 
       const std::optional<std::string>& topic) {
        // Handle received message
    }
);
```

### Python Example

```python
from mooncake.engine import ZmqInterface, ZmqSocketType, ZmqSocketOption, ZmqConfig

# Initialize with custom config
zmq = ZmqInterface()
config = ZmqConfig()
config.pool_size = 10
config.rcv_timeout_ms = 10000
config.high_water_mark = 2000
zmq.initialize(config)

# REQ/REP
rep = zmq.create_socket(ZmqSocketType.REP)
zmq.bind(rep, "0.0.0.0:5555")
zmq.start_server(rep)

req = zmq.create_socket(ZmqSocketType.REQ)
zmq.connect(req, "127.0.0.1:5555")

# Set socket options
zmq.set_socket_option(req, ZmqSocketOption.SNDTIMEO, 5000)
timeout = zmq.get_socket_option(req, ZmqSocketOption.SNDTIMEO)

# Set routing ID
zmq.set_routing_id(req, "client-1")
routing_id = zmq.get_routing_id(req)

# Check socket state
is_bound = zmq.is_bound(rep)
is_connected = zmq.is_connected(req)
endpoints = zmq.get_connected_endpoints(req)
bound_endpoint = zmq.get_bound_endpoint(rep)
socket_type = zmq.get_socket_type(req)

# Send/receive
response = zmq.request(req, b"Hello")

# PUB/SUB (Publisher binds, Subscriber connects)
pub = zmq.create_socket(ZmqSocketType.PUB)
sub = zmq.create_socket(ZmqSocketType.SUB)

zmq.bind(pub, "0.0.0.0:5556")
zmq.start_server(pub)
zmq.connect(sub, "127.0.0.1:5556")
zmq.subscribe(sub, "topic")

def callback(msg):
    print(f"Topic: {msg['topic']}, Data: {msg['data']}")

zmq.set_subscribe_callback(sub, callback)

zmq.publish(pub, "topic.sensor", b"temperature:25C")

# Python object serialization (NEW)
zmq.send_pyobj(pub, {"temperature": 25.3, "unit": "celsius"}, "sensor.data")

def on_pyobj(msg):
    print(f"Object: {msg['obj']}, Topic: {msg['topic']}")
zmq.set_pyobj_receive_callback(sub, on_pyobj)

# Multipart messages (NEW)
frames = [b"header", b"metadata", b"payload"]
zmq.send_multipart(pub, frames, "multipart.message")

def on_multipart(msg):
    print(f"Received {len(msg['frames'])} frames")
zmq.set_multipart_receive_callback(sub, on_multipart)

# Connection management
zmq.disconnect(sub, "127.0.0.1:5556")
zmq.unbind(pub, "0.0.0.0:5556")

# Blocking receive with polling mode (NEW)
sub2 = zmq.create_socket(ZmqSocketType.SUB)
zmq.connect(sub2, "127.0.0.1:5556")
zmq.subscribe(sub2, "topic")

# Enable polling mode for blocking recv
zmq.set_polling_mode(sub2, True)

# Blocking receive (like PyZMQ)
msg = zmq.recv(sub2)  # Blocks until message arrives
print(f"Received: {msg['data']}, topic: {msg['topic']}")

# Non-blocking receive with DONTWAIT flag
try:
    msg = zmq.recv(sub2, flags=1)  # Returns immediately if no message
except RuntimeError:
    print("No message available")

# Receive Python objects with blocking mode
msg = zmq.recv_pyobj(sub2)
print(f"Object: {msg['obj']}")

# Cleanup
zmq.close_socket(req)
zmq.close_socket(rep)
zmq.close_socket(sub2)
zmq.shutdown()
```

**Note:** 
- Module name is `mooncake.engine` (built as `build/mooncake-integration/engine.cpython-*.so`)
- Import with: `from mooncake.engine import ZmqInterface, ZmqSocketType, ZmqConfig`
- Requires `PYTHONPATH` to include `build/mooncake-integration`
- Endpoint format: Use `"host:port"` (e.g., `"127.0.0.1:5555"`), not `"tcp://host:port"`
- PUB/SUB: Publisher binds, Subscriber connects
- PUSH/PULL: Pull binds, Push connects

For complete examples demonstrating all patterns, see `mooncake-transfer-engine/tests/zmq_communicator_example.py`.

## Performance

### Benchmark Results

- **Local TCP**: ~2000 MB/s for 10 MB tensors
- **Zero-copy**: Attachment-based transfer for large payloads
- **Latency**: Sub-millisecond for small messages

### Optimization Tips

1. Use larger pool sizes for concurrent connections
2. Enable RDMA for better network performance: `export MC_RPC_PROTOCOL=rdma`
3. Use tensor transfer APIs for large data to leverage zero-copy

## Integration with Transfer Engine

ZMQ Communicator is built as a static library (`libzmq_communicator_lib.a`) and linked into `libtransfer_engine.a`. It reuses:

- **RPC Client Pools**: From `RpcCommunicator`
- **RPC Servers**: Shared across patterns
- **Network Stack**: TCP or RDMA via `coro_rpc`
- **Coroutine Runtime**: `async_simple` executor

## ZMQ Compatibility

### Implemented ZMQ Features

Mooncake's ZMQ Communicator implements the following ZMQ features:

1. **Socket Patterns**: REQ/REP, PUB/SUB, PUSH/PULL, PAIR
2. **Socket Options**: Timeouts, high water marks, linger, reconnection intervals, buffer sizes
3. **Connection Management**: bind, connect, unbind, disconnect
4. **State Queries**: isBound, isConnected, endpoint inspection
5. **Topic Filtering**: Subscription-based topic filtering for PUB/SUB
6. **Load Balancing**: Round-robin distribution for PUSH/PULL
7. **Zero-copy Transfer**: Large data transfer via RPC attachments
8. **Async/Sync APIs**: Both synchronous and asynchronous send/receive operations
9. **Routing Identity**: Custom socket routing IDs
10. **Tensor Support**: Native support for tensor data with shape/dtype preservation
    - **Limitation**: Maximum 4 dimensions supported (MAX_TENSOR_DIMS=4)
    - Tensors with >4 dimensions are rejected with an error message
    - This limitation ensures efficient wire format and prevents buffer overflows
11. **Python Object Serialization**: send_pyobj/recv_pyobj for automatic pickle serialization
    - Send any picklable Python object (dict, list, custom classes, etc.)
    - Automatic serialization/deserialization with pickle
    - Supports topic filtering in PUB/SUB mode
12. **Multipart Messages**: send_multipart/recv_multipart for multi-frame messages
    - Send/receive messages composed of multiple frames
    - Preserves frame order and supports empty frames
    - Useful for structured messages and protocol implementations
13. **JSON Serialization**: send_json/recv_json for JSON message interchange (ZMQ-compatible)
    - Automatic JSON encoding/decoding using Python's json module
    - Ideal for cross-language communication and structured data
    - Supports topic filtering and both callback/polling modes
14. **String Messages**: send_string/recv_string for text message handling (ZMQ-compatible)
    - UTF-8 encoding by default, supports custom encodings
    - Perfect for log streaming, commands, and multilingual text
    - Full Unicode support including CJK characters
15. **Blocking Receive Methods**: Full support for ZMQ-style blocking recv
    - `recv()` - Blocking receive for general data
    - `recv_pyobj()` - Blocking receive for Python objects
    - `recv_multipart()` - Blocking receive for multipart messages
    - `recv_json()` - Blocking receive for JSON objects
    - `recv_string()` - Blocking receive for string messages
    - `recv_tensor()` - Blocking receive for tensor metadata
    - All recv methods support flags (0=blocking, 1=DONTWAIT for non-blocking)
    - Configurable timeout via `RCVTIMEO` socket option
    - Both polling mode (recv) and callback mode (set_*_callback) supported
16. **Dual Receive Modes**:
    - **Callback Mode** (default): Asynchronous, event-driven message handling
    - **Polling Mode** (opt-in): Synchronous, blocking recv calls (ZMQ-compatible)
    - Switch modes with `set_polling_mode(socket_id, True/False)`

### Differences from ZMQ

1. **Transport Layer**: Uses coro_rpc (TCP/RDMA) instead of raw ZMQ sockets
2. **Protocol**: Custom message protocol built on RPC, not ZMTP
3. **Wire Format**: Uses RPC serialization, compatible with Mooncake Transfer Engine
4. **Performance**: Optimized for large tensor transfers with zero-copy mechanisms
5. **Integration**: Seamlessly integrated with Mooncake's distributed storage and transfer

### API Compatibility

The Python API closely follows ZMQ's design patterns:

| ZMQ Method | Mooncake Equivalent |
|------------|---------------------|
| `zmq.Context()` | `ZmqInterface()` |
| `ctx.socket(TYPE)` | `create_socket(TYPE)` |
| `socket.bind(endpoint)` | `bind(socket_id, endpoint)` |
| `socket.connect(endpoint)` | `connect(socket_id, endpoint)` |
| `socket.unbind(endpoint)` | `unbind(socket_id, endpoint)` |
| `socket.disconnect(endpoint)` | `disconnect(socket_id, endpoint)` |
| `socket.setsockopt(opt, val)` | `set_socket_option(socket_id, opt, val)` |
| `socket.getsockopt(opt)` | `get_socket_option(socket_id, opt)` |
| `socket.send(data)` | `send(socket_id, data)` / `push(socket_id, data)` |
| `socket.recv()` | **Polling mode:** `recv(socket_id)`<br>**Callback mode:** `set_receive_callback()` |
| `socket.recv(flags=zmq.DONTWAIT)` | `recv(socket_id, flags=1)` |
| `socket.subscribe(topic)` | `subscribe(socket_id, topic)` |
| `socket.unsubscribe(topic)` | `unsubscribe(socket_id, topic)` |
| `socket.send_pyobj(obj)` | `send_pyobj(socket_id, obj, topic="")` |
| `obj = socket.recv_pyobj()` | **Polling mode:** `recv_pyobj(socket_id)`<br>**Callback mode:** `set_pyobj_receive_callback()` |
| `socket.send_multipart(frames)` | `send_multipart(socket_id, frames, topic="")` |
| `frames = socket.recv_multipart()` | **Polling mode:** `recv_multipart(socket_id)`<br>**Callback mode:** `set_multipart_receive_callback()` |
| `socket.send_json(obj)` | `send_json(socket_id, obj, topic="")` |
| `obj = socket.recv_json()` | **Polling mode:** `recv_json(socket_id)`<br>**Callback mode:** `set_json_receive_callback()` |
| `socket.send_string(str)` | `send_string(socket_id, str, topic="", encoding="utf-8")` |
| `str = socket.recv_string()` | **Polling mode:** `recv_string(socket_id, encoding="utf-8")`<br>**Callback mode:** `set_string_receive_callback()` |
| N/A | `set_polling_mode(socket_id, True/False)` - Switch between polling and callback modes |

## Testing and Validation

### Test Files

The ZMQ Communicator includes comprehensive tests to validate all functionality:

#### 1. C++ Unit Tests (`zmq_communicator_test.cpp`)

Google Test framework-based tests covering:
- Message encoding/decoding
- Socket operations
- Pattern implementations
- Tensor transfer
- Large data transfer (10 MB+)

**Run:**
```bash
cd /root/Mooncake/build
make zmq_communicator_test
./mooncake-transfer-engine/tests/zmq_communicator_test
```

#### 2. Python Test Suite (`zmq_communicator_example.py`) ⭐

**Main test file** - Comprehensive examples and functional tests:
- All socket patterns (REQ/REP, PUB/SUB, PUSH/PULL, PAIR)
- Callback-based receiving (asynchronous)
- Polling-based receiving (blocking, ZMQ-compatible)
- Python object serialization (send_pyobj/recv_pyobj)
- Multipart messages (send_multipart/recv_multipart)
- JSON serialization (send_json/recv_json) ✨ NEW
- String encoding (send_string/recv_string) ✨ NEW
- Socket options and configuration

**Environment setup:**
```bash
# Activate your environment (if using conda/venv)
conda activate your_env  # or: source venv/bin/activate

# Set PYTHONPATH
cd /root/Mooncake
export PYTHONPATH=build/mooncake-integration:$PYTHONPATH
```

**Run all tests:**
```bash
python3 mooncake-transfer-engine/tests/zmq_communicator_example.py
```

**Run specific test:**
```bash
# List available tests
python3 mooncake-transfer-engine/tests/zmq_communicator_example.py --list

# Run specific test
python3 mooncake-transfer-engine/tests/zmq_communicator_example.py --test req-rep
python3 mooncake-transfer-engine/tests/zmq_communicator_example.py --test recv-polling
python3 mooncake-transfer-engine/tests/zmq_communicator_example.py --test pyobj
python3 mooncake-transfer-engine/tests/zmq_communicator_example.py --test multipart
```

**Available tests:**
- `req-rep` - REQ/REP request-response pattern
- `pub-sub` - PUB/SUB publish-subscribe pattern
- `push-pull` - PUSH/PULL pipeline pattern
- `pair` - PAIR exclusive pair pattern
- `pyobj` - Python object serialization
- `multipart` - Multipart messages
- `json` - JSON serialization ✨ NEW
- `string` - String encoding/decoding ✨ NEW
- `recv-polling` - Blocking receive (polling mode)
- `recv-pyobj` - Blocking receive for Python objects
- `recv-multipart` - Blocking receive for multipart messages
- `recv-json` - Blocking receive for JSON ✨ NEW
- `recv-string` - Blocking receive for strings ✨ NEW

#### 3. Performance Testing Tool (`zmq_communicator_bandwidth_test.py`)

Bandwidth measurement for all patterns:
- REQ/REP performance
- PUB/SUB performance
- PUSH/PULL performance
- PAIR performance
- Multi-threaded testing

**Examples:**
```bash
# Test PUSH/PULL pattern
# Terminal 1 (server):
python3 zmq_communicator_bandwidth_test.py push-pull puller --url 0.0.0.0:9007

# Terminal 2 (client):
python3 zmq_communicator_bandwidth_test.py push-pull pusher --url 127.0.0.1:9007 --threads 4 --data-size 10

# Test PUB/SUB pattern
# Terminal 1 (publisher):
python3 zmq_communicator_bandwidth_test.py pub-sub publisher --url 0.0.0.0:9006 --threads 4

# Terminal 2 (subscriber):
python3 zmq_communicator_bandwidth_test.py pub-sub subscriber --url 127.0.0.1:9006
```

### Quick Start

**Verify installation:**
```bash
cd /root/Mooncake
PYTHONPATH=build/mooncake-integration:$PYTHONPATH \
    python3 mooncake-transfer-engine/tests/zmq_communicator_example.py
```

This will run all tests and display a summary at the end.

### Test Coverage

- ✓ All socket patterns (REQ/REP, PUB/SUB, PUSH/PULL, PAIR)
- ✓ Callback-based receiving
- ✓ Polling-based receiving (blocking recv)
- ✓ Python object serialization (pickle)
- ✓ Multipart messages
- ✓ JSON serialization ✨ NEW
- ✓ String encoding ✨ NEW
- ✓ Async operations
- ✓ Socket options
- ✓ Connection management
- ✓ Large tensor transfer

### Troubleshooting

#### Build Issues

```bash
# Rebuild the engine module
cd /root/Mooncake/build
make engine -j4
```

Module built as: `build/mooncake-integration/engine.cpython-313-x86_64-linux-gnu.so`

#### Import Errors

```bash
# Make sure PYTHONPATH is set correctly
export PYTHONPATH=/root/Mooncake/build/mooncake-integration:$PYTHONPATH
```

#### Test Failures

- Make sure the engine module is built: `cd build && make engine -j4`
- Check that no other tests are using the same ports
- Some patterns (especially PUB/SUB) may need longer connection time

#### "rpc function not registered" Error

This is a known issue with PUSH/PULL pattern when handlers are registered before server is fully started. The recv functionality is correctly implemented, but the underlying RPC registration needs to be fixed separately.

## Implementation Details

### Recv Implementation

The ZMQ Communicator provides comprehensive recv functionality matching PyZMQ's API:

#### Core Recv Functions

**Blocking Recv Methods:**
- `recv(socket_id, flags=0)` - Receive general data
- `recvTensor(socket_id, flags=0)` - Receive tensor metadata
- `recvPyobj(socket_id, flags=0)` - Receive Python objects (pickle)
- `recvMultipart(socket_id, flags=0)` - Receive multipart messages
- `recvJson(socket_id, flags=0)` - Receive JSON objects ✨ NEW
- `recvString(socket_id, flags=0, encoding="utf-8")` - Receive string messages ✨ NEW

**Async Recv Methods:**
- `recvAsync(socket_id, loop, flags=0)` - Async receive data
- `recvTensorAsync(socket_id, loop, flags=0)` - Async receive tensor
- `recvPyobjAsync(socket_id, loop, flags=0)` - Async receive Python object
- `recvMultipartAsync(socket_id, loop, flags=0)` - Async receive multipart
- `recvJsonAsync(socket_id, loop, flags=0)` - Async receive JSON ✨ NEW
- `recvStringAsync(socket_id, loop, flags=0, encoding="utf-8")` - Async receive string ✨ NEW

**Mode Control:**
- `setPollingMode(socket_id, enable)` - Switch between callback and polling mode

#### Message Queue System

**Internal structures:**
- `ReceivedMessage` struct - Unified message container for all types (DATA, TENSOR, PYOBJ, MULTIPART, JSON, STRING)
- `MessageQueue` struct - Thread-safe queue with condition variable
- Support for timeout control via `RCVTIMEO` socket option
- Support for non-blocking mode via `DONTWAIT` flag

**Flags:**
- `0` - Blocking mode (waits for message or timeout)
- `1` - Non-blocking mode (DONTWAIT, returns immediately)

**Return Format:**

All recv methods return Python dict with metadata:
```python
{
    'source': str,   # Source address
    'data': bytes,   # Message data (for recv)
    'obj': object,   # Deserialized object (for recv_pyobj/recv_json)
    'string': str,   # Decoded string (for recv_string)
    'frames': list,  # List of frames (for recv_multipart)
    'shape': list,   # Tensor shape (for recv_tensor)
    'dtype': str,    # Tensor dtype (for recv_tensor)
    'topic': str     # Topic string
}
```

#### Dual Receive Modes

**Callback Mode (Default):**
- Asynchronous, event-driven message handling
- Use `set_*_callback()` to register handlers
- Callbacks fired when messages arrive
- No polling required

**Polling Mode (Opt-in):**
- Synchronous, blocking recv calls
- Enable with `set_polling_mode(socket_id, True)`
- PyZMQ-compatible blocking API
- Messages queued internally until recv is called

**Key Difference:**
- Callback mode: Messages trigger callbacks immediately
- Polling mode: Messages queue until `recv()` is called
- Both modes can be used on different sockets simultaneously
- Cannot mix both modes on the same socket

#### Implementation Statistics

- **C++ Implementation**: ~600 new lines in zmq_interface.cpp
- **Python Bindings**: 16 new methods exposed to Python
- **Thread-safe**: Proper GIL handling and mutex protection
- **Zero-copy**: Uses string_view and move semantics where possible

### File Organization

```
mooncake-transfer-engine/
├── src/transport/zmq_communicator/
│   ├── zmq_types.h              # Data structures and enums
│   ├── message_codec.{h,cpp}    # Message encoding/decoding
│   ├── base_pattern.h           # Abstract pattern interface
│   ├── req_rep_pattern.{h,cpp}  # REQ/REP implementation
│   ├── pub_sub_pattern.{h,cpp}  # PUB/SUB implementation
│   ├── push_pull_pattern.{h,cpp}# PUSH/PULL implementation
│   ├── pair_pattern.{h,cpp}     # PAIR implementation
│   ├── zmq_communicator.{h,cpp} # Main communicator class
│   └── zmq_interface.{h,cpp}    # Python bindings (1542 lines)
└── tests/
    ├── zmq_communicator_test.cpp           # C++ unit tests
    ├── zmq_communicator_example.py         # Main Python test suite ★
    └── zmq_communicator_bandwidth_test.py  # Performance tool

★ Main file for testing all functionality
```

## Future Work

- Additional patterns (ROUTER/DEALER, XPUB/XSUB)
- Message durability and persistence
- Multi-hop routing
- Enhanced security features (CURVE, PLAIN authentication)
- Socket polling and multiplexing (zmq_poll equivalent)
- Message properties and metadata
- Increased tensor dimension support (>4D)
- Fix RPC handler registration issue in PUSH/PULL pattern
- Add tensor reconstruction in recv_tensor (currently only returns metadata)
- Performance optimization for high-throughput scenarios
## Compilation and Testing Guide

### For Python 3.12 Users (zmq_test environment)

**Complete build steps:**

```bash
# 1. Activate environment
conda activate zmq_test

# 2. Clean build
cd /root/Mooncake
rm -rf build
mkdir build && cd build

# 3. Configure
cmake .. -DCMAKE_BUILD_TYPE=Release

# 4. Build engine module
make engine -j$(nproc)

# 5. Verify
ls -lh mooncake-integration/engine.cpython-312-x86_64-linux-gnu.so
```

**Quick test:**

```bash
export PYTHONPATH=/root/Mooncake/build/mooncake-integration:$PYTHONPATH
python3 -c "from mooncake.engine import ZmqInterface; print('✓ Success')"
python3 mooncake-transfer-engine/tests/zmq_communicator_example.py --test req-rep
```
