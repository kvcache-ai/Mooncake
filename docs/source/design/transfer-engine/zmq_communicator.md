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
from engine import ZmqInterface, ZmqSocketType, ZmqSocketOption, ZmqConfig

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

# Connection management
zmq.disconnect(sub, "127.0.0.1:5556")
zmq.unbind(pub, "0.0.0.0:5556")

# Cleanup
zmq.close_socket(req)
zmq.close_socket(rep)
zmq.shutdown()
```

**Note:** 
- Module name is `engine` (built as `engine.cpython-*.so`)
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
| `socket.recv()` | Callback-based: `set_receive_callback()` |
| `socket.subscribe(topic)` | `subscribe(socket_id, topic)` |
| `socket.unsubscribe(topic)` | `unsubscribe(socket_id, topic)` |

## Future Work

- Additional patterns (ROUTER/DEALER, XPUB/XSUB)
- Message durability and persistence
- Multi-hop routing
- Enhanced security features
- Socket polling and multiplexing
- Multipart messages
- Message properties and metadata
