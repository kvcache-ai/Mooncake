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
- **Framework**: Google Test (gtest)

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
from mooncake import ZmqCommunicator, ZmqSocketType

# Initialize
comm = ZmqCommunicator()
comm.initialize(pool_size=10)

# REQ/REP
rep = comm.create_socket(ZmqSocketType.REP)
comm.bind(rep, "127.0.0.1:5555")

req = comm.create_socket(ZmqSocketType.REQ)
comm.connect(req, "127.0.0.1:5555")

# Send/receive
await comm.send_data(req, b"Hello")

# PUB/SUB
pub = comm.create_socket(ZmqSocketType.PUB)
sub = comm.create_socket(ZmqSocketType.SUB)

comm.bind(pub, "127.0.0.1:5556")
comm.connect(sub, "127.0.0.1:5556")
comm.subscribe(sub, "topic")

def callback(source, data, topic):
    print(f"Received: {data}")

comm.set_receive_callback(sub, callback)
```

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

## Future Work

- Additional patterns (ROUTER/DEALER, XPUB/XSUB)
- Message durability and persistence
- Multi-hop routing
- Enhanced security features
