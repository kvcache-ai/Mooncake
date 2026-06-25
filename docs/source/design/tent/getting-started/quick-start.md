# TENT Quick Start Guide

This guide will help you get started with TENT (Transfer Engine NEXT) using the C++ API. For the Python API, see [Transfer Engine Python API](../../../../python-api-reference/transfer-engine.md).

## Prerequisites

- **Hardware**: RDMA NIC (recommended), or any supported network interface
- **OS**: Linux with kernel 4.18+
- **Compiler**: GCC 9+ or Clang 10+
- **Dependencies**: See [Build Guide](../../getting_started/build.md)

## Building TENT

TENT is built with the `-DUSE_TENT=ON` flag:

```bash
cd mooncake-transfer-engine
mkdir build && cd build
cmake -DUSE_TENT=ON ..
make -j$(nproc)
```

## Your First TENT Program

### Minimal Example

This example shows the simplest possible TENT program:

```cpp
#include "tent/transfer_engine.h"
#include <iostream>

using namespace mooncake::tent;

int main() {
    // 1. Create engine with configuration
    TransferEngine engine("./transfer-engine.json");

    // 2. Check if engine is available
    if (!engine.available()) {
        std::cerr << "Engine not available" << std::endl;
        return -1;
    }

    // 3. Get local segment name
    std::cout << "Local segment: " << engine.getSegmentName() << std::endl;

    return 0;
}
```

### Configuration File

Create `transfer-engine.json` in your working directory:

```json
{
  "metadata_type": "p2p",
  "metadata_servers": "127.0.0.1:2379"
}
```

### Compile and Run

```bash
g++ -std=c++17 -I/path/to/mooncake-transfer-engine \
    your_program.cpp -o your_program \
    -L/path/to/build -ltent -lstdc++

# Or if using cmake
cmake -DUSE_TENT=ON ..
make
./your_program
```

## Complete Client-Server Example

### Server Code (receiver.cpp)

```cpp
#include "tent/transfer_engine.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace mooncake::tent;

int main() {
    // Create and initialize engine
    TransferEngine engine("./transfer-engine.json");

    if (!engine.available()) {
        std::cerr << "Failed to initialize engine" << std::endl;
        return -1;
    }

    std::cout << "Server started" << std::endl;
    std::cout << "Segment name: " << engine.getSegmentName() << std::endl;
    std::cout << "RPC server: " << engine.getRpcServerAddress()
              << ":" << engine.getRpcServerPort() << std::endl;

    // Allocate and register memory (10MB)
    constexpr size_t BUFFER_SIZE = 10 * 1024 * 1024;
    void* buffer = nullptr;
    auto status = engine.allocateLocalMemory(buffer, BUFFER_SIZE);

    if (!status.ok()) {
        std::cerr << "Failed to allocate memory: " << status.toString() << std::endl;
        return -1;
    }

    std::cout << "Allocated " << BUFFER_SIZE << " bytes at " << buffer << std::endl;

    // Keep server running
    std::cout << "Press Ctrl+C to exit..." << std::endl;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Cleanup
    engine.freeLocalMemory(buffer);
    return 0;
}
```

### Client Code (sender.cpp)

```cpp
#include "tent/transfer_engine.h"
#include <iostream>
#include <cstring>
#include <thread>
#include <chrono>

using namespace mooncake::tent;

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <server_segment_name>" << std::endl;
        return -1;
    }

    const char* server_segment = argv[1];

    // Create and initialize engine
    TransferEngine engine("./transfer-engine.json");

    if (!engine.available()) {
        std::cerr << "Failed to initialize engine" << std::endl;
        return -1;
    }

    std::cout << "Client started" << std::endl;
    std::cout << "Segment name: " << engine.getSegmentName() << std::endl;

    // Allocate and register memory (1MB)
    constexpr size_t BUFFER_SIZE = 1024 * 1024;
    void* buffer = nullptr;
    auto status = engine.allocateLocalMemory(buffer, BUFFER_SIZE);

    if (!status.ok()) {
        std::cerr << "Failed to allocate memory: " << status.toString() << std::endl;
        return -1;
    }

    // Initialize buffer with pattern
    std::memset(buffer, 0xAB, BUFFER_SIZE);
    std::cout << "Allocated and initialized " << BUFFER_SIZE << " bytes" << std::endl;

    // Open server segment
    SegmentID server_handle;
    status = engine.openSegment(server_handle, server_segment);

    if (!status.ok()) {
        std::cerr << "Failed to open segment: " << status.toString() << std::endl;
        engine.freeLocalMemory(buffer);
        return -1;
    }

    std::cout << "Connected to server segment: " << server_segment << std::endl;

    // Allocate batch
    BatchID batch = engine.allocateBatch(1);
    std::cout << "Allocated batch: " << batch << std::endl;

    // Prepare transfer request
    Request req;
    req.opcode = Request::WRITE;
    req.source = buffer;
    req.target_id = server_handle;
    req.target_offset = 0;
    req.length = BUFFER_SIZE;
    req.priority = PRIO_MEDIUM;

    // Submit transfer
    status = engine.submitTransfer(batch, {req});

    if (!status.ok()) {
        std::cerr << "Failed to submit transfer: " << status.toString() << std::endl;
        engine.freeBatch(batch);
        engine.closeSegment(server_handle);
        engine.freeLocalMemory(buffer);
        return -1;
    }

    std::cout << "Transfer submitted, waiting for completion..." << std::endl;

    // Wait for completion
    TransferStatus transfer_status;
    do {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        status = engine.getTransferStatus(batch, 0, transfer_status);

        if (!status.ok()) {
            std::cerr << "Failed to get status: " << status.toString() << std::endl;
            break;
        }
    } while (transfer_status.s == TransferStatusEnum::PENDING);

    if (transfer_status.s == TransferStatusEnum::COMPLETED) {
        std::cout << "Transfer completed successfully!" << std::endl;
        std::cout << "Transferred " << transfer_status.transferred_bytes << " bytes" << std::endl;
    } else {
        std::cerr << "Transfer failed with state: "
                  << static_cast<int>(transfer_status.s) << std::endl;
    }

    // Cleanup
    engine.freeBatch(batch);
    engine.closeSegment(server_handle);
    engine.freeLocalMemory(buffer);

    return 0;
}
```

### Build and Run

```bash
# Build
cmake -DUSE_TENT=ON ..
make -j$(nproc)

# In terminal 1: Start server
./receiver

# In terminal 2: Start client (use server's segment name from output)
./sender "server_hostname:port"
```

## Advanced Usage

### Using Notifications

```cpp
// Send notification with transfer
Notification notify("transfer_complete", "chunk_1_done");
status = engine.submitTransfer(batch, {req}, notify);

// Receive notifications
std::vector<Notification> notifications;
status = engine.receiveNotification(notifications);

for (const auto& notif : notifications) {
    std::cout << "Received: " << notif.name
              << " - " << notif.message << std::endl;
}
```

### Batch Transfers

```cpp
// Allocate larger batch
BatchID batch = engine.allocateBatch(100);

// Submit multiple requests
std::vector<Request> requests;
for (int i = 0; i < 10; i++) {
    Request req;
    req.opcode = Request::WRITE;
    req.source = buffers[i];
    req.target_id = remote_segment;
    req.target_offset = i * BUFFER_SIZE;
    req.length = BUFFER_SIZE;
    req.priority = PRIO_MEDIUM;
    requests.push_back(req);
}

status = engine.submitTransfer(batch, requests);
```

### Memory Registration Options

```cpp
// Register with specific location and permission
MemoryOptions options;
options.location = "cuda:0";       // GPU memory
options.permission = Permission::kGlobalReadWrite;

void* cuda_buffer = nullptr;
status = engine.allocateLocalMemory(cuda_buffer, size, options);
```

### Progress-Driven Execution

```cpp
TransferStatus status;
do {
    // Drive one progress step
    status = engine.progressBatch(batch, status);

    // Check if completed
    if (status.s == TransferStatusEnum::COMPLETED) {
        break;
    }

    // Small delay to avoid busy waiting
    std::this_thread::sleep_for(std::chrono::microseconds(100));
} while (true);
```

## Configuration Examples

### Minimal Configuration (P2P Mode)

```json
{
  "metadata_type": "p2p",
  "metadata_servers": "127.0.0.1:2379"
}
```

### RDMA Configuration

```json
{
  "metadata_type": "p2p",
  "metadata_servers": "etcd://10.0.0.1:2379",
  "topology": {
    "rdma_whitelist": ["mlx5_0", "mlx5_1"]
  },
  "transports": {
    "rdma": {
      "enable": true,
      "workers": {
        "block_size": 65536
      }
    }
  },
  "policy": [
    {
      "name": "default",
      "segment_type": "memory",
      "transports": ["rdma", "tcp"]
    }
  ]
}
```

### Multi-Transport Configuration

```json
{
  "metadata_type": "p2p",
  "metadata_servers": "127.0.0.1:2379",
  "max_failover_attempts": 3,
  "transports": {
    "rdma": {
      "enable": true
    },
    "tcp": {
      "enable": true,
      "max_retry_count": 3
    }
  },
  "policy": [
    {
      "name": "high_priority",
      "segment_type": "memory",
      "priority": "high",
      "transports": ["rdma", "tcp"]
    },
    {
      "name": "low_priority",
      "segment_type": "memory",
      "priority": "low",
      "transports": ["tcp"]
    }
  ]
}
```

## Common Patterns

### Asynchronous Transfer Pattern

```cpp
// Submit transfer
BatchID batch = engine.allocateBatch(1);
status = engine.submitTransfer(batch, {req});

// Do other work while transfer progresses
doOtherWork();

// Check completion later
TransferStatus transfer_status;
status = engine.getTransferStatus(batch, 0, transfer_status);
```

### Retry Pattern

```cpp
constexpr int MAX_RETRIES = 3;
int retries = 0;

while (retries < MAX_RETRIES) {
    status = engine.submitTransfer(batch, {req});

    if (status.ok()) {
        break;
    }

    retries++;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}
```

### Resource Cleanup Pattern (RAII)

```cpp
// Use destructors for automatic cleanup
{
    TransferEngine engine("./transfer-engine.json");

    void* buffer = nullptr;
    engine.allocateLocalMemory(buffer, size);

    SegmentID segment;
    engine.openSegment(segment, "remote:port");

    BatchID batch = engine.allocateBatch(1);
    engine.submitTransfer(batch, {req});

    // All resources automatically cleaned up when scope ends
}
```

## Troubleshooting

### Engine Not Available

If `engine.available()` returns false:

1. Check configuration file path and syntax
2. Verify metadata server is accessible
3. Check network connectivity
4. Review logs with `log_level: debug` in config

### Segment Open Failure

If `openSegment()` fails:

1. Verify the target is running
2. Check the segment name format (`hostname:port`)
3. Ensure metadata server is reachable
4. Check firewall settings

### Transfer Failures

If transfers fail:

1. Verify memory is registered
2. Check target segment is open
3. Ensure buffers are large enough
4. Review transport configuration
5. Check system resource limits

### Performance Issues

For better performance:

1. Use batch transfers when possible
2. Reuse buffers and batches
3. Adjust `block_size` in configuration
4. Enable appropriate transports
5. Check NUMA alignment

## Next Steps

- [C++ API Reference](../api/cpp-api.md) - Complete API documentation
- [C API Reference](../api/c-api.md) - C API documentation
- [Configuration Reference](../configuration/configuration.md) - All configuration options
- [Transport Selector](../features/transport-selector.md) - Transport selection policies
- [QoS](../features/qos.md) - Quality of Service configuration
- [Failover](../features/failover.md) - Failure handling and recovery

## Getting Help

- GitHub Issues: [https://github.com/kvcache-ai/Mooncake/issues](https://github.com/kvcache-ai/Mooncake/issues)
- Documentation: [https://kvcache-ai.github.io/Mooncake/](https://kvcache-ai.github.io/Mooncake/)
