# TENT C++ API Reference

## Overview

This page documents the TENT C++ API in `mooncake-transfer-engine/tent/include/tent/transfer_engine.h`.

For conceptual background, see [TENT Overview](../overview.md). For migration from classic Transfer Engine, see [Migration Guide](../getting-started/migration.md).

**Prerequisites and API modes**
- **Build** with `-DUSE_TENT=ON` to enable TENT.
- TENT provides two API surfaces:
  - **TENT-native API** (documented below).
  - **TE-compatible API** via the compatibility shim (set `MC_USE_TENT=1`). For TE-compatible API, see [TE C++ API Reference](../../transfer-engine/cpp-api.md)

**Core APIs vs Advanced APIs**
- Core APIs form the minimal path to move data: create the engine, register memory, open segments, submit transfers, and query status.
- Advanced APIs are optional; they help with segment export/import, notifications, and engine introspection.

## Core APIs

### Core Usage Path (C++)

Most integrations follow a short, repeatable path: create the engine, register local memory, open a target segment, submit a batch transfer, poll status, and finally free resources. A minimal example is shown below.

```cpp
#include "tent/transfer_engine.h"

using mooncake::tent::TransferEngine;
using mooncake::tent::Request;
using mooncake::tent::TransferStatus;
using mooncake::tent::SegmentID;
using mooncake::tent::BatchID;

// Create engine (loads config from default path or environment)
TransferEngine engine;
if (!engine.available()) {
    // handle initialization failure
}

// Allocate and register local memory
void* local_addr = nullptr;
size_t length = 1024 * 1024;  // 1MB
engine.allocateLocalMemory(&local_addr, length, "cuda:0");

// Open remote segment
SegmentID remote_segment;
engine.openSegment(remote_segment, "remote_node");

// Prepare transfer request
Request req{};
req.opcode = Request::WRITE;
req.source = local_addr;
req.target_id = remote_segment;
req.target_offset = 0;
req.length = length;

// Allocate batch and submit transfer
BatchID batch = engine.allocateBatch(1);
engine.submitTransfer(batch, {req});

// Poll for completion
TransferStatus status;
do {
    engine.getTransferStatus(batch, status);
} while (status.s == mooncake::tent::PENDING);

// Cleanup
engine.freeBatch(batch);
engine.closeSegment(remote_segment);
engine.freeLocalMemory(local_addr);
```

### Constructors

```cpp
TransferEngine();
TransferEngine(const std::string config_path);
TransferEngine(std::shared_ptr<Config> config);
```

Constructs a TENT Transfer Engine instance.

- Default constructor: Loads configuration from the default path or environment variables.
- `config_path`: Path to a JSON configuration file.
- `config`: A pre-constructed `Config` object for programmatic configuration.

The engine is ready to use after construction if `available()` returns `true`.

### Types

#### Request

The core API provided by TENT is submitting a group of asynchronous `Request` tasks through the `submitTransfer` interface, and querying their status through the `getTransferStatus` interface.

```cpp
struct Request {
    enum OpCode { READ, WRITE };
    OpCode opcode;
    void* source;
    SegmentID target_id;
    uint64_t target_offset;
    size_t length;
};
```

- `opcode`: `READ` copies data from `<target_id, target_offset>` to `source`; `WRITE` copies data from `source` to `<target_id, target_offset>`.
- `source`: Local buffer address, must be registered via `registerLocalMemory` or allocated via `allocateLocalMemory`.
- `target_id`: Segment ID obtained from `openSegment`.
- `target_offset`: Offset within the target segment.
- `length`: Number of bytes to transfer.

#### TransferStatus

```cpp
enum TransferStatusEnum {
    INITIAL,    // Not yet started
    PENDING,    // Transfer in progress
    INVALID,    // Invalid parameters
    CANCELED,   // Transfer canceled
    COMPLETED,  // Transfer completed successfully
    TIMEOUT,    // Transfer timed out
    FAILED      // Transfer failed
};

struct TransferStatus {
    TransferStatusEnum s;
    size_t transferred_bytes;
};
```

- `s`: Current status of the transfer.
- `transferred_bytes`: Number of bytes successfully transferred (lower bound).

### Data Transfer

#### TransferEngine::allocateBatch

```cpp
BatchID allocateBatch(size_t batch_size);
```

Allocates a `BatchID` that can hold up to `batch_size` transfer requests.

- `batch_size`: Maximum number of requests that can be submitted under this batch.
- Return value: A valid `BatchID` on success.

#### TransferEngine::submitTransfer

```cpp
Status submitTransfer(BatchID batch_id,
                      const std::vector<Request>& request_list);

// With notification
Status submitTransfer(BatchID batch_id,
                      const std::vector<Request>& request_list,
                      const Notification& notifi);
```

Submits transfer requests to the specified batch. Requests are executed asynchronously.

- `batch_id`: The batch to submit requests to.
- `request_list`: Vector of `Request` objects.
- `notifi`: Optional notification to send upon completion.
- Return value: `Status::OK()` on success; otherwise a non-OK status.

#### TransferEngine::getTransferStatus

```cpp
// Get status of a single task
Status getTransferStatus(BatchID batch_id, size_t task_id,
                         TransferStatus& status);

// Get status of all tasks in a batch
Status getTransferStatus(BatchID batch_id,
                         std::vector<TransferStatus>& status_list);

// Get overall batch status
Status getTransferStatus(BatchID batch_id, TransferStatus& overall_status);
```

Queries the status of transfer requests.

- `batch_id`: The batch to query.
- `task_id`: Index of the specific task (for single-task query).
- `status` / `status_list` / `overall_status`: Output parameter(s) for status.
- Return value: `Status::OK()` on success; otherwise a non-OK status.

#### TransferEngine::freeBatch

```cpp
Status freeBatch(BatchID batch_id);
```

Releases a batch. All transfers in the batch must be completed before calling this.

- `batch_id`: The batch to release.
- Return value: `Status::OK()` on success; otherwise a non-OK status.

### Memory Management

#### TransferEngine::allocateLocalMemory

```cpp
Status allocateLocalMemory(void** addr, size_t size,
                           Location location = kWildcardLocation);

// Advanced version with MemoryOptions
Status allocateLocalMemory(void** addr, size_t size,
                           MemoryOptions& options);
```

Allocates memory that is automatically registered for transfers.

- `addr`: Output pointer to the allocated memory.
- `size`: Size in bytes to allocate.
- `location`: Device location hint (e.g., `"cuda:0"`, `"cpu:0"`, or `"*"` for auto-detect).
- `options`: Advanced options including location, permission, and transport type.
- Return value: `Status::OK()` on success; otherwise a non-OK status.

#### TransferEngine::freeLocalMemory

```cpp
Status freeLocalMemory(void* addr);
```

Frees memory previously allocated with `allocateLocalMemory`.

- `addr`: Pointer to the memory to free.
- Return value: `Status::OK()` on success; otherwise a non-OK status.

#### TransferEngine::registerLocalMemory

```cpp
Status registerLocalMemory(void* addr, size_t size,
                           Permission permission = kGlobalReadWrite);

// Batch registration
Status registerLocalMemory(std::vector<void*> addr_list,
                           std::vector<size_t> size_list,
                           Permission permission = kGlobalReadWrite);

// Advanced version with MemoryOptions
Status registerLocalMemory(void* addr, size_t size, MemoryOptions& options);

// Batch registration with MemoryOptions
Status registerLocalMemory(std::vector<void*> addr_list,
                           std::vector<size_t> size_list,
                           MemoryOptions& options);
```

Registers externally allocated memory for use in transfers.

- `addr`: Starting address of the memory region.
- `size`: Size in bytes.
- `permission`: Access permission (`kLocalReadWrite`, `kGlobalReadOnly`, `kGlobalReadWrite`).
- `addr_list` / `size_list`: For batch registration of multiple buffers.
- `options`: Advanced options for fine-grained control.
- Return value: `Status::OK()` on success; otherwise a non-OK status.

#### TransferEngine::unregisterLocalMemory

```cpp
Status unregisterLocalMemory(void* addr, size_t size = 0);

// Batch unregistration
Status unregisterLocalMemory(std::vector<void*> addr_list,
                             std::vector<size_t> size_list = {});
```

Unregisters previously registered memory.

- `addr`: Starting address of the memory region.
- `size`: Size in bytes (optional, can be 0 if the engine tracks it).
- Return value: `Status::OK()` on success; otherwise a non-OK status.

### Segment Management

#### TransferEngine::openSegment

```cpp
Status openSegment(SegmentID& handle, const std::string& segment_name);
```

Opens a segment by name and returns a handle for use in transfers.

- `handle`: Output parameter for the segment ID.
- `segment_name`: Name of the segment to open (typically the remote node name).
- Return value: `Status::OK()` on success; otherwise a non-OK status.

#### TransferEngine::closeSegment

```cpp
Status closeSegment(SegmentID handle);
```

Closes a previously opened segment.

- `handle`: The segment ID to close.
- Return value: `Status::OK()` on success; otherwise a non-OK status.

#### TransferEngine::getSegmentInfo

```cpp
Status getSegmentInfo(SegmentID handle, SegmentInfo& info);
```

Retrieves information about a segment.

- `handle`: The segment ID.
- `info`: Output parameter containing segment details (type, buffers).
- Return value: `Status::OK()` on success; otherwise a non-OK status.

## Advanced APIs

These APIs are optional; use them for segment export/import, notifications, or engine introspection.

### Segment Export and Import

Reserved for future use. These APIs are declared but currently return
`Status::NotImplemented`.

#### TransferEngine::exportLocalSegment

```cpp
Status exportLocalSegment(std::string& shared_handle);
```

Exports the local segment as a shareable handle string.

- `shared_handle`: Output string that can be passed to remote nodes.
- Return value: Currently returns `Status::NotImplemented`.
- Typical use: Share segment information through an external channel (e.g., gRPC, Redis).

#### TransferEngine::importRemoteSegment

```cpp
Status importRemoteSegment(SegmentID& handle,
                           const std::string& shared_handle);
```

Imports a remote segment from a shared handle string.

- `handle`: Output segment ID.
- `shared_handle`: The handle string obtained from `exportLocalSegment` on the remote side.
- Return value: Currently returns `Status::NotImplemented`.

### Notifications

TENT supports lightweight notifications to coordinate data movement between nodes.

#### TransferEngine::sendNotification

```cpp
Status sendNotification(SegmentID target_id, const Notification& notifi);
```

Sends a notification to a specific segment without data transfer.

- `target_id`: The segment to notify.
- `notifi`: The notification payload.
- Return value: `Status::OK()` on success; otherwise a non-OK status.

#### TransferEngine::receiveNotification

```cpp
Status receiveNotification(std::vector<Notification>& notifi_list);
```

Receives pending notifications from peers.

- `notifi_list`: Output vector of received notifications.
- Return value: `Status::OK()` on success; otherwise a non-OK status.
- Typical use: Polling loop to trigger follow-up actions on received data.

### Engine Introspection

#### TransferEngine::available

```cpp
bool available() const;
```

Returns `true` if the engine was initialized successfully and is ready for use.

#### TransferEngine::getSegmentName

```cpp
const std::string getSegmentName() const;
```

Returns the local segment name (node identifier).

#### TransferEngine::getRpcServerAddress

```cpp
const std::string getRpcServerAddress() const;
```

Returns the RPC server address for this engine instance.

#### TransferEngine::getRpcServerPort

```cpp
uint16_t getRpcServerPort() const;
```

Returns the RPC server port for this engine instance.

## Type Reference

### Permission

```cpp
enum Permission {
    kLocalReadWrite,   // Only local access
    kGlobalReadOnly,   // Remote read access
    kGlobalReadWrite,  // Remote read/write access
};
```

### Location

```cpp
using Location = std::string;
const static std::string kWildcardLocation = "*";
```

Location strings identify device affinity: `"cpu:0"`, `"cuda:0"`, `"cuda:1"`, etc. Use `"*"` for automatic detection.

### TransportType

```cpp
enum TransportType {
    RDMA = 0,
    MNNVL,
    SHM,
    NVLINK,
    GDS,
    IOURING,
    TCP,
    AscendDirect,
    UNSPEC
};
```

Transport types used internally by TENT. Applications typically do not need to specify these directly.

### MemoryOptions

```cpp
struct MemoryOptions {
    Location location = kWildcardLocation;
    Permission perm = kGlobalReadWrite;
    TransportType type = UNSPEC;
    std::string shm_path = "";
    size_t shm_offset = 0;
    bool internal = false;
};
```

Advanced options for memory allocation and registration.

### SegmentInfo

```cpp
struct SegmentInfo {
    enum Type { Memory, File };
    struct Buffer {
        uint64_t base, length;
        Location location;
    };
    Type type;
    std::vector<Buffer> buffers;
};
```

Information about a segment, including its type and registered buffers.

### Notification

```cpp
struct Notification {
    std::string name;
    std::string msg;
};
```

Lightweight notification payload for coordination between nodes.

### Status

The `Status` class is used for error handling throughout the API. Key methods:

```cpp
bool ok() const;           // Returns true if operation succeeded
std::string ToString() const;  // Human-readable error description

// Common status factory methods
static Status OK();
static Status InvalidArgument(std::string_view msg);
static Status InternalError(std::string_view msg);
// ... and more
```

## See Also

- [Quick Start Guide](../getting-started/quick-start.md) - Getting started with TENT
- [Migration Guide](../getting-started/migration.md) - Migrating from classic Transfer Engine
- [C API Reference](c-api.md) - C API documentation
- [Python API Reference](python-api.md) - Python API documentation
- [Configuration Reference](../configuration/configuration.md) - Configuration options
