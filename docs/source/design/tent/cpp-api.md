# TENT C++ API Reference

## Overview

This page summarizes the C++ APIs in `mooncake-transfer-engine/tent/include/tent/transfer_engine.h`.
It follows the same structure as the Transfer Engine API documentation.

For conceptual background, see [TENT Overview](overview.md).

**Prerequisites and API modes**
- **Build** with `-DUSE_TENT=ON` to enable TENT.
- TENT provides two API surfaces:
  - **TENT-native API** (documented below).
  - **TE-compatible API** via the compatibility shim (set `MC_USE_TENT=1`). For TE-compatible API, see [TE C++ API Reference](../transfer-engine/cpp-api.md)

**Core APIs vs Advanced APIs**
- Core APIs form the minimal path to move data: create the engine, register memory, open segments, submit transfers, and query status.
- Advanced APIs are optional; they help with segment export/import, notifications, and engine introspection.

## Key Differences from Transfer Engine

TENT redesigns the API surface based on different design goals. The following table summarizes the key differences and the rationale behind them.

| Aspect | Transfer Engine (TE) | TENT | Rationale |
|--------|---------------------|------|-----------|
| **Initialization** | Two-step: constructor + `init(metadata_conn_string, server_name, ...)` | Single-step: constructor with `Config` object or config file path | Simplifies initialization; centralizes configuration in a single place |
| **Transport Management** | Manual: `installTransport()`, `uninstallTransport()`, `getTransport()` | Removed from public API | TENT performs dynamic transport selection at runtime; applications should not manage transports directly |
| **Memory Allocation** | Not provided; users allocate memory externally | `allocateLocalMemory()` / `freeLocalMemory()` | Provides integrated memory management with automatic registration |
| **Memory Registration** | `registerLocalMemory(addr, len, location, remote_accessible, update_metadata)` | `registerLocalMemory(addr, size, Permission)` or `MemoryOptions` | Cleaner semantics; `Permission` replaces boolean flags; metadata updates are internal |
| **Segment Discovery** | Via metadata service; manual cache sync with `syncSegmentCache()` | Internal control plane (metadata type = `p2p` or central); no manual sync | Simplifies metadata management; discovery handled inside runtime |
| **Error Handling** | Mixed: some APIs return `int`, others return `Status` | Consistent: all APIs return `Status` | Uniform error handling across the API |
| **Topology/Introspection** | Exposed: `getLocalTopology()`, `getMetadata()`, `checkOverlap()`, etc. | Internalized; not exposed | TENT handles topology and path selection internally; reduces API complexity |

### Design Philosophy

1. **Declarative over Imperative**: Applications describe *what* data to move, not *how* to move it. Transport selection, path optimization, and failure handling are delegated to the TENT runtime.

2. **Single Initialization**: Instead of a two-phase `constructor + init()` pattern, TENT uses configuration objects that fully describe the engine state at construction time.

3. **Internal Metadata Management**: TENT does not expose metadata internals (`TransferMetadata`, `Topology`). Segment discovery is handled by the control plane (P2P or central), and the cache is managed automatically.

4. **Consistent Error Model**: All TENT APIs return `Status` objects, eliminating the mixed `int` / `Status` return types in TE.

## API Mapping: TE to TENT

For users migrating from Transfer Engine, the following table shows how TE APIs map to TENT APIs. TENT provides a backward-compatible shim (`MC_USE_TENT=1`) that allows existing TE code to run on the TENT runtime.

| Transfer Engine API | TENT API | Notes |
|---------------------|----------|-------|
| **Initialization** |||
| `TransferEngine(auto_discover)` | `TransferEngine()` | TENT ignores `auto_discover`; discovery is always automatic |
| `init(conn_string, server_name, ip, port)` | Constructor with `Config` | Config sets `metadata_type`, `metadata_servers`, `local_segment_name` |
| `freeEngine()` | Destructor | Resources released automatically on destruction |
| **Transport** |||
| `installTransport(proto, args)` | *Not available* | Transport selection is internal to TENT |
| `uninstallTransport(proto)` | *Not available* | — |
| `getTransport(proto)` | *Not available* | — |
| **Memory** |||
| `registerLocalMemory(addr, len, location, remote_accessible, update_metadata)` | `registerLocalMemory(addr, size, MemoryOptions)` | `MemoryOptions.location` replaces `location`; `MemoryOptions.perm` replaces `remote_accessible` |
| `unregisterLocalMemory(addr, update_metadata)` | `unregisterLocalMemory(addr)` | Metadata update is internal |
| `registerLocalMemoryBatch(buffer_list, location)` | `registerLocalMemory(addr_list, size_list, MemoryOptions)` | Batch API uses vectors instead of `BufferEntry` |
| `unregisterLocalMemoryBatch(addr_list)` | `unregisterLocalMemory(addr_list)` | — |
| *Not available* | `allocateLocalMemory(addr, size, location)` | TENT-only: allocates and registers in one call |
| *Not available* | `freeLocalMemory(addr)` | TENT-only: frees allocated memory |
| **Segment** |||
| `openSegment(segment_name)` → returns handle | `openSegment(handle, segment_name)` → via output param | Return style differs; `Status` indicates success/failure |
| `closeSegment(handle)` | `closeSegment(handle)` | Same semantics |
| `removeLocalSegment(segment_name)` | *Not available* | Segment lifecycle managed internally |
| `CheckSegmentStatus(sid)` | *Not available* | Status checking is internal |
| `syncSegmentCache(segment_name)` | *Not available* | Cache sync is automatic |
| *Not available* | `exportLocalSegment(shared_handle)` | Declared in TENT API but currently not implemented |
| *Not available* | `importRemoteSegment(handle, shared_handle)` | Declared in TENT API but currently not implemented |
| `getSegmentInfo(handle, info)` | `getSegmentInfo(handle, info)` | Same semantics |
| **Batch & Transfer** |||
| `allocateBatchID(batch_size)` | `allocateBatch(batch_size)` | Renamed |
| `freeBatchID(batch_id)` | `freeBatch(batch_id)` | Renamed |
| `submitTransfer(batch_id, entries)` | `submitTransfer(batch_id, request_list)` | `TransferRequest` → `Request` |
| `submitTransferWithNotify(batch_id, entries, notify_msg)` | `submitTransfer(batch_id, request_list, notifi)` | Unified API with optional notification |
| `getTransferStatus(batch_id, task_id, status)` | `getTransferStatus(batch_id, task_id, status)` | Same |
| `getBatchTransferStatus(batch_id, status)` | `getTransferStatus(batch_id, status)` | Overloaded; single `TransferStatus` output = overall status |
| *Not available* | `getTransferStatus(batch_id, status_list)` | TENT-only: get all task statuses at once |
| **Notification** |||
| `sendNotifyByID(target_id, notify_msg)` | `sendNotification(target_id, notifi)` | Renamed; uses `Notification` struct |
| `sendNotifyByName(remote_agent, notify_msg)` | `openSegment()` + `sendNotification()` | TENT requires explicit segment handle |
| `getNotifies(notifies)` | `receiveNotification(notifi_list)` | Renamed; uses `Notification` struct |
| **Introspection** |||
| `getLocalIpAndPort()` | `getRpcServerAddress()` + `getRpcServerPort()` | Split into two methods |
| `getRpcPort()` | `getRpcServerPort()` | Same |
| `getMetadata()` | *Not available* | Metadata internals not exposed |
| `getLocalTopology()` | *Not available* | Topology internals not exposed |
| `checkOverlap(addr, length)` | *Not available* | — |
| `setAutoDiscover(auto_discover)` | *Not available* | Always enabled (ignored under `MC_USE_TENT`) |
| `setWhitelistFilters(filters)` | *Not available* | Configure via `Config` |
| `numContexts()` | *Not available* | — |
| *Not available* | `available()` | TENT-only: check if engine initialized successfully |
| *Not available* | `getSegmentName()` | TENT-only: get local segment name |

### Using the Backward-Compatible Shim

Existing Transfer Engine code can run on TENT by setting the environment variable:

```bash
export MC_USE_TENT=1
```

When this variable is set, the `mooncake::TransferEngine` class internally delegates to `mooncake::tent::TransferEngine`. Most TE APIs are translated automatically. APIs that have no TENT equivalent (e.g., `installTransport`, `getMetadata`) become no-ops or return placeholder values.

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
```

Submits transfer requests to the specified batch. Requests are executed asynchronously.

- `batch_id`: The batch to submit requests to.
- `request_list`: Vector of `Request` objects.
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

#### TransferEngine::submitTransfer (with notification)

```cpp
Status submitTransfer(BatchID batch_id,
                      const std::vector<Request>& request_list,
                      const Notification& notifi);
```

Submits transfers and sends a notification upon completion.

- `notifi`: A `{name, msg}` payload delivered to the receiver when the transfer completes.
- Typical use: Signal that transferred data is ready for consumption.

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
**