# TENT Migration Guide

This guide helps migrate from classic Transfer Engine (TE) to TENT.

## Overview

| Aspect | Transfer Engine | TENT |
|--------|---------------|------|
| Initialization | `init()` after constructor | Constructor with `Config` |
| Transport management | Manual (`installTransport`) | Internal |
| Memory allocation | External only | `allocateLocalMemory()` available |
| Memory registration | Boolean flags | `Permission` enum |
| Segment discovery | Manual cache sync | Automatic |
| Error handling | Mixed `int`/`Status` | Consistent `Status` returns |

### API Mapping

**Initialization**

| TE API | TENT API |
|--------|----------|
| `init(conn, name, ip, port)` | Constructor with config |
| `freeEngine()` | Destructor |

**Memory**

| TE API | TENT API |
|--------|----------|
| `registerLocalMemory(addr, len, loc, remote, update)` | `registerLocalMemory(addr, size, perm)` |
| `unregisterLocalMemory(addr, update)` | `unregisterLocalMemory(addr)` |
| *N/A* | `allocateLocalMemory(addr, size, loc)` |
| *N/A* | `freeLocalMemory(addr)` |

**Segments**

| TE API | TENT API |
|--------|----------|
| `openSegment(name)` → returns handle | `openSegment(handle, name)` → output param |
| `closeSegment(handle)` | Same |
| `syncSegmentCache(name)` | Automatic |
| `removeLocalSegment(name)` | Internal |
| `CheckSegmentStatus(sid)` | Internal |

**Transfers**

| TE API | TENT API |
|--------|----------|
| `allocateBatchID(size)` | `allocateBatch(size)` |
| `freeBatchID(id)` | `freeBatch(id)` |
| `submitTransfer(id, entries)` | `submitTransfer(id, request_list)` |
| `getTransferStatus(id, task, status)` | Same |
| `getBatchTransferStatus(id, status)` | `getTransferStatus(id, status)` (overloaded) |

**Notifications**

| TE API | TENT API |
|--------|----------|
| `sendNotifyByID(id, msg)` | `sendNotification(id, notifi)` |
| `getNotifies(list)` | `receiveNotification(list)` |

**Removed APIs**

These TE APIs have no TENT equivalent:
- `installTransport()`, `uninstallTransport()`, `getTransport()` - transport selection is internal
- `getMetadata()`, `getLocalTopology()` - internalized
- `syncSegmentCache()` - automatic
- `checkOverlap()` - not exposed
- `setAutoDiscover()` - always enabled
- `setWhitelistFilters()` - use `Config`

**New APIs**

- `available()` - check initialization status
- `getSegmentName()` - get local segment name
- `getRpcServerAddress()`, `getRpcServerPort()` - RPC info

## Compatibility Shim

Set `MC_USE_TENT=1` to run existing TE code on TENT:

```bash
export MC_USE_TENT=1
./your_te_app
```

**Limitations**: `installTransport()`, `getMetadata()`, `getLocalTopology()`, `setAutoDiscover()` become no-ops.

## Examples

### Memory Registration

**TE:**
```cpp
void* buf = malloc(size);
engine.registerLocalMemory(buf, size, "cuda:0", true, true);
// ... use ...
engine.unregisterLocalMemory(buf, true);
free(buf);
```

**TENT:**
```cpp
void* buf = nullptr;
engine.allocateLocalMemory(&buf, size, "cuda:0");
// ... use ...
engine.freeLocalMemory(buf);
```

### Error Handling

**TE:**
```cpp
int ret = engine.submitTransfer(batch, requests);
if (ret != 0) { /* error */ }
```

**TENT:**
```cpp
auto status = engine.submitTransfer(batch, requests);
if (!status.ok()) { /* error, check status.ToString() */ }
```

### Full Example

**TE:**
```cpp
#include "transfer_engine.h"

mooncake::TransferEngine engine;
engine.init("etcd://localhost:2379", "node1", "127.0.0.1", 12345);

void* buffer = malloc(1024*1024);
engine.registerLocalMemory(buffer, 1024*1024, "cuda:0", true, true);

auto segment = engine.openSegment("node2");
auto batch = engine.allocateBatchID(10);

TransferRequest req;
req.opcode = 0; // READ
req.source = buffer;
req.target_segment = segment;
req.target_offset = 0;
req.length = 1024*1024;
engine.submitTransfer(batch, {req});

TransferStatus status;
do {
    engine.getTransferStatus(batch, 0, status);
} while (status.state == 1);

engine.unregisterLocalMemory(buffer, true);
free(buffer);
engine.freeBatchID(batch);
engine.freeEngine();
```

**TENT:**
```cpp
#include "tent/transfer_engine.h"

mooncake::tent::TransferEngine engine("./config.json");

if (!engine.available()) {
    // handle error
}

void* buffer = nullptr;
engine.allocateLocalMemory(&buffer, 1024*1024, "cuda:0");

SegmentID segment;
engine.openSegment(segment, "node2");

BatchID batch = engine.allocateBatch(10);

Request req;
req.opcode = Request::READ;
req.source = buffer;
req.target_id = segment;
req.target_offset = 0;
req.length = 1024*1024;
engine.submitTransfer(batch, {req});

TransferStatus status;
do {
    engine.getTransferStatus(batch, 0, status);
} while (status.s == TransferStatusEnum::PENDING);

engine.freeLocalMemory(buffer);
// batch and segment cleanup automatic or explicit
```

## Configuration Migration

TE used initialization parameters:

```cpp
engine.init("etcd://host:2379", "name", "ip", port);
engine.installTransport("rdma", rdma_args);
```

TENT uses a JSON config file:

```json
{
  "metadata_type": "etcd",
  "metadata_servers": "host:2379",
  "local_segment_name": "name",
  "rpc_server_hostname": "ip",
  "rpc_server_port": port,
  "transports": {
    "rdma": { "enable": true }
  }
}
```

See [Configuration Reference](../configuration/configuration.md) for complete options.

## See Also

- [Quick Start Guide](quick-start.md) - Getting started with TENT
- [C++ API Reference](../api/cpp-api.md) - Complete API documentation
- [Configuration Reference](../configuration/configuration.md) - Configuration options
