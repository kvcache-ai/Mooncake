# Transfer Engine C++ API Reference

## Overview
This page summarizes the C++ APIs in `mooncake-transfer-engine/include/transfer_engine.h`.
It follows the same narrative style as the design doc, while covering the full set of public C++ APIs.

For conceptual background, see [Transfer Engine](index.md).

**Core APIs vs Advanced APIs**
- Core APIs form the minimal path to move data: initialize the engine, register memory, open segments, submit transfers, and query status.
- Advanced APIs are optional; they help with transport control, notifications, metadata/cache maintenance, topology discovery, and debugging.

## Core APIs

### Core Usage Path (C++)
Most integrations follow a short, repeatable path: initialize the engine, register local memory, open a target segment, submit a batch transfer, poll status, and finally free resources (batch, segment, memory). A minimal example is shown below.

```cpp
#include "transfer_engine.h"

using mooncake::TransferEngine;
using mooncake::TransferRequest;
using mooncake::TransferStatus;

TransferEngine engine(true);
engine.init("etcd://127.0.0.1:2379", "node0");

void *local_addr = /* local buffer address */;
size_t length = /* bytes to transfer */;
uint64_t remote_addr = /* remote address or file offset */;

engine.registerLocalMemory(local_addr, length, "cpu:0", true);

auto segment = engine.openSegment("node1");
auto batch = engine.allocateBatchID(1);

TransferRequest req{};
req.opcode = TransferRequest::WRITE;
req.source = local_addr;
req.target_id = segment;
req.target_offset = remote_addr;
req.length = length;

engine.submitTransfer(batch, {req});

TransferStatus status{};
engine.getTransferStatus(batch, 0, status);

engine.freeBatchID(batch);
engine.closeSegment(segment);
engine.unregisterLocalMemory(local_addr);
```

### Constructors

```cpp
TransferEngine(bool auto_discover = false);
TransferEngine(bool auto_discover, const std::vector<std::string>& filter);
```

Constructs a Transfer Engine instance. `auto_discover` enables topology discovery; `filter` constrains discovery or device selection with a whitelist.

### Data Transfer

#### TransferEngine::TransferRequest

The core API provided by Mooncake Transfer Engine is submitting a group of asynchronous `TransferRequest` tasks through the `submitTransfer` interface, and querying their status through the `getTransferStatus` interface. Each `TransferRequest` specifies reading or writing a continuous data space of `length` starting from the local starting address `source`, to the position starting at `target_offset` in the segment corresponding to `target_id`.

The `TransferRequest` structure is defined as follows:

```cpp
struct TransferRequest
{
    enum OpCode { READ, WRITE };
    OpCode opcode;
    void *source;
    SegmentID target_id; // The ID of the target segment, which may correspond to local or remote DRAM/VRAM/NVMeof, with the specific routing logic hidden
    uint64_t target_offset;
    size_t length;
    int advise_retry_cnt = 0;
};
```

- `opcode` takes the values `READ` or `WRITE`. `READ` indicates that data is copied from the target address indicated by `<target_id, target_offset>` to the local starting address `source`; `WRITE` indicates that data is copied from `source` to the address indicated by `<target_id, target_offset>`.
- `source` represents the DRAM/VRAM buffer managed by the current `TransferEngine`, which must have been registered in advance by the `registerLocalMemory` interface.
- `target_id` represents the segment ID of the transfer target. The segment ID is obtained using the `openSegment` interface. Segments are divided into the following types:
  - RAM space type, covering DRAM/VRAM. As mentioned earlier, there is only one segment under the same process (or `TransferEngine` instance), which contains various types of Buffers (DRAM/VRAM). In this case, the segment name passed to the `openSegment` interface is equivalent to the server hostname. `target_offset` is the virtual address of the target server.
  - NVMeOF space type, where each file corresponds to a segment. In this case, the segment name passed to the `openSegment` interface is equivalent to the unique identifier of the file. `target_offset` is the offset of the target file.
- `length` represents the amount of data transferred. TransferEngine may further split this into multiple read/write requests internally.

#### TransferEngine::allocateBatchID

```cpp
BatchID allocateBatchID(size_t batch_size);
```

Allocates a `BatchID`. A maximum of `batch_size` `TransferRequest`s can be submitted under the same `BatchID`.

- `batch_size`: The maximum number of `TransferRequest`s that can be submitted under the same `BatchID`;
- Return value: If successful, returns a `BatchID`; on failure, returns an invalid handle (for example `INVALID_BATCH_ID`).

#### TransferEngine::submitTransfer

```cpp
Status submitTransfer(BatchID batch_id,
                      const std::vector<TransferRequest> &entries);
```

Submits new `TransferRequest` tasks to `batch_id`. The task is asynchronously submitted to the background thread pool. The total number of `entries` accumulated under the same `batch_id` should not exceed the `batch_size` defined at creation.

- `batch_id`: The `BatchID` it belongs to;
- `entries`: Array of `TransferRequest`;
- Return value: If successful, returns an OK status; otherwise, returns a non-OK status.

#### TransferEngine::getTransferStatus

```cpp
enum TransferStatusEnum
{
  WAITING,   // In the transfer phase
  PENDING,   // Not supported
  INVALID,   // Invalid parameters
  CANCELED,  // Not supported
  COMPLETED, // Transfer completed
  TIMEOUT,   // Not supported
  FAILED     // Transfer failed even after retries
};
struct TransferStatus {
  TransferStatusEnum s;
  size_t transferred_bytes; // How much data has been successfully transferred (not necessarily an accurate value, but it is a lower bound)
};
Status getTransferStatus(BatchID batch_id, size_t task_id, TransferStatus &status)
```

Obtains the running status of the `TransferRequest` with `task_id` in `batch_id`.

- `batch_id`: The `BatchID` it belongs to;
- `task_id`: The sequence number of the `TransferRequest` to query;
- `status`: Output Transfer status;
- Return value: If successful, returns an OK status; otherwise, returns a non-OK status.

#### TransferEngine::getBatchTransferStatus

```cpp
Status getBatchTransferStatus(BatchID batch_id, TransferStatus& status);
```

Obtains the aggregated status of the batch and the total transferred bytes.

- `batch_id`: The `BatchID` it belongs to;
- `status`: Output Transfer status;
- Return value: If successful, returns an OK status; otherwise, returns a non-OK status.

#### TransferEngine::freeBatchID

```cpp
Status freeBatchID(BatchID batch_id);
```

Recycles `BatchID`, and subsequent operations on `submitTransfer` and `getTransferStatus` are undefined. If there are still `TransferRequest`s pending completion in the `BatchID`, the operation is refused.

- `batch_id`: The `BatchID` it belongs to;
- Return value: If successful, returns an OK status; otherwise, returns a non-OK status.

### Space Registration

For the RDMA transfer process, the source pointer `TransferRequest::source` must be registered in advance as an RDMA readable/writable Memory Region space, that is, included as part of the RAM Segment of the current process. Therefore, the following functions are needed:

#### TransferEngine::registerLocalMemory

```cpp
int registerLocalMemory(void *addr,
                        size_t length,
                        const std::string& location = kWildcardLocation,
                        bool remote_accessible = true,
                        bool update_metadata = true);
```

Registers a space starting at address `addr` with a length of `size` on the local DRAM/VRAM.

- `addr`: The starting address of the registration space;
- `length`: The length of the registration space;
- `location`: The `device` corresponding to this memory segment, such as `cuda:0` indicating the GPU device, `cpu:0` indicating the CPU socket, by matching with the network card priority order table (see `installTransport`), the preferred network card is identified. You can also use `*`, Transfer Engine will try to automatically recognize the `device` corresponding to `addr`, if it fails to recognize the device, it will print a `WARNING` level log and use all network cards, no preferred network cards.
- `remote_accessible`: Indicates whether this memory can be accessed by remote nodes.
- `update_metadata`: Whether to publish the registration to the metadata service.
- Return value: If successful, returns 0; otherwise, returns a negative value.

#### TransferEngine::unregisterLocalMemory

```cpp
int unregisterLocalMemory(void *addr, bool update_metadata = true);
```

Unregisters the region.

- `addr`: The starting address of the registration space;
- `update_metadata`: Whether to publish the unregistration to the metadata service.
- Return value: If successful, returns 0; otherwise, returns a negative value.

#### TransferEngine::registerLocalMemoryBatch

```cpp
int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                             const std::string& location);
```

Registers multiple buffers in one call to reduce registration overhead.

- `buffer_list`: A list of `{addr, length}` entries.
- `location`: The `device` corresponding to these buffers.
- Return value: If successful, returns 0; otherwise, returns a negative value.

#### TransferEngine::unregisterLocalMemoryBatch

```cpp
int unregisterLocalMemoryBatch(const std::vector<void*>& addr_list);
```

Unregisters multiple buffers in one call.

- `addr_list`: A list of buffer start addresses.
- Return value: If successful, returns 0; otherwise, returns a negative value.

### Segment Management and Metadata Format

TransferEngine provides the `openSegment` function, which obtains a `SegmentHandle` for subsequent `Transport` transfers.
```cpp
SegmentHandle openSegment(const std::string& segment_name);
```

- `segment_name`: The unique identifier of the segment. For RAM Segment, this needs to be consistent with the `server_name` filled in by the peer process when initializing the TransferEngine object.
- Return value: If successful, returns the corresponding `SegmentHandle`; otherwise, returns an invalid handle.

```cpp
int closeSegment(SegmentHandle segment_id);
```

- `segment_id`: The unique identifier of the segment.
- Return value: If successful, returns 0; otherwise, returns a negative value.

#### TransferEngine::removeLocalSegment

```cpp
int removeLocalSegment(const std::string& segment_name);
```

Removes local segment metadata entries.

- `segment_name`: The local segment name to remove.
- Return value: If successful, returns 0; otherwise, returns a negative value.

#### TransferEngine::CheckSegmentStatus

```cpp
Status CheckSegmentStatus(SegmentID sid);
```

Checks whether a segment is reachable and valid.

- `sid`: The segment identifier.
- Return value: If successful, returns an OK status; otherwise, returns a non-OK status.

<details>
<summary><strong>Metadata Format</strong></summary>

```
// Used to find the communicable address and exposed rpc port based on server_name.
// Created: when calling TransferEngine::init().
// Deleted: when TransferEngine is destructed.
Key = mooncake/rpc_meta/[server_name]
Value = {
    'ip_or_host_name': 'node01'
    'rpc_port': 12345
}

// For segments, the key naming method of mooncake/[proto]/[segment_name] is used, and the segment name can use the Server Name.
// A segment corresponds to a machine, and a buffer corresponds to different segments of memory or different files or different disks on the machine. Different buffers of the same segment are in the same fault domain.

// RAM Segment, used by RDMA Transport to obtain transfer information.
// Created: command line tool register.py, at this time buffers are empty, only fill in the information that can be known in advance.
// Modified: TransferEngine at runtime through register / unregister to add or delete Buffer.
Key = mooncake/ram/[segment_name]
Value = {
    'server_name': server_name,
    'protocol': rdma,
    'devices': [
        { 'name': 'mlx5_2', 'lid': 17, 'gid': 'fe:00:...' },
        { 'name': 'mlx5_3', 'lid': 22, 'gid': 'fe:00:...' }
    ],
    'priority_matrix': {
        "cpu:0": [["mlx5_2"], ["mlx5_3"]],
        "cpu:1": [["mlx5_3"], ["mlx5_2"]],
        "cuda:0": [["mlx5_2"], ["mlx5_3"]],
    },
    'buffers': [
        {
            'name': 'cpu:0',
            'addr': 0x7fa16bdf5000,
            'length': 1073741824,
            'rkey': [1fe000, 1fdf00, ...], // The length is the same as the number of elements in the 'devices' field
        },
    ],
}

// Created: command line tool register.py, determine the file path that can be mounted.
// Modified: command line tool mount.py, add a mapping of the machine mounting the file to the file path on the mounting machine to the buffers.local_path_map.
Key = mooncake/nvmeof/[segment_name]
Value = {
    'server_name': server_name,
    'protocol': nvmeof,
    'buffers':[{
        'length': 1073741824,
        'file_path': "/mnt/nvme0" // The file path on this machine
        'local_path_map': {
            "node01": "/mnt/transfer_engine/node01/nvme0", // The machine mounting the file -> The file path on the mounting machine
            ...
        },
     },
     {
        'length': 1073741824,
        'file_path': "/mnt/nvme1",
        'local_path_map': {
            "node02": "/mnt/transfer_engine/node02/nvme1",
            ...
        },
     }
    ]
}
```
</details>

### HTTP Metadata Server

The HTTP server should implement three following RESTful APIs, while the metadata server configured to `http://host:port/metadata` as an example:

1. `GET /metadata?key=$KEY`: Get the metadata corresponding to `$KEY`.
2. `PUT /metadata?key=$KEY`: Update the metadata corresponding to `$KEY` to the value of the request body.
3. `DELETE /metadata?key=$KEY`: Delete the metadata corresponding to `$KEY`.

For specific implementation, refer to the demo service implemented in Golang at [mooncake-transfer-engine/example/http-metadata-server](../../../mooncake-transfer-engine/example/http-metadata-server).

### Initialization

TransferEngine needs to be initialized by calling the `init` method before further actions:
```cpp
TransferEngine();

int init(const std::string &metadata_conn_string,
         const std::string &local_server_name);
```
There is also an extended form to override RPC binding:
```cpp
int init(const std::string &metadata_conn_string,
         const std::string &local_server_name,
         const std::string &ip_or_host_name,
         uint64_t rpc_port);
```
- `metadata_conn_string`: Connecting string of metadata storage servers, i.e., the IP address/hostname of `etcd`/`redis` or the URI of the http service.
The general form is `[proto]://[hostname:port]`. For example, the following metadata server addresses are legal:
    - Using `etcd` as a metadata storage service: `"10.0.0.1:2379"` or `"etcd://10.0.0.1:2379"`.
    - Using `redis` as a metadata storage service: `"redis://10.0.0.1:6379"`
    - Using `http` as a metadata storage service: `"http://10.0.0.1:8080/metadata"`

- `local_server_name`: The local server name, ensuring uniqueness within the cluster. It also serves as the name of the RAM Segment that other nodes refer to the current instance (i.e., Segment Name).
- `ip_or_host_name`: Optional explicit bind address or hostname for the RPC service.
- `rpc_port`: Optional explicit RPC port.

```cpp
  ~TransferEngine();
```

Reclaims all allocated resources and also deletes the global meta data server information.

#### TransferEngine::freeEngine

```cpp
int freeEngine();
```

Releases resources early without waiting for destruction.

## Advanced APIs
These APIs are optional; use them when you need manual transport control, coordination notifications, metadata/cache refresh, topology discovery, or debugging. They are not required for the basic data path.

### Multi-Transport Management

The `TransferEngine` class internally manages multiple backend `Transport` classes.
And it will discover the topology between CPU/CUDA and RDMA devices automatically
(more device types are working in progress, feedbacks are welcome when the automatic discovery mechanism is not accurate),
and it will install `Transport` automatically based on the topology.

#### TransferEngine::installTransport

```cpp
Transport* installTransport(const std::string& proto, void** args);
```

Installs a transport backend explicitly.

- `proto`: Transport protocol name, such as `rdma`, `tcp`, or `nvmeof`.
- `args`: Transport-specific arguments.
> Note: In TENT, `installTransport` is not exposed (removed from the public API, including compatibility surfaces). Transport selection is internal to TENT.

#### TransferEngine::uninstallTransport

```cpp
int uninstallTransport(const std::string& proto);
```

Uninstalls a transport backend.

- `proto`: Transport protocol name.
- Return value: If successful, returns 0; otherwise, returns a negative value.

#### TransferEngine::getTransport

```cpp
Transport* getTransport(const std::string& proto);
```

Returns a transport instance by protocol name, mainly for advanced inspection or debugging.

### Notifications

TransferEngine can send and receive lightweight notifications across segments to coordinate data movement.

#### TransferEngine::submitTransferWithNotify

```cpp
Status submitTransferWithNotify(BatchID batch_id,
                                const std::vector<TransferRequest>& entries,
                                TransferMetadata::NotifyDesc notify_msg);
```

Submits a batch transfer and delivers a notification upon completion.

- `notify_msg`: A `{name, msg}` payload delivered to the receiver.
- Typical use: signal that a transferred buffer or KV-cache slice is ready to consume on the receiver side.

#### TransferEngine::getNotifies

```cpp
int getNotifies(std::vector<TransferMetadata::NotifyDesc>& notifies);
```

Gets pending notifications from peers.

- Return value: If successful, returns 0; otherwise, returns a negative value.
- Typical use: receiver-side polling loop to trigger follow-up actions (e.g., attaching the transferred buffer to a scheduler or cache).

#### TransferEngine::sendNotifyByID

```cpp
int sendNotifyByID(SegmentID target_id, TransferMetadata::NotifyDesc notify_msg);
```

Sends a notification to a specific segment by ID.
- Typical use: control-plane message such as "ready", "invalidate", or "retry" tied to a known segment handle.

#### TransferEngine::sendNotifyByName

```cpp
int sendNotifyByName(std::string remote_agent, TransferMetadata::NotifyDesc notify_msg);
```

Sends a notification to a remote agent by name.
- Typical use: best-effort coordination when you only know the peer's name (segment name), not the handle.

### Segment Cache and Metadata Access

#### TransferEngine::syncSegmentCache

```cpp
int syncSegmentCache(const std::string& segment_name = "");
```

Synchronizes local segment cache from the metadata service.

- `segment_name`: If empty, refreshes all segments; otherwise, refreshes the specified segment.
- Typical use: when peers dynamically register/unregister buffers and you need to refresh before opening or submitting transfers.

#### TransferEngine::getMetadata

```cpp
std::shared_ptr<TransferMetadata> getMetadata();
```

Returns the metadata subsystem instance for advanced workflows.
- Typical use: advanced integration or debugging of metadata content beyond the standard APIs.

### Topology and Discovery

#### TransferEngine::setAutoDiscover

```cpp
void setAutoDiscover(bool auto_discover);
```

Enables or disables topology discovery.
- Typical use: controlled environments or debugging where auto discovery is not desired.

#### TransferEngine::setWhitelistFilters

```cpp
void setWhitelistFilters(std::vector<std::string>&& filters);
```

Sets the device whitelist used during topology discovery.
- Typical use: restrict transfers to a subset of NICs or GPUs for performance isolation or testing.

#### TransferEngine::numContexts

```cpp
int numContexts() const;
```

Returns the number of active transport contexts.
- Typical use: introspection and resource monitoring.

#### TransferEngine::getLocalTopology

```cpp
std::shared_ptr<Topology> getLocalTopology();
```

Returns the local topology model used for path selection.
- Typical use: diagnose path selection or export topology to a scheduler.

### Introspection and Safety

#### TransferEngine::getLocalIpAndPort

```cpp
std::string getLocalIpAndPort();
```

Returns the resolved local RPC address.
- Typical use: publish the resolved address to external components or logs.

#### TransferEngine::getRpcPort

```cpp
int getRpcPort();
```

Returns the active RPC port.
- Typical use: verify port binding when dynamic ports are used.

#### TransferEngine::checkOverlap

```cpp
bool checkOverlap(void* addr, uint64_t length);
```

Checks whether a given address range overlaps with existing registered buffers.
- Typical use: validate registration plans before calling `registerLocalMemory` in complex buffer managers.
