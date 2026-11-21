---
orphan: true
---

# Shared NVMe KV Backend Design

## Overview

The shared NVMe KV backend exposes an external storage pool as `SHARED_NVME_KV` replicas through Mooncake Store's existing APIs. All participating clients access the same KV namespaces, while Master assigns allocation IDs and tracks replica state. Readers obtain a replica descriptor from Master and retrieve the object directly from the Target, so published replicas remain readable after the writing client exits.

The [local NVMe KV backend](nvme-kv-backend.md) exposes storage through `FileStorage` as `LOCAL_DISK` replicas, with remote reads served by the owning client. The shared backend instead uses allocation-based addressing and Master-managed replica state, while reusing the common NVMe KV object store, object format, Connector, and Linux ioctl/io_uring executors.

## Architecture

Each client connects to the shared namespaces through Linux device nodes, with the kernel managing the NVMe-oF connections. The client creates an object store, Connector, and executor for each configured namespace, and routes object I/O through these instances. Master manages allocation identities and replica visibility without reserving Target capacity; the external Target executes the KV commands and allocates physical capacity when Store succeeds.

| Component | Responsibility |
| --- | --- |
| Master | Assign allocation IDs, track replica state, and validate publication, revocation, and miss reports. |
| Client | Coordinate Put/Get with Master and route selected shared replicas to the backend. |
| `SharedNvmeKvBackend` | Select a namespace and pass allocation identities to the object store with the shared object policy. |
| `NvmeKvObjectStore` | Encode objects, gather write inputs, read into contiguous host buffers, validate stored data, and execute I/O under the backend policy. |
| `NvmeKvConnector` | Initialize one namespace from explicit common configuration and forward commands to its executor. |
| `NvmeKvCommandExecutor` | Submit NVMe KV commands and manage buffers and completions through ioctl or io_uring. |

Each namespace object store uses `SharedNvmeKvPolicy` to derive physical keys from allocation IDs and handle immutable writes.

## Data Model and Placement

Shared replicas carry a descriptor containing the payload size and allocation identity:

```cpp
struct SharedKvDescriptor {
    uint64_t object_size;
    uint64_t allocation_id;
};
```

Master assigns a new ID for each allocation, so recreating a logical key uses a different physical key. The shared policy encodes the allocation ID and part index directly into a fixed 16-byte NVMe KV key:

```text
physical_key = BE64(allocation_id) || BE64(part_index)
```

`BE64` denotes an unsigned 64-bit integer in big-endian byte order. Current objects use the inline layout with `part_index = 0`. This encoding avoids physical-key hash collisions and keeps successive allocations separate. The value uses the existing object format: a header, stored logical and physical identity, and payload. The complete encoded value, including transfer padding, must fit within the executor's effective value limit.

Namespace selection uses `XXH32(logical_key, seed=0) % namespace_count`; this hash selects where to send the command, while the allocation ID determines its physical key. Every client must therefore use the same ordered namespace list. Local device paths may differ between hosts, but corresponding entries must identify the same namespace, and the mapping must remain fixed while allocations exist, even when a namespace is temporarily unavailable. Cross-node identity and ordering checks are not yet implemented.

## Write Path

1. `PutStart` creates a `PROCESSING` replica and returns its allocation ID.
2. The client selects the namespace and passes one or more CPU input slices to the object store, which gathers and encodes the complete inline value and submits Store-if-not-exists.
3. `PutEnd` carries the allocation ID; Master checks it against the current write before marking the successful shared replica `COMPLETE`.

Publication and revocation use the allocation ID returned by `PutStart`, so a delayed completion or `PutRevoke` cannot change a newer allocation.

Values are immutable. If Store reports that the physical key already exists, the shared policy retrieves the stored value and compares it byte-for-byte with the complete encoded value being written. Identical values are accepted as a successful retry; different values return `INVALID_WRITE`. Physical reclamation is not implemented yet.

## Read Path

1. The reader queries Master for a complete replica and its read lease.
2. The client selects the namespace and retrieves the encoded value.
3. The object layer validates identity, length, and checksums before copying the payload to the output buffer.

Backend reads require a complete object and a contiguous CPU output buffer. RealClient passes caller buffers directly to Client, which combines adjacent slices into one output Slice without copying payload data. The object layer then copies the validated inline payload from the retrieved value into that destination. Shared reads do not use RealClient's host staging and scatter path, so direct backend reads into GPU or non-contiguous buffers are unsupported.

## Failure Semantics

`IsExist` and `BatchIsExist` consult Master metadata without reading the Target, so they cannot confirm that the stored value is still available. Reads validate the actual value and report failures per object, allowing other valid reads in the batch to complete. A definite miss invalidates only the replica matching the queried allocation ID; other read errors preserve the descriptor.

| Condition | Behavior |
| --- | --- |
| Store reports an existing entry | Retrieve and compare the complete encoded value. Accept an identical value; return `INVALID_WRITE` for different content. |
| Other Store failure | Do not publish the shared replica. Normal write finalization determines whether other replicas satisfy the write. No physical Delete is issued. |
| Target returns NotFound | Return `OBJECT_NOT_FOUND` and ask Master to invalidate only the complete shared replica matching the queried allocation ID. |
| Identity, length, or checksum mismatch | Fail the read without returning the value as a valid object. |
| Read lease expires | Return `LEASE_EXPIRED`; the caller must query again before retrying. |
| Completion or revocation carries a stale allocation ID | Reject the request without changing the current allocation. |
| Retrieve fails without a definite NotFound | Return the error and preserve the descriptor; do not switch transports or physically delete the value. |
| Shared backend read uses a GPU destination or non-contiguous output | Return `NOT_SUPPORTED`. |
| Partial-object read selects a shared replica | Return `INVALID_REPLICA`. |

## Configuration

The current implementation requires a fresh, empty dedicated storage pool for each Master run because allocation IDs reset on restart. Removing or revoking replicas changes metadata only and does not release Target capacity. Recovery and reclamation are listed under [Current limitations](#current-limitations).

Before starting clients, connect the same KV namespaces on every participating host using the system's NVMe-oF tooling. Set `MOONCAKE_SHARED_NVME_KV_CONFIG` to the path of your JSON configuration file before normal C++ or Python setup; there is no default file path, and leaving the variable unset disables shared KV.

```sh
export MOONCAKE_SHARED_NVME_KV_CONFIG=/path/to/shared-kv.json
```

```json
{
  "namespaces": [
    {
      "device_path": "/dev/ng0n1",
      "nsid": 1,
      "transport": "auto",
      "queue_depth": 256,
      "runtime_transfer_limit": 270336
    }
  ]
}
```

Each entry initializes a `NvmeKvConnector` and object store for one namespace, using the common connection options and defaults. The client installs the namespace list only after all executors initialize successfully. `transport` selects the local command submission API: `auto` tries io_uring when compiled in and falls back to ioctl during initialization, while explicit `io_uring` or `ioctl` selection fails if that executor cannot initialize.

The example uses the common Connector defaults. Transfer lengths are padded to a multiple of `MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES`, which defaults to 4096 bytes and is configured through the client process environment rather than the namespace JSON. The effective value limit is the smaller of `runtime_transfer_limit` and `MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE`, rounded down to this alignment. Objects whose encoded values exceed that limit are rejected.

After normal setup, select shared-only writes with `ReplicateConfig`:

```python
from mooncake.store import ReplicateConfig

config = ReplicateConfig()
config.replica_num = 0
config.shared_nvme_kv_replica_num = 1
assert store.put("example-key", b"immutable-value", config) == 0
```

C++ uses the same fields on `mooncake::ReplicateConfig`.

## Current limitations

The backend currently supports immutable inline objects with one active Master, dedicated namespaces, fixed routing, the default tenant, and one shared replica per object.

- Allocation IDs and shared replica state are not recovered across Master restarts. Persistent ID allocation, shared metadata recovery, and HA continuity are not implemented.
- Removing or revoking a shared replica changes Master metadata only and does not release Target capacity. Master LRU and physical reclamation are not implemented. The Target must not independently evict managed values.
- Shared objects must fit in one device value. Chunked shared objects and scatter-output reads remain future work.
- Upsert, range/session writes, and pin requests do not support shared replicas.
