---
orphan: true
---

# Shared NVMe KV Backend Design

## Overview

`SHARED_NVME_KV` replicas let multiple clients read and write an external NVMe KV storage pool directly through the existing Store APIs.
Published replicas remain readable after the writing client exits.

The [local NVMe KV backend](nvme-kv-backend.md) reports `LOCAL_DISK` replicas through `FileStorage`, and remote readers access them through the owning client.
Shared replicas use Master-assigned allocation identities and direct access from the requesting client to the Target.
They reuse the local backend's object format and Linux ioctl/io_uring executors.

## Architecture

| Component | Responsibility |
| --- | --- |
| Master | Assign allocation IDs, track replica state, and validate publication, revocation, and miss reports. |
| Client | Coordinate Put/Get with Master and route selected shared replicas to the backend. |
| `SharedNvmeKvBackend` | Select a namespace, adapt host slices, and bound batch memory use. |
| `SharedNvmeKv` | Encode inline objects and validate stored identity, length, and checksums. |
| `NvmeKvCommandExecutor` | Submit NVMe KV commands and manage buffers and completions through ioctl or io_uring. |

The external Target executes KV commands and allocates physical capacity when Store succeeds.
Clients connect to its namespaces through Linux device nodes; the kernel manages NVMe-oF connections.
Master assigns identities without reserving Target capacity.

## Data Model and Placement

Shared replicas carry a descriptor containing the payload size and allocation identity:

```cpp
struct SharedKvDescriptor {
    uint64_t object_size;
    uint64_t allocation_id;
};
```

A new allocation ID separates each physical allocation, including successive allocations of the same logical key.
The object layer derives a 16-byte physical key from this ID.
An inline value contains the existing object header, stored logical and physical identity, and payload.
The complete encoded value, including transfer padding, must fit within the executor's effective value limit.

Every client uses the same ordered namespace list and selects a namespace with `XXH32(logical_key, seed=0) % namespace_count`.
Local device paths may differ between hosts, but list entries must refer to the same namespaces in the same order.
Keep this mapping fixed while allocations exist, including when one namespace is temporarily unavailable.
Cross-node namespace identity and ordering checks are not yet implemented.

## Write Path

1. `PutStart` creates a `PROCESSING` replica and returns its allocation ID.
2. The client selects the namespace, encodes the complete inline value, and submits Store-if-not-exists.
3. `PutEnd` carries the allocation ID; Master checks it against the current write before marking the successful shared replica `COMPLETE`.

`PutRevoke` also checks the allocation identity, so a delayed request cannot revoke a newer allocation.
With MEMORY + Shared KV in flexible mode, either successful medium can satisfy the write.

Values are immutable: retries for the same allocation must use the same content.

## Read Path

1. The reader queries Master for a complete replica and its read lease.
2. The client checks the lease, selects the namespace, and retrieves the encoded value.
3. The object layer validates identity, length, and checksums before copying the payload to the output buffer; Client checks the lease again before reporting success.

`IsExist` and `BatchIsExist` consult Master metadata, so a positive result is not a Target read guarantee.

The backend accepts host buffers.
RealClient uses the existing host staging and scatter path for caller-provided buffers, including GPU destinations and partial reads.
Partial reads retrieve and validate the complete inline value before copying the requested range.
The current path includes payload copies.

## Failure Semantics

| Condition | Behavior |
| --- | --- |
| Store reports an existing entry | Retrieve it and validate identity, length, and checksums before accepting success. |
| Other Store failure | Do not publish the shared replica. Normal write finalization determines whether other replicas satisfy the write. No physical Delete is issued. |
| Target returns NotFound | Return `OBJECT_NOT_FOUND` and ask Master to invalidate only the complete shared replica matching the queried allocation ID. |
| Identity, length, or checksum mismatch | Fail the read without returning the value as a valid object. |
| Read lease expires | Return `LEASE_EXPIRED`; the caller must query again before retrying. |
| Completion or revocation carries a stale allocation ID | Reject the request without changing the current allocation. |
| Retrieve fails without a definite NotFound | Return the error and preserve the descriptor; do not switch transports or physically delete the value. |

## Configuration

Connect the dedicated KV namespaces using the system's NVMe-oF tooling before starting clients.
Set `MOONCAKE_SHARED_NVME_KV_CONFIG` to a JSON file before normal C++ or Python setup; leaving it unset disables shared KV.

```sh
export MOONCAKE_SHARED_NVME_KV_CONFIG=/etc/mooncake/shared-kv.json
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

Each entry binds one executor to one namespace and reuses the local NVMe KV connection options and defaults.
`transport` selects the local command submission API: `auto`, `io_uring`, or `ioctl`.
Building with liburing and NVMe `uring_cmd` headers enables io_uring.
`auto` tries io_uring first and falls back to ioctl only during initialization.
Explicit transport selection fails when that executor cannot initialize.
The client installs the namespace list only after all executors initialize successfully.

The JSON example uses the local backend defaults; transfer alignment defaults to 4 KiB.
The effective limit also depends on `MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE` and transfer alignment, as described in the [executor design](nvme-kv-backend.md#executor-design).
Objects exceeding that limit return an error.

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

- Allocation IDs come from an in-memory counter that resets on Master restart, so each Master run requires a fresh, empty dedicated test pool. Persistent ID allocation, shared metadata recovery, and HA continuity are not implemented.
- Removing or revoking a shared replica changes Master metadata only and does not release Target capacity. Master LRU and physical reclamation are not implemented. The Target must not independently evict managed values.
- Objects must fit in one device value; chunked objects are not supported.
- Upsert, range/session writes, pin requests, and the memory-only `prefer_alloc_in_same_node` batch mode do not support shared replicas.
