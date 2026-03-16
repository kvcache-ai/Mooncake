# SSD Offload

## Overview

Mooncake Store supports offloading KV cache objects from distributed memory to local SSD. When memory pressure is high, the master instructs clients to persist selected objects to disk. On a cache miss, the client automatically falls back to reading from SSD.

SSD offload is currently **only available in Real Client mode**. The real client is a standalone process that communicates with the application (e.g., SGLang) via RPC. All SSD reads and writes happen within this process.

## Startup Steps

### Step 1: Create the SSD storage directory

```bash
mkdir -p /nvme/mooncake_offload
```

### Step 2: Start the master

```bash
mooncake_master \
    --rpc_port=50051 \
    --enable-offload true
```

### Step 3: Start the real client with SSD offload enabled

Use the `--enable_offload` flag to enable SSD offload, and set environment variables to specify the storage path and backend:

```bash
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend

mooncake_client \
    --master_server_address=127.0.0.1:50051 \
    --host=<machine IP> \
    --protocol="rdma"  \
    --device_names=<NIC name, e.g. eth0> \
    --port=50052 \
    --global_segment_size="4 GB" \
    --enable_offload=true  \
    --metadata_server="P2PHANDSHAKE"
```

> **Note:** On startup, the real client automatically scans existing SSD data and reports it to the master. No manual recovery is needed.

### Step 4: Connect the application to the real client

The application (e.g., SGLang) connects to the real client via the `MooncakeDistributedStore` Python SDK. SSD offload and fallback loading are handled transparently.

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    local_hostname="<machine IP>",
    metadata_server="P2PHANDSHAKE",
    global_segment_size=4 * 1024 * 1024 * 1024,   # 4 GB
    local_buffer_size=512 * 1024 * 1024, #512MB
    protocol="rdma",
    device_name="eth0",
    master_server_address="127.0.0.1:50051",
)
```

---

## Real Client Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--master_server_address` | `127.0.0.1:50051` | Master address |
| `--host` | `0.0.0.0` | This machine's externally reachable IP |
| `--port` | `50052` | Real client RPC listening port |
| `--device_names` | ` ` | NIC name(s), e.g. `eth0` or `mlx5_0` |
| `--protocol` | `tcp` | Transport protocol: `tcp` or `rdma` |
| `--global_segment_size` | `4 GB` | Memory pool size allocated for this node |
| `--enable_offload` | `false` | **Must be set to `true` to enable SSD offload** |
| `--threads` | `1` | Number of RPC server threads |

---

## SSD Offload Configuration

### Core settings

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | `/data/file_storage` | Absolute path to the SSD storage directory |
| `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR` | `bucket_storage_backend` | Storage backend type (see below) |
| `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` | `1342177280` (1.25 GB) | Client-side staging buffer size |
| `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | `2199023255552` (2 TB) | Maximum disk usage |
| `MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT` | `10000000` | Maximum number of objects on disk |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | `10` | Interval for offload heartbeat to master (seconds) |
| `MOONCAKE_USE_URING` | `false` | Enable io_uring for async file I/O |

### Bucket backend settings

Applies when `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend`.

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES` | `268435456` (256 MB) | Max size per bucket |
| `MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT` | `500` | Max keys per bucket |
| `MOONCAKE_BUCKET_MAX_TOTAL_SIZE` | `0` | Eviction threshold in bytes. When set to `0`, the backend uses **90% of the physical disk capacity** as the quota — it does not mean unlimited. Set an explicit value to control disk usage precisely. |
| `MOONCAKE_BUCKET_EVICTION_POLICY` | `none` | Eviction policy: `none` / `fifo` / `lru` |

---

## Storage Backends

### `bucket_storage_backend` (recommended)

Groups multiple objects into bucket files. Reduces filesystem overhead, supports efficient batch I/O, and supports FIFO and LRU eviction.

**File layout:**
```
/nvme/mooncake_offload/
├── 1710000000000-0.bucket   # data file (multiple KV pairs)
├── 1710000000000-0.meta     # metadata file
├── 1710000000001-0.bucket
└── ...
```

Best for: general-purpose use, large-scale deployments.

### `file_per_key_storage_backend`

Stores each object in an individual file. Simple and easy to inspect, but generates many small files at scale.

Best for: debugging or small-scale deployments.

### `offset_allocator_storage_backend`

Pre-allocates a single large file and manages offset-based allocation within it. Highest concurrency via 1024-shard metadata.

> **Warning:** This backend does **not** support metadata recovery on restart. On initialization, the data file is truncated and all in-memory metadata is cleared. Any previously offloaded objects become inaccessible after a process restart.

**Capacity:** `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` is used directly as the pre-allocated file size (100%, no safety margin). Unlike `bucket_storage_backend`, there is no separate quota variable — this is the sole disk usage control. Set it below the physical disk capacity to avoid filling the disk; writes are rejected once usage reaches this limit.

Best for: high-concurrency scenarios with many small objects where restart durability is not required.

---

## Eviction (Bucket Backend Only)

When `MOONCAKE_BUCKET_MAX_TOTAL_SIZE` is set, the backend automatically evicts buckets before writing new ones if total disk usage would exceed the limit.

| Policy | Behavior |
|--------|----------|
| `none` | No eviction (default); writes fail when disk is full |
| `fifo` | Evict the oldest bucket first |
| `lru` | Evict the least recently read bucket first |

Eviction is two-phase: the bucket is removed from metadata and master is notified first, then in-flight reads are drained before files are deleted.

---

## Example

The following example starts a master and a real client on a single machine.

### Environment

- Machine IP: `192.168.1.10`
- NIC: `eth0`
- SSD mount point: `/nvme`
- Memory pool size: 4 GB (smaller than the total data written, to trigger offload)

### Start the master

```bash
mooncake_master \
    --rpc_port=50051
```

### Start the real client (new terminal)

```bash
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend
export MOONCAKE_BUCKET_MAX_TOTAL_SIZE=$((200 * 1024 * 1024 * 1024))  # 200 GB
export MOONCAKE_BUCKET_EVICTION_POLICY=lru

mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.10" \
    --device_names="eth0" \
    --port=50052 \
    --protocol="rdma" \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

---

## Notes

- `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` must be an absolute path to an existing, writable directory. Symbolic links and paths containing `..` are rejected.
- On real client restart, the backend automatically scans existing SSD files and reports them to the master, so previously offloaded objects remain accessible.
- Eviction only notifies the master and deletes local files; objects replicated on other nodes are unaffected.
- Each machine requires its own real client process. In multi-node deployments, ensure `--host` and `--port` are correctly set so nodes can reach each other.

**2-node example:** suppose Node A (`192.168.1.10`) runs the master and Node B (`192.168.1.11`) is a second worker. Both real clients must point to the same master and advertise their own externally reachable IP:

```bash
# Node A — runs the master and its own real client
mooncake_master --rpc_port=50051 --enable-offload true &

export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.10" \        # externally reachable IP of Node A
    --device_names="eth0" \
    --protocol="rdma" \
    --metadata_server="P2PHANDSHAKE" \
    --port=50052 \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

```bash
# Node B — real client only; points to the same master on Node A
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.11" \        # externally reachable IP of Node B, NOT 127.0.0.1
    --device_names="eth0" \
    --protocol="rdma" \
    --metadata_server="P2PHANDSHAKE" \
    --port=50052 \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

---

## Troubleshooting

### SSD offload is not triggering

- Confirm `--enable_offload=true` is passed to `mooncake_client` and `--enable-offload true` is passed to `mooncake_master`.
- Check that `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` points to an existing, writable directory. The client will fail silently if the path is invalid.
- Verify memory pressure is actually high enough for the master to trigger offload. If the memory pool (`--global_segment_size`) is large relative to the data written, offload may never activate.

### "Permission denied" or "No such file or directory" on the storage path

- Ensure the directory exists before starting the client: `mkdir -p <path>`.
- Confirm the process user has read/write access to the directory.
- Symbolic links and paths containing `..` are rejected — use an absolute, canonical path.

### "Failed to register buffer with UringFile" warning in logs

This warning appears when `MOONCAKE_USE_URING=true` and the io_uring fixed-buffer registration fails. The most common cause is that `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` exceeds the process's locked-memory limit (`RLIMIT_MEMLOCK`). io_uring requires the registered buffer to be pinned in physical memory, which counts against this limit.

Check the current limit:

```bash
ulimit -l        # in KB; "unlimited" means no cap
```

To raise it for the current session:

```bash
ulimit -l unlimited
```

To raise it permanently, add the following to `/etc/security/limits.conf`:

```
* soft memlock unlimited
* hard memlock unlimited
```

Alternatively, reduce `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` to a value within the existing limit. Note that the warning does not abort startup — the client falls back to non-fixed-buffer I/O — but performance may be lower than expected.
