# SSD Offload

## Overview

Mooncake Store supports offloading KV cache objects from distributed memory to local SSD. When memory pressure is high, the master instructs clients to persist selected objects to disk. On a cache miss, the client automatically falls back to reading from SSD.

SSD offload is currently **only available in Real Client mode**. The real client is a standalone process that communicates with the application (e.g., vLLM) via RPC. All SSD reads and writes happen within this process.

---

## Installation

After installing the whl package, the `mooncake_master` and `mooncake_client` commands are automatically added to PATH:

```bash
pip install mooncake-transfer-engine
```

Verify the installation:

```bash
mooncake_master --help
mooncake_client --help
```

---

## Startup Steps

### Step 1: Create the SSD storage directory

```bash
mkdir -p /nvme/mooncake_offload
```

### Step 2: Start the master

```bash
mooncake_master \
    --port=50051 \
    --enable_http_metadata_server=true \
    --http_port=8080
```

### Step 3: Start the real client with SSD offload enabled

Use the `--enable_offload` flag to enable SSD offload, and set environment variables to specify the storage path and backend:

```bash
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend

mooncake_client \
    --master_server_address=127.0.0.1:50051 \
    --host=<machine IP> \
    --device_names=<NIC name, e.g. eth0> \
    --port=50052 \
    --global_segment_size="4 GB" \
    --enable_offload=true
```

> **Note:** On startup, the real client automatically scans existing SSD data and reports it to the master. No manual recovery is needed.

### Step 4: Connect the application to the real client

The application (e.g., vLLM) connects to the real client via the `MooncakeDistributedStore` Python SDK. SSD offload and fallback loading are handled transparently.

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    local_hostname="<machine IP>",
    metadata_server="127.0.0.1:2379",   # etcd, or http://127.0.0.1:8080/metadata
    global_segment_size=4 * 1024 * 1024 * 1024,   # 4 GB
    local_buffer_size=512 * 1024 * 1024,           # 512 MB
    protocol="tcp",
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
| `MOONCAKE_BUCKET_MAX_TOTAL_SIZE` | `0` (unlimited) | Eviction threshold in bytes |
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

Best for: high-concurrency scenarios with many small objects.

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
    --port=50051 \
    --enable_http_metadata_server=true \
    --http_port=8080
```

### Start the real client (new terminal)

```bash
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend
export MOONCAKE_BUCKET_MAX_TOTAL_SIZE=$((200 * 1024 * 1024 * 1024))  # 200 GB
export MOONCAKE_BUCKET_EVICTION_POLICY=lru

mooncake_client \
    --master_server_address=192.168.1.10:50051 \
    --host=192.168.1.10 \
    --device_names=eth0 \
    --port=50052 \
    --global_segment_size="4 GB" \
    --enable_offload=true
```

---

## Notes

- `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` must be an absolute path to an existing, writable directory. Symbolic links and paths containing `..` are rejected.
- On real client restart, the backend automatically scans existing SSD files and reports them to the master, so previously offloaded objects remain accessible.
- Eviction only notifies the master and deletes local files; objects replicated on other nodes are unaffected.
- Each machine requires its own real client process. In multi-node deployments, ensure `--host` and `--port` are correctly set so nodes can reach each other.
