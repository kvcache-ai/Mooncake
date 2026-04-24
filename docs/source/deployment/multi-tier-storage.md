# Multi-Tier Storage

Mooncake Store supports a **multi-tier storage hierarchy** that extends the in-memory KV cache with a persistent distributed filesystem (DFS) layer. This enables workloads that cannot fit entirely in GPU/CPU memory to spill objects to a fast DFS backend (e.g., 3FS or any POSIX-compatible filesystem).

## Tier Overview

| Tier | Storage Medium | Access Path | Typical Latency |
|------|---------------|-------------|----------------|
| **G1** | GPU VRAM (on each client node) | Direct VRAM read/write | < 1 µs |
| **G2** | CPU DRAM (on each client node) | RDMA or local memcpy | 1–10 µs |
| **G3** | DFS (shared filesystem) | POSIX / 3FS USRBIO | 100 µs–ms |

Objects flow down the hierarchy as memory pressure increases (G1 → G2 → G3) and are promoted back up on a cache hit.

## Architecture

```
  Client Node 1                         Shared DFS
 ┌──────────────────┐                  ┌──────────────┐
 │ G1: GPU VRAM     │                  │ G3: 3FS /    │
 │ G2: CPU DRAM     │ ── POSIX/USRBIO──│    NFS /     │
 └──────────────────┘                  │    GPFS …    │
                                        └──────────────┘
  Client Node 2
 ┌──────────────────┐
 │ G1: GPU VRAM     │
 │ G2: CPU DRAM     │
 └──────────────────┘

   ↑ All clients share G3 via the DFS mount point
```

The master (`mooncake_master`) manages the G3 segment as a special **DFS segment** registered at startup. Client nodes write KV objects to the DFS when instructed by the master's eviction policy.

## Configuration

### Master Flags

The following flags enable and configure G3 DFS storage (from [Mooncake Store Deployment Guide](mooncake-store-deployment-guide)):

| Flag | Default | Description |
|------|---------|-------------|
| `--root_fs_dir` | — (empty, G3 disabled) | DFS mount directory. When set, the master registers a DFS-backed segment at startup. |
| `--global_file_segment_size` | `INT64_MAX` | Maximum bytes the DFS segment may occupy. Set to the available DFS capacity. |

### Client Flags

Client nodes do not need additional flags for DFS; the master instructs clients to write to the DFS segment automatically during eviction. Ensure the DFS mount is accessible from every client node at the path specified in `--root_fs_dir`.

## Deployment Example

### Step 1: Mount the DFS on All Nodes

```bash
# Example: mount 3FS (see docs/source/getting_started/plugin-usage/3FS-USRBIO-Plugin.md)
mount -t 3fs <3fs_server>:/kvcache /mnt/3fs

# Example: mount NFS
mount -t nfs <nfs_server>:/kvcache /mnt/kvcache
```

### Step 2: Start the Master with G3 Enabled

```bash
mooncake_master \
    --rpc_port=50051 \
    --enable_http_metadata_server=true \
    --http_metadata_server_port=8080 \
    --root_fs_dir=/mnt/3fs/mooncake \
    --global_file_segment_size=107374182400   # 100 GB
```

The master will create the DFS segment directory under `--root_fs_dir` and register it as a G3 segment.

### Step 3: Start Client Nodes (unchanged)

```bash
mooncake_client \
    --master_server_address=<master_ip>:50051 \
    --host=<client_ip> \
    --protocol=rdma \
    --device_names=mlx5_0 \
    --global_segment_size="32GB" \
    --metadata_server="P2PHANDSHAKE"
```

### Step 4: Connect the Application

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    local_hostname="<client_ip>",
    metadata_server="P2PHANDSHAKE",
    global_segment_size=32 * 1024 ** 3,
    local_buffer_size=4 * 1024 ** 3,
    protocol="rdma",
    device_name="mlx5_0",
    master_server_address="<master_ip>:50051",
)
```

G3 eviction and promotion are fully transparent to the application.

## 3FS USRBIO Integration

For maximum DFS throughput, Mooncake supports the **3FS USRBIO** (User-space Block IO) interface, which bypasses the kernel page cache and achieves near-NVMe throughput from user space.

To enable:
1. Build Mooncake with `USE_3FS=ON` and ensure the 3FS USRBIO plugin is installed (see [3FS USRBIO Plugin Guide](../getting_started/plugin-usage/3FS-USRBIO-Plugin)).
2. Mount 3FS with USRBIO support enabled.
3. Pass `--root_fs_dir` pointing to the 3FS mount.

When USRBIO is not available, Mooncake falls back to standard POSIX `read`/`write` calls.

## Eviction Policy

The master controls which objects are evicted from G1/G2 to G3 using the same watermark-based policy as SSD offload:

| Flag | Default | Description |
|------|---------|-------------|
| `--eviction_high_watermark_ratio` | `0.95` | Memory usage fraction that triggers eviction |
| `--eviction_ratio` | `0.05` | Fraction of objects evicted per cycle |
| `--allow_evict_soft_pinned_objects` | `true` | Whether soft-pinned objects can be evicted to G3 |

Hard-pinned objects are never evicted to G3.

## Performance Tips

- **Place the DFS close to clients**: Use a high-bandwidth interconnect (e.g., InfiniBand / 100GbE) between clients and the DFS storage nodes.
- **Use 3FS USRBIO** for best throughput when writing large KV cache tensors.
- **Size `--global_file_segment_size`** conservatively: set it to 80–90 % of actual available DFS capacity to leave room for snapshots and other data.
- **Monitor G3 usage** via the `/metrics` endpoint: `mooncake_master_dfs_segment_used_bytes`.

## See Also

- [SSD Offload](ssd-offload) — local NVMe offload (single-node)
- [3FS USRBIO Plugin](../getting_started/plugin-usage/3FS-USRBIO-Plugin) — 3FS setup and USRBIO configuration
- [Mooncake Store Deployment Guide](mooncake-store-deployment-guide) — all master flags
