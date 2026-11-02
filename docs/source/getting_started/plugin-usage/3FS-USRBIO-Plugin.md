# Mooncake HF3FS USRBIO Adapter (Experimental)

```{warning}
**Work in progress / experimental.** Descriptor-based DFS and its HF3FS (3FS
USRBIO) adapter are under development and are not production-ready. They are
not covered by Mooncake Store's general fault-tolerance, HA continuity,
durability, or multi-tenant guarantees. Behavior, build flags, and
configuration may change without notice. Use only for evaluation and testing.
```

This adapter implements the HF3FS native USRBIO data plane for Mooncake Store's
descriptor-based DFS replicas. The master allocates ranges in shared shard
files, and clients use USRBIO to access the ranges described by the replica
metadata.

The adapter is not enabled by the legacy `--root_fs_dir` option. It also does
not automatically fall back to POSIX I/O; select
`MOONCAKE_DFS_FS_ADAPTER=posix` explicitly when POSIX behavior is required.

## Prerequisites

### 1. HF3FS installation

- Build and install [3FS](https://github.com/deepseek-ai/3FS/)
- Required library: `libhf3fs_api_shared.so` (Default location: `3FS_PATH/build/src/lib/api`)
  → Install to: `/usr/lib/`
- Required header: `hf3fs_usrbio.h` (Default location: `3FS_PATH/src/lib/api`)
  → Install to: `/usr/include/`

### 2. Mooncake build

Enable HF3FS support during CMake configuration:

```bash
cmake -DUSE_3FS=ON ...
```

Then build and install Mooncake as usual.

## USRBIO resource settings

The adapter reads these optional environment variables once during
initialization:

| Variable | Default | Meaning |
| --- | --- | --- |
| `MOONCAKE_HF3FS_IOV_SIZE` | `16777216` (16 MiB) | Staging buffer size in bytes per ring entry |
| `MOONCAKE_HF3FS_IOR_ENTRIES` | `8` | Requested entries in each thread's read and write ring |

Each I/O thread allocates one shared staging buffer of
`MOONCAKE_HF3FS_IOR_ENTRIES * MOONCAKE_HF3FS_IOV_SIZE` bytes for its read and
write rings. The defaults allocate `8 * 16 MiB = 128 MiB` per thread.

For example, allocate 2 GiB of staging memory per I/O thread:

```bash
export MOONCAKE_HF3FS_IOV_SIZE=67108864  # 64 MiB per ring entry
export MOONCAKE_HF3FS_IOR_ENTRIES=32    # Entries per read/write ring
```

Set these variables before starting each process that uses the HF3FS adapter.
They are process-local tuning settings and may differ between clients and the
master. Resources are allocated lazily on first I/O using the settings captured
at adapter initialization; restart the process to apply changes.

Values must be positive decimal integers. The product of the two settings must
fit in `ptrdiff_t`, and ring entries must be less than `INT_MAX` because 3FS
reserves one additional internal slot. Empty, malformed, zero, negative, or
out-of-range values fail adapter initialization. Allocation can still fail if
available shared memory or 3FS limits are exceeded.

Each ring entry's staging slice is registered with 3FS as a separate buffer
block, so `MOONCAKE_HF3FS_IOV_SIZE` must stay within the 3FS client's
buffer-registration limit. Otherwise buffer creation still succeeds, but every
I/O through the adapter fails; on a local test cluster, entries larger than
1 GiB failed with permission errors.

Staging memory scales with both the ring entry count and the number of I/O
threads: the example above uses 16 GiB with eight I/O threads, plus ring
overhead. Each active request uses a separate slice of up to
`MOONCAKE_HF3FS_IOV_SIZE` bytes; larger requests use multiple waves. Increasing
ring entries increases total staging memory and preserves the per-entry size.

## Failed writes and range reuse

After a fatal completion-wait error, the adapter retires the thread's client
rings and staging buffer. This does not cancel writes already accepted by a
3FS worker: they can still land in the original inode after failure is returned.
Closing the file descriptor or recreating the rings does not make the failed
file ranges safe to reuse.

Conservatively abandon allocated ranges for positional requests that return
`FILE_WRITE_FAIL`. Retry at fresh, nonoverlapping offsets or in a new inode;
do not overwrite or recycle the failed ranges unless all earlier I/O is known
to have finished. The adapter cannot establish that after a fatal wait error.

## Usage

### Master

Enable descriptor-based DFS and point it at a directory on the shared HF3FS
mount:

```bash
export MOONCAKE_ENABLE_DFS=1
export MOONCAKE_DFS_ROOT_DIR=/mnt/3fs/mooncake
export MOONCAKE_DFS_FS_ADAPTER=hf3fs
export MOONCAKE_DFS_SHARD_COUNT=64
export MOONCAKE_DFS_SHARD_CAPACITY=4294967296
export MOONCAKE_DFS_ALIGNMENT=4096
export MOONCAKE_DFS_SINGLE_TENANT=true

./build/mooncake-store/src/mooncake_master [other master arguments]
```

The master creates and preallocates the configured shard files during startup.
Ensure the mount is available, writable, and has enough capacity before
starting the process.

### Clients

Every client that may read or write DFS replicas must use the same root,
adapter, shard count, shard capacity, and alignment. The DFS root must be an
absolute path and use the same path string in every process. For the standalone
store service:

```bash
export MOONCAKE_OFFLOAD_ENABLED=true
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=distributed_storage_backend
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/data/file_storage
export MOONCAKE_MASTER=127.0.0.1:50051
export MOONCAKE_DFS_ROOT_DIR=/mnt/3fs/mooncake
export MOONCAKE_DFS_FS_ADAPTER=hf3fs
export MOONCAKE_DFS_SHARD_COUNT=64
export MOONCAKE_DFS_SHARD_CAPACITY=4294967296
export MOONCAKE_DFS_ALIGNMENT=4096
export MOONCAKE_DFS_SINGLE_TENANT=true

python -m mooncake.mooncake_store_service
```

`MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` must already be an absolute, writable,
non-symlink directory. DFS shard data is stored under
`MOONCAKE_DFS_ROOT_DIR`; the separate FileStorage path is still validated
during client initialization.

For the complete configuration reference, request example, synchronous write
semantics, and current recovery limitations, see the {ref}`DFS deployment
documentation <dfs-storage>`.
