# Mooncake HF3FS USRBIO Adapter

> **Work in progress / experimental.** Descriptor-based DFS and this HF3FS
> adapter are intended for development and evaluation only. They are not
> production-ready or covered by Mooncake Store's general fault-tolerance, HA
> continuity, durability, or multi-tenant guarantees.

This adapter implements the HF3FS native USRBIO data plane for Mooncake Store's
descriptor-based DFS replicas. It is selected explicitly with
`MOONCAKE_DFS_FS_ADAPTER=hf3fs`; the legacy `--root_fs_dir` option does not
enable it, and it does not automatically fall back to POSIX I/O.

Descriptor batches use the per-thread USRBIO read or write ring to keep several
positional requests in flight. Each request owns a separate slice of the shared
staging buffer until its completion is reaped. Batches larger than the ring and
requests larger than a staging slice are processed in bounded waves; completion
order does not change the order of returned results. A short transfer or an I/O
error fails only the affected request. A failure to reap completions retires the
thread's rings and staging buffer before another call can reuse them.

The `hf3fs_adapter_batch_test` unit test uses a fake USRBIO API and needs no
mounted cluster. `dfs_hf3fs_test` exercises the real API, including batches that
exceed the ring and per-entry buffer capacities, using `MOONCAKE_DFS_ROOT_DIR`
(default: `/mnt/3fs/mooncake_test`). Both targets are built with `USE_3FS=ON`.

## Prerequisites

### 1. HF3FS installation

- Build and install [3FS](https://github.com/deepseek-ai/3FS/).
- Install `libhf3fs_api_shared.so` from `3FS_PATH/build/src/lib/api` in a
  library search path such as `/usr/lib/`.
- Install `hf3fs_usrbio.h` from `3FS_PATH/src/lib/api` in an include search
  path such as `/usr/include/`.

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
| `MOONCAKE_HF3FS_IOV_SIZE` | `33554432` (32 MiB) | Staging buffer size in bytes per ring entry |
| `MOONCAKE_HF3FS_IOR_ENTRIES` | `16` | Requested entries in each thread's read and write ring |

Each I/O thread allocates one shared staging buffer of
`MOONCAKE_HF3FS_IOR_ENTRIES * MOONCAKE_HF3FS_IOV_SIZE` bytes for its read and
write rings. The defaults allocate `16 * 32 MiB = 512 MiB` per thread.

For example, configure 64 MiB per entry and 32 entries per ring (2 GiB of
staging memory per thread) before starting a process that uses the adapter:

```bash
export MOONCAKE_HF3FS_IOV_SIZE=67108864
export MOONCAKE_HF3FS_IOR_ENTRIES=32
```

Values must be positive decimal integers. The product of the two settings must fit in
`ptrdiff_t`, and ring entries must be less than `INT_MAX` because 3FS reserves
one additional internal slot. Empty, malformed, zero, negative, or out-of-range
values fail adapter initialization. Resource allocation can still fail when
the configured sizes exceed available shared memory or 3FS limits.

These are process-local tuning settings and may differ between clients and the
master. Thread resources are allocated lazily on first I/O and retain the
settings captured at initialization. Restart the process to apply changes.
Staging memory scales with both the ring entry count and the number of I/O
threads. Each active request uses a separate slice of up to
`MOONCAKE_HF3FS_IOV_SIZE` bytes; larger requests use multiple waves. Increasing
ring entries increases total staging memory and preserves the per-entry size.

## Usage

Configure the master with a shared HF3FS root and shard layout:

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

Every client that may read or write DFS replicas must initialize FileStorage's
distributed backend with the same absolute root path and layout. Use the same
root path string in every process:

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
non-symlink directory. For the full configuration and current limitations, see
the [DFS deployment documentation](../../../docs/source/deployment/mooncake-store-deployment-guide.md#dfs-storage).
