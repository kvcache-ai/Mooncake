# TPU (PJRT) platform for TENT

This directory implements Google TPU support for the TENT transfer engine.
It is compiled only when TENT is built with `-DUSE_TPU=ON` (OFF by default).

## Why staging

TPU HBM is not addressable by the NIC, so a transfer that touches TPU memory
cannot be issued directly by RDMA/TCP. Instead every such transfer is **staged
through host DRAM** and the two hops are chained by the existing
`ProxyManager` pipeline:

```
  TPU HBM  <->  host DRAM        (PJRT device copy, this platform)
  host DRAM <->  remote host DRAM (RDMA / TCP, existing transports)
  remote host DRAM <-> remote TPU HBM (PJRT device copy on the peer)
```

No new networked transport is required — TENT reuses `ProxyManager`'s chunked
double-buffering, staging-buffer lifecycle, and async status tracking.

> New to the codebase and wondering why this needs a `TpuTransport` at all
> instead of a one-line branch in `ProxyManager`? See
> [`STAGING_ARCHITECTURE.md`](STAGING_ARCHITECTURE.md).

## Components

| File | Role |
|------|------|
| `tpu_platform.cpp` (`TpuPlatform`) | Derives `CpuPlatform`. Host DRAM + NIC topology are inherited (RDMA NICs if present, otherwise just host NUMA nodes for TCP); only the TPU-device-aware paths are overridden (`copy`, `getMemoryType`, `getLocation`, and one `MemEntry` per device in `probe`). |
| `tpu_pjrt_shim.cpp` (`TpuPjrtShim`) | Isolates all PJRT dependency. Resolves the device-copy adapter at runtime via `dlopen`. |
| `../../transport/tpu/tpu_transport.cpp` (`TpuTransport`) | The local HBM↔host staging executor. Advertises only `gpu_to_dram` / `dram_to_gpu`; runs the copy via `Platform::copy` for `LOCAL_SEGMENT_ID` requests. |

The staging policy that ties these together lives in
`TransferEngineImpl::findStagingPolicy` (case "TPU").

## The device-copy adapter (runtime dependency)

`TpuPjrtShim` does **not** link the PJRT runtime. Instead it loads an adapter
shared library at runtime that exports the C ABI declared in
[`tpu_pjrt_abi.h`](../../../include/tent/platform/tpu_pjrt_abi.h):

```c
int mc_tpu_pjrt_init(void);
int mc_tpu_pjrt_is_device_ptr(const void *addr);
int mc_tpu_pjrt_device_index(const void *addr);
int mc_tpu_pjrt_copy_d2h(void *host_dst, const void *device_src, size_t len);
int mc_tpu_pjrt_copy_h2d(void *device_dst, const void *host_src, size_t len);
int mc_tpu_pjrt_device_count(void);
int mc_tpu_pjrt_device_numa(int index);
```

- **Discovery:** the library path defaults to `libmooncake_tpu_pjrt.so` and can
  be overridden with the `MC_TPU_PJRT_LIB` environment variable.
- **Graceful absence:** if the adapter cannot be loaded or does not satisfy the
  ABI, `TpuPjrtShim::available()` returns false and all TPU operations return a
  non-OK `Status`. A `USE_TPU` build therefore links and runs without the
  runtime present (useful for CI and unit tests).
- **Pointer tokens:** the `const void *` "device pointer" is the stable token the
  serving-engine integration registers with TENT for a TPU buffer (via a
  `tpu:N` location). The adapter owns the mapping from that token to the
  underlying PJRT buffer. The token is **not** the buffer's data and must never
  be dereferenced — on real PJRT it is an internal handle that is
  host-readable but reads as unrelated bytes.
- **Interior pointers:** `ProxyManager` stages a transfer in `chunk_size`
  (4 MiB) pieces and passes `token + chunk_offset` for every chunk after the
  first. Classification and copy entrypoints must therefore resolve an address
  to the registered buffer whose range contains it. An adapter that only matches
  base addresses makes TENT classify HBM as host memory, and — because the token
  *is* readable — the staging copy degrades into a `memcpy` of unrelated bytes
  that reports success. `TpuTransport` defends against this by requiring exactly
  one TPU-device side per staging hop and failing loudly otherwise.

A mock adapter and unit tests that exercise this ABI on any Linux host live in
[`../../../tests/tpu/`](../../../tests/tpu/). The mock hands out poisoned tokens
backed by shadow storage, so a copy that bypasses the adapter yields `0xDD`
rather than accidentally producing the right answer.

## Not yet included (follow-ups)

- Serving-framework (JAX / PyTorch-XLA) integration that registers TPU buffers.
- DMA-mapped (pinned) staging buffers for true async device DMA, and the
  interaction between device DMA-mapping and RDMA memory registration on the
  same host buffer.
- Benchmarks on real TPU hardware.
