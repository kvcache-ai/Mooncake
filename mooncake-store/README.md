# Store implementation layout

- `client/`: client API, master RPC client, transfer and storage I/O, device
  integration, client configuration, and the C ABI.
- `master/`: master service and executable, placement, allocation, metadata,
  tenant quotas, snapshots, and HA coordination.
- `common/`: shared implementations and support libraries, including
  `local_ssd/`, `spdk/`, `hf3fs/`, and `cachelib_memory_allocator/`.
  `common/config/spdk_controller_config.*` is used by both the client and the
  master's NoF heartbeat probe.
- `include/client/`, `include/master/`, `include/common/`: headers grouped by
  the same ownership as the implementations. Includes use explicit paths such
  as `client/real_client.h`, `master/master_service.h`, and `common/types.h`.
  The C ABI header is now `client/store_c.h`; consumers should keep `include/`
  as their include search root. The vendored allocator headers remain under
  `include/cachelib_memory_allocator/`.
- `cmake/`: shared, master, and client target definitions, including optional
  backend sources.

There is no source `src/` directory. CMake intentionally retains the historical
`build/mooncake-store/src/` **output** directory so wheel packaging, Go/Rust
bindings, and existing launch scripts keep working.
