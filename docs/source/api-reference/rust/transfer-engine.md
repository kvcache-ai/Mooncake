# Transfer Engine Rust API

This page documents the Rust crate `transfer_engine_rust` (located at
`mooncake-transfer-engine/rust`). It is a **library** wrapper around the
Transfer Engine C API (`transfer_engine_c.h`).

Hot-path types (`TransferRequest`, `BufferEntry`, `TransferStatus`) are
`#[repr(C)]` and compile-time layout-checked against the bindgen C types, so
`submit_transfer` passes a Rust slice to C with **no heap allocation and no
per-request copy** on the Rust side.

For Transfer Engine design docs and non-Rust APIs, see:

- Transfer Engine design docs: `design/transfer-engine/index`
- Transfer Engine C++ API: `api-reference/cpp/transfer-engine`
- Transfer Engine Python API: `api-reference/python/transfer-engine`

## Build & runtime prerequisites

- **Build**:
  - Requires a Rust toolchain and libclang (bindgen).
  - CMake: `-DWITH_RUST_EXAMPLE=ON`, then
    `cmake --build build --target build_transfer_engine_rust`.
  - Or Cargo after exporting `MOONCAKE_BUILD_DIR` / `MOONCAKE_TE_LIB_DIR` /
    `MOONCAKE_TE_INCLUDE_DIR` (see `mooncake-transfer-engine/rust/README.md`).
- **Runtime**:
  - Dynamic linker must find Transfer Engine shared libraries (`libasio.so`, …).
  - A metadata server (HTTP metadata, etcd, or `P2PHANDSHAKE`) must be reachable.
  - GitHub Actions runs `scripts/ci/run_transfer_engine_rust_smoke.sh` after the
    C++ build (`cargo test --lib` plus the TCP loopback `minimal_smoke` test).

## Quick start

```rust
use transfer_engine_rust::{MemoryPool, TransferEngine, TransferRequest, WILDCARD_LOCATION};

fn main() -> Result<(), transfer_engine_rust::EngineError> {
    let engine = TransferEngine::initialize(
        "127.0.0.1:12345",
        "http://127.0.0.1:8080/metadata",
        "tcp",
        "",
    )?;

    let pool = MemoryPool::new(1 << 20);
    unsafe {
        engine.register_local_memory(pool.as_void_ptr(), pool.len(), WILDCARD_LOCATION)?;

        let seg = engine.open_segment("peer:12345")?;
        let req = TransferRequest::write(pool.as_void_ptr(), seg, /*offset*/ 0, 4096);
        engine.submit_and_wait(&[req], None)?;

        engine.unregister_local_memory(pool.as_void_ptr())?;
    }
    Ok(())
}
```

`initialize(local_hostname, metadata_server, protocol, device_name)` matches
the Python constructor. `device_name` is accepted for API compatibility; NIC
filtering is done with `MC_TE_FILTERS` because the C ABI has no device-name
argument.

The lower-level constructors map onto `createTransferEngine`:

- `TransferEngine::new(metadata_uri, local_server_name, rpc_port)`
- `TransferEngine::create(TransferEngineOptions { … })`

## Mental model

- Register local memory regions as RDMA/TCP-capable buffers.
- Open a remote segment to obtain a `SegmentId`.
- Allocate a `BatchId` for a fixed number of requests.
- Submit a `&[TransferRequest]` (zero-copy FFI).
- Poll `get_transfer_status` / `wait_all`, then `free_batch_id`.

Python-shaped helpers (`transfer_sync_write`, `batch_transfer_sync_*`,
`transfer_submit_write`) cache segment ids by hostname. They allocate a
batch internally. Use `submit_transfer` + `wait_all` when you need to keep
the batch/request arrays on the stack.

## API reference

### Types

- `Opcode::{Read, Write}` — `OPCODE_READ` / `OPCODE_WRITE`
- `TransferStatusCode::{Waiting, Pending, Invalid, Canceled, Completed, Timeout, Failed}`
- `TransferRequest { opcode, source, target_id, target_offset, length }` —
  layout matches `transfer_request_t`. Helpers: `TransferRequest::read`,
  `TransferRequest::write`.
- `BufferEntry { addr, length }` — layout matches `buffer_entry_t`
- `TransferStatus { status, transferred_bytes }` — layout matches `transfer_status_t`
- `BatchId(u64)` — `INVALID_BATCH` on allocate failure
- `NotifyMsg { name, msg }`
- `NicLoadStat { device_name, inflight_bytes, ewma_bandwidth_bps }`
- `MemoryPool` — page-aligned, zeroed host buffer for registration
- `WILDCARD_LOCATION` (`"*"`), `LOCAL_SEGMENT` (`0`)

### Engine lifecycle

- `initialize(local_hostname, metadata_server, protocol, device_name)`
- `new` / `create`
- `discover_topology`
- `install_transport` / `uninstall_transport`
- `local_ip_and_port`
- Drop destroys the native handle (no double-free)

### Memory

All pointer APIs are `unsafe`. Registered memory must stay valid until
unregistered.

- `register_local_memory` / `register_local_memory_ex` (remote-accessible flag)
- `unregister_local_memory`
- `register_memory` / `unregister_memory` — Python aliases using `WILDCARD_LOCATION`
- `register_local_memory_batch(&[BufferEntry])` — zero-copy
- `unregister_local_memory_batch(&[*mut c_void])`

### Segments

- `open_segment` / `open_segment_no_cache` / `open_segment_cached`
- `close_segment`
- `warmup_efa_segment`
- `remove_local_segment`
- `sync_segment_cache`

### Transfers (zero-copy hot path)

- `allocate_batch_id(batch_size)`
- `submit_transfer(batch_id, &[TransferRequest])`
- `submit_transfer_with_notify(batch_id, requests, &NotifyMsg)`
- `get_transfer_status(batch_id, task_id) -> TransferStatus`
- `wait_all(batch_id, count, timeout)`
- `submit_and_wait(&[TransferRequest], timeout)` — allocate + submit + wait + free
- `free_batch_id`

### Python-shaped transfers

These open (and cache) a segment by hostname:

- `transfer_sync` / `transfer_sync_write` / `transfer_sync_read`
- `batch_transfer_sync` / `batch_transfer_sync_write` / `batch_transfer_sync_read`
- `transfer_submit_write` — returns `BatchId`; caller must `free_batch_id`
- `transfer_check_status` — polls task 0; does **not** free the batch

### Notifications and diagnostics

- `take_notifies() -> Vec<NotifyMsg>`
- `send_notify(target_id, &NotifyMsg)`
- `nic_load_stats() -> Vec<NicLoadStat>`
- `enable_graceful_shutdown`
- `show_links(json: bool) -> String`

### Errors

`EngineError` (`thiserror`, `#[non_exhaustive]`):

- `NullHandle`
- `InvalidString` (interior NUL)
- `OperationFailed(i32)` — raw C status
- `InvalidArgument`
- `TransferFailed`
- `Timeout`

## Safety & thread-safety

- `TransferEngine` is `Send + Sync`; the C++ engine serializes internally.
- Pointer arguments must satisfy Rust aliasing and lifetime rules.
- Registered memory must remain valid until unregistered.
- `submit_transfer` does not copy request bytes; do not mutate a submitted
  `TransferRequest` until the C call returns (the C layer copies into its own
  vector before returning).
