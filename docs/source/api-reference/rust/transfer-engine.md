# Transfer Engine Rust API

This page documents the Rust bindings living under `mooncake-transfer-engine/rust`.

At the time of writing, the crate (`transfer_engine_rust`) is primarily used as a **Rust-side binding + example binary**. The public Rust types are implemented in `src/transfer_engine.rs` and wrap the Transfer Engine C API (`transfer_engine_c.h`).

For Transfer Engine design docs and non-Rust APIs, see:

- Transfer Engine design docs: `design/transfer-engine/index`
- Transfer Engine C++ API: `design/transfer-engine/cpp-api`

## Build & runtime prerequisites

The Rust crate uses bindgen + CMake to link against the Transfer Engine C/C++ build outputs.

- **Build**:
  - Requires Rust toolchain and libclang (bindgen).
  - The crate has `build.rs` that expects to find / build the native library via CMake.
- **Runtime**:
  - Dynamic linker must find Transfer Engine shared libraries.
  - You need a metadata server backend (commonly etcd) and a reachable peer segment registry.

## Mental model

The Transfer Engine operates on **segments** and **transfer batches**:

- You create a `TransferEngine` bound to:
  - `metadata_uri` (for example, etcd endpoint)
  - `local_server_name` (this node's address/name)
  - `rpc_port` (RPC listener port)
- You register local memory regions as RDMA-capable buffers.
- You open a remote segment to obtain a `segment_id` (an integer handle).
- You allocate a batch id for a fixed number of transfer requests.
- You submit a batch of `TransferRequest`.
- You poll status per task id inside the batch, then free the batch id.

## API reference

### Enums

#### `OpcodeEnum`

- `OpcodeEnum::Read`
- `OpcodeEnum::Write`

Used by `TransferRequest.opcode`.

#### `TransferStatusEnum`

Status values returned by the C layer. Common values you will check for:

- `Completed`
- `Failed`
- `Timeout`

### Structs

#### `TransferRequest`

One transfer operation inside a batch.

Fields:

- `opcode: OpcodeEnum`
- `source: *mut c_void`: local source/destination pointer (depends on opcode).
- `target_id: i32`: segment id returned by `open_segment()`.
- `target_offset: u64`: byte offset inside the target segment.
- `length: u64`: transfer length in bytes.

#### `BufferEntry`

Used for batch memory registration:

- `addr: *mut c_void`
- `length: u64`

### `TransferEngine`

#### `new(metadata_uri, local_server_name, rpc_port) -> anyhow::Result<TransferEngine>`

Create a new engine handle.

Notes:

- `metadata_uri` and `local_server_name` are passed through `CString`; interior `\0` bytes will error.
- The wrapper currently disables `auto_discover` in the underlying C call.

#### `discover_topology() -> anyhow::Result<()>`

Trigger topology discovery.

#### `install_transport(proto) -> anyhow::Result<()>`

Install a transport by name (e.g. `"tcp"`, `"rdma"`, `"efa"` depending on build/runtime support).

#### `register_local_memory(addr, length, location) -> anyhow::Result<()>`

Register a local memory region for zero-copy transfers.

- `addr`: pointer to the memory region.
- `length`: size in bytes.
- `location`: location string such as `"cpu:0"`.

#### `unregister_local_memory(addr) -> anyhow::Result<()>`

Unregister a previously registered memory region.

#### `register_local_memory_batch(buffer_list, location) -> anyhow::Result<()>`

Batch register multiple local buffers.

- No-op when `buffer_list` is empty.

#### `unregister_local_memory_batch(buffer_list) -> anyhow::Result<()>`

Batch unregister multiple local buffers.

#### `open_segment(name: String) -> anyhow::Result<i32>`

Open a remote segment by name and get a segment id.

#### `close_segment(segment_id: i32) -> anyhow::Result<()>`

Close a previously opened segment.

#### `warmup_efa_segment(name: &str) -> anyhow::Result<()>`

Eagerly establish EFA endpoints so the first `submit_transfer()` does not pay the serial connection setup cost.

- No-op on non-EFA transports.
- Call after `open_segment()` and after the metadata server has published the peer's NIC list.

#### `sync_segment_cache() -> anyhow::Result<()>`

Synchronize segment cache from metadata.

#### `allocate_batch_id(batch_size) -> anyhow::Result<u64>`

Allocate a batch id for `batch_size` transfer requests.

You must call `free_batch_id(batch_id)` after all tasks are done.

#### `submit_transfer(batch_id, requests) -> anyhow::Result<()>`

Submit a batch transfer request list.

- No-op when `requests` is empty.
- The wrapper converts each `TransferRequest` into the C representation (`transfer_request_t`).

#### `get_transfer_status(batch_id, task_id) -> anyhow::Result<(i32, u64)>`

Get status for one task in a batch.

- `task_id` is an index inside the batch, typically `0..batch_size`.
- Returns `(status_code, transferred_bytes)`.

The `status_code` maps to values in `TransferStatusEnum` (represented as `i32`).

#### `free_batch_id(batch_id) -> anyhow::Result<()>`

Free a previously allocated batch id.

## Minimal usage example (pseudo-code)

The crate's `src/main.rs` contains a full benchmark-style example. The following sketch shows the typical control flow:

```rust
use std::ffi::c_void;
use transfer_engine_rust::transfer_engine::{OpcodeEnum, TransferEngine, TransferRequest};

fn main() -> anyhow::Result<()> {
    let engine = TransferEngine::new("127.0.0.1:2379", "127.0.0.1", 12345)?;
    engine.discover_topology()?;
    engine.install_transport("tcp")?;

    // Register local memory (example only; you must allocate and pin memory appropriately).
    let mut buffer = vec![0u8; 4096];
    engine.register_local_memory(buffer.as_mut_ptr() as *mut c_void, buffer.len(), "cpu:0")?;

    let seg_id = engine.open_segment("target-seg".to_string())?;
    let batch_id = engine.allocate_batch_id(1)?;

    let mut reqs = [TransferRequest {
        opcode: OpcodeEnum::Write,
        source: buffer.as_mut_ptr() as *mut c_void,
        target_id: seg_id,
        target_offset: 0,
        length: buffer.len() as u64,
    }];

    engine.submit_transfer(batch_id, &mut reqs)?;
    let (status, bytes) = engine.get_transfer_status(batch_id, 0)?;
    println!("status={status}, bytes={bytes}");

    engine.free_batch_id(batch_id)?;
    engine.close_segment(seg_id)?;
    engine.unregister_local_memory(buffer.as_mut_ptr() as *mut c_void)?;
    Ok(())
}
```

## Safety & thread-safety

- The wrapper marks `TransferEngine` as `Send + Sync`, but it owns an FFI handle (`transfer_engine_t`).
- All pointer-based arguments must satisfy Rust’s aliasing and lifetime rules.
- You must ensure registered memory remains valid until it is unregistered.

