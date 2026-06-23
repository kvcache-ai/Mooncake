# Mooncake Store Rust API

This page documents the Rust crate `mooncake_store` (located at `mooncake-store/rust`).
It is a **safe wrapper** around the Mooncake Store C API (`store_c.h`).

For deployment and service prerequisites, also see:

- Mooncake Store deployment guide: `deployment/mooncake-store-deployment-guide`
- Error code reference: `troubleshooting/error-code`

## Build & runtime prerequisites

The Rust crate links against the C++ Mooncake build outputs.

- **Build**:
  - Build Mooncake with Store + Rust enabled via CMake: `-DWITH_STORE=ON -DWITH_STORE_RUST=ON`
  - Or build with Cargo after exporting the CMake build directory / include paths (see `mooncake-store/rust/README.md`).
- **Runtime**:
  - Dynamic linker must find Mooncake shared libraries (typically via `LD_LIBRARY_PATH` pointing at the CMake build outputs).
  - The store client requires:
    - a **metadata server** (HTTP metadata or etcd, depending on your deployment)
    - `mooncake_master`

## Quick start (copy-paste)

```rust
use mooncake_store::MooncakeStore;

fn main() -> Result<(), mooncake_store::StoreError> {
    let store = MooncakeStore::new()?;
    store.setup(
        "127.0.0.1",
        "http://127.0.0.1:8080/metadata",
        512 << 20, // global_segment_size
        128 << 20, // local_buffer_size
        "tcp",
        "",
        "127.0.0.1:50051",
    )?;

    store.put("hello", b"world", None)?;
    let value = store.get("hello")?;
    assert_eq!(value, b"world");

    store.remove("hello", false)?;
    Ok(())
}
```

## API reference

### `MooncakeStore`

#### `new() -> Result<MooncakeStore, StoreError>`

Allocate a new store handle (uninitialised). You must call `setup()` before any data operations.

#### `setup(...) -> Result<(), StoreError>`

Initialise the store client and establish connections.

Parameters:

- `local_hostname`: IP/hostname for this node.
- `metadata_server`: metadata URI, for example:
  - HTTP: `"http://127.0.0.1:8080/metadata"`
  - etcd: `"etcd://127.0.0.1:2379"`
- `global_segment_size`: per-segment size in bytes.
- `local_buffer_size`: local staging buffer size in bytes.
- `protocol`: transport protocol string (for example `"tcp"` / `"rdma"`).
- `device_name`: device selector; empty string means auto-select (when supported by the backend).
- `master_server_addr`: `mooncake_master` address, e.g. `"127.0.0.1:50051"`.

Returns `Ok(())` on success, otherwise `StoreError::OperationFailed(code)`.

#### `health_check() -> Result<(), StoreError>`

Connectivity health check. Returns `Ok(())` when the backend is reachable.

#### `put(key, value, config) -> Result<(), StoreError>`

Store `value` under `key`. This is a **copying** API: `value` is copied into store-managed buffers.

- `config`: optional replication settings (`ReplicateConfig`).

#### `get(key) -> Result<Vec<u8>, StoreError>`

Retrieve the full value for `key` into a newly allocated `Vec<u8>`.

Notes:

- Internally calls `get_size()` to allocate an exact-sized buffer, then `get_into()` to fill it.
- A missing key or backend failure can surface as `OperationFailed(...)` because the C API does not provide a distinct NotFound code in all paths.

#### `unsafe get_into(key, buffer, size) -> Result<i64, StoreError>`

Retrieve the value for `key` into a caller-provided buffer.

- **Returns**: number of bytes written on success.
- **Safety**: `buffer` must point to at least `size` bytes of writable valid memory.

#### `is_exist(key) -> Result<bool, StoreError>`

Existence check.

- `Ok(true)` if exists, `Ok(false)` if missing.
- Any other return code becomes `StoreError::OperationFailed(code)`.

#### `get_size(key) -> Result<i64, StoreError>`

Get the stored value size in bytes.

Important limitation:

- The underlying C API uses a single negative return code for multiple error conditions, so Rust surfaces errors as `OperationFailed(raw_code)` without distinguishing NotFound.

#### `get_hostname() -> Result<String, StoreError>`

Returns the hostname (and potentially port) that the store client is registered under.

#### `remove(key, force) -> Result<(), StoreError>`

Remove a key.

- If `force = true`, the key is removed even if another client is reading it.

#### `remove_by_regex(pattern, force) -> Result<i64, StoreError>`

Remove keys matching a regex pattern. Returns number of removed keys.

#### `remove_all(force) -> Result<i64, StoreError>`

Remove **all** keys. Returns number of removed keys.

### Zero-copy APIs (advanced)

The Rust wrapper exposes zero-copy APIs that map directly to the underlying RDMA-capable C++ store.

#### `unsafe register_buffer(buffer, size) -> Result<(), StoreError>`

Register a memory region for zero-copy operations.

- **Safety**: `buffer` must remain valid and pinned until `unregister_buffer()` is called.
- This is required before calling `put_from()` or other registered-memory operations.

#### `unsafe unregister_buffer(buffer) -> Result<(), StoreError>`

Deregister a previously registered buffer.

#### `unsafe put_from(key, buffer, size, config) -> Result<(), StoreError>`

Store from a registered buffer.

- **Safety**: `buffer` must have been registered via `register_buffer()` and be at least `size` bytes.

### Batch APIs

Batch forms are useful when you want to amortize RPC overhead.

#### `unsafe batch_put_from(keys, buffers, sizes, config) -> Result<Vec<i32>, StoreError>`

Batch version of `put_from()`.

- **Returns**: per-key result codes (0 = success, non-zero = error code for that key).
- **Safety**: each `buffers[i]` must be registered and valid for `sizes[i]` bytes.

#### `unsafe batch_get_into(keys, buffers, sizes) -> Result<Vec<i64>, StoreError>`

Batch version of `get_into()`.

- **Returns**: per-key bytes written (≥ 0) or error code (< 0).
- **Safety**: each destination buffer must be writable and at least `sizes[i]` bytes.

#### `batch_is_exist(keys) -> Result<Vec<bool>, StoreError>`

Batch existence check. Errors are returned as `OperationFailed(code)`.

### `ReplicateConfig`

Replication settings for write operations (`put`, `put_from`, `batch_put_from`).

Fields:

- `replica_num`: number of replicas (0 means “use server default”).
- `with_soft_pin`: prefer retaining the object in memory (soft pin).
- `with_hard_pin`: never evict (hard pin).
- `preferred_segments`: whitelist of segment names that should host a replica.

Example:

```rust
use mooncake_store::{MooncakeStore, ReplicateConfig};

fn write_with_replication(store: &MooncakeStore) -> Result<(), mooncake_store::StoreError> {
    let cfg = ReplicateConfig {
        replica_num: 2,
        with_soft_pin: true,
        with_hard_pin: false,
        preferred_segments: vec!["seg-a".to_string(), "seg-b".to_string()],
    };

    store.put("k", b"v", Some(&cfg))?;
    Ok(())
}
```

### `StoreError`

Errors returned by the Rust wrapper.

- `NullHandle`: store handle allocation failed.
- `InvalidString`: input string contained an interior `\0` (cannot form C string).
- `OperationFailed(i32)`: underlying C layer returned a non-zero / negative code.
- `NotFound`: convenience for consumers that implement a NotFound check externally.
- `InvalidArgument(String)`: wrapper-level argument validation failure (e.g. mismatched array lengths).

## Safety & thread-safety

- `MooncakeStore` is `Send + Sync` (the underlying C object is internally synchronised).
- Methods that accept raw pointers are marked `unsafe`:
  - You must uphold Rust aliasing and lifetime rules for buffers passed to FFI.
  - For zero-copy operations, buffers must be registered and remain valid until unregistered.

