# Mooncake Conductor Rust API

## Overview

The `mooncake_conductor` crate provides safe Rust bindings for the Mooncake
Conductor RPC client. It supports prefix-cache queries, publisher registration,
and inspection of registered services and indexed contexts.

The crate is located at `mooncake-conductor/rust`. It wraps the C ABI in
`conductor/client/conductor_c.h` and statically links the C++
`conductor_cpp_core` library. KV events do not pass through this client: engines
and Mooncake Store masters publish them directly to Conductor over ZMQ.

For the service and cache-hit semantics, see the
[Conductor architecture](../../design/conductor/conductor-architecture-design.md).
For another client interface, see the
[Python API](../python/conductor.md) or
[HTTP/MessagePack API](../http/conductor-indexer.md).

## Build

### Prerequisites

- A Mooncake source checkout and its C++ build dependencies.
- CMake and a C++20 compiler.
- Cargo and a Rust toolchain.
- A C compiler and libclang, required by `bindgen`.
- A running `mooncake_conductor` service for client calls.

Build both Conductor and the Rust crate from the Mooncake repository root:

```bash
cmake -S . -B build \
  -DWITH_CONDUCTOR=ON \
  -DWITH_CONDUCTOR_RUST=ON
cmake --build build --target build_mooncake_conductor_rust --parallel
```

`WITH_CONDUCTOR_RUST=ON` requires `WITH_CONDUCTOR=ON`. The CMake target builds
`libconductor_cpp_core.a` first and then runs `cargo build --release` for the
crate.

### Use the crate from another Cargo project

The crate uses a Mooncake build tree at compile time. Add it as a path
dependency:

```toml
[dependencies]
mooncake_conductor = { path = "/path/to/Mooncake/mooncake-conductor/rust" }
```

Build the C++ core, then set the build-tree locations explicitly when invoking
Cargo directly:

```bash
cmake -S /path/to/Mooncake -B /path/to/Mooncake/build \
  -DWITH_CONDUCTOR=ON
cmake --build /path/to/Mooncake/build --target conductor_cpp_core --parallel

export MOONCAKE_BUILD_DIR=/path/to/Mooncake/build
export MOONCAKE_CONDUCTOR_LIB_DIR=/path/to/Mooncake/build/mooncake-conductor
export MOONCAKE_CONDUCTOR_INCLUDE_DIR=/path/to/Mooncake/mooncake-conductor/include
cargo build --release
```

`MOONCAKE_CONDUCTOR_LIB_DIR` defaults to `../../build/mooncake-conductor`
relative to the crate. `MOONCAKE_CONDUCTOR_INCLUDE_DIR` defaults to the crate's
`../include`. The build script also inspects `MOONCAKE_BUILD_DIR`,
`CMAKE_PREFIX_PATH`, `LIBRARY_PATH`, and `LD_LIBRARY_PATH` for dependent
libraries.

Although `conductor_cpp_core` is linked statically, shared dependencies such as
`libasio` must remain discoverable at runtime. Keep the Mooncake build outputs
in place, or add the relevant directories, including
`build/mooncake-common`, to the platform's dynamic-library search path.

## Quick start

Start Conductor with HTTP and RPC enabled:

```json
{
  "http_server_port": 13333,
  "rpc_server_port": 13334,
  "kvevent_instance": {}
}
```

```bash
CONDUCTOR_CONFIG_PATH=/path/to/conductor.json \
  /path/to/Mooncake/build/mooncake-conductor/mooncake_conductor
```

The following example registers one vLLM publisher, queries its prefix-cache
hit, inspects the service state, and unregisters it. The publisher must already
be running at the configured endpoints. Registration starts event consumption;
it does not create cache entries by itself.

```rust
use mooncake_conductor::{
    ClientConfig, ConductorClient, ConductorError, HashProfileConfig, PublisherKind, QueryRequest,
    ServiceConfig,
};

fn main() -> Result<(), ConductorError> {
    let client = ConductorClient::new()?;
    client.setup(&ClientConfig::new("127.0.0.1:13334"))?;

    let profile = HashProfileConfig {
        strategy: "vllm_v1".to_string(),
        algorithm: "sha256_cbor".to_string(),
        python_hash_seed: "0".to_string(),
        index_projection: "low64_be".to_string(),
    };
    let mut service =
        ServiceConfig::new("vllm-0", "tcp://127.0.0.1:5557", "demo-model", 16, profile);
    service.publisher_kind = PublisherKind::Vllm;
    service.replay_endpoint = "tcp://127.0.0.1:5558".to_string();

    let registration = client.register(&service)?;
    println!("new registration: {}", registration.is_new);

    let tokens: Vec<i32> = (1..=48).collect();
    let request = QueryRequest::new("demo-model", 16, &tokens).with_instance_filter("vllm-0");
    let result = client.query(&request)?;
    for (instance_id, hit) in &result.hits {
        println!(
            "{instance_id}: device={} cpu_local={} cpu_share={} disk={}",
            hit.device, hit.cpu_local, hit.cpu_share, hit.disk
        );
    }

    println!("services: {:?}", client.list_services()?);
    println!("global view: {:?}", client.get_global_view()?);

    client.unregister("vllm-0", "", 0)?;
    client.close();
    Ok(())
}
```

A newly registered instance normally reports zeroes until matching KV events
arrive. An empty `result.hits` map means that the selected context or instance
was not found; an entry whose counts are all zero is a registered instance with
no matching prefix.

## Client lifecycle

### `ConductorClient::new`

```rust
pub fn new() -> Result<ConductorClient, ConductorError>
```

Creates an unconnected client. Call `setup()` before other operations. Dropping
the client closes the connection and destroys the native handle.

### `setup`

```rust
pub fn setup(&self, config: &ClientConfig) -> Result<(), ConductorError>
```

Connects to Conductor's `rpc_server_port`. `ClientConfig::new(addr)` accepts a
`host:port` string without a URL scheme and sets these defaults:

| Field | Default |
|---|---|
| `connect_timeout` | 1 second |
| `request_timeout` | 3 seconds |

Both fields use `std::time::Duration`. Every setup attempt replaces the previous
connection; a failed attempt leaves the client unavailable. `setup()` may be
retried or called again after `close()`.

### `close`

```rust
pub fn close(&self)
```

Closes the connection. It is idempotent and does not unregister publishers from
Conductor.

### `health_check`

```rust
pub fn health_check(&self) -> HealthStatus
```

Performs a `list_services()` probe and returns:

| Value | Meaning |
|---|---|
| `HealthStatus::NotInitialized` | No setup has succeeded on this client. |
| `HealthStatus::Healthy` | The RPC probe succeeded. |
| `HealthStatus::Unreachable` | Setup succeeded previously, but the probe now fails, including after `close()`. |

This health check tests RPC reachability, not whether every event subscription
is current.

## Query API

### `query`

```rust
pub fn query(
    &self,
    request: &QueryRequest<'_>,
) -> Result<QueryResult, ConductorError>
```

Construct a request with `QueryRequest::new` and customize it with builder
methods:

```rust
let request = QueryRequest::new("demo-model", 16, &tokens)
    .with_tenant_id("default")
    .with_lora_name("")
    .with_cache_salt("")
    .with_instance_filter("vllm-0");
```

| `QueryRequest` field | Description |
|---|---|
| `model_name` | Model identity matching the registration. |
| `block_size` | Positive KV block size in tokens. |
| `token_ids` | Borrowed `&[i32]` prompt tokens. |
| `tenant_id` | Empty or `"default"` selects the default tenant. |
| `lora_name` | Empty selects the base model. |
| `cache_salt` | Empty means no salt; otherwise it must match the publisher's hash namespace. |
| `instance_filter` | Empty queries all instances in the context; otherwise restricts the result to one instance ID. |

`QueryResult.hits` is a `BTreeMap<String, CacheHit>`. Each `CacheHit` contains:

| Field | Type | Description |
|---|---|---|
| `longest_matched` | `i64` | Longest continuous matched prefix in tokens; equal to `disk`. |
| `device` | `i64` | Prefix tokens reachable from engine device memory. |
| `cpu_local` | `i64` | Prefix tokens reachable using device and engine-local CPU memory. |
| `cpu_share` | `i64` | Prefix tokens reachable using the preceding tiers and shared CPU memory. |
| `disk` | `i64` | Prefix tokens reachable using all tiers, including disk. |
| `dp` | `BTreeMap<i64, i64>` | Device-tier token count by DP rank. |
| `rank_matches` | `BTreeMap<i64, RankMatch>` | All four cumulative tier counts by DP rank. |

`RankMatch` has `device`, `cpu_local`, `cpu_share`, and `disk` fields. Tier
counts are cumulative, so `device <= cpu_local <= cpu_share <= disk`; do not add
them together. Instance-level counts are the maximum across ranks. Unlike the
Python and HTTP representations, Rust map keys remain integer DP ranks.

Block coverage follows the registered hash strategy: `vllm_v1` considers complete
blocks, while `sglang` and `sglang_bigram` also consider a final partial block.
For `sglang_bigram`, a query with `n` tokens can match at most
`max(n - 1, 0)` logical KV positions.

## Registration API

### `ServiceConfig`

Use `ServiceConfig::new` for the required identity fields:

```rust
let service = ServiceConfig::new(
    "vllm-0",
    "tcp://127.0.0.1:5557",
    "demo-model",
    16,
    hash_profile,
);
```

The constructor leaves the remaining fields at their defaults. Modify the
public fields before registration when needed:

| Field | Default | Description |
|---|---|---|
| `publisher_kind` | `PublisherKind::Vllm` | `Vllm`, `Sglang`, or `Mooncake`. |
| `replay_endpoint` | Empty | Optional replay endpoint for vLLM/SGLang gap recovery. |
| `tenant_id` | Empty | Empty normalizes to `"default"`. |
| `lora_name` | Empty | Base model. |
| `dp_rank` | `0` | Nonnegative data-parallel rank. |
| `cache_group` | `None` | Only `Some(0)` is currently supported. |

`HashProfileConfig` has four owned string fields: `strategy`, `algorithm`,
`python_hash_seed`, and `index_projection`. Supported combinations are:

| Strategy | Algorithm | Index projection | Python hash seed |
|---|---|---|---|
| `vllm_v1` | `sha256` or `sha256_cbor` | `low64_be` | The publisher's decimal seed in `0..=4294967295`, or `"random"`. |
| `sglang`, `sglang_bigram` | `sha256_raw` | `first64_be` | Unused; an empty string is accepted. |

The profile must reproduce the publisher's block hashes. All services in one
`(tenant_id, model_name, lora_name, block_size)` context must use the same
profile. Conductor derives `root_digest`; it is not part of registration input.

### `register`

```rust
pub fn register(
    &self,
    config: &ServiceConfig,
) -> Result<RegisterResult, ConductorError>
```

Starts a subscription for one `(instance_id, tenant_id, dp_rank)` service key.
`RegisterResult.is_new` is `true` for a new service and `false` when an
identical registration already existed. A conflicting registration for an
existing key or endpoint returns an error.

### `unregister`

```rust
pub fn unregister(
    &self,
    instance_id: &str,
    tenant_id: &str,
    dp_rank: i32,
) -> Result<(), ConductorError>
```

Stops the subscription and removes entries contributed by that source. An empty
`tenant_id` means `"default"`. Unknown services return
`ConductorError::ServiceNotFound`.

## Service and index inspection

### `list_services`

```rust
pub fn list_services(&self) -> Result<Vec<ServiceInfo>, ConductorError>
```

Returns active publisher configurations as owned Rust values. `ServiceInfo`
uses Rust `snake_case` fields and a resolved `HashProfile`, which includes the
server-derived `root_digest`. These values are snapshots; do not treat them as a
subscription health or freshness guarantee.

### `get_global_view`

```rust
pub fn get_global_view(&self) -> Result<GlobalView, ConductorError>
```

Returns indexed contexts in `GlobalView.contexts`. Each `ContextView` contains
`model_name`, `lora_name`, `block_size`, `tenant_id`, `prefix_count`, a resolved
`hash_profile`, and `instances`. The instance map has type
`BTreeMap<String, BTreeSet<i64>>`, mapping instance IDs to registered DP ranks.

## Errors

All RPC methods return `Result<T, ConductorError>` instead of integer status
codes.

| Variant | Numeric code | Meaning |
|---|---:|---|
| `Internal` | `-1` | Internal service or client failure. |
| `InvalidParams` | `-600` | Invalid address, request, or registration. |
| `RpcFail` | `-900` | RPC connection or transport failure. |
| `RpcTimeout` | `-901` | RPC request exceeded `request_timeout`. |
| `Unavailable` | `-2000` | The client is not connected or Conductor is unavailable. |
| `ServiceNotFound` | `-2001` | The unregister target is not registered. |
| `Unknown(code)` | Preserved | A negative status unknown to this crate version. |
| `NullHandle` | None | Native handle allocation failed. |
| `InvalidString(error)` | None | An input string contains an interior NUL byte. |

`ConductorError::code()` returns the corresponding numeric code when one
exists. The enum is `#[non_exhaustive]`, so downstream `match` expressions must
include a wildcard arm.

## Concurrency and latency

- Every client operation is blocking. Use `tokio::task::spawn_blocking` or a
  dedicated thread when integrating with an async runtime; do not block an
  async worker thread directly.
- `ConductorClient` is `Send + Sync`, but calls on one handle are serialized by
  the native client's mutex. Sharing one handle does not provide parallel RPC
  queries.
- For concurrent request-path queries, use a bounded pool of independent
  clients. Bound both pool admission and the RPC call with one outer deadline:
  `request_timeout` starts only after a call acquires the handle's internal
  mutex, so time spent waiting for a busy handle is not covered by that timeout.
- A timed-out native blocking call cannot be cancelled by the Rust wrapper.
  Return its handle to a pool only after the call actually finishes.
- Registration is idempotent for identical configurations and is suitable for
  periodic reconciliation. Query failures should normally degrade routing to a
  load-only policy rather than fail inference requests.
