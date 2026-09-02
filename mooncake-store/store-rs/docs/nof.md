# NoF integration and operation

NoF is a remote persistence data plane attached below Cold Tier. It reuses Mooncake's existing hot
replica lifecycle, offload queue, restore path, target ownership, heartbeat, and replica selection.

The included provider is the KVCS 0.4.0 C API executor. `KvcsCapiExecutor` chooses Standard or
Low-Level mode when it starts and exposes only the traits supported by that mode. Both KVCS modes
are provider-owned: KVCS can find an object from its derived key, so Mooncake does not persist a
NoF placement route for KVCS.

## Architecture

`NofBackend` wraps an `Arc<dyn NofBacking>`. A backing composes independent, provider-neutral
capabilities:

| Capability | Trait | Responsibility |
| --- | --- | --- |
| logical-object I/O | `NofObjectWrite`, `NofObjectRead`, `NofObjectQuery`, `NofObjectDelete` | provider namespace, object, and manifest semantics |
| provider-addressed physical I/O | `NofPhysicalWrite`, `NofPhysicalRead`, `NofPhysicalQuery`, `NofPhysicalDelete` | complete-object I/O by deterministic key; the provider owns lookup metadata and privately chooses its record layout |
| health | `NofHealth` | liveness and optional capacity information |

Missing capabilities remain missing. Mooncake does not recreate them with a provider CLI, a
private service API, or a parallel disk-management implementation.

For KVCS, no provider placement is stored in `ObjectRoute`. Mooncake does not persist a KVCS
target, owner, locator, manifest, shard map, object length, or checksum. The route keeps only the
logical object identity. Each request derives the provider key from the stable scoped logical key
and asks the configured KVCS targets directly. `ObjectRoute.version` is control-plane state and is
not part of that key. Target and owner information is a request-local I/O descriptor and is never
published through route CAS or exposed by the admin API.

This route-free rule follows metadata authority, not the Standard/Low-Level or logical/physical
API shape. An allocator-backed executor that cannot locate an object from its key must return an
opaque location and use a Mooncake-managed NoF route. That route is authoritative for target and
replica placement, while record, extent, chunk, and alignment details remain private to the
executor. Such a managed-route executor must not reuse KVCS request-local discovery as its source
of truth.

Mooncake remains authoritative for logical object existence, while KVCS remains authoritative for
provider layout and provider-internal metadata:

- Standard stores its namespace and shard manifest in KVCS/Redis. Reads query KVCS and then fetch
  the shards reported by the provider.
- Low-Level stores raw values in KVCS. A value that exceeds the configured record limit uses an
  executor-private sidecar and derived record keys in the same KVCS key space.

### Standard and Low-Level

| Mode | Data plane | Replication and placement | Physical maintenance |
| --- | --- | --- | --- |
| `standard` | KVCS logical objects and shards | KVCS owns placement, replicas, and manifest completeness; Mooncake selects one provider target | KVCS owns disks, GC, watermarks, recovery, and rebuild |
| `low-level` | raw physical keys | Mooncake selects configured NoF targets with the shared Cold Tier replica policy; the same derived key is stored on every selected target | KVCS/EFC owns disks, capacity policy, GC, compaction, recovery, and rebuild |

A `StoreClient` cannot mix logical-object and physical-KV NoF targets. Use separate clients when
both data planes are required.

## Request lifecycle

On KVCS offload, Mooncake derives the provider key, selects healthy targets, and writes the
payload. A Standard write goes to one provider target because KVCS owns its internal replication.
A Low-Level write succeeds only after every target selected by `nof_replica_count` accepts the
value. No target list is written back to metadata.

When no hot or local-disk copy is available, the client derives the same key and probes its current
NoF configuration. The resulting length, checksum, target, and owner exist only for that restore
request. Removing an object fans out an idempotent, best-effort delete for its logical key to the
current target set. Mooncake does not persist a NoF delete intent or retry it after the logical
route has been removed. If the client exits between route deletion and provider deletion, the
provider's own GC must reclaim the orphan.

When an in-memory offload queue is rebuilt, Mooncake probes the provider for each active hot route.
It re-enqueues only missing objects or Low-Level objects with fewer than the required target copies.
This is route-driven recovery, not a provider key-space or disk scan.

KVCS targets must therefore be configured consistently on clients that share a route namespace.
Changing a target set does not create a metadata migration. Because KVCS 0.4.0 has no listing API,
Mooncake cannot enumerate or reclaim provider records that are unreachable from a current logical
route and target configuration.

## Object layout and batching

Cold Tier's existing `ValueChunkPlan` is the only object-splitting planner:

- Standard maps the plan to KVCS `shard_id` and `total_shards`; KVCS owns the manifest.
- Low-Level stores values at or below the configured limit directly at the root key.
- Larger Low-Level values use derived chunk keys and an executor-private 32-byte sidecar containing
  total length, chunk size, and chunk count.

The Low-Level inline put/get path uses one root key. It does not encode a sidecar, generate chunk
keys, or iterate a chunk plan. Inline delete first queries the root key and then deletes it.
Physical layout remains an executor concern, so another executor may use aligned extents or
another record format without changing the NoF framework.

Object splitting and SDK batch splitting are separate. Object splitting decides how many records
represent one value. KVCS already bounds C API batches by `max_keys_per_batch`, so Mooncake passes
positional batches without another fixed-size batch loop.

The configured single-value limit is also passed to the SDK client:

```shell
export MOONCAKE_KVCS_MAX_VALUE_SIZE=3221225472 # 3 GiB
```

KVCS 0.4.0 has no runtime getter for this limit. Unset uses the documented 4 MiB SDK default; valid
configured values are 1 byte through 4 GiB. The value must remain stable for a target because it is
part of the Low-Level private layout.

## Ownership and health

Low-Level `nof_replica_count` is clamped to `1..=8` and defaults to 1. The shared
`ReplicaLoadBalanceStrategy` selects write targets using provider score, accumulated writes, and
target ID.

NoF ownership reuses client leases and Rendezvous hashing. Clients with the same target-set
fingerprint derive the same `target_id -> owner` assignment:

- each client probes only the targets it owns;
- a background heartbeat runs once per second; request paths use its cached result;
- three consecutive failures exclude a target and one success restores it;
- owners publish unhealthy target IDs in the existing client lease;
- graceful shutdown releases ownership immediately, while crash takeover follows lease expiry and
  epoch fencing.

KVCS Low-Level health uses the public existence query and reports no capacity because the 0.4.0 ABI
has no capacity call. Standard exposes no KVCS health capability, so the framework treats a
configured Standard target as available and does not probe it. Health is never checked per object
request.

## KVCS maintenance boundary

The public KVCS 0.4.0 Low-Level API exposes put, get/get-into, delete, and existence query. It does
not expose key listing, capacity, disk lifecycle, GC, watermarks, compaction, recovery, rebuild, or
a separate durability barrier. KVCS/EFC owns those responsibilities. Mooncake does not scan KVCS,
reconcile provider disks, rebuild provider metadata, or call private maintenance APIs. Queue
recovery may query keys derived from currently active Mooncake routes; it cannot discover objects
that have no logical route.

## Source layout

```text
client/cold_tier/
  layout/
    physical_key.rs
    value_chunk.rs
  owner.rs
  replica_policy.rs
  nof/
    backing.rs
    backend.rs
    object.rs
    physical.rs
    physical_backend.rs
    runtime.rs
    kvcs/
      capi.rs
      executor.rs
      physical_layout.rs
```

The traits and Cold Tier adapters in this source tree implement provider-owned access. KVCS
configuration, C API calls, and its private Low-Level layout live under `nof/kvcs/`.
`physical_backend.rs` passes complete objects to a key-addressed provider; it defines no chunk,
extent, alignment, record format, or persisted placement route. A locator-returning,
Mooncake-managed executor uses a separate adapter and route lifecycle.

## Install KVCS SDK and EFC

The supported SDK version is **0.4.0**. Official entry points:

- [KVCacheStore quick start](https://www.alibabacloud.com/help/en/kvcachestore/quick-start)
- [KVCS installation script](https://kvcachestore.oss-accelerate.aliyuncs.com/scripts/install-kvcs.sh)

The complete-node installer installs EFC and the SDK under `/opt/kvcs-sdk/latest`:

```shell
curl -fL \
  https://kvcachestore.oss-accelerate.aliyuncs.com/scripts/install-kvcs.sh \
  -o install-kvcs.sh
less install-kvcs.sh
sudo bash install-kvcs.sh
test -S /var/run/kvcs/efc-grpc.sock
```

The application and EFC must run as the same operating-system user.

SDK-only archives:

| Architecture | Download | SHA-256 |
| --- | --- | --- |
| x86_64 | [kvcs-sdk-0.4.0-x86_64.tar.gz](https://kvcachestore.oss-accelerate.aliyuncs.com/sdk/kvcs-sdk-0.4.0-x86_64.tar.gz) | `61b1cee7c0e87975d8e3d723c1e3335cabc46a6af2efeced233918f688f2b3c9` |
| aarch64 | [kvcs-sdk-0.4.0-aarch64.tar.gz](https://kvcachestore.oss-accelerate.aliyuncs.com/sdk/kvcs-sdk-0.4.0-aarch64.tar.gz) | `22159a8fa911799857db6c8d084220efc4d6ac9adc79302a7e35371a71d1fd6c` |

```shell
KVCS_VERSION=0.4.0
KVCS_ARCH="$(uname -m)" # x86_64 or aarch64
curl -fL \
  "https://kvcachestore.oss-accelerate.aliyuncs.com/sdk/kvcs-sdk-${KVCS_VERSION}-${KVCS_ARCH}.tar.gz" \
  -o "kvcs-sdk-${KVCS_VERSION}-${KVCS_ARCH}.tar.gz"
tar xzf "kvcs-sdk-${KVCS_VERSION}-${KVCS_ARCH}.tar.gz"
sudo mkdir -p /opt/kvcs-sdk
sudo mv "kvcs-sdk-${KVCS_VERSION}" "/opt/kvcs-sdk/${KVCS_VERSION}"
sudo ln -sfnT "/opt/kvcs-sdk/${KVCS_VERSION}" /opt/kvcs-sdk/latest
```

EFC packages for manual provisioning:

| Format | x86_64 / amd64 | aarch64 / arm64 |
| --- | --- | --- |
| RPM | [x86_64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc-0.4.0-1.x86_64.rpm) | [aarch64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc-0.4.0-1.aarch64.rpm) |
| DEB | [amd64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc_0.4.0_amd64.deb) | [arm64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc_0.4.0_arm64.deb) |

The installer also supports a deploy-tarball package type, but a direct public 0.4.0 deploy-tarball
URL is not currently published. Use the installer or the RPM/DEB links above instead of guessing a
tarball path.

## Build dependencies

The default feature set has no KVCS dependency. `kvcs-capi` links the public shared C API and uses
the installed SDK's generated `rust/src/ffi.rs`; Mooncake does not build the vendor C++ sources,
run bindgen, or add the vendor Rust wrapper to the Cargo graph. SDK 0.4.0 omits
`kvcs_ll_client_destroy` from that generated Rust file, so Mooncake declares that one function
from the installed public C header until the SDK binding includes it.

The `kvcs-capi` feature supports Linux GNU targets on x86_64 and aarch64 only.

| Build | Linked library | Runtime directory |
| --- | --- | --- |
| production | `libkvcs.so.0` | `$KVCS_SDK_ROOT/lib` |
| official mock | `libkvcsmock.so.0` | `$KVCS_SDK_ROOT/mock/lib` |

`KVCS_SDK_ROOT` defaults to `/opt/kvcs-sdk/latest`. The build checks the package version and target
architecture, requires the C header and generated Rust file, checks their expected package shape,
and requires the selected shared library. It does not perform an independent ABI conformance
check. No RPATH is embedded, so the loader must resolve the complete shared-library SONAME chain.

Production build:

```shell
export KVCS_SDK_ROOT=/opt/kvcs-sdk/latest
export KVCS_SDK_USE_MOCK=0
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo build -p mooncake-store-client --features kvcs-capi --offline
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

Mock build:

```shell
export KVCS_SDK_ROOT=/opt/kvcs-sdk/latest
export KVCS_SDK_USE_MOCK=1
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo build -p mooncake-store-client --features kvcs-capi --offline
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/mock/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

`KVCS_SDK_USE_MOCK` is a build-time switch; changing only `LD_LIBRARY_PATH` cannot turn a
production-linked binary into a mock build.

## Runtime configuration

| Variable | Mode | Meaning |
| --- | --- | --- |
| `MC_STORE_RS_ENABLE_COLD_TIER` | both | set to `1` to enable the shared Cold Tier lifecycle |
| `MOONCAKE_KVCS_MODE` | both | `standard` or `low-level`; unset defaults to `low-level` |
| `MOONCAKE_KVCS_EFC_SOCKET` | both | EFC Unix socket; Low-Level defaults to `/var/run/kvcs/efc-grpc.sock` |
| `MOONCAKE_KVCS_REDIS_ENDPOINTS` | Standard | comma-separated Redis endpoints, for example `tcp://host:6379` |
| `MOONCAKE_KVCS_REDIS_PASSWORD` | Standard | optional Redis password |
| `MOONCAKE_KVCS_MOUNTPOINT_INDEX` | Low-Level | KVCS mountpoint index; defaults to 0 |
| `MOONCAKE_KVCS_MAX_VALUE_SIZE` | both | SDK single-value limit; defaults to 4 MiB, maximum 4 GiB |

```shell
# Standard
export MC_STORE_RS_ENABLE_COLD_TIER=1
export MOONCAKE_KVCS_MODE=standard
export MOONCAKE_KVCS_EFC_SOCKET=/var/run/kvcs/efc-grpc.sock
export MOONCAKE_KVCS_REDIS_ENDPOINTS=tcp://redis.example:6379

# Low-Level
export MOONCAKE_KVCS_MODE=low-level
export MOONCAKE_KVCS_MOUNTPOINT_INDEX=0
export MOONCAKE_KVCS_MAX_VALUE_SIZE=3221225472
```

Register the configured KVCS target on the existing client builder:

```rust,ignore
use std::sync::Arc;
use mooncake_store_client::{
    KvcsCapiExecutor, NofBackend, NofTargetConfig, StoreClientBuilder,
};

let executor = Arc::new(KvcsCapiExecutor::new()?); // reads MOONCAKE_KVCS_MODE
let target = NofTargetConfig::new("kvcs-mount-0", NofBackend::new(executor)?)?;
let client = StoreClientBuilder::new(metadata, "storage-0")
    .nof_target(target)
    .nof_replica_count(1)
    .build(expires_at_ms)?;
```

The generic NoF framework accepts multiple targets, but the current KVCS executor reads its EFC
socket and mountpoint from process-wide environment variables. Therefore one process must not
register the same KVCS executor or mountpoint under multiple target IDs. Multi-target KVCS
deployment requires target-specific executor construction, which is not exposed by this version.
Every client in one ownership group must configure the same target ID and data plane. A target ID,
its provider endpoint, mountpoint, mode, and maximum value size are immutable configuration.
The ownership fingerprint identifies target IDs and data-plane shape; it cannot verify that two
processes mapped a target ID to the same provider endpoint or mountpoint. Deployment configuration
must enforce that mapping.

Standard sharded writes return an SDK error when any shard fails, after which Mooncake may retry the
object write. Mooncake does not maintain a second shard manifest or independently clean up a
partially accepted write. Correct retry and incomplete-manifest behavior therefore depends on the
KVCS Standard API's idempotency and manifest contract.

## Validation

Default build and tests do not require KVCS:

```shell
cargo fmt --all -- --check
MOONCAKE_SKIP_NATIVE_BUILD=1 cargo test -p mooncake-store-core --offline
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --lib --offline -- --test-threads=1
```

Official mock validation:

```shell
export KVCS_SDK_ROOT=/opt/kvcs-sdk/latest
export KVCS_SDK_USE_MOCK=1
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/mock/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  nof:: --offline -- --test-threads=1

MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  client::cold_tier::nof::kvcs::executor::standard::tests::live_standard_round_trip_smoke \
  --offline -- --exact --ignored --test-threads=1

MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  client::cold_tier::nof::kvcs::executor::low_level::tests::live_low_level_round_trip_smoke \
  --offline -- --exact --ignored --test-threads=1

MOONCAKE_KVCS_MAX_VALUE_SIZE=8 MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  client::cold_tier::nof::kvcs::executor::low_level::tests::live_low_level_round_trip_smoke \
  --offline -- --exact --ignored --test-threads=1

MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo clippy -p mooncake-store-client --all-targets --features kvcs-capi \
  --offline -- -D warnings
```

Live Standard validation requires EFC plus Redis; live Low-Level validation requires EFC plus a
configured mountpoint:

```shell
export KVCS_SDK_ROOT=/opt/kvcs-sdk/latest
export KVCS_SDK_USE_MOCK=0
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export MOONCAKE_KVCS_EFC_SOCKET=/var/run/kvcs/efc-grpc.sock
export MOONCAKE_KVCS_REDIS_ENDPOINTS=tcp://127.0.0.1:6379

MOONCAKE_KVCS_MODE=standard MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  client::cold_tier::nof::kvcs::executor::standard::tests::live_standard_round_trip_smoke \
  --offline -- --exact --ignored --nocapture --test-threads=1

MOONCAKE_KVCS_MODE=low-level MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  client::cold_tier::nof::kvcs::executor::low_level::tests::live_low_level_round_trip_smoke \
  --offline -- --exact --ignored --nocapture --test-threads=1
```

The official mock validates ABI marshalling, error handling, and executor behavior. It does not
exercise real I/O, shared memory, Redis, timeouts, failover, disk faults, capacity, watermarks, or
performance. It also omits the full shard details needed for a Standard sharded-object read, so
that path requires live EFC and Redis.
