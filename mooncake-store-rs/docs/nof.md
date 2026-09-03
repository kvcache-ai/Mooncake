# NoF integration and operation

NoF is a remote persistence data plane attached below Cold Tier. It reuses Mooncake's existing hot
replica lifecycle, offload path, restore admission and batching, heartbeat ownership, cached health
state, and replica selection.

The included provider is the KVCS 0.4.0 C API executor. `KvcsCapiExecutor::new()` reads process
environment for a single target. For multiple Low-Level targets, one `KvcsLowLevelClient` owns the
SDK client for an EFC socket and creates lightweight executors bound to different mountpoint
indices. The selected mode exposes only the traits supported by that mode. Both KVCS modes are
provider-owned: KVCS can find an object from its derived key, so Mooncake does not persist KVCS
placement metadata.

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
target, owner, locator, manifest, shard map, object length, or checksum. Each request derives the
provider key from the stable scoped logical key and asks the configured KVCS targets directly.
`ObjectRoute.version` is control-plane state and is not part of that key. Target selection is a
request-local I/O descriptor and is never published through route CAS or exposed by the admin API.

Mooncake remains authoritative for logical object existence, while KVCS remains authoritative for
provider layout and provider-internal metadata:

- Standard values at or below the configured limit use one non-sharded root key
  (`shard_id=0`, `total_shard=1`). Larger values use KVCS shards, whose manifest is owned by
  KVCS/Redis.
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

Mooncake resolves the logical route first. Only an `Active` route without a readable hot or
local-disk copy causes the client to derive the provider key and query provider metadata for
existence and length. This keeps KVCS queries off the hot-hit path. The existing Cold Tier target
selector, singleflight, admission control, and batch restore path then call the provider batch-get
API and promote the payload to a hot copy. Route lookup and provider query are deliberately not
issued speculatively in parallel.

Removal first changes the logical route from `Active` to `Deleting`. It then fans out the stable
provider key to the current target set. A provider error is returned to the caller and the
`Deleting` route remains as the retry identity; put and upsert cannot replace it. After provider
deletion succeeds, Mooncake removes the logical route and reclaims its hot or local-disk copies.
Retry is caller-driven through another remove call; there is no NoF delete worker or provider scan.
This state records logical deletion progress, not KVCS target placement or provider metadata.

Startup rebuilds persisted local Cold Tier work only. KVCS objects are discovered lazily by
request-time metadata query; Mooncake neither downloads provider values nor scans active routes to
reconstruct a NoF queue.

KVCS targets must therefore be configured consistently on clients that share a route namespace.
Changing a target set does not create a metadata migration. Because KVCS 0.4.0 has no listing API,
Mooncake cannot enumerate or reclaim provider records that are unreachable from a current logical
route and target configuration.

## Object layout and batching

Cold Tier's existing `ValueChunkPlan` is the only object-splitting planner:

- Standard stores values at or below the configured limit directly under the root key with
  `shard_id=0` and `total_shard=1`. This path creates no Mooncake chunk key or manifest. Larger
  values reuse `ValueChunkPlan` and map its ranges to KVCS shards; KVCS owns their manifest.
- Low-Level stores values at or below the configured limit directly at the root key.
- Larger Low-Level values use derived chunk keys and an executor-private 32-byte sidecar containing
  total length, chunk size, and chunk count.

Low-Level writes send data records first and publish the sidecar last. The inline path remains one
direct root-record write and does not execute sidecar or chunk logic.

After provider discovery has supplied the object length, a Standard read at or below the limit
issues `kvcs_batch_get_into` directly for root shard 0. It does not make a second query for shard
details. Larger Standard reads query the KVCS-owned shard manifest before issuing the batch get.

The Low-Level inline put/get path uses one root key. It does not encode a sidecar, generate chunk
keys, or iterate a chunk plan. Inline delete calls the SDK delete API directly. Physical layout
remains an executor concern and is not defined by the NoF framework.

Object splitting and request batching are separate. Object splitting decides how many records
represent one value. Cold Tier groups objects by target, and the executor maps those positional
objects or records directly to the corresponding KVCS batch API.

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

NoF heartbeat ownership reuses client leases and Rendezvous hashing. Clients with the same
target-set fingerprint derive the same `target_id -> heartbeat owner` assignment:

- each client probes only the targets it owns;
- a background heartbeat runs once per second; request paths use its cached result;
- three consecutive failures exclude a target and one success restores it;
- owners publish unhealthy target IDs in the existing client lease;
- graceful shutdown releases ownership immediately, while crash takeover follows lease expiry and
  epoch fencing.

Ownership applies only to heartbeat work. Every client keeps its own SDK client for each configured
remote target and may read, write, or delete that target directly; the heartbeat owner never
proxies, authorizes, or gates data I/O.

KVCS Low-Level health uses the public existence query and reports no capacity because the 0.4.0 ABI
has no capacity call. Standard exposes no KVCS health capability, so the framework treats a
configured Standard target as available and does not probe it. Health is never checked per object
request.

## KVCS maintenance boundary

The public KVCS 0.4.0 Low-Level API exposes put, get/get-into, delete, and existence query. It does
not expose key listing, capacity, disk lifecycle, GC, watermarks, compaction, recovery, rebuild, or
a separate durability barrier. KVCS/EFC owns those responsibilities. Mooncake does not scan KVCS,
reconcile provider disks, rebuild provider metadata, or call private maintenance APIs.

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
extent, alignment, record format, or persisted placement route.

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

### Static target registration

KVCS 0.4.0 does not enumerate Low-Level mountpoints or expose a mountpoint change stream. The
deployment inventory is therefore the target-list authority. The same inventory must generate the
server-side EFC mountpoints and the Mooncake application configuration. It supplies a stable
`target_id` and the corresponding local `mountpoint_index`; an index is not a stable target ID.

The application bootstrap that constructs each `StoreClient` reads that static inventory and calls
`nof_targets` once. Every client sharing a route namespace registers the same target IDs and data
plane so that every client can query and read every target directly. Heartbeat ownership only
distributes health probes; it does not register targets, proxy I/O, or authorize requests.

Create one shared SDK client per EFC socket and bind its configured mountpoints:

```rust,ignore
use std::sync::Arc;
use mooncake_store_client::{
    KvcsLowLevelClient, NofBackend, NofTargetConfig, StoreClientBuilder,
};

// Supplied by the deployment configuration that also generates EFC mountpoints.
let configured_targets = [("nof-disk-a", 0), ("nof-disk-b", 1)];
let kvcs = KvcsLowLevelClient::new(
    "/var/run/kvcs/efc-grpc.sock".to_string(),
    3 * 1024 * 1024 * 1024,
)?;
let mut targets = Vec::with_capacity(configured_targets.len());
for (target_id, mountpoint_index) in configured_targets {
    let executor = Arc::new(kvcs.executor(mountpoint_index));
    targets.push(NofTargetConfig::new(
        target_id,
        NofBackend::new(executor)?,
    )?);
}
let client = StoreClientBuilder::new(metadata, "storage-0")
    .nof_targets(targets)
    .nof_replica_count(2)
    .build(expires_at_ms)?;
```

`KvcsCapiExecutor::new()` is the environment-variable convenience constructor for a single target.
`with_low_level_config` remains a single-target convenience API. Standard mode uses
`with_standard_config`; the provider owns its internal placement, so it is registered as one
logical target.

The target list is immutable for the lifetime of a `StoreClient`. Temporary target availability is
dynamic: the existing owner heartbeat excludes a target after three consecutive failures and
restores it after one successful probe, without changing the registered list. Permanent additions
or removals require a coordinated configuration rollout and client restart. A new target must not
accept writes until every client has registered it, and a removed target must be drained by the
provider before it disappears from the inventory. Mooncake does not parse private EFC
configuration, invent target IDs, or implement an SDK-independent discovery protocol.

A target ID, its provider endpoint, mountpoint, mode, and maximum value size are immutable
configuration. The ownership fingerprint identifies target IDs and data-plane shape; it cannot
verify that two processes mapped a target ID to the same provider endpoint or mountpoint.
Deployment configuration must enforce that mapping.

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
