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
| Mooncake-managed I/O | `NofManagedWrite`, `NofManagedRead`, `NofManagedAllocator` | complete-object I/O that returns an opaque locator; Mooncake persists placement in `ObjectRoute.nof_backing` and the target owner drives reserve/release/recovery through route CAS |
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
API shape. The included ExtentStore executor is the opposite case: its allocator returns an opaque
location, so Mooncake persists a distinct `NofBackingRoute`. That route is authoritative for target
and replica placement and is replayed at startup to rebuild the allocator. Record framing, aligned
extent layout, and free-range management remain private to ExtentStore. KVCS never enters this
managed-route path.

### Target registration and owner assignment

NoF target registration reuses the existing Cold Tier target catalog and runtime lifecycle, but
its owner is assigned differently from a local disk:

| Stage | Local disk | NoF target |
| --- | --- | --- |
| target registration | `ColdTierTargetConfig` is resolved and registered on the node that owns the disk | `NofTargetConfig` binds the remote executor and stable target identity |
| initial owner | the registering node's current runtime is the owner | an eligible active runtime is selected after registration |
| shared management | existing Cold Tier owner, health, allocator, placement, and cleanup paths | the same paths after the owner assignment |

For a local disk, `bootstrap_cold_tier_device` records the current runtime as the device owner
because that runtime is the only one with a local backend. A NoF target is visible to every client
that has the same static target inventory, so registration first makes the target available to the
shared catalog and then assigns one active runtime as its owner. Joining a client does not move an
existing target.

The owner assignment is target-scoped. It uses the existing active client leases and stable
rendezvous balancing to spread targets across clients. The target-set fingerprint is carried in
the lease labels, so every client derives the same assignment from the shared lease view. No
owner record is written to route metadata, and Redis is not a second owner database or a data-path
dependency.

Every client keeps a local executor handle for every statically configured NoF target. The owner
does not proxy reads or payload writes. For provider-owned KVCS targets, the owner is used only
for the shared heartbeat and target-health view; KVCS remains authoritative for provider
placement, manifests, and disk maintenance. For Mooncake-managed ExtentStore targets, the owner
also performs target allocation, route publication, release, recovery, and maintenance.

### Device lifecycle and maintenance ownership

NoF does not introduce a second disk register/unregister control plane. Local disks continue to
use the existing Cold Tier device API and `ColdTierDeviceRecord` state/CAS transitions
(`register`, `unregister`, `disable`, `enable`, and drain). `NofTargetConfig` and
`SpdkNofBlockDevice::connect` bind the remote data plane and then enter the same target/runtime
management framework; they do not duplicate local disk lifecycle logic.

Managed ExtentStore targets also reuse the existing Cold Tier scheduler. The same high/low
watermarks drive bounded victim selection, route-safe `PendingDelete` transitions, and allocator
release through the normal owner path. The shared `PersistentStorageBackend::compact` interface is
used by local ExtentStore and Managed NoF: local ExtentStore removes fully dead non-active
segments, while Managed NoF releases locators through the same route-safe owner path. Neither
implementation moves a live route without a route CAS and locator update.

Managed NoF maintenance is owner-scoped. Each background cleanup pass uses the current target owner
assignment and only maintains targets owned by the local runtime. Logical delete and reclaim release
locators through the existing managed owner/CAS path; there is no provider list scan. Watermark
cleanup uses the same Cold Tier high/low thresholds: if a managed target reports capacity and
available bytes and its used space reaches the high watermark, the owner selects bounded LRU victims
and releases target copies until the target falls below the low watermark or no safe victim remains.
A target copy is removable only when the object still has a hot replica or another materialized NoF
target copy.

Health is heartbeat based, not checked on every request. After short consecutive heartbeat failure,
the owner publishes the target in `nof.unhealthy-targets`, so writers stop selecting it and readers
fail over to other route copies. After a longer consecutive failure window, the owner performs
route-only downline for safe objects: the bad target is removed from `ObjectRoute.nof_backing`
without calling the unavailable executor. This makes the target disappear from read/write routing
quickly while preserving data safety. When the target later comes back, metadata-backed routes
restore allocator state directly; if metadata no longer has a healthy route but the managed
ExtentStore record still has a recovery manifest, the same startup manifest-recovery path can
recreate `ObjectRoute.nof_backing` for that object.

Released NoF extents do not have a second persistent quarantine journal. Graceful owner handoff
fences and flushes accepted writes before recovery, and recovery rebuilds the allocator only from
materialized routes while stale locators are no longer published. Normal shutdown relies on
drain to wait for writes accepted by the outgoing process before the owner releases locators. If
crash-time late I/O becomes a supported requirement, it needs a device/session epoch fence in the
SPDK transport rather than another allocator journal.

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

Startup recovery is capability-specific. KVCS objects are discovered lazily by request-time
metadata query; Mooncake neither downloads provider values nor scans KVCS active routes to
reconstruct a NoF queue. Mooncake-managed ExtentStore targets are different: their executor stores
the same cold-object manifest shape used by local Cold Tier recovery, so the current target owner
can feed recovered objects into the existing Cold Tier manifest-recovery path.

KVCS targets must therefore be configured consistently on clients that share a route namespace.
Changing a target set does not create a metadata migration. Because KVCS 0.4.0 has no listing API,
Mooncake cannot enumerate or reclaim provider records that are unreachable from a current logical
route and target configuration.

## Replica write coordination

The existing memory-replica policy chooses one primary client for a logical write. When several
clients hold memory replicas, only that primary may trigger the Cold Tier/NoF write. Secondary
memory replicas do not independently write the same object, publish a second route, or select a
different NoF replica set.

The primary reuses the shared Cold Tier replica policy to select the NoF targets and to apply the
configured success condition. Target writes may run in parallel, but the operation has one logical
writer:

```text
memory-replica primary
    -> shared target placement and replica policy
    -> direct writes to selected target executors
    -> target-owner route publication for each successful target
    -> return after the existing replica success condition
```

For Standard KVCS, one provider target is selected because KVCS owns its internal replication and
manifest. For Low-Level KVCS, all targets selected by `nof_replica_count` receive the same
derived key. For Mooncake-managed ExtentStore, each selected target has an independent
`NofBackingRoute` location and its own target owner.

If a request arrives at a secondary memory replica, the existing memory-replica forwarding path
chooses the primary before any NoF side effect. NoF does not add a second leader-election or
replica-repair algorithm.

## Managed ExtentStore and SPDK

`ExtentStoreExecutor` implements the managed traits. A write receives the complete object once,
allocates an aligned ExtentStore record, flushes the block device, and returns an opaque locator.
Publication is owner-driven: the target owner reserves an extent, CAS-publishes a
`PendingWrite` location in `ObjectRoute.nof_backing`, the writer performs the direct write and
flush, and the owner CAS-publishes `Materialized`. If the CAS fails or the entry is deleted,
the owner releases the locator after the normal drain/write ordering guarantees complete.
Reads, deletes, replicas, load balancing, retries, and owner-scoped health reuse the existing Cold
Tier runtime.

The managed owner has two recovery inputs, both intentionally thin:

- Owner handoff or restart with intact metadata lists materialized NoF routes filtered by
  `target_id` and passes their opaque locators to `NofManagedAllocator::recover`. ExtentStore
  rebuilds its free ranges from those routes; it does not keep a second allocation journal.
- If metadata no longer has a healthy route for an object but the target still contains the
  kvcache record, the owner uses the executor's optional recovery scan. The scan returns
  `RecoveredColdObject` values backed by the shared cold-object manifest codec, then the existing
  Cold Tier startup recovery CAS path creates or repairs `ObjectRoute.nof_backing`.

This is not a NoF-specific rebuild queue and not a cold-copy planner. It is the same disk-manifest
route recovery used by local Cold Tier, with the final backing field switched from
`cold_backing` to `nof_backing`. KVCS does not use this path because KVCS owns its external
metadata and has no provider list API in the current SDK.

Normal shutdown uses the graceful-drain route-snapshot handoff described in
[Ownership and health](#ownership-and-health). The replacement calls
`NofManagedAllocator::recover` with the transferred snapshot and does not enumerate the target
route index again. Crash takeover and target re-online use the route-index lookup above. The fast
path transfers metadata only; it does not move payloads or change executor locators.

`SpdkNofBlockDevice` is the transport below ExtentStore. It attaches an NVMe-oF controller,
serializes I/O through the current SPDK qpair, uses SPDK DMA buffers for submitted chunks,
performs aligned block I/O, exposes geometry, and implements the flush barrier. Extent alignment
is not part of the generic NoF traits.

A StoreClient target set uses exactly one authority mode. Provider-owned KVCS targets and
Mooncake-managed ExtentStore targets are configured on separate clients so recovery and deletion
semantics cannot be mixed.

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

Each NoF target has one current runtime owner for its control plane. The owner is a derived
control-plane view: every client computes it from the configured target-set fingerprint and the
current active client leases using stable rendezvous assignment. No separate owner database is
read on the data path.

```text
target_id
owner_runtime
assignment = rendezvous(target-set, active-client-leases)
```

The current owner publishes only its cached health result through the existing client lease
label. A client joining or leaving changes the computed assignment on the next lease refresh;
there is no second owner metadata store.

This lease-derived owner view is distinct from provider-owned external metadata. External metadata
is the provider's own object/manifest state (for example KVCS shard manifests) and is not used to
store NoF owner assignments or Mooncake-managed handoff snapshots.

The registration section above defines the initial owner. After assignment, local disks and NoF
targets use the same Cold Tier health, placement, replica, cleanup, and runtime lifecycle. Adding
a client does not rebalance existing owners.

All clients still create their own executor handle for every configured NoF target. The owner does
not proxy data I/O:

- reads use the materialized route and call the target executor directly;
- payload writes are sent directly to the executor;
- only target allocation, route publication/deletion, allocator recovery, GC/reclaim, and
  owner-scoped health require the current owner.

### Owner state transitions

There is no persisted owner record or owner-generation CAS. The current owner is recomputed from
the active compatible leases whenever the cached membership view changes. A client only performs
owner-scoped work while the derived owner for that target is its own runtime. A client joining
does not move an existing target unless the current owner drains or its lease expires.

### Graceful drain fast path

Normal shutdown first uses the existing client drain order to flush accepted offloads while the
old owner is still present. Before withdrawing its NoF lease labels, the old owner:

1. recomputes the current target assignment;
2. selects the next compatible runtime using the same rendezvous rule, excluding itself;
3. lists materialized `ObjectRoute` entries filtered by that target ID;
4. sends the route snapshot to the successor through the control plane.

The successor accepts the snapshot only for its own runtime and keeps it in process memory. When
the lease view changes and the target becomes locally owned, managed ExtentStore recovery consumes
that snapshot and calls the executor's existing `recover` interface. It does not enumerate the
route index again. The snapshot is metadata only: it does not move payloads, change locators, or
grant the successor data-proxy rights.

If there is no eligible successor, the transfer fails, the process crashes, or the target comes
back online later, recovery falls back to the normal target-scoped route lookup. That is the
crash/re-online path, not the normal shutdown fast path. The fast path is used only by
Mooncake-managed ExtentStore targets; provider-owned KVCS targets do not have Mooncake route
snapshots.

The local drain waits for writes already accepted by the outgoing process. It is not a
cross-client data-plane lock. Existing route states and publication ordering remain the
protections against late writes.

### Heartbeat health

Owner heartbeat is separate from owner handoff. A background heartbeat runs once per second and
request paths consume its cached result:

- each client probes only targets assigned to it;
- three consecutive failures exclude a target from placement and one success restores it;
- the owner publishes unhealthy target IDs and optional capacity in the existing client lease;
- a target health failure does not by itself reassign the owner;
- owner reassignment occurs on explicit drain, client lease expiry, or loss of owner eligibility.

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
    managed.rs
    managed_backend.rs
    runtime.rs
    extent_store/
      block.rs
      executor.rs
      spdk.rs
    kvcs/
      capi.rs
      executor.rs
      physical_layout.rs
```

KVCS configuration, C API calls, and its private Low-Level layout live under `nof/kvcs/`.
`physical_backend.rs` passes complete objects to a key-addressed provider and never publishes a
route. `managed_backend.rs` is the thin bridge between the existing Cold Tier state machine and a
locator-returning executor. It owns locator encoding, batch bounds, flush-before-publication, and
rollback; ExtentStore alone owns extent alignment and record layout.

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


### SPDK build dependency

The managed ExtentStore executor itself is pure Rust. Enable `nof-spdk` only when the NVMe-oF SPDK
transport is required. Install [SPDK](https://spdk.io/doc/getting_started.html) with the
`spdk_nvme`, `spdk_env_dpdk`, and `spdk_syslibs` pkg-config files, then point the build at either
an installed prefix or an SPDK build tree:

```shell
export MOONCAKE_SPDK_PREFIX=/opt/spdk-26.05
cargo build -p mooncake-store-client --features nof-spdk --offline
```

Without `nof-spdk`, `mooncake-nof-sys` does not compile or link the C++ shim. The transport shim is
the only C++ layer; allocation, record codec, checksum, buffer pooling, replica policy, and route
lifecycle reuse existing Rust components.

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
plane so that every client can query and read every target directly. Registration is followed by
the common target-owner assignment: local disks keep the registering node as owner, while NoF
targets are assigned to one eligible active runtime. The owner manages target control-plane
operations and health; it does not proxy I/O or authorize ordinary reads.

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

Register a managed ExtentStore target by wrapping an exclusive block-device region. The block
device may be an SPDK NVMe-oF namespace or another implementation of `NofBlockDevice`:

```rust,ignore
let executor = Arc::new(ExtentStoreExecutor::new(
    block_device,
    ExtentStoreExecutorConfig {
        base_offset: 0,
        size_bytes: 16 * 1024 * 1024 * 1024,
    },
)?);
let target = NofTargetConfig::new("nof-disk-0", NofBackend::new(executor)?)?;
let client = StoreClientBuilder::new(metadata, "storage-0")
    .nof_target(target)
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

## Four-node managed NoF validation

The SPDK shim is compiled as a static Rust/C++ shim and links the external SPDK/DPDK
libraries dynamically. This is intentional: the four NoF hosts run Ubuntu 24.04/glibc 2.39,
while the canonical Rust build container is Ubuntu 22.04/glibc 2.35. Linking the host-built SPDK
archives statically in the container produces `__isoc23_*`/`__strlcpy_chk` ABI failures. The
build therefore requires the SPDK shared libraries and keeps their directory in
`LD_LIBRARY_PATH` when the test binary runs.

The reproducible commands are:

```shell
# Run inside the canonical Mooncake Store-RS container.
source /root/.cargo/env
export MOONCAKE_SPDK_PREFIX=/nvme/cruz.zxp/spdk-26.05-host
# The reserved initiator and NoF hosts are CPU-only.
export MOONCAKE_ENABLE_CUDA=0
scripts/e2e/build-nof-multi-client.sh
# Optional: run the SPDK-backed NoF unit-test subset with the same environment.
NOF_RUN_UNIT_TESTS=1 scripts/e2e/build-nof-multi-client.sh
```

The multi-client build defaults `MOONCAKE_ENABLE_CUDA` to `0` because the
reserved validation hosts have no CUDA device. Override it explicitly to `1`
only when running the binary on a GPU-capable initiator. This avoids compiling
CUDA pointer/copy paths that fail during CPU-only validation even when CUDA
headers are present in the canonical build container.

`build-nof-multi-client.sh` validates `spdk_nvme.pc`, `spdk_env_dpdk.pc`,
`spdk_syslibs.pc`, the shared SPDK libraries, Rust formatting, and the final binary's dynamic
library resolution. The test artifact is `target/debug/nof_multi_client` (or the release path when
`NOF_BUILD_PROFILE=release`). The bastion runner passes the same profile through to staging; set
`NOF_RUN_UNIT_TESTS=1` to run the NoF unit subset before any target reset.

`run-nof-multi-client.sh` repeats `ldd -r` on the actual initiator after adding its SPDK runtime
directory, and writes hostname, kernel, interface inventory, binary hash, and runtime-library
status to `run-manifest.txt`. A run is not started when a shared library or symbol is unresolved.

The multi-node command runs on the bastion. It builds in the container, resets only the
provisioned NoF image on every configured target, stages the exact binary and runner with `rsync`,
verifies the binary SHA256 on each initiator, and runs the configured client subset on each
initiator. Initiators do not need to SSH to target public addresses, so target cleanup is
deliberately performed from the bastion:

```shell
# On 11.158.240.229.
rsync -a -e 'ssh -o BatchMode=yes' \
  root@10.88.0.5:/nvme/cruz.zxp/Mooncake-Store-RS-nof-pr2-integrated/scripts/e2e/run-nof-multi-client-from-bastion.sh \
  /tmp/run-nof-multi-client-from-bastion.sh
bash /tmp/run-nof-multi-client-from-bastion.sh
```

The script defaults are localhost-safe placeholders. A real reservation must pass the topology
explicitly:

- `NOF_TARGETS` is a comma-separated target list. Each entry is
  `public_host|traddr|target_id|subnqn|port`. The number of NoF targets is the number of entries.
- `NOF_CLIENT_IDS` is the global client list used by the test binary for barriers and read
  verification.
- `NOF_INITIATORS` is a semicolon-separated initiator list. Each entry is
  `ssh_host|bind_ip|local_client_ids`; the number of initiator machines is the number of entries.
  `local_client_ids` is the comma-separated subset started on that initiator.
- `NOF_BARRIER_REDIS_URL` controls the cross-initiator barrier. It defaults to `NOF_REDIS_URL`.

Every reset stops `mooncake-nvmf.service`, removes and recreates `/var/lib/mooncake-nof/nof.img`
at `NOF_TEST_IMAGE_BYTES`, and restarts the service; it never touches the system NVMe disk. Run
logs and the manifest are left under `/tmp/mooncake-nof-multi-client/logs/<run-tag>/` on each
initiator.

For a manually staged run, use `scripts/e2e/run-nof-multi-client.sh` with
`NOF_RESET_TARGETS=0`, `NOF_SKIP_TARGET_SSH_CHECK=1`, the local `NOF_BIND_IP`, and the same
Redis/SPDK settings after performing the reset from the bastion. `NOF_LOCAL_CLIENT_IDS` can be
used to start only the client subset that belongs on the current machine while keeping
`NOF_CLIENT_IDS` as the global client set. Do not use `scp`.

The runner sets `MC_STORE_RS_ENABLE_COLD_TIER=1`, creates per-run registration, write/offload,
and read-start barriers, and starts the client IDs from `NOF_CLIENT_IDS` with
`NOF_REPLICA_COUNT`. Each client writes a disjoint object set, waits until all configured clients
finish offload and the routes report materialized Managed NoF backing, then all clients issue
`batch_get_into` reads concurrently and verify the complete object set. The test binary uses
`NoopTransport` for the local hot segment, so it does not claim to validate remote hot-memory
transfers; the cross-client assertion is the managed NoF read path. A passing run must contain the
configured write/offload and read verification counts in each client log. The target list is
configured by `NOF_TARGETS`, client IDs by `NOF_CLIENT_IDS`, local client subset by
`NOF_LOCAL_CLIENT_IDS`, initiator machines by `NOF_INITIATORS`, and the image size by
`NOF_TEST_IMAGE_BYTES`; the reset operation removes and recreates only
`/var/lib/mooncake-nof/nof.img` and never touches the system NVMe disk.

## Validation

Default build and tests do not require KVCS:

```shell
cargo fmt --all -- --check
MOONCAKE_SKIP_NATIVE_BUILD=1 cargo test -p mooncake-store-core --offline
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --lib --offline -- --test-threads=1
```

Managed ExtentStore validation does not require KVCS or SPDK:

```shell
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --lib \
  client::cold_tier::nof::extent_store:: --offline -- --test-threads=1
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

## Compatibility

Managed NoF routes are stored in `ObjectRoute.nof_backing` using protobuf field 15; field 14
remains reserved. The metadata capability gate rejects managed NoF routes on backends that do not
advertise NoF backing support. Existing routes without `nof_backing` continue to decode normally;
a cluster must enable the backing-route capability before it writes managed NoF placement metadata.
