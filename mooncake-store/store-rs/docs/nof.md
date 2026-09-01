# NoF architecture

NoF is one capability-based framework under the existing Cold Tier subsystem. It has no separate
control plane and no high-level/low-level framework split. `NofBacking` is the runtime composition
trait; a backing exposes only the capability traits that Mooncake is allowed to call.

## Capability model

The capabilities are independent:

| Capability | Trait | Purpose |
| --- | --- | --- |
| logical object write/read/query/delete | `NofObjectWrite`, `NofObjectRead`, `NofObjectQuery`, `NofObjectDelete` | provider object APIs with namespaces and native manifests |
| physical KV write/read/query/delete | `NofPhysicalWrite`, `NofPhysicalRead`, `NofPhysicalQuery`, `NofPhysicalDelete` | raw key/value APIs used by the shared physical adapter |
| external logical metadata | `NofExternalMetadata` | typed NoF get/list/CAS over the existing `MetadataBackend` |
| backing health | `NofHealth` | liveness and optional capacity without claiming storage or device ownership |
| storage maintenance | `NofStorageManagement` | optional physical inventory and capacity contract for Mooncake-managed backings |
| device lifecycle | `NofDeviceManagement` | provider-supported health and device lifecycle integration |

Capability absence is authoritative. It means the provider owns the responsibility internally or
does not expose it through a supported API. Mooncake does not infer capabilities from a mode name
and does not emulate a missing SDK function with an undocumented CLI or private service API.

`NofBackend` is the single public facade. It contains an `Arc<dyn NofBacking>`, validates the
limits advertised for the selected data plane, performs the existing provider-neutral shard/key
planning, and optionally composes `NofExternalMetadata`. A backing may expose object I/O, physical
I/O, health, or management-only capabilities without creating another framework branch.

The current KVCS mapping is:

| `MOONCAKE_KVCS_MODE` | Exposed data-plane traits | External metadata | Health/management traits |
| --- | --- | --- | --- |
| `standard` | object read/write/query/delete | absent; KVCS owns namespace and manifest metadata | absent; EFC owns physical maintenance and disks |
| `low-level` (default) | physical read/write/query/delete | composed from Mooncake's existing metadata backend by the runtime binding | query-backed `NofHealth`; storage/device management absent because EFC owns them |

Both modes use one `KvcsCapiExecutor`. Mode selection initializes only the corresponding SDK
client configuration and controls which `NofBacking` capability accessors return `Some`. Calling a
trait from the wrong mode is rejected; it never silently crosses into the other SDK API.

## Persisted backing metadata

The runtime `NofBacking` trait is deliberately separate from the persisted `NofBackingRoute`
record. Traits cannot be a stable Redis/protobuf schema. Local disks use
`ObjectRoute.cold_backing: ColdBackingRoute`, while NoF uses
`ObjectRoute.nof_backing: NofBackingRoute`; a route cannot contain both. A remote NoF target is
never encoded as a local `cold_tier_id`.

Core types, Redis indexing, protobuf transport, admin output and `NofExternalMetadata` retain
`nof_backing` as its own field. The metadata trait is a typed extension over the injected
`MetadataBackend` (in-memory, Redis or etcd); it owns no connection, cache, journal or second
keyspace implementation. NoF enumeration uses `NofBackingRouteFilter`, while local Cold Tier
cleanup continues to use `ColdBackingRouteFilter`.

The additive route field is gated by `nof-backing-route-v1`. A writer cannot publish
`nof_backing` unless its route compatibility descriptor advertises that capability. Operators
must enable NoF only after every control-plane node that may decode and rewrite routes supports
the field.

## Reuse of Cold Tier

The physical adapter reuses the internal `PersistentStorageBackend` I/O envelope,
`MetadataBackend`, checksum validation, `ValueChunkPlan`, `PhysicalKeyCodec` and the existing Cold
Tier batching entry points.
It adds only collision-resistant physical keys, value chunk records, a small root manifest and
calls to the backing's physical `put/get/delete/flush` traits. It does not add a scheduler,
allocator, buffer pool, metadata service, GC loop or device manager.

Multi-replica placement and read balancing share the provider-neutral
`ReplicaLoadBalanceStrategy`. Both `ColdBackingRoute` and `NofBackingRoute` implement the same
`ReplicaTargetSet`: write placement keeps the existing provider-computed score ordering and uses
the accumulated write count to spread equal-score targets, while reads keep the existing
`(inflight_io, batch_ios, global_accum_ios)` ordering. Backings still own
eligibility and capacity inputs, so sharing the strategy does not make a NoF mountpoint a local disk
or opt it into local-disk maintenance.

`NofStorageManagement` is the optional inventory/capacity boundary for a backing that delegates
physical maintenance to Mooncake. Its storage health feeds the existing backend health and
management classification; the provider-specific adapter must supply exhaustive inventory before
physical reconciliation can be enabled. If the capability is absent, the provider owns orphan
collection, watermarks and compaction. `NofDeviceManagement` is evaluated independently. KVCS
exposes neither management trait because its public 0.4.0 client ABI does not provide those
operations, so no KVCS path runs Mooncake physical inventory or watermark maintenance. KVCS
Low-Level does expose `NofHealth`: it sends a side-effect-free existence query to the selected
mountpoint from the background heartbeat worker, while leaving capacity unknown because the SDK
has no capacity API.

`StoreClientBuilder::nof_target(s)` registers the runtime backends. The binding reuses the existing
Cold Tier state machine in this order: select target, publish `PendingWrite`, enqueue, pin/read the
hot payload, batch write, flush, publish the materialized route by CAS, and roll back an unpublished
payload on conflict. The same restore singleflight, batch grouping, checksum validation, promotion
and delayed-delete machinery handles NoF routes. Only the provider-specific I/O call changes.

The old backend trait still accepts a `ColdBackingRoute`, so the runtime creates a short-lived
in-memory envelope when invoking it. That envelope is never written to metadata. Every route CAS
for a NoF object writes `ObjectRoute.nof_backing`, and restore/delete convert it only at the I/O
boundary. This keeps the mature orchestration without pretending that a remote NoF target is a
local disk.

Target selection is capability driven:

- Cold Tier resolves target authority in one shared `owner` module. A local-disk backing is fixed
  to the runtime that selected the device and persists that runtime as its authoritative owner;
  only NoF targets use distributed ownership and treat the route owner as a snapshot;
- a logical-object target is selected once; namespace metadata, placement and replication remain
  provider owned; Mooncake derives a versioned provider object key with the existing SHA-256 key
  codec so delayed reclamation of an overwritten route cannot delete the replacement value;
- physical-KV targets use `nof_replica_count` and the shared `ReplicaLoadBalanceStrategy`; each
  successful target write contributes its real locator to `NofBackingRoute.replicas`;
- single and batch reads use the same inflight/batch/global counters to choose among runtime-local
  replicas;
- NoF-enabled clients publish a `nof.target-set.v1` lease label. The value fingerprints the sorted
  target IDs and selected data plane, so a client with a different NoF configuration is never made
  owner of an incompatible target;
- all clients derive the same `target_id -> ClientRuntimeId` ownership map from the existing live
  client cache and the existing Rendezvous placement hash. The stable client ID is the hash
  candidate: targets are statistically spread across clients, an epoch restart keeps the same
  placement, and removing one client remaps only the targets that it owned;
- one background owner worker runs per NoF-enabled client. Once per second it refreshes the map
  from the local membership snapshot and probes only the targets currently owned by that client.
  Request paths never call the SDK health operation; a local owner uses its executor-health
  snapshot, while another client requires the elected owner's Client lease to remain live and
  consumes its `nof.unhealthy-targets.v1` snapshot;
- the owner updates that health label only when the unhealthy set changes. Normal Client heartbeat
  publication preserves the two NoF-managed labels, so no target-by-client polling or per-request
  metadata read is introduced;
- a target remains eligible through two transient heartbeat failures and is excluded after the
  third consecutive failure; the first successful heartbeat makes it eligible again immediately;
  a cold restore filters unavailable targets, promotes a healthy replica and publishes the pruned
  `nof_backing` through the existing restore CAS;
- NoF-only operation does not load `ColdTierDeviceRecord`, run local watermarks, or update local
  disk capacity accounting.

The primary and every replica in `NofBackingRoute` record the owner elected for that target when
the route is published. This field is a routing/audit snapshot, not a second lease. Current
authority always comes from the live target-owner map, so an ownership change does not scan and
rewrite every object route. Delete/reclaim resolves the current owner by `target_id`; pending-write
recovery enumerates the targets currently owned by the client and also retains the existing hot
replica-owner source for EmbeddedWrh routes.

During graceful client shutdown, the NoF manager removes `nof.target-set.v1` from its existing
Client lease before longer route and local-device cleanup starts. Other clients observe that
withdrawal through the normal one-second membership refresh and immediately recompute ownership.
For a process or machine crash, takeover follows the existing Client lease expiry and epoch fence;
NoF does not add a parallel membership or lease service.

Startup recovery requeues `PendingWrite` routes from external metadata and from the existing route
authority's replica-owner listing. The second source covers the default EmbeddedWrh mode, where
live routes are not duplicated into the external metadata table. These route indexes are the
supported recovery source when the SDK has no physical list API. A definite route CAS conflict
removes every unpublished NoF target. An unknown CAS outcome retains the idempotent locator for
retry rather than risking an applied route pointing at deleted data; records left by a process
crash or an outcome that can never be confirmed remain the provider's GC responsibility.

## Module layout

```text
client/
  cold_tier/
    layout/
      mod.rs
      physical_key.rs
      value_chunk.rs
    owner.rs
    nof/
      mod.rs
      backing.rs
      backend.rs
      object.rs
      physical.rs
      physical_adapter.rs
      runtime.rs
      external_metadata/
        mod.rs
        tests.rs
      kvcs/
        mod.rs
        executor.rs
        capi.rs
        capi_tests.rs
```

Public provider-neutral traits live directly under `nof/`. All KVCS-specific configuration, C API
calls and tests live under `nof/kvcs/`. `executor.rs` contains one mode-selecting KVCS executor;
`capi.rs` is the private binding for the vendor's public C ABI.

## KVCS Low-Level capability boundary

The capability boundary below was verified against the public 0.4.0 C header, exported ELF
symbols, official Rust/Python/Go wrappers, SDK mock, EFC package configuration and the EFC tools
shipped in the same release.

| Capability | Low-Level SDK ABI | EFC/provider surface | Mooncake decision |
| --- | --- | --- | --- |
| raw put/get/get-into/delete | yes | EFC data plane | use SDK |
| existence query | yes; `ok`, `not_found`, `unavailable` | `KVCS_OP_EXISTS` | use SDK for liveness and metadata-driven reconciliation |
| namespace/object metadata | no; namespace is fixed to `ll` | Standard client uses Redis | keep route/version/owner/checksum/replicas in `NofBackingRoute` within shared Mooncake metadata |
| placement or replica locations | no | Standard client only | keep low-level replica placement in NoF external metadata |
| mountpoint selection | yes, one `mountpoint_index` per batch | EFC maps the index to a configured filesystem | use SDK; one NoF target is one configured mountpoint |
| physical key list/scan | no | `kvcs_cli list --max N` is an EFC operations command, not SDK ABI | optional only; KVCS does not advertise it |
| capacity/available bytes | no; SDK metrics contain operation counters only | EFC `stat` and Prometheus expose node total/used/ratio | do not fabricate SDK capacity; use configured logical scheduling capacity or a separate supported telemetry integration |
| physical GC/watermarks | no client action | provider/EFC managed | skip Mooncake physical inventory GC and watermark cleanup for KVCS |
| disk offline/online/recover/rebuild | no | `kvcs-disk-ops` and EFC own physical disks | do not duplicate in Mooncake |
| flush/durability barrier | no | no public Low-Level barrier | rely only on documented synchronous operation completion; do not claim a stronger barrier |

EFC's operations surface includes `GetStat`, `ExecuteGC`, `BatchDelete`, a local key-list command,
Prometheus capacity gauges, and disk hot-offline/recovery tooling. Those observations establish
that physical maintenance belongs below the SDK, but they do not make the private gRPC schema or
CLI output a supported executor ABI. Integrating such a surface later requires a separately
versioned provider management adapter.

Because the SDK has no physical list, KVCS supports only one-way, metadata-driven reconciliation:
enumerate the NoF backing routes for the target, decode each stored NoF
root locator, and call Low-Level `query`. A missing root invalidates that target reference; a
healthy replica may be promoted before the stale reference is removed. Physical objects with no
Mooncake route cannot be discovered through the SDK and are left to provider GC. A full
`MooncakeManaged` executor instead supplies a stable paginated list, which enables two-way
metadata/physical reconciliation and orphan deletion.

The SDK exposes a filesystem/mountpoint target, not EFC's individual NVMe disk identities. A
runtime binding may fence and retire a failed NoF target when query returns `ENODEV` or
`unavailable`, but individual disk offline/replacement remains an EFC operation. Such a binding
must remove target metadata only after route references have been promoted or pruned; deleting a
target record first would create dangling routes.

## SDK and EFC downloads

The supported public KVCS baseline is **0.4.0**, commit
`2089637f8d1a0144814689c8ba49d3d9034799d9`. One SDK archive contains the C ABI, production and
mock libraries, the Rust path crate, and the Python and Go packages; there are no separate
language downloads.

Authoritative entry points:

- [KVCacheStore quick start](https://www.alibabacloud.com/help/en/kvcachestore/quick-start)
- [official installer](https://kvcachestore.oss-accelerate.aliyuncs.com/scripts/install-kvcs.sh)

Versioned SDK archives:

| Architecture | Download | SHA-256 snapshot verified 2026-09-01 |
| --- | --- | --- |
| x86_64 | [kvcs-sdk-0.4.0-x86_64.tar.gz](https://kvcachestore.oss-accelerate.aliyuncs.com/sdk/kvcs-sdk-0.4.0-x86_64.tar.gz) | `61b1cee7c0e87975d8e3d723c1e3335cabc46a6af2efeced233918f688f2b3c9` |
| aarch64 | [kvcs-sdk-0.4.0-aarch64.tar.gz](https://kvcachestore.oss-accelerate.aliyuncs.com/sdk/kvcs-sdk-0.4.0-aarch64.tar.gz) | `22159a8fa911799857db6c8d084220efc4d6ac9adc79302a7e35371a71d1fd6c` |

The hashes are locally recorded snapshots because the vendor does not currently publish signed
sidecar checksums. Recheck them when artifacts are refreshed.

Live tests require matching EFC 0.4.0. The official installer selects the package for the host;
the direct package URLs are listed here for reproducible provisioning:

| Format | x86_64 / amd64 | aarch64 / arm64 |
| --- | --- | --- |
| RPM | [x86_64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc-0.4.0-1.x86_64.rpm) | [aarch64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc-0.4.0-1.aarch64.rpm) |
| DEB | [amd64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc_0.4.0_amd64.deb) | [arm64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc_0.4.0_arm64.deb) |

All six versioned URLs and the installer returned HTTP 200 when verified on 2026-09-01. The
installer's previously documented deployment tarball naming is not published for 0.4.0, so it is
not presented as an available package.

To fetch only the SDK without installing EFC:

```text
KVCS_VERSION=0.4.0
KVCS_ARCH="$(uname -m)"  # x86_64 or aarch64
curl -fL \
  "https://kvcachestore.oss-accelerate.aliyuncs.com/sdk/kvcs-sdk-${KVCS_VERSION}-${KVCS_ARCH}.tar.gz" \
  -o "kvcs-sdk-${KVCS_VERSION}-${KVCS_ARCH}.tar.gz"
tar xzf "kvcs-sdk-${KVCS_VERSION}-${KVCS_ARCH}.tar.gz"
sudo mkdir -p /opt/kvcs-sdk
sudo mv "kvcs-sdk-${KVCS_VERSION}" "/opt/kvcs-sdk/${KVCS_VERSION}"
sudo ln -sfnT "/opt/kvcs-sdk/${KVCS_VERSION}" /opt/kvcs-sdk/latest
```

For a complete node installation, download the installer first, inspect it, and then run it. The
installer downloads the architecture-specific EFC and the same multi-language SDK archive:

```text
curl -fL \
  https://kvcachestore.oss-accelerate.aliyuncs.com/scripts/install-kvcs.sh \
  -o install-kvcs.sh
less install-kvcs.sh
sudo bash install-kvcs.sh
```

The SDK ships an official Rust crate, but it is currently documented only as a local path
dependency (`/opt/kvcs-sdk/latest/rust`), not as a registry coordinate. Adding that absolute path
to this workspace would make even default Cargo metadata depend on a host installation. The
integration therefore keeps the opt-in feature portable and links the SDK's sole supported public
C ABI. It does not copy SDK algorithms or provider control-plane code. If a stable registry package
becomes available, the C declarations can be replaced by that dependency without changing the NoF
traits.

## KVCS executor construction

`KvcsCapiExecutor::new` reads `MOONCAKE_KVCS_MODE`; unset defaults to `low-level`. Tests and
embedding code may select the same behavior explicitly with `with_mode`:

```rust,ignore
use std::sync::Arc;
use mooncake_store_client::{
    KvcsCapiExecutor, KvcsMode, NofBackend,
};

let executor = Arc::new(KvcsCapiExecutor::with_mode(KvcsMode::LowLevel)?);
let backend = NofBackend::new(executor)?;
let target = mooncake_store_client::NofTargetConfig::new("kvcs-mount-0", backend)?;

let client = mooncake_store_client::StoreClientBuilder::new(metadata, "storage-0")
    .nof_target(target)
    // Used only by physical-KV targets; Standard/provider-object mode ignores it.
    .nof_replica_count(2)
    .build(expires_at_ms)?;
```

NoF uses the existing `MC_STORE_RS_ENABLE_COLD_TIER=1` lifecycle switch because it reuses the Cold
Tier offload/restore workers. The runtime binding writes `nof_backing`, never the target in
persisted `cold_backing`.
Any configured Mooncake capacity is a logical admission/load-balancing limit, not a claim about
EFC physical free space and not a request for Mooncake watermark GC. KVCS mountpoint selection is
supplied to the concrete executor (`MOONCAKE_KVCS_MOUNTPOINT_INDEX`).

## KVCS build and runtime dependencies

The default feature set has no KVCS dependency. `kvcs-capi` also adds no Cargo package: the
dependency graph and `Cargo.lock` are unchanged because Mooncake uses the SDK's public C ABI
directly. The existing `libc` crate supplies C-compatible Rust types; the existing protobuf build
dependencies are unrelated to KVCS.

| Build mode | SDK files required at build time | Linked library | Runtime loader path |
| --- | --- | --- | --- |
| default features | none | none | none |
| `kvcs-capi`, production | `COMMIT_ID`, `C/include/kvcs_capi.h`, `lib/libkvcs.so` and its SONAME chain | dynamic `libkvcs.so.0` | `$KVCS_SDK_ROOT/lib` |
| `kvcs-capi`, official mock | the same metadata/header plus `mock/lib/libkvcsmock.so` and its SONAME chain | dynamic `libkvcsmock.so.0` | `$KVCS_SDK_ROOT/mock/lib` |

The adapter deliberately does not depend on the SDK's `rust/` path crate, `pkg-config`, `bindgen`,
the KVCS C++ sources, `libkvcs.a`, or `libstdc++-static`. Those belong to the vendor Rust wrapper's
default static-link flow, not to Mooncake's C ABI integration. EFC and Redis are runtime services,
not compilation dependencies: Standard-mode operations require EFC plus Redis, while
Low-Level operations require EFC and a configured mountpoint but do not use Redis.

`build.rs` enforces the supported binary boundary before emitting a linker directive:

- Linux GNU target only; musl, macOS and Windows are rejected;
- Cargo target architecture must be `x86_64` or `aarch64` and must match `arch` in `COMMIT_ID`;
- `version` in `COMMIT_ID` must be exactly `0.4.0` until another SDK ABI has been reviewed;
- the public header and the selected unversioned `.so` linker name must both exist.

The metadata check prevents accidental ABI or architecture mismatches; it is not an integrity
check. Verify the downloaded archive against the SHA-256 snapshot above before installation.

The published x86_64 production library has SONAME `libkvcs.so.0` and directly needs only the GNU
loader plus glibc's `libc`, `libdl`, `librt`, `libpthread` and `libm`; its newest referenced glibc
symbol is `GLIBC_2.17`. It has no dynamic `libstdc++`, gRPC, protobuf or OpenSSL dependency. The
x86_64 mock directly needs the GNU loader, `libc` and `libm`, with `GLIBC_2.14` as its newest
referenced symbol. These are properties of the inspected 0.4.0 SDK libraries only: the final
Mooncake binary can require a newer glibc if it is built on a newer baseline.

Mooncake emits a link search path but does not embed an RPATH. Production images must therefore
install the complete `.so` SONAME chain in the system loader configuration or set
`LD_LIBRARY_PATH` when starting the process. Do not copy only the unversioned linker symlink.

### Build environment

`KVCS_SDK_ROOT` is a Mooncake build input and defaults to `/opt/kvcs-sdk/latest`.
`KVCS_SDK_USE_MOCK` is also a build-time switch: unset or `0` selects production, and `1` selects
the official mock. Any other value is rejected. Cargo reruns the build script when either value
changes. `MOONCAKE_KVCS_MODE` is a runtime construction parameter: `standard` exposes only the
logical-object traits, while `low-level` exposes only the physical-KV traits; unset defaults to
`low-level`.

The executor exposes only the connectivity and target-selection inputs required by the two public
SDK modes:

| Variable | Mode | Meaning |
| --- | --- | --- |
| `MOONCAKE_KVCS_EFC_SOCKET` | both | EFC Unix socket; Low-Level defaults to `/var/run/kvcs/efc-grpc.sock` |
| `MOONCAKE_KVCS_REDIS_ENDPOINTS` | Standard | comma-separated Redis endpoints such as `tcp://host:6379` |
| `MOONCAKE_KVCS_REDIS_PASSWORD` | Standard | optional Redis password |
| `MOONCAKE_KVCS_MOUNTPOINT_INDEX` | Low-Level | provider mountpoint index; unset or `0` selects the default filesystem |

Mooncake does not create a parallel tuning surface for SDK worker counts, ring depth, value/key
limits, logging, metrics or performance reporting. Those `kvcs_*_config_t` fields are left at the
public ABI's documented zero values, so the SDK owns its defaults. The adapter advertises the same
documented 4 MiB raw value, 256-byte raw key and 256-item batch defaults to the NoF framework.

The vendor's top-level `env.sh` assigns `KVCS_SDK_ROOT` as a shell variable but does not export it,
and `mock/env.sh` configures the vendor Rust wrapper through `KVCS_DYNAMIC`. Mooncake does not read
`KVCS_DYNAMIC`, `KVCS_NO_STDLIB_STATIC`, `PKG_CONFIG_PATH` or `LIBRARY_PATH`, so set the Mooncake
variables explicitly:

```text
# Production build.
export KVCS_SDK_ROOT=/opt/kvcs-sdk/0.4.0
export KVCS_SDK_USE_MOCK=0
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo build -p mooncake-store-client --features kvcs-capi --offline

# Production runtime or live tests.
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# Official mock build and test process.
export KVCS_SDK_USE_MOCK=1
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/mock/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  nof:: --offline -- --test-threads=1

# The regular filter leaves live-provider smoke tests ignored. With the official mock linked,
# run both C ABI round trips explicitly.
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  client::cold_tier::nof::kvcs::executor::standard::tests::live_standard_round_trip_smoke \
  --offline -- --exact --ignored --test-threads=1
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  client::cold_tier::nof::kvcs::executor::low_level::tests::live_low_level_round_trip_smoke \
  --offline -- --exact --ignored --test-threads=1
```

Build and runtime library selection must agree. `KVCS_SDK_USE_MOCK=1` changes the library recorded
at link time; changing only `LD_LIBRARY_PATH` is not a supported way to replace a production build
with the mock.

Public 0.4.0 requires a non-empty Low-Level EFC socket. The executor uses
`MOONCAKE_KVCS_EFC_SOCKET` when set and otherwise passes the documented
`/var/run/kvcs/efc-grpc.sock`; it never passes a null socket and relies on an SDK-version-specific
fallback.

The adapter enforces the SDK's documented raw value limit and validates the key after converting
the opaque physical key to the C ABI's hex string. The installed header defaults to a 256-byte raw
key limit and a 4 MiB value limit. These limits remain inside the KVCS executor.

The KVCS C ABI exposes a mountpoint index but no manager epoch or device-generation fence. This
integration therefore does not claim physical fencing or C++ backend key/layout compatibility.
The official mock implements all published Low-Level functions, including existence query, but
does not simulate real I/O, shared memory, Redis, timeouts, failover, disk failure, capacity,
watermarks or performance. Mock tests prove ABI marshalling and adapter behavior only.

## Validation

Offline validation:

```text
cargo fmt --all -- --check
cargo test -p mooncake-store-core --offline
cargo test -p mooncake-store-client --lib --offline -- --test-threads=1
export KVCS_SDK_ROOT=/opt/kvcs-sdk/0.4.0
export KVCS_SDK_USE_MOCK=0
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo build -p mooncake-store-client --features kvcs-capi --offline
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  nof:: --offline -- --test-threads=1

# Exercise both Standard and Low-Level C ABI round trips without live services.
export KVCS_SDK_USE_MOCK=1
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/mock/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  live_ --offline -- --ignored --test-threads=1
```

Live Standard smoke requires EFC plus Redis. Live Low-Level smoke requires EFC and a configured
mountpoint. Use the same filters without the mock library in `LD_LIBRARY_PATH`:

```text
export KVCS_SDK_ROOT=/opt/kvcs-sdk/0.4.0
export KVCS_SDK_USE_MOCK=0
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
MOONCAKE_SKIP_NATIVE_BUILD=1 \
cargo test -p mooncake-store-client --features kvcs-capi --lib \
  live_ --offline -- \
  --ignored --test-threads=1
```

An SDK-linked build is not a live EFC/Redis/NVMe end-to-end result; record those outcomes
separately.

## Implementation verification checklist

Use these checks when changing the NoF implementation:

1. capabilities and reuse: confirm no NoF control plane, allocator, metadata backend, scheduler,
   GC or rebuild loop was introduced, and that absent KVCS management traits are not invoked;
2. correctness and ABI: check positional batches, partial failures, manifest visibility, checksum
   validation, key/value limits, environment parsing, C ABI layouts and the public 0.4.0 SDK/mock;
3. reproducibility: verify every package URL above, default and `kvcs-capi` builds, tests, clippy
   and formatting.
