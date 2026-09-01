# NoF architecture

The current implementation adds two deliberately separate provider seams under the existing
Cold Tier subsystem. It does not add a separate NoF control plane.

## Ownership

Logical metadata, physical-storage maintenance, and physical-device management are independent.
`NofOwnership` records all three dimensions instead of treating high-level and low-level as two
indivisible ownership modes:

| Execution mode | Logical metadata | GC/watermarks/compaction | disks/health/rebuild |
| --- | --- | --- | --- |
| Standard/high-level SDK | `ProviderManaged` | `ProviderManaged` | `ProviderManaged` |
| KVCS Low-Level SDK | `ExternalMetadata` | `ProviderManaged` | `ProviderManaged` |
| Full raw low-level executor | `ExternalMetadata` | `MooncakeManaged` | `MooncakeManaged` |

`NofHighLevelBackend` is for a Standard/object SDK. The provider owns namespaces, manifests,
placement, replicas, object visibility, capacity policy, and physical maintenance. Mooncake only
plans native SDK shard ranges and forwards object operations; it does not publish a second Cold
Tier route or run disk management for this path.

`NofLowLevelTarget` is for physical KV I/O. Low-Level operations have no namespace, route,
version, owner, checksum, or replica fields, so Mooncake's existing `MetadataBackend` remains the
external logical metadata authority. The persistence service is shared, but the data model is
not: local disks use `ObjectRoute.cold_backing: ColdBackingRoute`, while NoF uses
`ObjectRoute.nof_backing: NofBackingRoute`. A route cannot contain both. This is not a second
Redis/etcd implementation and it does not encode a remote NoF target as a local `cold_tier_id`.

The metadata boundary is complete independently of a concrete data-plane executor: core types,
Redis indexing, protobuf transport, admin output and typed external-metadata operations all retain
`nof_backing` as its own field. A runtime executor integration must publish and update that field;
mapping a NoF target into persisted `cold_backing` is not a supported compatibility shortcut.

The provider-neutral low-level adapter is implemented against the existing Cold Tier backend
contract so a runtime binding can reuse:

- device/target registration, offload preparation, reservation and batch scheduling;
- route CAS and settle, replica selection, restore selection and logical load balancing through
  the NoF external-metadata seam;
- route deletion and `PendingDelete` convergence through the executor's key-level `delete`;
- checksum verification, record layout, device fencing and operational entry points.

The current public API exposes the executor contracts, `NofLowLevelTarget`, and typed external
metadata; the generic backend adapter remains internal. It does not attach a NoF target to
`StoreClientBuilder`: doing so through the existing Cold Tier target hook would publish
`cold_backing`, which is invalid for NoF. Runtime offload/restore attachment belongs behind a
binding that publishes `nof_backing` throughout its route state machine.

Each management group is optional. A Mooncake-maintained storage executor additionally supplies an
exhaustive paginated object inventory and real capacity/available-space health, allowing the
existing Cold Tier reconciliation, watermark cleanup and compaction paths to operate. A
provider-maintained storage executor does not need those methods: the provider owns physical orphan
collection, watermarks and compaction. Device management is declared separately; a
provider-managed device layer owns discovery, capacity/health reporting, offline/recovery and
rebuild. Mooncake must not emulate a missing SDK method by spawning an undocumented provider CLI.

If a backend owns logical object metadata, it uses the shared high-level metadata/object seam;
the low-level `PersistentStorageBackend` adapter is only valid with `ExternalMetadata`. This keeps
provider metadata handling in one place instead of creating a second low-level manifest database.
The three-way declaration still lets a new backend independently choose provider or Mooncake
ownership for metadata, storage maintenance, and devices.

The generic low-level adapter adds only a collision-resistant physical key codec, value chunk
records, a small root manifest, and calls to `put/get/delete/flush`. It does not define a
scheduler, device-task schema, allocator, or route state machine.

The implementation deliberately reuses the existing `PersistentStorageBackend`,
`MetadataBackend`, checksum implementation and batching path. The external
metadata interface is the stable boundary that allows an executor integration to reuse Cold Tier
orchestration without reusing `ColdBackingRoute` as the persisted NoF schema.
`ValueChunkPlan` and `PhysicalKeyCodec` are
small primitives owned by the Cold Tier layout layer and consumed where needed by the NoF
execution modes; they are not global store-core concepts. No second allocator, buffer pool,
metadata backend, GC loop or device manager is introduced.

## Module layout

NoF belongs to the existing Cold Tier subsystem. High-level and low-level are its two execution
models; a concrete SDK appears only as an executor at the leaf of one of those models:

```text
client/
  cold_tier/
    layout/
      mod.rs
      physical_key.rs
      value_chunk.rs
    nof/
      mod.rs
      external_metadata.rs
      highlevel/
        mod.rs
        executor.rs
        kvcs_executor.rs
      lowlevel/
        mod.rs
        executor.rs
        backend.rs
        kvcs_executor.rs
      kvcs_capi.rs
```

`layout/` owns the Cold Tier-only physical key and value-splitting primitives. The two
`executor.rs` files are the provider-neutral contracts that any new high-level or low-level
executor implements. Their neighboring `mod.rs` files only assemble the facade/target and module
exports. `lowlevel/backend.rs` adapts the low-level contract to the existing Cold Tier
`PersistentStorageBackend`. The two `kvcs_executor.rs` files contain only concrete KVCS SDK
behavior. `kvcs_capi.rs` contains the private shared bindings for the vendor's public C API and is
included by the parent module solely to avoid duplicating C declarations between the two KVCS
executors. It is neither a third NoF execution model nor a public SDK layer.

`ExternalMetadata` is an ownership value, not another storage service under `nof/`.
`external_metadata.rs` is a typed extension over the injected `MetadataBackend` (in-memory,
Redis or etcd): it exposes NoF get/list/CAS operations, validates that local and NoF backing are
mutually exclusive, and delegates persistence to the existing backend. It owns no connection,
cache, journal or keyspace implementation. NoF enumeration uses `NofBackingRouteFilter`; local
Cold Tier cleanup continues to use `ColdBackingRouteFilter`, so the two maintenance domains
cannot select each other's objects. Target and owner filters match the same physical primary or
replica entry, and Redis indexes every primary and replica target rather than only the route's
primary.

The additive route field is gated by `nof-backing-route-v1`. A writer cannot publish
`nof_backing` unless its route compatibility descriptor advertises that capability. Operators
must enable NoF only after every control-plane node that may decode and rewrite routes supports
the field; otherwise an older protobuf endpoint could discard the unknown field during a
read-modify-write cycle.

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

## Low-level executor construction

The implemented public API constructs a provider-neutral target without registering it as a local
Cold Tier device:

```rust,ignore
use std::sync::Arc;
use mooncake_store_client::{
    KvcsCapiLowLevelExecutor, NofLowLevelTarget,
};

let executor = Arc::new(KvcsCapiLowLevelExecutor::new()?);
let target = NofLowLevelTarget::new(executor);
```

The runtime binding must write `nof_backing`, never encode the target in persisted `cold_backing`.
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
not compilation dependencies: Standard/high-level operations require EFC plus Redis, while
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
changes.

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
```

Build and runtime library selection must agree. `KVCS_SDK_USE_MOCK=1` changes the library recorded
at link time; changing only `LD_LIBRARY_PATH` is not a supported way to replace a production build
with the mock.

Public 0.4.0 requires a non-empty Low-Level EFC socket. The executor uses
`MOONCAKE_KVCS_EFC_SOCKET` when set and otherwise passes the documented
`/var/run/kvcs/efc-grpc.sock`; it never passes a null socket and relies on an SDK-version-specific
fallback.

The adapter validates KVCS's configured raw value limit and validates the key after converting the
opaque physical key to the C ABI's hex string. The installed header currently defaults to a 256
byte raw key limit and a 4 MiB value limit. These limits remain inside the KVCS executor.

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
  kvcs_executor::tests::live_ --offline -- --ignored --test-threads=1
```

Live Standard smoke requires EFC plus Redis. Live Low-Level smoke requires EFC and a configured
mountpoint. Use the same filters without the mock library in `LD_LIBRARY_PATH`:

```text
export KVCS_SDK_ROOT=/opt/kvcs-sdk/0.4.0
export KVCS_SDK_USE_MOCK=0
export LD_LIBRARY_PATH="$KVCS_SDK_ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
MOONCAKE_SKIP_NATIVE_BUILD=1 \
cargo test -p mooncake-store-client --features kvcs-capi --lib \
  kvcs_executor::tests::live_ --offline -- \
  --ignored --test-threads=1
```

An SDK-linked build is not a live EFC/Redis/NVMe end-to-end result; record those outcomes
separately.

## Implementation verification checklist

Use these checks when changing the NoF implementation:

1. ownership and reuse: confirm no NoF control plane, allocator, metadata backend, scheduler, GC or
   rebuild loop was introduced, and that provider-managed maintenance is not invoked for KVCS;
2. correctness and ABI: check positional batches, partial failures, manifest visibility, checksum
   validation, key/value limits, environment parsing, C ABI layouts and the public 0.4.0 SDK/mock;
3. reproducibility: verify every package URL above, default and `kvcs-capi` builds, tests, clippy
   and formatting.
