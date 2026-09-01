# NoF architecture

The current implementation adds two deliberately separate provider seams under the existing
Cold Tier subsystem. It does not add a separate NoF control plane.

## Ownership

`NofHighLevelBackend` is for a Standard/object SDK. The provider owns namespaces, manifests,
placement, replicas, object visibility, and maintenance. Mooncake only plans native SDK shard
ranges and forwards object operations; it does not publish a second Cold Tier route or replica.

`NofLowLevelTarget` is for physical KV I/O. It replaces only the physical backend for a matching
Cold Tier device. The existing Cold Tier path remains authoritative for:

- device registration, admission, capacity and watermarks;
- offload preparation, reservation, batch scheduling, flush, route CAS and settle;
- replica selection, replica writes, restore target selection and load balancing;
- failed-route cleanup, GC, drain, rebuild, compaction and operational entry points.

The generic low-level adapter adds only a collision-resistant physical key codec, value chunk
records, a small root manifest, and calls to `put/get/delete/flush`. It does not define a
scheduler, device-task schema, allocator, or route state machine.

The implementation deliberately reuses the existing `PersistentStorageBackend`, Cold Tier
device cache, offload/restore workers, checksum implementation, route types, batching path and
builder lifecycle. `ValueChunkPlan` and `PhysicalKeyCodec` are small primitives owned by the Cold
Tier layout layer and consumed where needed by the NoF execution modes; they are not global
store-core concepts. No second allocator, buffer pool, GC loop or device manager is introduced.

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
      highlevel/
        mod.rs
        executor.rs
        kvcs_executor.rs
      lowlevel/
        mod.rs
        executor.rs
        backend.rs
        kvcs_executor.rs
      kvcs_ffi.rs
```

`layout/` owns the Cold Tier-only physical key and value-splitting primitives. The two
`executor.rs` files are the provider-neutral contracts that any new high-level or low-level
executor implements. Their neighboring `mod.rs` files only assemble the facade/target and module
exports. `lowlevel/backend.rs` adapts the low-level contract to the existing Cold Tier
`PersistentStorageBackend`. The two `kvcs_executor.rs` files contain only concrete KVCS SDK
behavior. `kvcs_ffi.rs` is private shared raw-ABI plumbing, included by the parent module solely
to avoid duplicating C declarations between the two KVCS executors; it is not a third NoF
execution model or a public SDK layer.

## SDK and EFC downloads

The supported public KVCS baseline is **0.3.2**. On 2026-08-31 both SDK archives below were
downloaded, their C headers were compared with the development header, and the x86_64 archive was
linked with the current implementation on the `.5` validation container. One SDK archive contains
all of the following; there are no separate C, Python, Go or Rust downloads:

- `C/include/kvcs_capi.h`, `lib/libkvcs.so` and pkg-config metadata;
- the official Rust path crate under `rust/`;
- the Python and Go packages under `python/` and `go/`;
- the official in-process C ABI mock under `mock/`.

Authoritative entry points:

- [KVCacheStore quick start](https://www.alibabacloud.com/help/en/kvcachestore/quick-start)
- [official installer](https://kvcachestore.oss-accelerate.aliyuncs.com/scripts/install-kvcs.sh)

Versioned SDK archives:

| Architecture | Download | SHA-256 snapshot verified 2026-08-31 |
| --- | --- | --- |
| x86_64 | [kvcs-sdk-0.3.2-x86_64.tar.gz](https://kvcachestore.oss-accelerate.aliyuncs.com/sdk/kvcs-sdk-0.3.2-x86_64.tar.gz) | `c14be14fb56e758ec6efa2d63417a3e6329164c61b6ba1aa7151dbae29c411b4` |
| aarch64 | [kvcs-sdk-0.3.2-aarch64.tar.gz](https://kvcachestore.oss-accelerate.aliyuncs.com/sdk/kvcs-sdk-0.3.2-aarch64.tar.gz) | `b222a07536aa007ac1083b3c720cb763725ffeee782086a7acc07a0f0f62aee3` |

The official 0.3.2 `.sha256` sidecar URLs currently return 404, so the hashes above are locally
recorded snapshots, not a vendor-signed checksum channel. Recheck the archive hash when the
artifact is refreshed. The versioned download URLs returned HTTP 200 when last verified.

Live tests also require the matching EFC service. The official installer chooses the correct
package automatically. These are the direct 0.3.2 package addresses used by that installer:

| Format | x86_64 / amd64 | aarch64 / arm64 |
| --- | --- | --- |
| RPM | [x86_64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc-0.3.2-1.x86_64.rpm) | [aarch64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc-0.3.2-1.aarch64.rpm) |
| DEB | [amd64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc_0.3.2_amd64.deb) | [arm64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-efc_0.3.2_arm64.deb) |
| tarball | [x86_64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-deploy-0.3.2-x86_64.tar.gz) | [aarch64](https://kvcachestore.oss-accelerate.aliyuncs.com/packages/kvcs-deploy-0.3.2-aarch64.tar.gz) |

The validation container also has internal development SDK `0.4.0-dev.cad6271`, commit
`cad62716d51fd0d22091f7440e97d4d5736aba47`, installed at
`/opt/kvcs-sdk-0.4.0-dev.cad6271`. Its Aone artifact names are `kvcs-sdk-x86_64` and
`kvcs-sdk-aarch64`. That development version is **not** published under the public OSS paths above;
the guessed URLs return 404. Use the public 0.3.2 downloads for a reproducible checkout, or obtain
the exact development artifacts from their authenticated Aone pipeline. Do not record expiring
signed Aone download URLs in the repository.

To fetch only the SDK without installing EFC:

```text
KVCS_VERSION=0.3.2
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

## Low-level builder wiring

A low-level executor is bound to an existing `ColdTierTargetConfig` with the same ID:

```rust,ignore
use std::sync::Arc;
use mooncake_store_client::{
    ColdTierKind, ColdTierTargetConfig, KvcsCapiLowLevelExecutor,
    NofLowLevelTarget, StoreClientBuilder,
};

let executor = Arc::new(KvcsCapiLowLevelExecutor::new()?);
let client = StoreClientBuilder::new(metadata, "runtime")
    .cold_tier_target(ColdTierTargetConfig::directory(
        "nof-0",
        ColdTierKind::Nfs,
        "/mnt/nof-0",
    ))
    .nof_low_level_target("nof-0", NofLowLevelTarget::new(executor))
    .build(expires_at_ms)?;
```

The target path is still used by the normal Cold Tier lifecycle and capacity configuration. KVCS
mountpoint selection is supplied to the concrete executor (`MOONCAKE_KVCS_MOUNTPOINT_INDEX`).

## KVCS feature

The default feature set has no KVCS library dependency. Enable the adapter explicitly:

```text
source /opt/kvcs-sdk/latest/env.sh
MOONCAKE_SKIP_NATIVE_BUILD=1 \
cargo check -p mooncake-store-client --features kvcs-capi --lib --offline
```

`KVCS_SDK_ROOT` defaults to `/opt/kvcs-sdk/latest`. Set it explicitly when validating another
extracted version. The build checks for both `C/include/kvcs_capi.h` and `lib/libkvcs.so` before
linking, so a partial or incorrectly rooted package fails early.

The adapter validates KVCS's configured raw value limit and validates the key after converting the
opaque physical key to the C ABI's hex string. The installed header currently defaults to a 256
byte raw key limit and a 4 MiB value limit. These limits remain inside the KVCS executor.

The KVCS C ABI exposes a mountpoint index but no manager epoch or device-generation fence. This
integration therefore does not claim physical fencing or C++ backend key/layout compatibility.

## Validation

Offline validation:

```text
cargo fmt --all -- --check
cargo test -p mooncake-store-core --offline
cargo test -p mooncake-store-client --lib --offline -- --test-threads=1
source /opt/kvcs-sdk/latest/env.sh
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo check -p mooncake-store-client --features kvcs-capi --lib --offline
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  nof:: --offline -- --test-threads=1

# Exercise both Standard and Low-Level C ABI round trips without live services.
KVCS_SDK_USE_MOCK=1 \
LD_LIBRARY_PATH="$KVCS_SDK_ROOT/mock/lib:$LD_LIBRARY_PATH" \
MOONCAKE_SKIP_NATIVE_BUILD=1 \
  cargo test -p mooncake-store-client --features kvcs-capi --lib \
  kvcs_executor::tests::live_ --offline -- --ignored --test-threads=1
```

Live Standard smoke requires EFC plus Redis. Live Low-Level smoke requires EFC and a configured
mountpoint. Use the same filters without the mock library in `LD_LIBRARY_PATH`:

```text
source /opt/kvcs-sdk/latest/env.sh
MOONCAKE_SKIP_NATIVE_BUILD=1 \
cargo test -p mooncake-store-client --features kvcs-capi --lib \
  kvcs_executor::tests::live_ --offline -- \
  --ignored --test-threads=1
```

An SDK-linked build is not a live EFC/Redis/NVMe end-to-end result; record those outcomes
separately.

## Implementation verification checklist

Use these checks when changing the NoF implementation:

1. ownership and reuse: confirm no NoF control plane, allocator, scheduler, GC or rebuild loop was
   introduced, and that the low-level target is only a `PersistentStorageBackend` replacement;
2. correctness and ABI: check positional batches, partial failures, manifest visibility, checksum
   validation, key/value limits, environment parsing, FFI layouts and both 0.3.2/development SDK
   linking;
3. reproducibility: verify every package URL above, default and `kvcs-capi` builds, tests, clippy
   and formatting.
