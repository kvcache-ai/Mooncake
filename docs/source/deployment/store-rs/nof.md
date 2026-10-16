# Store-RS NoF Operations

This page covers NoF build prerequisites, runtime configuration, and multi-node
validation. For design and request-path behavior, see the
[NoF design](../../design/store/store-rs/nof.md).

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
      executor.rs
      nof_spdk_shim.c
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
export MOONCAKE_SPDK_PREFIX=<spdk-prefix>
export PKG_CONFIG_PATH="$MOONCAKE_SPDK_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
cargo build -p mooncake-store-client --features nof-spdk --offline
```

Use the actual directory containing `spdk_nvme.pc` if the installation places pkg-config files
under `install/lib/pkgconfig`, `build/lib/pkgconfig`, or `build/lib64/pkgconfig`. Without
`nof-spdk`, `mooncake-store-client` does not compile or link `nof_spdk_shim.c`. The transport shim
is the only C layer; allocation, record codec, checksum, buffer pooling, replica policy, and route
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

```rust
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

Register a managed ExtentStore target by wrapping an exclusive region of an SPDK NVMe-oF
namespace:

```rust
let executor = Arc::new(ExtentStoreExecutor::new(
    block_device,
    ExtentStoreExecutorConfig::new(0, 16 * 1024 * 1024 * 1024),
)?);
let target = NofTargetConfig::new("nof-disk-0", NofBackend::new(executor)?)?;
let client = StoreClientBuilder::new(metadata, "storage-0")
    .nof_target(target)
    .nof_replica_count(1)
    .build(expires_at_ms)?;
```

`KvcsCapiExecutor::new()` is the environment-variable convenience constructor for a single target.
`with_low_level_config` remains a single-target convenience API. Standard mode uses
`with_standard_config`; the provider owns its internal placement, so it is registered as one
logical target.

The target list is immutable for the lifetime of a `StoreClient`. Temporary target availability is
dynamic: the existing owner heartbeat excludes a target after three consecutive failures. One
successful probe restores a short-circuited target; after long-failure route downline, manifest
recovery must also succeed before the target is admitted. Permanent additions or removals require a
coordinated configuration rollout and client restart. A new target must not accept writes until
every client has registered it, and a removed target must be drained by the provider before it
disappears from the inventory. Mooncake does not parse private EFC
configuration, invent target IDs, or implement an SDK-independent discovery protocol.

A target ID, its provider endpoint, mountpoint, mode, and maximum value size are immutable
configuration. The ownership fingerprint identifies target IDs, data-plane shape, and replica
count; it cannot verify that two processes mapped a target ID to the same provider endpoint or mountpoint.
Deployment configuration must enforce that mapping.

Standard sharded writes return an SDK error when any shard fails, after which Mooncake may retry the
object write. Mooncake does not maintain a second shard manifest or independently clean up a
partially accepted write. Correct retry and incomplete-manifest behavior therefore depends on the
KVCS Standard API's idempotency and manifest contract.

## Four-node managed NoF validation

The SPDK shim is compiled with the Rust/C bridge and links the external SPDK/DPDK libraries
dynamically. Build on a system whose libc is compatible with every initiator. Every initiator must
already have an ABI-compatible RDMA-enabled SPDK runtime at `NOF_SPDK_LIB_DIR`; the runner keeps
that directory in `LD_LIBRARY_PATH` and rejects unresolved symbols before starting a client.

The reproducible commands are:

```shell
# Run from the Mooncake Store-RS repository root on the build host.
export MOONCAKE_SPDK_PREFIX=<spdk-prefix>
# The reserved initiator and NoF hosts are CPU-only.
export MOONCAKE_ENABLE_CUDA=0
mooncake-store-rs/scripts/e2e/build-nof-multi-client.sh
# Optional: run the SPDK-backed NoF unit-test subset with the same environment.
NOF_RUN_UNIT_TESTS=1 mooncake-store-rs/scripts/e2e/build-nof-multi-client.sh
```

The multi-client build defaults `MOONCAKE_ENABLE_CUDA` to `0` because the
reserved validation hosts have no CUDA device. Override it explicitly to `1`
only when running the binary on a GPU-capable initiator. This avoids compiling
CUDA pointer/copy paths that fail during CPU-only validation even when CUDA headers are present on
the build host.

`build-nof-multi-client.sh` validates `spdk_nvme.pc`, `spdk_env_dpdk.pc`,
`spdk_syslibs.pc`, the shared SPDK libraries, Rust formatting, and the final binary's dynamic
library resolution. It also requires a clean tree and verifies that `HEAD` did not change during
the build; the emitted build manifest binds that commit to the binary SHA256. The test artifact is
`target/debug/nof_multi_client` (or the release path when `NOF_BUILD_PROFILE=release`). The bastion
runner passes the same profile and expected commit through to staging; set `NOF_RUN_UNIT_TESTS=1`
to run the NoF unit subset before any target reset.

RDMA targets require an RDMA-capable NIC on both endpoints and an SPDK build configured with
`--with-rdma`; `NOF_SPDK_LIB_DIR` must select that RDMA-enabled runtime on each initiator.

`run-nof-multi-client.sh` repeats `ldd -r` on the actual initiator after adding its SPDK runtime
directory, and writes hostname, kernel, interface inventory, binary hash, and runtime-library
status to `run-manifest.txt`. A run is not started when a shared library or symbol is unresolved.

The multi-node command runs on the bastion. It builds on `NOF_BUILD_HOST`, optionally resets the
provisioned NoF image on every configured target, stages the exact binary and runner with `rsync`,
verifies the build commit and binary SHA256 on each initiator, and runs the configured client
subset on each initiator. Initiators do not need to SSH to target public addresses, so target
cleanup, when enabled, is deliberately performed from the bastion:

```shell
# On the bastion, from a checked-out Mooncake Store-RS repository.
NOF_BUILD_HOST='<build-ssh-host>' \
NOF_BUILD_ROOT='<repo-path-on-build-host>' \
MOONCAKE_SPDK_PREFIX='<spdk-prefix-on-build-host-and-initiators>' \
NOF_SPDK_LIB_DIR='<spdk-shared-library-directory-on-initiators>' \
NOF_TARGETS='<target-ssh-host-a>|<target-traddr-a>|<target-id-a>|<target-subnqn-a>|<target-port-a>|tcp,<target-ssh-host-b>|<target-traddr-b>|<target-id-b>|<target-subnqn-b>|<target-port-b>|rdma' \
NOF_CLIENT_IDS='<client-id-a>,<client-id-b>' \
NOF_INITIATORS='<initiator-ssh-host-a>|<initiator-bind-ip-a>|<client-id-a>;<initiator-ssh-host-b>|<initiator-bind-ip-b>|<client-id-b>' \
NOF_REDIS_URL='redis://<redis-host>:<redis-port>/<redis-db>' \
NOF_REPLICA_COUNT='<replica-count>' \
mooncake-store-rs/scripts/e2e/run-nof-multi-client-from-bastion.sh
```

The multi-node script intentionally has no target, initiator, Redis, build-host, or client-list
defaults. A real reservation must pass the topology explicitly:

- `NOF_TARGETS` is a comma-separated target list. Each entry is
  `public_host|traddr|target_id|subnqn|port[|transport]`. `transport` accepts `tcp` or `rdma` and
  defaults to `tcp` when omitted. Both transports use the same SPDK block-device and ExtentStore
  implementation. The number of NoF targets is the number of entries.
- `NOF_CLIENT_IDS` is the global client list used by the test binary for barriers and read
  verification.
- `NOF_INITIATORS` is a semicolon-separated initiator list. Each entry is
  `ssh_host|bind_ip|local_client_ids`; the number of initiator machines is the number of entries.
  `local_client_ids` is the comma-separated subset started on that initiator.
- `MOONCAKE_SPDK_PREFIX` is required on the build host. `NOF_SPDK_LIB_DIR` is required explicitly
  and must name the ABI-compatible SPDK shared-library directory present on every initiator.
- `NOF_BARRIER_REDIS_URL` controls the cross-initiator barrier. It defaults to `NOF_REDIS_URL`.
- `NOF_HOST_NQN` is required by the per-initiator runner. The bastion runner derives one from the
  initiator client list when it is omitted from an initiator entry.
- `NOF_RESET_TARGETS=1` enables target image reset from the bastion. When it is enabled,
  `NOF_NVMF_SERVICE` and `NOF_TARGET_IMAGE` must also be set explicitly. This convenience mode
  supports at most one configured target per SSH host; provision targets separately when one host
  exports multiple images or uses different services.

Every reset stops `NOF_NVMF_SERVICE`, removes and recreates `NOF_TARGET_IMAGE` at
`NOF_TEST_IMAGE_BYTES`, and restarts the service; it never touches the system NVMe disk unless the
operator explicitly points `NOF_TARGET_IMAGE` there. Run logs and the manifest are left under
`NOF_INITIATOR_DIR/logs/<run-tag>/` on each initiator.

For a manually staged run, use `mooncake-store-rs/scripts/e2e/run-nof-multi-client.sh` with
`NOF_RESET_TARGETS=0`, `NOF_SKIP_TARGET_SSH_CHECK=1`, the local `NOF_BIND_IP`, and the same
Redis/SPDK settings after performing the reset from the bastion. `NOF_LOCAL_CLIENT_IDS` can be
used to start only the client subset that belongs on the current machine while keeping
`NOF_CLIENT_IDS` as the global client set. Do not use `scp`.

The runner sets `MC_STORE_RS_ENABLE_COLD_TIER=1`, creates per-run registration, write/offload,
and read-start barriers, and starts the client IDs from `NOF_CLIENT_IDS` with
`NOF_REPLICA_COUNT`. Each client writes a disjoint object set and calls the synchronous
`debug_evict_all` API. A client joins the offload barrier only after eviction reports completion,
zero remaining hot replicas, and no replica dropped without cold backing. After every configured
client reaches that barrier, all clients issue `batch_get_into` reads concurrently and verify the
complete object set. Read-only recovery runs wait separately for recovered target manifests because
there are no hot replicas for `debug_evict_all` to process. The test binary uses
`NoopTransport` for the local hot segment, so it does not claim to validate remote hot-memory
transfers; the cross-client assertion is the managed NoF read path. A passing run must contain the
configured write/offload and read verification counts in each client log. The target list is
configured by `NOF_TARGETS`, client IDs by `NOF_CLIENT_IDS`, local client subset by
`NOF_LOCAL_CLIENT_IDS`, initiator machines by `NOF_INITIATORS`, and the image size by
`NOF_TEST_IMAGE_BYTES`; the reset operation removes and recreates only `NOF_TARGET_IMAGE`.

The same runner exposes focused lifecycle checks without embedding a topology:

- `NOF_EXPECT_NOF_COPIES` verifies the number of materialized target copies per route and requires
  those copies to use distinct target IDs.
- `NOF_EXPECT_MAX_TARGET_COPY_SKEW` verifies that total materialized copies are distributed across
  configured targets within the supplied maximum count difference.
- `NOF_WATERMARK_HIGH_BYTES` and `NOF_WATERMARK_LOW_BYTES`, combined with
  `NOF_POST_OFFLOAD_WAIT_SECONDS`, verify owner-driven high-to-low watermark cleanup.
- `NOF_EXPECT_POST_WAIT_MISSING_ROUTES=true` verifies that watermark reclaim removes logical routes
  when it evicts their final payload copy.

Watermark validation is intentionally layered. Existing Cold Tier CLOCK unit tests verify hot/cold
victim ordering, the managed-NoF tracker unit test verifies that only locally owned target copies
enter that same CLOCK, and the multi-machine test verifies that selected records are physically
released until the low watermark and are not resurrected on restart. The multi-machine test does
not implement a second LRU oracle.
- `NOF_DELETE_AND_REWRITE=true` deletes the first object set and writes a second set, allowing a
  deliberately small `NOF_DEVICE_BYTES` to verify extent release and reuse; the test also requires
  an old and rewritten route to share a target/locator pair.
- `NOF_HANDOFF_DEPARTING_CLIENT=<client-id>` makes one of exactly two clients exit normally after
  offload; the survivor then deletes, rewrites, offloads, and reads the complete object set.
- `NOF_HANDOFF_ABRUPT_EXIT=true` kills that departing client with `SIGKILL`, without drain; the
  runner accepts exit status 137 only for that client. The survivor continues renewing its own
  lease while it waits for the departed owner's lease to expire, then exercises takeover. Set
  `NOF_LEASE_TTL_MS` to a short test lease and set
  `NOF_HANDOFF_WAIT_SECONDS` greater than the lease plus two seconds. The per-client timeout must
  also exceed the handoff wait.
- `NOF_READ_ONLY=true` skips writes and validates routes rebuilt from existing target manifests.
- `NOF_EXPECT_MISSING_ROUTES=true` combines with read-only mode to verify that tombstoned/reclaimed
  records are not resurrected after a fresh startup scan, then writes and reads a probe object to
  prove that the recovered target is usable.
- `NOF_EXPECT_ABSENT_TARGETS` verifies that long-failed target copies have been removed after an
  externally orchestrated target outage.
- `NOF_EXIT_AFTER_POST_WAIT=true` ends a successful fault-downline stage before the final read. It
  is intended for a following read-only invocation, with the same keyspace and a new barrier run
  ID, that restarts the target and verifies manifest recovery from its original records.
- `NOF_REONLINE_WAIT_SECONDS` adds a second live-client window after downline. Restart the failed
  target after all clients reach the `reonline-ready` barrier; the same client processes then
  require every pre-fault target/key association and replica count to recover before reading.
- `NOF_BARRIER_RUN_ID` namespaces synchronization keys independently of `NOF_KEYSPACE`; use a new
  value for every invocation, including restart checks that intentionally reuse the same objects.

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
