# SSD streaming restore and fixed CPU L2 direct reads

With explicit `MOONCAKE_OFFLOAD_SSD_PUSH=true`, trusted peer setup and a
registered CPU L2 pool, public Get/range APIs can select remote direct restore.
The pool keeps its address and MR until successful unregister/close. Normal
single-Owner reads use one terminal `ReadInto` control RPC, without Resolve,
Query or network Forget. Public Store reads, including errors and exceptions,
return only after their submitted work can no longer access caller buffers.
An explicit positive RealClient batch timeout is one logical budget shared by metadata
lookup, MEMORY, SSD and every owner group. Expiry stops new work and suppresses
late success; accepted work drains before return. Zero retains the existing
default transfer timeout behavior.
DummyClient IPC rejects positive explicit budgets before submission because its
existing RPC protocol does not propagate this timeout.

For uncertain direct SSD writes, Mooncake closes the original RPC at its
deadline and uses short internal recovery RPCs until a matching terminal drain
is confirmed. FluentLLM keeps using its existing flat/nested batch APIs and
soft timeout/detach mechanism; it does not call `recover_ssd_push()` or manage
SSD buffer leases. A timeout can therefore outlast its logical budget in the
background L3 worker. A permanent transport fault without a verified remote
fence can retain the affected pages and worker indefinitely; a failed CQE,
peer restart or fixed sleep does not permit reuse. Native advanced
`SsdFixedL2Requester::Read` callers still manage explicit recovery themselves.

Busy unregister/close
returns `OBJECT_HAS_LEASE` (`-706`), so retain external buffers and the Store
until recovery and cleanup succeed. Registration allocation failures roll back
confirmed MRs; uncertain physical cleanup also returns Busy and permits retry.
For incomplete registration cleanup, retry `unregister_buffer()` explicitly;
`recover_ssd_push()` recovers read attempts, not physical registration failures.
The one-RPC count excludes master queries and TE metadata/connections. Each
Owner batch reuses one native ring and its registered buffers across objects.
The previous object's pipeline and write callbacks must drain before an
acknowledged fixed-file update selects the next source. These resources remain
scoped to the request; control connections are still created per call, and
this implementation does not claim a lifetime ring pool.

The Owner streams through bounded slots using native Linux io_uring
registered files/buffers and `READ_FIXED` CQEs, or the compatible buffered
`pread` driver. Classic TCP v2 WRITE acknowledgments and RDMA RC successful
completions provide host destination completion. Transport errors do not prove
destination drain. Device destinations and TENT direct restore are unsupported;
existing compatible paths remain available before direct admission.

Peer JSON contains setup credentials: exchange it only through a trusted channel
and keep files private. `MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY` optionally enables
automatic reciprocal discovery inside Mooncake. Point every participating
process at the same deployment-owned private directory (including a private
shared mount for different hosts), with the same service UID and mode `0700`.
Each runtime atomically publishes a `0600` record with its exact identity and
boot epoch, then removes only its own record after safe close. Discovery is
bounded to 64 records; duplicate routes, insecure files and symlinks fail
closed. A deployment must remove stale records from crashed processes before
reusing their legacy endpoint, while retaining records needed by old active
attempts. Do not publish this directory or its bearer credentials through
public TE metadata. Explicit trusted peer configuration remains supported;
without either configuration, unsupported remote direct routes fall back
before submission. Master-based discovery is not added by this option.

See the public [streaming restore design](../../../docs/source/design/store/ssd-streaming-restore.md)
for the buffer contract, fixed CPU pools, trusted setup and fallback rules.
The V1 explicit components below remain useful for regression coverage; their
per-read owned buffers and Submit/Query/Forget flow describe that older API,
not the fixed L2 normal path. No hardware throughput improvement is asserted.

`MOONCAKE_OFFLOAD_LOCAL_STREAMING_READ=true` enables a bounded restore path
when the requester and SSD owner are the same process and every destination
is writable CPU memory. The default is `false`.

The bucket backend pins the immutable object location with its existing read
guard and an owned file descriptor. A checked plan divides the requested range
into chunks. Shared bounded workers read a staging slot with the selected pipeline
driver,
then copy its fragments into the caller's final slices. A slot is reused only
after its write has drained. Objects can exceed the staging arena capacity;
the arena only needs room for the configured slots and alignment padding.

The new pipeline uses `MOONCAKE_OFFLOAD_STREAM_USE_URING` independently of
legacy `MOONCAKE_OFFLOAD_USE_URING`. Without configured fixed L2 direct
restore, remote owners use the existing
restore path. Unsupported backends and rejections before submit may fall back;
a validated drained terminal rejection with zero written bytes may also fall
back. Accepted or uncertain writes must recover without a legacy retry. Same-process
device ranges can use streaming into their existing
CPU staging buffer before the existing host-to-device copy; their direct device
transport remains unchanged. File formats and disk replica descriptors remain
compatible; fixed L2 uses a separate bounded control envelope. Existing checksum verification stays in the requester.

This option selects local streaming. Fixed L2 remote routing is enabled
separately by SSD Push, trusted peer configuration and CPU pool registration.

## Configuration

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `MOONCAKE_OFFLOAD_LOCAL_STREAMING_READ` | `false` | Enable local CPU streaming |
| `MOONCAKE_OFFLOAD_SSD_PUSH` | `false` | Enable Owner streaming and fixed CPU L2 direct routing with trusted peers |
| `MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY` | unset | Private shared directory for automatic native peer publication/discovery; no FluentLLM API changes |
| `MOONCAKE_OFFLOAD_STREAM_USE_URING` | `true` with liburing and SSD Push, otherwise `false` | Select native fixed-buffer/file pipeline; separate from legacy `MOONCAKE_OFFLOAD_USE_URING` |
| `MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES` | `1048576` | Slot capacity; 4 KiB multiple, 4 KiB–64 MiB |
| `MOONCAKE_OFFLOAD_STREAM_SLOTS` | `4` | Slots per active batch; 1–64 |
| `MOONCAKE_OFFLOAD_STREAM_READ_THREADS` | `4` | Shared read workers; 1–64 |
| `MOONCAKE_OFFLOAD_READ_CONCURRENCY` | `1` | Concurrent Owner endpoint groups per logical read, bounded to 8 |
| `MOONCAKE_OFFLOAD_READ_IO_THREADS` | `64` | Dedicated SSD RPC executor threads, 1–128 |
| `MOONCAKE_OFFLOAD_STREAM_WRITE_THREADS` | `2` | Shared CPU copy workers; 1–64 |
| `MOONCAKE_OFFLOAD_STREAM_MAX_OUTSTANDING` | `512` with SSD Push, otherwise `64` | Queued + running tasks per executor, and live source credits across streaming callers |

Outstanding capacity must be at least twice the slot count and at most 65,536.
The local buffer must accommodate `slots * (slot_bytes + 4095)` bytes. Every
batch reserves one source credit per request before opening any files; a batch
larger than the source budget is rejected before writes and RealClient falls
back to legacy. Direct runtime defaults allow 512 objects per Owner batch and
65,536 fixed pool registrations, covering FluentLLM's normal 128-page/256-KV
object batches and layer/chunk backing layouts. Old peer advertisements without
`max_batch_objects` retain their historic 64-object limit. Explicit source-credit
overrides remain effective. Payload reservations share the existing allocator with legacy
reads, so contention can also cause admission failure. The planner bounds total
chunks and fragments to 65,536 each per batch (configurable through C++).

The C++ `ReadLocalStreaming` API takes tenant-scoped storage keys and complete
object sizes. The destination sizes define a contiguous source range in slice
order. Destinations must be held exclusively through return and may not overlap
across requests or alias the staging arena. Validation failures happen before
writes. I/O failures may leave partial data; publish only on whole-batch success.
All accepted I/O is drained before any return releases source/slot ownership.
Wait timeouts never prove drain or release an accepted slot.

SSD read/total operation counters count successfully restored objects and payload
bytes; each successful nonempty batch contributes one latency observation. For this path,
latency covers source acquisition, planning, reads and CPU copies. It therefore
has a different timing scope from the legacy backend-only read observation.
`get_offload_rpc_read_count` retains its existing logical-read semantics and
must not be used to infer actual RPC elimination.

## Local CPU tests

Portable planner, state machine, POSIX source and threaded driver tests:

```bash
cmake -S mooncake-store/tests/ssd_offload -B build-ssd-core \
  -DSSD_OFFLOAD_GTEST_SOURCE=/path/to/googletest \
  -DSSD_OFFLOAD_PEER_DIRECTORY_TESTS=ON
cmake --build build-ssd-core
ctest --test-dir build-ssd-core --output-on-failure
```

Use a separate build with `-DSSD_OFFLOAD_SANITIZERS=ON` for ASan/UBSan.
There are no network downloads in the standalone build. It uses an installed
GTest package or the supplied local source directory.

For a normally configured Linux CPU Store build with unit tests enabled:

```bash
CMAKE_BUILD_PARALLEL_LEVEL=2 scripts/ci/run_ssd_streaming_tests.sh build
CMAKE_BUILD_PARALLEL_LEVEL=2 scripts/ci/run_ssd_streaming_tests.sh build --uring
```

`--uring` enables both the legacy configuration and the new native streaming
pipeline. Native coverage requires Linux/liburing and actual CQE test execution;
a portable build does not prove native I/O. Native coverage includes
bucket lease/deletion, source credits, batch preflight, physical overlap,
20 MiB restore through an 8 MiB arena, tenant-scoped client restore through a
512 KiB arena, non-tail ranges, admission-to-legacy fallback, and a host-backed
simulated device range through CPU staging. Source and worker error/cancel/drain tests
are also part of the portable suite.

## Hardware validation to run separately

Build a wheel with the target NPU/CUDA and RDMA toolchain first. Start a test
master with `--enable_offload=true --default_kv_lease_ttl=10` and disable
promotion-on-hit. The hardware script produces a unique patterned object,
waits for SSD offload, removes its MEMORY replica after the lease expires,
checks that reads use only LOCAL_DISK, and compares full/non-tail/EOF ranges
with destination canaries. It removes only its own key when the producer ends.

Local CPU streaming through a small arena:

```bash
python mooncake-wheel/tests/ssd_streaming_hardware_smoke.py \
  --role local --master MASTER:50051 --hostname LOCAL:17813
```

Local NPU fallback (requires `torch` and `torch_npu`):

```bash
python mooncake-wheel/tests/ssd_streaming_hardware_smoke.py \
  --role local --device npu:0 --protocol rdma --rdma-devices DEVICE \
  --master MASTER:50051 --hostname LOCAL:17813
```

Two-host legacy RDMA fallback: run the owner on host A; keep it alive and copy its
printed key into the requester command on B. Both use the same master, metadata
service and tenant. Nondefault `--tenant` values require a master with
`--enable_multi_tenants` and a matching quota policy; the script defaults to
`default`. The producer has an 8 MiB legacy arena by default; the
6 MiB object is intentionally small enough for remote/device fallback.

```bash
# Host A: READY prints the key; Ctrl-C also closes and removes the test key.
python mooncake-wheel/tests/ssd_streaming_hardware_smoke.py \
  --role owner --protocol rdma --rdma-devices DEVICE_A \
  --master MASTER:50051 --hostname HOST_A:17813 \
  --ssd-path /path/on/ssd --hold-seconds 600

# Host B: CPU or NPU destination; --device npu:0 is optional.
python mooncake-wheel/tests/ssd_streaming_hardware_smoke.py \
  --role requester --key KEY_PRINTED_BY_A --protocol rdma \
  --rdma-devices DEVICE_B --master MASTER:50051 --hostname HOST_B:17813
```

Repeat with `MOONCAKE_OFFLOAD_LOCAL_STREAMING_READ=false`, and with mixed
versions (old owner/new requester and new owner/old requester) to verify legacy
interoperability. NPU/RDMA execution and performance results must be recorded
separately. RNIC errors, failed QP drain and delayed DMA require separate fault validation;
the normal-path script does not inject them.

For fixed L2 direct validation, choose the same explicit `--key` for both
roles, add `--ssd-push --io-driver uring` and reciprocal `--peer-info-file`
and `--peer-file` paths. Start both roles before waiting for READY, then
exchange those files privately before reads. The script uses a 512 KiB Owner arena to
restore a 6 MiB object, reuses one registered CPU pool and checks whole/range/EOF
bytes and canaries. Publish Owner `--stats-file` through a trusted channel to
Requester `--owner-stats-file` for exact Owner
ReadInto RPC delta checks; `--expected-read-rpcs 9` on the owner asserts the
three-repetition total. With RDMA, `--exercise-rdma-read` also verifies a remote
MEMORY READ. Repeat with `--io-driver pread` for the compatible driver. These
are CPU L2 tests; an NPU server does not imply NPU tensor/DMA coverage.

To exercise automatic native bootstrap, create one private shared directory
with mode `0700` owned by the service UID on both hosts, then use
`--peer-directory /private/shared/ssd-peers` instead of the reciprocal peer file
arguments. Add `--nested --read-timeout-ms 2000` to the requester to test both
FluentLLM batch layouts with an explicit shared logical budget; the expected
normal ReadInto delta becomes 12 for three repetitions. These options keep
the same Store setup, pool registration and Get APIs.

Fault validation must distinguish deadline expiry from physical completion.
Hold an accepted MEMORY READ or Owner SSD WRITE beyond the logical deadline,
then let it drain: the Store call must return failure, and immediate reuse of
the destination must not be overwritten. Repeat with partial batch failure,
lost terminal RPC, failed CQE, TENT failover/staging and NPU provider errors.
If no remote destination fence can be established, an SSD read may remain in
the background worker; do not free the borrowed pool or report bounded hard
fault reclamation. Separately verify that FluentLLM's detached inference
continues while `IODone` and host-page release wait for physical completion.

## Destination leases and V1 protocol contracts

The V1 components include `DestinationRegistry`, checked protocol types and explicit
YLT wire reflection. These V1 contracts underpin the runtime components. Fixed L2 direct mode
additionally imports CPU `register_buffer()` pools as writable destinations
when SSD Push is explicitly enabled; Push-disabled permissions are unchanged.

A registration binds a requester boot identity, region ID, generation and write
credential to a logical range inside its transport MR. The authorized adapter
must issue the credential securely and pin the registration or parent mapping.
The caller keeps its actual allocation alive until logical retirement and
transport deregistration both succeed. Padding outside the logical range,
physical aliases, overlapping attempts and global byte/fragment limits are
checked before references or credits change.

Publishing transport views exposes an attempt. A software timeout or abandoned
handle quarantines it; a matching acknowledgment must include both Requester and
Owner boot identities plus explicit physical drain. Late success releases the
quarantine but does not permit publishing a timed-out request. If all recovery
handles disappear without drain proof, the registry retains backing rather than
freeing memory that could still be written. Applications must retain a recovery
handle and drain attempts during shutdown.

Completed request summaries release their fragment arrays. Explicitly forgetting
a completed request advances a bounded owner-session replay floor. Request IDs
increase within the Requester boot epoch; unseen older requests can then be
conservatively rejected. Already tracked older attempts remain drainable. No
active or quarantined request is forgotten or expired by a timer.

V1 validators require the exact peers/version, bounded decoded metadata, valid
tenant names, source/destination ranges, unique result indices and matching
checksum metadata. The requester still verifies actual data integrity. RPC
integration must additionally cap wire body size and container allocations before
deserialization; post-decode validation does not provide that allocation limit.
Capability selection requires Push, range, scatter, drain acknowledgment and the
selected transport. A transport probe error is handled separately from an old
peer's missing capability.

To include codec round trips using existing yalantinglibs headers:

```bash
cmake -S mooncake-store/tests/ssd_offload -B build-ssd-core \
  -DSSD_OFFLOAD_RPC_CODEC_TESTS=ON \
  -DSSD_OFFLOAD_YLT_INCLUDE=/path/to/yalantinglibs/include
cmake --build build-ssd-core
ctest --test-dir build-ssd-core --output-on-failure
```

Registered-read integration tests use real POSIX files, final CPU scatter
buffers and the same production planner/driver/registry. They also check
cancellation and retirement while accepted file I/O drains. Their synchronous
CPU copy target does not validate RNIC or remote transport drain.

## Explicit runtime destination registration

`DestinationRegistration` adds a separate C++ registration API for SSD writable
destinations. Each logical region owns a distinct transport MR and a shared
allocation/parent mapping lifetime token. Credentials use OS cryptographic
entropy. `TransferEngineDestinationRegistration` publishes an explicitly
writable host MR and retains its TE. TENT's reference counted MR semantics are
rejected; classic TE works in a TENT-enabled build. Device/Dummy registration is not supported
by this initial adapter. With SSD Push disabled, existing `register_buffer()` behavior is unchanged.
Fixed L2 mode explicitly imports registered CPU pools as writable destinations.

Revoke denies new attempts immediately. Published attempts keep the MR busy
until a matching terminal drain confirmation. Transport deregistration then
runs; failure keeps backing pinned and the exact region can be retried. Partial
registration failure issues no destination handle and occupies bounded cleanup
capacity. `Close()` retries cleanup only when registration ownership is confirmed.
The initial TE backend cannot prove ownership from an ambiguous error/exception
and permanently quarantines it with `kNotDrained`, avoiding accidental removal
of an MR owned by another API. Shutdown without drain/cleanup proof
keeps the adapter/engine/backing quarantined. Retain the adapter and retry
`Close()` before releasing application-owned memory.

Transport callbacks and backing destruction run outside the adapter lock.
In-progress registration/deregistration stays pinned and returns busy on revoke.
Credentials validate grants in the SSD control protocol; they do not add TCP
authentication or replace RNIC keys. TCP's existing registration implementation
ignores the `remote_accessible` flag, so this API does not make arbitrary TE
addresses safe to accept in a future Push RPC.

For a configured Linux CPU build, native TE validation can run without building
the complete Store service:

```bash
cmake --build build --target ssd_destination_tcp_test --parallel 2
ctest --test-dir build --output-on-failure -R '^ssd_destination_tcp_test$'
```

This test uses two Linux processes, real TCP v2 WRITE acknowledgments and a real
`3 MiB + 123` byte file restored through a 64 KiB slot, with full data/canary
checks. A timed-out lease retains its MR while writes finish and a terminal
confirmation is delayed. A local Unix control socket coordinates the test; this component test does not exercise Store direct routing. Software timeout or failed scatter
completion alone still cannot release a remotely writable destination. NPU/RDMA
and failed remote-session drain recovery remain separate validation work.

## Bounded grant and Owner control RPC

`SsdRequesterGrants` prepares local-only bearer grants over the complete request
and both boot identities. Only authoritative registry views can be resolved.
Timeout before resolution denies future exposure; timeout after resolution keeps
backing pinned until a matching terminal result. Terminal recovery is immutable
and late success cannot publish a timed-out attempt.

`SsdOwnerRequestLedger` bounds canonical requests, active payload and sessions,
deduplicates submissions, and caches terminal results until explicit acknowledgment.
Queued/running/abandoned work does not imply drain. Owner monotonic budgets start
at the local RPC entry and include decode, validation and queue time. Explicit
forgetting requires all attempts in a logical request to have drained and advances
a replay floor; no timer evicts active work.

`SsdControlRpcEndpoint` uses a dedicated bounded executor with owned work copies.
It never runs blocking network/file/TE work on the RPC I/O thread. A full queue
records a zero-byte rejection because no work was accepted. A worker exception
quarantines the request and retains credits. Submit returns admission/current
state; callers must wait for terminal success and verify actual destination data.

The V1 control envelope has a 2 MiB body limit and two-pass decoding: all nested
counts, lengths and payload bounds are checked before constructing containers.
Use the dedicated capped `SsdControlRpcServer`; registering the handler on an
ordinary unbounded server does not enforce the outer body cap. The compatibility
client caps response headers before body allocation, rejects attachments and uses
already resolved numeric IPv4/IPv6 addresses. Network errors never fabricate drain.
Resolve peer DNS names during bounded setup outside the request path.

Default control limits are 128 records, 64 sessions, 16 MiB conservative retained
metadata credit and 1 GiB active payload. Grant credits reserve the registry's
4096-byte endpoint bound per view; this can constrain batches below the separate
protocol fragment limit. Worker defaults are two threads and 64 queued+running
tasks. These counters exclude allocator/transport/caller-handle overhead.

```bash
cmake --build build --target ssd_control_rpc_test --parallel 1
ctest --test-dir build --output-on-failure -R '^ssd_control_rpc_test$'
```

Native tests use actual coro_rpc endpoints plus classic TE TCP WRITE, a proxy that
loses a terminal reply, and full data/canary checks. They also cover giant request
and response headers, a single combined-role I/O server while its worker is
blocked, queue rejection and exceptions. These V1 component tests are separate from the fixed L2 Store
Get/bootstrap integration and its terminal ReadInto/recovery protocol.

## FileStorage Owner streaming worker

`MOONCAKE_OFFLOAD_SSD_PUSH=true` enables `ReadOwnerStreaming` independently
of the local CPU copy option. Both modes share the existing bounded arena,
source credits and read/write executors. This flag also enables fixed L2 RealClient direct restore after
trusted peer setup and CPU pool registration.

`SsdOwnerReadWorker` binds each requester to a trusted bootstrap identity,
tenant and TE segment endpoint. The configured resolver captures its numeric
control route; incoming read requests cannot choose an arbitrary write route.
Resolve views must match the entire request's destination order, sizes, host
memory and endpoint, and cannot physically overlap. A per-request target maps
one-based flat ordinals to these authoritative views.

`MakeFileStorageOwnerReader` retains FileStorage through queued/running work.
The backend preflights the complete batch, opens immutable scoped sources,
reserves aligned slots from the fixed arena and streams objects larger than
the arena. Unknown keys, size/range/plan limits, unsupported checksums and
admission failures reject the batch before accepting a write. The backend has
no persisted checksum metadata and rejects expected-checksum requests rather
than reporting an unverified hash.

An Owner deadline stops refill before delivering a completion that could
schedule more work. Already accepted I/O is still drained; its successful
bytes may appear in a cancelled result but do not authorize publication.
`SourceDrainedWriteTarget` proves staging reuse independently from remote
destination drain. A terminal ledger result requires both the reader result
and target to confirm destination drain; uncertain transport errors or reader
exceptions retain quarantine and registration references. `DrainedWriteTarget`
keeps its stricter two-sided drain contract for existing CPU copy targets.

```bash
cmake --build build --target file_storage_test ssd_owner_read_worker_test --parallel 1
build/mooncake-store/tests/file_storage_test --gtest_filter='FileStorageTest.OwnerStreaming*'
ctest --test-dir build --output-on-failure -R '^ssd_owner_read_worker_test$'
```

FileStorage integrations use real requester/owner control RPC servers, real
bucket files, final CPU buffers and canaries. They check duplicate Submit,
terminal Query/Forget and backend lifetime with both a CPU copy target and
the production classic TE TCP v2 target. Fixed L2 RealClient/Python routing is covered by the direct integration tests.
Automatic peer discovery and NPU/RDMA failure drain remain separate work.

## Production classic TCP write target

`PrepareSsdTcpWriteTarget` accepts only authoritative host destination views
from the requester grant. It validates registered ranges, physical aliases,
metadata limits and all chunk fragments before submitting a transfer. Source
slots must be registered CPU staging held through return and must not alias any
destination. A loopback write overlapping the current source slot is rejected.
The initial adapter rejects TENT, SHM and mixed transport engines.

The adapter writes directly from the bounded FileStorage arena without an
additional payload copy. Scatter owns request metadata before publishing work
and waits for local physical completion. New default-false C++ request/range
fields require acknowledged TCP WRITE for this path; actual TCP dispatch rejects
legacy framing, including a runtime `MC_TCP_PROTO=1` change. Existing wire framing
and default request behavior are unchanged. Failed WRITE completion never proves
destination drain; the Owner result remains quarantined rather than freeing the
requester's registration.

Native cases restore a real `3 MiB + 123` file through 64 KiB staging across two
processes, retain an expired lease through delayed confirmation, and exercise
unregistered destination failure and source alias rejection. Generic scatter
regressions also inject exceptions after publication, partial publication, and
failure of the error-message allocation itself. These tests retain source state
through drain and never report an unsent fragment successful.

This supplies the Owner data target used by explicitly configured fixed L2
RealClient direct reads. No RDMA/NPU performance result is asserted.

## Owned CPU requester

`SsdPushRequester` retains the older explicit per-read CPU ownership API.
Fixed L2 RealClient routing instead uses `SsdFixedL2Requester` with imported
long-lived pools. Trusted local `SsdPushPeerInfo` binds an Owner UUID,
boot epoch, tenant, legacy LOCAL_DISK endpoint, numeric control address, data
endpoint and complete TCP Push capability. A read cannot supply a new route.
`MakeSsdRequesterControlCall` uses the existing bounded control RPC client.

Each read owns one distinct registered allocation for the batch. Byte and read
credits remain charged while actual backing is pinned, including uncertain
registration failures. Request/peer limits and ranges are checked before
allocation. Wire keys are tenant-local; FileStorage scopes them once.

The first version permits one logical request per Owner, with multiple objects
and ranges in that request. This prevents a Forget replay floor from overtaking
an older request still in flight. Higher per-Owner concurrency needs an explicit
acknowledgment window before changing that limit.
Use exactly one coordinator for a requester UUID/boot lifetime. Replacing it
requires a fresh epoch and trusted bootstrap because local request IDs restart
at one and must not reuse an Owner's existing replay floor.

Submit occurs once. A network error or exception marks the read uncertain; the
session retains its handle for Query recovery even when the application drops
its copy. Only a validated successful, drained result exposes a borrowed
read-only `data()` view. Deadline checks include local validation/allocation.
Late results permit cleanup without making a timed-out Get successful. Content
checksum validation remains the caller's responsibility.

Release performs Owner Forget, local Forget and MR revoke, retrying transient
cleanup failures with the same handle. An unexposed grant can be reclaimed after
local Timeout has drained it and fenced every future Resolve; an exposed grant
requires an actual Owner terminal result. Close fences Resolve and new work;
unknown accepted writes keep their registration and backing alive.

```bash
cmake --build build --target ssd_push_requester_test ssd_requester_allocation_test file_storage_test --parallel 1
ctest --test-dir build --output-on-failure -R '^(ssd_push_requester_test|ssd_requester_allocation_test)$'
build/mooncake-store/tests/file_storage_test --gtest_filter='FileStorageTest.OwnerStreamingThroughOwnedRequesterAndTcpWriteTarget'
```

The real RPC/FileStorage/TCP integration restores a `3 MiB + 123` object and a
range through 64 KiB staging into owned requester buffers, then checks zero
remaining allocation credits, registrations and Owner records after Release.
Portable tests cover reply loss, wrong identity, deadline expiry, Close during
Submit, unregister retries and isolated allocation faults with sanitizers.
Fixed L2 RealClient bootstrap/Get routing and Python recovery are tested by
`ssd_runtime_lifetime_test`, selected `pybind_client_test` cases and
`test_ssd_fixed_l2_direct.py`; this older API retains its own buffer semantics.
