# MNNVL Architecture and PD Hardening

## Scope

MNNVL (Multi-Node NVLink) lets GPUs access exported memory across nodes in an
NVLink fabric domain. Mooncake has an existing MNNVL data path; the changes
outlined here harden the native TENT implementation and reduce submission and
completion overhead. They do not establish that this branch has passed
production acceptance, nor imply that previously deployed MNNVL systems were
unusable.

This page describes the workspace-root implementation, including the F5/F2/F1
changes and the registration/routing repairs. The classic implementation,
serving-framework connectors, public Transfer Engine requests, and Python
bindings are unchanged by this work.

Implementation entry points:
- [Classic NvlinkTransport](gh-file:mooncake-transfer-engine/src/transport/nvlink_transport/nvlink_transport.cpp)
- [Native MnnvlTransport](gh-file:mooncake-transfer-engine/tent/src/transport/mnnvl/mnnvl_transport.cpp)
- [Native runtime registration and routing](gh-file:mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp)
- [MNNVL regression tests](gh-file:mooncake-transfer-engine/tent/tests/mnnvl_transport_test.cpp)

## Two Engines, Different Transport Selection

| Path | Implementation | Build/runtime selection | Memory sharing |
| --- | --- | --- | --- |
| Classic MNNVL | `NvlinkTransport`, protocol `nvlink` | `USE_MNNVL=ON`; `MC_FORCE_MNNVL=1` influences automatic discovery | CUDA VMM and FABRIC exports; classic IPC handling where applicable |
| Native TENT MNNVL | `MnnvlTransport`, type `MNNVL` | `USE_TENT=ON`, `USE_CUDA=ON`; public wrapper selects TENT with `MC_USE_TENT=1` or `MC_USE_TEV1=1`; native loader selects MNNVL with `MC_ENABLE_MNNVL` or strict mode | CUDA VMM, normally FABRIC handles |
| Same-host NVLink | Classic `IntraNodeNvlinkTransport` or native TENT `NVLinkTransport` | Separate intra-node/NVLINK selection | Same-machine CUDA IPC/P2P; not proof of cross-node MNNVL |

`MC_FORCE_MNNVL` and `MC_ENABLE_MNNVL` are not interchangeable. The new
`MC_MNNVL_*` hardening controls below apply to native TENT, not classic
`NvlinkTransport`. Installing a transport is also distinct from selecting it
for a particular request.

The classic path already caches caller-sync events and CUDA streams per thread
and device. F2 adds a different resource: reusable **completion events** for
native TENT submission groups, together with shared polling.

## End-to-End Architecture

```text
Application / SGLang PD connector
  | allocate KV memory, retain existing producer fences
  | register logical ranges; exchange segment and destination addresses
  v
Transfer Engine public wrapper
  | classic engine OR native TENT (selected at construction)
  v
Native TENT runtime
  | local SegmentTracker + immutable segment snapshots
  | remote segment metadata cache + transport policy/capability selection
  v
MnnvlTransport
  | local endpoint validation
  | remote FABRIC import + virtual-address relocation/cache
  | caller-stream dependency -> internal CUDA copy stream
  | asynchronous READ/WRITE -> per-submission completion event
  v
CUDA VMM / IMEX / NVLink fabric
  | GPU accesses the peer's exported allocation
  v
Completion polling -> application receiver-ready protocol -> decode
```

### Allocation and Export

1. A shareable GPU allocation is created through CUDA VMM: `cuMemCreate`,
   `cuMemAddressReserve`, `cuMemMap`, and `cuMemSetAccess`. Allocation size is
   rounded to CUDA granularity. Transport-owned allocations retain their full
   mapped extent in `allocate_set_`.
2. Registration records the caller's logical address and length in `BufferDesc`.
   For remotely accessible VMM memory, the transport retains the allocation
   handle, verifies its properties, and exports a FABRIC handle into
   `mnnvl_handle`.
3. Segment metadata advertises logical ranges, memory locations, supported
   transports, and the handle. With P2P metadata, peers exchange descriptors
   through the control-plane handshake; other metadata backends are also
   supported. This traffic is separate from the NVLink payload path.
4. Registration with `kLocalReadWrite` is intentionally local-only: it does not
   create a remote FABRIC export. Ordinary host memory can be CUDA-pinned as a
   local endpoint; pinning alone does not make it remotely FABRIC-addressable.

For EGM (Extended GPU Memory), host DRAM is allocated as CUDA VMM `HOST_NUMA`
memory. Enabling `transports/mnnvl/egm` adds host-memory transfer capabilities.
An ordinary `malloc` buffer is not equivalent to an EGM allocation. Native
TENT EGM and the classic Mooncake Store EGM pool are separate configurations;
see {ref}`NVLink Transport <nvlink-transport>`.

### Import and Address Translation

The initiator finds a remote logical registration containing the requested
range, imports its allocation handle, reserves/maps the exported extent, and
grants access to local GPUs. A peer device ordinal is not used as a local GPU
ordinal: each process can have a different `CUDA_VISIBLE_DEVICES` mapping.

For a logical registration beginning inside an allocation:

```text
allocation:       [ allocation_base ......................... ]
logical range:                     [ logical_base ... length ]
                                   ^ offset
imported mapping: [ imported_base ........................... ]
request pointer = imported_base + offset + (target_offset - logical_base)
```

Both the slow import path and cached relocation apply this offset. Cache range
checks use the **logical length**, not the physical mapping extent. CUDA maps
the full allocation at mapping offset zero; the application pointer is then
adjusted, so a logical subrange need not itself satisfy CUDA mapping alignment.

Whole-allocation registrations keep the legacy raw handle. Other registrations
encode the following versioned JSON object inside the existing `mnnvl_handle`
string (the raw handle is schematic):

```json
{
  "version": 1,
  "handle": "<raw FABRIC handle>",
  "mapped_length": 8388608,
  "offset": 2097216
}
```

`BufferDesc.addr` and `BufferDesc.length` still describe the logical range. The
importer validates the envelope version/types and requires
`length <= mapped_length - offset`. POSIX-FD exports use the same envelope
around their raw `<pid>-<fd>` handle, but FD transfer is a same-host mechanism,
not a cross-node substitute for FABRIC/IMEX.

### Submission, Ordering, and Completion

For each native TENT submission, the transport validates the entire request
list before changing an existing sub-batch's tasks. `Request.source` is the
local endpoint for either opcode:

- WRITE: local endpoint -> translated target.
- READ: translated target -> local endpoint.
- `LOCAL_SEGMENT_ID`: validate the target locally and use its existing pointer;
  no remote import is needed.

A sub-batch obtains an internal CUDA stream lazily. Its first selected device
owns that stream; mixed-device requests still rely on CUDA peer access. This
work does not introduce device partitioning or a new dispatch queue.

Copies use the existing CUDA-version branches: CUDA 13.0+ and CUDA 12.8 batch
copy APIs with `cudaMemcpySrcAccessOrderStream`, or older per-copy
`cudaMemcpyAsync`. A completion event is recorded after a submission's copies.
Appending tasks creates another completion group instead of replacing the
previous event. Task indices retain their own completion-group association.

Successful submission means work was accepted, not that KV data is ready.
Completion must still be polled before the application publishes receiver
readiness or reuses buffers. Copy/record/query error paths attempt to drain
accepted work and report terminal task failure. They do not return an error
that invites blind resubmission of an already accepted copy prefix. A failed
CUDA drain is logged and cannot be treated as confirmed safe reuse.

## F5: Strict FABRIC, No Silent Downgrade

`require_fabric=true` makes native TENT fail closed for MNNVL-relevant memory
transfers: CUDA on either endpoint, an explicit MNNVL hint, or EGM-enabled
memory transfers. It is not a blanket ban on all control-plane TCP traffic or
unrelated host-only/non-memory operations.

- Bootstrap requires a CUDA build/platform and usable MNNVL installation.
  Disabled MNNVL, non-FABRIC handle selection, and forced TCP conflict with
  strict configuration.
- Relevant transport-owned allocations must succeed through CUDA VMM rather
  than silently falling back to `cudaMalloc` or ordinary host allocation.
- Remotely accessible registration requires an actual FABRIC export and an
  MNNVL transport tag; a backend's success without an export is insufficient.
- Routing requires policy authorization and MNNVL capability. A conflicting
  transport hint or policy exclusion fails instead of overriding policy.
- Relevant remote targets require an accessible export. Process-local targets
  are exempt from **remote-export** checks, but still require registration,
  policy authorization, and a valid opcode/permission. WRITE to a locally known
  `kGlobalReadOnly` target remains rejected.
- Submit-stage and poll-stage failover cannot downgrade a strict MNNVL task to
  RDMA/TCP. Failures surface to the caller.

These checks prevent unintended data-path downgrade; they do not prove that an
external allocator, serving framework, or a historical peer is correctly
configured. Verify the actual allocation, engine, and per-transfer route.

## F2: Completion Pooling and Shared Polling

- Idle completion events are pooled per device and shared across sub-batches.
  Each submission owns a noncopyable completion lease retaining pool lifetime.
- Only successfully completed, error-free recorded events are recycled.
  Pending, failed, or unrecorded events are discarded. Sub-batch teardown
  attempts to drain pending copy work before returning stream resources.
- `completion_event_pool_capacity` limits retained **idle events per device**;
  it is not admission control and does not cap all in-flight events.
- Pooling also enables shared completion queries. An increasing task sweep
  reuses its group's pending result; equal/earlier task indices query again so
  polling one task or restarting a sweep cannot freeze progress. Terminal
  completion/failure is shared by that group's tasks.
- A mutex serializes submit/poll bookkeeping within one sub-batch. Independent
  sub-batches retain independent events and may submit concurrently.

The intended benefit is fewer event create/destroy calls and repeated queries.
No throughput or latency improvement has been measured for this branch yet.

## F1: GPU-Side Caller Dependency

The transport records its existing caller-sync event on
`cudaStreamPerThread`, on the original caller's device:

```text
cpu_block: record caller event -> CPU event synchronize -> enqueue copy
 gpu_wait: record caller event -> copy stream waits on event -> enqueue copy
```

`gpu_wait` removes the host wait for this dependency; it does not remove CUDA
stream ordering or make one sub-batch's copies run in parallel. Re-recording
the cached caller event does not change previously enqueued waits.

Existing PD producer-event fencing remains mandatory. An event on the
transport caller's per-thread stream does **not** automatically capture work
on an arbitrary PyTorch/application stream. No producer-event propagation was
added to requests, connectors, or Python APIs.

`gpu_wait` requires `enable_runtime_queue=false`: deferred worker dispatch
would observe the worker's stream rather than the original caller's stream.

## Reviewed P1/P2 Repairs

| Severity | Defect | Repair and regression coverage |
| --- | --- | --- |
| P1 | Exporting an owned allocation replaced the logical registered range with the full mapping, widening metadata and breaking exact duplicate/unregister identity | Keep logical address/length; encode mapping extent/offset separately; test prefix/interior registrations, duplicate refcounts, original-range unregister, import readback, and cache bounds |
| P2 | Defensive cleanup after a failed host registration could unpin an earlier successful registration at the same address | Attach a local-only shared ownership token to successful pins; cleanup checks address, length, and owner identity; test partial batch rollback and stale snapshots against a later owner |
| P2 | Strict routing treated a process-local target as though it needed a remote export | Exempt only `LOCAL_SEGMENT_ID` from remote-export requirements; test READ/WRITE, implicit/explicit MNNVL hints, both selectors, policy exclusion, invalid target range, and no TCP submission |

The host-pin token is copied through local registry snapshots but is not
serialized into peer metadata. A failed registration gets no ownership token
and cannot remove a previously acquired pin.

## Configuration

Native TENT bootstrap defaults remain conservative:

| JSON path | Environment override | Default |
| --- | --- | --- |
| `transports/mnnvl/require_fabric` | `MC_MNNVL_REQUIRE_FABRIC` | `false` |
| `transports/mnnvl/completion_event_pool` | `MC_MNNVL_COMPLETION_EVENT_POOL` | `false` |
| `transports/mnnvl/completion_event_pool_capacity` | None added | `256` idle events/device |
| `transports/mnnvl/sync_mode` | `MC_MNNVL_SYNC_MODE` | `cpu_block` |
| `transports/mnnvl/egm` | `MC_MNNVL_EGM` | `false` |

Capacity accepts integers in `[0, 1048576]`; zero retains no idle events.
`sync_mode` accepts only `cpu_block` or `gpu_wait`. The new boolean environment
controls accept `true/false`, `1/0`, `yes/no`, and `on/off`; malformed new
controls fail configuration. Use actual JSON booleans in configuration files.
Recreate the engine to change these bootstrap settings.

MNNVL retains some legacy-named tuning keys under `transports/nvlink`, including
`handle_type` (normally `fabric`) and `host_register` (default `true`).
`MC_ENABLE_MNNVL` is presence-based in the native loader; setting it to `0`
still enables loading. `MC_FORCE_TCP` is also presence-based and conflicts with
strict mode, even when its text is `0`.

An opt-in native configuration, supplied through `MC_TENT_CONF` as an inline
JSON object or a file path:

```json
{
  "enable_runtime_queue": false,
  "transports": {
    "mnnvl": {
      "enable": true,
      "require_fabric": true,
      "completion_event_pool": true,
      "completion_event_pool_capacity": 256,
      "sync_mode": "gpu_wait",
      "egm": false
    },
    "nvlink": { "handle_type": "fabric" }
  }
}
```

Strict mode causes native MNNVL loading without needing `MC_ENABLE_MNNVL`.
For non-strict native use, set `MC_ENABLE_MNNVL=1`; the `enable` key alone is
not a substitute for the current loader's activation condition.

### Metrics

With compile-time and runtime TENT metrics enabled, observe
`tent_mnnvl_events_total{event="..."}` with events `fabric_fallback`,
`strict_failure`, `pool_hit`, `pool_miss`, and `completion_query`. JSON output
has corresponding `tent_mnnvl_<event>_total` aliases.

Compare pool hit rate, completion-query count per request, host submission
latency, CPU utilization, throughput, and PD p95/p99 latency against defaults.
A zero fallback counter alone is not proof that KV payloads took MNNVL: inspect
engine selection, exported metadata, route, and fabric/NIC traffic together.

## SGLang Prefill/Decode Integration

Direct PD continues to use SGLang's `mooncake` disaggregation transfer backend
and Transfer Engine peer handshakes. It does not require a Mooncake Store
master merely to send KV between prefill and decode workers. Keep each
framework version's receiver-buffer exchange, producer events, completion
polling, and ready notification unchanged.

For a classic MNNVL deployment, the established selection pattern is:

```bash
export SGLANG_MOONCAKE_CUSTOM_MEM_POOL=NVLINK
export MC_FORCE_MNNVL=1
```

Use a compatible `USE_MNNVL` build on both sides, with TENT selection variables
unset. Keep `MC_INTRANODE_NVLINK` and forced TCP unset. Check the installed
SGLang version's handling of `MOONCAKE_PROTOCOL` and auto-discovery rather than
assuming that every version has the same defaults.

For testing this branch's native TENT optimizations on compatible peers:

```bash
export SGLANG_MOONCAKE_CUSTOM_MEM_POOL=NVLINK
export MC_USE_TENT=1
export MC_ENABLE_MNNVL=1
export MC_MNNVL_REQUIRE_FABRIC=1
export MC_MNNVL_COMPLETION_EVENT_POOL=1
export MC_MNNVL_SYNC_MODE=gpu_wait
```

Leave `enable_runtime_queue=false`, and do not use `MC_FORCE_MNNVL` as a
substitute for native activation. These settings are a validation example,
not a production-certified recipe: the external allocator limitation below
must be checked against the actual SGLang/Mooncake versions first.

Launch prefill and decode workers using their existing model, network, and TP
settings, adding `--disaggregation-mode prefill` or
`--disaggregation-mode decode` and
`--disaggregation-transfer-backend mooncake`. Connect the existing PD router
to those endpoints. Matching model/KV layouts, sufficient memory, fabric
membership, and usable control-plane addresses remain required.

## Compatibility and Remaining Boundaries

- Both nodes need compatible CUDA/driver support, a shared usable IMEX domain,
  fabric permissions/device exposure, and exportable KV allocations. Merely
  running on an NVLink-equipped machine does not satisfy those conditions.
- Subrange envelopes require upgraded **native TENT importers**. Coordinate
  peer upgrades; do not mix the new subrange format with old/native or classic
  importers. Exact whole-allocation raw handles retain their previous format,
  but that is not a blanket classic/TENT interoperability guarantee.
- External caller-created VMM mappings are not in `allocate_set_`.
  `cuMemGetAddressRange` need not discover their extent. If authoritative
  extent discovery fails, strict registration rejects with
  `Strict MNNVL cannot determine export allocation extent`. This work does
  not add an external allocator extent API. An SGLang custom pool can expose
  this limitation; allocator fallback is not a remedy for strict FABRIC.
- Non-strict external-VMM handling retains the prior best-effort extent
  fallback. Correct arbitrary interior-subrange discovery is not guaranteed
  when the underlying allocation extent is unknown.
- Logical TE range validation is not hardware isolation from an untrusted
  peer: a CUDA FABRIC handle grants allocation-level access. Use trusted peers
  and appropriate fabric isolation.
- General remote mapping invalidation, peer restart/address reuse,
  thread-local relocation-cache lifecycle, and complete imported mapping/FD
  teardown are not redesigned. `uninstall()` still has an opened-entry cleanup
  TODO. This work must not be read as peer-restart or leak-free lifecycle
  certification.
- Keep registrations and allocations alive until outstanding transfers are
  drained. Do not assume a failed CUDA drain permits immediate buffer reuse.
- F3/F4/F6, new PD event plumbing, queue redesign, Store EGM migration, and
  classic-engine hardening are outside this change.

## Validation Before Rollout

On a Linux/CUDA host with project dependencies installed, build from the
repository root:

```bash
cmake -S . -B build-mnnvl \
  -DUSE_TENT=ON -DUSE_CUDA=ON -DBUILD_UNIT_TESTS=ON \
  -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF \
  -DTENT_METRICS_ENABLED=ON
cmake --build build-mnnvl -j
ctest --test-dir build-mnnvl --output-on-failure \
  -R 'config_lifecycle|engine_failover_e2e|local_memory_lifecycle|transport_selector|metrics_recording|mnnvl'
```

Repeat in a separate build with `TENT_METRICS_ENABLED=OFF`. Run the configured
CTest cases instead of running every MNNVL Google Test in one process: the
POSIX-FD exporter cases fork before the parent first touches CUDA and are
registered as process-isolated tests. Inspect skips: FD import needs Linux
`pidfd_getfd` permissions, and FABRIC tests need real support/IMEX.

Hardware acceptance still requires:

1. Two-node FABRIC READ/WRITE with whole allocations and interior logical
   ranges, plus byte-level KV checks. Same-host POSIX-FD readback is not a
   substitute for this test.
2. Strict negative cases: unavailable FABRIC/IMEX, unsupported/nonexportable
   allocation, absent export, conflicting policy/hint, and copy/poll errors;
   no unintended RDMA/TCP payload fallback.
3. Delayed producers on the existing PD stream/event path in both sync modes,
   concurrent batches, repeated appends, polling progress, and teardown.
4. SGLang PD correctness and sustained p95/p99 latency/CPU/throughput comparison
   with defaults, then staged capacity testing under realistic concurrency.

The repairs and regression sources are present, but this development machine
has not compiled or executed the Linux/CUDA tests or validated two-node FABRIC
and SGLang PD. Performance and production acceptance remain pending.
