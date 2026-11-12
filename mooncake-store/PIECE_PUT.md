# PiecePut: assemble one object from several producers over TCP

PiecePut lets independent C++ Clients write disjoint byte ranges of one object.
Each producer supplies the key, final object length, its offset and local slices,
and a common `write_id`. Producers do not declare the complete piece layout to
the Master. Pieces can arrive in any order.

The Master allocates the complete object once, validates each contribution, and
publishes it only after the completed pieces cover the entire object. Get,
BatchGet and Exists do not expose the incomplete object. After publication, the
object uses the existing metadata and read paths; the temporary piece state is
released. Reading individual completed pieces before publication is not supported.

## Supported configuration

This initial implementation supports one Master, the OFFSET allocator, classic
TCP, and one memory replica. HA, OpLog, CXL, SSD/DFS, NoF and RDMA PiecePut are not
supported. This version exposes the C++ `BatchPiecePut` and `RetryPiecePut`
APIs; Python bindings and canonical piece-index reads are not included. The
Master selects placement using its configured allocation strategy; callers cannot
supply per-request placement or soft-pin options. A newly assembled object has
no soft pin. Ordinary operations on other keys remain available while a PiecePut
is incomplete. Further transport and configuration coverage is follow-up work.

PiecePut is disabled by default. Enable a standalone Master with:

```bash
mooncake_master --enable_piece_put --memory_allocator=offset \
  --max_piece_put_rounds=1024 --piece_put_timeout_sec=30
```

The same three keys are accepted in the Master's JSON/YAML configuration;
explicit command-line values override file values. The standalone Master rejects
incompatible modes and invalid PiecePut limits at startup. Embedded deployments
can set `InProcMasterConfig::enable_piece_put = true`.

Each target Client must call `EnablePiecePutTarget()` before any classic TCP
memory registration, including source-buffer registration and Store mounts.
Use a fresh TCP TransferEngine for that target: an engine that has exposed legacy
memory must not be upgraded, even after that memory is unregistered. A bound
PiecePut target engine cannot subsequently be used for an ordinary Client mount.
This prevents a delayed legacy write from reaching a new protected allocation.
The same target Client may still serve ordinary Put/Get through protected mounts.
Do not expose the same target backing through a separate legacy engine or an
unprotected shared-memory alias. Protection tracks the registrations of this
engine; it is not a global registry of physical-memory aliases.
All participants must use a version that understands protected descriptors.
After a Master restart, recreate target engines and Clients rather than rebinding
the old TCP incarnation to the new Master instance.

## Client API

The following shows the input for one producer. `producer` is a connected Client;
its source memory has already been registered and remains alive through the call.
Other producers use the same key, total length and write ID with disjoint ranges.

```cpp
std::vector<PiecePutInput> inputs{
    {"kv-block", 4096, 1024, "request-42/recompute-0"}};
std::vector<std::vector<Slice>> pieces{{{source, 1024}}};
auto results = producer->BatchPiecePut(inputs, pieces);
```

A batch contains at most 64 items. Each input has a corresponding result in the
same order. An oversized batch is rejected with `INVALID_PARAMS` for every item
before any RPC or data transfer. An empty batch returns an empty result. The
length of a piece is the sum of its slices. Each result distinguishes local
operation completion from full-object publication. Finishing one piece does not
imply the object is readable yet.

Use `RetryPiecePut()` with the returned recovery handle when a result requests
metadata recovery. The handle contains no payload: accepted data transfers have
already drained before the original call returns. Keep the handle until recovery
settles; do not replace recovery with a new call that reuses the same payload.
Retry batches have the same 64-item limit. Oversized retries return per-item
`INVALID_PARAMS` and retain valid recovery handles belonging to this Client;
submit those handles in smaller batches to continue settlement. A size-validation
failure does not settle an outstanding operation.

A `write_id` identifies one logical computation, not a producer. The inference
framework must give all contributors the same ID. Recalculation with different
bytes must use a different ID, even if it reuses the same key. Separate Clients
cannot take over another Client's already-issued operation identity.

## State ownership and failure handling

The existing tenant/key object entry owns a temporary PiecePut runtime while the
object is incomplete. It contains the write ID, non-overlapping fragment records,
completion state and the hold on the selected allocation. A compact producer
admission window fences delayed Start requests without keeping one permanent
record for every completed piece. The Master maintains these structures; there
is no per-piece target-owner RPC.

The normal path is Start, strict TCP data transfer, then Finish. A strict write
requires the intended TCP incarnation and a remote acknowledgement. The local
source remains valid until accepted transfers have drained. Each strict WRITE
also carries a marker in the existing opcode, so an ordinary stale write cannot
borrow the identity of a previously verified connection. Ordinary writes to a
protected target use the same lifetime protection, so they cannot bypass it.

- A retry of the same Client/session/sequence returns the original operation;
  it does not allocate another fragment. Changed geometry or overlapping pieces
  are rejected.
- A lost Start or Finish reply is resolved using the original operation identity.
  Old tokens cannot publish a new generation or mix different write IDs.
- Expiry closes the round to new contributions and keeps it hidden. It does not
  prove an issued remote write has stopped.
- Reclaiming an expired round requires every issued write to have a definite
  outcome, or the exact target TCP endpoint to have been physically retired.
  Unknown writes remain isolated, potentially retaining the key, allocation and
  round budget. Writer crash alone is not an automatic recovery mechanism.
- `RetirePiecePutEndpoint()` drains the entire TCP endpoint before reporting
  retirement to the Master. It is terminal: create a new engine/Client to resume
  service. Removing a segment or losing heartbeat is not equivalent proof.

## Resource limits

| State | Bound |
|---|---:|
| Items per batch | 64 |
| Fragments per incomplete object | 128 |
| Write ID length | 256 bytes |
| Incomplete rounds | `max_piece_put_rounds`, default 1024 |
| Producer admission windows | 8192 |
| Operation sequences per admission window | 4096 |
| Historical protected TCP identities per Master lifetime | 65536 |

Fragment capacity is reserved before issuing any address, so allocation failure
cannot invalidate an outstanding contribution. The collection deadline defaults
to 30 seconds and must be positive and no greater than 86400 seconds. It closes
incomplete rounds; it is not a safe-free timeout for unknown writes. Admission
windows expire after inactivity, with in-flight admission protected during use.
The historical TCP identity limit is cumulative; reaching it rejects additional
protected identities rather than dropping stale-address protection.

## Validation scope

Unit and CPU/TCP integration tests cover out-of-order pieces, overlap rejection,
reply loss, stale generations, quota/resource failures, ordinary-operation
coexistence and retirement. Application tests use several real Client processes,
multiple keys and segments, concurrent single/batch I/O, writer restart and target
replacement, with complete byte comparisons. Single-host TCP tests do not prove
cross-host failure behavior, large-cluster scaling or unsupported transport paths.
