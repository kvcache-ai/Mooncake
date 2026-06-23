# RDMA Connection Reliability Specification

## 1. Overview

This document describes the target reliability design for RDMA connection
re-establishment in Mooncake. The target scenario is a production cluster where
a peer may restart, a retired endpoint may still have in-flight work requests,
both sides may initiate handshakes concurrently, and delayed RPC responses or
CQEs may still arrive after the local state has moved on.

The primary goal is:

**During peer restart and re-handshake, the cluster must converge to a stable
RDMA connection without reusing a retired connected endpoint, losing completion
ownership, or corrupting the state of an already converged endpoint.**

The central design choice is therefore:

**A connected `RdmaEndPoint` has a unidirectional lifecycle. Once it disconnects
or detects that the peer incarnation changed, it is retired and eventually
destroyed. It is not reconstructed in place. A subsequent connection uses a
replacement endpoint.**

Every peer incarnation is represented by a distinct endpoint lifecycle. This
keeps QP state, peer QP numbers, CQEs, pending completions, and data-path
submissions within one well-defined ownership boundary.

## 2. Failure Model

The design assumes the following failures can happen in normal operation:

1. A peer process restarts and creates different QPs for the same
   `peer_nic_path`.
2. A connection is destroyed while WRs are still in flight.
3. QP transition to `ERR` flushes WRs to CQ after the endpoint has already been
   removed from the active endpoint map.
4. `ibv_modify_qp(QP, ERR)` may fail, so some WRs may never receive hardware
   completions.
5. Both peers may initiate active handshakes at the same time.
6. A delayed active handshake response may return after a later local handshake
   attempt has started.
7. A stale or replayed handshake request may arrive after the endpoint already
   converged.

The design does not try to solve process crash after memory is gone, kernel
crash, or malicious/forged handshakes. Those require different mechanisms.

## 3. Design Principles

### 3.1 Endpoint Incarnations Are One-shot

Once an endpoint has reached `CONNECTED`, it owns RDMA QPs, peer QP numbers,
in-flight WR counters, CQ outstanding counters, and pending completion
ownership. This ownership remains attached to that endpoint until all accepted
WRs are drained or manually failed.

The target lifecycle separates a retired endpoint from its replacement:

```text
retired endpoint:     CONNECTED -> DESTROYING -> DESTROYED
replacement endpoint: UNCONNECTED -> CONNECTING -> CONNECTED
```

Why this matters:

- CQEs for a retired connection are handled within the retired endpoint.
- Submissions for a replacement connection use only the replacement endpoint.
- Peer QP mismatch has a single meaning: retire the current endpoint and retry.
- Endpoint fields do not need to represent multiple peer incarnations.

`CONNECTING` is the only state that may be reset to `UNCONNECTED`, because the
endpoint has not yet become part of the data path. This exception is needed for
handshake-local retry, for example auto-GID reprobe and simultaneous-open
arbitration.

### 3.2 Keep CQ Ownership Centralized

CQ polling remains owned by `WorkerPool::performPollCq()`. `finishDestroy()`
does not actively poll CQ. This avoids splitting CQ ownership across worker
threads and reclaim code.

The consequence is that destruction is two-stage:

1. `beginDestroy()` marks the endpoint inactive, moves it to `DESTROYING`, and
   attempts to transition QPs to `ERR` so hardware can flush WRs.
2. `finishDestroy()` waits for the worker to drain completions. If this cannot
   happen within the timeout, it manually fails the remaining claimed pending
   slices.

This design keeps CQ handling centralized and provides a bounded fallback for
the cases where hardware will not produce completions.

### 3.3 Completion Must Have a Single Owner

Every successfully accepted WR must eventually produce exactly one terminal
slice status: success or failure. The hard part is not generating failures; the
hard part is avoiding two different paths completing the same slice.

The endpoint therefore keeps a pending set of posted slices. Completion paths
must claim ownership before touching counters or marking status:

- Normal CQ completion claims from the pending set in batch.
- Manual destroy fallback claims all remaining pending slices.
- A path that cannot claim the slice must not update the slice or counters.

Batching is important. The pending set is not intended to be a per-CQE
scheduler. It is an ownership guard. `performPollCq()` groups completions by
endpoint and calls `claimPendingSlices()` once per endpoint group, so lock
traffic scales with endpoint fanout in the CQ batch rather than raw CQE count.

### 3.4 Use Handshake Fields as Guards, Not as the Source of Truth

The handshake reliability fields are defensive guards:

- `handshake_version` rejects stale active responses for the same local
  endpoint.
- `handshake_timestamp` rejects obviously stale or impossible messages.
- `handshake_flags` lets a passive side explicitly signal peer restart
  detection.

They are not a distributed consensus protocol. In particular, remote wall-clock
timestamps are not used as strict cross-host ordering. Clock skew is expected,
so timestamp validation is a sanity check only. The strongest convergence rule
remains endpoint replacement on peer QP mismatch.

## 4. Endpoint Lifecycle

### 4.1 State Machine

```text
INITIALIZING
    |
    v
UNCONNECTED <---------+
    |                 |
    v                 |
CONNECTING -----------+
    |
    v
CONNECTED
    |
    v
DESTROYING
    |
    v
DESTROYED
```

The back edge from `CONNECTING` to `UNCONNECTED` is only for unestablished
handshake retry. There is no back edge from `CONNECTED`.

### 4.2 State Semantics

| State | Meaning | Allowed Reliability Behavior |
| --- | --- | --- |
| `INITIALIZING` | QP resources are not fully constructed | No handshake or data path |
| `UNCONNECTED` | QPs exist but no peer connection is established | Active/passive handshake may start |
| `CONNECTING` | Handshake or QP state transition is in progress | May reset to `UNCONNECTED` if not established |
| `CONNECTED` | Endpoint is on the data path | May submit WRs; errors retire the endpoint |
| `DESTROYING` | Endpoint removed from active use, waiting for drain | Reject submissions; CQ drain or manual fallback |
| `DESTROYED` | QPs/resources destroyed | No use |

### 4.3 Why Connected Endpoints Are Not Reused

The lifecycle rule forbids `CONNECTED -> UNCONNECTED -> CONNECTING` because a
connected endpoint is already part of the data path. A retired connected
endpoint must satisfy these ownership constraints:

- no worker can still poll a CQE and process it without claiming ownership;
- no completed slice can keep a live back-pointer to a reclaimed endpoint;
- all QP depth and CQ outstanding counters were repaired;
- no concurrent submitter observed the endpoint as connected;
- no delayed active RPC response can mutate a converged endpoint.

The one-shot lifecycle makes the proof local: a retired endpoint only drains or
fails its own pending WRs, while a replacement endpoint starts with its own QP
state.

## 5. Handshake Protocol

### 5.1 Message Fields

The existing handshake descriptor carries NIC path, GID/LID, and peer QP
numbers. The reliability design adds:

```cpp
uint64_t handshake_version;
uint64_t timestamp;
uint32_t flags;
```

Wire encoding uses:

- `handshake_version`
- `handshake_timestamp`
- `handshake_flags`

Missing fields decode to zero so that descriptors without these fields remain
valid inputs.

### 5.2 Active Handshake

The active side increments a local version before sending the RPC:

```text
beginHandshakeAttempt()
fill local_desc(version, timestamp, flags)
sendHandshake()
validate response
doSetupConnection()
CONNECTED
```

Validation is intentionally ordered:

1. If the active RPC failed but a concurrent passive handshake already connected
   the endpoint, keep the converged connection and return success.
2. Otherwise, reject stale active responses whose echoed version no longer
   matches the endpoint version.
3. Reject responses that exceed the local handshake timeout.
4. Reject timestamps outside the sanity window.
5. Reject responses carrying `FLAG_PEER_RESTART_DETECTED`.
6. If the endpoint became connected during the RPC, reuse it only when peer QP
   numbers match.
7. If the endpoint became connected with different peer QPs, retire it and let
   the caller retry with a replacement endpoint.

Why this works:

- Version protects against delayed responses for earlier local attempts.
- QP comparison protects against peer incarnation changes.
- Already-converged same-QP connections are treated as idempotent success.

### 5.3 Passive Handshake

The passive side first fills a response descriptor and echoes the incoming
`handshake_version`.

If the endpoint is already connected:

- Same peer QP list means duplicate handshake; return success.
- Different peer QP list means peer restart or peer incarnation change. The
  endpoint enters `DESTROYING`, response sets
  `FLAG_PEER_RESTART_DETECTED`, and the transport layer retries with a
  replacement endpoint.

If the endpoint is `CONNECTING`, simultaneous-open arbitration is applied:

```text
if local_nic_path < peer_local_nic_path:
    keep local active attempt and reject passive request
else:
    reset unestablished local attempt and accept passive request
```

The ordering is deterministic and uses values both sides know, so two peers
cannot both decide to accept or both decide to back off forever.

### 5.4 Timestamp Semantics

Timestamp validation is deliberately weak:

- `0` is accepted as an absent timestamp.
- A timestamp too far in the future is rejected.
- A timestamp outside the handshake timeout window is rejected.
- A remote timestamp is not compared against the local request timestamp to
  decide global ordering.

Timestamp is not used as strict ordering because:

- hosts can have clock skew;
- network delay can reorder observations;
- peer restart detection is already stronger when based on QP number changes.

Timestamp exists to filter obviously stale or replayed messages, not to define
global ordering.

## 6. Completion Guarantee

### 6.1 Problem

RDMA WR completion has two possible sources after destruction begins:

1. hardware flushes WRs to CQ after QP enters `ERR`;
2. software manually fails pending slices if hardware cannot or does not flush.

Without ownership tracking, both paths can complete the same slice. Or worse,
neither path completes it because the endpoint is removed before CQ drain
finishes.

### 6.2 Pending Ownership Set

The endpoint tracks posted slices in a pending set:

```text
submitPostSend:
    prepare WRs
    add slices to pending set
    increment QP/CQ counters
    ibv_post_send
    remove failed post_send tail from pending set

performPollCq:
    group CQEs by endpoint
    claim pending slices in batch
    only claimed CQEs update counters and status

finishDestroy:
    wait for worker-driven CQ drain
    on timeout or QP-to-ERR failure, claim all remaining pending slices
    mark claimed slices failed
```

Manual completion also clears the slice's endpoint and QP-depth back-pointers.
This protects against late CQEs after the endpoint has been reclaimed: the
worker sees no endpoint owner and skips the CQE.

### 6.3 Counter Invariant

The design does not require `pending_slices_.size()` to equal
`cq_outstanding_` at every instruction boundary, because counters and pending
set are updated around `ibv_post_send()` and CQ polling in different code
paths. The required invariant is weaker and more useful:

**For every WR accepted into the pending set, exactly one owner will claim it
and perform the matching `wr_depth_list_` and `cq_outstanding_` decrement.**

This is the invariant that prevents double completion, leaked completion, and
counter underflow.

## 7. Peer Restart Convergence

A peer restart normally appears as the same `peer_nic_path` with a different
QP list. The local side handles that as a distinct peer incarnation.

Passive path:

```text
connected endpoint receives handshake
peer_qp_list differs
    -> mark current endpoint DESTROYING
    -> set FLAG_PEER_RESTART_DETECTED
    -> return ERR_REJECT_HANDSHAKE
transport deletes current endpoint from active store
transport creates replacement endpoint
transport retries passive setup
```

Active/data path:

```text
post or CQ error marks path failure
endpoint is deleted from active store
retired endpoint drains in waiting list
slice retry obtains a replacement endpoint
active handshake establishes fresh QPs
```

Why this improves cluster stability:

- The retired endpoint is not asked to represent multiple peer incarnations.
- In-flight work on the retired endpoint is failed or flushed independently.
- Subsequent traffic routes through a clean replacement endpoint.

## 8. Concurrency Considerations

### 8.1 Concurrent Active Setup

Only one active setup attempt may transition `UNCONNECTED -> CONNECTING`.
Other callers wait for the in-progress attempt. If waiting times out, the
unestablished endpoint is reset so later attempts can retry.

### 8.2 Active and Passive Race

An active RPC is sent without holding the endpoint lock. During that RPC, the
peer may initiate its own handshake and the local passive path may connect the
same endpoint. When the active side resumes:

- if the endpoint is already connected with the same peer QPs, it returns
  success;
- if the endpoint is already connected with different peer QPs, it retires the
  endpoint;
- if the RPC failed but passive setup already connected the endpoint, the
  connected state wins.

This gives priority to an already converged connection and avoids destroying a
valid endpoint only because a redundant active RPC failed.

### 8.3 Destroy and CQ Race

`beginDestroy()` prevents further submissions by marking the endpoint inactive and
moving it to `DESTROYING` under the endpoint lock. Existing WRs are then drained
by the CQ worker or manually failed later.

The pending ownership set protects the race where manual fallback and CQ
polling observe the same WR. Clearing slice back-pointers protects the later
race where a CQE arrives after manual fallback and endpoint reclaim.

## 9. Tradeoffs and Non-goals

### 9.1 No In-place Reconstruct

Connected endpoints are not reconstructed in place. A failure or peer
incarnation change retires the current endpoint and requires a replacement
endpoint. This keeps ownership boundaries explicit for QPs, CQEs, counters, and
pending slices.

### 9.2 No Active CQ Polling in `finishDestroy()`

Polling CQ in both worker and reclaim paths would complicate ownership and
increase the chance of double processing. The design keeps CQ handling in the
worker and uses manual completion only as a bounded fallback.

### 9.3 Timestamp Is Not a Total Order

The design avoids depending on cross-machine clock ordering. If deployment
clock skew is larger than the current sanity window, the window should become
configurable rather than changing timestamp into a stronger ordering primitive.

### 9.4 Missing Handshake Fields

Descriptors that do not contain the reliability fields decode them as zero.
Zero values keep the descriptor valid, while nonzero values enable stricter
version and timestamp checks.

## 10. Implementation Map

The implementation points are:

- `RdmaEndPoint::resetConnection()` resets only `CONNECTING` endpoints. A
  `CONNECTED` endpoint is moved to `DESTROYING`.
- `RdmaEndPoint::setupConnectionsByActive()` generates and validates handshake
  versions, handles concurrent passive convergence, and rejects stale
  responses.
- `RdmaEndPoint::setupConnectionsByPassive()` detects duplicate QPs, peer QP
  mismatch, and simultaneous open.
- `RdmaTransport::onSetupRdmaConnections()` retries with a replacement endpoint
  only when `FLAG_PEER_RESTART_DETECTED` is set.
- `RdmaEndPoint::submitPostSend()` adds posted slices to the pending set in
  batches.
- `WorkerPool::performPollCq()` claims pending slices in endpoint batches
  before updating status or counters.
- `RdmaEndPoint::finishDestroy()` waits for worker-driven drain and manually
  fails remaining pending slices only after timeout or QP-to-ERR failure.

## 11. Validation Plan

The design should be reviewed and tested in stages:

1. Metadata compatibility: encode/decode descriptors with and without
   `handshake_version`, `handshake_timestamp`, and `handshake_flags`.
2. State machine: prove `CONNECTED` cannot return to `UNCONNECTED`.
3. Passive peer restart: same QP is idempotent; different QP sets restart flag
   and retires the endpoint.
4. Active stale response: advancing local handshake version causes earlier
   response rejection.
5. Simultaneous open: both peers deterministically converge to one accepted
   connection.
6. Completion ownership: CQ claim and manual fallback cannot both complete the
   same slice.
7. Hardware integration: peer restart during in-flight transfer, QP-to-ERR
   flush, forced destroy timeout, and late CQE after manual completion.
