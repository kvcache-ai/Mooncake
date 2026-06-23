# RDMA Connection and Reliability Specification

## Overview

This document describes Mooncake's RDMA connection establishment protocol and reliability guarantee mechanism. The design handles all edge cases during the connection lifecycle:

- **Connection Establishment**: Handshake protocol with peer restart detection, simultaneous connection handling, and timestamp validation
- **Endpoint Lifecycle**: State machine ensuring atomic transitions and proper cleanup
- **Completion Guarantee**: Every RDMA request receives a completion event (SUCCESS or FAILED)

## Table of Contents

1. [Endpoint Lifecycle](#1-endpoint-lifecycle)
2. [Handshake Protocol](#2-handshake-protocol)
3. [Completion Guarantee](#3-completion-guarantee)
4. [Integration Workflow](#4-integration-workflow)
5. [Reference](#5-reference)

---

## 1. Endpoint Lifecycle

### 1.1 State Machine

```
UNCONNECTED → CONNECTING → CONNECTED → DESTROYING → DESTROYED
  ↓         ↓         ↓         ↑
  └─────────┴─────────┴─────────┘
        (On any error)
      Transition to DESTROYING
```

### 1.2 State Definitions

| State | Description | Allowed Operations |
|-------|-------------|-------------------|
| **UNCONNECTED** | Initial state after creation, or after destruction | `setupConnectionsBy*()`, `beginDestroy()` |
| **CONNECTING** | Handshake in progress | `beginDestroy()` (abort handshake) |
| **CONNECTED** | Connection established, ready for data transfer | `submitPostSend()`, `beginDestroy()` |
| **DESTROYING** | Draining in-flight requests, QPs in error state | `finishDestroy()` only |
| **DESTROYED** | Resources destroyed, endpoint unusable | None |

### 1.3 Core Invariants

For system correctness, the following invariants must always hold:

1. **State Constraint**: Endpoint state must be one of the five states above
2. **Uniqueness Constraint**: For any pair of local and peer addresses, there can be at most one endpoint in CONNECTING, CONNECTED, or DESTROYING state
3. **Liveness Constraint**: A connected endpoint must be active and have peer queue pair numbers
4. **Counter Consistency**: Number of requests in send queue equals number of pending slices

### 1.4 Lifecycle Operations

#### Connection Establishment

```
Active Handshake (initiator):
    setupConnectionsByActive()
        → beginHandshakeAttempt()        // Increment version
        → Send HandshakeRequest(version, timestamp)
        → Verify response version, timestamp, peer QP number
        → If valid: doSetupConnection()
        → Record successful handshake
        → Enter CONNECTED state

Passive Handshake (responder):
    setupConnectionsByPassive(peer_desc, local_desc)
        → beginHandshakeAttempt()        // Increment version
        → Validate peer_desc timestamp, nic path
        → Detect peer restart
        → Handle simultaneous connection
        → If valid: doSetupConnection()
        → Record successful handshake
        → Enter CONNECTED state
```

#### Data Transfer

```
Operations in CONNECTED state:
    submitPostSend(slice_list)
        → Check if status is CONNECTED
        → For each slice:
            - addToPending(slice)
            - Write slice address to work request ID
            - Increment send queue depth counter
            - Increment completion queue counter
        → Call ibv_post_send() to submit
        → If submit fails: removeFromPending(slice)

    Worker Thread:
        performPollCq()
            → For each completion queue entry:
                - Extract slice address
                - removeFromPending(slice)
                - Decrement send queue depth counter
                - Decrement completion queue counter
                - Mark slice success or failure
```

#### Two-Phase Destruction

```
Phase 1: beginDestroy() - Immediate (non-blocking)
    - Set active flag to false
    - Change status to DESTROYING
    - Record inactive timestamp
    - For each queue pair: call ibv_modify_qp to set to error state
    - If any QP transition fails: set needs_manual_completion flag
    - Return immediately

Phase 2: finishDestroy() - Deferred
    - Actively poll completion queue (up to 100 iterations):
        For each completion entry flushed from error QPs:
            - removeFromPending(slice)
            - Update counters
            - Mark slice SUCCESS or FAILED

    - Check if all requests are drained:
        If all send queue depths are 0:
            Proceed with destruction
        Else if timeout or needs manual completion:
            Call generateManualCompletion()
            For each pending slice:
                - Mark as FAILED
                - Update counters
            Force all queue pair depths to 0

    - Call deconstructLocked():
        - Verify counters are zero
        - Destroy all queue pairs
        - Release pending set
        - Change status to DESTROYED
```

### 1.5 Lifecycle Examples

```
Normal Connection and Transfer:
    t0: Endpoint created, status = UNCONNECTED
    t1: setupConnectionsByActive() called
    t2: status = CONNECTING, RPC sent
    t3: Response received and validated
    t4: doSetupConnection() completes, status = CONNECTED
    t5-tN: Multiple submitPostSend() / performPollCq() cycles
    tX: beginDestroy() called (normal shutdown or error)
    tX+0: status = DESTROYING, QPs enter error state
    tX+Δ: Worker thread polls CQ, processes flushed requests
    tY: All requests drained, finishDestroy() succeeds
    tY: status = DESTROYED, QPs destroyed

Peer Restart During Transfer:
    t0: Connection established, status = CONNECTED
    t1-5: Data transfer ongoing
    t6: Peer restarts (new QP numbers)
    t7: New handshake arrives with different peer QP number
        OR submitPostSend() detects failure
    t8: beginDestroy() called immediately
    t9: Old endpoint → DESTROYING
    t10: New endpoint created for same peer
    t11: New handshake completes
    t12: New endpoint → CONNECTED
    t13: Old endpoint finishDestroy() completes
```

---

## 2. Handshake Protocol

### 2.1 Design Principles

1. **Idempotency**: Duplicate handshake requests must not create multiple connections
2. **Temporal Correctness**: Only the latest handshake response is accepted
3. **Restart Detection**: Reliable detection of peer restarts in all scenarios
4. **Simultaneous Connection**: Correct handling when both sides initiate connection
5. **Timeout Protection**: Prevention of indefinite waiting
6. **State Consistency**: Atomic endpoint state transitions

### 2.2 Message Structure

```cpp
struct HandShakeDesc {
    // Basic connection info
    std::string local_nic_path;      // Local NIC path (e.g., "server@mlx5_0")
    std::string peer_nic_path;       // Peer NIC path
    std::string local_gid;           // Local GID
    uint16_t local_lid;              // Local LID
    std::vector<uint32_t> qp_num;    // Queue pair numbers

    // Enhanced fields for robustness
    uint64_t handshake_version = 0;  // Monotonically increasing version
    uint64_t timestamp = 0;          // Timestamp for ordering
    uint32_t flags = 0;              // Handshake flags
    std::string reply_msg;           // Error/rejection message

    enum Flags {
        FLAG_PEER_RESTART_DETECTED = 0x1,  // Peer restart detected
    };
};
```

### 2.3 Version Mechanism

Each endpoint maintains a monotonically increasing handshake version:

```cpp
class RdmaEndPoint {
    std::atomic<uint64_t> handshake_version_ = 0;

    uint64_t beginHandshakeAttempt() {
        return ++handshake_version_;
    }

    bool verifyHandshakeVersion(uint64_t response_version) const {
        return handshake_version_.load() == response_version;
    }
};
```

**Purpose**: Prevent accepting stale responses after connection state changes.

### 2.4 Active Handshake Flow (Initiator)

```
Initiator                          Responder
   │                                   │
   │  ┌─────────────────────────┐      │
   │  │ 1. Generate version N   │      │
   │  │ 2. Fill local_desc      │      │
   │  └─────────────────────────┘      │
   │                                   │
   │  HandshakeRequest(local_desc) ────→│
   │                                   │
   │                              ┌─────┤
   │                              │ 1.  │Check if already connected
   │                              │ 2.  │Verify nic path match
   │                              │ 3.  │Detect peer restart
   │                              │ 4.  │Setup QPs
   │                              └─────┤
   │                                   │
   │  ←─── HandshakeResponse(peer_desc)│
   │                                   │
   │  ┌─────────────────────────┐      │
   │  │ Verify version == N     │      │
   │  │ Verify timestamp        │      │
   │  │ Check peer QP number    │      │
   │  │ Finalize connection     │      │
   │  └─────────────────────────┘      │
```

**Validation Steps:**

1. **Version Check**: Reject if `peer_desc.handshake_version != current_version`
2. **Timestamp Check**: Reject if timestamp is invalid (too old or in future)
3. **Peer Restart Detection**: Reject if `peer QP number` differs from established
4. **Response Ordering**: Reject if `peer_desc.timestamp < local_desc.timestamp`
5. **Rejection Flag**: Reject if peer sets `FLAG_PEER_RESTART_DETECTED`

### 2.5 Passive Handshake Flow (Responder)

```
Responder receives HandshakeRequest(peer_desc):
    ┌─────────────────────────────────┐
    │ 1. Check timestamp validity     │
    │    - Reject if too old/future   │
    │                                   │
    │ 2. Check if already connected    │
    │    - If connected and QP matches:│
    │      → Return OK (idempotent)   │
    │    - If connected but QP differs:│
    │      → Check timestamp is newer │
    │      → Set FLAG_PEER_RESTART_   │
    │        DETECTED flag            │
    │      → Reject with error        │
    │                                   │
    │ 3. Handle simultaneous open      │
    │    - If also CONNECTING:          │
    │      → Compare local nic paths  │
    │        using dictionary order   │
    │      → If we win (local < peer): │
    │        → Reject peer's request  │
    │      → If we lose (local >= peer):│
    │        → Transition to UNCONNECTED│
    │        → Accept peer's handshake │
    │        (see section 2.6)        │
    │                                   │
    │ 4. Verify nic path match         │
    │    - local == peer.peer_nic_path│
    │    - peer == local.local_nic_path│
    │                                   │
    │ 5. Setup QPs                     │
    │    - State transitions          │
    │    - Record success             │
    └─────────────────────────────────┘
```

### 2.6 Simultaneous Connection Handling

When both sides initiate handshake simultaneously, dictionary order is used to deterministically decide which side continues and which backs off:

```cpp
bool we_win = (local_nic_path < peer_local_nic_path);
if (we_win) {
    // Continue our active handshake
    // Reject peer's passive handshake request
    return ERR_REJECT_HANDSHAKE;
} else {
    // Give up our active handshake
    status_ = UNCONNECTED;
    // Continue to accept peer's handshake
}
```

**Implementation**: See [rdma_endpoint.cpp:737-762](../src/transport/rdma_transport/rdma_endpoint.cpp#L737-L762)

**Why this prevents livelock**: Dictionary order ensures a deterministic outcome. If both sides are in CONNECTING state when receiving the other's handshake request:
- The side with the lexicographically smaller NIC path "wins" and continues its active handshake
- The side with the lexicographically larger NIC path "loses" by transitioning to UNCONNECTED and accepting the peer's handshake
- Both sides will never reject each other simultaneously, eliminating the livelock possibility

### 2.7 Edge Case Coverage

| Scenario | Detection Method | Handling |
|----------|------------------|----------|
| Peer restart during handshake | Peer QP number mismatch | Destroy endpoint + reject |
| Simultaneous connection | Dictionary order comparison | One side gives up (see 2.6) |
| Endpoint destroyed during handshake | Version number check | Reject stale response |
| RPC timeout but peer succeeds | Timestamp validation | Reject old responses |
| Restart immediately after handshake | Worker retry logic | Re-handshake |
| Delayed/out-of-order responses | Version + timestamp | Reject non-matching |
| Concurrent handshake requests | Version serialization | Accept latest only |
| Handshake timeout | Timeout protection | Destroy endpoint + error |

### 2.8 Timeout Values

| Operation | Timeout | Purpose |
|-----------|---------|---------|
| Handshake RPC | 30s | Prevent indefinite wait |
| Version staleness | 10s | Reject old responses |
| Waiting for existing handshake | 10s | Prevent deadlock |
| Connection drain | 30s | Wait for request flush completion |

### 2.9 Error Codes

```cpp
enum HandshakeError {
    ERR_REJECT_HANDSHAKE = -1,     // Handshake rejected, retry with new endpoint
    ERR_INVALID_ARGUMENT = -2,      // Invalid parameters
    ERR_ENDPOINT = -3,              // Endpoint error
    ERR_TIMEOUT = -4,                // Handshake timeout
    ERR_DEVICE_NOT_FOUND = -5,      // Peer device not found
};
```

---

## 3. Completion Guarantee

### 3.1 Core Principle

**Every RDMA request submitted to hardware must receive a completion event, either SUCCESS or FAILED.**

This guarantee ensures:
- No request is silently lost
- Upper layers can reliably track operation status
- Resources are properly cleaned up
- The system can recover from all error conditions

### 3.2 Implementation Mechanism

#### Pending Slices Tracking

Each endpoint maintains a set of pending (in-flight) slices:

```cpp
class RdmaEndPoint {
    using PendingSet = std::unordered_set<Transport::Slice*>;
    PendingSet *pending_slices_ = nullptr;

    // Lazy allocation - only create when needed
    PendingSet* getPendingSet() {
        if (!pending_slices_) {
            pending_slices_ = new PendingSet();
        }
        return pending_slices_;
    }
};
```

#### Work Request Submission

```
submitPostSend(slice_list):
    For each slice in slice_list:
        addToPending(slice)       // Mark as pending
        wr.wr_id = (uint64_t)slice
        wr_depth_list[qp]++
        cq_outstanding++

    ibv_post_send(wr_list)

    If post_send fails:
        For each failed request:
            removeFromPending(slice)  // Remove from tracking
            wr_depth_list[qp]--
            cq_outstanding--
```

#### Normal Completion

```
performPollCq():
    For each completion queue entry:
        slice = (Slice*)wc.wr_id
        removeFromPending(slice)       // No longer pending
        wr_depth_list[qp]--
        cq_outstanding--

        If wc.status == IBV_WC_SUCCESS:
            slice.markSuccess()
        Else:
            slice.markFailed()  // Network error, retry
```

#### Completion During Endpoint Destruction

```
finishDestroy():
    // Phase 1: Active poll to drain flushed requests
    For up to 100 iterations:
        Poll CQ for entries flushed from error QPs
        For each completion entry:
            removeFromPending(slice)
            Update counters
            Mark slice SUCCESS or FAILED

    // Phase 2: Check for remaining requests
    If has_outstanding OR needs_manual_completion:
        // Phase 3: Manual completion for remaining
        generateManualCompletion()
        For each slice in pending_slices_:
            Mark as FAILED               // Artificial completion
            Update counters
        Clear pending_slices_

    // Phase 4: Verify and destroy
    Assert(cq_outstanding == 0)
    Assert(all wr_depth_list[] == 0)
    deconstructLocked()
```

### 3.3 Coverage Analysis

| Scenario | Request Status | CQ Generated | Handled By | Result |
|----------|---------------|--------------|------------|--------|
| Normal completion | Posted | ✅ Yes | performPollCq | SUCCESS |
| Network error | Posted | ✅ Yes | performPollCq | FAILED + retry |
| QP error flush | Posted | ✅ Yes | finishDestroy poll | FAILED |
| ibv_modify_qp fails | Posted | ❌ No | generateManualCompletion | FAILED |
| Process crash | Posted | N/A | N/A | **Lost (physical limit)** |
| Submit after destroy | Not submitted | N/A | submitPostSend check | FAILED (immediate) |

### 3.4 Guarantee Statement

**For all physically possible scenarios (where process remains alive):**

```
For each slice submitted via submitPostSend():
    Eventually(slice.status ∈ {SUCCESS, FAILED})

Proof:
    Case 1: submitPostSend succeeds
        → slice ∈ pending_slices_

        Subcase 1a: Normal completion
            → CQ event generated
            → performPollCq() processes
            → removeFromPending(slice)
            → slice marked SUCCESS or FAILED

        Subcase 1b: Endpoint destroyed before CQ
            → beginDestroy() → QP error
            → Hardware flushes WR to CQ
            → finishDestroy() polls CQ
            → removeFromPending(slice)
            → slice marked FAILED

        Subcase 1c: ibv_modify_qp fails
            → needs_manual_completion_ = true
            → finishDestroy() calls generateManualCompletion()
            → slice marked FAILED

        ∴ slice always gets final status

    Case 2: submitPostSend rejected (endpoint not CONNECTED)
        → slice ∈ failed_slice_list
        → Caller knows immediately
        → slice.status = FAILED (by caller)

    Case 3: Process crashes
        → Physical limit, no code executes
        → ∴ Not a software failure

∴ Every slice receives completion event in all recoverable scenarios
```

### 3.5 Failure Modes and Recovery

#### Hardware Failure (ibv_modify_qp fails)

```
Detection:
    beginDestroyNoLock()
        For each queue pair:
            If ibv_modify_qp(QP, ERR) fails:
                any_qp_failed = true
        If any queue pair failed:
            needs_manual_completion_ = true

Recovery:
    finishDestroy()
        If needs_manual_completion_:
            LOG(ERROR) << "Hardware failure detected"
            generateManualCompletion()
            // All pending slices get FAILED status

Result:
    - All slices marked FAILED (not lost)
    - Counters corrected
    - Resources cleaned up
    - Upper layer notified via LOG(ERROR)
```

#### Timeout During Destruction

```
Detection:
    finishDestroy()
        After active polling:
        If wr_depth_list[] not all zero:
            If elapsed time > 30s:
                LOG(WARNING) << "Destruction timeout"

Recovery:
    generateManualCompletion()
    // Manually complete remaining slices

Result:
    - No resource leak
    - All slices get completion
```

#### Peer Restart

```
Detection:
    setupConnectionsByActive() OR setupConnectionsByPassive()
        If peer QP number mismatch:
            LOG(WARNING) << "Peer restart detected"
            beginDestroy()
            Return ERR_REJECT_HANDSHAKE

Recovery:
    WorkerPool::performPostSend()
        If ERR_REJECT_HANDSHAKE:
            deleteEndpointByPtr(old_endpoint)
            endpoint = context().endpoint(peer_nic_path)  // New endpoint
            setupConnectionsByActive()

Result:
    - Old connection drained
    - New connection established
    - All slices from old connection: FAILED
    - New slices can be submitted
```

### 3.6 Performance Considerations

**Memory Overhead:**
- Pending set: ~8 bytes per slice (pointer)
- For 256 work requests: ~2 KB per endpoint
- Lazy allocation: only when endpoint is used

**CPU Overhead:**
- addToPending/removeFromPending: O(1) hash operation
- generateManualCompletion: O(pending_count) only on destruction
- Active polling in finishDestroy: bounded (100 iterations × 64 entries)

**Conclusion:** Minimal overhead for strong reliability guarantee.

---

## 4. Integration Workflow

### 4.1 End-to-End Flow

```
Connection Establishment:
    1. Handshake with version + timestamp validation
    2. Peer restart detection
    3. Simultaneous connection handling
    → Enter CONNECTED state

Data Transfer:
    1. Submit with pending tracking
    2. Poll with pending removal
    3. Retry on failure (up to max_retry_cnt)
    → Every slice gets completion

Error Recovery:
    1. Network error → slice.markFailed() → retry
    2. Peer restart → old endpoint drain → new endpoint create
    3. Hardware failure → manual completion → LOG(ERROR)

Cleanup:
    1. beginDestroy() - immediate QP error
    2. finishDestroy() - drain + manual completion
    3. deconstructLocked() - resource cleanup
    → Enter DESTROYED state
```

### 4.2 Combined Invariants

```
For all endpoints and submitted slices:
    Eventually(slice.status ∈ {SUCCESS, FAILED})

For all endpoints:
    pending_slices_ = { submitted but not yet completed slices }

For all endpoints:
    If status = DESTROYING:
        Eventually all pending slices will complete

For all endpoints:
    cq_outstanding = |pending_slices_|
```

### 4.3 Reliability Guarantees

The combination of handshake protocol and completion guarantee provides:

1. **Connection Reliability**: Correct peer connection in all scenarios
2. **Data Reliability**: Every request gets completion (SUCCESS or FAILED)
3. **Recovery Reliability**: System can recover from all error conditions
4. **Resource Reliability**: No resource leaks even under hardware failure

### 4.4 Limitations

The only scenario where requests are lost is **process or kernel crash**, which is a physical limit beyond software control. In all other scenarios, every request receives a completion event.

---

## 5. Data Path and Error Recovery

### 5.1 Retry Logic

When a work request fails (network error, path failure, etc.), the system implements a retry mechanism:

```cpp
struct Slice {
    int rdma;
    int max_retry_cnt;
};

bool shouldRetrySlice(Slice* slice) {
    slice->rdma.retry_cnt++;
    return slice->rdma.retry_cnt < slice->rdma.max_retry_cnt;
}
```

**Retry Flow:**
1. Failed slice is collected in `failed_slice_list`
2. `shouldRetrySlice()` increments retry count and checks limit
3. If retry allowed: slice is redispatched to an alternate path
4. If retry exhausted: slice is marked FAILED

**Configuration:** `max_retry_cnt` is configurable per slice to balance reliability and latency.

### 5.2 Rail Failure Handling

To prevent cascading failures and provide fast recovery, the system implements per-path health monitoring:

**RailState Structure:**
```cpp
struct RailState {
    int error_count = 0;              // Consecutive errors
    uint64_t pause_until_ns = 0;     // Auto-recovery timestamp
};
```

**Failure Detection:**
- Each path (identified by `peer_nic_path`) maintains an error counter
- Threshold: `kRailErrorThreshold = 5` consecutive errors
- On threshold exceeded: path paused for `kRailPauseNs = 1 second`

**Auto-Recovery:**
```cpp
bool isRailAvailable(peer_nic_path):
    if pause_until_ns == 0: return true
    if now >= pause_until_ns:
        // Auto-recover: pause expired
        error_count = 0
        pause_until_ns = 0
        return true
    return false
```

**Path Selection:**
- During slice submission, if selected rail is paused
- System tries alternative devices/paths for the same segment
- If all rails unavailable: slice marked FAILED

### 5.3 Context Health Tracking

When all paths through a local RNIC fail (catastrophic hardware failure), the context is marked inactive:

**Detection:**
- If all submitted slices fail due to rail unavailability
- Increment `context_failure_count`
- Threshold: `kContextFailureThreshold = 32` consecutive failures

**Recovery:**
- Context marked inactive: `context_.set_active(false)`
- All endpoints disconnected
- On any successful CQ entry: `markContextSuccess()` resets counter
- On port active event: context reactivated

**Protection:**
- `performPostSend()` fast-fails if context unhealthy
- Prevents spinning on dead hardware
- All slices marked FAILED immediately

### 5.4 Redispatch Mechanism

When slices fail and need retry, they are redispatched to alternate paths:

```
redispatch(failed_slices):
    For each slice:
        If retry_cnt >= max_retry_cnt:
            markFailed()
        Else:
            Reload segment metadata (may have changed)
            Select alternative device/path
            Update slice->peer_nic_path
            Add to collective_slice_queue_ for retry
```

**Coordination:**
- `redispatch_counter_` notified to all workers
- Workers flush their local queues and re-dispatch
- Ensures slices reach healthy paths after topology changes

---

## 6. Concurrency and Thread Safety

### 6.1 Lock Strategy

**RWSpinlock Usage:**
- Endpoint state protected by `RWSpinlock lock_`
- Write lock: state transitions, connection setup, destruction
- Read lock: state queries, passive access

**Atomic Operations:**
- `status_`: `std::atomic<Status>` for lock-free reads
- `active_`: `volatile bool` with explicit memory barriers
- `cq_outstanding_`: `volatile int*` with atomic fetch-add/sub

**Memory Ordering:**
- Relaxed ordering for frequent reads (status checks)
- Release/acquire for state transitions
- Explicit `__sync_fetch_and_add` for counter updates

### 6.2 Concurrent Handshake Prevention

**Problem:** Multiple threads may simultaneously call `setupConnectionsByActive()` for the same endpoint.

**Solution:** Compare-and-swap on status:
```cpp
if (status == UNCONNECTED) {
    if (compare_exchange_swap(CONNECTING)) {
        // We won the race, proceed with RPC
        do_rpc = true
    } else {
        // Another thread won, wait for completion
        wait_with_timeout()
    }
}
```

**Wait Mechanism:**
- Spin-then-sleep: 500 iterations of PAUSE(), then exponential backoff
- Timeout: 10 seconds maximum wait
- On timeout: destroy endpoint, return ERR_REJECT_HANDSHAKE

### 6.3 Two-Phase Destruction Concurrency

**Problem:** `submitPostSend()` may run concurrently with `finishDestroy()`.

**Solution:** Non-blocking beginDestroy + deferred finishDestroy:

```
Thread A (data path):          Thread B (reclaim):
submitPostSend()                finishDestroy()
    Check active == true            → beginDestroy() called earlier
    Check status == CONNECTED        → Active poll CQ
    Submit to QP                     → Process WCs
                                       → wr_depth[] drops to 0
                                       → deconstruct()

Thread A blocked at submitPostSend:
    Check active == false       (Thread B set it)
    Check status != CONNECTED   (Thread B set it to DESTROYING)
    → Return slices in failed_slice_list
```

**Key Insight:** By checking `active` and `status` under lock, we ensure concurrent submission fails safely without accessing destroyed QPs.

---

## 7. Implementation Details

### 7.1 Established Peer Tracking

For peer restart detection, the endpoint stores information about the successfully connected peer:

```cpp
struct PeerInfo {
    uint32_t qp_num = 0;              // First peer QP number
    uint64_t handshake_version = 0;    // Version of successful handshake
    uint64_t timestamp = 0;            // When connection was established
} established_peer_;
```

**Usage in Passive Handshake:**
```cpp
If connected AND peer_qp != established_peer_.qp_num:
    If peer_desc.timestamp > established_peer_.timestamp:
        → Peer restarted with newer connection
        → Reject with FLAG_PEER_RESTART_DETECTED
    Else:
        → Stale/replay request
        → Reject
```

**Purpose:** Distinguish between genuine peer restart and stale/replay requests.

### 7.2 Pending Set Lifecycle

**Allocation (Lazy):**
```cpp
PendingSet* getPendingSet() {
    if (!pending_slices_) {
        pending_slices_ = new PendingSet();
    }
    return pending_slices_;
}
```
Created only when first slice is submitted to endpoint.

**Operations:**
- `addToPending(slice)`: Called in `submitPostSend()` before QP submission
- `removeFromPending(slice)`: Called in `performPollCq()` or `finishDestroy()`

**Deallocation:**
```cpp
releasePendingSet() {
    if (pending_slices_) {
        delete pending_slices_;
        pending_slices_ = nullptr;
    }
}
```
Called in destructor and after `finishDestroy()` completes.

### 7.3 Manual Completion Triggers

`generateManualCompletion()` is called when:
1. `ibv_modify_qp(QP, ERR)` fails (hardware failure)
2. `finishDestroy()` timeout (WRs not draining)
3. Any slice remains in `pending_slices_` after active polling

**Effect:** All pending slices marked FAILED, counters force-zeroed, pending set cleared.

**Logging:** `LOG(WARNING)` or `LOG(ERROR)` to notify upper layer of abnormal completion.

---

## 8. Reference

### 5.1 Security Considerations

1. **Timestamp Validation**: Reject timestamps in the future or too far in the past
2. **NIC Path Verification**: Ensure bidirectional path consistency
3. **Replay Protection**: Version numbers prevent replay attacks
4. **State Machine Enforcement**: Strict state transitions prevent invalid operations

### 5.2 Performance Impact

**Overhead per handshake:**
- Version increment: atomic integer operation (negligible)
- Timestamp generation: system call (~0.1μs)
- Additional validation: O(1) comparisons

**Memory overhead:**
- Per endpoint: ~24 bytes for version, peer info

**Conclusion**: Minimal impact on normal path while providing robust edge case handling.

### 5.3 Future Enhancements

1. **Cryptographic Authentication**: Add MAC to handshake messages
2. **Connection Migration**: Support for seamless connection migration
3. **Batch Handshakes**: Optimize for multiple endpoint creation
4. **Fast Path Optimization**: Bypass validation for known-good peers
