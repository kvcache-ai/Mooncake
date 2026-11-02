# Session-Level KV Cache Reference Counting Design

## Problem Statement

In the Mooncake x SGLang HiCache architecture, L3 (distributed storage) acts as
a cluster-wide shared KV cache pool. Multiple SGLang inference instances share
prefix KV cache blocks stored in Mooncake Store. Each SGLang request session
associates its KV cache objects with a `session_id` on the Master for lifecycle
management (automatic cleanup on session expiry via `session_ttl_sec`).

The core problem arises when **a session expires but some of its associated KV
cache blocks are still being reused by other active sessions**:

- Session A writes prefix block X to Mooncake Store with `session_id = "sess_a"`.
- Session B (from a different SGLang process) prefetches the same prefix block
  X from L3 — no duplicate write needed (HiCache cross-instance deduplication).
- Session A expires. The old `RemoveBySession("sess_a")` calls `BatchRemove(keys,
  force=true)`, unconditionally deleting block X.
- Session B's subsequent reads of block X fail — data corruption / missing
  cache.

The original design had **no mechanism to express "this object is referenced by
multiple sessions"**. `ObjectMetadata::session_id` is a `std::string` single
value — one object can belong to at most one session. It is set via the
`BeginSessionTag`/`CommitSessionTag` mechanism at `PutEnd` time, not through
the constructor.

## Design Decisions and Rationale

### Decision 1: Simple Atomic Counter vs. Multi-Session Reverse Index

**Chosen: Atomic counter (`session_refcount_`) on `ObjectMetadata`.**

| Aspect | Atomic Counter | Multi-Session Reverse Index (`key_sessions_`) |
|--------|---------------|----------------------------------------------|
| Per-key memory | `uint32_t` (4 bytes) | `unordered_set<string>` (~64 bytes minimum) |
| Dec logic | `fetch_sub(1)`, compare to 0 | Query set, check if any other session is active |
| Needs `IsActive()` | No | Yes — must check `ActiveSessionTracker` per remaining session |
| Failure mode if tracker is stale | Counter is self-contained, no external dependency | Must handle stale tracker entries |
| Implementation complexity | Low — one atomic, one forward map | Medium — bidirectional map, active-session queries |

The counter approach is simpler because the delete decision is purely numeric
("did the count reach 0?") rather than relational ("are any of the other
sessions in the set still active?"). It avoids coupling the delete path to the
`ActiveSessionTracker`, which is best-effort and updated via asynchronous HTTP
heartbeats from SGLang engines.

### Decision 2: Forward-Only `SessionKeyIndex` (session → keys)

**Chosen: Remove the reverse mapping (`key_sessions_`), keep only forward
(`session_keys_`).**

The original code maintained a bidirectional map: `session_keys_` (session →
keys) and `key_sessions_` (key → sessions). With the counter approach, the
reverse mapping is unnecessary — `RemoveBySession` only needs to answer "which
keys does this session reference?" (forward lookup). The per-key reference count
lives on `ObjectMetadata` itself, so there is no need to look up "which sessions
reference this key" (reverse lookup).

Removing `key_sessions_`:
- Eliminates dual-map maintenance complexity.
- Halves the memory overhead of the index.
- Simplifies `RemoveSession` to a single `session_keys_.erase(session_id)`.
- `RegisterKey` returns `bool` (insert result), enabling idempotent refcount
  increment — the same (session, key) pair registered twice does not double-inc.

### Decision 3: Increment on `GetReplicaList`, Not on Write-Back

**Chosen: `session_refcount_` is incremented in `GetReplicaList` (the read
path), not only in the write-back path.**

This is the most critical design decision. The alternatives considered were:

| Trigger Point | When It Fires | Coverage | Problem |
|---------------|--------------|----------|---------|
| `PutStart` only | Object creation | Only the writing session | Cross-session read-only sessions never register |
| Write-back to L3 | HiCache `write_backup_storage` | Only when SGLang writes data back | `write_back` policy: pure reads that don't evict L1 never trigger write-back. `write_through_selective`: cold reads don't exceed the threshold. **Pure read sessions are missed.** |
| `GetReplicaList` | Every L3 read/prefetch | **All sessions that read from L3** | None — this is the mandatory entry point for any L3 data access. |

SGLang's HiCache workflow: `local match → prefetch from L3 → prefill`. The
prefetch step **must** call `GetReplicaList` to obtain replica locations before
RDMA transfer. Therefore, any session that uses L3 data will pass through
`GetReplicaList`. This is the only point that universally covers both readers
and writers.

### Decision 4: Embed in Existing `GetReplicaList` Flow, No New RPC

**Chosen: Add `session_id` as an optional parameter to the existing
`GetReplicaList` and `BatchGetReplicaList` APIs. Performance overhead is
negligible.**

The reasoning:

1. **No additional RPC round-trip**: SGLang already calls `GetReplicaList` for
   every L3 access. The refcount increment happens within the same request's
   critical section.

2. **Overhead comparison with existing operations in `GetReplicaList`**:
   - `GrantReadLease`: SpinLock + `chrono::max` — ~50ns
   - `GrantLeaseForGroup`: iterates group members, grants lease to each — O(group_size)
   - `dfs_allocator_->UpdateAccess`: unordered_map lookup + update
   - **New `session_refcount_.fetch_add(1)`**: single atomic — ~5ns
   - **New `RegisterKey`**: `unordered_set::insert` — ~30ns, only on first cross-session access

   The new operations are cheaper than the lease grant that already runs in the
   same code block.

3. **Idempotent**: `RegisterKey` returns `false` if the (session, key) pair
   already exists, so repeated reads from the same session do not double-increment
   the counter. The `fetch_add` only executes when `RegisterKey` returns `true`.

4. **Same-session skip**: When `session_id == metadata.session_id` (the reader
   is the object's owner session), the increment is skipped entirely — no
   `RegisterKey` call, no atomic operation.

### Decision 5: `RemoveBySession` Decrement-and-Conditionally-Delete

**Chosen: Per-key `fetch_sub(1)`, only `BatchRemove` keys whose refcount reaches
0.**

The original `RemoveBySession` called `BatchRemove(all_keys, force=true)` — a
bulk unconditional delete. The new logic:

```
for each key in session_keys_[session_id]:
    prev = metadata.session_refcount_.fetch_sub(1)
    if prev <= 1:
        # refcount was 1 (only this session), now 0 → safe to delete
        add key to keys_to_delete
    # else: other sessions still reference this key → skip
    UnregisterKey(session_id, key)  # clean forward index

BatchRemove(keys_to_delete, force=true)  # only delete unreferenced keys
```

Why `prev <= 1` instead of `prev == 1`:
- Defensive: if a bug or race causes refcount to underflow, `prev == 0` would
  make `fetch_sub(1)` wrap to `UINT32_MAX`. The `<= 1` check catches this.
- When `prev <= 1`, we explicitly `store(0)` to reset the counter, preventing
  underflow propagation.

Why per-key locking rather than a single bulk operation:
- Each key may belong to a different namespace. The per-key loop acquires
  `MetadataAccessorRW` only for the key being decremented, minimizing lock
  contention.
- `BatchRemove` at the end operates on the filtered `keys_to_delete` list, which
  is typically much smaller than the original set.

### Decision 6: EraseMetadata Does Not Decrement

When an object is deleted through the normal eviction path or explicit `Remove`,
`EraseMetadata` is called. It cleans up the **owner session's** forward mapping
via `UnregisterKey(metadata.session_id, scoped_key)`, but does **not** decrement
`session_refcount_`.

Why: `EraseMetadata` is the terminal operation — the `ObjectMetadata` is about to
be destroyed. Decrementing the counter is meaningless since the object is gone.
The forward-index entries from other sessions that registered this key will be
cleaned up lazily: when those sessions expire, `RemoveBySession` will look up the
key, find that the metadata no longer exists (iterator past end → `continue`),
skip the decrement, and `UnregisterKey` will clean the forward index. This is
safe — no refcount underflow, no dangling pointer.

### Decision 7: Persist `session_refcount_` in All Serialization Paths

If the Master restarts and reconstructs metadata from a snapshot, the refcount
must be restored. Otherwise:
- Refcount defaults to 0 after restore.
- The next `RemoveBySession` would see `prev = 0`, fail the `<= 1` check
  (well, `0 <= 1` is true, so it would actually delete), which is the correct
  behavior for objects with no cross-session references.
- **But**: if cross-session references existed before the crash, the refcount
  information is lost, and `RemoveBySession("sess_a")` would delete a key that session
  B is still using.

Therefore `session_refcount_` is persisted in:

1. **`MetadataPayload`** (OpLog serialization): `struct_pack::compatible<uint32_t, 4>`
   — backward-compatible field added with version tag 4. Note that
   `compatible<3>` is `object_checksum` (uint64_t), a Mooncake-specific field
   not present in mooncake-test.
2. **`StandbyObjectMetadata`** (standby snapshot): same compatible field
   (`compatible<uint32_t, 4>`), plus `compatible<uint64_t, 3>` for
   `object_checksum`.
3. **`DeserializeStandbyObjectMetadata`** (catalog-backed snapshot provider):
   reads it back.
4. **`RestoreFromStandbySnapshot`**: restores via `accessor.Publish(...)`, then
   sets `metadata.session_id` and `metadata.session_refcount_.store(...)`,
   falling back to 1 if the field is absent but `session_id` is non-empty
   (old-format snapshots).

Mooncake uses two persistence paths: **OpLog** (struct_pack-based, via
`MetadataPayload`) and **catalog-backed snapshot** (via
`StandbyObjectMetadata`). Mooncake does **not** use the fork-based msgpack
`MetadataSerializer` mechanism that existed in mooncake-test.

Fallback logic in restore: `session_refcount.value_or(!session_id.value_or("").empty() ? 1 : 0)`.
This handles snapshots written by old code that pre-dates the refcount field:
- If `session_id` is non-empty, assume refcount = 1 (only the owner session).
- If `session_id` is empty, refcount = 0 (no session association, not subject to
  session cleanup).

### Decision 8: `session_id` as `std::string` with `nullopt` Default

**Chosen: `session_id` is a `std::string` throughout the entire stack. Empty
string `""` is the sentinel for "no session association".**

SGLang's inference engine produces session IDs as opaque strings (e.g.,
`"sess_abc123"`, `"request-batch-42"`). Using `uint64_t` internally would require
the caller to hash or convert these strings, losing debuggability and risking
collision if a non-injective hash is used. A `std::string` type preserves the
original identifier end-to-end.

Backward compatibility for snapshot deserialization: old snapshots written with
`uint64_t` session IDs are handled gracefully — the deserializer detects old
numeric format, skips the value (old numeric session IDs are no longer valid
with string-based lookup), and defaults to empty string.

The `GetReplicaList` and `BatchGetReplicaList` signatures use
`std::optional<std::string>` as the default `std::nullopt`. This ensures full
backward compatibility:
- Existing callers that do not pass `session_id` will not trigger refcount logic.
- The `WrappedMasterService` RPC layer passes it through transparently.
- coro_rpc serialization handles `std::optional` and `std::string` natively.

### Decision 9: `BatchGetReplicaList` Accepts a Single Session ID

**Chosen: `BatchGetReplicaList` accepts `std::optional<std::string>
session_id` — a single session ID.**

SGLang's batch read (prefetch) request carries a **single session ID**
(the session currently active on the engine for this batch). The same
session ID applies to all keys in the batch.

**Refcount increment strategy for batch reads:**

For each key in the batch whose `metadata.session_id` is non-empty:
1. If the reader `session_id` equals the owner session, skip (same-session
   read — the owner is still active and already has refcount = 1 from
   creation).
2. If the reader `session_id` differs from the owner session, register the
   reader session on this key and increment refcount if newly registered.

```
for each key in batch (inside MetadataAccessorRO):
    if metadata.session_id is empty: skip
    if session_id is empty or session_id == metadata.session_id: skip
    if RegisterKey(session_id, scoped_key) returns true (new pair):
        session_refcount_.fetch_add(1)
```

**Why a single session ID?**

- SGLang sends one session ID per batch read request, shared across all keys.
- The `RegisterKey` idempotency ensures no double-counting: if the same
  (session, key) pair was already registered by a previous batch, the refcount
  is not incremented again.

**Cost**: For a batch of K keys, worst-case work is O(K)
`RegisterKey` calls (each ~30ns `unordered_set::insert`). For typical SGLang
concurrency (hundreds of keys per batch), this is negligible compared to the
RDMA transfer that follows.

## Data Structures

### `ObjectMetadata::session_id` and `session_refcount_`

```cpp
std::string session_id;  // "" = no session association
mutable std::atomic<uint32_t> session_refcount_{0};
```

- `session_id` is `std::string` (not `const`): set via the
  `BeginSessionTag`/`CommitSessionTag` mechanism at `PutEnd` time, not through
  the constructor. Empty string `""` means no session association.
- `mutable` on `session_refcount_`: Modified inside `MetadataAccessorRO`
  (read-only access). The `GetReplicaList` path holds a read lock; the atomic
  increment is safe because `atomic::fetch_add` is lock-free and does not
  violate the read lock semantics.
- `uint32_t`: Sufficient capacity. The practical maximum is the number of
  concurrent sessions in the cluster, which is bounded by SGLang's concurrency
  (typically tens to low hundreds).
- Initialized to 0 in the constructor. Set to 1 in `InsertMetadata` when
  `session_id` is non-empty (session registration happens after the
  `BeginSessionTag` call within `InsertMetadata`).

### `SessionKeyIndex`

```cpp
class SessionKeyIndex {
    using SessionId = std::string;
    mutable std::shared_mutex mutex_;
    std::unordered_map<SessionId, std::unordered_set<std::string>>
        session_keys_ GUARDED_BY(mutex_);
};
```

- Forward-only: `session_id → set<scoped_key>`.
- `RegisterKey` takes `const SessionId&` (const ref to avoid copies).
- `RegisterKey` returns `bool` (insert result) for idempotent refcount inc.
- `RemoveSession` is a single `erase` call — no reverse-index cleanup needed.
- Lock order: after shard mutex (documented in the class comment).

### `ActiveSessionTracker`

```cpp
class ActiveSessionTracker {
    using SessionId = std::string;
    using TimePoint = std::chrono::system_clock::time_point;
    mutable std::shared_mutex mutex_;
    std::unordered_map<SessionId, TimePoint> sessions_ GUARDED_BY(mutex_);
};
```

- Tracks active session IDs (strings) and their last-active timestamps.
- Updated via HTTP API from SGLang engines.

## Complete Workflow

### Write Path

```
SGLang-A, session="sess_a":
  PutStart(key=X, config={session_id="sess_a"})
    → InsertMetadata:
        → ObjectMetadata created (session_id not yet set)
        → BeginSessionTag sets session_id="sess_a" on the metadata
        → session_refcount_.store(1)    ← owner session
        → SessionKeyIndex::RegisterKey("sess_a", X) → true
  PutEnd(key=X)
    → CommitSessionTag finalizes session_id on the metadata
    → Object becomes readable
```

### Read Path — Single-Key `GetReplicaList` (Cross-Session Reuse)

```
SGLang-B, session="sess_b":
  GetReplicaList(key=X, session_id="sess_b")
    → MetadataAccessorRO finds X (exists, session_id="sess_a" on metadata)
    → GrantReadLease (existing behavior)
    → session_id="sess_b" != metadata.session_id="sess_a"
      → SessionKeyIndex::RegisterKey("sess_b", X) → true (first time)
        → session_refcount_.fetch_add(1) → now 2
    → Return replica descriptors
  [SGLang-B RDMA reads X into local L2]
```

### Read Path — Batch `BatchGetReplicaList` (Single Session ID)

```
SGLang-B, session="sess_b":
  BatchGetReplicaList(keys=[X, Y, Z], session_id="sess_b")

  For key X (owner="sess_a"):
    "sess_b" != "sess_a" → cross-session
    RegisterKey("sess_b", X) → true (first time)
    refcount: 1 → 2

  For key Y (owner="sess_b"):
    "sess_b" == "sess_b" → skip (same-session read)

  For key Z (owner=""): no session → skip entirely
```

### Same-Session Read (No Increment)

```
SGLang-A, session="sess_a":
  GetReplicaList(key=X, session_id="sess_a")
    → session_id="sess_a" == metadata.session_id="sess_a" → skip (no inc)
```

### Session Expiry — Referenced Key Retained

```
Session "sess_a" TTL expires:
  SessionCleanupFunc → RemoveBySession("sess_a")
    → scoped_keys = {X}
    → For X:
        → fetch_sub(1) → prev=4, 4 > 1 → should_delete=false
        → UnregisterKey("sess_a", X)
    → keys_to_delete = {} (empty)
    → No BatchRemove call
    → RemoveSession("sess_a") — cleans forward index
    → active_session_tracker_.Remove("sess_a")
    → LOG: "deleted=0, retained=1"
```

### Session Expiry — Unreferenced Key Deleted

```
Session "sess_d" TTL expires:
  SessionCleanupFunc → RemoveBySession("sess_d")
    → scoped_keys = {X, Y}
    → For X:
        → fetch_sub(1) → prev=3, 3 > 1 → should_delete=false
        → UnregisterKey("sess_d", X)
    → For Y:
        → fetch_sub(1) → prev=3, 3 > 1 → should_delete=false
        → UnregisterKey("sess_d", Y)
    → keys_to_delete = {} (empty — other sessions still hold refs)
    → LOG: "deleted=0, retained=2"

  ...later, sessions "sess_b" and "sess_c" also expire...
  Last session's RemoveBySession decrements to 0 → key deleted.
```

### Eviction Path (Independent of Session Refcount)

```
Memory pressure triggers BatchEvict:
  → Selects X as eviction candidate (approximate LRU)
  → Checks lease — if active, skips (existing behavior)
  → If lease expired, calls EraseMetadata(X)
    → UnregisterGroupMember(X)
    → UnregisterKey(metadata.session_id="sess_a", X)  ← cleans owner's forward index
    → Does NOT decrement session_refcount_ (object is being destroyed)
    → Frees replica memory

Later, RemoveBySession("sess_a") runs:
  → scoped_keys from session_keys_["sess_a"] no longer contains X
  → (already unregistered by EraseMetadata)
  → Forward index is consistent
```

### Manual Session Removal (HTTP API)

```
POST /api/v1/sessions/remove  {"session_id": "sess_abc123"}
  → HandleRemoveBySession → RemoveBySession("sess_abc123")
  → Same refcount-based logic as automatic cleanup
```

### SGLang Crash Recovery

```
SGLang-B crashes (session "sess_b"'s heartbeat stops):
  → ActiveSessionTracker stops receiving updates for "sess_b"
  → After session_ttl_sec, SessionCleanupFunc triggers RemoveBySession("sess_b")
  → All keys registered under "sess_b" get fetch_sub(1)
  → Keys exclusively owned by "sess_b" are deleted
  → Keys shared with other sessions are retained
```

## Serialization and HA Recovery

### OpLog Serialization (`MetadataPayload`)

Uses `struct_pack::compatible<std::string, 2>` for `session_id`,
`struct_pack::compatible<uint64_t, 3>` for `object_checksum` (Mooncake-specific),
and `struct_pack::compatible<uint32_t, 4>` for `session_refcount` — versioned
optional fields that old code will simply ignore when deserializing
(forward-compatible) and new code will treat as absent (defaulting to `""`,
`0`, and `0` respectively) when reading old data.

### Persistence Paths

Mooncake uses two persistence paths for metadata:

1. **OpLog** (struct_pack-based serialization via `MetadataPayload`) — used for
   HA replication. The `session_id`, `object_checksum`, and `session_refcount`
   fields are all carried as `struct_pack::compatible` fields, as described
   above.

2. **Catalog-backed snapshot** (via `StandbyObjectMetadata`) — used for standby
   snapshot export/import. Uses the same `struct_pack::compatible` mechanism.

Mooncake does **not** use the fork-based msgpack `MetadataSerializer` mechanism
that existed in mooncake-test. The `MetadataSerializer::SerializeMetadata` /
`DeserializeMetadata` functions and the msgpack array format are not present in
the Mooncake codebase.

### Standby Snapshot (`StandbyObjectMetadata`)

Uses `struct_pack::compatible<std::string, 2>` for `session_id`,
`struct_pack::compatible<uint64_t, 3>` for `object_checksum` (Mooncake-specific),
and `struct_pack::compatible<uint32_t, 4>` for `session_refcount` — same
forward-compatible mechanism as `MetadataPayload`. The `ToStandbyMetadata()`
conversion in `metadata_store.h` propagates the values.

### Catalog-Backed Snapshot Provider

`DeserializeStandbyObjectMetadata` in
`catalog_backed_snapshot_provider.cpp` reads from the same struct_pack format
as `StandbyObjectMetadata`. The `object_checksum` field is read and discarded
(standby does not use it). `session_id` and `session_refcount` are read and
propagated to the restored metadata.

### Restore Paths

| Path | Code Location | Restore Logic |
|------|-------------|---------------|
| Snapshot deserialize | `DeserializeShard` in `master_service.cpp` | `metadata_ptr->session_refcount_.load()` → `it->second.session_refcount_.store(...)` |
| Standby snapshot | `RestoreFromStandbySnapshot` in `master_service.cpp` | Uses `accessor.Publish(...)` to restore metadata, then sets `metadata.session_id` and `metadata.session_refcount_`. Falls back to `session_refcount.value_or(!session_id.value_or("").empty() ? 1 : 0)` |
| SessionKeyIndex rebuild | `RebuildSessionKeyIndex` | Unchanged — reconstructs forward index from `metadata.session_id`, does not touch refcount |

## C API and Python SDK

### C API (`store_c.h`)

```c
struct mooncake_replicate_config {
    ...
    const char* session_id;  // NULL or "" = no session association
};
```

The C API uses `const char*` for session_id. The `to_replicate_config` function
in `store_c.cpp` converts it to `std::optional<std::string>` for the C++
`ReplicateConfig`.

### Python SDK (`store_py.cpp`)

`ReplicateConfig::session_id` is exposed to Python as a `std::optional<std::string>`
field via pybind11 `def_readwrite`. Python callers can set it directly:

```python
config = store.ReplicateConfig()
config.session_id = "sess_abc123"
```

## Rejected Alternatives

### Alternative A: Shared Blocks Set `session_id = ""`

**Idea**: SGLang marks prefix-shared blocks with `session_id = ""` so they are
exempt from session-level cleanup.

**Rejected because**:
- RadixTree within a single SGLang process can identify shared nodes (refcount
  > 1), but **cross-process sharing cannot be determined locally**. SGLang-A's
  RadixTree does not know that SGLang-B is using the same prefix block.
- Would require a cluster-wide coordination protocol to determine sharing, which
  is exactly the problem we are solving.
- Shared blocks would rely solely on eviction for cleanup, with no timely
  reclamation.

### Alternative B: Lease-Based Protection in `RemoveBySession`

**Idea**: `RemoveBySession` uses `force=false`, so keys with active leases
(during a `Get` operation) are protected.

**Rejected because**:
- Lease TTL is short (default 10 seconds). A session B that prefetched block X
  and is now doing GPU computation has no active lease on X, but still needs X
  for future requests.
- Lease does not express session-level intent ("I will need this block for the
  lifetime of my session"). It only protects against concurrent read/delete
  races.
- Would cause false deletions: session B's lease on X expires, session A's
  `RemoveBySession` deletes X, session B's next prefetch misses.

### Alternative C: Write-Back Path Registration

**Idea**: When SGLang-B's HiCache writes back to L3 and finds block X already
exists (dedup), register session B's reference at that point.

**Rejected because**:
- HiCache's `write_back` policy only writes to L3 when data is evicted from L1.
  A session that only reads (never triggers L1 eviction) would never register.
- `write_through_selective` only writes back after the access frequency exceeds
  a threshold — cold read sessions are missed.
- `write_through` writes back on every access, but this is just one of three
  policies and may not be the default.

### Alternative D: Multi-Session Reverse Index with Active-Session Check

**Idea**: Maintain the bidirectional map (`key_sessions_`), and in
`RemoveBySession`, for each key, check if any other session in
`key_sessions_[key]` is still active via `ActiveSessionTracker`.

**Rejected because**:
- `ActiveSessionTracker` is updated via best-effort HTTP heartbeats. If a
  heartbeat is delayed, a live session might appear expired, causing premature
  deletion.
- Requires `IsActive(session_id)` query per remaining session per key — O(sessions
  × keys) complexity in the delete path.
- The counter approach is self-contained: the refcount is the authoritative
  truth, not dependent on the accuracy of a secondary tracking system.
- Higher memory overhead: `unordered_set<string>` per key vs. one `uint32_t`.

### Alternative E: Per-Key Session ID in Batch Reads

**Idea**: Instead of a session-ID array, pass a per-key `session_id` vector so
each key in the batch is associated with exactly one session.

**Rejected because**:
- SGLang's HiCache prefetch path does not track which session triggered each
  individual key lookup. The RadixTree traversal collects a set of keys to
  prefetch and a set of active sessions, but the per-key attribution is not
  available.
- Would require SGLang to maintain per-key session attribution, adding
  complexity to its own code and coupling the two systems more tightly.
- The conservative array-based approach (register all non-owner sessions) is
  safe — it may over-count refcounts, but over-counting only delays cleanup
  (the key is retained longer than strictly necessary), which is a strictly
  better failure mode than premature deletion.

## Edge Cases and Safety

### Refcount Underflow

If due to a bug or race the refcount drops below 0, `fetch_sub` wraps to
`UINT32_MAX`. The `prev <= 1` check catches `prev == 0` (deletes the key,
resets counter to 0). For `prev == UINT32_MAX`, the key would be retained
indefinitely. This is a safe failure mode: data is not deleted prematurely;
it will eventually be reclaimed by eviction.

### Concurrent `GetReplicaList` and `RemoveBySession`

- `GetReplicaList` holds `MetadataAccessorRO` (read lock).
- `RemoveBySession` acquires `MetadataAccessorRW(this, MakeObjectIdentity(user_key, tid))`
  (write lock) per key.
- The `shared_mutex` ensures mutual exclusion: the `fetch_add` in
  `GetReplicaList` either completes before or after the `fetch_sub` in
  `RemoveBySession`.
- If `GetReplicaList` runs first: refcount goes 1→2, then `RemoveBySession`
  does 2→1, key is retained. Correct.
- If `RemoveBySession` runs first: refcount goes 1→0, key is deleted, then
  `GetReplicaList` returns `OBJECT_NOT_FOUND`. SGLang treats this as a cache
  miss and recomputes. Correct — no data corruption.

### Object Deleted by Eviction While Referenced by Another Session

- Eviction calls `EraseMetadata`, which unregisters the owner session's forward
  mapping but does not decrement refcount (object is being destroyed).
- When the other session's `RemoveBySession` runs later, it looks up the key,
  finds metadata gone (iterator past end → `continue`), skips the decrement,
  and calls `UnregisterKey` to clean the forward index.
- No crash, no underflow. The key's forward-index entries are cleaned up lazily.

### Master Restart (Snapshot Recovery)

- All `session_refcount_` values are restored from the snapshot.
- `RebuildSessionKeyIndex` reconstructs the forward index (`session → keys`)
  from `metadata.session_id`.
- **Limitation**: the forward index only knows about the **owner session**. If
  session B had registered a cross-session reference (via `GetReplicaList`)
  before the crash, that forward-index entry (`B → key`) is not reconstructed
  because `metadata.session_id` is still A.
- **Impact**: After restart, `RemoveBySession(A)` will see refcount > 1 (restored
  from snapshot) and retain the key. But `RemoveBySession(B)` will not find key X
  in `session_keys_[B]` (forward index entry lost), so it won't decrement the
  refcount. The refcount will be permanently 1 too high.
- **Mitigation**: The extra refcount prevents premature deletion — safe failure
  mode. The key will eventually be reclaimed by eviction. A future enhancement
  could persist the forward index alongside the snapshot.

### `session_id` Empty (No Session Association)

Objects with empty `session_id` have `session_refcount_ == 0`. They are:
- Never incremented by `GetReplicaList` (the `!session_id->empty()` check skips
  them).
- Never touched by `RemoveBySession` (not in any `session_keys_` entry).
- Managed solely by the eviction policy — same as before this design.

## File Change Summary

| File | Changes |
|------|---------|
| `mooncake-store/include/master_service.h` | `ObjectMetadata::session_id` is `std::string` (empty = none, set via `BeginSessionTag`/`CommitSessionTag`); `session_refcount_` atomic counter; `SessionKeyIndex` uses `std::string` SessionId; `GetReplicaList` gains `std::optional<std::string>`; `BatchGetReplicaList` gains `std::optional<std::string>`; `RemoveBySession` takes `const std::string&` |
| `mooncake-store/include/rpc_service.h` | `WrappedMasterService::GetReplicaList` gains `std::optional<std::string>`; `BatchGetReplicaList` gains `std::optional<std::string>`; session management APIs use `std::string` |
| `mooncake-store/include/metadata_store.h` | `MetadataPayload` and `StandbyObjectMetadata` gain `struct_pack::compatible<uint64_t, 3> object_checksum`, `struct_pack::compatible<std::string, 2> session_id`, and `struct_pack::compatible<uint32_t, 4> session_refcount`; `ToStandbyMetadata()` propagates them |
| `mooncake-store/include/replica.h` | `ReplicateConfig::session_id` is `std::optional<std::string>` |
| `mooncake-store/include/store_c.h` | `mooncake_replicate_config::session_id` is `const char*` (NULL or "" = none) |
| `mooncake-store/src/master_service.cpp` | `GetReplicaList` embeds single-session refcount inc; `BatchGetReplicaList` embeds single-session refcount inc; `InsertMetadata` initializes refcount=1 after `BeginSessionTag`; `RemoveBySession` rewritten to decrement-and-conditionally-delete; all serialization paths write `session_id` as string and `session_refcount` as uint32 via struct_pack compatible fields |
| `mooncake-store/src/rpc_service.cpp` | `WrappedMasterService` implementations pass `session_id` through; session management APIs use `std::string` |
| `mooncake-store/src/ha/snapshot/catalog_backed_snapshot_provider.cpp` | `DeserializeStandbyObjectMetadata` reads `session_id` and `session_refcount` from struct_pack compatible fields |
| `mooncake-store/src/master_admin_service.cpp` | HTTP API uses `session_id` as string directly (no `std::stoull` conversion) |
| `mooncake-store/src/store_c.cpp` | Converts `const char*` to `std::optional<std::string>` |
| `mooncake-integration/store/store_py.cpp` | Exposes `session_id` field on `ReplicateConfig` to Python |
