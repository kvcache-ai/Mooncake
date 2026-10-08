---
orphan: true
---

# Session-aware object membership and soft pin

KV sessions associate an application conversation with existing Store objects.
Multiple clients can use the same session, and an object can belong to multiple
sessions. Applications supply a unique string `session_id` per conversation;
all DP ranks share that ID. There is no separate create operation or handle.
IDs use the client's existing tenant context (`default` in ordinary deployments).
IDs must be nonempty, at most 4096 bytes, and contain no NUL bytes.

Sessions are independent of `group_id` and ranged transfer sessions
(`batch_put_session_start`). They do not change object placement, group objects
for atomic eviction, or grant read leases. The application's token/prefix/SWA
metadata remains outside Store.

## Lifecycle and retention

A successful attach to an existing object, or admission of a tagged write,
creates an unknown session automatically. New sessions are unpinned. Repeated
registration is additive and idempotent; it preserves existing pin state.
Attaching only missing keys, an empty batch, or a rejected object update does
not leave an empty session. Membership can include allocated objects whose
writes have not finished; membership alone does not promise readability.

When the last member disappears, Store removes the session **and its pin
state**. This includes object eviction, deletion, failed-write cleanup, and
an update that retains no members. Removing one replica keeps membership if
the object still has another replica. A later write or attach using that ID creates a new,
unpinned session. Session metadata does not accumulate after all its KV is gone.

Pin/unpin use existing sessions only and have no TTL. Pinning a missing session
returns `SESSION_NOT_FOUND`; first register KV, then pin. A pinned session gives
its current and subsequently registered members soft-pin priority. An object
is effectively soft pinned if any associated session is pinned **or** its
native TTL soft pin is active. Unpin does not remove membership or cancel
another session's pin.

Membership controls are attach (add associations), update (retain the complete
current-context set), and close (remove all associations). There is no separate
per-key detach operation; object cleanup removes its own associations automatically.

Normal eviction prefers eligible unpinned objects. The native
`allow_evict_soft_pinned_objects` setting controls pressure fallback; its
default `true` permits eviction of soft-pinned objects. Hard pins, leases,
busy replicas, and group policies retain their native behavior. Session pin
is not a residency or disk durability guarantee and does not fetch data.

Close removes current associations without deleting shared object data. It
is idempotent and stores no tombstone. An attach or tagged write arriving after
close may recreate the session; an already admitted PutEnd does not register
it again. Callers should stop submitting session work before close. Conflicting
control commands have no whole-session ordering guarantee. Soft-policy callers
may overlap pin/unpin/update with writes and accept approximate retention.

If update/close times out after processing some keys, the removed associations
are not rolled back. Callers may continue work or retry. Only callers requiring
an exact retained set need to pause session work and retry the same complete
keep set (or close); retries are idempotent while writes/controls are paused.

The registry is runtime-only and excluded from snapshots and HA logs. After
restart/promotion, attach available keys (or tag subsequent writes), then
restore desired pins. Client disconnection does not itself close a shared
session, but loss of its last object removes the session normally.

## Python API

Use an initialized `MooncakeDistributedStore` real client. Client and master
must both support this feature; these controls are not exposed by the Python
IPC dummy-client interface.

| Method | Result |
| --- | --- |
| `pin_kv_session(session_id: str)` | `0` on success |
| `unpin_kv_session(session_id: str)` | `0` on success |
| `close_kv_session(session_id: str)` | `0`, including absent session |
| `update_kv_session(session_id: str, keep_keys: list[str])` | `0`; retain only existing associations in the complete keep set |
| `get_kv_session(session_id: str)` | `KvSessionInfo(session_id, pinned, member_count)` |
| `list_kv_session_keys(session_id, cursor="", limit=1024)` | `KvSessionPage(keys, next_cursor)` |
| `attach_kv_session(session_id, keys)` | Per-key status; creates session on first successful association |

Control failures raise `RuntimeError`. Attach batches return `0` per
successful key or a negative code per failed key; they are not cross-key
transactions. Important codes: `OBJECT_NOT_FOUND=-704`,
`SESSION_NOT_FOUND=-1800`, `SESSION_LIMIT_EXCEEDED=-1802`.
Invalid IDs/shapes/limits return `INVALID_PARAMS`. Old masters reject the new
RPCs instead of silently reporting successful session control.

Pagination uses an exclusive lexical key cursor; continue with `next_cursor`
until empty. Enumeration is weakly consistent, grants no lease, and can fail
with `SESSION_NOT_FOUND` if the last object disappears between pages.

## Shrinking a session after compaction or rollback

After a context change, call `update_kv_session(session_id, keep_keys)` with
**one complete set of physical object keys** to retain. The operation replaces
membership A with its intersection with keep_keys; it does not attach new keys,
create objects, or delete payloads. Duplicate keys have no additional effect;
unknown or unassociated keys are ignored. A missing session is a successful
no-op. An empty intersection removes the session and its pin state, just like
last-member eviction; a later tagged write starts unpinned again.

A surviving session preserves its pin flag. Removed objects lose only this
session's association and soft-pin contribution; other sessions, native TTL
pins, hard pins, and groups are unaffected. New tagged writes to a surviving
pinned session inherit that pin as before.

Update visits keys incrementally, without holding the registry mutex for the
whole session. It copies at most 256 keys (or the configured batch limit, if
smaller), releases the index lock, then resolves each unwanted key under the
existing metadata shard lock. Objects evicted/deleted in between are skipped;
update never dereferences a stored pointer to another object's membership.
Readers may observe intermediate membership counts. Close uses the same walk
with an empty keep set. Neither operation is a whole-session transaction.

Invalid IDs or an input list longer than `MC_KV_SESSION_MAX_MEMBERS` are rejected
before any membership changes (duplicate entries count toward this input limit). Unlike
attach, the full keep set is not limited by `MC_KV_SESSION_MAX_BATCH`.
**Do not split it into multiple update calls**: successive intersections would
remove members retained only by another chunk or another DP rank.

Update may run concurrently with inference, registration and other controls when
used as a best effort soft-retention hint. A late attach/tagged write can add
removed keys again. Registrations overlapping the paged walk may be visited or
missed, and concurrent updates need not leave either caller's exact keep set.
If the session becomes empty, its pin is lost and later registration is unpinned.
The existing index/object locks remain necessary for memory safety.

For an exact retained set, callers instead finish prior writes/registrations and
pause new session work until update succeeds. Store does not enforce this
optional scheduling discipline or keep a generation. Eviction and cleanup may
still proceed. Repeating the same update is idempotent while writes/controls are
paused, including recovery from a lost RPC response.

```python
# All prior writes/attaches for B have completed. Keep the current context only.
store.update_kv_session(b, ["shared-prefix", "block-3"])
# Other B-only objects remain cached but lose B's soft-pin contribution.
```

## Tagged writes

`ReplicateConfig.kv_sessions` is an optional `list[list[str]]`, one inner list
per key (including single-key writes). Omitted tags and empty inner lists add
nothing; they never clear membership. Repeated tags are deduplicated.

Tags flow through Put, Upsert, batch, buffer, tensor, and ranged-write APIs
accepting `ReplicateConfig`. TP helpers expand tags alongside physical keys.
Registration happens during write admission, before readability, so existing
pinned sessions cover new objects without a post-write attach gap. The first
object of a new session starts unpinned. Deduplicated Put still adds membership;
Upsert preserves membership through successful size-changing reallocation
and preemption of an unfinished write. Rejected group, busy-replica, lease,
and leased-replacement allocation checks do not add memberships.
Accepted membership on an existing object is not rolled back if its update
later fails; failed new objects lose membership when metadata is removed.

```python
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
assert store.setup(
    "127.0.0.1", "P2PHANDSHAKE", 64 << 20, 16 << 20,
    "tcp", "", "127.0.0.1:50051",
) == 0

a, b = "conversation-a", "conversation-b"  # Application-supplied unique IDs.
config = ReplicateConfig()
config.kv_sessions = [[a]]
assert store.put("shared-prefix", b"kv-payload", config) == 0
store.pin_kv_session(a)
assert store.attach_kv_session(b, ["shared-prefix"]) == [0]
store.pin_kv_session(b)
store.unpin_kv_session(a)  # B still gives the shared object soft-pin priority.
store.close_kv_session(a)  # Does not delete data or B's association.
config.kv_sessions = [[b], [b]]
assert store.put_batch(["block-2", "block-3"], [b"kv2", b"kv3"], config) == 0
assert store.get_kv_session(b).member_count == 3
store.close_kv_session(b)
store.close()
```

## C++ and RPC

`Client` and `MasterClient` expose `SetKvSessionPin`, `CloseKvSession`,
`UpdateKvSession(session_id, keep_keys)`,
`GetKvSession`, `ListKvSessionKeys`, and
`AttachKvSession(session_id, keys)`. Return values use
`tl::expected<T, ErrorCode>`; membership batches return a vector of results.

Tagged writes use `PutStartWithKvSessions`, `BatchPutStartWithKvSessions`,
`UpsertStartWithKvSessions`, and `BatchUpsertStartWithKvSessions`. Untagged
writes retain native RPC names and compatible config encoding. Separate tagged
RPCs prevent an older master from accepting payloads while ignoring tags.

## SGLang integration

1. Pass the request's session ID directly; there is no Store session creation
   or handle exchange. All DP ranks use the same string in the same Store/tenant.
2. Map radix token ranges to stable physical Store keys. Tag every actual
   Full/SWA/checkpoint component written, with all associated session IDs.
3. On prefix hits, including L1-only hits, attach the session to known remote
   keys. Batch/deduplicate updates. Missing keys remain cache misses; later
   writeback supplies the tags. Merge rank additions, not local snapshots.
4. Keep L1 and remote pin independent. Remote pin covers all registered SWA
   checkpoints; prefetch still selects the required final window and components.
   If all remote members disappear, registration starts unpinned again; the
   controller decides whether to pin again after new registration.
5. Local radix eviction/split does not remove remote ownership. Global close
   stops new session work and clears associations without deleting shared KV.
6. For compaction/rollback, the controller calls SGLang's
   `update_session(session_id, request)` as a best effort retention hint; it may
   overlap inference, writes, and registration. Late completion may reattach
   removed keys. SGLang resolves the retained context with the inference
   tokenizer/template, reuses radix matching/splitting to trim local
   associations, and sends one complete set of physical keep keys covering all
   DP ranks to `update_kv_session`. Store receives
   keys, never raw request text. Keep valid checkpoints on the retained SWA
   prefix; the prefetch window is a separate decision. Keys must also cover
   retained remote objects whose L1 nodes have already been evicted.
7. After recovery, rebuild associations by attaching available keys or tagged
   writeback, then restore desired pins. Store cannot reconstruct token order
   or missing checkpoints.

## Bounds and costs

Master startup accepts positive-integer environment overrides:

| Variable | Default | Meaning |
| --- | --- | --- |
| `MC_KV_SESSION_MAX_SESSIONS` | 100000 | Active sessions in this registry |
| `MC_KV_SESSION_MAX_MEMBERS` | 1000000 | Members per session and entries per complete update |
| `MC_KV_SESSION_MAX_PER_OBJECT` | 100000 | Session tags per object/request |
| `MC_KV_SESSION_MAX_BATCH` | 4096 | Membership batch/page size |

The per-object default allows every session under the default global limit to
share one prefix. It is an operational resource bound, not a protocol limit:
each association consumes master memory, and an unpinned object's eviction
check can scan all its sessions. Keep finite deployment-specific limits to
bound that cost. Raising the global session limit may also require raising the
per-object limit. Membership lookup/removal uses a hash set; repeated attaches
do not linearly scan all sessions already associated with the object.

The registry owns its session map and a mutex protecting short index operations:
lookup, bounded-page enumeration, and adding/removing one object's associations.
There is no separate `KvSessionDomain` and no cross-object membership pointer in
the session index; each record stores an ordered set of keys. ObjectMetadata's
existing shard lock protects its local session-reference hash set. A shared
registry reference keeps automatic cleanup safe during master destruction.

Pin/unpin looks up one session in O(log S) and stores an atomic boolean.
Eviction checks load these flags under the existing object lock without taking
the registry mutex. Relaxed atomics are sufficient for a soft retention hint;
this does not make the maps or membership sets safe to access without their locks.
Lock order is metadata shard then registry, never registry then metadata shard.

Update builds an O(K) temporary keep set, then update/close enumerate bounded
pages and visit objects one by one. Index and object locks are released between
keys/pages, allowing ordinary eviction and other session operations to proceed.
They remain synchronous RPCs and may be slow for large sessions, but do not hold
one registry lock for the complete walk. Pin/unpin changes one atomic flag;
concurrent commands take effect in execution order. Writers need not drain for
best effort pin/update; exact pruning and final close require caller coordination.
Automatic cleanup removes empty sessions as before. Untagged objects skip session
lookups. Workload-scale performance remains to be measured.

Large keep sets must also fit the RPC transport message limit; this version has
no chunked update protocol or global L1/Store transaction. SGLang integration is
a design contract, not implemented here.
