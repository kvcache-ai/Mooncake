# Allocation Reclamation Lifecycle

Store-RS separates a logical route update from physical allocation release.
Route authorities own reference information. The storage owner owns its local
allocator, CLOCK tracking and pending reclamation state.

## Allocation identity

An allocation is identified by its owner runtime, segment name, byte offset,
length and allocation generation. Reservations and replica routes carry an
optional `allocation_id`; a nonzero value fences one lifetime of a byte range.
Cleanup for an old generation cannot release a newer allocation at the same
offset. Missing or zero IDs retain the legacy wire contract.

Rust callers constructing replica routes or reservations directly must
initialize the new identity field. New producers propagate the returned
reservation ID; `None` or zero is for legacy records. Generation fencing
requires cleanup participants to preserve that ID; legacy cleanup retains
its previous guarantees.

Every route producer preserves the reservation's generation, including batch
writes, restore and migration. Release checks the current allocator record;
a stale generation is not a successful claim on its replacement.

## Publication

For a lifecycle-capable owner, reserve records a pending publication in the
same critical section that allocates space. The writer writes the payload and
publishes the route through the existing CAS path. A batched tracking message
finalizes the pending identity and makes it available to CLOCK.

Finalize is asynchronous. A lost notification or an unknown publication result
leaves the owner's recovery fence intact. Publication attempts bind allocations
to an exact publisher sequence, primary incarnation, receiver set, and enrollment
permits. The owner closes receiver registration and revokes publication admission
before reclaiming an unreferenced allocation. Each receiver checks admission and
references under the same authority slot lock. Missing, restarted, or indeterminate
receivers retain the allocation for bounded recovery retries. Routine reads do
not query this ledger or scan route authorities.

Recovery prepares the initial cold backing before completing a referenced
publication. A preparation failure retains the pending identity for retry.

The deadline starts recovery rather than authorizing reuse. A retired writer
must reserve and transfer again before publishing. Existing-holder cleanup
compares the complete expected route, including publication identity, and only
removes the exact allocation. A lost prune reply can recover the pinned primary's
deletion marker through a dedicated operation; an arbitrary missing read cannot
authorize release.

Before publication, failure cleanup targets the exact prepared reservations. A
failed release is retained for retry rather than discarded. A separate known
limitation remains when a logical transport timeout precedes physical completion:
accepted work can remain pending after deferred batch cleanup while reservations
and associated buffers are released. Publication fencing does not close this
physical-I/O lifetime window; it requires a separate follow-up. Allocation-owner
backpressure remains a distinct control-plane
error: a routed writer can try another soft placement candidate without
quarantining the saturated owner. If no candidate can accept the write, the
backpressure result is preserved.

Batch publication retains an outcome for each item. A successful item is not
replayed because another item failed. Once route publication is applied or its
outcome is unknown, cleanup retains the affected allocation; a later
finalization error does not abort its tenant quota reservation as though the
publication had never happened.

## Eviction

The storage owner selects a CLOCK victim and claims its exact identity before
updating the route. The claim excludes competing reclamation of that generation.

Batch route tracking holds the allocator lock through hot and NoF CLOCK updates.
Each route's pending publication is cleared and its hot tracking is updated in
the same critical section, using the allocator-to-CLOCK lock order.

Orphan reconciliation admits pinned allocations for authority confirmation.
Confirmed absence can commit reclamation while a borrower still holds a pin;
the allocator retains the bytes and defers physical release until the final pin
drops. A live or unknown reference retains the allocation for the corresponding
tracking or retry path.

- An explicitly applied route CAS enters the local commit path. The allocator
  generation and read pins are checked before physical release.
- A CAS conflict refreshes or restores the tracked state.
- An unknown result retains the token and requires authority confirmation.
- A missing route lookup alone is not proof of absence. Historical or untracked
  candidates use the confirmation/recovery path.

Confirmation distinguishes a positive reference from absence. It uses the
current and retained authority membership according to the query contract;
transport failures, incompatible peers and membership changes cannot be
interpreted as an absent reference. Candidate processing and RPC batches are
bounded. Compatibility reconciliation can still perform local collection work;
it is not a constant-time replacement for all maintenance.

The existing CLOCK free-slot stack only manages metadata vector indexes. It
does not own physical allocation release, and remains independent of these
generation and reference checks.

Background watermark eviction groups bounded route lookups and CAS requests
for up to 256 victims. Applied items commit independently; conflicts restore
tracking and unknown outcomes retain their tokens. Counters report the replicas
and bytes actually released. The eviction and stale-reclaim cursors use distinct
runtime-derived seeds, including after a tracker rebuild. A shrink publication
refreshes active membership before ensuring the route at its new authorities.

Hits on tracked CLOCK entries update their heat and consume any pending hit
record, so a later route synchronization does not replay the same hit. Unknown
DRAM keys can retain a hit until tracking arrives. NoF reports heat only for
already tracked entries, and runtimes without NoF targets skip the NoF CLOCK.

Cold offload preparation partitions work by device admission. After one wave
settles, deferred entries are prepared again in the same invocation while work
can make progress. Ready device groups run in parallel, bounded by the runtime
offload in-flight limit. Preparation failures retain claimed work for retry;
payload pins still live through actual I/O settlement.

An active route is materialized only while all existing cold and NoF backings
are pending offload. If inline eviction cannot claim an entry, it reports no
progress and leaves the existing owner responsible for that work; it does not
force an in-flight entry back into the queue.

## Delete

An active route deletion prepares reclamation with lifecycle-capable owners,
performs the route CAS and schedules owner-local delete notifications. The
preparation and post-delete notification use the same pending queue and exact
identity. This is not an unconditional all-owner acknowledgment protocol:
legacy or unknown peers and queue limits retain the applicable compatibility
or Publication/CLOCK recovery path.

The successful route update and quota-finalization contract determine the
logical delete result. Physical release happens later, after exact reference
confirmation, the reclaim grace period and read-pin checks. Repeated
notifications are idempotent for the same identity; a new object at the same
key or physical offset is a different generation.

Due work is claimed in bounded batches. Unknown confirmation results remain
pending with retry rather than being discarded. Shutdown retains and flushes
remote notifications; reducing a notification's grace to zero does not bypass
reference or generation validation.

evict-all uses the same storage-owner eviction path and accounts for identities
already owned by pending delete processing. It does not maintain a separate
force-release state machine. An unavailable cold backend can therefore produce
no progress or a timeout instead of forcing physical release. The existing
`repaired` result field remains available and is zero on this path.

The compatibility `remove_all` entry point invokes owner-local eviction rather
than deleting the writer's previously recorded keys. The native path preserves
errors and partial-progress status. The Python cache wrapper logs failures and
returns `-1`; it does not fall back to logical deletion. A nonnegative result
counts evicted replicas, not removed logical objects. The `force` argument is
retained for API compatibility and does not bypass owner reclamation checks.

Cold-backed deletion first publishes `Deleting`/`PendingDelete` and sends the
cold target to its owner through the retained delete-notification path. The
owner drains a bounded retry queue using exact route-key confirmation. Repeated
deletes re-arm the token. Payload removal and device usage accounting share the
same cleanup implementation as indexed cold garbage collection. This queue is
runtime-local; persistent route state supplies recovery information after a
restart, rather than an in-memory token surviving the process.

An offload from local DRAM pins the allocation generation and borrows its bytes
until asynchronous I/O settles and the payload guard drops. A concurrent release
cannot make the span available for reuse while pinned. Backends that cannot
borrow retain their copy-based compatibility path.

## Compatibility and lifetime

The `allocation-lifecycle-clock-v1` capability gates the new reserve semantics.
Writers set the pending-publication flag only for owners advertising it.
Explicit legacy peers and temporarily unknown owner capabilities are distinct;
an unknown owner is not destructively downgraded to the legacy release path.

Cold-only delete notifications require an updated owner that understands the
optional cold target. Older decoders require a DRAM identity and cannot consume
that notification. The additive protobuf fields preserve decoding for existing
DRAM messages; they do not establish mixed-version cold-delete recovery.

`MetadataOnly` does not advertise the capability because owner recovery requires
the exact allocation-reference authority query. Its existing reserve and
reclaim contract remains available for bring-up and debugging.

The DRAM pending ledger has the same process-local lifetime as the allocator
and memory registration. It is not a durable route journal. Persistent cold
backings keep their backend-specific lifetime and recovery semantics; a daemon
restart does not prove that durable bytes have disappeared.

Authorities retain deletion versions in a bounded FIFO marker store. Each
authority uses at least 128 MiB; the default budget is 1% of configured storage
memory. `MC_STORE_RS_DELETE_MARKER_MEMORY_RATIO` accepts a finite ratio in
`(0, 1]`; invalid values use the default. Capacity eviction removes the oldest
marker, so this is a bounded replay fence rather than permanent history.
`version_floor` remains a legacy wire/query alias; updated peers also carry
`delete_version` through route reads and CAS replies. Rust callers constructing
`CasResult` initialize the additional optional field.

Mesh reads and read repair represent deletion as an explicit `DeleteMarker`,
not as a synthetic `ObjectRoute` in Tombstone state. A marker at the same
version wins over a live route; a strictly newer live route can supersede it.
Versioned mirror replacements carry `delete_version` separately in the control
plane, including when the receiving authority has no route or an older route.
Writer-side deletion still advances the CAS fence to `version.next()`; this
fence and allocation-generation validation are independent of the read-side
representation.

Asynchronous mirrors reject a publish older than the currently stored route or
not newer than its deletion floor. A delayed delete carrying an expected
version cannot remove a newer live route. Single-key and batch floor queries
take the highest version returned by the queried top-k authorities.

Mirrors retain a bounded FIFO of update batches and group work by authority
when draining. Under capacity pressure the queue drops its oldest pending
batch. Route application enforces generation and version checks; the queue
does not replace individual pending keys by version. Mirroring is best effort;
authoritative publication is not a mirror ACK.

Readable-membership filtering prunes authority-local unreadable routes. It is
not a distributed logical delete: a delayed first mirror can become visible
after the filter is cleared at an authority that had not yet seen that route.
An ignored regression records this existing window; ordinary filter-cleanup
tests first wait for initial mirrors and do not claim to fence that window.

The standalone client exposes the offload limits through flags and environment
variables. Flags take precedence over environment values; omitted values use
`ColdTierRateLimitConfig` defaults.

| Flag | Environment variable |
| --- | --- |
| `--cold-tier-offload-runtime-max-in-flight` | `MC_STORE_RS_COLD_TIER_OFFLOAD_RUNTIME_MAX_IN_FLIGHT` |
| `--cold-tier-offload-device-max-in-flight` | `MC_STORE_RS_COLD_TIER_OFFLOAD_DEVICE_MAX_IN_FLIGHT` |
| `--cold-tier-offload-runtime-ops-per-sec` | `MC_STORE_RS_COLD_TIER_OFFLOAD_RUNTIME_OPS_PER_SEC` |
| `--cold-tier-offload-device-ops-per-sec` | `MC_STORE_RS_COLD_TIER_OFFLOAD_DEVICE_OPS_PER_SEC` |
| `--cold-tier-foreground-offload-kick-batch` | `MC_STORE_RS_COLD_TIER_FOREGROUND_OFFLOAD_KICK_BATCH` |
| `--cold-tier-offload-poll-interval-ms` | `MC_STORE_RS_COLD_TIER_OFFLOAD_POLL_INTERVAL_MS` |

## Validation entry points

The client test modules cover publication failures, partial batch publication,
reservation rollback and retryable reclaim. Route tests exercise delayed
mirrors and deletion-floor aggregation through authority and directory entry
points.

The `mooncake-store-e2e` executable supports
`MC_STORE_RS_E2E_LIFECYCLE_ONLY=1` for remote put/readback/delete, owner usage
recovery and replacement at a reused offset. It requires a working native TENT
backend and a dedicated Redis instance; compiling this scenario does not prove
native execution.

The `multi_tenant_perf` Criterion target includes local and remote
`put_allocation_lifecycle` groups. Each measured put starts with fresh metadata,
clients and allocator state, followed by the same warmup. These TestTransport
recipes exercise CPU lifecycle paths, not native network performance. Warmup
can leave asynchronous finalize work in flight, so the current recipes are
sequence measurements and smoke checks rather than isolated policy or pure
put-latency comparisons. The native write-benchmark mode also reuses its
clients across batch sizes and has the same sequence-measurement boundary.
Performance reports must separately lock source and executable hashes,
dependencies, toolchain, configuration, full command and measurement window.
