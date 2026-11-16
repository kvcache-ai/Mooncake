# PiecePut (C++ Store API)

PiecePut assembles disjoint contributions from independent producers into one
ordinary Store object. Each producer knows the final byte length, its own
contiguous destination range, and a framework-supplied shared `write_id`.
Different computations must use different write IDs, even when reusing a key.

## Calling the API

Source buffers must be registered as for ordinary Put and remain alive until
the synchronous call returns. Multiple source slices are concatenated at the
item's destination offset; the API does not derive tensor layouts.

```cpp
PiecePutItem item{
    .key = "kv/request42/block7",
    .total_size = 8192,
    .offset = 4096,
    .write_id = "request42/attempt1",
    .slices = {{registered_source, 4096}},
};
auto result = client.PiecePut(item, ReplicateConfig{});
if (result && result->pending_commit) {
    // Data has finished. Confirm metadata only; do not transfer the piece again.
    auto confirmed = client.CommitPiecePut(
        std::span<const PiecePutCommitHandle>(&*result->pending_commit, 1));
}
```

`BatchPiecePut(span<const PiecePutItem>, config)` returns one result per input
and processes large batches in bounded internal windows. Per-key hints are
selected using the original input index. A single item wraps the same batch
implementation.

| Result | Meaning |
| --- | --- |
| expected error | The operation failed; do not use a late Start address. |
| piece_completed=true | Master confirmed this contribution. Other producers can still fail the whole assembly. |
| object_published=true | Master confirmed the complete object; ordinary retention/deletion rules then apply. |
| pending_commit present | All target writes succeeded but Finish was not confirmed. Commit retries metadata only. |

The SDK retries uncertain metadata RPCs with the original operation identity.
It submits the data stage at most once per logical call. Calling PiecePut again
creates a new operation, not a resumable retransmission. An unknown Start has
no CommitHandle; the incomplete round expires under ordinary write timeouts.

## Master ownership and common failures

The first Start allocates fixed MEMORY replicas for the full object.
An external `PiecePutManager` records only active ranges and their completion.
Ordinary object metadata owns the replicas and allocations. Neither
`ObjectState`, `ObjectMetadata` nor Namespace gains a PiecePut field.

All replicas stay PROCESSING until the confirmed, non-overlapping ranges cover
the object. Finish publishes the whole object and removes the active record
under one key write guard. Standard Get/BatchGet/Exists expose no partial
assembly. There is no per-piece read while writing.

A failed unfinished contribution, expired deadline or lost fixed target aborts
the whole round. Advertised allocations enter the existing discard queue for
its configured grace period; that queue is not a proof of arbitrary delayed
DMA draining. Supported transport and deployment timeouts must fit the existing
lifetime contract. An already confirmed contribution is not revoked by a late
failure message.

Ordinary PutEnd/PutRevoke cannot settle an active round. Ordinary Upsert retains
its explicit preemption behavior. A new PiecePut write ID does not implicitly
preempt an active older round. Old Finish credentials are rejected after object
replacement or Master restart. After publication, confirmation is limited to
the still-present object write; there is no permanent operation history.

The Manager uses the existing per-key write guard followed by a short map lock.
It has at most 1024 active rounds and 128 contributions per round; it does not
add a background thread. Completed objects retain no piece geometry. Applications
that previously read by piece index must retain offsets and use existing range
reads instead.

## Initial scope

Fixed one or more MEMORY replicas use existing native range-scatter transfer.
CPU/TCP v2 is the validation backend. Native RDMA/GPU paths share the transfer
interface but need hardware validation. Legacy TCP v1 and the current TENT Store
adapter are rejected because their completion/lifetime contracts differ.

HA/OpLog, direct SSD/DFS/NoF assembly, CXL and managed weight groups are rejected
before allocation. The cross-key `prefer_alloc_in_same_node` batch policy is
also rejected; independent producer batches cannot enforce it consistently.
A fully published object still uses ordinary offload hooks.
This feature currently provides C++ APIs, not Python bindings. Whole-object
checksums are not synthesized from independent contributions. Clients with
`MOONCAKE_STORE_CHECKSUM=1` reject PiecePut before Start instead of silently
skipping the enabled checksum contract. A participating producer invalidates its
local hot cache before writing and after confirmation (including pending
confirmation); this does not introduce cross-client cache invalidation.

No TransferEngine protocol, Segment lifetime model or ordinary write-proof
protocol is added. This version is under validation; see the external test
report for exact build/test evidence and measured configuration.
