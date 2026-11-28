# Bounded weight streaming

The existing weight planner, `MooncakeTransferEngineReader`, and
`InplaceWeightResharder` already provide planning, fresh physical binding, and
transfer lifetime management. Streaming adds source backpressure; it does not
introduce a new local transport, weaken the typed layout contract, or schedule
overlapping source and target writes automatically.

## Shared coordination

`mooncake.reshard.weight.streaming.WeightStreamCoordinator` is a framework-neutral
metadata coordinator. Host it on a serialized event loop or wrap it in the
framework's actor mechanism. It requires neither Ray nor Torch. Configure the
consumer identities and writer world once. Each writer acquires its generation,
publishes a header and immutable chunk descriptions, and waits for source-slot
reuse before replacing data in a ring slot. The default is two slots; a caller
can select another positive `slot_count` when constructing the coordinator.

A consumer calls `ack_chunk` only after the transfer **and consumption** of that
chunk have completed. A transfer returning does not establish completion of a
subsequent asynchronous GPU unpack or collective. The coordinator trusts this
caller-supplied completion boundary; it does not establish CUDA fences or retain
GPU allocations. Existing Reader allocation guards and the framework must retain
the actual owners. Unknown completion must not produce an ACK. Source failure
blocks slot reuse and wakes pending waiters without making the memory reusable.

`acknowledge` is the separate final generation ACK. It follows all chunk ACKs and
framework post-load completion. A new generation cannot start until all
consumers complete the previous generation. Empty writers still publish headers.
The ring bounds payload slots, not the metadata history: metadata scales with
chunk count and is cleared on the next completed generation transition.

The same coordinator composes with both the same-participant inplace entry point
and ordinary Reader execution between different participant sets. The inplace
entry point retains its same-participant precondition; streaming does not relax
it. Logical plan cache hits still require fresh runtime binding and guards.

## Optional source packing

`mooncake.reshard.weight.stream_chunks` provides `plan_chunks`, `chunk_nbytes`,
`chunk_table`, `pack_bucket`, and `iter_packed_chunks`. Canonical shard inputs are
dataclass instances exposing `tensor_id`, `tensor`, `global_shape`, and
`global_offset`; dataclass replacement preserves adapter-specific fields.
Tensors use the Torch tensor interface, but Torch is passed by the caller rather
than imported by the SDK module. Packing supports canonical vectors and matrices,
splits oversized rows, preserves storage bits, and uses the existing
`TiledCOOLayout` for chunk metadata. The optional grouping callback is owned by
the adapter; the SDK does not infer model layers from parameter names.

`partition_source_views(shard, axis, partitions)` optionally divides a canonical
source into aliasing `narrow` views before chunk planning. The adapter supplies
the axis and partition count. Global boundaries are `extent * i // partitions`,
so uneven extents and partial local shards retain exact, non-overlapping
coverage; empty intersections are omitted. The count must be a positive integer
no greater than the global axis extent. One partition returns the original
shard. Views preserve dtype, global geometry, payload bytes, and adapter fields
such as owner references, without allocating tensor storage.

```python
from mooncake.reshard.weight.stream_chunks import iter_packed_chunks, plan_chunks

chunks = plan_chunks(canonical_shards, max_bytes=slot_capacity)
for ordinal, chunk, packed in iter_packed_chunks(
    chunks,
    registered_gpu_slots,
    acquire=wait_for_slot_consumption,
    ready=fence_producer_copies,
    torch_module=torch,
):
    publish_ready_chunk(ordinal, chunk, packed)
```

The acquisition callback blocks before overwrite. The ready callback runs before
publication. Normal iterator exhaustion waits for consumption of all remaining
slots; early close or exceptions do not acknowledge unfinished work. The caller
must propagate failure and retain allocations. Empty writers need no slots.
Already suitable live source allocations can bypass packing and use the existing
Reader directly; inplace execution is not forced through a staging ring.

## Adapter boundary and validation

ROLL retains Ray metadata RPCs, model conversions, gate/up and expert storage
mapping, NCCL group management, producer/consumer CUDA fences, and final post-load
ACK. Its coordinator and chunk helpers delegate to these SDK modules. Its existing
tiled-COO consumer still uses the public TE executor; it has **not** been converted
to the typed Reader. The latter requires complete valid placement manifests, and
a cropped tiled batch must not be disguised as a complete typed placement.

CPU tests cover the coordinator and packing plus composition with the actual
public Reader and inplace entry point. They verify byte results, changed target
addresses with cache hits, consumption-fence gating, and unknown-completion
ownership. They use a fake hardware engine and do not validate GPU, RDMA, NCCL,
model-size scaling, or performance.
