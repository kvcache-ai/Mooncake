# Mooncake Reshard

`mooncake-reshard` defines framework-neutral contracts, address-free N-D
logical planning, runtime binding, Store-backed snapshots, and execution
adapters for reusable runtime resources.

Framework-owned adapters inspect framework runtime objects, normalize
framework-specific values, and construct typed canonical manifests. Mooncake
core accepts only those typed values; it does not import or inspect framework
objects or accept alternate field names or duck-typed records.

The public Python API is split by responsibility:

- `mooncake.reshard.contracts` exposes `ResourceManifest`,
  `PlacementManifest`, and `RuntimeBindingManifest` as structural `Protocol`
  contracts for resource-neutral identity and lifecycle;
- `mooncake.reshard.transfer_engine` owns resource-neutral physical batches,
  completion tracking, pending-resource quarantine, and registration leases;
- `mooncake.reshard.weight` defines model-weight placement, runtime-binding
  input contracts, N-D planning, Store, and Transfer Engine adapters;
- `mooncake.reshard.kv_cache` defines KV-cache topology, placement, runtime
  binding, serialization, and logical transfer planning.

## Weight Placement Model

`WeightPlacementManifest` describes one complete, address-free global logical
placement of a model-weight generation. It contains:

- a `ParallelTopology` with TP, PP, EP, and DP sizes plus the selected
  participants;
- per-tensor `SplitAxis(kind, dim)`, `ReplicatedAxis(kind)`, and
  `OwnershipAxis(kind)` entries that distinguish logical sharding, complete
  replicas, and ownership;
- canonical `TensorDescriptor` values whose only shard representation is
  `shard_dims`;
- one `WeightPlacementPart` for every selected participant;
- canonical global tensor descriptors and N-D logical fragments;
- a placement ID and digest computed after the complete part set validates.

`ParallelTopology.world_size` is the selected participant count. It is not
inferred from `tp_size * pp_size * ep_size * dp_size`: parallel axes may share
workers, and a placement may select one complete DP replica while retaining the
runtime's declared `dp_size`. A tensor that declares independent `SplitAxis`
values must provide the rank combinations needed to prove its axis-to-dimension
splits.

Framework adapters first construct address-free `WeightPlacementPart` values.
A collection barrier assembles the complete participant set and validates
logical tensor coverage. The logical planner does not consume GPU addresses or
allocation metadata.

An alias group may span placement parts. `WeightPlacementManifest` performs the
global check after collection: every alias member must be in the complete
tensor catalog and every fragment for every member must declare the identical
group.

## Logical Planning

The planner consumes complete source and target placements:

```python
logical_plan = plan_placement_transfer(source_placement, target_placement)
```

The result is a backend-neutral `LogicalTransferPlan`. TP and EP use N-D
logical-box split and merge, PP routes framework-provided ownership, and DP
selects a complete source replica or follows a declared DP owner. The plan
contains compact N-D overlap regions but no runtime addresses, endpoints,
allocation bounds, leases, or backend request.

`plan_stored_transfer_to_target_placement` accepts a committed address-free
`StoredWeightManifest` as the logical source. The plan retains the manifest's
canonical identity and selected fragments so a later binding layer can verify
that it is still using the intended Store snapshot.

`PlanningLimits` bounds both transfer-region creation and later flattened
segment expansion. A backend must supply an explicit expansion bound; it cannot
turn a compact N-D plan into an unbounded number of operations.

The logical planner is copy-only. It does not infer model semantics from tensor
names or transform dtype, quantization, packing, swizzle, or checkpoint format.

### Stored-source geometry

For an already published source, use `StoredWeightManifest` and
`plan_stored_transfer_to_target_placement(source_manifest, target_placement)`.
Each `StoredFragmentSnapshot` identifies a logical box by `global_offset` and
`local_shape`, and its bytes by `object_key`, `object_offset`, and `nbytes`.
There is no source rank, training mesh, or framework runtime object in this
fragment contract. Source fragment count, boundaries, and dimensions can differ
from the target placement.

The framework adapter determines the logical boxes and which consistent source
copy to publish. It must decompose any nonrectangular local allocation into
valid boxes. Mooncake validates the declared snapshot and geometry, computes
source/target intersections, and lowers them to byte ranges; it does not infer
training sharding or select a training replica from runtime state.

The stored-source planner tests reconstruct exact bytes for nonuniform 4-to-8
and 8-to-4 layouts, both tensor dimensions, shuffled fragments, and padded
object offsets. These are CPU planning regressions, not transport or throughput
measurements. This path does not replace sparse coordinate planning or change
its wire format.

## Tiled COO Partial Reads

`TiledCOOLayout` and `plan_tiled_coo_read` provide a CPU-only, two-dimensional
range planner for sparse coordinate/value streams. Source and target boxes may
split different dimensions or use different parallel degrees. This planner
does not gather a full tensor or prepartition the payload for a particular
target topology.

The producer orders entries by source record, then by row-major tile. Tiles
are aligned to global tensor coordinates; each source record has its own
contiguous tile directory even when records share a tensor ID. Coordinates and
values remain separate flat arrays in the same entry order. A cumulative
directory of `total_tiles + 1` entry offsets addresses the entire packed stream,
including empty tiles; offsets do not restart at zero for each record.

```python
from mooncake.reshard.weight import TiledCOOLayout, plan_tiled_coo_read

layout = TiledCOOLayout.build(
    names=("weight",),
    shapes=((512, 4096),),
    offsets=((0, 0),),
    tile_shape=(128, 64),
)
plan = plan_tiled_coo_read(layout, local_target_fragments)
```

Reuse `layout` and `plan` while tensor geometry and target placement stay the
same. For every generation:

1. Read `plan.offset_ranges` into a compact directory buffer. The planner may
   combine nearby metadata windows, with at most 64 extra directory entries
   per bridged gap.
2. Project those windows using `plan.offset_gather_indices` and pass the fresh
   integer endpoints to `plan.bind_offsets`. Never reuse previous-generation
   offsets merely because the model structure is unchanged.
3. Read the returned `payload_ranges` from both coordinate and value arrays,
   converting entry offsets to bytes for each array's element size. Payload
   ranges never bridge unrelated entries. `counts` and `compact_entries` map
   each `plan.tile_runs` item to its compact destination interval, including
   empty runs.
4. Filter coordinates against the exact source/target intersection before
   applying updates: a boundary tile may contain entries outside the target.

Multiple targets may read the same tile into different compact destination
intervals. This API does not deduplicate replicas, select a source generation,
choose sparse/dense thresholds, encode values, or define additive versus
replacement updates. Those decisions belong to the producer/consumer layer.
The executor owns array-length, address, capacity and native integer-overflow
checks, transport completion, and buffer lifetime. No Store, CUDA, BufferPool,
or framework dependency is required to construct or bind this plan.

## Runtime Binding

`bind_logical_transfer_plan` attaches selected typed
`WeightRuntimeBindingManifest` values to a `LogicalTransferPlan`. It rechecks
placement identity and digest, runtime fragment geometry, address/allocation
bounds, generation, lease, target coverage, and alias scope before returning a
bound `TransferPlan`.

This plan records runtime evidence but neither submits a copy nor retains the
underlying allocation. A Transfer Engine executor must acquire allocation
guards and revalidate bindings atomically with the submission that consumes
the plan.

### Shared allocation validation

Runtime binding checks every fragment's logical geometry and physical view
range. Many disjoint model-weight views may share one backing allocation;
identical `(storage_address, storage_nbytes)` pairs are collapsed only for the
backing-allocation overlap sweep. Distinct overlapping allocations still fail,
including a partial overlap hidden behind repeated aliases of another arena.
The sweep sorts unique allocations once instead of repeatedly comparing every
pair of views in the same arena. View-overlap and declared-alias checks are
unchanged; no persistent validation cache or new lifetime owner is introduced.

## Transfer Engine Execution

`mooncake.reshard.transfer_engine` executes resource-neutral physical batches
without model or framework semantics. Its completion fence retains registrations
and framework allocation tokens when native completion is unknown, then drains
or quarantines them before a later submission can reuse the same engine.

Range batches use
`scatter_transfer_sync_read/write_with_ticket`, which forwards allocation
bases, capacities, and offset vectors to `TransferEngine::submitScatter()`.
The returned ticket distinguishes completed, failed-and-drained, and unknown
completion. Unknown completion retains registrations and allocation tokens
until `drain_pending_transfer()` reaches a terminal state.

Under TENT, Scatter pins a direct route that supports cancellation before
publishing transport work. An unsupported or staged route is rejected before
DMA submission, so status-query failures always have a cancellation path.

An interrupted completion wait raises `TransferCompletionInterrupted` with the
pending transfer ID, engine identity, and original interruption. The affected
submission remains unusable, while its registered buffers follow the same
pending drain lifecycle.

Engines without the scatter ticket entry point continue to use the flat
`batch_transfer_sync_read/write` compatibility path. A zero result is terminal;
any non-zero result quarantines registrations and allocation tokens as
restart-required because the flat API provides no drainable operation handle.
Registration cleanup uses the same pending lifecycle. Explicit non-zero cleanup
results remain retryable; an exception or interruption makes the cleanup
outcome restart-required. Allocation tokens are retired one at a time, so a
later unknown release never replays tokens whose release already completed.

Model-weight execution is exposed through `MooncakeTransferEngineReader` and
`MooncakeTransferEngineSink`. The reader initiates bounded reads from selected
live source bindings into one local target binding; the sink submits bounded
writes from one selected local source binding to target bindings. Both adapters
consume a bound `TransferPlan`, acquire framework allocation guards, verify the
fresh binding against the planned placement, generation, lease, allocation, and
fragment evidence, then lower N-D regions into bounded physical batches.

The lowerer preserves backing-allocation ranges. Range batches stay grouped by
allocation and are submitted through the scatter ticket API; flat batches use
the compatibility entry point. `max_batch_operations`,
`max_region_segments`, and `max_total_lowered_segments` bound physical
expansion before native submission.

Set `max_inflight_batches` on the reader or sink to overlap batches across
selected endpoints. The default, `1`, drains each batch before submitting the
next. Larger windows require the native `submit_scatter_read/write` methods,
which return a completion ticket immediately after submission. The executor
drains the oldest ticket when the window is full and returns receipts in
submission order. Batch size limits still apply to each submitted batch.

All published tickets are drained before returning a known failure. Unresolved
tickets share one pending-transfer handle, which retains the active
registrations and allocation tokens until every ticket reaches a terminal
state. A native submit exception without a recoverable ticket requires engine
restart. The resource-neutral API exposes the same window through
`TransferSubmission.execute_batches(..., max_inflight_batches=N)`.

Each composite pending-ticket drain shares one timeout budget across its
children. Later children receive the remaining budget, including zero for a
non-blocking completion check. Backend calls must honor that timeout; status
queries and scheduling overhead are additional to the wait budget.

`execute_batches` borrows its input iterable and does not call `close()` on
caller-owned iterators. Keep a resource-owning generator alive on early exit,
and explicitly close it after outstanding transfers reach a terminal state.
Its `finally` block must not free buffers still retained by a pending transfer.

Live TE adapters accept runtime-bound source fragments. Store-backed restore
continues through `WeightStore.load()`, which uses the same target placement and
binding contracts with `get_into_ranges`.

## Store Snapshots

`StoredResourceManifest` is the persistent resource base. The concrete
`StoredWeightManifest` adds revision, weight generation, tensor descriptors,
stored fragments, and its canonical digest. It contains no GPU address or
runtime allocation owner.

`MooncakeDistributedStore.begin_weight_snapshot()` returns a
`WeightStoreWriter` for one immutable snapshot. A framework adapter supplies
complete placement/binding inventories, resolves each `write_tensor()` call to
canonical fragment IDs, and provides allocation guards for Store I/O. After
every required fragment succeeds, `commit()` publishes the single
`StoredWeightManifest`.

### Public API Migration

This release removes the public `*_with_parallelism` API family and its helper
types `ParallelAxis`, `TensorParallelism`, and `ReadTarget`. This is a breaking
change for applications using the former multi-axis tensor API. Weight snapshot
writers now use the explicit lifecycle:

```python
writer = WeightStore(store).begin_weight_snapshot(descriptor, adapter)
writer.write_tensor(tensor_id, tensor)
manifest = writer.commit()
```

The snapshot writer stores manifest-managed payload fragments and publishes one
`StoredWeightManifest`; it does not produce an ordinary Store tensor object.
Weight snapshot readers use `WeightStore.load_manifest()`, `plan_load()`, and
`load()` with target placement and runtime binding manifests. The existing
single-axis TP APIs named `*_with_tp` remain separate compatibility APIs.

If `commit()` reports a manifest publication failure after the Store records
the commit decision, the writer stays open and preserves its uploaded payloads.
Retry `commit()` on the same writer to complete publication.

`WeightStore.load_manifest()` and `WeightStore.plan_load()` use the stored
manifest as the source of truth for restore. The target framework supplies the
placement, runtime binding, and allocation guards required by `get_into_ranges`.

## Module Responsibilities

- `types.py` defines tensor and logical-fragment contracts.
- `topology.py` defines parallel sizes and selected participants.
- `part.py` defines one participant's address-free contribution.
- `placement.py` assembles and identifies a complete global placement.
- `runtime.py` defines typed physical-binding input contracts for later phases.
- `validation.py` validates manifest-level geometry, coverage, aliases, and
  runtime-binding shape contracts.
- `_planner/contracts.py` computes N-D logical overlap regions and logical
  coverage.
- `_planner/binding.py`, `bound_contracts.py`, and `bound_validation.py` bind
  a logical plan to typed runtime snapshots and validate physical evidence.
- `planner.py` exposes both logical planning and runtime-binding APIs.
- `manifest.py` preserves the public import surface.
- `transfer_engine/` contains resource-neutral physical transfer batches,
  registration, completion, and pending-resource lifecycle management.
- `_te/` contains weight-specific N-D lowering plus source/target execution
  adapters; `te.py` preserves their public import surface.

## KV Cache Placement Model

`KVCachePlacementManifest` describes an address-free KV-cache placement over a
selected topology. Each participant contributes a `KVCachePlacementPart`, and
`KVCacheRuntimeBindingManifest` supplies operation-scoped live buffers. Runtime
bindings contain no lease or eviction state; the framework owns pinning and
lifetime. `KVCacheSnapshotDescriptor` independently describes model and token
semantics. Legacy logical planning permits omission for compatibility; the
Runtime-to-Runtime executor requires an explicit snapshot and operation ID.

The KV-cache implementation is split by responsibility:

- `snapshot.py` defines content identity;
- `topology.py`, `part.py`, and `placement.py` define logical placement;
- `runtime.py` and `binding.py` define and validate live buffer bindings;
- `planner.py` defines semantically validated logical and prepared plans;
- `resolved.py` and `transfer.py` validate and lower bounded resolved ranges;
- `executor.py` and `completion.py` execute TE writes and collect all participants;
- `runtime_serde.py` serializes resolved bindings, operations, and receipts;
- `serde.py`, `snapshot_serde.py`, and `plan_serde.py` define strict JSON
  boundaries.

KV planning is source/target-role agnostic. A placement may expose one or more
complete DP replicas, and each local-target plan selects exactly one source DP
replica. Callers may choose that source rank explicitly; otherwise the planner
maps target DP ranks round-robin over the source placement's available DP ranks.

`weight_placement_to_json` and `weight_placement_from_json` are the explicit
public JSON APIs. Their wire format contains only canonical fields, and
deserialization rejects alternate field names rather than translating
framework-specific input.

Run the reshard tests from the repository root:

```bash
PYTHONPATH=mooncake-reshard/python \
python3 -m pytest -q mooncake-reshard/tests

npx --yes pyright --project mooncake-reshard/pyrightconfig.json
```
