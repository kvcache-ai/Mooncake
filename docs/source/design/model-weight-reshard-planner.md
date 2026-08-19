# Model Weight Reshard Planner And Runtime Binding

`mooncake-reshard` plans an address-free conversion between complete model
weight placements, then binds the selected logical regions to immutable
runtime snapshots. It does not inspect framework runtime objects or submit a
transfer.

## Inputs and Output

The source is either a complete `WeightPlacementManifest` or a committed
`WeightManifest` snapshot. The target is a complete
`WeightPlacementManifest`. Both sides must identify the same resource,
revision, and weight generation.

The public APIs are:

- `plan_placement_transfer(source_placement, target_placement)`;
- `plan_placement_transfer_to_local_target(source_placement,
  target_placement, target_participant_id)`;
- `plan_stored_transfer_to_target_placement(source_manifest,
  target_placement)`.

Each API returns a `LogicalTransferPlan`. It contains only canonical tensor
descriptors, selected placement participants, and logical regions. It contains
no GPU address, endpoint, allocation range, lease, or backend handle.

`bind_logical_transfer_plan(logical_plan, target_bindings, ...)` is the second
public step. It accepts typed `WeightRuntimeBindingManifest` values and returns
a `TransferPlan` with selected runtime fragments, binding attestations, and
executor projections.

## N-D Regions

Each `TransferRegion` represents one source/target N-D box overlap. It records
the overlap offset and shape, source and target base byte offsets, contiguous
`inner_bytes`, outer loop counts, and source/target byte strides.

The planner preserves a compact strided representation. It does not expand a
cross-dimension overlap into one operation per row or element. `PlanningLimits`
bounds the total number of regions and any later segment expansion fails closed
when it exceeds the configured limit.

## Parallel Semantics

- **TP** changes logical boxes. The same overlap algorithm handles split,
  merge, and source/target sharding on different dimensions.
- **PP** is explicit framework-provided tensor or layer ownership. Regions are
  grouped by source and target PP owner and optional pipeline stage; the
  planner does not infer ownership from a tensor name or layer-count formula.
- **EP** is represented by a logical expert coordinate. Independent expert
  allocations remain independent logical fragments and are never packed or
  all-gathered by the planner.
- **DP** does not change tensor geometry. A `ReplicatedAxis(kind="dp")` uses a
  complete source replica. An `OwnershipAxis(kind="dp")` routes each tensor
  through its declared owner and does not require every tensor on every DP
  rank.

All four axes are resolved by one logical-box plan, rather than by model-wide
per-axis conversion passes.

## Validation

Placement construction validates the complete participant set, tensor
descriptors, topology, and logical coverage before planning. Planning then
fails closed when source and target tensor identity, dtype, shape, layout
fingerprint, ownership, or coverage differ.

A `WeightManifest` source is retained as an immutable logical snapshot. Its
canonical identity and selected stored fragments are revalidated whenever a
logical plan is constructed or reconstructed. This proves that the plan still
refers to the same Store snapshot; it does not make Store persistence or
runtime loading part of this layer.

Coverage validation uses an ordered interval scan for 1-D inputs and a
coordinate-compressed sweep for 2-D inputs, both with `O(N log N)` behavior.
For 3-D and higher logical boxes, exact intersection remains supported under an
explicit pairwise-comparison budget; inputs that exceed it fail closed rather
than making validation work unbounded.

## Runtime Binding

Binding rechecks the exact source and target placement identities, placement
digests, participant selection, runtime fragment geometry, device/allocation
bounds, lease generation, and declared alias scope. It rejects a reconstructed
logical plan with incomplete target coverage, forged Store fragments, or
conflicting physical target ranges.

The returned `TransferPlan` is a bound, attested snapshot. It has no `execute`
or `submit` operation and does not retain allocation lifetime. A later Transfer
Engine executor must acquire real allocation guards and revalidate its bindings
atomically with submission.

Transfer Engine lowering, DMA submission, Store persistence/lifecycle, and
framework activation remain outside this phase. Framework adapters own model
semantics and conversion into canonical manifests; Mooncake core does not infer
those semantics from framework objects or parameter names.
