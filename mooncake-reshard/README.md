# Mooncake Reshard

`mooncake-reshard` defines framework-neutral contracts, address-free N-D
logical planning, and runtime binding for reusable runtime resources. This
change adds the model-weight manifest, logical reshard planner, and an
attested bound-plan contract; storage lifecycle and transfer execution remain
separate phases.

Framework-owned adapters inspect framework runtime objects, normalize
framework-specific values, and construct typed canonical manifests. Mooncake
core accepts only those typed values; it does not import or inspect framework
objects or accept alternate field names or duck-typed records.

The public Python API is split by responsibility:

- `mooncake.reshard.contracts` exposes `ResourceManifest`,
  `PlacementManifest`, and `RuntimeBindingManifest` as structural `Protocol`
  contracts for resource-neutral identity and lifecycle;
- `mooncake.reshard.weight` defines model-weight placement, runtime-binding
  input contracts, and address-free N-D planning.

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
`WeightManifest` as the logical source. The plan retains the manifest's
canonical identity and selected fragments so a later binding layer can verify
that it is still using the intended Store snapshot.

`PlanningLimits` bounds both transfer-region creation and later flattened
segment expansion. A backend must supply an explicit expansion bound; it cannot
turn a compact N-D plan into an unbounded number of operations.

The logical planner is copy-only. It does not infer model semantics from tensor
names or transform dtype, quantization, packing, swizzle, or checkpoint format.

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

`kv_cache` remains reserved as a resource discriminator. This change does not
define a KVCache manifest; a future KVCache reshard adapter can reuse the
resource/placement/binding boundary without changing the model-weight planner.

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
