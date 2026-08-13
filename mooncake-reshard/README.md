# Mooncake Reshard

`mooncake-reshard` defines framework-neutral contracts for reusable runtime
resources. This change adds the model-weight manifest contract; planning,
storage, and transfer execution are added separately.

Framework-owned adapters outside Mooncake inspect framework runtime objects,
normalize framework-specific values, and construct the typed canonical
manifests. Mooncake core accepts only those typed values; it does not import or
inspect framework objects or accept alternate field names or duck-typed
records.

The public Python API is split by responsibility:

- `mooncake.reshard.contracts` exposes `ResourceManifest`,
  `PlacementManifest`, and `RuntimeBindingManifest` as structural `Protocol`
  contracts for resource-neutral identity and lifecycle;
- `mooncake.reshard.weight` defines model-weight placement and runtime binding.

## Weight Placement Model

`WeightPlacementManifest` describes one complete, address-free global logical
placement of a model-weight generation. It contains:

- a `ParallelTopology` with TP, PP, EP, and DP sizes plus the exact selected
  participants;
- per-tensor `SplitAxis(kind, dim)`, `ReplicatedAxis(kind)`, and
  `OwnershipAxis(kind)` entries that distinguish logical sharding, complete
  replicas, and ownership without overloading an optional dimension;
- canonical `TensorDescriptor` values whose only shard representation is
  `shard_dims`;
- one `WeightPlacementPart` for every selected participant;
- canonical global tensor descriptors and N-D logical fragments;
- a placement ID and digest computed after the full part set validates.

`ParallelTopology.world_size` is the selected participant count. It is not
inferred from `tp_size * pp_size * ep_size * dp_size`: parallel axes may share
workers, and a placement may select one complete DP replica while retaining the
runtime's declared `dp_size`. The overall participant map may be non-Cartesian,
but a tensor that declares multiple independent `SplitAxis` values must provide
the Cartesian rank combinations needed to prove each axis-to-dimension split.

For each framework participant, the framework adapter first constructs an
address-free `WeightPlacementPart`. A collection barrier assembles the exact
participant set and validates complete logical tensor coverage. Each part
declares exactly the tensor descriptors referenced by its fragments. For each
live participant that owns fragments, the adapter then constructs one
`WeightRuntimeBindingManifest` with physical fragments, generation, and lease,
attesting the global placement ID and digest. A physical fragment preserves its
item size, view shape, byte strides, storage base, byte offset, and allocation
size so binding validation can prove canonical contiguity and address bounds.

An alias group may span placement parts, so an individual part checks only its
local fragment invariants. `WeightPlacementManifest` performs the global check
after collection: every alias member must be in the complete tensor catalog and
every fragment for every member must declare the identical group. Runtime paths
consume only this globally validated placement.

Empty participants need no runtime binding; any participant referenced by
execution must provide one.

The weight implementation is split by responsibility:

- `types.py` defines tensor and logical-fragment contracts;
- `topology.py` defines parallel sizes and selected participants;
- `part.py` defines one participant's address-free contribution;
- `placement.py` assembles and identifies the complete global placement;
- `runtime.py` defines typed physical bindings;
- `validation.py` checks logical geometry, coverage, declared storage alias
  groups, and addresses;
- `binding.py` validates placement and binding attestation;
- `manifest.py` preserves the public import surface.

`kv_cache` is reserved as a resource discriminator, but this change does not
define a KVCache manifest. Framework adapters must provide tensor semantics;
Mooncake does not infer them from parameter names.

`weight_placement_to_json` and `weight_placement_from_json` are the explicit
public JSON APIs. Their wire format contains only canonical fields, and
deserialization rejects alternate field names rather than translating
framework-specific input.

Run the contract and static type checks from the repository root:

```bash
PYTHONPATH=mooncake-wheel:mooncake-reshard/python \
python -m pytest -q mooncake-reshard/tests

bash scripts/check_reshard_types.sh
```
