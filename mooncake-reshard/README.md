# Mooncake Reshard

`mooncake-reshard` defines framework-neutral contracts for reusable runtime
resources. This change adds the model-weight manifest contract; planning,
storage, and transfer execution are added separately.

The public Python API is split by responsibility:

- `mooncake.reshard.contracts` defines resource-neutral identity and lifecycle
  contracts;
- `mooncake.reshard.weight` defines model-weight placement and runtime binding.

## Weight Placement Model

`WeightPlacementManifest` describes one complete, address-free global logical
placement of a model-weight generation. It contains:

- a `ParallelTopology` with TP, PP, EP, and DP sizes plus the exact selected
  participants;
- per-tensor `TensorParallelAxis` entries that map TP/EP layout axes to logical
  split dimensions and retain PP/DP ownership axes;
- one `WeightPlacementPart` for every selected participant;
- canonical global tensor descriptors and N-D logical fragments;
- a placement ID and digest computed after the full part set validates.

`ParallelTopology.world_size` is the selected participant count. It is not
inferred from `tp_size * pp_size * ep_size * dp_size`: parallel axes may share
workers, and a placement may select one complete DP replica while retaining the
runtime's declared `dp_size`.

Each framework participant first exports an address-free
`WeightPlacementPart`. A collection barrier assembles the exact participant set
and validates complete logical tensor coverage. Each part declares exactly the
tensor descriptors referenced by its fragments. Each live participant that
owns fragments then exports one `WeightRuntimeBindingManifest` with physical
fragments, generation, and lease, attesting the global placement ID and digest.
A physical fragment preserves its item size, view shape, byte strides, storage
base, byte offset, and allocation size so binding validation can prove canonical
contiguity and address bounds. Empty participants need no runtime binding; any
participant referenced by execution must provide one.

The weight implementation is split by responsibility:

- `types.py` defines tensor and logical-fragment contracts;
- `topology.py` defines parallel sizes and selected participants;
- `part.py` defines one participant's address-free contribution;
- `placement.py` assembles and identifies the complete global placement;
- `runtime.py` imports framework inventories and defines physical bindings;
- `validation.py` checks logical geometry, coverage, aliases, and addresses;
- `binding.py` validates placement and binding attestation;
- `manifest.py` preserves the public import surface.

`kv_cache` is reserved as a resource discriminator, but this change does not
define a KVCache manifest. Framework adapters must provide tensor semantics;
Mooncake does not infer them from parameter names.

Run the contract tests from the repository root:

```bash
PYTHONPATH=mooncake-reshard/python \
python -m pytest -q mooncake-reshard/tests
```
