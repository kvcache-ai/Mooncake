# Resource Reshard Manifest Contract

This document defines the framework-neutral resource contract used by
Mooncake resharding and its model-weight specialization. The contract separates
complete logical placement from live physical addresses so planning can finish
before a runtime binding is available.

The implementation is owned by the top-level `mooncake-reshard` module. Common
contracts are exposed through `mooncake.reshard.contracts`; the public weight API
is `mooncake.reshard.weight`.

## Contract Split

| Contract | Contents | Lifetime |
|----------|----------|----------|
| `ResourceManifest` | resource identity and kind | shared base contract |
| `PlacementManifest` | address-free placement identity and digest | serializable and reusable |
| `RuntimeBindingManifest` | placement attestation, runtime instance, generation, and lease | one live runtime snapshot |
| `ParallelTopology` | TP/PP/EP/DP sizes and the explicit participant-to-rank mapping | one logical placement |
| `TensorParallelAxis` | one tensor's parallel kind and optional logical split dimension | one tensor descriptor |
| `WeightPlacementPart` | one participant's address-free tensors and logical fragments | framework-local contribution |
| `WeightPlacementManifest` | one complete global logical placement of a weight generation | serializable and reusable |
| `WeightRuntimeBindingManifest` | one participant's physical fragments for that global placement | one live runtime snapshot |

Weight revision, tensor geometry, model semantics, parallel ownership, and
weight generation belong to the weight specialization. GPU addresses,
endpoints, owners, generations, and leases never appear in
`WeightPlacementManifest`.

`model_weight` is the serialized resource discriminator. Resource adapters are
registered explicitly by `ResourceKind`; Mooncake does not infer a resource or
model type from parameter names.

## Global Placement Assembly

`ParallelTopology` declares the runtime's TP, PP, EP, and DP sizes and the exact
participants selected for this placement. Its `world_size` is the number of
declared participants, not `tp_size * pp_size * ep_size * dp_size`. Frameworks
may map axes such as TP and EP onto the same workers, and a placement may select
one DP replica while retaining the runtime's declared `dp_size`.

Each participant exports one `WeightPlacementPart`. A part carries the common
resource ID, revision, weight generation, placement-set ID, topology ID,
participant ID, parallel rank, tensor descriptors, and logical fragments. It
contains no physical address. A part declares exactly the tensor descriptors
referenced by its fragments; an empty part declares neither.

A collection barrier assembles all declared parts into one
`WeightPlacementManifest`. Assembly fails when a participant is missing or
duplicated, when a part belongs to a different resource, generation, placement
set, or topology, or when its rank disagrees with the topology. Only after the
complete placement validates are its canonical `placement_id` and digest
available.

Each live participant then exports a `WeightRuntimeBindingManifest` that names
its `participant_id` and attests the same global `placement_id` and digest.
Binding-set validation requires every participant that owns fragments exactly
once, and exact logical-fragment membership for each such participant. Empty
participants require no runtime binding.

## Logical Semantics

Each tensor has a stable `tensor_id`, full `global_shape`, dtype, item size,
layout fingerprint, and optional layer or expert identity. Each fragment is an
N-D logical box described by `global_offset` and `local_shape`.

`partition_dim` is the single-dimensional shorthand. `shard_dims` is the
normalized N-D form. When both are present they must describe the same single
dimension. Each `TensorParallelAxis` records a framework-provided axis kind and
its optional `split_dim`. Axis size comes from `ParallelTopology`; a fragment's
axis rank comes from `ParallelRank`. Together these fields distinguish, for
example, EP on logical dimension 0 from TP on logical dimension 1 without
inferring semantics from tensor names. Ownership-only PP, DP, or independently
allocated EP axes leave `split_dim` unset. The split dimensions must match
`shard_dims` exactly.

The global manifest validates complete logical coverage. Every selected DP
replica must provide a gap-free cover of every tensor. Equivalent replicated
boxes are counted once; the remaining N-D boxes must not overlap. DP therefore
may select one complete replica for transfer while the topology retains the
original `dp_size`.

PP is layer or tensor ownership. A logical tensor may have complete replicas on
multiple PP owners, but every owner must independently provide a gap-free
cover; fragments from different PP owners cannot be combined to satisfy
coverage. For grouped expert tensors, EP sharding is represented by the leading
logical expert coordinate rather than only an EP rank label. Independently
allocated experts remain independent tensors with an explicit expert identity.

Mooncake does not infer layer, expert, layout, or partition semantics from
model parameter names. Framework adapters must provide those facts.

`placement_fragment_id` defaults to a canonical hash of tensor identity,
logical box, parallel rank, byte size, and alias group. Frameworks may supply
an explicit stable ID when they intentionally need a different identity. An
alias group is valid only when it contains the fragment's own `tensor_id`; two
fragments may share one runtime range only when both tensor IDs belong to the
same compatible alias group.

## Identity And Fencing

Canonical placement identity covers the resource, revision, weight generation,
placement-set ID, topology, global tensor descriptors, participant ownership,
and logical fragments. Runtime addresses, workers, endpoints, owners, and
leases do not affect placement identity.

Every runtime binding carries the global placement ID and digest. Validation
rejects a binding when the logical placement changes, a participant is unknown,
a fragment is missing or unexpected, or its byte range differs. Generation and
lease fences remain live-runtime state and must be checked before transfer.

Every runtime `address` points to the first transferable byte of a contiguous
tensor view. A runtime fragment preserves `itemsize`, `local_shape`, byte
strides, storage base address, normalized storage byte offset, and storage
allocation size. Binding validation compares item size, shape, and canonical
byte strides with the logical placement, verifies
`address = storage_address + storage_offset_bytes`, and requires the complete
view range to remain inside the allocation. An optional
framework `is_contiguous` flag may reject a view early but is never accepted as
the sole proof of contiguity. Address zero is reserved as a null sentinel, and
all address ranges must have representable unsigned 64-bit exclusive ends.
Owner objects may keep framework allocations alive but are never serialized.

## Integration Flow

1. The framework declares one `ParallelTopology` and a shared resource,
   revision, weight generation, and placement-set ID.
2. Every selected participant exports one `WeightPlacementPart`.
3. A barrier collects the exact part set and constructs one complete
   `WeightPlacementManifest`.
4. Each live participant exports a `WeightRuntimeBindingManifest` against the
   resulting placement ID and digest.
5. Planning consumes one source and one target `WeightPlacementManifest`.
6. Binding and execution use only the participant bindings referenced by the
   logical plan, while preserving their generation and lease fences.

Inventory adapters accept mappings or attribute-based records and do not import
SGLang, vLLM, PyTorch, or another model framework. Integer-valued contract
fields require Python `int` values and reject `bool`; framework adapters must
normalize framework-specific scalar types before constructing a manifest.

## Boundaries

The manifest contract does not infer model semantics, synthesize framework
placements, execute transfers, or define discovery, activation, rollback, and
other control-plane policies. Planner, Store, and Transfer Engine adapters
consume this contract without changing its logical identity rules.
