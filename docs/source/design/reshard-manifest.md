# Resource Reshard Manifest Contract

This document defines the framework-neutral resource contract used by
Mooncake resharding and its model-weight specialization. The contract separates
complete logical placement from live physical addresses so planning can finish
before a runtime binding is available.

The implementation lives in the root Python project under
`python/mooncake/reshard`. Common contracts are exposed through
`mooncake.reshard.contracts`; the public weight API is
`mooncake.reshard.weight`.

Framework-owned adapters outside Mooncake inspect framework runtime objects,
normalize framework-specific values, and construct the typed canonical
manifests. Mooncake core accepts only those typed values; it does not import or
inspect framework objects or accept alternate field names or duck-typed
records.

## Contract Split

| Contract | Contents | Lifetime |
|----------|----------|----------|
| `ResourceManifest` | structural protocol for resource identity and kind | shared public contract |
| `PlacementManifest` | structural protocol for address-free placement identity and digest | serializable and reusable |
| `RuntimeBindingManifest` | structural protocol for placement attestation, runtime instance, generation, and lease | one live runtime snapshot |
| `ParallelTopology` | TP/PP/EP/DP sizes and the explicit participant-to-rank mapping | one logical placement |
| `SplitAxis` | a parallel kind that shards one explicit logical dimension | one tensor descriptor |
| `ReplicatedAxis` | a parallel kind whose ranks each hold a complete replica | one tensor descriptor |
| `OwnershipAxis` | a parallel kind that assigns tensor or object ownership without splitting a dimension | one tensor descriptor |
| `WeightPlacementPart` | one participant's address-free tensors and logical fragments | framework-local contribution |
| `WeightPlacementManifest` | one complete global logical placement of a weight generation | serializable and reusable |
| `WeightRuntimeBindingManifest` | one participant's physical fragments for that global placement | one live runtime snapshot |

The three common manifest contracts are public structural `Protocol` types.
Consumers depend on their fields and behavior, not inheritance from a Mooncake
base class.

Weight revision, tensor geometry, model semantics, parallel ownership, and
weight generation belong to the weight specialization. GPU addresses,
endpoints, owners, generations, and leases never appear in
`WeightPlacementManifest`.

`model_weight` is the serialized resource discriminator. Typed manifests carry
their `ResourceKind` explicitly; Mooncake does not infer a resource or model
type from parameter names.

## Global Placement Assembly

`ParallelTopology` declares the runtime's TP, PP, EP, and DP sizes and the exact
participants selected for this placement. Its `world_size` is the number of
declared participants, not `tp_size * pp_size * ep_size * dp_size`. Frameworks
may map axes such as TP and EP onto the same workers, and a placement may select
one DP replica while retaining the runtime's declared `dp_size`.

For each participant, the framework adapter constructs one typed
`WeightPlacementPart`. A part carries the common resource ID, revision, weight
generation, placement-set ID, topology ID, participant ID, parallel rank,
tensor descriptors, and logical fragments. It contains no physical address. A
part declares exactly the tensor descriptors referenced by its fragments; an
empty part declares neither.

A collection barrier assembles all declared parts into one
`WeightPlacementManifest`. Assembly fails when a participant is missing or
duplicated, when a part belongs to a different resource, generation, placement
set, or topology, or when its rank disagrees with the topology. Only after the
complete placement validates are its canonical `placement_id` and digest
available.

For each live participant, the framework adapter then constructs a typed
`WeightRuntimeBindingManifest` that names its `participant_id` and attests the
same global `placement_id` and digest. Binding-set validation requires every
participant that owns fragments exactly once, and exact logical-fragment
membership for each such participant. Empty participants require no runtime
binding.

## Logical Semantics

Each tensor has a stable `tensor_id`, full `global_shape`, dtype, item size,
layout fingerprint, and optional layer or expert identity. Each fragment is an
N-D logical box described by `global_offset` and `local_shape`.

`TensorDescriptor.shard_dims` is the only canonical shard representation.
`SplitAxis(kind, dim)` explicitly shards one logical dimension;
`ReplicatedAxis(kind)` requires each selected rank to provide a complete copy;
and `OwnershipAxis(kind)` assigns tensor or object ownership without splitting
a logical dimension. Axis size comes from `ParallelTopology`, and a fragment's
axis rank comes from `ParallelRank`. The dimensions named by all `SplitAxis`
values must match `shard_dims` exactly.

The global manifest validates complete logical coverage. Every selected DP
replica must provide a gap-free cover of every tensor. `OwnershipAxis` and
`ReplicatedAxis` values form independent covers. Fragments across a `SplitAxis`
instead form one non-overlapping cover, and every split-axis rank declared by
the topology must participate. The explicit participant mapping defines the
selected workers and may be non-Cartesian overall. Within one tensor's owner and
replica cover, however, coordinates for multiple declared `SplitAxis` values
must form their Cartesian product so that each rank-to-dimension assignment is
provable. A physical coordinate coupled to another split rank but not
independently sharding the tensor is left out of that tensor's `parallel_axes`.
DP may therefore select one complete replica for transfer while the topology
retains the original `dp_size`.

PP is layer or tensor ownership. A logical tensor may have complete replicas on
multiple PP owners, but every owner must independently provide a gap-free
cover; fragments from different PP owners cannot be combined to satisfy
coverage. For grouped expert tensors, EP uses `SplitAxis` on the leading logical
expert dimension rather than only an EP rank label. Independently allocated
experts use `OwnershipAxis` and remain independent tensors with an explicit
expert identity.

Mooncake does not infer layer, expert, layout, or partition semantics from
model parameter names. Framework adapters must provide those facts.

`placement_fragment_id` defaults to a canonical hash of tensor identity,
logical box, parallel rank, byte size, and alias group. Frameworks may supply
an explicit stable ID when they intentionally need a different identity. An
alias group is valid only when it contains the fragment's own `tensor_id`; two
fragments may share one runtime range only when both tensor IDs belong to the
same compatible alias group.

Because an alias group can cross placement participants, a local
`WeightPlacementPart` validates only its own fragments. Complete
`WeightPlacementManifest` assembly is the authorization boundary: every alias
member must be in the global tensor catalog and every fragment of every member
must declare the same alias group before any runtime binding is accepted.

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
strides normalized on singleton dimensions, storage base address, normalized
storage byte offset, and storage allocation size. Binding validation compares
item size, shape, and contiguous
row-major byte strides with the logical placement; singleton dimensions do not
constrain their corresponding stride. It also verifies
`address = storage_address + storage_offset_bytes`, and requires the complete
view range to remain inside the allocation. An optional
framework `is_contiguous` flag may reject a view early but is never accepted as
the sole proof of contiguity. Address zero is reserved as a null sentinel, and
all address ranges must have representable unsigned 64-bit exclusive ends.
Owner objects may keep framework allocations alive but are never serialized.

## Integration Flow

1. A framework-owned adapter reads framework state and constructs one typed
   `ParallelTopology` plus the shared resource, revision, weight generation,
   and placement-set ID.
2. The adapter constructs one typed `WeightPlacementPart` for every selected
   participant.
3. A barrier collects the exact part set and constructs one complete
   `WeightPlacementManifest`.
4. The adapter constructs a typed `WeightRuntimeBindingManifest` for each live
   participant against the resulting placement ID and digest.
5. Planning consumes one source and one target `WeightPlacementManifest`.
6. Binding and execution use only the participant bindings referenced by the
   logical plan, while preserving their generation and lease fences.

`weight_placement_to_json` and `weight_placement_from_json` are the explicit
public JSON APIs for the canonical wire schema. Deserialization accepts exactly
the canonical fields and values; it does not accept aliases, attribute-based
records, or other framework-shaped inputs. Integer-valued contract fields
require Python `int` values and reject `bool`; framework adapters must normalize
framework-specific scalar types before constructing a manifest.

## Boundaries

The manifest contract does not inspect framework objects, infer model semantics,
synthesize framework placements, execute transfers, or define discovery,
activation, rollback, and other control-plane policies. Framework adapters own
object inspection and normalization. Planner, Store, and Transfer Engine
adapters consume the resulting canonical manifests without changing their
logical identity rules.
