# Model Weight Store Upload Planning

`plan_weight_upload` turns one complete runtime weight placement into an
immutable `WeightUploadPlan`. The plan supplies the Store writer with a
canonical `StoredWeightManifest`, payload object locations, and source
evidence for each upload operation.

## Inputs

- `WeightPlacementManifest` describes global tensor geometry and TP, PP, EP,
  and DP ownership.
- `WeightRuntimeBindingManifest` values provide the live source bindings for
  populated placement participants.

The planner validates every supplied binding against the placement. Model
semantics remain in framework adapters, which export canonical tensor and
parallel-axis metadata before calling this API.

## Replica Selection

The planner stores one complete source replica:

1. It requires replicated DP semantics and a complete logical coverage for a
   DP replica.
2. It requires every selected source binding in that replica to have the same
   generation.
3. It selects the lowest eligible DP rank deterministically.
4. It retains logical TP, PP, and EP ownership in every selected fragment.

The resulting manifest has one stored fragment per selected logical source
fragment. DP replicas do not duplicate payload objects.

## Plan Contents

`WeightUploadPlan` contains:

- a `StoredWeightManifest` with immutable Store group, manifest, and payload keys;
- an `UploadOperation` for each stored fragment;
- the source placement identity and digest;
- an upload transaction group and its control key.

An operation stores an owner-free `RuntimeFragmentSnapshot`, source
participant and instance IDs, lease ID, and generation. The payload writer
rebinds this evidence to a fresh runtime manifest and acquires the framework
allocation guard before Store I/O.

## Execution Boundary

The Store writer performs payload writes, registration, transaction commit/abort,
and Store-to-runtime reads. These layers consume `WeightUploadPlan`; they do
not infer model layouts or parallelism from Store keys.

## Snapshot API

`MooncakeDistributedStore.begin_weight_snapshot(descriptor, adapter)` returns
one `WeightStoreWriter`. The framework adapter exports the complete source
placement and live bindings once. The caller then writes each framework tensor
through `writer.write_tensor(tensor_id, tensor)`.

The adapter maps that tensor to canonical placement fragment IDs. The writer
uploads only the runtime fragments attested by those bindings and
`commit()` publishes one `StoredWeightManifest` after every required fragment
is durable.

The writer owns the storage policy for its immutable payload and metadata
objects. Per-tensor replication, partition, and upsert parameters are outside
this API, so every committed snapshot has one explicit manifest contract.

Restore begins from `StoredWeightManifest`. The loader reconstructs sources
from each stored fragment's tensor ID, global offset, local shape, object key,
object offset, and byte length, then uses `get_into_ranges` with the target
placement and runtime binding. This path uses no legacy `TensorMetadata` head
or legacy parallel-tensor reconstruction metadata.

The persisted manifest contains logical tensor descriptors, fragment geometry,
payload keys, and snapshot identity. Runtime addresses, allocation owners,
leases, and worker instances remain in the live binding and allocation-guard
path used during Store I/O.

## Native Store Requirement

The native Store writer uses group semantics to keep payload, manifest, and
transaction-control objects in their declared groups. It requires a Mooncake wheel
whose `ReplicateConfig` exposes `group_ids` (the API introduced by PR #3000).
The adapter rejects an older binding before Store I/O starts.
