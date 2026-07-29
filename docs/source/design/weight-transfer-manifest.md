# Weight Transfer Manifest Contract

This document defines the framework-neutral contract used to describe model
weight placement and live runtime locations. It is the foundation for future
heterogeneous reshard planning; it does not implement planning, transfer
execution, or weight storage.

## Contract Split

The contract separates logical placement from process-local state:

| Contract | Contents | Lifetime |
|----------|----------|----------|
| `PlacementManifest` | tensor identity, global shape, dtype, logical boxes, shard dimensions, ownership coordinates, and layout fingerprint | serializable and reusable |
| `RuntimeBindingManifest` | worker, endpoint, device, contiguous address range, lease, generation, and allocation owner | one live runtime snapshot |
| `RuntimeManifest` | validated logical and physical runtime snapshot | one live runtime snapshot |

`PlacementManifest` never contains GPU addresses, endpoints, leases, or owner
objects. A runtime restart can therefore reuse the same logical placement and
produce a new binding.

## Logical Semantics

Each tensor has a stable `tensor_id`, full `global_shape`, dtype, item size,
layout fingerprint, and optional layer or expert identity. Each fragment is an
N-D logical box described by `global_offset` and `local_shape`.

`partition_dim` is the single-dimensional shorthand. `shard_dims` is the
normalized N-D representation. When both are present they must describe the
same single dimension.

`ParallelRank` records the framework-provided owner coordinates for routing.
It is not a second sharding model: logical coverage comes from the N-D box and
`shard_dims`. Topology sizes and target-placement synthesis remain planner
inputs, consistent with the unified parallel tensor I/O design.

Mooncake does not infer layer, expert, layout, or partition semantics from
model parameter names. Framework adapters must provide those facts.

## Identity And Fencing

A placement is serialized in canonical tensor and fragment order.
`placement_id` is derived from that canonical logical content; a supplied ID
must match the derived value. Runtime instance IDs, addresses, workers,
endpoints, and leases never affect it. A second SHA-256 digest covers the
complete serialized placement, including the derived placement ID.
Canonical hashes use UTF-8 JSON with sorted keys, compact separators, and
normalized single-axis `partition_dim`/`shard_dims` semantics.

Every runtime binding carries that digest. `bind_runtime_manifest()` rejects a
binding when any logical placement field has changed. Different
`placement_fragment_id` values cannot describe the same tensor, owner rank,
offset, and shape.

The contract carries the lease and generation fence; the caller or control
plane must verify liveness immediately before binding and transfer. Optional
owner objects keep framework allocations alive and are never serialized.
If a binding fragment provides its own lease generation, it must equal the
binding's snapshot generation. Address ranges are validated within each
`(instance, worker, device)` address space; endpoints are routing metadata, not
independent memory spaces.

Every runtime `address` points to the first transferable byte of the tensor
view, not to the allocation base. Framework offsets are metadata that are
already reflected in that address and must not be applied again. An inventory
with a non-zero `storage_offset` or `byte_offset` must explicitly pass
`address_semantics="view"`. Runtime bindings accept only an explicitly
contiguous view. Its half-open address range must have a representable
unsigned 64-bit exclusive end.

`RuntimeManifest` can represent an address-bearing snapshot without a lease.
It is not the safe consumer boundary: projection to `RuntimeBindingManifest`
requires a lease and a known generation, and transfer consumers bind the
placement and binding before using an address. Runtime inputs require an
explicit layout fingerprint, a known generation, matching per-fragment lease
generations, and canonical contiguous runtime views. Bindings are always
content-attested.

## Integration Flow

1. A framework exports runtime tensor facts and model semantics.
2. `RuntimeManifest.from_runtime_inventory()` validates the live snapshot.
3. Projection produces a reusable `PlacementManifest` and an ephemeral
   `RuntimeBindingManifest`.
4. A consumer validates and binds both halves before planning or transfer.
5. A new runtime generation creates a new binding while retaining the logical
   placement identity.

The inventory adapters accept mappings or attribute-based records and do not
import SGLang, vLLM, PyTorch, or another model framework.

## Non-Goals

This contract does not:

- generate source-to-target reshard regions;
- infer model semantics from parameter names;
- execute Transfer Engine operations;
- persist weights in Mooncake Store;
- define snapshot discovery, activation, rollback, or control-plane policy.

Those capabilities build on this contract in separate changes.
