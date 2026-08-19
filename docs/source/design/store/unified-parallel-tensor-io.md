# Unified Parallel Tensor IO

## Goal

This document defines the single source of truth for Mooncake's next-generation tensor IO API. The purpose is to keep implementation aligned around one explicit design and avoid drifting into ad hoc TP/EP/DP/PP-specific interfaces.

The target outcome is:

- one unified write API family
- one unified read API family
- one unified upsert API family
- TP-specific APIs retained only as compatibility wrappers
- minimal caller-facing inputs, with layout and planning details derived internally
- one stable abstraction that can cover TP / DP / EP / PP combinations without exploding the public API surface

## Design principles

1. **One API family, not one family per parallelism type.**
   Public APIs should not branch into separate long-term `*_with_tp`, `*_with_ep`, `*_with_pp`, and `*_with_dp` families.
2. **Parallelism is modeled as explicit axes.**
   `mixed` must not be a vague enum value. A shard should be described by the exact axis coordinates that identify it.
3. **Reads must encode caller intent explicitly.**
   A read request must say whether the caller wants the stored view, a target shard/view, or the reconstructed full tensor.
4. **Only require true caller intent.**
   If Mooncake can derive a field from the input tensor, stored metadata, or planning context, it should not be required in the public API.
5. **Planner/runtime complexity stays internal.**
   Byte ranges, payload offsets, reconstruction plans, and lowering to `get_into_ranges(...)` are runtime concerns, not public API concerns.
6. **Compatibility wrappers stay thin.**
   Existing TP methods should lower to the same unified implementation path.

## Core model

### Parallelism is an axis list

A tensor object may be identified by one axis or by multiple orthogonal axes.

Examples:

- pure TP shard: `[TP]`
- DP + TP shard: `[DP, TP]`
- PP + TP shard: `[PP, TP]`
- DP + PP + TP shard: `[DP, PP, TP]`
- DP + PP + EP + TP shard: `[DP, PP, EP, TP]`

So the design should not use:

```python
kind = "mixed"
```

Instead it should use:

```python
class ParallelAxis:
    kind: Literal["tp", "dp", "ep", "pp"]
    rank: int
    size: int
    split_dim: Optional[int] = None
    expert_id: Optional[int] = None
    stage_id: Optional[int] = None
```

```python
class TensorParallelism:
    axes: list[ParallelAxis]
```

Single-axis cases are just special cases of this model:

- TP only: `axes=[TP(...)]`
- EP only: `axes=[EP(...)]`
- PP + TP: `axes=[PP(...), TP(...)]`

## Public API shape

### Write / upsert side

```python
def put_tensor_with_parallelism(
    self,
    key: str,
    tensor: torch.Tensor,
    parallelism: TensorParallelism | None = None,
    replica: ReplicateConfig | None = None,
) -> int
```

```python
def batch_put_tensor_with_parallelism(
    self,
    keys: list[str],
    tensors: list[torch.Tensor],
    parallelisms: list[TensorParallelism | None] | None = None,
    replica: ReplicateConfig | None = None,
) -> list[int]
```

```python
def upsert_tensor_with_parallelism(
    self,
    key: str,
    tensor: torch.Tensor,
    parallelism: TensorParallelism | None = None,
    replica: ReplicateConfig | None = None,
) -> int
```

```python
def batch_upsert_tensor_with_parallelism(
    self,
    keys: list[str],
    tensors: list[torch.Tensor],
    parallelisms: list[TensorParallelism | None] | None = None,
    replica: ReplicateConfig | None = None,
) -> list[int]
```

### Read side

Read-side requests need one extra structure because the request must encode not only the target parallel coordinates, but also the materialization mode.

```python
class ReadTarget:
    mode: Literal["as_stored", "shard", "full"]
    parallelism: TensorParallelism | None = None
```

```python
def get_tensor_with_parallelism(
    self,
    key: str,
    target: ReadTarget | None = None,
) -> torch.Tensor
```

```python
def batch_get_tensor_with_parallelism(
    self,
    keys: list[str],
    targets: list[ReadTarget | None] | None = None,
) -> list[torch.Tensor]
```

Optional zero-copy forms follow the same model:

```python
def get_tensor_with_parallelism_into(
    self,
    key: str,
    buffer_ptr: int,
    size: int,
    target: ReadTarget | None = None,
) -> torch.Tensor
```

```python
def batch_get_tensor_with_parallelism_into(
    self,
    keys: list[str],
    buffer_ptrs: list[int],
    sizes: list[int],
    targets: list[ReadTarget | None] | None = None,
) -> list[torch.Tensor]
```

## Caller inputs vs internal derived fields

### Caller must provide

These are true caller intent and belong in the public API:

- the tensor itself
- whether the write target is full or shard-like
- the shard identity axes when writing a shard object
- the read target mode: `as_stored`, `shard`, or `full`
- the target axis coordinates when requesting a target shard/view
- `ReplicateConfig` when the caller wants replication / publish-like behavior

### Mooncake should derive internally

These should not be mandatory public inputs when they are derivable:

- logical shape
- local shard shape
- metadata encoding/version details
- payload offsets
- source and destination byte ranges
- reconstruction plans
- lowering to `get_into_ranges(...)`

## Why `ReadTarget` is required

`parallelism` alone is not enough to describe a read.

For example, if a tensor is stored as TP shards, a request that carries TP axis coordinates is ambiguous unless it also says whether the caller wants:

- the stored local shard
- a target shard/view
- the reconstructed full tensor

So reads must explicitly encode:

```python
ReadTarget(mode="as_stored")
ReadTarget(mode="shard", parallelism=...)
ReadTarget(mode="full")
```

This distinction must not be guessed from the axis metadata.

## Mixed-parallel scenarios in training and inference

`mixed` is not a mode. It means the shard identity needs more than one axis coordinate to be uniquely described.

### Scenario table

| Scenario | Typical axis list | Meaning |
|---|---|---|
| TP training | `[TP]` | one TP slice of a logical tensor |
| DP + TP training | `[DP, TP]` | one TP slice within one DP replica/group |
| PP + TP training | `[PP, TP]` | one TP slice owned by one pipeline stage |
| DP + PP + TP training | `[DP, PP, TP]` | one TP slice in one stage in one DP replica |
| DP + PP + EP + TP training | `[DP, PP, EP, TP]` | one expert-local TP slice in one stage and one DP replica |
| TP inference | `[TP]` | one TP slice used by one inference rank |
| PP + TP inference | `[PP, TP]` | one TP slice owned by one inference pipeline stage |
| EP inference | `[EP]` or `[EP, TP]` | one expert shard, optionally further TP-sliced |
| multi-replica serving inference | `[DP, TP]` or `[DP, PP, TP]` | one shard scoped to a serving replica plus model-parallel axes |

### Important distinctions

- TP and EP often affect the tensor's physical layout directly.
- PP and serving-replica / DP often act more like ownership or scope tags, even when they do not themselves change the byte layout inside the local shard.

## Write-side matrix

The unified write family should be driven by the identity of the object being written, not by method-name proliferation.

| Caller holds | Wants to store | API | Required `parallelism` |
|---|---|---|---|
| full tensor | full tensor | `put_tensor_with_parallelism(..., parallelism=None)` | none |
| full tensor | TP shard | `put_tensor_with_parallelism(...)` | `axes=[TP(rank,size,split_dim)]` |
| full tensor | DP-scoped shard/replica | `put_tensor_with_parallelism(...)` | `axes=[DP(rank,size)]` plus layout axis if actually sharded |
| full tensor | EP shard | `put_tensor_with_parallelism(...)` | `axes=[EP(rank,size,expert_id)]` plus `split_dim` if needed |
| full tensor | PP stage shard | `put_tensor_with_parallelism(...)` | `axes=[PP(rank,size,stage_id)]` |
| full tensor | combined shard | `put_tensor_with_parallelism(...)` | explicit axis list such as `[PP(...), TP(...)]` |
| shard tensor | shard object | `put_tensor_with_parallelism(...)` | explicit axis list describing that shard identity |

For TP-containing **multi-axis** layouts, the write semantic is now: the caller may pass the **full source tensor**, and the provided TP rank/layout tells Mooncake which uniform shard to materialize and persist. Callers no longer need to pre-split the tensor themselves for `dp_tp` / `pp_tp` / `ep_tp` style writes.

Single-axis TP compatibility wrappers and the preserved plain-TP `with_parallelism` behavior still accept shard input rather than auto-materializing from a full tensor.

Pure DP still does not invent a split axis on its own. If the stored object is actually sharded, the request must still include the layout axis that defines the shard shape.

The same matrix applies to `upsert_tensor_with_parallelism(...)`.

## Read-side matrix

The unified read family should be driven by `ReadTarget`.

| Stored layout | Caller wants | API | `ReadTarget` |
|---|---|---|---|
| full tensor | stored full tensor | `get_tensor_with_parallelism(...)` | `None` or `mode="as_stored"` |
| TP shard object | stored shard | `get_tensor_with_parallelism(...)` | `mode="as_stored"` |
| TP shard set | target TP shard | `get_tensor_with_parallelism(...)` | `mode="shard", parallelism=TP(...)` |
| TP shard set | full tensor | `get_tensor_with_parallelism(...)` | `mode="full"` |
| EP shard set | target expert shard | `get_tensor_with_parallelism(...)` | `mode="shard", parallelism=EP(...)` |
| EP shard set | full tensor | `get_tensor_with_parallelism(...)` | `mode="full"` |
| PP + TP shard set | target PP+TP shard | `get_tensor_with_parallelism(...)` | `mode="shard", parallelism=[PP(...), TP(...)]` |
| mixed shard set | full tensor | `get_tensor_with_parallelism(...)` | `mode="full"` |

## Source-layout to target-layout matrix

The planner must eventually cover these conversions, but the public API should remain the same across all of them.

| Stored source layout | Requested target layout | Support model |
|---|---|---|
| TP | TP | direct shard fetch or shard-local fast path |
| TP | full | reconstruct full tensor |
| TP | EP / PP / DP / mixed | planner-driven remap |
| EP | EP | direct shard fetch or shard-local fast path |
| EP | full | reconstruct full tensor |
| EP | TP / PP / DP / mixed | planner-driven remap |
| PP | PP | direct shard fetch |
| PP | full | reconstruct full tensor when meaningful |
| PP | TP / EP / DP / mixed | planner-driven remap |
| mixed | mixed | direct fetch if exact match, else planner-driven remap |
| mixed | full | reconstruct full tensor |
| mixed | TP / EP / PP / DP | planner-driven remap |

The important point is that these combinations must not create public API explosion.

## Compatibility wrappers

Existing TP APIs remain compatibility wrappers.

Conceptually:

```python
put_tensor_with_tp(key, tensor, tp_rank, tp_size, split_dim)
```

lowers to:

```python
put_tensor_with_parallelism(
    key,
    tensor,
    TensorParallelism(axes=[TP(rank=tp_rank, size=tp_size, split_dim=split_dim)]),
)
```

and:

```python
get_tensor_with_tp(key, tp_rank, tp_size, split_dim)
```

lowers to:

```python
get_tensor_with_parallelism(
    key,
    ReadTarget(
        mode="shard",
        parallelism=TensorParallelism(
            axes=[TP(rank=tp_rank, size=tp_size, split_dim=split_dim)]
        ),
    ),
)
```

Existing TP behavior stays stable, but implementation should flow through the unified path.

## Runtime lowering direction

Internally, reads should be planner-driven.

When a request can be lowered to explicit:

- key
- src offset
- dst offset
- size

ranges, the runtime should reuse `get_into_ranges(...)` to assemble the result directly into the output buffer.

Otherwise it can fall back to the simpler whole-object path.

This keeps the public interface stable while allowing future planner work to add DP / TP / EP / PP remapping and optimized reconstruction without another API redesign.

## Current implementation status

The current `store_py.cpp` implementation now reflects the main shape of this design:

- unified write APIs are exposed as `put_tensor_with_parallelism(...)` and `batch_put_tensor_with_parallelism(...)`
- unified read APIs are exposed as `get_tensor_with_parallelism(...)` and `batch_get_tensor_with_parallelism(...)`
- unified upsert APIs are exposed as `upsert_tensor_with_parallelism(...)` and `batch_upsert_tensor_with_parallelism(...)`
- zero-copy `_into` and `_from` variants exist for the unified API family
- TP-specific APIs remain available as compatibility wrappers and should not be treated as the long-term surface area

### Implemented write-side convenience: `writer_partitions`

Batch write and batch upsert paths also support `writer_partitions` as a convenience input for full tensors that should be written as stored shards.

This is intentionally narrower than the full `TensorParallelism` model:

- it is a write-side convenience, not a replacement for `TensorParallelism`
- it is primarily for batch full-tensor writes where the caller already knows rank / size / split_dim per item
- it should not change the unified read-side abstraction

`writer_partitions` remains a separate explicit route. The newer TP-containing `parallelism` write semantic now overlaps with it for the common case of “full tensor in, store one requested shard”, but `writer_partitions` is still useful when the caller wants a lighter write-side request shape without constructing `TensorParallelism` objects.

### Implemented read-side behavior

Read-side support includes:

- returning the stored local object (`mode="as_stored"`)
- returning a target shard (`mode="shard"`)
- reconstructing the full tensor (`mode="full"`)
- lowering reconstruction-oriented paths onto existing runtime helpers such as `get_into_ranges(...)` where appropriate

### Compatibility boundary

The project should continue to preserve this boundary:

- keep old TP APIs functional
- keep them thin
- do not expand the old TP-specific family as the primary interface
- document and evolve the unified `*_with_parallelism` family instead

## Scope still intentionally limited

This document describes the stable public API direction, but not every theoretical source-layout to target-layout remap is fully implemented.

In particular, the design should continue to avoid over-promising planner coverage for arbitrary remaps across all DP / TP / EP / PP combinations until those paths are explicitly implemented and tested.

The safe documented contract today is:

1. define the public API structures and signatures clearly
2. align `store_py.cpp` with `axes + ReadTarget`
3. keep TP wrappers working by lowering into the unified path
4. do not introduce a new long-term `kind="mixed"` model
5. do not push planner internals such as byte ranges and derived shapes into public arguments
6. do not expand into many parallelism-specific public methods
7. reuse existing runtime helpers like `get_into_ranges(...)` rather than inventing a parallel reconstruction path from scratch
