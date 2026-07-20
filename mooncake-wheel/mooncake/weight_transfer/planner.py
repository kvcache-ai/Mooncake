from __future__ import annotations

from dataclasses import dataclass
from math import prod
from typing import Iterable, Sequence

from .manifest import (
    ParallelRank,
    RuntimeFragment,
    RuntimeManifest,
    StoredFragment,
    TensorDescriptor,
    WeightManifest,
)


SourceFragment = RuntimeFragment | StoredFragment


@dataclass(frozen=True)
class RuntimeLeaseSnapshot:
    fragment_id: str
    tensor_id: str
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    address: int
    nbytes: int
    worker_id: str
    endpoint: str
    lease_generation: int

    @classmethod
    def from_fragment(cls, fragment: RuntimeFragment) -> RuntimeLeaseSnapshot:
        return cls(
            fragment_id=fragment.fragment_id,
            tensor_id=fragment.tensor_id,
            global_offset=fragment.global_offset,
            local_shape=fragment.local_shape,
            address=fragment.address,
            nbytes=fragment.nbytes,
            worker_id=fragment.worker_id,
            endpoint=fragment.endpoint,
            lease_generation=fragment.lease_generation,
        )


@dataclass(frozen=True)
class ExecutorTransferPlan:
    instance_id: str
    worker_id: str
    rank: ParallelRank
    fragment_ids: tuple[str, ...]
    fragment_leases: tuple[RuntimeLeaseSnapshot, ...]
    operation_indices: tuple[int, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "fragment_ids", tuple(self.fragment_ids))
        object.__setattr__(self, "fragment_leases", tuple(self.fragment_leases))
        object.__setattr__(self, "operation_indices", tuple(self.operation_indices))
        if not self.instance_id or not self.worker_id or not self.fragment_ids:
            raise ValueError("executor plan identifiers must not be empty")
        if len(self.fragment_ids) != len(set(self.fragment_ids)):
            raise ValueError("executor plan has duplicate fragment IDs")
        if not all(
            isinstance(lease, RuntimeLeaseSnapshot) for lease in self.fragment_leases
        ):
            raise ValueError("executor plan has invalid runtime lease metadata")
        if (
            tuple(lease.fragment_id for lease in self.fragment_leases)
            != self.fragment_ids
        ):
            raise ValueError("executor plan fragment lease IDs do not match")
        if any(type(index) is not int or index < 0 for index in self.operation_indices):
            raise ValueError("executor operation indices must be non-negative integers")


@dataclass(frozen=True)
class CopyRange:
    tensor_id: str
    source: SourceFragment
    target: RuntimeFragment
    source_offset: int
    target_offset: int
    nbytes: int
    repeat: int = 1
    source_stride: int = 0
    target_stride: int = 0

    def __post_init__(self) -> None:
        if not self.tensor_id:
            raise ValueError("copy range tensor_id must not be empty")
        if (
            self.source.tensor_id != self.tensor_id
            or self.target.tensor_id != self.tensor_id
        ):
            raise ValueError("copy range tensor mismatch")
        for name in (
            "source_offset",
            "target_offset",
            "nbytes",
            "repeat",
            "source_stride",
            "target_stride",
        ):
            value = getattr(self, name)
            if type(value) is not int:
                raise ValueError(f"copy range {name} must be an integer")
        if (
            min(
                self.source_offset,
                self.target_offset,
                self.source_stride,
                self.target_stride,
            )
            < 0
        ):
            raise ValueError("copy range values must be non-negative")
        if self.nbytes <= 0 or self.repeat <= 0:
            raise ValueError("copy range size and repeat must be positive")
        self.validate_bounds()

    def validate_bounds(self) -> None:
        source_end = (
            self.source_offset + (self.repeat - 1) * self.source_stride + self.nbytes
        )
        if source_end > self.source.nbytes:
            raise ValueError("copy range exceeds source fragment")
        target_end = (
            self.target_offset + (self.repeat - 1) * self.target_stride + self.nbytes
        )
        if target_end > self.target.nbytes:
            raise ValueError("copy range exceeds target fragment")

    @property
    def total_bytes(self) -> int:
        return self.nbytes * self.repeat

    def iter_segments(self) -> Iterable[tuple[int, int, int]]:
        for index in range(self.repeat):
            yield (
                self.source_offset + index * self.source_stride,
                self.target_offset + index * self.target_stride,
                self.nbytes,
            )


@dataclass(frozen=True)
class TransferPlan:
    model_id: str
    revision: str
    operations: tuple[CopyRange, ...]
    source_executors: tuple[ExecutorTransferPlan, ...] = ()
    target_executors: tuple[ExecutorTransferPlan, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "operations", tuple(self.operations))
        object.__setattr__(self, "source_executors", tuple(self.source_executors))
        object.__setattr__(self, "target_executors", tuple(self.target_executors))
        if not self.model_id or not self.revision:
            raise ValueError("transfer plan identifiers must not be empty")
        for executor in (*self.source_executors, *self.target_executors):
            if any(
                index >= len(self.operations) for index in executor.operation_indices
            ):
                raise ValueError("executor operation index is out of range")

    @property
    def total_bytes(self) -> int:
        return sum(operation.total_bytes for operation in self.operations)


def _collect_manifests(
    manifests: Sequence[RuntimeManifest], label: str
) -> tuple[dict[str, TensorDescriptor], list[RuntimeFragment]]:
    if not manifests:
        raise ValueError(f"{label} manifests must not be empty")

    model_id = manifests[0].model_id
    revision = manifests[0].revision
    tensors: dict[str, TensorDescriptor] = {}
    fragments: list[RuntimeFragment] = []
    fragment_ids: set[str] = set()
    for manifest in manifests:
        if manifest.model_id != model_id or manifest.revision != revision:
            raise ValueError(f"{label} manifests describe different revisions")
        for tensor in manifest.tensors:
            previous = tensors.setdefault(tensor.tensor_id, tensor)
            if previous != tensor:
                raise ValueError(
                    f"{label} tensor descriptor mismatch: {tensor.tensor_id}"
                )
        for fragment in manifest.fragments:
            if fragment.fragment_id in fragment_ids:
                raise ValueError(
                    f"duplicate {label} fragment_id: {fragment.fragment_id}"
                )
            fragment_ids.add(fragment.fragment_id)
            fragments.append(fragment)
    return tensors, fragments


def _build_executor_plans(
    manifests: Sequence[RuntimeManifest],
    operations: Sequence[CopyRange],
    side: str,
) -> tuple[ExecutorTransferPlan, ...]:
    if side not in ("source", "target"):
        raise ValueError(f"invalid executor side: {side}")
    result = []
    ranks: set[ParallelRank] = set()
    operation_indices_by_fragment: dict[str, list[int]] = {}
    for index, operation in enumerate(operations):
        fragment_id = getattr(operation, side).fragment_id
        operation_indices_by_fragment.setdefault(fragment_id, []).append(index)
    for manifest in manifests:
        if not manifest.fragments:
            raise ValueError(f"{side} executor manifest has no fragments")
        fragments_by_rank: dict[ParallelRank, list[RuntimeFragment]] = {}
        for fragment in manifest.fragments:
            fragments_by_rank.setdefault(fragment.rank, []).append(fragment)
        for rank, fragments in fragments_by_rank.items():
            workers = {fragment.worker_id for fragment in fragments}
            if len(workers) != 1:
                raise ValueError(f"{side} executor rank spans multiple workers: {rank}")
            if rank in ranks:
                raise ValueError(f"duplicate {side} executor rank: {rank}")
            ranks.add(rank)
            ordered_fragments = sorted(
                fragments, key=lambda fragment: fragment.fragment_id
            )
            fragment_ids = tuple(fragment.fragment_id for fragment in ordered_fragments)
            operation_indices = tuple(
                sorted(
                    index
                    for fragment_id in fragment_ids
                    for index in operation_indices_by_fragment.get(fragment_id, ())
                )
            )
            result.append(
                ExecutorTransferPlan(
                    instance_id=manifest.instance_id,
                    worker_id=next(iter(workers)),
                    rank=rank,
                    fragment_ids=fragment_ids,
                    fragment_leases=tuple(
                        RuntimeLeaseSnapshot.from_fragment(fragment)
                        for fragment in ordered_fragments
                    ),
                    operation_indices=operation_indices,
                )
            )
    result.sort(
        key=lambda item: (item.rank.dp, item.rank.pp, item.rank.ep, item.rank.tp)
    )
    return tuple(result)


def resolve_executor_plans(
    plan: TransferPlan,
    manifest: RuntimeManifest,
    side: str,
) -> tuple[ExecutorTransferPlan, ...]:
    if side == "source":
        executors = plan.source_executors
    elif side == "target":
        executors = plan.target_executors
    else:
        raise ValueError(f"invalid executor side: {side}")
    if not executors:
        raise ValueError(f"transfer plan has no {side} executor metadata")
    if not manifest.fragments:
        raise ValueError(f"{side} executor snapshot mismatch: no fragments")
    expected_executors = tuple(
        executor
        for executor in executors
        if executor.instance_id == manifest.instance_id
    )
    if not expected_executors:
        raise ValueError(f"{side} executor snapshot mismatch: unknown instance")
    executor_by_rank = {executor.rank: executor for executor in expected_executors}
    fragments_by_rank: dict[ParallelRank, list[RuntimeFragment]] = {}
    for fragment in manifest.fragments:
        fragments_by_rank.setdefault(fragment.rank, []).append(fragment)
    if set(fragments_by_rank) != set(executor_by_rank):
        raise ValueError(f"{side} executor snapshot mismatch: executor set changed")
    result = []
    for rank, fragments in fragments_by_rank.items():
        executor = executor_by_rank.get(rank)
        workers = {fragment.worker_id for fragment in fragments}
        ordered_fragments = sorted(fragments, key=lambda fragment: fragment.fragment_id)
        current_ids = tuple(fragment.fragment_id for fragment in ordered_fragments)
        current_leases = tuple(
            RuntimeLeaseSnapshot.from_fragment(fragment)
            for fragment in ordered_fragments
        )
        if executor is None or len(workers) != 1:
            raise ValueError(f"{side} executor snapshot mismatch: unknown rank {rank}")
        if (
            manifest.instance_id != executor.instance_id
            or next(iter(workers)) != executor.worker_id
            or current_ids != executor.fragment_ids
            or current_leases != executor.fragment_leases
        ):
            raise ValueError(f"{side} executor snapshot mismatch")
        result.append(executor)
    result.sort(
        key=lambda item: (item.rank.dp, item.rank.pp, item.rank.ep, item.rank.tp)
    )
    return tuple(result)


def resolve_executor_plan(
    plan: TransferPlan,
    manifest: RuntimeManifest,
    side: str,
) -> ExecutorTransferPlan:
    executors = resolve_executor_plans(plan, manifest, side)
    if len(executors) != 1:
        raise ValueError(f"{side} executor snapshot contains multiple ranks")
    return executors[0]


def _validate_tensor_compatibility(
    source: TensorDescriptor, target: TensorDescriptor
) -> None:
    if source.layout_fingerprint != target.layout_fingerprint:
        raise ValueError(f"layout mismatch for tensor {source.tensor_id}")
    if (
        source.global_shape != target.global_shape
        or source.dtype != target.dtype
        or source.itemsize != target.itemsize
        or source.partition_dim != target.partition_dim
        or source.layer_id != target.layer_id
        or source.expert_id != target.expert_id
    ):
        raise ValueError(f"tensor descriptor mismatch: {source.tensor_id}")


def _validate_tensor_sets(
    source_tensors: dict[str, TensorDescriptor],
    target_tensors: dict[str, TensorDescriptor],
) -> None:
    source_ids = set(source_tensors)
    target_ids = set(target_tensors)
    missing = sorted(source_ids - target_ids)
    if missing:
        raise ValueError(f"target manifests are missing tensors: {', '.join(missing)}")
    unexpected = sorted(target_ids - source_ids)
    if unexpected:
        raise ValueError(
            f"target manifests contain unknown tensors: {', '.join(unexpected)}"
        )


def _validate_tensor_subset(
    source_tensors: dict[str, TensorDescriptor],
    target_tensors: dict[str, TensorDescriptor],
) -> None:
    unexpected = sorted(set(target_tensors) - set(source_tensors))
    if unexpected:
        raise ValueError(
            f"target manifests contain unknown tensors: {', '.join(unexpected)}"
        )


def _validate_target_coverage(
    target_tensors: dict[str, TensorDescriptor],
    target_fragments: Sequence[RuntimeFragment],
) -> None:
    if not target_fragments:
        raise ValueError("target manifests have no fragments")
    dp_ranks = sorted({fragment.rank.dp for fragment in target_fragments})
    for dp_rank in dp_ranks:
        for tensor in target_tensors.values():
            fragments_by_owner: dict[tuple[int, int], list[RuntimeFragment]] = {}
            for fragment in target_fragments:
                if (
                    fragment.rank.dp != dp_rank
                    or fragment.tensor_id != tensor.tensor_id
                ):
                    continue
                fragments_by_owner.setdefault(
                    (fragment.rank.pp, fragment.rank.ep), []
                ).append(fragment)
            if not fragments_by_owner or any(
                not _fragments_fully_cover_tensor(tensor, fragments)
                for fragments in fragments_by_owner.values()
            ):
                raise ValueError(
                    f"target tensor is not fully covered: {tensor.tensor_id}: "
                    f"dp={dp_rank}"
                )


def _fragments_fully_cover_tensor(
    tensor: TensorDescriptor,
    fragments: Sequence[SourceFragment],
) -> bool:
    geometries = {
        (fragment.global_offset, fragment.local_shape): fragment
        for fragment in fragments
        if fragment.tensor_id == tensor.tensor_id
    }
    unique_fragments = tuple(geometries.values())
    if tensor.partition_dim is None:
        return len(unique_fragments) == 1
    dim = tensor.partition_dim
    intervals = sorted(
        (
            fragment.global_offset[dim],
            fragment.global_offset[dim] + fragment.local_shape[dim],
        )
        for fragment in unique_fragments
    )
    cursor = 0
    for begin, end in intervals:
        if begin != cursor:
            return False
        cursor = end
    return cursor == tensor.global_shape[dim]


def _complete_runtime_source_replicas(
    source_tensors: dict[str, TensorDescriptor],
    source_fragments: Sequence[RuntimeFragment],
) -> dict[int, dict[str, tuple[int, int]]]:
    replicas: dict[int, dict[str, tuple[int, int]]] = {}
    generation_by_dp: dict[int, int] = {}
    for dp_rank in sorted({fragment.rank.dp for fragment in source_fragments}):
        replica_fragments = [
            fragment for fragment in source_fragments if fragment.rank.dp == dp_rank
        ]
        generations = {fragment.lease_generation for fragment in replica_fragments}
        if len(generations) != 1:
            continue
        owner_by_tensor: dict[str, tuple[int, int]] = {}
        complete = True
        for tensor in source_tensors.values():
            fragments_by_owner: dict[tuple[int, int], list[RuntimeFragment]] = {}
            for fragment in replica_fragments:
                if fragment.tensor_id != tensor.tensor_id:
                    continue
                fragments_by_owner.setdefault(
                    (fragment.rank.pp, fragment.rank.ep), []
                ).append(fragment)
            complete_owners = [
                owner
                for owner, fragments in fragments_by_owner.items()
                if _fragments_fully_cover_tensor(tensor, fragments)
            ]
            if not fragments_by_owner or len(complete_owners) != len(
                fragments_by_owner
            ):
                complete = False
                break
            owner_by_tensor[tensor.tensor_id] = min(complete_owners)
        if complete:
            replicas[dp_rank] = owner_by_tensor
            generation_by_dp[dp_rank] = next(iter(generations))
    if not replicas:
        raise ValueError(
            "source manifests have no complete DP replica; tensors are not fully covered"
        )
    if len(set(generation_by_dp.values())) != 1:
        raise ValueError("source DP replicas have inconsistent lease generations")
    return replicas


def _validate_local_target_inventory(
    target_tensors: dict[str, TensorDescriptor],
    target_fragments: Sequence[RuntimeFragment],
) -> None:
    if not target_fragments:
        raise ValueError("local target manifest has no fragments")
    ranks = {fragment.rank for fragment in target_fragments}
    workers = {fragment.worker_id for fragment in target_fragments}
    if len(ranks) != 1 or len(workers) != 1:
        raise ValueError("local target manifest must describe exactly one executor")
    missing = sorted(
        set(target_tensors) - {item.tensor_id for item in target_fragments}
    )
    if missing:
        raise ValueError(
            f"local target manifest is missing fragments: {', '.join(missing)}"
        )


def _geometry_key(fragment: SourceFragment) -> tuple:
    return fragment.tensor_id, fragment.global_offset, fragment.local_shape


def _source_sort_key(fragment: SourceFragment) -> tuple:
    if isinstance(fragment, StoredFragment):
        return (0, 0, 0, 0, fragment.object_key, fragment.fragment_id)
    return (
        fragment.rank.dp,
        fragment.rank.pp,
        fragment.rank.ep,
        fragment.rank.tp,
        fragment.worker_id,
        fragment.fragment_id,
    )


def _target_interval(
    tensor: TensorDescriptor, fragment: SourceFragment
) -> tuple[int, int]:
    if tensor.partition_dim is None:
        return 0, 1
    dim = tensor.partition_dim
    start = fragment.global_offset[dim]
    return start, start + fragment.local_shape[dim]


def _overlap(
    tensor: TensorDescriptor,
    source: SourceFragment,
    target: RuntimeFragment,
) -> tuple[int, int] | None:
    source_start, source_end = _target_interval(tensor, source)
    target_start, target_end = _target_interval(tensor, target)
    begin = max(source_start, target_start)
    end = min(source_end, target_end)
    if begin >= end:
        return None
    return begin, end


def _copy_ranges(
    tensor: TensorDescriptor,
    source: SourceFragment,
    target: RuntimeFragment,
    begin: int,
    end: int,
) -> Iterable[CopyRange]:
    dim = tensor.partition_dim
    if dim is None:
        yield CopyRange(
            tensor_id=tensor.tensor_id,
            source=source,
            target=target,
            source_offset=0,
            target_offset=0,
            nbytes=source.nbytes,
        )
        return

    source_start, _ = _target_interval(tensor, source)
    target_start, _ = _target_interval(tensor, target)
    source_extent = source.local_shape[dim]
    target_extent = target.local_shape[dim]
    prefix = prod(tensor.global_shape[:dim])
    suffix = prod(tensor.global_shape[dim + 1 :])
    row_bytes = (end - begin) * suffix * tensor.itemsize

    source_stride = source_extent * suffix * tensor.itemsize
    target_stride = target_extent * suffix * tensor.itemsize
    if source_stride == row_bytes and target_stride == row_bytes:
        yield CopyRange(
            tensor_id=tensor.tensor_id,
            source=source,
            target=target,
            source_offset=(begin - source_start) * suffix * tensor.itemsize,
            target_offset=(begin - target_start) * suffix * tensor.itemsize,
            nbytes=row_bytes * prefix,
        )
        return
    yield CopyRange(
        tensor_id=tensor.tensor_id,
        source=source,
        target=target,
        source_offset=(begin - source_start) * suffix * tensor.itemsize,
        target_offset=(begin - target_start) * suffix * tensor.itemsize,
        nbytes=row_bytes,
        repeat=prefix,
        source_stride=source_stride,
        target_stride=target_stride,
    )


def _plan_transfer(
    model_id: str,
    revision: str,
    source_tensors: dict[str, TensorDescriptor],
    source_fragments: Sequence[SourceFragment],
    target_tensors: dict[str, TensorDescriptor],
    target_fragments: Sequence[RuntimeFragment],
    *,
    local_target: bool = False,
) -> TransferPlan:
    if local_target:
        _validate_tensor_subset(source_tensors, target_tensors)
        _validate_local_target_inventory(target_tensors, target_fragments)
    else:
        _validate_tensor_sets(source_tensors, target_tensors)
        _validate_target_coverage(target_tensors, target_fragments)
    runtime_sources = all(
        isinstance(fragment, RuntimeFragment) for fragment in source_fragments
    )
    stored_sources = all(
        isinstance(fragment, StoredFragment) for fragment in source_fragments
    )
    if not runtime_sources and not stored_sources:
        raise ValueError("source fragments mix runtime and stored locations")
    source_replicas = (
        _complete_runtime_source_replicas(source_tensors, source_fragments)
        if runtime_sources
        else {}
    )
    source_dp_ranks = sorted(source_replicas)
    source_dp_by_target_dp = (
        {
            target_dp: source_dp_ranks[target_dp % len(source_dp_ranks)]
            for target_dp in {fragment.rank.dp for fragment in target_fragments}
        }
        if runtime_sources
        else {}
    )
    candidates: dict[str, dict[tuple, list[SourceFragment]]] = {}
    for fragment in source_fragments:
        candidates.setdefault(fragment.tensor_id, {}).setdefault(
            _geometry_key(fragment), []
        ).append(fragment)
    for tensor_candidates in candidates.values():
        for group in tensor_candidates.values():
            group.sort(key=_source_sort_key)

    operations: list[CopyRange] = []
    for target in sorted(target_fragments, key=lambda item: item.fragment_id):
        target_tensor = target_tensors[target.tensor_id]
        source_tensor = source_tensors.get(target.tensor_id)
        if source_tensor is None:
            raise ValueError(f"missing source tensor: {target.tensor_id}")
        _validate_tensor_compatibility(source_tensor, target_tensor)

        overlaps: list[tuple[int, int, SourceFragment]] = []
        for group in candidates.get(target.tensor_id, {}).values():
            if runtime_sources:
                source_dp = source_dp_by_target_dp[target.rank.dp]
                source_owner = source_replicas[source_dp][target.tensor_id]
                eligible = [
                    fragment
                    for fragment in group
                    if isinstance(fragment, RuntimeFragment)
                    and fragment.rank.dp == source_dp
                    and (fragment.rank.pp, fragment.rank.ep) == source_owner
                ]
                if not eligible:
                    continue
                representative = eligible[0]
                selected = representative
            else:
                representative = group[0]
                selected = representative
            interval = _overlap(source_tensor, representative, target)
            if interval is None:
                continue
            overlaps.append((*interval, selected))
        overlaps.sort(key=lambda item: (item[0], item[1], item[2].fragment_id))

        target_start, target_end = _target_interval(target_tensor, target)
        cursor = target_start
        for begin, end, source in overlaps:
            if begin != cursor:
                raise ValueError(
                    f"target fragment is not fully covered: {target.fragment_id}"
                )
            cursor = end
            operations.extend(_copy_ranges(target_tensor, source, target, begin, end))
        if cursor != target_end:
            raise ValueError(
                f"target fragment is not fully covered: {target.fragment_id}"
            )

    operations.sort(
        key=lambda item: (
            item.target.fragment_id,
            item.target_offset,
            item.source.fragment_id,
            item.source_offset,
        )
    )
    return TransferPlan(
        model_id=model_id,
        revision=revision,
        operations=tuple(operations),
    )


def _operation_sort_key(operation: CopyRange) -> tuple:
    source_location = (
        (
            "runtime",
            operation.source.worker_id,
            operation.source.endpoint,
            operation.source.lease_generation,
            operation.source.address + operation.source_offset,
        )
        if isinstance(operation.source, RuntimeFragment)
        else (
            "stored",
            operation.source.object_key,
            operation.source.object_offset + operation.source_offset,
        )
    )
    return (
        operation.tensor_id,
        operation.target.worker_id,
        operation.target.endpoint,
        operation.target.address + operation.target_offset,
        source_location,
    )


def _source_copy_identity(operation: CopyRange) -> tuple:
    if isinstance(operation.source, RuntimeFragment):
        return (
            "runtime",
            operation.source.worker_id,
            operation.source.endpoint,
            operation.source.lease_generation,
            operation.source.address + operation.source_offset,
        )
    return (
        "stored",
        operation.source.object_key,
        operation.source.object_offset + operation.source_offset,
    )


def _target_copy_identity(operation: CopyRange) -> tuple:
    return (
        operation.target.worker_id,
        operation.target.endpoint,
        operation.target.lease_generation,
        operation.target.address + operation.target_offset,
        operation.nbytes,
        operation.repeat,
        operation.target_stride,
    )


def _descriptor_alias_key(descriptor: TensorDescriptor) -> tuple:
    return (
        descriptor.global_shape,
        descriptor.dtype,
        descriptor.itemsize,
        descriptor.partition_dim,
        descriptor.layer_id,
        descriptor.expert_id,
        descriptor.layout_fingerprint,
    )


def _is_declared_target_alias(
    left: CopyRange,
    right: CopyRange,
    source_tensors: dict[str, TensorDescriptor],
    target_tensors: dict[str, TensorDescriptor],
) -> bool:
    left_target = left.target
    right_target = right.target
    if (
        len(left_target.aliases) < 2
        or left_target.aliases != right_target.aliases
        or left_target.worker_id != right_target.worker_id
        or left_target.endpoint != right_target.endpoint
        or left_target.lease_generation != right_target.lease_generation
        or left_target.address != right_target.address
        or left_target.nbytes != right_target.nbytes
        or left_target.global_offset != right_target.global_offset
        or left_target.local_shape != right_target.local_shape
        or left.source.global_offset != right.source.global_offset
        or left.source.local_shape != right.source.local_shape
        or left.source_offset != right.source_offset
        or left.source_stride != right.source_stride
    ):
        return False
    return (
        _descriptor_alias_key(source_tensors[left.tensor_id])
        == _descriptor_alias_key(source_tensors[right.tensor_id])
        == _descriptor_alias_key(target_tensors[left.tensor_id])
        == _descriptor_alias_key(target_tensors[right.tensor_id])
    )


def _deduplicate_target_copies(
    operations: Sequence[CopyRange],
    source_tensors: dict[str, TensorDescriptor],
    target_tensors: dict[str, TensorDescriptor],
) -> tuple[CopyRange, ...]:
    result = []
    seen = set()
    for operation in sorted(operations, key=_operation_sort_key):
        identity = (
            _source_copy_identity(operation),
            _target_copy_identity(operation),
            operation.nbytes,
            operation.repeat,
            operation.source_stride,
        )
        if identity in seen:
            continue
        seen.add(identity)
        result.append(operation)

    by_target: dict[tuple, CopyRange] = {}
    deduplicated = []
    for operation in result:
        identity = _target_copy_identity(operation)
        previous = by_target.get(identity)
        if previous is None:
            by_target[identity] = operation
            deduplicated.append(operation)
            continue
        if _is_declared_target_alias(
            previous,
            operation,
            source_tensors,
            target_tensors,
        ):
            continue
        deduplicated.append(operation)

    result = sorted(
        deduplicated,
        key=lambda item: (
            item.target.fragment_id,
            item.target_offset,
            item.source.fragment_id,
            item.source_offset,
        ),
    )
    _validate_target_physical_ranges(result)
    return tuple(result)


def _target_physical_bounds(operation: CopyRange) -> tuple[int, int]:
    begin = operation.target.address + operation.target_offset
    end = begin + (operation.repeat - 1) * operation.target_stride + operation.nbytes
    return begin, end


def _target_segments_overlap(left: CopyRange, right: CopyRange) -> bool:
    left_begin = left.target.address + left.target_offset
    right_begin = right.target.address + right.target_offset
    left_repeat = 1 if left.target_stride == 0 else left.repeat
    right_repeat = 1 if right.target_stride == 0 else right.repeat

    if left.target_stride == right.target_stride and left.target_stride > 0:
        stride = left.target_stride
        difference = left_begin - right_begin
        minimum = -(left.nbytes - 1)
        maximum = right.nbytes - 1
        lowest_delta = -(right_repeat - 1)
        highest_delta = left_repeat - 1
        first = max(lowest_delta, -((-(minimum - difference)) // stride))
        last = min(highest_delta, (maximum - difference) // stride)
        return first <= last

    left_index = 0
    right_index = 0
    while left_index < left_repeat and right_index < right_repeat:
        left_start = left_begin + left_index * left.target_stride
        right_start = right_begin + right_index * right.target_stride
        left_end = left_start + left.nbytes
        right_end = right_start + right.nbytes
        if left_end <= right_start:
            if left.target_stride == 0:
                return False
            left_index = max(
                left_index + 1,
                (right_start - left.nbytes - left_begin) // left.target_stride + 1,
            )
        elif right_end <= left_start:
            if right.target_stride == 0:
                return False
            right_index = max(
                right_index + 1,
                (left_start - right.nbytes - right_begin) // right.target_stride + 1,
            )
        else:
            return True
    return False


def _validate_target_physical_ranges(operations: Sequence[CopyRange]) -> None:
    by_executor: dict[tuple[str, str], list[CopyRange]] = {}
    for operation in operations:
        if operation.repeat > 1 and operation.target_stride < operation.nbytes:
            raise ValueError(
                f"conflicting target physical range: {operation.target.fragment_id}"
            )
        by_executor.setdefault(
            (operation.target.worker_id, operation.target.endpoint), []
        ).append(operation)

    for scoped_operations in by_executor.values():
        ordered = sorted(scoped_operations, key=_target_physical_bounds)
        active: list[CopyRange] = []
        for operation in ordered:
            begin, _ = _target_physical_bounds(operation)
            active = [
                candidate
                for candidate in active
                if _target_physical_bounds(candidate)[1] > begin
            ]
            if any(
                _target_segments_overlap(candidate, operation) for candidate in active
            ):
                raise ValueError(
                    f"conflicting target physical range: {operation.target.fragment_id}"
                )
            active.append(operation)


def plan_runtime_transfer(
    source_manifests: Sequence[RuntimeManifest],
    target_manifests: Sequence[RuntimeManifest],
) -> TransferPlan:
    source_tensors, source_fragments = _collect_manifests(source_manifests, "source")
    target_tensors, target_fragments = _collect_manifests(target_manifests, "target")
    source = source_manifests[0]
    target = target_manifests[0]
    if source.model_id != target.model_id:
        raise ValueError("source and target model_id differ")
    if source.revision != target.revision:
        raise ValueError("source and target revision differ")
    transfer = _plan_transfer(
        source.model_id,
        source.revision,
        source_tensors,
        source_fragments,
        target_tensors,
        target_fragments,
    )
    operations = _deduplicate_target_copies(
        transfer.operations, source_tensors, target_tensors
    )
    return TransferPlan(
        model_id=transfer.model_id,
        revision=transfer.revision,
        operations=operations,
        source_executors=_build_executor_plans(source_manifests, operations, "source"),
        target_executors=_build_executor_plans(target_manifests, operations, "target"),
    )


def plan_runtime_transfer_to_local_target(
    source_manifests: Sequence[RuntimeManifest],
    target_manifest: RuntimeManifest,
) -> TransferPlan:
    """Plan the fragments needed by one independently starting target worker.

    SGLang initializes every target worker in parallel and provides the global
    startup barrier. A local plan therefore selects the tensors owned by one
    PP/EP worker and the TP ranges resident on that worker, while retaining the
    complete source executor snapshot for fencing and routing.
    """

    source_tensors, source_fragments = _collect_manifests(source_manifests, "source")
    target_tensors, target_fragments = _collect_manifests((target_manifest,), "target")
    source = source_manifests[0]
    if source.model_id != target_manifest.model_id:
        raise ValueError("source and target model_id differ")
    if source.revision != target_manifest.revision:
        raise ValueError("source and target revision differ")

    transfer = _plan_transfer(
        source.model_id,
        source.revision,
        source_tensors,
        source_fragments,
        target_tensors,
        target_fragments,
        local_target=True,
    )
    operations = _deduplicate_target_copies(
        transfer.operations, source_tensors, target_tensors
    )
    target_executors = _build_executor_plans((target_manifest,), operations, "target")
    if len(target_executors) != 1:
        raise ValueError("local target manifest must describe exactly one executor")
    return TransferPlan(
        model_id=transfer.model_id,
        revision=transfer.revision,
        operations=operations,
        source_executors=_build_executor_plans(source_manifests, operations, "source"),
        target_executors=target_executors,
    )


def plan_stored_transfer(
    source_manifest: WeightManifest,
    target_manifests: Sequence[RuntimeManifest],
) -> TransferPlan:
    target_tensors, target_fragments = _collect_manifests(target_manifests, "target")
    target = target_manifests[0]
    if source_manifest.model_id != target.model_id:
        raise ValueError("source and target model_id differ")
    if source_manifest.revision != target.revision:
        raise ValueError("source and target revision differ")
    source_tensors = {tensor.tensor_id: tensor for tensor in source_manifest.tensors}
    transfer = _plan_transfer(
        source_manifest.model_id,
        source_manifest.revision,
        source_tensors,
        source_manifest.fragments,
        target_tensors,
        target_fragments,
    )
    operations = _deduplicate_target_copies(
        transfer.operations, source_tensors, target_tensors
    )
    return TransferPlan(
        model_id=transfer.model_id,
        revision=transfer.revision,
        operations=operations,
        target_executors=_build_executor_plans(target_manifests, operations, "target"),
    )
