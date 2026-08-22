"""Pure KV topology planning followed by late physical preparation."""

from __future__ import annotations

from dataclasses import dataclass, field

from ..contracts import ParticipantId, PlacementId
from .binding import validate_runtime_binding
from .part import KVCachePlacementPart
from .placement import KVCachePlacementManifest
from .runtime import KVCacheRuntimeBindingManifest
from .types import KVCacheComponent, require_integer, require_nonempty_string


@dataclass(frozen=True)
class KVCacheTransferEdge:
    source_participant_id: ParticipantId
    target_participant_id: ParticipantId
    global_layer_id: int
    component: KVCacheComponent
    global_head_start: int
    head_count: int
    source_head_offset: int
    target_head_offset: int
    head_dim: int
    itemsize: int

    def __post_init__(self) -> None:
        for name in ("source_participant_id", "target_participant_id"):
            require_nonempty_string(getattr(self, name), name)
        for name in (
            "global_layer_id",
            "global_head_start",
            "source_head_offset",
            "target_head_offset",
        ):
            require_integer(getattr(self, name), name)
        for name in ("head_count", "head_dim", "itemsize"):
            require_integer(getattr(self, name), name, minimum=1)
        if not isinstance(self.component, KVCacheComponent):
            raise ValueError("component must be a KVCacheComponent")  # noqa: TRY004

    @property
    def inner_bytes(self) -> int:
        return self.head_count * self.head_dim * self.itemsize


@dataclass(frozen=True)
class KVCacheLogicalTransferPlan:
    source_placement: KVCachePlacementManifest = field(repr=False)
    target_placement: KVCachePlacementManifest = field(repr=False)
    target_participant_id: ParticipantId
    edges: tuple[KVCacheTransferEdge, ...]
    expected_writer_ids: tuple[ParticipantId, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.source_placement, KVCachePlacementManifest):
            raise ValueError("source_placement is invalid")  # noqa: TRY004
        if not isinstance(self.target_placement, KVCachePlacementManifest):
            raise ValueError("target_placement is invalid")  # noqa: TRY004
        self.target_placement.part(self.target_participant_id)
        if not self.edges or not all(
            isinstance(edge, KVCacheTransferEdge) for edge in self.edges
        ):
            raise ValueError("logical transfer plan must contain transfer edges")
        if any(
            edge.target_participant_id != self.target_participant_id
            for edge in self.edges
        ):
            raise ValueError("logical plan contains a different target participant")
        edge_writer_ids = tuple(
            sorted({edge.source_participant_id for edge in self.edges})
        )
        expected = tuple(sorted(self.expected_writer_ids or edge_writer_ids))
        if not set(edge_writer_ids).issubset(expected):
            raise ValueError("expected_writer_ids omit an edge writer")
        object.__setattr__(self, "expected_writer_ids", expected)

        source_dp_ranks = {
            self.source_placement.part(edge.source_participant_id).rank.dp
            for edge in self.edges
        }
        if len(source_dp_ranks) != 1:
            raise ValueError(
                "logical transfer plan must select exactly one source DP replica"
            )

    @property
    def source_dp_rank(self) -> int:
        source_dp_ranks = {
            self.source_placement.part(edge.source_participant_id).rank.dp
            for edge in self.edges
        }
        if len(source_dp_ranks) != 1:
            raise ValueError("logical transfer plan source DP replica is ambiguous")
        return next(iter(source_dp_ranks))

    @property
    def target_part(self) -> KVCachePlacementPart:
        return self.target_placement.part(self.target_participant_id)

    @property
    def source_participant_ids(self) -> tuple[ParticipantId, ...]:
        return tuple(sorted({edge.source_participant_id for edge in self.edges}))

    def for_source(self, participant_id: ParticipantId) -> KVCacheLogicalTransferPlan:
        edges = tuple(
            edge for edge in self.edges if edge.source_participant_id == participant_id
        )
        if not edges:
            raise ValueError(
                f"source participant has no transfer edges: {participant_id}"
            )
        return KVCacheLogicalTransferPlan(
            source_placement=self.source_placement,
            target_placement=self.target_placement,
            target_participant_id=self.target_participant_id,
            edges=edges,
            expected_writer_ids=self.expected_writer_ids,
        )


@dataclass(frozen=True)
class KVCachePreparedTransferEdge:
    endpoint: str
    global_layer_id: int
    component: KVCacheComponent
    source_base_address: int
    source_capacity: int
    target_base_address: int
    target_capacity: int
    source_row_stride: int
    target_row_stride: int
    source_head_offset_bytes: int
    target_head_offset_bytes: int
    nbytes: int

    @property
    def is_full_row(self) -> bool:
        return (
            self.source_head_offset_bytes == 0
            and self.target_head_offset_bytes == 0
            and self.nbytes == self.source_row_stride
            and self.nbytes == self.target_row_stride
        )


@dataclass(frozen=True)
class KVCachePreparedTransferPlan:
    source_placement_id: PlacementId
    source_placement_digest: str
    target_placement_id: PlacementId
    target_placement_digest: str
    page_size: int
    edges: tuple[KVCachePreparedTransferEdge, ...]


def _resolve_source_dp_rank(
    source_placement: KVCachePlacementManifest,
    target: KVCachePlacementPart,
    requested_source_dp_rank: int | None,
) -> int:
    available = source_placement.dp_ranks
    if not available:
        raise ValueError("source placement contains no DP replica")
    if requested_source_dp_rank is not None:
        require_integer(requested_source_dp_rank, "source_dp_rank")
        if requested_source_dp_rank not in available:
            raise ValueError(
                f"source DP rank {requested_source_dp_rank} is absent from placement"
            )
        return requested_source_dp_rank
    # Deterministic balanced default for arbitrary source/target DP sizes.
    return available[target.rank.dp % len(available)]


def plan_kv_cache_transfer_to_local_target(
    source_placement: KVCachePlacementManifest,
    target_placement: KVCachePlacementManifest,
    target_participant_id: ParticipantId,
    *,
    source_dp_rank: int | None = None,
) -> KVCacheLogicalTransferPlan:
    """Plan one target participant from arbitrary source/target topologies."""

    if not isinstance(source_placement, KVCachePlacementManifest):
        raise TypeError("source_placement must be a KVCachePlacementManifest")
    if not isinstance(target_placement, KVCachePlacementManifest):
        raise TypeError("target_placement must be a KVCachePlacementManifest")
    target = target_placement.part(target_participant_id)
    checks = {
        "resource_id": source_placement.resource_id,
        "revision": source_placement.revision,
        "descriptor": source_placement.descriptor,
    }
    for name, expected in checks.items():
        if getattr(target_placement, name) != expected:
            raise ValueError(f"source and target {name} differ")

    selected_source_dp_rank = _resolve_source_dp_rank(
        source_placement, target, source_dp_rank
    )
    descriptor = target_placement.descriptor
    edges: list[KVCacheTransferEdge] = []
    for layer_id in target.layer_ids:
        head = target.head_start
        target_end = target.head_start + target.head_count
        while head < target_end:
            candidates = sorted(
                (
                    part
                    for part in source_placement.parts
                    if part.rank.dp == selected_source_dp_rank
                    and layer_id in part.layer_ids
                    and part.head_start <= head < part.head_start + part.head_count
                ),
                key=lambda part: (part.replica_ordinal, part.participant_id),
            )
            if not candidates:
                raise ValueError(
                    f"source placement misses target layer {layer_id} head {head}"
                )
            selected = candidates[target.replica_ordinal % len(candidates)]
            run_end = head + 1
            while run_end < target_end:
                next_candidates = sorted(
                    (
                        part
                        for part in source_placement.parts
                        if part.rank.dp == selected_source_dp_rank
                        and layer_id in part.layer_ids
                        and part.head_start
                        <= run_end
                        < part.head_start + part.head_count
                    ),
                    key=lambda part: (part.replica_ordinal, part.participant_id),
                )
                if not next_candidates:
                    break
                if (
                    next_candidates[target.replica_ordinal % len(next_candidates)]
                    != selected
                ):
                    break
                run_end += 1
            for component, head_dim in (
                (KVCacheComponent.KEY, descriptor.key_head_dim),
                (KVCacheComponent.VALUE, descriptor.value_head_dim),
            ):
                edges.append(
                    KVCacheTransferEdge(
                        source_participant_id=selected.participant_id,
                        target_participant_id=target.participant_id,
                        global_layer_id=layer_id,
                        component=component,
                        global_head_start=head,
                        head_count=run_end - head,
                        source_head_offset=head - selected.head_start,
                        target_head_offset=head - target.head_start,
                        head_dim=head_dim,
                        itemsize=descriptor.itemsize,
                    )
                )
            head = run_end
    return KVCacheLogicalTransferPlan(
        source_placement=source_placement,
        target_placement=target_placement,
        target_participant_id=target.participant_id,
        edges=tuple(edges),
    )


def prepare_kv_cache_transfer(
    logical_plan: KVCacheLogicalTransferPlan,
    source_binding: KVCacheRuntimeBindingManifest,
    target_binding: KVCacheRuntimeBindingManifest,
) -> KVCachePreparedTransferPlan:
    """Validate global placement attestations and compile static edge metadata."""

    if not isinstance(logical_plan, KVCacheLogicalTransferPlan):
        raise TypeError("logical_plan must be a KVCacheLogicalTransferPlan")
    source_ids = logical_plan.source_participant_ids
    if source_ids != (source_binding.participant_id,):
        raise ValueError("prepare requires a plan restricted to one source participant")
    if target_binding.participant_id != logical_plan.target_participant_id:
        raise ValueError("target binding participant differs from logical plan")
    validate_runtime_binding(logical_plan.source_placement, source_binding)
    validate_runtime_binding(logical_plan.target_placement, target_binding)

    source_buffers = {
        (item.global_layer_id, item.component): item.fragment
        for item in source_binding.buffers
    }
    target_buffers = {
        (item.global_layer_id, item.component): item.fragment
        for item in target_binding.buffers
    }
    endpoint = target_binding.buffers[0].fragment.endpoint
    prepared_edges: list[KVCachePreparedTransferEdge] = []
    for edge in logical_plan.edges:
        key = (edge.global_layer_id, edge.component)
        source_buffer = source_buffers[key]
        target_buffer = target_buffers[key]
        if target_buffer.endpoint != endpoint:
            raise ValueError("one KV binding must use a single transfer endpoint")
        prepared_edges.append(
            KVCachePreparedTransferEdge(
                endpoint=target_buffer.endpoint,
                global_layer_id=edge.global_layer_id,
                component=edge.component,
                source_base_address=source_buffer.storage_address,
                source_capacity=source_buffer.storage_nbytes,
                target_base_address=target_buffer.storage_address,
                target_capacity=target_buffer.storage_nbytes,
                source_row_stride=source_buffer.strides_bytes[0],
                target_row_stride=target_buffer.strides_bytes[0],
                source_head_offset_bytes=(
                    source_buffer.storage_offset_bytes
                    + edge.source_head_offset * source_buffer.strides_bytes[1]
                ),
                target_head_offset_bytes=(
                    target_buffer.storage_offset_bytes
                    + edge.target_head_offset * target_buffer.strides_bytes[1]
                ),
                nbytes=edge.inner_bytes,
            )
        )
    return KVCachePreparedTransferPlan(
        source_placement_id=logical_plan.source_placement.placement_id,
        source_placement_digest=logical_plan.source_placement.digest,
        target_placement_id=logical_plan.target_placement.placement_id,
        target_placement_digest=logical_plan.target_placement.digest,
        page_size=logical_plan.source_placement.descriptor.page_size,
        edges=tuple(prepared_edges),
    )


__all__ = [
    "KVCacheLogicalTransferPlan",
    "KVCachePreparedTransferEdge",
    "KVCachePreparedTransferPlan",
    "KVCacheTransferEdge",
    "plan_kv_cache_transfer_to_local_target",
    "prepare_kv_cache_transfer",
]
