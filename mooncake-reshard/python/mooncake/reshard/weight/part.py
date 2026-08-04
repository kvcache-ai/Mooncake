"""Per-participant logical weight placement exported by a framework runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .types import (
    ParallelRank,
    PlacementFragment,
    TensorParallelAxis,
    TensorDescriptor,
    _canonical_tensor_descriptor,
    _read_aliases,
    _read_field,
    _read_optional_field,
    _read_weight_resource_id,
    _require_manifest_items,
    _require_nonempty_string,
    _require_sequence,
    _require_u64,
)
from .validation import _validate_fragments


@dataclass(frozen=True)
class WeightPlacementPart:
    """One participant's address-free contribution to a global placement."""

    resource_id: str
    revision: str
    weight_generation: int
    placement_set_id: str
    topology_id: str
    participant_id: str
    rank: ParallelRank
    tensors: tuple[TensorDescriptor, ...]
    fragments: tuple[PlacementFragment, ...]

    def __post_init__(self) -> None:
        for name in (
            "resource_id",
            "revision",
            "placement_set_id",
            "topology_id",
            "participant_id",
        ):
            _require_nonempty_string(getattr(self, name), name)
        _require_u64(self.weight_generation, "weight_generation")
        if not isinstance(self.rank, ParallelRank):
            raise ValueError("placement part rank must be a ParallelRank")

        tensors = _require_manifest_items(
            self.tensors,
            "WeightPlacementPart tensors",
            TensorDescriptor,
        )
        tensors = tuple(_canonical_tensor_descriptor(tensor) for tensor in tensors)
        fragments = _require_manifest_items(
            self.fragments,
            "WeightPlacementPart fragments",
            PlacementFragment,
        )
        if any(fragment.rank != self.rank for fragment in fragments):
            raise ValueError("placement part fragment rank differs from part rank")
        referenced_tensor_ids = {fragment.tensor_id for fragment in fragments}
        unreferenced_tensor_ids = sorted(
            {tensor.tensor_id for tensor in tensors} - referenced_tensor_ids
        )
        if unreferenced_tensor_ids:
            raise ValueError(
                "placement part contains an unreferenced tensor: "
                f"{unreferenced_tensor_ids[0]}"
            )
        object.__setattr__(
            self,
            "tensors",
            tuple(sorted(tensors, key=lambda item: item.tensor_id)),
        )
        object.__setattr__(
            self,
            "fragments",
            tuple(sorted(fragments, key=lambda item: item.placement_fragment_id)),
        )
        _validate_fragments(self.tensors, self.fragments)

    @property
    def model_id(self) -> str:
        """Return the weight-specific name for the common resource ID."""

        return self.resource_id

    @classmethod
    def from_runtime_inventory(cls, inventory: Any) -> WeightPlacementPart:
        """Import one framework participant's address-free tensor inventory."""

        tensors: dict[str, TensorDescriptor] = {}
        fragments = []
        for record in _require_sequence(
            _read_field(inventory, "tensors"), "placement part inventory tensors"
        ):
            shard_dims = _read_optional_field(record, "shard_dims")
            descriptor = _canonical_tensor_descriptor(
                TensorDescriptor(
                    tensor_id=_read_field(record, "tensor_id"),
                    global_shape=_read_field(record, "global_shape"),
                    dtype=_read_field(record, "dtype"),
                    itemsize=_read_field(record, "itemsize"),
                    partition_dim=_read_field(record, "partition_dim"),
                    layer_id=_read_optional_field(record, "layer_id"),
                    expert_id=_read_optional_field(record, "expert_id"),
                    layout_fingerprint=_read_field(record, "layout_fingerprint"),
                    shard_dims=shard_dims,
                    parallel_axes=_tensor_parallel_axes(
                        _read_field(record, "parallel_axes")
                    ),
                )
            )
            previous = tensors.setdefault(descriptor.tensor_id, descriptor)
            if previous != descriptor:
                raise ValueError(
                    f"placement descriptor mismatch: {descriptor.tensor_id}"
                )
            rank = _parallel_rank(_read_field(record, "rank"))
            fragments.append(
                PlacementFragment(
                    placement_fragment_id=_read_optional_field(
                        record,
                        "placement_fragment_id",
                    ),
                    tensor_id=descriptor.tensor_id,
                    global_offset=_read_field(record, "global_offset"),
                    local_shape=_read_field(record, "local_shape"),
                    nbytes=_read_field(record, "nbytes"),
                    rank=rank,
                    aliases=_read_aliases(record),
                )
            )

        declared_rank = _parallel_rank(_read_field(inventory, "rank"))
        return cls(
            resource_id=_read_weight_resource_id(inventory),
            revision=_read_field(inventory, "revision"),
            weight_generation=_read_field(inventory, "weight_generation"),
            placement_set_id=_read_field(inventory, "placement_set_id"),
            topology_id=_read_field(inventory, "topology_id"),
            participant_id=_read_field(inventory, "participant_id"),
            rank=declared_rank,
            tensors=tuple(sorted(tensors.values(), key=lambda item: item.tensor_id)),
            fragments=tuple(fragments),
        )


def _parallel_rank(value: Any) -> ParallelRank:
    return ParallelRank(
        dp=_read_field(value, "dp"),
        tp=_read_field(value, "tp"),
        pp=_read_field(value, "pp"),
        ep=_read_field(value, "ep"),
    )


def _tensor_parallel_axes(value: Any) -> tuple[TensorParallelAxis, ...]:
    return tuple(
        TensorParallelAxis(
            kind=_read_field(axis, "kind"),
            split_dim=_read_optional_field(axis, "split_dim"),
        )
        for axis in _require_sequence(value, "tensor parallel_axes")
    )


__all__ = ["WeightPlacementPart"]
