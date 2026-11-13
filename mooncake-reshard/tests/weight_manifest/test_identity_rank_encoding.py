from __future__ import annotations

from dataclasses import asdict
from itertools import product
import json

import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    ReplicatedAxis,
    TopologyParticipant,
    weight_placement_to_json,
)
from mooncake.reshard.weight import placement as placement_module

from .helpers import (
    descriptor,
    parallel_topology,
    placement_fragment,
    placement_manifest,
)


@pytest.mark.parametrize("coordinates", tuple(product((0, 1, 7), repeat=4)))
@pytest.mark.parametrize("pipeline_stage_id", (None, 3))
def test_fragment_rank_identity_matches_dataclass_encoding(
    coordinates: tuple[int, int, int, int], pipeline_stage_id: int | None
) -> None:
    fragment = placement_fragment(
        rank=ParallelRank(*coordinates), pipeline_stage_id=pipeline_stage_id
    )

    identity = placement_module._placement_fragment_identity(fragment)
    legacy = dict(identity, rank=asdict(fragment.rank))

    assert json.dumps(identity).encode() == json.dumps(legacy).encode()


@pytest.mark.parametrize("pipeline_stage_id", (None, 3))
def test_public_manifest_identity_and_serialization_remain_byte_identical(
    monkeypatch: pytest.MonkeyPatch, pipeline_stage_id: int | None
) -> None:
    ranks = tuple(ParallelRank(*values) for values in product(range(2), repeat=4))
    topology = parallel_topology(
        participants=tuple(
            TopologyParticipant(f"worker-{index}", rank)
            for index, rank in enumerate(ranks)
        )
    )
    aliases = ("a.weight", "b.weight")
    tensors = tuple(
        descriptor(
            tensor_id=tensor_id,
            shard_dims=(),
            parallel_axes=tuple(
                ReplicatedAxis(kind=kind) for kind in ("dp", "tp", "pp", "ep")
            ),
        )
        for tensor_id in aliases
    )
    fragments = tuple(
        placement_fragment(
            placement_fragment_id=f"{tensor_id}-{index}",
            tensor_id=tensor_id,
            rank=rank,
            aliases=aliases,
            pipeline_stage_id=pipeline_stage_id,
        )
        for index, rank in enumerate(ranks)
        for tensor_id in aliases
    )
    canonical_payloads = []
    canonical_digest = placement_module._canonical_json_digest
    fragment_identity = placement_module._placement_fragment_identity

    def capture_canonical(value):
        canonical_payloads.append(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        )
        return canonical_digest(value)

    def legacy_identity(fragment):
        return dict(fragment_identity(fragment), rank=asdict(fragment.rank))

    monkeypatch.setattr(placement_module, "_canonical_json_digest", capture_canonical)
    current = placement_manifest(
        topology=topology, tensors=tensors, fragments=fragments
    )
    monkeypatch.setattr(
        placement_module, "_placement_fragment_identity", legacy_identity
    )
    legacy = placement_manifest(topology=topology, tensors=tensors, fragments=fragments)

    assert canonical_payloads[0] == canonical_payloads[1]
    assert current.placement_id.encode() == legacy.placement_id.encode()
    assert (
        weight_placement_to_json(current).encode()
        == weight_placement_to_json(legacy).encode()
    )
    assert current.digest.encode() == legacy.digest.encode()
