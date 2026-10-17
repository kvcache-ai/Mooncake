"""Source layout manifest validation, digesting and codec tests."""

from __future__ import annotations

import json

import pytest

from mooncake.reshard.kv_cache.store_reshard import (
    KVCacheHostLayout,
    SourceLayoutManifest,
    SourcePartCoverage,
)

from .helpers import (
    head_sharded_manifest,
    layer_sharded_manifest,
    make_domain,
    make_part,
)


def test_layer_sharded_manifest_is_valid() -> None:
    manifest = layer_sharded_manifest(make_domain())
    assert manifest.part_count == 2
    assert manifest.part(0).layer_ids == (0, 1)
    assert manifest.part(1).layer_ids == (2, 3)


def test_head_sharded_manifest_is_valid() -> None:
    manifest = head_sharded_manifest(make_domain())
    # Both parts cover all four layers, with disjoint head intervals.
    for layer in (0, 1, 2, 3):
        covering = manifest.parts_for_layer(layer)
        assert [part.head_start for part in covering] == [0, 1]


def test_layer_gap_is_rejected() -> None:
    domain = make_domain()
    with pytest.raises(ValueError):
        SourceLayoutManifest(
            domain=domain,
            layout=KVCacheHostLayout.PLHD,
            parts=(make_part(0, (0, 1)),),
        )


def test_layer_overlap_is_rejected() -> None:
    domain = make_domain()
    with pytest.raises(ValueError):
        SourceLayoutManifest(
            domain=domain,
            layout=KVCacheHostLayout.PLHD,
            parts=(
                make_part(0, (0, 1, 2)),
                make_part(1, (2, 3)),
            ),
        )


def test_unknown_layer_is_rejected() -> None:
    domain = make_domain()
    with pytest.raises(ValueError):
        SourceLayoutManifest(
            domain=domain,
            layout=KVCacheHostLayout.PLHD,
            parts=(
                make_part(0, (0, 1)),
                make_part(1, (2, 9)),
            ),
        )


def test_head_gap_for_a_layer_is_rejected() -> None:
    domain = make_domain()
    with pytest.raises(ValueError):
        SourceLayoutManifest(
            domain=domain,
            layout=KVCacheHostLayout.PLHD,
            parts=(
                make_part(0, (0, 1), head_start=0, head_count=1),
                make_part(1, (2, 3), head_start=1, head_count=1),
            ),
        )


def test_part_indices_must_be_dense() -> None:
    domain = make_domain()
    with pytest.raises(ValueError):
        SourceLayoutManifest(
            domain=domain,
            layout=KVCacheHostLayout.PLHD,
            parts=(
                make_part(0, (0, 1)),
                make_part(2, (2, 3)),
            ),
        )


def test_head_split_follows_part_boundaries() -> None:
    manifest = head_sharded_manifest(make_domain())
    # One layer, full head axis: one triple per covering part.
    splits = manifest.head_split(1, 0, 2)
    assert [(part.part_index, start, count) for part, start, count in splits] == [
        (0, 0, 1),
        (1, 1, 1),
    ]


def test_head_split_of_a_subinterval() -> None:
    manifest = head_sharded_manifest(make_domain(kv_heads=4))
    splits = manifest.head_split(0, 1, 2)
    assert [(part.part_index, start, count) for part, start, count in splits] == [
        (0, 1, 1),
        (1, 2, 1),
    ]


def test_manifest_round_trip_through_json() -> None:
    manifest = layer_sharded_manifest(make_domain())
    decoded = SourceLayoutManifest.from_json(manifest.to_json())
    assert decoded.digest == manifest.digest
    assert decoded == manifest


def test_tampered_payload_is_rejected_by_digest() -> None:
    manifest = layer_sharded_manifest(make_domain())
    payload = json.loads(manifest.to_json())
    payload["layout"] = KVCacheHostLayout.LPHD.value
    with pytest.raises(ValueError, match="digest does not match"):
        SourceLayoutManifest.from_json(json.dumps(payload))


def test_wrong_schema_is_rejected() -> None:
    with pytest.raises(ValueError, match="schema"):
        SourceLayoutManifest.from_json('{"schema": "something-else", "digest": "x"}')


def test_missing_digest_is_rejected() -> None:
    manifest = layer_sharded_manifest(make_domain())
    payload = json.loads(manifest.to_json())
    del payload["digest"]
    with pytest.raises(ValueError, match="digest"):
        SourceLayoutManifest.from_json(json.dumps(payload))


def test_manifest_digest_is_layout_sensitive() -> None:
    domain = make_domain()
    plhd = SourceLayoutManifest(
        domain=domain,
        layout=KVCacheHostLayout.PLHD,
        parts=layer_sharded_manifest(domain).parts,
    )
    lphd = SourceLayoutManifest(
        domain=domain,
        layout=KVCacheHostLayout.LPHD,
        parts=layer_sharded_manifest(domain).parts,
    )
    assert plhd.digest != lphd.digest


def test_unknown_part_index_lookup() -> None:
    manifest = layer_sharded_manifest(make_domain())
    with pytest.raises(KeyError):
        manifest.part(7)


def test_covering_part_lookup() -> None:
    manifest = layer_sharded_manifest(make_domain())
    assert manifest.covering_part(0, 1).part_index == 0
    assert manifest.covering_part(3, 0).part_index == 1
    with pytest.raises(KeyError):
        manifest.covering_part(0, 9)


def test_part_coverage_queries() -> None:
    part = SourcePartCoverage(
        part_index=0, layer_ids=(3, 1), head_start=2, head_count=2
    )
    assert part.layer_ids == (1, 3)
    assert part.contains_layer(1)
    assert not part.contains_layer(2)
    assert part.contains_head(2)
    assert part.contains_head(3)
    assert not part.contains_head(4)
