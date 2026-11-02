"""KV logical coverage, manifest identity and TP/PP reshard planning."""

from dataclasses import replace

import pytest
from _store_test_utils import FORMATS, _layout, _manifest
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheStoreFormat,
    KVCacheStorePlanningLimits,
    KVCacheStoreShard,
    kv_cache_store_layout_from_json,
    kv_cache_store_layout_to_json,
    kv_cache_store_manifest_from_json,
    kv_cache_store_manifest_to_json,
    plan_kv_cache_store_load,
    plan_kv_cache_store_upload,
)
from test_kv_cache_reshard import _placement


@pytest.mark.parametrize("source_tp,target_tp", [(1, 2), (2, 1), (8, 2), (2, 8)])
def test_upload_and_load_cover_selected_dp_and_all_target_heads(source_tp, target_tp):
    source = _placement("writer", ((0, 2), (1, 3)), source_tp, dp_size=2)
    target = _placement("reader", ((0,), (1, 2), (3,)), target_tp, dp_size=2)
    upload = plan_kv_cache_store_upload(
        source, object_format=KVCacheStoreFormat.PLHD, source_dp_rank=1
    )
    assert all(
        source.part(writer).rank.dp == 1 and source.part(writer).replica_ordinal == 0
        for writer in upload.part_writers
    )
    load = plan_kv_cache_store_load(upload.layout, target, target_dp_rank=1)
    # Check the logical plan here; execution tests check actual copied bytes.
    for part in target.parts:
        actual = [
            (r.fragment.global_layer_id, r.fragment.component, head)
            for r in load.ranges
            if r.participant_id == part.participant_id
            for head in range(
                r.fragment.head_start, r.fragment.head_start + r.fragment.head_count
            )
        ]
        expected = (
            [
                (layer, component, head)
                for layer in part.layer_ids
                for component in KVCacheComponent
                for head in range(part.head_start, part.head_start + part.head_count)
            ]
            if part.rank.dp == 1
            else []
        )
        assert sorted(actual) == sorted(expected)


@pytest.mark.parametrize("fmt", FORMATS)
def test_tp1pp3_to_tp2pp2_read_splits_layers_and_heads(fmt):
    layout = _layout(1, (tuple(range(4)), tuple(range(4, 8)), tuple(range(8, 12))), fmt)
    target = _placement("reader", (tuple(range(6)), tuple(range(6, 12))), 2)
    plan = plan_kv_cache_store_load(layout, target)
    local = [r.fragment for r in plan.ranges if r.participant_id == "reader-p0-t0"]
    assert {f.global_layer_id for f in local} == set(range(6))
    assert all((f.head_start, f.head_count) == (0, 2) for f in local)
    assert {f.part_index for f in local} == {0, 1}


def test_shared_domain_excludes_layout_and_format_but_includes_semantics():
    first = _manifest(_layout(1))
    other = _manifest(_layout(2, ((0, 1, 2, 3),)))
    assert first.layout != other.layout
    assert first.manifest_key != other.manifest_key
    assert first.model_domain == other.model_domain
    assert first.digest != other.digest
    for name in ("namespace", "model_id", "model_revision", "semantic_fingerprint"):
        assert replace(first, **{name: "different"}).model_domain != first.model_domain
    assert (
        replace(first, layout=_layout(fmt=FORMATS[1])).model_domain
        == first.model_domain
    )


def test_layout_is_independent_of_worker_names_and_selected_dp():
    a = plan_kv_cache_store_upload(
        _placement("a", ((0, 1),), 2, dp_size=2), object_format=FORMATS[0]
    )
    b = plan_kv_cache_store_upload(
        _placement("b", ((0, 1),), 2, dp_size=2),
        object_format=FORMATS[0],
        source_dp_rank=1,
    )
    assert a.layout == b.layout and a.layout.digest == b.layout.digest
    assert a.part_writers != b.part_writers
    assert replace(a.layout, shards=tuple(reversed(a.layout.shards))) == a.layout


@pytest.mark.parametrize("fmt", FORMATS)
def test_k_and_v_dimensions_produce_independent_part_sizes(fmt):
    layout = _layout(fmt=fmt)
    layout = replace(layout, descriptor=replace(layout.descriptor, value_head_dim=4))
    for index, shard in enumerate(layout.shards):
        key_size = (
            len(shard.layer_ids)
            * layout.token_count
            * shard.head_count
            * layout.descriptor.key_head_dim
            * 2
        )
        assert layout.part_sizes[index] == key_size * 3 // 2
        value_fragments = [
            f
            for f in layout.fragments
            if f.part_index == index and f.component is KVCacheComponent.VALUE
        ]
        assert min(f.part_offset for f in value_fragments) == key_size
    for f in layout.fragments:
        dim = (
            layout.descriptor.key_head_dim if f.component is KVCacheComponent.KEY else 4
        )
        end = (
            f.part_offset
            + (layout.token_count - 1) * f.token_stride_bytes
            + (f.head_count - 1) * f.head_stride_bytes
            + dim * 2
        )
        assert end <= layout.part_sizes[f.part_index]


@pytest.mark.parametrize(
    "shards",
    [
        (),
        (
            KVCacheStoreShard((0, 1, 2, 3), 0, 1, "left"),
            KVCacheStoreShard((0, 1, 2, 3), 2, 2, "right"),
        ),
        (
            KVCacheStoreShard((0, 1, 2, 3), 0, 4, "first"),
            KVCacheStoreShard((0, 1, 2, 3), 0, 4, "duplicate"),
        ),
        (KVCacheStoreShard((0,), 0, 4, "slot-1"),),
        (KVCacheStoreShard((0, 1, 2, 3), 0, 3, "slot-2"),),
        (KVCacheStoreShard((0, 1, 2, 3), 0, 5, "slot-3"),),
        (KVCacheStoreShard((0, 1, 2, 3, 99), 0, 4, "slot-4"),),
        (
            KVCacheStoreShard((0, 1, 2, 3), 0, 4, "slot-5"),
            KVCacheStoreShard((0,), 0, 1, "slot-6"),
        ),
    ],
)
def test_invalid_logical_coverage_is_rejected(shards):
    with pytest.raises(ValueError):
        replace(_layout(), shards=shards)


@pytest.mark.parametrize(
    "limit",
    [
        KVCacheStorePlanningLimits(max_parts=1),
        KVCacheStorePlanningLimits(max_fragments=1),
        KVCacheStorePlanningLimits(max_bytes=1),
    ],
)
def test_planning_limits(limit):
    with pytest.raises(ValueError, match="limit"):
        plan_kv_cache_store_upload(
            _placement("s", ((0, 1),), 2), object_format=FORMATS[0], limits=limit
        )
    with pytest.raises(ValueError, match="limit"):
        plan_kv_cache_store_load(
            _layout(),
            _placement("s", ((0, 1), (2, 3)), 2),
            limits=limit,
        )


def test_load_replica_expansion_obeys_byte_budget():
    layout = _layout()
    runtime = _placement("s", ((0, 1), (2, 3)), 8)
    limits = KVCacheStorePlanningLimits(max_bytes=sum(layout.part_sizes))
    plan_kv_cache_store_upload(runtime, object_format=FORMATS[0], limits=limits)
    with pytest.raises(ValueError, match="byte limit"):
        plan_kv_cache_store_load(layout, runtime, limits=limits)


def test_plan_identity_and_unsupported_conversions():
    layout = _layout()
    runtime = _placement("s", ((0, 1), (2, 3)), 2)
    load = plan_kv_cache_store_load(layout, runtime)
    assert load.digest == plan_kv_cache_store_load(layout, runtime).digest
    with pytest.raises(TypeError):
        plan_kv_cache_store_upload(runtime, layout, object_format=FORMATS[0])
    with pytest.raises(ValueError, match="descriptors differ"):
        plan_kv_cache_store_load(
            layout,
            _placement("s", ((0, 1), (2, 3)), 2, page_size=32),
        )
    with pytest.raises(ValueError, match="DP rank"):
        plan_kv_cache_store_load(layout, runtime, target_dp_rank=2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_store_manifest_round_trips(fmt):
    layout = _layout(fmt=fmt)
    manifest = _manifest(layout)
    assert (
        kv_cache_store_layout_from_json(kv_cache_store_layout_to_json(layout)) == layout
    )
    assert (
        kv_cache_store_manifest_from_json(kv_cache_store_manifest_to_json(manifest))
        == manifest
    )


def test_intersection_expansion_obeys_range_limit():
    first = _placement("first", ((0,),), 2, total_kv_heads=12)
    target = _placement("target", ((0,),), 3, total_kv_heads=12)
    layout = plan_kv_cache_store_upload(first, object_format=FORMATS[0]).layout
    with pytest.raises(ValueError, match="transfer range limit"):
        plan_kv_cache_store_load(
            layout,
            target,
            limits=KVCacheStorePlanningLimits(max_fragments=6),
        )
