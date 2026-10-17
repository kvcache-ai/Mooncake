"""Read-template cache tests: reuse plans, never addresses."""

from __future__ import annotations

from mooncake.reshard.kv_cache.store_reshard import (
    KVCacheHostLayout,
    ReadTemplate,
    ReadTemplateCache,
    TargetRequirement,
    plan_reshard_read,
)
from mooncake.reshard.kv_cache.types import KVCacheComponent

from .helpers import layer_sharded_manifest, make_domain


def make_template(
    *,
    manifest_digest: str = "sha256:aaa",
    target_binding_id: str = "binding-1",
    requirement_fingerprint: str = "key:0,1:0+2",
):
    domain = make_domain()
    manifest = layer_sharded_manifest(domain)
    plan = plan_reshard_read(
        manifest,
        TargetRequirement(
            component=KVCacheComponent.KEY,
            layer_ids=(0, 1, 2, 3),
            head_start=0,
            head_count=2,
        ),
        KVCacheHostLayout.PLHD,
    )
    return ReadTemplate(
        manifest_digest=manifest_digest,
        target_binding_id=target_binding_id,
        requirement_fingerprint=requirement_fingerprint,
        plan=plan,
    )


def test_put_then_get_round_trip() -> None:
    cache = ReadTemplateCache()
    template = make_template()
    assert cache.put(template) is template
    assert cache.get("sha256:aaa", "binding-1", "key:0,1:0+2") is template


def test_miss_on_unknown_key() -> None:
    cache = ReadTemplateCache()
    assert cache.get("sha256:zzz", "binding-1", "key:0,1:0+2") is None


def test_refresh_does_not_grow_the_cache() -> None:
    cache = ReadTemplateCache()
    template = make_template()
    cache.put(template)
    cache.put(template)
    assert len(cache) == 1


def test_different_requirement_is_a_different_key() -> None:
    cache = ReadTemplateCache()
    template = make_template()
    cache.put(template)
    assert cache.get("sha256:aaa", "binding-1", "key:2,3:0+2") is None


def test_invalidate_binding_removes_only_that_binding() -> None:
    cache = ReadTemplateCache()
    first = make_template(target_binding_id="binding-1")
    second = make_template(target_binding_id="binding-2")
    cache.put(first)
    cache.put(second)
    assert cache.invalidate_binding("binding-1") == 1
    assert cache.get("sha256:aaa", "binding-1", "key:0,1:0+2") is None
    assert cache.get("sha256:aaa", "binding-2", "key:0,1:0+2") is second


def test_bounded_eviction_is_insertion_order() -> None:
    cache = ReadTemplateCache(max_entries=2)
    first = make_template(target_binding_id="binding-1")
    second = make_template(target_binding_id="binding-2")
    third = make_template(target_binding_id="binding-3")
    cache.put(first)
    cache.put(second)
    cache.put(third)
    assert len(cache) == 2
    assert cache.get("sha256:aaa", "binding-1", "key:0,1:0+2") is None
    assert cache.get("sha256:aaa", "binding-2", "key:0,1:0+2") is second
    assert cache.get("sha256:aaa", "binding-3", "key:0,1:0+2") is third


def test_clear_empties_the_cache() -> None:
    cache = ReadTemplateCache()
    cache.put(make_template())
    cache.clear()
    assert len(cache) == 0


def test_invalid_template_is_rejected() -> None:
    cache = ReadTemplateCache()
    import pytest

    with pytest.raises(ValueError):
        cache.put("not a template")
