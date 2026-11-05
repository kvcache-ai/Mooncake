"""Semantic cache-key isolation without a storage service or source layout."""

from dataclasses import replace

import pytest
from mooncake.reshard.kv_cache import KVCacheDescriptor, KVCacheModelDomain


@pytest.fixture
def domain():
    descriptor = KVCacheDescriptor((0, 1, 2, 3), "float16", 2, 16, 8, 128, 128)
    return KVCacheModelDomain("cache", "model", "revision", "semantic", descriptor)


def test_existing_domain_encoding_is_preserved(domain):
    assert domain.digest == (
        "a2af93318c854d9a067e61ff5525ebdb24c5b42eba214c32ec9712004b46b853"
    )
    assert domain.logical_page_key("backend", "prefix-hash") == (
        "backend_prefix-hash_m" + domain.digest
    )


@pytest.mark.parametrize(
    "field", ["namespace", "model_id", "model_revision", "semantic_fingerprint"]
)
def test_semantic_changes_isolate_cached_pages(domain, field):
    other = replace(domain, **{field: "other"})
    assert other.digest != domain.digest
    assert other.logical_page_key("b", "h") != domain.logical_page_key("b", "h")


@pytest.mark.parametrize(
    "field,value",
    [
        ("global_layer_ids", (0, 1, 2)),
        ("dtype", "bfloat16"),
        ("itemsize", 4),
        ("page_size", 32),
        ("total_kv_heads", 4),
        ("key_head_dim", 64),
        ("value_head_dim", 64),
    ],
)
def test_incompatible_geometry_changes_identity(domain, field, value):
    other = replace(domain, descriptor=replace(domain.descriptor, **{field: value}))
    assert other.digest != domain.digest


def test_layer_order_is_canonical(domain):
    other = replace(
        domain, descriptor=replace(domain.descriptor, global_layer_ids=(3, 1, 0, 2))
    )
    assert other.digest == domain.digest


def test_prefix_and_backend_identity_are_retained(domain):
    key = domain.logical_page_key("backend-a", "prefix-a")
    assert domain.logical_page_key("backend-b", "prefix-a") != key
    assert domain.logical_page_key("backend-a", "prefix-b") != key


@pytest.mark.parametrize(
    "field", ["namespace", "model_id", "model_revision", "semantic_fingerprint"]
)
@pytest.mark.parametrize("value", ["", None])
def test_missing_semantic_identity_is_rejected(domain, field, value):
    with pytest.raises(ValueError):
        replace(domain, **{field: value})


def test_descriptor_is_required(domain):
    with pytest.raises(ValueError, match="descriptor"):
        replace(domain, descriptor=None)


@pytest.mark.parametrize("prefix,token_hash", [("", "h"), ("b", ""), (None, "h")])
def test_page_key_requires_both_names(domain, prefix, token_hash):
    with pytest.raises(ValueError):
        domain.logical_page_key(prefix, token_hash)
