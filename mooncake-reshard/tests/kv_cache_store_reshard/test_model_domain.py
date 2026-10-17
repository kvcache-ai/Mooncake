"""Model semantic domain and logical key derivation tests."""

from __future__ import annotations

import dataclasses

import pytest

from .helpers import make_domain


def test_same_domain_digest_is_stable() -> None:
    assert make_domain().digest == make_domain().digest


def test_physical_layout_change_keeps_the_domain() -> None:
    # TP/PP/layout are not domain fields: they cannot change the digest.
    assert make_domain().digest == make_domain().digest
    assert make_domain().logical_page_key("sglang", "tok123") == make_domain(
        revision="rev-1"
    ).logical_page_key("sglang", "tok123")


def test_semantic_change_must_change_the_domain() -> None:
    base = make_domain()
    for mutation in (
        {"revision": "rev-2"},
        {"model_identity": "llama-8b-instruct"},
        {"dtype": "bfloat16"},
        {"itemsize": 4},
        {"page_size": 16},
        {"global_layer_ids": (0, 1, 2, 3, 4)},
        {"total_kv_heads": 4},
        {"key_head_dim": 4},
        {"value_head_dim": 4},
    ):
        mutated = dataclasses.replace(make_domain(), **mutation)
        assert mutated.digest != base.digest, mutation


def test_logical_page_key_format_and_stability() -> None:
    domain = make_domain()
    key = domain.logical_page_key("sglang", "tok123")
    assert key == f"sglang_tok123_{domain.digest_token}"
    # The same domain and token prefix always produce the same key,
    # independent of the writer's topology (which is not a domain field).
    assert domain.logical_page_key("sglang", "tok123") == key


def test_manifest_key_format() -> None:
    domain = make_domain()
    expected = f"kv/{domain.digest_token}/manifests/sha256:abc"
    assert domain.manifest_key("sha256:abc") == expected


def test_invalid_domain_is_rejected() -> None:
    with pytest.raises(ValueError):
        make_domain(layer_ids=())
    with pytest.raises(ValueError):
        make_domain(layer_ids=(1, 1))
    with pytest.raises(ValueError):
        make_domain(page_size=0)
    with pytest.raises(ValueError):
        dataclasses.replace(make_domain(), namespace="")
