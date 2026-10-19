"""Store memory validation stays independent of runtime-to-runtime identity."""

from dataclasses import fields, replace

import pytest
from _store_test_utils import memory_binding
from mooncake.reshard.kv_cache import (
    KVCacheResolvedRuntimeBinding,
    KVCacheStoreFormat,
    KVCacheStoreRuntimeBinding,
    KVCacheTransferLimits,
)
from mooncake.reshard.kv_cache._store.binding import validate_store_binding
from test_kv_cache_reshard import _placement


@pytest.fixture
def bound_page():
    placement = _placement("source", ((0, 1),), 1)
    binding, buffers, _ = memory_binding(
        placement,
        placement.parts[0].participant_id,
        KVCacheStoreFormat.PLHD,
        1,
        "read",
    )
    return placement, binding, buffers


def validate(placement, binding):
    validate_store_binding(
        placement,
        binding,
        "read",
        0,
        placement.descriptor.page_size,
        KVCacheTransferLimits(),
    )


def test_store_and_r2r_bindings_keep_separate_identity_contracts(bound_page):
    placement, binding, _ = bound_page
    assert isinstance(binding, KVCacheStoreRuntimeBinding)
    assert not hasattr(binding, "snapshot_id")
    validate(placement, binding)
    values = {field.name: getattr(binding, field.name) for field in fields(binding)}
    with pytest.raises(ValueError):
        KVCacheResolvedRuntimeBinding(**values, snapshot_id=None, snapshot_digest=None)
    r2r = KVCacheResolvedRuntimeBinding(
        **values, snapshot_id="snapshot", snapshot_digest="a" * 64
    )
    with pytest.raises(TypeError, match="KVCacheStoreRuntimeBinding"):
        validate(placement, r2r)


@pytest.mark.parametrize(
    "mutation,error",
    [
        (lambda b: replace(b, operation_id="other"), "operation_id differs"),
        (lambda b: replace(b, revision="other"), "revision differs"),
        (lambda b: replace(b, ranges=b.ranges[1:]), "coverage"),
        (lambda b: replace(b, ranges=b.ranges + b.ranges[:1]), "overlap"),
        (
            lambda b: replace(
                b, regions=(replace(b.regions[0], nbytes=1), *b.regions[1:])
            ),
            "bounds",
        ),
        (
            lambda b: replace(
                b,
                regions=(
                    replace(b.regions[0], address=b.regions[0].address + 1),
                    *b.regions[1:],
                ),
            ),
            "aligned",
        ),
        (
            lambda b: replace(
                b,
                ranges=(replace(b.ranges[0], global_layer_id=999), *b.ranges[1:]),
            ),
            "unowned layer",
        ),
        (
            lambda b: replace(
                b,
                ranges=(
                    replace(b.ranges[0], token_count=b.ranges[0].token_count + 1),
                    *b.ranges[1:],
                ),
            ),
            "token interval",
        ),
        (
            lambda b: replace(
                b,
                ranges=(replace(b.ranges[0], head_stride_bytes=1), *b.ranges[1:]),
            ),
            "strides overlap",
        ),
    ],
)
def test_store_binding_rejects_invalid_memory_before_io(bound_page, mutation, error):
    placement, binding, _ = bound_page
    with pytest.raises(ValueError, match=error):
        validate(placement, mutation(binding))
