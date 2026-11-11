"""Store memory validation stays independent of runtime-to-runtime identity."""

from dataclasses import fields, replace

import pytest
from _store_test_utils import memory_binding
from mooncake.reshard.kv_cache import (
    KVCacheResolvedRuntimeBinding,
    KVCacheStoreFormat,
    KVCacheStoreRuntimeBinding,
    KVCacheTransferLimits,
    validate_resolved_runtime_binding,
)
from mooncake.reshard.kv_cache._store.binding import validate_store_binding
from test_kv_cache_reshard import _placement, _snapshot


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


def validate(placement, binding, limits=KVCacheTransferLimits()):
    validate_store_binding(
        placement,
        binding,
        "read",
        0,
        placement.descriptor.page_size,
        limits,
    )


def as_resolved(placement, binding):
    snapshot = _snapshot(
        resource_id=placement.resource_id,
        token_count=placement.descriptor.page_size,
    )
    values = {field.name: getattr(binding, field.name) for field in fields(binding)}
    return snapshot, KVCacheResolvedRuntimeBinding(
        **values, snapshot_id=snapshot.snapshot_id, snapshot_digest=snapshot.digest
    )


@pytest.fixture(params=("store", "r2r"))
def validator(request):
    if request.param == "store":
        return validate

    def validate_r2r(placement, binding, limits=KVCacheTransferLimits()):
        snapshot, resolved = as_resolved(placement, binding)
        validate_resolved_runtime_binding(
            placement, snapshot, resolved, operation_id="read", limits=limits
        )

    return validate_r2r


def test_store_and_r2r_bindings_keep_separate_identity_contracts(bound_page):
    placement, binding, _ = bound_page
    assert isinstance(binding, KVCacheStoreRuntimeBinding)
    assert not hasattr(binding, "snapshot_id")
    validate(placement, binding)
    values = {field.name: getattr(binding, field.name) for field in fields(binding)}
    with pytest.raises(ValueError):
        KVCacheResolvedRuntimeBinding(**values, snapshot_id=None, snapshot_digest=None)
    snapshot, r2r = as_resolved(placement, binding)
    validate_resolved_runtime_binding(placement, snapshot, r2r, operation_id="read")
    with pytest.raises(TypeError, match="KVCacheStoreRuntimeBinding"):
        validate(placement, r2r)
    with pytest.raises(TypeError, match="KVCacheResolvedRuntimeBinding"):
        validate_resolved_runtime_binding(
            placement, snapshot, binding, operation_id="read"
        )
    for field, value in (("snapshot_id", "other"), ("snapshot_digest", "a" * 64)):
        with pytest.raises(ValueError, match=f"{field} differs"):
            validate_resolved_runtime_binding(
                placement, snapshot, replace(r2r, **{field: value}), operation_id="read"
            )


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
def test_bindings_reject_invalid_memory_before_io(
    bound_page, validator, mutation, error
):
    placement, binding, _ = bound_page
    with pytest.raises(ValueError, match=error):
        validator(placement, mutation(binding))


@pytest.mark.parametrize(
    "limits,error",
    [
        (KVCacheTransferLimits(max_ranges=1), "range count limit"),
        (KVCacheTransferLimits(max_operations=1), "physical expansion limit"),
        (KVCacheTransferLimits(max_validation_work=1), "validation work limit"),
    ],
)
def test_bindings_enforce_geometry_limits(bound_page, validator, limits, error):
    placement, binding, _ = bound_page
    with pytest.raises(ValueError, match=error):
        validator(placement, binding, limits)
