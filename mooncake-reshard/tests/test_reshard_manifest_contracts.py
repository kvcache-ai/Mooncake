import json
from dataclasses import fields
from importlib.util import find_spec
from typing import Protocol

import mooncake.reshard.weight.placement as weight_placement
import pytest
from mooncake import reshard
from mooncake.reshard import weight
from mooncake.reshard.contracts import (
    PlacementManifest,
    ResourceKind,
    ResourceManifest,
    RuntimeBindingManifest,
)
from mooncake.reshard.weight import (
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
    weight_placement_from_json,
    weight_placement_to_json,
)
from weight_manifest.helpers import (
    binding_manifest,
    placement_manifest,
)


def test_reshard_public_api_is_resource_neutral():
    assert reshard.__all__ == [
        "ResourceKind",
        "ResourceManifest",
        "PlacementManifest",
        "RuntimeBindingManifest",
    ]


def test_common_manifest_contracts_are_structural_protocols():
    assert issubclass(ResourceManifest, Protocol)
    assert issubclass(PlacementManifest, Protocol)
    assert issubclass(RuntimeBindingManifest, Protocol)
    assert PlacementManifest not in WeightPlacementManifest.__mro__
    assert RuntimeBindingManifest not in WeightRuntimeBindingManifest.__mro__


def test_weight_api_has_no_combined_runtime_manifest():
    assert not hasattr(weight, "WeightRuntimeManifest")


def test_weight_placement_has_one_canonical_contract_without_side_aliases():
    assert not hasattr(weight_placement, "SourcePlacementManifest")
    assert not hasattr(weight_placement, "TargetPlacementManifest")


def test_concrete_weight_manifests_own_and_validate_common_fields():
    assert [field.name for field in fields(WeightPlacementManifest)][:2] == [
        "resource_id",
        "placement_id",
    ]
    assert [field.name for field in fields(WeightRuntimeBindingManifest)][:6] == [
        "resource_id",
        "placement_id",
        "placement_digest",
        "instance_id",
        "generation",
        "lease_id",
    ]


def test_weight_manifests_expose_common_resource_fields():
    placement = placement_manifest()
    manifests = (
        placement,
        binding_manifest(placement=placement),
    )

    for manifest in manifests:
        assert not hasattr(manifest, "model_id")
        assert manifest.resource_id == "model"
        assert manifest.resource_kind is ResourceKind.MODEL_WEIGHT


def test_legacy_model_weight_namespace_is_not_installed():
    assert find_spec("mooncake.model_weight") is None


def test_kv_cache_is_reserved_without_a_manifest_implementation():
    assert ResourceKind.KV_CACHE.value == "kv_cache"
    assert find_spec("mooncake.reshard.kv_cache") is None


def test_weight_placement_json_has_a_strict_resource_kind():
    placement = placement_manifest()
    payload = json.loads(weight_placement_to_json(placement))

    assert payload["resource_kind"] == "model_weight"
    payload["resource_kind"] = "kv_cache"

    with pytest.raises(ValueError, match="resource_kind"):
        weight_placement_from_json(json.dumps(payload))


@pytest.mark.parametrize("resource_kind", [None, "unknown"])
def test_weight_placement_json_rejects_missing_or_unknown_resource_kind(
    resource_kind,
):
    payload = json.loads(weight_placement_to_json(placement_manifest()))
    if resource_kind is None:
        del payload["resource_kind"]
    else:
        payload["resource_kind"] = resource_kind

    with pytest.raises(ValueError):
        weight_placement_from_json(json.dumps(payload))
