"""Strict wire contracts; runtime writes are always reconstructed locally."""

from __future__ import annotations

import json
from dataclasses import asdict, fields
from typing import Any, TypeVar, cast

from .completion import KVCacheTargetReceipt, KVCacheWriterReceipt
from .plan_serde import kv_cache_logical_plan_from_json, kv_cache_logical_plan_to_json
from .resolved import (
    DEFAULT_TRANSFER_LIMITS,
    KVCacheRegisteredRegion,
    KVCacheResolvedRange,
    KVCacheResolvedRuntimeBinding,
    KVCacheTransferLimits,
)
from .serde import _load_json_object, _require_exact_fields, _sequence, _string
from .transfer import KVCacheRuntimeTransferPlan
from .types import KVCacheComponent

_T = TypeVar(
    "_T",
    KVCacheRegisteredRegion,
    KVCacheResolvedRange,
    KVCacheResolvedRuntimeBinding,
    KVCacheTransferLimits,
    KVCacheWriterReceipt,
    KVCacheTargetReceipt,
)


def _construct(cls: type[_T], value: object) -> _T:
    data = dict(
        _require_exact_fields(value, {f.name for f in fields(cls)}, cls.__name__)
    )
    if cls is KVCacheResolvedRange:
        data["component"] = KVCacheComponent(_string(data["component"], "component"))
    # Field names are allowlisted above; dataclass constructors validate exact
    # scalar types and nested values. No dynamic class name comes from the wire.
    return cls(**cast(Any, data))


def _dump(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def kv_cache_resolved_binding_to_json(binding: KVCacheResolvedRuntimeBinding) -> str:
    if not isinstance(binding, KVCacheResolvedRuntimeBinding):
        raise TypeError("binding must be a KVCacheResolvedRuntimeBinding")
    return _dump(
        {
            "schema": "kv-cache-resolved-binding",
            "binding": asdict(binding),
            "digest": binding.digest,
        }
    )


def kv_cache_resolved_binding_from_json(value: str) -> KVCacheResolvedRuntimeBinding:
    payload = _require_exact_fields(
        _load_json_object(value, "resolved binding"),
        {"schema", "binding", "digest"},
        "resolved binding",
    )
    if payload["schema"] != "kv-cache-resolved-binding":
        raise ValueError("resolved binding schema differs")
    data = dict(
        _require_exact_fields(
            payload["binding"],
            {f.name for f in fields(KVCacheResolvedRuntimeBinding)},
            "resolved binding fields",
        )
    )
    regions = tuple(
        _construct(KVCacheRegisteredRegion, r)
        for r in _sequence(data.pop("regions"), "regions")
    )
    ranges = tuple(
        _construct(KVCacheResolvedRange, r)
        for r in _sequence(data.pop("ranges"), "ranges")
    )
    result = KVCacheResolvedRuntimeBinding(
        **cast(Any, data), regions=regions, ranges=ranges
    )
    if result.digest != _string(payload["digest"], "digest"):
        raise ValueError("resolved binding digest differs")
    return result


def kv_cache_runtime_transfer_to_json(plan: KVCacheRuntimeTransferPlan) -> str:
    if not isinstance(plan, KVCacheRuntimeTransferPlan):
        raise TypeError("plan must be a KVCacheRuntimeTransferPlan")
    return _dump(
        {
            "schema": "kv-cache-runtime-transfer",
            "operation_id": plan.operation_id,
            "logical_plans": [
                kv_cache_logical_plan_to_json(p) for p in plan.logical_plans
            ],
            "source_bindings": [
                kv_cache_resolved_binding_to_json(b) for b in plan.source_bindings
            ],
            "target_bindings": [
                kv_cache_resolved_binding_to_json(b) for b in plan.target_bindings
            ],
            "limits": asdict(plan.limits),
            "digest": plan.digest,
        }
    )


def kv_cache_runtime_transfer_from_json(
    value: str,
    *,
    limits: KVCacheTransferLimits = DEFAULT_TRANSFER_LIMITS,
) -> KVCacheRuntimeTransferPlan:
    payload = _require_exact_fields(
        _load_json_object(value, "runtime transfer"),
        {
            "schema",
            "operation_id",
            "logical_plans",
            "source_bindings",
            "target_bindings",
            "limits",
            "digest",
        },
        "runtime transfer",
    )
    if payload["schema"] != "kv-cache-runtime-transfer":
        raise ValueError("runtime transfer schema differs")
    if not isinstance(limits, KVCacheTransferLimits):
        raise TypeError("limits must be KVCacheTransferLimits")
    requested_limits = _construct(KVCacheTransferLimits, payload["limits"])
    if any(
        getattr(requested_limits, f.name) > getattr(limits, f.name)
        for f in fields(KVCacheTransferLimits)
    ):
        raise ValueError("wire limits exceed receiver policy")
    result = KVCacheRuntimeTransferPlan(
        _string(payload["operation_id"], "operation_id"),
        tuple(
            kv_cache_logical_plan_from_json(_string(p, "logical plan"))
            for p in _sequence(payload["logical_plans"], "logical plans")
        ),
        tuple(
            kv_cache_resolved_binding_from_json(_string(b, "source binding"))
            for b in _sequence(payload["source_bindings"], "source bindings")
        ),
        tuple(
            kv_cache_resolved_binding_from_json(_string(b, "target binding"))
            for b in _sequence(payload["target_bindings"], "target bindings")
        ),
        requested_limits,
    )
    if result.digest != _string(payload["digest"], "digest"):
        raise ValueError("runtime transfer digest differs")
    return result


def kv_cache_writer_receipt_to_json(receipt: KVCacheWriterReceipt) -> str:
    if not isinstance(receipt, KVCacheWriterReceipt):
        raise TypeError("receipt must be a KVCacheWriterReceipt")
    return _dump(asdict(receipt))


def kv_cache_writer_receipt_from_json(value: str) -> KVCacheWriterReceipt:
    return _construct(KVCacheWriterReceipt, _load_json_object(value, "writer receipt"))


def kv_cache_target_receipt_to_json(receipt: KVCacheTargetReceipt) -> str:
    if not isinstance(receipt, KVCacheTargetReceipt):
        raise TypeError("receipt must be a KVCacheTargetReceipt")
    return _dump(asdict(receipt))


def kv_cache_target_receipt_from_json(value: str) -> KVCacheTargetReceipt:
    return _construct(KVCacheTargetReceipt, _load_json_object(value, "target receipt"))
