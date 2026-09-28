"""Weight-specific acquisition of framework allocation lifetime guards."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping, Optional, Protocol, Sequence, runtime_checkable

from ..contracts import ParticipantId, RuntimeFragmentId, RuntimeInstanceId
from ..lifetime import (
    AllocationFence,
    AllocationLifetimeToken,
    AllocationTokenSet,
    TerminalTransferState,
)
from .manifest import RuntimeBindingFragment, WeightRuntimeBindingManifest
from .planner import TransferPlan


@dataclass(frozen=True)
class AcquiredWeightBinding:
    """Fresh runtime binding observed while the framework allocation is pinned."""

    binding: WeightRuntimeBindingManifest
    token: AllocationLifetimeToken


@runtime_checkable
class WeightAllocationGuardProvider(Protocol):
    """Framework adapter that atomically pins one participant's allocations."""

    def acquire(
        self,
        *,
        transfer_id: str,
        expected_binding: WeightRuntimeBindingManifest,
        required_fragment_ids: Sequence[RuntimeFragmentId],
    ) -> AcquiredWeightBinding: ...


GuardProviderKey = tuple[RuntimeInstanceId, ParticipantId]
WeightAllocationGuardProviders = Mapping[
    GuardProviderKey, WeightAllocationGuardProvider
]


def acquire_weight_lifetime_tokens(
    *,
    transfer_id: str,
    plan: TransferPlan,
    bindings: Sequence[WeightRuntimeBindingManifest],
    side: str,
    providers: Optional[WeightAllocationGuardProviders],
) -> tuple[tuple[WeightRuntimeBindingManifest, ...], AllocationTokenSet]:
    """Acquire pins before using a runtime address or TE registration lease."""

    if side not in ("source", "target"):
        raise ValueError(f"invalid lifetime side: {side}")
    if providers is None:
        raise ValueError(f"{side} allocation guard providers are required")
    expected_executors = (
        plan.source_executors if side == "source" else plan.target_executors
    )
    required_by_key: dict[GuardProviderKey, tuple[RuntimeFragmentId, ...]] = {}
    for executor in expected_executors:
        key = (executor.instance_id, executor.participant_id)
        existing = required_by_key.get(key, ())
        required_by_key[key] = tuple(sorted(set((*existing, *executor.fragment_ids))))
    binding_by_key = {
        (binding.instance_id, binding.participant_id): binding for binding in bindings
    }
    if len(binding_by_key) != len(bindings):
        raise ValueError(f"duplicate {side} runtime binding participant")
    if not set(binding_by_key).issubset(required_by_key):
        raise ValueError(f"{side} runtime binding participants differ from plan")

    acquired_bindings: list[WeightRuntimeBindingManifest] = []
    tokens: list[AllocationLifetimeToken] = []
    try:
        for key in sorted(binding_by_key):
            provider = providers.get(key)
            if provider is None or not isinstance(
                provider, WeightAllocationGuardProvider
            ):
                raise ValueError(f"missing {side} allocation guard provider: {key}")
            expected_binding = binding_by_key[key]
            required_fragment_ids = required_by_key[key]
            acquired = provider.acquire(
                transfer_id=transfer_id,
                expected_binding=expected_binding,
                required_fragment_ids=required_fragment_ids,
            )
            if not isinstance(acquired, AcquiredWeightBinding):
                raise ValueError(f"{side} allocation guard returned an invalid binding")
            fresh_binding = acquired.binding
            expected_fence = weight_allocation_fence(
                fresh_binding,
                required_fragment_ids,
                token_id=acquired.token.fence.token_id,
            )
            if acquired.token.fence != expected_fence:
                raise ValueError(f"{side} allocation guard fence differs from binding")
            _validate_acquired_binding(
                expected_binding, fresh_binding, required_fragment_ids
            )
            acquired_bindings.append(fresh_binding)
            tokens.append(acquired.token)
    except BaseException:
        AllocationTokenSet(tuple(tokens)).release_after_terminal(
            TerminalTransferState.ABORTED
        )
        raise
    return tuple(acquired_bindings), AllocationTokenSet(tuple(tokens))


def acquire_weight_binding_token(
    *,
    transfer_id: str,
    expected_binding: WeightRuntimeBindingManifest,
    required_fragment_ids: Sequence[RuntimeFragmentId],
    side: str,
    providers: Optional[WeightAllocationGuardProviders],
) -> tuple[WeightRuntimeBindingManifest, AllocationTokenSet]:
    """Acquire one framework pin for synchronous Store or live-TE I/O."""

    if side not in ("source", "target"):
        raise ValueError(f"invalid lifetime side: {side}")
    if providers is None:
        raise ValueError(f"{side} allocation guard providers are required")
    required = tuple(sorted(set(required_fragment_ids)))
    if not required:
        raise ValueError(f"{side} allocation guard requires fragments")
    key = (expected_binding.instance_id, expected_binding.participant_id)
    provider = providers.get(key)
    if provider is None or not isinstance(provider, WeightAllocationGuardProvider):
        raise ValueError(f"missing {side} allocation guard provider: {key}")
    acquired = provider.acquire(
        transfer_id=transfer_id,
        expected_binding=expected_binding,
        required_fragment_ids=required,
    )
    if not isinstance(acquired, AcquiredWeightBinding):
        raise ValueError(f"{side} allocation guard returned an invalid binding")
    try:
        fresh_binding = acquired.binding
        expected_fence = weight_allocation_fence(
            fresh_binding,
            required,
            token_id=acquired.token.fence.token_id,
        )
        if acquired.token.fence != expected_fence:
            raise ValueError(f"{side} allocation guard fence differs from binding")
        _validate_acquired_binding(expected_binding, fresh_binding, required)
    except BaseException:
        AllocationTokenSet((acquired.token,)).release_after_terminal(
            TerminalTransferState.ABORTED
        )
        raise
    return fresh_binding, AllocationTokenSet((acquired.token,))


def weight_allocation_fence(
    binding: WeightRuntimeBindingManifest,
    required_fragment_ids: Sequence[RuntimeFragmentId],
    *,
    token_id: str,
) -> AllocationFence:
    fragment_ids = tuple(sorted(required_fragment_ids))
    fragments_by_id = {fragment.fragment_id: fragment for fragment in binding.fragments}
    missing = set(fragment_ids) - set(fragments_by_id)
    if missing:
        raise ValueError(
            f"runtime binding is missing guarded fragments: {sorted(missing)}"
        )
    return AllocationFence(
        resource_id=binding.resource_id,
        revision=binding.revision,
        placement_id=binding.placement_id,
        placement_digest=binding.placement_digest,
        instance_id=binding.instance_id,
        participant_id=binding.participant_id,
        runtime_lease_id=binding.lease_id,
        runtime_generation=binding.generation,
        binding_digest=_binding_digest(
            tuple(fragments_by_id[fragment_id] for fragment_id in fragment_ids)
        ),
        fragment_ids=fragment_ids,
        token_id=token_id,
    )


def _validate_acquired_binding(
    expected: WeightRuntimeBindingManifest,
    fresh: WeightRuntimeBindingManifest,
    required_fragment_ids: Sequence[RuntimeFragmentId],
) -> None:
    for name in (
        "resource_id",
        "revision",
        "placement_id",
        "placement_digest",
        "instance_id",
        "participant_id",
        "generation",
        "lease_id",
    ):
        if getattr(expected, name) != getattr(fresh, name):
            raise ValueError(f"acquired runtime binding {name} differs from plan")
    expected_by_id = {fragment.fragment_id: fragment for fragment in expected.fragments}
    fresh_by_id = {fragment.fragment_id: fragment for fragment in fresh.fragments}
    for fragment_id in required_fragment_ids:
        expected_fragment = expected_by_id.get(fragment_id)
        fresh_fragment = fresh_by_id.get(fragment_id)
        if expected_fragment is None or fresh_fragment is None:
            raise ValueError("acquired runtime binding fragments differ from plan")
        if _runtime_fragment_identity(expected_fragment) != _runtime_fragment_identity(
            fresh_fragment
        ):
            raise ValueError("acquired runtime binding address differs from plan")


def _binding_digest(fragments: Sequence[RuntimeBindingFragment]) -> str:
    content = [
        _runtime_fragment_identity(fragment)
        for fragment in sorted(fragments, key=lambda item: item.fragment_id)
    ]
    return hashlib.sha256(
        json.dumps(content, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def _runtime_fragment_identity(fragment: RuntimeBindingFragment) -> dict[str, object]:
    return {
        "placement_fragment_id": fragment.placement_fragment_id,
        "fragment_id": fragment.fragment_id,
        "address": fragment.address,
        "nbytes": fragment.nbytes,
        "worker_id": fragment.worker_id,
        "endpoint": fragment.endpoint,
        "device": fragment.device,
        "itemsize": fragment.itemsize,
        "local_shape": fragment.local_shape,
        "strides_bytes": fragment.strides_bytes,
        "storage_address": fragment.storage_address,
        "storage_nbytes": fragment.storage_nbytes,
        "storage_offset_bytes": fragment.storage_offset_bytes,
    }


__all__ = [
    "AcquiredWeightBinding",
    "WeightAllocationGuardProvider",
    "WeightAllocationGuardProviders",
    "acquire_weight_binding_token",
    "acquire_weight_lifetime_tokens",
    "weight_allocation_fence",
]
