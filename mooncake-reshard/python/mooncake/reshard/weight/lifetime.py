"""Framework allocation-lifetime contracts shared by weight executors and Store."""

from ._lifetime import (
    AcquiredWeightBinding,
    WeightAllocationGuardProvider,
    WeightAllocationGuardProviders,
    acquire_weight_binding_token,
    acquire_weight_lifetime_tokens,
    weight_allocation_fence,
)

__all__ = [
    "AcquiredWeightBinding",
    "WeightAllocationGuardProvider",
    "WeightAllocationGuardProviders",
    "acquire_weight_binding_token",
    "acquire_weight_lifetime_tokens",
    "weight_allocation_fence",
]
