"""Weight-facing names for resource-neutral buffer registration."""

from ...transfer_engine.registration import (
    BufferRegistrationLease,
    registered_sources,
    registered_targets,
    registration_map,
    same_runtime_snapshot,
    validate_registration,
)

MemoryRegistrationLease = BufferRegistrationLease

__all__ = [
    "BufferRegistrationLease",
    "MemoryRegistrationLease",
    "registered_sources",
    "registered_targets",
    "registration_map",
    "same_runtime_snapshot",
    "validate_registration",
]
