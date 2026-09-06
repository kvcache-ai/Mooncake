from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Optional, Protocol, Sequence, Union

from ..contracts import LeaseId, RuntimeBindingFragment, RuntimeFragmentId
from .completion import (
    _PendingCompletionWaitInterrupted,
    TransferCompletionUnknownError,
    TransferEngineError,
)
from .lifetime import AllocationLifetimeToken, AllocationTokenSet


class _RegistrationEngine(Protocol):
    def register_memory(self, address: int, nbytes: int) -> int: ...

    def unregister_memory(self, address: int) -> int: ...


class _PendingResourceOwner(Protocol):
    def _retain_pending_resources(
        self,
        pending_transfer_id: str,
        *,
        registrations: Sequence[int],
        resources: Sequence[object],
        allocation_tokens: Sequence[AllocationLifetimeToken] = (),
    ) -> None: ...


@dataclass(frozen=True)
class BufferRegistrationLease:
    """Lease for one registered allocation plus its bound tensor view."""

    fragment_id: RuntimeFragmentId
    worker_id: str
    device: str
    address: int
    nbytes: int
    itemsize: int
    local_shape: tuple[int, ...]
    strides_bytes: tuple[int, ...]
    storage_address: int
    storage_nbytes: int
    storage_offset_bytes: int
    lease_generation: int
    runtime_lease_id: Optional[LeaseId] = None

    def __post_init__(self) -> None:
        if not self.fragment_id or not self.worker_id or not self.device:
            raise ValueError("registration lease identifiers must not be empty")
        for name in (
            "address",
            "nbytes",
            "itemsize",
            "storage_address",
            "storage_nbytes",
            "storage_offset_bytes",
            "lease_generation",
        ):
            value = getattr(self, name)
            if type(value) is not int:
                raise ValueError(f"registration lease {name} must be an integer")
        if (
            self.address <= 0
            or self.nbytes <= 0
            or self.itemsize <= 0
            or self.storage_address <= 0
            or self.storage_nbytes <= 0
            or self.storage_offset_bytes < 0
            or self.lease_generation < 0
        ):
            raise ValueError("registration lease values are invalid")
        local_shape = tuple(self.local_shape)
        strides_bytes = tuple(self.strides_bytes)
        if (
            not local_shape
            or len(local_shape) != len(strides_bytes)
            or any(type(value) is not int or value <= 0 for value in local_shape)
            or any(type(value) is not int or value <= 0 for value in strides_bytes)
        ):
            raise ValueError("registration lease tensor geometry is invalid")
        object.__setattr__(self, "local_shape", local_shape)
        object.__setattr__(self, "strides_bytes", strides_bytes)
        if self.address != self.storage_address + self.storage_offset_bytes:
            raise ValueError("registration lease view address is invalid")
        if self.storage_offset_bytes > self.storage_nbytes - self.nbytes:
            raise ValueError("registration lease view exceeds allocation")
        if self.runtime_lease_id is not None and (
            type(self.runtime_lease_id) is not str or not self.runtime_lease_id
        ):
            raise ValueError("registration runtime_lease_id must be a non-empty string")

    @classmethod
    def from_fragment(
        cls,
        fragment: RuntimeBindingFragment,
        *,
        lease_generation: int,
        runtime_lease_id: LeaseId,
    ) -> BufferRegistrationLease:
        return cls(
            fragment_id=fragment.fragment_id,
            worker_id=fragment.worker_id,
            device=fragment.device,
            address=fragment.address,
            nbytes=fragment.nbytes,
            itemsize=fragment.itemsize,
            local_shape=fragment.local_shape,
            strides_bytes=fragment.strides_bytes,
            storage_address=fragment.storage_address,
            storage_nbytes=fragment.storage_nbytes,
            storage_offset_bytes=fragment.storage_offset_bytes,
            lease_generation=lease_generation,
            runtime_lease_id=runtime_lease_id,
        )


def same_runtime_snapshot(
    current: RuntimeBindingFragment,
    planned: RuntimeBindingFragment,
) -> bool:
    return (
        current.placement_fragment_id == planned.placement_fragment_id
        and current.fragment_id == planned.fragment_id
        and current.address == planned.address
        and current.nbytes == planned.nbytes
        and current.worker_id == planned.worker_id
        and current.endpoint == planned.endpoint
        and current.device == planned.device
        and current.itemsize == planned.itemsize
        and current.local_shape == planned.local_shape
        and current.strides_bytes == planned.strides_bytes
        and current.storage_address == planned.storage_address
        and current.storage_nbytes == planned.storage_nbytes
        and current.storage_offset_bytes == planned.storage_offset_bytes
    )


def registration_map(
    registrations: Optional[Sequence[BufferRegistrationLease]],
    label: str,
) -> dict[RuntimeFragmentId, BufferRegistrationLease]:
    if registrations is None:
        raise TransferEngineError(f"{label} registration leases are required")
    result: dict[RuntimeFragmentId, BufferRegistrationLease] = {}
    for registration in registrations:
        if registration.fragment_id in result:
            raise TransferEngineError(
                f"duplicate {label} registration lease: {registration.fragment_id}"
            )
        result[registration.fragment_id] = registration
    return result


def validate_registration(
    fragment: RuntimeBindingFragment,
    registrations: dict[RuntimeFragmentId, BufferRegistrationLease],
    label: str,
    *,
    lease_generation: int,
    runtime_lease_id: LeaseId,
) -> None:
    registration = registrations.get(fragment.fragment_id)
    if registration is None or (
        registration.worker_id != fragment.worker_id
        or registration.device != fragment.device
        or registration.address != fragment.address
        or registration.nbytes != fragment.nbytes
        or registration.itemsize != fragment.itemsize
        or registration.local_shape != fragment.local_shape
        or registration.strides_bytes != fragment.strides_bytes
        or registration.storage_address != fragment.storage_address
        or registration.storage_nbytes != fragment.storage_nbytes
        or registration.storage_offset_bytes != fragment.storage_offset_bytes
        or registration.lease_generation != lease_generation
        or registration.runtime_lease_id != runtime_lease_id
    ):
        raise TransferEngineError(
            f"{label} registration lease mismatch: {fragment.fragment_id}"
        )


def _handoff_pending_resources(
    pending_owner: _PendingResourceOwner,
    error: Union[TransferCompletionUnknownError, _PendingCompletionWaitInterrupted],
    *,
    registrations: Sequence[int],
    resources: Sequence[object],
    lifetime_tokens: Optional[AllocationTokenSet] = None,
) -> BaseException:
    allocation_tokens = lifetime_tokens.tokens if lifetime_tokens is not None else ()
    pending_owner._retain_pending_resources(
        error.pending_transfer_id,
        registrations=registrations,
        resources=resources,
        allocation_tokens=allocation_tokens,
    )
    if lifetime_tokens is not None:
        lifetime_tokens.handoff_to_pending()
    if isinstance(error, _PendingCompletionWaitInterrupted):
        return error.interruption
    return error


@contextmanager
def registered_sources(
    engine: _RegistrationEngine,
    pending_owner: _PendingResourceOwner,
    fragments: Sequence[RuntimeBindingFragment],
    *,
    pre_registered: bool,
    registrations: Optional[Sequence[BufferRegistrationLease]],
    lease_generation: int,
    runtime_lease_id: LeaseId,
    resources: Sequence[object],
    lifetime_tokens: Optional[AllocationTokenSet] = None,
) -> Generator[None, None, None]:
    if pre_registered:
        registration_by_id = registration_map(registrations, "source")
        for fragment in fragments:
            validate_registration(
                fragment,
                registration_by_id,
                "source",
                lease_generation=lease_generation,
                runtime_lease_id=runtime_lease_id,
            )
        try:
            yield
        except (
            TransferCompletionUnknownError,
            _PendingCompletionWaitInterrupted,
        ) as error:
            raise _handoff_pending_resources(
                pending_owner,
                error,
                registrations=(),
                resources=resources,
                lifetime_tokens=lifetime_tokens,
            )
        return
    if registrations is not None:
        raise TransferEngineError(
            "source registration leases require source_pre_registered=True"
        )

    sizes_by_address = _registered_allocations(fragments, "source")
    owned: list[int] = []
    primary_error: Optional[BaseException] = None
    try:
        for address, nbytes in sizes_by_address.items():
            try:
                result = engine.register_memory(address, nbytes)
            except Exception as error:
                raise TransferEngineError(
                    f"source register_memory failed for {address}: {error}"
                ) from error
            if result != 0:
                raise TransferEngineError(
                    f"source register_memory failed for {address}: {result}"
                )
            owned.append(address)
        yield
    except BaseException as error:
        primary_error = error

    if isinstance(
        primary_error,
        (
            TransferCompletionUnknownError,
            _PendingCompletionWaitInterrupted,
        ),
    ):
        raise _handoff_pending_resources(
            pending_owner,
            primary_error,
            registrations=owned,
            resources=resources,
            lifetime_tokens=lifetime_tokens,
        )

    failures: list[tuple[int, Union[str, int]]] = []
    for address in reversed(owned):
        try:
            result = engine.unregister_memory(address)
        except Exception as error:
            failures.append((address, repr(error)))
            continue
        if result != 0:
            failures.append((address, result))
    if failures:
        detail = f"source unregister_memory failed: {failures}"
        if primary_error is not None:
            raise TransferEngineError(f"{primary_error}; {detail}") from primary_error
        raise TransferEngineError(detail)
    if primary_error is not None:
        raise primary_error


# Preserve the weight TE public name while exposing a resource-neutral name.
MemoryRegistrationLease = BufferRegistrationLease


@contextmanager
def registered_targets(
    engine: _RegistrationEngine,
    pending_owner: _PendingResourceOwner,
    fragments: Sequence[RuntimeBindingFragment],
    *,
    pre_registered: bool,
    resources: Sequence[object],
    lifetime_tokens: Optional[AllocationTokenSet] = None,
) -> Generator[None, None, None]:
    if pre_registered:
        try:
            yield
        except (
            TransferCompletionUnknownError,
            _PendingCompletionWaitInterrupted,
        ) as error:
            raise _handoff_pending_resources(
                pending_owner,
                error,
                registrations=(),
                resources=resources,
                lifetime_tokens=lifetime_tokens,
            )
        return

    sizes_by_address = _registered_allocations(fragments, "target")
    owned: list[int] = []
    primary_error: Optional[BaseException] = None
    try:
        for address, nbytes in sizes_by_address.items():
            try:
                result = engine.register_memory(address, nbytes)
            except Exception as error:
                raise TransferEngineError(
                    f"target register_memory failed for {address}: {error}"
                ) from error
            if result != 0:
                raise TransferEngineError(
                    f"target register_memory failed for {address}: {result}"
                )
            owned.append(address)
        yield
    except BaseException as error:
        primary_error = error

    if isinstance(
        primary_error,
        (
            TransferCompletionUnknownError,
            _PendingCompletionWaitInterrupted,
        ),
    ):
        raise _handoff_pending_resources(
            pending_owner,
            primary_error,
            registrations=owned,
            resources=resources,
            lifetime_tokens=lifetime_tokens,
        )

    failures: list[tuple[int, Union[str, int]]] = []
    for address in reversed(owned):
        try:
            result = engine.unregister_memory(address)
        except Exception as error:
            failures.append((address, repr(error)))
            continue
        if result != 0:
            failures.append((address, result))
    if failures:
        detail = f"target unregister_memory failed: {failures}"
        if primary_error is not None:
            raise TransferEngineError(f"{primary_error}; {detail}") from primary_error
        raise TransferEngineError(detail)
    if primary_error is not None:
        raise primary_error


def _registered_allocations(
    fragments: Sequence[RuntimeBindingFragment],
    label: str,
) -> dict[int, int]:
    allocations: dict[int, int] = {}
    for fragment in fragments:
        existing = allocations.get(fragment.storage_address)
        if existing is not None and existing != fragment.storage_nbytes:
            raise TransferEngineError(
                f"{label} allocation capacity mismatch: {fragment.storage_address}"
            )
        allocations[fragment.storage_address] = fragment.storage_nbytes
    return allocations
