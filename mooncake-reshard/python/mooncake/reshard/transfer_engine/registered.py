"""Retained allocation registrations over the shared terminal-aware executor."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from .contracts import (
    TransferBatch,
    TransferBatchRange,
    TransferBatchReceipt,
    TransferDirection,
)
from .executor import MooncakeTransferEngineExecutor
from .registration import (
    _register_allocations,
    _RegistrationEngine,
    _unregister_owned_allocations,
    registered_targets,
)


class RegisteredTransferEngine:
    """Own registrations until explicit close or terminal cleanup.

    Framework adapters supply allocation owners and enforce their mutation
    fences. This class has no tensor, directory, or publication dependency.
    """

    def __init__(
        self,
        engine: _RegistrationEngine,
        *,
        endpoint: str,
        executor: MooncakeTransferEngineExecutor | None = None,
        completion_drain_timeout_ms: int = 1000,
    ) -> None:
        if not isinstance(endpoint, str) or not endpoint:
            raise ValueError("endpoint must be non-empty")
        self.engine = engine
        self.endpoint = endpoint
        self.executor = executor or MooncakeTransferEngineExecutor(
            engine, completion_drain_timeout_ms=completion_drain_timeout_ms
        )
        self.registered: dict[int, int] = {}
        self.owners: dict[int, object] = {}
        self._owner_allocations: dict[int, dict[int, int]] = {}
        self.poisoned = False
        self._closed = False
        self._cleanup: set[int] = set()

    def _check_active(self) -> None:
        if self._closed or self.poisoned or self.executor.pending_transfer_ids():
            raise RuntimeError("Transport is closed or completion is unresolved")

    def register_buffer(self, ptr: int, size: int, owner: object = None) -> int:
        self._check_active()
        if type(ptr) is not int or type(size) is not int or min(ptr, size) <= 0:
            raise ValueError("Invalid registration allocation")
        TransferBatchRange(ptr, size, ptr, size, (0,), (0,), (1,))
        if ptr in self.registered:
            if self.registered[ptr] != size or self.owners.get(ptr) is not owner:
                raise ValueError("Registered allocation or owner changed")
            return 0
        try:
            _register_allocations(
                self.engine,
                self.executor,
                {ptr: size},
                label="retained",
                resources=(self, owner),
                lifetime_tokens=None,
            )
        except BaseException:
            self.poisoned = bool(self.executor.pending_transfer_ids())
            raise
        self.registered[ptr] = size
        self.owners[ptr] = owner
        self._owner_allocations.setdefault(id(owner), {})[ptr] = size
        return 0

    def unregister_buffer(self, ptr: int) -> int:
        self._check_active()
        if ptr not in self.registered:
            raise ValueError("Allocation is not registered")
        try:
            _unregister_owned_allocations(
                self.engine,
                self.executor,
                (ptr,),
                label="retained",
                primary_error=None,
                body_entered=False,
                resources=(self, self.owners.get(ptr)),
                lifetime_tokens=None,
            )
        except BaseException:
            self._cleanup.add(ptr)
            self.poisoned = True
            raise
        self._forget_registration(ptr)
        return 0

    def _forget_registration(self, ptr: int) -> None:
        self.registered.pop(ptr, None)
        owner = self.owners.pop(ptr, None)
        allocations = self._owner_allocations.get(id(owner), {})
        allocations.pop(ptr, None)
        if not allocations:
            self._owner_allocations.pop(id(owner), None)

    def execute_batch(
        self,
        batch: TransferBatch,
        direction: TransferDirection,
        resources: Sequence[object] = (),
    ) -> TransferBatchReceipt:
        self._check_active()
        # The existing registration frame transfers strong owner references to
        # the engine-wide pending manager before an unknown result escapes.
        try:
            with registered_targets(
                self.engine,
                self.executor,
                (),
                pre_registered=True,
                resources=(self, *resources),
            ):
                return self.executor.execute_batch(batch, direction)
        except BaseException:
            if self.executor.pending_transfer_ids():
                self.poisoned = True
            raise

    def read_into_targets(
        self,
        source_endpoint: str,
        source_address: int,
        source_capacity: int,
        spans: Iterable[tuple[int, int, int, object]],
    ) -> TransferBatchReceipt | None:
        self._check_active()
        physical: list[TransferBatchRange] = []
        resources: list[object] = []
        for source_offset, target_ptr, size, owner in spans:
            # Owner identity is mandatory: equal addresses alone do not pin the
            # same allocation across framework storage replacement.
            allocations = self._owner_allocations.get(id(owner), {})
            matches = [
                (base, capacity)
                for base, capacity in allocations.items()
                if base <= target_ptr < base + capacity
            ]
            if len(matches) != 1 or owner is None:
                raise ValueError("Target must have one retained registration")
            base, capacity = matches[0]
            physical.append(
                TransferBatchRange(
                    source_address,
                    source_capacity,
                    base,
                    capacity,
                    (source_offset,),
                    (target_ptr - base,),
                    (size,),
                )
            )
            resources.append(owner)
        if not physical:
            return None
        batch = TransferBatch.from_ranges(
            endpoint=source_endpoint, ranges=tuple(physical)
        )
        return self.execute_batch(
            batch, TransferDirection.READ, resources=tuple(resources)
        )

    def drain_pending(self, *, timeout_ms: int = 1000) -> tuple[str, ...]:
        for identity in self.executor.pending_transfer_ids():
            self.executor.drain_pending_transfer(identity, timeout_ms=timeout_ms)
        remaining = self.executor.pending_transfer_ids()
        if not remaining:
            for ptr in self._cleanup:
                self._forget_registration(ptr)
            self._cleanup.clear()
            self.poisoned = False
        return remaining

    def close(self) -> None:
        self._check_active()
        for ptr in tuple(self.registered):
            self.unregister_buffer(ptr)
        self._closed = True
