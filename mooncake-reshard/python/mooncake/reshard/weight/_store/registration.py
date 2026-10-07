from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Lock
from typing import Generator, Iterable, Optional, Sequence, Union
from uuid import uuid4

from ...lifetime import (
    AllocationTokenSet,
    AllocationLifetimeToken,
    TerminalTransferState,
    release_tokens_after_terminal,
)
from ..manifest import RuntimeBindingFragment, WeightRuntimeBindingManifest
from .backend import StoreBackend
from .errors import WeightStoreError


@dataclass
class _PendingStoreRegistration:
    addresses: set[int]
    tokens: tuple[AllocationLifetimeToken, ...]
    terminal_state: TerminalTransferState


@dataclass
class StoreRegistrationLease:
    """Own long-lived Store registrations and their framework allocation pins."""

    _owner: "StoreBufferRegistration" = field(repr=False)
    binding: WeightRuntimeBindingManifest
    fragment_ids: tuple[str, ...]
    addresses: tuple[tuple[int, int], ...]
    _tokens: AllocationTokenSet = field(repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    @property
    def closed(self) -> bool:
        """Whether this lease can no longer authorize Store I/O."""

        return self._closed

    def validate(
        self,
        binding: WeightRuntimeBindingManifest,
        fragments: Sequence[RuntimeBindingFragment],
    ) -> None:
        if self._closed:
            raise WeightStoreError("Store registration lease is closed")
        if binding != self.binding:
            raise WeightStoreError("Store registration lease binding differs")
        if (
            tuple(sorted(fragment.fragment_id for fragment in fragments))
            != self.fragment_ids
        ):
            raise WeightStoreError("Store registration lease fragments differ")
        if _registration_requests(fragments) != dict(self.addresses):
            raise WeightStoreError("Store registration lease address bounds differ")

    def close(
        self,
        terminal_state: TerminalTransferState = TerminalTransferState.COMPLETED,
    ) -> None:
        self._owner._close_lease(self, terminal_state)


class StoreBufferRegistration:
    """Own synchronous Store registrations and quarantine failed cleanup."""

    def __init__(self, store: StoreBackend) -> None:
        self.store = store
        self._lock = Lock()
        self._pending: dict[str, _PendingStoreRegistration] = {}

    def pending_registration_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._pending))

    def drain_pending_registration(self, pending_registration_id: str) -> None:
        with self._lock:
            pending = self._pending.get(pending_registration_id)
            if pending is None:
                raise WeightStoreError(
                    f"pending Store registration does not exist: {pending_registration_id}"
                )
            addresses = tuple(sorted(pending.addresses, reverse=True))
        failures = _unregister_addresses(self.store, addresses)
        if failures:
            with self._lock:
                current = self._pending.get(pending_registration_id)
                if current is not None:
                    current.addresses = {address for address, _ in failures}
            raise WeightStoreError(
                f"pending Store registration cleanup failed: {failures}"
            )
        with self._lock:
            current = self._pending.get(pending_registration_id)
            if current is None:
                raise WeightStoreError(
                    f"pending Store registration does not exist: {pending_registration_id}"
                )
            tokens = current.tokens
            terminal_state = current.terminal_state
            current.tokens = ()
        try:
            release_tokens_after_terminal(tokens, terminal_state)
        except Exception as error:
            with self._lock:
                current = self._pending.get(pending_registration_id)
                if current is not None:
                    current.tokens = tokens
            raise WeightStoreError(
                f"pending Store allocation lifetime release failed: {error}"
            ) from error
        with self._lock:
            self._pending.pop(pending_registration_id, None)

    def acquire_lease(
        self,
        binding: WeightRuntimeBindingManifest,
        fragments: Sequence[RuntimeBindingFragment],
        tokens: AllocationTokenSet,
    ) -> StoreRegistrationLease:
        self._require_submission_allowed()
        requests = _registration_requests(fragments)
        owned: list[int] = []
        try:
            for address, nbytes in requests.items():
                result = self.store.register_buffer(address, nbytes)
                if result != 0:
                    raise WeightStoreError(
                        f"register_buffer failed for {address}: {result}"
                    )
                owned.append(address)
        except BaseException as error:
            failures = _unregister_addresses(self.store, reversed(owned))
            if failures:
                pending_id = self._quarantine(
                    {address for address, _ in failures},
                    tokens,
                    TerminalTransferState.ABORTED,
                )
                raise WeightStoreError(
                    f"{error}; unregister_buffer failed: {failures}; "
                    f"allocation lifetime is quarantined as {pending_id}"
                ) from error
            tokens.release_after_terminal(TerminalTransferState.ABORTED)
            raise
        return StoreRegistrationLease(
            _owner=self,
            binding=binding,
            fragment_ids=tuple(sorted(fragment.fragment_id for fragment in fragments)),
            addresses=tuple(sorted(requests.items())),
            _tokens=tokens,
        )

    @contextmanager
    def registered(
        self,
        fragments: Sequence[RuntimeBindingFragment],
        *,
        pre_registered_lease: Optional[StoreRegistrationLease] = None,
        lifetime_tokens: Optional[AllocationTokenSet] = None,
    ) -> Generator[None, None, None]:
        self._require_submission_allowed()
        if pre_registered_lease is not None:
            pre_registered_lease.validate(pre_registered_lease.binding, fragments)
            yield
            return
        if lifetime_tokens is None:
            raise WeightStoreError(
                "Store registration requires allocation lifetime tokens"
            )

        requests = _registration_requests(fragments)
        owned: list[int] = []
        body_entered = False
        primary_error: Optional[BaseException] = None
        try:
            for address, nbytes in requests.items():
                result = self.store.register_buffer(address, nbytes)
                if result != 0:
                    raise WeightStoreError(
                        f"register_buffer failed for {address}: {result}"
                    )
                owned.append(address)
            body_entered = True
            yield
        except BaseException as error:
            primary_error = error

        failures = _unregister_addresses(self.store, reversed(owned))
        if failures:
            terminal_state = (
                TerminalTransferState.COMPLETED
                if primary_error is None
                else (
                    TerminalTransferState.FAILED_DRAINED
                    if body_entered
                    else TerminalTransferState.ABORTED
                )
            )
            pending_id = self._quarantine(
                {address for address, _ in failures},
                lifetime_tokens,
                terminal_state,
            )
            detail = (
                f"unregister_buffer failed: {failures}; allocation lifetime is "
                f"quarantined as {pending_id}"
            )
            if primary_error is not None:
                raise WeightStoreError(f"{primary_error}; {detail}") from primary_error
            raise WeightStoreError(detail)
        if primary_error is not None:
            raise primary_error

    def _close_lease(
        self,
        lease: StoreRegistrationLease,
        terminal_state: TerminalTransferState,
    ) -> None:
        if lease._closed:
            return
        failures = _unregister_addresses(
            self.store,
            (address for address, _ in reversed(lease.addresses)),
        )
        if failures:
            pending_id = self._quarantine(
                {address for address, _ in failures},
                lease._tokens,
                terminal_state,
            )
            # The token has moved to Store-owned pending cleanup. The lease is
            # no longer a usable registration, even if the caller retries close.
            lease._closed = True
            raise WeightStoreError(
                f"unregister_buffer failed: {failures}; allocation lifetime is "
                f"quarantined as {pending_id}"
            )
        lease._tokens.release_after_terminal(terminal_state)
        lease._closed = True

    def _require_submission_allowed(self) -> None:
        with self._lock:
            if self._pending:
                pending_id = min(self._pending)
                raise WeightStoreError(
                    "Store weight I/O is blocked by pending registration cleanup: "
                    f"{pending_id}"
                )

    def _quarantine(
        self,
        addresses: set[int],
        tokens: AllocationTokenSet,
        terminal_state: TerminalTransferState,
    ) -> str:
        pending_id = uuid4().hex
        raw_tokens = tokens.handoff_to_pending()
        with self._lock:
            self._pending[pending_id] = _PendingStoreRegistration(
                addresses=addresses,
                tokens=raw_tokens,
                terminal_state=terminal_state,
            )
        return pending_id


def _registration_requests(
    fragments: Sequence[RuntimeBindingFragment],
) -> dict[int, int]:
    requests: dict[int, int] = {}
    for fragment in fragments:
        previous = requests.get(fragment.address)
        if previous is not None and previous != fragment.nbytes:
            raise WeightStoreError(
                f"Store registration address capacity mismatch: {fragment.address}"
            )
        requests[fragment.address] = fragment.nbytes
    return requests


def _unregister_addresses(
    store: StoreBackend,
    addresses: Iterable[int],
) -> list[tuple[int, Union[str, int]]]:
    failures: list[tuple[int, Union[str, int]]] = []
    for address in addresses:
        try:
            result = store.unregister_buffer(address)
        except Exception as error:
            failures.append((address, repr(error)))
            continue
        if result != 0:
            failures.append((address, result))
    return failures


__all__ = ["StoreBufferRegistration", "StoreRegistrationLease"]
