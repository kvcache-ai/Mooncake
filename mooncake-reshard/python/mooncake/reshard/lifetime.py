"""Reusable allocation lifetime contracts.

These objects deliberately stay outside placement manifests and serialized
plans. Framework adapters own the actual allocator pin; Store and Transfer
Engine consumers verify the returned fence and release it after completion.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol, Sequence, runtime_checkable


class TerminalTransferState(str, Enum):
    ABORTED = "aborted"
    COMPLETED = "completed"
    FAILED_DRAINED = "failed_drained"


@dataclass(frozen=True)
class AllocationFence:
    """Comparable identity of one framework-pinned runtime allocation scope."""

    resource_id: str
    revision: str
    placement_id: str
    placement_digest: str
    instance_id: str
    participant_id: str
    runtime_lease_id: str
    runtime_generation: int
    binding_digest: str
    fragment_ids: tuple[str, ...]
    token_id: str

    def __post_init__(self) -> None:
        for name in (
            "resource_id",
            "revision",
            "placement_id",
            "placement_digest",
            "instance_id",
            "participant_id",
            "runtime_lease_id",
            "binding_digest",
            "token_id",
        ):
            value = getattr(self, name)
            if type(value) is not str or not value:
                raise ValueError(f"allocation fence {name} must be non-empty")
        if type(self.runtime_generation) is not int or self.runtime_generation < 0:
            raise ValueError("allocation fence runtime_generation is invalid")
        fragment_ids = tuple(self.fragment_ids)
        if not fragment_ids or any(
            type(value) is not str or not value for value in fragment_ids
        ):
            raise ValueError("allocation fence fragment_ids are invalid")
        if len(fragment_ids) != len(set(fragment_ids)):
            raise ValueError("allocation fence fragment_ids contain duplicates")
        object.__setattr__(self, "fragment_ids", tuple(sorted(fragment_ids)))


@runtime_checkable
class AllocationLifetimeToken(Protocol):
    """Opaque framework allocation pin retained until a terminal TE outcome."""

    @property
    def fence(self) -> AllocationFence: ...

    def release_after_terminal(self, terminal_state: TerminalTransferState) -> None: ...


@dataclass
class AllocationTokenSet:
    """Own a prepared set of allocation pins across normal and pending paths."""

    tokens: tuple[AllocationLifetimeToken, ...]
    _pending: bool = field(default=False, init=False, repr=False)
    _released: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.tokens = tuple(self.tokens)
        seen_token_ids: set[str] = set()
        for token in self.tokens:
            if not isinstance(token, AllocationLifetimeToken):
                raise ValueError("allocation token has invalid lifetime interface")
            token_id = token.fence.token_id
            if token_id in seen_token_ids:
                raise ValueError("allocation token set contains duplicate token_id")
            seen_token_ids.add(token_id)

    @property
    def pending(self) -> bool:
        return self._pending

    @property
    def released(self) -> bool:
        return self._released

    def handoff_to_pending(self) -> tuple[AllocationLifetimeToken, ...]:
        if self._released:
            raise RuntimeError("allocation token set was already released")
        self._pending = True
        return self.tokens

    def release_after_terminal(self, terminal_state: TerminalTransferState) -> None:
        if self._pending or self._released:
            return
        _release_tokens(self.tokens, terminal_state)
        self._released = True


def release_tokens_after_terminal(
    tokens: Sequence[AllocationLifetimeToken],
    terminal_state: TerminalTransferState,
) -> None:
    """Release already-acquired pins after registrations are safely cleaned up."""

    _release_tokens(tuple(tokens), terminal_state)


def _release_tokens(
    tokens: Sequence[AllocationLifetimeToken],
    terminal_state: TerminalTransferState,
) -> None:
    for token in reversed(tuple(tokens)):
        token.release_after_terminal(terminal_state)


__all__ = [
    "AllocationFence",
    "AllocationLifetimeToken",
    "AllocationTokenSet",
    "TerminalTransferState",
    "release_tokens_after_terminal",
]
