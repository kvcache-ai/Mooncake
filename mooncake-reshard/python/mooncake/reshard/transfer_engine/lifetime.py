"""Compatibility exports for Transfer Engine allocation lifetime contracts."""

from ..lifetime import (
    AllocationFence,
    AllocationLifetimeToken,
    AllocationTokenSet,
    TerminalTransferState,
    release_tokens_after_terminal,
)

__all__ = [
    "AllocationFence",
    "AllocationLifetimeToken",
    "AllocationTokenSet",
    "TerminalTransferState",
    "release_tokens_after_terminal",
]
