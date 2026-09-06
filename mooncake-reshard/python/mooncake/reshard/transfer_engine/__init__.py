"""Physical transfer primitives shared by reusable resource adapters."""

from .completion import (
    PendingTransferManager,
    TransferCompletionFailedError,
    TransferCompletionUnknownError,
    TransferEngineError,
)
from .contracts import (
    TransferBatch,
    TransferBatchRange,
    TransferBatchReceipt,
    TransferDirection,
)
from .executor import MooncakeTransferEngineExecutor
from .lifetime import (
    AllocationFence,
    AllocationLifetimeToken,
    TerminalTransferState,
)
from .registration import BufferRegistrationLease

__all__ = [
    "BufferRegistrationLease",
    "AllocationFence",
    "AllocationLifetimeToken",
    "MooncakeTransferEngineExecutor",
    "PendingTransferManager",
    "TransferBatch",
    "TransferBatchRange",
    "TransferBatchReceipt",
    "TransferCompletionUnknownError",
    "TransferCompletionFailedError",
    "TransferDirection",
    "TransferEngineError",
    "TerminalTransferState",
]
