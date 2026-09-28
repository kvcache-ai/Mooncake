"""Physical transfer primitives shared by reusable resource adapters."""

from .completion import (
    PendingTransferManager,
    TransferCompletionFailedError,
    TransferCompletionInterrupted,
    TransferCompletionUnknownError,
    TransferEngineError,
    TransferRegistrationCleanupPendingError,
)
from .contracts import (
    TransferBatch,
    TransferBatchRange,
    TransferBatchReceipt,
    TransferDirection,
)
from .executor import MooncakeTransferEngineExecutor, TransferSubmission
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
    "TransferCompletionInterrupted",
    "TransferCompletionFailedError",
    "TransferDirection",
    "TransferEngineError",
    "TransferRegistrationCleanupPendingError",
    "TransferSubmission",
    "TerminalTransferState",
]
