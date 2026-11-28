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
from .registered import RegisteredTransferEngine
from .registration import BufferRegistrationLease

__all__ = [
    "BufferRegistrationLease",
    "RegisteredTransferEngine",
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
