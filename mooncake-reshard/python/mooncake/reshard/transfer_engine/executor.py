"""Resource-neutral Mooncake Transfer Engine batch execution."""

from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Generator
from enum import Enum, auto
from typing import Any, Sequence

from .completion import (
    PendingTransferManager,
    TransferCompletionFailedError,
    TransferCompletionInterrupted,
    TransferCompletionUnknownError,
    TransferEngineError,
    _CompletionUnknown,
    _CompletionWaitInterrupted,
    _batch_transfer_with_completion_fence,
)
from .contracts import TransferBatch, TransferBatchReceipt, TransferDirection
from .lifetime import AllocationLifetimeToken, TerminalTransferState


class _TransferSubmissionState(Enum):
    ACTIVE = auto()
    POISONED = auto()
    CLOSED = auto()


class TransferSubmission:
    """Execute batches while one resource submission owns the native engine."""

    _executor: MooncakeTransferEngineExecutor
    _state: _TransferSubmissionState

    def __init__(self, executor: MooncakeTransferEngineExecutor) -> None:
        raise TransferEngineError(
            "transfer submissions must be created by executor.submission()"
        )

    @classmethod
    def _create(
        cls,
        executor: MooncakeTransferEngineExecutor,
    ) -> TransferSubmission:
        submission = object.__new__(cls)
        submission._executor = executor
        submission._state = _TransferSubmissionState.ACTIVE
        return submission

    def execute_batch(
        self,
        batch: TransferBatch,
        direction: TransferDirection,
    ) -> TransferBatchReceipt:
        if self._state is _TransferSubmissionState.CLOSED:
            raise TransferEngineError("transfer submission is no longer active")
        if self._state is _TransferSubmissionState.POISONED:
            raise TransferEngineError(
                "transfer submission completion is unresolved and cannot accept "
                "another batch"
            )
        try:
            return self._executor._execute_batch(batch, direction)
        except (
            TransferCompletionInterrupted,
            TransferCompletionUnknownError,
        ):
            self._state = _TransferSubmissionState.POISONED
            raise

    def _close(self) -> None:
        self._state = _TransferSubmissionState.CLOSED


class MooncakeTransferEngineExecutor:
    """Submit address ranges without owning weight or KV semantics."""

    def __init__(
        self,
        engine: Any,
        *,
        max_completion_drain_attempts: int = 3,
        completion_drain_timeout_ms: int = 1000,
    ) -> None:
        if (
            type(max_completion_drain_attempts) is not int
            or max_completion_drain_attempts < 0
        ):
            raise ValueError("max_completion_drain_attempts must be non-negative")
        if (
            type(completion_drain_timeout_ms) is not int
            or completion_drain_timeout_ms < 0
        ):
            raise ValueError("completion_drain_timeout_ms must be non-negative")
        self.engine = engine
        self.max_completion_drain_attempts = max_completion_drain_attempts
        self.completion_drain_timeout_ms = completion_drain_timeout_ms
        self._pending_manager = PendingTransferManager(engine)

    @property
    def engine_identity(self) -> tuple[str, int]:
        """Return the process-local identity used for submission fencing."""

        return self._pending_manager.engine_identity

    @contextmanager
    def submission(self) -> Generator[TransferSubmission, None, None]:
        """Reserve the native engine across all batches of one resource plan."""

        self._pending_manager._reserve_submission()
        submission = TransferSubmission._create(self)
        try:
            yield submission
        finally:
            submission._close()
            self._pending_manager._release_submission()

    def execute_batch(
        self,
        batch: TransferBatch,
        direction: TransferDirection,
    ) -> TransferBatchReceipt:
        """Execute one standalone batch with engine-wide admission fencing."""

        with self.submission() as submission:
            return submission.execute_batch(batch, direction)

    def _execute_batch(
        self,
        batch: TransferBatch,
        direction: TransferDirection,
    ) -> TransferBatchReceipt:
        """Execute a batch inside an already reserved resource submission."""

        if not isinstance(batch, TransferBatch):
            raise TypeError("batch must be a TransferBatch")
        if not isinstance(direction, TransferDirection):
            raise TypeError("direction must be a TransferDirection")

        scatter_method_name = (
            "scatter_transfer_sync_read_with_ticket"
            if direction is TransferDirection.READ
            else "scatter_transfer_sync_write_with_ticket"
        )
        scatter_method = getattr(self.engine, scatter_method_name, None)
        if batch.ranges and callable(scatter_method):
            ticket_method_name = scatter_method_name
            legacy_method_name = (
                "batch_transfer_sync_read"
                if direction is TransferDirection.READ
                else "batch_transfer_sync_write"
            )
            if direction is TransferDirection.READ:
                local_base_addresses = [
                    item.target_base_address for item in batch.ranges
                ]
                local_capacities = [item.target_capacity for item in batch.ranges]
                remote_base_addresses = [
                    item.source_base_address for item in batch.ranges
                ]
                remote_capacities = [item.source_capacity for item in batch.ranges]
                local_offsets = [list(item.target_offsets) for item in batch.ranges]
                remote_offsets = [list(item.source_offsets) for item in batch.ranges]
            else:
                local_base_addresses = [
                    item.source_base_address for item in batch.ranges
                ]
                local_capacities = [item.source_capacity for item in batch.ranges]
                remote_base_addresses = [
                    item.target_base_address for item in batch.ranges
                ]
                remote_capacities = [item.target_capacity for item in batch.ranges]
                local_offsets = [list(item.source_offsets) for item in batch.ranges]
                remote_offsets = [list(item.target_offsets) for item in batch.ranges]
            arguments = (
                batch.endpoint,
                local_base_addresses,
                local_capacities,
                remote_base_addresses,
                remote_capacities,
                local_offsets,
                remote_offsets,
                [list(item.sizes) for item in batch.ranges],
            )
            failure_label = (
                f"from {batch.endpoint}"
                if direction is TransferDirection.READ
                else f"to {batch.endpoint}"
            )
        elif direction is TransferDirection.READ:
            ticket_method_name = "batch_transfer_sync_read_with_ticket"
            legacy_method_name = "batch_transfer_sync_read"
            arguments = (
                batch.endpoint,
                list(batch.target_addresses),
                list(batch.source_addresses),
                list(batch.sizes),
            )
            failure_label = f"from {batch.endpoint}"
        else:
            ticket_method_name = "batch_transfer_sync_write_with_ticket"
            legacy_method_name = "batch_transfer_sync_write"
            arguments = (
                batch.endpoint,
                list(batch.source_addresses),
                list(batch.target_addresses),
                list(batch.sizes),
            )
            failure_label = f"to {batch.endpoint}"

        try:
            result = _batch_transfer_with_completion_fence(
                self.engine,
                ticket_method_name=ticket_method_name,
                legacy_method_name=legacy_method_name,
                arguments=arguments,
                max_drain_attempts=self.max_completion_drain_attempts,
                drain_timeout_ms=self.completion_drain_timeout_ms,
            )
        except _CompletionUnknown as error:
            pending_transfer_id = self._pending_manager._retain_pending_ticket(
                error.ticket
            )
            self._handoff_active_registration_frames(pending_transfer_id)
            restart_required = getattr(error.ticket, "restart_required", False)
            suffix = (
                "; legacy API exposes no drainable ticket, so this engine is "
                "restart-required"
                if restart_required
                else ""
            )
            raise TransferCompletionUnknownError(
                "batch transfer completion is unknown; registrations remain "
                f"quarantined as {pending_transfer_id}{suffix}",
                pending_transfer_id=pending_transfer_id,
                engine_identity=self.engine_identity,
            ) from error
        except _CompletionWaitInterrupted as error:
            pending_transfer_id = self._pending_manager._retain_pending_ticket(
                error.ticket
            )
            self._handoff_active_registration_frames(pending_transfer_id)
            raise TransferCompletionInterrupted(
                pending_transfer_id,
                error.interruption,
                engine_identity=self.engine_identity,
            ) from error
        except Exception as error:
            raise TransferEngineError(
                f"batch transfer {failure_label} failed: {error}"
            ) from error
        if result != 0:
            raise TransferCompletionFailedError(
                f"batch transfer {failure_label} failed: {result}"
            )
        return TransferBatchReceipt(
            endpoint=batch.endpoint,
            direction=direction,
            operation_count=batch.operation_count,
            nbytes=batch.nbytes,
        )

    def _handoff_active_registration_frames(self, pending_transfer_id: str) -> None:
        from .registration import _handoff_active_registration_frames

        _handoff_active_registration_frames(
            pending_transfer_id,
            engine_identity=self.engine_identity,
        )

    def retain_pending_resources(
        self,
        pending_transfer_id: str,
        *,
        registrations: Sequence[int],
        resources: Sequence[Any],
        allocation_tokens: Sequence[AllocationLifetimeToken] = (),
    ) -> None:
        self._pending_manager._retain_pending_resources(
            pending_transfer_id,
            registrations=registrations,
            resources=resources,
            allocation_tokens=allocation_tokens,
        )

    def _stage_pending_resources(
        self,
        pending_transfer_id: str,
        *,
        registrations: Sequence[int],
        resources: Sequence[Any],
        allocation_tokens: Sequence[AllocationLifetimeToken] = (),
    ) -> None:
        self._pending_manager._retain_pending_resources(
            pending_transfer_id,
            registrations=registrations,
            resources=resources,
            allocation_tokens=allocation_tokens,
            handoff_complete=False,
        )

    def _seal_pending_resource_handoff(self, pending_transfer_id: str) -> None:
        self._pending_manager._seal_pending_resource_handoff(pending_transfer_id)

    def retain_pending_registration_cleanup(
        self,
        *,
        terminal_state: TerminalTransferState,
        registrations: Sequence[int],
        resources: Sequence[Any],
        allocation_tokens: Sequence[AllocationLifetimeToken] = (),
        restart_required: bool = False,
    ) -> str:
        return self._pending_manager._retain_pending_registration_cleanup(
            terminal_state=terminal_state,
            registrations=registrations,
            resources=resources,
            allocation_tokens=allocation_tokens,
            restart_required=restart_required,
        )

    def pending_transfer_ids(self) -> tuple[str, ...]:
        return self._pending_manager.pending_transfer_ids()

    def pending_transfer_status(self, pending_transfer_id: str) -> str:
        return self._pending_manager.pending_transfer_status(pending_transfer_id)

    def drain_pending_transfer(
        self,
        pending_transfer_id: str,
        *,
        timeout_ms: int = 1000,
    ) -> str:
        return self._pending_manager.drain_pending_transfer(
            pending_transfer_id,
            timeout_ms=timeout_ms,
        )
