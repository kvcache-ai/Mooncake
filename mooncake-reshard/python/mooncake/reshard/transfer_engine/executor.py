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
    TransferRegistrationCleanupPendingError,
    _CompletionUnknown,
    _CompletionWaitInterrupted,
    _batch_transfer_with_completion_fence,
)
from .contracts import TransferBatch, TransferBatchReceipt, TransferDirection
from .lifetime import (
    AllocationLifetimeToken,
    AllocationTokenSet,
    TerminalTransferState,
)


class _TransferSubmissionState(Enum):
    ACTIVE = auto()
    POISONED = auto()
    CLOSED = auto()


class TransferSubmission:
    """Execute batches while one resource submission owns the native engine."""

    _executor: MooncakeTransferEngineExecutor
    _state: _TransferSubmissionState
    _physical_io_started: bool

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
        submission._physical_io_started = False
        return submission

    @property
    def physical_io_started(self) -> bool:
        return self._physical_io_started

    def _mark_physical_io_started(self) -> None:
        self._physical_io_started = True

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
            return self._executor._execute_batch(batch, direction, self)
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
        submission: TransferSubmission,
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

        submission._mark_physical_io_started()
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

        handed_off = _handoff_active_registration_frames(
            pending_transfer_id,
            engine_identity=self.engine_identity,
        )
        if not handed_off:
            self._seal_pending_resource_handoff(pending_transfer_id)

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

    def finalize_terminal_resources(
        self,
        lifetime_tokens: AllocationTokenSet,
        terminal_state: TerminalTransferState,
    ) -> None:
        """Release terminal resources through the recoverable pending path."""

        if not isinstance(lifetime_tokens, AllocationTokenSet):
            raise TypeError("lifetime_tokens must be an AllocationTokenSet")
        if not isinstance(terminal_state, TerminalTransferState):
            raise TypeError("terminal_state must be a TerminalTransferState")
        if lifetime_tokens.pending or lifetime_tokens.released:
            return
        pending_transfer_id = self.retain_pending_registration_cleanup(
            terminal_state=terminal_state,
            registrations=(),
            resources=(),
            allocation_tokens=lifetime_tokens.tokens,
        )
        lifetime_tokens.handoff_to_pending()
        try:
            self.drain_pending_transfer(pending_transfer_id, timeout_ms=0)
        except BaseException as error:
            detail = (
                f"allocation lifetime cleanup is quarantined as {pending_transfer_id}"
            )
            if isinstance(error, Exception):
                raise TransferRegistrationCleanupPendingError(
                    detail,
                    pending_transfer_id=pending_transfer_id,
                ) from error
            add_note = getattr(error, "add_note", None)
            if callable(add_note):
                add_note(detail)
            raise

    def finalize_terminal_resource_sets(
        self,
        lifetime_token_sets: Sequence[AllocationTokenSet],
        terminal_state: TerminalTransferState,
    ) -> None:
        """Finalize every acquired token set before surfacing cleanup errors."""

        token_sets = tuple(lifetime_token_sets)
        if any(not isinstance(item, AllocationTokenSet) for item in token_sets):
            raise TypeError("lifetime_token_sets must contain AllocationTokenSet")
        errors: list[BaseException] = []
        for lifetime_tokens in token_sets:
            try:
                self.finalize_terminal_resources(lifetime_tokens, terminal_state)
            except BaseException as error:
                errors.append(error)
        if not errors:
            return
        primary_error = errors[0]
        add_note = getattr(primary_error, "add_note", None)
        if callable(add_note):
            for additional_error in errors[1:]:
                add_note(
                    f"additional terminal resource cleanup error: {additional_error}"
                )
        raise primary_error

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
