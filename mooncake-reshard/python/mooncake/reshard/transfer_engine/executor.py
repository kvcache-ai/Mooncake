"""Resource-neutral Mooncake Transfer Engine batch execution."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Generator, Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, NoReturn, cast

from .completion import (
    PendingTransferManager,
    TransferCompletionFailedError,
    TransferCompletionInterrupted,
    TransferCompletionUnknownError,
    TransferEngineError,
    TransferRegistrationCleanupPendingError,
    _batch_transfer_with_completion_fence,
    _CompletionTicket,
    _CompletionUnknown,
    _CompletionWaitInterrupted,
    _composite_completion_ticket,
    _drain_completion_ticket,
    _UndrainableCompletionUnknownTicket,
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


@dataclass(frozen=True)
class _PreparedBatch:
    """Share arguments; synchronous paths use method names, async uses submit_method."""

    batch: TransferBatch
    direction: TransferDirection
    ticket_method_name: str
    legacy_method_name: str
    submit_method: Callable[..., _CompletionTicket] | None
    arguments: tuple[Any, ...]
    failure_label: str


@dataclass(frozen=True)
class _PublishedBatch:
    index: int
    prepared: _PreparedBatch
    ticket: _CompletionTicket


def _submit_method_name(direction: TransferDirection) -> str:
    return (
        "submit_scatter_read"
        if direction is TransferDirection.READ
        else "submit_scatter_write"
    )


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

    def execute_batches(
        self,
        batches: Iterable[TransferBatch],
        direction: TransferDirection,
        *,
        max_inflight_batches: int = 1,
    ) -> tuple[TransferBatchReceipt, ...]:
        """Borrow batches; the caller owns iterator cleanup after terminal drain."""
        if self._state is _TransferSubmissionState.CLOSED:
            raise TransferEngineError("transfer submission is no longer active")
        if self._state is _TransferSubmissionState.POISONED:
            raise TransferEngineError(
                "transfer submission completion is unresolved and cannot accept "
                "another batch"
            )
        try:
            return self._executor._execute_batches(
                batches,
                direction,
                max_inflight_batches=max_inflight_batches,
                submission=self,
            )
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

        prepared = self._prepare_batch(batch, direction, require_async=False)
        submission._mark_physical_io_started()
        try:
            result = _batch_transfer_with_completion_fence(
                self.engine,
                ticket_method_name=prepared.ticket_method_name,
                legacy_method_name=prepared.legacy_method_name,
                arguments=prepared.arguments,
                max_drain_attempts=self.max_completion_drain_attempts,
                drain_timeout_ms=self.completion_drain_timeout_ms,
            )
        except _CompletionUnknown as error:
            self._raise_completion_unknown(error.ticket, error)
        except _CompletionWaitInterrupted as error:
            self._raise_completion_interrupted(error.ticket, error.interruption, error)
        except Exception as error:
            raise TransferEngineError(
                f"batch transfer {prepared.failure_label} failed: {error}"
            ) from error
        if result != 0:
            raise TransferCompletionFailedError(
                f"batch transfer {prepared.failure_label} failed: {result}"
            )
        return self._receipt(prepared)

    def _prepare_batch(
        self,
        batch: TransferBatch,
        direction: TransferDirection,
        *,
        require_async: bool,
    ) -> _PreparedBatch:
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
        submit_method_name = _submit_method_name(direction)
        submit_method: Callable[..., _CompletionTicket] | None = None
        if require_async and not batch.ranges:
            raise TransferEngineError(
                "bounded in-flight execution requires Scatter range batches"
            )
        if require_async:
            candidate = getattr(self.engine, submit_method_name, None)
            if callable(candidate):
                submit_method = cast(Callable[..., _CompletionTicket], candidate)
        if batch.ranges and (callable(scatter_method) or require_async):
            if require_async and submit_method is None:
                raise TransferEngineError(
                    f"bounded in-flight execution requires {submit_method_name}"
                )
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
        return _PreparedBatch(
            batch=batch,
            direction=direction,
            ticket_method_name=ticket_method_name,
            legacy_method_name=legacy_method_name,
            submit_method=submit_method,
            arguments=arguments,
            failure_label=failure_label,
        )

    @staticmethod
    def _receipt(prepared: _PreparedBatch) -> TransferBatchReceipt:
        batch = prepared.batch
        return TransferBatchReceipt(
            endpoint=batch.endpoint,
            direction=prepared.direction,
            operation_count=batch.operation_count,
            nbytes=batch.nbytes,
        )

    def _execute_batches(
        self,
        batches: Iterable[TransferBatch],
        direction: TransferDirection,
        *,
        max_inflight_batches: int,
        submission: TransferSubmission,
    ) -> tuple[TransferBatchReceipt, ...]:
        if type(max_inflight_batches) is not int or max_inflight_batches <= 0:
            raise ValueError("max_inflight_batches must be a positive integer")
        if max_inflight_batches == 1:
            return tuple(
                submission.execute_batch(batch, direction) for batch in batches
            )

        if not isinstance(direction, TransferDirection):
            raise TypeError("direction must be a TransferDirection")
        submit_method_name = _submit_method_name(direction)
        # Reject before consuming input; preparation rechecks mutable engine methods.
        if not callable(getattr(self.engine, submit_method_name, None)):
            raise TransferEngineError(
                f"bounded in-flight execution requires {submit_method_name}"
            )

        published: deque[_PublishedBatch] = deque()
        receipts: list[TransferBatchReceipt | None] = []
        unresolved: list[_CompletionTicket] = []
        first_failure: tuple[_PreparedBatch, object] | None = None
        interruption: BaseException | None = None
        deferred_error: BaseException | None = None
        unknown_cause: BaseException | None = None

        def drain_published(item: _PublishedBatch) -> None:
            nonlocal deferred_error, first_failure, interruption, unknown_cause
            try:
                result = _drain_completion_ticket(
                    item.ticket,
                    max_drain_attempts=self.max_completion_drain_attempts,
                    drain_timeout_ms=self.completion_drain_timeout_ms,
                )
            except _CompletionUnknown as error:
                unresolved.append(error.ticket)
                if unknown_cause is None:
                    unknown_cause = error
            except _CompletionWaitInterrupted as error:
                unresolved.append(error.ticket)
                if interruption is None:
                    interruption = error.interruption
            else:
                if result != 0:
                    if first_failure is None:
                        first_failure = (item.prepared, result)
                else:
                    try:
                        receipts[item.index] = self._receipt(item.prepared)
                    except BaseException as error:
                        if deferred_error is None:
                            deferred_error = error

        batch_iterator = iter(batches)
        while (
            deferred_error is None
            and first_failure is None
            and not unresolved
            and interruption is None
        ):
            if len(published) >= max_inflight_batches:
                drain_published(published.popleft())
                if (
                    deferred_error is not None
                    or first_failure is not None
                    or unresolved
                    or interruption is not None
                ):
                    break
            try:
                batch = next(batch_iterator)
            except StopIteration:
                break
            except BaseException as error:
                deferred_error = error
                break
            try:
                prepared = self._prepare_batch(
                    batch,
                    direction,
                    require_async=True,
                )
            except BaseException as error:
                deferred_error = error
                break
            index = len(receipts)
            receipts.append(None)
            submit_method = prepared.submit_method
            if submit_method is None:
                deferred_error = TransferEngineError(
                    "async Scatter submit was not prepared"
                )
                break
            submission._mark_physical_io_started()
            try:
                ticket = submit_method(*prepared.arguments)
            except Exception as error:
                unresolved.append(_UndrainableCompletionUnknownTicket())
                unknown_cause = error
                break
            except BaseException as error:
                unresolved.append(_UndrainableCompletionUnknownTicket())
                interruption = error
                break
            try:
                # Probe both attributes before admitting the ticket to the window.
                drain = getattr(ticket, "drain", None)
                getattr(ticket, "status")
            except Exception as error:
                unresolved.append(_UndrainableCompletionUnknownTicket(ticket))
                unknown_cause = error
                break
            except BaseException as error:
                unresolved.append(_UndrainableCompletionUnknownTicket(ticket))
                interruption = error
                break
            if not callable(drain):
                unresolved.append(_UndrainableCompletionUnknownTicket(ticket))
                unknown_cause = TransferEngineError(
                    "Scatter ticket has no callable drain"
                )
                break
            published.append(
                _PublishedBatch(index=index, prepared=prepared, ticket=ticket)
            )

        while published:
            drain_published(published.popleft())

        if (
            deferred_error is not None
            and not isinstance(deferred_error, Exception)
            and interruption is None
        ):
            interruption = deferred_error
        if unresolved:
            composite = _composite_completion_ticket(
                unresolved,
                terminal_failed=(
                    first_failure is not None or deferred_error is not None
                ),
            )
            if interruption is not None:
                self._raise_completion_interrupted(
                    composite,
                    interruption,
                    interruption,
                )
            self._raise_completion_unknown(
                composite,
                unknown_cause if unknown_cause is not None else deferred_error,
            )
        if first_failure is not None:
            prepared, result = first_failure
            raise TransferCompletionFailedError(
                f"batch transfer {prepared.failure_label} failed: {result}"
            )
        if deferred_error is not None:
            raise deferred_error
        completed_receipts: list[TransferBatchReceipt] = []
        for receipt in receipts:
            if receipt is None:
                raise TransferEngineError("completed batch is missing its receipt")
            completed_receipts.append(receipt)
        return tuple(completed_receipts)

    def _raise_completion_unknown(
        self,
        ticket: _CompletionTicket,
        cause: BaseException | None,
    ) -> NoReturn:
        pending_transfer_id = self._pending_manager._retain_pending_ticket(ticket)
        self._handoff_active_registration_frames(pending_transfer_id)
        restart_required = getattr(ticket, "restart_required", False)
        suffix = (
            "; native submission returned no drainable ticket, so this engine is "
            "restart-required"
            if restart_required
            else ""
        )
        error = TransferCompletionUnknownError(
            "batch transfer completion is unknown; registrations remain "
            f"quarantined as {pending_transfer_id}{suffix}",
            pending_transfer_id=pending_transfer_id,
            engine_identity=self.engine_identity,
        )
        if cause is None:
            raise error
        raise error from cause

    def _raise_completion_interrupted(
        self,
        ticket: _CompletionTicket,
        interruption: BaseException,
        cause: BaseException,
    ) -> NoReturn:
        pending_transfer_id = self._pending_manager._retain_pending_ticket(ticket)
        self._handoff_active_registration_frames(pending_transfer_id)
        raise TransferCompletionInterrupted(
            pending_transfer_id,
            interruption,
            engine_identity=self.engine_identity,
        ) from cause

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
