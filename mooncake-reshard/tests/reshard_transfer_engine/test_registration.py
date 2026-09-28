from __future__ import annotations

import gc
import weakref
from contextlib import AbstractContextManager

import pytest

from mooncake.reshard.contracts import RuntimeBindingFragment
from mooncake.reshard.transfer_engine import (
    AllocationFence,
    BufferRegistrationLease,
    MooncakeTransferEngineExecutor,
    TerminalTransferState,
    TransferBatch,
    TransferCompletionInterrupted,
    TransferCompletionUnknownError,
    TransferDirection,
    TransferEngineError,
    TransferRegistrationCleanupPendingError,
)
from mooncake.reshard.transfer_engine.lifetime import AllocationTokenSet
from mooncake.reshard.transfer_engine.registration import (
    registered_sources,
    registered_targets,
)


class FakeRegistrationEngine:
    def __init__(
        self,
        register_results: list[int | BaseException] | None = None,
        unregister_results: list[int | BaseException] | None = None,
    ) -> None:
        self.register_calls: list[tuple[int, int]] = []
        self.unregister_calls: list[int] = []
        self.register_results = list(register_results or ())
        self.unregister_results = list(unregister_results or ())

    def get_engine_ptr(self) -> int:
        return id(self)

    def register_memory(self, address: int, nbytes: int) -> int:
        self.register_calls.append((address, nbytes))
        if self.register_results:
            result = self.register_results.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result
        return 0

    def unregister_memory(self, address: int) -> int:
        self.unregister_calls.append(address)
        if self.unregister_results:
            result = self.unregister_results.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result
        return 0


class InterruptedTicket:
    status = "COMPLETION_UNKNOWN"

    def __init__(self) -> None:
        self._interrupt = True

    def drain(self, timeout_ms: int) -> str:
        if self._interrupt:
            self._interrupt = False
            raise KeyboardInterrupt("completion wait interrupted")
        self.status = "COMPLETED"
        return self.status


class InterruptingRegistrationEngine(FakeRegistrationEngine):
    def __init__(self) -> None:
        super().__init__()
        self.ticket = InterruptedTicket()

    def batch_transfer_sync_write_with_ticket(self, *arguments):
        return self.ticket


class UnknownTicket:
    status = "COMPLETION_UNKNOWN"

    def drain(self, timeout_ms: int) -> str:
        return self.status


class UnknownRegistrationEngine(FakeRegistrationEngine):
    def __init__(self) -> None:
        super().__init__()
        self.ticket = UnknownTicket()

    def batch_transfer_sync_write_with_ticket(self, *arguments):
        return self.ticket


class RegistrationResource:
    pass


class HandoffOrderingProbe:
    def __init__(self, executor: MooncakeTransferEngineExecutor) -> None:
        self.executor = executor
        self.observed_incomplete_stage = False

    @property
    def engine_identity(self) -> tuple[str, int]:
        return self.executor.engine_identity

    def retain_pending_resources(self, pending_transfer_id: str, **kwargs) -> None:
        self.executor.retain_pending_resources(pending_transfer_id, **kwargs)

    def _stage_pending_resources(self, pending_transfer_id: str, **kwargs) -> None:
        self.executor._stage_pending_resources(pending_transfer_id, **kwargs)
        with pytest.raises(TransferEngineError, match="handoff is incomplete"):
            self.executor.pending_transfer_status(pending_transfer_id)
        self.observed_incomplete_stage = True

    def _seal_pending_resource_handoff(self, pending_transfer_id: str) -> None:
        assert self.observed_incomplete_stage
        self.executor._seal_pending_resource_handoff(pending_transfer_id)

    def retain_pending_registration_cleanup(self, **kwargs) -> str:
        return self.executor.retain_pending_registration_cleanup(**kwargs)


class RecordingAllocationToken:
    def __init__(
        self,
        fence: AllocationFence,
        released_states: list[TerminalTransferState],
    ) -> None:
        self.fence = fence
        self.released_states = released_states

    def release_after_terminal(self, terminal_state: TerminalTransferState) -> None:
        self.released_states.append(terminal_state)


def _fragment(
    fragment_id: str,
    *,
    storage_address: int = 0x10000,
    storage_nbytes: int = 64,
    storage_offset_bytes: int = 0,
) -> RuntimeBindingFragment:
    return RuntimeBindingFragment(
        placement_fragment_id=f"placement-{fragment_id}",
        fragment_id=fragment_id,
        address=storage_address + storage_offset_bytes,
        nbytes=16,
        worker_id="worker-0",
        endpoint="worker-0:12345",
        device="cuda:0",
        itemsize=4,
        local_shape=(4,),
        strides_bytes=(4,),
        storage_address=storage_address,
        storage_nbytes=storage_nbytes,
        storage_offset_bytes=storage_offset_bytes,
    )


def _token(
    token_id: str,
    released_states: list[TerminalTransferState],
) -> RecordingAllocationToken:
    return RecordingAllocationToken(
        AllocationFence(
            resource_id="resource",
            revision="revision",
            placement_id="placement",
            placement_digest="placement-digest",
            instance_id="instance",
            participant_id="participant",
            runtime_lease_id="lease",
            runtime_generation=1,
            binding_digest="binding-digest",
            fragment_ids=("runtime-0",),
            token_id=token_id,
        ),
        released_states,
    )


def _registered(
    label: str,
    engine: FakeRegistrationEngine,
    pending_owner: MooncakeTransferEngineExecutor | HandoffOrderingProbe,
    fragments: tuple[RuntimeBindingFragment, ...],
    *,
    pre_registered: bool = False,
    resources: tuple[object, ...] = (),
    lifetime_tokens: AllocationTokenSet | None = None,
) -> AbstractContextManager[None]:
    if label == "source":
        registrations = (
            tuple(
                BufferRegistrationLease.from_fragment(
                    fragment,
                    lease_generation=1,
                    runtime_lease_id="lease",
                )
                for fragment in fragments
            )
            if pre_registered
            else None
        )
        return registered_sources(
            engine,
            pending_owner,
            fragments,
            pre_registered=pre_registered,
            registrations=registrations,
            lease_generation=1,
            runtime_lease_id="lease",
            resources=resources,
            lifetime_tokens=lifetime_tokens,
        )
    if label == "target":
        return registered_targets(
            engine,
            pending_owner,
            fragments,
            pre_registered=pre_registered,
            resources=resources,
            lifetime_tokens=lifetime_tokens,
        )
    raise AssertionError(f"unknown registration label: {label}")


def _batch() -> TransferBatch:
    return TransferBatch(
        endpoint="worker-1:12345",
        source_addresses=(0x1000,),
        target_addresses=(0x2000,),
        sizes=(16,),
    )


@pytest.mark.parametrize("label", ("source", "target"))
def test_registration_registers_and_unregisters_each_allocation_once(
    label: str,
) -> None:
    engine = FakeRegistrationEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    fragments = (
        _fragment("runtime-0"),
        _fragment("runtime-1", storage_offset_bytes=16),
    )

    with _registered(label, engine, executor, fragments):
        assert engine.register_calls == [(0x10000, 64)]
        assert engine.unregister_calls == []

    assert engine.unregister_calls == [0x10000]


@pytest.mark.parametrize(
    ("outer_label", "inner_label"),
    (("source", "target"), ("target", "source")),
)
@pytest.mark.parametrize(
    ("outer_pre_registered", "inner_pre_registered"),
    ((False, False), (False, True), (True, False), (True, True)),
)
def test_nested_registrations_retain_all_allocations_on_interruption(
    outer_label: str,
    inner_label: str,
    outer_pre_registered: bool,
    inner_pre_registered: bool,
) -> None:
    engine = InterruptingRegistrationEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    fragments = {
        "source": (_fragment("source", storage_address=0x10000),),
        "target": (_fragment("target", storage_address=0x20000),),
    }

    with pytest.raises(TransferCompletionInterrupted) as raised:
        with _registered(
            outer_label,
            engine,
            executor,
            fragments[outer_label],
            pre_registered=outer_pre_registered,
        ):
            with _registered(
                inner_label,
                engine,
                executor,
                fragments[inner_label],
                pre_registered=inner_pre_registered,
            ):
                executor.execute_batch(_batch(), TransferDirection.WRITE)

    unregister_calls_before_drain = tuple(engine.unregister_calls)
    pending_transfer_id = raised.value.pending_transfer_id
    assert executor.drain_pending_transfer(pending_transfer_id) == "COMPLETED"

    assert unregister_calls_before_drain == ()
    expected_unregistered = {
        fragments[label][0].storage_address
        for label, pre_registered in (
            (outer_label, outer_pre_registered),
            (inner_label, inner_pre_registered),
        )
        if not pre_registered
    }
    assert set(engine.unregister_calls) == expected_unregistered


def test_outer_registration_remains_handed_off_when_interruption_is_caught() -> None:
    engine = InterruptingRegistrationEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    pending_transfer_id = ""

    with _registered(
        "source",
        engine,
        executor,
        (_fragment("source", storage_address=0x10000),),
    ):
        try:
            with _registered(
                "target",
                engine,
                executor,
                (_fragment("target", storage_address=0x20000),),
            ):
                executor.execute_batch(_batch(), TransferDirection.WRITE)
        except TransferCompletionInterrupted as error:
            pending_transfer_id = error.pending_transfer_id

    assert pending_transfer_id
    assert engine.unregister_calls == []
    assert executor.drain_pending_transfer(pending_transfer_id) == "COMPLETED"
    assert set(engine.unregister_calls) == {0x10000, 0x20000}


@pytest.mark.parametrize("label", ("source", "target"))
@pytest.mark.parametrize("completion", ("unknown", "interrupted"))
def test_caught_pending_completion_still_hands_off_registration_resources(
    label: str,
    completion: str,
) -> None:
    engine = (
        UnknownRegistrationEngine()
        if completion == "unknown"
        else InterruptingRegistrationEngine()
    )
    executor = MooncakeTransferEngineExecutor(engine)
    released_states: list[TerminalTransferState] = []
    token_set = AllocationTokenSet((_token(f"{label}-{completion}", released_states),))
    resource = RegistrationResource()
    resource_ref = weakref.ref(resource)
    pending_transfer_id = ""

    with _registered(
        label,
        engine,
        executor,
        (_fragment("runtime-0"),),
        resources=(resource,),
        lifetime_tokens=token_set,
    ):
        try:
            executor.execute_batch(_batch(), TransferDirection.WRITE)
        except (TransferCompletionUnknownError, TransferCompletionInterrupted) as error:
            pending_transfer_id = error.pending_transfer_id

    assert pending_transfer_id
    assert engine.unregister_calls == []
    assert token_set.pending
    assert released_states == []
    del resource
    gc.collect()
    assert resource_ref() is not None

    engine.ticket.status = "COMPLETED"
    assert executor.drain_pending_transfer(pending_transfer_id) == "COMPLETED"
    assert engine.unregister_calls == [0x10000]
    assert released_states == [TerminalTransferState.COMPLETED]


def test_pending_handoff_is_sealed_after_all_frames_are_transferred() -> None:
    engine = InterruptingRegistrationEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    pending_owner = HandoffOrderingProbe(executor)

    with pytest.raises(TransferCompletionInterrupted) as raised:
        with _registered(
            "source",
            engine,
            pending_owner,
            (_fragment("source", storage_address=0x10000),),
        ):
            with _registered(
                "target",
                engine,
                pending_owner,
                (_fragment("target", storage_address=0x20000),),
            ):
                executor.execute_batch(_batch(), TransferDirection.WRITE)

    assert pending_owner.observed_incomplete_stage
    assert executor.pending_transfer_status(raised.value.pending_transfer_id) == (
        "COMPLETION_UNKNOWN"
    )
    assert (
        executor.drain_pending_transfer(raised.value.pending_transfer_id) == "COMPLETED"
    )


def test_nested_different_engines_only_handoff_matching_registrations() -> None:
    outer_engine = FakeRegistrationEngine()
    outer_executor = MooncakeTransferEngineExecutor(outer_engine)
    inner_engine = InterruptingRegistrationEngine()
    inner_executor = MooncakeTransferEngineExecutor(inner_engine)

    with pytest.raises(TransferCompletionInterrupted) as raised:
        with _registered(
            "source",
            outer_engine,
            outer_executor,
            (_fragment("source", storage_address=0x10000),),
        ):
            with _registered(
                "target",
                inner_engine,
                inner_executor,
                (_fragment("target", storage_address=0x20000),),
            ):
                inner_executor.execute_batch(_batch(), TransferDirection.WRITE)

    assert outer_engine.unregister_calls == [0x10000]
    assert inner_engine.unregister_calls == []
    assert (
        inner_executor.drain_pending_transfer(raised.value.pending_transfer_id)
        == "COMPLETED"
    )
    assert inner_engine.unregister_calls == [0x20000]


@pytest.mark.parametrize("label", ("source", "target"))
def test_unregister_failure_retains_cleanup_until_retry_succeeds(
    label: str,
) -> None:
    engine = FakeRegistrationEngine(unregister_results=[-7, -8, 0])
    executor = MooncakeTransferEngineExecutor(engine)
    released_states: list[TerminalTransferState] = []
    token = _token(f"{label}-token", released_states)
    token_ref = weakref.ref(token)
    token_set = AllocationTokenSet((token,))
    resource = object()

    with pytest.raises(TransferRegistrationCleanupPendingError) as raised:
        with _registered(
            label,
            engine,
            executor,
            (_fragment("runtime-0"),),
            resources=(resource,),
            lifetime_tokens=token_set,
        ):
            pass

    pending_transfer_id = raised.value.pending_transfer_id
    assert engine.register_calls == [(0x10000, 64)]
    assert engine.unregister_calls == [0x10000]
    assert executor.pending_transfer_ids() == (pending_transfer_id,)
    assert executor.pending_transfer_status(pending_transfer_id) == "COMPLETED"
    assert token_set.pending
    assert released_states == []

    del raised
    del token
    del token_set
    gc.collect()
    assert token_ref() is not None

    with pytest.raises(TransferEngineError, match="registration cleanup failed"):
        executor.drain_pending_transfer(pending_transfer_id)

    assert executor.pending_transfer_ids() == (pending_transfer_id,)
    assert engine.unregister_calls == [0x10000, 0x10000]
    assert released_states == []
    gc.collect()
    assert token_ref() is not None

    assert executor.drain_pending_transfer(pending_transfer_id) == "COMPLETED"
    assert executor.pending_transfer_ids() == ()
    assert engine.unregister_calls == [0x10000, 0x10000, 0x10000]
    assert released_states == [TerminalTransferState.COMPLETED]
    gc.collect()
    assert token_ref() is None


@pytest.mark.parametrize("label", ("source", "target"))
def test_pending_unregister_exception_requires_restart_without_replay(
    label: str,
) -> None:
    engine = FakeRegistrationEngine(unregister_results=[-7])
    executor = MooncakeTransferEngineExecutor(engine)

    with pytest.raises(TransferRegistrationCleanupPendingError) as raised:
        with _registered(
            label,
            engine,
            executor,
            (_fragment("runtime-0"),),
        ):
            pass

    pending_transfer_id = raised.value.pending_transfer_id
    engine.unregister_results.append(RuntimeError("cleanup outcome unknown"))
    with pytest.raises(TransferEngineError, match="restart required"):
        executor.drain_pending_transfer(pending_transfer_id)

    assert executor.pending_transfer_status(pending_transfer_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert executor.drain_pending_transfer(pending_transfer_id) == (
        "COMPLETION_UNKNOWN"
    )
    assert engine.unregister_calls == [0x10000, 0x10000]


@pytest.mark.parametrize("label", ("source", "target"))
def test_registration_rejects_duplicate_allocation_capacity_mismatch(
    label: str,
) -> None:
    engine = FakeRegistrationEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    fragments = (
        _fragment("runtime-0", storage_nbytes=64),
        _fragment(
            "runtime-1",
            storage_nbytes=32,
            storage_offset_bytes=16,
        ),
    )

    with pytest.raises(
        TransferEngineError,
        match=rf"{label} allocation capacity mismatch",
    ):
        with _registered(label, engine, executor, fragments):
            pass

    assert engine.register_calls == []
    assert engine.unregister_calls == []


@pytest.mark.parametrize("label", ("source", "target"))
def test_cleanup_interruption_quarantines_registration_until_restart(
    label: str,
) -> None:
    engine = FakeRegistrationEngine(
        unregister_results=[KeyboardInterrupt("cleanup interrupted")]
    )
    executor = MooncakeTransferEngineExecutor(engine)
    released_states: list[TerminalTransferState] = []
    token_set = AllocationTokenSet((_token(f"{label}-token", released_states),))

    with pytest.raises(KeyboardInterrupt, match="cleanup interrupted"):
        with _registered(
            label,
            engine,
            executor,
            (_fragment("runtime-0"),),
            resources=(object(),),
            lifetime_tokens=token_set,
        ):
            pass

    (pending_transfer_id,) = executor.pending_transfer_ids()
    assert executor.pending_transfer_status(pending_transfer_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert executor.drain_pending_transfer(pending_transfer_id) == (
        "COMPLETION_UNKNOWN"
    )
    assert engine.unregister_calls == [0x10000]
    assert token_set.pending
    assert released_states == []
    with pytest.raises(TransferEngineError, match="restart-required"):
        with executor.submission():
            pass


@pytest.mark.parametrize("label", ("source", "target"))
def test_registration_interruption_quarantines_unknown_allocation_until_restart(
    label: str,
) -> None:
    engine = FakeRegistrationEngine(
        register_results=[KeyboardInterrupt("registration interrupted")]
    )
    executor = MooncakeTransferEngineExecutor(engine)

    with pytest.raises(KeyboardInterrupt, match="registration interrupted"):
        with _registered(
            label,
            engine,
            executor,
            (_fragment("runtime-0"),),
            resources=(object(),),
        ):
            pass

    (pending_transfer_id,) = executor.pending_transfer_ids()
    assert executor.pending_transfer_status(pending_transfer_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert engine.register_calls == [(0x10000, 64)]
    assert engine.unregister_calls == []
