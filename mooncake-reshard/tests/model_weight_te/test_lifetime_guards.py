from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    TransferCompletionFailedError,
    MooncakeTransferEngineSink,
    TransferCompletionUnknownError,
    TransferEngineError,
    TransferRegistrationCleanupPendingError,
)
from mooncake.reshard.transfer_engine.lifetime import TerminalTransferState
from mooncake.reshard.weight.lifetime import (
    AcquiredWeightBinding,
    weight_allocation_fence,
)

from .helpers import (
    FakeBatchTransferTicket,
    FakeAllocationLifetimeToken,
    FakeTransferEngine,
    allocation_guards,
    execute_reader,
    execute_sink,
    manifests,
    plan_transfer,
    plan_transfer_to_local_target,
    registration_leases,
)


class ReleaseFailingAllocationGuard:
    def __init__(self, binding) -> None:
        self.binding = binding
        self.tokens = []

    def acquire(
        self,
        *,
        transfer_id,
        expected_binding,
        required_fragment_ids,
    ) -> AcquiredWeightBinding:
        assert expected_binding == self.binding
        token = ReleaseFailingAllocationToken(
            weight_allocation_fence(
                self.binding,
                required_fragment_ids,
                token_id=f"release-failure-{transfer_id}",
            )
        )
        self.tokens.append(token)
        return AcquiredWeightBinding(binding=self.binding, token=token)


class ReleaseFailingAllocationToken(FakeAllocationLifetimeToken):
    def release_after_terminal(self, terminal_state) -> None:
        self.released_states.append(terminal_state)
        raise RuntimeError("release failed")


class FixedTokenIdAllocationGuard:
    def __init__(self, binding, *, token_id: str, fail_release: bool) -> None:
        self.binding = binding
        self.token_id = token_id
        self.fail_release = fail_release
        self.tokens = []

    def acquire(
        self,
        *,
        transfer_id,
        expected_binding,
        required_fragment_ids,
    ) -> AcquiredWeightBinding:
        assert expected_binding == self.binding
        token_type = (
            ReleaseFailingAllocationToken
            if self.fail_release
            else FakeAllocationLifetimeToken
        )
        token = token_type(
            weight_allocation_fence(
                self.binding,
                required_fragment_ids,
                token_id=self.token_id,
            )
        )
        self.tokens.append(token)
        return AcquiredWeightBinding(binding=self.binding, token=token)


class DriftingReleaseFailingGuard:
    def __init__(self, fresh_binding) -> None:
        self.fresh_binding = fresh_binding
        self.tokens = []

    def acquire(
        self,
        *,
        transfer_id,
        expected_binding,
        required_fragment_ids,
    ) -> AcquiredWeightBinding:
        token = ReleaseFailingAllocationToken(
            weight_allocation_fence(
                self.fresh_binding,
                required_fragment_ids,
                token_id=f"drifting-release-{transfer_id}",
            )
        )
        self.tokens.append(token)
        return AcquiredWeightBinding(binding=self.fresh_binding, token=token)


class DriftingAllocationGuard:
    def __init__(self, fresh_binding) -> None:
        self.fresh_binding = fresh_binding
        self.tokens = []

    def acquire(
        self,
        *,
        transfer_id,
        expected_binding,
        required_fragment_ids,
    ) -> AcquiredWeightBinding:
        token = FakeAllocationLifetimeToken(
            weight_allocation_fence(
                self.fresh_binding,
                required_fragment_ids,
                token_id=f"drift-{transfer_id}",
            )
        )
        self.tokens.append(token)
        return AcquiredWeightBinding(binding=self.fresh_binding, token=token)


def _all_tokens(*guard_maps):
    return tuple(
        token
        for guard_map in guard_maps
        for guard in guard_map.values()
        for token in guard.tokens
    )


def test_live_te_rejects_raw_bindings_without_allocation_guards() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)

    with pytest.raises(ValueError, match="source allocation guard providers"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan_transfer(source, target),
            source.placement,
            source.bindings[0],
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
        )


def test_fresh_binding_validation_failure_releases_current_token() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    expected = source.bindings[0]
    guard = DriftingAllocationGuard(
        replace(expected, generation=expected.generation + 1)
    )
    engine = FakeTransferEngine()

    with pytest.raises(ValueError, match="generation differs from plan"):
        MooncakeTransferEngineSink(engine).execute(
            plan_transfer(source, target),
            source.placement,
            expected,
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
            source_allocation_guards={
                (expected.instance_id, expected.participant_id): guard
            },
            target_allocation_guards=allocation_guards(target),
        )

    assert engine.calls == []
    assert len(guard.tokens) == 1
    assert guard.tokens[0].released_states == [TerminalTransferState.ABORTED]


def test_live_te_holds_source_and_target_guards_until_known_completion() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_guards = allocation_guards(target)

    MooncakeTransferEngineSink(FakeTransferEngine()).execute(
        plan_transfer(source, target),
        source.placement,
        source.bindings[0],
        target.placement,
        target.bindings,
        target_registrations=registration_leases(target),
        source_allocation_guards=source_guards,
        target_allocation_guards=target_guards,
    )

    assert _all_tokens(source_guards, target_guards)
    assert all(
        token.released_states == [TerminalTransferState.COMPLETED]
        for token in _all_tokens(source_guards, target_guards)
    )


def test_pending_transfer_keeps_guards_until_drain_reaches_terminal() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_guards = allocation_guards(target)
    engine = FakeTransferEngine()
    ticket = FakeBatchTransferTicket(["COMPLETION_UNKNOWN"] * 3)
    engine.batch_transfer_sync_write_with_ticket = lambda *args: ticket
    sink = MooncakeTransferEngineSink(engine, max_completion_drain_attempts=1)

    with pytest.raises(TransferCompletionUnknownError) as raised:
        sink.execute(
            plan_transfer(source, target),
            source.placement,
            source.bindings[0],
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
            source_allocation_guards=source_guards,
            target_allocation_guards=target_guards,
        )

    assert all(
        token.released_states == []
        for token in _all_tokens(source_guards, target_guards)
    )
    ticket._statuses = ["COMPLETED"]
    assert sink.drain_pending_transfer(raised.value.pending_transfer_id) == "COMPLETED"
    assert all(
        token.released_states == [TerminalTransferState.COMPLETED]
        for token in _all_tokens(source_guards, target_guards)
    )


def test_known_native_failure_releases_guards_after_failed_drain() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_guards = allocation_guards(target)
    engine = FakeTransferEngine()
    ticket = FakeBatchTransferTicket(["FAILED_DRAINED"])
    engine.batch_transfer_sync_write_with_ticket = lambda *args: ticket

    with pytest.raises(TransferCompletionFailedError, match="target-t0:12345"):
        MooncakeTransferEngineSink(engine).execute(
            plan_transfer(source, target),
            source.placement,
            source.bindings[0],
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
            source_allocation_guards=source_guards,
            target_allocation_guards=target_guards,
        )

    assert all(
        token.released_states == [TerminalTransferState.FAILED_DRAINED]
        for token in _all_tokens(source_guards, target_guards)
    )


def test_native_exception_without_completion_ticket_keeps_guards_quarantined() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_guards = allocation_guards(target)
    engine = FakeTransferEngine()

    def fail_without_ticket(*args, **kwargs):
        raise RuntimeError("native submission raised")

    engine.batch_transfer_sync_write = fail_without_ticket
    sink = MooncakeTransferEngineSink(engine)
    with pytest.raises(TransferCompletionUnknownError) as raised:
        sink.execute(
            plan_transfer(source, target),
            source.placement,
            source.bindings[0],
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
            source_allocation_guards=source_guards,
            target_allocation_guards=target_guards,
        )

    assert (
        sink.pending_transfer_status(raised.value.pending_transfer_id)
        == "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert all(
        token.released_states == []
        for token in _all_tokens(source_guards, target_guards)
    )


@pytest.mark.parametrize("executor_name", ("sink", "reader"))
def test_guard_release_failure_is_quarantined_and_fences_engine(
    executor_name: str,
) -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_binding = target.bindings[0]
    failing_guard = ReleaseFailingAllocationGuard(target_binding)
    target_guards = {
        (target_binding.instance_id, target_binding.participant_id): failing_guard
    }
    engine = FakeTransferEngine()

    if executor_name == "sink":
        executor = MooncakeTransferEngineSink(engine)

        def execute() -> None:
            execute_sink(
                executor,
                plan_transfer(source, target),
                source,
                target,
                target_registrations=registration_leases(target),
                source_allocation_guards=source_guards,
                target_allocation_guards=target_guards,
            )

    else:
        executor = MooncakeTransferEngineReader(engine)

        def execute() -> None:
            execute_reader(
                executor,
                plan_transfer_to_local_target(source, target),
                source,
                target,
                source_registrations=registration_leases(source),
                source_allocation_guards=source_guards,
                target_allocation_guards=target_guards,
            )

    with pytest.raises(TransferRegistrationCleanupPendingError) as raised:
        execute()

    pending_id = raised.value.pending_transfer_id
    assert executor.pending_transfer_ids() == (pending_id,)
    assert (
        executor.pending_transfer_status(pending_id)
        == "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert failing_guard.tokens[0].released_states == [TerminalTransferState.COMPLETED]
    assert all(token.released_states == [] for token in _all_tokens(source_guards))
    with pytest.raises(TransferEngineError, match="restart-required"):
        execute()


@pytest.mark.parametrize("executor_name", ("sink", "reader"))
def test_guard_rejection_preserves_every_acquired_token(
    executor_name: str,
) -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_binding = source.bindings[0]
    target_binding = target.bindings[0]
    source_guard = FixedTokenIdAllocationGuard(
        source_binding,
        token_id="duplicate-token-id",
        fail_release=False,
    )
    target_guard = FixedTokenIdAllocationGuard(
        target_binding,
        token_id="duplicate-token-id",
        fail_release=True,
    )
    source_guards = {
        (source_binding.instance_id, source_binding.participant_id): source_guard
    }
    target_guards = {
        (target_binding.instance_id, target_binding.participant_id): target_guard
    }
    engine = FakeTransferEngine()

    if executor_name == "sink":
        executor = MooncakeTransferEngineSink(engine)

        def execute() -> None:
            execute_sink(
                executor,
                plan_transfer(source, target),
                source,
                target,
                target_registrations=registration_leases(target),
                source_allocation_guards=source_guards,
                target_allocation_guards=target_guards,
            )

    else:
        executor = MooncakeTransferEngineReader(engine)

        def execute() -> None:
            execute_reader(
                executor,
                plan_transfer_to_local_target(source, target),
                source,
                target,
                source_registrations=registration_leases(source),
                source_allocation_guards=source_guards,
                target_allocation_guards=target_guards,
            )

    with pytest.raises(TransferRegistrationCleanupPendingError):
        execute()

    assert target_guard.tokens[0].released_states == [TerminalTransferState.ABORTED]
    assert source_guard.tokens[0].released_states == [TerminalTransferState.ABORTED]
    assert len(executor.pending_transfer_ids()) == 1


def test_acquisition_rollback_failure_retains_every_pin() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    expected = target.bindings[0]
    guard = DriftingReleaseFailingGuard(
        replace(expected, generation=expected.generation + 1)
    )
    source_guards = allocation_guards(source)
    sink = MooncakeTransferEngineSink(FakeTransferEngine())

    with pytest.raises(TransferRegistrationCleanupPendingError):
        sink.execute(
            plan_transfer(source, target),
            source.placement,
            source.bindings[0],
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
            source_allocation_guards=source_guards,
            target_allocation_guards={
                (expected.instance_id, expected.participant_id): guard
            },
        )

    assert guard.tokens[0].released_states == [TerminalTransferState.ABORTED]
    assert all(
        token.released_states == [TerminalTransferState.ABORTED]
        for token in _all_tokens(source_guards)
    )
    assert len(sink.pending_transfer_ids()) == 1


def test_multi_token_acquisition_rollback_finalizes_all_prior_pins() -> None:
    source = manifests(tp=2, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    expected = source.bindings[1]
    failing_guard = DriftingReleaseFailingGuard(
        replace(expected, generation=expected.generation + 1)
    )
    source_guards[(expected.instance_id, expected.participant_id)] = failing_guard
    target_guards = allocation_guards(target)
    reader = MooncakeTransferEngineReader(FakeTransferEngine())

    with pytest.raises(TransferRegistrationCleanupPendingError):
        reader.execute(
            plan_transfer_to_local_target(source, target),
            source.placement,
            source.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=registration_leases(source),
            target_pre_registered=True,
            target_registrations=registration_leases(target),
            source_allocation_guards=source_guards,
            target_allocation_guards=target_guards,
        )

    successful_guard = source_guards[
        (source.bindings[0].instance_id, source.bindings[0].participant_id)
    ]
    assert successful_guard.tokens[0].released_states == [TerminalTransferState.ABORTED]
    assert failing_guard.tokens[0].released_states == [TerminalTransferState.ABORTED]
    assert len(reader.pending_transfer_ids()) == 1


def test_acquisition_duplicate_token_ids_release_each_distinct_pin() -> None:
    source = manifests(tp=2, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = {
        (binding.instance_id, binding.participant_id): FixedTokenIdAllocationGuard(
            binding,
            token_id="duplicate-acquisition-token",
            fail_release=False,
        )
        for binding in source.bindings
    }

    with pytest.raises(ValueError, match="duplicate token_id"):
        MooncakeTransferEngineReader(FakeTransferEngine()).execute(
            plan_transfer_to_local_target(source, target),
            source.placement,
            source.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=registration_leases(source),
            target_pre_registered=True,
            target_registrations=registration_leases(target),
            source_allocation_guards=source_guards,
            target_allocation_guards=allocation_guards(target),
        )

    assert all(
        guard.tokens[0].released_states == [TerminalTransferState.ABORTED]
        for guard in source_guards.values()
    )


@pytest.mark.parametrize("executor_name", ("sink", "reader"))
def test_interruption_after_physical_io_reports_failed_drained(
    executor_name: str,
) -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_guards = allocation_guards(target)
    engine = FakeTransferEngine()

    if executor_name == "sink":
        executor = MooncakeTransferEngineSink(engine)
        original_transfer = executor._transfer_batch

        def interrupt_after_transfer(*args, **kwargs):
            original_transfer(*args, **kwargs)
            raise KeyboardInterrupt("interrupted after physical I/O")

        executor._transfer_batch = interrupt_after_transfer

        def execute() -> None:
            execute_sink(
                executor,
                plan_transfer(source, target),
                source,
                target,
                target_registrations=registration_leases(target),
                source_allocation_guards=source_guards,
                target_allocation_guards=target_guards,
            )

    else:
        executor = MooncakeTransferEngineReader(engine)
        original_transfer = executor._transfer_batch

        def interrupt_after_transfer(*args, **kwargs):
            original_transfer(*args, **kwargs)
            raise KeyboardInterrupt("interrupted after physical I/O")

        executor._transfer_batch = interrupt_after_transfer

        def execute() -> None:
            execute_reader(
                executor,
                plan_transfer_to_local_target(source, target),
                source,
                target,
                source_registrations=registration_leases(source),
                source_allocation_guards=source_guards,
                target_allocation_guards=target_guards,
            )

    with pytest.raises(KeyboardInterrupt, match="after physical I/O"):
        execute()

    assert engine.calls
    assert all(
        token.released_states == [TerminalTransferState.FAILED_DRAINED]
        for token in _all_tokens(source_guards, target_guards)
    )
