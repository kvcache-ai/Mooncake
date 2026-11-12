import pytest
import torch
import torch.distributed as dist

from mooncake.mooncake_ep_buffer import Buffer


class _Runtime:
    def __init__(self, disabled=False, fail_update=False, p2p_ready=True):
        self.disabled = disabled
        self.fail_update = fail_update
        self.p2p_ready = p2p_ready
        self.updates = 0

    def update_local_qpns(self):
        self.updates += 1
        if self.fail_update:
            self.disabled = True

    def ibgda_disabled(self):
        return self.disabled

    def p2p_enabled(self):
        return True

    def get_ipc_handle(self):
        return [1]

    def get_mr_info(self):
        return 4096, 1

    def get_local_qpns(self):
        return [1, 2]

    def get_local_lids(self):
        return [1, 1]

    def get_gid(self):
        return 0, 0

    def sync_ibgda_peers(self, *args):
        pass

    def sync_nvlink_ipc_handles(self, remote_handles, active_ranks):
        pass

    def use_fast_path(self):
        return not self.disabled or self.p2p_ready


@pytest.fixture
def buffer(monkeypatch):
    buffer = object.__new__(Buffer)
    buffer.device_type = "cuda"
    buffer._use_fallback = False
    buffer.group = object()
    buffer.group_size = 2
    buffer.rank = 0
    buffer.runtime = _Runtime()
    buffer._bootstrap_device = lambda: torch.device("cpu")
    buffer._active_ranks_list = lambda device: [1, 1]

    def all_to_all(outputs, inputs, group):
        for output, value in zip(outputs, inputs):
            output.copy_(value)

    monkeypatch.setattr(
        dist,
        "all_gather",
        lambda outputs, tensor, group: [output.copy_(tensor) for output in outputs],
    )
    monkeypatch.setattr(dist, "all_to_all", all_to_all)
    return buffer


@pytest.mark.parametrize(
    "local_disabled,peer_disabled",
    [(False, False), (False, True), (True, False), (True, True)],
)
@pytest.mark.parametrize("p2p_enabled", [False, True])
def test_initial_connect_agrees_on_transport_before_metadata(
    monkeypatch, buffer, local_disabled, peer_disabled, p2p_enabled
):
    buffer.runtime.disabled = local_disabled
    buffer._use_fallback = local_disabled
    monkeypatch.setattr(buffer.runtime, "p2p_enabled", lambda: p2p_enabled)
    collectives = []
    all_gather = dist.all_gather

    def gather(outputs, value, group):
        collectives.append(("gather", value.dtype))
        all_gather(outputs, value, group)

    def all_reduce(value, *, op, group):
        assert op == dist.ReduceOp.MIN
        assert group is buffer.group
        collectives.append(("reduce", value.numel()))
        if value.numel() == 2:
            value[0] = min(int(value[0]), int(not peer_disabled))
            value[1] = min(int(value[1]), int(peer_disabled))

    monkeypatch.setattr(dist, "all_gather", gather)
    monkeypatch.setattr(dist, "all_reduce", all_reduce)

    buffer.connect()

    mixed = local_disabled != peer_disabled
    assert collectives[:1] == [("reduce", 2)]
    assert (("gather", torch.int64) in collectives) is (
        not local_disabled and not peer_disabled
    )
    assert buffer._use_fallback is (mixed or (local_disabled and not p2p_enabled))
    assert buffer.runtime.updates == 0


@pytest.mark.parametrize(
    "already_fallback,disabled,fail_update,peer_failure_phase,p2p_ready,"
    "peer_p2p_failure,mixed_ibgda,expected_updates,expected_fallback",
    [
        (False, False, False, 1, True, False, False, 1, True),
        (False, False, True, None, True, False, False, 1, True),
        (False, False, False, 0, True, False, True, 0, True),
        (True, False, False, None, True, False, False, 0, True),
        (False, True, False, None, True, False, False, 0, False),
        (False, True, False, None, False, False, False, 0, True),
        (False, True, False, None, True, True, False, 0, True),
        (False, True, False, None, True, False, True, 0, True),
        (True, True, False, None, True, False, False, 0, False),
    ],
    ids="peer-qp local-qp peer-off fallback p2p p2p-off peer-ipc mixed retry".split(),
)
def test_recovery_selects_a_group_wide_usable_path(
    monkeypatch,
    buffer,
    already_fallback,
    disabled,
    fail_update,
    peer_failure_phase,
    p2p_ready,
    peer_p2p_failure,
    mixed_ibgda,
    expected_updates,
    expected_fallback,
):
    buffer._use_fallback = already_fallback
    buffer.runtime = _Runtime(
        disabled=disabled, fail_update=fail_update, p2p_ready=p2p_ready
    )

    reductions = []

    def all_reduce(tensor, *, op, group):
        assert op == dist.ReduceOp.MIN
        assert group is buffer.group
        phase = len(reductions)
        reductions.append(tensor.tolist())
        if phase == 0 and mixed_ibgda and tensor.numel() == 2:
            tensor[1] = 0
        if phase == peer_failure_phase:
            tensor.fill_(0)
        if peer_p2p_failure and phase == 1:
            tensor.fill_(0)

    monkeypatch.setattr(dist, "all_reduce", all_reduce)

    buffer.update_ep_member()

    assert buffer._use_fallback is expected_fallback
    assert buffer.runtime.updates == expected_updates


@pytest.mark.parametrize("p2p_enabled", [False, True])
@pytest.mark.parametrize(
    "local_failure,peer_failure", [(True, False), (False, True), (False, False)]
)
def test_peer_sync_failure_selects_fallback(
    monkeypatch, buffer, p2p_enabled, local_failure, peer_failure
):
    monkeypatch.setattr(buffer.runtime, "p2p_enabled", lambda: p2p_enabled)

    def sync_peers(*args):
        buffer.runtime.disabled = local_failure

    monkeypatch.setattr(buffer.runtime, "sync_ibgda_peers", sync_peers)
    reductions = []

    def all_reduce(value, *, op, group):
        assert op == dist.ReduceOp.MIN
        assert group is buffer.group
        phase = len(reductions)
        reductions.append(value.tolist())
        if peer_failure and phase == 2:
            value.fill_(0)

    monkeypatch.setattr(dist, "all_reduce", all_reduce)

    buffer.update_ep_member()

    assert buffer._use_fallback is (local_failure or peer_failure)
