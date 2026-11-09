import pytest
import torch
import torch.distributed as dist

from mooncake.mooncake_ep_buffer import Buffer


class _Runtime:
    def __init__(self, disabled=False, fail_update=False):
        self.disabled = disabled
        self.fail_update = fail_update
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

    def sync_nvlink_ipc_handles(self, remote_handles, active_ranks):
        pass

    def use_fast_path(self):
        return True


@pytest.mark.parametrize(
    "already_fallback,disabled,fail_update,peer_failure_phase,expected_updates",
    [
        (False, False, False, 1, 1),  # Peer fails after QP update.
        (False, False, True, None, 1),  # Local QP update fails.
        (False, False, False, 0, 0),  # Peer is disabled before QP update.
        (True, False, False, None, 0),  # Earlier fallback stays coordinated.
        (False, True, False, None, 0),  # Local IBGDA is already disabled.
    ],
)
def test_coordinated_ibgda_failure_keeps_all_ranks_on_fallback(
    monkeypatch,
    already_fallback,
    disabled,
    fail_update,
    peer_failure_phase,
    expected_updates,
):
    buffer = object.__new__(Buffer)
    buffer.device_type = "cuda"
    buffer._use_fallback = already_fallback
    buffer.group = object()
    buffer.group_size = 2
    buffer.rank = 0
    buffer.runtime = _Runtime(disabled=disabled, fail_update=fail_update)
    buffer._bootstrap_device = lambda: torch.device("cpu")
    buffer._active_ranks_list = lambda device: [1, 1]

    reductions = []

    def all_reduce(tensor, *, op, group):
        assert op == dist.ReduceOp.MIN
        assert group is buffer.group
        phase = len(reductions)
        reductions.append(tensor.item())
        if phase == peer_failure_phase:
            tensor.fill_(0)

    monkeypatch.setattr(dist, "all_reduce", all_reduce)
    monkeypatch.setattr(
        dist,
        "all_gather",
        lambda outputs, tensor, group: [output.copy_(tensor) for output in outputs],
    )

    buffer.connect(is_update=True)

    assert buffer._use_fallback is True
    assert buffer.runtime.updates == expected_updates
    # IPC still advertises a fast path; coordinated failure must veto it.
    assert reductions[-1] == 0
    assert len(reductions) == (3 if expected_updates else 2)
