import torch
import torch.distributed as dist

from mooncake.mooncake_ep_buffer import Buffer


class _Runtime:
    def update_local_qpns(self):
        pass

    def ibgda_disabled(self):
        return False

    def p2p_enabled(self):
        return True

    def get_ipc_handle(self):
        return [1]

    def sync_nvlink_ipc_handles(self, remote_handles, active_ranks):
        pass

    def use_fast_path(self):
        return True


def test_coordinated_ibgda_failure_keeps_all_ranks_on_fallback(monkeypatch):
    buffer = object.__new__(Buffer)
    buffer.device_type = "cuda"
    buffer._use_fallback = False
    buffer.group = object()
    buffer.group_size = 2
    buffer.rank = 0
    buffer.runtime = _Runtime()
    buffer._bootstrap_device = lambda: torch.device("cpu")
    buffer._active_ranks_list = lambda device: [1, 1]

    # Simulate another rank reporting a QP-update failure in the group MAX.
    monkeypatch.setattr(dist, "all_reduce", lambda tensor, **kwargs: tensor.fill_(1))
    monkeypatch.setattr(
        dist,
        "all_gather",
        lambda outputs, tensor, group: [output.copy_(tensor) for output in outputs],
    )

    buffer.connect(is_update=True)

    assert buffer._use_fallback is True
