from types import SimpleNamespace

import pytest
import torch
import torch.distributed as dist

import mooncake
from mooncake.mooncake_elastic_buffer import ElasticBuffer


class _Runtime:
    def __init__(self, disabled: bool, fail_update: bool) -> None:
        self.disabled = disabled
        self.fail_update = fail_update
        self.updates = 0
        self.peer_connections = 0
        self.ipc_connections = 0

    def ibgda_disabled(self) -> bool:
        return self.disabled

    def update_local_qpns(self) -> None:
        self.updates += 1
        if self.fail_update:
            self.disabled = True

    def get_mr_info(self) -> tuple[int, int]:
        return 4096, 1

    def get_local_qpns(self) -> list[int]:
        return [1, 2]

    def get_local_lids(self) -> list[int]:
        return [1, 1]

    def get_gid(self) -> tuple[int, int]:
        return 0, 0

    def sync_ibgda_peers(self, *args) -> None:
        self.peer_connections += 1

    def get_ipc_handle(self) -> list[int]:
        return [1]

    def sync_nvlink_ipc_handles(self, handles, active_ranks) -> None:
        self.ipc_connections += 1


@pytest.fixture
def make_buffer(monkeypatch):
    monkeypatch.setattr(mooncake, "ep", SimpleNamespace(MAX_QP_COUNT=2), raising=False)
    monkeypatch.setattr(dist, "get_backend", lambda group: "gloo")
    tensor = torch.tensor
    empty = torch.empty

    def cpu_tensor(data, **kwargs):
        return tensor(data, **{**kwargs, "device": "cpu"})

    def cpu_empty(*shape, **kwargs):
        return empty(*shape, **{**kwargs, "device": "cpu"})

    def all_gather(outputs, value, group):
        for output in outputs:
            output.copy_(value)

    def all_to_all(outputs, inputs, group):
        for output, value in zip(outputs, inputs):
            output.copy_(value)

    monkeypatch.setattr(torch, "tensor", cpu_tensor)
    monkeypatch.setattr(torch, "empty", cpu_empty)
    monkeypatch.setattr(dist, "all_gather", all_gather)
    monkeypatch.setattr(dist, "all_to_all", all_to_all)

    def create(disabled=False, fail_update=False, peer_failure=False):
        buffer = object.__new__(ElasticBuffer)
        buffer.transport = "ibgda"
        buffer._store_collectives = None
        buffer.group = object()
        buffer.num_ranks = 2
        buffer.rank_idx = 0
        buffer.runtime = _Runtime(disabled, fail_update)
        buffer._started_with_ibgda = not disabled
        buffer._active_ranks_mask = lambda: [1, 1]

        def all_reduce(value, *, op, group):
            assert op == dist.ReduceOp.MAX
            assert group is buffer.group
            if peer_failure and buffer.runtime.updates:
                value.fill_(1)

        monkeypatch.setattr(dist, "all_reduce", all_reduce)
        return buffer

    return create


def test_peer_qp_update_failure_is_reported_locally(make_buffer):
    buffer = make_buffer(peer_failure=True)

    with pytest.raises(RuntimeError, match="IBGDA.*refreshing local QPs"):
        buffer.update_ep_member()

    assert buffer.runtime.ipc_connections == 0


@pytest.mark.parametrize("use_store", [False, True])
@pytest.mark.parametrize("fail_qp_update", [False, True])
def test_all_rank_failure_cannot_become_p2p_only_on_retry(
    make_buffer, monkeypatch, use_store, fail_qp_update
):
    buffer = make_buffer(fail_update=fail_qp_update)
    if use_store:
        buffer._store_collectives = SimpleNamespace(
            all_gather=lambda scope, value: [value, value]
        )
    if not fail_qp_update:

        def fail_peer_sync(*args):
            buffer.runtime.disabled = True

        monkeypatch.setattr(buffer.runtime, "sync_ibgda_peers", fail_peer_sync)

    stage = (
        "refreshing local QPs" if fail_qp_update else "peer synchronization disabled"
    )
    with pytest.raises(RuntimeError, match=f"IBGDA.*{stage}"):
        buffer.update_ep_member()

    with pytest.raises(RuntimeError, match="IBGDA.*refreshing local QPs"):
        buffer.update_ep_member()

    assert buffer.runtime.ipc_connections == 0


def test_store_rejects_a_peer_with_runtime_ibgda_failure(make_buffer):
    buffer = make_buffer(disabled=True)

    def all_gather(scope, value):
        peer = (*value[:5], True)
        return [value, peer]

    buffer._store_collectives = SimpleNamespace(all_gather=all_gather)

    with pytest.raises(RuntimeError, match="IBGDA"):
        buffer.update_ep_member()

    assert buffer.runtime.ipc_connections == 0


@pytest.mark.parametrize("use_store", [False, True])
def test_existing_p2p_only_transport_can_refresh_ipc(make_buffer, use_store):
    buffer = make_buffer(disabled=True)
    if use_store:
        buffer._store_collectives = SimpleNamespace(
            all_gather=lambda scope, value: [value, value]
        )

    buffer.update_ep_member()

    assert buffer.runtime.updates == 0
    assert buffer.runtime.ipc_connections == 1


def test_successful_qp_update_reconnects_peers(make_buffer):
    buffer = make_buffer()

    buffer.update_ep_member()

    assert buffer.runtime.updates == 1
    assert buffer.runtime.peer_connections == 1
    assert buffer.runtime.ipc_connections == 1


def test_remote_peer_sync_failure_is_reported_before_ipc(make_buffer, monkeypatch):
    buffer = make_buffer()

    def all_reduce(value, *, op, group):
        assert op == dist.ReduceOp.MAX
        if buffer.runtime.peer_connections:
            value.fill_(1)

    monkeypatch.setattr(dist, "all_reduce", all_reduce)

    with pytest.raises(RuntimeError, match="IBGDA peer synchronization disabled"):
        buffer.update_ep_member()

    assert buffer.runtime.ipc_connections == 0


@pytest.mark.parametrize("disabled", [False, True])
def test_mixed_readiness_is_rejected_before_metadata_exchange(
    make_buffer, monkeypatch, disabled
):
    buffer = make_buffer(disabled=disabled)

    def all_reduce(value, *, op, group):
        assert op == dist.ReduceOp.MAX
        # The other rank has the opposite readiness. Both indicators are set.
        if value.numel() == 2:
            value.fill_(1)

    monkeypatch.setattr(dist, "all_reduce", all_reduce)

    with pytest.raises(RuntimeError, match="disagree on IBGDA readiness"):
        buffer.update_ep_member()

    assert buffer.runtime.updates == 0
    assert buffer.runtime.peer_connections == 0
    assert buffer.runtime.ipc_connections == 0
