# Copyright 2026 KVCache.AI

from __future__ import annotations

import inspect

import pytest
import torch.distributed as dist

from mooncake.mooncake_elastic_buffer import (
    ElasticBuffer,
    _requested_transport,
    _resolve_transport_consensus,
    _select_transport,
    _select_transport_for_group,
)


def test_public_constructor_defaults_are_backward_compatible() -> None:
    parameters = inspect.signature(ElasticBuffer).parameters
    assert parameters["transport"].default == "auto"
    assert parameters["explicitly_destroy"].default is False


@pytest.mark.parametrize(
    ("num_ranks", "num_rdma_ranks", "num_nvlink_ranks"),
    [
        (2, 1, 2),
        (8, 1, 8),
        (8, 2, 4),
        (16, 2, 8),
        (16, 4, 4),
    ],
)
def test_auto_prefers_nccl_for_compiled_topologies(
    num_ranks: int, num_rdma_ranks: int, num_nvlink_ranks: int
) -> None:
    assert (
        _select_transport(
            "auto",
            True,
            num_ranks,
            num_rdma_ranks,
            num_nvlink_ranks,
            True,
        )
        == "nccl"
    )


@pytest.mark.parametrize(
    ("nccl_available", "num_ranks", "num_rdma_ranks", "num_nvlink_ranks"),
    [
        (False, 8, 1, 8),
        (True, 1, 1, 1),
        (True, 4, 1, 4),
        (True, 32, 4, 8),
        (True, 10, 2, 4),
    ],
)
def test_auto_falls_back_to_ibgda_when_nccl_cannot_run(
    nccl_available: bool,
    num_ranks: int,
    num_rdma_ranks: int,
    num_nvlink_ranks: int,
) -> None:
    assert (
        _select_transport(
            "auto",
            nccl_available,
            num_ranks,
            num_rdma_ranks,
            num_nvlink_ranks,
            True,
        )
        == "ibgda"
    )


@pytest.mark.parametrize("requested", ["ibgda", "nccl"])
def test_explicit_transport_is_never_rewritten(requested: str) -> None:
    assert _select_transport(requested, False, 4, 1, 4, True) == requested


def test_auto_falls_back_without_required_hybrid_mode() -> None:
    assert _select_transport("auto", True, 8, 2, 4, False) == "ibgda"


def test_torchrun_local_world_size_drives_topology(monkeypatch) -> None:
    class Group:
        @staticmethod
        def size() -> int:
            return 16

    monkeypatch.delenv("MOONCAKE_EP_NUM_LOCAL_RANKS", raising=False)
    monkeypatch.setenv("LOCAL_WORLD_SIZE", "4")
    assert ElasticBuffer._calculate_physical_domain_size(Group()) == (4, 4)


def test_environment_can_roll_auto_mode_back_to_ibgda(monkeypatch) -> None:
    monkeypatch.setenv("MOONCAKE_EP_TRANSPORT", "IBGDA")
    assert _requested_transport("auto") == "ibgda"
    assert _requested_transport("nccl") == "nccl"


def test_invalid_transport_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("MOONCAKE_EP_TRANSPORT", "tcp")
    with pytest.raises(ValueError, match="transport must be one of"):
        _requested_transport("auto")


def test_group_transport_selection_uses_every_rank_capability(monkeypatch) -> None:
    class Group:
        @staticmethod
        def size() -> int:
            return 2

    test_group = Group()
    monkeypatch.setattr(dist, "get_backend", lambda _: "gloo")

    def fake_all_gather(gathered_states, local_state, group=None) -> None:
        assert group is test_group
        for state in gathered_states:
            state.copy_(local_state)
        # Simulate rank 1 having the same request but no usable NCCL backend.
        gathered_states[1][1] = 0

    monkeypatch.setattr(dist, "all_gather", fake_all_gather)
    assert (
        _select_transport_for_group(test_group, "auto", True, 2, 1, 2, True) == "ibgda"
    )


def test_transport_consensus_selects_nccl_when_every_rank_is_ready() -> None:
    assert _resolve_transport_consensus([("auto", True), ("auto", True)]) == "nccl"


@pytest.mark.parametrize(
    "rank_states",
    [
        [("auto", True), ("auto", False)],
        [("auto", False), ("auto", True)],
    ],
)
def test_transport_consensus_falls_back_to_ibgda_group_wide(
    rank_states: list[tuple[str, bool]],
) -> None:
    assert _resolve_transport_consensus(rank_states) == "ibgda"


def test_transport_consensus_rejects_inconsistent_requests() -> None:
    with pytest.raises(RuntimeError, match="requests differ"):
        _resolve_transport_consensus([("auto", True), ("ibgda", True)])


def test_transport_consensus_rejects_explicit_nccl_when_a_rank_is_unready() -> None:
    with pytest.raises(RuntimeError, match="requirements are not met on ranks: 1"):
        _resolve_transport_consensus([("nccl", True), ("nccl", False)])
