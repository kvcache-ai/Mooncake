# Copyright 2026 KVCache.AI

from __future__ import annotations

import inspect

import pytest

from mooncake.mooncake_elastic_buffer import (
    ElasticBuffer,
    _requested_transport,
    _select_transport,
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
