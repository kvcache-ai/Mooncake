# Copyright 2026 KVCache.AI

from __future__ import annotations

import inspect
import sys
import types

import pytest
import torch
import torch.distributed as dist

from mooncake.mooncake_elastic_buffer import (
    ElasticBuffer,
    _bootstrap_collective_device,
    _requested_transport,
    _resolve_nccl_membership_masks,
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


@pytest.mark.parametrize("backend_name", ["gloo", "mooncake-cpu"])
def test_group_transport_selection_uses_every_rank_capability(
    monkeypatch, backend_name: str
) -> None:
    class Group:
        @staticmethod
        def size() -> int:
            return 2

    test_group = Group()
    monkeypatch.setattr(dist, "get_backend", lambda _: backend_name)

    def fake_all_gather(gathered_states, local_state, group=None) -> None:
        assert group is test_group
        assert local_state.device.type == "cpu"
        for state in gathered_states:
            state.copy_(local_state)
        # Simulate rank 1 having the same request but no usable NCCL backend.
        gathered_states[1][1] = 0

    monkeypatch.setattr(dist, "all_gather", fake_all_gather)
    assert (
        _select_transport_for_group(test_group, "auto", True, 2, 1, 2, True) == "ibgda"
    )


@pytest.mark.parametrize("rank_world_sizes", [[2, 2, 8, 8, 8, 8, 8, 8], [8, 8]])
def test_nccl_consensus_rejects_changed_world_size_on_every_rank(
    monkeypatch, rank_world_sizes: list[int]
) -> None:
    class Group:
        @staticmethod
        def size() -> int:
            return len(rank_world_sizes)

    test_group = Group()
    monkeypatch.setattr(dist, "get_backend", lambda _: "gloo")

    def fake_all_gather(gathered_states, local_state, group=None) -> None:
        assert group is test_group
        assert len(gathered_states) == test_group.size()
        assert local_state.numel() == 4
        for state, world_size in zip(gathered_states, rank_world_sizes):
            # Survivors require NCCL; newly constructed buffers request auto.
            required_code = 2 if world_size != test_group.size() else 0
            state.copy_(
                torch.tensor([0, 1, required_code, world_size], dtype=torch.int32)
            )

    monkeypatch.setattr(dist, "all_gather", fake_all_gather)
    for world_size in rank_world_sizes:
        with pytest.raises(RuntimeError, match="fixed logical world size"):
            _select_transport_for_group(
                test_group,
                "auto",
                True,
                world_size,
                1,
                world_size,
                True,
                required_transport="nccl" if world_size != test_group.size() else None,
            )


def test_nccl_consensus_rejects_missing_rank_state(monkeypatch) -> None:
    class Group:
        @staticmethod
        def size() -> int:
            return 2

    test_group = Group()
    monkeypatch.setattr(dist, "get_backend", lambda _: "gloo")

    def fake_all_gather(gathered_states, local_state, group=None) -> None:
        # Mooncake PG can leave inactive ranks' output slots untouched.
        gathered_states[0].copy_(local_state)

    monkeypatch.setattr(dist, "all_gather", fake_all_gather)
    with pytest.raises(RuntimeError, match="requirements are not met on ranks: 1"):
        _select_transport_for_group(test_group, "nccl", True, 2, 1, 2, True)


def test_transport_consensus_selects_nccl_when_every_rank_is_ready() -> None:
    assert (
        _resolve_transport_consensus([("auto", True, None), ("auto", True, None)])
        == "nccl"
    )


@pytest.mark.parametrize(
    "rank_states",
    [
        [("auto", True, None), ("auto", False, None)],
        [("auto", False, None), ("auto", True, None)],
    ],
)
def test_transport_consensus_falls_back_to_ibgda_group_wide(
    rank_states: list[tuple[str, bool, str | None]],
) -> None:
    assert _resolve_transport_consensus(rank_states) == "ibgda"


def test_transport_consensus_rejects_inconsistent_requests() -> None:
    with pytest.raises(RuntimeError, match="requests differ"):
        _resolve_transport_consensus([("auto", True, None), ("ibgda", True, None)])


def test_transport_consensus_rejects_explicit_nccl_when_a_rank_is_unready() -> None:
    with pytest.raises(RuntimeError, match="requirements are not met on ranks: 1"):
        _resolve_transport_consensus([("nccl", True, None), ("nccl", False, None)])


def test_transport_consensus_preserves_existing_nccl_generation() -> None:
    assert (
        _resolve_transport_consensus([("auto", True, "nccl"), ("auto", True, None)])
        == "nccl"
    )


def test_existing_nccl_generation_rejects_an_unready_replacement() -> None:
    with pytest.raises(RuntimeError, match="requirements are not met on ranks: 1"):
        _resolve_transport_consensus([("auto", True, "nccl"), ("auto", False, None)])


def test_transport_consensus_preserves_existing_ibgda_generation() -> None:
    assert (
        _resolve_transport_consensus([("auto", True, "ibgda"), ("auto", True, None)])
        == "ibgda"
    )


def test_nccl_reconfiguration_accepts_consistent_full_membership() -> None:
    _resolve_nccl_membership_masks([[1, 1], [1, 1]])


@pytest.mark.parametrize(
    "rank_masks",
    [
        [[1, 0], [1, 0]],
        [[1, 1], [1, 0]],
        [[1, 1], [1]],
    ],
)
def test_nccl_reconfiguration_rejects_incomplete_membership(
    rank_masks: list[list[int]],
) -> None:
    with pytest.raises(RuntimeError, match="complete fixed logical rank set"):
        _resolve_nccl_membership_masks(rank_masks)


@pytest.mark.parametrize("backend_name", ["gloo", "mooncake-cpu"])
@pytest.mark.parametrize("active_mask", [[1, 1], [1, 1, 0, 0]])
def test_nccl_membership_accepts_reserved_capacity(
    monkeypatch, active_mask, backend_name: str
) -> None:
    buffer = object.__new__(ElasticBuffer)
    buffer.num_ranks = 2
    buffer.group = object()
    monkeypatch.setattr(buffer, "_active_ranks_mask", lambda: active_mask)
    monkeypatch.setattr(dist, "get_backend", lambda _: backend_name)

    def fake_all_gather(gathered_masks, local_mask, group=None) -> None:
        assert group is buffer.group
        assert local_mask.device.type == "cpu"
        assert local_mask.tolist() == [1, 1]
        assert len(gathered_masks) == buffer.num_ranks
        for mask in gathered_masks:
            mask.copy_(local_mask)

    monkeypatch.setattr(dist, "all_gather", fake_all_gather)
    buffer._require_full_nccl_membership()


@pytest.mark.parametrize("active_mask", [[1, 0, 0, 0], [1], []])
def test_nccl_membership_rejects_missing_logical_ranks(
    monkeypatch, active_mask
) -> None:
    buffer = object.__new__(ElasticBuffer)
    buffer.num_ranks = 2
    buffer.group = object()
    monkeypatch.setattr(buffer, "_active_ranks_mask", lambda: active_mask)
    monkeypatch.setattr(dist, "get_backend", lambda _: "gloo")

    def fake_all_gather(gathered_masks, local_mask, group=None) -> None:
        for mask in gathered_masks:
            mask.copy_(local_mask)

    monkeypatch.setattr(dist, "all_gather", fake_all_gather)
    with pytest.raises(RuntimeError, match="complete fixed logical rank set"):
        buffer._require_full_nccl_membership()


def test_nccl_membership_rejects_incomplete_remote_view(monkeypatch) -> None:
    buffer = object.__new__(ElasticBuffer)
    buffer.num_ranks = 2
    buffer.group = object()
    monkeypatch.setattr(buffer, "_active_ranks_mask", lambda: [1, 1, 0, 0])
    monkeypatch.setattr(dist, "get_backend", lambda _: "gloo")

    def fake_all_gather(gathered_masks, local_mask, group=None) -> None:
        gathered_masks[0].copy_(local_mask)
        gathered_masks[1].copy_(torch.tensor([1, 0], dtype=torch.int32))

    monkeypatch.setattr(dist, "all_gather", fake_all_gather)
    with pytest.raises(RuntimeError, match=r"logical slots: \[1\]"):
        buffer._require_full_nccl_membership()


def test_nccl_generation_rejects_stale_handle() -> None:
    class Handle:
        _generation = (1, 2, 3)

    buffer = object.__new__(ElasticBuffer)
    buffer._generation = (4, 5, 6)
    with pytest.raises(RuntimeError, match="obsolete NCCL ElasticBuffer generation"):
        buffer._validate_handle_generation(Handle())


def test_handle_without_generation_is_rejected_by_nccl_generation() -> None:
    class LegacyHandle:
        pass

    buffer = object.__new__(ElasticBuffer)
    buffer._generation = (4, 5, 6)
    with pytest.raises(RuntimeError, match="obsolete NCCL ElasticBuffer generation"):
        buffer._validate_handle_generation(LegacyHandle())


def test_handle_without_generation_remains_valid_for_ibgda() -> None:
    class LegacyHandle:
        pass

    buffer = object.__new__(ElasticBuffer)
    buffer._generation = None
    buffer._validate_handle_generation(LegacyHandle())


@pytest.mark.parametrize("backend_name", ["mooncake", "mooncake-cpu"])
def test_active_rank_mask_uses_registered_mooncake_backend(
    monkeypatch, backend_name: str
) -> None:
    backend = object()
    buffer = object.__new__(ElasticBuffer)
    buffer.backend = backend
    buffer.num_ranks = 2
    buffer._device_index = 3

    fake_ep = types.ModuleType("mooncake.ep")
    fake_ep.get_active_ranks = lambda group: torch.tensor([1, 0])
    monkeypatch.setitem(sys.modules, "mooncake.ep", fake_ep)
    monkeypatch.setattr(dist, "get_backend", lambda group: backend_name)
    synchronized_devices = []
    monkeypatch.setattr(torch.cuda, "synchronize", synchronized_devices.append)

    assert buffer._active_ranks_mask() == [1, 0]
    assert synchronized_devices == [3]


@pytest.mark.parametrize("backend_name", ["gloo", "nccl"])
def test_non_mooncake_group_uses_full_membership(monkeypatch, backend_name) -> None:
    buffer = object.__new__(ElasticBuffer)
    buffer.backend = object()
    buffer.num_ranks = 2
    monkeypatch.setattr(dist, "get_backend", lambda _: backend_name)
    # No native EP/PG helper is available for these process-group backends.
    monkeypatch.setitem(sys.modules, "mooncake.ep", types.ModuleType("mooncake.ep"))
    assert buffer._active_ranks_mask() == [1, 1]


@pytest.mark.parametrize(
    ("backend_name", "device"),
    [("gloo", "cpu"), ("mooncake-cpu", "cpu"), ("mooncake", "cuda"), ("nccl", "cuda")],
)
def test_bootstrap_collective_device(monkeypatch, backend_name, device) -> None:
    monkeypatch.setattr(dist, "get_backend", lambda _: backend_name)
    assert _bootstrap_collective_device(object()) == device


@pytest.mark.parametrize("backend_name", ["gloo", "mooncake-cpu"])
def test_nccl_unique_id_exchange_uses_cpu_metadata(monkeypatch, backend_name) -> None:
    buffer = object.__new__(ElasticBuffer)
    buffer.group = object()
    buffer.rank_idx = 0
    expected_id = list(range(buffer._NCCL_UNIQUE_ID_WORDS))
    fake_ep = types.SimpleNamespace(create_nccl_unique_id=lambda: expected_id)
    monkeypatch.setattr(dist, "get_backend", lambda _: backend_name)
    monkeypatch.setattr(dist, "get_global_rank", lambda group, rank: 0)
    broadcasts = []

    def fake_broadcast(tensor, src, group) -> None:
        assert group is buffer.group
        assert src == 0
        assert tensor.device.type == "cpu"
        broadcasts.append(tensor.tolist())

    monkeypatch.setattr(dist, "broadcast", fake_broadcast)
    assert buffer._exchange_nccl_unique_id(fake_ep) == expected_id
    assert broadcasts == [[0], expected_id]
