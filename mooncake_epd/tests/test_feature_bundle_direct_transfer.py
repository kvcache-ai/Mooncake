from __future__ import annotations

import pytest
import torch

from mooncake_epd.core.state import FeatureBundle
from mooncake_epd.core.transfer import TransferEngine


class _FakeMooncakeEngine:
    def __init__(self):
        self.calls = []
        self.registered = []
        self.unregistered = []

    def register_memory(self, pointer, length):
        self.registered.append((int(pointer), int(length)))
        return 0

    def unregister_memory(self, pointer):
        self.unregistered.append(int(pointer))
        return 0

    def batch_transfer_sync_write(self, remote_session, local_ptrs, remote_ptrs, lengths):
        self.calls.append((remote_session, list(local_ptrs), list(remote_ptrs), list(lengths)))
        return 0

    def transfer_sync_write(self, remote_session, local_ptr, remote_ptr, length):
        self.calls.append((remote_session, [int(local_ptr)], [int(remote_ptr)], [int(length)]))
        return 0


def _bundle() -> FeatureBundle:
    return FeatureBundle(
        image_hash="img-direct",
        last_hidden=torch.ones((2, 4), dtype=torch.float32),
        intermediates=[(1, torch.full((2, 4), 2.0, dtype=torch.float32))],
        grid_thw=torch.tensor([[1, 2, 1]], dtype=torch.int64),
    )


def test_feature_bundle_peer_buffer_plan_validates_complete_targets():
    engine = TransferEngine(protocol="tcp")
    bundle = _bundle()
    items = dict(engine.feature_bundle_tensor_items(bundle))

    with pytest.raises(ValueError, match="missing FeatureBundle peer-buffer targets"):
        engine.build_feature_bundle_peer_buffer_plan(
            bundle,
            remote_session="prefill-session",
            remote_pointers={"last_hidden": 1000},
        )

    remote = {name: 10_000 + idx * 1024 for idx, name in enumerate(items)}
    remote.update({f"{name}:nbytes": tensor.nelement() * tensor.element_size() for name, tensor in items.items()})
    plan = engine.build_feature_bundle_peer_buffer_plan(
        bundle,
        remote_session="prefill-session",
        remote_pointers=remote,
        checksum=True,
    )

    assert plan.feature_id == "img-direct"
    assert plan.remote_session == "prefill-session"
    assert [target.name for target in plan.targets] == ["last_hidden", "grid_thw", "intermediate:1:0"]
    assert plan.descriptor["feature_id"] == "img-direct"


def test_feature_bundle_peer_buffer_transfer_uses_bound_direct_engine():
    engine = TransferEngine(protocol="tcp")
    fake = _FakeMooncakeEngine()
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)
    bundle = _bundle()
    remote = {
        name: 20_000 + idx * 4096
        for idx, (name, _) in enumerate(engine.feature_bundle_tensor_items(bundle))
    }
    plan = engine.build_feature_bundle_peer_buffer_plan(
        bundle,
        remote_session="prefill-session",
        remote_pointers=remote,
    )

    result = engine.transfer_feature_bundle_peer_buffer_plan(bundle, plan)

    assert result.backend_label == "feature_peer_buffer_direct"
    assert result.tensor_count == 3
    assert result.descriptor_count == 3
    assert result.nbytes == sum(target.nbytes for target in plan.targets)
    assert fake.calls
    remote_session, local_ptrs, remote_ptrs, lengths = fake.calls[0]
    assert remote_session == "prefill-session"
    assert len(local_ptrs) == len(remote_ptrs) == len(lengths) == 3
    assert len(fake.registered) == 3
    assert fake.unregistered == [pointer for pointer, _ in fake.registered]
    snap = engine.stats.snapshot()
    assert snap["encoder_to_prefill_peer_buffer_direct"]["transfers"] == 1


def test_feature_bundle_peer_buffer_batch_coalesces_multimodal_write():
    """Several images for one Prefill worker issue one Mooncake write."""

    engine = TransferEngine(protocol="tcp")
    fake = _FakeMooncakeEngine()
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)
    first = _bundle()
    second = FeatureBundle(
        image_hash="img-direct-second",
        last_hidden=torch.full((2, 4), 3.0, dtype=torch.float32),
        intermediates=[(1, torch.full((2, 4), 4.0, dtype=torch.float32))],
        grid_thw=torch.tensor([[1, 2, 1]], dtype=torch.int64),
    )
    bundles = [first, second]
    plans = []
    for bundle_index, bundle in enumerate(bundles):
        remote = {
            name: 40_000 + bundle_index * 16_384 + tensor_index * 4096
            for tensor_index, (name, _tensor) in enumerate(
                engine.feature_bundle_tensor_items(bundle)
            )
        }
        plans.append(
            engine.build_feature_bundle_peer_buffer_plan(
                bundle,
                remote_session="prefill-session",
                remote_pointers=remote,
            )
        )

    results = engine.transfer_feature_bundle_peer_buffer_plans(bundles, plans)

    assert [result.feature_id for result in results] == [
        "img-direct",
        "img-direct-second",
    ]
    assert len(fake.calls) == 1
    remote_session, local_ptrs, remote_ptrs, lengths = fake.calls[0]
    assert remote_session == "prefill-session"
    assert len(local_ptrs) == len(remote_ptrs) == len(lengths) == 6
    assert results[0].nbytes == sum(target.nbytes for target in plans[0].targets)
    assert results[1].nbytes == sum(target.nbytes for target in plans[1].targets)
    assert results[0].timings_ms["batch_feature_count"] == 2.0
    assert results[1].timings_ms["batch_index"] == 1.0
    snap = engine.stats.snapshot()["encoder_to_prefill_peer_buffer_direct"]
    assert snap["transfers"] == 1
    assert snap["total_bytes"] == sum(result.nbytes for result in results)


def test_feature_bundle_peer_buffer_batch_rejects_mixed_prefill_sessions():
    engine = TransferEngine(protocol="tcp")
    first = _bundle()
    second = FeatureBundle(
        image_hash="img-direct-second",
        last_hidden=torch.ones((2, 4), dtype=torch.float32),
    )
    first_plan = engine.build_feature_bundle_peer_buffer_plan(
        first,
        remote_session="prefill-a",
        remote_pointers={
            name: 50_000 + index * 4096
            for index, (name, _tensor) in enumerate(engine.feature_bundle_tensor_items(first))
        },
    )
    second_plan = engine.build_feature_bundle_peer_buffer_plan(
        second,
        remote_session="prefill-b",
        remote_pointers={"last_hidden": 90_000},
    )

    with pytest.raises(ValueError, match="requires one remote_session"):
        engine.transfer_feature_bundle_peer_buffer_plans(
            [first, second],
            [first_plan, second_plan],
        )
