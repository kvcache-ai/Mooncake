from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from mooncake.weight_transfer import RuntimeManifest, WeightStore


def _golden_inventory() -> dict:
    return json.loads(
        Path(
            "mooncake-wheel/tests/fixtures/qwen3_5_moe_runtime_manifest.json"
        ).read_text()
    )


def test_sglang_qwen_runtime_inventory_contract_is_parsed_without_sglang() -> None:
    manifest = RuntimeManifest.from_runtime_inventory(_golden_inventory())

    assert manifest.model_id == "qwen3.5-moe"
    assert manifest.revision == "step-42"
    assert manifest.lease_id == "<runtime-lease>"
    assert len(manifest.tensors) == 4
    assert len(manifest.fragments) == 4
    assert {fragment.rank.pp for fragment in manifest.fragments} == {1}
    assert {fragment.rank.ep for fragment in manifest.fragments} == {2}
    assert {tensor.expert_id for tensor in manifest.tensors} == {4, 5}


def test_sglang_tp4_ep4_inventory_becomes_source_fragment_upload_plan() -> None:
    base = RuntimeManifest.from_runtime_inventory(_golden_inventory())
    manifests = []
    for tp_rank in range(4):
        worker_id = f"source-p1-e2-t{tp_rank}"
        fragments = tuple(
            replace(
                fragment,
                fragment_id=f"{fragment.fragment_id}-tp{tp_rank}",
                global_offset=(tp_rank * 2, 0),
                address=fragment.address + tp_rank * 0x1000,
                worker_id=worker_id,
                endpoint=f"{worker_id}:12345",
                rank=replace(fragment.rank, tp=tp_rank),
            )
            for fragment in base.fragments
        )
        manifests.append(
            RuntimeManifest(
                model_id=base.model_id,
                revision=base.revision,
                instance_id=worker_id,
                tensors=base.tensors,
                fragments=fragments,
            )
        )

    plan = WeightStore(object()).prepare_upload(manifests)
    expert_five_gate = [
        operation
        for operation in plan.operations
        if operation.source.tensor_id == "layers.10.mlp.experts.5.gate_proj.weight"
    ]

    assert len(plan.operations) == 16
    assert [item.source.global_offset for item in expert_five_gate] == [
        (0, 0),
        (2, 0),
        (4, 0),
        (6, 0),
    ]
    assert all(item.source.rank.ep == 2 for item in expert_five_gate)
    assert all(not hasattr(item.target, "rank") for item in expert_five_gate)
