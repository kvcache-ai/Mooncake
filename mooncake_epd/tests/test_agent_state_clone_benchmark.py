from __future__ import annotations

from types import SimpleNamespace

import torch

from mooncake_epd.scripts.run_agent_state_clone_benchmark import (
    _synchronize_for_timing,
    write_agent_clone_raw_artifacts,
)


def test_agent_clone_benchmark_synchronizes_cuda_timing_boundaries(monkeypatch):
    calls = []
    monkeypatch.setattr(torch.cuda, "synchronize", lambda device: calls.append(device))

    _synchronize_for_timing(torch.device("cpu"))
    _synchronize_for_timing(torch.device("cuda:3"))

    assert calls == [torch.device("cuda:3")]


def test_agent_clone_benchmark_writes_redacted_replay_evidence(tmp_path):
    args = SimpleNamespace(
        device="cpu",
        dtype="float32",
        node_id="node-a",
        workflow_id="wf-clone",
        branches=8,
        pages=64,
        page_size=16,
        num_layers=32,
        num_kv_heads=8,
        head_dim=128,
    )
    summary = {
        "speedup_avg": 3.2,
        "zero_copy_clone_bytes": 0,
        "store_stats_after_release": {"states": 0},
        "page_manager_stats_after_release": {"directory_orphans": 0},
    }

    raw = write_agent_clone_raw_artifacts(
        summary=summary,
        args=args,
        output=tmp_path / "clone.json",
    )

    assert all((tmp_path / "clone_evidence" / "raw_artifacts" / name).exists() for name in (
        "requests.jsonl",
        "responses.jsonl",
        "service_logs.json",
        "metrics.json",
        "environment.json",
        "hardware.json",
    ))
    assert set(raw) == {"requests_jsonl", "responses_jsonl", "service_logs", "metrics", "environment", "hardware"}
