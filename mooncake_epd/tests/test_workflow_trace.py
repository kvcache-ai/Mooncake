from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.benchmarks.workflow_trace import build_workflow_traces, export_workflow_traces  # noqa: E402
from mooncake_epd.tests.dataset import build_dataset  # noqa: E402


def test_build_workflow_traces_matches_dataset():
    dataset = build_dataset()
    traces = build_workflow_traces(dataset)
    assert len(traces) == len(dataset)
    assert all(trace.source_dataset == "synthetic" for trace in traces)
    assert all(trace.images for trace in traces)
    assert all(trace.steps for trace in traces)


def test_export_workflow_traces_json(tmp_path):
    out = tmp_path / "workflow_traces.json"
    export_workflow_traces(str(out))
    data = json.loads(out.read_text(encoding="utf-8"))
    assert len(data) == len(build_dataset())
    assert {
        "workflow_id",
        "source_dataset",
        "task_type",
        "priority_class",
        "modality",
        "routing_path",
        "expected_prefetchable_images",
        "images",
        "steps",
        "gold_answer",
        "scoring",
    } <= set(data[0].keys())
