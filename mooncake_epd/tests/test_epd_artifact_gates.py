from __future__ import annotations

import json

from mooncake_epd.scripts.check_epd_artifact_gates import check_serving_summary


def _serving_summary(*, transport_evidence=None):
    summary = {
        "mooncake_protocol": "nvlink_intra",
        "metrics": {
            "metrics": {
                "layered_receive_failures": 0,
                "layered_transfer_failed_batches": 0,
                "fallback_batches": 0,
                "peer_buffer_batches": 1,
                "remote_transfer_backend_counts": {"peer_buffer_direct": 1},
            }
        },
    }
    if transport_evidence is not None:
        summary["transport_runtime_evidence"] = transport_evidence
    return summary


def test_serving_artifact_gate_requires_runtime_native_transport_proof(tmp_path):
    path = tmp_path / "summary.json"
    path.write_text(json.dumps(_serving_summary()), encoding="utf-8")

    failures = check_serving_summary(str(path))

    assert any("transport_runtime_evidence missing" in item for item in failures)


def test_serving_artifact_gate_accepts_matching_runtime_native_transport_proof(tmp_path):
    path = tmp_path / "summary.json"
    path.write_text(
        json.dumps(
            _serving_summary(
                transport_evidence={
                    "requested_transport": "nvlink_intra",
                    "actual_transport": "nvlink_intra",
                    "pass": True,
                }
            )
        ),
        encoding="utf-8",
    )

    assert check_serving_summary(str(path)) == []


def test_serving_artifact_gate_rejects_mismatched_runtime_native_transport_proof(tmp_path):
    path = tmp_path / "summary.json"
    path.write_text(
        json.dumps(
            _serving_summary(
                transport_evidence={
                    "requested_transport": "nvlink_intra",
                    "actual_transport": "tcp",
                    "pass": False,
                }
            )
        ),
        encoding="utf-8",
    )

    failures = check_serving_summary(str(path))

    assert any("transport_runtime_evidence.pass is false" in item for item in failures)
    assert any("actual transport mismatch" in item for item in failures)


def test_serving_artifact_gate_requires_direct_engine_evidence_for_multimodal_path(tmp_path):
    summary = _serving_summary(
        transport_evidence={
            "requested_transport": "nvlink_intra",
            "actual_transport": "nvlink_intra",
            "pass": True,
        }
    )
    summary.update(
        {
            "text_only": False,
            "direct_engine_protocol": "tcp",
        }
    )
    path = tmp_path / "summary.json"
    path.write_text(json.dumps(summary), encoding="utf-8")

    failures = check_serving_summary(str(path))

    assert any("direct_transport_runtime_evidence missing" in item for item in failures)
