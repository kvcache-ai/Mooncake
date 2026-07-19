from __future__ import annotations

from mooncake_epd.scripts.check_epd_transport_evidence import evaluate_transport_evidence


def test_rdma_claim_requires_actual_backend_and_hardware_metadata():
    rejected = evaluate_transport_evidence({"protocol": "tcp", "remote_host": "127.0.0.1"}, requested="rdma")
    assert rejected["pass"] is False
    assert "artifact_has_no_actual_rdma_backend_label" in rejected["failures"]

    accepted = evaluate_transport_evidence(
        {
            "actual_transport": "rdma",
            "rdma": {"nic": "mlx5_0", "gid": "fe80::1", "remote_host": "10.0.0.2"},
        },
        requested="rdma",
    )
    assert accepted["pass"] is True


def test_tcp_claim_cannot_pass_from_only_a_cli_request_value():
    result = evaluate_transport_evidence({"requested_protocol": "tcp"}, requested="tcp")
    assert result["pass"] is False


def test_protocol_field_is_not_mistaken_for_actual_transport_evidence():
    result = evaluate_transport_evidence({"protocol": "nvlink_intra"}, requested="nvlink_intra")
    assert result["pass"] is False


def test_runtime_worker_evidence_is_required_to_be_consistent():
    result = evaluate_transport_evidence(
        {
            "protocol": "nvlink_intra",
            "transport_runtime_evidence": {
                "requested_transport": "nvlink_intra",
                "actual_transport": "tcp",
                "pass": False,
            },
        },
        requested="nvlink_intra",
    )
    assert result["pass"] is False
    assert "runtime_transport_evidence_failed" in result["failures"]
    assert "runtime_transport_evidence_actual_transport_is_not_nvlink_intra" in result["failures"]
