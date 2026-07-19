from __future__ import annotations

from mooncake_epd.benchmarks.omni_pipeline_benchmark import run_benchmark
from mooncake_epd.scripts.run_omni_worker_pipeline_e2e import run_pipeline


def test_omni_worker_pipeline_runner_reports_actual_posix_shm_process_path():
    result = run_pipeline(elements=8, timeout=20.0)

    assert result["status"] == "ok"
    assert result["actual_transport"] == "posix_shm"
    assert result["claim_supported"] is False
    assert len(result["stage_pids"]) == 3
    assert len(result["stage_timings_ms"]) == 3
    assert result["pipeline_timing_ms"]["wall_ms"] > 0


def test_omni_rdma_is_a_capability_skip_not_a_relabelled_cpu_transport():
    result = run_pipeline(transport="rdma")

    assert result["status"] == "skipped"
    assert result["actual_transport"] is None
    assert result["claim_supported"] is False
    assert result["rdma_evidence"] is None


def test_omni_tcp_runner_uses_tcp_not_a_posix_shm_label():
    result = run_pipeline(transport="tcp", elements=8, timeout=20.0)

    assert result["status"] == "ok"
    assert result["actual_transport"] == "tcp"
    assert all(row["transport"] == "tcp" for row in result["stage_timings_ms"])


def test_omni_benchmark_preserves_rdma_skip_as_non_claim_result():
    result = run_benchmark(transport="rdma", warmup=0, rounds=1)

    assert result["status"] == "skipped"
    assert result["claim_supported"] is False
    assert result["actual_transport"] is None
