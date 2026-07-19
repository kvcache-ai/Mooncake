from __future__ import annotations

import os
import time

import pytest
import torch

from mooncake_epd.core.omni import OmniProcessPipeline, ProcessStage
from mooncake_epd.core.omni_pipeline import OmniProcessPipeline as LegacyOmniProcessPipeline


def _ar_stage(tensors, metadata):
    return {"latent": tensors["image"] * 2}, {"ar_pid": os.getpid(), "trace": metadata.get("trace")}


def _generation_stage(tensors, metadata):
    return {"audio": tensors["latent"] + 3}, {"generation_pid": os.getpid()}


def _slow_stage(tensors, metadata):
    time.sleep(0.4)
    return {"audio": tensors["image"]}, {"slow_pid": os.getpid()}


def test_process_pipeline_uses_descriptor_only_posix_shm_edges():
    pipeline = OmniProcessPipeline(
        [
            ProcessStage("ar", _ar_stage, generation="ar-g1"),
            ProcessStage("generation", _generation_stage, generation="gen-g4"),
        ],
        queue_capacity=2,
        start_method="spawn",
        namespace="epd_omni_test",
    )
    try:
        output, metadata = pipeline.run(
            {"image": torch.tensor([1.0, 2.0])},
            metadata={"trace": "job-1"},
            timeout=20,
        )
        assert torch.equal(output["audio"], torch.tensor([5.0, 7.0]))
        assert metadata["trace"] == "job-1"
        assert metadata["ar_pid"] != os.getpid()
        assert metadata["generation_pid"] != os.getpid()
        assert metadata["ar_pid"] != metadata["generation_pid"]
        assert [row["stage"] for row in metadata["stage_timings_ms"]] == ["ar", "generation"]
        assert all(row["transport"] == "posix_shm" for row in metadata["stage_timings_ms"])
        assert metadata["pipeline_timing_ms"]["wall_ms"] > 0
        assert pipeline.stats()["transport"] == "posix_shm"
        assert pipeline.stats()["released_tensors"] == 3
    finally:
        pipeline.stop()


def test_monolithic_omni_pipeline_module_preserves_process_runtime_import():
    assert LegacyOmniProcessPipeline is OmniProcessPipeline


def test_process_pipeline_uses_actual_tcp_relay_edges_without_tensor_queue_payloads():
    pipeline = OmniProcessPipeline(
        [
            ProcessStage("ar", _ar_stage, generation="ar-g1"),
            ProcessStage("generation", _generation_stage, generation="gen-g4"),
        ],
        queue_capacity=2,
        start_method="spawn",
        namespace="epd_omni_tcp_test",
        transport="tcp",
    )
    try:
        output, metadata = pipeline.run(
            {"image": torch.tensor([1.0, 2.0])},
            metadata={"trace": "job-tcp"},
            timeout=20,
        )
        assert torch.equal(output["audio"], torch.tensor([5.0, 7.0]))
        assert all(row["transport"] == "tcp" for row in metadata["stage_timings_ms"])
        stats = pipeline.stats()
        assert stats["transport"] == "tcp"
        assert stats["tcp_relay"]["puts"] >= 3
        assert stats["tcp_relay"]["gets"] >= 3
        assert stats["tcp_relay"]["objects"] == 0
        assert stats["released_tensors"] == 3
    finally:
        pipeline.stop()


def test_process_pipeline_timeout_can_be_stopped_and_restarted_without_reusing_payloads():
    pipeline = OmniProcessPipeline(
        [ProcessStage("slow", _slow_stage, generation="slow-g1")],
        queue_capacity=1,
        start_method="spawn",
        namespace="epd_omni_restart_test",
    )
    try:
        with pytest.raises(TimeoutError, match="queue timed out"):
            pipeline.run({"image": torch.tensor([9.0])}, timeout=0.1, job_id="timed-out")
        pipeline.stop(timeout=2.0)
        output, metadata = pipeline.run(
            {"image": torch.tensor([3.0])},
            timeout=5.0,
            job_id="restart-success",
        )
        assert torch.equal(output["audio"], torch.tensor([3.0]))
        assert metadata["slow_pid"] != os.getpid()
        assert pipeline.stats()["jobs"] == 1
        assert pipeline.stats()["failures"] == 1
    finally:
        pipeline.stop()
