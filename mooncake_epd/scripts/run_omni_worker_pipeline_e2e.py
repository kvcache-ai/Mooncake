#!/usr/bin/env python3
"""Run the truthful CPU Omni worker-process pipeline smoke test.

This runner is deliberately not a Qwen2.5-Omni benchmark.  It proves the
worker boundary used by the real pipeline: independent PIDs exchange only
POSIX-SHM tensor descriptors and validate checksums.  CUDA IPC/NVLink/TCP/RDMA
are capability-gated instead of being relabelled as this CPU path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Sequence

import torch

from mooncake_epd.core.omni import OmniProcessPipeline, ProcessStage


_SUPPORTED_TRANSPORTS = ("posix_shm", "tcp")
_DEFAULT_TRANSPORT = "posix_shm"
_KNOWN_TRANSPORTS = ("posix_shm", "cuda_ipc", "nvlink_intra", "tcp", "rdma", "copy")


def _ar_stage(tensors: Dict[str, torch.Tensor], metadata: Dict[str, Any]):
    return {"latent": tensors["input"] * 2}, {"ar_pid": os.getpid()}


def _generation_stage(tensors: Dict[str, torch.Tensor], metadata: Dict[str, Any]):
    return {"tokens": tensors["latent"] + 3}, {"generation_pid": os.getpid()}


def _diffusion_stage(tensors: Dict[str, torch.Tensor], metadata: Dict[str, Any]):
    return {"audio": tensors["tokens"] - 1}, {"diffusion_pid": os.getpid()}


def _checksum(tensor: torch.Tensor) -> str:
    array = tensor.detach().cpu().contiguous().numpy()
    return hashlib.sha256(array.tobytes()).hexdigest()


def unavailable_transport_result(transport: str) -> Dict[str, Any]:
    return {
        "status": "skipped",
        "claim_supported": False,
        "requested_transport": str(transport),
        "actual_transport": None,
        "reason": (
            f"transport={transport} has no installed Omni worker adapter; "
            "the CPU POSIX-SHM smoke path is not a substitute"
        ),
        "rdma_evidence": None,
    }


def run_pipeline(
    *,
    transport: str = _DEFAULT_TRANSPORT,
    elements: int = 64,
    queue_capacity: int = 4,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """Run one descriptor-only three-stage CPU process pipeline."""

    requested = str(transport).strip().lower()
    if requested not in _KNOWN_TRANSPORTS:
        raise ValueError(f"unknown transport={transport}; choices={','.join(_KNOWN_TRANSPORTS)}")
    if requested not in _SUPPORTED_TRANSPORTS:
        return unavailable_transport_result(requested)
    if int(elements) <= 0:
        raise ValueError("elements must be positive")
    namespace = f"mooncake_epd_omni_e2e_{os.getpid()}_{time.time_ns()}"
    pipeline = OmniProcessPipeline(
        [
            ProcessStage("ar", _ar_stage, generation="ar-cpu-g1"),
            ProcessStage("generation", _generation_stage, generation="gen-cpu-g1"),
            ProcessStage("diffusion", _diffusion_stage, generation="diff-cpu-g1"),
        ],
        queue_capacity=max(1, int(queue_capacity)),
        start_method="spawn",
        namespace=namespace,
        transport=requested,
    )
    source = torch.arange(int(elements), dtype=torch.float32)
    started = time.perf_counter()
    try:
        output, metadata = pipeline.run(
            {"input": source},
            metadata={"runner": "omni_cpu_process_smoke"},
            timeout=max(0.1, float(timeout)),
            job_id="omni-cpu-process-smoke",
        )
        expected = source * 2 + 2
        if not torch.equal(output["audio"], expected):
            raise RuntimeError("Omni CPU process pipeline produced an unexpected output")
        stats = pipeline.stats()
        stage_pids = [
            int(metadata[key])
            for key in ("ar_pid", "generation_pid", "diffusion_pid")
        ]
        if any(pid == os.getpid() for pid in stage_pids) or len(set(stage_pids)) != 3:
            raise RuntimeError("Omni stages did not execute in three independent worker processes")
        return {
            "status": "ok",
            "claim_supported": False,
            "semantic_mode": "synthetic_cpu_process_smoke",
            "requested_transport": requested,
            "actual_transport": requested,
            "rdma_evidence": None,
            "source": {
                "shape": list(source.shape),
                "dtype": str(source.dtype),
                "checksum": _checksum(source),
            },
            "output": {
                "shape": list(output["audio"].shape),
                "dtype": str(output["audio"].dtype),
                "checksum": _checksum(output["audio"]),
            },
            "stage_pids": stage_pids,
            "stage_timings_ms": list(metadata.get("stage_timings_ms") or []),
            "pipeline_timing_ms": dict(metadata.get("pipeline_timing_ms") or {}),
            "runtime_stats": stats,
            "wall_ms": (time.perf_counter() - started) * 1000.0,
        }
    finally:
        pipeline.stop()


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    destination = Path(path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=_KNOWN_TRANSPORTS, default=_DEFAULT_TRANSPORT)
    parser.add_argument("--elements", type=int, default=64)
    parser.add_argument("--queue-capacity", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--json-out", default=None)
    parser.add_argument(
        "--strict-capability",
        action="store_true",
        help="return nonzero when the requested transport has no concrete adapter",
    )
    args = parser.parse_args(argv)
    result = run_pipeline(
        transport=args.transport,
        elements=args.elements,
        queue_capacity=args.queue_capacity,
        timeout=args.timeout,
    )
    if args.json_out:
        _write_json(args.json_out, result)
    print(json.dumps(result, sort_keys=True))
    return 2 if args.strict_capability and result["status"] != "ok" else 0


if __name__ == "__main__":
    raise SystemExit(main())
