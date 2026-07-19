"""Durable, redact-safe raw evidence bundles for serving benchmarks."""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterable, Mapping


_SENSITIVE_ENV_MARKERS = (
    "auth",
    "authorization",
    "bearer",
    "cookie",
    "credential",
    "key",
    "password",
    "passwd",
    "private",
    "secret",
    "session",
    "token",
)


def _is_sensitive_key(key: Any) -> bool:
    normalized = str(key).lower()
    return any(marker in normalized for marker in _SENSITIVE_ENV_MARKERS)


def _redact_mapping_values(value: Any) -> Any:
    """Recursively redact values whose mapping key looks credential-bearing."""

    if isinstance(value, Mapping):
        return {
            str(key): "<redacted>"
            if _is_sensitive_key(key)
            else _redact_mapping_values(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_mapping_values(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_mapping_values(item) for item in value]
    return value


def _redacted_environment(extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Capture enough runtime context to reproduce a run without secrets."""

    environment: dict[str, str] = {}
    for key, value in os.environ.items():
        environment[str(key)] = (
            "<redacted>"
            if _is_sensitive_key(key)
            else str(value)
        )
    return {
        "captured_at_unix": time.time(),
        "python": sys.version,
        "platform": platform.platform(),
        "environment": environment,
        "runtime": _redact_mapping_values(dict(extra or {})),
    }


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def _capture_hardware_command(argv: list[str]) -> dict[str, Any]:
    """Capture a bounded, shell-free hardware probe without making it a dependency.

    Benchmark execution must remain usable on CPU CI and developer laptops.
    Rather than silently omitting the probe, record an explicit unavailable or
    failed result so a campaign can decide whether hardware inventory is a
    required gate for that scenario.
    """

    executable = shutil.which(argv[0])
    if executable is None:
        return {"argv": argv, "available": False, "returncode": None, "stdout": "", "stderr": "not found"}
    try:
        result = subprocess.run(
            [executable, *argv[1:]],
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "argv": argv,
            "available": True,
            "returncode": None,
            "stdout": "",
            "stderr": f"{type(exc).__name__}: {exc}",
        }
    return {
        "argv": argv,
        "available": True,
        "returncode": int(result.returncode),
        "stdout": str(result.stdout or "")[-20000:],
        "stderr": str(result.stderr or "")[-4000:],
    }


def _hardware_inventory() -> dict[str, Any]:
    """Return a reproducible GPU/topology snapshot captured with the artifact."""

    probes = {
        "gpu_query": [
            "nvidia-smi",
            "--query-gpu=index,uuid,name,driver_version,memory.total,memory.used,pstate",
            "--format=csv,noheader",
        ],
        "topology": ["nvidia-smi", "topo", "-m"],
        "nvlink": ["nvidia-smi", "nvlink", "-s"],
    }
    return {
        "schema_version": 1,
        "captured_at_unix": time.time(),
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "commands": {
            name: _capture_hardware_command(argv) for name, argv in probes.items()
        },
    }


def write_raw_benchmark_artifacts(
    *,
    workdir: Path,
    request_rows: Iterable[Mapping[str, Any]],
    response_rows: Iterable[Mapping[str, Any]],
    service_logs: Mapping[str, str | Path],
    metrics: Mapping[str, Any],
    runtime: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Write the minimum evidence set required by the claim gate.

    The service-log entry is a manifest rather than a copy of potentially large
    logs.  It preserves every source path and explicitly records any missing
    path, so an artifact consumer can distinguish unavailable evidence from an
    empty log.
    """

    root = Path(workdir) / "raw_artifacts"
    root.mkdir(parents=True, exist_ok=True)
    requests_path = root / "requests.jsonl"
    responses_path = root / "responses.jsonl"
    service_logs_path = root / "service_logs.json"
    metrics_path = root / "metrics.json"
    environment_path = root / "environment.json"
    hardware_path = root / "hardware.json"

    _write_jsonl(requests_path, request_rows)
    _write_jsonl(responses_path, response_rows)
    log_paths = {str(key): str(value) for key, value in service_logs.items()}
    service_logs_path.write_text(
        json.dumps(
            {
                "logs": log_paths,
                "missing": [path for path in log_paths.values() if not Path(path).exists()],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    metrics_path.write_text(
        json.dumps(dict(metrics), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    environment_path.write_text(
        json.dumps(_redacted_environment(runtime), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    hardware_path.write_text(
        json.dumps(_hardware_inventory(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {
        "requests_jsonl": str(requests_path),
        "responses_jsonl": str(responses_path),
        "service_logs": str(service_logs_path),
        "metrics": str(metrics_path),
        "environment": str(environment_path),
        "hardware": str(hardware_path),
    }
