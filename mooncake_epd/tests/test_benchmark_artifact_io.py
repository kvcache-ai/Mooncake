from __future__ import annotations

import json
from pathlib import Path

from mooncake_epd.scripts.benchmark_artifact_io import write_raw_benchmark_artifacts


def test_raw_benchmark_artifacts_are_complete_and_redact_secrets(tmp_path, monkeypatch):
    service_log = tmp_path / "service.log"
    service_log.write_text("ready\n", encoding="utf-8")
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN", "must-not-leak")

    artifacts = write_raw_benchmark_artifacts(
        workdir=tmp_path,
        request_rows=[{"index": 0, "request": {"prompt": "hello"}}],
        response_rows=[{"index": 0, "status_code": 200}],
        service_logs={"server": service_log},
        metrics={"requests_total": 1},
        runtime={
            "model": "Qwen3-VL-8B-Instruct",
            "direct_buffer_auth_token": "must-not-leak",
        },
    )

    assert all(Path(value).exists() for value in artifacts.values())
    assert set(artifacts) == {
        "requests_jsonl",
        "responses_jsonl",
        "service_logs",
        "metrics",
        "environment",
        "hardware",
    }
    environment = json.loads(Path(artifacts["environment"]).read_text(encoding="utf-8"))
    assert environment["environment"]["MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN"] == "<redacted>"
    assert environment["runtime"]["model"] == "Qwen3-VL-8B-Instruct"
    assert environment["runtime"]["direct_buffer_auth_token"] == "<redacted>"
    hardware = json.loads(Path(artifacts["hardware"]).read_text(encoding="utf-8"))
    assert hardware["schema_version"] == 1
    assert set(hardware["commands"]) == {"gpu_query", "topology", "nvlink"}
