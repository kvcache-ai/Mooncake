from __future__ import annotations

import json
from pathlib import Path

from mooncake_epd.scripts.check_epd_model_preflight import inspect_model


def _runtime(*, omni: bool = False, omni_utils: bool = False):
    return {
        "vllm_available": True,
        "vllm_version": "0.23.0",
        "vllm_model_classes": {
            "qwen3_vl": True,
            "qwen2_5_vl": True,
            "qwen2_5_omni": True,
            "minicpmo": True,
        },
        "vllm_omni_available": omni,
        "qwen_omni_utils_available": omni_utils,
    }


def _write_config(path: Path, model_type: str, **extra: object) -> None:
    path.mkdir(parents=True, exist_ok=True)
    payload = {"model_type": model_type, "architectures": ["TestModel"]}
    payload.update(extra)
    (path / "config.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_index(path: Path, names: list[str]) -> None:
    (path / "model.safetensors.index.json").write_text(
        json.dumps({"weight_map": {f"weight_{idx}": name for idx, name in enumerate(names)}}),
        encoding="utf-8",
    )


def test_qwen3_preflight_requires_complete_checkpoint_and_marks_epd_supported(tmp_path):
    model = tmp_path / "qwen3"
    _write_config(model, "qwen3_vl")
    _write_index(model, ["model-00001-of-00001.safetensors"])
    (model / "model-00001-of-00001.safetensors").write_bytes(b"weight")

    report = inspect_model(model, runtime=_runtime())

    assert report["checkpoint"]["complete"] is True
    assert report["baseline"]["ready"] is True
    assert report["strict_epd"]["status"] == "supported"
    assert report["strict_epd"]["ready"] is True


def test_preflight_reports_exact_missing_checkpoint_shards_before_gpu_launch(tmp_path):
    model = tmp_path / "qwen25"
    _write_config(model, "qwen2_5_vl")
    _write_index(
        model,
        ["model-00001-of-00002.safetensors", "model-00002-of-00002.safetensors"],
    )
    (model / "model-00002-of-00002.safetensors").write_bytes(b"only-second")

    report = inspect_model(model, runtime=_runtime())

    assert report["checkpoint"]["complete"] is False
    assert report["checkpoint"]["missing_weight_files"] == ["model-00001-of-00002.safetensors"]
    assert report["baseline"]["status"] == "blocked"
    assert report["strict_epd"]["ready"] is False


def test_qwen25_vl_preflight_marks_the_model_specific_adapter_supported(tmp_path):
    model = tmp_path / "qwen25-vl"
    _write_config(model, "qwen2_5_vl")
    (model / "model.safetensors").write_bytes(b"weight")

    report = inspect_model(model, runtime=_runtime())

    assert report["strict_epd"] == {
        "status": "supported",
        "ready": True,
        "reasons": [],
        "encoder_family": "qwen2_5_vl",
        "vllm_target": "vllm.model_executor.models.qwen2_5_vl.Qwen2_5_VLForConditionalGeneration",
    }


def test_qwen_omni_preflight_blocks_full_pipeline_without_omni_runtime(tmp_path):
    model = tmp_path / "omni"
    _write_config(model, "qwen2_5_omni")
    (model / "model.safetensors").write_bytes(b"weight")

    report = inspect_model(model, runtime=_runtime(omni=False, omni_utils=False))

    assert report["baseline"]["ready"] is True
    assert report["strict_epd"]["status"] == "supported"
    assert report["strict_epd"]["ready"] is True
    assert report["full_omni_pipeline"]["status"] == "blocked"
    assert any("vllm_omni" in reason for reason in report["full_omni_pipeline"]["reasons"])


def test_minicpmo_preflight_refuses_generic_qwen_feature_handle_reuse(tmp_path):
    model = tmp_path / "minicpmo"
    _write_config(model, "minicpmo", auto_map={"AutoModel": "modeling.MiniCPMO"})
    (model / "model.safetensors").write_bytes(b"weight")

    report = inspect_model(model, runtime=_runtime())

    assert report["baseline"]["ready"] is True
    assert report["baseline"]["requires_remote_code"] is True
    assert report["strict_epd"]["status"] == "needs_model_adapter"
    assert any("model-specific" in reason for reason in report["strict_epd"]["reasons"])
