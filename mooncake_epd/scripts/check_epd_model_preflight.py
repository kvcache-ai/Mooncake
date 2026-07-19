#!/usr/bin/env python3
"""Fail-closed local model preflight for the EPD evaluation matrix.

The benchmark runners deliberately do not guess whether a checkpoint is
complete or whether a model can consume Mooncake ``FeatureHandle`` tensors.
This checker makes those prerequisites explicit before a costly GPU launch.

It is intentionally a *preflight*, not a synthetic compatibility claim:

* a complete checkpoint plus a vLLM model class means a colocated baseline is
  safe to attempt, not that it has passed a real serving run;
* Qwen3-VL, Qwen2.5-VL, and the Qwen2.5-Omni Thinker image path are reported
  as strict E->P->D eligible only because their model-specific encoder and
  precomputed-embedding adapters are regression-covered and have passed the
  transport gate.  That eligibility is not a claim of token-for-token output
  equivalence with a colocated server;
* Qwen2.5-Omni full AR/Generation/Diffusion serving is blocked without the
  separately installed ``vllm_omni`` runtime;
* MiniCPM-O stays fail-closed until a model-specific hidden-state adapter and
  output-equivalence oracle exist.

The report is JSON so benchmark campaigns and PR documentation can distinguish
an absent model artifact from an unsupported EPD path instead of turning both
into opaque vLLM startup failures.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence


_MODEL_SPECS: Mapping[str, Mapping[str, str]] = {
    "qwen3_vl": {
        "vllm_module": "vllm.model_executor.models.qwen3_vl",
        "vllm_class": "Qwen3VLForConditionalGeneration",
        "encoder_family": "qwen3_vl",
        "epd_status": "supported",
        "epd_reason": "Qwen3-VL FeatureHandle encoder/injection path is implemented and real-GPU covered.",
    },
    "qwen2_5_vl": {
        "vllm_module": "vllm.model_executor.models.qwen2_5_vl",
        "vllm_class": "Qwen2_5_VLForConditionalGeneration",
        "encoder_family": "qwen2_5_vl",
        "epd_status": "supported",
        "epd_reason": "Qwen2.5-VL FeatureHandle extraction and precomputed-image injection are implemented; real-GPU transport-gate coverage is recorded separately from output-equivalence benchmarking.",
    },
    "qwen2_5_omni": {
        "vllm_module": "vllm.model_executor.models.qwen2_5_omni_thinker",
        "vllm_class": "Qwen2_5OmniThinkerForConditionalGeneration",
        "encoder_family": "qwen2_5_omni",
        "epd_status": "supported",
        "epd_reason": "The Qwen2.5-Omni Thinker image FeatureHandle contract is implemented and real-GPU transport-gate covered; full Omni AR/Generation/Diffusion remains a separate runtime requirement.",
    },
    "minicpmo": {
        "vllm_module": "vllm.model_executor.models.minicpmo",
        "vllm_class": "MiniCPMO4_5",
        "encoder_family": "",
        "epd_status": "needs_model_adapter",
        "epd_reason": "MiniCPM-O image embeddings use a model-specific layout; generic Qwen injection is unsafe.",
    },
}

_INDEX_FILENAMES = (
    "model.safetensors.index.json",
    "pytorch_model.bin.index.json",
    "model.bin.index.json",
)
_SINGLE_WEIGHT_FILENAMES = (
    "model.safetensors",
    "pytorch_model.bin",
    "model.bin",
)


def _json_file(path: Path) -> tuple[Dict[str, Any] | None, str | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(payload, dict):
        return None, "JSON root must be an object"
    return payload, None


def inspect_checkpoint_files(model_path: Path) -> Dict[str, Any]:
    """Return deterministic local checkpoint completeness evidence."""

    index_paths = [model_path / name for name in _INDEX_FILENAMES if (model_path / name).is_file()]
    expected: set[str] = set()
    index_errors: Dict[str, str] = {}
    for index_path in index_paths:
        payload, error = _json_file(index_path)
        if error:
            index_errors[index_path.name] = error
            continue
        weight_map = payload.get("weight_map") if payload else None
        if not isinstance(weight_map, dict) or not weight_map:
            index_errors[index_path.name] = "missing non-empty weight_map"
            continue
        expected.update(str(value) for value in weight_map.values() if str(value).strip())

    if not index_paths:
        expected.update(name for name in _SINGLE_WEIGHT_FILENAMES if (model_path / name).is_file())

    missing = sorted(name for name in expected if not (model_path / name).is_file())
    present = sorted(name for name in expected if (model_path / name).is_file())
    # An index with no parseable entries is not a complete checkpoint even if
    # unrelated shard files happen to be in the directory.
    complete = bool(expected) and not missing and not index_errors
    return {
        "index_files": [path.name for path in index_paths],
        "expected_weight_files": sorted(expected),
        "present_weight_files": present,
        "missing_weight_files": missing,
        "index_errors": index_errors,
        "complete": complete,
    }


def _module_has_class(module_name: str, class_name: str) -> bool:
    try:
        module = importlib.import_module(module_name)
        return isinstance(getattr(module, class_name, None), type)
    except Exception:
        return False


def runtime_capabilities() -> Dict[str, Any]:
    """Probe imports without allocating a GPU or loading a model."""

    try:
        vllm = importlib.import_module("vllm")
        vllm_version = str(getattr(vllm, "__version__", ""))
        vllm_available = True
    except Exception as exc:
        vllm_version = ""
        vllm_available = False
        vllm_error = f"{type(exc).__name__}: {exc}"
    else:
        vllm_error = ""
    model_classes = {
        model_type: _module_has_class(spec["vllm_module"], spec["vllm_class"])
        if vllm_available
        else False
        for model_type, spec in _MODEL_SPECS.items()
    }
    return {
        "python": sys.executable,
        "vllm_available": vllm_available,
        "vllm_version": vllm_version,
        "vllm_error": vllm_error,
        "vllm_model_classes": model_classes,
        "vllm_omni_available": importlib.util.find_spec("vllm_omni") is not None,
        "qwen_omni_utils_available": importlib.util.find_spec("qwen_omni_utils") is not None,
    }


def _not_applicable() -> Dict[str, Any]:
    return {"status": "not_applicable", "ready": False, "reasons": []}


def inspect_model(
    model: str | Path,
    *,
    runtime: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Inspect one model path and return explicit baseline/EPD eligibility."""

    model_path = Path(model).expanduser().resolve()
    runtime = dict(runtime or runtime_capabilities())
    result: Dict[str, Any] = {
        "model_path": str(model_path),
        "exists": model_path.exists(),
        "is_directory": model_path.is_dir(),
        "model_type": "",
        "architectures": [],
        "checkpoint": {},
        "baseline": {"status": "blocked", "ready": False, "reasons": []},
        "strict_epd": {"status": "blocked", "ready": False, "reasons": []},
        "full_omni_pipeline": _not_applicable(),
    }
    if not model_path.is_dir():
        result["baseline"]["reasons"] = ["model directory does not exist"]
        result["strict_epd"]["reasons"] = ["model directory does not exist"]
        return result

    config_path = model_path / "config.json"
    if not config_path.is_file():
        result["checkpoint"] = inspect_checkpoint_files(model_path)
        reason = "config.json is missing"
        result["baseline"]["reasons"] = [reason]
        result["strict_epd"]["reasons"] = [reason]
        return result
    config, config_error = _json_file(config_path)
    if config_error:
        result["checkpoint"] = inspect_checkpoint_files(model_path)
        reason = f"invalid config.json: {config_error}"
        result["baseline"]["reasons"] = [reason]
        result["strict_epd"]["reasons"] = [reason]
        return result

    assert config is not None
    model_type = str(config.get("model_type") or "").strip().lower()
    architectures = config.get("architectures")
    result["model_type"] = model_type
    result["architectures"] = list(architectures) if isinstance(architectures, list) else []
    result["requires_remote_code"] = bool(config.get("auto_map"))
    result["checkpoint"] = inspect_checkpoint_files(model_path)
    spec = _MODEL_SPECS.get(model_type)
    complete = bool(result["checkpoint"].get("complete"))
    if spec is None:
        reasons = [f"unsupported model_type={model_type or '<missing>'}"]
        if not complete:
            reasons.append("checkpoint files are incomplete")
        result["baseline"]["reasons"] = reasons
        result["strict_epd"]["reasons"] = reasons + ["no model-specific FeatureHandle adapter"]
        return result

    model_class_available = bool(
        (runtime.get("vllm_model_classes") or {}).get(model_type)
    )
    baseline_reasons: list[str] = []
    if not bool(runtime.get("vllm_available")):
        baseline_reasons.append("vLLM is not importable")
    if not model_class_available:
        baseline_reasons.append(
            f"local vLLM class {spec['vllm_module']}.{spec['vllm_class']} is unavailable"
        )
    if not complete:
        missing = result["checkpoint"].get("missing_weight_files") or []
        baseline_reasons.append(
            "checkpoint files are incomplete"
            + (f": missing {', '.join(missing)}" if missing else "")
        )
    result["baseline"] = {
        "status": "ready_to_attempt" if not baseline_reasons else "blocked",
        "ready": not baseline_reasons,
        "reasons": baseline_reasons,
        "vllm_target": f"{spec['vllm_module']}.{spec['vllm_class']}",
        "requires_remote_code": bool(result["requires_remote_code"]),
    }

    epd_reasons: list[str] = []
    epd_status = str(spec["epd_status"])
    if not complete:
        epd_reasons.append("checkpoint files are incomplete")
    if not model_class_available:
        epd_reasons.append("local vLLM model class is unavailable")
    if epd_status != "supported":
        epd_reasons.append(str(spec["epd_reason"]))
    result["strict_epd"] = {
        "status": "supported" if not epd_reasons else (epd_status if complete else "blocked"),
        "ready": not epd_reasons,
        "reasons": epd_reasons,
        "encoder_family": str(spec["encoder_family"]),
        "vllm_target": f"{spec['vllm_module']}.{spec['vllm_class']}",
    }

    if model_type == "qwen2_5_omni":
        omni_reasons: list[str] = []
        if not complete:
            omni_reasons.append("checkpoint files are incomplete")
        if not bool(runtime.get("vllm_omni_available")):
            omni_reasons.append("vllm_omni is not installed; full AR/Generation/Diffusion serving is unavailable")
        if not bool(runtime.get("qwen_omni_utils_available")):
            omni_reasons.append("qwen_omni_utils is not installed")
        result["full_omni_pipeline"] = {
            "status": "ready_to_attempt" if not omni_reasons else "blocked",
            "ready": not omni_reasons,
            "reasons": omni_reasons,
        }
    return result


def build_report(models: Sequence[str | Path]) -> Dict[str, Any]:
    runtime = runtime_capabilities()
    rows = [inspect_model(model, runtime=runtime) for model in models]
    return {
        "schema_version": 1,
        "runtime": runtime,
        "models": rows,
        "summary": {
            "count": len(rows),
            "baseline_ready_to_attempt": sum(bool(row["baseline"].get("ready")) for row in rows),
            "strict_epd_ready": sum(bool(row["strict_epd"].get("ready")) for row in rows),
            "full_omni_ready_to_attempt": sum(
                bool(row["full_omni_pipeline"].get("ready")) for row in rows
            ),
        },
    }


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fail-closed EPD model compatibility preflight.")
    parser.add_argument(
        "--model",
        action="append",
        required=True,
        help="Model directory; repeat once per checkpoint.",
    )
    parser.add_argument("--output", required=True, help="Path for the JSON report.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    report = build_report(args.model)
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"output": str(output), "summary": report["summary"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
