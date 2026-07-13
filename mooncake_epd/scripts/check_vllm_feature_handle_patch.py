#!/usr/bin/env python3
"""Check Mooncake EPD FeatureHandle hooks in the active vLLM runtime.

This checker is intentionally read-only.  It validates whether the repo-local
``sitecustomize`` hooks are active for:
- GPUModelRunner request-level ``kv_transfer_params.mm_feature_handles`` injection
- Qwen-VL model ``embed_multimodal`` / native ``image_embeds`` hidden-state path
- Mooncake provider importability

Exit code is non-zero when ``--require-vllm`` or ``--require-hooks`` is used and
that requirement is not satisfied.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from typing import Any, Dict


def _flag(obj: Any, name: str) -> bool:
    return bool(getattr(obj, name, False))


def build_report() -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "python": sys.executable,
        "vllm_available": False,
        "provider_available": False,
        "gpu_model_runner_hook": False,
        "gpu_model_runner_kv_params_hook": False,
        "direct_feature_buffer_build_app_hook": False,
        "qwen_hooks": [],
        "errors": [],
    }
    try:
        import mooncake_epd.core.state.vllm_feature_handle_provider as provider

        report["provider_available"] = hasattr(provider, "FeatureHandleProvider")
    except Exception as exc:
        report["errors"].append(f"provider import failed: {type(exc).__name__}: {exc}")

    try:
        vllm = importlib.import_module("vllm")
        report["vllm_available"] = True
        report["vllm_file"] = getattr(vllm, "__file__", "")
    except Exception as exc:
        report["errors"].append(f"vllm import failed: {type(exc).__name__}: {exc}")
        return report

    try:
        gm = importlib.import_module("vllm.v1.worker.gpu_model_runner")
        runner = getattr(gm, "GPUModelRunner")
        method = getattr(runner, "_batch_mm_inputs_from_scheduler", None)
        report["gpu_model_runner_hook"] = _flag(method, "_mooncake_epd_feature_handle_patch")
        update_states = getattr(runner, "_update_states", None)
        report["gpu_model_runner_kv_params_hook"] = _flag(update_states, "_mooncake_epd_kv_params_patch")
    except Exception as exc:
        report["errors"].append(f"GPUModelRunner check failed: {type(exc).__name__}: {exc}")

    try:
        api_server = importlib.import_module("vllm.entrypoints.openai.api_server")
        build_app = getattr(api_server, "build_app", None)
        report["direct_feature_buffer_build_app_hook"] = _flag(
            build_app, "_mooncake_epd_direct_feature_buffer_patch"
        )
    except Exception as exc:
        report["errors"].append(f"api_server build_app check failed: {type(exc).__name__}: {exc}")

    for module_name, class_name in (
        ("vllm.model_executor.models.qwen3_vl", "Qwen3VLForConditionalGeneration"),
        ("vllm.model_executor.models.qwen2_5_vl", "Qwen2_5_VLForConditionalGeneration"),
        ("vllm.model_executor.models.qwen2_vl", "Qwen2VLForConditionalGeneration"),
    ):
        try:
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            hooks = {}
            for method_name in ("embed_multimodal", "_process_image_input", "_parse_and_validate_image_input"):
                method = getattr(cls, method_name, None)
                hooks[method_name] = {
                    "exists": method is not None,
                    "feature_handle_hook": _flag(method, "_mooncake_epd_feature_handle_patch"),
                }
            report["qwen_hooks"].append(
                {"module": module_name, "class": class_name, "methods": hooks}
            )
        except Exception as exc:
            report["qwen_hooks"].append(
                {"module": module_name, "class": class_name, "error": f"{type(exc).__name__}: {exc}"}
            )
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true", help="print compact JSON only")
    ap.add_argument("--require-vllm", action="store_true")
    ap.add_argument("--require-hooks", action="store_true")
    args = ap.parse_args()

    report = build_report()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))

    failed = False
    if args.require_vllm and not report.get("vllm_available"):
        failed = True
    if args.require_hooks:
        has_qwen_hook = any(
            any(method.get("feature_handle_hook") for method in item.get("methods", {}).values())
            for item in report.get("qwen_hooks", [])
        )
        if (
            not report.get("provider_available")
            or not report.get("gpu_model_runner_hook")
            or not report.get("gpu_model_runner_kv_params_hook")
            or not report.get("direct_feature_buffer_build_app_hook")
            or not has_qwen_hook
        ):
            failed = True
    if failed:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
