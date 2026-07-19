"""Explicit installation boundary for the Mooncake EPD vLLM hooks.

``sitecustomize`` is retained only as a compatibility bridge for old launch
scripts.  New launchers call :func:`install_vllm_epd_adapter` in the worker
interpreter before they enter vLLM's CLI, which makes the activation point,
version check and capability report reproducible.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from .capabilities import (
    PROMPT_ONLY_PROTOCOL_VERSION,
    VLLMCapabilityReport,
    probe_vllm_epd_capabilities,
    required_capability_errors,
)


class VLLMAdapterError(RuntimeError):
    """Raised when a strict EPD worker cannot install compatible hooks."""


_INSTALL_LOCK = threading.RLock()
_LAST_REPORT: Dict[str, Any] | None = None


def _strict_requested(explicit: Optional[bool]) -> bool:
    if explicit is not None:
        return bool(explicit)
    try:
        from mooncake_epd.core.strict_mode import strict_no_fallback_enabled

        return bool(strict_no_fallback_enabled())
    except Exception:
        return str(os.getenv("MOONCAKE_EPD_STRICT", "0")).lower() in {"1", "true", "yes", "on"}


def _legacy_patch_module() -> Any:
    """Load the temporary hook carrier without triggering compatibility boot."""

    existing = sys.modules.get("sitecustomize")
    if existing is not None:
        return existing
    old = os.environ.get("MOONCAKE_EPD_ADAPTER_COMPAT_BOOTSTRAP")
    os.environ["MOONCAKE_EPD_ADAPTER_COMPAT_BOOTSTRAP"] = "0"
    try:
        return importlib.import_module("sitecustomize")
    finally:
        if old is None:
            os.environ.pop("MOONCAKE_EPD_ADAPTER_COMPAT_BOOTSTRAP", None)
        else:
            os.environ["MOONCAKE_EPD_ADAPTER_COMPAT_BOOTSTRAP"] = old


def _write_report(report: Dict[str, Any]) -> None:
    destination = str(os.getenv("MOONCAKE_EPD_VLLM_CAPABILITY_REPORT", "")).strip()
    if not destination:
        return
    path = Path(destination).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def adapter_installation_report() -> Dict[str, Any] | None:
    """Return the last immutable installation report for observability/tests."""

    with _INSTALL_LOCK:
        return dict(_LAST_REPORT) if _LAST_REPORT is not None else None


def install_vllm_epd_adapter(*, strict: Optional[bool] = None) -> Dict[str, Any]:
    """Probe then install all EPD-specific vLLM hooks in this interpreter.

    Installation is idempotent.  In strict mode every required private API is
    verified before monkeypatching begins, preventing a partly-patched process
    from accepting a request and silently recomputing the vision tower.
    """

    global _LAST_REPORT
    strict_mode = _strict_requested(strict)
    with _INSTALL_LOCK:
        capability = probe_vllm_epd_capabilities()
        failures = list(required_capability_errors(capability))
        report: Dict[str, Any] = capability.to_dict()
        report["strict"] = strict_mode
        report["installed"] = False
        report["installation_errors"] = []
        if failures:
            report["installation_errors"] = failures
            _LAST_REPORT = report
            _write_report(report)
            if strict_mode:
                raise VLLMAdapterError("Mooncake EPD vLLM adapter preflight failed: " + "; ".join(failures))
            return dict(report)

        # The compatibility carrier's helpers deliberately consult the same
        # opt-in predicate.  Explicit activation must work even though new
        # launchers keep MOONCAKE_EPD_ENABLE_VLLM_PATCHES=0 to suppress global
        # sitecustomize bootstrapping.
        previous_enabled = os.environ.get("MOONCAKE_EPD_ENABLE_VLLM_PATCHES")
        os.environ["MOONCAKE_EPD_ENABLE_VLLM_PATCHES"] = "1"
        try:
            legacy = _legacy_patch_module()
            legacy._patch_vllm_prompt_envelope_metadata()
            legacy._patch_vllm_decode_token_multimodal_metadata()
            legacy._patch_vllm_prompt_only_prefill()
            legacy._patch_vllm_qwen25_omni_precomputed_image_embed_contract()
            legacy._patch_vllm_qwen25_vl_precomputed_image_embed_observability()
            legacy._patch_vllm_feature_handle_injection()
            legacy._patch_vllm_gpu_model_runner_kv_params()
            legacy._patch_vllm_gpu_model_runner_feature_handles()
            legacy._patch_vllm_openai_app_direct_feature_buffer_routes()
        except Exception as exc:
            report["installation_errors"] = [f"{type(exc).__name__}: {exc}"]
            _LAST_REPORT = report
            _write_report(report)
            raise VLLMAdapterError("Mooncake EPD vLLM adapter install failed") from exc
        finally:
            if previous_enabled is None:
                os.environ.pop("MOONCAKE_EPD_ENABLE_VLLM_PATCHES", None)
            else:
                os.environ["MOONCAKE_EPD_ENABLE_VLLM_PATCHES"] = previous_enabled

        report["installed"] = True
        try:
            from vllm.sampling_params import SamplingParams
            from vllm.v1.core.sched.scheduler import Scheduler
            from vllm.entrypoints.openai.chat_completion.serving import (
                OpenAIServingChat,
            )
            from vllm.entrypoints.openai.completion.serving import (
                OpenAIServingCompletion,
            )

            sampling_protocol = int(
                getattr(
                    SamplingParams,
                    "_mooncake_epd_prompt_only_protocol_version",
                    0,
                )
                or 0
            )
            scheduler_protocol = int(
                getattr(
                    Scheduler.update_from_output,
                    "_mooncake_epd_prompt_only_protocol_version",
                    0,
                )
                or 0
            )
            report["prompt_only_patch_installed"] = bool(
                getattr(
                    SamplingParams,
                    "_mooncake_epd_prompt_only_patch",
                    False,
                )
                and getattr(
                    Scheduler.update_from_output,
                    "_mooncake_epd_prompt_only_patch",
                    False,
                )
            )
            report["prompt_only_protocol_version"] = min(
                sampling_protocol,
                scheduler_protocol,
            )
            report["prompt_envelope_metadata_patch_installed"] = bool(
                getattr(
                    OpenAIServingChat.render_chat_request,
                    "_mooncake_epd_prompt_envelope_patch",
                    False,
                )
                and getattr(
                    OpenAIServingChat.create_chat_completion,
                    "_mooncake_epd_prompt_envelope_patch",
                    False,
                )
            )
            report["decode_token_multimodal_patch_installed"] = bool(
                getattr(
                    OpenAIServingCompletion.render_completion_request,
                    "_mooncake_epd_decode_token_mm_patch",
                    False,
                )
            )
        except Exception as exc:
            report["prompt_only_patch_installed"] = False
            report["prompt_only_protocol_version"] = 0
            report["installation_errors"] = [
                f"prompt-only installation verification failed: {type(exc).__name__}: {exc}"
            ]
        if (
            not bool(report.get("prompt_only_patch_installed", False))
            or int(report.get("prompt_only_protocol_version", 0) or 0)
            != PROMPT_ONLY_PROTOCOL_VERSION
        ):
            _LAST_REPORT = report
            _write_report(report)
            raise VLLMAdapterError(
                "Mooncake EPD prompt-only protocol installation verification failed"
            )
        report["adapter_version"] = "v2"
        _LAST_REPORT = report
        _write_report(report)
        return dict(report)
