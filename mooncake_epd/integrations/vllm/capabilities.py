"""vLLM compatibility probes used by the explicit EPD adapter.

The EPD hooks touch vLLM private interfaces.  A process therefore probes the
exact runtime before installing anything; an incompatible runtime is a startup
failure in strict mode, never a silent visual-encoder fallback.
"""

from __future__ import annotations

import importlib
import inspect
import re
import sys
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Tuple


_SUPPORTED_VLLM_MINOR = {(0, 23)}
PROMPT_ONLY_PROTOCOL_VERSION = 2
_QWEN_TARGETS: Tuple[Tuple[str, str], ...] = (
    ("vllm.model_executor.models.qwen3_vl", "Qwen3VLForConditionalGeneration"),
    ("vllm.model_executor.models.qwen3_vl", "Qwen3VLModel"),
    ("vllm.model_executor.models.qwen2_5_vl", "Qwen2_5_VLForConditionalGeneration"),
    # vLLM's Qwen2.5-Omni Thinker exposes the same native image_embeds /
    # image_grid_thw contract as Qwen2.5-VL.  It is intentionally the Thinker
    # only: full Omni AR/Generation/Diffusion still requires vllm_omni.
    (
        "vllm.model_executor.models.qwen2_5_omni_thinker",
        "Qwen2_5OmniThinkerForConditionalGeneration",
    ),
    ("vllm.model_executor.models.qwen2_vl", "Qwen2VLForConditionalGeneration"),
)
_QWEN_METHODS = (
    "embed_multimodal",
    "get_multimodal_embeddings",
    "get_input_embeddings",
    "forward",
)


def _version_tuple(value: object) -> Tuple[int, int] | None:
    match = re.match(r"^(\d+)\.(\d+)", str(value or ""))
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


def _accepts_image_embeds(method: Any) -> bool:
    try:
        parameters = inspect.signature(method).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == "image_embeds"
        or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _has_expected_positional_argument(method: Any, expected: str) -> bool:
    try:
        parameters = inspect.signature(method).parameters
    except (TypeError, ValueError):
        return False
    return expected in parameters or len(parameters) >= 2


@dataclass(frozen=True)
class VLLMCapabilityReport:
    """A JSON-serializable preflight result for one vLLM process."""

    available: bool
    version: str = ""
    version_supported: bool = False
    python: str = ""
    gpu_model_runner_batch_mm: bool = False
    gpu_model_runner_update_states: bool = False
    openai_build_app: bool = False
    prompt_only_sampling_params: bool = False
    prompt_only_scheduler_finalize: bool = False
    qwen_image_embed_hooks: Tuple[str, ...] = ()
    errors: Tuple[str, ...] = ()
    checked_targets: Tuple[str, ...] = ()

    @property
    def feature_handle_ready(self) -> bool:
        return bool(
            self.available
            and self.version_supported
            and self.gpu_model_runner_batch_mm
            and self.gpu_model_runner_update_states
            and self.openai_build_app
            and self.qwen_image_embed_hooks
        )

    @property
    def prompt_only_ready(self) -> bool:
        return bool(
            self.available
            and self.version_supported
            and self.prompt_only_sampling_params
            and self.prompt_only_scheduler_finalize
        )

    @property
    def ready(self) -> bool:
        return self.feature_handle_ready and self.prompt_only_ready

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["qwen_image_embed_hooks"] = list(self.qwen_image_embed_hooks)
        payload["errors"] = list(self.errors)
        payload["checked_targets"] = list(self.checked_targets)
        payload["feature_handle_ready"] = self.feature_handle_ready
        payload["prompt_only_ready"] = self.prompt_only_ready
        payload["prompt_only_protocol_version"] = PROMPT_ONLY_PROTOCOL_VERSION
        payload["ready"] = self.ready
        payload["supported_vllm_minors"] = ["%d.%d" % item for item in sorted(_SUPPORTED_VLLM_MINOR)]
        return payload


def probe_vllm_epd_capabilities() -> VLLMCapabilityReport:
    """Inspect the active vLLM installation without applying EPD patches."""

    errors: List[str] = []
    checked: List[str] = []
    try:
        vllm = importlib.import_module("vllm")
    except Exception as exc:
        return VLLMCapabilityReport(
            available=False,
            python=sys.executable,
            errors=(f"vllm import failed: {type(exc).__name__}: {exc}",),
        )

    version = str(getattr(vllm, "__version__", ""))
    version_minor = _version_tuple(version)
    version_supported = version_minor in _SUPPORTED_VLLM_MINOR
    if not version_supported:
        errors.append(
            "unsupported vLLM version "
            f"{version!r}; supported minors are "
            + ", ".join("%d.%d" % item for item in sorted(_SUPPORTED_VLLM_MINOR))
        )

    batch_mm = False
    update_states = False
    try:
        runner_module = importlib.import_module("vllm.v1.worker.gpu_model_runner")
        runner = getattr(runner_module, "GPUModelRunner")
        batch_mm = _has_expected_positional_argument(
            getattr(runner, "_batch_mm_inputs_from_scheduler", None), "scheduler_output"
        )
        update_states = _has_expected_positional_argument(
            getattr(runner, "_update_states", None), "scheduler_output"
        )
        checked.append("vllm.v1.worker.gpu_model_runner.GPUModelRunner")
    except Exception as exc:
        errors.append(f"GPUModelRunner probe failed: {type(exc).__name__}: {exc}")

    build_app = False
    try:
        api_server = importlib.import_module("vllm.entrypoints.openai.api_server")
        build_app = callable(getattr(api_server, "build_app", None))
        checked.append("vllm.entrypoints.openai.api_server.build_app")
    except Exception as exc:
        errors.append(f"OpenAI app probe failed: {type(exc).__name__}: {exc}")

    prompt_only = False
    try:
        sampling_params = importlib.import_module("vllm.sampling_params")
        prompt_only = callable(getattr(sampling_params, "SamplingParams", None))
        checked.append("vllm.sampling_params.SamplingParams")
    except Exception as exc:
        errors.append(f"SamplingParams probe failed: {type(exc).__name__}: {exc}")

    prompt_only_scheduler_finalize = False
    try:
        scheduler_module = importlib.import_module("vllm.v1.core.sched.scheduler")
        scheduler_utils = importlib.import_module("vllm.v1.core.sched.utils")
        engine_module = importlib.import_module("vllm.v1.engine")
        request_module = importlib.import_module("vllm.v1.request")
        scheduler = getattr(scheduler_module, "Scheduler")
        prompt_only_scheduler_finalize = bool(
            callable(getattr(scheduler, "update_from_output", None))
            and callable(getattr(scheduler_utils, "remove_all", None))
            and getattr(engine_module, "EngineCoreOutput", None) is not None
            and getattr(engine_module, "EngineCoreOutputs", None) is not None
            and getattr(request_module, "RequestStatus", None) is not None
        )
        checked.append("vllm.v1.core.sched.scheduler.Scheduler.update_from_output")
    except Exception as exc:
        errors.append(
            f"prompt-only scheduler probe failed: {type(exc).__name__}: {exc}"
        )

    qwen_hooks: List[str] = []
    for module_name, class_name in _QWEN_TARGETS:
        target_name = f"{module_name}.{class_name}"
        try:
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            checked.append(target_name)
        except Exception:
            continue
        for method_name in _QWEN_METHODS:
            method = getattr(cls, method_name, None)
            if method is not None and _accepts_image_embeds(method):
                qwen_hooks.append(f"{target_name}.{method_name}")

    if not qwen_hooks:
        errors.append("no compatible Qwen-VL image_embeds hook was found")

    return VLLMCapabilityReport(
        available=True,
        version=version,
        version_supported=version_supported,
        python=sys.executable,
        gpu_model_runner_batch_mm=batch_mm,
        gpu_model_runner_update_states=update_states,
        openai_build_app=build_app,
        prompt_only_sampling_params=prompt_only,
        prompt_only_scheduler_finalize=prompt_only_scheduler_finalize,
        qwen_image_embed_hooks=tuple(qwen_hooks),
        errors=tuple(errors),
        checked_targets=tuple(checked),
    )


def required_capability_errors(report: VLLMCapabilityReport) -> Iterable[str]:
    """Return only failures that make the strict EPD path unsafe."""

    if not report.available:
        yield "vLLM is not importable"
        return
    if not report.version_supported:
        yield f"unsupported vLLM version {report.version!r}"
    if not report.gpu_model_runner_batch_mm:
        yield "GPUModelRunner._batch_mm_inputs_from_scheduler is incompatible"
    if not report.gpu_model_runner_update_states:
        yield "GPUModelRunner._update_states is incompatible"
    if not report.openai_build_app:
        yield "OpenAI build_app hook is unavailable"
    if not report.prompt_only_sampling_params:
        yield "SamplingParams hook is unavailable"
    if not report.prompt_only_scheduler_finalize:
        yield "prompt-only Scheduler finalization hook is unavailable"
    if not report.qwen_image_embed_hooks:
        yield "no Qwen-VL image_embeds hook is compatible"
