"""Vision-only native-vLLM encoders for EPD FeatureHandle production.

The E-stage must emit the exact post-vision tensor ABI consumed by the vLLM
Prefill worker.  For Qwen3-VL, a Transformers vision tower and vLLM's vision
tower use the same checkpoint but may select different attention kernels.  A
small numerical difference in the packed DeepStack tensor can change a greedy
continuation, which makes an EPD semantic comparison needlessly ambiguous.

This module loads *only* vLLM's Qwen3 vision tower and its ``model.visual.*``
checkpoint tensors.  It deliberately does not construct the language model,
so it keeps the E-stage memory footprint bounded to the vision tower while
producing vLLM's native packed visual representation.  The implementation is
isolated to the Encoder service process; it never mutates a running Prefill or
Decode vLLM process.
"""

from __future__ import annotations

import json
import logging
import socket
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

import torch

from .epd_workers import EncoderOutput
from .state.feature_store import FeatureBundle, hash_pixel_values


logger = logging.getLogger(__name__)


class VLLMNativeVisionEncoderError(RuntimeError):
    """Raised when the isolated native-vLLM vision runtime cannot start."""


def split_qwen3_vllm_packed_visual_output(
    packed: torch.Tensor,
    *,
    visual_dim: int,
    deepstack_layers: Sequence[int],
) -> tuple[torch.Tensor, tuple[tuple[int, torch.Tensor], ...]]:
    """Split Qwen3-VL's packed vLLM visual ABI into FeatureBundle tensors.

    vLLM emits one ``[visual_dim * (1 + num_deepstack),]`` token vector.  The
    Mooncake data plane intentionally transports its main and DeepStack pieces
    separately so Prefill can materialize each target tensor directly, then
    the existing FeatureHandle provider repacks them for vLLM's
    ``image_embeds`` branch.
    """

    if not isinstance(packed, torch.Tensor) or packed.ndim != 2:
        raise VLLMNativeVisionEncoderError(
            "Qwen3 native vLLM visual output must be a rank-2 tensor"
        )
    width = max(1, int(visual_dim))
    layers = tuple(int(layer) for layer in deepstack_layers)
    expected_width = width * (1 + len(layers))
    if int(packed.shape[-1]) != expected_width:
        raise VLLMNativeVisionEncoderError(
            "Qwen3 native vLLM visual output width mismatch: "
            f"got={int(packed.shape[-1])} expected={expected_width}"
        )
    pieces = packed.split(width, dim=-1)
    # ``Tensor.split`` returns strided views of the packed output.  Direct
    # FeatureBuffer publication requires independently addressable contiguous
    # tensors, so make that ownership boundary explicit here.
    main = pieces[0].contiguous()
    deepstack = tuple(
        (layer, piece.contiguous())
        for layer, piece in zip(layers, pieces[1:])
    )
    return main, deepstack


def _dtype_from_name(value: str) -> torch.dtype:
    normalized = str(value or "bfloat16").strip().lower().replace("torch.", "")
    aliases = {
        "bfloat16": torch.bfloat16,
        "bf16": torch.bfloat16,
        "float16": torch.float16,
        "half": torch.float16,
        "float32": torch.float32,
        "float": torch.float32,
    }
    dtype = aliases.get(normalized)
    if dtype is None:
        raise VLLMNativeVisionEncoderError(
            f"unsupported vLLM native vision dtype: {value!r}"
        )
    return dtype


def _reserve_loopback_init_method() -> str:
    """Return a local single-rank rendezvous endpoint for the E-stage process."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        port = int(listener.getsockname()[1])
    return f"tcp://127.0.0.1:{port}"


def _component_fingerprint(component: Any) -> str:
    if component is None:
        return ""
    config = getattr(component, "config", None)
    name = (
        getattr(component, "name_or_path", None)
        or getattr(config, "name_or_path", None)
        or getattr(config, "_name_or_path", None)
        or component.__class__.__name__
    )
    return "|".join(
        str(part)
        for part in (
            component.__class__.__name__,
            name,
            getattr(config, "model_type", "") if config is not None else "",
        )
        if str(part)
    )


class VLLMNativeQwen3VisionEncoder:
    """A vision-only Qwen3-VL E-stage backed by vLLM 0.23's native tower.

    The vLLM vision module assumes a model-parallel context even at TP=1.  An
    Encoder service is a dedicated process, so creating an isolated one-rank
    Gloo group is safer than sharing or modifying the Prefill/Decode groups.
    """

    runtime_name = "vllm_native_qwen3_vl"
    _WEIGHT_PREFIX = "model.visual."

    def __init__(
        self,
        model_path: str | Path,
        processor: Any,
        *,
        device: torch.device | str,
        dtype: str = "bfloat16",
    ) -> None:
        self.model_path = Path(model_path).expanduser().resolve()
        self.processor = processor
        self.device = torch.device(device)
        self.dtype = _dtype_from_name(dtype)
        self._closed = False
        self._distributed_initialized = False
        self._model_parallel_initialized = False
        self._vllm_config: Any = None
        self._visual: Any = None
        self._visual_dim = 0
        self._deepstack_layers: tuple[int, ...] = ()
        self._vllm_version = ""

        if self.device.type != "cuda" or not torch.cuda.is_available():
            raise VLLMNativeVisionEncoderError(
                "vllm_native encoder runtime requires an available CUDA device"
            )
        if not self.model_path.is_dir():
            raise VLLMNativeVisionEncoderError(
                f"native vLLM encoder model directory is unavailable: {self.model_path}"
            )
        self._load()

    @contextmanager
    def _vllm_context(self) -> Iterator[None]:
        if self._vllm_config is None:
            raise VLLMNativeVisionEncoderError("vLLM native vision config is not initialized")
        from vllm.config import set_current_vllm_config

        with set_current_vllm_config(self._vllm_config):
            yield

    def _load(self) -> None:
        try:
            from transformers import AutoConfig
            import vllm
            from vllm.config import VllmConfig
            from vllm.distributed.parallel_state import (
                init_distributed_environment,
                initialize_model_parallel,
            )
            from vllm.model_executor.models.qwen3_vl import Qwen3_VisionTransformer
        except Exception as exc:  # pragma: no cover - exercised on deployment.
            raise VLLMNativeVisionEncoderError(
                "vllm_native Qwen3 vision runtime requires vLLM and Transformers"
            ) from exc

        if torch.distributed.is_initialized():
            raise VLLMNativeVisionEncoderError(
                "vllm_native Qwen3 vision runtime requires an isolated Encoder process; "
                "torch.distributed is already initialized"
            )

        config = AutoConfig.from_pretrained(
            self.model_path,
            local_files_only=True,
            trust_remote_code=False,
        )
        if str(getattr(config, "model_type", "")).lower() != "qwen3_vl":
            raise VLLMNativeVisionEncoderError(
                "vllm_native runtime currently supports only model_type=qwen3_vl; "
                f"got {getattr(config, 'model_type', None)!r}"
            )
        vision_config = getattr(config, "vision_config", None)
        if vision_config is None:
            raise VLLMNativeVisionEncoderError("Qwen3-VL checkpoint has no vision_config")

        self._vllm_config = VllmConfig()
        old_dtype = torch.get_default_dtype()
        try:
            torch.cuda.set_device(self.device)
            torch.set_default_dtype(self.dtype)
            with self._vllm_context():
                init_distributed_environment(
                    world_size=1,
                    rank=0,
                    distributed_init_method=_reserve_loopback_init_method(),
                    local_rank=0,
                    backend="gloo",
                )
                self._distributed_initialized = True
                initialize_model_parallel(
                    tensor_model_parallel_size=1,
                    pipeline_model_parallel_size=1,
                    prefill_context_model_parallel_size=1,
                    decode_context_model_parallel_size=1,
                    backend="gloo",
                )
                self._model_parallel_initialized = True
                with torch.device(self.device):
                    self._visual = Qwen3_VisionTransformer(
                        vision_config,
                        norm_eps=float(getattr(config, "rms_norm_eps", 1e-6)),
                        prefix="visual",
                    )
                self._load_visual_weights()
                self._visual.eval()
        except Exception:
            self.close()
            raise
        finally:
            torch.set_default_dtype(old_dtype)

        self._visual_dim = int(getattr(vision_config, "out_hidden_size", 0) or 0)
        self._deepstack_layers = tuple(
            int(layer)
            for layer in (getattr(vision_config, "deepstack_visual_indexes", ()) or ())
        )
        if self._visual_dim <= 0:
            self.close()
            raise VLLMNativeVisionEncoderError("Qwen3-VL vision_config.out_hidden_size is invalid")
        self._vllm_version = str(getattr(vllm, "__version__", "unknown"))

    def _load_visual_weights(self) -> None:
        try:
            from safetensors import safe_open
        except Exception as exc:  # pragma: no cover - dependency is part of vLLM installs.
            raise VLLMNativeVisionEncoderError(
                "vllm_native Qwen3 vision runtime requires safetensors"
            ) from exc

        index_path = self.model_path / "model.safetensors.index.json"
        if not index_path.is_file():
            raise VLLMNativeVisionEncoderError(
                "vllm_native Qwen3 vision runtime requires model.safetensors.index.json"
            )
        try:
            raw_index = json.loads(index_path.read_text(encoding="utf-8"))
            weight_map = dict(raw_index.get("weight_map") or {})
        except Exception as exc:
            raise VLLMNativeVisionEncoderError(
                f"cannot parse Qwen3 safetensors index: {index_path}"
            ) from exc

        visual_keys = sorted(
            key for key in weight_map if str(key).startswith(self._WEIGHT_PREFIX)
        )
        if not visual_keys:
            raise VLLMNativeVisionEncoderError(
                "Qwen3 safetensors index contains no model.visual.* weights"
            )
        expected = set(dict(self._visual.named_parameters(remove_duplicate=False)))
        loaded: set[str] = set()
        for shard_name in sorted({str(weight_map[key]) for key in visual_keys}):
            shard_path = (self.model_path / shard_name).resolve()
            if self.model_path not in shard_path.parents or not shard_path.is_file():
                raise VLLMNativeVisionEncoderError(
                    f"invalid Qwen3 visual safetensors shard: {shard_name!r}"
                )
            with safe_open(shard_path, framework="pt", device="cpu") as handle:
                pairs = [
                    (
                        key.removeprefix(self._WEIGHT_PREFIX),
                        handle.get_tensor(key),
                    )
                    for key in visual_keys
                    if str(weight_map[key]) == shard_name
                ]
                loaded.update(self._visual.load_weights(pairs))
        missing = sorted(expected - loaded)
        unexpected = sorted(loaded - expected)
        if missing or unexpected:
            raise VLLMNativeVisionEncoderError(
                "native vLLM Qwen3 visual weight coverage mismatch: "
                f"missing={missing[:8]} unexpected={unexpected[:8]}"
            )

    def encode(
        self,
        pixel_values: torch.Tensor,
        image_grid_thw: torch.Tensor,
        image_id: str | None = None,
    ) -> EncoderOutput:
        if self._closed or self._visual is None:
            raise VLLMNativeVisionEncoderError("native vLLM Qwen3 vision runtime is closed")
        started = time.perf_counter()
        with self._vllm_context(), torch.inference_mode():
            packed = self._visual(
                pixel_values.to(self.device, non_blocking=True),
                image_grid_thw.to(self.device, non_blocking=True),
            )
        main, deepstack = split_qwen3_vllm_packed_visual_output(
            packed,
            visual_dim=self._visual_dim,
            deepstack_layers=self._deepstack_layers,
        )
        grid = image_grid_thw.to(self.device, non_blocking=True).contiguous()
        image_hash = image_id or hash_pixel_values(pixel_values)
        bundle = FeatureBundle(
            image_hash=image_hash,
            last_hidden=main,
            intermediates=list(deepstack),
            grid_thw=grid,
            metadata={
                "kind": "qwen_vl_hidden_state",
                "encoder_runtime": self.runtime_name,
                "vllm_visual_abi": "qwen3_vl_packed_deepstack_v1",
                "vllm_version": self._vllm_version,
                "model_fingerprint": (
                    f"vllm-native-qwen3|{self.model_path}|{self.dtype}|{self._vllm_version}"
                ),
                "processor_fingerprint": _component_fingerprint(self.processor),
                "last_hidden_shape": tuple(int(dim) for dim in main.shape),
                "last_hidden_dtype": str(main.dtype).replace("torch.", ""),
                "deepstack_layers": [layer for layer, _ in deepstack],
                "deepstack_shapes": [
                    tuple(int(dim) for dim in tensor.shape)
                    for _, tensor in deepstack
                ],
                "grid_thw_shape": tuple(int(dim) for dim in grid.shape),
            },
        )
        return EncoderOutput(
            bundle=bundle,
            encode_time_ms=(time.perf_counter() - started) * 1000.0,
            image_id=image_hash,
        )

    def close(self) -> None:
        """Release this process's private vLLM distributed state exactly once."""

        if self._closed:
            return
        self._closed = True
        self._visual = None
        try:
            from vllm.distributed.parallel_state import (
                destroy_distributed_environment,
                destroy_model_parallel,
            )

            if self._model_parallel_initialized:
                destroy_model_parallel()
            if self._distributed_initialized:
                destroy_distributed_environment()
        except Exception:
            logger.debug("native vLLM Qwen3 vision runtime teardown skipped", exc_info=True)
        finally:
            self._model_parallel_initialized = False
            self._distributed_initialized = False
