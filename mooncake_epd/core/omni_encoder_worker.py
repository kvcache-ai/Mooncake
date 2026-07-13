"""Qwen2.5-Omni encoder worker with hidden-state prefix caching.

This is the production E-stage adapter for Omni hidden-state reuse.  It encodes
all images in a multimodal segment as one exact hidden segment by default,
matching the real-GPU finding that Qwen2.5-Omni image features are not safely
item-independent for `[A,B] -> [A,C]` partial reuse.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import torch
from PIL import Image

from .epd_workers import _component_fingerprint
from .state.feature_store import FeatureBundle
from .state.omni_hidden_prefix_cache import (
    OmniHiddenPrefixCache,
    OmniHiddenPrefixCacheConfig,
    install_qwen2_5_omni_hidden_prefix_cache,
)


@dataclass
class OmniEncoderOutput:
    bundle: FeatureBundle
    encode_time_ms: float
    image_id: str


@dataclass
class OmniBatchEncoderOutput:
    outputs: List[OmniEncoderOutput]
    encode_time_ms: float
    cache_stats: Dict[str, Any]


class Qwen25OmniImageEncoderWorker:
    """Runs Qwen2.5-Omni thinker image encoder with exact hidden-prefix cache."""

    def __init__(
        self,
        model: Any,
        processor: Any,
        device: torch.device,
        *,
        cache: Optional[OmniHiddenPrefixCache] = None,
        enable_hidden_prefix_cache: bool = True,
        allow_partial_prefix_reuse: bool = False,
        cache_metrics_path: Optional[str] = None,
    ):
        self.model = model
        self.processor = processor
        self.device = torch.device(device)
        self.model_fingerprint = _component_fingerprint(model)
        self.processor_fingerprint = _component_fingerprint(processor)
        self.cache = cache
        if self.cache is None and enable_hidden_prefix_cache:
            self.cache = OmniHiddenPrefixCache(
                OmniHiddenPrefixCacheConfig(
                    enabled=True,
                    metrics_path=cache_metrics_path,
                    allow_partial_prefix_reuse=bool(allow_partial_prefix_reuse),
                )
            )
        if self.cache is not None:
            install_qwen2_5_omni_hidden_prefix_cache(self.model, self.cache)

    @property
    def cache_stats(self) -> Dict[str, Any]:
        return dict(self.cache.stats) if self.cache is not None else {"enabled": False}

    def encode_images(
        self,
        images: Sequence[Image.Image],
        *,
        image_ids: Sequence[str],
        prompt: str = "Describe the image.",
    ) -> OmniBatchEncoderOutput:
        if len(images) != len(image_ids):
            raise ValueError(f"images/image_ids length mismatch: {len(images)} != {len(image_ids)}")
        if not images:
            return OmniBatchEncoderOutput(outputs=[], encode_time_ms=0.0, cache_stats=self.cache_stats)
        inputs = self._processor_inputs(images, prompt)
        if "pixel_values" not in inputs or "image_grid_thw" not in inputs:
            raise RuntimeError(f"processor did not produce pixel_values/image_grid_thw; keys={list(inputs)}")
        pixel_values = inputs["pixel_values"].to(self.device)
        image_grid_thw = inputs["image_grid_thw"].to(self.device)
        if int(image_grid_thw.shape[0]) != len(images):
            raise RuntimeError(
                f"image_grid_thw rows mismatch: grid_rows={int(image_grid_thw.shape[0])} images={len(images)}"
            )

        t0 = time.perf_counter()
        with torch.no_grad():
            encoded = self.model.get_image_features(
                pixel_values,
                image_grid_thw=image_grid_thw,
                return_dict=True,
            )
            hidden = getattr(encoded, "pooler_output", None)
        if hidden is None:
            raise RuntimeError("Qwen2.5-Omni get_image_features did not return pooler_output")
        encode_ms = (time.perf_counter() - t0) * 1000.0

        output_sizes = self._output_sizes(image_grid_thw)
        if sum(output_sizes) != int(hidden.shape[0]):
            raise RuntimeError(
                "hidden split mismatch: "
                f"sum(output_sizes)={sum(output_sizes)} hidden_tokens={int(hidden.shape[0])} "
                f"grid={image_grid_thw.detach().cpu().tolist()}"
            )
        chunks = list(torch.split(hidden, output_sizes, dim=0))
        outputs: List[OmniEncoderOutput] = []
        for index, (image_id, chunk) in enumerate(zip(image_ids, chunks)):
            bundle = FeatureBundle(
                image_hash=str(image_id),
                last_hidden=chunk.detach().cpu().contiguous(),
                intermediates=[],
                grid_thw=image_grid_thw[index : index + 1].detach().cpu().contiguous(),
                metadata={
                    "kind": "qwen2_5_omni_image_hidden_state",
                    "model_fingerprint": self.model_fingerprint,
                    "processor_fingerprint": self.processor_fingerprint,
                    "source_segment_size": len(images),
                    "source_index": index,
                    "last_hidden_shape": tuple(int(dim) for dim in chunk.shape),
                    "last_hidden_dtype": str(chunk.dtype).replace("torch.", ""),
                    "grid_thw": image_grid_thw[index : index + 1].detach().cpu().tolist(),
                    "hidden_prefix_cache": self.cache_stats,
                },
            )
            outputs.append(OmniEncoderOutput(bundle=bundle, encode_time_ms=encode_ms, image_id=str(image_id)))
        return OmniBatchEncoderOutput(outputs=outputs, encode_time_ms=encode_ms, cache_stats=self.cache_stats)

    def encode(
        self,
        pixel_values: torch.Tensor,
        image_grid_thw: torch.Tensor,
        image_id: Optional[str] = None,
    ) -> OmniEncoderOutput:
        """Compatibility path for single preprocessed-image callers."""

        pixel_values = pixel_values.to(self.device)
        image_grid_thw = image_grid_thw.to(self.device)
        t0 = time.perf_counter()
        with torch.no_grad():
            encoded = self.model.get_image_features(
                pixel_values,
                image_grid_thw=image_grid_thw,
                return_dict=True,
            )
            hidden = getattr(encoded, "pooler_output", None)
        if hidden is None:
            raise RuntimeError("Qwen2.5-Omni get_image_features did not return pooler_output")
        encode_ms = (time.perf_counter() - t0) * 1000.0
        feature_id = image_id or "omni-image"
        bundle = FeatureBundle(
            image_hash=feature_id,
            last_hidden=hidden.detach().cpu().contiguous(),
            intermediates=[],
            grid_thw=image_grid_thw.detach().cpu().contiguous(),
            metadata={
                "kind": "qwen2_5_omni_image_hidden_state",
                "model_fingerprint": self.model_fingerprint,
                "processor_fingerprint": self.processor_fingerprint,
                "last_hidden_shape": tuple(int(dim) for dim in hidden.shape),
                "last_hidden_dtype": str(hidden.dtype).replace("torch.", ""),
                "hidden_prefix_cache": self.cache_stats,
            },
        )
        return OmniEncoderOutput(bundle=bundle, encode_time_ms=encode_ms, image_id=feature_id)

    def _processor_inputs(self, images: Sequence[Image.Image], prompt: str) -> Dict[str, torch.Tensor]:
        content: List[Dict[str, Any]] = [{"type": "image", "image": image.convert("RGB")} for image in images]
        content.append({"type": "text", "text": prompt or "Describe the image."})
        messages = [{"role": "user", "content": content}]
        # Qwen2.5-Omni processor uses text=[chat_template], images=[PIL...].
        if callable(self.processor) and hasattr(self.processor, "apply_chat_template"):
            text = self.processor.apply_chat_template(messages, add_generation_prompt=True, tokenize=False)
            return self.processor(text=[text], images=list(images), return_tensors="pt", padding=True)
        # Test doubles may implement apply_chat_template returning tensors directly.
        if hasattr(self.processor, "apply_chat_template"):
            return self.processor.apply_chat_template(
                messages,
                tokenize=True,
                add_generation_prompt=True,
                return_dict=True,
                return_tensors="pt",
            )
        raise TypeError("processor must be callable or expose apply_chat_template")

    def _output_sizes(self, image_grid_thw: torch.Tensor) -> List[int]:
        visual = getattr(self.model, "visual", None)
        merge = int(getattr(visual, "spatial_merge_size", 1) or 1)
        denom = max(1, merge * merge)
        return [max(1, int(x) // denom) for x in image_grid_thw.prod(-1).detach().cpu().tolist()]
