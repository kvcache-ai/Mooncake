"""EPD workers for Qwen3-VL with real DeepStack-aware execution."""

from __future__ import annotations

import inspect
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import torch

from .state.feature_store import FeatureBundle, FeatureBundleDescriptor, hash_pixel_values
from .state.feature_handle import FeatureHandle, FeatureHandleRegistry
from .state.mm_store import MMStore, MMStoreHandle
from .state.page_manager import BlockRef, PagedKVManager
from .transfer.engine import TransferEngine
from .transfer.policy import Channel, Mode, TransferPolicy


def _component_fingerprint(component) -> str:
    """Return a stable-enough runtime fingerprint for model/processor compatibility checks."""

    if component is None:
        return ""
    config = getattr(component, "config", None)
    name = (
        getattr(component, "name_or_path", None)
        or getattr(config, "name_or_path", None)
        or getattr(config, "_name_or_path", None)
        or component.__class__.__name__
    )
    cls = component.__class__.__name__
    model_type = getattr(config, "model_type", "") if config is not None else ""
    torch_dtype = getattr(config, "torch_dtype", "") if config is not None else ""
    return "|".join(str(part) for part in (cls, name, model_type, torch_dtype) if str(part))


def _optional_to_device(tensor: Optional[torch.Tensor], device: torch.device) -> Optional[torch.Tensor]:
    return tensor.to(device) if tensor is not None else None


def _call_with_supported_kwargs(fn, /, **kwargs):
    """Call a HF/vLLM hook while tolerating version-specific keyword drift."""

    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return fn(**{key: value for key, value in kwargs.items() if value is not None})
    if any(param.kind is inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values()):
        accepted = {key: value for key, value in kwargs.items() if value is not None}
    else:
        accepted = {
            key: value
            for key, value in kwargs.items()
            if key in signature.parameters and value is not None
        }
    return fn(**accepted)


@dataclass
class EncoderOutput:
    bundle: FeatureBundle
    encode_time_ms: float
    image_id: str


class EncoderWorker:
    """Runs the Qwen3-VL vision tower on a dedicated GPU."""

    def __init__(
        self,
        model,
        processor,
        device: torch.device,
        transfer: Optional[TransferEngine] = None,
    ):
        self.model = model
        self.processor = processor
        self.device = device
        self.transfer = transfer
        self.model_fingerprint = _component_fingerprint(model)
        self.processor_fingerprint = _component_fingerprint(processor)
        self.deepstack_indices: List[int] = list(
            model.config.vision_config.deepstack_visual_indexes
        )

    def encode(
        self,
        pixel_values: torch.Tensor,
        image_grid_thw: torch.Tensor,
        image_id: Optional[str] = None,
    ) -> EncoderOutput:
        pixel_values = pixel_values.to(self.device)
        image_grid_thw = image_grid_thw.to(self.device)
        t0 = time.perf_counter()
        with torch.no_grad():
            vision_output = self.model.model.get_image_features(
                pixel_values, image_grid_thw=image_grid_thw
            )

        if isinstance(vision_output, tuple):
            image_embeds = vision_output[0]
            deepstack_image_embeds = vision_output[1] if len(vision_output) > 1 else None
        else:
            image_embeds = getattr(vision_output, "pooler_output", vision_output)
            # Transformers Qwen3-VL returns a
            # BaseModelOutputWithDeepstackFeatures where the field is named
            # ``deepstack_features``.  Older local tests / prototype adapters
            # used ``deepstack_hidden_states``.  Prefer the upstream field but
            # keep the legacy fallback so the encoder emits the full packed
            # Qwen3-VL visual state instead of only the 4096-d main hidden
            # state.  vLLM's Qwen3-VL fast path later splits the packed tensor
            # into [visual_dim, num_deepstack_layers * visual_dim].
            deepstack_image_embeds = getattr(vision_output, "deepstack_features", None)
            if deepstack_image_embeds is None:
                deepstack_image_embeds = getattr(vision_output, "deepstack_hidden_states", None)

        flat_embeds = (
            torch.cat(image_embeds, dim=0)
            if isinstance(image_embeds, (list, tuple))
            else image_embeds
        )
        intermediates: List[Tuple[int, torch.Tensor]] = []
        if deepstack_image_embeds is not None:
            for idx, feat in zip(self.deepstack_indices, deepstack_image_embeds):
                intermediates.append((idx, feat))

        image_hash = image_id or hash_pixel_values(pixel_values)
        bundle = FeatureBundle(
            image_hash=image_hash,
            last_hidden=flat_embeds,
            intermediates=intermediates,
            grid_thw=image_grid_thw,
            metadata={
                "kind": "qwen_vl_hidden_state",
                "model_fingerprint": self.model_fingerprint,
                "processor_fingerprint": self.processor_fingerprint,
                "num_images": len(image_embeds) if isinstance(image_embeds, (list, tuple)) else 1,
                "split_sizes": [
                    int(embed.shape[0]) for embed in image_embeds
                ] if isinstance(image_embeds, (list, tuple)) else [int(flat_embeds.shape[0])],
                "last_hidden_shape": tuple(int(dim) for dim in flat_embeds.shape),
                "last_hidden_dtype": str(flat_embeds.dtype).replace("torch.", ""),
                "deepstack_layers": [int(idx) for idx, _ in intermediates],
                "deepstack_shapes": [tuple(int(dim) for dim in feat.shape) for _, feat in intermediates],
                "grid_thw_shape": tuple(int(dim) for dim in image_grid_thw.shape),
            },
        )
        encode_ms = (time.perf_counter() - t0) * 1000
        return EncoderOutput(bundle=bundle, encode_time_ms=encode_ms, image_id=image_hash)

    def encode_and_prefetch(
        self,
        pixel_values: torch.Tensor,
        image_grid_thw: torch.Tensor,
        *,
        mm_store: MMStore,
        target_worker_id: str,
        target_device: torch.device,
        image_id: Optional[str] = None,
        policy: Optional[TransferPolicy] = None,
    ) -> Tuple[EncoderOutput, MMStoreHandle]:
        out = self.encode(
            pixel_values=pixel_values,
            image_grid_thw=image_grid_thw,
            image_id=image_id,
        )
        mm_store.publish(out.bundle)
        handle = mm_store.prefetch(
            out.bundle.image_hash,
            target_worker_id=target_worker_id,
            target_device=target_device,
            policy=policy
            or TransferPolicy(
                mode=Mode.PULL,
                channel=Channel.ENCODER_TO_PREFILL,
                extra={"force_copy": True},
            ),
        )
        return out, handle

    def encode_and_publish_handle(
        self,
        pixel_values: torch.Tensor,
        image_grid_thw: torch.Tensor,
        *,
        feature_registry: FeatureHandleRegistry,
        image_id: Optional[str] = None,
        ttl_seconds: Optional[float] = None,
        checksum: bool = False,
        metadata: Optional[dict] = None,
    ) -> Tuple[EncoderOutput, FeatureHandle]:
        """Encode a real image tensor and publish a control-plane FeatureHandle."""

        out = self.encode(
            pixel_values=pixel_values,
            image_grid_thw=image_grid_thw,
            image_id=image_id,
        )
        handle = feature_registry.publish_bundle(
            out.bundle,
            ttl_seconds=ttl_seconds,
            checksum=checksum,
            metadata=metadata,
        )
        return out, handle


@dataclass
class PrefillOutput:
    kv_refs: List[BlockRef]
    first_logits: torch.Tensor
    token_ids: torch.Tensor
    prefill_time_ms: float
    rope_deltas: Optional[torch.Tensor] = None


class PrefillWorker:
    """Runs one multimodal/text prefill step on the prefill GPU."""

    def __init__(
        self,
        model,
        processor,
        device: torch.device,
        page_manager: PagedKVManager,
        transfer: Optional[TransferEngine] = None,
    ):
        self.model = model
        self.processor = processor
        self.device = device
        self.pm = page_manager
        self.transfer = transfer
        self.model_fingerprint = _component_fingerprint(model)
        self.processor_fingerprint = _component_fingerprint(processor)

    @staticmethod
    def _build_incremental_mrope_position_ids(
        *,
        batch_size: int,
        seq_len: int,
        past_seq_len: int,
        rope_deltas: torch.Tensor,
        device: torch.device,
    ) -> torch.Tensor:
        """Build delta-only 3D M-RoPE positions for incremental multimodal prefill.

        Qwen3-VL's current HF implementation expects the incremental path to reuse
        cached ``rope_deltas`` and then derive delta positions from the *delta*
        sequence length. Passing a full-length attention mask together with
        delta-only ``input_ids`` can produce a shape mismatch between query
        states and rotary positions. Constructing the delta-only position ids
        explicitly keeps the path aligned with the model's true incremental
        semantics.
        """
        if rope_deltas.ndim == 2 and rope_deltas.shape[-1] == 1:
            rope_deltas = rope_deltas[:, 0]
        rope_deltas = rope_deltas.to(device=device, dtype=torch.long).reshape(-1)
        if rope_deltas.numel() == 1 and batch_size > 1:
            rope_deltas = rope_deltas.expand(batch_size)
        if rope_deltas.numel() != batch_size:
            raise ValueError(
                f"rope_deltas batch mismatch: got {rope_deltas.numel()} entries for batch_size={batch_size}"
            )
        base = torch.arange(
            past_seq_len,
            past_seq_len + seq_len,
            device=device,
            dtype=torch.long,
        ).view(1, 1, seq_len).expand(3, batch_size, seq_len)
        delta = rope_deltas.view(1, batch_size, 1)
        return base + delta

    def prefill(
        self,
        input_ids: torch.Tensor,
        bundle: Optional[FeatureBundle],
        attention_mask: Optional[torch.Tensor] = None,
        pixel_values: Optional[torch.Tensor] = None,
        image_grid_thw: Optional[torch.Tensor] = None,
        prefix_kv_refs: Optional[List[BlockRef]] = None,
        prefix_rope_deltas: Optional[torch.Tensor] = None,
        mm_token_type_ids: Optional[torch.Tensor] = None,
        feature_descriptor: Optional[FeatureBundleDescriptor] = None,
    ) -> PrefillOutput:
        from transformers.cache_utils import DynamicCache

        input_ids = input_ids.to(self.device)
        if attention_mask is not None:
            attention_mask = attention_mask.to(self.device)
        mm_token_type_ids = _optional_to_device(mm_token_type_ids, self.device)

        rope_deltas = None
        t0 = time.perf_counter()
        with torch.no_grad():
            if prefix_kv_refs:
                if bundle is not None or pixel_values is not None or image_grid_thw is not None:
                    raise ValueError(
                        "incremental prefill with prefix_kv_refs does not support new multimodal "
                        "inputs in the same call; prefill the multimodal prefix first, then append text deltas"
                    )
                prefix_seq_len = int(sum(ref.filled for ref in prefix_kv_refs))
                pkv_tuple = self.pm.materialize_kv_cache(prefix_kv_refs)
                moved = [(k.to(self.device), v.to(self.device)) for k, v in pkv_tuple]
                pkv = DynamicCache(moved)
                position_ids = None
                if prefix_rope_deltas is not None:
                    self.model.model.rope_deltas = prefix_rope_deltas.to(self.device)
                    rope_deltas = self.model.model.rope_deltas
                    position_ids = self._build_incremental_mrope_position_ids(
                        batch_size=int(input_ids.shape[0]),
                        seq_len=int(input_ids.shape[-1]),
                        past_seq_len=prefix_seq_len,
                        rope_deltas=rope_deltas,
                        device=self.device,
                    )
                incremental_attention_mask = attention_mask
                if incremental_attention_mask is not None and incremental_attention_mask.shape[-1] != int(input_ids.shape[-1]):
                    incremental_attention_mask = incremental_attention_mask[:, -int(input_ids.shape[-1]) :]
                if (
                    incremental_attention_mask is not None
                    and position_ids is not None
                    and bool(torch.all(incremental_attention_mask != 0).item())
                ):
                    # Avoid the current HF/Qwen3-VL shape-mismatch path where a
                    # full-length mask is used to rebuild positions for a delta-only
                    # query. We already pass exact delta position ids above.
                    incremental_attention_mask = None
                out = _call_with_supported_kwargs(
                    self.model,
                    input_ids=input_ids,
                    attention_mask=incremental_attention_mask,
                    position_ids=position_ids,
                    past_key_values=pkv,
                    mm_token_type_ids=mm_token_type_ids,
                    use_cache=True,
                    return_dict=True,
                )
                rope_deltas = getattr(out, "rope_deltas", rope_deltas)
            elif bundle is not None:
                descriptor = feature_descriptor or bundle.descriptor(checksum=False)
                bundle.validate_against(
                    descriptor,
                    expected_model_fingerprint=(
                        self.model_fingerprint
                        if bundle.metadata.get("model_fingerprint")
                        else None
                    ),
                    expected_processor_fingerprint=(
                        self.processor_fingerprint
                        if bundle.metadata.get("processor_fingerprint")
                        else None
                    ),
                )
                inputs_embeds = self.model.model.get_input_embeddings()(input_ids)
                image_embeds = bundle.last_hidden.to(self.device, inputs_embeds.dtype)
                image_mask, _ = self.model.model.get_placeholder_mask(
                    input_ids=input_ids,
                    inputs_embeds=inputs_embeds,
                    image_features=image_embeds,
                )
                inputs_embeds = inputs_embeds.masked_scatter(image_mask, image_embeds)
                visual_pos_masks = image_mask[..., 0]
                deepstack = (
                    [feat.to(self.device, inputs_embeds.dtype) for _, feat in bundle.intermediates]
                    if bundle.intermediates else None
                )
                position_ids, rope_deltas = _call_with_supported_kwargs(
                    self.model.model.get_rope_index,
                    input_ids=input_ids,
                    image_grid_thw=bundle.grid_thw.to(self.device) if bundle.grid_thw is not None else None,
                    attention_mask=attention_mask,
                    mm_token_type_ids=mm_token_type_ids,
                )
                out = self.model.model.language_model(
                    input_ids=None,
                    position_ids=position_ids,
                    attention_mask=attention_mask,
                    inputs_embeds=inputs_embeds,
                    use_cache=True,
                    visual_pos_masks=visual_pos_masks,
                    deepstack_visual_embeds=deepstack,
                    return_dict=True,
                )
            else:
                out = _call_with_supported_kwargs(
                    self.model,
                    input_ids=input_ids,
                    attention_mask=attention_mask,
                    pixel_values=pixel_values.to(self.device) if pixel_values is not None else None,
                    image_grid_thw=image_grid_thw.to(self.device) if image_grid_thw is not None else None,
                    mm_token_type_ids=mm_token_type_ids,
                    use_cache=True,
                    return_dict=True,
                )
                rope_deltas = getattr(out, "rope_deltas", None)

        prefill_ms = (time.perf_counter() - t0) * 1000
        pkv = getattr(out, "past_key_values", None)
        if pkv is None:
            raise RuntimeError("prefill did not return past_key_values")

        kv_refs = self.pm.ingest_kv_cache(
            pkv,
            token_start=(sum(ref.filled for ref in prefix_kv_refs) if prefix_kv_refs else 0),
        )
        raw_logits = getattr(out, "logits", None)
        if raw_logits is None:
            last_hidden = out[0]
            raw_logits = self.model.lm_head(last_hidden[:, -1:, :])
        else:
            raw_logits = raw_logits[:, -1:, :]

        return PrefillOutput(
            kv_refs=kv_refs,
            first_logits=raw_logits,
            token_ids=input_ids,
            prefill_time_ms=prefill_ms,
            rope_deltas=rope_deltas,
        )

    def prefill_from_prefetch(
        self,
        *,
        input_ids: torch.Tensor,
        prefetch_handle: MMStoreHandle,
        attention_mask: Optional[torch.Tensor] = None,
        feature_descriptor: Optional[FeatureBundleDescriptor] = None,
    ) -> PrefillOutput:
        bundle = prefetch_handle.wait(timeout=30.0)
        if bundle is None:
            raise RuntimeError("prefetch did not resolve a feature bundle")
        return self.prefill(
            input_ids=input_ids,
            bundle=bundle,
            attention_mask=attention_mask,
            feature_descriptor=feature_descriptor,
        )

    def prefill_from_feature_handle(
        self,
        *,
        input_ids: torch.Tensor,
        feature_handle: FeatureHandle,
        feature_registry: FeatureHandleRegistry,
        target_worker_id: str,
        attention_mask: Optional[torch.Tensor] = None,
        mm_token_type_ids: Optional[torch.Tensor] = None,
        timeout: float = 30.0,
        policy: Optional[TransferPolicy] = None,
    ) -> PrefillOutput:
        """Resolve an E→P FeatureHandle, validate hidden states, then prefill."""

        prefetch_handle = feature_registry.prefetch(
            feature_handle,
            target_worker_id=target_worker_id,
            target_device=self.device,
            policy=policy,
        )
        bundle = feature_registry.wait_and_validate(
            feature_handle,
            prefetch_handle,
            timeout=timeout,
            expected_model_fingerprint=(
                self.model_fingerprint
                if feature_handle.descriptor.model_fingerprint
                else None
            ),
            expected_processor_fingerprint=(
                self.processor_fingerprint
                if feature_handle.descriptor.processor_fingerprint
                else None
            ),
            require_checksum=bool(feature_handle.descriptor.last_hidden.checksum),
        )
        return self.prefill(
            input_ids=input_ids,
            bundle=bundle,
            attention_mask=attention_mask,
            mm_token_type_ids=mm_token_type_ids,
            feature_descriptor=feature_handle.descriptor,
        )


@dataclass
class DecodeOutput:
    generated_ids: List[int]
    generated_text: str
    decode_time_ms: float
    tokens_per_second: float


class DecodeWorker:
    """Autoregressive token generation from a managed KV cache."""

    def __init__(
        self,
        model,
        processor,
        device: torch.device,
        page_manager: PagedKVManager,
        transfer: Optional[TransferEngine] = None,
    ):
        self.model = model
        self.processor = processor
        self.device = device
        self.pm = page_manager
        self.transfer = transfer
        self.eos_token_id = (
            int(model.config.eos_token_id)
            if hasattr(model.config, "eos_token_id") and model.config.eos_token_id is not None
            else None
        )

    def decode(
        self,
        first_logits: torch.Tensor,
        kv_refs: List[BlockRef],
        max_new_tokens: int = 64,
        temperature: float = 0.0,
        rope_deltas: Optional[torch.Tensor] = None,
    ) -> DecodeOutput:
        from transformers.cache_utils import DynamicCache

        pkv_tuple = self.pm.materialize_kv_cache(kv_refs)
        moved = [(k.to(self.device), v.to(self.device)) for k, v in pkv_tuple]
        pkv = DynamicCache(moved)

        if temperature > 0:
            probs = torch.softmax(first_logits[:, -1, :].float(), dim=-1)
            first_token = torch.multinomial(probs, num_samples=1)
        else:
            first_token = first_logits[:, -1, :].argmax(dim=-1, keepdim=True)
        first_token = first_token.to(self.device)

        if rope_deltas is not None:
            self.model.model.rope_deltas = rope_deltas.to(self.device)

        t0 = time.perf_counter()
        input_ids = first_token
        generated: List[int] = [int(first_token.item())]
        cache_position = torch.tensor([pkv.get_seq_length()], device=self.device, dtype=torch.long)
        with torch.no_grad():
            for _ in range(max_new_tokens - 1):
                out = self.model(
                    input_ids=input_ids,
                    past_key_values=pkv,
                    cache_position=cache_position,
                    use_cache=True,
                    return_dict=True,
                )
                logits = out.logits[:, -1, :]
                if temperature > 0:
                    probs = torch.softmax(logits.float(), dim=-1)
                    next_token = torch.multinomial(probs, num_samples=1)
                else:
                    next_token = logits.argmax(dim=-1, keepdim=True)

                tid = int(next_token.item())
                generated.append(tid)
                if self.eos_token_id is not None and tid == self.eos_token_id:
                    break

                input_ids = next_token
                cache_position = cache_position + 1

        decode_ms = (time.perf_counter() - t0) * 1000
        text = self.processor.decode(generated, skip_special_tokens=True)
        tps = (len(generated) - 1) / (decode_ms / 1000) if decode_ms > 0 else 0.0
        return DecodeOutput(
            generated_ids=generated,
            generated_text=text,
            decode_time_ms=decode_ms,
            tokens_per_second=tps,
        )
