#!/usr/bin/env python
"""Real Qwen2.5-Omni AR→Generation→Diffusion stage-transfer E2E.

The script validates the worker-level OmniPipeline transport in a real local
environment:

1. load the real Qwen2.5-Omni thinker model from disk;
2. run its image hidden-state boundary as the AR/conditioning stage;
3. move AR→Generation and Generation→Diffusion tensors through SHM or Mooncake
   direct peer-buffer transport; and
4. write a JSON artifact with latency, backend counts, tensor checksums and
   model metadata.

It supports three stage implementations:

* ``tensor`` keeps the original deterministic transport smoke path over real
  image hidden states.
* ``dataset_tensor`` uses real dataset images to create pickleable CPU tensors for process-isolated SHM validation.
* ``semantic`` splits the real Qwen2.5-Omni model into Thinker/AR,
  Talker/speech-code generation, and Token2Wav diffusion/vocoder stages so the
  pipeline validates semantic text+audio generation quality as well as SHM
  transport.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import torch
from PIL import Image, ImageDraw

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.omni_encoder_worker import Qwen25OmniImageEncoderWorker  # noqa: E402
from mooncake_epd.core.omni_pipeline import (  # noqa: E402
    OmniPipeline,
    OmniPipelineProcessRuntime,
    OmniPipelineRuntime,
    OmniStageWorkerSpec,
)
from mooncake_epd.core.state import FeatureBundle  # noqa: E402
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy  # noqa: E402


def _sync(device: str) -> None:
    if device.startswith("cuda") and torch.cuda.is_available():
        torch.cuda.synchronize(torch.device(device))


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _default_stage_devices() -> List[str]:
    if torch.cuda.is_available() and torch.cuda.device_count() >= 3:
        return ["cuda:0", "cuda:1", "cuda:2"]
    if torch.cuda.is_available():
        return ["cuda:0", "cuda:0", "cuda:0"]
    return ["cpu", "cpu", "cpu"]


def _make_test_image() -> Image.Image:
    img = Image.new("RGB", (384, 256), color=(28, 54, 104))
    draw = ImageDraw.Draw(img)
    draw.rectangle((24, 32, 360, 220), outline=(255, 255, 255), width=4)
    draw.ellipse((64, 72, 168, 176), fill=(240, 180, 60))
    draw.rectangle((210, 84, 324, 178), fill=(70, 170, 110))
    draw.text((40, 16), "Mooncake Omni Stage Transfer", fill=(255, 255, 255))
    return img


class RealOmniARStage:
    name = "AR"

    def __init__(
        self,
        worker: Qwen25OmniImageEncoderWorker,
        image: Optional[Image.Image],
        prompt: str,
    ):
        self.worker = worker
        self.image = image
        self.prompt = prompt

    def run(self, input_refs):
        sample = input_refs[0] if input_refs else {}
        if isinstance(sample, dict) and isinstance(sample.get("image"), Image.Image):
            image = sample["image"]
            image_id = str(sample.get("image_id") or "dataset-image")
            prompt = str(sample.get("prompt") or self.prompt)
        else:
            image = self.image or _make_test_image()
            image_id = "omni-e2e-image"
            prompt = self.prompt
        out = self.worker.encode_images([image], image_ids=[image_id], prompt=prompt)
        return [out.outputs[0].bundle]


class TensorGenerationStage:
    name = "Generation"

    def __init__(self, device: str):
        self.device = torch.device(device)

    def run(self, input_refs):
        bundle = input_refs[0]
        if not isinstance(bundle, FeatureBundle):
            raise TypeError(f"Generation expected FeatureBundle, got {type(bundle)!r}")
        hidden = bundle.last_hidden.to(self.device, non_blocking=True)
        # Deterministic "generation" kernel over real hidden states: normalize a
        # pooled context and keep enough dimensions to exercise stage transport.
        pooled = hidden.float().mean(dim=0, keepdim=True)
        cond = torch.nn.functional.normalize(pooled[:, : min(1024, pooled.shape[-1])], dim=-1)
        return [{"condition": cond.to(hidden.dtype), "tokens": torch.arange(cond.shape[-1], device=self.device)}]


class TensorDiffusionStage:
    name = "Diffusion"

    def __init__(self, device: str, steps: int = 8):
        self.device = torch.device(device)
        self.steps = max(1, int(steps))

    def run(self, input_refs):
        payload = input_refs[0]
        cond = payload["condition"].to(self.device)
        latent = torch.zeros_like(cond, device=self.device)
        for idx in range(self.steps):
            alpha = 1.0 / float(idx + 2)
            latent = latent * (1.0 - alpha) + cond * alpha
        return [{"latent": latent, "latent_norm": latent.float().norm().detach().cpu()}]


class DatasetTensorARStage:
    """Pickleable real-dataset tensor source for process-runtime SHM validation."""

    name = "AR"

    def __init__(self, image_size: int = 32):
        self.image_size = max(8, int(image_size))

    def run(self, input_refs):
        sample = input_refs[0] if input_refs else {}
        image = sample.get("image") if isinstance(sample, dict) else None
        if not isinstance(image, Image.Image):
            image = _make_test_image()
        small = image.convert("RGB").resize((self.image_size, self.image_size))
        data = torch.tensor(list(small.tobytes()), dtype=torch.float32).view(1, -1) / 255.0
        return [
            {
                "features": data.contiguous(),
                "image_size": torch.tensor(list(image.size), dtype=torch.int64),
            }
        ]


class DatasetTensorGenerationStage:
    name = "Generation"

    def __init__(self, width: int = 1024):
        self.width = max(16, int(width))

    def run(self, input_refs):
        payload = input_refs[0]
        features = payload["features"]
        if not features.is_shared():
            raise RuntimeError("dataset_tensor Generation expected CPU SHM tensor")
        repeat = (self.width + features.shape[-1] - 1) // features.shape[-1]
        cond = features.repeat(1, repeat)[:, : self.width].contiguous()
        cond = torch.nn.functional.normalize(cond, dim=-1)
        return [{"condition": cond, "image_size": payload["image_size"]}]


class DatasetTensorDiffusionStage:
    name = "Diffusion"

    def __init__(self, steps: int = 8):
        self.steps = max(1, int(steps))

    def run(self, input_refs):
        payload = input_refs[0]
        cond = payload["condition"]
        if not cond.is_shared():
            raise RuntimeError("dataset_tensor Diffusion expected CPU SHM tensor")
        latent = torch.zeros_like(cond)
        for idx in range(self.steps):
            alpha = 1.0 / float(idx + 2)
            latent = latent * (1.0 - alpha) + cond * alpha
        return [{"latent": latent, "latent_norm": latent.float().norm(), "image_size": payload["image_size"]}]


class SemanticARThinkerStage:

    """Real Qwen2.5-Omni Thinker autoregressive stage."""

    name = "AR"

    def __init__(
        self,
        model: Any,
        processor: Any,
        device: str,
        image: Optional[Image.Image],
        prompt: str,
        *,
        max_new_tokens: int,
        use_audio_in_video: bool = False,
    ):
        self.model = model
        self.processor = processor
        self.device = torch.device(device)
        self.image = image
        self.prompt = prompt
        self.max_new_tokens = max(1, int(max_new_tokens))
        self.use_audio_in_video = bool(use_audio_in_video)

    def run(self, input_refs):
        sample = input_refs[0] if input_refs else {}
        if isinstance(sample, dict) and isinstance(sample.get("image"), Image.Image):
            image = sample["image"]
            image_id = str(sample.get("image_id") or "dataset-image")
            prompt = str(sample.get("prompt") or self.prompt)
        else:
            image = self.image or _make_test_image()
            image_id = "omni-e2e-image"
            prompt = self.prompt
        inputs = _processor_inputs(self.processor, image, prompt)
        inputs = {k: (v.to(self.device) if torch.is_tensor(v) else v) for k, v in inputs.items()}
        input_ids = inputs.pop("input_ids")
        thinker_kwargs = dict(inputs)
        thinker_kwargs.update(
            {
                "max_new_tokens": self.max_new_tokens,
                "output_hidden_states": True,
                "return_dict_in_generate": True,
                "use_audio_in_video": self.use_audio_in_video,
            }
        )
        with torch.no_grad():
            thinker_result = self.model.thinker.generate(input_ids=input_ids, **thinker_kwargs)
        sequences = thinker_result.sequences
        generated_ids = sequences[:, input_ids.size(1) :]
        return [
            {
                "input_ids": input_ids,
                "attention_mask": thinker_kwargs.get("attention_mask"),
                "hidden_states": thinker_result.hidden_states,
                "sequences": sequences,
                "generated_ids": generated_ids,
                "input_length": torch.tensor([input_ids.size(1)], dtype=torch.int64, device=self.device),
                "has_audio_features": torch.tensor([int(thinker_kwargs.get("input_features") is not None)], dtype=torch.int64, device=self.device),
                "has_pixel_values": torch.tensor([int(thinker_kwargs.get("pixel_values") is not None)], dtype=torch.int64, device=self.device),
                "has_pixel_values_videos": torch.tensor([int(thinker_kwargs.get("pixel_values_videos") is not None)], dtype=torch.int64, device=self.device),
                "sample_meta": {
                    "image_id": image_id,
                    "prompt": prompt,
                },
            }
        ]


class SemanticTalkerGenerationStage:
    """Real Qwen2.5-Omni Talker speech-code generation stage."""

    name = "Generation"

    def __init__(
        self,
        model: Any,
        device: str,
        *,
        speaker: str,
        max_new_tokens: int,
        do_sample: bool,
        top_k: int,
        top_p: float,
        temperature: float,
        repetition_penalty: float,
        eos_token_id: List[int],
    ):
        self.model = model
        self.device = torch.device(device)
        self.speaker = speaker
        self.max_new_tokens = max(1, int(max_new_tokens))
        self.do_sample = bool(do_sample)
        self.top_k = int(top_k)
        self.top_p = float(top_p)
        self.temperature = float(temperature)
        self.repetition_penalty = float(repetition_penalty)
        self.eos_token_id = [int(x) for x in eos_token_id]

    def _speaker_params(self) -> Dict[str, Any]:
        if self.speaker not in self.model.speaker_map:
            raise ValueError(f"speaker {self.speaker!r} is not available; choices={list(self.model.speaker_map)}")
        return self.model.speaker_map[self.speaker]

    def _embed_tokens_on_talker_device(self, token_ids: torch.Tensor) -> torch.Tensor:
        embed = self.model.thinker.get_input_embeddings()
        embed_device = next(embed.parameters()).device
        return embed(token_ids.to(embed_device)).to(self.device)

    def run(self, input_refs):
        payload = input_refs[0]
        input_ids = payload["input_ids"].to(self.device)
        sequences = payload["sequences"].to(self.device)
        hidden_states = payload["hidden_states"]
        generated_ids = sequences[:, int(payload["input_length"].flatten()[0].item()) :]
        speaker_params = self._speaker_params()

        embeds_to_talker = hidden_states[0][0].clone().to(self.device)
        if int(payload.get("has_audio_features", torch.zeros(1)).flatten()[0].item()):
            audio_ids_mask = input_ids == self.model.config.thinker_config.audio_token_index
            embeds_to_talker.masked_scatter_(
                audio_ids_mask.unsqueeze(-1),
                torch.zeros([audio_ids_mask.sum(), embeds_to_talker.shape[-1]], dtype=embeds_to_talker.dtype, device=self.device),
            )
        if int(payload.get("has_pixel_values", torch.zeros(1)).flatten()[0].item()):
            image_ids_mask = input_ids == self.model.config.thinker_config.image_token_index
            embeds_to_talker.masked_scatter_(
                image_ids_mask.unsqueeze(-1),
                torch.zeros([image_ids_mask.sum(), embeds_to_talker.shape[-1]], dtype=embeds_to_talker.dtype, device=self.device),
            )
        if int(payload.get("has_pixel_values_videos", torch.zeros(1)).flatten()[0].item()):
            video_ids_mask = input_ids == self.model.config.thinker_config.video_token_index
            embeds_to_talker.masked_scatter_(
                video_ids_mask.unsqueeze(-1),
                torch.zeros([video_ids_mask.sum(), embeds_to_talker.shape[-1]], dtype=embeds_to_talker.dtype, device=self.device),
            )

        processed_hidden = ((embeds_to_talker,) + hidden_states[0][1:],) + hidden_states[1:]
        token_embeds = [token_hidden_states[0].to(self.device) for token_hidden_states in processed_hidden]
        final_hidden = [token_hidden_states[-1].to(self.device) for token_hidden_states in processed_hidden]

        talker_text_bos_token = int(speaker_params["bos_token"])
        talker_input_text_ids = torch.cat(
            [
                input_ids,
                torch.tensor([[talker_text_bos_token]], dtype=torch.long, device=self.device),
                generated_ids[:, :1],
            ],
            dim=-1,
        )
        talker_input_ids = torch.cat(
            [
                torch.full_like(input_ids, fill_value=self.model.talker.codec_mask_token),
                torch.tensor([[self.model.talker.codec_pad_token]], dtype=torch.long, device=self.device),
                torch.tensor([[self.model.talker.codec_bos_token]], dtype=torch.long, device=self.device),
            ],
            dim=1,
        )

        thinker_reply_part = torch.cat(final_hidden[1:], dim=1) + torch.cat(token_embeds[1:], dim=1)
        talker_inputs_embeds = final_hidden[0] + token_embeds[0]
        bos_embed = self._embed_tokens_on_talker_device(torch.tensor([[talker_text_bos_token]], dtype=torch.long))
        talker_inputs_embeds = torch.cat([talker_inputs_embeds, bos_embed, thinker_reply_part[:, :1, :]], dim=1)

        eos_embed = self._embed_tokens_on_talker_device(torch.tensor([[self.model.talker.text_eos_token]], dtype=torch.long))
        pad_embed = self._embed_tokens_on_talker_device(torch.tensor([[self.model.talker.text_pad_token]], dtype=torch.long))
        thinker_reply_part = torch.cat([thinker_reply_part[:, 1:, :], eos_embed, pad_embed], dim=1)

        attention_mask = payload.get("attention_mask")
        talker_attention_mask = None
        if torch.is_tensor(attention_mask):
            talker_attention_mask = torch.cat([attention_mask.to(self.device), attention_mask.new_ones((1, 2)).to(self.device)], dim=1)

        with torch.no_grad():
            talker_result = self.model.talker.generate(
                input_ids=talker_input_ids,
                input_text_ids=talker_input_text_ids,
                thinker_reply_part=thinker_reply_part,
                inputs_embeds=talker_inputs_embeds,
                attention_mask=talker_attention_mask,
                suppress_tokens=[self.model.talker.codec_bos_token],
                max_new_tokens=self.max_new_tokens,
                do_sample=self.do_sample,
                top_k=self.top_k,
                top_p=self.top_p,
                temperature=self.temperature,
                eos_token_id=self.eos_token_id,
                repetition_penalty=self.repetition_penalty,
            )
        codes = talker_result[:, talker_input_ids.shape[1] : -1]
        return [
            {
                "sequences": sequences,
                "input_length": payload["input_length"],
                "talker_codes": codes,
                "speaker_conditioning": speaker_params["cond"].to(self.device).float(),
                "speaker_reference_mel": speaker_params["ref_mel"].to(self.device).float(),
                "sample_meta": payload.get("sample_meta", {}),
            }
        ]


class SemanticToken2WavDiffusionStage:
    """Real Qwen2.5-Omni Token2Wav diffusion/vocoder stage."""

    name = "Diffusion"

    def __init__(self, model: Any, device: str, *, num_steps: int, guidance_scale: float, sway_coefficient: float):
        self.model = model
        self.device = torch.device(device)
        self.num_steps = max(1, int(num_steps))
        self.guidance_scale = float(guidance_scale)
        self.sway_coefficient = float(sway_coefficient)

    def run(self, input_refs):
        payload = input_refs[0]
        token2wav = self.model.token2wav
        if token2wav.dtype != torch.float:
            token2wav.float()
        codes = payload["talker_codes"].to(self.device)
        conditioning = payload["speaker_conditioning"].to(self.device).float()
        reference_mel = payload["speaker_reference_mel"].to(self.device).float()
        with torch.no_grad():
            waveform = token2wav(
                codes,
                conditioning=conditioning,
                reference_mel=reference_mel,
                num_steps=self.num_steps,
                guidance_scale=self.guidance_scale,
                sway_coefficient=self.sway_coefficient,
            ).float()
        return [
            {
                "sequences": payload["sequences"],
                "input_length": payload["input_length"],
                "talker_codes": codes,
                "waveform": waveform.detach(),
                "waveform_abs_mean": waveform.detach().float().abs().mean().cpu(),
                "waveform_rms": waveform.detach().float().pow(2).mean().sqrt().cpu(),
                "sample_meta": payload.get("sample_meta", {}),
            }
        ]


def _processor_inputs(processor: Any, image: Image.Image, prompt: str) -> Dict[str, torch.Tensor]:
    messages = [{"role": "user", "content": [{"type": "image", "image": image.convert("RGB")}, {"type": "text", "text": prompt}]}]
    text = processor.apply_chat_template(messages, add_generation_prompt=True, tokenize=False)
    return processor(text=[text], images=[image.convert("RGB")], return_tensors="pt", padding=True)


def _load_semantic_model(model_dir: Path, stage_devices: List[str], dtype: str):
    from transformers import AutoProcessor, Qwen2_5OmniForConditionalGeneration

    torch_dtype = _torch_dtype(dtype)
    processor = AutoProcessor.from_pretrained(str(model_dir), trust_remote_code=True, local_files_only=True)
    started = _now_ms()
    model = Qwen2_5OmniForConditionalGeneration.from_pretrained(
        str(model_dir),
        dtype=torch_dtype,
        device_map={"thinker": stage_devices[0], "talker": stage_devices[1], "token2wav": stage_devices[2]},
        low_cpu_mem_usage=True,
        trust_remote_code=True,
        local_files_only=True,
    )
    model.eval()
    for device in stage_devices:
        _sync(device)
    return model, processor, _now_ms() - started


def _torch_dtype(dtype: str) -> torch.dtype:
    return {
        "bf16": torch.bfloat16,
        "bfloat16": torch.bfloat16,
        "fp16": torch.float16,
        "float16": torch.float16,
        "fp32": torch.float32,
        "float32": torch.float32,
    }[dtype.lower()]


def _load_real_model(model_dir: Path, device: str, dtype: str):
    from transformers import AutoProcessor, Qwen2_5OmniThinkerForConditionalGeneration

    torch_dtype = _torch_dtype(dtype)
    processor = AutoProcessor.from_pretrained(
        str(model_dir),
        trust_remote_code=True,
        local_files_only=True,
    )
    started = _now_ms()
    model = Qwen2_5OmniThinkerForConditionalGeneration.from_pretrained(
        str(model_dir),
        dtype=torch_dtype,
        device_map={"": torch.device(device)},
        low_cpu_mem_usage=True,
        trust_remote_code=True,
        local_files_only=True,
    )
    model.eval()
    _sync(device)
    return model, processor, _now_ms() - started


def _extract_prompt(record: Dict[str, Any]) -> str:
    steps = record.get("steps")
    if isinstance(steps, list) and steps:
        first = steps[0] if isinstance(steps[0], dict) else {}
        prompt = first.get("input_prompt")
        if prompt:
            return str(prompt)
    messages = record.get("messages")
    if isinstance(messages, list):
        texts: List[str] = []
        for message in messages:
            for part in message.get("content", []) if isinstance(message, dict) else []:
                if isinstance(part, dict) and part.get("type") == "text" and part.get("text"):
                    texts.append(str(part["text"]))
        if texts:
            return "\n".join(texts)
    return str(record.get("generation_prompt") or "Describe the image.")


def _record_image(record: Dict[str, Any]) -> Dict[str, Any]:
    images = record.get("images")
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict) and first.get("path_or_url"):
            return {
                "image_id": str(first.get("image_id") or Path(str(first["path_or_url"])).stem),
                "path": str(first["path_or_url"]),
            }
    messages = record.get("messages")
    if isinstance(messages, list):
        for message in messages:
            for part in message.get("content", []) if isinstance(message, dict) else []:
                if isinstance(part, dict) and part.get("type") == "image" and part.get("image"):
                    return {
                        "image_id": Path(str(part["image"])).stem,
                        "path": str(part["image"]),
                    }
    raise ValueError(f"dataset record has no image: keys={list(record)}")


def _load_dataset_samples(dataset_jsonl: Optional[str], dataset_root: str, limit: int) -> List[Dict[str, Any]]:
    if not dataset_jsonl:
        return []
    root = Path(dataset_root).expanduser().resolve()
    jsonl = Path(dataset_jsonl).expanduser()
    if not jsonl.is_absolute():
        jsonl = root / jsonl
    samples: List[Dict[str, Any]] = []
    with jsonl.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            record = json.loads(line)
            image_info = _record_image(record)
            image_path = Path(image_info["path"]).expanduser()
            if not image_path.is_absolute():
                image_path = root / image_path
            image = Image.open(image_path).convert("RGB")
            samples.append(
                {
                    "sample_id": str(record.get("sample_id") or record.get("workflow_id") or len(samples)),
                    "workflow_id": str(record.get("workflow_id") or ""),
                    "source_dataset": str(record.get("source_dataset") or ""),
                    "task_type": str(record.get("task_type") or ""),
                    "image_id": str(image_info["image_id"]),
                    "image_path": str(image_path),
                    "image": image,
                    "prompt": _extract_prompt(record),
                }
            )
            if limit > 0 and len(samples) >= limit:
                break
    if not samples:
        raise ValueError(f"dataset yielded no samples: {jsonl}")
    return samples


def _decode_generated_text(processor: Any, sequences: torch.Tensor, input_length: torch.Tensor) -> Tuple[str, int]:
    prompt_len = int(input_length.flatten()[0].item()) if torch.is_tensor(input_length) else int(input_length)
    generated = sequences[:, prompt_len:].detach().cpu()
    if hasattr(processor, "batch_decode"):
        text = processor.batch_decode(generated, skip_special_tokens=True, clean_up_tokenization_spaces=False)[0]
    elif hasattr(processor, "decode"):
        text = processor.decode(generated[0].tolist(), skip_special_tokens=True)
    else:
        text = ""
    return str(text), int(generated.shape[-1])


def _make_transfer_policy(args: argparse.Namespace) -> Tuple[Mode, Dict[str, Any]]:
    if args.transport_backend == "shm":
        return Mode.SHM, {"omni_transport_backend": "shm", "force_copy": args.force_copy}
    return Mode.PUSH_BATCH, {
        "transport_backend": "mooncake_engine_direct",
        "source_memory_mode": args.source_memory_mode,
        "strict_no_fallback": bool(args.strict_no_fallback),
    }


def run(args: argparse.Namespace) -> Dict[str, Any]:
    model_dir = Path(args.model).expanduser().resolve()
    if not model_dir.exists():
        raise FileNotFoundError(f"model not found: {model_dir}")
    stage_devices = args.stage_devices or _default_stage_devices()
    if len(stage_devices) != 3:
        raise ValueError("--stage-devices must provide exactly three devices: AR Generation Diffusion")
    if args.runtime == "process" and args.stage_impl in {"semantic", "tensor"}:
        raise ValueError(
            "--runtime process is supported for --stage-impl dataset_tensor transport validation. "
            "Use --runtime thread for real-model tensor/semantic modes; CUDA fork with loaded models is not reliable. "
            "The semantic mode keeps one top-level model sharded across stage devices instead of loading duplicate model copies per process."
        )
    dataset_samples = _load_dataset_samples(args.dataset_jsonl, args.dataset_root, args.limit)

    if args.stage_impl == "semantic":
        model, processor, model_load_ms = _load_semantic_model(model_dir, stage_devices, args.dtype)
        stages = [
            SemanticARThinkerStage(
                model,
                processor,
                stage_devices[0],
                None if dataset_samples else _make_test_image(),
                args.prompt,
                max_new_tokens=args.thinker_max_new_tokens,
                use_audio_in_video=args.use_audio_in_video,
            ),
            SemanticTalkerGenerationStage(
                model,
                stage_devices[1],
                speaker=args.speaker,
                max_new_tokens=args.talker_max_new_tokens,
                do_sample=args.talker_do_sample,
                top_k=args.talker_top_k,
                top_p=args.talker_top_p,
                temperature=args.talker_temperature,
                repetition_penalty=args.talker_repetition_penalty,
                eos_token_id=args.talker_eos_token_id,
            ),
            SemanticToken2WavDiffusionStage(
                model,
                stage_devices[2],
                num_steps=args.token2wav_num_steps,
                guidance_scale=args.token2wav_guidance_scale,
                sway_coefficient=args.token2wav_sway_coefficient,
            ),
        ]
    elif args.stage_impl == "dataset_tensor":
        model = None
        processor = None
        model_load_ms = 0.0
        stages = [
            DatasetTensorARStage(image_size=args.dataset_tensor_image_size),
            DatasetTensorGenerationStage(width=args.dataset_tensor_width),
            DatasetTensorDiffusionStage(steps=args.diffusion_steps),
        ]
        stage_devices = ["cpu", "cpu", "cpu"]
    else:
        model, processor, model_load_ms = _load_real_model(model_dir, stage_devices[0], args.dtype)
        worker = Qwen25OmniImageEncoderWorker(
            model,
            processor,
            torch.device(stage_devices[0]),
            enable_hidden_prefix_cache=False,
        )
        stages = [
            RealOmniARStage(worker, None if dataset_samples else _make_test_image(), args.prompt),
            TensorGenerationStage(stage_devices[1]),
            TensorDiffusionStage(stage_devices[2], steps=args.diffusion_steps),
        ]

    transfer = TransferEngine(
        protocol=args.protocol,
        local_hostname=args.local_hostname,
        metadata_server=args.metadata_server,
        device_name=args.device_name,
    )
    if args.protocol in {"tcp", "rdma"} and args.init_direct_engine:
        transfer.initialize()

    mode, edge_extra = _make_transfer_policy(args)
    pipe = OmniPipeline(
        stages,
        transfer=transfer,
        device_per_stage=stage_devices,
        worker_per_stage=[
            OmniStageWorkerSpec("AR", worker_id="ar-0", device=stage_devices[0], transport_backend=args.transport_backend),
            OmniStageWorkerSpec("Generation", worker_id="gen-0", device=stage_devices[1], transport_backend=args.transport_backend),
            OmniStageWorkerSpec("Diffusion", worker_id="diff-0", device=stage_devices[2], transport_backend=args.transport_backend),
        ],
        policy_per_edge=[
            TransferPolicy(mode, channel=Channel.AGENT_TO_AGENT, extra=dict(edge_extra)),
            TransferPolicy(mode, channel=Channel.AGENT_TO_AGENT, extra=dict(edge_extra)),
        ],
    )
    if args.runtime == "process":
        runtime = OmniPipelineProcessRuntime(
            pipe,
            queue_size=args.queue_size,
            worker_name_prefix="omni-e2e-proc",
            start_method=args.process_start_method,
        )
    else:
        runtime = OmniPipelineRuntime(pipe, queue_size=args.queue_size, worker_name_prefix="omni-e2e")

    started = _now_ms()
    sample_results: List[Dict[str, Any]] = []
    try:
        run_inputs = dataset_samples or [{}]
        for index, sample in enumerate(run_inputs):
            sample_started = _now_ms()
            result = runtime.run([sample], timeout=args.timeout_s)
            for device in stage_devices:
                _sync(device)
            payload = result[0]
            common = {
                "index": index,
                "sample_id": str(sample.get("sample_id") or "synthetic"),
                "workflow_id": str(sample.get("workflow_id") or ""),
                "source_dataset": str(sample.get("source_dataset") or "synthetic"),
                "task_type": str(sample.get("task_type") or "synthetic"),
                "image_id": str(sample.get("image_id") or "omni-e2e-image"),
                "image_path": str(sample.get("image_path") or ""),
                "latency_ms": _now_ms() - sample_started,
            }
            if args.stage_impl == "semantic":
                text, generated_tokens = _decode_generated_text(processor, payload["sequences"], payload["input_length"])
                waveform = payload["waveform"]
                codes = payload["talker_codes"]
                sample_results.append(
                    {
                        **common,
                        "generated_text": text,
                        "generated_text_preview": text[:240],
                        "generated_tokens": generated_tokens,
                        "talker_codes_shape": list(codes.shape),
                        "talker_codes_device": str(codes.device),
                        "waveform_shape": list(waveform.shape),
                        "waveform_device": str(waveform.device),
                        "waveform_dtype": str(waveform.dtype).replace("torch.", ""),
                        "waveform_abs_mean": float(payload["waveform_abs_mean"].item()),
                        "waveform_rms": float(payload["waveform_rms"].item()),
                        "semantic_quality": {
                            "nonempty_text": bool(text.strip()),
                            "has_audio_waveform": int(waveform.numel()) > 0,
                            "nonzero_audio": float(payload["waveform_abs_mean"].item()) > 0.0,
                            "has_talker_codes": int(codes.numel()) > 0,
                        },
                    }
                )
            else:
                latent = payload["latent"]
                sample_results.append(
                    {
                        **common,
                        "latent_shape": list(latent.shape),
                        "latent_dtype": str(latent.dtype).replace("torch.", ""),
                        "latent_device": str(latent.device),
                        "latent_norm": float(payload["latent_norm"].item()),
                    }
                )
    finally:
        runtime.stop()
        transfer.shutdown()
    e2e_ms = _now_ms() - started

    last_sample = sample_results[-1]
    if args.stage_impl == "semantic":
        result_summary = {
            "generated_text_preview": last_sample["generated_text_preview"],
            "generated_tokens": last_sample["generated_tokens"],
            "talker_codes_shape": last_sample["talker_codes_shape"],
            "waveform_shape": last_sample["waveform_shape"],
            "waveform_dtype": last_sample["waveform_dtype"],
            "waveform_abs_mean": last_sample["waveform_abs_mean"],
            "waveform_rms": last_sample["waveform_rms"],
        }
    else:
        result_summary = {
            "latent_shape": last_sample["latent_shape"],
            "latent_dtype": last_sample["latent_dtype"],
            "latent_device": last_sample["latent_device"],
            "latent_norm": last_sample["latent_norm"],
        }

    artifact = {
        "status": "ok",
        "model": str(model_dir),
        "model_load_ms": model_load_ms,
        "dataset": {
            "jsonl": str(args.dataset_jsonl or ""),
            "root": str(args.dataset_root or ""),
            "samples": len(sample_results),
        },
        "stage_devices": stage_devices,
        "runtime": args.runtime,
        "stage_impl": args.stage_impl,
        "protocol": args.protocol,
        "transport_backend": args.transport_backend,
        "dtype": args.dtype,
        "e2e_ms": e2e_ms,
        "result": result_summary,
        "sample_results": sample_results,
        "pipeline_stats": runtime.stats(),
        "transfer_stats": transfer.stats.snapshot(),
        "cuda": {
            "available": torch.cuda.is_available(),
            "device_count": torch.cuda.device_count() if torch.cuda.is_available() else 0,
            "names": [
                torch.cuda.get_device_name(i)
                for i in range(torch.cuda.device_count())
            ] if torch.cuda.is_available() else [],
        },
        "timestamp_unix": time.time(),
    }
    return artifact


def main() -> None:
    ap = argparse.ArgumentParser(description="Run real Qwen2.5-Omni OmniPipeline stage-transfer E2E")
    ap.add_argument("--model", default="/home/songbinbin/Qwen2.5-Omni-7B")
    ap.add_argument("--stage-devices", nargs=3, default=None, metavar=("AR", "GEN", "DIFF"))
    ap.add_argument("--dtype", default="bf16", choices=["bf16", "bfloat16", "fp16", "float16", "fp32", "float32"])
    ap.add_argument("--protocol", default="local", choices=["local", "tcp", "rdma"])
    ap.add_argument("--transport-backend", default="shm", choices=["shm", "mooncake_engine_direct"])
    ap.add_argument("--runtime", default="thread", choices=["thread", "process"], help="Worker runtime: threaded live model path or process-isolated transport path")
    ap.add_argument("--process-start-method", default="fork", choices=["fork", "spawn", "forkserver"], help="multiprocessing start method for --runtime process")
    ap.add_argument("--stage-impl", default="tensor", choices=["tensor", "dataset_tensor", "semantic"], help="real encoder tensor smoke, process-safe dataset tensor, or full Qwen2.5-Omni semantic stages")
    ap.add_argument("--source-memory-mode", default="managed_buffer", choices=["managed_buffer", "registered_tensor"])
    ap.add_argument("--strict-no-fallback", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--force-copy", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--init-direct-engine", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--local-hostname", default=os.getenv("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1"))
    ap.add_argument("--metadata-server", default=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"))
    ap.add_argument("--device-name", default=os.getenv("MOONCAKE_DEVICE_NAME", ""))
    ap.add_argument("--prompt", default="Describe the geometric shapes in the image.")
    ap.add_argument("--dataset-jsonl", default=None, help="Real dataset JSONL, absolute or relative to --dataset-root")
    ap.add_argument("--dataset-root", default="/home/songbinbin/Proj/Proj_LWX/mooncake_test_dataset")
    ap.add_argument("--limit", type=int, default=0, help="Max dataset samples; 0 means all records")
    ap.add_argument("--diffusion-steps", type=int, default=8, help="Synthetic tensor diffusion steps for tensor/dataset_tensor modes")
    ap.add_argument("--dataset-tensor-image-size", type=int, default=32)
    ap.add_argument("--dataset-tensor-width", type=int, default=1024)
    ap.add_argument("--thinker-max-new-tokens", type=int, default=8)
    ap.add_argument("--talker-max-new-tokens", type=int, default=24)
    ap.add_argument("--talker-do-sample", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--talker-top-k", type=int, default=40)
    ap.add_argument("--talker-top-p", type=float, default=0.8)
    ap.add_argument("--talker-temperature", type=float, default=0.9)
    ap.add_argument("--talker-repetition-penalty", type=float, default=1.05)
    ap.add_argument("--talker-eos-token-id", type=int, nargs="+", default=[8292, 8294])
    ap.add_argument("--token2wav-num-steps", type=int, default=1)
    ap.add_argument("--token2wav-guidance-scale", type=float, default=0.5)
    ap.add_argument("--token2wav-sway-coefficient", type=float, default=-1.0)
    ap.add_argument("--speaker", default="Chelsie")
    ap.add_argument("--use-audio-in-video", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--queue-size", type=int, default=2)
    ap.add_argument("--timeout-s", type=float, default=180.0)
    ap.add_argument("--output", default="artifacts/qwen25_omni_stage_transfer_e2e.json")
    args = ap.parse_args()

    artifact = run(args)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"status": artifact["status"], "output": str(out), "e2e_ms": artifact["e2e_ms"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
