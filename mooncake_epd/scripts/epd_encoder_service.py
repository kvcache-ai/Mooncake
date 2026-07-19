#!/usr/bin/env python3
"""Online E-stage service for Mooncake EPD FeatureHandle generation.

The service is intentionally thin: it owns the real vision encoder process,
publishes hidden-state FeatureBundles, and returns lightweight FeatureHandle
control payloads for the vLLM prefill hot path.  It can publish either to the
local file transport used by same-node development or to a real Mooncake Store
(``--publish-backend mooncake``).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import inspect
import io
import json
import logging
import os
import ipaddress
import sys
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse

import httpx
import torch
from fastapi import FastAPI, HTTPException
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.epd_workers import EncoderWorker  # noqa: E402
from mooncake_epd.core.omni_encoder_worker import Qwen25OmniImageEncoderWorker  # noqa: E402
from mooncake_epd.core.vllm_native_vision_encoder import (  # noqa: E402
    VLLMNativeQwen3VisionEncoder,
)
from mooncake_epd.core.state import (  # noqa: E402
    FeatureHandle,
    MooncakeFeatureBundleStore,
    MooncakeFeatureBundleStoreConfig,
    publish_feature_bundle_to_dir,
)
from mooncake_epd.core.state.hidden_cache_key import (  # noqa: E402
    HiddenCacheKeyError,
    canonical_mm_processor_kwargs,
    stable_multimodal_asset_hash,
    stable_multimodal_feature_id,
)
from mooncake_epd.core.transfer import TransferEngine  # noqa: E402

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


_ENCODER_FAMILIES = {"qwen3_vl", "qwen2_5_vl", "qwen2_5_omni"}
_ENCODER_RUNTIMES = {"transformers", "vllm_native"}


def resolve_encoder_family(requested_family: str, model_type: str = "") -> str:
    """Return the canonical E-stage worker family for a loaded checkpoint.

    Never silently route Qwen2.5-VL through Qwen3-VL.  Both models expose a
    similar vision API, but their processor/model checkpoint contracts differ;
    choosing the exact class is required for reliable E→P hidden-state reuse.
    """

    requested = str(requested_family or "auto").strip().lower()
    aliases = {
        "qwen3vl": "qwen3_vl",
        "qwen_vl": "qwen3_vl",
        "qwen25_vl": "qwen2_5_vl",
        "qwen2.5_vl": "qwen2_5_vl",
        "qwen25_omni": "qwen2_5_omni",
        "omni": "qwen2_5_omni",
    }
    requested = aliases.get(requested, requested)
    if requested == "auto":
        normalized_type = str(model_type or "").strip().lower()
        if normalized_type == "qwen2_5_omni":
            return "qwen2_5_omni"
        if normalized_type == "qwen2_5_vl":
            return "qwen2_5_vl"
        return "qwen3_vl"
    if requested not in _ENCODER_FAMILIES:
        raise ValueError(f"unsupported encoder_family: {requested_family}")
    return requested


def resolve_encoder_runtime(requested_runtime: str, encoder_family: str) -> str:
    """Validate the E-stage implementation selected for one model family."""

    requested = str(requested_runtime or "transformers").strip().lower()
    aliases = {
        "hf": "transformers",
        "transformer": "transformers",
        "vllm": "vllm_native",
        "native_vllm": "vllm_native",
    }
    requested = aliases.get(requested, requested)
    if requested not in _ENCODER_RUNTIMES:
        raise ValueError(f"unsupported encoder_runtime: {requested_runtime}")
    if requested == "vllm_native" and str(encoder_family) != "qwen3_vl":
        raise ValueError(
            "encoder_runtime=vllm_native currently requires encoder_family=qwen3_vl"
        )
    return requested


@dataclass(frozen=True)
class EncoderServiceConfig:
    model: str = os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct")
    device: str = "cuda:0"
    dtype: str = "bfloat16"
    encoder_family: str = "auto"  # auto | qwen3_vl | qwen2_5_vl | qwen2_5_omni
    # ``vllm_native`` is an explicit Qwen3-VL numerical-parity runtime.  Keep
    # Transformers as the portable default for all supported families.
    encoder_runtime: str = "transformers"  # transformers | vllm_native
    publish_backend: str = "file"  # file | mooncake | direct_engine
    store_dir: str = "/tmp/mooncake_epd_feature_handle_store"
    mooncake_store_url: Optional[str] = None
    mooncake_store_id: str = "mooncake-mm-store"
    mooncake_config: Optional[str] = None
    mooncake_timeout_s: float = 30.0
    mooncake_protocol: str = "tcp"
    mooncake_local_hostname: str = "localhost"
    mooncake_metadata_server: str = "P2PHANDSHAKE"
    mooncake_device_name: str = ""
    direct_source_mode: str = "registered_tensor"
    checksum: bool = False
    max_image_bytes: int = 32 * 1024 * 1024
    request_timeout_s: float = 15.0
    direct_ticket_ttl_s: float = 60.0
    # Request images are untrusted input.  The default accepts only data URLs,
    # which is sufficient for the real demo and prevents the encoder service
    # becoming a general local-file/SSRF proxy.  File and HTTP are explicit
    # deployment-scoped opt-ins.
    allowed_image_root: Optional[str] = None
    allow_http_images: bool = False
    allowed_http_hosts: Tuple[str, ...] = ()
    allow_file_images: bool = False
    enable_omni_hidden_prefix_cache: bool = True
    omni_hidden_prefix_cache_metrics: Optional[str] = None
    omni_allow_partial_prefix: bool = False
    omni_partial_oracle_validated: bool = False
    feature_cache_entries: int = 64
    feature_cache_max_bytes: int = 2 * 1024 * 1024 * 1024
    warmup_on_startup: bool = True


class _FeatureBundleLRU:
    def __init__(self, *, max_entries: int, max_bytes: int):
        self.max_entries = max(0, int(max_entries))
        self.max_bytes = max(0, int(max_bytes))
        self._items: "OrderedDict[str, Tuple[int, Any, Dict[str, Any]]]" = OrderedDict()
        self._bytes = 0
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Tuple[Any, Dict[str, Any]]]:
        if self.max_entries <= 0 or self.max_bytes <= 0:
            self.misses += 1
            return None
        item = self._items.pop(str(key), None)
        if item is None:
            self.misses += 1
            return None
        nbytes, bundle, metadata = item
        self._items[str(key)] = (nbytes, bundle, dict(metadata))
        self.hits += 1
        return bundle, dict(metadata)

    def put(self, key: str, bundle: Any, metadata: Dict[str, Any]) -> None:
        if self.max_entries <= 0 or self.max_bytes <= 0:
            return
        try:
            nbytes = int(bundle.nbytes())
        except Exception:
            return
        if nbytes <= 0 or nbytes > self.max_bytes:
            return
        old = self._items.pop(str(key), None)
        if old is not None:
            self._bytes -= int(old[0])
        self._items[str(key)] = (nbytes, bundle, dict(metadata))
        self._bytes += nbytes
        while self._items and (len(self._items) > self.max_entries or self._bytes > self.max_bytes):
            _, (evicted, _, _) = self._items.popitem(last=False)
            self._bytes -= int(evicted)

    def stats(self) -> Dict[str, Any]:
        lookups = self.hits + self.misses
        return {
            "entries": len(self._items),
            "bytes": self._bytes,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": (self.hits / lookups) if lookups else 0.0,
            "max_entries": self.max_entries,
            "max_bytes": self.max_bytes,
        }


class _LazyEncoder:
    def __init__(self, config: EncoderServiceConfig):
        self.config = config
        self._lock = asyncio.Lock()
        self._worker: Optional[Any] = None
        self._processor = None
        self._model = None

    async def worker(self) -> Any:
        if self._worker is not None:
            return self._worker
        async with self._lock:
            if self._worker is None:
                self._worker = await asyncio.to_thread(self._load_sync)
            return self._worker

    def _load_sync(self) -> Any:
        from transformers import AutoConfig, AutoProcessor

        dtype = _torch_dtype(self.config.dtype)
        device = torch.device(self.config.device)
        requested_family = str(self.config.encoder_family or "auto").lower()
        model_type = ""
        if requested_family == "auto":
            cfg = AutoConfig.from_pretrained(self.config.model, trust_remote_code=True)
            model_type = str(getattr(cfg, "model_type", "") or "").lower()
        family = resolve_encoder_family(requested_family, model_type)
        processor = AutoProcessor.from_pretrained(self.config.model, trust_remote_code=True)
        runtime = resolve_encoder_runtime(self.config.encoder_runtime, family)
        if runtime == "vllm_native":
            self._processor = processor
            self._model = None
            return VLLMNativeQwen3VisionEncoder(
                self.config.model,
                processor,
                device=device,
                dtype=self.config.dtype,
            )
        if family in {"qwen2_5_omni", "qwen25_omni", "omni"}:
            from transformers import Qwen2_5OmniThinkerForConditionalGeneration

            model = Qwen2_5OmniThinkerForConditionalGeneration.from_pretrained(
                self.config.model,
                dtype=dtype,
                device_map={"": device},
                low_cpu_mem_usage=True,
                local_files_only=True,
                trust_remote_code=True,
            )
            model.eval()
            self._processor = processor
            self._model = model
            return Qwen25OmniImageEncoderWorker(
                model,
                processor,
                device=device,
                enable_hidden_prefix_cache=bool(self.config.enable_omni_hidden_prefix_cache),
                allow_partial_prefix_reuse=bool(self.config.omni_allow_partial_prefix),
                partial_oracle_validated=bool(self.config.omni_partial_oracle_validated),
                cache_metrics_path=self.config.omni_hidden_prefix_cache_metrics,
            )
        if family == "qwen2_5_vl":
            from transformers import Qwen2_5_VLForConditionalGeneration

            model = Qwen2_5_VLForConditionalGeneration.from_pretrained(
                self.config.model,
                dtype=dtype,
                device_map={"": device},
                low_cpu_mem_usage=True,
                local_files_only=True,
                trust_remote_code=True,
            )
            model.eval()
            self._processor = processor
            self._model = model
            return EncoderWorker(model, processor, device=device)
        if family == "qwen3_vl":
            from transformers import Qwen3VLForConditionalGeneration

            model = Qwen3VLForConditionalGeneration.from_pretrained(
                self.config.model,
                dtype=dtype,
                device_map={"": device},
                low_cpu_mem_usage=True,
            )
            model.eval()
            self._processor = processor
            self._model = model
            return EncoderWorker(model, processor, device=device)
        raise AssertionError(f"unreachable encoder_family: {family}")

    @property
    def processor(self):
        if self._processor is None:
            raise RuntimeError("encoder is not loaded yet")
        return self._processor


def create_app(
    config: Optional[EncoderServiceConfig] = None,
    *,
    encoder: Optional[Any] = None,
    direct_transfer_engine: Optional[TransferEngine] = None,
) -> FastAPI:
    """Create the online encoder FastAPI app.

    ``encoder`` is an optional dependency injection point for local tests and
    process managers that already own a loaded EncoderWorker-compatible object.
    Production code should leave it unset so the real Qwen-VL model is loaded
    lazily on the configured GPU.
    """

    config = config or EncoderServiceConfig()
    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        engine = getattr(app.state, "direct_transfer_engine", None)
        if engine is not None and getattr(app.state, "owns_direct_transfer_engine", False):
            # Direct-engine initialization does topology discovery and metadata
            # registration.  Doing it at service startup keeps that one-time
            # cost out of the first user-visible /publish_direct TTFT path and
            # also avoids recreating a Mooncake engine per request.
            await asyncio.to_thread(engine.initialize)
        if bool(getattr(app.state.config, "warmup_on_startup", False)) and app.state.encoder is None:
            # Production serving should not charge model construction to the
            # first user request. Load the real encoder before health readiness;
            # requests still perform real preprocessing and vision encoding.
            app.state.encoder = await app.state.lazy_encoder.worker()
        try:
            yield
        finally:
            # Native vLLM vision owns a private one-rank distributed group.
            # Dependency-injected test/embedding workers remain caller-owned.
            if bool(getattr(app.state, "owns_encoder", False)):
                worker = getattr(app.state, "encoder", None)
                close = getattr(worker, "close", None)
                if callable(close):
                    try:
                        await asyncio.to_thread(close)
                    except Exception:
                        logger.exception("encoder worker shutdown failed")
            http = getattr(app.state, "http", None)
            if http is not None:
                await http.aclose()
            if engine is not None and getattr(app.state, "owns_direct_transfer_engine", False):
                await asyncio.to_thread(engine.shutdown)

    app = FastAPI(title="Mooncake EPD Encoder Service", lifespan=_lifespan)
    app.state.config = config
    app.state.lazy_encoder = None if encoder is not None else _LazyEncoder(config)
    app.state.encoder = encoder
    app.state.owns_encoder = encoder is None
    if direct_transfer_engine is None and config.publish_backend == "direct_engine":
        direct_transfer_engine = TransferEngine(
            protocol=os.getenv("MOONCAKE_EPD_DIRECT_ENGINE_PROTOCOL", config.mooncake_protocol),
            local_hostname=config.mooncake_local_hostname,
            metadata_server=config.mooncake_metadata_server,
            device_name=config.mooncake_device_name,
        )
        app.state.owns_direct_transfer_engine = True
    else:
        app.state.owns_direct_transfer_engine = False
    app.state.direct_transfer_engine = direct_transfer_engine
    app.state.pending_direct_bundles = {}
    app.state.pending_direct_lock = asyncio.Lock()
    app.state.direct_publish_locks = {}
    app.state.feature_cache = _FeatureBundleLRU(
        max_entries=int(config.feature_cache_entries),
        max_bytes=int(config.feature_cache_max_bytes),
    )
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(config.request_timeout_s, connect=5.0),
        follow_redirects=False,
        trust_env=False,
    )

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return {
            "status": "ok",
            "model": config.model,
            "device": config.device,
            "encoder_family": config.encoder_family,
            "encoder_runtime": config.encoder_runtime,
            "publish_backend": config.publish_backend,
            "omni_hidden_prefix_cache": bool(config.enable_omni_hidden_prefix_cache),
            "feature_cache": app.state.feature_cache.stats(),
            "loaded": app.state.encoder is not None,
        }

    async def _worker_and_processor() -> Tuple[Any, Any]:
        worker = app.state.encoder
        if worker is None:
            worker = await app.state.lazy_encoder.worker()
            app.state.encoder = worker
        processor = getattr(worker, "processor", None) or getattr(app.state.lazy_encoder, "processor", None)
        if processor is None:
            raise HTTPException(status_code=500, detail="encoder worker has no processor")
        return worker, processor

    async def _encode_records(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], float, Dict[str, Any]]:
        items = list(_iter_mm_image_items(payload))
        if not items:
            raise HTTPException(status_code=400, detail="no image/image_url items found")
        worker, processor = await _worker_and_processor()
        request_processor_kwargs = _request_mm_processor_kwargs(payload)
        image_processor_kwargs = _image_processor_kwargs_for_request(
            processor,
            request_processor_kwargs,
        )
        preprocessing_fingerprint = _preprocessing_fingerprint(request_processor_kwargs)

        records: List[Dict[str, Any]] = []
        encode_ms_total = 0.0
        cache_stats: Dict[str, Any] = {}
        loaded: List[Tuple[Dict[str, Any], Image.Image, str, str, str, str, str]] = []
        for item in items:
            image, content_type, source_digest = await _load_image_item(
                app,
                item,
                max_bytes=int(config.max_image_bytes),
            )
            source_mm_hash = _stable_mm_hash(item)
            feature_id = stable_multimodal_feature_id(item, request_processor_kwargs)
            cache_key = _feature_cache_key(
                config,
                source_digest=source_digest,
                request_processor_kwargs=request_processor_kwargs,
            )
            cached = app.state.feature_cache.get(cache_key)
            if cached is not None:
                bundle, cached_metadata = cached
                metadata = dict(cached_metadata)
                metadata.update(
                    {
                        "source_mm_hash": source_mm_hash,
                        "feature_identity": feature_id,
                        "source_digest": source_digest,
                        "source_content_type": content_type,
                        "mm_processor_kwargs": request_processor_kwargs,
                        "image_processor_kwargs": image_processor_kwargs,
                        "mm_preprocess_fingerprint": preprocessing_fingerprint,
                        "encoder_feature_cache_hit": True,
                        "encode_time_ms": 0.0,
                    }
                )
                records.append({"bundle": bundle, "metadata": metadata})
                continue
            loaded.append(
                (
                    item,
                    image,
                    content_type,
                    source_digest,
                    source_mm_hash,
                    feature_id,
                    cache_key,
                )
            )

        if hasattr(worker, "encode_images"):
            prompt = _prompt_for_processor(payload)
            encode_images_kwargs: Dict[str, Any] = {
                "image_ids": [feature_id for _, _, _, _, _, feature_id, _ in loaded],
                "prompt": prompt,
            }
            if _call_accepts_keyword(worker.encode_images, "image_processor_kwargs"):
                encode_images_kwargs["image_processor_kwargs"] = dict(image_processor_kwargs)
            batch_out = await asyncio.to_thread(
                worker.encode_images,
                [image for _, image, _, _, _, _, _ in loaded],
                **encode_images_kwargs,
            )
            encode_ms_total += float(getattr(batch_out, "encode_time_ms", 0.0) or 0.0)
            cache_stats = dict(getattr(batch_out, "cache_stats", {}) or {})
            for index, (enc_out, (_, _, content_type, source_digest, source_mm_hash, feature_id, cache_key)) in enumerate(
                zip(batch_out.outputs, loaded)
            ):
                metadata = {
                    "source_mm_hash": source_mm_hash,
                    "feature_identity": feature_id,
                    "source_digest": source_digest,
                    "source_content_type": content_type,
                    "mm_processor_kwargs": request_processor_kwargs,
                    "image_processor_kwargs": image_processor_kwargs,
                    "mm_preprocess_fingerprint": preprocessing_fingerprint,
                    "source_index": index,
                    "encoder_service_model": config.model,
                    "encoder_service_device": config.device,
                    "encoder_family": config.encoder_family,
                    "encode_time_ms": enc_out.encode_time_ms,
                    "publish_backend": config.publish_backend,
                    "omni_hidden_prefix_cache": cache_stats,
                    "encoder_feature_cache_hit": False,
                }
                app.state.feature_cache.put(cache_key, enc_out.bundle, metadata)
                records.append({"bundle": enc_out.bundle, "metadata": metadata})
        else:
            prompt = _prompt_for_processor(payload)
            for index, (_, image, content_type, source_digest, source_mm_hash, feature_id, cache_key) in enumerate(loaded):
                inputs = await asyncio.to_thread(
                    _processor_inputs,
                    processor,
                    image,
                    prompt,
                    image_processor_kwargs=image_processor_kwargs,
                )
                if "pixel_values" not in inputs or "image_grid_thw" not in inputs:
                    raise HTTPException(status_code=500, detail="processor did not produce pixel_values/image_grid_thw")

                enc_out = await asyncio.to_thread(
                    worker.encode,
                    pixel_values=inputs["pixel_values"],
                    image_grid_thw=inputs["image_grid_thw"],
                    image_id=feature_id,
                )
                encode_ms_total += float(enc_out.encode_time_ms)
                metadata = {
                    "source_mm_hash": source_mm_hash,
                    "feature_identity": feature_id,
                    "source_digest": source_digest,
                    "source_content_type": content_type,
                    "mm_processor_kwargs": request_processor_kwargs,
                    "image_processor_kwargs": image_processor_kwargs,
                    "mm_preprocess_fingerprint": preprocessing_fingerprint,
                    "source_index": index,
                    "encoder_service_model": config.model,
                    "encoder_service_device": config.device,
                    "encoder_family": config.encoder_family,
                    "encode_time_ms": enc_out.encode_time_ms,
                    "publish_backend": config.publish_backend,
                    "encoder_feature_cache_hit": False,
                }
                app.state.feature_cache.put(cache_key, enc_out.bundle, metadata)
                records.append({"bundle": enc_out.bundle, "metadata": metadata})
        cache_stats = dict(cache_stats or {})
        cache_stats["feature_bundle_cache"] = app.state.feature_cache.stats()
        return records, encode_ms_total, cache_stats

    async def _sweep_pending_direct_bundles(*, now: Optional[float] = None) -> int:
        now = time.monotonic() if now is None else float(now)
        ttl = max(1.0, float(config.direct_ticket_ttl_s))
        async with app.state.pending_direct_lock:
            expired = [
                ticket
                for ticket, item in app.state.pending_direct_bundles.items()
                if now - float(item.get("created_at", now) or now) > ttl
            ]
            for ticket in expired:
                app.state.pending_direct_bundles.pop(ticket, None)
            return len(expired)

    async def _publish_direct_records(
        records: Sequence[Dict[str, Any]],
        direct_targets: Sequence[Dict[str, Any]],
        *,
        publish_mask: Optional[Sequence[bool]] = None,
    ) -> List[FeatureHandle]:
        """Publish one request's direct features with session-scoped batching.

        This is shared by the explicit ``/describe`` -> ``/publish_direct``
        handshake and the compatibility ``/encode`` endpoint.  Keeping both
        routes on the same primitive prevents an older caller from quietly
        losing the multi-image batching TTFT improvement.
        """

        if len(direct_targets) != len(records):
            raise ValueError(
                f"direct target count mismatch: targets={len(direct_targets)} descriptors={len(records)}"
            )
        if publish_mask is None:
            publish_mask = [True] * len(records)
        if len(publish_mask) != len(records):
            raise ValueError("direct publish mask must match direct targets")

        handles_by_index: List[Optional[FeatureHandle]] = [None] * len(records)
        publish_groups: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
        for index, record in enumerate(records):
            bundle = record["bundle"]
            if not bool(publish_mask[index]):
                handles_by_index[index] = _build_direct_handle_without_publish(
                    config,
                    bundle,
                    dict(record["metadata"]),
                    direct_targets[index],
                )
                continue
            remote_session = str(direct_targets[index].get("remote_session") or "")
            publish_groups.setdefault(remote_session, []).append((index, record))

        # One Prefill worker owns all normal request targets, so the common
        # path below converts N image-level writes into one Mooncake batch.
        # Different sessions cannot share a batch by Mooncake API contract;
        # execute those groups independently without silently changing the
        # receiver selected by the control plane.
        for _remote_session, group in publish_groups.items():
            # Cached FeatureBundles can share tensor storage across concurrent
            # requests. Serialize every source feature in a stable order, then
            # release in reverse order. This retains the prior registration-
            # safety guarantee while allowing distinct images in this request
            # to reach one batch_transfer_sync_write call.
            locks_by_feature: Dict[str, asyncio.Lock] = {}
            for _index, record in group:
                feature_id = str(getattr(record["bundle"], "image_hash", "") or "")
                lock = app.state.direct_publish_locks.get(feature_id)
                if lock is None:
                    lock = asyncio.Lock()
                    app.state.direct_publish_locks[feature_id] = lock
                locks_by_feature[feature_id] = lock
            acquired_locks = [locks_by_feature[key] for key in sorted(locks_by_feature)]
            for lock in acquired_locks:
                await lock.acquire()
            try:
                published = await asyncio.to_thread(
                    _publish_direct_bundles,
                    config,
                    [
                        (
                            record["bundle"],
                            dict(record["metadata"]),
                            direct_targets[index],
                        )
                        for index, record in group
                    ],
                    direct_transfer_engine=app.state.direct_transfer_engine,
                )
            finally:
                for lock in reversed(acquired_locks):
                    lock.release()
            if len(published) != len(group):  # pragma: no cover - defensive ABI guard
                raise RuntimeError(
                    f"direct publish returned {len(published)} handles for {len(group)} targets"
                )
            for (index, _record), handle in zip(group, published):
                handles_by_index[index] = handle

        if any(handle is None for handle in handles_by_index):  # pragma: no cover - defensive ABI guard
            raise RuntimeError("direct publish did not produce every FeatureHandle")
        return [handle for handle in handles_by_index if handle is not None]

    @app.post("/describe")
    async def describe(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Encode once and return descriptors for Prefill direct-buffer allocation.

        For ``publish_backend=direct_engine`` the bundle is held in encoder
        memory under a short-lived ticket until ``/publish_direct`` supplies
        Prefill-owned peer-buffer targets. This avoids the bad double-encode
        control flow while still keeping Prefill as the owner of destination
        tensors.
        """

        started = time.perf_counter()
        records, encode_ms_total, cache_stats = await _encode_records(payload)
        ticket = uuid.uuid4().hex
        await _sweep_pending_direct_bundles()
        async with app.state.pending_direct_lock:
            app.state.pending_direct_bundles[ticket] = {
                "records": records,
                "created_at": time.monotonic(),
                "request_metadata": dict(payload.get("metadata") or {}),
            }
        descriptors = [record["bundle"].descriptor(checksum=bool(config.checksum)).to_dict() for record in records]
        return {
            "ticket": ticket,
            "descriptors": descriptors,
            "metadata": [dict(record["metadata"]) for record in records],
            "store_id": config.mooncake_store_id,
            "count": len(records),
            "encode_time_ms": encode_ms_total,
            "total_time_ms": (time.perf_counter() - started) * 1000.0,
            "publish_backend": config.publish_backend,
            "encoder_family": config.encoder_family,
            "omni_hidden_prefix_cache": cache_stats,
        }

    @app.post("/publish_direct")
    async def publish_direct(payload: Dict[str, Any]) -> Dict[str, Any]:
        if config.publish_backend != "direct_engine":
            raise HTTPException(status_code=400, detail="/publish_direct requires publish_backend=direct_engine")
        ticket = str(payload.get("ticket") or "")
        async with app.state.pending_direct_lock:
            pending = app.state.pending_direct_bundles.pop(ticket, None)
        if pending is None:
            raise HTTPException(status_code=404, detail=f"unknown or already consumed direct publish ticket: {ticket}")
        direct_targets = _direct_feature_targets_for_payload(payload)
        raw_mask = payload.get("mooncake_epd_direct_publish_mask") or payload.get("direct_publish_mask")
        publish_mask = [True] * len(direct_targets)
        if raw_mask is not None:
            if not isinstance(raw_mask, list) or len(raw_mask) != len(direct_targets):
                raise HTTPException(
                    status_code=400,
                    detail="direct publish mask must be a list matching direct targets",
                )
            publish_mask = [bool(item) for item in raw_mask]
        records = list(pending.get("records") or [])
        if len(direct_targets) != len(records):
            raise HTTPException(
                status_code=400,
                detail=f"direct target count mismatch: targets={len(direct_targets)} descriptors={len(records)}",
            )
        try:
            handles = await _publish_direct_records(
                records,
                direct_targets,
                publish_mask=publish_mask,
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"direct publish failed: {exc}") from exc
        return {
            "handles": [handle.as_control_payload() for handle in handles],
            "count": len(handles),
            "publish_backend": config.publish_backend,
            "encoder_family": config.encoder_family,
        }

    @app.post("/release_direct_ticket")
    async def release_direct_ticket(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Idempotently release an unconsumed direct-publish ticket."""

        ticket = str(payload.get("ticket") or "")
        if not ticket:
            raise HTTPException(status_code=400, detail="ticket is required")
        async with app.state.pending_direct_lock:
            released = app.state.pending_direct_bundles.pop(ticket, None) is not None
        return {"ticket": ticket, "released": released, "idempotent": not released}

    @app.post("/encode")
    async def encode(payload: Dict[str, Any]) -> Dict[str, Any]:
        started = time.perf_counter()
        records, encode_ms_total, cache_stats = await _encode_records(payload)
        direct_targets = _direct_feature_targets_for_payload(payload)
        if config.publish_backend == "direct_engine":
            try:
                handles = await _publish_direct_records(records, direct_targets)
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"feature publish failed: {exc}") from exc
        else:
            handles = []
            for index, record in enumerate(records):
                try:
                    handle = await asyncio.to_thread(
                        _publish_bundle,
                        config,
                        record["bundle"],
                        dict(record["metadata"]),
                        direct_targets[index] if index < len(direct_targets) else None,
                        app.state.direct_transfer_engine,
                    )
                except Exception as exc:
                    raise HTTPException(status_code=502, detail=f"feature publish failed: {exc}") from exc
                handles.append(handle)

        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return {
            "handles": [handle.as_control_payload() for handle in handles],
            "count": len(handles),
            "encode_time_ms": encode_ms_total,
            "total_time_ms": elapsed_ms,
            "publish_backend": config.publish_backend,
            "encoder_family": config.encoder_family,
            "omni_hidden_prefix_cache": cache_stats,
        }

    return app


def _torch_dtype(name: str) -> torch.dtype:
    normalized = str(name or "").lower().replace("torch.", "")
    if normalized in {"bf16", "bfloat16"}:
        return torch.bfloat16
    if normalized in {"fp16", "float16", "half"}:
        return torch.float16
    if normalized in {"fp32", "float32", "float"}:
        return torch.float32
    raise ValueError(f"unsupported dtype: {name}")


def _iter_mm_image_items(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    messages = payload.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and _image_url_from_item(item):
                        yield item
    prompt = payload.get("prompt")
    if isinstance(prompt, list):
        for item in prompt:
            if isinstance(item, dict) and _image_url_from_item(item):
                yield item
    images = payload.get("images")
    if isinstance(images, list):
        for image in images:
            if isinstance(image, dict):
                yield image
            elif isinstance(image, str):
                yield {"type": "image_url", "image_url": {"url": image}}


def _image_url_from_item(item: Dict[str, Any]) -> Optional[str]:
    item_type = str(item.get("type", "")).strip().lower()
    if item_type not in {"image", "image_url", "input_image"}:
        return None
    image_url = item.get("image_url")
    if isinstance(image_url, str):
        return image_url
    if isinstance(image_url, dict) and image_url.get("url"):
        return str(image_url.get("url"))
    if item.get("url"):
        return str(item.get("url"))
    return None


async def _load_image_item(app: FastAPI, item: Dict[str, Any], *, max_bytes: int) -> Tuple[Image.Image, str, str]:
    url = _image_url_from_item(item)
    if not url:
        raise HTTPException(status_code=400, detail="image item has no URL")
    payload, content_type = await _load_url_bytes(app, url, max_bytes=max_bytes)
    try:
        image = Image.open(io.BytesIO(payload)).convert("RGB")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid image bytes: {exc}") from exc
    return image, content_type, hashlib.sha256(payload).hexdigest()


async def _load_url_bytes(app: FastAPI, url: str, *, max_bytes: int) -> Tuple[bytes, str]:
    if url.startswith("data:"):
        return _parse_data_url(url, max_bytes=max_bytes)
    config: EncoderServiceConfig = app.state.config
    if url.startswith("file://"):
        if not config.allow_file_images:
            raise HTTPException(status_code=400, detail="file image URLs are disabled")
        return _read_local_image(Path(url[7:]), config=config, max_bytes=max_bytes)
    if not (url.startswith("http://") or url.startswith("https://")):
        path = Path(url).expanduser()
        if config.allow_file_images and path.exists():
            return _read_local_image(path, config=config, max_bytes=max_bytes)
        raise HTTPException(status_code=400, detail=f"unsupported image URL scheme: {url[:64]}")
    if not config.allow_http_images:
        raise HTTPException(status_code=400, detail="HTTP image URLs are disabled")
    _validate_http_image_url(url, config=config)
    try:
        async with app.state.http.stream("GET", url) as response:
            response.raise_for_status()
            content_type = response.headers.get("content-type", "application/octet-stream").split(";")[0].strip()
            if not content_type.lower().startswith("image/"):
                raise HTTPException(status_code=400, detail="remote URL content-type is not an image")
            payload = await _read_limited_response(response, max_bytes=max_bytes)
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"remote image fetch failed: {type(exc).__name__}") from exc
    return payload, content_type or "application/octet-stream"


def _read_local_image(path: Path, *, config: EncoderServiceConfig, max_bytes: int) -> Tuple[bytes, str]:
    root_value = str(config.allowed_image_root or "").strip()
    if not root_value:
        raise HTTPException(status_code=400, detail="local image root is not configured")
    root = Path(root_value).expanduser().resolve()
    try:
        resolved = path.expanduser().resolve(strict=True)
        resolved.relative_to(root)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="image path is outside the allowed image root") from exc
    if not resolved.is_file():
        raise HTTPException(status_code=400, detail="image path is not a regular file")
    size = resolved.stat().st_size
    if size > max_bytes:
        raise HTTPException(status_code=413, detail=f"image too large: {size} > {max_bytes}")
    payload = resolved.read_bytes()
    return payload, _guess_content_type(resolved)


def _validate_http_image_url(url: str, *, config: EncoderServiceConfig) -> None:
    parsed = urlparse(url)
    host = str(parsed.hostname or "").rstrip(".").lower()
    if not host:
        raise HTTPException(status_code=400, detail="remote image URL has no host")
    allowed = {str(value).rstrip(".").lower() for value in config.allowed_http_hosts if str(value).strip()}
    if host not in allowed:
        raise HTTPException(status_code=400, detail="remote image host is not allowlisted")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return
    if address.is_private or address.is_loopback or address.is_link_local or address.is_reserved or address.is_multicast:
        raise HTTPException(status_code=400, detail="remote image host is not publicly routable")


async def _read_limited_response(response: httpx.Response, *, max_bytes: int) -> bytes:
    declared = response.headers.get("content-length")
    if declared:
        try:
            if int(declared) > max_bytes:
                raise HTTPException(status_code=413, detail=f"image too large: {declared} > {max_bytes}")
        except ValueError:
            pass
    chunks: List[bytes] = []
    total = 0
    async for chunk in response.aiter_bytes():
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(status_code=413, detail=f"image too large: {total} > {max_bytes}")
        chunks.append(chunk)
    return b"".join(chunks)


def _parse_data_url(url: str, *, max_bytes: int) -> Tuple[bytes, str]:
    header, sep, data = url.partition(",")
    if sep != "," or not header.startswith("data:"):
        raise HTTPException(status_code=400, detail="invalid data URL")
    meta = header[5:]
    parts = [part for part in meta.split(";") if part]
    content_type = parts[0] if parts and "/" in parts[0] else "application/octet-stream"
    if "base64" in parts:
        payload = base64.b64decode(data, validate=True)
    else:
        from urllib.parse import unquote_to_bytes
        payload = unquote_to_bytes(data)
    if len(payload) > max_bytes:
        raise HTTPException(status_code=413, detail=f"image too large: {len(payload)} > {max_bytes}")
    return payload, content_type


def _guess_content_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".png":
        return "image/png"
    if suffix == ".webp":
        return "image/webp"
    return "application/octet-stream"


def _prompt_for_processor(payload: Dict[str, Any]) -> str:
    pieces: List[str] = []
    for message in payload.get("messages") or []:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if isinstance(content, str):
            pieces.append(content)
        elif isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and str(item.get("type", "text")) in {"text", "input_text"}:
                    pieces.append(str(item.get("text") or item.get("content") or ""))
    if not pieces and isinstance(payload.get("prompt"), str):
        pieces.append(str(payload["prompt"]))
    return " ".join(part for part in pieces if part).strip() or "Describe the image."


_SUPPORTED_IMAGE_PROCESSOR_KWARGS = {
    "size",
    "min_pixels",
    "max_pixels",
    "do_resize",
    "do_convert_rgb",
    "do_rescale",
    "rescale_factor",
    "do_normalize",
    "image_mean",
    "image_std",
    "resample",
    "default_to_square",
    "do_pad",
    "pad_size",
    "do_center_crop",
    "crop_size",
    "data_format",
    "input_data_format",
    "patch_size",
    "temporal_patch_size",
    "merge_size",
    "image_seq_length",
}
_IMAGE_SIZE_KEYS = {
    "height",
    "width",
    "longest_edge",
    "shortest_edge",
    "max_height",
    "max_width",
}


def _request_mm_processor_kwargs(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Parse request preprocessing config before it reaches the vision tower."""

    try:
        return canonical_mm_processor_kwargs(payload.get("mm_processor_kwargs"))
    except HiddenCacheKeyError as exc:
        raise HTTPException(status_code=400, detail=f"invalid mm_processor_kwargs: {exc}") from exc


def _positive_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool):
        raise HTTPException(status_code=400, detail=f"{name} must be a positive integer")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{name} must be a positive integer") from exc
    if normalized <= 0 or normalized != value:
        raise HTTPException(status_code=400, detail=f"{name} must be a positive integer")
    return normalized


def _image_processor_size_defaults(processor: Any) -> Dict[str, Any]:
    image_processor = getattr(processor, "image_processor", None)
    raw_size = getattr(image_processor, "size", None)
    if isinstance(raw_size, Mapping):
        return dict(raw_size)
    try:
        return dict(raw_size or {})
    except (TypeError, ValueError):
        return {}


def _image_processor_kwargs_for_request(
    processor: Any,
    request_kwargs: Mapping[str, Any],
) -> Dict[str, Any]:
    """Resolve vLLM image kwargs into the exact HF image-processor ABI.

    Qwen vLLM maps ``max_pixels``/``min_pixels`` to the image processor's
    ``size.longest_edge``/``size.shortest_edge`` before preprocessing.  Calling
    HuggingFace's processor with ``max_pixels`` directly is a no-op in the
    installed Qwen processor, which previously let E and P compute different
    image grids.  Resolve that mapping once and pass the resolved image kwargs
    to both generic Qwen-VL and Qwen2.5-Omni encoder workers.
    """

    raw = dict(request_kwargs)
    unsupported = sorted(set(raw).difference(_SUPPORTED_IMAGE_PROCESSOR_KWARGS))
    if unsupported:
        raise HTTPException(
            status_code=400,
            detail=(
                "unsupported mm_processor_kwargs for EPD image encoding: "
                + ", ".join(unsupported)
            ),
        )

    resolved: Dict[str, Any] = {}
    for key, value in raw.items():
        if key in {"size", "min_pixels", "max_pixels"}:
            continue
        resolved[key] = value

    size = _image_processor_size_defaults(processor)
    raw_size = raw.get("size")
    if raw_size is not None:
        if not isinstance(raw_size, Mapping):
            raise HTTPException(status_code=400, detail="mm_processor_kwargs.size must be a mapping")
        unknown_size_keys = sorted(set(raw_size).difference(_IMAGE_SIZE_KEYS))
        if unknown_size_keys:
            raise HTTPException(
                status_code=400,
                detail="unsupported image size keys: " + ", ".join(unknown_size_keys),
            )
        for key, value in raw_size.items():
            if value is None:
                continue
            size[str(key)] = _positive_int(value, name=f"mm_processor_kwargs.size.{key}")
    if "min_pixels" in raw:
        size["shortest_edge"] = _positive_int(
            raw["min_pixels"],
            name="mm_processor_kwargs.min_pixels",
        )
    if "max_pixels" in raw:
        size["longest_edge"] = _positive_int(
            raw["max_pixels"],
            name="mm_processor_kwargs.max_pixels",
        )
    min_pixels = size.get("shortest_edge")
    max_pixels = size.get("longest_edge")
    if min_pixels is not None and max_pixels is not None and int(min_pixels) > int(max_pixels):
        raise HTTPException(
            status_code=400,
            detail="mm_processor_kwargs.min_pixels cannot exceed max_pixels",
        )
    if raw_size is not None or "min_pixels" in raw or "max_pixels" in raw:
        resolved["size"] = size
    return resolved


def _preprocessing_fingerprint(request_kwargs: Mapping[str, Any]) -> str:
    if not request_kwargs:
        return "default"
    raw = json.dumps(
        canonical_mm_processor_kwargs(request_kwargs),
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _call_accepts_keyword(fn: Any, name: str) -> bool:
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    return name in signature.parameters or any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )


def _processor_inputs(
    processor: Any,
    image: Image.Image,
    prompt: str,
    *,
    image_processor_kwargs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, torch.Tensor]:
    image_processor_kwargs = dict(image_processor_kwargs or {})
    image_processor = getattr(processor, "image_processor", None)
    if callable(image_processor):
        # Direct image preprocessing is tensor-identical to Qwen's
        # apply_chat_template -> processor route, but avoids text tokenization
        # on the E-stage hot path.  It also makes the resolved vLLM resize
        # configuration explicit instead of silently relying on HF defaults.
        return image_processor(
            images=[image.convert("RGB")],
            return_tensors="pt",
            **image_processor_kwargs,
        )
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image", "image": image},
                {"type": "text", "text": prompt},
            ],
        }
    ]
    return processor.apply_chat_template(
        messages,
        tokenize=True,
        add_generation_prompt=True,
        return_dict=True,
        return_tensors="pt",
        processor_kwargs={"images_kwargs": image_processor_kwargs},
    )


def _stable_mm_hash(item: Dict[str, Any]) -> str:
    return stable_multimodal_asset_hash(item)


def _feature_cache_key(
    config: EncoderServiceConfig,
    *,
    source_digest: str,
    request_processor_kwargs: Optional[Mapping[str, Any]] = None,
) -> str:
    payload = {
        "model": str(config.model),
        "dtype": str(config.dtype),
        "encoder_family": str(config.encoder_family),
        "source_digest": str(source_digest),
        "mm_processor_kwargs": canonical_mm_processor_kwargs(request_processor_kwargs),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _direct_feature_targets_for_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    metadata = dict(payload.get("metadata") or {})
    raw = (
        metadata.get("mooncake_epd_direct_feature_targets")
        or metadata.get("direct_feature_targets")
        or payload.get("mooncake_epd_direct_feature_targets")
        or payload.get("direct_feature_targets")
        or []
    )
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="direct feature targets must be a list")
    return [dict(item or {}) for item in raw]


def _publish_bundle(
    config: EncoderServiceConfig,
    bundle,
    metadata: Dict[str, Any],
    direct_target: Optional[Dict[str, Any]] = None,
    direct_transfer_engine: Optional[TransferEngine] = None,
) -> FeatureHandle:
    if config.publish_backend == "file":
        return publish_feature_bundle_to_dir(
            bundle,
            config.store_dir,
            checksum=bool(config.checksum),
            metadata=metadata,
        )
    if config.publish_backend == "mooncake":
        store = MooncakeFeatureBundleStore(
            MooncakeFeatureBundleStoreConfig(
                store_id=config.mooncake_store_id,
                store_url=config.mooncake_store_url,
                config_path=config.mooncake_config,
                timeout_s=float(config.mooncake_timeout_s),
            )
        )
        try:
            return store.publish_bundle(
                bundle,
                checksum=bool(config.checksum),
                metadata=metadata,
            )
        finally:
            store.close()
    if config.publish_backend == "direct_engine":
        return _publish_direct_bundles(
            config,
            [(bundle, metadata, direct_target)],
            direct_transfer_engine=direct_transfer_engine,
        )[0]
    raise ValueError(f"unsupported publish backend: {config.publish_backend}")


def _publish_direct_bundles(
    config: EncoderServiceConfig,
    entries: Sequence[Tuple[Any, Dict[str, Any], Optional[Dict[str, Any]]]],
    *,
    direct_transfer_engine: Optional[TransferEngine] = None,
) -> List[FeatureHandle]:
    """Publish same-Prefill FeatureBundles in one Mooncake write.

    ``TransferEngine`` can batch tensor descriptors only when they share a
    remote session.  The HTTP handler groups records before calling this helper
    so one multimodal request avoids one Mooncake dispatch per image while
    retaining a distinct, ordered FeatureHandle for every descriptor.
    """

    if not entries:
        return []
    engine = direct_transfer_engine or TransferEngine(
        protocol=os.getenv("MOONCAKE_EPD_DIRECT_ENGINE_PROTOCOL", config.mooncake_protocol),
        local_hostname=config.mooncake_local_hostname,
        metadata_server=config.mooncake_metadata_server,
        device_name=config.mooncake_device_name,
    )
    bundles: List[Any] = []
    plans = []
    for bundle, _metadata, direct_target in entries:
        if not isinstance(direct_target, dict) or not direct_target:
            raise ValueError(
                "publish_backend=direct_engine requires metadata.mooncake_epd_direct_feature_targets"
            )
        remote_session = str(direct_target.get("remote_session") or "")
        remote_pointers = direct_target.get("remote_pointers")
        if not isinstance(remote_pointers, dict):
            raise ValueError("direct feature target requires remote_pointers dict")
        normalized_target = dict(direct_target)
        normalized_target["remote_session"] = remote_session
        normalized_target["remote_pointers"] = {
            str(key): int(value) for key, value in remote_pointers.items()
        }
        bundles.append(bundle)
        plans.append(
            engine.build_feature_bundle_peer_buffer_plan(
                bundle,
                remote_session=remote_session,
                remote_pointers=normalized_target["remote_pointers"],
                checksum=bool(config.checksum),
            )
        )

    results = engine.transfer_feature_bundle_peer_buffer_plans(
        bundles,
        plans,
        source_memory_mode=config.direct_source_mode,
    )
    handles: List[FeatureHandle] = []
    for (bundle, metadata, direct_target), plan, result in zip(entries, plans, results):
        descriptor = bundle.descriptor(checksum=bool(config.checksum))
        md = dict(metadata)
        direct_allocation_id = str(
            dict(direct_target or {}).get("allocation_id") or ""
        ).strip()
        md.update(
            {
                "backend": "direct_engine",
                "direct_backend": result.backend_label,
                "direct_remote_session": plan.remote_session,
                "direct_tensor_count": result.tensor_count,
                "direct_descriptor_count": result.descriptor_count,
                "direct_bytes": result.nbytes,
                "direct_transfer_timings_ms": dict(result.timings_ms or {}),
                "direct_plan": {
                    "feature_id": plan.feature_id,
                    "targets": [
                        {
                            "name": target.name,
                            "remote_pointer": target.remote_pointer,
                            "nbytes": target.nbytes,
                        }
                        for target in plan.targets
                    ],
                },
            }
        )
        if direct_allocation_id:
            md["direct_allocation_id"] = direct_allocation_id
        handles.append(
            FeatureHandle(
                handle_id=f"direct-{bundle.image_hash}-{int(time.time() * 1_000_000)}",
                feature_id=str(bundle.image_hash),
                store_id=config.mooncake_store_id,
                uri=f"epd-direct://{config.mooncake_store_id}/{bundle.image_hash}",
                descriptor=descriptor,
                metadata=md,
            )
        )
    return handles


def _build_direct_handle_without_publish(
    config: EncoderServiceConfig,
    bundle: FeatureBundle,
    metadata: Dict[str, Any],
    direct_target: Dict[str, Any],
) -> FeatureHandle:
    """Build an epd-direct handle for an already-populated Prefill buffer."""

    remote_session = str(direct_target.get("remote_session") or "")
    direct_allocation_id = str(direct_target.get("allocation_id") or "").strip()
    remote_pointers = direct_target.get("remote_pointers")
    if not remote_session or not isinstance(remote_pointers, dict):
        raise ValueError("cached direct feature target requires remote_session and remote_pointers")
    descriptor = bundle.descriptor(checksum=bool(config.checksum))
    plan_targets = []
    for name, pointer in {str(k): int(v) for k, v in remote_pointers.items()}.items():
        if name.endswith(":nbytes"):
            continue
        nbytes = int(remote_pointers.get(f"{name}:nbytes", 0) or 0)
        if nbytes <= 0:
            raise ValueError(f"cached direct target missing nbytes for {name}")
        plan_targets.append(
            {
                "name": name,
                "remote_pointer": int(pointer),
                "nbytes": nbytes,
            }
        )
    md = dict(metadata)
    md.update(
        {
            "backend": "direct_engine",
            "direct_backend": "prefill_persistent_cache",
            "direct_remote_session": remote_session,
            "direct_tensor_count": len(plan_targets),
            "direct_descriptor_count": len(plan_targets),
            "direct_bytes": sum(int(item["nbytes"]) for item in plan_targets),
            "direct_transfer_timings_ms": {
                "total_ms": 0.0,
                "write_ms": 0.0,
                "register_memory_ms": 0.0,
                "unregister_memory_ms": 0.0,
                "descriptor_count": float(len(plan_targets)),
                "nbytes": float(sum(int(item["nbytes"]) for item in plan_targets)),
            },
            "direct_plan": {
                "feature_id": str(bundle.image_hash),
                "targets": plan_targets,
            },
        }
    )
    if direct_allocation_id:
        md["direct_allocation_id"] = direct_allocation_id
    return FeatureHandle(
        handle_id=f"direct-cache-{bundle.image_hash}-{int(time.time() * 1_000_000)}",
        feature_id=str(bundle.image_hash),
        store_id=config.mooncake_store_id,
        uri=f"epd-direct://{config.mooncake_store_id}/{bundle.image_hash}",
        descriptor=descriptor,
        metadata=md,
    )


def parse_args() -> EncoderServiceConfig:
    ap = argparse.ArgumentParser(description="Mooncake EPD online encoder service")
    ap.add_argument("--model", default=os.getenv("MOONCAKE_EPD_ENCODER_MODEL", EncoderServiceConfig.model))
    ap.add_argument("--device", default=os.getenv("MOONCAKE_EPD_ENCODER_DEVICE", EncoderServiceConfig.device))
    ap.add_argument("--dtype", default=os.getenv("MOONCAKE_EPD_ENCODER_DTYPE", EncoderServiceConfig.dtype))
    ap.add_argument(
        "--encoder-family",
        choices=["auto", "qwen3_vl", "qwen2_5_vl", "qwen2_5_omni"],
        default=os.getenv("MOONCAKE_EPD_ENCODER_FAMILY", "auto"),
    )
    ap.add_argument(
        "--encoder-runtime",
        choices=sorted(_ENCODER_RUNTIMES),
        default=os.getenv("MOONCAKE_EPD_ENCODER_RUNTIME", EncoderServiceConfig.encoder_runtime),
        help=(
            "E-stage implementation; vllm_native is Qwen3-VL-only and emits "
            "the exact vLLM packed DeepStack visual ABI."
        ),
    )
    ap.add_argument("--publish-backend", choices=["file", "mooncake", "direct_engine"], default=os.getenv("MOONCAKE_EPD_ENCODER_PUBLISH_BACKEND", "file"))
    ap.add_argument("--mooncake-local-hostname", default=os.getenv("MOONCAKE_LOCAL_HOSTNAME", "localhost"))
    ap.add_argument("--mooncake-metadata-server", default=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"))
    ap.add_argument("--mooncake-device-name", default=os.getenv("MOONCAKE_DEVICE_NAME", ""))
    ap.add_argument("--direct-source-mode", choices=["registered_tensor", "managed_buffer"], default=os.getenv("MOONCAKE_EPD_DIRECT_SOURCE_MODE", "registered_tensor"))
    ap.add_argument("--store-dir", default=os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_DIR", EncoderServiceConfig.store_dir))
    ap.add_argument("--mooncake-store-url", default=os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_URL"))
    ap.add_argument("--mooncake-store-id", default=os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_ID", EncoderServiceConfig.mooncake_store_id))
    ap.add_argument("--mooncake-config", default=os.getenv("MOONCAKE_CONFIG_PATH"))
    ap.add_argument("--mooncake-timeout-s", type=float, default=float(os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_TIMEOUT_S", "30")))
    ap.add_argument("--mooncake-protocol", default=os.getenv("MOONCAKE_PROTOCOL", EncoderServiceConfig.mooncake_protocol))
    ap.add_argument("--checksum", action=argparse.BooleanOptionalAction, default=os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_CHECKSUM", "0") in {"1", "true", "TRUE"})
    ap.add_argument("--max-image-bytes", type=int, default=int(os.getenv("MOONCAKE_EPD_ENCODER_MAX_IMAGE_BYTES", str(32 * 1024 * 1024))))
    ap.add_argument("--request-timeout-s", type=float, default=float(os.getenv("MOONCAKE_EPD_ENCODER_REQUEST_TIMEOUT_S", "15")))
    ap.add_argument(
        "--allowed-image-root",
        default=os.getenv("MOONCAKE_EPD_ENCODER_ALLOWED_IMAGE_ROOT"),
    )
    ap.add_argument(
        "--allow-http-images",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_ENCODER_ALLOW_HTTP_IMAGES", "0").lower()
        in {"1", "true", "yes", "on"},
    )
    ap.add_argument(
        "--allowed-http-hosts",
        nargs="*",
        default=[
            value.strip()
            for value in os.getenv("MOONCAKE_EPD_ENCODER_ALLOWED_HTTP_HOSTS", "").split(",")
            if value.strip()
        ],
    )
    ap.add_argument(
        "--allow-file-images",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_ENCODER_ALLOW_FILE_IMAGES", "0").lower()
        in {"1", "true", "yes", "on"},
    )
    ap.add_argument("--feature-cache-entries", type=int, default=int(os.getenv("MOONCAKE_EPD_ENCODER_FEATURE_CACHE_ENTRIES", "64")))
    ap.add_argument("--feature-cache-max-bytes", type=int, default=int(os.getenv("MOONCAKE_EPD_ENCODER_FEATURE_CACHE_MAX_BYTES", str(2 * 1024 * 1024 * 1024))))
    ap.add_argument(
        "--warmup-on-startup",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_ENCODER_WARMUP_ON_STARTUP", "1").lower()
        not in {"0", "false", "no", "off"},
    )
    ap.add_argument(
        "--enable-omni-hidden-prefix-cache",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE", "1").lower()
        not in {"0", "false", "no", "off"},
    )
    ap.add_argument(
        "--omni-hidden-prefix-cache-metrics",
        default=os.getenv("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_METRICS"),
    )
    ap.add_argument(
        "--omni-allow-partial-prefix",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_ALLOW_PARTIAL", "0").lower()
        in {"1", "true", "yes", "on"},
    )
    ap.add_argument(
        "--omni-partial-oracle-validated",
        action=argparse.BooleanOptionalAction,
        default=os.getenv(
            "MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_PARTIAL_ORACLE_VALIDATED",
            "0",
        ).lower()
        in {"1", "true", "yes", "on"},
        help="Required together with --omni-allow-partial-prefix; records that the model-specific oracle gate passed.",
    )
    args = ap.parse_args()
    payload = vars(args)
    payload["allowed_http_hosts"] = tuple(payload.get("allowed_http_hosts") or ())
    return EncoderServiceConfig(**payload)


def main() -> None:
    import uvicorn

    config = parse_args()
    host = os.getenv("MOONCAKE_EPD_ENCODER_HOST", "127.0.0.1")
    port = int(os.getenv("MOONCAKE_EPD_ENCODER_PORT", "8300"))
    uvicorn.run(create_app(config), host=host, port=port)


if __name__ == "__main__":
    main()
