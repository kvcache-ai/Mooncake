"""Production-oriented vLLM disaggregated proxy with EPD control-plane hooks.

Compared with the upstream Mooncake proxy, this variant adds:

- stable ``transfer_id`` propagation on the first prefill leg;
- admission / backpressure before prefill and decode dispatch;
- A2A-style 2PC handoff bookkeeping around the P->D transition;
- request-level metadata injection for layered KV transfer, MM prefetch, and
  transport backend hints;
- health / metrics endpoints for operational inspection.

The downstream data plane is still real vLLM + Mooncake. This proxy only owns
routing, metadata propagation, and serving-time control semantics.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import ipaddress
import json
import logging
import os
import random
import socket
import sys
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlsplit

import httpx
import tokenizers
import torch
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from vllm.tokenizers import get_tokenizer

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.agent.coordination.scheduler import AdmissionAction  # noqa: E402
from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig  # noqa: E402
from mooncake_epd.core.control.vllm_incarnation import (  # noqa: E402
    VLLM_EXPECTED_INCARNATION_HEADER,
    VLLM_INCARNATION_ENDPOINT,
    VLLM_INCARNATION_HEADER,
    VLLM_INCARNATION_MISMATCH_HEADER,
)
from mooncake_epd.core.strict_mode import strict_no_fallback_enabled  # noqa: E402
from mooncake_epd.core.state import FeatureBundle, FeatureHandle, MMStore  # noqa: E402
from mooncake_epd.core.transfer import TransferEngine  # noqa: E402


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class ProxyConfig:
    host: str = "127.0.0.1"
    port: int = 8000
    prefiller_instances: List[tuple[str, int]] = field(default_factory=lambda: [("127.0.0.1", 8100)])
    decoder_instances: List[tuple[str, int]] = field(default_factory=lambda: [("127.0.0.1", 8200)])
    layers_per_group: int = 4
    group_delay_ms: float = 0.0
    max_group_bytes: int = 0
    warn_rho: float = 0.85
    critical_rho: float = 0.95
    max_backpressure_delay_ms: float = 150.0
    transport_backend: str = "mooncake_engine_direct"
    node_id: str = "proxy"
    owner_shards: int = 1
    kv_directory_rpc_url: Optional[str] = None
    connector_metrics_dir: Optional[str] = None
    workflow_registry_wal_path: Optional[str] = None
    workflow_registry_wal_fsync_interval_s: float = 0.25
    workflow_registry_wal_max_pending_records: int = 64
    enable_decode_pipeline: bool = False
    decode_pipeline_max_inflight: int = 0
    enable_prerendered_decode: bool = False
    prerendered_decode_model: Optional[str] = None
    enable_decode_mm_hash_cache: bool = False
    decode_mm_hash_cache_max_entries: int = 64
    decode_mm_hash_cache_ttl_s: float = 120.0
    decode_mm_hash_epoch_poll_s: float = 1.0
    decode_mm_hash_epoch_probe_timeout_s: float = 0.5
    decode_mm_hash_epoch_freshness_s: float = 0.0
    decode_mm_hash_epoch_endpoint: str = "/metrics"
    enable_decode_mm_hash_epoch_guard: bool = False
    enable_decode_mm_hash_epoch_probe_singleflight: bool = True
    enable_mm_prefetch: bool = True
    mm_prefetch_mode: str = "asset_bytes"
    prefill_supports_feature_handles: bool = False
    mm_prefetch_wait_ms: float = 100.0
    mm_prefetch_max_asset_bytes: int = 16 * 1024 * 1024
    mm_prefetch_queue_size: int = 256
    allow_private_mm_urls: bool = False
    encoder_service_url: Optional[str] = None
    encoder_service_timeout_s: float = 120.0
    prefill_direct_buffer_service_url: Optional[str] = None
    prefill_direct_buffer_timeout_s: float = 30.0
    release_direct_feature_buffers_after_prefill: bool = True
    enable_direct_feature_handle_cache: bool = False
    direct_feature_handle_cache_max_entries: int = 64
    direct_feature_handle_cache_max_bytes: int = 4 * 1024 * 1024 * 1024
    direct_feature_handle_cache_ttl_s: float = 600.0
    prefill_incarnation_poll_s: float = 0.0
    prefill_incarnation_poll_jitter_ratio: float = 0.0
    prefill_incarnation_failure_threshold: int = 3
    prefill_incarnation_probe_timeout_s: float = 0.5
    prefill_incarnation_freshness_s: float = 0.0
    prefill_incarnation_endpoint: str = VLLM_INCARNATION_ENDPOINT
    enable_prefill_incarnation_guard: bool = False
    enable_prefill_incarnation_probe_singleflight: bool = True
    enable_prefill_render_cache: bool = False
    prefill_render_cache_max_entries: int = 128
    prefill_render_cache_max_bytes: int = 512 * 1024 * 1024
    prefill_render_cache_ttl_s: float = 600.0
    enable_client_mm_uuid_references: bool = False
    strict_no_fallback: bool = field(default_factory=strict_no_fallback_enabled)
    enable_agent_state_clone: bool = True
    high_prefill_worker_ids: List[str] = field(default_factory=list)
    low_latency_decode_worker_ids: List[str] = field(default_factory=list)
    standard_prefill_worker_ids: List[str] = field(default_factory=list)
    standard_decode_worker_ids: List[str] = field(default_factory=list)
    upstream_max_connections: int = 64
    upstream_max_keepalive_connections: int = 16
    upstream_keepalive_expiry_s: float = 1.0


@dataclass
class _DispatchContext:
    request_id: str
    request_body: Dict[str, Any]
    control_ctx: Any


@dataclass
class _PrefillContinuation:
    text: str = ""
    completion_tokens: int = 0
    prompt_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    finish_reason: Optional[str] = None

    @property
    def active(self) -> bool:
        return self.completion_tokens > 0 and bool(self.text)


@dataclass
class _OpenedDecodeStream:
    stream_ctx: Any
    response: httpx.Response
    started_at: float
    opened_at: float


@dataclass
class _DecodeMMHashLease:
    """One request's ownership of a Decode-side multimodal cache assumption."""

    cache: Any
    worker_id: str
    referenced_hashes: Tuple[str, ...]
    promote_hashes: Tuple[str, ...]
    invalidate_hashes: Tuple[str, ...]
    mode: str
    avoided_bytes: int = 0
    expected_worker_epoch: Optional[str] = None
    epoch_guard_attached: bool = False
    epoch_guard_rejection_recorded: bool = False
    finalized: bool = False

    def succeed(self) -> None:
        if self.finalized:
            return
        self.cache.complete(self, success=True)
        self.finalized = True

    def fail(self) -> None:
        if self.finalized:
            return
        self.cache.complete(self, success=False)
        self.finalized = True

    def record_epoch_guard_rejection(
        self,
        actual_worker_epoch: Optional[str] = None,
    ) -> None:
        if self.epoch_guard_rejection_recorded:
            return
        self.cache.record_epoch_guard_rejection(
            worker_id=self.worker_id,
            actual_worker_epoch=actual_worker_epoch,
        )
        self.epoch_guard_rejection_recorded = True


@dataclass
class _PipelinedDecodeDispatch:
    decision: Any
    client: Dict[str, Any]
    started_at: float
    open_task: asyncio.Task
    topology: Dict[str, Any]
    kv_transfer_params: Dict[str, Any]
    mm_hash_lease: Optional[_DecodeMMHashLease] = None


@dataclass(frozen=True)
class _PrerenderedDecodeContext:
    tokenizer: Any
    prompt_token_ids: Tuple[int, ...]
    model: str
    skip_special_tokens: bool = True


class _IncrementalTokenDecoder:
    """Per-request incremental decoder for the token-only disagg endpoint.

    Fast HF tokenizers use the Rust ``DecodeStream`` primed with prompt token
    IDs, preserving byte-fallback and whitespace semantics without repeatedly
    decoding the complete output.  The cumulative fallback keeps injected test
    tokenizers and uncommon slow tokenizers correct.
    """

    def __init__(
        self,
        tokenizer_obj: Any,
        *,
        prompt_token_ids: Sequence[int],
        skip_special_tokens: bool,
    ) -> None:
        self._tokenizer = tokenizer_obj
        self._skip_special_tokens = bool(skip_special_tokens)
        self._prompt_token_ids = [int(token_id) for token_id in prompt_token_ids]
        self._output_token_ids: List[int] = []
        self._decoded_text = ""
        self._stream = None
        backend = getattr(tokenizer_obj, "_tokenizer", None)
        if backend is not None:
            try:
                self._stream = tokenizers.decoders.DecodeStream(
                    ids=self._prompt_token_ids,
                    skip_special_tokens=self._skip_special_tokens,
                )
                self._backend = backend
            except Exception:
                logger.exception(
                    "failed to initialize fast incremental detokenizer; "
                    "using cumulative decode"
                )
                self._stream = None
                self._backend = None
        else:
            self._backend = None

    def push(self, token_ids: Sequence[int]) -> str:
        normalized = [int(token_id) for token_id in token_ids]
        if not normalized:
            return ""
        if self._stream is not None and self._backend is not None:
            pieces: List[str] = []
            for token_id in normalized:
                piece = self._stream.step(self._backend, token_id)
                if piece:
                    pieces.append(piece)
            self._output_token_ids.extend(normalized)
            decoded = "".join(pieces)
            self._decoded_text += decoded
            return decoded

        self._output_token_ids.extend(normalized)
        prompt_text = str(
            self._tokenizer.decode(
                self._prompt_token_ids,
                skip_special_tokens=self._skip_special_tokens,
            )
        )
        full_text = str(
            self._tokenizer.decode(
                self._prompt_token_ids + self._output_token_ids,
                skip_special_tokens=self._skip_special_tokens,
            )
        )
        if not full_text.startswith(prompt_text):
            raise RuntimeError(
                "tokenizer changed the decoded prompt boundary; strict "
                "prerendered Decode cannot preserve streaming semantics"
            )
        decoded = full_text[len(prompt_text) :]
        if not decoded.startswith(self._decoded_text):
            raise RuntimeError(
                "tokenizer produced a non-monotonic cumulative decode; "
                "strict prerendered Decode cannot preserve streaming semantics"
            )
        delta = decoded[len(self._decoded_text) :]
        self._decoded_text = decoded
        return delta


class _DecodePipelineStartupError(RuntimeError):
    """Raised when an early Decode stream fails before Prefill completes."""


@dataclass
class _CachedDirectFeatureHandle:
    handle: Dict[str, Any]
    nbytes: int
    expires_at: float


class _DirectFeatureHandleCache:
    """Bounded Prefill-owned cache for reusable E-stage feature buffers.

    Entries retain the original ``epd-direct://`` handle and destination
    allocation.  A vLLM MM-cache hit avoids resolving the handle entirely; if
    that cache evicts, the still-live direct allocation remains a correctness
    fallback and can be read again without rerunning the Encoder.
    """

    def __init__(self, *, enabled: bool, max_entries: int, max_bytes: int, ttl_s: float) -> None:
        self.enabled = bool(enabled)
        self.max_entries = max(1, int(max_entries))
        self.max_bytes = max(1, int(max_bytes))
        self.ttl_s = max(0.0, float(ttl_s))
        self._entries: "OrderedDict[Tuple[str, str], _CachedDirectFeatureHandle]" = OrderedDict()
        self._bytes = 0
        self._hits = 0
        self._misses = 0
        self._stores = 0
        self._evictions = 0
        self._expired = 0
        self._worker_incarnations: Dict[str, str] = {}
        self._worker_incarnation_observed_at: Dict[str, float] = {}
        self._worker_incarnation_observations = 0
        self._worker_incarnation_changes = 0
        self._worker_invalidations = 0
        self._worker_invalidated_entries = 0
        self._worker_unavailable = 0
        self._worker_unavailable_transitions = 0
        self._worker_recovery_transitions = 0
        self._worker_availability: Dict[str, bool] = {}
        self._incarnation_probe_responses = 0
        self._incarnation_probe_failures = 0
        self._incarnation_probe_response_bytes = 0
        self._incarnation_probe_latency_ms = 0.0
        self._incarnation_monitor_failure_streaks: Dict[str, int] = {}
        self._incarnation_monitor_max_failure_streak = 0
        self._incarnation_monitor_transient_failures = 0
        self._incarnation_monitor_threshold_reaches = 0
        self._incarnation_monitor_streak_resets = 0
        self._incarnation_monitor_superseded_failures = 0
        self._synchronous_incarnation_probes = 0
        self._synchronous_incarnation_probe_dispatches = 0
        self._synchronous_incarnation_probe_collapsed = 0
        self._incarnation_freshness_skips = 0
        self._incarnation_guarded_requests = 0
        self._incarnation_guard_rejections = 0
        self._stale_singleflight_rejections = 0

    @staticmethod
    def _key(target_worker_id: str, feature_id: str) -> Tuple[str, str]:
        return str(target_worker_id), str(feature_id)

    def has_worker_entries(self, target_worker_id: str) -> bool:
        worker_id = str(target_worker_id)
        return any(key[0] == worker_id for key in self._entries)

    def worker_incarnation(self, target_worker_id: str) -> Optional[str]:
        return self._worker_incarnations.get(str(target_worker_id))

    def worker_incarnation_observed_since(
        self,
        target_worker_id: str,
        started_at: float,
    ) -> bool:
        observed_at = self._worker_incarnation_observed_at.get(
            str(target_worker_id)
        )
        return observed_at is not None and observed_at > float(started_at)

    def use_fresh_worker_incarnation(
        self,
        target_worker_id: str,
        freshness_s: float,
    ) -> bool:
        freshness_s = max(0.0, float(freshness_s))
        worker_id = str(target_worker_id)
        observed_at = self._worker_incarnation_observed_at.get(worker_id)
        if (
            freshness_s <= 0
            or observed_at is None
            or worker_id not in self._worker_incarnations
            or time.monotonic() - observed_at > freshness_s
        ):
            return False
        self._incarnation_freshness_skips += 1
        return True

    def _invalidate_worker_entries(self, target_worker_id: str) -> List[str]:
        worker_id = str(target_worker_id)
        feature_ids: List[str] = []
        for key in [key for key in self._entries if key[0] == worker_id]:
            entry = self._entries.pop(key)
            self._bytes -= int(entry.nbytes)
            feature_ids.append(str(entry.handle.get("feature_id") or key[1]))
        if feature_ids:
            self._worker_invalidations += 1
            self._worker_invalidated_entries += len(feature_ids)
        return sorted(set(feature_id for feature_id in feature_ids if feature_id))

    def invalidate_worker(self, target_worker_id: str) -> List[str]:
        return self._invalidate_worker_entries(target_worker_id)

    def mark_worker_unavailable(self, target_worker_id: str) -> List[str]:
        worker_id = str(target_worker_id)
        self._worker_unavailable += 1
        if self._worker_availability.get(worker_id) is not False:
            self._worker_unavailable_transitions += 1
        self._worker_availability[worker_id] = False
        # Keep the last authoritative token so the next successful probe can
        # distinguish a restart from a transient outage.  Freshness is revoked
        # and all process-owned entries are still invalidated immediately.
        self._worker_incarnation_observed_at.pop(worker_id, None)
        return self._invalidate_worker_entries(worker_id)

    def mark_worker_available(self, target_worker_id: str) -> None:
        worker_id = str(target_worker_id)
        previous = self._worker_availability.get(worker_id)
        self._worker_availability[worker_id] = True
        if previous is False:
            self._worker_recovery_transitions += 1

    def observe_worker_incarnation(
        self,
        target_worker_id: str,
        incarnation: str,
    ) -> Tuple[bool, List[str]]:
        worker_id = str(target_worker_id)
        token = _bounded_epoch_token(incarnation)
        if token is None:
            raise ValueError("invalid Prefill process-incarnation token")
        previous = self._worker_incarnations.get(worker_id)
        self._worker_incarnation_observations += 1
        changed = previous is not None and previous != token
        invalidated = self._invalidate_worker_entries(worker_id) if changed else []
        if changed:
            self._worker_incarnation_changes += 1
        self._worker_incarnations[worker_id] = token
        self._worker_incarnation_observed_at[worker_id] = time.monotonic()
        # Any authoritative success, including a synchronous request fence,
        # breaks the background monitor's consecutive-failure sequence.
        self.reset_incarnation_monitor_failure_streak(worker_id)
        return changed, invalidated

    def record_incarnation_probe_response(
        self,
        *,
        response_bytes: int,
        latency_s: float,
    ) -> None:
        self._incarnation_probe_responses += 1
        self._incarnation_probe_response_bytes += max(0, int(response_bytes))
        self._incarnation_probe_latency_ms += max(0.0, float(latency_s)) * 1000.0

    def record_incarnation_probe_failure(self) -> None:
        self._incarnation_probe_failures += 1

    def reset_incarnation_monitor_failure_streak(
        self,
        target_worker_id: str,
    ) -> None:
        worker_id = str(target_worker_id)
        if self._incarnation_monitor_failure_streaks.pop(worker_id, 0) > 0:
            self._incarnation_monitor_streak_resets += 1

    def record_superseded_incarnation_monitor_failure(self) -> None:
        self._incarnation_monitor_superseded_failures += 1

    def record_incarnation_monitor_probe_result(
        self,
        target_worker_id: str,
        *,
        healthy: bool,
        failure_threshold: int,
    ) -> bool:
        """Track monitor-only failures and signal one threshold transition.

        Synchronous freshness probes deliberately do not use this debounce: a
        request that cannot prove the current Prefill incarnation must still
        fail closed.  The threshold only prevents a single background timeout
        from removing an otherwise healthy worker from scheduling.
        """

        worker_id = str(target_worker_id)
        threshold = max(1, int(failure_threshold))
        previous = self._incarnation_monitor_failure_streaks.get(worker_id, 0)
        if healthy:
            self.reset_incarnation_monitor_failure_streak(worker_id)
            return False

        streak = previous + 1
        self._incarnation_monitor_failure_streaks[worker_id] = streak
        self._incarnation_monitor_max_failure_streak = max(
            self._incarnation_monitor_max_failure_streak,
            streak,
        )
        if streak < threshold:
            self._incarnation_monitor_transient_failures += 1
            return False
        if streak == threshold:
            self._incarnation_monitor_threshold_reaches += 1
            return True
        return False

    def record_synchronous_incarnation_probe(self) -> None:
        self._synchronous_incarnation_probes += 1

    def record_synchronous_incarnation_probe_dispatch(self) -> None:
        self._synchronous_incarnation_probe_dispatches += 1

    def record_synchronous_incarnation_probe_collapsed(self) -> None:
        self._synchronous_incarnation_probe_collapsed += 1

    def record_incarnation_guarded_request(self) -> None:
        self._incarnation_guarded_requests += 1

    def record_incarnation_guard_rejection(self) -> None:
        self._incarnation_guard_rejections += 1

    def get_many(
        self,
        *,
        target_worker_id: str,
        feature_ids: Sequence[str],
    ) -> Tuple[Optional[List[Dict[str, Any]]], List[str]]:
        if not self.enabled or not feature_ids:
            return None, []
        now = time.monotonic()
        handles: List[Dict[str, Any]] = []
        expired_ids: List[str] = []
        for feature_id in feature_ids:
            key = self._key(target_worker_id, feature_id)
            entry = self._entries.get(key)
            if entry is None:
                self._misses += 1
                return None, expired_ids
            if entry.expires_at > 0 and entry.expires_at <= now:
                self._entries.pop(key, None)
                self._bytes -= int(entry.nbytes)
                self._expired += 1
                self._misses += 1
                expired_ids.append(str(entry.handle.get("feature_id") or feature_id))
                return None, expired_ids
            self._entries.move_to_end(key)
            handles.append(dict(entry.handle))
        self._hits += len(handles)
        return handles, expired_ids

    def put_many(
        self,
        *,
        target_worker_id: str,
        handles: Sequence[Dict[str, Any]],
        expected_worker_incarnation: Optional[str] = None,
        enforce_expected_incarnation: bool = False,
    ) -> Tuple[bool, List[str]]:
        if not self.enabled:
            return False, []
        evicted_ids: List[str] = []
        incarnations = {
            token
            for raw_handle in handles
            if (
                token := _bounded_epoch_token(
                    (dict(raw_handle).get("metadata") or {}).get(
                        "direct_remote_incarnation"
                    )
                )
            )
            is not None
        }
        if len(incarnations) > 1:
            raise ValueError(
                "direct feature handles disagree on Prefill process incarnation"
            )
        incoming_incarnation = next(iter(incarnations)) if incarnations else None
        expected_incarnation = _bounded_epoch_token(expected_worker_incarnation)
        current_incarnation = self.worker_incarnation(target_worker_id)
        if (
            enforce_expected_incarnation
            and
            current_incarnation is not None
            and current_incarnation != expected_incarnation
            and incoming_incarnation != current_incarnation
        ):
            self._stale_singleflight_rejections += 1
            raise RuntimeError(
                "stale Prefill direct-handle singleflight completed after an incarnation change"
            )
        incarnation_changed = False
        if incoming_incarnation is not None:
            incarnation_changed, invalidated_ids = self.observe_worker_incarnation(
                target_worker_id,
                incoming_incarnation,
            )
            evicted_ids.extend(invalidated_ids)
        now = time.monotonic()
        expires_at = now + self.ttl_s if self.ttl_s > 0 else 0.0
        for raw_handle in handles:
            handle = dict(raw_handle)
            feature_id = str(
                (handle.get("metadata") or {}).get("source_mm_hash")
                or handle.get("feature_id")
                or ""
            )
            if not feature_id:
                continue
            descriptor = dict(handle.get("descriptor") or {})
            nbytes = int(descriptor.get("nbytes", 0) or 0)
            if nbytes <= 0 or nbytes > self.max_bytes:
                continue
            key = self._key(target_worker_id, feature_id)
            previous = self._entries.pop(key, None)
            if previous is not None:
                self._bytes -= int(previous.nbytes)
            self._entries[key] = _CachedDirectFeatureHandle(
                handle=handle,
                nbytes=nbytes,
                expires_at=expires_at,
            )
            self._bytes += nbytes
            self._stores += 1

        while self._entries and (
            len(self._entries) > self.max_entries or self._bytes > self.max_bytes
        ):
            (_worker, feature_id), entry = self._entries.popitem(last=False)
            self._bytes -= int(entry.nbytes)
            self._evictions += 1
            evicted_ids.append(str(entry.handle.get("feature_id") or feature_id))
        return incarnation_changed, sorted(set(evicted_ids))

    def drain(self) -> List[str]:
        feature_ids = [
            str(entry.handle.get("feature_id") or key[1])
            for key, entry in self._entries.items()
        ]
        self._entries.clear()
        self._bytes = 0
        self._worker_incarnations.clear()
        self._worker_incarnation_observed_at.clear()
        self._worker_availability.clear()
        self._incarnation_monitor_failure_streaks.clear()
        return sorted(set(fid for fid in feature_ids if fid))

    def stats(self) -> Dict[str, Any]:
        lookups = self._hits + self._misses
        return {
            "enabled": self.enabled,
            "entries": len(self._entries),
            "bytes": self._bytes,
            "max_entries": self.max_entries,
            "max_bytes": self.max_bytes,
            "ttl_s": self.ttl_s,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": (float(self._hits) / float(lookups)) if lookups else 0.0,
            "stores": self._stores,
            "evictions": self._evictions,
            "expired": self._expired,
            "worker_incarnations": dict(self._worker_incarnations),
            "worker_incarnation_observations": self._worker_incarnation_observations,
            "worker_incarnation_changes": self._worker_incarnation_changes,
            "worker_invalidations": self._worker_invalidations,
            "worker_invalidated_entries": self._worker_invalidated_entries,
            "worker_unavailable": self._worker_unavailable,
            "worker_unavailable_attempts": self._worker_unavailable,
            "worker_unavailable_transitions": self._worker_unavailable_transitions,
            "worker_recovery_transitions": self._worker_recovery_transitions,
            "worker_availability": dict(self._worker_availability),
            "unavailable_workers": sorted(
                worker_id
                for worker_id, available in self._worker_availability.items()
                if not available
            ),
            "incarnation_probe_responses": self._incarnation_probe_responses,
            "incarnation_probe_failures": self._incarnation_probe_failures,
            "incarnation_probe_response_bytes": self._incarnation_probe_response_bytes,
            "incarnation_probe_latency_ms": self._incarnation_probe_latency_ms,
            "incarnation_monitor_failure_streaks": dict(
                self._incarnation_monitor_failure_streaks
            ),
            "incarnation_monitor_max_failure_streak": (
                self._incarnation_monitor_max_failure_streak
            ),
            "incarnation_monitor_transient_failures": (
                self._incarnation_monitor_transient_failures
            ),
            "incarnation_monitor_threshold_reaches": (
                self._incarnation_monitor_threshold_reaches
            ),
            "incarnation_monitor_streak_resets": (
                self._incarnation_monitor_streak_resets
            ),
            "incarnation_monitor_superseded_failures": (
                self._incarnation_monitor_superseded_failures
            ),
            "synchronous_incarnation_probes": self._synchronous_incarnation_probes,
            "synchronous_incarnation_probe_dispatches": (
                self._synchronous_incarnation_probe_dispatches
            ),
            "synchronous_incarnation_probe_collapsed": (
                self._synchronous_incarnation_probe_collapsed
            ),
            "incarnation_freshness_skips": self._incarnation_freshness_skips,
            "incarnation_guarded_requests": self._incarnation_guarded_requests,
            "incarnation_guard_rejections": self._incarnation_guard_rejections,
            "stale_singleflight_rejections": self._stale_singleflight_rejections,
        }


@dataclass
class _CachedPrefillRender:
    payload: Dict[str, Any]
    nbytes: int
    expires_at: float


class _PrefillRenderCache:
    """Bounded semantic cache for vLLM's processor/render response.

    The cached object is an intermediate prompt representation, never a model
    response.  Per-request KV-transfer metadata is injected only after lookup,
    so a hit cannot reuse another request's transfer_id or handoff topology.
    """

    def __init__(self, *, enabled: bool, max_entries: int, max_bytes: int, ttl_s: float) -> None:
        self.enabled = bool(enabled)
        self.max_entries = max(1, int(max_entries))
        self.max_bytes = max(1, int(max_bytes))
        self.ttl_s = max(0.0, float(ttl_s))
        self._entries: "OrderedDict[Tuple[str, str, str, str], _CachedPrefillRender]" = OrderedDict()
        self._bytes = 0
        self._hits = 0
        self._misses = 0
        self._stores = 0
        self._evictions = 0
        self._expired = 0
        self._worker_invalidations = 0
        self._worker_invalidated_entries = 0

    def get(self, key: Tuple[str, str, str, str]) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        entry = self._entries.get(key)
        if entry is None:
            self._misses += 1
            return None
        now = time.monotonic()
        if entry.expires_at > 0 and entry.expires_at <= now:
            self._entries.pop(key, None)
            self._bytes -= entry.nbytes
            self._misses += 1
            self._expired += 1
            return None
        self._entries.move_to_end(key)
        self._hits += 1
        return dict(entry.payload)

    def put(self, key: Tuple[str, str, str, str], payload: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        nbytes = len(encoded)
        if nbytes <= 0 or nbytes > self.max_bytes:
            return
        previous = self._entries.pop(key, None)
        if previous is not None:
            self._bytes -= previous.nbytes
        expires_at = time.monotonic() + self.ttl_s if self.ttl_s > 0 else 0.0
        self._entries[key] = _CachedPrefillRender(
            payload=dict(payload),
            nbytes=nbytes,
            expires_at=expires_at,
        )
        self._bytes += nbytes
        self._stores += 1
        while self._entries and (
            len(self._entries) > self.max_entries or self._bytes > self.max_bytes
        ):
            _, evicted = self._entries.popitem(last=False)
            self._bytes -= evicted.nbytes
            self._evictions += 1

    def clear(self) -> None:
        self._entries.clear()
        self._bytes = 0

    def invalidate_worker(self, worker_id: str) -> int:
        worker_id = str(worker_id)
        removed = 0
        for key in [key for key in self._entries if key[0] == worker_id]:
            entry = self._entries.pop(key)
            self._bytes -= entry.nbytes
            removed += 1
        if removed:
            self._worker_invalidations += 1
            self._worker_invalidated_entries += removed
        return removed

    def stats(self) -> Dict[str, Any]:
        lookups = self._hits + self._misses
        return {
            "enabled": self.enabled,
            "entries": len(self._entries),
            "bytes": self._bytes,
            "max_entries": self.max_entries,
            "max_bytes": self.max_bytes,
            "ttl_s": self.ttl_s,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": (float(self._hits) / float(lookups)) if lookups else 0.0,
            "stores": self._stores,
            "evictions": self._evictions,
            "expired": self._expired,
            "worker_invalidations": self._worker_invalidations,
            "worker_invalidated_entries": self._worker_invalidated_entries,
        }


class _DecodeMMHashWarmCache:
    """Bounded evidence cache for vLLM's Decode-side MM receiver cache.

    vLLM's internal Generate protocol accepts ``features.kwargs_data=None``
    only when the engine-core receiver cache already owns every referenced
    multimodal item. Hashes are therefore promoted only after a real Generate
    response passes protocol and detokenization validation. A failed hash-only
    request invalidates the assumption and is never retried in-place.
    """

    def __init__(self, *, enabled: bool, max_entries: int, ttl_s: float) -> None:
        self.enabled = bool(enabled)
        self.max_entries = max(1, int(max_entries))
        self.ttl_s = max(0.0, float(ttl_s))
        self._entries: "OrderedDict[Tuple[str, str], float]" = OrderedDict()
        self._lookups = 0
        self._hash_only_requests = 0
        self._full_requests = 0
        self._no_feature_requests = 0
        self._promotions = 0
        self._renewals = 0
        self._invalidations = 0
        self._worker_invalidations = 0
        self._worker_invalidated_entries = 0
        self._worker_epochs: Dict[str, str] = {}
        self._worker_epoch_observed_at: Dict[str, float] = {}
        self._epoch_observations = 0
        self._epoch_changes = 0
        self._epoch_probe_failures = 0
        self._epoch_probe_responses = 0
        self._epoch_probe_response_bytes = 0
        self._epoch_probe_latency_seconds = 0.0
        self._epoch_probe_latency_seconds_max = 0.0
        self._epoch_probe_source_counts: Dict[str, int] = {}
        self._epoch_guarded_hash_only_requests = 0
        self._epoch_guard_rejections = 0
        self._epoch_guard_epoch_learns = 0
        self._worker_unavailable_events = 0
        self._epoch_unconfirmed_full_requests = 0
        self._synchronous_epoch_probes = 0
        self._synchronous_epoch_probe_dispatches = 0
        self._synchronous_epoch_probe_collapsed = 0
        self._epoch_freshness_skips = 0
        self._evictions = 0
        self._expired = 0
        self._rejected_cold_metadata_only = 0
        self._avoided_bytes = 0

    @staticmethod
    def _key(worker_id: str, mm_hash: str) -> Tuple[str, str]:
        return str(worker_id), str(mm_hash)

    def _is_warm(self, key: Tuple[str, str], *, now: float) -> bool:
        expires_at = self._entries.get(key)
        if expires_at is None:
            return False
        if expires_at > 0 and expires_at <= now:
            self._entries.pop(key, None)
            self._expired += 1
            return False
        self._entries.move_to_end(key)
        return True

    @staticmethod
    def _flatten_features(
        features: Dict[str, Any],
    ) -> Tuple[List[Tuple[str, str, Optional[str]]], int]:
        mm_hashes = features.get("mm_hashes")
        if not isinstance(mm_hashes, dict) or not mm_hashes:
            raise ValueError("Decode multimodal features require non-empty mm_hashes")
        kwargs_data = features.get("kwargs_data")
        if kwargs_data is not None and not isinstance(kwargs_data, dict):
            raise TypeError("Decode multimodal kwargs_data must be an object or null")

        flattened: List[Tuple[str, str, Optional[str]]] = []
        full_bytes = 0
        for modality, raw_hashes in mm_hashes.items():
            if not isinstance(raw_hashes, list) or not raw_hashes:
                raise ValueError(
                    f"Decode multimodal hashes for {modality!r} must be non-empty"
                )
            if kwargs_data is None:
                raw_items: List[Optional[str]] = [None] * len(raw_hashes)
            else:
                candidate = kwargs_data.get(modality)
                if not isinstance(candidate, list) or len(candidate) != len(raw_hashes):
                    raise ValueError(
                        "Decode multimodal kwargs_data must be parallel to mm_hashes"
                    )
                raw_items = list(candidate)
            for raw_hash, raw_item in zip(raw_hashes, raw_items):
                if not isinstance(raw_hash, str) or not raw_hash:
                    raise TypeError("Decode multimodal hashes must be non-empty strings")
                if raw_item is not None and not isinstance(raw_item, str):
                    raise TypeError(
                        "Decode multimodal kwargs_data entries must be strings or null"
                    )
                if isinstance(raw_item, str):
                    full_bytes += len(raw_item.encode("utf-8"))
                flattened.append((str(modality), raw_hash, raw_item))
        return flattened, full_bytes

    def prepare(
        self,
        *,
        worker_id: str,
        payload: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[_DecodeMMHashLease]]:
        if not self.enabled:
            return payload, None
        raw_features = payload.get("features")
        if raw_features is None:
            self._no_feature_requests += 1
            return payload, None
        if not isinstance(raw_features, dict):
            raise TypeError("Decode multimodal features must be an object")

        features = dict(raw_features)
        flattened, full_bytes = self._flatten_features(features)
        now = time.monotonic()
        self._lookups += 1
        referenced = tuple(dict.fromkeys(item[1] for item in flattened))
        warm = {
            mm_hash: self._is_warm(self._key(worker_id, mm_hash), now=now)
            for mm_hash in referenced
        }
        metadata_only = tuple(
            dict.fromkeys(
                mm_hash
                for _modality, mm_hash, item in flattened
                if item is None
            )
        )
        cold_metadata_only = [mm_hash for mm_hash in metadata_only if not warm[mm_hash]]
        if cold_metadata_only:
            self._rejected_cold_metadata_only += 1
            raise RuntimeError(
                "Decode render supplied hash-only multimodal features before "
                "the target worker cache was confirmed warm"
            )

        promote_hashes = tuple(
            dict.fromkeys(
                mm_hash
                for _modality, mm_hash, item in flattened
                if item is not None
            )
        )
        epoch_confirmed = str(worker_id) in self._worker_epochs
        if all(warm.values()) and epoch_confirmed:
            optimized_payload = dict(payload)
            features["kwargs_data"] = None
            optimized_payload["features"] = features
            self._hash_only_requests += 1
            self._avoided_bytes += full_bytes
            return optimized_payload, _DecodeMMHashLease(
                cache=self,
                worker_id=str(worker_id),
                referenced_hashes=referenced,
                promote_hashes=(),
                invalidate_hashes=referenced,
                mode="hash-only",
                avoided_bytes=full_bytes,
                expected_worker_epoch=self._worker_epochs.get(str(worker_id)),
            )

        if all(warm.values()) and not epoch_confirmed:
            self._epoch_unconfirmed_full_requests += 1

        self._full_requests += 1
        return payload, _DecodeMMHashLease(
            cache=self,
            worker_id=str(worker_id),
            referenced_hashes=referenced,
            promote_hashes=promote_hashes,
            invalidate_hashes=metadata_only,
            mode="full",
        )

    def complete(self, lease: _DecodeMMHashLease, *, success: bool) -> None:
        if success:
            now = time.monotonic()
            expires_at = now + self.ttl_s if self.ttl_s > 0 else 0.0
            if lease.mode == "hash-only":
                # Renew only after a legal terminal response. Never recreate an
                # entry removed by an epoch change/unavailable event while the
                # request was in flight.
                for mm_hash in lease.referenced_hashes:
                    key = self._key(lease.worker_id, mm_hash)
                    if key not in self._entries:
                        continue
                    self._entries[key] = expires_at
                    self._entries.move_to_end(key)
                    self._renewals += 1
                return
            for mm_hash in lease.promote_hashes:
                key = self._key(lease.worker_id, mm_hash)
                self._entries.pop(key, None)
                self._entries[key] = expires_at
                self._promotions += 1
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
                self._evictions += 1
            return

        # A hash-only failure is evidence that the target Decode worker's
        # receiver-cache generation may have changed (for example, after a
        # worker restart).  Every warm assumption for that worker belongs to
        # the same opaque engine-local cache lifetime, so retaining unrelated
        # hashes would turn one restart into one user-visible failure per hash.
        # Keep strict-no-fallback semantics for the failed request, but
        # conservatively make all subsequent requests cold for this worker.
        if lease.mode == "hash-only":
            self.invalidate_worker(lease.worker_id)
            return

        for mm_hash in lease.invalidate_hashes:
            if self._entries.pop(self._key(lease.worker_id, mm_hash), None) is not None:
                self._invalidations += 1

    def invalidate_worker(self, worker_id: str) -> int:
        normalized_worker = str(worker_id)
        keys = [key for key in self._entries if key[0] == normalized_worker]
        if not keys:
            return 0
        for key in keys:
            self._entries.pop(key, None)
        count = len(keys)
        self._invalidations += count
        self._worker_invalidations += 1
        self._worker_invalidated_entries += count
        return count

    def has_worker_entries(self, worker_id: str) -> bool:
        normalized_worker = str(worker_id)
        return any(key[0] == normalized_worker for key in self._entries)

    def observe_worker_epoch(self, worker_id: str, epoch: str) -> None:
        normalized_worker = str(worker_id)
        normalized_epoch = str(epoch)
        previous = self._worker_epochs.get(normalized_worker)
        self._epoch_observations += 1
        if previous is not None and previous != normalized_epoch:
            self._epoch_changes += 1
            self.invalidate_worker(normalized_worker)
        self._worker_epochs[normalized_worker] = normalized_epoch
        self._worker_epoch_observed_at[normalized_worker] = time.monotonic()

    def use_fresh_worker_epoch(self, worker_id: str, freshness_s: float) -> bool:
        """Consume a bounded recent epoch observation instead of probing again.

        The optimization is disabled at zero. Correctness remains fail-closed:
        monitor-confirmed epoch changes invalidate all warm hashes, while any
        failed hash-only Generate still invalidates the worker generation and
        is never retried in place.
        """

        max_age = max(0.0, float(freshness_s))
        if max_age <= 0:
            return False
        normalized_worker = str(worker_id)
        if normalized_worker not in self._worker_epochs:
            return False
        observed_at = self._worker_epoch_observed_at.get(normalized_worker)
        if observed_at is None or time.monotonic() - observed_at > max_age:
            return False
        self._epoch_freshness_skips += 1
        return True

    def mark_worker_unavailable(self, worker_id: str) -> None:
        normalized_worker = str(worker_id)
        self.record_epoch_probe_failure()
        had_epoch = normalized_worker in self._worker_epochs
        invalidated = self.invalidate_worker(normalized_worker)
        if had_epoch or invalidated > 0:
            self._worker_unavailable_events += 1
        # Removing the epoch makes the first successful observation after
        # recovery establish a new generation without double-counting a
        # second invalidation event.
        self._worker_epochs.pop(normalized_worker, None)
        self._worker_epoch_observed_at.pop(normalized_worker, None)

    def record_epoch_probe_failure(self) -> None:
        self._epoch_probe_failures += 1

    def record_epoch_probe_response(
        self,
        *,
        response_bytes: int,
        latency_s: float,
        source: Optional[str] = None,
    ) -> None:
        self._epoch_probe_responses += 1
        self._epoch_probe_response_bytes += max(0, int(response_bytes))
        normalized_latency = max(0.0, float(latency_s))
        self._epoch_probe_latency_seconds += normalized_latency
        self._epoch_probe_latency_seconds_max = max(
            self._epoch_probe_latency_seconds_max,
            normalized_latency,
        )
        if source:
            normalized_source = str(source)
            self._epoch_probe_source_counts[normalized_source] = (
                self._epoch_probe_source_counts.get(normalized_source, 0) + 1
            )

    def record_epoch_guarded_hash_only_request(self) -> None:
        self._epoch_guarded_hash_only_requests += 1

    def record_epoch_guard_rejection(
        self,
        *,
        worker_id: Optional[str] = None,
        actual_worker_epoch: Optional[str] = None,
    ) -> None:
        self._epoch_guard_rejections += 1
        if worker_id is None or actual_worker_epoch is None:
            return
        normalized_worker = str(worker_id)
        normalized_epoch = str(actual_worker_epoch)
        previous = self._worker_epochs.get(normalized_worker)
        self.observe_worker_epoch(normalized_worker, normalized_epoch)
        if previous != normalized_epoch:
            self._epoch_guard_epoch_learns += 1

    def record_synchronous_epoch_probe(self) -> None:
        self._synchronous_epoch_probes += 1

    def record_synchronous_epoch_probe_dispatch(self) -> None:
        self._synchronous_epoch_probe_dispatches += 1

    def record_synchronous_epoch_probe_collapsed(self) -> None:
        self._synchronous_epoch_probe_collapsed += 1

    def clear(self) -> None:
        self._entries.clear()

    def stats(self) -> Dict[str, Any]:
        entries_by_worker: Dict[str, int] = {}
        for worker_id, _mm_hash in self._entries:
            entries_by_worker[worker_id] = entries_by_worker.get(worker_id, 0) + 1
        return {
            "enabled": self.enabled,
            "entries": len(self._entries),
            "entries_by_worker": entries_by_worker,
            "max_entries": self.max_entries,
            "ttl_s": self.ttl_s,
            "lookups": self._lookups,
            "hash_only_requests": self._hash_only_requests,
            "full_requests": self._full_requests,
            "no_feature_requests": self._no_feature_requests,
            "promotions": self._promotions,
            "renewals": self._renewals,
            "invalidations": self._invalidations,
            "worker_invalidations": self._worker_invalidations,
            "worker_invalidated_entries": self._worker_invalidated_entries,
            "worker_epochs": dict(self._worker_epochs),
            "epoch_observations": self._epoch_observations,
            "epoch_changes": self._epoch_changes,
            "epoch_probe_failures": self._epoch_probe_failures,
            "epoch_probe_responses": self._epoch_probe_responses,
            "epoch_probe_response_bytes": self._epoch_probe_response_bytes,
            "epoch_probe_response_bytes_avg": (
                float(self._epoch_probe_response_bytes)
                / float(self._epoch_probe_responses)
                if self._epoch_probe_responses
                else 0.0
            ),
            "epoch_probe_latency_ms_avg": (
                1000.0 * self._epoch_probe_latency_seconds
                / float(self._epoch_probe_responses)
                if self._epoch_probe_responses
                else 0.0
            ),
            "epoch_probe_latency_ms_max": (
                1000.0 * self._epoch_probe_latency_seconds_max
            ),
            "epoch_probe_source_counts": dict(self._epoch_probe_source_counts),
            "epoch_guarded_hash_only_requests": (
                self._epoch_guarded_hash_only_requests
            ),
            "epoch_guard_rejections": self._epoch_guard_rejections,
            "epoch_guard_epoch_learns": self._epoch_guard_epoch_learns,
            "worker_unavailable_events": self._worker_unavailable_events,
            "epoch_unconfirmed_full_requests": self._epoch_unconfirmed_full_requests,
            "synchronous_epoch_probes": self._synchronous_epoch_probes,
            "synchronous_epoch_probe_dispatches": (
                self._synchronous_epoch_probe_dispatches
            ),
            "synchronous_epoch_probe_collapsed": (
                self._synchronous_epoch_probe_collapsed
            ),
            "epoch_freshness_skips": self._epoch_freshness_skips,
            "evictions": self._evictions,
            "expired": self._expired,
            "rejected_cold_metadata_only": self._rejected_cold_metadata_only,
            "avoided_serialized_bytes": self._avoided_bytes,
        }


def parse_args() -> ProxyConfig:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--prefiller-hosts", "--prefiller-host", type=str, nargs="+", default=["127.0.0.1"])
    parser.add_argument("--prefiller-ports", "--prefiller-port", type=int, nargs="+", default=[8100])
    parser.add_argument("--decoder-hosts", "--decoder-host", type=str, nargs="+", default=["127.0.0.1"])
    parser.add_argument("--decoder-ports", "--decoder-port", type=int, nargs="+", default=[8200])
    parser.add_argument("--layers-per-group", type=int, default=4)
    parser.add_argument("--group-delay-ms", type=float, default=0.0)
    parser.add_argument("--max-group-bytes", type=int, default=0)
    parser.add_argument("--warn-rho", type=float, default=0.85)
    parser.add_argument("--critical-rho", type=float, default=0.95)
    parser.add_argument("--max-backpressure-delay-ms", type=float, default=150.0)
    parser.add_argument("--transport-backend", type=str, default="mooncake_engine_direct")
    parser.add_argument("--node-id", type=str, default="proxy")
    parser.add_argument("--owner-shards", type=int, default=1)
    parser.add_argument("--kv-directory-rpc-url", type=str, default=None)
    parser.add_argument("--connector-metrics-dir", type=str, default=None)
    parser.add_argument("--workflow-registry-wal", type=str, default=None)
    parser.add_argument("--workflow-registry-wal-fsync-interval-s", type=float, default=0.25)
    parser.add_argument("--workflow-registry-wal-max-pending", type=int, default=64)
    parser.add_argument("--enable-decode-pipeline", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--decode-pipeline-max-inflight",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_DECODE_PIPELINE_MAX_INFLIGHT", "0")),
        help=(
            "Maximum already-active Decode requests that may coexist with a new "
            "early-open pipeline; 0 keeps the legacy unlimited behavior."
        ),
    )
    parser.add_argument(
        "--enable-prerendered-decode",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_PRERENDERED_DECODE", "0").lower()
        not in {"", "0", "false", "no", "off"},
    )
    parser.add_argument(
        "--prerendered-decode-model",
        type=str,
        default=os.getenv("MOONCAKE_EPD_PRERENDERED_DECODE_MODEL"),
    )
    parser.add_argument(
        "--enable-decode-mm-hash-cache",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_DECODE_MM_HASH_CACHE", "0").lower()
        not in {"", "0", "false", "no", "off"},
        help=(
            "After a successful full multimodal Generate request, send only "
            "hashes to the same Decode worker while the bounded evidence TTL is live."
        ),
    )
    parser.add_argument(
        "--decode-mm-hash-cache-max-entries",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_DECODE_MM_HASH_CACHE_MAX_ENTRIES", "64")),
    )
    parser.add_argument(
        "--decode-mm-hash-cache-ttl-s",
        type=float,
        default=float(os.getenv("MOONCAKE_EPD_DECODE_MM_HASH_CACHE_TTL_S", "120")),
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-poll-s",
        type=float,
        default=float(os.getenv("MOONCAKE_EPD_DECODE_MM_HASH_EPOCH_POLL_S", "1")),
        help=(
            "Background interval for fencing warm MM hashes with Decode process_start_time_seconds; "
            "0 disables proactive restart detection."
        ),
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-probe-timeout-s",
        type=float,
        default=float(
            os.getenv("MOONCAKE_EPD_DECODE_MM_HASH_EPOCH_PROBE_TIMEOUT_S", "0.5")
        ),
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-endpoint",
        type=str,
        default=os.getenv(
            "MOONCAKE_EPD_DECODE_MM_HASH_EPOCH_ENDPOINT",
            "/metrics",
        ),
        help=(
            "Decode worker path returning a process-incarnation token. "
            "Use /metrics for backward compatibility or "
            "/mooncake_epd/incarnation with the repo vLLM middleware."
        ),
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-guard",
        action=argparse.BooleanOptionalAction,
        default=os.getenv(
            "MOONCAKE_EPD_DECODE_MM_HASH_EPOCH_GUARD",
            "0",
        ).lower()
        not in {"", "0", "false", "no", "off"},
        help=(
            "Attach the observed Decode incarnation to hash-only requests so "
            "the vLLM middleware rejects stale cache generations before engine dispatch."
        ),
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-probe-singleflight",
        action=argparse.BooleanOptionalAction,
        default=os.getenv(
            "MOONCAKE_EPD_DECODE_MM_HASH_EPOCH_PROBE_SINGLEFLIGHT",
            "1",
        ).lower()
        not in {"", "0", "false", "no", "off"},
        help="Collapse concurrent hot requests for one Decode worker onto one epoch probe.",
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-freshness-s",
        type=float,
        default=float(
            os.getenv("MOONCAKE_EPD_DECODE_MM_HASH_EPOCH_FRESHNESS_S", "0")
        ),
        help=(
            "Reuse a successful Decode epoch observation for this bounded interval. "
            "Zero preserves per-hot-request probing."
        ),
    )
    parser.add_argument("--enable-mm-prefetch", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--mm-prefetch-mode", choices=["asset_bytes", "feature_handle"], default="asset_bytes")
    parser.add_argument("--prefill-supports-feature-handles", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--mm-prefetch-wait-ms", type=float, default=100.0)
    parser.add_argument("--mm-prefetch-max-asset-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--mm-prefetch-queue-size", type=int, default=256)
    parser.add_argument(
        "--allow-private-mm-urls",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Allow HTTP(S) multimodal URLs resolving to private/local networks. "
            "Disabled by default to prevent serving-side SSRF."
        ),
    )
    parser.add_argument("--encoder-service-url", type=str, default=os.getenv("MOONCAKE_EPD_ENCODER_SERVICE_URL"))
    parser.add_argument("--encoder-service-timeout-s", type=float, default=float(os.getenv("MOONCAKE_EPD_ENCODER_SERVICE_TIMEOUT_S", "120")))
    parser.add_argument("--prefill-direct-buffer-service-url", type=str, default=os.getenv("MOONCAKE_EPD_PREFILL_DIRECT_BUFFER_SERVICE_URL"))
    parser.add_argument("--prefill-direct-buffer-timeout-s", type=float, default=float(os.getenv("MOONCAKE_EPD_PREFILL_DIRECT_BUFFER_TIMEOUT_S", "30")))
    parser.add_argument("--release-direct-feature-buffers-after-prefill", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--enable-direct-feature-handle-cache",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE", "0").lower()
        not in {"", "0", "false", "no", "off"},
    )
    parser.add_argument(
        "--direct-feature-handle-cache-max-entries",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE_MAX_ENTRIES", "64")),
    )
    parser.add_argument(
        "--direct-feature-handle-cache-max-bytes",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE_MAX_BYTES", str(4 * 1024 * 1024 * 1024))),
    )
    parser.add_argument(
        "--direct-feature-handle-cache-ttl-s",
        type=float,
        default=float(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE_TTL_S", "600")),
    )
    parser.add_argument(
        "--prefill-incarnation-poll-s",
        type=float,
        default=float(os.getenv("MOONCAKE_EPD_PREFILL_INCARNATION_POLL_S", "0")),
        help=(
            "Background interval for invalidating direct handles and cached KV "
            "topology after a Prefill API-process restart; 0 disables polling."
        ),
    )
    parser.add_argument(
        "--prefill-incarnation-poll-jitter-ratio",
        type=float,
        default=float(
            os.getenv(
                "MOONCAKE_EPD_PREFILL_INCARNATION_POLL_JITTER_RATIO",
                "0",
            )
        ),
        help=(
            "Symmetric fractional jitter applied after each background Prefill "
            "incarnation probe; 0 preserves a fixed interval and 1 allows "
            "delays from zero to twice the configured interval."
        ),
    )
    parser.add_argument(
        "--prefill-incarnation-probe-timeout-s",
        type=float,
        default=float(
            os.getenv("MOONCAKE_EPD_PREFILL_INCARNATION_PROBE_TIMEOUT_S", "0.5")
        ),
    )
    parser.add_argument(
        "--prefill-incarnation-failure-threshold",
        type=int,
        default=int(
            os.getenv("MOONCAKE_EPD_PREFILL_INCARNATION_FAILURE_THRESHOLD", "3")
        ),
        help=(
            "Consecutive failed background Prefill incarnation probes required "
            "before removing the worker; synchronous request fences remain "
            "fail-closed on the first failed proof."
        ),
    )
    parser.add_argument(
        "--prefill-incarnation-freshness-s",
        type=float,
        default=float(
            os.getenv("MOONCAKE_EPD_PREFILL_INCARNATION_FRESHNESS_S", "0")
        ),
        help="Bounded lifetime of a successful Prefill incarnation observation.",
    )
    parser.add_argument(
        "--prefill-incarnation-endpoint",
        type=str,
        default=os.getenv(
            "MOONCAKE_EPD_PREFILL_INCARNATION_ENDPOINT",
            VLLM_INCARNATION_ENDPOINT,
        ),
    )
    parser.add_argument(
        "--prefill-incarnation-guard",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_PREFILL_INCARNATION_GUARD", "0").lower()
        not in {"", "0", "false", "no", "off"},
        help=(
            "Attach the expected Prefill API-process incarnation so stale direct "
            "handles are rejected before render or engine dispatch."
        ),
    )
    parser.add_argument(
        "--prefill-incarnation-probe-singleflight",
        action=argparse.BooleanOptionalAction,
        default=os.getenv(
            "MOONCAKE_EPD_PREFILL_INCARNATION_PROBE_SINGLEFLIGHT",
            "1",
        ).lower()
        not in {"", "0", "false", "no", "off"},
        help="Collapse concurrent direct-handle reuse checks per Prefill worker.",
    )
    parser.add_argument(
        "--enable-prefill-render-cache",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_PREFILL_RENDER_CACHE", "0").lower()
        not in {"", "0", "false", "no", "off"},
    )
    parser.add_argument(
        "--prefill-render-cache-max-entries",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_PREFILL_RENDER_CACHE_MAX_ENTRIES", "128")),
    )
    parser.add_argument(
        "--prefill-render-cache-max-bytes",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_PREFILL_RENDER_CACHE_MAX_BYTES", str(512 * 1024 * 1024))),
    )
    parser.add_argument(
        "--prefill-render-cache-ttl-s",
        type=float,
        default=float(os.getenv("MOONCAKE_EPD_PREFILL_RENDER_CACHE_TTL_S", "600")),
    )
    parser.add_argument(
        "--enable-client-mm-uuid-references",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_CLIENT_MM_UUID_REFERENCES", "0").lower()
        not in {"", "0", "false", "no", "off"},
        help=(
            "Accept vLLM-compatible UUID-only multimodal items after a full warmup. "
            "Cold direct-feature or Prefill-render misses fail closed."
        ),
    )
    parser.add_argument("--strict-no-fallback", action=argparse.BooleanOptionalAction, default=strict_no_fallback_enabled())
    parser.add_argument("--enable-agent-state-clone", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--high-prefill-worker-ids", nargs="*", default=None)
    parser.add_argument("--low-latency-decode-worker-ids", nargs="*", default=None)
    parser.add_argument("--standard-prefill-worker-ids", nargs="*", default=None)
    parser.add_argument("--standard-decode-worker-ids", nargs="*", default=None)
    parser.add_argument("--upstream-max-connections", type=int, default=64)
    parser.add_argument("--upstream-max-keepalive-connections", type=int, default=16)
    parser.add_argument("--upstream-keepalive-expiry-s", type=float, default=1.0)
    args = parser.parse_args()

    if len(args.prefiller_hosts) != len(args.prefiller_ports):
        raise ValueError("Number of prefiller hosts must match number of prefiller ports")
    if len(args.decoder_hosts) != len(args.decoder_ports):
        raise ValueError("Number of decoder hosts must match number of decoder ports")
    if args.upstream_max_connections < 1:
        raise ValueError("upstream max connections must be positive")
    if not 0 <= args.upstream_max_keepalive_connections <= args.upstream_max_connections:
        raise ValueError(
            "upstream max keepalive connections must be between zero and max connections"
        )
    if args.upstream_keepalive_expiry_s < 0:
        raise ValueError("upstream keepalive expiry must be non-negative")
    if args.workflow_registry_wal_fsync_interval_s < 0:
        raise ValueError("workflow registry WAL fsync interval must be non-negative")
    if args.workflow_registry_wal_max_pending < 1:
        raise ValueError("workflow registry WAL max pending records must be positive")
    if args.decode_pipeline_max_inflight < 0:
        raise ValueError("Decode pipeline max inflight must be non-negative")
    if args.direct_feature_handle_cache_max_entries < 1:
        raise ValueError("direct feature handle cache max entries must be positive")
    if args.direct_feature_handle_cache_max_bytes < 1:
        raise ValueError("direct feature handle cache max bytes must be positive")
    if args.direct_feature_handle_cache_ttl_s < 0:
        raise ValueError("direct feature handle cache TTL must be non-negative")
    if args.prefill_incarnation_poll_s < 0:
        raise ValueError("Prefill incarnation poll interval must be non-negative")
    if not 0 <= args.prefill_incarnation_poll_jitter_ratio <= 1:
        raise ValueError("Prefill incarnation poll jitter ratio must be between 0 and 1")
    if args.prefill_incarnation_probe_timeout_s <= 0:
        raise ValueError("Prefill incarnation probe timeout must be positive")
    if args.prefill_incarnation_failure_threshold < 1:
        raise ValueError("Prefill incarnation failure threshold must be positive")
    if args.prefill_incarnation_freshness_s < 0:
        raise ValueError("Prefill incarnation freshness must be non-negative")
    prefill_incarnation_endpoint_parts = urlsplit(
        str(args.prefill_incarnation_endpoint)
    )
    if (
        not prefill_incarnation_endpoint_parts.path.startswith("/")
        or prefill_incarnation_endpoint_parts.scheme
        or prefill_incarnation_endpoint_parts.netloc
        or prefill_incarnation_endpoint_parts.query
        or prefill_incarnation_endpoint_parts.fragment
    ):
        raise ValueError(
            "Prefill incarnation endpoint must be an absolute URL path without "
            "scheme, host, query, or fragment"
        )
    if (
        args.prefill_incarnation_guard
        and prefill_incarnation_endpoint_parts.path.rstrip("/")
        != VLLM_INCARNATION_ENDPOINT
    ):
        raise ValueError(
            "Prefill incarnation guard requires the repo incarnation endpoint"
        )
    if args.prefill_render_cache_max_entries < 1:
        raise ValueError("prefill render cache max entries must be positive")
    if args.prefill_render_cache_max_bytes < 1:
        raise ValueError("prefill render cache max bytes must be positive")
    if args.prefill_render_cache_ttl_s < 0:
        raise ValueError("prefill render cache TTL must be non-negative")
    if args.enable_prerendered_decode and not args.prerendered_decode_model:
        raise ValueError(
            "--enable-prerendered-decode requires --prerendered-decode-model"
        )
    if args.enable_decode_mm_hash_cache and not args.enable_prerendered_decode:
        raise ValueError(
            "--enable-decode-mm-hash-cache requires --enable-prerendered-decode"
        )
    if args.decode_mm_hash_cache_max_entries < 1:
        raise ValueError("Decode MM hash cache max entries must be positive")
    if args.decode_mm_hash_cache_ttl_s < 0:
        raise ValueError("Decode MM hash cache TTL must be non-negative")
    if args.decode_mm_hash_epoch_poll_s < 0:
        raise ValueError("Decode MM hash epoch poll interval must be non-negative")
    if args.decode_mm_hash_epoch_probe_timeout_s <= 0:
        raise ValueError("Decode MM hash epoch probe timeout must be positive")
    if args.decode_mm_hash_epoch_freshness_s < 0:
        raise ValueError("Decode MM hash epoch freshness must be non-negative")
    epoch_endpoint_parts = urlsplit(str(args.decode_mm_hash_epoch_endpoint))
    if (
        not epoch_endpoint_parts.path.startswith("/")
        or epoch_endpoint_parts.scheme
        or epoch_endpoint_parts.netloc
        or epoch_endpoint_parts.query
        or epoch_endpoint_parts.fragment
    ):
        raise ValueError(
            "Decode MM hash epoch endpoint must be an absolute URL path without "
            "scheme, host, query, or fragment"
        )
    if (
        args.decode_mm_hash_epoch_guard
        and epoch_endpoint_parts.path.rstrip("/") != VLLM_INCARNATION_ENDPOINT
    ):
        raise ValueError(
            "Decode MM hash epoch guard requires the repo incarnation endpoint"
        )

    return ProxyConfig(
        host=args.host,
        port=args.port,
        prefiller_instances=list(zip(args.prefiller_hosts, args.prefiller_ports)),
        decoder_instances=list(zip(args.decoder_hosts, args.decoder_ports)),
        layers_per_group=args.layers_per_group,
        group_delay_ms=args.group_delay_ms,
        max_group_bytes=args.max_group_bytes,
        warn_rho=args.warn_rho,
        critical_rho=args.critical_rho,
        max_backpressure_delay_ms=args.max_backpressure_delay_ms,
        transport_backend=args.transport_backend,
        node_id=args.node_id,
        owner_shards=args.owner_shards,
        kv_directory_rpc_url=args.kv_directory_rpc_url,
        connector_metrics_dir=args.connector_metrics_dir,
        workflow_registry_wal_path=args.workflow_registry_wal,
        workflow_registry_wal_fsync_interval_s=(
            args.workflow_registry_wal_fsync_interval_s
        ),
        workflow_registry_wal_max_pending_records=(
            args.workflow_registry_wal_max_pending
        ),
        enable_decode_pipeline=bool(args.enable_decode_pipeline),
        decode_pipeline_max_inflight=int(args.decode_pipeline_max_inflight),
        enable_prerendered_decode=bool(args.enable_prerendered_decode),
        prerendered_decode_model=args.prerendered_decode_model,
        enable_decode_mm_hash_cache=bool(args.enable_decode_mm_hash_cache),
        decode_mm_hash_cache_max_entries=int(args.decode_mm_hash_cache_max_entries),
        decode_mm_hash_cache_ttl_s=float(args.decode_mm_hash_cache_ttl_s),
        decode_mm_hash_epoch_poll_s=float(args.decode_mm_hash_epoch_poll_s),
        decode_mm_hash_epoch_probe_timeout_s=float(
            args.decode_mm_hash_epoch_probe_timeout_s
        ),
        decode_mm_hash_epoch_freshness_s=float(
            args.decode_mm_hash_epoch_freshness_s
        ),
        decode_mm_hash_epoch_endpoint=str(args.decode_mm_hash_epoch_endpoint),
        enable_decode_mm_hash_epoch_guard=bool(
            args.decode_mm_hash_epoch_guard
        ),
        enable_decode_mm_hash_epoch_probe_singleflight=bool(
            args.decode_mm_hash_epoch_probe_singleflight
        ),
        enable_mm_prefetch=bool(args.enable_mm_prefetch),
        mm_prefetch_mode=str(args.mm_prefetch_mode),
        prefill_supports_feature_handles=bool(args.prefill_supports_feature_handles),
        mm_prefetch_wait_ms=args.mm_prefetch_wait_ms,
        mm_prefetch_max_asset_bytes=args.mm_prefetch_max_asset_bytes,
        mm_prefetch_queue_size=args.mm_prefetch_queue_size,
        allow_private_mm_urls=bool(args.allow_private_mm_urls),
        encoder_service_url=args.encoder_service_url,
        encoder_service_timeout_s=args.encoder_service_timeout_s,
        prefill_direct_buffer_service_url=args.prefill_direct_buffer_service_url,
        prefill_direct_buffer_timeout_s=args.prefill_direct_buffer_timeout_s,
        release_direct_feature_buffers_after_prefill=bool(args.release_direct_feature_buffers_after_prefill),
        enable_direct_feature_handle_cache=bool(args.enable_direct_feature_handle_cache),
        direct_feature_handle_cache_max_entries=int(args.direct_feature_handle_cache_max_entries),
        direct_feature_handle_cache_max_bytes=int(args.direct_feature_handle_cache_max_bytes),
        direct_feature_handle_cache_ttl_s=float(args.direct_feature_handle_cache_ttl_s),
        prefill_incarnation_poll_s=float(args.prefill_incarnation_poll_s),
        prefill_incarnation_poll_jitter_ratio=float(
            args.prefill_incarnation_poll_jitter_ratio
        ),
        prefill_incarnation_probe_timeout_s=float(
            args.prefill_incarnation_probe_timeout_s
        ),
        prefill_incarnation_failure_threshold=int(
            args.prefill_incarnation_failure_threshold
        ),
        prefill_incarnation_freshness_s=float(
            args.prefill_incarnation_freshness_s
        ),
        prefill_incarnation_endpoint=str(args.prefill_incarnation_endpoint),
        enable_prefill_incarnation_guard=bool(args.prefill_incarnation_guard),
        enable_prefill_incarnation_probe_singleflight=bool(
            args.prefill_incarnation_probe_singleflight
        ),
        enable_prefill_render_cache=bool(args.enable_prefill_render_cache),
        prefill_render_cache_max_entries=int(args.prefill_render_cache_max_entries),
        prefill_render_cache_max_bytes=int(args.prefill_render_cache_max_bytes),
        prefill_render_cache_ttl_s=float(args.prefill_render_cache_ttl_s),
        enable_client_mm_uuid_references=bool(
            args.enable_client_mm_uuid_references
        ),
        strict_no_fallback=bool(args.strict_no_fallback),
        enable_agent_state_clone=bool(args.enable_agent_state_clone),
        high_prefill_worker_ids=list(args.high_prefill_worker_ids or []),
        low_latency_decode_worker_ids=list(args.low_latency_decode_worker_ids or []),
        standard_prefill_worker_ids=list(args.standard_prefill_worker_ids or []),
        standard_decode_worker_ids=list(args.standard_decode_worker_ids or []),
        upstream_max_connections=args.upstream_max_connections,
        upstream_max_keepalive_connections=args.upstream_max_keepalive_connections,
        upstream_keepalive_expiry_s=args.upstream_keepalive_expiry_s,
    )


def _make_client(base_url: str, config: ProxyConfig) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=None,
        base_url=base_url,
        limits=httpx.Limits(
            max_connections=config.upstream_max_connections,
            max_keepalive_connections=config.upstream_max_keepalive_connections,
            keepalive_expiry=config.upstream_keepalive_expiry_s,
        ),
        trust_env=False,
    )


def _decode_process_epoch(metrics_text: str) -> Optional[str]:
    for raw_line in str(metrics_text).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith("process_start_time_seconds"):
            continue
        parts = line.split()
        if len(parts) == 2 and parts[1]:
            return parts[1]
    return None


def _bounded_epoch_token(raw_value: Any) -> Optional[str]:
    value = str(raw_value or "").strip()
    if not value or len(value) > 512 or "\n" in value or "\r" in value:
        return None
    return value


def _decode_worker_epoch_from_response(
    response: httpx.Response,
    *,
    epoch_endpoint: str,
) -> Tuple[Optional[str], Optional[str]]:
    header_value = getattr(response, "headers", {}).get(VLLM_INCARNATION_HEADER)
    header_epoch = _bounded_epoch_token(header_value)
    if header_epoch is not None:
        return header_epoch, "incarnation-header"
    if str(epoch_endpoint).rstrip("/") == "/metrics":
        metrics_epoch = _decode_process_epoch(response.text)
        if metrics_epoch is not None:
            return metrics_epoch, "prometheus-process-start"
        return None, None
    body_epoch = _bounded_epoch_token(response.text)
    if body_epoch is not None:
        return body_epoch, "incarnation-body"
    return None, None


async def _probe_decode_worker_epoch(
    *,
    cache: _DecodeMMHashWarmCache,
    client_info: Dict[str, Any],
    timeout_s: float,
    epoch_endpoint: str = "/metrics",
    control_plane: Optional[ServingControlPlane] = None,
) -> bool:
    worker_id = str(client_info.get("worker_id") or "")
    client: httpx.AsyncClient = client_info["client"]

    def _set_available(available: bool) -> None:
        if control_plane is not None:
            control_plane.set_stage_worker_available(
                "decode",
                worker_id,
                available=available,
            )

    probe_started_at = time.perf_counter()
    try:
        response = await asyncio.wait_for(
            client.get(str(epoch_endpoint)),
            timeout=max(0.05, float(timeout_s)),
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        cache.mark_worker_unavailable(worker_id)
        _set_available(False)
        return False
    try:
        try:
            response_bytes = len(response.content)
        except Exception:
            response_bytes = len(str(response.text).encode("utf-8"))
        epoch: Optional[str] = None
        source: Optional[str] = None
        if response.status_code == 200:
            epoch, source = _decode_worker_epoch_from_response(
                response,
                epoch_endpoint=str(epoch_endpoint),
            )
        cache.record_epoch_probe_response(
            response_bytes=response_bytes,
            latency_s=time.perf_counter() - probe_started_at,
            source=source,
        )
        if response.status_code >= 500:
            cache.mark_worker_unavailable(worker_id)
            _set_available(False)
            return False
        if response.status_code != 200:
            cache.record_epoch_probe_failure()
            cache.invalidate_worker(worker_id)
            _set_available(False)
            return False
        if epoch is None:
            cache.record_epoch_probe_failure()
            cache.invalidate_worker(worker_id)
            _set_available(False)
            return False
        cache.observe_worker_epoch(worker_id, epoch)
        _set_available(True)
        return True
    finally:
        await response.aclose()


async def _monitor_decode_worker_epochs(app: FastAPI) -> None:
    config: ProxyConfig = app.state.proxy_config
    cache: _DecodeMMHashWarmCache = app.state.decode_mm_hash_cache
    interval_s = max(0.01, float(config.decode_mm_hash_epoch_poll_s))
    while True:
        await asyncio.gather(
            *(
                _probe_decode_worker_epoch(
                    cache=cache,
                    client_info=client_info,
                    timeout_s=config.decode_mm_hash_epoch_probe_timeout_s,
                    epoch_endpoint=config.decode_mm_hash_epoch_endpoint,
                    control_plane=app.state.control_plane,
                )
                for client_info in app.state.decode_clients
            )
        )
        await asyncio.sleep(interval_s)


async def _fence_decode_mm_hash_reuse(
    *,
    app: FastAPI,
    decode_client: Dict[str, Any],
) -> None:
    cache: _DecodeMMHashWarmCache = app.state.decode_mm_hash_cache
    worker_id = str(decode_client.get("worker_id") or "")
    if not cache.enabled or not cache.has_worker_entries(worker_id):
        return
    freshness_s = float(
        getattr(
            app.state.proxy_config,
            "decode_mm_hash_epoch_freshness_s",
            0.0,
        )
    )
    if cache.use_fresh_worker_epoch(worker_id, freshness_s):
        return
    cache.record_synchronous_epoch_probe()
    if not app.state.proxy_config.enable_decode_mm_hash_epoch_probe_singleflight:
        cache.record_synchronous_epoch_probe_dispatch()
        await _probe_decode_worker_epoch(
            cache=cache,
            client_info=decode_client,
            timeout_s=app.state.proxy_config.decode_mm_hash_epoch_probe_timeout_s,
            epoch_endpoint=getattr(
                app.state.proxy_config,
                "decode_mm_hash_epoch_endpoint",
                "/metrics",
            ),
            control_plane=getattr(app.state, "control_plane", None),
        )
        return
    inflight: Dict[str, asyncio.Task] = app.state.decode_epoch_probe_inflight
    task = inflight.get(worker_id)
    if task is None:
        cache.record_synchronous_epoch_probe_dispatch()
        task = asyncio.create_task(
            _probe_decode_worker_epoch(
                cache=cache,
                client_info=decode_client,
                timeout_s=(
                    app.state.proxy_config.decode_mm_hash_epoch_probe_timeout_s
                ),
                epoch_endpoint=getattr(
                    app.state.proxy_config,
                    "decode_mm_hash_epoch_endpoint",
                    "/metrics",
                ),
                control_plane=getattr(app.state, "control_plane", None),
            ),
            name=f"epd-decode-epoch-fence-{worker_id}",
        )
        inflight[worker_id] = task

        def _remove_completed_probe(done: asyncio.Task) -> None:
            if inflight.get(worker_id) is done:
                inflight.pop(worker_id, None)

        task.add_done_callback(_remove_completed_probe)
    else:
        cache.record_synchronous_epoch_probe_collapsed()
    await asyncio.shield(task)


def _invalidate_prefill_cached_topology(prefill_client: Dict[str, Any]) -> None:
    prefill_client.pop("remote_kv_topology", None)
    prefill_client.pop("remote_kv_topology_api_incarnation", None)


async def _release_retired_direct_feature_ids(
    app: FastAPI,
    feature_ids: Sequence[str],
) -> None:
    if not feature_ids:
        return
    try:
        await _release_direct_feature_ids(app, feature_ids)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning(
            "failed to release retired Prefill direct feature buffers",
            exc_info=True,
        )


async def _observe_prefill_worker_incarnation(
    *,
    app: FastAPI,
    prefill_client: Dict[str, Any],
    incarnation: str,
) -> bool:
    cache: _DirectFeatureHandleCache = app.state.direct_feature_handle_cache
    worker_id = str(prefill_client.get("worker_id") or "")
    changed, retired_ids = cache.observe_worker_incarnation(
        worker_id,
        incarnation,
    )
    cache.mark_worker_available(worker_id)
    prefill_client["remote_api_incarnation"] = cache.worker_incarnation(worker_id)
    if changed:
        _invalidate_prefill_cached_topology(prefill_client)
        render_cache = getattr(app.state, "prefill_render_cache", None)
        if render_cache is not None:
            render_cache.invalidate_worker(worker_id)
    await _release_retired_direct_feature_ids(app, retired_ids)
    return changed


async def _mark_prefill_worker_unavailable(
    *,
    app: FastAPI,
    prefill_client: Dict[str, Any],
) -> None:
    cache: _DirectFeatureHandleCache = app.state.direct_feature_handle_cache
    worker_id = str(prefill_client.get("worker_id") or "")
    retired_ids = cache.mark_worker_unavailable(worker_id)
    prefill_client.pop("remote_api_incarnation", None)
    _invalidate_prefill_cached_topology(prefill_client)
    render_cache = getattr(app.state, "prefill_render_cache", None)
    if render_cache is not None:
        render_cache.invalidate_worker(worker_id)
    control_plane = getattr(app.state, "control_plane", None)
    if control_plane is not None:
        control_plane.set_stage_worker_available(
            "prefill",
            worker_id,
            available=False,
        )
    await _release_retired_direct_feature_ids(app, retired_ids)


async def _probe_prefill_worker_incarnation(
    *,
    app: FastAPI,
    prefill_client: Dict[str, Any],
    mark_unavailable_on_failure: bool = True,
) -> bool:
    cache: _DirectFeatureHandleCache = app.state.direct_feature_handle_cache
    config = app.state.proxy_config
    client: httpx.AsyncClient = prefill_client["client"]
    endpoint = str(
        getattr(config, "prefill_incarnation_endpoint", VLLM_INCARNATION_ENDPOINT)
    )
    timeout_s = max(
        0.05,
        float(getattr(config, "prefill_incarnation_probe_timeout_s", 0.5)),
    )
    probe_started_at = time.perf_counter()
    try:
        response = await asyncio.wait_for(
            client.get(endpoint),
            timeout=timeout_s,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        cache.record_incarnation_probe_failure()
        if mark_unavailable_on_failure:
            await _mark_prefill_worker_unavailable(
                app=app,
                prefill_client=prefill_client,
            )
        return False
    try:
        try:
            response_bytes = len(response.content)
        except Exception:
            response_bytes = len(str(response.text).encode("utf-8"))
        cache.record_incarnation_probe_response(
            response_bytes=response_bytes,
            latency_s=time.perf_counter() - probe_started_at,
        )
        incarnation: Optional[str] = None
        if response.status_code == 200:
            incarnation, _source = _decode_worker_epoch_from_response(
                response,
                epoch_endpoint=endpoint,
            )
        if response.status_code != 200 or incarnation is None:
            cache.record_incarnation_probe_failure()
            if mark_unavailable_on_failure:
                await _mark_prefill_worker_unavailable(
                    app=app,
                    prefill_client=prefill_client,
                )
            return False
        await _observe_prefill_worker_incarnation(
            app=app,
            prefill_client=prefill_client,
            incarnation=incarnation,
        )
        control_plane = getattr(app.state, "control_plane", None)
        if control_plane is not None:
            control_plane.set_stage_worker_available(
                "prefill",
                str(prefill_client.get("worker_id") or ""),
                available=True,
            )
        return True
    finally:
        await response.aclose()


async def _probe_prefill_worker_incarnation_for_monitor(
    *,
    app: FastAPI,
    prefill_client: Dict[str, Any],
) -> bool:
    """Probe one Prefill worker with monitor-only consecutive-failure debounce."""

    probe_started_at = time.monotonic()
    healthy = await _probe_prefill_worker_incarnation(
        app=app,
        prefill_client=prefill_client,
        mark_unavailable_on_failure=False,
    )
    cache: _DirectFeatureHandleCache = app.state.direct_feature_handle_cache
    worker_id = str(prefill_client.get("worker_id") or "")
    if (
        not healthy
        and cache.worker_incarnation_observed_since(worker_id, probe_started_at)
    ):
        # A newer synchronous proof completed while this older background
        # probe was in flight.  Do not let the stale failure overwrite the
        # authoritative success or contribute to worker removal.
        cache.record_superseded_incarnation_monitor_failure()
        return False
    threshold = max(
        1,
        int(
            getattr(
                app.state.proxy_config,
                "prefill_incarnation_failure_threshold",
                3,
            )
        ),
    )
    threshold_reached = cache.record_incarnation_monitor_probe_result(
        worker_id,
        healthy=healthy,
        failure_threshold=threshold,
    )
    if threshold_reached:
        await _mark_prefill_worker_unavailable(
            app=app,
            prefill_client=prefill_client,
        )
    return healthy


def _jittered_poll_delay_s(
    *,
    interval_s: float,
    jitter_ratio: float,
    unit_sample: float,
) -> float:
    """Return a bounded symmetric poll delay for replica de-synchronization.

    ``unit_sample`` is explicit rather than sampled internally so the bound is
    deterministic in tests and callers may own an isolated RNG.  The 10 ms
    floor matches the monitor's existing minimum interval.
    """

    interval = max(0.01, float(interval_s))
    ratio = min(1.0, max(0.0, float(jitter_ratio)))
    sample = min(1.0, max(0.0, float(unit_sample)))
    return max(0.01, interval * (1.0 + ratio * (2.0 * sample - 1.0)))


async def _monitor_prefill_worker_incarnations(app: FastAPI) -> None:
    config: ProxyConfig = app.state.proxy_config
    interval_s = max(0.01, float(config.prefill_incarnation_poll_s))
    jitter_ratio = min(
        1.0,
        max(
            0.0,
            float(
                getattr(
                    config,
                    "prefill_incarnation_poll_jitter_ratio",
                    0.0,
                )
            ),
        ),
    )
    rng = getattr(app.state, "prefill_incarnation_poll_rng", None)
    if rng is None:
        rng = random.Random(f"{os.getpid()}:{time.time_ns()}")
        app.state.prefill_incarnation_poll_rng = rng
    while True:
        await asyncio.gather(
            *(
                _probe_prefill_worker_incarnation_for_monitor(
                    app=app,
                    prefill_client=prefill_client,
                )
                for prefill_client in app.state.prefill_clients
            )
        )
        await asyncio.sleep(
            _jittered_poll_delay_s(
                interval_s=interval_s,
                jitter_ratio=jitter_ratio,
                unit_sample=rng.random(),
            )
        )


async def _fence_prefill_direct_handle_reuse(
    *,
    app: FastAPI,
    prefill_client: Dict[str, Any],
) -> None:
    cache: _DirectFeatureHandleCache = app.state.direct_feature_handle_cache
    worker_id = str(prefill_client.get("worker_id") or "")
    config = app.state.proxy_config
    fencing_enabled = bool(
        getattr(config, "enable_prefill_incarnation_guard", False)
        or float(getattr(config, "prefill_incarnation_poll_s", 0.0)) > 0
    )
    if (
        not fencing_enabled
        or not cache.enabled
        or not cache.has_worker_entries(worker_id)
    ):
        return
    freshness_s = float(
        getattr(app.state.proxy_config, "prefill_incarnation_freshness_s", 0.0)
    )
    if cache.use_fresh_worker_incarnation(worker_id, freshness_s):
        return
    cache.record_synchronous_incarnation_probe()
    if not bool(
        getattr(
            app.state.proxy_config,
            "enable_prefill_incarnation_probe_singleflight",
            True,
        )
    ):
        cache.record_synchronous_incarnation_probe_dispatch()
        healthy = await _probe_prefill_worker_incarnation(
            app=app,
            prefill_client=prefill_client,
        )
        if not healthy:
            raise HTTPException(
                status_code=502,
                detail="Prefill incarnation probe failed; stale direct handles were not reused",
            )
        return

    inflight: Dict[str, asyncio.Task] = app.state.prefill_incarnation_probe_inflight
    task = inflight.get(worker_id)
    if task is None:
        cache.record_synchronous_incarnation_probe_dispatch()
        task = asyncio.create_task(
            _probe_prefill_worker_incarnation(
                app=app,
                prefill_client=prefill_client,
            ),
            name=f"epd-prefill-incarnation-fence-{worker_id}",
        )
        inflight[worker_id] = task

        def _remove_completed_probe(done: asyncio.Task) -> None:
            if inflight.get(worker_id) is done:
                inflight.pop(worker_id, None)

        task.add_done_callback(_remove_completed_probe)
    else:
        cache.record_synchronous_incarnation_probe_collapsed()
    healthy = await asyncio.shield(task)
    if not healthy:
        raise HTTPException(
            status_code=502,
            detail="Prefill incarnation probe failed; stale direct handles were not reused",
        )


async def _learn_prefill_incarnation_guard_rejection(
    *,
    app: FastAPI,
    prefill_client: Dict[str, Any],
    exc: BaseException,
) -> None:
    if not _is_decode_epoch_guard_rejection(exc):
        return
    cache: _DirectFeatureHandleCache = app.state.direct_feature_handle_cache
    cache.record_incarnation_guard_rejection()
    actual = _decode_epoch_guard_rejection_epoch(exc)
    if actual is None:
        await _mark_prefill_worker_unavailable(
            app=app,
            prefill_client=prefill_client,
        )
        return
    await _observe_prefill_worker_incarnation(
        app=app,
        prefill_client=prefill_client,
        incarnation=actual,
    )


@asynccontextmanager
async def _lifespan(app: FastAPI):
    config: ProxyConfig = app.state.proxy_config
    control_plane: ServingControlPlane = app.state.control_plane
    prefill_overrides = getattr(app.state, "prefill_client_overrides", None)
    decode_overrides = getattr(app.state, "decode_client_overrides", None)

    if prefill_overrides is None:
        app.state.prefill_clients = [
            {
                "client": _make_client(f"http://{host}:{port}", config),
                "host": host,
                "port": port,
                "id": idx,
                "worker_id": f"prefill-{idx}",
            }
            for idx, (host, port) in enumerate(config.prefiller_instances)
        ]
    else:
        app.state.prefill_clients = list(prefill_overrides)

    if decode_overrides is None:
        app.state.decode_clients = [
            {
                "client": _make_client(f"http://{host}:{port}", config),
                "host": host,
                "port": port,
                "id": idx,
                "worker_id": f"decode-{idx}",
            }
            for idx, (host, port) in enumerate(config.decoder_instances)
        ]
    else:
        app.state.decode_clients = list(decode_overrides)

    control_plane.register_stage_workers(
        "prefill", [client["worker_id"] for client in app.state.prefill_clients]
    )
    control_plane.register_stage_workers(
        "decode", [client["worker_id"] for client in app.state.decode_clients]
    )
    app.state.mm_fetch_client = httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=5.0),
        follow_redirects=False,
        trust_env=False,
    )
    app.state.encoder_client = httpx.AsyncClient(
        timeout=httpx.Timeout(config.encoder_service_timeout_s, connect=10.0),
        follow_redirects=True,
        trust_env=False,
        base_url=(config.encoder_service_url.rstrip("/") if config.encoder_service_url else ""),
    )
    app.state.prefill_direct_buffer_client = httpx.AsyncClient(
        timeout=httpx.Timeout(config.prefill_direct_buffer_timeout_s, connect=5.0),
        follow_redirects=True,
        trust_env=False,
        base_url=(
            config.prefill_direct_buffer_service_url.rstrip("/")
            if config.prefill_direct_buffer_service_url
            else ""
        ),
    )
    decode_epoch_monitor_task: Optional[asyncio.Task] = None
    if (
        config.enable_decode_mm_hash_cache
        and config.decode_mm_hash_epoch_poll_s > 0
    ):
        decode_epoch_monitor_task = asyncio.create_task(
            _monitor_decode_worker_epochs(app),
            name="epd-decode-mm-hash-epoch-monitor",
        )
    app.state.decode_mm_hash_epoch_monitor_task = decode_epoch_monitor_task
    prefill_incarnation_monitor_task: Optional[asyncio.Task] = None
    if (
        app.state.direct_feature_handle_cache.enabled
        and config.prefill_incarnation_poll_s > 0
    ):
        prefill_incarnation_monitor_task = asyncio.create_task(
            _monitor_prefill_worker_incarnations(app),
            name="epd-prefill-incarnation-monitor",
        )
    app.state.prefill_incarnation_monitor_task = prefill_incarnation_monitor_task

    try:
        yield
    finally:
        if prefill_incarnation_monitor_task is not None:
            prefill_incarnation_monitor_task.cancel()
            await asyncio.gather(
                prefill_incarnation_monitor_task,
                return_exceptions=True,
            )
        prefill_incarnation_probes = list(
            getattr(app.state, "prefill_incarnation_probe_inflight", {}).values()
        )
        for task in prefill_incarnation_probes:
            if not task.done():
                task.cancel()
        if prefill_incarnation_probes:
            await asyncio.gather(
                *prefill_incarnation_probes,
                return_exceptions=True,
            )
        app.state.prefill_incarnation_probe_inflight.clear()
        if decode_epoch_monitor_task is not None:
            decode_epoch_monitor_task.cancel()
            await asyncio.gather(decode_epoch_monitor_task, return_exceptions=True)
        epoch_probes = list(
            getattr(app.state, "decode_epoch_probe_inflight", {}).values()
        )
        for task in epoch_probes:
            if not task.done():
                task.cancel()
        if epoch_probes:
            await asyncio.gather(*epoch_probes, return_exceptions=True)
        app.state.decode_epoch_probe_inflight.clear()
        mm_fetch_client = getattr(app.state, "mm_fetch_client", None)
        if mm_fetch_client is not None:
            await mm_fetch_client.aclose()
        encoder_client = getattr(app.state, "encoder_client", None)
        if encoder_client is not None:
            await encoder_client.aclose()
        prefill_direct_client = getattr(app.state, "prefill_direct_buffer_client", None)
        inflight = list(
            getattr(app.state, "direct_feature_handle_inflight", {}).values()
        )
        for task in inflight:
            if not task.done():
                task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        render_inflight = list(
            getattr(app.state, "prefill_render_inflight", {}).values()
        )
        for task in render_inflight:
            if not task.done():
                task.cancel()
        if render_inflight:
            await asyncio.gather(*render_inflight, return_exceptions=True)
        render_cache = getattr(app.state, "prefill_render_cache", None)
        if render_cache is not None:
            render_cache.clear()
        decode_mm_hash_cache = getattr(app.state, "decode_mm_hash_cache", None)
        if decode_mm_hash_cache is not None:
            decode_mm_hash_cache.clear()
        cache = getattr(app.state, "direct_feature_handle_cache", None)
        cached_feature_ids = cache.drain() if cache is not None else []
        if prefill_direct_client is not None and cached_feature_ids:
            try:
                response = await prefill_direct_client.post(
                    "release",
                    json={"feature_ids": cached_feature_ids},
                )
                response.raise_for_status()
                await response.aclose()
            except Exception:
                logger.exception("failed to release cached direct feature buffers during shutdown")
        if prefill_direct_client is not None:
            await prefill_direct_client.aclose()
        mm_store = getattr(app.state, "mm_store", None)
        if mm_store is not None:
            mm_store.stop()
        for client_info in list(app.state.prefill_clients) + list(app.state.decode_clients):
            client = client_info.get("client")
            if client is not None:
                await client.aclose()
        control_plane.close()


def create_app(
    config: Optional[ProxyConfig] = None,
    *,
    prefill_clients: Optional[Sequence[Dict[str, Any]]] = None,
    decode_clients: Optional[Sequence[Dict[str, Any]]] = None,
    control_plane: Optional[ServingControlPlane] = None,
    decode_tokenizer: Any = None,
) -> FastAPI:
    config = config or ProxyConfig()
    if config.enable_decode_mm_hash_cache and not config.enable_prerendered_decode:
        raise ValueError(
            "Decode MM hash cache requires the prerendered Decode protocol"
        )
    if config.enable_prerendered_decode and decode_tokenizer is None:
        if not config.prerendered_decode_model:
            raise ValueError(
                "prerendered Decode requires an explicit tokenizer/model path"
            )
        decode_tokenizer = get_tokenizer(
            config.prerendered_decode_model,
            trust_remote_code=True,
        )
    cp = control_plane or ServingControlPlane(
        ServingControlPlaneConfig(
            node_id=config.node_id,
            layers_per_group=config.layers_per_group,
            group_delay_ms=config.group_delay_ms,
            max_group_bytes=config.max_group_bytes,
            warn_rho=config.warn_rho,
            critical_rho=config.critical_rho,
            max_backpressure_delay_ms=config.max_backpressure_delay_ms,
            transport_backend=config.transport_backend,
            owner_shards=config.owner_shards,
            kv_directory_rpc_url=config.kv_directory_rpc_url,
            connector_metrics_dir=config.connector_metrics_dir,
            workflow_registry_wal_path=config.workflow_registry_wal_path,
            workflow_registry_wal_fsync_interval_s=(
                config.workflow_registry_wal_fsync_interval_s
            ),
            workflow_registry_wal_max_pending_records=(
                config.workflow_registry_wal_max_pending_records
            ),
            enable_mm_prefetch=config.enable_mm_prefetch,
            strict_no_fallback=config.strict_no_fallback,
            enable_agent_state_clone=config.enable_agent_state_clone,
            high_prefill_worker_ids=tuple(config.high_prefill_worker_ids),
            low_latency_decode_worker_ids=tuple(config.low_latency_decode_worker_ids),
            standard_prefill_worker_ids=tuple(config.standard_prefill_worker_ids),
            standard_decode_worker_ids=tuple(config.standard_decode_worker_ids),
        )
    )
    app = FastAPI(lifespan=_lifespan)
    app.state.proxy_config = config
    app.state.control_plane = cp
    app.state.direct_feature_handle_cache = _DirectFeatureHandleCache(
        enabled=(
            config.enable_direct_feature_handle_cache
            and bool(config.prefill_direct_buffer_service_url)
        ),
        max_entries=config.direct_feature_handle_cache_max_entries,
        max_bytes=config.direct_feature_handle_cache_max_bytes,
        ttl_s=config.direct_feature_handle_cache_ttl_s,
    )
    app.state.direct_feature_handle_inflight = {}
    app.state.prefill_incarnation_probe_inflight = {}
    app.state.prefill_render_cache = _PrefillRenderCache(
        enabled=config.enable_prefill_render_cache,
        max_entries=config.prefill_render_cache_max_entries,
        max_bytes=config.prefill_render_cache_max_bytes,
        ttl_s=config.prefill_render_cache_ttl_s,
    )
    app.state.prefill_render_inflight = {}
    app.state.client_mm_uuid_reference_stats = {
        "legacy_full_requests": 0,
        "uuid_full_requests": 0,
        "compact_requests": 0,
        "mixed_requests": 0,
        "uuid_items": 0,
        "compact_items": 0,
        "request_body_bytes": 0,
        "compact_request_body_bytes": 0,
        "cold_misses": 0,
        "cold_misses_by_stage": {},
    }
    app.state.decode_mm_hash_cache = _DecodeMMHashWarmCache(
        enabled=config.enable_decode_mm_hash_cache,
        max_entries=config.decode_mm_hash_cache_max_entries,
        ttl_s=config.decode_mm_hash_cache_ttl_s,
    )
    app.state.decode_epoch_probe_inflight = {}
    app.state.decode_tokenizer = decode_tokenizer
    app.state.prerendered_decode_stats = {
        "selected": 0,
        "streaming": 0,
        "non_streaming": 0,
        "bypassed": 0,
        "bypass_reasons": {},
    }
    app.state.decode_pipeline_stats = {
        "eligible": 0,
        "active": 0,
        "suppressed_inflight": 0,
    }
    app.state.prefill_client_overrides = list(prefill_clients) if prefill_clients is not None else None
    app.state.decode_client_overrides = list(decode_clients) if decode_clients is not None else None
    app.state.mm_store = MMStore(
        transfer_engine=TransferEngine(protocol="local"),
        max_queue_size=max(1, int(config.mm_prefetch_queue_size)),
        dispatcher_workers=2,
        inline_fallback_on_queue_full=not config.strict_no_fallback,
    ) if config.enable_mm_prefetch else None

    @app.get("/health")
    @app.get("/healthcheck")
    async def health() -> Dict[str, Any]:
        return {"status": "ok", "prefill_clients": len(app.state.prefill_clients), "decode_clients": len(app.state.decode_clients)}

    @app.get("/ready")
    async def ready() -> Response:
        async def _check(
            label: str,
            client: httpx.AsyncClient,
            path: str = "/health",
        ) -> Tuple[str, Dict[str, Any]]:
            try:
                response = await asyncio.wait_for(client.get(path), timeout=1.0)
            except Exception as exc:
                return label, {
                    "ready": False,
                    "error": type(exc).__name__,
                }
            try:
                return label, {
                    "ready": response.status_code == 200,
                    "status_code": int(response.status_code),
                }
            finally:
                await response.aclose()

        probes = [
            _check(
                f"prefill:{client_info['worker_id']}",
                client_info["client"],
            )
            for client_info in app.state.prefill_clients
        ]
        probes.extend(
            _check(
                f"decode:{client_info['worker_id']}",
                client_info["client"],
            )
            for client_info in app.state.decode_clients
        )
        if config.encoder_service_url:
            probes.append(_check("encoder", app.state.encoder_client))
        if config.prefill_direct_buffer_service_url:
            probes.append(
                _check("prefill_direct_buffer", app.state.prefill_direct_buffer_client)
            )
        results = dict(await asyncio.gather(*probes))
        is_ready = bool(results) and all(
            bool(item.get("ready")) for item in results.values()
        )
        return JSONResponse(
            {
                "status": "ready" if is_ready else "not_ready",
                "upstreams": results,
            },
            status_code=200 if is_ready else 503,
        )

    @app.get("/metrics")
    async def metrics() -> Dict[str, Any]:
        payload = cp.snapshot()
        payload["proxy_upstream_http_pool"] = {
            "max_connections": config.upstream_max_connections,
            "max_keepalive_connections": config.upstream_max_keepalive_connections,
            "keepalive_expiry_s": config.upstream_keepalive_expiry_s,
        }
        mm_store = getattr(app.state, "mm_store", None)
        if mm_store is not None:
            payload["mm_store"] = mm_store.stats()
        direct_cache_stats = app.state.direct_feature_handle_cache.stats()
        direct_cache_stats["inflight"] = len(app.state.direct_feature_handle_inflight)
        direct_cache_stats["incarnation_monitor_enabled"] = bool(
            app.state.direct_feature_handle_cache.enabled
            and config.prefill_incarnation_poll_s > 0
        )
        direct_cache_stats["incarnation_poll_s"] = float(
            config.prefill_incarnation_poll_s
        )
        direct_cache_stats["incarnation_poll_jitter_ratio"] = float(
            config.prefill_incarnation_poll_jitter_ratio
        )
        if float(config.prefill_incarnation_poll_s) > 0:
            direct_cache_stats["incarnation_poll_delay_min_s"] = max(
                0.01,
                float(config.prefill_incarnation_poll_s)
                * (1.0 - float(config.prefill_incarnation_poll_jitter_ratio)),
            )
            direct_cache_stats["incarnation_poll_delay_max_s"] = max(
                0.01,
                float(config.prefill_incarnation_poll_s)
                * (1.0 + float(config.prefill_incarnation_poll_jitter_ratio)),
            )
        else:
            direct_cache_stats["incarnation_poll_delay_min_s"] = 0.0
            direct_cache_stats["incarnation_poll_delay_max_s"] = 0.0
        direct_cache_stats["incarnation_probe_timeout_s"] = float(
            config.prefill_incarnation_probe_timeout_s
        )
        direct_cache_stats["incarnation_failure_threshold"] = int(
            config.prefill_incarnation_failure_threshold
        )
        direct_cache_stats["incarnation_freshness_s"] = float(
            config.prefill_incarnation_freshness_s
        )
        direct_cache_stats["incarnation_endpoint"] = str(
            config.prefill_incarnation_endpoint
        )
        direct_cache_stats["incarnation_guard_enabled"] = bool(
            config.enable_prefill_incarnation_guard
        )
        direct_cache_stats["incarnation_probe_singleflight_enabled"] = bool(
            config.enable_prefill_incarnation_probe_singleflight
        )
        payload["direct_feature_handle_cache"] = direct_cache_stats
        render_cache_stats = app.state.prefill_render_cache.stats()
        render_cache_stats["inflight"] = len(app.state.prefill_render_inflight)
        payload["prefill_render_cache"] = render_cache_stats
        uuid_stats = dict(app.state.client_mm_uuid_reference_stats)
        uuid_stats["cold_misses_by_stage"] = dict(
            uuid_stats.get("cold_misses_by_stage") or {}
        )
        uuid_stats["enabled"] = bool(config.enable_client_mm_uuid_references)
        payload["client_mm_uuid_references"] = uuid_stats
        decode_mm_hash_stats = app.state.decode_mm_hash_cache.stats()
        decode_mm_hash_stats["epoch_monitor_enabled"] = bool(
            config.enable_decode_mm_hash_cache
            and config.decode_mm_hash_epoch_poll_s > 0
        )
        decode_mm_hash_stats["epoch_poll_s"] = float(
            config.decode_mm_hash_epoch_poll_s
        )
        decode_mm_hash_stats["epoch_probe_timeout_s"] = float(
            config.decode_mm_hash_epoch_probe_timeout_s
        )
        decode_mm_hash_stats["epoch_freshness_s"] = float(
            config.decode_mm_hash_epoch_freshness_s
        )
        decode_mm_hash_stats["epoch_endpoint"] = str(
            config.decode_mm_hash_epoch_endpoint
        )
        decode_mm_hash_stats["epoch_guard_enabled"] = bool(
            config.enable_decode_mm_hash_epoch_guard
        )
        decode_mm_hash_stats["epoch_probe_singleflight_enabled"] = bool(
            config.enable_decode_mm_hash_epoch_probe_singleflight
        )
        payload["decode_mm_hash_cache"] = decode_mm_hash_stats
        prerendered_stats = dict(app.state.prerendered_decode_stats)
        prerendered_stats["bypass_reasons"] = dict(
            prerendered_stats.get("bypass_reasons") or {}
        )
        prerendered_stats["enabled"] = bool(config.enable_prerendered_decode)
        prerendered_stats["model"] = config.prerendered_decode_model
        payload["prerendered_decode"] = prerendered_stats
        pipeline_stats = dict(app.state.decode_pipeline_stats)
        pipeline_stats["enabled"] = bool(config.enable_decode_pipeline)
        pipeline_stats["max_inflight"] = int(config.decode_pipeline_max_inflight)
        payload["decode_pipeline"] = pipeline_stats
        return payload

    @app.post("/mooncake_epd/agent_state/register")
    async def register_agent_state(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return cp.register_agent_state(
                workflow_id=str(payload.get("workflow_id") or ""),
                state_id=(
                    str(payload.get("state_id"))
                    if payload.get("state_id") is not None
                    else None
                ),
                kv_block_ids=[
                    str(item)
                    for item in list(payload.get("kv_block_ids") or payload.get("block_ids") or [])
                ],
                token_ids=[
                    int(item)
                    for item in list(payload.get("token_ids") or [])
                ],
                feature_hashes=[
                    str(item)
                    for item in list(payload.get("feature_hashes") or payload.get("image_ids") or [])
                ],
                target_node_id=(
                    str(payload.get("target_node_id"))
                    if payload.get("target_node_id") is not None
                    else None
                ),
                kv_transfer_params=(
                    dict(payload.get("kv_transfer_params") or payload.get("consume_kv_transfer_params") or {})
                    if isinstance(payload.get("kv_transfer_params") or payload.get("consume_kv_transfer_params"), dict)
                    else None
                ),
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/mooncake_epd/agent_state/fork")
    @app.post("/mooncake_epd/agent/fork")
    async def fork_agent_state(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return cp.fork_workflow_state(
                workflow_id=str(payload.get("workflow_id") or ""),
                parent_request_id=(
                    str(payload.get("parent_request_id"))
                    if payload.get("parent_request_id") is not None
                    else None
                ),
                branch_count=int(payload.get("branch_count", 2) or 2),
                target_node_id=(
                    str(payload.get("target_node_id"))
                    if payload.get("target_node_id") is not None
                    else None
                ),
                for_write=bool(payload.get("for_write", False)),
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/mooncake_epd/agent_state/materialize")
    async def materialize_agent_state(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return cp.materialize_agent_state(
                state_id=str(payload.get("state_id") or ""),
                target_node_id=(
                    str(payload.get("target_node_id"))
                    if payload.get("target_node_id") is not None
                    else None
                ),
                for_write=bool(payload.get("for_write", False)),
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/mooncake_epd/agent_state/consume")
    async def consume_agent_state(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            request_id = str(payload.get("request_id") or uuid.uuid4().hex)
            req_data = dict(payload.get("request") or {})
            metadata = dict(req_data.get("metadata") or {})
            if payload.get("workflow_id") and not metadata.get("workflow_id"):
                metadata["workflow_id"] = str(payload.get("workflow_id"))
                req_data["metadata"] = metadata
            ctx = cp.start_request(req_data, request_id)
            target = str(payload.get("target_node_id") or payload.get("decode_worker_id") or cp.config.target_agent_id)
            kv = cp.consume_agent_state(
                ctx,
                state_id=str(payload.get("state_id") or payload.get("agent_state_id") or ""),
                target_node_id=target,
                for_write=bool(payload.get("for_write", False)),
            )
            cp.finish_request(request_id)
            return {
                "request_id": request_id,
                "state_id": str(payload.get("state_id") or payload.get("agent_state_id") or ""),
                "target_node_id": target,
                "consume_kv_transfer_params": kv,
            }
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/mooncake_epd/agent_state/release")
    async def release_agent_state(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return cp.release_agent_state(
                state_id=str(payload.get("state_id") or ""),
                release_physical=bool(payload.get("release_physical", True)),
                sweep_orphans=bool(payload.get("sweep_orphans", True)),
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/mooncake_epd/agent_state/stats")
    async def agent_state_stats(sweep_orphans: bool = False) -> Dict[str, Any]:
        return cp.agent_state_stats(sweep_orphans=bool(sweep_orphans))

    @app.post("/v1/completions")
    async def handle_completions(request: Request):
        return await _handle_generation_request(app, "/v1/completions", request)

    @app.post("/v1/chat/completions")
    async def handle_chat_completions(request: Request):
        return await _handle_generation_request(app, "/v1/chat/completions", request)

    return app


async def _cancel_background_task(task: Optional[asyncio.Task]) -> None:
    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await task
    except BaseException:
        return


async def _abort_pipelined_decode_dispatch(
    dispatch: Optional[_PipelinedDecodeDispatch],
) -> None:
    if dispatch is None:
        return
    if dispatch.mm_hash_lease is not None:
        dispatch.mm_hash_lease.fail()
    task = dispatch.open_task
    if not task.done():
        task.cancel()
    try:
        opened = await task
    except BaseException:
        return
    await _close_opened_decode_stream(opened)


async def _open_decode_stream_after_backpressure(
    *,
    api: str,
    control_plane: ServingControlPlane,
    ctx,
    decode_client: Dict[str, Any],
    decode_payload: Dict[str, Any],
    decode_headers: Dict[str, str],
    wait_ms: float,
) -> _OpenedDecodeStream:
    if wait_ms > 0:
        wait_started = time.monotonic()
        await asyncio.sleep(wait_ms / 1000.0)
        control_plane.record_stage_span(
            ctx,
            "decode_backpressure_wait",
            started_at=wait_started,
        )
    return await _open_decode_stream(
        api=api,
        decode_client=decode_client,
        decode_payload=decode_payload,
        decode_headers=decode_headers,
    )


async def _await_pipelined_prefill(
    *,
    prefill_task: asyncio.Task,
    dispatch: _PipelinedDecodeDispatch,
) -> Dict[str, Any]:
    done, _ = await asyncio.wait(
        {prefill_task, dispatch.open_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    if dispatch.open_task in done:
        if dispatch.open_task.cancelled():
            await _cancel_background_task(prefill_task)
            raise _DecodePipelineStartupError(
                "early Decode stream was cancelled before Prefill completed"
            )
        startup_error = dispatch.open_task.exception()
        if startup_error is not None:
            if (
                dispatch.mm_hash_lease is not None
                and _is_decode_epoch_guard_rejection(startup_error)
            ):
                dispatch.mm_hash_lease.record_epoch_guard_rejection(
                    _decode_epoch_guard_rejection_epoch(startup_error)
                )
            await _cancel_background_task(prefill_task)
            raise _DecodePipelineStartupError(
                f"{type(startup_error).__name__}: {startup_error}"
            ) from startup_error
    return await prefill_task


async def _handle_generation_request(app: FastAPI, api: str, request: Request):
    request_started = time.monotonic()
    req_data = await request.json()
    request_body_bytes = len(await request.body())
    request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
    req_data = _merge_control_headers(req_data, request)
    try:
        client_mm_uuid_mode, uuid_items, compact_items = (
            _client_mm_uuid_request_mode(req_data)
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    config: ProxyConfig = app.state.proxy_config
    if compact_items and not config.enable_client_mm_uuid_references:
        raise HTTPException(
            status_code=400,
            detail=(
                "UUID-only multimodal inputs require "
                "--enable-client-mm-uuid-references"
            ),
        )
    _record_client_mm_uuid_request(
        app,
        mode=client_mm_uuid_mode,
        uuid_items=uuid_items,
        compact_items=compact_items,
        request_body_bytes=request_body_bytes,
    )
    request_parsed = time.monotonic()
    control_plane: ServingControlPlane = app.state.control_plane
    ctx = control_plane.start_request(
        req_data,
        request_id,
        created_at=request_started,
    )
    control_plane.record_stage_span(
        ctx,
        "proxy_parse",
        started_at=request_started,
        ended_at=request_parsed,
    )

    prefill_admission_started = time.monotonic()
    try:
        prefill_decision = _admit_or_raise(control_plane, "prefill", ctx)
    except HTTPException:
        control_plane.record_stage_span(
            ctx,
            "prefill_admission",
            started_at=prefill_admission_started,
        )
        control_plane.finish_request(request_id)
        raise
    control_plane.record_stage_span(
        ctx,
        "prefill_admission",
        started_at=prefill_admission_started,
    )
    decode_peek = _peek_lowest_load_worker(control_plane, "decode")
    prefill_client = _client_for_worker(app.state.prefill_clients, prefill_decision.worker_id)
    if prefill_client is None:
        control_plane.mark_stage_complete(
            "prefill",
            prefill_decision.worker_id,
            latency_ms=0.0,
            success=False,
        )
        control_plane.finish_request(request_id)
        raise HTTPException(status_code=503, detail="no prefill client available")

    if prefill_decision.wait_ms > 0:
        prefill_wait_started = time.monotonic()
        await asyncio.sleep(prefill_decision.wait_ms / 1000.0)
        control_plane.record_stage_span(
            ctx,
            "prefill_backpressure_wait",
            started_at=prefill_wait_started,
        )

    mm_prepare_started = time.monotonic()
    try:
        req_data = await _prepare_multimodal_inputs_for_prefill(
            app=app,
            req_data=req_data,
            ctx=ctx,
            target_worker_id=prefill_decision.worker_id,
        )
    except HTTPException:
        control_plane.record_stage_span(
            ctx,
            "mm_prepare",
            started_at=mm_prepare_started,
        )
        control_plane.mark_stage_complete(
            "prefill",
            prefill_decision.worker_id,
            latency_ms=0.0,
            success=False,
        )
        control_plane.finish_request(request_id)
        raise
    control_plane.record_stage_span(
        ctx,
        "mm_prepare",
        started_at=mm_prepare_started,
    )

    prefill_headers = _prefill_request_headers(
        app=app,
        request=request,
        request_id=request_id,
        prefill_client=prefill_client,
    )
    prefill_start = time.monotonic()
    prefill_response = None
    prompt_only_prefill = _should_use_prompt_only_prefill(api)
    cached_topology = _cached_prefill_kv_topology(prefill_client)
    pipeline_eligible = bool(
        config.enable_decode_pipeline
        and prompt_only_prefill
        and req_data.get("stream")
        and cached_topology is not None
    )
    if pipeline_eligible:
        app.state.decode_pipeline_stats["eligible"] += 1
    pipeline_inflight = int(getattr(decode_peek, "current_load", 0) or 0)
    pipeline_limit = int(config.decode_pipeline_max_inflight)
    use_decode_pipeline = bool(
        pipeline_eligible
        and (pipeline_limit <= 0 or pipeline_inflight < pipeline_limit)
    )
    if use_decode_pipeline:
        app.state.decode_pipeline_stats["active"] += 1
    elif pipeline_eligible:
        app.state.decode_pipeline_stats["suppressed_inflight"] += 1
    decode_decision = None
    decode_client = None
    pipelined_dispatch: Optional[_PipelinedDecodeDispatch] = None
    pipelined_prefill_task: Optional[asyncio.Task] = None
    pipelined_handoff_id: Optional[str] = None
    rendered_payload: Optional[Dict[str, Any]] = None
    prerendered_decode_context: Optional[_PrerenderedDecodeContext] = None
    decode_mm_hash_lease: Optional[_DecodeMMHashLease] = None
    prerendered_bypass_reason = None
    use_prerendered_decode = False
    if config.enable_prerendered_decode:
        prerendered_bypass_reason = _prerendered_decode_bypass_reason(
            api=api,
            request_body=req_data,
            tokenizer_obj=app.state.decode_tokenizer,
        )
        use_prerendered_decode = prerendered_bypass_reason is None
        _record_prerendered_decode_selection(
            app,
            selected=use_prerendered_decode,
            stream=bool(req_data.get("stream")),
            bypass_reason=prerendered_bypass_reason,
        )

    async def _finalize_failed_prefill() -> None:
        await _cancel_background_task(pipelined_prefill_task)
        await _abort_pipelined_decode_dispatch(pipelined_dispatch)
        failed_at = time.monotonic()
        control_plane.record_stage_span(
            ctx,
            "prefill_dispatch",
            started_at=prefill_start,
            ended_at=failed_at,
        )
        control_plane.mark_stage_complete(
            "prefill",
            prefill_client["worker_id"],
            latency_ms=(failed_at - prefill_start) * 1000.0,
            success=False,
        )
        if use_decode_pipeline and decode_decision is not None:
            decode_started_at = (
                pipelined_dispatch.started_at
                if pipelined_dispatch is not None
                else failed_at
            )
            control_plane.mark_stage_complete(
                "decode",
                decode_decision.worker_id,
                latency_ms=max(0.0, (failed_at - decode_started_at) * 1000.0),
                success=False,
            )
        control_plane.finish_request(request_id)

    try:
        if use_decode_pipeline:
            decode_admission_started = time.monotonic()
            try:
                decode_decision = _admit_or_raise(control_plane, "decode", ctx)
            except HTTPException:
                control_plane.record_stage_span(
                    ctx,
                    "decode_admission",
                    started_at=decode_admission_started,
                )
                raise
            control_plane.record_stage_span(
                ctx,
                "decode_admission",
                started_at=decode_admission_started,
            )
            decode_client = _client_for_worker(
                app.state.decode_clients,
                decode_decision.worker_id,
            )
            if decode_client is None:
                raise HTTPException(status_code=503, detail="no decode client available")
            pipelined_handoff_id = control_plane.reserve_handoff_id(ctx)

        prefill_base_params = dict(req_data.get("kv_transfer_params") or {})
        prefill_kv_params = control_plane.build_prefill_kv_params(
            ctx,
            prefill_decision,
            decode_worker_id=(
                decode_decision.worker_id
                if decode_decision is not None
                else (decode_peek.worker_id if decode_peek is not None else None)
            ),
            base_params=prefill_base_params,
        )
        if prefill_base_params.get("mm_feature_handles"):
            prefill_kv_params["mm_prefetch_policy"] = "feature_handle"
            prefill_kv_params["mm_feature_handles"] = prefill_base_params["mm_feature_handles"]
            prefill_kv_params["mm_feature_handle_target_worker"] = prefill_base_params.get(
                "mm_feature_handle_target_worker",
                prefill_decision.worker_id,
            )
        if prompt_only_prefill:
            prefill_render_started = time.monotonic()
            try:
                rendered_payload = await _get_or_render_prompt_only_prefill(
                    app=app,
                    api=api,
                    prefill_client=prefill_client,
                    prefill_headers=prefill_headers,
                    request_body=req_data,
                    mm_hashes=list(getattr(ctx, "mm_hashes", []) or []),
                )
                if use_prerendered_decode:
                    prerendered_decode_context = _build_prerendered_decode_context(
                        rendered_payload,
                        tokenizer_obj=app.state.decode_tokenizer,
                    )
            finally:
                control_plane.record_stage_span(
                    ctx,
                    "prefill_render",
                    started_at=prefill_render_started,
                )
            prefill_generate_started = time.monotonic()
            try:
                if use_decode_pipeline:
                    assert cached_topology is not None
                    assert decode_decision is not None
                    assert decode_client is not None
                    provisional_decode_kv = _build_pipelined_decode_kv_params(
                        prefill_kv_params=prefill_kv_params,
                        topology=cached_topology,
                        prefill_worker_id=prefill_client["worker_id"],
                        decode_decision=decode_decision,
                        handoff_id=str(pipelined_handoff_id or ""),
                    )
                    if use_prerendered_decode:
                        provisional_decode_payload = (
                            _inject_decode_kv_into_rendered_request(
                                rendered_payload,
                                request_id=request_id,
                                request_body=req_data,
                                kv_transfer_params=provisional_decode_kv,
                            )
                        )
                        await _fence_decode_mm_hash_reuse(
                            app=app,
                            decode_client=decode_client,
                        )
                        (
                            provisional_decode_payload,
                            decode_mm_hash_lease,
                        ) = app.state.decode_mm_hash_cache.prepare(
                            worker_id=decode_client["worker_id"],
                            payload=provisional_decode_payload,
                        )
                        provisional_decode_api = "/inference/v1/generate"
                    else:
                        provisional_decode_payload = dict(req_data)
                        provisional_decode_payload["kv_transfer_params"] = (
                            provisional_decode_kv
                        )
                        provisional_decode_api = api
                    pipelined_prefill_task = asyncio.create_task(
                        _dispatch_rendered_prompt_only_prefill(
                            prefill_client=prefill_client,
                            prefill_headers=prefill_headers,
                            request_id=request_id,
                            rendered_payload=rendered_payload,
                            kv_transfer_params=prefill_kv_params,
                        ),
                        name=f"epd-prefill-{request_id}",
                    )
                    pipeline_started = time.monotonic()
                    decode_open_task = asyncio.create_task(
                        _open_decode_stream_after_backpressure(
                            api=provisional_decode_api,
                            control_plane=control_plane,
                            ctx=ctx,
                            decode_client=decode_client,
                            decode_payload=provisional_decode_payload,
                            decode_headers=_decode_request_headers(
                                app=app,
                                request=request,
                                request_id=request_id,
                                mm_hash_lease=decode_mm_hash_lease,
                            ),
                            wait_ms=float(decode_decision.wait_ms),
                        ),
                        name=f"epd-decode-open-{request_id}",
                    )
                    pipelined_dispatch = _PipelinedDecodeDispatch(
                        decision=decode_decision,
                        client=decode_client,
                        started_at=pipeline_started,
                        open_task=decode_open_task,
                        topology=dict(cached_topology),
                        kv_transfer_params=provisional_decode_kv,
                        mm_hash_lease=decode_mm_hash_lease,
                    )
                    prefill_json = await _await_pipelined_prefill(
                        prefill_task=pipelined_prefill_task,
                        dispatch=pipelined_dispatch,
                    )
                else:
                    prefill_json = await _dispatch_rendered_prompt_only_prefill(
                        prefill_client=prefill_client,
                        prefill_headers=prefill_headers,
                        request_id=request_id,
                        rendered_payload=rendered_payload,
                        kv_transfer_params=prefill_kv_params,
                    )
            finally:
                control_plane.record_stage_span(
                    ctx,
                    "prefill_generate",
                    started_at=prefill_generate_started,
                )
        else:
            prefill_payload = dict(req_data)
            prefill_payload["kv_transfer_params"] = prefill_kv_params
            prefill_payload["stream"] = False
            prefill_payload["max_tokens"] = 1
            if "max_completion_tokens" in prefill_payload:
                prefill_payload["max_completion_tokens"] = 1
            prefill_payload.pop("stream_options", None)
            prefill_response = await prefill_client["client"].post(
                api,
                json=prefill_payload,
                headers=prefill_headers,
            )
            prefill_response.raise_for_status()
            prefill_json = prefill_response.json()
    except asyncio.CancelledError:
        await _finalize_failed_prefill()
        raise
    except HTTPException:
        await _finalize_failed_prefill()
        raise
    except _DecodePipelineStartupError as exc:
        await _finalize_failed_prefill()
        raise HTTPException(
            status_code=502,
            detail=f"decode request failed during early pipeline startup: {exc}",
        ) from exc
    except Exception as exc:
        await _learn_prefill_incarnation_guard_rejection(
            app=app,
            prefill_client=prefill_client,
            exc=exc,
        )
        await _finalize_failed_prefill()
        raise HTTPException(
            status_code=502,
            detail=f"prefill request failed: {type(exc).__name__}: {exc}",
        ) from exc
    finally:
        if prefill_response is not None:
            await prefill_response.aclose()

    try:
        await _release_direct_feature_buffers_after_prefill(app, req_data)
    except asyncio.CancelledError:
        await _abort_pipelined_decode_dispatch(pipelined_dispatch)
        prefill_end = time.monotonic()
        control_plane.record_stage_span(
            ctx,
            "prefill_dispatch",
            started_at=prefill_start,
            ended_at=prefill_end,
        )
        control_plane.mark_stage_complete(
            "prefill",
            prefill_client["worker_id"],
            latency_ms=(prefill_end - prefill_start) * 1000.0,
            success=True,
        )
        if decode_decision is not None:
            control_plane.mark_stage_complete(
                "decode",
                decode_decision.worker_id,
                latency_ms=0.0,
                success=False,
            )
        control_plane.finish_request(request_id)
        raise

    prefill_end = time.monotonic()
    control_plane.record_stage_span(
        ctx,
        "prefill_dispatch",
        started_at=prefill_start,
        ended_at=prefill_end,
    )
    control_plane.mark_stage_complete(
        "prefill",
        prefill_client["worker_id"],
        latency_ms=(prefill_end - prefill_start) * 1000.0,
        success=True,
    )
    actual_topology = _cache_prefill_kv_topology(
        prefill_client,
        prefill_json.get("kv_transfer_params"),
    )
    if pipelined_dispatch is not None and actual_topology != pipelined_dispatch.topology:
        await _abort_pipelined_decode_dispatch(pipelined_dispatch)
        control_plane.mark_stage_complete(
            "decode",
            pipelined_dispatch.decision.worker_id,
            latency_ms=max(
                0.0,
                (time.monotonic() - pipelined_dispatch.started_at) * 1000.0,
            ),
            success=False,
        )
        control_plane.finish_request(request_id)
        raise HTTPException(
            status_code=502,
            detail=(
                "Prefill KV topology changed while the early Decode request "
                "was in flight; strict mode will not retry or fall back"
            ),
        )
    prefill_continuation = (
        _PrefillContinuation()
        if prompt_only_prefill
        else _extract_prefill_continuation(api, prefill_json)
    )
    if not prompt_only_prefill and _should_short_circuit_after_prefill(req_data, prefill_continuation):
        control_plane.mark_first_token(ctx)
        control_plane.finish_request(request_id)
        return _build_prefill_terminal_response(
            api=api,
            prefill_json=prefill_json,
            request_id=request_id,
            routing_path=ctx.routing_path,
            admission_action=prefill_decision.decision.action.value,
            degrade_level=ctx.degrade_level.value,
        )

    if decode_decision is None:
        decode_admission_started = time.monotonic()
        try:
            decode_decision = _admit_or_raise(control_plane, "decode", ctx)
        except HTTPException:
            control_plane.record_stage_span(
                ctx,
                "decode_admission",
                started_at=decode_admission_started,
            )
            control_plane.finish_request(request_id)
            raise
        control_plane.record_stage_span(
            ctx,
            "decode_admission",
            started_at=decode_admission_started,
        )
        decode_client = _client_for_worker(
            app.state.decode_clients,
            decode_decision.worker_id,
        )
        if decode_client is None:
            control_plane.mark_stage_complete(
                "decode",
                decode_decision.worker_id,
                latency_ms=0.0,
                success=False,
            )
            control_plane.finish_request(request_id)
            raise HTTPException(status_code=503, detail="no decode client available")

    assert decode_client is not None

    try:
        prefill_kv = control_plane.note_prefill_response(
            ctx,
            prefill_json.get("kv_transfer_params"),
            decode_worker_id=decode_client["worker_id"],
        )
    except Exception as exc:
        await _abort_pipelined_decode_dispatch(pipelined_dispatch)
        control_plane.mark_stage_complete(
            "decode",
            decode_decision.worker_id,
            latency_ms=0.0,
            success=False,
        )
        control_plane.finish_request(request_id)
        raise HTTPException(
            status_code=502,
            detail=f"prefill response missing usable KV handoff metadata: {exc}",
        ) from exc

    if pipelined_dispatch is None and decode_decision.wait_ms > 0:
        decode_wait_started = time.monotonic()
        await asyncio.sleep(decode_decision.wait_ms / 1000.0)
        control_plane.record_stage_span(
            ctx,
            "decode_backpressure_wait",
            started_at=decode_wait_started,
        )

    decode_prepare_started = time.monotonic()
    try:
        decode_kv_params = control_plane.build_decode_kv_params(
            ctx,
            decode_decision,
            prefill_kv,
        )
        if pipelined_dispatch is not None:
            _validate_pipelined_decode_kv_params(
                pipelined_dispatch.kv_transfer_params,
                decode_kv_params,
            )
        if prefill_continuation.active:
            decode_kv_params.update(
                _build_prefill_decode_semantic_hints(
                    continuation=prefill_continuation,
                    prefill_kv=prefill_kv,
                )
            )
        if use_prerendered_decode:
            if rendered_payload is None or prerendered_decode_context is None:
                raise RuntimeError(
                    "prerendered Decode was selected without a validated render payload"
                )
            if pipelined_dispatch is not None:
                # The early stream already owns the provisional payload. The
                # actual Prefill topology was checked above, so rebuilding the
                # full rendered payload here would only rescan/copy large MM
                # features that _dispatch_streaming_decode never consumes.
                decode_payload = {}
                decode_mm_hash_lease = pipelined_dispatch.mm_hash_lease
            else:
                decode_payload = _inject_decode_kv_into_rendered_request(
                    rendered_payload,
                    request_id=request_id,
                    request_body=req_data,
                    kv_transfer_params=decode_kv_params,
                )
                await _fence_decode_mm_hash_reuse(
                    app=app,
                    decode_client=decode_client,
                )
                (
                    decode_payload,
                    decode_mm_hash_lease,
                ) = app.state.decode_mm_hash_cache.prepare(
                    worker_id=decode_client["worker_id"],
                    payload=decode_payload,
                )
            decode_api = "/inference/v1/generate"
        else:
            decode_payload = dict(req_data)
            decode_payload = _apply_prefill_continuation_to_decode_payload(
                api=api,
                decode_payload=decode_payload,
                continuation=prefill_continuation,
            )
            decode_payload["kv_transfer_params"] = decode_kv_params
            decode_api = api
    except Exception as exc:
        control_plane.record_stage_span(
            ctx,
            "decode_prepare",
            started_at=decode_prepare_started,
        )
        await _abort_pipelined_decode_dispatch(pipelined_dispatch)
        control_plane.rollback_handoff(ctx)
        control_plane.mark_stage_complete(
            "decode",
            decode_decision.worker_id,
            latency_ms=0.0,
            success=False,
        )
        control_plane.finish_request(request_id)
        raise HTTPException(
            status_code=502,
            detail=f"decode request missing usable KV transfer params: {exc}",
        ) from exc
    control_plane.record_stage_span(
        ctx,
        "decode_prepare",
        started_at=decode_prepare_started,
    )

    response_headers = {
        "X-Request-Id": request_id,
        "X-EPD-Routing-Path": ctx.routing_path,
        "X-EPD-Admission": decode_decision.decision.action.value,
        "X-EPD-Degrade-Level": ctx.degrade_level.value,
        "X-EPD-Decode-Pipeline": (
            "active" if pipelined_dispatch is not None else "serial"
        ),
        "X-EPD-Decode-Protocol": (
            "prerendered-generate" if use_prerendered_decode else "openai"
        ),
        "X-EPD-Decode-MM-Features": (
            decode_mm_hash_lease.mode if decode_mm_hash_lease is not None else "unchanged"
        ),
        "X-EPD-Decode-Worker": str(decode_client["worker_id"]),
        "X-EPD-Client-MM-UUID-Mode": client_mm_uuid_mode,
    }
    if bool(req_data.get("stream")):
        return await _dispatch_streaming_decode(
            api=api,
            decode_api=decode_api,
            control_plane=control_plane,
            ctx=ctx,
            decode_client=decode_client,
            decode_payload=decode_payload,
            decode_headers=_decode_request_headers(
                app=app,
                request=request,
                request_id=request_id,
                mm_hash_lease=decode_mm_hash_lease,
            ),
            response_headers=response_headers,
            continuation=prefill_continuation,
            pipelined_dispatch=pipelined_dispatch,
            prerendered_context=prerendered_decode_context,
            mm_hash_lease=decode_mm_hash_lease,
        )
    return await _dispatch_non_streaming_decode(
        api=api,
        decode_api=decode_api,
        control_plane=control_plane,
        ctx=ctx,
        decode_client=decode_client,
        decode_payload=decode_payload,
        decode_headers=_decode_request_headers(
            app=app,
            request=request,
            request_id=request_id,
            mm_hash_lease=decode_mm_hash_lease,
        ),
        response_headers=response_headers,
        continuation=prefill_continuation,
        prerendered_context=prerendered_decode_context,
        mm_hash_lease=decode_mm_hash_lease,
    )



async def _prepare_multimodal_inputs_for_prefill(
    *,
    app: FastAPI,
    req_data: Dict[str, Any],
    ctx,
    target_worker_id: str,
) -> Dict[str, Any]:
    config: ProxyConfig = app.state.proxy_config
    mode = str(config.mm_prefetch_mode or "asset_bytes").strip().lower()
    if mode == "asset_bytes":
        return await _prefetch_and_rewrite_multimodal_assets(
            app=app,
            req_data=req_data,
            ctx=ctx,
            target_worker_id=target_worker_id,
        )
    if mode == "feature_handle":
        return await _prepare_feature_handle_multimodal_inputs(
            app=app,
            req_data=req_data,
            ctx=ctx,
            config=config,
            target_worker_id=target_worker_id,
        )
    raise HTTPException(status_code=500, detail=f"unsupported mm_prefetch_mode: {mode}")


def _copy_request_for_mm_url_rewrite(req_data: Dict[str, Any]) -> Dict[str, Any]:
    """Structurally copy only containers mutated by MM URL rewriting.

    OpenAI-compatible multimodal requests can contain multi-megabyte data URLs.
    A JSON serialize/parse round trip duplicates those strings and spends CPU on
    content that remains immutable.  This copy keeps large strings shared while
    cloning every dict/list shell that `_set_image_url_on_item` may mutate.
    """

    rewritten = dict(req_data)

    messages = req_data.get("messages")
    if isinstance(messages, list):
        copied_messages: List[Any] = []
        for message in messages:
            if not isinstance(message, dict):
                copied_messages.append(message)
                continue
            copied_message = dict(message)
            content = message.get("content")
            if isinstance(content, list):
                copied_content: List[Any] = []
                for item in content:
                    if not isinstance(item, dict):
                        copied_content.append(item)
                        continue
                    copied_item = dict(item)
                    image_url = item.get("image_url")
                    if isinstance(image_url, dict):
                        copied_item["image_url"] = dict(image_url)
                    copied_content.append(copied_item)
                copied_message["content"] = copied_content
            copied_messages.append(copied_message)
        rewritten["messages"] = copied_messages

    prompt = req_data.get("prompt")
    if isinstance(prompt, list):
        copied_prompt: List[Any] = []
        for item in prompt:
            if not isinstance(item, dict):
                copied_prompt.append(item)
                continue
            copied_item = dict(item)
            image_url = item.get("image_url")
            if isinstance(image_url, dict):
                copied_item["image_url"] = dict(image_url)
            copied_prompt.append(copied_item)
        rewritten["prompt"] = copied_prompt

    return rewritten


async def _prepare_feature_handle_multimodal_inputs(
    *,
    app: FastAPI,
    req_data: Dict[str, Any],
    ctx,
    config: ProxyConfig,
    target_worker_id: str,
) -> Dict[str, Any]:
    if not config.enable_mm_prefetch or not getattr(ctx, "mm_hashes", None):
        return req_data
    if not config.prefill_supports_feature_handles:
        raise HTTPException(
            status_code=501,
            detail=(
                "mm_prefetch_mode=feature_handle requires a Prefill runtime that "
                "can consume external multimodal hidden-state handles"
            ),
        )
    metadata = dict(req_data.get("metadata") or {})
    raw_handles = (
        metadata.get("mooncake_epd_feature_handles")
        or metadata.get("feature_handles")
        or req_data.get("mooncake_epd_feature_handles")
    )
    if not isinstance(raw_handles, list) or not raw_handles:
        if config.prefill_direct_buffer_service_url:
            raw_handles = await _get_or_create_direct_feature_handles(
                app=app,
                req_data=req_data,
                target_worker_id=target_worker_id,
                feature_ids=list(getattr(ctx, "mm_hashes", []) or []),
            )
        else:
            raw_handles = await _request_feature_handles_from_encoder_service(
                app=app,
                req_data=req_data,
                target_worker_id=target_worker_id,
            )
    try:
        handles = [FeatureHandle.from_control_payload(dict(item)) for item in raw_handles]
        handle_payloads = [handle.as_control_payload() for handle in handles]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid feature handle payload: {exc}") from exc
    expected = len(list(getattr(ctx, "mm_hashes", []) or []))
    if expected and len(handles) != expected:
        raise HTTPException(
            status_code=400,
            detail=f"feature handle count mismatch: handles={len(handles)} multimodal_items={expected}",
        )
    for handle, image_hash in zip(handles, list(getattr(ctx, "mm_hashes", []) or [])):
        if handle.feature_id != image_hash and handle.metadata.get("source_mm_hash") != image_hash:
            raise HTTPException(
                status_code=400,
                detail=(
                    "feature handle does not match request multimodal hash: "
                    f"handle={handle.feature_id} request={image_hash}"
                ),
            )
    rewritten = dict(req_data)
    metadata = dict(rewritten.get("metadata") or {})
    metadata["mooncake_epd_feature_handles"] = handle_payloads
    metadata["mooncake_epd_feature_handle_target_worker"] = target_worker_id
    rewritten["metadata"] = metadata
    kv = dict(rewritten.get("kv_transfer_params") or {})
    kv["mm_prefetch_policy"] = "feature_handle"
    kv["mm_feature_handles"] = handle_payloads
    kv["mm_feature_handle_target_worker"] = target_worker_id
    rewritten["kv_transfer_params"] = kv
    return rewritten


async def _request_feature_handles_from_encoder_service(
    *,
    app: FastAPI,
    req_data: Dict[str, Any],
    target_worker_id: str,
) -> List[Dict[str, Any]]:
    config: ProxyConfig = app.state.proxy_config
    if not config.encoder_service_url:
        raise HTTPException(
            status_code=400,
            detail=(
                "feature_handle mode requires metadata.mooncake_epd_feature_handles "
                "or --encoder-service-url for online E-stage encoding"
            ),
        )
    payload = dict(req_data)
    metadata = dict(payload.get("metadata") or {})
    metadata["mooncake_epd_target_worker_id"] = target_worker_id
    payload["metadata"] = metadata
    if config.prefill_direct_buffer_service_url:
        return await _request_direct_feature_handles_from_encoder_service(
            app=app,
            payload=payload,
            target_worker_id=target_worker_id,
        )
    client: httpx.AsyncClient = app.state.encoder_client
    try:
        response = await client.post("/encode", json=payload)
        response.raise_for_status()
        encoded = response.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:1000] if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=f"encoder service returned error: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"encoder service request failed: {exc}") from exc
    handles = encoded.get("handles")
    if not isinstance(handles, list) or not handles:
        raise HTTPException(status_code=502, detail="encoder service returned no feature handles")
    return [dict(item) for item in handles]


async def _release_direct_feature_ids(app: FastAPI, feature_ids: Sequence[str]) -> None:
    ids = sorted(set(str(feature_id) for feature_id in feature_ids if feature_id))
    if not ids:
        return
    client: httpx.AsyncClient = app.state.prefill_direct_buffer_client
    response = await client.post("release", json={"feature_ids": ids})
    try:
        response.raise_for_status()
    finally:
        await response.aclose()


async def _get_or_create_direct_feature_handles(
    *,
    app: FastAPI,
    req_data: Dict[str, Any],
    target_worker_id: str,
    feature_ids: Sequence[str],
) -> List[Dict[str, Any]]:
    """Reuse Prefill-owned feature buffers and coalesce concurrent misses."""

    cache: _DirectFeatureHandleCache = app.state.direct_feature_handle_cache
    prefill_client = _client_for_worker(
        app.state.prefill_clients,
        str(target_worker_id),
    )
    if prefill_client is None:
        raise HTTPException(
            status_code=503,
            detail=f"no Prefill client for direct feature target {target_worker_id}",
        )
    await _fence_prefill_direct_handle_reuse(
        app=app,
        prefill_client=prefill_client,
    )
    normalized_ids = tuple(str(feature_id) for feature_id in feature_ids if feature_id)
    cached, expired = cache.get_many(
        target_worker_id=target_worker_id,
        feature_ids=normalized_ids,
    )
    if expired:
        await _release_direct_feature_ids(app, expired)
    if cached is not None:
        return cached

    if _request_has_compact_mm_uuid_references(req_data):
        _record_client_mm_uuid_cold_miss(app, "direct_feature_handle")
        raise HTTPException(
            status_code=409,
            detail=(
                "multimodal UUID reference missed the direct feature-handle cache; "
                "resend the full media payload to warm the EPD path"
            ),
        )

    if not cache.enabled or not normalized_ids:
        return await _request_direct_feature_handles_from_encoder_service(
            app=app,
            payload=req_data,
            target_worker_id=target_worker_id,
        )

    inflight: Dict[Tuple[str, str, Tuple[str, ...]], asyncio.Task] = (
        app.state.direct_feature_handle_inflight
    )
    expected_incarnation = cache.worker_incarnation(target_worker_id)
    flight_key = (
        str(target_worker_id),
        str(expected_incarnation or ""),
        normalized_ids,
    )
    task = inflight.get(flight_key)
    if task is None:
        async def _create() -> List[Dict[str, Any]]:
            handles = await _request_direct_feature_handles_from_encoder_service(
                app=app,
                payload=req_data,
                target_worker_id=target_worker_id,
            )
            try:
                incarnation_changed, evicted = cache.put_many(
                    target_worker_id=target_worker_id,
                    handles=handles,
                    expected_worker_incarnation=expected_incarnation,
                    enforce_expected_incarnation=True,
                )
            except RuntimeError as exc:
                await _release_retired_direct_feature_ids(
                    app,
                    [
                        str(dict(handle).get("feature_id") or "")
                        for handle in handles
                    ],
                )
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Prefill incarnation changed while direct feature handles "
                        "were being created; the stale result was discarded"
                    ),
                ) from exc
            observed_incarnation = cache.worker_incarnation(target_worker_id)
            if observed_incarnation is not None:
                prefill_client["remote_api_incarnation"] = observed_incarnation
            if incarnation_changed:
                _invalidate_prefill_cached_topology(prefill_client)
            if evicted:
                await _release_direct_feature_ids(app, evicted)
            return handles

        task = asyncio.create_task(
            _create(),
            name=f"epd-direct-feature-{target_worker_id}-{'-'.join(normalized_ids)}",
        )
        inflight[flight_key] = task

        def _release(done: asyncio.Task, *, key=flight_key) -> None:
            if inflight.get(key) is done:
                inflight.pop(key, None)

        task.add_done_callback(_release)
    return await asyncio.shield(task)


async def _request_direct_feature_handles_from_encoder_service(
    *,
    app: FastAPI,
    payload: Dict[str, Any],
    target_worker_id: str,
) -> List[Dict[str, Any]]:
    encoder_client: httpx.AsyncClient = app.state.encoder_client
    direct_client: httpx.AsyncClient = app.state.prefill_direct_buffer_client
    ticket = ""
    allocated_feature_ids: List[str] = []

    async def _cleanup_failed_handshake() -> List[str]:
        errors: List[str] = []
        if ticket:
            try:
                cleanup = await encoder_client.post(
                    "/discard_direct",
                    json={"ticket": ticket},
                )
                cleanup.raise_for_status()
            except Exception as exc:
                errors.append(f"encoder ticket cleanup failed: {exc}")
        if allocated_feature_ids:
            try:
                await _release_direct_feature_ids(app, allocated_feature_ids)
            except Exception as exc:
                errors.append(f"prefill allocation cleanup failed: {exc}")
        return errors

    try:
        described_resp = await encoder_client.post("/describe", json=payload)
        described_resp.raise_for_status()
        described = described_resp.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:1000] if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=f"encoder describe returned error: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"encoder describe request failed: {exc}") from exc

    descriptors = described.get("descriptors")
    ticket = str(described.get("ticket") or "")
    if not ticket or not isinstance(descriptors, list) or not descriptors:
        raise HTTPException(status_code=502, detail="encoder describe returned no direct descriptors/ticket")

    try:
        alloc_resp = await direct_client.post(
            "allocate",
            json={
                "descriptors": descriptors,
                "target_worker_id": target_worker_id,
                "zero_fill": False,
            },
        )
        alloc_resp.raise_for_status()
        allocation = alloc_resp.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:1000] if exc.response is not None else str(exc)
        cleanup_errors = await _cleanup_failed_handshake()
        if cleanup_errors:
            detail += f"; cleanup={cleanup_errors}"
        raise HTTPException(status_code=502, detail=f"prefill direct allocation returned error: {detail}") from exc
    except Exception as exc:
        cleanup_errors = await _cleanup_failed_handshake()
        raise HTTPException(
            status_code=502,
            detail=f"prefill direct allocation failed: {exc}; cleanup={cleanup_errors}",
        ) from exc

    targets = allocation.get("targets")
    if isinstance(targets, list):
        allocated_feature_ids.extend(
            str(dict(target).get("feature_id") or "")
            for target in targets
            if isinstance(target, dict) and str(target.get("feature_id") or "")
        )
    if not isinstance(targets, list) or len(targets) != len(descriptors):
        cleanup_errors = await _cleanup_failed_handshake()
        raise HTTPException(
            status_code=502,
            detail=(
                "prefill direct allocation target count mismatch: "
                f"targets={0 if not isinstance(targets, list) else len(targets)} "
                f"descriptors={len(descriptors)} cleanup={cleanup_errors}"
            ),
        )

    publish_payload = {
        "ticket": ticket,
        "metadata": dict(payload.get("metadata") or {}),
        "mooncake_epd_direct_feature_targets": targets,
    }
    try:
        publish_resp = await encoder_client.post("/publish_direct", json=publish_payload)
        publish_resp.raise_for_status()
        published = publish_resp.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:1000] if exc.response is not None else str(exc)
        cleanup_errors = await _cleanup_failed_handshake()
        if cleanup_errors:
            detail += f"; cleanup={cleanup_errors}"
        raise HTTPException(status_code=502, detail=f"encoder direct publish returned error: {detail}") from exc
    except Exception as exc:
        cleanup_errors = await _cleanup_failed_handshake()
        raise HTTPException(
            status_code=502,
            detail=f"encoder direct publish failed: {exc}; cleanup={cleanup_errors}",
        ) from exc
    handles = published.get("handles")
    if not isinstance(handles, list) or not handles:
        cleanup_errors = await _cleanup_failed_handshake()
        raise HTTPException(
            status_code=502,
            detail=(
                "encoder direct publish returned no feature handles; "
                f"cleanup={cleanup_errors}"
            ),
        )
    for handle in handles:
        if not str(dict(handle).get("uri") or "").startswith("epd-direct://"):
            cleanup_errors = await _cleanup_failed_handshake()
            raise HTTPException(
                status_code=502,
                detail=(
                    "encoder direct publish returned non-direct handle; "
                    f"cleanup={cleanup_errors}"
                ),
            )
    return [dict(item) for item in handles]


async def _release_direct_feature_buffers_after_prefill(app: FastAPI, req_data: Dict[str, Any]) -> None:
    """Release Prefill-owned E→P direct buffers once Prefill has consumed them."""

    config: ProxyConfig = app.state.proxy_config
    cache = getattr(app.state, "direct_feature_handle_cache", None)
    if cache is not None and cache.enabled:
        return
    if not config.release_direct_feature_buffers_after_prefill:
        return
    if not config.prefill_direct_buffer_service_url:
        return
    feature_ids: List[str] = []
    kv = dict(req_data.get("kv_transfer_params") or {})
    metadata = dict(req_data.get("metadata") or {})
    raw_handles = (
        kv.get("mm_feature_handles")
        or metadata.get("mooncake_epd_feature_handles")
        or metadata.get("feature_handles")
        or []
    )
    if not isinstance(raw_handles, list):
        return
    for item in raw_handles:
        if not isinstance(item, dict):
            continue
        if not str(item.get("uri") or "").startswith("epd-direct://"):
            continue
        fid = str(item.get("feature_id") or "")
        if fid:
            feature_ids.append(fid)
    if not feature_ids:
        return
    try:
        await _release_direct_feature_ids(app, feature_ids)
    except Exception as exc:
        # Release failure is operationally serious but the P→D handoff may have
        # already succeeded. Report through logs/metrics rather than corrupting
        # the user response after Prefill has completed.
        logger.error("failed to release direct feature buffers after prefill: %s", exc)


async def _prefetch_and_rewrite_multimodal_assets(
    *,
    app: FastAPI,
    req_data: Dict[str, Any],
    ctx,
    target_worker_id: str,
) -> Dict[str, Any]:
    """Prefetch multimodal assets into MMStore and rewrite URLs to data URLs.

    This is the serving-compatible E→P prefetch path for the OpenAI/vLLM API:
    the proxy owns multimodal source bytes, moves them through the real MMStore
    event queue/TransferEngine, and sends prefill/decode identical data URLs so
    vLLM avoids repeated remote image fetches while preserving request semantics.
    """
    config: ProxyConfig = app.state.proxy_config
    mm_store: Optional[MMStore] = getattr(app.state, "mm_store", None)
    if not config.enable_mm_prefetch or mm_store is None or not getattr(ctx, "mm_hashes", None):
        return req_data

    rewritten = _copy_request_for_mm_url_rewrite(req_data)
    items = list(_iter_mutable_mm_url_items(rewritten))
    if not items:
        return req_data

    attempted = completed = failed = worker_hits = shared_hits = recomputed = 0
    bytes_total = 0
    wait_start = time.perf_counter()
    hash_iter = iter(list(ctx.mm_hashes))

    for item in items:
        image_hash = next(hash_iter, None)
        if not image_hash:
            break
        url = _image_url_from_item(item)
        if not url:
            continue
        attempted += 1
        try:
            bundle, data_url, shared_hit = await _get_or_create_serving_mm_bundle(
                app=app,
                mm_store=mm_store,
                image_hash=image_hash,
                url=url,
                max_bytes=int(config.mm_prefetch_max_asset_bytes),
            )
            bytes_total += int(bundle.metadata.get("bytes", bundle.nbytes()) or 0)
            if shared_hit:
                shared_hits += 1
            handle = mm_store.prefetch(
                image_hash,
                target_worker_id=target_worker_id,
                target_device=torch.device("cpu"),
            )
            timeout_s = max(0.0, float(config.mm_prefetch_wait_ms)) / 1000.0
            if timeout_s > 0:
                await asyncio.to_thread(handle.wait, timeout_s)
            if handle.done.is_set() and handle.error is None:
                completed += 1
                worker_hits += int(bool(handle.worker_cache_hit))
                shared_hits += int(bool(handle.shared_store_hit and not shared_hit))
                recomputed += int(bool(handle.recomputed))
            _set_image_url_on_item(item, data_url)
        except Exception:
            failed += 1
            logger.exception("MM prefetch failed")
            if config.strict_no_fallback:
                raise HTTPException(
                    status_code=502,
                    detail="MM prefetch failed in strict-no-fallback mode",
                )
            logger.warning("preserving original multimodal URL after MM prefetch failure")

    wait_ms = (time.perf_counter() - wait_start) * 1000.0
    control_plane: ServingControlPlane = app.state.control_plane
    control_plane.record_mm_prefetch_result(
        ctx,
        attempted=attempted,
        completed=completed,
        failed=failed,
        worker_cache_hits=worker_hits,
        shared_store_hits=shared_hits,
        recomputed=recomputed,
        bytes_transferred=bytes_total,
        wait_ms=wait_ms if attempted else 0.0,
    )
    return rewritten


def _iter_mutable_mm_url_items(req_data: Dict[str, Any]):
    messages = req_data.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and _is_image_content_item(item):
                        yield item
    prompt = req_data.get("prompt")
    if isinstance(prompt, list):
        for item in prompt:
            if isinstance(item, dict) and _is_image_content_item(item):
                yield item


def _is_image_content_item(item: Dict[str, Any]) -> bool:
    return str(item.get("type", "")).strip().lower() in {
        "image",
        "image_url",
        "input_image",
    }


def _iter_multimodal_content_items(req_data: Dict[str, Any]):
    messages = req_data.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if not isinstance(content, list):
                continue
            for item in content:
                if not isinstance(item, dict):
                    continue
                if str(item.get("type", "")).strip().lower() in {
                    "image",
                    "image_url",
                    "input_image",
                    "audio",
                    "audio_url",
                    "input_audio",
                    "video",
                    "video_url",
                    "input_video",
                    "file",
                    "input_file",
                }:
                    yield item
    prompt = req_data.get("prompt")
    if isinstance(prompt, list):
        for item in prompt:
            if isinstance(item, dict) and str(
                item.get("type", "")
            ).strip().lower() in {
                "image",
                "image_url",
                "input_image",
                "audio",
                "audio_url",
                "input_audio",
                "video",
                "video_url",
                "input_video",
                "file",
                "input_file",
            }:
                yield item


def _media_payload_present(item: Dict[str, Any]) -> bool:
    for key in (
        "image_url",
        "image",
        "image_pil",
        "audio_url",
        "audio",
        "input_audio",
        "video_url",
        "video",
        "file",
        "file_data",
        "url",
    ):
        if key not in item:
            continue
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            if value:
                return True
            continue
        if isinstance(value, dict):
            if "url" in value:
                if value.get("url"):
                    return True
                continue
            if value:
                return True
            continue
        return True
    return False


def _client_mm_uuid_request_mode(
    req_data: Dict[str, Any],
) -> Tuple[str, int, int]:
    items = list(_iter_multimodal_content_items(req_data))
    if not items:
        return "none", 0, 0
    uuid_items = 0
    compact_items = 0
    for item in items:
        supplied_uuid = item.get("uuid")
        has_uuid = supplied_uuid is not None and bool(str(supplied_uuid).strip())
        has_payload = _media_payload_present(item)
        if has_uuid:
            uuid_items += 1
        if not has_payload:
            if not has_uuid:
                raise ValueError(
                    "multimodal item with an omitted payload requires a non-empty uuid"
                )
            compact_items += 1
    if compact_items == len(items):
        return "compact", uuid_items, compact_items
    if compact_items:
        return "mixed", uuid_items, compact_items
    if uuid_items:
        return "full", uuid_items, 0
    return "legacy-full", 0, 0


def _request_has_compact_mm_uuid_references(req_data: Dict[str, Any]) -> bool:
    try:
        _mode, _uuid_items, compact_items = _client_mm_uuid_request_mode(req_data)
    except ValueError:
        return False
    return compact_items > 0


def _record_client_mm_uuid_request(
    app: FastAPI,
    *,
    mode: str,
    uuid_items: int,
    compact_items: int,
    request_body_bytes: int,
) -> None:
    stats = app.state.client_mm_uuid_reference_stats
    counter = {
        "legacy-full": "legacy_full_requests",
        "full": "uuid_full_requests",
        "compact": "compact_requests",
        "mixed": "mixed_requests",
    }.get(str(mode))
    if counter is not None:
        stats[counter] = int(stats.get(counter, 0)) + 1
    stats["uuid_items"] = int(stats.get("uuid_items", 0)) + int(uuid_items)
    stats["compact_items"] = int(stats.get("compact_items", 0)) + int(
        compact_items
    )
    stats["request_body_bytes"] = int(stats.get("request_body_bytes", 0)) + int(
        request_body_bytes
    )
    if compact_items:
        stats["compact_request_body_bytes"] = int(
            stats.get("compact_request_body_bytes", 0)
        ) + int(request_body_bytes)


def _record_client_mm_uuid_cold_miss(app: FastAPI, stage: str) -> None:
    stats = app.state.client_mm_uuid_reference_stats
    stats["cold_misses"] = int(stats.get("cold_misses", 0)) + 1
    by_stage = stats.setdefault("cold_misses_by_stage", {})
    key = str(stage)
    by_stage[key] = int(by_stage.get(key, 0)) + 1


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


def _set_image_url_on_item(item: Dict[str, Any], data_url: str) -> None:
    if isinstance(item.get("image_url"), dict):
        updated = dict(item["image_url"])
        updated["url"] = data_url
        item["image_url"] = updated
    elif "image_url" in item:
        item["image_url"] = data_url
    else:
        item["url"] = data_url


def _set_canonical_mm_cache_url(item: Dict[str, Any], cache_url: str) -> None:
    """Normalize full and UUID-only image parts to one render-cache shape."""

    if "image_url" in item:
        current = item.get("image_url")
        extras = (
            {key: value for key, value in current.items() if key != "url"}
            if isinstance(current, dict)
            else {}
        )
        item["image_url"] = {"url": cache_url, **extras}
        return
    item["url"] = cache_url


async def _get_or_create_serving_mm_bundle(
    *,
    app: FastAPI,
    mm_store: MMStore,
    image_hash: str,
    url: str,
    max_bytes: int,
) -> Tuple[FeatureBundle, str, bool]:
    cached = mm_store.shared_store.get(image_hash)
    if cached is not None:
        data_url = str(cached.metadata.get("data_url") or "")
        if data_url:
            return cached, data_url, True
    payload, content_type = await _load_mm_url_bytes(app, url, max_bytes=max_bytes)
    data_url = _bytes_to_data_url(payload, content_type)
    tensor = torch.frombuffer(bytearray(payload), dtype=torch.uint8).clone()
    bundle = FeatureBundle(
        image_hash=image_hash,
        last_hidden=tensor,
        intermediates=[],
        grid_thw=None,
        metadata={
            "kind": "serving_mm_asset_bytes",
            "content_type": content_type,
            "bytes": len(payload),
            "source_url_sha256": _stable_text_hash(url),
            "data_url": data_url,
        },
    )
    mm_store.publish(bundle)
    return bundle, data_url, False


async def _validate_remote_mm_url(url: str, *, allow_private: bool) -> None:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"unsupported multimodal URL: {url[:64]}")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("multimodal URL credentials are not allowed")
    if allow_private:
        return

    hostname = str(parsed.hostname).strip().rstrip(".")
    try:
        addresses = [ipaddress.ip_address(hostname)]
    except ValueError:
        try:
            resolved = await asyncio.to_thread(
                socket.getaddrinfo,
                hostname,
                parsed.port or (443 if parsed.scheme == "https" else 80),
                type=socket.SOCK_STREAM,
            )
        except socket.gaierror as exc:
            raise ValueError(
                f"multimodal URL hostname could not be resolved: {hostname}"
            ) from exc
        addresses = []
        for item in resolved:
            address = ipaddress.ip_address(item[4][0])
            if address not in addresses:
                addresses.append(address)
    if not addresses or any(not address.is_global for address in addresses):
        raise ValueError(
            "multimodal URL resolves to a non-public address; "
            "use --allow-private-mm-urls only in a trusted deployment"
        )


async def _load_mm_url_bytes(
    app: FastAPI,
    url: str,
    *,
    max_bytes: int,
) -> Tuple[bytes, str]:
    if url.startswith("data:"):
        return _parse_data_url(url, max_bytes=max_bytes)
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError(f"unsupported multimodal URL scheme: {url[:32]}")
    if max_bytes < 1:
        raise ValueError("multimodal asset byte limit must be positive")
    client: httpx.AsyncClient = app.state.mm_fetch_client
    current_url = url
    for _ in range(4):
        await _validate_remote_mm_url(
            current_url,
            allow_private=bool(app.state.proxy_config.allow_private_mm_urls),
        )
        async with client.stream("GET", current_url) as response:
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                if not location:
                    raise ValueError("multimodal URL redirect is missing Location")
                current_url = urljoin(current_url, location)
                continue
            response.raise_for_status()
            content_length = response.headers.get("content-length")
            if content_length is not None:
                try:
                    if int(content_length) > max_bytes:
                        raise ValueError(
                            "multimodal asset Content-Length exceeds limit: "
                            f"{content_length} > {max_bytes}"
                        )
                except ValueError as exc:
                    if "exceeds limit" in str(exc):
                        raise
            chunks: List[bytes] = []
            total = 0
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > max_bytes:
                    raise ValueError(
                        f"multimodal asset too large: {total} > {max_bytes}"
                    )
                chunks.append(chunk)
            content_type = response.headers.get(
                "content-type",
                "application/octet-stream",
            ).split(";")[0].strip()
            return b"".join(chunks), content_type or "application/octet-stream"
    raise ValueError("multimodal URL exceeded the redirect limit")


def _parse_data_url(url: str, *, max_bytes: int) -> Tuple[bytes, str]:
    header, sep, data = url.partition(",")
    if sep != "," or not header.startswith("data:"):
        raise ValueError("invalid data URL")
    meta = header[5:]
    parts = [part for part in meta.split(";") if part]
    content_type = parts[0] if parts and "/" in parts[0] else "application/octet-stream"
    if "base64" in parts:
        payload = base64.b64decode(data, validate=True)
    else:
        from urllib.parse import unquote_to_bytes
        payload = unquote_to_bytes(data)
    if len(payload) > max_bytes:
        raise ValueError(f"multimodal asset too large: {len(payload)} > {max_bytes}")
    return payload, content_type


def _bytes_to_data_url(payload: bytes, content_type: str) -> str:
    encoded = base64.b64encode(payload).decode("ascii")
    return f"data:{content_type or 'application/octet-stream'};base64,{encoded}"


def _stable_text_hash(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _peek_lowest_load_worker(control_plane: ServingControlPlane, stage: str):
    workers = control_plane.stage_workers(stage)
    if not workers:
        return None
    return min(workers, key=lambda w: (w.current_load + w.queue_size, w.avg_latency_ms))


def _merge_control_headers(req_data: Dict[str, Any], request: Request) -> Dict[str, Any]:
    """Copy stable Agent scheduling hints from HTTP headers into metadata.

    This keeps the public OpenAI request body compatible while letting real
    Agent gateways express THINKING / INTERACTIVE / HYBRID, priority and SLO.
    Body metadata wins over headers.
    """

    header_map = {
        "X-Agent-Type": "agent_type",
        "X-Agent-Priority": "priority",
        "X-Agent-Deadline-Ms": "deadline_ms",
        "X-Agent-SLO-Ms": "slo_ms",
        "X-Workflow-Id": "workflow_id",
    }
    updates: Dict[str, Any] = {}
    for header, key in header_map.items():
        value = request.headers.get(header)
        if value is not None and str(value).strip():
            updates[key] = value
    if not updates:
        return req_data
    merged = dict(req_data)
    metadata = dict(merged.get("metadata") or {})
    for key, value in updates.items():
        metadata.setdefault(key, value)
    merged["metadata"] = metadata
    return merged


def _admit_or_raise(control_plane: ServingControlPlane, stage: str, ctx) -> Any:
    try:
        decision = control_plane.admit_stage(stage, ctx)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if decision.decision.action is AdmissionAction.REJECT:
        raise HTTPException(status_code=503, detail=decision.decision.reason)
    return decision


def _client_for_worker(clients: Sequence[Dict[str, Any]], worker_id: str) -> Optional[Dict[str, Any]]:
    for client in clients:
        if client.get("worker_id") == worker_id:
            return client
    return None


def _normalize_remote_kv_topology(
    kv_transfer_params: Any,
) -> Optional[Dict[str, Any]]:
    if not isinstance(kv_transfer_params, dict):
        return None
    remote_engine_id = str(kv_transfer_params.get("remote_engine_id") or "").strip()
    remote_bootstrap_addr = str(
        kv_transfer_params.get("remote_bootstrap_addr") or ""
    ).strip().rstrip("/")
    try:
        tp_size = int(kv_transfer_params.get("tp_size"))
    except (TypeError, ValueError):
        return None
    if not remote_engine_id or not remote_bootstrap_addr or tp_size < 1:
        return None
    topology = {
        "remote_engine_id": remote_engine_id,
        "remote_bootstrap_addr": remote_bootstrap_addr,
        "tp_size": tp_size,
    }
    remote_engine_incarnation = str(
        kv_transfer_params.get("remote_engine_incarnation") or ""
    ).strip()
    if remote_engine_incarnation:
        topology["remote_engine_incarnation"] = remote_engine_incarnation
    return topology


def _cached_prefill_kv_topology(
    prefill_client: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    current_api_incarnation = str(
        prefill_client.get("remote_api_incarnation") or ""
    )
    topology_api_incarnation = str(
        prefill_client.get("remote_kv_topology_api_incarnation") or ""
    )
    if (
        current_api_incarnation
        and topology_api_incarnation != current_api_incarnation
    ):
        _invalidate_prefill_cached_topology(prefill_client)
        return None
    topology = _normalize_remote_kv_topology(
        prefill_client.get("remote_kv_topology")
    )
    if topology is None:
        _invalidate_prefill_cached_topology(prefill_client)
        return None
    return topology


def _cache_prefill_kv_topology(
    prefill_client: Dict[str, Any],
    kv_transfer_params: Any,
) -> Optional[Dict[str, Any]]:
    topology = _normalize_remote_kv_topology(kv_transfer_params)
    if topology is None:
        _invalidate_prefill_cached_topology(prefill_client)
        return None
    # Replace the whole mapping atomically so concurrent request coroutines
    # never observe a partially refreshed engine topology.
    prefill_client["remote_kv_topology"] = topology
    current_api_incarnation = str(
        prefill_client.get("remote_api_incarnation") or ""
    )
    if current_api_incarnation:
        prefill_client["remote_kv_topology_api_incarnation"] = (
            current_api_incarnation
        )
    else:
        prefill_client.pop("remote_kv_topology_api_incarnation", None)
    return dict(topology)


def _build_pipelined_decode_kv_params(
    *,
    prefill_kv_params: Dict[str, Any],
    topology: Dict[str, Any],
    prefill_worker_id: str,
    decode_decision: Any,
    handoff_id: str,
) -> Dict[str, Any]:
    transfer_id = str(prefill_kv_params.get("transfer_id") or "").strip()
    if not transfer_id:
        raise RuntimeError("early Decode pipeline requires a stable transfer_id")
    normalized_topology = _normalize_remote_kv_topology(topology)
    if normalized_topology is None:
        raise RuntimeError("early Decode pipeline requires a complete cached KV topology")
    handoff_id = str(handoff_id or "").strip()
    if not handoff_id:
        raise RuntimeError("early Decode pipeline requires a reserved handoff_id")

    decision = decode_decision.decision
    kv = dict(prefill_kv_params)
    kv.pop("remote_block_ids", None)
    kv.update(normalized_topology)
    kv.update(
        {
            "transfer_id": transfer_id,
            "do_remote_prefill": True,
            "do_remote_decode": False,
            "control_stage": "decode",
            "control_worker_id": decode_decision.worker_id,
            "scheduler_rho": decision.rho,
            "admission_action": decision.action.value,
            "degrade_level": decision.degrade_level.value,
            "handoff_id": handoff_id,
            "a2a_source_node": prefill_worker_id,
            "a2a_target_node": decode_decision.worker_id,
        }
    )
    return kv


def _validate_pipelined_decode_kv_params(
    provisional_params: Dict[str, Any],
    finalized_params: Dict[str, Any],
) -> None:
    provisional_topology = _normalize_remote_kv_topology(provisional_params)
    finalized_topology = _normalize_remote_kv_topology(finalized_params)
    if provisional_topology is None or finalized_topology != provisional_topology:
        raise RuntimeError(
            "Prefill KV topology changed while the early Decode request was in flight"
        )
    provisional_transfer_id = str(provisional_params.get("transfer_id") or "")
    finalized_transfer_id = str(finalized_params.get("transfer_id") or "")
    if not provisional_transfer_id or finalized_transfer_id != provisional_transfer_id:
        raise RuntimeError(
            "Prefill transfer_id changed while the early Decode request was in flight"
        )
    if not finalized_params.get("do_remote_prefill") or finalized_params.get(
        "do_remote_decode"
    ):
        raise RuntimeError("finalized Decode KV direction is incompatible with the pipeline")
    provisional_handoff_id = str(provisional_params.get("handoff_id") or "")
    finalized_handoff_id = str(finalized_params.get("handoff_id") or "")
    if not provisional_handoff_id or finalized_handoff_id != provisional_handoff_id:
        raise RuntimeError(
            "Prefill handoff_id changed while the early Decode request was in flight"
        )


def _should_use_prompt_only_prefill(api: str) -> bool:
    return api.endswith("/chat/completions") or api.endswith("/completions")


def _render_api_for(api: str) -> str:
    if api.endswith("/chat/completions"):
        return "/v1/chat/completions/render"
    if api.endswith("/completions"):
        return "/v1/completions/render"
    raise ValueError(f"unsupported render api for {api}")


def _normalize_rendered_prefill_payload(api: str, render_payload: Any) -> Dict[str, Any]:
    if api.endswith("/chat/completions"):
        if not isinstance(render_payload, dict):
            raise TypeError("chat render response must be an object")
        return dict(render_payload)
    if not isinstance(render_payload, list) or len(render_payload) != 1:
        raise ValueError("completion render response must contain exactly one prompt")
    first = render_payload[0]
    if not isinstance(first, dict):
        raise TypeError("completion render response item must be an object")
    return dict(first)


def _inject_prefill_kv_into_rendered_request(
    rendered_request: Dict[str, Any],
    *,
    request_id: str,
    kv_transfer_params: Dict[str, Any],
) -> Dict[str, Any]:
    payload = dict(rendered_request)
    sampling_params = dict(payload.get("sampling_params") or {})
    extra_args = dict(sampling_params.get("extra_args") or {})
    extra_args["kv_transfer_params"] = dict(kv_transfer_params)
    sampling_params["extra_args"] = extra_args
    sampling_params["max_tokens"] = 0
    sampling_params["min_tokens"] = 0
    payload["sampling_params"] = sampling_params
    payload["request_id"] = request_id
    payload["stream"] = False
    payload.pop("stream_options", None)
    payload["kv_transfer_params"] = dict(kv_transfer_params)
    return payload


def _prerendered_decode_bypass_reason(
    *,
    api: str,
    request_body: Dict[str, Any],
    tokenizer_obj: Any,
) -> Optional[str]:
    """Return why the token-only Decode adapter cannot preserve semantics.

    The internal Generate protocol is intentionally selected only for the
    common, high-throughput single-choice chat path.  Requests whose public
    response needs server-side tool/reasoning/logprob parsing remain on the
    OpenAI endpoint rather than approximating their schema in the proxy.
    """

    if api != "/v1/chat/completions":
        return "unsupported_api"
    if tokenizer_obj is None:
        return "tokenizer_unavailable"
    try:
        if int(request_body.get("n", 1) or 1) != 1:
            return "multiple_choices"
    except (TypeError, ValueError):
        return "invalid_choice_count"
    if request_body.get("tools"):
        return "tool_response_parsing"
    tool_choice = request_body.get("tool_choice")
    if tool_choice not in (None, "none"):
        return "tool_response_parsing"
    if request_body.get("logprobs") or request_body.get("top_logprobs") is not None:
        return "logprobs_response"
    if request_body.get("prompt_logprobs") is not None:
        return "prompt_logprobs_response"
    if request_body.get("return_token_ids") or request_body.get(
        "return_tokens_as_token_ids"
    ):
        return "token_id_response"
    if request_body.get("echo"):
        return "echo_response"
    return None


def _record_prerendered_decode_selection(
    app: FastAPI,
    *,
    selected: bool,
    stream: bool,
    bypass_reason: Optional[str] = None,
) -> None:
    stats = app.state.prerendered_decode_stats
    if selected:
        stats["selected"] = int(stats.get("selected", 0)) + 1
        key = "streaming" if stream else "non_streaming"
        stats[key] = int(stats.get(key, 0)) + 1
        return
    stats["bypassed"] = int(stats.get("bypassed", 0)) + 1
    reason = str(bypass_reason or "unknown")
    reasons = stats.setdefault("bypass_reasons", {})
    reasons[reason] = int(reasons.get(reason, 0)) + 1


def _build_prerendered_decode_context(
    rendered_request: Dict[str, Any],
    *,
    tokenizer_obj: Any,
) -> _PrerenderedDecodeContext:
    token_ids = rendered_request.get("token_ids")
    if not isinstance(token_ids, list) or not token_ids:
        raise ValueError("rendered Decode request must contain non-empty token_ids")
    if any(not isinstance(token_id, int) for token_id in token_ids):
        raise TypeError("rendered Decode token_ids must be integers")
    sampling_params = dict(rendered_request.get("sampling_params") or {})
    model = str(rendered_request.get("model") or "").strip()
    if not model:
        raise ValueError("rendered Decode request must contain a model")
    return _PrerenderedDecodeContext(
        tokenizer=tokenizer_obj,
        prompt_token_ids=tuple(token_ids),
        model=model,
        skip_special_tokens=bool(sampling_params.get("skip_special_tokens", True)),
    )


def _inject_decode_kv_into_rendered_request(
    rendered_request: Dict[str, Any],
    *,
    request_id: str,
    request_body: Dict[str, Any],
    kv_transfer_params: Dict[str, Any],
) -> Dict[str, Any]:
    """Clone reusable render semantics and attach per-request Decode state."""

    payload = dict(rendered_request)
    sampling_params = dict(payload.get("sampling_params") or {})
    extra_args = dict(sampling_params.get("extra_args") or {})
    extra_args["kv_transfer_params"] = dict(kv_transfer_params)
    sampling_params["extra_args"] = extra_args
    payload["sampling_params"] = sampling_params
    payload["request_id"] = request_id
    payload["stream"] = bool(request_body.get("stream"))
    if payload["stream"] and request_body.get("stream_options") is not None:
        payload["stream_options"] = dict(request_body.get("stream_options") or {})
    else:
        payload.pop("stream_options", None)
    payload["kv_transfer_params"] = dict(kv_transfer_params)
    return payload


async def _dispatch_prompt_only_prefill(
    *,
    api: str,
    prefill_client: Dict[str, Any],
    prefill_headers: Dict[str, str],
    request_id: str,
    request_body: Dict[str, Any],
    kv_transfer_params: Dict[str, Any],
) -> Dict[str, Any]:
    rendered_payload = await _render_prompt_only_prefill(
        api=api,
        prefill_client=prefill_client,
        prefill_headers=prefill_headers,
        request_body=request_body,
    )
    return await _dispatch_rendered_prompt_only_prefill(
        prefill_client=prefill_client,
        prefill_headers=prefill_headers,
        request_id=request_id,
        rendered_payload=rendered_payload,
        kv_transfer_params=kv_transfer_params,
    )


def _prefill_render_cache_digest(
    request_body: Dict[str, Any],
    *,
    mm_hashes: Sequence[str],
) -> str:
    """Hash render semantics without re-hashing multi-megabyte image URLs.

    Workflow/request IDs and KV handoff topology do not alter tokenization or
    multimodal preprocessing.  Image URLs are replaced by the scheduler's
    already-computed content hashes; all prompt/tool/template-affecting fields
    remain in the canonical payload.
    """

    normalized = _copy_request_for_mm_url_rewrite(request_body)
    normalized.pop("stream", None)
    normalized.pop("stream_options", None)
    metadata = dict(normalized.get("metadata") or {})
    for key in (
        "workflow_id",
        "request_id",
        "trace_id",
        "span_id",
        "mooncake_epd_target_worker_id",
        "mooncake_epd_decode_worker_id",
    ):
        metadata.pop(key, None)
    if metadata:
        normalized["metadata"] = metadata
    else:
        normalized.pop("metadata", None)
    kv = dict(normalized.get("kv_transfer_params") or {})
    for key in (
        "transfer_id",
        "remote_engine_id",
        "remote_bootstrap_addr",
        "remote_block_ids",
        "do_remote_prefill",
        "do_remote_decode",
    ):
        kv.pop(key, None)
    if kv:
        normalized["kv_transfer_params"] = kv
    else:
        normalized.pop("kv_transfer_params", None)
    for index, item in enumerate(_iter_mutable_mm_url_items(normalized)):
        if index < len(mm_hashes):
            _set_canonical_mm_cache_url(
                item,
                f"mm-hash://{mm_hashes[index]}",
            )
    canonical = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


async def _get_or_render_prompt_only_prefill(
    *,
    app: FastAPI,
    api: str,
    prefill_client: Dict[str, Any],
    prefill_headers: Dict[str, str],
    request_body: Dict[str, Any],
    mm_hashes: Sequence[str],
) -> Dict[str, Any]:
    cache: _PrefillRenderCache = app.state.prefill_render_cache
    digest = _prefill_render_cache_digest(request_body, mm_hashes=mm_hashes)
    worker_id = str(prefill_client.get("worker_id") or "")
    worker_incarnation = str(
        app.state.direct_feature_handle_cache.worker_incarnation(worker_id) or ""
    )
    key = (worker_id, worker_incarnation, str(api), digest)
    cached = cache.get(key)
    if cached is not None:
        return cached
    if _request_has_compact_mm_uuid_references(request_body):
        _record_client_mm_uuid_cold_miss(app, "prefill_render")
        raise HTTPException(
            status_code=409,
            detail=(
                "multimodal UUID reference missed the Prefill render cache; "
                "resend the full media payload to warm the EPD path"
            ),
        )
    if not cache.enabled:
        return await _render_prompt_only_prefill(
            api=api,
            prefill_client=prefill_client,
            prefill_headers=prefill_headers,
            request_body=request_body,
        )

    inflight: Dict[Tuple[str, str, str, str], asyncio.Task] = (
        app.state.prefill_render_inflight
    )
    task = inflight.get(key)
    if task is None:
        async def _create() -> Dict[str, Any]:
            payload = await _render_prompt_only_prefill(
                api=api,
                prefill_client=prefill_client,
                prefill_headers=prefill_headers,
                request_body=request_body,
            )
            cache.put(key, payload)
            return payload

        task = asyncio.create_task(
            _create(),
            name=(
                f"epd-prefill-render-{key[0]}-"
                f"{hashlib.sha256(key[1].encode()).hexdigest()[:8]}-{digest[:12]}"
            ),
        )
        inflight[key] = task

        def _release(done: asyncio.Task, *, flight_key=key) -> None:
            if inflight.get(flight_key) is done:
                inflight.pop(flight_key, None)

        task.add_done_callback(_release)
    return dict(await asyncio.shield(task))


async def _render_prompt_only_prefill(
    *,
    api: str,
    prefill_client: Dict[str, Any],
    prefill_headers: Dict[str, str],
    request_body: Dict[str, Any],
) -> Dict[str, Any]:
    render_response = await prefill_client["client"].post(
        _render_api_for(api),
        json=request_body,
        headers=prefill_headers,
    )
    try:
        render_response.raise_for_status()
        rendered_payload = _normalize_rendered_prefill_payload(
            api,
            render_response.json(),
        )
    finally:
        await render_response.aclose()
    return rendered_payload


async def _dispatch_rendered_prompt_only_prefill(
    *,
    prefill_client: Dict[str, Any],
    prefill_headers: Dict[str, str],
    request_id: str,
    rendered_payload: Dict[str, Any],
    kv_transfer_params: Dict[str, Any],
) -> Dict[str, Any]:
    prefill_payload = _inject_prefill_kv_into_rendered_request(
        rendered_payload,
        request_id=request_id,
        kv_transfer_params=kv_transfer_params,
    )
    generate_response = await prefill_client["client"].post(
        "/inference/v1/generate",
        json=prefill_payload,
        headers=prefill_headers,
    )
    try:
        generate_response.raise_for_status()
        payload = dict(generate_response.json())
    finally:
        await generate_response.aclose()
    payload.setdefault("kv_transfer_params", dict(kv_transfer_params))
    return payload


def _forward_headers(request: Request, request_id: str) -> Dict[str, str]:
    auth = request.headers.get("Authorization") or f"Bearer {os.environ.get('OPENAI_API_KEY', 'sk-local')}"
    headers = {
        "Authorization": auth,
        "X-Request-Id": request_id,
    }
    workflow_id = request.headers.get("X-Workflow-Id")
    if workflow_id:
        headers["X-Workflow-Id"] = workflow_id
    return headers


def _prefill_request_headers(
    *,
    app: FastAPI,
    request: Request,
    request_id: str,
    prefill_client: Dict[str, Any],
) -> Dict[str, str]:
    headers = _forward_headers(request, request_id)
    if not app.state.proxy_config.enable_prefill_incarnation_guard:
        return headers
    cache: _DirectFeatureHandleCache = app.state.direct_feature_handle_cache
    expected_incarnation = _bounded_epoch_token(
        cache.worker_incarnation(str(prefill_client.get("worker_id") or ""))
    )
    if expected_incarnation is None:
        return headers
    headers[VLLM_EXPECTED_INCARNATION_HEADER] = expected_incarnation
    cache.record_incarnation_guarded_request()
    return headers


def _decode_request_headers(
    *,
    app: FastAPI,
    request: Request,
    request_id: str,
    mm_hash_lease: Optional[_DecodeMMHashLease],
) -> Dict[str, str]:
    headers = _forward_headers(request, request_id)
    if (
        mm_hash_lease is None
        or mm_hash_lease.mode != "hash-only"
        or not app.state.proxy_config.enable_decode_mm_hash_epoch_guard
    ):
        return headers
    expected_epoch = _bounded_epoch_token(mm_hash_lease.expected_worker_epoch)
    if expected_epoch is None:
        raise RuntimeError(
            "hash-only Decode epoch guard requires a confirmed worker incarnation"
        )
    headers[VLLM_EXPECTED_INCARNATION_HEADER] = expected_epoch
    if not mm_hash_lease.epoch_guard_attached:
        mm_hash_lease.cache.record_epoch_guarded_hash_only_request()
        mm_hash_lease.epoch_guard_attached = True
    return headers


def _is_decode_epoch_guard_rejection(exc: BaseException) -> bool:
    response = getattr(exc, "response", None)
    if response is None or int(getattr(response, "status_code", 0) or 0) != 409:
        return False
    return str(
        getattr(response, "headers", {}).get(
            VLLM_INCARNATION_MISMATCH_HEADER,
            "",
        )
    ) == "1"


def _decode_epoch_guard_rejection_epoch(
    exc: BaseException,
) -> Optional[str]:
    """Return the authoritative Decode incarnation carried by a guard 409.

    A guarded hash-only request is intentionally failed rather than retried.
    The middleware response still provides a safe control-plane observation:
    learning it here prevents a full rewarm from being followed by another
    request carrying an already-known stale epoch.
    """

    if not _is_decode_epoch_guard_rejection(exc):
        return None
    response = getattr(exc, "response", None)
    return _bounded_epoch_token(
        getattr(response, "headers", {}).get(VLLM_INCARNATION_HEADER)
    )


def _extract_prefill_continuation(api: str, payload: Dict[str, Any]) -> _PrefillContinuation:
    usage = dict(payload.get("usage") or {})
    choices = list(payload.get("choices") or [])
    choice = dict(choices[0] or {}) if choices else {}
    text = _extract_choice_text(api, choice)
    finish_reason = choice.get("finish_reason")
    completion_tokens = int(usage.get("completion_tokens", payload.get("completion_tokens", 0)) or 0)
    prompt_tokens = usage.get("prompt_tokens")
    total_tokens = usage.get("total_tokens")
    return _PrefillContinuation(
        text=text,
        completion_tokens=completion_tokens,
        prompt_tokens=int(prompt_tokens) if prompt_tokens is not None else None,
        total_tokens=int(total_tokens) if total_tokens is not None else None,
        finish_reason=str(finish_reason) if finish_reason is not None else None,
    )


def _requested_completion_budget(req_data: Dict[str, Any]) -> Optional[int]:
    for field_name in ("max_completion_tokens", "max_tokens"):
        value = req_data.get(field_name)
        if value is None:
            continue
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return None
    return None


def _should_short_circuit_after_prefill(
    req_data: Dict[str, Any],
    continuation: _PrefillContinuation,
) -> bool:
    requested_budget = _requested_completion_budget(req_data)
    if continuation.finish_reason and continuation.finish_reason != "length":
        return True
    if requested_budget is not None and requested_budget <= max(0, continuation.completion_tokens):
        return True
    return False


def _build_prefill_terminal_response(
    *,
    api: str,
    prefill_json: Dict[str, Any],
    request_id: str,
    routing_path: str,
    admission_action: str,
    degrade_level: str,
) -> Response:
    headers = {
        "X-Request-Id": request_id,
        "X-EPD-Routing-Path": routing_path,
        "X-EPD-Admission": admission_action,
        "X-EPD-Degrade-Level": degrade_level,
    }
    return JSONResponse(prefill_json, headers=headers)


def _apply_prefill_continuation_to_decode_payload(
    *,
    api: str,
    decode_payload: Dict[str, Any],
    continuation: _PrefillContinuation,
) -> Dict[str, Any]:
    if not continuation.active:
        return decode_payload
    requested_budget = _requested_completion_budget(decode_payload)
    if requested_budget is not None:
        remaining_budget = max(1, requested_budget - continuation.completion_tokens)
        if "max_tokens" in decode_payload:
            decode_payload["max_tokens"] = remaining_budget
        if "max_completion_tokens" in decode_payload:
            decode_payload["max_completion_tokens"] = remaining_budget
    if api.endswith("/chat/completions"):
        decode_payload["messages"] = _append_chat_assistant_prefix(
            decode_payload.get("messages"),
            continuation.text,
        )
        decode_payload["continue_final_message"] = True
        decode_payload["add_generation_prompt"] = False
        return decode_payload
    if api.endswith("/completions"):
        prompt = decode_payload.get("prompt")
        decode_payload["prompt"] = _append_completion_prefix(prompt, continuation.text)
    return decode_payload


def _append_chat_assistant_prefix(messages: Any, prefix_text: str) -> List[Dict[str, Any]]:
    existing_messages = [dict(message or {}) for message in list(messages or [])]
    if not existing_messages:
        return [{"role": "assistant", "content": prefix_text}]
    last = dict(existing_messages[-1] or {})
    if str(last.get("role", "")).strip().lower() == "assistant":
        last["content"] = _append_textual_content(last.get("content"), prefix_text)
        existing_messages[-1] = last
        return existing_messages
    existing_messages.append({"role": "assistant", "content": prefix_text})
    return existing_messages


def _append_completion_prefix(prompt: Any, prefix_text: str) -> Any:
    if isinstance(prompt, str):
        return prompt + prefix_text
    if isinstance(prompt, list):
        if not prompt:
            return [prefix_text]
        updated = list(prompt)
        last = updated[-1]
        if isinstance(last, str):
            updated[-1] = last + prefix_text
            return updated
    return prompt


def _append_textual_content(content: Any, prefix_text: str) -> Any:
    if isinstance(content, str):
        return content + prefix_text
    if isinstance(content, list):
        appended = False
        updated: List[Any] = []
        for item in content:
            if (
                not appended
                and isinstance(item, dict)
                and str(item.get("type", "")).strip().lower() == "text"
            ):
                merged = dict(item)
                merged["text"] = str(merged.get("text", "")) + prefix_text
                updated.append(merged)
                appended = True
            else:
                updated.append(item)
        if not appended:
            updated.append({"type": "text", "text": prefix_text})
        return updated
    return prefix_text


def _build_prefill_decode_semantic_hints(
    *,
    continuation: _PrefillContinuation,
    prefill_kv: Dict[str, Any],
) -> Dict[str, Any]:
    hints: Dict[str, Any] = {
        "remote_prefill_prompt_tokens": continuation.prompt_tokens,
        "remote_prefill_completion_tokens": continuation.completion_tokens,
        "remote_prefill_semantic_continuation": True,
    }
    remote_block_ids = prefill_kv.get("remote_block_ids")
    if isinstance(remote_block_ids, list):
        hints["remote_prefill_block_counts"] = [
            len(group) if isinstance(group, list) else 0 for group in remote_block_ids
        ]
    return hints


def _decode_prerendered_choice_text(
    choice: Dict[str, Any],
    *,
    context: _PrerenderedDecodeContext,
) -> str:
    token_ids = choice.get("token_ids")
    if not isinstance(token_ids, list):
        raise TypeError("prerendered Decode choice is missing token_ids")
    if any(not isinstance(token_id, int) for token_id in token_ids):
        raise TypeError("prerendered Decode choice token_ids must be integers")
    decoder = _IncrementalTokenDecoder(
        context.tokenizer,
        prompt_token_ids=context.prompt_token_ids,
        skip_special_tokens=context.skip_special_tokens,
    )
    return decoder.push(token_ids)


def _adapt_prerendered_non_stream_payload(
    payload: Dict[str, Any],
    *,
    request_id: str,
    context: _PrerenderedDecodeContext,
) -> Dict[str, Any]:
    choices = list(payload.get("choices") or [])
    if len(choices) != 1 or not isinstance(choices[0], dict):
        raise ValueError(
            "strict prerendered Decode requires exactly one response choice"
        )
    source_choice = dict(choices[0])
    text = _decode_prerendered_choice_text(source_choice, context=context)
    return {
        "id": f"chatcmpl-{request_id}",
        "object": "chat.completion",
        "created": int(payload.get("created") or time.time()),
        "model": str(payload.get("model") or context.model),
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "logprobs": None,
                "finish_reason": source_choice.get("finish_reason") or "stop",
                "stop_reason": None,
            }
        ],
        "usage": dict(payload.get("usage") or {}),
    }


async def _dispatch_non_streaming_decode(
    *,
    api: str,
    decode_api: str,
    control_plane: ServingControlPlane,
    ctx,
    decode_client: Dict[str, Any],
    decode_payload: Dict[str, Any],
    decode_headers: Dict[str, str],
    response_headers: Dict[str, str],
    continuation: _PrefillContinuation,
    prerendered_context: Optional[_PrerenderedDecodeContext] = None,
    mm_hash_lease: Optional[_DecodeMMHashLease] = None,
) -> Response:
    decode_start = time.monotonic()
    decode_response = None
    success = False
    try:
        decode_response = await decode_client["client"].post(
            decode_api,
            json=decode_payload,
            headers=decode_headers,
        )
        decode_response.raise_for_status()
        decode_json = decode_response.json()
        if prerendered_context is not None:
            patched_json = _adapt_prerendered_non_stream_payload(
                decode_json,
                request_id=ctx.request_id,
                context=prerendered_context,
            )
        else:
            patched_json = _patch_non_stream_payload(api, decode_json, continuation)
        decode_ready = time.monotonic()
        control_plane.record_stage_span(
            ctx,
            "decode_first_response",
            started_at=decode_start,
            ended_at=decode_ready,
        )
        control_plane.commit_handoff(ctx)
        control_plane.mark_first_token(ctx)
        if mm_hash_lease is not None:
            mm_hash_lease.succeed()
        success = True
        return JSONResponse(
            patched_json,
            status_code=decode_response.status_code,
            headers=response_headers,
        )
    except Exception as exc:
        if mm_hash_lease is not None:
            if _is_decode_epoch_guard_rejection(exc):
                mm_hash_lease.record_epoch_guard_rejection(
                    _decode_epoch_guard_rejection_epoch(exc)
                )
            mm_hash_lease.fail()
        logger.exception(
            "decode non-streaming dispatch failed request_id=%s worker_id=%s exc_type=%s",
            ctx.request_id,
            decode_client.get("worker_id"),
            type(exc).__name__,
        )
        control_plane.rollback_handoff(ctx)
        raise HTTPException(
            status_code=502,
            detail=f"decode request failed: {type(exc).__name__}: {exc}",
        ) from exc
    finally:
        if decode_response is not None:
            await decode_response.aclose()
        control_plane.mark_stage_complete(
            "decode",
            decode_client["worker_id"],
            latency_ms=(time.monotonic() - decode_start) * 1000.0,
            success=success,
        )
        control_plane.finish_request(ctx.request_id)


async def _open_decode_stream(
    *,
    api: str,
    decode_client: Dict[str, Any],
    decode_payload: Dict[str, Any],
    decode_headers: Dict[str, str],
) -> _OpenedDecodeStream:
    started_at = time.monotonic()
    stream_ctx = decode_client["client"].stream(
        "POST",
        api,
        json=decode_payload,
        headers=decode_headers,
    )
    decode_response = None
    stream_entered = False
    try:
        decode_response = await stream_ctx.__aenter__()
        stream_entered = True
        decode_response.raise_for_status()
        return _OpenedDecodeStream(
            stream_ctx=stream_ctx,
            response=decode_response,
            started_at=started_at,
            opened_at=time.monotonic(),
        )
    except BaseException as exc:
        if decode_response is not None:
            try:
                await decode_response.aclose()
            except Exception:
                logger.exception("failed to close decode response after startup error")
        if stream_entered:
            try:
                await stream_ctx.__aexit__(type(exc), exc, exc.__traceback__)
            except Exception:
                logger.exception("failed to close decode stream context after startup error")
        raise


async def _close_opened_decode_stream(opened: _OpenedDecodeStream) -> None:
    try:
        await opened.response.aclose()
    except Exception:
        logger.exception("failed to close opened decode response")
    try:
        await opened.stream_ctx.__aexit__(None, None, None)
    except Exception:
        logger.exception("failed to close opened decode stream context")


def _openai_chat_stream_packet(
    *,
    request_id: str,
    created: int,
    model: str,
    choice: Optional[Dict[str, Any]] = None,
    usage: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    packet: Dict[str, Any] = {
        "id": f"chatcmpl-{request_id}",
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [choice] if choice is not None else [],
    }
    if usage is not None:
        packet["usage"] = dict(usage)
        packet["system_fingerprint"] = "mooncake-epd-prerendered"
    return packet


def _encode_sse_packet(packet: Dict[str, Any]) -> bytes:
    return (
        f"data: {json.dumps(packet, ensure_ascii=False, separators=(',', ':'))}\n\n"
    ).encode("utf-8")


def _dispatch_prerendered_streaming_response(
    *,
    opened: _OpenedDecodeStream,
    control_plane: ServingControlPlane,
    ctx,
    decode_client: Dict[str, Any],
    response_headers: Dict[str, str],
    context: _PrerenderedDecodeContext,
    mm_hash_lease: Optional[_DecodeMMHashLease] = None,
) -> StreamingResponse:
    decode_response = opened.response
    decode_start = opened.started_at

    async def generate_stream():
        success = True
        handoff_committed = False
        first_token_seen = False
        role_emitted = False
        done_seen = False
        created = int(time.time())
        decoder = _IncrementalTokenDecoder(
            context.tokenizer,
            prompt_token_ids=context.prompt_token_ids,
            skip_special_tokens=context.skip_special_tokens,
        )
        try:
            async for raw_line in decode_response.aiter_lines():
                if raw_line is None or raw_line == "":
                    continue
                if not raw_line.startswith("data:"):
                    raise RuntimeError(
                        "strict prerendered Decode received a non-SSE response line"
                    )
                payload_text = raw_line[5:].strip()
                if payload_text == "[DONE]":
                    if not handoff_committed:
                        raise RuntimeError(
                            "strict prerendered Decode completed without a response packet"
                        )
                    done_seen = True
                    yield b"data: [DONE]\n\n"
                    continue
                try:
                    packet = json.loads(payload_text)
                except Exception as exc:
                    raise RuntimeError(
                        "strict prerendered Decode received malformed SSE JSON"
                    ) from exc
                if not isinstance(packet, dict):
                    raise TypeError(
                        "strict prerendered Decode SSE payload must be an object"
                    )
                if packet.get("error") is not None:
                    raise RuntimeError(
                        f"prerendered Decode generation error: {packet['error']}"
                    )
                usage = packet.get("usage")
                choices = list(packet.get("choices") or [])
                decoded_choices: List[Tuple[Dict[str, Any], str]] = []
                for source in choices:
                    if not isinstance(source, dict):
                        raise ValueError(
                            "strict prerendered Decode received an invalid choice"
                        )
                    try:
                        choice_index = int(source.get("index", 0))
                    except (TypeError, ValueError) as exc:
                        raise ValueError(
                            "strict prerendered Decode received an invalid choice index"
                        ) from exc
                    if choice_index != 0:
                        raise ValueError(
                            "strict prerendered Decode received an invalid choice"
                        )
                    token_ids = source.get("token_ids")
                    if not isinstance(token_ids, list) or any(
                        not isinstance(token_id, int) for token_id in token_ids
                    ):
                        raise TypeError(
                            "strict prerendered Decode stream choice is missing "
                            "integer token_ids"
                        )
                    decoded_choices.append((source, decoder.push(token_ids)))
                if not decoded_choices and not isinstance(usage, dict):
                    raise ValueError(
                        "strict prerendered Decode packet has neither choices nor usage"
                    )
                if not handoff_committed:
                    control_plane.commit_handoff(ctx)
                    handoff_committed = True
                if not role_emitted:
                    yield _encode_sse_packet(
                        _openai_chat_stream_packet(
                            request_id=ctx.request_id,
                            created=created,
                            model=context.model,
                            choice={
                                "index": 0,
                                "delta": {"role": "assistant", "content": ""},
                                "logprobs": None,
                                "finish_reason": None,
                            },
                        )
                    )
                    role_emitted = True

                for source, delta in decoded_choices:
                    if delta:
                        first_chunk_ready = time.monotonic()
                        if not first_token_seen:
                            control_plane.record_stage_span(
                                ctx,
                                "decode_first_chunk_wait",
                                started_at=decode_start,
                                ended_at=first_chunk_ready,
                            )
                            control_plane.mark_first_token(
                                ctx,
                                emitted_at=first_chunk_ready,
                            )
                            first_token_seen = True
                        yield _encode_sse_packet(
                            _openai_chat_stream_packet(
                                request_id=ctx.request_id,
                                created=created,
                                model=context.model,
                                choice={
                                    "index": 0,
                                    "delta": {"content": delta},
                                    "logprobs": None,
                                    "finish_reason": None,
                                },
                            )
                        )
                    finish_reason = source.get("finish_reason")
                    if finish_reason is not None:
                        yield _encode_sse_packet(
                            _openai_chat_stream_packet(
                                request_id=ctx.request_id,
                                created=created,
                                model=context.model,
                                choice={
                                    "index": 0,
                                    "delta": {"content": ""},
                                    "logprobs": None,
                                    "finish_reason": finish_reason,
                                    "stop_reason": None,
                                },
                            )
                        )
                if isinstance(usage, dict):
                    yield _encode_sse_packet(
                        _openai_chat_stream_packet(
                            request_id=ctx.request_id,
                            created=created,
                            model=context.model,
                            usage=usage,
                        )
                    )
            if not done_seen:
                raise RuntimeError(
                    "strict prerendered Decode stream closed without [DONE]"
                )
            if mm_hash_lease is not None:
                mm_hash_lease.succeed()
        except BaseException:
            success = False
            if mm_hash_lease is not None:
                mm_hash_lease.fail()
            if not handoff_committed:
                control_plane.rollback_handoff(ctx)
            raise
        finally:
            if success and not handoff_committed:
                success = False
                if mm_hash_lease is not None:
                    mm_hash_lease.fail()
                control_plane.rollback_handoff(ctx)
            await _close_opened_decode_stream(opened)
            control_plane.mark_stage_complete(
                "decode",
                decode_client["worker_id"],
                latency_ms=(time.monotonic() - decode_start) * 1000.0,
                success=success,
            )
            control_plane.finish_request(ctx.request_id)

    return StreamingResponse(
        generate_stream(),
        status_code=decode_response.status_code,
        headers=response_headers,
        media_type="text/event-stream",
    )


async def _dispatch_streaming_decode(
    *,
    api: str,
    decode_api: str,
    control_plane: ServingControlPlane,
    ctx,
    decode_client: Dict[str, Any],
    decode_payload: Dict[str, Any],
    decode_headers: Dict[str, str],
    response_headers: Dict[str, str],
    continuation: _PrefillContinuation,
    pipelined_dispatch: Optional[_PipelinedDecodeDispatch] = None,
    prerendered_context: Optional[_PrerenderedDecodeContext] = None,
    mm_hash_lease: Optional[_DecodeMMHashLease] = None,
) -> StreamingResponse:
    decode_start = (
        pipelined_dispatch.started_at
        if pipelined_dispatch is not None
        else time.monotonic()
    )
    try:
        if pipelined_dispatch is None:
            opened = await _open_decode_stream(
                api=decode_api,
                decode_client=decode_client,
                decode_payload=decode_payload,
                decode_headers=decode_headers,
            )
        else:
            opened = await pipelined_dispatch.open_task
        decode_start = opened.started_at
        control_plane.record_stage_span(
            ctx,
            "decode_stream_open",
            started_at=decode_start,
            ended_at=opened.opened_at,
        )
    except asyncio.CancelledError:
        if mm_hash_lease is not None:
            mm_hash_lease.fail()
        control_plane.rollback_handoff(ctx)
        control_plane.mark_stage_complete(
            "decode",
            decode_client["worker_id"],
            latency_ms=(time.monotonic() - decode_start) * 1000.0,
            success=False,
        )
        control_plane.finish_request(ctx.request_id)
        raise
    except Exception as exc:
        if mm_hash_lease is not None:
            if _is_decode_epoch_guard_rejection(exc):
                mm_hash_lease.record_epoch_guard_rejection()
            mm_hash_lease.fail()
        logger.exception(
            "decode streaming startup failed request_id=%s worker_id=%s exc_type=%s",
            ctx.request_id,
            decode_client.get("worker_id"),
            type(exc).__name__,
        )
        control_plane.rollback_handoff(ctx)
        control_plane.mark_stage_complete(
            "decode",
            decode_client["worker_id"],
            latency_ms=(time.monotonic() - decode_start) * 1000.0,
            success=False,
        )
        control_plane.finish_request(ctx.request_id)
        raise HTTPException(
            status_code=502,
            detail=f"decode request failed: {type(exc).__name__}: {exc}",
        ) from exc

    decode_response = opened.response

    if prerendered_context is not None:
        return _dispatch_prerendered_streaming_response(
            opened=opened,
            control_plane=control_plane,
            ctx=ctx,
            decode_client=decode_client,
            response_headers=response_headers,
            context=prerendered_context,
            mm_hash_lease=mm_hash_lease,
        )

    async def generate_stream():
        success = True
        first_line_seen = False
        first_token_seen = False
        pending_prefix = continuation.text if continuation.active else ""
        try:
            async for raw_line in decode_response.aiter_lines():
                if raw_line is None:
                    continue
                if raw_line == "":
                    continue
                line_out = raw_line
                packet = None
                if raw_line.startswith("data:"):
                    payload = raw_line[5:].strip()
                    if payload == "[DONE]":
                        line_out = "data: [DONE]"
                    else:
                        try:
                            packet = json.loads(payload)
                        except Exception:
                            packet = None
                        if isinstance(packet, dict):
                            packet, pending_prefix = _patch_stream_packet(
                                api=api,
                                packet=packet,
                                continuation=continuation,
                                pending_prefix=pending_prefix,
                            )
                            line_out = f"data: {json.dumps(packet, ensure_ascii=False)}"
                if not first_line_seen:
                    control_plane.commit_handoff(ctx)
                    first_line_seen = True
                if (
                    not first_token_seen
                    and isinstance(packet, dict)
                    and _stream_packet_has_visible_token(api, packet)
                ):
                    first_chunk_ready = time.monotonic()
                    control_plane.record_stage_span(
                        ctx,
                        "decode_first_chunk_wait",
                        started_at=decode_start,
                        ended_at=first_chunk_ready,
                    )
                    control_plane.mark_first_token(ctx, emitted_at=first_chunk_ready)
                    first_token_seen = True
                yield (line_out + "\n\n").encode("utf-8")
        except BaseException:
            success = False
            if not first_line_seen:
                control_plane.rollback_handoff(ctx)
            raise
        finally:
            if success and not first_line_seen:
                success = False
                control_plane.rollback_handoff(ctx)
            await _close_opened_decode_stream(opened)
            control_plane.mark_stage_complete(
                "decode",
                decode_client["worker_id"],
                latency_ms=(time.monotonic() - decode_start) * 1000.0,
                success=success,
            )
            control_plane.finish_request(ctx.request_id)

    media_type = decode_response.headers.get("content-type", "text/event-stream")
    return StreamingResponse(
        generate_stream(),
        status_code=decode_response.status_code,
        headers=response_headers,
        media_type=media_type,
    )


def _patch_stream_packet(
    *,
    api: str,
    packet: Dict[str, Any],
    continuation: _PrefillContinuation,
    pending_prefix: str,
) -> tuple[Dict[str, Any], str]:
    choices = list(packet.get("choices") or [])
    if choices and pending_prefix:
        choice = dict(choices[0] or {})
        merged, consumed = _merge_choice_prefix(api, choice, pending_prefix)
        choices[0] = merged
        packet["choices"] = choices
        if consumed:
            pending_prefix = ""
    if "usage" in packet:
        packet["usage"] = _patch_usage(dict(packet.get("usage") or {}), continuation)
    return packet, pending_prefix


def _stream_packet_has_visible_token(api: str, packet: Dict[str, Any]) -> bool:
    for choice in list(packet.get("choices") or []):
        if not isinstance(choice, dict):
            continue
        if api == "/v1/chat/completions":
            delta = choice.get("delta")
            if not isinstance(delta, dict):
                continue
            content = delta.get("content")
            if isinstance(content, str) and content:
                return True
            if isinstance(content, list) and content:
                return True
            continue
        text = choice.get("text")
        if isinstance(text, str) and text:
            return True
    return False


def _patch_non_stream_payload(
    api: str,
    payload: Dict[str, Any],
    continuation: _PrefillContinuation,
) -> Dict[str, Any]:
    patched = dict(payload)
    choices = list(patched.get("choices") or [])
    if choices and continuation.active:
        choice = dict(choices[0] or {})
        choice, _ = _merge_choice_prefix(api, choice, continuation.text)
        choices[0] = choice
        patched["choices"] = choices
    if "usage" in patched:
        patched["usage"] = _patch_usage(dict(patched.get("usage") or {}), continuation)
    return patched


def _merge_choice_prefix(api: str, choice: Dict[str, Any], prefix_text: str) -> tuple[Dict[str, Any], bool]:
    if not prefix_text:
        return choice, False
    merged = dict(choice)
    if api.endswith("/chat/completions"):
        delta = merged.get("delta")
        if isinstance(delta, dict):
            delta = dict(delta)
            content = delta.get("content")
            if content is not None:
                delta["content"] = _prepend_stream_content(content, prefix_text)
                merged["delta"] = delta
                return merged, True
            if merged.get("finish_reason") is not None:
                delta["content"] = prefix_text
                merged["delta"] = delta
                return merged, True
        message = merged.get("message")
        if isinstance(message, dict):
            message = dict(message)
            content = message.get("content")
            message["content"] = _prepend_message_content(content, prefix_text)
            merged["message"] = message
            return merged, True
        return merged, False
    text = merged.get("text")
    if text is not None:
        merged["text"] = prefix_text + str(text)
        return merged, True
    return merged, False


def _prepend_stream_content(content: Any, prefix_text: str) -> Any:
    if isinstance(content, str):
        return prefix_text + content
    if isinstance(content, list):
        if content:
            first = content[0]
            if isinstance(first, dict) and str(first.get("type", "")).strip().lower() == "text":
                first = dict(first)
                first["text"] = prefix_text + str(first.get("text", ""))
                return [first, *content[1:]]
        return [{"type": "text", "text": prefix_text}, *content]
    return prefix_text


def _prepend_message_content(content: Any, prefix_text: str) -> Any:
    if isinstance(content, str):
        return prefix_text + content
    if isinstance(content, list):
        if content:
            first = content[0]
            if isinstance(first, dict) and str(first.get("type", "")).strip().lower() == "text":
                first = dict(first)
                first["text"] = prefix_text + str(first.get("text", ""))
                return [first, *content[1:]]
        return [{"type": "text", "text": prefix_text}, *content]
    return prefix_text


def _patch_usage(usage: Dict[str, Any], continuation: _PrefillContinuation) -> Dict[str, Any]:
    if not continuation.active:
        return usage
    patched = dict(usage)
    completion_tokens = int(patched.get("completion_tokens", 0) or 0) + continuation.completion_tokens
    if continuation.prompt_tokens is not None:
        prompt_tokens = continuation.prompt_tokens
    else:
        prompt_tokens = int(patched.get("prompt_tokens", 0) or 0)
    patched["prompt_tokens"] = prompt_tokens
    patched["completion_tokens"] = completion_tokens
    patched["total_tokens"] = prompt_tokens + completion_tokens
    return patched


def _extract_choice_text(api: str, choice: Dict[str, Any]) -> str:
    if api.endswith("/chat/completions"):
        return _flatten_message_content(
            (choice.get("message") or {}).get("content")
        )
    return str(choice.get("text") or "")


def _flatten_message_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, dict) and str(item.get("type", "")).strip().lower() == "text":
                parts.append(str(item.get("text", "")))
        return "".join(parts)
    return ""


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = parse_args()
    app = create_app(config)

    import uvicorn

    uvicorn.run(app, host=config.host, port=config.port)


if __name__ == "__main__":
    main()
