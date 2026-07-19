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
import hmac
import hashlib
import json
import logging
import math
import os
import sys
import time
import uuid
from collections import OrderedDict
from copy import deepcopy
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx
import torch
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.agent.coordination.scheduler import AdmissionAction  # noqa: E402
from mooncake_epd.core.control import (  # noqa: E402
    DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION,
    DecodeTokenEnvelope,
    DecodeTokenEnvelopeError,
    ServingControlPlane,
    ServingControlPlaneConfig,
    build_decode_token_envelope,
)
from mooncake_epd.integrations.vllm.capabilities import (  # noqa: E402
    PROMPT_ONLY_PROTOCOL_VERSION,
)
from mooncake_epd.core.strict_mode import strict_no_fallback_enabled  # noqa: E402
from mooncake_epd.core.state import (  # noqa: E402
    FeatureBundle,
    FeatureBundleDescriptor,
    FeatureHandle,
    FeatureHandleError,
    MMStore,
)
from mooncake_epd.core.transfer import TransferEngine  # noqa: E402


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _set_proxy_timing(req_data: Dict[str, Any], key: str, value_ms: float) -> None:
    metadata = dict(req_data.get("metadata") or {})
    timings = dict(metadata.get("mooncake_epd_proxy_timings_ms") or {})
    timings[str(key)] = float(value_ms)
    metadata["mooncake_epd_proxy_timings_ms"] = timings
    req_data["metadata"] = metadata


def _get_proxy_timings(req_data: Dict[str, Any]) -> Dict[str, float]:
    metadata = dict(req_data.get("metadata") or {})
    raw = dict(metadata.get("mooncake_epd_proxy_timings_ms") or {})
    out: Dict[str, float] = {}
    for key, value in raw.items():
        try:
            out[str(key)] = float(value)
        except Exception:
            continue
    return out


def _timing_header(req_data: Dict[str, Any]) -> str:
    return json.dumps(_get_proxy_timings(req_data), ensure_ascii=True, separators=(",", ":"))


def _merge_timing_header(headers: Dict[str, str], key: str, value_ms: float) -> None:
    try:
        current = json.loads(str(headers.get("X-EPD-Timing-Ms") or "{}"))
        if not isinstance(current, dict):
            current = {}
    except Exception:
        current = {}
    current[str(key)] = float(value_ms)
    headers["X-EPD-Timing-Ms"] = json.dumps(
        current,
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _build_json_request_with_metrics(
    client: httpx.AsyncClient,
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    metrics: Dict[str, float],
    *,
    prefix: str,
) -> httpx.Request:
    """Encode one outgoing JSON request once and expose its actual wire size.

    Calling ``json.dumps`` only for observability would duplicate the most
    expensive CPU/memory operation on multimodal requests.  Building the
    request explicitly lets httpx perform the same encoding it would perform
    in ``post(json=...)`` while making the encoded body available for metrics
    and the subsequent send.
    """

    encode_started = time.perf_counter()
    outgoing = client.build_request(
        "POST",
        url,
        json=payload,
        headers=headers,
    )
    metrics[f"{prefix}_json_encode_ms"] = (
        time.perf_counter() - encode_started
    ) * 1000.0
    try:
        metrics[f"{prefix}_request_bytes"] = float(len(outgoing.content))
    except httpx.RequestNotRead:
        # ``json=`` currently creates an in-memory byte stream. Fail closed to
        # an absent size if a future httpx version changes that contract rather
        # than consuming a one-shot stream for instrumentation.
        pass
    return outgoing


def _response_content_bytes(response: httpx.Response) -> float:
    try:
        return float(len(response.content))
    except httpx.ResponseNotRead:
        raw = response.headers.get("content-length")
        try:
            return float(max(0, int(raw or 0)))
        except (TypeError, ValueError):
            return 0.0


def _packet_content_delta(packet: Dict[str, Any]) -> str:
    try:
        choices = list(packet.get("choices") or [])
        if not choices:
            return ""
        first = dict(choices[0] or {})
        delta = first.get("delta")
        if isinstance(delta, dict):
            content = delta.get("content")
            return str(content) if content is not None else ""
        message = first.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            return str(content) if content is not None else ""
    except Exception:
        return ""
    return ""


def _attach_packet_timings(packet: Dict[str, Any], timings: Dict[str, float]) -> Dict[str, Any]:
    if not timings:
        return packet
    merged = dict(packet.get("_mooncake_epd_proxy_timings_ms") or {})
    for key, value in timings.items():
        try:
            merged[str(key)] = float(value)
        except Exception:
            continue
    packet["_mooncake_epd_proxy_timings_ms"] = merged
    return packet


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
    mooncake_protocol: str = "tcp"
    node_id: str = "proxy"
    owner_shards: int = 1
    kv_directory_rpc_url: Optional[str] = None
    connector_metrics_dir: Optional[str] = None
    workflow_registry_wal_path: Optional[str] = None
    enable_mm_prefetch: bool = True
    mm_prefetch_mode: str = "asset_bytes"
    prefill_supports_feature_handles: bool = False
    mm_prefetch_wait_ms: float = 100.0
    mm_prefetch_max_asset_bytes: int = 16 * 1024 * 1024
    mm_prefetch_queue_size: int = 256
    encoder_service_url: Optional[str] = None
    encoder_service_timeout_s: float = 120.0
    prefill_direct_buffer_service_url: Optional[str] = None
    prefill_direct_buffer_service_urls: List[str] = field(default_factory=list)
    prefill_direct_buffer_timeout_s: float = 30.0
    direct_feature_buffer_auth_token: Optional[str] = None
    release_direct_feature_buffers_after_prefill: bool = True
    direct_feature_release_max_inflight: int = 128
    # Post-Prefill releases are independent ownership decrements. Coalesce a
    # bounded burst into one RPC per Prefill worker so cleanup cannot contend
    # with cache lookup/generate traffic on the same vLLM API server.
    direct_feature_release_batch_max_jobs: int = 16
    enable_direct_feature_handle_cache: bool = False
    direct_feature_handle_cache_max_entries: int = 4096
    direct_feature_handle_cache_ttl_s: float = 600.0
    # Kept CLI/config compatible with earlier lock-retaining releases. Live
    # single-flight futures now self-remove at completion, so this no longer
    # controls a historical-key cache; request admission bounds their count.
    direct_feature_singleflight_max_locks: int = 4096
    # Keep a small pool of *reference leases*, never stale handles, for an
    # identical E->P descriptor tuple.  Every checkout still triggers the
    # ordinary post-Prefill release.  The pool is generation-fenced and its
    # unused references are released on TTL, eviction, explicit flush, and
    # process shutdown. ``1`` keeps serial requests at one RPC; the separate
    # adaptive option may still coalesce already-concurrent requests.
    direct_feature_lease_prefetch: int = 1
    direct_feature_lease_prefetch_max_entries: int = 64
    direct_feature_lease_prefetch_ttl_s: float = 30.0
    # Keep the single-request default while collapsing only a set of already
    # concurrent identical direct-cache lookups into one multi-reference
    # reservation.  The coordinator yields once (never sleeps for a wall-clock
    # interval), so a serial request still acquires exactly one reference.
    # This is deliberately opt-in: a higher-concurrency launch can alter the
    # timing of P->D admissions even though FeatureBuffer reference accounting
    # remains correct. Enable it only with a workload-specific strict run.
    direct_feature_adaptive_lease_prefetch: bool = False
    direct_feature_adaptive_lease_prefetch_max: int = 2
    # ``render_generate`` needs vLLM to convert an OpenAI request into an
    # internal GenerateRequest before the zero-token Prefill call.  That
    # conversion is deterministic for an identical request and model worker,
    # but it can dominate TTFT for repeated multimodal prompts.  Cache only
    # that rendered control-plane artifact; every request still receives fresh
    # P->D KV metadata below.
    enable_rendered_prefill_cache: bool = True
    rendered_prefill_cache_max_entries: int = 64
    rendered_prefill_cache_max_bytes: int = 256 * 1024 * 1024
    rendered_prefill_cache_ttl_s: float = 300.0
    prefill_dispatch_mode: str = "render_generate"  # render_generate | openai_prompt_only
    # ``shadow`` validates the token envelope but retains the legacy Decode
    # request. ``token_ids`` sends the compact, media-free Completion request
    # only after both worker capability and envelope identity validation.
    decode_dispatch_mode: str = "legacy"  # legacy | shadow | token_ids
    # No implicit fast-path fallback: an explicit non-strict ablation may opt
    # into legacy Decode when a request uses unsupported Chat-only semantics.
    allow_decode_token_fallback: bool = False
    # A dropped/stale HTTP/1.1 idle connection can surface after a POST has
    # been selected but before vLLM returns response headers.  Retrying that
    # POST is unsafe because Prefill may already have created a KV handoff.
    # Keep Prefill idle-connection reuse disabled by default; deployments that
    # have validated their upstream keep-alive behavior may opt in explicitly.
    prefill_http_keepalive: bool = False
    # ``openai_prompt_only`` has a lower control-plane cost, but has not shown
    # output equivalence for every real vLLM P-D continuation configuration.
    # Strict serving therefore requires an explicit compatibility override.
    allow_unverified_openai_prompt_only: bool = False
    strict_no_fallback: bool = field(default_factory=strict_no_fallback_enabled)
    enable_agent_state_clone: bool = True
    require_real_agent_state_store: bool = False
    high_prefill_worker_ids: List[str] = field(default_factory=list)
    low_latency_decode_worker_ids: List[str] = field(default_factory=list)
    standard_prefill_worker_ids: List[str] = field(default_factory=list)
    standard_decode_worker_ids: List[str] = field(default_factory=list)
    scheduler_policy: str = "agent_aware"
    prefill_decode_affinity: List[Tuple[str, str]] = field(default_factory=list)
    worker_generations: List[Tuple[str, str]] = field(default_factory=list)
    worker_generation_dir: Optional[str] = None


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


def _parse_prefill_decode_affinity(values: Optional[Sequence[str]]) -> List[Tuple[str, str]]:
    """Parse explicit `prefill-worker=decode-worker` locality pairs."""

    parsed: List[Tuple[str, str]] = []
    seen: Dict[str, str] = {}
    for raw in values or ():
        source, separator, target = str(raw or "").partition("=")
        source = source.strip()
        target = target.strip()
        if not separator or not source or not target:
            raise ValueError(
                "--prefill-decode-affinity entries must use prefill-worker=decode-worker"
            )
        previous = seen.get(source)
        if previous is not None and previous != target:
            raise ValueError(
                f"conflicting Decode affinity targets for {source}: {previous} and {target}"
            )
        if previous is None:
            seen[source] = target
            parsed.append((source, target))
    return parsed


def _parse_worker_generations(values: Optional[Sequence[str]]) -> List[Tuple[str, str]]:
    """Parse stable worker-generation fences emitted by config generation."""

    parsed: List[Tuple[str, str]] = []
    seen: Dict[str, str] = {}
    for raw in values or ():
        worker, separator, generation = str(raw or "").partition("=")
        worker = worker.strip()
        generation = generation.strip()
        if not separator or not worker or not generation:
            raise ValueError(
                "--worker-generation entries must use worker-id=generation"
            )
        existing = seen.get(worker)
        if existing is not None and existing != generation:
            raise ValueError(f"conflicting generation fences for worker={worker}")
        if existing is None:
            seen[worker] = generation
            parsed.append((worker, generation))
    return parsed


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
    parser.add_argument("--mooncake-protocol", type=str, default=os.getenv("MOONCAKE_PROTOCOL", "tcp"))
    parser.add_argument("--node-id", type=str, default="proxy")
    parser.add_argument("--owner-shards", type=int, default=1)
    parser.add_argument("--kv-directory-rpc-url", type=str, default=None)
    parser.add_argument("--connector-metrics-dir", type=str, default=None)
    parser.add_argument("--workflow-registry-wal", type=str, default=None)
    parser.add_argument("--enable-mm-prefetch", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--mm-prefetch-mode", choices=["asset_bytes", "feature_handle"], default="asset_bytes")
    parser.add_argument("--prefill-supports-feature-handles", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--mm-prefetch-wait-ms", type=float, default=100.0)
    parser.add_argument("--mm-prefetch-max-asset-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--mm-prefetch-queue-size", type=int, default=256)
    parser.add_argument("--encoder-service-url", type=str, default=os.getenv("MOONCAKE_EPD_ENCODER_SERVICE_URL"))
    parser.add_argument("--encoder-service-timeout-s", type=float, default=float(os.getenv("MOONCAKE_EPD_ENCODER_SERVICE_TIMEOUT_S", "120")))
    parser.add_argument("--prefill-direct-buffer-service-url", type=str, default=os.getenv("MOONCAKE_EPD_PREFILL_DIRECT_BUFFER_SERVICE_URL"))
    parser.add_argument("--prefill-direct-buffer-service-urls", type=str, nargs="*", default=None)
    parser.add_argument("--prefill-direct-buffer-timeout-s", type=float, default=float(os.getenv("MOONCAKE_EPD_PREFILL_DIRECT_BUFFER_TIMEOUT_S", "30")))
    parser.add_argument(
        "--direct-feature-buffer-auth-token",
        default=os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN"),
    )
    parser.add_argument("--release-direct-feature-buffers-after-prefill", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--direct-feature-release-max-inflight",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_RELEASE_MAX_INFLIGHT", "128")),
    )
    parser.add_argument(
        "--direct-feature-release-batch-max-jobs",
        type=int,
        default=int(
            os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_RELEASE_BATCH_MAX_JOBS", "16")
        ),
        help=(
            "Maximum completed-Prefill release jobs merged into one control "
            "RPC per worker; this never changes reference multiplicity."
        ),
    )
    parser.add_argument("--enable-direct-feature-handle-cache", action=argparse.BooleanOptionalAction, default=os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE", "0").lower() in {"1", "true", "yes", "on"})
    parser.add_argument("--direct-feature-handle-cache-max-entries", type=int, default=int(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE_MAX_ENTRIES", "4096")))
    parser.add_argument("--direct-feature-handle-cache-ttl-s", type=float, default=float(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE_TTL_S", "600")))
    parser.add_argument("--direct-feature-singleflight-max-locks", type=int, default=int(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_SINGLEFLIGHT_MAX_LOCKS", "4096")))
    parser.add_argument(
        "--direct-feature-lease-prefetch",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_LEASE_PREFETCH", "1")),
        help=(
            "Reserve this many generation-fenced direct-buffer references per "
            "hot E->P lookup; 1 keeps serial requests at one reference."
        ),
    )
    parser.add_argument(
        "--direct-feature-lease-prefetch-max-entries",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_LEASE_PREFETCH_MAX_ENTRIES", "64")),
    )
    parser.add_argument(
        "--direct-feature-lease-prefetch-ttl-s",
        type=float,
        default=float(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_LEASE_PREFETCH_TTL_S", "30")),
    )
    parser.add_argument(
        "--enable-direct-feature-adaptive-lease-prefetch",
        action=argparse.BooleanOptionalAction,
        default=os.getenv(
            "MOONCAKE_EPD_DIRECT_FEATURE_ADAPTIVE_LEASE_PREFETCH", "0"
        ).lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Opt in to coalescing already-concurrent identical direct-cache "
            "lookups into one bounded reservation when fixed lease count is 1."
        ),
    )
    parser.add_argument(
        "--direct-feature-adaptive-lease-prefetch-max",
        type=int,
        default=int(
            os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_ADAPTIVE_LEASE_PREFETCH_MAX", "2")
        ),
        help="Maximum references in one adaptive direct-cache reservation.",
    )
    parser.add_argument(
        "--enable-rendered-prefill-cache",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_RENDERED_PREFILL_CACHE", "1").lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Reuse generation-fenced vLLM /render artifacts for identical "
            "immutable requests; fresh KV metadata is injected per request."
        ),
    )
    parser.add_argument(
        "--rendered-prefill-cache-max-entries",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_RENDERED_PREFILL_CACHE_MAX_ENTRIES", "64")),
    )
    parser.add_argument(
        "--rendered-prefill-cache-max-bytes",
        type=int,
        default=int(
            os.getenv(
                "MOONCAKE_EPD_RENDERED_PREFILL_CACHE_MAX_BYTES",
                str(256 * 1024 * 1024),
            )
        ),
    )
    parser.add_argument(
        "--rendered-prefill-cache-ttl-s",
        type=float,
        default=float(os.getenv("MOONCAKE_EPD_RENDERED_PREFILL_CACHE_TTL_S", "300")),
    )
    parser.add_argument("--prefill-dispatch-mode", choices=["render_generate", "openai_prompt_only"], default=os.getenv("MOONCAKE_EPD_PREFILL_DISPATCH_MODE", "render_generate"))
    parser.add_argument(
        "--decode-dispatch-mode",
        choices=["legacy", "shadow", "token_ids"],
        default=os.getenv("MOONCAKE_EPD_DECODE_DISPATCH_MODE", "legacy"),
        help=(
            "legacy forwards the original request; shadow validates a v1 "
            "token envelope; token_ids sends the compact media-free request."
        ),
    )
    parser.add_argument(
        "--allow-decode-token-fallback",
        action=argparse.BooleanOptionalAction,
        default=os.getenv(
            "MOONCAKE_EPD_ALLOW_DECODE_TOKEN_FALLBACK", "0"
        ).lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Permit an explicitly recorded non-strict token_ids -> legacy "
            "fallback for unsupported request semantics."
        ),
    )
    parser.add_argument(
        "--prefill-http-keepalive",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_PREFILL_HTTP_KEEPALIVE", "0").lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Reuse idle Proxy->Prefill HTTP connections. Disabled by default "
            "because a stale connection makes non-idempotent Prefill POSTs "
            "unsafe to retry."
        ),
    )
    parser.add_argument(
        "--allow-unverified-openai-prompt-only",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_ALLOW_UNVERIFIED_OPENAI_PROMPT_ONLY", "0").lower()
        in {"1", "true", "yes", "on"},
        help="Permit openai_prompt_only in strict serving after workload-specific output-equivalence validation.",
    )
    parser.add_argument("--strict-no-fallback", action=argparse.BooleanOptionalAction, default=strict_no_fallback_enabled())
    parser.add_argument("--enable-agent-state-clone", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--require-real-agent-state-store",
        action=argparse.BooleanOptionalAction,
        default=strict_no_fallback_enabled(),
        help="Reject client-supplied KV block IDs; require server-owned capture through MooncakeKVStateStore.",
    )
    parser.add_argument(
        "--scheduler-policy",
        choices=["round_robin", "least_loaded", "static_type_route", "agent_aware"],
        default=os.getenv("MOONCAKE_EPD_SCHEDULER_POLICY", "agent_aware"),
    )
    parser.add_argument("--high-prefill-worker-ids", nargs="*", default=None)
    parser.add_argument("--low-latency-decode-worker-ids", nargs="*", default=None)
    parser.add_argument("--standard-prefill-worker-ids", nargs="*", default=None)
    parser.add_argument("--standard-decode-worker-ids", nargs="*", default=None)
    parser.add_argument(
        "--prefill-decode-affinity",
        nargs="*",
        default=None,
        metavar="PREFILL=DECODE",
        help="Soft P->D transport-locality pairs; overload admission may select another Decode worker.",
    )
    parser.add_argument(
        "--worker-generation",
        action="append",
        default=[],
        metavar="WORKER=GENERATION",
        help="Worker incarnation fence used to validate strict P->D KV manifests.",
    )
    parser.add_argument(
        "--worker-generation-dir",
        default=os.getenv("MOONCAKE_EPD_WORKER_GENERATION_DIR"),
        help="Directory containing atomically updated <worker-id> generation fence files.",
    )
    args = parser.parse_args()

    if len(args.prefiller_hosts) != len(args.prefiller_ports):
        raise ValueError("Number of prefiller hosts must match number of prefiller ports")
    if len(args.decoder_hosts) != len(args.decoder_ports):
        raise ValueError("Number of decoder hosts must match number of decoder ports")

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
        mooncake_protocol=str(args.mooncake_protocol),
        node_id=args.node_id,
        owner_shards=args.owner_shards,
        kv_directory_rpc_url=args.kv_directory_rpc_url,
        connector_metrics_dir=args.connector_metrics_dir,
        workflow_registry_wal_path=args.workflow_registry_wal,
        enable_mm_prefetch=bool(args.enable_mm_prefetch),
        mm_prefetch_mode=str(args.mm_prefetch_mode),
        prefill_supports_feature_handles=bool(args.prefill_supports_feature_handles),
        mm_prefetch_wait_ms=args.mm_prefetch_wait_ms,
        mm_prefetch_max_asset_bytes=args.mm_prefetch_max_asset_bytes,
        mm_prefetch_queue_size=args.mm_prefetch_queue_size,
        encoder_service_url=args.encoder_service_url,
        encoder_service_timeout_s=args.encoder_service_timeout_s,
        prefill_direct_buffer_service_url=args.prefill_direct_buffer_service_url,
        prefill_direct_buffer_service_urls=list(args.prefill_direct_buffer_service_urls or []),
        prefill_direct_buffer_timeout_s=args.prefill_direct_buffer_timeout_s,
        direct_feature_buffer_auth_token=args.direct_feature_buffer_auth_token,
        release_direct_feature_buffers_after_prefill=bool(args.release_direct_feature_buffers_after_prefill),
        direct_feature_release_max_inflight=max(1, int(args.direct_feature_release_max_inflight)),
        direct_feature_release_batch_max_jobs=min(
            1024, max(1, int(args.direct_feature_release_batch_max_jobs))
        ),
        enable_direct_feature_handle_cache=bool(args.enable_direct_feature_handle_cache),
        direct_feature_handle_cache_max_entries=max(0, int(args.direct_feature_handle_cache_max_entries)),
        direct_feature_handle_cache_ttl_s=max(0.0, float(args.direct_feature_handle_cache_ttl_s)),
        direct_feature_singleflight_max_locks=max(16, int(args.direct_feature_singleflight_max_locks)),
        direct_feature_lease_prefetch=min(1024, max(1, int(args.direct_feature_lease_prefetch))),
        direct_feature_lease_prefetch_max_entries=max(0, int(args.direct_feature_lease_prefetch_max_entries)),
        direct_feature_lease_prefetch_ttl_s=max(0.0, float(args.direct_feature_lease_prefetch_ttl_s)),
        direct_feature_adaptive_lease_prefetch=bool(
            args.enable_direct_feature_adaptive_lease_prefetch
        ),
        direct_feature_adaptive_lease_prefetch_max=min(
            1024, max(1, int(args.direct_feature_adaptive_lease_prefetch_max))
        ),
        enable_rendered_prefill_cache=bool(args.enable_rendered_prefill_cache),
        rendered_prefill_cache_max_entries=max(
            0, int(args.rendered_prefill_cache_max_entries)
        ),
        rendered_prefill_cache_max_bytes=max(
            0, int(args.rendered_prefill_cache_max_bytes)
        ),
        rendered_prefill_cache_ttl_s=max(
            0.0, float(args.rendered_prefill_cache_ttl_s)
        ),
        prefill_dispatch_mode=str(args.prefill_dispatch_mode),
        decode_dispatch_mode=str(args.decode_dispatch_mode),
        allow_decode_token_fallback=bool(args.allow_decode_token_fallback),
        prefill_http_keepalive=bool(args.prefill_http_keepalive),
        allow_unverified_openai_prompt_only=bool(args.allow_unverified_openai_prompt_only),
        strict_no_fallback=bool(args.strict_no_fallback),
        enable_agent_state_clone=bool(args.enable_agent_state_clone),
        require_real_agent_state_store=bool(args.require_real_agent_state_store),
        scheduler_policy=str(args.scheduler_policy),
        high_prefill_worker_ids=list(args.high_prefill_worker_ids or []),
        low_latency_decode_worker_ids=list(args.low_latency_decode_worker_ids or []),
        standard_prefill_worker_ids=list(args.standard_prefill_worker_ids or []),
        standard_decode_worker_ids=list(args.standard_decode_worker_ids or []),
        prefill_decode_affinity=_parse_prefill_decode_affinity(
            args.prefill_decode_affinity
        ),
        worker_generations=_parse_worker_generations(args.worker_generation),
        worker_generation_dir=(str(args.worker_generation_dir) if args.worker_generation_dir else None),
    )


def _make_client(
    base_url: str,
    *,
    keepalive: bool = True,
) -> httpx.AsyncClient:
    """Build a stage client with an explicit idle-connection policy.

    ``max_keepalive_connections=0`` closes a connection after its response is
    complete.  This deliberately avoids blindly retrying a non-idempotent
    Prefill ``/inference/v1/generate`` POST when a server has closed an idle
    HTTP/1.1 connection.  Decode keeps the historical reuse policy because it
    does not create the one-shot P->D handoff at this boundary.
    """

    return httpx.AsyncClient(
        timeout=None,
        base_url=base_url,
        limits=httpx.Limits(
            max_connections=None,
            max_keepalive_connections=None if keepalive else 0,
        ),
        trust_env=False,
    )


def _upstream_exception_summary(exc: BaseException) -> str:
    """Return a bounded, payload-free summary for an upstream failure.

    ``httpx`` transport errors commonly stringify to an empty string (for
    example ``ReadError(\"\")``).  Omitting the exception class turns a
    transient connection failure into an un-actionable ``502`` and makes it
    impossible to distinguish it from a Prefill application error.  Do not
    include request data or response bodies here: this helper is used on the
    serving hot path and its result is safe to emit in logs and a FastAPI
    response detail.
    """

    name = type(exc).__name__
    message = str(exc).strip()
    if message:
        return f"{name}: {message[:500]}"
    # Some transport exceptions retain useful structure only in ``repr``.
    # Bound it as well because third-party exception implementations can put
    # arbitrary data in their args.
    rendered = repr(exc).strip()
    if rendered and rendered != name:
        return f"{name}: {rendered[:500]}"
    return name


def _make_control_client(
    base_url: str,
    timeout_s: float,
    *,
    auth_token: Optional[str] = None,
) -> httpx.AsyncClient:
    headers = {}
    if str(auth_token or "").strip():
        headers["X-Mooncake-EPD-Token"] = str(auth_token)
    return httpx.AsyncClient(
        timeout=httpx.Timeout(float(timeout_s), connect=5.0),
        follow_redirects=True,
        trust_env=False,
        base_url=str(base_url).rstrip("/"),
        headers=headers,
    )


def _has_prefill_direct_buffer_service(config: ProxyConfig) -> bool:
    return bool(
        (config.prefill_direct_buffer_service_url or "").strip()
        or [u for u in list(config.prefill_direct_buffer_service_urls or []) if str(u or "").strip()]
    )


def _direct_cache_key(target_worker_id: str, feature_id: str) -> str:
    return f"{str(target_worker_id or '*')}::{str(feature_id)}"


def _direct_worker_id_from_handle(handle: Dict[str, Any], default_worker_id: str = "") -> str:
    metadata = dict(handle.get("metadata") or {}) if isinstance(handle, dict) else {}
    return str(
        metadata.get("mooncake_epd_target_worker_id")
        or metadata.get("target_worker_id")
        or handle.get("target_worker_id")
        or default_worker_id
        or ""
    )


def _prefill_direct_buffer_client_for(app: FastAPI, target_worker_id: str) -> Optional[httpx.AsyncClient]:
    single_client = getattr(app.state, "prefill_direct_buffer_client", None)
    clients = getattr(app.state, "prefill_direct_buffer_clients", None)
    if isinstance(clients, dict):
        client = clients.get(str(target_worker_id or ""))
        if client is not None:
            # Unit tests often replace the legacy single client after lifespan
            # startup with an ASGITransport client.  Preserve production
            # per-worker routing, but honor that explicit override when it
            # points at the same endpoint.
            if (
                single_client is not None
                and single_client is not client
                and str(getattr(single_client, "base_url", "")) == str(getattr(client, "base_url", ""))
            ):
                return single_client
            return client
        default_client = clients.get("*")
        if default_client is not None:
            if (
                single_client is not None
                and single_client is not default_client
                and str(getattr(single_client, "base_url", "")) == str(getattr(default_client, "base_url", ""))
            ):
                return single_client
            return default_client
    return single_client


def _verified_prompt_only_capability(
    prefill_client: Dict[str, Any],
    request_body: Dict[str, Any],
) -> tuple[bool, str]:
    report = prefill_client.get("capabilities")
    if not isinstance(report, dict):
        return False, str(prefill_client.get("capability_error") or "missing report")
    if not bool(report.get("installed", False)):
        return False, "adapter is not installed"
    if not bool(report.get("version_supported", False)):
        return False, "vLLM version is not supported"
    if not bool(report.get("prompt_only_ready", False)):
        return False, "prompt-only hook surface is not ready"
    if not bool(report.get("prompt_only_patch_installed", False)):
        return False, "prompt-only patch is not installed"
    if not bool(report.get("prompt_only_verified", False)):
        return False, "worker did not verify prompt-only protocol"
    installed_protocol = int(report.get("prompt_only_protocol_version", 0) or 0)
    if installed_protocol != PROMPT_ONLY_PROTOCOL_VERSION:
        return (
            False,
            "prompt-only protocol mismatch: "
            f"worker={installed_protocol} proxy={PROMPT_ONLY_PROTOCOL_VERSION}",
        )
    if str(report.get("role") or "").strip().lower() != "prefill":
        return False, f"worker role is not prefill: {report.get('role')!r}"
    if str(report.get("kv_role") or "").strip().lower() != "kv_producer":
        return False, f"worker KV role is not producer: {report.get('kv_role')!r}"
    reported_worker = str(report.get("worker_id") or "").strip()
    expected_worker = str(prefill_client.get("worker_id") or "").strip()
    if reported_worker and expected_worker and reported_worker != expected_worker:
        return (
            False,
            f"worker identity mismatch: report={reported_worker} expected={expected_worker}",
        )
    model = str(request_body.get("model") or "")
    reported_model_hash = str(report.get("model_id_sha256") or "")
    if reported_model_hash and model:
        model_hash = hashlib.sha256(model.encode("utf-8")).hexdigest()
        if not hmac.compare_digest(reported_model_hash, model_hash):
            return False, "request model does not match Prefill capability model"
    return True, ""


def _verified_decode_token_capability(
    decode_client: Dict[str, Any],
    request_body: Dict[str, Any],
    *,
    expected_model_id_sha256: str = "",
) -> tuple[bool, str]:
    report = decode_client.get("capabilities")
    if not isinstance(report, dict):
        return False, str(decode_client.get("capability_error") or "missing report")
    if not bool(report.get("installed", False)):
        return False, "adapter is not installed"
    if not bool(report.get("version_supported", False)):
        return False, "vLLM version is not supported"
    if not bool(report.get("decode_token_prompt_ready", False)):
        return False, "Decode token-prompt endpoint is not ready"
    try:
        installed_protocol = int(
            report.get("decode_token_envelope_protocol_version", 0) or 0
        )
    except (TypeError, ValueError):
        return False, "Decode token-envelope protocol is invalid"
    if installed_protocol != DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION:
        return (
            False,
            "Decode token-envelope protocol mismatch: "
            f"worker={installed_protocol} proxy="
            f"{DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION}",
        )
    if str(report.get("decode_token_endpoint") or "") != "/v1/completions":
        return False, "Decode token endpoint is not /v1/completions"
    if str(report.get("role") or "").strip().lower() != "decode":
        return False, f"worker role is not decode: {report.get('role')!r}"
    if str(report.get("kv_role") or "").strip().lower() != "kv_consumer":
        return False, f"worker KV role is not consumer: {report.get('kv_role')!r}"
    reported_worker = str(report.get("worker_id") or "").strip()
    expected_worker = str(decode_client.get("worker_id") or "").strip()
    if reported_worker and expected_worker and reported_worker != expected_worker:
        return (
            False,
            f"worker identity mismatch: report={reported_worker} "
            f"expected={expected_worker}",
        )
    reported_model_hash = str(report.get("model_id_sha256") or "")
    request_model = str(request_body.get("model") or "")
    request_model_hash = (
        hashlib.sha256(request_model.encode("utf-8")).hexdigest()
        if request_model
        else ""
    )
    for label, model_hash in (
        ("Prefill", str(expected_model_id_sha256 or "")),
        ("request", request_model_hash),
    ):
        if (
            reported_model_hash
            and model_hash
            and not hmac.compare_digest(reported_model_hash, model_hash)
        ):
            return False, f"Decode model identity does not match {label}"
    return True, ""


async def _refresh_worker_capability(
    worker_client: Dict[str, Any],
    *,
    timeout_s: float = 5.0,
) -> None:
    worker_client.pop("capabilities", None)
    worker_client.pop("capability_error", None)
    response = None
    try:
        response = await asyncio.wait_for(
            worker_client["client"].get("/mooncake_epd/capabilities"),
            timeout=max(0.1, float(timeout_s)),
        )
        response.raise_for_status()
        report = response.json()
        if not isinstance(report, dict):
            raise TypeError("capability response is not an object")
        worker_client["capabilities"] = dict(report)
    except Exception as exc:
        worker_client["capability_error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if response is not None:
            await response.aclose()


async def _refresh_prefill_capability(
    prefill_client: Dict[str, Any],
    *,
    timeout_s: float = 5.0,
) -> None:
    await _refresh_worker_capability(prefill_client, timeout_s=timeout_s)


async def _probe_prefill_capabilities(
    prefill_clients: Sequence[Dict[str, Any]],
) -> None:
    """Attach fail-closed capability evidence to each Prefill client."""

    await asyncio.gather(
        *(
            _refresh_worker_capability(prefill_client)
            for prefill_client in prefill_clients
        )
    )


async def _probe_decode_capabilities(
    decode_clients: Sequence[Dict[str, Any]],
) -> None:
    """Attach fail-closed token-prompt capability evidence to Decode clients."""

    await asyncio.gather(
        *(
            _refresh_worker_capability(decode_client)
            for decode_client in decode_clients
        )
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
                "client": _make_client(
                    f"http://{host}:{port}",
                    keepalive=bool(config.prefill_http_keepalive),
                ),
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
                "client": _make_client(f"http://{host}:{port}"),
                "host": host,
                "port": port,
                "id": idx,
                "worker_id": f"decode-{idx}",
            }
            for idx, (host, port) in enumerate(config.decoder_instances)
        ]
    else:
        app.state.decode_clients = list(decode_overrides)

    if (
        str(config.prefill_dispatch_mode or "").strip().lower()
        == "openai_prompt_only"
        or str(config.decode_dispatch_mode or "").strip().lower()
        in {"shadow", "token_ids"}
    ):
        await _probe_prefill_capabilities(app.state.prefill_clients)
    if str(config.decode_dispatch_mode or "").strip().lower() in {
        "shadow",
        "token_ids",
    }:
        await _probe_decode_capabilities(app.state.decode_clients)

    control_plane.register_stage_workers(
        "prefill", [client["worker_id"] for client in app.state.prefill_clients]
    )
    control_plane.register_stage_workers(
        "decode", [client["worker_id"] for client in app.state.decode_clients]
    )
    app.state.mm_fetch_client = httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=5.0),
        follow_redirects=True,
        trust_env=False,
    )
    app.state.encoder_client = httpx.AsyncClient(
        timeout=httpx.Timeout(config.encoder_service_timeout_s, connect=10.0),
        follow_redirects=True,
        trust_env=False,
        base_url=(config.encoder_service_url.rstrip("/") if config.encoder_service_url else ""),
    )
    direct_urls = [
        str(url).rstrip("/")
        for url in list(config.prefill_direct_buffer_service_urls or [])
        if str(url or "").strip()
    ]
    if not direct_urls and config.prefill_direct_buffer_service_url:
        direct_urls = [str(config.prefill_direct_buffer_service_url).rstrip("/")]
    app.state.prefill_direct_buffer_clients = {}
    app.state.prefill_direct_buffer_urls = {}
    if direct_urls:
        prefill_clients = list(getattr(app.state, "prefill_clients", []) or [])
        if len(direct_urls) == 1:
            client = _make_control_client(
                direct_urls[0],
                config.prefill_direct_buffer_timeout_s,
                auth_token=config.direct_feature_buffer_auth_token,
            )
            app.state.prefill_direct_buffer_client = client
            app.state.prefill_direct_buffer_clients["*"] = client
            for info in prefill_clients:
                worker_id = str(info.get("worker_id") or "")
                if worker_id:
                    app.state.prefill_direct_buffer_clients[worker_id] = client
                    app.state.prefill_direct_buffer_urls[worker_id] = direct_urls[0]
        else:
            if len(direct_urls) != len(prefill_clients):
                raise RuntimeError(
                    "--prefill-direct-buffer-service-urls count must match prefiller count "
                    f"when multiple URLs are provided: urls={len(direct_urls)} prefill={len(prefill_clients)}"
                )
            first_client: Optional[httpx.AsyncClient] = None
            for info, url in zip(prefill_clients, direct_urls):
                worker_id = str(info.get("worker_id") or "")
                client = _make_control_client(
                    url,
                    config.prefill_direct_buffer_timeout_s,
                    auth_token=config.direct_feature_buffer_auth_token,
                )
                if first_client is None:
                    first_client = client
                if worker_id:
                    app.state.prefill_direct_buffer_clients[worker_id] = client
                    app.state.prefill_direct_buffer_urls[worker_id] = url
            app.state.prefill_direct_buffer_client = first_client
    else:
        app.state.prefill_direct_buffer_client = None

    _start_direct_feature_lease_prefetch_sweeper(app)
    try:
        yield
    finally:
        await _shutdown_direct_feature_lease_prefetch_pool(app)
        await _shutdown_direct_feature_release_dispatcher(app)
        mm_fetch_client = getattr(app.state, "mm_fetch_client", None)
        if mm_fetch_client is not None:
            await mm_fetch_client.aclose()
        encoder_client = getattr(app.state, "encoder_client", None)
        if encoder_client is not None:
            await encoder_client.aclose()
        closed_direct_clients = set()
        for prefill_direct_client in list((getattr(app.state, "prefill_direct_buffer_clients", {}) or {}).values()):
            if prefill_direct_client is not None and id(prefill_direct_client) not in closed_direct_clients:
                closed_direct_clients.add(id(prefill_direct_client))
                await prefill_direct_client.aclose()
        prefill_direct_client = getattr(app.state, "prefill_direct_buffer_client", None)
        if prefill_direct_client is not None and id(prefill_direct_client) not in closed_direct_clients:
            await prefill_direct_client.aclose()
        mm_store = getattr(app.state, "mm_store", None)
        if mm_store is not None:
            mm_store.stop()
        for client_info in list(app.state.prefill_clients) + list(app.state.decode_clients):
            client = client_info.get("client")
            if client is not None:
                await client.aclose()


def create_app(
    config: Optional[ProxyConfig] = None,
    *,
    prefill_clients: Optional[Sequence[Dict[str, Any]]] = None,
    decode_clients: Optional[Sequence[Dict[str, Any]]] = None,
    control_plane: Optional[ServingControlPlane] = None,
) -> FastAPI:
    config = config or ProxyConfig()
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
            mooncake_protocol=config.mooncake_protocol,
            owner_shards=config.owner_shards,
            kv_directory_rpc_url=config.kv_directory_rpc_url,
            connector_metrics_dir=config.connector_metrics_dir,
            workflow_registry_wal_path=config.workflow_registry_wal_path,
            enable_mm_prefetch=config.enable_mm_prefetch,
            strict_no_fallback=config.strict_no_fallback,
            enable_agent_state_clone=config.enable_agent_state_clone,
            require_real_agent_state_store=config.require_real_agent_state_store,
            scheduler_policy=config.scheduler_policy,
            high_prefill_worker_ids=tuple(config.high_prefill_worker_ids),
            low_latency_decode_worker_ids=tuple(config.low_latency_decode_worker_ids),
            standard_prefill_worker_ids=tuple(config.standard_prefill_worker_ids),
            standard_decode_worker_ids=tuple(config.standard_decode_worker_ids),
            prefill_decode_affinity=tuple(config.prefill_decode_affinity),
            worker_generations=tuple(config.worker_generations),
            worker_generation_dir=config.worker_generation_dir,
        )
    )
    app = FastAPI(lifespan=_lifespan)
    app.state.proxy_config = config
    app.state.control_plane = cp
    app.state.prefill_client_overrides = list(prefill_clients) if prefill_clients is not None else None
    app.state.decode_client_overrides = list(decode_clients) if decode_clients is not None else None
    app.state.direct_feature_handle_cache = OrderedDict()
    app.state.direct_feature_handle_cache_stats = {
        "hits": 0,
        "misses": 0,
        "stores": 0,
        "evictions": 0,
        "expired": 0,
        "ttl_sweeps": 0,
        "ttl_sweep_entries_scanned": 0,
        "entries": 0,
    }
    app.state.direct_feature_handle_cache_next_ttl_sweep_at = 0.0
    # ``*_locks`` remains as an inert compatibility surface for embedders that
    # introspect old proxy state.  New single-flight coordination keeps only
    # live completion futures: retaining an unlocked lock per image made the
    # metrics misleading and, more importantly, encouraged serial hot-cache
    # lookups after the one cold E→P publish had already completed.
    app.state.direct_feature_handle_inflight_locks = {}
    app.state.direct_feature_handle_inflight_last_used = {}
    app.state.direct_feature_handle_inflight_flights = {}
    app.state.direct_feature_handle_singleflight_stats = {"created": 0, "joined": 0, "evicted": 0, "active": 0}
    app.state.direct_feature_lease_prefetch_pool = OrderedDict()
    app.state.direct_feature_lease_prefetch_locks = {}
    app.state.direct_feature_lease_prefetch_waiters = {}
    app.state.direct_feature_lease_prefetch_next_ttl_sweep_at = 0.0
    app.state.direct_feature_lease_prefetch_sweeper_task = None
    app.state.direct_feature_lease_prefetch_stats = {
        "lookups": 0,
        "pool_hits": 0,
        "pool_misses": 0,
        "reservations": 0,
        "reserved_references": 0,
        "adaptive_reservations": 0,
        "adaptive_reserved_references": 0,
        "adaptive_waiter_peak": 0,
        "checkouts": 0,
        "checkout_references": 0,
        "release_enqueued_references": 0,
        "release_failures": 0,
        "evictions": 0,
        "expired": 0,
        "generation_bypasses": 0,
        "disabled_bypasses": 0,
        "ttl_sweeps": 0,
        "ttl_sweep_entries_scanned": 0,
        "flushes": 0,
        "entries": 0,
        "held_leases": 0,
        "held_references": 0,
    }
    app.state.rendered_prefill_cache = OrderedDict()
    app.state.rendered_prefill_cache_stats = {
        "lookups": 0,
        "hits": 0,
        "misses": 0,
        "stores": 0,
        "evictions": 0,
        "expired": 0,
        "oversize_skips": 0,
        "bypasses": 0,
        "mutable_media_bypasses": 0,
        "unfenced_worker_bypasses": 0,
        "uncacheable_request_bypasses": 0,
        "ttl_sweeps": 0,
        "ttl_sweep_entries_scanned": 0,
        "entries": 0,
        "bytes": 0,
    }
    app.state.rendered_prefill_cache_next_ttl_sweep_at = 0.0
    app.state.direct_feature_release_queue = None
    app.state.direct_feature_release_workers = set()
    app.state.direct_feature_release_overflow = {}
    app.state.direct_feature_release_stats = {
        "scheduled": 0,
        "completed": 0,
        "failed": 0,
        "batches": 0,
        "batched_jobs": 0,
        "released_feature_references": 0,
        "batch_max_jobs": max(
            1,
            int(getattr(config, "direct_feature_release_batch_max_jobs", 16) or 16),
        ),
        "max_inflight": 0,
        "queue_full": 0,
        "coalesced": 0,
    }
    mm_store_protocol = "shm" if str(config.mooncake_protocol).lower() == "shm" else "local"
    app.state.mm_store = MMStore(
        transfer_engine=TransferEngine(protocol=mm_store_protocol),
        max_queue_size=max(1, int(config.mm_prefetch_queue_size)),
        dispatcher_workers=2,
        inline_fallback_on_queue_full=not config.strict_no_fallback,
    ) if config.enable_mm_prefetch else None

    @app.get("/health")
    @app.get("/healthcheck")
    async def health() -> Dict[str, Any]:
        return {"status": "ok", "prefill_clients": len(app.state.prefill_clients), "decode_clients": len(app.state.decode_clients)}

    @app.get("/metrics")
    async def metrics() -> Dict[str, Any]:
        payload = cp.snapshot()
        mm_store = getattr(app.state, "mm_store", None)
        if mm_store is not None:
            payload["mm_store"] = mm_store.stats()
        sf_stats = getattr(app.state, "direct_feature_handle_singleflight_stats", None)
        if isinstance(sf_stats, dict):
            stats = dict(sf_stats)
            flights = getattr(app.state, "direct_feature_handle_inflight_flights", None)
            if isinstance(flights, dict):
                # Count only work that can still block a follower. Historical
                # implementations reported retained, unlocked lock objects as
                # active, obscuring whether cold-publish deduplication was on
                # the serving critical path.
                stats["active"] = len(flights)
            payload["direct_feature_singleflight"] = stats
        direct_cache = getattr(app.state, "direct_feature_handle_cache", None)
        direct_cache_stats = getattr(app.state, "direct_feature_handle_cache_stats", None)
        if isinstance(direct_cache_stats, dict):
            stats = dict(direct_cache_stats)
            stats["entries"] = len(direct_cache) if isinstance(direct_cache, dict) else 0
            payload["direct_feature_handle_cache"] = stats
        await _prune_direct_feature_lease_prefetch_pool(app)
        lease_pool = getattr(app.state, "direct_feature_lease_prefetch_pool", None)
        lease_stats = getattr(app.state, "direct_feature_lease_prefetch_stats", None)
        if isinstance(lease_stats, dict):
            stats = dict(lease_stats)
            stats.update(_direct_feature_lease_prefetch_pool_gauges(lease_pool))
            payload["direct_feature_lease_prefetch"] = stats
        _prune_rendered_prefill_cache(app)
        rendered_cache = getattr(app.state, "rendered_prefill_cache", None)
        rendered_cache_stats = getattr(app.state, "rendered_prefill_cache_stats", None)
        if isinstance(rendered_cache_stats, dict):
            stats = dict(rendered_cache_stats)
            stats["entries"] = len(rendered_cache) if isinstance(rendered_cache, dict) else 0
            stats["bytes"] = _rendered_prefill_cache_bytes(rendered_cache)
            payload["rendered_prefill_cache"] = stats
        payload["prefill_capabilities"] = {
            str(client.get("worker_id") or ""): (
                dict(client.get("capabilities") or {})
                if isinstance(client.get("capabilities"), dict)
                else {"error": str(client.get("capability_error") or "missing report")}
            )
            for client in list(getattr(app.state, "prefill_clients", []) or [])
        }
        payload["decode_capabilities"] = {
            str(client.get("worker_id") or ""): (
                dict(client.get("capabilities") or {})
                if isinstance(client.get("capabilities"), dict)
                else {
                    "error": str(
                        client.get("capability_error") or "missing report"
                    )
                }
            )
            for client in list(getattr(app.state, "decode_clients", []) or [])
        }
        release_stats = getattr(app.state, "direct_feature_release_stats", None)
        if isinstance(release_stats, dict):
            stats = dict(release_stats)
            queue = getattr(app.state, "direct_feature_release_queue", None)
            overflow = getattr(app.state, "direct_feature_release_overflow", None)
            workers = getattr(app.state, "direct_feature_release_workers", None)
            stats["queued"] = queue.qsize() if isinstance(queue, asyncio.Queue) else 0
            stats["overflow_features"] = (
                sum(len(ids) for ids in overflow.values())
                if isinstance(overflow, dict)
                else 0
            )
            stats["workers"] = len(workers) if isinstance(workers, set) else 0
            stats["inflight"] = stats["queued"] + stats["overflow_features"]
            payload["direct_feature_release"] = stats
        return payload

    @app.post("/mooncake_epd/direct_feature_lease_prefetch/flush")
    async def flush_direct_feature_lease_prefetch(request: Request) -> Dict[str, Any]:
        """Release unused leased references at a controlled benchmark boundary.

        This endpoint owns no user data and only releases Proxy-reserved
        references.  When a direct-buffer token is configured, require the
        same internal token used by allocation/release RPCs.
        """

        expected_token = str(config.direct_feature_buffer_auth_token or "").strip()
        provided_token = str(request.headers.get("X-Mooncake-EPD-Token") or "")
        if expected_token and not hmac.compare_digest(expected_token, provided_token):
            raise HTTPException(status_code=403, detail="invalid direct FeatureBuffer token")
        released, failures = await _flush_direct_feature_lease_prefetch_pool(
            app,
            reason="explicit_flush",
            wait_for_release=True,
        )
        stats = _direct_feature_lease_prefetch_stats(app)
        result = dict(stats)
        result.update(
            _direct_feature_lease_prefetch_pool_gauges(
                getattr(app.state, "direct_feature_lease_prefetch_pool", None)
            )
        )
        return {
            "released_references": released,
            "release_failures": failures,
            "stats": result,
        }

    def _register_agent_state_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Compatibility registration; strict mode still requires server refs."""

        return cp.register_agent_state(
            workflow_id=str(payload.get("workflow_id") or ""),
            state_id=(
                str(payload.get("state_id"))
                if payload.get("state_id") is not None
                else None
            ),
            capture_request_id=(
                str(payload.get("request_id") or payload.get("parent_request_id"))
                if payload.get("request_id") is not None or payload.get("parent_request_id") is not None
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

    @app.post("/mooncake_epd/agent_state/register")
    async def register_agent_state(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return _register_agent_state_payload(payload)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/mooncake_epd/agent_state/capture")
    async def capture_agent_state(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve only an adapter-owned KV capture; never accept raw blocks."""

        try:
            return cp.capture_agent_state(
                request_id=str(payload.get("request_id") or payload.get("parent_request_id") or ""),
                state_id=(str(payload["state_id"]) if payload.get("state_id") is not None else None),
            )
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
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

    @app.get("/mooncake_epd/agent_state/{state_id}")
    async def get_agent_state(state_id: str) -> Dict[str, Any]:
        try:
            return cp.get_agent_state(state_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/v1/completions")
    async def handle_completions(request: Request):
        return await _handle_generation_request(app, "/v1/completions", request)

    @app.post("/v1/chat/completions")
    async def handle_chat_completions(request: Request):
        return await _handle_generation_request(app, "/v1/chat/completions", request)

    return app


def _extract_agent_state_id(req_data: Dict[str, Any], request: Optional[Request] = None) -> str:
    metadata = req_data.get("metadata") if isinstance(req_data.get("metadata"), dict) else {}
    candidates = [
        metadata.get("mooncake_epd_agent_state_id"),
        metadata.get("agent_state_id"),
        metadata.get("branch_id"),
        req_data.get("mooncake_epd_agent_state_id"),
        req_data.get("agent_state_id"),
        req_data.get("branch_id"),
    ]
    if request is not None:
        candidates.extend(
            [
                request.headers.get("X-Agent-State-Id"),
                request.headers.get("X-Mooncake-EPD-Agent-State-Id"),
            ]
        )
    for item in candidates:
        text = str(item or "").strip()
        if text:
            return text
    return ""


async def _handle_agent_state_decode_request(
    *,
    app: FastAPI,
    api: str,
    request: Request,
    req_data: Dict[str, Any],
    request_id: str,
    ctx,
    agent_state_id: str,
    request_started_at: float | None = None,
):
    control_plane: ServingControlPlane = app.state.control_plane
    ctx.routing_path = "AGENT_STATE"
    try:
        decode_decision = _admit_or_raise(control_plane, "decode", ctx)
    except HTTPException:
        control_plane.finish_request(request_id)
        raise
    decode_client = _client_for_worker(app.state.decode_clients, decode_decision.worker_id)
    if decode_client is None:
        control_plane.mark_stage_complete(
            "decode",
            decode_decision.worker_id,
            latency_ms=0.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(request_id)
        raise HTTPException(status_code=503, detail="no decode client available")
    if decode_decision.wait_ms > 0:
        await asyncio.sleep(decode_decision.wait_ms / 1000.0)
    try:
        metadata = dict(req_data.get("metadata") or {})
        for_write = bool(metadata.get("agent_state_for_write", req_data.get("agent_state_for_write", False)))
        decode_payload = dict(req_data)
        decode_payload["kv_transfer_params"] = control_plane.consume_agent_state(
            ctx,
            state_id=agent_state_id,
            target_node_id=decode_client["worker_id"],
            for_write=for_write,
        )
        # Keep the user payload intact for vLLM tokenization, but make the
        # state-consumption contract visible to downstream logs/handlers.
        metadata = dict(decode_payload.get("metadata") or {})
        metadata["mooncake_epd_agent_state_id"] = agent_state_id
        metadata["mooncake_epd_agent_state_decode_only"] = True
        decode_payload["metadata"] = metadata
    except Exception as exc:
        control_plane.mark_stage_complete(
            "decode",
            decode_decision.worker_id,
            latency_ms=0.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(request_id)
        raise HTTPException(status_code=409, detail=f"agent state consume failed: {exc}") from exc

    response_headers = {
        "X-Request-Id": request_id,
        "X-EPD-Routing-Path": "AGENT_STATE",
        "X-EPD-Admission": decode_decision.decision.action.value,
        "X-EPD-Degrade-Level": ctx.degrade_level.value,
        "X-EPD-Decode-Worker": str(decode_client["worker_id"]),
        "X-Agent-State-Id": agent_state_id,
    }
    if bool(req_data.get("stream")):
        return await _dispatch_streaming_decode(
            api=api,
            control_plane=control_plane,
            ctx=ctx,
            decode_client=decode_client,
            decode_payload=decode_payload,
            decode_headers=_forward_headers(request, request_id),
            response_headers=response_headers,
            continuation=_PrefillContinuation(),
            request_started_at=request_started_at,
        )
    return await _dispatch_non_streaming_decode(
        api=api,
        control_plane=control_plane,
        ctx=ctx,
        decode_client=decode_client,
        decode_payload=decode_payload,
        decode_headers=_forward_headers(request, request_id),
        response_headers=response_headers,
        continuation=_PrefillContinuation(),
    )


async def _handle_generation_request(app: FastAPI, api: str, request: Request):
    request_started_at = time.perf_counter()
    body_read_started_at = time.perf_counter()
    raw_body = await request.body()
    body_read_finished_at = time.perf_counter()
    json_decode_started_at = time.perf_counter()
    req_data = json.loads(raw_body)
    json_decode_finished_at = time.perf_counter()
    _set_proxy_timing(
        req_data,
        "proxy_request_body_bytes",
        float(len(raw_body)),
    )
    _set_proxy_timing(
        req_data,
        "proxy_request_body_read_ms",
        (body_read_finished_at - body_read_started_at) * 1000.0,
    )
    _set_proxy_timing(
        req_data,
        "proxy_request_json_decode_ms",
        (json_decode_finished_at - json_decode_started_at) * 1000.0,
    )
    _set_proxy_timing(
        req_data,
        "proxy_request_parse_ms",
        (json_decode_finished_at - request_started_at) * 1000.0,
    )
    request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
    req_data = _merge_control_headers(req_data, request)
    config: ProxyConfig = app.state.proxy_config
    control_plane: ServingControlPlane = app.state.control_plane
    ctx = control_plane.start_request(req_data, request_id)
    agent_state_id = _extract_agent_state_id(req_data, request)
    if agent_state_id:
        return await _handle_agent_state_decode_request(
            app=app,
            api=api,
            request=request,
            req_data=req_data,
            request_id=request_id,
            ctx=ctx,
            agent_state_id=agent_state_id,
            request_started_at=request_started_at,
        )

    try:
        prefill_decision = _admit_or_raise(control_plane, "prefill", ctx)
    except HTTPException:
        control_plane.finish_request(request_id)
        raise
    prefill_client = _client_for_worker(app.state.prefill_clients, prefill_decision.worker_id)
    if prefill_client is None:
        control_plane.mark_stage_complete(
            "prefill",
            prefill_decision.worker_id,
            latency_ms=0.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(request_id)
        raise HTTPException(status_code=503, detail="no prefill client available")

    if prefill_decision.wait_ms > 0:
        await asyncio.sleep(prefill_decision.wait_ms / 1000.0)

    prepare_start = time.perf_counter()
    _set_proxy_timing(
        req_data,
        "proxy_request_to_mm_prepare_start_ms",
        (prepare_start - request_started_at) * 1000.0,
    )
    try:
        req_data = await _prepare_multimodal_inputs_for_prefill(
            app=app,
            req_data=req_data,
            ctx=ctx,
            target_worker_id=prefill_decision.worker_id,
        )
        _set_proxy_timing(req_data, "mm_prepare_ms", (time.perf_counter() - prepare_start) * 1000.0)
    except HTTPException as exc:
        control_plane.mark_stage_complete(
            "prefill",
            prefill_decision.worker_id,
            latency_ms=0.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(request_id)
        logger.warning(
            "EPD multimodal preparation failed request_id=%s prefill_worker=%s: %s",
            request_id,
            prefill_decision.worker_id,
            exc.detail,
        )
        raise

    control_plane.mark_stage_started(
        "prefill",
        prefill_client["worker_id"],
        ctx=ctx,
    )
    prefill_headers = _forward_headers(request, request_id)
    prefill_start = time.perf_counter()
    prefill_response = None
    prompt_only_prefill = _should_use_prompt_only_prefill(api)
    try:
        prefill_base_params = dict(req_data.get("kv_transfer_params") or {})
        paired_decode_worker_id = control_plane.preferred_decode_worker_for_prefill(
            prefill_decision.worker_id
        )
        decode_peek = (
            None
            if paired_decode_worker_id
            else _peek_lowest_load_worker(control_plane, "decode")
        )
        prefill_kv_params = control_plane.build_prefill_kv_params(
            ctx,
            prefill_decision,
            decode_worker_id=(
                paired_decode_worker_id
                or (decode_peek.worker_id if decode_peek is not None else None)
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
            dispatch_mode = str(config.prefill_dispatch_mode or "render_generate").strip().lower()
            if dispatch_mode == "openai_prompt_only":
                capability_verified, capability_error = (
                    _verified_prompt_only_capability(
                        prefill_client,
                        req_data,
                    )
                )
                if not capability_verified:
                    # Proxy can start while vLLM is still compiling. Refresh a
                    # failed startup probe on the first warmup request; the
                    # verified report remains cached for measured traffic.
                    await _refresh_prefill_capability(prefill_client)
                    capability_verified, capability_error = (
                        _verified_prompt_only_capability(
                            prefill_client,
                            req_data,
                        )
                    )
                _set_proxy_timing(
                    req_data,
                    "prefill_prompt_only_capability_verified",
                    float(capability_verified),
                )
                _set_proxy_timing(
                    req_data,
                    "prefill_prompt_only_unverified_override",
                    float(
                        config.strict_no_fallback
                        and not capability_verified
                        and config.allow_unverified_openai_prompt_only
                    ),
                )
                if (
                    config.strict_no_fallback
                    and not capability_verified
                    and not config.allow_unverified_openai_prompt_only
                ):
                    raise ValueError(
                        "openai_prompt_only capability is not verified for strict "
                        f"P-D serving on {prefill_client['worker_id']}: "
                        f"{capability_error}; use render_generate or an explicitly "
                        "recorded unverified ablation"
                    )
                prefill_json = await _dispatch_openai_prompt_only_prefill(
                    api=api,
                    prefill_client=prefill_client,
                    prefill_headers=prefill_headers,
                    request_id=request_id,
                    request_body=req_data,
                    kv_transfer_params=prefill_kv_params,
                )
            elif dispatch_mode == "render_generate":
                prefill_json = await _dispatch_prompt_only_prefill(
                    app=app,
                    api=api,
                    prefill_client=prefill_client,
                    prefill_headers=prefill_headers,
                    request_id=request_id,
                    request_body=req_data,
                    kv_transfer_params=prefill_kv_params,
                )
            else:
                raise ValueError(f"unsupported prefill dispatch mode: {dispatch_mode}")
            for timing_key, timing_value in dict(
                prefill_json.get("_mooncake_epd_proxy_timings_ms") or {}
            ).items():
                _set_proxy_timing(req_data, str(timing_key), float(timing_value))
        else:
            prefill_payload = dict(req_data)
            prefill_payload["kv_transfer_params"] = prefill_kv_params
            prefill_payload["stream"] = False
            prefill_payload["max_tokens"] = 1
            if "max_completion_tokens" in prefill_payload:
                prefill_payload["max_completion_tokens"] = 1
            prefill_payload.pop("stream_options", None)
            dispatch_metrics: Dict[str, float] = {}
            prefill_request = _build_json_request_with_metrics(
                prefill_client["client"],
                api,
                prefill_payload,
                prefill_headers,
                dispatch_metrics,
                prefix="prefill_openai",
            )
            prefill_http_started_at = time.perf_counter()
            prefill_response = await prefill_client["client"].send(prefill_request)
            dispatch_metrics["prefill_openai_http_ms"] = (
                time.perf_counter() - prefill_http_started_at
            ) * 1000.0
            dispatch_metrics["prefill_openai_response_bytes"] = _response_content_bytes(
                prefill_response
            )
            prefill_response.raise_for_status()
            prefill_json_decode_started_at = time.perf_counter()
            prefill_json = prefill_response.json()
            dispatch_metrics["prefill_openai_json_decode_ms"] = (
                time.perf_counter() - prefill_json_decode_started_at
            ) * 1000.0
            for timing_key, timing_value in dispatch_metrics.items():
                _set_proxy_timing(req_data, timing_key, timing_value)
    except Exception as exc:
        control_plane.mark_stage_complete(
            "prefill",
            prefill_client["worker_id"],
            latency_ms=(time.perf_counter() - prefill_start) * 1000.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(request_id)
        upstream_error = _upstream_exception_summary(exc)
        logger.warning(
            "EPD prefill dispatch failed request_id=%s prefill_worker=%s: %s",
            request_id,
            prefill_client["worker_id"],
            upstream_error,
            exc_info=True,
        )
        raise HTTPException(
            status_code=502,
            detail=f"prefill request failed: {upstream_error}",
        ) from exc
    finally:
        if prefill_response is not None:
            await prefill_response.aclose()

    await _schedule_direct_feature_buffer_release(app, req_data)
    _set_proxy_timing(req_data, "prefill_ms", (time.perf_counter() - prefill_start) * 1000.0)
    prefill_finished_at = time.perf_counter()

    control_plane.mark_stage_complete(
        "prefill",
        prefill_client["worker_id"],
        latency_ms=(time.perf_counter() - prefill_start) * 1000.0,
        success=True,
        ctx=ctx,
    )
    prefill_continuation = (
        _PrefillContinuation()
        if prompt_only_prefill
        else _extract_prefill_continuation(api, prefill_json)
    )
    if not prompt_only_prefill and _should_short_circuit_after_prefill(req_data, prefill_continuation):
        control_plane.finish_request(request_id)
        return _build_prefill_terminal_response(
            api=api,
            prefill_json=prefill_json,
            request_id=request_id,
            routing_path=ctx.routing_path,
            admission_action=prefill_decision.decision.action.value,
            degrade_level=ctx.degrade_level.value,
        )

    try:
        decode_decision = _admit_or_raise(control_plane, "decode", ctx)
    except HTTPException:
        control_plane.finish_request(request_id)
        raise
    decode_client = _client_for_worker(app.state.decode_clients, decode_decision.worker_id)
    if decode_client is None:
        control_plane.mark_stage_complete(
            "decode",
            decode_decision.worker_id,
            latency_ms=0.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(request_id)
        raise HTTPException(status_code=503, detail="no decode client available")

    try:
        prefill_kv = control_plane.note_prefill_response(
            ctx,
            prefill_json.get("kv_transfer_params"),
            decode_worker_id=decode_client["worker_id"],
        )
    except Exception as exc:
        control_plane.mark_stage_complete(
            "decode",
            decode_decision.worker_id,
            latency_ms=0.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(request_id)
        logger.warning(
            "EPD handoff metadata rejected request_id=%s prefill_worker=%s "
            "decode_worker=%s response_keys=%s: %s",
            request_id,
            prefill_client["worker_id"],
            decode_client["worker_id"],
            sorted(str(key) for key in prefill_json.keys()),
            exc,
        )
        raise HTTPException(
            status_code=502,
            detail=f"prefill response missing usable KV handoff metadata: {exc}",
        ) from exc

    if decode_decision.wait_ms > 0:
        await asyncio.sleep(decode_decision.wait_ms / 1000.0)

    decode_dispatch_started_at = time.perf_counter()
    _set_proxy_timing(
        req_data,
        "proxy_prefill_to_decode_dispatch_ms",
        (decode_dispatch_started_at - prefill_finished_at) * 1000.0,
    )
    _set_proxy_timing(
        req_data,
        "proxy_request_to_decode_dispatch_ms",
        (decode_dispatch_started_at - request_started_at) * 1000.0,
    )
    decode_payload = dict(req_data)
    decode_payload = _apply_prefill_continuation_to_decode_payload(
        api=api,
        decode_payload=decode_payload,
        continuation=prefill_continuation,
    )
    try:
        decode_payload["kv_transfer_params"] = control_plane.build_decode_kv_params(
            ctx,
            decode_decision,
            prefill_kv,
        )
        if prefill_continuation.active:
            decode_payload["kv_transfer_params"].update(
                _build_prefill_decode_semantic_hints(
                    continuation=prefill_continuation,
                    prefill_kv=prefill_kv,
                )
            )
    except Exception as exc:
        control_plane.rollback_handoff(ctx)
        control_plane.mark_stage_complete(
            "decode",
            decode_decision.worker_id,
            latency_ms=0.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(request_id)
        logger.warning(
            "EPD decode handoff construction failed request_id=%s prefill_worker=%s "
            "decode_worker=%s: %s",
            request_id,
            prefill_client["worker_id"],
            decode_client["worker_id"],
            exc,
        )
        raise HTTPException(
            status_code=502,
            detail=f"decode request missing usable KV transfer params: {exc}",
        ) from exc

    decode_wire_api = api
    adapt_completion_to_chat = False
    try:
        (
            decode_wire_api,
            decode_payload,
            adapt_completion_to_chat,
            _decode_token_envelope,
        ) = await _prepare_versioned_decode_dispatch(
            config=config,
            api=api,
            request_body=decode_payload,
            prefill_response=prefill_json,
            prefill_client=prefill_client,
            decode_client=decode_client,
            kv_transfer_params=dict(
                decode_payload.get("kv_transfer_params") or {}
            ),
            ctx=ctx,
        )
        for timing_key, timing_value in _get_proxy_timings(
            decode_payload
        ).items():
            _set_proxy_timing(req_data, timing_key, timing_value)
    except DecodeTokenEnvelopeError as exc:
        control_plane.rollback_handoff(ctx)
        control_plane.mark_stage_complete(
            "decode",
            decode_decision.worker_id,
            latency_ms=0.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(request_id)
        logger.warning(
            "EPD Decode token-envelope rejected request_id=%s "
            "prefill_worker=%s decode_worker=%s mode=%s: %s",
            request_id,
            prefill_client["worker_id"],
            decode_client["worker_id"],
            config.decode_dispatch_mode,
            exc,
        )
        raise HTTPException(
            status_code=502,
            detail=f"decode token-envelope rejected: {exc}",
        ) from exc

    response_headers = {
        "X-Request-Id": request_id,
        "X-EPD-Routing-Path": ctx.routing_path,
        "X-EPD-Admission": decode_decision.decision.action.value,
        "X-EPD-Degrade-Level": ctx.degrade_level.value,
        # Aggregate counters cannot prove that a multi-worker scale-out run
        # exercised more than one replica.  Return the selected owners so raw
        # benchmark artifacts can report per-stage distribution.
        "X-EPD-Prefill-Worker": str(prefill_client["worker_id"]),
        "X-EPD-Decode-Worker": str(decode_client["worker_id"]),
        "X-EPD-Timing-Ms": _timing_header(req_data),
    }
    if bool(req_data.get("stream")):
        return await _dispatch_streaming_decode(
            api=api,
            control_plane=control_plane,
            ctx=ctx,
            decode_client=decode_client,
            decode_payload=decode_payload,
            decode_headers=_forward_headers(request, request_id),
            response_headers=response_headers,
            continuation=prefill_continuation,
            request_started_at=request_started_at,
            wire_api=decode_wire_api,
            adapt_completion_to_chat=adapt_completion_to_chat,
        )
    return await _dispatch_non_streaming_decode(
        api=api,
        control_plane=control_plane,
        ctx=ctx,
        decode_client=decode_client,
        decode_payload=decode_payload,
        decode_headers=_forward_headers(request, request_id),
        response_headers=response_headers,
        continuation=prefill_continuation,
        wire_api=decode_wire_api,
        adapt_completion_to_chat=adapt_completion_to_chat,
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
        if (
            _has_prefill_direct_buffer_service(config)
            and not config.release_direct_feature_buffers_after_prefill
        ):
            raw_handles = _lookup_proxy_cached_direct_feature_handles(
                app=app,
                feature_ids=[str(item) for item in list(getattr(ctx, "mm_hashes", []) or [])],
                target_worker_id=target_worker_id,
            )
        if (not isinstance(raw_handles, list) or not raw_handles) and _has_prefill_direct_buffer_service(config):
            raw_handles = await _lookup_prefill_cached_direct_feature_handles(
                app=app,
                feature_ids=[str(item) for item in list(getattr(ctx, "mm_hashes", []) or [])],
                target_worker_id=target_worker_id,
            )
        if not isinstance(raw_handles, list) or not raw_handles:
            raw_handles = await _request_feature_handles_from_encoder_service_singleflight(
                app=app,
                req_data=req_data,
                target_worker_id=target_worker_id,
                feature_ids=[str(item) for item in list(getattr(ctx, "mm_hashes", []) or [])],
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
    _store_proxy_direct_feature_handles(app, handle_payloads, target_worker_id=target_worker_id)
    # Only metadata and KV-transfer metadata are rewritten below.  Avoid a
    # JSON round-trip over large image/audio data URLs on every cache hit.
    rewritten = dict(req_data)
    metadata = dict(rewritten.get("metadata") or {})
    metadata["mooncake_epd_feature_handles"] = handle_payloads
    metadata["mooncake_epd_feature_handle_target_worker"] = target_worker_id
    if handles:
        direct_timings = dict(handles[0].metadata.get("proxy_direct_timings_ms") or {})
        if direct_timings:
            timings = dict(metadata.get("mooncake_epd_proxy_timings_ms") or {})
            timings.update(direct_timings)
            metadata["mooncake_epd_proxy_timings_ms"] = timings
    rewritten["metadata"] = metadata
    kv = dict(rewritten.get("kv_transfer_params") or {})
    kv["mm_prefetch_policy"] = "feature_handle"
    kv["mm_feature_handles"] = handle_payloads
    kv["mm_feature_handle_target_worker"] = target_worker_id
    rewritten["kv_transfer_params"] = kv
    return rewritten


def _singleflight_stats(app: FastAPI) -> Dict[str, Any]:
    stats = getattr(app.state, "direct_feature_handle_singleflight_stats", None)
    if not isinstance(stats, dict):
        stats = {"created": 0, "joined": 0, "evicted": 0, "active": 0}
        app.state.direct_feature_handle_singleflight_stats = stats
    return stats


def _singleflight_flights(app: FastAPI) -> Dict[Any, asyncio.Future[bool]]:
    """Return only live cold-publish completion fences.

    A lock is necessary while a missing FeatureBundle is allocated and
    published, but it is harmful after that point: serializing every follower
    through the same lock also serializes the now-independent direct-buffer
    lease lookups.  A completion future lets followers wait for the cold
    transaction once and then acquire their own generation-fenced reference in
    parallel.

    The proxy runs this bookkeeping on one asyncio event loop, so creating and
    inserting a future without an ``await`` is an atomic leader election for a
    key.  Futures are removed immediately on completion, so cardinality is
    naturally bounded by concurrent cold publishes rather than historical
    image keys.
    """

    flights = getattr(app.state, "direct_feature_handle_inflight_flights", None)
    if not isinstance(flights, dict):
        flights = {}
        app.state.direct_feature_handle_inflight_flights = flights
    return flights


async def _lookup_existing_direct_feature_handles(
    *,
    app: FastAPI,
    feature_ids: Sequence[str],
    target_worker_id: str,
) -> List[Dict[str, Any]]:
    """Return a live direct handle while acquiring this request's lease.

    The retained proxy cache is valid only when callers deliberately retain
    Prefill buffers.  The normal release-after-Prefill path always reaches the
    Prefill registry so every request owns an independent reference.  This
    helper centralizes that distinction for both the normal fast path and
    single-flight followers.
    """

    config: ProxyConfig = app.state.proxy_config
    raw_handles: List[Dict[str, Any]] = []
    if not config.release_direct_feature_buffers_after_prefill:
        raw_handles = _lookup_proxy_cached_direct_feature_handles(
            app=app,
            feature_ids=list(feature_ids),
            target_worker_id=target_worker_id,
        )
    if not raw_handles:
        raw_handles = await _lookup_prefill_cached_direct_feature_handles(
            app=app,
            feature_ids=list(feature_ids),
            target_worker_id=target_worker_id,
        )
    return [dict(item) for item in raw_handles] if raw_handles else []


async def _request_feature_handles_from_encoder_service_singleflight(
    *,
    app: FastAPI,
    req_data: Dict[str, Any],
    target_worker_id: str,
    feature_ids: Sequence[str],
) -> List[Dict[str, Any]]:
    """Request online feature handles with per-target single-flight de-dup.

    Concurrent same-image requests are common in agent/multimodal workloads.
    Without this guard, every request can allocate/publish the same Prefill
    direct buffer, while non-publishers wait on ``ready`` and may time out
    behind a slow duplicate publish.  The first request for a
    ``(prefill_worker, feature_ids)`` tuple performs E→P direct publish;
    followers wait only for that cold transaction, then concurrently acquire
    their own Prefill-registry lease.  The latter is essential for C16-style
    bursts: a ready buffer does not need serial reuse merely because its first
    publication did.
    """

    config: ProxyConfig = app.state.proxy_config
    ids = tuple(str(item) for item in feature_ids if str(item or "").strip())
    if not ids or not _has_prefill_direct_buffer_service(config):
        return await _request_feature_handles_from_encoder_service(
            app=app,
            req_data=req_data,
            target_worker_id=target_worker_id,
        )
    key = (str(target_worker_id), ids)
    stats = _singleflight_stats(app)
    flights = _singleflight_flights(app)

    while True:
        flight = flights.get(key)
        if flight is not None:
            # Shield the shared publication from an individual client
            # cancellation.  A canceled follower must not abort the one
            # transaction on which other requests still rely.
            stats["joined"] = int(stats.get("joined", 0) or 0) + 1
            await asyncio.shield(flight)
            hot_handles = await _lookup_existing_direct_feature_handles(
                app=app,
                feature_ids=ids,
                target_worker_id=target_worker_id,
            )
            if hot_handles:
                return hot_handles
            # A failed/canceled leader publishes ``False`` and a successful
            # leader should make the registry ready before resolving its
            # future.  If neither yielded a handle (for example a buffer was
            # immediately evicted), retry leader election instead of reusing a
            # stale pointer or silently skipping a required reference.
            continue

        completion: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        # No await occurs between lookup and insertion, so this task is the
        # sole leader for this cold descriptor tuple on the proxy event loop.
        flights[key] = completion
        stats["created"] = int(stats.get("created", 0) or 0) + 1
        try:
            # Re-check after leader election. Another request may have made a
            # direct allocation ready between the caller's first cache probe
            # and this function, and it is always cheaper/safer to acquire a
            # fresh registry reference than to publish the same bytes again.
            hot_handles = await _lookup_existing_direct_feature_handles(
                app=app,
                feature_ids=ids,
                target_worker_id=target_worker_id,
            )
            if hot_handles:
                # Publish the retained-cache view before waking followers.
                # The outer request preparation repeats this idempotently, but
                # ordering it here prevents a release-disabled follower from
                # taking an unnecessary registry reference in the tiny gap
                # between this function returning and the outer cache store.
                _store_proxy_direct_feature_handles(
                    app,
                    hot_handles,
                    target_worker_id=target_worker_id,
                )
                completion.set_result(True)
                return hot_handles
            handles = await _request_feature_handles_from_encoder_service(
                app=app,
                req_data=req_data,
                target_worker_id=target_worker_id,
            )
            # A follower must observe the completed publication through the
            # same generation/lifetime contract as the leader.  Store before
            # resolving the fence so release-disabled deployments use the
            # retained proxy handle rather than leaking an extra registry ref.
            _store_proxy_direct_feature_handles(
                app,
                handles,
                target_worker_id=target_worker_id,
            )
            completion.set_result(True)
            return handles
        except BaseException:
            # Followers re-probe after the fence resolves. Returning a status
            # rather than storing an exception prevents an abandoned Future
            # from emitting an unobserved-exception warning during shutdown.
            if not completion.done():
                completion.set_result(False)
            raise
        finally:
            if flights.get(key) is completion:
                flights.pop(key, None)


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
    # This request is serialized by httpx immediately; no nested user content
    # is mutated, so a top-level copy avoids duplicating multimodal payloads.
    payload = dict(req_data)
    metadata = dict(payload.get("metadata") or {})
    metadata["mooncake_epd_target_worker_id"] = target_worker_id
    payload["metadata"] = metadata
    if _has_prefill_direct_buffer_service(config):
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


def _lookup_proxy_cached_direct_feature_handles(
    *,
    app: FastAPI,
    feature_ids: Sequence[str],
    target_worker_id: str,
) -> List[Dict[str, Any]]:
    config: ProxyConfig = app.state.proxy_config
    if not config.enable_direct_feature_handle_cache:
        return []
    ids = [str(item) for item in feature_ids if str(item or "").strip()]
    if not ids:
        return []
    cache = getattr(app.state, "direct_feature_handle_cache", None)
    if not isinstance(cache, dict):
        return []
    stats = _direct_feature_handle_cache_stats(app)
    started = time.perf_counter()
    handles: List[Dict[str, Any]] = []
    now = time.monotonic()
    for fid in ids:
        cache_key = _direct_cache_key(target_worker_id, fid)
        cached_entry = cache.get(cache_key)
        if not isinstance(cached_entry, dict):
            stats["misses"] = int(stats.get("misses", 0) or 0) + 1
            return []
        cached = dict(cached_entry.get("handle") or {}) if "handle" in cached_entry else dict(cached_entry)
        stored_at = float(cached_entry.get("stored_at", now) or now) if "handle" in cached_entry else now
        ttl_s = float(getattr(config, "direct_feature_handle_cache_ttl_s", 600.0) or 0.0)
        if ttl_s > 0.0 and now - stored_at > ttl_s:
            cache.pop(cache_key, None)
            stats["expired"] = int(stats.get("expired", 0) or 0) + 1
            stats["misses"] = int(stats.get("misses", 0) or 0) + 1
            stats["entries"] = len(cache)
            return []
        if isinstance(cache, OrderedDict):
            cache.move_to_end(cache_key)
        elif isinstance(cached_entry, dict) and "handle" in cached_entry:
            cached_entry["last_used_at"] = now
        item = _clone_direct_feature_handle(cached)
        md = dict(item.get("metadata") or {})
        timings = {
            "direct_proxy_cache_lookup_ms": (time.perf_counter() - started) * 1000.0,
            "direct_proxy_cache_hits": float(len(ids)),
            "direct_cache_lookup_ms": 0.0,
            "direct_describe_ms": 0.0,
            "direct_allocate_ms": 0.0,
            "direct_publish_ms": 0.0,
        }
        md["proxy_direct_timings_ms"] = timings
        md["direct_backend"] = "prefill_proxy_handle_cache"
        item["metadata"] = md
        handles.append(item)
    stats["hits"] = int(stats.get("hits", 0) or 0) + len(ids)
    stats["entries"] = len(cache)
    return handles


def _store_proxy_direct_feature_handles(app: FastAPI, handles: Sequence[Dict[str, Any]], *, target_worker_id: str) -> None:
    config: ProxyConfig = app.state.proxy_config
    if not config.enable_direct_feature_handle_cache:
        return
    cache = getattr(app.state, "direct_feature_handle_cache", None)
    if not isinstance(cache, dict):
        return
    stats = _direct_feature_handle_cache_stats(app)
    max_entries = max(0, int(getattr(config, "direct_feature_handle_cache_max_entries", 4096) or 0))
    if max_entries == 0:
        if cache:
            stats["evictions"] = int(stats.get("evictions", 0) or 0) + len(cache)
            cache.clear()
            stats["entries"] = 0
        return
    now = time.monotonic()
    for handle in handles:
        if not isinstance(handle, dict):
            continue
        uri = str(handle.get("uri") or "")
        feature_id = str(handle.get("feature_id") or "")
        metadata = dict(handle.get("metadata") or {})
        if not feature_id or not uri.startswith("epd-direct://"):
            continue
        if not metadata.get("direct_plan") or not metadata.get("direct_remote_session"):
            continue
        cached = _clone_direct_feature_handle(handle)
        cached_md = dict(cached.get("metadata") or {})
        cached_md["mooncake_epd_target_worker_id"] = str(target_worker_id)
        cached_md.pop("proxy_direct_timings_ms", None)
        cached["metadata"] = cached_md
        cache_key = _direct_cache_key(target_worker_id, feature_id)
        cache[cache_key] = {
            "handle": cached,
            "stored_at": now,
            "last_used_at": now,
        }
        if isinstance(cache, OrderedDict):
            cache.move_to_end(cache_key)
        stats["stores"] = int(stats.get("stores", 0) or 0) + 1
    # Capacity eviction is O(1) with the OrderedDict. TTL expiry can require a
    # full scan, so perform it at a bounded cadence rather than once per handle
    # on every multimodal request.
    _prune_proxy_direct_feature_handle_cache(app, now=now)
    stats["entries"] = len(cache)


def _clone_direct_feature_handle(handle: Dict[str, Any]) -> Dict[str, Any]:
    """Copy only mutable FeatureHandle control-plane containers.

    Direct handles carry descriptors and pointer plans, not image bytes. A
    JSON round-trip was nevertheless used here and in the cache-hit path. It
    added serialization work to every cached multimodal request and is unsafe
    for future non-JSON metadata. Keep the clone narrow and explicit instead.
    """

    cloned = dict(handle)
    descriptor = handle.get("descriptor")
    if isinstance(descriptor, dict):
        descriptor_copy = dict(descriptor)
        if isinstance(descriptor.get("metadata"), dict):
            descriptor_copy["metadata"] = dict(descriptor["metadata"])
        if isinstance(descriptor.get("intermediates"), list):
            descriptor_copy["intermediates"] = [
                dict(item) if isinstance(item, dict) else item
                for item in descriptor["intermediates"]
            ]
        cloned["descriptor"] = descriptor_copy
    metadata = handle.get("metadata")
    if isinstance(metadata, dict):
        metadata_copy = dict(metadata)
        direct_plan = metadata.get("direct_plan")
        if isinstance(direct_plan, dict):
            direct_plan_copy = dict(direct_plan)
            if isinstance(direct_plan.get("targets"), list):
                direct_plan_copy["targets"] = [
                    dict(item) if isinstance(item, dict) else item
                    for item in direct_plan["targets"]
                ]
            metadata_copy["direct_plan"] = direct_plan_copy
        timings = metadata.get("proxy_direct_timings_ms")
        if isinstance(timings, dict):
            metadata_copy["proxy_direct_timings_ms"] = dict(timings)
        cloned["metadata"] = metadata_copy
    return cloned


def _direct_feature_handle_cache_stats(app: FastAPI) -> Dict[str, Any]:
    stats = getattr(app.state, "direct_feature_handle_cache_stats", None)
    if not isinstance(stats, dict):
        stats = {
            "hits": 0,
            "misses": 0,
            "stores": 0,
            "evictions": 0,
            "expired": 0,
            "ttl_sweeps": 0,
            "ttl_sweep_entries_scanned": 0,
            "entries": 0,
        }
        app.state.direct_feature_handle_cache_stats = stats
    return stats


def _prune_proxy_direct_feature_handle_cache(
    app: FastAPI,
    *,
    now: Optional[float] = None,
    force_ttl_sweep: bool = False,
) -> None:
    config: ProxyConfig = app.state.proxy_config
    cache = getattr(app.state, "direct_feature_handle_cache", None)
    if not isinstance(cache, dict):
        return
    stats = _direct_feature_handle_cache_stats(app)
    max_entries = max(0, int(getattr(config, "direct_feature_handle_cache_max_entries", 4096) or 0))
    ttl_s = max(0.0, float(getattr(config, "direct_feature_handle_cache_ttl_s", 600.0) or 0.0))
    now = time.monotonic() if now is None else float(now)
    next_ttl_sweep_at = float(
        getattr(app.state, "direct_feature_handle_cache_next_ttl_sweep_at", 0.0) or 0.0
    )
    should_sweep_ttl = ttl_s > 0.0 and (force_ttl_sweep or now >= next_ttl_sweep_at)
    if should_sweep_ttl:
        # Keep expiry bounded without rescanning up to 4096 cached handles on
        # every cache write. Per-key lookup still rejects an expired handle
        # immediately, so this cadence only affects stale-memory reclamation.
        sweep_interval_s = min(10.0, max(0.25, ttl_s / 16.0))
        app.state.direct_feature_handle_cache_next_ttl_sweep_at = now + sweep_interval_s
        stats["ttl_sweeps"] = int(stats.get("ttl_sweeps", 0) or 0) + 1
        stats["ttl_sweep_entries_scanned"] = int(
            stats.get("ttl_sweep_entries_scanned", 0) or 0
        ) + len(cache)
        expired_keys = []
        for key, entry in cache.items():
            if not isinstance(entry, dict):
                continue
            stored_at = float(entry.get("stored_at", now) or now) if "handle" in entry else now
            if now - stored_at > ttl_s:
                expired_keys.append(key)
        for key in expired_keys:
            if cache.pop(key, None) is not None:
                stats["expired"] = int(stats.get("expired", 0) or 0) + 1
    if max_entries <= 0:
        if cache:
            stats["evictions"] = int(stats.get("evictions", 0) or 0) + len(cache)
            cache.clear()
        stats["entries"] = 0
        return
    while len(cache) > max_entries:
        if isinstance(cache, OrderedDict):
            cache.popitem(last=False)
        else:
            victim = min(
                cache.items(),
                key=lambda item: float(
                    dict(item[1]).get("last_used_at", dict(item[1]).get("stored_at", 0.0))
                    if isinstance(item[1], dict)
                    else 0.0
                ),
            )[0]
            cache.pop(victim, None)
        stats["evictions"] = int(stats.get("evictions", 0) or 0) + 1
    stats["entries"] = len(cache)


_RENDERED_PREFILL_CACHE_IGNORED_METADATA_KEYS = frozenset(
    {
        # Populated by this proxy while a request is in flight. They are
        # observability-only and would make an otherwise identical request
        # miss the cache on every retry/repeat.
        "mooncake_epd_proxy_timings_ms",
        "proxy_direct_timings_ms",
        # Per-request tracing/routing identities are consumed by this proxy
        # and the Mooncake control plane, never by vLLM's OpenAI /render
        # endpoint. Benchmark and production callers intentionally assign a
        # distinct workflow id to every handoff.
        "workflow_id",
        "conversation_id",
        "session_id",
        # Canonical dataset replays attach these fields solely so the raw
        # benchmark artifact can identify the selected source row and replay
        # iteration.  They are neither OpenAI render inputs nor EPD routing
        # controls.  Including the cycle index made an otherwise immutable
        # repeated request miss the rendered-prompt cache on every iteration,
        # reintroducing a costly /render call on the TTFT path.
        "benchmark_source_workflow_id",
        "benchmark_dataset_cycle_index",
        "benchmark_dataset_source_index",
    }
)
# These are proxy/connector control-plane fields, not OpenAI render inputs.
# The cached object is only the output of ``/render``; fresh FeatureHandles and
# fresh P->D KV transfer metadata are attached to its ``/generate`` request on
# every dispatch. Keeping transient handle IDs/timings in the cache key would
# prevent reuse after a Prefill persistent direct-buffer lookup even when the
# user-visible image and prompt are identical.
_RENDERED_PREFILL_CACHE_IGNORED_CONTROL_KEYS = frozenset(
    {
        # vLLM's cache_salt changes prefix-cache identity, not tokenizer/
        # render semantics. The cached object is a rendered prompt; the fresh
        # salt is injected into every /generate dispatch below. Ignoring it
        # here lets a cache-isolation ablation retain the independent /render
        # cache instead of accidentally changing two cache layers at once.
        "cache_salt",
        "mooncake_epd_feature_handles",
        "feature_handles",
        "mm_feature_handles",
        "mooncake_epd_feature_handle_target_worker",
        "mm_feature_handle_target_worker",
        "mooncake_epd_target_worker_id",
        "target_worker_id",
    }
)
_MUTABLE_EXTERNAL_URI_PREFIXES = ("http://", "https://", "file://", "ftp://")


def _rendered_prefill_cache_stats(app: FastAPI) -> Dict[str, Any]:
    stats = getattr(app.state, "rendered_prefill_cache_stats", None)
    if not isinstance(stats, dict):
        stats = {
            "lookups": 0,
            "hits": 0,
            "misses": 0,
            "stores": 0,
            "evictions": 0,
            "expired": 0,
            "oversize_skips": 0,
            "bypasses": 0,
            "mutable_media_bypasses": 0,
            "unfenced_worker_bypasses": 0,
            "uncacheable_request_bypasses": 0,
            "ttl_sweeps": 0,
            "ttl_sweep_entries_scanned": 0,
            "entries": 0,
            "bytes": 0,
        }
        app.state.rendered_prefill_cache_stats = stats
    return stats


def _rendered_prefill_cache_bytes(cache: Any) -> int:
    if not isinstance(cache, dict):
        return 0
    total = 0
    for entry in cache.values():
        if not isinstance(entry, dict):
            continue
        try:
            total += max(0, int(entry.get("size_bytes", 0) or 0))
        except (TypeError, ValueError):
            continue
    return total


def _rendered_prefill_cache_worker_generation(
    app: FastAPI,
    prefill_client: Dict[str, Any],
) -> str:
    """Return the Prefill incarnation fence that scopes rendered token ids.

    A cached ``/render`` response contains token ids and serialized multimodal
    feature descriptors. Reusing it after a worker restart with a different
    tokenizer/model would be unsafe, so this cache deliberately requires the
    same generation fence already used by strict P->D manifests.
    """

    worker_id = str(prefill_client.get("worker_id") or "").strip()
    direct_generation = str(
        prefill_client.get("worker_generation")
        or prefill_client.get("generation")
        or ""
    ).strip()
    if direct_generation:
        return direct_generation
    control_plane = getattr(app.state, "control_plane", None)
    resolver = getattr(control_plane, "_generation_for_worker", None)
    if callable(resolver) and worker_id:
        try:
            resolved = str(resolver(worker_id) or "").strip()
            if resolved:
                return resolved
        except Exception:
            # An unavailable generation fence is a cache bypass, not a serving
            # failure. The ordinary strict render path remains valid.
            logger.debug("failed to resolve Prefill generation for render cache", exc_info=True)
    # Tests and embedders may provide a custom ServingControlPlane whose
    # configuration is intentionally narrower than the ProxyConfig. Preserve
    # the explicit ProxyConfig fence as a safe static fallback.
    config: ProxyConfig = app.state.proxy_config
    for configured_worker, configured_generation in list(config.worker_generations or []):
        if str(configured_worker) == worker_id and str(configured_generation).strip():
            return str(configured_generation).strip()
    return ""


def _project_rendered_prefill_cache_value(value: Any) -> Any:
    """Build a bounded exact-content cache-key projection.

    Large data URLs are represented by a full SHA-256 digest instead of being
    serialized into a second huge JSON document. This preserves equality for
    practical purposes while keeping the lookup off the critical path as much
    as possible. Mutable external URIs are rejected altogether: vLLM may
    refetch them during render and the same URL can legitimately change.
    """

    if isinstance(value, str):
        lowered = value.lstrip().lower()
        if lowered.startswith(_MUTABLE_EXTERNAL_URI_PREFIXES):
            raise ValueError("mutable_media")
        if len(value) > 1024:
            encoded = value.encode("utf-8")
            return {
                "__mooncake_epd_sha256__": hashlib.sha256(encoded).hexdigest(),
                "__mooncake_epd_length__": len(value),
            }
        return value
    if isinstance(value, list):
        return [_project_rendered_prefill_cache_value(item) for item in value]
    if isinstance(value, tuple):
        return [_project_rendered_prefill_cache_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _project_rendered_prefill_cache_value(item)
            for key, item in value.items()
            if str(key) not in _RENDERED_PREFILL_CACHE_IGNORED_METADATA_KEYS
            and str(key) not in _RENDERED_PREFILL_CACHE_IGNORED_CONTROL_KEYS
        }
    return value


def _rendered_prefill_cache_key(
    *,
    app: FastAPI,
    api: str,
    prefill_client: Dict[str, Any],
    request_body: Dict[str, Any],
) -> tuple[Optional[str], str]:
    """Return a cache key or a concrete bypass reason.

    The key is scoped to API route, logical Prefill worker and its published
    generation fence. It includes all semantically relevant request fields;
    only proxy-generated timing metadata is stripped.
    """

    config: ProxyConfig = app.state.proxy_config
    if not config.enable_rendered_prefill_cache:
        return None, "disabled"
    generation = _rendered_prefill_cache_worker_generation(app, prefill_client)
    if not generation:
        return None, "unfenced_worker"
    try:
        projection = _project_rendered_prefill_cache_value(request_body)
        canonical = json.dumps(
            {
                "api": str(api),
                "prefill_worker_id": str(prefill_client.get("worker_id") or ""),
                "prefill_worker_generation": generation,
                "request": projection,
            },
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except ValueError as exc:
        if str(exc) == "mutable_media":
            return None, "mutable_media"
        return None, "uncacheable_request"
    except (TypeError, OverflowError):
        return None, "uncacheable_request"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest(), "eligible"


def _note_rendered_prefill_cache_bypass(app: FastAPI, reason: str) -> None:
    if reason == "disabled":
        return
    stats = _rendered_prefill_cache_stats(app)
    stats["bypasses"] = int(stats.get("bypasses", 0) or 0) + 1
    if reason == "mutable_media":
        stats["mutable_media_bypasses"] = int(
            stats.get("mutable_media_bypasses", 0) or 0
        ) + 1
    elif reason == "unfenced_worker":
        stats["unfenced_worker_bypasses"] = int(
            stats.get("unfenced_worker_bypasses", 0) or 0
        ) + 1
    else:
        stats["uncacheable_request_bypasses"] = int(
            stats.get("uncacheable_request_bypasses", 0) or 0
        ) + 1


def _lookup_rendered_prefill_cache(
    app: FastAPI,
    cache_key: str,
) -> Optional[Dict[str, Any]]:
    config: ProxyConfig = app.state.proxy_config
    if not config.enable_rendered_prefill_cache:
        return None
    cache = getattr(app.state, "rendered_prefill_cache", None)
    if not isinstance(cache, dict):
        return None
    stats = _rendered_prefill_cache_stats(app)
    stats["lookups"] = int(stats.get("lookups", 0) or 0) + 1
    entry = cache.get(cache_key)
    if not isinstance(entry, dict) or not isinstance(entry.get("payload"), dict):
        stats["misses"] = int(stats.get("misses", 0) or 0) + 1
        return None
    now = time.monotonic()
    ttl_s = max(
        0.0,
        float(getattr(config, "rendered_prefill_cache_ttl_s", 300.0) or 0.0),
    )
    stored_at = float(entry.get("stored_at", now) or now)
    if ttl_s > 0.0 and now - stored_at > ttl_s:
        cache.pop(cache_key, None)
        stats["expired"] = int(stats.get("expired", 0) or 0) + 1
        stats["misses"] = int(stats.get("misses", 0) or 0) + 1
        stats["entries"] = len(cache)
        stats["bytes"] = _rendered_prefill_cache_bytes(cache)
        return None
    entry["last_used_at"] = now
    if isinstance(cache, OrderedDict):
        cache.move_to_end(cache_key)
    stats["hits"] = int(stats.get("hits", 0) or 0) + 1
    stats["entries"] = len(cache)
    stats["bytes"] = _rendered_prefill_cache_bytes(cache)
    # ``_inject_prefill_kv_into_rendered_request`` mutates nested sampling
    # parameters. Never hand the cache-owned object to a request.
    return deepcopy(dict(entry["payload"]))


def _store_rendered_prefill_cache(
    app: FastAPI,
    *,
    cache_key: str,
    rendered_payload: Dict[str, Any],
    size_bytes: int,
) -> None:
    config: ProxyConfig = app.state.proxy_config
    if not config.enable_rendered_prefill_cache:
        return
    cache = getattr(app.state, "rendered_prefill_cache", None)
    if not isinstance(cache, dict):
        return
    stats = _rendered_prefill_cache_stats(app)
    max_entries = max(
        0,
        int(getattr(config, "rendered_prefill_cache_max_entries", 64) or 0),
    )
    max_bytes = max(
        0,
        int(getattr(config, "rendered_prefill_cache_max_bytes", 256 * 1024 * 1024) or 0),
    )
    if max_entries <= 0 or max_bytes <= 0:
        _prune_rendered_prefill_cache(app, force_ttl_sweep=False)
        return
    entry_bytes = max(0, int(size_bytes or 0))
    if entry_bytes <= 0:
        # httpx normally exposes the already-decoded response bytes. Keep a
        # conservative fallback for test transports that do not.
        try:
            entry_bytes = len(
                json.dumps(rendered_payload, ensure_ascii=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
        except (TypeError, ValueError, OverflowError):
            stats["oversize_skips"] = int(stats.get("oversize_skips", 0) or 0) + 1
            return
    if entry_bytes > max_bytes:
        stats["oversize_skips"] = int(stats.get("oversize_skips", 0) or 0) + 1
        return
    now = time.monotonic()
    cache[cache_key] = {
        "payload": deepcopy(rendered_payload),
        "size_bytes": entry_bytes,
        "stored_at": now,
        "last_used_at": now,
    }
    if isinstance(cache, OrderedDict):
        cache.move_to_end(cache_key)
    stats["stores"] = int(stats.get("stores", 0) or 0) + 1
    _prune_rendered_prefill_cache(app, now=now)


def _prune_rendered_prefill_cache(
    app: FastAPI,
    *,
    now: Optional[float] = None,
    force_ttl_sweep: bool = False,
) -> None:
    config: ProxyConfig = app.state.proxy_config
    cache = getattr(app.state, "rendered_prefill_cache", None)
    if not isinstance(cache, dict):
        return
    stats = _rendered_prefill_cache_stats(app)
    max_entries = max(
        0,
        int(getattr(config, "rendered_prefill_cache_max_entries", 64) or 0),
    )
    max_bytes = max(
        0,
        int(getattr(config, "rendered_prefill_cache_max_bytes", 256 * 1024 * 1024) or 0),
    )
    ttl_s = max(
        0.0,
        float(getattr(config, "rendered_prefill_cache_ttl_s", 300.0) or 0.0),
    )
    now = time.monotonic() if now is None else float(now)
    next_ttl_sweep_at = float(
        getattr(app.state, "rendered_prefill_cache_next_ttl_sweep_at", 0.0) or 0.0
    )
    should_sweep_ttl = ttl_s > 0.0 and (force_ttl_sweep or now >= next_ttl_sweep_at)
    if should_sweep_ttl:
        # Rendered payloads can contain large base64 feature kwargs. Sweep at
        # a bounded cadence rather than scanning this cache per request.
        sweep_interval_s = min(10.0, max(0.25, ttl_s / 16.0))
        app.state.rendered_prefill_cache_next_ttl_sweep_at = now + sweep_interval_s
        stats["ttl_sweeps"] = int(stats.get("ttl_sweeps", 0) or 0) + 1
        stats["ttl_sweep_entries_scanned"] = int(
            stats.get("ttl_sweep_entries_scanned", 0) or 0
        ) + len(cache)
        expired_keys = []
        for key, entry in cache.items():
            if not isinstance(entry, dict):
                continue
            stored_at = float(entry.get("stored_at", now) or now)
            if now - stored_at > ttl_s:
                expired_keys.append(key)
        for key in expired_keys:
            if cache.pop(key, None) is not None:
                stats["expired"] = int(stats.get("expired", 0) or 0) + 1
    if max_entries <= 0 or max_bytes <= 0:
        if cache:
            stats["evictions"] = int(stats.get("evictions", 0) or 0) + len(cache)
            cache.clear()
        stats["entries"] = 0
        stats["bytes"] = 0
        return
    cache_bytes = _rendered_prefill_cache_bytes(cache)
    while cache and (len(cache) > max_entries or cache_bytes > max_bytes):
        if isinstance(cache, OrderedDict):
            _, entry = cache.popitem(last=False)
        else:
            victim_key, entry = min(
                cache.items(),
                key=lambda item: float(
                    dict(item[1]).get("last_used_at", dict(item[1]).get("stored_at", 0.0))
                    if isinstance(item[1], dict)
                    else 0.0
                ),
            )
            cache.pop(victim_key, None)
        try:
            cache_bytes -= max(0, int(dict(entry).get("size_bytes", 0) or 0))
        except (TypeError, ValueError):
            cache_bytes = _rendered_prefill_cache_bytes(cache)
        stats["evictions"] = int(stats.get("evictions", 0) or 0) + 1
    stats["entries"] = len(cache)
    stats["bytes"] = max(0, cache_bytes)


async def _request_direct_feature_handles_from_encoder_service(
    *,
    app: FastAPI,
    payload: Dict[str, Any],
    target_worker_id: str,
) -> List[Dict[str, Any]]:
    encoder_client: httpx.AsyncClient = app.state.encoder_client
    direct_client = _prefill_direct_buffer_client_for(app, target_worker_id)
    if direct_client is None:
        raise HTTPException(status_code=500, detail=f"no Prefill direct-buffer service configured for {target_worker_id}")
    timings: Dict[str, float] = {}
    ticket = ""
    allocated_feature_ids: List[str] = []
    return_items: List[Dict[str, Any]] = []
    transaction_complete = False
    try:
        started = time.perf_counter()
        described_resp = await encoder_client.post("/describe", json=payload)
        described_resp.raise_for_status()
        described = described_resp.json()
        timings["direct_describe_ms"] = (time.perf_counter() - started) * 1000.0
        descriptors = described.get("descriptors")
        ticket = str(described.get("ticket") or "")
        if not ticket or not isinstance(descriptors, list) or not descriptors:
            raise HTTPException(status_code=502, detail="encoder describe returned no direct descriptors/ticket")

        started = time.perf_counter()
        alloc_resp = await direct_client.post(
            "/mooncake_epd/direct_feature_buffer/allocate",
            json={
                "descriptors": descriptors,
                "target_worker_id": target_worker_id,
                "zero_fill": False,
                "reuse_ready": True,
            },
        )
        alloc_resp.raise_for_status()
        allocation = alloc_resp.json()
        timings["direct_allocate_ms"] = (time.perf_counter() - started) * 1000.0
        targets = allocation.get("targets")
        if not isinstance(targets, list) or len(targets) != len(descriptors):
            raise HTTPException(
                status_code=502,
                detail=(
                    "prefill direct allocation target count mismatch: "
                    f"targets={0 if not isinstance(targets, list) else len(targets)} descriptors={len(descriptors)}"
                ),
            )
        publish_mask = [bool(dict(target).get("publish_required", True)) for target in targets]
        allocated_feature_ids = [
            str(dict(target).get("feature_id") or "")
            for target in targets
            if str(dict(target).get("feature_id") or "")
        ]
        publish_payload = {
            "ticket": ticket,
            "metadata": dict(payload.get("metadata") or {}),
            "mooncake_epd_direct_feature_targets": targets,
            "mooncake_epd_direct_publish_mask": publish_mask,
        }
        started = time.perf_counter()
        publish_resp = await encoder_client.post("/publish_direct", json=publish_payload)
        publish_resp.raise_for_status()
        published = publish_resp.json()
        timings["direct_publish_ms"] = (time.perf_counter() - started) * 1000.0
        handles = published.get("handles")
        if not isinstance(handles, list) or len(handles) != len(targets):
            raise HTTPException(
                status_code=502,
                detail=(
                    "encoder direct publish handle count mismatch: "
                    f"handles={0 if not isinstance(handles, list) else len(handles)} "
                    f"targets={len(targets)}"
                ),
            )
        for index, (handle, target) in enumerate(zip(handles, targets)):
            if not isinstance(handle, dict):
                raise HTTPException(
                    status_code=502,
                    detail=f"encoder direct publish returned a non-object handle at index={index}",
                )
            expected_feature_id = str(dict(target).get("feature_id") or "")
            actual_feature_id = str(handle.get("feature_id") or "")
            if not expected_feature_id or actual_feature_id != expected_feature_id:
                raise HTTPException(
                    status_code=502,
                    detail=(
                        "encoder direct publish feature id mismatch at "
                        f"index={index}: expected={expected_feature_id!r} actual={actual_feature_id!r}"
                    ),
                )
        ready_feature_ids = [
            str(handle.get("feature_id") or "")
            for handle, should_publish in zip(handles, publish_mask)
            if should_publish and isinstance(handle, dict) and str(handle.get("feature_id") or "")
        ]
        wait_feature_ids = [
            str(handle.get("feature_id") or "")
            for handle, should_publish in zip(handles, publish_mask)
            if (not should_publish) and isinstance(handle, dict) and str(handle.get("feature_id") or "")
        ]

        async def _complete_readiness(
            endpoint: str,
            payload: Dict[str, Any],
            timing_key: str,
        ) -> Tuple[str, float]:
            started = time.perf_counter()
            response = await direct_client.post(endpoint, json=payload)
            response.raise_for_status()
            return timing_key, (time.perf_counter() - started) * 1000.0

        # A multi-item request can contain both a newly published feature and
        # a coalesced feature that is already being published by another
        # request. These readiness RPCs touch disjoint feature sets. Running
        # them concurrently removes one control-plane RTT from the E->P TTFT
        # path while preserving the transaction rule that *both* complete
        # before any handle is exposed to Prefill.
        readiness_tasks = []
        if ready_feature_ids:
            readiness_tasks.append(
                _complete_readiness(
                    "/mooncake_epd/direct_feature_buffer/mark_ready",
                    {"feature_ids": sorted(set(ready_feature_ids))},
                    "direct_mark_ready_ms",
                )
            )
        if wait_feature_ids:
            readiness_tasks.append(
                _complete_readiness(
                    "/mooncake_epd/direct_feature_buffer/wait_ready",
                    {
                        "feature_ids": sorted(set(wait_feature_ids)),
                        "timeout_s": max(
                            1.0,
                            float(
                                getattr(
                                    app.state.proxy_config,
                                    "prefill_direct_buffer_timeout_s",
                                    30.0,
                                )
                            ),
                        ),
                    },
                    "direct_wait_ready_ms",
                )
            )
        if readiness_tasks:
            for timing_key, elapsed_ms in await asyncio.gather(*readiness_tasks):
                timings[timing_key] = elapsed_ms

        # Validate and finish the proxy-side response before committing. A
        # published/ready buffer is still request-owned if its handle cannot
        # safely be handed to Prefill.
        for handle in handles:
            if not isinstance(handle, dict):
                continue
            md = dict(handle.get("metadata") or {})
            sub = md.get("direct_transfer_timings_ms")
            if not isinstance(sub, dict):
                continue
            # A multi-image E->P publication now shares one Mooncake batch.
            # Each FeatureHandle carries the batch wall time so a caller can
            # understand its readiness, but summing that shared timing once per
            # handle would over-report transfer time/bytes by the image count.
            # ``batch_index=0`` is the single request-level accounting record;
            # every independent remote-session batch also has its own index 0.
            try:
                batch_feature_count = float(sub.get("batch_feature_count", 1.0) or 1.0)
                batch_index = float(sub.get("batch_index", 0.0) or 0.0)
            except (TypeError, ValueError):
                batch_feature_count = 1.0
                batch_index = 0.0
            if batch_feature_count > 1.0 and batch_index > 0.0:
                continue
            for key, value in sub.items():
                try:
                    timings[f"direct_publish_engine_{key}"] = (
                        float(timings.get(f"direct_publish_engine_{key}", 0.0) or 0.0)
                        + float(value or 0.0)
                    )
                except Exception:
                    continue
        for handle in handles:
            item = dict(handle)
            if not str(item.get("uri") or "").startswith("epd-direct://"):
                raise HTTPException(status_code=502, detail="encoder direct publish returned non-direct handle")
            metadata = dict(item.get("metadata") or {})
            metadata["proxy_direct_timings_ms"] = dict(timings)
            metadata["mooncake_epd_target_worker_id"] = str(target_worker_id)
            metadata["target_worker_id"] = str(target_worker_id)
            item["metadata"] = metadata
            item["target_worker_id"] = str(target_worker_id)
            return_items.append(item)

        # Encoder publish alone does not commit the transaction.  The Prefill
        # allocation remains owned by this request until published targets are
        # ready, coalesced targets have observed readiness, and every returned
        # handle has passed proxy-side validation.
        transaction_complete = True
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:1000] if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=f"direct FeatureHandle transaction failed: {detail}") from exc
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(status_code=502, detail=f"direct FeatureHandle transaction failed: {exc}") from exc
    finally:
        if not transaction_complete:
            if ticket:
                try:
                    await encoder_client.post("/release_direct_ticket", json={"ticket": ticket})
                except Exception:
                    logger.warning("failed to release encoder direct ticket=%s", ticket, exc_info=True)
            if allocated_feature_ids:
                try:
                    await direct_client.post(
                        "/mooncake_epd/direct_feature_buffer/release",
                        # Each allocation call owns one reference. Do not
                        # deduplicate here: repeated feature IDs within one
                        # request legitimately increment the registry count.
                        json={"feature_ids": list(allocated_feature_ids)},
                    )
                except Exception:
                    logger.warning("failed to release Prefill direct features after transaction failure", exc_info=True)

    return return_items


async def _lookup_prefill_cached_direct_feature_handles(
    *,
    app: FastAPI,
    feature_ids: Sequence[str],
    target_worker_id: str,
) -> List[Dict[str, Any]]:
    """Return hot Prefill-side direct handles without touching Encoder.

    This is the zero-copy closed-loop fast path for repeated multimodal inputs:
    the Prefill API process already owns registered peer buffers from an earlier
    E→P publish, so the proxy only fetches descriptors/pointers and sends an
    ``epd-direct://`` handle.  EngineCore still reads the registered peer buffer
    through Mooncake; no Python bytes/object-store/file path is introduced.
    """

    config: ProxyConfig = app.state.proxy_config
    if not _has_prefill_direct_buffer_service(config):
        return []
    ids = [str(item) for item in feature_ids if str(item or "").strip()]
    if not ids:
        return []

    lease_count, max_entries, _ = _direct_feature_lease_prefetch_settings(config)
    adaptive_lease_prefetch, adaptive_lease_prefetch_max = (
        _direct_feature_adaptive_lease_prefetch_settings(config)
    )
    # A retained proxy Handle cache owns a different lifetime contract.  Lease
    # prefetch is specifically for the default release-after-Prefill mode,
    # where pre-reserving registry references is safe and old pointer handles
    # are not retained beyond their reference.
    if (
        not config.release_direct_feature_buffers_after_prefill
        or (lease_count <= 1 and not adaptive_lease_prefetch)
        or max_entries <= 0
    ):
        if lease_count > 1 or adaptive_lease_prefetch:
            stats = _direct_feature_lease_prefetch_stats(app)
            stats["disabled_bypasses"] = int(stats.get("disabled_bypasses", 0) or 0) + 1
        return await _lookup_prefill_cached_direct_feature_handles_once(
            app=app,
            ids=ids,
            target_worker_id=target_worker_id,
            lease_count=1,
        )

    generation = _prefill_worker_generation_for_direct_feature_lease_prefetch(
        app,
        target_worker_id,
    )
    if not generation:
        # A reference is safe only while the registered pointer belongs to the
        # same Prefill incarnation.  Fall back to the existing single lookup
        # rather than guessing a generation fence.
        stats = _direct_feature_lease_prefetch_stats(app)
        stats["generation_bypasses"] = int(stats.get("generation_bypasses", 0) or 0) + 1
        return await _lookup_prefill_cached_direct_feature_handles_once(
            app=app,
            ids=ids,
            target_worker_id=target_worker_id,
            lease_count=1,
        )

    return await _checkout_or_reserve_direct_feature_lease_prefetch(
        app=app,
        ids=tuple(ids),
        target_worker_id=str(target_worker_id),
        generation=generation,
        lease_count=lease_count,
        adaptive_lease_prefetch=adaptive_lease_prefetch,
        adaptive_lease_prefetch_max=adaptive_lease_prefetch_max,
    )


def _direct_feature_lease_prefetch_settings(
    config: ProxyConfig,
) -> Tuple[int, int, float]:
    return (
        min(1024, max(1, int(getattr(config, "direct_feature_lease_prefetch", 1) or 1))),
        max(
            0,
            int(
                getattr(config, "direct_feature_lease_prefetch_max_entries", 64)
                or 0
            ),
        ),
        max(
            0.0,
            float(
                getattr(config, "direct_feature_lease_prefetch_ttl_s", 30.0)
                or 0.0
            ),
        ),
    )


def _direct_feature_adaptive_lease_prefetch_settings(
    config: ProxyConfig,
) -> Tuple[bool, int]:
    """Return the safe request-coalescing mode for fixed one-lease serving.

    An explicit static reservation count greater than one remains authoritative;
    adaptive coalescing is intentionally limited to the historical fixed-one
    mode so deployment A/B settings retain their exact meaning.
    """

    fixed_lease_count = min(
        1024,
        max(1, int(getattr(config, "direct_feature_lease_prefetch", 1) or 1)),
    )
    return (
        fixed_lease_count == 1
        and bool(
            getattr(config, "direct_feature_adaptive_lease_prefetch", False)
        ),
        min(
            1024,
            max(
                1,
                int(
                    getattr(
                        config,
                        "direct_feature_adaptive_lease_prefetch_max",
                        2,
                    )
                    or 1
                ),
            ),
        ),
    )


def _direct_feature_lease_prefetch_stats(app: FastAPI) -> Dict[str, Any]:
    stats = getattr(app.state, "direct_feature_lease_prefetch_stats", None)
    if not isinstance(stats, dict):
        stats = {
            "lookups": 0,
            "pool_hits": 0,
            "pool_misses": 0,
            "reservations": 0,
            "reserved_references": 0,
            "adaptive_reservations": 0,
            "adaptive_reserved_references": 0,
            "adaptive_waiter_peak": 0,
            "checkouts": 0,
            "checkout_references": 0,
            "release_enqueued_references": 0,
            "release_failures": 0,
            "evictions": 0,
            "expired": 0,
            "generation_bypasses": 0,
            "disabled_bypasses": 0,
            "ttl_sweeps": 0,
            "ttl_sweep_entries_scanned": 0,
            "flushes": 0,
            "entries": 0,
            "held_leases": 0,
            "held_references": 0,
        }
        app.state.direct_feature_lease_prefetch_stats = stats
    return stats


def _direct_feature_lease_prefetch_pool_gauges(pool: Any) -> Dict[str, int]:
    if not isinstance(pool, dict):
        return {"entries": 0, "held_leases": 0, "held_references": 0}
    held_leases = 0
    held_references = 0
    for entry in pool.values():
        if not isinstance(entry, dict):
            continue
        remaining = max(0, int(entry.get("remaining_leases", 0) or 0))
        feature_count = len(list(entry.get("feature_ids") or ()))
        held_leases += remaining
        held_references += remaining * feature_count
    return {
        "entries": len(pool),
        "held_leases": held_leases,
        "held_references": held_references,
    }


def _prefill_worker_generation_for_direct_feature_lease_prefetch(
    app: FastAPI,
    target_worker_id: str,
) -> str:
    worker_id = str(target_worker_id or "").strip()
    for client in list(getattr(app.state, "prefill_clients", []) or []):
        if str(client.get("worker_id") or "") == worker_id:
            return _rendered_prefill_cache_worker_generation(app, dict(client))
    # The control plane can resolve configured/static fences even in narrow
    # unit-test embeddings that do not build the normal client list.
    return _rendered_prefill_cache_worker_generation(app, {"worker_id": worker_id})


def _direct_feature_lease_prefetch_key(
    target_worker_id: str,
    generation: str,
    feature_ids: Sequence[str],
) -> Tuple[str, str, Tuple[str, ...]]:
    return (
        str(target_worker_id),
        str(generation),
        tuple(str(feature_id) for feature_id in feature_ids),
    )


def _direct_feature_lease_prefetch_locks(app: FastAPI) -> Dict[Any, asyncio.Lock]:
    locks = getattr(app.state, "direct_feature_lease_prefetch_locks", None)
    if not isinstance(locks, dict):
        locks = {}
        app.state.direct_feature_lease_prefetch_locks = locks
    return locks


def _direct_feature_lease_prefetch_waiters(app: FastAPI) -> Dict[Any, int]:
    waiters = getattr(app.state, "direct_feature_lease_prefetch_waiters", None)
    if not isinstance(waiters, dict):
        waiters = {}
        app.state.direct_feature_lease_prefetch_waiters = waiters
    return waiters


def _clone_direct_feature_lease_handles(
    handles: Sequence[Dict[str, Any]],
    *,
    lease_count: int,
    checkout_ms: float,
) -> List[Dict[str, Any]]:
    copied: List[Dict[str, Any]] = []
    for source in handles:
        item = _clone_direct_feature_handle(dict(source))
        item["handle_id"] = (
            f"direct-lease-{str(item.get('feature_id') or 'feature')}-"
            f"{uuid.uuid4().hex}"
        )
        metadata = dict(item.get("metadata") or {})
        timings = dict(metadata.get("proxy_direct_timings_ms") or {})
        timings.update(
            {
                "direct_cache_lookup_ms": 0.0,
                "direct_cache_lease_checkout_ms": max(0.0, float(checkout_ms)),
                "direct_cache_lease_prefetch": float(lease_count),
                "direct_cache_hits": float(len(handles)),
                "direct_describe_ms": 0.0,
                "direct_allocate_ms": 0.0,
                "direct_publish_ms": 0.0,
            }
        )
        metadata["proxy_direct_timings_ms"] = timings
        metadata["direct_backend"] = "prefill_persistent_cache_lease_prefetch"
        item["metadata"] = metadata
        copied.append(item)
    return copied


def _take_direct_feature_lease_prefetch(
    *,
    app: FastAPI,
    key: Tuple[str, str, Tuple[str, ...]],
    lease_count: int,
    now: float,
) -> List[Dict[str, Any]]:
    pool = getattr(app.state, "direct_feature_lease_prefetch_pool", None)
    if not isinstance(pool, dict):
        return []
    entry = pool.get(key)
    if not isinstance(entry, dict) or int(entry.get("remaining_leases", 0) or 0) <= 0:
        return []
    entry["remaining_leases"] = int(entry.get("remaining_leases", 0) or 0) - 1
    entry["last_used_at"] = now
    if isinstance(pool, OrderedDict):
        pool.move_to_end(key)
    stats = _direct_feature_lease_prefetch_stats(app)
    stats["pool_hits"] = int(stats.get("pool_hits", 0) or 0) + 1
    stats["checkouts"] = int(stats.get("checkouts", 0) or 0) + 1
    stats["checkout_references"] = int(
        stats.get("checkout_references", 0) or 0
    ) + len(list(entry.get("feature_ids") or ()))
    reserved_lease_count = max(
        1, int(entry.get("lease_count", lease_count) or lease_count)
    )
    handles = _clone_direct_feature_lease_handles(
        list(entry.get("handles") or []),
        lease_count=reserved_lease_count,
        checkout_ms=0.0,
    )
    # A zero-lease entry carries no safe pointer lifetime. Remove it instead
    # of retaining descriptor state that could be mistaken for a cache hit.
    if int(entry.get("remaining_leases", 0) or 0) <= 0:
        pool.pop(key, None)
    stats.update(_direct_feature_lease_prefetch_pool_gauges(pool))
    return handles


async def _checkout_or_reserve_direct_feature_lease_prefetch(
    *,
    app: FastAPI,
    ids: Tuple[str, ...],
    target_worker_id: str,
    generation: str,
    lease_count: int,
    adaptive_lease_prefetch: bool,
    adaptive_lease_prefetch_max: int,
) -> List[Dict[str, Any]]:
    """Checkout a reserved direct-buffer reference or atomically reserve one.

    The pool stores no CUDA pointer ownership by itself; it merely accounts
    for registry references obtained in a single ``/lookup`` RPC.  Each
    successful request still releases exactly one reference per FeatureHandle
    through the normal Prefill-completion cleanup path.
    """

    await _prune_direct_feature_lease_prefetch_pool(app)
    now = time.monotonic()
    key = _direct_feature_lease_prefetch_key(target_worker_id, generation, ids)
    stats = _direct_feature_lease_prefetch_stats(app)
    stats["lookups"] = int(stats.get("lookups", 0) or 0) + 1
    cached = _take_direct_feature_lease_prefetch(
        app=app,
        key=key,
        lease_count=lease_count,
        now=now,
    )
    if cached:
        return cached

    waiters: Optional[Dict[Any, int]] = None
    if adaptive_lease_prefetch:
        waiters = _direct_feature_lease_prefetch_waiters(app)
        current_waiters = max(0, int(waiters.get(key, 0) or 0)) + 1
        waiters[key] = current_waiters
        stats["adaptive_waiter_peak"] = max(
            int(stats.get("adaptive_waiter_peak", 0) or 0), current_waiters
        )
    try:
        locks = _direct_feature_lease_prefetch_locks(app)
        lock = locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            locks[key] = lock
            # Locks are not resource leases; prune only unlocked old keys to keep
            # a high-cardinality image workload bounded without disrupting waiters.
            max_locks = max(16, int(getattr(app.state.proxy_config, "direct_feature_lease_prefetch_max_entries", 64) or 64) * 2)
            if len(locks) > max_locks:
                for stale_key, stale_lock in list(locks.items()):
                    if len(locks) <= max_locks:
                        break
                    if stale_key != key and not stale_lock.locked():
                        locks.pop(stale_key, None)
        async with lock:
            cached = _take_direct_feature_lease_prefetch(
                app=app,
                key=key,
                lease_count=lease_count,
                now=time.monotonic(),
            )
            if cached:
                return cached
            stats["pool_misses"] = int(stats.get("pool_misses", 0) or 0) + 1
            reserved_lease_count = lease_count
            if adaptive_lease_prefetch:
                # Give only tasks that are already runnable in this event-loop
                # turn a chance to join.  This adds no timer-based delay to a
                # serial request and avoids speculative multi-reference leases.
                await asyncio.sleep(0)
                reserved_lease_count = min(
                    max(1, int(adaptive_lease_prefetch_max)),
                    max(1, int((waiters or {}).get(key, 1) or 1)),
                )
            handles = await _lookup_prefill_cached_direct_feature_handles_once(
                app=app,
                ids=list(ids),
                target_worker_id=target_worker_id,
                lease_count=reserved_lease_count,
            )
            if not handles:
                return []
            # The request consumes one lease per FeatureHandle after Prefill.
            # Keep only the remaining references for concurrent/next identical
            # requests, and record the actual reservation count so checkout
            # timing reflects an adaptive multi-reference acquisition.
            remaining = max(0, int(reserved_lease_count) - 1)
            if adaptive_lease_prefetch and reserved_lease_count > 1:
                stats["adaptive_reservations"] = int(
                    stats.get("adaptive_reservations", 0) or 0
                ) + 1
                stats["adaptive_reserved_references"] = int(
                    stats.get("adaptive_reserved_references", 0) or 0
                ) + len(ids) * remaining
            if remaining <= 0:
                return handles
            pool = getattr(app.state, "direct_feature_lease_prefetch_pool", None)
            if not isinstance(pool, dict):
                # We successfully acquired the extra references, so compensating
                # release is mandatory if an embedder replaced the pool state.
                await _release_direct_feature_buffer_ids(
                    app,
                    {target_worker_id: list(ids) * remaining},
                )
                return handles
            now = time.monotonic()
            pool[key] = {
                "target_worker_id": target_worker_id,
                "generation": generation,
                "feature_ids": tuple(ids),
                "handles": [_clone_direct_feature_handle(item) for item in handles],
                "lease_count": reserved_lease_count,
                "remaining_leases": remaining,
                "reserved_at": now,
                "last_used_at": now,
            }
            if isinstance(pool, OrderedDict):
                pool.move_to_end(key)
            stats["reservations"] = int(stats.get("reservations", 0) or 0) + 1
            stats["reserved_references"] = int(
                stats.get("reserved_references", 0) or 0
            ) + len(ids) * remaining
            await _prune_direct_feature_lease_prefetch_pool(app, now=now)
            stats.update(_direct_feature_lease_prefetch_pool_gauges(pool))
            return handles
    finally:
        if waiters is not None:
            remaining_waiters = max(0, int(waiters.get(key, 0) or 0) - 1)
            if remaining_waiters:
                waiters[key] = remaining_waiters
            else:
                waiters.pop(key, None)


async def _lookup_prefill_cached_direct_feature_handles_once(
    *,
    app: FastAPI,
    ids: Sequence[str],
    target_worker_id: str,
    lease_count: int,
) -> List[Dict[str, Any]]:
    """Perform one Prefill registry lookup and acquire ``lease_count`` refs."""

    direct_client = _prefill_direct_buffer_client_for(app, target_worker_id)
    if direct_client is None:
        return []
    requested_leases = min(1024, max(1, int(lease_count)))
    started = time.perf_counter()
    try:
        resp = await direct_client.post(
            "/mooncake_epd/direct_feature_buffer/lookup",
            json={
                "feature_ids": list(ids),
                "target_worker_id": target_worker_id,
                "lease_count": requested_leases,
            },
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        logger.debug("prefill direct cache lookup failed", exc_info=True)
        return []
    try:
        returned_leases = int(payload.get("lease_count", 1) or 1)
    except (TypeError, ValueError):
        returned_leases = 1
    try:
        server_lookup_ms = float(payload.get("server_lookup_ms", 0.0) or 0.0)
    except (TypeError, ValueError):
        server_lookup_ms = 0.0
    if not math.isfinite(server_lookup_ms) or server_lookup_ms < 0.0:
        server_lookup_ms = 0.0
    lease_protocol_honored = returned_leases == requested_leases
    if not bool(payload.get("all_hit")):
        # Release every acknowledged partial reservation, not merely one
        # reference. A legacy route that does not echo the lease count is
        # treated as a one-reference lookup and cannot enter the pool.
        hits = [
            str(item.get("feature_id") or "")
            for item in list(payload.get("hits") or [])
            if isinstance(item, dict) and str(item.get("feature_id") or "")
        ]
        if hits:
            try:
                await direct_client.post(
                    "/mooncake_epd/direct_feature_buffer/release",
                    json={
                        "feature_ids": hits
                        * (requested_leases if lease_protocol_honored else 1),
                        "target_worker_id": target_worker_id,
                    },
                )
            except Exception:
                logger.debug("prefill direct partial-cache release failed", exc_info=True)
        return []
    if requested_leases > 1 and not lease_protocol_honored:
        # A mixed-version direct-buffer route may accept an unknown JSON field
        # but still acquire only one legacy reference. Never populate the pool
        # unless the server positively acknowledges the requested count.
        hits = [
            str(item.get("feature_id") or "")
            for item in list(payload.get("hits") or [])
            if isinstance(item, dict) and str(item.get("feature_id") or "")
        ]
        if hits:
            try:
                await direct_client.post(
                    "/mooncake_epd/direct_feature_buffer/release",
                    json={
                        "feature_ids": hits,
                        "target_worker_id": target_worker_id,
                    },
                )
            except Exception:
                logger.debug("failed to release legacy direct cache lookup", exc_info=True)
        logger.warning(
            "direct FeatureBuffer lookup did not acknowledge lease_count=%s; "
            "bypassing lease prefetch for worker=%s",
            requested_leases,
            target_worker_id,
        )
        return []
    handles: List[Dict[str, Any]] = []
    lookup_ms = (time.perf_counter() - started) * 1000.0
    by_id = {
        str(item.get("feature_id") or ""): dict(item)
        for item in list(payload.get("hits") or [])
        if isinstance(item, dict)
    }
    raw_hits = [
        item
        for item in list(payload.get("hits") or [])
        if isinstance(item, dict)
    ]
    if len(raw_hits) != len(ids):
        # The reference contract is one acquired lease per requested FeatureID
        # occurrence. A malformed/legacy response cannot prove that for
        # duplicated images, so release and fall back instead of risking an
        # under-counted lifetime.
        hit_ids = [str(item.get("feature_id") or "") for item in raw_hits]
        if hit_ids:
            try:
                await direct_client.post(
                    "/mooncake_epd/direct_feature_buffer/release",
                    json={
                        "feature_ids": hit_ids * requested_leases,
                        "target_worker_id": target_worker_id,
                    },
                )
            except Exception:
                logger.debug("failed to release malformed direct cache lookup", exc_info=True)
        return []
    for feature_id in ids:
        item = by_id.get(str(feature_id))
        if not item:
            return []
        descriptor = FeatureBundleDescriptor.from_dict(dict(item.get("descriptor") or {}))
        target = dict(item.get("target") or {})
        handle = _build_direct_handle_from_target(
            descriptor=descriptor,
            target=target,
            store_id=str(target.get("store_id") or "direct"),
            metadata={
                "backend": "direct_engine",
                "direct_backend": "prefill_persistent_cache",
                "source_mm_hash": str(feature_id),
                "mooncake_epd_target_worker_id": str(target_worker_id),
                "target_worker_id": str(target_worker_id),
                "proxy_direct_timings_ms": {
                    "direct_cache_lookup_ms": lookup_ms,
                    "direct_cache_server_lookup_ms": server_lookup_ms,
                    "direct_cache_lease_prefetch": float(requested_leases),
                    "direct_cache_hits": float(len(ids)),
                    "direct_describe_ms": 0.0,
                    "direct_allocate_ms": 0.0,
                    "direct_publish_ms": 0.0,
                },
            },
        )
        handles.append(handle.as_control_payload())
    return handles


def _direct_feature_lease_prefetch_release_ids(
    entries: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, List[str]], int]:
    by_worker: Dict[str, List[str]] = {}
    references = 0
    for entry in entries:
        worker_id = str(entry.get("target_worker_id") or "")
        feature_ids = [
            str(feature_id)
            for feature_id in list(entry.get("feature_ids") or ())
            if str(feature_id or "").strip()
        ]
        remaining = max(0, int(entry.get("remaining_leases", 0) or 0))
        if not worker_id or not feature_ids or remaining <= 0:
            continue
        ids = feature_ids * remaining
        by_worker.setdefault(worker_id, []).extend(ids)
        references += len(ids)
    return by_worker, references


async def _prune_direct_feature_lease_prefetch_pool(
    app: FastAPI,
    *,
    now: Optional[float] = None,
    force_ttl_sweep: bool = False,
) -> int:
    """Evict expired/bounded lease reservations without blocking TTFT.

    Releases are handed to the existing bounded cleanup dispatcher.  A request
    never awaits a direct-buffer release RPC merely because an unrelated cache
    entry expired or was evicted.
    """

    pool = getattr(app.state, "direct_feature_lease_prefetch_pool", None)
    if not isinstance(pool, dict):
        return 0
    config: ProxyConfig = app.state.proxy_config
    _, max_entries, ttl_s = _direct_feature_lease_prefetch_settings(config)
    now = time.monotonic() if now is None else float(now)
    stats = _direct_feature_lease_prefetch_stats(app)
    next_sweep = float(
        getattr(app.state, "direct_feature_lease_prefetch_next_ttl_sweep_at", 0.0)
        or 0.0
    )
    should_sweep = ttl_s > 0.0 and (force_ttl_sweep or now >= next_sweep)
    victims: List[Dict[str, Any]] = []
    if should_sweep:
        interval = min(5.0, max(0.25, ttl_s / 8.0))
        app.state.direct_feature_lease_prefetch_next_ttl_sweep_at = now + interval
        stats["ttl_sweeps"] = int(stats.get("ttl_sweeps", 0) or 0) + 1
        stats["ttl_sweep_entries_scanned"] = int(
            stats.get("ttl_sweep_entries_scanned", 0) or 0
        ) + len(pool)
        expired_keys = []
        for key, entry in pool.items():
            if not isinstance(entry, dict):
                expired_keys.append(key)
                continue
            last_used = float(
                entry.get("last_used_at", entry.get("reserved_at", now)) or now
            )
            if now - last_used > ttl_s:
                expired_keys.append(key)
        for key in expired_keys:
            entry = pool.pop(key, None)
            if isinstance(entry, dict):
                victims.append(entry)
                stats["expired"] = int(stats.get("expired", 0) or 0) + 1
    if max_entries <= 0:
        while pool:
            if isinstance(pool, OrderedDict):
                _, entry = pool.popitem(last=False)
            else:
                key, entry = next(iter(pool.items()))
                pool.pop(key, None)
            if isinstance(entry, dict):
                victims.append(entry)
                stats["evictions"] = int(stats.get("evictions", 0) or 0) + 1
    else:
        while len(pool) > max_entries:
            if isinstance(pool, OrderedDict):
                _, entry = pool.popitem(last=False)
            else:
                key, entry = min(
                    pool.items(),
                    key=lambda item: float(
                        dict(item[1]).get(
                            "last_used_at", dict(item[1]).get("reserved_at", 0.0)
                        )
                        if isinstance(item[1], dict)
                        else 0.0
                    ),
                )
                pool.pop(key, None)
            if isinstance(entry, dict):
                victims.append(entry)
                stats["evictions"] = int(stats.get("evictions", 0) or 0) + 1
    by_worker, references = _direct_feature_lease_prefetch_release_ids(victims)
    if by_worker:
        _enqueue_direct_feature_buffer_release_job(app, by_worker)
        stats["release_enqueued_references"] = int(
            stats.get("release_enqueued_references", 0) or 0
        ) + references
    stats.update(_direct_feature_lease_prefetch_pool_gauges(pool))
    return references


async def _flush_direct_feature_lease_prefetch_pool(
    app: FastAPI,
    *,
    reason: str,
    wait_for_release: bool,
) -> Tuple[int, int]:
    """Drop every spare lease, optionally waiting for terminal cleanup."""

    pool = getattr(app.state, "direct_feature_lease_prefetch_pool", None)
    if not isinstance(pool, dict):
        return 0, 0
    entries = [dict(entry) for entry in pool.values() if isinstance(entry, dict)]
    pool.clear()
    stats = _direct_feature_lease_prefetch_stats(app)
    stats["flushes"] = int(stats.get("flushes", 0) or 0) + 1
    by_worker, references = _direct_feature_lease_prefetch_release_ids(entries)
    failures = 0
    if by_worker:
        if wait_for_release:
            failures = await _release_direct_feature_buffer_ids(app, by_worker)
            if failures:
                stats["release_failures"] = int(
                    stats.get("release_failures", 0) or 0
                ) + failures
        else:
            _enqueue_direct_feature_buffer_release_job(app, by_worker)
            stats["release_enqueued_references"] = int(
                stats.get("release_enqueued_references", 0) or 0
            ) + references
    stats.update(_direct_feature_lease_prefetch_pool_gauges(pool))
    if references:
        logger.debug(
            "released %s unused direct-feature lease references reason=%s failures=%s",
            references,
            reason,
            failures,
        )
    return references, failures


async def _direct_feature_lease_prefetch_sweeper(app: FastAPI) -> None:
    """Periodically return expired reserve references while the Proxy is idle."""

    while True:
        config: ProxyConfig = app.state.proxy_config
        _, _, ttl_s = _direct_feature_lease_prefetch_settings(config)
        if ttl_s <= 0.0:
            return
        await asyncio.sleep(min(5.0, max(0.25, ttl_s / 8.0)))
        await _prune_direct_feature_lease_prefetch_pool(app, force_ttl_sweep=True)


def _start_direct_feature_lease_prefetch_sweeper(app: FastAPI) -> None:
    config: ProxyConfig = app.state.proxy_config
    lease_count, max_entries, ttl_s = _direct_feature_lease_prefetch_settings(config)
    adaptive_lease_prefetch, _ = _direct_feature_adaptive_lease_prefetch_settings(
        config
    )
    if (
        (lease_count <= 1 and not adaptive_lease_prefetch)
        or max_entries <= 0
        or ttl_s <= 0.0
        or not config.release_direct_feature_buffers_after_prefill
    ):
        return
    task = getattr(app.state, "direct_feature_lease_prefetch_sweeper_task", None)
    if task is None or task.done():
        app.state.direct_feature_lease_prefetch_sweeper_task = asyncio.create_task(
            _direct_feature_lease_prefetch_sweeper(app)
        )


async def _shutdown_direct_feature_lease_prefetch_pool(app: FastAPI) -> None:
    task = getattr(app.state, "direct_feature_lease_prefetch_sweeper_task", None)
    if task is not None and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    app.state.direct_feature_lease_prefetch_sweeper_task = None
    await _flush_direct_feature_lease_prefetch_pool(
        app,
        reason="proxy_shutdown",
        wait_for_release=True,
    )


def _build_direct_handle_from_target(
    *,
    descriptor: FeatureBundleDescriptor,
    target: Dict[str, Any],
    store_id: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> FeatureHandle:
    remote_session = str(target.get("remote_session") or "")
    direct_allocation_id = str(target.get("allocation_id") or "").strip()
    remote_pointers = dict(target.get("remote_pointers") or {})
    if not remote_session or not remote_pointers:
        raise FeatureHandleError("cached direct target missing remote session/pointers")
    plan_targets = []
    for name, pointer in remote_pointers.items():
        name = str(name)
        if name.endswith(":nbytes"):
            continue
        nbytes = int(remote_pointers.get(f"{name}:nbytes", 0) or 0)
        if nbytes <= 0:
            raise FeatureHandleError(f"cached direct target missing nbytes for {name}")
        plan_targets.append(
            {
                "name": name,
                "remote_pointer": int(pointer),
                "nbytes": nbytes,
            }
        )
    md = dict(metadata or {})
    md.update(
        {
            "direct_remote_session": remote_session,
            "direct_plan": {
                "feature_id": descriptor.feature_id,
                "targets": plan_targets,
            },
            "direct_tensor_count": len(plan_targets),
            "direct_descriptor_count": len(plan_targets),
            "direct_bytes": sum(int(item["nbytes"]) for item in plan_targets),
        }
    )
    if direct_allocation_id:
        md["direct_allocation_id"] = direct_allocation_id
    return FeatureHandle(
        handle_id=f"direct-cache-{descriptor.feature_id}-{int(time.time() * 1_000_000)}",
        feature_id=str(descriptor.feature_id),
        store_id=str(store_id or "direct"),
        uri=f"epd-direct://{store_id or 'direct'}/{descriptor.feature_id}",
        descriptor=descriptor,
        metadata=md,
    )


async def _schedule_direct_feature_buffer_release(
    app: FastAPI,
    req_data: Dict[str, Any],
) -> None:
    """Detach post-Prefill buffer cleanup from the user-visible decode path.

    A direct FeatureBundle is safe to release after the Prefill OpenAI response
    returns: the GPU worker has consumed the handle before that response is
    emitted. Cleanup is handed to a bounded queue. In particular, a saturated
    cleanup plane must never block Decode dispatch and inflate TTFT.
    """

    config: ProxyConfig = app.state.proxy_config
    if not config.release_direct_feature_buffers_after_prefill:
        return
    if not _has_prefill_direct_buffer_service(config):
        return

    by_worker = _collect_direct_feature_release_ids(req_data)
    if not by_worker:
        return

    started = time.perf_counter()
    _enqueue_direct_feature_buffer_release_job(app, by_worker)
    _set_proxy_timing(
        req_data,
        "direct_release_enqueue_ms",
        (time.perf_counter() - started) * 1000.0,
    )


async def _release_direct_feature_buffers_after_prefill(app: FastAPI, req_data: Dict[str, Any]) -> None:
    """Release Prefill-owned E→P direct buffers once Prefill has consumed them."""

    config: ProxyConfig = app.state.proxy_config
    if not config.release_direct_feature_buffers_after_prefill:
        return
    if not _has_prefill_direct_buffer_service(config):
        return
    by_worker = _collect_direct_feature_release_ids(req_data)
    if by_worker:
        await _release_direct_feature_buffer_ids(app, by_worker)


def _collect_direct_feature_release_ids(req_data: Dict[str, Any]) -> Dict[str, List[str]]:
    kv = dict(req_data.get("kv_transfer_params") or {})
    metadata = dict(req_data.get("metadata") or {})
    default_worker_id = str(
        kv.get("mm_feature_handle_target_worker")
        or metadata.get("mooncake_epd_feature_handle_target_worker")
        or ""
    )
    raw_handles = (
        kv.get("mm_feature_handles")
        or metadata.get("mooncake_epd_feature_handles")
        or metadata.get("feature_handles")
        or []
    )
    if not isinstance(raw_handles, list):
        return {}
    by_worker: Dict[str, List[str]] = {}
    for item in raw_handles:
        if not isinstance(item, dict):
            continue
        if not str(item.get("uri") or "").startswith("epd-direct://"):
            continue
        fid = str(item.get("feature_id") or "")
        if not fid:
            continue
        worker_id = _direct_worker_id_from_handle(item, default_worker_id)
        by_worker.setdefault(worker_id, []).append(fid)
    return {worker_id: list(feature_ids) for worker_id, feature_ids in by_worker.items() if feature_ids}


async def _release_direct_feature_buffer_ids(
    app: FastAPI,
    by_worker: Dict[str, List[str]],
) -> int:
    """Issue release RPCs and return the number of failed worker batches."""

    failures = 0
    for worker_id, feature_ids in by_worker.items():
        if not feature_ids:
            continue
        client = _prefill_direct_buffer_client_for(app, worker_id)
        if client is None:
            logger.error("no direct-buffer client for release worker=%s feature_ids=%s", worker_id, feature_ids)
            failures += 1
            continue
        try:
            response = await client.post(
                "/mooncake_epd/direct_feature_buffer/release",
                json={"feature_ids": list(feature_ids), "target_worker_id": worker_id},
            )
            response.raise_for_status()
        except Exception as exc:
            # Release failure is operationally serious but the P→D handoff may have
            # already succeeded. Report through logs/metrics rather than corrupting
            # the user response after Prefill has completed.
            logger.error("failed to release direct feature buffers after prefill worker=%s: %s", worker_id, exc)
            failures += 1
    return failures


def _ensure_direct_feature_release_dispatcher(app: FastAPI) -> asyncio.Queue[Dict[str, List[str]]]:
    queue = getattr(app.state, "direct_feature_release_queue", None)
    if isinstance(queue, asyncio.Queue):
        return queue
    config: ProxyConfig = app.state.proxy_config
    queue = asyncio.Queue(
        maxsize=max(
            1,
            int(getattr(config, "direct_feature_release_max_inflight", 128) or 128),
        )
    )
    app.state.direct_feature_release_queue = queue
    workers = getattr(app.state, "direct_feature_release_workers", None)
    if not isinstance(workers, set):
        workers = set()
        app.state.direct_feature_release_workers = workers
    workers.add(asyncio.create_task(_direct_feature_release_worker(app, queue)))
    return queue


def _direct_feature_release_batch_max_jobs(app: FastAPI) -> int:
    config: ProxyConfig = app.state.proxy_config
    return min(
        1024,
        max(
            1,
            int(getattr(config, "direct_feature_release_batch_max_jobs", 16) or 16),
        ),
    )


def _merge_direct_feature_release_jobs(
    jobs: Sequence[Dict[str, List[str]]],
) -> Dict[str, List[str]]:
    """Merge cleanup jobs without deduplicating reference-owned IDs."""

    merged: Dict[str, List[str]] = {}
    for job in jobs:
        for worker_id, feature_ids in job.items():
            if not feature_ids:
                continue
            merged.setdefault(str(worker_id), []).extend(
                str(feature_id) for feature_id in feature_ids
            )
    return merged


def _enqueue_direct_feature_buffer_release_job(
    app: FastAPI,
    by_worker: Dict[str, List[str]],
) -> None:
    """Queue reference cleanup without making the caller await I/O."""

    if not by_worker:
        return
    stats = getattr(app.state, "direct_feature_release_stats", None)
    if not isinstance(stats, dict):
        stats = {
            "scheduled": 0,
            "completed": 0,
            "failed": 0,
            "batches": 0,
            "batched_jobs": 0,
            "released_feature_references": 0,
            "batch_max_jobs": _direct_feature_release_batch_max_jobs(app),
            "max_inflight": 0,
            "queue_full": 0,
            "coalesced": 0,
        }
        app.state.direct_feature_release_stats = stats
    queue = _ensure_direct_feature_release_dispatcher(app)
    try:
        queue.put_nowait(by_worker)
    except asyncio.QueueFull:
        _merge_direct_feature_release_overflow(app, by_worker)
        stats["queue_full"] = int(stats.get("queue_full", 0) or 0) + 1
        stats["coalesced"] = int(stats.get("coalesced", 0) or 0) + sum(
            len(feature_ids) for feature_ids in by_worker.values()
        )
    else:
        stats["scheduled"] = int(stats.get("scheduled", 0) or 0) + 1
    overflow = getattr(app.state, "direct_feature_release_overflow", {})
    inflight = queue.qsize() + (
        sum(len(ids) for ids in overflow.values()) if isinstance(overflow, dict) else 0
    )
    stats["max_inflight"] = max(int(stats.get("max_inflight", 0) or 0), inflight)


def _merge_direct_feature_release_overflow(
    app: FastAPI,
    by_worker: Dict[str, List[str]],
) -> None:
    overflow = getattr(app.state, "direct_feature_release_overflow", None)
    if not isinstance(overflow, dict):
        overflow = {}
        app.state.direct_feature_release_overflow = overflow
    for worker_id, feature_ids in by_worker.items():
        bucket = overflow.setdefault(str(worker_id), [])
        bucket.extend(str(feature_id) for feature_id in feature_ids)


def _take_direct_feature_release_overflow(app: FastAPI) -> Dict[str, List[str]]:
    overflow = getattr(app.state, "direct_feature_release_overflow", None)
    if not isinstance(overflow, dict) or not overflow:
        return {}
    app.state.direct_feature_release_overflow = {}
    return {
        str(worker_id): [str(feature_id) for feature_id in feature_ids]
        for worker_id, feature_ids in overflow.items()
        if feature_ids
    }


async def _direct_feature_release_worker(
    app: FastAPI,
    queue: asyncio.Queue[Dict[str, List[str]] | None],
) -> None:
    while True:
        job = await queue.get()
        jobs: List[Dict[str, List[str]]] = []
        try:
            if job is None:
                return
            jobs.append(job)
            # Yield once after the first completed Prefill request. Other
            # responses from the same event-loop turn can enqueue their
            # independent release references, but no user request waits for
            # this background cleanup. Do not wait a wall-clock interval: the
            # feature lifetime remains bounded and shutdown drains the queue.
            if _direct_feature_release_batch_max_jobs(app) > 1:
                await asyncio.sleep(0)
                while len(jobs) < _direct_feature_release_batch_max_jobs(app):
                    try:
                        next_job = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if next_job is None:
                        # Shutdown normally inserts sentinels only after
                        # queue.join(), so this branch is defensive. Preserve
                        # the sentinel for the next loop iteration without
                        # perturbing Queue unfinished-task accounting.
                        queue.task_done()
                        queue.put_nowait(None)
                        break
                    jobs.append(next_job)
            merged = _merge_direct_feature_release_jobs(jobs)
            failures = await _release_direct_feature_buffer_ids(app, merged)
            stats = getattr(app.state, "direct_feature_release_stats", None)
            if isinstance(stats, dict):
                stats["completed"] = int(stats.get("completed", 0) or 0) + len(jobs)
                stats["batches"] = int(stats.get("batches", 0) or 0) + 1
                stats["batched_jobs"] = int(stats.get("batched_jobs", 0) or 0) + max(
                    0, len(jobs) - 1
                )
                stats["released_feature_references"] = int(
                    stats.get("released_feature_references", 0) or 0
                ) + sum(len(feature_ids) for feature_ids in merged.values())
                if failures:
                    stats["failed"] = int(stats.get("failed", 0) or 0) + failures
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("background direct feature release failed")
            stats = getattr(app.state, "direct_feature_release_stats", None)
            if isinstance(stats, dict):
                stats["failed"] = int(stats.get("failed", 0) or 0) + 1
        finally:
            if job is not None:
                for _ in jobs:
                    queue.task_done()
            else:
                queue.task_done()

        # Coalesced work is only moved back into the bounded queue by the
        # worker, never by a request handler. This preserves a strict TTFT
        # boundary even when release RPCs are slower than incoming Prefill work.
        overflow = _take_direct_feature_release_overflow(app)
        if overflow:
            try:
                queue.put_nowait(overflow)
                stats = getattr(app.state, "direct_feature_release_stats", None)
                if isinstance(stats, dict):
                    stats["scheduled"] = int(stats.get("scheduled", 0) or 0) + 1
            except asyncio.QueueFull:
                _merge_direct_feature_release_overflow(app, overflow)


async def _shutdown_direct_feature_release_dispatcher(app: FastAPI) -> None:
    queue = getattr(app.state, "direct_feature_release_queue", None)
    workers = getattr(app.state, "direct_feature_release_workers", None)
    if not isinstance(queue, asyncio.Queue) or not isinstance(workers, set) or not workers:
        return
    overflow = _take_direct_feature_release_overflow(app)
    if overflow:
        await queue.put(overflow)
    await queue.join()
    active_workers = list(workers)
    for _ in active_workers:
        await queue.put(None)
    await asyncio.gather(*active_workers, return_exceptions=True)
    workers.clear()


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

    # Request bodies are already JSON-compatible dict/list trees. Avoid a JSON
    # serialize/parse round trip on the E->P hot path while keeping the caller's
    # payload immutable for the later Decode leg.
    rewritten = deepcopy(req_data)
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
                    if isinstance(item, dict) and _image_url_from_item(item):
                        yield item
    prompt = req_data.get("prompt")
    if isinstance(prompt, list):
        for item in prompt:
            if isinstance(item, dict) and _image_url_from_item(item):
                yield item


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


async def _load_mm_url_bytes(app: FastAPI, url: str, *, max_bytes: int) -> Tuple[bytes, str]:
    if url.startswith("data:"):
        return _parse_data_url(url, max_bytes=max_bytes)
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError(f"unsupported multimodal URL scheme: {url[:32]}")
    client: httpx.AsyncClient = app.state.mm_fetch_client
    response = await client.get(url)
    response.raise_for_status()
    payload = response.content
    if len(payload) > max_bytes:
        raise ValueError(f"multimodal asset too large: {len(payload)} > {max_bytes}")
    content_type = response.headers.get("content-type", "application/octet-stream").split(";")[0].strip()
    return payload, content_type or "application/octet-stream"


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
    # Header propagation mutates only top-level metadata. Deep JSON cloning
    # duplicated large image/audio data URLs before Prefill dispatch.
    merged = dict(req_data)
    metadata = dict(req_data.get("metadata") or {})
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
    cache_salt: Optional[str] = None,
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
    # A rendered artifact can be reused across cache-isolated requests. Its
    # original cache_salt must never leak into a later /generate request,
    # otherwise vLLM may reuse a prior prefix despite the benchmark policy.
    if cache_salt:
        payload["cache_salt"] = cache_salt
    else:
        payload.pop("cache_salt", None)
    payload["kv_transfer_params"] = dict(kv_transfer_params)
    return payload


async def _dispatch_prompt_only_prefill(
    *,
    app: FastAPI,
    api: str,
    prefill_client: Dict[str, Any],
    prefill_headers: Dict[str, str],
    request_id: str,
    request_body: Dict[str, Any],
    kv_transfer_params: Dict[str, Any],
) -> Dict[str, Any]:
    timings: Dict[str, float] = {}
    render_started = time.perf_counter()
    cache_lookup_started = time.perf_counter()
    cache_key, cache_reason = _rendered_prefill_cache_key(
        app=app,
        api=api,
        prefill_client=prefill_client,
        request_body=request_body,
    )
    rendered_payload: Optional[Dict[str, Any]] = None
    if cache_key is not None:
        rendered_payload = _lookup_rendered_prefill_cache(app, cache_key)
    else:
        _note_rendered_prefill_cache_bypass(app, cache_reason)
    timings["prefill_render_cache_lookup_ms"] = (
        time.perf_counter() - cache_lookup_started
    ) * 1000.0
    timings["prefill_render_cache_hit"] = float(rendered_payload is not None)

    if rendered_payload is None:
        upstream_render_started = time.perf_counter()
        render_request = _build_json_request_with_metrics(
            prefill_client["client"],
            _render_api_for(api),
            request_body,
            prefill_headers,
            timings,
            prefix="prefill_render",
        )
        render_response = await prefill_client["client"].send(render_request)
        try:
            render_response.raise_for_status()
            timings["prefill_render_response_bytes"] = _response_content_bytes(
                render_response
            )
            render_json_decode_started_at = time.perf_counter()
            rendered_payload = _normalize_rendered_prefill_payload(
                api,
                render_response.json(),
            )
            timings["prefill_render_json_decode_ms"] = (
                time.perf_counter() - render_json_decode_started_at
            ) * 1000.0
            response_bytes = len(render_response.content)
        finally:
            await render_response.aclose()
        timings["prefill_render_upstream_ms"] = (
            time.perf_counter() - upstream_render_started
        ) * 1000.0
        if cache_key is not None:
            _store_rendered_prefill_cache(
                app,
                cache_key=cache_key,
                rendered_payload=rendered_payload,
                size_bytes=response_bytes,
            )
    timings["prefill_render_ms"] = (time.perf_counter() - render_started) * 1000.0

    prefill_payload = _inject_prefill_kv_into_rendered_request(
        rendered_payload,
        request_id=request_id,
        kv_transfer_params=kv_transfer_params,
        cache_salt=(
            str(request_body.get("cache_salt"))
            if isinstance(request_body.get("cache_salt"), str)
            and str(request_body.get("cache_salt"))
            else None
        ),
    )
    started = time.perf_counter()
    generate_request = _build_json_request_with_metrics(
        prefill_client["client"],
        "/inference/v1/generate",
        prefill_payload,
        prefill_headers,
        timings,
        prefix="prefill_generate",
    )
    generate_response = await prefill_client["client"].send(generate_request)
    try:
        generate_response.raise_for_status()
        timings["prefill_generate_response_bytes"] = _response_content_bytes(
            generate_response
        )
        generate_json_decode_started_at = time.perf_counter()
        payload = dict(generate_response.json())
        timings["prefill_generate_json_decode_ms"] = (
            time.perf_counter() - generate_json_decode_started_at
        ) * 1000.0
    finally:
        await generate_response.aclose()
    timings["prefill_generate_ms"] = (time.perf_counter() - started) * 1000.0
    timings["prefill_render_generate_ms"] = timings["prefill_render_ms"] + timings["prefill_generate_ms"]
    # The render result is the authoritative model-visible token sequence.
    # Carry it only in the Proxy-local response object so Decode can bind the
    # compact request to the exact KV-producing prompt.
    payload["prompt_token_ids"] = list(rendered_payload.get("token_ids") or [])
    payload["_mooncake_epd_proxy_timings_ms"] = timings
    payload.setdefault("kv_transfer_params", dict(kv_transfer_params))
    return payload


async def _dispatch_openai_prompt_only_prefill(
    *,
    api: str,
    prefill_client: Dict[str, Any],
    prefill_headers: Dict[str, str],
    request_id: str,
    request_body: Dict[str, Any],
    kv_transfer_params: Dict[str, Any],
) -> Dict[str, Any]:
    """Run Prefill through one OpenAI request with max_tokens=0.

    vLLM's OpenAI chat/completion protocol accepts ``kv_transfer_params`` and
    returns ``kv_transfer_params`` on the final response when a KV connector is
    configured.  With the repo-local prompt-only scheduler patch, max_tokens=0
    finishes immediately after prompt forward, so this path avoids the previous
    render + internal generate double HTTP/double scheduling overhead.
    """

    started = time.perf_counter()
    # The prompt-only leg changes only top-level request controls and metadata.
    # A deep JSON clone here previously reserialized large data URLs before the
    # actual Prefill HTTP request, directly reducing high-QPS MM throughput.
    prefill_payload = dict(request_body)
    prefill_payload["stream"] = False
    prefill_payload["max_tokens"] = 0
    if "max_completion_tokens" in prefill_payload:
        prefill_payload["max_completion_tokens"] = 0
    # The local prompt-only patch may receive the sampled token produced by
    # the final prompt forward.  Ask vLLM to retain its id for observability;
    # the proxy does not expose it to clients or alter Decode semantics here.
    prefill_payload["return_token_ids"] = True
    prefill_payload.pop("stream_options", None)
    prefill_payload["kv_transfer_params"] = dict(kv_transfer_params)
    metadata = dict(prefill_payload.get("metadata") or {})
    metadata["mooncake_epd_prompt_only_prefill"] = True
    metadata["mooncake_epd_prefill_dispatch_mode"] = "openai_prompt_only"
    prefill_payload["metadata"] = metadata

    timings: Dict[str, float] = {}
    prefill_request = _build_json_request_with_metrics(
        prefill_client["client"],
        api,
        prefill_payload,
        prefill_headers,
        timings,
        prefix="prefill_openai_prompt_only",
    )
    response = await prefill_client["client"].send(prefill_request)
    try:
        response.raise_for_status()
        timings["prefill_openai_prompt_only_response_bytes"] = _response_content_bytes(
            response
        )
        response_json_decode_started_at = time.perf_counter()
        payload = dict(response.json())
        timings["prefill_openai_prompt_only_json_decode_ms"] = (
            time.perf_counter() - response_json_decode_started_at
        ) * 1000.0
    finally:
        await response.aclose()
    timings.update(
        {
            "prefill_openai_prompt_only_ms": (time.perf_counter() - started) * 1000.0,
            "prefill_render_ms": 0.0,
            "prefill_generate_ms": 0.0,
        }
    )
    timings["prefill_render_generate_ms"] = timings["prefill_openai_prompt_only_ms"]
    observed = _extract_prompt_only_sample_observation(api, payload)
    timings["prefill_sampled_token_count"] = float(len(observed["token_ids"]))
    payload["_mooncake_epd_prompt_only_sample"] = observed
    payload["_mooncake_epd_proxy_timings_ms"] = timings
    returned_kv = payload.get("kv_transfer_params")
    if not isinstance(returned_kv, dict) or not returned_kv:
        raise RuntimeError(
            "openai_prompt_only prefill returned no kv_transfer_params; "
            "this vLLM build/path cannot be used for EPD prompt-only prefill"
        )
    return payload


def _extract_prompt_only_sample_observation(
    api: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Capture, but do not consume, the prompt-forward sampled token.

    This is deliberately an internal observation.  It lets real serving prove
    whether vLLM already sampled a first token during ``max_tokens=0`` before
    enabling any producer-first-token optimization that could affect output
    semantics.
    """

    choices = list(payload.get("choices") or [])
    choice = dict(choices[0] or {}) if choices else {}
    token_ids: List[int] = []
    for token_id in list(choice.get("token_ids") or []):
        try:
            token_ids.append(int(token_id))
        except (TypeError, ValueError):
            continue
    return {
        "token_ids": token_ids,
        "text": _extract_choice_text(api, choice),
        "completion_tokens": int(
            dict(payload.get("usage") or {}).get("completion_tokens", 0) or 0
        ),
        "finish_reason": choice.get("finish_reason"),
    }


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


_TOKEN_COMPLETION_FORWARD_FIELDS = (
    "model",
    "echo",
    "frequency_penalty",
    "logit_bias",
    "logprobs",
    "max_tokens",
    "n",
    "presence_penalty",
    "seed",
    "stop",
    "stream",
    "stream_options",
    "suffix",
    "temperature",
    "top_p",
    "use_beam_search",
    "top_k",
    "min_p",
    "repetition_penalty",
    "length_penalty",
    "stop_token_ids",
    "include_stop_str_in_output",
    "ignore_eos",
    "min_tokens",
    "skip_special_tokens",
    "spaces_between_special_tokens",
    "truncate_prompt_tokens",
    "truncation_side",
    "allowed_token_ids",
    "prompt_logprobs",
    "response_format",
    "structured_outputs",
    "priority",
    "request_id",
    "return_tokens_as_token_ids",
    "return_token_ids",
    "cache_salt",
    "vllm_xargs",
    "repetition_detection",
    "thinking_token_budget",
)


def _decode_model_identity(
    prefill_client: Dict[str, Any],
    decode_client: Dict[str, Any],
    request_body: Dict[str, Any],
) -> str:
    prefill_hash = str(
        dict(prefill_client.get("capabilities") or {}).get(
            "model_id_sha256"
        )
        or ""
    )
    decode_hash = str(
        dict(decode_client.get("capabilities") or {}).get(
            "model_id_sha256"
        )
        or ""
    )
    if (
        prefill_hash
        and decode_hash
        and not hmac.compare_digest(prefill_hash, decode_hash)
    ):
        raise DecodeTokenEnvelopeError(
            "Prefill and Decode model identities differ"
        )
    request_model = str(request_body.get("model") or "")
    request_hash = (
        hashlib.sha256(request_model.encode("utf-8")).hexdigest()
        if request_model
        else ""
    )
    worker_hash = prefill_hash or decode_hash
    if (
        worker_hash
        and request_hash
        and not hmac.compare_digest(worker_hash, request_hash)
    ):
        raise DecodeTokenEnvelopeError(
            "request model does not match EPD worker model identity"
        )
    return worker_hash or request_hash


def _verify_prefill_decode_render_contract(
    prefill_client: Dict[str, Any],
    decode_client: Dict[str, Any],
) -> None:
    prefill = dict(prefill_client.get("capabilities") or {})
    decode = dict(decode_client.get("capabilities") or {})
    for field_name, label in (
        ("model_config_sha256", "model config"),
        ("tokenizer_id_sha256", "tokenizer"),
        ("chat_template_sha256", "chat template"),
        ("kv_manifest_schema_version", "KV manifest schema"),
    ):
        prefill_value = str(prefill.get(field_name) or "")
        decode_value = str(decode.get(field_name) or "")
        if not prefill_value or not decode_value:
            raise DecodeTokenEnvelopeError(
                f"Prefill/Decode {label} identity is unavailable"
            )
        if not hmac.compare_digest(prefill_value, decode_value):
            raise DecodeTokenEnvelopeError(
                f"Prefill and Decode {label} identities differ"
            )
    prefill_family = str(prefill.get("model_family") or "")
    decode_family = str(decode.get("model_family") or "")
    if not prefill_family or not decode_family:
        raise DecodeTokenEnvelopeError(
            "Prefill/Decode model family identity is unavailable"
        )
    if prefill_family != decode_family:
        raise DecodeTokenEnvelopeError(
            "Prefill and Decode model families differ"
        )


def _token_decode_ineligible_reason(
    api: str,
    request_body: Dict[str, Any],
) -> str:
    if not (
        api.endswith("/chat/completions")
        or api.endswith("/completions")
    ):
        return f"unsupported API {api!r}"
    if api.endswith("/chat/completions"):
        tools = list(request_body.get("tools") or [])
        tool_choice = request_body.get("tool_choice")
        if tools or tool_choice not in (None, "none"):
            return "Chat tool parsing requires the native Chat response path"
        if bool(request_body.get("logprobs", False)):
            return "Chat logprobs are not preserved by Completion adaptation"
        if bool(request_body.get("echo", False)):
            return "Chat echo semantics are not supported by token Decode"
        if request_body.get("return_prompt_text"):
            return "return_prompt_text requires the native Chat render path"
        if request_body.get("include_reasoning") is False:
            return "include_reasoning=false requires the native reasoning parser"
        if list(request_body.get("bad_words") or []):
            return "CompletionRequest does not preserve Chat bad_words semantics"
    if request_body.get("prompt_embeds") is not None:
        return "prompt_embeds cannot be combined with a token envelope"
    return ""


def _build_token_decode_payload(
    *,
    api: str,
    request_body: Dict[str, Any],
    kv_transfer_params: Dict[str, Any],
    envelope: DecodeTokenEnvelope,
) -> tuple[str, Dict[str, Any], bool]:
    reason = _token_decode_ineligible_reason(api, request_body)
    if reason:
        raise DecodeTokenEnvelopeError(reason)

    payload = {
        field_name: request_body[field_name]
        for field_name in _TOKEN_COMPLETION_FORWARD_FIELDS
        if field_name in request_body
    }
    max_completion_tokens = request_body.get("max_completion_tokens")
    if max_completion_tokens is not None:
        payload["max_tokens"] = max_completion_tokens
    payload["prompt"] = list(envelope.prompt_token_ids)
    # The envelope already contains the exact rendered input. Any additional
    # BOS/special token would desynchronize Decode from the transferred KV.
    payload["add_special_tokens"] = False
    payload["kv_transfer_params"] = dict(kv_transfer_params)
    metadata = dict(request_body.get("metadata") or {})
    metadata["mooncake_epd_decode_token_envelope"] = envelope.to_dict(
        include_prompt_token_ids=False
    )
    metadata["mooncake_epd_decode_media_stripped"] = True
    payload["metadata"] = metadata
    return "/v1/completions", payload, api.endswith("/chat/completions")


async def _prepare_versioned_decode_dispatch(
    *,
    config: ProxyConfig,
    api: str,
    request_body: Dict[str, Any],
    prefill_response: Dict[str, Any],
    prefill_client: Dict[str, Any],
    decode_client: Dict[str, Any],
    kv_transfer_params: Dict[str, Any],
    ctx: Any,
) -> tuple[str, Dict[str, Any], bool, Optional[DecodeTokenEnvelope]]:
    """Validate shadow state, then optionally construct compact Decode input."""

    mode = str(config.decode_dispatch_mode or "legacy").strip().lower()
    if mode == "legacy":
        return api, request_body, False, None
    if mode not in {"shadow", "token_ids"}:
        raise DecodeTokenEnvelopeError(
            f"unsupported Decode dispatch mode: {mode}"
        )

    started = time.perf_counter()
    prefill_report = dict(prefill_client.get("capabilities") or {})
    if not bool(
        prefill_report.get("prompt_envelope_metadata_patch_installed", False)
    ):
        await _refresh_prefill_capability(prefill_client)
        prefill_report = dict(prefill_client.get("capabilities") or {})
    if not bool(
        prefill_report.get("prompt_envelope_metadata_patch_installed", False)
    ):
        raise DecodeTokenEnvelopeError(
            "Prefill prompt-envelope metadata capture is not installed"
        )
    model_hash = _decode_model_identity(
        prefill_client,
        decode_client,
        request_body,
    )
    capability_ok, capability_error = _verified_decode_token_capability(
        decode_client,
        request_body,
        expected_model_id_sha256=str(
            prefill_report.get("model_id_sha256") or model_hash
        ),
    )
    if not capability_ok:
        # vLLM can still be compiling during the lifespan probe. Refresh once
        # on warmup and retain the verified report for measured traffic.
        await _refresh_worker_capability(decode_client)
        model_hash = _decode_model_identity(
            prefill_client,
            decode_client,
            request_body,
        )
        capability_ok, capability_error = _verified_decode_token_capability(
            decode_client,
            request_body,
            expected_model_id_sha256=str(
                prefill_report.get("model_id_sha256") or model_hash
            ),
        )
    _set_proxy_timing(
        request_body,
        "decode_token_capability_verified",
        float(capability_ok),
    )
    if not capability_ok:
        raise DecodeTokenEnvelopeError(
            "Decode token capability is not verified: " + capability_error
        )
    # Validate cross-worker rendering identity only after the lazy Decode
    # capability refresh.  During cold vLLM compilation the lifespan probe can
    # legitimately miss the Decode endpoint; comparing against that empty
    # snapshot would incorrectly reject an otherwise ready worker.
    _verify_prefill_decode_render_contract(
        prefill_client,
        decode_client,
    )

    envelope = build_decode_token_envelope(
        api=api,
        request_body=request_body,
        prefill_response=prefill_response,
        kv_transfer_params=kv_transfer_params,
        prefill_worker_id=str(prefill_client.get("worker_id") or ""),
        decode_worker_id=str(decode_client.get("worker_id") or ""),
        model_id_sha256=model_hash,
        multimodal_feature_hashes=list(getattr(ctx, "mm_hashes", []) or []),
    )
    _set_proxy_timing(request_body, "decode_token_envelope_validated", 1.0)
    _set_proxy_timing(
        request_body,
        "decode_token_envelope_build_ms",
        (time.perf_counter() - started) * 1000.0,
    )
    _set_proxy_timing(
        request_body,
        "decode_token_prompt_tokens",
        float(len(envelope.prompt_token_ids)),
    )
    if mode == "shadow":
        _set_proxy_timing(request_body, "decode_token_shadow_only", 1.0)
        return api, request_body, False, envelope

    _set_proxy_timing(request_body, "decode_token_fast_path", 1.0)
    _set_proxy_timing(request_body, "decode_token_media_stripped", 1.0)
    try:
        wire_api, payload, adapt_completion_to_chat = (
            _build_token_decode_payload(
                api=api,
                request_body=request_body,
                kv_transfer_params=kv_transfer_params,
                envelope=envelope,
            )
        )
    except DecodeTokenEnvelopeError:
        if (
            not config.strict_no_fallback
            and config.allow_decode_token_fallback
        ):
            _set_proxy_timing(request_body, "decode_token_fast_path", 0.0)
            _set_proxy_timing(
                request_body, "decode_token_media_stripped", 0.0
            )
            _set_proxy_timing(
                request_body, "decode_token_legacy_fallback", 1.0
            )
            return api, request_body, False, envelope
        raise
    return wire_api, payload, adapt_completion_to_chat, envelope


def _completion_payload_to_chat(payload: Dict[str, Any]) -> Dict[str, Any]:
    converted = dict(payload)
    converted["object"] = "chat.completion"
    raw_id = str(converted.get("id") or "")
    if raw_id.startswith("cmpl-"):
        converted["id"] = "chatcmpl-" + raw_id[len("cmpl-") :]
    choices: List[Dict[str, Any]] = []
    prompt_token_ids = None
    for raw_choice in list(converted.get("choices") or []):
        choice = dict(raw_choice or {})
        if prompt_token_ids is None and isinstance(
            choice.get("prompt_token_ids"), list
        ):
            prompt_token_ids = list(choice["prompt_token_ids"])
        text = str(choice.pop("text", "") or "")
        choice.pop("prompt_token_ids", None)
        choice["message"] = {"role": "assistant", "content": text}
        choices.append(choice)
    converted["choices"] = choices
    if prompt_token_ids is not None:
        converted["prompt_token_ids"] = prompt_token_ids
    return converted


def _completion_stream_packet_to_chat(
    packet: Dict[str, Any],
    *,
    include_role: bool,
) -> tuple[Dict[str, Any], bool]:
    converted = dict(packet)
    converted["object"] = "chat.completion.chunk"
    raw_id = str(converted.get("id") or "")
    if raw_id.startswith("cmpl-"):
        converted["id"] = "chatcmpl-" + raw_id[len("cmpl-") :]
    choices: List[Dict[str, Any]] = []
    role_pending = include_role
    for raw_choice in list(converted.get("choices") or []):
        choice = dict(raw_choice or {})
        text = str(choice.pop("text", "") or "")
        choice.pop("prompt_token_ids", None)
        delta: Dict[str, Any] = {}
        if role_pending:
            delta["role"] = "assistant"
            role_pending = False
        if text:
            delta["content"] = text
        choice["delta"] = delta
        choices.append(choice)
    converted["choices"] = choices
    return converted, role_pending


async def _dispatch_non_streaming_decode(
    *,
    api: str,
    control_plane: ServingControlPlane,
    ctx,
    decode_client: Dict[str, Any],
    decode_payload: Dict[str, Any],
    decode_headers: Dict[str, str],
    response_headers: Dict[str, str],
    continuation: _PrefillContinuation,
    wire_api: Optional[str] = None,
    adapt_completion_to_chat: bool = False,
) -> Response:
    decode_start = time.perf_counter()
    control_plane.mark_stage_started(
        "decode",
        decode_client["worker_id"],
        ctx=ctx,
    )
    decode_response = None
    success = False
    try:
        decode_metrics: Dict[str, float] = {}
        decode_request = _build_json_request_with_metrics(
            decode_client["client"],
            wire_api or api,
            decode_payload,
            decode_headers,
            decode_metrics,
            prefix="decode",
        )
        decode_response = await decode_client["client"].send(decode_request)
        decode_http_ms = (time.perf_counter() - decode_start) * 1000.0
        decode_response.raise_for_status()
        decode_metrics["decode_response_bytes"] = _response_content_bytes(decode_response)
        decode_json_decode_started_at = time.perf_counter()
        decode_json = decode_response.json()
        decode_metrics["decode_json_decode_ms"] = (
            time.perf_counter() - decode_json_decode_started_at
        ) * 1000.0
        if adapt_completion_to_chat:
            decode_json = _completion_payload_to_chat(dict(decode_json))
        patched_json = _patch_non_stream_payload(api, decode_json, continuation)
        decode_total_ms = (time.perf_counter() - decode_start) * 1000.0
        for metric_key, metric_value in decode_metrics.items():
            _merge_timing_header(response_headers, metric_key, metric_value)
        _merge_timing_header(response_headers, "decode_http_ms", decode_http_ms)
        _merge_timing_header(response_headers, "decode_total_ms", decode_total_ms)
        control_plane.mark_stage_first_token(
            "decode",
            decode_client["worker_id"],
            ctx=ctx,
            # Non-streaming OpenAI responses expose only completion time. Keep
            # the approximation explicit rather than leaving the request in
            # first_token_pending until completion.
            first_token_ms=decode_http_ms,
        )
        control_plane.commit_handoff(ctx)
        success = True
        return JSONResponse(
            patched_json,
            status_code=decode_response.status_code,
            headers=response_headers,
        )
    except Exception as exc:
        control_plane.rollback_handoff(ctx)
        raise HTTPException(status_code=502, detail=f"decode request failed: {exc}") from exc
    finally:
        if decode_response is not None:
            await decode_response.aclose()
        control_plane.mark_stage_complete(
            "decode",
            decode_client["worker_id"],
            latency_ms=(time.perf_counter() - decode_start) * 1000.0,
            success=success,
            ctx=ctx,
        )
        control_plane.finish_request(ctx.request_id)


async def _dispatch_streaming_decode(
    *,
    api: str,
    control_plane: ServingControlPlane,
    ctx,
    decode_client: Dict[str, Any],
    decode_payload: Dict[str, Any],
    decode_headers: Dict[str, str],
    response_headers: Dict[str, str],
    continuation: _PrefillContinuation,
    request_started_at: float | None = None,
    wire_api: Optional[str] = None,
    adapt_completion_to_chat: bool = False,
) -> StreamingResponse:
    decode_start = time.perf_counter()
    control_plane.mark_stage_started(
        "decode",
        decode_client["worker_id"],
        ctx=ctx,
    )
    decode_response = None
    try:
        decode_metrics: Dict[str, float] = {}
        decode_request = _build_json_request_with_metrics(
            decode_client["client"],
            wire_api or api,
            decode_payload,
            decode_headers,
            decode_metrics,
            prefix="decode",
        )
        decode_response = await decode_client["client"].send(
            decode_request,
            stream=True,
        )
        decode_response.raise_for_status()
        for metric_key, metric_value in decode_metrics.items():
            _merge_timing_header(response_headers, metric_key, metric_value)
        _merge_timing_header(
            response_headers,
            "decode_stream_open_ms",
            (time.perf_counter() - decode_start) * 1000.0,
        )
        if request_started_at is not None:
            _merge_timing_header(
                response_headers,
                "proxy_request_to_decode_stream_open_ms",
                (time.perf_counter() - request_started_at) * 1000.0,
            )
    except Exception as exc:
        control_plane.rollback_handoff(ctx)
        if decode_response is not None:
            try:
                await decode_response.aclose()
            except Exception:
                logger.exception("failed to close decode response after startup error")
        control_plane.mark_stage_complete(
            "decode",
            decode_client["worker_id"],
            latency_ms=(time.perf_counter() - decode_start) * 1000.0,
            success=False,
            ctx=ctx,
        )
        control_plane.finish_request(ctx.request_id)
        raise HTTPException(status_code=502, detail=f"decode request failed: {exc}") from exc

    async def generate_stream():
        success = True
        first_line_seen = False
        first_content_seen = False
        chat_role_pending = bool(adapt_completion_to_chat)
        pending_prefix = continuation.text if continuation.active else ""
        try:
            async for raw_line in decode_response.aiter_lines():
                if raw_line is None:
                    continue
                if raw_line == "":
                    continue
                line_out = raw_line
                if raw_line.startswith("data:"):
                    payload = raw_line[5:].strip()
                    if payload == "[DONE]":
                        line_out = "data: [DONE]"
                    elif (
                        not continuation.active
                        and first_content_seen
                        and not adapt_completion_to_chat
                    ):
                        # Prompt-only EPD has no semantic continuation to
                        # merge. After first-content timing is recorded, pass
                        # subsequent SSE packets through unchanged instead of
                        # JSON parsing and reserializing every decoded token.
                        line_out = raw_line
                    else:
                        try:
                            packet = json.loads(payload)
                        except Exception:
                            packet = None
                        if isinstance(packet, dict):
                            if adapt_completion_to_chat:
                                packet, chat_role_pending = (
                                    _completion_stream_packet_to_chat(
                                        packet,
                                        include_role=chat_role_pending,
                                    )
                                )
                            packet, pending_prefix = _patch_stream_packet(
                                api=api,
                                packet=packet,
                                continuation=continuation,
                                pending_prefix=pending_prefix,
                            )
                            packet_timings: Dict[str, float] = {}
                            now_ms = (time.perf_counter() - decode_start) * 1000.0
                            if not first_line_seen:
                                packet_timings["decode_first_event_ms"] = now_ms
                                if request_started_at is not None:
                                    packet_timings["proxy_request_to_first_event_ms"] = (
                                        time.perf_counter() - request_started_at
                                    ) * 1000.0
                            if (not first_content_seen) and _packet_content_delta(packet):
                                packet_timings["decode_first_content_ms"] = now_ms
                                if request_started_at is not None:
                                    packet_timings["proxy_request_to_first_content_ms"] = (
                                        time.perf_counter() - request_started_at
                                    ) * 1000.0
                                first_content_seen = True
                                control_plane.mark_stage_first_token(
                                    "decode",
                                    decode_client["worker_id"],
                                    ctx=ctx,
                                    first_token_ms=now_ms,
                                )
                            if packet_timings:
                                packet = _attach_packet_timings(packet, packet_timings)
                            line_out = f"data: {json.dumps(packet, ensure_ascii=False)}"
                if not first_line_seen:
                    control_plane.commit_handoff(ctx)
                    first_line_seen = True
                yield (line_out + "\n\n").encode("utf-8")
        except Exception:
            success = False
            if not first_line_seen:
                control_plane.rollback_handoff(ctx)
            raise
        finally:
            if success and not first_line_seen:
                success = False
                control_plane.rollback_handoff(ctx)
            try:
                await decode_response.aclose()
            finally:
                control_plane.mark_stage_complete(
                    "decode",
                    decode_client["worker_id"],
                    latency_ms=(time.perf_counter() - decode_start) * 1000.0,
                    success=success,
                    ctx=ctx,
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
