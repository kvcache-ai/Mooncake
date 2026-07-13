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
import json
import logging
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
from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig  # noqa: E402
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
    enable_direct_feature_handle_cache: bool = False
    direct_feature_handle_cache_max_entries: int = 4096
    direct_feature_handle_cache_ttl_s: float = 600.0
    direct_feature_singleflight_max_locks: int = 4096
    prefill_dispatch_mode: str = "render_generate"  # render_generate | openai_prompt_only
    # ``openai_prompt_only`` has a lower control-plane cost, but has not shown
    # output equivalence for every real vLLM P-D continuation configuration.
    # Strict serving therefore requires an explicit compatibility override.
    allow_unverified_openai_prompt_only: bool = False
    strict_no_fallback: bool = field(default_factory=strict_no_fallback_enabled)
    enable_agent_state_clone: bool = True
    high_prefill_worker_ids: List[str] = field(default_factory=list)
    low_latency_decode_worker_ids: List[str] = field(default_factory=list)
    standard_prefill_worker_ids: List[str] = field(default_factory=list)
    standard_decode_worker_ids: List[str] = field(default_factory=list)
    prefill_decode_affinity: List[Tuple[str, str]] = field(default_factory=list)


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
    parser.add_argument("--enable-direct-feature-handle-cache", action=argparse.BooleanOptionalAction, default=os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE", "0").lower() in {"1", "true", "yes", "on"})
    parser.add_argument("--direct-feature-handle-cache-max-entries", type=int, default=int(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE_MAX_ENTRIES", "4096")))
    parser.add_argument("--direct-feature-handle-cache-ttl-s", type=float, default=float(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_HANDLE_CACHE_TTL_S", "600")))
    parser.add_argument("--direct-feature-singleflight-max-locks", type=int, default=int(os.getenv("MOONCAKE_EPD_DIRECT_FEATURE_SINGLEFLIGHT_MAX_LOCKS", "4096")))
    parser.add_argument("--prefill-dispatch-mode", choices=["render_generate", "openai_prompt_only"], default=os.getenv("MOONCAKE_EPD_PREFILL_DISPATCH_MODE", "render_generate"))
    parser.add_argument(
        "--allow-unverified-openai-prompt-only",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_ALLOW_UNVERIFIED_OPENAI_PROMPT_ONLY", "0").lower()
        in {"1", "true", "yes", "on"},
        help="Permit openai_prompt_only in strict serving after workload-specific output-equivalence validation.",
    )
    parser.add_argument("--strict-no-fallback", action=argparse.BooleanOptionalAction, default=strict_no_fallback_enabled())
    parser.add_argument("--enable-agent-state-clone", action=argparse.BooleanOptionalAction, default=True)
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
        enable_direct_feature_handle_cache=bool(args.enable_direct_feature_handle_cache),
        direct_feature_handle_cache_max_entries=max(0, int(args.direct_feature_handle_cache_max_entries)),
        direct_feature_handle_cache_ttl_s=max(0.0, float(args.direct_feature_handle_cache_ttl_s)),
        direct_feature_singleflight_max_locks=max(16, int(args.direct_feature_singleflight_max_locks)),
        prefill_dispatch_mode=str(args.prefill_dispatch_mode),
        allow_unverified_openai_prompt_only=bool(args.allow_unverified_openai_prompt_only),
        strict_no_fallback=bool(args.strict_no_fallback),
        enable_agent_state_clone=bool(args.enable_agent_state_clone),
        high_prefill_worker_ids=list(args.high_prefill_worker_ids or []),
        low_latency_decode_worker_ids=list(args.low_latency_decode_worker_ids or []),
        standard_prefill_worker_ids=list(args.standard_prefill_worker_ids or []),
        standard_decode_worker_ids=list(args.standard_decode_worker_ids or []),
        prefill_decode_affinity=_parse_prefill_decode_affinity(
            args.prefill_decode_affinity
        ),
    )


def _make_client(base_url: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=None,
        base_url=base_url,
        limits=httpx.Limits(
            max_connections=None,
            max_keepalive_connections=None,
        ),
        trust_env=False,
    )


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


@asynccontextmanager
async def _lifespan(app: FastAPI):
    config: ProxyConfig = app.state.proxy_config
    control_plane: ServingControlPlane = app.state.control_plane
    prefill_overrides = getattr(app.state, "prefill_client_overrides", None)
    decode_overrides = getattr(app.state, "decode_client_overrides", None)

    if prefill_overrides is None:
        app.state.prefill_clients = [
            {
                "client": _make_client(f"http://{host}:{port}"),
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

    try:
        yield
    finally:
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
            high_prefill_worker_ids=tuple(config.high_prefill_worker_ids),
            low_latency_decode_worker_ids=tuple(config.low_latency_decode_worker_ids),
            standard_prefill_worker_ids=tuple(config.standard_prefill_worker_ids),
            standard_decode_worker_ids=tuple(config.standard_decode_worker_ids),
            prefill_decode_affinity=tuple(config.prefill_decode_affinity),
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
    app.state.direct_feature_handle_inflight_locks = {}
    app.state.direct_feature_handle_inflight_last_used = {}
    app.state.direct_feature_handle_singleflight_stats = {"created": 0, "joined": 0, "evicted": 0, "active": 0}
    app.state.direct_feature_release_queue = None
    app.state.direct_feature_release_workers = set()
    app.state.direct_feature_release_overflow = {}
    app.state.direct_feature_release_stats = {
        "scheduled": 0,
        "completed": 0,
        "failed": 0,
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
            locks = getattr(app.state, "direct_feature_handle_inflight_locks", None)
            if isinstance(locks, dict):
                stats["active"] = len(locks)
            payload["direct_feature_singleflight"] = stats
        direct_cache = getattr(app.state, "direct_feature_handle_cache", None)
        direct_cache_stats = getattr(app.state, "direct_feature_handle_cache_stats", None)
        if isinstance(direct_cache_stats, dict):
            stats = dict(direct_cache_stats)
            stats["entries"] = len(direct_cache) if isinstance(direct_cache, dict) else 0
            payload["direct_feature_handle_cache"] = stats
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
        )
        control_plane.finish_request(request_id)
        raise HTTPException(status_code=409, detail=f"agent state consume failed: {exc}") from exc

    response_headers = {
        "X-Request-Id": request_id,
        "X-EPD-Routing-Path": "AGENT_STATE",
        "X-EPD-Admission": decode_decision.decision.action.value,
        "X-EPD-Degrade-Level": ctx.degrade_level.value,
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
    req_data = await request.json()
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
        )
        control_plane.finish_request(request_id)
        raise HTTPException(status_code=503, detail="no prefill client available")

    if prefill_decision.wait_ms > 0:
        await asyncio.sleep(prefill_decision.wait_ms / 1000.0)

    prepare_start = time.perf_counter()
    try:
        req_data = await _prepare_multimodal_inputs_for_prefill(
            app=app,
            req_data=req_data,
            ctx=ctx,
            target_worker_id=prefill_decision.worker_id,
        )
        _set_proxy_timing(req_data, "mm_prepare_ms", (time.perf_counter() - prepare_start) * 1000.0)
    except HTTPException:
        control_plane.mark_stage_complete(
            "prefill",
            prefill_decision.worker_id,
            latency_ms=0.0,
            success=False,
        )
        control_plane.finish_request(request_id)
        raise

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
                if config.strict_no_fallback and not config.allow_unverified_openai_prompt_only:
                    raise ValueError(
                        "openai_prompt_only is not verified for strict P-D serving; "
                        "use render_generate or explicitly set "
                        "--allow-unverified-openai-prompt-only after output-equivalence validation"
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
            prefill_response = await prefill_client["client"].post(
                api,
                json=prefill_payload,
                headers=prefill_headers,
            )
            prefill_response.raise_for_status()
            prefill_json = prefill_response.json()
    except Exception as exc:
        control_plane.mark_stage_complete(
            "prefill",
            prefill_client["worker_id"],
            latency_ms=(time.perf_counter() - prefill_start) * 1000.0,
            success=False,
        )
        control_plane.finish_request(request_id)
        raise HTTPException(status_code=502, detail=f"prefill request failed: {exc}") from exc
    finally:
        if prefill_response is not None:
            await prefill_response.aclose()

    await _schedule_direct_feature_buffer_release(app, req_data)
    _set_proxy_timing(req_data, "prefill_ms", (time.perf_counter() - prefill_start) * 1000.0)

    control_plane.mark_stage_complete(
        "prefill",
        prefill_client["worker_id"],
        latency_ms=(time.perf_counter() - prefill_start) * 1000.0,
        success=True,
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
        )
        control_plane.finish_request(request_id)
        raise HTTPException(
            status_code=502,
            detail=f"prefill response missing usable KV handoff metadata: {exc}",
        ) from exc

    if decode_decision.wait_ms > 0:
        await asyncio.sleep(decode_decision.wait_ms / 1000.0)

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
        )
        control_plane.finish_request(request_id)
        raise HTTPException(
            status_code=502,
            detail=f"decode request missing usable KV transfer params: {exc}",
        ) from exc

    response_headers = {
        "X-Request-Id": request_id,
        "X-EPD-Routing-Path": ctx.routing_path,
        "X-EPD-Admission": decode_decision.decision.action.value,
        "X-EPD-Degrade-Level": ctx.degrade_level.value,
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


def _prune_singleflight_locks(app: FastAPI, *, max_locks: int) -> None:
    locks = getattr(app.state, "direct_feature_handle_inflight_locks", None)
    last_used = getattr(app.state, "direct_feature_handle_inflight_last_used", None)
    if not isinstance(locks, dict) or not isinstance(last_used, dict):
        return
    limit = max(16, int(max_locks))
    if len(locks) <= limit:
        return
    stats = _singleflight_stats(app)
    # Remove oldest currently-unlocked entries only.  Locked entries may have
    # waiters and must keep their rendezvous object until all waiters finish.
    candidates = sorted(last_used.items(), key=lambda item: float(item[1] or 0.0))
    target = max(0, limit // 2)
    for key, _ in candidates:
        if len(locks) <= target:
            break
        lock = locks.get(key)
        if lock is not None and getattr(lock, "locked", lambda: False)():
            continue
        if locks.pop(key, None) is not None:
            last_used.pop(key, None)
            stats["evicted"] = int(stats.get("evicted", 0) or 0) + 1


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
    followers wait for the lock, then re-check both proxy and Prefill
    persistent caches and return hot ``epd-direct://`` handles.
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
    locks = getattr(app.state, "direct_feature_handle_inflight_locks", None)
    if not isinstance(locks, dict):
        locks = {}
        app.state.direct_feature_handle_inflight_locks = locks
    last_used = getattr(app.state, "direct_feature_handle_inflight_last_used", None)
    if not isinstance(last_used, dict):
        last_used = {}
        app.state.direct_feature_handle_inflight_last_used = last_used
    stats = _singleflight_stats(app)
    lock = locks.get(key)
    if lock is None:
        _prune_singleflight_locks(
            app,
            max_locks=int(getattr(config, "direct_feature_singleflight_max_locks", 4096)),
        )
        lock = locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        locks[key] = lock
        stats["created"] = int(stats.get("created", 0) or 0) + 1
    else:
        stats["joined"] = int(stats.get("joined", 0) or 0) + int(bool(lock.locked()))
    last_used[key] = time.monotonic()
    async with lock:
        last_used[key] = time.monotonic()
        raw_handles = []
        if not config.release_direct_feature_buffers_after_prefill:
            raw_handles = _lookup_proxy_cached_direct_feature_handles(
                app=app,
                feature_ids=list(ids),
                target_worker_id=target_worker_id,
            )
        if not isinstance(raw_handles, list) or not raw_handles:
            raw_handles = await _lookup_prefill_cached_direct_feature_handles(
                app=app,
                feature_ids=list(ids),
                target_worker_id=target_worker_id,
            )
        if isinstance(raw_handles, list) and raw_handles:
            return [dict(item) for item in raw_handles]
        return await _request_feature_handles_from_encoder_service(
            app=app,
            req_data=req_data,
            target_worker_id=target_worker_id,
        )


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
    try:
        started = time.perf_counter()
        described_resp = await encoder_client.post("/describe", json=payload)
        described_resp.raise_for_status()
        described = described_resp.json()
        timings["direct_describe_ms"] = (time.perf_counter() - started) * 1000.0
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
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:1000] if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=f"prefill direct allocation returned error: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"prefill direct allocation failed: {exc}") from exc

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
    publish_payload = {
        "ticket": ticket,
        "metadata": dict(payload.get("metadata") or {}),
        "mooncake_epd_direct_feature_targets": targets,
        "mooncake_epd_direct_publish_mask": publish_mask,
    }
    try:
        started = time.perf_counter()
        publish_resp = await encoder_client.post("/publish_direct", json=publish_payload)
        publish_resp.raise_for_status()
        published = publish_resp.json()
        timings["direct_publish_ms"] = (time.perf_counter() - started) * 1000.0
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:1000] if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=f"encoder direct publish returned error: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"encoder direct publish failed: {exc}") from exc
    handles = published.get("handles")
    if not isinstance(handles, list) or not handles:
        raise HTTPException(status_code=502, detail="encoder direct publish returned no feature handles")
    ready_feature_ids = [
        str(handle.get("feature_id") or "")
        for handle, should_publish in zip(handles, publish_mask)
        if should_publish and isinstance(handle, dict) and str(handle.get("feature_id") or "")
    ]
    if ready_feature_ids:
        try:
            started = time.perf_counter()
            ready_resp = await direct_client.post(
                "/mooncake_epd/direct_feature_buffer/mark_ready",
                json={"feature_ids": sorted(set(ready_feature_ids))},
            )
            ready_resp.raise_for_status()
            timings["direct_mark_ready_ms"] = (time.perf_counter() - started) * 1000.0
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text[:1000] if exc.response is not None else str(exc)
            raise HTTPException(status_code=502, detail=f"prefill direct mark_ready returned error: {detail}") from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"prefill direct mark_ready failed: {exc}") from exc
    wait_feature_ids = [
        str(handle.get("feature_id") or "")
        for handle, should_publish in zip(handles, publish_mask)
        if (not should_publish) and isinstance(handle, dict) and str(handle.get("feature_id") or "")
    ]
    if wait_feature_ids:
        try:
            started = time.perf_counter()
            wait_resp = await direct_client.post(
                "/mooncake_epd/direct_feature_buffer/wait_ready",
                json={
                    "feature_ids": sorted(set(wait_feature_ids)),
                    "timeout_s": max(1.0, float(getattr(app.state.proxy_config, "prefill_direct_buffer_timeout_s", 30.0))),
                },
            )
            wait_resp.raise_for_status()
            timings["direct_wait_ready_ms"] = (time.perf_counter() - started) * 1000.0
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text[:1000] if exc.response is not None else str(exc)
            raise HTTPException(status_code=502, detail=f"prefill direct wait_ready returned error: {detail}") from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"prefill direct wait_ready failed: {exc}") from exc
    # Surface encoder-side direct-engine sub-timings in the same response
    # header as proxy timings. This makes real serving TTFT attribution precise:
    # /publish_direct wall time can now be split into tensor registration,
    # Mooncake peer-buffer write, unregister, and any staging/build overhead.
    for handle in handles:
        if not isinstance(handle, dict):
            continue
        md = dict(handle.get("metadata") or {})
        sub = md.get("direct_transfer_timings_ms")
        if not isinstance(sub, dict):
            continue
        for key, value in sub.items():
            try:
                timings[f"direct_publish_engine_{key}"] = (
                    float(timings.get(f"direct_publish_engine_{key}", 0.0) or 0.0)
                    + float(value or 0.0)
                )
            except Exception:
                continue
    return_items: List[Dict[str, Any]] = []
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
    direct_client = _prefill_direct_buffer_client_for(app, target_worker_id)
    if direct_client is None:
        return []
    started = time.perf_counter()
    try:
        resp = await direct_client.post(
            "/mooncake_epd/direct_feature_buffer/lookup",
            json={"feature_ids": ids, "target_worker_id": target_worker_id},
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        logger.debug("prefill direct cache lookup failed", exc_info=True)
        return []
    if not bool(payload.get("all_hit")):
        # Release any acquired partial hits immediately; mixed all-hit/all-miss
        # composition is handled by the normal direct publish path below.
        hits = [str(item.get("feature_id") or "") for item in list(payload.get("hits") or []) if isinstance(item, dict)]
        if hits:
            try:
                await direct_client.post(
                    "/mooncake_epd/direct_feature_buffer/release",
                    json={"feature_ids": hits},
                )
            except Exception:
                logger.debug("prefill direct partial-cache release failed", exc_info=True)
        return []
    handles: List[Dict[str, Any]] = []
    lookup_ms = (time.perf_counter() - started) * 1000.0
    by_id = {
        str(item.get("feature_id") or ""): dict(item)
        for item in list(payload.get("hits") or [])
        if isinstance(item, dict)
    }
    for feature_id in ids:
        item = by_id.get(feature_id)
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
                "source_mm_hash": feature_id,
                "mooncake_epd_target_worker_id": str(target_worker_id),
                "target_worker_id": str(target_worker_id),
                "proxy_direct_timings_ms": {
                    "direct_cache_lookup_ms": lookup_ms,
                    "direct_cache_hits": float(len(ids)),
                    "direct_describe_ms": 0.0,
                    "direct_allocate_ms": 0.0,
                    "direct_publish_ms": 0.0,
                },
            },
        )
        handles.append(handle.as_control_payload())
    return handles


def _build_direct_handle_from_target(
    *,
    descriptor: FeatureBundleDescriptor,
    target: Dict[str, Any],
    store_id: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> FeatureHandle:
    remote_session = str(target.get("remote_session") or "")
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
    stats = getattr(app.state, "direct_feature_release_stats", None)
    if not isinstance(stats, dict):
        stats = {
            "scheduled": 0,
            "completed": 0,
            "failed": 0,
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
    return {
        worker_id: sorted(set(feature_ids))
        for worker_id, feature_ids in by_worker.items()
        if feature_ids
    }


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
                json={"feature_ids": sorted(set(feature_ids)), "target_worker_id": worker_id},
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


def _merge_direct_feature_release_overflow(
    app: FastAPI,
    by_worker: Dict[str, List[str]],
) -> None:
    overflow = getattr(app.state, "direct_feature_release_overflow", None)
    if not isinstance(overflow, dict):
        overflow = {}
        app.state.direct_feature_release_overflow = overflow
    for worker_id, feature_ids in by_worker.items():
        bucket = overflow.setdefault(str(worker_id), set())
        bucket.update(str(feature_id) for feature_id in feature_ids)


def _take_direct_feature_release_overflow(app: FastAPI) -> Dict[str, List[str]]:
    overflow = getattr(app.state, "direct_feature_release_overflow", None)
    if not isinstance(overflow, dict) or not overflow:
        return {}
    app.state.direct_feature_release_overflow = {}
    return {
        str(worker_id): sorted(str(feature_id) for feature_id in feature_ids)
        for worker_id, feature_ids in overflow.items()
        if feature_ids
    }


async def _direct_feature_release_worker(
    app: FastAPI,
    queue: asyncio.Queue[Dict[str, List[str]] | None],
) -> None:
    while True:
        job = await queue.get()
        try:
            if job is None:
                return
            failures = await _release_direct_feature_buffer_ids(app, job)
            stats = getattr(app.state, "direct_feature_release_stats", None)
            if isinstance(stats, dict):
                stats["completed"] = int(stats.get("completed", 0) or 0) + 1
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


async def _dispatch_prompt_only_prefill(
    *,
    api: str,
    prefill_client: Dict[str, Any],
    prefill_headers: Dict[str, str],
    request_id: str,
    request_body: Dict[str, Any],
    kv_transfer_params: Dict[str, Any],
) -> Dict[str, Any]:
    timings: Dict[str, float] = {}
    started = time.perf_counter()
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
    timings["prefill_render_ms"] = (time.perf_counter() - started) * 1000.0

    prefill_payload = _inject_prefill_kv_into_rendered_request(
        rendered_payload,
        request_id=request_id,
        kv_transfer_params=kv_transfer_params,
    )
    started = time.perf_counter()
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
    timings["prefill_generate_ms"] = (time.perf_counter() - started) * 1000.0
    timings["prefill_render_generate_ms"] = timings["prefill_render_ms"] + timings["prefill_generate_ms"]
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

    response = await prefill_client["client"].post(
        api,
        json=prefill_payload,
        headers=prefill_headers,
    )
    try:
        response.raise_for_status()
        payload = dict(response.json())
    finally:
        await response.aclose()
    timings = {
        "prefill_openai_prompt_only_ms": (time.perf_counter() - started) * 1000.0,
        "prefill_render_ms": 0.0,
        "prefill_generate_ms": 0.0,
    }
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
) -> Response:
    decode_start = time.perf_counter()
    decode_response = None
    success = False
    try:
        decode_response = await decode_client["client"].post(api, json=decode_payload, headers=decode_headers)
        decode_http_ms = (time.perf_counter() - decode_start) * 1000.0
        decode_response.raise_for_status()
        decode_json = decode_response.json()
        patched_json = _patch_non_stream_payload(api, decode_json, continuation)
        decode_total_ms = (time.perf_counter() - decode_start) * 1000.0
        _merge_timing_header(response_headers, "decode_http_ms", decode_http_ms)
        _merge_timing_header(response_headers, "decode_total_ms", decode_total_ms)
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
) -> StreamingResponse:
    decode_start = time.perf_counter()
    stream_ctx = decode_client["client"].stream("POST", api, json=decode_payload, headers=decode_headers)
    decode_response = None
    stream_entered = False
    try:
        decode_response = await stream_ctx.__aenter__()
        stream_entered = True
        decode_response.raise_for_status()
        _merge_timing_header(
            response_headers,
            "decode_stream_open_ms",
            (time.perf_counter() - decode_start) * 1000.0,
        )
    except Exception as exc:
        control_plane.rollback_handoff(ctx)
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
        control_plane.mark_stage_complete(
            "decode",
            decode_client["worker_id"],
            latency_ms=(time.perf_counter() - decode_start) * 1000.0,
            success=False,
        )
        control_plane.finish_request(ctx.request_id)
        raise HTTPException(status_code=502, detail=f"decode request failed: {exc}") from exc

    async def generate_stream():
        success = True
        first_line_seen = False
        first_content_seen = False
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
                    elif not continuation.active and first_content_seen:
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
                            if (not first_content_seen) and _packet_content_delta(packet):
                                packet_timings["decode_first_content_ms"] = now_ms
                                first_content_seen = True
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
                await stream_ctx.__aexit__(None, None, None)
                control_plane.mark_stage_complete(
                    "decode",
                    decode_client["worker_id"],
                    latency_ms=(time.perf_counter() - decode_start) * 1000.0,
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
