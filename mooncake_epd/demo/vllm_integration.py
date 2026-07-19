"""Generate runnable vLLM + MooncakeConnector configs for this checkout.

Model and Python-environment locations are resolved from deployment variables
or repository-relative defaults. The generated commands opt into the repo-local
MooncakeConnector so layered transfer scheduling and serving control-plane
metadata are available on the real vLLM serving path.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import shlex
import socket
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Tuple

from mooncake_epd.core.transfer import validate_mooncake_protocol_pair

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _resolve_venv_root() -> Path:
    override = os.getenv("MOONCAKE_EPD_VENV_ROOT")
    candidates = []
    if override:
        candidates.append(Path(override).expanduser())
    if sys.prefix != sys.base_prefix:
        candidates.append(Path(sys.prefix))
    candidates.extend(
        [
            REPO_ROOT.parent / ".venv",
            REPO_ROOT.parent.parent / "venv_mooncake",
            REPO_ROOT.parent / "venv_mooncake",
        ]
    )
    for candidate in candidates:
        if (candidate / "bin" / "python").exists():
            return candidate
    return candidates[0]


def _resolve_model_path() -> str:
    raw = os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT.parent / path
    return str(path.resolve(strict=False))


# Backward-compatible snapshot for runners that build subprocess commands at
# import time. Config generation resolves the deployment root again so an
# explicit environment override remains authoritative.
VENV_ROOT = _resolve_venv_root()
MODEL_PATH = str(REPO_ROOT.parent / "models" / "Qwen3-VL-8B-Instruct")
CONNECTOR_MODULE_PATH = "mooncake_epd.core.control.vllm_mooncake_connector"
# Keep the deployment generator and proxy CLI on one explicit policy contract.
# A benchmark must record the selected policy; silently falling back to the
# proxy default makes a 2P2D capacity result impossible to reproduce.
SCHEDULER_POLICIES = frozenset(
    {"round_robin", "least_loaded", "static_type_route", "agent_aware"}
)

# vLLM's Mooncake sender binds its ZMQ side channel from the kernel ephemeral
# range before it starts the upstream Bootstrap HTTP server.  Selecting a
# Bootstrap port from that same range can therefore make one EngineCore race
# with itself: the ZMQ listener wins, then Bootstrap fails to bind.  Keep
# generated (non-explicit) Bootstrap ports outside the Linux ephemeral range
# while retaining a wide enough pool for independent local deployments.
_BOOTSTRAP_PORT_MIN = 20_000
_BOOTSTRAP_PORT_MAX = 29_999


def _effective_direct_engine_protocol(config: "VLLMDisaggConfig") -> str:
    protocol = str(config.protocol).strip().lower()
    direct = str(config.direct_engine_protocol or "").strip().lower()
    return direct or ("tcp" if protocol == "shm" else protocol)


def _port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, port)) == 0


def _pick_free_port(preferred: int, host: str = "127.0.0.1") -> int:
    if preferred > 0 and not _port_in_use(preferred, host):
        return preferred
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _can_bind_bootstrap_port(port: int) -> bool:
    """Check the wildcard bind used by vLLM's Bootstrap server.

    A TCP connect check against ``local_hostname`` is not sufficient here:
    upstream binds Bootstrap on ``0.0.0.0``, so an existing listener on another
    loopback alias can still cause startup to fail.  The socket is only a
    probe; Bootstrap owns the actual bind at worker startup.
    """

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("0.0.0.0", int(port)))
    except OSError:
        return False
    return True


def _bootstrap_port_candidates(seed: str):
    """Yield a stable permutation of the non-ephemeral Bootstrap port pool."""

    width = _BOOTSTRAP_PORT_MAX - _BOOTSTRAP_PORT_MIN + 1
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    start = int.from_bytes(digest[:8], "big") % width
    for offset in range(width):
        yield _BOOTSTRAP_PORT_MIN + ((start + offset) % width)


def _pick_bootstrap_port(
    preferred: int,
    *,
    seed: str,
    reserved: set[int],
) -> int:
    """Select a unique Bootstrap port without entering the ephemeral range.

    Explicit ports remain supported for deployment compatibility.  Generated
    ports intentionally use the non-ephemeral range because vLLM allocates the
    Mooncake ZMQ side channel dynamically during EngineCore initialization.
    """

    requested = int(preferred)
    if (
        requested > 0
        and requested not in reserved
        and _can_bind_bootstrap_port(requested)
    ):
        return requested
    for candidate in _bootstrap_port_candidates(seed):
        if candidate not in reserved and _can_bind_bootstrap_port(candidate):
            return candidate
    raise RuntimeError(
        "no free non-ephemeral vLLM Mooncake Bootstrap port is available"
    )


@dataclass
class VLLMDisaggConfig:
    model: str = field(default_factory=_resolve_model_path)
    prefill_port: int = 8100
    decode_port: int = 8200
    proxy_port: int = 8000
    metadata_port: int = 8090
    master_port: int = 50061
    master_metrics_port: int = 59003
    prefill_bootstrap_port: int = 0
    decode_bootstrap_port: int = 0
    tensor_parallel_size: int = 1
    max_model_len: int = 4096
    gpu_memory_utilization: float = 0.65
    # Keep vLLM scheduler capacity explicit in benchmark artifacts. ``None``
    # preserves upstream defaults; production tuning can raise these only after
    # a real workload proves a gain.
    max_num_batched_tokens: Optional[int] = None
    max_num_seqs: Optional[int] = None
    # Benchmark and correctness-sensitive deployments must not silently inherit
    # a model repository's sampling defaults. ``None`` retains vLLM's default;
    # runners opt into ``vllm`` explicitly and record it in their artifact.
    generation_config: Optional[str] = None
    protocol: str = "tcp"
    # TCP's sender-side write completion is not a remote GPU-write completion.
    # The patched Transfer Engine can wait for the receiver's post-cudaMemcpy
    # acknowledgement. Keep it enabled for generated EPD deployments and make
    # the switch explicit in artifacts for valid latency/throughput ablations.
    tcp_write_completion_ack: bool = True
    # Reusing TCP connections avoids connect setup, but the current TCP
    # transport implementation has not yet passed the strict multimodal
    # response-consistency campaign under concurrent P→D descriptor traffic.
    # Never inherit this experimental mode from the launcher shell: generated
    # deployments explicitly disable it unless the caller opts in and records
    # a workload-specific correctness result.
    tcp_connection_pool: bool = False
    # E→P FeatureBundle and P→D KV can use different data-plane transports.
    # On a host where an RDMA HCA cannot register the short-lived E→P buffers,
    # retaining TCP for E→P still lets the persistent vLLM KV connector use
    # RDMA.  None follows the main P→D protocol (except SHM, whose direct
    # engine fallback remains TCP until CUDA-IPC registration is available).
    direct_engine_protocol: Optional[str] = None
    prefill_gpu: int = 3
    decode_gpu: int = 4
    prefill_gpus: Tuple[int, ...] = ()
    decode_gpus: Tuple[int, ...] = ()
    prefill_ports: Tuple[int, ...] = ()
    decode_ports: Tuple[int, ...] = ()
    prefill_bootstrap_ports: Tuple[int, ...] = ()
    decode_bootstrap_ports: Tuple[int, ...] = ()
    local_hostname: str = "127.0.0.1"
    global_segment_size: int = 1073741824
    local_buffer_size: int = 268435456
    # Qwen-VL real serving showed that larger layer groups reduce P→D
    # peer-buffer handshakes and Decode first-token latency without fallback.
    # lpg32/64MiB/512desc improved avg TTFT from ~315ms to ~258ms versus the
    # earlier lpg16/32MiB/256desc default on the same 2P4D Qwen3-VL setup.
    layers_per_group: int = 32
    group_delay_ms: float = 0.0
    max_group_bytes: int = 64 * 1024 * 1024
    max_transfer_descriptors: int = 512
    max_transfer_bytes: int = 64 * 1024 * 1024
    # ``0`` selects a protocol-aware sender topology. A small NVLink/CUDA-IPC
    # sender pool overlaps host-side planning with P2P submission while
    # avoiding the link contention observed with four independent streams.
    # TCP/RDMA keep the established four-worker default for network progress.
    transfer_workers: int = 0
    allow_transfer_fallback: bool = False
    transfer_retry_attempts: int = 6
    transfer_retry_backoff_ms: float = 250.0
    # Explicit P->D locality pairs. When empty, nvlink_intra deployments pair
    # list-aligned Prefill/Decode workers so callers can order GPU lists by
    # physical topology while retaining admission-time overload escape.
    prefill_decode_affinity: Tuple[Tuple[str, str], ...] = ()
    proxy_warn_rho: float = 0.85
    proxy_critical_rho: float = 0.95
    proxy_max_backpressure_delay_ms: float = 150.0
    # The routing policy is a serving parameter, not a benchmark-only proxy
    # flag.  Expose it here so generated deployment scripts and benchmark
    # artifacts describe the policy that actually selected P/D workers.
    scheduler_policy: str = "agent_aware"
    owner_shards: int = 1
    kv_directory_rpc_url: Optional[str] = None
    # A sync-on-every-transition workflow WAL is required for restart-safe
    # Agent-state recovery, but it is not on the correctness path of ordinary
    # P→D serving.  Keep the durable default for generated deployments while
    # allowing the serving benchmark to select the in-memory hot path
    # explicitly and report that durability boundary in its artifact.
    enable_workflow_registry_wal: bool = True
    workflow_registry_wal_path: Optional[str] = None
    connector_metrics_dir: Optional[str] = None
    # Keep compiler artifacts off the usually space-constrained system volume.
    # This is not a serving cache: it only avoids failed/repeated compilation
    # during real multi-model benchmark campaigns.
    vllm_cache_root: Optional[str] = None
    # Group-level connector accounting is written periodically and at terminal
    # boundaries; eager JSON snapshots are too expensive on the TCP P→D path.
    connector_metrics_flush_interval_ms: float = 250.0
    mm_prefetch_mode: str = "asset_bytes"
    prefill_supports_feature_handles: bool = False
    encoder_service_url: Optional[str] = None
    prefill_direct_buffer_service_url: Optional[str] = None
    prefill_direct_buffer_service_urls: Tuple[str, ...] = ()
    enable_prefill_direct_feature_buffer_routes: bool = False
    enable_direct_feature_handle_cache: bool = False
    direct_feature_handle_cache_max_entries: int = 4096
    direct_feature_handle_cache_ttl_s: float = 600.0
    # Safe only with release-after-Prefill: reserve a bounded number of
    # registry references for a generation-fenced repeated descriptor tuple.
    # ``1`` keeps the historical serial one-lookup behavior; adaptive
    # coalescing below may still serve a concurrent burst safely.
    direct_feature_lease_prefetch: int = 1
    direct_feature_lease_prefetch_max_entries: int = 64
    direct_feature_lease_prefetch_ttl_s: float = 30.0
    # With the fixed one-reference default, reserve extra references only for
    # requests that are already concurrently waiting on the same descriptor.
    # This preserves single-request TTFT behavior while reducing hot-burst
    # Prefill registry RPC contention.
    # Opt in only after a strict workload-specific run: coalescing changes the
    # timing of concurrent admissions even though it preserves reference count.
    direct_feature_adaptive_lease_prefetch: bool = False
    direct_feature_adaptive_lease_prefetch_max: int = 2
    # Cache vLLM's deterministic OpenAI /render output on the Proxy. This is
    # distinct from the E->P direct FeatureHandle cache: it removes repeated
    # control-plane rendering while every request still carries fresh P->D KV
    # transfer parameters.
    enable_rendered_prefill_cache: bool = True
    rendered_prefill_cache_max_entries: int = 64
    rendered_prefill_cache_max_bytes: int = 256 * 1024 * 1024
    rendered_prefill_cache_ttl_s: float = 300.0
    direct_feature_buffer_root_routes: bool = False
    direct_feature_target_mode: str = "registered_tensor"
    direct_feature_register_memory: bool = True
    direct_feature_persistent_cache: bool = False
    direct_feature_cache_max_entries: int = 64
    direct_feature_cache_max_bytes: int = 2 * 1024 * 1024 * 1024
    # ``None`` preserves the deployment environment's current default.  A
    # benchmark may pin this switch so a cold/no-cache control cannot silently
    # inherit a warm worker-level multimodal hidden-state cache.
    vllm_mm_hidden_cache: Optional[bool] = None
    direct_feature_buffer_auth_token: Optional[str] = None
    release_direct_feature_buffers_after_prefill: bool = True
    # Release is off the response path; a small batch reduces HTTP control
    # traffic against the Prefill vLLM API without changing lease accounting.
    direct_feature_release_batch_max_jobs: int = 16
    strict_no_fallback: bool = False
    # ``render_generate`` is the verified OpenAI-compatible continuation path.
    # The one-call ``openai_prompt_only`` variant may be selected explicitly
    # only for a vLLM version/workload pair that has passed output-equivalence
    # validation; see the proxy's strict-serving gate.
    prefill_dispatch_mode: str = "render_generate"
    # Validate the v1 token envelope in shadow before enabling the compact
    # media-free Decode leg for a real workload.
    decode_dispatch_mode: str = "legacy"
    allow_decode_token_fallback: bool = False
    # Disable idle Proxy->Prefill HTTP/1.1 connection reuse by default. A
    # transport error after a non-idempotent Prefill POST cannot be retried
    # safely because the worker may have already produced a P->D KV handoff.
    prefill_http_keepalive: bool = False
    allow_unverified_openai_prompt_only: bool = False
    feature_handle_store_url: Optional[str] = None
    feature_handle_store_id: Optional[str] = None
    feature_handle_store_config: Optional[str] = None
    feature_handle_store_timeout_s: float = 30.0
    feature_handle_require_checksum: bool = False
    feature_handle_http_binary_payload: bool = False

    @property
    def metadata_server(self) -> str:
        return f"http://{self.local_hostname}:{self.metadata_port}/metadata"

    @property
    def master_server(self) -> str:
        return f"{self.local_hostname}:{self.master_port}"

    def to_mooncake_json(self) -> Dict[str, object]:
        return {
            "local_hostname": self.local_hostname,
            "metadata_server": self.metadata_server,
            "global_segment_size": self.global_segment_size,
            "local_buffer_size": self.local_buffer_size,
            "protocol": self.protocol,
            "device_name": "",
            "master_server_address": self.master_server,
        }

    def kv_transfer_config(self, role: str, engine_id: str) -> Dict[str, object]:
        extra_config: Dict[str, object] = {
            "mooncake_protocol": self.protocol,
            "num_workers": self.effective_transfer_workers,
            "layered_kv_transfer": True,
            "layers_per_group": self.layers_per_group,
            "group_delay_ms": self.group_delay_ms,
            "max_group_bytes": self.max_group_bytes,
            "max_transfer_descriptors": self.max_transfer_descriptors,
            "max_transfer_bytes": self.max_transfer_bytes,
            "allow_transfer_fallback": self.allow_transfer_fallback,
            "transfer_retry_attempts": self.transfer_retry_attempts,
            "transfer_retry_backoff_ms": self.transfer_retry_backoff_ms,
            "transport_backend": "mooncake_engine_direct",
            # Strict real runs reject a Decode pull that lacks the checksummed
            # P->D envelope. Compatibility checking stays opt-in until every
            # worker reports a complete model/layout revision tuple.
            "require_kv_transfer_manifest_v2": bool(self.strict_no_fallback),
            "require_kv_transfer_manifest_generation": bool(self.strict_no_fallback),
        }
        if self.connector_metrics_dir:
            extra_config["connector_metrics_dir"] = self.connector_metrics_dir
            extra_config["connector_metrics_flush_interval_ms"] = max(
                0.0,
                float(self.connector_metrics_flush_interval_ms),
            )
        return {
            "kv_connector": "MooncakeConnector",
            "kv_role": role,
            "engine_id": engine_id,
            "kv_connector_module_path": CONNECTOR_MODULE_PATH,
            "kv_connector_extra_config": extra_config,
        }

    @property
    def effective_transfer_workers(self) -> int:
        configured = int(self.transfer_workers or 0)
        if configured > 0:
            return configured
        if str(self.protocol).strip().lower() == "nvlink_intra":
            return 2
        return 4

    @property
    def resolved_prefill_decode_affinity(self) -> Tuple[Tuple[str, str], ...]:
        if self.prefill_decode_affinity:
            return tuple(
                (str(prefill).strip(), str(decode).strip())
                for prefill, decode in self.prefill_decode_affinity
                if str(prefill).strip() and str(decode).strip()
            )
        if str(self.protocol).strip().lower() != "nvlink_intra":
            return ()
        prefill_count = len(self.prefill_gpus or (self.prefill_gpu,))
        decode_count = len(self.decode_gpus or (self.decode_gpu,))
        return tuple(
            (f"prefill-{idx}", f"decode-{idx}")
            for idx in range(min(prefill_count, decode_count))
        )


def _expand_ints(primary: int, values: Tuple[int, ...], count: int, *, fill: int = 0) -> list[int]:
    if values:
        out = [int(v) for v in values]
    else:
        out = [int(primary)]
    while len(out) < count:
        out.append(int(fill))
    return out[:count]


def validate_environment(config: Optional[VLLMDisaggConfig] = None) -> Dict[str, object]:
    config = config or VLLMDisaggConfig()
    venv_root = _resolve_venv_root()
    checks = {
        "model_exists": Path(config.model).exists(),
        "venv_exists": venv_root.exists(),
        "vllm_bin": str(venv_root / "bin" / "vllm"),
        "mooncake_master_bin": str(venv_root / "bin" / "mooncake_master"),
        "python_bin": str(venv_root / "bin" / "python"),
        "proxy_script": str(REPO_ROOT / "scripts" / "vllm_disagg_proxy.py"),
        "connector_module": CONNECTOR_MODULE_PATH,
    }
    checks["vllm_bin_exists"] = Path(checks["vllm_bin"]).exists()
    checks["mooncake_master_exists"] = Path(checks["mooncake_master_bin"]).exists()
    checks["python_bin_exists"] = Path(checks["python_bin"]).exists()
    checks["proxy_script_exists"] = Path(checks["proxy_script"]).exists()
    return checks


def _common_env_block(
    config: VLLMDisaggConfig,
    mooncake_json: Path,
    *,
    bootstrap_port: Optional[int] = None,
) -> str:
    # ``REPO_ROOT`` is the package root (``.../mooncake_epd``); importing the
    # package requires the containing Mooncake checkout on PYTHONPATH.  Keep this
    # explicit so generated scripts are portable between the standalone tree and
    # the merged Mooncake repository.
    parent_path = str(REPO_ROOT.parent)
    protocol = str(config.protocol).strip().lower()
    direct_engine_protocol = _effective_direct_engine_protocol(config)
    venv_root = _resolve_venv_root()
    vllm_cache_root = str(
        config.vllm_cache_root
        or os.getenv(
            "MOONCAKE_EPD_VLLM_CACHE_ROOT",
            str(REPO_ROOT.parent / ".cache" / "vllm"),
        )
    )
    mm_hidden_cache_env = (
        "export MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE=${MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE:-1}"
        if config.vllm_mm_hidden_cache is None
        else "export MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE="
        + ("1" if config.vllm_mm_hidden_cache else "0")
    )
    lines = [
        # Mooncake's Bootstrap client uses httpx.  Leaving ALL_PROXY in the
        # service environment makes local 127.0.0.1 registration attempt a
        # SOCKS connection (and fail before KV caches are registered), even if
        # HTTP(S)_PROXY were cleared.  Serving workers only communicate with
        # local control-plane endpoints generated below, so isolate every
        # proxy spelling rather than inheriting a developer shell's proxy.
        "unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY",
        "export NO_PROXY=127.0.0.1,localhost,::1",
        "export no_proxy=${NO_PROXY}",
        f"export PYTHONPATH={parent_path}:${{PYTHONPATH:-}}",
        # vLLM/Torch compile artifacts can exceed tens of GiB across model and
        # shape variants. Keep them under the repo's data-volume cache by
        # default, while preserving an explicit deployment override.
        f"export VLLM_CACHE_ROOT={shlex.quote(vllm_cache_root)}",
        "export TORCHINDUCTOR_CACHE_DIR=${VLLM_CACHE_ROOT}/torch_compile_cache",
        "export TRITON_CACHE_DIR=${VLLM_CACHE_ROOT}/triton",
        "mkdir -p \"${TORCHINDUCTOR_CACHE_DIR}\" \"${TRITON_CACHE_DIR}\"",
        # vLLM hook activation is explicit in the worker launcher.  This
        # prevents the workspace's sitecustomize module from mutating every
        # Python interpreter that merely has the repository on PYTHONPATH.
        "export MOONCAKE_EPD_ENABLE_VLLM_PATCHES=0",
        f"source {shlex.quote(str(venv_root / 'bin' / 'activate'))}",
        f"export MOONCAKE_CONFIG_PATH={mooncake_json}",
        f"export MOONCAKE_MASTER={config.master_server}",
        f"export MOONCAKE_TE_META_DATA_SERVER={config.metadata_server}",
        f"export MOONCAKE_PROTOCOL={protocol}",
        f"export MOONCAKE_EPD_DIRECT_ENGINE_PROTOCOL={direct_engine_protocol}",
        (
            "export MC_FORCE_TCP=1"
            if protocol == "tcp"
            else "unset MC_FORCE_TCP"
        ),
        (
            "export MC_INTRANODE_NVLINK=1"
            if protocol == "nvlink_intra"
            else "unset MC_INTRANODE_NVLINK"
        ),
        (
            "export MC_TCP_WRITE_COMPLETION_ACK=1"
            if config.tcp_write_completion_ack
            and "tcp" in {protocol, direct_engine_protocol}
            else "unset MC_TCP_WRITE_COMPLETION_ACK"
        ),
        (
            "export MC_TCP_ENABLE_CONNECTION_POOL="
            + ("1" if config.tcp_connection_pool else "0")
            if "tcp" in {protocol, direct_engine_protocol}
            else "unset MC_TCP_ENABLE_CONNECTION_POOL"
        ),
        f"export MOONCAKE_LOCAL_HOSTNAME={config.local_hostname}",
        f"export VLLM_HOST_IP={config.local_hostname}",
        f"export MOONCAKE_GLOBAL_SEGMENT_SIZE={config.global_segment_size}",
        f"export MOONCAKE_LOCAL_BUFFER_SIZE={config.local_buffer_size}",
        "export OPENAI_API_KEY=sk-local",
        mm_hidden_cache_env,
        "export MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_ENTRIES=${MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_ENTRIES:-64}",
        "export MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_BYTES=${MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_BYTES:-2147483648}",
        f"export MOONCAKE_EPD_REPO_ROOT={REPO_ROOT.parent}",
    ]
    if protocol == "rdma":
        # libibverbs must enable fork safety before vLLM initializes CUDA or
        # starts EngineCore workers.  Without this, CUDA memory registration
        # can fail with `fork compatibility: Invalid argument` on irdma.
        lines.extend(
            [
                "export RDMAV_FORK_SAFE=1",
                "export IBV_FORK_SAFE=1",
            ]
        )
    if config.strict_no_fallback:
        lines.extend(
            [
                "export MOONCAKE_EPD_STRICT=1",
                "export MOONCAKE_EPD_VLLM_FEATURE_HANDLE_STRICT=1",
                "export MOONCAKE_EPD_ALLOW_TRANSFER_FALLBACK=0",
            ]
        )
    if config.feature_handle_store_url:
        lines.extend(
            [
                f"export MOONCAKE_EPD_FEATURE_HANDLE_STORE_URL={config.feature_handle_store_url}",
                f"export MOONCAKE_STORE_URL={config.feature_handle_store_url}",
            ]
        )
    if config.feature_handle_store_id:
        lines.append(
            f"export MOONCAKE_EPD_FEATURE_HANDLE_STORE_ID={config.feature_handle_store_id}"
        )
    if config.feature_handle_store_config:
        lines.append(
            f"export MOONCAKE_EPD_FEATURE_HANDLE_STORE_CONFIG={config.feature_handle_store_config}"
        )
    if config.feature_handle_store_timeout_s > 0:
        lines.append(
            "export MOONCAKE_EPD_FEATURE_HANDLE_STORE_TIMEOUT_S="
            f"{float(config.feature_handle_store_timeout_s):.3f}"
        )
        lines.append(
            "export MOONCAKE_EPD_FEATURE_HANDLE_TIMEOUT_S="
            f"{float(config.feature_handle_store_timeout_s):.3f}"
        )
    if config.feature_handle_require_checksum:
        lines.append("export MOONCAKE_EPD_FEATURE_HANDLE_REQUIRE_CHECKSUM=1")
    if config.feature_handle_http_binary_payload:
        lines.append("export MOONCAKE_EPD_FEATURE_HANDLE_HTTP_BINARY=1")
    if bootstrap_port is not None:
        lines.append(f"export VLLM_MOONCAKE_BOOTSTRAP_PORT={bootstrap_port}")
    if config.connector_metrics_dir:
        lines.append(
            f"export MOONCAKE_EPD_CONNECTOR_METRICS_DIR={config.connector_metrics_dir}"
        )
    return "\n".join(lines)


def _json_flag(payload: Dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _vllm_scheduler_flags(config: VLLMDisaggConfig) -> str:
    flags: list[str] = []
    if config.max_num_batched_tokens is not None and int(config.max_num_batched_tokens) > 0:
        flags.append(f"--max-num-batched-tokens {int(config.max_num_batched_tokens)}")
    if config.max_num_seqs is not None and int(config.max_num_seqs) > 0:
        flags.append(f"--max-num-seqs {int(config.max_num_seqs)}")
    return (" ".join(flags) + " ") if flags else ""


def generate_configs(output_dir: str, config: Optional[VLLMDisaggConfig] = None) -> Dict[str, object]:
    config = config or VLLMDisaggConfig()
    out_dir = Path(output_dir)
    validate_mooncake_protocol_pair(
        config.protocol,
        _effective_direct_engine_protocol(config),
    )
    if str(config.scheduler_policy) not in SCHEDULER_POLICIES:
        allowed = ", ".join(sorted(SCHEDULER_POLICIES))
        raise ValueError(
            f"unsupported scheduler_policy={config.scheduler_policy!r}; expected one of {allowed}"
        )
    prefill_gpus = list(config.prefill_gpus or (config.prefill_gpu,))
    decode_gpus = list(config.decode_gpus or (config.decode_gpu,))
    if not prefill_gpus:
        raise ValueError("at least one prefill GPU is required")
    if not decode_gpus:
        raise ValueError("at least one decode GPU is required")
    prefill_ports = _expand_ints(config.prefill_port, config.prefill_ports, len(prefill_gpus))
    decode_ports = _expand_ints(config.decode_port, config.decode_ports, len(decode_gpus))
    prefill_bootstrap_ports = _expand_ints(
        config.prefill_bootstrap_port,
        config.prefill_bootstrap_ports,
        len(prefill_gpus),
    )
    decode_bootstrap_ports = _expand_ints(
        config.decode_bootstrap_port,
        config.decode_bootstrap_ports,
        len(decode_gpus),
    )
    prefill_ports = [_pick_free_port(port, config.local_hostname) for port in prefill_ports]
    decode_ports = [_pick_free_port(port, config.local_hostname) for port in decode_ports]
    used_bootstrap: set[int] = set()
    bootstrap_seed_prefix = str(out_dir.expanduser().resolve())
    for idx, port in enumerate(prefill_bootstrap_ports):
        selected = _pick_bootstrap_port(
            port,
            seed=f"{bootstrap_seed_prefix}:prefill:{idx}",
            reserved=used_bootstrap,
        )
        prefill_bootstrap_ports[idx] = selected
        used_bootstrap.add(selected)
    for idx, port in enumerate(decode_bootstrap_ports):
        selected = _pick_bootstrap_port(
            port,
            seed=f"{bootstrap_seed_prefix}:decode:{idx}",
            reserved=used_bootstrap,
        )
        decode_bootstrap_ports[idx] = selected
        used_bootstrap.add(selected)
    config.prefill_port = prefill_ports[0]
    config.decode_port = decode_ports[0]
    config.prefill_bootstrap_port = prefill_bootstrap_ports[0]
    config.decode_bootstrap_port = decode_bootstrap_ports[0]
    config.proxy_port = _pick_free_port(config.proxy_port, config.local_hostname)
    config.metadata_port = _pick_free_port(config.metadata_port, config.local_hostname)
    config.master_port = _pick_free_port(config.master_port, config.local_hostname)
    config.master_metrics_port = _pick_free_port(config.master_metrics_port, config.local_hostname)
    if config.enable_prefill_direct_feature_buffer_routes:
        if not str(config.direct_feature_buffer_auth_token or "").strip():
            config.direct_feature_buffer_auth_token = secrets.token_urlsafe(32)
        if not config.prefill_direct_buffer_service_urls and config.prefill_direct_buffer_service_url:
            config.prefill_direct_buffer_service_urls = (str(config.prefill_direct_buffer_service_url),)
        if not config.prefill_direct_buffer_service_urls:
            config.prefill_direct_buffer_service_urls = tuple(
                f"http://{config.local_hostname}:{port}" for port in prefill_ports
            )
        if len(config.prefill_direct_buffer_service_urls) == 1:
            config.prefill_direct_buffer_service_url = str(config.prefill_direct_buffer_service_urls[0])
        elif len(config.prefill_direct_buffer_service_urls) != len(prefill_ports):
            raise ValueError(
                "prefill_direct_buffer_service_urls must have one URL or match prefill worker count: "
                f"urls={len(config.prefill_direct_buffer_service_urls)} prefill={len(prefill_ports)}"
            )
    out_dir.mkdir(parents=True, exist_ok=True)
    worker_generation_dir = out_dir / "worker_generations"
    worker_generation_dir.mkdir(parents=True, exist_ok=True)

    def _worker_generation_env(worker_id: str) -> str:
        """Publish the current worker incarnation for Proxy-side fencing.

        A restarted generated worker receives a new default generation before
        vLLM starts.  The atomic rename prevents the Proxy from observing a
        partially written fence while it dispatches a P->D handoff.
        """

        return (
            f"export MOONCAKE_EPD_GENERATION_DIR={shlex.quote(str(worker_generation_dir))}\n"
            "mkdir -p \"$MOONCAKE_EPD_GENERATION_DIR\"\n"
            "export MOONCAKE_EPD_WORKER_GENERATION=\"${MOONCAKE_EPD_WORKER_GENERATION:-${MOONCAKE_EPD_ENGINE_ID}:$(date +%s%N)}\"\n"
            f"printf '%s\\n' \"$MOONCAKE_EPD_WORKER_GENERATION\" > \"$MOONCAKE_EPD_GENERATION_DIR/.{worker_id}.$$\"\n"
            f"mv \"$MOONCAKE_EPD_GENERATION_DIR/.{worker_id}.$$\" \"$MOONCAKE_EPD_GENERATION_DIR/{worker_id}\"\n"
        )

    files: Dict[str, object] = {}
    scheduler_flags = _vllm_scheduler_flags(config)
    generation_config = str(config.generation_config or "").strip()
    generation_config_flag = (
        f"--generation-config {shlex.quote(generation_config)} "
        if generation_config
        else ""
    )
    if config.enable_workflow_registry_wal:
        config.workflow_registry_wal_path = (
            config.workflow_registry_wal_path
            or str(out_dir / "proxy_workflow_registry.jsonl")
        )
    else:
        config.workflow_registry_wal_path = None
    config.connector_metrics_dir = (
        config.connector_metrics_dir
        or str(out_dir / "connector_metrics")
    )
    mooncake_path = out_dir / "mooncake.json"
    mooncake_path.write_text(json.dumps(config.to_mooncake_json(), indent=2), encoding="utf-8")
    files["mooncake_json"] = str(mooncake_path)

    metadata_script = out_dir / "start_metadata.sh"
    metadata_script.write_text(
        "#!/bin/bash\n"
        + _common_env_block(config, mooncake_path)
        + "\n"
        + f"python -m mooncake.http_metadata_server --host {config.local_hostname} --port {config.metadata_port}\n",
        encoding="utf-8",
    )
    metadata_script.chmod(0o755)
    files["metadata"] = str(metadata_script)

    master_script = out_dir / "start_master.sh"
    master_script.write_text(
        "#!/bin/bash\n"
        + _common_env_block(config, mooncake_path)
        + "\n"
        + f"mooncake_master --rpc_port={config.master_port} --metrics_port={config.master_metrics_port}\n",
        encoding="utf-8",
    )
    master_script.chmod(0o755)
    files["master"] = str(master_script)

    prefill_scripts: list[str] = []
    for idx, (gpu, port, bootstrap_port) in enumerate(
        zip(prefill_gpus, prefill_ports, prefill_bootstrap_ports)
    ):
        engine_id = f"epd-prefill-{idx}" if len(prefill_gpus) > 1 else "epd-prefill"
        prefill_kv_cfg = _json_flag(config.kv_transfer_config("kv_producer", engine_id))
        prefill_script = out_dir / ("start_prefill.sh" if idx == 0 else f"start_prefill_{idx}.sh")
        prefill_script.write_text(
            "#!/bin/bash\n"
            + _common_env_block(
                config,
                mooncake_path,
                bootstrap_port=bootstrap_port,
            )
            + "\n"
            + f"export MOONCAKE_EPD_ENGINE_ID={engine_id}\n"
            + f"export MOONCAKE_EPD_MODEL_ID={shlex.quote(str(config.model))}\n"
            + f"export MOONCAKE_EPD_VLLM_CAPABILITY_REPORT={out_dir / (engine_id + '-capabilities.json')}\n"
            + "export MOONCAKE_EPD_KV_ROLE=kv_producer\n"
            + "export MOONCAKE_EPD_VLLM_ROLE=prefill\n"
            + f"export MOONCAKE_EPD_WORKER_ID=prefill-{idx}\n"
            + _worker_generation_env(f"prefill-{idx}")
            + (
                "export MOONCAKE_EPD_ENABLE_DIRECT_FEATURE_BUFFER=1\n"
                f"export MOONCAKE_EPD_DIRECT_BUFFER_WORKER_ID=prefill-{idx}\n"
                f"export MOONCAKE_EPD_FEATURE_HANDLE_WORKER_ID=prefill-{idx}\n"
                "export MOONCAKE_EPD_DIRECT_BUFFER_DEVICE=cuda\n"
                "export MOONCAKE_EPD_DIRECT_READ_MODE=registered_tensor\n"
                f"export MOONCAKE_EPD_DIRECT_LOCAL_HOSTNAME={config.local_hostname}:{18000 + idx}\n"
                f"export MOONCAKE_EPD_DIRECT_TARGET_MODE={config.direct_feature_target_mode}\n"
                f"export MOONCAKE_EPD_DIRECT_REGISTER_MEMORY={1 if config.direct_feature_register_memory else 0}\n"
                f"export MOONCAKE_EPD_DIRECT_BUFFER_PERSISTENT_CACHE={1 if config.direct_feature_persistent_cache else 0}\n"
                f"export MOONCAKE_EPD_DIRECT_BUFFER_CACHE_MAX_ENTRIES={int(config.direct_feature_cache_max_entries)}\n"
                f"export MOONCAKE_EPD_DIRECT_BUFFER_CACHE_MAX_BYTES={int(config.direct_feature_cache_max_bytes)}\n"
                "export MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN="
                f"{shlex.quote(str(config.direct_feature_buffer_auth_token))}\n"
                f"export MOONCAKE_EPD_DIRECT_BUFFER_ROOT_ROUTES={1 if config.direct_feature_buffer_root_routes else 0}\n"
                if config.enable_prefill_direct_feature_buffer_routes
                else ""
            )
            + f"CUDA_VISIBLE_DEVICES={gpu} python -m mooncake_epd.integrations.vllm.launcher -- vllm serve {config.model} "
            + f"--port {port} "
            + f"--tensor-parallel-size {config.tensor_parallel_size} "
            + f"--max-model-len {config.max_model_len} "
            + f"--gpu-memory-utilization {config.gpu_memory_utilization} "
            + generation_config_flag
            + scheduler_flags
            + "--kv-transfer-config "
            + f"'{prefill_kv_cfg}'\n",
            encoding="utf-8",
        )
        prefill_script.chmod(0o755)
        prefill_scripts.append(str(prefill_script))
    files["prefill"] = prefill_scripts[0]
    files["prefill_scripts"] = prefill_scripts
    files["prefill_ports"] = prefill_ports
    files["prefill_gpus"] = prefill_gpus

    decode_scripts: list[str] = []
    for idx, (gpu, port, bootstrap_port) in enumerate(
        zip(decode_gpus, decode_ports, decode_bootstrap_ports)
    ):
        engine_id = f"epd-decode-{idx}" if len(decode_gpus) > 1 else "epd-decode"
        decode_kv_cfg = _json_flag(config.kv_transfer_config("kv_consumer", engine_id))
        decode_script = out_dir / ("start_decode.sh" if idx == 0 else f"start_decode_{idx}.sh")
        decode_script.write_text(
            "#!/bin/bash\n"
            + _common_env_block(
                config,
                mooncake_path,
                bootstrap_port=bootstrap_port,
            )
            + "\n"
            + f"export MOONCAKE_EPD_ENGINE_ID={engine_id}\n"
            + f"export MOONCAKE_EPD_MODEL_ID={shlex.quote(str(config.model))}\n"
            + f"export MOONCAKE_EPD_VLLM_CAPABILITY_REPORT={out_dir / (engine_id + '-capabilities.json')}\n"
            + "export MOONCAKE_EPD_KV_ROLE=kv_consumer\n"
            + "export MOONCAKE_EPD_VLLM_ROLE=decode\n"
            + f"export MOONCAKE_EPD_WORKER_ID=decode-{idx}\n"
            + _worker_generation_env(f"decode-{idx}")
            + f"CUDA_VISIBLE_DEVICES={gpu} python -m mooncake_epd.integrations.vllm.launcher -- vllm serve {config.model} "
            + f"--port {port} "
            + f"--tensor-parallel-size {config.tensor_parallel_size} "
            + f"--max-model-len {config.max_model_len} "
            + f"--gpu-memory-utilization {config.gpu_memory_utilization} "
            + generation_config_flag
            + scheduler_flags
            + "--kv-transfer-config "
            + f"'{decode_kv_cfg}'\n",
            encoding="utf-8",
        )
        decode_script.chmod(0o755)
        decode_scripts.append(str(decode_script))
    files["decode"] = decode_scripts[0]
    files["decode_scripts"] = decode_scripts
    files["decode_ports"] = decode_ports
    files["decode_gpus"] = decode_gpus

    proxy_script = out_dir / "start_proxy.sh"
    prefill_hosts_flag = " ".join([config.local_hostname for _ in prefill_ports])
    prefill_ports_flag = " ".join(str(port) for port in prefill_ports)
    decode_hosts_flag = " ".join([config.local_hostname for _ in decode_ports])
    decode_ports_flag = " ".join(str(port) for port in decode_ports)
    high_prefill_ids = " ".join(["prefill-0"]) if prefill_ports else ""
    standard_prefill_ids = " ".join(f"prefill-{idx}" for idx in range(1, len(prefill_ports)))
    low_decode_ids = " ".join(["decode-0"]) if decode_ports else ""
    standard_decode_ids = " ".join(f"decode-{idx}" for idx in range(1, len(decode_ports)))
    prefill_decode_affinity_flag = " ".join(
        shlex.quote(f"{prefill}={decode}")
        for prefill, decode in config.resolved_prefill_decode_affinity
    )
    proxy_script.write_text(
        "#!/bin/bash\n"
        + _common_env_block(config, mooncake_path)
        + "\n"
        + (
            "export MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN="
            f"{shlex.quote(str(config.direct_feature_buffer_auth_token))}\n"
            if config.enable_prefill_direct_feature_buffer_routes
            else ""
        )
        + f"python {REPO_ROOT / 'scripts' / 'vllm_disagg_proxy.py'} "
        + f"--prefiller-hosts {prefill_hosts_flag} --prefiller-ports {prefill_ports_flag} "
        + f"--decoder-hosts {decode_hosts_flag} --decoder-ports {decode_ports_flag} "
        + f"--layers-per-group {config.layers_per_group} "
        + f"--group-delay-ms {config.group_delay_ms} "
        + f"--max-group-bytes {config.max_group_bytes} "
        + f"--warn-rho {config.proxy_warn_rho} "
        + f"--critical-rho {config.proxy_critical_rho} "
        + f"--max-backpressure-delay-ms {config.proxy_max_backpressure_delay_ms} "
        + f"--transport-backend mooncake_engine_direct "
        + f"--mooncake-protocol {config.protocol} "
        + f"--mm-prefetch-mode {config.mm_prefetch_mode} "
        + ("--prefill-supports-feature-handles " if config.prefill_supports_feature_handles else "")
        + f"--owner-shards {config.owner_shards} "
        + (
            f"--kv-directory-rpc-url {config.kv_directory_rpc_url} "
            if config.kv_directory_rpc_url
            else ""
        )
        + f"--connector-metrics-dir {config.connector_metrics_dir} "
        + (
            f"--workflow-registry-wal {shlex.quote(str(config.workflow_registry_wal_path))} "
            if config.workflow_registry_wal_path
            else ""
        )
        + f"--worker-generation-dir {shlex.quote(str(worker_generation_dir))} "
        + (f"--high-prefill-worker-ids {high_prefill_ids} " if high_prefill_ids else "")
        + (f"--standard-prefill-worker-ids {standard_prefill_ids} " if standard_prefill_ids else "")
        + (f"--low-latency-decode-worker-ids {low_decode_ids} " if low_decode_ids else "")
        + (f"--standard-decode-worker-ids {standard_decode_ids} " if standard_decode_ids else "")
        + (
            f"--prefill-decode-affinity {prefill_decode_affinity_flag} "
            if prefill_decode_affinity_flag
            else ""
        )
        + (f"--encoder-service-url {config.encoder_service_url} " if config.encoder_service_url else "")
        + (
            f"--prefill-direct-buffer-service-urls {' '.join(str(u) for u in config.prefill_direct_buffer_service_urls)} "
            if len(config.prefill_direct_buffer_service_urls) > 1
            else (
                f"--prefill-direct-buffer-service-url {config.prefill_direct_buffer_service_url or config.prefill_direct_buffer_service_urls[0]} "
                if (config.prefill_direct_buffer_service_url or config.prefill_direct_buffer_service_urls)
                else ""
            )
        )
        + (
            "--release-direct-feature-buffers-after-prefill "
            if config.release_direct_feature_buffers_after_prefill
            else "--no-release-direct-feature-buffers-after-prefill "
        )
        + f"--direct-feature-release-batch-max-jobs {max(1, min(1024, int(config.direct_feature_release_batch_max_jobs)))} "
        + (
            "--enable-direct-feature-handle-cache "
            if config.enable_direct_feature_handle_cache
            else "--no-enable-direct-feature-handle-cache "
        )
        + f"--direct-feature-handle-cache-max-entries {int(config.direct_feature_handle_cache_max_entries)} "
        + f"--direct-feature-handle-cache-ttl-s {float(config.direct_feature_handle_cache_ttl_s)} "
        + f"--direct-feature-lease-prefetch {max(1, min(1024, int(config.direct_feature_lease_prefetch)))} "
        + f"--direct-feature-lease-prefetch-max-entries {max(0, int(config.direct_feature_lease_prefetch_max_entries))} "
        + f"--direct-feature-lease-prefetch-ttl-s {max(0.0, float(config.direct_feature_lease_prefetch_ttl_s))} "
        + (
            "--enable-direct-feature-adaptive-lease-prefetch "
            if config.direct_feature_adaptive_lease_prefetch
            else "--no-enable-direct-feature-adaptive-lease-prefetch "
        )
        + f"--direct-feature-adaptive-lease-prefetch-max {max(1, min(1024, int(config.direct_feature_adaptive_lease_prefetch_max)))} "
        + (
            "--enable-rendered-prefill-cache "
            if config.enable_rendered_prefill_cache
            else "--no-enable-rendered-prefill-cache "
        )
        + f"--rendered-prefill-cache-max-entries {int(config.rendered_prefill_cache_max_entries)} "
        + f"--rendered-prefill-cache-max-bytes {int(config.rendered_prefill_cache_max_bytes)} "
        + f"--rendered-prefill-cache-ttl-s {float(config.rendered_prefill_cache_ttl_s)} "
        + f"--prefill-dispatch-mode {config.prefill_dispatch_mode} "
        + f"--decode-dispatch-mode {config.decode_dispatch_mode} "
        + (
            "--allow-decode-token-fallback "
            if config.allow_decode_token_fallback
            else "--no-allow-decode-token-fallback "
        )
        + (
            "--prefill-http-keepalive "
            if config.prefill_http_keepalive
            else "--no-prefill-http-keepalive "
        )
        + (
            "--allow-unverified-openai-prompt-only "
            if config.allow_unverified_openai_prompt_only
            else "--no-allow-unverified-openai-prompt-only "
        )
        + "--enable-agent-state-clone "
        + ("--require-real-agent-state-store " if config.strict_no_fallback else "--no-require-real-agent-state-store ")
        + f"--scheduler-policy {shlex.quote(str(config.scheduler_policy))} "
        + ("--strict-no-fallback " if config.strict_no_fallback else "--no-strict-no-fallback ")
        + f"--port {config.proxy_port}\n",
        encoding="utf-8",
    )
    proxy_script.chmod(0o755)
    files["proxy"] = str(proxy_script)
    files["proxy_workflow_registry"] = config.workflow_registry_wal_path
    files["connector_metrics_dir"] = config.connector_metrics_dir
    files["worker_generation_dir"] = str(worker_generation_dir)

    test_req = out_dir / "test_request.json"
    test_req.write_text(
        json.dumps(
            {
                "model": config.model,
                "messages": [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "Please introduce Mooncake PD disaggregation briefly."}],
                    }
                ],
                "max_tokens": 64,
                "temperature": 0.0,
                "metadata": {"workflow_id": "demo-text-workflow"},
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    files["test_request"] = str(test_req)
    return files


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = VLLMDisaggConfig()
    checks = validate_environment(config)
    files = generate_configs(str(REPO_ROOT / "config"), config)

    print("Environment checks:")
    for k, v in checks.items():
        print(f"  {k}: {v}")

    print("\nGenerated files:")
    for k, v in files.items():
        print(f"  {k}: {v}")

    print("\nStartup order:")
    print(f"  1. bash {files['metadata']}")
    print(f"  2. bash {files['master']}")
    print(f"  3. bash {files['prefill']}")
    print(f"  4. bash {files['decode']}")
    print(f"  5. bash {files['proxy']}")
    print(
        "  6. python "
        f"{REPO_ROOT / 'scripts' / 'check_vllm_disagg.py'} "
        f"--prefill-url http://{config.local_hostname}:{config.prefill_port} "
        f"--decode-url http://{config.local_hostname}:{config.decode_port} "
        f"--proxy-url http://{config.local_hostname}:{config.proxy_port} "
        f"--request {files['test_request']}"
    )


if __name__ == "__main__":
    main()
