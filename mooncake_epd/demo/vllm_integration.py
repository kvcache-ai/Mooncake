"""Generate runnable vLLM + MooncakeConnector configs for this repo.

Targets the local real-model environment:
- model: ``MOONCAKE_EPD_MODEL_PATH`` or /data01/LWX/Qwen3-VL-8B-Instruct
- prefill GPU: 3
- decode GPU: 4

The generated commands opt into the repo-local external MooncakeConnector
module so layered transfer scheduling and serving control-plane metadata are
available on the real vLLM serving path.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import urlsplit

from mooncake_epd.core.control.vllm_incarnation import (
    VLLM_INCARNATION_ENDPOINT,
    VLLM_INCARNATION_MIDDLEWARE,
)
from mooncake_epd.core.transfer.rdma import (
    default_rdma_bind_address,
    detect_rdma_capabilities,
    resolve_rdma_protocol,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
VENV_ROOT = REPO_ROOT.parent / "venv_mooncake"
MODEL_PATH = os.getenv("MOONCAKE_EPD_MODEL_PATH", "/data01/LWX/Qwen3-VL-8B-Instruct")
CONNECTOR_MODULE_PATH = "mooncake_epd.core.control.vllm_mooncake_connector"


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


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


@dataclass
class VLLMDisaggConfig:
    model: str = MODEL_PATH
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
    protocol: str = "tcp"
    transport_backend: str = "auto"
    rdmacm_bind_address: str = ""
    rdmacm_remote_address: str = ""
    rdmacm_port_offset: int = 2711
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
    layers_per_group: int = 4
    group_delay_ms: float = 0.0
    max_group_bytes: int = 16 * 1024 * 1024
    max_transfer_descriptors: int = 64
    max_transfer_bytes: int = 16 * 1024 * 1024
    enable_descriptor_coalescing: bool = field(
        default_factory=lambda: _env_flag(
            "MOONCAKE_EPD_ENABLE_DESCRIPTOR_COALESCING",
            True,
        )
    )
    connector_metrics_flush_interval_s: float = 0.25
    connector_metrics_max_pending_records: int = 64
    allow_transfer_fallback: bool = False
    transfer_retry_attempts: int = 6
    transfer_retry_backoff_ms: float = 250.0
    proxy_warn_rho: float = 0.85
    proxy_critical_rho: float = 0.95
    proxy_max_backpressure_delay_ms: float = 150.0
    owner_shards: int = 1
    kv_directory_rpc_url: Optional[str] = None
    workflow_registry_wal_path: Optional[str] = None
    workflow_registry_wal_fsync_interval_s: float = 0.25
    workflow_registry_wal_max_pending_records: int = 64
    enable_decode_pipeline: bool = False
    decode_pipeline_max_inflight: int = 0
    enable_prerendered_decode: bool = False
    enable_decode_mm_hash_cache: bool = False
    decode_mm_hash_cache_max_entries: int = 64
    decode_mm_hash_cache_ttl_s: float = 120.0
    decode_mm_hash_epoch_poll_s: float = 1.0
    decode_mm_hash_epoch_probe_timeout_s: float = 0.5
    decode_mm_hash_epoch_freshness_s: float = 0.0
    decode_mm_hash_epoch_endpoint: str = "/metrics"
    enable_decode_mm_hash_epoch_guard: bool = False
    enable_decode_mm_hash_epoch_probe_singleflight: bool = True
    connector_metrics_dir: Optional[str] = None
    enable_mm_prefetch: bool = True
    mm_prefetch_mode: str = "asset_bytes"
    prefill_supports_feature_handles: bool = False
    encoder_service_url: Optional[str] = None
    prefill_direct_buffer_service_url: Optional[str] = None
    enable_prefill_direct_feature_buffer_routes: bool = False
    direct_feature_buffer_root_routes: bool = True
    release_direct_feature_buffers_after_prefill: bool = True
    enable_direct_feature_handle_cache: bool = False
    direct_feature_handle_cache_max_entries: int = 64
    direct_feature_handle_cache_max_bytes: int = 4 * 1024**3
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
    prefill_render_cache_max_bytes: int = 512 * 1024**2
    prefill_render_cache_ttl_s: float = 600.0
    enable_client_mm_uuid_references: bool = False
    upstream_max_connections: int = 32
    upstream_max_keepalive_connections: int = 16
    upstream_keepalive_expiry_s: float = 1.0
    strict_no_fallback: bool = False

    @property
    def metadata_server(self) -> str:
        return f"http://{self.local_hostname}:{self.metadata_port}/metadata"

    @property
    def master_server(self) -> str:
        return f"{self.local_hostname}:{self.master_port}"

    @property
    def data_protocol(self) -> str:
        caps = detect_rdma_capabilities()
        # An RNIC is not useful for two workers on the same host: the kernel
        # resolves the destination as a local route and rdma_cm cannot create a
        # hardware iWARP path. A remote RDMA address explicitly marks a
        # cross-host deployment.
        return resolve_rdma_protocol(
            self.protocol,
            caps,
            same_host=(
                self.protocol == "auto"
                and not bool(self.rdmacm_remote_address)
            ),
        )

    @property
    def mooncake_protocol(self) -> str:
        return "rdma" if self.data_protocol == "rdma" else "tcp"

    @property
    def selected_transport_backend(self) -> str:
        requested = str(self.transport_backend).strip().lower()
        if requested and requested != "auto":
            return requested
        if self.data_protocol == "rdmacm":
            return "rdmacm_staged"
        return "mooncake_engine_direct"

    @property
    def selected_rdmacm_bind_address(self) -> str:
        if self.rdmacm_bind_address:
            return self.rdmacm_bind_address
        return default_rdma_bind_address(detect_rdma_capabilities())

    def to_mooncake_json(self) -> Dict[str, object]:
        return {
            "local_hostname": self.local_hostname,
            "metadata_server": self.metadata_server,
            "global_segment_size": self.global_segment_size,
            "local_buffer_size": self.local_buffer_size,
            "protocol": self.mooncake_protocol,
            "device_name": "",
            "master_server_address": self.master_server,
        }

    def kv_transfer_config(self, role: str, engine_id: str) -> Dict[str, object]:
        extra_config: Dict[str, object] = {
            "mooncake_protocol": self.mooncake_protocol,
            "num_workers": 4,
            "layered_kv_transfer": True,
            "layers_per_group": self.layers_per_group,
            "group_delay_ms": self.group_delay_ms,
            "max_group_bytes": self.max_group_bytes,
            "max_transfer_descriptors": self.max_transfer_descriptors,
            "max_transfer_bytes": self.max_transfer_bytes,
            "enable_descriptor_coalescing": self.enable_descriptor_coalescing,
            "connector_metrics_flush_interval_s": (
                self.connector_metrics_flush_interval_s
            ),
            "connector_metrics_max_pending_records": (
                self.connector_metrics_max_pending_records
            ),
            "allow_transfer_fallback": self.allow_transfer_fallback,
            "transfer_retry_attempts": self.transfer_retry_attempts,
            "transfer_retry_backoff_ms": self.transfer_retry_backoff_ms,
            "transport_backend": self.selected_transport_backend,
        }
        if self.selected_transport_backend in {"rdmacm", "rdmacm_staged", "iwarp"}:
            extra_config.update(
                {
                    "rdmacm_bind_address": self.selected_rdmacm_bind_address,
                    "rdmacm_remote_address": self.rdmacm_remote_address,
                    "rdmacm_port_offset": self.rdmacm_port_offset,
                }
            )
        if self.connector_metrics_dir:
            extra_config["connector_metrics_dir"] = self.connector_metrics_dir
        return {
            "kv_connector": "MooncakeConnector",
            "kv_role": role,
            "engine_id": engine_id,
            "kv_connector_module_path": CONNECTOR_MODULE_PATH,
            "kv_connector_extra_config": extra_config,
        }


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
    checks = {
        "model_exists": Path(config.model).exists(),
        "venv_exists": VENV_ROOT.exists(),
        "vllm_bin": str(VENV_ROOT / "bin" / "vllm"),
        "mooncake_master_bin": str(VENV_ROOT / "bin" / "mooncake_master"),
        "python_bin": str(VENV_ROOT / "bin" / "python"),
        "proxy_script": str(REPO_ROOT / "scripts" / "vllm_disagg_proxy.py"),
        "connector_module": CONNECTOR_MODULE_PATH,
    }
    checks["vllm_bin_exists"] = Path(str(checks["vllm_bin"])).exists()
    checks["mooncake_master_exists"] = Path(
        str(checks["mooncake_master_bin"])
    ).exists()
    checks["python_bin_exists"] = Path(str(checks["python_bin"])).exists()
    checks["proxy_script_exists"] = Path(str(checks["proxy_script"])).exists()
    return checks


def _common_env_block(
    config: VLLMDisaggConfig,
    mooncake_json: Path,
    *,
    bootstrap_port: Optional[int] = None,
) -> str:
    parent_path = str(REPO_ROOT.parent)
    lines = [
        "unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY",
        "export NO_PROXY=127.0.0.1,localhost",
        f"export PYTHONPATH={parent_path}:${{PYTHONPATH:-}}",
        f"source {VENV_ROOT}/bin/activate",
        f"export MOONCAKE_CONFIG_PATH={mooncake_json}",
        f"export MOONCAKE_MASTER={config.master_server}",
        f"export MOONCAKE_TE_META_DATA_SERVER={config.metadata_server}",
        f"export MOONCAKE_PROTOCOL={config.mooncake_protocol}",
        f"export MOONCAKE_EPD_DATA_PROTOCOL={config.data_protocol}",
        f"export MOONCAKE_LOCAL_HOSTNAME={config.local_hostname}",
        f"export VLLM_HOST_IP={config.local_hostname}",
        f"export MOONCAKE_GLOBAL_SEGMENT_SIZE={config.global_segment_size}",
        f"export MOONCAKE_LOCAL_BUFFER_SIZE={config.local_buffer_size}",
        "export OPENAI_API_KEY=sk-local",
        "export MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE=${MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE:-1}",
        "export MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_ENTRIES=${MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_ENTRIES:-64}",
        "export MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_BYTES=${MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_BYTES:-2147483648}",
    ]
    if config.selected_transport_backend in {"rdmacm", "rdmacm_staged", "iwarp"}:
        lines.extend(
            [
                f"export MOONCAKE_EPD_RDMACM_BIND_ADDRESS={config.selected_rdmacm_bind_address}",
                f"export MOONCAKE_EPD_RDMACM_REMOTE_ADDRESS={config.rdmacm_remote_address}",
                f"export MOONCAKE_EPD_RDMACM_PORT_OFFSET={config.rdmacm_port_offset}",
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
    if bootstrap_port is not None:
        lines.append(f"export VLLM_MOONCAKE_BOOTSTRAP_PORT={bootstrap_port}")
    if config.connector_metrics_dir:
        lines.append(
            f"export MOONCAKE_EPD_CONNECTOR_METRICS_DIR={config.connector_metrics_dir}"
        )
    return "\n".join(lines)


def _json_flag(payload: Dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def generate_configs(output_dir: str, config: Optional[VLLMDisaggConfig] = None) -> Dict[str, object]:
    config = config or VLLMDisaggConfig()
    if config.enable_decode_mm_hash_cache and not config.enable_prerendered_decode:
        raise ValueError("Decode MM hash cache requires prerendered Decode")
    if config.decode_mm_hash_cache_max_entries < 1:
        raise ValueError("Decode MM hash cache max entries must be positive")
    if config.decode_mm_hash_cache_ttl_s < 0:
        raise ValueError("Decode MM hash cache TTL must be non-negative")
    if (
        config.enable_decode_mm_hash_epoch_guard
        and config.decode_mm_hash_epoch_endpoint.rstrip("/")
        != VLLM_INCARNATION_ENDPOINT
    ):
        raise ValueError(
            "Decode MM hash epoch guard requires the repo incarnation endpoint"
        )
    if config.prefill_incarnation_poll_s < 0:
        raise ValueError("Prefill incarnation poll interval must be non-negative")
    if not 0 <= config.prefill_incarnation_poll_jitter_ratio <= 1:
        raise ValueError(
            "Prefill incarnation poll jitter ratio must be between 0 and 1"
        )
    if config.prefill_incarnation_probe_timeout_s <= 0:
        raise ValueError("Prefill incarnation probe timeout must be positive")
    if config.prefill_incarnation_failure_threshold < 1:
        raise ValueError("Prefill incarnation failure threshold must be positive")
    if config.prefill_incarnation_freshness_s < 0:
        raise ValueError("Prefill incarnation freshness must be non-negative")
    prefill_incarnation_endpoint_parts = urlsplit(
        str(config.prefill_incarnation_endpoint)
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
        config.enable_prefill_incarnation_guard
        and config.prefill_incarnation_endpoint.rstrip("/")
        != VLLM_INCARNATION_ENDPOINT
    ):
        raise ValueError(
            "Prefill incarnation guard requires the repo incarnation endpoint"
        )
    if config.decode_pipeline_max_inflight < 0:
        raise ValueError("Decode pipeline max inflight must be non-negative")
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
    prefill_bootstrap_ports = [
        _pick_free_port(port, config.local_hostname) for port in prefill_bootstrap_ports
    ]
    decode_bootstrap_ports = [
        _pick_free_port(port, config.local_hostname) for port in decode_bootstrap_ports
    ]
    used_bootstrap = set()
    for idx, port in enumerate(prefill_bootstrap_ports):
        while port in used_bootstrap:
            port = _pick_free_port(0, config.local_hostname)
        prefill_bootstrap_ports[idx] = port
        used_bootstrap.add(port)
    for idx, port in enumerate(decode_bootstrap_ports):
        while port in used_bootstrap:
            port = _pick_free_port(0, config.local_hostname)
        decode_bootstrap_ports[idx] = port
        used_bootstrap.add(port)
    config.prefill_port = prefill_ports[0]
    config.decode_port = decode_ports[0]
    config.prefill_bootstrap_port = prefill_bootstrap_ports[0]
    config.decode_bootstrap_port = decode_bootstrap_ports[0]
    config.proxy_port = _pick_free_port(config.proxy_port, config.local_hostname)
    config.metadata_port = _pick_free_port(config.metadata_port, config.local_hostname)
    config.master_port = _pick_free_port(config.master_port, config.local_hostname)
    config.master_metrics_port = _pick_free_port(config.master_metrics_port, config.local_hostname)
    if (
        config.enable_prefill_direct_feature_buffer_routes
        and not config.prefill_direct_buffer_service_url
        and len(prefill_ports) == 1
    ):
        config.prefill_direct_buffer_service_url = f"http://{config.local_hostname}:{prefill_ports[0]}"
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files: Dict[str, object] = {}
    config.workflow_registry_wal_path = (
        config.workflow_registry_wal_path
        or str(out_dir / "proxy_workflow_registry.jsonl")
    )
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
            + "export MOONCAKE_EPD_KV_ROLE=kv_producer\n"
            + (
                "export MOONCAKE_EPD_ENABLE_DIRECT_FEATURE_BUFFER=1\n"
                f"export MOONCAKE_EPD_DIRECT_BUFFER_WORKER_ID=prefill-{idx}\n"
                f"export MOONCAKE_EPD_FEATURE_HANDLE_WORKER_ID=prefill-{idx}\n"
                "export MOONCAKE_EPD_DIRECT_BUFFER_DEVICE=cuda\n"
                f"export MOONCAKE_EPD_DIRECT_LOCAL_HOSTNAME={config.local_hostname}:{18000 + idx}\n"
                "export MOONCAKE_EPD_DIRECT_TARGET_MODE=managed_buffer\n"
                "export MOONCAKE_EPD_DIRECT_REGISTER_MEMORY=0\n"
                f"export MOONCAKE_EPD_DIRECT_BUFFER_ROOT_ROUTES={1 if config.direct_feature_buffer_root_routes else 0}\n"
                if config.enable_prefill_direct_feature_buffer_routes
                else ""
            )
            + f"CUDA_VISIBLE_DEVICES={gpu} vllm serve {config.model} "
            + f"--port {port} "
            + f"--tensor-parallel-size {config.tensor_parallel_size} "
            + f"--max-model-len {config.max_model_len} "
            + f"--gpu-memory-utilization {config.gpu_memory_utilization} "
            + (
                f"--middleware {VLLM_INCARNATION_MIDDLEWARE} "
                if config.prefill_incarnation_endpoint.rstrip("/")
                == VLLM_INCARNATION_ENDPOINT
                and (
                    config.enable_prefill_incarnation_guard
                    or config.prefill_incarnation_poll_s > 0
                )
                else ""
            )
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
            + "export MOONCAKE_EPD_KV_ROLE=kv_consumer\n"
            + f"CUDA_VISIBLE_DEVICES={gpu} vllm serve {config.model} "
            + f"--port {port} "
            + f"--tensor-parallel-size {config.tensor_parallel_size} "
            + f"--max-model-len {config.max_model_len} "
            + f"--gpu-memory-utilization {config.gpu_memory_utilization} "
            + (
                f"--middleware {VLLM_INCARNATION_MIDDLEWARE} "
                if config.decode_mm_hash_epoch_endpoint.rstrip("/")
                == VLLM_INCARNATION_ENDPOINT
                else ""
            )
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
    proxy_script.write_text(
        "#!/bin/bash\n"
        + _common_env_block(config, mooncake_path)
        + "\n"
        + f"python {REPO_ROOT / 'scripts' / 'vllm_disagg_proxy.py'} "
        + f"--prefiller-hosts {prefill_hosts_flag} --prefiller-ports {prefill_ports_flag} "
        + f"--decoder-hosts {decode_hosts_flag} --decoder-ports {decode_ports_flag} "
        + f"--layers-per-group {config.layers_per_group} "
        + f"--group-delay-ms {config.group_delay_ms} "
        + f"--max-group-bytes {config.max_group_bytes} "
        + f"--warn-rho {config.proxy_warn_rho} "
        + f"--critical-rho {config.proxy_critical_rho} "
        + f"--max-backpressure-delay-ms {config.proxy_max_backpressure_delay_ms} "
        + f"--transport-backend {config.selected_transport_backend} "
        + (
            "--enable-mm-prefetch "
            if config.enable_mm_prefetch
            else "--no-enable-mm-prefetch "
        )
        + f"--mm-prefetch-mode {config.mm_prefetch_mode} "
        + ("--prefill-supports-feature-handles " if config.prefill_supports_feature_handles else "")
        + f"--owner-shards {config.owner_shards} "
        + (
            f"--kv-directory-rpc-url {config.kv_directory_rpc_url} "
            if config.kv_directory_rpc_url
            else ""
        )
        + f"--connector-metrics-dir {config.connector_metrics_dir} "
        + f"--workflow-registry-wal {config.workflow_registry_wal_path} "
        + (
            "--workflow-registry-wal-fsync-interval-s "
            f"{config.workflow_registry_wal_fsync_interval_s} "
        )
        + (
            "--workflow-registry-wal-max-pending "
            f"{config.workflow_registry_wal_max_pending_records} "
        )
        + (
            "--enable-decode-pipeline "
            if config.enable_decode_pipeline
            else "--no-enable-decode-pipeline "
        )
        + f"--decode-pipeline-max-inflight {config.decode_pipeline_max_inflight} "
        + (
            "--enable-prerendered-decode "
            if config.enable_prerendered_decode
            else "--no-enable-prerendered-decode "
        )
        + f"--prerendered-decode-model {config.model} "
        + (
            "--enable-decode-mm-hash-cache "
            if config.enable_decode_mm_hash_cache
            else "--no-enable-decode-mm-hash-cache "
        )
        + (
            "--decode-mm-hash-cache-max-entries "
            f"{config.decode_mm_hash_cache_max_entries} "
        )
        + (
            "--decode-mm-hash-cache-ttl-s "
            f"{config.decode_mm_hash_cache_ttl_s} "
        )
        + f"--decode-mm-hash-epoch-poll-s {config.decode_mm_hash_epoch_poll_s} "
        + (
            "--decode-mm-hash-epoch-probe-timeout-s "
            f"{config.decode_mm_hash_epoch_probe_timeout_s} "
        )
        + (
            "--decode-mm-hash-epoch-freshness-s "
            f"{config.decode_mm_hash_epoch_freshness_s} "
        )
        + (
            "--decode-mm-hash-epoch-endpoint "
            f"{config.decode_mm_hash_epoch_endpoint} "
        )
        + (
            "--decode-mm-hash-epoch-guard "
            if config.enable_decode_mm_hash_epoch_guard
            else "--no-decode-mm-hash-epoch-guard "
        )
        + (
            "--decode-mm-hash-epoch-probe-singleflight "
            if config.enable_decode_mm_hash_epoch_probe_singleflight
            else "--no-decode-mm-hash-epoch-probe-singleflight "
        )
        + (f"--high-prefill-worker-ids {high_prefill_ids} " if high_prefill_ids else "")
        + (f"--standard-prefill-worker-ids {standard_prefill_ids} " if standard_prefill_ids else "")
        + (f"--low-latency-decode-worker-ids {low_decode_ids} " if low_decode_ids else "")
        + (f"--standard-decode-worker-ids {standard_decode_ids} " if standard_decode_ids else "")
        + (f"--encoder-service-url {config.encoder_service_url} " if config.encoder_service_url else "")
        + (
            f"--prefill-direct-buffer-service-url {config.prefill_direct_buffer_service_url} "
            if config.prefill_direct_buffer_service_url
            else ""
        )
        + (
            "--enable-direct-feature-handle-cache "
            if config.enable_direct_feature_handle_cache
            else "--no-enable-direct-feature-handle-cache "
        )
        + (
            "--direct-feature-handle-cache-max-entries "
            f"{config.direct_feature_handle_cache_max_entries} "
        )
        + (
            "--direct-feature-handle-cache-max-bytes "
            f"{config.direct_feature_handle_cache_max_bytes} "
        )
        + (
            "--direct-feature-handle-cache-ttl-s "
            f"{config.direct_feature_handle_cache_ttl_s} "
        )
        + f"--prefill-incarnation-poll-s {config.prefill_incarnation_poll_s} "
        + (
            "--prefill-incarnation-poll-jitter-ratio "
            f"{config.prefill_incarnation_poll_jitter_ratio} "
        )
        + (
            "--prefill-incarnation-probe-timeout-s "
            f"{config.prefill_incarnation_probe_timeout_s} "
        )
        + (
            "--prefill-incarnation-failure-threshold "
            f"{config.prefill_incarnation_failure_threshold} "
        )
        + (
            "--prefill-incarnation-freshness-s "
            f"{config.prefill_incarnation_freshness_s} "
        )
        + (
            "--prefill-incarnation-endpoint "
            f"{shlex.quote(str(config.prefill_incarnation_endpoint))} "
        )
        + (
            "--prefill-incarnation-guard "
            if config.enable_prefill_incarnation_guard
            else "--no-prefill-incarnation-guard "
        )
        + (
            "--prefill-incarnation-probe-singleflight "
            if config.enable_prefill_incarnation_probe_singleflight
            else "--no-prefill-incarnation-probe-singleflight "
        )
        + (
            "--enable-prefill-render-cache "
            if config.enable_prefill_render_cache
            else "--no-enable-prefill-render-cache "
        )
        + f"--prefill-render-cache-max-entries {config.prefill_render_cache_max_entries} "
        + f"--prefill-render-cache-max-bytes {config.prefill_render_cache_max_bytes} "
        + f"--prefill-render-cache-ttl-s {config.prefill_render_cache_ttl_s} "
        + (
            "--enable-client-mm-uuid-references "
            if config.enable_client_mm_uuid_references
            else "--no-enable-client-mm-uuid-references "
        )
        + (
            "--release-direct-feature-buffers-after-prefill "
            if config.release_direct_feature_buffers_after_prefill
            else "--no-release-direct-feature-buffers-after-prefill "
        )
        + f"--upstream-max-connections {config.upstream_max_connections} "
        + (
            "--upstream-max-keepalive-connections "
            f"{config.upstream_max_keepalive_connections} "
        )
        + f"--upstream-keepalive-expiry-s {config.upstream_keepalive_expiry_s} "
        + "--enable-agent-state-clone "
        + ("--strict-no-fallback " if config.strict_no_fallback else "--no-strict-no-fallback ")
        + f"--port {config.proxy_port}\n",
        encoding="utf-8",
    )
    proxy_script.chmod(0o755)
    files["proxy"] = str(proxy_script)
    files["proxy_workflow_registry"] = config.workflow_registry_wal_path
    files["connector_metrics_dir"] = config.connector_metrics_dir

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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "config"))
    parser.add_argument(
        "--protocol",
        default="tcp",
        choices=["local", "tcp", "rdma", "rdmacm", "auto"],
    )
    parser.add_argument(
        "--transport-backend",
        default="auto",
        choices=["auto", "mooncake_engine_direct", "rdmacm_staged"],
    )
    parser.add_argument("--rdmacm-bind-address", default="")
    parser.add_argument("--rdmacm-remote-address", default="")
    parser.add_argument("--rdmacm-port-offset", type=int, default=2711)
    parser.add_argument("--prefill-gpu", type=int, default=3)
    parser.add_argument("--decode-gpu", type=int, default=4)
    parser.add_argument(
        "--decode-pipeline",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument("--decode-pipeline-max-inflight", type=int, default=0)
    parser.add_argument(
        "--prerendered-decode",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--decode-mm-hash-cache",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument("--decode-mm-hash-cache-max-entries", type=int, default=64)
    parser.add_argument("--decode-mm-hash-cache-ttl-s", type=float, default=120.0)
    parser.add_argument("--decode-mm-hash-epoch-poll-s", type=float, default=1.0)
    parser.add_argument(
        "--decode-mm-hash-epoch-probe-timeout-s",
        type=float,
        default=0.5,
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-freshness-s",
        type=float,
        default=0.0,
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-endpoint",
        default="/metrics",
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-guard",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--decode-mm-hash-epoch-probe-singleflight",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--prefill-incarnation-poll-s", type=float, default=0.0)
    parser.add_argument(
        "--prefill-incarnation-poll-jitter-ratio",
        type=float,
        default=0.0,
    )
    parser.add_argument(
        "--prefill-incarnation-probe-timeout-s",
        type=float,
        default=0.5,
    )
    parser.add_argument(
        "--prefill-incarnation-failure-threshold",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--prefill-incarnation-freshness-s",
        type=float,
        default=0.0,
    )
    parser.add_argument(
        "--prefill-incarnation-endpoint",
        default=VLLM_INCARNATION_ENDPOINT,
    )
    parser.add_argument(
        "--prefill-incarnation-guard",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--prefill-incarnation-probe-singleflight",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--enable-mm-prefetch",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--client-mm-uuid-references",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--strict-no-fallback",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    args = parser.parse_args()
    if args.decode_mm_hash_cache and not args.prerendered_decode:
        parser.error("--decode-mm-hash-cache requires --prerendered-decode")
    if args.decode_pipeline_max_inflight < 0:
        parser.error("--decode-pipeline-max-inflight must be >= 0")
    if args.decode_mm_hash_cache_max_entries < 1:
        parser.error("--decode-mm-hash-cache-max-entries must be >= 1")
    if args.decode_mm_hash_cache_ttl_s < 0:
        parser.error("--decode-mm-hash-cache-ttl-s must be >= 0")
    if args.decode_mm_hash_epoch_poll_s < 0:
        parser.error("--decode-mm-hash-epoch-poll-s must be >= 0")
    if args.decode_mm_hash_epoch_probe_timeout_s <= 0:
        parser.error("--decode-mm-hash-epoch-probe-timeout-s must be > 0")
    if args.decode_mm_hash_epoch_freshness_s < 0:
        parser.error("--decode-mm-hash-epoch-freshness-s must be >= 0")
    if not str(args.decode_mm_hash_epoch_endpoint).startswith("/"):
        parser.error("--decode-mm-hash-epoch-endpoint must be an absolute path")
    if (
        args.decode_mm_hash_epoch_guard
        and str(args.decode_mm_hash_epoch_endpoint).rstrip("/")
        != VLLM_INCARNATION_ENDPOINT
    ):
        parser.error(
            "--decode-mm-hash-epoch-guard requires the repo incarnation endpoint"
        )
    if args.prefill_incarnation_poll_s < 0:
        parser.error("--prefill-incarnation-poll-s must be >= 0")
    if not 0 <= args.prefill_incarnation_poll_jitter_ratio <= 1:
        parser.error(
            "--prefill-incarnation-poll-jitter-ratio must be between 0 and 1"
        )
    if args.prefill_incarnation_probe_timeout_s <= 0:
        parser.error("--prefill-incarnation-probe-timeout-s must be > 0")
    if args.prefill_incarnation_failure_threshold < 1:
        parser.error("--prefill-incarnation-failure-threshold must be >= 1")
    if args.prefill_incarnation_freshness_s < 0:
        parser.error("--prefill-incarnation-freshness-s must be >= 0")
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
        parser.error(
            "--prefill-incarnation-endpoint must be an absolute URL path "
            "without scheme, host, query, or fragment"
        )
    if (
        args.prefill_incarnation_guard
        and str(args.prefill_incarnation_endpoint).rstrip("/")
        != VLLM_INCARNATION_ENDPOINT
    ):
        parser.error(
            "--prefill-incarnation-guard requires the repo incarnation endpoint"
        )
    config = VLLMDisaggConfig(
        protocol=args.protocol,
        transport_backend=args.transport_backend,
        rdmacm_bind_address=args.rdmacm_bind_address,
        rdmacm_remote_address=args.rdmacm_remote_address,
        rdmacm_port_offset=args.rdmacm_port_offset,
        prefill_gpu=args.prefill_gpu,
        decode_gpu=args.decode_gpu,
        enable_decode_pipeline=bool(args.decode_pipeline),
        decode_pipeline_max_inflight=int(args.decode_pipeline_max_inflight),
        enable_prerendered_decode=bool(args.prerendered_decode),
        enable_decode_mm_hash_cache=bool(args.decode_mm_hash_cache),
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
        enable_mm_prefetch=bool(args.enable_mm_prefetch),
        enable_client_mm_uuid_references=bool(
            args.client_mm_uuid_references
        ),
        strict_no_fallback=bool(args.strict_no_fallback),
    )
    checks = validate_environment(config)
    files = generate_configs(args.output_dir, config)

    print("Environment checks:")
    for k, v in checks.items():
        print(f"  {k}: {v}")

    print("\nGenerated files:")
    for k, v in files.items():
        print(f"  {k}: {v}")
    print(
        "\nTransport resolution: "
        f"requested={config.protocol} data={config.data_protocol} "
        f"mooncake={config.mooncake_protocol} "
        f"backend={config.selected_transport_backend}"
    )

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
