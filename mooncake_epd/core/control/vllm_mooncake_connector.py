"""Repo-local MooncakeConnector wrapper for vLLM serving.

This module keeps vLLM upstream Mooncake's real data plane but extends it in
three production-facing directions:

1. restore router-facing decode handoff params on producer completion;
2. drive *real* layer/group-aware KV transfer from vLLM's
   ``wait_for_layer_load`` / ``save_kv_layer`` hooks;
3. expose grouped-transfer worker metadata for observability.

Unlike the earlier repo version, the layered path below does not wait until
prefill fully finishes before pushing all descriptors. The producer now:

- learns request block ids during ``update_state_after_alloc``;
- marks layer groups ready from ``save_kv_layer`` as the forward progresses;
- transfers only the regions that correspond to the finished group; and
- notifies the consumer after each group so ``wait_for_layer_load`` can unblock
  the matching attention layers.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable

import msgspec

from vllm.config import VllmConfig
from vllm.distributed.kv_transfer.kv_connector.v1.base import (
    KVConnectorBase_V1,
    KVConnectorMetadata,
    KVConnectorRole,
    SupportsHMA,
)
from vllm.distributed.kv_transfer.kv_connector.v1.metrics import KVConnectorStats
from vllm.distributed.kv_transfer.kv_connector.v1.mooncake.mooncake_connector import (
    MooncakeConnectorMetadata,
    MooncakeConnectorScheduler as UpstreamMooncakeConnectorScheduler,
    MooncakeConnectorWorker as UpstreamMooncakeConnectorWorker,
    MooncakeXferMetadata,
    MooncakeXferResponse,
    MooncakeXferResponseStatus,
    PullReqMeta,
    SendBlockMeta,
    get_mooncake_bootstrap_addr,
)
from vllm.distributed.kv_transfer.kv_connector.v1.mooncake.stats import (
    MooncakeKVConnectorStats,
)
from vllm.forward_context import ForwardContext
from vllm.logger import init_logger
from vllm.v1.attention.backend import AttentionMetadata
from vllm.v1.request import RequestStatus

from .vllm_transfer_primitives import (
    LayeredTransferWorkerMeta,
    chunk_transfer_descriptors,
    coalesce_transfer_descriptors,
    is_retryable_transfer_failure,
)
from .connector_metrics import ConnectorMetricsSink
from .vllm_incarnation import VLLM_PROCESS_INCARNATION
from ..transfer import TransferEngine as PeerTransferEngine

if TYPE_CHECKING:
    from vllm.v1.kv_cache_interface import KVCacheConfig

logger = init_logger(__name__)


@dataclass
class _LayerRegionSlice:
    start: int
    stop: int


@dataclass
class _TransferDispatchResult:
    ret_code: int
    backend_label: str
    used_fallback: bool = False
    error_message: str | None = None


@dataclass
class _LayeredSendState:
    transfer_id: str
    total_groups: int
    group_ready_events: list[threading.Event]
    announced_groups: set[int] = field(default_factory=set)
    failed: str | None = None

    @classmethod
    def create(cls, transfer_id: str, total_groups: int) -> "_LayeredSendState":
        return cls(
            transfer_id=transfer_id,
            total_groups=max(1, int(total_groups)),
            group_ready_events=[threading.Event() for _ in range(max(1, int(total_groups)))],
        )

    def mark_group_ready(self, group_idx: int) -> None:
        if 0 <= group_idx < len(self.group_ready_events):
            self.group_ready_events[group_idx].set()
            self.announced_groups.add(group_idx)

    def fail(self, message: str) -> None:
        self.failed = str(message)
        for event in self.group_ready_events:
            event.set()


@dataclass
class _LayeredReceiveState:
    request_id: str
    transfer_id: str
    total_groups: int
    expected_tasks: int
    group_events: list[threading.Event]
    remaining_by_group: list[int]
    failure: str | None = None

    @classmethod
    def create(
        cls,
        request_id: str,
        transfer_id: str,
        total_groups: int,
        expected_tasks: int,
    ) -> "_LayeredReceiveState":
        total_groups = max(1, int(total_groups))
        expected_tasks = max(1, int(expected_tasks))
        return cls(
            request_id=request_id,
            transfer_id=transfer_id,
            total_groups=total_groups,
            expected_tasks=expected_tasks,
            group_events=[threading.Event() for _ in range(total_groups)],
            remaining_by_group=[expected_tasks for _ in range(total_groups)],
        )

    def reset_expected_tasks(self, expected_tasks: int) -> None:
        expected_tasks = max(1, int(expected_tasks))
        if expected_tasks == self.expected_tasks:
            return
        for idx, event in enumerate(self.group_events):
            if event.is_set():
                continue
            self.remaining_by_group[idx] = expected_tasks
        self.expected_tasks = expected_tasks

    def ensure_total_groups(self, total_groups: int) -> None:
        total_groups = max(1, int(total_groups))
        if total_groups == self.total_groups:
            return
        new_events = [threading.Event() for _ in range(total_groups)]
        new_remaining = [self.expected_tasks for _ in range(total_groups)]
        shared = min(self.total_groups, total_groups)
        for idx in range(shared):
            if self.group_events[idx].is_set():
                new_events[idx].set()
            new_remaining[idx] = self.remaining_by_group[idx]
        if self.failure is not None:
            for event in new_events:
                event.set()
        self.total_groups = total_groups
        self.group_events = new_events
        self.remaining_by_group = new_remaining

    def ack_group(self, group_idx: int) -> None:
        if self.failure is not None:
            return
        if group_idx < 0 or group_idx >= self.total_groups:
            return
        self.remaining_by_group[group_idx] = max(0, self.remaining_by_group[group_idx] - 1)
        if self.remaining_by_group[group_idx] == 0:
            self.group_events[group_idx].set()

    def fail(self, message: str) -> None:
        self.failure = str(message)
        for event in self.group_events:
            event.set()

    def wait_group(self, group_idx: int, timeout_s: float) -> None:
        if group_idx < 0 or group_idx >= self.total_groups:
            return
        event = self.group_events[group_idx]
        ok = event.wait(timeout_s)
        if not ok:
            raise TimeoutError(
                f"timed out waiting for layered KV group={group_idx} "
                f"request={self.request_id} transfer_id={self.transfer_id}"
            )
        if self.failure is not None:
            raise RuntimeError(self.failure)

    def complete(self) -> bool:
        return all(event.is_set() for event in self.group_events)


class LayeredMooncakeXferMetadata(
    msgspec.Struct,
    omit_defaults=True,  # type: ignore[call-arg]
):
    remote_hostname: str
    remote_port: int
    remote_tp_size: int
    remote_tp_rank: int
    req_blocks: dict[str, tuple[str, list[list[int]]]]
    kv_caches_base_addr: list[int]
    block_lens: list[int]
    layered: bool = False
    total_groups: int = 0


class LayeredMooncakeXferResponse(
    msgspec.Struct,
    omit_defaults=True,  # type: ignore[call-arg]
):
    status: MooncakeXferResponseStatus
    ok_reqs: list[str] | None = None
    err_reqs: list[str] | None = None
    err_msg: str | None = None
    group_index: int = -1
    total_groups: int = 0


class EPDMooncakeConnectorScheduler(UpstreamMooncakeConnectorScheduler):
    """Scheduler shim that restores decode handoff params and early block ids."""

    def __init__(
        self,
        vllm_config: VllmConfig,
        engine_id: str,
        kv_cache_config: "KVCacheConfig",
    ):
        super().__init__(vllm_config, engine_id, kv_cache_config)
        self.engine_id = engine_id
        bootstrap_host, bootstrap_port = get_mooncake_bootstrap_addr(vllm_config)
        self.remote_bootstrap_addr = f"http://{bootstrap_host}:{bootstrap_port}"
        parallel_config = getattr(vllm_config, "parallel_config", None)
        self.tp_size = int(getattr(parallel_config, "tensor_parallel_size", 1) or 1)
        extra = dict(vllm_config.kv_transfer_config.kv_connector_extra_config or {})
        self.layered_kv_transfer = bool(extra.get("layered_kv_transfer", False))

    @staticmethod
    def _int_param(params: dict[str, Any], key: str) -> int | None:
        value = params.get(key)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_block_count_list(value: Any) -> list[int] | None:
        if not isinstance(value, (list, tuple)):
            return None
        counts: list[int] = []
        for item in value:
            try:
                counts.append(max(0, int(item)))
            except (TypeError, ValueError):
                return None
        return counts

    def _remote_prefill_prompt_tokens(
        self,
        params: dict[str, Any],
        token_ids: list[int],
    ) -> int:
        override = self._int_param(params, "remote_prefill_prompt_tokens")
        if override is None:
            return len(token_ids)
        return max(0, min(len(token_ids), override))

    def _clip_remote_prefill_recv_blocks(
        self,
        params: dict[str, Any],
        block_ids: list[list[int]],
    ) -> list[list[int]]:
        if not block_ids:
            return block_ids
        counts = self._normalize_block_count_list(params.get("remote_prefill_block_counts"))
        if counts is None or len(counts) != len(block_ids):
            return block_ids
        clipped: list[list[int]] = []
        changed = False
        for group, count in zip(block_ids, counts):
            group_list = list(group)
            if len(group_list) > count:
                clipped.append(group_list[:count])
                changed = True
            else:
                clipped.append(group_list)
        return clipped if changed else block_ids

    def get_num_new_matched_tokens(
        self,
        request,
        num_computed_tokens: int,
    ) -> tuple[int, bool]:
        params = dict(getattr(request, "kv_transfer_params", None) or {})
        if params.get("do_remote_prefill"):
            token_ids = list(getattr(request, "prompt_token_ids", None) or [])
            count = self._remote_prefill_prompt_tokens(params, token_ids) - int(num_computed_tokens)
            if count > 0:
                return count, True
            return 0, False
        return super().get_num_new_matched_tokens(request, num_computed_tokens)

    def update_state_after_alloc(self, request, blocks, num_external_tokens: int):
        params = dict(getattr(request, "kv_transfer_params", None) or {})
        result = super().update_state_after_alloc(request, blocks, num_external_tokens)
        if params.get("do_remote_prefill") and request.request_id in self._reqs_need_recv:
            recv_request, local_block_ids = self._reqs_need_recv[request.request_id]
            clipped_block_ids = self._clip_remote_prefill_recv_blocks(params, local_block_ids)
            self._reqs_need_recv[request.request_id] = (recv_request, clipped_block_ids)
        if (
            self.layered_kv_transfer
            and params.get("do_remote_decode")
            and not self.is_kv_consumer
        ):
            # The Decode engine owns a distinct KV cache and can allocate the
            # full prompt even when Prefill reuses hashed prefix-cache blocks.
            # Sending only Prefill's unhashed suffix makes the producer expose
            # fewer blocks than Decode requested (for example 1 vs 64), which
            # is fatal inside vLLM's connector path.  All allocated Prefill
            # blocks already contain valid KV, so transfer the complete set.
            if hasattr(blocks, "blocks"):
                allocated_block_ids = tuple(
                    [
                        block.block_id
                        for block in group
                        if not bool(getattr(block, "is_null", False))
                    ]
                    for group in blocks.blocks
                )
            elif hasattr(blocks, "get_block_ids"):
                allocated_block_ids = blocks.get_block_ids()
            elif hasattr(blocks, "get_unhashed_block_ids_all_groups"):
                allocated_block_ids = blocks.get_unhashed_block_ids_all_groups()
            else:
                allocated_block_ids = ()
            local_block_ids = self.get_sw_clipped_blocks(
                allocated_block_ids or ()
            )
            self._reqs_need_send[request.request_id] = (request, local_block_ids)
        return result

    def request_finished(
        self,
        request,
        block_ids: tuple[list[int], ...],
    ) -> tuple[bool, dict[str, Any] | None]:
        params = dict(getattr(request, "kv_transfer_params", None) or {})
        delay_free_blocks, upstream_params = super().request_finished(request, block_ids)
        if upstream_params is not None:
            upstream_params = dict(upstream_params)
            if upstream_params.get("do_remote_prefill"):
                upstream_params.setdefault(
                    "remote_engine_incarnation", VLLM_PROCESS_INCARNATION
                )
            return delay_free_blocks, upstream_params
        if not params or not params.get("transfer_id"):
            return delay_free_blocks, None
        if params.get("do_remote_prefill"):
            return delay_free_blocks, None
        if not params.get("do_remote_decode"):
            return delay_free_blocks, None
        if getattr(request, "status", None) != RequestStatus.FINISHED_LENGTH_CAPPED:
            return delay_free_blocks, None
        return delay_free_blocks, {
            "do_remote_prefill": True,
            "do_remote_decode": False,
            "remote_block_ids": self.get_sw_clipped_blocks(block_ids),
            "remote_engine_id": self.engine_id,
            "remote_engine_incarnation": VLLM_PROCESS_INCARNATION,
            "remote_bootstrap_addr": self.remote_bootstrap_addr,
            "tp_size": self.tp_size,
            "transfer_id": params["transfer_id"],
        }

    def build_connector_meta(self, scheduler_output) -> KVConnectorMetadata:
        del scheduler_output
        meta = MooncakeConnectorMetadata()
        request_routing_paths: dict[str, str] = {}
        transfer_routing_paths: dict[str, str] = {}
        remote_engine_incarnations: dict[str, str] = {}

        if not self.is_kv_producer:
            for req_id, (req, block_ids) in self._reqs_need_recv.items():
                assert req.kv_transfer_params is not None
                params = dict(req.kv_transfer_params)
                meta.add_new_req(
                    request_id=req_id,
                    local_block_ids=block_ids,
                    kv_transfer_params=params,
                )
                remote_engine_id = str(params.get("remote_engine_id") or "").strip()
                remote_engine_incarnation = str(
                    params.get("remote_engine_incarnation") or ""
                ).strip()
                if remote_engine_id and remote_engine_incarnation:
                    previous = remote_engine_incarnations.setdefault(
                        remote_engine_id, remote_engine_incarnation
                    )
                    if previous != remote_engine_incarnation:
                        raise RuntimeError(
                            "conflicting process incarnations for remote engine "
                            f"{remote_engine_id!r} in one connector metadata batch"
                        )
                routing_path = str(params.get("routing_path") or "UNKNOWN").strip().upper() or "UNKNOWN"
                request_routing_paths[str(req_id)] = routing_path
                transfer_routing_paths[str(params["transfer_id"])] = routing_path
            self._reqs_need_recv.clear()

        if not self.is_kv_consumer:
            for req_id, (req, block_ids) in self._reqs_need_send.items():
                assert req.kv_transfer_params is not None
                params = dict(req.kv_transfer_params)
                meta.add_new_req(
                    request_id=req_id,
                    local_block_ids=block_ids,
                    kv_transfer_params=params,
                    load_remote_cache=False,
                )
                routing_path = str(params.get("routing_path") or "UNKNOWN").strip().upper() or "UNKNOWN"
                request_routing_paths[str(req_id)] = routing_path
                transfer_routing_paths[str(params["transfer_id"])] = routing_path
            self._reqs_need_send.clear()
            meta.reqs_not_processed = self._reqs_not_processed
            self._reqs_not_processed = set()

        meta.request_routing_paths = request_routing_paths
        meta.transfer_routing_paths = transfer_routing_paths
        meta.remote_engine_incarnations = remote_engine_incarnations
        return meta


class MooncakeConnector(KVConnectorBase_V1, SupportsHMA):
    def __init__(
        self,
        vllm_config: VllmConfig,
        role: KVConnectorRole,
        kv_cache_config: "KVCacheConfig",
    ):
        super().__init__(vllm_config, role, kv_cache_config)
        assert vllm_config.kv_transfer_config is not None
        assert vllm_config.kv_transfer_config.engine_id is not None
        self.engine_id = vllm_config.kv_transfer_config.engine_id
        if role == KVConnectorRole.SCHEDULER:
            self.connector_scheduler: EPDMooncakeConnectorScheduler | None = (
                EPDMooncakeConnectorScheduler(vllm_config, self.engine_id, kv_cache_config)
            )
            self.connector_worker: EPDMooncakeConnectorWorker | None = None
        elif role == KVConnectorRole.WORKER:
            self.connector_scheduler = None
            self.connector_worker = EPDMooncakeConnectorWorker(
                vllm_config, self.engine_id, kv_cache_config
            )
        else:  # pragma: no cover - defensive
            raise ValueError(f"unsupported role: {role}")

    @classmethod
    def get_required_kvcache_layout(cls, vllm_config: VllmConfig):
        from vllm.distributed.kv_transfer.kv_connector.v1.mooncake.mooncake_connector import (
            MooncakeConnector as UpstreamMooncakeConnector,
        )

        return UpstreamMooncakeConnector.get_required_kvcache_layout(vllm_config)

    @classmethod
    def requires_piecewise_for_cudagraph(cls, extra_config: dict[str, Any]) -> bool:
        return bool((extra_config or {}).get("layered_kv_transfer", False))

    # ------------------------------------------------------------------
    # Scheduler-side methods
    # ------------------------------------------------------------------
    def get_num_new_matched_tokens(self, request, num_computed_tokens: int):
        assert self.connector_scheduler is not None
        return self.connector_scheduler.get_num_new_matched_tokens(request, num_computed_tokens)

    def update_state_after_alloc(self, request, blocks, num_external_tokens: int):
        assert self.connector_scheduler is not None
        return self.connector_scheduler.update_state_after_alloc(request, blocks, num_external_tokens)

    def build_connector_meta(self, scheduler_output):
        assert self.connector_scheduler is not None
        return self.connector_scheduler.build_connector_meta(scheduler_output)

    def request_finished(self, request, block_ids: list[int]):
        assert self.connector_scheduler is not None
        return self.connector_scheduler.request_finished(request, (block_ids,))

    def request_finished_all_groups(self, request, block_ids: tuple[list[int], ...]):
        assert self.connector_scheduler is not None
        return self.connector_scheduler.request_finished(request, block_ids)

    # ------------------------------------------------------------------
    # Worker-side methods
    # ------------------------------------------------------------------
    def register_kv_caches(self, kv_caches):
        assert self.connector_worker is not None
        self.connector_worker.register_kv_caches(kv_caches)

    def get_finished(self, finished_req_ids: set[str]):
        assert self.connector_worker is not None
        return self.connector_worker.get_finished()

    def start_load_kv(self, forward_context: "ForwardContext", **kwargs) -> None:
        assert self.connector_worker is not None
        assert isinstance(self._connector_metadata, MooncakeConnectorMetadata)
        self.connector_worker.start_load_kv(self._connector_metadata)

    def wait_for_layer_load(self, layer_name: str) -> None:
        assert self.connector_worker is not None
        self.connector_worker.wait_for_layer_load(layer_name)

    def save_kv_layer(
        self,
        layer_name: str,
        kv_layer,
        attn_metadata: "AttentionMetadata",
        **kwargs,
    ) -> None:
        assert self.connector_worker is not None
        self.connector_worker.save_kv_layer(layer_name, kv_layer, attn_metadata, **kwargs)

    def wait_for_save(self):
        if self.connector_worker is not None:
            self.connector_worker.wait_for_save()

    def get_kv_connector_stats(self) -> KVConnectorStats | None:
        if self.connector_worker is None:
            return None
        return self.connector_worker.get_kv_connector_stats()

    def build_connector_worker_meta(self):
        if self.connector_worker is None:
            return None
        return self.connector_worker.build_connector_worker_meta()

    @classmethod
    def build_kv_connector_stats(cls, data: dict[str, Any] | None = None) -> KVConnectorStats | None:
        return MooncakeKVConnectorStats(data=data or {})


class EPDMooncakeConnectorWorker(UpstreamMooncakeConnectorWorker):
    def __init__(
        self,
        vllm_config: VllmConfig,
        engine_id: str,
        kv_cache_config: "KVCacheConfig | None" = None,
    ):
        extra = dict(vllm_config.kv_transfer_config.kv_connector_extra_config or {})
        protocol = str(extra.get("mooncake_protocol", "")).strip().lower()
        force_tcp_transport = extra.get("force_tcp_transport", True)
        if protocol == "tcp" and force_tcp_transport:
            os.environ.setdefault("MC_FORCE_TCP", "1")
        super().__init__(vllm_config, engine_id, kv_cache_config)
        self.layered_kv_transfer = bool(extra.get("layered_kv_transfer", False))
        self.mooncake_protocol = protocol or str(os.getenv("MOONCAKE_PROTOCOL", "tcp"))
        self.transport_backend = str(
            extra.get("transport_backend", "mooncake_engine_direct")
        )
        self.layers_per_group = max(1, int(extra.get("layers_per_group", 4)))
        self.group_delay_ms = max(0.0, float(extra.get("group_delay_ms", 0.0)))
        self.max_group_bytes = max(0, int(extra.get("max_group_bytes", 0) or 0))
        backend_key = self.transport_backend.strip().lower()
        default_max_transfer_descriptors = (
            128
            if protocol == "rdma"
            or backend_key in {"rdmacm", "rdmacm_staged", "iwarp"}
            else 64
        )
        self.max_transfer_descriptors = max(
            1,
            int(
                extra.get(
                    "max_transfer_descriptors",
                    os.getenv(
                        "MOONCAKE_EPD_MAX_TRANSFER_DESCRIPTORS",
                        default_max_transfer_descriptors,
                    ),
                )
                or default_max_transfer_descriptors
            ),
        )
        self.max_transfer_bytes = max(
            0,
            int(
                extra.get(
                    "max_transfer_bytes",
                    os.getenv("MOONCAKE_EPD_MAX_TRANSFER_BYTES", 16 * 1024 * 1024),
                )
                or 0
            ),
        )
        self.enable_descriptor_coalescing = str(
            extra.get(
                "enable_descriptor_coalescing",
                os.getenv("MOONCAKE_EPD_ENABLE_DESCRIPTOR_COALESCING", "1"),
            )
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.rdmacm_bind_address = str(
            extra.get(
                "rdmacm_bind_address",
                os.getenv("MOONCAKE_EPD_RDMACM_BIND_ADDRESS", ""),
            )
        ).strip()
        self.rdmacm_remote_address = str(
            extra.get(
                "rdmacm_remote_address",
                os.getenv("MOONCAKE_EPD_RDMACM_REMOTE_ADDRESS", ""),
            )
        ).strip()
        self.rdmacm_port_offset = int(
            extra.get(
                "rdmacm_port_offset",
                os.getenv("MOONCAKE_EPD_RDMACM_PORT_OFFSET", 2711),
            )
        )
        self.allow_transfer_fallback = str(
            extra.get(
                "allow_transfer_fallback",
                os.getenv("MOONCAKE_EPD_ALLOW_TRANSFER_FALLBACK", "0"),
            )
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.transfer_retry_attempts = max(
            0,
            int(
                extra.get(
                    "transfer_retry_attempts",
                    os.getenv("MOONCAKE_EPD_TRANSFER_RETRY_ATTEMPTS", 6),
                )
                or 0
            ),
        )
        self.transfer_retry_backoff_ms = max(
            0.0,
            float(
                extra.get(
                    "transfer_retry_backoff_ms",
                    os.getenv("MOONCAKE_EPD_TRANSFER_RETRY_BACKOFF_MS", 250.0),
                )
                or 0.0
            ),
        )
        self.layer_load_timeout_seconds = max(
            1.0,
            float(extra.get("layer_load_timeout_seconds", 30.0)),
        )
        self.trace_layered_kv = str(
            extra.get(
                "layered_trace_log",
                os.getenv("MOONCAKE_EPD_TRACE_LAYERED_KV", "0"),
            )
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.trace_layered_kv_path = str(
            extra.get(
                "layered_trace_log_path",
                os.getenv(
                    "MOONCAKE_EPD_TRACE_LAYERED_KV_PATH",
                    "/tmp/mooncake_epd_layered_kv.log",
                ),
            )
        )
        connector_metrics_dir = str(
            extra.get(
                "connector_metrics_dir",
                os.getenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", ""),
            )
        ).strip()
        connector_metrics_flush_interval_s = max(
            0.0,
            float(
                extra.get(
                    "connector_metrics_flush_interval_s",
                    os.getenv("MOONCAKE_EPD_CONNECTOR_METRICS_FLUSH_INTERVAL_S", 0.25),
                )
                or 0.0
            ),
        )
        connector_metrics_max_pending_records = max(
            1,
            int(
                extra.get(
                    "connector_metrics_max_pending_records",
                    os.getenv("MOONCAKE_EPD_CONNECTOR_METRICS_MAX_PENDING", 64),
                )
                or 1
            ),
        )
        self._worker_meta = LayeredTransferWorkerMeta()
        self._connector_metrics_pending = LayeredTransferWorkerMeta()
        self._connector_metrics_pending_by_path: dict[str, LayeredTransferWorkerMeta] = {}
        self._connector_metrics_sink = ConnectorMetricsSink(
            connector_metrics_dir or None,
            engine_id=self.engine_id,
            role="producer" if self.is_kv_producer else "consumer",
            hostname=getattr(self, "hostname", ""),
            rpc_port=getattr(self, "rpc_port", None),
            tp_rank=getattr(self, "tp_rank", None),
            flush_interval_s=connector_metrics_flush_interval_s,
            max_pending_records=connector_metrics_max_pending_records,
        )
        self._peer_transfer_engine: PeerTransferEngine | None = None
        self._rdmacm_transfer_engine: PeerTransferEngine | None = None
        self._registered_region_count = 1
        self._send_request_routing_paths: dict[str, str] = {}
        self._send_transfer_routing_paths: dict[str, str] = {}
        self._recv_request_routing_paths: dict[str, str] = {}
        self._recv_transfer_routing_paths: dict[str, str] = {}
        self._remote_engine_incarnations: dict[str, str] = {}
        self._remote_incarnation_lock = asyncio.Lock()
        self._layer_names: list[str] = []
        self._layer_base_counts: list[int] = []
        self._layer_to_index: dict[str, int] = {}
        self._layer_to_group: dict[str, int] = {}
        self._layer_region_slices: dict[str, _LayerRegionSlice] = {}
        self._group_region_slices: list[_LayerRegionSlice] = []
        self._sender_group_count = 1
        self._layered_send_lock = threading.RLock()
        self._layered_send_states: dict[str, _LayeredSendState] = {}
        self._current_send_transfer_ids: set[str] = set()
        self._layered_recv_lock = threading.RLock()
        self._layered_recv_states: dict[str, _LayeredReceiveState] = {}
        self._current_recv_req_ids: list[str] = []
        if self.layered_kv_transfer:
            self._xfer_meta_decoder = msgspec.msgpack.Decoder(LayeredMooncakeXferMetadata)
            self._xfer_resp_decoder = msgspec.msgpack.Decoder(LayeredMooncakeXferResponse)
        self._trace(
            "worker init engine_id=%s role=%s layered=%s groups=%d trace_path=%s",
            self.engine_id,
            "producer" if self.is_kv_producer else "consumer",
            self.layered_kv_transfer,
            self._sender_group_count,
            self.trace_layered_kv_path,
        )

    # ------------------------------------------------------------------
    # Layer/group mapping
    # ------------------------------------------------------------------
    def _collect_layer_base_counts(self, kv_caches: dict[str, Any]) -> None:
        self._layer_names = list(kv_caches.keys())
        self._layer_base_counts = []
        split_k_and_v = self.transfer_topo.split_k_and_v
        for _, cache_or_caches in kv_caches.items():
            cache_list = cache_or_caches if split_k_and_v else [cache_or_caches]
            seen: set[int] = set()
            count = 0
            for cache in cache_list:
                base_addr = int(cache.data_ptr())
                if base_addr in seen:
                    continue
                seen.add(base_addr)
                count += 1
            self._layer_base_counts.append(max(1, count))

    def _rebuild_layer_group_mappings(self) -> None:
        self._layer_to_index = {name: idx for idx, name in enumerate(self._layer_names)}
        self._layer_to_group = {
            name: idx // self.layers_per_group for idx, name in enumerate(self._layer_names)
        }
        self._sender_group_count = max(1, math.ceil(len(self._layer_names) / self.layers_per_group))
        self._layer_region_slices = {}
        self._group_region_slices = []

        if not self._layer_names or not self.kv_caches_base_addr or not self.block_len_per_layer:
            return

        regions = self._get_transfer_regions(self.kv_caches_base_addr, self.block_len_per_layer)
        self._registered_region_count = max(1, len(regions))
        base_count = max(1, len(self.kv_caches_base_addr))
        regions_per_base = max(1, len(regions) // base_count)

        cursor = 0
        for layer_name, layer_base_count in zip(self._layer_names, self._layer_base_counts):
            region_count = max(1, layer_base_count * regions_per_base)
            start = min(cursor, len(regions))
            stop = min(len(regions), start + region_count)
            self._layer_region_slices[layer_name] = _LayerRegionSlice(start=start, stop=stop)
            cursor = stop

        if cursor != len(regions):
            logger.warning(
                "EPD layered connector region mapping mismatch: mapped=%d total=%d. "
                "Falling back to evenly-partitioned layer slices.",
                cursor,
                len(regions),
            )
            per_layer = max(1, math.ceil(len(regions) / max(1, len(self._layer_names))))
            self._layer_region_slices = {}
            cursor = 0
            for idx, layer_name in enumerate(self._layer_names):
                start = min(cursor, len(regions))
                stop = min(len(regions), start + per_layer)
                if idx == len(self._layer_names) - 1:
                    stop = len(regions)
                self._layer_region_slices[layer_name] = _LayerRegionSlice(start=start, stop=stop)
                cursor = stop

        for group_idx in range(self._sender_group_count):
            first_idx = group_idx * self.layers_per_group
            last_idx = min(len(self._layer_names), first_idx + self.layers_per_group) - 1
            first_layer = self._layer_names[first_idx]
            last_layer = self._layer_names[last_idx]
            first_slice = self._layer_region_slices[first_layer]
            last_slice = self._layer_region_slices[last_layer]
            self._group_region_slices.append(
                _LayerRegionSlice(start=first_slice.start, stop=last_slice.stop)
            )

    def _group_for_layer(self, layer_name: str) -> int | None:
        return self._layer_to_group.get(str(layer_name))

    def _is_group_tail_layer(self, layer_name: str) -> bool:
        layer_idx = self._layer_to_index.get(str(layer_name))
        if layer_idx is None:
            return False
        group_idx = layer_idx // self.layers_per_group
        tail_idx = min(len(self._layer_names), (group_idx + 1) * self.layers_per_group) - 1
        return layer_idx == tail_idx

    def _iter_current_recv_states(self) -> Iterable[_LayeredReceiveState]:
        with self._layered_recv_lock:
            req_ids = list(self._current_recv_req_ids)
            states = [
                self._layered_recv_states[req_id]
                for req_id in req_ids
                if req_id in self._layered_recv_states
            ]
        return states

    def _ensure_layered_send_state(self, transfer_id: str) -> _LayeredSendState:
        with self._layered_send_lock:
            state = self._layered_send_states.get(transfer_id)
            if state is None or state.total_groups != self._sender_group_count:
                state = _LayeredSendState.create(transfer_id, self._sender_group_count)
                self._layered_send_states[transfer_id] = state
            return state

    def _trace(self, message: str, *args: Any) -> None:
        if not self.trace_layered_kv:
            return
        rendered = (message % args) if args else message
        logger.info("EPD layered KV | %s", rendered)
        try:
            with open(self.trace_layered_kv_path, "a", encoding="utf-8") as fh:
                fh.write(
                    f"{time.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"engine={self.engine_id} "
                    f"role={'producer' if self.is_kv_producer else 'consumer'} "
                    f"{rendered}\n"
                )
        except Exception:
            logger.debug("failed to append layered trace file", exc_info=True)

    def _mark_group_ready(self, group_idx: int) -> None:
        with self._layered_send_lock:
            active_transfer_ids = set(getattr(self, "_current_send_transfer_ids", set()) or set())
            if active_transfer_ids:
                states = [
                    state
                    for transfer_id, state in self._layered_send_states.items()
                    if transfer_id in active_transfer_ids
                ]
            else:
                # Backward-compatible fallback for direct unit tests or older vLLM
                # call paths that invoke save_kv_layer without a preceding
                # start_load_kv metadata scope.
                states = list(self._layered_send_states.values())
        for state in states:
            state.mark_group_ready(group_idx)

    def _mark_ready_groups_up_to(self, group_idx: int) -> None:
        for ready_group_idx in range(0, max(0, int(group_idx)) + 1):
            self._mark_group_ready(ready_group_idx)

    def _fail_all_send_states(self, message: str) -> None:
        with self._layered_send_lock:
            states = list(self._layered_send_states.values())
        for state in states:
            state.fail(message)

    def _record_send_routing_paths(self, metadata: MooncakeConnectorMetadata) -> None:
        request_paths = dict(getattr(metadata, "request_routing_paths", {}) or {})
        transfer_paths = dict(getattr(metadata, "transfer_routing_paths", {}) or {})
        for req_id, (transfer_id, _) in metadata.reqs_to_send.items():
            routing_path = self._normalize_routing_path(
                request_paths.get(req_id) or transfer_paths.get(transfer_id)
            )
            self._send_request_routing_paths[str(req_id)] = routing_path
            self._send_transfer_routing_paths[str(transfer_id)] = routing_path
        for transfer_id in metadata.reqs_not_processed:
            self._send_transfer_routing_paths.pop(str(transfer_id), None)

    def _record_recv_routing_paths(self, metadata: MooncakeConnectorMetadata) -> None:
        request_paths = dict(getattr(metadata, "request_routing_paths", {}) or {})
        transfer_paths = dict(getattr(metadata, "transfer_routing_paths", {}) or {})
        for pull_metas in metadata.reqs_to_recv.values():
            for req_id, pull_meta in pull_metas.items():
                routing_path = self._normalize_routing_path(
                    request_paths.get(req_id) or transfer_paths.get(pull_meta.transfer_id)
                )
                self._recv_request_routing_paths[str(req_id)] = routing_path
                self._recv_transfer_routing_paths[str(pull_meta.transfer_id)] = routing_path

    def _routing_path_for_send(
        self,
        *,
        req_id: str | None = None,
        transfer_id: str | None = None,
    ) -> str:
        if req_id is not None:
            path = self._send_request_routing_paths.get(str(req_id))
            if path:
                return path
        if transfer_id is not None:
            path = self._send_transfer_routing_paths.get(str(transfer_id))
            if path:
                return path
        return "UNKNOWN"

    def _routing_path_for_recv(
        self,
        *,
        req_id: str | None = None,
        transfer_id: str | None = None,
    ) -> str:
        if req_id is not None:
            path = self._recv_request_routing_paths.get(str(req_id))
            if path:
                return path
        if transfer_id is not None:
            path = self._recv_transfer_routing_paths.get(str(transfer_id))
            if path:
                return path
        return "UNKNOWN"

    def _ensure_layered_recv_states(
        self,
        pull_metas: dict[str, PullReqMeta],
        *,
        expected_tasks: int,
    ) -> None:
        with self._layered_recv_lock:
            for req_id, pull_meta in pull_metas.items():
                state = self._layered_recv_states.get(req_id)
                if state is None:
                    self._layered_recv_states[req_id] = _LayeredReceiveState.create(
                        request_id=req_id,
                        transfer_id=pull_meta.transfer_id,
                        total_groups=self._sender_group_count,
                        expected_tasks=expected_tasks,
                    )
                else:
                    state.reset_expected_tasks(expected_tasks)

    # ------------------------------------------------------------------
    # vLLM connector methods
    # ------------------------------------------------------------------
    def register_kv_caches(self, kv_caches: dict[str, Any]):
        self._collect_layer_base_counts(kv_caches)
        super().register_kv_caches(kv_caches)
        self._rebuild_layer_group_mappings()
        if (
            self.transport_backend.strip().lower()
            in {"rdmacm", "rdmacm_staged", "iwarp"}
            and self.is_kv_consumer
        ):
            engine = self._get_rdmacm_transfer_engine()
            server = engine.start_rdmacm_server(
                bind_address=self.rdmacm_bind_address,
                port=self._rdmacm_port(self.rpc_port),
            )
            for base_address, block_len in zip(
                self.kv_caches_base_addr,
                self.block_len_per_layer,
            ):
                server.register_region(
                    int(base_address),
                    int(self.num_blocks) * int(block_len),
                    memory_kind="cuda",
                )
            logger.info(
                "EPD rdmacm staged receiver listening address=%s port=%d regions=%d",
                server.bind_address,
                server.port,
                len(self.kv_caches_base_addr),
            )

    def shutdown(self):
        with contextlib.suppress(Exception):
            self._publish_connector_metrics(force=True)
        sink = getattr(self, "_connector_metrics_sink", None)
        if sink is not None:
            with contextlib.suppress(Exception):
                sink.close()
        rdmacm_engine = getattr(self, "_rdmacm_transfer_engine", None)
        if rdmacm_engine is not None:
            with contextlib.suppress(Exception):
                rdmacm_engine.shutdown()
            self._rdmacm_transfer_engine = None
        return super().shutdown()

    def start_load_kv(self, metadata: MooncakeConnectorMetadata):
        remote_engine_incarnations = dict(
            getattr(metadata, "remote_engine_incarnations", {}) or {}
        )
        if remote_engine_incarnations:
            for remote_engine_id, pull_metas in metadata.reqs_to_recv.items():
                incarnation = str(
                    remote_engine_incarnations.get(str(remote_engine_id)) or ""
                ).strip()
                if not incarnation:
                    continue
                for pull_meta in pull_metas.values():
                    # PullReqMeta is an upstream non-slotted dataclass.  Attach
                    # the process token before upstream schedules _start_load_kv
                    # so the receiver event loop can fence its own topology
                    # cache without cross-thread mutation.
                    pull_meta.remote_engine_incarnation = incarnation
        if self.layered_kv_transfer and not self.is_kv_producer and metadata.reqs_to_recv:
            self._record_recv_routing_paths(metadata)
            with self._layered_recv_lock:
                self._current_recv_req_ids = [
                    req_id
                    for pull_metas in metadata.reqs_to_recv.values()
                    for req_id in pull_metas
                ]
            for pull_metas in metadata.reqs_to_recv.values():
                self._ensure_layered_recv_states(pull_metas, expected_tasks=1)
            self._trace(
                "consumer start_load_kv reqs=%s groups=%d",
                self._current_recv_req_ids,
                self._sender_group_count,
            )
        if self.layered_kv_transfer and not self.is_kv_consumer:
            self._record_send_routing_paths(metadata)
            with self._layered_send_lock:
                self._current_send_transfer_ids = {
                    str(transfer_id)
                    for _, (transfer_id, _) in metadata.reqs_to_send.items()
                }
                for _, (transfer_id, _) in metadata.reqs_to_send.items():
                    self._layered_send_states.setdefault(
                        transfer_id,
                        _LayeredSendState.create(transfer_id, self._sender_group_count),
                    )
                for transfer_id in metadata.reqs_not_processed:
                    self._layered_send_states.pop(transfer_id, None)
                    self._current_send_transfer_ids.discard(str(transfer_id))
            self._trace(
                "producer start_load_kv send=%s not_processed=%s groups=%d",
                list(metadata.reqs_to_send),
                list(metadata.reqs_not_processed),
                self._sender_group_count,
            )
        super().start_load_kv(metadata)

    async def _start_load_kv(
        self,
        reqs_to_recv: dict[str, dict[str, PullReqMeta]],
    ):
        remote_engine_incarnations: dict[str, str] = {}
        remote_bootstrap_addrs: dict[str, str] = {}
        for remote_engine_id, pull_metas in reqs_to_recv.items():
            incarnations = {
                str(getattr(pull_meta, "remote_engine_incarnation", "") or "").strip()
                for pull_meta in pull_metas.values()
            }
            incarnations.discard("")
            if len(incarnations) > 1:
                raise RuntimeError(
                    "conflicting process incarnations for remote engine "
                    f"{remote_engine_id!r} in one receive batch"
                )
            if incarnations:
                remote_engine_incarnations[str(remote_engine_id)] = incarnations.pop()
            if pull_metas:
                remote_bootstrap_addrs[str(remote_engine_id)] = str(
                    next(iter(pull_metas.values())).remote_bootstrap_addr
                ).rstrip("/")

        await self._fence_remote_engine_incarnations(
            remote_engine_incarnations,
            remote_bootstrap_addrs,
        )
        await super()._start_load_kv(reqs_to_recv)

    async def _fence_remote_engine_incarnations(
        self,
        remote_engine_incarnations: dict[str, str],
        remote_bootstrap_addrs: dict[str, str],
    ) -> list[str]:
        """Invalidate only stale Prefill bootstrap topology on its event loop.

        The upstream Mooncake connector caches bootstrap worker addresses by
        stable ``engine_id``.  A restarted Prefill intentionally keeps that
        ID, so a long-lived Decode otherwise keeps sending ZMQ pull requests
        to the dead process.  Process incarnation is an explicit cache epoch:
        first observation and repeats are free; a change drains any bootstrap
        query already in flight, removes only that engine's topology, and lets
        upstream singleflight rediscover the new worker address.
        """

        if not remote_engine_incarnations:
            return []

        refreshed: list[str] = []
        observations = 0
        pending_waits = 0
        pending_wait_ms = 0.0
        lock = getattr(self, "_remote_incarnation_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._remote_incarnation_lock = lock

        async with lock:
            for remote_engine_id in sorted(remote_engine_incarnations):
                incarnation = str(
                    remote_engine_incarnations[remote_engine_id] or ""
                ).strip()
                if not incarnation:
                    continue
                observations += 1
                previous = self._remote_engine_incarnations.get(remote_engine_id)
                if previous is None:
                    self._remote_engine_incarnations[remote_engine_id] = incarnation
                    logger.info(
                        "EPD observed Prefill topology incarnation engine=%s epoch=%s",
                        remote_engine_id,
                        hashlib.sha256(incarnation.encode("utf-8")).hexdigest()[:16],
                    )
                    continue
                if previous == incarnation:
                    continue

                remote_bootstrap_addr = str(
                    remote_bootstrap_addrs.get(remote_engine_id) or ""
                ).rstrip("/")
                pending = self._pending_bootstrap_queries.get(remote_bootstrap_addr)
                if pending is not None:
                    wait_started = time.perf_counter()
                    pending_waits += 1
                    await pending.wait()
                    pending_wait_ms += (time.perf_counter() - wait_started) * 1000.0

                had_cached_topology = remote_engine_id in self._remote_agents
                self._remote_agents.pop(remote_engine_id, None)
                self._tp_size.pop(remote_engine_id, None)
                self._remote_engine_incarnations[remote_engine_id] = incarnation
                refreshed.append(remote_engine_id)
                logger.warning(
                    "EPD invalidated stale Prefill topology engine=%s old_epoch=%s "
                    "new_epoch=%s cached=%s bootstrap=%s",
                    remote_engine_id,
                    hashlib.sha256(previous.encode("utf-8")).hexdigest()[:16],
                    hashlib.sha256(incarnation.encode("utf-8")).hexdigest()[:16],
                    had_cached_topology,
                    remote_bootstrap_addr,
                )

        self._accumulate_worker_meta(
            LayeredTransferWorkerMeta(
                topology_incarnation_observations=observations,
                topology_incarnation_refreshes=len(refreshed),
                topology_incarnation_pending_waits=pending_waits,
                topology_incarnation_wait_ms=pending_wait_ms,
            )
        )
        return refreshed

    def wait_for_layer_load(self, layer_name: str) -> None:
        if not self.layered_kv_transfer or self.is_kv_producer:
            return
        group_idx = self._group_for_layer(layer_name)
        if group_idx is None:
            return
        start_time = time.perf_counter()
        self._trace(
            "wait_for_layer_load layer=%s group=%d active_recv=%s",
            layer_name,
            group_idx,
            list(self._current_recv_req_ids),
        )
        try:
            for state in self._iter_current_recv_states():
                state.wait_group(group_idx, self.layer_load_timeout_seconds)
        finally:
            self._accumulate_worker_meta(
                LayeredTransferWorkerMeta(
                    layer_wait_calls=1,
                    layer_wait_ms=(time.perf_counter() - start_time) * 1000.0,
                )
            )
        self._trace(
            "wait_for_layer_load released layer=%s group=%d",
            layer_name,
            group_idx,
        )

    def save_kv_layer(
        self,
        layer_name: str,
        kv_layer,
        attn_metadata: "AttentionMetadata",
        **kwargs,
    ) -> None:
        del kv_layer, attn_metadata, kwargs
        if not self.layered_kv_transfer or self.is_kv_consumer:
            return
        group_idx = self._group_for_layer(layer_name)
        if group_idx is None:
            return
        if not self._is_group_tail_layer(layer_name):
            return
        # vLLM's save_kv_layer callback is layer-scoped rather than
        # transfer_id-scoped.  Under chunked/prefill scheduling the first tail
        # callback observed by the connector can be a later layer group.  Marking
        # all earlier groups ready is conservative for waiters and prevents a
        # later-layer callback from leaving group-0 forever blocked.  The sender
        # still waits for request block metadata before issuing any transfer.
        self._mark_ready_groups_up_to(group_idx)
        self._trace(
            "save_kv_layer tail layer=%s group=%d announced_up_to=%d",
            layer_name,
            group_idx,
            group_idx,
        )

    def wait_for_save(self):
        return None

    def get_finished(self) -> tuple[set[str] | None, set[str] | None]:
        finished_sending, finished_recving = super().get_finished()
        for req_id in list(finished_sending or set()):
            self._send_request_routing_paths.pop(str(req_id), None)
        if self.layered_kv_transfer:
            with self._layered_recv_lock:
                for req_id in list(finished_recving or set()):
                    state = self._layered_recv_states.pop(req_id, None)
                    if req_id in self._current_recv_req_ids:
                        self._current_recv_req_ids.remove(req_id)
                    self._recv_request_routing_paths.pop(str(req_id), None)
                    if state is not None:
                        self._recv_transfer_routing_paths.pop(
                            str(state.transfer_id), None
                        )
        if finished_sending or finished_recving:
            # Normal metadata publication remains batched, but a completed
            # request is a durability/observability boundary: force the final
            # cumulative snapshot so the proxy can settle metrics without
            # turning every scheduler metadata build into atomic file I/O.
            self._publish_connector_metrics(force=True)
        return finished_sending, finished_recving

    def build_connector_worker_meta(self):
        meta = self._worker_meta
        self._publish_connector_metrics(force=False)
        if meta.is_empty():
            return None
        self._worker_meta = LayeredTransferWorkerMeta()
        return meta

    @staticmethod
    def _normalize_routing_path(path: str | None) -> str:
        normalized = str(path or "UNKNOWN").strip().upper()
        return normalized or "UNKNOWN"

    def _accumulate_worker_meta(self, delta: LayeredTransferWorkerMeta | None) -> None:
        if delta is None or delta.is_empty():
            return
        if not hasattr(self, "_worker_meta"):
            self._worker_meta = LayeredTransferWorkerMeta()
        if not hasattr(self, "_connector_metrics_pending"):
            self._connector_metrics_pending = LayeredTransferWorkerMeta()
        self._worker_meta = self._worker_meta.aggregate(delta)
        self._connector_metrics_pending = self._connector_metrics_pending.aggregate(
            delta
        )

    def _accumulate_worker_meta_by_path(
        self,
        path_deltas: dict[str, LayeredTransferWorkerMeta] | None,
    ) -> None:
        if not path_deltas:
            return
        if not hasattr(self, "_connector_metrics_pending_by_path"):
            self._connector_metrics_pending_by_path = {}
        for path, delta in dict(path_deltas).items():
            if delta is None or delta.is_empty():
                continue
            bucket = self._normalize_routing_path(path)
            existing = self._connector_metrics_pending_by_path.get(
                bucket, LayeredTransferWorkerMeta()
            )
            self._connector_metrics_pending_by_path[bucket] = existing.aggregate(delta)

    def _publish_connector_metrics(self, *, force: bool = False) -> None:
        sink = getattr(self, "_connector_metrics_sink", None)
        if sink is None:
            return
        pending = getattr(self, "_connector_metrics_pending", None)
        pending_by_path = dict(
            getattr(self, "_connector_metrics_pending_by_path", {}) or {}
        )
        has_path_pending = any(not meta.is_empty() for meta in pending_by_path.values())
        if (pending is None or pending.is_empty()) and not has_path_pending:
            if force:
                sink.flush()
            return
        sink.record(pending, path_totals=pending_by_path, force=force)
        self._connector_metrics_pending = LayeredTransferWorkerMeta()
        self._connector_metrics_pending_by_path = {}

    # ------------------------------------------------------------------
    # Consumer receive path
    # ------------------------------------------------------------------
    def receive_kv(
        self,
        remote_engine_id: str,
        pull_metas: dict[str, PullReqMeta],
    ):
        if self.layered_kv_transfer:
            remote_tp_ranks = self.transfer_topo.handshake_target_ranks(
                self._tp_size[remote_engine_id]
            )
            self._ensure_layered_recv_states(
                pull_metas,
                expected_tasks=len(remote_tp_ranks),
            )
            self._trace(
                "receive_kv remote_engine=%s reqs=%s expected_tasks=%d",
                remote_engine_id,
                list(pull_metas),
                len(remote_tp_ranks),
            )
        return super().receive_kv(remote_engine_id, pull_metas)

    async def receive_kv_from_single_worker(
        self,
        worker_addr: str,
        pull_metas: dict[str, PullReqMeta],
    ):
        if not self.layered_kv_transfer:
            return await super().receive_kv_from_single_worker(worker_addr, pull_metas)

        req_ids = set(pull_metas)
        metadata = LayeredMooncakeXferMetadata(
            remote_hostname=self.hostname,
            remote_port=self.rpc_port,
            remote_tp_size=self.tp_size,
            remote_tp_rank=self.tp_rank,
            req_blocks={
                req_id: (pull_meta.transfer_id, pull_meta.local_block_ids)
                for req_id, pull_meta in pull_metas.items()
            },
            kv_caches_base_addr=self.kv_caches_base_addr,
            block_lens=self.block_len_per_layer,
            layered=True,
            total_groups=self._sender_group_count,
        )

        encoded_data = self._encoder.encode(metadata)
        logger.debug(
            "Size of encoded LayeredMooncakeXferMetadata: %d bytes", len(encoded_data)
        )
        logger.debug(
            "Sending layered kv transfer request for %s on path: %s", req_ids, worker_addr
        )
        self._trace(
            "receive_kv_from_single_worker send_meta reqs=%s worker=%s groups=%d",
            list(req_ids),
            worker_addr,
            self._sender_group_count,
        )

        try:
            from vllm import envs
            from vllm.utils.network_utils import make_zmq_socket
            import zmq

            with make_zmq_socket(
                self.async_zmq_ctx, worker_addr, zmq.DEALER, bind=False, linger=0
            ) as sock:
                sock.setsockopt(
                    zmq.RCVTIMEO, (envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT + 60) * 1000
                )
                await sock.send(encoded_data)
                while True:
                    ret_msg = await sock.recv()
                    response = self._xfer_resp_decoder.decode(ret_msg)
                    if response.status == MooncakeXferResponseStatus.ERROR:
                        logger.error(
                            "Error happens during layered transferring kvcache for %s: %s",
                            req_ids,
                            response.err_msg,
                        )
                        self.xfer_stats.record_failed_recv()
                        self._accumulate_worker_meta(
                            LayeredTransferWorkerMeta(
                                receive_failures=max(1, len(req_ids)),
                            )
                        )
                        self._publish_connector_metrics()
                        with self._layered_recv_lock:
                            for req_id in req_ids:
                                state = self._layered_recv_states.get(req_id)
                                if state is not None:
                                    state.fail(response.err_msg or "layered transfer failed")
                        return
                    self.process_pulling_result(response, pull_metas)
                    if response.status == MooncakeXferResponseStatus.FINISH:
                        self._trace(
                            "receive_kv_from_single_worker finished reqs=%s worker=%s",
                            list(req_ids),
                            worker_addr,
                        )
                        break
        except Exception as exc:
            logger.error("LayeredMooncake transfer failed for %s: %s", req_ids, exc)
            self.xfer_stats.record_failed_recv()
            self._accumulate_worker_meta(
                LayeredTransferWorkerMeta(
                    receive_failures=max(1, len(req_ids)),
                )
            )
            self._publish_connector_metrics()
            with self._layered_recv_lock:
                for req_id in req_ids:
                    state = self._layered_recv_states.get(req_id)
                    if state is not None:
                        state.fail(str(exc))
            return

    def process_pulling_result(
        self,
        response: MooncakeXferResponse | LayeredMooncakeXferResponse,
        pull_metas: dict[str, PullReqMeta],
    ):
        if not self.layered_kv_transfer:
            return super().process_pulling_result(response, pull_metas)

        ok_reqs = list(response.ok_reqs or [])
        group_idx = int(getattr(response, "group_index", -1))
        response_total_groups = int(getattr(response, "total_groups", 0) or 0)
        finished_reqs: list[str] = []
        finished_paths: list[str] = []
        with self._layered_recv_lock:
            for req_id in ok_reqs:
                state = self._layered_recv_states.get(req_id)
                if state is None:
                    continue
                if response_total_groups > 0:
                    state.ensure_total_groups(response_total_groups)
                if group_idx >= 0:
                    state.ack_group(group_idx)
                if state.complete():
                    pull_meta = pull_metas.get(req_id)
                    finished_req_id = (
                        pull_meta.d_req_id
                        if pull_meta is not None and getattr(pull_meta, "d_req_id", None)
                        else req_id
                    )
                    if finished_req_id not in self.finished_recving_reqs:
                        self.finished_recving_reqs.add(finished_req_id)
                        finished_reqs.append(finished_req_id)
                        finished_paths.append(self._routing_path_for_recv(req_id=req_id))

            if response.err_reqs:
                for req_id in response.err_reqs:
                    state = self._layered_recv_states.get(req_id)
                    if state is not None:
                        state.fail(response.err_msg or "layered transfer failed")

        delta = LayeredTransferWorkerMeta(
            received_group_batches=1 if ok_reqs and group_idx >= 0 else 0,
            received_finished_reqs=len(finished_reqs),
            receive_failures=len(response.err_reqs or []),
        )
        path_deltas: dict[str, LayeredTransferWorkerMeta] = {}
        if ok_reqs and group_idx >= 0:
            for req_id in ok_reqs:
                path = self._routing_path_for_recv(req_id=req_id)
                existing = path_deltas.get(path, LayeredTransferWorkerMeta())
                if existing.received_group_batches == 0:
                    existing = existing.aggregate(
                        LayeredTransferWorkerMeta(received_group_batches=1)
                    )
                path_deltas[path] = existing
        for path in finished_paths:
            existing = path_deltas.get(path, LayeredTransferWorkerMeta())
            path_deltas[path] = existing.aggregate(
                LayeredTransferWorkerMeta(received_finished_reqs=1)
            )
        for req_id in response.err_reqs or []:
            path = self._routing_path_for_recv(req_id=req_id)
            existing = path_deltas.get(path, LayeredTransferWorkerMeta())
            path_deltas[path] = existing.aggregate(
                LayeredTransferWorkerMeta(receive_failures=1)
            )
        self._accumulate_worker_meta(delta)
        self._accumulate_worker_meta_by_path(path_deltas)
        if not delta.is_empty():
            self._publish_connector_metrics()

        if ok_reqs:
            self._trace(
                "process_pulling_result group=%s ok=%s finished=%s",
                group_idx,
                ok_reqs,
                finished_reqs,
            )

        if response.err_reqs:
            logger.error(
                "layered pulling kv_caches for %s failed: %s",
                response.err_reqs,
                response.err_msg,
            )

    # ------------------------------------------------------------------
    # Producer send path
    # ------------------------------------------------------------------
    async def record_send_reqs(self, metadata: MooncakeConnectorMetadata):
        if not self.layered_kv_transfer:
            await super().record_send_reqs(metadata)
            return

        from vllm import envs

        ready_transfers: list[str] = []
        pending_transfers: list[str] = []

        for p_req_id, (transfer_id, block_ids) in metadata.reqs_to_send.items():
            send_meta = self.reqs_need_send.get(transfer_id)
            if send_meta is None:
                send_meta = SendBlockMeta(
                    p_req_id=p_req_id,
                    transfer_id=transfer_id,
                    local_block_ids=[],
                    ready=asyncio.Event(),
                )
                self.reqs_need_send[transfer_id] = send_meta
            elif not send_meta.p_req_id:
                send_meta.p_req_id = p_req_id

            has_any_block = bool(block_ids) and any(len(group) > 0 for group in block_ids)
            if has_any_block:
                send_meta.p_req_id = p_req_id
                send_meta.local_block_ids = block_ids
                send_meta.expire_time = (
                    time.perf_counter() + envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT
                )
                send_meta.ready.set()
                ready_transfers.append(transfer_id)
            else:
                send_meta.local_block_ids = block_ids
                pending_transfers.append(transfer_id)

        for transfer_id in metadata.reqs_not_processed:
            send_meta = self.reqs_need_send.pop(transfer_id, None)
            if send_meta is not None and send_meta.ready.is_set():
                logger.warning(
                    "EPD layered connector dropping already-ready transfer_id=%s "
                    "because scheduler marked it not processed.",
                    transfer_id,
                )

        with self._layered_send_lock:
            self._current_send_transfer_ids = {
                str(transfer_id)
                for _, (transfer_id, _) in metadata.reqs_to_send.items()
            }
            for _, (transfer_id, _) in metadata.reqs_to_send.items():
                self._layered_send_states.setdefault(
                    transfer_id,
                    _LayeredSendState.create(transfer_id, self._sender_group_count),
                )
            for transfer_id in metadata.reqs_not_processed:
                self._layered_send_states.pop(transfer_id, None)
                self._current_send_transfer_ids.discard(str(transfer_id))
        self._trace(
            "record_send_reqs ready=%s pending=%s not_processed=%s",
            ready_transfers,
            pending_transfers,
            list(metadata.reqs_not_processed),
        )

    async def send_kv_to_decode(
        self,
        identity: bytes,
        sock,
        meta: MooncakeXferMetadata | LayeredMooncakeXferMetadata,
    ):
        if not self.layered_kv_transfer or not getattr(meta, "layered", False):
            return await self._send_kv_to_decode_nonlayered(identity, sock, meta)

        from vllm import envs

        pending_reqs: dict[str, SendBlockMeta] = {}
        remote_tp_ranks = self.transfer_topo.handshake_target_ranks(meta.remote_tp_size)
        if meta.remote_tp_rank not in remote_tp_ranks:
            msg = (
                "This D tp_rank "
                f"{meta.remote_tp_rank} is not paired with P tp_rank "
                f"{self.tp_rank}; expected one of {remote_tp_ranks}."
            )
            logger.error(msg)
            response = LayeredMooncakeXferResponse(
                status=MooncakeXferResponseStatus.ERROR,
                err_msg=msg,
            )
            await sock.send_multipart((identity, self._encoder.encode(response)))
            return

        local_regions = self._get_transfer_regions(
            self.kv_caches_base_addr,
            self.block_len_per_layer,
        )
        remote_regions = self._get_transfer_regions(
            meta.kv_caches_base_addr,
            meta.block_lens,
        )
        validation_err = self._validate_regions(local_regions, remote_regions, meta)
        if validation_err is not None:
            response = LayeredMooncakeXferResponse(
                status=MooncakeXferResponseStatus.ERROR,
                err_msg=validation_err,
            )
            await sock.send_multipart((identity, self._encoder.encode(response)))
            return

        for d_req_id, (transfer_id, _) in meta.req_blocks.items():
            if transfer_id not in self.reqs_need_send:
                self.reqs_need_send[transfer_id] = SendBlockMeta(
                    p_req_id="",
                    transfer_id=transfer_id,
                    local_block_ids=[],
                    ready=asyncio.Event(),
                )
            pending_reqs[d_req_id] = self.reqs_need_send[transfer_id]
        requested_total_groups = int(getattr(meta, "total_groups", 0) or 0)
        total_groups = self._sender_group_count
        if requested_total_groups not in (0, total_groups):
            message = (
                "Layered KV group-count mismatch: consumer requested "
                f"{requested_total_groups} groups but producer has {total_groups}. "
                "Refusing transfer to avoid layer/KV slice misalignment."
            )
            logger.error(message)
            self._trace(
                "send_kv_to_decode group_mismatch consumer=%d producer=%d reqs=%s",
                requested_total_groups,
                total_groups,
                list(pending_reqs),
            )
            response = LayeredMooncakeXferResponse(
                status=MooncakeXferResponseStatus.ERROR,
                err_reqs=list(pending_reqs),
                err_msg=message,
                total_groups=total_groups,
            )
            await sock.send_multipart((identity, self._encoder.encode(response)))
            return
        self._trace(
            "send_kv_to_decode begin reqs=%s total_groups=%d remote_tp_rank=%d",
            list(pending_reqs),
            total_groups,
            meta.remote_tp_rank,
        )

        wait_tasks = [
            asyncio.create_task(self._wait_send_meta_ready(send_meta))
            for send_meta in pending_reqs.values()
        ]
        try:
            await asyncio.wait_for(
                asyncio.gather(*wait_tasks),
                timeout=envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT,
            )
            self._trace(
                "send_kv_to_decode ready reqs=%s transfer_ids=%s",
                list(pending_reqs),
                [send_meta.transfer_id for send_meta in pending_reqs.values()],
            )
        except Exception as exc:
            for task in wait_tasks:
                task.cancel()
            message = f"Timeout waiting for P side ready: {exc}"
            logger.warning(message)
            with self._layered_send_lock:
                for send_meta in pending_reqs.values():
                    state = self._layered_send_states.get(send_meta.transfer_id)
                    if state is not None:
                        state.fail(message)
            response = LayeredMooncakeXferResponse(
                status=MooncakeXferResponseStatus.FINISH,
                err_reqs=list(pending_reqs),
                err_msg=message,
            )
            await sock.send_multipart((identity, self._encoder.encode(response)))
            return

        for send_meta in pending_reqs.values():
            if not send_meta.need_send:
                self.resolve_need_send(send_meta, remote_tp_ranks)

        remote_session = f"{meta.remote_hostname}:{meta.remote_port}"

        for group_idx in range(total_groups):
            for send_meta in pending_reqs.values():
                send_meta.sending += 1

            try:
                self._trace(
                    "send_kv_to_decode waiting group=%d/%d reqs=%s",
                    group_idx,
                    total_groups,
                    list(pending_reqs),
                )
                await self._wait_group_ready(pending_reqs.values(), group_idx)
                self._trace(
                    "send_kv_to_decode group_ready group=%d/%d reqs=%s",
                    group_idx,
                    total_groups,
                    list(pending_reqs),
                )
                (
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                    err_reqs,
                    err_msg,
                    path_stats,
                    descriptor_paths,
                ) = await self._build_transfer_params_for_group(
                    ready_reqs=list(pending_reqs.items()),
                    agent_meta=meta,
                    local_regions=local_regions,
                    remote_regions=remote_regions,
                    group_idx=group_idx,
                )
                self._trace(
                    "send_kv_to_decode params group=%d desc=%d bytes=%d err_reqs=%s",
                    group_idx,
                    len(src_ptrs),
                    sum(lengths),
                    err_reqs,
                )
                err_req_set = set(err_reqs)
                ok_reqs = [req_id for req_id in pending_reqs if req_id not in err_req_set]
                if src_ptrs:
                    dispatch_ret = await self.sender_loop.run_in_executor(
                        self._sender_executor,
                        self._send_blocks,
                        remote_session,
                        src_ptrs,
                        dst_ptrs,
                        lengths,
                        descriptor_paths,
                    )
                    if dispatch_ret != 0:
                        transfer_err_msg = (
                            f"Mooncake transfer engine returned {dispatch_ret}"
                        )
                        err_msg = (
                            transfer_err_msg if err_msg is None else f"{err_msg}; {transfer_err_msg}"
                        )
                        err_reqs = list(err_reqs)
                        for req_id in ok_reqs:
                            err_reqs.append(req_id)
                            err_req_set.add(req_id)
                        ok_reqs = []
                self._trace(
                    "send_kv_to_decode response group=%d status=%s ok=%s err=%s",
                    group_idx,
                    (
                        "FINISH"
                        if group_idx + 1 == total_groups or not [
                            req_id for req_id in pending_reqs if req_id not in err_req_set
                        ]
                        else "CONTINUE"
                    ),
                    ok_reqs,
                    err_reqs,
                )
                remaining_req_ids = [
                    req_id for req_id in pending_reqs if req_id not in err_req_set
                ]
                response = LayeredMooncakeXferResponse(
                    status=(
                        MooncakeXferResponseStatus.FINISH
                        if group_idx + 1 == total_groups or not remaining_req_ids
                        else MooncakeXferResponseStatus.CONTINUE
                    ),
                    ok_reqs=ok_reqs or None,
                    err_reqs=err_reqs or None,
                    err_msg=err_msg,
                    group_index=group_idx,
                    total_groups=total_groups,
                )
                await sock.send_multipart((identity, self._encoder.encode(response)))
                if err_req_set:
                    with self._layered_send_lock:
                        for req_id in err_req_set:
                            transfer_id = meta.req_blocks[req_id][0]
                            state = self._layered_send_states.get(transfer_id)
                            if state is not None:
                                state.fail(err_msg or "layered transfer failed")
                    for req_id in list(err_req_set):
                        failed_meta = pending_reqs.pop(req_id, None)
                        if failed_meta is not None:
                            self.reqs_need_send.pop(failed_meta.transfer_id, None)
                            failed_req_id = (
                                str(failed_meta.p_req_id)
                                if getattr(failed_meta, "p_req_id", "")
                                else str(req_id)
                            )
                            self._send_request_routing_paths.pop(failed_req_id, None)
                            self._send_transfer_routing_paths.pop(
                                str(failed_meta.transfer_id), None
                            )
                            with self._layered_send_lock:
                                self._layered_send_states.pop(failed_meta.transfer_id, None)
                                self._current_send_transfer_ids.discard(str(failed_meta.transfer_id))
                    if not pending_reqs:
                        break
            finally:
                for send_meta in pending_reqs.values():
                    send_meta.sending = max(0, send_meta.sending - 1)

        for d_req_id, send_meta in pending_reqs.items():
            send_meta.sent += 1
            if (
                send_meta.sent == send_meta.need_send
                and self.reqs_need_send.pop(send_meta.transfer_id, None) is not None
            ):
                self.finished_sending_reqs.add(send_meta.p_req_id)
            self._send_request_routing_paths.pop(str(send_meta.p_req_id), None)
            self._send_transfer_routing_paths.pop(str(send_meta.transfer_id), None)
            with self._layered_send_lock:
                self._layered_send_states.pop(send_meta.transfer_id, None)
            logger.debug(
                "layered kv send finished for request %s transfer_id=%s",
                d_req_id,
                send_meta.transfer_id,
            )
        with self._layered_send_lock:
            for send_meta in pending_reqs.values():
                self._current_send_transfer_ids.discard(str(send_meta.transfer_id))

    async def _send_kv_to_decode_nonlayered(
        self,
        identity: bytes,
        sock,
        meta: MooncakeXferMetadata | LayeredMooncakeXferMetadata,
    ):
        from vllm import envs

        pending_reqs: dict[str, SendBlockMeta] = {}
        remote_tp_ranks = self.transfer_topo.handshake_target_ranks(meta.remote_tp_size)
        if meta.remote_tp_rank not in remote_tp_ranks:
            msg = (
                "This D tp_rank "
                f"{meta.remote_tp_rank} is not paired with P tp_rank "
                f"{self.tp_rank}; expected one of {remote_tp_ranks}."
            )
            logger.error(msg)
            response = MooncakeXferResponse(
                status=MooncakeXferResponseStatus.ERROR,
                err_msg=msg,
            )
            await sock.send_multipart((identity, self._encoder.encode(response)))
            return
        local_regions = self._get_transfer_regions(
            self.kv_caches_base_addr,
            self.block_len_per_layer,
        )
        remote_regions = self._get_transfer_regions(
            meta.kv_caches_base_addr,
            meta.block_lens,
        )
        validation_err = self._validate_regions(local_regions, remote_regions, meta)
        if validation_err is not None:
            response = MooncakeXferResponse(
                status=MooncakeXferResponseStatus.ERROR,
                err_msg=validation_err,
            )
            await sock.send_multipart((identity, self._encoder.encode(response)))
            return

        for d_req_id, (transfer_id, _) in meta.req_blocks.items():
            if transfer_id not in self.reqs_need_send:
                self.reqs_need_send[transfer_id] = SendBlockMeta(
                    p_req_id="",
                    transfer_id=transfer_id,
                    local_block_ids=[],
                    ready=asyncio.Event(),
                )
            pending_reqs[d_req_id] = self.reqs_need_send[transfer_id]

        async def wait_and_ret(
            d_req_id: str,
            send_meta: SendBlockMeta,
        ) -> tuple[str, SendBlockMeta]:
            await send_meta.ready.wait()
            return d_req_id, send_meta

        wait_tasks = [
            asyncio.create_task(wait_and_ret(d_req_id, send_meta))
            for d_req_id, send_meta in pending_reqs.items()
        ]

        while wait_tasks:
            done, pending = await asyncio.wait(
                wait_tasks,
                timeout=envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not done:
                for task in wait_tasks:
                    task.cancel()
                logger.warning(
                    "Timeout waiting for P side ready: %s", list(pending_reqs)
                )
                response = MooncakeXferResponse(
                    status=MooncakeXferResponseStatus.FINISH,
                    err_reqs=list(pending_reqs),
                    err_msg="Timeout waiting for P side ready.",
                )
                await sock.send_multipart((identity, self._encoder.encode(response)))
                for req_id in list(pending_reqs):
                    failed_meta = pending_reqs.get(req_id)
                    if failed_meta is None:
                        continue
                    failed_req_id = (
                        str(failed_meta.p_req_id)
                        if getattr(failed_meta, "p_req_id", "")
                        else str(req_id)
                    )
                    self._send_request_routing_paths.pop(failed_req_id, None)
                    self._send_transfer_routing_paths.pop(
                        str(failed_meta.transfer_id), None
                    )
                break

            wait_tasks = list(pending)
            response_status = (
                MooncakeXferResponseStatus.CONTINUE
                if wait_tasks
                else MooncakeXferResponseStatus.FINISH
            )
            ready_reqs: list[tuple[str, SendBlockMeta]] = []
            for task in done:
                d_req_id, send_meta = task.result()
                del pending_reqs[d_req_id]
                if send_meta.transfer_id in self.reqs_need_send:
                    send_meta.sending += 1
                    if not send_meta.need_send:
                        self.resolve_need_send(send_meta, remote_tp_ranks)
                    ready_reqs.append((d_req_id, send_meta))
                else:
                    logger.warning(
                        "Request %s expired before sending on P side.", d_req_id
                    )

            (
                src_ptrs,
                dst_ptrs,
                lengths,
                err_reqs,
                err_msg,
                _path_stats,
                descriptor_paths,
            ) = await self._build_transfer_params_with_path_stats(
                ready_reqs,
                meta,
                local_regions,
                remote_regions,
            )
            err_req_set = set(err_reqs)
            ok_ready_reqs = [
                (d_req_id, send_meta)
                for d_req_id, send_meta in ready_reqs
                if d_req_id not in err_req_set
            ]

            if src_ptrs:
                remote_session = f"{meta.remote_hostname}:{meta.remote_port}"
                ret_value = await self.sender_loop.run_in_executor(
                    self._sender_executor,
                    self._send_blocks,
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                    descriptor_paths,
                )

                if ret_value != 0:
                    transfer_err_msg = f"Mooncake transfer engine returned {ret_value}"
                    err_msg = (
                        transfer_err_msg
                        if err_msg is None
                        else f"{err_msg}; {transfer_err_msg}"
                    )
                    err_reqs = list(err_reqs)
                    for d_req_id, _ in ok_ready_reqs:
                        err_reqs.append(d_req_id)
                        err_req_set.add(d_req_id)
                    ok_ready_reqs = []

            for d_req_id, send_meta in ready_reqs:
                send_meta.sending = max(0, send_meta.sending - 1)

                if d_req_id in err_req_set:
                    failed_req_id = (
                        str(send_meta.p_req_id)
                        if getattr(send_meta, "p_req_id", "")
                        else str(d_req_id)
                    )
                    self._send_request_routing_paths.pop(failed_req_id, None)
                    self._send_transfer_routing_paths.pop(
                        str(send_meta.transfer_id), None
                    )
                    continue

                send_meta.sent += 1
                if (
                    send_meta.sent == send_meta.need_send
                    and self.reqs_need_send.pop(send_meta.transfer_id, None) is not None
                ):
                    self.finished_sending_reqs.add(send_meta.p_req_id)
                self._send_request_routing_paths.pop(str(send_meta.p_req_id), None)
                self._send_transfer_routing_paths.pop(
                    str(send_meta.transfer_id), None
                )

            response = MooncakeXferResponse(
                status=response_status,
                ok_reqs=[d_req_id for d_req_id, _ in ok_ready_reqs] or None,
                err_reqs=err_reqs or None,
                err_msg=err_msg,
            )
            await sock.send_multipart((identity, self._encoder.encode(response)))

    async def _wait_send_meta_ready(self, send_meta: SendBlockMeta) -> None:
        await send_meta.ready.wait()

    async def _wait_group_ready(
        self,
        send_metas: Iterable[SendBlockMeta],
        group_idx: int,
    ) -> None:
        from vllm import envs

        for send_meta in send_metas:
            state = self._ensure_layered_send_state(send_meta.transfer_id)
            wait_ok = await asyncio.wait_for(
                asyncio.to_thread(state.group_ready_events[group_idx].wait),
                timeout=envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT,
            )
            if not wait_ok:
                raise TimeoutError(
                    f"timed out waiting for group {group_idx} "
                    f"transfer_id={send_meta.transfer_id}"
                )
            if state.failed is not None:
                raise RuntimeError(state.failed)

    def _validate_regions(
        self,
        local_regions,
        remote_regions,
        meta: MooncakeXferMetadata | LayeredMooncakeXferMetadata,
    ) -> str | None:
        from vllm.distributed.kv_transfer.kv_connector.v1.mooncake.mooncake_connector import (
            _validate_asymmetric_region_lengths,
        )

        return _validate_asymmetric_region_lengths(
            local_regions=local_regions,
            remote_regions=remote_regions,
            local_tp_size=self.tp_size,
            remote_tp_size=meta.remote_tp_size,
            producer_cache_replicated=self._producer_cache_is_replicated(),
        )

    async def _build_transfer_params_for_group(
        self,
        *,
        ready_reqs: list[tuple[str, SendBlockMeta]],
        agent_meta: MooncakeXferMetadata | LayeredMooncakeXferMetadata,
        local_regions: list[Any],
        remote_regions: list[Any],
        group_idx: int,
    ) -> tuple[
        list[int],
        list[int],
        list[int],
        list[str],
        str | None,
        dict[str, tuple[int, int]],
        list[str],
    ]:
        if not self._group_region_slices:
            return await self._build_transfer_params_with_path_stats(
                ready_reqs,
                agent_meta,
                local_regions,
                remote_regions,
            )
        group_slice = self._group_region_slices[min(group_idx, len(self._group_region_slices) - 1)]
        sliced_local = local_regions[group_slice.start : group_slice.stop]
        sliced_remote = remote_regions[group_slice.start : group_slice.stop]
        return await self._build_transfer_params_with_path_stats(
            ready_reqs,
            agent_meta,
            sliced_local,
            sliced_remote,
        )

    async def _build_transfer_params_with_path_stats(
        self,
        ready_reqs: list[tuple[str, SendBlockMeta]],
        agent_meta: MooncakeXferMetadata | LayeredMooncakeXferMetadata,
        local_regions: list[Any],
        remote_regions: list[Any],
    ) -> tuple[
        list[int],
        list[int],
        list[int],
        list[str],
        str | None,
        dict[str, tuple[int, int]],
        list[str],
    ]:
        build_started = time.perf_counter()
        from vllm.distributed.kv_transfer.kv_connector.v1.mooncake.mooncake_connector import (
            _can_coalesce_block_transfers,
            group_concurrent_contiguous,
        )

        src_ptrs: list[int] = []
        dst_ptrs: list[int] = []
        lengths: list[int] = []
        err_reqs: list[str] = []
        err_msg: str | None = None
        descriptor_paths: list[str] = []
        coalesce_keys: list[tuple[Any, ...]] = []

        for d_req_id, send_meta in ready_reqs:
            _, remote_block_ids_per_group = agent_meta.req_blocks[d_req_id]
            if not remote_block_ids_per_group or all(
                len(group) == 0 for group in remote_block_ids_per_group
            ):
                continue

            local_block_ids: list[int] = []
            remote_block_ids: list[int] = []
            has_block_error = False
            if len(send_meta.local_block_ids) != len(remote_block_ids_per_group):
                logger.error(
                    "req %s: KV group count mismatch: local=%d, remote=%d",
                    d_req_id,
                    len(send_meta.local_block_ids),
                    len(remote_block_ids_per_group),
                )
                err_reqs.append(d_req_id)
                if err_msg is None:
                    err_msg = "KV group count mismatch"
                continue

            for local_group, remote_group in zip(
                send_meta.local_block_ids, remote_block_ids_per_group
            ):
                n_local = len(local_group)
                n_remote = len(remote_group)
                if n_local < n_remote:
                    logger.error(
                        "req %s: local blocks(%d) < remote blocks(%d) in a KV cache group",
                        d_req_id,
                        n_local,
                        n_remote,
                    )
                    has_block_error = True
                    break
                if n_local > n_remote:
                    local_group = local_group[-n_remote:]
                local_block_ids.extend(local_group)
                remote_block_ids.extend(remote_group)

            if has_block_error:
                err_reqs.append(d_req_id)
                if err_msg is None:
                    err_msg = "P num blocks less than D"
                continue
            if not local_block_ids:
                continue

            group_local_block_ids, group_remote_block_ids = group_concurrent_contiguous(
                local_block_ids, remote_block_ids
            )
            routing_path = self._routing_path_for_send(
                req_id=d_req_id,
                transfer_id=send_meta.transfer_id,
            )

            for region_idx, (local_region, remote_region) in enumerate(
                zip(local_regions, remote_regions)
            ):
                should_transfer, src_region_offset, dst_region_offset, transfer_len = (
                    self._get_sender_transfer_plan(
                        local_kv_block_len=local_region.kv_block_len,
                        remote_kv_block_len=remote_region.kv_block_len,
                        remote_tp_rank=agent_meta.remote_tp_rank,
                        remote_tp_size=agent_meta.remote_tp_size,
                    )
                )
                if not should_transfer:
                    continue

                assert src_region_offset + transfer_len <= local_region.kv_block_len
                assert dst_region_offset + transfer_len <= remote_region.kv_block_len
                can_coalesce = _can_coalesce_block_transfers(
                    local_region_block_len=local_region.block_len,
                    remote_region_block_len=remote_region.block_len,
                    src_region_offset=src_region_offset,
                    dst_region_offset=dst_region_offset,
                    transfer_len=transfer_len,
                )
                provenance_key = (
                    int(region_idx),
                    int(local_region.base_addr),
                    int(local_region.block_len),
                    int(local_region.kv_block_len),
                    int(remote_region.base_addr),
                    int(remote_region.block_len),
                    int(remote_region.kv_block_len),
                    int(src_region_offset),
                    int(dst_region_offset),
                    int(transfer_len),
                )

                for group_local_block_id, group_remote_block_id in zip(
                    group_local_block_ids, group_remote_block_ids
                ):
                    if can_coalesce:
                        transfer_size = transfer_len * len(group_local_block_id)
                        src_ptrs.append(
                            local_region.base_addr
                            + group_local_block_id[0] * local_region.block_len
                            + src_region_offset
                        )
                        dst_ptrs.append(
                            remote_region.base_addr
                            + group_remote_block_id[0] * remote_region.block_len
                            + dst_region_offset
                        )
                        lengths.append(transfer_size)
                        descriptor_paths.append(routing_path)
                        coalesce_keys.append(provenance_key)
                    else:
                        for local_block_id, remote_block_id in zip(
                            group_local_block_id,
                            group_remote_block_id,
                        ):
                            src_ptrs.append(
                                local_region.base_addr
                                + local_block_id * local_region.block_len
                                + src_region_offset
                            )
                            dst_ptrs.append(
                                remote_region.base_addr
                                + remote_block_id * remote_region.block_len
                                + dst_region_offset
                            )
                            lengths.append(transfer_len)
                            descriptor_paths.append(routing_path)
                            coalesce_keys.append(provenance_key)

        input_descriptors = len(src_ptrs)
        coalesced = coalesce_transfer_descriptors(
            src_ptrs,
            dst_ptrs,
            lengths,
            coalesce_keys=(
                coalesce_keys
                if getattr(self, "enable_descriptor_coalescing", False)
                else None
            ),
            descriptor_paths=descriptor_paths,
        )
        src_ptrs = coalesced.src_ptrs
        dst_ptrs = coalesced.dst_ptrs
        lengths = coalesced.lengths
        descriptor_paths = coalesced.descriptor_paths
        path_stats = self._path_stats_from_descriptor_paths(descriptor_paths, lengths)
        self._accumulate_worker_meta(
            LayeredTransferWorkerMeta(
                descriptor_build_calls=1,
                descriptor_build_input_descriptors=input_descriptors,
                descriptor_build_output_descriptors=len(src_ptrs),
                coalesced_descriptors=coalesced.coalesced_descriptors,
                descriptor_build_ms=(time.perf_counter() - build_started) * 1000.0,
            )
        )

        return src_ptrs, dst_ptrs, lengths, err_reqs, err_msg, path_stats, descriptor_paths

    @staticmethod
    def _path_stats_from_descriptor_paths(
        descriptor_paths: list[str] | tuple[str, ...] | None,
        lengths: list[int],
    ) -> dict[str, tuple[int, int]]:
        if not descriptor_paths or len(descriptor_paths) != len(lengths):
            return {}
        aggregated: dict[str, tuple[int, int]] = {}
        for path, size in zip(descriptor_paths, lengths):
            bucket = EPDMooncakeConnectorWorker._normalize_routing_path(path)
            prev_descs, prev_bytes = aggregated.get(bucket, (0, 0))
            aggregated[bucket] = (prev_descs + 1, prev_bytes + int(size))
        return aggregated

    def _path_deltas_for_batch(
        self,
        path_stats: dict[str, tuple[int, int]] | None,
        *,
        backend_label: str,
        duration_ms: float = 0.0,
        success: bool = True,
    ) -> dict[str, LayeredTransferWorkerMeta]:
        deltas: dict[str, LayeredTransferWorkerMeta] = {}
        total_path_bytes = sum(
            max(0, int(stats[1]))
            for stats in dict(path_stats or {}).values()
        )
        path_count = sum(
            1
            for stats in dict(path_stats or {}).values()
            if int(stats[0]) > 0 or int(stats[1]) > 0
        )
        for path, stats in dict(path_stats or {}).items():
            desc_count, byte_count = int(stats[0]), int(stats[1])
            if desc_count <= 0 and byte_count <= 0:
                continue
            if total_path_bytes > 0:
                path_duration_ms = max(0.0, float(duration_ms)) * (
                    max(0, byte_count) / total_path_bytes
                )
            else:
                path_duration_ms = max(0.0, float(duration_ms)) / max(1, path_count)
            grouped_delta = (
                LayeredTransferWorkerMeta(
                    grouped_batches=1,
                    grouped_bytes=byte_count,
                    grouped_descriptors=desc_count,
                )
                if success
                else LayeredTransferWorkerMeta()
            )
            deltas[self._normalize_routing_path(path)] = grouped_delta.aggregate(
                self._transfer_backend_delta(
                    backend_label,
                    total_bytes=byte_count,
                    duration_ms=path_duration_ms,
                    success=success,
                )
            )
        return deltas

    def _get_peer_transfer_engine(self) -> PeerTransferEngine:
        engine = self._peer_transfer_engine
        if engine is None:
            engine = PeerTransferEngine(protocol=self.mooncake_protocol)
            engine.bind_mooncake_backend(self.engine, initialized=True, owns_backend=False)
            self._peer_transfer_engine = engine
        return engine

    def _get_rdmacm_transfer_engine(self) -> PeerTransferEngine:
        engine = self._rdmacm_transfer_engine
        if engine is None:
            engine = PeerTransferEngine(protocol="rdmacm")
            engine.initialize()
            self._rdmacm_transfer_engine = engine
        return engine

    def _rdmacm_port(self, mooncake_rpc_port: int) -> int:
        # Keep the endpoint deterministic from the Mooncake-advertised port so
        # no additional control-plane field is required. Wrap inside the
        # non-privileged port range rather than overflowing at high ephemeral
        # Mooncake ports.
        return 1024 + (
            (int(mooncake_rpc_port) - 1024 + int(self.rdmacm_port_offset))
            % (65535 - 1024)
        )

    def _rdmacm_remote_session(self, mooncake_remote_session: str) -> str:
        hostname, separator, raw_port = str(mooncake_remote_session).rpartition(":")
        if not separator or not hostname:
            raise ValueError(
                f"invalid Mooncake remote session for rdmacm: {mooncake_remote_session!r}"
            )
        address = self.rdmacm_remote_address or hostname
        return f"{address}:{self._rdmacm_port(int(raw_port))}"

    def _transfer_region_descriptors_via_rdmacm(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
    ) -> int:
        engine = self._get_rdmacm_transfer_engine()
        rdmacm_session = self._rdmacm_remote_session(remote_session)
        self._trace(
            "rdmacm staged plan remote=%s desc=%d bytes=%d",
            rdmacm_session,
            len(src_ptrs),
            sum(int(value) for value in lengths),
        )
        plan = engine.build_pointer_transfer_plan(
            remote_session=rdmacm_session,
            local_pointers=src_ptrs,
            remote_pointers=dst_ptrs,
            lengths=lengths,
            registered=True,
        )
        engine.transfer_peer_buffer_plan(plan)
        return 0

    def _transfer_region_descriptors_via_peer_engine(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
    ) -> int:
        total_bytes = sum(int(length) for length in lengths)
        self._trace(
            "peer_buffer plan remote=%s desc=%d bytes=%d",
            remote_session,
            len(src_ptrs),
            total_bytes,
        )
        engine = self._get_peer_transfer_engine()
        plan = engine.build_pointer_transfer_plan(
            remote_session=remote_session,
            local_pointers=src_ptrs,
            remote_pointers=dst_ptrs,
            lengths=lengths,
            registered=True,
        )
        engine.transfer_peer_buffer_plan(plan)
        return 0

    def _batched_transfer_regions(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
    ) -> _TransferDispatchResult:
        backend = self.transport_backend.strip().lower()
        if backend in {"rdmacm", "rdmacm_staged", "iwarp"}:
            try:
                ret_code = self._transfer_region_descriptors_via_rdmacm(
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                )
                return _TransferDispatchResult(
                    ret_code=ret_code,
                    backend_label="rdmacm_staged",
                )
            except Exception as exc:
                message = str(exc)
                if not getattr(self, "allow_transfer_fallback", True):
                    logger.exception(
                        "rdmacm staged transfer failed and fallback is disabled"
                    )
                    return _TransferDispatchResult(
                        ret_code=-1,
                        backend_label="rdmacm_staged",
                        error_message=message,
                    )
                logger.exception(
                    "rdmacm staged transfer failed; falling back to Mooncake TCP"
                )
                ret_code = self.engine.batch_transfer_sync_write(
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                )
                return _TransferDispatchResult(
                    ret_code=ret_code,
                    backend_label="rdmacm_to_tcp_fallback",
                    used_fallback=True,
                    error_message=message,
                )
        if backend in {"mooncake_engine_direct", "engine_direct", "direct_engine"}:
            try:
                ret_code = self._transfer_region_descriptors_via_peer_engine(
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                )
                self._trace(
                    "peer_buffer committed remote=%s desc=%d bytes=%d",
                    remote_session,
                    len(src_ptrs),
                    sum(lengths),
                )
                return _TransferDispatchResult(
                    ret_code=ret_code,
                    backend_label="peer_buffer_direct",
                )
            except Exception as exc:
                message = str(exc)
                if not getattr(self, "allow_transfer_fallback", True):
                    logger.exception("peer-buffer transfer path failed and fallback is disabled")
                    return _TransferDispatchResult(
                        ret_code=-1,
                        backend_label="peer_buffer_direct",
                        error_message=message,
                    )
                logger.exception(
                    "peer-buffer transfer path failed; falling back to raw batch_transfer_sync_write"
                )
                self._trace(
                    "peer_buffer fallback remote=%s desc=%d bytes=%d err=%s",
                    remote_session,
                    len(src_ptrs),
                    sum(lengths),
                    message,
                )
                ret_code = self.engine.batch_transfer_sync_write(
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                )
                return _TransferDispatchResult(
                    ret_code=ret_code,
                    backend_label="batch_transfer_fallback",
                    used_fallback=True,
                    error_message=message,
                )
        ret_code = self.engine.batch_transfer_sync_write(
            remote_session,
            src_ptrs,
            dst_ptrs,
            lengths,
        )
        return _TransferDispatchResult(
            ret_code=ret_code,
            backend_label="batch_transfer_native",
        )

    def _transfer_backend_delta(
        self,
        backend_label: str,
        *,
        total_bytes: int,
        duration_ms: float = 0.0,
        success: bool = True,
    ) -> LayeredTransferWorkerMeta:
        duration_ms = max(0.0, float(duration_ms))
        total_bytes = max(0, int(total_bytes))
        if not success:
            return LayeredTransferWorkerMeta(
                transfer_attempts=1,
                transfer_attempt_elapsed_ms=duration_ms,
                backend_failures={backend_label: 1},
            )
        delta = LayeredTransferWorkerMeta(
            transfer_attempts=1,
            transfer_successes=1,
            transfer_bytes=total_bytes,
            transfer_elapsed_ms=duration_ms,
            transfer_attempt_elapsed_ms=duration_ms,
            backend_counts={backend_label: 1},
            backend_bytes={backend_label: total_bytes},
            backend_elapsed_ms={backend_label: duration_ms},
        )
        if backend_label == "peer_buffer_direct":
            delta.peer_buffer_batches = 1
            delta.peer_buffer_bytes = total_bytes
        elif backend_label in {
            "batch_transfer_fallback",
            "rdmacm_to_tcp_fallback",
        }:
            delta.fallback_batches = 1
            delta.fallback_bytes = total_bytes
        return delta

    def _uses_direct_peer_backend(self) -> bool:
        return self.transport_backend.strip().lower() in {
            "mooncake_engine_direct",
            "engine_direct",
            "direct_engine",
            "rdmacm",
            "rdmacm_staged",
            "iwarp",
        }

    # ------------------------------------------------------------------
    # Direct send helpers and observability
    # ------------------------------------------------------------------
    def _send_blocks(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
        descriptor_paths: list[str] | None = None,
    ) -> int:
        if not src_ptrs:
            return 0

        full_path_stats = self._path_stats_from_descriptor_paths(descriptor_paths, lengths)
        single_batch_desc_limit = max(1, int(getattr(self, "max_transfer_descriptors", 128)))
        single_batch_byte_limit = max(0, int(getattr(self, "max_transfer_bytes", 0)))
        must_chunk_for_transport = (
            len(src_ptrs) > single_batch_desc_limit
            or (single_batch_byte_limit > 0 and sum(int(v) for v in lengths) > single_batch_byte_limit)
        )
        if not must_chunk_for_transport:
            if self.layered_kv_transfer and len(src_ptrs) > 1:
                return self._send_region_group_with_retry(
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                    full_path_stats,
                    descriptor_paths,
                ).ret_code
            return self._send_region_group(
                remote_session,
                src_ptrs,
                dst_ptrs,
                lengths,
                path_stats=full_path_stats,
            )

        # Layered callers already pass one completed layer-group slice.  The
        # previous code divided that slice again by the total registered layer
        # count, multiplying synchronous dispatches.  Only transport safety
        # limits should split this already-scoped descriptor batch.
        descriptors_per_group = single_batch_desc_limit
        effective_max_group_bytes = self.max_group_bytes
        if single_batch_byte_limit > 0:
            effective_max_group_bytes = (
                min(effective_max_group_bytes, single_batch_byte_limit)
                if effective_max_group_bytes > 0
                else single_batch_byte_limit
            )
        groups = chunk_transfer_descriptors(
            src_ptrs,
            dst_ptrs,
            lengths,
            descriptors_per_group=descriptors_per_group,
            max_group_bytes=effective_max_group_bytes,
        )
        logger.info(
            "EPD Mooncake grouped transfer: remote=%s groups=%d descriptors=%d layers_per_group=%d max_group_bytes=%d max_transfer_descriptors=%d max_transfer_bytes=%d delay_ms=%.3f",
            remote_session,
            len(groups),
            len(src_ptrs),
            self.layers_per_group,
            effective_max_group_bytes,
            single_batch_desc_limit,
            single_batch_byte_limit,
            self.group_delay_ms,
        )

        path_cursor = 0
        for group_idx, (src_group, dst_group, len_group) in enumerate(groups):
            group_paths: list[str] | None = None
            if descriptor_paths is not None:
                next_cursor = path_cursor + len(src_group)
                group_paths = descriptor_paths[path_cursor:next_cursor]
                path_cursor = next_cursor
            group_path_stats = self._path_stats_from_descriptor_paths(group_paths, len_group)
            dispatch = self._send_region_group_with_retry(
                remote_session,
                src_group,
                dst_group,
                len_group,
                group_path_stats,
                group_paths,
            )
            total_bytes = sum(len_group)
            if dispatch.ret_code != 0:
                logger.warning(
                    "Grouped Mooncake transfer failed remote=%s group=%d/%d ret=%s bytes=%d desc=%d",
                    remote_session,
                    group_idx + 1,
                    len(groups),
                    dispatch.ret_code,
                    total_bytes,
                    len(src_group),
                )
                return dispatch.ret_code
            if self.group_delay_ms > 0 and group_idx + 1 < len(groups):
                self._accumulate_worker_meta(
                    LayeredTransferWorkerMeta(
                        accumulated_group_delay_ms=self.group_delay_ms
                    )
                )
                time.sleep(self.group_delay_ms / 1000.0)
            self._publish_connector_metrics()
        return 0


    def _send_region_group_with_retry(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
        path_stats: dict[str, tuple[int, int]] | None = None,
        descriptor_paths: list[str] | None = None,
        *,
        attempt: int = 0,
    ) -> _TransferDispatchResult:
        max_attempts = max(0, int(getattr(self, "transfer_retry_attempts", 0)))
        has_retry_budget = len(src_ptrs) > 1 and attempt < max_attempts
        dispatch = self._send_region_group_dispatch(
            remote_session,
            src_ptrs,
            dst_ptrs,
            lengths,
            path_stats,
            record_failure_metrics=False,
        )
        if dispatch.ret_code == 0:
            return dispatch

        retryable = is_retryable_transfer_failure(
            dispatch.ret_code,
            dispatch.error_message,
        )
        can_retry = has_retry_budget and retryable
        if not can_retry:
            # The speculative dispatch above suppressed terminal failure
            # accounting because retryability was not known until it returned.
            self._record_terminal_transfer_failure(
                dispatch,
                path_stats=path_stats,
            )
            if has_retry_budget and not retryable:
                logger.warning(
                    "Not retrying non-retryable Mooncake transfer remote=%s ret=%s desc=%d err=%s",
                    remote_session,
                    dispatch.ret_code,
                    len(src_ptrs),
                    dispatch.error_message or "",
                )
            return dispatch

        backoff_ms = max(0.0, float(getattr(self, "transfer_retry_backoff_ms", 0.0)))
        if backoff_ms > 0:
            # Mooncake's TCP transport can transiently fail under descriptor bursts
            # because each batch may open many short-lived connections.  A small
            # bounded backoff lets the transport release sockets before retrying
            # smaller sub-batches.  Re-sending the same KV bytes is idempotent.
            time.sleep((backoff_ms * (2 ** attempt)) / 1000.0)

        mid = max(1, len(src_ptrs) // 2)
        self._trace(
            "retrying Mooncake transfer remote=%s attempt=%d desc=%d split=%d/%d ret=%s",
            remote_session,
            attempt + 1,
            len(src_ptrs),
            mid,
            len(src_ptrs) - mid,
            dispatch.ret_code,
        )
        logger.warning(
            "Retrying Mooncake transfer remote=%s attempt=%d desc=%d bytes=%d split=%d/%d after ret=%s",
            remote_session,
            attempt + 1,
            len(src_ptrs),
            sum(lengths),
            mid,
            len(src_ptrs) - mid,
            dispatch.ret_code,
        )

        left_paths = descriptor_paths[:mid] if descriptor_paths is not None else None
        right_paths = descriptor_paths[mid:] if descriptor_paths is not None else None
        left = self._send_region_group_with_retry(
            remote_session,
            src_ptrs[:mid],
            dst_ptrs[:mid],
            lengths[:mid],
            self._path_stats_from_descriptor_paths(left_paths, lengths[:mid]),
            left_paths,
            attempt=attempt + 1,
        )
        if left.ret_code != 0:
            return left
        right = self._send_region_group_with_retry(
            remote_session,
            src_ptrs[mid:],
            dst_ptrs[mid:],
            lengths[mid:],
            self._path_stats_from_descriptor_paths(right_paths, lengths[mid:]),
            right_paths,
            attempt=attempt + 1,
        )
        if right.ret_code != 0:
            return right
        return _TransferDispatchResult(
            ret_code=0,
            backend_label=f"{left.backend_label}+retry_split",
            used_fallback=left.used_fallback or right.used_fallback,
        )

    def _record_terminal_transfer_failure(
        self,
        dispatch: _TransferDispatchResult,
        *,
        path_stats: dict[str, tuple[int, int]] | None,
    ) -> None:
        failure_delta = LayeredTransferWorkerMeta(failed_batches=1)
        self._accumulate_worker_meta(failure_delta)
        path_failure_deltas = {
            self._normalize_routing_path(path): LayeredTransferWorkerMeta(
                failed_batches=1
            )
            for path, stats in dict(path_stats or {}).items()
            if int(stats[0]) > 0 or int(stats[1]) > 0
        }
        if path_failure_deltas:
            self._accumulate_worker_meta_by_path(path_failure_deltas)
        self.xfer_stats.record_failed_transfer()
        self._publish_connector_metrics()

    def _send_region_group_dispatch(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
        path_stats: dict[str, tuple[int, int]] | None = None,
        *,
        record_failure_metrics: bool = True,
    ) -> _TransferDispatchResult:
        start_time = time.perf_counter()
        dispatch = self._batched_transfer_regions(remote_session, src_ptrs, dst_ptrs, lengths)
        duration = time.perf_counter() - start_time
        total_bytes = sum(lengths)
        if dispatch.ret_code == 0:
            delta = LayeredTransferWorkerMeta(
                grouped_batches=1,
                grouped_bytes=total_bytes,
                grouped_descriptors=len(src_ptrs),
            ).aggregate(
                self._transfer_backend_delta(
                    dispatch.backend_label,
                    total_bytes=total_bytes,
                    duration_ms=duration * 1000.0,
                )
            )
            self._accumulate_worker_meta(delta)
            path_deltas = self._path_deltas_for_batch(
                path_stats,
                backend_label=dispatch.backend_label,
                duration_ms=duration * 1000.0,
            )
            if path_deltas:
                self._accumulate_worker_meta_by_path(path_deltas)
            self._publish_connector_metrics()
            self.xfer_stats.record_transfer(
                duration_s=duration,
                total_bytes=total_bytes,
                num_descs=len(src_ptrs),
            )
            logger.debug(
                "Layered Mooncake group send done remote=%s bytes=%d desc=%d path=%s group_delay_ms=%.3f",
                remote_session,
                total_bytes,
                len(src_ptrs),
                dispatch.backend_label,
                self.group_delay_ms,
            )
        else:
            failure_delta = self._transfer_backend_delta(
                dispatch.backend_label,
                total_bytes=total_bytes,
                duration_ms=duration * 1000.0,
                success=False,
            )
            if record_failure_metrics:
                failure_delta = failure_delta.aggregate(
                    LayeredTransferWorkerMeta(failed_batches=1)
                )
                self.xfer_stats.record_failed_transfer()
            self._accumulate_worker_meta(failure_delta)
            path_failure_deltas = self._path_deltas_for_batch(
                path_stats,
                backend_label=dispatch.backend_label,
                duration_ms=duration * 1000.0,
                success=False,
            )
            if record_failure_metrics:
                path_failure_deltas = {
                    path: delta.aggregate(LayeredTransferWorkerMeta(failed_batches=1))
                    for path, delta in path_failure_deltas.items()
                }
            if path_failure_deltas:
                self._accumulate_worker_meta_by_path(path_failure_deltas)
            self._publish_connector_metrics()
            logger.warning(
                "Layered Mooncake group send failed remote=%s ret=%s bytes=%d desc=%d path=%s",
                remote_session,
                dispatch.ret_code,
                total_bytes,
                len(src_ptrs),
                dispatch.backend_label,
            )
        return dispatch

    def _send_region_group(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
        path_stats: dict[str, tuple[int, int]] | None = None,
    ) -> int:
        return self._send_region_group_dispatch(
            remote_session,
            src_ptrs,
            dst_ptrs,
            lengths,
            path_stats,
        ).ret_code
