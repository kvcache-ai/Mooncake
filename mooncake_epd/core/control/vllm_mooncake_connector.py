"""Repo-local MooncakeConnector wrapper for vLLM serving.

This module keeps vLLM upstream Mooncake's real data plane but extends it in
three production-facing directions:

1. restore router-facing decode handoff params on producer completion;
2. drive *real* layer/group-aware KV transfer from vLLM's
   ``wait_for_layer_load`` / ``save_kv_layer`` hooks;
3. expose grouped-transfer worker metadata for observability.

When a Decode pull arrives while Prefill is still executing, the layered path
below avoids waiting for every request in that pull batch before it can send
the first ready KV group. The producer:

- learns request block ids during ``update_state_after_alloc``;
- marks layer groups ready from ``save_kv_layer`` as the forward progresses;
- transfers only the regions that correspond to the finished group; and
- notifies the consumer after each group so ``wait_for_layer_load`` can unblock
  the matching attention layers.
"""

from __future__ import annotations

import asyncio
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable, Mapping

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
)
from .connector_metrics import ConnectorMetricsSink
from ..transfer import (
    TransferEngine as PeerTransferEngine,
    require_mooncake_protocol_support,
)
from ..state.kv_transfer_manifest_v2 import (
    KVTransferLayoutV2,
    KVTransferManifestError,
    KVTransferManifestV2,
)

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
    dispatch_ms: float = 0.0
    prepare_ms: float = 0.0
    write_ms: float = 0.0
    native_engine_lock_wait_ms: float = 0.0


@dataclass
class _EPDPullReqMeta(PullReqMeta):
    """Declared EPD extension of vLLM's Decode pull metadata.

    vLLM's upstream ``PullReqMeta`` intentionally contains only the fields it
    needs for its native Mooncake protocol.  A P->D manifest is an EPD control
    envelope, not a data-plane pointer, so keeping it as a declared dataclass
    field lets it follow the pull request through scheduler/worker transports
    without changing the upstream wire format.  It also gives the worker a
    request-local source of truth when an older executor strips metadata
    sidecars while rebuilding connector metadata.
    """

    epd_kv_transfer_manifest_v2: dict[str, Any] | None = None


@dataclass
class _LayeredSendState:
    transfer_id: str
    total_groups: int
    group_ready_events: list[threading.Event]
    # A vLLM attention invocation only enqueues the writes to its KV cache
    # before it calls ``save_kv_layer``.  The transfer engine runs from a
    # different host thread, therefore the thread event alone is not enough to
    # prove that a source region is readable.  Keep the CUDA event that fences
    # each logical layer group alongside the host-side readiness event.
    source_ready_events: list[Any | None]
    announced_groups: set[int] = field(default_factory=set)
    failed: str | None = None
    highest_announced_group: int = -1
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    # ``save_kv_layer`` may run outside the sender loop.  Keep a loop-local
    # async event mirror so ready callbacks wake coroutines directly instead of
    # consuming a default-executor thread for every pending request/group.
    _async_group_events: dict[int, tuple[asyncio.AbstractEventLoop, list[asyncio.Event]]] = field(
        default_factory=dict,
        repr=False,
    )

    @classmethod
    def create(cls, transfer_id: str, total_groups: int) -> "_LayeredSendState":
        return cls(
            transfer_id=transfer_id,
            total_groups=max(1, int(total_groups)),
            group_ready_events=[threading.Event() for _ in range(max(1, int(total_groups)))],
            source_ready_events=[None for _ in range(max(1, int(total_groups)))],
        )

    def mark_group_ready(
        self,
        group_idx: int,
        *,
        source_ready_event: Any | None = None,
    ) -> None:
        with self._lock:
            if group_idx < 0 or group_idx >= len(self.group_ready_events):
                return
            if (
                source_ready_event is not None
                and self.source_ready_events[group_idx] is None
            ):
                self.source_ready_events[group_idx] = source_ready_event
            if not self.group_ready_events[group_idx].is_set():
                self.group_ready_events[group_idx].set()
                self._wake_async_group_locked(group_idx)
            self.announced_groups.add(group_idx)
            self.highest_announced_group = max(self.highest_announced_group, group_idx)

    def mark_groups_ready_through(
        self,
        group_idx: int,
        *,
        floor: int = 0,
        source_ready_event: Any | None = None,
    ) -> None:
        """Mark a contiguous ready range and wake async waiters once per group."""

        with self._lock:
            stop = min(len(self.group_ready_events) - 1, int(group_idx))
            start = max(0, int(floor), self.highest_announced_group + 1)
            if stop < start:
                return
            for ready_group_idx in range(start, stop + 1):
                if (
                    source_ready_event is not None
                    and self.source_ready_events[ready_group_idx] is None
                ):
                    self.source_ready_events[ready_group_idx] = source_ready_event
                self.group_ready_events[ready_group_idx].set()
                self.announced_groups.add(ready_group_idx)
                self._wake_async_group_locked(ready_group_idx)
            self.highest_announced_group = max(self.highest_announced_group, stop)

    def source_ready_event(self, group_idx: int) -> Any | None:
        """Return the source-completion fence for one logical KV group."""

        with self._lock:
            if group_idx < 0 or group_idx >= len(self.source_ready_events):
                return None
            return self.source_ready_events[group_idx]

    def async_group_event(
        self,
        group_idx: int,
        loop: asyncio.AbstractEventLoop,
    ) -> asyncio.Event:
        """Return a sender-loop event mirrored from the thread-safe state."""

        if group_idx < 0 or group_idx >= len(self.group_ready_events):
            raise IndexError(f"layered KV group out of range: {group_idx}")
        with self._lock:
            loop_key = id(loop)
            entry = self._async_group_events.get(loop_key)
            if entry is None or entry[0] is not loop:
                entry = (loop, [asyncio.Event() for _ in self.group_ready_events])
                self._async_group_events[loop_key] = entry
            event = entry[1][group_idx]
            # This method is invoked by the owning sender loop, so setting the
            # event inline is safe and avoids a needless loop turn for already
            # completed Prefill requests.
            if self.failed is not None or self.group_ready_events[group_idx].is_set():
                event.set()
            return event

    def _wake_async_group_locked(self, group_idx: int) -> None:
        stale: list[int] = []
        for loop_key, (loop, events) in self._async_group_events.items():
            if loop.is_closed():
                stale.append(loop_key)
                continue
            if 0 <= group_idx < len(events):
                loop.call_soon_threadsafe(events[group_idx].set)
        for loop_key in stale:
            self._async_group_events.pop(loop_key, None)

    def _wake_all_async_locked(self) -> None:
        stale: list[int] = []
        for loop_key, (loop, events) in self._async_group_events.items():
            if loop.is_closed():
                stale.append(loop_key)
                continue
            for event in events:
                loop.call_soon_threadsafe(event.set)
        for loop_key in stale:
            self._async_group_events.pop(loop_key, None)

    def fail(self, message: str) -> None:
        with self._lock:
            self.failed = str(message)
            for event in self.group_ready_events:
                event.set()
            self._wake_all_async_locked()


@dataclass
class _LayeredReceiveState:
    request_id: str
    transfer_id: str
    total_groups: int
    expected_tasks: int
    group_events: list[threading.Event]
    remaining_by_group: list[int]
    failure: str | None = None
    started_at: float | None = None
    first_group_at: float | None = None
    finished_at: float | None = None

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

    def mark_started(self, now: float | None = None) -> bool:
        if self.started_at is not None:
            return False
        self.started_at = time.perf_counter() if now is None else float(now)
        return True

    def ack_group(self, group_idx: int, now: float | None = None) -> bool:
        if self.failure is not None:
            return False
        if group_idx < 0 or group_idx >= self.total_groups:
            return False
        was_set = self.group_events[group_idx].is_set()
        self.remaining_by_group[group_idx] = max(0, self.remaining_by_group[group_idx] - 1)
        if self.remaining_by_group[group_idx] == 0:
            self.group_events[group_idx].set()
            if not was_set:
                event_time = time.perf_counter() if now is None else float(now)
                if self.first_group_at is None:
                    self.first_group_at = event_time
                    return True
        return False

    def mark_finished_if_complete(self, now: float | None = None) -> bool:
        if self.finished_at is not None or not self.complete():
            return False
        self.finished_at = time.perf_counter() if now is None else float(now)
        return True

    def first_group_latency_ms(self) -> float:
        if self.started_at is None or self.first_group_at is None:
            return 0.0
        return max(0.0, (self.first_group_at - self.started_at) * 1000.0)

    def finished_latency_ms(self) -> float:
        if self.started_at is None or self.finished_at is None:
            return 0.0
        return max(0.0, (self.finished_at - self.started_at) * 1000.0)

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
    # The regular vLLM Mooncake metadata has no extension field.  Keep the
    # checksummed P->D envelope in the repo-local layered side-channel so the
    # producer can fence its own generation immediately before it uses remote
    # pointer descriptors.  Keys are Decode request IDs, matching req_blocks.
    kv_transfer_manifests: dict[str, dict[str, Any]] | None = None


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
        self.worker_id = str(
            extra.get("worker_id")
            or os.getenv("MOONCAKE_EPD_WORKER_ID")
            or self.engine_id
        )
        self.worker_generation = str(
            extra.get("worker_generation")
            or os.getenv("MOONCAKE_EPD_WORKER_GENERATION")
            or f"{self.engine_id}:{uuid.uuid4().hex}"
        )
        self.require_kv_transfer_manifest_v2 = self._as_enabled(
            extra.get(
                "require_kv_transfer_manifest_v2",
                os.getenv("MOONCAKE_EPD_STRICT", "0"),
            )
        )
        self.require_kv_transfer_manifest_generation = self._as_enabled(
            extra.get(
                "require_kv_transfer_manifest_generation",
                os.getenv("MOONCAKE_EPD_STRICT", "0"),
            )
        )
        self.require_kv_transfer_manifest_compatibility = self._as_enabled(
            extra.get(
                "require_kv_transfer_manifest_compatibility",
                os.getenv("MOONCAKE_EPD_REQUIRE_KV_TRANSFER_MANIFEST_COMPATIBILITY", "0"),
            )
        )
        self.kv_transfer_layout_v2 = self._build_kv_transfer_layout(vllm_config)
        # vLLM's upstream Mooncake scheduler consumes ``do_remote_prefill``
        # by mutating the request's KV parameters to ``False`` immediately
        # after it records the pull.  EPD must retain the original control
        # envelope until ``build_connector_meta`` serializes the pull for the
        # worker; otherwise the V2 manifest is silently omitted from P->D.
        self._recv_kv_transfer_params_by_request: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _as_enabled(value: Any) -> bool:
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _first_config_value(*values: Any, default: Any = "unknown") -> Any:
        for value in values:
            if value is not None and str(value).strip() not in {"", "None"}:
                return value
        return default

    @classmethod
    def _build_kv_transfer_layout(cls, vllm_config: VllmConfig) -> dict[str, Any]:
        """Capture a serializable source layout without relying on raw pointers."""

        model_config = getattr(vllm_config, "model_config", None)
        cache_config = getattr(vllm_config, "cache_config", None)
        parallel_config = getattr(vllm_config, "parallel_config", None)
        hf_config = getattr(model_config, "hf_config", None)
        text_config = getattr(hf_config, "text_config", None)
        num_layers = 0
        for candidate in (model_config, hf_config, text_config):
            for name in ("num_hidden_layers", "num_layers"):
                value = getattr(candidate, name, None)
                try:
                    if value is not None and int(value) > 0:
                        num_layers = int(value)
                        break
                except (TypeError, ValueError):
                    continue
            if num_layers:
                break
        try:
            block_size = int(getattr(cache_config, "block_size", 0) or 0)
        except (TypeError, ValueError):
            block_size = 0
        try:
            tp_size = int(getattr(parallel_config, "tensor_parallel_size", 1) or 1)
        except (TypeError, ValueError):
            tp_size = 1
        layout = KVTransferLayoutV2(
            model_id=str(
                cls._first_config_value(
                    getattr(model_config, "model", None),
                    getattr(model_config, "model_name", None),
                )
            ),
            model_revision=str(cls._first_config_value(getattr(model_config, "revision", None))),
            vllm_adapter_version=str(
                os.getenv("MOONCAKE_EPD_VLLM_ADAPTER_VERSION", "mooncake-epd-v1")
            ),
            dtype=str(cls._first_config_value(getattr(model_config, "dtype", None))),
            block_size=block_size,
            num_layers=num_layers,
            tp_size=max(1, tp_size),
            tp_rank=0,
            extra={"connector": "MooncakeConnector", "layout_source": "vllm_config"},
        )
        return layout.as_control_payload()

    def _source_manifest_hints(self) -> dict[str, Any]:
        return {
            "source_worker_id": self.worker_id,
            "source_generation": self.worker_generation,
            "kv_layout_v2": dict(self.kv_transfer_layout_v2),
        }

    def _validate_kv_transfer_manifest_params(self, params: Mapping[str, Any]) -> None:
        """Fail before a Decode worker receives stale/misrouted P->D KV."""

        raw_manifest = params.get("kv_transfer_manifest_v2")
        if raw_manifest is None:
            if self.require_kv_transfer_manifest_v2:
                raise RuntimeError("P->D KV transfer manifest v2 is required")
            return
        try:
            manifest = KVTransferManifestV2.from_control_payload(raw_manifest)
            source_generation = params.get("source_generation")
            if source_generation is not None and not str(source_generation).strip():
                source_generation = None
            destination_generation: str | None = None
            if str(manifest.destination_generation).strip().lower() not in {
                "",
                "unknown",
                "none",
                "null",
            }:
                destination_generation = self.worker_generation
            manifest.validate_for_consumer(
                transfer_id=str(params.get("transfer_id") or ""),
                workflow_id=str(params.get("workflow_id") or ""),
                source_engine_id=str(params.get("remote_engine_id") or ""),
                source_generation=(str(source_generation) if source_generation is not None else None),
                destination_engine_id=self.worker_id,
                destination_generation=destination_generation,
                require_generation_fences=self.require_kv_transfer_manifest_generation,
                require_compatibility=self.require_kv_transfer_manifest_compatibility,
            )
            if self.require_kv_transfer_manifest_compatibility:
                local_layout = KVTransferLayoutV2.from_control_payload(
                    self.kv_transfer_layout_v2
                )
                if manifest.layout_checksum != local_layout.checksum:
                    raise KVTransferManifestError(
                        "KV transfer layout does not match this Decode worker"
                    )
        except (KVTransferManifestError, TypeError, ValueError) as exc:
            raise RuntimeError(f"P->D KV transfer manifest rejected: {exc}") from exc

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
        is_remote_prefill = bool(params.get("do_remote_prefill"))
        if is_remote_prefill:
            # Validate before upstream consumes the one-shot marker.  This
            # makes a malformed strict P->D request fail at admission rather
            # than later in the Decode worker after a partial scheduler step.
            self._validate_kv_transfer_manifest_params(params)
        result = super().update_state_after_alloc(request, blocks, num_external_tokens)
        if is_remote_prefill and request.request_id in self._reqs_need_recv:
            # Preserve the unmodified params separately.  The upstream call
            # above intentionally flips request.kv_transfer_params[
            # "do_remote_prefill"] to False to prevent a second pull, but
            # the request is also the object stored in _reqs_need_recv.
            # Re-reading it in build_connector_meta would therefore lose the
            # manifest and all other EPD-only fields.
            self._recv_kv_transfer_params_by_request[str(request.request_id)] = dict(params)
            recv_request, local_block_ids = self._reqs_need_recv[request.request_id]
            clipped_block_ids = self._clip_remote_prefill_recv_blocks(params, local_block_ids)
            self._reqs_need_recv[request.request_id] = (recv_request, clipped_block_ids)
        elif is_remote_prefill:
            # Do not retain an envelope when the upstream connector rejected
            # the pull parameters and did not create a receive entry.
            self._recv_kv_transfer_params_by_request.pop(str(request.request_id), None)
        if (
            self.layered_kv_transfer
            and params.get("do_remote_decode")
            and not self.is_kv_consumer
            and hasattr(blocks, "get_unhashed_block_ids_all_groups")
        ):
            local_block_ids = self.get_sw_clipped_blocks(
                blocks.get_unhashed_block_ids_all_groups()
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
            if params.get("do_remote_decode"):
                upstream_params = {
                    **dict(upstream_params),
                    **{
                        key: value
                        for key, value in self._source_manifest_hints().items()
                        if key not in upstream_params
                    },
                }
            return delay_free_blocks, upstream_params
        if not params or not params.get("transfer_id"):
            return delay_free_blocks, None
        if params.get("do_remote_prefill"):
            return delay_free_blocks, None
        if not params.get("do_remote_decode"):
            return delay_free_blocks, None
        if getattr(request, "status", None) != RequestStatus.FINISHED_LENGTH_CAPPED:
            return delay_free_blocks, None
        result = {
            "do_remote_prefill": True,
            "do_remote_decode": False,
            "remote_block_ids": self.get_sw_clipped_blocks(block_ids),
            "remote_engine_id": self.engine_id,
            "remote_bootstrap_addr": self.remote_bootstrap_addr,
            "tp_size": self.tp_size,
            "transfer_id": params["transfer_id"],
        }
        result.update(self._source_manifest_hints())
        return delay_free_blocks, result

    def build_connector_meta(self, scheduler_output) -> KVConnectorMetadata:
        del scheduler_output
        meta = MooncakeConnectorMetadata()
        request_routing_paths: dict[str, str] = {}
        transfer_routing_paths: dict[str, str] = {}
        # ``MooncakeConnectorMetadata`` is a normal Python object transported
        # from the Scheduler to the Worker by vLLM.  An explicit sidecar keeps
        # V2 manifests out of upstream ``PullReqMeta`` while preserving them
        # until the Worker constructs its ZMQ request to the producer.
        kv_transfer_manifests_by_request: dict[str, dict[str, Any]] = {}

        if not self.is_kv_producer:
            for req_id, (req, block_ids) in self._reqs_need_recv.items():
                assert req.kv_transfer_params is not None
                saved_recv_params = getattr(
                    self, "_recv_kv_transfer_params_by_request", {}
                ).pop(str(req_id), None)
                # See update_state_after_alloc: use the captured one-shot
                # envelope rather than the request object after upstream has
                # consumed do_remote_prefill.
                params = dict(saved_recv_params or req.kv_transfer_params)
                manifest_payload: dict[str, Any] | None = None
                if params.get("do_remote_prefill"):
                    self._validate_kv_transfer_manifest_params(params)
                    raw_manifest = params.get("kv_transfer_manifest_v2")
                    if raw_manifest is not None:
                        if not isinstance(raw_manifest, Mapping):
                            raise RuntimeError(
                                "P->D KV transfer manifest must be an object"
                            )
                        manifest_payload = dict(raw_manifest)
                        kv_transfer_manifests_by_request[str(req_id)] = manifest_payload
                meta.add_new_req(
                    request_id=req_id,
                    local_block_ids=block_ids,
                    kv_transfer_params=params,
                )
                if manifest_payload is not None:
                    # Keep the manifest on the concrete pull request as well as
                    # in the metadata sidecar.  Some vLLM executor paths rebuild
                    # metadata objects, but retain the request entries they must
                    # execute.  A declared dataclass field therefore preserves
                    # the control envelope without relying on an undocumented
                    # dynamic attribute on the outer metadata object.
                    remote_engine_id = str(params["remote_engine_id"])
                    upstream_pull_meta = meta.reqs_to_recv[remote_engine_id][req_id]
                    meta.reqs_to_recv[remote_engine_id][req_id] = _EPDPullReqMeta(
                        d_req_id=upstream_pull_meta.d_req_id,
                        transfer_id=upstream_pull_meta.transfer_id,
                        local_block_ids=upstream_pull_meta.local_block_ids,
                        remote_engine_id=upstream_pull_meta.remote_engine_id,
                        remote_bootstrap_addr=upstream_pull_meta.remote_bootstrap_addr,
                        expire_time=upstream_pull_meta.expire_time,
                        pull_tasks_count=upstream_pull_meta.pull_tasks_count,
                        epd_kv_transfer_manifest_v2=manifest_payload,
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
        # Do not attach an empty field: older worker code can ignore the
        # attribute, while strict current workers fail closed when a pull
        # actually lacks a required manifest.
        if kv_transfer_manifests_by_request:
            meta.epd_kv_transfer_manifests_v2 = kv_transfer_manifests_by_request
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
        requested_protocol = protocol or str(
            os.getenv("MOONCAKE_PROTOCOL", "tcp")
        ).strip().lower()
        # Upstream Mooncake initializes its native engine inside ``super``.
        # Validate before that call: older engine.so builds otherwise log an
        # nvlink_intra error and silently auto-select RDMA/TCP.
        require_mooncake_protocol_support(requested_protocol)
        if requested_protocol == "nvlink_intra":
            os.environ.pop("MC_FORCE_TCP", None)
            os.environ.setdefault("MC_INTRANODE_NVLINK", "1")
        force_tcp_transport = extra.get("force_tcp_transport", True)
        if protocol == "tcp" and force_tcp_transport:
            os.environ.setdefault("MC_FORCE_TCP", "1")
        super().__init__(vllm_config, engine_id, kv_cache_config)
        self.layered_kv_transfer = bool(extra.get("layered_kv_transfer", False))
        self.mooncake_protocol = requested_protocol
        # Scheduler and Worker run in different processes.  Both therefore
        # read the deployment-provided incarnation fence rather than deriving
        # an in-process random value.  In strict mode a missing/mismatched
        # fence is a hard error before the producer dereferences a descriptor.
        self.worker_id = str(
            extra.get("worker_id")
            or os.getenv("MOONCAKE_EPD_WORKER_ID")
            or self.engine_id
        )
        self.worker_generation = str(
            extra.get("worker_generation")
            or os.getenv("MOONCAKE_EPD_WORKER_GENERATION")
            or f"{self.engine_id}:{uuid.uuid4().hex}"
        )
        self.require_kv_transfer_manifest_v2 = (
            EPDMooncakeConnectorScheduler._as_enabled(
                extra.get(
                    "require_kv_transfer_manifest_v2",
                    os.getenv("MOONCAKE_EPD_STRICT", "0"),
                )
            )
        )
        self.require_kv_transfer_manifest_generation = (
            EPDMooncakeConnectorScheduler._as_enabled(
                extra.get(
                    "require_kv_transfer_manifest_generation",
                    os.getenv("MOONCAKE_EPD_STRICT", "0"),
                )
            )
        )
        self.require_kv_transfer_manifest_compatibility = (
            EPDMooncakeConnectorScheduler._as_enabled(
                extra.get(
                    "require_kv_transfer_manifest_compatibility",
                    os.getenv(
                        "MOONCAKE_EPD_REQUIRE_KV_TRANSFER_MANIFEST_COMPATIBILITY",
                        "0",
                    ),
                )
            )
        )
        self.kv_transfer_layout_v2 = EPDMooncakeConnectorScheduler._build_kv_transfer_layout(
            vllm_config
        )
        self.layers_per_group = max(1, int(extra.get("layers_per_group", 4)))
        self.group_delay_ms = max(0.0, float(extra.get("group_delay_ms", 0.0)))
        self.max_group_bytes = max(0, int(extra.get("max_group_bytes", 0) or 0))
        self.max_transfer_descriptors = max(
            1,
            int(
                extra.get(
                    "max_transfer_descriptors",
                    os.getenv("MOONCAKE_EPD_MAX_TRANSFER_DESCRIPTORS", 128),
                )
                or 128
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
        self.transport_backend = str(extra.get("transport_backend", "mooncake_engine_direct"))
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
        connector_metrics_flush_interval_ms = max(
            0.0,
            float(
                extra.get(
                    "connector_metrics_flush_interval_ms",
                    os.getenv(
                        "MOONCAKE_EPD_CONNECTOR_METRICS_FLUSH_INTERVAL_MS",
                        250.0,
                    ),
                )
                or 0.0
            ),
        )
        self._connector_metrics_flush_interval_s = (
            connector_metrics_flush_interval_ms / 1000.0
        )
        # Do not move every layered-KV group through the cross-process metrics
        # sink. The counters remain in a bounded in-memory aggregate and are
        # persisted on a timer or at a terminal request boundary.
        self._connector_metrics_next_publish_monotonic = 0.0
        self._worker_meta = LayeredTransferWorkerMeta()
        self._connector_metrics_pending = LayeredTransferWorkerMeta()
        self._connector_metrics_pending_by_path: dict[str, LayeredTransferWorkerMeta] = {}
        self._connector_metrics_lock = threading.RLock()
        self._connector_metrics_sink = ConnectorMetricsSink(
            connector_metrics_dir or None,
            engine_id=self.engine_id,
            role="producer" if self.is_kv_producer else "consumer",
            hostname=getattr(self, "hostname", ""),
            rpc_port=getattr(self, "rpc_port", None),
            tp_rank=getattr(self, "tp_rank", None),
            flush_interval_s=self._connector_metrics_flush_interval_s,
        )
        self._peer_transfer_engine: PeerTransferEngine | None = None
        # The producer's sender executor can run several ready layer groups in
        # parallel.  A source-ready CUDA event proves that a group's KV bytes
        # have been written *at that instant*, not that the scheduler will keep
        # those physical blocks unchanged while the task waits behind another
        # dispatch.  Keep the event fence and the corresponding native transfer
        # in one small critical section.  This mirrors the verified single
        # sender-worker semantics while retaining parallel metadata waits and
        # request/group construction outside the lock.
        self._layered_source_to_send_lock = threading.Lock()
        self._registered_region_count = 1
        self._send_request_routing_paths: dict[str, str] = {}
        self._send_transfer_routing_paths: dict[str, str] = {}
        self._recv_request_routing_paths: dict[str, str] = {}
        self._recv_transfer_routing_paths: dict[str, str] = {}
        # Small, payload-free sidecar populated from Scheduler metadata.  It
        # is consumed only to create the ZMQ control message and never treats
        # raw pointers as durable state.
        self._recv_kv_transfer_manifests_by_request: dict[str, dict[str, Any]] = {}
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
                # vLLM reaps a finished receive state later through
                # get_finished(). Until then it must not be polled once per
                # Decode layer/token. Failed states remain visible so the
                # failure still aborts the forward path.
                and (
                    self._layered_recv_states[req_id].failure is not None
                    or self._layered_recv_states[req_id].finished_at is None
                )
            ]
        return states

    def _ensure_layered_send_state(self, transfer_id: str) -> _LayeredSendState:
        with self._layered_send_lock:
            state = self._layered_send_states.get(transfer_id)
            if state is None or state.total_groups != self._sender_group_count:
                state = _LayeredSendState.create(transfer_id, self._sender_group_count)
                self._layered_send_states[transfer_id] = state
            return state

    def _active_layered_send_states(self) -> list[_LayeredSendState]:
        """Snapshot states belonging to the forward currently saving KV.

        ``record_send_reqs`` runs asynchronously on the sender loop, whereas
        ``save_kv_layer`` runs in the model-forward thread.  The latter must
        never fall back to every outstanding transfer: a metadata update for a
        later request can otherwise publish stale KV for an earlier one.
        """

        with self._layered_send_lock:
            active_transfer_ids = set(
                getattr(self, "_current_send_transfer_ids", set()) or set()
            )
            if not active_transfer_ids:
                return []
            return [
                state
                for transfer_id, state in self._layered_send_states.items()
                if transfer_id in active_transfer_ids
            ]

    def _has_active_layered_send_scope(self) -> bool:
        with self._layered_send_lock:
            return bool(
                getattr(self, "_current_send_transfer_ids", set()) or set()
            )

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

    def _mark_group_ready(
        self,
        group_idx: int,
        *,
        source_ready_event: Any | None = None,
    ) -> int:
        states = self._active_layered_send_states()
        for state in states:
            state.mark_group_ready(group_idx, source_ready_event=source_ready_event)
        return len(states)

    def _mark_ready_groups_up_to(
        self,
        group_idx: int,
        *,
        source_ready_event: Any | None = None,
    ) -> int:
        states = self._active_layered_send_states()
        for state in states:
            # Each state remembers its highest completed group. This preserves
            # the conservative earlier-group guarantee while avoiding the
            # previous O(groups^2 * active_transfers) repeated event scans.
            state.mark_groups_ready_through(
                group_idx,
                source_ready_event=source_ready_event,
            )
        return len(states)

    @staticmethod
    def _cuda_device_for_kv_layer(kv_layer: Any) -> Any | None:
        """Best-effort CUDA-device extraction without coupling to cache shape."""

        candidates: Iterable[Any]
        if isinstance(kv_layer, (list, tuple)):
            candidates = kv_layer
        else:
            candidates = (kv_layer,)
        for candidate in candidates:
            device = getattr(candidate, "device", None)
            if getattr(device, "type", None) == "cuda":
                return device
        return None

    def _record_source_ready_event(self, kv_layer: Any) -> Any | None:
        """Fence a KV source group without globally synchronizing the GPU.

        vLLM invokes ``save_kv_layer`` after the attention callable returns,
        which guarantees only enqueue order.  Recording a CUDA event on that
        stream gives the sender thread a request-local completion fence.  If
        event creation itself fails, the conservative device synchronize is a
        correctness fallback rather than silently allowing a stale read.
        """

        device = self._cuda_device_for_kv_layer(kv_layer)
        if device is None:
            return None
        try:
            import torch

            with torch.cuda.device(device):
                event = torch.cuda.Event(
                    enable_timing=False,
                    blocking=False,
                    interprocess=False,
                )
                event.record(torch.cuda.current_stream(device=device))
            return event
        except Exception as exc:
            try:
                import torch

                with torch.cuda.device(device):
                    torch.cuda.synchronize(device=device)
            except Exception as sync_exc:
                raise RuntimeError(
                    "failed to establish a source-KV completion fence"
                ) from sync_exc
            logger.warning(
                "EPD layered KV could not record a CUDA source-ready event; "
                "used a conservative device synchronize instead: %s",
                exc,
            )
            self._accumulate_worker_meta(
                LayeredTransferWorkerMeta(source_ready_sync_fallbacks=1)
            )
            return None

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

    def _record_recv_kv_transfer_manifests(
        self,
        metadata: MooncakeConnectorMetadata,
    ) -> None:
        """Bind Scheduler-side V2 envelopes to Worker pull metadata.

        Upstream ``PullReqMeta`` deliberately exposes only its stable fields.
        We retain the EPD extension in a metadata sidecar and attach a copy in
        this Worker process, so it crosses the later Decode->Prefill ZMQ hop in
        ``LayeredMooncakeXferMetadata`` without requiring a vLLM fork.
        """

        raw_by_request = getattr(metadata, "epd_kv_transfer_manifests_v2", None)
        if raw_by_request is None:
            raw_by_request = {}
        if not isinstance(raw_by_request, Mapping):
            raise RuntimeError("P->D KV transfer manifest sidecar must be an object")

        known_request_ids = {
            str(req_id)
            for pull_metas in metadata.reqs_to_recv.values()
            for req_id in pull_metas
        }
        unknown_request_ids = sorted(
            str(req_id) for req_id in raw_by_request if str(req_id) not in known_request_ids
        )
        if unknown_request_ids:
            raise RuntimeError(
                "P->D KV transfer manifest sidecar contains unknown requests: "
                + ", ".join(unknown_request_ids[:8])
            )

        self._trace(
            "consumer manifest metadata reqs=%s sidecar_keys=%s metadata_keys=%s",
            sorted(known_request_ids),
            sorted(str(req_id) for req_id in raw_by_request),
            sorted(getattr(metadata, "__dict__", {}).keys()),
        )

        for pull_metas in metadata.reqs_to_recv.values():
            for req_id, pull_meta in pull_metas.items():
                request_key = str(req_id)
                embedded_manifest = getattr(
                    pull_meta,
                    "epd_kv_transfer_manifest_v2",
                    None,
                )
                sidecar_manifest = raw_by_request.get(request_key)
                if (
                    embedded_manifest is not None
                    and sidecar_manifest is not None
                    and embedded_manifest != sidecar_manifest
                ):
                    raise RuntimeError(
                        "P->D KV transfer manifest mismatch between pull metadata "
                        f"and sidecar for request={request_key}"
                    )
                raw_manifest = (
                    embedded_manifest
                    if embedded_manifest is not None
                    else sidecar_manifest
                )
                if raw_manifest is None:
                    if bool(getattr(self, "require_kv_transfer_manifest_v2", False)):
                        raise RuntimeError(
                            "P->D KV transfer manifest v2 is required in Worker metadata "
                            f"for request={request_key}"
                        )
                    continue
                if not isinstance(raw_manifest, Mapping):
                    raise RuntimeError(
                        "P->D KV transfer manifest must be an object "
                        f"for request={request_key}"
                    )
                copied = dict(raw_manifest)
                # PullReqMeta is a normal dataclass in the supported vLLM
                # version.  The dynamic attribute remains local to this Worker
                # process; the typed ZMQ envelope below is the wire contract.
                setattr(pull_meta, "epd_kv_transfer_manifest_v2", copied)
                self._recv_kv_transfer_manifests_by_request[request_key] = copied

    def _validated_kv_transfer_manifests_for_pull_metas(
        self,
        pull_metas: Mapping[str, PullReqMeta],
    ) -> dict[str, dict[str, Any]]:
        """Return fresh consumer-validated manifests keyed by Decode request.

        The Decode Scheduler validates before allocation, but the lease can
        expire while the Worker waits for Prefill bootstrap or a ZMQ peer.  A
        second check immediately before emitting pointer descriptors avoids a
        time-of-check/time-of-use hole and gives the producer a trusted envelope
        to validate against its own generation.
        """

        payloads: dict[str, dict[str, Any]] = {}
        for req_id, pull_meta in pull_metas.items():
            request_key = str(req_id)
            raw_manifest = getattr(pull_meta, "epd_kv_transfer_manifest_v2", None)
            if raw_manifest is None:
                raw_manifest = self._recv_kv_transfer_manifests_by_request.get(request_key)
            if raw_manifest is None:
                if bool(getattr(self, "require_kv_transfer_manifest_v2", False)):
                    raise RuntimeError(
                        "P->D KV transfer manifest v2 is required before Decode sends "
                        f"pointer descriptors for request={request_key}"
                    )
                continue
            try:
                manifest = KVTransferManifestV2.from_control_payload(raw_manifest)
                manifest.validate_for_consumer(
                    transfer_id=str(pull_meta.transfer_id),
                    source_engine_id=str(pull_meta.remote_engine_id),
                    destination_engine_id=(
                        self.worker_id
                        if bool(getattr(self, "require_kv_transfer_manifest_v2", False))
                        else None
                    ),
                    destination_generation=(
                        self.worker_generation
                        if bool(
                            getattr(self, "require_kv_transfer_manifest_generation", False)
                        )
                        else None
                    ),
                    require_generation_fences=bool(
                        getattr(self, "require_kv_transfer_manifest_generation", False)
                    ),
                    require_compatibility=bool(
                        getattr(self, "require_kv_transfer_manifest_compatibility", False)
                    ),
                )
                if bool(getattr(self, "require_kv_transfer_manifest_compatibility", False)):
                    local_layout = KVTransferLayoutV2.from_control_payload(
                        self.kv_transfer_layout_v2
                    )
                    if manifest.layout_checksum != local_layout.checksum:
                        raise KVTransferManifestError(
                            "KV transfer layout does not match this Decode Worker"
                        )
            except (KVTransferManifestError, TypeError, ValueError) as exc:
                raise RuntimeError(
                    "P->D KV transfer manifest rejected by Decode Worker "
                    f"for request={request_key}: {exc}"
                ) from exc
            payloads[request_key] = manifest.as_control_payload()
        return payloads

    def _validate_kv_transfer_manifests_for_producer(
        self,
        meta: LayeredMooncakeXferMetadata,
    ) -> None:
        """Fence a ZMQ pull against the current Prefill worker incarnation."""

        raw_by_request = meta.kv_transfer_manifests or {}
        if not isinstance(raw_by_request, Mapping):
            raise RuntimeError("P->D KV transfer manifests must be an object")
        expected_request_ids = {str(req_id) for req_id in meta.req_blocks}
        unknown_request_ids = sorted(
            str(req_id) for req_id in raw_by_request if str(req_id) not in expected_request_ids
        )
        if unknown_request_ids:
            raise RuntimeError(
                "P->D KV transfer manifests contain unknown requests: "
                + ", ".join(unknown_request_ids[:8])
            )

        for request_key, (transfer_id, _) in meta.req_blocks.items():
            request_id = str(request_key)
            raw_manifest = raw_by_request.get(request_id)
            if raw_manifest is None:
                if bool(getattr(self, "require_kv_transfer_manifest_v2", False)):
                    raise RuntimeError(
                        "P->D KV transfer manifest v2 is required before Prefill sends "
                        f"KV for request={request_id}"
                    )
                continue
            try:
                manifest = KVTransferManifestV2.from_control_payload(raw_manifest)
                manifest.validate_for_producer(
                    transfer_id=str(transfer_id),
                    source_engine_id=str(self.engine_id),
                    source_generation=(
                        self.worker_generation
                        if bool(
                            getattr(self, "require_kv_transfer_manifest_generation", False)
                        )
                        else None
                    ),
                    require_generation_fences=bool(
                        getattr(self, "require_kv_transfer_manifest_generation", False)
                    ),
                    require_compatibility=bool(
                        getattr(self, "require_kv_transfer_manifest_compatibility", False)
                    ),
                )
                if bool(getattr(self, "require_kv_transfer_manifest_compatibility", False)):
                    local_layout = KVTransferLayoutV2.from_control_payload(
                        self.kv_transfer_layout_v2
                    )
                    if manifest.layout_checksum != local_layout.checksum:
                        raise KVTransferManifestError(
                            "KV transfer layout does not match this Prefill Worker"
                        )
            except (KVTransferManifestError, TypeError, ValueError) as exc:
                raise RuntimeError(
                    "P->D KV transfer manifest rejected by Prefill Worker "
                    f"for request={request_id}: {exc}"
                ) from exc

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

    def start_load_kv(self, metadata: MooncakeConnectorMetadata):
        if self.layered_kv_transfer and not self.is_kv_producer and metadata.reqs_to_recv:
            self._record_recv_routing_paths(metadata)
            self._record_recv_kv_transfer_manifests(metadata)
            # Fail before Decode schedules a forward pass if Scheduler->Worker
            # metadata lost, tampered with, or expired the V2 envelope.
            for pull_metas in metadata.reqs_to_recv.values():
                self._validated_kv_transfer_manifests_for_pull_metas(pull_metas)
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

    def wait_for_layer_load(self, layer_name: str) -> None:
        if not self.layered_kv_transfer or self.is_kv_producer:
            return
        group_idx = self._group_for_layer(layer_name)
        if group_idx is None:
            return
        states = list(self._iter_current_recv_states())
        if not states:
            return
        start_time = time.perf_counter()
        self._trace(
            "wait_for_layer_load layer=%s group=%d active_recv=%s",
            layer_name,
            group_idx,
            list(self._current_recv_req_ids),
        )
        try:
            for state in states:
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
        del attn_metadata, kwargs
        if not self.layered_kv_transfer or self.is_kv_consumer:
            return
        group_idx = self._group_for_layer(layer_name)
        if group_idx is None:
            return
        if not self._is_group_tail_layer(layer_name):
            return
        if not self._has_active_layered_send_scope():
            # ``record_send_reqs`` is asynchronous and can observe unrelated
            # scheduler metadata while this worker performs a no-send forward.
            # Failing closed here is essential: publishing all outstanding
            # states was able to copy another request's uninitialized KV.
            self._trace(
                "save_kv_layer ignored tail layer=%s group=%d without active send scope",
                layer_name,
                group_idx,
            )
            return
        source_ready_event = self._record_source_ready_event(kv_layer)
        # vLLM's save_kv_layer callback is layer-scoped rather than
        # transfer_id-scoped.  Under chunked/prefill scheduling the first tail
        # callback observed by the connector can be a later layer group.  Marking
        # all earlier groups ready is conservative for waiters and prevents a
        # later-layer callback from leaving group-0 forever blocked.  The sender
        # still waits for request block metadata and the per-group CUDA source
        # completion fence before issuing any transfer.
        announced_states = self._mark_ready_groups_up_to(
            group_idx,
            source_ready_event=source_ready_event,
        )
        self._trace(
            "save_kv_layer tail layer=%s group=%d announced_up_to=%d states=%d source_event=%s",
            layer_name,
            group_idx,
            group_idx,
            announced_states,
            source_ready_event is not None,
        )

    def wait_for_save(self):
        if self.layered_kv_transfer and not self.is_kv_consumer:
            with self._layered_send_lock:
                completed_scope = sorted(self._current_send_transfer_ids)
                self._current_send_transfer_ids.clear()
            if completed_scope:
                self._trace(
                    "producer wait_for_save cleared send scope=%s",
                    completed_scope,
                )
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
            self._publish_connector_metrics(force=True)
        return finished_sending, finished_recving

    def build_connector_worker_meta(self):
        with self._connector_metrics_guard():
            meta = self._worker_meta
            self._worker_meta = LayeredTransferWorkerMeta()
        self._publish_connector_metrics(force=True)
        if meta.is_empty():
            return None
        return meta

    @staticmethod
    def _normalize_routing_path(path: str | None) -> str:
        normalized = str(path or "UNKNOWN").strip().upper()
        return normalized or "UNKNOWN"

    def _connector_metrics_guard(self) -> threading.RLock:
        lock = getattr(self, "_connector_metrics_lock", None)
        if lock is None:
            lock = threading.RLock()
            self._connector_metrics_lock = lock
        return lock

    def _accumulate_worker_meta(self, delta: LayeredTransferWorkerMeta | None) -> None:
        if delta is None or delta.is_empty():
            return
        with self._connector_metrics_guard():
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
        with self._connector_metrics_guard():
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
        if not bool(getattr(sink, "enabled", True)):
            return
        interval_s = max(
            0.0,
            float(getattr(self, "_connector_metrics_flush_interval_s", 0.0) or 0.0),
        )
        now = time.monotonic()
        if (
            not force
            and interval_s > 0.0
            and now < float(
                getattr(self, "_connector_metrics_next_publish_monotonic", 0.0)
                or 0.0
            )
        ):
            return
        with self._connector_metrics_guard():
            pending = getattr(self, "_connector_metrics_pending", None)
            pending_by_path = dict(
                getattr(self, "_connector_metrics_pending_by_path", {}) or {}
            )
            has_path_pending = any(not meta.is_empty() for meta in pending_by_path.values())
            if (pending is None or pending.is_empty()) and not has_path_pending:
                if force:
                    sink.flush(force=True)
                return
            self._connector_metrics_pending = LayeredTransferWorkerMeta()
            self._connector_metrics_pending_by_path = {}
        sink.record(pending, path_totals=pending_by_path, force=force)
        if interval_s > 0.0 and not force:
            self._connector_metrics_next_publish_monotonic = (
                time.monotonic() + interval_s
            )

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
            started_paths: dict[str, LayeredTransferWorkerMeta] = {}
            with self._layered_recv_lock:
                now = time.perf_counter()
                for req_id in pull_metas:
                    state = self._layered_recv_states.get(req_id)
                    if state is not None:
                        state.mark_started(now)
                    path = self._routing_path_for_recv(req_id=req_id)
                    existing = started_paths.get(path, LayeredTransferWorkerMeta())
                    started_paths[path] = existing.aggregate(
                        LayeredTransferWorkerMeta(receive_kv_requests=1)
                    )
            self._accumulate_worker_meta(
                LayeredTransferWorkerMeta(receive_kv_requests=len(pull_metas))
            )
            self._accumulate_worker_meta_by_path(started_paths)
            self._publish_connector_metrics()
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
        kv_transfer_manifests = self._validated_kv_transfer_manifests_for_pull_metas(
            pull_metas
        )
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
            kv_transfer_manifests=kv_transfer_manifests or None,
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

        roundtrip_start = time.perf_counter()
        first_response_ms: float | None = None
        last_response_ms: float | None = None
        response_messages = 0
        processed_response_messages = 0
        response_process_ms = 0.0
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
                    response_arrival_ms = (time.perf_counter() - roundtrip_start) * 1000.0
                    if first_response_ms is None:
                        first_response_ms = response_arrival_ms
                    last_response_ms = response_arrival_ms
                    response_messages += 1
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
                    process_start = time.perf_counter()
                    self.process_pulling_result(response, pull_metas)
                    response_process_ms += (time.perf_counter() - process_start) * 1000.0
                    processed_response_messages += 1
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
        finally:
            elapsed_ms = (time.perf_counter() - roundtrip_start) * 1000.0
            delta = LayeredTransferWorkerMeta(
                receive_kv_worker_roundtrips=1,
                receive_kv_worker_ms=elapsed_ms,
                receive_kv_response_messages=response_messages,
                receive_kv_first_response_count=1 if first_response_ms is not None else 0,
                receive_kv_first_response_ms=float(first_response_ms or 0.0),
                receive_kv_last_response_count=1 if last_response_ms is not None else 0,
                receive_kv_last_response_ms=float(last_response_ms or 0.0),
                receive_kv_response_process_count=processed_response_messages,
                receive_kv_response_process_ms=response_process_ms,
            )
            path_deltas: dict[str, LayeredTransferWorkerMeta] = {}
            for req_id in req_ids:
                path = self._routing_path_for_recv(req_id=req_id)
                existing = path_deltas.get(path, LayeredTransferWorkerMeta())
                if existing.receive_kv_worker_roundtrips == 0:
                    existing = existing.aggregate(delta)
                path_deltas[path] = existing
            self._accumulate_worker_meta(delta)
            self._accumulate_worker_meta_by_path(path_deltas)
            self._publish_connector_metrics()

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
        first_group_paths: list[str] = []
        first_group_latencies_ms: list[float] = []
        finished_latencies_ms: list[float] = []
        with self._layered_recv_lock:
            now = time.perf_counter()
            for req_id in ok_reqs:
                state = self._layered_recv_states.get(req_id)
                if state is None:
                    continue
                state.mark_started(now)
                if response_total_groups > 0:
                    state.ensure_total_groups(response_total_groups)
                group_became_ready = False
                if group_idx >= 0:
                    group_became_ready = state.ack_group(group_idx, now)
                if group_became_ready:
                    first_group_paths.append(self._routing_path_for_recv(req_id=req_id))
                    first_group_latencies_ms.append(state.first_group_latency_ms())
                if state.mark_finished_if_complete(now):
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
                        finished_latencies_ms.append(state.finished_latency_ms())

            if response.err_reqs:
                for req_id in response.err_reqs:
                    state = self._layered_recv_states.get(req_id)
                    if state is not None:
                        state.fail(response.err_msg or "layered transfer failed")

        delta = LayeredTransferWorkerMeta(
            received_group_batches=1 if ok_reqs and group_idx >= 0 else 0,
            received_finished_reqs=len(finished_reqs),
            receive_failures=len(response.err_reqs or []),
            receive_kv_first_group_count=len(first_group_latencies_ms),
            receive_kv_first_group_ms=sum(first_group_latencies_ms),
            receive_kv_finished_count=len(finished_latencies_ms),
            receive_kv_finished_ms=sum(finished_latencies_ms),
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
        for path, latency_ms in zip(first_group_paths, first_group_latencies_ms):
            existing = path_deltas.get(path, LayeredTransferWorkerMeta())
            path_deltas[path] = existing.aggregate(
                LayeredTransferWorkerMeta(
                    receive_kv_first_group_count=1,
                    receive_kv_first_group_ms=latency_ms,
                )
            )
        for path, latency_ms in zip(finished_paths, finished_latencies_ms):
            existing = path_deltas.get(path, LayeredTransferWorkerMeta())
            path_deltas[path] = existing.aggregate(
                LayeredTransferWorkerMeta(
                    received_finished_reqs=1,
                    receive_kv_finished_count=1,
                    receive_kv_finished_ms=latency_ms,
                )
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
            # ``start_load_kv`` owns the model-forward scope synchronously.
            # This coroutine runs on the sender loop and may lag or lead that
            # forward; assigning _current_send_transfer_ids here used to make
            # save_kv_layer announce unrelated outstanding transfers.
            for _, (transfer_id, _) in metadata.reqs_to_send.items():
                self._layered_send_states.setdefault(
                    transfer_id,
                    _LayeredSendState.create(transfer_id, self._sender_group_count),
                )
            for transfer_id in metadata.reqs_not_processed:
                self._layered_send_states.pop(transfer_id, None)
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
            if bool(getattr(self, "require_kv_transfer_manifest_v2", False)):
                # Upstream's non-layered wire struct has no extension field in
                # the supported vLLM version.  Do not silently bypass the
                # producer fence in a strict deployment.
                raise RuntimeError(
                    "strict P->D KV transfer manifests require the repo-local "
                    "layered connector wire protocol"
                )
            return await self._send_kv_to_decode_nonlayered(identity, sock, meta)

        from vllm import envs

        pending_reqs: dict[str, SendBlockMeta] = {}
        try:
            self._validate_kv_transfer_manifests_for_producer(meta)
        except RuntimeError as exc:
            response = LayeredMooncakeXferResponse(
                status=MooncakeXferResponseStatus.ERROR,
                err_reqs=list(meta.req_blocks),
                err_msg=str(exc),
                total_groups=self._sender_group_count,
            )
            await sock.send_multipart((identity, self._encoder.encode(response)))
            return
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

        remote_session = f"{meta.remote_hostname}:{meta.remote_port}"
        # A Decode worker can batch requests that reached Prefill at very
        # different times. Waiting for all of their SendBlockMeta objects here
        # turns the slowest request into a head-of-line barrier for every fast
        # one. Drive the transfer as an event graph instead: a request enters
        # group 0 as soon as its metadata is ready, then advances group by
        # group.  Crucially, a grouped CUDA source fence only protects the
        # physical KV blocks of *one* request.  Do not combine independently
        # ready requests after their fences have been published: waiting for a
        # second request's event can leave the first request's source blocks
        # reusable before its peer write starts.  The transport may still
        # coalesce contiguous descriptors inside that request/group.
        wait_tasks: dict[asyncio.Task[Any], tuple[str, int]] = {}
        active_req_ids = set(pending_reqs)
        sending_req_ids: set[str] = set()

        def _schedule_meta_wait(d_req_id: str, send_meta: SendBlockMeta) -> None:
            task = asyncio.create_task(self._wait_send_meta_ready(send_meta))
            wait_tasks[task] = (d_req_id, -1)

        def _schedule_group_wait(
            d_req_id: str,
            send_meta: SendBlockMeta,
            group_idx: int,
        ) -> None:
            task = asyncio.create_task(self._wait_group_ready((send_meta,), group_idx))
            wait_tasks[task] = (d_req_id, group_idx)

        def _finish_success(d_req_id: str, send_meta: SendBlockMeta) -> None:
            active_req_ids.discard(d_req_id)
            if d_req_id in sending_req_ids:
                send_meta.sending = max(0, send_meta.sending - 1)
                sending_req_ids.discard(d_req_id)
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
                    self._current_send_transfer_ids.discard(str(send_meta.transfer_id))

        def _finish_failure(
            d_req_id: str,
            send_meta: SendBlockMeta,
            message: str,
        ) -> None:
            active_req_ids.discard(d_req_id)
            if d_req_id in sending_req_ids:
                send_meta.sending = max(0, send_meta.sending - 1)
                sending_req_ids.discard(d_req_id)
            with self._layered_send_lock:
                state = self._layered_send_states.get(send_meta.transfer_id)
                if state is not None:
                    state.fail(message)
                self._layered_send_states.pop(send_meta.transfer_id, None)
                self._current_send_transfer_ids.discard(str(send_meta.transfer_id))
            self.reqs_need_send.pop(send_meta.transfer_id, None)
            failed_req_id = (
                str(send_meta.p_req_id)
                if getattr(send_meta, "p_req_id", "")
                else str(d_req_id)
            )
            self._send_request_routing_paths.pop(failed_req_id, None)
            self._send_transfer_routing_paths.pop(str(send_meta.transfer_id), None)

        async def _send_response(
            *,
            group_idx: int,
            ok_reqs: list[str],
            err_reqs: list[str],
            err_msg: str | None,
        ) -> None:
            response = LayeredMooncakeXferResponse(
                status=(
                    MooncakeXferResponseStatus.FINISH
                    if not active_req_ids
                    else MooncakeXferResponseStatus.CONTINUE
                ),
                ok_reqs=ok_reqs or None,
                err_reqs=err_reqs or None,
                err_msg=err_msg,
                group_index=group_idx,
                total_groups=total_groups,
            )
            await sock.send_multipart((identity, self._encoder.encode(response)))

        for d_req_id, send_meta in pending_reqs.items():
            _schedule_meta_wait(d_req_id, send_meta)

        try:
            while active_req_ids:
                if not wait_tasks:
                    message = "layered KV transfer lost all readiness waiters"
                    failed = list(active_req_ids)
                    for d_req_id in failed:
                        _finish_failure(d_req_id, pending_reqs[d_req_id], message)
                    await _send_response(
                        group_idx=-1,
                        ok_reqs=[],
                        err_reqs=failed,
                        err_msg=message,
                    )
                    break

                done, _ = await asyncio.wait(
                    tuple(wait_tasks),
                    timeout=envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    message = "Timeout waiting for P side ready or layered KV group."
                    logger.warning("%s reqs=%s", message, sorted(active_req_ids))
                    failed = list(active_req_ids)
                    for task in wait_tasks:
                        task.cancel()
                    wait_tasks.clear()
                    for d_req_id in failed:
                        _finish_failure(d_req_id, pending_reqs[d_req_id], message)
                    await _send_response(
                        group_idx=-1,
                        ok_reqs=[],
                        err_reqs=failed,
                        err_msg=message,
                    )
                    break

                ready_by_group: dict[int, list[tuple[str, SendBlockMeta]]] = {}
                failed_by_message: dict[str, list[str]] = {}
                for task in done:
                    d_req_id, group_idx = wait_tasks.pop(task)
                    if d_req_id not in active_req_ids:
                        continue
                    send_meta = pending_reqs[d_req_id]
                    try:
                        task.result()
                    except Exception as exc:
                        failed_by_message.setdefault(str(exc), []).append(d_req_id)
                        continue
                    if group_idx < 0:
                        if send_meta.transfer_id not in self.reqs_need_send:
                            failed_by_message.setdefault(
                                "request expired before layered KV send",
                                [],
                            ).append(d_req_id)
                            continue
                        if not send_meta.need_send:
                            self.resolve_need_send(send_meta, remote_tp_ranks)
                        send_meta.sending += 1
                        sending_req_ids.add(d_req_id)
                        _schedule_group_wait(d_req_id, send_meta, 0)
                    else:
                        ready_by_group.setdefault(group_idx, []).append(
                            (d_req_id, send_meta)
                        )

                for message, failed in failed_by_message.items():
                    for d_req_id in failed:
                        _finish_failure(d_req_id, pending_reqs[d_req_id], message)
                    await _send_response(
                        group_idx=-1,
                        ok_reqs=[],
                        err_reqs=failed,
                        err_msg=message,
                    )

                for group_idx in sorted(ready_by_group):
                    ready_reqs = [
                        item
                        for item in ready_by_group[group_idx]
                        if item[0] in active_req_ids
                    ]
                    if not ready_reqs:
                        continue
                    # A layered pull is a group-level protocol: its consumer
                    # advances all requests represented by this response as
                    # one transport round.  Preserve that wire/ordering
                    # contract while still fencing every request's exact CUDA
                    # source event and coalescing their descriptors into one
                    # native transaction.  Splitting this into independent
                    # responses made cold concurrent Qwen3-VL requests
                    # semantically corrupt even though every transfer reported
                    # success.
                    (
                        src_ptrs,
                        dst_ptrs,
                        lengths,
                        err_reqs,
                        err_msg,
                        _path_stats,
                        descriptor_paths,
                    ) = await self._build_transfer_params_for_group(
                        ready_reqs=ready_reqs,
                        agent_meta=meta,
                        local_regions=local_regions,
                        remote_regions=remote_regions,
                        group_idx=group_idx,
                    )
                    err_req_set = set(err_reqs)
                    ok_reqs = [
                        d_req_id
                        for d_req_id, _ in ready_reqs
                        if d_req_id not in err_req_set
                    ]
                    if src_ptrs and ok_reqs:
                        source_events = self._source_ready_events_for_group(
                            ready_reqs,
                            group_idx,
                        )
                        dispatch_error: str | None = None
                        try:
                            dispatch_ret = await self.sender_loop.run_in_executor(
                                self._sender_executor,
                                self._wait_source_events_and_send_blocks,
                                source_events,
                                remote_session,
                                src_ptrs,
                                dst_ptrs,
                                lengths,
                                descriptor_paths,
                                group_idx + 1 < total_groups,
                            )
                        except Exception as exc:
                            dispatch_ret = -1
                            dispatch_error = (
                                "source-ready fence or Mooncake transfer dispatch failed: "
                                f"{exc}"
                            )
                        if dispatch_ret != 0:
                            transfer_err_msg = dispatch_error or (
                                f"Mooncake transfer engine returned {dispatch_ret}"
                            )
                            err_msg = (
                                transfer_err_msg
                                if err_msg is None
                                else f"{err_msg}; {transfer_err_msg}"
                            )
                            err_req_set.update(ok_reqs)
                            err_reqs = list(err_req_set)
                            ok_reqs = []

                    for failed_req_id in err_req_set:
                        _finish_failure(
                            failed_req_id,
                            pending_reqs[failed_req_id],
                            err_msg or "layered transfer failed",
                        )
                    for succeeded_req_id in ok_reqs:
                        succeeded_meta = pending_reqs[succeeded_req_id]
                        if group_idx + 1 >= total_groups:
                            _finish_success(succeeded_req_id, succeeded_meta)
                        else:
                            _schedule_group_wait(
                                succeeded_req_id,
                                succeeded_meta,
                                group_idx + 1,
                            )
                    await _send_response(
                        group_idx=group_idx,
                        ok_reqs=ok_reqs,
                        err_reqs=list(err_req_set),
                        err_msg=err_msg,
                    )
        finally:
            pending_tasks = list(wait_tasks)
            for task in pending_tasks:
                task.cancel()
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            for d_req_id in list(sending_req_ids):
                send_meta = pending_reqs.get(d_req_id)
                if send_meta is not None:
                    send_meta.sending = max(0, send_meta.sending - 1)
            self._publish_connector_metrics(force=True)

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
            if state.failed is not None:
                raise RuntimeError(state.failed)
            event = state.async_group_event(group_idx, asyncio.get_running_loop())
            if event.is_set():
                # Finished Prefill requests commonly reach this path with every
                # group ready. No executor work is needed for the fast path.
                if state.failed is not None:
                    raise RuntimeError(state.failed)
                continue
            try:
                await asyncio.wait_for(
                    event.wait(),
                    timeout=envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT,
                )
            except asyncio.TimeoutError as exc:
                raise TimeoutError(
                    f"timed out waiting for group {group_idx} "
                    f"transfer_id={send_meta.transfer_id}"
                ) from exc
            if state.failed is not None:
                raise RuntimeError(state.failed)

    def _source_ready_events_for_group(
        self,
        ready_reqs: Iterable[tuple[str, SendBlockMeta]],
        group_idx: int,
    ) -> list[Any]:
        """Return deduplicated CUDA fences for the transfer's source groups."""

        with self._layered_send_lock:
            states_by_transfer = {
                transfer_id: self._layered_send_states.get(transfer_id)
                for _, send_meta in ready_reqs
                if (transfer_id := str(send_meta.transfer_id))
            }
        events: list[Any] = []
        seen_event_ids: set[int] = set()
        for state in states_by_transfer.values():
            if state is None:
                continue
            event = state.source_ready_event(group_idx)
            if event is None or id(event) in seen_event_ids:
                continue
            seen_event_ids.add(id(event))
            events.append(event)
        return events

    def _wait_source_events_and_send_blocks(
        self,
        source_events: Iterable[Any],
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
        descriptor_paths: list[str] | str | None = None,
        apply_logical_group_delay: bool = False,
    ) -> int:
        """Synchronize source KV events, then issue one grouped data-plane send.

        This runs on the existing sender executor so the model-forward thread
        never blocks on a transfer.  A CUDA event waits only for the matching
        group's enqueued KV writes; it is intentionally narrower than
        ``torch.cuda.synchronize`` and preserves producer/transfer overlap.
        """

        # Do not synchronize a source event and then queue behind another
        # sender worker: vLLM may recycle the same KV block once a later batch
        # advances.  The lock is intentionally acquired *before* the event
        # wait, making completion-fence -> peer write an atomic source-lifetime
        # boundary.  It is per producer worker, so independent Prefill workers
        # and all decode-side work remain concurrent.
        with self._layered_source_to_send_lock:
            events: list[Any] = []
            seen_event_ids: set[int] = set()
            for event in source_events:
                if event is None or id(event) in seen_event_ids:
                    continue
                seen_event_ids.add(id(event))
                events.append(event)
            if events:
                start_time = time.perf_counter()
                try:
                    for event in events:
                        event.synchronize()
                finally:
                    self._accumulate_worker_meta(
                        LayeredTransferWorkerMeta(
                            source_ready_event_waits=len(events),
                            source_ready_event_wait_ms=(
                                time.perf_counter() - start_time
                            )
                            * 1000.0,
                        )
                    )
            return self._send_blocks(
                remote_session,
                src_ptrs,
                dst_ptrs,
                lengths,
                descriptor_paths,
                apply_logical_group_delay,
            )

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
        list[str] | str | None,
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
        list[str] | str | None,
    ]:
        from vllm.distributed.kv_transfer.kv_connector.v1.mooncake.mooncake_connector import (
            _can_coalesce_block_transfers,
            group_concurrent_contiguous,
        )

        src_ptrs: list[int] = []
        dst_ptrs: list[int] = []
        lengths: list[int] = []
        err_reqs: list[str] = []
        err_msg: str | None = None
        # Homogeneous PD/EPD requests are the normal serving case. Retain one
        # path label for the whole descriptor vector and only allocate a label
        # list when a batch actually mixes routing paths.
        descriptor_paths: list[str] | str | None = None
        descriptor_path_count = 0

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
            path_descs = 0

            for local_region, remote_region in zip(local_regions, remote_regions):
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
                        path_descs += 1
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
                            path_descs += 1
            if path_descs <= 0:
                continue
            if descriptor_paths is None:
                descriptor_paths = routing_path
            elif isinstance(descriptor_paths, str):
                if descriptor_paths != routing_path:
                    descriptor_paths = [descriptor_paths] * descriptor_path_count
                    descriptor_paths.extend([routing_path] * path_descs)
            else:
                descriptor_paths.extend([routing_path] * path_descs)
            descriptor_path_count += path_descs

        # Metrics are derived where descriptors become actual transport
        # batches. The former request-level aggregate was not consumed and
        # duplicated a full descriptor traversal on the hot path.
        return src_ptrs, dst_ptrs, lengths, err_reqs, err_msg, {}, descriptor_paths

    @staticmethod
    def _path_stats_from_descriptor_paths(
        descriptor_paths: list[str] | tuple[str, ...] | str | None,
        lengths: list[int],
    ) -> dict[str, tuple[int, int]]:
        if not descriptor_paths:
            return {}
        if isinstance(descriptor_paths, str):
            return {
                EPDMooncakeConnectorWorker._normalize_routing_path(descriptor_paths): (
                    len(lengths),
                    sum(int(size) for size in lengths),
                )
            }
        if len(descriptor_paths) != len(lengths):
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
    ) -> dict[str, LayeredTransferWorkerMeta]:
        deltas: dict[str, LayeredTransferWorkerMeta] = {}
        for path, stats in dict(path_stats or {}).items():
            desc_count, byte_count = int(stats[0]), int(stats[1])
            if desc_count <= 0 and byte_count <= 0:
                continue
            deltas[self._normalize_routing_path(path)] = LayeredTransferWorkerMeta(
                grouped_batches=1,
                grouped_bytes=byte_count,
                grouped_descriptors=desc_count,
            ).aggregate(
                self._transfer_backend_delta(
                    backend_label,
                    total_bytes=byte_count,
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

    def _transfer_region_descriptors_via_peer_engine(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
    ) -> _TransferDispatchResult:
        # Avoid scanning a large descriptor list solely to format a disabled
        # trace message. The direct engine computes the byte total for result
        # accounting below.
        if self.trace_layered_kv:
            self._trace(
                "peer_buffer plan remote=%s desc=%d bytes=%d",
                remote_session,
                len(src_ptrs),
                sum(int(length) for length in lengths),
            )
        engine = self._get_peer_transfer_engine()
        result = engine.transfer_registered_pointer_batch(
            remote_session=remote_session,
            local_pointers=src_ptrs,
            remote_pointers=dst_ptrs,
            lengths=lengths,
        )
        timings = dict(result.timings_ms or {})
        return _TransferDispatchResult(
            ret_code=0,
            backend_label="peer_buffer_direct",
            dispatch_ms=float(timings.get("total_ms", 0.0) or 0.0),
            prepare_ms=float(timings.get("prepare_ms", 0.0) or 0.0),
            write_ms=float(timings.get("write_ms", 0.0) or 0.0),
            native_engine_lock_wait_ms=float(
                timings.get("native_engine_lock_wait_ms", 0.0) or 0.0
            ),
        )

    def _batched_transfer_regions(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
    ) -> _TransferDispatchResult:
        backend = self.transport_backend.strip().lower()
        if backend in {"mooncake_engine_direct", "engine_direct", "direct_engine"}:
            try:
                dispatch = self._transfer_region_descriptors_via_peer_engine(
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                )
                if self.trace_layered_kv:
                    self._trace(
                        "peer_buffer committed remote=%s desc=%d bytes=%d",
                        remote_session,
                        len(src_ptrs),
                        sum(lengths),
                    )
                return dispatch
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
                fallback_started = time.perf_counter()
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
                    dispatch_ms=(time.perf_counter() - fallback_started) * 1000.0,
                )
        start_time = time.perf_counter()
        ret_code = self.engine.batch_transfer_sync_write(
            remote_session,
            src_ptrs,
            dst_ptrs,
            lengths,
        )
        return _TransferDispatchResult(
            ret_code=ret_code,
            backend_label="batch_transfer_native",
            dispatch_ms=(time.perf_counter() - start_time) * 1000.0,
        )

    def _transfer_backend_delta(
        self,
        backend_label: str,
        *,
        total_bytes: int,
        dispatch: _TransferDispatchResult | None = None,
    ) -> LayeredTransferWorkerMeta:
        delta = LayeredTransferWorkerMeta(
            backend_counts={backend_label: 1},
        )
        if backend_label == "peer_buffer_direct":
            delta.peer_buffer_batches = 1
            delta.peer_buffer_bytes = total_bytes
            if dispatch is not None:
                delta.peer_buffer_dispatch_ms = max(
                    0.0, float(getattr(dispatch, "dispatch_ms", 0.0) or 0.0)
                )
                delta.peer_buffer_prepare_ms = max(
                    0.0, float(getattr(dispatch, "prepare_ms", 0.0) or 0.0)
                )
                delta.peer_buffer_write_ms = max(
                    0.0, float(getattr(dispatch, "write_ms", 0.0) or 0.0)
                )
                delta.peer_buffer_native_engine_lock_wait_ms = max(
                    0.0,
                    float(
                        getattr(
                            dispatch,
                            "native_engine_lock_wait_ms",
                            0.0,
                        )
                        or 0.0
                    ),
                )
        elif backend_label == "batch_transfer_fallback":
            delta.fallback_batches = 1
            delta.fallback_bytes = total_bytes
        return delta

    def _uses_direct_peer_backend(self) -> bool:
        return self.transport_backend.strip().lower() in {
            "mooncake_engine_direct",
            "engine_direct",
            "direct_engine",
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
        descriptor_paths: list[str] | str | None = None,
        apply_logical_group_delay: bool = False,
    ) -> int:
        if not src_ptrs:
            return 0

        single_batch_desc_limit = max(1, int(getattr(self, "max_transfer_descriptors", 128)))
        single_batch_byte_limit = max(0, int(getattr(self, "max_transfer_bytes", 0)))
        total_bytes = sum(int(v) for v in lengths)
        must_chunk_for_transport = (
            len(src_ptrs) > single_batch_desc_limit
            or (single_batch_byte_limit > 0 and total_bytes > single_batch_byte_limit)
        )
        if not must_chunk_for_transport:
            full_path_stats = self._path_stats_from_descriptor_paths(
                descriptor_paths, lengths
            )
            return self._send_region_group(
                remote_session,
                src_ptrs,
                dst_ptrs,
                lengths,
                path_stats=full_path_stats,
            )

        # ``_send_blocks`` is already invoked with one logical layered-KV group
        # by ``send_kv_to_decode``.  Re-slicing that group by the model-wide
        # layer count created dozens of tiny peer-buffer submissions per
        # request and made Decode TTFT dominated by control-plane handshakes.
        # Chunk here only for real transport safety limits (descriptor count or
        # bytes), so the direct peer-buffer path stays zero-copy while avoiding
        # unnecessary per-chunk round trips.
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
        logger.debug(
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
            group_paths: list[str] | str | None = None
            if isinstance(descriptor_paths, str):
                group_paths = descriptor_paths
            elif descriptor_paths is not None:
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
        # The chunks above are transport safety chunks within one logical layer
        # group.  Sleeping between them serialized an otherwise contiguous
        # peer-buffer write and inflated TTFT.  If a caller explicitly requests
        # a scheduling delay, apply it once at the logical group boundary.
        if apply_logical_group_delay and self.group_delay_ms > 0:
            self._accumulate_worker_meta(
                LayeredTransferWorkerMeta(
                    accumulated_group_delay_ms=self.group_delay_ms
                )
            )
            time.sleep(self.group_delay_ms / 1000.0)
        return 0


    def _send_region_group_with_retry(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
        path_stats: dict[str, tuple[int, int]] | None = None,
        descriptor_paths: list[str] | str | None = None,
        *,
        attempt: int = 0,
    ) -> _TransferDispatchResult:
        max_attempts = max(0, int(getattr(self, "transfer_retry_attempts", 0)))
        can_retry = len(src_ptrs) > 1 and attempt < max_attempts
        dispatch = self._send_region_group_dispatch(
            remote_session,
            src_ptrs,
            dst_ptrs,
            lengths,
            path_stats,
            record_failure_metrics=not can_retry,
        )
        if dispatch.ret_code == 0:
            return dispatch

        if not can_retry:
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

        if isinstance(descriptor_paths, list):
            left_paths: list[str] | str | None = descriptor_paths[:mid]
            right_paths: list[str] | str | None = descriptor_paths[mid:]
        else:
            left_paths = descriptor_paths
            right_paths = descriptor_paths
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
                    dispatch=dispatch,
                )
            )
            self._accumulate_worker_meta(delta)
            path_deltas = self._path_deltas_for_batch(
                path_stats,
                backend_label=dispatch.backend_label,
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
            if record_failure_metrics:
                self._accumulate_worker_meta(
                    LayeredTransferWorkerMeta(failed_batches=1)
                )
                path_failure_deltas = {
                    self._normalize_routing_path(path): LayeredTransferWorkerMeta(failed_batches=1)
                    for path in dict(path_stats or {})
                }
                if path_failure_deltas:
                    self._accumulate_worker_meta_by_path(path_failure_deltas)
                self._publish_connector_metrics()
                self.xfer_stats.record_failed_transfer()
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
