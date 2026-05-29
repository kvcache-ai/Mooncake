"""
Usage:
Adding the following params to vllm command:
Prefill: --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer", "kv_connector_module_path":"mooncake.mooncake_connector_v1"}'
Decode: --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer", "kv_connector_module_path":"mooncake.mooncake_connector_v1"}'
Proxy: Running vllm_v1_proxy_server.py
"""

import contextlib
import asyncio
import threading
import time
import importlib.metadata
import json
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import IntEnum
from queue import Queue
from os import getenv
from typing import TYPE_CHECKING, Any, Optional

import msgspec
import numpy as np
import torch
import zmq

from vllm.attention.selector import get_attn_backend
from vllm.config import VllmConfig
from vllm.distributed.kv_transfer.kv_connector.v1.base import (
    KVConnectorBase_V1, KVConnectorMetadata, KVConnectorRole)

# SupportsHMA was introduced in vllm-project/vllm PR #25712 and is enforced
# for KV connectors by PR #27592. It is the marker the Hybrid Memory
# Allocator uses to allow PD-disaggregation with hybrid (e.g. attention +
# Mamba2) models. We import it conditionally so this connector keeps
# working on older vLLM releases that pre-date the interface; on those
# releases the marker is a no-op object base and the
# `request_finished_all_groups` shim below is dead code.
try:
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        SupportsHMA)
except ImportError:  # pragma: no cover - older vLLM
    SupportsHMA = object  # type: ignore[assignment, misc]
from vllm.distributed.parallel_state import (get_tensor_model_parallel_rank,
                                             get_tp_group)
from vllm.forward_context import ForwardContext
from vllm.logger import init_logger
try:
    from vllm.utils import get_ip, make_zmq_path, make_zmq_socket
except ImportError:
    from vllm.utils.network_utils import get_ip, make_zmq_path, make_zmq_socket
from vllm.v1.attention.backends.utils import get_kv_cache_layout
from vllm.v1.core.sched.output import SchedulerOutput
from vllm.v1.request import RequestStatus

if TYPE_CHECKING:
    from vllm.attention.backends.abstract import AttentionMetadata
    from vllm.v1.core.kv_cache_manager import KVCacheBlocks
    from vllm.v1.request import Request

EngineId = str
ReqId = str

TRANS_DONE = b"trans_done"
TRANS_ERROR = b"trans_error"
VLLM_MOONCAKE_SIDE_CHANNEL_PORT = int(getenv("VLLM_MOONCAKE_SIDE_CHANNEL_PORT", 6557))
VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT = int(getenv("VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT", 120))
VLLM_MOONCAKE_SENDER_WORKERS = int(getenv("VLLM_MOONCAKE_SENDER_WORKERS", 10))
VLLM_MOONCAKE_PROTOCOL = getenv("VLLM_MOONCAKE_PROTOCOL", "rdma")
VLLM_MOONCAKE_MIGRATION_BLOCK_POOL_SIZE = int(getenv("VLLM_MOONCAKE_MIGRATION_BLOCK_POOL_SIZE", "4096"))
VLLM_MOONCAKE_MIGRATION_BANDWIDTH_MBPS = int(getenv("MOONCAKE_MIGRATION_BANDWIDTH_MBPS", "0"))
VLLM_MOONCAKE_MIGRATION_HTTP_PORT = int(getenv("VLLM_MOONCAKE_MIGRATION_HTTP_PORT", "6558"))
VLLM_MOONCAKE_MIGRATION_DUAL_WRITE_SYNC_INTERVAL = float(
    getenv("VLLM_MOONCAKE_MIGRATION_DUAL_WRITE_SYNC_INTERVAL", "0.2")
)
VLLM_MOONCAKE_MIGRATION_SWITCH_STABLE_CHECKS = int(
    getenv("VLLM_MOONCAKE_MIGRATION_SWITCH_STABLE_CHECKS", "5")
)

logger = init_logger(__name__)


class MigrationPhase(IntEnum):
    IDLE = 0
    BACKGROUND_COPY = 1
    DUAL_WRITE = 2
    SWITCH_OVER = 3
    COMPLETED = 4


@dataclass
class MigratingRequestMeta:
    request_id: ReqId
    target_host: str
    target_port: int              # RPC port (for TransferEngine)
    migration_port: int = 18900   # HTTP port (for migration control API)
    target_base_addr: list[int] = field(default_factory=list)
    block_id_map: dict[int, int] = field(default_factory=dict)
    phase: MigrationPhase = MigrationPhase.IDLE
    phase_event: asyncio.Event = field(default_factory=asyncio.Event)
    # Phase 2: dual-write tracking
    pending_block_ids: set[int] = field(default_factory=set)
    synced_block_ids: set[int] = field(default_factory=set)
    extra_target_pool: list[int] = field(default_factory=list)
    switch_check_counter: int = 0
    last_synced_count: int = 0
    target_block_pool_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class MigrationTargetInfo:
    request_id: ReqId
    target_block_ids: list[int]
    kv_caches_base_addr: list[int]


class MigrationRateLimiter:
    """Token bucket rate limiter for migration bandwidth."""

    def __init__(self, max_mb_per_sec: int = 0):
        self.max_bytes_per_sec = max_mb_per_sec * 1024 * 1024
        if self.max_bytes_per_sec > 0:
            self.bucket_size = self.max_bytes_per_sec * 2  # burst up to 2x
            self.tokens = float(self.bucket_size)
            self.last_refill = time.monotonic()
        else:
            self.bucket_size = 0
            self.tokens = 0.0
            self.last_refill = 0.0

    async def acquire(self, bytes_count: float):
        if self.max_bytes_per_sec <= 0:
            return  # unlimited
        while self.tokens < bytes_count:
            self._refill()
            await asyncio.sleep(0.01)
        self.tokens -= bytes_count

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(
            self.bucket_size,
            self.tokens + elapsed * self.max_bytes_per_sec
        )
        self.last_refill = now


class MooncakeAgentMetadata(
        msgspec.Struct,
        omit_defaults=True,  # type: ignore[call-arg]
        # required for @cached_property.
        dict=True):
    remote_hostname: str
    remote_port: int
    request_ids: list[ReqId]
    kv_caches_base_addr: list[int]
    block_ids: list[list[int]]


@dataclass
class RecvReqMeta:
    local_block_ids: list[int]
    remote_host: str
    remote_port: int
    remote_request_id: Optional[ReqId] = None


@dataclass
class SendBlockMeta:
    local_block_ids: list[int]
    ready: asyncio.Event
    expire_time: float = float("inf")


class MooncakeConnectorMetadata(KVConnectorMetadata):

    def __init__(self):
        self.reqs_to_recv: dict[ReqId, RecvReqMeta] = {}
        self.reqs_to_send: dict[ReqId, list[int]] = {}

    def add_new_req(self,
                    request_id: ReqId,
                    local_block_ids: list[int],
                    kv_transfer_params: dict[str, Any],
                    load_remote_cache: bool = True):
        if load_remote_cache:
            self.reqs_to_recv[request_id] = RecvReqMeta(
                local_block_ids=local_block_ids,
                remote_host=kv_transfer_params["remote_host"],
                remote_port=kv_transfer_params["remote_port"],
                remote_request_id=kv_transfer_params.get("remote_request_id"))
        else:
            self.reqs_to_send[request_id] = local_block_ids


class MooncakeConnector(KVConnectorBase_V1, SupportsHMA):
    # Subclassing SupportsHMA gates this connector through vLLM's Hybrid
    # Memory Allocator path, which is required to PD-disaggregate hybrid
    # attention + Mamba2 models (e.g. nvidia/NVIDIA-Nemotron-Nano-9B-v2).
    # On vLLM versions that pre-date PR #25712 the marker resolves to
    # `object` and this is identical to the previous single-base class.

    def __init__(self, vllm_config: VllmConfig, role: KVConnectorRole):
        assert vllm_config.kv_transfer_config is not None
        assert vllm_config.kv_transfer_config.engine_id is not None
        super().__init__(vllm_config, role)
        self.engine_id: EngineId = vllm_config.kv_transfer_config.engine_id
        
        if role == KVConnectorRole.SCHEDULER:
            self.connector_scheduler: Optional[MooncakeConnectorScheduler] = \
                MooncakeConnectorScheduler(vllm_config, self.engine_id)
            self.connector_worker: Optional[MooncakeConnectorWorker] = None
        elif role == KVConnectorRole.WORKER:
            self.connector_scheduler = None
            self.connector_worker = MooncakeConnectorWorker(
                vllm_config, self.engine_id)

    ############################################################
    # Scheduler Side Methods
    ############################################################

    def get_num_new_matched_tokens(
            self, request: "Request",
            num_computed_tokens: int) -> tuple[int, bool]:
        assert self.connector_scheduler is not None
        return self.connector_scheduler.get_num_new_matched_tokens(
            request, num_computed_tokens)

    def update_state_after_alloc(self, request: "Request",
                                 blocks: "KVCacheBlocks",
                                 num_external_tokens: int):
        assert self.connector_scheduler is not None
        return self.connector_scheduler.update_state_after_alloc(
            request, blocks, num_external_tokens)

    def build_connector_meta(
        self,
        scheduler_output: SchedulerOutput,
    ) -> KVConnectorMetadata:
        assert self.connector_scheduler is not None
        return self.connector_scheduler.build_connector_meta(scheduler_output)

    def request_finished(
        self,
        request: "Request",
        block_ids: list[int],
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        assert self.connector_scheduler is not None
        return self.connector_scheduler.request_finished(request, block_ids)

    def request_finished_all_groups(
        self,
        request: "Request",
        block_ids: tuple[list[int], ...],
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        """SupportsHMA hook for hybrid (multi-group) KV cache layouts.

        Hybrid models (e.g. attention + Mamba2) expose one block-id list
        per KV cache group instead of a single flat list. The Mooncake
        transport itself does not yet distinguish groups on the wire, so
        we flatten the per-group lists and delegate to the existing
        single-group `request_finished`. This is the minimum-viable shim
        that satisfies the `SupportsHMA` contract and unblocks the
        Hybrid Memory Allocator gate so the engine can start up; cross-
        node fidelity for non-attention SSM/Mamba state is not asserted
        by this method and remains a follow-up (the Mamba2 backend in
        vLLM still raises NotImplementedError from
        `get_kv_cache_shape()`).
        """
        flat: list[int] = [b for group in block_ids for b in group]
        return self.request_finished(request, flat)

    ############################################################
    # Worker Side Methods
    ############################################################
    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]):
        assert self.connector_worker is not None
        self.connector_worker.register_kv_caches(kv_caches)

    def get_finished(
        self, finished_req_ids: set[str]
    ) -> tuple[Optional[set[str]], Optional[set[str]]]:
        """Get the finished recving and sending requests."""
        assert self.connector_worker is not None
        return self.connector_worker.get_finished()

    def start_load_kv(self, forward_context: "ForwardContext",
                      **kwargs) -> None:
        assert self.connector_worker is not None
        assert isinstance(self._connector_metadata, MooncakeConnectorMetadata)
        self.connector_worker.start_load_kv(self._connector_metadata)

    def wait_for_layer_load(self, layer_name: str) -> None:
        """MooncakeConnector does not do layerwise saving."""
        pass

    def save_kv_layer(self, layer_name: str, kv_layer: torch.Tensor,
                      attn_metadata: "AttentionMetadata", **kwargs) -> None:
        """MooncakeConnector does not save explicitly."""
        pass

    def wait_for_save(self):
        pass


class MooncakeConnectorScheduler:
    """Implementation of Scheduler side methods"""

    def __init__(self, vllm_config: VllmConfig, engine_id: str):
        self.vllm_config = vllm_config
        self.engine_id: EngineId = engine_id
        self.side_channel_host = get_ip()
        self.side_channel_port = get_mooncake_side_channel_port(vllm_config)

        assert vllm_config.kv_transfer_config
        self.kv_role = vllm_config.kv_transfer_config.kv_role
        logger.info("Initializing Mooncake Transfer Engine Scheduler %s",
                    engine_id)

        # Requests that need to start recv/send.
        # New requests are added by update_state_after_alloc in
        # the scheduler. Used to make metadata passed to Worker.
        self._reqs_need_recv: dict[ReqId, tuple[Request, list[int]]] = {}
        self._reqs_need_send: dict[ReqId, list[int]] = {}

    def get_num_new_matched_tokens(
            self, request: "Request",
            num_computed_tokens: int) -> tuple[int, bool]:
        """
        For remote prefill, pull all prompt blocks from remote
        asynchronously relative to engine execution.

        Args:
            request (Request): the request object.
            num_computed_tokens (int): the number of locally
                computed tokens for this request
        Returns:
            * the number of tokens that can be loaded from the
              external KV cache beyond what is already computed.
            * true if the external KV cache tokens will be loaded
              asynchronously (between scheduler steps).
        """

        params = request.kv_transfer_params
        logger.debug(
            "MooncakeConnector get_num_new_matched_tokens: "
            "num_computed_tokens=%s, kv_transfer_params=%s",
            num_computed_tokens, params)

        if params is not None and params.get("do_remote_prefill"):
            # Remote prefill: get all prompt blocks from remote.
            count = len(request.prompt_token_ids) - num_computed_tokens
            if count > 0:
                return count, True

        # No remote prefill for this request.
        return 0, False

    def update_state_after_alloc(self, request: "Request",
                                 blocks: "KVCacheBlocks",
                                 num_external_tokens: int):

        params = request.kv_transfer_params
        logger.debug(
            "MooncakeConnector update_state_after_alloc: "
            "num_external_tokens=%s, kv_transfer_params=%s",
            num_external_tokens, params)

        if not params:
            return

        if params.get("do_remote_prefill"):
            assert self.kv_role != "kv_producer"
            if all(p in params for p in ("remote_host", "remote_port")):
                # If remote_blocks and num_external_tokens = 0, we have
                # a full prefix cache hit on the D worker. We need to call
                # send_notif in _read_blocks to free the memory on the P.
                local_block_ids = (blocks.get_unhashed_block_ids()
                                   if num_external_tokens > 0 else [])
                # Get unhashed blocks to pull from remote.
                self._reqs_need_recv[request.request_id] = (request,
                                                            local_block_ids)
            else:
                logger.warning(
                    "Got invalid KVTransferParams: %s. This "
                    "request will not utilize KVTransfer", params)
            # Only trigger 1 KV transfer per request.
            params["do_remote_prefill"] = False

        elif params.get("do_remote_decode"):
            # Add an empty list to worker to create event.
            self._reqs_need_send[request.request_id] = []

    def build_connector_meta(
        self,
        scheduler_output: SchedulerOutput,
    ) -> KVConnectorMetadata:
        meta = MooncakeConnectorMetadata()

        # Loop through scheduled reqs and convert to RecvReqMeta.
        if self.kv_role != "kv_producer":
            for req_id, (req, block_ids) in self._reqs_need_recv.items():
                assert req.kv_transfer_params is not None
                meta.add_new_req(request_id=req_id,
                                 local_block_ids=block_ids,
                                 kv_transfer_params=req.kv_transfer_params)
            self._reqs_need_recv.clear()

        if self.kv_role != "kv_consumer":
            for req_id, block_ids in self._reqs_need_send.items():
                meta.add_new_req(request_id=req_id,
                                 local_block_ids=block_ids,
                                 kv_transfer_params={},
                                 load_remote_cache=False)
            self._reqs_need_send.clear()

        return meta

    def request_finished(
        self,
        request: "Request",
        block_ids: list[int],
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        """
        Once a request is finished, determine whether request blocks
        should be freed now or will be sent asynchronously and freed later.
        """

        params = request.kv_transfer_params
        logger.debug(
            "MooncakeConnector request_finished, request_status=%s, "
            "kv_transfer_params=%s", request.status, params)
        if not params:
            return False, None

        if params.get("do_remote_prefill"):
            # If do_remote_prefill is still True when the request is finished,
            # update_state_after_alloc must not have been called (the request
            # must have been aborted before it was scheduled).
            # To avoid stranding the prefill blocks in the prefill instance,
            # we must add empty block_ids to _reqs_need_recv so that our
            # worker side will notify and free blocks in the prefill instance.
            assert self.kv_role != "kv_producer"
            self._reqs_need_recv[request.request_id] = (request, [])
            params["do_remote_prefill"] = False
            return False, None

        if (not params.get("do_remote_decode")
                or request.status != RequestStatus.FINISHED_LENGTH_CAPPED):
            return False, None

        assert self.kv_role != "kv_consumer"

        # TODO: check whether block_ids actually ever be 0. If not we could
        # remove the conditional below
        delay_free_blocks = len(block_ids) > 0

        if delay_free_blocks:
            self._reqs_need_send[request.request_id] = block_ids

        return delay_free_blocks, dict(
            do_remote_prefill=True,
            do_remote_decode=False,
            remote_host=self.side_channel_host,
            remote_port=self.side_channel_port,
            remote_request_id=request.request_id)


class MooncakeConnectorWorker:
    """Implementation of Worker side methods"""

    def __init__(self, vllm_config: VllmConfig, engine_id: str):
        try:
            from mooncake.engine import TransferEngine
        except ImportError as e:
            raise ImportError(
                "Please install mooncake by following the instructions at "
                "https://github.com/kvcache-ai/Mooncake/blob/main/doc/en/build.md "  # noqa: E501
                "to run VLLM with MooncakeTransferEngine.") from e
        logger.info("Initializing Mooncake Transfer Engine worker %s",
                    engine_id)

        self.vllm_config = vllm_config

        self.engine = TransferEngine()
        self.hostname = get_ip()
        ret_value = self.engine.initialize(self.hostname, "P2PHANDSHAKE",
                                           VLLM_MOONCAKE_PROTOCOL, "")
        if ret_value != 0:
            raise RuntimeError(
                "Mooncake Transfer Engine initialization failed.")

        self.rpc_port = self.engine.get_rpc_port()

        logger.debug("Mooncake Transfer Engine initialized at %s:%d",
                     self.hostname, self.rpc_port)

        # Mooncake handshake port.
        self.side_channel_port = get_mooncake_side_channel_port(vllm_config)

        self.engine_id: EngineId = engine_id
        self.tp_rank = get_tensor_model_parallel_rank()
        self.tp_group = get_tp_group()
        self.num_blocks = 0

        assert vllm_config.kv_transfer_config
        self.kv_role = vllm_config.kv_transfer_config.kv_role
        self.num_sender_workers = VLLM_MOONCAKE_SENDER_WORKERS
        # Create more tasks than workers to keep the thread pool saturated.
        # Tasks can await async events, so a surplus (2x is a robust heuristic)
        # prevents workers from idling.
        self.num_sender_tasks = self.num_sender_workers * 2

        self.kv_caches_base_addr: list[int] = []
        self.device_kv_caches: dict[str, torch.Tensor] = {}
        self.reqs_need_send: dict[ReqId, SendBlockMeta] = {}

        # For kv_both, we will act both prefiller and decoder.
        if self.kv_role != "kv_consumer":
            # Background threads for sending kvcaches to D.
            self._sender_executor = ThreadPoolExecutor(
                max_workers=self.num_sender_workers,
                thread_name_prefix="vllm-mooncake-sender",
            )
            logger.debug(
                "Mooncake Prefiller: use %d workers to send kvcaches",
                self.num_sender_workers,
            )
            # An asyncio queue to buffer incoming requests for the sender
            self.sender_worker_queue = asyncio.Queue[tuple[bytes, bytes]]()
            self.sender_loop = asyncio.new_event_loop()
            # Background thread for processing new sending requests.
            self._sender_listener_t = threading.Thread(
                target=_async_loop, args=(self.sender_loop,), daemon=True
            )
            self._sender_listener_t.start()

        if self.kv_role != "kv_producer":
            self.receiver_loop = asyncio.new_event_loop()
            self._mooncake_receiver_t = threading.Thread(
                target=_async_loop, args=(self.receiver_loop,), daemon=True
            )
            self._mooncake_receiver_t.start()
            logger.debug("Mooncake Decoder: start receiver thread")

        self.finished_sending_reqs: set[ReqId] = set()
        self.finished_recving_reqs: set[ReqId] = set()

        self.block_size = vllm_config.cache_config.block_size
        self.model_config = vllm_config.model_config
        self.cache_config = vllm_config.cache_config
        self.use_mla = self.model_config.use_mla

        backend = get_attn_backend(self.model_config.get_head_size(),
                                   self.model_config.dtype,
                                   self.cache_config.cache_dtype,
                                   self.block_size,
                                   use_mla=self.use_mla)
        self.backend_name = backend.get_name()
        vllm_version = importlib.metadata.version("vllm")
        versions_to_check = ("0.10.", "0.11.0", "0.11.1", "0.11.2", "0.12.0")
        version_checks = {
            version: vllm_version.startswith(version) 
            for version in versions_to_check
        }
        if any(version_checks[v] for v in ("0.10.", "0.11.0")):
            from vllm.attention.selector import backend_name_to_enum
            from vllm.platforms import _Backend
            attn_backend = backend_name_to_enum(self.backend_name)
            if version_checks["0.11.0"]:
                self._use_flashinfer = attn_backend == _Backend.FLASHINFER
                self._use_pallas_v1 = attn_backend == _Backend.PALLAS
            else:
                self._use_flashinfer = attn_backend == _Backend.FLASHINFER_VLLM_V1
                self._use_pallas_v1 = attn_backend == _Backend.PALLAS_VLLM_V1
        elif any(version_checks[v] for v in ("0.11.1", "0.11.2", "0.12.0")):
            from vllm.attention.selector import AttentionBackendEnum
            attn_backend = AttentionBackendEnum[self.backend_name]
            self._use_flashinfer = attn_backend in [AttentionBackendEnum.FLASHINFER, AttentionBackendEnum.FLASHINFER_MLA]
            self._use_pallas_v1 = attn_backend == AttentionBackendEnum.PALLAS
        else:
            raise Exception("Unsupported vllm version %s: This OOT module is intended for "
            "backward compatibility with earlier versions of vllm. For vllm 0.13.0 and newer, "
            "please use the built-in mooncake connector.", vllm_version)
        self.kv_cache_layout = get_kv_cache_layout()
        logger.debug("Detected attention backend %s", self.backend_name)
        logger.debug("Detected kv cache layout %s", self.kv_cache_layout)

        self.async_zmq_ctx = zmq.asyncio.Context()
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder(MooncakeAgentMetadata)

        # Migration live-migration state
        self.migration_reqs: dict[ReqId, MigratingRequestMeta] = {}
        self.migration_block_pool: Optional[asyncio.Queue[int]] = None
        self.migration_executor = ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix="vllm-mooncake-migration",
        )
        self.migration_rate_limiter = MigrationRateLimiter(
            VLLM_MOONCAKE_MIGRATION_BANDWIDTH_MBPS,
        )
        self.migration_http_port = VLLM_MOONCAKE_MIGRATION_HTTP_PORT
        self.migration_http_server: Optional[asyncio.AbstractServer] = None
        self.active_recving_reqs: set[ReqId] = set()

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        """Cleanup background threads on destruction."""
        # Cleanup migration resources
        self.migration_reqs.clear()
        if self.migration_http_server is not None:
            try:
                self.migration_http_server.close()
            except Exception:
                pass
            self.migration_http_server = None
        self.migration_executor.shutdown(wait=False)

        self.async_zmq_ctx.term()
        if self.kv_role != "kv_consumer":
            self._sender_executor.shutdown(wait=False)
            if self.sender_loop.is_running():
                self.sender_loop.call_soon_threadsafe(self.sender_loop.stop)
                self._sender_listener_t.join()
        if self.kv_role != "kv_producer" and self.receiver_loop.is_running():
            self.receiver_loop.call_soon_threadsafe(self.receiver_loop.stop)
            self._mooncake_receiver_t.join()

    async def _mooncake_sender_listener(
        self, ready_event: threading.Event, base_port: int, tp_rank: int
    ):
        """
        Background thread that listens for Mooncake requests, dispatches them
        to a thread pool, and sends acknowledgments upon completion.
        """

        path = make_zmq_path("tcp", self.hostname, base_port + tp_rank)
        sock = make_zmq_socket(self.async_zmq_ctx, path, zmq.ROUTER)
        logger.debug("Mooncake sender starting listening on path: %s", path)

        # Create async worker tasks that process items from the queue
        sender_tasks = [
            asyncio.create_task(self._sender_worker(sock))
            for _ in range(self.num_sender_tasks)
        ]

        ready_event.set()

        try:
            while True:
                identity, _, metadata_bytes = await sock.recv_multipart()
                await self.sender_worker_queue.put((identity, metadata_bytes))
        except zmq.ContextTerminated:
            logger.debug("ZMQ context terminated, exiting Mooncake sender thread.")
        except Exception as e:
            logger.error("Error in Mooncake sender thread: %s. Exiting thread.", str(e))
        finally:
            # Clean up worker tasks
            for task in sender_tasks:
                task.cancel()
            await asyncio.gather(*sender_tasks, return_exceptions=True)
            sock.close()

    async def _sender_worker(self, sock: zmq.asyncio.Socket):
        while True:
            try:
                identity, metadata_bytes = await self.sender_worker_queue.get()
                try:
                    metadata = self._decoder.decode(metadata_bytes)
                    await self.send_kv_to_decode(metadata)
                    await sock.send_multipart((identity, b"", TRANS_DONE))
                except Exception as e:
                    logger.error("Error processing Mooncake xfer request: %s", e)
                    await sock.send_multipart((identity, b"", TRANS_ERROR))
                finally:
                    self.sender_worker_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in _sender_worker: %s", e)

    async def send_kv_to_decode(self, meta: MooncakeAgentMetadata):
        send_reqs: list[tuple[ReqId, SendBlockMeta]] = []
        for req_id in meta.request_ids:
            send_meta = self.reqs_need_send.get(req_id)
            if send_meta is None:
                logger.warning("Request %s not found in reqs_need_send", req_id)
                return
            # Mark it as not expired. We will send it now.
            send_meta.expire_time = float("inf")
            send_reqs.append((req_id, send_meta))

        src_ptrs, dst_ptrs, lengths = await self._build_transfer_params(send_reqs, meta)
        remote_session = f"{meta.remote_hostname}:{meta.remote_port}"
        ret_value = await self.sender_loop.run_in_executor(
            self._sender_executor,
            self._send_blocks,
            remote_session,
            src_ptrs,
            dst_ptrs,
            lengths,
        )

        if ret_value != 0:
            raise RuntimeError(f"Error in batch_transfer_sync_write: {ret_value}")

        for req_id in meta.request_ids:
            del self.reqs_need_send[req_id]

        self.finished_sending_reqs.update(meta.request_ids)

    async def _build_transfer_params(
        self,
        send_reqs: list[tuple[ReqId, SendBlockMeta]],
        agent_meta: MooncakeAgentMetadata,
    ) -> tuple[list[int], list[int], list[int]]:
        src_ptrs = []
        dst_ptrs = []
        lengths = []
        local_base_addr = self.kv_caches_base_addr
        remote_base_addr = agent_meta.kv_caches_base_addr
        block_len = self.block_len
        remote_session = f"{agent_meta.remote_hostname}:{agent_meta.remote_port}"

        assert len(send_reqs) == len(agent_meta.block_ids)
        for (req_id, send_meta), remote_block_ids in zip(
            send_reqs, agent_meta.block_ids
        ):
            await send_meta.ready.wait()

            num_remote_blocks = len(remote_block_ids)
            if num_remote_blocks == 0:
                continue

            local_block_ids = send_meta.local_block_ids
            # Partial prefix cache hit: just read uncomputed blocks.
            num_local_blocks = len(local_block_ids)
            assert num_local_blocks >= num_remote_blocks
            if num_local_blocks > num_remote_blocks:
                local_block_ids = local_block_ids[-num_remote_blocks:]

            # Group by indices
            group_local_block_ids, group_remote_block_ids = group_concurrent_contiguous(
                local_block_ids, remote_block_ids
            )

            for local_layer_addr, remote_layer_addr in zip(
                local_base_addr, remote_base_addr
            ):
                for group_local_block_id, group_remote_block_id in zip(
                    group_local_block_ids, group_remote_block_ids
                ):
                    src_ptrs.append(
                        local_layer_addr + group_local_block_id[0] * block_len
                    )
                    dst_ptrs.append(
                        remote_layer_addr + group_remote_block_id[0] * block_len
                    )
                    lengths.append(block_len * len(group_local_block_id))

            logger.debug(
                "Sending kv_caches for request %s (%d blocks) to %s",
                req_id,
                num_remote_blocks,
                remote_session,
            )

        return src_ptrs, dst_ptrs, lengths

    def _send_blocks(
        self,
        remote_session: str,
        src_ptrs: list[int],
        dst_ptrs: list[int],
        lengths: list[int],
    ) -> int:
        start_time = time.perf_counter()
        ret_value = self.engine.batch_transfer_sync_write(
            remote_session, src_ptrs, dst_ptrs, lengths
        )
        if ret_value == 0:
            logger.debug(
                "Sending to %s done, took %s",
                remote_session,
                time.perf_counter() - start_time,
            )
        return ret_value

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]):
        """Register the KV Cache data in mooncake."""

        logger.info("Registering KV_Caches. use_mla: %s", self.use_mla)

        kv_data_ptrs = []
        kv_data_lens = []
        seen_base_addresses = []

        self.split_k_and_v = not (self.use_mla or self._use_pallas_v1
                                  or self._use_flashinfer)
        tensor_size_bytes = None
        for layer_name, cache_or_caches in kv_caches.items():
            logger.debug("registering layer %s with shape %s", layer_name,
                         cache_or_caches.shape)
            cache_list = cache_or_caches if self.split_k_and_v else [
                cache_or_caches
            ]

            for cache in cache_list:
                base_addr = cache.data_ptr()
                if base_addr in seen_base_addresses:
                    continue

                seen_base_addresses.append(base_addr)
                curr_tensor_size_bytes = cache.nbytes

                if tensor_size_bytes is None:
                    tensor_size_bytes = curr_tensor_size_bytes
                    self.num_blocks = cache.shape[0]

                assert tensor_size_bytes == curr_tensor_size_bytes, \
                    "All kv cache tensors must have the same size"
                kv_data_ptrs.append(base_addr)
                kv_data_lens.append(tensor_size_bytes)

        self.kv_caches_base_addr = seen_base_addresses

        ret_value = self.engine.batch_register_memory(kv_data_ptrs,
                                                      kv_data_lens)
        if ret_value != 0:
            raise RuntimeError("Mooncake batch memory registration failed.")

        assert tensor_size_bytes is not None
        assert self.num_blocks != 0
        assert tensor_size_bytes % self.num_blocks == 0
        self.block_len = tensor_size_bytes // self.num_blocks
        self.device_kv_caches = kv_caches
        logger.debug("regiestered num_blocks=%d block_len=%d", self.num_blocks,
                     self.block_len)

        # No need to launch server for D node, but start migration HTTP server.
        if self.kv_role == "kv_consumer":
            if self.migration_http_port > 0:
                asyncio.run_coroutine_threadsafe(
                    self._start_migration_http_server(),
                    self.receiver_loop,
                )
            return

        ready_event = threading.Event()
        asyncio.run_coroutine_threadsafe(
            self._mooncake_sender_listener(
                ready_event, self.side_channel_port, self.tp_rank
            ),
            self.sender_loop,
        )
        ready_event.wait()  # Wait for listener ZMQ socket to be ready.

    # ──── Migration HTTP Server ────

    async def _start_migration_http_server(self):
        """Start minimal async HTTP server for migration control API."""
        if self.migration_block_pool is None:
            self.migration_block_pool = asyncio.Queue()
            for i in range(VLLM_MOONCAKE_MIGRATION_BLOCK_POOL_SIZE):
                self.migration_block_pool.put_nowait(i)
        self.migration_http_server = await asyncio.start_server(
            self._migration_http_handler,
            host="0.0.0.0",
            port=self.migration_http_port,
        )
        logger.info(
            "Migration HTTP server listening on 0.0.0.0:%d",
            self.migration_http_port,
        )

    async def _migration_http_handler(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        """Handle incoming HTTP requests for migration control."""
        try:
            request_line = await reader.readuntil(b"\r\n")
            method, path, _ = request_line.decode().strip().split(" ", 2)

            headers = {}
            while True:
                header_line = await reader.readuntil(b"\r\n")
                if header_line == b"\r\n":
                    break
                key, value = header_line.decode().strip().split(": ", 1)
                headers[key.lower()] = value

            body = b""
            if "content-length" in headers:
                body_len = int(headers["content-length"])
                body = await reader.readexactly(body_len)

            if method == "GET" and path == "/api/v1/active_requests":
                response_body = json.dumps(
                    {"request_ids": list(self.active_recving_reqs)}
                ).encode()
                await self._send_http_response(writer, 200, response_body)
            elif method == "POST" and path == "/api/v1/prepare_migration":
                result = await self._handle_prepare_migration(body)
                await self._send_http_response(
                    writer, 200, result.encode()
                )
            elif method == "POST" and path == "/api/v1/start_migration":
                result = await self._handle_start_migration(body)
                await self._send_http_response(
                    writer, 200, result.encode()
                )
            elif method == "POST" and path == "/api/v1/allocate_blocks":
                result = await self._handle_allocate_blocks(body)
                await self._send_http_response(
                    writer, 200, result.encode()
                )
            elif method == "POST" and path == "/api/v1/switch_over":
                result = await self._handle_switch_over(body)
                await self._send_http_response(
                    writer, 200, result.encode()
                )
            elif method == "GET" and path.startswith("/api/v1/migration/status"):
                req_id = path.split("/")[-1]
                if req_id in self.migration_reqs:
                    meta = self.migration_reqs[req_id]
                    result = json.dumps({
                        "request_id": req_id,
                        "phase": meta.phase.name,
                        "target_host": meta.target_host,
                        "target_port": meta.target_port,
                    }).encode()
                else:
                    result = json.dumps({
                        "request_id": req_id, "phase": "UNKNOWN"
                    }).encode()
                await self._send_http_response(writer, 200, result)
            else:
                await self._send_http_response(
                    writer, 404, b'{"error":"not found"}'
                )
        except Exception as e:
            logger.error("Migration HTTP handler error: %s", e)
            try:
                await self._send_http_response(
                    writer, 500,
                    json.dumps({"error": str(e)}).encode(),
                )
            except Exception:
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _send_http_response(
        self, writer: asyncio.StreamWriter, status: int, body: bytes
    ):
        """Send a minimal HTTP response."""
        status_text = {
            200: "OK",
            404: "Not Found",
            500: "Internal Server Error",
        }.get(status, "Unknown")
        response = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode() + body
        writer.write(response)
        await writer.drain()

    # ──── Migration API Handlers ────

    async def _migration_http_request(
        self, host: str, port: int, method: str, path: str,
        body: Optional[dict] = None,
    ) -> dict:
        """Make an HTTP request to a migration endpoint."""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5.0
            )
            body_bytes = json.dumps(body).encode() if body else b""
            request_line = f"{method} {path} HTTP/1.1\r\n".encode()
            headers = (
                f"Host: {host}:{port}\r\n"
                f"Content-Length: {len(body_bytes)}\r\n"
                f"Content-Type: application/json\r\n"
                f"Connection: close\r\n\r\n"
            ).encode()
            writer.write(request_line + headers + body_bytes)
            await writer.drain()

            # Read response
            while True:
                line = await reader.readuntil(b"\r\n")
                if line == b"\r\n":
                    break
            resp_body = b""
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                resp_body += chunk
            writer.close()
            await writer.wait_closed()
            return json.loads(resp_body) if resp_body else {}
        except Exception as e:
            logger.error("Migration HTTP request to %s:%d%s failed: %s",
                         host, port, path, e)
            return {"error": str(e)}

    async def _handle_prepare_migration(self, body: bytes) -> str:
        """Pre-allocate blocks for migration on target node."""
        data = json.loads(body)
        request_id = data["request_id"]
        num_blocks = data["num_blocks"]
        extra_blocks = data.get("extra_blocks", 0)

        if self.migration_block_pool is None:
            return json.dumps({"error": "migration_block_pool not initialized"})

        target_block_ids = []
        for _ in range(num_blocks):
            try:
                block_id = await asyncio.wait_for(
                    self.migration_block_pool.get(), timeout=5.0
                )
                target_block_ids.append(block_id)
            except asyncio.TimeoutError:
                logger.warning(
                    "Migration block pool exhausted for request %s (%d/%d)",
                    request_id, len(target_block_ids), num_blocks,
                )
                break

        extra_target_block_ids = []
        for _ in range(extra_blocks):
            try:
                block_id = await asyncio.wait_for(
                    self.migration_block_pool.get(), timeout=5.0
                )
                extra_target_block_ids.append(block_id)
            except asyncio.TimeoutError:
                logger.warning(
                    "Extra block pool exhausted for request %s (%d/%d)",
                    request_id, len(extra_target_block_ids), extra_blocks,
                )
                break

        logger.info(
            "Prepared migration for request %s: %d blocks + %d extra",
            request_id, len(target_block_ids), len(extra_target_block_ids),
        )
        return json.dumps({
            "request_id": request_id,
            "target_block_ids": target_block_ids,
            "extra_target_block_ids": extra_target_block_ids,
            "kv_caches_base_addr": self.kv_caches_base_addr,
        })

    async def _handle_start_migration(self, body: bytes) -> str:
        """Start migration on source node (trigger Phase 1 BACKGROUND_COPY)."""
        data = json.loads(body)
        request_id = data["request_id"]
        target_host = data["target_host"]
        target_port = data["target_port"]
        target_base_addr = data["target_base_addr"]
        block_id_map = {int(k): v for k, v in data["block_id_map"].items()}
        extra_target_block_ids = data.get("extra_target_block_ids", [])

        meta = MigratingRequestMeta(
            request_id=request_id,
            target_host=target_host,
            target_port=target_port,
            migration_port=data.get("migration_port", VLLM_MOONCAKE_MIGRATION_HTTP_PORT),
            target_base_addr=target_base_addr,
            block_id_map=block_id_map,
            phase=MigrationPhase.BACKGROUND_COPY,
            phase_event=asyncio.Event(),
            extra_target_pool=list(extra_target_block_ids),
        )
        self.migration_reqs[request_id] = meta

        logger.info(
            "Starting migration for request %s -> %s:%d (%d blocks, %d extra)",
            request_id, target_host, target_port,
            len(block_id_map), len(extra_target_block_ids),
        )

        # Start Phase 1 in background
        asyncio.ensure_future(self._background_copy_existing_blocks(meta))

        return json.dumps({"status": "started", "phase": "BACKGROUND_COPY"})

    async def _handle_allocate_blocks(self, body: bytes) -> str:
        """Allocate additional migration blocks for an existing migration."""
        data = json.loads(body)
        request_id = data["request_id"]
        num_blocks = data["num_blocks"]

        if self.migration_block_pool is None:
            return json.dumps({"error": "migration_block_pool not initialized"})

        target_block_ids = []
        for _ in range(num_blocks):
            try:
                block_id = await asyncio.wait_for(
                    self.migration_block_pool.get(), timeout=5.0
                )
                target_block_ids.append(block_id)
            except asyncio.TimeoutError:
                logger.warning(
                    "Allocate blocks pool exhausted for request %s (%d/%d)",
                    request_id, len(target_block_ids), num_blocks,
                )
                break

        logger.info(
            "Allocated %d extra blocks for request %s",
            len(target_block_ids), request_id,
        )
        return json.dumps({
            "request_id": request_id,
            "target_block_ids": target_block_ids,
            "kv_caches_base_addr": self.kv_caches_base_addr,
        })

    # ──── Migration Phase 1: BACKGROUND_COPY ────

    async def _background_copy_existing_blocks(
        self, meta: MigratingRequestMeta
    ):
        """Phase 1: Copy all existing blocks from source to target."""
        request_id = meta.request_id

        try:
            src_ptrs, dst_ptrs, lengths, remote_session = (
                self._build_migration_transfer_params(meta)
            )

            if not src_ptrs:
                logger.info(
                    "No existing blocks to copy for request %s", request_id
                )
                meta.phase = MigrationPhase.DUAL_WRITE
                meta.phase_event.set()
                return

            total_bytes = sum(lengths)
            await self.migration_rate_limiter.acquire(total_bytes)

            loop = asyncio.get_running_loop()
            ret_value = await loop.run_in_executor(
                self.migration_executor,
                self._send_blocks,
                remote_session,
                src_ptrs,
                dst_ptrs,
                lengths,
            )

            if ret_value != 0:
                logger.error(
                    "Migration Phase 1 failed for request %s: %d",
                    request_id, ret_value,
                )
                meta.phase = MigrationPhase.IDLE
                return

            logger.info(
                "Migration Phase 1 (BACKGROUND_COPY) complete for request %s: "
                "%d blocks, %d bytes",
                request_id, len(src_ptrs), total_bytes,
            )

            # Populate all existing blocks as synced
            meta.synced_block_ids = set(meta.block_id_map.keys())

            meta.phase = MigrationPhase.DUAL_WRITE
            meta.phase_event.set()

            # Start Phase 2 dual-write sync loop and switch readiness check
            asyncio.ensure_future(self._dual_write_sync_loop(meta))
            asyncio.ensure_future(self._check_switch_readiness(meta))

        except Exception as e:
            logger.error(
                "Migration Phase 1 error for request %s: %s",
                request_id, e,
            )
            meta.phase = MigrationPhase.IDLE

    def _build_migration_transfer_params(
        self, meta: MigratingRequestMeta,
    ) -> tuple[list[int], list[int], list[int], str]:
        """Build transfer params using block_id_map."""
        src_ptrs = []
        dst_ptrs = []
        lengths = []
        local_base_addr = self.kv_caches_base_addr
        remote_base_addr = meta.target_base_addr
        block_len = self.block_len
        remote_session = f"{meta.target_host}:{meta.target_port}"

        block_id_map = meta.block_id_map
        if not block_id_map:
            return src_ptrs, dst_ptrs, lengths, remote_session

        src_block_ids = list(block_id_map.keys())
        dst_block_ids = [block_id_map[sid] for sid in src_block_ids]

        group_src, group_dst = group_concurrent_contiguous(
            src_block_ids, dst_block_ids
        )

        for local_base, remote_base in zip(local_base_addr, remote_base_addr):
            for src_group, dst_group in zip(group_src, group_dst):
                src_ptrs.append(
                    local_base + src_group[0] * block_len
                )
                dst_ptrs.append(
                    remote_base + dst_group[0] * block_len
                )
                lengths.append(block_len * len(src_group))

        return src_ptrs, dst_ptrs, lengths, remote_session

    # ──── Migration Phase 2: Dual-Write Sync ────

    async def _dual_write_sync_loop(self, meta: MigratingRequestMeta):
        """Phase 2: Continuously sync newly received blocks to target."""
        request_id = meta.request_id
        logger.info(
            "Dual-write sync loop started for request %s", request_id,
        )

        while True:
            try:
                await asyncio.sleep(VLLM_MOONCAKE_MIGRATION_DUAL_WRITE_SYNC_INTERVAL)

                if meta.phase not in (MigrationPhase.DUAL_WRITE,):
                    break

                # Pick up pending blocks
                async with meta.target_block_pool_lock:
                    pending = list(meta.pending_block_ids)
                    meta.pending_block_ids.clear()

                if not pending:
                    continue

                # Allocate target blocks if extra pool is exhausted
                new_target_ids = []
                for src_block_id in pending:
                    async with meta.target_block_pool_lock:
                        if meta.extra_target_pool:
                            target_id = meta.extra_target_pool.pop(0)
                        else:
                            new_target_ids.append(src_block_id)
                            continue
                    meta.block_id_map[src_block_id] = target_id

                if new_target_ids:
                    resp = await self._migration_http_request(
                        meta.target_host, meta.migration_port,
                        "POST", "/api/v1/allocate_blocks",
                        {"request_id": request_id, "num_blocks": len(new_target_ids)},
                    )
                    if "error" in resp:
                        logger.error(
                            "Failed to allocate extra blocks for %s: %s",
                            request_id, resp.get("error"),
                        )
                        continue
                    allocated = resp.get("target_block_ids", [])
                    async with meta.target_block_pool_lock:
                        meta.extra_target_pool.extend(allocated)
                    # Retry mapping
                    remaining = list(new_target_ids)
                    new_target_ids = []
                    for src_block_id in remaining:
                        async with meta.target_block_pool_lock:
                            if meta.extra_target_pool:
                                target_id = meta.extra_target_pool.pop(0)
                            else:
                                new_target_ids.append(src_block_id)
                                continue
                        meta.block_id_map[src_block_id] = target_id
                    if new_target_ids:
                        logger.warning(
                            "Still %d blocks unallocated for %s after allocate_blocks",
                            len(new_target_ids), request_id,
                        )
                        # Put back into pending
                        async with meta.target_block_pool_lock:
                            meta.pending_block_ids.update(new_target_ids)
                        continue

                # Build transfer params for pending blocks only
                meta_subset = MigratingRequestMeta(
                    request_id=meta.request_id,
                    target_host=meta.target_host,
                    target_port=meta.target_port,
                    target_base_addr=meta.target_base_addr,
                    block_id_map={
                        sid: meta.block_id_map[sid] for sid in pending
                        if sid in meta.block_id_map
                    },
                    phase=meta.phase,
                    phase_event=asyncio.Event(),
                )
                src_ptrs, dst_ptrs, lengths, remote_session = (
                    self._build_migration_transfer_params(meta_subset)
                )
                if not src_ptrs:
                    continue

                total_bytes = sum(lengths)
                await self.migration_rate_limiter.acquire(total_bytes)

                loop = asyncio.get_running_loop()
                ret_value = await loop.run_in_executor(
                    self.migration_executor,
                    self._send_blocks,
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                )

                if ret_value != 0:
                    logger.error(
                        "Dual-write transfer failed for request %s: %d",
                        request_id, ret_value,
                    )
                    async with meta.target_block_pool_lock:
                        meta.pending_block_ids.update(pending)
                    continue

                # Mark as synced
                meta.synced_block_ids.update(
                    sid for sid in pending if sid in meta.block_id_map
                )
                logger.debug(
                    "Dual-write synced %d blocks for %s "
                    "(total synced: %d)",
                    len(pending), request_id, len(meta.synced_block_ids),
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Dual-write sync error for %s: %s", request_id, e,
                )
                await asyncio.sleep(1.0)

        logger.info("Dual-write sync loop ended for request %s", request_id)

    # ──── Migration Phase 3: Switch Readiness ────

    async def _check_switch_readiness(self, meta: MigratingRequestMeta):
        """Monitor for stability and transition to SWITCH_OVER."""
        request_id = meta.request_id
        logger.info(
            "Switch readiness check started for request %s", request_id,
        )
        stable_checks_required = VLLM_MOONCAKE_MIGRATION_SWITCH_STABLE_CHECKS

        while True:
            try:
                await asyncio.sleep(VLLM_MOONCAKE_MIGRATION_DUAL_WRITE_SYNC_INTERVAL)

                if meta.phase not in (MigrationPhase.DUAL_WRITE,):
                    break

                current_synced = len(meta.synced_block_ids)
                if current_synced == meta.last_synced_count:
                    meta.switch_check_counter += 1
                    logger.debug(
                        "Switch check %d/%d for %s (synced: %d)",
                        meta.switch_check_counter, stable_checks_required,
                        request_id, current_synced,
                    )
                else:
                    # Still making progress, reset counter
                    meta.switch_check_counter = 0
                    meta.last_synced_count = current_synced

                if meta.switch_check_counter >= stable_checks_required:
                    logger.info(
                        "Request %s stable for %d checks, "
                        "triggering SWITCH_OVER",
                        request_id, stable_checks_required,
                    )
                    asyncio.ensure_future(self._do_switch_over(meta))
                    break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Switch readiness check error for %s: %s",
                    request_id, e,
                )
                await asyncio.sleep(1.0)

    async def _do_switch_over(self, meta: MigratingRequestMeta):
        """Phase 3: Switch over to target node."""
        request_id = meta.request_id
        meta.phase = MigrationPhase.SWITCH_OVER
        meta.phase_event.set()

        logger.info(
            "SWITCH_OVER for request %s -> %s:%d",
            request_id, meta.target_host, meta.target_port,
        )

        # Notify target node to activate this request's blocks
        resp = await self._migration_http_request(
            meta.target_host, meta.migration_port,
            "POST", "/api/v1/switch_over",
            {"request_id": request_id},
        )
        if "error" in resp:
            logger.error(
                "Switch-over notification to target failed for %s: %s",
                request_id, resp.get("error"),
            )
            return

        meta.phase = MigrationPhase.COMPLETED
        logger.info(
            "Migration COMPLETED for request %s", request_id,
        )

    async def _handle_switch_over(self, body: bytes) -> str:
        """Handle switch-over notification on target node."""
        data = json.loads(body)
        request_id = data["request_id"]
        logger.info(
            "Switch-over received for request %s on target", request_id,
        )
        # Target-side: mark blocks as active (they're already in memory)
        return json.dumps({
            "request_id": request_id, "status": "activated",
        })

    async def fetch_finished_recving_reqs(self) -> set[ReqId]:
        finished_recving_reqs = self.finished_recving_reqs
        self.finished_recving_reqs = set()
        return finished_recving_reqs

    async def fetch_finished_sending_reqs(self) -> set[ReqId]:
        finished_sending_reqs = self.finished_sending_reqs
        self.finished_sending_reqs = set()

        # Handle timeout to avoid stranding blocks on remote.
        now = time.perf_counter()
        expired_reqs = [
            req_id
            for req_id, send_meta in self.reqs_need_send.items()
            if send_meta.expire_time < now
        ]
        for req_id in expired_reqs:
            logger.warning(
                "Request %s timed out after %d seconds without "
                "being sent. Freeing its blocks on the producer side.",
                req_id,
                VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT,
            )
            del self.reqs_need_send[req_id]
        if expired_reqs:
            finished_sending_reqs.update(expired_reqs)

        return finished_sending_reqs

    def get_finished(self) -> tuple[set[str] | None, set[str] | None]:
        """
        Get requests that are done sending or recving on this specific worker.
        The scheduler process (via the MultiprocExecutor) will use this output
        to track which workers are done.
        """
        recv_fut = None
        send_fut = None
        if self.kv_role != "kv_producer":
            recv_fut = asyncio.run_coroutine_threadsafe(
                self.fetch_finished_recving_reqs(), self.receiver_loop
            )

        if self.kv_role != "kv_consumer":
            send_fut = asyncio.run_coroutine_threadsafe(
                self.fetch_finished_sending_reqs(), self.sender_loop
            )

        finished_recving_reqs = recv_fut.result() if recv_fut else set()
        finished_sending_reqs = send_fut.result() if send_fut else set()

        if finished_sending_reqs or finished_recving_reqs:
            logger.debug(
                "Rank %s, get_finished: %s requests done sending "
                "and %s requests done recving",
                self.tp_rank,
                len(finished_sending_reqs),
                len(finished_recving_reqs),
            )

        return finished_sending_reqs or None, finished_recving_reqs or None

    async def receive_kv(
        self, path: str, req_blocks: list[tuple[str, str, list[int]]]
    ):
        local_req_ids, remote_req_ids, block_ids = map(list, zip(*req_blocks))
        metadata = MooncakeAgentMetadata(
            remote_hostname=self.hostname,
            remote_port=self.rpc_port,
            request_ids=remote_req_ids,
            kv_caches_base_addr=self.kv_caches_base_addr,
            block_ids=block_ids,
        )

        encoded_data = self._encoder.encode(metadata)
        logger.debug(
            "Size of encoded MooncakeAgentMetadata: %d bytes", len(encoded_data)
        )
        logger.debug(
            "Sending kv transfer request for %s on path: %s "
            "(local requests: %s)", remote_req_ids, path, local_req_ids)

        # Send query for the request.
        sock: zmq.asyncio.Socket = make_zmq_socket(
            self.async_zmq_ctx, path, zmq.REQ, bind=False, linger=0
        )
        sock.setsockopt(zmq.RCVTIMEO, 60000)
        try:
            await sock.send(encoded_data)
            ret_msg = await sock.recv()
            if ret_msg != TRANS_DONE:
                logger.error(
                    "Error happens during transferring kvcache for %s, see logs in prefiller.",  # noqa: E501
                    remote_req_ids,
                )
                return
        except zmq.ContextTerminated:
            logger.debug("ZMQ context terminated, exiting Mooncake receiver thread.")
        except Exception as e:
            logger.error(
                "MooncakeAgentMetadata transfer failed for %s: %s",
                remote_req_ids, e)
            return
        finally:
            sock.close()

        self.finished_recving_reqs.update(local_req_ids)

        # Migration dual-write hook: if any received request is undergoing
        # DUAL_WRITE migration, add its new blocks to pending for sync
        for local_req_id, blocks in zip(local_req_ids, block_ids):
            if local_req_id in self.migration_reqs:
                meta = self.migration_reqs[local_req_id]
                if meta.phase == MigrationPhase.DUAL_WRITE and blocks:
                    async with meta.target_block_pool_lock:
                        meta.pending_block_ids.update(blocks)
                    logger.debug(
                        "Dual-write: queued %d new blocks for %s",
                        len(blocks), local_req_id,
                    )

        logger.debug(
            "pulling kv_caches for %s finished (local requests: %s)",
            remote_req_ids, local_req_ids)

    def group_kv_pull(self, metadata: MooncakeConnectorMetadata):
        kv_pulls = defaultdict(list)
        for req_id, meta in metadata.reqs_to_recv.items():
            logger.debug(
                "start_load_kv for request %s from remote engine. "
                "Num local_block_ids: %s.", req_id, len(meta.local_block_ids))
            path = make_zmq_path("tcp", meta.remote_host,
                                 meta.remote_port + self.tp_rank)
            remote_req_id = meta.remote_request_id or req_id
            if remote_req_id != req_id:
                logger.debug(
                    "request %s will pull remote kv for producer request %s",
                    req_id, remote_req_id)
            kv_pulls[path].append((req_id, remote_req_id, meta.local_block_ids))

        return kv_pulls

    async def record_send_reqs(self, metadata: MooncakeConnectorMetadata):
        for req_id, block_ids in metadata.reqs_to_send.items():
            if block_ids:
                # Already gone through request_finished()
                send_meta = self.reqs_need_send[req_id]
                send_meta.local_block_ids = block_ids
                send_meta.expire_time = (
                    time.perf_counter() + VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT
                )
                send_meta.ready.set()
            else:
                # From update_state_after_alloc(),
                # but not reach request_finished() yet
                self.reqs_need_send[req_id] = SendBlockMeta(
                    local_block_ids=[],
                    ready=asyncio.Event(),
                )

    def start_load_kv(self, metadata: MooncakeConnectorMetadata):
        if self.kv_role != "kv_producer":
            kv_pulls = self.group_kv_pull(metadata)
            # Track active requests for migration API
            for req_id in metadata.reqs_to_recv:
                self.active_recving_reqs.add(req_id)
            for path, req_blocks in kv_pulls.items():
                asyncio.run_coroutine_threadsafe(
                    self.receive_kv(path, req_blocks), self.receiver_loop
                )

        if self.kv_role != "kv_consumer":
            asyncio.run_coroutine_threadsafe(
                self.record_send_reqs(metadata), self.sender_loop
            )


@contextlib.contextmanager
def zmq_ctx(socket_type: Any, addr: str) -> Iterator[zmq.Socket]:
    """Context manager for a ZMQ socket"""

    if socket_type not in (zmq.ROUTER, zmq.REQ):
        raise ValueError(f"Unexpected socket type: {socket_type}")

    ctx: Optional[zmq.Context] = None
    try:
        ctx = zmq.Context()  # type: ignore[attr-defined]
        yield make_zmq_socket(ctx=ctx,
                              path=addr,
                              socket_type=socket_type,
                              bind=socket_type == zmq.ROUTER)
    finally:
        if ctx is not None:
            ctx.destroy(linger=0)


def group_concurrent_contiguous(
        src_indices: list[int],
        dst_indices: list[int]) -> tuple[list[list[int]], list[list[int]]]:
    """Vectorised NumPy implementation."""
    if len(src_indices) == 0:
        return [], []

    brk = np.where((np.diff(src_indices) != 1)
                   | (np.diff(dst_indices) != 1))[0] + 1
    src_groups = np.split(src_indices, brk)
    dst_groups = np.split(dst_indices, brk)

    src_groups = [g.tolist() for g in src_groups]
    dst_groups = [g.tolist() for g in dst_groups]

    return src_groups, dst_groups

def get_mooncake_side_channel_port(vllm_config: VllmConfig) -> int:
    # This logic is now centralized
    return (
        VLLM_MOONCAKE_SIDE_CHANNEL_PORT
        + vllm_config.parallel_config.data_parallel_rank
        * vllm_config.parallel_config.tensor_parallel_size
    )


def _async_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()
