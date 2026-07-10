"""Serving-path control plane for vLLM PD/EPD proxy integration.

This module bridges the repo's scheduler / KV-directory semantics into the
OpenAI-compatible vLLM proxy path. It is intentionally lightweight:

- admission + backpressure happen before the prefill/decode leg is sent;
- request metadata is serialized into ``kv_transfer_params`` so downstream
  vLLM/Mooncake connectors can observe layered-transfer / A2A hints;
- handoff bookkeeping uses the local KV directory's 2PC-shaped API so the
  serving plane and the standalone EPD state plane share one control model.

The current implementation is in-memory and process-local, but the API keeps
node / workflow / handoff identifiers explicit so it can later be backed by a
real distributed control store.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ...agent.coordination.scheduler import (
    AdmissionAction,
    AdmissionDecision,
    AgentRequest,
    AgentScheduler,
    AgentType,
    DegradeLevel,
    WorkerLoad,
)
from ..state.workflow_registry import WorkflowStateRecord, WorkflowStateRegistry
from .connector_metrics import ConnectorMetricsReader
from .kv_directory import LocalKVDirectory
from .kv_directory_rpc import RemoteKVDirectory, build_default_directory


TEXTUAL_CONTENT_TYPES = {
    "text",
    "input_text",
    "output_text",
}
MULTIMODAL_CONTENT_TYPES = {
    "image",
    "image_url",
    "input_image",
    "audio",
    "input_audio",
    "video",
    "input_video",
    "file",
    "input_file",
}


_KEEP_EXISTING = object()


@dataclass
class ServingControlPlaneConfig:
    node_id: str = "proxy"
    layered_kv_transfer: bool = True
    layers_per_group: int = 4
    group_delay_ms: float = 0.0
    max_group_bytes: int = 0
    transport_backend: str = "mooncake_engine_direct"
    mm_prefetch_policy: str = "event_driven"
    enable_mm_prefetch: bool = True
    warn_rho: float = 0.85
    critical_rho: float = 0.95
    max_backpressure_delay_ms: float = 150.0
    queue_capacity: int = 64
    prefill_capacity: int = 32
    decode_capacity: int = 64
    prefill_service_rate: float = 8.0
    decode_service_rate: float = 32.0
    source_agent_id: str = "prefill"
    target_agent_id: str = "decode"
    connector_metrics_dir: Optional[str] = None
    workflow_registry_wal_path: Optional[str] = None
    request_state_ttl_seconds: float = 900.0
    owner_shards: int = 1
    kv_directory_rpc_url: Optional[str] = None
    enable_workflow_affinity: bool = True
    workflow_affinity_ttl_seconds: float = 900.0
    strict_no_fallback: bool = False
    enable_agent_state_clone: bool = False
    agent_state_consume_requires_kv_handoff: bool = True
    mooncake_protocol: str = "tcp"
    high_prefill_worker_ids: Tuple[str, ...] = ()
    low_latency_decode_worker_ids: Tuple[str, ...] = ()
    standard_prefill_worker_ids: Tuple[str, ...] = ()
    standard_decode_worker_ids: Tuple[str, ...] = ()
    # Ordered Prefill -> Decode locality preferences. These remain soft so
    # admission can bypass a saturated Decode worker.
    prefill_decode_affinity: Tuple[Tuple[str, str], ...] = ()


@dataclass
class RequestContext:
    request_id: str
    workflow_id: str
    modality: str
    routing_path: str
    mm_hashes: List[str] = field(default_factory=list)
    estimated_input_tokens: int = 0
    created_at: float = field(default_factory=time.monotonic)
    deadline_at: Optional[float] = None
    transfer_id: str = ""
    prefill_worker_id: Optional[str] = None
    decode_worker_id: Optional[str] = None
    decode_affinity_worker_id: Optional[str] = None
    handoff_id: Optional[str] = None
    block_ids: List[str] = field(default_factory=list)
    placeholder_block_ids: List[str] = field(default_factory=list)
    degrade_level: DegradeLevel = DegradeLevel.NONE
    admission_action: AdmissionAction = AdmissionAction.ADMIT
    prefill_rho: float = 0.0
    decode_rho: float = 0.0
    handoff_committed: bool = False
    registry_version_seq: int = 0
    registry_version_id: str = ""
    registry_parent_version_id: Optional[str] = None
    token_ids: List[int] = field(default_factory=list)
    reuse_telemetry: Dict[str, Any] = field(default_factory=dict)
    reuse_candidate_block_ids: List[str] = field(default_factory=list)
    reuse_source_request_id: Optional[str] = None
    agent_type_hint: Optional[AgentType] = None
    priority: int = 0
    expected_output_tokens: int = 0
    agent_pd_profile: Dict[str, Any] = field(default_factory=dict)
    agent_state_id: Optional[str] = None


@dataclass
class StageRouteDecision:
    worker_id: str
    decision: AdmissionDecision
    wait_ms: float


class ServingControlPlane:
    """In-memory serving control plane for the disaggregated proxy."""

    def __init__(
        self,
        config: Optional[ServingControlPlaneConfig] = None,
        kv_directory: Optional[Any] = None,
        workflow_registry: Optional[WorkflowStateRegistry] = None,
    ):
        self.config = config or ServingControlPlaneConfig()
        if kv_directory is not None:
            self.kv_directory = kv_directory
        elif self.config.kv_directory_rpc_url:
            self.kv_directory = RemoteKVDirectory(self.config.kv_directory_rpc_url)
        else:
            self.kv_directory = build_default_directory(
                owner_shards=self.config.owner_shards,
                node_id=self.config.node_id,
            )
        self.workflow_registry = workflow_registry
        if self.workflow_registry is None and (
            self.config.workflow_registry_wal_path or self.config.enable_agent_state_clone
        ):
            # Agent-state lifecycle APIs need a durable/catalogued state id
            # even when the caller did not explicitly configure a WAL.  A
            # path-less registry remains process-local; a configured path keeps
            # the same recovery semantics as the serving request registry.
            self.workflow_registry = WorkflowStateRegistry(self.config.workflow_registry_wal_path)
        if self.config.connector_metrics_dir is None:
            self.config.connector_metrics_dir = (
                str(os.getenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", "")).strip() or None
            )
        self._lock = threading.RLock()
        self._prefill_scheduler = AgentScheduler([])
        self._decode_scheduler = AgentScheduler([])
        self._request_ctx: Dict[str, RequestContext] = {}
        self._worker_last_arrival: Dict[str, float] = {}
        self._workflow_latest: Dict[str, Dict[str, Any]] = {}
        self._workflow_affinity: Dict[str, Tuple[str, float]] = {}
        self._prefill_decode_affinity = self._normalize_prefill_decode_affinity(
            self.config.prefill_decode_affinity
        )
        self._connector_metrics = ConnectorMetricsReader(self.config.connector_metrics_dir)
        self._metrics: Dict[str, Any] = {
            "requests_total": 0,
            "requests_multimodal": 0,
            "requests_text": 0,
            "backpressure_events": 0,
            "deadline_backpressure_events": 0,
            "reject_events": 0,
            "deadline_reject_events": 0,
            "handoff_prepared": 0,
            "handoff_committed": 0,
            "handoff_rolled_back": 0,
            "handoff_prepare_ms": [],
            "handoff_commit_ms": [],
            "handoff_rollback_ms": [],
            "deadline_miss_risk": [],
            "degrade_level_counts": {},
            "transport_backend_counts": {},
            "mm_prefetch_announced": 0,
            "mm_prefetch_attempted": 0,
            "mm_prefetch_completed": 0,
            "mm_prefetch_failed": 0,
            "mm_prefetch_worker_cache_hits": 0,
            "mm_prefetch_shared_store_hits": 0,
            "mm_prefetch_recomputed": 0,
            "mm_prefetch_bytes": 0,
            "mm_prefetch_wait_ms": [],
            "serving_cross_step_reuse_candidates": 0,
            "serving_cross_step_reused_tokens": 0,
            "serving_workflow_state_commits": 0,
            "serving_workflow_affinity_hits": 0,
            "pd_transport_affinity_candidates": 0,
            "pd_transport_affinity_hits": 0,
            "pd_transport_affinity_fallbacks": 0,
            "agent_pd_route_total": 0,
            "agent_pd_route_correct": 0,
            "agent_pd_route_incorrect": 0,
            "agent_pd_by_type": {},
            "agent_pd_stage_pool_counts": {
                "prefill": {},
                "decode": {},
            },
            "agent_pd_hybrid_fast_path": 0,
            "agent_pd_route_decisions": [],
            "agent_state_clone_requests": 0,
            "agent_state_clone_branches": 0,
            "agent_state_clone_zero_copy_branches": 0,
            "agent_state_clone_copied_bytes": 0,
            "agent_state_clone_failures": 0,
            "agent_state_clone_retained_blocks": 0,
            "agent_state_consume_requests": 0,
            "agent_state_consume_success": 0,
            "agent_state_materialize_requests": 0,
            "agent_state_materialize_success": 0,
            "agent_state_materialize_failures": 0,
            "agent_state_expired_releases": 0,
            "path_stats": {
                "PD": self._new_path_stats(),
                "EPD": self._new_path_stats(),
            },
        }

    # ------------------------------------------------------------------
    # Worker registration / accounting
    # ------------------------------------------------------------------
    def register_stage_workers(self, stage: str, worker_ids: Sequence[str]) -> None:
        scheduler = self._scheduler_for_stage(stage)
        existing = {worker.worker_id for worker in scheduler.workers}
        total = len(worker_ids)
        for ordinal, worker_id in enumerate(worker_ids):
            worker_id = str(worker_id)
            if worker_id in existing:
                continue
            scheduler.add_worker(
                WorkerLoad(
                    worker_id=worker_id,
                    worker_type=stage,
                    max_capacity=(
                        self.config.prefill_capacity if stage == "prefill" else self.config.decode_capacity
                    ),
                    queue_capacity=self.config.queue_capacity,
                    service_rate=(
                        self.config.prefill_service_rate if stage == "prefill" else self.config.decode_service_rate
                    ),
                    critical_rho=self.config.warn_rho,
                    pool_tags=self._pool_tags_for_worker(stage, worker_id, ordinal=ordinal, total=total),
                )
            )
            existing.add(worker_id)

    def update_worker_load(self, stage: str, worker_id: str, **fields: Any) -> None:
        scheduler = self._scheduler_for_stage(stage)
        if "critical_rho" not in fields:
            fields["critical_rho"] = self.config.warn_rho
        scheduler.update_load(worker_id, **fields)

    def stage_workers(self, stage: str) -> List[WorkerLoad]:
        return list(self._scheduler_for_stage(stage).workers)

    @staticmethod
    def _normalize_prefill_decode_affinity(
        pairs: Sequence[Tuple[str, str]] | Sequence[str],
    ) -> Dict[str, str]:
        """Validate a stable Prefill -> Decode transport-affinity map.

        A worker may have one preferred Decode peer. Keeping the map explicit
        makes physical topology a deployment concern rather than an accidental
        side effect of independent least-load scheduling.
        """

        normalized: Dict[str, str] = {}
        for raw_pair in pairs or ():
            if isinstance(raw_pair, str):
                source, separator, target = raw_pair.partition("=")
                if not separator:
                    raise ValueError(
                        "prefill/decode affinity entries must use prefill=decode"
                    )
            else:
                try:
                    source, target = raw_pair
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "prefill/decode affinity entries must contain two worker ids"
                    ) from exc
            source = str(source).strip()
            target = str(target).strip()
            if not source or not target:
                raise ValueError(
                    "prefill/decode affinity worker ids must be non-empty"
                )
            previous = normalized.get(source)
            if previous is not None and previous != target:
                raise ValueError(
                    f"prefill worker {source!r} has conflicting decode affinity "
                    f"targets {previous!r} and {target!r}"
                )
            normalized[source] = target
        return normalized

    def preferred_decode_worker_for_prefill(
        self, prefill_worker_id: Optional[str]
    ) -> Optional[str]:
        """Return the configured Decode peer only if it is registered."""

        if not prefill_worker_id:
            return None
        target = self._prefill_decode_affinity.get(str(prefill_worker_id))
        if not target:
            return None
        if any(worker.worker_id == target for worker in self._decode_scheduler.workers):
            return target
        return None

    # ------------------------------------------------------------------
    # Request classification / control metadata
    # ------------------------------------------------------------------
    def start_request(self, req_data: Dict[str, Any], request_id: str) -> RequestContext:
        modality, mm_hashes = self.classify_request(req_data)
        workflow_id = self._workflow_id(req_data, request_id)
        routing_path = "EPD" if modality == "multimodal" else "PD"
        ctx = RequestContext(
            request_id=request_id,
            workflow_id=workflow_id,
            modality=modality,
            routing_path=routing_path,
            mm_hashes=mm_hashes,
            estimated_input_tokens=self.estimate_input_tokens(req_data),
            deadline_at=self._extract_deadline(req_data),
            token_ids=self._request_token_ids(req_data),
            transfer_id=request_id,
            agent_type_hint=self._agent_type_hint(req_data),
            priority=self._request_priority(req_data),
            expected_output_tokens=self._expected_output_tokens(req_data),
        )
        ctx.agent_pd_profile = self._agent_pd_profile(req_data, ctx)
        self._prepare_cross_step_reuse(ctx)
        with self._lock:
            self._request_ctx[request_id] = ctx
            self._metrics["requests_total"] += 1
            self._record_path_metric(ctx.routing_path, "requests_total")
            if modality == "multimodal":
                self._metrics["requests_multimodal"] += 1
                self._metrics["mm_prefetch_announced"] += len(mm_hashes)
                self._record_path_metric(ctx.routing_path, "requests_multimodal")
                self._record_path_metric(
                    ctx.routing_path,
                    "mm_prefetch_announced",
                    delta=len(mm_hashes),
                )
            else:
                self._metrics["requests_text"] += 1
                self._record_path_metric(ctx.routing_path, "requests_text")
        self._sync_request_registry(
            ctx,
            status="ACTIVE",
            event="SERVING_REQUEST_STARTED",
            agent_id=self.config.node_id,
            step_index=0,
        )
        return ctx

    def classify_request(self, req_data: Dict[str, Any]) -> Tuple[str, List[str]]:
        hashes: List[str] = []
        found_multimodal = False
        for item in self._iter_content_items(req_data):
            item_type = str(item.get("type", "text"))
            if item_type in MULTIMODAL_CONTENT_TYPES:
                found_multimodal = True
                hashes.append(self._stable_mm_hash(item))
        return ("multimodal" if found_multimodal else "text", hashes)

    def estimate_input_tokens(self, req_data: Dict[str, Any]) -> int:
        pieces: List[str] = []
        for item in self._iter_content_items(req_data):
            item_type = str(item.get("type", "text"))
            if item_type in TEXTUAL_CONTENT_TYPES:
                pieces.append(str(item.get("text") or item.get("content") or ""))
        if not pieces and isinstance(req_data.get("prompt"), str):
            pieces.append(str(req_data["prompt"]))
        text = " ".join(pieces).strip()
        if not text:
            return 0
        return max(1, len(text.split()))

    def _request_token_ids(self, req_data: Dict[str, Any]) -> List[int]:
        pieces: List[str] = []
        for item in self._iter_content_items(req_data):
            item_type = str(item.get("type", "text"))
            if item_type in TEXTUAL_CONTENT_TYPES:
                pieces.append(str(item.get("text") or item.get("content") or ""))
        if not pieces and isinstance(req_data.get("prompt"), str):
            pieces.append(str(req_data["prompt"]))
        tokens: List[int] = []
        for word in " ".join(pieces).strip().split():
            digest = hashlib.sha256(word.encode("utf-8")).digest()
            tokens.append(int.from_bytes(digest[:4], "big", signed=False))
        return tokens

    def build_prefill_kv_params(
        self,
        ctx: RequestContext,
        decision: StageRouteDecision,
        *,
        decode_worker_id: Optional[str] = None,
        base_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        ctx.prefill_worker_id = decision.worker_id
        # Keep the producer-side target hint aligned with the Decode admission
        # preference. Independent least-load choices can otherwise invert an
        # explicitly configured GPU pair and bypass the fastest P2P link.
        paired_decode = self.preferred_decode_worker_for_prefill(decision.worker_id)
        if paired_decode:
            decode_worker_id = paired_decode
            ctx.decode_affinity_worker_id = paired_decode
            with self._lock:
                self._metrics["pd_transport_affinity_candidates"] += 1
        else:
            ctx.decode_affinity_worker_id = None
        ctx.admission_action = decision.decision.action
        ctx.degrade_level = decision.decision.degrade_level
        ctx.prefill_rho = decision.decision.rho
        ctx.decode_worker_id = decode_worker_id
        self._set_workflow_affinity(ctx.workflow_id, decision.worker_id)
        kv = dict(base_params or {})
        kv.update(
            self._base_control_fields(
                ctx=ctx,
                stage="prefill",
                worker_id=decision.worker_id,
                rho=decision.decision.rho,
                action=decision.decision.action,
                degrade=decision.decision.degrade_level,
            )
        )
        kv.update(
            {
                "do_remote_decode": True,
                "do_remote_prefill": False,
                "a2a_source_node": decision.worker_id,
                "a2a_target_node": decode_worker_id,
            }
        )
        self._sync_request_registry(
            ctx,
            status="PREFILL_DISPATCHED",
            event="SERVING_PREFILL_DISPATCHED",
            agent_id=decision.worker_id,
            step_index=1,
            target_agent_id=decode_worker_id,
            target_node_id=decode_worker_id,
        )
        return kv

    def note_prefill_response(
        self,
        ctx: RequestContext,
        kv_transfer_params: Optional[Dict[str, Any]],
        *,
        decode_worker_id: Optional[str],
    ) -> Dict[str, Any]:
        kv_transfer_params = dict(kv_transfer_params or {})
        missing = [
            field_name
            for field_name in ("remote_engine_id", "remote_bootstrap_addr")
            if not kv_transfer_params.get(field_name)
        ]
        if missing:
            raise RuntimeError(
                "prefill response missing required KV handoff fields: "
                + ", ".join(missing)
            )
        remote_engine_id = str(kv_transfer_params["remote_engine_id"])
        ctx.transfer_id = str(kv_transfer_params.get("transfer_id") or ctx.transfer_id or ctx.request_id)
        block_ids = self._normalize_block_ids(
            kv_transfer_params.get("remote_block_ids"),
            namespace=remote_engine_id,
        )
        if not block_ids:
            raise RuntimeError(
                "prefill response missing remote_block_ids; refusing to commit "
                "directory-managed P→D handoff without concrete KV block metadata"
            )
        ctx.block_ids = block_ids
        source_node_id = str(ctx.prefill_worker_id or "prefill")
        target_node_id = str(decode_worker_id or ctx.decode_worker_id or "decode")
        ctx.placeholder_block_ids = self._ensure_block_records(
            block_ids=block_ids,
            workflow_id=ctx.workflow_id,
            owner_shard=source_node_id,
            physical_node_id=source_node_id,
        )
        started = time.perf_counter()
        handoff_id = uuid.uuid4().hex
        self.kv_directory.begin_handoff(
            handoff_id=handoff_id,
            state_id=ctx.request_id,
            workflow_id=ctx.workflow_id,
            source_agent_id=self.config.source_agent_id,
            target_agent_id=self.config.target_agent_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            block_ids=block_ids,
            feature_hashes=list(ctx.mm_hashes),
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        ctx.handoff_id = handoff_id
        with self._lock:
            self._metrics["handoff_prepared"] += 1
            self._metrics["handoff_prepare_ms"].append(elapsed_ms)
            self._record_path_metric(ctx.routing_path, "handoff_prepared")
        ctx.reuse_telemetry = {
            **dict(ctx.reuse_telemetry),
            "kv_handoff": self._kv_handoff_payload(
                kv_transfer_params,
                block_ids=block_ids,
                source_node_id=source_node_id,
                target_node_id=target_node_id,
            ),
        }
        self._sync_request_registry(
            ctx,
            status="HANDING_OVER",
            event="SERVING_HANDOFF_PREPARED",
            agent_id=source_node_id,
            step_index=2,
            handoff_id=handoff_id,
            target_agent_id=target_node_id,
            target_node_id=target_node_id,
        )
        kv_transfer_params["handoff_id"] = handoff_id
        kv_transfer_params["epd_non_directory_managed_kv"] = False
        kv_transfer_params["a2a_source_node"] = source_node_id
        kv_transfer_params["a2a_target_node"] = target_node_id
        kv_transfer_params["workflow_id"] = ctx.workflow_id
        self._commit_workflow_latest(ctx)
        return kv_transfer_params

    def build_decode_kv_params(
        self,
        ctx: RequestContext,
        decision: StageRouteDecision,
        upstream_params: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        ctx.decode_worker_id = decision.worker_id
        ctx.decode_rho = decision.decision.rho
        ctx.admission_action = decision.decision.action
        if decision.decision.degrade_level is not DegradeLevel.NONE:
            ctx.degrade_level = decision.decision.degrade_level
        kv = dict(upstream_params or {})
        required_remote_fields = ("remote_engine_id", "remote_bootstrap_addr", "transfer_id")
        missing = [field_name for field_name in required_remote_fields if not kv.get(field_name)]
        if missing:
            raise RuntimeError(
                "decode request missing required remote KV fields from prefill: "
                + ", ".join(missing)
            )
        kv.update(
            self._base_control_fields(
                ctx=ctx,
                stage="decode",
                worker_id=decision.worker_id,
                rho=decision.decision.rho,
                action=decision.decision.action,
                degrade=decision.decision.degrade_level,
            )
        )
        kv["transfer_id"] = str(kv.get("transfer_id") or ctx.transfer_id or ctx.request_id)
        kv["do_remote_prefill"] = True
        kv["do_remote_decode"] = False
        kv["handoff_id"] = ctx.handoff_id
        kv["a2a_source_node"] = ctx.prefill_worker_id
        kv["a2a_target_node"] = decision.worker_id
        self._sync_request_registry(
            ctx,
            status="DECODE_DISPATCHED",
            event="SERVING_DECODE_DISPATCHED",
            agent_id=decision.worker_id,
            step_index=3,
            handoff_id=ctx.handoff_id,
            target_agent_id=decision.worker_id,
            target_node_id=decision.worker_id,
        )
        return kv

    # ------------------------------------------------------------------
    # Admission / backpressure
    # ------------------------------------------------------------------
    def admit_stage(self, stage: str, ctx: RequestContext) -> StageRouteDecision:
        scheduler = self._scheduler_for_stage(stage)
        if not scheduler.workers:
            raise RuntimeError(f"no registered workers for stage={stage}")
        self._decay_arrival_rates(scheduler)
        agent_type = self._agent_type_for(stage, ctx)
        if stage == "prefill":
            preferred_worker_id = self._preferred_worker_for(ctx.workflow_id)
        elif stage == "decode":
            preferred_worker_id = ctx.decode_affinity_worker_id
        else:
            preferred_worker_id = None
        req = AgentRequest(
            request_id=f"{ctx.request_id}:{stage}",
            agent_type=agent_type,
            input_tokens=max(1, int(ctx.agent_pd_profile.get("input_tokens") or ctx.estimated_input_tokens)),
            output_tokens=max(0, int(ctx.agent_pd_profile.get("expected_output_tokens") or ctx.expected_output_tokens)),
            priority=ctx.priority,
            deadline=ctx.deadline_at,
            metadata={
                "modality": ctx.modality,
                "routing_path": ctx.routing_path,
                "workflow_id": ctx.workflow_id,
                "preferred_worker_id": preferred_worker_id,
                "stage": stage,
                "preferred_pool": self._preferred_pool_for_stage(stage, ctx),
                "avoid_pool": self._avoid_pool_for_stage(stage, ctx),
                "routing_target": ctx.agent_pd_profile.get("routing_target"),
                "agent_pd_profile": dict(ctx.agent_pd_profile),
            },
        )
        decision = scheduler.admit(req)
        if decision.worker is None:
            with self._lock:
                self._metrics["reject_events"] += 1
                self._record_path_metric(ctx.routing_path, "reject_events")
                self._record_path_stage_metric(
                    ctx.routing_path,
                    stage,
                    "stage_reject_events",
                )
            raise RuntimeError(f"stage={stage} admission failed: no worker")
        if decision.action is AdmissionAction.REJECT:
            self._release_if_admitted(decision)
            with self._lock:
                self._metrics["reject_events"] += 1
                self._record_path_metric(ctx.routing_path, "reject_events")
                if decision.deadline_slack_ms is not None:
                    self._metrics["deadline_reject_events"] += 1
                    self._record_path_metric(ctx.routing_path, "deadline_reject_events")
                self._record_path_stage_metric(
                    ctx.routing_path,
                    stage,
                    "stage_reject_events",
                )
            raise RuntimeError(f"stage={stage} rejected: {decision.reason}")
        preferred_worker_id = req.metadata.get("preferred_worker_id") if isinstance(req.metadata, dict) else None
        if preferred_worker_id and decision.worker is not None and str(decision.worker.worker_id) == str(preferred_worker_id):
            with self._lock:
                if stage == "prefill":
                    self._metrics["serving_workflow_affinity_hits"] += 1
                    self._record_path_metric(ctx.routing_path, "serving_workflow_affinity_hits")
                elif stage == "decode" and ctx.decode_affinity_worker_id:
                    self._metrics["pd_transport_affinity_hits"] += 1
        elif stage == "decode" and ctx.decode_affinity_worker_id:
            with self._lock:
                self._metrics["pd_transport_affinity_fallbacks"] += 1
        wait_ms = 0.0
        with self._lock:
            self._metrics["deadline_miss_risk"].append(
                float(max(0.0, min(1.0, decision.deadline_miss_risk)))
            )
            self._record_path_stage_metric(
                ctx.routing_path,
                stage,
                "stage_dispatches",
            )
            if decision.action is AdmissionAction.BACKPRESSURE:
                wait_ms = self._backpressure_delay_ms(decision)
                if decision.deadline_slack_ms is not None and "deadline" in decision.reason:
                    wait_ms = min(wait_ms, max(0.0, decision.estimated_wait_ms * 0.25))
                self._metrics["backpressure_events"] += 1
                self._record_path_metric(ctx.routing_path, "backpressure_events")
                self._record_path_stage_metric(
                    ctx.routing_path,
                    stage,
                    "stage_backpressure_events",
                )
                if decision.deadline_slack_ms is not None:
                    self._metrics["deadline_backpressure_events"] += 1
                    self._record_path_metric(ctx.routing_path, "deadline_backpressure_events")
        self._record_degrade(decision.degrade_level)
        self._record_backend(self.config.transport_backend)
        self._record_arrival(decision.worker.worker_id)
        self._record_agent_pd_decision(stage, ctx, decision)
        return StageRouteDecision(
            worker_id=decision.worker.worker_id,
            decision=decision,
            wait_ms=wait_ms,
        )

    def mark_stage_complete(
        self,
        stage: str,
        worker_id: str,
        *,
        latency_ms: float,
        success: bool = True,
    ) -> None:
        scheduler = self._scheduler_for_stage(stage)
        worker = next((w for w in scheduler.workers if w.worker_id == worker_id), None)
        if worker is None:
            return
        latency_ms = max(0.0, float(latency_ms))
        worker.current_load = max(0, worker.current_load - 1)
        worker.queue_size = max(0, worker.queue_size - 1)
        if latency_ms > 0:
            if worker.avg_latency_ms <= 0:
                worker.avg_latency_ms = latency_ms
            else:
                worker.avg_latency_ms = worker.avg_latency_ms * 0.8 + latency_ms * 0.2
            inst_service_rate = 1000.0 / latency_ms
            worker.service_rate = max(1e-6, worker.service_rate * 0.8 + inst_service_rate * 0.2)
        if not success:
            worker.arrival_rate = max(0.0, worker.arrival_rate * 0.9)

    # ------------------------------------------------------------------
    # A2A 2PC bookkeeping
    # ------------------------------------------------------------------
    def commit_handoff(self, ctx: RequestContext) -> None:
        if not ctx.handoff_id or ctx.handoff_committed:
            return
        started = time.perf_counter()
        self.kv_directory.commit_handoff(ctx.handoff_id)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        ctx.handoff_committed = True
        with self._lock:
            self._metrics["handoff_committed"] += 1
            self._metrics["handoff_commit_ms"].append(elapsed_ms)
            self._record_path_metric(ctx.routing_path, "handoff_committed")
        self._sync_request_registry(
            ctx,
            status="ACTIVE",
            event="SERVING_HANDOFF_COMMITTED",
            agent_id=str(ctx.decode_worker_id or self.config.target_agent_id),
            step_index=4,
            handoff_id=None,
            target_agent_id=None,
            target_node_id=None,
        )

    def rollback_handoff(self, ctx: RequestContext) -> None:
        if not ctx.handoff_id or ctx.handoff_committed:
            return
        started = time.perf_counter()
        self.kv_directory.rollback_handoff(ctx.handoff_id)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        with self._lock:
            self._metrics["handoff_rolled_back"] += 1
            self._metrics["handoff_rollback_ms"].append(elapsed_ms)
            self._record_path_metric(ctx.routing_path, "handoff_rolled_back")
        self._sync_request_registry(
            ctx,
            status="ROLLED_BACK",
            event="SERVING_HANDOFF_ROLLED_BACK",
            agent_id=str(ctx.prefill_worker_id or self.config.source_agent_id),
            step_index=4,
            handoff_id=ctx.handoff_id,
            target_agent_id=str(ctx.decode_worker_id or self.config.target_agent_id),
            target_node_id=str(ctx.decode_worker_id or self.config.target_agent_id),
        )

    def finish_request(self, request_id: str) -> Optional[RequestContext]:
        with self._lock:
            ctx = self._request_ctx.pop(request_id, None)
        if ctx is None:
            return None
        with self._lock:
            self._record_path_metric(ctx.routing_path, "requests_finished")
        self._sync_request_registry(
            ctx,
            status="COMPLETED" if ctx.handoff_committed else "RELEASED",
            event="SERVING_REQUEST_COMPLETED" if ctx.handoff_committed else "SERVING_REQUEST_RELEASED",
            agent_id=str(ctx.decode_worker_id or ctx.prefill_worker_id or self.config.node_id),
            step_index=5,
            handoff_id=None,
            target_agent_id=None,
            target_node_id=None,
        )
        if self.workflow_registry is not None:
            self.workflow_registry.mark_released(ctx.request_id)
        if ctx.handoff_id:
            self.kv_directory.clear_handoff(ctx.handoff_id)
        for gid in ctx.placeholder_block_ids:
            if self._is_workflow_pinned_block(gid):
                with self._lock:
                    self._metrics["agent_state_clone_retained_blocks"] += 1
                continue
            self.kv_directory.drop_block_record(gid, only_placeholder=True)
        return ctx

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        now = time.monotonic()
        connector_aggregate = self._connector_metrics.aggregate()
        mm_hidden_cache = self._connector_metrics.aggregate_mm_hidden_cache()
        decode_engine_timing = self._connector_metrics.aggregate_decode_engine_timing()
        connector_totals = connector_aggregate.totals
        with self._lock:
            handoff_records = (
                self.kv_directory.handoff_states()
                if hasattr(self.kv_directory, "handoff_states")
                else {}
            )
            handoff_states = {
                hid: record.status
                for hid, record in handoff_records.items()
            }
            return {
                "config": {
                    "node_id": self.config.node_id,
                    "layered_kv_transfer": self.config.layered_kv_transfer,
                    "layers_per_group": self.config.layers_per_group,
                    "group_delay_ms": self.config.group_delay_ms,
                    "transport_backend": self.config.transport_backend,
                    "connector_metrics_dir": self.config.connector_metrics_dir,
                    "workflow_registry_wal_path": self.config.workflow_registry_wal_path,
                    "owner_shards": int(self.config.owner_shards),
                    "kv_directory_rpc_url": self.config.kv_directory_rpc_url,
                    "enable_workflow_affinity": self.config.enable_workflow_affinity,
                    "strict_no_fallback": self.config.strict_no_fallback,
                    "enable_agent_state_clone": self.config.enable_agent_state_clone,
                    "agent_state_consume_requires_kv_handoff": self.config.agent_state_consume_requires_kv_handoff,
                    "mooncake_protocol": self.config.mooncake_protocol,
                    "warn_rho": self.config.warn_rho,
                    "critical_rho": self.config.critical_rho,
                    "high_prefill_worker_ids": list(self.config.high_prefill_worker_ids),
                    "low_latency_decode_worker_ids": list(self.config.low_latency_decode_worker_ids),
                    "standard_prefill_worker_ids": list(self.config.standard_prefill_worker_ids),
                    "standard_decode_worker_ids": list(self.config.standard_decode_worker_ids),
                    "prefill_decode_affinity": [
                        [source, target]
                        for source, target in sorted(self._prefill_decode_affinity.items())
                    ],
                },
                "metrics": {
                    **{
                        k: v
                        for k, v in self._metrics.items()
                        if k != "path_stats" and not isinstance(v, list)
                    },
                    "handoff_prepare_ms_avg": self._avg(self._metrics["handoff_prepare_ms"]),
                    "handoff_commit_ms_avg": self._avg(self._metrics["handoff_commit_ms"]),
                    "handoff_rollback_ms_avg": self._avg(self._metrics["handoff_rollback_ms"]),
                    "deadline_miss_risk_avg": self._avg(self._metrics["deadline_miss_risk"]),
                    "mm_prefetch_wait_ms_avg": self._avg(self._metrics["mm_prefetch_wait_ms"]),
                    "agent_pd_route_decisions_recent": list(self._metrics.get("agent_pd_route_decisions", []))[-256:],
                    "path_stats": self._snapshot_path_stats(),
                    "connector_path_stats": {
                        str(path): meta.to_dict()
                        for path, meta in sorted(connector_aggregate.path_totals.items())
                    },
                    "connector_metric_workers": connector_aggregate.workers,
                    "connector_metrics_updated_at": connector_aggregate.updated_at_max,
                    "layered_transfer_grouped_batches": connector_totals.grouped_batches,
                    "layered_transfer_grouped_bytes": connector_totals.grouped_bytes,
                    "layered_transfer_grouped_descriptors": connector_totals.grouped_descriptors,
                    "layered_transfer_failed_batches": connector_totals.failed_batches,
                    "peer_buffer_batches": connector_totals.peer_buffer_batches,
                    "peer_buffer_bytes": connector_totals.peer_buffer_bytes,
                    "peer_buffer_dispatch_ms": connector_totals.peer_buffer_dispatch_ms,
                    "peer_buffer_dispatch_ms_avg": (
                        connector_totals.peer_buffer_dispatch_ms
                        / connector_totals.peer_buffer_batches
                        if connector_totals.peer_buffer_batches
                        else 0.0
                    ),
                    "peer_buffer_prepare_ms": connector_totals.peer_buffer_prepare_ms,
                    "peer_buffer_prepare_ms_avg": (
                        connector_totals.peer_buffer_prepare_ms
                        / connector_totals.peer_buffer_batches
                        if connector_totals.peer_buffer_batches
                        else 0.0
                    ),
                    "peer_buffer_write_ms": connector_totals.peer_buffer_write_ms,
                    "peer_buffer_write_ms_avg": (
                        connector_totals.peer_buffer_write_ms
                        / connector_totals.peer_buffer_batches
                        if connector_totals.peer_buffer_batches
                        else 0.0
                    ),
                    "peer_buffer_write_bandwidth_gbps": (
                        connector_totals.peer_buffer_bytes
                        * 8.0
                        / connector_totals.peer_buffer_write_ms
                        / 1_000_000.0
                        if connector_totals.peer_buffer_write_ms > 0.0
                        else 0.0
                    ),
                    "fallback_batches": connector_totals.fallback_batches,
                    "fallback_bytes": connector_totals.fallback_bytes,
                    "layered_transfer_delay_ms": connector_totals.accumulated_group_delay_ms,
                    "layered_receive_group_batches": connector_totals.received_group_batches,
                    "layered_receive_finished_reqs": connector_totals.received_finished_reqs,
                    "layer_load_wait_calls": connector_totals.layer_wait_calls,
                    "layer_load_wait_ms": connector_totals.layer_wait_ms,
                    "layered_receive_failures": connector_totals.receive_failures,
                    "layered_receive_kv_requests": connector_totals.receive_kv_requests,
                    "layered_receive_kv_worker_roundtrips": connector_totals.receive_kv_worker_roundtrips,
                    "layered_receive_kv_worker_ms": connector_totals.receive_kv_worker_ms,
                    "layered_receive_kv_worker_ms_avg": (
                        connector_totals.receive_kv_worker_ms
                        / connector_totals.receive_kv_worker_roundtrips
                        if connector_totals.receive_kv_worker_roundtrips
                        else 0.0
                    ),
                    "layered_receive_kv_response_messages": connector_totals.receive_kv_response_messages,
                    "layered_receive_kv_first_response_count": connector_totals.receive_kv_first_response_count,
                    "layered_receive_kv_first_response_ms": connector_totals.receive_kv_first_response_ms,
                    "layered_receive_kv_first_response_ms_avg": (
                        connector_totals.receive_kv_first_response_ms
                        / connector_totals.receive_kv_first_response_count
                        if connector_totals.receive_kv_first_response_count
                        else 0.0
                    ),
                    "layered_receive_kv_last_response_count": connector_totals.receive_kv_last_response_count,
                    "layered_receive_kv_last_response_ms": connector_totals.receive_kv_last_response_ms,
                    "layered_receive_kv_last_response_ms_avg": (
                        connector_totals.receive_kv_last_response_ms
                        / connector_totals.receive_kv_last_response_count
                        if connector_totals.receive_kv_last_response_count
                        else 0.0
                    ),
                    "layered_receive_kv_response_process_count": connector_totals.receive_kv_response_process_count,
                    "layered_receive_kv_response_process_ms": connector_totals.receive_kv_response_process_ms,
                    "layered_receive_kv_response_process_ms_avg": (
                        connector_totals.receive_kv_response_process_ms
                        / connector_totals.receive_kv_response_process_count
                        if connector_totals.receive_kv_response_process_count
                        else 0.0
                    ),
                    "layered_receive_kv_first_group_count": connector_totals.receive_kv_first_group_count,
                    "layered_receive_kv_first_group_ms": connector_totals.receive_kv_first_group_ms,
                    "layered_receive_kv_first_group_ms_avg": (
                        connector_totals.receive_kv_first_group_ms
                        / connector_totals.receive_kv_first_group_count
                        if connector_totals.receive_kv_first_group_count
                        else 0.0
                    ),
                    "layered_receive_kv_finished_count": connector_totals.receive_kv_finished_count,
                    "layered_receive_kv_finished_ms": connector_totals.receive_kv_finished_ms,
                    "layered_receive_kv_finished_ms_avg": (
                        connector_totals.receive_kv_finished_ms
                        / connector_totals.receive_kv_finished_count
                        if connector_totals.receive_kv_finished_count
                        else 0.0
                    ),
                    "remote_transfer_backend_counts": dict(connector_totals.backend_counts),
                    "mm_hidden_cache_workers": int(mm_hidden_cache.get("workers", 0) or 0),
                    "mm_hidden_cache_enabled_workers": int(mm_hidden_cache.get("enabled_workers", 0) or 0),
                    "mm_hidden_cache_lookups": int(mm_hidden_cache.get("lookups", 0) or 0),
                    "mm_hidden_cache_hits": int(mm_hidden_cache.get("hits", 0) or 0),
                    "mm_hidden_cache_misses": int(mm_hidden_cache.get("misses", 0) or 0),
                    "mm_hidden_cache_stores": int(mm_hidden_cache.get("stores", 0) or 0),
                    "mm_hidden_cache_evictions": int(mm_hidden_cache.get("evictions", 0) or 0),
                    "mm_hidden_cache_bytes": int(mm_hidden_cache.get("bytes", 0) or 0),
                    "mm_hidden_cache_entries": int(mm_hidden_cache.get("entries", 0) or 0),
                    "mm_hidden_cache_hit_rate": float(mm_hidden_cache.get("hit_rate", 0.0) or 0.0),
                    "mm_hidden_cache_stable_key_lookups": int(mm_hidden_cache.get("stable_key_lookups", 0) or 0),
                    "mm_hidden_cache_tensor_key_lookups": int(mm_hidden_cache.get("tensor_key_lookups", 0) or 0),
                    "mm_hidden_cache_skipped_unkeyed_calls": int(mm_hidden_cache.get("skipped_unkeyed_calls", 0) or 0),
                    "mm_hidden_cache_native_encoder_cache_hits": int(mm_hidden_cache.get("native_encoder_cache_hits", 0) or 0),
                    "mm_hidden_cache_precomputed_image_embeds_hits": int(mm_hidden_cache.get("precomputed_image_embeds_hits", 0) or 0),
                    "mm_hidden_cache_partial_hit_batches": int(mm_hidden_cache.get("partial_hit_batches", 0) or 0),
                    "mm_hidden_cache_full_hit_batches": int(mm_hidden_cache.get("full_hit_batches", 0) or 0),
                    "mm_hidden_cache_full_miss_batches": int(mm_hidden_cache.get("full_miss_batches", 0) or 0),
                    "mm_hidden_cache_vision_compute_ms_avg": float(
                        mm_hidden_cache.get("vision_compute_ms_avg", 0.0) or 0.0
                    ),
                    "mm_hidden_cache_load_ms_avg": float(
                        mm_hidden_cache.get("cache_load_ms_avg", 0.0) or 0.0
                    ),
                    "mm_hidden_cache_errors": int(mm_hidden_cache.get("errors", 0) or 0),
                    "decode_engine_timing_workers": int(decode_engine_timing.get("workers", 0) or 0),
                    "decode_engine_first_token_requests": int(
                        decode_engine_timing.get("first_token_requests", 0) or 0
                    ),
                    "decode_engine_first_token_latency_ms_total": float(
                        decode_engine_timing.get("first_token_latency_ms_total", 0.0) or 0.0
                    ),
                    "decode_engine_first_token_latency_ms_avg": float(
                        decode_engine_timing.get("first_token_latency_ms_avg", 0.0) or 0.0
                    ),
                    "decode_engine_kv_first_token_requests": int(
                        decode_engine_timing.get("kv_first_token_requests", 0) or 0
                    ),
                    "decode_engine_kv_first_token_latency_ms_total": float(
                        decode_engine_timing.get("kv_first_token_latency_ms_total", 0.0)
                        or 0.0
                    ),
                    "decode_engine_kv_first_token_latency_ms_avg": float(
                        decode_engine_timing.get("kv_first_token_latency_ms_avg", 0.0) or 0.0
                    ),
                    "decode_engine_kv_first_token_output_tokens": int(
                        decode_engine_timing.get("kv_first_token_output_tokens", 0) or 0
                    ),
                    "decode_engine_scheduler_update_calls": int(
                        decode_engine_timing.get("scheduler_update_calls", 0) or 0
                    ),
                    "decode_engine_timing_updated_at": float(
                        decode_engine_timing.get("updated_at_max", 0.0) or 0.0
                    ),
                },
                "workers": {
                    "prefill": [
                        self._worker_to_dict(
                            w,
                            rho=self._predict_worker_rho(
                                scheduler=self._prefill_scheduler,
                                worker=w,
                                now=now,
                            ),
                        )
                        for w in self._prefill_scheduler.workers
                    ],
                    "decode": [
                        self._worker_to_dict(
                            w,
                            rho=self._predict_worker_rho(
                                scheduler=self._decode_scheduler,
                                worker=w,
                                now=now,
                            ),
                        )
                        for w in self._decode_scheduler.workers
                    ],
                },
                "active_requests": sorted(self._request_ctx.keys()),
                "handoffs": handoff_states,
                "workflow_registry": self._workflow_registry_snapshot(),
            }


    def record_mm_prefetch_result(
        self,
        ctx: RequestContext,
        *,
        attempted: int = 0,
        completed: int = 0,
        failed: int = 0,
        worker_cache_hits: int = 0,
        shared_store_hits: int = 0,
        recomputed: int = 0,
        bytes_transferred: int = 0,
        wait_ms: float = 0.0,
    ) -> None:
        """Record real serving-path MM prefetch outcomes.

        The proxy performs the async fetch/cache/rewrite work because it owns the
        OpenAI request body. The control plane keeps the counters so RFC §8
        reports can correlate prefetch health with TTFT/goodput.
        """
        with self._lock:
            updates = {
                "mm_prefetch_attempted": attempted,
                "mm_prefetch_completed": completed,
                "mm_prefetch_failed": failed,
                "mm_prefetch_worker_cache_hits": worker_cache_hits,
                "mm_prefetch_shared_store_hits": shared_store_hits,
                "mm_prefetch_recomputed": recomputed,
                "mm_prefetch_bytes": bytes_transferred,
            }
            for key, delta in updates.items():
                self._metrics[key] += int(delta)
                if delta:
                    self._record_path_metric(ctx.routing_path, key, delta=int(delta))
            if wait_ms > 0:
                self._metrics["mm_prefetch_wait_ms"].append(float(wait_ms))
                path_stats = self._ensure_path_stats(ctx.routing_path)
                path_stats["mm_prefetch_wait_ms_total"] = (
                    float(path_stats.get("mm_prefetch_wait_ms_total", 0.0) or 0.0) + float(wait_ms)
                )
                path_stats["mm_prefetch_wait_ms_count"] = (
                    int(path_stats.get("mm_prefetch_wait_ms_count", 0) or 0) + 1
                )

    # ------------------------------------------------------------------
    # Agent State Cloning / branch control
    # ------------------------------------------------------------------
    @staticmethod
    def _kv_handoff_payload(
        kv_transfer_params: Dict[str, Any],
        *,
        block_ids: Sequence[str],
        source_node_id: str,
        target_node_id: str,
    ) -> Dict[str, Any]:
        """Return serializable metadata needed to consume remote KV later.

        This is intentionally a descriptor, not copied tensor data.  A later
        Agent branch can use it to ask a real vLLM Mooncake connector to pull
        the same remote blocks, provided the producing engine still owns/pins
        those blocks.  Missing required fields fail closed at consumption time.
        """

        kv = dict(kv_transfer_params or {})
        keep = {
            "remote_engine_id",
            "remote_bootstrap_addr",
            "remote_block_ids",
            "tp_size",
            "remote_prefill_prompt_tokens",
            "remote_prefill_block_counts",
            "mooncake_protocol",
            "transport_backend",
            "layered_kv_transfer",
            "layers_per_group",
            "group_delay_ms",
            "max_group_bytes",
            "routing_path",
        }
        payload = {key: kv.get(key) for key in keep if kv.get(key) is not None}
        payload.update(
            {
                "source_node_id": str(source_node_id),
                "target_node_id": str(target_node_id),
                "normalized_kv_block_ids": list(block_ids),
                "created_at": time.monotonic(),
            }
        )
        return payload

    @staticmethod
    def _extract_kv_handoff_from_record(record: WorkflowStateRecord) -> Dict[str, Any]:
        telemetry = dict(record.reuse_telemetry or {})
        handoff = telemetry.get("kv_handoff")
        return dict(handoff) if isinstance(handoff, dict) else {}

    def _build_agent_state_consume_kv_params(
        self,
        *,
        record: WorkflowStateRecord,
        target_node_id: str,
        transfer_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        handoff = self._extract_kv_handoff_from_record(record)
        required = ("remote_engine_id", "remote_bootstrap_addr", "remote_block_ids")
        missing = [name for name in required if not handoff.get(name)]
        if missing and self.config.agent_state_consume_requires_kv_handoff:
            raise RuntimeError(
                "agent state cannot be consumed by vLLM hot path because KV handoff metadata is missing: "
                + ", ".join(missing)
            )
        kv = dict(handoff)
        kv.update(
            {
                "do_remote_prefill": True,
                "do_remote_decode": False,
                "transfer_id": str(transfer_id or f"agent-state-{record.state_id}-{uuid.uuid4().hex}"),
                "workflow_id": record.workflow_id,
                "agent_state_id": record.state_id,
                "a2a_source_node": str(handoff.get("source_node_id") or record.agent_id),
                "a2a_target_node": str(target_node_id),
                "epd_agent_state_consume": True,
                "epd_non_directory_managed_kv": False,
                "routing_path": "AGENT_STATE",
                "mooncake_protocol": str(kv.get("mooncake_protocol") or self.config.mooncake_protocol),
                "transport_backend": str(kv.get("transport_backend") or self.config.transport_backend),
                "layered_kv_transfer": bool(kv.get("layered_kv_transfer", self.config.layered_kv_transfer)),
                "layers_per_group": int(kv.get("layers_per_group", self.config.layers_per_group)),
                "group_delay_ms": float(kv.get("group_delay_ms", self.config.group_delay_ms)),
                "max_group_bytes": int(kv.get("max_group_bytes", self.config.max_group_bytes)),
            }
        )
        return kv

    def consume_agent_state(
        self,
        ctx: RequestContext,
        *,
        state_id: str,
        target_node_id: str,
        for_write: bool = False,
    ) -> Dict[str, Any]:
        """Prepare a real vLLM decode request to consume a cloned Agent KV state.

        Read-only cross-node consumption returns connector metadata that can pull
        remote KV blocks through Mooncake.  Write-intent still requires physical
        materialization/CoW and therefore fails closed unless a lower-level
        materializer is integrated.
        """

        with self._lock:
            self._metrics["agent_state_consume_requests"] += 1
        mat = self.materialize_agent_state(
            state_id=state_id,
            target_node_id=target_node_id,
            for_write=for_write,
        )
        registry = self.workflow_registry
        if registry is None:
            raise KeyError("workflow registry is not enabled")
        record = registry.get_record(state_id)
        if record is None:
            raise KeyError(f"unknown agent state_id={state_id}")
        kv = self._build_agent_state_consume_kv_params(
            record=record,
            target_node_id=target_node_id,
        )
        block_ids = list(record.kv_block_ids)
        source_node = str(kv.get("a2a_source_node") or record.agent_id or self.config.source_agent_id)
        handoff_id = uuid.uuid4().hex
        self.kv_directory.begin_handoff(
            handoff_id=handoff_id,
            state_id=state_id,
            workflow_id=record.workflow_id,
            source_agent_id=source_node,
            target_agent_id=str(target_node_id),
            source_node_id=source_node,
            target_node_id=str(target_node_id),
            block_ids=block_ids,
            feature_hashes=list(record.feature_hashes),
        )
        kv["handoff_id"] = handoff_id
        ctx.agent_state_id = state_id
        ctx.workflow_id = record.workflow_id or ctx.workflow_id
        ctx.block_ids = block_ids
        ctx.mm_hashes = list(record.feature_hashes or record.image_ids or ctx.mm_hashes)
        ctx.token_ids = list(record.token_ids or ctx.token_ids)
        ctx.transfer_id = str(kv["transfer_id"])
        ctx.decode_worker_id = str(target_node_id)
        ctx.handoff_id = handoff_id
        ctx.reuse_telemetry = {
            **dict(ctx.reuse_telemetry),
            "agent_state_consumed": True,
            "agent_state_id": state_id,
            "agent_state_materialize": mat,
            "kv_handoff": self._kv_handoff_payload(
                kv,
                block_ids=block_ids,
                source_node_id=source_node,
                target_node_id=str(target_node_id),
            ),
        }
        self._sync_request_registry(
            ctx,
            status="AGENT_STATE_DECODING",
            event="AGENT_STATE_CONSUME_DISPATCHED",
            agent_id=str(target_node_id),
            step_index=max(1, int(record.step_index) + 1),
            handoff_id=handoff_id,
            target_agent_id=str(target_node_id),
            target_node_id=str(target_node_id),
        )
        with self._lock:
            self._metrics["agent_state_consume_success"] += 1
            self._record_path_metric(ctx.routing_path, "agent_state_consume_success")
        return kv

    def fork_workflow_state(
        self,
        *,
        workflow_id: str,
        parent_request_id: Optional[str] = None,
        branch_count: int = 2,
        target_node_id: Optional[str] = None,
        for_write: bool = False,
    ) -> Dict[str, Any]:
        """Create zero-copy Agent branches from the latest vLLM KV state.

        The serving hot path commits concrete vLLM remote KV block ids in
        ``note_prefill_response``.  Forking is therefore a control-plane
        operation over real directory block records: every branch increments the
        authoritative owner-shard refcount and receives the same block
        descriptors.  Copy-on-write materialization remains delegated to the
        lower KV state store when a branch mutates; this method must not invent
        tensor data or silently fall back to deep-copy.
        """

        if not self.config.enable_agent_state_clone:
            with self._lock:
                self._metrics["agent_state_clone_failures"] += 1
            raise RuntimeError("agent state cloning is disabled for this serving control plane")
        workflow_id = str(workflow_id or "").strip()
        if not workflow_id:
            with self._lock:
                self._metrics["agent_state_clone_failures"] += 1
            raise ValueError("workflow_id is required")
        branch_count = int(branch_count)
        if branch_count <= 0 or branch_count > 128:
            with self._lock:
                self._metrics["agent_state_clone_failures"] += 1
            raise ValueError("branch_count must be in [1, 128]")

        source = self._resolve_workflow_clone_source(
            workflow_id=workflow_id,
            parent_request_id=parent_request_id,
        )
        block_ids = [str(gid) for gid in list(source.get("block_ids") or []) if str(gid)]
        if not block_ids:
            with self._lock:
                self._metrics["agent_state_clone_failures"] += 1
            raise RuntimeError(
                f"workflow_id={workflow_id} has no concrete KV block ids to fork"
            )

        missing = [gid for gid in block_ids if self.kv_directory.get_record(gid) is None]
        if missing:
            with self._lock:
                self._metrics["agent_state_clone_failures"] += 1
            raise RuntimeError(
                "cannot fork workflow state because KV directory records are missing: "
                + ", ".join(missing[:8])
            )

        branch_records: List[Dict[str, Any]] = []
        refcounts: Dict[str, int] = {}
        target = str(target_node_id or source.get("decode_worker_id") or self.config.target_agent_id)
        parent_state_id = str(source.get("request_id") or parent_request_id or "")
        parent_version_id = str(source.get("version_id") or source.get("registry_version_id") or "") or None
        source_kv_handoff = dict(source.get("kv_handoff") or {})
        now = time.monotonic()
        ttl_deadline = now + max(1.0, float(self.config.request_state_ttl_seconds))
        acquired_refs: List[str] = []
        acquired_leases: List[str] = []
        persisted_branch_ids: List[str] = []

        try:
            for branch_index in range(branch_count):
                branch_id = f"{workflow_id}:branch:{uuid.uuid4().hex}"
                for gid in block_ids:
                    refcounts[gid] = self.kv_directory.incref(gid)
                    acquired_refs.append(gid)
                    if for_write and hasattr(self.kv_directory, "lease"):
                        # A write-intent branch must keep the physical owner
                        # alive until lower-layer CoW materialization completes.
                        self.kv_directory.lease(gid)
                        acquired_leases.append(gid)
                branch_records.append(
                    {
                        "branch_id": branch_id,
                        "workflow_id": workflow_id,
                        "parent_request_id": parent_state_id,
                        "target_node_id": target,
                        "kv_block_ids": list(block_ids),
                        "zero_copy": True,
                        "for_write": bool(for_write),
                        "consume_kv_transfer_params": self._build_agent_state_consume_kv_params(
                            record=WorkflowStateRecord(
                                state_id=branch_id,
                                workflow_id=workflow_id,
                                version_id=f"{branch_id}:preview",
                                parent_version_id=parent_version_id,
                                snapshot_epoch=int(source.get("snapshot_epoch", 0) or 0) + 1,
                                step_index=int(source.get("step_index", 0) or 0) + 1,
                                agent_id=target,
                                status="AGENT_BRANCH_ACTIVE",
                                reuse_telemetry={"kv_handoff": source_kv_handoff},
                                token_ids=list(source.get("token_ids") or []),
                                image_ids=list(source.get("mm_hashes") or []),
                                feature_hashes=list(source.get("mm_hashes") or []),
                                kv_block_ids=list(block_ids),
                                target_agent_id=target,
                                target_node_id=target,
                            ),
                            target_node_id=target,
                            transfer_id=f"agent-state-{branch_id}",
                        ) if source_kv_handoff else {},
                    }
                )
                if self.workflow_registry is not None:
                    self.workflow_registry.upsert_record(
                        WorkflowStateRecord(
                            state_id=branch_id,
                            workflow_id=workflow_id,
                            version_id=f"{branch_id}:clone",
                            parent_version_id=parent_version_id,
                            snapshot_epoch=int(source.get("snapshot_epoch", 0) or 0) + 1,
                            step_index=int(source.get("step_index", 0) or 0) + 1,
                            agent_id=target,
                            status="AGENT_BRANCH_ACTIVE",
                            approximate=False,
                            reuse_telemetry={
                                "agent_state_clone": True,
                                "zero_copy": True,
                                "parent_request_id": parent_state_id,
                                "branch_index": branch_index,
                                "for_write": bool(for_write),
                                "kv_handoff": source_kv_handoff,
                            },
                            ttl_deadline=ttl_deadline,
                            token_ids=list(source.get("token_ids") or []),
                            image_ids=list(source.get("mm_hashes") or []),
                            feature_hashes=list(source.get("mm_hashes") or []),
                            kv_block_ids=list(block_ids),
                            target_agent_id=target,
                            target_node_id=target,
                            updated_at=now,
                        ),
                        event="AGENT_STATE_FORKED",
                        preserve_existing_handoff=False,
                    )
                    persisted_branch_ids.append(branch_id)
        except Exception:
            # Forking is a reference-counting transaction.  A failure after a
            # partial branch must leave neither live workflow records nor KV
            # leases/refcounts behind, otherwise future requests retain orphan
            # GPU/remote blocks indefinitely.
            if self.workflow_registry is not None:
                for branch_id in reversed(persisted_branch_ids):
                    try:
                        self.workflow_registry.mark_released(
                            branch_id,
                            event="AGENT_STATE_FORK_ROLLED_BACK",
                        )
                    except Exception:
                        pass
            if hasattr(self.kv_directory, "release_lease"):
                for gid in reversed(acquired_leases):
                    try:
                        self.kv_directory.release_lease(gid)
                    except Exception:
                        pass
            for gid in reversed(acquired_refs):
                try:
                    self.kv_directory.decref(gid)
                except Exception:
                    pass
            with self._lock:
                self._metrics["agent_state_clone_failures"] += 1
            raise

        with self._lock:
            self._metrics["agent_state_clone_requests"] += 1
            self._metrics["agent_state_clone_branches"] += branch_count
            self._metrics["agent_state_clone_zero_copy_branches"] += branch_count
            # No tensor bytes are copied on the serving-control fork path.
            self._metrics["agent_state_clone_copied_bytes"] += 0

        return {
            "workflow_id": workflow_id,
            "parent_request_id": parent_state_id,
            "branch_count": branch_count,
            "branches": branch_records,
            "kv_block_ids": list(block_ids),
            "refcounts": refcounts,
            "copied_bytes": 0,
            "zero_copy_branches": branch_count,
            "clone_semantics": "same_node_block_ref_zero_copy",
            "target_node_id": target,
        }

    def register_agent_state(
        self,
        *,
        workflow_id: str,
        state_id: Optional[str] = None,
        kv_block_ids: Sequence[str],
        token_ids: Optional[Sequence[int]] = None,
        feature_hashes: Optional[Sequence[str]] = None,
        target_node_id: Optional[str] = None,
        kv_transfer_params: Optional[Dict[str, Any]] = None,
        status: str = "AGENT_STATE_ACTIVE",
    ) -> Dict[str, Any]:
        """Register a serving-visible Agent KV state and pin its KV blocks.

        This is a control-plane registration over real KV-directory block ids.
        It never manufactures tensor contents: missing block records are
        recorded as explicit external placeholders owned by the supplied target
        node so later materialization/fork operations can fail closed if the
        data plane cannot resolve them.
        """

        if not self.config.enable_agent_state_clone:
            with self._lock:
                self._metrics["agent_state_clone_failures"] += 1
            raise RuntimeError("agent state cloning is disabled for this serving control plane")
        workflow_id = str(workflow_id or "").strip()
        if not workflow_id:
            raise ValueError("workflow_id is required")
        block_ids = [str(gid) for gid in list(kv_block_ids or []) if str(gid)]
        if not block_ids:
            raise ValueError("kv_block_ids is required")
        registry = self.workflow_registry
        if registry is None:  # defensive; __init__ creates it for clone-enabled planes.
            registry = WorkflowStateRegistry()
            self.workflow_registry = registry

        state_id = str(state_id or f"{workflow_id}:state:{uuid.uuid4().hex}")
        existing = registry.get_record(state_id)
        if existing is not None and existing.status != "RELEASED":
            return {
                "state_id": state_id,
                "workflow_id": workflow_id,
                "registered": False,
                "idempotent": True,
                "kv_block_ids": list(existing.kv_block_ids),
                "refcounts": {
                    gid: self.kv_directory.refcount(gid)
                    for gid in list(existing.kv_block_ids)
                },
                "status": existing.status,
            }

        target = str(target_node_id or self.config.target_agent_id)
        refcounts: Dict[str, int] = {}
        for gid in block_ids:
            current = self.kv_directory.get_record(gid)
            if current is None:
                self.kv_directory.ensure_block_record(
                    gid,
                    workflow_id=workflow_id,
                    owner_shard=target,
                    physical_node_id=target,
                    external_placeholder=True,
                )
                refcounts[gid] = self.kv_directory.refcount(gid)
            else:
                refcounts[gid] = self.kv_directory.incref(gid)

        now = time.monotonic()
        ttl_deadline = now + max(1.0, float(self.config.request_state_ttl_seconds))
        record = WorkflowStateRecord(
            state_id=state_id,
            workflow_id=workflow_id,
            version_id=f"{state_id}:registered",
            parent_version_id=None if existing is None else existing.version_id,
            snapshot_epoch=0 if existing is None else existing.snapshot_epoch + 1,
            step_index=0 if existing is None else existing.step_index + 1,
            agent_id=target,
            status=status,
            approximate=False,
            reuse_telemetry={
                "agent_state_registered": True,
                "kv_handoff": self._kv_handoff_payload(
                    dict(kv_transfer_params or {}),
                    block_ids=block_ids,
                    source_node_id=str((kv_transfer_params or {}).get("a2a_source_node") or target),
                    target_node_id=target,
                ) if kv_transfer_params else {},
            },
            ttl_deadline=ttl_deadline,
            token_ids=[int(t) for t in list(token_ids or [])],
            image_ids=[str(h) for h in list(feature_hashes or [])],
            feature_hashes=[str(h) for h in list(feature_hashes or [])],
            kv_block_ids=list(block_ids),
            target_agent_id=target,
            target_node_id=target,
            updated_at=now,
        )
        registry.upsert_record(
            record,
            event="AGENT_STATE_REGISTERED",
            preserve_existing_handoff=False,
        )
        with self._lock:
            self._metrics["agent_state_clone_retained_blocks"] += len(block_ids)
        return {
            "state_id": state_id,
            "workflow_id": workflow_id,
            "registered": True,
            "idempotent": False,
            "kv_block_ids": list(block_ids),
            "refcounts": refcounts,
            "status": status,
        }

    def materialize_agent_state(
        self,
        *,
        state_id: str,
        target_node_id: Optional[str] = None,
        for_write: bool = False,
    ) -> Dict[str, Any]:
        """Validate/record an Agent state materialization request.

        Same-node read-only use is already materialized as block-reference
        sharing.  Cross-node or write-intent materialization requires a real KV
        data-plane materializer; this method deliberately fails closed instead
        of pretending that a physical copy has happened.
        """

        with self._lock:
            self._metrics["agent_state_materialize_requests"] += 1
        registry = self.workflow_registry
        if registry is None:
            with self._lock:
                self._metrics["agent_state_materialize_failures"] += 1
            raise KeyError("workflow registry is not enabled")
        state_id = str(state_id or "").strip()
        record = registry.get_record(state_id)
        if record is None:
            with self._lock:
                self._metrics["agent_state_materialize_failures"] += 1
            raise KeyError(f"unknown agent state_id={state_id}")
        if record.status == "RELEASED":
            with self._lock:
                self._metrics["agent_state_materialize_failures"] += 1
            raise RuntimeError(f"agent state_id={state_id} is already released")
        target = str(target_node_id or record.target_node_id or record.agent_id or self.config.node_id)
        block_records = {
            gid: self.kv_directory.get_record(gid)
            for gid in list(record.kv_block_ids)
        }
        missing = [gid for gid, block_record in block_records.items() if block_record is None]
        if missing:
            with self._lock:
                self._metrics["agent_state_materialize_failures"] += 1
            raise RuntimeError("cannot materialize missing KV blocks: " + ", ".join(missing[:8]))
        remote_blocks = [
            gid
            for gid, block_record in block_records.items()
            if block_record is not None and str(block_record.physical_node_id) != target
        ]
        kv_handoff = self._extract_kv_handoff_from_record(record)
        if for_write:
            registry.transition(
                state_id,
                status="AGENT_STATE_MATERIALIZE_REQUIRED",
                target_node_id=target,
                event="AGENT_STATE_MATERIALIZE_REQUESTED",
            )
            with self._lock:
                self._metrics["agent_state_materialize_failures"] += 1
            raise RuntimeError(
                "agent state write materialization requires a real KV materializer "
                f"(target_node_id={target}, remote_blocks={len(remote_blocks)}, for_write={bool(for_write)})"
            )
        if remote_blocks and not kv_handoff:
            registry.transition(
                state_id,
                status="AGENT_STATE_MATERIALIZE_REQUIRED",
                target_node_id=target,
                event="AGENT_STATE_MATERIALIZE_REQUESTED",
            )
            with self._lock:
                self._metrics["agent_state_materialize_failures"] += 1
            raise RuntimeError(
                "agent state read materialization requires persisted KV handoff metadata "
                f"(target_node_id={target}, remote_blocks={len(remote_blocks)})"
            )
        if remote_blocks:
            updated = registry.transition(
                state_id,
                status="AGENT_STATE_REMOTE_READ_READY",
                target_node_id=target,
                event="AGENT_STATE_REMOTE_READ_READY",
            )
            with self._lock:
                self._metrics["agent_state_materialize_success"] += 1
            return {
                "state_id": state_id,
                "workflow_id": record.workflow_id,
                "materialized": True,
                "for_write": False,
                "target_node_id": target,
                "clone_semantics": "remote_read_via_mooncake_connector",
                "kv_block_ids": list(record.kv_block_ids),
                "consume_kv_transfer_params": self._build_agent_state_consume_kv_params(
                    record=record,
                    target_node_id=target,
                ),
                "status": updated.status if updated is not None else "AGENT_STATE_REMOTE_READ_READY",
            }
        updated = registry.transition(
            state_id,
            status="AGENT_STATE_ACTIVE",
            target_node_id=target,
            event="AGENT_STATE_MATERIALIZED_SAME_NODE",
        )
        with self._lock:
            self._metrics["agent_state_materialize_success"] += 1
        return {
            "state_id": state_id,
            "workflow_id": record.workflow_id,
            "materialized": True,
            "for_write": bool(for_write),
            "target_node_id": target,
            "clone_semantics": "same_node_block_ref_zero_copy",
            "kv_block_ids": list(record.kv_block_ids),
            "status": updated.status if updated is not None else "AGENT_STATE_ACTIVE",
        }

    def release_agent_state(
        self,
        *,
        state_id: str,
        release_physical: bool = True,
        sweep_orphans: bool = True,
    ) -> Dict[str, Any]:
        """Idempotently release a registered/forked Agent state reference."""

        registry = self.workflow_registry
        if registry is None:
            raise KeyError("workflow registry is not enabled")
        state_id = str(state_id or "").strip()
        record = registry.get_record(state_id)
        if record is None:
            raise KeyError(f"unknown agent state_id={state_id}")
        if record.status == "RELEASED" and bool(record.reuse_telemetry.get("agent_state_release_complete")):
            return {
                "state_id": state_id,
                "workflow_id": record.workflow_id,
                "released": False,
                "idempotent": True,
                "refcounts": {
                    gid: self.kv_directory.refcount(gid)
                    for gid in list(record.kv_block_ids)
                },
                "orphans_swept": 0,
                "status": "RELEASED",
            }

        refcounts: Dict[str, int] = {}
        for gid in list(record.kv_block_ids):
            if self.kv_directory.get_record(gid) is None:
                refcounts[gid] = 0
                continue
            refcount = self.kv_directory.decref(gid)
            if release_physical and refcount <= 0:
                self.kv_directory.release_physical(gid)
                refcount = self.kv_directory.refcount(gid)
            refcounts[gid] = refcount
        record.status = "RELEASED"
        record.released_at = record.released_at or time.monotonic()
        record.updated_at = time.monotonic()
        record.reuse_telemetry = {
            **dict(record.reuse_telemetry),
            "agent_state_release_complete": True,
        }
        with self._lock:
            for workflow_id, latest in list(self._workflow_latest.items()):
                if str(latest.get("request_id") or "") == state_id:
                    self._workflow_latest.pop(workflow_id, None)
        released = registry.upsert_record(
            record,
            event="AGENT_STATE_RELEASED",
            preserve_existing_handoff=False,
            preserve_existing_reuse=False,
        )
        swept = self.kv_directory.sweep_orphans() if sweep_orphans else 0
        return {
            "state_id": state_id,
            "workflow_id": record.workflow_id,
            "released": True,
            "idempotent": False,
            "refcounts": refcounts,
            "orphans_swept": swept,
            "status": released.status if released is not None else "RELEASED",
        }

    def agent_state_stats(self, *, sweep_orphans: bool = False) -> Dict[str, Any]:
        """Return Agent-state registry and KV-directory lifecycle counters."""

        expired_released = self.sweep_expired_agent_states() if sweep_orphans else 0
        swept = self.kv_directory.sweep_orphans() if sweep_orphans else 0
        registry = self.workflow_registry
        rows = registry.all_records() if registry is not None else []
        agent_rows = [
            row for row in rows
            if row.status.startswith("AGENT_")
            or bool(row.reuse_telemetry.get("agent_state_clone"))
            or bool(row.reuse_telemetry.get("agent_state_registered"))
        ]
        active_rows = [row for row in agent_rows if row.status != "RELEASED"]
        block_ids = sorted({gid for row in agent_rows for gid in row.kv_block_ids})
        return {
            "enabled": self.config.enable_agent_state_clone,
            "registry_enabled": registry is not None,
            "active_states": len(active_rows),
            "tracked_states": len(agent_rows),
            "active_state_ids": sorted(row.state_id for row in active_rows),
            "status_counts": self._count_record_statuses(agent_rows),
            "refcounts": {gid: self.kv_directory.refcount(gid) for gid in block_ids},
            "orphan_block_ids": (
                self.kv_directory.orphan_block_ids()
                if hasattr(self.kv_directory, "orphan_block_ids")
                else []
            ),
            "kv_directory": self.kv_directory.stats(),
            "orphans_swept": swept,
            "clone_semantics": {
                "same_node": "block_ref_zero_copy",
                "cross_node": "descriptor_share_then_materialize",
            },
        }

    def sweep_expired_agent_states(self, *, now: Optional[float] = None) -> int:
        """Release expired Agent-state pins and stale workflow-latest pins.

        The serving hot path keeps the latest workflow KV blocks pinned so an
        Agent can fork after the request completes.  This method is the safety
        valve for abandoned workflows: it releases exactly that retained logical
        reference and leaves branch references intact via the KV directory
        refcount.
        """

        if not self.config.enable_agent_state_clone:
            return 0
        now_f = time.monotonic() if now is None else float(now)
        released = 0
        registry = self.workflow_registry
        if registry is not None:
            for record in list(registry.all_records()):
                if record.status == "RELEASED" or record.ttl_deadline == float("inf"):
                    continue
                if record.ttl_deadline > now_f:
                    continue
                is_agent = (
                    record.status.startswith("AGENT_")
                    or bool(record.reuse_telemetry.get("agent_state_clone"))
                    or bool(record.reuse_telemetry.get("agent_state_registered"))
                )
                if not is_agent:
                    continue
                try:
                    self.release_agent_state(
                        state_id=record.state_id,
                        release_physical=True,
                        sweep_orphans=False,
                    )
                    released += 1
                except Exception:
                    with self._lock:
                        self._metrics["agent_state_clone_failures"] += 1
        ttl = max(1.0, float(self.config.request_state_ttl_seconds))
        with self._lock:
            expired_latest = [
                workflow_id
                for workflow_id, latest in self._workflow_latest.items()
                if now_f - float(latest.get("updated_at", now_f) or now_f) > ttl
            ]
        for workflow_id in expired_latest:
            with self._lock:
                latest = self._workflow_latest.pop(workflow_id, None)
            if not latest:
                continue
            for gid in list(latest.get("block_ids") or []):
                if self.kv_directory.get_record(gid) is None:
                    continue
                refcount = self.kv_directory.decref(gid)
                if refcount <= 0:
                    self.kv_directory.release_physical(gid)
            released += 1
        if released:
            with self._lock:
                self._metrics["agent_state_expired_releases"] += released
        return released

    def _preferred_worker_for(self, workflow_id: str) -> Optional[str]:
        if not self.config.enable_workflow_affinity or not workflow_id:
            return None
        entry = self._workflow_affinity.get(workflow_id)
        if entry is None:
            return None
        worker_id, updated_at = entry
        if time.monotonic() - updated_at > max(1.0, float(self.config.workflow_affinity_ttl_seconds)):
            self._workflow_affinity.pop(workflow_id, None)
            return None
        return worker_id

    def _set_workflow_affinity(self, workflow_id: str, worker_id: str) -> None:
        if self.config.enable_workflow_affinity and workflow_id and worker_id:
            self._workflow_affinity[workflow_id] = (worker_id, time.monotonic())

    def _prepare_cross_step_reuse(self, ctx: RequestContext) -> None:
        previous = self._workflow_latest.get(ctx.workflow_id)
        if not previous or previous.get("request_id") == ctx.request_id:
            return
        prev_tokens = list(previous.get("token_ids") or [])
        if not prev_tokens or not ctx.token_ids:
            return
        prev_images = list(previous.get("mm_hashes") or [])
        # Avoid unsafe multimodal KV reuse if the image context changed. Feature
        # cache can still hit independently through MMStore, but KV reuse hints
        # must stay scoped to identical image context.
        if prev_images != list(ctx.mm_hashes):
            return
        common = 0
        for left, right in zip(prev_tokens, ctx.token_ids):
            if left != right:
                break
            common += 1
        if common <= 0:
            return
        total = max(1, len(ctx.token_ids))
        telemetry = {
            "cross_step": True,
            "approximate": False,
            "reused_tokens": int(common),
            "total_tokens": int(total),
            "delta_tokens": int(max(0, total - common)),
            "reuse_ratio": float(common / total),
            "source_request_id": previous.get("request_id"),
            "source_prefill_worker_id": previous.get("prefill_worker_id"),
        }
        ctx.reuse_telemetry = telemetry
        ctx.reuse_candidate_block_ids = list(previous.get("block_ids") or [])
        ctx.reuse_source_request_id = str(previous.get("request_id") or "") or None
        preferred = previous.get("prefill_worker_id")
        if preferred:
            self._set_workflow_affinity(ctx.workflow_id, str(preferred))
        with self._lock:
            self._metrics["serving_cross_step_reuse_candidates"] += 1
            self._metrics["serving_cross_step_reused_tokens"] += int(common)
            self._record_path_metric(ctx.routing_path, "serving_cross_step_reuse_candidates")
            self._record_path_metric(ctx.routing_path, "serving_cross_step_reused_tokens", delta=int(common))

    def _commit_workflow_latest(self, ctx: RequestContext) -> None:
        if not ctx.workflow_id:
            return
        self._workflow_latest[ctx.workflow_id] = {
            "request_id": ctx.request_id,
            "workflow_id": ctx.workflow_id,
            "token_ids": list(ctx.token_ids),
            "mm_hashes": list(ctx.mm_hashes),
            "block_ids": list(ctx.block_ids),
            "prefill_worker_id": ctx.prefill_worker_id,
            "decode_worker_id": ctx.decode_worker_id,
            "kv_handoff": dict(ctx.reuse_telemetry.get("kv_handoff") or {}),
            "updated_at": time.monotonic(),
        }
        if ctx.prefill_worker_id:
            self._set_workflow_affinity(ctx.workflow_id, ctx.prefill_worker_id)
        with self._lock:
            self._metrics["serving_workflow_state_commits"] += 1
            self._record_path_metric(ctx.routing_path, "serving_workflow_state_commits")

    def _resolve_workflow_clone_source(
        self,
        *,
        workflow_id: str,
        parent_request_id: Optional[str],
    ) -> Dict[str, Any]:
        if parent_request_id:
            parent = str(parent_request_id)
            active = self._request_ctx.get(parent)
            if active is not None and active.workflow_id == workflow_id:
                return {
                    "request_id": active.request_id,
                    "workflow_id": active.workflow_id,
                    "token_ids": list(active.token_ids),
                    "mm_hashes": list(active.mm_hashes),
                    "block_ids": list(active.block_ids),
                    "prefill_worker_id": active.prefill_worker_id,
                    "decode_worker_id": active.decode_worker_id,
                    "registry_version_id": active.registry_version_id,
                    "snapshot_epoch": active.registry_version_seq,
                    "step_index": active.registry_version_seq,
                }
            if self.workflow_registry is not None:
                record = self.workflow_registry.get_record(parent)
                if record is not None and record.workflow_id == workflow_id:
                    return {
                        "request_id": record.state_id,
                        "workflow_id": record.workflow_id,
                        "version_id": record.version_id,
                        "token_ids": list(record.token_ids),
                        "mm_hashes": list(record.feature_hashes or record.image_ids),
                        "block_ids": list(record.kv_block_ids),
                        "decode_worker_id": record.target_node_id or record.agent_id,
                        "kv_handoff": self._extract_kv_handoff_from_record(record),
                        "snapshot_epoch": record.snapshot_epoch,
                        "step_index": record.step_index,
                    }
        latest = self._workflow_latest.get(workflow_id)
        if latest:
            return dict(latest)
        if self.workflow_registry is not None:
            record = self.workflow_registry.latest_workflow_record(workflow_id)
            if record is not None:
                return {
                    "request_id": record.state_id,
                    "workflow_id": record.workflow_id,
                    "version_id": record.version_id,
                    "token_ids": list(record.token_ids),
                    "mm_hashes": list(record.feature_hashes or record.image_ids),
                    "block_ids": list(record.kv_block_ids),
                    "decode_worker_id": record.target_node_id or record.agent_id,
                    "kv_handoff": self._extract_kv_handoff_from_record(record),
                    "snapshot_epoch": record.snapshot_epoch,
                    "step_index": record.step_index,
                }
        with self._lock:
            self._metrics["agent_state_clone_failures"] += 1
        raise KeyError(f"no committed workflow state found for workflow_id={workflow_id}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _scheduler_for_stage(self, stage: str) -> AgentScheduler:
        if stage == "prefill":
            return self._prefill_scheduler
        if stage == "decode":
            return self._decode_scheduler
        raise ValueError(f"unsupported stage: {stage}")

    def _agent_type_for(self, stage: str, ctx: RequestContext) -> AgentType:
        if ctx.agent_type_hint is not None:
            return ctx.agent_type_hint
        if stage == "prefill":
            return AgentType.THINKING if ctx.modality == "multimodal" else AgentType.HYBRID
        return AgentType.INTERACTIVE if ctx.modality == "text" else AgentType.HYBRID

    def _base_control_fields(
        self,
        *,
        ctx: RequestContext,
        stage: str,
        worker_id: str,
        rho: float,
        action: AdmissionAction,
        degrade: DegradeLevel,
    ) -> Dict[str, Any]:
        return {
            "transfer_id": ctx.transfer_id or ctx.request_id,
            "workflow_id": ctx.workflow_id,
            "routing_path": ctx.routing_path,
            "request_modality": ctx.modality,
            "control_stage": stage,
            "control_node_id": self.config.node_id,
            "control_worker_id": worker_id,
            "scheduler_rho": rho,
            "admission_action": action.value,
            "degrade_level": degrade.value,
            "layered_kv_transfer": self.config.layered_kv_transfer,
            "layers_per_group": self.config.layers_per_group,
            "group_delay_ms": self.config.group_delay_ms,
            "max_group_bytes": self.config.max_group_bytes,
            "transport_backend": self.config.transport_backend,
            "mooncake_protocol": self.config.mooncake_protocol,
            "handoff_id": ctx.handoff_id,
            "mm_prefetch_image_hashes": list(ctx.mm_hashes),
            "mm_prefetch_policy": (
                self.config.mm_prefetch_policy if self.config.enable_mm_prefetch and ctx.mm_hashes else "none"
            ),
            "serving_reuse_telemetry": dict(ctx.reuse_telemetry),
            "serving_reuse_candidate_block_ids": list(ctx.reuse_candidate_block_ids),
            "serving_reuse_source_request_id": ctx.reuse_source_request_id,
            "agent_type": ctx.agent_type_hint.value if ctx.agent_type_hint is not None else None,
            "agent_pd_profile": dict(ctx.agent_pd_profile),
            "agent_pd_prefill_pool": self._preferred_pool_for_stage("prefill", ctx),
            "agent_pd_decode_pool": self._preferred_pool_for_stage("decode", ctx),
            "agent_pd_routing_target": ctx.agent_pd_profile.get("routing_target"),
            "expected_output_tokens": int(ctx.expected_output_tokens),
            "priority": int(ctx.priority),
        }

    @staticmethod
    def _new_path_stats() -> Dict[str, Any]:
        return {
            "requests_total": 0,
            "requests_finished": 0,
            "requests_text": 0,
            "requests_multimodal": 0,
            "mm_prefetch_announced": 0,
            "mm_prefetch_attempted": 0,
            "mm_prefetch_completed": 0,
            "mm_prefetch_failed": 0,
            "mm_prefetch_worker_cache_hits": 0,
            "mm_prefetch_shared_store_hits": 0,
            "mm_prefetch_recomputed": 0,
            "mm_prefetch_bytes": 0,
            "mm_prefetch_wait_ms_total": 0.0,
            "mm_prefetch_wait_ms_count": 0,
            "mm_prefetch_wait_ms_avg": 0.0,
            "serving_cross_step_reuse_candidates": 0,
            "serving_cross_step_reused_tokens": 0,
            "serving_workflow_state_commits": 0,
            "serving_workflow_affinity_hits": 0,
            "handoff_prepared": 0,
            "handoff_committed": 0,
            "handoff_rolled_back": 0,
            "backpressure_events": 0,
            "deadline_backpressure_events": 0,
            "reject_events": 0,
            "deadline_reject_events": 0,
            "stage_dispatches": {
                "prefill": 0,
                "decode": 0,
            },
            "stage_backpressure_events": {
                "prefill": 0,
                "decode": 0,
            },
            "stage_reject_events": {
                "prefill": 0,
                "decode": 0,
            },
            "agent_pd_route_total": 0,
            "agent_pd_route_correct": 0,
            "agent_pd_route_incorrect": 0,
        }

    @staticmethod
    def _normalize_routing_path(path: Optional[str]) -> str:
        normalized = str(path or "UNKNOWN").strip().upper()
        return normalized or "UNKNOWN"

    def _ensure_path_stats(self, path: Optional[str]) -> Dict[str, Any]:
        bucket = self._normalize_routing_path(path)
        all_stats = self._metrics.setdefault("path_stats", {})
        stats = all_stats.get(bucket)
        if not isinstance(stats, dict):
            stats = self._new_path_stats()
            all_stats[bucket] = stats
        return stats

    def _record_path_metric(self, path: Optional[str], key: str, *, delta: int = 1) -> None:
        stats = self._ensure_path_stats(path)
        stats[key] = int(stats.get(key, 0) or 0) + int(delta)

    def _record_path_stage_metric(
        self,
        path: Optional[str],
        stage: str,
        key: str,
        *,
        delta: int = 1,
    ) -> None:
        stats = self._ensure_path_stats(path)
        stage_counts = stats.setdefault(key, {})
        stage_counts[stage] = int(stage_counts.get(stage, 0) or 0) + int(delta)

    def _snapshot_path_stats(self) -> Dict[str, Dict[str, Any]]:
        active_counts: Dict[str, int] = {}
        for ctx in self._request_ctx.values():
            bucket = self._normalize_routing_path(ctx.routing_path)
            active_counts[bucket] = active_counts.get(bucket, 0) + 1
        out: Dict[str, Dict[str, Any]] = {}
        for path, stats in dict(self._metrics.get("path_stats") or {}).items():
            copied: Dict[str, Any] = {}
            for key, value in stats.items():
                copied[key] = dict(value) if isinstance(value, dict) else value
            wait_count = int(copied.get("mm_prefetch_wait_ms_count", 0) or 0)
            if wait_count > 0:
                copied["mm_prefetch_wait_ms_avg"] = (
                    float(copied.get("mm_prefetch_wait_ms_total", 0.0) or 0.0) / float(wait_count)
                )
            copied["requests_active"] = int(active_counts.get(path, 0))
            out[path] = copied
        for path, active in active_counts.items():
            if path not in out:
                out[path] = self._new_path_stats()
                out[path]["requests_active"] = int(active)
        return out

    def _ensure_block_records(
        self,
        *,
        block_ids: Sequence[str],
        workflow_id: str,
        owner_shard: str,
        physical_node_id: str,
    ) -> List[str]:
        placeholders: List[str] = []
        for gid in block_ids:
            record = self.kv_directory.ensure_block_record(
                gid,
                workflow_id=workflow_id,
                owner_shard=owner_shard,
                physical_node_id=physical_node_id,
                external_placeholder=True,
            )
            if record.external_placeholder:
                placeholders.append(gid)
        return placeholders

    def _is_workflow_pinned_block(self, global_block_id: str) -> bool:
        if not self.config.enable_agent_state_clone:
            return False
        for latest in self._workflow_latest.values():
            if global_block_id in set(latest.get("block_ids") or []):
                return True
        return False

    @staticmethod
    def _iter_content_items(req_data: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        messages = req_data.get("messages") or []
        if isinstance(messages, list):
            for message in messages:
                content = message.get("content") if isinstance(message, dict) else None
                if isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict):
                            yield item
                elif isinstance(content, str):
                    yield {"type": "text", "text": content}
        prompt = req_data.get("prompt")
        if isinstance(prompt, str):
            yield {"type": "text", "text": prompt}

    @staticmethod
    def _stable_mm_hash(item: Dict[str, Any]) -> str:
        payload = {
            k: item.get(k)
            for k in sorted(item)
            if k not in {"detail"}
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:16]

    @staticmethod
    def _workflow_id(req_data: Dict[str, Any], request_id: str) -> str:
        metadata = req_data.get("metadata") if isinstance(req_data.get("metadata"), dict) else {}
        for key in ("workflow_id", "conversation_id", "session_id"):
            value = metadata.get(key) or req_data.get(key)
            if value:
                return str(value)
        return request_id

    @staticmethod
    def _extract_deadline(req_data: Dict[str, Any]) -> Optional[float]:
        metadata = req_data.get("metadata") if isinstance(req_data.get("metadata"), dict) else {}
        now = time.monotonic()
        candidates = [
            metadata.get("deadline_at_monotonic"),
            req_data.get("deadline_at_monotonic"),
        ]
        for value in candidates:
            if value is None:
                continue
            try:
                deadline = float(value)
            except Exception:
                continue
            if deadline > 0:
                return deadline
        relative_candidates = [
            metadata.get("deadline_ms"),
            req_data.get("deadline_ms"),
            metadata.get("slo_ms"),
            req_data.get("slo_ms"),
        ]
        for value in relative_candidates:
            if value is None:
                continue
            try:
                delta_ms = float(value)
            except Exception:
                continue
            if delta_ms > 0:
                return now + delta_ms / 1000.0
        return None

    @staticmethod
    def _agent_type_hint(req_data: Dict[str, Any]) -> Optional[AgentType]:
        metadata = req_data.get("metadata") if isinstance(req_data.get("metadata"), dict) else {}
        raw = (
            metadata.get("agent_type")
            or metadata.get("priority_class")
            or req_data.get("agent_type")
            or req_data.get("priority_class")
        )
        if raw is None:
            return None
        try:
            return AgentType(str(raw).strip().upper())
        except ValueError:
            return None

    @staticmethod
    def _request_priority(req_data: Dict[str, Any]) -> int:
        metadata = req_data.get("metadata") if isinstance(req_data.get("metadata"), dict) else {}
        raw = metadata.get("priority", req_data.get("priority", 0))
        try:
            return int(raw)
        except Exception:
            return 0

    @staticmethod
    def _expected_output_tokens(req_data: Dict[str, Any]) -> int:
        metadata = req_data.get("metadata") if isinstance(req_data.get("metadata"), dict) else {}
        for key in ("expected_output_tokens", "max_tokens", "max_completion_tokens"):
            raw = metadata.get(key, req_data.get(key))
            if raw is None:
                continue
            try:
                value = int(raw)
            except Exception:
                continue
            if value > 0:
                return value
        bucket = str(metadata.get("expected_output_tokens_bucket") or req_data.get("expected_output_tokens_bucket") or "")
        parsed = ServingControlPlane._bucket_midpoint(bucket)
        return parsed if parsed > 0 else 256

    @staticmethod
    def _bucket_midpoint(bucket: str) -> int:
        text = str(bucket or "").strip().lower().replace(" ", "")
        if not text:
            return 0
        scale = 1
        text = text.replace("tokens", "")
        if "k" in text:
            scale = 1000
            text = text.replace("k", "")
        if "-" in text:
            left, right = text.split("-", 1)
            try:
                return int((float(left) + float(right)) * 0.5 * scale)
            except Exception:
                return 0
        try:
            return int(float(text) * scale)
        except Exception:
            return 0


    @staticmethod
    def _has_agent_pd_signal(req_data: Dict[str, Any]) -> bool:
        metadata = req_data.get("metadata") if isinstance(req_data.get("metadata"), dict) else {}
        signal_keys = {
            "agent_type",
            "priority_class",
            "routing_target",
            "reasoning_depth",
            "context_length_level",
            "input_tokens_bucket",
            "expected_output_tokens_bucket",
            "expected_output_tokens",
            "latency_sensitivity",
            "quality_priority",
            "tool_use_required",
            "sla_ms",
        }
        for key in signal_keys:
            if metadata.get(key) is not None or req_data.get(key) is not None:
                return True
        return False
    def _agent_pd_profile(self, req_data: Dict[str, Any], ctx: RequestContext) -> Dict[str, Any]:
        metadata = req_data.get("metadata") if isinstance(req_data.get("metadata"), dict) else {}
        enabled = self._has_agent_pd_signal(req_data)
        agent_type = ctx.agent_type_hint or self._infer_agent_type(req_data, ctx)
        if enabled:
            ctx.agent_type_hint = agent_type
        routing_target = str(
            metadata.get("routing_target")
            or req_data.get("routing_target")
            or ""
        ).strip().lower()
        if enabled and not routing_target:
            if agent_type is AgentType.THINKING:
                routing_target = "high_prefill_pool"
            elif agent_type is AgentType.INTERACTIVE:
                routing_target = "low_latency_decode_pool"
            else:
                routing_target = "mixed"
        prefill_pool, decode_pool, route_label = "", "", ""
        if enabled:
            if agent_type is AgentType.THINKING:
                prefill_pool, decode_pool = "high_prefill_pool", "standard_decode_pool"
                route_label = "high_prefill_pool"
            elif agent_type is AgentType.INTERACTIVE:
                prefill_pool, decode_pool = "standard_prefill_pool", "low_latency_decode_pool"
                route_label = "low_latency_decode_pool"
            else:
                prefill_pool, decode_pool = "high_prefill_pool", "low_latency_decode_pool"
                route_label = "mixed"
            if routing_target == "high_prefill_pool":
                prefill_pool = "high_prefill_pool"
            elif routing_target == "low_latency_decode_pool":
                decode_pool = "low_latency_decode_pool"
            elif routing_target in {"mixed", "hybrid"}:
                prefill_pool, decode_pool, route_label = "high_prefill_pool", "low_latency_decode_pool", "mixed"
        profile = {
            "enabled": bool(enabled),
            "agent_type": agent_type.value,
            "routing_target": (route_label if routing_target in {"", "hybrid"} else routing_target) if enabled else "",
            "prefill_pool": prefill_pool,
            "decode_pool": decode_pool,
            "input_tokens": max(
                int(ctx.estimated_input_tokens),
                self._safe_int(metadata.get("input_tokens", req_data.get("input_tokens")), 0),
                self._bucket_midpoint(str(metadata.get("input_tokens_bucket") or req_data.get("input_tokens_bucket") or "")),
            ),
            "expected_output_tokens": max(
                int(ctx.expected_output_tokens),
                self._safe_int(metadata.get("expected_output_tokens", req_data.get("expected_output_tokens")), 0),
                self._bucket_midpoint(
                    str(metadata.get("expected_output_tokens_bucket") or req_data.get("expected_output_tokens_bucket") or "")
                ),
            ),
            "reasoning_depth": self._safe_int(metadata.get("reasoning_depth", req_data.get("reasoning_depth")), 0),
            "latency_sensitivity": str(metadata.get("latency_sensitivity") or req_data.get("latency_sensitivity") or ""),
            "quality_priority": str(metadata.get("quality_priority") or req_data.get("quality_priority") or ""),
            "hybrid_fast_path": bool(agent_type is AgentType.HYBRID),
        }
        return profile

    def _infer_agent_type(self, req_data: Dict[str, Any], ctx: RequestContext) -> AgentType:
        metadata = req_data.get("metadata") if isinstance(req_data.get("metadata"), dict) else {}
        latency = str(metadata.get("latency_sensitivity") or req_data.get("latency_sensitivity") or "").lower()
        quality = str(metadata.get("quality_priority") or req_data.get("quality_priority") or "").lower()
        context_level = str(metadata.get("context_length_level") or req_data.get("context_length_level") or "").lower()
        depth = self._safe_int(metadata.get("reasoning_depth", req_data.get("reasoning_depth")), 0)
        tool_required = bool(metadata.get("tool_use_required", req_data.get("tool_use_required", False)))
        output_tokens = ctx.expected_output_tokens
        input_bucket = self._bucket_midpoint(str(metadata.get("input_tokens_bucket") or req_data.get("input_tokens_bucket") or ""))
        input_tokens = max(int(ctx.estimated_input_tokens), input_bucket)
        if latency == "high" and input_tokens <= 1024 and output_tokens <= 256 and depth <= 2:
            return AgentType.INTERACTIVE
        if context_level == "long" or input_tokens >= 4096 or depth >= 4 or tool_required or quality in {"accuracy", "quality"}:
            if latency == "high" and output_tokens <= 1024:
                return AgentType.HYBRID
            return AgentType.THINKING
        if latency == "high" and quality == "balanced":
            return AgentType.HYBRID
        return AgentType.HYBRID if output_tokens >= 512 else AgentType.INTERACTIVE

    @staticmethod
    def _safe_int(raw: Any, default: int = 0) -> int:
        try:
            return int(raw)
        except Exception:
            return int(default)

    def _pool_tags_for_worker(self, stage: str, worker_id: str, *, ordinal: int, total: int) -> List[str]:
        stage = str(stage).lower()
        worker_id = str(worker_id)
        lowered = worker_id.lower()
        tags: List[str] = []
        if stage == "prefill":
            explicit_high = set(map(str, self.config.high_prefill_worker_ids))
            explicit_standard = set(map(str, self.config.standard_prefill_worker_ids))
            if worker_id in explicit_high or "high" in lowered or "thinking" in lowered:
                tags.append("high_prefill_pool")
            if worker_id in explicit_standard or "standard" in lowered or "interactive" in lowered:
                tags.append("standard_prefill_pool")
            if not tags:
                if total <= 1:
                    tags.extend(["high_prefill_pool", "standard_prefill_pool"])
                elif ordinal == 0:
                    tags.append("high_prefill_pool")
                else:
                    tags.append("standard_prefill_pool")
        elif stage == "decode":
            explicit_low = set(map(str, self.config.low_latency_decode_worker_ids))
            explicit_standard = set(map(str, self.config.standard_decode_worker_ids))
            if worker_id in explicit_low or "low" in lowered or "interactive" in lowered:
                tags.append("low_latency_decode_pool")
            if worker_id in explicit_standard or "standard" in lowered or "thinking" in lowered:
                tags.append("standard_decode_pool")
            if not tags:
                if total <= 1:
                    tags.extend(["low_latency_decode_pool", "standard_decode_pool"])
                elif ordinal == 0:
                    tags.append("low_latency_decode_pool")
                else:
                    tags.append("standard_decode_pool")
        return sorted(set(tags))

    def _preferred_pool_for_stage(self, stage: str, ctx: RequestContext) -> str:
        if not bool(ctx.agent_pd_profile.get("enabled")):
            return ""
        if stage == "prefill":
            return str(ctx.agent_pd_profile.get("prefill_pool") or "")
        if stage == "decode":
            return str(ctx.agent_pd_profile.get("decode_pool") or "")
        return ""

    def _avoid_pool_for_stage(self, stage: str, ctx: RequestContext) -> str:
        if not bool(ctx.agent_pd_profile.get("enabled")):
            return ""
        if stage == "prefill" and ctx.agent_type_hint is AgentType.INTERACTIVE:
            return "high_prefill_pool"
        if stage == "decode" and ctx.agent_type_hint is AgentType.THINKING:
            return "low_latency_decode_pool"
        return ""

    def _record_agent_pd_decision(
        self,
        stage: str,
        ctx: RequestContext,
        decision: AdmissionDecision,
    ) -> None:
        if decision.worker is None:
            return
        profile = dict(ctx.agent_pd_profile or {})
        if not bool(profile.get("enabled")):
            return
        agent_type = str(profile.get("agent_type") or (ctx.agent_type_hint.value if ctx.agent_type_hint else "UNKNOWN"))
        preferred_pool = self._preferred_pool_for_stage(stage, ctx)
        worker_tags = set(getattr(decision.worker, "pool_tags", []) or [])
        affinity_worker = (
            self._preferred_worker_for(ctx.workflow_id)
            if stage == "prefill" and ctx.reuse_candidate_block_ids
            else None
        )
        affinity_override = bool(
            affinity_worker and str(decision.worker.worker_id) == str(affinity_worker)
        )
        route_correct = bool(not preferred_pool or preferred_pool in worker_tags or affinity_override)
        record = {
            "request_id": ctx.request_id,
            "workflow_id": ctx.workflow_id,
            "stage": stage,
            "agent_type": agent_type,
            "routing_target": profile.get("routing_target"),
            "preferred_pool": preferred_pool,
            "worker_id": decision.worker.worker_id,
            "worker_pool_tags": sorted(worker_tags),
            "route_correct": route_correct,
            "affinity_override": affinity_override,
            "admission_action": decision.action.value,
            "rho": decision.rho,
            "estimated_wait_ms": decision.estimated_wait_ms,
            "estimated_service_ms": decision.estimated_service_ms,
            "deadline_miss_risk": decision.deadline_miss_risk,
        }
        with self._lock:
            self._metrics["agent_pd_route_total"] += 1
            if route_correct:
                self._metrics["agent_pd_route_correct"] += 1
            else:
                self._metrics["agent_pd_route_incorrect"] += 1
            by_type = self._metrics.setdefault("agent_pd_by_type", {})
            by_type[agent_type] = int(by_type.get(agent_type, 0) or 0) + 1
            stage_counts = self._metrics.setdefault("agent_pd_stage_pool_counts", {}).setdefault(stage, {})
            pool_key = preferred_pool or "none"
            stage_counts[pool_key] = int(stage_counts.get(pool_key, 0) or 0) + 1
            if profile.get("hybrid_fast_path") and stage == "decode":
                self._metrics["agent_pd_hybrid_fast_path"] += 1
            recent = self._metrics.setdefault("agent_pd_route_decisions", [])
            recent.append(record)
            if len(recent) > 512:
                del recent[: len(recent) - 512]
            self._record_path_metric(ctx.routing_path, "agent_pd_route_total")
            self._record_path_metric(
                ctx.routing_path,
                "agent_pd_route_correct" if route_correct else "agent_pd_route_incorrect",
            )

    @staticmethod
    def _normalize_block_ids(block_ids: Any, namespace: Optional[str] = None) -> List[str]:
        if block_ids is None:
            return []
        out: List[str] = []
        if isinstance(block_ids, (list, tuple)):
            for item in block_ids:
                if isinstance(item, (list, tuple)):
                    out.extend(ServingControlPlane._normalize_block_ids(item, namespace=namespace))
                else:
                    out.append(ServingControlPlane._format_block_gid(item, namespace=namespace))
        else:
            out.append(ServingControlPlane._format_block_gid(block_ids, namespace=namespace))
        return out

    @staticmethod
    def _format_block_gid(block_id: Any, namespace: Optional[str] = None) -> str:
        text = str(block_id)
        if not namespace:
            return text
        return text if text.startswith(f"{namespace}:") else f"{namespace}:{text}"

    def _backpressure_delay_ms(self, decision: AdmissionDecision) -> float:
        worker = decision.worker
        if worker is None:
            return 0.0
        critical = max(worker.critical_rho, self.config.warn_rho)
        span = max(1e-6, self.config.critical_rho - critical)
        severity = max(0.0, min(1.0, (decision.rho - critical) / span))
        return round(severity * self.config.max_backpressure_delay_ms, 3)

    def _release_if_admitted(self, decision: AdmissionDecision) -> None:
        worker = decision.worker
        if worker is None:
            return
        worker.current_load = max(0, worker.current_load - 1)
        worker.queue_size = max(0, worker.queue_size - 1)

    def _record_arrival(self, worker_id: str) -> None:
        now = time.monotonic()
        last = self._worker_last_arrival.get(worker_id)
        self._worker_last_arrival[worker_id] = now
        if last is None:
            return
        delta = now - last
        if delta <= 0:
            return
        arrival_rate = 1.0 / delta
        for scheduler in (self._prefill_scheduler, self._decode_scheduler):
            for worker in scheduler.workers:
                if worker.worker_id == worker_id:
                    worker.arrival_rate = worker.arrival_rate * 0.8 + arrival_rate * 0.2
                    return

    def _decay_arrival_rates(self, scheduler: AgentScheduler) -> None:
        now = time.monotonic()
        for worker in scheduler.workers:
            if worker.arrival_rate <= 0:
                continue
            last = self._worker_last_arrival.get(worker.worker_id)
            if last is None:
                continue
            idle_s = max(0.0, now - last)
            if idle_s <= 0:
                continue
            decay = math.exp(-idle_s * max(worker.service_rate, 1e-6))
            worker.arrival_rate *= decay

    def _predict_worker_rho(
        self,
        *,
        scheduler: AgentScheduler,
        worker: WorkerLoad,
        now: float,
    ) -> float:
        arrival_rate = max(0.0, float(worker.arrival_rate))
        last = self._worker_last_arrival.get(worker.worker_id)
        if last is not None:
            idle_s = max(0.0, now - last)
            if idle_s > 0 and arrival_rate > 0:
                arrival_rate *= math.exp(-idle_s * max(worker.service_rate, 1e-6))
        load_ratio = worker.current_load / max(1, worker.max_capacity)
        queue_ratio = worker.queue_size / max(1, worker.queue_capacity)
        predictive = arrival_rate / max(worker.service_rate, 1e-6)
        return max(load_ratio, queue_ratio, min(0.99, predictive))

    def _record_degrade(self, degrade: DegradeLevel) -> None:
        key = degrade.value
        counts = self._metrics["degrade_level_counts"]
        counts[key] = counts.get(key, 0) + 1

    def _record_backend(self, backend: str) -> None:
        counts = self._metrics["transport_backend_counts"]
        counts[backend] = counts.get(backend, 0) + 1

    def _sync_request_registry(
        self,
        ctx: RequestContext,
        *,
        status: str,
        event: str,
        agent_id: Optional[str],
        step_index: int,
        handoff_id: Any = _KEEP_EXISTING,
        target_agent_id: Any = _KEEP_EXISTING,
        target_node_id: Any = _KEEP_EXISTING,
    ) -> None:
        if self.workflow_registry is None:
            return
        previous_version = ctx.registry_version_id or None
        ctx.registry_version_seq += 1
        ctx.registry_parent_version_id = previous_version
        ctx.registry_version_id = f"{ctx.request_id}:serving:{ctx.registry_version_seq}"
        ttl_deadline = ctx.created_at + max(1.0, float(self.config.request_state_ttl_seconds))
        if handoff_id is _KEEP_EXISTING:
            resolved_handoff_id = ctx.handoff_id
        else:
            resolved_handoff_id = handoff_id
        resolved_target_agent = (
            (ctx.decode_worker_id or None)
            if target_agent_id is _KEEP_EXISTING
            else target_agent_id
        )
        resolved_target_node = (
            (ctx.decode_worker_id or None)
            if target_node_id is _KEEP_EXISTING
            else target_node_id
        )
        record = WorkflowStateRecord(
            state_id=ctx.request_id,
            workflow_id=ctx.workflow_id,
            version_id=ctx.registry_version_id,
            parent_version_id=previous_version,
            snapshot_epoch=step_index,
            step_index=step_index,
            agent_id=str(agent_id or ctx.decode_worker_id or ctx.prefill_worker_id or self.config.node_id),
            status=status,
            approximate=bool(ctx.reuse_telemetry.get("approximate", False)),
            reuse_telemetry=dict(ctx.reuse_telemetry),
            ttl_deadline=ttl_deadline,
            token_ids=list(ctx.token_ids),
            image_ids=list(ctx.mm_hashes),
            feature_hashes=list(ctx.mm_hashes),
            kv_block_ids=list(ctx.block_ids),
            handoff_id=resolved_handoff_id,
            target_agent_id=resolved_target_agent,
            target_node_id=resolved_target_node,
            updated_at=time.monotonic(),
        )
        self.workflow_registry.upsert_record(
            record,
            event=event,
            preserve_existing_handoff=False,
        )

    def _workflow_registry_snapshot(self) -> Dict[str, Any]:
        if self.workflow_registry is None:
            return {"enabled": False}
        rows = self.workflow_registry.all_records()
        status_counts: Dict[str, int] = {}
        workflow_status: Dict[str, str] = {}
        active_request_status: Dict[str, str] = {}
        for row in rows:
            status_counts[row.status] = status_counts.get(row.status, 0) + 1
            workflow_status[row.workflow_id] = row.status
            if row.state_id in self._request_ctx:
                active_request_status[row.state_id] = row.status
        reuse_summary = self.workflow_registry.reuse_telemetry_summary(rows)
        return {
            "enabled": True,
            "tracked_states": len(rows),
            "active_state_ids": self.workflow_registry.active_state_ids(),
            "status_counts": status_counts,
            "workflow_status": workflow_status,
            "active_request_status": active_request_status,
            "reuse_summary": reuse_summary,
        }

    @staticmethod
    def _count_record_statuses(rows: Sequence[WorkflowStateRecord]) -> Dict[str, int]:
        status_counts: Dict[str, int] = {}
        for row in rows:
            status_counts[row.status] = status_counts.get(row.status, 0) + 1
        return status_counts

    @staticmethod
    def _avg(values: Sequence[float]) -> float:
        if not values:
            return 0.0
        return float(sum(values) / len(values))

    @staticmethod
    def _worker_to_dict(worker: WorkerLoad, *, rho: float) -> Dict[str, Any]:
        return {
            "worker_id": worker.worker_id,
            "worker_type": worker.worker_type,
            "current_load": worker.current_load,
            "queue_size": worker.queue_size,
            "avg_latency_ms": worker.avg_latency_ms,
            "gpu_utilization": worker.gpu_utilization,
            "service_rate": worker.service_rate,
            "arrival_rate": worker.arrival_rate,
            "rho": rho,
            "pool_tags": list(getattr(worker, "pool_tags", []) or []),
        }
