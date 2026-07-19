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
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ...agent.coordination.scheduler import (
    AdmissionAction,
    AdmissionDecision,
    AgentRequest,
    AgentScheduler,
    AgentType,
    DegradeLevel,
    WorkerLoad,
)
from ...agent.state_clone import AgentStateCloner
from ..state.agent_state_catalog import AgentStateCatalog, CatalogPageRef
from ..state.kv_manifest import KVManifestError, KVStateManifest, manifest_checksum
from ..state.kv_state_store import MooncakeKVStateStore
from ..state.vllm_kv_materializer import VLLMKVMaterializer
from ..state.kv_transfer_manifest_v2 import (
    KVTransferLayoutV2,
    KVTransferManifestError,
    KVTransferManifestV2,
)
from ..state.hidden_cache_key import stable_multimodal_feature_id
from ..state.page_manager import BlockRef
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
    # When unset, the catalog shares the workflow-registry WAL and uses a
    # distinct ``kind`` field.  Keeping one journal by default makes a serving
    # restart recover both aliases and page-ref facts atomically enough for the
    # local control plane without adding a new deployment prerequisite.
    agent_state_catalog_wal_path: Optional[str] = None
    request_state_ttl_seconds: float = 900.0
    owner_shards: int = 1
    kv_directory_rpc_url: Optional[str] = None
    enable_workflow_affinity: bool = True
    workflow_affinity_ttl_seconds: float = 900.0
    # Topology locality is expressed in milliseconds and remains bounded by
    # the congestion escape threshold; it is no longer a dominant fixed score.
    affinity_benefit_ms: float = 3.0
    affinity_congestion_escape_ms: float = 25.0
    affinity_congestion_hysteresis_ms: float = 10.0
    strict_no_fallback: bool = False
    # P->D control-plane envelope.  The connector owns pointer lifetime and
    # transfer completion; this manifest prevents a stale or misrouted pull
    # from reaching that data plane.
    enable_kv_transfer_manifest_v2: bool = True
    require_kv_transfer_manifest_v2: bool = False
    require_kv_transfer_manifest_generation: bool = False
    require_kv_transfer_manifest_compatibility: bool = False
    kv_transfer_manifest_lease_seconds: float = 120.0
    # Deployment-generated worker generations bind a manifest to one worker
    # incarnation. They are fences, not credentials.
    worker_generations: Tuple[Tuple[str, str], ...] = ()
    worker_generation_dir: Optional[str] = None
    enable_agent_state_clone: bool = False
    agent_state_consume_requires_kv_handoff: bool = True
    # Strict deployments require server-owned page refs/Store manifests.  The
    # legacy external-block-id API remains available only for compatibility
    # tests and must never be mistaken for a physical vLLM capture.
    require_real_agent_state_store: bool = False
    # A version-locked worker adapter supplies this acknowledgement callback.
    # The proxy can materialize Store pages without it for CPU/control-plane
    # validation, but strict Agent consumption may require the Decode-side
    # connector to accept the newly allocated block ids before dispatch.
    require_agent_state_connector_commit: bool = False
    mooncake_protocol: str = "tcp"
    high_prefill_worker_ids: Tuple[str, ...] = ()
    low_latency_decode_worker_ids: Tuple[str, ...] = ()
    standard_prefill_worker_ids: Tuple[str, ...] = ()
    standard_decode_worker_ids: Tuple[str, ...] = ()
    scheduler_policy: str = "agent_aware"
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
    # Per-stage idempotent lifecycle:
    # queued -> running -> first_token (Decode only) -> completed.
    stage_lifecycle: Dict[str, Dict[str, Any]] = field(default_factory=dict)


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
        kv_state_store: Optional[MooncakeKVStateStore] = None,
        agent_state_connector_committer: Optional[
            Callable[[Mapping[str, Any]], Optional[Mapping[str, Any]]]
        ] = None,
    ):
        self.config = config or ServingControlPlaneConfig()
        if self.config.strict_no_fallback:
            self.config.require_kv_transfer_manifest_v2 = True
        if self.config.strict_no_fallback and self.config.worker_generation_dir:
            self.config.require_kv_transfer_manifest_generation = True
        if (
            self.config.require_kv_transfer_manifest_v2
            and not self.config.enable_kv_transfer_manifest_v2
        ):
            raise ValueError(
                "require_kv_transfer_manifest_v2 requires enable_kv_transfer_manifest_v2"
            )
        if self.config.strict_no_fallback and self.config.enable_agent_state_clone:
            # Strict serving must not acknowledge a client-supplied list of
            # block identifiers as if it captured allocator-owned KV.  A real
            # state store may still be absent when Agent clone is unused, in
            # which case the capture endpoint fails closed on first use.
            self.config.require_real_agent_state_store = True
            self.config.require_agent_state_connector_commit = True
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
        self.kv_state_store = kv_state_store
        self._vllm_kv_materializer = (
            VLLMKVMaterializer(
                kv_state_store,
                commit_to_connector=agent_state_connector_committer,
            )
            if kv_state_store is not None
            else None
        )
        self._agent_state_cloner = (
            AgentStateCloner(kv_state_store=kv_state_store)
            if kv_state_store is not None
            else None
        )
        self._serving_kv_state_ids: Dict[str, str] = {}
        # A real vLLM adapter captures allocator-owned refs during a Prefill
        # request. Keep the request-to-public-state alias explicit rather than
        # requiring an HTTP client to guess a state id; the alias is also
        # persisted in WorkflowStateRecord telemetry for restart recovery.
        self._agent_capture_by_request_id: Dict[str, str] = {}
        if self.workflow_registry is None and (
            self.config.workflow_registry_wal_path or self.config.enable_agent_state_clone
        ):
            # Agent-state lifecycle APIs need a durable/catalogued state id
            # even when the caller did not explicitly configure a WAL.  A
            # path-less registry remains process-local; a configured path keeps
            # the same recovery semantics as the serving request registry.
            self.workflow_registry = WorkflowStateRegistry(self.config.workflow_registry_wal_path)
        self.agent_state_catalog: Optional[AgentStateCatalog] = None
        if self.config.enable_agent_state_clone:
            catalog_wal = (
                self.config.agent_state_catalog_wal_path
                or self.config.workflow_registry_wal_path
            )
            self.agent_state_catalog = AgentStateCatalog(
                catalog_wal,
                page_reclaimer=self._reclaim_catalog_page_objects
                if (
                    self.kv_state_store is not None
                    and self.kv_state_store.kv_page_store is not None
                )
                else None,
            )
        if self.config.connector_metrics_dir is None:
            self.config.connector_metrics_dir = (
                str(os.getenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", "")).strip() or None
            )
        self._lock = threading.RLock()
        self._prefill_scheduler = AgentScheduler([], policy=self.config.scheduler_policy)
        self._decode_scheduler = AgentScheduler([], policy=self.config.scheduler_policy)
        self._request_ctx: Dict[str, RequestContext] = {}
        self._worker_last_arrival: Dict[str, float] = {}
        self._workflow_latest: Dict[str, Dict[str, Any]] = {}
        self._workflow_affinity: Dict[str, Tuple[str, float]] = {}
        self._prefill_decode_affinity = self._normalize_prefill_decode_affinity(
            self.config.prefill_decode_affinity
        )
        self._worker_generations = self._normalize_worker_generations(
            self.config.worker_generations
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
            "kv_transfer_manifests_created": 0,
            "kv_transfer_manifests_validated": 0,
            "kv_transfer_manifests_rebound": 0,
            "kv_transfer_manifest_rejections": 0,
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

    @staticmethod
    def _normalize_worker_generations(
        values: Sequence[Tuple[str, str]] | Sequence[str],
    ) -> Dict[str, str]:
        """Validate deployment-supplied worker incarnation fences."""

        normalized: Dict[str, str] = {}
        for raw in values or ():
            if isinstance(raw, str):
                worker_id, separator, generation = raw.partition("=")
                if not separator:
                    raise ValueError("worker generation entries must use worker_id=generation")
            else:
                try:
                    worker_id, generation = raw
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "worker generation entries must contain worker_id and generation"
                    ) from exc
            worker = str(worker_id).strip()
            fence = str(generation).strip()
            if not worker or not fence or fence.lower() in {"unknown", "none", "null"}:
                raise ValueError("worker generations must be explicit non-empty fences")
            existing = normalized.get(worker)
            if existing is not None and existing != fence:
                raise ValueError(
                    f"worker {worker!r} has conflicting generation fences"
                )
            normalized[worker] = fence
        return normalized

    def _generation_for_worker(self, worker_id: Optional[str]) -> Optional[str]:
        if worker_id is None:
            return None
        worker = str(worker_id).strip()
        generation_dir = str(self.config.worker_generation_dir or "").strip()
        if generation_dir and worker and Path(worker).name == worker:
            try:
                root = Path(generation_dir).expanduser().resolve(strict=False)
                path = (root / worker).resolve(strict=False)
                if path.parent == root and path.is_file():
                    value = path.read_text(encoding="utf-8")[:256].strip()
                    if value and value.lower() not in {"unknown", "none", "null"}:
                        return value
            except OSError:
                # The worker replaces this file atomically during startup. A
                # transient read failure falls through to static compatibility
                # configuration; strict compatibility rejects an unresolved
                # generation at manifest validation time.
                pass
        return self._worker_generations.get(worker)

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
        mm_processor_kwargs = req_data.get("mm_processor_kwargs")
        for item in self._iter_content_items(req_data):
            item_type = str(item.get("type", "text"))
            if item_type in MULTIMODAL_CONTENT_TYPES:
                found_multimodal = True
                # Feature handles represent preprocessed hidden states, not
                # raw bytes. Bind image identities to the request processor
                # projection so a persistent E->P buffer cannot be reused at
                # a different Qwen resize/grid setting.
                if item_type in {"image", "image_url", "input_image"}:
                    hashes.append(stable_multimodal_feature_id(item, mm_processor_kwargs))
                else:
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
        self._upsert_kv_transfer_manifest(
            ctx,
            kv_transfer_params,
            destination_engine_id=target_node_id,
        )
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
        self._upsert_kv_transfer_manifest(
            ctx,
            kv,
            destination_engine_id=decision.worker_id,
        )
        ctx.reuse_telemetry = {
            **dict(ctx.reuse_telemetry),
            "kv_handoff": self._kv_handoff_payload(
                kv,
                block_ids=ctx.block_ids,
                source_node_id=str(ctx.prefill_worker_id or self.config.source_agent_id),
                target_node_id=decision.worker_id,
            ),
        }
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
                "affinity_benefit_ms": float(
                    self.config.affinity_benefit_ms
                ),
                "affinity_congestion_escape_ms": float(
                    self.config.affinity_congestion_escape_ms
                ),
                "affinity_congestion_hysteresis_ms": float(
                    self.config.affinity_congestion_hysteresis_ms
                ),
                "estimated_transfer_bytes": float(
                    ctx.reuse_telemetry.get("estimated_transfer_bytes", 0.0)
                    or 0.0
                ),
                "control_cost_ms": float(
                    ctx.reuse_telemetry.get("control_cost_ms", 0.0) or 0.0
                ),
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
        ctx.stage_lifecycle[str(stage)] = {
            "state": "queued",
            "worker_id": str(decision.worker.worker_id),
            "admitted_at": time.monotonic(),
            "started_at": None,
            "first_token_at": None,
            "completed_at": None,
            "success": None,
            "scheduled_tokens": max(
                1,
                int(
                    ctx.expected_output_tokens
                    if str(stage) == "decode"
                    else ctx.estimated_input_tokens
                ),
            ),
        }
        return StageRouteDecision(
            worker_id=decision.worker.worker_id,
            decision=decision,
            wait_ms=wait_ms,
        )

    def mark_stage_started(
        self,
        stage: str,
        worker_id: str,
        *,
        ctx: Optional[RequestContext] = None,
    ) -> bool:
        scheduler = self._scheduler_for_stage(stage)
        worker = next(
            (w for w in scheduler.workers if w.worker_id == worker_id), None
        )
        if worker is None:
            return False
        now = time.monotonic()
        record = (
            ctx.stage_lifecycle.get(str(stage)) if ctx is not None else None
        )
        if record is not None:
            if record.get("state") in {
                "running",
                "first_token",
                "completed",
                "failed",
                "cancelled",
            }:
                return False
            if str(record.get("worker_id") or "") != str(worker_id):
                return False
            admitted_at = float(record.get("admitted_at") or now)
        else:
            admitted_at = now
        queue_wait_ms = max(0.0, (now - admitted_at) * 1000.0)
        scheduler.mark_started(
            worker,
            queue_wait_ms=queue_wait_ms,
            scheduled_tokens=int(
                (record or {}).get("scheduled_tokens", 0) or 0
            ),
        )
        if record is not None:
            record["state"] = "running"
            record["started_at"] = now
            record["queue_wait_ms"] = queue_wait_ms
        return True

    def mark_stage_first_token(
        self,
        stage: str,
        worker_id: str,
        *,
        ctx: Optional[RequestContext] = None,
        first_token_ms: Optional[float] = None,
    ) -> bool:
        if str(stage).lower() != "decode":
            return False
        scheduler = self._scheduler_for_stage(stage)
        worker = next(
            (w for w in scheduler.workers if w.worker_id == worker_id), None
        )
        if worker is None:
            return False
        record = (
            ctx.stage_lifecycle.get(str(stage)) if ctx is not None else None
        )
        if record is not None and record.get("state") == "queued":
            self.mark_stage_started(stage, worker_id, ctx=ctx)
        if record is not None and record.get("state") in {
            "first_token",
            "completed",
            "failed",
            "cancelled",
        }:
            return False
        now = time.monotonic()
        if first_token_ms is None:
            started_at = (
                float(record.get("started_at") or now)
                if record is not None
                else now
            )
            first_token_ms = max(0.0, (now - started_at) * 1000.0)
        scheduler.mark_first_token(
            worker, first_token_ms=max(0.0, float(first_token_ms))
        )
        if record is not None:
            record["state"] = "first_token"
            record["first_token_at"] = now
            record["first_token_ms"] = max(0.0, float(first_token_ms))
        return True

    def mark_stage_complete(
        self,
        stage: str,
        worker_id: str,
        *,
        latency_ms: float,
        success: bool = True,
        ctx: Optional[RequestContext] = None,
    ) -> None:
        scheduler = self._scheduler_for_stage(stage)
        worker = next((w for w in scheduler.workers if w.worker_id == worker_id), None)
        if worker is None:
            return
        latency_ms = max(0.0, float(latency_ms))
        record = (
            ctx.stage_lifecycle.get(str(stage)) if ctx is not None else None
        )
        if record is not None and record.get("state") in {
            "completed",
            "failed",
            "cancelled",
        }:
            return
        if record is not None:
            state = str(record.get("state") or "queued")
            was_queued = state == "queued"
            first_token_observed = state == "first_token"
        else:
            # Compatibility for callers that do not carry RequestContext.
            was_queued = worker.current_load <= 0 and worker.queue_size > 0
            first_token_observed = (
                worker.worker_type == "decode"
                and worker.first_token_pending < worker.current_load
            )
        scheduler.mark_complete(
            worker,
            latency_ms=latency_ms,
            was_queued=was_queued,
            first_token_observed=first_token_observed,
            scheduled_tokens=int(
                (record or {}).get("scheduled_tokens", 0) or 0
            ),
        )
        if record is not None:
            record["state"] = "completed" if success else "failed"
            record["completed_at"] = time.monotonic()
            record["latency_ms"] = latency_ms
            record["success"] = bool(success)
        if latency_ms > 0:
            if worker.avg_latency_ms <= 0:
                worker.avg_latency_ms = latency_ms
            else:
                worker.avg_latency_ms = worker.avg_latency_ms * 0.8 + latency_ms * 0.2
            inst_service_rate = 1000.0 / latency_ms
            worker.service_rate = max(1e-6, worker.service_rate * 0.8 + inst_service_rate * 0.2)
        if not success:
            worker.arrival_rate = max(0.0, worker.arrival_rate * 0.9)

    def mark_stage_failed_or_cancelled(
        self,
        stage: str,
        worker_id: str,
        *,
        ctx: Optional[RequestContext] = None,
        latency_ms: float = 0.0,
        cancelled: bool = False,
    ) -> None:
        """Idempotently release lifecycle counts on errors or cancellation."""

        self.mark_stage_complete(
            stage,
            worker_id,
            latency_ms=latency_ms,
            success=False,
            ctx=ctx,
        )
        record = (
            ctx.stage_lifecycle.get(str(stage)) if ctx is not None else None
        )
        if record is not None and cancelled and record.get("state") == "failed":
            record["state"] = "cancelled"

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
                    "agent_state_catalog_wal_path": (
                        self.config.agent_state_catalog_wal_path
                        or self.config.workflow_registry_wal_path
                    ),
                    "owner_shards": int(self.config.owner_shards),
                    "kv_directory_rpc_url": self.config.kv_directory_rpc_url,
                    "enable_workflow_affinity": self.config.enable_workflow_affinity,
                    "strict_no_fallback": self.config.strict_no_fallback,
                    "enable_kv_transfer_manifest_v2": self.config.enable_kv_transfer_manifest_v2,
                    "require_kv_transfer_manifest_v2": self.config.require_kv_transfer_manifest_v2,
                    "require_kv_transfer_manifest_generation": self.config.require_kv_transfer_manifest_generation,
                    "require_kv_transfer_manifest_compatibility": self.config.require_kv_transfer_manifest_compatibility,
                    "kv_transfer_manifest_lease_seconds": self.config.kv_transfer_manifest_lease_seconds,
                    "worker_generations": dict(self._worker_generations),
                    "worker_generation_dir": self.config.worker_generation_dir,
                    "enable_agent_state_clone": self.config.enable_agent_state_clone,
                    "require_real_agent_state_store": self.config.require_real_agent_state_store,
                    "require_agent_state_connector_commit": self.config.require_agent_state_connector_commit,
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
                    "peer_buffer_native_engine_lock_wait_ms": (
                        connector_totals.peer_buffer_native_engine_lock_wait_ms
                    ),
                    "peer_buffer_native_engine_lock_wait_ms_avg": (
                        connector_totals.peer_buffer_native_engine_lock_wait_ms
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
                    "layered_source_ready_event_waits": connector_totals.source_ready_event_waits,
                    "layered_source_ready_event_wait_ms": connector_totals.source_ready_event_wait_ms,
                    "layered_source_ready_event_wait_ms_avg": (
                        connector_totals.source_ready_event_wait_ms
                        / connector_totals.source_ready_event_waits
                        if connector_totals.source_ready_event_waits
                        else 0.0
                    ),
                    "layered_source_ready_sync_fallbacks": connector_totals.source_ready_sync_fallbacks,
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
    def _optional_generation(
        params: Mapping[str, Any],
        *names: str,
    ) -> Optional[str]:
        for name in names:
            value = params.get(name)
            if value is not None and str(value).strip():
                return str(value).strip()
        return None

    def _kv_transfer_layout_from_params(
        self,
        params: Mapping[str, Any],
    ) -> KVTransferLayoutV2:
        raw_layout = params.get("kv_layout_v2")
        if raw_layout is not None:
            if not isinstance(raw_layout, Mapping):
                raise KVTransferManifestError("kv_layout_v2 must be an object")
            return KVTransferLayoutV2.from_control_payload(raw_layout)
        try:
            return KVTransferLayoutV2(
                model_id=str(params.get("model_id") or params.get("model") or "unknown"),
                model_revision=str(params.get("model_revision") or "unknown"),
                vllm_adapter_version=str(params.get("vllm_adapter_version") or "unknown"),
                dtype=str(params.get("kv_dtype") or params.get("dtype") or "unknown"),
                block_size=int(params.get("kv_block_size", params.get("block_size", 0)) or 0),
                num_layers=int(params.get("kv_num_layers", params.get("num_layers", 0)) or 0),
                tp_size=int(params.get("tp_size", 1) or 1),
                tp_rank=int(params.get("tp_rank", 0) or 0),
                extra={"source": "legacy_kv_transfer_params"},
            )
        except (TypeError, ValueError) as exc:
            raise KVTransferManifestError(f"invalid KV layout fields: {exc}") from exc

    def _build_kv_transfer_manifest(
        self,
        ctx: RequestContext,
        params: Mapping[str, Any],
        *,
        destination_engine_id: str,
    ) -> KVTransferManifestV2:
        source_engine_id = str(params.get("remote_engine_id") or "").strip()
        if not source_engine_id:
            raise KVTransferManifestError("remote_engine_id is required for a KV transfer manifest")
        layout = self._kv_transfer_layout_from_params(params)
        raw_layer_groups = params.get("kv_layer_groups_v2", params.get("layer_groups"))
        if raw_layer_groups is None and int(layout.num_layers) > 0:
            size = max(1, int(params.get("layers_per_group", self.config.layers_per_group) or 1))
            raw_layer_groups = [
                [start, min(int(layout.num_layers), start + size)]
                for start in range(0, int(layout.num_layers), size)
            ]
        raw_transport = params.get("kv_transport_v2")
        if raw_transport is None:
            transport: Dict[str, Any] = {}
        elif isinstance(raw_transport, Mapping):
            transport = dict(raw_transport)
        else:
            raise KVTransferManifestError("kv_transport_v2 must be an object")
        transport.setdefault("protocol", str(params.get("mooncake_protocol") or self.config.mooncake_protocol))
        transport.setdefault("backend", str(params.get("transport_backend") or self.config.transport_backend))
        transport.setdefault("remote_bootstrap_addr", str(params.get("remote_bootstrap_addr") or ""))
        transport.setdefault("source_node_id", str(ctx.prefill_worker_id or self.config.source_agent_id))
        transport.setdefault("destination_node_id", str(destination_engine_id))
        try:
            token_count = int(
                params.get("remote_prefill_prompt_tokens", params.get("token_count", len(ctx.token_ids)))
                or 0
            )
        except (TypeError, ValueError) as exc:
            raise KVTransferManifestError(f"invalid KV transfer token_count: {exc}") from exc
        return KVTransferManifestV2.create(
            transfer_id=str(params.get("transfer_id") or ctx.transfer_id or ctx.request_id),
            workflow_id=ctx.workflow_id,
            source_engine_id=source_engine_id,
            source_generation=(
                self._optional_generation(
                    params,
                    "source_generation",
                    "producer_generation",
                    "remote_worker_generation",
                )
                or self._generation_for_worker(ctx.prefill_worker_id)
                or "unknown"
            ),
            destination_engine_id=str(destination_engine_id),
            destination_generation=(
                self._generation_for_worker(destination_engine_id)
                or self._optional_generation(
                    params,
                    "destination_generation",
                    "consumer_generation",
                    "target_worker_generation",
                )
                or "unknown"
            ),
            layout=layout,
            remote_block_ids=params.get("remote_block_ids"),
            token_count=token_count,
            layer_groups=raw_layer_groups,
            transport=transport,
            attempt=int(params.get("kv_transfer_attempt", 0) or 0),
            lease_seconds=max(0.0, float(self.config.kv_transfer_manifest_lease_seconds)),
            metadata={
                "routing_path": ctx.routing_path,
                "request_id": ctx.request_id,
                "source_node_id": str(ctx.prefill_worker_id or self.config.source_agent_id),
                "destination_node_id": str(destination_engine_id),
            },
        )

    def _upsert_kv_transfer_manifest(
        self,
        ctx: RequestContext,
        params: Dict[str, Any],
        *,
        destination_engine_id: str,
    ) -> Optional[KVTransferManifestV2]:
        """Attach or validate the P-to-D envelope before connector dispatch.

        The proxy owns a trusted Prefill response and may rebind a valid source
        manifest when Decode admission changes the selected worker.  Rebinding
        does not extend the original source lease; it only changes the
        consumer-facing fence and increments the attempt.
        """

        raw_manifest = params.get("kv_transfer_manifest_v2")
        if raw_manifest is None and not self.config.enable_kv_transfer_manifest_v2:
            if self.config.require_kv_transfer_manifest_v2:
                raise RuntimeError("KV transfer manifest v2 is required but disabled")
            return None
        try:
            source_engine_id = str(params.get("remote_engine_id") or "").strip()
            source_generation = (
                self._optional_generation(
                    params,
                    "source_generation",
                    "producer_generation",
                    "remote_worker_generation",
                )
                or self._generation_for_worker(ctx.prefill_worker_id)
            )
            destination_generation = (
                self._generation_for_worker(destination_engine_id)
                or self._optional_generation(
                    params,
                    "destination_generation",
                    "consumer_generation",
                    "target_worker_generation",
                )
            )
            if raw_manifest is None:
                manifest = self._build_kv_transfer_manifest(
                    ctx,
                    params,
                    destination_engine_id=destination_engine_id,
                )
                action = "created"
            else:
                manifest = KVTransferManifestV2.from_control_payload(raw_manifest)
                manifest.validate_for_consumer(
                    transfer_id=str(params.get("transfer_id") or ctx.transfer_id or ctx.request_id),
                    workflow_id=ctx.workflow_id,
                    source_engine_id=source_engine_id,
                    source_generation=source_generation,
                    require_generation_fences=self.config.require_kv_transfer_manifest_generation,
                    require_compatibility=self.config.require_kv_transfer_manifest_compatibility,
                )
                target_changed = manifest.destination_engine_id != str(destination_engine_id)
                generation_changed = (
                    destination_generation is not None
                    and manifest.destination_generation != destination_generation
                )
                if target_changed or generation_changed:
                    manifest = manifest.rebind_destination(
                        destination_engine_id=str(destination_engine_id),
                        destination_generation=destination_generation or manifest.destination_generation,
                    )
                    action = "rebound"
                else:
                    action = "validated"
            manifest.validate_for_consumer(
                transfer_id=str(params.get("transfer_id") or ctx.transfer_id or ctx.request_id),
                workflow_id=ctx.workflow_id,
                source_engine_id=source_engine_id,
                source_generation=source_generation,
                destination_engine_id=str(destination_engine_id),
                destination_generation=destination_generation,
                require_generation_fences=self.config.require_kv_transfer_manifest_generation,
                require_compatibility=self.config.require_kv_transfer_manifest_compatibility,
            )
        except (KVTransferManifestError, TypeError, ValueError) as exc:
            with self._lock:
                self._metrics["kv_transfer_manifest_rejections"] += 1
                self._record_path_metric(ctx.routing_path, "kv_transfer_manifest_rejections")
            raise RuntimeError(f"KV transfer manifest rejected: {exc}") from exc
        params["kv_transfer_manifest_v2"] = manifest.as_control_payload()
        with self._lock:
            metric_key = {
                "created": "kv_transfer_manifests_created",
                "validated": "kv_transfer_manifests_validated",
                "rebound": "kv_transfer_manifests_rebound",
            }[action]
            self._metrics[metric_key] += 1
            self._record_path_metric(ctx.routing_path, metric_key)
        return manifest

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
        # Agent state has a distinct lease/pinning protocol.  Persist the
        # source P->D envelope for audit only; do not pass it through as an
        # active manifest for a later branch with a different transfer id.
        if kv.get("kv_transfer_manifest_v2") is not None:
            payload["source_kv_transfer_manifest_v2"] = kv["kv_transfer_manifest_v2"]
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

    def _reclaim_catalog_page_objects(self, refs: Sequence[CatalogPageRef]) -> None:
        """Delete only Store objects proven unreferenced by the catalog.

        ``AgentStateCatalog`` owns the refcount decision and invokes this
        callback only after a release transition is durable.  The page store
        still performs per-key status validation, so a failed backend delete is
        surfaced and retried rather than being reported as a successful GC.
        """

        if self.kv_state_store is None or self.kv_state_store.kv_page_store is None:
            raise RuntimeError("catalog page GC requires a MooncakeKVPageStore")
        keys = [
            key
            for ref in refs
            for key in (ref.key_store_key, ref.value_store_key)
        ]
        self.kv_state_store.kv_page_store.remove_page_objects(keys)

    def _catalog_register_store_state(
        self,
        *,
        public_state_id: str,
        descriptor,
        target_node_id: str,
        lease_deadline_unix: float,
        status: str,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Persist a Store/state-store descriptor after real export succeeds."""

        catalog = self.agent_state_catalog
        if catalog is None:
            return
        catalog.register_state(
            state_id=str(public_state_id),
            workflow_id=str(descriptor.workflow_id),
            state_store_id=str(descriptor.state_id),
            manifests=descriptor.manifests,
            owner_node_id=str(descriptor.owner_node_id),
            target_node_id=str(target_node_id),
            lease_deadline_unix=float(lease_deadline_unix),
            status=str(status),
            metadata={
                "store_backed": bool(
                    descriptor.manifests
                    and all(manifest.has_store_payload for manifest in descriptor.manifests)
                ),
                **dict(metadata or {}),
            },
        )

    def _restore_catalogued_store_state(
        self,
        public_state_id: str,
        *,
        target_node_id: str,
        feature_hashes: Optional[Sequence[str]] = None,
    ):
        """Rehydrate a WAL-recovered state from immutable Store manifests.

        A fresh proxy must not reuse a catalogued physical block id.  Instead,
        the local state store performs destination allocation plus checked
        ``batch_get_into`` import, then the normal materialize/fork paths see
        newly allocated block refs.
        """

        if self.kv_state_store is None or self.agent_state_catalog is None:
            return None
        catalog_record = self.agent_state_catalog.get_record(public_state_id)
        if catalog_record is None:
            return None
        if catalog_record.status == "RELEASED":
            raise RuntimeError(f"catalogued Agent state is already released: {public_state_id}")
        remaining = float(catalog_record.lease_deadline_unix) - time.time()
        if remaining <= 0.0:
            raise RuntimeError(f"catalogued Agent state lease expired: {public_state_id}")
        store_state_id = catalog_record.state_store_id
        descriptor = self.kv_state_store.get_state(store_state_id)
        if descriptor is None:
            descriptor = self.kv_state_store.restore_state_from_store(
                store_state_id,
                catalog_record.manifests,
                target_node_id=str(target_node_id),
                ttl_deadline=time.monotonic() + max(1.0, remaining),
                metadata={"catalog_recovered": True, "public_state_id": public_state_id},
                feature_hashes=feature_hashes,
            )
        self._serving_kv_state_ids[str(public_state_id)] = descriptor.state_id
        return descriptor

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
        parent_state_id = str(source.get("request_id") or parent_request_id or "")
        if self._agent_state_cloner is not None and self.kv_state_store is not None:
            catalog_parent = (
                self.agent_state_catalog.get_record(parent_state_id)
                if self.agent_state_catalog is not None
                else None
            )
            if parent_state_id not in self._serving_kv_state_ids and catalog_parent is not None:
                self._restore_catalogued_store_state(
                    parent_state_id,
                    target_node_id=str(catalog_parent.owner_node_id),
                    feature_hashes=list(source.get("mm_hashes") or []),
                )
            if parent_state_id in self._serving_kv_state_ids:
                return self._fork_store_workflow_state(
                    workflow_id=workflow_id,
                    parent_state_id=parent_state_id,
                    branch_count=branch_count,
                    target_node_id=target_node_id or self.kv_state_store.node_id,
                    for_write=for_write,
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
        source_nodes = {
            str(record.physical_node_id)
            for gid in block_ids
            for record in [self.kv_directory.get_record(gid)]
            if record is not None
        }
        if not source_nodes:
            with self._lock:
                self._metrics["agent_state_clone_failures"] += 1
            raise RuntimeError("cannot determine KV owner node for workflow fork")
        same_node = len(source_nodes) == 1 and target in source_nodes
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
                        # No tensor bytes are copied by this control-plane
                        # operation. Physical same-node zero-copy and
                        # cross-node descriptor sharing are distinct contracts.
                        "zero_copy": bool(same_node),
                        "descriptor_shared": bool(not same_node),
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
                                "zero_copy": bool(same_node),
                                "descriptor_shared": bool(not same_node),
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
            self._metrics["agent_state_clone_zero_copy_branches"] += branch_count if same_node else 0
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
            "zero_copy_branches": branch_count if same_node else 0,
            "descriptor_shared_branches": branch_count if not same_node else 0,
            "clone_semantics": (
                "same_node_block_ref_zero_copy"
                if same_node
                else "cross_node_descriptor_share"
            ),
            "target_node_id": target,
        }

    def _fork_store_workflow_state(
        self,
        *,
        workflow_id: str,
        parent_state_id: str,
        branch_count: int,
        target_node_id: str,
        for_write: bool,
    ) -> Dict[str, Any]:
        """Fork a server-captured KV state through the real state-store path.

        No client-provided physical IDs participate here.  The state store
        owns page references/leases, and a Store-backed cross-node child is
        descriptor-only until ``materialize_agent_state`` requests target
        blocks.  On any partial failure every child state is released.
        """

        assert self._agent_state_cloner is not None
        assert self.kv_state_store is not None
        parent_store_state_id = self._serving_kv_state_ids[parent_state_id]
        parent = self.kv_state_store.get_state(parent_store_state_id)
        if parent is None:
            raise RuntimeError(f"captured Agent state is unavailable: {parent_state_id}")
        target = str(target_node_id or self.kv_state_store.node_id)
        catalog = self.agent_state_catalog
        parent_catalog = catalog.get_record(parent_state_id) if catalog is not None else None
        if catalog is not None and parent_catalog is None:
            # Compatibility/recovery bridge for a descriptor registered before
            # the catalog was enabled.  It still records only server-owned
            # manifests, never an external physical-id placeholder.
            self._catalog_register_store_state(
                public_state_id=parent_state_id,
                descriptor=parent,
                target_node_id=parent.owner_node_id,
                lease_deadline_unix=time.time()
                + max(1.0, float(self.config.request_state_ttl_seconds)),
                status="ACTIVE",
                metadata={"catalog_backfilled": True},
            )
            parent_catalog = catalog.get_record(parent_state_id)
        branches: List[Dict[str, Any]] = []
        created: List[str] = []
        catalog_created: List[str] = []
        try:
            for _ in range(int(branch_count)):
                branch_id = f"{workflow_id}:branch:{uuid.uuid4().hex}"
                if target == parent.owner_node_id:
                    branch = self._agent_state_cloner.clone_kv_same_node_zero_copy(
                        parent_store_state_id,
                        branch_id,
                    )
                else:
                    branch = self._agent_state_cloner.share_kv_state_descriptor_cross_node(
                        parent_store_state_id,
                        branch_id,
                        target_node_id=target,
                    )
                descriptor = self.kv_state_store.get_state(branch.kv_state_id or branch_id)
                if descriptor is None:
                    raise RuntimeError(f"missing child descriptor for branch={branch_id}")
                created.append(descriptor.state_id)
                if catalog is not None:
                    catalog.fork_state(
                        parent_state_id=parent_state_id,
                        child_state_id=branch_id,
                        child_state_store_id=descriptor.state_id,
                        manifests=descriptor.manifests,
                        owner_node_id=descriptor.owner_node_id,
                        target_node_id=target,
                        lease_deadline_unix=(
                            parent_catalog.lease_deadline_unix
                            if parent_catalog is not None
                            else time.time()
                            + max(1.0, float(self.config.request_state_ttl_seconds))
                        ),
                        status=(
                            "MATERIALIZE_REQUIRED"
                            if target != parent.owner_node_id
                            else "ACTIVE"
                        ),
                        metadata={"for_write": bool(for_write)},
                    )
                    catalog_created.append(branch_id)
                self._serving_kv_state_ids[branch_id] = descriptor.state_id
                manifests = [manifest.as_control_payload() for manifest in descriptor.manifests]
                record = WorkflowStateRecord(
                    state_id=branch_id,
                    workflow_id=workflow_id,
                    version_id=descriptor.version_id,
                    parent_version_id=parent.version_id,
                    snapshot_epoch=descriptor.snapshot_epoch,
                    step_index=1,
                    agent_id=target,
                    status="AGENT_STATE_MATERIALIZE_REQUIRED" if target != parent.owner_node_id else "AGENT_BRANCH_ACTIVE",
                    reuse_telemetry={
                        "agent_state_clone": True,
                        "store_backed": True,
                        "clone_semantics": branch.clone_semantics,
                        "for_write": bool(for_write),
                        "clone_payload_bytes": 0,
                    },
                    ttl_deadline=descriptor.ttl_deadline,
                    kv_block_ids=list(descriptor.block_ids),
                    kv_logical_filled=[int(ref.filled) for ref in self.kv_state_store.get_refs(descriptor.state_id)],
                    kv_manifests=manifests,
                    target_agent_id=target,
                    target_node_id=target,
                )
                if self.workflow_registry is not None:
                    self.workflow_registry.upsert_record(
                        record,
                        event="AGENT_STATE_STORE_FORKED",
                        preserve_existing_handoff=False,
                    )
                branches.append(
                    {
                        "branch_id": branch_id,
                        "agent_state_store_state_id": descriptor.state_id,
                        "clone_semantics": branch.clone_semantics,
                        "zero_copy": branch.clone_semantics == "same_node_block_ref_zero_copy",
                        "descriptor_shared": branch.clone_semantics == "cross_node_descriptor_share",
                        "clone_payload_bytes": 0,
                        "kv_block_ids": list(descriptor.block_ids),
                        "kv_manifests": manifests,
                        "target_node_id": target,
                    }
                )
        except Exception:
            if catalog is not None:
                for branch_id in reversed(catalog_created):
                    try:
                        catalog.release_state(branch_id, reclaim=False)
                    except Exception:
                        pass
            for state_id in reversed(created):
                self.kv_state_store.release_state(state_id)
            for branch_id in catalog_created:
                self._serving_kv_state_ids.pop(branch_id, None)
            for branch in branches:
                self._serving_kv_state_ids.pop(str(branch["branch_id"]), None)
            with self._lock:
                self._metrics["agent_state_clone_failures"] += 1
            raise
        with self._lock:
            self._metrics["agent_state_clone_requests"] += 1
            self._metrics["agent_state_clone_branches"] += len(branches)
            self._metrics["agent_state_clone_zero_copy_branches"] += sum(
                1 for branch in branches if branch["zero_copy"]
            )
        return {
            "workflow_id": workflow_id,
            "parent_request_id": parent_state_id,
            "branch_count": len(branches),
            "branches": branches,
            "copied_bytes": 0,
            "clone_semantics": (
                "same_node_physical_zero_copy"
                if all(branch["zero_copy"] for branch in branches)
                else "cross_node_manifest_clone"
            ),
            "target_node_id": target,
        }

    def register_agent_state(
        self,
        *,
        workflow_id: str,
        state_id: Optional[str] = None,
        capture_request_id: Optional[str] = None,
        kv_block_ids: Sequence[str],
        kv_refs: Optional[Sequence[BlockRef]] = None,
        kv_logical_filled: Optional[Sequence[int]] = None,
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
        if not block_ids and not kv_refs:
            raise ValueError("kv_block_ids or server-owned kv_refs is required")
        registry = self.workflow_registry
        if registry is None:  # defensive; __init__ creates it for clone-enabled planes.
            registry = WorkflowStateRegistry()
            self.workflow_registry = registry

        state_id = str(state_id or f"{workflow_id}:state:{uuid.uuid4().hex}")
        capture_request_id = str(capture_request_id or state_id).strip()
        existing = registry.get_record(state_id)
        if existing is not None and existing.status != "RELEASED":
            if capture_request_id:
                with self._lock:
                    self._agent_capture_by_request_id[capture_request_id] = state_id
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
                "kv_manifests": list(existing.kv_manifests),
            }

        target = str(target_node_id or self.config.target_agent_id)
        now = time.monotonic()
        ttl_seconds = max(1.0, float(self.config.request_state_ttl_seconds))
        ttl_deadline = now + ttl_seconds
        catalog_lease_deadline_unix = time.time() + ttl_seconds
        refcounts: Dict[str, int] = {}
        store_state_id: Optional[str] = None
        manifests: List[Dict[str, Any]] = []
        if kv_refs is not None:
            if self.kv_state_store is None:
                raise RuntimeError("server-owned kv_refs require MooncakeKVStateStore")
            descriptor = self.kv_state_store.register_state(
                list(kv_refs),
                workflow_id=workflow_id,
                state_id=state_id,
                ttl_deadline=ttl_deadline,
                metadata={"captured_by": "ServingControlPlane", "target_node_id": target},
            )
            try:
                if self.kv_state_store.kv_page_store is not None:
                    descriptor = self.kv_state_store.export_state_to_store(descriptor.state_id)
                self._catalog_register_store_state(
                    public_state_id=state_id,
                    descriptor=descriptor,
                    target_node_id=target,
                    lease_deadline_unix=catalog_lease_deadline_unix,
                    status="ACTIVE",
                    metadata={"serving_status": str(status)},
                )
            except Exception:
                self.kv_state_store.release_state(descriptor.state_id)
                raise
            store_state_id = descriptor.state_id
            block_ids = list(descriptor.block_ids)
            refcounts = {
                gid: self.kv_state_store.pm.refcount(gid)
                for gid in block_ids
            }
            manifests = [manifest.as_control_payload() for manifest in descriptor.manifests]
        else:
            if self.config.require_real_agent_state_store:
                raise RuntimeError(
                    "agent state capture requires server-owned KV refs; external block ids are disabled"
                )
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
                "agent_state_capture_request_id": capture_request_id,
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
            kv_logical_filled=[int(value) for value in list(kv_logical_filled or [])],
            kv_manifests=manifests,
            target_agent_id=target,
            target_node_id=target,
            updated_at=now,
        )
        registry.upsert_record(
            record,
            event="AGENT_STATE_REGISTERED",
            preserve_existing_handoff=False,
        )
        if store_state_id is not None:
            self._serving_kv_state_ids[state_id] = store_state_id
        if capture_request_id:
            with self._lock:
                self._agent_capture_by_request_id[capture_request_id] = state_id
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
            "kv_manifests": manifests,
            "store_backed": store_state_id is not None,
        }

    def capture_agent_state(
        self,
        *,
        request_id: str,
        state_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return a previously server-captured Agent state for a request.

        The public HTTP surface intentionally has no way to turn a client
        supplied block id into a physical capture.  A version-locked vLLM
        adapter must first call :meth:`register_agent_state` in-process with
        allocator-owned ``kv_refs``.  This endpoint then provides an
        idempotent request-id based handle for the serving proxy.  Until that
        adapter capture exists it returns capability-unavailable rather than
        fabricating an external-placeholder success.
        """

        request_id = str(request_id or "").strip()
        public_state_id = str(state_id or "").strip()
        if not request_id:
            raise ValueError("request_id is required for Agent state capture")
        if not public_state_id:
            with self._lock:
                public_state_id = str(self._agent_capture_by_request_id.get(request_id) or "")
        if not public_state_id and self.workflow_registry is not None:
            # The in-memory map is intentionally only a cache. WAL recovery
            # reconstructs the request alias from the durable registry row.
            for candidate in self.workflow_registry.all_records():
                if str(candidate.reuse_telemetry.get("agent_state_capture_request_id") or "") == request_id:
                    public_state_id = candidate.state_id
                    break
        if not public_state_id:
            public_state_id = request_id
        descriptor_id = self._serving_kv_state_ids.get(public_state_id)
        descriptor = (
            self.kv_state_store.get_state(descriptor_id)
            if self.kv_state_store is not None and descriptor_id is not None
            else None
        )
        if descriptor is None:
            raise NotImplementedError(
                "Agent state capture is unavailable for request_id="
                f"{request_id}: the vLLM adapter has not registered "
                "server-owned allocator KV refs"
            )
        result = self.get_agent_state(public_state_id)
        result.update(
            {
                "captured": True,
                "idempotent": True,
                "capture_request_id": request_id,
                "agent_state_store_state_id": descriptor.state_id,
            }
        )
        return result

    def get_agent_state(self, state_id: str) -> Dict[str, Any]:
        """Expose durable Agent-state facts without exposing page payloads.

        The response is operational metadata only: page/block ids are useful
        to the internal connector, while physical pointers and tensor bytes
        remain exclusively allocator/Store owned.
        """

        registry = self.workflow_registry
        if registry is None:
            raise KeyError("workflow registry is not enabled")
        state_id = str(state_id or "").strip()
        record = registry.get_record(state_id)
        if record is None:
            raise KeyError(f"unknown agent state_id={state_id}")
        store_state_id = self._serving_kv_state_ids.get(state_id)
        descriptor = (
            self.kv_state_store.get_state(store_state_id)
            if self.kv_state_store is not None and store_state_id is not None
            else None
        )
        catalog_record = (
            self.agent_state_catalog.get_record(state_id)
            if self.agent_state_catalog is not None
            else None
        )
        manifests = list(record.kv_manifests)
        if descriptor is not None:
            manifests = [manifest.as_control_payload() for manifest in descriptor.manifests]
        elif catalog_record is not None:
            manifests = list(catalog_record.manifests)
        manifest_checksums = [
            str(manifest.get("checksum") or "")
            for manifest in manifests
            if isinstance(manifest, Mapping)
        ]
        block_ids = list(descriptor.block_ids) if descriptor is not None else list(record.kv_block_ids)
        owner_node_id = (
            descriptor.owner_node_id
            if descriptor is not None
            else catalog_record.owner_node_id
            if catalog_record is not None
            else record.agent_id
        )
        target_node_id = (
            catalog_record.target_node_id
            if catalog_record is not None and catalog_record.target_node_id is not None
            else record.target_node_id
            or record.target_agent_id
            or owner_node_id
        )
        return {
            "state_id": state_id,
            "workflow_id": record.workflow_id,
            "status": record.status,
            "version_id": record.version_id,
            "parent_version_id": record.parent_version_id,
            "snapshot_epoch": record.snapshot_epoch,
            "owner_node_id": owner_node_id,
            "target_node_id": target_node_id,
            "lease": {
                "workflow_deadline_monotonic": record.ttl_deadline,
                "catalog_deadline_unix": (
                    catalog_record.lease_deadline_unix
                    if catalog_record is not None
                    else None
                ),
            },
            "physical_state": {
                "store_backed": bool(
                    descriptor is not None
                    and descriptor.manifests
                    and all(manifest.has_store_payload for manifest in descriptor.manifests)
                )
                or bool(
                    catalog_record is not None
                    and catalog_record.manifests
                    and all(
                        isinstance(manifest, Mapping)
                        and manifest.get("key_store_key")
                        and manifest.get("value_store_key")
                        for manifest in catalog_record.manifests
                    )
                ),
                "allocator_state_live": descriptor is not None,
                "agent_state_store_state_id": store_state_id,
                "materialized": bool(
                    descriptor is not None
                    and (
                        descriptor.owner_node_id == str(target_node_id)
                        or descriptor.metadata.get("materialized_from_owner_node_id")
                    )
                ),
            },
            "kv": {
                "block_ids": block_ids,
                "logical_filled": list(record.kv_logical_filled),
                "manifest_checksums": manifest_checksums,
                "manifest_count": len(manifests),
            },
            "catalog": (
                {
                    "status": catalog_record.status,
                    "catalog_version": catalog_record.catalog_version,
                    "fence_token": catalog_record.fence_token,
                    "clone_payload_bytes": catalog_record.clone_payload_bytes,
                    "materialized_bytes": catalog_record.materialized_bytes,
                    "gc_completed": catalog_record.gc_completed,
                }
                if catalog_record is not None
                else {"enabled": False}
            ),
            "orphan_block_count": len(
                self.kv_directory.orphan_block_ids()
                if hasattr(self.kv_directory, "orphan_block_ids")
                else []
            ),
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
        store_state_id = self._serving_kv_state_ids.get(state_id)
        if (
            self.kv_state_store is not None
            and store_state_id is None
            and self.agent_state_catalog is not None
            and self.agent_state_catalog.get_record(state_id) is not None
        ):
            restored = self._restore_catalogued_store_state(
                state_id,
                target_node_id=target,
                feature_hashes=list(record.feature_hashes),
            )
            store_state_id = None if restored is None else restored.state_id
        if self.kv_state_store is not None and store_state_id is not None:
            descriptor = self.kv_state_store.get_state(store_state_id)
            if descriptor is None:
                with self._lock:
                    self._metrics["agent_state_materialize_failures"] += 1
                raise RuntimeError(f"captured Agent state is unavailable: {state_id}")
            catalog = self.agent_state_catalog
            if catalog is not None:
                catalog_record = catalog.get_record(state_id)
                if catalog_record is None:
                    self._catalog_register_store_state(
                        public_state_id=state_id,
                        descriptor=descriptor,
                        target_node_id=target,
                        lease_deadline_unix=time.time()
                        + max(1.0, float(self.config.request_state_ttl_seconds)),
                        status="ACTIVE",
                        metadata={"catalog_backfilled": True},
                    )
                    catalog_record = catalog.get_record(state_id)
                if (
                    catalog_record is None
                    or catalog_record.status == "RELEASED"
                    or catalog_record.lease_deadline_unix <= time.time()
                ):
                    with self._lock:
                        self._metrics["agent_state_materialize_failures"] += 1
                    raise RuntimeError(f"Agent state catalog lease is unavailable: {state_id}")
            else:
                catalog_record = None
            try:
                if self._vllm_kv_materializer is None:
                    raise RuntimeError("Agent state materializer is not configured")
                materialization = (
                    self._vllm_kv_materializer.materialize_write(
                        store_state_id,
                        target_node_id=target,
                        require_connector_commit=bool(
                            self.config.require_agent_state_connector_commit
                        ),
                    )
                    if target != descriptor.owner_node_id or for_write
                    else self._vllm_kv_materializer.materialize_read(
                        store_state_id,
                        target_node_id=target,
                        require_connector_commit=bool(
                            self.config.require_agent_state_connector_commit
                        ),
                    )
                )
                descriptor = self.kv_state_store.get_state(store_state_id)
                if descriptor is None:
                    raise RuntimeError(
                        f"materializer did not retain Agent state descriptor: {state_id}"
                    )
                self._serving_kv_state_ids[state_id] = descriptor.state_id
                materialized_bytes = int(materialization.materialized_bytes)
                if catalog is not None:
                    catalog.update_materialized(
                        state_id,
                        manifests=descriptor.manifests,
                        owner_node_id=descriptor.owner_node_id,
                        target_node_id=target,
                        materialized_bytes=materialized_bytes,
                        expected_version=(
                            catalog_record.catalog_version
                            if catalog_record is not None
                            else None
                        ),
                    )
                record.kv_block_ids = list(descriptor.block_ids)
                record.kv_logical_filled = [
                    int(ref.filled) for ref in self.kv_state_store.get_refs(descriptor.state_id)
                ]
                record.kv_manifests = [manifest.as_control_payload() for manifest in descriptor.manifests]
                record.reuse_telemetry = {
                    **dict(record.reuse_telemetry),
                    "agent_state_materialized": True,
                    "materialized_bytes": materialized_bytes,
                }
                updated = registry.upsert_record(
                    record,
                    event="AGENT_STATE_STORE_MATERIALIZED",
                    preserve_existing_handoff=True,
                    preserve_existing_reuse=False,
                )
            except Exception:
                with self._lock:
                    self._metrics["agent_state_materialize_failures"] += 1
                raise
            with self._lock:
                self._metrics["agent_state_materialize_success"] += 1
            return {
                "state_id": state_id,
                "workflow_id": record.workflow_id,
                "materialized": True,
                "for_write": bool(for_write),
                "target_node_id": target,
                "clone_semantics": (
                    "same_node_physical_zero_copy"
                    if descriptor.owner_node_id == self.kv_state_store.node_id
                    else "cross_node_materialized_copy"
                ),
                "kv_block_ids": list(descriptor.block_ids),
                "kv_manifests": list(record.kv_manifests),
                "manifest_checksums": [manifest.checksum for manifest in descriptor.manifests],
                "materialized_bytes": materialized_bytes,
                "backend": materialization.backend,
                "connector_committed": materialization.committed,
                "connector_commit": materialization.connector_metadata.get("connector_commit"),
                "status": updated.status,
            }
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
            catalog_gc_objects = 0
            if self.agent_state_catalog is not None:
                catalog_gc_objects = self.agent_state_catalog.collect_garbage()
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
                "catalog_gc_objects": catalog_gc_objects,
                "status": "RELEASED",
            }

        store_state_id = self._serving_kv_state_ids.pop(state_id, None)
        with self._lock:
            for request_id, captured_state_id in list(self._agent_capture_by_request_id.items()):
                if captured_state_id == state_id:
                    self._agent_capture_by_request_id.pop(request_id, None)
        catalog_record = (
            self.agent_state_catalog.get_record(state_id)
            if self.agent_state_catalog is not None
            else None
        )
        if (
            self.kv_state_store is not None
            and store_state_id is None
            and catalog_record is not None
        ):
            recovered_descriptor = self.kv_state_store.get_state(catalog_record.state_store_id)
            if recovered_descriptor is not None:
                store_state_id = recovered_descriptor.state_id
            else:
                # The previous process no longer owns allocator pages.  Do
                # not materialize merely to release; journal the catalog-only
                # cleanup so immutable Store objects can be reclaimed.
                record.status = "RELEASED"
                record.released_at = record.released_at or time.monotonic()
                record.updated_at = time.monotonic()
                record.reuse_telemetry = {
                    **dict(record.reuse_telemetry),
                    "agent_state_release_complete": True,
                    "store_state_released": False,
                    "catalog_recovery_release": True,
                }
                released = registry.upsert_record(
                    record,
                    event="AGENT_STATE_CATALOG_RECOVERY_RELEASED",
                    preserve_existing_handoff=False,
                    preserve_existing_reuse=False,
                )
                catalog_released = self.agent_state_catalog.release_state(state_id)
                return {
                    "state_id": state_id,
                    "workflow_id": record.workflow_id,
                    "released": True,
                    "idempotent": False,
                    "freed_pages": 0,
                    "refcounts": {
                        gid: self.kv_directory.refcount(gid)
                        for gid in list(record.kv_block_ids)
                    },
                    "orphans_swept": 0,
                    "catalog_gc_objects": int(catalog_released.gc_completed),
                    "status": released.status,
                }
        if self.kv_state_store is not None and store_state_id is not None:
            freed = self.kv_state_store.release_state(store_state_id)
            record.status = "RELEASED"
            record.released_at = record.released_at or time.monotonic()
            record.updated_at = time.monotonic()
            record.reuse_telemetry = {
                **dict(record.reuse_telemetry),
                "agent_state_release_complete": True,
                "store_state_released": True,
            }
            released = registry.upsert_record(
                record,
                event="AGENT_STATE_STORE_RELEASED",
                preserve_existing_handoff=False,
                preserve_existing_reuse=False,
            )
            catalog_gc_objects = 0
            if self.agent_state_catalog is not None and self.agent_state_catalog.get_record(state_id) is not None:
                # Page deletion is deliberately after allocator release and
                # registry journaling.  A Store failure remains retryable via
                # the idempotent release branch above.
                page_store = self.kv_state_store.kv_page_store
                gc_before = (
                    int(page_store.stats().get("gc_objects", 0))
                    if page_store is not None
                    else 0
                )
                self.agent_state_catalog.release_state(state_id)
                gc_after = (
                    int(page_store.stats().get("gc_objects", 0))
                    if page_store is not None
                    else gc_before
                )
                # ``release_state`` performs durable GC before it returns,
                # so its cleared reclaim queue cannot be used as a count.
                # The Store adapter is the authoritative observation point.
                catalog_gc_objects = max(0, gc_after - gc_before)
            return {
                "state_id": state_id,
                "workflow_id": record.workflow_id,
                "released": True,
                "idempotent": False,
                "freed_pages": freed,
                "refcounts": {
                    gid: self.kv_state_store.pm.refcount(gid)
                    for gid in list(record.kv_block_ids)
                },
                "orphans_swept": 0,
                "catalog_gc_objects": catalog_gc_objects,
                "status": released.status,
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
            "catalog": (
                self.agent_state_catalog.stats()
                if self.agent_state_catalog is not None
                else {"enabled": False}
            ),
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
        # The state-store owns physical page leases.  Sweep it before the
        # registry records so a crash-recovered catalog cannot leave pages
        # retained merely because its in-memory state-id map was lost.
        if self.kv_state_store is not None:
            released += self.kv_state_store.sweep_expired_states(now=now_f)
            for public_state_id, store_state_id in list(self._serving_kv_state_ids.items()):
                if self.kv_state_store.get_state(store_state_id) is None:
                    self._serving_kv_state_ids.pop(public_state_id, None)
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
        # A one-worker stage is not physically partitioned into role pools.
        # Deployment generation may still designate that worker as the first
        # high/low member, but treating the explicit label as exclusive makes
        # an otherwise valid interactive or thinking request look misrouted.
        # Advertise both logical capabilities so Agent-PD correctness metrics
        # describe the real 1P1D deployment rather than a nonexistent pool.
        if int(total) <= 1:
            if stage == "prefill":
                return ["high_prefill_pool", "standard_prefill_pool"]
            if stage == "decode":
                return ["low_latency_decode_pool", "standard_decode_pool"]
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
                if ordinal == 0:
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
                if ordinal == 0:
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
        worker.queue_size = max(0, worker.queue_size - 1)
        worker.queued_requests = worker.queue_size

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
            "queued_requests": worker.queued_requests,
            "running_requests": worker.running_requests,
            "first_token_pending": worker.first_token_pending,
            "active_decode_sequences": worker.active_decode_sequences,
            "queued_tokens": worker.queued_tokens,
            "running_tokens": worker.running_tokens,
            "active_decode_tokens": worker.active_decode_tokens,
            "avg_queue_wait_ms": worker.avg_queue_wait_ms,
            "avg_first_token_ms": worker.avg_first_token_ms,
            "avg_completion_ms": worker.avg_completion_ms,
            "avg_latency_ms": worker.avg_latency_ms,
            "gpu_utilization": worker.gpu_utilization,
            "service_rate": worker.service_rate,
            "arrival_rate": worker.arrival_rate,
            "rho": rho,
            "pool_tags": list(getattr(worker, "pool_tags", []) or []),
            "telemetry": {
                field_name: (
                    getattr(worker, field_name)
                    if worker.telemetry_is_known(field_name)
                    else None
                )
                for field_name in (
                    "gpu_utilization",
                    "gpu_memory_bytes",
                    "gpu_memory_free_bytes",
                    "kv_free_blocks",
                    "kv_cache_usage",
                    "avg_tpot_ms",
                    "output_tokens_per_s",
                    "transfer_backlog_bytes",
                    "transfer_bandwidth_bytes_per_s",
                    "connector_lock_wait_ms",
                    "http_queue_depth",
                )
            },
            "telemetry_known": {
                field_name: worker.telemetry_is_known(field_name)
                for field_name in worker.telemetry_known
            },
            "telemetry_source": worker.telemetry_source,
            "telemetry_updated_at": worker.telemetry_updated_at,
        }
