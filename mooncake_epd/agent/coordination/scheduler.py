"""Agent scheduler: priority + type-aware routing (RFC §6.2).

Routes agent requests to Prefill / Decode workers based on agent type:

- **THINKING** agents prefer high-capacity Prefill workers (long prompts).
- **INTERACTIVE** agents prefer low-latency Decode workers (fast response).
- **HYBRID** agents balance both.

A weighted scoring function combines remaining capacity, queue depth,
GPU utilization, and average latency. Batch routing sorts by priority
but preserves the original request order in the returned assignments.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class AgentType(str, Enum):
    THINKING = "THINKING"
    INTERACTIVE = "INTERACTIVE"
    HYBRID = "HYBRID"


class AdmissionAction(str, Enum):
    ADMIT = "ADMIT"
    BACKPRESSURE = "BACKPRESSURE"
    REJECT = "REJECT"


class DegradeLevel(str, Enum):
    NONE = "NONE"
    DISABLE_APPROX_REUSE = "DISABLE_APPROX_REUSE"
    RAISE_COMPRESSION = "RAISE_COMPRESSION"
    OFFLOAD_AGGRESSIVELY = "OFFLOAD_AGGRESSIVELY"
    REJECT = "REJECT"


class SchedulingPolicy(str, Enum):
    """Selectable, reproducible routing policies for Agent-PD ablations."""

    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    STATIC_TYPE_ROUTE = "static_type_route"
    AGENT_AWARE = "agent_aware"


@dataclass
class AgentRequest:
    request_id: str
    agent_type: AgentType
    input_tokens: int
    output_tokens: int = 0
    priority: int = 0                       # higher = more urgent
    deadline: Optional[float] = None
    metadata: Dict = field(default_factory=dict)


@dataclass
class WorkerLoad:
    worker_id: str
    worker_type: str                         # "prefill" | "decode"
    current_load: int = 0
    queue_size: int = 0
    # Explicit lifecycle gauges. ``current_load``/``queue_size`` remain the
    # compatibility views used by existing dashboards.
    queued_requests: int = 0
    running_requests: int = 0
    first_token_pending: int = 0
    active_decode_sequences: int = 0
    queued_tokens: int = 0
    running_tokens: int = 0
    active_decode_tokens: int = 0
    avg_queue_wait_ms: float = 0.0
    avg_first_token_ms: float = 0.0
    avg_completion_ms: float = 0.0
    avg_tpot_ms: float = 0.0
    output_tokens_per_s: float = 0.0
    gpu_utilization: float = 0.0
    avg_latency_ms: float = 0.0
    max_capacity: int = 100
    queue_capacity: int = 128
    service_rate: float = 100.0            # requests / second
    arrival_rate: float = 0.0              # EWMA requests / second
    critical_rho: float = 0.90
    pool_tags: List[str] = field(default_factory=list)
    kv_free_blocks: int = 0
    gpu_memory_bytes: int = 0
    gpu_memory_free_bytes: int = 0
    kv_cache_usage: float = 0.0
    transfer_backlog_bytes: int = 0
    transfer_bandwidth_bytes_per_s: float = 0.0
    connector_lock_wait_ms: float = 0.0
    http_queue_depth: int = 0
    telemetry_known: Dict[str, bool] = field(default_factory=dict)
    telemetry_source: str = ""
    telemetry_updated_at: float = 0.0
    telemetry_ttl_s: float = 5.0
    heartbeat_at: float = field(default_factory=time.monotonic)
    healthy: bool = True
    generation: str = ""

    def __post_init__(self) -> None:
        # Request lifecycle gauges are owned by the proxy and are therefore
        # known even when their value is exactly zero. External GPU/vLLM/
        # connector metrics remain unknown until an observation is reported.
        for field_name in (
            "current_load",
            "queue_size",
            "queued_requests",
            "running_requests",
            "first_token_pending",
            "active_decode_sequences",
            "queued_tokens",
            "running_tokens",
            "active_decode_tokens",
        ):
            self.telemetry_known.setdefault(field_name, True)
        for field_name in (
            "avg_queue_wait_ms",
            "avg_first_token_ms",
            "avg_completion_ms",
            "avg_tpot_ms",
            "output_tokens_per_s",
            "gpu_utilization",
            "avg_latency_ms",
            "arrival_rate",
            "kv_free_blocks",
            "gpu_memory_bytes",
            "gpu_memory_free_bytes",
            "kv_cache_usage",
            "transfer_backlog_bytes",
            "transfer_bandwidth_bytes_per_s",
            "connector_lock_wait_ms",
            "http_queue_depth",
        ):
            value = getattr(self, field_name)
            self.telemetry_known.setdefault(
                field_name,
                isinstance(value, (int, float)) and float(value) > 0.0,
            )

    def telemetry_is_known(
        self,
        field_name: str,
        *,
        now: Optional[float] = None,
    ) -> bool:
        if not bool(self.telemetry_known.get(field_name, False)):
            return False
        if field_name in {
            "current_load",
            "queue_size",
            "queued_requests",
            "running_requests",
            "first_token_pending",
            "active_decode_sequences",
            "queued_tokens",
            "running_tokens",
            "active_decode_tokens",
            "avg_queue_wait_ms",
            "avg_first_token_ms",
            "avg_completion_ms",
        }:
            return True
        if self.telemetry_updated_at <= 0 or self.telemetry_ttl_s <= 0:
            return True
        observed_at = time.monotonic() if now is None else float(now)
        return observed_at - self.telemetry_updated_at <= self.telemetry_ttl_s


@dataclass
class AdmissionDecision:
    action: AdmissionAction
    worker: Optional[WorkerLoad]
    rho: float
    degrade_level: DegradeLevel = DegradeLevel.NONE
    reason: str = ""
    estimated_wait_ms: float = 0.0
    estimated_service_ms: float = 0.0
    predicted_finish_ms: float = 0.0
    deadline_slack_ms: Optional[float] = None
    deadline_miss_risk: float = 0.0
    score_breakdown: Dict[str, float] = field(default_factory=dict)


class AgentScheduler:
    """Priority + type-aware routing of AgentRequests to workers."""

    def __init__(
        self,
        workers: Optional[List[WorkerLoad]] = None,
        prefer_high_capacity: bool = True,
        prefer_low_latency: bool = True,
        policy: SchedulingPolicy | str = SchedulingPolicy.AGENT_AWARE,
    ):
        self.workers: List[WorkerLoad] = list(workers or [])
        self.prefer_high_capacity = prefer_high_capacity
        self.prefer_low_latency = prefer_low_latency
        self.policy = SchedulingPolicy(policy)
        self._round_robin_cursor = 0
        self._affinity_escape_active: Dict[str, str] = {}
        self._last_route_trace: Dict[str, object] = {}
        self._stats = {
            "routed": 0,
            "by_type": {},
            "admitted": 0,
            "backpressured": 0,
            "rejected": 0,
            "deadline_backpressured": 0,
            "deadline_rejected": 0,
            "policy_routes": {},
            "congestion_escapes": 0,
            "congestion_reentries": 0,
        }

    def add_worker(self, w: WorkerLoad) -> None:
        self.workers.append(w)

    def update_load(self, worker_id: str, **fields) -> None:
        for w in self.workers:
            if w.worker_id == worker_id:
                explicit_known = fields.pop("telemetry_known", None)
                observed_external = False
                for k, v in fields.items():
                    if hasattr(w, k):
                        if v is None:
                            w.telemetry_known[k] = False
                            continue
                        setattr(w, k, v)
                        w.telemetry_known[k] = True
                        if k not in {
                            "current_load",
                            "queue_size",
                            "queued_requests",
                            "running_requests",
                            "first_token_pending",
                            "active_decode_sequences",
                            "queued_tokens",
                            "running_tokens",
                            "active_decode_tokens",
                            "healthy",
                            "heartbeat_at",
                            "generation",
                            "pool_tags",
                            "max_capacity",
                            "queue_capacity",
                            "service_rate",
                            "critical_rho",
                            "telemetry_source",
                            "telemetry_ttl_s",
                            "telemetry_updated_at",
                        }:
                            observed_external = True
                if isinstance(explicit_known, dict):
                    for key, known in explicit_known.items():
                        if hasattr(w, str(key)):
                            w.telemetry_known[str(key)] = bool(known)
                if "current_load" in fields and "running_requests" not in fields:
                    w.running_requests = max(0, int(w.current_load))
                if "queue_size" in fields and "queued_requests" not in fields:
                    w.queued_requests = max(0, int(w.queue_size))
                if observed_external and "telemetry_updated_at" not in fields:
                    w.telemetry_updated_at = time.monotonic()
                return

    # ------------------------------------------------------------------
    def score_components(self, request: AgentRequest, worker: WorkerLoad) -> Dict[str, float]:
        """Explain the agent-aware score without mutating scheduler state."""
        if not worker.healthy or worker.current_load >= worker.max_capacity:
            return {"total": float("-inf"), "healthy": 0.0}
        remaining = max(1, worker.max_capacity - worker.current_load)
        capacity_score = remaining / worker.max_capacity
        queue_score = 1.0 / (1.0 + worker.queue_size)
        util_known = worker.telemetry_is_known("gpu_utilization")
        latency_known = worker.telemetry_is_known("avg_latency_ms")
        util_score = (
            1.0 - min(1.0, worker.gpu_utilization)
            if util_known
            else 0.5
        )
        latency_score = (
            1.0 / (1.0 + worker.avg_latency_ms / 100.0)
            if latency_known
            else 0.5
        )
        deadline_score = 1.0

        stage = str(request.metadata.get("stage", worker.worker_type) if isinstance(request.metadata, dict) else worker.worker_type).lower()
        if request.agent_type is AgentType.THINKING and stage == "prefill":
            # Long-context thinking tasks are prefill-bound: route to high
            # capacity workers even if their per-request latency is higher.
            w_cap, w_lat, w_util, w_queue = 0.62, 0.06, 0.14, 0.18
        elif request.agent_type is AgentType.INTERACTIVE and stage == "decode":
            # User-facing short turns are decode/TTFT-bound: low latency and
            # queue depth dominate over raw capacity.
            w_cap, w_lat, w_util, w_queue = 0.08, 0.62, 0.10, 0.20
        elif request.agent_type is AgentType.INTERACTIVE and stage == "prefill":
            # Interactive prompts still need prefill, but should not occupy
            # dedicated high-prefill workers when standard workers are healthy.
            w_cap, w_lat, w_util, w_queue = 0.16, 0.38, 0.14, 0.32
        elif request.agent_type is AgentType.THINKING and stage == "decode":
            # Thinking responses can tolerate more decode latency than UI turns.
            w_cap, w_lat, w_util, w_queue = 0.28, 0.22, 0.20, 0.30
        else:
            w_cap, w_lat, w_util, w_queue = 0.30, 0.30, 0.15, 0.25

        if request.deadline is not None:
            _, _, slack_ms, miss_risk = self._deadline_diagnostics(request, worker)
            deadline_score = max(0.0, min(1.0, 1.0 - miss_risk))
            if slack_ms is not None and slack_ms < 0:
                latency_score *= 1.15
                queue_score *= 1.15

        affinity_score = 0.0
        pool_score = 0.0
        preferred_worker_id = request.metadata.get("preferred_worker_id") if isinstance(request.metadata, dict) else None
        if preferred_worker_id and str(preferred_worker_id) == str(worker.worker_id):
            # Convert topology locality into a bounded time benefit.  The old
            # fixed 3.50 bonus dominated every load signal and effectively
            # pinned traffic to an overloaded worker.
            affinity_benefit_ms = max(
                0.0,
                float(request.metadata.get("affinity_benefit_ms", 3.0) or 0.0),
            )
            affinity_score = min(0.15, affinity_benefit_ms / 100.0)
        preferred_worker_ids = {
            str(x) for x in request.metadata.get("preferred_worker_ids", [])
        } if isinstance(request.metadata, dict) else set()
        excluded_worker_ids = {
            str(x) for x in request.metadata.get("excluded_worker_ids", [])
        } if isinstance(request.metadata, dict) else set()
        # Pool/worker preferences are soft locality hints. Their influence
        # decays as the target fills up so they cannot pin traffic to one
        # instance and erase the throughput benefit of a multi-worker pool.
        load_pressure = max(
            float(worker.current_load) / max(1, int(worker.max_capacity)),
            float(worker.queue_size) / max(1, int(worker.queue_capacity)),
        )
        healthy_headroom = max(0.0, 1.0 - min(1.0, load_pressure))
        if str(worker.worker_id) in preferred_worker_ids:
            pool_score += 0.30 * healthy_headroom
        if str(worker.worker_id) in excluded_worker_ids:
            pool_score -= 0.30 * healthy_headroom
        preferred_pool = str(request.metadata.get("preferred_pool") or "") if isinstance(request.metadata, dict) else ""
        avoid_pool = str(request.metadata.get("avoid_pool") or "") if isinstance(request.metadata, dict) else ""
        tags = {str(tag) for tag in getattr(worker, "pool_tags", []) or []}
        if preferred_pool and preferred_pool in tags:
            pool_score += 0.20 * healthy_headroom
        if avoid_pool and avoid_pool in tags:
            pool_score -= 0.16 * healthy_headroom

        kv_known = worker.telemetry_is_known("kv_free_blocks")
        transfer_backlog_known = worker.telemetry_is_known(
            "transfer_backlog_bytes"
        )
        bandwidth_known = worker.telemetry_is_known(
            "transfer_bandwidth_bytes_per_s"
        )
        kv_score = (
            min(
                1.0,
                max(
                    0.0,
                    float(worker.kv_free_blocks)
                    / max(1.0, float(worker.max_capacity)),
                ),
            )
            if kv_known
            else 0.5
        )
        transfer_score = (
            1.0
            / (
                1.0
                + max(0.0, float(worker.transfer_backlog_bytes))
                / (64 * 1024 * 1024)
            )
            if transfer_backlog_known
            else 0.5
        )
        queue_wait_ms = max(
            float(worker.avg_queue_wait_ms),
            self.estimate_wait_ms(worker),
        )
        if stage == "decode":
            stage_service_ms = (
                float(worker.avg_first_token_ms)
                if worker.avg_first_token_ms > 0
                else (
                    float(worker.avg_latency_ms)
                    if worker.avg_latency_ms > 0
                    else 1000.0 / max(worker.service_rate, 1e-6)
                )
            )
        else:
            stage_service_ms = (
                float(worker.avg_completion_ms)
                if worker.avg_completion_ms > 0
                else (
                    float(worker.avg_latency_ms)
                    if worker.avg_latency_ms > 0
                    else 1000.0 / max(worker.service_rate, 1e-6)
                )
            )
        transfer_cost_ms = 0.0
        bandwidth = max(0.0, float(worker.transfer_bandwidth_bytes_per_s))
        if bandwidth_known and bandwidth > 0:
            transfer_cost_ms = (
                max(0.0, float(worker.transfer_backlog_bytes))
                + max(
                    0.0,
                    float(
                        request.metadata.get("estimated_transfer_bytes", 0.0)
                        or 0.0
                    ),
                )
            ) * 1000.0 / bandwidth
        control_cost_ms = max(
            0.0,
            float(request.metadata.get("control_cost_ms", 0.0) or 0.0),
        )
        known_signals = (
            util_known,
            latency_known,
            kv_known,
            transfer_backlog_known,
            bandwidth_known,
        )
        telemetry_confidence = sum(bool(item) for item in known_signals) / len(
            known_signals
        )
        telemetry_uncertainty_penalty = (
            0.08 * (1.0 - telemetry_confidence)
        )
        predicted_time_cost_ms = (
            queue_wait_ms
            + stage_service_ms
            + transfer_cost_ms
            + control_cost_ms
        )
        time_cost_penalty = min(0.60, predicted_time_cost_ms / 2000.0)
        total = (
            w_cap * capacity_score
            + w_lat * latency_score
            + w_util * util_score
            + w_queue * queue_score
            + 0.25 * deadline_score
            + affinity_score
            + pool_score
            + 0.04 * kv_score
            + 0.03 * transfer_score
            - time_cost_penalty
            - telemetry_uncertainty_penalty
        )
        return {
            "total": total,
            "capacity": w_cap * capacity_score,
            "latency": w_lat * latency_score,
            "utilization": w_util * util_score,
            "queue": w_queue * queue_score,
            "deadline": 0.25 * deadline_score,
            "affinity": affinity_score,
            "pool": pool_score,
            "kv_free": 0.04 * kv_score,
            "transfer": 0.03 * transfer_score,
            "queue_wait_ms": queue_wait_ms,
            "stage_service_ms": stage_service_ms,
            "transfer_cost_ms": transfer_cost_ms,
            "control_cost_ms": control_cost_ms,
            "predicted_time_cost_ms": predicted_time_cost_ms,
            "time_cost_penalty": -time_cost_penalty,
            "telemetry_confidence": telemetry_confidence,
            "telemetry_uncertainty_penalty": (
                -telemetry_uncertainty_penalty
            ),
        }

    def score(self, request: AgentRequest, worker: WorkerLoad) -> float:
        """Higher score = better fit. Returns -inf for unavailable workers."""

        return float(self.score_components(request, worker)["total"])

    # ------------------------------------------------------------------
    def route(self, request: AgentRequest) -> Optional[WorkerLoad]:
        if not self.workers:
            return None
        # Only consider workers whose type matches the request's preference
        # (prefill for THINKING, decode for INTERACTIVE, any for HYBRID).
        candidates = self.workers
        if request.agent_type is AgentType.THINKING:
            prefill = [w for w in self.workers if w.worker_type == "prefill"]
            if prefill:
                candidates = prefill
        elif request.agent_type is AgentType.INTERACTIVE:
            decode = [w for w in self.workers if w.worker_type == "decode"]
            if decode:
                candidates = decode
        candidates = [worker for worker in candidates if self.score(request, worker) != float("-inf")]
        if not candidates:
            return None
        best = self._select_worker(request, candidates)
        if best is None:
            return None
        self._record(request.agent_type)
        self._stats["policy_routes"][self.policy.value] = (
            int(self._stats["policy_routes"].get(self.policy.value, 0)) + 1
        )
        self._last_route_trace = {
            "request_id": request.request_id,
            "policy": self.policy.value,
            "chosen_worker_id": best.worker_id,
            "scores": {
                worker.worker_id: self.score_components(request, worker)
                for worker in sorted(candidates, key=lambda item: item.worker_id)
            },
        }
        return best

    def _select_worker(
        self,
        request: AgentRequest,
        candidates: List[WorkerLoad],
    ) -> Optional[WorkerLoad]:
        if self.policy is SchedulingPolicy.ROUND_ROBIN:
            ordered = sorted(candidates, key=lambda worker: worker.worker_id)
            selected = ordered[self._round_robin_cursor % len(ordered)]
            self._round_robin_cursor = (self._round_robin_cursor + 1) % len(ordered)
            return selected
        if self.policy is SchedulingPolicy.LEAST_LOADED:
            return min(
                candidates,
                key=lambda worker: (
                    self.worker_rho(worker),
                    worker.queue_size,
                    worker.current_load,
                    worker.worker_id,
                ),
            )
        if self.policy is SchedulingPolicy.STATIC_TYPE_ROUTE:
            preferred_tag = ""
            stage = str(request.metadata.get("stage", "") if isinstance(request.metadata, dict) else "")
            if request.agent_type is AgentType.THINKING and stage == "prefill":
                preferred_tag = "high-prefill"
            elif request.agent_type is AgentType.INTERACTIVE and stage == "decode":
                preferred_tag = "low-latency-decode"
            tagged = [
                worker for worker in candidates
                if preferred_tag and preferred_tag in {str(tag) for tag in worker.pool_tags}
            ]
            return min(
                tagged or candidates,
                key=lambda worker: (self.worker_rho(worker), worker.queue_size, worker.worker_id),
            )
        # Agent-aware is deterministic for an identical telemetry snapshot.
        ordered = sorted(
            candidates,
            key=lambda worker: (-self.score(request, worker), worker.worker_id),
        )
        selected = ordered[0]
        preferred_worker_id = (
            request.metadata.get("preferred_worker_id")
            if isinstance(request.metadata, dict)
            else None
        )
        preferred = next(
            (
                worker
                for worker in candidates
                if preferred_worker_id
                and str(worker.worker_id) == str(preferred_worker_id)
            ),
            None,
        )
        if preferred is not None and len(candidates) > 1:
            lowest_time = min(
                candidates,
                key=lambda worker: (
                    float(
                        self.score_components(request, worker).get(
                            "predicted_time_cost_ms", float("inf")
                        )
                    ),
                    worker.worker_id,
                ),
            )
            preferred_cost = float(
                self.score_components(request, preferred).get(
                    "predicted_time_cost_ms", float("inf")
                )
            )
            lowest_cost = float(
                self.score_components(request, lowest_time).get(
                    "predicted_time_cost_ms", float("inf")
                )
            )
            escape_threshold_ms = max(
                1.0,
                float(
                    request.metadata.get(
                        "affinity_congestion_escape_ms", 25.0
                    )
                    or 25.0
                ),
            )
            hysteresis_ms = max(
                0.0,
                min(
                    escape_threshold_ms,
                    float(
                        request.metadata.get(
                            "affinity_congestion_hysteresis_ms", 10.0
                        )
                        or 0.0
                    ),
                ),
            )
            affinity_key = (
                f"{str(request.metadata.get('stage') or '')}:"
                f"{preferred.worker_id}"
            )
            escaped_worker_id = self._affinity_escape_active.get(
                affinity_key
            )
            escape_active = bool(escaped_worker_id)
            escape_gap_ms = (
                escape_threshold_ms - hysteresis_ms
                if escape_active
                else escape_threshold_ms
            )
            if (
                lowest_time is not preferred
                and preferred_cost > lowest_cost + escape_gap_ms
            ):
                selected = lowest_time
                self._affinity_escape_active[affinity_key] = (
                    lowest_time.worker_id
                )
                if not escape_active:
                    self._stats["congestion_escapes"] += 1
            else:
                # Preserve locality only while its predicted time cost remains
                # within the bounded escape budget. Once escaped, require the
                # smaller re-entry gap before returning to the preferred peer;
                # this hysteresis prevents route flapping around one threshold.
                selected = preferred
                if escape_active:
                    self._affinity_escape_active.pop(affinity_key, None)
                    self._stats["congestion_reentries"] += 1
        return selected

    def last_route_trace(self) -> Dict[str, object]:
        return dict(self._last_route_trace)

    def worker_rho(self, worker: WorkerLoad) -> float:
        if worker.max_capacity <= 0:
            return 1.0
        queue_pressure = worker.queue_size / max(1, worker.queue_capacity)
        load_pressure = worker.current_load / max(1, worker.max_capacity)
        actual_pressure = max(queue_pressure, load_pressure)
        arrival_pressure = (
            worker.arrival_rate / max(worker.service_rate, 1e-6)
            if worker.service_rate > 0
            else 1.0
        )
        if arrival_pressure > actual_pressure:
            arrival_pressure = min(
                arrival_pressure,
                actual_pressure + 0.5 * max(0.0, 1.0 - actual_pressure),
            )
        return max(actual_pressure, arrival_pressure)

    def estimate_wait_ms(self, worker: WorkerLoad) -> float:
        if worker.service_rate <= 0:
            return float("inf")
        queued = max(0.0, float(worker.queue_size))
        load_penalty = max(0.0, float(worker.current_load) - float(worker.max_capacity))
        return max(0.0, (queued + load_penalty) * 1000.0 / max(worker.service_rate, 1e-6))

    def estimate_service_ms(self, request: AgentRequest, worker: WorkerLoad) -> float:
        if worker.avg_latency_ms > 0:
            base_ms = float(worker.avg_latency_ms)
        elif worker.service_rate > 0:
            base_ms = 1000.0 / float(worker.service_rate)
        else:
            base_ms = 1000.0
        token_scale = max(
            0.5,
            min(4.0, float(max(1, request.input_tokens)) / 256.0),
        )
        stage = str(request.metadata.get("stage", worker.worker_type) if isinstance(request.metadata, dict) else worker.worker_type).lower()
        if stage == "decode" and request.output_tokens > 0:
            token_scale = max(0.25, min(8.0, float(request.output_tokens) / 128.0))
        if request.agent_type is AgentType.INTERACTIVE:
            token_scale = max(0.35, min(2.0, token_scale))
        elif request.agent_type is AgentType.THINKING:
            token_scale = max(0.75, min(8.0, token_scale))
        return max(1.0, base_ms * token_scale)

    def _deadline_diagnostics(
        self,
        request: AgentRequest,
        worker: WorkerLoad,
    ) -> tuple[float, float, Optional[float], float]:
        wait_ms = self.estimate_wait_ms(worker)
        service_ms = self.estimate_service_ms(request, worker)
        if request.deadline is None:
            return wait_ms, service_ms, None, 0.0
        now = time.monotonic()
        remaining_ms = max(0.0, (float(request.deadline) - now) * 1000.0)
        predicted_finish_ms = wait_ms + service_ms
        slack_ms = remaining_ms - predicted_finish_ms
        risk_scale = max(50.0, service_ms)
        miss_risk = 1.0 / (1.0 + math.exp(slack_ms / risk_scale))
        return wait_ms, service_ms, slack_ms, max(0.0, min(1.0, miss_risk))

    def admission_decision(self, request: AgentRequest) -> AdmissionDecision:
        worker = self.route(request)
        if worker is None:
            self._stats["rejected"] += 1
            return AdmissionDecision(
                action=AdmissionAction.REJECT,
                worker=None,
                rho=1.0,
                degrade_level=DegradeLevel.REJECT,
                reason="no worker available",
            )
        rho = self.worker_rho(worker)
        wait_ms, service_ms, deadline_slack_ms, miss_risk = self._deadline_diagnostics(
            request,
            worker,
        )
        predicted_finish_ms = wait_ms + service_ms
        if worker.queue_size >= worker.queue_capacity:
            self._stats["rejected"] += 1
            return AdmissionDecision(
                action=AdmissionAction.REJECT,
                worker=worker,
                rho=rho,
                degrade_level=DegradeLevel.REJECT,
                reason="queue capacity exhausted",
                estimated_wait_ms=wait_ms,
                estimated_service_ms=service_ms,
                predicted_finish_ms=predicted_finish_ms,
                deadline_slack_ms=deadline_slack_ms,
                deadline_miss_risk=miss_risk,
            )
        if rho >= 1.0:
            self._stats["rejected"] += 1
            return AdmissionDecision(
                action=AdmissionAction.REJECT,
                worker=worker,
                rho=rho,
                degrade_level=DegradeLevel.REJECT,
                reason="worker overloaded",
                estimated_wait_ms=wait_ms,
                estimated_service_ms=service_ms,
                predicted_finish_ms=predicted_finish_ms,
                deadline_slack_ms=deadline_slack_ms,
                deadline_miss_risk=miss_risk,
            )
        if deadline_slack_ms is not None and deadline_slack_ms < 0:
            if abs(deadline_slack_ms) >= max(100.0, service_ms * 0.5):
                self._stats["rejected"] += 1
                self._stats["deadline_rejected"] += 1
                return AdmissionDecision(
                    action=AdmissionAction.REJECT,
                    worker=worker,
                    rho=rho,
                    degrade_level=DegradeLevel.REJECT,
                    reason="deadline miss predicted",
                    estimated_wait_ms=wait_ms,
                    estimated_service_ms=service_ms,
                    predicted_finish_ms=predicted_finish_ms,
                    deadline_slack_ms=deadline_slack_ms,
                    deadline_miss_risk=miss_risk,
                )
            self._stats["backpressured"] += 1
            self._stats["deadline_backpressured"] += 1
            degrade = max(
                self._degrade_level_for_rho(rho),
                DegradeLevel.RAISE_COMPRESSION,
                key=lambda item: list(DegradeLevel).index(item),
            )
            return AdmissionDecision(
                action=AdmissionAction.BACKPRESSURE,
                worker=worker,
                rho=rho,
                degrade_level=degrade,
                reason="deadline at risk",
                estimated_wait_ms=wait_ms,
                estimated_service_ms=service_ms,
                predicted_finish_ms=predicted_finish_ms,
                deadline_slack_ms=deadline_slack_ms,
                deadline_miss_risk=miss_risk,
            )
        if deadline_slack_ms is not None and deadline_slack_ms <= service_ms * 0.5:
            self._stats["backpressured"] += 1
            self._stats["deadline_backpressured"] += 1
            degrade = max(
                self._degrade_level_for_rho(rho),
                DegradeLevel.DISABLE_APPROX_REUSE,
                key=lambda item: list(DegradeLevel).index(item),
            )
            return AdmissionDecision(
                action=AdmissionAction.BACKPRESSURE,
                worker=worker,
                rho=rho,
                degrade_level=degrade,
                reason="deadline slack low",
                estimated_wait_ms=wait_ms,
                estimated_service_ms=service_ms,
                predicted_finish_ms=predicted_finish_ms,
                deadline_slack_ms=deadline_slack_ms,
                deadline_miss_risk=miss_risk,
            )
        if rho >= worker.critical_rho:
            self._stats["backpressured"] += 1
            degrade = self._degrade_level_for_rho(rho)
            return AdmissionDecision(
                action=AdmissionAction.BACKPRESSURE,
                worker=worker,
                rho=rho,
                degrade_level=degrade,
                reason="critical utilization reached",
                estimated_wait_ms=wait_ms,
                estimated_service_ms=service_ms,
                predicted_finish_ms=predicted_finish_ms,
                deadline_slack_ms=deadline_slack_ms,
                deadline_miss_risk=miss_risk,
            )
        self._stats["admitted"] += 1
        return AdmissionDecision(
            action=AdmissionAction.ADMIT,
            worker=worker,
            rho=rho,
            reason="healthy",
            estimated_wait_ms=wait_ms,
            estimated_service_ms=service_ms,
            predicted_finish_ms=predicted_finish_ms,
            deadline_slack_ms=deadline_slack_ms,
            deadline_miss_risk=miss_risk,
        )

    def admit(self, request: AgentRequest) -> AdmissionDecision:
        decision = self.admission_decision(request)
        if decision.worker is not None and decision.action is not AdmissionAction.REJECT:
            decision.worker.queue_size = min(
                decision.worker.queue_capacity,
                decision.worker.queue_size + 1,
            )
            decision.worker.queued_requests = decision.worker.queue_size
            scheduled_tokens = max(
                1,
                int(
                    request.output_tokens
                    if decision.worker.worker_type == "decode"
                    else request.input_tokens
                ),
            )
            decision.worker.queued_tokens += scheduled_tokens
            decision.worker.telemetry_known["queued_tokens"] = True
        return decision

    @staticmethod
    def _ewma(previous: float, observed: float) -> float:
        observed = max(0.0, float(observed))
        return observed if previous <= 0 else previous * 0.8 + observed * 0.2

    def mark_started(
        self,
        worker: WorkerLoad,
        *,
        queue_wait_ms: float = 0.0,
        scheduled_tokens: int = 0,
    ) -> None:
        worker.queue_size = max(0, worker.queue_size - 1)
        worker.queued_requests = worker.queue_size
        worker.current_load += 1
        worker.running_requests = worker.current_load
        tokens = max(0, int(scheduled_tokens))
        worker.queued_tokens = max(0, worker.queued_tokens - tokens)
        worker.running_tokens += tokens
        worker.avg_queue_wait_ms = self._ewma(
            worker.avg_queue_wait_ms, queue_wait_ms
        )
        if worker.worker_type == "decode":
            worker.first_token_pending += 1
            worker.active_decode_sequences += 1
            worker.active_decode_tokens += tokens

    def mark_first_token(
        self,
        worker: WorkerLoad,
        *,
        first_token_ms: float,
    ) -> None:
        if worker.worker_type != "decode":
            return
        worker.first_token_pending = max(0, worker.first_token_pending - 1)
        worker.avg_first_token_ms = self._ewma(
            worker.avg_first_token_ms, first_token_ms
        )

    def mark_complete(
        self,
        worker: WorkerLoad,
        *,
        latency_ms: float,
        was_queued: bool,
        first_token_observed: bool,
        scheduled_tokens: int = 0,
    ) -> None:
        tokens = max(0, int(scheduled_tokens))
        if was_queued:
            worker.queue_size = max(0, worker.queue_size - 1)
            worker.queued_requests = worker.queue_size
            worker.queued_tokens = max(0, worker.queued_tokens - tokens)
        else:
            worker.current_load = max(0, worker.current_load - 1)
            worker.running_requests = worker.current_load
            worker.running_tokens = max(0, worker.running_tokens - tokens)
            if worker.worker_type == "decode":
                worker.active_decode_sequences = max(
                    0, worker.active_decode_sequences - 1
                )
                worker.active_decode_tokens = max(
                    0, worker.active_decode_tokens - tokens
                )
                if not first_token_observed:
                    worker.first_token_pending = max(
                        0, worker.first_token_pending - 1
                    )
        worker.avg_completion_ms = self._ewma(
            worker.avg_completion_ms, latency_ms
        )

    def batch_route(
        self, requests: List[AgentRequest]
    ) -> List[Optional[WorkerLoad]]:
        """Priority-sorted routing that preserves input order in the output."""
        indexed = list(enumerate(requests))
        sorted_reqs = sorted(indexed, key=lambda x: -x[1].priority)
        out: List[Optional[WorkerLoad]] = [None] * len(requests)
        for i, req in sorted_reqs:
            worker = self.route(req)
            out[i] = worker
            if worker is not None:
                worker.current_load += 1
        return out

    # ------------------------------------------------------------------
    # Preemption (RFC §6.2): evict the lowest-priority active request on
    # the candidate worker when a high-priority request needs admission.
    # ------------------------------------------------------------------
    def preempt_for(
        self,
        high_priority: AgentRequest,
        active_requests: Optional[List[AgentRequest]] = None,
        offload_manager=None,
    ) -> List[AgentRequest]:
        """Evict the lowest-priority active request(s) to admit ``high_priority``.

        Returns the list of evicted requests. The caller is responsible for
        re-routing them later. When ``offload_manager`` is provided, the
        victim's state is offloaded to free GPU memory.
        """
        if active_requests is None:
            # Track internally if the caller doesn't supply one.
            active_requests = getattr(self, "_active", [])
        if not active_requests:
            return []
        # Only preempt if the high-priority request is strictly more urgent.
        victim = min(active_requests, key=lambda r: r.priority)
        if victim.priority >= high_priority.priority:
            return []
        # Offload the victim's KV if an offload_manager was supplied and
        # the victim has an associated state.
        if offload_manager is not None and getattr(victim, "state", None) is not None:
            offload_manager.offload(victim.state)
        active_requests.remove(victim)
        return [victim]

    def register_active(self, req: AgentRequest) -> None:
        """Register a request as active (used by preempt_for when the caller
        does not supply an active list)."""
        if not hasattr(self, "_active"):
            self._active = []
        self._active.append(req)

    # ------------------------------------------------------------------
    def _record(self, agent_type: AgentType) -> None:
        self._stats["routed"] += 1
        by_type = self._stats["by_type"]
        by_type[agent_type.value] = by_type.get(agent_type.value, 0) + 1

    @staticmethod
    def _degrade_level_for_rho(rho: float) -> DegradeLevel:
        if rho >= 0.99:
            return DegradeLevel.REJECT
        if rho >= 0.97:
            return DegradeLevel.OFFLOAD_AGGRESSIVELY
        if rho >= 0.94:
            return DegradeLevel.RAISE_COMPRESSION
        if rho >= 0.90:
            return DegradeLevel.DISABLE_APPROX_REUSE
        return DegradeLevel.NONE

    def stats(self) -> Dict:
        out = dict(self._stats)
        out["workers"] = [
            {
                "worker_id": w.worker_id,
                "worker_type": w.worker_type,
                "pool_tags": list(getattr(w, "pool_tags", []) or []),
                "current_load": w.current_load,
                "queue_size": w.queue_size,
                "queued_requests": w.queued_requests,
                "running_requests": w.running_requests,
                "first_token_pending": w.first_token_pending,
                "active_decode_sequences": w.active_decode_sequences,
                "avg_queue_wait_ms": w.avg_queue_wait_ms,
                "avg_first_token_ms": w.avg_first_token_ms,
                "avg_completion_ms": w.avg_completion_ms,
                "max_capacity": w.max_capacity,
                "avg_latency_ms": w.avg_latency_ms,
                "rho": self.worker_rho(w),
            }
            for w in self.workers
        ]
        return out
