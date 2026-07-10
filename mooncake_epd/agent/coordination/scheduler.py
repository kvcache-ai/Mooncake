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
    gpu_utilization: float = 0.0
    avg_latency_ms: float = 0.0
    max_capacity: int = 100
    queue_capacity: int = 128
    service_rate: float = 100.0            # requests / second
    arrival_rate: float = 0.0              # EWMA requests / second
    critical_rho: float = 0.90
    pool_tags: List[str] = field(default_factory=list)


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


class AgentScheduler:
    """Priority + type-aware routing of AgentRequests to workers."""

    def __init__(
        self,
        workers: Optional[List[WorkerLoad]] = None,
        prefer_high_capacity: bool = True,
        prefer_low_latency: bool = True,
    ):
        self.workers: List[WorkerLoad] = list(workers or [])
        self.prefer_high_capacity = prefer_high_capacity
        self.prefer_low_latency = prefer_low_latency
        self._stats = {
            "routed": 0,
            "by_type": {},
            "admitted": 0,
            "backpressured": 0,
            "rejected": 0,
            "deadline_backpressured": 0,
            "deadline_rejected": 0,
        }

    def add_worker(self, w: WorkerLoad) -> None:
        self.workers.append(w)

    def update_load(self, worker_id: str, **fields) -> None:
        for w in self.workers:
            if w.worker_id == worker_id:
                for k, v in fields.items():
                    if hasattr(w, k):
                        setattr(w, k, v)
                return

    # ------------------------------------------------------------------
    def score(self, request: AgentRequest, worker: WorkerLoad) -> float:
        """Higher score = better fit. Returns -inf if worker is at capacity."""
        if worker.current_load >= worker.max_capacity:
            return float("-inf")
        remaining = max(1, worker.max_capacity - worker.current_load)
        capacity_score = remaining / worker.max_capacity
        queue_score = 1.0 / (1.0 + worker.queue_size)
        util_score = 1.0 - min(1.0, worker.gpu_utilization)
        latency_score = 1.0 / (1.0 + worker.avg_latency_ms / 100.0)
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
            # Workflow affinity is a serving hot-path hint, not a soft cosmetic
            # preference: for cross-step Agent workflows it preserves vLLM
            # prefix/MM/KV locality. It must beat generic pool preferences while
            # still allowing capacity/rho guards to reject overloaded workers.
            affinity_score = 3.50
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

        return (
            w_cap * capacity_score
            + w_lat * latency_score
            + w_util * util_score
            + w_queue * queue_score
            + 0.25 * deadline_score
            + affinity_score
            + pool_score
        )

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
        best = max(candidates, key=lambda w: self.score(request, w))
        if self.score(request, best) == float("-inf"):
            return None
        self._record(request.agent_type)
        return best

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
            decision.worker.current_load += 1
            decision.worker.queue_size = min(
                decision.worker.queue_capacity,
                decision.worker.queue_size + 1,
            )
        return decision

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
                "max_capacity": w.max_capacity,
                "avg_latency_ms": w.avg_latency_ms,
                "rho": self.worker_rho(w),
            }
            for w in self.workers
        ]
        return out
