"""
Agent PD 调度策略

根据 Agent 任务类型动态路由到不同的 P/D 资源。
"""

import time
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

MAX_ROUTING_LOG = 10000


class AgentType(Enum):
    THINKING = "thinking"
    INTERACTIVE = "interactive"
    HYBRID = "hybrid"


class AdmissionAction(Enum):
    ADMIT = "admit"
    BACKPRESSURE = "backpressure"
    REJECT = "reject"


@dataclass
class RoutingDecision:
    routing: Dict[str, str]
    action: AdmissionAction
    reason: str = ""
    prefill_rho: float = 0.0
    decode_rho: float = 0.0
    degrade_level: str = "none"


@dataclass
class AgentRequest:
    request_id: str
    agent_type: AgentType
    input_ids: Optional[Any] = None
    pixel_values: Optional[Any] = None
    priority: int = 0
    max_tokens: int = 256
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkerLoad:
    worker_id: str
    worker_type: str
    current_load: float = 0.0
    queue_size: int = 0
    gpu_utilization: float = 0.0
    avg_latency_ms: float = 0.0
    max_capacity: int = 1
    queue_capacity: int = 128
    service_rate: float = 100.0
    arrival_rate: float = 0.0
    critical_rho: float = 0.90


class AgentPDScheduler:
    """
    Agent PD Disaggregation 调度策略

    - 思考型 Agent → 高算力 Prefill Worker
    - 交互型 Agent → 低延迟 Decode Worker
    - 混合型 Agent → 均衡选择
    """

    def __init__(
        self,
        prefill_workers: Optional[List[str]] = None,
        decode_workers: Optional[List[str]] = None,
    ):
        self.prefill_workers = prefill_workers or ["prefill_0"]
        self.decode_workers = decode_workers or ["decode_0"]
        self._worker_loads: Dict[str, WorkerLoad] = {}
        self._routing_log: List[Dict[str, Any]] = []

        for wid in self.prefill_workers:
            self._worker_loads[wid] = WorkerLoad(worker_id=wid, worker_type="prefill")
        for wid in self.decode_workers:
            self._worker_loads[wid] = WorkerLoad(worker_id=wid, worker_type="decode")

    def route(self, request: AgentRequest) -> Dict[str, str]:
        return self.route_with_admission(request).routing

    def route_with_admission(self, request: AgentRequest) -> RoutingDecision:
        prefill_worker = self._select_prefill_worker(request)
        decode_worker = self._select_decode_worker(request)
        routing = {"prefill_worker": prefill_worker, "decode_worker": decode_worker}
        p_load = self._worker_loads[prefill_worker]
        d_load = self._worker_loads[decode_worker]
        p_rho = self._rho(p_load)
        d_rho = self._rho(d_load)
        worst = max(p_rho, d_rho)
        action = AdmissionAction.ADMIT
        reason = "healthy"
        degrade = "none"
        if p_load.queue_size >= p_load.queue_capacity or d_load.queue_size >= d_load.queue_capacity:
            action = AdmissionAction.REJECT
            reason = "queue capacity exhausted"
            degrade = "reject"
        elif worst >= 1.0:
            action = AdmissionAction.REJECT
            reason = "worker overloaded"
            degrade = "reject"
        elif worst >= max(p_load.critical_rho, d_load.critical_rho):
            action = AdmissionAction.BACKPRESSURE
            reason = "critical utilization reached"
            degrade = "raise_compression" if worst >= 0.94 else "disable_approx_reuse"

        decision = RoutingDecision(
            routing=routing,
            action=action,
            reason=reason,
            prefill_rho=p_rho,
            decode_rho=d_rho,
            degrade_level=degrade,
        )
        if len(self._routing_log) < MAX_ROUTING_LOG:
            self._routing_log.append({
                "request_id": request.request_id,
                "agent_type": request.agent_type.value,
                "routing": routing,
                "admission": action.value,
                "reason": reason,
                "prefill_rho": p_rho,
                "decode_rho": d_rho,
                "timestamp": time.time(),
            })
        logger.info(
            f"Routed {request.request_id} ({request.agent_type.value}): "
            f"P={prefill_worker}, D={decode_worker}, action={action.value}, reason={reason}"
        )
        return decision

    def _select_prefill_worker(self, request: AgentRequest) -> str:
        if request.agent_type == AgentType.THINKING:
            return self._select_least_loaded(self.prefill_workers, prefer_high_capacity=True)
        elif request.agent_type == AgentType.HYBRID:
            return self._select_least_loaded(self.prefill_workers)
        else:
            return self._select_least_loaded(self.prefill_workers, prefer_low_latency=True)

    def _select_decode_worker(self, request: AgentRequest) -> str:
        if request.agent_type in (AgentType.INTERACTIVE, AgentType.HYBRID):
            return self._select_least_loaded(self.decode_workers, prefer_low_latency=True)
        return self._select_least_loaded(self.decode_workers)

    def _select_least_loaded(
        self,
        workers: List[str],
        prefer_high_capacity: bool = False,
        prefer_low_latency: bool = False,
    ) -> str:
        if not workers:
            raise ValueError("No workers available")

        def score(wid: str) -> float:
            load = self._worker_loads.get(wid)
            if load is None:
                return float("inf")
            s = load.current_load + load.queue_size * 0.1
            if prefer_low_latency:
                s += load.avg_latency_ms / 1000.0
            if prefer_high_capacity:
                s -= load.max_capacity * 0.01
            return s

        return min(workers, key=score)

    def _rho(self, load: WorkerLoad) -> float:
        queue_pressure = load.queue_size / max(1, load.queue_capacity)
        load_pressure = load.current_load / max(1, load.max_capacity)
        arrival_pressure = load.arrival_rate / max(load.service_rate, 1e-6) if load.service_rate > 0 else 1.0
        return max(queue_pressure, load_pressure, min(1.5, arrival_pressure))

    def update_load(self, worker_id: str, **kwargs):
        if worker_id in self._worker_loads:
            load = self._worker_loads[worker_id]
            for k, v in kwargs.items():
                if hasattr(load, k) and k not in ("worker_id", "worker_type"):
                    setattr(load, k, v)

    def batch_route(self, requests: List[AgentRequest]) -> List[Dict[str, str]]:
        """批量路由，按优先级排序后路由，返回与输入顺序对应的结果"""
        indexed = list(enumerate(requests))
        indexed.sort(key=lambda x: -x[1].priority)

        results = [None] * len(requests)
        for orig_idx, req in indexed:
            results[orig_idx] = self.route(req)
        return results

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_routed": len(self._routing_log),
            "routing_by_type": self._routing_distribution(),
            "worker_loads": {
                wid: {
                    "load": wl.current_load,
                    "queue": wl.queue_size,
                    "gpu_util": wl.gpu_utilization,
                    "avg_latency_ms": wl.avg_latency_ms,
                    "rho": self._rho(wl),
                    "queue_capacity": wl.queue_capacity,
                    "service_rate": wl.service_rate,
                    "arrival_rate": wl.arrival_rate,
                }
                for wid, wl in self._worker_loads.items()
            },
        }

    def _routing_distribution(self) -> Dict[str, int]:
        dist: Dict[str, int] = {}
        for entry in self._routing_log:
            at = entry["agent_type"]
            dist[at] = dist.get(at, 0) + 1
        return dist
