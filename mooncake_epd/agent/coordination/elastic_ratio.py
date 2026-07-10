"""Elastic E:P:D ratio monitor + role switching (RFC §6.1).

The baseline EPD ratio is E:P:D = 1:2:4. The monitor observes per-stage
utilization, queue depth, and Encoder cache hit rate, and suggests
rebalancing when one stage becomes a bottleneck.

**Role switching (append-prefill on decode):** when the Decode node is
idle or the cost of transferring the full KV across nodes is too high,
we route incremental (continuation) prefill to the Decode node itself,
eliminating the cross-node P->D transfer.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class StageStats:
    utilization: float = 0.0         # 0..1
    queue_depth: int = 0
    avg_latency_ms: float = 0.0
    hit_rate: float = 0.0            # encoder feature-cache hit rate
    samples: int = 0
    updated_at: float = 0.0


class ElasticEPDRatio:
    """Dynamic E:P:D ratio monitor + role-switching decision engine."""

    BASELINE_RATIO = {"encoder": 1, "prefill": 2, "decode": 4}
    UTILIZATION_HIGH = 0.85
    UTILIZATION_LOW = 0.25
    QUEUE_DEPTH_HIGH = 16

    def __init__(self):
        self._stages: Dict[str, StageStats] = {
            "encoder": StageStats(),
            "prefill": StageStats(),
            "decode": StageStats(),
        }
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    def update(self, stage: str, **fields) -> None:
        with self._lock:
            s = self._stages.setdefault(stage, StageStats())
            for k, v in fields.items():
                if hasattr(s, k):
                    setattr(s, k, v)
            s.samples += 1
            s.updated_at = time.monotonic()

    def snapshot(self) -> Dict[str, dict]:
        with self._lock:
            return {
                name: {
                    "utilization": s.utilization,
                    "queue_depth": s.queue_depth,
                    "avg_latency_ms": s.avg_latency_ms,
                    "hit_rate": s.hit_rate,
                    "samples": s.samples,
                }
                for name, s in self._stages.items()
            }

    # ------------------------------------------------------------------
    def suggest_rebalance(self) -> Optional[Dict[str, int]]:
        """Propose a new E:P:D ratio when an imbalance is detected.

        Heuristics:
        - If Encoder has high hit_rate (> 0.9) and low utilization, shrink
          its share (yield GPUs to prefill/decode).
        - If Prefill queue is deep and utilization is high, grow its share.
        - If Decode is saturated, grow its share.
        """
        with self._lock:
            enc = self._stages["encoder"]
            pre = self._stages["prefill"]
            dec = self._stages["decode"]

        ratio = dict(self.BASELINE_RATIO)
        # Encoder high hit rate -> yield
        if enc.hit_rate > 0.9 and enc.utilization < self.UTILIZATION_LOW:
            ratio["encoder"] = max(1, ratio["encoder"] - 1)
            ratio["prefill"] += 1
        # Prefill overloaded
        if pre.utilization > self.UTILIZATION_HIGH or pre.queue_depth > self.QUEUE_DEPTH_HIGH:
            ratio["prefill"] += 1
            if dec.utilization < self.UTILIZATION_LOW and ratio["decode"] > 2:
                ratio["decode"] -= 1
        # Decode overloaded
        if dec.utilization > self.UTILIZATION_HIGH or dec.queue_depth > self.QUEUE_DEPTH_HIGH:
            ratio["decode"] += 1
            if pre.utilization < self.UTILIZATION_LOW and ratio["prefill"] > 1:
                ratio["prefill"] -= 1
        if ratio == self.BASELINE_RATIO:
            return None
        return ratio

    # ------------------------------------------------------------------
    def should_route_append_prefill(self, decode_worker_id: str) -> bool:
        """Decide whether continuation prefill should run on the decode
        worker (avoiding a cross-node P->D transfer).

        Returns True when the decode worker is underutilized (< 0.5) and
        prefill has a non-trivial queue.
        """
        with self._lock:
            dec = self._stages["decode"]
            pre = self._stages["prefill"]
        if dec.utilization < 0.5 and pre.queue_depth >= 4:
            return True
        return False
