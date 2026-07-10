"""Omni multi-stage pipeline abstraction (RFC §7).

Generalizes EPD to arbitrary worker-level stage chains.  Stage outputs are
transported through the same production TransferEngine primitives used by
E→P/P→D/A2A: tensors, FeatureBundles and nested containers are moved to the
next worker device; per-edge policies make RDMA/SHM/TCP selection explicit.
"""

from __future__ import annotations

import time
import queue
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence

import torch

from .state import FeatureBundle
from .transfer.engine import TransferEngine
from .transfer.policy import Channel, Mode, TransferPolicy


class OmniStage(Protocol):
    name: str

    def run(self, input_refs: Sequence[Any]) -> List[Any]: ...


@dataclass
class StageStats:
    name: str
    runs: int = 0
    total_ms: float = 0.0
    last_ms: float = 0.0


@dataclass
class EdgeTransferStats:
    edge: str
    transfers: int = 0
    objects: int = 0
    total_ms: float = 0.0
    last_ms: float = 0.0


@dataclass
class OmniJob:
    job_id: str
    payload: List[Any]
    done: threading.Event = field(default_factory=threading.Event, repr=False)
    result: Optional[List[Any]] = None
    error: Optional[BaseException] = None

    def wait(self, timeout: Optional[float] = None) -> List[Any]:
        if not self.done.wait(timeout=timeout):
            raise TimeoutError(f"Omni job timed out: {self.job_id}")
        if self.error is not None:
            raise self.error
        return list(self.result or [])


class OmniPipeline:
    """Linear chain of stage workers with real inter-stage transport.

    Args:
        stages: ordered worker stages (AR → Generation → Diffusion, or E→P→D).
        transfer: TransferEngine facade (local CUDA copy, Mooncake TCP/RDMA,
            or direct peer-buffer depending on policy/backend).
        device_per_stage: optional torch devices for each stage.
        policy_per_edge: optional TransferPolicy for edge ``i -> i+1``.
    """

    def __init__(
        self,
        stages: Sequence[OmniStage],
        transfer: Optional[TransferEngine] = None,
        device_per_stage: Optional[Sequence[Any]] = None,
        policy_per_edge: Optional[Sequence[TransferPolicy]] = None,
    ):
        if len(stages) < 1:
            raise ValueError("OmniPipeline requires at least one stage")
        if device_per_stage is not None and len(device_per_stage) != len(stages):
            raise ValueError("device_per_stage must match stages length")
        if policy_per_edge is not None and len(policy_per_edge) != max(0, len(stages) - 1):
            raise ValueError("policy_per_edge must have len(stages)-1 entries")
        self.stages = list(stages)
        self.transfer = transfer
        self.device_per_stage = [torch.device(d) for d in device_per_stage] if device_per_stage else None
        self.policy_per_edge = list(policy_per_edge) if policy_per_edge else None
        self._stats: Dict[str, StageStats] = {s.name: StageStats(name=s.name) for s in self.stages}
        self._edge_stats: Dict[str, EdgeTransferStats] = {}

    def process(self, inputs: Sequence[Any]) -> List[Any]:
        current = list(inputs)
        for i, stage in enumerate(self.stages):
            t0 = time.perf_counter()
            out = list(stage.run(current))
            ms = (time.perf_counter() - t0) * 1000
            stats = self._stats[stage.name]
            stats.runs += 1
            stats.total_ms += ms
            stats.last_ms = ms
            if i + 1 < len(self.stages):
                out = self._transfer_edge(i, out)
            current = out
        return current

    def _transfer_edge(self, edge_index: int, objects: List[Any]) -> List[Any]:
        if self.transfer is None or self.device_per_stage is None:
            return objects
        target_device = self.device_per_stage[edge_index + 1]
        policy = self._edge_policy(edge_index)
        edge_name = f"{self.stages[edge_index].name}->{self.stages[edge_index + 1].name}"
        t0 = time.perf_counter()
        moved = [self._move_object(obj, target_device, policy) for obj in objects]
        ms = (time.perf_counter() - t0) * 1000
        estats = self._edge_stats.setdefault(edge_name, EdgeTransferStats(edge=edge_name))
        estats.transfers += 1
        estats.objects += len(objects)
        estats.total_ms += ms
        estats.last_ms = ms
        return moved

    def _edge_policy(self, edge_index: int) -> TransferPolicy:
        if self.policy_per_edge is not None:
            return self.policy_per_edge[edge_index]
        return TransferPolicy(Mode.STREAM, channel=Channel.AGENT_TO_AGENT)

    def _move_object(self, obj: Any, target_device: torch.device, policy: TransferPolicy) -> Any:
        if isinstance(obj, torch.Tensor):
            return self.transfer.transfer_tensor(obj, target_device, policy)  # type: ignore[union-attr]
        if isinstance(obj, FeatureBundle):
            return self.transfer.transfer_feature_bundle(obj, target_device, policy)  # type: ignore[union-attr]
        if isinstance(obj, tuple):
            return tuple(self._move_object(x, target_device, policy) for x in obj)
        if isinstance(obj, list):
            return [self._move_object(x, target_device, policy) for x in obj]
        if isinstance(obj, Mapping):
            return {k: self._move_object(v, target_device, policy) for k, v in obj.items()}
        return obj

    def stats(self) -> dict:
        return {
            "stages": {
                name: {
                    "runs": s.runs,
                    "total_ms": s.total_ms,
                    "last_ms": s.last_ms,
                    "avg_ms": s.total_ms / s.runs if s.runs else 0.0,
                }
                for name, s in self._stats.items()
            },
            "edges": {
                name: {
                    "transfers": s.transfers,
                    "objects": s.objects,
                    "total_ms": s.total_ms,
                    "last_ms": s.last_ms,
                    "avg_ms": s.total_ms / s.transfers if s.transfers else 0.0,
                }
                for name, s in self._edge_stats.items()
            },
        }


class OmniPipelineRuntime:
    """Threaded worker-level runtime for an ``OmniPipeline``.

    Each stage runs in its own worker thread with bounded queues between stages.
    Stage outputs are moved through the pipeline's real ``TransferEngine`` edge
    policy before the next worker sees them.  This is still single-process, but
    it exercises the same worker/queue/transfer semantics used by process or RPC
    deployments and avoids mock transport shortcuts.
    """

    _STOP = object()

    def __init__(
        self,
        pipeline: OmniPipeline,
        *,
        queue_size: int = 16,
        worker_name_prefix: str = "omni",
    ):
        self.pipeline = pipeline
        self.queue_size = max(1, int(queue_size))
        self.worker_name_prefix = str(worker_name_prefix)
        self._queues: list["queue.Queue[Any]"] = [
            queue.Queue(maxsize=self.queue_size)
            for _ in range(len(self.pipeline.stages))
        ]
        self._jobs: Dict[str, OmniJob] = {}
        self._threads: list[threading.Thread] = []
        self._lock = threading.RLock()
        self._started = False
        self._stopped = False

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._stopped = False
            for index, stage in enumerate(self.pipeline.stages):
                thread = threading.Thread(
                    target=self._worker_loop,
                    args=(index,),
                    name=f"{self.worker_name_prefix}-{stage.name}",
                    daemon=True,
                )
                thread.start()
                self._threads.append(thread)
            self._started = True

    def stop(self, timeout: float = 5.0) -> None:
        with self._lock:
            if not self._started or self._stopped:
                return
            self._stopped = True
            for q in self._queues:
                q.put(self._STOP)
        deadline = time.monotonic() + max(0.0, float(timeout))
        for thread in list(self._threads):
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(timeout=remaining)

    def submit(self, inputs: Sequence[Any], *, job_id: Optional[str] = None, timeout: Optional[float] = None) -> OmniJob:
        self.start()
        job = OmniJob(job_id=job_id or uuid.uuid4().hex, payload=list(inputs))
        with self._lock:
            if self._stopped:
                raise RuntimeError("OmniPipelineRuntime is stopped")
            self._jobs[job.job_id] = job
        self._queues[0].put(job, timeout=timeout)
        return job

    def run(self, inputs: Sequence[Any], *, timeout: Optional[float] = None) -> List[Any]:
        return self.submit(inputs, timeout=timeout).wait(timeout=timeout)

    def _worker_loop(self, stage_index: int) -> None:
        q = self._queues[stage_index]
        stage = self.pipeline.stages[stage_index]
        while True:
            item = q.get()
            if item is self._STOP:
                if stage_index + 1 < len(self._queues):
                    self._queues[stage_index + 1].put(self._STOP)
                return
            assert isinstance(item, OmniJob)
            try:
                t0 = time.perf_counter()
                out = list(stage.run(item.payload))
                ms = (time.perf_counter() - t0) * 1000
                stats = self.pipeline._stats[stage.name]  # noqa: SLF001
                stats.runs += 1
                stats.total_ms += ms
                stats.last_ms = ms
                if stage_index + 1 < len(self.pipeline.stages):
                    item.payload = self.pipeline._transfer_edge(stage_index, out)  # noqa: SLF001
                    self._queues[stage_index + 1].put(item)
                else:
                    item.result = out
                    item.done.set()
                    with self._lock:
                        self._jobs.pop(item.job_id, None)
            except BaseException as exc:  # propagate worker failures to caller
                item.error = exc
                item.done.set()
                with self._lock:
                    self._jobs.pop(item.job_id, None)

    def stats(self) -> dict:
        out = self.pipeline.stats()
        out["runtime"] = {
            "started": self._started,
            "stopped": self._stopped,
            "queues": [q.qsize() for q in self._queues],
            "jobs": len(self._jobs),
            "threads_alive": sum(1 for t in self._threads if t.is_alive()),
        }
        return out
