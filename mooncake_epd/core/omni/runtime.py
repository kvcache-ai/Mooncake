"""Independent-process Omni stage runtime over descriptor-only SHM edges."""

from __future__ import annotations

import multiprocessing as mp
import queue
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple

import torch

from .protocol import StageEnvelope, StagePayload
from .transports import (
    PosixSharedMemoryTransport,
    StageTransportError,
    TCPRelayServer,
    build_stage_transport,
)


StageCallable = Callable[[Dict[str, torch.Tensor], Mapping[str, Any]], Tuple[Dict[str, torch.Tensor], Dict[str, Any]]]


@dataclass(frozen=True)
class ProcessStage:
    name: str
    handler: StageCallable
    generation: str = "g1"


def _stage_worker_main(
    stage: ProcessStage,
    inbound: Any,
    outbound: Any,
    namespace: str,
    is_final: bool,
    transport_name: str,
    tcp_endpoint: Mapping[str, Any] | None,
) -> None:
    transport = build_stage_transport(
        transport_name,
        namespace=namespace,
        tcp_endpoint=tcp_endpoint,
    )
    while True:
        item = inbound.get()
        if item is None:
            outbound.put(None)
            return
        if isinstance(item, dict) and item.get("ok") is False:
            # Forward a prior-stage failure without trying to interpret it as
            # an envelope.  The terminal queue is owned by the coordinator.
            outbound.put(item)
            return
        envelope = StageEnvelope.from_dict(item)
        opened = []
        try:
            if time.monotonic() > envelope.deadline_monotonic:
                raise TimeoutError(f"stage deadline expired before {stage.name}")
            open_started = time.perf_counter()
            tensors: Dict[str, torch.Tensor] = {}
            for name, ref in envelope.payload.items():
                tensor, segment = transport.open(ref)
                tensors[name] = tensor
                opened.append(segment)
            input_open_ms = (time.perf_counter() - open_started) * 1000.0
            compute_started = time.perf_counter()
            outputs, metadata = stage.handler(tensors, dict(envelope.metadata))
            compute_ms = (time.perf_counter() - compute_started) * 1000.0
            if not isinstance(outputs, dict) or not all(isinstance(value, torch.Tensor) for value in outputs.values()):
                raise TypeError("stage handler must return Dict[str, torch.Tensor]")
            publish_started = time.perf_counter()
            output_refs: StagePayload = {}
            cleanup_handles = list(envelope.cleanup_handles)
            for name, tensor in outputs.items():
                ref = transport.publish(tensor)
                output_refs[str(name)] = ref
                cleanup_handles.append(ref.handle)
            output_publish_ms = (time.perf_counter() - publish_started) * 1000.0
            next_metadata = {**dict(envelope.metadata), **dict(metadata)}
            prior_timing = list(next_metadata.get("stage_timings_ms") or [])
            prior_timing.append(
                {
                    "stage": stage.name,
                    "pid": mp.current_process().pid,
                    "input_open_ms": input_open_ms,
                    "compute_ms": compute_ms,
                    "output_publish_ms": output_publish_ms,
                    "input_tensor_count": len(tensors),
                    "output_tensor_count": len(output_refs),
                    "transport": transport.label,
                }
            )
            next_metadata["stage_timings_ms"] = prior_timing
            next_envelope = StageEnvelope(
                schema_version=1,
                job_id=envelope.job_id,
                stage=stage.name,
                attempt=envelope.attempt,
                producer_generation=stage.generation,
                consumer_generation="",
                deadline_monotonic=envelope.deadline_monotonic,
                payload=output_refs,
                metadata={**next_metadata, "stage_pid": mp.current_process().pid},
                cleanup_handles=tuple(dict.fromkeys(cleanup_handles)),
            )
            if is_final:
                outbound.put({"ok": True, "envelope": next_envelope.to_dict()})
            else:
                outbound.put(next_envelope.to_dict())
        except Exception as exc:
            outbound.put(
                {
                    "ok": False,
                    "job_id": envelope.job_id,
                    "cleanup_handles": list(envelope.cleanup_handles),
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc(),
                }
            )
        finally:
            for segment in opened:
                if segment is not None:
                    segment.close()


class OmniProcessPipeline:
    """A linear independent-process pipeline with bounded descriptor queues.

    It deliberately supports CPU tensors only. ``posix_shm`` is descriptor
    sharing on one host; ``tcp`` sends bytes through an authenticated TCP relay
    while process queues still carry descriptors only. Choosing CUDA IPC,
    NVLink or RDMA without a real adapter is a configuration error rather than
    a hidden tensor copy. The stage graph can overlap jobs because every edge
    is a bounded process queue; ``run`` is the simple synchronous facade.
    """

    def __init__(
        self,
        stages: Sequence[ProcessStage],
        *,
        queue_capacity: int = 8,
        start_method: str = "spawn",
        namespace: str = "mooncake_epd_omni",
        transport: str = "posix_shm",
        tcp_host: str = "127.0.0.1",
        tcp_port: int = 0,
        tcp_max_payload_bytes: int = 512 * 1024 * 1024,
    ) -> None:
        if not stages:
            raise ValueError("at least one process stage is required")
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be positive")
        self.stages = list(stages)
        self.queue_capacity = int(queue_capacity)
        self.context = mp.get_context(start_method)
        self.namespace = namespace
        self.transport_name = str(transport).strip().lower()
        if self.transport_name not in {"posix_shm", "tcp"}:
            raise StageTransportError(
                f"transport={transport} has no installed concrete Omni adapter; "
                "RDMA/CUDA IPC/NVLink cannot fall back to a CPU transport"
            )
        self.tcp_host = str(tcp_host)
        self.tcp_port = int(tcp_port)
        self.tcp_max_payload_bytes = int(tcp_max_payload_bytes)
        self._tcp_relay: TCPRelayServer | None = None
        self._tcp_endpoint: Mapping[str, Any] | None = None
        self._queues: List[Any] = []
        self._processes: List[mp.Process] = []
        self._started = False
        self._metrics = {
            "jobs": 0,
            "failures": 0,
            "published_tensors": 0,
            "released_tensors": 0,
            "last_job_timing_ms": {},
        }

    def start(self) -> None:
        if self._started:
            return
        if self.transport_name == "tcp":
            self._tcp_relay = TCPRelayServer(
                host=self.tcp_host,
                port=self.tcp_port,
                max_payload_bytes=self.tcp_max_payload_bytes,
            )
            self._tcp_endpoint = dict(self._tcp_relay.start())
        self._queues = [self.context.Queue(maxsize=self.queue_capacity) for _ in range(len(self.stages) + 1)]
        self._processes = []
        for index, stage in enumerate(self.stages):
            process = self.context.Process(
                target=_stage_worker_main,
                args=(
                    stage,
                    self._queues[index],
                    self._queues[index + 1],
                    self.namespace,
                    index == len(self.stages) - 1,
                    self.transport_name,
                    self._tcp_endpoint,
                ),
                name=f"mooncake-omni-{stage.name}",
                daemon=True,
            )
            process.start()
            self._processes.append(process)
        self._started = True

    def stop(self, *, timeout: float = 5.0) -> None:
        if not self._started:
            return
        for queue_ in self._queues[:-1]:
            try:
                queue_.put_nowait(None)
            except queue.Full:
                # A saturated stage is allowed to finish or be terminated
                # below; blocking shutdown would turn backpressure into a
                # coordinator deadlock.
                pass
        deadline = time.monotonic() + max(0.1, timeout)
        for process in self._processes:
            process.join(timeout=max(0.0, deadline - time.monotonic()))
            if process.is_alive():
                process.terminate()
                process.join(timeout=1.0)
        # A caller can time out before the terminal worker has emitted its
        # result. Drain completed descriptors after workers exit so their
        # producer-owned SHM/TCP payloads still reach the coordinator's
        # idempotent cleanup path.
        transport = build_stage_transport(
            self.transport_name,
            namespace=self.namespace,
            tcp_endpoint=self._tcp_endpoint,
        )
        leaked_handles: List[str] = []
        for queue_ in self._queues:
            while True:
                try:
                    item = queue_.get_nowait()
                except queue.Empty:
                    break
                leaked_handles.extend(self._cleanup_handles_from_queue_item(item))
        for handle in dict.fromkeys(leaked_handles):
            transport.release(handle)
            self._metrics["released_tensors"] += 1
        for queue_ in self._queues:
            queue_.close()
        self._queues = []
        self._processes = []
        if self._tcp_relay is not None:
            self._tcp_relay.stop()
        self._tcp_relay = None
        self._tcp_endpoint = None
        self._started = False

    @staticmethod
    def _cleanup_handles_from_queue_item(item: Any) -> List[str]:
        """Extract lifecycle handles from success and failure queue payloads."""

        if not isinstance(item, Mapping):
            return []
        handles = [str(value) for value in item.get("cleanup_handles") or () if str(value)]
        nested = item.get("envelope")
        if isinstance(nested, Mapping):
            handles.extend(
                str(value)
                for value in nested.get("cleanup_handles") or ()
                if str(value)
            )
        return handles

    def run(
        self,
        tensors: Mapping[str, torch.Tensor],
        *,
        metadata: Mapping[str, Any] | None = None,
        timeout: float = 30.0,
        job_id: str | None = None,
    ) -> tuple[Dict[str, torch.Tensor], Dict[str, Any]]:
        self.start()
        transport = build_stage_transport(
            self.transport_name,
            namespace=self.namespace,
            tcp_endpoint=self._tcp_endpoint,
        )
        refs: StagePayload = {}
        cleanup_handles: List[str] = []
        job_started = time.perf_counter()
        ingress_publish_ms = 0.0
        queue_put_ms = 0.0
        result_wait_ms = 0.0
        egress_open_clone_ms = 0.0
        try:
            ingress_started = time.perf_counter()
            for name, tensor in tensors.items():
                ref = transport.publish(tensor)
                refs[str(name)] = ref
                cleanup_handles.append(ref.handle)
            ingress_publish_ms = (time.perf_counter() - ingress_started) * 1000.0
            envelope = StageEnvelope(
                schema_version=1,
                job_id=job_id or uuid.uuid4().hex,
                stage="ingress",
                attempt=0,
                producer_generation="client",
                consumer_generation=self.stages[0].generation,
                deadline_monotonic=time.monotonic() + max(0.1, float(timeout)),
                payload=refs,
                metadata=dict(metadata or {}),
                cleanup_handles=tuple(cleanup_handles),
            )
            envelope.validate()
            queue_started = time.perf_counter()
            self._queues[0].put(envelope.to_dict(), timeout=max(0.1, float(timeout)))
            queue_put_ms = (time.perf_counter() - queue_started) * 1000.0
            wait_started = time.perf_counter()
            result = self._queues[-1].get(timeout=max(0.1, float(timeout)))
            result_wait_ms = (time.perf_counter() - wait_started) * 1000.0
            if result is None:
                raise StageTransportError("Omni worker exited before returning a result")
            cleanup_handles = list(dict.fromkeys(result.get("cleanup_handles") or cleanup_handles))
            if not result.get("ok"):
                self._metrics["failures"] += 1
                raise StageTransportError(result.get("error", "unknown Omni stage failure"))
            final = StageEnvelope.from_dict(result["envelope"])
            # Successful terminal results carry cleanup handles inside the
            # final envelope (failure results carry them at the top level).
            # Retain both so every intermediate producer-owned SHM/TCP object
            # is released by the coordinator rather than leaking after a
            # multi-stage success path.
            cleanup_handles = list(
                dict.fromkeys([*cleanup_handles, *final.cleanup_handles])
            )
            opened = []
            output: Dict[str, torch.Tensor] = {}
            egress_started = time.perf_counter()
            try:
                for name, ref in final.payload.items():
                    tensor, segment = transport.open(ref)
                    output[name] = tensor.clone()
                    opened.append(segment)
            finally:
                for segment in opened:
                    if segment is not None:
                        segment.close()
            egress_open_clone_ms = (time.perf_counter() - egress_started) * 1000.0
            pipeline_timing_ms = {
                "ingress_publish_ms": ingress_publish_ms,
                "ingress_queue_put_ms": queue_put_ms,
                "result_wait_ms": result_wait_ms,
                "egress_open_clone_ms": egress_open_clone_ms,
                "wall_ms": (time.perf_counter() - job_started) * 1000.0,
            }
            self._metrics["jobs"] += 1
            self._metrics["published_tensors"] += len(refs) + len(final.payload)
            self._metrics["last_job_timing_ms"] = dict(pipeline_timing_ms)
            output_metadata = dict(final.metadata)
            output_metadata["pipeline_timing_ms"] = pipeline_timing_ms
            return output, output_metadata
        except (queue.Empty, queue.Full) as exc:
            self._metrics["failures"] += 1
            raise TimeoutError("Omni process pipeline queue timed out") from exc
        finally:
            for handle in dict.fromkeys(cleanup_handles):
                transport.release(handle)
                self._metrics["released_tensors"] += 1

    def stats(self) -> Dict[str, Any]:
        return {
            **self._metrics,
            "started": self._started,
            "stage_pids": [process.pid for process in self._processes],
            "transport": self.transport_name,
            "tcp_relay": (
                dict(self._tcp_relay.stats())
                if self._tcp_relay is not None
                else None
            ),
            "queue_capacity": self.queue_capacity,
        }
