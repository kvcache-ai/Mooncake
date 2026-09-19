"""Omni multi-stage pipeline abstraction (RFC §7).

Generalizes EPD to arbitrary worker-level stage chains.  Stage outputs are
transported through the same production TransferEngine primitives used by
E→P/P→D/A2A: tensors, FeatureBundles and nested containers are moved to the
next worker device; per-edge policies make RDMA/SHM/TCP selection explicit.
"""

from __future__ import annotations

import contextlib
import multiprocessing as mp
import os
import queue
import socket
import threading
import traceback
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

import torch

from .state import FeatureBundle
from .transfer.engine import TransferEngine
from .transfer.policy import Channel, Mode, TransferPolicy
from .strict_mode import strict_no_fallback_enabled


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
    bytes: int = 0
    total_ms: float = 0.0
    last_ms: float = 0.0
    last_backend: str = ""
    backend_counts: Dict[str, int] = field(default_factory=dict)
    fallback_count: int = 0
    fallback_reasons: List[str] = field(default_factory=list)
    protocol_counts: Dict[str, int] = field(default_factory=dict)
    path_counts: Dict[str, int] = field(default_factory=dict)
    source_memory_mode_counts: Dict[str, int] = field(default_factory=dict)
    remote_sessions: List[str] = field(default_factory=list)
    remote_pointer_count: int = 0
    remote_pointers_sample: List[int] = field(default_factory=list)
    last_remote_session: str = ""
    last_remote_pointer: Optional[int] = None


@dataclass(frozen=True)
class OmniRemoteTensorRef:
    """Descriptor for a tensor transferred into a remote stage-owned buffer."""

    remote_session: str
    remote_pointer: int
    nbytes: int
    shape: Tuple[int, ...]
    dtype: str
    device: str
    owner_worker_id: str = ""
    backend_label: str = "mooncake_engine_direct:remote_peer_buffer"


@dataclass(frozen=True)
class OmniStageWorkerSpec:
    """Worker placement and transport hints for one Omni stage.

    The stage abstraction stays intentionally small (`run(input_refs)`), while
    this sidecar describes where the worker lives and how adjacent workers
    should move tensors.  `same_host=True` enables the low-latency SHM/CUDA-copy
    path.  Cross-host deployments can pass a real Mooncake
    ``remote_session``/peer-buffer metadata through ``TransferPolicy.extra`` and
    select ``transport_backend="mooncake_engine_direct"`` for one-sided RDMA/TCP
    writes.
    """

    stage_name: str
    worker_id: str = ""
    device: Optional[str] = None
    hostname: str = field(default_factory=socket.gethostname)
    same_host: bool = True
    transport_backend: str = "auto"  # auto | shm | mooncake_engine_direct | rdma

    @classmethod
    def from_stage(cls, stage: OmniStage, value: Any = None) -> "OmniStageWorkerSpec":
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            data = dict(value)
            data.setdefault("stage_name", getattr(stage, "name", "stage"))
            return cls(**data)
        return cls(stage_name=getattr(stage, "name", "stage"))


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
        worker_per_stage: Optional[Sequence[Any]] = None,
    ):
        if len(stages) < 1:
            raise ValueError("OmniPipeline requires at least one stage")
        if device_per_stage is not None and len(device_per_stage) != len(stages):
            raise ValueError("device_per_stage must match stages length")
        if policy_per_edge is not None and len(policy_per_edge) != max(0, len(stages) - 1):
            raise ValueError("policy_per_edge must have len(stages)-1 entries")
        if worker_per_stage is not None and len(worker_per_stage) != len(stages):
            raise ValueError("worker_per_stage must match stages length")
        self.stages = list(stages)
        self.transfer = transfer
        self.worker_per_stage = [
            OmniStageWorkerSpec.from_stage(stage, worker_per_stage[idx] if worker_per_stage else None)
            for idx, stage in enumerate(self.stages)
        ]
        self.device_per_stage: Optional[List[torch.device]]
        if device_per_stage is not None:
            self.device_per_stage = [torch.device(d) for d in device_per_stage]
        else:
            inferred_devices = [
                spec.device for spec in self.worker_per_stage if spec.device is not None
            ]
            self.device_per_stage = (
                [torch.device(spec.device or "cpu") for spec in self.worker_per_stage]
                if len(inferred_devices) == len(self.worker_per_stage)
                else None
            )
        self.policy_per_edge = list(policy_per_edge) if policy_per_edge else None
        if self.device_per_stage is not None:
            mismatches = [
                idx
                for idx, spec in enumerate(self.worker_per_stage)
                if spec.device is not None and torch.device(spec.device) != self.device_per_stage[idx]
            ]
            if mismatches:
                raise ValueError(
                    "worker_per_stage device topology conflicts with device_per_stage; "
                    f"mismatched stages={mismatches}"
                )
        if self.transfer is not None and self.device_per_stage is None and len(self.stages) > 1:
            strict_edges = [
                idx for idx in range(len(self.stages) - 1)
                if self._edge_requires_transfer(idx, self._edge_policy(idx))
            ]
            if strict_edges:
                raise ValueError(
                    "device_per_stage or complete worker_per_stage devices are required "
                    f"for explicit/strict Omni edge transfer; incomplete edges={strict_edges}"
                )
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
        policy = self._edge_policy(edge_index)
        if self.transfer is None or self.device_per_stage is None:
            if self._edge_requires_transfer(edge_index, policy):
                raise RuntimeError(
                    "explicit/strict Omni stage transfer cannot run without "
                    "TransferEngine and complete device_per_stage"
                )
            return objects
        target_device = self.device_per_stage[edge_index + 1]
        edge_name = f"{self.stages[edge_index].name}->{self.stages[edge_index + 1].name}"
        t0 = time.perf_counter()
        moved: List[Any] = []
        backends: List[str] = []
        transfer_details: List[Dict[str, Any]] = []
        moved_bytes = 0
        fallback_count = 0
        fallback_reasons: List[str] = []
        for obj in objects:
            next_obj, obj_backends, obj_bytes, obj_fallbacks, obj_reasons, obj_details = self._move_object(
                obj,
                target_device,
                policy,
                edge_index=edge_index,
            )
            moved.append(next_obj)
            backends.extend(obj_backends)
            moved_bytes += obj_bytes
            fallback_count += obj_fallbacks
            fallback_reasons.extend(obj_reasons)
            transfer_details.extend(obj_details)
        ms = (time.perf_counter() - t0) * 1000
        estats = self._edge_stats.setdefault(edge_name, EdgeTransferStats(edge=edge_name))
        estats.transfers += 1
        estats.objects += len(objects)
        estats.bytes += moved_bytes
        estats.total_ms += ms
        estats.last_ms = ms
        estats.fallback_count += fallback_count
        estats.fallback_reasons.extend(fallback_reasons)
        for backend in backends or ["metadata_only"]:
            estats.backend_counts[backend] = estats.backend_counts.get(backend, 0) + 1
            estats.last_backend = backend
        for detail in transfer_details:
            protocol = str(detail.get("protocol") or "")
            if protocol:
                estats.protocol_counts[protocol] = estats.protocol_counts.get(protocol, 0) + 1
            path = str(detail.get("path") or "")
            if path:
                estats.path_counts[path] = estats.path_counts.get(path, 0) + 1
            source_memory_mode = str(detail.get("source_memory_mode") or "")
            if source_memory_mode:
                estats.source_memory_mode_counts[source_memory_mode] = (
                    estats.source_memory_mode_counts.get(source_memory_mode, 0) + 1
                )
            remote_session = str(detail.get("remote_session") or "")
            if remote_session:
                estats.last_remote_session = remote_session
                if remote_session not in estats.remote_sessions and len(estats.remote_sessions) < 16:
                    estats.remote_sessions.append(remote_session)
            remote_pointer = detail.get("remote_pointer")
            if remote_pointer is not None:
                ptr = int(remote_pointer)
                estats.last_remote_pointer = ptr
                estats.remote_pointer_count += 1
                if len(estats.remote_pointers_sample) < 16:
                    estats.remote_pointers_sample.append(ptr)
        return moved

    def _edge_policy(self, edge_index: int) -> TransferPolicy:
        if self.policy_per_edge is not None:
            return self.policy_per_edge[edge_index]
        return TransferPolicy(Mode.STREAM, channel=Channel.AGENT_TO_AGENT)

    def _edge_requires_transfer(self, edge_index: int, policy: TransferPolicy) -> bool:
        extra = getattr(policy, "extra", {}) or {}
        if strict_no_fallback_enabled(extra):
            return True
        requested = str(
            extra.get("omni_transport_backend")
            or extra.get("transport_backend")
            or ""
        ).lower()
        if requested in {"shm", "shared_memory", "cuda_ipc", "rdma", "engine_direct", "mooncake_engine_direct", "direct_engine"}:
            return True
        if policy.mode in {Mode.SHM, Mode.PUSH_BATCH, Mode.PULL}:
            return True
        if edge_index + 1 < len(self.worker_per_stage):
            src = self.worker_per_stage[edge_index]
            dst = self.worker_per_stage[edge_index + 1]
            spec_backend = str(dst.transport_backend or src.transport_backend or "auto").lower()
            return spec_backend not in {"", "auto", "local"}
        return False

    def _move_object(
        self,
        obj: Any,
        target_device: torch.device,
        policy: TransferPolicy,
        *,
        edge_index: int,
    ) -> Tuple[Any, List[str], int, int, List[str], List[Dict[str, Any]]]:
        if isinstance(obj, torch.Tensor):
            moved, backend, fallback, reasons, detail = self._move_tensor(
                obj,
                target_device,
                policy,
                edge_index=edge_index,
            )
            return moved, [backend], self._tensor_nbytes(obj), int(fallback), reasons, [detail]
        if isinstance(obj, FeatureBundle):
            return self._move_feature_bundle(
                obj,
                target_device,
                policy,
                edge_index=edge_index,
            )
        if isinstance(obj, tuple):
            values: List[Any] = []
            backends: List[str] = []
            total_bytes = 0
            fallbacks = 0
            reasons: List[str] = []
            details: List[Dict[str, Any]] = []
            for x in obj:
                moved, obj_backends, obj_bytes, obj_fallbacks, obj_reasons, obj_details = self._move_object(
                    x,
                    target_device,
                    policy,
                    edge_index=edge_index,
                )
                values.append(moved)
                backends.extend(obj_backends)
                total_bytes += obj_bytes
                fallbacks += obj_fallbacks
                reasons.extend(obj_reasons)
                details.extend(obj_details)
            return tuple(values), backends, total_bytes, fallbacks, reasons, details
        if isinstance(obj, list):
            values = []
            backends: List[str] = []
            total_bytes = 0
            fallbacks = 0
            reasons: List[str] = []
            details: List[Dict[str, Any]] = []
            for x in obj:
                moved, obj_backends, obj_bytes, obj_fallbacks, obj_reasons, obj_details = self._move_object(
                    x,
                    target_device,
                    policy,
                    edge_index=edge_index,
                )
                values.append(moved)
                backends.extend(obj_backends)
                total_bytes += obj_bytes
                fallbacks += obj_fallbacks
                reasons.extend(obj_reasons)
                details.extend(obj_details)
            return values, backends, total_bytes, fallbacks, reasons, details
        if isinstance(obj, Mapping):
            mapped_values: Dict[Any, Any] = {}
            backends: List[str] = []
            total_bytes = 0
            fallbacks = 0
            reasons: List[str] = []
            details: List[Dict[str, Any]] = []
            for k, v in obj.items():
                moved, obj_backends, obj_bytes, obj_fallbacks, obj_reasons, obj_details = self._move_object(
                    v,
                    target_device,
                    policy,
                    edge_index=edge_index,
                )
                mapped_values[k] = moved
                backends.extend(obj_backends)
                total_bytes += obj_bytes
                fallbacks += obj_fallbacks
                reasons.extend(obj_reasons)
                details.extend(obj_details)
            return mapped_values, backends, total_bytes, fallbacks, reasons, details
        return obj, [], 0, 0, [], []

    @staticmethod
    def _tensor_nbytes(tensor: torch.Tensor) -> int:
        return int(tensor.nelement() * tensor.element_size())

    def _move_feature_bundle(
        self,
        bundle: FeatureBundle,
        target_device: torch.device,
        policy: TransferPolicy,
        *,
        edge_index: int,
    ) -> Tuple[FeatureBundle, List[str], int, int, List[str], List[Dict[str, Any]]]:
        backend = self._resolve_backend(policy, edge_index=edge_index)
        if backend in {"rdma_peer_buffer", "shm"}:
            last_hidden, b0, n0, f0, r0, d0 = self._move_object(
                bundle.last_hidden,
                target_device,
                policy,
                edge_index=edge_index,
            )
            grid_thw = None
            b1: List[str] = []
            n1 = f1 = 0
            r1: List[str] = []
            d1: List[Dict[str, Any]] = []
            if bundle.grid_thw is not None:
                grid_thw, b1, n1, f1, r1, d1 = self._move_object(
                    bundle.grid_thw,
                    target_device,
                    policy,
                    edge_index=edge_index,
                )
            restored = []
            backends = list(b0) + list(b1)
            total_bytes = int(n0) + int(n1)
            fallbacks = int(f0) + int(f1)
            reasons = list(r0) + list(r1)
            details = list(d0) + list(d1)
            for idx, tensor in bundle.intermediates:
                moved, tensor_backends, tensor_bytes, tensor_fallbacks, tensor_reasons, tensor_details = self._move_object(
                    tensor,
                    target_device,
                    policy,
                    edge_index=edge_index,
                )
                restored.append((idx, moved))
                backends.extend(tensor_backends)
                total_bytes += tensor_bytes
                fallbacks += tensor_fallbacks
                reasons.extend(tensor_reasons)
                details.extend(tensor_details)
            return (
                FeatureBundle(
                    image_hash=bundle.image_hash,
                    last_hidden=last_hidden,
                    intermediates=restored,
                    grid_thw=grid_thw,
                    metadata=bundle.metadata,
                ),
                backends,
                total_bytes,
                fallbacks,
                reasons,
                details,
            )
        if self.transfer is None:
            return bundle, ["metadata_only"], 0, 0, [], []
        moved = self.transfer.transfer_feature_bundle(bundle, target_device, policy)
        return moved, [backend], int(bundle.nbytes()), 0, [], [
            self._transfer_detail(backend, policy, edge_index=edge_index)
        ]

    def _move_tensor(
        self,
        tensor: torch.Tensor,
        target_device: torch.device,
        policy: TransferPolicy,
        *,
        edge_index: int,
    ) -> Tuple[Any, str, bool, List[str], Dict[str, Any]]:
        if self.transfer is None:
            return tensor, "metadata_only", False, [], self._transfer_detail(
                "metadata_only", policy, edge_index=edge_index
            )
        backend = self._resolve_backend(policy, edge_index=edge_index)
        if backend == "shm":
            return (
                self._transfer_tensor_shm(tensor, target_device, policy),
                "shm",
                False,
                [],
                self._transfer_detail("shm", policy, edge_index=edge_index),
            )
        if backend == "rdma_peer_buffer":
            try:
                moved, label, detail = self._transfer_tensor_peer_buffer_direct(
                    tensor,
                    target_device,
                    policy,
                    edge_index=edge_index,
                )
                return moved, label, False, [], detail
            except Exception as exc:
                if strict_no_fallback_enabled(getattr(policy, "extra", {}) or {}):
                    raise
                fallback_label = f"{self._direct_backend_label('fallback_shm')}"
                fallback_reason = f"{type(exc).__name__}: {exc}"
                return (
                    self._transfer_tensor_shm(tensor, target_device, policy),
                    fallback_label,
                    True,
                    [fallback_reason],
                    self._transfer_detail(
                        fallback_label,
                        policy,
                        edge_index=edge_index,
                        path="fallback_shm",
                    ),
                )
        return (
            self.transfer.transfer_tensor(tensor, target_device, policy),
            backend,
            False,
            [],
            self._transfer_detail(backend, policy, edge_index=edge_index),
        )

    def _transfer_detail(
        self,
        backend: str,
        policy: TransferPolicy,
        *,
        edge_index: int,
        path: Optional[str] = None,
        remote_session: Optional[str] = None,
        remote_pointer: Optional[int] = None,
    ) -> Dict[str, Any]:
        extra = getattr(policy, "extra", {}) or {}
        if path is None:
            if backend == "shm":
                path = "shm_same_host"
            elif backend.endswith(":local_dst_tensor"):
                path = "local_destination_tensor"
            elif backend.endswith(":remote_peer_buffer"):
                path = "remote_peer_buffer"
            elif "fallback" in backend:
                path = "fallback"
            elif backend == "metadata_only":
                path = "metadata_only"
            else:
                path = backend or "unknown"
        source_memory_mode = str(extra.get("source_memory_mode") or "")
        if not source_memory_mode and str(backend).startswith("mooncake_engine_direct:"):
            source_memory_mode = "registered_tensor"
        detail: Dict[str, Any] = {
            "backend": backend,
            "protocol": self.transfer.protocol if self.transfer is not None else "none",
            "path": path,
            "source_memory_mode": source_memory_mode,
            "source_worker_id": self.worker_per_stage[edge_index].worker_id,
            "target_worker_id": self.worker_per_stage[edge_index + 1].worker_id,
        }
        session = remote_session if remote_session is not None else extra.get("remote_session")
        if session:
            detail["remote_session"] = str(session)
        pointer = remote_pointer if remote_pointer is not None else extra.get("peer_buffer_addr")
        if pointer is not None:
            detail["remote_pointer"] = int(pointer)
        return detail

    def _resolve_backend(self, policy: TransferPolicy, *, edge_index: int) -> str:
        extra = getattr(policy, "extra", {}) or {}
        requested = str(
            extra.get("omni_transport_backend")
            or extra.get("transport_backend")
            or ""
        ).lower()
        src = self.worker_per_stage[edge_index]
        dst = self.worker_per_stage[edge_index + 1]
        spec_backend = str(dst.transport_backend or src.transport_backend or "auto").lower()

        if requested in {"shm", "shared_memory", "cuda_ipc"} or policy.mode is Mode.SHM:
            return "shm"
        if requested in {"rdma", "engine_direct", "mooncake_engine_direct", "direct_engine"}:
            if requested == "rdma" and self.transfer is not None and self.transfer.protocol != "rdma" and strict_no_fallback_enabled(extra):
                raise RuntimeError(
                    f"strict RDMA edge requested but TransferEngine protocol is {self.transfer.protocol!r}"
                )
            if (
                requested in {"engine_direct", "mooncake_engine_direct", "direct_engine"}
                and self.transfer is not None
                and self.transfer.protocol not in {"tcp", "rdma"}
                and strict_no_fallback_enabled(extra)
            ):
                raise RuntimeError(
                    "strict Mooncake direct edge requires TransferEngine protocol "
                    f"'tcp' or 'rdma', got {self.transfer.protocol!r}"
                )
            return "rdma_peer_buffer"
        if spec_backend in {"rdma", "engine_direct", "mooncake_engine_direct", "direct_engine"}:
            return "rdma_peer_buffer"
        if spec_backend in {"shm", "shared_memory", "cuda_ipc"}:
            return "shm"
        if src.same_host and dst.same_host and src.hostname == dst.hostname:
            return "shm"
        if self.transfer is not None and self.transfer.protocol in {"rdma", "tcp"}:
            return "remote"
        return "local"

    def _transfer_tensor_shm(
        self,
        tensor: torch.Tensor,
        target_device: torch.device,
        policy: TransferPolicy,
    ) -> torch.Tensor:
        """Same-host low-latency transfer.

        CPU tensors are promoted to POSIX shared memory so process-based stage
        workers can reuse the storage without a file/object-store hop.  CUDA
        tensors use PyTorch's real device-to-device copy path; this is the
        reliable same-host primitive behind CUDA IPC/P2P deployments and keeps
        the API materialized for in-process workers.
        """

        start = time.perf_counter()
        nbytes = self._tensor_nbytes(tensor)
        copy_flag = bool((getattr(policy, "extra", {}) or {}).get("force_copy", False))
        if tensor.device.type == "cpu" and target_device.type == "cpu":
            result = tensor.detach().contiguous().clone() if copy_flag else tensor.detach().contiguous()
            if not result.is_shared():
                result.share_memory_()
        else:
            result = tensor.to(
                target_device,
                copy=(tensor.device != target_device) or copy_flag,
            )
        if self.transfer is not None:
            self.transfer.stats.record(
                policy.channel.value if policy.channel else "omni_shm",
                nbytes,
                (time.perf_counter() - start) * 1000.0,
            )
        return result

    def _transfer_tensor_peer_buffer_direct(
        self,
        tensor: torch.Tensor,
        target_device: torch.device,
        policy: TransferPolicy,
        *,
        edge_index: int,
    ) -> Tuple[Any, str, Dict[str, Any]]:
        """Transfer tensor through Mooncake direct peer-buffer descriptors.

        Production mode accepts receiver-owned ``remote_session`` +
        ``peer_buffer_addr`` descriptors and returns an ``OmniRemoteTensorRef``
        unless a materialized receiver tensor is supplied. Local validation mode
        allocates a destination tensor on the next stage device, registers it,
        and writes directly into that tensor pointer. The former proves the
        control-plane contract; the latter gives in-process stages a materialized
        tensor without Python bytes serialization.
        """

        extra = getattr(policy, "extra", {}) or {}
        remote_session = extra.get("remote_session")
        peer_buffer_addr = extra.get("peer_buffer_addr")
        peer_buffer_nbytes = (
            extra.get("peer_buffer_nbytes")
            if extra.get("peer_buffer_nbytes") is not None
            else extra.get("remote_nbytes", extra.get("nbytes"))
        )
        materialized = extra.get("materialized_result")
        if remote_session and peer_buffer_addr is not None:
            required_nbytes = self._tensor_nbytes(tensor)
            if peer_buffer_nbytes is None and strict_no_fallback_enabled(extra):
                raise RuntimeError(
                    "strict direct Omni edge remote peer buffer requires explicit "
                    "peer_buffer_nbytes/remote_nbytes/nbytes metadata"
                )
            if peer_buffer_nbytes is not None and int(peer_buffer_nbytes) < required_nbytes:
                raise RuntimeError(
                    "remote peer buffer is smaller than tensor payload: "
                    f"remote_nbytes={int(peer_buffer_nbytes)}, required={required_nbytes}"
                )
            result = self._transfer_tensor_to_remote_peer_buffer(
                tensor,
                policy,
                remote_session=str(remote_session),
                remote_pointer=int(peer_buffer_addr),
                materialized_result=materialized if isinstance(materialized, torch.Tensor) else None,
                owner_worker_id=self.worker_per_stage[edge_index + 1].worker_id,
            )
            label = self._direct_backend_label("remote_peer_buffer")
            return result, label, self._transfer_detail(
                label,
                policy,
                edge_index=edge_index,
                path="remote_peer_buffer",
                remote_session=str(remote_session),
                remote_pointer=int(peer_buffer_addr),
            )

        if strict_no_fallback_enabled(extra) and bool(extra.get("require_remote_peer_buffer", False)):
            raise RuntimeError(
                "strict direct Omni edge requires receiver-provided remote_session "
                "and peer_buffer_addr"
            )
        moved, local_session, local_pointer = self._transfer_tensor_to_local_destination_buffer(
            tensor, target_device, policy
        )
        label = self._direct_backend_label("local_dst_tensor")
        return (
            moved,
            label,
            self._transfer_detail(
                label,
                policy,
                edge_index=edge_index,
                path="local_destination_tensor",
                remote_session=local_session,
                remote_pointer=local_pointer,
            ),
        )

    def _transfer_tensor_to_remote_peer_buffer(
        self,
        tensor: torch.Tensor,
        policy: TransferPolicy,
        *,
        remote_session: str,
        remote_pointer: int,
        materialized_result: Optional[torch.Tensor] = None,
        owner_worker_id: str = "",
    ) -> Any:
        if self.transfer is None:
            raise RuntimeError("Mooncake direct remote transfer requires TransferEngine")
        nbytes = self._tensor_nbytes(tensor)
        source_tensor = tensor.detach().contiguous()
        started = time.perf_counter()
        plan = self.transfer.build_peer_transfer_plan(
            tensors=[source_tensor],
            remote_session=remote_session,
            remote_pointers=[remote_pointer],
            mirror_tensors=[materialized_result] if materialized_result is not None else None,
            mirror_local_copy=False,
            target_device=materialized_result.device if materialized_result is not None else None,
        )
        result = self.transfer.transfer_peer_buffer_plan(plan)
        self.transfer.stats.record(
            policy.channel.value if policy.channel else "omni_direct_remote_peer_buffer",
            nbytes,
            (time.perf_counter() - started) * 1000.0,
        )
        if materialized_result is not None:
            mirrored = result.mirrored_tensors[0] if result.mirrored_tensors else None
            return mirrored if isinstance(mirrored, torch.Tensor) else materialized_result
        return OmniRemoteTensorRef(
            remote_session=remote_session,
            remote_pointer=remote_pointer,
            nbytes=nbytes,
            shape=tuple(int(dim) for dim in tensor.shape),
            dtype=str(tensor.dtype).replace("torch.", ""),
            device=str(tensor.device),
            owner_worker_id=owner_worker_id,
            backend_label=self._direct_backend_label("remote_peer_buffer"),
        )

    def _transfer_tensor_to_local_destination_buffer(
        self,
        tensor: torch.Tensor,
        target_device: torch.device,
        policy: TransferPolicy,
    ) -> Tuple[torch.Tensor, str, int]:
        if self.transfer is None:
            raise RuntimeError("RDMA peer-buffer stage transfer requires TransferEngine")
        nbytes = self._tensor_nbytes(tensor)
        if nbytes == 0:
            result = tensor.to(target_device, copy=True)
            return result, "", int(result.data_ptr())
        remote_session = self._default_peer_remote_session()
        started = time.perf_counter()
        source_tensor = tensor.detach().contiguous()
        target_tensor = torch.empty_like(source_tensor, device=target_device)
        target_handle = self.transfer.register_tensor_memory(target_tensor)
        try:
            plan = self.transfer.build_peer_transfer_plan(
                tensors=[source_tensor],
                remote_session=remote_session,
                remote_pointers=[int(target_tensor.data_ptr())],
                mirror_tensors=[target_tensor],
                mirror_local_copy=False,
                target_device=target_device,
            )
            self.transfer.transfer_peer_buffer_plan(plan)
            self.transfer.stats.record(
                policy.channel.value if policy.channel else "omni_rdma_peer_buffer",
                nbytes,
                (time.perf_counter() - started) * 1000.0,
            )
        finally:
            with contextlib.suppress(Exception):
                self.transfer.unregister_tensor_memory(target_handle)
        return target_tensor, remote_session, int(target_tensor.data_ptr())

    def _default_peer_remote_session(self) -> str:
        """Return the concrete local Mooncake peer session for loopback writes."""

        if self.transfer is None:
            raise RuntimeError("TransferEngine is required")
        backend = getattr(self.transfer, "_mooncake", None)
        if backend is not None and hasattr(backend, "get_rpc_port"):
            port = int(backend.get_rpc_port())
            if port > 0:
                return f"{self.transfer.local_hostname}:{port}"
        return self.transfer.direct_remote_session()

    def _direct_backend_label(self, suffix: str) -> str:
        protocol = self.transfer.protocol if self.transfer is not None else "none"
        return f"mooncake_engine_direct:{protocol}:{suffix}"

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
                    "bytes": s.bytes,
                    "total_ms": s.total_ms,
                    "last_ms": s.last_ms,
                    "avg_ms": s.total_ms / s.transfers if s.transfers else 0.0,
                    "last_backend": s.last_backend,
                    "backend_counts": dict(s.backend_counts),
                    "fallback_count": s.fallback_count,
                    "fallback_reasons": list(s.fallback_reasons),
                    "protocol_counts": dict(s.protocol_counts),
                    "path_counts": dict(s.path_counts),
                    "source_memory_mode_counts": dict(s.source_memory_mode_counts),
                    "remote_sessions": list(s.remote_sessions),
                    "remote_pointer_count": s.remote_pointer_count,
                    "remote_pointers_sample": list(s.remote_pointers_sample),
                    "last_remote_session": s.last_remote_session,
                    "last_remote_pointer": s.last_remote_pointer,
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


_PROCESS_STOP = "STOP"
_PROCESS_JOB = "JOB"
_PROCESS_OK = "OK"
_PROCESS_ERR = "ERR"


def _edge_stats_delta(before: Mapping[str, Any], after: Mapping[str, Any]) -> Dict[str, Any]:
    """Return per-job edge stat delta from cumulative child-process stats."""

    before = dict(before or {})
    after = dict(after or {})
    out: Dict[str, Any] = {}
    for key in ("transfers", "objects", "bytes", "fallback_count", "remote_pointer_count"):
        out[key] = int(after.get(key, 0) or 0) - int(before.get(key, 0) or 0)
    for key in ("total_ms",):
        out[key] = float(after.get(key, 0.0) or 0.0) - float(before.get(key, 0.0) or 0.0)
    for key in ("last_ms", "last_backend", "last_remote_session", "last_remote_pointer"):
        out[key] = after.get(key)
    for key in ("backend_counts", "protocol_counts", "path_counts", "source_memory_mode_counts"):
        merged: Dict[str, int] = {}
        before_counts = dict(before.get(key, {}) or {})
        for sub_key, value in dict(after.get(key, {}) or {}).items():
            delta = int(value) - int(before_counts.get(sub_key, 0) or 0)
            if delta:
                merged[str(sub_key)] = delta
        out[key] = merged
    for key in ("fallback_reasons", "remote_sessions", "remote_pointers_sample"):
        before_values = list(before.get(key, []) or [])
        out[key] = [value for value in list(after.get(key, []) or []) if value not in before_values]
    transfers = int(out.get("transfers", 0) or 0)
    out["avg_ms"] = float(out.get("total_ms", 0.0) or 0.0) / transfers if transfers else 0.0
    return out


def _process_worker_loop(
    pipeline: OmniPipeline,
    stage_index: int,
    queues: Sequence[Any],
    result_queue: Any,
) -> None:
    stage = pipeline.stages[stage_index]
    while True:
        item = queues[stage_index].get()
        tag = item[0] if isinstance(item, tuple) and item else None
        if tag == _PROCESS_STOP:
            if stage_index + 1 < len(queues):
                queues[stage_index + 1].put((_PROCESS_STOP,))
            return
        if tag != _PROCESS_JOB:
            continue
        if len(item) >= 4:
            _, job_id, payload, traces = item
        else:
            _, job_id, payload = item
            traces = []
        try:
            t0 = time.perf_counter()
            out = list(stage.run(payload))
            ms = (time.perf_counter() - t0) * 1000.0
            trace = {
                "stage": stage.name,
                "stage_index": stage_index,
                "pid": os.getpid(),
                "last_stage_ms": ms,
            }
            if stage_index + 1 < len(pipeline.stages):
                edge_name = f"{pipeline.stages[stage_index].name}->{pipeline.stages[stage_index + 1].name}"
                before_edge_stats = pipeline.stats().get("edges", {}).get(edge_name, {})
                moved = pipeline._transfer_edge(stage_index, out)  # noqa: SLF001
                after_edge_stats = pipeline.stats().get("edges", {}).get(edge_name, {})
                trace["edge"] = edge_name
                trace["edge_stats"] = _edge_stats_delta(before_edge_stats, after_edge_stats)
                queues[stage_index + 1].put((_PROCESS_JOB, job_id, moved, list(traces) + [trace]))
            else:
                result_queue.put(
                    (
                        _PROCESS_OK,
                        job_id,
                        out,
                        {
                            "stage_traces": list(traces) + [trace],
                        },
                    )
                )
        except BaseException as exc:  # propagate child failure details
            result_queue.put(
                (
                    _PROCESS_ERR,
                    job_id,
                    repr(exc),
                    traceback.format_exc(),
                )
            )


class OmniPipelineProcessRuntime:
    """Process-isolated worker runtime for pickleable Omni stages.

    This runtime is intentionally stricter than the thread runtime: each stage
    executes in a separate OS process and communicates through multiprocessing
    queues. It is suitable for production integration harnesses and CPU/SHM
    validation. Heavy CUDA model workers should be created via process-safe
    stage factories in a higher-level launcher; the class itself avoids hiding
    the process boundary behind threads.
    """

    def __init__(
        self,
        pipeline: OmniPipeline,
        *,
        queue_size: int = 16,
        worker_name_prefix: str = "omni-proc",
        start_method: Optional[str] = None,
    ):
        self.pipeline = pipeline
        self.queue_size = max(1, int(queue_size))
        self.worker_name_prefix = str(worker_name_prefix)
        self._ctx = mp.get_context(start_method) if start_method else mp.get_context()
        self._queues = [
            self._ctx.Queue(maxsize=self.queue_size)
            for _ in range(len(self.pipeline.stages))
        ]
        self._result_queue = self._ctx.Queue()
        self._processes: List[mp.Process] = []
        self._started = False
        self._stopped = False
        self._last_job_meta: Dict[str, Any] = {}
        self._aggregate_stage_stats: Dict[str, Dict[str, Any]] = {}
        self._aggregate_edge_stats: Dict[str, Dict[str, Any]] = {}

    def start(self) -> None:
        if self._started:
            return
        self._stopped = False
        for index, stage in enumerate(self.pipeline.stages):
            proc = self._ctx.Process(
                target=_process_worker_loop,
                args=(self.pipeline, index, self._queues, self._result_queue),
                name=f"{self.worker_name_prefix}-{stage.name}",
                daemon=True,
            )
            proc.start()
            self._processes.append(proc)
        self._started = True

    def stop(self, timeout: float = 5.0) -> None:
        if not self._started or self._stopped:
            return
        self._stopped = True
        self._queues[0].put((_PROCESS_STOP,))
        deadline = time.monotonic() + max(0.0, float(timeout))
        for proc in list(self._processes):
            remaining = max(0.0, deadline - time.monotonic())
            proc.join(timeout=remaining)
        for proc in list(self._processes):
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=1.0)

    def run(self, inputs: Sequence[Any], *, timeout: Optional[float] = None) -> List[Any]:
        self.start()
        if self._stopped:
            raise RuntimeError("OmniPipelineProcessRuntime is stopped")
        job_id = uuid.uuid4().hex
        self._queues[0].put((_PROCESS_JOB, job_id, list(inputs), []), timeout=timeout)
        deadline = None if timeout is None else time.monotonic() + max(0.0, float(timeout))
        while True:
            wait_timeout = None if deadline is None else max(0.0, deadline - time.monotonic())
            if wait_timeout == 0.0:
                raise TimeoutError(f"Omni process job timed out: {job_id}")
            tag, seen_job_id, payload, meta = self._result_queue.get(timeout=wait_timeout)
            if seen_job_id != job_id:
                continue
            if tag == _PROCESS_OK:
                self._last_job_meta = dict(meta or {})
                self._accumulate_job_meta(self._last_job_meta)
                return list(payload)
            if tag == _PROCESS_ERR:
                raise RuntimeError(f"Omni process worker failed: {payload}\n{meta}")

    def _accumulate_job_meta(self, meta: Mapping[str, Any]) -> None:
        for trace in meta.get("stage_traces", []) if isinstance(meta, Mapping) else []:
            if not isinstance(trace, Mapping):
                continue
            stage_name = str(trace.get("stage") or "")
            if stage_name:
                stage_stats = self._aggregate_stage_stats.setdefault(
                    stage_name,
                    {"runs": 0, "total_ms": 0.0, "last_ms": 0.0},
                )
                stage_ms = float(trace.get("last_stage_ms") or 0.0)
                stage_stats["runs"] += 1
                stage_stats["total_ms"] += stage_ms
                stage_stats["last_ms"] = stage_ms

            edge_name = str(trace.get("edge") or "")
            edge_stats = trace.get("edge_stats")
            if edge_name and isinstance(edge_stats, Mapping):
                aggregate = self._aggregate_edge_stats.setdefault(edge_name, {})
                self._merge_edge_stats(aggregate, edge_stats)

    @staticmethod
    def _merge_edge_stats(aggregate: Dict[str, Any], update: Mapping[str, Any]) -> None:
        for key in ("transfers", "objects", "bytes", "fallback_count", "remote_pointer_count"):
            aggregate[key] = int(aggregate.get(key, 0)) + int(update.get(key, 0) or 0)
        for key in ("total_ms",):
            aggregate[key] = float(aggregate.get(key, 0.0)) + float(update.get(key, 0.0) or 0.0)
        for key in ("last_ms", "last_backend", "last_remote_session", "last_remote_pointer"):
            if key in update:
                aggregate[key] = update.get(key)
        for key in (
            "backend_counts",
            "protocol_counts",
            "path_counts",
            "source_memory_mode_counts",
        ):
            merged = dict(aggregate.get(key, {}) or {})
            for sub_key, value in dict(update.get(key, {}) or {}).items():
                merged[str(sub_key)] = int(merged.get(str(sub_key), 0)) + int(value)
            aggregate[key] = merged
        for key in ("fallback_reasons", "remote_sessions", "remote_pointers_sample"):
            merged_list = list(aggregate.get(key, []) or [])
            for value in list(update.get(key, []) or []):
                if value not in merged_list:
                    merged_list.append(value)
            aggregate[key] = merged_list[:16] if key == "remote_pointers_sample" else merged_list
        transfers = int(aggregate.get("transfers", 0) or 0)
        aggregate["avg_ms"] = float(aggregate.get("total_ms", 0.0) or 0.0) / transfers if transfers else 0.0

    def stats(self) -> dict:
        stage_stats = {
            name: {
                "runs": int(values.get("runs", 0)),
                "total_ms": float(values.get("total_ms", 0.0)),
                "last_ms": float(values.get("last_ms", 0.0)),
                "avg_ms": float(values.get("total_ms", 0.0)) / int(values.get("runs", 0))
                if int(values.get("runs", 0))
                else 0.0,
            }
            for name, values in self._aggregate_stage_stats.items()
        }
        edge_stats = {name: dict(values) for name, values in self._aggregate_edge_stats.items()}
        return {
            "stages": stage_stats,
            "edges": edge_stats,
            "runtime": {
                "started": self._started,
                "stopped": self._stopped,
                "processes": [
                    {
                        "pid": proc.pid,
                        "name": proc.name,
                        "alive": proc.is_alive(),
                        "exitcode": proc.exitcode,
                    }
                    for proc in self._processes
                ],
                "queues": [
                    (q.qsize() if hasattr(q, "qsize") else -1)
                    for q in self._queues
                ],
                "last_job_meta": dict(self._last_job_meta),
            },
        }
