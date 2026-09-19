"""Transfer engine: protocol-agnostic transport over tensors and pages.

The engine exposes a uniform interface for four channel types (E->P, P->D,
A2A, Offload) and four mode back-ends (shm, stream, pull, push_batch).
On the local host, every "transfer" reduces to a CUDA ``tensor.to()``
between devices -- still a real, measured, bandwidth-bound copy. On
distributed deployments the same calls are routed to the Mooncake
Transfer Engine or the Mooncake Distributed Store.

Compression (``fp8`` / ``per_level`` / ``cacheGen``) is applied at the
Python layer before the wire transfer and reversed on the receiver side.
In local mode the compression still runs (and is measured) so the cost
and reconstruction error are observable without a cluster.
"""

from __future__ import annotations

import base64
import contextlib
import io
import json
import os
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import torch

from .policy import (
    Channel,
    CompressMode,
    HwCaps,
    Mode,
    Precision,
    TransferPolicy,
)
from .rdma import (
    RdmaCapabilities,
    default_rdma_bind_address,
    detect_rdma_capabilities,
    resolve_rdma_protocol,
)
from .rdmacm import RdmaStagedClient, RdmaStagedServer, RegisteredRegion
from .cachegen import _compress_cachegen, _decompress_cachegen
from ..strict_mode import strict_no_fallback_enabled


# ---------------------------------------------------------------------------
# Compression helpers (real, no mocking)
# ---------------------------------------------------------------------------
def _cast_precision(t: torch.Tensor, precision: Precision) -> torch.Tensor:
    if precision is Precision.BF16:
        if t.dtype in (torch.float16, torch.bfloat16):
            return t.to(torch.bfloat16) if t.dtype != torch.bfloat16 else t
        return t
    if precision is Precision.FP8:
        # Fall back to fp16 where fp8 is unsupported (older GPUs).
        target = torch.float16
        try:
            target = torch.float8_e4m3fn  # type: ignore[attr-defined]
        except AttributeError:
            pass
        return t.to(target)
    if precision is Precision.Q4:
        raise NotImplementedError(
            "Precision.Q4 requires shape/scale metadata and is not enabled for "
            "production transfer_tensor until loss-bounded dequantization is wired."
        )
    raise ValueError(precision)


def _compress_per_level(
    features: Sequence[Tuple[int, torch.Tensor]],
    base_precision: Precision,
    high_prec_threshold: float = 0.6,
    mid_prec_threshold: float = 0.3,
) -> List[Tuple[int, torch.Tensor]]:
    """Per-level compression for DeepStack feature bundles.

    Higher-index (closer-to-output) layers get higher precision; earlier
    layers are compressed harder because they carry less final-signal
    weight. Returns a list of (layer_idx, compressed_tensor).

    Thresholds are configurable:
    - ``high_prec_threshold``: rel >= this -> keep ``base_precision``.
    - ``mid_prec_threshold``:  rel >= this (and < high) -> fp8.
    - below ``mid_prec_threshold``: also fp8 (Q4 is avoided because its
      inverse requires (scale, offset) metadata not carried in-band).
    """
    if not features:
        return []
    max_idx = max(idx for idx, _ in features) or 1
    out: List[Tuple[int, torch.Tensor]] = []
    for idx, tensor in features:
        rel = idx / max_idx  # 0..1, higher = closer to output
        if rel >= high_prec_threshold:
            prec = base_precision
        elif rel >= mid_prec_threshold:
            prec = Precision.FP8
        else:
            prec = Precision.FP8
        out.append((idx, _cast_precision(tensor, prec)))
    return out


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------
@dataclass
class TransferStats:
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _records: Dict[str, List[Tuple[int, float]]] = field(
        default_factory=dict, repr=False,
    )

    def record(self, channel: str, nbytes: int, time_ms: float) -> None:
        with self._lock:
            self._records.setdefault(channel, []).append((nbytes, time_ms))

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            out: Dict[str, Dict[str, float]] = {}
            for ch, recs in self._records.items():
                total_bytes = sum(b for b, _ in recs)
                total_ms = sum(t for _, t in recs)
                peak = max(((b * 8) / (t / 1000) / 1e9) if t > 0 else 0 for b, t in recs)
                avg = (total_bytes * 8) / (total_ms / 1000) / 1e9 if total_ms else 0.0
                out[ch] = {
                    "transfers": len(recs),
                    "total_bytes": total_bytes,
                    "avg_bandwidth_gbps": avg,
                    "peak_bandwidth_gbps": peak,
                }
            return out


# ---------------------------------------------------------------------------
# TransferHandle
# ---------------------------------------------------------------------------
@dataclass
class TransferHandle:
    """Opaque async handle. ``.result()`` blocks until the transfer is done."""

    _future: Future
    nbytes: int = 0
    channel: str = ""

    def result(self, timeout: Optional[float] = None) -> Any:
        return self._future.result(timeout=timeout)

    def done(self) -> bool:
        return self._future.done()


@dataclass
class LayerTransferBatch:
    page_index: int
    layer_start: int
    layer_stop: int
    token_count: int
    bytes_transferred: int
    transfer_time_ms: float


@dataclass
class DirectPeerBuffer:
    pointer: int
    size_bytes: int
    registered: bool = False
    registration_id: Optional[str] = None
    borrowed: bool = False
    owns_registration: bool = False
    released: bool = False


@dataclass
class _RegistrationRecord:
    registration_id: str
    pointer: int
    size_bytes: int
    owned_by_engine: bool
    references: int = 1
    in_flight: int = 0


@dataclass
class PeerTransferDescriptor:
    local_pointer: int
    remote_pointer: int
    size_bytes: int
    tensor: Optional[torch.Tensor] = None
    local_buffer: Optional[DirectPeerBuffer] = None
    mirror_tensor: Optional[torch.Tensor] = None
    needs_unregister: bool = False


@dataclass
class PeerTransferPlan:
    remote_session: str
    descriptors: List[PeerTransferDescriptor]
    mirror_local_copy: bool = True
    target_device: Optional[torch.device] = None


@dataclass
class PeerTransferResult:
    nbytes: int
    descriptor_count: int
    mirrored_tensors: List[Optional[torch.Tensor]] = field(default_factory=list)


@dataclass(frozen=True)
class FeatureTensorPeerTarget:
    """Remote peer-buffer target for one tensor in an E→P FeatureBundle."""

    name: str
    remote_pointer: int
    nbytes: int


@dataclass(frozen=True)
class FeatureBundlePeerBufferPlan:
    """Direct Mooncake peer-buffer transfer plan for a FeatureBundle."""

    feature_id: str
    remote_session: str
    descriptor: Dict[str, Any]
    targets: Tuple[FeatureTensorPeerTarget, ...]


@dataclass(frozen=True)
class FeatureBundlePeerBufferResult:
    """Observed result of a direct E→P FeatureBundle peer-buffer transfer."""

    feature_id: str
    nbytes: int
    tensor_count: int
    descriptor_count: int
    backend_label: str = "feature_peer_buffer_direct"


def _tensor_raw_bytes(tensor: torch.Tensor) -> bytes:
    contig = tensor.detach().contiguous()
    byte_view = contig.view(torch.uint8).cpu()
    return byte_view.numpy().tobytes()


# ---------------------------------------------------------------------------
# Transfer engine
# ---------------------------------------------------------------------------
class TransferEngine:
    """Protocol-agnostic transport.

    Construction:

        engine = TransferEngine(protocol="local", hw_caps=HwCaps.detect())

    ``protocol="local"`` skips the Mooncake services entirely and uses
    CUDA memcpy between devices -- the right mode for single-host,
    multi-GPU testing. Pass ``protocol="tcp"`` or ``"rdma"`` together
    with a running Mooncake environment to enable cross-node transfers.
    """

    def __init__(
        self,
        protocol: str = "local",
        hw_caps: Optional[HwCaps] = None,
        local_hostname: str = "localhost",
        metadata_server: str = "P2PHANDSHAKE",
        device_name: str = "",
        max_workers: int = 4,
    ):
        self.requested_protocol = str(protocol).strip().lower()
        self.rdma_capabilities: RdmaCapabilities = detect_rdma_capabilities()
        self.protocol = resolve_rdma_protocol(
            self.requested_protocol,
            self.rdma_capabilities,
            same_host=self.requested_protocol == "local",
        )
        self.hw = hw_caps or HwCaps.detect()
        self.local_hostname = local_hostname
        self.metadata_server = metadata_server
        self.device_name = device_name
        self.stats = TransferStats()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._mooncake = None
        self._store = None
        self._rdmacm_client: Optional[RdmaStagedClient] = None
        self._rdmacm_server: Optional[RdmaStagedServer] = None
        self._initialized = False
        self._owns_mooncake_backend = True
        self._registration_lock = threading.RLock()
        self._registrations: Dict[str, _RegistrationRecord] = {}
        self._registration_by_pointer: Dict[int, str] = {}
        self._registration_metrics: Dict[str, int] = {
            "owned_registrations": 0,
            "borrowed_registrations": 0,
            "unregister_calls": 0,
            "illegal_unregisters": 0,
            "registration_leaks_on_shutdown": 0,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def initialize(self) -> None:
        if self._initialized:
            return
        if self.protocol in ("tcp", "rdma"):
            try:
                if self.protocol == "tcp":
                    # Mooncake's transfer engine can auto-discover RDMA HCAs
                    # even when the public protocol parameter is "tcp".  On
                    # hosts with present-but-unusable RDMA devices this makes
                    # direct peer-buffer writes fail at QP creation time
                    # (rc=-1).  `protocol=tcp` is an explicit transport
                    # contract for EPD, so force the C++ engine to install TCP
                    # only before it snapshots environment settings.
                    os.environ.setdefault("MC_FORCE_TCP", "1")
                from mooncake.engine import TransferEngine as _MTE
                mooncake_backend = _MTE()
                mooncake_backend.initialize(
                    self.local_hostname,
                    self.metadata_server,
                    self.protocol,
                    self.device_name,
                )
                self._mooncake = mooncake_backend
            except Exception as e:
                raise RuntimeError(f"Mooncake Transfer Engine init failed: {e}") from e
            if os.getenv("MOONCAKE_EPD_INIT_PYTHON_STORE_ON_ENGINE_INIT", "0").lower() in {
                "1",
                "true",
                "yes",
                "on",
            }:
                self._maybe_initialize_store()
        elif self.protocol == "rdmacm":
            self._rdmacm_client = RdmaStagedClient(
                capabilities=self.rdma_capabilities,
            )
        self._initialized = True

    def shutdown(self) -> None:
        self._shutdown_registrations()
        if self._mooncake is not None and self._owns_mooncake_backend:
            try:
                self._mooncake.shutdown()
            except Exception:
                pass
            self._mooncake = None
        elif self._mooncake is not None:
            self._mooncake = None
        if self._store is not None:
            with contextlib.suppress(Exception):
                self._store.close()
            self._store = None
        if self._rdmacm_server is not None:
            with contextlib.suppress(Exception):
                self._rdmacm_server.close()
            self._rdmacm_server = None
        self._rdmacm_client = None
        self._executor.shutdown(wait=False)
        self._initialized = False
        self._owns_mooncake_backend = True

    def bind_mooncake_backend(
        self,
        backend: Any,
        *,
        initialized: bool = True,
        owns_backend: bool = False,
    ) -> None:
        """Reuse an already-created Mooncake backend instead of creating a new one.

        This is the production path for vLLM integration, where the connector
        already owns a configured/registered Mooncake transfer engine and the
        repo-level `TransferEngine` should act as a protocol-agnostic façade
        over that same backend rather than creating a second engine instance.
        """
        self._mooncake = backend
        self._initialized = bool(initialized)
        self._owns_mooncake_backend = bool(owns_backend)

    def direct_remote_session(self) -> str:
        """Return the Mooncake session string peers use for one-sided writes.

        Mooncake peer-buffer APIs address a remote engine by
        ``<hostname>:<rpc_port>``.  This method is intentionally fail-fast for
        protocols without a real Mooncake backend so direct E→P code cannot
        silently devolve into local/file transport.
        """

        self.initialize()
        if self.protocol == "rdmacm":
            if self._rdmacm_server is None:
                raise RuntimeError("rdmacm server is not started")
            return f"{self._rdmacm_server.bind_address}:{self._rdmacm_server.port}"
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is not initialized")
        if os.getenv("MOONCAKE_EPD_DIRECT_SESSION_INCLUDES_RPC_PORT", "0").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }:
            rpc_port = int(self._mooncake.get_rpc_port())
            if rpc_port <= 0:
                raise RuntimeError(f"Mooncake direct engine returned invalid rpc port: {rpc_port}")
            return f"{self.local_hostname}:{rpc_port}"
        return str(self.local_hostname)

    # ------------------------------------------------------------------
    # rdma_cm/iWARP staged endpoint helpers
    # ------------------------------------------------------------------
    def start_rdmacm_server(
        self,
        *,
        bind_address: str = "",
        port: int,
        regions: Optional[Sequence[RegisteredRegion]] = None,
    ) -> RdmaStagedServer:
        """Start the receiver-side iWARP endpoint.

        The endpoint is only needed on the target worker.  Destination pointer
        ranges must be registered before a producer may write them.
        """

        if self.protocol != "rdmacm":
            raise RuntimeError(
                f"rdmacm server requested for resolved protocol {self.protocol!r}"
            )
        if self._rdmacm_server is not None:
            return self._rdmacm_server
        server = RdmaStagedServer(
            bind_address=(
                bind_address
                or os.getenv("MOONCAKE_EPD_RDMACM_BIND_ADDRESS", "")
                or default_rdma_bind_address(self.rdma_capabilities)
            ),
            port=int(port),
            capabilities=self.rdma_capabilities,
        )
        if regions:
            server.register_regions(regions)
        server.start()
        self._rdmacm_server = server
        self._initialized = True
        return server

    def register_rdmacm_region(
        self,
        base_address: int,
        size_bytes: int,
        *,
        memory_kind: str = "cuda",
    ) -> None:
        if self._rdmacm_server is None:
            raise RuntimeError("rdmacm server is not started")
        self._rdmacm_server.register_region(
            int(base_address),
            int(size_bytes),
            memory_kind=memory_kind,
        )

    # ------------------------------------------------------------------
    # Direct engine peer-buffer helpers
    # ------------------------------------------------------------------
    def allocate_peer_buffer(self, size_bytes: int) -> DirectPeerBuffer:
        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is not initialized")
        ptr = int(self._mooncake.allocate_managed_buffer(int(size_bytes)))
        if ptr <= 0:
            raise RuntimeError(f"allocate_managed_buffer failed for size={size_bytes}")
        return DirectPeerBuffer(pointer=ptr, size_bytes=int(size_bytes), registered=False)

    def free_peer_buffer(self, handle: DirectPeerBuffer) -> None:
        if self._mooncake is None:
            return
        rc = self._mooncake.free_managed_buffer(int(handle.pointer), int(handle.size_bytes))
        if rc != 0:
            raise RuntimeError(f"free_managed_buffer failed: rc={rc}")

    def write_peer_buffer(self, handle: DirectPeerBuffer, payload: bytes) -> None:
        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is not initialized")
        try:
            rc = self._mooncake.write_bytes_to_buffer(
                int(handle.pointer),
                payload,
                int(len(payload)),
            )
        except TypeError:
            rc = self._mooncake.write_bytes_to_buffer(
                int(handle.pointer),
                payload.decode("latin1"),
                int(len(payload)),
            )
        if rc != 0:
            raise RuntimeError(f"write_bytes_to_buffer failed: rc={rc}")

    def read_peer_buffer(self, handle: DirectPeerBuffer, nbytes: int) -> bytes:
        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is not initialized")
        raw = self._mooncake.read_bytes_from_buffer(int(handle.pointer), int(nbytes))
        if isinstance(raw, bytes):
            return raw
        if isinstance(raw, str):
            return raw.encode("latin1")
        return bytes(raw)

    def register_tensor_memory(self, tensor: torch.Tensor) -> DirectPeerBuffer:
        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is not initialized")
        nbytes = int(tensor.nelement() * tensor.element_size())
        ptr = int(tensor.data_ptr())
        with self._registration_lock:
            existing_id = self._registration_by_pointer.get(ptr)
            if existing_id is not None:
                record = self._registrations[existing_id]
                if record.size_bytes != nbytes:
                    raise RuntimeError(
                        "registered pointer size mismatch: "
                        f"ptr={ptr} existing={record.size_bytes} requested={nbytes}"
                    )
                record.references += 1
                self._registration_metrics["borrowed_registrations"] += 1
                return DirectPeerBuffer(
                    pointer=ptr,
                    size_bytes=nbytes,
                    registered=True,
                    registration_id=record.registration_id,
                    borrowed=True,
                    owns_registration=False,
                )

            rc = self._mooncake.register_memory(ptr, nbytes)
            if rc not in (0, -600):
                raise RuntimeError(f"register_memory failed: rc={rc}, ptr={ptr}, nbytes={nbytes}")
            registration_id = uuid.uuid4().hex
            owned = rc == 0
            record = _RegistrationRecord(
                registration_id=registration_id,
                pointer=ptr,
                size_bytes=nbytes,
                owned_by_engine=owned,
            )
            self._registrations[registration_id] = record
            self._registration_by_pointer[ptr] = registration_id
            metric = "owned_registrations" if owned else "borrowed_registrations"
            self._registration_metrics[metric] += 1
            return DirectPeerBuffer(
                pointer=ptr,
                size_bytes=nbytes,
                registered=True,
                registration_id=registration_id,
                borrowed=not owned,
                owns_registration=owned,
            )

    def unregister_tensor_memory(self, handle: DirectPeerBuffer) -> None:
        if not handle.registered or handle.released:
            return
        with self._registration_lock:
            handle.released = True
            registration_id = handle.registration_id
            if registration_id is None:
                # Pointer-only descriptors are declared registered by their
                # external owner. This facade has no unregister authority.
                return
            record = self._registrations.get(registration_id)
            if record is None:
                return
            record.references = max(0, record.references - 1)
            self._maybe_finalize_registration_locked(record)

    def _retain_registration_for_transfer(self, handle: DirectPeerBuffer) -> None:
        registration_id = handle.registration_id
        if registration_id is None:
            return
        with self._registration_lock:
            record = self._registrations.get(registration_id)
            if record is None or handle.released:
                raise RuntimeError(
                    f"registration is not active for transfer: ptr={handle.pointer}"
                )
            record.in_flight += 1

    def _release_registration_from_transfer(self, handle: DirectPeerBuffer) -> None:
        registration_id = handle.registration_id
        if registration_id is None:
            return
        with self._registration_lock:
            record = self._registrations.get(registration_id)
            if record is None:
                return
            record.in_flight = max(0, record.in_flight - 1)
            self._maybe_finalize_registration_locked(record)

    def _maybe_finalize_registration_locked(self, record: _RegistrationRecord) -> None:
        if record.references > 0 or record.in_flight > 0:
            return
        if record.owned_by_engine:
            if self._mooncake is None:
                self._registration_metrics["registration_leaks_on_shutdown"] += 1
                return
            rc = self._mooncake.unregister_memory(int(record.pointer))
            if rc not in (0, -601):
                raise RuntimeError(
                    f"unregister_memory failed: rc={rc}, ptr={record.pointer}"
                )
            self._registration_metrics["unregister_calls"] += 1
        self._registrations.pop(record.registration_id, None)
        self._registration_by_pointer.pop(record.pointer, None)

    def _shutdown_registrations(self) -> None:
        with self._registration_lock:
            for record in list(self._registrations.values()):
                if record.in_flight > 0:
                    self._registration_metrics["registration_leaks_on_shutdown"] += 1
                    continue
                record.references = 0
                self._maybe_finalize_registration_locked(record)

    def registration_stats(self) -> Dict[str, int]:
        with self._registration_lock:
            out = dict(self._registration_metrics)
            out["active_registrations"] = len(self._registrations)
            out["active_registration_refs"] = sum(
                record.references for record in self._registrations.values()
            )
            out["inflight_registrations"] = sum(
                1 for record in self._registrations.values() if record.in_flight > 0
            )
            out["borrowed_active_registrations"] = sum(
                1 for record in self._registrations.values() if not record.owned_by_engine
            )
            return out

    def build_peer_transfer_plan(
        self,
        *,
        tensors: Sequence[torch.Tensor],
        remote_session: str,
        remote_pointers: Sequence[int],
        mirror_tensors: Optional[Sequence[Optional[torch.Tensor]]] = None,
        mirror_local_copy: bool = True,
        target_device: Optional[torch.device] = None,
    ) -> PeerTransferPlan:
        if len(tensors) != len(remote_pointers):
            raise ValueError("tensors and remote_pointers must have identical lengths")
        if mirror_tensors is not None and len(mirror_tensors) != len(tensors):
            raise ValueError("mirror_tensors must match tensors length")
        descriptors: List[PeerTransferDescriptor] = []
        for idx, (tensor, remote_pointer) in enumerate(zip(tensors, remote_pointers)):
            nbytes = int(tensor.nelement() * tensor.element_size())
            descriptors.append(
                PeerTransferDescriptor(
                    local_pointer=int(tensor.data_ptr()),
                    remote_pointer=int(remote_pointer),
                    size_bytes=nbytes,
                    tensor=tensor,
                    mirror_tensor=(
                        None if mirror_tensors is None else mirror_tensors[idx]
                    ),
                )
            )
        return PeerTransferPlan(
            remote_session=str(remote_session),
            descriptors=descriptors,
            mirror_local_copy=bool(mirror_local_copy),
            target_device=target_device,
        )

    def build_pointer_transfer_plan(
        self,
        *,
        remote_session: str,
        local_pointers: Sequence[int],
        remote_pointers: Sequence[int],
        lengths: Sequence[int],
        registered: bool = True,
    ) -> PeerTransferPlan:
        if not (len(local_pointers) == len(remote_pointers) == len(lengths)):
            raise ValueError(
                "local_pointers, remote_pointers and lengths must have identical lengths"
            )
        descriptors: List[PeerTransferDescriptor] = []
        for local_ptr, remote_ptr, length in zip(local_pointers, remote_pointers, lengths):
            length = int(length)
            descriptors.append(
                PeerTransferDescriptor(
                    local_pointer=int(local_ptr),
                    remote_pointer=int(remote_ptr),
                    size_bytes=length,
                    local_buffer=DirectPeerBuffer(
                        pointer=int(local_ptr),
                        size_bytes=length,
                        registered=bool(registered),
                    ),
                )
            )
        return PeerTransferPlan(
            remote_session=str(remote_session),
            descriptors=descriptors,
            mirror_local_copy=False,
            target_device=None,
        )

    def transfer_registered_descriptors(
        self,
        plan: PeerTransferPlan,
    ) -> PeerTransferResult:
        self.initialize()
        if self.protocol != "rdmacm" and self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is unavailable")
        if not plan.descriptors:
            return PeerTransferResult(nbytes=0, descriptor_count=0)

        cleanup_handles: List[DirectPeerBuffer] = []
        transfer_handles: List[DirectPeerBuffer] = []
        local_ptrs: List[int] = []
        remote_ptrs: List[int] = []
        lengths: List[int] = []
        mirrored: List[Optional[torch.Tensor]] = []
        total_bytes = 0

        for desc in plan.descriptors:
            try:
                if desc.local_buffer is not None:
                    if not desc.local_buffer.registered and desc.tensor is None:
                        raise ValueError(
                            "pointer-only peer-buffer descriptors must reference registered memory"
                        )
                    local_ptr = int(desc.local_buffer.pointer)
                    if desc.local_buffer.registration_id is not None:
                        self._retain_registration_for_transfer(desc.local_buffer)
                        transfer_handles.append(desc.local_buffer)
                elif desc.local_pointer:
                    local_ptr = int(desc.local_pointer)
                elif desc.tensor is not None:
                    handle = self.register_tensor_memory(desc.tensor.detach())
                    cleanup_handles.append(handle)
                    self._retain_registration_for_transfer(handle)
                    transfer_handles.append(handle)
                    local_ptr = int(handle.pointer)
                    desc.needs_unregister = True
                else:
                    raise ValueError("descriptor must provide tensor, local_pointer or local_buffer")

                if (
                    desc.tensor is not None
                    and desc.local_buffer is None
                    and not desc.needs_unregister
                ):
                    handle = self.register_tensor_memory(desc.tensor.detach())
                    cleanup_handles.append(handle)
                    self._retain_registration_for_transfer(handle)
                    transfer_handles.append(handle)
                    local_ptr = int(handle.pointer)
                    desc.needs_unregister = True

                local_ptrs.append(local_ptr)
                remote_ptrs.append(int(desc.remote_pointer))
                lengths.append(int(desc.size_bytes))
                total_bytes += int(desc.size_bytes)
            except BaseException:
                for handle in transfer_handles:
                    with contextlib.suppress(Exception):
                        self._release_registration_from_transfer(handle)
                for handle in cleanup_handles:
                    with contextlib.suppress(Exception):
                        self.unregister_tensor_memory(handle)
                raise

        try:
            if self.protocol == "rdmacm":
                if self._rdmacm_client is None:
                    raise RuntimeError("rdmacm client is unavailable")
                host, separator, raw_port = str(plan.remote_session).rpartition(":")
                if not separator or not host:
                    raise ValueError(
                        "rdmacm remote_session must be '<IPv4-address>:<port>'"
                    )
                self._rdmacm_client.write_descriptors(
                    remote_address=host,
                    remote_port=int(raw_port),
                    source_pointers=local_ptrs,
                    destination_pointers=remote_ptrs,
                    lengths=lengths,
                    source_memory="cuda",
                    destination_memory="cuda",
                )
            else:
                assert self._mooncake is not None
                if len(local_ptrs) == 1:
                    rc = self._mooncake.transfer_sync_write(
                        str(plan.remote_session),
                        int(local_ptrs[0]),
                        int(remote_ptrs[0]),
                        int(lengths[0]),
                    )
                else:
                    rc = self._mooncake.batch_transfer_sync_write(
                        str(plan.remote_session),
                        local_ptrs,
                        remote_ptrs,
                        lengths,
                    )
                if rc != 0:
                    raise RuntimeError(f"peer-buffer transfer failed: rc={rc}")
        finally:
            for handle in transfer_handles:
                with contextlib.suppress(Exception):
                    self._release_registration_from_transfer(handle)
            for handle in cleanup_handles:
                with contextlib.suppress(Exception):
                    self.unregister_tensor_memory(handle)

        target_device = plan.target_device
        for desc in plan.descriptors:
            if isinstance(desc.mirror_tensor, torch.Tensor):
                mirrored.append(
                    desc.mirror_tensor.to(target_device or desc.mirror_tensor.device, copy=True)
                )
            elif plan.mirror_local_copy and isinstance(desc.tensor, torch.Tensor):
                mirrored.append(
                    desc.tensor.to(target_device or desc.tensor.device, copy=True)
                )
            else:
                mirrored.append(None)

        return PeerTransferResult(
            nbytes=total_bytes,
            descriptor_count=len(plan.descriptors),
            mirrored_tensors=mirrored,
        )

    def transfer_peer_buffer_plan(self, plan: PeerTransferPlan) -> PeerTransferResult:
        return self.transfer_registered_descriptors(plan)

    def read_remote_peer_buffers(
        self,
        *,
        remote_session: str,
        remote_pointers: Sequence[int],
        lengths: Sequence[int],
    ) -> List[bytes]:
        """Read remote Mooncake peer buffers into local managed buffers.

        This is the receive-side counterpart to ``transfer_peer_buffer_plan``:
        it still uses Mooncake's direct engine data plane, but the caller only
        receives materialized byte payloads.  It is used by vLLM EngineCore
        processes to consume direct E→P FeatureHandles whose target buffers were
        allocated by the colocated API process.
        """

        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is unavailable")
        if not remote_session:
            raise ValueError("remote_session is required for peer-buffer read")
        if len(remote_pointers) != len(lengths):
            raise ValueError("remote_pointers and lengths must have identical lengths")
        handles: List[DirectPeerBuffer] = []
        local_pointers: List[int] = []
        read_lengths: List[int] = []
        try:
            for length in lengths:
                nbytes = int(length)
                if nbytes < 0:
                    raise ValueError(f"negative peer-buffer read length: {nbytes}")
                handle = self.allocate_peer_buffer(max(1, nbytes))
                handles.append(handle)
                local_pointers.append(int(handle.pointer))
                read_lengths.append(nbytes)
            if not handles:
                return []
            if len(handles) == 1:
                rc = self._mooncake.transfer_sync_read(
                    str(remote_session),
                    int(local_pointers[0]),
                    int(remote_pointers[0]),
                    int(read_lengths[0]),
                )
            else:
                rc = self._mooncake.batch_transfer_sync_read(
                    str(remote_session),
                    local_pointers,
                    [int(ptr) for ptr in remote_pointers],
                    read_lengths,
                )
            if rc != 0:
                raise RuntimeError(f"peer-buffer read failed: rc={rc}")
            return [
                self.read_peer_buffer(handle, nbytes)
                for handle, nbytes in zip(handles, read_lengths)
            ]
        finally:
            for handle in handles:
                with contextlib.suppress(Exception):
                    self.free_peer_buffer(handle)

    def read_remote_peer_buffers_into_tensors(
        self,
        *,
        remote_session: str,
        remote_pointers: Sequence[int],
        tensors: Sequence[torch.Tensor],
    ) -> Dict[str, float]:
        """Read peer buffers directly into caller-owned contiguous tensors.

        The receive tensor is the final materialization target.  Compared with
        :meth:`read_remote_peer_buffers`, this avoids a managed-buffer staging
        allocation, Python ``bytes`` creation, and a subsequent CPU-to-device
        copy.  Registration ownership is delegated to the existing reference-
        counted registration layer, so buffers already registered by another
        component are borrowed and never unregistered by this call.
        """

        total_started = time.perf_counter()
        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is unavailable")
        if not remote_session:
            raise ValueError("remote_session is required for peer-buffer read")
        if len(remote_pointers) != len(tensors):
            raise ValueError("remote_pointers and tensors must have identical lengths")

        handles: List[DirectPeerBuffer] = []
        local_pointers: List[int] = []
        lengths: List[int] = []
        register_ms = 0.0
        transfer_ms = 0.0
        visibility_sync_ms = 0.0
        unregister_ms = 0.0
        completed = False
        try:
            for tensor in tensors:
                if not isinstance(tensor, torch.Tensor):
                    raise TypeError("direct peer-buffer read targets must be torch.Tensor instances")
                if not tensor.is_contiguous():
                    raise ValueError("direct peer-buffer read target tensors must be contiguous")
                register_started = time.perf_counter()
                handle = self.register_tensor_memory(tensor)
                register_ms += (time.perf_counter() - register_started) * 1000.0
                handles.append(handle)
                local_pointers.append(int(handle.pointer))
                lengths.append(int(handle.size_bytes))

            if not handles:
                completed = True
                return {
                    "total_ms": (time.perf_counter() - total_started) * 1000.0,
                    "register_memory_ms": 0.0,
                    "read_ms": 0.0,
                    "visibility_sync_ms": 0.0,
                    "unregister_memory_ms": 0.0,
                    "descriptor_count": 0.0,
                    "nbytes": 0.0,
                }

            transfer_started = time.perf_counter()
            if len(handles) == 1:
                rc = self._mooncake.transfer_sync_read(
                    str(remote_session),
                    int(local_pointers[0]),
                    int(remote_pointers[0]),
                    int(lengths[0]),
                )
            else:
                rc = self._mooncake.batch_transfer_sync_read(
                    str(remote_session),
                    local_pointers,
                    [int(ptr) for ptr in remote_pointers],
                    lengths,
                )
            transfer_ms = (time.perf_counter() - transfer_started) * 1000.0
            if rc != 0:
                raise RuntimeError(f"peer-buffer read failed: rc={rc}")
            # Mooncake's synchronous return establishes transport completion,
            # but an external DMA into CUDA memory is not associated with a
            # PyTorch stream. Establish device visibility before the target
            # tensors are consumed or their registrations are released.
            cuda_devices = {
                tensor.device
                for tensor in tensors
                if isinstance(tensor, torch.Tensor) and tensor.device.type == "cuda"
            }
            if cuda_devices:
                sync_started = time.perf_counter()
                for device in sorted(cuda_devices, key=str):
                    torch.cuda.synchronize(device)
                visibility_sync_ms = (time.perf_counter() - sync_started) * 1000.0
            completed = True
        finally:
            for handle in handles:
                with contextlib.suppress(Exception):
                    unregister_started = time.perf_counter()
                    self.unregister_tensor_memory(handle)
                    unregister_ms += (time.perf_counter() - unregister_started) * 1000.0

        if not completed:
            raise RuntimeError("peer-buffer read did not complete")
        return {
            "total_ms": (time.perf_counter() - total_started) * 1000.0,
            "register_memory_ms": register_ms,
            "read_ms": transfer_ms,
            "visibility_sync_ms": visibility_sync_ms,
            "unregister_memory_ms": unregister_ms,
            "descriptor_count": float(len(handles)),
            "nbytes": float(sum(lengths)),
        }

    # ------------------------------------------------------------------
    # E→P FeatureBundle direct peer-buffer helpers
    # ------------------------------------------------------------------
    @staticmethod
    def feature_bundle_tensor_items(bundle: Any) -> List[Tuple[str, torch.Tensor]]:
        """Return FeatureBundle tensors in a stable direct-transfer order.

        Names are part of the control-plane ABI.  A receiver that allocates
        buffers for ``last_hidden``, optional ``grid_thw``, and each
        ``intermediate:<layer>:<ordinal>`` can pass the pointer map to
        ``build_feature_bundle_peer_buffer_plan`` and receive bytes without a
        filesystem/object-store round trip.
        """

        items: List[Tuple[str, torch.Tensor]] = [("last_hidden", bundle.last_hidden)]
        grid = getattr(bundle, "grid_thw", None)
        if grid is not None:
            items.append(("grid_thw", grid))
        for ordinal, (layer, tensor) in enumerate(list(getattr(bundle, "intermediates", []) or [])):
            items.append((f"intermediate:{int(layer)}:{ordinal}", tensor))
        return items

    def build_feature_bundle_peer_buffer_plan(
        self,
        bundle: Any,
        *,
        remote_session: str,
        remote_pointers: Dict[str, int],
        checksum: bool = False,
    ) -> FeatureBundlePeerBufferPlan:
        """Build a fail-fast E→P direct transfer plan for a FeatureBundle.

        ``remote_pointers`` must contain one registered prefill-owned buffer per
        tensor name. Optional ``"<name>:nbytes"`` entries let the sender validate
        receiver capacity before data movement. The method never falls back to
        file/Mooncake Store transport when the direct plan is incomplete.
        """

        if not remote_session:
            raise ValueError("remote_session is required for FeatureBundle peer-buffer transfer")
        if not remote_pointers:
            raise ValueError("remote_pointers is required for FeatureBundle peer-buffer transfer")
        try:
            descriptor = bundle.descriptor(checksum=checksum).to_dict()
            feature_id = str(bundle.image_hash)
        except Exception as exc:
            raise TypeError("bundle must be a FeatureBundle-like object with descriptor()") from exc

        targets: List[FeatureTensorPeerTarget] = []
        missing: List[str] = []
        undersized: List[str] = []
        for name, tensor in self.feature_bundle_tensor_items(bundle):
            if name not in remote_pointers:
                missing.append(name)
                continue
            nbytes = int(tensor.nelement() * tensor.element_size())
            capacity = remote_pointers.get(f"{name}:nbytes")
            if capacity is not None and int(capacity) < nbytes:
                undersized.append(f"{name} capacity={capacity} required={nbytes}")
            targets.append(
                FeatureTensorPeerTarget(
                    name=name,
                    remote_pointer=int(remote_pointers[name]),
                    nbytes=nbytes,
                )
            )
        if missing:
            raise ValueError(f"missing FeatureBundle peer-buffer targets: {missing}")
        if undersized:
            raise ValueError(f"undersized FeatureBundle peer-buffer targets: {undersized}")
        return FeatureBundlePeerBufferPlan(
            feature_id=feature_id,
            remote_session=str(remote_session),
            descriptor=descriptor,
            targets=tuple(targets),
        )

    def transfer_feature_bundle_peer_buffer_plan(
        self,
        bundle: Any,
        plan: FeatureBundlePeerBufferPlan,
        *,
        source_memory_mode: Optional[str] = None,
    ) -> FeatureBundlePeerBufferResult:
        """Transfer FeatureBundle tensors through Mooncake direct peer buffers.

        ``source_memory_mode`` controls the sender-side memory contract:
        - ``registered_tensor``: use tensor.data_ptr() directly; caller/platform
          must ensure the tensor memory is registered/registrable.
        - ``managed_buffer``: stage each tensor into a local Mooncake managed
          buffer, then write managed-buffer pointers to the remote targets.  This
          remains a direct-engine data plane and avoids file/object-store
          fallback on platforms where torch CUDA allocations cannot be
          registered by Mooncake.
        """

        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("FeatureBundle direct peer-buffer transfer requires a real Mooncake engine")
        mode = str(
            source_memory_mode
            or os.getenv("MOONCAKE_EPD_DIRECT_SOURCE_MODE", "registered_tensor")
        ).lower()
        if mode not in {"registered_tensor", "managed_buffer"}:
            raise ValueError(f"unsupported FeatureBundle direct source_memory_mode: {mode}")

        tensors_by_name = {name: tensor for name, tensor in self.feature_bundle_tensor_items(bundle)}
        local_pointers: List[int] = []
        remote_pointers: List[int] = []
        lengths: List[int] = []
        staged_buffers: List[DirectPeerBuffer] = []
        for target in plan.targets:
            tensor = tensors_by_name.get(target.name)
            if tensor is None:
                raise ValueError(f"plan references tensor not present in bundle: {target.name}")
            nbytes = int(tensor.nelement() * tensor.element_size())
            if nbytes != int(target.nbytes):
                raise ValueError(
                    f"tensor byte size changed before transfer: {target.name} "
                    f"plan={target.nbytes} actual={nbytes}"
                )
            if mode == "managed_buffer":
                handle = self.allocate_peer_buffer(nbytes)
                staged_buffers.append(handle)
                self.write_peer_buffer(handle, _tensor_raw_bytes(tensor))
                local_pointers.append(int(handle.pointer))
            else:
                local_pointers.append(int(tensor.data_ptr()))
            remote_pointers.append(int(target.remote_pointer))
            lengths.append(nbytes)

        started = time.perf_counter()
        try:
            pointer_plan = self.build_pointer_transfer_plan(
                remote_session=plan.remote_session,
                local_pointers=local_pointers,
                remote_pointers=remote_pointers,
                lengths=lengths,
                registered=True,
            )
            result = self.transfer_peer_buffer_plan(pointer_plan)
        finally:
            for handle in staged_buffers:
                with contextlib.suppress(Exception):
                    self.free_peer_buffer(handle)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        self.stats.record("encoder_to_prefill_peer_buffer_direct", result.nbytes, elapsed_ms)
        return FeatureBundlePeerBufferResult(
            feature_id=plan.feature_id,
            nbytes=int(result.nbytes),
            tensor_count=len(plan.targets),
            descriptor_count=int(result.descriptor_count),
        )

    def probe_direct_engine(self, buffer_bytes: int = 4096) -> Dict[str, Any]:
        """Best-effort smoke probe for the Mooncake direct-engine peer-buffer path.

        This intentionally exercises the real C-extension path (initialize ->
        allocate managed buffer -> write -> read -> free). Callers should run
        it in a subprocess when the local Mooncake build is known to be
        unstable, so crashes do not take down the main test runner.
        """
        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is unavailable")
        topology = self._mooncake.get_local_topology(self.device_name or "")
        payload = b"mooncake-direct-engine-ok"
        handle = self.allocate_peer_buffer(max(int(buffer_bytes), len(payload)))
        try:
            self.write_peer_buffer(handle, payload)
            echoed = self.read_peer_buffer(handle, len(payload))
        finally:
            self.free_peer_buffer(handle)
        return {
            "ok": echoed == payload,
            "rpc_port": int(self._mooncake.get_rpc_port()),
            "buffer_bytes": int(buffer_bytes),
            "payload_len": len(payload),
            "topology": topology,
            "echoed": echoed.decode("latin1"),
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def transfer_tensor(
        self,
        tensor: torch.Tensor,
        target_device: torch.device,
        policy: Optional[TransferPolicy] = None,
    ) -> torch.Tensor:
        policy = policy or TransferPolicy(Mode.STREAM)
        start = time.perf_counter()
        nbytes = tensor.nelement() * tensor.element_size()

        # Precision conversion
        payload = _cast_precision(tensor, policy.precision)

        # CacheGen compression: quantize -> move -> dequantize. Useful on
        # bandwidth-bound P->D links; the compression itself is fast.
        if policy.compress is CompressMode.CACHEGEN and tensor.device != target_device:
            compressed, meta = _compress_cachegen(payload)
            # In local mode, simulate the "wire transfer" by a CPU round-trip.
            # Real deployments would ship `compressed` over TCP/RDMA.
            reconstructed = _decompress_cachegen(compressed, meta).to(target_device)
            result = reconstructed
        elif self.protocol == "local" or (tensor.device == target_device):
            copy_flag = bool(getattr(policy, "extra", {}).get("force_copy", False))
            result = payload.to(
                target_device,
                copy=(tensor.device != target_device) or copy_flag,
            )
        else:
            result = self._remote_transfer(payload, target_device, policy)

        # Cast back to original dtype so callers see an unchanged tensor type
        if result.dtype != tensor.dtype:
            result = result.to(tensor.dtype)
        elapsed_ms = (time.perf_counter() - start) * 1000
        ch = policy.channel.value if policy.channel else "tensor"
        self.stats.record(ch, nbytes, elapsed_ms)
        return result

    def transfer_pages(
        self,
        page_manager,
        refs: Sequence,
        target_device: torch.device,
        policy: Optional[TransferPolicy] = None,
    ) -> Tuple:
        """Move a list of BlockRefs to a target device.

        Returns ``(target_page_manager, new_refs)``. If the target device
        matches the source manager's device, the source manager is reused.
        """
        from ..state.page_manager import PagedKVManager
        policy = policy or TransferPolicy(Mode.STREAM, channel=Channel.PREFILL_TO_DECODE)
        if policy.extra.get("layered", False):
            return self.transfer_pages_layered(
                page_manager,
                refs,
                target_device,
                policy=policy,
            )
        reuse_source = (
            page_manager.device == target_device
            and not bool(policy.extra.get("force_new_manager", False))
        )
        if reuse_source and not bool(policy.extra.get("force_copy", False)):
            start = time.perf_counter()
            forked_refs = page_manager.fork_refs(refs)
            total_bytes = 0
            for ref in refs:
                key, value = page_manager.get_page_slice(ref)
                total_bytes += key.nelement() * key.element_size() + value.nelement() * value.element_size()
            self.stats.record("pages_zero_copy", total_bytes, (time.perf_counter() - start) * 1000)
            return page_manager, forked_refs
        if reuse_source:
            target_mgr = page_manager
        else:
            target_mgr = PagedKVManager(
                page_size=page_manager.page_size,
                num_layers=page_manager.num_layers,
                num_kv_heads=page_manager.num_kv_heads,
                head_dim=page_manager.head_dim,
                dtype=page_manager.dtype,
                device=target_device,
            )

        new_refs = []
        total_bytes = 0
        start = time.perf_counter()
        for ref in refs:
            k, v = page_manager.get_page(ref)
            k_t = self.transfer_tensor(k, target_device, policy)
            v_t = self.transfer_tensor(v, target_device, policy)
            new_ref = target_mgr.allocate_page(filled=ref.filled)
            target_mgr.write_page_slots(new_ref, k_t, v_t, offset=0)
            new_refs.append(new_ref)
            total_bytes += k.nelement() * k.element_size() + v.nelement() * v.element_size()
        elapsed_ms = (time.perf_counter() - start) * 1000
        self.stats.record("pages", total_bytes, elapsed_ms)
        return target_mgr, new_refs

    def transfer_pages_layered(
        self,
        page_manager,
        refs: Sequence,
        target_device: torch.device,
        policy: Optional[TransferPolicy] = None,
    ) -> Tuple:
        """Transfer P→D KV pages in layer groups instead of page-monolithic copies."""
        from ..state.page_manager import BlockRef, PagedKVManager

        policy = policy or TransferPolicy(Mode.STREAM, channel=Channel.PREFILL_TO_DECODE)
        layers_per_group = int(policy.extra.get("layers_per_group", 4))
        if layers_per_group <= 0:
            raise ValueError("layers_per_group must be positive")
        group_delay_ms = float(policy.extra.get("group_delay_ms", 0.0))
        target_mgr = PagedKVManager(
            page_size=page_manager.page_size,
            num_layers=page_manager.num_layers,
            num_kv_heads=page_manager.num_kv_heads,
            head_dim=page_manager.head_dim,
            dtype=page_manager.dtype,
            device=target_device,
            node_id=f"{page_manager.node_id}-layered-target",
        )

        new_refs: List[BlockRef] = []
        layer_batches: List[LayerTransferBatch] = []
        total_bytes = 0
        transfer_start = time.perf_counter()
        for page_index, ref in enumerate(refs):
            key, value = page_manager.get_page(ref)
            target_ref = target_mgr.allocate_page(filled=ref.filled)
            for layer_start in range(0, page_manager.num_layers, layers_per_group):
                layer_stop = min(page_manager.num_layers, layer_start + layers_per_group)
                batch_start = time.perf_counter()
                key_slice = key[layer_start:layer_stop, :, : ref.filled, :]
                value_slice = value[layer_start:layer_stop, :, : ref.filled, :]
                moved_key = self.transfer_tensor(key_slice, target_device, policy)
                moved_value = self.transfer_tensor(value_slice, target_device, policy)
                target_mgr.write_page_layer_slots(
                    target_ref,
                    moved_key,
                    moved_value,
                    layer_start=layer_start,
                    offset=0,
                )
                batch_ms = (time.perf_counter() - batch_start) * 1000
                batch_bytes = (
                    key_slice.nelement() * key_slice.element_size()
                    + value_slice.nelement() * value_slice.element_size()
                )
                total_bytes += batch_bytes
                layer_batches.append(
                    LayerTransferBatch(
                        page_index=page_index,
                        layer_start=layer_start,
                        layer_stop=layer_stop,
                        token_count=ref.filled,
                        bytes_transferred=batch_bytes,
                        transfer_time_ms=batch_ms,
                    )
                )
                if group_delay_ms > 0:
                    time.sleep(group_delay_ms / 1000.0)
            new_refs.append(
                BlockRef(
                    physical_id=target_ref.physical_id,
                    filled=ref.filled,
                    global_block_id=target_ref.global_block_id,
                    physical_node_id=target_ref.physical_node_id,
                    logical_index=page_index,
                    virtual_offset=ref.virtual_offset,
                )
            )

        total_ms = (time.perf_counter() - transfer_start) * 1000
        self.stats.record("pages_layered", total_bytes, total_ms)
        self.stats.record(
            policy.channel.value if policy.channel else "pages_layered",
            total_bytes,
            total_ms,
        )
        setattr(target_mgr, "_last_layer_batches", layer_batches)
        return target_mgr, new_refs

    def transfer_feature_bundle(
        self,
        bundle,
        target_device: torch.device,
        policy: Optional[TransferPolicy] = None,
    ):
        """Transfer a FeatureBundle. ``per_level`` compresses deeper layers harder."""
        from ..state.feature_store import FeatureBundle
        policy = policy or TransferPolicy(Mode.SHM, channel=Channel.ENCODER_TO_PREFILL)
        start = time.perf_counter()
        total_bytes = bundle.nbytes()

        if policy.compress is CompressMode.PER_LEVEL:
            intermediates = _compress_per_level(bundle.intermediates, policy.precision)
        else:
            intermediates = [
                (idx, _cast_precision(t, policy.precision))
                for idx, t in bundle.intermediates
            ]

        last_hidden = self.transfer_tensor(bundle.last_hidden, target_device, policy)
        grid_thw = (
            self.transfer_tensor(bundle.grid_thw, target_device, policy)
            if bundle.grid_thw is not None
            else None
        )
        # Route every DeepStack intermediate through the same TransferEngine
        # path as last_hidden. This preserves remote protocol accounting and
        # prevents E→P from silently becoming a local `.to()` for intermediates.
        restored = []
        for idx, t in intermediates:
            moved = self.transfer_tensor(t, target_device, policy)
            restored.append((idx, moved))
        elapsed_ms = (time.perf_counter() - start) * 1000
        self.stats.record(
            policy.channel.value if policy.channel else "feature",
            total_bytes, elapsed_ms,
        )
        return FeatureBundle(
            image_hash=bundle.image_hash,
            last_hidden=last_hidden,
            intermediates=restored,
            grid_thw=grid_thw,
            metadata=bundle.metadata,
        )

    # ------------------------------------------------------------------
    # Async primitive
    # ------------------------------------------------------------------
    def transfer_async(
        self,
        refs,
        policy: TransferPolicy,
        target: torch.device,
    ) -> TransferHandle:
        """Submit an async transfer and return a handle to await."""
        fut = self._executor.submit(self._dispatch_sync, refs, policy, target)
        return TransferHandle(_future=fut, channel=policy.channel.value if policy.channel else "async")

    def _dispatch_sync(self, refs, policy, target):
        # Route based on mode
        if policy.mode is Mode.SHM:
            return [self.transfer_tensor(r, target, policy) for r in refs]
        return [self.transfer_tensor(r, target, policy) for r in refs]

    # ------------------------------------------------------------------
    # Remote transfer (Mooncake path)
    # ------------------------------------------------------------------
    def _remote_transfer(
        self,
        tensor: torch.Tensor,
        target_device: torch.device,
        policy: Optional[TransferPolicy] = None,
    ) -> torch.Tensor:
        policy = policy or TransferPolicy(Mode.STREAM)
        extra = getattr(policy, "extra", {}) or {}
        if str(extra.get("transport_backend", "")).lower() in {
            "engine_direct",
            "mooncake_engine_direct",
            "direct_engine",
        }:
            if not self._initialized:
                self.initialize()
            if self._mooncake is not None:
                return self._remote_transfer_via_engine_buffer(
                    tensor=tensor,
                    target_device=target_device,
                    policy=policy,
                )
        store_url = self._resolve_store_url(policy)
        if store_url:
            return self._remote_transfer_via_store_http(
                tensor=tensor,
                target_device=target_device,
                policy=policy,
                store_url=store_url,
            )
        if self._store is None and os.getenv("MOONCAKE_EPD_ENABLE_PYTHON_STORE_TRANSFER", "0").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }:
            self._maybe_initialize_store()
        if self._store is not None:
            return self._remote_transfer_via_python_store(
                tensor=tensor,
                target_device=target_device,
                policy=policy,
            )
        if self._mooncake is None:
            if strict_no_fallback_enabled(extra):
                raise RuntimeError(
                    "strict no-fallback remote transfer requires a real Mooncake "
                    "backend or Mooncake Store; no backend was configured"
                )
            # Compatibility mode for local developer paths only.
            return tensor.to(target_device, copy=True)
        raise NotImplementedError(
            "Cross-node RDMA/TCP transfer path requires running mooncake_master + store; "
            "see scripts/start_mooncake.sh. Local-mode transfers use tensor.to()."
        )

    def _maybe_initialize_store(self) -> None:
        if self._store is not None:
            return
        try:
            from mooncake.store import MooncakeDistributedStore
        except Exception:
            return
        config = self._load_store_config()
        if config is None:
            return
        store = MooncakeDistributedStore()
        rc = store.setup(config)
        if rc == 0:
            self._store = store

    def _load_store_config(self) -> Optional[dict]:
        cfg_path = os.getenv("MOONCAKE_CONFIG_PATH")
        if cfg_path and os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data
        metadata_server = os.getenv("MOONCAKE_TE_META_DATA_SERVER")
        master_server = os.getenv("MOONCAKE_MASTER")
        local_hostname = os.getenv("MOONCAKE_LOCAL_HOSTNAME", self.local_hostname)
        if not metadata_server or not master_server:
            return None
        return {
            "local_hostname": local_hostname,
            "metadata_server": metadata_server,
            "global_segment_size": int(os.getenv("MOONCAKE_GLOBAL_SEGMENT_SIZE", str(16 * 1024 * 1024))),
            "local_buffer_size": int(os.getenv("MOONCAKE_LOCAL_BUFFER_SIZE", str(16 * 1024 * 1024))),
            "protocol": os.getenv("MOONCAKE_PROTOCOL", self.protocol),
            "device_name": self.device_name,
            "master_server_address": master_server,
        }

    def _resolve_store_url(self, policy: TransferPolicy) -> Optional[str]:
        extra = getattr(policy, "extra", {}) or {}
        url = extra.get("store_url") or os.getenv("MOONCAKE_STORE_URL")
        if not url:
            return None
        return str(url).rstrip("/")

    @staticmethod
    def _http_session() -> requests.Session:
        session = requests.Session()
        session.trust_env = False
        return session

    @staticmethod
    def _serialize_tensor_to_ascii_payload(tensor: torch.Tensor) -> str:
        buf = io.BytesIO()
        torch.save(tensor.detach().cpu(), buf)
        return base64.b64encode(buf.getvalue()).decode("ascii")

    @staticmethod
    def _deserialize_tensor_from_ascii_payload(payload: bytes) -> torch.Tensor:
        raw = base64.b64decode(payload)
        buf = io.BytesIO(raw)
        try:
            return torch.load(buf, map_location="cpu", weights_only=True)
        except TypeError:
            return torch.load(buf, map_location="cpu")

    def _remote_transfer_via_store_http(
        self,
        tensor: torch.Tensor,
        target_device: torch.device,
        policy: TransferPolicy,
        store_url: str,
    ) -> torch.Tensor:
        extra = getattr(policy, "extra", {}) or {}
        key = extra.get("store_key")
        if not key:
            key = (
                f"epd-transfer-"
                f"{policy.channel.value if policy.channel else 'tensor'}-"
                f"{uuid.uuid4().hex}"
            )
        key = str(key).replace("/", "__")
        cleanup = bool(extra.get("store_cleanup", False))
        timeout = float(extra.get("timeout_seconds", 30.0))
        payload = self._serialize_tensor_to_ascii_payload(tensor)

        session = self._http_session()
        put_resp = session.put(
            f"{store_url}/api/put",
            json={"key": key, "value": payload},
            timeout=timeout,
        )
        put_resp.raise_for_status()

        get_resp = session.get(
            f"{store_url}/api/get/{key}",
            timeout=timeout,
        )
        get_resp.raise_for_status()
        restored = self._deserialize_tensor_from_ascii_payload(get_resp.content)

        if cleanup:
            with contextlib.suppress(Exception):
                session.delete(f"{store_url}/api/remove/{key}", timeout=timeout)

        return restored.to(target_device, copy=True)

    def _remote_transfer_via_python_store(
        self,
        tensor: torch.Tensor,
        target_device: torch.device,
        policy: TransferPolicy,
    ) -> torch.Tensor:
        extra = getattr(policy, "extra", {}) or {}
        key = extra.get("store_key")
        if not key:
            key = (
                f"epd-transfer-"
                f"{policy.channel.value if policy.channel else 'tensor'}-"
                f"{uuid.uuid4().hex}"
            )
        key = str(key).replace("/", "__")
        cleanup = bool(extra.get("store_cleanup", True))
        store = self._store
        if store is None:
            raise RuntimeError("Mooncake Python store is unavailable")
        rc = store.put_tensor(key, tensor.detach())
        if rc != 0:
            raise RuntimeError(f"MooncakeDistributedStore.put_tensor failed: rc={rc}")
        restored = store.get_tensor(key)
        if restored is None:
            raise RuntimeError(f"MooncakeDistributedStore.get_tensor returned None for key={key}")
        if cleanup:
            with contextlib.suppress(Exception):
                store.remove(key, True)
        return restored.to(target_device, copy=True)

    def _remote_transfer_via_engine_buffer(
        self,
        tensor: torch.Tensor,
        target_device: torch.device,
        policy: TransferPolicy,
    ) -> torch.Tensor:
        """Direct-engine remote write with optional local materialization shim.

        The real zero-copy effect happens in the Mooncake one-sided write:
        source memory is registered, then written into a peer buffer described
        by ``remote_session`` + ``peer_buffer_addr``. Because the repo-level
        ``transfer_tensor`` API expects a materialized tensor return value, this
        helper optionally mirrors the result with a local copy unless the caller
        provides an already-materialized destination tensor via
        ``materialized_result``.
        """
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is unavailable")
        extra = getattr(policy, "extra", {}) or {}
        remote_session = extra.get("remote_session")
        peer_buffer_addr = extra.get("peer_buffer_addr")
        if not remote_session or peer_buffer_addr is None:
            raise RuntimeError(
                "engine_direct backend requires policy.extra['remote_session'] "
                "and policy.extra['peer_buffer_addr']"
            )
        plan = self.build_peer_transfer_plan(
            tensors=[tensor.detach()],
            remote_session=str(remote_session),
            remote_pointers=[int(peer_buffer_addr)],
            mirror_tensors=[
                extra.get("materialized_result")
                if isinstance(extra.get("materialized_result"), torch.Tensor)
                else None
            ],
            mirror_local_copy=bool(extra.get("mirror_local_copy", True)),
            target_device=target_device,
        )
        result = self.transfer_registered_descriptors(plan)
        materialized = extra.get("materialized_result")
        if isinstance(materialized, torch.Tensor):
            mirrored = result.mirrored_tensors[0]
            if isinstance(mirrored, torch.Tensor):
                return mirrored.to(target_device, copy=True)
            return materialized.to(target_device, copy=True)
        if bool(extra.get("mirror_local_copy", True)):
            mirrored = result.mirrored_tensors[0]
            if isinstance(mirrored, torch.Tensor):
                return mirrored.to(target_device, copy=True)
            if strict_no_fallback_enabled(extra):
                raise RuntimeError(
                    "strict no-fallback direct-engine transfer did not return a "
                    "materialized mirror tensor"
                )
            return tensor.to(target_device, copy=True)
        return tensor
