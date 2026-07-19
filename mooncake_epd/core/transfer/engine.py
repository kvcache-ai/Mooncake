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
import importlib
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
from .cachegen import _compress_cachegen, _decompress_cachegen
from ..strict_mode import strict_no_fallback_enabled


class MooncakeProtocolError(RuntimeError):
    """Raised when a requested Mooncake transport cannot be used safely."""


class MooncakeProtocolCapabilityError(MooncakeProtocolError):
    """Raised when the deployed native engine lacks a requested protocol."""


def _normalize_mooncake_protocol(protocol: str | None) -> str:
    return str(protocol or "").strip().lower()


def validate_mooncake_protocol_pair(
    kv_protocol: str | None,
    direct_engine_protocol: str | None,
) -> None:
    """Reject unsafe P→D / E→P ``nvlink_intra`` protocol splits.

    Mooncake's intra-node NVLink selection is process-scoped through
    ``MC_INTRANODE_NVLINK``. A Prefill process can host both the vLLM KV
    connector and the direct FeatureHandle reader, so mixing ``nvlink_intra``
    with another native protocol would let one engine select the other's
    transport. Refuse that configuration rather than silently falling back.
    """

    kv = _normalize_mooncake_protocol(kv_protocol)
    direct = _normalize_mooncake_protocol(direct_engine_protocol)
    if "nvlink_intra" in {kv, direct} and kv != direct:
        raise MooncakeProtocolError(
            "P→D and E→P Mooncake protocols must both be 'nvlink_intra' "
            "when either selects intra-node NVLink; the native transport "
            "selector is process-scoped and mixed protocols can silently "
            f"select the wrong backend (P→D={kv!r}, E→P={direct!r})."
        )


def require_mooncake_protocol_support(
    protocol: str | None,
    *,
    engine_module: Any | None = None,
) -> None:
    """Fail closed when a native-only Mooncake protocol is unavailable.

    Older Mooncake ``engine.so`` builds log an error for ``nvlink_intra`` but
    still initialize their automatic transport selector, which can choose
    RDMA/TCP instead. The module capability attribute is compiled into newer
    engines; an absent attribute is deliberately treated as unsupported so an
    old binary cannot produce a misleading NVLink benchmark artifact.
    """

    requested = _normalize_mooncake_protocol(protocol)
    capability_attr = {"nvlink_intra": "SUPPORT_INTRA_NVLINK"}.get(requested)
    if capability_attr is None:
        return
    if engine_module is None:
        try:
            engine_module = importlib.import_module("mooncake.engine")
        except Exception as exc:
            raise MooncakeProtocolCapabilityError(
                f"requested Mooncake protocol {requested!r}, but mooncake.engine "
                "could not be imported. Rebuild and install Mooncake with "
                "-DUSE_CUDA=ON -DUSE_INTRA_NVLINK=ON; refusing transport fallback."
            ) from exc
    if getattr(engine_module, capability_attr, None) is not True:
        raise MooncakeProtocolCapabilityError(
            f"requested Mooncake protocol {requested!r}, but the deployed "
            f"mooncake.engine does not expose {capability_attr}=true. Rebuild "
            "and install Mooncake with -DUSE_CUDA=ON -DUSE_INTRA_NVLINK=ON; "
            "refusing silent transport fallback."
        )


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
    # ``register_memory`` returns ``-600`` when another owner has already
    # registered the same region.  Such a handle is usable for a transfer but
    # must never unregister memory it did not register itself.
    owns_registration: bool = False


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
    timings_ms: Dict[str, float] = field(default_factory=dict)


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
    timings_ms: Dict[str, float] = field(default_factory=dict)


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
        self.protocol = str(protocol).lower()
        self.hw = hw_caps or HwCaps.detect()
        self.local_hostname = local_hostname
        self.metadata_server = metadata_server
        self.device_name = device_name
        self.stats = TransferStats()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        # One vLLM worker can dispatch several E→P or P→D operations through
        # the same bound Mooncake C++ TransferEngine.  The native TCP backend
        # multiplexes its own I/O context, but the Python binding keeps
        # request/session and registration state per engine instance.  In
        # particular, overlapping register → transfer → unregister sequences
        # can report success while one request observes another request's
        # payload.  This re-entrant lock therefore protects every native-engine
        # transaction, not only the P→D batch-write fast path.  CPU-side
        # descriptor preparation and CUDA source fences stay outside whenever
        # they do not touch the native backend.
        self._native_transfer_lock = threading.RLock()
        self._mooncake = None
        self._store = None
        self._initialized = False
        self._owns_mooncake_backend = True

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def initialize(self) -> None:
        with self._native_transfer_lock:
            if self._initialized:
                return
            if self.protocol in ("tcp", "rdma", "nvlink_intra"):
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
                    if self.protocol == "nvlink_intra":
                        require_mooncake_protocol_support(self.protocol)
                        # The current native selector consumes this process-level
                        # setting during initialization. It is safe only after
                        # the capability check above has ruled out the historical
                        # RDMA/TCP auto-selection fallback.
                        os.environ.pop("MC_FORCE_TCP", None)
                        os.environ.setdefault("MC_INTRANODE_NVLINK", "1")
                    engine_module = importlib.import_module("mooncake.engine")
                    _MTE = engine_module.TransferEngine
                    self._mooncake = _MTE()
                    rc = self._mooncake.initialize(
                        self.local_hostname,
                        self.metadata_server,
                        self.protocol,
                        self.device_name,
                    )
                    if rc not in (None, 0):
                        raise RuntimeError(
                            "Mooncake Transfer Engine initialize returned "
                            f"rc={rc} for protocol={self.protocol!r}"
                        )
                except Exception as e:
                    raise RuntimeError(f"Mooncake Transfer Engine init failed: {e}") from e
                if os.getenv("MOONCAKE_EPD_INIT_PYTHON_STORE_ON_ENGINE_INIT", "0").lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }:
                    self._maybe_initialize_store()
            self._initialized = True

    def shutdown(self) -> None:
        with self._native_transfer_lock:
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
            self._initialized = False
            self._owns_mooncake_backend = True
        self._executor.shutdown(wait=False)

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
        with self._native_transfer_lock:
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
        with self._native_transfer_lock:
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
    # Direct engine peer-buffer helpers
    # ------------------------------------------------------------------
    def allocate_peer_buffer(self, size_bytes: int) -> DirectPeerBuffer:
        self.initialize()
        with self._native_transfer_lock:
            if self._mooncake is None:
                raise RuntimeError("Mooncake direct engine is not initialized")
            ptr = int(self._mooncake.allocate_managed_buffer(int(size_bytes)))
            if ptr <= 0:
                raise RuntimeError(f"allocate_managed_buffer failed for size={size_bytes}")
            return DirectPeerBuffer(pointer=ptr, size_bytes=int(size_bytes), registered=False)

    def free_peer_buffer(self, handle: DirectPeerBuffer) -> None:
        with self._native_transfer_lock:
            if self._mooncake is None:
                return
            rc = self._mooncake.free_managed_buffer(int(handle.pointer), int(handle.size_bytes))
            if rc != 0:
                raise RuntimeError(f"free_managed_buffer failed: rc={rc}")

    def write_peer_buffer(self, handle: DirectPeerBuffer, payload: bytes) -> None:
        self.initialize()
        with self._native_transfer_lock:
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
        with self._native_transfer_lock:
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
        with self._native_transfer_lock:
            if self._mooncake is None:
                raise RuntimeError("Mooncake direct engine is not initialized")
            nbytes = int(tensor.nelement() * tensor.element_size())
            ptr = int(tensor.data_ptr())
            rc = self._mooncake.register_memory(ptr, nbytes)
            if rc not in (0, -600):
                raise RuntimeError(f"register_memory failed: rc={rc}, ptr={ptr}, nbytes={nbytes}")
            return DirectPeerBuffer(
                pointer=ptr,
                size_bytes=nbytes,
                registered=True,
                owns_registration=(rc == 0),
            )

    def unregister_tensor_memory(self, handle: DirectPeerBuffer) -> None:
        with self._native_transfer_lock:
            if (
                not handle.registered
                or not handle.owns_registration
                or self._mooncake is None
            ):
                return
            rc = self._mooncake.unregister_memory(int(handle.pointer))
            if rc not in (0, -601):
                raise RuntimeError(f"unregister_memory failed: rc={rc}, ptr={handle.pointer}")

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

    def transfer_registered_pointer_batch(
        self,
        *,
        remote_session: str,
        local_pointers: Sequence[int],
        remote_pointers: Sequence[int],
        lengths: Sequence[int],
    ) -> PeerTransferResult:
        """Transfer already-registered pointers without descriptor allocation.

        The vLLM P->D KV path already owns persistent, Mooncake-registered KV
        cache regions.  Constructing a ``PeerTransferPlan`` for every layer
        group adds Python descriptor and wrapper allocation but cannot improve
        safety or data movement.  This narrow fast path retains the same direct
        Mooncake engine calls while accepting the native pointer arrays.

        Callers are responsible for the registration lifetime of every local
        pointer.  No tensor registration, unregister, mirror copy, staging, or
        payload copy is performed here.
        """

        if not (len(local_pointers) == len(remote_pointers) == len(lengths)):
            raise ValueError(
                "local_pointers, remote_pointers and lengths must have identical lengths"
            )
        if not remote_session:
            raise ValueError("remote_session is required for peer-buffer transfer")

        total_started = time.perf_counter()
        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is unavailable")
        if not local_pointers:
            return PeerTransferResult(
                nbytes=0,
                descriptor_count=0,
                timings_ms={
                    "total_ms": (time.perf_counter() - total_started) * 1000.0,
                    "prepare_ms": 0.0,
                    "write_ms": 0.0,
                    "descriptor_count": 0.0,
                    "nbytes": 0.0,
                    "registered_pointer_fast_path": 1.0,
                },
            )

        prepare_started = time.perf_counter()
        # vLLM supplies lists on the hot path. Keep those lists intact so the
        # Python binding can consume them directly; normalize only foreign
        # sequences passed by diagnostics or tests.
        src_ptrs = (
            local_pointers
            if isinstance(local_pointers, list)
            else [int(pointer) for pointer in local_pointers]
        )
        dst_ptrs = (
            remote_pointers
            if isinstance(remote_pointers, list)
            else [int(pointer) for pointer in remote_pointers]
        )
        transfer_lengths = (
            lengths if isinstance(lengths, list) else [int(length) for length in lengths]
        )
        total_bytes = 0
        for length in transfer_lengths:
            if int(length) < 0:
                raise ValueError(f"negative peer-buffer transfer length: {length}")
            total_bytes += int(length)
        prepare_ms = (time.perf_counter() - prepare_started) * 1000.0

        lock_wait_started = time.perf_counter()
        with self._native_transfer_lock:
            native_engine_lock_wait_ms = (
                time.perf_counter() - lock_wait_started
            ) * 1000.0
            write_started = time.perf_counter()
            if len(src_ptrs) == 1:
                rc = self._mooncake.transfer_sync_write(
                    str(remote_session),
                    int(src_ptrs[0]),
                    int(dst_ptrs[0]),
                    int(transfer_lengths[0]),
                )
            else:
                rc = self._mooncake.batch_transfer_sync_write(
                    str(remote_session),
                    src_ptrs,
                    dst_ptrs,
                    transfer_lengths,
                )
            write_ms = (time.perf_counter() - write_started) * 1000.0
        if rc != 0:
            raise RuntimeError(f"registered peer-buffer transfer failed: rc={rc}")

        return PeerTransferResult(
            nbytes=total_bytes,
            descriptor_count=len(src_ptrs),
            timings_ms={
                "total_ms": (time.perf_counter() - total_started) * 1000.0,
                "prepare_ms": prepare_ms,
                "write_ms": write_ms,
                "native_engine_lock_wait_ms": native_engine_lock_wait_ms,
                "descriptor_count": float(len(src_ptrs)),
                "nbytes": float(total_bytes),
                "registered_pointer_fast_path": 1.0,
            },
        )

    def transfer_registered_descriptors(
        self,
        plan: PeerTransferPlan,
    ) -> PeerTransferResult:
        total_started = time.perf_counter()
        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is unavailable")
        if not plan.descriptors:
            return PeerTransferResult(nbytes=0, descriptor_count=0)

        mirrored: List[Optional[torch.Tensor]] = []
        local_ptrs: List[int] = []
        remote_ptrs: List[int] = []
        lengths: List[int] = []
        total_bytes = 0
        prepare_ms = 0.0
        register_ms = 0.0
        write_ms = 0.0
        unregister_ms = 0.0
        lock_wait_started = time.perf_counter()
        with self._native_transfer_lock:
            native_engine_lock_wait_ms = (
                time.perf_counter() - lock_wait_started
            ) * 1000.0
            cleanup_handles: List[DirectPeerBuffer] = []
            try:
                prepare_started = time.perf_counter()
                for desc in plan.descriptors:
                    if desc.local_buffer is not None:
                        if not desc.local_buffer.registered and desc.tensor is None:
                            raise ValueError(
                                "pointer-only peer-buffer descriptors must reference registered memory"
                            )
                        local_ptr = int(desc.local_buffer.pointer)
                    elif desc.tensor is not None:
                        # A plan can be reused.  Register the tensor for this
                        # transaction rather than retaining a stale pointer
                        # registration from a prior call.
                        reg_started = time.perf_counter()
                        handle = self.register_tensor_memory(desc.tensor.detach())
                        register_ms += (time.perf_counter() - reg_started) * 1000.0
                        cleanup_handles.append(handle)
                        local_ptr = int(handle.pointer)
                    elif desc.local_pointer:
                        local_ptr = int(desc.local_pointer)
                    else:
                        raise ValueError(
                            "descriptor must provide tensor, local_pointer or local_buffer"
                        )

                    local_ptrs.append(local_ptr)
                    remote_ptrs.append(int(desc.remote_pointer))
                    lengths.append(int(desc.size_bytes))
                    total_bytes += int(desc.size_bytes)
                prepare_ms = (time.perf_counter() - prepare_started) * 1000.0

                write_started = time.perf_counter()
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
                write_ms = (time.perf_counter() - write_started) * 1000.0
                if rc != 0:
                    raise RuntimeError(f"peer-buffer transfer failed: rc={rc}")
            finally:
                for handle in cleanup_handles:
                    with contextlib.suppress(Exception):
                        unreg_started = time.perf_counter()
                        self.unregister_tensor_memory(handle)
                        unregister_ms += (time.perf_counter() - unreg_started) * 1000.0

        target_device = plan.target_device
        mirror_started = time.perf_counter()
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
        mirror_ms = (time.perf_counter() - mirror_started) * 1000.0

        return PeerTransferResult(
            nbytes=total_bytes,
            descriptor_count=len(plan.descriptors),
            mirrored_tensors=mirrored,
            timings_ms={
                "total_ms": (time.perf_counter() - total_started) * 1000.0,
                "prepare_ms": prepare_ms,
                "register_memory_ms": register_ms,
                "write_ms": write_ms,
                "unregister_memory_ms": unregister_ms,
                "mirror_ms": mirror_ms,
                "native_engine_lock_wait_ms": native_engine_lock_wait_ms,
                "descriptor_count": float(len(plan.descriptors)),
                "nbytes": float(total_bytes),
            },
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
        # The managed buffer allocation, native read, payload extraction, and
        # free form one ownership transaction.  Letting another request enter
        # between these steps can reuse or unregister a buffer while its bytes
        # are still being read from the same C++ engine instance.
        with self._native_transfer_lock:
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
        """Read remote peer buffers directly into caller-owned tensors.

        This is the zero-copy receive-side hot path for ``epd-direct://``:
        EngineCore allocates final hidden-state tensors on its target device,
        registers those tensor pointers with Mooncake, and the direct engine
        writes bytes into those tensors. It avoids managed-buffer staging,
        Python ``bytes`` materialization, and CPU→GPU tensor copies.
        """

        total_started = time.perf_counter()
        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("Mooncake direct engine is unavailable")
        if not remote_session:
            raise ValueError("remote_session is required for peer-buffer read")
        if len(remote_pointers) != len(tensors):
            raise ValueError("remote_pointers and tensors must have identical lengths")
        register_ms = 0.0
        transfer_ms = 0.0
        unregister_ms = 0.0
        completed = False
        lock_wait_started = time.perf_counter()
        with self._native_transfer_lock:
            native_engine_lock_wait_ms = (
                time.perf_counter() - lock_wait_started
            ) * 1000.0
            handles: List[DirectPeerBuffer] = []
            local_pointers: List[int] = []
            lengths: List[int] = []
            try:
                for tensor in tensors:
                    if not tensor.is_contiguous():
                        raise ValueError("direct peer-buffer read target tensors must be contiguous")
                    reg_started = time.perf_counter()
                    handle = self.register_tensor_memory(tensor)
                    register_ms += (time.perf_counter() - reg_started) * 1000.0
                    handles.append(handle)
                    local_pointers.append(int(handle.pointer))
                    lengths.append(int(handle.size_bytes))
                if not handles:
                    return {
                        "total_ms": (time.perf_counter() - total_started) * 1000.0,
                        "register_memory_ms": 0.0,
                        "read_ms": 0.0,
                        "unregister_memory_ms": 0.0,
                        "native_engine_lock_wait_ms": native_engine_lock_wait_ms,
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
                completed = True
            finally:
                for handle in handles:
                    with contextlib.suppress(Exception):
                        unreg_started = time.perf_counter()
                        self.unregister_tensor_memory(handle)
                        unregister_ms += (time.perf_counter() - unreg_started) * 1000.0
        if not completed:
            raise RuntimeError("peer-buffer read did not complete")
        return {
            "total_ms": (time.perf_counter() - total_started) * 1000.0,
            "register_memory_ms": register_ms,
            "read_ms": transfer_ms,
            "unregister_memory_ms": unregister_ms,
            "native_engine_lock_wait_ms": native_engine_lock_wait_ms,
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
        """Transfer one FeatureBundle through Mooncake direct peer buffers.

        This compatibility entry point deliberately uses the same batching
        primitive as the multi-feature hot path.  A one-item batch preserves
        the former public contract while keeping registration, capacity
        validation, timing fields, and cleanup identical to a real request
        containing multiple images.
        """

        return self.transfer_feature_bundle_peer_buffer_plans(
            [bundle],
            [plan],
            source_memory_mode=source_memory_mode,
        )[0]

    def transfer_feature_bundle_peer_buffer_plans(
        self,
        bundles: Sequence[Any],
        plans: Sequence[FeatureBundlePeerBufferPlan],
        *,
        source_memory_mode: Optional[str] = None,
    ) -> List[FeatureBundlePeerBufferResult]:
        """Transfer several FeatureBundles to one Prefill session in one write.

        A multimodal request can contain several independent images.  Before
        this helper, each image issued a separate Mooncake
        ``batch_transfer_sync_write`` even though all of its destination
        buffers belonged to the same Prefill worker.  Coalescing the tensor
        descriptors into one engine batch removes those control/data-plane
        dispatches from the E->P critical path without changing ownership:
        Prefill still allocates every destination buffer and every bundle is
        validated immediately before the write.

        Mooncake's batch API accepts one remote session, so callers must group
        entries by ``FeatureBundlePeerBufferPlan.remote_session``.  We fail
        fast rather than silently splitting or falling back; the online
        service performs that grouping while preserving the request's handle
        order.

        ``source_memory_mode`` has the same contract as the single-bundle
        method: ``registered_tensor`` registers the source tensor memory for
        the write, while ``managed_buffer`` stages it in a Mooncake managed
        buffer.  Both modes perform exactly one peer-buffer write per batch.
        """

        if len(bundles) != len(plans):
            raise ValueError("bundles and plans must have identical lengths")
        if not bundles:
            return []

        remote_session = str(plans[0].remote_session or "")
        if not remote_session:
            raise ValueError("remote_session is required for FeatureBundle peer-buffer transfer")
        differing_sessions = sorted(
            {
                str(plan.remote_session or "")
                for plan in plans
                if str(plan.remote_session or "") != remote_session
            }
        )
        if differing_sessions:
            raise ValueError(
                "FeatureBundle peer-buffer batch requires one remote_session; "
                f"expected={remote_session!r} got={differing_sessions!r}"
            )

        self.initialize()
        if self._mooncake is None:
            raise RuntimeError("FeatureBundle direct peer-buffer transfer requires a real Mooncake engine")
        mode = str(
            source_memory_mode
            or os.getenv("MOONCAKE_EPD_DIRECT_SOURCE_MODE", "registered_tensor")
        ).lower()
        if mode not in {"registered_tensor", "managed_buffer"}:
            raise ValueError(f"unsupported FeatureBundle direct source_memory_mode: {mode}")

        total_started = time.perf_counter()
        tensors: List[torch.Tensor] = []
        remote_pointers: List[int] = []
        lengths: List[int] = []
        per_bundle: List[Tuple[FeatureBundlePeerBufferPlan, int, int]] = []
        for bundle, plan in zip(bundles, plans):
            tensors_by_name = {
                name: tensor for name, tensor in self.feature_bundle_tensor_items(bundle)
            }
            bundle_nbytes = 0
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
                tensors.append(tensor)
                remote_pointers.append(int(target.remote_pointer))
                lengths.append(nbytes)
                bundle_nbytes += nbytes
            per_bundle.append((plan, bundle_nbytes, len(plan.targets)))

        if mode == "registered_tensor":
            build_started = time.perf_counter()
            peer_plan = self.build_peer_transfer_plan(
                tensors=tensors,
                remote_session=remote_session,
                remote_pointers=remote_pointers,
                mirror_local_copy=False,
            )
            build_plan_ms = (time.perf_counter() - build_started) * 1000.0
            result = self.transfer_peer_buffer_plan(peer_plan)
            staging_ms = 0.0
        else:
            staging_ms = 0.0
            local_pointers: List[int] = []
            staged_buffers: List[DirectPeerBuffer] = []
            try:
                for tensor, nbytes in zip(tensors, lengths):
                    stage_started = time.perf_counter()
                    handle = self.allocate_peer_buffer(nbytes)
                    staged_buffers.append(handle)
                    self.write_peer_buffer(handle, _tensor_raw_bytes(tensor))
                    staging_ms += (time.perf_counter() - stage_started) * 1000.0
                    local_pointers.append(int(handle.pointer))
                build_started = time.perf_counter()
                pointer_plan = self.build_pointer_transfer_plan(
                    remote_session=remote_session,
                    local_pointers=local_pointers,
                    remote_pointers=remote_pointers,
                    lengths=lengths,
                    registered=True,
                )
                build_plan_ms = (time.perf_counter() - build_started) * 1000.0
                result = self.transfer_peer_buffer_plan(pointer_plan)
            finally:
                for handle in staged_buffers:
                    with contextlib.suppress(Exception):
                        self.free_peer_buffer(handle)

        elapsed_ms = (time.perf_counter() - total_started) * 1000.0
        # One stat record denotes one actual Mooncake write.  Recording it once
        # prevents a multi-image request from being misreported as N separate
        # data-plane operations simply because it returns N control handles.
        self.stats.record("encoder_to_prefill_peer_buffer_direct", result.nbytes, elapsed_ms)
        shared_timings = dict(result.timings_ms or {})
        shared_timings.update(
            {
                "total_ms": elapsed_ms,
                "staging_ms": staging_ms,
                "build_peer_plan_ms": build_plan_ms,
                "batch_total_ms": elapsed_ms,
                "batch_write_ms": float((result.timings_ms or {}).get("write_ms", 0.0)),
                "batch_feature_count": float(len(plans)),
                "batch_descriptor_count": float(result.descriptor_count),
                "batch_nbytes": float(result.nbytes),
            }
        )
        results: List[FeatureBundlePeerBufferResult] = []
        for batch_index, (plan, bundle_nbytes, tensor_count) in enumerate(per_bundle):
            timings = dict(shared_timings)
            # The wall time is intentionally shared: all features become ready
            # only when the single Mooncake batch completes.  The index makes
            # it clear to telemetry consumers that these are per-handle views
            # of one transfer rather than independent writes.
            timings["batch_index"] = float(batch_index)
            results.append(
                FeatureBundlePeerBufferResult(
                    feature_id=plan.feature_id,
                    nbytes=int(bundle_nbytes),
                    tensor_count=int(tensor_count),
                    descriptor_count=int(tensor_count),
                    timings_ms=timings,
                )
            )
        return results

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
        elif self.protocol in ("local", "shm") or (tensor.device == target_device):
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
        rc = self._store.put_tensor(key, tensor.detach())
        if rc != 0:
            raise RuntimeError(f"MooncakeDistributedStore.put_tensor failed: rc={rc}")
        restored = self._store.get_tensor(key)
        if restored is None:
            raise RuntimeError(f"MooncakeDistributedStore.get_tensor returned None for key={key}")
        if cleanup:
            with contextlib.suppress(Exception):
                self._store.remove(key, True)
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
