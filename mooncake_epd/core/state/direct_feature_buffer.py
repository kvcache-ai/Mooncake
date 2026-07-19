"""Prefill-owned direct FeatureBundle buffer registry.

The E→P direct path is two-sided:

1. Prefill allocates destination tensors/buffers and exposes their registered
   pointers to the Encoder control plane.
2. Encoder writes FeatureBundle tensor bytes through Mooncake TransferEngine.
3. Prefill resolves the resulting ``epd-direct://`` FeatureHandle by looking up
   those exact pre-allocated buffers and validating the descriptor.

This module implements step 1/3 without file or object-store fallback.  It is
safe to import in vLLM workers and can be wired into sitecustomize/provider hooks.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional

import torch

from .feature_handle import FeatureHandle, FeatureHandleError
from .feature_store import FeatureBundle, FeatureBundleDescriptor, TensorSpec


_DTYPE_BY_NAME = {
    "float32": torch.float32,
    "float": torch.float32,
    "fp32": torch.float32,
    "float16": torch.float16,
    "half": torch.float16,
    "fp16": torch.float16,
    "bfloat16": torch.bfloat16,
    "bf16": torch.bfloat16,
    "int64": torch.int64,
    "long": torch.int64,
    "int32": torch.int32,
    "int": torch.int32,
    "uint8": torch.uint8,
}


def _dtype_from_spec(spec: TensorSpec) -> torch.dtype:
    dtype = str(spec.dtype).replace("torch.", "").lower()
    if dtype not in _DTYPE_BY_NAME:
        raise FeatureHandleError(f"unsupported direct feature tensor dtype: {spec.dtype}")
    return _DTYPE_BY_NAME[dtype]


@dataclass
class DirectFeatureBufferAllocation:
    # ``feature_id`` identifies immutable feature *content*.  It does not
    # identify one concrete GPU allocation: a non-persistent buffer may be
    # released and later recreated for the same descriptor.  Keep an opaque
    # allocation incarnation on the target/control-plane contract so vLLM's
    # resolved-tensor cache can never retain a pointer-backed bundle across
    # that lifetime boundary.
    feature_id: str
    descriptor: FeatureBundleDescriptor
    tensors: Dict[str, torch.Tensor]
    allocation_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    nbytes: int = 0
    created_at: float = field(default_factory=time.monotonic)
    consumed: bool = False
    registered_buffers: List[Any] = field(default_factory=list)
    managed_buffers: Dict[str, Any] = field(default_factory=dict)
    transfer_engine: Optional[Any] = None
    target_memory_mode: str = "registered_tensor"
    remote_session: str = ""
    worker_id: str = "prefill"
    ready: bool = False
    publish_started: bool = False
    ref_count: int = 1
    last_used_at: float = field(default_factory=time.monotonic)
    ready_event: threading.Event = field(default_factory=threading.Event)

    @property
    def remote_pointers(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        if self.managed_buffers:
            for name, handle in self.managed_buffers.items():
                out[name] = int(handle.pointer)
                out[f"{name}:nbytes"] = int(handle.size_bytes)
            return out
        for name, tensor in self.tensors.items():
            out[name] = int(tensor.data_ptr())
            out[f"{name}:nbytes"] = int(tensor.nelement() * tensor.element_size())
        return out


    def as_direct_target(self) -> Dict[str, Any]:
        if not self.remote_session:
            raise FeatureHandleError(
                "direct feature target export requires remote_session; provide a "
                "Mooncake TransferEngine or explicit remote_session"
            )
        return {
            "allocation_id": self.allocation_id,
            "feature_id": self.feature_id,
            "worker_id": self.worker_id,
            "remote_session": self.remote_session,
            "remote_pointers": self.remote_pointers,
            "target_memory_mode": self.target_memory_mode,
            "descriptor": self.descriptor.to_dict(),
            "ready": bool(self.ready),
        }


@dataclass
class _DirectFeatureAllocationFlight:
    """Shared completion state for one feature buffer being allocated."""

    ready_event: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None


class DirectFeatureBufferRegistry:
    """Registry of prefill-owned tensors addressed by direct FeatureHandles.

    ``transfer_engine`` should be the Mooncake TransferEngine owned by the same
    Prefill/vLLM process. When ``register_memory`` is true, every tensor is
    registered with that engine before its pointer is returned. A standalone
    sidecar process may use this class for diagnostics, but production serving
    must embed it in the process that later resolves the FeatureHandle.
    """

    def __init__(
        self,
        *,
        worker_id: str = "prefill",
        device: str | torch.device = "cpu",
        transfer_engine: Optional[Any] = None,
        remote_session: Optional[str] = None,
        register_memory: bool = False,
        target_memory_mode: str = "registered_tensor",
        persistent_cache: bool = False,
        max_cache_entries: int = 64,
        max_cache_bytes: int = 2 * 1024 * 1024 * 1024,
    ):
        self.worker_id = str(worker_id)
        self.device = torch.device(device)
        self.transfer_engine = transfer_engine
        self._remote_session_override = None if remote_session is None else str(remote_session)
        self.register_memory = bool(register_memory)
        self.target_memory_mode = str(target_memory_mode or "registered_tensor").lower()
        self.persistent_cache = bool(persistent_cache)
        self.max_cache_entries = max(0, int(max_cache_entries))
        self.max_cache_bytes = max(0, int(max_cache_bytes))
        self._allocations: Dict[str, DirectFeatureBufferAllocation] = {}
        self._allocation_flights: Dict[str, _DirectFeatureAllocationFlight] = {}
        self._lock = threading.RLock()
        # CUDA allocation can proceed independently, but Mooncake registration
        # owns shared engine state. Keep the critical section to that operation
        # instead of serializing full HTTP allocate/release requests.
        self._registration_lock = threading.RLock()
        self._current_bytes = 0

    @property
    def remote_session(self) -> str:
        if self._remote_session_override:
            return self._remote_session_override
        if self.transfer_engine is None:
            return ""
        return str(self.transfer_engine.direct_remote_session())

    def allocate_for_descriptor(
        self,
        descriptor: FeatureBundleDescriptor,
        *,
        feature_id: Optional[str] = None,
        zero_fill: bool = True,
        reuse_ready: bool = True,
    ) -> DirectFeatureBufferAllocation:
        fid = str(feature_id or descriptor.feature_id)
        flight: _DirectFeatureAllocationFlight | None = None
        with self._lock:
            existing = self._allocations.get(fid)
            if existing is not None:
                if not _descriptor_compatible(existing.descriptor, descriptor):
                    raise FeatureHandleError(
                        "direct feature_id collision with an incompatible descriptor: "
                        f"feature_id={fid}"
                    )
                if not reuse_ready:
                    raise FeatureHandleError(
                        "replacing a live direct feature allocation is unsafe; "
                        "release it first or use a versioned feature_id"
                    )
                existing.ref_count += 1
                existing.last_used_at = time.monotonic()
                return existing
            flight = self._allocation_flights.get(fid)
            if flight is None:
                flight = _DirectFeatureAllocationFlight()
                self._allocation_flights[fid] = flight
                owner = True
            else:
                owner = False

        if not owner:
            assert flight is not None
            flight.ready_event.wait()
            if flight.error is not None:
                raise FeatureHandleError(
                    f"concurrent direct feature allocation failed: feature_id={fid}"
                ) from flight.error
            # The owner published its allocation while holding the registry lock.
            # Re-enter the normal path so this caller acquires a reference too.
            return self.allocate_for_descriptor(
                descriptor,
                feature_id=fid,
                zero_fill=zero_fill,
                reuse_ready=reuse_ready,
            )

        try:
            tensors = _allocate_tensors_for_descriptor(descriptor, self.device, zero_fill=zero_fill)
            registered: List[Any] = []
            managed: Dict[str, Any] = {}
            mode = self.target_memory_mode
            if mode not in {"registered_tensor", "managed_buffer", "auto"}:
                raise FeatureHandleError(f"unsupported direct target memory mode: {self.target_memory_mode}")
            try:
                if mode in {"registered_tensor", "auto"} and self.register_memory:
                    try:
                        registered = self._register_tensors(tensors)
                        mode = "registered_tensor"
                    except Exception:
                        if mode != "auto":
                            raise
                        self._unregister_buffers(registered)
                        registered = []
                        managed = self._allocate_managed_buffers_for_tensors(tensors)
                        mode = "managed_buffer"
                elif mode == "managed_buffer":
                    managed = self._allocate_managed_buffers_for_tensors(tensors)
                else:
                    mode = "registered_tensor"
            except Exception:
                self._unregister_buffers(registered)
                self._free_managed_buffers(managed.values())
                raise
            allocation = DirectFeatureBufferAllocation(
                fid,
                descriptor,
                tensors,
                nbytes=sum(
                    int(tensor.nelement() * tensor.element_size())
                    for tensor in tensors.values()
                ),
                registered_buffers=registered,
                managed_buffers=managed,
                transfer_engine=self.transfer_engine,
                target_memory_mode=mode,
                remote_session=self.remote_session,
                worker_id=self.worker_id,
            )
        except BaseException as exc:
            with self._lock:
                current = self._allocation_flights.pop(fid, None)
                if current is not None:
                    current.error = exc
                    current.ready_event.set()
            raise
        victims: List[DirectFeatureBufferAllocation]
        with self._lock:
            self._allocations[fid] = allocation
            self._current_bytes += self._allocation_nbytes(allocation)
            victims = self._enforce_limits_locked()
            current = self._allocation_flights.pop(fid, None)
            if current is not None:
                current.ready_event.set()
        self._release_allocations(victims)
        return allocation

    def register_allocation(self, allocation: DirectFeatureBufferAllocation) -> None:
        victims: List[DirectFeatureBufferAllocation]
        with self._lock:
            old = self._allocations.pop(str(allocation.feature_id), None)
            if old is not None:
                self._current_bytes = max(0, self._current_bytes - self._allocation_nbytes(old))
            self._allocations[str(allocation.feature_id)] = allocation
            self._current_bytes += self._allocation_nbytes(allocation)
            victims = self._enforce_limits_locked()
        if old is not None:
            self._release_allocation(old)
        self._release_allocations(victims)

    def get(self, feature_id: str) -> Optional[DirectFeatureBufferAllocation]:
        with self._lock:
            return self._allocations.get(str(feature_id))

    def release(self, feature_id: str) -> None:
        victims: List[DirectFeatureBufferAllocation] = []
        allocation_to_release: DirectFeatureBufferAllocation | None = None
        with self._lock:
            allocation = self._allocations.get(str(feature_id))
            if allocation is None:
                return

            # Every /allocate or /lookup call owns one logical reference.  The
            # old non-persistent branch removed the allocation unconditionally,
            # so two concurrent requests sharing a feature could release the
            # peer buffer while the second Prefill worker was still reading it.
            # Keep the same refcount invariant for both cache modes.  A
            # non-persistent allocation is removed only after its final request
            # reference is released; a persistent allocation remains resident
            # with ref_count=0 and is eligible for LRU eviction.
            allocation.ref_count = max(0, int(allocation.ref_count) - 1)
            allocation.last_used_at = time.monotonic()
            if self.persistent_cache and allocation.ready:
                victims = self._enforce_limits_locked()
            elif allocation.ref_count > 0:
                return
            else:
                allocation_to_release = self._allocations.pop(str(feature_id), None)
                if allocation_to_release is not None:
                    self._current_bytes = max(
                        0,
                        self._current_bytes
                        - self._allocation_nbytes(allocation_to_release),
                    )
        self._release_allocations(victims)
        if allocation_to_release is not None:
            self._release_allocation(allocation_to_release)

    def mark_ready(self, feature_id: str) -> DirectFeatureBufferAllocation:
        victims: List[DirectFeatureBufferAllocation]
        with self._lock:
            allocation = self._allocations.get(str(feature_id))
            if allocation is None:
                raise FeatureHandleError(f"direct feature allocation not found: {feature_id}")
            allocation.ready = True
            allocation.publish_started = False
            allocation.last_used_at = time.monotonic()
            allocation.ready_event.set()
            victims = self._enforce_limits_locked()
        self._release_allocations(victims)
        return allocation

    def wait_ready(self, feature_id: str, *, timeout_s: Optional[float] = None) -> DirectFeatureBufferAllocation:
        with self._lock:
            allocation = self._allocations.get(str(feature_id))
        if allocation is None:
            raise FeatureHandleError(f"direct feature allocation not found: {feature_id}")
        if not allocation.ready_event.wait(None if timeout_s is None else max(0.0, float(timeout_s))):
            raise TimeoutError(f"direct feature allocation is not ready: {feature_id}")
        with self._lock:
            allocation.last_used_at = time.monotonic()
        return allocation

    def lookup_ready(
        self,
        feature_id: str,
        *,
        lease_count: int = 1,
    ) -> Optional[DirectFeatureBufferAllocation]:
        """Return a ready allocation and acquire ``lease_count`` references.

        A normal direct-feature lookup acquires one reference that the Proxy
        releases after Prefill consumes the handle.  The Proxy can also reserve
        a small, bounded batch of those references for an identical
        generation-fenced request tuple.  Reserving them here is atomic with
        the ready check, so a persistent-cache eviction can never reclaim the
        peer buffer between a successful lookup and a later checkout.
        """

        try:
            requested_leases = int(lease_count)
        except (TypeError, ValueError) as exc:
            raise FeatureHandleError("direct feature lease_count must be an integer") from exc
        if requested_leases <= 0:
            raise FeatureHandleError("direct feature lease_count must be positive")
        with self._lock:
            allocation = self._allocations.get(str(feature_id))
            if allocation is None or not allocation.ready:
                return None
            allocation.ref_count += requested_leases
            allocation.last_used_at = time.monotonic()
            return allocation

    def resolve_handle(self, handle: FeatureHandle, *, consume: bool = False) -> FeatureBundle:
        if not str(handle.uri or "").startswith("epd-direct://"):
            raise FeatureHandleError(f"not an epd-direct handle: {handle.uri}")
        allocation = self.get(handle.feature_id)
        if allocation is None:
            raise FeatureHandleError(
                f"direct feature buffers not allocated for feature_id={handle.feature_id} worker={self.worker_id}"
            )
        if allocation.consumed and consume:
            raise FeatureHandleError(f"direct feature buffers already consumed: {handle.feature_id}")
        _validate_direct_plan_matches_allocation(handle, allocation)
        bundle = _bundle_from_allocation(allocation)
        handle.descriptor.validate_bundle(bundle)
        if consume:
            allocation.consumed = True
        return bundle

    def stats(self) -> Dict[str, int | str | bool]:
        with self._lock:
            return {
                "worker_id": self.worker_id,
                "device": str(self.device),
                "remote_session": self._remote_session_override or "dynamic",
                "register_memory": self.register_memory,
                "persistent_cache": self.persistent_cache,
                "max_cache_entries": self.max_cache_entries,
                "max_cache_bytes": self.max_cache_bytes,
                "allocations": len(self._allocations),
                "tensors": sum(len(a.tensors) for a in self._allocations.values()),
                "bytes": self._current_bytes,
                "consumed": sum(1 for a in self._allocations.values() if a.consumed),
                "ready": sum(1 for a in self._allocations.values() if a.ready),
                "ref_count": sum(int(a.ref_count) for a in self._allocations.values()),
                "registered_buffers": sum(len(a.registered_buffers) for a in self._allocations.values()),
                "managed_buffers": sum(len(a.managed_buffers) for a in self._allocations.values()),
                "allocating": len(self._allocation_flights),
            }

    def _register_tensors(self, tensors: Mapping[str, torch.Tensor]) -> List[Any]:
        if self.transfer_engine is None:
            raise FeatureHandleError("register_memory requires a Mooncake TransferEngine")
        registered: List[Any] = []
        with self._registration_lock:
            try:
                for tensor in tensors.values():
                    registered.append(self.transfer_engine.register_tensor_memory(tensor))
                return registered
            except Exception:
                self._unregister_buffers(registered)
                raise

    def _allocate_managed_buffers_for_tensors(self, tensors: Mapping[str, torch.Tensor]) -> Dict[str, Any]:
        if self.transfer_engine is None:
            raise FeatureHandleError("managed_buffer mode requires a Mooncake TransferEngine")
        managed: Dict[str, Any] = {}
        with self._registration_lock:
            try:
                for name, tensor in tensors.items():
                    nbytes = int(tensor.nelement() * tensor.element_size())
                    managed[name] = self.transfer_engine.allocate_peer_buffer(nbytes)
                return managed
            except Exception:
                self._free_managed_buffers(managed.values())
                raise

    def _free_managed_buffers(self, buffers: Iterable[Any]) -> None:
        if self.transfer_engine is None:
            return
        with self._registration_lock:
            for handle in list(buffers):
                self.transfer_engine.free_peer_buffer(handle)

    def _unregister_buffers(self, buffers: Iterable[Any]) -> None:
        if self.transfer_engine is None:
            return
        with self._registration_lock:
            for handle in list(buffers):
                self.transfer_engine.unregister_tensor_memory(handle)

    def _release_allocation(self, allocation: DirectFeatureBufferAllocation) -> None:
        self._unregister_buffers(allocation.registered_buffers)
        allocation.registered_buffers.clear()
        self._free_managed_buffers(allocation.managed_buffers.values())
        allocation.managed_buffers.clear()
        allocation.ready = False
        allocation.ready_event.set()

    @staticmethod
    def _allocation_nbytes(allocation: DirectFeatureBufferAllocation) -> int:
        if int(allocation.nbytes) > 0:
            return int(allocation.nbytes)
        return sum(
            int(tensor.nelement() * tensor.element_size())
            for tensor in allocation.tensors.values()
        )

    def _release_allocations(
        self,
        allocations: Iterable[DirectFeatureBufferAllocation],
    ) -> None:
        for allocation in allocations:
            self._release_allocation(allocation)

    def _enforce_limits_locked(self) -> List[DirectFeatureBufferAllocation]:
        if not self.persistent_cache:
            return []

        victims_to_release: List[DirectFeatureBufferAllocation] = []
        while True:
            too_many = self.max_cache_entries > 0 and len(self._allocations) > self.max_cache_entries
            too_large = self.max_cache_bytes > 0 and self._current_bytes > self.max_cache_bytes
            if not too_many and not too_large:
                return victims_to_release
            victims = [
                a for a in self._allocations.values()
                if a.ready and int(a.ref_count) <= 0
            ]
            if not victims:
                return victims_to_release
            victim = min(victims, key=lambda a: float(a.last_used_at))
            self._allocations.pop(victim.feature_id, None)
            self._current_bytes = max(
                0, self._current_bytes - self._allocation_nbytes(victim)
            )
            victims_to_release.append(victim)


def _allocate_tensors_for_descriptor(
    descriptor: FeatureBundleDescriptor,
    device: torch.device,
    *,
    zero_fill: bool,
) -> Dict[str, torch.Tensor]:
    tensors: Dict[str, torch.Tensor] = {}

    def alloc(name: str, spec: TensorSpec) -> None:
        dtype = _dtype_from_spec(spec)
        shape = tuple(int(x) for x in spec.shape)
        if zero_fill:
            tensor = torch.zeros(shape, dtype=dtype, device=device)
        else:
            tensor = torch.empty(shape, dtype=dtype, device=device)
        if int(tensor.nelement() * tensor.element_size()) != int(spec.nbytes):
            raise FeatureHandleError(
                f"direct tensor allocation nbytes mismatch for {name}: "
                f"allocated={tensor.nelement() * tensor.element_size()} spec={spec.nbytes}"
            )
        tensors[name] = tensor

    alloc("last_hidden", descriptor.last_hidden)
    if descriptor.grid_thw is not None:
        alloc("grid_thw", descriptor.grid_thw)
    for ordinal, (layer, spec) in enumerate(descriptor.intermediates):
        alloc(f"intermediate:{int(layer)}:{ordinal}", spec)
    return tensors


def _descriptor_compatible(left: FeatureBundleDescriptor, right: FeatureBundleDescriptor) -> bool:
    """Return true when two descriptors can safely share one direct buffer.

    The direct cache reuses GPU/peer-buffer storage by feature id.  Reuse is
    only valid when the full tensor contract matches, including fingerprints,
    dtype/shape/nbytes and descriptor metadata.  Keeping this strict avoids a
    fast but unsafe hidden-state alias when a model or processor changes.
    """

    return left.to_dict() == right.to_dict()


def _materialize_managed_buffers(allocation: DirectFeatureBufferAllocation) -> None:
    if not allocation.managed_buffers:
        return
    if allocation.transfer_engine is None:
        raise FeatureHandleError("managed direct allocation has no transfer engine")
    for name, handle in allocation.managed_buffers.items():
        tensor = allocation.tensors.get(name)
        if tensor is None:
            raise FeatureHandleError(f"managed direct allocation missing tensor {name}")
        nbytes = int(tensor.nelement() * tensor.element_size())
        if int(handle.size_bytes) < nbytes:
            raise FeatureHandleError(
                f"managed direct buffer undersized for {name}: buffer={handle.size_bytes} tensor={nbytes}"
            )
        raw = allocation.transfer_engine.read_peer_buffer(handle, nbytes)
        cpu = torch.frombuffer(bytearray(raw), dtype=tensor.dtype).reshape(tuple(tensor.shape))
        tensor.copy_(cpu.to(device=tensor.device, dtype=tensor.dtype), non_blocking=False)


def _bundle_from_allocation(allocation: DirectFeatureBufferAllocation) -> FeatureBundle:
    _materialize_managed_buffers(allocation)
    descriptor = allocation.descriptor
    tensors = allocation.tensors
    intermediates = []
    for ordinal, (layer, _) in enumerate(descriptor.intermediates):
        name = f"intermediate:{int(layer)}:{ordinal}"
        if name not in tensors:
            raise FeatureHandleError(f"direct allocation missing tensor {name}")
        intermediates.append((int(layer), tensors[name]))
    grid = tensors.get("grid_thw") if descriptor.grid_thw is not None else None
    if "last_hidden" not in tensors:
        raise FeatureHandleError("direct allocation missing tensor last_hidden")
    return FeatureBundle(
        image_hash=descriptor.feature_id,
        last_hidden=tensors["last_hidden"],
        intermediates=intermediates,
        grid_thw=grid,
        metadata=dict(descriptor.metadata),
    )


def _validate_direct_plan_matches_allocation(
    handle: FeatureHandle,
    allocation: DirectFeatureBufferAllocation,
) -> None:
    expected_allocation_id = str(
        dict(handle.metadata or {}).get("direct_allocation_id") or ""
    ).strip()
    if expected_allocation_id and expected_allocation_id != allocation.allocation_id:
        raise FeatureHandleError(
            "direct allocation incarnation mismatch: "
            f"handle={expected_allocation_id} allocation={allocation.allocation_id}"
        )
    plan = dict(handle.metadata.get("direct_plan") or {})
    raw_targets = list(plan.get("targets") or [])
    if not raw_targets:
        raise FeatureHandleError("epd-direct handle metadata missing direct_plan.targets")
    by_name = {str(item.get("name")): dict(item) for item in raw_targets if isinstance(item, Mapping)}
    for name, tensor in allocation.tensors.items():
        target = by_name.get(name)
        if target is None:
            raise FeatureHandleError(f"direct plan missing target {name}")
        ptr = int(target.get("remote_pointer", -1))
        nbytes = int(target.get("nbytes", -1))
        handle = allocation.managed_buffers.get(name) if allocation.managed_buffers else None
        actual_ptr = int(handle.pointer) if handle is not None else int(tensor.data_ptr())
        actual_nbytes = int(handle.size_bytes) if handle is not None else int(tensor.nelement() * tensor.element_size())
        if ptr != actual_ptr:
            raise FeatureHandleError(
                f"direct pointer mismatch for {name}: plan={ptr} allocation={actual_ptr}"
            )
        if nbytes != actual_nbytes:
            raise FeatureHandleError(
                f"direct nbytes mismatch for {name}: plan={nbytes} allocation={actual_nbytes}"
            )


_GLOBAL_REGISTRIES: Dict[str, DirectFeatureBufferRegistry] = {}
_GLOBAL_LOCK = threading.RLock()


def register_direct_feature_buffer_registry(registry: DirectFeatureBufferRegistry) -> None:
    with _GLOBAL_LOCK:
        _GLOBAL_REGISTRIES[registry.worker_id] = registry


def unregister_direct_feature_buffer_registry(worker_id: str) -> None:
    with _GLOBAL_LOCK:
        registry = _GLOBAL_REGISTRIES.pop(str(worker_id), None)
    if registry is not None:
        for feature_id in list(registry._allocations):  # noqa: SLF001 - global lifecycle helper
            registry.release(feature_id)


def get_direct_feature_buffer_registry(worker_id: str) -> Optional[DirectFeatureBufferRegistry]:
    with _GLOBAL_LOCK:
        return _GLOBAL_REGISTRIES.get(str(worker_id))


def iter_direct_feature_buffer_registries() -> Iterable[DirectFeatureBufferRegistry]:
    with _GLOBAL_LOCK:
        return tuple(_GLOBAL_REGISTRIES.values())
