"""Process-shared host memory for one-writer/many-reader KV offload.

MLA keeps the same KV in every rank of a TP group, so offloading it per rank
multiplies host memory by the TP size for no benefit. A shared segment allocates
the pages once, in ``owner_rank``, and maps them into every rank of the group.

Virtual addresses are deliberately not forced to match across ranks: peers agree
on the byte layout, so the reserved address space equals the segment size rather
than size x world_size. Layout (which offset holds which tensor) is computed
here; C++ only shares the raw span.

    from mooncake.shared_segment import create_shared_segment

    seg = create_shared_segment(
        "vllm_sparse_kv",
        blocks={
            "k": dict(count=num_layers, shape=k_shape, dtype=torch.bfloat16),
            "v": dict(count=num_layers, shape=v_shape, dtype=torch.bfloat16),
        },
        world_size=tp_size,
        rank_id=tp_rank,
        tp_group=tp_group,
        mmap=True,
    )
    k_caches = seg.tensors("k")
"""

from __future__ import annotations

import ctypes
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import torch
import torch.distributed as dist

from .engine import SharedSegment as _CppSharedSegment

__all__ = [
    "SharedSegment",
    "SharedSegmentError",
    "create_shared_segment",
    "shared_segment_supported",
]

_ALIGN = 4096
_ACL_HOST_GET_DEVICE_POINTER = None


class SharedSegmentError(RuntimeError):
    """Raised when a shared segment cannot be created or addressed."""


@dataclass(frozen=True)
class _BlockSpec:
    count: int
    shape: Tuple[int, ...]
    dtype: torch.dtype
    nbytes: int


def shared_segment_supported(mmap: bool = False) -> bool:
    return _CppSharedSegment.supported(mmap)


def _align_up(value: int, alignment: int = _ALIGN) -> int:
    return (value + alignment - 1) // alignment * alignment


def _parse_block(name: str, spec: Mapping[str, Any]) -> _BlockSpec:
    try:
        count = int(spec["count"])
        shape = tuple(int(dim) for dim in spec["shape"])
        dtype = spec["dtype"]
    except (KeyError, TypeError) as exc:
        raise SharedSegmentError(
            f"Block {name!r} needs count, shape and dtype"
        ) from exc
    if count <= 0:
        raise SharedSegmentError(f"Block {name!r} needs a positive count")
    if not shape or any(dim <= 0 for dim in shape):
        raise SharedSegmentError(f"Block {name!r} needs a positive shape")

    element_size = torch.empty(0, dtype=dtype).element_size()
    nbytes = element_size
    for dim in shape:
        nbytes *= dim
    return _BlockSpec(count=count, shape=shape, dtype=dtype, nbytes=nbytes)


def _build_layout(
    specs: Dict[str, _BlockSpec], names: Sequence[str]
) -> Tuple[Dict[str, int], int, int]:
    """Returns (offsets_by_name, group_stride, total_size)."""
    stride = 0
    offsets: Dict[str, int] = {}
    for name in names:
        offsets[name] = stride
        stride += _align_up(specs[name].nbytes)
    if stride == 0:
        raise SharedSegmentError("Shared segment needs at least one block")
    total = stride * max(spec.count for spec in specs.values())
    return offsets, stride, total


def _current_device_index() -> int:
    """The accelerator this rank runs on, whatever vendor it is."""
    accelerator = getattr(torch, "accelerator", None)
    if accelerator is not None:
        try:
            return int(accelerator.current_device_index())
        except (RuntimeError, AttributeError):
            pass
    for backend in ("cuda", "npu"):
        module = getattr(torch, backend, None)
        if module is not None and module.is_available():
            return int(module.current_device())
    return 0


def _resolve_cpu_group(tp_group: Any) -> Any:
    """Accepts a vLLM GroupCoordinator or a plain process group.

    The blobs are exchanged as CPU byte tensors, so a gloo group is required; a
    coordinator exposes one as ``cpu_group``.
    """
    cpu_group = getattr(tp_group, "cpu_group", None)
    return cpu_group if cpu_group is not None else tp_group


def _all_gather_blob(blob: bytes, world_size: int, tp_group: Any) -> List[bytes]:
    local = torch.frombuffer(bytearray(blob), dtype=torch.uint8)
    gathered = [torch.empty_like(local) for _ in range(world_size)]
    dist.all_gather(gathered, local, group=_resolve_cpu_group(tp_group))
    return [bytes(tensor.numpy()) for tensor in gathered]


def _load_acl_host_get_device_pointer():
    """Lazy-load aclrtHostGetDevicePointer from libascendcl."""
    global _ACL_HOST_GET_DEVICE_POINTER
    if _ACL_HOST_GET_DEVICE_POINTER is not False and _ACL_HOST_GET_DEVICE_POINTER is not None:
        return _ACL_HOST_GET_DEVICE_POINTER
    if _ACL_HOST_GET_DEVICE_POINTER is False:
        return None

    candidates = []
    ascend_home = os.environ.get("ASCEND_HOME_PATH") or os.environ.get("ASCEND_TOOLKIT_HOME")
    if ascend_home:
        candidates.append(os.path.join(ascend_home, "lib64", "libascendcl.so"))
    candidates.extend(
        (
            "libascendcl.so",
            "/usr/local/Ascend/ascend-toolkit/latest/lib64/libascendcl.so",
            "/usr/local/Ascend/cann/lib64/libascendcl.so",
        )
    )
    for path in candidates:
        try:
            lib = ctypes.CDLL(path)
            fn = lib.aclrtHostGetDevicePointer
            fn.argtypes = [
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_void_p),
                ctypes.c_uint32,
            ]
            fn.restype = ctypes.c_int32
            _ACL_HOST_GET_DEVICE_POINTER = fn
            return fn
        except (OSError, AttributeError):
            continue
    _ACL_HOST_GET_DEVICE_POINTER = False
    return None


def _host_to_device_addr(host_addr: int) -> Optional[int]:
    """Return the HostRegister-mapped device VA for ``host_addr``, if any."""
    fn = _load_acl_host_get_device_pointer()
    if fn is None or host_addr == 0:
        return None
    device_ptr = ctypes.c_void_p()
    if fn(ctypes.c_void_p(host_addr), ctypes.byref(device_ptr), 0) != 0:
        return None
    if not device_ptr.value:
        return None
    return int(device_ptr.value)


def _contiguous_strides(shape: Sequence[int]) -> Tuple[int, ...]:
    strides: List[int] = [1] * len(shape)
    for i in range(len(shape) - 2, -1, -1):
        strides[i] = strides[i + 1] * shape[i + 1]
    return tuple(strides)


def _tensor_from_device_ptr(
    device_ptr: int,
    nbytes: int,
    shape: Tuple[int, ...],
    dtype: torch.dtype,
    device_id: int,
    holder: Any,
) -> torch.Tensor:
    """Wrap a HostRegister-mapped device VA as an NPU tensor."""
    try:
        import torch_npu
    except ImportError as exc:
        raise SharedSegmentError(
            "torch_npu is required to expose HostRegister-mapped device tensors"
        ) from exc

    device = torch.device(f"npu:{device_id}")
    storage = torch_npu._C._construct_storage_from_data_pointer(
        int(device_ptr), device, int(nbytes)
    )
    view = torch.empty(0, dtype=dtype, device=device)
    view.set_(storage, 0, shape, _contiguous_strides(shape))
    view._mooncake_segment = holder
    return view


def _tensor_from_host_addr(
    host_addr: int, nbytes: int, shape: Tuple[int, ...], dtype: torch.dtype, holder: Any
) -> torch.Tensor:
    buffer = (ctypes.c_int8 * nbytes).from_address(host_addr)
    view = torch.frombuffer(buffer, dtype=dtype).view(shape)
    view._mooncake_segment = holder
    return view


class SharedSegment:
    """Handle to one shared host segment. Alive for as long as the mapping is needed."""

    def __init__(
        self,
        segment: Any,
        specs: Dict[str, _BlockSpec],
        offsets: Dict[str, int],
        stride: int,
        device_id: int,
    ) -> None:
        self._segment = segment
        self._specs = specs
        self._offsets = offsets
        self._stride = stride
        self._device_id = device_id
        # Prefer the HostRegister-mapped device VA when present so callers can
        # register tensors(...).data_ptr() with TE as location=npu.
        self._device_base = _host_to_device_addr(int(segment.base_addr()))

    def tensors(self, block: str) -> List[torch.Tensor]:
        """Zero-copy views of every entry of ``block``, in index order.

        With ``mmap=True`` on Ascend the pages are HostRegister(MAPPED)'d, so
        each view is an NPU tensor whose ``data_ptr()`` is the mapped device VA.
        Register that pointer with Transfer Engine as ``location=\"npu\"`` for
        ROCE D2rH. Views share physical pages across the TP group: what one
        rank writes is what the others read. Each view holds a reference back
        to this segment.
        """
        spec = self._specs.get(block)
        if spec is None:
            raise SharedSegmentError(f"Unknown shared segment block {block!r}")
        host_base = int(self._segment.base_addr())
        offset = self._offsets[block]
        views = []
        for index in range(spec.count):
            byte_off = offset + index * self._stride
            if self._device_base is not None:
                view = _tensor_from_device_ptr(
                    self._device_base + byte_off,
                    spec.nbytes,
                    spec.shape,
                    spec.dtype,
                    self._device_id,
                    self,
                )
            else:
                view = _tensor_from_host_addr(
                    host_base + byte_off,
                    spec.nbytes,
                    spec.shape,
                    spec.dtype,
                    self,
                )
            views.append(view)
        return views

    def base_addr(self) -> int:
        return self._segment.base_addr()

    def total_size(self) -> int:
        return self._segment.size()


def create_shared_segment(
    name: str,
    blocks: Mapping[str, Mapping[str, Any]],
    world_size: int,
    rank_id: int,
    owner_rank: int = 0,
    device_id: Optional[int] = None,
    tp_group: Any = None,
    block_order: Optional[Sequence[str]] = None,
    mmap: bool = False,
) -> SharedSegment:
    """Creates a shared host segment and returns it ready to use.

    Every rank of the group must call this with an identical declaration; a
    disagreement is reported here rather than silently producing wrong reads.
    ``tp_group`` may be omitted only when ``world_size`` is 1. ``device_id``
    defaults to the rank's current accelerator, which is the device the segment
    is granted access to. Set ``mmap=True`` to share via POSIX shm + HostRegister
    instead of the platform VMM fabric path.
    """
    if not shared_segment_supported(mmap=mmap):
        raise SharedSegmentError(
            "This mooncake build has no mmap shared-segment backend"
            if mmap
            else "This mooncake build has no VMM backend for shared segments"
        )
    # Rank order decides the byte layout, so it must not depend on dict
    # iteration order differing between ranks.
    names = list(block_order) if block_order is not None else sorted(blocks)
    if set(names) != set(blocks):
        raise SharedSegmentError("block_order must list exactly the declared blocks")

    specs = {
        block_name: _parse_block(block_name, blocks[block_name]) for block_name in names
    }
    offsets, stride, total = _build_layout(specs, names)
    # Fold the layout into the name so C++'s fingerprint catches peers that
    # disagree on count/size without exchanging the full declaration.
    tag = ";".join(f"{n}:{specs[n].count}x{specs[n].nbytes}" for n in names)
    device = _current_device_index() if device_id is None else device_id

    try:
        segment, blob = _CppSharedSegment.create(
            f"{name}|{tag}",
            total,
            world_size,
            rank_id,
            owner_rank,
            device,
            mmap,
        )
    except RuntimeError as exc:
        raise SharedSegmentError(str(exc)) from exc

    try:
        if world_size == 1:
            blobs = [blob]
        elif tp_group is None:
            raise SharedSegmentError("tp_group is required when world_size > 1")
        else:
            blobs = _all_gather_blob(blob, world_size, tp_group)
        segment.complete(blobs)
    except RuntimeError as exc:
        raise SharedSegmentError(str(exc)) from exc
    return SharedSegment(segment, specs, offsets, stride, device)
