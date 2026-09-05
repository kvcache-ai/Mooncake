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
        comm_group=tp_group,
        mmap=False,  # Ascend VMM; tensors() are NPU views of the SVM VA
    )
    k_caches = seg.tensors("k")
"""

from __future__ import annotations

import ctypes
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

try:
    import torch
    import torch.distributed as dist
except ModuleNotFoundError as exc:
    if exc.name != "torch":
        raise
    torch = None
    dist = None

from .engine import SharedSegment as _CppSharedSegment

__all__ = [
    "SharedSegment",
    "SharedSegmentError",
    "create_shared_segment",
    "shared_segment_supported",
]

_ALIGN = 4096


class SharedSegmentError(RuntimeError):
    """Raised when a shared segment cannot be created or addressed."""


def _require_torch() -> Any:
    if torch is None:
        raise SharedSegmentError(
            "PyTorch is required to create shared-segment tensor views; "
            "install mooncake-transfer-engine[hardware]"
        )
    return torch


def create_shared_segment(
    name: str,
    blocks: Mapping[str, Mapping[str, Any]],
    world_size: int,
    rank_id: int,
    owner_rank: int = 0,
    device_id: int | None = None,
    comm_group: Any = None,
    mmap: bool = True,
    host_register: bool = False,
    *,
    tp_group: Any = None,
) -> SharedSegment:
    """Creates a shared host segment and returns it ready to use.

    Every rank of the group must call this with an identical declaration; a
    disagreement is reported here rather than silently producing wrong reads.
    ``comm_group`` is the torch process group (or vLLM GroupCoordinator) used
    to all-gather export blobs; it may be omitted only when ``world_size`` is
    1. ``tp_group`` is a deprecated alias of ``comm_group``. ``device_id``
    defaults to the rank's current accelerator.

    ``mmap`` (default True) uses POSIX shm. Set ``mmap=False`` for the platform
    VMM fabric path. ``host_register`` (default False) HostRegister's mmap pages
    for ``device_id`` so ``tensors().data_ptr()`` is a device VA suitable for TE
    ``location=\"npu\"`` ROCE D2rH; requires ``mmap=True``. Ascend VMM
    (``mmap=False``) also exposes NPU tensors: the host SVM VA is already
    device-accessible after ``MemSetAccess`` (owner ``MallocMem``, peer
    ``ImportAndMap``), so ``tensors().data_ptr()`` is that same VA.
    """
    comm_group = _select_comm_group(comm_group, tp_group)
    if host_register and not mmap:
        raise SharedSegmentError("host_register requires mmap=True")
    _require_torch()
    if not shared_segment_supported(mmap=mmap, host_register=host_register):
        if not mmap:
            raise SharedSegmentError(
                "This mooncake build has no VMM backend for shared segments"
            )
        if host_register:
            raise SharedSegmentError(
                "This mooncake build cannot HostRegister shared-segment pages"
            )
        raise SharedSegmentError(
            "This mooncake build has no mmap shared-segment backend"
        )

    # Sorted names keep the byte layout identical across ranks regardless of
    # dict iteration order.
    names = sorted(blocks)
    specs = {
        block_name: _parse_block(block_name, blocks[block_name]) for block_name in names
    }
    offsets, stride, total = _build_layout(specs, names)
    # Fold the layout into the name so C++'s fingerprint catches peers that
    # disagree on count, shape, dtype, or size without exchanging the full
    # declaration.
    tag = ";".join(_block_layout_tag(n, specs[n]) for n in names)
    device = _current_device_index() if device_id is None else device_id

    segment = None
    blob = b""
    create_error = ""
    try:
        segment, blob = _CppSharedSegment.create(
            f"{name}|{tag}",
            total,
            world_size,
            rank_id,
            owner_rank,
            device,
            mmap,
            host_register,
        )
    except RuntimeError as exc:
        create_error = str(exc)

    _raise_if_any_rank_failed(create_error, world_size, rank_id, comm_group, "create")
    if segment is None:
        raise SharedSegmentError("Shared segment create returned no segment")

    complete_error = ""
    try:
        if world_size == 1:
            blobs = [blob]
        else:
            blobs = _all_gather_blob(blob, world_size, comm_group)
        segment.complete(blobs)
    except (RuntimeError, SharedSegmentError) as exc:
        complete_error = str(exc)

    _raise_if_any_rank_failed(
        complete_error, world_size, rank_id, comm_group, "complete"
    )
    return SharedSegment(segment, specs, offsets, stride, device)


class SharedSegment:
    """Handle to one shared host segment.

    The handle must remain alive for as long as its mapping is needed.
    """

    def __init__(
        self,
        segment: Any,
        specs: dict[str, _BlockSpec],
        offsets: dict[str, int],
        stride: int,
        device_id: int,
    ) -> None:
        self._segment = segment
        self._specs = specs
        self._offsets = offsets
        self._stride = stride
        self._device_id = device_id
        # Device-accessible VA when present: HostRegister device ptr (mmap), or
        # the host SVM VA itself after MemSetAccess (Ascend VMM).
        self._device_base = int(segment.device_addr()) or None

    def tensors(self, block: str) -> list[torch.Tensor]:
        """Zero-copy views of every entry of ``block``, in index order.

        On Ascend, ``host_register=True`` (mmap) and ``mmap=False`` (VMM) both
        yield NPU tensors whose ``data_ptr()`` can be registered with Transfer
        Engine as ``location=\"npu\"`` for ROCE D2rH. mmap without HostRegister
        yields host CPU tensors. Views share physical pages across the TP
        group. Each view holds a reference back to this segment.
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


def shared_segment_supported(mmap: bool = True, host_register: bool = False) -> bool:
    return _CppSharedSegment.supported(mmap, host_register)


@dataclass(frozen=True)
class _BlockSpec:
    count: int
    shape: tuple[int, ...]
    dtype: torch.dtype
    nbytes: int


def _block_layout_tag(name: str, spec: _BlockSpec) -> str:
    shape = "x".join(str(dim) for dim in spec.shape)
    return f"{name}:{spec.count}:{shape}:{spec.dtype}:{spec.nbytes}"


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

    element_size = _require_torch().empty(0, dtype=dtype).element_size()
    nbytes = element_size
    for dim in shape:
        nbytes *= dim
    return _BlockSpec(count=count, shape=shape, dtype=dtype, nbytes=nbytes)


def _build_layout(
    specs: dict[str, _BlockSpec], names: Sequence[str]
) -> tuple[dict[str, int], int, int]:
    """Returns (offsets_by_name, group_stride, total_size)."""
    stride = 0
    offsets: dict[str, int] = {}
    for name in names:
        offsets[name] = stride
        stride += _align_up(specs[name].nbytes)
    if stride == 0:
        raise SharedSegmentError("Shared segment needs at least one block")
    total = stride * max(spec.count for spec in specs.values())
    return offsets, stride, total


def _current_device_index() -> int:
    """The accelerator this rank runs on, whatever vendor it is."""
    torch_module = _require_torch()
    accelerator = getattr(torch_module, "accelerator", None)
    if accelerator is not None:
        try:
            return int(accelerator.current_device_index())
        except (RuntimeError, AttributeError):
            pass
    for backend in ("npu",):
        module = getattr(torch_module, backend, None)
        if module is not None and module.is_available():
            return int(module.current_device())
    return 0


def _select_comm_group(comm_group: Any, tp_group: Any) -> Any:
    """``tp_group`` is a deprecated alias of ``comm_group``."""
    if tp_group is None:
        return comm_group
    if comm_group is None or comm_group is tp_group:
        return tp_group
    raise SharedSegmentError(
        "pass only comm_group; tp_group is a deprecated alias of comm_group"
    )


def _resolve_cpu_group(comm_group: Any) -> Any:
    """Accepts a vLLM GroupCoordinator or a plain process group.

    The blobs are exchanged as CPU byte tensors, so a gloo group is required; a
    coordinator exposes one as ``cpu_group``.
    """
    cpu_group = getattr(comm_group, "cpu_group", None)
    return cpu_group if cpu_group is not None else comm_group


def _raise_if_any_rank_failed(
    local_error: str,
    world_size: int,
    rank_id: int,
    comm_group: Any,
    phase: str,
) -> None:
    """Makes every rank leave a failed collective phase with the same error."""
    if world_size == 1:
        if local_error:
            raise SharedSegmentError(local_error)
        return
    if comm_group is None:
        raise SharedSegmentError("comm_group is required when world_size > 1")

    failures: list[tuple[int, str] | None] = [None] * world_size
    local_failure = (rank_id, local_error) if local_error else None
    dist.all_gather_object(
        failures, local_failure, group=_resolve_cpu_group(comm_group)
    )
    errors = [failure for failure in failures if failure is not None]
    if errors:
        detail = "; ".join(
            f"rank {failed_rank}: {message}" for failed_rank, message in errors
        )
        raise SharedSegmentError(f"Shared segment {phase} failed: {detail}")


def _all_gather_blob(blob: bytes, world_size: int, comm_group: Any) -> list[bytes]:
    torch_module = _require_torch()
    local = torch_module.frombuffer(bytearray(blob), dtype=torch_module.uint8)
    gathered = [torch_module.empty_like(local) for _ in range(world_size)]
    dist.all_gather(gathered, local, group=_resolve_cpu_group(comm_group))
    return [bytes(tensor.numpy()) for tensor in gathered]


def _contiguous_strides(shape: Sequence[int]) -> tuple[int, ...]:
    strides: list[int] = [1] * len(shape)
    for i in range(len(shape) - 2, -1, -1):
        strides[i] = strides[i + 1] * shape[i + 1]
    return tuple(strides)


def _tensor_from_device_ptr(
    device_ptr: int,
    nbytes: int,
    shape: tuple[int, ...],
    dtype: torch.dtype,
    device_id: int,
    holder: Any,
) -> torch.Tensor:
    """Wrap a device-accessible VA (HostRegister or Ascend VMM) as an NPU tensor."""
    torch_module = _require_torch()
    try:
        import torch_npu
    except ImportError as exc:
        raise SharedSegmentError(
            "torch_npu is required to expose device-mapped shared-segment tensors"
        ) from exc

    device = torch_module.device(f"npu:{device_id}")
    storage = torch_npu._C._construct_storage_from_data_pointer(
        int(device_ptr), device, int(nbytes)
    )
    view = torch_module.empty(0, dtype=dtype, device=device)
    view.set_(storage, 0, shape, _contiguous_strides(shape))
    view._mooncake_segment = holder
    return view


def _tensor_from_host_addr(
    host_addr: int, nbytes: int, shape: tuple[int, ...], dtype: torch.dtype, holder: Any
) -> torch.Tensor:
    buffer = (ctypes.c_int8 * nbytes).from_address(host_addr)
    view = _require_torch().frombuffer(buffer, dtype=dtype).view(shape)
    view._mooncake_segment = holder
    return view
