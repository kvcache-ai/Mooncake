import os
import warnings
from typing import Any, List, Optional, Sequence, Tuple, Union

import torch
import torch.distributed as dist

from .mooncake_ep_buffer import (
    EventOverlap,
    _device_type,
    _native_current_stream_ptr,
    _synchronize_device,
    _wait_native_event_on_current_stream,
)
from .mooncake_ep_tcpstore import TCPStoreCollectives


_VALID_TRANSPORTS = {"auto", "ibgda", "nccl"}
_TRANSPORT_TO_CODE = {"auto": 0, "ibgda": 1, "nccl": 2}
_CODE_TO_TRANSPORT = {code: name for name, code in _TRANSPORT_TO_CODE.items()}


def _requested_transport(transport: str) -> str:
    """Return the normalized user request, including the rollout override."""
    requested = transport.strip().lower()
    if requested == "auto":
        requested = os.getenv("MOONCAKE_EP_TRANSPORT", "auto").strip().lower()
    if requested not in _VALID_TRANSPORTS:
        choices = ", ".join(sorted(_VALID_TRANSPORTS))
        raise ValueError(
            f"ElasticBuffer transport must be one of {choices}, got {requested!r}"
        )
    return requested


def _ep_device_type() -> str:
    """ "xpu" for an XPU build of the native module, else "cuda" -- also when it
    is not built, so the pure-Python helpers work without it."""
    try:
        return _device_type()
    except ImportError:
        return "cuda"


def _requested_xpu_transport(transport: str) -> str:
    """XPU has one transport setting, 'ipc': Level Zero IPC, and RDMA
    through a host proxy to remote ranks."""
    requested = transport.strip().lower()
    if requested == "auto":
        requested = os.getenv("MOONCAKE_EP_TRANSPORT", "auto").strip().lower()
    if requested not in {"auto", "ipc"}:
        raise ValueError(
            "ElasticBuffer on XPU supports only transport 'ipc' (or 'auto'), "
            f"got {requested!r}"
        )
    return "ipc"


class _XpuRdmaError(RuntimeError):
    """Raised on every rank when XPU ranks on other nodes cannot connect."""


def _current_device_index() -> int:
    if _ep_device_type() == "xpu":
        return torch.xpu.current_device()
    return torch.cuda.current_device()


def _nccl_topology_supported(
    num_ranks: int,
    num_rdma_ranks: int,
    num_nvlink_ranks: int,
    allow_hybrid_mode: bool,
) -> bool:
    """Whether the compiled NCCL kernels cover the inferred EP topology."""
    if num_ranks != num_rdma_ranks * num_nvlink_ranks:
        return False
    if num_rdma_ranks == 1:
        return num_nvlink_ranks in {2, 8}
    if not allow_hybrid_mode:
        return False
    if num_rdma_ranks == 2:
        return num_nvlink_ranks in {4, 8}
    return num_rdma_ranks == 4 and num_nvlink_ranks == 4


def _select_transport(
    requested: str,
    nccl_available: bool,
    num_ranks: int,
    num_rdma_ranks: int,
    num_nvlink_ranks: int,
    allow_hybrid_mode: bool,
) -> str:
    """Prefer NCCL for auto mode without breaking unsupported deployments."""
    if requested != "auto":
        return requested
    if nccl_available and _nccl_topology_supported(
        num_ranks,
        num_rdma_ranks,
        num_nvlink_ranks,
        allow_hybrid_mode,
    ):
        return "nccl"
    return "ibgda"


def _resolve_transport_consensus(
    rank_states: Sequence[Tuple[str, bool, Optional[str]]],
) -> str:
    """Resolve rank-local NCCL readiness into one group-wide choice."""
    if not rank_states:
        raise RuntimeError(
            "cannot select an ElasticBuffer transport for an empty group"
        )

    requested_modes = {requested for requested, _, _ in rank_states}
    if len(requested_modes) != 1:
        details = ", ".join(
            f"rank {rank}={requested!r}"
            for rank, (requested, _, _) in enumerate(rank_states)
        )
        raise RuntimeError(
            "ElasticBuffer transport requests differ across process-group ranks: "
            f"{details}. Pass the same transport argument and "
            "MOONCAKE_EP_TRANSPORT setting to every rank."
        )

    required_modes = {
        required for _, _, required in rank_states if required is not None
    }
    if len(required_modes) > 1:
        details = ", ".join(
            f"rank {rank}={required!r}"
            for rank, (_, _, required) in enumerate(rank_states)
            if required is not None
        )
        raise RuntimeError(
            "ElasticBuffer transport requirements differ across process-group "
            f"ranks: {details}"
        )

    requested = rank_states[0][0]
    required = next(iter(required_modes), None)
    if required is not None and requested not in {"auto", required}:
        raise RuntimeError(
            f"ElasticBuffer transport {required!r} is required by an existing "
            f"buffer generation, but the group requested {requested!r}"
        )

    unready_ranks = [
        rank for rank, (_, nccl_ready, _) in enumerate(rank_states) if not nccl_ready
    ]
    if requested == "nccl" or required == "nccl":
        if unready_ranks:
            ranks = ", ".join(str(rank) for rank in unready_ranks)
            raise RuntimeError(
                "transport='nccl' requires NCCL Device API support and a "
                "supported NCCL kernel topology on every process-group rank; "
                f"requirements are not met on ranks: {ranks}"
            )
        return "nccl"
    if required == "ibgda":
        return "ibgda"

    # In auto mode, a single rank that cannot use NCCL makes the whole group
    # choose IBGDA. No rank enters transport-specific bootstrap until this
    # collective decision has completed.
    if requested == "auto" and not unready_ranks:
        return "nccl"
    return "ibgda"


def _resolve_nccl_membership_masks(rank_masks: Sequence[Sequence[int]]) -> None:
    """Require one consistent, fully restored fixed-world membership view."""
    if not rank_masks:
        raise RuntimeError("cannot reconfigure NCCL for an empty group")
    num_ranks = len(rank_masks)
    malformed_observers = [
        rank for rank, mask in enumerate(rank_masks) if len(mask) != num_ranks
    ]
    inactive_slots = sorted(
        {
            slot
            for mask in rank_masks
            for slot, active in enumerate(mask[:num_ranks])
            if int(active) != 1
        }
    )
    if malformed_observers or inactive_slots:
        details = []
        if inactive_slots:
            details.append(f"inactive or inconsistent logical slots: {inactive_slots}")
        if malformed_observers:
            details.append(f"invalid membership views on ranks: {malformed_observers}")
        raise RuntimeError(
            "NCCL ElasticBuffer reconfiguration requires the complete fixed "
            "logical rank set to be restored; " + "; ".join(details)
        )


def _bootstrap_collective_device(group: dist.ProcessGroup) -> str:
    """Choose metadata tensors independently of the GPU EP data path."""
    backend = str(dist.get_backend(group)).lower()
    return "cpu" if backend in {"gloo", "mooncake-cpu"} else _ep_device_type()


def _select_transport_for_group(
    group: dist.ProcessGroup,
    requested: str,
    nccl_available: bool,
    num_ranks: int,
    num_rdma_ranks: int,
    num_nvlink_ranks: int,
    allow_hybrid_mode: bool,
    required_transport: Optional[str] = None,
) -> str:
    """Agree on one transport before starting transport-specific bootstrap."""
    if required_transport not in {None, "ibgda", "nccl"}:
        raise ValueError(f"invalid required transport: {required_transport!r}")

    local_auto_choice = _select_transport(
        "auto",
        nccl_available,
        num_ranks,
        num_rdma_ranks,
        num_nvlink_ranks,
        allow_hybrid_mode,
    )
    collective_device = _bootstrap_collective_device(group)
    group_size = group.size()
    local_state = torch.tensor(
        [
            _TRANSPORT_TO_CODE[requested],
            int(local_auto_choice == "nccl"),
            _TRANSPORT_TO_CODE[required_transport or "auto"],
            num_ranks,
        ],
        dtype=torch.int32,
        device=collective_device,
    )
    gathered_states = [torch.full_like(local_state, -1) for _ in range(group_size)]
    dist.all_gather(gathered_states, local_state, group=group)

    rank_states = []
    mismatched_world_sizes = []
    for rank, state in enumerate(gathered_states):
        values = [int(value) for value in state.cpu().tolist()]
        if values == [-1, -1, -1, -1]:
            rank_states.append((requested, False, None))
            continue
        requested_code, rank_nccl_ready, required_code, rank_world_size = values
        if rank_world_size != group_size:
            mismatched_world_sizes.append(f"rank {rank}={rank_world_size}")
        if (
            requested_code not in _CODE_TO_TRANSPORT
            or required_code not in _CODE_TO_TRANSPORT
        ):
            raise RuntimeError(
                "received an invalid ElasticBuffer transport state during "
                "group consensus"
            )
        required_name = _CODE_TO_TRANSPORT[required_code]
        rank_states.append(
            (
                _CODE_TO_TRANSPORT[requested_code],
                bool(rank_nccl_ready),
                None if required_name == "auto" else required_name,
            )
        )
    # Exchange the expected world size in this fixed-width collective, before
    # the membership exchange uses num_ranks-sized tensors. Survivors retain
    # their original size while a new buffer sees the current PG size; a local
    # check alone could leave replacements entering a different collective.
    if mismatched_world_sizes:
        raise RuntimeError(
            "ElasticBuffer requires a fixed logical world size during bootstrap; "
            f"the process group has {group_size} ranks, but buffers expect "
            + ", ".join(mismatched_world_sizes)
            + ". Restore the original logical rank set or recreate all buffers "
            "for the new group size."
        )
    return _resolve_transport_consensus(rank_states)


def _using_musa_backend() -> bool:
    return os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {
        "1",
        "ON",
        "TRUE",
        "YES",
    }


def _dist_barrier(group: dist.ProcessGroup) -> None:
    if _using_musa_backend():
        dist.barrier(group=group, device_ids=[torch.cuda.current_device()])
    else:
        dist.barrier(group=group)


def _ceil_div(x: int, y: int) -> int:
    return (x + y - 1) // y


def _align(x: int, alignment: int) -> int:
    return _ceil_div(x, alignment) * alignment


class EPHandle:
    """
    Official DeepEP elastic-compatible communication handle.

    The field names and semantics intentionally follow the official DeepEP elastic
    handle contract so that model code can select Mooncake ElasticBuffer without
    switching back to the legacy Buffer tuple handle.  Mooncake stores the native
    legacy handle as an implementation detail while the elastic kernels are being
    wired to the Device API backend.
    """

    def __init__(
        self,
        do_expand: bool,
        num_experts: int,
        expert_alignment: int,
        num_max_tokens_per_rank: int,
        num_sms: int,
        topk_idx: torch.Tensor,
        num_recv_tokens_per_expert_list: List[int],
        psum_num_recv_tokens_per_scaleup_rank: torch.Tensor,
        psum_num_recv_tokens_per_expert: torch.Tensor,
        recv_src_metadata: torch.Tensor,
        dst_buffer_slot_idx: torch.Tensor,
        token_metadata_at_forward: Optional[torch.Tensor],
        channel_linked_list: Optional[torch.Tensor],
        native_handle: Optional[Tuple[Any, ...]] = None,
        generation: Optional[Tuple[int, ...]] = None,
    ) -> None:
        assert topk_idx is not None
        self.do_expand = do_expand
        self.num_experts = num_experts
        self.expert_alignment = expert_alignment
        self.num_max_tokens_per_rank = num_max_tokens_per_rank
        self.num_sms = num_sms
        self.topk_idx = topk_idx
        self.psum_num_recv_tokens_per_scaleup_rank = (
            psum_num_recv_tokens_per_scaleup_rank
        )
        self.psum_num_recv_tokens_per_expert = psum_num_recv_tokens_per_expert
        self.num_recv_tokens_per_expert_list = num_recv_tokens_per_expert_list
        self.recv_src_metadata = recv_src_metadata
        self.dst_buffer_slot_idx = dst_buffer_slot_idx
        self.token_metadata_at_forward = token_metadata_at_forward
        self.channel_linked_list = channel_linked_list
        self.native_handle = native_handle
        self._generation = generation

        # Same convention as DeepEP: without a CPU sync this is an inferred upper
        # bound; after CPU sync it tracks the actual received-token count.
        self.num_recv_tokens = int(recv_src_metadata.shape[0])


class ElasticBuffer:
    """
    Official DeepEP elastic EP API backed by Mooncake EP transports.

    Public API source of truth: official DeepEP `ElasticBuffer`.  The implementation is
    deliberately separate from Mooncake's legacy `Buffer` API, while reusing the
    existing Mooncake Device API transport/bootstrap path for the native data
    movement backend.

    The default ``transport="auto"`` prefers NCCL when the extension contains
    NCCL Device API support and the current topology has a compiled NCCL kernel.
    It otherwise retains the IPC + IBGDA backend. Set ``transport="ibgda"`` or
    ``MOONCAKE_EP_TRANSPORT=ibgda`` to force the legacy backend during rollout.
    """

    # Mirrors DeepEP's fixed workspace assumptions closely enough for sizing and
    # keeping one reusable buffer for all elastic EP shapes.
    _NUM_MAX_RANKS = 1024
    _NUM_MAX_EXPERTS = 2048
    _NUM_MAX_CHANNELS = 8 * 160
    _NUM_BARRIER_TAGS = 16
    _NUM_MAX_INFLIGHT_AGRS = 32
    _NCCL_UNIQUE_ID_WORDS = 32
    _store_collectives: Optional[TCPStoreCollectives] = None

    def __init__(
        self,
        group: dist.ProcessGroup,
        num_bytes: Optional[int] = None,
        num_max_tokens_per_rank: int = 0,
        hidden: int = 0,
        num_topk: int = 0,
        use_fp8_dispatch: bool = False,
        deterministic: bool = False,
        allow_hybrid_mode: bool = True,
        allow_multiple_reduction: bool = True,
        prefer_overlap_with_compute: bool = True,
        sl_idx: int = 3,
        num_allocated_qps: int = 0,
        num_cpu_timeout_secs: int = 300,
        num_gpu_timeout_secs: int = 100,
        explicitly_destroy: bool = False,
        transport: str = "auto",
    ) -> None:
        if not allow_multiple_reduction:
            raise NotImplementedError(
                "Mooncake ElasticBuffer currently supports only "
                "allow_multiple_reduction=True"
            )
        self.group = group
        self.rank_idx = group.rank()
        self.num_ranks = group.size()
        self._store_collectives: Optional[TCPStoreCollectives] = None
        on_xpu = _ep_device_type() == "xpu"
        self._device_index = _current_device_index()
        self.requested_transport = (
            _requested_xpu_transport(transport)
            if on_xpu
            else _requested_transport(transport)
        )
        self.allow_hybrid_mode = allow_hybrid_mode
        self.allow_multiple_reduction = allow_multiple_reduction
        self.prefer_overlap_with_compute = prefer_overlap_with_compute
        self.deterministic = deterministic
        self.sl_idx = int(os.getenv("EP_OVERRIDE_RDMA_SL", sl_idx))
        self.num_allocated_qps = num_allocated_qps
        self.num_cpu_timeout_secs = num_cpu_timeout_secs
        self.num_gpu_timeout_secs = num_gpu_timeout_secs
        self.explicitly_destroy = explicitly_destroy

        self.num_max_tokens_per_rank = num_max_tokens_per_rank
        self.hidden = hidden
        self.num_topk = num_topk
        self.use_fp8_dispatch = use_fp8_dispatch

        if num_bytes is None:
            num_bytes = self.get_buffer_size_hint(
                group,
                num_max_tokens_per_rank,
                hidden,
                num_topk=num_topk,
                use_fp8_dispatch=use_fp8_dispatch,
                allow_hybrid_mode=allow_hybrid_mode,
                allow_multiple_reduction=allow_multiple_reduction,
            )
        self.num_bytes = num_bytes

        (
            self.num_scaleout_ranks,
            self.num_scaleup_ranks,
        ) = self._calculate_logical_domain_size(group, allow_hybrid_mode)
        self.scaleout_rank_idx = self.rank_idx // self.num_scaleup_ranks
        self.scaleup_rank_idx = self.rank_idx % self.num_scaleup_ranks
        self.num_rdma_ranks, self.num_nvlink_ranks = (
            self._calculate_physical_domain_size(group)
        )

        self.backend = group
        self._generation: Optional[Tuple[int, ...]] = None

        # Native Mooncake transport/runtime.  This keeps the legacy Buffer ABI
        # untouched while giving ElasticBuffer users a dedicated native entrypoint.
        from mooncake import ep

        if on_xpu:
            self.transport = "ipc"
        else:
            self.transport = _select_transport_for_group(
                self.group,
                self.requested_transport,
                ep.has_nccl_device_support(),
                self.num_ranks,
                self.num_rdma_ranks,
                self.num_nvlink_ranks,
                self.allow_hybrid_mode,
            )

        nccl_unique_id = []
        if self.transport == "nccl":
            self._require_full_nccl_membership()
            nccl_unique_id = self._exchange_nccl_unique_id(ep)
            self._generation = tuple(nccl_unique_id)
        self.runtime = ep.ElasticBuffer(
            self.rank_idx,
            self.num_ranks,
            num_bytes,
            num_max_tokens_per_rank,
            hidden,
            num_topk,
            use_fp8_dispatch,
            deterministic,
            allow_hybrid_mode,
            allow_multiple_reduction,
            prefer_overlap_with_compute,
            self.sl_idx,
            num_allocated_qps,
            num_cpu_timeout_secs,
            num_gpu_timeout_secs,
            self.transport,
            nccl_unique_id,
            # The XPU runtime allocates on, and orders against, this queue.
            **({"stream_ptr": _native_current_stream_ptr()} if on_xpu else {}),
        )
        # NCCL's LSA team is the transport authority for the local domain; it
        # may differ from the CUDA-visible-device heuristic used before native
        # initialization. Keep the public topology fields in sync with the
        # exact native launch topology for both backends.
        self._refresh_topology_from_runtime()
        self._connect_native()
        if on_xpu:
            # The XPU runtime learns the nodes while connecting (init_rdma).
            self._refresh_topology_from_runtime()
        # The native elastic kernels currently use fixed communicator
        # membership. Reuse this compatibility argument instead of allocating
        # and filling a CUDA tensor for every dispatch and combine call.
        self._active_ranks = torch.ones(
            self.num_ranks, dtype=torch.int32, device=_ep_device_type()
        )

        _synchronize_device()
        _dist_barrier(group)
        _synchronize_device()

    @classmethod
    def create_elastic(
        cls,
        rank: int,
        num_ranks: int,
        tcp_store_group: dist.TCPStore,
        *,
        store_namespace: str,
        num_bytes: Optional[int] = None,
        num_max_tokens_per_rank: int = 0,
        hidden: int = 0,
        num_topk: int = 0,
        use_fp8_dispatch: bool = False,
        deterministic: bool = False,
        allow_hybrid_mode: bool = True,
        allow_multiple_reduction: bool = True,
        prefer_overlap_with_compute: bool = True,
        sl_idx: int = 3,
        num_allocated_qps: int = 0,
        num_cpu_timeout_secs: int = 300,
        num_gpu_timeout_secs: int = 100,
        explicitly_destroy: bool = False,
        transport: str = "auto",
        store_timeout_secs: int = 300,
    ) -> "ElasticBuffer":
        """Create a fixed-world buffer without initializing a ProcessGroup.

        Every rank in one buffer instance must use the same ``store_namespace``.
        Choose a fresh namespace for every other buffer instance and process
        restart; the Store protocol never reuses keys from an earlier instance.
        All ranks must call bootstrap, barriers, updates, and NCCL destruction in
        the same order. This path does not support membership changes or rank
        failure recovery.
        """
        if _ep_device_type() == "xpu":
            raise NotImplementedError(
                "ElasticBuffer.create_elastic is not supported on XPU yet; "
                "use ElasticBuffer(group, ...)"
            )
        if not allow_multiple_reduction:
            raise NotImplementedError(
                "Mooncake ElasticBuffer currently supports only "
                "allow_multiple_reduction=True"
            )
        if num_ranks <= 0 or not 0 <= rank < num_ranks:
            raise ValueError("rank must be within the positive Store world size.")

        self = cls.__new__(cls)
        self.group = None
        self.rank_idx = rank
        self.num_ranks = num_ranks
        self._device_index = torch.cuda.current_device()
        self.requested_transport = _requested_transport(transport)
        self.allow_hybrid_mode = allow_hybrid_mode
        self.allow_multiple_reduction = allow_multiple_reduction
        self.prefer_overlap_with_compute = prefer_overlap_with_compute
        self.deterministic = deterministic
        self.sl_idx = int(os.getenv("EP_OVERRIDE_RDMA_SL", sl_idx))
        self.num_allocated_qps = num_allocated_qps
        self.num_cpu_timeout_secs = num_cpu_timeout_secs
        self.num_gpu_timeout_secs = num_gpu_timeout_secs
        self.explicitly_destroy = explicitly_destroy
        self.num_max_tokens_per_rank = num_max_tokens_per_rank
        self.hidden = hidden
        self.num_topk = num_topk
        self.use_fp8_dispatch = use_fp8_dispatch
        self._store_collectives = TCPStoreCollectives(
            tcp_store_group,
            rank,
            num_ranks,
            store_namespace,
            store_timeout_secs,
        )

        if num_bytes is None:
            num_bytes = cls.get_buffer_size_hint(
                num_ranks,
                num_max_tokens_per_rank,
                hidden,
                num_topk=num_topk,
                use_fp8_dispatch=use_fp8_dispatch,
                allow_hybrid_mode=allow_hybrid_mode,
                allow_multiple_reduction=allow_multiple_reduction,
            )
        self.num_bytes = num_bytes
        (
            self.num_scaleout_ranks,
            self.num_scaleup_ranks,
        ) = cls._calculate_logical_domain_size(num_ranks, allow_hybrid_mode)
        self.scaleout_rank_idx = rank // self.num_scaleup_ranks
        self.scaleup_rank_idx = rank % self.num_scaleup_ranks
        self.num_rdma_ranks, self.num_nvlink_ranks = (
            cls._calculate_physical_domain_size(num_ranks)
        )
        self.backend = None
        self._generation: Optional[Tuple[int, ...]] = None

        from mooncake import ep

        self.transport = self._select_store_transport(ep)
        nccl_unique_id = []
        if self.transport == "nccl":
            nccl_unique_id = self._exchange_nccl_unique_id_from_store(ep)
            self._generation = tuple(nccl_unique_id)
        self.runtime = ep.ElasticBuffer(
            rank,
            num_ranks,
            num_bytes,
            num_max_tokens_per_rank,
            hidden,
            num_topk,
            use_fp8_dispatch,
            deterministic,
            allow_hybrid_mode,
            allow_multiple_reduction,
            prefer_overlap_with_compute,
            self.sl_idx,
            num_allocated_qps,
            num_cpu_timeout_secs,
            num_gpu_timeout_secs,
            self.transport,
            nccl_unique_id,
        )
        self._refresh_topology_from_runtime()
        self._connect_native()
        self._active_ranks = torch.ones(
            self.num_ranks, dtype=torch.int32, device="cuda"
        )
        torch.cuda.synchronize()
        self._store_collectives.barrier("initialize")
        torch.cuda.synchronize()
        return self

    def _select_store_transport(
        self, ep: Any, required_transport: Optional[str] = None
    ) -> str:
        nccl_available = bool(ep.has_nccl_device_support())
        local_auto_choice = _select_transport(
            "auto",
            nccl_available,
            self.num_ranks,
            self.num_rdma_ranks,
            self.num_nvlink_ranks,
            self.allow_hybrid_mode,
        )
        states = self._store_collectives.all_gather(
            "transport-consensus",
            (
                self.requested_transport,
                local_auto_choice == "nccl",
                required_transport,
            ),
        )
        return _resolve_transport_consensus(states)

    def _active_ranks_mask(self) -> list:
        # `mooncake.pg.get_active_ranks` is a Mooncake PG helper and performs a
        # native static cast to MooncakeBackend. PyTorch exposes custom process
        # groups through its base ProcessGroup type, so query the registered
        # backend name instead of relying on the Python wrapper's type name.
        # Regular NCCL/Gloo groups treat every supplied rank as active.
        if "mooncake" not in str(dist.get_backend(self.backend)).lower():
            return [1] * self.num_ranks

        from mooncake.pg import get_active_ranks

        # Mooncake PG refreshes this CUDA mirror on its own stream. Wait for
        # that update before taking the host snapshot used for membership
        # agreement, including the replacement process's initial snapshot.
        if _ep_device_type() == "xpu":
            torch.xpu.synchronize(self._device_index)
        else:
            torch.cuda.synchronize(self._device_index)
        return get_active_ranks(self.backend).cpu().tolist()

    def _require_full_nccl_membership(self) -> None:
        """Agree that every fixed logical rank has rejoined before bootstrap."""
        try:
            active_mask = self._active_ranks_mask()
        except Exception:
            active_mask = []
        collective_device = _bootstrap_collective_device(self.group)
        local_mask = torch.full(
            (self.num_ranks,), -1, dtype=torch.int32, device=collective_device
        )
        # Mooncake PG masks cover max_group_size (or a larger user-provided
        # tensor), not just the logical ranks used by this fixed-world buffer.
        if len(active_mask) >= self.num_ranks:
            local_mask.copy_(
                torch.tensor(
                    active_mask[: self.num_ranks],
                    dtype=torch.int32,
                    device=collective_device,
                )
            )
        gathered_masks = [
            torch.full_like(local_mask, -1) for _ in range(self.num_ranks)
        ]
        dist.all_gather(gathered_masks, local_mask, group=self.group)
        _resolve_nccl_membership_masks(
            [rank_mask.cpu().tolist() for rank_mask in gathered_masks]
        )

    def _refresh_topology_from_runtime(self) -> None:
        self.num_rdma_ranks, self.num_nvlink_ranks = (
            self.runtime.get_physical_domain_size()
        )
        self.num_scaleout_ranks, self.num_scaleup_ranks = (
            self.runtime.get_logical_domain_size()
        )
        self.scaleout_rank_idx = self.rank_idx // self.num_scaleup_ranks
        self.scaleup_rank_idx = self.rank_idx % self.num_scaleup_ranks

    def _validate_handle_generation(self, handle: EPHandle) -> None:
        handle_generation = getattr(handle, "_generation", None)
        if self._generation is not None and handle_generation != self._generation:
            raise RuntimeError(
                "EPHandle belongs to an obsolete NCCL ElasticBuffer generation; "
                "dispatch again after update_ep_member()"
            )

    def _connect_native(self, is_update: bool = False) -> None:
        from mooncake import ep

        if self.transport == "nccl":
            return
        if self._store_collectives is not None:
            self._connect_native_from_store(ep, is_update)
            return

        if not bool(self.runtime.ibgda_disabled()):
            raddr, rkey = self.runtime.get_mr_info()
            raddr_tensor = torch.tensor([raddr], dtype=torch.int64, device="cuda")
            raddrs = [
                torch.empty(1, dtype=torch.int64, device="cuda")
                for _ in range(self.num_ranks)
            ]
            dist.all_gather(raddrs, raddr_tensor, self.group)
            raddrs_list = torch.cat(raddrs).tolist()

            rkey_tensor = torch.tensor([rkey], dtype=torch.int32, device="cuda")
            rkeys = [
                torch.empty(1, dtype=torch.int32, device="cuda")
                for _ in range(self.num_ranks)
            ]
            dist.all_gather(rkeys, rkey_tensor, self.group)
            rkeys_list = torch.cat(rkeys).tolist()

            all_to_all_size = ep.MAX_QP_COUNT // self.num_ranks
            if is_update:
                self.runtime.update_local_qpns()

            local_qpns = torch.tensor(
                self.runtime.get_local_qpns(), dtype=torch.int32, device="cuda"
            ).view(-1, all_to_all_size)
            remote_qpns = [
                torch.empty(all_to_all_size, dtype=torch.int32, device="cuda")
                for _ in range(self.num_ranks)
            ]
            dist.all_to_all(remote_qpns, list(torch.unbind(local_qpns)), self.group)
            peer_qpns = [remote_qpns[r].tolist() for r in range(self.num_ranks)]

            local_lids = torch.tensor(
                self.runtime.get_local_lids(), dtype=torch.int32, device="cuda"
            ).view(-1, all_to_all_size)
            remote_lids = [
                torch.empty(all_to_all_size, dtype=torch.int32, device="cuda")
                for _ in range(self.num_ranks)
            ]
            dist.all_to_all(remote_lids, list(torch.unbind(local_lids)), self.group)
            peer_lids = [remote_lids[r].tolist() for r in range(self.num_ranks)]

            subnet_prefix, interface_id = self.runtime.get_gid()
            subnet_prefix_tensor = torch.tensor(
                [subnet_prefix], dtype=torch.int64, device="cuda"
            )
            subnet_prefixes = [
                torch.empty(1, dtype=torch.int64, device="cuda")
                for _ in range(self.num_ranks)
            ]
            dist.all_gather(subnet_prefixes, subnet_prefix_tensor, self.group)
            subnet_prefixes_list = torch.cat(subnet_prefixes).tolist()

            interface_id_tensor = torch.tensor(
                [interface_id], dtype=torch.int64, device="cuda"
            )
            interface_ids = [
                torch.empty(1, dtype=torch.int64, device="cuda")
                for _ in range(self.num_ranks)
            ]
            dist.all_gather(interface_ids, interface_id_tensor, self.group)
            interface_ids_list = torch.cat(interface_ids).tolist()

            active_ranks_mask = self._active_ranks_mask()
            self.runtime.sync_ibgda_peers(
                raddrs_list,
                rkeys_list,
                peer_qpns,
                peer_lids,
                subnet_prefixes_list,
                interface_ids_list,
                active_ranks_mask,
            )

        try:
            local_handle_ints = self.runtime.get_ipc_handle()
            collective_device = _bootstrap_collective_device(self.group)
            local_handle_tensor = torch.tensor(
                local_handle_ints, dtype=torch.int32, device=collective_device
            )
            handles = [
                torch.empty(
                    len(local_handle_ints),
                    dtype=torch.int32,
                    device=collective_device,
                )
                for _ in range(self.num_ranks)
            ]
            dist.all_gather(handles, local_handle_tensor, self.group)
            remote_handles = [h.tolist() for h in handles]

            active_ranks_mask = self._active_ranks_mask()
            if _ep_device_type() == "xpu":
                self._connect_xpu_rdma(
                    remote_handles, active_ranks_mask, collective_device
                )
            # On XPU, the Level Zero IPC import.
            self.runtime.sync_nvlink_ipc_handles(remote_handles, active_ranks_mask)
        except _XpuRdmaError:
            raise
        except Exception as exc:
            if bool(self.runtime.ibgda_disabled()):
                raise RuntimeError(
                    f"[Rank {self.rank_idx}] Failed to exchange IPC handles "
                    "for ElasticBuffer and RDMA is disabled; native elastic "
                    "mode cannot continue safely."
                ) from exc
            warnings.warn(
                f"[Rank {self.rank_idx}] Failed to exchange IPC handles for ElasticBuffer: {exc}. "
                "Continuing with RDMA-only routing.",
                RuntimeWarning,
                stacklevel=2,
            )

    def _connect_native_from_store(self, ep: Any, is_update: bool) -> None:
        was_disabled = bool(self.runtime.ibgda_disabled())
        if is_update:
            self.runtime.update_local_qpns()
        ibgda_disabled = bool(self.runtime.ibgda_disabled())

        metadata = None
        max_qp_count = 0
        if not ibgda_disabled:
            raddr, rkey = self.runtime.get_mr_info()
            subnet_prefix, interface_id = self.runtime.get_gid()
            max_qp_count = int(ep.MAX_QP_COUNT)
            metadata = {
                "raddr": int(raddr),
                "rkey": int(rkey),
                "subnet_prefix": int(subnet_prefix),
                "interface_id": int(interface_id),
                "qpns": tuple(int(value) for value in self.runtime.get_local_qpns()),
                "lids": tuple(int(value) for value in self.runtime.get_local_lids()),
            }
        local_handle = tuple(int(value) for value in self.runtime.get_ipc_handle())
        peers = self._store_collectives.all_gather(
            "peer-bootstrap",
            (was_disabled, ibgda_disabled, metadata, max_qp_count, local_handle),
        )
        if (
            len({peer[0] for peer in peers}) != 1
            or len({peer[1] for peer in peers}) != 1
        ):
            raise RuntimeError(
                "ElasticBuffer ranks disagree on IBGDA readiness; native peer "
                "synchronization cannot safely continue."
            )
        was_disabled = peers[0][0]
        ibgda_disabled = peers[0][1]
        if is_update and not was_disabled and ibgda_disabled:
            raise RuntimeError(
                "ElasticBuffer IBGDA was disabled while refreshing local QPs; "
                "the fixed-world Store path cannot silently fall back."
            )

        if not ibgda_disabled:
            qp_limits = {peer[3] for peer in peers}
            if len(qp_limits) != 1:
                raise RuntimeError("ElasticBuffer ranks disagree on MAX_QP_COUNT.")
            expected_qps = next(iter(qp_limits)) // self.num_ranks * self.num_ranks
            if expected_qps <= 0 or any(
                len(peer[2]["qpns"]) != expected_qps
                or len(peer[2]["lids"]) != expected_qps
                for peer in peers
            ):
                raise RuntimeError(
                    "IBGDA QP/LID metadata must have matching USE_QP_COUNT "
                    "entries on every rank."
                )
            qps_per_rank = expected_qps // self.num_ranks
            peer_qpns = [
                list(
                    peer[2]["qpns"][
                        self.rank_idx * qps_per_rank : (self.rank_idx + 1)
                        * qps_per_rank
                    ]
                )
                for peer in peers
            ]
            peer_lids = [
                list(
                    peer[2]["lids"][
                        self.rank_idx * qps_per_rank : (self.rank_idx + 1)
                        * qps_per_rank
                    ]
                )
                for peer in peers
            ]
            self.runtime.sync_ibgda_peers(
                [peer[2]["raddr"] for peer in peers],
                [peer[2]["rkey"] for peer in peers],
                peer_qpns,
                peer_lids,
                [peer[2]["subnet_prefix"] for peer in peers],
                [peer[2]["interface_id"] for peer in peers],
                [1] * self.num_ranks,
            )
            disabled_after_sync = self._store_collectives.all_gather(
                "ibgda-peer-sync", bool(self.runtime.ibgda_disabled())
            )
            if any(disabled_after_sync):
                raise RuntimeError(
                    "ElasticBuffer native IBGDA peer synchronization disabled "
                    "the transport; the fixed-world Store path cannot silently "
                    "fall back."
                )

        handles = [list(peer[4]) for peer in peers]
        try:
            self.runtime.sync_nvlink_ipc_handles(handles, [1] * self.num_ranks)
        except Exception as exc:
            if ibgda_disabled:
                raise RuntimeError(
                    f"[Rank {self.rank_idx}] Failed to exchange IPC handles for "
                    "ElasticBuffer and RDMA is disabled; native elastic mode "
                    "cannot continue safely."
                ) from exc
            warnings.warn(
                f"[Rank {self.rank_idx}] Failed to exchange IPC handles for "
                f"ElasticBuffer: {exc}. Continuing with RDMA-only routing.",
                RuntimeWarning,
                stacklevel=2,
            )

    def _connect_xpu_rdma(
        self,
        handles: List[List[int]],
        active_ranks_mask: List[int],
        device: Union[str, torch.device],
    ) -> None:
        """Connect the ranks on other nodes over RDMA (XPU only).

        As in Buffer._connect_xpu_rdma, a rank is on another node if its IPC
        handle comes from another host or pid namespace (words 3 and 4 of the
        handle), or, with MC_EP_RANKS_PER_NODE=n, if rank // n differs. Unlike
        the Buffer, the ElasticBuffer has no fallback path: if any rank cannot
        connect, every rank raises.
        """
        per_node = int(os.environ.get("MC_EP_RANKS_PER_NODE", "0") or 0)
        if per_node > 0:
            nodes = [r // per_node for r in range(self.num_ranks)]
        else:
            nodes = [tuple(h[3:5]) for h in handles]
        remote = [int(node != nodes[self.rank_idx]) for node in nodes]
        # Every rank sees the same nodes, so all of them return here or none.
        if not any(remote):
            return

        def agree(error: str) -> None:
            ok = torch.tensor([int(not error)], dtype=torch.int32, device=device)
            dist.all_reduce(ok, op=dist.ReduceOp.MIN, group=self.group)
            if not ok.item():
                raise _XpuRdmaError(
                    f"[Rank {self.rank_idx}] ElasticBuffer ranks span several "
                    "nodes, which XPU reaches over RDMA, and "
                    + (error or "another rank could not connect")
                )

        info: List[int] = []
        error = ""
        if not self.runtime.rdma_available():
            error = (
                "this build lacks the XPU verbs path (rdma-core with "
                "ibv_reg_dmabuf_mr)"
            )
        else:
            try:
                info = list(self.runtime.init_rdma(remote))
            except Exception as e:
                error = f"its setup failed: {e}"
        agree(error)
        local = torch.tensor(info, dtype=torch.int32, device=device)
        infos = [torch.empty_like(local) for _ in range(self.num_ranks)]
        dist.all_gather(infos, local, group=self.group)
        try:
            self.runtime.connect_rdma([t.tolist() for t in infos], active_ranks_mask)
        except Exception as e:
            error = f"connecting failed: {e}"
        agree(error)

    def _exchange_nccl_unique_id(self, ep: Any) -> List[int]:
        create_unique_id = getattr(ep, "create_nccl_unique_id", None)
        collective_device = _bootstrap_collective_device(self.group)
        root_global_rank = dist.get_global_rank(self.group, 0)

        # NCCL requires ncclGetUniqueId to be called once per communicator.
        # Catch root-side failures and broadcast the status first so every
        # subgroup rank either receives the same ID or raises instead of
        # waiting forever in the payload broadcast.
        local_unique_id: List[int] = []
        root_error: Optional[str] = None
        if self.rank_idx == 0:
            try:
                if create_unique_id is None:
                    raise RuntimeError(
                        "Mooncake EP was built without NCCL Device API support"
                    )
                local_unique_id = list(create_unique_id())
                if len(local_unique_id) != self._NCCL_UNIQUE_ID_WORDS:
                    raise RuntimeError(
                        "NCCL returned an invalid communicator unique ID: "
                        f"expected {self._NCCL_UNIQUE_ID_WORDS} int32 words, "
                        f"got {len(local_unique_id)}"
                    )
            except Exception as exc:
                root_error = f"{type(exc).__name__}: {exc}"

        status = torch.tensor(
            [int(root_error is not None)],
            dtype=torch.int32,
            device=collective_device,
        )
        dist.broadcast(
            status,
            src=root_global_rank,
            group=self.group,
        )
        if int(status.item()) != 0:
            detail = f": {root_error}" if root_error is not None else ""
            raise RuntimeError(
                f"group root failed to create the NCCL communicator unique ID{detail}"
            )

        unique_id = torch.zeros(
            self._NCCL_UNIQUE_ID_WORDS,
            dtype=torch.int32,
            device=collective_device,
        )
        if self.rank_idx == 0:
            unique_id.copy_(
                torch.tensor(
                    local_unique_id,
                    dtype=torch.int32,
                    device=collective_device,
                )
            )
        dist.broadcast(
            unique_id,
            src=root_global_rank,
            group=self.group,
        )
        return unique_id.cpu().tolist()

    def _exchange_nccl_unique_id_from_store(self, ep: Any) -> List[int]:
        payload = None
        if self.rank_idx == 0:
            create_unique_id = getattr(ep, "create_nccl_unique_id", None)
            try:
                if create_unique_id is None:
                    raise RuntimeError(
                        "Mooncake EP was built without NCCL Device API support"
                    )
                unique_id = list(create_unique_id())
                if len(unique_id) != self._NCCL_UNIQUE_ID_WORDS:
                    raise RuntimeError(
                        "NCCL returned an invalid communicator unique ID: "
                        f"expected {self._NCCL_UNIQUE_ID_WORDS} int32 words, "
                        f"got {len(unique_id)}"
                    )
                payload = {"error": None, "unique_id": unique_id}
            except Exception as exc:
                payload = {
                    "error": f"{type(exc).__name__}: {exc}",
                    "unique_id": None,
                }
        result = self._store_collectives.broadcast("nccl-unique-id", payload, root=0)
        if result["error"] is not None:
            raise RuntimeError(
                "group root failed to create the NCCL communicator unique ID: "
                + result["error"]
            )
        return [int(value) for value in result["unique_id"]]

    def update_ep_member(self) -> None:
        if self.transport != "nccl":
            self._connect_native(True)
            return
        if self.runtime is None:
            raise RuntimeError("ElasticBuffer has been destroyed")

        # NCCL cannot patch one peer in place. Join the same bootstrap sequence
        # used by a replacement rank's constructor and publish a complete new
        # communicator/device/window generation at a quiescent EP boundary.
        # Restore the device captured at construction because applications may
        # change the thread's current CUDA device between EP calls.
        with torch.cuda.device(self._device_index):
            torch.cuda.synchronize()
            from mooncake import ep

            if self._store_collectives is not None:
                selected_transport = self._select_store_transport(
                    ep, required_transport=self.transport
                )
            else:
                selected_transport = _select_transport_for_group(
                    self.group,
                    self.requested_transport,
                    ep.has_nccl_device_support(),
                    self.num_ranks,
                    self.num_rdma_ranks,
                    self.num_nvlink_ranks,
                    self.allow_hybrid_mode,
                    required_transport=self.transport,
                )
            if selected_transport != "nccl":
                raise RuntimeError(
                    "NCCL ElasticBuffer recovery no longer has a supported NCCL "
                    "topology; recreate the buffer with transport='ibgda'"
                )
            if self._store_collectives is None:
                self._require_full_nccl_membership()
            if self._store_collectives is not None:
                nccl_unique_id = self._exchange_nccl_unique_id_from_store(ep)
            else:
                nccl_unique_id = self._exchange_nccl_unique_id(ep)
            self.runtime.reconfigure_nccl(nccl_unique_id)
            # The native generation changes atomically when reconfigure_nccl()
            # returns. Publish the matching Python generation before any other
            # fallible work so an old handle cannot be accepted afterward.
            self._generation = tuple(nccl_unique_id)
            self._refresh_topology_from_runtime()
            self._active_ranks.fill_(1)

            torch.cuda.synchronize()
            if self._store_collectives is not None:
                self._store_collectives.barrier("nccl-update-complete")
            else:
                _dist_barrier(self.group)
            torch.cuda.synchronize()

    def destroy(self) -> None:
        runtime = self.runtime
        if runtime is None:
            return
        is_nccl = self.transport == "nccl"
        if is_nccl:
            # Quiesce every local CUDA stream before any rank deregisters the
            # symmetric window. The following process-group barrier then makes
            # that quiescence visible across all ranks.
            torch.cuda.synchronize()
            if self._store_collectives is not None:
                self._store_collectives.barrier("destroy-before")
            else:
                _dist_barrier(self.group)
        try:
            native_destroy = getattr(runtime, "destroy", None)
            if native_destroy is not None:
                native_destroy()
        finally:
            self.runtime = None
            if is_nccl:
                if self._store_collectives is not None:
                    self._store_collectives.barrier("destroy-after")
                else:
                    _dist_barrier(self.group)

    @staticmethod
    def _workspace_num_bytes() -> int:
        num_bytes = 0
        num_bytes += ElasticBuffer._NUM_BARRIER_TAGS * (
            8 + 2 * ElasticBuffer._NUM_MAX_RANKS * 4
        )
        num_bytes += (ElasticBuffer._NUM_MAX_RANKS + ElasticBuffer._NUM_MAX_EXPERTS) * 8
        num_bytes += ElasticBuffer._NUM_MAX_RANKS * 8 * 2
        num_bytes += ElasticBuffer._NUM_MAX_EXPERTS * 8 * 2
        num_bytes += ElasticBuffer._NUM_MAX_RANKS * 4
        num_bytes += ElasticBuffer._NUM_MAX_RANKS * 4 * 2
        num_bytes += ElasticBuffer._NUM_MAX_EXPERTS * 4 * 2
        num_bytes += ElasticBuffer._NUM_MAX_RANKS * ElasticBuffer._NUM_MAX_CHANNELS * 8
        num_bytes += ElasticBuffer._NUM_MAX_RANKS * ElasticBuffer._NUM_MAX_CHANNELS * 4
        num_bytes += 2 * 2 * 8
        num_bytes += (
            (ElasticBuffer._NUM_MAX_INFLIGHT_AGRS + 1)
            * ElasticBuffer._NUM_MAX_RANKS
            * 4
        )
        return _align(num_bytes, 32)

    @staticmethod
    def _atomic_scratch_num_bytes() -> int:
        # Mirrors the native runtime: this is IBGDA response scratch or NCCL
        # GIN-only signal storage, separate from the regular workspace signals.
        return ElasticBuffer._workspace_num_bytes()

    @staticmethod
    def get_buffer_size_hint(
        group: Union[dist.ProcessGroup, int],
        num_max_tokens_per_rank: int,
        hidden: int,
        num_topk: int = 0,
        use_fp8_dispatch: bool = False,
        allow_hybrid_mode: bool = True,
        allow_multiple_reduction: bool = True,
    ) -> int:
        num_ranks = group if isinstance(group, int) else group.size()
        if _ep_device_type() == "xpu":
            # On XPU, remote ranks need room of their own in the buffer even
            # though dispatch stays flat. Reserve it whenever the ranks span
            # nodes, so that allow_hybrid_mode=False works there as on CUDA.
            allow_hybrid_mode = (
                allow_hybrid_mode or ElasticBuffer._xpu_ranks_span_nodes(group)
            )
        try:
            from mooncake import ep

            return int(
                ep.calculate_elastic_buffer_size(
                    num_ranks,
                    num_max_tokens_per_rank,
                    hidden,
                    num_topk,
                    use_fp8_dispatch,
                    allow_hybrid_mode,
                    allow_multiple_reduction,
                )
            )
        except Exception:
            pass

        num_topk = max(1, num_topk)
        dtype_bytes = 1 if use_fp8_dispatch else 2
        scale_bytes = _ceil_div(hidden, 128) * 4 if use_fp8_dispatch else 0
        token_bytes = _align(hidden * dtype_bytes, 32) + _align(scale_bytes, 32)
        metadata_bytes = _align(num_topk * (4 + 4) + (1 + num_topk) * 4, 32)
        per_slot_bytes = token_bytes + metadata_bytes

        # Direct elastic send/recv buffers plus room for combine reduce buffers.
        dispatch_bytes = (
            num_ranks * num_max_tokens_per_rank * num_topk * per_slot_bytes * 2
        )
        combine_factor = 3 if allow_multiple_reduction else 4
        combine_bytes = dispatch_bytes * combine_factor
        hybrid_factor = 2 if allow_hybrid_mode and num_ranks > 1 else 1
        return int(
            ElasticBuffer._workspace_num_bytes()
            + ElasticBuffer._atomic_scratch_num_bytes()
            + hybrid_factor * (dispatch_bytes + combine_bytes)
        )

    @staticmethod
    def get_engram_storage_size_hint(
        num_entries: int,
        hidden: int,
        num_max_tokens_per_rank: int,
        dtype: torch.dtype = torch.bfloat16,
    ) -> int:
        num_sf_packs = _ceil_div(hidden, 128) if dtype.itemsize <= 1 else 0
        num_bytes_per_entry = _align(hidden * dtype.itemsize + num_sf_packs * 4, 32)
        return num_bytes_per_entry * (num_entries + num_max_tokens_per_rank)

    @staticmethod
    def get_pp_buffer_size_hint(
        num_max_tensor_bytes: int, num_max_inflight_tensors: int
    ) -> int:
        return _align(num_max_tensor_bytes, 32) * num_max_inflight_tensors * 2 * 2

    @staticmethod
    def get_agrs_buffer_size_hint(
        group: dist.ProcessGroup, num_max_session_bytes: int
    ) -> int:
        return num_max_session_bytes

    @staticmethod
    def _xpu_ranks_span_nodes(group: Union[dist.ProcessGroup, int]) -> bool:
        """Whether XPU ranks span nodes, as far as is known before the buffer
        exists: from MC_EP_RANKS_PER_NODE as in _connect_xpu_rdma, else from
        the physical domain size. Ranks found remote only from the host words
        of their IPC handles are not seen here."""
        per_node = int(os.environ.get("MC_EP_RANKS_PER_NODE", "0") or 0)
        if per_node > 0:
            return per_node < (group if isinstance(group, int) else group.size())
        return ElasticBuffer._calculate_physical_domain_size(group)[0] > 1

    @staticmethod
    def _calculate_physical_domain_size(
        group: Union[dist.ProcessGroup, int],
    ) -> Tuple[int, int]:
        if isinstance(group, int):
            num_ranks = group
            num_local_ranks = int(os.getenv("MOONCAKE_EP_NUM_LOCAL_RANKS", "0"))
        else:
            num_ranks = group.size()
            num_local_ranks = int(
                os.getenv(
                    "MOONCAKE_EP_NUM_LOCAL_RANKS",
                    os.getenv("LOCAL_WORLD_SIZE", "0"),
                )
            )
        if num_local_ranks <= 0:
            try:
                device_module = torch.xpu if _ep_device_type() == "xpu" else torch.cuda
                num_local_ranks = max(1, min(num_ranks, device_module.device_count()))
            except Exception:
                num_local_ranks = 1
        num_local_ranks = max(1, min(num_local_ranks, num_ranks))
        return _ceil_div(num_ranks, num_local_ranks), num_local_ranks

    @staticmethod
    def _calculate_logical_domain_size(
        group: Union[dist.ProcessGroup, int], allow_hybrid_mode: bool = True
    ) -> Tuple[int, int]:
        num_ranks = group if isinstance(group, int) else group.size()
        num_rdma_ranks, num_nvlink_ranks = (
            ElasticBuffer._calculate_physical_domain_size(group)
        )
        if allow_hybrid_mode and num_rdma_ranks > 1:
            return num_rdma_ranks, num_nvlink_ranks
        return 1, num_ranks

    def get_physical_domain_size(self) -> Tuple[int, int]:
        return self.num_rdma_ranks, self.num_nvlink_ranks

    def get_logical_domain_size(self) -> Tuple[int, int]:
        return self.num_scaleout_ranks, self.num_scaleup_ranks

    def _hybrid_num_channels(self, num_sms: int) -> int:
        channels_per_sm = 8 if self.transport == "nccl" else 4
        return max(1, num_sms) * channels_per_sm

    def _hybrid_num_max_tokens_per_channel(
        self, num_max_tokens_per_rank: int, num_sms: int
    ) -> int:
        return _ceil_div(num_max_tokens_per_rank, self._hybrid_num_channels(num_sms))

    def barrier(
        self, use_comm_stream: bool = True, with_cpu_sync: bool = False
    ) -> None:
        if with_cpu_sync:
            _synchronize_device()
        if self._store_collectives is not None:
            self._store_collectives.barrier("public-barrier")
        else:
            _dist_barrier(self.group)
        if with_cpu_sync:
            _synchronize_device()

    @staticmethod
    def capture() -> Any:
        from mooncake import ep

        return ep.EventHandle(_native_current_stream_ptr())

    def get_theoretical_num_sms(self, num_experts: int, num_topk: int) -> int:
        if _ep_device_type() == "xpu":
            # The XPU kernels ignore num_sms unless
            # MOONCAKE_EP_XPU_ASYNC_WITH_COMPUTE_STREAM_ENABLE is set; then it
            # caps their work-groups, with the same defaults as on CUDA.
            props = torch.xpu.get_device_properties(torch.xpu.current_device())
            sm_count = int(getattr(props, "max_compute_units", 1))
        else:
            device = torch.cuda.current_device()
            sm_count = torch.cuda.get_device_properties(device).multi_processor_count
        if self.prefer_overlap_with_compute:
            return max(1, min(24, sm_count // 4))
        return max(1, min(40, sm_count // 2, num_experts * max(1, num_topk)))

    def dispatch(
        self,
        x: Union[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]],
        topk_idx: Optional[torch.Tensor] = None,
        topk_weights: Optional[torch.Tensor] = None,
        num_experts: Optional[int] = None,
        num_max_tokens_per_rank: Optional[int] = None,
        expert_alignment: Optional[int] = None,
        handle: Optional[EPHandle] = None,
        do_expand: bool = False,
        do_cpu_sync: Optional[bool] = None,
        num_sms: Optional[int] = None,
        async_with_compute_stream: bool = False,
    ) -> Tuple[
        Union[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]],
        Optional[torch.Tensor],
        Optional[torch.Tensor],
        EPHandle,
        EventOverlap,
    ]:
        if self.runtime is None:
            raise RuntimeError("ElasticBuffer has been destroyed")
        if handle is not None:
            self._validate_handle_generation(handle)
            if topk_idx is not None or topk_weights is not None:
                raise AssertionError(
                    "topk_idx and topk_weights must be None when cached handle is provided"
                )
            if do_cpu_sync:
                raise AssertionError("Cannot do CPU sync with cached handle")
            if do_expand or handle.do_expand:
                raise AssertionError(
                    "Cached EPHandle currently supports only do_expand=False"
                )
            if handle.native_handle is None:
                raise RuntimeError(
                    "Cached EPHandle is missing its native Mooncake handle"
                )
            topk_idx = handle.topk_idx
            num_max_tokens_per_rank = (
                num_max_tokens_per_rank or handle.num_max_tokens_per_rank
            )
            num_experts = num_experts or handle.num_experts
            expert_alignment = (
                handle.expert_alignment
                if expert_alignment is None
                else expert_alignment
            )
            num_sms = handle.num_sms if num_sms is None else num_sms
            do_cpu_sync = False
        else:
            if topk_idx is None:
                raise AssertionError(
                    "topk_idx must be provided when cached handle is not provided"
                )
            expert_alignment = 1 if expert_alignment is None else expert_alignment
            do_cpu_sync = True if do_cpu_sync is None else do_cpu_sync
        if do_expand:
            warnings.warn(
                "do_expand=True was requested. Mooncake currently returns the native packed expert layout; "
                "expanded contiguous expert layout will be produced by the native elastic kernels.",
                RuntimeWarning,
                stacklevel=2,
            )

        x_data = x[0] if isinstance(x, tuple) else x
        sf = x[1] if isinstance(x, tuple) else None
        assert x_data.dim() == 2 and x_data.is_contiguous()
        # The kernels copy scale factors as 4-byte packs.
        assert sf is None or (sf.dim() == 2 and sf.element_size() == 4)
        if num_experts is None:
            num_experts = int(torch.max(topk_idx).item()) + 1
        if num_max_tokens_per_rank is None:
            num_max_tokens_per_rank = self.num_max_tokens_per_rank or x_data.shape[0]
        if num_sms is None:
            num_sms = self.get_theoretical_num_sms(num_experts, topk_idx.shape[1])
        num_tokens = int(x_data.shape[0])
        hidden = int(x_data.shape[1])
        num_topk = int(topk_idx.shape[1])
        num_local_experts = num_experts // self.num_ranks
        max_num_recv_tokens = num_max_tokens_per_rank * self.num_ranks
        num_recv_tokens = (
            int(handle.recv_src_metadata.shape[0])
            if handle is not None
            else max_num_recv_tokens
        )
        # Expanded, each local expert's rows start at an expert_alignment
        # boundary, so leave room for up to expert_alignment - 1 padding rows
        # per expert.
        num_recv_output_capacity = (
            num_recv_tokens * num_topk + num_local_experts * (expert_alignment - 1)
            if do_expand
            else num_recv_tokens
        )
        use_hybrid = self.num_scaleout_ranks != 1
        hybrid_channels = self._hybrid_num_channels(num_sms) if use_hybrid else 0
        hybrid_max_tokens_per_channel = (
            self._hybrid_num_max_tokens_per_channel(num_max_tokens_per_rank, num_sms)
            if use_hybrid
            else 0
        )
        num_sf_packs = int(sf.shape[1]) if sf is not None else 0
        sf_token_stride = int(sf.stride(0)) if sf is not None else 0
        sf_hidden_stride = int(sf.stride(1)) if sf is not None else 0

        active_ranks = self._active_ranks
        full_psum_num_recv_tokens_per_expert = (
            handle.psum_num_recv_tokens_per_expert
            if handle is not None and do_expand
            else (
                torch.cat(
                    (
                        torch.zeros(1, dtype=torch.int32, device=x_data.device),
                        handle.psum_num_recv_tokens_per_expert,
                    )
                )
                if handle is not None
                else torch.empty(
                    num_local_experts + 1, dtype=torch.int32, device=x_data.device
                )
            )
        )
        psum_num_recv_tokens_per_scaleup_rank = (
            handle.psum_num_recv_tokens_per_scaleup_rank
            if handle is not None
            else torch.empty(
                self.num_scaleup_ranks, dtype=torch.int32, device=x_data.device
            )
        )
        dst_buffer_slot_idx = (
            handle.dst_buffer_slot_idx
            if handle is not None
            else (
                torch.empty(
                    (
                        hybrid_channels,
                        self.num_scaleout_ranks,
                        hybrid_max_tokens_per_channel,
                        num_topk,
                    ),
                    dtype=torch.int32,
                    device=x_data.device,
                )
                if use_hybrid
                else torch.empty(
                    (num_tokens, num_topk), dtype=torch.int32, device=x_data.device
                )
            )
        )
        token_metadata_at_forward = (
            handle.token_metadata_at_forward
            if handle is not None
            else (
                torch.empty(
                    (
                        hybrid_channels,
                        self.num_scaleout_ranks * hybrid_max_tokens_per_channel + 1,
                        2 + num_topk * 2,
                    ),
                    dtype=torch.int32,
                    device=x_data.device,
                )
                if use_hybrid
                else None
            )
        )
        channel_linked_list = (
            handle.channel_linked_list
            if handle is not None
            else (
                torch.empty(
                    (
                        hybrid_channels,
                        self.num_scaleout_ranks * hybrid_max_tokens_per_channel + 1,
                        self.num_scaleup_ranks,
                    ),
                    dtype=torch.int32,
                    device=x_data.device,
                )
                if use_hybrid
                else None
            )
        )
        recv_x = torch.empty(
            (num_recv_output_capacity, hidden), dtype=x_data.dtype, device=x_data.device
        )
        recv_x_scales = (
            torch.empty(
                (num_recv_output_capacity, num_sf_packs),
                dtype=sf.dtype,
                device=sf.device,
            )
            if sf is not None
            else None
        )
        recv_topk_idx = torch.empty(
            (num_recv_tokens, num_topk), dtype=topk_idx.dtype, device=topk_idx.device
        )
        recv_topk_weights = (
            torch.empty(
                (num_recv_output_capacity,)
                if do_expand
                else (num_recv_tokens, num_topk),
                dtype=topk_weights.dtype,
                device=topk_weights.device,
            )
            if topk_weights is not None
            else None
        )
        recv_src_metadata = (
            handle.recv_src_metadata
            if handle is not None
            else torch.empty(
                (num_recv_tokens, num_topk + 2),
                dtype=torch.int32,
                device=x_data.device,
            )
        )
        event = self.runtime.dispatch(
            x_data.data_ptr(),
            x_data.element_size(),
            0 if sf is None else sf.data_ptr(),
            num_tokens,
            hidden,
            num_sf_packs,
            sf_token_stride,
            sf_hidden_stride,
            topk_idx.data_ptr(),
            num_topk,
            0 if topk_weights is None else topk_weights.data_ptr(),
            active_ranks.data_ptr(),
            num_experts,
            num_max_tokens_per_rank,
            expert_alignment,
            num_sms,
            do_expand,
            async_with_compute_stream,
            _native_current_stream_ptr(),
            handle is not None,
            num_recv_tokens,
            psum_num_recv_tokens_per_scaleup_rank.data_ptr(),
            full_psum_num_recv_tokens_per_expert.data_ptr(),
            dst_buffer_slot_idx.data_ptr(),
            0
            if token_metadata_at_forward is None
            else token_metadata_at_forward.data_ptr(),
            0 if channel_linked_list is None else channel_linked_list.data_ptr(),
            recv_x.data_ptr(),
            0 if recv_x_scales is None else recv_x_scales.data_ptr(),
            recv_topk_idx.data_ptr(),
            0 if recv_topk_weights is None else recv_topk_weights.data_ptr(),
            recv_src_metadata.data_ptr(),
        )
        handle_psum_num_recv_tokens_per_expert = (
            full_psum_num_recv_tokens_per_expert
            if do_expand
            else full_psum_num_recv_tokens_per_expert[1:]
        )

        num_recv_tokens_per_expert_list: List[int] = []
        if do_cpu_sync:
            # With async_with_compute_stream the dispatch kernels run on the
            # communication stream, but .cpu() copies on the compute stream. Make
            # the compute stream wait for the dispatch so the copy reads the
            # final counts. This makes do_cpu_sync=True effectively synchronous:
            # for overlap, pass do_cpu_sync=False or a cached handle.
            if async_with_compute_stream:
                _wait_native_event_on_current_stream(event)
            scaleup_psum_cpu = psum_num_recv_tokens_per_scaleup_rank.cpu()
            expert_psum_cpu = full_psum_num_recv_tokens_per_expert.cpu()
            actual_num_recv_tokens = int(
                scaleup_psum_cpu[self.num_scaleup_ranks - 1].item()
            )
            actual_num_output_tokens = actual_num_recv_tokens

            def _align_count(value: int) -> int:
                return _ceil_div(value, expert_alignment) * expert_alignment

            if do_expand:
                previous_psum = 0
                for i in range(num_local_experts):
                    current_psum = int(expert_psum_cpu[i].item())
                    count = current_psum - _align_count(previous_psum)
                    num_recv_tokens_per_expert_list.append(count)
                    previous_psum = current_psum
                actual_num_output_tokens = (
                    0
                    if num_local_experts == 0
                    else int(expert_psum_cpu[num_local_experts - 1].item())
                )
            else:
                for i in range(num_local_experts):
                    count = int(
                        expert_psum_cpu[i + 1].item() - expert_psum_cpu[i].item()
                    )
                    num_recv_tokens_per_expert_list.append(count)

            recv_x = recv_x[:actual_num_output_tokens]
            if recv_x_scales is not None:
                recv_x_scales = recv_x_scales[:actual_num_output_tokens]
            recv_topk_idx = recv_topk_idx[:actual_num_recv_tokens]
            if recv_topk_weights is not None:
                recv_topk_weights = recv_topk_weights[:actual_num_output_tokens]
            recv_src_metadata = recv_src_metadata[:actual_num_recv_tokens]

        elastic_handle = handle
        if elastic_handle is None:
            elastic_handle = EPHandle(
                do_expand=do_expand,
                num_experts=num_experts,
                expert_alignment=expert_alignment,
                num_max_tokens_per_rank=num_max_tokens_per_rank,
                num_sms=num_sms,
                topk_idx=topk_idx.clone(),
                num_recv_tokens_per_expert_list=num_recv_tokens_per_expert_list,
                psum_num_recv_tokens_per_scaleup_rank=psum_num_recv_tokens_per_scaleup_rank,
                psum_num_recv_tokens_per_expert=handle_psum_num_recv_tokens_per_expert,
                recv_src_metadata=recv_src_metadata,
                dst_buffer_slot_idx=dst_buffer_slot_idx,
                token_metadata_at_forward=token_metadata_at_forward,
                channel_linked_list=channel_linked_list,
                native_handle=True,
                generation=self._generation,
            )
        recv_x_out = (recv_x, recv_x_scales) if recv_x_scales is not None else recv_x
        tensors_to_record = None
        if async_with_compute_stream:
            tensors_to_record = (
                x_data,
                topk_idx,
                active_ranks,
                recv_x,
                recv_topk_idx,
                elastic_handle.topk_idx,
                elastic_handle.psum_num_recv_tokens_per_scaleup_rank,
                elastic_handle.psum_num_recv_tokens_per_expert,
                elastic_handle.recv_src_metadata,
                elastic_handle.dst_buffer_slot_idx,
                *(() if sf is None else (sf,)),
                *(() if topk_weights is None else (topk_weights,)),
                *(() if recv_x_scales is None else (recv_x_scales,)),
                *(() if recv_topk_weights is None else (recv_topk_weights,)),
                *(
                    ()
                    if elastic_handle.token_metadata_at_forward is None
                    else (elastic_handle.token_metadata_at_forward,)
                ),
                *(
                    ()
                    if elastic_handle.channel_linked_list is None
                    else (elastic_handle.channel_linked_list,)
                ),
            )
        return (
            recv_x_out,
            recv_topk_idx,
            recv_topk_weights,
            elastic_handle,
            EventOverlap(event, tensors_to_record),
        )

    def combine(
        self,
        x: torch.Tensor,
        handle: EPHandle,
        topk_weights: Optional[torch.Tensor] = None,
        num_sms: Optional[int] = None,
        async_with_compute_stream: bool = False,
    ) -> Tuple[torch.Tensor, Optional[torch.Tensor], EventOverlap]:
        if self.runtime is None:
            raise RuntimeError("ElasticBuffer has been destroyed")
        self._validate_handle_generation(handle)
        if handle.native_handle is None:
            raise RuntimeError("Mooncake EPHandle does not contain a native handle")
        assert x.dim() == 2 and x.is_contiguous()
        assert x.dtype == torch.bfloat16
        active_ranks = self._active_ranks
        if topk_weights is None:
            topk_weights = torch.ones_like(
                handle.topk_idx, dtype=torch.float32, device=x.device
            )
        combined_x = torch.empty(
            (handle.topk_idx.shape[0], x.shape[1]), dtype=x.dtype, device=x.device
        )
        event = self.runtime.combine(
            x.data_ptr(),
            x.shape[0],
            x.shape[1],
            handle.topk_idx.data_ptr(),
            handle.topk_idx.shape[0],
            handle.topk_idx.shape[1],
            topk_weights.data_ptr(),
            handle.psum_num_recv_tokens_per_scaleup_rank.data_ptr(),
            handle.recv_src_metadata.data_ptr(),
            0
            if handle.token_metadata_at_forward is None
            else handle.token_metadata_at_forward.data_ptr(),
            0
            if handle.channel_linked_list is None
            else handle.channel_linked_list.data_ptr(),
            active_ranks.data_ptr(),
            handle.num_experts,
            handle.num_max_tokens_per_rank,
            handle.do_expand,
            num_sms if num_sms is not None else handle.num_sms,
            async_with_compute_stream,
            _native_current_stream_ptr(),
            combined_x.data_ptr(),
        )
        tensors_to_record = None
        if async_with_compute_stream:
            tensors_to_record = (
                x,
                topk_weights,
                active_ranks,
                combined_x,
                handle.topk_idx,
                handle.psum_num_recv_tokens_per_scaleup_rank,
                handle.psum_num_recv_tokens_per_expert,
                handle.recv_src_metadata,
                handle.dst_buffer_slot_idx,
                *(
                    ()
                    if handle.token_metadata_at_forward is None
                    else (handle.token_metadata_at_forward,)
                ),
                *(
                    ()
                    if handle.channel_linked_list is None
                    else (handle.channel_linked_list,)
                ),
            )
        return (
            combined_x,
            None,
            EventOverlap(event, tensors_to_record),
        )


__all__ = ["ElasticBuffer", "EPHandle", "EventOverlap"]
