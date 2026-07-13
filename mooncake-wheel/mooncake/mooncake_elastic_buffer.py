import os
import warnings
from typing import Any, List, Optional, Tuple, Union

import torch
import torch.distributed as dist

from .mooncake_ep_buffer import EventOverlap, _native_current_stream_ptr


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
        group.barrier()


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
    ) -> None:
        assert topk_idx is not None
        self.do_expand = do_expand
        self.num_experts = num_experts
        self.expert_alignment = expert_alignment
        self.num_max_tokens_per_rank = num_max_tokens_per_rank
        self.num_sms = num_sms
        self.topk_idx = topk_idx
        self.psum_num_recv_tokens_per_scaleup_rank = psum_num_recv_tokens_per_scaleup_rank
        self.psum_num_recv_tokens_per_expert = psum_num_recv_tokens_per_expert
        self.num_recv_tokens_per_expert_list = num_recv_tokens_per_expert_list
        self.recv_src_metadata = recv_src_metadata
        self.dst_buffer_slot_idx = dst_buffer_slot_idx
        self.token_metadata_at_forward = token_metadata_at_forward
        self.channel_linked_list = channel_linked_list
        self.native_handle = native_handle

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
    """

    # Mirrors DeepEP's fixed workspace assumptions closely enough for sizing and
    # keeping one reusable buffer for all elastic EP shapes.
    _NUM_MAX_RANKS = 1024
    _NUM_MAX_EXPERTS = 2048
    _NUM_MAX_CHANNELS = 8 * 160
    _NUM_BARRIER_TAGS = 16
    _NUM_MAX_INFLIGHT_AGRS = 32

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
    ) -> None:
        if not allow_multiple_reduction:
            raise NotImplementedError(
                "Mooncake ElasticBuffer currently supports only "
                "allow_multiple_reduction=True"
            )
        self.group = group
        self.rank_idx = group.rank()
        self.num_ranks = group.size()
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
        self.num_rdma_ranks, self.num_nvlink_ranks = self._calculate_physical_domain_size(group)

        self.backend = group

        # Native Mooncake transport/runtime.  This keeps the legacy Buffer ABI
        # untouched while giving ElasticBuffer users a dedicated native entrypoint.
        from mooncake import ep

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
        )
        self._connect_native()

        torch.cuda.synchronize()
        _dist_barrier(group)
        torch.cuda.synchronize()

    def _active_ranks_mask(self) -> list:
        # `mooncake.ep.get_active_ranks` is a Mooncake PG helper and performs a
        # native static cast to MooncakeBackend. ElasticBuffer transport
        # bootstrap can also be driven by a regular NCCL/Gloo ProcessGroup; in
        # that case every rank in the supplied group is active by definition.
        if "Mooncake" not in type(self.backend).__name__:
            return [1] * self.num_ranks

        from mooncake.ep import get_active_ranks

        return get_active_ranks(self.backend).tolist()

    def _connect_native(self, is_update: bool = False) -> None:
        from mooncake import ep

        if not bool(self.runtime.ibgda_disabled()):
            raddr, rkey = self.runtime.get_mr_info()
            raddr_tensor = torch.tensor([raddr], dtype=torch.int64, device="cuda")
            raddrs = [torch.empty(1, dtype=torch.int64, device="cuda") for _ in range(self.num_ranks)]
            dist.all_gather(raddrs, raddr_tensor, self.group)
            raddrs_list = torch.cat(raddrs).tolist()

            rkey_tensor = torch.tensor([rkey], dtype=torch.int32, device="cuda")
            rkeys = [torch.empty(1, dtype=torch.int32, device="cuda") for _ in range(self.num_ranks)]
            dist.all_gather(rkeys, rkey_tensor, self.group)
            rkeys_list = torch.cat(rkeys).tolist()

            all_to_all_size = ep.MAX_QP_COUNT // self.num_ranks
            if is_update:
                self.runtime.update_local_qpns()

            local_qpns = torch.tensor(self.runtime.get_local_qpns(), dtype=torch.int32, device="cuda").view(
                -1, all_to_all_size
            )
            remote_qpns = [torch.empty(all_to_all_size, dtype=torch.int32, device="cuda") for _ in range(self.num_ranks)]
            dist.all_to_all(remote_qpns, list(torch.unbind(local_qpns)), self.group)
            peer_qpns = [remote_qpns[r].tolist() for r in range(self.num_ranks)]

            local_lids = torch.tensor(self.runtime.get_local_lids(), dtype=torch.int32, device="cuda").view(
                -1, all_to_all_size
            )
            remote_lids = [torch.empty(all_to_all_size, dtype=torch.int32, device="cuda") for _ in range(self.num_ranks)]
            dist.all_to_all(remote_lids, list(torch.unbind(local_lids)), self.group)
            peer_lids = [remote_lids[r].tolist() for r in range(self.num_ranks)]

            subnet_prefix, interface_id = self.runtime.get_gid()
            subnet_prefix_tensor = torch.tensor([subnet_prefix], dtype=torch.int64, device="cuda")
            subnet_prefixes = [torch.empty(1, dtype=torch.int64, device="cuda") for _ in range(self.num_ranks)]
            dist.all_gather(subnet_prefixes, subnet_prefix_tensor, self.group)
            subnet_prefixes_list = torch.cat(subnet_prefixes).tolist()

            interface_id_tensor = torch.tensor([interface_id], dtype=torch.int64, device="cuda")
            interface_ids = [torch.empty(1, dtype=torch.int64, device="cuda") for _ in range(self.num_ranks)]
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
            local_handle_tensor = torch.tensor(local_handle_ints, dtype=torch.int32, device="cuda")
            handles = [torch.empty(len(local_handle_ints), dtype=torch.int32, device="cuda") for _ in range(self.num_ranks)]
            dist.all_gather(handles, local_handle_tensor, self.group)
            remote_handles = [h.tolist() for h in handles]

            active_ranks_mask = self._active_ranks_mask()
            self.runtime.sync_nvlink_ipc_handles(remote_handles, active_ranks_mask)
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

    def update_ep_member(self) -> None:
        self._connect_native(True)

    def destroy(self) -> None:
        # Existing Mooncake Buffer owns native resources through object lifetime.
        # Keep the method to match the official ElasticBuffer API.
        self.runtime = None

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
        num_bytes += (ElasticBuffer._NUM_MAX_INFLIGHT_AGRS + 1) * ElasticBuffer._NUM_MAX_RANKS * 4
        return _align(num_bytes, 32)

    @staticmethod
    def _atomic_scratch_num_bytes() -> int:
        # Mirrors the native runtime: RDMA atomics need a local response area
        # separate from the remote-visible workspace.
        return ElasticBuffer._workspace_num_bytes()

    @staticmethod
    def get_buffer_size_hint(
        group: dist.ProcessGroup,
        num_max_tokens_per_rank: int,
        hidden: int,
        num_topk: int = 0,
        use_fp8_dispatch: bool = False,
        allow_hybrid_mode: bool = True,
        allow_multiple_reduction: bool = True,
    ) -> int:
        try:
            from mooncake import ep

            return int(
                ep.calculate_elastic_buffer_size(
                    group.size(),
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

        num_ranks = group.size()
        num_topk = max(1, num_topk)
        dtype_bytes = 1 if use_fp8_dispatch else 2
        scale_bytes = _ceil_div(hidden, 128) * 4 if use_fp8_dispatch else 0
        token_bytes = _align(hidden * dtype_bytes, 32) + _align(scale_bytes, 32)
        metadata_bytes = _align(num_topk * (4 + 4) + (1 + num_topk) * 4, 32)
        per_slot_bytes = token_bytes + metadata_bytes

        # Direct elastic send/recv buffers plus room for combine reduce buffers.
        dispatch_bytes = num_ranks * num_max_tokens_per_rank * num_topk * per_slot_bytes * 2
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
    def get_pp_buffer_size_hint(num_max_tensor_bytes: int, num_max_inflight_tensors: int) -> int:
        return _align(num_max_tensor_bytes, 32) * num_max_inflight_tensors * 2 * 2

    @staticmethod
    def get_agrs_buffer_size_hint(group: dist.ProcessGroup, num_max_session_bytes: int) -> int:
        return num_max_session_bytes

    @staticmethod
    def _calculate_physical_domain_size(group: dist.ProcessGroup) -> Tuple[int, int]:
        num_ranks = group.size()
        num_local_ranks = int(os.getenv("MOONCAKE_EP_NUM_LOCAL_RANKS", "0"))
        if num_local_ranks <= 0:
            try:
                num_local_ranks = max(1, min(num_ranks, torch.cuda.device_count()))
            except Exception:
                num_local_ranks = 1
        num_local_ranks = max(1, min(num_local_ranks, num_ranks))
        return _ceil_div(num_ranks, num_local_ranks), num_local_ranks

    @staticmethod
    def _calculate_logical_domain_size(group: dist.ProcessGroup, allow_hybrid_mode: bool = True) -> Tuple[int, int]:
        num_ranks = group.size()
        num_rdma_ranks, num_nvlink_ranks = ElasticBuffer._calculate_physical_domain_size(group)
        if allow_hybrid_mode and num_rdma_ranks > 1:
            return num_rdma_ranks, num_nvlink_ranks
        return 1, num_ranks

    def get_physical_domain_size(self) -> Tuple[int, int]:
        return self.num_rdma_ranks, self.num_nvlink_ranks

    def get_logical_domain_size(self) -> Tuple[int, int]:
        return self.num_scaleout_ranks, self.num_scaleup_ranks

    @staticmethod
    def _hybrid_num_channels(num_sms: int) -> int:
        return max(1, num_sms) * 4

    @staticmethod
    def _hybrid_num_max_tokens_per_channel(
        num_max_tokens_per_rank: int, num_sms: int
    ) -> int:
        return _ceil_div(
            num_max_tokens_per_rank, ElasticBuffer._hybrid_num_channels(num_sms)
        )

    def barrier(self, use_comm_stream: bool = True, with_cpu_sync: bool = False) -> None:
        if with_cpu_sync:
            torch.cuda.synchronize()
        _dist_barrier(self.group)
        if with_cpu_sync:
            torch.cuda.synchronize()

    @staticmethod
    def capture() -> Any:
        from mooncake import ep

        return ep.EventHandle(_native_current_stream_ptr())

    def get_theoretical_num_sms(self, num_experts: int, num_topk: int) -> int:
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
    ) -> Tuple[Union[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]], Optional[torch.Tensor], Optional[torch.Tensor], EPHandle, EventOverlap]:
        if self.runtime is None:
            raise RuntimeError("ElasticBuffer has been destroyed")
        if handle is not None:
            if topk_idx is not None or topk_weights is not None:
                raise AssertionError("topk_idx and topk_weights must be None when cached handle is provided")
            if do_cpu_sync:
                raise AssertionError("Cannot do CPU sync with cached handle")
            if do_expand or handle.do_expand:
                raise AssertionError("Cached EPHandle currently supports only do_expand=False")
            if handle.native_handle is None:
                raise RuntimeError("Cached EPHandle is missing its native Mooncake handle")
            topk_idx = handle.topk_idx
            num_max_tokens_per_rank = num_max_tokens_per_rank or handle.num_max_tokens_per_rank
            num_experts = num_experts or handle.num_experts
            expert_alignment = handle.expert_alignment if expert_alignment is None else expert_alignment
            num_sms = handle.num_sms if num_sms is None else num_sms
            do_cpu_sync = False
        else:
            if topk_idx is None:
                raise AssertionError("topk_idx must be provided when cached handle is not provided")
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
        num_recv_tokens = num_max_tokens_per_rank * self.num_ranks
        num_recv_output_capacity = num_recv_tokens * num_topk if do_expand else num_recv_tokens
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

        active_ranks = torch.ones(self.num_ranks, dtype=torch.int32, device=x_data.device)
        full_psum_num_recv_tokens_per_expert = (
            handle.psum_num_recv_tokens_per_expert
            if handle is not None and do_expand
            else (
                torch.cat(
                    (
                        torch.zeros(
                            1, dtype=torch.int32, device=x_data.device
                        ),
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
            else torch.empty(self.num_scaleup_ranks, dtype=torch.int32, device=x_data.device)
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
                else torch.empty((num_tokens, num_topk), dtype=torch.int32, device=x_data.device)
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
        recv_x = torch.empty((num_recv_output_capacity, hidden), dtype=x_data.dtype, device=x_data.device)
        recv_x_scales = (
            torch.empty((num_recv_output_capacity, num_sf_packs), dtype=sf.dtype, device=sf.device)
            if sf is not None
            else None
        )
        recv_topk_idx = torch.empty((num_recv_tokens, num_topk), dtype=topk_idx.dtype, device=topk_idx.device)
        recv_topk_weights = (
            torch.empty(
                (num_recv_output_capacity,) if do_expand else (num_recv_tokens, num_topk),
                dtype=topk_weights.dtype,
                device=topk_weights.device,
            )
            if topk_weights is not None
            else None
        )
        recv_src_metadata = torch.empty(
            (num_recv_tokens, num_topk + 2), dtype=torch.int32, device=x_data.device
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
            psum_num_recv_tokens_per_scaleup_rank.data_ptr(),
            full_psum_num_recv_tokens_per_expert.data_ptr(),
            dst_buffer_slot_idx.data_ptr(),
            0 if token_metadata_at_forward is None else token_metadata_at_forward.data_ptr(),
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
            scaleup_psum_cpu = psum_num_recv_tokens_per_scaleup_rank.cpu()
            expert_psum_cpu = full_psum_num_recv_tokens_per_expert.cpu()
            actual_num_recv_tokens = int(scaleup_psum_cpu[self.num_scaleup_ranks - 1].item())
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
                    0 if num_local_experts == 0 else int(expert_psum_cpu[num_local_experts - 1].item())
                )
            else:
                for i in range(num_local_experts):
                    count = int(expert_psum_cpu[i + 1].item() - expert_psum_cpu[i].item())
                    num_recv_tokens_per_expert_list.append(count)

            recv_x = recv_x[:actual_num_output_tokens]
            if recv_x_scales is not None:
                recv_x_scales = recv_x_scales[:actual_num_output_tokens]
            recv_topk_idx = recv_topk_idx[:actual_num_recv_tokens]
            if recv_topk_weights is not None:
                recv_topk_weights = recv_topk_weights[:actual_num_output_tokens]
            recv_src_metadata = recv_src_metadata[:actual_num_recv_tokens]

        elastic_handle = EPHandle(
            do_expand=do_expand,
            num_experts=num_experts,
            expert_alignment=expert_alignment,
            num_max_tokens_per_rank=num_max_tokens_per_rank,
            num_sms=num_sms,
            topk_idx=handle.topk_idx if handle is not None else topk_idx.clone(),
            num_recv_tokens_per_expert_list=num_recv_tokens_per_expert_list,
            psum_num_recv_tokens_per_scaleup_rank=psum_num_recv_tokens_per_scaleup_rank,
            psum_num_recv_tokens_per_expert=handle_psum_num_recv_tokens_per_expert,
            recv_src_metadata=recv_src_metadata,
            dst_buffer_slot_idx=dst_buffer_slot_idx,
            token_metadata_at_forward=token_metadata_at_forward,
            channel_linked_list=channel_linked_list,
            native_handle=True,
        )
        recv_x_out = (recv_x, recv_x_scales) if recv_x_scales is not None else recv_x
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
            *(() if elastic_handle.token_metadata_at_forward is None else (elastic_handle.token_metadata_at_forward,)),
            *(() if elastic_handle.channel_linked_list is None else (elastic_handle.channel_linked_list,)),
        )
        return (
            recv_x_out,
            recv_topk_idx,
            recv_topk_weights,
            elastic_handle,
            EventOverlap(event, tensors_to_record if async_with_compute_stream else None),
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
        if handle.native_handle is None:
            raise RuntimeError("Mooncake EPHandle does not contain a native handle")
        assert x.dim() == 2 and x.is_contiguous()
        assert x.dtype == torch.bfloat16
        active_ranks = torch.ones(self.num_ranks, dtype=torch.int32, device=x.device)
        if topk_weights is None:
            topk_weights = torch.ones_like(handle.topk_idx, dtype=torch.float32, device=x.device)
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
            0 if handle.token_metadata_at_forward is None else handle.token_metadata_at_forward.data_ptr(),
            0 if handle.channel_linked_list is None else handle.channel_linked_list.data_ptr(),
            active_ranks.data_ptr(),
            handle.num_experts,
            handle.num_max_tokens_per_rank,
            handle.do_expand,
            num_sms if num_sms is not None else handle.num_sms,
            async_with_compute_stream,
            _native_current_stream_ptr(),
            combined_x.data_ptr(),
        )
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
            *(() if handle.token_metadata_at_forward is None else (handle.token_metadata_at_forward,)),
            *(() if handle.channel_linked_list is None else (handle.channel_linked_list,)),
        )
        return (
            combined_x,
            None,
            EventOverlap(event, tensors_to_record if async_with_compute_stream else None),
        )


__all__ = ["ElasticBuffer", "EPHandle", "EventOverlap"]
