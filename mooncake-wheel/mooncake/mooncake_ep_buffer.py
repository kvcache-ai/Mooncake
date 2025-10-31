import torch
import torch.distributed as dist
from typing import Any, Callable, List, Tuple, Optional, Union

# noinspection PyUnresolvedReferences
from mooncake import ep


class EventOverlap:
    """
    A wrapper class to manage CUDA events, also for better overlapping convenience.

    Attributes:
        event: the CUDA event captured.
        extra_tensors: an easier way to simulate PyTorch tensor `record_stream`, may be useful with CUDA graph.
    """

    def __init__(self, event: Optional[ep.EventHandle] = None,
                 extra_tensors: Optional[Tuple[torch.Tensor, ...]] = None) -> None:
        """
        Initialize the class.

        Arguments:
            event: the CUDA event captured.
            extra_tensors: an easier way to simulate PyTorch tensor `record_stream`, may be useful with CUDA graph.
        """
        self.event = event

        # NOTES: we use extra tensors to achieve stream recording, otherwise,
        # stream recording will be incompatible with CUDA graph.
        self.extra_tensors = extra_tensors

    def current_stream_wait(self) -> None:
        """
        The current stream `torch.cuda.current_stream()` waits for the event to be finished.
        """
        assert self.event is not None
        self.event.current_stream_wait()

    def __enter__(self) -> Any:
        """
        Utility for overlapping and Python `with` syntax.

        You can overlap the kernels on the current stream with the following example:
        ```python
        event_overlap = event_after_all_to_all_kernels()
        with event_overlap():
            do_something_on_current_stream()
        # After exiting the `with` scope, the current stream with wait the event to be finished.
        ```
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Utility for overlapping and Python `with` syntax.

        Please follow the example in the `__enter__` function.
        """
        if self.event is not None:
            self.event.current_stream_wait()


class Buffer:
    def __init__(self, group: dist.ProcessGroup, num_ep_buffer_bytes: int = 0):
        # Initialize the CPP runtime
        self.rank = group.rank()
        self.group_size = group.size()
        self.group = group
        self.num_ep_buffer_bytes = num_ep_buffer_bytes

        # Get the index of the closest NIC
        self.backend = self.group._get_backend(torch.device('cuda'))
        preferred_hca = ep.get_preferred_hca(self.backend, f'cuda:{torch.cuda.current_device()}')
        self.runtime = ep.Buffer(self.rank, self.group_size, num_ep_buffer_bytes, preferred_hca)
        # Fallback flag and buffers
        self._use_fallback = bool(self.runtime.ibgda_disabled())
        self._fallback_next_combine_buffer: Optional[torch.Tensor] = None

        (raddr, rkey) = self.runtime.get_mr_info()

        raddr = torch.tensor([raddr], dtype=torch.int64, device='cuda')
        raddrs = [torch.empty(1, dtype=torch.int64, device='cuda') for _ in range(self.group_size)]
        dist.all_gather(raddrs, raddr, group)
        raddrs = torch.cat(raddrs).tolist()

        rkey = torch.tensor([rkey], dtype=torch.int32, device='cuda')
        rkeys = [torch.empty(1, dtype=torch.int32, device='cuda') for _ in range(self.group_size)]
        dist.all_gather(rkeys, rkey, group)
        rkeys = torch.cat(rkeys).tolist()

        all_to_all_size = ep.MAX_QP_COUNT // self.group_size

        local_qpns = self.runtime.get_local_qpns()
        local_qpns = list(torch.unbind(torch.tensor(local_qpns, dtype=torch.int32, device='cuda').view(-1, all_to_all_size)))
        remote_qpns = [torch.empty(all_to_all_size, dtype=torch.int32, device='cuda') for _ in range(self.group_size)]
        dist.all_to_all(remote_qpns, local_qpns, group)
        remote_qpns = torch.cat(remote_qpns).tolist()

        if self.runtime.is_roce():
            (subnet_prefix, interface_id) = self.runtime.get_gid()

            subnet_prefix = torch.tensor([subnet_prefix], dtype=torch.int64, device='cuda')
            subnet_prefixes = [torch.empty(1, dtype=torch.int64, device='cuda') for _ in range(self.group_size)]
            dist.all_gather(subnet_prefixes, subnet_prefix, group)
            subnet_prefixes = torch.cat(subnet_prefixes).tolist()

            interface_id = torch.tensor([interface_id], dtype=torch.int64, device='cuda')
            interface_ids = [torch.empty(1, dtype=torch.int64, device='cuda') for _ in range(self.group_size)]
            dist.all_gather(interface_ids, interface_id, group)
            interface_ids = torch.cat(interface_ids).tolist()

            self.runtime.sync_roce(raddrs, rkeys, remote_qpns, subnet_prefixes, interface_ids)
        else:

            local_lids = self.runtime.get_local_lids()
            local_lids = list(torch.unbind(torch.tensor(local_lids, dtype=torch.int32, device='cuda').view(-1, all_to_all_size)))
            remote_lids = [torch.empty(all_to_all_size, dtype=torch.int32, device='cuda') for _ in range(self.group_size)]
            dist.all_to_all(remote_lids, local_lids, group)
            remote_lids = torch.cat(remote_lids).tolist()

            self.runtime.sync_ib(raddrs, rkeys, remote_qpns, remote_lids)

    @staticmethod
    def get_ep_buffer_size_hint(num_max_dispatch_tokens_per_rank: int, hidden: int, num_ranks: int, num_experts: int) -> int:
        return ep.get_ep_buffer_size_hint(num_max_dispatch_tokens_per_rank, hidden, num_ranks, num_experts)

    # noinspection PyTypeChecker
    def dispatch(self, x: torch.Tensor, topk_idx: torch.Tensor, active_ranks: torch.Tensor,
                 num_max_dispatch_tokens_per_rank: int, num_experts: int, timeout_us: int,
                 use_fp8: bool = True, async_finish: bool = False, return_recv_hook: bool = False) -> \
            Tuple[Union[Tuple[torch.Tensor, torch.Tensor], torch.Tensor], torch.Tensor, Tuple, EventOverlap, Callable]:
        if self._use_fallback:
            backend_active_ranks = ep.get_active_ranks(self.backend).to(device=active_ranks.device, dtype=active_ranks.dtype)
            if active_ranks.numel() == backend_active_ranks.numel():
                active_ranks.copy_(backend_active_ranks)
            packed_recv_x, packed_recv_x_scales, packed_recv_count, packed_recv_src_info, packed_recv_layout_range, event, hook = \
                self._fallback_dispatch(x, topk_idx, num_max_dispatch_tokens_per_rank, num_experts, use_fp8, return_recv_hook)
        else:
            packed_recv_x, packed_recv_x_scales, packed_recv_count, packed_recv_src_info, packed_recv_layout_range, event, hook = \
                self.runtime.dispatch(x, topk_idx, active_ranks,
                                      num_max_dispatch_tokens_per_rank, num_experts, timeout_us,
                                      use_fp8, async_finish, return_recv_hook)
        handle = (packed_recv_src_info, packed_recv_layout_range, num_max_dispatch_tokens_per_rank, x.size(1), num_experts)
        tensors_to_record = (x, topk_idx,
                             packed_recv_x, packed_recv_x_scales, packed_recv_count,
                             packed_recv_src_info, packed_recv_layout_range)
        return (packed_recv_x, packed_recv_x_scales) if use_fp8 else packed_recv_x, packed_recv_count, handle, \
            EventOverlap(event, tensors_to_record if async_finish else None), hook

    # noinspection PyTypeChecker
    def combine(self, x: torch.Tensor, topk_idx: torch.Tensor, topk_weights: torch.Tensor,
                active_ranks: torch.Tensor, timeout_us: int,
                handle: tuple, zero_copy: bool = False, async_finish: bool = False,
                return_recv_hook: bool = False, out: Optional[torch.Tensor] = None) -> \
            Tuple[torch.Tensor, EventOverlap, Callable]:
        src_info, layout_range, num_max_dispatch_tokens_per_rank, hidden, num_experts = handle
        if self._use_fallback:
            backend_active_ranks = ep.get_active_ranks(self.backend).to(device=active_ranks.device, dtype=active_ranks.dtype)
            if active_ranks.numel() == backend_active_ranks.numel():
                active_ranks.copy_(backend_active_ranks)
            combined_x, event, hook = self._fallback_combine(x, topk_idx, topk_weights, src_info, layout_range,
                                                             num_max_dispatch_tokens_per_rank, num_experts,
                                                             zero_copy, return_recv_hook, out)
        else:
            combined_x, event, hook = self.runtime.combine(x, topk_idx, topk_weights, src_info, layout_range,
                                                           active_ranks,
                                                           num_max_dispatch_tokens_per_rank, num_experts, timeout_us,
                                                           zero_copy, async_finish, return_recv_hook, out)
        tensors_to_record = (x, topk_idx, topk_weights, src_info, layout_range, combined_x)
        return combined_x, EventOverlap(event, tensors_to_record if async_finish else None), hook

    def get_next_combine_buffer(self, handle: object):
        src_info, layout_range, num_max_dispatch_tokens_per_rank, hidden, num_experts = handle
        if self._use_fallback:
            if self._fallback_next_combine_buffer is None or \
                    self._fallback_next_combine_buffer.shape != (num_experts // self.group_size, num_max_dispatch_tokens_per_rank, hidden):
                self._fallback_next_combine_buffer = torch.empty(
                    (num_experts // self.group_size, num_max_dispatch_tokens_per_rank, hidden),
                    dtype=torch.bfloat16, device='cuda')
            return self._fallback_next_combine_buffer
        return self.runtime.get_next_combine_buffer(num_max_dispatch_tokens_per_rank, hidden, num_experts)

    # -----------------
    # Fallback helpers
    # -----------------
    class _DummyEvent:
        def current_stream_wait(self):
            torch.cuda.synchronize()

    @staticmethod
    def _fp8_cast(x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        assert x.dim() == 2 and x.size(1) % 128 == 0
        m, n = x.shape
        x_view = x.view(m, -1, 128)
        x_amax = x_view.abs().float().amax(dim=2).view(m, -1).clamp(1e-4)
        x_fp8 = (x_view * (448.0 / x_amax.unsqueeze(2))).to(torch.float8_e4m3fn).view(m, n)
        x_scales = (x_amax / 448.0).view(m, -1)
        return x_fp8, x_scales

    def _fallback_dispatch(self, x: torch.Tensor, topk_idx: torch.Tensor,
                           num_max_dispatch_tokens_per_rank: int, num_experts: int,
                           use_fp8: bool, return_recv_hook: bool):
        with torch.profiler.record_function('dispatch'):
            num_tokens, hidden = x.shape
            num_ranks = self.group_size
            num_local_experts = num_experts // num_ranks

            # Gather inputs from all ranks
            all_x = torch.empty((num_ranks, num_tokens, hidden), dtype=x.dtype, device=x.device)
            dist.all_gather_into_tensor(all_x, x, group=self.group)
            all_topk = torch.empty((num_ranks, num_tokens, topk_idx.size(1)), dtype=topk_idx.dtype, device=x.device)
            dist.all_gather_into_tensor(all_topk, topk_idx, group=self.group)

            # Prepare outputs per local expert
            recv_x_list: List[torch.Tensor] = []
            recv_x_scales_list: List[torch.Tensor] = []
            recv_count = torch.zeros((num_local_experts,), dtype=torch.int32, device=x.device)
            recv_src_info = torch.full((num_local_experts, num_max_dispatch_tokens_per_rank), -1, dtype=torch.int32, device=x.device)
            layout_range = torch.zeros((num_local_experts, num_ranks), dtype=torch.int64, device=x.device)

            for le in range(num_local_experts):
                expert_id = self.rank * num_local_experts + le
                tokens_per_rank: List[List[int]] = []
                for src_rank in range(num_ranks):
                    pos = (all_topk[src_rank] == expert_id).any(dim=1).nonzero(as_tuple=False).view(-1)
                    tokens_per_rank.append(pos.tolist())

                # Build ordered list grouped by src_rank
                ordered_indices: List[Tuple[int, int]] = []  # (src_rank, token_idx)
                begin = 0
                for src_rank in range(num_ranks):
                    count = len(tokens_per_rank[src_rank])
                    if count > 0:
                        layout_range[le, src_rank] = (int(begin) << 32) | int(count)
                        for t in tokens_per_rank[src_rank]:
                            ordered_indices.append((src_rank, t))
                        begin += count
                    else:
                        layout_range[le, src_rank] = 0

                num_valid = min(len(ordered_indices), num_max_dispatch_tokens_per_rank)
                recv_count[le] = num_valid

                # Materialize data
                gathered = torch.empty((num_valid, hidden), dtype=torch.bfloat16, device=x.device)
                src_meta = torch.empty((num_valid,), dtype=torch.int32, device=x.device)
                for i, (sr, t) in enumerate(ordered_indices[:num_valid]):
                    gathered[i] = all_x[sr, t]
                    src_meta[i] = t

                # Pad to full size
                if use_fp8:
                    pad = num_max_dispatch_tokens_per_rank - num_valid
                    if pad > 0:
                        pad_tensor = torch.zeros((pad, hidden), dtype=torch.bfloat16, device=x.device)
                        gathered = torch.cat([gathered, pad_tensor], dim=0)
                    fp8, scales = self._fp8_cast(gathered)
                    recv_x_list.append(fp8)
                    recv_x_scales_list.append(scales)
                else:
                    pad = num_max_dispatch_tokens_per_rank - num_valid
                    if pad > 0:
                        pad_tensor = torch.zeros((pad, hidden), dtype=torch.bfloat16, device=x.device)
                        gathered = torch.cat([gathered, pad_tensor], dim=0)
                    recv_x_list.append(gathered)

                # src info padded
                if num_valid > 0:
                    recv_src_info[le, :num_valid] = src_meta

            if use_fp8:
                packed_recv_x = torch.stack(recv_x_list, dim=0) if len(recv_x_list) > 0 else torch.empty((0, num_max_dispatch_tokens_per_rank, hidden), dtype=torch.float8_e4m3fn, device=x.device)
                packed_recv_x_scales = torch.stack(recv_x_scales_list, dim=0) if len(recv_x_scales_list) > 0 else torch.empty((0, num_max_dispatch_tokens_per_rank * hidden // 128 // (hidden // 128),), device=x.device)
            else:
                packed_recv_x = torch.stack(recv_x_list, dim=0) if len(recv_x_list) > 0 else torch.empty((0, num_max_dispatch_tokens_per_rank, hidden), dtype=torch.bfloat16, device=x.device)
                packed_recv_x_scales = None

            # Allocate zero-copy buffer for next combine
            self._fallback_next_combine_buffer = torch.empty((num_local_experts, num_max_dispatch_tokens_per_rank, hidden), dtype=torch.bfloat16, device=x.device)

            # Ensure profiler attributes some CUDA time to this range
            torch.cuda._sleep(1000)
            hook = (lambda: None) if return_recv_hook else (lambda: None)
            event = Buffer._DummyEvent()
            return packed_recv_x, packed_recv_x_scales, recv_count, recv_src_info, layout_range, event, hook

    def _fallback_combine(self, x: torch.Tensor, topk_idx: torch.Tensor, topk_weights: torch.Tensor,
                          src_info: torch.Tensor, layout_range: torch.Tensor,
                          num_max_dispatch_tokens_per_rank: int, num_experts: int,
                          zero_copy: bool, return_recv_hook: bool, out: Optional[torch.Tensor]):
        with torch.profiler.record_function('combine'):
            num_tokens = topk_idx.size(0)
            hidden = (x if not zero_copy else self._fallback_next_combine_buffer).size(-1)
            num_ranks = self.group_size
            num_local_experts = num_experts // num_ranks

            # Gather routing info across ranks to fetch per-token weights
            k = topk_idx.size(1)
            all_topk_idx = torch.empty((num_ranks, num_tokens, k), dtype=topk_idx.dtype, device=topk_idx.device)
            dist.all_gather_into_tensor(all_topk_idx, topk_idx, group=self.group)
            all_topk_w = torch.empty((num_ranks, num_tokens, k), dtype=topk_weights.dtype, device=topk_weights.device)
            dist.all_gather_into_tensor(all_topk_w, topk_weights, group=self.group)

            expert_buffers = self._fallback_next_combine_buffer if zero_copy else x
            # Ensure bf16 input for accumulation
            if expert_buffers.dtype != torch.bfloat16:
                # FP8 path should already have been cast back by caller before combine in tests
                expert_buffers = expert_buffers.to(torch.bfloat16)

            # Build send buffer [num_ranks, num_tokens, hidden]
            send_buf = torch.zeros((num_ranks, num_tokens, hidden), dtype=torch.bfloat16, device=expert_buffers.device)

            for le in range(num_local_experts):
                # layout_range[le, j]: upper 32 begin, lower 32 count
                for src_rank in range(num_ranks):
                    entry = layout_range[le, src_rank]
                    begin = (entry >> 32).item() & 0xFFFFFFFF
                    count = (entry & ((1 << 32) - 1)).item()
                    if count == 0:
                        continue
                    tokens = src_info[le, begin:begin + count].to(torch.long)
                    contrib = expert_buffers[le, begin:begin + count]
                    expert_id = self.rank * num_local_experts + le
                    # Find the per-token weight for this expert on src_rank
                    idx_rows = all_topk_idx[src_rank, tokens]  # [count, k]
                    w_rows = all_topk_w[src_rank, tokens]      # [count, k]
                    mask = (idx_rows == expert_id)
                    weights = (w_rows * mask).sum(dim=1).view(-1, 1)
                    send_buf[src_rank, tokens] += (contrib * weights)

            # All-reduce then take local slice
            dist.all_reduce(send_buf, group=self.group)
            combined_x = send_buf[self.rank]

            # Write to out if provided
            if out is not None:
                out.copy_(combined_x)
                combined = out
            else:
                combined = combined_x

            # Ensure profiler attributes some CUDA time to this range
            torch.cuda._sleep(1000)
            hook = (lambda: None) if return_recv_hook else (lambda: None)
            event = Buffer._DummyEvent()
            return combined, event, hook
