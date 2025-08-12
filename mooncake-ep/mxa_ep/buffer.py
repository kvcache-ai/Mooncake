import numpy as np
import torch
import torch.distributed as dist
from typing import Callable, List, Tuple, Optional, Union

# noinspection PyUnresolvedReferences
import mxa_ep_cpp
from .utils import EventOverlap

class Buffer:
    def __init__(self, group: dist.ProcessGroup, num_mxa_bytes: int = 0, bytes_reserved: int = 0):
        # Initialize the CPP runtime
        self.rank = group.rank()
        self.group_size = group.size()
        self.group = group
        self.num_mxa_bytes = num_mxa_bytes
        self.runtime = mxa_ep_cpp.Buffer(self.rank, self.group_size, num_mxa_bytes, bytes_reserved)

        (raddr, rkey) = self.runtime.get_mr_info()

        raddr = torch.tensor([raddr], dtype=torch.int64)
        raddrs = [torch.empty(1, dtype=torch.int64) for _ in range(self.group_size)]
        dist.all_gather(raddrs, raddr)
        raddrs = torch.cat(raddrs).tolist()

        rkey = torch.tensor([rkey], dtype=torch.int32)
        rkeys = [torch.empty(1, dtype=torch.int32) for _ in range(self.group_size)]
        dist.all_gather(rkeys, rkey)
        rkeys = torch.cat(rkeys).tolist()

        (subnet_prefix, interface_id) = self.runtime.get_gid()

        subnet_prefix = torch.tensor([subnet_prefix], dtype=torch.int64)
        subnet_prefixes = [torch.empty(1, dtype=torch.int64) for _ in range(self.group_size)]
        dist.all_gather(subnet_prefixes, subnet_prefix)
        subnet_prefixes = torch.cat(subnet_prefixes).tolist()

        interface_id = torch.tensor([interface_id], dtype=torch.int64)
        interface_ids = [torch.empty(1, dtype=torch.int64) for _ in range(self.group_size)]
        dist.all_gather(interface_ids, interface_id)
        interface_ids = torch.cat(interface_ids).tolist()

        local_qpns = self.runtime.get_local_qpns()
        remote_qpns = [torch.empty(1, dtype=torch.int32) for _ in range(self.group_size)]
        dist.all_to_all(remote_qpns, local_qpns)
        remote_qpns = torch.cat(remote_qpns).tolist()

        self.runtime.sync(raddrs, rkeys, remote_qpns, subnet_prefixes, interface_ids)

    @staticmethod
    def get_mxa_size_hint(num_max_dispatch_tokens_per_rank: int, hidden: int, num_ranks: int, num_experts: int, bytes_reserved: int = 0) -> int:
        return mxa_ep_cpp.get_mxa_size_hint(num_max_dispatch_tokens_per_rank, hidden, num_ranks, num_experts, bytes_reserved)

    # noinspection PyTypeChecker
    def dispatch(self, x: torch.Tensor, topk_idx: torch.Tensor, broken_nodes: torch.Tensor,
                 num_max_dispatch_tokens_per_rank: int, num_experts: int, timeout_us: int,
                 use_fp8: bool = True, async_finish: bool = False, return_recv_hook: bool = False) -> \
            Tuple[Tuple[torch.Tensor, torch.Tensor], torch.Tensor, Tuple, EventOverlap, Callable]:
        packed_recv_x, packed_recv_x_scales, packed_recv_count, packed_recv_src_info, packed_recv_layout_range, event, hook = \
            self.runtime.dispatch(x, topk_idx, broken_nodes,
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
                gathered_experts: torch.Tensor, timeout_us: int,
                handle: tuple, zero_copy: bool = False, async_finish: bool = False,
                return_recv_hook: bool = False, out: Optional[torch.Tensor] = None) -> \
            Tuple[torch.Tensor, torch.Tensor, EventOverlap, Callable]:
        src_info, layout_range, num_max_dispatch_tokens_per_rank, hidden, num_experts = handle
        combined_x, event, hook = self.runtime.combine(x, topk_idx, topk_weights, src_info, layout_range,
                                                       gathered_experts,
                                                       num_max_dispatch_tokens_per_rank, num_experts, timeout_us,
                                                       zero_copy, async_finish, return_recv_hook, out)
        tensors_to_record = (x, topk_idx, topk_weights, src_info, layout_range, combined_x)
        return combined_x, EventOverlap(event, tensors_to_record if async_finish else None), hook

    def get_next_combine_buffer(self, handle: object):
        src_info, layout_range, num_max_dispatch_tokens_per_rank, hidden, num_experts = handle
        return self.runtime.get_next_combine_buffer(num_max_dispatch_tokens_per_rank, hidden, num_experts)

    def all_reduce_without(self, broken_nodes: torch.Tensor, x: torch.Tensor):
        self.runtime.all_reduce_without(broken_nodes, x)