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
        backend = self.group._get_backend(torch.device('cuda'))
        preferred_hca = ep.get_preferred_hca(backend, f'cuda:{torch.cuda.current_device()}')
        nic_id = int(preferred_hca.split('_')[1])
        self.runtime = ep.Buffer(self.rank, self.group_size, num_ep_buffer_bytes, nic_id)

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
        combined_x, event, hook = self.runtime.combine(x, topk_idx, topk_weights, src_info, layout_range,
                                                       active_ranks,
                                                       num_max_dispatch_tokens_per_rank, num_experts, timeout_us,
                                                       zero_copy, async_finish, return_recv_hook, out)
        tensors_to_record = (x, topk_idx, topk_weights, src_info, layout_range, combined_x)
        return combined_x, EventOverlap(event, tensors_to_record if async_finish else None), hook

    def get_next_combine_buffer(self, handle: object):
        src_info, layout_range, num_max_dispatch_tokens_per_rank, hidden, num_experts = handle
        return self.runtime.get_next_combine_buffer(num_max_dispatch_tokens_per_rank, hidden, num_experts)
