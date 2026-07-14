import random
import os
import subprocess
import torch
import torch.distributed as dist
from functools import partial

from mooncake.mooncake_ep_buffer import Buffer
from ep_test_utils import init_dist, bench, bench_kineto, calc_diff, hash_tensor, per_token_cast_back

_USE_MACA = (
    os.getenv("MOONCAKE_EP_USE_MACA", "").upper() in {"1", "ON", "TRUE", "YES"}
    or bool(getattr(torch.version, "maca", None))
)
_EP_DIAG_TIMING = os.getenv("MOONCAKE_EP_DIAG_TIMING", "").upper() in {
    "1",
    "ON",
    "TRUE",
    "YES",
}
_EP_DIAG_ITERS = int(os.getenv("MOONCAKE_EP_DIAG_ITERS", "10"))
_EP_DIAG_NAMES = (
    "phase_ack_wait",
    "mark_wait_phase_ack",
    "dispatch_recv_wait",
    "combine_recv_wait",
)


def _get_ep_diag_clock_rate_khz() -> int:
    env_clock = os.getenv("MOONCAKE_EP_DIAG_CLOCK_KHZ")
    if env_clock:
        return int(env_clock)

    props = torch.cuda.get_device_properties(torch.cuda.current_device())
    clock_rate_khz = getattr(props, "clock_rate", 0)
    if clock_rate_khz:
        return int(clock_rate_khz)

    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                f"--id={torch.cuda.current_device()}",
                "--query-gpu=clocks.sm",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return int(result.stdout.strip().splitlines()[0]) * 1000
    except (OSError, subprocess.SubprocessError, ValueError, IndexError):
        return 0


def _print_ep_diag(rank: int, diagnostic: torch.Tensor):
    torch.cuda.synchronize()
    stats = diagnostic.cpu()
    clock_rate_khz = _get_ep_diag_clock_rate_khz()
    for idx, name in enumerate(_EP_DIAG_NAMES):
        count = int(stats[idx, 0].item())
        total_ticks = int(stats[idx, 1].item())
        max_ticks = int(stats[idx, 2].item())
        if count == 0:
            continue
        avg_ticks = total_ticks / count
        if clock_rate_khz:
            avg_us = avg_ticks * 1000.0 / clock_rate_khz
            max_us = max_ticks * 1000.0 / clock_rate_khz
            print(
                f"[rank {rank}] EP diag {name}: count={count}, "
                f"avg={avg_us:.2f} us ({avg_ticks:.0f} ticks), "
                f"max={max_us:.2f} us ({max_ticks} ticks)",
                flush=True,
            )
        else:
            print(
                f"[rank {rank}] EP diag {name}: count={count}, "
                f"avg={avg_ticks:.0f} ticks, max={max_ticks} ticks",
                flush=True,
            )


def test_main(num_tokens: int, hidden: int, num_experts: int, num_topk: int,
              rank: int, num_ranks: int, group: dist.ProcessGroup, cpu_group: dist.ProcessGroup, buffer: Buffer, seed: int = 0):
    torch.manual_seed(seed + rank)
    random.seed(seed + rank)

    assert num_experts % num_ranks == 0
    num_local_experts = num_experts // num_ranks

    # NOTES: the integers greater than 256 exceeds the BF16 precision limit
    rank_offset = 128
    assert num_ranks - rank_offset < 257, 'Too many ranks (exceeding test precision limit)'

    x = torch.ones((num_tokens, hidden), dtype=torch.bfloat16, device='cuda') * (rank - rank_offset)
    x[:, -128:] = torch.arange(num_tokens, device='cuda').to(torch.bfloat16).view(-1, 1)
    scores = torch.randn((num_tokens, num_experts), dtype=torch.float32, device='cuda').abs() + 1
    topk_idx = torch.topk(scores, num_topk, dim=-1, largest=True, sorted=True)[1]
    topk_weights = torch.randn((num_tokens, num_topk), dtype=torch.float32, device='cuda').abs()

    # Randomly mask some positions
    for i in range(10):
        topk_idx[random.randint(0, num_tokens - 1), random.randint(0, num_topk - 1)] = -1

    # Check dispatch correctness
    do_check = True
    hash_value, num_times = 0, 0
    active_ranks = torch.ones((num_tokens, ), dtype=torch.int32, device='cuda')
    for return_recv_hook in (False, True):
        for dispatch_use_fp8 in ([False] if _USE_MACA else [False, True]):
            num_times += 1
            for i in range((num_times % 2) + 1):
                packed_recv_x, packed_recv_count, handle, event, hook = \
                    buffer.dispatch(x, topk_idx, active_ranks, num_tokens, num_experts, -1, use_fp8=dispatch_use_fp8,
                                    async_finish=not return_recv_hook, return_recv_hook=return_recv_hook)
                hook() if return_recv_hook else event.current_stream_wait()
            packed_recv_x = (packed_recv_x[0], packed_recv_x[1].contiguous()) if dispatch_use_fp8 else packed_recv_x
            simulated_gemm_x = per_token_cast_back(packed_recv_x[0].view(-1, hidden), packed_recv_x[1].view(-1, hidden // 128)).view(packed_recv_x[0].shape) \
                if dispatch_use_fp8 else packed_recv_x.clone()
            all_topk_idx = torch.empty((num_ranks, num_tokens, num_topk), dtype=topk_idx.dtype, device='cuda')
            dist.all_gather_into_tensor(all_topk_idx, topk_idx, group=group)
            for i in range(num_local_experts if do_check else 0):
                expert_id = rank * num_local_experts + i
                recv_x = per_token_cast_back(packed_recv_x[0][i], packed_recv_x[1][i]) if dispatch_use_fp8 else packed_recv_x[i]
                recv_count, recv_src_info, recv_layout_range = packed_recv_count[i], handle[0][i], handle[1][i]

                # Check expert indices
                int_mask = (2 ** 32) - 1
                num_valid_tokens = recv_count.item()
                assert num_valid_tokens == (recv_layout_range & int_mask).sum().item(), f'{num_valid_tokens} != {recv_layout_range & int_mask}.sum().item()'
                assert num_valid_tokens == (all_topk_idx == expert_id).sum().item(), f'{num_valid_tokens} != {(all_topk_idx == expert_id).sum().item()}'

                # Check received data
                recv_x = recv_x[:num_valid_tokens]
                recv_x_amin = recv_x[:, :-128].amin(dim=-1)
                recv_src_info = recv_src_info[:num_valid_tokens]
                assert torch.equal(recv_x_amin, recv_x[:, :-128].amax(dim=-1))
                assert (recv_x[:, -128:] - recv_src_info.view(-1, 1) % num_tokens).sum().item() == 0
                for j in range(num_ranks):
                    begin_idx, count = (recv_layout_range[j] >> 32).item(), (recv_layout_range[j] & int_mask).item()
                    assert (recv_x_amin == j - rank_offset).sum().item() == (all_topk_idx[j] == expert_id).sum().item()
                    assert (recv_x[begin_idx:begin_idx + count][:-128] - j).sum().item() == 0
                if dispatch_use_fp8:
                    hash_value ^= hash_tensor(packed_recv_x[0][i, :num_valid_tokens])
                    hash_value ^= hash_tensor(packed_recv_x[1][i, :num_valid_tokens])
                else:
                    hash_value ^= hash_tensor(packed_recv_x[i, :num_valid_tokens])

            # Check combine correctness
            for zero_copy in (False, True):
                if zero_copy:
                    buffer.get_next_combine_buffer(handle)[:, :, :] = simulated_gemm_x
                out = torch.empty((num_tokens, hidden), dtype=torch.bfloat16, device='cuda')
                combined_x, event, hook = buffer.combine(simulated_gemm_x, topk_idx, topk_weights, active_ranks, -1, handle,
                                                         async_finish=not return_recv_hook, zero_copy=zero_copy,
                                                         return_recv_hook=return_recv_hook, out=out)
                hook() if return_recv_hook else event.current_stream_wait()
                if do_check:
                    diff = calc_diff(x * topk_weights.masked_fill(topk_idx == -1, 0).sum(dim=1).view(-1, 1), combined_x)
                    assert torch.isnan(combined_x).sum().item() == 0
                    assert diff < 1e-5, f'Error: {diff=}, {zero_copy=}'
                    hash_value ^= hash_tensor(combined_x)

    def create_test_cast_with_outliers(num_outliers):
        tmp = torch.randn((num_tokens, hidden), dtype=torch.bfloat16, device='cuda')
        tmp /= tmp.abs().amax(dim=1).view(-1, 1)
        assert tmp.abs().amax().item() <= 1

        # Create some amax outliers
        for i in range(num_outliers):
            tmp[random.randint(0, num_tokens - 1)] *= 1e3
        return tmp

    # noinspection PyShadowingNames
    def large_gemm_with_hook(hook):
        mat_0 = torch.randn((8192, 8192), dtype=torch.float)
        mat_1 = torch.randn((8192, 8192), dtype=torch.float)
        mat_0 @ mat_1
        hook()

    # noinspection PyShadowingNames
    def test_func(zero_copy: bool, return_recv_hook: bool, diagnostic=None):
        recv_x, recv_count, handle, event, hook = \
            buffer.dispatch(x, topk_idx, active_ranks, num_tokens, num_experts, -1,
                            async_finish=False, return_recv_hook=return_recv_hook,
                            diagnostic=diagnostic)
        if return_recv_hook:
            large_gemm_with_hook(hook)
        else:
            event.current_stream_wait()
        if zero_copy:
            buffer.get_next_combine_buffer(handle)[:, :, :] = simulated_gemm_x
        combined_x, event, hook = buffer.combine(simulated_gemm_x, topk_idx, topk_weights, active_ranks, -1, handle,
                                                 zero_copy=zero_copy, return_recv_hook=return_recv_hook,
                                                 diagnostic=diagnostic)
        if return_recv_hook:
            large_gemm_with_hook(hook)
        else:
            event.current_stream_wait()

    # Calculate bandwidth
    num_fp8_bytes, num_bf16_bytes = (hidden + hidden / 128 * 4 + 16), hidden * 2
    num_dispatch_comm_bytes, num_combine_comm_bytes = 0, 0
    for i in range(num_tokens):
        num_selections = (topk_idx[i] != -1).sum().item()
        num_dispatch_comm_bytes += num_fp8_bytes * num_selections
        num_combine_comm_bytes += num_bf16_bytes * num_selections

    # Dispatch + combine testing
    avg_t, min_t, max_t = bench(partial(test_func, zero_copy=False, return_recv_hook=False))
    print(f'[rank {rank}] Dispatch + combine bandwidth: {(num_dispatch_comm_bytes + num_combine_comm_bytes) / 1e9 / avg_t:.2f} GB/s, '
          f'avg_t={avg_t * 1e6:.2f} us, min_t={min_t * 1e6:.2f} us, max_t={max_t * 1e6:.2f} us', flush=True)

    if _EP_DIAG_TIMING:
        diagnostic = torch.zeros((len(_EP_DIAG_NAMES), 3), dtype=torch.int64, device="cuda")
        for _ in range(_EP_DIAG_ITERS):
            test_func(zero_copy=False, return_recv_hook=False, diagnostic=diagnostic)
        _print_ep_diag(rank, diagnostic)

    # Separate profiling
    # Skip profiling in fallback mode as kernels are Python functions, not CUDA kernels
    if not buffer._use_fallback:
        for return_recv_hook in (False, True):
            for profile_rank in range(num_ranks):
                cpu_group.barrier()
                dispatch_t, combine_t = bench_kineto(
                    partial(test_func, zero_copy=True, return_recv_hook=return_recv_hook),
                    kernel_names=('dispatch', 'combine'), barrier_comm_profiling=True,
                    suppress_kineto_output=True, profile_enabled=(rank == profile_rank))
                if rank != profile_rank:
                    continue
                if not return_recv_hook:
                    # In fallback mode, kernels may not be found (they're Python functions)
                    # So we skip bandwidth calculation if time is 0
                    if dispatch_t > 0 and combine_t > 0:
                        print(f'[rank {rank}] Dispatch bandwidth: {num_dispatch_comm_bytes / 1e9 / dispatch_t:.2f} GB/s, avg_t={dispatch_t * 1e6:.2f} us | '
                              f'Combine bandwidth: {num_combine_comm_bytes / 1e9 / combine_t:.2f} GB/s, avg_t={combine_t * 1e6:.2f} us', flush=True)
                    else:
                        print(f'[rank {rank}] Profiling skipped (kernels not found in CUDA profiler)', flush=True)
                else:
                    if dispatch_t > 0 and combine_t > 0:
                        print(f'[rank {rank}] Dispatch send/recv time: {dispatch_t * 2 * 1e6:.2f} us | '
                              f'Combine send/recv time: {combine_t * 2 * 1e6:.2f} us', flush=True)
                    else:
                        print(f'[rank {rank}] Profiling skipped (kernels not found in CUDA profiler)', flush=True)
    else:
        if rank == 0:
            print(f'[rank {rank}] Profiling skipped (fallback mode - using Python implementation)', flush=True)

    return hash_value


# noinspection PyUnboundLocalVariable
def test_loop(local_rank: int, num_local_ranks: int):
    rank, num_ranks, group, cpu_group = init_dist(local_rank, num_local_ranks)
    num_tokens, hidden, num_topk, num_experts = 128, 7168, 8, 288

    num_ep_buffer_bytes = Buffer.get_ep_buffer_size_hint(num_tokens, hidden, num_ranks, num_experts)
    if local_rank == 0:
        print(f'Allocating buffer size: {num_ep_buffer_bytes / 1e6} MB ...', flush=True)
    buffer = Buffer(group, num_ep_buffer_bytes=num_ep_buffer_bytes)
    buffer.update_ep_member()

    test_main(num_tokens, hidden, num_experts, num_topk, rank, num_ranks, group, cpu_group, buffer, seed=1)

    do_pressure_test = False
    for seed in range(int(1e9) if do_pressure_test else 0):
        if local_rank == 0:
            print(f'Testing with seed {seed} ...', flush=True)
        ref_hash = test_main(num_tokens, hidden, num_experts, num_topk, rank, num_ranks, group, cpu_group, buffer, seed=seed)
        for i in range(20):
            assert test_main(num_tokens, hidden, num_experts, num_topk, rank, num_ranks, group, cpu_group, buffer, seed=seed) == ref_hash, f'Error: seed={seed}'

    # Cleanup with error handling (TCPStore warnings are expected in mooncake backend)
    try:
        dist.destroy_process_group()
    except Exception as e:
        # Ignore cleanup errors (TCPStore connection errors are expected)
        pass


if __name__ == '__main__':
    # TODO: you may modify NUMA binding for less CPU overhead
    num_processes = 8
    torch.multiprocessing.spawn(test_loop, args=(num_processes,), nprocs=num_processes)
