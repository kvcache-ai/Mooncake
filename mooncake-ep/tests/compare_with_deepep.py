import os
import itertools
import torch
import torch.distributed as dist
import faulthandler

from deep_ep.buffer import Buffer as DeepEPBuffer
from mooncake.mooncake_ep_buffer import Buffer as MooncakeBuffer


def check_close(
    actual: torch.Tensor, expected: torch.Tensor, rank: int, msg: str, **kwargs
) -> int:
    if torch.allclose(actual, expected, **kwargs):
        return 0

    # same as torch.allclose
    rtol = kwargs.get("rtol", 1e-05)
    atol = kwargs.get("atol", 1e-08)

    a = actual.cpu().to(torch.float64)
    b = expected.cpu().to(torch.float64)

    a_flat = a.reshape(-1)
    b_flat = b.reshape(-1)
    diff = (a_flat - b_flat).abs()

    max_abs = float(diff.max().item())
    mean_abs = float(diff.mean().item())

    eps = 1e-12
    rel = diff / (b_flat.abs() + eps)
    max_rel = float(rel.max().item())

    tol = atol + rtol * b_flat.abs()
    above_mask = diff > tol
    count_above = int(above_mask.sum().item())
    total = diff.numel()
    frac_above = count_above / total if total > 0 else 0.0

    metrics = (
        f"max_abs={max_abs:.3e}, mean_abs={mean_abs:.3e}, "
        f"max_rel={max_rel:.3e}, {count_above}/{total} elements > tol ({frac_above:.2%})"
    )

    print(
        f"❌ Rank {rank} assert fail: {msg}. Metrics: {metrics}.\n"
        + f"Expected:\n{expected},\nbut got:\n{actual}.",
        flush=True,
    )
    return 1


class CUDATimer:
    def __init__(self):
        self.start_event = torch.cuda.Event(enable_timing=True)
        self.end_event = torch.cuda.Event(enable_timing=True)

    def __enter__(self):
        self.start_event.record()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_event.record()

    def get_time_ms(self) -> float:
        # We synchronize here *only* when the time is explicitly requested,
        # which naturally coincides with the correctness verification step.
        torch.cuda.synchronize()
        return self.start_event.elapsed_time(self.end_event)


def dequantize_fp8(x_fp8: torch.Tensor, scales: torch.Tensor) -> torch.Tensor:
    hidden = x_fp8.shape[-1]

    # FP8 shapes from dispatch: (num_experts, max_tokens, hidden)
    # Scales shape: (num_experts, max_tokens, hidden // 128)
    x_view = x_fp8.reshape(-1, hidden // 128, 128).float()
    scales_view = scales.reshape(-1, hidden // 128, 1).float()

    dequantized = (x_view * scales_view).reshape(x_fp8.shape)
    return dequantized.to(torch.bfloat16)


def run_test_iteration(
    group: dist.ProcessGroup,
    rank: int,
    num_ranks: int,
    num_tokens: int,
    hidden: int,
    num_experts: int,
    top_k: int,
    use_fp8: bool,
    zero_copy: bool,
    async_finish: bool,
    use_fallback: bool,
):
    """
    Executes a single end-to-end combination of dispatch -> expert -> combine
    for both MooncakeEP and DeepEP, validating correctness and recording latency.
    """
    # 1. Generate identical deterministic inputs across both implementations
    torch.manual_seed(2026_03_14 + rank)
    x = torch.randn(num_tokens, hidden, dtype=torch.bfloat16, device="cuda")
    scores = torch.randn((num_tokens, num_experts), dtype=torch.float32, device="cuda")
    topk_idx = torch.topk(scores, top_k, dim=-1)[1]
    topk_weights = torch.rand(num_tokens, top_k, dtype=torch.float32, device="cuda")

    # Active ranks: fully active for this test
    # Keep consistent with mooncake-wheel/tests/test_mooncake_ep.py:
    # active_ranks is a per-token mask; length == num_tokens.
    active_ranks = torch.ones((num_tokens,), dtype=torch.int32, device="cuda")

    failed_check_count = 0

    # 2. Initialize buffers
    if rank == 0:
        print("Initializing buffers...", flush=True)

    num_ep_buffer_bytes = MooncakeBuffer.get_ep_buffer_size_hint(
        num_tokens, hidden, num_ranks, num_experts
    )
    m_buf = MooncakeBuffer(group, num_ep_buffer_bytes)

    num_rdma_bytes = DeepEPBuffer.get_low_latency_rdma_size_hint(
        num_tokens, hidden, num_ranks, num_experts
    )
    d_buf = DeepEPBuffer(
        group,
        num_rdma_bytes=num_rdma_bytes,
        low_latency_mode=True,
        num_qps_per_rank=num_experts // num_ranks,
        explicitly_destroy=True,
    )

    # Force fallback if requested to ensure we test the python-level fallback implementation
    if use_fallback:
        m_buf._use_fallback = True

    if rank == 0:
        print("Running Dispatch/Combine...", flush=True)

    # --- PHASE 1: DISPATCH ---

    # DeepEP Dispatch
    d_timer = CUDATimer()
    with d_timer:
        d_recv, d_count, d_handle, d_event, _ = d_buf.low_latency_dispatch(
            x,
            topk_idx,
            num_tokens,
            num_experts,
            use_fp8=use_fp8,
            async_finish=async_finish,
            return_recv_hook=False,
        )
        if async_finish:
            d_event.current_stream_wait()
    d_disp_time = d_timer.get_time_ms()

    # Mooncake Dispatch
    m_timer = CUDATimer()
    with m_timer:
        m_recv, m_count, m_handle, m_event, _ = m_buf.dispatch(
            x,
            topk_idx,
            active_ranks,
            num_tokens,
            num_experts,
            timeout_us=-1,
            use_fp8=use_fp8,
            async_finish=async_finish,
            return_recv_hook=False,
        )
        if async_finish:
            m_event.current_stream_wait()
    m_disp_time = m_timer.get_time_ms()

    # Verify Dispatch Correctness
    torch.cuda.synchronize()

    # Don't check m_recv, d_recv here as they can contain
    # uninitialized values. Besides, Check after combine should be enough.

    # Check m_count, d_count
    failed_check_count += check_close(
        m_count, d_count, rank, "Dispatch token counts differ"
    )

    # --- PHASE 2: MOCK EXPERT COMPUTATION ---

    # We mock the expert layer by applying a simple transformation.
    # This proves the data can be manipulated and correctly recombined.
    if use_fp8:
        # Cast back to BF16, simulating an expert that computes in BF16/FP16
        m_expert_out = dequantize_fp8(m_recv[0], m_recv[1]) * 1.5
        d_expert_out = dequantize_fp8(d_recv[0], d_recv[1]) * 1.5
    else:
        m_expert_out = m_recv * 1.5
        d_expert_out = d_recv * 1.5

    # Zero-Copy
    if zero_copy:
        m_cb_buf = m_buf.get_next_combine_buffer(m_handle)
        m_cb_buf.copy_(m_expert_out)
        m_expert_to_pass = m_cb_buf.contiguous()

        d_cb_buf = d_buf.get_next_low_latency_combine_buffer(d_handle)
        d_cb_buf.copy_(d_expert_out)
        d_expert_to_pass = d_cb_buf.contiguous()
    else:
        m_expert_to_pass = m_expert_out.contiguous()
        d_expert_to_pass = d_expert_out.contiguous()

    # --- PHASE 3: COMBINE ---

    # Use zeros to avoid comparing uninitialized memory in check_close.
    m_out = torch.zeros((num_tokens, hidden), dtype=torch.bfloat16, device="cuda")
    d_out = torch.zeros((num_tokens, hidden), dtype=torch.bfloat16, device="cuda")

    # DeepEP Combine
    d_timer = CUDATimer()
    with d_timer:
        d_combined, d_event, _ = d_buf.low_latency_combine(
            d_expert_to_pass,
            topk_idx,
            topk_weights,
            handle=d_handle,
            zero_copy=zero_copy,
            async_finish=async_finish,
            return_recv_hook=False,
            out=d_out,
        )
        if async_finish:
            d_event.current_stream_wait()
    d_comb_time = d_timer.get_time_ms()

    # Mooncake Combine
    m_timer = CUDATimer()
    with m_timer:
        m_combined, m_event, _ = m_buf.combine(
            m_expert_to_pass,
            topk_idx,
            topk_weights,
            active_ranks,
            timeout_us=-1,
            handle=m_handle,
            zero_copy=zero_copy,
            async_finish=async_finish,
            return_recv_hook=False,
            out=m_out,
        )
        if async_finish:
            m_event.current_stream_wait()
    m_comb_time = m_timer.get_time_ms()

    # Verify Combine Correctness
    torch.cuda.synchronize()
    failed_check_count += check_close(
        m_combined,
        d_combined,
        rank,
        "Combined outputs differ",
        rtol=0.15 if use_fp8 else 5e-2,
        atol=5e-3 if use_fp8 else 1e-3,
    )

    # Destroy buffer
    d_buf.destroy()
    torch.cuda.synchronize()

    return m_disp_time, d_disp_time, m_comb_time, d_comb_time, failed_check_count


def worker(rank: int, num_ranks: int):
    # Fixed parameters
    NUM_TOKENS = 256
    HIDDEN_DIM = 2048
    NUM_EXPERTS = 288
    TOP_K = 8

    # Prepare test grid
    hyper_grid = {
        "use_fp8": [False, True],
        "zero_copy": [False, True],
        "async_finish": [False, True],
        "use_fallback": [False, True],
    }
    keys = list(hyper_grid.keys())
    combinations = list(itertools.product(*[hyper_grid[k] for k in keys]))
    total_configs = len(combinations)

    # Device filter
    device_filter = ["mlx5_1", "mlx5_2", "mlx5_3", "mlx5_4"]
    if device_filter:
        import mooncake.pg as pg

        pg.set_device_filter(device_filter)

    # Print message
    if rank == 0:
        print("\n" + "=" * 110)
        print(f"Differential Test between MooncakeEP and DeepEP Started...")
        print(
            f"   Tokens: {NUM_TOKENS} | Hidden: {HIDDEN_DIM} | Experts: {NUM_EXPERTS} | Top-K: {TOP_K}"
        )
        print(f"   Total Configurations to Test: {total_configs}")
        print(f"   Using device_filter: {device_filter if device_filter else 'None'}")
        print("=" * 110 + "\n")

    # Init distributed environment
    ip = os.getenv("MASTER_ADDR", "127.0.0.1")
    port = int(os.getenv("MASTER_PORT", "26314"))
    torch.set_default_device("cuda")
    torch.cuda.set_device(rank)

    dist.init_process_group(
        backend="mooncake",
        init_method=f"tcp://{ip}:{port}",
        world_size=num_ranks,
        rank=rank,
    )

    group = dist.new_group(list(range(num_ranks)))
    cpu_group = dist.new_group(list(range(num_ranks)), backend="mooncake-cpu")

    # Prepare results
    results_table = []
    error_logs = []

    # Barrier before starting
    cpu_group.barrier()

    # Enumerate all combinations
    for i, config in enumerate(combinations):
        cfg_dict = dict(zip(keys, config))

        if rank == 0:
            print(
                f"[Progress] Starting {i+1}/{total_configs} | Config: {cfg_dict}",
                flush=True,
            )

        m_disp, d_disp, m_comb, d_comb, failed_cnt = run_test_iteration(
            group=group,
            rank=rank,
            num_ranks=num_ranks,
            num_tokens=NUM_TOKENS,
            hidden=HIDDEN_DIM,
            num_experts=NUM_EXPERTS,
            top_k=TOP_K,
            **cfg_dict,
        )

        status_tensor = torch.tensor(failed_cnt, dtype=torch.int32, device="cuda")
        dist.all_reduce(status_tensor, op=dist.ReduceOp.SUM, group=group)

        if status_tensor.item() == 0:
            success = "✅ PASS"
        else:
            success = f"❌ MISMATCH"
            if rank == 0:
                # Store the error instead of printing it immediately
                error_logs.append(f"[Config {i+1}] Validation failed for {cfg_dict}.")

        if rank == 0:
            # Format the table row and store it in our results list
            flags = " | ".join(f"{str(v):<6}" for v in config)
            row = f"| {flags} | {m_disp:>10.2f} | {d_disp:>10.2f} | {m_comb:>10.2f} | {d_comb:>10.2f} | {success:<6} |"
            results_table.append(row)

            print(
                f"[Progress] Finished {i+1}/{total_configs} -> {success} | Config: {cfg_dict}",
                flush=True,
            )

    # Final dump of the beautiful table and any errors
    if rank == 0:
        print("\n\n" + "=" * 110)
        print(" Summary: MooncakeEP vs DeepEP")
        print("=" * 110)

        header = f"| {' | '.join(f'{k[:6]:<6}' for k in keys)} |"
        header += " M-Disp(ms) | D-Disp(ms) | M-Comb(ms) | D-Comb(ms) | Match? |"
        print(header)

        separator = "-" * len(header)
        print(separator)

        # Sequentially print all collected rows
        for row in results_table:
            print(row)

        print(separator)

        # Print any collected errors sequentially at the bottom
        if error_logs:
            print("\n⚠️  Errors Encountered During Execution:")
            for err in error_logs:
                print(err)
            print("=" * 110)
            print("❌ Some tests failed. Please review the logs above.\n")
        else:
            print("=" * 110)
            print("🎉 All tests passed.\n")


if __name__ == "__main__":
    faulthandler.enable()
    num_processes = int(os.environ.get("NUM_PROCESSES", torch.cuda.device_count()))
    torch.multiprocessing.spawn(worker, args=(num_processes,), nprocs=num_processes)
