import os
import itertools
import unittest

import torch
import torch.distributed as dist
import torch.multiprocessing as mp
import torch.testing as testing
import faulthandler
import traceback

from mooncake.mooncake_ep_buffer import Buffer
import mooncake.pg as pg


def dequantize_fp8(x_fp8: torch.Tensor, scales: torch.Tensor) -> torch.Tensor:
    hidden = x_fp8.shape[-1]
    x_view = x_fp8.reshape(-1, hidden // 128, 128).float()
    scales_view = scales.reshape(-1, hidden // 128, 1).float()
    dequantized = (x_view * scales_view).reshape(x_fp8.shape)
    return dequantized.to(torch.bfloat16)


def run_test_iteration(
    group: dist.ProcessGroup,
    cpu_group: dist.ProcessGroup,
    rank: int,
    num_ranks: int,
    max_tokens: int,
    hidden: int,
    num_experts: int,
    top_k: int,
    use_fp8: bool,
    zero_copy: bool,
    async_finish: bool,
    return_recv_hook: bool,
    use_fallback: bool,
    fail_rank: int,
):
    assert not (
        async_finish and return_recv_hook
    ), "Should be filtered out by generate_tests."

    torch.manual_seed(2026 + rank)
    scale = 1.0 - 0.05 * (rank / num_ranks)
    num_tokens = int(max_tokens * scale)

    # Prepare test data
    x = torch.randn(num_tokens, hidden, dtype=torch.bfloat16)
    scores = torch.randn((num_tokens, num_experts), dtype=torch.float32)
    topk_idx = torch.topk(scores, top_k, dim=-1)[1]
    topk_weights = torch.softmax(
        torch.rand(num_tokens, top_k, dtype=torch.float32), dim=-1
    )
    active_ranks = torch.ones((num_ranks,), dtype=torch.int32)

    # Prepare expected result
    def get_mock_factor(expert_id):
        return expert_id * 0.1 + 1.0

    # Since `get_mock_factor` is simply *0.1 + 1, we can make it more efficient
    # instead of the nested loops below:
    #
    #   for i in range(num_tokens):
    #       for j in range(top_k):
    #           expert_id = topk_idx[i, j].item()
    #           w = topk_weights[i, j].item()
    #           mock_expert_factor = get_mock_factor(expert_id)
    #           if fail_rank != -1 and (expert_id // num_local_experts == fail_rank):
    #               continue
    #           expected_out[i] += (x[i] * mock_expert_factor) * w
    num_local_experts = num_experts // num_ranks
    factors = get_mock_factor(topk_idx)
    if fail_rank != -1:
        expert_owners = topk_idx // num_local_experts
        valid_mask = expert_owners != fail_rank
        factors = factors * valid_mask.to(factors.dtype)
    sum_weights = (factors * topk_weights).sum(dim=1, keepdim=True)
    expected_out = x * sum_weights
    expected_out = expected_out.to(torch.bfloat16)

    # Initialize the buffer.
    num_ep_buffer_bytes = Buffer.get_ep_buffer_size_hint(
        max_tokens, hidden, num_ranks, num_experts
    )
    buf = Buffer(group, num_ep_buffer_bytes)

    if use_fallback:
        buf._use_fallback = True

    # 5s timeout if we simulate a failed rank
    timeout_us = 5 * 1_000_000 if fail_rank != -1 else -1

    cpu_group.barrier()

    if rank == fail_rank:
        os._exit(0)

    # Dispatch
    recv_x, recv_count, handle, event, hook = buf.dispatch(
        x,
        topk_idx,
        active_ranks,
        num_max_dispatch_tokens_per_rank=max_tokens,
        num_experts=num_experts,
        timeout_us=timeout_us,
        use_fp8=use_fp8,
        async_finish=async_finish,
        return_recv_hook=return_recv_hook,
    )

    if return_recv_hook:
        hook()
    if async_finish:
        event.current_stream_wait()

    torch.cuda.synchronize()
    # Fault-tolerance check
    if fail_rank != -1:
        assert active_ranks[fail_rank].item() == 0, (
            f"[Rank {rank}] Failed rank {fail_rank} is not recorded in active_ranks. "
            f"active_ranks: {active_ranks}"
        )
        assert active_ranks.sum().item() == active_ranks.numel() - 1, (
            f"[Rank {rank}] Expected exactly one failed rank {fail_rank}, but found "
            f"{active_ranks.numel() - active_ranks.sum().item()} inactive ranks. "
            f"Maybe the timeout is too small? "
            f"active_ranks: {active_ranks}"
        )

    # Mock expert forward
    if use_fp8:
        recv_bf16 = dequantize_fp8(recv_x[0], recv_x[1])
    else:
        recv_bf16 = recv_x

    expert_out = torch.empty_like(recv_bf16)

    for le in range(num_local_experts):
        expert_id = rank * num_local_experts + le
        mock_expert_factor = get_mock_factor(expert_id)
        expert_out[le] = recv_bf16[le] * mock_expert_factor

    expert_out = expert_out.to(torch.bfloat16)

    # Combine
    if zero_copy:
        cb_buf = buf.get_next_combine_buffer(handle)
        cb_buf.copy_(expert_out)
        expert_to_pass = cb_buf.contiguous()
    else:
        expert_to_pass = expert_out.contiguous()

    out_tensor = torch.zeros_like(x)
    combined_x, event, hook = buf.combine(
        expert_to_pass,
        topk_idx,
        topk_weights,
        active_ranks,
        timeout_us=timeout_us,
        handle=handle,
        zero_copy=zero_copy,
        async_finish=async_finish,
        return_recv_hook=return_recv_hook,
        out=out_tensor,
    )

    if return_recv_hook:
        hook()
    if async_finish:
        event.current_stream_wait()

    torch.cuda.synchronize()

    testing.assert_close(
        combined_x,
        expected_out,
        rtol=0.15 if use_fp8 else 5e-2,
        atol=5e-3 if use_fp8 else 1e-3,
        msg=lambda msg: f"[Rank {rank}] Combine Mismatch. {msg}",
    )

    torch.cuda.synchronize()
    dist.barrier(cpu_group)


def worker(rank, world_size, config_dict):
    # Device filter
    device_filter = [
        f
        for f in os.getenv("DEVICE_FILTER", "mlx5_1,mlx5_2,mlx5_3,mlx5_4").split(",")
        if f
    ]
    if device_filter:
        pg.set_device_filter(device_filter)

    torch.cuda.set_device(rank)
    torch.set_default_dtype(torch.bfloat16)
    torch.set_default_device("cuda")

    dist.init_process_group(backend="mooncake", rank=rank, world_size=world_size)
    group = dist.group.WORLD
    cpu_group = dist.new_group(list(range(world_size)), backend="mooncake-cpu")

    try:
        run_test_iteration(
            group=group,
            cpu_group=cpu_group,
            rank=rank,
            num_ranks=world_size,
            **config_dict,
        )
    except Exception as e:
        traceback.print_exc()
        raise

    dist.destroy_process_group()


class TestMooncakeEPBuffer(unittest.TestCase):
    def setUp(self):
        self.world_size = torch.cuda.device_count()
        os.environ["MASTER_ADDR"] = "127.0.0.1"
        os.environ["MASTER_PORT"] = "29500"

    def run_single_config(self, config_dict):
        mp.spawn(
            worker,
            args=(self.world_size, config_dict),
            nprocs=self.world_size,
            join=True,
            daemon=False,
        )


def make_test_name(cfg):
    parts = ["test_ep"]

    flags = []
    if cfg["use_fp8"]:
        flags.append("fp8")
    if cfg["zero_copy"]:
        flags.append("0copy")
    if cfg["async_finish"]:
        flags.append("async")
    if cfg["return_recv_hook"]:
        flags.append("hook")
    if cfg["use_fallback"]:
        flags.append("fallback")

    if flags:
        parts.append("_".join(flags))
    else:
        parts.append("base")

    fail_rank = cfg["fail_rank"]
    if fail_rank == -1:
        parts.append("nofail")
    else:
        parts.append(f"fail{fail_rank}")

    parts.append(f"t{cfg['max_tokens']}")
    parts.append(f"h{cfg['hidden']}")
    parts.append(f"e{cfg['num_experts']}")
    parts.append(f"k{cfg['top_k']}")

    return "_".join(parts)


def generate_tests():
    test_grid = {
        "use_fp8": [False, True],
        "zero_copy": [False, True],
        "async_finish": [False, True],
        "return_recv_hook": [False, True],
        "use_fallback": [False, True],
        "fail_rank": [-1, 1],
        "shapes": [
            {"max_tokens": 256, "hidden": 2048, "num_experts": 288, "top_k": 8},
        ],
    }

    keys = list(test_grid.keys())
    config_tuples = list(itertools.product(*[test_grid[k] for k in keys]))

    for t in config_tuples:
        raw_dict = dict(zip(keys, t))

        if raw_dict["async_finish"] and raw_dict["return_recv_hook"]:
            continue

        # Flatten
        config_dict = {}
        for k, v in raw_dict.items():
            if k == "shapes":
                config_dict.update(v)
            else:
                config_dict[k] = v

        # Make a test name
        test_name = make_test_name(config_dict)

        def test_func(self, cfg=config_dict):
            self.run_single_config(cfg)

        test_func.__name__ = test_name
        setattr(TestMooncakeEPBuffer, test_name, test_func)


if __name__ == "__main__":
    faulthandler.enable()
    generate_tests()
    unittest.main(verbosity=2)
