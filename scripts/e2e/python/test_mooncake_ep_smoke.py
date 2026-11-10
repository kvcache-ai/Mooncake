"""Two-GPU native Mooncake EP correctness smoke, launched with torchrun."""

import os
from datetime import timedelta

import torch
import torch.distributed as dist

from mooncake.mooncake_ep_buffer import Buffer


def main():
    local_rank = int(os.environ["LOCAL_RANK"])
    torch.cuda.set_device(local_rank)
    dist.init_process_group("nccl", timeout=timedelta(seconds=120))
    try:
        rank = dist.get_rank()
        assert dist.get_world_size() == 2, "Launch with --nproc-per-node=2"
        device = torch.device("cuda", local_rank)
        print(
            f"rank={rank} device={torch.cuda.get_device_name(local_rank)} "
            f"capability={torch.cuda.get_device_capability(local_rank)}",
            flush=True,
        )

        num_tokens, hidden, num_experts = 16, 2048, 4
        buffer = Buffer(
            dist.group.WORLD,
            Buffer.get_ep_buffer_size_hint(num_tokens, hidden, 2, num_experts),
        )
        try:
            assert not buffer._use_fallback, "Native EP fast path is required"
            print(f"rank={rank} native EP connected; fallback=False", flush=True)
            active_ranks = torch.ones(2, dtype=torch.int32, device=device)
            tokens = torch.arange(num_tokens, device=device)
            # Every token visits one expert on each GPU; all four experts receive
            # exactly 16 routes across the two source ranks.
            topk_idx = torch.stack(
                ((tokens + rank) % 2, 2 + (tokens // 2 + rank) % 2), dim=1
            ).contiguous()
            topk_weights = torch.full(
                (num_tokens, 2), 0.5, dtype=torch.float32, device=device
            )
            factors = torch.tensor([1, 2, 4, 8], dtype=torch.float32, device=device)
            local_factors = factors[rank * 2 : rank * 2 + 2].view(2, 1, 1)

            for iteration in range(3):
                # Multiples of 1/8 and power-of-two expert factors make the
                # weighted reference exactly representable in BF16.
                values = torch.arange(num_tokens * hidden, device=device)
                x = ((values + rank * 5 + iteration * 3) % 17 - 8).reshape(
                    num_tokens, hidden
                ).to(torch.bfloat16) / 8
                received, counts, handle, _, _ = buffer.dispatch(
                    x,
                    topk_idx,
                    active_ranks,
                    num_max_dispatch_tokens_per_rank=num_tokens,
                    num_experts=num_experts,
                    timeout_us=5_000_000,
                    use_fp8=False,
                )
                torch.cuda.synchronize()
                torch.testing.assert_close(counts, torch.full_like(counts, num_tokens))
                expert_out = (received.float() * local_factors).to(torch.bfloat16)
                combined, _, _ = buffer.combine(
                    expert_out.contiguous(),
                    topk_idx,
                    topk_weights,
                    active_ranks,
                    timeout_us=5_000_000,
                    handle=handle,
                )
                torch.cuda.synchronize()
                expected = (
                    x.float()
                    * (factors[topk_idx] * topk_weights).sum(dim=1, keepdim=True)
                ).to(torch.bfloat16)
                torch.testing.assert_close(combined, expected, rtol=0, atol=0)
                torch.testing.assert_close(active_ranks, torch.ones_like(active_ranks))
                print(
                    f"rank={rank} iteration={iteration} dispatch/combine PASS",
                    flush=True,
                )

            dist.barrier()
            if rank == 0:
                print("MOONCAKE_EP_SMOKE_PASS: BF16, 2 ranks, 3 iterations", flush=True)
        finally:
            del buffer
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
