"""Bounded two-GPU profile reusing the source-tree EP grid correctness checks.

Launch with torchrun and put python/tests/ep on PYTHONPATH. The installed
Mooncake wheel supplies the implementation under test.
"""

import itertools
import os
from datetime import timedelta

import torch
import torch.distributed as dist

from mooncake.mooncake_ep_buffer import Buffer
from test_ep_grid import run_test_iteration


def main():
    local_rank = int(os.environ["LOCAL_RANK"])
    torch.cuda.set_device(local_rank)
    torch.set_default_dtype(torch.bfloat16)
    torch.set_default_device("cuda")
    dist.init_process_group("nccl", timeout=timedelta(seconds=120))
    try:
        rank = dist.get_rank()
        assert dist.get_world_size() == 2, "Launch with --nproc-per-node=2"
        print(
            f"rank={rank} device={torch.cuda.get_device_name(local_rank)} "
            f"capability={torch.cuda.get_device_capability(local_rank)}",
            flush=True,
        )
        # Keep the A10 CI profile bounded. Full-grid failure injection and
        # no-P2P cases remain in the original test suite for suitable hardware.
        shapes = [(16, 2048, 4, 2), (128, 2048, 32, 4)]
        completion_modes = [(False, False), (True, False), (False, True)]
        for max_tokens, hidden, num_experts, top_k in shapes:
            buffer = Buffer(
                dist.group.WORLD,
                Buffer.get_ep_buffer_size_hint(max_tokens, hidden, 2, num_experts),
            )
            try:
                assert not buffer._use_fallback, "Native EP fast path is required"
                for use_fp8, (async_finish, hook) in itertools.product(
                    (False, True), completion_modes
                ):
                    for _ in range(3):
                        run_test_iteration(
                            group=dist.group.WORLD,
                            rank=rank,
                            num_ranks=2,
                            max_tokens=max_tokens,
                            hidden=hidden,
                            num_experts=num_experts,
                            top_k=top_k,
                            use_fp8=use_fp8,
                            async_finish=async_finish,
                            return_recv_hook=hook,
                            use_fallback=False,
                            fail_rank=-1,
                            buf=buffer,
                        )
                    print(
                        f"rank={rank} grid PASS tokens={max_tokens} "
                        f"hidden={hidden} experts={num_experts} topk={top_k} "
                        f"fp8={use_fp8} async={async_finish} hook={hook} "
                        "repeats=3 fallback=False",
                        flush=True,
                    )
            finally:
                del buffer
        dist.barrier()
        if rank == 0:
            print("MOONCAKE_EP_GRID_PASS: 12 configs, 3 repeats", flush=True)
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
