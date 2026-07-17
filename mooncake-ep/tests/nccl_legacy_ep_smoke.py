import os

import torch
import torch.distributed as dist

from mooncake.mooncake_ep_buffer import Buffer


def main():
    dist.init_process_group("nccl")
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    torch.cuda.set_device(0)

    tokens, hidden, topk, experts = 4, 7168, 1, world_size
    x = torch.full((tokens, hidden), float(rank + 1), dtype=torch.bfloat16,
                   device="cuda")
    topk_idx = torch.full((tokens, topk), (rank + 1) % world_size,
                          dtype=torch.int64, device="cuda")
    topk_weights = torch.ones((tokens, topk), dtype=torch.float32,
                              device="cuda")
    active_ranks = torch.ones(world_size, dtype=torch.int32, device="cuda")
    size = Buffer.get_ep_buffer_size_hint(tokens, hidden, world_size, experts)
    buffer = Buffer(dist.group.WORLD, num_ep_buffer_bytes=size)
    assert buffer.runtime.nccl_transport_enabled()

    packed, count, handle, event, hook = buffer.dispatch(
        x, topk_idx, active_ranks, tokens, experts, -1, async_finish=False)
    event.current_stream_wait()
    assert int(count[0].item()) == tokens, count

    output, event, hook = buffer.combine(
        packed.clone(), topk_idx, topk_weights, active_ranks, -1, handle,
        async_finish=False)
    event.current_stream_wait()
    torch.cuda.synchronize()
    expected = torch.full_like(output, float(rank + 1))
    torch.testing.assert_close(output, expected)
    print(f"NCCL_LEGACY_EP_SMOKE_PASS rank={rank}", flush=True)
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
