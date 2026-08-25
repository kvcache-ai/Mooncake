"""Two-rank smoke test for the runtime-built Torch PG adapter."""

import os

import torch
import torch.distributed as dist


def main() -> None:
    rank = int(os.environ["RANK"])
    world_size = int(os.environ["WORLD_SIZE"])
    assert world_size == 2, world_size

    import mooncake.pg as pg

    dist.init_process_group(
        backend="mooncake",
        rank=rank,
        world_size=world_size,
        init_method="env://",
        pg_options=pg.MooncakeBackendOptions(world_size, False),
    )
    value = torch.tensor([rank + 1], device="cuda", dtype=torch.int32)
    dist.all_reduce(value, group=dist.group.WORLD)
    assert value.item() == 3, value
    dist.destroy_process_group()
    print(f"PG_JIT_SMOKE_OK rank={rank}", flush=True)


if __name__ == "__main__":
    main()
