import os

import torch
# importing dummy_collectives makes torch.distributed recognize `dummy`
# as a valid backend.
import dummy_collectives

import torch.distributed as dist

os.environ['MASTER_ADDR'] = 'localhost'
os.environ['MASTER_PORT'] = '29500'

# Alternatively:
# dist.init_process_group("dummy", rank=0, world_size=1)
dist.init_process_group("cpu:gloo,cuda:dummy", rank=0, world_size=1)

# this goes through gloo
x = torch.ones(6)
dist.all_reduce(x)
print(f"cpu allreduce: {x}")

# this goes through dummy
if torch.cuda.is_available():
    y = x.cuda()
    dist.all_reduce(y)
    print(f"cuda allreduce: {y}")

    try:
        dist.broadcast(y, 0)
    except RuntimeError:
        print("got RuntimeError when calling broadcast")