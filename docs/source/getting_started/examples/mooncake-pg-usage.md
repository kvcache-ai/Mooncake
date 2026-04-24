# Mooncake Process Group — Usage Guide

This guide shows how to build `mooncake-pg`, register it with PyTorch distributed, and use it as a drop-in replacement for NCCL in collective and point-to-point workloads.

## Prerequisites

| Requirement | Version |
|-------------|---------|
| CUDA | 11.8 or later |
| PyTorch | 2.1 or later |
| RDMA hardware | InfiniBand / RoCE / NVLink (TCP fallback available) |
| Mooncake | Built with `-DWITH_PG=ON -DUSE_CUDA=ON` |

## Installation

### Step 1: Build Mooncake with PG support

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
git submodule update --init --recursive

mkdir build && cd build
cmake .. \
    -DWITH_PG=ON \
    -DUSE_CUDA=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

### Step 2: Install the Python wheel

```bash
# Copy built extension into the wheel directory
cp mooncake-pg/mooncake_pg.cpython-*.so ../mooncake-wheel/mooncake/

# Install with pip
cd ..
pip install -e mooncake-wheel --no-build-isolation
```

### Step 3: Verify the import

```python
import mooncake_pg   # no error = installed correctly
import torch.distributed as dist
```

## Initializing the Process Group

`mooncake-pg` registers two backends with PyTorch:
- `"mooncake"` for CUDA tensors.
- `"mooncake-cpu"` for CPU tensors.

Use the standard `torch.distributed.init_process_group` call:

```python
import torch
import torch.distributed as dist

# Standard torchrun / mpirun launch sets MASTER_ADDR, MASTER_PORT,
# RANK, WORLD_SIZE automatically.
dist.init_process_group(
    backend="mooncake",   # or "mooncake-cpu" for CPU-only
    init_method="env://",
)

rank = dist.get_rank()
world_size = dist.get_world_size()
print(f"[rank {rank}/{world_size}] process group initialized")
```

The process group creation triggers `MooncakeBackend` construction, which:
1. Initialises the shared `TransferEngine` singleton (first call only, subsequent calls reuse it).
2. Registers local GPU memory buffers.
3. Starts the `ConnectionPoller` background thread.

## Example: All-Reduce

```python
import torch
import torch.distributed as dist

dist.init_process_group(backend="mooncake", init_method="env://")
rank = dist.get_rank()

# Create a tensor on the local GPU
device = torch.device(f"cuda:{rank}")
tensor = torch.ones(1024, device=device) * rank

# In-place allreduce (SUM)
dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

# On rank 0 with world_size=4: tensor == [0+1+2+3] * 1024 == 6144
print(f"[rank {rank}] allreduce result[0] = {tensor[0].item()}")
dist.destroy_process_group()
```

## Example: Point-to-Point (P2P)

```python
import torch
import torch.distributed as dist

dist.init_process_group(backend="mooncake", init_method="env://")
rank = dist.get_rank()
world_size = dist.get_world_size()

device = torch.device(f"cuda:{rank}")
N = 1024 * 1024  # 4 MB

if rank == 0:
    tensor = torch.arange(N, dtype=torch.float32, device=device)
    dist.send(tensor, dst=1, tag=42)
    print("[rank 0] sent tensor")
elif rank == 1:
    tensor = torch.empty(N, dtype=torch.float32, device=device)
    dist.recv(tensor, src=0, tag=42)
    print(f"[rank 1] received tensor[0]={tensor[0].item()}")

dist.destroy_process_group()
```

## Example: All-to-All (Expert Parallelism)

This pattern is the primary use case for `mooncake-pg` in MoE (Mixture-of-Experts) models, where tokens are routed to expert GPUs.

```python
import torch
import torch.distributed as dist

dist.init_process_group(backend="mooncake", init_method="env://")
rank = dist.get_rank()
world_size = dist.get_world_size()

device = torch.device(f"cuda:{rank}")
tokens_per_rank = 128
hidden_dim = 4096

# Each rank sends a different chunk to every other rank
input_tensor = torch.randn(world_size * tokens_per_rank, hidden_dim, device=device)
output_tensor = torch.empty_like(input_tensor)

dist.all_to_all_single(output_tensor, input_tensor)
print(f"[rank {rank}] all_to_all complete, output shape={output_tensor.shape}")

dist.destroy_process_group()
```

## Example: Batch Async P2P (batch_isend_irecv)

```python
import torch
import torch.distributed as dist

dist.init_process_group(backend="mooncake", init_method="env://")
rank = dist.get_rank()
device = torch.device(f"cuda:{rank}")
N = 1024

peer = (rank + 1) % dist.get_world_size()
send_tensor = torch.ones(N, device=device) * rank
recv_tensor = torch.zeros(N, device=device)

ops = [
    dist.P2POp(dist.isend, send_tensor, peer),
    dist.P2POp(dist.irecv, recv_tensor, peer),
]
works = dist.batch_isend_irecv(ops)
for w in works:
    w.wait()

print(f"[rank {rank}] received from {peer}: {recv_tensor[0].item()}")
dist.destroy_process_group()
```

## Launch Commands

### torchrun (recommended)

```bash
# 4 GPUs on 1 node
torchrun \
    --nproc_per_node=4 \
    --master_addr=127.0.0.1 \
    --master_port=29500 \
    your_script.py

# 16 GPUs across 2 nodes (run on each node)
torchrun \
    --nproc_per_node=8 \
    --nnodes=2 \
    --node_rank=0 \          # 1 on second node
    --master_addr=10.0.0.1 \
    --master_port=29500 \
    your_script.py
```

### mpirun

```bash
mpirun -np 8 \
    -x MASTER_ADDR=10.0.0.1 \
    -x MASTER_PORT=29500 \
    python your_script.py
```

## Elastic Group Membership

`mooncake-pg` exposes Python helpers for fault-tolerant setups via `mooncake_pg`:

```python
import mooncake_pg
import torch.distributed as dist

dist.init_process_group(backend="mooncake", init_method="env://")
pg = dist.group.WORLD

# Check which ranks are alive
active = mooncake_pg.get_active_ranks(pg)           # torch.BoolTensor
n_synced = mooncake_pg.get_num_synced_ranks(pg)      # int

# Dynamically grow the group
mooncake_pg.extend_group_size_to(pg, new_size=16)

# Check specific peer liveness
states = mooncake_pg.get_peer_state(pg, [2, 5, 7])  # list[bool]

# Re-admit a recovered rank
mooncake_pg.recover_ranks(pg, [5])
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MOONCAKE_MASTER` | — | Mooncake master address (`ip:port`) |
| `MOONCAKE_PROTOCOL` | `rdma` | Transfer protocol (`rdma`, `tcp`, `nvlink`, …) |
| `MOONCAKE_DEVICE` | auto | RDMA/HCA device name (e.g., `mlx5_0`) |
| `MC_METADATA_SERVER` | `P2PHANDSHAKE` | Metadata server URL |
| `MC_FORCE_MNNVL` | `false` | Force MNNVL even when RDMA NICs are present |

## Limitations

- Only `SUM` is supported for reduce operations (`allreduce`, `reduce`).
- Sparse tensors are not supported.
- Each `send`/`recv` call must transfer a single tensor; use `batch_isend_irecv` for multiple tensors.

## See Also

- [Mooncake PG Design](../../design/mooncake-pg)
- [Transfer Engine](../../design/transfer-engine/index)
- [Supported Protocols](../supported-protocols)
