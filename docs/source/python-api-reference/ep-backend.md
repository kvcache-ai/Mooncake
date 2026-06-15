# Mooncake EP & Mooncake Backend (PG)

## Overview

Mooncake provides two closely related components for fault-tolerant MoE
inference:

- **Mooncake Backend (PG)** is a `torch.distributed` ProcessGroup backend. It
  registers the `mooncake` accelerator backend and the `mooncake-cpu` backend,
  implements common collective and point-to-point APIs, tracks active ranks, and
  exposes elastic recovery helpers.
- **Mooncake EP** is an expert-parallel dispatch/combine runtime for
  latency-sensitive MoE inference. It follows the DeepEP low-latency programming
  model while adding rank activeness awareness and Mooncake transport support.

The usual integration pattern is to initialize a Mooncake process group first,
then construct a Mooncake EP `Buffer` from that group. The process group is used
both for regular collectives and for exchanging EP bootstrap metadata.

For implementation details, see the [Mooncake Backend (PG) design guide](../design/mooncake-backend-pg.md)
and the [Mooncake EP design guide](../design/mooncake-ep.md).

## Installation and build notes

Mooncake EP and PG are included in CUDA-enabled Mooncake wheels. When building
from source, enable the EP/PG extensions with:

```bash
cmake .. -DWITH_EP=ON
```

The extensions are compiled against a specific PyTorch version. At import time,
`mooncake.pg` and `mooncake.ep` load version-suffixed extension modules that
match the active `torch.__version__`. If the current PyTorch version does not
match a built extension, import will fail with a message such as
`Mooncake PG was not built against torch==...`.

## Mooncake Backend (PG) quick start

### CUDA backend

```python
import os

import torch
import torch.distributed as dist
from mooncake import pg


rank = int(os.environ["RANK"])
world_size = int(os.environ["WORLD_SIZE"])
local_rank = int(os.environ.get("LOCAL_RANK", rank))

torch.cuda.set_device(local_rank)
device = torch.device("cuda", local_rank)

# Backend-level active-rank mask. Use int32 and place it on the backend device.
active_ranks = torch.ones(world_size, dtype=torch.int32, device=device)

dist.init_process_group(
    backend="mooncake",
    rank=rank,
    world_size=world_size,
    pg_options=pg.MooncakeBackendOptions(active_ranks),
)

x = torch.tensor([rank + 1], dtype=torch.int32, device=device)
dist.all_reduce(x, op=dist.ReduceOp.SUM)
print(f"rank={rank}, all_reduce={int(x.cpu())}")
```

Run it with the usual PyTorch launcher, for example:

```bash
torchrun --nproc-per-node=2 pg_quickstart.py
```

### CPU backend

Use `backend="mooncake-cpu"` and put `active_ranks` on CPU:

```python
active_ranks = torch.ones(world_size, dtype=torch.int32)
dist.init_process_group(
    backend="mooncake-cpu",
    rank=rank,
    world_size=world_size,
    pg_options=pg.MooncakeBackendOptions(active_ranks),
)
```

### Selecting network devices

To explicitly restrict Mooncake to a list of NIC / HCA devices, call
`pg.set_device_filter(...)` before `init_process_group()`:

```python
from mooncake import pg

pg.set_device_filter(["mlx5_1", "mlx5_2"])
```

For test and benchmark commands, the same setting is commonly passed through
`MOONCAKE_PGTEST_DEVICE_FILTERS=mlx5_1,mlx5_2`.

## Mooncake Backend (PG) API reference

### `MooncakeBackendOptions`

```python
pg.MooncakeBackendOptions(active_ranks)
pg.MooncakeBackendOptions(active_ranks, is_extension)
pg.MooncakeBackendOptions(active_ranks, is_extension, max_world_size)
```

Arguments:

- `active_ranks`: `torch.int32` tensor used as the backend-level rank-health
  mask. For `mooncake`, it must be on the accelerator device; for
  `mooncake-cpu`, it must be on CPU. When `max_world_size` is set, size this
  tensor to `max_world_size`, not the current visible world size.
- `is_extension`: set to `True` for a replacement or joining process that will
  enter an existing group through `join_group()`.
- `max_world_size`: optional upper bound for reserved rank slots. It lets
  healthy ranks reserve inactive future ranks while keeping
  `dist.get_world_size()` equal to the current active size.

### Utility functions

| Function | Purpose | Notes |
| --- | --- | --- |
| `pg.set_host_ip(host_ip)` | Override the host IP used by the backend. | Call before `init_process_group()`. |
| `pg.set_device_filter(filters)` | Restrict NIC/HCA selection. | Call before `init_process_group()`. |
| `pg.set_transfer_engine(engine)` | Reuse an external `TransferEngine`. | The engine must outlive all process groups. |
| `pg.get_preferred_hca(backend, location)` | Query topology-preferred HCA for a location. | Useful for topology-aware placement/debugging. |
| `pg.get_active_ranks(backend)` | Return the backend active-rank tensor. | Used by EP fallback and recovery paths. |
| `pg.get_num_synced_ranks(backend)` | Return the number of ranks synchronized by the backend. | Diagnostic helper. |
| `pg.extend_group_size_to(backend, size)` | Reserve additional inactive ranks. | Newly extended ranks do not participate until recovered. |
| `pg.get_peer_state(backend, ranks)` | Check whether candidate ranks have published peer metadata. | Collective among healthy ranks. |
| `pg.recover_ranks(backend, ranks)` | Activate ready ranks and publish extension state. | Requires peer metadata to be ready. |
| `pg.join_group(backend)` | Joiner-side blocking call for extension ranks. | Used after `is_extension=True` initialization. |

### Supported distributed operations

Mooncake Backend implements the following `torch.distributed` APIs. Support may
depend on device type, dtype, PyTorch version, and whether the current backend is
`mooncake` or `mooncake-cpu`; run the PG tests on the target environment before
production use.

| API family | Examples | Notes |
| --- | --- | --- |
| Collectives | `all_reduce`, `broadcast`, `all_gather`, `all_gather_into_tensor`, `reduce_scatter_tensor`, `all_to_all`, `barrier`, `reduce`, `gather`, `scatter` | Active ranks participate; inactive ranks are skipped by backend internals. |
| Async work | `dist.all_reduce(..., async_op=True)` | Wait on the returned work object, then synchronize the device stream as needed. |
| P2P | `isend`, `irecv`, `batch_isend_irecv` | Single-tensor P2P is routed through the Mooncake P2P shim. |

## Elastic recovery protocol

Mooncake PG supports a two-sided recovery protocol. Existing healthy ranks poll
for replacement rank readiness, then activate those ranks. Replacement ranks
start in extension mode, publish metadata, and wait until healthy ranks recover
them.

### Healthy-rank side

```python
from mooncake import pg

active_ranks = torch.tensor([1, 1, 0], dtype=torch.int32, device=device)
dist.init_process_group(
    backend="mooncake",
    rank=rank,
    world_size=2,
    pg_options=pg.MooncakeBackendOptions(
        active_ranks,
        False,             # is_extension
        3,                 # max_world_size
    ),
)

backend = dist.group.WORLD._get_backend(device)
join_ranks = [2]

while not all(pg.get_peer_state(backend, join_ranks)):
    # Continue serving, back off, or poll according to your scheduler policy.
    pass

pg.recover_ranks(backend, join_ranks)
```

### Joining-rank side

```python
from mooncake import pg

active_ranks = torch.tensor([1, 1, 1], dtype=torch.int32, device=device)
dist.init_process_group(
    backend="mooncake",
    rank=2,
    world_size=3,
    pg_options=pg.MooncakeBackendOptions(
        active_ranks,
        True,              # is_extension
        3,                 # max_world_size
    ),
)

backend = dist.group.WORLD._get_backend(device)
pg.join_group(backend)
```

Important semantics:

- `get_peer_state()` is collective among the current healthy ranks. Call it in a
  consistent order across those ranks.
- New ranks are inactive after `extend_group_size_to()` and become collective
  participants only after `recover_ranks()`.
- A joining rank initialized with `is_extension=True` starts with local-only
  behavior and blocks in `join_group()` until the corresponding healthy ranks
  publish recovery state.
- Subgroups must be created in the same order on healthy and joining processes,
  following PyTorch `new_group()` ordering rules.

## Mooncake EP quick start

Mooncake EP exposes `Buffer` from `mooncake.mooncake_ep_buffer`. Initialize it
with a Mooncake process group and a workspace size computed from the expected
dispatch shape.

```python
import torch
import torch.distributed as dist
from mooncake import pg
from mooncake.mooncake_ep_buffer import Buffer


# Assume dist.init_process_group(..., backend="mooncake", ...) has completed.
group = dist.group.WORLD
rank = dist.get_rank(group)
world_size = dist.get_world_size(group)

num_tokens = 128
hidden = 7168
num_experts = 288
top_k = 8
max_tokens_per_rank = 128

x = torch.randn(num_tokens, hidden, dtype=torch.bfloat16, device="cuda")
scores = torch.randn(num_tokens, num_experts, dtype=torch.float32, device="cuda")
topk_idx = torch.topk(scores, top_k, dim=-1).indices
topk_weights = torch.softmax(
    torch.randn(num_tokens, top_k, dtype=torch.float32, device="cuda"), dim=-1
)

num_ep_buffer_bytes = Buffer.get_ep_buffer_size_hint(
    max_tokens_per_rank,
    hidden,
    world_size,
    num_experts,
)
buffer = Buffer(group, num_ep_buffer_bytes)

# EP-level rank-health tensor. Kernels may update it to 0 when timeout_us
# detects a failed source rank.
active_ranks = torch.ones(world_size, dtype=torch.int32, device="cuda")

recv_x, recv_count, handle, event, hook = buffer.dispatch(
    x,
    topk_idx,
    active_ranks,
    num_max_dispatch_tokens_per_rank=max_tokens_per_rank,
    num_experts=num_experts,
    timeout_us=-1,
    use_fp8=True,
    async_finish=False,
    return_recv_hook=False,
)
event.current_stream_wait()

# Run local experts on recv_x here. If use_fp8=True, recv_x is a
# (data, scales) tuple; dequantize or feed it into an FP8-aware expert kernel.
expert_out = run_local_experts(recv_x, recv_count)

combined_x, event, hook = buffer.combine(
    expert_out,
    topk_idx,
    topk_weights,
    active_ranks,
    timeout_us=-1,
    handle=handle,
)
event.current_stream_wait()
```

## Mooncake EP API reference

### `Buffer.get_ep_buffer_size_hint(...)`

```python
Buffer.get_ep_buffer_size_hint(
    num_max_dispatch_tokens_per_rank: int,
    hidden: int,
    num_ranks: int,
    num_experts: int,
) -> int
```

Returns the workspace size in bytes for the EP buffer. Use the maximum number of
tokens a rank may dispatch in one step. Underestimating this value can cause
buffer overflow or incorrect dispatch results.

### `Buffer(group, num_ep_buffer_bytes=0)`

Creates the EP runtime for a Mooncake process group. The constructor exchanges
RDMA and IPC metadata through the group, initializes fast-path transports when
available, and falls back to the Python implementation if the fast path is not
usable.

### `Buffer.dispatch(...)`

```python
recv_x, recv_count, handle, event, hook = buffer.dispatch(
    x,
    topk_idx,
    active_ranks,
    num_max_dispatch_tokens_per_rank,
    num_experts,
    timeout_us,
    use_fp8=True,
    async_finish=False,
    return_recv_hook=False,
)
```

Arguments:

- `x`: local token hidden states, shape `[num_tokens, hidden]`, typically BF16
  on CUDA.
- `topk_idx`: selected expert IDs, shape `[num_tokens, top_k]`. Use `-1` to mark
  masked selections.
- `active_ranks`: EP-level rank-health tensor, shape `[num_ranks]`, dtype
  `torch.int32`. Timeout detection may set failed source ranks to `0`.
- `num_max_dispatch_tokens_per_rank`: workspace capacity per rank. It should be
  at least the maximum local `num_tokens` across ranks for the current step.
- `num_experts`: global expert count. It must be divisible by `num_ranks`.
- `timeout_us`: timeout in microseconds. Use `-1` to disable timeout detection.
- `use_fp8`: when `True`, dispatch returns FP8 data plus scales.
- `async_finish`: when `True`, returned tensors are associated with the returned
  event for stream-lifetime management.
- `return_recv_hook`: when `True`, call the returned `hook()` to complete receive
  synchronization; otherwise use `event.current_stream_wait()`.

Returns:

- `recv_x`: packed local-expert inputs. If `use_fp8=True`, this is
  `(packed_data, packed_scales)`; otherwise it is a BF16 tensor.
- `recv_count`: number of tokens received by each local expert.
- `handle`: opaque metadata required by `combine()` and
  `get_next_combine_buffer()`.
- `event`: `EventOverlap` helper; call `event.current_stream_wait()` before using
  outputs when no hook is used.
- `hook`: optional synchronization hook used when `return_recv_hook=True`.

### `Buffer.combine(...)`

```python
combined_x, event, hook = buffer.combine(
    x,
    topk_idx,
    topk_weights,
    active_ranks,
    timeout_us,
    handle,
    zero_copy=False,
    async_finish=False,
    return_recv_hook=False,
    out=None,
)
```

Arguments:

- `x`: local expert outputs packed in the layout returned by `dispatch()`.
- `topk_idx` and `topk_weights`: routing metadata for combining expert outputs
  back to local tokens.
- `active_ranks`: same EP-level rank-health tensor used by `dispatch()`.
- `timeout_us`: timeout in microseconds; use `-1` to disable timeout detection.
- `handle`: the handle returned by the matching `dispatch()` call.
- `zero_copy`: when `True`, write expert outputs into
  `buffer.get_next_combine_buffer(handle)` and pass that tensor to `combine()`.
- `out`: optional output tensor for the combined result.

### `Buffer.get_next_combine_buffer(handle)`

Returns the next combine buffer for zero-copy expert output. Use it only with the
matching dispatch `handle` and pass the resulting tensor back to `combine()` with
`zero_copy=True`.

### `Buffer.update_ep_member()`

Reconnects EP peers after backend membership changes. Call it after PG recovery
updates rank activeness so EP transport metadata and QPs can be refreshed.

## Active-rank tensors: PG vs EP

There are two active-rank tensors in the API surface:

- **PG active-rank mask**: passed to `pg.MooncakeBackendOptions`. This is the
  backend-level health mask used by collective and recovery logic.
- **EP active-rank tensor**: passed to `Buffer.dispatch()` and `Buffer.combine()`.
  It is also rank-level (`[num_ranks]`, `torch.int32`) and may be updated by EP
  kernels when timeout detection marks a peer as failed.

In simple integrations these tensors often carry the same health information,
but they are passed through different API layers. Keep their dtype, device, and
shape consistent with the process group world size or reserved `max_world_size`.

## Tests and examples

- PG collectives: `mooncake-pg/tests/test_pg_collectives.py`
- PG elastic recovery and subgroup extension: `mooncake-pg/tests/test_pg_elastic.py`
- PG benchmark harness: `mooncake-pg/benchmark/README.md`
- EP correctness and failure simulation: `mooncake-ep/tests/test_ep_grid.py`
- Wheel-level EP example: `mooncake-wheel/tests/test_mooncake_ep.py`

See [PG/EP troubleshooting](../troubleshooting/pg-ep-troubleshooting.md) for
common setup and runtime issues.
