# Mooncake EP & Mooncake PG

## Overview

Mooncake provides two closely related components for fault-tolerant MoE
inference:

- **Mooncake PG** is a `torch.distributed` ProcessGroup backend. It registers
  the `mooncake` accelerator backend and the `mooncake-cpu` backend, implements
  collective and point-to-point APIs, and exposes dynamic-membership helpers.
- **Mooncake EP** is an expert-parallel dispatch/combine runtime for
  latency-sensitive MoE inference. It follows the DeepEP low-latency programming
  model while adding rank activeness awareness and Mooncake transport support.

The usual integration pattern is to initialize a Mooncake process group first,
then construct a Mooncake EP `Buffer` from that group. The process group is used
both for regular collectives and for exchanging EP bootstrap metadata.

For implementation details, see the
[Mooncake PG design guide](../../design/mooncake-backend-pg.md) and the
[Mooncake EP design guide](../../design/mooncake-ep.md).

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

## Mooncake PG quick start

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

dist.init_process_group(
    backend="mooncake",
    rank=rank,
    world_size=world_size,
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

Use `backend="mooncake-cpu"`:

```python
dist.init_process_group(
    backend="mooncake-cpu",
    rank=rank,
    world_size=world_size,
)
```

`pg_options` is optional for a fixed-size group using the default failure
handling. Pass `MooncakeBackendOptions` when reserving additional group
capacity, joining as an extension, or selecting a non-default failure mode.

### Selecting network devices

To explicitly restrict Mooncake to a list of NIC / HCA devices, call
`pg.set_device_filter(...)` before `init_process_group()`:

```python
from mooncake import pg

pg.set_device_filter(["mlx5_1", "mlx5_2"])
```

For test and benchmark commands, the same setting is commonly passed through
`MOONCAKE_PGTEST_DEVICE_FILTERS=mlx5_1,mlx5_2`.

## Mooncake PG Torch API reference

### `MooncakeBackendOptions`

```python
pg.MooncakeBackendOptions(max_group_size)
pg.MooncakeBackendOptions(max_group_size, is_extension)
pg.MooncakeBackendOptions(
    max_group_size,
    is_extension,
    auto_deactivate_on_failure,
    auto_sync_on_failure,
)

# Explicit active-rank mirror overloads
pg.MooncakeBackendOptions(active_ranks)
pg.MooncakeBackendOptions(active_ranks, is_extension)
pg.MooncakeBackendOptions(active_ranks, is_extension, max_group_size)
```

Arguments:

- `max_group_size`: fixed in-group slot capacity. It must be at least the
  initially declared group size and cannot be increased later.
- `active_ranks`: optional contiguous `torch.int32` storage used as a mirror of
  committed PG membership. Its initial contents are ignored. Size it to
  `max_group_size`; it may be on CPU or GPU.
- `is_extension`: set to `True` for a replacement or joining process that will
  enter an existing group through `join_group()`.
- `auto_deactivate_on_failure` and `auto_sync_on_failure`: select automatic or
  framework-managed failure handling. Both default to `True`; auto-sync
  requires auto-deactivation.

### Utility functions

| Function | Purpose | Notes |
| --- | --- | --- |
| `pg.set_host_ip(host_ip)` | Override the host IP used by the backend. | Call before `init_process_group()`. |
| `pg.set_device_filter(filters)` | Restrict NIC/HCA selection. | Call before `init_process_group()`. |
| `pg.set_transfer_engine(engine)` | Reuse an external `TransferEngine`. | The engine must outlive all process groups. |
| `pg.get_active_ranks(backend)` | Return the backend active-rank tensor. | Used by EP fallback and recovery paths. |
| `pg.get_num_synced_ranks(backend)` | Return the number of locally activatable group slots. | Diagnostic helper. |
| `pg.get_peer_state(backend, ranks)` | Read locally mirrored activation readiness. | A lightweight, communication-free query. |
| `pg.activate_ranks(backend, ranks)` | Propose activation through the Coordinator. | A single call from any online rank is sufficient. |
| `pg.recover_ranks(backend, ranks)` | Propose activation through the Coordinator. | Compatibility alias to `activate_ranks`. |
| `pg.deactivate_ranks(backend, ranks)` | Propose deactivation through the Coordinator. | A single call from any online rank is sufficient. |
| `pg.join_group(backend)` | Confirm readiness for activation and remain blocked until activation actually occurs. | Used for scale-up, replacement, and in-place rejoin. |
| `pg.sync_after_failure(backend)` | Report current link observations, wait for reconciliation, and apply the latest group view. | Called automatically when `auto_sync_on_failure=True`; it may also be called manually. |

### Supported distributed operations

Mooncake PG implements the following `torch.distributed` APIs. Support may
depend on device type, dtype, PyTorch version, and whether the current backend is
`mooncake` or `mooncake-cpu`; run the PG tests on the target environment before
production use.

| API family | Examples | Notes |
| --- | --- | --- |
| Collectives | `all_reduce`, `broadcast`, `all_gather`, `all_gather_into_tensor`, `reduce_scatter_tensor`, `all_to_all`, `barrier`, `reduce`, `gather`, `scatter` | Active ranks participate; inactive ranks are skipped by backend internals. |
| Async work | `dist.all_reduce(..., async_op=True)` | Wait on the returned work object, then synchronize the device stream as needed. |
| P2P | `isend`, `irecv`, `batch_isend_irecv` | Single-tensor P2P is routed through the Mooncake backend shim. |

## Elastic recovery protocol

Mooncake PG separates join preparation from membership activation. A joining or
recovering rank completes local warmup, calls `join_group()`, and waits. An
existing rank may poll local readiness and then issue the activation proposal.
The Coordinator validates and distributes the resulting membership.

### Healthy-rank side

```python
from mooncake import pg

dist.init_process_group(
    backend="mooncake",
    rank=rank,
    world_size=2,
    pg_options=pg.MooncakeBackendOptions(
        3,                 # max_group_size
        False,             # is_extension
    ),
)

backend = dist.group.WORLD
join_ranks = [2]

while not all(pg.get_peer_state(backend, join_ranks)):
    # Continue serving, back off, or poll according to your scheduler policy.
    pass

pg.recover_ranks(backend, join_ranks)
```

### Joining-rank side

```python
from mooncake import pg

dist.init_process_group(
    backend="mooncake",
    rank=2,
    world_size=3,
    pg_options=pg.MooncakeBackendOptions(
        3,                 # max_group_size
        True,              # is_extension
    ),
)

backend = dist.group.WORLD

# Collectives are local-only before join_group. Use this
# window for framework-specific preparation, for example:
# capture_cuda_graphs()
# warm_up_model()

pg.join_group(backend)
```

Important semantics:

- `get_peer_state()` is a local best-effort readiness query, not a collective.
- Capacity must be reserved with `max_group_size` when founding members create
  the group. A joining registration appends inactive slots within that capacity.
- A joining rank starts with local-only collective behavior until `join_group`.
  The join call then blocks until a Coordinator-approved activation commits.
- A single `activate_ranks()` call, or its `recover_ranks()` alias, from any
  online rank is sufficient; redundant equivalent calls are safe.
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

## Default NCCL backend for `ElasticBuffer`

`mooncake.mooncake_elastic_buffer.ElasticBuffer` now defaults to
`transport="auto"`. Auto mode uses NCCL when the extension was built with the
NCCL Device API and the inferred EP topology is supported by the compiled NCCL
kernels. Existing constructor calls require no changes. If NCCL cannot be
used, auto mode falls back to IPC + IBGDA and retains the previous backend
behavior. As before, the requested workload must have a compiled elastic kernel
shape.

NCCL support is opt-in. Build with
`-DWITH_EP=ON -DUSE_CUDA=ON -DUSE_NCCL_DEVICE=ON`; the option defaults to
`OFF`. NCCL-enabled EP extensions currently link directly to `libnccl`, so
importing `mooncake.ep` requires a matching NCCL runtime even when the NCCL
transport is not selected. Keep the option disabled for deployments that must
remain compatible with older NCCL runtimes.

No application-side communicator bootstrap is required. Auto mode creates one
NCCL unique ID on process-group rank zero and broadcasts it to the group:

```python
import torch.distributed as dist

from mooncake.mooncake_elastic_buffer import ElasticBuffer

# Run this program with torchrun so rank metadata is available.
dist.init_process_group(backend="nccl")
buffer = ElasticBuffer(
    dist.group.WORLD,
    num_max_tokens_per_rank=128,
    hidden=4096,
    num_topk=8,
)
print(f"Mooncake EP selected {buffer.transport}")

try:
    # Call buffer.dispatch(...) and buffer.combine(...).
    pass
finally:
    # Deterministic collective cleanup is recommended when NCCL was selected.
    buffer.destroy()

dist.destroy_process_group()
```

For a controlled rollout, pass `transport="ibgda"` or set
`MOONCAKE_EP_TRANSPORT=ibgda`. Explicit `transport="nccl"` disables automatic
fallback and reports an error if NCCL support is unavailable. The
`explicitly_destroy` argument remains optional for compatibility with the
DeepEP API; calling `destroy()` collectively is still the most predictable way
to release NCCL symmetric windows before destroying the process group.

The NCCL backend currently has the following constraints:

- It requires NCCL 2.30.4 or newer with Device API and GIN support. The NCCL
  headers used to build Mooncake must exactly match the loaded `libnccl`.
  Rebuild Mooncake after an NCCL upgrade. If PyTorch would load another NCCL
  first, configure or preload the matching runtime before initializing the
  process group.
- Process-group ranks must form contiguous, equal-sized NCCL LSA teams. The
  compiled kernels support one team of two or eight GPUs (`1x2` or `1x8`), two
  teams of four or eight GPUs (`2x4` or `2x8`), and four teams of four GPUs
  (`4x4`). Cross-team communication uses hybrid mode and rail GIN. Auto mode
  selects IPC + IBGDA for other shapes.
- Groups with more than one rank request GIN resources, including runs whose
  data path remains inside one LSA team.
- Communicator membership is fixed. Create a new `ElasticBuffer` instead of
  calling `update_ep_member()` after membership changes.
- A rank-local failure before the internal status collective is established
  (for example, mismatched configuration/runtime or failure to allocate its
  minimal CUDA control resources) is not recoverable in place and may require
  restarting the process group. Use identical NCCL/CUDA configuration on every
  rank.

## Active-rank tensors: PG vs EP

There are two active-rank tensors in the API surface:

- **PG active-rank mask**: passed to `pg.MooncakeBackendOptions`. This mirrors
  the Coordinator's committed membership.
- **EP active-rank tensor**: passed to `Buffer.dispatch()` and `Buffer.combine()`.
  It is also rank-level (`[num_ranks]`, `torch.int32`) and may be updated by EP
  kernels when timeout detection marks a peer as failed.

Their values may coincide in a simple integration, but their semantics are not
interchangeable: PG membership is configuration, while EP may update its mask
from kernel-level timeout observations. Keep the mapping, dtype, device, and
capacity consistent when propagating committed PG membership into EP.

## Tests and examples

- PG collectives: `mooncake-pg/tests/test_pg_collectives.py`
- PG elastic recovery and subgroup extension: `mooncake-pg/tests/test_pg_elastic.py`
- PG benchmark harness: `mooncake-pg/benchmark/README.md`
- EP correctness and failure simulation: `python/tests/ep/test_ep_grid.py`
- EP wrapper example: `python/tests/ep/test_mooncake_ep.py`

See [PG/EP troubleshooting](../../troubleshooting/pg-ep-troubleshooting.md) for
common setup and runtime issues.
