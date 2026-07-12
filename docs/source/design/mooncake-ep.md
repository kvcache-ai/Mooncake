# Mooncake EP Design

Mooncake EP is the expert-parallel communication runtime used for MoE token
dispatch and combine. It follows the DeepEP low-latency programming model while
adding Mooncake transport integration and rank activeness awareness.

This document explains the runtime at a level useful for developers maintaining
Mooncake EP or integrating it into inference engines.

## Goals

Mooncake EP is designed to:

- provide low-latency dispatch/combine operations for expert-parallel MoE
  inference;
- keep the Python programming model close to DeepEP low-latency mode;
- use Mooncake device transports for fast intra-node and inter-node movement;
- detect failed source ranks through timeout-aware kernels;
- interoperate with Mooncake Backend (PG) for bootstrap metadata exchange and
  rank-health state.

## High-level data flow

MoE inference with Mooncake EP has three phases:

1. **Dispatch**: each rank sends token hidden states to the ranks that own the
   selected experts. The receiver packs tokens by local expert.
2. **Expert compute**: each rank runs its local experts over the packed inputs.
3. **Combine**: expert outputs are routed back to the original token owners and
   reduced with routing weights.

```mermaid
flowchart LR
    A[Local tokens x + topk_idx] --> B[dispatch]
    B --> C[Packed local expert inputs]
    C --> D[Local expert kernels]
    D --> E[Packed expert outputs]
    E --> F[combine]
    F --> G[Combined local token outputs]
```

## Relationship with Mooncake Backend (PG)

Mooncake EP is constructed from a `torch.distributed` process group:

```python
from mooncake.mooncake_ep_buffer import Buffer

buffer = Buffer(group, num_ep_buffer_bytes)
```

The group should normally be a Mooncake Backend process group. EP uses it to
exchange RDMA memory-region metadata, QP information, GID/LID information, and
IPC handles. The backend active-rank state is also used by the fallback path and
by peer synchronization.

After PG recovery changes membership, call `Buffer.update_ep_member()` to refresh
EP peer metadata and QPs before relying on the recovered ranks for dispatch and
combine.

## Runtime objects

### Python wrapper

`mooncake.mooncake_ep_buffer.Buffer` is the user-facing wrapper. It owns:

- the process group;
- the native EP buffer runtime;
- the fallback flag;
- Python fallback buffers used when fast path is unavailable.

`Buffer.connect()` exchanges peer metadata. It first tries the RDMA/IBGDA path,
then exchanges IPC handles for intra-node P2P. If neither fast path is usable, it
falls back to a Python implementation based on PyTorch collectives.

### Native buffer

The native `MooncakeEpBuffer` owns or references:

- rank and world-size metadata;
- a GDR workspace buffer;
- P2P and RDMA device transports;
- a communication stream;
- temporary workspace used by dispatch/combine kernels.

If an external Transfer Engine is supplied by a higher layer, EP can reference
device transports owned by that engine; otherwise EP creates and owns the
transports itself.

### Buffer layout

EP allocates paired send/receive buffers for double-buffered dispatch/combine.
Each pair contains:

- RDMA send signal buffer;
- RDMA receive signal buffer;
- RDMA send data buffer;
- RDMA receive data buffer.

The workspace size returned by `Buffer.get_ep_buffer_size_hint()` is derived from
`num_max_dispatch_tokens_per_rank`, `hidden`, `num_ranks`, and `num_experts`.
Size this for peak dispatch demand, not the average request.

## Fast paths and fallback

Mooncake EP has three broad execution modes:

| Mode | Purpose | Notes |
| --- | --- | --- |
| IBGDA / RDMA fast path | Inter-node GPU memory movement. | Requires RDMA-capable environment and successful metadata/QP setup. |
| P2P / IPC fast path | Intra-node peer access such as NVLink. | Requires peer accessibility and IPC handle exchange. |
| Python fallback | Functional fallback for unsupported environments. | Slower; useful for correctness and limited testing. |

The runtime reports whether IBGDA is disabled and whether the fast path is usable
through native helper methods surfaced in Python. The Python wrapper updates its
fallback flag after metadata exchange.

## Metadata exchange

During `Buffer.connect()`, ranks exchange:

- RDMA memory-region address and key;
- local QP numbers;
- LID and GID information;
- subnet prefix and interface ID;
- CUDA IPC handles for local peer access;
- current active-rank mask from the backend.

The exchange uses `dist.all_gather()` and `dist.all_to_all()` on the process
group. For this reason, the process group must already be initialized and healthy
before the EP buffer is constructed or refreshed.

## Dispatch internals

`dispatch()` takes local token hidden states and selected expert IDs. It sends
tokens to expert-owner ranks and packs received tokens into local-expert-major
layout.

Important inputs:

- `x`: `[num_tokens, hidden]` token hidden states;
- `topk_idx`: `[num_tokens, top_k]` global expert IDs;
- `active_ranks`: `[num_ranks]` int32 rank-health tensor;
- `num_max_dispatch_tokens_per_rank`: receive capacity per source rank;
- `num_experts`: global expert count, divisible by `num_ranks`;
- `timeout_us`: failure-detection timeout.

Important outputs:

- packed local-expert input tensor;
- per-local-expert receive counts;
- source/layout metadata handle used by `combine()`;
- event/hook synchronization helpers.

When `use_fp8=True`, dispatch returns packed FP8 data and FP32 scales. The local
expert path must either consume that format directly or dequantize before expert
compute.

## Combine internals

`combine()` sends local expert outputs back to token-owner ranks and applies
routing weights. It consumes the handle produced by the matching `dispatch()`
call.

For zero-copy combine, call `get_next_combine_buffer(handle)`, write expert
outputs into that buffer, then call `combine(..., zero_copy=True)` with the same
handle. Do not reuse a handle across unrelated dispatch/combine pairs.

## Rank activeness and timeout behavior

Mooncake EP receives an `active_ranks` tensor in both dispatch and combine. The
native kernels poll receive signals from source ranks. If `timeout_us` is not
`-1` and a source rank does not make progress before the timeout, the kernel can
mark `active_ranks[src_rank] = 0` and skip that source.

This EP-level tensor is rank-level and should have shape `[num_ranks]`. It is
related to, but not automatically identical to, the backend-level active-rank
mask passed through `MooncakeBackendOptions`. Integrations should propagate
health updates consistently between scheduling logic, PG state, and EP buffers.

## Stream synchronization model

Mooncake EP operations return an `EventOverlap` object and, optionally, a hook:

- If `return_recv_hook=False`, call `event.current_stream_wait()` before using
  the output tensors on the current stream.
- If `return_recv_hook=True`, call the returned `hook()` at the chosen overlap
  point.
- If `async_finish=True`, the wrapper records extra tensors in the event helper
  to keep lifetimes safe for asynchronous use and CUDA graph scenarios.

Always make synchronization explicit when composing EP with custom expert
kernels, CUDA graphs, or application-level streams.

## Recovery integration

When a rank fails, multiple layers must be updated:

1. PG active-rank state must stop collectives from waiting for the failed rank.
2. Scheduler / MoE routing should stop assigning tokens to unavailable experts.
3. Replacement ranks should join through the PG elastic protocol.
4. EP buffers should refresh peer metadata with `update_ep_member()` after the
   process group activates recovered ranks.

EP timeout detection can mark a rank inactive in the EP-level tensor, but higher
layers still need to coordinate recovery and routing decisions.

## Performance considerations

- Prefer fast path operation with working RDMA/IBGDA or P2P peer access.
- Size `num_max_dispatch_tokens_per_rank` for the worst expected per-rank token
  count to avoid overflow.
- Use `return_recv_hook=True` or `async_finish=True` only when the application
  deliberately overlaps communication and compute.
- `use_fp8=True` reduces dispatch bandwidth but requires FP8-aware expert code
  or explicit dequantization.
- Repeatedly reconstructing EP buffers is expensive; refresh membership only
  when PG membership changes.

## Developer test checklist

For EP changes, run correctness across:

- BF16 and FP8 dispatch;
- zero-copy and non-zero-copy combine;
- synchronous event wait and hook-based synchronization;
- fallback and fast path when available;
- failure simulation with finite `timeout_us`;
- multi-rank topologies where `num_experts % num_ranks == 0`.

Useful entry points:

```bash
# EP grid correctness test
python mooncake-ep/tests/test_ep_grid.py

# Wheel-level EP smoke test
python mooncake-wheel/tests/test_mooncake_ep.py
```

Adapt launch commands to the target environment and number of GPUs.

## Related documentation

- [Mooncake Backend (PG) design](mooncake-backend-pg.md)
- [Python API reference](../python-api-reference/ep-backend.md)
- [PG/EP troubleshooting](../troubleshooting/pg-ep-troubleshooting.md)
