# Mooncake PG/EP Troubleshooting

This page covers common setup, import, runtime, and recovery issues for
Mooncake PG and Mooncake EP.

## PG JIT import fails

Symptoms:

```text
Mooncake PG JIT requires Ninja to compile the Torch adapter.
Mooncake PG Torch adapter JIT requires a CUDA toolkit with nvcc.
```

Cause:

`mooncake.pg` builds its Torch-facing adapter for the installed PyTorch at
first import. That build requires Ninja and a CUDA toolkit with `nvcc`.

Fixes:

1. Verify the active PyTorch version:

   ```bash
   python - <<'PY'
   import torch
   print(torch.__version__)
   print(torch.version.cuda)
   PY
   ```

2. Install Ninja and a CUDA toolkit compatible with the installed PyTorch:

   ```bash
   python -m pip install ninja
   ```

3. Re-import `mooncake.pg`; it will rebuild into the local JIT cache.

## `dist.get_world_size()` differs from the number of active ranks

Mooncake PG preserves stable in-group rank slots. Consequently:

- `max_group_size` is the fixed slot capacity;
- `dist.get_world_size()` is the highest active in-group rank plus one;
- holes below that extent remain visible in the rank space but are skipped by
  the active mask.

Use `pg.get_active_ranks(backend)` to inspect the current backend mask.

## `join_group()` hangs or activation times out

Common causes:

- No existing rank submitted `activate_ranks()` / `recover_ranks()` after the
  joining process entered `join_group()`.
- The future active set is not mutually connected, so the activation proposal
  remains pending until timeout.

## EP dispatch/combine timeout marks a rank inactive

Mooncake EP kernels can update the EP-level `active_ranks` tensor when a source
rank does not make progress before `timeout_us`.

If this happens unexpectedly:

- Increase `timeout_us` to rule out slow startup or transient scheduling delay.
- Check whether the source rank exited, crashed, or skipped the matching
  dispatch/combine call.
- Confirm `num_experts % num_ranks == 0` and that every rank uses the same
  `num_experts`, `top_k`, and buffer sizing assumptions.
- Ensure all ranks call dispatch and combine in the same order.

If timeout detection is not desired for a test, pass `timeout_us=-1`.

## EP falls back instead of using the fast path

The Python wrapper uses fallback when the native runtime cannot use IBGDA/RDMA or
fully accessible P2P.

Possible causes:

- RDMA devices or drivers are not available inside the container.
- HCA selection picked the wrong device.
- GPUDirect RDMA / peer-memory support is missing.
- CUDA IPC or peer access is unavailable between local GPUs.
- The environment was built without the required accelerator support.

Debug checklist:

1. Restrict HCA selection with `pg.set_device_filter([...])` or
   `MOONCAKE_PGTEST_DEVICE_FILTERS`.
2. Check whether all intended ranks can see the same accelerator and RDMA
   devices.
3. Run PG collectives first; EP metadata exchange depends on a healthy process
   group.
4. Run EP tests with fallback enabled and disabled to separate correctness from
   transport setup.

## RDMA connection errors or severe latency from wrong HCA selection

Symptoms can include connection setup failures, repeated transport timeouts,
unexpected fallback, very low bandwidth, or RDMA retry-style errors when the
backend auto-selects an unsuitable NIC/HCA. This commonly happens on machines
with multiple RDMA devices where some devices are for management traffic, are on
different fabrics, or are not reachable from peer ranks.

Fix: set an explicit device filter before initializing Mooncake PG/EP so all
ranks use the intended HCA list.

For application code:

```python
from mooncake import pg

pg.set_device_filter(["mlx5_1", "mlx5_2"])
# call dist.init_process_group(...) after setting the filter
```

For PG tests and benchmarks:

```bash
MOONCAKE_PGTEST_DEVICE_FILTERS=mlx5_1,mlx5_2 \
python -m unittest discover -s mooncake-pg/tests -k CUDA -v
```

Use the same filter on every rank. If the problem disappears after setting the
filter, treat the original failure as a topology/device-selection issue rather
than an EP kernel or collective correctness problem.

## EP output mismatch or buffer overflow symptoms

Common causes:

- `num_max_dispatch_tokens_per_rank` is smaller than the actual per-rank token
  count.
- `num_experts` is not divisible by `num_ranks`.
- `topk_idx` contains expert IDs outside `[0, num_experts)` except for masked
  `-1` entries.
- The `handle` from one dispatch call is reused with an unrelated combine call.
- `zero_copy=True` is used without writing expert outputs into
  `get_next_combine_buffer(handle)`.

Fixes:

- Size the buffer for peak traffic:

  ```python
  num_ep_buffer_bytes = Buffer.get_ep_buffer_size_hint(
      max_tokens_per_rank, hidden, world_size, num_experts
  )
  ```

- Keep dispatch and combine paired by using the matching `handle`.
- Call `event.current_stream_wait()` or the returned `hook()` before consuming
  operation outputs.

## P2P send/recv errors

Mooncake PG's PyTorch P2P path currently supports single-tensor send/recv
through the backend shim. If `batch_isend_irecv()` fails:

- verify each `P2POp` contains one tensor;
- ensure peer ranks are active;
- check that all ranks issue matching send/recv operations;
- test CPU and accelerator backends separately to isolate device-specific
  transport issues.

## Useful smoke commands

```bash
# PG CPU sanity
python -m unittest discover -s mooncake-pg/tests -k CPU -v

# PG CUDA sanity
python -m unittest discover -s mooncake-pg/tests -k CUDA -v

# PG all-reduce benchmark smoke
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/pgbench.py \
  --collective all_reduce --backend mooncake --device cuda -g 2 -b 8 -e 1M -f 2

# EP grid test
python mooncake-ep/tests/test_ep_grid.py
```

Adapt process counts, device filters, and launchers to the target cluster.

## What to include in bug reports

When reporting PG/EP issues, include:

- Mooncake commit and installation method;
- PyTorch version and CUDA/accelerator runtime version;
- exact backend name (`mooncake` or `mooncake-cpu`);
- world size, max world size, rank IDs, and subgroup layout;
- active-rank tensor dtype/device/values;
- HCA/device filters;
- minimal command or script reproducing the issue;
- logs from all ranks around initialization, failure detection, and recovery.
