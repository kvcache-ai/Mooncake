# Mooncake PG/EP Troubleshooting

This page covers common setup, import, runtime, and recovery issues for
Mooncake Backend (PG) and Mooncake EP.

## Import fails with a PyTorch version error

Symptoms:

```text
Mooncake PG was not built against torch==...
Mooncake EP was not built against torch==...
```

Cause:

`mooncake.pg` and `mooncake.ep` load native extension modules whose names include
the current PyTorch version. If the wheel or source build does not contain an
extension for the active `torch.__version__`, import fails.

Fixes:

1. Verify the active PyTorch version:

   ```bash
   python - <<'PY'
   import torch
   print(torch.__version__)
   print(torch.version.cuda)
   PY
   ```

2. Install a Mooncake wheel built for that PyTorch/CUDA combination, or rebuild
   from source with EP/PG enabled:

   ```bash
   cmake .. -DWITH_EP=ON
   make -j
   ```

3. Make sure the Python environment used at runtime is the same one used for the
   build.

## `activeRanks must be int` or device mismatch

Symptoms:

```text
activeRanks must be int.
activeRanks must be on CPU.
activeRanks must be on GPU.
activeRanks must be sized to max_world_size when max_world_size is set
```

Causes and fixes:

- Use `torch.int32`, not `bool`, `int64`, or floating-point types.
- For `backend="mooncake"`, put `active_ranks` on the accelerator device.
- For `backend="mooncake-cpu"`, put `active_ranks` on CPU.
- If `max_world_size` is set, allocate `active_ranks` with length
  `max_world_size`, even if the initial visible `world_size` is smaller.

Examples:

```python
# CUDA / accelerator backend
active_ranks = torch.ones(max_world_size, dtype=torch.int32, device="cuda")

# CPU backend
active_ranks = torch.ones(max_world_size, dtype=torch.int32)
```

## `dist.get_world_size()` is smaller than `max_world_size`

This is expected. Mooncake PG distinguishes reserved capacity from visible active
membership:

- `max_world_size` reserves future rank slots.
- `dist.get_world_size()` returns the visible active size.
- Reserved ranks are inactive until `recover_ranks()` activates them.

Use `pg.get_active_ranks(backend)` to inspect the current backend mask.

## `get_peer_state()` or `join_group()` hangs

Common causes:

- Healthy ranks are not all calling `get_peer_state()` in the same order.
- The joining rank did not initialize with `is_extension=True`.
- `max_world_size` / rank numbering differs between healthy and joining ranks.
- Subgroups were created in different orders on healthy and joining processes.
- The joining process has not published peer metadata yet.
- Network device filters differ across ranks.

Debug checklist:

1. Confirm all ranks use the same rendezvous address and store.
2. Print rank, visible `world_size`, and `max_world_size` at initialization.
3. Confirm `active_ranks` length and values on every rank.
4. Verify healthy ranks poll the same `join_ranks` list.
5. For subgroup recovery, confirm every rank calls `dist.new_group()` in the same
   order.
6. If using RDMA, set the same HCA whitelist on every rank.

## Newly extended ranks participate too early

After `pg.extend_group_size_to(backend, new_size)`, new ranks are reserved but
inactive. They should not participate in collectives until healthy ranks call
`pg.recover_ranks(backend, ranks)`.

If a new rank appears to participate early, check whether:

- `active_ranks` was initialized with `1` for future ranks without masking them
  through the backend protocol;
- the application called collectives on the joining process before
  `pg.join_group()` returned;
- different ranks used inconsistent `max_world_size` or rank IDs.

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
