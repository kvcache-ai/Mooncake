# mooncake-pg pgbench

Python benchmark harness for mooncake-pg with nccl-tests style CLI and output.

## Quick Start (CPU)
```
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/pgbench.py \
  --collective all_reduce --backend mooncake-cpu --device cpu -g 2 -b 256 -e 256 -n 2 -w 1 -c 1
```

## Quick Start (CUDA)
```
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/pgbench.py \
  --collective all_reduce --backend mooncake --device cuda -g 8 -b 8 -e 128M -f 2
```

### Explicit Tree + Simple

The Coordinator reads `MOONCAKE_PG_COLLECTIVE_ALGORITHM` at startup. Set it on
all launched benchmark workers so the Coordinator receives the selection:

```bash
MOONCAKE_PG_PREFERRED_GPU_COLLECTIVE_BACKEND=new \
MOONCAKE_PG_COLLECTIVE_ALGORITHM=tree \
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/pgbench.py \
  --collective all_reduce --backend mooncake --device cuda -g 4 -b 8 -e 128M -f 2
```

`tree` requires the group to select the new backend and every active GPU rank
to advertise compatible Tree/Simple endpoints. Initialization or activation is
rejected when that requirement fails. CPU groups are unaffected. `auto`, `ring`
and `oneshot` keep their existing selection and fallback behavior; auto does
not select Tree. Changing the environment of an existing Coordinator does not
change its selection.

Tree uses a single NCCL-style binary tree and the shared Simple resources. Its
CUDA Graph keeps a fixed kernel and reads the latest plan on compatible
membership changes within the communicator capacity. Captured input addresses,
count, dtype/op and algorithm remain fixed; changing them requires recapture.
A failed AllReduce can partially overwrite output and is not retried or rolled
back automatically. Restore input before replaying after failure.

For C API users, set `preferredGpuCollectiveBackend` to
`mooncakePgGpuCollectiveBackendNew` in the existing comm config; its initializer
continues to default to legacy. No public C API signatures or config fields
were added for Tree.

Set `MOONCAKE_PGTEST_DEVICE_FILTERS=mlx5_1,mlx5_2,...` to explicitly set HCA whitelist.

## Notes
- Single-node only (v1). Use `-g/--ngpus` as local ranks when spawning.
- Dtypes align with nccl-tests; use `-d all` to sweep supported types.
- Sizes accept suffixes like `K`, `M`, `G` (e.g., `-b 8 -e 128M -f 2`).
- Size/count columns are always human-readable.
- Unsupported collectives are ignored (not implemented).
- JSON output (`-J`) is not supported yet.
- Supported backends: `mooncake`, `mooncake-cpu`, `nccl`, `gloo`.

## P2P Regular-k Bench

Use `p2p_regular_k_bench.py` for pure P2P traffic tests with configurable
regular-k topology (each rank sends to `+1..+k` peers and receives from
`-1..-k` peers modulo world size).

### Quick Start (CPU)
```
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/p2p_regular_k_bench.py \
  --backend mooncake-cpu --device cpu -g 8 -k 2 --tensor-bytes 4M -w 5 -n 30
```

### Quick Start (CUDA)
```
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/p2p_regular_k_bench.py \
  --backend mooncake --device cuda -g 8 -k 3 --tensor-bytes 8M -w 10 -n 50
```

### Output
- `lat_mean_us`, `lat_p50_us`, `lat_p95_us`, `lat_p99_us`
- `rank_bidir_GBps`: per-rank bidirectional bandwidth
- `cluster_bidir_GBps`: aggregate bidirectional bandwidth over all ranks
