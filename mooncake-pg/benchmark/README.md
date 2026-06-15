# mooncake-pg benchmarks

This directory contains Python benchmark harnesses for Mooncake Backend (PG).
The CLI and output style intentionally resemble `nccl-tests` where possible.

## `pgbench.py`

`pgbench.py` measures collective operations across Mooncake, NCCL, or Gloo
backends.

### CPU quick start

```bash
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/pgbench.py \
  --collective all_reduce --backend mooncake-cpu --device cpu \
  -g 2 -b 256 -e 256 -n 2 -w 1 -c 1
```

### CUDA quick start

```bash
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/pgbench.py \
  --collective all_reduce --backend mooncake --device cuda \
  -g 8 -b 8 -e 128M -f 2
```

Select HCAs explicitly when needed:

```bash
MOONCAKE_PGTEST_DEVICE_FILTERS=mlx5_1,mlx5_2 \
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/pgbench.py \
  --collective all_reduce --backend mooncake --device cuda \
  -g 8 -b 8 -e 128M -f 2
```

### Common options

| Option | Meaning |
| --- | --- |
| `--collective` | Collective to benchmark, such as `all_reduce`. Unsupported collectives are skipped. |
| `--backend` | `mooncake`, `mooncake-cpu`, `nccl`, or `gloo`. |
| `--device` | `cuda` or `cpu`. Match this with the selected backend. |
| `-g, --ngpus` | Number of local worker processes / local ranks. |
| `-b` | Minimum message size. Supports suffixes such as `K`, `M`, `G`. |
| `-e` | Maximum message size. |
| `-f` | Size multiplier between benchmark steps. |
| `-w` | Warmup iterations. |
| `-n` | Timed iterations. |
| `-c` | Parallel channels / concurrency setting when supported by the harness. |
| `-d` | Dtype selection. Use `-d all` to sweep supported types. |

### Interpreting results

Use the benchmark primarily for relative comparisons across backend, device,
message size, and HCA selection. Before interpreting bandwidth numbers, confirm:

- all ranks are on the expected devices;
- Mooncake is using the intended NICs;
- CPU and CUDA clocks are stable enough for the target measurement;
- warmup iterations are sufficient for the message size;
- no rank reports fallback, timeout, or active-rank changes unexpectedly.

## `p2p_regular_k_bench.py`

`p2p_regular_k_bench.py` measures pure P2P traffic. Each rank sends to
`+1..+k` peers and receives from `-1..-k` peers modulo world size.

### CPU quick start

```bash
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/p2p_regular_k_bench.py \
  --backend mooncake-cpu --device cpu -g 8 -k 2 --tensor-bytes 4M -w 5 -n 30
```

### CUDA quick start

```bash
PYTHONPATH=mooncake-pg \
python mooncake-pg/benchmark/p2p_regular_k_bench.py \
  --backend mooncake --device cuda -g 8 -k 3 --tensor-bytes 8M -w 10 -n 50
```

### Output fields

- `lat_mean_us`: mean P2P round latency in microseconds.
- `lat_p50_us`, `lat_p95_us`, `lat_p99_us`: latency percentiles.
- `rank_bidir_GBps`: per-rank bidirectional bandwidth.
- `cluster_bidir_GBps`: aggregate bidirectional bandwidth across all ranks.

## Notes and limitations

- Benchmarks are currently single-node oriented. Use `-g/--ngpus` as local
  ranks when spawning.
- Dtypes are aligned with `nccl-tests` conventions where possible.
- JSON output (`-J`) is not supported yet.
- Always run functional PG tests before relying on benchmark numbers.
- For RDMA-related regressions, compare runs with and without explicit HCA
  filters to identify topology or device-selection issues.
