# Mooncake EP Dispatch/Combine Benchmark

Measures dispatch/combine throughput and tail latency of the Mooncake EP Buffer
under uniform, k-hot incast, and Zipfian routing patterns.

## Usage

Single-node (uses `mp.spawn` internally):

```bash
python mooncake-ep/benchmarks/ep_benchmark/run_ep_benchmark.py \
    --num-ranks 8 --num-experts 256 --hidden-size 7168 \
    --top-k 8 --num-tokens 1024 --dtype bf16 \
    --routing-mode k_hot --hot-experts 32 --hot-fraction 0.9 \
    --zero-copy --async-finish \
    --warmup-iters 20 --iters 100 \
    --pg-backend nccl \
    --json-output results/khot_8rank.json
```

Multi-node via `torchrun` (`--num-ranks` is ignored, `WORLD_SIZE` from torchrun is used):

```bash
torchrun --nnodes=2 --nproc_per_node=4 --rdzv_backend=c10d \
    --rdzv_endpoint=$HEAD_NODE \
    mooncake-ep/benchmarks/ep_benchmark/run_ep_benchmark.py \
    --num-experts 256 --hidden-size 7168 \
    --top-k 8 --num-tokens 1024 --dtype bf16 \
    --routing-mode k_hot --hot-experts 32 --hot-fraction 0.9 \
    --zero-copy --async-finish \
    --pg-backend nccl \
    --json-output results/khot_8node.json
```

Using a config file (CLI flags override config values):

```bash
python mooncake-ep/benchmarks/ep_benchmark/run_ep_benchmark.py \
    --config mooncake-ep/benchmarks/ep_benchmark/configs/cuda_zipfian.json \
    --num-ranks 8
```

## Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | None | Path to JSON config file (CLI flags override) |
| `--num-ranks` | `8` | Number of EP ranks / GPUs |
| `--num-experts` | `256` | Total experts across all ranks |
| `--hidden-size` | `7168` | Hidden dimension |
| `--top-k` | `8` | Experts per token |
| `--num-tokens` | `1024` | Tokens per rank |
| `--dtype` | `bf16` | Dispatch data type (`bf16` or `fp8`) |
| `--routing-mode` | `uniform` | `uniform`, `k_hot`, or `zipf` |
| `--hot-experts` | `32` | Hot expert count (k_hot mode) |
| `--hot-fraction` | `0.9` | Fraction of tokens to hot experts (k_hot mode) |
| `--zipf-alpha` | `1.0` | Zipf distribution alpha (zipf mode) |
| `--zero-copy` | off | Zero-copy combine via get_next_combine_buffer |
| `--async-finish` | off | Event-based async sync |
| `--return-recv-hook` | off | Hook-based sync (mutually exclusive with `--async-finish`) |
| `--pg-backend` | `nccl` | `nccl` or `mooncake` (mooncake requires RDMA) |
| `--warmup-iters` | `20` | Warmup iterations (not timed) |
| `--iters` | `100` | Measured iterations |
| `--seed` | `0` | Base random seed (each rank uses `seed + rank`) |
| `--json-output` | stdout | Output JSON file path |
| `--master-addr` | `127.0.0.1` | Process group master address |
| `--master-port` | `29500` | Process group master port |

`num_experts` must be divisible by `num_ranks`.

`end_to_end_latency_ms` measures the full dispatch → mock expert forward → combine cycle, not dispatch + combine alone.

## Output

```json
{
  "benchmark": "mooncake_ep",
  "world_size": 8,
  "num_experts": 256,
  "hidden_size": 7168,
  "routing_mode": "k_hot",
  "metrics": {
    "dispatch_latency_ms": {"p50": 0, "p90": 0, "p99": 0, "p999": 0, "mean": 0},
    "combine_latency_ms": {"p50": 0, "p90": 0, "p99": 0, "p999": 0, "mean": 0},
    "end_to_end_latency_ms": {"p50": 0, "p90": 0, "p99": 0, "p999": 0, "mean": 0},
    "tokens_per_second": 0,
    "expert_load": {
      "max_tokens_per_expert": 0,
      "min_tokens_per_expert": 0,
      "mean_tokens_per_expert": 0,
      "imbalance_ratio": 0
    }
  }
}
```
