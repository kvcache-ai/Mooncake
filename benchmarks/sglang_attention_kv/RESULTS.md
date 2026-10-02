# One validation run

Numbers from one run of the full step matrix (600 steps) and one long-context run (6 steps) on the
machine below, every step passing its three checks. They bound the example figures in this repository
to that machine and those versions; they are not a precondition of the benchmark, and a run elsewhere
reports its own CSV and manifest.

## The machine

| Item | Value during this run |
|---|---|
| GPU | NVIDIA H20, 96 GB of memory, compute capability 9.0 |
| Driver | 580.126.20, driver-reported CUDA version 13.0 |
| Host memory | 1929 GB |
| Python | 3.10.12 |
| PyTorch | 2.13.0+cu130 |
| SGLang | 0.5.20 |
| FlashInfer | 0.6.18, which is what the wrappers SGLang calls come from |
| Model | `Qwen/Qwen3-8B`, 36 layers, 32 query heads, 8 KV heads, head_dim=128, bf16 |
| TP | 4, so each rank holds 2 KV heads and 8 query heads: 36864 bytes of KV per token per rank |
| Branch | `ragged_prefix_merge` for the extend steps, with `SGLANG_FLASHINFER_USE_PAGED` unset |

Commands:

```bash
python -m benchmarks.sglang_attention_kv --full --model <model> --tp-size 4 --device cuda:0 \
    --kernel-warmup 10 --kernel-timed 100 --result-dir <matrix-dir>
python -m benchmarks.sglang_attention_kv --full --model <model> --tp-size 4 --device cuda:0 \
    --input-lens 16384 32768 --chunk-lens 512 --batch-sizes 1 --page-sizes 64 --layouts random \
    --kernel-warmup 10 --kernel-timed 100 --result-dir <long-dir>
```

All values in ms, p50 of 100 timed iterations after 10 warmups. `step` is `indices` + `kv_write` +
`plan` + `attention`. `attention GB/s` divides the bytes the paged read covers by the attention phase;
`gather GB/s` divides the bytes the probe moves — both tensors of the rows it reads — by its own phase.

## The step, page size 64, contiguous pool, one sequence

| step | indices | kv_write | plan | attention | step | gather | gather GB/s | attention GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|
| prefill 128 | 0.130 | 1.187 | 0.153 | 1.032 | 2.511 | 1.151 | 4.1 | 4.6 | 1.2 | 258 |
| prefill 512 | 0.133 | 0.996 | 0.159 | 1.441 | 2.725 | 1.146 | 16.5 | 13.1 | 13.4 | 1026 |
| prefill 2048 | 0.306 | 0.095 | 0.300 | 8.422 | 9.143 | 1.133 | 66.6 | 9.0 | 36.7 | 4098 |
| prefill 8192 | 0.441 | 0.232 | 0.404 | 72.061 | 73.139 | 1.144 | 264.0 | 4.2 | 68.7 | 16386 |
| prefill 16384 | 0.428 | 0.412 | 0.408 | 245.972 | 247.259 | 1.174 | 514.3 | 2.5 | 80.5 | 32770 |
| prefill 32768 | 0.437 | 0.775 | 0.458 | 938.703 | 940.387 | 1.767 | 683.7 | 1.3 | 84.3 | 65538 |
| decode at 129 | 0.329 | 1.199 | 0.208 | 1.684 | 3.423 | 1.154 | 4.1 | 4.2 | 0.0 | 3 |
| decode at 513 | 0.333 | 1.215 | 0.208 | 1.698 | 3.462 | 1.161 | 16.3 | 12.5 | 0.0 | 4 |
| decode at 2049 | 0.337 | 1.244 | 0.214 | 1.748 | 3.564 | 1.147 | 65.9 | 44.5 | 0.2 | 4 |
| decode at 8193 | 0.332 | 1.231 | 0.210 | 1.733 | 3.523 | 1.172 | 257.6 | 175.7 | 0.7 | 4 |
| decode at 16385 | 0.348 | 1.232 | 0.220 | 1.715 | 3.526 | 1.159 | 521.4 | 353.6 | 1.4 | 4 |
| decode at 32769 | 0.349 | 1.234 | 0.219 | 1.705 | 3.520 | 1.769 | 682.8 | 710.0 | 2.8 | 4 |

The rows above 8192 tokens come from the long-context run, which used the random layout; every other
row is one sequence in a contiguous pool.

## The query-length axis, extend steps in the merge branch

| step | indices | kv_write | plan | attention | step | attention GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|
| 128 queries behind 8192 | 0.432 | 1.254 | 0.444 | 4.792 | 6.950 | 64.0 | 32.5 | 508 |
| 512 queries behind 8192 | 0.619 | 0.062 | 0.570 | 11.780 | 13.040 | 27.2 | 54.1 | 1988 |
| 2048 queries behind 8192 | 0.791 | 0.098 | 0.645 | 45.580 | 47.119 | 8.3 | 61.1 | 7373 |
| 128 queries behind 2048 | 0.434 | 1.254 | 0.445 | 4.786 | 6.939 | 16.8 | 8.3 | 497 |
| 512 queries behind 2048 | 0.439 | 1.241 | 0.458 | 4.970 | 7.126 | 19.0 | 35.0 | 1844 |
| 2048 queries behind 2048 | 0.750 | 0.097 | 0.627 | 18.160 | 19.643 | 8.3 | 51.1 | 6145 |

## The other axes

| axis | configuration | attention | step |
|---|---|---|---|
| page size, decode at a 8193-token history | 1, 16, 32, 64, 128 | 1.748, 1.741, 1.745, 1.733, 1.746 | 3.554, 3.544, 3.544, 3.523, 3.545 |
| layout, decode at a 8193-token history | contiguous, random | 1.733, 1.721 | 3.523, 3.517 |
| batch, prefill 8192 | 1 sequence, 4 even, 4 ragged | 72.061, 241.846, 84.480 | 73.139, 243.553, 85.736 |

## What the numbers show

These are measurements of a step's phases. What a phase costs can be read off the table; what causes it
cannot, and none of it is inferred here from the ratio of two phases or from subtracting one step's time
from another's. Attributing a phase to the bytes it moves, to the arithmetic, or to a kernel's launch
overhead needs a profiler or an experiment built for that question, and this benchmark does not carry
one.

**The phases, by size.** A 32768-token prefill spends 938.703 ms in `attention`, 0.775 ms in `kv_write`,
0.458 ms in `plan` and 0.437 ms in `indices`, and the probe moves the same history's 1.2 GB per rank in
1.767 ms. A decode step at a 32769-token history spends 1.705 ms, 1.234 ms, 0.219 ms and 0.349 ms, and
its probe moves the same history in 1.769 ms.

**Query length moves the attention phase; history length leaves the decode phases flat.** Behind an
8192-token history the merge branch takes 4.792 ms for 128 queries, 11.780 ms for 512 and 45.580 ms for
2048, while a decode step's four phases stay within 0.05 ms from a 129-token to a 32769-token history
even though what it reads grows by 254×.

**The `kv_write` interval is either the write's execution or the host's issue time.** It reads 0.062 ms
in the 512-queries-behind-8192 row and 1.254 ms in the 128-queries-behind-8192 row, for the same 36
per-layer calls: the interval the device takes to pass between the events is the execution time when
the host has already issued the calls, and the issue time when it has not. The per-layer figure is in
the CSV; a reader comparing two rows should compare the same quantity. The same caveat applies to
`indices` and `plan`, which are small host-side phases.

**The page size and the layout barely move any phase.** Decode at a 8193-token history is 3.523 to
3.554 ms across page sizes 1 to 128, and 3.523 against 3.517 ms for a contiguous pool against a churned
one.

**Batch shape moves the prefill phase in proportion to the arithmetic.** A 8192-token prefill takes
72.061 ms at one sequence, 241.846 ms at four even sequences and 84.480 ms for four sequences of 8192,
4096, 2048 and 1024.

## Reading a row back

`kernel_summary.csv` carries, per step: the branch and the order the step ran in, the ledger
(`kv_bytes_written`, `kv_bytes_read_valid`, `kv_bytes_read_pages`, `gather_rows`, `gather_bytes`,
`attention_pairs`, `attention_flops`), every phase's min, p50, p95 and p99, the derived figures, and the
result of the three checks. The min of a phase is the uncontended floor: on a shared machine a
co-tenant can slow part of a run, and this run's own check for that is that no phase's p50 exceeds its
min by more than 0.7 ms out of 241 ms in the batch-4 prefill rows, the largest phases in the matrix.
