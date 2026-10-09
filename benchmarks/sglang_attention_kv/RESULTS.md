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

The machine is shared: other tenants held 66 to 69 GB of each GPU's memory while these runs were
taking place. Every window is therefore recorded as its min, p50, p95 and p99, and the tables below
quote p50.

## The columns

All values are milliseconds, p50 of 100 timed iterations after 10 warmups. `indices`, `plan` and `loop`
are the three windows a step is timed in, in the order a forward pass runs them; `sum` is their sum,
`phase_sum`; `window` is the whole step as one span, `step_window`. `write` and `attn` are the two
component passes of section 1 of the README: the step's `set_kv_buffer` calls in one window, and the
branch's attention path over every layer in one window, each run after the timed loop and never added
to the step. `gather` is the read-only probe over the rows the branch's paged side reads.

The step is 0.040 to 0.070 ms longer than the sum of its three windows in every one of the 600 matrix
steps, and 0.053 to 0.060 ms longer in the six long-context steps: that is the between-window interval,
what the three windows do not cover. The measurement does not say what the interval contains.

`gather GB/s` and `read GB/s` are unique-payload normalisations: each divides the bytes its own path
reads, counted once per layer for every token the path covers, by the window that path ran in.
`TFLOP/s` and `FLOP/byte` divide the step's attention arithmetic by the attention window and by those
read bytes.

The ledger behind those last two columns is split by the path that reads the KV, which differs per
branch. For the 8192-token steps at page size 64, one rank:

| step | branch | paged read | ragged read | attention read | page capacity | probe rows |
|---|---|---|---|---|---|---|
| prefill 8192 | `ragged_no_prefix` | 0 B | 302.0 MB | 302.0 MB | 302.0 MB | 8192 |
| 512 queries behind 8192 | `ragged_prefix_merge` | 302.0 MB | 18.9 MB | 320.9 MB | 320.9 MB | 8192 |
| decode at 8193 | `paged_decode` | 302.0 MB | 0 B | 302.0 MB | 304.3 MB | 8193 |

A merge step reads its 8192-token history through the paged wrapper and its own 512 tokens through the
ragged one. A prefill step reads no paged KV at all, because its queries attend to the K/V it computes,
through the ragged wrapper. A decode step reads its whole context through the paged wrapper, where the
page capacity of 304.3 MB covers the 302.0 MB of valid tokens plus one page of padding.

## The step, page size 64, contiguous pool, one sequence

| step | indices | plan | loop | sum | window | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| prefill 128 | 0.126 | 0.150 | 2.415 | 2.682 | 2.735 | 1.137 | 0.962 | 1.129 | 4.2 | 4.9 | 1.3 | 258 |
| prefill 512 | 0.126 | 0.156 | 2.623 | 2.907 | 2.958 | 1.134 | 1.419 | 1.126 | 16.8 | 13.3 | 13.7 | 1026 |
| prefill 2048 | 0.255 | 0.267 | 8.623 | 9.146 | 9.189 | 1.159 | 8.583 | 1.115 | 67.7 | 8.8 | 36.0 | 4098 |
| prefill 8192 | 0.345 | 0.352 | 71.900 | 72.604 | 72.651 | 1.140 | 72.200 | 1.131 | 266.9 | 4.2 | 68.5 | 16386 |
| decode at 129 | 0.339 | 0.215 | 3.315 | 3.879 | 3.933 | 1.150 | 1.652 | 1.156 | 4.1 | 2.9 | 0.0 | 4 |
| decode at 513 | 0.345 | 0.218 | 3.313 | 3.883 | 3.940 | 1.162 | 1.644 | 1.163 | 16.3 | 11.5 | 0.0 | 4 |
| decode at 2049 | 0.329 | 0.209 | 3.225 | 3.765 | 3.820 | 1.141 | 1.590 | 1.127 | 67.0 | 47.5 | 0.2 | 4 |
| decode at 8193 | 0.335 | 0.214 | 3.248 | 3.803 | 3.860 | 1.144 | 1.592 | 1.147 | 263.4 | 189.7 | 0.8 | 4 |

## The long-context run, page size 64, random layout, one sequence

| step | indices | plan | loop | sum | window | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| prefill 16384 | 0.437 | 0.427 | 247.164 | 248.062 | 248.115 | 1.166 | 245.800 | 1.184 | 510.0 | 2.5 | 80.5 | 32770 |
| prefill 32768 | 0.449 | 0.482 | 938.644 | 939.592 | 939.646 | 1.180 | 938.895 | 1.773 | 681.5 | 1.3 | 84.3 | 65538 |
| 512 queries behind 16384 | 0.748 | 0.618 | 21.047 | 22.422 | 22.474 | 1.174 | 21.174 | 1.160 | 520.8 | 29.4 | 59.3 | 2017 |
| 512 queries behind 32768 | 0.764 | 0.620 | 39.411 | 40.806 | 40.860 | 1.165 | 39.523 | 1.787 | 675.9 | 31.0 | 63.1 | 2032 |
| decode at 16385 | 0.355 | 0.225 | 3.387 | 3.972 | 4.032 | 1.182 | 1.671 | 1.163 | 519.5 | 361.4 | 1.4 | 4 |
| decode at 32769 | 0.350 | 0.221 | 3.369 | 3.946 | 4.004 | 1.173 | 1.647 | 1.778 | 679.2 | 733.4 | 2.9 | 4 |

A 32768-token history is 512 pages of 64 tokens and 1152 MiB of KV per rank, and the step's pool
allocated 1096 pages for it, because the churned layout leaves gaps. A 16384-token history is 256 pages
and 576 MiB, out of 552 allocated.

## The query-length axis, extend steps in the merge branch

Page size 64, contiguous pool, one sequence.

| step | indices | plan | loop | sum | window | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 128 queries behind 128 | 0.431 | 0.434 | 6.377 | 7.259 | 7.317 | 1.157 | 4.418 | 1.133 | 4.2 | 2.1 | 0.8 | 385 |
| 512 queries behind 128 | 0.434 | 0.454 | 6.751 | 7.650 | 7.708 | 1.174 | 4.735 | 1.146 | 4.1 | 5.0 | 6.1 | 1230 |
| 2048 queries behind 128 | 0.536 | 0.516 | 9.604 | 10.663 | 10.710 | 1.157 | 9.537 | 1.136 | 4.2 | 8.4 | 36.5 | 4339 |
| 128 queries behind 512 | 0.444 | 0.451 | 6.661 | 7.578 | 7.643 | 1.161 | 4.648 | 1.142 | 16.5 | 5.1 | 2.3 | 461 |
| 512 queries behind 512 | 0.457 | 0.472 | 6.883 | 7.834 | 7.894 | 1.160 | 4.874 | 1.146 | 16.5 | 7.7 | 11.9 | 1537 |
| 2048 queries behind 512 | 0.541 | 0.521 | 11.313 | 12.378 | 12.424 | 1.157 | 11.293 | 1.142 | 16.5 | 8.4 | 41.1 | 4917 |
| 128 queries behind 2048 | 0.453 | 0.463 | 6.741 | 7.665 | 7.725 | 1.158 | 4.665 | 1.138 | 66.3 | 17.2 | 8.5 | 497 |
| 512 queries behind 2048 | 0.452 | 0.474 | 6.962 | 7.894 | 7.955 | 1.159 | 4.889 | 1.141 | 66.2 | 19.3 | 35.6 | 1844 |
| 2048 queries behind 2048 | 0.641 | 0.560 | 18.192 | 19.398 | 19.448 | 1.159 | 18.222 | 1.138 | 66.3 | 8.3 | 50.9 | 6145 |
| 128 queries behind 8192 | 0.444 | 0.456 | 6.696 | 7.611 | 7.671 | 1.163 | 4.730 | 1.160 | 260.3 | 64.8 | 32.9 | 508 |
| 512 queries behind 8192 | 0.550 | 0.519 | 11.780 | 12.857 | 12.905 | 1.173 | 11.806 | 1.144 | 264.0 | 27.2 | 54.0 | 1988 |
| 2048 queries behind 8192 | 0.694 | 0.587 | 45.580 | 46.869 | 46.918 | 1.173 | 45.626 | 1.156 | 261.3 | 8.3 | 61.0 | 7373 |

## The other axes

| axis | configuration | loop | window | attn |
|---|---|---|---|---|
| page size, decode at a 8193-token history | 1, 16, 32, 64, 128 | 3.262, 3.258, 3.256, 3.248, 3.244 | 3.869, 3.857, 3.857, 3.860, 3.845 | 1.596, 1.594, 1.591, 1.592, 1.596 |
| layout, decode at a 8193-token history | contiguous, random | 3.248, 3.244 | 3.860, 3.842 | 1.592, 1.577 |
| batch, prefill 8192 | 1 sequence, 4 even, 4 ragged of 8192/4096/2048/1024 | 71.900, 242.444, 84.714 | 72.651, 243.307, 85.493 | 72.200, 241.868, 84.527 |

## What the numbers show

**Query length and history length move the loop.** A prefill of 128 tokens spends 2.415 ms in the loop,
512 tokens 2.623 ms, 2048 tokens 8.623 ms and 8192 tokens 71.900 ms. Behind an 8192-token history the
merge branch's loop reads 6.696 ms for 128 queries, 11.780 ms for 512 and 45.580 ms for 2048; behind a
2048-token history the same three queries read 6.741, 6.962 and 18.192 ms. In the long-context run 512
queries behind 16384 tokens read 21.047 ms and behind 32768 tokens 39.411 ms.

**A decode step's loop stays between 3.214 and 3.389 ms over the 122 decode steps of the two runs**,
its whole-step window between 3.813 and 4.047 ms and its attention window between 1.577 and 1.672 ms,
while the KV it reads grows from 4.7 MB to 1.2 GB per rank: at a 8193-token history the paged read
covers 302.0 MB in a 1.592 ms window.

**The page size and the layout move no window by more than a tenth of a millisecond.** Decode at a
8193-token history reads 3.244 to 3.262 ms in the loop, 3.845 to 3.869 ms in the whole-step window and
1.591 to 1.596 ms in the attention window over page sizes 1 to 128, and 3.248 against 3.244 ms in the
loop for a contiguous pool against a churned one. This is what the replay builds: SGLang hands
FlashInfer a token-level CSR stream and plans the wrappers at `page_size=1` whatever the pool's page
size is, so the page size reaches the kernels through the addresses in that stream.

**Batch shape moves the loop with the tokens and the pairs the batch computes.** One 8192-token sequence
computes 8192 tokens and 33,558,528 query-key pairs in a 71.900 ms loop; four of them compute 32768
tokens and 134,234,112 pairs in 242.444 ms; four ragged sequences of 8192, 4096, 2048 and 1024 compute
15360 tokens and 44,572,160 pairs in 84.714 ms.

**The gather probe prices a copy of the rows the branch's paged side reads, and those are not always
the rows the attention reads.** On the merge step behind 32768 tokens the probe moves the 32768 rows of
paged history, both K and V, in 1.787 ms, 675.9 GB/s, while the attention window over that history is
39.523 ms. On a prefill step, which reads no paged KV, the probe moves the rows the step wrote instead,
and the attention reads the step's own K and V tensors: the 32768-token prefill moves 1.2 GB per rank in
1.773 ms, 681.5 GB/s, in a run whose attention window is 938.895 ms.

## What the component windows are, and what they are not

The write and the attention components are windows of their own, measured after the timed loop, and
they do not decompose it.

They do not add up to the loop. On the four-sequence 512-token prefill rows the two windows sum to
about 3.5 ms against a loop of 2.42 to 2.49 ms, with the attention window alone (2.254 to 2.379 ms)
already most of the loop. On the 8192-token prefill the attention window (72.200 ms) is above the loop
(71.900 ms), while on the 128-query extend behind 8192 tokens it is 4.730 ms against a loop of
6.696 ms. The loop holds a layer's attention and its KV write as the branch interleaves them; each
component holds one of the two, and the merge branch's path is two wrapper calls and one `merge_state`
per layer rather than one call. The CSV records their ratio to `phase_sum`, as a ratio of two measured
windows, and no table here treats the components as parts that reconstruct the step.

The write window is reported as a window rather than as a rate. It reads 1.114 to 1.182 ms for steps
that write 1 to 32768 tokens, so it does not follow the bytes written, and inside a single row its p95
sits 0.088 ms above its min at the median and 0.220 ms above it at most. Nothing in the measurement
separates the host's issue of the calls from the device's execution of them, which is why the CSV
derives no bandwidth and no per-token cost from the write side.

Each step's record in `kernel_summary.csv` carries the values behind every statement here: each
window's min, p50, p95 and p99, the ledger, the derived figures and the three check results.

## Reading a row back

`kernel_summary.csv` carries, per step: the branch and the order the step ran in, the configuration the
step was built with, the ledger — `kv_bytes_written`, `kv_bytes_paged_read`, `kv_bytes_ragged_read`,
`kv_bytes_attention_read`, `kv_bytes_page_capacity`, `gather_rows`, `gather_bytes`, `attention_pairs`,
`attention_flops` — every window's min, p50, p95 and p99, the derived figures, and the result of the
three checks. The min of a window is the smallest sample it took in its row, which is the figure a
slower sample during the run does not move: the largest loops of the matrix, the batch-4 prefills of
8192-token sequences at 242.41 to 242.49 ms, have a p95 0.65 to 0.85 ms above their min.
