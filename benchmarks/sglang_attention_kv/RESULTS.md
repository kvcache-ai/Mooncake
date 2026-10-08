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

The machine is shared: other tenants held 66 to 87 GB of each GPU's memory and one of them ran work on
the same device during these runs. Every phase is therefore recorded as its min, p50, p95 and p99, and
the tables below quote p50.

## The columns

All values are milliseconds, p50 of 100 timed iterations after 10 warmups. `indices`, `plan` and `loop`
are the three windows a step is timed in, in the order a forward pass runs them; `step` is their sum.
`write` and `attn` are the two component passes of section 1 of the README: all 36 `set_kv_buffer`
calls in one window, and all 36 attention calls in one window, each run after the timed loop and never
added to the step. `gather` is the read-only probe over the rows the step reads.

`GB/s` divides the step's logical valid KV bytes by the window those bytes belong to: `read` for the
attention window, and the probe's own bytes for `gather`. No figure here divides by the page capacity:
that count is in the CSV as `kv_bytes_read_pages`, an allocation figure.

## The step, page size 64, contiguous pool, one sequence

| step | indices | plan | loop | step | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|
| prefill 128 | 0.128 | 0.152 | 2.445 | 2.718 | 1.162 | 0.995 | 1.151 | 4.1 | 4.7 | 1.2 | 258 |
| prefill 512 | 0.129 | 0.158 | 2.677 | 2.970 | 1.159 | 1.420 | 1.145 | 16.5 | 13.3 | 13.6 | 1026 |
| prefill 2048 | 0.267 | 0.275 | 8.527 | 9.074 | 1.162 | 8.449 | 1.125 | 67.1 | 8.9 | 36.6 | 4098 |
| prefill 8192 | 0.361 | 0.358 | 71.737 | 72.464 | 1.158 | 72.208 | 1.144 | 264.0 | 4.2 | 68.5 | 16386 |
| decode at 129 | 0.347 | 0.219 | 3.306 | 3.881 | 1.165 | 1.641 | 1.158 | 4.1 | 2.9 | 0.0 | 4 |
| decode at 513 | 0.349 | 0.221 | 3.353 | 3.925 | 1.171 | 1.642 | 1.162 | 16.3 | 11.5 | 0.0 | 4 |
| decode at 2049 | 0.342 | 0.226 | 3.315 | 3.888 | 1.171 | 1.649 | 1.148 | 65.8 | 45.8 | 0.2 | 4 |
| decode at 8193 | 0.340 | 0.218 | 3.341 | 3.902 | 1.167 | 1.642 | 1.161 | 260.1 | 183.9 | 0.7 | 4 |

## The long-context run, page size 64, random layout, one sequence

| step | indices | plan | loop | step | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|
| prefill 16384 | 0.389 | 0.390 | 245.216 | 246.010 | 1.145 | 244.341 | 1.170 | 516.2 | 2.5 | 81.0 | 32770 |
| prefill 32768 | 0.401 | 0.445 | 931.418 | 932.299 | 1.144 | 931.228 | 1.741 | 693.9 | 1.3 | 85.0 | 65538 |
| 512 queries behind 16384 | 2.422 | 4.747 | 46.051 | 52.926 | 0.066 | 43.916 | 0.966 | 625.3 | 14.2 | 28.6 | 2017 |
| 512 queries behind 32768 | 0.729 | 0.610 | 39.080 | 40.421 | 1.148 | 39.173 | 1.770 | 682.3 | 31.3 | 63.6 | 2032 |
| decode at 16385 | 0.362 | 0.227 | 3.375 | 3.968 | 1.164 | 1.629 | 1.166 | 518.2 | 370.8 | 1.5 | 4 |
| decode at 32769 | 0.353 | 0.222 | 3.347 | 3.926 | 1.159 | 1.636 | 1.758 | 687.1 | 738.4 | 3.0 | 4 |

A 32768-token history holds 1096 pages of 64 tokens and 2468 MiB of KV per rank; a 16384-token history
holds 552 pages and 1244 MiB per rank.

## The query-length axis, extend steps in the merge branch

Page size 64, contiguous pool, one sequence.

| step | indices | plan | loop | step | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 128 queries behind 128 | 0.464 | 0.461 | 6.512 | 7.451 | 1.168 | 4.457 | 1.148 | 4.1 | 2.1 | 0.8 | 385 |
| 512 queries behind 128 | 0.465 | 0.482 | 6.735 | 7.692 | 1.171 | 4.626 | 1.147 | 4.1 | 5.1 | 6.3 | 1230 |
| 2048 queries behind 128 | 0.536 | 0.519 | 9.595 | 10.652 | 1.178 | 9.535 | 1.154 | 4.1 | 8.4 | 36.5 | 4339 |
| 128 queries behind 512 | 0.452 | 0.461 | 6.712 | 7.630 | 1.174 | 4.656 | 1.150 | 16.4 | 5.1 | 2.3 | 461 |
| 512 queries behind 512 | 0.469 | 0.487 | 6.919 | 7.887 | 1.166 | 4.867 | 1.148 | 16.4 | 7.8 | 11.9 | 1537 |
| 2048 queries behind 512 | 0.555 | 0.525 | 11.308 | 12.395 | 1.151 | 11.281 | 1.146 | 16.5 | 8.4 | 41.1 | 4917 |
| 128 queries behind 2048 | 0.459 | 0.468 | 6.743 | 7.680 | 1.171 | 4.664 | 1.138 | 66.4 | 17.2 | 8.5 | 497 |
| 512 queries behind 2048 | 0.463 | 0.490 | 6.950 | 7.914 | 1.187 | 4.878 | 1.130 | 66.8 | 19.3 | 35.7 | 1844 |
| 2048 queries behind 2048 | 0.659 | 0.569 | 18.184 | 19.418 | 1.172 | 18.219 | 1.152 | 65.5 | 8.3 | 50.9 | 6145 |
| 128 queries behind 8192 | 0.459 | 0.478 | 6.803 | 7.759 | 1.170 | 4.712 | 1.168 | 258.5 | 65.1 | 33.1 | 508 |
| 512 queries behind 8192 | 0.562 | 0.527 | 11.769 | 12.867 | 1.160 | 11.785 | 1.136 | 265.8 | 27.2 | 54.1 | 1988 |
| 2048 queries behind 8192 | 0.740 | 0.604 | 45.578 | 46.924 | 1.157 | 45.677 | 1.172 | 257.6 | 8.3 | 60.9 | 7373 |

The last row is a re-run. In the matrix run its attention window read 97.579 ms while the identical
shape at page sizes 1, 16, 32 and 128 read 45.591 to 45.657 ms in the same run; the row above is the
same step measured again with a quiet device, and its 45.677 ms agrees with those four.

## The other axes

| axis | configuration | loop | step | attn |
|---|---|---|---|---|
| page size, decode at a 8193-token history | 1, 16, 32, 64, 128 | 3.334, 3.285, 3.305, 3.341, 3.339 | 3.903, 3.883, 3.885, 3.902, 3.912 | 1.642, 1.645, 1.645, 1.642, 1.645 |
| layout, decode at a 8193-token history | contiguous, random | 3.341, 3.304 | 3.902, 3.895 | 1.642, 1.648 |
| batch, prefill 8192 | 1 sequence, 4 even, 4 ragged of 8192/4096/2048/1024 | 71.737, 242.374, 84.745 | 72.464, 243.236, 85.516 | 72.208, 241.754, 84.533 |

## What the numbers show

**Query length and history length move the loop.** A prefill step of 8192 tokens spends 71.737 ms in
the loop, 2048 tokens 8.527 ms and 512 tokens 2.677 ms. Behind an 8192-token history the merge branch's
loop reads 6.803 ms for 128 queries, 11.769 ms for 512 and 45.578 ms for 2048; behind a 2048-token
history the same three queries read 6.743, 6.950 and 18.184 ms.

**A decode step's loop stays between 3.285 and 3.375 ms from a 129-token to a 32769-token history**,
and its isolated attention window between 1.629 and 1.649 ms over the same range, while the KV the step
reads grows from 4.7 MB to 1.2 GB per rank. The whole step reads 3.881 to 3.968 ms across the twelve
decode steps of the two runs.

**The page size and the layout move no window beyond a few hundredths of a millisecond.** Decode at a
8193-token history reads 3.285 to 3.341 ms in the loop and 3.883 to 3.912 ms in the step over page
sizes 1 to 128, and 3.341 against 3.304 ms in the loop for a contiguous pool against a churned one.
This matches what the benchmark replays: SGLang hands FlashInfer a token-level CSR stream and plans the
wrappers at `page_size=1` whatever the pool's page size is, so the page size reaches the kernels
through the addresses in that stream and nowhere else.

**Batch shape scales the loop with the tokens the batch computes.** One 8192-token sequence takes
71.737 ms of loop, four take 242.374 ms, and four sequences of 8192, 4096, 2048 and 1024 take 84.745 ms,
which is the same 8192 tokens under a causal mask.

**The gather probe moves the same rows about twenty times faster than the attention reads them.** At a
32768-token history it moves 1.2 GB per rank in 1.741 ms, 693.9 GB/s, and the attention window over the
same rows is 931.228 ms. The probe only indexes; the attention reads the same bytes through the paged
kernel and multiplies them.

## Two kinds of window behind the two components

The two component passes do not price the same quantity, and a reader comparing them with the loop
should know which regime each window caught.

`attn` is device time wherever the attention keeps the device busy for longer than the host takes to
issue its calls, which is every row whose attention exceeds a millisecond: it reads 72.208 ms against a
loop of 71.737 ms for the 8192-token prefill, and 45.677 against 45.578 for the merge step above.

`write` reads two regimes for the same 72 calls. In 583 of the 600 matrix rows its p50 is 1.127 to
1.296 ms, and in the other 17 it is 0.050 to 0.240 ms — for calls whose count and byte count do not
change, 1.127 ms at 128 new tokens and 1.158 ms at 8192 in one regime against 0.101 ms at 2048 tokens
in the other. Rows in the small regime carry both: `extend_pref512_chunk2048_bs4_ps16_random` reads
p50 0.240 ms and p95 1.180 ms in the same pass. The window therefore covers the host's issue of the
calls as well as their execution on the device, which is why the CSV records it as a window and derives
no bandwidth or per-token cost from the write side.

**A component can also be slowed by the machine.** Both component passes and the probe run after the
timed loop, one window per iteration, and a co-tenant that takes the device during one of those passes
shows up in that pass. The signature is a value that disagrees with the same shape at another page size
or layout in the same run: besides the merge row re-run above, the matrix run has 14 more rows where a
component's sum exceeds its own loop by more than a quarter, and the same shape at the neighbouring
page sizes reads clean values. The loop and the components of one row are not a decomposition of each
other, which is why no table here normalises one by the other.

## Reading a row back

`kernel_summary.csv` carries, per step: the branch and the order the step ran in, the ledger
(`kv_bytes_written`, `kv_bytes_read_valid`, `kv_bytes_read_pages`, `gather_rows`, `gather_bytes`,
`attention_pairs`, `attention_flops`), every window's min, p50, p95 and p99, the derived figures, and
the result of the three checks. The min is the smallest window the phase ever took in that row, and on a
shared machine it is the figure closest to what a quiet device gives: the matrix run's largest loop, the
242.374 ms batch-4 prefill of 8192-token sequences, has a p95 0.77 ms above its min.
