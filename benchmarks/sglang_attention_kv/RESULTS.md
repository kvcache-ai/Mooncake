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
are the three windows a step is timed in, in the order a forward pass runs them; `step` is their sum.
`write` and `attn` are the two component passes of section 1 of the README: the step's `set_kv_buffer`
calls in one window, and the branch's attention path for every layer in one window, each run after the
timed loop and never added to the step. `gather` is the read-only probe over the rows the branch's paged
side reads.

`GB/s` divides the bytes the window's own path moves by that window: `read` for the attention window,
the probe's own bytes for `gather`. `FLOP/byte` divides the arithmetic of the step's attention by the
same read bytes.

The ledger behind those two columns is split by the path that reads the KV, which differs per branch.
For the 8192-token steps at page size 64, one rank:

| step | branch | paged read | ragged read | attention read | page capacity |
|---|---|---|---|---|---|
| prefill 8192 | `ragged_no_prefix` | 0 B | 302.0 MB | 302.0 MB | 302.0 MB |
| 512 queries behind 8192 | `ragged_prefix_merge` | 302.0 MB | 18.9 MB | 320.9 MB | 320.9 MB |
| decode at 8193 | `paged_decode` | 302.0 MB | 0 B | 302.0 MB | 304.3 MB |

A merge step reads its 8192-token history through the paged wrapper and its own 512 tokens through the
ragged one. A prefill step reads no paged KV at all, because its queries attend to the K/V it computes,
through the ragged wrapper. A decode step reads its whole context through the paged wrapper, where the
page capacity of 304.3 MB covers the 302.0 MB of valid tokens plus one page of padding.

## The step, page size 64, contiguous pool, one sequence

| step | indices | plan | loop | step | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|
| prefill 128 | 0.129 | 0.151 | 2.428 | 2.708 | 1.139 | 0.984 | 1.146 | 4.1 | 4.8 | 1.2 | 258 |
| prefill 512 | 0.131 | 0.159 | 2.689 | 2.983 | 1.150 | 1.420 | 1.143 | 16.5 | 13.3 | 13.6 | 1026 |
| prefill 2048 | 0.269 | 0.278 | 8.615 | 9.166 | 1.141 | 8.599 | 1.136 | 66.5 | 8.8 | 36.0 | 4098 |
| prefill 8192 | 0.368 | 0.364 | 71.893 | 72.631 | 1.140 | 72.204 | 1.138 | 265.4 | 4.2 | 68.5 | 16386 |
| decode at 129 | 0.344 | 0.215 | 3.290 | 3.853 | 1.155 | 1.635 | 1.160 | 4.1 | 2.9 | 0.0 | 4 |
| decode at 513 | 0.341 | 0.212 | 3.298 | 3.857 | 1.148 | 1.632 | 1.164 | 16.2 | 11.6 | 0.0 | 4 |
| decode at 2049 | 0.339 | 0.215 | 3.288 | 3.843 | 1.148 | 1.616 | 1.142 | 66.1 | 46.7 | 0.2 | 4 |
| decode at 8193 | 0.344 | 0.220 | 3.296 | 3.867 | 1.152 | 1.622 | 1.160 | 260.4 | 186.2 | 0.7 | 4 |

`read GB/s` on the prefill rows divides the bytes of the step's own K/V by a window that does the
step's whole attention over them: the four prefill rows move 4.7, 18.9, 75.5 and 302.0 MB in windows of
0.984, 1.420, 8.599 and 72.204 ms. The decode rows read the same 302.0 MB at 8193 tokens in 1.622 ms.

## The long-context run, page size 64, random layout, one sequence

| step | indices | plan | loop | step | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|
| prefill 16384 | 0.472 | 0.443 | 247.155 | 248.126 | 1.157 | 245.893 | 1.161 | 520.2 | 2.5 | 80.5 | 32770 |
| prefill 32768 | 0.479 | 0.480 | 938.666 | 939.635 | 1.146 | 938.202 | 1.771 | 681.9 | 1.3 | 84.4 | 65538 |
| 512 queries behind 16384 | 0.781 | 0.625 | 21.030 | 22.450 | 1.176 | 21.178 | 1.153 | 523.9 | 29.4 | 59.3 | 2017 |
| 512 queries behind 32768 | 0.795 | 0.630 | 39.387 | 40.823 | 1.183 | 39.539 | 1.787 | 676.0 | 31.0 | 63.1 | 2032 |
| decode at 16385 | 0.348 | 0.220 | 3.398 | 3.965 | 1.178 | 1.654 | 1.158 | 521.6 | 365.3 | 1.5 | 4 |
| decode at 32769 | 0.350 | 0.222 | 3.401 | 3.970 | 1.191 | 1.647 | 1.777 | 679.9 | 733.4 | 2.9 | 4 |

A 32768-token history holds 1096 pages of 64 tokens and 1152 MiB of KV per rank of valid tokens, and a
16384-token history 552 pages and 576 MiB; the capacity is one page more in each case.

## The query-length axis, extend steps in the merge branch

Page size 64, contiguous pool, one sequence.

| step | indices | plan | loop | step | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 128 queries behind 128 | 0.443 | 0.447 | 6.311 | 7.218 | 1.128 | 4.302 | 1.131 | 4.2 | 2.2 | 0.8 | 385 |
| 512 queries behind 128 | 0.432 | 0.456 | 6.634 | 7.531 | 1.144 | 4.604 | 1.132 | 4.2 | 5.1 | 6.3 | 1230 |
| 2048 queries behind 128 | 0.525 | 0.508 | 9.601 | 10.641 | 1.145 | 9.536 | 1.141 | 4.1 | 8.4 | 36.5 | 4339 |
| 128 queries behind 512 | 0.433 | 0.446 | 6.565 | 7.448 | 1.145 | 4.561 | 1.144 | 16.5 | 5.2 | 2.4 | 461 |
| 512 queries behind 512 | 0.439 | 0.465 | 6.780 | 7.683 | 1.138 | 4.713 | 1.146 | 16.5 | 8.0 | 12.3 | 1537 |
| 2048 queries behind 512 | 0.543 | 0.515 | 11.322 | 12.386 | 1.130 | 11.288 | 1.135 | 16.6 | 8.4 | 41.1 | 4917 |
| 128 queries behind 2048 | 0.430 | 0.447 | 6.631 | 7.516 | 1.155 | 4.603 | 1.136 | 66.5 | 17.4 | 8.7 | 497 |
| 512 queries behind 2048 | 0.423 | 0.448 | 6.702 | 7.586 | 1.132 | 4.795 | 1.131 | 66.7 | 19.7 | 36.3 | 1844 |
| 2048 queries behind 2048 | 0.651 | 0.565 | 18.213 | 19.431 | 1.120 | 18.243 | 1.145 | 66.0 | 8.3 | 50.9 | 6145 |
| 128 queries behind 8192 | 0.435 | 0.453 | 6.649 | 7.548 | 1.147 | 4.608 | 1.149 | 262.8 | 66.6 | 33.8 | 508 |
| 512 queries behind 8192 | 0.551 | 0.521 | 11.786 | 12.866 | 1.158 | 11.809 | 1.140 | 264.8 | 27.2 | 54.0 | 1988 |
| 2048 queries behind 8192 | 0.684 | 0.577 | 45.628 | 46.900 | 1.141 | 45.667 | 1.149 | 262.8 | 8.3 | 60.9 | 7373 |

## The other axes

| axis | configuration | loop | step | attn |
|---|---|---|---|---|
| page size, decode at a 8193-token history | 1, 16, 32, 64, 128 | 3.295, 3.271, 3.319, 3.296, 3.181 | 3.850, 3.826, 3.878, 3.867, 3.731 | 1.642, 1.620, 1.626, 1.622, 1.575 |
| layout, decode at a 8193-token history | contiguous, random | 3.296, 3.211 | 3.867, 3.760 | 1.622, 1.608 |
| batch, prefill 8192 | 1 sequence, 4 even, 4 ragged of 8192/4096/2048/1024 | 71.893, 242.339, 84.668 | 72.631, 243.158, 85.429 | 72.204, 241.723, 84.527 |

## What the numbers show

**Query length and history length move the loop.** A prefill of 128 tokens spends 2.428 ms in the loop,
512 tokens 2.689 ms, 2048 tokens 8.615 ms and 8192 tokens 71.893 ms. Behind an 8192-token history the
merge branch's loop reads 6.649 ms for 128 queries, 11.786 ms for 512 and 45.628 ms for 2048; behind a
2048-token history the same three queries read 6.631, 6.702 and 18.213 ms. In the long-context run 512
queries behind 16384 tokens read 21.030 ms and behind 32768 tokens 39.387 ms.

**A decode step's loop stays between 3.175 and 3.325 ms over the 120 decode steps of the matrix run**,
and its attention window between 1.563 and 1.661 ms, while the KV it reads grows from 4.7 MB to 1.2 GB
per rank: at a 8193-token history the paged read covers 302.0 MB in a 1.622 ms window. The whole step
reads 3.731 to 3.924 ms across those steps, and 3.965 and 3.970 ms for the two 16K and 32K histories of
the long-context run.

**The page size and the layout move no window by more than a tenth of a millisecond.** Decode at a
8193-token history reads 3.181 to 3.319 ms in the loop, 3.731 to 3.878 ms in the step and 1.575 to
1.642 ms in the attention window over page sizes 1 to 128, and 3.296 against 3.211 ms in the loop for a
contiguous pool against a churned one. This is what the replay builds: SGLang hands FlashInfer a
token-level CSR stream and plans the wrappers at `page_size=1` whatever the pool's page size is, so the
page size reaches the kernels through the addresses in that stream.

**Batch shape moves the loop.** One 8192-token sequence takes 71.893 ms of loop, four take 242.339 ms,
and four sequences of 8192, 4096, 2048 and 1024 take 84.668 ms, which is the same 8192 tokens under a
causal mask.

**The gather probe moves the same rows in a window of its own.** At a 32768-token history it moves
1.2 GB per rank in 1.771 ms, 681.9 GB/s, while the attention window over the same rows is 938.202 ms
and reads them at 1.3 GB/s. The probe indexes and copies; the attention window reads the same bytes
through the branch's kernels and multiplies them.

## What the component windows are, and what they are not

The write and the attention components are windows of their own, measured after the timed loop, and
they do not decompose it. Three observations from this run's CSV and from an earlier one say where a
reader can and cannot use them.

**They do not add up to the loop.** On the four-sequence 512-token prefill rows the two windows sum to
about 3.5 ms against a loop of 2.46 to 2.52 ms, with the attention window alone (2.255 to 2.383 ms)
already most of the loop. On the 8192-token prefill the attention window (72.204 ms) is above the loop
(71.893 ms), while on the 128-query extend behind 8192 tokens it is 4.608 ms against a loop of
6.649 ms. The loop holds a layer's attention and its KV write as the branch interleaves them; each
component holds one of the two. The CSV records their share of the step as a quotient of two measured
windows, and no table here treats the components as parts that reconstruct the step.

**The write window reads one regime in this run and another in an earlier one.** Every one of the 600
matrix rows reads 0.695 to 1.211 ms here, for steps that write 128 to 32768 tokens, so the window does
not follow the bytes written. An earlier run of the same matrix on the same machine with the same
settings read 0.050 to 0.240 ms in 17 of its 600 rows and 1.127 to 1.296 ms in the rest, and one of
those rows carried both regimes inside its own pass: 0.240 ms at p50 and 1.180 ms at p95. Nothing in
the measurement separates the host's issue of the calls from the device's execution of them, so the CSV
records the write side as the window it is and derives no bandwidth or per-token cost from it.

**The same steps read differently in an earlier run.** In that earlier run five steps' attention
windows read 181.141, 97.579, 85.132, 59.842 and 22.841 ms in rows whose loops were 84.738, 45.618,
39.522, 28.320 and 12.098 ms, and re-measuring those steps read 84.568, 45.677, 39.615, 28.244 and
12.091 ms. This run has no such row. The min column of a window is the smallest sample it took in its
row, which is the figure a slower sample during the run does not move: the largest loops of the matrix,
the batch-4 prefills of 8192-token sequences at 242.3 to 242.5 ms, have a p95 0.73 ms above their min.

## Reading a row back

`kernel_summary.csv` carries, per step: the branch and the order the step ran in, the configuration the
step was built with, the ledger — `kv_bytes_written`, `kv_bytes_paged_read`, `kv_bytes_ragged_read`,
`kv_bytes_attention_read`, `kv_bytes_page_capacity`, `gather_rows`, `gather_bytes`, `attention_pairs`,
`attention_flops` — every window's min, p50, p95 and p99, the derived figures, and the result of the
three checks. A run of another model, another sharding or another branch reports its own row for every
step, and the two sides of every figure in it are recorded there.
