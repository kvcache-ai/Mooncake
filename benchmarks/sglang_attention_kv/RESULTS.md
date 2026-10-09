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

The step is 0.043 to 0.071 ms longer than the sum of its three windows in every one of the 600 matrix
steps, and 0.051 to 0.061 ms longer in the six long-context steps: that is the between-window interval,
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
| prefill 128 | 0.128 | 0.153 | 2.529 | 2.806 | 2.861 | 1.176 | 1.005 | 1.136 | 4.2 | 4.7 | 1.2 | 258 |
| prefill 512 | 0.125 | 0.156 | 2.683 | 2.966 | 3.018 | 1.152 | 1.418 | 1.124 | 16.8 | 13.3 | 13.7 | 1026 |
| prefill 2048 | 0.288 | 0.292 | 8.538 | 9.122 | 9.167 | 1.157 | 8.593 | 1.115 | 67.7 | 8.8 | 36.0 | 4098 |
| prefill 8192 | 0.424 | 0.415 | 71.881 | 72.722 | 72.774 | 1.178 | 72.248 | 1.136 | 265.8 | 4.2 | 68.5 | 16386 |
| decode at 129 | 0.346 | 0.214 | 3.423 | 3.987 | 4.046 | 1.179 | 1.684 | 1.151 | 4.1 | 2.8 | 0.0 | 4 |
| decode at 513 | 0.344 | 0.214 | 3.425 | 3.986 | 4.046 | 1.181 | 1.674 | 1.161 | 16.3 | 11.3 | 0.0 | 4 |
| decode at 2049 | 0.343 | 0.213 | 3.321 | 3.879 | 3.937 | 1.159 | 1.621 | 1.127 | 67.0 | 46.6 | 0.2 | 4 |
| decode at 8193 | 0.338 | 0.214 | 3.366 | 3.917 | 3.975 | 1.170 | 1.652 | 1.149 | 262.8 | 182.8 | 0.7 | 4 |

## The long-context run, page size 64, random layout, one sequence

| step | indices | plan | loop | sum | window | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| prefill 16384 | 0.381 | 0.406 | 247.161 | 247.984 | 248.045 | 1.173 | 245.870 | 1.164 | 519.1 | 2.5 | 80.5 | 32770 |
| prefill 32768 | 0.382 | 0.456 | 938.660 | 939.518 | 939.573 | 1.168 | 938.130 | 1.762 | 685.7 | 1.3 | 84.4 | 65538 |
| 512 queries behind 16384 | 0.672 | 0.565 | 21.006 | 22.248 | 22.299 | 1.175 | 21.116 | 1.152 | 524.4 | 29.5 | 59.5 | 2017 |
| 512 queries behind 32768 | 0.678 | 0.565 | 39.382 | 40.634 | 40.685 | 1.178 | 39.463 | 1.777 | 679.8 | 31.1 | 63.2 | 2032 |
| decode at 16385 | 0.351 | 0.219 | 3.384 | 3.961 | 4.020 | 1.177 | 1.656 | 1.155 | 523.1 | 364.7 | 1.5 | 4 |
| decode at 32769 | 0.348 | 0.220 | 3.390 | 3.963 | 4.021 | 1.186 | 1.659 | 1.770 | 682.5 | 728.0 | 2.9 | 4 |

A 32768-token history is 512 pages of 64 tokens and 1152 MiB of KV per rank, and the step's pool
allocated 1096 pages for it, because the churned layout leaves gaps. A 16384-token history is 256 pages
and 576 MiB, out of 552 allocated.

## The query-length axis, extend steps in the merge branch

Page size 64, contiguous pool, one sequence.

| step | indices | plan | loop | sum | window | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 128 queries behind 128 | 0.457 | 0.465 | 6.524 | 7.462 | 7.522 | 1.168 | 4.521 | 1.127 | 4.2 | 2.1 | 0.8 | 385 |
| 512 queries behind 128 | 0.446 | 0.469 | 6.658 | 7.575 | 7.638 | 1.154 | 4.615 | 1.126 | 4.2 | 5.1 | 6.3 | 1230 |
| 2048 queries behind 128 | 0.569 | 0.554 | 9.593 | 10.721 | 10.767 | 1.157 | 9.542 | 1.139 | 4.1 | 8.4 | 36.5 | 4339 |
| 128 queries behind 512 | 0.443 | 0.460 | 6.566 | 7.478 | 7.538 | 1.154 | 4.567 | 1.132 | 16.7 | 5.2 | 2.4 | 461 |
| 512 queries behind 512 | 0.477 | 0.502 | 7.080 | 8.068 | 8.132 | 1.189 | 4.986 | 1.142 | 16.5 | 7.6 | 11.6 | 1537 |
| 2048 queries behind 512 | 0.591 | 0.564 | 11.311 | 12.471 | 12.519 | 1.176 | 11.280 | 1.145 | 16.5 | 8.4 | 41.1 | 4917 |
| 128 queries behind 2048 | 0.471 | 0.475 | 6.911 | 7.863 | 7.924 | 1.181 | 4.759 | 1.124 | 67.2 | 16.9 | 8.4 | 497 |
| 512 queries behind 2048 | 0.480 | 0.504 | 7.109 | 8.103 | 8.174 | 1.182 | 5.013 | 1.136 | 66.5 | 18.8 | 34.7 | 1844 |
| 2048 queries behind 2048 | 0.715 | 0.625 | 18.188 | 19.534 | 19.585 | 1.184 | 18.261 | 1.145 | 65.9 | 8.3 | 50.8 | 6145 |
| 128 queries behind 8192 | 0.455 | 0.469 | 6.646 | 7.587 | 7.647 | 1.153 | 4.644 | 1.144 | 264.0 | 66.0 | 33.6 | 508 |
| 512 queries behind 8192 | 0.597 | 0.566 | 11.781 | 12.951 | 13.000 | 1.178 | 11.795 | 1.150 | 262.5 | 27.2 | 54.1 | 1988 |
| 2048 queries behind 8192 | 0.763 | 0.642 | 45.564 | 46.983 | 47.035 | 1.169 | 45.626 | 1.144 | 264.1 | 8.3 | 61.0 | 7373 |

## The other axes

| axis | configuration | loop | window | attn |
|---|---|---|---|---|
| page size, decode at a 8193-token history | 1, 16, 32, 64, 128 | 3.303, 3.298, 3.339, 3.366, 3.329 | 3.916, 3.922, 3.947, 3.975, 3.941 | 1.618, 1.612, 1.637, 1.652, 1.640 |
| layout, decode at a 8193-token history | contiguous, random | 3.366, 3.362 | 3.975, 3.975 | 1.652, 1.634 |
| batch, prefill 8192 | 1 sequence, 4 even, 4 ragged of 8192/4096/2048/1024 | 71.881, 242.476, 84.713 | 72.774, 243.476, 85.631 | 72.248, 241.937, 84.588 |

## What the numbers show

**Query length and history length move the loop.** A prefill of 128 tokens spends 2.529 ms in the loop,
512 tokens 2.683 ms, 2048 tokens 8.538 ms and 8192 tokens 71.881 ms. Behind an 8192-token history the
merge branch's loop reads 6.646 ms for 128 queries, 11.781 ms for 512 and 45.564 ms for 2048; behind a
2048-token history the same three queries read 6.911, 7.109 and 18.188 ms. In the long-context run 512
queries behind 16384 tokens read 21.006 ms and behind 32768 tokens 39.382 ms.

**A decode step's loop stays between 3.275 and 3.462 ms over the 122 decode steps of the two runs**,
its whole-step window between 3.899 and 4.108 ms and its attention window between 1.607 and 1.699 ms,
while the KV it reads grows from 4.7 MB to 1.2 GB per rank: at a 8193-token history the paged read
covers 302.0 MB in a 1.652 ms window.

**The page size and the layout move no window by more than a tenth of a millisecond.** Decode at a
8193-token history reads 3.298 to 3.366 ms in the loop, 3.916 to 3.975 ms in the whole-step window and
1.612 to 1.652 ms in the attention window over page sizes 1 to 128, and 3.366 against 3.362 ms in the
loop for a contiguous pool against a churned one. This is what the replay builds: SGLang hands
FlashInfer a token-level CSR stream and plans the wrappers at `page_size=1` whatever the pool's page
size is, so the page size reaches the kernels through the addresses in that stream.

**Batch shape moves the loop with the tokens and the pairs the batch computes.** One 8192-token sequence
computes 8192 tokens and 33,558,528 query-key pairs in a 71.881 ms loop; four of them compute 32768
tokens and 134,234,112 pairs in 242.476 ms; four ragged sequences of 8192, 4096, 2048 and 1024 compute
15360 tokens and 44,572,160 pairs in 84.713 ms.

**The gather probe prices a copy of the rows the branch's paged side reads, and those are not always
the rows the attention reads.** On the merge step behind 32768 tokens the probe moves the 32768 rows of
paged history, both K and V, in 1.777 ms, 679.8 GB/s, while the attention window over that history is
39.463 ms. On a prefill step, which reads no paged KV, the probe moves the rows the step wrote instead,
and the attention reads the step's own K and V tensors: the 32768-token prefill moves 1.2 GB per rank in
1.762 ms, 685.7 GB/s, in a run whose attention window is 938.130 ms.

## What the component windows are, and what they are not

The write and the attention components are windows of their own, measured after the timed loop, and
they do not decompose it.

They do not add up to the loop. On the four-sequence 512-token prefill rows the two windows sum to
about 3.5 ms against a loop of 2.44 to 2.51 ms, with the attention window alone (2.254 to 2.384 ms)
already most of the loop. On the 8192-token prefill the attention window (72.248 ms) is above the loop
(71.881 ms), while on the 128-query extend behind 8192 tokens it is 4.644 ms against a loop of
6.646 ms. The loop holds a layer's attention and its KV write as the branch interleaves them; each
component holds one of the two, and the merge branch's path is two wrapper calls and one `merge_state`
per layer rather than one call. The CSV records their ratio to `phase_sum`, as a ratio of two measured
windows, and no table here treats the components as parts that reconstruct the step.

The write window is reported as a window rather than as a rate. It reads 1.137 to 1.204 ms for steps
that write 1 to 32768 tokens, so it does not follow the bytes written, and inside a single row its p95
sits 0.086 ms above its min at the median and 0.144 ms above it at most. Nothing in the measurement
separates the host's issue of the calls from the device's execution of them, which is why the CSV
derives no bandwidth and no per-token cost from the write side.

Each step's record in `kernel_summary.csv` carries the values behind every statement here: each
window's min, p50, p95 and p99, the ledger, the derived figures and the three check results.

## Reading a row back

`kernel_summary.csv` carries, per step: the branch and the order the step ran in, the four settings the
replay fixes (`attention_backend`, `wrapper_page_size`, `decode_use_tensor_cores`, `kv_write_stream`),
the ledger — `kv_bytes_written`, `kv_bytes_paged_read`, `kv_bytes_ragged_read`,
`kv_bytes_attention_read`, `kv_bytes_page_capacity`, `gather_rows`, `gather_bytes`, `attention_pairs`,
`attention_flops` — every window's min, p50, p95 and p99, the derived figures, and the result of the
three checks. The min of a window is the smallest sample it took in its row, which is the figure a
slower sample during the run does not move: the largest loops of the matrix, the batch-4 prefills of
8192-token sequences at 242.34 to 242.49 ms, have a p95 0.70 to 0.93 ms above their min.
