# One validation run

Numbers from one run of the full step matrix (600 steps) and one long-context run (6 steps) on the
machine below, every step passing its three checks. The result files of both runs — the summary CSVs,
the manifests and the step matrix — are published at
<https://gist.github.com/CAICAIIs/ef191bfb97db681926d3b560b16464e6>, with the host name and the local
paths of the manifest replaced by placeholders. They bound the example figures in this repository to
that machine and those versions; they are not a precondition of the benchmark, and a run elsewhere
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

The step is 0.029 to 0.062 ms longer than the sum of its three windows in every one of the 600 matrix
steps, and 0.045 to 0.057 ms longer in the six long-context steps: that is the between-window interval,
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
| prefill 128 | 0.014 | 0.166 | 2.509 | 2.684 | 2.737 | 1.161 | 0.993 | 1.153 | 4.1 | 4.8 | 1.2 | 258 |
| prefill 512 | 0.014 | 0.172 | 2.741 | 2.923 | 2.975 | 1.162 | 1.422 | 1.143 | 16.5 | 13.3 | 13.6 | 1026 |
| prefill 2048 | 0.018 | 0.358 | 8.540 | 8.917 | 8.958 | 1.176 | 8.602 | 1.140 | 66.2 | 8.8 | 36.0 | 4098 |
| prefill 8192 | 0.021 | 0.502 | 71.912 | 72.433 | 72.485 | 1.170 | 72.225 | 1.144 | 264.0 | 4.2 | 68.5 | 16386 |
| decode at 129 | 0.076 | 0.225 | 3.312 | 3.614 | 3.668 | 1.154 | 1.626 | 1.165 | 4.1 | 2.9 | 0.0 | 4 |
| decode at 513 | 0.076 | 0.223 | 3.337 | 3.639 | 3.690 | 1.159 | 1.633 | 1.162 | 16.3 | 11.6 | 0.0 | 4 |
| decode at 2049 | 0.078 | 0.229 | 3.378 | 3.684 | 3.736 | 1.179 | 1.669 | 1.142 | 66.1 | 45.3 | 0.2 | 4 |
| decode at 8193 | 0.077 | 0.228 | 3.378 | 3.683 | 3.736 | 1.179 | 1.668 | 1.166 | 259.1 | 181.1 | 0.7 | 4 |

The `indices` window is 0.014 to 0.078 ms on these rows: the metadata buffers are held from the case
and the window is SGLang's kernel filling the CSR stream, which is what a server's index stage does on
a warmed step.

## The long-context run, page size 64, random layout, one sequence

| step | indices | plan | loop | sum | window | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| prefill 16384 | 0.024 | 0.553 | 247.201 | 247.805 | 247.856 | 1.112 | 245.863 | 1.146 | 527.2 | 2.5 | 80.5 | 32770 |
| prefill 32768 | 0.023 | 0.607 | 938.698 | 939.342 | 939.392 | 1.144 | 938.133 | 1.767 | 683.7 | 1.3 | 84.4 | 65538 |
| 512 queries behind 16384 | 0.201 | 0.714 | 21.063 | 21.982 | 22.032 | 1.125 | 21.134 | 1.139 | 530.3 | 29.5 | 59.4 | 2017 |
| 512 queries behind 32768 | 0.213 | 0.719 | 39.436 | 40.374 | 40.419 | 1.119 | 39.503 | 1.786 | 676.4 | 31.1 | 63.1 | 2032 |
| decode at 16385 | 0.079 | 0.226 | 3.235 | 3.549 | 3.606 | 1.120 | 1.593 | 1.139 | 530.3 | 379.1 | 1.5 | 4 |
| decode at 32769 | 0.088 | 0.224 | 3.226 | 3.537 | 3.584 | 1.131 | 1.576 | 1.765 | 684.5 | 766.6 | 3.1 | 4 |

A 32768-token history is 512 pages of 64 tokens and 1152 MiB of KV per rank, and the step's pool
allocated 1096 pages for it, because the churned layout leaves gaps. A 16384-token history is 256 pages
and 576 MiB, out of 552 allocated.

## The query-length axis, extend steps in the merge branch

Page size 64, contiguous pool, one sequence.

| step | indices | plan | loop | sum | window | write | attn | gather | gather GB/s | read GB/s | TFLOP/s | FLOP/byte |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 128 queries behind 128 | 0.131 | 0.492 | 6.574 | 7.207 | 7.268 | 1.167 | 4.508 | 1.155 | 4.1 | 2.1 | 0.8 | 385 |
| 512 queries behind 128 | 0.127 | 0.505 | 6.758 | 7.396 | 7.452 | 1.171 | 4.708 | 1.151 | 4.1 | 5.0 | 6.2 | 1230 |
| 2048 queries behind 128 | 0.153 | 0.598 | 9.602 | 10.359 | 10.405 | 1.180 | 9.553 | 1.159 | 4.1 | 8.4 | 36.4 | 4339 |
| 128 queries behind 512 | 0.130 | 0.493 | 6.744 | 7.387 | 7.442 | 1.169 | 4.698 | 1.160 | 16.3 | 5.0 | 2.3 | 461 |
| 512 queries behind 512 | 0.131 | 0.518 | 6.938 | 7.594 | 7.651 | 1.173 | 4.868 | 1.158 | 16.3 | 7.8 | 11.9 | 1537 |
| 2048 queries behind 512 | 0.157 | 0.608 | 11.309 | 12.077 | 12.122 | 1.169 | 11.299 | 1.154 | 16.3 | 8.4 | 41.1 | 4917 |
| 128 queries behind 2048 | 0.126 | 0.491 | 6.625 | 7.252 | 7.309 | 1.143 | 4.576 | 1.134 | 66.6 | 17.5 | 8.7 | 497 |
| 512 queries behind 2048 | 0.130 | 0.521 | 6.957 | 7.616 | 7.672 | 1.168 | 4.863 | 1.142 | 66.1 | 19.4 | 35.8 | 1844 |
| 2048 queries behind 2048 | 0.189 | 0.715 | 18.198 | 19.105 | 19.152 | 1.158 | 18.243 | 1.157 | 65.3 | 8.3 | 50.9 | 6145 |
| 128 queries behind 8192 | 0.127 | 0.491 | 6.686 | 7.310 | 7.368 | 1.161 | 4.639 | 1.161 | 260.2 | 66.1 | 33.6 | 508 |
| 512 queries behind 8192 | 0.158 | 0.606 | 11.794 | 12.559 | 12.604 | 1.135 | 11.818 | 1.149 | 262.9 | 27.1 | 54.0 | 1988 |
| 2048 queries behind 8192 | 0.203 | 0.763 | 45.580 | 46.548 | 46.597 | 1.174 | 45.622 | 1.155 | 261.5 | 8.3 | 61.0 | 7373 |

## The other axes

| axis | configuration | loop | window | attn |
|---|---|---|---|---|
| page size, decode at a 8193-token history | 1, 16, 32, 64, 128 | 3.355, 3.353, 3.393, 3.378, 3.375 | 3.714, 3.714, 3.762, 3.736, 3.740 | 1.658, 1.659, 1.661, 1.668, 1.660 |
| layout, decode at a 8193-token history | contiguous, random | 3.378, 3.366 | 3.736, 3.730 | 1.668, 1.663 |
| batch, prefill 8192 | 1 sequence, 4 even, 4 ragged of 8192/4096/2048/1024 | 71.912, 242.376, 84.716 | 72.485, 243.050, 85.328 | 72.225, 241.742, 84.537 |

The page size also selects the allocator: the page size 1 rows of this run used SGLang's token
allocator and the rows above it the paged one, which the `slot_allocator` column of the CSV states.

## What the numbers show

**Query length and history length move the loop.** A prefill of 128 tokens spends 2.509 ms in the loop,
512 tokens 2.741 ms, 2048 tokens 8.540 ms and 8192 tokens 71.912 ms. Behind an 8192-token history the
merge branch's loop reads 6.686 ms for 128 queries, 11.794 ms for 512 and 45.580 ms for 2048; behind a
2048-token history the same three queries read 6.625, 6.957 and 18.198 ms. In the long-context run 512
queries behind 16384 tokens read 21.063 ms and behind 32768 tokens 39.436 ms.

**A decode step's loop stays between 3.226 and 3.423 ms over the 122 decode steps of the two runs**,
its whole-step window between 3.584 and 3.812 ms and its attention window between 1.576 and 1.681 ms,
while the KV it reads grows from 4.7 MB to 1.2 GB per rank: at a 8193-token history the paged read
covers 302.0 MB in a 1.668 ms window.

**The page size and the layout move no window by more than a tenth of a millisecond.** Decode at a
8193-token history reads 3.353 to 3.393 ms in the loop, 3.714 to 3.762 ms in the whole-step window and
1.658 to 1.668 ms in the attention window over page sizes 1 to 128, and 3.378 against 3.366 ms in the
loop for a contiguous pool against a churned one. This is what the replay builds: SGLang hands
FlashInfer a token-level CSR stream and plans the wrappers at `page_size=1` whatever the pool's page
size is, so the pool's page size reaches the kernels through the addresses in that stream.

**Batch shape moves the loop with the tokens and the pairs the batch computes.** One 8192-token sequence
computes 8192 tokens and 33,558,528 query-key pairs in a 71.912 ms loop; four of them compute 32768
tokens and 134,234,112 pairs in 242.376 ms; four ragged sequences of 8192, 4096, 2048 and 1024 compute
15360 tokens and 44,572,160 pairs in 84.716 ms.

**The gather probe prices a copy of the rows the branch's paged side reads, and those are not always
the rows the attention reads.** On the merge step behind 32768 tokens the probe moves the 32768 rows of
paged history, both K and V, in 1.786 ms, 676.4 GB/s, while the attention window over that history is
39.503 ms. On a prefill step, which reads no paged KV, the probe moves the rows the step wrote instead,
and the attention reads the step's own K and V tensors: the 32768-token prefill moves 1.2 GB per rank in
1.767 ms, 683.7 GB/s, in a run whose attention window is 938.133 ms.

## What the component windows are, and what they are not

The write and the attention components are windows of their own, measured after the timed loop, and
they do not decompose it.

They do not add up to the loop. On the four-sequence 512-token prefill rows the two windows sum to
about 3.6 ms against a loop of 2.52 to 2.56 ms, with the attention window alone (2.251 to 2.385 ms)
already most of the loop. On the 8192-token prefill the attention window (72.225 ms) is above the loop
(71.912 ms), while on the 128-query extend behind 8192 tokens it is 4.639 ms against a loop of
6.686 ms. The loop holds a layer's attention and its KV write as the branch interleaves them; each
component holds one of the two, and the merge branch's path is two wrapper calls and one `merge_state`
per layer rather than one call. The CSV records their ratio to `phase_sum`, as a ratio of two measured
windows, and no table here treats the components as parts that reconstruct the step.

The write window is reported as a window rather than as a rate. It reads 0.694 to 1.191 ms for steps
that write 1 to 32768 tokens, so it does not follow the bytes written, and inside a single row its p95
sits 0.092 ms above its min at the median and 0.192 ms above it at most. Nothing in the measurement
separates the host's issue of the calls from the device's execution of them, which is why the CSV
derives no bandwidth and no per-token cost from the write side.

## Reading a row back

`kernel_summary.csv` carries, per step: the branch and the order the step ran in, the four settings the
replay fixes (`attention_backend`, `wrapper_page_size`, `decode_use_tensor_cores`, `kv_write_stream`),
the allocator the pool's page size selects (`slot_allocator`), the ledger — `kv_bytes_written`,
`kv_bytes_paged_read`, `kv_bytes_ragged_read`, `kv_bytes_attention_read`, `kv_bytes_page_capacity`,
`gather_rows`, `gather_bytes`, `attention_pairs`, `attention_flops` — every window's min, p50, p95 and
p99, the derived figures, and the result of the three checks. The min of a window is the smallest
sample it took in its row, which is the figure a slower sample during the run does not move: the largest
loops of the matrix, the batch-4 prefills of 8192-token sequences at 242.32 to 242.49 ms, have a p95
0.62 to 0.86 ms above their min. The two runs' files, including the CSVs these tables come from, are at
<https://gist.github.com/CAICAIIs/ef191bfb97db681926d3b560b16464e6>.
