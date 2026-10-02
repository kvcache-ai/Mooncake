# Attention -> KV cache access benchmark

This benchmark measures two independent things and always reports them separately:

1. **Attention kernel and KV cache access cost**: how long attention takes to write newly generated
   K/V into the paged KV cache, how long paged attention takes to read historical KV back through
   the block/page table, and how long the index mapping takes.
2. **End-to-end benefit of Mooncake KV transfer and HiCache reuse**: the difference in TTFT, ITL,
   TPOT, end-to-end latency, and token throughput across three cache tiers, GPU-only, GPU+host, and
   GPU+host+Mooncake.

Raw bandwidth of Transfer Engine alone cannot characterize attention performance, and the reverse
holds as well. This benchmark therefore never mixes numbers from the two categories into one table
and never infers one category from the other.

## 1. What is measured

### 1.1 Kernel phase

`kernel_bench.py` constructs every structure that a single forward pass requires:

- multi-layer paged KV cache with shape
  `[num_layers, num_pages, page_size, num_kv_heads, head_dim]` (NHD layout);
- page table: the mapping from the logical pages of each sequence to physical pages, with physical
  page numbers randomly permuted so the access pattern does not degenerate into sequential access;
- slot mapping: the mapping from each new token to its element offset inside the flattened cache;
- seq lens, `paged_kv_indptr`, `paged_kv_indices`, `paged_kv_last_page_len`, `qo_indptr`;
- page size, number of layers, number of KV heads, and head_dim all come from the model's real
  `config.json`.

Four stages are measured, plus one aggregate:

| Stage | Content | Real kernel used |
|---|---|---|
| `metadata` | construction of the page table, slot mapping, and paged KV prefix sums and indices, plus the H2D copy | no kernel, CPU construction + `Tensor.to` |
| `kv_write` | write newly generated K/V into the KV cache according to the page table and positions | `flashinfer.append_paged_kv_cache` |
| `kv_gather` | fetch the whole historical KV by the row indices given by the page table, moving data without computing | `Tensor.index_select` |
| `attention` | paged attention reads historical KV and completes the computation | `flashinfer.BatchPrefillWithPagedKVCacheWrapper` / `BatchDecodeWithPagedKVCacheWrapper` (`fa2` backend) |
| `attention_plan` | the backend compiles the page table into a kernel schedule | `Batch*Wrapper.plan` |
| `total_step` | sum of `metadata`, `kv_write`, `attention_plan`, and `attention` | — |

`total_step` is the attention step, which is what a forward pass runs, so the token throughput and
per-token figures derived from it describe a step rather than a step plus a probe. The H2D copy of
the page table tensors happens inside the window that times `metadata`, so it is reported as the
`metadata_h2d` breakdown of that phase and is never added to it.

`kv_gather` is a read-only probe, not part of the step: it copies every layer's KV through the page
table to price the read path on its own. It is compared against the step and never added to it, so
it does not appear in `total_step`. It gives the upper bound of the read path when it only moves
data; `attention` includes both the KV read and the computation, so the difference between the two
shows whether attention is bound by memory bandwidth or by computation.

Prefill and decode are measured separately: prefill writes and reads the whole sequence, while
decode appends a single token at the end but reads the whole context.

Timing uses CUDA events, and the warmup and timed iteration counts are controlled by
`--kernel-warmup` (default 10) and `--kernel-timed` (default 100). The output includes p50/p95/p99,
tokens/s, effective GB/s, and per-token latency.

Correctness checks run at every measurement point:

- `scatter`: after a short sequence is written, it is read back through the slot mapping; the
  maximum element-wise absolute difference for K and V must be 0, and slots that were never written
  must remain 0;
- `attention`: compared against a plain tensor reference implementation over the same page table,
  the maximum relative error must be below 1e-2.

**A synthetic kernel benchmark is not equivalent to end-to-end serving.** It gives the physical cost
of KV cache access within a single forward pass, without queueing, batching, scheduling, or cache
lookup. The end-to-end benefit can only come from the second category of measurement.

### 1.2 End-to-end phase

`e2e.py` acts only as client and launcher and does not modify the SGLang server implementation. It
starts the server per cache tier and sends streaming requests through SGLang's native `/generate`
endpoint. Requests pass `input_ids` directly, so the input length is exactly controllable and hit
patterns can be constructed by token count.

The real flow corresponding to the SGLang path:

1. the scheduler matches existing prefixes against radix/HiRadixTree metadata;
2. the request is mapped to physical tokens and pages through `ReqToTokenPool`;
3. hierarchical storage: GPU is L1, host memory is L2, Mooncake Store is L3;
4. on an L3 hit, data is first fetched back to L2 and then moved from L2 to the GPU;
5. new K/V is written into `TokenToKVPool`;
6. FlashInfer/FlashAttention reads historical KV through paged KV metadata;
7. later requests reuse the prefix, or hit partial hit and miss;
8. data is written back to L2/L3 according to the write policy and reclaimed according to the
   eviction policy.

The vLLM path serves as a reference implementation to check against (this benchmark does not enable
vLLM end-to-end measurement):

1. the scheduler computes locally computed tokens and external KV tokens
   (`get_num_new_matched_tokens`);
2. `block_table`, `slot_mapping`, and `seq_lens` describe the KV blocks;
3. the connector uses `num_external_tokens` to distinguish full hit, partial hit, and miss;
4. only blocks that were not computed are transferred (`_build_transfer_params` slices by
   `local_block_ids[-num_remote_blocks:]`);
5. the connector organizes src/dst/length by layer and block (`group_concurrent_contiguous` merges
   contiguous segments);
6. the attention backend consumes these blocks through the same paged KV cache.

`--backend` currently accepts only `sglang`. vLLM end-to-end measurement requires a matching version
of the connector and of the Mooncake Python bindings, which have not been verified in the
environment used for the measurements in this repository, so it is not supported and no vLLM
results are produced.

## 2. Model and KV byte conversion

`config.py` reads the number of layers, the number of query heads, the number of KV heads, head_dim,
and dtype from the model's real `config.json`, with nothing hardcoded. Standard GQA/MHA uses:

```
kv_bytes_per_token = num_layers * 2 * num_kv_heads * head_dim * dtype_bytes
```

Taking `Qwen/Qwen3-8B` as an example (36 layers, 32 query heads, 8 KV heads, head_dim=128, bf16):

```
36 * 2 * 8 * 128 * 2 = 147456 bytes/token
```

Under tensor parallelism sharding, two figures are recorded at the same time:

- aggregate: bytes of a single token across every KV head of every layer, still `147456` at TP=8;
- per-rank: `147456 / tp_size`, which is `18432` at TP=8.

Page object bytes equal per-token bytes multiplied by page size. At TP=8 and page_size=64 the
aggregate is `147456 * 64 = 9437184` bytes and the per-rank value is `18432 * 64 = 1179648` bytes.

When `num_kv_heads` or `num_attention_heads` is not divisible by `tp_size`, when the dtype is not
recognized, or when the model uses MLA (`kv_lora_rank` / `qk_rope_head_dim`), the benchmark fails
immediately at the stage where the config is read. Both head counts must divide evenly by `tp_size`:
checking only the KV heads lets the integer division of query heads silently drop heads, and the
measurement then describes a shape that does not exist. MLA's latent KV layout differs from standard
GQA, and mixing the two makes the byte counts meaningless.

The dtype of the kernel phase comes from the model config rather than being hardcoded. The attention
backends support only bfloat16 and float16; when the model declares a precision such as fp8, the
benchmark fails at the stage where measurement points are generated, rather than computing byte
counts and throughput with a dtype that does not match reality.

### Two figures for bytes read

Paged reads work on whole pages, so a last page that is not full is read together with its padding.
Two byte counts are therefore recorded:

- `kv_bytes_read_valid`: bytes of valid tokens, used for per-token figures such as per-token
  latency;
- `kv_bytes_read_moved`: bytes the kernel actually moves, including the padding of the last page.

Bandwidth and arithmetic intensity use `kv_bytes_read_moved`. When the sequence length is not a
multiple of the page size the two figures diverge, and computing bandwidth from valid token counts
skews the result. Records also include `padding_tokens_read` so the size of the divergence can be
checked.

## 3. Cache tiers and hit patterns

Cache tiers (`--tiers`):

| Value | Meaning | SGLang launch flags |
|---|---|---|
| `gpu_only` | KV cache in GPU memory only | no HiCache flags |
| `host` | GPU + host memory | `--enable-hierarchical-cache --hicache-write-policy write_through --hicache-ratio 2` |
| `mooncake` | GPU + host memory + Mooncake Store | the previous row plus `--hicache-storage-backend mooncake --hicache-storage-prefetch-policy wait_complete` |

Hit patterns (`--patterns`):

| Value | How it is constructed | Expected observation |
|---|---|---|
| `cold_miss` | every request uses a random token sequence that is independent of the others and different on every repeat | L1/L2/L3 all miss, highest TTFT |
| `full_hit` | all requests share one prefix, with one warmup request writing that prefix into the cache | TTFT of the measured requests drops markedly, hit rate close to 1 |
| `partial_hit` | the first half of the input is shared, the second half differs per request | hit rate around 0.5, TTFT between cold miss and full hit |
| `multiturn` | the input of each round is the previous round's input concatenated with the previous round's output | the prefix grows with the number of rounds, and hit rate and TTFT vary with the round count |

The warmup of `full_hit` and `partial_hit` sends the shared prefix alone, never one of the measured
requests. Sending a whole request would also cache that request's own suffix, which for `partial_hit`
would turn one measured request into a full hit and lift the measured hit rate above the half the
pattern stands for.

The combinations of these three hit patterns with the three cache tiers, plus multi-turn, cover all
seven end-to-end configurations in the requirements.

**A tier only shows its own benefit when the pools below it cannot serve the reuse.** All three tiers
serve the same reuse while the working set fits in the L1 pool, and the extra tiers then only add
overhead; the report marks, per row, whether each pool can hold the working set of that round, and
states which tier served the tokens. The default matrix fits in L1 on this machine at 8192-token
inputs, so it separates the tiers only from 32768 tokens upwards (`--input-lens 32768`), where the
working set also exceeds the L2 pool at `--hicache-ratio 1`.

Push the working set past the L2 pool as well and only L3 can serve the reuse. Measured on this
machine with `--input-lens 32768 --requests 20 --rounds 3 --hicache-ratio 1` (working set 655360
tokens against an L1 pool of 170112 and an L2 pool of 170112), **the storage backend served no token
at all**: rounds 1 and 2 reported zero cached tokens from every tier and recomputed the whole
prefill, while the store received about 3.9 million tokens of new KV in each round. The same
deployment with `--hicache-ratio 8` serves the reuse from L2 and never touches L3. The report states
this per run rather than leaving it to be inferred, and no L3 benefit is claimed on the basis of a
`host` versus `mooncake` difference.

## 4. Cache cleanup discipline

A return value of 0 from `store.remove_all()` does not mean the objects are gone; using it for
cleanup makes every run after the first measure the "object already exists" path. This benchmark
does not treat `remove_all()` as a cleanup mechanism. Instead:

- every repeat uses its own random prefix, derived from `run_id`, the workload point, and the repeat
  index together, so a cold start is a genuine cold start;
- at the end of every workload point, SGLang's `/flush_cache` is called once to clear the local
  prefix cache. If that call fails the process exits with a non-zero code: leftover local cache
  makes later cold miss and eviction results incomparable;
- for the Mooncake tier, `mooncake_master` is restarted at the start of the measurement of each
  cache tier, so L3 state does not carry over between tiers;
- switching cache tiers rebuilds the server process, so configurations do not contaminate each
  other.

The `/flush_cache` response states that the operation is not performed while requests are running or
queued. This benchmark guarantees that no request is in flight at each call, but that semantics
means the call alone is not enough to guarantee isolation, so independent random prefixes are the
primary means of isolation.

## 5. Command line

```bash
python -m benchmarks.attention_kv --help
python -m benchmarks.attention_kv --full --dry-run
python -m benchmarks.attention_kv --quick \
    --model /mnt/afs/models/Qwen3-8B \
    --tp-size 8 --page-size 64 \
    --result-dir artifacts/attention-kv/quick
```

`--dry-run` only prints the full workload matrix; it starts no server and allocates no GPU memory,
so it also runs on a machine without a GPU.

Main arguments:

| Argument | Description |
|---|---|
| `--quick` / `--full` | minimal workload / full workload matrix, exactly one of the two must be given |
| `--dry-run` | print the workload matrix and exit |
| `--model` | model directory, must contain `config.json` |
| `--backend` | framework used for the end-to-end phase, currently only `sglang` |
| `--tp-size` | tensor parallel size; KV head count and query head count are sharded by it; when omitted it is taken automatically from the number of visible GPUs on the machine |
| `--page-size` | number of tokens per page in paged attention |
| `--input-lens` | override the input length list |
| `--output-len` | override the output length |
| `--requests` | number of requests per workload point |
| `--concurrency` | override the concurrency list |
| `--repeats` | number of repeats, at least 3 for final results |
| `--rounds` | number of multi-turn rounds |
| `--seed` | random seed |
| `--tiers` / `--patterns` | measure only the given cache tiers or hit patterns |
| `--skip-kernel` / `--skip-e2e` | run only one of the two phases |
| `--result-dir` | result directory |
| `--mem-fraction-static` | SGLang static memory fraction, must be lowered under memory pressure |
| `--mooncake-master` | path to the `mooncake_master` executable |
| `--store-port` / `--store-segment-bytes` | port and segment size of the local Mooncake master |
| `--kernel-warmup` / `--kernel-timed` | warmup and timed iteration counts of the kernel phase |

## 6. Workload matrix

The matrix that `--full` expands to:

- input length: 128, 512, 2048, 8192;
- output length: 1, 128;
- requests: 20;
- max concurrency: 1, 4;
- cache tiers: `gpu_only`, `host`, `mooncake`;
- hit patterns: `cold_miss`, `full_hit`, `partial_hit`, `multiturn` (rounds=5);
- seed: 42;
- warmup: 1;
- repeats: 3.

When memory and runtime allow, add 16384 and 32768 with `--input-lens`. `--quick` uses the two
lengths 128 and 512, 1 output length, 4 requests, and 1 repeat, in order to verify connectivity
first.

## 7. Metrics

For every (configuration, workload point, repeat), the end-to-end phase records:

- p50/p95/p99 of TTFT, ITL, TPOT, and end-to-end latency;
- input token throughput, output token throughput, and total token throughput;
- cache hit rate, hit/miss tokens, hit/miss pages;
- which tier served the reuse, in tokens per tier, from the per-request breakdown the server reports;
- the L1 and L2 pool sizes, the working set of the round, and whether each pool can hold it;
- KV bytes per token and total KV bytes for the prompt;
- L2 to GPU load-back time and bandwidth, GPU to L2 backup time and bandwidth, device eviction time,
  L3 tokens written, and L3 prefetch hit tokens;
- number of errors, timeouts, and failed requests.

hit/miss pages are converted from hit/miss tokens and the page size, and the report marks them as
derived values with `pages_source = derived_from_tokens`.

Every bandwidth divides the bytes of a window by the duration of that same window, so numerator and
denominator cover the same transfers. All of these are window totals: a load-back prefetch can serve
a later round, so the report derives no per-request share of TTFT from them.

### Breakdown metrics that cannot be obtained

When SGLang does not expose a breakdown metric at runtime, the report outputs `unavailable` and makes
no guess. What SGLang 0.5.20 does expose, and what it does not:

| Quantity | Metric | Available |
|---|---|---|
| L2 to GPU load-back time | `sglang:load_back_duration_seconds` with `sglang:load_back_bytes_total` for the bandwidth | yes |
| GPU to L2 backup time | `sglang:hicache_backup_duration_seconds` with `sglang:hicache_backup_bytes_total` | yes |
| device eviction time | `sglang:eviction_duration_seconds` | yes |
| L3 tokens written | `sglang:backuped_tokens_total` | token count only |
| L3 prefetch hit tokens | `sglang:storage_prefetch_hit_tokens_total` | token count only |
| L3 fetch time | — | no duration is exposed anywhere; timing would have to be added around the batch read of MooncakeStore |
| prefix lookup time | — | no histogram of the HiCache radix match; timing would have to be added to the prefix matching path in the scheduler |

`report.py` matches these quantities only by **complete base metric name** and, once matched,
converts according to the declared unit (Prometheus histogram `_sum` values are in seconds and the
report converts everything to milliseconds). Substring matching would mix in metrics with entirely
different semantics: `hicache_backup_duration_seconds` times the GPU to host DRAM copy and is not
the L3 write-back, and `storage_prefetch_hit_tokens_total` is a token count and not a duration.
Because no L3 duration exists, the report carries L3 token counts and derives no L3 bandwidth or
latency from them. When a metric family does not exist the value is `unavailable`, and when the
family exists but was not observed in this window the value is 0.

Under tensor parallelism the same metric has one series per rank; when reading, all series are summed
by base metric name and then filtered by label.

### Distinguishing L1, L2, and L3 hits

Every response carries `meta_info.cached_tokens_details`, which splits the cached prompt tokens into
`device` (the GPU pool, L1), `host` (host DRAM, L2) and `storage` (the storage backend, L3), with
`storage_backend` naming the backend. The same split is counted in
`sglang:cached_tokens_total{cache_source="device"|"host"|"storage"}`; the benchmark reads the
per-request field and the counter and records both, so the two can be checked against each other.

The report therefore states which tier served the reuse rather than leaving it to be inferred, and it
refuses a tier claim the run cannot support:

- a row whose `hit_tier` is `host` or `storage` names the tier that served it, and one whose
  `hit_tier` is `mixed` names every tier that did;
- the report compares the working set of the round against the L1 and L2 pool sizes; a tier can only
  have served the reuse if its pool cannot hold that working set. The pool sizes are read from the
  running server, so a machine with a larger pool reports a different boundary;
- for the `mooncake` tier the report states when the storage backend served no token. Such a run
  measures what having the L3 tier configured costs, and not an L3 benefit; exercising L3 needs the
  working set to exceed the L2 pool, which means a smaller host pool (`--hicache-size` or
  `--hicache-ratio`) or longer inputs (`--input-lens`). On the measured deployment that was not
  enough either: with both pools smaller than the working set the storage backend still served
  nothing while accepting millions of tokens of writes, so this benchmark reports the L3 tier as
  written but never read there, and claims no L3 benefit.

**`sglang:prefill_effective_tokens_total` is not used for this.** With the `mode="host_hit"` and
`mode="storage_hit"` labels it looks like the tier split, but on the deployment used for the
measurements it stays at 0 even when end-to-end latency has dropped by an order of magnitude and
`cached_tokens` already reports the full hit volume. The raw counter values are still recorded in
`metrics_present` and `metrics_delta` of the JSONL, for verification.

## 8. Result files

```
<result-dir>/
  plan.json            the workload matrix that was actually in effect
  manifest.json        hardware, driver, framework versions, Mooncake commit, model revision, dtype, TP, page size, attention backend, KV layout, command, timestamp, git commit
  kernel.jsonl         raw records of the kernel phase, one measurement point per line
  e2e.jsonl            raw end-to-end records, one request or one run's metric delta per line
  summary.json         summary
  e2e_summary.csv      end-to-end summary table
  kernel_summary.csv   kernel summary table
  report.md            human-readable report
  logs/                stdout/stderr of the server and the client
```

Section three of `report.md` lists the cases whose p50 varies by more than 5% across repeats,
together with the minimum, the maximum, and the relative variation, to help judge whether the
numbers are usable.

## 9. Traps and exit codes

- all child processes (the SGLang server, `mooncake_master`) are registered in `CleanupRegistry` and
  are reclaimed by process group on `SIGINT`/`SIGTERM` or an exceptional exit;
- `/health` is checked before the server is used; if the server exits before it becomes ready, or the
  wait times out, the benchmark fails immediately and keeps the log paths;
- failed requests, server exceptions, scatter validation failures, and attention reference
  comparison failures are all recorded as failures, and the process exits with a non-zero code.

## 10. Environment used for validation

This benchmark is not tied to any platform: GPU model, GPU count, dtype, page size, and TP are either
probed at runtime or taken from the model config. The table below lists the values reported by the
machine during **one successful validation run**, to bound the applicability of the example numbers
in this document; it is not a precondition of this benchmark.

Hardware and system (only to bound the applicability of the example numbers):

| Item | Value during validation |
|---|---|
| GPU | 8 cards, 96 GB of memory each, compute capability 9.0, 78 SMs |
| Driver | 580.126.20, driver-reported CUDA version 13.0 |
| Host memory | 1929 GB |
| RDMA NICs | 5, no cross-node peer |

Software:

| Item | Value during validation |
|---|---|
| Python | 3.10.12 |
| PyTorch | 2.13.0+cu130 |
| SGLang | 0.5.20 |
| FlashInfer | 0.6.18 in the kernel phase (0.6.6 also used for cross-checking) |
| Mooncake Store module | built from the local source |
| Attention backend | fa3, as selected by SGLang's default |

Model and KV structure:

| Item | Value during validation |
|---|---|
| Model | `Qwen/Qwen3-8B`, 36 layers, 32 query heads, 8 KV heads, head_dim=128, bf16 |
| KV bytes per token | 147456 aggregate, 18432 per rank at TP=8 |
| page size | 64, 1179648 bytes per page per rank |
| TP | 8 (when unspecified, determined by the number of visible GPUs on the machine) |
| Memory budget | `--mem-fraction-static 0.55`, with the server reporting a KV cache pool of 170112 tokens per rank |

On that machine, other long-running jobs occupied most of the memory of the 8 cards, leaving about
10.7 GB per card available. `--mem-fraction-static` is a fraction of the memory **available** at
server startup, and the remainder is headroom for activations and CUDA graphs, so a lower value
leaves less room for the KV cache; under such a constrained environment it must be lowered
explicitly, and SGLang itself reports the usable lower bound when the value is too low. The
`deep_gemm` JIT requires nvcc 12.9 or newer, and `nvcc` and `libcudart` come from the CUDA toolkit
bundled in the venv.

## 11. Limitations

- Single-machine deployment. L3 is provided by a local `mooncake_master`, with both the client and
  the master on `127.0.0.1` and the protocol over TCP. This path only represents local loopback and
  does not represent cross-node RDMA performance; the report labels it as local/loopback throughout.
  No cross-node bandwidth numbers are produced when there is no RDMA peer.
- This benchmark does not modify the SGLang server implementation. Breakdown durations that SGLang
  does not expose are recorded as `unavailable` and are not filled in with inferred values.
- The kernel phase is a synthetic workload, used to give a lower bound and a breakdown of KV cache
  access cost; it cannot replace end-to-end measurement.
- When the end-to-end phase runs on a machine with limited GPU memory, the KV cache pool is markedly
  smaller than in a regular deployment, so the hit rate of multi-turn conversations is more easily
  affected by the eviction policy. The manifest records the `mem-fraction-static` that was actually
  in effect, and results from different machines must be compared with this item in view.
