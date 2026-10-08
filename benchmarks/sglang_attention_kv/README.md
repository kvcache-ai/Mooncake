# Attention step to KV cache access benchmark

This benchmark measures what one attention step spends on the paged KV cache, split into the stages
the step runs in the order it runs them. Every call in it belongs to SGLang: the request to token
table, the allocator, the KV writer, the index kernel and the FlashInfer paged attention wrappers are
the ones a server uses, so the numbers describe SGLang's own access path and not a stand-in for it.

The numbers from one validation run are in [`RESULTS.md`](RESULTS.md); this document is the method, the
matrix, the command line and the boundaries.

## 1. The stages

The step itself is three windows, in the order a forward pass runs them:

| Stage | What it covers | SGLang call it replays |
|---|---|---|
| `indices` | the query offsets and the CSR stream of KV slots the paged side reads | `create_flashinfer_kv_indices_triton`, the kernel `KVIndexTranslator.fill_packed_read_stream` launches (`sglang/kernels/ops/kvcache/kv_indices.py`) |
| `attention_plan` | the backend compiling the CSR stream into a kernel schedule | the wrappers' `plan`, as `FlashInferAttnBackend` calls it in `init_forward_metadata` |
| `layer_loop` | the per-layer loop: one layer's attention and one layer's KV write, layer by layer, in the order the branch runs them | the wrappers' `forward` / `forward_return_lse` / `merge_state` and `MHATokenToKVPool.set_kv_buffer` (`sglang/srt/layers/attention/flashinfer_backend.py`) |
| `total_step` | `indices` + `attention_plan` + `layer_loop` | — |

The loop is timed as one window on purpose. Putting an event pair around each layer's write and read
would measure the events as much as the work — 144 events per iteration cost about 0.6 ms of the 5 ms
step on the validation machine — so the write and the read are priced in passes of their own instead,
and they are components rather than the schedule the step runs:

| Component | What it covers |
|---|---|
| `kv_write_component` | all 36 `set_kv_buffer` calls of a step in one window, without the attention they interleave with |
| `attention_component` | all 36 attention calls of a step in one window, without the writes |
| `kv_gather` | a read-only probe: the same rows of K and V, moved with `index_select` and no arithmetic |

None of the three is added to the step. Each runs after the timed loop, in windows of its own, so no
step iteration is ever timed with a probe inside it. Their share of the step is named as a component
share, because the step interleaves the two per layer.

The structures the step is built on are SGLang's throughout: `ReqToTokenPool` for the request to token
table, `PagedTokenToKVPoolAllocator` for the slots (`alloc`, `alloc_extend` or `alloc_decode`,
depending on the step), `MHATokenToKVPool` for the buffers, and a workspace buffer sized like the one
`FlashInferAttnBackend` builds its wrappers with.

## 2. The modes and the branches

A step is a batch of sequences, each with a cached history and a number of queries it computes. The
mode says which of those the step is, and each mode replays one branch of `FlashInferAttnBackend`:

| Mode | History | Queries | Branch | What runs per layer |
|---|---|---|---|---|
| `prefill` | none | the whole sequence | `ragged_no_prefix` | one ragged prefill call over the step's own K/V, then the KV write |
| `extend` | cached | one chunk (`--chunk-lens`) | `ragged_prefix_merge` (default) | ragged suffix attention, paged history attention, `merge_state`, then the KV write |
| `extend` | cached | one chunk | `paged_extend` (`--extend-branch paged_extend`) | the KV write, then one paged prefill call over the whole context |
| `decode` | cached | one token | `paged_decode` | the KV write, then the paged decode call over the whole context |

`ragged_prefix_merge` is what a server runs while `SGLANG_FLASHINFER_USE_PAGED` is False, which is its
default in this SGLang version: `use_ragged` is true and the extend step takes the ragged branch, where
the new tokens are attended to through the ragged wrapper, the cached history through the paged one,
and the two are combined with `merge_state`. `paged_extend` is the single call that variable selects
instead when it is set. Every record states the branch, the order it ran in, and the value of that
variable, so a row says which server configuration it describes.

The branch also decides the order of the KV write: the two ragged branches compute the attention first
and save the cache afterwards, the paged branches write first. This benchmark times them in that order.

The page size and the layout are properties of the pool, not of the kernel call: SGLang hands
FlashInfer a token-level CSR stream and plans the wrappers with `page_size=1` and a last-page length of
1, whatever the pool's page size is. What the page size changes is the allocator's granularity, and
what the layout changes is which physical slots a sequence's tokens occupy:

- `contiguous`: the sequence's pages come from a pool that has not churned, so its slots are ascending;
- `random`: the step's pages are a random subset of the pool drawn from the case's seed, in a random
  order, which is a pool that has churned. The draw happens once, so every timed iteration of a step
  sees the same mapping.

## 3. Model and KV byte conversion

`model_config.py` reads the number of layers, the number of query heads, the number of KV heads,
head_dim and the dtype from the model's real `config.json`, with nothing hardcoded. For grouped-query
and multi-head attention (GQA and MHA), where several query heads share one KV head, the per-token KV
is:

```
kv_bytes_per_token = num_layers * 2 * num_kv_heads * head_dim * dtype_bytes
```

The `2` is K and V, one tensor each; `head_dim` is the width of a single head; `dtype_bytes` is 2 for
bf16 and fp16. Query heads do not appear because they share the KV heads.

Under tensor parallelism sharding, two figures are recorded at the same time:

- aggregate: bytes of a single token across every KV head of every layer, `147456` for Qwen3-8B at any TP;
- per-rank: `147456 / tp_size`, which is `36864` at TP=4 and `18432` at TP=8.

Both head counts have to divide evenly by `tp_size`; checking only the KV heads lets the integer
division of query heads silently drop heads, and the measurement then describes a shape that does not
exist.

**A model whose layers do not all keep a full-length KV is refused.** The loader classifies the
attention layout from the config and prints the field it decided on:

| Layout | Evidence in `config.json` | What happens |
|---|---|---|
| `dense` | none of the fields below | measured |
| `mla` | `kv_lora_rank`, `qk_rope_head_dim` | refused: MLA compresses K and V into one latent vector, so the per-token formula above does not describe its layout |
| `hybrid` | `layer_types` with an entry that is not a full-attention layer, `sliding_window` without `use_sliding_window: false`, `full_attention_interval` above 1, `attention_types`, `sliding_window_pattern`, `linear_attn_config`, `mamba_d_state`, `hybrid_override_pattern`, or `is_encoder_decoder` | refused: sliding-window and linear layers keep less than a full-length KV, and an encoder-decoder model reads a second cache, so one per-token count would describe none of them |

The dtype of the measurement comes from the model config rather than being hardcoded; a model that
declares a precision the paged attention kernels cannot run fails while the cases are built.

## 4. The ledger

Every derived figure divides by one of these, and all of them are recorded per step:

| Quantity | Definition |
|---|---|
| `kv_bytes_written` | `layers * 2 * new_tokens * kv_heads * head_dim * dtype_bytes`: the tokens this step computes |
| `kv_bytes_read_valid` | the same count over the step's valid context tokens |
| `kv_bytes_read_pages` | the same count over whole pages, so a last page that is not full is counted with its padding; `padding_tokens` states how many tokens that is. It is an allocation figure — the capacity the pages occupy — and no bandwidth divides by it |
| `gather_bytes` | the bytes the read-only probe moves: both K and V of the `gather_rows` rows it reads, for every layer |
| `attention_pairs` | `sum over sequences of new * prefix + new * (new + 1) / 2`: a causal mask aligned to the end of the context leaves each query the history and everything computed before it |
| `attention_flops` | `layers * 4 * attention_pairs * qo_heads * head_dim`: QK^T and PV are one multiply-add each, so a query-key pair costs 4 operations, and every layer does that work |
| `attention_arithmetic_intensity` | `attention_flops / kv_bytes_read_valid` |

Bandwidth divides logical bytes by the window those bytes belong to: the valid KV a step reads
(`attention_effective_gbps`) and the rows the probe moves (`kv_gather_effective_gbps`). What the memory
system actually transfers is not observable without a profiler, so no figure here claims it: a paged
read touches whole pages and a write may coalesce, and the page capacity is reported as the allocation
figure it is rather than as traffic.

The write side carries no rate of its own. Its window reads two regimes for the same calls, around
0.1 ms and around 1.16 ms, so it prices the host's issue of the `set_kv_buffer` calls as much as their
execution on the device, and a quotient of it would report whichever regime the window happened to
catch. `kv_write_component` is recorded as the window it is.

## 5. Correctness checks

Each step runs three checks, and a failure fails the run:

- `indices`: the CSR stream must name exactly the token table rows the paged side of the branch reads,
  in order. A stream built from a different page table than the one the KV was written through fails
  here.
- `history`: every token the step reads — the cached history and the tokens this step wrote — must hold
  what was written, **K and V**, for every layer. This is the check that a page table which moved
  between the write and the read fails, and it compares against the values the benchmark generated
  rather than against the tensor the kernel also read. `test_a_corrupted_v_is_caught` writes a value
  into one V row behind the benchmark's back and asserts the check fails.
- `attention`: the kernel output must match a plain tensor reference computed in float32 over the same
  content, relative error below 1e-2. The reference is causal in every branch: a step's queries must not
  see the tokens after them, and in the merge branch the full causal attention over history and queries
  is what its two kernels and `merge_state` have to add up to. The same comparison against a reference
  without the mask has to fail, and a test asserts that it does, so a kernel that saw the future could
  not pass. The reference materialises a query by context score matrix, so it runs on a short step of
  the same shape — the same mode, page size, layout and head counts, with the lengths capped — while
  the two checks above run at full size. Every record states the short step it used in
  `correctness.attention.case`.

## 6. Command line

```bash
python -m benchmarks.sglang_attention_kv --help
python -m benchmarks.sglang_attention_kv --full --dry-run
python -m benchmarks.sglang_attention_kv --quick \
    --model /models/Qwen3-8B \
    --tp-size 4 --result-dir artifacts/sglang-attention-kv/quick
```

`--dry-run` prints the whole step matrix; it touches no GPU, so it also runs on a machine without one.

Main arguments:

| Argument | Description |
|---|---|
| `--quick` / `--full` | minimal / full step matrix, exactly one of the two must be given |
| `--dry-run` | print the step matrix and exit |
| `--model` | model directory, must contain `config.json` |
| `--tp-size` | tensor parallel size; KV head count and query head count are sharded by it; when omitted it is taken from the number of visible GPUs, and `--dry-run` prints `auto (visible GPU count)` |
| `--modes` | which steps to measure: `prefill`, `extend`, `decode` |
| `--input-lens` | history lengths: a prefill step computes this many tokens, an extend or decode step caches this many |
| `--chunk-lens` | query lengths of an extend step, one step per entry, over the history lengths of `--input-lens` |
| `--extend-branch` | which extend branch to replay: `ragged_prefix_merge`, the default a server runs, or `paged_extend` |
| `--batch-sizes` | sequences per step; a batch above one is measured with equal lengths and with ragged lengths |
| `--page-sizes` | tokens per page of the pool |
| `--layouts` | `contiguous`, or `random` for a pool that has churned |
| `--device` | device the steps run on, `cuda:0` unless given |
| `--result-dir` | result directory, defaulting to `artifacts/sglang-attention-kv/<timestamp>`; point it outside the checkout to keep the repository clean |
| `--kernel-warmup` / `--kernel-timed` | warmup and timed iterations per step, 10 and 100 unless given. Lower values are accepted for a smoke run and make the run exit non-zero, and every record carries the counts that were used |
| `--git-commit` / `--git-branch` / `--git-subject` / `--repo-dir` | provenance written into the manifest |

## 7. The step matrix

The matrix that `--full` expands to:

- modes: `prefill`, `extend`, `decode`;
- history lengths: 128, 512, 2048, 8192;
- query lengths of an extend step: 128, 512, 2048;
- batch sizes: 1, and 4 with equal lengths and with lengths of 1, 1/2, 1/4 and 1/8;
- page sizes: 1, 16, 32, 64, 128;
- layouts: contiguous and random;
- 600 steps in total.

The history length and the query length are separate axes: a prefill step's queries are its whole
sequence, an extend step takes its history from `--input-lens` and its queries from `--chunk-lens`, and
a decode step has one query by definition. When memory and runtime allow, add 16384 and 32768 with
`--input-lens`. `--quick` uses the three modes, the lengths 128 and 512, a chunk of 128, one sequence
per step, the page sizes 1 and 64, and both layouts.

## 8. Result files

```
<result-dir>/
  plan.json            the step matrix that was actually in effect
  manifest.json        GPU, driver, framework versions, model revision, dtype, TP, command, timestamp, git commit
  kernel.jsonl         raw records, one measured step per line
  summary.json         the summary table, in JSON
  kernel_summary.csv   summary table, one row per measured step
```

Every phase record carries p50, p95 and p99 over the timed iterations, the branch the step ran and the
order it ran in, and the CSV also carries the derived figures of section 4 plus the result of every
correctness check, so a reader can see which step a number came from and whether its check passed.

## 9. What one run measured

`RESULTS.md` holds the numbers from one validation run, the machine they were taken on, and what they
say about the four stages. This document keeps the method, the matrix and the boundaries: a run on
another machine, model or sharding reports its own numbers, and the CSV and manifest of that run are
what a reader compares, because the two sides of every figure in it are recorded there.

Nothing here is a precondition of the benchmark: the GPU, the driver, the tensor parallel size, the
model's shapes and the page sizes are either probed at runtime or taken from the model config, and the
manifest of a run states every one of them.

## 10. Limitations

- One step on one GPU. A serving step also holds the queue in front of it, the scheduler, and the rest
  of the forward pass; this benchmark prices the KV access inside a step, not a request's latency.
- The steps are synthetic: the token table and the slots are built by the benchmark through SGLang's
  own allocator, but the sequence lengths and the batch shapes are chosen rather than taken from a
  trace. A real workload's mix is what a reader has to map onto these steps.
- The K/V values are random. The bytes moved and the arithmetic done are the shapes' own, and the
  checks compare content exactly, but nothing here depends on the data's distribution.
- The page size changes the allocator's granularity and the pool's addresses. It does not change the
  index tensors or the plan, because SGLang hands FlashInfer a token-level stream at every page size.
- The extension to a second GPU rank, or to a second model, changes the per-rank head counts and the
  per-token bytes; the manifest records both, and results from different shardings are only comparable
  through them.
