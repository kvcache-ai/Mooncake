# Attention step to KV cache access benchmark

This benchmark replays attention and KV writes for fixed synthetic batches using SGLang's request to
token table, allocator, KV writer, index kernel and FlashInfer wrappers. It reports timing windows for
this replay; metadata preparation and planning differ from a running SGLang server.

This document is the method, the matrix, the command line and the boundaries. A run writes its own
result directory — the raw records, a summary CSV and a manifest naming the machine, the versions and
the command — so a report is made from the output of the run it describes.

## 1. The windows

The step itself is three windows, in the order a forward pass runs them:

| Window | What it covers | SGLang call it replays |
|---|---|---|
| `indices` | refill the CSR stream of KV slots for paged branches; no index kernel runs for a no-prefix ragged prefill | `create_flashinfer_kv_indices_triton`, the kernel `KVIndexTranslator.fill_packed_read_stream` launches (`sglang/kernels/ops/kvcache/kv_indices.py`) |
| `attention_plan` | direct FlashInfer wrapper planning for the fixed batch | the wrappers' `plan` |
| `layer_loop` | the per-layer loop: one layer's attention and one layer's KV write, layer by layer, in the order the branch runs them | the wrappers' `forward` / `forward_return_lse` / `merge_state` and `MHATokenToKVPool.set_kv_buffer` (`sglang/srt/layers/attention/flashinfer_backend.py`) |
| `phase_sum` | the three windows added up | — |
| `step_window` | one window over the whole step, from the first index call to the last layer | — |

The index buffers are allocated once per case. Query and KV offsets are computed during setup, and
each timed iteration refills only the CSR slot stream. SGLang also updates offsets for changing batches
and its paged wrapper planning uses `non_blocking=True`; this replay calls `plan` with its default copy
behavior. The windows therefore exclude some server metadata work and use a different planning path.

`step_window` is the step as one span and `phase_sum` is the sum of its parts. A reader comparing them
sees the between-window interval: what the three windows do not cover. The measurement does not say
what that interval contains. The derived ratios divide `phase_sum` and are named `*_ratio_of_phase_sum`
for it; the throughput figure divides `step_window`.

The loop is timed as one window on purpose: putting an event pair around each layer's write and read
would measure the events as much as the work, so the write and the read are priced in passes of their
own instead, and they are components rather than the schedule the step runs:

| Component | What it covers |
|---|---|
| `kv_write_component` | one `set_kv_buffer` call per layer, over every layer of the step, in one window, without the attention they interleave with |
| `attention_component` | the branch's attention path over every layer of the step, in one window, without the writes. What that path is depends on the branch: one ragged call, two wrapper calls and one `merge_state`, one paged prefill call, or one paged decode call. The merge branch's path is several calls per layer, so one component window holds more than one call per layer |
| `kv_gather` | a read-only probe: the rows the branch's paged side reads (or the slots the step wrote, when it reads no paged KV), both K and V, moved with `index_select` and no arithmetic |

Each of the two components holds one call per layer per step, so a step of a model with L layers holds
L write calls and L per-layer attention paths.

None of the three is added to the step. Each runs after the timed loop, in windows of its own, so no
step iteration is ever timed with a probe inside it, and the wrappers are planned against the probe's
own indices before those passes rather than inside them. Their ratio to `phase_sum` is recorded as a
ratio rather than as a share of the step, because the step interleaves the two per layer and a
component window can read above the window it belongs to: the loop holds a layer's attention and its
KV write as the branch interleaves them, each component holds one of the two, and the interval the
three step windows leave out is not a component of anything.

The step uses `ReqToTokenPool` for the token table, `TokenToKVPoolAllocator` at page size 1 or
`PagedTokenToKVPoolAllocator` above it, and `MHATokenToKVPool` for the KV buffers.

## 2. The modes and the branches

A step is a batch of sequences, each with a cached history and a number of queries it computes. The
mode says which of those the step is, and each mode replays one branch of `FlashInferAttnBackend`:

| Mode | History | Queries | Branch | What runs per layer |
|---|---|---|---|---|
| `prefill` | none | the whole sequence | `ragged_no_prefix` | one ragged prefill call over the step's own K/V, then the KV write |
| `prefill` | none | the whole sequence | `paged_extend` (paged lane) | the KV write, then one causal paged prefill call over those slots |
| `extend` | cached | one chunk (`--chunk-lens`) | `ragged_prefix_merge` (default) | ragged suffix attention, paged history attention, `merge_state`, then the KV write |
| `extend` | cached | one chunk | `paged_extend` (`--extend-branch paged_extend`) | the KV write, then one paged prefill call over the whole context |
| `decode` | cached | one token | `paged_decode` | the KV write, then the paged decode call over the whole context |

`ragged_prefix_merge` is what a server runs while `SGLANG_FLASHINFER_USE_PAGED` is False, which is its
default in this SGLang version: `use_ragged` is true and the extend step takes the ragged branch, where
the new tokens are attended to through the ragged wrapper, the cached history through the paged one,
and the two are combined with `merge_state`. `paged_extend` is the single call that variable selects
instead when it is set.

The paged lane also applies to prefill with no cached history. It reads all new tokens through the KV
pool after writing them; the default ragged lane reads their K/V tensors before the KV write.

That variable decides the prefill/extend lane. With it set, a run takes the branch it
selects; with it unset, `--extend-branch` decides and defaults to `ragged_prefix_merge`. Passing the
other branch while the variable is set fails the run rather than overriding it, and every record states
the branch, the order it ran in, and the value of the variable, so a row says which server
configuration it describes.

The branch also decides the order of the KV write: the two ragged branches compute the attention first
and save the cache afterwards, the paged branches write first. This benchmark times them in that order.

The page size and the layout are properties of the pool, not of the kernel call: SGLang hands
FlashInfer a token-level CSR stream and plans the wrappers with `page_size=1` and a last-page length of
1, whatever the pool's page size is. What the page size changes is the allocator, and what the layout
changes is which physical slots a sequence's tokens occupy:

- the allocator: SGLang builds `TokenToKVPoolAllocator` when the pool's page size is 1 and
  `PagedTokenToKVPoolAllocator` above it, and the two allocate a step's slots through different entry
  points. The token allocator takes `alloc` for the step's tokens, which is what
  `mem_cache/allocation.py` does at that page size, and raises on `alloc_extend` and `alloc_decode`,
  which are the paged allocator's step entry points. A step uses the allocator its page size selects,
  and its record states which one that was in `slot_allocator`;
- `contiguous`: the sequence's pages come from a pool that has not churned, so its slots are ascending;
- `random`: the step's pages are a random subset of the pool drawn from the case's seed, in a random
  order, which is a pool that has churned. The draw happens once, so every timed iteration of a step
  sees the same mapping. The draw hands the pool's spare pages back through the allocator, which both
  allocators serve with `free_page_ids`.

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
exist. A run also needs a whole number of query heads per KV head on each rank.

Two shardings are therefore out of scope for this benchmark rather than unsupported by SGLang: KV head
replication, which SGLang uses with a `tp_size` above the model's KV head count, and a sharding whose
head counts do not divide. Both describe a per-token KV that is not `total / tp_size`, so a run states
the sharding it measured and a reader compares results across shardings through the manifest.

**A model whose layers do not all keep a full-length KV is refused.** The loader classifies the
attention layout from the config and prints the field it decided on:

| Layout | Evidence in `config.json` | What happens |
|---|---|---|
| `dense` | none of the fields below | measured |
| `mla` | `kv_lora_rank`, `qk_rope_head_dim` | refused: MLA compresses K and V into one latent vector, so the per-token formula above does not describe its layout |
| `hybrid` | `layer_types` with an entry that is not a full-attention layer, `sliding_window` without `use_sliding_window: false`, `full_attention_interval` above 1, `attention_types`, `sliding_window_pattern`, `linear_attn_config`, `linear_attention_config`, `mamba_d_state`, `hybrid_override_pattern`, `full_attn_idxs`, `layers_block_type`, `attention_chunk_size`, or `is_encoder_decoder` | refused: these configurations require attention or cache layouts outside this benchmark |

The dtype of the measurement comes from the model config rather than being hardcoded; a model that
declares a precision the paged attention kernels cannot run fails while the cases are built.

## 4. The ledger

The read side is split by the path that reads it, because the branches read different KV: the two
ragged branches hand their own tokens to the ragged wrapper and let the paged wrapper see the cached
history only, while the paged branches read the whole context through the paged wrapper. Every figure
below is recorded per step:

| Quantity | Definition |
|---|---|
| `kv_bytes_written` | `layers * 2 * new_tokens * kv_heads * head_dim * dtype_bytes`: the tokens this step computes |
| `kv_bytes_paged_read` | the same count over the tokens the branch's paged side reads: the cached history for `ragged_prefix_merge`, the whole context for `paged_extend` and `decode`, and nothing for `ragged_no_prefix` |
| `kv_bytes_ragged_read` | the same count over the tokens the branch's ragged side reads: the step's own tokens for the two ragged branches, nothing for the paged ones |
| `kv_bytes_attention_read` | `kv_bytes_paged_read + kv_bytes_ragged_read`: what the attention window covers |
| `kv_bytes_page_capacity` | the same per-token count over whole pages, so a last page that is not full counts with its padding and `padding_tokens` states how many tokens that is. It is an allocation figure — the capacity the pages occupy — and no bandwidth divides by it |
| `gather_bytes` | the bytes the read-only probe moves: both K and V of the `gather_rows` rows it reads, for every layer |
| `attention_pairs` | `sum over sequences of new * prefix + new * (new + 1) / 2`: a causal mask aligned to the end of the context leaves each query the history and everything computed before it |
| `attention_flops` | `layers * 4 * attention_pairs * qo_heads * head_dim`: QK^T and PV are one multiply-add each, so a query-key pair costs 4 operations, and every layer does that work |
| `attention_flops_per_unique_payload_byte` | `attention_flops / kv_bytes_attention_read` |

The two rates a row reports are unique-payload normalisations, and their column names say so:
`attention_unique_payload_gbps` divides `kv_bytes_attention_read`, and `kv_gather_unique_payload_gbps`
divides `gather_bytes`, each by the window that path ran in. The payload is counted once per layer for
each token the path reads, however many queries read that token: the repeated operand a query costs
appears in the FLOP count, not in those bytes, so the rate is not a kernel bandwidth and not the traffic
the memory system moved. What the memory system actually transfers is not observable without a
profiler, and no figure here claims it: a paged read touches whole pages and a write may coalesce, and
the page capacity is reported as the allocation figure it is rather than as traffic.

The write side carries no rate of its own. Its window does not track the bytes a step writes, so what it
covers is not determined by the measurement and a quotient of it would report whatever the window
caught. `kv_write_component` is recorded as the window it is, with its min, p50, p95 and p99 beside it
so a reader can see how far apart its samples are within one row.

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
| `--extend-branch` | the prefill/extend lane: `ragged_prefix_merge` defaults to ragged prefill and merged extend; `paged_extend` uses paged attention for both. It selects the lane when `SGLANG_FLASHINFER_USE_PAGED` is unset; a conflicting choice fails when the variable is set |
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

Every window carries min, p50, p95 and p99 over the timed iterations: the three step windows, their sum
`phase_sum`, the step as one span `step_window`, and the components beside them. The branch the step
ran, the order it ran in and the configuration it was built with are fields of the record beside
them — the `configuration` object of `kernel.jsonl`, and the `branch`, `reads_before_write`,
`attention_backend`, `wrapper_page_size`, `decode_use_tensor_cores`, `kv_write_stream` and
`slot_allocator` columns of the CSV — so a reader can see which step a number came from. The CSV also
carries the derived figures of section 4 plus the result of every correctness check.

## 9. Reporting a run

A report of this benchmark is made from the output of the run it describes: the summary CSV holds the
two sides of every figure — the bytes, the window they are divided by, the ledger and the checks — and
the manifest holds the machine, the versions, the model, the sharding and the command that produced it.
Nothing here is a precondition of the benchmark: the GPU, the driver, the tensor parallel size, the
model's shapes and the page sizes are either probed at runtime or taken from the model config, so a run
elsewhere states its own.

A report quotes the windows as they are named in the CSV, and says which lane the run used, which
allocator the pool's page size selected, and which machine and versions it came from. Figures from two
runs are comparable through those fields, and the components are reported beside the step rather than
added into it for the reason in section 1.

## 10. Limitations

- One step on one GPU. A serving step also holds the queue in front of it, the scheduler, and the rest
  of the forward pass; this benchmark prices the KV access inside a step, not a request's latency.
- The steps are synthetic: the token table and the slots are built by the benchmark through SGLang's
  own allocator, but the sequence lengths and the batch shapes are chosen rather than taken from a
  trace. A real workload's mix is what a reader has to map onto these steps.
- The K/V values are random. The bytes moved and the arithmetic done are the shapes' own, and the
  checks compare content exactly, but nothing here depends on the data's distribution.
- The page size selects the allocator SGLang builds — the token allocator at page size 1 and the paged
  one above it — and the pool's addresses follow from the slots that allocator hands out. It does not
  change the index tensors or the plan, because SGLang hands FlashInfer a token-level stream at every
  page size.
- The extension to a second GPU rank, or to a second model, changes the per-rank head counts and the
  per-token bytes; the manifest records both, and results from different shardings are only comparable
  through them.

The replay fixes four settings, each stated in the record's `configuration` and in the CSV, so a row
says which configuration it describes. They are the benchmark's own lane, read off SGLang 0.5.20 as the
version below states; they are not what SGLang picks for every model:

| Setting | Value here | What SGLang does |
|---|---|---|
| `attention_backend` | `flashinfer-fa2` | a server selects its backend; this benchmark calls the FlashInfer wrappers, so it measures that backend and no other |
| `wrapper_page_size` | `1`, and every last-page length is 1 | the same on every model: SGLang hands FlashInfer a token-level CSR stream, and the pool's page size is a separate figure that reaches the kernels only through the addresses in it |
| `decode_use_tensor_cores` | `true` | `FlashInferAttnBackend` decides this per model through `should_use_tensor_core`: a bf16 or fp16 cache at a group size of 4 or more query heads per KV head takes the tensor-core path, which a model at that group size reaches. A model below that threshold builds the wrapper without it, and `SGLANG_FLASHINFER_USE_TENSOR_CORE` overrides the choice either way |
| `kv_write_stream` | `step`: the pool is built with `enable_alt_stream=False` | in SGLang 0.5.20 that stream is used only inside CUDA graph capture: `_set_kv_buffer_impl` branches on `get_is_capture_mode()` and otherwise writes through the fused `store_cache` kernel on the current stream. This replay captures no graph, so the flag records how the pool was built rather than a different write path |

The allocator is not one of those four: it follows the pool's page size the way SGLang's own
`kv_cache_configurator` selects it, and the record states which one a step used in `slot_allocator`.
