# Attention step to KV cache access benchmark

This benchmark replays attention and KV writes for fixed synthetic batches using SGLang's request to
token table, allocator, KV writer, index kernel and FlashInfer wrappers. It reports timing windows for
this replay; metadata preparation and planning differ from a running SGLang server.

A run writes its own result directory: the raw records, a summary CSV and a manifest naming the
machine, the versions, the model and the command. A report is made from the output of the run it
describes.

## 1. The windows

The step itself is three windows, in the order a forward pass runs them:

| Window | What it covers | SGLang call it replays |
|---|---|---|
| `indices` | refill the CSR stream of KV slots for paged branches; no index kernel runs for a no-prefix ragged prefill | `create_flashinfer_kv_indices_triton`, the kernel `KVIndexTranslator.fill_packed_read_stream` launches (`sglang/kernels/ops/kvcache/kv_indices.py`) |
| `attention_plan` | direct FlashInfer wrapper planning for the fixed batch | the wrappers' `plan` |
| `layer_loop` | the per-layer loop: one layer's attention and one layer's KV write, in the order the branch runs them | the wrappers' `forward` / `forward_return_lse` / `merge_state` and `MHATokenToKVPool.set_kv_buffer` (`sglang/srt/layers/attention/flashinfer_backend.py`) |
| `phase_sum` | the three windows added up | — |
| `step_window` | one window over the whole step, from the first index call to the last layer | — |

The index buffers are allocated once per case, and each timed iteration refills only the CSR slot
stream. SGLang also updates offsets for changing batches and plans its paged wrappers with
`non_blocking=True`, so the windows exclude that metadata work and use `plan`'s default copy. A reader
comparing `step_window` with `phase_sum` sees the interval the three windows do not cover, and what
that interval contains is not measured. The derived ratios divide `phase_sum` and are named
`*_ratio_of_phase_sum` for it; the throughput figure divides `step_window`.

The loop is timed as one window on purpose: an event pair around each layer's write and read would
measure the events as much as the work, so the write and the read are priced in passes of their own:

| Component | What it covers |
|---|---|
| `kv_write_component` | one `set_kv_buffer` call per layer, over every layer of the step, in one window, without the attention they interleave with |
| `attention_component` | the branch's attention path over every layer of the step, in one window, without the writes: one ragged call, two wrapper calls and one `merge_state`, one paged prefill call, or one paged decode call |
| `kv_gather` | a read-only probe: the rows the branch's paged side reads (or the slots the step wrote, when it reads no paged KV), both K and V, moved with `index_select` and no arithmetic |
| `kv_fetch` | the move of the cached history into the pool through Mooncake, for a step whose history is held outside the GPU (section 3). One entry per run of consecutive pool slots over every layer and both tensors, submitted as one batch; the window covers building the entries, the engine's submit and wait, and the device synchronise, and the two wall-clock parts are recorded beside it |

None of the four is added to the step: each runs in a window of its own, planned against the probe's own
indices. Their ratio to `phase_sum` is recorded as a ratio rather than as a share, because the step
interleaves the write and the read per layer.

The step windows are windows on the device's timeline: they start and end with an event on the step's
stream, so they include the device waiting for the next launch that the host has not issued yet. Every
record also carries a **deferred-sync comparison**: the same iterations timed with the host allowed to
run ahead, which removes some of that wait rather than all of it, because the wrappers' plan runs on the
host inside each iteration and waits for the device itself. The two passes are therefore compared for
the size of the gap they differ by, and the plan's own wall clock is recorded beside them.

The step is built from SGLang's own `ReqToTokenPool` for the token table and `MHATokenToKVPool` for the
KV buffers, and its slots come from the allocator its page size selects (section 2).

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

`ragged_prefix_merge` is what a server runs while `SGLANG_FLASHINFER_USE_PAGED` is False, its default
in this SGLang version: the new tokens are attended to through the ragged wrapper, the cached history
through the paged one, and the two are combined with `merge_state`. `paged_extend` is the single call
that variable selects instead, and it also applies to prefill with no cached history, where it reads
all new tokens through the KV pool after writing them while the ragged lane reads their K/V tensors
before the write. With the variable unset, `--extend-branch` selects the lane and defaults to
`ragged_prefix_merge`; passing the other branch while the variable is set fails the run. Every record
states the branch, the order it ran in and the value of the variable.

The branch decides the order of the KV write: the two ragged branches compute the attention first and
save the cache afterwards, the paged branches write first. The pages themselves are properties of the
pool: the page size selects `TokenToKVPoolAllocator` at 1 and `PagedTokenToKVPoolAllocator` above it,
which take a step's slots through different entry points, and the layout decides which physical slots a
sequence's tokens occupy.

- `contiguous`: the sequence's pages come from a pool that has not churned, so its slots are ascending.
- `random`: the step's pages are a random subset of the pool drawn from the case's seed, in a random
  order, which is a pool that has churned. The draw happens once, so every timed iteration of a step
  sees the same mapping, and it hands the pool's spare pages back through the allocator, which both
  allocators serve with `free_page_ids`.

## 3. Where the cached history is held

A step's cached history is read from the pool by default: `--kv-sources gpu` puts it there before the
step and nothing moves during the run. `--kv-sources host` holds it outside the GPU instead, in host
memory registered with Mooncake's Transfer Engine, and moves it into the pool in a window of its own
before the step:

| Part | What it does |
|---|---|
| staging | the generated history is written into a host buffer per layer and per tensor, laid out with the pool's row pitch and one row per token of the aligned prefix, so a run of consecutive slots on both sides is one entry |
| registration | the host buffers and the pool's own K/V buffers are registered with the engine, because an RDMA transfer can only write into registered memory |
| the move | one batch over every layer: one entry per run of consecutive pool slots, so a pool whose history sits together needs one entry per sequence and a pool that churned needs one per page. `kv_transfer_entries` states what a case needed |
| the record | the row states the source, the engine and its version, the endpoint it was told to write to, the NICs the topology puts next to the GPU, the rows and bytes registered, the entries, and the bytes moved, beside the window in `kv_fetch` and its two wall-clock parts |

**What this is.** A component-level move through Mooncake's Transfer Engine between buffers this process
registered: the endpoint is the engine's own, so the route may be an RNIC loopback or a local path
depending on the backend. It is not SGLang's deployment path for an offloaded cache, which asks a Store
for the blocks into a host pool and then submits the host-to-device copies per layer with a readiness
check per layer, and it is not a move between machines. Those layers sit above this benchmark, and the
numbers here are for the configuration the row states.

The move happens before the step's warmup and every timed iteration reads the history it left in the
pool, so the content check that follows a step is a check on the transfer: the rows that arrive have to
be the ones the benchmark generated. A step with no cached history holds nothing whatever its source
says, and records no holder. `--kv-source-protocol` picks the transport, `rdma` unless the machine has
none.

## 4. Model and KV byte conversion

The model loader reads the number of layers, the number of query heads, the number of KV heads,
head_dim and the dtype from the model's real `config.json`, with nothing hardcoded. For grouped-query
and multi-head attention (GQA and MHA), where several query heads share one KV head, the per-token KV
is `num_layers * 2 * num_kv_heads * head_dim * dtype_bytes`: the `2` is K and V, one tensor each, and
the query heads do not appear because they share the KV heads.

Under tensor parallelism, two figures are recorded: the aggregate over every KV head of every layer,
`147456` for Qwen3-8B at any TP, and the per-rank figure `aggregate / tp_size`, which is `36864` at
TP=4. Both head counts have to divide evenly by `tp_size`, and each rank needs a whole number of query
heads per KV head: checking only the KV heads lets the integer division of query heads drop heads
silently. KV head replication, which SGLang uses with a `tp_size` above the model's KV head count, and
a sharding whose head counts do not divide are out of scope, because both describe a per-token KV that
is not `aggregate / tp_size`.

**A model whose layers do not all keep a full-length KV is refused.** The loader classifies the
attention layout from the config and prints the field it decided on:

| Layout | Evidence in `config.json` | What happens |
|---|---|---|
| `dense` | none of the fields below | measured |
| `mla` | `kv_lora_rank`, `qk_rope_head_dim` | refused: MLA compresses K and V into one latent vector, so the per-token formula above does not describe its layout |
| `hybrid` | `layer_types` with an entry that is not a full-attention layer, `sliding_window` without `use_sliding_window: false`, `full_attention_interval` above 1, `attention_types`, `sliding_window_pattern`, `linear_attn_config`, `linear_attention_config`, `mamba_d_state`, `hybrid_override_pattern`, `full_attn_idxs`, `layers_block_type`, `attention_chunk_size`, or `is_encoder_decoder` | refused: these configurations require attention or cache layouts outside this benchmark |

The dtype of the measurement also comes from the model config; a model that declares a precision the
paged attention kernels cannot run fails while the cases are built.

## 5. The ledger

The read side is split by the path that reads it: the two ragged branches hand their own tokens to the
ragged wrapper and let the paged wrapper see the cached history only, while the paged branches read the
whole context through the paged wrapper. Every figure below is recorded per step:

| Quantity | Definition |
|---|---|
| `kv_bytes_written` | `layers * 2 * new_tokens * kv_heads * head_dim * dtype_bytes`: the tokens this step computes |
| `kv_bytes_paged_read` | the same count over the tokens the branch's paged side reads: the cached history for `ragged_prefix_merge`, the whole context for `paged_extend` and `decode`, nothing for `ragged_no_prefix` |
| `kv_bytes_ragged_read` | the same count over the tokens the branch's ragged side reads: the step's own tokens for the two ragged branches, nothing for the paged ones |
| `kv_bytes_attention_read` | `kv_bytes_paged_read + kv_bytes_ragged_read`: what the attention window covers |
| `kv_bytes_page_capacity` | the same per-token count over whole pages, so a last page that is not full counts with the capacity it holds unused, and `padding_tokens` states how many tokens that is |
| `gather_bytes` | the bytes the read-only probe moves: both K and V of the `gather_rows` rows it reads, for every layer |
| `attention_pairs` | `sum over sequences of new * prefix + new * (new + 1) / 2`: a causal mask aligned to the end of the context leaves each query the history and everything computed before it |
| `attention_flops` | `layers * 4 * attention_pairs * qo_heads * head_dim`: QK^T and PV are one multiply-add each, so a query-key pair costs 4 operations |
| `attention_flops_per_unique_payload_byte` | `attention_flops / kv_bytes_attention_read` |
| `kv_cache_bytes_per_rank` | the pool's capacity on this rank: `num_pages_allocated * page_size * layers * 2 * kv_heads * head_dim * dtype_bytes` |
| `num_pages_allocated` | the pages the pool holds: what the case needs, plus the spare pages the layout leaves gaps in |

The two rates a row reports are unique-payload normalisations, and their column names say so:
`attention_unique_payload_gbps` divides `kv_bytes_attention_read` and `kv_gather_unique_payload_gbps`
divides `gather_bytes`, each by the window that path ran in. The payload is counted once per layer for
each token the path reads, however many queries read that token, so the rate is neither a kernel
bandwidth nor the traffic the memory system moved. What the memory system transfers needs a profiler:
`kv_bytes_page_capacity` and `kv_write_component` are reported as what they are, an allocation figure
and the latency of the writer's calls, so no bandwidth divides by either.

## 6. Correctness checks

Each step runs four checks, and a failure fails the run:

- `indices`: the CSR stream must name exactly the token table rows the paged side of the branch reads,
  in order. A stream built from a different page table than the one the KV was written through fails
  here, and so does a stream of another length.
- `history`: every token the step reads — the cached history and the tokens this step wrote — must hold
  what was written, **K and V**, for every layer, compared against the values the benchmark generated.
  For a step whose history Mooncake holds, this is also the check on the move (section 3).
- `attention_rows`: the output of a timed iteration itself, at full size, against a float32 reference
  built from the same content. It follows the first, middle and last query of each sequence — a decode
  step has one — because the last query may see the whole context and would leave a wrong mask looking
  right, while the first two must not see the tokens after them. A row costs one score per context token
  per head, so the whole context fits. The reference is built from the layer whose output is compared,
  and an output or a reference that is not finite fails the check rather than being compared.
- `attention`: the kernel output must match a plain tensor reference computed in float32 over the same
  content, relative error below 1e-2. The reference is causal in every branch, and a reference without
  the mask has to fail the same comparison, so a kernel that saw the future could not pass. It
  materialises a query by context score matrix, so the reference runs on a short step of the same shape
  — the same mode, page size, layout and per-rank head counts, on one layer, with every length of each
  axis scaled by one factor — while the three checks above run at full size. Every record states the
  short step it used in `correctness.attention.case`.

The checks run before every pass that rewrites the pool or replans the wrappers, so what they read is
the state the last timed iteration left and the state the last measured move left, rather than a state a
diagnostic pass produced.

Each record also states how many write calls and index fills its timed loop made, against the counts
that loop has to make: every iteration writes one layer's K/V per layer and fills one index stream. The
checks read the state the last timed iteration left, and without that count a timed loop that skipped
its work would pass on the state the warmup left, because every iteration writes the same content to
the same slots.

## 7. Command line

```bash
python -m benchmarks.sglang_attention_kv --help
python -m benchmarks.sglang_attention_kv --full --dry-run
python -m benchmarks.sglang_attention_kv --quick \
    --model /models/Qwen3-8B \
    --tp-size 4 --result-dir artifacts/sglang-attention-kv/quick
```

`--quick` and `--full` select the minimal or the full step matrix, exactly one of the two, and
`--dry-run` prints the matrix and exits without touching a GPU. `--help` lists every argument with its
default; the ones a run usually has to decide are `--model`, `--tp-size` (taken from the number of
visible GPUs when omitted), `--extend-branch` for the lane, `--page-sizes` and `--layouts` for the
pool, `--kv-sources` for where the cached history is held (section 3), and `--result-dir`, whose default
lands inside the checkout. `--kernel-warmup` and `--kernel-timed` default to 10 and 100; lower values
make the run exit non-zero, and every record carries the counts it used.

## 8. The step matrix

The matrix that `--full` expands to: the three modes; history lengths 128, 512, 2048 and 8192; extend
query lengths 128, 512 and 2048; batch sizes 1, and 4 with equal lengths and with lengths of 1, 1/2,
1/4 and 1/8; page sizes 1, 16, 32, 64 and 128; both layouts; 600 steps in total. `--quick` uses the
three modes, the lengths 128 and 512, a chunk of 128, one sequence per step, the page sizes 1 and 64,
and both layouts. The history length and the query length are separate axes: a prefill step's queries
are its whole sequence, an extend step takes its history from `--input-lens` and its queries from
`--chunk-lens`, and a decode step has one query by definition. When memory and runtime allow, add 16384
and 32768 with `--input-lens`.

## 9. Result files

```
<result-dir>/
  plan.json            the step matrix that was actually in effect
  manifest.json        what the run was measured on: machine, driver, versions, model, sharding,
                       command, commit
  kernel.jsonl         raw records, one measured step per line
  summary.json         the summary table, in JSON
  kernel_summary.csv   summary table, one row per measured step
```

Every window carries min, p50, p95 and p99 over the timed iterations: the three step windows, their sum
`phase_sum`, the step as one span `step_window`, and the components beside them. The branch the step
ran, the order it ran in and the configuration it was built with are fields of the record beside them —
the `configuration` object of `kernel.jsonl`, and the `branch`, `reads_before_write`,
`attention_backend`, `wrapper_page_size`, `decode_use_tensor_cores`, `merge_state_impl`, `kv_source`,
`kv_write_stream` and `slot_allocator` columns of the CSV — so a reader can see which step a number came
from. The CSV also carries the derived figures of section 5, the fetched bytes and transfer entries of
section 3, the plan's own wall clock, and the result of every correctness check.

A report quotes the figures as the CSV names them, and states the lane the run used, the allocator its
page size selected, and the machine and versions the manifest holds, which is what makes two runs
comparable. The GPU, the driver, the tensor parallel size, the model's shapes and the page sizes are
either probed at runtime or read from the model config, so a run elsewhere states its own.

## 10. Limitations

- One step on one GPU. A serving step also holds the queue in front of it, the scheduler, and the rest
  of the forward pass; this benchmark prices the KV access inside a step, not a request's latency.
- The steps are synthetic: the token table and the slots are built through SGLang's own allocator, but
  the sequence lengths and the batch shapes are chosen rather than taken from a trace. A real
  workload's mix is what a reader has to map onto these steps.
- The K/V values are random. The bytes moved and the arithmetic done are the shapes' own and the checks
  compare content exactly, but nothing here depends on the data's distribution.
- A different GPU rank count or model changes the per-rank head counts and the per-token bytes, which
  the manifest records; results from different shardings are only comparable through them.
- The step windows are windows on the device's timeline and include the device waiting for a launch the
  host has not issued yet. The deferred-sync comparison in every record states the same iterations with
  the host allowed to run ahead; it removes some of that wait and not all of it, and the wrappers' plan —
  which runs on the host and waits for the device itself — stays inside both.
- The Mooncake source moves the history from host memory into the pool of the same process, through the
  engine's own endpoint. It is not a Store request, not a decision between cache tiers, not SGLang's
  host-pool-then-copy order for an offloaded cache, and not a move between machines.

The replay fixes four settings and records two it derives per case, each stated in the record's
`configuration` and in the CSV, so a row says which configuration it describes. The four are this
benchmark's own lane, read off SGLang 0.5.20, and they are not what SGLang picks for every model:

| Setting | Value here | What SGLang does |
|---|---|---|
| `attention_backend` | `flashinfer-fa2` | a server selects its backend; this benchmark calls the FlashInfer wrappers, so it measures that backend and no other |
| `wrapper_page_size` | `1`, and every last-page length is 1 | the same on every model: SGLang hands FlashInfer a token-level CSR stream, and the pool's page size reaches the kernels only through the addresses in it |
| `decode_use_tensor_cores` | per case, from SGLang's own `should_use_tensor_core` | the replay calls that function with the case's cache dtype and its per-rank head counts, which is how `FlashInferAttnBackend` resolves its decode wrapper: fp8 takes the tensor-core path, fp16 and bf16 take it at a group size of 4 or more query heads per KV head, `SGLANG_FLASHINFER_USE_TENSOR_CORE` overrides the choice, and the row records the value a case resolved to |
| `kv_write_stream` | `step`: the pool is built with `enable_alt_stream=False` | in SGLang 0.5.20 that stream is used only inside CUDA graph capture, where `_set_kv_buffer_impl` branches on `get_is_capture_mode()` and otherwise writes through the fused `store_cache` kernel on the current stream. This replay captures no graph, so the field records how the pool was built |
| `merge_state_impl` | per case, from the backend's own `_merge_state_max_safe_num_heads`: FlashInfer's kernel up to 64 query heads at head_dim 128 in bf16, SGLang's Triton merge above it | the merge branch's two halves are joined by `_safe_merge_state`, which picks between the same two implementations on the same threshold, so a case records the merge a server would run |

The allocator is not one of those: it follows the pool's page size the way SGLang's own
`kv_cache_configurator` selects it, and the record states which one a step used in `slot_allocator`.
