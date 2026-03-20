# Engram: Mooncake Data-Plane Integration

## Overview

In the current Mooncake branch, Engram integration is intentionally scoped to the data plane:

- Mooncake stores Engram embedding tables in Mooncake Store.
- Mooncake hashes token sequences into deterministic row indices.
- Mooncake queries the corresponding embedding rows and returns them as `[B, L, H, D]`.

Mooncake no longer implements Engram's model-side forward path. The following pieces are outside Mooncake and should live in the model/runtime that consumes the queried embeddings:

- context-aware gating
- value projection
- short convolution
- residual fusion

That boundary keeps Mooncake focused on what it does best: high-throughput, low-latency storage and retrieval of deterministic embedding data.

**Paper Reference:** [Conditional Memory via Scalable Lookup: A New Axis of Sparsity for Large Language Models](https://arxiv.org/abs/2601.07372)

## Current Architecture

```text
input_ids [B, L]
    |
    v
hash_input_ids(...)
    |
    v
hash_ids [B, L, H]
    |
    v
Mooncake Store lookup
  - key:   engram:l{layer_id}:h{head_idx}
  - table: [N_h, D]
    |
    v
query(...)
    |
    v
embeddings [B, L, H, D]
    |
    v
model-side fusion outside Mooncake
```

Where:

- `B`: batch size
- `L`: sequence length
- `H = (max_ngram_size - 1) * n_head_per_ngram`
- `D = n_embed_per_ngram / n_head_per_ngram`
- `N_h`: the real table size for head `h`

## What Mooncake Provides for Engram

### 1. Per-layer, per-head distributed storage

Engram tables are stored in Mooncake Store under keys of the form:

```text
engram:l{layer_id}:h{head_idx}
```

Each key maps to one 2D embedding table:

```text
[N_h, D]
```

This layout means:

- each layer is isolated
- each head is independently addressable
- each head can be placed, migrated, cached, or read independently
- Mooncake can use batch metadata/data operations across all heads

### 2. A deterministic hash-to-row mapping

Mooncake exposes `hash_input_ids(...)`, which converts token IDs into per-head row indices.

The hash path is deterministic:

1. Shift the sequence to form N-gram windows.
2. Mix shifted token IDs with per-layer odd multipliers via XOR.
3. Apply modulo with the target head's table size.

The output shape is:

```text
[B, L, H]
```

This is not ANN or similarity search. The row address is fully determined by the input token sequence, which is exactly the kind of access pattern Mooncake can serve efficiently.

### 3. Store-backed embedding query

Mooncake exposes `query(...)` / `query_embeddings(...)`, which read the embedding rows addressed by the hash output and materialize:

```text
[B, L, H, D]
```

This is the only tensor Mooncake returns for Engram. Any later fusion with model hidden states happens outside Mooncake.

### 4. A batch populate path

Mooncake exposes `populate_store_from_buffers(...)` to upload all head tables in one pass.

The populate path:

- validates that each head buffer is 2D
- validates that each buffer matches the expected `[N_h, D]`
- registers the buffers with Mooncake Store
- uploads them with `batch_put_from(...)`

This gives Engram one consistent path for initialization, checkpoint restore, or offline table construction.

### 5. A layer-scoped cleanup path

Mooncake exposes `remove_from_store(force=False)` to delete all store objects owned by one Engram layer instance.

The cleanup path:

- enumerates the exact per-head keys for the current layer
- checks whether each key currently exists
- deletes them one by one with the store's regular remove API
- ignores already-missing keys so repeated cleanup calls remain safe
- invalidates Engram's cached query metadata after cleanup

Operationally:

- `force=False` respects normal Mooncake lease protection, so cleanup can fail if readers still hold active leases
- `force=True` bypasses the lease check, but it is still not meant to override replica/task safety checks during concurrent write/replication activity

This is the recommended production cleanup mechanism for Engram data. It is much safer than calling store-wide `remove_all()`.

## Engram Data Model in Mooncake

### Per-head table layout

For a given layer, Mooncake stores Engram as `H` separate 2D tables rather than one monolithic 4D tensor:

```text
key = (layer_id, head_idx)
value = [N_h, D]
```

This is important because each head can have a different table size `N_h`.

### Is a dimension missing?

No. The implementation does **not** lose a dimension.

From the model view, the queried result is still:

```text
[B, L, H, D]
```

From the storage view:

- `H` is represented in the key space (`head_idx`)
- each head owns a table `[N_h, D]`

So the head dimension is split into storage objects, not removed.

### Use `get_table_vocab_sizes()`, not raw `engram_vocab_size`

A key correctness detail is that the real table size of each head is not always the raw configured `engram_vocab_size`.

Mooncake expands each per-N-gram base size into per-head prime-sized tables to reduce collisions. As a result:

- different heads can have different `N_h`
- the correct table shape must come from `get_table_vocab_sizes()`
- upload/query correctness depends on the hash modulo space matching the stored table size exactly

If tables are created from the raw `engram_vocab_size` only, the storage layout can become inconsistent with the row indices produced by hashing.

## Query Path

### Hash phase

`hash_input_ids(...)` computes row indices shaped `[B, L, H]`.

In the current C++ Mooncake path, hashing operates on the provided `input_ids` directly. Model-side tokenizer compression / vocabulary projection is not part of the current store-backed query path.

### Read phase

`query_embeddings(...)` uses a hybrid read strategy.

1. Compute the hash IDs.
2. Resolve table metadata with `batch_query(...)`, using a lease-aware cache when placement is stable.
3. Try the local-direct fast path first:
   - if a queried table replica lives in one of this client's mounted segments
   - Mooncake resolves it to a directly accessible virtual address
   - lookup then reads rows with local `memcpy` and skips `get_into_range(...)` / `batch_get_buffer(...)`
4. If local direct access is not available, use scratch buffers for the range-read path.
   - Mooncake first tries to reuse an internal registered workspace across queries
   - if the caller passes a large enough workspace, that can be used as scratch space too
5. In the range-read path:
   - batch-resolve metadata for all head tables with `batch_query(...)`
   - collect the unique rows needed by the current batch
   - sort row indices and merge adjacent rows into ranges
   - issue one batched multi-range transfer into the scratch workspace
   - scatter/copy the retrieved rows into the final `[B, L, H, D]` output buffer

### Optional workspace

The query API accepts an optional preallocated workspace sized by:

- `get_embedding_tables_workspace_size()`
- `get_query_workspace_size(B, L)`

Two sizes matter:

- `get_embedding_tables_workspace_size()` is the size needed to hold every head table in full.
- `get_query_workspace_size(B, L)` is the smaller scratch size needed for the current batch, capped by the number of rows that can actually be touched.

If the caller does not provide a workspace, Engram can still reuse an internal registered workspace managed inside the C++ object. This avoids per-query register/unregister overhead in the common repeated-query case.

## Current Performance Optimizations

### 1. Per-head object layout

Keeping one object per head makes sparse row access much more practical than a single giant tensor. It also keeps placement and parallelism flexible.

### 2. Query-result caching

`batch_query(...)` results are cached inside the Engram object while their leases remain valid. The cache is invalidated after repopulating the tables.

This avoids repeated metadata lookups for steady-state query traffic.

### 3. Same-node local-direct reads

If a table replica is mounted in the same client process, Mooncake resolves the replica to a local virtual address and reads rows directly from the mounted segment.

This is the critical fast path for same-node Engram benchmarks:

- no RDMA read is needed
- no range-read staging buffer is needed
- no `batch_get_buffer(...)` is needed

In this case the store-read cost becomes close to pure local memory access.

### 4. Internal registered workspace reuse

When the query cannot use the local-direct path, Engram reuses an internal scratch workspace that stays registered with Mooncake across queries.

This removes repeated buffer registration overhead from the steady-state query path, while still allowing a caller-provided workspace when explicit control is needed.

### 5. Unique-row extraction and range merge

For each head, Mooncake first deduplicates the requested row indices, then merges adjacent rows into ranges. This reduces duplicated reads and improves RDMA/storage efficiency when nearby rows are requested together.

### 6. Hybrid read strategy

The implementation uses a mixed policy:

- when the number of merged ranges is small, it reads only the touched ranges
- when the total range count exceeds the current threshold, or the requested bytes approach a full-table read, it falls back to fetching table buffers

This prevents extremely fragmented range reads from becoming slower than bulk fetch.

### 7. Direct write into output layout

The query API materializes output directly in `[B, L, H, D]` layout, which avoids extra result objects and keeps the interface simple for downstream model code.

### 8. Batch upload path

Engram table population uses registered buffers plus `batch_put_from(...)`, which is much better suited than one-object-at-a-time uploads when many head tables are initialized together.

## What Engram Data Looks Like from a Systems Perspective

Engram data has a very specific access pattern:

- **static or mostly static**: tables are populated once and then read heavily
- **read-dominant**: query cost matters much more than update cost
- **deterministic addressing**: row addresses are computed directly from token IDs
- **multi-table sparse access**: each request touches many heads but only a small subset of rows in each table
- **hotspot potential**: repeated N-grams can create hot rows or hot heads across requests

That combination makes Mooncake a good fit, because Mooncake is optimized for batched metadata resolution, remote object access, registered buffers, and efficient data movement.

## Cross-Node Control Plane

Cross-node sparse queries now use a producer-side remote-gather path instead of
issuing many tiny reads directly from the consumer. The flow is:

- the consumer plans per-node sparse ranges and packs them into a compact binary request
- the request is written into a pre-registered descriptor slot
- a small doorbell notify tells the remote producer which slot to fetch
- the producer gathers rows into staging buffers and writes the packed result back
- a small completion ACK tells the consumer that the remote work has finished

This keeps large descriptors off the control socket, keeps the data path on the
existing transfer primitives, and makes the mechanism reusable for other sparse
scatter/gather workloads beyond Engram.

### Cross-Node Operating Conditions

The remote-gather path is opportunistic rather than mandatory. Today it is used
only when all of the following are true:

- the store is live and the Engram instance still owns that store handle
- the active transfer protocol is RDMA
- `MC_ENGRAM_REMOTE_GATHER` is not disabled
- the consumer can provision descriptor-ring slots large enough for the current
  request shape

If any of those conditions is not met, `query()` still follows the same API and
falls back to the existing range-read or bulk-fetch path. In other words, remote
gather is an optimization layer, not a separate query contract.

The producer side also relies on the Engram object remaining alive while it owns
registered control/data buffers and its background notify loop. The recommended
lifecycle remains:

1. populate/query through `Engram`
2. stop using that instance
3. destroy the `Engram` object
4. close the backing store/client

The destructor is defensive and attempts to clean up even if the caller gets the
order wrong, but the safe production rule is still "destroy Engram first, close
store second".

### Cross-Node Tuning Knobs

The current implementation exposes a few environment variables for capacity and
polling control:

- `MC_ENGRAM_REMOTE_GATHER`
  - default: enabled
  - set to `0` / `false` / `off` to force the legacy cross-node fallback path
- `MC_ENGRAM_SG_RING_SLOT_BYTES`
  - default: `256 KiB`
  - base descriptor-slot size reserved in the consumer-side request ring
- `MC_ENGRAM_SG_RING_SLOTS`
  - default: `64`
  - number of descriptor slots kept in the request ring
- `MC_ENGRAM_SG_RING_MAX_SLOT_BYTES`
  - default: `1 MiB`
  - upper bound when a large request forces slot growth
- `MC_ENGRAM_REMOTE_GATHER_HOT_POLLS`
  - default: `64`
  - how many immediate notify polls the producer-side service will do before it
    falls back to the normal sleep interval

Practical guidance:

- increase slot bytes or max slot bytes when one sparse query touches many
  merged ranges per remote node
- increase slot count when many queries may be in flight concurrently from the
  same consumer
- leave hot polls low unless you are specifically chasing control-plane tail
  latency and can afford extra CPU busy-polling

## Current Limits and Follow-up Opportunities

The current implementation is intentionally minimal and correct for the data path, but there is still room to improve:

1. **Tokenizer compression / vocabulary projection**
   - The store-backed C++ path currently hashes raw token IDs.
   - If we want full parity with upstream Engram variants that compress vocabulary before hashing, this should be added explicitly.

2. **Adaptive read policy**
   - The range-vs-full-table switch is currently a fixed threshold.
   - It could be made adaptive based on head size, unique-row count, bandwidth, and observed latency.

3. **Cache hot rows / hot heads**
   - Repeated N-grams should make caching effective for real workloads.

4. **Reduce scatter overhead**
   - Query currently still has to scatter the retrieved rows into `[B, L, H, D]`.
   - There is room to improve vectorization, memory layout, and GPU handoff.

5. **Remote / cross-node query path**
   - The current biggest win comes from the same-node local-direct path.
   - Cross-node queries now have a producer-side remote-gather path, but they
     still have more control-plane and staging overhead than the same-node fast
     path. This remains the next important place to optimize if we need
     microsecond-class latency beyond single-node access.

6. **Explore finer-grained object layouts**
   - Today each head is one object.
   - For some workloads, row-block layouts or hot/cold partitioning may reduce remote read amplification further.

## Next-Generation Cross-Node Path

The current branch already implements an Engram-specific vertical slice of the
new control plane: binary descriptors, descriptor-ring submission, small
doorbells, and producer-side gather execution.

The next stage is to generalize and harden that path:

- extract the scatter-gather substrate into a reusable Mooncake capability
- replace per-request ACK handling with a cleaner generic completion model
- formalize slot ownership, timeout, and peer-restart semantics as a public
  contract
- keep Engram as the first planner built on top of that shared substrate

The architectural details are tracked in the dedicated design document:

- [Scatter-Gather Control Plane for Engram and Beyond](./scatter-gather-control-plane.md)

## Validation

The current implementation is validated with:

- `scripts/test_engram.py` for API/correctness coverage
- `scripts/bench_engram_27b.py` for one-token batch-size latency and throughput sweeps

Operationally, benchmark/test environments must be cleaned before launching a new master or client. A stale `mooncake_master` can cause bind failures and invalidate measurements.

## Production Cleanup Guidance

For online cleanup, use the dedicated `Engram.remove_from_store(force=False)` API rather than `remove_all()`.

Recommendations:

- drain traffic for the target layer/model first
- prefer exact per-layer cleanup over regex-wide or store-wide deletion
- keep `force=False` in normal production flows so lease protection remains active
- only use `force=True` during controlled maintenance when you are sure no live readers still depend on the objects
- if cleanup succeeds and the same worker will continue serving, repopulate the tables before re-enabling traffic

## Summary

Mooncake's role for Engram is now clear:

> Mooncake is not Engram's model computation module.
> Mooncake is Engram's deterministic embedding data plane: it stores per-layer/per-head embedding tables, hashes token sequences into row indices, and returns `[B, L, H, D]` embeddings as fast as possible.

## References

- **Paper**: [Conditional Memory via Scalable Lookup: A New Axis of Sparsity for Large Language Models](https://arxiv.org/abs/2601.07372)
- **Mooncake Store**: [Design Documentation](./mooncake-store.md)
