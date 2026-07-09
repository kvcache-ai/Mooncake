# SGLang HiCache + Mooncake L3 Observability on AI Studio A800

This note records a small, reproducible SGLang HiCache + Mooncake Store L3
experiment on a single AI Studio A800 runtime. It is intended as an
observability and reproduction note, not as a performance-win benchmark.

## Scope

- Platform: Baidu AI Studio A800 runtime.
- Model: Qwen3-0.6B.
- Runtime layout: single-node TCP-oriented setup.
- Workload: repeated-prefix requests with short output to emphasize TTFT.
- Goal: verify whether SGLang's Mooncake backend emits L3 write/read metrics and
  whether Store reload beats a no-store baseline in this constrained setup.

## Result Summary

| case | no-store p50 TTFT | store reload p50 TTFT | exists hit pages | get success pages | conclusion |
|---|---:|---:|---:|---:|---|
| p4096_c1_n32 | 44.815 ms | 45.955 ms | 3095 | 3095 | L3 read-back observed; reload was 2.544% slower |
| p8192_c1_n16 | 69.110 ms | 73.600 ms | 0 | 0 | no read-back hit; reload was 6.497% slower |

A previous gapfill run also showed successful Store write-back metrics:

```text
set_requested_pages = 13268
set_success_pages = 13268
```

## Interpretation

The `p4096_c1_n32` case demonstrates that backend-level L3 read-back can be
observed through `exists` and `get` counters. It does not demonstrate a latency
improvement. In this small-model, single-node setup, Store reload added enough
overhead that no-store remained faster.

This result is useful as a lower-bound diagnostic case:

- L3 write-back is observable.
- One L3 read-back case is observable.
- Positive TTFT or throughput gains require a stronger workload or topology,
  such as larger models, longer shared prefixes, higher concurrency, multi-turn
  histories, RAG-style repeated documents, or an independent storage process
  that remains alive across SGLang restarts.

## Follow-Up Work

1. Rebuild or install a same-runtime `mooncake_client` for AI Studio Ubuntu
   20.04 to validate an independent persistent Store topology.
2. Extend the matrix to larger models, longer prefixes, multi-turn workloads,
   RAG workloads, and higher concurrency.
3. Add first-class counters for L3 exists/get/set pages and success ratios in
   SGLang/Mooncake integration logs so negative and positive results can be
   interpreted without screenshots or ad hoc parsing.
4. Validate prefix-aware eviction/orphan suffix cleanup under real Store
   pressure before presenting it as an optimization.

## Claim Boundary

Safe claim:

```text
SGLang HiCache + Mooncake Store can produce observable L3 write-back and one
controlled read-back case on AI Studio A800.
```

Unsafe claim:

```text
Mooncake Store improves TTFT or throughput in this setup.
```
