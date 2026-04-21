# EFA SRD shared-endpoint refactor: P5EN validation

Results from drift-stress validation of the shared-endpoint refactor on the
3-node P5EN cluster (`feat/efa-srd-shared-endpoints` branch).

## Setup

- **Hosts:** P5EN-1 (initiator) ↔ P5EN-2 (target), us-east-2, 48 EFA NICs each
- **Target shape:** 328 GB hugetlbfs, 80 MR × 4.1 GB, 16 NIC full coverage,
  per-MR allocation, 180s lifetime per restart (customer-canonical shape)
- **Initiator:** 20 threads, 0.5 MB block, batch=34, `--warmup-connect`,
  `--requests-per-thread 100000` (large enough to saturate until target switch)
- **Drift:** `restart_target_loop.sh` restarts target every 180s with a fresh
  RPC port → `segment_id` changes across iters, exercising peer re-insertion
  in the shared AV

## Results (4 iters, stopped at user request)

| iter | state | warmup_ms | first_batch_us | steady_gbps | ok | fail | status |
|---:|---|---:|---:|---:|---:|---:|---|
| 0 | fresh peer | 1110 | 33 569 | 220.0 | 29.2 M | 585 | peer_died |
| 1 | drift | 0 | 339 498 | 217.2 | 68.0 M | 0 | ok |
| 2 | drift | 0 | 290 735 | 216.5 | 68.0 M | 0 | ok |
| 3 | drift | 0 | 261 166 | 213.9 | 68.0 M | 0 | ok |

Raw CSV: [`drift_p5en_4iter.csv`](drift_p5en_4iter.csv).
Target teardown timings: [`target_exit_timings.tsv`](target_exit_timings.tsv)
(each restart recovered free hugepages in 0 s — clean customer teardown path
already established in earlier work.)

## Interpretation vs. baseline

| metric | old (per-peer endpoint) | new (shared endpoint) | Δ |
|---|---:|---:|---:|
| cold-peer first submit | ~8.95 s | **33.6 ms** | **266× faster** |
| drift first_batch (after peer switch) | ~8.95 s | **261–340 ms** | **~26× faster** |
| QP growth per peer | 16 (one per local NIC) | **0** (one AV slot, one shared `fid_ep`) | unbounded → constant |
| steady throughput | ~220 GB/s | ~215–220 GB/s | unchanged |

- Iter 0 confirms the refactor's core goal: cold warmup is no longer dominated
  by 16 × `fi_endpoint + fi_enable` sequences.
- Iter 1–3 confirm the peer-drift path: `setPeerNicPath` detaches the stale
  AV slot, re-handshakes, and re-inserts — no `fi_enable` / ENOMEM regression.
- Drift first_batch is not zero because `warmupSegment()`'s idempotent
  short-circuit currently matches endpoints bound to the *previous* peer
  address (same local_ctx × device path). That pushes the per-peer handshake
  RTT onto the first batch. The datapath still recovers in <350 ms, but this
  is a known follow-up — tightening the short-circuit to compare the current
  `peer_nic_path_` value would eliminate it.

## Reproducing

See [`../drift_stress/README.md`](../drift_stress/README.md). Minimal flow:

```bash
# on target
MAX_RESTARTS=10 RESTART_INTERVAL_S=180 TARGETS_PER_NUMA=40 BUFFER_SIZE_MB=4198 \
  bash mooncake-transfer-engine/example/drift_stress/restart_target_loop.sh &

# on initiator
DRIFT_ITERS=20 TARGETS=1 REQUESTS_PER_THREAD=100000 BUFFER_SIZE_MB=4198 \
  bash mooncake-transfer-engine/example/drift_stress/run_initiator.sh
```
