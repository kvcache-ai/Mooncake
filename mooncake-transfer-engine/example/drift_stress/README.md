# EFA drift-stress harness

Long-running EFA initiator talking to 1-2 targets whose `segment_id` (ip:port)
changes each iteration. Validates two things:

1. **Init correctness** — warmup/open_segment against a port-drifting peer
   always produces a usable connection (no stale `fi_addr_t` left behind).
2. **QP accumulation** — on EFA (~768 QP/device cap) the endpoint store
   must stay bounded across restarts, otherwise `fi_enable` returns ENOMEM.

Without the fix in commit `8fe37a0a` (evict-on-ENOMEM), QP exhaustion hits
around iter `768 / (16 * num_targets)` — i.e. ~24 with two targets.

## Topology

| Role | Host | Script |
|---|---|---|
| initiator | P5EN-1 | `run_initiator.sh` |
| target A  | P5EN-2 | `restart_target_loop.sh` + `start_target.sh` |
| target B  | P5EN-3 | `restart_target_loop.sh` + `start_target.sh` |

Each target restart picks a fresh `--rpc-port` (unix timestamp % 20000 + 20000)
and writes its own `/tmp/drift_peer.txt` with one line `ip:port,base0,base1`.
`run_initiator.sh` rsyncs both peer files to P5EN-1 and runs
`customer_pattern --drift-iters N`.

## Usage

From your laptop:

```
bash mooncake-transfer-engine/example/drift_stress/orchestrate.sh
```

Env overrides (all optional):

```
DRIFT_ITERS=100                # per-target restart count
RESTART_INTERVAL_S=15          # sec between kills on each target
TARGETS_PER_NUMA=80
BUFFER_SIZE_MB=4198            # 4.1 GB per MR (customer shape)
INITIATOR_THREADS=8            # modest — we care about connect churn, not throughput
REQUESTS_PER_THREAD=4          # 4 batches per iter = fast turnaround
LOG_DIR=./logs/<timestamp>
```

Exits when initiator finishes its `DRIFT_ITERS` iterations or hits
5 consecutive failures. Results land in `$LOG_DIR/`:

```
initiator.log       # full stdout/stderr of customer_pattern
targetA.log
targetB.log
drift.csv           # iter,peer,open_ms,warmup_ms,burst_ms,ok,fail,status
summary.txt         # derived from drift.csv
```

## Files

- `start_target.sh` — one target launch, writes peer file
- `restart_target_loop.sh` — loops kill+start_target
- `run_initiator.sh` — waits for both peer files, launches initiator
- `orchestrate.sh` — local driver that SSHes into all three
- `parse_results.sh` — produces summary from `drift.csv`
