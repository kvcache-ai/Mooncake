# Address order and receiver prefetch: negative results

**Still below 400 GB/s.** The recommended repeated result remains **281.074
GB/s** versus the same-round 278.003 baseline (+1.10%, 3 x 20 seconds).
Matched-footprint cyclic perftest was 276.219 GB/s. The tests below do not improve
that result and are not enabled in the recommended configuration.

A -> B CPU DRAM Write; one TE process, eight submission threads, **1 GiB
independent payload/thread**, eight NICs, 16 authorized logical CPUs/host.
Each NUMA node registers 8 GiB (16 GiB/host); active payload is 8 GiB/host.
A 32-request window bounds in-flight bytes, not the address range.
Progress messages use unused registration tails and are excluded from payload
GB/s. Source prefetch remains off. DDIO/BIOS/CAT/global THP/firmware unchanged.

## Code, commands and results in order

These are single screening runs (10 seconds unless stated), not repeated gains.
Do not subtract values from different rounds or add step gains.
Use this directory's `run_te.sh`; the parent script deliberately clears these
additional experimental environment variables.

| Step | Code change | Environment on both hosts unless noted | Payload GB/s |
| --- | --- | --- | ---: |
| 0 | Previous final binary, no new changes | Parent `CONFIG=fast` | Address-round fresh baseline: 281.201 (20 s) |
| 1 | `address.patch`: coprime chunk walk + starting phase; full coverage and live-chunk exclusion preserved | `MC_EXPERIMENTAL_CHUNK_STRIDE=17 MC_EXPERIMENTAL_PHASE_CHUNKS=83` | 280.112; unchanged walk: 281.258 |
| 2 | Same patch: optional per-thread source/target padding, checked registration bounds | `MC_EXPERIMENTAL_TARGET_THREAD_GAP=4096` | 279.086; all padding/walk combinations: 279.086–280.760 |
| 3 | `progress.patch`: 64-byte progress Write after each 512 KiB payload chunk, per-slot source ownership; B polls and prefetches future chunks | A: `MC_EXPERIMENTAL_PROGRESS_PREFETCH=1`; B: `=0` (hint-only control) | 192.893; same binary disabled: 279.358 |
| 4 | Enable eight receiver helpers; private, disjoint cursor addresses derived from submission thread ID, not HCA enumeration index | Both: progress=1; `MC_EXPERIMENTAL_PROGRESS_STRIDE=256 MC_EXPERIMENTAL_PROGRESS_AHEAD=16 MC_EXPERIMENTAL_PROGRESS_SPAN=2` | 192.926; strides128/512: 193.136/192.981 |
| 5 | `coarse.patch`: send progress only once per 16 chunks (8 MiB), safely remove stale control descriptors on reused slots | Both: `MC_EXPERIMENTAL_PROGRESS_EVERY=16`; A progress=1, B progress=0 | 280.241; same binary disabled: 280.983 |
| 6 | Enable helpers with coarse progress, same complete payload range | Both: progress=1, every=16, stride=512, ahead=32, span=16 | 271.302; all coarse helper settings: 262.152–271.302 |
| 7 | Control: B helpers spin without incoming progress | A progress=0; B progress=1 | 280.689; no improvement over disabled 280.983 |

`progress`, `every`, `stride`, `ahead`, `span` in the table abbreviate
`MC_EXPERIMENTAL_PROGRESS_PREFETCH`, `_EVERY`, `_STRIDE`, `_AHEAD`, `_SPAN`.
All 32 successful timing cases and exact per-host overrides are in
[results.json](results.json). It includes binary SHA256 hashes and which cases
ran full write-seed/read-verify. No new perftest or CPU profile is claimed here;
those comparisons remain from the preceding repeated round.

## Reproduce

1. Restore a disposable historical notify-stage source using the preceding
   handoff archive, then apply the parent `pin`, `fast`, `source-prefetch` series.
   Do **not** apply these patches to the PR's newer production source.
2. Run `bash apply.sh /absolute/archived-source coarse` from this directory.
   Use `address` or `progress` as the last argument for intermediate binaries.
   Input hashes are checked before patching; coarse final hashes are checked
   afterward. Build a separate runtime per stage and deploy identical binaries
   to both hosts. The earlier Linux build recipe uses Release, classic CPU,
   `USE_MLX5DV=ON`, `USE_TENT=OFF`, `USE_CUDA=OFF`, `BUILD_BENCHMARK=ON`.
3. Reserve idle resources; set runtime/IP/CPU variables and authorized NUMA-local
   `NIC_ORDER`, `SUBMIT_CPUS`, `WORKER_CPUS` as in the parent guide. These commands
   reuse the same eight submit + eight worker CPU budget. B helper threads run
   on its eight submit CPUs. Verify GID index3 matches the new environment.
4. Example for step6, set these variables on **both** hosts:

```bash
export CONFIG=fast
export MC_EXPERIMENTAL_PROGRESS_PREFETCH=1
export MC_EXPERIMENTAL_PROGRESS_EVERY=16
export MC_EXPERIMENTAL_PROGRESS_STRIDE=512
export MC_EXPERIMENTAL_PROGRESS_AHEAD=32
export MC_EXPERIMENTAL_PROGRESS_SPAN=16
# B: start target and copy its complete TARGET_SEG value.
bash run_te.sh target "$RUNTIME_COARSE" "$B_CPUS" "$B_IP"
```

On A, with the same exported settings:

```bash
bash run_te.sh initiator "$RUNTIME_COARSE" "$A_CPUS" "$A_IP" "$TARGET_SEG" 16 4096 1 write_seed
bash run_te.sh initiator "$RUNTIME_COARSE" "$A_CPUS" "$A_IP" "$TARGET_SEG" 16 4096 1 read_verify
RESULT_JSONL=coarse-r1.jsonl bash run_te.sh initiator "$RUNTIME_COARSE" "$A_CPUS" "$A_IP" "$TARGET_SEG" 16 4096 10 write
```

For step5, override `MC_EXPERIMENTAL_PROGRESS_PREFETCH=0` **on B only**.
For disabled control, set it to0 on both hosts. Restart only this task's target
when changing settings/runtime. Clear unrelated walk/gap/progress flags between
cases. Step1/2 use `RUNTIME_ADDRESS`; step3/4 use `RUNTIME_PROGRESS` and omit
`MC_EXPERIMENTAL_PROGRESS_EVERY`. Compare controls before/after each candidate;
use 3 x 20 seconds and a sustained run before asserting an improvement.

## Validation and failure record

- Actual rolling/mapping methods passed CPU-only ASan/UBSan tests: complete
  coverage, no live overlap, descriptor reuse, payload/control accounting,
  Read without hints, bounds, failed submissions and stop/drain.
- Linux stage builds completed; identical binary hashes on both hosts. Successful
  timing checks: 14 address-round +18 progress-round cases; full-payload
  seed/read files:12 +10. All eight NICs active, completed bytes and full 1 GiB
  coverage checked, authorized affinity and standard RDMA error counters checked.
- Initial progress v1 incorrectly equated internal HCA index with `mlx5_N`.
  A range-equality guard aborted write_seed before timing. The corrected patch
  derives NUMA/rank from submission thread ID and tests all HCA index mappings.
  The failed version is excluded from the timing table and patch chain.
- All task processes exited; existing service identities unchanged, memory
  failcnt delta0, reservations released. Deferred-cleanup warnings occurred;
  successful process/correctness checks are not proof of zero internal warnings.
- These are measured historical experimental patches, not a Linux/RDMA test of
  the current PR head. Progress is a speculative prefetch hint, not payload
  completion or a CPU-reader synchronization protocol. Only DSW VF access was
  available; these results neither verify DDIO state nor establish its role as
  the sole bottleneck or the hardware bandwidth ceiling.
