# ReadPlan Benchmark

`read_plan_bench.py` compares three ways to read object ranges through the Store
Python API. It requires a running Master and a Python package built with
`create_read_plan`; load the platform runtime environment before running it.
No model or SGLang installation is required.

| Mode | Execution |
| --- | --- |
| `legacy` | One session, with Python expanding ranges and reading each group |
| `plan_serial` | Native ReadPlan with sequential group reads |
| `plan_pipeline` | Native ReadPlan with a two-group read window |

All modes use the same keys, registered CPU destinations, and group order.
Each key contains `layers * layer_bytes` bytes; group `g` reads `layer_bytes`
bytes at source offset `g * layer_bytes`. Total bytes per trial are
`layers * keys * layer_bytes`. Destination ranges do not overlap.

## Run

For a small TCP smoke test, start a Master in another terminal:

```bash
build/mooncake-store/src/mooncake_master \
  --rpc_address=127.0.0.1 --rpc_port=50051
```

Then run from the repository root with the built Python package installed:

```bash
MC_STORE_MEMCPY=0 python3 mooncake-store/benchmarks/read_plan_bench.py \
  --master 127.0.0.1:50051 --hostname 127.0.0.1 \
  --protocol tcp --global-segment-bytes 67108864 \
  --layers 4 --keys 3 --layer-bytes 128 --warmup 1 --repeats 3
```

This smoke test provides capacity in the reader process. For performance
measurements, use a separate Store owner with sufficient capacity and leave
`--global-segment-bytes=0` (the default). For example, with at least 8 GiB of
capacity on the owner, using the appropriate host and RDMA device:

```bash
MC_STORE_MEMCPY=0 python3 mooncake-store/benchmarks/read_plan_bench.py \
  --master HOST:50051 --hostname READER_IP --protocol rdma --devices DEVICE \
  --layers 43 --keys 5568 --layer-bytes 16657 --warmup 2 --repeats 9
```

That workload reads 3.714 GiB per trial and requires the same amount of registered
CPU destination memory. `--layer-bytes` is the per-key, per-group size, not the
complete object size. Use platform-specific transport settings consistently
across modes; the Hygon/SHCA reference measurement used
`MC_IB_PCI_RELAXED_ORDERING=0`. Ensure `MC_FORCE_TCP` is unset for RDMA runs.

Connection defaults also accept `MOONCAKE_MASTER`, `MOONCAKE_LOCAL_HOSTNAME`,
`MOONCAKE_PROTOCOL`, `MOONCAKE_DEVICE`, and `MOONCAKE_TE_META_DATA_SERVER`.
Metadata defaults to `P2PHANDSHAKE`. See `--help` for all options.

## Measurement and output

- The script rotates mode order per iteration and excludes warmups from
  summaries. Every trial validates all destination bytes against key- and
  offset-dependent contents.
- Total read time includes executor scheduling, plan creation where applicable,
  session start/end, and reads. Fixture writes, registration, buffer reset, and
  content verification are outside the timed region. All modes publish the final
  group only after session cleanup.
- The terminal prints p50/p95 read latency, p50 first-group readiness, and speedup
  relative to the Python loop. These are read-path metrics, not model TTFT,
  model throughput, or the fraction of time spent in Python.
- `--output` selects the parent directory for a unique run directory containing
  `summary.json` (configuration, relevant environment, raw trials, per-group
  waits, and summaries) and `fixture_keys.json` (owned keys).
- `--consumer-ms` adds synthetic sleep after each group; it defaults to zero and
  does not simulate GPU computation. Observed readiness is when the consumer
  returns from `wait()`, not the underlying transport completion timestamp.
- The script joins reads before releasing buffers and removes only its UUID
  keys. Cleanup failures produce a nonzero exit and are recorded in the report.
  An externally terminated run may leave keys listed in `fixture_keys.json`.

Record the build revision, topology, device, and CPU/NUMA affinity with any
published result. A same-node run does not establish cross-node performance;
a small sample count does not establish stable tail latency. The benchmark
has no fixed speedup assertion and is not registered as a CTest performance gate.

A layout/data self-check can run without a Store installation:

```bash
python3 mooncake-store/benchmarks/read_plan_bench.py --self-test
```
