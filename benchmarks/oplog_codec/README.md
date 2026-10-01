# P01 codec evidence on W02

This standalone benchmark extends W02 PR #4141 at
`e92af584fa106668c3e311b66e1592bd8922b42b` (asynchronous
`async_simple::Future<ErrorCode>`), not its earlier blocking implementation.
It implements the P01 experiment in roadmap #3808. No production source,
writer selection, or persisted production format is changed.

## Build

Use a Release build and the checkout's pinned yalantinglibs dependency.
Install JsonCpp, xxHash, glog, Folly, etcd, msgpack-cxx headers, and
nlohmann/json **3.12.0**.
`ETCD_WRAPPER_DIR` must contain the Go etcd wrapper built from this checkout,
including `libetcd_wrapper.h` and its shared library.

```bash
cmake -S benchmarks/oplog_codec -B /tmp/mooncake-p01-build \
  -DCMAKE_BUILD_TYPE=Release \
  -DYLT_INCLUDE_DIR=/path/to/yalantinglibs/include \
  -DETCD_WRAPPER_DIR=/path/to/etcd-wrapper \
  -DNLOHMANN_JSON_INCLUDE_DIR=/path/to/nlohmann-include
cmake --build /tmp/mooncake-p01-build -j2
ctest --test-dir /tmp/mooncake-p01-build --output-on-failure
/tmp/mooncake-p01-build/oplog_codec_bench 50 8 > /tmp/p01-codec.csv
python3 benchmarks/oplog_codec/summarize.py < /tmp/p01-codec.csv > /tmp/p01-summary.json
python3 benchmarks/oplog_codec/run_e2e.py \
  --build-dir /tmp/mooncake-p01-build \
  --wrapper-dir /path/to/etcd-wrapper \
  --output-dir /tmp/p01-e2e
```

For an etcd build that requires explicit ARM enablement, prefix the runner
with `ETCD_UNSUPPORTED_ARCH=arm64`. Its isolated server is always stopped;
the temporary data directory is retained and recorded for inspection.

## Experiment contract

- JSON calls the actual production `EncodeOpLogBatchRecord` and
  `DecodeOpLogBatchRecord`, including canonical base64 and JSON checksum work.
- Experimental CBOR and MessagePack use the same logical six-element envelope
  and entries, but carry payloads as binary bytes. They checksum the canonical
  five-element body with xxHash32 and use the production entry/shape validators.
  They are **not** supported Mooncake wire formats, mixed-history readers, or
  proposals to deploy a binary writer. P02/P03 remain separate conditional work.
- `json-control` uses the same nlohmann library and envelope/checksum pipeline
  as the binary candidates, but retains base64 payloads. Its bytes must exactly
  match production JSON for every generated workload and the Unicode/uint64
  edge case. This controls the library choice, not every implementation detail:
  production JsonCpp reconstructs the body separately while the common candidate
  implementation reuses its body. Both production-relative and control-relative
  CPU ratios are reported; neither is a service speedup.
- The six deterministic, synthetic workloads vary batch count (1–1024), key
  length, and opaque payload size (128 B–64 KiB), including all byte values.
  Five additional generated production-schema profiles use actual struct_pack
  `MetadataPayload`/Segment operation serialization, varying tenant, replica
  count, memory/disk descriptors, and batch size. The lifecycle trace covers
  PUT_END/upsert, REMOVE, PUT_REVOKE, and Segment mount/update/unmount. These are
  schema-generated fixtures, not captured production traffic. Results must be
  confirmed with real workload distributions before selecting a production
  binary format.
- CSV reports exact payload/wire bytes, process CPU microseconds per batch,
  wall microseconds per batch, and batches/second. Every format has five warmup
  iterations. Format order rotates between repetitions. Build optimization,
  host architecture, library versions, iteration counts, and raw repeated
  samples belong in the result artifact; do not compare Debug to Release.
  The timed executable does not link the etcd Go wrapper, so its background
  runtime cannot contribute to process CPU measurements.
  Production-relative CPU differences include library/implementation choices;
  use the same-library control to assess how much of that difference remains.
- Validation covers roundtrip equality against the production JSON projection,
  corruption/truncation, null destinations, full uint64 ranges, binary payloads,
  and invalid candidate version, enum, tenant, field types, and sequence ranges.
  Like production batch JSON, timestamps/prefix hashes are not persisted and
  entry sequences are implicit in the batch cursor.

## Real-etcd E2E boundary

The fixture uses the actual W02 writer, `OpLogBatchStorage`,
`EtcdHaKvBackend`, and Go wrapper. A test-only backend bridge transcodes only
the isolated `/oplog/p01-w02-{format}/batches/` values; durable prefixes and
producer-view transactions remain production JSON/etcd operations.

For each format it gates the real transaction, registers a pending Future on
the calling thread without spawning a blocking waiter, proves the prefix is
not yet visible, then checks durable completion and a reentrant continuation.
It reads the candidate bytes directly from etcd. After restarting etcd on the
same data directory, fresh processes verify every persisted batch, a ready
restored-prefix Future, stale-producer fencing without prefix/batch visibility,
and corrupted-record rejection. Only the fixture's own bytes are fault-injected.
Before and after restart, the complete decoded trace is replayed through the
production `OpLogApplier` and `StandbyMetadataStore`: exact object metadata,
tenant isolation, deleted/revoked objects, Segment state, replay cursor, replica
IDs, and duplicate replay idempotence are checked. Memory addresses in descriptors
are fixtures; no data buffers are dereferenced or serving resources rebuilt.
The P02 gating notes (record retained locally)
separate codec/schema evidence from the serving-recovery validation required for
production rollout, including cases where descriptor semantics affect selection.

The bridge deliberately does extra JSON/binary conversions: its latency is
**not a binary storage-throughput benchmark**. This is storage correctness E2E,
not RPC Put/Upsert, standby promotion/resource rebuilding, or full HA service
E2E. A size/CPU gain here does not establish an end-to-end service improvement.

## Recorded evidence

See the 2026-09-16 report (record retained locally) for both repeated
raw sample passes, summaries, build metadata, and the eight-case E2E report.
