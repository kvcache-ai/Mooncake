# P01 codec evidence, 2026-09-16

Scope: roadmap #3808 P01, directly based on asynchronous W02 PR #4141 at
`e92af584fa106668c3e311b66e1592bd8922b42b`. No production source, writer selection,
or persisted production format is changed. P02/P03 remain conditional.

## Protocol and environment

Apple M5, arm64, macOS 26.6.1. Release `-O3 -DNDEBUG`, AppleClang
21.0.0.21000101. JsonCpp 1.9.8, nlohmann/json 3.12.0, xxHash 0.8.3,
glog 0.7.1, Folly 2026.07.27.00_1, msgpack-cxx 9.0.0 headers, etcd 3.7.1
(Go 1.26.5). Candidate MessagePack encoding uses nlohmann, not msgpack-cxx.
The timed executable does not link the Go etcd wrapper.

Two passes each measure 11 workloads × four formats × eight repetitions:
352 raw samples per pass, 704 total. Each sample has five warmups followed by
50 timed encodes and 50 timed decodes. Format order rotates, balanced over eight
repetitions. The CSV records bytes, process CPU, wall time, and codec batches/s.
The summaries report medians, min/max CPU, production-relative ratios, and
same-library JSON-control ratios. Passes remain separate because repetition IDs
restart at zero. Pass 1 preceded four additional checksum-only validation checks;
the timed codec and workload implementation is identical in both passes.

The shared host is not CPU-isolated, frequency-pinned, or thermally controlled.
Sample min/max is not a confidence interval. These observations do not establish
a fine-grained CBOR/MessagePack ranking or service-throughput improvement.

## Validation

- Updated W02 unit tests: 50/50 passed.
- Same W02 tests under ThreadSanitizer: 50/50 passed, no race report.
- Final codec validation: 269 checks passed, including roundtrip, Unicode,
  full uint64, payload bytes, shape/type/range, well-formed wrong checksum,
  corruption/truncation, and production semantic replay.
- Real etcd E2E: 8/8 passed, four formats × persist/server restart.
- Scoped pre-commit and independent clang-format 20 validation passed.

Each E2E case audits all 11 acknowledged lifecycle operations in two actual
W02 batches. JSON/control totals 1,659 stored bytes, CBOR 1,226, MessagePack 1,229.
Checks include pending asynchronous durable futures before the transaction,
reentrant completion, ready restored-prefix futures, stale-producer fencing
without batch/prefix visibility, and corrupted-record rejection.

Before and after restart, actual OpLogApplier/StandbyMetadataStore replay verifies
exact object metadata, tenant isolation, upsert, remove/revoke, Segment
mount/update/unmount, replica identity, replay cursor, and duplicate idempotence.
These schema-generated payloads are not captured production traffic. Descriptor
memory addresses are fixtures; serving buffers/resources are not rebuilt.

## Exact size: generated production-schema payloads

| Profile | Entries | JSON/control bytes | CBOR bytes | MessagePack bytes | Reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| PUT, 2 replicas | 32 | 7,789 | 5,905 | 5,905 | 24.19% |
| PUT, 8 replicas | 256 | 173,166 | 130,693 | 130,693 | 24.53% |
| PUT, 2 replicas | 1,024 | 253,595 | 192,275 | 192,275 | 24.18% |
| Segment descriptors | 256 | 27,066 | 20,092 | 20,092 | 25.77% |
| Lifecycle trace | 11 | 1,636 | 1,215 | 1,218 | 25.73% / 25.55% |

These are outer batch sizes, not object data sizes. The lifecycle measurement
uses one batch; E2E uses two writer-chosen batches with an extra envelope.

## CPU: same-library control

JSON-control shares nlohmann and the candidate envelope/checksum pipeline but
retains base64 payloads. Its wire exactly matches production JSON for all generated
workloads and the Unicode/uint64 case. This controls library choice, not every
implementation detail: JsonCpp separately reconstructs its checksum body whereas
the common candidate implementation reuses it. Production-relative CPU ratios
also include library/implementation differences.

Final-pass median CPU, generated PUT metadata with 32 objects/two replicas:

| Implementation | Encode µs/batch | Decode µs/batch | Encode ratio vs control | Decode ratio vs control |
| --- | ---: | ---: | ---: | ---: |
| Production JsonCpp JSON | 165.94 | 147.08 | 0.52× | 0.80× |
| nlohmann JSON/base64 control | 85.61 | 117.75 | 1.00× | 1.00× |
| nlohmann CBOR | 14.28 | 56.43 | 6.00× | 2.09× |
| nlohmann MessagePack | 15.63 | 54.86 | 5.48× | 2.15× |

Across five generated-schema profiles, final-pass median ratios versus control
are 3.00–14.36× encode and 1.37–3.57× decode. Pass 1 observed 2.89–14.14× encode
and 1.44–3.55× decode. This supports an observed CPU benefit, not its magnitude
under production traffic or a format winner.

Example encode CPU ranges: 112.42–221.26 µs JSON, 56.56–125.10 µs control,
10.12–25.36 µs CBOR, 10.24–24.46 µs MessagePack. Decode ranges overlap between
implementations. Keep variability limits with the medians; do not claim
statistical significance or quote a single best run.

## Boundary and reproduction

The test-only backend bridge transcodes isolated fixture batch values while
reusing production storage transactions. Its extra transcoding is not timed.
RPC Put/Upsert, promotion/resource rebuilding, and full HA service E2E are not
covered. No candidate is selected or enabled for production.

See [the benchmark README](../../README.md) for build/test/sample commands.
`codec-samples.csv` / `codec-summary.json` are the final pass; `*-pass1.*` retain
the first pass. `build_metadata.json` pins the writer/header hashes;
`e2e-report.json` records the eight passing cases. Local temporary paths and
loopback endpoints are omitted from the published metadata.
