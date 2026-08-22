# Master service scenario DSL

`dsl/` contains the typed `MasterScenario` vocabulary used to describe
client-visible behavior of methods exported by the master RPC service. The test
harness invokes `MasterService` in process, while scenarios use the same
operations, inputs, results, and error codes that a client observes.

The DSL is intentionally limited to the client boundary:

- actions model exported RPC operations and make success the default;
- expected failures must name their `ErrorCode` explicitly;
- assertions use client-visible queries and results;
- deterministic helpers may prepare storage tiers or coordinate concurrent
  clients, but do not expose arbitrary callbacks;
- private indexes, metrics, allocators, background-worker state, RPC adapter
  normalization, and subsystem invariants remain in focused direct tests.

Existing MasterService tests are migrated by component in follow-up changes.
The foundation introduced here does not reorganize or rewrite those suites.
