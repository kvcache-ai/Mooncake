# Master service scenario DSL

`dsl/` contains the typed `MasterScenario` vocabulary used to describe
client-visible behavior of methods exported by the master RPC service. The test
harness invokes `MasterService` in process, while scenarios use the same
operations, inputs, results, and error codes that a client observes.

The DSL is intentionally limited to the client boundary:

- actions model exported RPC operations and make success the default;
- expected failures must name their `ErrorCode` explicitly;
- assertions use client-visible queries and results;
- private indexes, metrics, allocators, background-worker state, RPC adapter
  normalization, and subsystem invariants remain in focused direct tests.

Existing MasterService tests are migrated by component in follow-up changes.
Component-specific actions and deterministic concurrency helpers are introduced
with their first consumer and contract coverage. The foundation introduced here
does not reorganize or rewrite those suites.

## Private-state access

`master_service_test_peer.h` defines `mooncake::test::MasterServiceTestPeer`,
the only test friend of `MasterService`. Fixtures, the scenario DSL, and the
eviction benchmark use it instead of adding friends or public `ForTesting`
methods to the production service:

```cpp
#include "master_service/master_service_test_peer.h"

MasterServiceTestPeer peer(service);
peer.RunBatchEvictForTesting(0.8, 0.7);
```

Add new private inspection/mutation helpers to the peer. Nontrivial test-only
implementations live in `master_service_test_peer.cpp`; the
`master_service_test_peer` library is built only for tests or benchmarks and is
not linked into production targets. The Store test helpers link it automatically.

Raw state accessors do not take locks. Preserve the service's lock order and
existing synchronization when using them. Synchronous drivers must not race the
corresponding background worker. Runtime hook invocation points remain in the
production code; the peer only installs their callbacks.
