(tent-testing)=
# TENT Testing

TENT's runtime does not inspect how a byte moved. It decides from a small
`Transport` contract: capabilities, a submit `Status`, and a
`TransferStatus` (`PENDING` / `COMPLETED` / `FAILED`). That is the
opening for FakeTransport: an in-memory backend that speaks the same
contract, moves no bytes, and lets tests drive the real
`TransferEngineImpl` with deterministic faults.

This page is about that mechanism. Individual test names and binaries
change; the contract does not. Transport-layer tests (RDMA work-request
errors, NVLink, GPU registration) stay with each real backend and need
hardware.

## Why a Fake Transport Works

Correctness splits into two layers:

1. **Transport-layer** — how a backend moves bytes and reports hardware
   faults. Owned by each `Transport`.
2. **Runtime-layer** — failover, batch lifecycle, progress worker,
   shutdown. Owned by `TransferEngineImpl` and `ProxyManager`. The
   runtime never looks at DMA; it only consumes the contract above.

A real transport produces those values as a side effect of moving data.
A fake that returns the same values at the same times drives
`submitTransfer`, `getTransferStatus`, `resubmitTransferTask`,
`progressBatch`, and shutdown identically. Failures that are hard to
stage on hardware — a completion that never arrives, the Nth submit
failing, a batch still `PENDING` when memory is freed — become a
controlled return value.

The production runtime is not stubbed. Tests wrap or replace the
transport slots, then exercise the engine as an application would.

```
Test
  |
  v
TransferEngineImpl / ProxyManager     (unmodified production code)
  |
  +-- swapTransportForTest(...)
  |
  v
[optional] fault-injecting decorator
  |
  v
FakeTransport                         (programmable Status, no DMA)
```

## The Transport Contract Tests Actually Use

FakeTransport implements `Transport` with the minimum surface the
runtime needs:

- **Capabilities.** Advertise `dram_to_dram` so the engine considers the
  slot available. Leave GPU capability bits off; FakeTransport tests
  register CPU buffers. The runtime will not route a CUDA request to a
  CPU-only fake.
- **Buffer tagging.** `addMemoryBuffer` records the fake's slot on
  `BufferDesc::transports`. Without that tag, `resolveTransport()` will
  not pick the swapped-in backend.
- **Submit.** Record the request and stamp a `TransferStatus`. Submit
  itself succeeds unless a decorator or subclass fails it.
- **Poll.** Return the stamped status, or override it from the poll
  count (for example `PENDING` for N polls, then `COMPLETED`).
- **Memory.** `malloc` / `free`. No pinning, no GPU registration, no
  DMA. `warmupMemory` declines so the engine uses its own warmup path.

Two optional hooks cover most status control without subclassing:

- stamp a status at submit time
- override the status on each `getTransferStatus`, given the poll count

Call counts on install / submit / poll / register are how a test proves
the runtime took a path (for example primary submitted once, secondary
submitted after failover) without looking inside the engine.

Copies of FakeTransport live in the test files that need them, not in a
shared header. Each test can extend its copy; divergence is expected.
The point of the fake is the contract, not a single implementation.

## Fault Injection Is a Decorator

FakeTransport, by itself, completes successfully. Faults are layered on
top so the engine still sees an ordinary `Transport`:

| Layer | Role |
|-------|------|
| FakeTransport | In-memory success path; programmable completions. |
| `FaultProxyTransport` | Wraps any `Transport` and injects policy-driven faults: failed submit, `COMPLETED → FAILED` on poll, failed `install()`, optional delay. |
| Test-local subclass | When the fault is not a policy (poison only some requests; flip an atomic mid-loop). |

The engine's failover and retry paths run unmodified. The decorator is
what makes a completion-stage `FAILED` look like a WC error or a dropped
peer, without a verbs layer.

`FaultProxyTransport` lives under
`mooncake-transfer-engine/tent/transport/fault_proxy/`. Its policy
fields are the source of truth; this page does not duplicate them.

## Installing Fakes Without Bypassing the Runtime

`TransferEngineImpl::swapTransportForTest` replaces one slot in
`transport_list_` after `construct()`. That is the only test hook that
installs a fake (or a wrapped fake) while leaving `resolveTransport()`
and `resubmitTransferTask()` on the production path. Production code
never calls it.

A typical setup:

1. Build a config with `p2p` metadata on loopback and **real transports
   disabled**, so `construct()` does not require Redis, etcd, or an RDMA
   device.
2. Construct the engine.
3. `install()` the fake (or proxy), then `swapTransportForTest` into the
   slots under test. Swap only replaces the slot; it does not re-run
   engine availability.
4. Register memory *after* the swap, so `addMemoryBuffer` tags the
   buffers.
5. Submit and poll through the public engine API.

Install two slots when the scenario needs a fallback (typically RDMA +
TCP). Install one slot when the test wants no failover.

`USE_CUDA=OFF` is required on CPU-only hosts. With CUDA enabled and no
GPU, pointer queries fail, memory type becomes unknown, every transport
looks unavailable, and `resolveTransport()` returns `UNSPEC` before any
injected fault runs.

## What the Mechanism Can Prove

FakeTransport proves how the runtime *reacts* to the transport contract.
It cannot prove that a backend moved the right bytes.

**In scope.** Cross-transport failover and budget exhaustion; poll vs
`progressBatch` recovery; progress-worker and `freeBatch` races; queue /
hint / metrics behavior that only depends on submit and poll results;
concurrency of runtime maps that FakeTransport can reach.

**Out of scope.** DMA integrity; verbs-level failures (WC error, QP
failure); GPU registration; NVLink link faults; true async RPC timing
(in-process callbacks fire synchronously). Those belong to each
transport's tests, `tebench --check_consistency=true` on real peers, or
multi-node hardware CI.

(staging-trigger-constraints)=

**Staging is decided before the request reaches a transport.**
`findStagingPolicy()` inspects which hardware backends are installed and
what memory types the request uses, then `ProxyManager` runs the staging
loop. Swapping FakeTransport into RDMA or TCP does not change that
decision, and every current trigger is gated on NVLink, MNNVL, or TPU —
none of which a CPU-only fake replaces. A CPU-only host therefore cannot
exercise `transferEventLoop` with either real transports or fakes.
Covering that path is a hardware-runner problem, or a product change to
the staging policy, not a missing FakeTransport feature.

## Running

```bash
cmake -S . -B build-tent \
  -DUSE_TENT=ON -DUSE_CUDA=OFF \
  -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF \
  -DBUILD_UNIT_TESTS=ON -DCMAKE_BUILD_TYPE=Release

cmake --build build-tent --target mooncake_common -j
ctest --test-dir build-tent/mooncake-transfer-engine/tent/tests \
  --output-on-failure
```

`tent_link_group` links `mooncake_common` by archive path, which does
not create a CMake build-order dependency — build `mooncake_common`
first. The `tent-ci` `cuda-off` legs in GitHub Actions run the same
`ctest` directory. The `cuda-on` leg compiles only: runners have no GPU,
and CUDA stubs would bypass the fakes.

Concurrency tests that touch shared runtime maps should also be run
under ThreadSanitizer (`-fsanitize=thread`). Which binaries those are
belongs in the test sources, not here.
