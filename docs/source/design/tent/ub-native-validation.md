# TENT Native UB Hardware Validation Guide

This guide validates the TENT-native UB transport on machines with a real
UMDK/URMA provider. It covers build linkage, two-node READ/WRITE correctness,
multi-rail scheduling, fault recovery, retryable shutdown, and a performance
baseline. The repository's CI uses an injected adapter; the hardware cases in
this guide are therefore required before declaring native UB production-ready.

## 1. Record the Test Environment

Run every case on the same Mooncake commit and record the following on both
hosts:

```bash
git rev-parse HEAD
uname -a
gcc --version | head -1
cmake --version | head -1
urma_admin -l
ldconfig -p | grep liburma
```

The two hosts must have mutually reachable EIDs and TCP reachability for the
TENT P2P control plane. Use the vendor-supported tool to verify each active UB
port, for example:

```bash
urma_admin -p urma0
```

If UMDK is installed outside the system search paths, export its library path
before configuring and running Mooncake:

```bash
export LD_LIBRARY_PATH=/path/to/umdk/lib:${LD_LIBRARY_PATH}
```

Unset `MC_TENT_CONF` for the commands below unless the referenced file
explicitly enables `transports/ub`. The environment config replaces, rather
than merges with, the configuration assembled by `tebench`.

```bash
unset MC_TENT_CONF
```

## 2. Build and Verify Real URMA Linkage

Configure a Debug build first. CMake discovers standard UMDK installations
automatically; pass `URMA_SYSTEM_INCLUDE_DIR` and `URMA_LIBRARY` only for a
non-standard installation.

```bash
cmake -S . -B build-ub-debug \
  -DUSE_TENT=ON \
  -DUSE_UB=ON \
  -DUSE_CUDA=OFF \
  -DBUILD_UNIT_TESTS=ON \
  -DBUILD_BENCHMARK=ON \
  -DCMAKE_BUILD_TYPE=Debug

cmake --build build-ub-debug -j"$(nproc)" --target \
  tent_ub_core_test \
  tent_ub_teardown_test \
  tent_ub_native_data_path_test \
  tent_transport_selector_test \
  tebench
```

Confirm that CMake found a real library and that `tebench` has a runtime
dependency on it:

```bash
grep '^URMA_LIBRARY:' build-ub-debug/CMakeCache.txt
ldd build-ub-debug/mooncake-transfer-engine/benchmark/tebench | grep urma
```

Both commands must show the intended UMDK installation. A header-only fallback
is sufficient for mock tests but **not** for hardware validation.

Run the deterministic mock suite before touching the devices:

```bash
ctest --test-dir build-ub-debug --output-on-failure \
  -R 'tent_(ub_core|ub_teardown|ub_native_data_path|transport_selector)_test'
```

Expected result: all selected tests pass. In particular, the teardown test
must demonstrate that a failed unregister/delete retains ownership and that a
second cleanup call releases the same handle. It also verifies that a failed
device unpublishes every old endpoint and cannot recover until every JFC has
reported healthy in the current failure epoch.

## 3. Two-Node Correctness

Use `HOST_A` as the target and `HOST_B` as the initiator. The examples use a
fixed control-plane port so peer-restart tests can reuse the same segment name.

On `HOST_A`:

```bash
build-ub-debug/mooncake-transfer-engine/benchmark/tebench \
  --backend=tent \
  --metadata_type=p2p \
  --rpc_server_port=18001 \
  --xport_type=ub \
  --seg_type=DRAM \
  --total_buffer_size=1073741824 2>&1 | tee target-ub.log
```

Copy the printed segment name. On `HOST_B`, run a consistency-checked mixed
transfer first:

```bash
TARGET_SEG='<segment printed by HOST_A>'

build-ub-debug/mooncake-transfer-engine/benchmark/tebench \
  --backend=tent \
  --metadata_type=p2p \
  --target_seg_name="${TARGET_SEG}" \
  --xport_type=ub \
  --tent_transport_hint=ub \
  --seg_type=DRAM \
  --op_type=mix \
  --check_consistency=true \
  --total_buffer_size=1073741824 \
  --start_block_size=4096 \
  --max_block_size=1048576 \
  --start_batch_size=1 \
  --max_batch_size=8 \
  --start_num_threads=1 \
  --max_num_threads=4 \
  --duration=10 2>&1 | tee initiator-mix.log
```

Then repeat with `--op_type=read` and `--op_type=write`. All rows must finish,
consistency checking must report no mismatch, and neither host may log a
fallback to a non-UB transport. This covers both upward-facing TENT APIs and
native URMA READ/WRITE without converting requests to the Classic TE path.

## 4. Multi-Device and Pressure Scheduling

Leave `transports/ub/device_filter` empty so every discovered active EID is
eligible. Confirm that the startup topology lists at least two `ub:<device>:eid`
entries on each host, then run a fixed high-concurrency case:

```bash
build-ub-debug/mooncake-transfer-engine/benchmark/tebench \
  --backend=tent \
  --metadata_type=p2p \
  --target_seg_name="${TARGET_SEG}" \
  --xport_type=ub \
  --tent_transport_hint=ub \
  --seg_type=DRAM \
  --op_type=write \
  --total_buffer_size=1073741824 \
  --start_block_size=65536 \
  --max_block_size=65536 \
  --start_batch_size=16 \
  --max_batch_size=16 \
  --start_num_threads=16 \
  --max_num_threads=16 \
  --duration=60 2>&1 | tee initiator-multirail.log
```

Monitor per-device traffic with the platform's supported counters. Every
healthy eligible rail should make progress; saturating one path must not stop
posting on another path with free quota. Record aggregate bandwidth and the
per-device byte deltas.

## 5. Fault and Recovery Cases

Run these cases with a long-lived embedding harness that keeps the initiator's
TENT engine alive across each fault. Preserve both host logs, note exact fault
timestamps, and record endpoint generations before and after recovery. A new
`tebench` process may be used for basic reconnect smoke testing, but it is not
sufficient evidence for generation isolation.

### Peer restart and endpoint rebuild

1. Stop `HOST_A` with `SIGINT` while transfers are active.
2. Start it again with the same `--rpc_server_port=18001` command.
3. In the still-running `HOST_B` harness, discover the republished segment and
   submit READ and WRITE again without reinstalling the local TENT transport.

The old endpoint generation must not be reused. The live engine must establish
a new generation and complete new transfers; stale completions must not change
a new task. Save both generation values as part of the PASS evidence.

### Remote segment republish

Using the embedding application's TENT API, unregister and re-register the
target buffer at the same virtual address while the engines remain alive.
Publish the returned `BufferDesc`, then submit another READ and WRITE. The new
metadata generation must be imported and the stale imported segment must be
released. Do not use `tebench` for this case because it registers its buffers
only once at process startup.

### UB port down/up

1. Disable one UB port using the deployment's vendor-approved administration
   procedure; do not power-cycle a host as the first test.
2. Confirm that traffic continues on another healthy rail.
3. Re-enable the port and wait at least the configured endpoint cooldown.
4. Confirm that the recovered rail resumes traffic and that a new endpoint is
   built before posting.

The current recovery loop first unpublishes every endpoint backed by the failed
device. It reactivates the context only after every provider JFC has polled
successfully in the latest failure epoch, the cooldown has elapsed, and all old
WRs have drained. This supports a transient port/link failure when the provider
keeps Context/JFC handles valid. A destructive device reset that invalidates
those handles still requires transport reinstall and must be reported as
unsupported for in-place recovery.

### Cancellation and timeout

Exercise cancellation through the embedding application's TENT API while a
large multi-slice transfer is active. For timeout testing, drop the UB path
without stopping the control-plane TCP connection. The transfer must either
complete on another rail or reach a terminal timeout/failure; it must not hang
and shutdown must still drain safely.

### Transport fallback

The `tebench --xport_type=ub` commands above deliberately disable every other
transport, so they validate UB path failover but **cannot** validate transport
fallback. Use an embedding test harness with both UB and TCP (or RDMA) enabled,
and a selector policy that prefers UB but permits the second transport. Submit
the request with `transport_hint=UNSPEC`; do not pin it to UB.

1. Verify the first consistency-checked READ and WRITE select UB.
2. Disable every usable UB path while requests remain active.
3. Verify the runtime reaches a terminal result for the failed UB attempt and
   resubmits through TCP/RDMA rather than leaving the task pending.
4. Restore UB, wait for its cooldown, and verify later policy-selected traffic
   can use UB again.

Capture selector/failover logs or transport metrics that identify both the
failed UB attempt and the replacement transport. A successful copy without
this evidence does not prove transport fallback.

## 6. Repeated Shutdown and Sanitizers

Configure a separate sanitizer build:

```bash
cmake -S . -B build-ub-asan \
  -DUSE_TENT=ON \
  -DUSE_UB=ON \
  -DUSE_CUDA=OFF \
  -DBUILD_UNIT_TESTS=ON \
  -DBUILD_BENCHMARK=ON \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS='-fsanitize=address,undefined -fno-omit-frame-pointer' \
  -DCMAKE_EXE_LINKER_FLAGS='-fsanitize=address,undefined' \
  -DCMAKE_SHARED_LINKER_FLAGS='-fsanitize=address,undefined'

cmake --build build-ub-asan -j"$(nproc)" --target tebench
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1
export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
```

Use an embedding test harness to perform at least 100
install-register-transfer-unregister-uninstall cycles **in one process**.
Include ten cycles where a UB port or peer is unavailable during uninstall. If
unregister or uninstall returns a provider `busy`/error, keep the engine alive
and retry the same cleanup operation; the retry must act on the retained native
handle and eventually succeed after the fault is removed. Also run 100
target/initiator start-transfer-`SIGINT` process cycles as a supplementary
check.

A cycle is successful only if shutdown returns, the next cycle can reuse the
same buffers and control-plane port, and ASan/UBSan reports no UAF, double free,
or leaked native ownership graph. Record the first and retry return codes plus
the provider log around every injected cleanup failure.

## 7. Performance Baseline

Use a `RelWithDebInfo` build and keep CPU affinity, NUMA placement, block size,
batch size, concurrency, UMDK version, and device set fixed. Record:

| Case | Required measurements |
| --- | --- |
| 4 KiB, batch 1, one thread | average, P99, and P999 latency |
| 64 KiB and 1 MiB, one thread | single-stream bandwidth and CPU usage |
| 64 KiB and 1 MiB, 16 threads | aggregate bandwidth and CPU usage |
| Multi-device, 16 threads | aggregate and per-device bandwidth |
| First transfer to a peer | endpoint connection time |

Run the same matrix against Classic TE UB using the existing Classic UB
benchmark on the same hosts. Any material regression needs a profile and an
explanation; do not compare results from different UMDK versions or topology.

## 8. Result Checklist

Attach the following to the review:

```text
Mooncake commit:
Host/OS/kernel:
UMDK package and liburma path:
Devices/EIDs:
Debug USE_TENT+USE_UB build: PASS/FAIL
Mock UB tests: PASS/FAIL
Two-node READ: PASS/FAIL
Two-node WRITE: PASS/FAIL
Consistency-checked mix: PASS/FAIL
Multi-device pressure scheduling: PASS/FAIL
Peer restart / endpoint rebuild: PASS/FAIL
Segment republish: PASS/FAIL
Port down/up: PASS/FAIL
Cancellation and timeout: PASS/FAIL
UB to TCP/RDMA transport fallback: PASS/FAIL
100-cycle ASan/UBSan shutdown: PASS/FAIL
Classic UB baseline artifact:
TENT UB baseline artifact:
Known provider errors or unsupported cases:
```

Do not mark the hardware gate complete from bandwidth alone. Correctness,
generation isolation, retryable resource release, and sanitizer-clean shutdown
are independent acceptance requirements.
