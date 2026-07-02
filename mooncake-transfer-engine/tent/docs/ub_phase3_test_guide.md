# TENT UB Transport Phase 3 Test Guide

This guide documents the tests for the `UB_TENT` branch. It is written against
the current code in this branch and calls out the functional differences from
`main` so that reviewers can map each test to the changed code.

`main` in this repository is `812eb8d67bbf83dc96d51013cdc2e8f9ec358b80`.
`UB_TENT` is `7604e53a187e0c5e52076f250a92697b5d1cff45`.

## 1. Scope Compared With `main`

`UB_TENT` adds the TENT adapter for Kunpeng UB/URMA and also adjusts the
existing old Transfer Engine UB implementation so that it can be reused by
TENT.

| Area | `main` | `UB_TENT` |
|---|---|---|
| TENT transport enum | No `UB` transport type | Adds `TransportType::UB`, Python binding, and `"ub"` string mapping |
| Transport loading | TENT cannot instantiate UB | `transport_loader` creates `UbTentTransport` when `USE_UB=ON` and `transports/ub/enable` is true |
| Selector | Policy strings cannot select `"ub"` | `TransportSelector` parses and prints `"ub"` and can route policy entries to UB |
| Metadata bridge | UB only uses old-TE `TransferMetadata` | Adds `UbTentMetadataBridge` to convert TENT segment metadata into old-TE UB descriptors |
| Handshake | Old UB uses the old handshake path | Adds TENT `BootstrapUb` RPC and routes UB jetty exchange through TENT control plane |
| Local segment publishing | TENT memory segments do not carry UB-specific EID/tseg data | UB transport writes device EIDs and buffer tseg handles into TENT `transport_attrs` |
| Submit fallback | A submit failure with `UNSPEC` type did not get poll-time resubmit | Failed sub-batch submit resets `failover_count` and poll failover can resubmit |
| Old UB registration | Each context attempted to register the same VA | Primary context registers once; other contexts adopt the segment to avoid URMA duplicate registration |
| Tests | No TENT UB tests | Adds `tent_ub_transport_test` and `tent_ub_e2e_dual_node_test` |

Changed files to review when validating the branch:

- TENT UB adapter: `tent/src/transport/ub/*`,
  `tent/include/tent/transport/ub/*`
- TENT control-plane changes: `tent/include/tent/runtime/control_plane.h`,
  `tent/src/runtime/control_plane.cpp`, `tent/include/tent/rpc/rpc.h`
- TENT selection/loading changes: `tent/include/tent/common/types.h`,
  `tent/src/runtime/transport_selector.cpp`,
  `tent/src/runtime/transport_loader.cpp`,
  `tent/src/runtime/transfer_engine_impl.cpp`
- Tests: `tent/tests/ub_tent_transport_test.cpp`,
  `tent/tests/ub_e2e_dual_node_test.cpp`, `tent/tests/CMakeLists.txt`
- Old UB support changes:
  `include/transport/kunpeng_transport/ub_context.h`,
  `include/transport/kunpeng_transport/urma/urma_endpoint.h`,
  `src/transport/kunpeng_transport/ub_transport.cpp`,
  `src/transport/kunpeng_transport/urma/urma_endpoint.cpp`

## 2. Build Configuration

There is no `MOCK_URMA` CMake option in the current code. Mock URMA is selected
by the existing UB CMake logic when `liburma.so` is not found:

- If `/usr/lib64/liburma.so` is found, `ub_transport` links the real URMA
  library.
- If it is not found, `ub_transport` builds with
  `src/transport/kunpeng_transport/urma/mock_urma.cpp`.
- URMA headers are still required. Provide them with `URMA_INCLUDE_DIR`, with
  `FETCHCONTENT_SOURCE_DIR_URMA`, or allow `FindUrma.cmake` to fetch UMDK.

Use `BUILD_UNIT_TESTS`, not `BUILD_TESTS`.

```bash
cmake -S . -B build-ub-tent \
  -DUSE_TENT=ON \
  -DUSE_UB=ON \
  -DBUILD_UNIT_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Debug

cmake --build build-ub-tent \
  --target tent_xport_ub tent_ub_transport_test tent_ub_e2e_dual_node_test \
  --parallel
```

If URMA headers are installed in a non-default location:

```bash
cmake -S . -B build-ub-tent \
  -DUSE_TENT=ON \
  -DUSE_UB=ON \
  -DBUILD_UNIT_TESTS=ON \
  -DURMA_INCLUDE_DIR=/path/to/urma/include
```

## 3. Local Unit Test

Unit test source:

```text
mooncake-transfer-engine/tent/tests/ub_tent_transport_test.cpp
```

CTest target:

```bash
ctest --test-dir build-ub-tent -R '^tent_ub_transport_test$' \
  --output-on-failure -V
```

Direct binary:

```bash
cd build-ub-tent
GLOG_logtostderr=1 \
  ./mooncake-transfer-engine/tent/tests/tent_ub_transport_test
```

Run a single real GTest case:

```bash
cd build-ub-tent
GLOG_logtostderr=1 \
  ./mooncake-transfer-engine/tent/tests/tent_ub_transport_test \
  --gtest_filter="UbTentTransportTest.InstallWithMockUrma"
```

The executable is registered with CTest as `tent_ub_transport_test`.

### Covered GTest Cases

| Test case | Coverage |
|---|---|
| `UbSelectorTest.TypeNameRoundTrip` | `TransportSelector::transportTypeName(UB)` returns `"ub"` |
| `UbSelectorTest.ParseUbString` | `"ub"` parses to `TransportType::UB` |
| `UbSelectorTest.ParseUnknownStillReturnsUnspec` | Unknown transport strings still return `UNSPEC` |
| `UbSelectorTest.UbEnumValue` | `UB` enum is inside the supported transport range |
| `UbSelectorTest.SelectorPicksUbWhenFirstInPolicy` | A policy with `"transports": ["ub", "rdma", "tcp"]` selects UB when UB is available |
| `UbTentTransportTest.InstallWithMockUrma` | `UbTentTransport::install()` succeeds with mock URMA, exposes name `"ub"`, and advertises DRAM-to-DRAM capability |
| `UbTentTransportTest.AddAndRemoveMemoryBuffer` | Page-aligned memory can be registered/unregistered and `desc.transports` is updated with/removes `UB` |
| `UbTentTransportTest.AllocateAndFreeSubBatch` | TENT sub-batches allocate/free through old-TE UB batch IDs |
| `UbTentTransportTest.SubmitAndPollMockTransfer` | Local mock submit/poll path does not crash or hang, even if mock submit cannot complete a real peer transfer |
| `UbTentTransportTest.DoubleUninstallSafe` | `uninstall()` is idempotent |

On a host where real `liburma.so` is present but no usable UB HCA exists, the
transport install may fail and the hardware-dependent unit tests will call
`GTEST_SKIP()`. That is an expected local-developer outcome; on a pure mock
build, these tests should run.

In short, the mock test covers selector mapping, install/uninstall, memory
buffer lifecycle, sub-batch lifecycle, and a mock transfer no-crash/no-hang
path. It does not guarantee that the real UB data plane completes, and it does
not guarantee the full semantics of `transport_attrs[UB]`.

### What The Unit Test Does Not Assert

The unit test currently checks that `desc.transports` contains `UB`, but it does
not assert the serialized tseg stored in `desc.transport_attrs[UB]`. That field
is populated by `UbTentTransport::addMemoryBuffer()` from the old-TE local
segment and is consumed by `UbTentMetadataBridge::convertFromTent()` on remote
nodes. For debugging, inspect the descriptor after registration:

```cpp
auto it = desc.transport_attrs.find(TransportType::UB);
CHECK(it != desc.transport_attrs.end());
LOG(INFO) << "UB tseg JSON: " << it->second;
```

## 4. TENT UB Behavior To Validate

### 4.1 Transport Selection

The branch adds `UB` to the TENT transport enum and maps it to `"ub"`.
The selector should accept this policy:

```json
{
  "policy": [
    {
      "name": "kunpeng_ub_memory",
      "segment_type": "memory",
      "local_memory": "cpu",
      "remote_memory": "cpu",
      "same_machine": false,
      "transports": ["ub", "rdma", "tcp"]
    }
  ]
}
```

This is covered by `UbSelectorTest.SelectorPicksUbWhenFirstInPolicy`.

### 4.2 UB Transport Loading

When built with `-DUSE_UB=ON -DUSE_TENT=ON`, TENT loads UB if:

```json
{
  "transports": {
    "ub": { "enable": true }
  }
}
```

The default in `transport_loader.cpp` is also true when the key is absent.
Disable it explicitly with:

```json
{
  "transports": {
    "ub": { "enable": false }
  }
}
```

### 4.3 Device Selection

`UbTentTransport::install()` resolves UB devices in this order:

1. TENT config key `transports/ub/device_name`
2. Environment variable `MC_UB_DEVICE_NAME`
3. Auto-discover all UB devices

`device_name` and `MC_UB_DEVICE_NAME` may be comma-separated lists. Production
deployments normally pin one logical/bonded device, for example:

```json
{
  "transports": {
    "ub": {
      "enable": true,
      "device_name": "bonding_dev_0"
    }
  }
}
```

Equivalent environment override:

```bash
export MC_UB_DEVICE_NAME=bonding_dev_0
```

### 4.4 Local Segment Publishing

After `UbTentTransport::install()` succeeds, `setupUbLocalSegment()` mirrors
old-TE UB device EIDs into the local TENT `MemorySegmentDesc.devices` and sets
segment-level UB availability:

- Each UB device is written as `DeviceDesc.transport_attrs[UB] = <eid>`.
- The memory segment is tagged with
  `MemorySegmentDesc.transport_attrs[static_cast<int>(UB)] = "ub"`.
- `SegmentManager::synchronizeLocal()` publishes the updated segment.

After `registerLocalMemory()`, `addMemoryBuffer()` also records each buffer's
UB tseg handle in `BufferDesc.transport_attrs[UB]` and adds `UB` to
`BufferDesc.transports`.

### 4.5 Metadata Bridge

`UbTentMetadataBridge` replaces old-TE remote metadata lookup for the UB data
path:

- `LOCAL_SEGMENT_ID` still uses the old-TE base-class in-memory cache.
- Remote `getSegmentDescByID()` uses TENT `SegmentManager::getRemoteCached()`.
- `force_update=true` invalidates both the bridge cache and TENT remote cache.
- `getSegmentDescByName()` reads the remote TENT segment and converts it.
- `getSegmentID(name)` opens the TENT remote segment and returns that handle as
  the old-TE segment ID.
- `convertFromTent()` extracts device EIDs and buffer tsegs from
  `transport_attrs[UB]` and builds a minimal old-TE topology.

### 4.6 UB Handshake Through TENT RPC

`startHandshakeDaemon()` is a no-op in the bridge. It stores the old-TE UB
handshake callback. `UbTentTransport::install()` registers that callback with
`ControlService::setBootstrapUbCallback()`.

Active connection setup calls:

```text
UbEndpoint -> TransferMetadata::sendHandshake()
           -> UbTentMetadataBridge::sendHandshake()
           -> ControlClient::bootstrapUb()
           -> remote ControlService::onBootstrapUb()
           -> stored old-TE UB handshake callback
```

`UbBootstrapDesc` carries:

- `local_nic_path`
- `peer_nic_path`
- `jetty_num`
- `local_eid`
- `reply_msg`

The old-TE URMA endpoint was also changed so that active/passive setup can use
the `local_eid` returned by the RPC response directly.

## 5. Dual-Node Integration Test

Integration test source:

```text
mooncake-transfer-engine/tent/tests/ub_e2e_dual_node_test.cpp
```

Build target:

```bash
cmake --build build-ub-tent --target tent_ub_e2e_dual_node_test --parallel
```

This executable is intentionally not registered with CTest because it requires
real Kunpeng URMA hardware and two nodes sharing a TENT metadata backend.

### 5.1 Hardware And Service Requirements

On both nodes:

```bash
ls /dev/urma*
urma_cmd -q all
```

If needed, load the platform driver before running the test:

```bash
modprobe urma_udrv
```

Both nodes must also be able to reach:

- The shared TENT metadata backend, such as etcd.
- Each other's TENT RPC server address.
- The UB/URMA fabric.

### 5.2 Recommended Config Files

The test binary has a built-in UB-only config, but for real two-node testing it
is clearer to provide explicit config files with shared metadata and stable
segment names.

Node A (`node_a_ub.json`, used as `/path/to/ub_config.json` on the server):

```json
{
  "metadata_type": "etcd",
  "metadata_servers": "ETCD_IP:2379",
  "local_segment_name": "node_a_seg",
  "rpc_server_hostname": "NODE_A_IP",
  "rpc_server_port": 0,
  "transports": {
    "tcp": { "enable": false },
    "rdma": { "enable": false },
    "ub": {
      "enable": true,
      "device_name": "bonding_dev_0"
    }
  },
  "policy": [
    {
      "name": "ub_memory",
      "segment_type": "memory",
      "local_memory": "cpu",
      "remote_memory": "cpu",
      "same_machine": false,
      "transports": ["ub"]
    }
  ]
}
```

Node B (`node_b_ub.json`, used as `/path/to/ub_config.json` on the client)
should use the same metadata backend but a different local segment name and
hostname:

```json
{
  "metadata_type": "etcd",
  "metadata_servers": "ETCD_IP:2379",
  "local_segment_name": "node_b",
  "rpc_server_hostname": "NODE_B_IP",
  "rpc_server_port": 0,
  "transports": {
    "tcp": { "enable": false },
    "rdma": { "enable": false },
    "ub": {
      "enable": true,
      "device_name": "bonding_dev_0"
    }
  },
  "policy": [
    {
      "name": "ub_memory",
      "segment_type": "memory",
      "local_memory": "cpu",
      "remote_memory": "cpu",
      "same_machine": false,
      "transports": ["ub"]
    }
  ]
}
```

Notes:

- `--segment_name` is required by the server test, but the actual TENT segment
  name comes from `local_segment_name` in the config for non-`p2p` metadata.
  Keep them identical to avoid confusion.
- If `metadata_type` is left as `p2p`, TENT replaces the local segment name
  with the RPC address (`host:port`). In that mode, the client must open that
  generated segment name rather than `node_a_seg`.

### 5.3 Run The Test

Node A:

```bash
cd build-ub-tent
GLOG_logtostderr=1 GLOG_v=1 \
  ./mooncake-transfer-engine/tent/tests/tent_ub_e2e_dual_node_test \
  --role=server \
  --segment_name=node_a_seg \
  --transport_config=/path/to/ub_config.json
```

Node B:

```bash
cd build-ub-tent
GLOG_logtostderr=1 GLOG_v=1 \
  ./mooncake-transfer-engine/tent/tests/tent_ub_e2e_dual_node_test \
  --role=client \
  --remote_segment=node_a_seg \
  --transport_config=/path/to/ub_config.json \
  --data_size=1048576 \
  --operation=write
```

Client-side expected result:

```text
Client: WRITE 1048576 bytes ... COMPLETED
Client: READ 1048576 bytes ... COMPLETED
Client: data integrity VERIFIED
Client: test PASSED
```

`--operation=write` writes a known pattern to the remote buffer, reads it back,
and verifies every byte. `--operation=read` only executes the read path and
does not verify a known pattern.

### 5.4 What This Integration Test Covers

- The server publishes a memory segment with UB device EIDs.
- The server registers page-aligned memory for old-TE UB.
- The client opens the server's TENT segment.
- The client reads remote `SegmentInfo` and uses the first remote buffer base.
- The client submits WRITE and READ through the TENT API.
- `UbTentTransport` translates TENT requests to old-TE UB transfer requests.
- UB endpoint setup uses TENT `BootstrapUb` RPC for the URMA jetty exchange.
- Data integrity is verified after write + read-back.

### 5.5 Debugging Integration Failures

| Symptom | Checks |
|---|---|
| `openSegment('node_a_seg') failed` | Confirm Node A used non-`p2p` metadata, `local_segment_name` is `node_a_seg`, both nodes point to the same metadata backend, and Node A is still running |
| UB transport skipped during startup | Check `USE_UB=ON`, `transports/ub/enable=true`, URMA headers/library, device name, and `MC_UB_DEVICE_NAME` |
| Remote segment has no buffers | Confirm server `registerLocalMemory()` succeeded and segment publication reached the metadata backend |
| BootstrapUb RPC failed | Check remote segment `rpc_server_addr`, firewall, RPC hostname, and whether `setBootstrapUbCallback()` was registered after UB install |
| URMA duplicate registration | Confirm the branch contains the old UB changes that register on the primary context and adopt the segment on other contexts |
| Data mismatch | Confirm both nodes use the same `data_size`, the client used `--operation=write`, and no fallback transport was enabled accidentally |

Useful metadata inspection with etcd:

```bash
etcdctl get mooncake/tent/ --prefix
etcdctl get mooncake/tent/node_a_seg --print-value-only | python3 -m json.tool
```

Look for:

- `detail.devices[*].transport_attrs` containing the UB enum key and EID.
- `detail.buffers[*].transport_attrs` containing the UB enum key and tseg JSON.
- `rpc_server_addr` pointing to Node A's reachable TENT RPC address.

## 6. Fallback And Regression Tests

`UB_TENT` changes `TransferEngineImpl::commitPreparedSubmit()` and
`updateTaskStatusAfterPoll()` so a failed sub-batch submit can still be retried
by poll-time failover. This is not UB-specific and should be covered by the
existing TENT failover tests.

Run:

```bash
ctest --test-dir build-ub-tent -R '^tent_engine_failover_e2e_test$' \
  --output-on-failure -V

ctest --test-dir build-ub-tent -R '^tent_transport_hint_test$' \
  --output-on-failure -V

ctest --test-dir build-ub-tent -R '^tent_transport_selector_test$' \
  --output-on-failure -V
```

Old Transfer Engine UB regression target:

```bash
cmake --build build-ub-tent --target ub_transport_test --parallel

GLOG_logtostderr=1 \
  build-ub-tent/mooncake-transfer-engine/tests/ub_transport_test \
  --device_name=mock_urma_device
```

`ub_transport_test` is built when `USE_UB=ON`, but it is not registered with
CTest in the current code. It allocates a large NUMA buffer and may not be
appropriate for every developer machine.

## 7. Test Matrix

| Test | Hardware | CTest | Main-to-UB_TENT coverage |
|---|---:|---:|---|
| `tent_ub_transport_test` | No real UB hardware if mock URMA is compiled | Yes | UB enum/string mapping, selector, install lifecycle, memory registration, sub-batch lifecycle, submit/poll smoke |
| `tent_ub_e2e_dual_node_test` | Yes, two Kunpeng URMA nodes | No | TENT segment publishing, metadata bridge, BootstrapUb RPC, old-TE UB data path through TENT |
| `tent_engine_failover_e2e_test` | No | Yes | Submit-failure poll-time resubmit behavior |
| `tent_transport_hint_test` | No | Yes | Per-request routing remains valid with the expanded transport enum |
| `tent_transport_selector_test` | No | Yes | Selector regression around transport policy handling |
| `ub_transport_test` | Mock or real UB, depending on build | No | Old-TE UB registration/transfer regression |

## 8. Common Pitfalls

- Do not pass `-DMOCK_URMA=ON`; the option does not exist.
- Do not use `-DBUILD_TESTS=ON`; the correct option is `-DBUILD_UNIT_TESTS=ON`.
- Use the real UB TENT targets: `tent_ub_transport_test` and
  `tent_ub_e2e_dual_node_test`.
- The dual-node executable is not a CTest test.
- `--segment_name` in the dual-node server does not override TENT config; use
  `local_segment_name` in the config for stable cross-node names.
- UB memory registration should use page-aligned buffers. The tests use
  `posix_memalign(..., 4096, size)` for this reason.
