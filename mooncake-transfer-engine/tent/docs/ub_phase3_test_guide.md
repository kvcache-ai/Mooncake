# TENT UB Transport Phase 3 Test Guide

This guide documents how to validate UB transport support in TENT on the `UB_TENT` branch.

This document focuses on the current Phase 3 work in this branch: enabling the existing Kunpeng UB/URMA data path to be reused by TENT through a TENT transport adapter, metadata bridge, and control-plane bootstrap path.

## 1. Scope

Compared with `main`, this branch adds TENT-side support for UB transport and adjusts the existing old Transfer Engine UB implementation so that it can be reused by TENT.

| Area                     | `main`                                     | `UB_TENT`                                                          |
| ------------------------ | ------------------------------------------ | ------------------------------------------------------------------ |
| TENT transport enum      | No `UB` transport type                     | Adds `TransportType::UB`                                           |
| Transport selector       | Cannot parse or select `"ub"`              | Supports `"ub"` as a transport policy entry                        |
| Transport loader         | Cannot instantiate UB in TENT              | Creates `UbTentTransport` when UB is enabled                       |
| TENT UB adapter          | Not available                              | Adds `UbTentTransport`                                             |
| Metadata bridge          | Old UB depends on old `TransferMetadata`   | Adds `UbTentMetadataBridge` to resolve TENT segments for old UB    |
| UB bootstrap             | Old UB handshake path only                 | Adds TENT `BootstrapUb` RPC path                                   |
| Local segment publishing | No UB-specific TENT attrs                  | Publishes UB EID and tseg data through TENT transport attrs        |
| Submit fallback          | Submit failure may not be retried cleanly  | Allows failed sub-batches to be retried through poll-time failover |
| URMA registration        | Multiple contexts may register the same VA | Primary context registers once; other contexts adopt the segment   |
| Tests                    | No TENT UB tests                           | Adds `tent_ub_transport_test` and `tent_ub_e2e_dual_node_test`     |

Main files to review:

```text
mooncake-transfer-engine/tent/include/tent/transport/ub/*
mooncake-transfer-engine/tent/src/transport/ub/*
mooncake-transfer-engine/tent/include/tent/runtime/control_plane.h
mooncake-transfer-engine/tent/src/runtime/control_plane.cpp
mooncake-transfer-engine/tent/include/tent/rpc/rpc.h
mooncake-transfer-engine/tent/src/runtime/transport_selector.cpp
mooncake-transfer-engine/tent/src/runtime/transport_loader.cpp
mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp
mooncake-transfer-engine/tent/tests/ub_tent_transport_test.cpp
mooncake-transfer-engine/tent/tests/ub_e2e_dual_node_test.cpp
mooncake-transfer-engine/tent/tests/CMakeLists.txt
mooncake-transfer-engine/src/transport/kunpeng_transport/ub_transport.cpp
mooncake-transfer-engine/src/transport/kunpeng_transport/urma/urma_endpoint.cpp
mooncake-transfer-engine/include/transport/kunpeng_transport/ub_context.h
mooncake-transfer-engine/include/transport/kunpeng_transport/urma/urma_endpoint.h
```

## 2. Build Configuration

Use `BUILD_UNIT_TESTS`, not `BUILD_TESTS`.

There is no `MOCK_URMA` CMake option in the current code. Mock URMA is selected by the existing UB CMake logic when the real URMA library is not found.

Typical local build:

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
  -DURMA_INCLUDE_DIR=/path/to/urma/include \
  -DCMAKE_BUILD_TYPE=Debug
```

If the real URMA runtime is available on the test machine, make sure the runtime library and the selected UB device are usable before running hardware tests.

## 3. Local Unit Test

Unit test source:

```text
mooncake-transfer-engine/tent/tests/ub_tent_transport_test.cpp
```

CTest command:

```bash
ctest --test-dir build-ub-tent \
  -R '^tent_ub_transport_test$' \
  --output-on-failure \
  -V
```

Direct binary command:

```bash
cd build-ub-tent

GLOG_logtostderr=1 \
./mooncake-transfer-engine/tent/tests/tent_ub_transport_test
```

Run one GTest case:

```bash
cd build-ub-tent

GLOG_logtostderr=1 \
./mooncake-transfer-engine/tent/tests/tent_ub_transport_test \
  --gtest_filter="UbTentTransportTest.InstallWithMockUrma"
```

## 4. Unit Test Coverage

| Test case                                         | Coverage                                                                                 |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `UbSelectorTest.TypeNameRoundTrip`                | Verifies that `TransportSelector::transportTypeName(UB)` returns `"ub"`                  |
| `UbSelectorTest.ParseUbString`                    | Verifies that `"ub"` parses to `TransportType::UB`                                       |
| `UbSelectorTest.ParseUnknownStillReturnsUnspec`   | Verifies that unknown transport strings still return `UNSPEC`                            |
| `UbSelectorTest.UbEnumValue`                      | Verifies that the UB enum value is inside the supported transport range                  |
| `UbSelectorTest.SelectorPicksUbWhenFirstInPolicy` | Verifies that a policy with `"ub"` first can select UB when UB is available              |
| `UbTentTransportTest.InstallWithMockUrma`         | Verifies basic install lifecycle with mock URMA and checks the transport name/capability |
| `UbTentTransportTest.AddAndRemoveMemoryBuffer`    | Verifies page-aligned memory add/remove flow and updates to `desc.transports`            |
| `UbTentTransportTest.AllocateAndFreeSubBatch`     | Verifies sub-batch allocation and release through the old UB batch path                  |
| `UbTentTransportTest.SubmitAndPollMockTransfer`   | Verifies that the mock submit/poll path does not crash or hang                           |
| `UbTentTransportTest.DoubleUninstallSafe`         | Verifies that repeated uninstall is safe                                                 |

The unit test mainly covers selector mapping, adapter lifecycle, memory buffer lifecycle, sub-batch lifecycle, and mock transfer smoke behavior.

It does not prove that the real UB data plane completes on hardware. It also does not fully assert the serialized `transport_attrs[UB]` content. That deeper validation belongs to the dual-node integration test and hardware inspection.

On a host where real `liburma.so` is present but no usable UB HCA exists, hardware-dependent setup may fail and the corresponding test path may skip. That is expected for local developer machines without Kunpeng UB hardware.

## 5. Behavior To Validate

### 5.1 Transport Selection

The branch adds `UB` to the TENT transport enum and maps it to `"ub"`.

Example policy:

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

Expected behavior:

```text
The selector can parse "ub" and select TransportType::UB when UB is available.
```

### 5.2 UB Transport Loading

When built with both `USE_UB=ON` and `USE_TENT=ON`, TENT can load UB if UB is enabled in config.

Example:

```json
{
  "transports": {
    "ub": {
      "enable": true
    }
  }
}
```

Disable UB explicitly:

```json
{
  "transports": {
    "ub": {
      "enable": false
    }
  }
}
```

### 5.3 Device Selection

`UbTentTransport::install()` should resolve UB devices in this order:

```text
1. TENT config key: transports/ub/device_name
2. Environment variable: MC_UB_DEVICE_NAME
3. Auto-discovery of UB devices
```

For Kunpeng SuperNode environments, pin the expected logical or bonded UB device explicitly.

Example config:

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

`device_name` and `MC_UB_DEVICE_NAME` may be comma-separated lists if multiple UB devices should be considered.

### 5.4 Local Segment Publishing

After UB transport installation, the local TENT segment should publish UB-specific device information.

Expected behavior:

```text
MemorySegmentDesc.devices[*].transport_attrs[UB] contains UB EID information.
MemorySegmentDesc.transport_attrs[UB] marks UB availability.
BufferDesc.transport_attrs[UB] contains the UB tseg handle after memory registration.
BufferDesc.transports contains TransportType::UB after successful registration.
```

This is needed because the remote node reconstructs the old-TE UB segment descriptor from TENT metadata.

### 5.5 Metadata Bridge

`UbTentMetadataBridge` allows old UB code to resolve TENT segments through the old `TransferMetadata` interface.

Expected behavior:

```text
Remote getSegmentDescByID() uses the TENT SegmentManager remote cache.
force_update=true invalidates both bridge-side and TENT-side remote cache.
getSegmentDescByName() opens the remote TENT segment and converts it.
getSegmentID(name) returns the TENT remote segment handle as the old-TE segment ID.
convertFromTent() extracts UB EID and tseg data from TENT transport attrs.
```

### 5.6 UB Bootstrap Through TENT RPC

The old UB handshake daemon is not started by the bridge. Instead, UB endpoint bootstrap is routed through TENT control-plane RPC.

Expected call path:

```text
UbEndpoint
  -> TransferMetadata::sendHandshake()
  -> UbTentMetadataBridge::sendHandshake()
  -> ControlClient::bootstrapUb()
  -> remote ControlService::onBootstrapUb()
  -> old-TE UB handshake callback
```

`UbBootstrapDesc` should carry:

```text
local_nic_path
peer_nic_path
jetty_num
local_eid
reply_msg
```

This validates that UB connection setup can use the TENT control plane instead of the old standalone UB handshake daemon.

## 6. Dual-Node Integration Test

Integration test source:

```text
mooncake-transfer-engine/tent/tests/ub_e2e_dual_node_test.cpp
```

Build target:

```bash
cmake --build build-ub-tent \
  --target tent_ub_e2e_dual_node_test \
  --parallel
```

This executable is intentionally not registered with CTest because it requires:

```text
1. Two Kunpeng UB-capable nodes
2. A usable URMA runtime
3. A shared TENT metadata backend
4. Network reachability between both TENT RPC servers
5. UB/URMA fabric connectivity between the two nodes
```

### 6.1 Hardware And Runtime Checks

Run on both nodes:

```bash
ls /dev/urma* || true
urma_cmd -q all
```

If needed, load the platform driver:

```bash
modprobe urma_udrv
```

Also confirm that both nodes can reach:

```text
The shared metadata backend, for example etcd
Each other's TENT RPC server address
The selected UB device, for example bonding_dev_0
```

### 6.2 Recommended Config Files

The test binary has a built-in UB-only config, but real two-node testing should use explicit config files.

Server config example, `node_a_ub.json`:

```json
{
  "metadata_type": "etcd",
  "metadata_servers": "ETCD_IP:2379",
  "local_segment_name": "node_a_seg",
  "rpc_server_hostname": "NODE_A_IP",
  "rpc_server_port": 0,
  "transports": {
    "tcp": {
      "enable": false
    },
    "rdma": {
      "enable": false
    },
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

Client config example, `node_b_ub.json`:

```json
{
  "metadata_type": "etcd",
  "metadata_servers": "ETCD_IP:2379",
  "local_segment_name": "node_b_seg",
  "rpc_server_hostname": "NODE_B_IP",
  "rpc_server_port": 0,
  "transports": {
    "tcp": {
      "enable": false
    },
    "rdma": {
      "enable": false
    },
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

```text
1. Use the same metadata backend on both nodes.
2. Use different local segment names on the two nodes.
3. Keep the server's --segment_name consistent with local_segment_name to avoid confusion.
4. If metadata_type is p2p, TENT may replace the local segment name with the RPC address.
5. For stable cross-node tests, etcd is easier to reason about than p2p metadata.
```

### 6.3 Run The Test

On Node A:

```bash
cd build-ub-tent

GLOG_logtostderr=1 GLOG_v=1 \
./mooncake-transfer-engine/tent/tests/tent_ub_e2e_dual_node_test \
  --role=server \
  --segment_name=node_a_seg \
  --transport_config=/path/to/node_a_ub.json
```

On Node B:

```bash
cd build-ub-tent

GLOG_logtostderr=1 GLOG_v=1 \
./mooncake-transfer-engine/tent/tests/tent_ub_e2e_dual_node_test \
  --role=client \
  --remote_segment=node_a_seg \
  --transport_config=/path/to/node_b_ub.json \
  --data_size=1048576 \
  --operation=write
```

Expected client-side result:

```text
Client: WRITE 1048576 bytes ... COMPLETED
Client: READ 1048576 bytes ... COMPLETED
Client: data integrity VERIFIED
Client: test PASSED
```

`--operation=write` writes a known pattern to the remote buffer, reads it back, and verifies the returned bytes.

`--operation=read` only executes the read path and does not verify a known pattern.

### 6.4 What The Integration Test Covers

The dual-node integration test validates:

```text
1. Server-side TENT segment publication
2. UB EID publication through TENT segment device attrs
3. UB tseg publication through TENT buffer attrs
4. Client-side remote segment open
5. Metadata conversion through UbTentMetadataBridge
6. TENT request conversion through UbTentTransport
7. UB endpoint bootstrap through BootstrapUb RPC
8. Old-TE UB data path reuse from TENT
9. Remote WRITE
10. Remote READ
11. Data integrity after write + read-back
```

## 7. Debugging Integration Failures

| Symptom                            | Checks                                                                                                                          |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `openSegment("node_a_seg") failed` | Confirm both nodes use the same metadata backend, the server is still running, and `local_segment_name` is `node_a_seg`         |
| UB transport not installed         | Check `USE_UB=ON`, `USE_TENT=ON`, `transports.ub.enable=true`, URMA headers/runtime, and `device_name`                          |
| Wrong UB device selected           | Set `transports.ub.device_name` or `MC_UB_DEVICE_NAME` explicitly                                                               |
| Remote segment has no buffers      | Confirm server-side memory registration succeeded and segment synchronization reached metadata                                  |
| Remote segment has no UB attrs     | Confirm UB transport installation happened before segment synchronization                                                       |
| `BootstrapUb` RPC failed           | Check `rpc_server_hostname`, firewall, remote RPC reachability, and callback registration                                       |
| URMA duplicate registration error  | Confirm the branch includes the primary-register plus adopt-segment lifecycle change                                            |
| Transfer hangs                     | Check UB fabric connectivity, selected UB device, endpoint creation logs, and poll/fallback logs                                |
| Data mismatch                      | Use `--operation=write`, confirm both nodes use the same `data_size`, and make sure no unintended fallback transport is enabled |

Useful etcd inspection:

```bash
etcdctl get mooncake/tent/ --prefix

etcdctl get mooncake/tent/node_a_seg --print-value-only | python3 -m json.tool
```

Look for:

```text
detail.devices[*].transport_attrs containing UB EID information
detail.buffers[*].transport_attrs containing UB tseg information
rpc_server_addr pointing to Node A's reachable TENT RPC address
```

## 8. Fallback And Regression Tests

This branch also changes submit failure handling so that a failed sub-batch submit can be retried through poll-time failover.

Run existing TENT regression tests:

```bash
ctest --test-dir build-ub-tent \
  -R '^tent_engine_failover_e2e_test$' \
  --output-on-failure \
  -V

ctest --test-dir build-ub-tent \
  -R '^tent_transport_hint_test$' \
  --output-on-failure \
  -V

ctest --test-dir build-ub-tent \
  -R '^tent_transport_selector_test$' \
  --output-on-failure \
  -V
```

Old Transfer Engine UB regression target:

```bash
cmake --build build-ub-tent \
  --target ub_transport_test \
  --parallel
```

Example run:

```bash
GLOG_logtostderr=1 \
build-ub-tent/mooncake-transfer-engine/tests/ub_transport_test \
  --device_name=mock_urma_device
```

`ub_transport_test` may allocate a large NUMA buffer, so it may not be suitable for every developer machine.

## 9. Test Matrix

| Test                            |                                 Hardware | CTest | Coverage                                                                                                                        |
| ------------------------------- | ---------------------------------------: | ----: | ------------------------------------------------------------------------------------------------------------------------------- |
| `tent_ub_transport_test`        | No real UB hardware if mock URMA is used |   Yes | UB enum/string mapping, selector, install lifecycle, memory registration lifecycle, sub-batch lifecycle, mock submit/poll smoke |
| `tent_ub_e2e_dual_node_test`    |        Yes, two Kunpeng UB-capable nodes |    No | TENT segment publishing, metadata bridge, BootstrapUb RPC, old-TE UB data path through TENT, data integrity                     |
| `tent_engine_failover_e2e_test` |                                       No |   Yes | Submit-failure poll-time resubmit behavior                                                                                      |
| `tent_transport_hint_test`      |                                       No |   Yes | Transport hint behavior after adding UB                                                                                         |
| `tent_transport_selector_test`  |                                       No |   Yes | Selector regression around transport policy handling                                                                            |
| `ub_transport_test`             |      Mock or real UB, depending on build |    No | Old Transfer Engine UB registration and transfer regression                                                                     |

## 10. Common Pitfalls

Do not use:

```bash
-DMOCK_URMA=ON
```

The current code does not define this CMake option.

Do not use:

```bash
-DBUILD_TESTS=ON
```

Use:

```bash
-DBUILD_UNIT_TESTS=ON
```

Use the real TENT UB targets:

```text
tent_ub_transport_test
tent_ub_e2e_dual_node_test
```

Do not use old or planned names such as:

```text
tent_ub_transfer_test
tent_ub_e2e_test
```

The dual-node executable is not registered as a CTest test.

For dual-node tests, prefer explicit config files and shared metadata. Keep the server `--segment_name` aligned with the config `local_segment_name`.

UB memory registration should use page-aligned buffers. The tests use page-aligned allocation for this reason.

## 11. Expected Validation Summary

A complete validation should include:

```text
1. Build with USE_TENT=ON and USE_UB=ON.
2. Run tent_ub_transport_test locally.
3. Run selector, hint, and failover regression tests.
4. Run old Transfer Engine ub_transport_test when the environment allows it.
5. Run tent_ub_e2e_dual_node_test on two Kunpeng UB-capable nodes.
6. Confirm metadata contains UB EID and tseg attrs.
7. Confirm BootstrapUb RPC is exercised during connection setup.
8. Confirm write + read-back data integrity in the dual-node test.
```
