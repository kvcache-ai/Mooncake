# Native MLU support in TENT

Build TENT with `USE_MLU=ON` to select `MluPlatform` and enable the `mlu`
transport. Neuware headers, `libcnrt`, and `libcndrv` are required. This uses the
native SDK and does not require PyTorch or `torch_mlu`.

From the repository root, with the normal Mooncake build dependencies installed:

```sh
cmake -S . -B build-mlu -DUSE_TENT=ON -DUSE_CUDA=OFF -DUSE_MLU=ON \
  -DNEUWARE_ROOT=/usr/local/neuware -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF -DWITH_STORE_RS=OFF \
  -DBUILD_UNIT_TESTS=ON -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DUSE_ETCD=OFF
cmake --build build-mlu --target tent_mlu_platform_test tent_mlu_transport_test \
  tent_mlu_engine_test tent_mlu_process_test -j8
ctest --test-dir build-mlu --output-on-failure -R '^tent_mlu_.*_test$'
```

Use `mlu:N` for a visible device ordinal (`mlu` means device 0), or `cpu:N`
for host memory. Existing device allocations can be registered without copying
ownership to the engine. Pointer classification and allocation bounds come from
Neuware, including interior pointers used by chunked transfers. Keep allocations
alive until transfers finish, and synchronize producer work before submitting a
transfer that reads its output.

For TCP-only testing, use this configuration:

```json
{
  "metadata_type": "p2p",
  "rpc_server_hostname": "127.0.0.1",
  "rpc_server_port": "0",
  "transports": {
    "mlu": {"enable": true},
    "tcp": {"enable": true},
    "rdma": {"enable": false},
    "shm": {"enable": false},
    "hp_tcp": {"enable": false}
  }
}
```

For two hosts, set `rpc_server_hostname` to an address reachable from the peer.
The transport is enabled by default in an MLU build. It handles only local
MLU-to-host and host-to-MLU copies. The existing ProxyManager chains those copies
with a host-to-host network transfer:

```text
MLU A -> host stage A -> TCP -> host stage B -> MLU B
```

A host-memory endpoint omits its device stage. Both READ and WRITE are supported.
A peer transfer uses a network transport hint such as `TCP`; `MLU` is only a
local-copy hint. For separate allocation and registration calls, use fresh
`MemoryOptions` or reset `type = UNSPEC` before automatic registration: allocation
may fill in the transport type. Explicit MLU-only device registration is also
supported, since the network stage registers its own host buffers.

Staging admission and retry follow TENT's existing policy. A full staging pool
can fail a request; applications decide whether and when to resubmit. A failed
transfer may have completed some chunks, so callers must account for partial
writes and keep buffers alive while any physical operations remain in flight.

Device copies are synchronous in this version. There is no MLU peer-to-peer,
MLU IPC, direct device RDMA, or copy/network overlap implementation here. The
existing host staging planner can also select HP TCP. An invalid allocation
range, missing local copy transport, or missing staging location fails instead
of submitting device pointers to the host network path.

The tests cover real SDK allocations, fresh worker threads, interior pointers,
out-of-bounds rejection, transport routing, and READ/WRITE payload equality.
The engine suite covers TCP/HP TCP, legacy/policy selection, and runtime queues.
The process suite starts a second executable with independent allocations and
passes payloads through loopback TCP; test pipes carry only commands and metadata.
Run it on each supported SDK/device combination. Single-host success does not
establish multi-host bandwidth or RDMA support.
