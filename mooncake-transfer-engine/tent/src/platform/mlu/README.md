# Native MLU support in TENT

Build TENT with `USE_MLU=ON` to select `MluPlatform` and enable the `mlu`
transport. Neuware headers, `libcnrt`, and `libcndrv` are required.

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
for host memory. Applications retain ownership of registered device allocations.
Pointer classification and allocation bounds come from Neuware, including
interior pointers used by chunked transfers. Keep allocations
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
writes. A staging task remains pending until its accepted child operations have
finished, even if another child fails. Keep both allocations alive and source
contents unchanged until the transfer reaches a terminal state; a polling error
alone does not permit reuse.

Pinned D2H copies and pinned H2D copies larger than 64 KiB use per-context,
per-direction device queues and report completion through notifiers. MLU staging
explicitly selects the MLU allocator, which tries `cnrtHostMalloc`; ordinary
internal CPU allocations retain the engine's normal allocator selection. Explicit
host allocations through the MLU transport use the same allocator. If a pinned
allocation does not satisfy a requested `cpu:N` location, it falls back to the
normal NUMA allocator. Pageable host buffers use synchronous copies.

Host-to-MLU copies of at most 64 KiB run synchronously when their context's H2D
queue is absent or idle, avoiding queue overhead. With earlier H2D work pending,
pinned copies join the same queue; pageable copies wait for that queue to drain
before copying synchronously. Submission is serialized through the idle check
and inline copy, preserving H2D copy order within each context. Dependencies
across directions or contexts still require waiting for the preceding transfer
to complete. The staging pipeline waits for each chunk's dependent steps while
allowing independent chunks to overlap device copies with TCP or HP TCP transfers.

Freeing a sub-batch or unregistering a buffer waits for its in-flight copies;
transport shutdown drains all copy queues. If a copy may have been submitted but
its completion cannot be tracked, it remains pending until the queue drains.
Drain failures are reported and resources are retained for retry. External pinned
allocations remain owned by the caller.

There is no MLU peer-to-peer, MLU IPC, or direct device RDMA. An invalid allocation
range, missing local copy transport, or missing staging location fails instead
of submitting device pointers to the host network path.

The tests cover real SDK allocations, fresh worker threads, interior pointers,
out-of-bounds rejection, transport routing, and READ/WRITE payload equality.
Transport tests hold real device queues to check pending status and resource
lifetimes, and inject notifier-record failure after a copy is submitted.
The engine suite covers TCP/HP TCP, legacy/policy selection, and runtime queues.
The process suite starts a second executable with independent allocations and
passes payloads through loopback TCP/HP TCP; test pipes carry only commands
and metadata.
Run it on each supported SDK/device combination. Single-host success does not
establish multi-host bandwidth or RDMA support.
