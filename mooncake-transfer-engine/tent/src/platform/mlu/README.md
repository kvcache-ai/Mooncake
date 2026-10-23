# Native MLU support in TENT

Build TENT with `USE_MLU=ON` to select `MluPlatform` and enable the `mlu`
transport. Neuware headers, `libcnrt`, and `libcndrv` are required.

From the repository root, with the normal Mooncake build dependencies installed:

```sh
cmake -S . -B build-mlu -DUSE_TENT=ON -DUSE_CUDA=OFF -DUSE_MLU=ON \
  -DNEUWARE_ROOT=/usr/local/neuware -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF -DWITH_STORE_RS=OFF \
  -DBUILD_UNIT_TESTS=ON -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DUSE_ETCD=OFF
cmake --build build-mlu --target tent_mlu_platform_test tent_mlu_transport_test \
  tent_mlu_engine_test tent_mlu_process_test tent_mlu_rdma_test \
  tent_mlu_topology_test -j8
ctest --test-dir build-mlu --output-on-failure -R '^tent_mlu_.*_test$'
```

Use `mlu:N` for a visible device ordinal (`mlu` means device 0), or `cpu:N`
for host memory. Applications retain ownership of registered device allocations.
Pointer classification and allocation bounds come from Neuware, including
interior pointers used by chunked transfers. Keep allocations
alive until transfers finish, and synchronize producer work before submitting a
transfer that reads its output.

NIC affinity uses three ranks: prefer the NICs closest within the same PCIe
root, then other NICs on the same NUMA node, then remaining NICs.
All equally close NICs remain eligible. Missing sysfs paths and paths across
different PCIe roots do not provide a comparable PCIe distance; if none of the
NICs has a known distance within the device's root, discovery falls back to
the existing NUMA-only preference. Device names use visible MLU ordinals, while
affinity is resolved from each visible device's PCI bus ID. NIC whitelist and
blacklist filtering still apply before ranking.

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

## Direct RDMA

With the RDMA transport enabled, TENT can transfer directly between registered
MLU device buffers and peer MLU or host buffers. The allocation's device and
Neuware runtime must support dma-buf export, and each enabled NIC must support
`ibv_reg_dmabuf_mr`.

TENT exports a page-aligned range within the allocation and shares that export
across NIC registrations. It releases the exported FD after registration; each
MR retains its own kernel reference until deregistration. Interior transfer
addresses are supported when the aligned export fits within the allocation.

Use `MemoryOptions::type = UNSPEC` for automatic registration. If device export
or NIC registration fails, automatic registration retains the MLU copy path
and uses host staging. Explicit `type = RDMA` reports registration failure.
Partial NIC registration is rolled back across all NICs. MLU device memory is
never passed to host `ibv_reg_mr` as a fallback.

Direct selection requires RDMA registration of both endpoint buffers, the
matching RDMA device capabilities, and a transport hint/policy that admits RDMA.
MLU-only registration, a host-only transport policy, or
`transports/rdma/disable_gpu_direct_rdma = true` selects staging. With
`enable_progress_worker=true`, RDMA batches are polled for completion even
when the runtime queue is disabled.

Complete producer work before submission and wait for transfer completion
before consuming the destination. Keep registered allocations alive until all
transfers using them have drained, and drain batches before unregistering
buffers or destroying the engine.

The tests cover real SDK allocations, fresh worker threads, interior pointers,
out-of-bounds rejection, transport routing, and READ/WRITE payload equality.
Transport tests hold real device queues to check pending status and resource
lifetimes, and inject notifier-record failure after a copy is submitted.
The engine suite covers TCP/HP TCP, legacy/policy selection, and runtime queues.
The process suite starts a second executable with independent allocations and
passes payloads through loopback TCP/HP TCP; test pipes carry only commands
and metadata.
Run it on each supported SDK/device combination. Single-host success does not
establish multi-host bandwidth. The RDMA suite checks direct/staged routes and
payloads with both selector modes. Set `MC_TENT_MLU_REQUIRE_RDMA=1` to fail instead
of skipping when direct RDMA is unavailable; optional `MC_TENT_TEST_NIC` and
`MC_TENT_TEST_GID_INDEX` select the NIC and GID for this suite. Generic
`tent_rdma_transport_test` cases cover owned/borrowed FD lifetimes and partial
registration rollback without requiring an RDMA device.
