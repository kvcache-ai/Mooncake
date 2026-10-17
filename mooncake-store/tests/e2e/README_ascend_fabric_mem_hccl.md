# Ascend FabricMem and HCCL initialization probe

This manual probe requires an idle, owned A3 NPU, a running test Mooncake
master, torch, and torch-npu. It submits no KV transfers. It does not establish
in-flight transfer cancellation, device quiescence after timeouts, or bandwidth.
It is deliberately not collected by pytest.

Run each case in a fresh process with unused endpoint and rendezvous ports:

```sh
# Set these to your isolated test master and local node address.
export TEST_MASTER_ADDR=your-test-master:50051
export TEST_NODE_IP=your-local-node-ip
python mooncake-store/tests/e2e/ascend_fabric_mem_hccl_init.py \
  --master "$TEST_MASTER_ADDR" --endpoint "$TEST_NODE_IP:19101" \
  --device 0 --dist-port 19201 --fabric-start 128
python mooncake-store/tests/e2e/ascend_fabric_mem_hccl_init.py \
  --master "$TEST_MASTER_ADDR" --endpoint "$TEST_NODE_IP:19102" \
  --device 0 --dist-port 19202 --fabric-start 128 --hixl-first
```

The default order is HCCL initialization and barrier, then HIXL initialization.
With `--hixl-first`, the probe retains an initial zero-capacity HIXL client,
initializes HCCL, and creates a second HIXL client. JSON records setup and close
return codes. Exit status zero means initialization and cleanup returned success. Close errors
produce CLOSE_FAIL and a nonzero process exit status.
Failures remain observations; a failed baseline is not automatically an expected
or acceptable outcome on every stack. SDK logs identify the failing stage.

On one tested stack (Mooncake 5d77d3446b9fee89df1ea50468018b1315638a37,
CANN 9.2.0, HDK 25.5.1.1, torch 2.10.0, torch-npu 2.10.0.post4), HCCL-first
could fail with `aclrtReserveMemAddressNoUCMemory`, status 207001, requesting
35184372088832 bytes of virtual address space. This is not proof of physical
DRAM exhaustion. Moving `fabric_memory.start_address` to 128 succeeded in some
runs and failed in others. Disabling expandable segments also had a failing
repeated sample. Neither is a demonstrated general fix.

Six fresh-process HIXL-first cases on that stack initialized both clients and
HCCL successfully and returned zero from both closes. A separate single-layer
model integration run also completed shared KV loading, idle writer-process
loss and replacement, and fresh-consumer recomputation after Store loss. These
bounded observations motivate investigation of an explicit resource lifecycle
before HCCL. They do not establish a production-safe implementation, an SDK root
cause, or compatibility with other models, parallel layouts, and driver stacks.
The address 128 is an experimental layout choice, not a portable default or a
DRAM quota. Keep Store and clients on the same chosen SDK layout.

CPU-only harness checks (no SDK or NPU required):

```sh
python -m unittest discover -s mooncake-store/tests/e2e \
  -p test_ascend_fabric_mem_hccl_init.py -v
```

These checks use test doubles for initialization order and error/cleanup reporting;
they do not replace fresh-process hardware runs. The observations above predate
the CPU-tested result-reporting hardening in this revision.
