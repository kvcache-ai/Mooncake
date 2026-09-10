# MACA IBGDA Device API validation

The MACA Device API uses the shared CUDA/MUSA mlx5 IBGDA transport to post GPU-initiated RDMA WRITEs and masked 32-bit atomic adds. MACA control queues use host-mapped memory. On the tested C500 / MACA 3.7.1.13 system, GPU-VA control UMEM was rejected by DevX CREATE_QP.

## Payload allocation

Allocate GPU-produced RDMA payload with fine-grained device memory:

```cpp
void* buffer = nullptr;
mcExtMallocWithFlags(&buffer, bytes, mcDeviceMallocFinegrained);
// Register buffer with RdmaTransport::registerMemory before exchanging metadata.
```

Default mcMalloc allocations produced successful requester CQEs but stale payload when the GPU generated the data, even with device/system fences. Fine-grained sender memory resolved this in the tested environment. A successful CQE alone does not establish payload visibility.

The existing Mooncake MACA P2pTransport allocator accepts `MOONCAKE_EP_MACA_ALLOC=fine`. This setting provides an allocation entry point for integration; the tests here validate the IBGDA primitives independently of EP.

## Build and run

Use a MACA development environment with mxcc, cu-bridge headers/libraries and libmlx5 development files. From a normally configured Mooncake build:

```bash
cmake -S . -B build -G Ninja \
  -DUSE_MACA=ON -DWITH_EP=OFF \
  -DBUILD_EXAMPLES=ON -DBUILD_MACA_IBGDA_TEST=ON \
  -DMACA_ROOT=/opt/maca -DMACA_INCLUDE_DIR=/opt/maca/include \
  -DMACA_LIB_DIR=/opt/maca/lib \
  '-DMACA_RUNTIME_LIBS=mcruntime;mxc-runtime64;rt'
cmake --build build --target maca_ibgda_suite -j1

# Replace HCA names and GPU ordinals with the allocated topology.
bash mooncake-transfer-engine/example/run_maca_ibgda_suite.sh \
  build/mooncake-transfer-engine/example/maca_ibgda_suite \
  mlx5_0 mlx5_1 0 1
```

The launcher creates a unique exchange/log directory and bounds both processes with timeouts. The binary accepts `RANK GPU NIC EXCHANGE_PREFIX`. Both ranks need access to the same exchange directory. The suite is manual and opt-in; it is not registered with CTest.

## Validation contract

The suite uses two GPUs, independent send/receive regions, fine-grained payload memory, and four QPs per peer. Each case runs in both directions concurrently. Sixteen cases cover:

- WRITE sizes 1, 7, 63, 64, 257, 4096 and 1 MiB;
- 64-WQE bursts and eight CTAs contending on one QP;
- 16,384-entry WQ ring wrap and 16-bit WQE-counter wrap;
- positive and negative masked 32-bit atomic add, including adjacent-byte guards;
- 1,000 GPU-only payload/signal/ack rounds with buffer reuse;
- four peer QPs under eight CTAs, and QP recreation/reconnection.

Each rank posts 88,166 WQEs. Success requires valid requester completions, exact payload/remote atomic results, preserved guards and peer acknowledgement. The GPU protocol verifies each round without a host barrier inside the loop. Bounded diagnostic CQ waits precede the shared polling helper. Mooncake uses collapsed CQs, so steady-state completion is identified by opcode and WQE counter rather than an ordinary CQ ring owner sequence.

The tested configuration is one host with two C500 GPUs and distinct RoCE HCAs on MACA 3.7.1.13. Cross-host validation, throughput, atomic fetch-return byte order and full EP integration remain separate tests.
