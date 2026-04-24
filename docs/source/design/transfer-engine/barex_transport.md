# Barex Transport

The Barex transport (`BarexTransport`) is a **bare-metal RDMA extension** that provides an alternative RDMA data path with different queue pair management and flow-control characteristics compared to the standard `RdmaTransport`.

## Overview

While the standard `RdmaTransport` targets high-throughput, multi-NIC environments with endpoint pooling and topology-aware path selection, `BarexTransport` is designed for scenarios that require:

- **Dedicated queue pairs per connection** rather than shared endpoint pools.
- **Fine-grained flow control** via a countdown-latch mechanism that gates completion acknowledgements.
- **Simplified connection lifecycle** for bare-metal or HPC environments where connections are long-lived and the overhead of dynamic endpoint management is undesirable.

Both transports use the same ibverbs interface (`infiniband/verbs.h`) and can be compiled into the same binary; the choice is made at runtime via the protocol string.

## Hardware Requirements

- RDMA-capable NIC (InfiniBand or RoCE), same as `RdmaTransport`.
- `libibverbs` installed on the host.

## Build

```bash
cmake .. \
    -DUSE_BAREX=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

`USE_BAREX` links `barex_transport` into the `transfer_engine` shared library. It can be combined with `USE_CUDA`, `USE_MNNVL`, and other feature flags.

## Usage

### Protocol String

Use `"barex"` as the protocol string:

```python
from mooncake.engine import TransferEngine

te = TransferEngine()
te.initialize("node1:12345", "P2PHANDSHAKE", "barex", "mlx5_0")
```

### When to Use Barex Instead of rdma

| Scenario | Recommended Transport |
|----------|-----------------------|
| High-throughput, many concurrent connections, topology-aware routing | `rdma` |
| Long-lived dedicated connections, HPC / bare-metal, fixed topology | `barex` |
| AWS EFA | `efa` |

## Internal Design

### BarexContext

Each `BarexContext` object manages the ibverbs resources for a single local RDMA NIC:
- Protection Domain (`ibv_pd`)
- Completion Queue (`ibv_cq`)
- Memory Regions (`ibv_mr`) for each registered buffer

### Queue Pair Management

Unlike `RdmaTransport` which uses endpoint pooling with the SIEVE eviction algorithm, `BarexTransport` allocates a dedicated Queue Pair (QP) per connection pair. QPs are transitioned to the `RTS` (Ready to Send) state during the connection handshake and remain open for the lifetime of the Transfer Engine instance.

### CountDownLatch

`BarexTransport` uses a `CountDownLatch` synchronisation primitive to track in-flight send operations. When a batch of transfer requests is submitted, the latch count is set to the number of outstanding operations. Each CQ completion decrements the count; the initiating thread blocks until the count reaches zero.

This approach simplifies correctness at the cost of some throughput compared to the fully asynchronous pipeline in `RdmaTransport`.

## Limitations

- Not designed for environments with many dynamically appearing / disappearing peers (the static QP model does not scale to thousands of short-lived connections).
- Does not support topology-aware multi-NIC path selection.
- Requires `USE_BAREX=ON` at compile time; cannot be selected at runtime if not compiled in.

## See Also

- [Transfer Engine Architecture](index) — standard RDMA transport
- [Supported Protocols](../../getting_started/supported-protocols)
- [EFA Transport](efa_transport) — alternative for AWS EFA
