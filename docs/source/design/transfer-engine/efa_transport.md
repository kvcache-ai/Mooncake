# AWS EFA Transport for Mooncake

This document describes how to build and use Mooncake with AWS Elastic Fabric Adapter (EFA) support using libfabric.

## Prerequisites

### 1. AWS EFA Driver and libfabric

EFA driver and libfabric should be pre-installed on AWS instances with EFA support (e.g., p6-b200.48xlarge, p5e.48xlarge, p4d.24xlarge).

Verify installation:
```bash
# Check EFA devices
fi_info -p efa

# Verify libfabric location
ls /opt/amazon/efa/lib/libfabric.so
ls /opt/amazon/efa/include/rdma/fabric.h
```

If not installed, follow [AWS EFA documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html).

### 2. Build Dependencies

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    libgflags-dev \
    libgoogle-glog-dev \
    libjsoncpp-dev \
    libnuma-dev \
    libibverbs-dev \
    libboost-all-dev \
    libcurl4-openssl-dev \
    libyaml-cpp-dev \
    libgtest-dev \
    pybind11-dev \
    python3-dev

# Install yalantinglibs (required)
cd /tmp
git clone https://github.com/alibaba/yalantinglibs.git
cd yalantinglibs
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local
make -j$(nproc)
sudo make install
```

## Building Mooncake with EFA Support

### 1. Clone the Repository

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
git submodule update --init --recursive
```

### 2. Build with EFA Enabled

```bash
mkdir build && cd build

cmake .. \
    -DUSE_EFA=ON \
    -DUSE_CUDA=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo

make -j$(nproc)
```

> **Note:** `-DUSE_CUDA=ON` is required when transferring GPU memory (e.g., KV cache in vLLM). Without it, the TCP transport (used as fallback when `mooncake_protocol` is set to `"tcp"`) cannot detect GPU memory and will fail with "Bad address" (EFAULT) errors.

### 3. Install Python Package

```bash
# Copy built modules to wheel directory
cp mooncake-integration/engine.cpython-*.so ../mooncake-wheel/mooncake/
cp mooncake-asio/libasio.so ../mooncake-wheel/mooncake/

# Install with pip
pip install -e ../mooncake-wheel --no-build-isolation
```

## Verification

Test EFA transport initialization:

```python
from mooncake.engine import TransferEngine

te = TransferEngine()
result = te.initialize('127.0.0.1', 'P2PHANDSHAKE', 'efa', '')
print(f'Initialize result: {result}')  # Should be 0

# You should see logs like:
# EFA device (libfabric): rdmap79s0, domain: rdmap79s0-rdm, provider: efa
```

## Usage with vLLM

### Prefill Instance

```bash
VLLM_MOONCAKE_BOOTSTRAP_PORT=8998 \
vllm serve <model_path> -tp 8 \
   --port 8010 \
   --trust-remote-code \
   --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer","kv_connector_extra_config":{"mooncake_protocol":"efa"}}'
```

### Decode Instance

```bash
vllm serve <model_path> -tp 8 \
   --port 8020 \
   --trust-remote-code \
   --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer","kv_connector_extra_config":{"mooncake_protocol":"efa"}}'
```

## Unit Tests

Run the EFA transport unit tests (requires EFA hardware):

```bash
./build/mooncake-transfer-engine/tests/efa_transport_test
```

The test suite includes:

| Test | Description |
|------|-------------|
| `InstallTransport` | Verify EFA transport installation |
| `LoopbackWrite` | Loopback write operation |
| `WriteAndRead` | Write then read with data integrity check |
| `MultiWrite` | Batch write (16 requests) |
| `StressMultipleBatches` | Stress test (20 batches x 8 requests) |

You can also run all unit tests via CTest:

```bash
cd build && ctest --output-on-failure
```

Environment variables for test configuration:

```bash
export MC_METADATA_SERVER=P2PHANDSHAKE     # default
export MC_LOCAL_SERVER_NAME=127.0.0.1:12345  # default
```

## Performance Benchmark

Use `transfer_engine_bench` to measure EFA transport throughput between two nodes.

### Target Node (receiver)

```bash
./build/mooncake-transfer-engine/example/transfer_engine_bench \
    --mode=target \
    --protocol=efa \
    --metadata_server=P2PHANDSHAKE
```

### Initiator Node (sender)

```bash
./build/mooncake-transfer-engine/example/transfer_engine_bench \
    --mode=initiator \
    --protocol=efa \
    --metadata_server=P2PHANDSHAKE \
    --segment_id=<target_hostname>:<target_port> \
    --operation=write \
    --duration=10 \
    --threads=8 \
    --block_size=65536 \
    --batch_size=128 \
    --buffer_size=1073741824 \
    --report_unit=GB
```

Replace `<target_hostname>:<target_port>` with the target node's address shown in the target's startup log (e.g., `ip-172-31-29-226:12345`).

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--block_size` | 65536 | Bytes per transfer request |
| `--batch_size` | 128 | Requests per batch |
| `--threads` | 12 | Concurrent submission threads |
| `--buffer_size` | 1 GB | Total buffer size |
| `--duration` | 10 | Test duration in seconds |
| `--operation` | read | `read` or `write` |
| `--report_unit` | GB | `GB\|GiB\|Gb\|MB\|MiB\|Mb` |

### Benchmark Results

Tested on two p6-b200.48xlarge instances (8 EFA devices each, 8×400 Gbps) in the same AWS placement group.

#### Optimized Results

With tuned parameters (`MC_SLICE_SIZE=262144`):

| Operation | Throughput | Configuration |
|-----------|-----------|---------------|
| **Write** | **167.63 GB/s** | threads=48, block_size=128KB, batch_size=128, MC_SLICE_SIZE=256KB |
| **Read**  | **171.89 GB/s** | threads=48, block_size=128KB, batch_size=128, MC_SLICE_SIZE=256KB |

#### Parameter Tuning Results

The following table shows how different parameters affect write throughput:

| block_size | threads | batch_size | MC_SLICE_SIZE | Throughput |
|-----------|---------|------------|---------------|-----------|
| 64KB  | 8  | 128 | default (64KB) | 69.47 GB/s |
| 256KB | 8  | 128 | default        | 70.09 GB/s |
| 64KB  | 16 | 128 | default        | 78.80 GB/s |
| 64KB  | 32 | 256 | default        | 87.65 GB/s |
| 64KB  | 64 | 256 | default        | 85.72 GB/s |
| 128KB | 32 | 128 | default        | 92.33 GB/s |
| 128KB | 32 | 128 | 128KB          | 152.26 GB/s |
| 128KB | 32 | 128 | 256KB          | 156.18 GB/s |
| 128KB | 48 | 128 | 256KB          | **160.34 GB/s** |
| 128KB | 64 | 128 | 256KB          | 158.82 GB/s |

Key findings:
- **MC_SLICE_SIZE** is the most impactful tuning parameter — increasing from default 64KB to 256KB nearly **doubles** throughput (92→160 GB/s)
- **block_size=128KB** outperforms 64KB by ~10-15%
- **threads=48** is optimal for 8 EFA devices; 64 threads shows slight diminishing returns
- **batch_size=128** is sufficient; increasing to 256+ causes "Cannot select device" errors at higher thread counts

#### Cross-Transport Comparison

| Transport | Throughput | Per-NIC Bandwidth | Notes |
|-----------|-----------|-------------------|-------|
| **EFA (tuned)** | **168-172 GB/s** | ~207-214 Gbps × 8 NICs | MC_SLICE_SIZE=256KB, threads=48 |
| **EFA (default)** | **69.47 GB/s** | ~86 Gbps × 8 NICs | Default parameters |
| TCP (iperf3 baseline) | 9.5 GB/s | 76 Gbps total | Kernel TCP stack, 8 parallel streams |
| TCP (Mooncake) | 0.11 GB/s | — | Mooncake TCP transport, unoptimized for throughput |

**EFA (tuned) vs TCP**: EFA delivers **17.7x** the raw TCP bandwidth by bypassing the kernel network stack.

**EFA vs RoCE RDMA**: On comparable 8×400 Gbps RoCE networks, Mooncake's RDMA transport achieves ~190 GB/s. Tuned EFA reaches **~88%** of RoCE performance, demonstrating that proper parameter tuning can largely close the gap between SRD-based EFA and hardware-offloaded RDMA.

### Tuning Tips

- **Set `MC_SLICE_SIZE=262144` (256KB)** — this is the single most important tuning knob, nearly doubling throughput from defaults
- Increase `--threads` to 32-48 to saturate multiple EFA devices (6 threads per device is a good starting point)
- Use `--block_size=131072` (128KB) for optimal per-request efficiency
- Keep `--batch_size=128`; higher values may cause device selection failures with many threads
- Allocate buffers on both NUMA nodes for balanced NIC utilization (the bench tool does this by default)
- Avoid `--block_size=256KB` or larger with many threads — this can trigger "Cannot select device" errors due to buffer boundary alignment across 8 EFA devices

## Technical Details

### Why libfabric instead of ibverbs?

AWS EFA exposes RDMA-like devices through the ibverbs interface, but does not support the full ibverbs API. Specifically:
- Queue Pair (QP) creation fails with "Operation not supported" (error 95)
- EFA requires using libfabric's `FI_EP_RDM` (Reliable Datagram Message) endpoint type

### EFA Transport Architecture

```
┌─────────────────────────────────────────────────────┐
│                   EfaTransport                       │
├─────────────────────────────────────────────────────┤
│  EfaContext (per device)                            │
│  ├── fi_info      (fabric info)                     │
│  ├── fid_fabric   (fabric handle)                   │
│  ├── fid_domain   (protection domain)               │
│  ├── fid_av       (address vector for peer lookup)  │
│  ├── fid_cq       (completion queues)               │
│  └── fid_mr       (memory regions)                  │
├─────────────────────────────────────────────────────┤
│  EfaEndpoint (per connection)                       │
│  ├── fid_ep       (RDM endpoint)                    │
│  ├── fi_addr_t    (peer address)                    │
│  └── local_addr   (local endpoint address)          │
└─────────────────────────────────────────────────────┘
```

### Thread Safety

The EFA transport requests `FI_THREAD_SAFE` from the libfabric provider and adds per-endpoint spinlocks to serialize `fi_write` calls. This is necessary because:

- Multiple submission threads may route slices to the same endpoint concurrently
- libfabric RDM endpoints default to `FI_THREAD_UNSPEC` (no thread safety guarantees)
- Concurrent `fi_write` without serialization corrupts provider internals, causing completions to silently vanish

CQ completion queues are polled by dedicated worker threads (one per EFA device) that run independently of submission threads.

### EFA vs RoCE RDMA

| Feature | EFA (libfabric SRD) | RoCE (ibverbs) |
|---------|--------------------|--------------------|
| Protocol | Scalable Reliable Datagram | RDMA over Converged Ethernet |
| Endpoint type | `FI_EP_RDM` (message-based) | Queue Pairs (true RDMA) |
| Write operation | Software-emulated via messages + ACKs | Hardware-offloaded one-sided RDMA |
| CPU overhead | Moderate (provider processes ACKs) | Minimal (NIC handles everything) |
| Throughput (8×400G) | ~170 GB/s (tuned) | ~190 GB/s |
| AWS availability | All EFA-enabled instances | Not available on AWS |

### Supported AWS Instance Types

- p6-b200.48xlarge (8 EFA devices, `rdmap*` naming)
- p5e.48xlarge (16 EFA devices, `rdmap*` naming)
- p4d.24xlarge (4 EFA devices)
- Other EFA-enabled instances

Use `fi_info -p efa` to list available EFA devices on your instance.

## Troubleshooting

### No EFA devices found

```
EfaTransport: No EFA devices found
```

Solution: Verify EFA is available with `fi_info -p efa`

### Permission denied

```
fi_fabric failed: Permission denied
```

Solution: Ensure proper permissions or run with sudo for testing

### libfabric not found

```
cannot find -lfabric
```

Solution: Verify `/opt/amazon/efa/lib` is in the library path:
```bash
export LD_LIBRARY_PATH=/opt/amazon/efa/lib:$LD_LIBRARY_PATH
```

### Workers hang under high concurrency

If `transfer_engine_bench` hangs with some workers never completing:

1. **Ensure both nodes are running the same build** — the CQ backpressure and thread-safety fixes must be present on both sides
2. **Reduce concurrency** to verify basic connectivity: `--threads=1 --batch_size=16`
3. **Check CQ poller threads**: logs should show "Started N CQ polling worker threads" where N matches the number of EFA devices
