# AWS EFA Transport for Mooncake

This document describes how to build and use Mooncake with AWS Elastic Fabric Adapter (EFA) support using libfabric.

## Prerequisites

### 1. AWS EFA Driver and libfabric

EFA driver and libfabric should be pre-installed on AWS instances with EFA support (e.g., p6-b300.48xlarge, p6-b200.48xlarge, p5en.48xlarge, p5e.48xlarge, p5.48xlarge).

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

Clone the repository and install all dependencies:

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
sudo ./dependencies.sh -y
```

This installs all system packages, git submodules (including pybind11 and yalantinglibs), and Go.

**Additional EFA-specific dependencies** (not covered by `dependencies.sh`):

```bash
# gflags is needed by transfer_engine_bench and EFA unit tests
sudo apt-get install -y libgflags-dev
```

> **Note:** The EFA driver and libfabric are **not** installed by `dependencies.sh`. They must be pre-installed on the instance (see section 1 above).

## Building Mooncake with EFA Support

### 1. Build with EFA Enabled

**GPU memory transfers (e.g., KV cache in vLLM):**

```bash
mkdir build && cd build

cmake .. \
    -DUSE_EFA=ON \
    -DUSE_CUDA=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo

make -j$(nproc)
```

> **Note:** `-DUSE_CUDA=ON` is required when transferring GPU memory (e.g., KV cache in vLLM). Without it, the TCP transport (used as fallback when `mooncake_protocol` is set to `"tcp"`) cannot detect GPU memory and will fail with "Bad address" (EFAULT) errors.

**CPU memory transfers only (no GPU dependency):**

```bash
mkdir build && cd build

cmake .. \
    -DUSE_EFA=ON \
    -DUSE_CUDA=OFF \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo

make -j$(nproc)
```

> **Note:** With `-DUSE_CUDA=OFF`, the benchmark tool uses DRAM buffers allocated via `numa_alloc_onnode`. This is useful for measuring EFA transport throughput independently of GPU hardware.

### 2. Install Python Package

```bash
# Copy built modules to wheel directory
cp mooncake-integration/engine.cpython-*.so ../mooncake-wheel/mooncake/
cp mooncake-integration/store.cpython-*.so ../mooncake-wheel/mooncake/
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

> **Tip:** For CPU-to-CPU benchmarks, prepend `CUDA_VISIBLE_DEVICES=""` to prevent the CUDA runtime from being initialized. Without it, `nvidia-smi` may show GPU memory usage (due to CUDA context initialization) even though the benchmark only uses DRAM.

Replace `<target_hostname>:<target_port>` with the target node's address shown in the target's startup log (e.g., `ip-172-31-29-226:12345`).

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--block_size` | 65536 | Bytes per transfer request |
| `--batch_size` | 128 | Requests per batch |
| `--threads` | 12 | Concurrent submission threads |
| `--buffer_size` | 1 GB | Total buffer size (per GPU when `--gpu_id=-1`) |
| `--duration` | 10 | Test duration in seconds |
| `--operation` | write | `read` or `write` |
| `--report_unit` | GB | `GB\|GiB\|Gb\|MB\|MiB\|Mb` |
| `--gpu_id` | 0 | GPU device ID; `-1` to use all GPUs (requires `-DUSE_CUDA=ON`) |

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MC_SLICE_SIZE` | 65536 | Slice size for RDMA transport. **Not used by EFA transport** (see note below). |
| `MC_EFA_STRIPING_THRESHOLD` | 2097152 | Transfers larger than this (bytes) are striped across all NICs |

> **Note on EFA slicing:** Unlike RDMA transport which splits every transfer into fixed `MC_SLICE_SIZE` chunks, EFA transport uses a different strategy: transfers ≤ `MC_EFA_STRIPING_THRESHOLD` (default 2MB) are sent as a **single `fi_write`/`fi_read`** whose size equals `block_size`; transfers larger than the threshold are striped across all NICs (one chunk per NIC). This means **`block_size` directly determines per-operation size** and is the key tuning parameter for EFA, while `MC_SLICE_SIZE` has no effect.

> **Note:** `buffer_size` must be >= `block_size * batch_size * threads`. The benchmark auto-adjusts if too small.

### Benchmark Results

#### p6-b200.48xlarge (B200, 8 EFA × 400 Gbps)

Tested on two p6-b200.48xlarge instances in the same AWS placement group.

**GPU-to-GPU** (build with `-DUSE_CUDA=ON`, `--gpu_id=-1` for all 8 GPUs):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=32, batch=64, buf=2GB/GPU | 285-296 GB/s | 312 GB/s |
| **block=1MB, threads=16, batch=128, buf=2GB/GPU** | **302 GB/s** | **313 GB/s** |

**CPU-to-CPU** (build with `-DUSE_CUDA=OFF`):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=32, batch=128, buf=4GB | **222 GB/s** (stable over 6 runs) | **226 GB/s** |

<details>
<summary>CPU Parameter Tuning History (p6-b200)</summary>

Earlier CPU-to-CPU tuning results (before EFA striping optimization, when `MC_SLICE_SIZE` was still used by EFA):

| block_size | threads | batch_size | MC_SLICE_SIZE | Throughput |
|-----------|---------|------------|---------------|-----------|
| 64KB  | 8  | 128 | default (64KB) | 69.47 GB/s |
| 128KB | 32 | 128 | default        | 92.33 GB/s |
| 128KB | 32 | 128 | 256KB          | 156.18 GB/s |
| 128KB | 48 | 128 | 256KB          | 160.34 GB/s |

> **Note:** These results predate the EFA striping optimization. With the current code, `MC_SLICE_SIZE` no longer affects EFA performance. Use `--block_size=1048576` (1MB) instead, which achieves 222 GB/s.

</details>

#### p6-b300.48xlarge (B300, 16 EFA × 400 Gbps)

Tested on two p6-b300.48xlarge instances (Intel Xeon Platinum 8559C, 8× B300, 16 EFA devices) in the same AWS placement group.

**GPU-to-GPU** (build with `-DUSE_CUDA=ON`, `--gpu_id=-1` for all 8 GPUs, `--buffer_size=2147483648`):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=16, batch=128 | 701 GB/s | **697 GB/s** |
| **block=1MB, threads=32, batch=64** | **752 GB/s** | 713 GB/s |
| block=1MB, threads=32, batch=32 | 751 GB/s | - |
| block=1MB, threads=64, batch=32 | 728 GB/s | - |

> **Peak: 752 GB/s write**, reaching ~94% of the 800 GB/s theoretical line rate (16×400 Gbps). GPUDirect RDMA bypasses DRAM entirely (HBM3e → PCIe switch → NIC), so performance is not bottlenecked by CPU memory bandwidth.

**CPU-to-CPU** (build with `-DUSE_CUDA=OFF`):

| Configuration | Write | Read |
|---------------|-------|------|
| **block=1MB, threads=32, batch=128, buf=4GB** | **230 GB/s** | 180 GB/s |
| block=16MB, threads=32, batch=8, buf=8GB (striping off) | 233 GB/s | - |

> CPU-to-CPU is bounded by DRAM bandwidth (~250 GB/s/socket on Xeon 8559C). Per-NIC sampling shows NUMA-0 NICs at 90 Gbps and NUMA-1 NICs at 53 Gbps, confirming DRAM controller saturation rather than NIC limit.

#### p5en.48xlarge (H200, 16 EFA × 200 Gbps)

Tested on two p5en.48xlarge instances (Intel Xeon 8488C, 8× H200 141GB, 16 EFA devices) in the same AWS placement group.

**GPU-to-GPU** (build with `-DUSE_CUDA=ON`, `--gpu_id=-1` for all 8 GPUs):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=8, batch=128, buf=1GB/GPU | 236 GB/s | 271 GB/s |
| block=1MB, threads=16, batch=128, buf=2GB/GPU | 271 GB/s | **297-308 GB/s** |
| **block=1MB, threads=32, batch=64, buf=2GB/GPU** | **337-347 GB/s** | 274 GB/s |

> GPU HBM bandwidth (>3 TB/s) eliminates the memory bottleneck, allowing full EFA utilization. Write and read have different optimal thread counts: write peaks at 32 threads, read peaks at 16 threads.

> **Note:** EFA memory region registration (fi_mr_reg) for GPU memory segfaults at 4GB+ per GPU. Use `--buffer_size=2147483648` (2GB) as the maximum per-GPU buffer.

**CPU-to-CPU** (build with `-DUSE_CUDA=OFF`):

| Configuration | Write | Read |
|---------------|-------|------|
| Single instance (block=1MB, threads=32, batch=128, buf=4GB) | 179 GB/s | 185 GB/s |
| NUMA-split (block=1MB, 2 instances, 8 NICs each, threads=16, buf=2GB) | **192 GB/s** | **182 GB/s** |

> CPU-to-CPU throughput is bottlenecked by DRAM bandwidth (~155 GB/s per NUMA node, measured with STREAM Copy).

#### Cross-Transport Comparison

| Transport | Throughput | Notes |
|-----------|-----------|-------|
| **EFA GPU-to-GPU (B300)** | **752 GB/s** | p6-b300.48xlarge, 16×400G, block=1MB, ~94% line rate |
| **EFA GPU-to-GPU (H200)** | **347 GB/s** | p5en.48xlarge, 16×200G, block=1MB |
| **EFA GPU-to-GPU (B200)** | **313 GB/s** | p6-b200.48xlarge, 8×400G, block=1MB |
| **EFA CPU-to-CPU (B300)** | **230 GB/s** | p6-b300.48xlarge, 16×400G, block=1MB, DRAM-limited |
| **EFA CPU-to-CPU (B200)** | **222 GB/s** | p6-b200.48xlarge, 8×400G, block=1MB, DRAM-limited |
| **EFA CPU-to-CPU (H200)** | **192 GB/s** | p5en.48xlarge, block=1MB, NUMA-split, DRAM-limited |
| EFA (default params) | 69.47 GB/s | Default block=64KB |
| TCP (iperf3 baseline) | 9.5 GB/s | Kernel TCP stack, 8 parallel streams |

**EFA vs RoCE RDMA**: On comparable 8×400 Gbps RoCE networks, Mooncake's RDMA transport achieves ~190 GB/s. Tuned EFA **exceeds** RoCE performance with GPU memory (313-347 GB/s) and on CPU-to-CPU (222 GB/s).

### Tuning Tips

- **Use `--block_size=1048576` (1MB)** — this is the most important tuning parameter for EFA. Each `block_size`-sized transfer becomes a single `fi_write`/`fi_read` call, so larger blocks amortize per-operation overhead. 1MB gives ~2× throughput over the 64KB default.
- `MC_SLICE_SIZE` has **no effect** on EFA transport (it only applies to RDMA transport). Use `block_size` instead.
- Increase `--threads` to 32-48 to saturate multiple EFA devices (2-4 threads per device is a good starting point)
- For **CPU-to-CPU**: use `--block_size=1048576` (1MB) with NUMA-split (separate instances per NUMA node) for best results
- For **GPU-to-GPU**: use `--block_size=1048576` (1MB), `--gpu_id=-1` (all GPUs), and `--buffer_size=2147483648` (2GB max per GPU). Write peaks at threads=32, read at threads=16
- Keep `--batch_size` such that `block_size * batch_size * threads <= buffer_size`
- Allocate buffers on both NUMA nodes for balanced NIC utilization (the bench tool does this by default for CPU mode)
- On 16-NIC instances (p5en), writes are NUMA-sensitive: 8 local-NUMA NICs reach 90 Gbps each, while 8 cross-NUMA NICs only reach ~20 Gbps without NUMA-split

### Eager endpoint warmup (first-request latency)

libfabric `FI_EP_RDM` endpoints resolve peer addresses lazily: `fi_av_insert()` and the metadata handshake fire on the first send to each `(local_ctx, peer_nic)` pair. On 16-NIC instances that gives `16 × N_peer_NICs` serial handshakes inside the first `submitTransfer`, which shows up as a single-digit-second first-batch stall (measured ~4 s on p6-B300 for a 100 × 0.5 MB batch; the first batch runs at <0.1 GB/s while the CQ drains, steady-state afterwards is unaffected).

Mooncake exposes an explicit eager-warmup API to eliminate the stall:

- C++: `EfaTransport::warmupSegment(const std::string& segment_name)`
- C: `int warmupEfaSegment(transfer_engine_t engine, const char *segment_name)`
- Rust: `TransferEngine::warmup_efa_segment(name: &str)`

Call it once per peer segment, right after `openSegment` (or after any metadata change that adds a new peer). Every `(local_ctx, peer_nic)` endpoint is connected concurrently via `std::async`; the critical path becomes `max(handshake RTT)` instead of `sum(handshake RTT)`. The call is idempotent — safe to re-run.

Measured on p6-B300 (16 local NICs × 16 peer NICs, dual-NUMA initiator, 100 × 0.5 MB batch):

| | first-batch latency | steady-state |
|---|---:|---:|
| No warmup | 4,043 ms | 141 GB/s |
| `warmup_efa_segment` (256 endpoints connected in 4.1 s) | **13.5 ms** (~300×) | 230 GB/s |

The warmup call itself takes roughly the same wall time as the stall it replaces — the win is that it's a one-time setup cost decoupled from the critical path of the first real transfer, not paid inside your latency budget.

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

## Usage with SGLang

SGLang's Mooncake integration currently hardcodes the `"rdma"` protocol. To use EFA transport, apply the provided patch and set environment variables.

### 1. Apply EFA Patch

SGLang's transfer engine initialization needs to be patched to read the protocol from an environment variable instead of using hardcoded `"rdma"`. Use the [patch script](https://github.com/whn09/kimi-k2-sglang):

```bash
bash patch_sglang_efa.sh
```

This is idempotent and safe to rerun.

### 2. Environment Variables

```bash
export MOONCAKE_PROTOCOL=efa
export FI_PROVIDER=efa
export FI_EFA_USE_DEVICE_RDMA=1
export GLOO_SOCKET_IFNAME=enp71s0  # adjust to your instance's primary interface
```

For multi-node expert parallelism (EP) deployments, also set:

```bash
export NVSHMEM_REMOTE_TRANSPORT=libfabric
export NVSHMEM_LIBFABRIC_PROVIDER=efa
```

> **Warning:** Do **not** set NVSHMEM variables on single-node deployments — doing so causes segmentation faults.

### 3. Docker Launch Example

```bash
docker run -d --name sglang \
  --runtime=nvidia --gpus all --network host \
  --privileged --shm-size=600g \
  --device=/dev/infiniband \
  -e MOONCAKE_PROTOCOL=efa \
  -e FI_PROVIDER=efa \
  -e FI_EFA_USE_DEVICE_RDMA=1 \
  <image> bash start.sh
```

> **Note:** Ensure the Docker image's libfabric version matches the host's EFA driver. If not, mount the host's EFA libraries into the container (see [Troubleshooting](#libfabric-version-mismatch-in-docker)).

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

The EFA transport requests `FI_THREAD_SAFE` from the libfabric provider and adds per-endpoint spinlocks to serialize `fi_write`/`fi_read` calls. This is necessary because:

- Multiple submission threads may route slices to the same endpoint concurrently
- libfabric RDM endpoints default to `FI_THREAD_UNSPEC` (no thread safety guarantees)
- Concurrent `fi_write`/`fi_read` without serialization corrupts provider internals, causing completions to silently vanish

CQ completion queues are polled by dedicated worker threads (one per EFA device) that run independently of submission threads.

### EFA vs RoCE RDMA

| Feature | EFA (libfabric SRD) | RoCE (ibverbs) |
|---------|--------------------|--------------------|
| Protocol | Scalable Reliable Datagram | RDMA over Converged Ethernet |
| Endpoint type | `FI_EP_RDM` (message-based) | Queue Pairs (true RDMA) |
| Write operation | Software-emulated via messages + ACKs | Hardware-offloaded one-sided RDMA |
| CPU overhead | Moderate (provider processes ACKs) | Minimal (NIC handles everything) |
| Throughput CPU-to-CPU (8×400G) | 222 GB/s (tuned) | ~190 GB/s |
| Throughput GPU-to-GPU (16×200G) | 347 GB/s (tuned) | N/A |
| Throughput GPU-to-GPU (8×400G) | 313 GB/s (tuned) | N/A |
| AWS availability | All EFA-enabled instances | Not available on AWS |

### Supported AWS Instance Types

- p6-b300.48xlarge (16 EFA devices × 400 Gbps = 6,400 Gbps, `rdmap*` naming)
- p6-b200.48xlarge (8 EFA devices × 400 Gbps = 3,200 Gbps, `rdmap*` naming)
- p5en.48xlarge (16 EFA devices × 200 Gbps = 3,200 Gbps, `rdmap*` naming)
- p5e.48xlarge (32 EFA devices × 100 Gbps = 3,200 Gbps, `rdmap*` naming)
- p5.48xlarge (32 EFA devices × 100 Gbps = 3,200 Gbps, `rdmap*` naming)
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

### Building on AWS Deep Learning AMI

On AWS Deep Learning AMI (e.g., Ubuntu 24.04), the system Python and CUDA toolkit are bundled inside the `/opt/pytorch` virtual environment. You must activate it and set CUDA paths before building:

```bash
# Activate the PyTorch environment (provides Python 3.13 + CUDA toolkit)
source /opt/pytorch/bin/activate

# Set CUDA paths (nvcc, headers and libs are inside the pip-installed nvidia packages)
export CUDA_HOME=/opt/pytorch/lib/python3.13/site-packages/nvidia/cu13
export PATH=$CUDA_HOME/bin:$PATH
export CPLUS_INCLUDE_PATH=$CUDA_HOME/include:$CPLUS_INCLUDE_PATH
export LD_LIBRARY_PATH=$CUDA_HOME/lib:$LD_LIBRARY_PATH
export LIBRARY_PATH=$CUDA_HOME/lib:$LIBRARY_PATH

# Build with CUDA support
cd ~/Mooncake
mkdir -p build && cd build
cmake .. -DUSE_EFA=ON -DUSE_CUDA=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

Without activating the environment, you may encounter:
- `Could not find nvcc, please set CUDAToolkit_ROOT` — nvcc is not in PATH
- `fatal error: cuda.h: No such file or directory` — CUDA headers not in include path, set `CPLUS_INCLUDE_PATH`
- `cannot find -lcudart: No such file or directory` — CUDA libs not in library path, set `LIBRARY_PATH` and `LD_LIBRARY_PATH`
- `ModuleNotFoundError: No module named 'mooncake.engine'` — `.so` built against wrong Python version (e.g., 3.12 vs 3.13)

### libfabric version mismatch in Docker

```
fi_ep_bind (av) failed: Function not implemented
```

or:

```
undefined reference to `efadv_query_qp_wqs@EFA_1.4'
```

This happens when the Docker container's libfabric version is older than the host's EFA driver. Check with `fi_info --version` on both host and container.

Solution: Mount the host's EFA libraries into the container:

```bash
docker run --gpus all --device=/dev/infiniband --net=host --privileged \
  -v /opt/amazon/efa:/opt/amazon/efa \
  -v /lib/x86_64-linux-gnu/libefa.so.1:/lib/x86_64-linux-gnu/libefa.so.1 \
  -v /lib/x86_64-linux-gnu/libefa.so:/lib/x86_64-linux-gnu/libefa.so \
  -v /lib/x86_64-linux-gnu/libibverbs.so.1:/lib/x86_64-linux-gnu/libibverbs.so.1 \
  -e LD_LIBRARY_PATH=/opt/amazon/efa/lib:$LD_LIBRARY_PATH \
  -it <image>
```

Then rebuild Mooncake inside the container to link against the host's libfabric.
