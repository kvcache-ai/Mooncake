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
cp mooncake-common/libasio.so ../mooncake-wheel/mooncake/

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
| `WarmupSegmentLoopback` | `warmupSegment()` handshake path + idempotent re-call |
| `WarmupSegmentNotFound` | `warmupSegment()` fails cleanly for an unknown segment |
| `RegisterMemoryBatch` | `registerLocalMemoryBatch` / `unregisterLocalMemoryBatch` round-trip |
| `LargeTransfer` | 128 MB buffer, 64 x 1 MB slices — exercises WR / CQ pacing |
| `RepeatedOpenSegment` | `openSegment()` on the same peer repeatedly still transfers correctly |

You can also run the EFA tests via CTest:

```bash
cd build && ctest --output-on-failure -R 'efa'
```

> **Note:** `ctest --output-on-failure` without a filter runs every test in
> the build, including TCP / metadata / master-service suites that require
> an etcd server or a running `mooncake_master`. Those will fail or hang on
> a machine that is only provisioned for EFA testing — the failures are not
> EFA-specific. Use `-R 'efa'` to restrict the run to the EFA tests.

## Performance Benchmark

Use `transfer_engine_bench` to measure EFA transport throughput between two nodes.

The following commands are the GPU-to-GPU configuration that produces
the headline numbers in the [Benchmark Results](#benchmark-results)
tables (≈ 350 GB/s write on a p5en.48xlarge pair, ≈ 302 GB/s on
p6-b200.48xlarge). Two things matter the most:

- `--gpu_id=-1` on **both** sides — this fans buffers across every GPU,
  which in turn lets both NUMA nodes' NICs saturate. Pinning a single
  GPU (the default `--gpu_id=0`) halves throughput because half the
  NICs end up cross-NUMA.
- `--block_size=1048576` (1MB, not the 64 KB default) — each block
  becomes one `fi_write` / `fi_read`, so larger blocks amortize
  per-op overhead and are the main knob for hitting line rate.

### Target Node (receiver)

```bash
./build/mooncake-transfer-engine/example/transfer_engine_bench \
    --mode=target \
    --protocol=efa \
    --metadata_server=P2PHANDSHAKE \
    --buffer_size=4294967296 \
    --gpu_id=-1
```

`--buffer_size` must be at least as large as the initiator's
`--buffer_size` — the initiator writes into offsets `[0, buffer_size)`
on the target, so keep these in sync.

### Initiator Node (sender)

```bash
./build/mooncake-transfer-engine/example/transfer_engine_bench \
    --mode=initiator \
    --protocol=efa \
    --metadata_server=P2PHANDSHAKE \
    --segment_id=<target_hostname>:<target_port> \
    --operation=write \
    --duration=10 \
    --threads=16 \
    --block_size=1048576 \
    --batch_size=128 \
    --buffer_size=4294967296 \
    --gpu_id=-1 \
    --report_unit=GB
```

Replace `<target_hostname>:<target_port>` with the target node's
address shown in the target's startup log (e.g., `ip-172-31-29-226:12345`).

> **CPU-to-CPU** (no GPUs): build with `-DUSE_CUDA=OFF`, **or** pass
> `--use_vram=false` to a CUDA-enabled binary. Drop `--gpu_id=-1` in
> that case — the bench will spread buffers across NUMA nodes instead.

> **Why `threads=16` and not 32:** the SRD shared endpoint caps
> outstanding WRs per NIC (default 256 — see `MC_MAX_WR`). With
> `threads × batch ≤ NICs × max_wr` the CQ never saturates; going
> higher triggers backoff and times out. 32 threads × 128 batch =
> 4096 slices chasing 16 × 256 = 4096 WRs has no headroom, so the
> steady-state config settles at 16 threads.

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--mode` | initiator | `initiator` (sender) or `target` (receiver) |
| `--protocol` | rdma | Transport protocol; use `efa` here |
| `--metadata_server` | `192.168.3.77:2379` | etcd address or `P2PHANDSHAKE` for standalone use |
| `--segment_id` | `192.168.3.76` | Initiator only: `<target_host>:<target_port>` from the target's startup log |
| `--operation` | read | `read` or `write` |
| `--block_size` | 65536 | Bytes per transfer request; **1 MB (1048576) is the main knob for EFA throughput** |
| `--batch_size` | 128 | Requests per batch |
| `--threads` | 12 | Concurrent submission threads (initiator) |
| `--buffer_size` | 1 GB | Buffer size (per GPU when `--gpu_id=-1`, otherwise total) |
| `--duration` | 10 | Test duration in seconds |
| `--report_unit` | GB | `GB\|GiB\|Gb\|MB\|MiB\|Mb\|KB\|KiB\|Kb` |
| `--use_vram` | true | Allocate from GPU VRAM (requires `-DUSE_CUDA=ON`); pass `--use_vram=false` for CPU-to-CPU on a CUDA build |
| `--gpu_id` | 0 | GPU device ID when `--use_vram=true`; `-1` fans buffers across every GPU and is what actually saturates all NICs in a GPU-to-GPU run |
| `--init_mem` | true | Zero-fill the allocated buffer; rarely needs to change |
| `--auto_discovery` | false | Auto-discover topology on init; off for reproducible runs |

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MC_EFA_STRIPING_THRESHOLD` | 2097152 | Transfers larger than this (bytes) are striped across all NICs |

> **Note on EFA slicing:** EFA transport does not split each transfer into fixed-size slices the way RDMA transport does. Transfers ≤ `MC_EFA_STRIPING_THRESHOLD` (default 2MB) are sent as a **single `fi_write`/`fi_read`** whose size equals `block_size`; transfers larger than the threshold are striped across all NICs (one chunk per NIC). So **`block_size` is the key tuning parameter** for EFA throughput.

> **Note:** `buffer_size` must be >= `block_size * batch_size * threads`. The benchmark auto-adjusts if too small.

### Benchmark Results

#### p6-b300.48xlarge (B300, 16 EFA × 400 Gbps)

Tested on two p6-b300.48xlarge instances (Intel Xeon Platinum 8559C, 8× B300, 16 EFA devices) in the same AWS placement group.

> **Note:** numbers below predate the SRD shared-endpoint refactor and
> current EFA tuning work. They are a lower bound for the current
> code; we will re-sweep and update when a B300 pair is available
> again.

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

#### p6-b200.48xlarge (B200, 8 EFA × 400 Gbps)

Tested on two p6-b200.48xlarge instances in the same AWS placement group.

> **Note:** numbers below predate the SRD shared-endpoint refactor and
> current EFA tuning work. They are a lower bound for the current
> code; we will re-sweep and update when a B200 pair is available
> again.

**GPU-to-GPU** (build with `-DUSE_CUDA=ON`, `--gpu_id=-1` for all 8 GPUs):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=32, batch=64, buf=2GB/GPU | 285-296 GB/s | 312 GB/s |
| **block=1MB, threads=16, batch=128, buf=2GB/GPU** | **302 GB/s** | **313 GB/s** |

**CPU-to-CPU** (build with `-DUSE_CUDA=OFF`):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=32, batch=128, buf=4GB | **222 GB/s** (stable over 6 runs) | **226 GB/s** |

#### p5en.48xlarge (H200, 16 EFA × 200 Gbps)

Tested on two p5en.48xlarge instances (Intel Xeon 8488C, 8× H200 141GB, 16 EFA devices) in the same AWS placement group.

**GPU-to-GPU** (build with `-DUSE_CUDA=ON`, `--gpu_id=-1` for all 8 GPUs, `--buffer_size=4294967296`):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=8, batch=128 | 318.71 GB/s | 277.06 GB/s |
| **block=1MB, threads=16, batch=128** | **365.66 GB/s** | 284.22 GB/s |
| **block=1MB, threads=16, batch=32** | 297.23 GB/s | **303.78 GB/s** |
| block=1MB, threads=32, batch=64 | 357.12 GB/s | 279.61 GB/s |
| block=1MB, threads=32, batch=128 | 364.21 GB/s | 250.90 GB/s |
| block=1MB, threads=48, batch=64 | 363.47 GB/s | 268.43 GB/s |

> **Peak write: 365 GB/s** at `threads=16, batch=128` — ~91% of the
> 400 GB/s theoretical line rate (16×200 Gbps). Write saturates on
> batch size, so `batch=128` outperforms smaller batches as long as
> `threads × batch ≤ 16 × 256 = 4096` (the shared-endpoint WR cap).
> **Peak read: 304 GB/s** at `threads=16, batch=32` — reads tolerate
> smaller in-flight queues, and throughput drops as batch grows.

**CPU-to-CPU** (build with `-DUSE_CUDA=OFF`, or `--use_vram=false` on a CUDA build, `--buffer_size=4294967296`):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=8, batch=128 | 210.76 GB/s | 209.93 GB/s |
| block=1MB, threads=16, batch=128 | 212.71 GB/s | 211.21 GB/s |
| **block=1MB, threads=16, batch=32** | 211.67 GB/s | **212.18 GB/s** |
| block=1MB, threads=32, batch=128 | 212.99 GB/s | 210.33 GB/s |
| **block=1MB, threads=48, batch=32** | **213.57 GB/s** | 206.92 GB/s |

> CPU-to-CPU is DRAM-bound — throughput is essentially flat (~205–214 GB/s)
> across every thread / batch combination that doesn't hit the WR cap.
> Peak write 213.57 GB/s, peak read 212.18 GB/s.

**block_size sweep** (p5en GPU-to-GPU, `threads=16 batch=128 buf=4GB --gpu_id=-1`):

| block | Write | Read |
|-------|-------|------|
| 64 KB (default) | 97.11 GB/s | 96.87 GB/s |
| 128 KB | 190.82 GB/s | 203.88 GB/s |
| 256 KB | 324.90 GB/s | 296.19 GB/s |
| 512 KB | 357.62 GB/s | 289.47 GB/s |
| **1 MB (recommended)** | **352.59 GB/s** | **301.14 GB/s** |
| 2 MB | 366.87 GB/s | 302.25 GB/s |

> The 64 KB default only reaches ~26% of peak. Write throughput
> climbs steeply up to ~512 KB and plateaus between 1 MB and 2 MB;
> read saturates at ~256 KB. 2 MB is the `MC_EFA_STRIPING_THRESHOLD`
> boundary — above it, each transfer is striped across all NICs
> (a different code path). 1 MB is the sweet spot: within 4% of the
> 2 MB peak and safely below the striping threshold.

**buffer_size sweep** (p5en GPU-to-GPU, `threads=16 batch=128 block=1MB --gpu_id=-1`):

| buffer_size | Write | Read |
|-------------|-------|------|
| 2 GB (min: `block × batch × threads`) | 353.51 GB/s | 297.03 GB/s |
| 4 GB | 364.86 GB/s | 293.79 GB/s |

> `buffer_size` only needs to satisfy `buffer_size ≥ block_size × batch_size × threads`
> (the bench auto-adjusts if smaller, but silently). Anything larger
> than that minimum does not change throughput — 2 GB vs 4 GB differs
> by ~3% on write, read is flat within noise. The example commands
> use 4 GB because it is safe for any reasonable threads/batch
> combination without having to recompute the minimum.

### Tuning Tips

- **Use `--block_size=1048576` (1MB)** — this is the most important tuning parameter for EFA. Each `block_size`-sized transfer becomes a single `fi_write`/`fi_read` call, so larger blocks amortize per-operation overhead. 1MB gives ~2× throughput over the 64KB default.
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
