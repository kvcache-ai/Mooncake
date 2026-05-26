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

> **Note:** `ctest --output-on-failure` without a filter runs every test in the build, including TCP / metadata / master-service suites that require an etcd server or a running `mooncake_master`. Those will fail or hang on a machine that is only provisioned for EFA testing — the failures are not EFA-specific. Use `-R 'efa'` to restrict the run to the EFA tests.

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

### 1. Target Node (receiver)

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

### 2. Initiator Node (sender)

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

> **CPU-to-CPU** (no GPUs): build with `-DUSE_CUDA=OFF`, **or** pass `--use_vram=false` to a CUDA-enabled binary. Drop `--gpu_id=-1` in that case — the bench will spread buffers across NUMA nodes instead.

> **Why `threads=16` and not 32:** the SRD shared endpoint caps outstanding WRs per NIC (default 256 — see `MC_MAX_WR`). With `threads × batch ≤ NICs × max_wr` the CQ never saturates; going higher triggers backoff and times out. 32 threads × 128 batch = 4096 slices chasing 16 × 256 = 4096 WRs has no headroom, so the steady-state config settles at 16 threads.

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

> **Note on EFA slicing:** EFA transport does not split each transfer
> into fixed-size slices the way RDMA transport does — each transfer
> is sent as a single `fi_write` / `fi_read` whose size equals
> `block_size`, round-robin'd across NICs per request. **`block_size`
> is the key tuning parameter** for EFA throughput.

> **Note:** `buffer_size` must be >= `block_size * batch_size * threads`. The benchmark auto-adjusts if too small.

### Benchmark Results

#### 1. p6-b300.48xlarge (B300, 16 EFA × 400 Gbps)

Tested on two p6-b300.48xlarge instances (Intel Xeon Platinum 8559C, 8× B300, 16 EFA devices) in the same AWS placement group. Numbers below are post-SRD-shared-endpoint (#1944) on a fresh `main` build with the DLAMI pytorch env (CUDA 13).

**GPU-to-GPU** (build with `-DUSE_CUDA=ON`, `--gpu_id=-1` for all 8 GPUs, `--buffer_size=2147483648`):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=16, batch=128 | 758.88 GB/s | 720.35 GB/s |
| block=1MB, threads=32, batch=64 | 753.32 GB/s | **755.78 GB/s** |
| **block=1MB, threads=32, batch=32** | **780.33 GB/s** | - |
| block=1MB, threads=64, batch=32 | 780.23 GB/s | - |

> **Peak: 780 GB/s write**, reaching ~97.5% of the 800 GB/s theoretical line rate (16×400 Gbps). GPUDirect RDMA bypasses DRAM entirely (HBM3e → PCIe switch → NIC), so performance is not bottlenecked by CPU memory bandwidth.

**CPU-to-CPU** (build with `-DUSE_CUDA=OFF`, or `--use_vram=false` on a CUDA build, `--buffer_size=4294967296`):

| Configuration | Write | Read |
|---------------|-------|------|
| **block=1MB, threads=32, batch=128** | **282.93 GB/s** | **270.47 GB/s** |
| block=1MB, threads=16, batch=128 | 282.04 GB/s | 249.67 GB/s |
| block=1MB, threads=32, batch=64 | 282.84 GB/s | 256.26 GB/s |

> CPU-to-CPU is bounded by DRAM bandwidth on the Xeon 8559C — write throughput is essentially flat across thread/batch combinations (~282 GB/s), confirming DRAM controller saturation rather than a NIC or in-flight-WR limit.

#### 2. p6-b200.48xlarge (B200, 8 EFA × 400 Gbps)

Tested on two p6-b200.48xlarge instances in the same AWS placement group.

> **Note:** numbers below predate the SRD shared-endpoint refactor (#1944) and
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

#### 3. p5en.48xlarge (H200, 16 EFA × 200 Gbps)

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

> **Peak write: 365 GB/s** at `threads=16, batch=128` — ~91% of the 400 GB/s theoretical line rate (16×200 Gbps). Write saturates on batch size, so `batch=128` outperforms smaller batches as long as `threads × batch ≤ 16 × 256 = 4096` (the shared-endpoint WR cap). **Peak read: 304 GB/s** at `threads=16, batch=32` — reads tolerate smaller in-flight queues, and throughput drops as batch grows.

**CPU-to-CPU** (build with `-DUSE_CUDA=OFF`, or `--use_vram=false` on a CUDA build, `--buffer_size=4294967296`):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=8, batch=128 | 210.76 GB/s | 209.93 GB/s |
| block=1MB, threads=16, batch=128 | 212.71 GB/s | 211.21 GB/s |
| **block=1MB, threads=16, batch=32** | 211.67 GB/s | **212.18 GB/s** |
| block=1MB, threads=32, batch=128 | 212.99 GB/s | 210.33 GB/s |
| **block=1MB, threads=48, batch=32** | **213.57 GB/s** | 206.92 GB/s |

> CPU-to-CPU is DRAM-bound — throughput is essentially flat (~205–214 GB/s) across every thread / batch combination that doesn't hit the WR cap. Peak write 213.57 GB/s, peak read 212.18 GB/s.

**block_size sweep** (p5en GPU-to-GPU, `threads=16 batch=128 buf=4GB --gpu_id=-1`):

| block | Write | Read |
|-------|-------|------|
| 64 KB (default) | 97.11 GB/s | 96.87 GB/s |
| 128 KB | 190.82 GB/s | 203.88 GB/s |
| 256 KB | 324.90 GB/s | 296.19 GB/s |
| 512 KB | 357.62 GB/s | 289.47 GB/s |
| **1 MB (recommended)** | **352.59 GB/s** | **301.14 GB/s** |
| 2 MB | 366.87 GB/s | 302.25 GB/s |

> The 64 KB default only reaches ~26% of peak. Write throughput climbs steeply up to ~512 KB and plateaus between 1 MB and 2 MB; read saturates at ~256 KB. 1 MB is the recommended value — within a few percent of the 2 MB peak with more headroom for `batch_size` under the shared-endpoint WR cap.

**buffer_size sweep** (p5en GPU-to-GPU, `threads=16 batch=128 block=1MB --gpu_id=-1`):

| buffer_size | Write | Read |
|-------------|-------|------|
| 2 GB (min: `block × batch × threads`) | 353.51 GB/s | 297.03 GB/s |
| 4 GB | 364.86 GB/s | 293.79 GB/s |

> `buffer_size` only needs to satisfy `buffer_size ≥ block_size × batch_size × threads` (the bench auto-adjusts if smaller, but silently). Anything larger than that minimum does not change throughput — 2 GB vs 4 GB differs by ~3% on write, read is flat within noise. The example commands use 4 GB because it is safe for any reasonable threads/batch combination without having to recompute the minimum.

#### 4. p5.48xlarge (H100, 32 EFA × 100 Gbps)

Tested on two p5.48xlarge instances (AMD EPYC 7R13, 8× H100 80GB, 32 EFA devices) in the same AWS placement group. Per-NIC line rate is half of p5en's, but with twice the NIC count the aggregate ceiling is the same 400 GB/s. The shared-endpoint WR cap scales with NIC count: `32 NICs × 256 = 8192` in-flight slots, so `threads × batch_size ≤ 8192` (vs 4096 on p5en).

**GPU-to-GPU** (build with `-DUSE_CUDA=ON`, `--gpu_id=-1` for all 8 GPUs, `--buffer_size=4294967296`):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=8, batch=128 | 335.11 GB/s | - |
| block=1MB, threads=16, batch=128 | 388.52 GB/s | 379.10 GB/s |
| block=1MB, threads=32, batch=64 | 388.83 GB/s | 379.78 GB/s |
| **block=1MB, threads=32, batch=128** | **388.90 GB/s** | **381.64 GB/s** |
| block=1MB, threads=16, batch=32 | - | 356.94 GB/s |
| block=1MB, threads=32, batch=32 | - | 380.66 GB/s |

> **Peak write: 389 GB/s** at `threads=32, batch=128` — ~97% of the 400 GB/s theoretical line rate (32×100 Gbps). The plateau is wide: any `(threads, batch)` between `(16, 128)` and `(32, 128)` lands within 0.1% of peak. **Peak read: 382 GB/s** at `threads=32, batch=128` — unlike p5en, reads on this host scale with batch size up to 128 because the wider 32-NIC fabric absorbs larger in-flight queues without backoff. `(32, 256)` and `(64, 128)` (both at the 8192 WR cap) fail with no headroom for retries.

**CPU-to-CPU** (build with `-DUSE_CUDA=OFF`, or `--use_vram=false` on a CUDA build, `--buffer_size=4294967296`):

| Configuration | Write | Read |
|---------------|-------|------|
| block=1MB, threads=16, batch=128 | 39.83 GB/s | 40.43 GB/s |
| block=1MB, threads=32, batch=64 | 47.31 GB/s | 48.06 GB/s |
| block=1MB, threads=32, batch=128 | 47.75 GB/s | 48.62 GB/s |
| block=1MB, threads=32, batch=32 | 55.66 GB/s | 56.68 GB/s |
| **block=1MB, threads=48, batch=16** | **63.60 GB/s** | 63.00 GB/s |
| **block=1MB, threads=64, batch=16** | 63.39 GB/s | 63.05 GB/s |
| block=1MB, threads=32, batch=16 | 63.21 GB/s | 60.82 GB/s |
| block=1MB, threads=96, batch=32 | 57.61 GB/s | 59.63 GB/s |

> **Peak: ~64 GB/s** on both write and read — far below the GPU-to-GPU number despite identical NIC count. The bottleneck is DDR4-3200 DRAM bandwidth on the EPYC 7R13 (Milan): `batch=16` consistently wins because larger in-flight queues only deepen DRAM contention without unlocking new NIC capacity. p5.48xlarge CPU-to-CPU runs around **3× slower than p5en** (DDR5 Xeon 8488C, ~213 GB/s) at the same NIC aggregate. For PD KV transfer, the GPU-to-GPU path is the relevant one.

### Single-host loopback

EFA NICs have no hardware loopback short-circuit: even when both endpoints resolve to the same host, `fi_write`/`fi_read` drive a real DMA round-trip through the EFA device. For deployments where the producer and consumer run as **separate processes on the same host** (single-machine development, benchmarks, co-located workers), this is strictly slower than libfabric's emulated RDMA path, which resolves the same-host case to a memcpy and skips the NIC entirely.

Set `FI_EFA_USE_DEVICE_RDMA=0` (a libfabric provider env, [documented by the EFA installer](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-runtime-tuning.html)) on processes that only do same-host transfers; leave it unset (default `1`) on cross-host or mixed processes. Mooncake does not wrap this knob — it is a provider-level flag resolved at `fi_getinfo` time, and a single `EfaTransport` instance may serve a mix of loopback and cross-host peers, so flipping it disables device RDMA for **every** transfer in the process. Apply it per-process based on that process's traffic pattern.

Measured on p5.48xlarge (1 NIC, ~1.2 GiB per `put_from` call, same-host producer/consumer):

| `FI_EFA_USE_DEVICE_RDMA` | per-write latency |
|---|---:|
| `1` (default after #2041, device RDMA) | ~830 ms |
| `0` (emulated, same-host memcpy fast path) | ~390 ms |

For reference, a cross-host `put_from` of the same payload (device RDMA, 1 NIC) is ~340 ms — i.e., device RDMA on a same-host loopback is *slower* than going over the wire to another host, because the NIC has no fast-path for loopback. Cross-host benchmarks are unaffected by this env: leave `FI_EFA_USE_DEVICE_RDMA` at its default on any process that also talks to remote peers.

### Tuning Tips

- **Use `--block_size=1048576` (1MB)** — the single most important knob. The 64 KB default reaches only ~26% of peak. 1 MB is within a few percent of the 2 MB plateau while leaving headroom for `batch_size` under the shared-endpoint WR cap.
- **Keep `threads × batch_size ≤ num_nics × max_wr`** — under the SRD shared endpoint each NIC carries one `fid_ep` with a 256 WR cap (`MC_MAX_WR`), giving `16 NICs × 256 = 4096` in-flight slots on a 16-NIC host (b300, b200, p5en) and `32 NICs × 256 = 8192` on p5. Exceeding this trips "timed out waiting for CQ drain". `threads=16, batch=128` is a solid baseline on 16-NIC hosts; on 32-NIC p5, `threads=32, batch=128` works the same way.
- **Write vs read:** write benefits from larger batches (peak at `batch=128`); on 16-NIC p5en read prefers smaller queues (peak at `batch=32`), but on 32-NIC p5 reads scale up to `batch=128` because the wider fabric absorbs larger in-flight queues.
- For **GPU-to-GPU**: pass `--gpu_id=-1` on **both** sides so buffers fan out across every GPU. Pinning a single GPU halves throughput because half the NICs end up cross-NUMA.
- For **CPU-to-CPU**: DRAM bandwidth is the ceiling. NUMA-split (separate initiator/target instances per NUMA node) can help reduce contention when one instance can't saturate both nodes.
- `--buffer_size` only needs `≥ block × batch × threads`; larger
  values do not improve throughput. The example commands use 4 GB
  because that is safe for any reasonable config.

### First-request latency

Peer addressing resolves lazily: `fi_av_insert()` and the metadata handshake fire on the first send to each `(local_NIC, peer_NIC)` pair. On 16-NIC hosts, the first few `submitTransfer` calls carry this cost before steady state.

**Measured on p5en (16 × 16 NICs, cross-node, 1 MB write, 3 reps, median):**

| | cold submit #0 (no warmup) | `warmupSegment()` (all 256 pairs) |
|---|---:|---:|
| SRD shared endpoint (#1944) | **26 ms** | **1.1 s** |
| Per-peer `fid_ep` (upstream main) | 99 ms | 17 s |
| Speedup | **~4×** | **~15×** |

The SRD shared-endpoint refactor (#1944) speeds up first-request latency two different ways:

- **Without any code change from callers** — the cold `submitTransfer` is ~4× faster (26 ms vs 99 ms), because the shared endpoint removes the per-peer `fi_endpoint` / `fi_enable` that used to dominate. This is what existing Mooncake callers (vLLM, SGLang, etc.) will see.
- **For callers that want sub-10 ms first-request latency**, an explicit eager-warmup API lets you pay the handshake cost up front, outside the critical path:
  - C++: `EfaTransport::warmupSegment(const std::string& segment_name)`
  - C: `int warmupEfaSegment(transfer_engine_t engine, const char *segment_name)`
  - Rust: `TransferEngine::warmup_efa_segment(name: &str)`
  - Python: `engine.warmup_efa_segment(segment_name)`

  Call once per peer right after `openSegment`. The call is idempotent. Under this refactor `warmupSegment` itself is ~15× faster than the pre-#1944 code (1.1 s vs 17 s), bounded by the peer's single-threaded handshake RPC daemon (`accept` + JSON parse serialized on one thread), so it scales linearly with the number of fresh NIC pairs.

vLLM and SGLang do not currently call `warmupSegment` — they go through the generic `TransferEngine` interface and pick up the 4× cold-submit speedup automatically. The API is there for direct Mooncake callers that want the larger win.

#### Reproducing the numbers — `efa_first_submit_probe`

`mooncake-transfer-engine/example/efa_first_submit_probe.cpp` is a two-host probe that isolates the two latency costs `transfer_engine_bench` hides inside its 10-second throughput average:

1. **Cold first-submit** — the handshake + `fi_av_insert()` that fires on the first send to each `(local_NIC, peer_NIC)` pair.
2. **Eager warmup** — how much of that is paid up front by an explicit `warmupSegment()` call.

It is EFA-specific (there is no `warmupSegment` on the RDMA / TCP transports — they establish connections at `connect` time, so "pre-warming" has no meaning there) and intentionally not wired into `ctest`: it needs two hosts.

**Run**:

```bash
# On the target host:
./build/mooncake-transfer-engine/example/efa_first_submit_probe \
    --mode=target \
    --metadata_server=P2PHANDSHAKE \
    --local_server_name=$(hostname):12345

# Note the "[target] ready, addr=<ip>:<port>" line, then on the initiator:
./build/mooncake-transfer-engine/example/efa_first_submit_probe \
    --mode=initiator \
    --metadata_server=P2PHANDSHAKE \
    --local_server_name=$(hostname):12346 \
    --segment_id=<target_ip>:<target_port> \
    --warmup=1 \
    --iters=5
```

**What it prints**:

```
warmup:   1094.74 ms (rc=0)           # present only with --warmup=1
submit #0:   6.95 ms                   # first user-visible submit
submit #1:   5.45 ms                   # steady state
submit #2:   5.92 ms
submit #3:   5.67 ms
submit #4:   0.09 ms
```

- `warmup:` is the wall time of `EfaTransport::warmupSegment()` — it opens every `(local_NIC, peer_NIC)` pair (16 × 16 = 256 on p5en) up front.
- `submit #0` through `#N-1` are the timings of single 1 MB `submitTransfer + poll`. With `--warmup=1` they are all in the steady-state regime. With `--warmup=0`, `#0` pays the handshake cost, `#1+` are steady state.

**Flags**:

| Flag | Default | What it controls |
|---|---|---|
| `--warmup` | `true` | Call `warmupSegment()` before the first submit |
| `--iters` | `5` | How many timed submits after warmup |
| `--xfer_size` | `1<<20` (1 MB) | Bytes per submit |
| `--buffer_size` | `1<<30` (1 GB) | Registered buffer size |
| `--local_server_name` | `hostname:12345` | Local metadata advertise name |

**Use cases**:

- **Deciding whether your application needs `warmupSegment()`**: if `submit #0` in the `--warmup=0` run is acceptable for your use case, you don't need to call `warmupSegment` at all.
- **Comparing PR branches**: run the probe on the same hardware on this PR's branch vs `main` (or whatever upstream you're benchmarking against) to see per-NIC-pair handshake cost directly, without having the number drowned in a 10-second throughput average.

## Usage with vLLM

### 1. Prefill Instance

```bash
VLLM_MOONCAKE_BOOTSTRAP_PORT=8998 \
vllm serve <model_path> -tp 8 \
   --port 8010 \
   --trust-remote-code \
   --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer","kv_connector_extra_config":{"mooncake_protocol":"efa"}}'
```

### 2. Decode Instance

```bash
vllm serve <model_path> -tp 8 \
   --port 8020 \
   --trust-remote-code \
   --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer","kv_connector_extra_config":{"mooncake_protocol":"efa"}}'
```

### 3. Router

Front the prefill / decode pair with `vllm-router`. **`PREFILL_HOST` / `DECODE_HOST` must be each node's reachable private IP, not `127.0.0.1`** — the router forwards these addresses to the peer for the KV handshake; localhost will fail with `Connection refused` and stall traffic.

```bash
vllm-router --policy round_robin \
  --vllm-pd-disaggregation \
  --prefill http://<prefill_ip>:8010 \
  --decode  http://<decode_ip>:8020 \
  --kv-connector mooncake \
  --host 0.0.0.0 --port 30000 \
  --intra-node-data-parallel-size 8
```

`--intra-node-data-parallel-size` should match the per-node DP size of your prefill / decode instances (8 in this example, matching `-tp 8`).

## Usage with SGLang

SGLang's PD-disaggregation Mooncake integration reads the transport from `MOONCAKE_PROTOCOL`. Set it to `efa` and select the Mooncake backend with `--disaggregation-transfer-backend mooncake`.

### 1. Apply EFA Patch (only if SGLang version predates PR #25083)

Older SGLang releases hardcode `"rdma"` in the transfer engine init. Since [SGLang PR #25083](https://github.com/sgl-project/sglang/pull/25083) the protocol is read from `MOONCAKE_PROTOCOL`, so once that PR is in your build (or upstream `main`) **this step is unnecessary** — skip to step 2.

If you are pinned to an older release, apply the [patch script](https://github.com/whn09/kimi-k2-sglang):

```bash
bash patch_sglang_efa.sh
```

The script is idempotent and safe to rerun.

### 2. Environment Variables

Only one Mooncake-specific env is required:

```bash
export MOONCAKE_PROTOCOL=efa
```

If your container does not already export libfabric/EFA paths in its `Dockerfile`, also set:

```bash
export FI_PROVIDER=efa
export FI_EFA_USE_DEVICE_RDMA=1
export LD_LIBRARY_PATH=/opt/amazon/efa/lib:$LD_LIBRARY_PATH
```

> **Note on additional `MC_*` knobs:** `MC_NUM_CQ_PER_CTX`, `MC_MAX_WR`, `MC_MAX_CQE_PER_CTX`, `MC_SLICE_SIZE`, and `MC_EFA_STRIPING_THRESHOLD` are **not** required at typical PD-disagg loads — the SRD shared-endpoint refactor (#1944) makes them redundant up to high concurrency on 1k/1k traffic. Treat them as emergency switches for CQ-overflow or long-running drift symptoms.

### 3. Prefill Instance

```bash
MOONCAKE_PROTOCOL=efa \
sglang serve <model_path> \
  --trust-remote-code \
  --tp 8 --dp 2 --enable-dp-attention --enable-dp-lm-head \
  --host 0.0.0.0 --port 8010 \
  --disaggregation-mode prefill \
  --disaggregation-transfer-backend mooncake \
  --disaggregation-bootstrap-port 8998
```

### 4. Decode Instance

```bash
MOONCAKE_PROTOCOL=efa \
sglang serve <model_path> \
  --trust-remote-code \
  --tp 8 --dp 2 --enable-dp-attention --enable-dp-lm-head \
  --host 0.0.0.0 --port 8020 \
  --disaggregation-mode decode \
  --disaggregation-transfer-backend mooncake \
  --disaggregation-bootstrap-port 8998
```

### 5. Router

Front the pair with `sglang_router` from the prefill host. **`PREFILL_HOST` must be the prefill node's reachable IP, not `127.0.0.1`** — the router forwards this address to the decode node for the bootstrap_room handshake; localhost will fail with `Connection refused` and stall traffic at 0/N.

```bash
python3 -m sglang_router.launch_router \
  --pd-disaggregation \
  --prefill "http://<prefill_ip>:8010" 8998 \
  --decode  "http://<decode_ip>:8020" \
  --policy round_robin \
  --host 0.0.0.0 --port 8000
```

The trailing `8998` after `--prefill` must match the prefill's `--disaggregation-bootstrap-port`.

## Technical Details

### Why libfabric instead of ibverbs?

AWS EFA exposes RDMA-like devices through the ibverbs interface, but does not support the full ibverbs API. Specifically:
- Queue Pair (QP) creation fails with "Operation not supported" (error 95)
- EFA requires using libfabric's `FI_EP_RDM` (Reliable Datagram Message) endpoint type

### EFA Transport Architecture

Under the SRD shared-endpoint model every peer is addressed through one `fid_ep` per local NIC — peers are AV-slot entries, not separate endpoints.

```
┌───────────────────────────────────────────────────────────┐
│                   EfaTransport                            │
├───────────────────────────────────────────────────────────┤
│  EfaContext (per local NIC)                               │
│  ├── fid_fabric     (fabric handle)                       │
│  ├── fid_domain     (protection domain)                   │
│  ├── fid_av         (address vector — one slot per peer)  │
│  ├── fid_cq         (completion queues)                   │
│  ├── fid_mr         (memory regions)                      │
│  ├── shared_ep_     (the single fid_ep that serves every  │
│  │                   peer via fi_addr_t lookup in the AV) │
│  └── peer_map_      (normalized nic_path -> EfaEndPoint)  │
├───────────────────────────────────────────────────────────┤
│  EfaEndPoint (per peer)                                   │
│  └── peer_fi_addr_  (AV slot index for this peer; sends   │
│                      route through the owning context's    │
│                      shared_ep_ with this fi_addr_t)      │
└───────────────────────────────────────────────────────────┘
```

### Thread Safety

The EFA transport requests `FI_THREAD_SAFE` at the domain level and guards the shared endpoint with a single `post_lock_` spinlock (one per `EfaContext`, i.e. one per local NIC) to serialize `fi_write`/`fi_read` calls. This is necessary because:

- Multiple submission threads may route slices through the same shared endpoint concurrently.
- libfabric's EFA RDM endpoints are not thread-safe for concurrent `fi_write`/`fi_read` even under `FI_THREAD_SAFE` at the domain level — concurrent posts corrupt provider internals and completions silently vanish.

CQ completion queues are polled by dedicated worker threads (one per EFA device) that run independently of submission threads.

### EFA vs RoCE RDMA

| Feature | EFA (libfabric SRD) | RoCE (ibverbs) |
|---------|--------------------|--------------------|
| Protocol | Scalable Reliable Datagram | RDMA over Converged Ethernet |
| Endpoint type | `FI_EP_RDM` (message-based) | Queue Pairs (true RDMA) |
| Write operation | Software-emulated via messages + ACKs | Hardware-offloaded one-sided RDMA |
| CPU overhead | Moderate (provider processes ACKs) | Minimal (NIC handles everything) |
| Throughput GPU-to-GPU (16×200G, p5en) | 365 GB/s (tuned) | N/A |
| Throughput CPU-to-CPU (16×200G, p5en) | 213 GB/s (tuned) | — |
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
