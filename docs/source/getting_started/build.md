# Build Guide

This document describes how to build Mooncake.

## Build From Source

### Recommended Version
- OS: Ubuntu 22.04 LTS+
- cmake: 3.20.x
- gcc: 9.4+

### Default Build

Install common build dependencies first. A stable Internet connection is
required because the script installs system packages, initializes submodules,
installs Go, installs the xxHash development package, and builds/installs
yalantinglibs from the `extern/yalantinglibs` submodule.

```bash
sudo bash dependencies.sh
```

Then build and install Mooncake:

```bash
mkdir build
cd build
cmake ..
make -j
sudo make install
```

### Build with NVMe-oF SSD Pool

To enable the NVMe-oF SSD pool, install the SPDK dependencies and build
Mooncake with `USE_NOF` enabled:

```bash
sudo bash dependencies.sh --with-spdk

mkdir build
cd build
cmake .. -DUSE_NOF=ON
make -j
sudo make install
```

`-DUSE_NOF=ON` builds the NoF registration APIs and deployment tools. Use
`-DUSE_NOF=OFF` or omit the option when the NVMe-oF SSD pool is not needed.

### Hardware Backend Setup

Run `sudo bash dependencies.sh` before using any of these backend-specific build
options. The script handles common dependencies; vendor SDKs and runtime
environment setup must be prepared separately.

| Hardware / backend | Build option | External SDK / setup | Environment and notes |
| --- | --- | --- | --- |
| NVIDIA CUDA / GPUDirect | `-DUSE_CUDA=ON` | Install CUDA 12.1+ and enable `nvidia-fs` for cuFile builds. | Add CUDA libraries to `LIBRARY_PATH` and `LD_LIBRARY_PATH`, for example `/usr/local/cuda/lib64`. |
| NVIDIA Multi-Node NVLink | `-DUSE_MNNVL=ON` | Requires CUDA. | Also set `-DUSE_CUDA=ON`. Not used with MUSA, HIP, or MACA builds. |
| Moore Threads MUSA | `-DUSE_MUSA=ON` | Install MUSA SDK and `mthreads-peermem` for GPUDirect RDMA. | Add `/usr/local/musa/lib` to `LIBRARY_PATH` and `LD_LIBRARY_PATH`. |
| Cambricon MLU | `-DUSE_MLU=ON` | Install Cambricon Neuware SDK. | Set `NEUWARE_HOME`, or pass `-DNEUWARE_ROOT=/path/to/neuware`. Use `-DMLU_INCLUDE_DIR` and `-DMLU_LIB_DIR` for custom layouts. |
| MetaX MACA | `-DUSE_MACA=ON` | Install MACA SDK. | Set `MACA_HOME`, or pass `-DMACA_ROOT=/path/to/maca`. Use `-DMACA_INCLUDE_DIR`, `-DMACA_LIB_DIR`, and `-DMACA_RUNTIME_LIBS` for custom layouts. |
| Huawei Ascend Direct | `-DUSE_ASCEND_DIRECT=ON` | Install Ascend CANN Toolkit and ADXL dependencies. | Source `/usr/local/Ascend/cann/set_env.sh` before configuring CMake. This is the recommended Ascend path. |
| Huawei Ascend UBSHMEM | `-DUSE_UBSHMEM=ON` | Install Ascend CANN Toolkit. Requires CANN >= 9.0.0, driver >= 26.0.0, Lingqu >= 1.5. | Source the CANN `set_env.sh` before configuring CMake. |
| AMD HIP / ROCm | `-DUSE_HIP=ON` | Install ROCm/HIP SDK. | Ensure HIP compiler, headers, and runtime libraries are visible to CMake. |
| Hygon DCU | `-DUSE_HYGON=ON` | Install DTK SDK. | Set `DTK_HOME`, or pass `-DDTK_ROOT=/path/to/dtk`. Use `-DDTK_INCLUDE_DIR` and `-DDTK_LIB_DIR` for custom layouts. |
| Iluvatar CoreX | `-DUSE_COREX=ON` | Install CoreX SDK. | Set `COREX_HOME`, or pass `-DCOREX_ROOT=/path/to/corex`. Use `-DCOREX_INCLUDE_DIR` and `-DCOREX_LIB_DIR` for custom layouts. |

```{admonition} GPU-Direct RDMA
:class: note
Mooncake can use the DMA-BUF path for GPU-Direct RDMA, which does **not**
require the `nvidia-peermem` kernel module. Set `WITH_NVIDIA_PEERMEM=0` before
starting Mooncake to use DMA-BUF. Set `WITH_NVIDIA_PEERMEM=1` to use the legacy
`ibv_reg_mr` path, which requires `nvidia-peermem`. See Section 3.7 of
https://docs.nvidia.com/cuda/gpudirect-rdma/ for `nvidia-peermem` installation
instructions.
```

## Use Mooncake in Docker Containers
Mooncake supports Docker-based deployment. You can either build the image from
this repository with `docker/mooncake.Dockerfile` or substitute a published
tag that matches the release you want to run.
For the container to use the host's network resources, you need to add the `--device` option when starting the container. The following is an example.

```
# In host
sudo docker build -f docker/mooncake.Dockerfile -t mooncake:from-source .
sudo docker run --net=host --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/rdma_cm --ulimit memlock=-1 -t -i mooncake:from-source /bin/bash
# Run transfer engine in container
cd /Mooncake-main/build/mooncake-transfer-engine/example
./transfer_engine_bench --device_name=ibp6s0 --metadata_server=10.1.101.3:2379 --mode=target --local_server_name=10.1.100.3
```

For SGLang HiCache deployments inside Docker, reserve HugeTLB pages on the host before starting the container and pass the allocator settings through the container environment:

```bash
python3 scripts/check_hicache_hugepage_requirements.py \
  --tp-size 4 \
  --hicache-size 64gb \
  --global-segment-size 8gb \
  --arena-pool-size 56gb \
  --available-hugetlb 512gb

sudo sysctl -w vm.nr_hugepages=262144
grep -E 'HugePages_Total|HugePages_Free|Hugepagesize' /proc/meminfo

sudo docker run --gpus all \
  --net=host \
  --ipc=host \
  --ulimit memlock=-1 \
  --shm-size=128g \
  --device=/dev/infiniband/uverbs0 \
  --device=/dev/infiniband/rdma_cm \
  -e MC_STORE_USE_HUGEPAGE=1 \
  -e MC_STORE_HUGEPAGE_SIZE=2MB \
  -e MOONCAKE_GLOBAL_SEGMENT_SIZE=8gb \
  -e MC_MMAP_ARENA_POOL_SIZE=56gb \
  -t -i mooncake:from-source /bin/bash
```

The `64gb` / `56gb` values above are tuned examples for large HiCache deployments, not defaults. The arena remains disabled unless you explicitly enable it, and if you enable it via gflag without an env override the default pool size is `8gb`. On smaller hosts, start with `8gb` or `16gb` and size upward with the helper. When you want the baseline direct-`mmap()` path instead of the arena, set `MC_DISABLE_MMAP_ARENA=1` (also accepts `true`, `yes`, or `on`) and omit `MC_MMAP_ARENA_POOL_SIZE`. Set it before the first Mooncake mmap-buffer allocation in the process. If you build the image from source with `docker/mooncake.Dockerfile`, that source-built image also installs the helper as `mooncake-hicache-sizing`.
Without `MC_STORE_USE_HUGEPAGE=1`, the arena may opportunistically try hugepages and then retry on regular pages if HugeTLB is unavailable. When `MC_STORE_USE_HUGEPAGE=1` is set, both the arena path and the direct-`mmap()` fallback path require HugeTLB pages. Mooncake will not silently degrade that explicit hugepage request to regular pages.

## Advanced Compile Options
The following options can be passed to `cmake ..`.

### Accelerator and Hardware Options

| Option | Default | Description |
| --- | --- | --- |
| `-DUSE_CUDA=ON/OFF` | `OFF` | Enable GPU memory support, including GPUDirect RDMA, NVMe-oF, and GPU-aware TCP transport. Required when transferring GPU memory, even when using TCP. |
| `-DUSE_MNNVL=ON/OFF` | `OFF` | Enable Multi-Node NVLink transport. Requires `-DUSE_CUDA=ON`; not used with MUSA, HIP, or MACA builds. |
| `-DUSE_MUSA=ON/OFF` | `OFF` | Enable Moore Threads GPU support via MUSA. |
| `-DUSE_MACA=ON/OFF` | `OFF` | Enable MetaX (Muxi) GPU support via MACA. |
| `-DUSE_HIP=ON/OFF` | `OFF` | Enable AMD GPU support via HIP/ROCm. |
| `-DUSE_HYGON=ON/OFF` | `OFF` | Enable Hygon DCU support via DTK SDK. Uses a CUDA-compatible runtime. |
| `-DUSE_COREX=ON/OFF` | `OFF` | Enable Iluvatar CoreX GPU support. Uses a CUDA-compatible runtime. |
| `-DUSE_MLU=ON/OFF` | `OFF` | Enable Cambricon MLU memory support via Neuware, including memory detection, topology discovery, and RDMA registration. |
| `-DUSE_ASCEND_DIRECT=ON/OFF` | `OFF` | Enable Ascend Direct transport and HCCS support via the ADXL engine. Recommended for Ascend builds. |
| `-DUSE_UBSHMEM=ON/OFF` | `OFF` | Enable Huawei Ascend NPU shared memory transport via CANN VMM APIs. |
| `-DUSE_INTRA_NVLINK=ON/OFF` | `OFF` | Enable intranode NVLink transport. |
| `-DUSE_CXL=ON/OFF` | `OFF` | Enable CXL support. |

### Vendor SDK Path Overrides

| Option | Applies when | Description |
| --- | --- | --- |
| `-DMACA_ROOT=/path/to/maca` | `-DUSE_MACA=ON` | Override the MACA SDK root. `MACA_HOME` is also honored; default is `/opt/maca`. |
| `-DMACA_INCLUDE_DIR=/path/to/include` | `-DUSE_MACA=ON` | Override the MACA include directory. |
| `-DMACA_LIB_DIR=/path/to/lib64` | `-DUSE_MACA=ON` | Override the MACA library directory. |
| `-DMACA_RUNTIME_LIBS="mcruntime;mxc-runtime64;rt"` | `-DUSE_MACA=ON` | Override MACA runtime libraries linked by `transfer_engine`. |
| `-DDTK_ROOT=/path/to/dtk` | `-DUSE_HYGON=ON` | Override the DTK SDK root. `DTK_HOME` is also honored; default is `/opt/dtk`. |
| `-DDTK_INCLUDE_DIR=/path/to/include` | `-DUSE_HYGON=ON` | Override the DTK include directory. |
| `-DDTK_LIB_DIR=/path/to/lib64` | `-DUSE_HYGON=ON` | Override the DTK library directory. |
| `-DCOREX_ROOT=/path/to/corex` | `-DUSE_COREX=ON` | Override the CoreX SDK root. `COREX_HOME` is also honored; default is `/usr/local/corex`. |
| `-DCOREX_INCLUDE_DIR=/path/to/include` | `-DUSE_COREX=ON` | Override the CoreX include directory. |
| `-DCOREX_LIB_DIR=/path/to/lib` | `-DUSE_COREX=ON` | Override the CoreX library directory. |
| `-DNEUWARE_ROOT=/path/to/neuware` | `-DUSE_MLU=ON` | Override the Neuware SDK root. `NEUWARE_HOME` is also honored; default is `/usr/local/neuware`. |
| `-DMLU_INCLUDE_DIR=/path/to/include` | `-DUSE_MLU=ON` | Override the Neuware include directory. |
| `-DMLU_LIB_DIR=/path/to/lib64` | `-DUSE_MLU=ON` | Override the Neuware library directory. |

### Transport and Metadata Options

| Option | Default | Description |
| --- | --- | --- |
| `-DUSE_EFA=ON/OFF` | `OFF` | Enable AWS Elastic Fabric Adapter transport via libfabric. See [EFA Transport](../design/transfer-engine/efa_transport.md). |
| `-DUSE_NOF=ON/OFF` | `OFF` | Build Mooncake Store with NVMe-oF SSD pool support. Use `sudo bash dependencies.sh --with-spdk` before enabling it. |
| `-DUSE_REDIS=ON/OFF` | `OFF` | Enable Redis-based metadata service for Transfer Engine. Requires hiredis. |
| `-DUSE_HTTP=ON/OFF` | `ON` | Enable HTTP-based metadata service. |
| `-DUSE_ETCD=ON/OFF` | `OFF` | Enable etcd-based metadata service. Requires Go 1.23+. |
| `-DSTORE_USE_ETCD=ON/OFF` | `OFF` | Enable etcd-based failover for Mooncake Store. Independent from `-DUSE_ETCD`. Requires Go 1.23+. |
| `-DSTORE_USE_REDIS=ON/OFF` | `OFF` | Enable Redis-based failover for Mooncake Store. Independent from `-DUSE_REDIS`. Requires hiredis. |
| `-DSTORE_USE_K8S_LEASE=ON/OFF` | `OFF` | Enable Kubernetes Lease-based failover for Mooncake Store. Cannot be enabled with `-DSTORE_USE_ETCD=ON` or non-legacy `-DUSE_ETCD=ON`. |

### Component Options

| Option | Default | Description |
| --- | --- | --- |
| `-DWITH_TE=ON/OFF` | `ON` | Build the Mooncake Transfer Engine component and sample code. |
| `-DWITH_STORE=ON/OFF` | `ON` | Build the Mooncake Store component. |
| `-DWITH_STORE_GO=ON/OFF` | `OFF` | Build Go bindings for Mooncake Store when `-DWITH_STORE=ON`. |
| `-DWITH_CONDUCTOR=ON/OFF` | `OFF` | Build the Mooncake Conductor service. |
| `-DWITH_P2P_STORE=ON/OFF` | `OFF` | Enable Golang support and build the P2P Store component. Requires Go 1.23+. |
| `-DWITH_RUST_EXAMPLE=ON/OFF` | `OFF` | Build the Transfer Engine Rust interface and sample code. |
| `-DWITH_STORE_RUST=ON/OFF` | `ON` | Build Mooncake Store Rust bindings and CMake Rust targets. |
| `-DWITH_EP=ON/OFF` | `OFF` | Build the EP and PG Python extensions for CUDA. Requires CUDA toolkit and PyTorch. Use `-DEP_TORCH_VERSIONS="2.12.1"` to build for specific PyTorch versions, or leave empty to use the currently installed torch. The CUDA version is detected automatically. |

To build only the Conductor target from a configured build tree, enable the
component and build `mooncake_conductor`:

```bash
cmake -S . -B build -DWITH_CONDUCTOR=ON
cmake --build build --target mooncake_conductor
```

### Build Behavior Options

| Option | Default | Description |
| --- | --- | --- |
| `-DBUILD_SHARED_LIBS=ON/OFF` | `OFF` | Build Transfer Engine as a shared library. |
| `-DBUILD_UNIT_TESTS=ON/OFF` | `ON` | Build unit tests. |
| `-DBUILD_EXAMPLES=ON/OFF` | `ON` | Build examples. |
| `-DSTORE_USE_JEMALLOC=ON/OFF` | `OFF` | Use jemalloc in the Mooncake Store master. |
