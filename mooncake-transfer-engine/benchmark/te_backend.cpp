// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdio>
#include <fstream>
#include <limits>

#include "te_backend.h"
#include "utils.h"
#include "split_output.h"
#include "common.h"
#include "char_util.h"
#include "numa_first_touch.h"

#include <linux/mman.h>
#include <sys/mman.h>

#if defined(USE_CUDA)
#include <bits/stdint-uintn.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include "cuda_alike.h"
#ifdef USE_MNNVL
#include "transport/nvlink_transport/nvlink_transport.h"
#endif
#elif defined(USE_SUNRISE)
#include "cuda_alike.h"
#endif

namespace mooncake {
namespace tent {

#ifdef USE_CUDA
static inline int getNumaNodeFromPciDevice(const std::string& pci_bdf) {
    std::string sysfs_path = "/sys/bus/pci/devices/" + pci_bdf + "/numa_node";
    std::ifstream numa_file(sysfs_path);
    if (!numa_file.is_open()) return -1;
    int numa_node = -1;
    numa_file >> numa_node;
    if (numa_file.fail()) return -1;
    return numa_node;
}
static inline int getCudaDeviceNumaID(int cuda_id) {
    char pci_bus_id[20];
    auto err = cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), cuda_id);
    if (err != cudaSuccess) {
        LOG(WARNING) << "cudaDeviceGetPCIBusId: " << cudaGetErrorString(err);
        return 0;
    }
    for (char* ch = pci_bus_id; (*ch = to_lower(*ch)); ch++);
    return getNumaNodeFromPciDevice(pci_bus_id);
}
#elif defined(USE_SUNRISE)
static inline int getSunriseDeviceNumaID(int dev_id) {
    char pci_bus_id[20];
    tangError_t err =
        tangDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), dev_id);
    if (err != tangSuccess) {
        LOG(WARNING) << "tangDeviceGetPCIBusId failed: "
                     << tangGetErrorString(err);
        return 0;
    }
    for (char* ch = pci_bus_id; (*ch = to_lower(*ch)); ch++);
    std::string sysfs_path =
        "/sys/bus/pci/devices/" + std::string(pci_bus_id) + "/numa_node";
    std::ifstream numa_file(sysfs_path);
    if (!numa_file.is_open()) return -1;
    int numa_node = -1;
    numa_file >> numa_node;
    if (numa_file.fail()) return -1;
    return numa_node;
}
#else
static inline int getCudaDeviceNumaID(int cuda_id) { return 0; }
#endif

// freeBatchID() returns BatchCleanupDeferred when a completion callback is
// still unwinding (#4569). The handle is already released and the engine
// finishes the cleanup, so for a finished batch this is success.
static mooncake::Status releaseFinishedBatch(mooncake::TransferEngine& engine,
                                             BatchID batch_id) {
    mooncake::Status status = engine.freeBatchID(batch_id);
    if (status.IsBatchCleanupDeferred()) return mooncake::Status::OK();
    return status;
}

volatile bool g_te_running = true;
volatile bool g_te_triggered_sig = false;

void signalHandlerV0(int signum) {
    if (g_te_triggered_sig) {
        LOG(ERROR) << "Received signal " << signum
                   << " again, forcefully terminating...";
        std::exit(EXIT_FAILURE);
    }
    LOG(INFO) << "Received signal " << signum << ", stopping target server...";
    g_te_running = false;
    g_te_triggered_sig = true;
}

#ifdef __linux__
struct IsolatedLayout {
    size_t guard = 0;
    size_t inner = 0;
    size_t map_len = 0;
};

static size_t dramPageSize() {
    const long ps = sysconf(_SC_PAGESIZE);
    return ps > 0 ? static_cast<size_t>(ps) : 4096;
}

static bool dramPageAlignUp(size_t n, size_t* aligned) {
    if (!aligned) return false;
    const size_t pg = dramPageSize();
    const size_t max = std::numeric_limits<size_t>::max();
    if (pg == 0 || n > max - (pg - 1)) return false;
    *aligned = (n + pg - 1) / pg * pg;
    return true;
}

static bool makeIsolatedLayout(size_t size, IsolatedLayout* out) {
    if (!out) return false;
    const size_t pg = dramPageSize();
    const size_t max = std::numeric_limits<size_t>::max();
    size_t inner = 0;
    if (pg == 0 || pg > max / 2 || !dramPageAlignUp(size, &inner)) return false;
    if (inner > max - 2 * pg) return false;
    out->guard = pg;
    out->inner = inner;
    out->map_len = inner + 2 * pg;
    return true;
}

// Sandwich the buffer with PROT_NONE pages so adjacent anonymous mmaps
// cannot merge. numa_maps is per-VMA; a merged VMA would mix nodes.
static void* mmapIsolatedAnonymous(const IsolatedLayout& layout) {
    void* base = mmap(nullptr, layout.map_len, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED) return MAP_FAILED;
    auto* p = static_cast<char*>(base);
    if (mprotect(p, layout.guard, PROT_NONE) != 0 ||
        mprotect(p + layout.guard + layout.inner, layout.guard, PROT_NONE) !=
            0) {
        munmap(base, layout.map_len);
        return MAP_FAILED;
    }
    return p + layout.guard;
}

static int munmapIsolatedAnonymous(void* addr, const IsolatedLayout& layout) {
    if (!addr) return 0;
    return munmap(static_cast<char*>(addr) - layout.guard, layout.map_len);
}

// Place DRAM with CPU affinity + first-touch so pages land on `node` even
// when mbind/get_mempolicy are denied. If affinity cannot be saved, do not
// pin: a later restore would be skipped and the thread would stay bound.
static void* allocateDramOnNode(size_t size, int node,
                                const IsolatedLayout& layout) {
    CpuAffinityGuard guard(node);
    if (!guard.pinned()) {
        LOG(WARNING) << "tebench: allocating cpu:" << node
                     << " without pinning to that node";
    }
    // Do not MAP_POPULATE: it can commit pages before this thread has
    // actually migrated onto `node`.
    void* buf = mmapIsolatedAnonymous(layout);
    if (buf == MAP_FAILED) {
        PLOG(ERROR) << "mmap DRAM buffer failed, size=" << size
                    << " node=" << node;
        return nullptr;
    }
    if (numa_available() >= 0 && node >= 0) {
        numa_tonode_memory(buf, size, node);
    }
    firstTouchPages(buf, size);
    return buf;
}
#else
static void firstTouchPages(void* addr, size_t size) {
    if (!addr || size == 0) return;
    auto* p = static_cast<char*>(addr);
    p[0] = 0;
    p[size - 1] = 0;
}

static void* allocateDramOnNode(size_t size, int node) {
    return numa_alloc_onnode(size, node);
}
#endif

static void* allocateMemoryPool(size_t size, int buffer_id,
                                bool from_vram = false) {
#if defined(USE_CUDA) || defined(USE_SUNRISE)
    if (from_vram) {
        int gpu_id = buffer_id;
        void* d_buf;
        LOG(INFO) << "Allocating memory on GPU " << gpu_id;
        cudaSetDevice(gpu_id);
#ifdef USE_MNNVL
        d_buf = mooncake::NvlinkTransport::allocatePinnedLocalMemory(size);
#else
        cudaError_t alloc_err = cudaMalloc(&d_buf, size);
        if (alloc_err != cudaSuccess) {
            LOG(ERROR) << "cudaMalloc failed on GPU " << gpu_id
                       << ": err=" << alloc_err << " ("
                       << cudaGetErrorString(alloc_err) << ")"
                       << " size=" << size;
#ifdef USE_SUNRISE
            LOG(ERROR) << "Sunrise classic benchmark VRAM allocation failed. "
                       << "Try a smaller -total_buffer_size, for example "
                       << "-total_buffer_size=4096 for a minimal smoke test.";
#endif
            return nullptr;
        }
#endif
        LOG(INFO) << "Allocated GPU memory at " << d_buf << " on GPU "
                  << gpu_id;
        return d_buf;
    }
#endif
#ifndef __linux__
    return allocateDramOnNode(size, buffer_id);
#else
    (void)size;
    (void)buffer_id;
    return nullptr;
#endif
}

static int parseIndex(const std::string& loc) {
    auto pos = loc.find(':');
    if (pos == std::string::npos || pos + 1 >= loc.size()) {
        throw std::invalid_argument("Invalid loc format: " + loc);
    }
    return std::stoi(loc.substr(pos + 1));
}

// POSIX shm mmap does not place pages. Pin the allocating thread then
// first-touch so pages land on `node` even when mbind is denied. Callers
// must map with populate=false so this is the first access.
static void bindShmBufferToNode(void* addr, size_t size, int node) {
    if (!addr || size == 0) return;
#ifdef __linux__
    if (numa_available() < 0 || node < 0 ||
        node >= numa_num_configured_nodes()) {
        LOG(WARNING) << "tebench: cannot pin shm buffer to cpu:" << node;
        firstTouchPages(addr, size);
        return;
    }
    CpuAffinityGuard guard(node);
    if (!guard.pinned()) {
        LOG(WARNING) << "tebench: shm first-touch cpu:" << node
                     << " without pinning to that node";
    } else if (numa_available() >= 0) {
        numa_tonode_memory(addr, size, node);
    }
    firstTouchPages(addr, size);
#else
    (void)node;
    firstTouchPages(addr, size);
#endif
}

// Resolve the node this buffer occupies. The VMA must cover only `[addr,
// addr+size)` — a merged anonymous mapping would mix adjacent buffers.
static std::string cpuLocationFromPages(void* addr, size_t size,
                                        int expected_node) {
    const auto expected = "cpu:" + std::to_string(expected_node);
#ifdef __linux__
    uintptr_t vma_start = 0, vma_end = 0;
    if (!vmaRangeFor(addr, &vma_start, &vma_end)) {
        LOG(ERROR) << "tebench: cannot find VMA for DRAM buffer intended for "
                   << expected;
        return {};
    }
    const uintptr_t buf = reinterpret_cast<uintptr_t>(addr);
    size_t aligned = 0;
    if (!dramPageAlignUp(size, &aligned) ||
        buf > std::numeric_limits<uintptr_t>::max() - aligned) {
        LOG(ERROR) << "tebench: DRAM buffer intended for " << expected
                   << " has a size that overflows VMA bounds";
        return {};
    }
    const uintptr_t expect_end = buf + aligned;
    if (vma_start != buf || vma_end < buf + size || vma_end > expect_end) {
        LOG(ERROR) << "tebench: DRAM buffer intended for " << expected
                   << " shares a VMA with other mappings ([" << std::hex
                   << vma_start << "," << vma_end << ") vs buffer " << buf
                   << " size " << std::dec << size
                   << "); refuse to register a mixed range";
        return {};
    }
    const std::string maps = numaMapsLineFor(addr);
    const auto placement = parseNumaMapsPlacement(maps);
    if (placement.dominant_node < 0) {
        LOG(ERROR) << "tebench: cannot resolve NUMA node for DRAM buffer "
                      "intended for "
                   << expected << " (" << maps << ")";
        return {};
    }
    if (placement.node_count > 1) {
        LOG(ERROR) << "tebench: DRAM buffer intended for " << expected
                   << " is split across NUMA nodes (" << maps
                   << "); free memory on that node and retry";
        return {};
    }
    auto location = "cpu:" + std::to_string(placement.dominant_node);
    if (placement.dominant_node != expected_node) {
        LOG(WARNING) << "tebench: DRAM buffer intended for " << expected
                     << " is on " << location << " (" << maps << ")";
    } else {
        LOG(INFO) << "tebench: DRAM buffer " << expected << " addr=" << addr
                  << " size=" << size << " " << maps;
    }
    return location;
#else
    (void)size;
    const auto entries = getMemoryLocation(addr, 1, true);
    if (!entries.empty() && entries[0].location.rfind("cpu:", 0) == 0) {
        if (parseIndex(entries[0].location) != expected_node) {
            LOG(WARNING) << "tebench: DRAM buffer intended for " << expected
                         << " is on " << entries[0].location;
        }
        return entries[0].location;
    }
    LOG(ERROR) << "tebench: cannot resolve NUMA node for DRAM buffer intended "
                  "for "
               << expected;
    return {};
#endif
}

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << 26)
#endif
#ifndef MAP_HUGE_1GB
#define MAP_HUGE_1GB (30 << 26)
#endif
#ifndef MAP_HUGE_512MB
#define MAP_HUGE_512MB (29 << 26)
#endif

static size_t hugepageBytes() {
    return XferBenchConfig::hugepage_size == 0
               ? mooncake::SharedMemoryOptions::kHugepage2MB
               : XferBenchConfig::hugepage_size;
}

static int hugepageMmapFlags(size_t hp, bool populate) {
    int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB;
    if (populate) flags |= MAP_POPULATE;
    if (hp == mooncake::SharedMemoryOptions::kHugepage2MB)
        flags |= MAP_HUGE_2MB;
    else if (hp == mooncake::SharedMemoryOptions::kHugepage512MB)
        flags |= MAP_HUGE_512MB;
    else if (hp == mooncake::SharedMemoryOptions::kHugepage1GB)
        flags |= MAP_HUGE_1GB;
    return flags;
}

static void* allocateHugepageOnNode(size_t size, int node) {
    const size_t hp = hugepageBytes();
    if (hp == 0 || size % hp != 0) {
        LOG(ERROR) << "RDMA hugepage buffer " << size
                   << " is not a multiple of hugepage size " << hp;
        return nullptr;
    }
#ifdef __linux__
    CpuAffinityGuard guard(node);
    const bool pinned = guard.pinned();
    if (!pinned) {
        LOG(WARNING) << "tebench: hugepage first-touch cpu:" << node
                     << " without pinning to that node";
    }
    void* buf = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     hugepageMmapFlags(hp, pinned), -1, 0);
#else
    void* buf = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     hugepageMmapFlags(hp, true), -1, 0);
#endif
    if (buf == MAP_FAILED) {
        PLOG(ERROR) << "MAP_HUGETLB mmap failed for " << size
                    << " bytes (hp=" << hp
                    << "); refusing 4K fallback to avoid blowing NIC PTEs";
        return nullptr;
    }
#ifdef __linux__
    if (pinned && numa_available() >= 0 && node >= 0) {
        numa_tonode_memory(buf, size, node);
    }
    firstTouchPages(buf, size);
#else
    auto* p = static_cast<char*>(buf);
    for (size_t off = 0; off < size; off += hp) p[off] = 0;
    if (size > 0) p[size - 1] = 0;
#endif
    return buf;
}

static void freeMemoryPool(void* addr, size_t size) {
#if defined(USE_CUDA) && defined(USE_MNNVL)
    CUmemGenericAllocationHandle handle;
    auto result = cuMemRetainAllocationHandle(&handle, addr);
    if (result == CUDA_SUCCESS) {
        mooncake::NvlinkTransport::freePinnedLocalMemory(addr);
        return;
    }
#endif
#if defined(USE_CUDA) || defined(USE_SUNRISE)
    cudaPointerAttributes attributes;
    cudaPointerGetAttributes(&attributes, addr);

    if (attributes.type == cudaMemoryTypeDevice) {
        cudaFree(addr);
    } else if (attributes.type == cudaMemoryTypeHost ||
               attributes.type == cudaMemoryTypeUnregistered) {
        numa_free(addr, size);
    } else {
        LOG(ERROR) << "Unknown memory type, " << addr << " " << attributes.type;
    }
#else
    numa_free(addr, size);
#endif
}

int TEBenchRunner::allocateBuffers() {
    auto total_buffer_size = XferBenchConfig::total_buffer_size;
    const bool use_shm = engine_->getTransport("shm") != nullptr;
    if (XferBenchConfig::seg_type == "DRAM") {
        int num_buffers = numa_num_configured_nodes();
        pinned_buffer_list_.resize(num_buffers, nullptr);
#ifdef __linux__
        IsolatedLayout isolated{};
#endif
        if (use_shm) {
            dram_backing_ = DramBacking::kSharedMemory;
        } else if (XferBenchConfig::use_hugepage) {
            dram_backing_ = DramBacking::kMmap;
        } else {
#ifdef __linux__
            if (!makeIsolatedLayout(total_buffer_size, &isolated)) {
                LOG(ERROR) << "tebench: total_buffer_size=" << total_buffer_size
                           << " overflows isolated mmap length";
                return -1;
            }
            mmap_guard_ = isolated.guard;
            mmap_span_ = isolated.map_len;
            dram_backing_ = DramBacking::kIsolatedMmap;
#else
            dram_backing_ = DramBacking::kNuma;
#endif
        }
        for (int i = 0; i < num_buffers; ++i) {
            std::string location = "cpu:" + std::to_string(i);
            if (dram_backing_ == DramBacking::kSharedMemory) {
                mooncake::SharedMemoryOptions opt;
                // Populate after pinning; MAP_POPULATE here would first-touch
                // on the allocating thread's current node.
                opt.populate = false;
                if (XferBenchConfig::use_hugepage) {
                    opt.use_hugepage = true;
                    opt.hugepage_size = XferBenchConfig::hugepage_size;
                    opt.hugetlbfs_path = XferBenchConfig::hugetlbfs_path;
                }
                pinned_buffer_list_[i] =
                    engine_->allocateSharedMemory(total_buffer_size, opt);
                if (pinned_buffer_list_[i]) {
                    bindShmBufferToNode(pinned_buffer_list_[i],
                                        total_buffer_size, i);
                    location = cpuLocationFromPages(pinned_buffer_list_[i],
                                                    total_buffer_size, i);
                }
            } else if (dram_backing_ == DramBacking::kMmap) {
                pinned_buffer_list_[i] =
                    allocateHugepageOnNode(total_buffer_size, i);
                if (pinned_buffer_list_[i]) {
                    location = cpuLocationFromPages(pinned_buffer_list_[i],
                                                    total_buffer_size, i);
                }
            } else {
#ifdef __linux__
                pinned_buffer_list_[i] =
                    allocateDramOnNode(total_buffer_size, i, isolated);
#else
                pinned_buffer_list_[i] =
                    allocateDramOnNode(total_buffer_size, i);
#endif
                if (pinned_buffer_list_[i]) {
                    location = cpuLocationFromPages(pinned_buffer_list_[i],
                                                    total_buffer_size, i);
                }
            }
            if (!pinned_buffer_list_[i]) return -1;
            if (location.empty()) {
                LOG(ERROR) << "Failed to resolve NUMA location for DRAM buffer "
                           << i;
                return -1;
            }
            if (engine_->registerLocalMemory(pinned_buffer_list_[i],
                                             total_buffer_size, location)) {
                LOG(ERROR) << "Failed to register DRAM buffer " << i;
                return -1;
            }
        }
        if (use_shm) {
            LOG(INFO) << "tebench: DRAM buffers allocated from "
                      << (XferBenchConfig::use_hugepage ? "hugetlbfs"
                                                        : "POSIX shm")
                      << " and bound to NUMA nodes";
        } else if (XferBenchConfig::use_hugepage) {
            LOG(INFO) << "tebench: DRAM buffers allocated with MAP_HUGETLB "
                         "hugepages (hp="
                      << hugepageBytes() << ") for RDMA MR registration";
        } else {
            LOG(INFO) << "tebench: DRAM allocated " << num_buffers
                      << " NUMA-local buffers via CPU affinity + first-touch";
        }
#if defined(USE_CUDA) || defined(USE_SUNRISE)
    } else if (XferBenchConfig::seg_type == "VRAM") {
        int gpu_count = 0;
        cudaGetDeviceCount(&gpu_count);
        int start_gpu = 0, num_buffers = gpu_count;
        if (XferBenchConfig::local_gpu_id != -1) {
            start_gpu = XferBenchConfig::local_gpu_id;
            num_buffers = 1;
            LOG_ASSERT(start_gpu >= 0 && start_gpu < gpu_count)
                << "local_gpu_id " << start_gpu << " out of range [0, "
                << gpu_count << ")";
        }
        pinned_buffer_list_.resize(num_buffers, nullptr);
        for (int i = 0; i < num_buffers; ++i) {
            int gpu_id = start_gpu + i;
            auto location = std::string(GPU_PREFIX) + std::to_string(gpu_id);
            pinned_buffer_list_[i] =
                allocateMemoryPool(total_buffer_size, gpu_id, true);
            if (!pinned_buffer_list_[i]) return -1;
            engine_->registerLocalMemory(pinned_buffer_list_[i],
                                         total_buffer_size, location);
        }
#endif
    } else {
        LOG(ERROR) << "Unknown seg_type: " << XferBenchConfig::seg_type;
    }
    return 0;
}

int TEBenchRunner::freeBuffers() {
    const uint64_t abandoned = abandoned_batches_.load();
    if (abandoned != 0) {
        // A transport may still copy into these buffers; the process is
        // exiting, so leaving them allocated costs nothing.
        LOG(WARNING) << abandoned
                     << " transfer batch(es) were still in flight when the "
                        "run stopped; leaving the local buffers allocated";
        pinned_buffer_list_.clear();
        dram_backing_ = DramBacking::kNone;
        mmap_guard_ = 0;
        mmap_span_ = 0;
        return 0;
    }
    auto total_buffer_size = XferBenchConfig::total_buffer_size;
    for (size_t i = 0; i < pinned_buffer_list_.size(); ++i) {
        if (!pinned_buffer_list_[i]) continue;
        if (dram_backing_ == DramBacking::kSharedMemory) {
            engine_->freeSharedMemory(pinned_buffer_list_[i]);
        } else {
            engine_->unregisterLocalMemory(pinned_buffer_list_[i]);
            if (dram_backing_ == DramBacking::kIsolatedMmap) {
#ifdef __linux__
                IsolatedLayout layout;
                layout.guard = mmap_guard_;
                layout.map_len = mmap_span_;
                if (munmapIsolatedAnonymous(pinned_buffer_list_[i], layout) !=
                    0) {
                    PLOG(WARNING) << "munmap DRAM buffer failed";
                }
#else
                if (munmap(pinned_buffer_list_[i], total_buffer_size) != 0) {
                    PLOG(WARNING) << "munmap DRAM buffer failed";
                }
#endif
            } else if (dram_backing_ == DramBacking::kMmap) {
                if (munmap(pinned_buffer_list_[i], total_buffer_size) != 0) {
                    PLOG(WARNING) << "munmap DRAM buffer failed";
                }
            } else {
                freeMemoryPool(pinned_buffer_list_[i], total_buffer_size);
            }
        }
    }
    pinned_buffer_list_.clear();
    dram_backing_ = DramBacking::kNone;
    mmap_guard_ = 0;
    mmap_span_ = 0;
    return 0;
}

TEBenchRunner::TEBenchRunner() {
    signal(SIGINT, signalHandlerV0);
    signal(SIGTERM, signalHandlerV0);
    // Disable auto-discovery when an explicit non-RDMA xport is requested
    // (e.g. flagcx, shm) so we can installTransport() ourselves below.
    bool auto_disc = XferBenchConfig::xport_type != "flagcx" &&
                     XferBenchConfig::xport_type != "shm" &&
                     XferBenchConfig::xport_type != "efa";
    engine_ = std::make_unique<mooncake::TransferEngine>(auto_disc);
    auto conn_str = XferBenchConfig::metadata_type == "p2p"
                        ? "P2PHANDSHAKE"
                        : XferBenchConfig::metadata_url_list;
    auto seg_name = (XferBenchConfig::metadata_type == "p2p" &&
                     XferBenchConfig::seg_name.empty())
                        ? mooncake::getHostname()
                        : XferBenchConfig::seg_name;
    engine_->init(conn_str, seg_name);
#ifdef USE_SUNRISE
    // Sunrise IPC handles must be produced by the installed transport before
    // the benchmark starts issuing transfers, so classic tebench installs the
    // transport explicitly instead of relying on TransferEngineImpl auto-setup.
    auto* sunrise_transport =
        engine_->installTransport("sunrise_link", nullptr);
    LOG_ASSERT(sunrise_transport != nullptr)
        << "Failed to install SunriseLink transport";
#endif
    if (XferBenchConfig::xport_type == "flagcx") {
        auto* xp = engine_->installTransport("flagcx", nullptr);
        LOG_ASSERT(xp) << "installTransport(flagcx) failed";
        LOG(INFO) << "tebench: FlagCX transport installed";
    }
    if (XferBenchConfig::xport_type == "shm") {
        if (!engine_->getTransport("shm")) {
            auto* shm = engine_->installTransport("shm", nullptr);
            LOG_ASSERT(shm) << "installTransport(shm) failed";
        }
        LOG(INFO) << "tebench: SHM transport installed";
    }
    if (XferBenchConfig::xport_type == "efa") {
        // Discover manually: auto-discovery would install RDMA instead.
        engine_->getLocalTopology()->discover({});
        auto* xp = engine_->installTransport("efa", nullptr);
        LOG_ASSERT(xp) << "installTransport(efa) failed";
        LOG(INFO) << "tebench: EFA transport installed";
    }
    init_ok_ = (allocateBuffers() == 0);
    if (!init_ok_) {
        LOG(ERROR) << "TEBenchRunner initialization failed";
        return;
    }
}

TEBenchRunner::~TEBenchRunner() { freeBuffers(); }

int TEBenchRunner::runTarget() {
    if (!init_ok_) {
        LOG(ERROR) << "Target cannot start: initialization failed";
        return -1;
    }
    while (g_te_running) sleep(1);
    return 0;
}

size_t TEBenchRunner::targetIndex(int thread_id) const {
    LOG_ASSERT(!target_handles_.empty());
    return static_cast<size_t>(thread_id) % target_handles_.size();
}

int TEBenchRunner::localTargetThreadId(int thread_id) const {
    LOG_ASSERT(!target_handles_.empty());
    return thread_id / static_cast<int>(target_handles_.size());
}

uint64_t TEBenchRunner::getTargetSegmentId(int thread_id) const {
    return target_handles_[targetIndex(thread_id)];
}

size_t TEBenchRunner::getTargetCount() const {
    return std::max<size_t>(target_handles_.size(), 1);
}

int TEBenchRunner::startInitiator(int num_threads) {
    if (!init_ok_) {
        LOG(ERROR) << "Initiator cannot start: initialization failed";
        return -1;
    }
    auto names = splitCommaSeparated(XferBenchConfig::target_seg_name);
    if (names.empty()) {
        LOG(ERROR) << "No target segment name specified";
        return -1;
    }
    target_handles_.clear();
    target_infos_.clear();
    for (const auto& name : names) {
        SegmentID handle = engine_->openSegment(name);
        auto info = engine_->getMetadata()->getSegmentDescByID(handle);
        std::sort(info->buffers.begin(), info->buffers.end(),
                  [](const TransferMetadata::BufferDesc& a,
                     const TransferMetadata::BufferDesc& b) {
                      return a.name < b.name;
                  });
        target_handles_.push_back(handle);
        target_infos_.push_back(std::move(info));
    }
    threads_.resize(num_threads);
    g_te_running = true;
    current_task_.resize(threads_.size());
    for (size_t i = 0; i < threads_.size(); ++i)
        threads_[i] = std::thread(&TEBenchRunner::runner, this, i);
    return 0;
}

int TEBenchRunner::stopInitiator() {
    {
        std::unique_lock<std::mutex> lk(mtx_);
        g_te_running = false;
        cv_task_.notify_all();
        cv_done_.notify_all();
    }
    for (auto& thread : threads_) {
        thread.join();
    }
    return 0;
}

void TEBenchRunner::pinThread(int thread_id) {
#ifdef USE_SUNRISE
    if (XferBenchConfig::seg_type == "VRAM" && !pinned_buffer_list_.empty()) {
        // Tang pointer queries are device-context sensitive on worker threads.
        int base_gpu = std::max(0, XferBenchConfig::local_gpu_id);
        int device_id =
            base_gpu +
            (thread_id % static_cast<int>(pinned_buffer_list_.size()));
        auto err = cudaSetDevice(device_id);
        LOG_ASSERT(err == cudaSuccess)
            << "cudaSetDevice failed before getMemoryLocation: "
            << cudaGetErrorString(err) << " device_id=" << device_id;
    }
#endif
    uint64_t addr =
        (uint64_t)pinned_buffer_list_[thread_id % pinned_buffer_list_.size()];
    auto result = getMemoryLocation((void*)addr, 1, true);
    if (result[0].location.starts_with("cpu")) {
        auto socket_id = parseIndex(result[0].location);
        bindToSocket(socket_id);
#ifdef USE_CUDA
    } else if (result[0].location.starts_with("cuda")) {
        auto device_id = parseIndex(result[0].location);
        auto socket_id = getCudaDeviceNumaID(device_id);
        bindToSocket(socket_id);
#elif defined(USE_SUNRISE)
    } else if (result[0].location.starts_with("cuda")) {
        auto device_id = parseIndex(result[0].location);
        auto socket_id = getSunriseDeviceNumaID(device_id);
        bindToSocket(socket_id);
#endif
    }
}

int TEBenchRunner::runner(int thread_id) {
    while (g_te_running) {
        std::function<int(int)> task;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_task_.wait(
                lk, [&] { return !g_te_running || current_task_[thread_id]; });
            if (!g_te_running) break;
            std::swap(task, current_task_[thread_id]);
        }
        if (task) task(thread_id);
        {
            std::unique_lock<std::mutex> lk(mtx_);
            if (--pending_ == 0) cv_done_.notify_all();
        }
    }
    return 0;
}

int TEBenchRunner::runInitiatorTasks(
    const std::function<int(int /* thread_id */)>& func) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (size_t id = 0; id < current_task_.size(); ++id)
        current_task_[id] = func;
    pending_ = (int)threads_.size();
    cv_task_.notify_all();
    cv_done_.wait(lk, [&] { return !g_te_running || pending_ == 0; });
    return g_te_running ? 0 : -1;
}

void TEBenchRunner::noteTransferFailed() {
    std::lock_guard<std::mutex> lk(mtx_);
    g_te_running = false;
    cv_task_.notify_all();
    cv_done_.notify_all();
}

// How long a stopped run keeps waiting for a batch already handed to the
// transports before abandoning it. A second SIGINT still exits immediately.
static constexpr uint64_t kStopDrainTimeoutUs = 5ull * 1000 * 1000;

mooncake::Status TEBenchRunner::waitForBatch(BatchID batch_id,
                                             mooncake::TransferStatus& status,
                                             uint64_t& polls) {
    bool stop_seen = false;
    XferBenchTimer since_stop;
    while (true) {
        auto polled = engine_->getBatchTransferStatus(batch_id, status);
        ++polls;
        if (!polled.ok()) {
            // The batch state is unknown, so treat it as still in flight.
            abandoned_batches_.fetch_add(1);
            return polled;
        }
        if (status.s != TransferStatusEnum::WAITING &&
            status.s != TransferStatusEnum::PENDING) {
            return mooncake::Status::OK();
        }
        if (g_te_running) continue;
        if (!stop_seen) {
            stop_seen = true;
            since_stop.reset();
        } else if (since_stop.lap_us(false) > kStopDrainTimeoutUs) {
            abandoned_batches_.fetch_add(1);
            status.s = TransferStatusEnum::PENDING;
            return mooncake::Status::OK();
        }
    }
}

double TEBenchRunner::runSingleTransfer(uint64_t local_addr, uint64_t target_id,
                                        uint64_t target_addr,
                                        uint64_t block_size,
                                        uint64_t batch_size, OpCode opcode,
                                        uint64_t deadline_ns,
                                        IntentType intent_type) {
    (void)deadline_ns;
    (void)intent_type;
    if (!g_te_running) return -1.0;
    auto batch_id = engine_->allocateBatchID(batch_size);
    std::vector<TransferRequest> requests;
    for (uint64_t i = 0; i < batch_size; ++i) {
        TransferRequest entry;
        entry.opcode =
            opcode == READ ? TransferRequest::READ : TransferRequest::WRITE;
        entry.length = block_size;
        entry.source = (void*)(local_addr + block_size * i);
        entry.target_id = target_id;
        entry.target_offset = target_addr + block_size * i;
        requests.emplace_back(entry);
    }
    // A deferred free means transport work may still own the buffers (see
    // TransferEngine::freeBatchID), e.g. a TIMEOUT task whose slices are
    // still waiting. Keep freeBuffers() from releasing them at exit.
    auto releaseUnfinishedBatch = [&] {
        if (engine_->freeBatchID(batch_id).IsBatchCleanupDeferred()) {
            abandoned_batches_.fetch_add(1);
        }
    };
    auto fail = [&](const char* what, const mooncake::Status& status) {
        LOG(ERROR) << what << ": " << status.ToString();
        noteTransferFailed();
        return -1.0;
    };

    XferBenchTimer timer;
    auto submitted = engine_->submitTransfer(batch_id, requests);
    if (!submitted.ok()) {
        releaseUnfinishedBatch();
        return fail("Failed to submit transfer", submitted);
    }
    const uint64_t submit_us = timer.lap_us(false);
    uint64_t polls = 0;
    mooncake::TransferStatus overall_status;
    auto polled = waitForBatch(batch_id, overall_status, polls);
    if (!polled.ok()) return fail("Failed to poll transfer", polled);
    if (overall_status.s == TransferStatusEnum::COMPLETED) {
        const uint64_t total_us = timer.lap_us(false);
        if (splitOutputEnabled()) {
            logSplitXfer({batch_size, submit_us, total_us - submit_us, polls});
        }
        auto released = releaseFinishedBatch(*engine_, batch_id);
        if (!released.ok()) return fail("Failed to free batch", released);
        return static_cast<double>(total_us);
    }
    if (overall_status.s == TransferStatusEnum::PENDING) {
        // Abandoned after a stop; waitForBatch() already counted it.
        (void)engine_->freeBatchID(batch_id);
        return -1.0;
    }
    releaseUnfinishedBatch();
    LOG(ERROR) << "Failed transfer detected";
    noteTransferFailed();
    return -1.0;
}

}  // namespace tent
}  // namespace mooncake
