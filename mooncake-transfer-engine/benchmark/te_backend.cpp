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

#include <fstream>

#include "te_backend.h"
#include "utils.h"
#include "common.h"
#include "char_util.h"

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
    return numa_alloc_onnode(size, buffer_id);
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
    if (XferBenchConfig::seg_type == "DRAM") {
        int num_buffers = numa_num_configured_nodes();
        pinned_buffer_list_.resize(num_buffers, nullptr);
        for (int i = 0; i < num_buffers; ++i) {
            auto location = "cpu:" + std::to_string(i);
            pinned_buffer_list_[i] =
                allocateMemoryPool(total_buffer_size, i, false);
            if (!pinned_buffer_list_[i]) return -1;
            engine_->registerLocalMemory(pinned_buffer_list_[i],
                                         total_buffer_size, location);
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
    auto total_buffer_size = XferBenchConfig::total_buffer_size;
    for (size_t i = 0; i < pinned_buffer_list_.size(); ++i) {
        if (!pinned_buffer_list_[i]) continue;
        engine_->unregisterLocalMemory(pinned_buffer_list_[i]);
        freeMemoryPool(pinned_buffer_list_[i], total_buffer_size);
    }
    pinned_buffer_list_.clear();
    return 0;
}

TEBenchRunner::TEBenchRunner() {
    signal(SIGINT, signalHandlerV0);
    signal(SIGTERM, signalHandlerV0);
    engine_ = std::make_unique<mooncake::TransferEngine>(true);
    auto conn_str = XferBenchConfig::metadata_type == "p2p"
                        ? "P2PHANDSHAKE"
                        : XferBenchConfig::metadata_url_list;
    auto seg_name = XferBenchConfig::metadata_type == "p2p"
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

int TEBenchRunner::startInitiator(int num_threads) {
    if (!init_ok_) {
        LOG(ERROR) << "Initiator cannot start: initialization failed";
        return -1;
    }
    handle_ = engine_->openSegment(XferBenchConfig::target_seg_name);
    info_ = engine_->getMetadata()->getSegmentDescByID(handle_);
    std::sort(
        info_->buffers.begin(), info_->buffers.end(),
        [](const TransferMetadata::BufferDesc& a,
           const TransferMetadata::BufferDesc& b) { return a.name < b.name; });
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

static int parseIndex(const std::string& loc) {
    auto pos = loc.find(':');
    if (pos == std::string::npos || pos + 1 >= loc.size()) {
        throw std::invalid_argument("Invalid loc format: " + loc);
    }
    return std::stoi(loc.substr(pos + 1));
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

double TEBenchRunner::runSingleTransfer(uint64_t local_addr,
                                        uint64_t target_addr,
                                        uint64_t block_size,
                                        uint64_t batch_size, OpCode opcode) {
    auto batch_id = engine_->allocateBatchID(batch_size);
    std::vector<TransferRequest> requests;
    for (uint64_t i = 0; i < batch_size; ++i) {
        TransferRequest entry;
        entry.opcode =
            opcode == READ ? TransferRequest::READ : TransferRequest::WRITE;
        entry.length = block_size;
        entry.source = (void*)(local_addr + block_size * i);
        entry.target_id = handle_;
        entry.target_offset = target_addr + block_size * i;
        requests.emplace_back(entry);
    }
    XferBenchTimer timer;
    CHECK_FAIL(engine_->submitTransfer(batch_id, requests));
    while (true) {
        uint64_t success_count = 0;
        for (uint64_t i = 0; i < batch_size; ++i) {
            mooncake::TransferStatus overall_status;
            CHECK_FAIL(engine_->getTransferStatus(batch_id, i, overall_status));
            if (overall_status.s == TransferStatusEnum::COMPLETED) {
                success_count++;
            } else if (overall_status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "Failed transfer detected";
                exit(EXIT_FAILURE);
            }
        }
        if (success_count == batch_size) break;
    }
    auto duration = timer.lap_us();
    CHECK_FAIL(engine_->freeBatchID(batch_id));
    return duration;
}

}  // namespace tent
}  // namespace mooncake
