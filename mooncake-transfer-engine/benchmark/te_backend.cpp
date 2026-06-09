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

#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda.h>
#include <cuda_runtime.h>
#ifdef USE_MNNVL
#include "transport/nvlink_transport/nvlink_transport.h"
#endif
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
    for (char* ch = pci_bus_id; (*ch = tolower(*ch)); ch++);
    return getNumaNodeFromPciDevice(pci_bus_id);
}
#else
static inline int getCudaDeviceNumaID(int cuda_id) { return 0; }
#endif

// Use atomic<bool> for thread-safe signal handling
std::atomic<bool> g_te_running{true};
std::atomic<bool> g_te_triggered_sig{false};

// Signal handler that only performs async-signal-safe operations
void signalHandlerV0(int signum) {
    // Only set atomic flags - this is async-signal-safe
    bool was_already_triggered = g_te_triggered_sig.exchange(true);
    if (was_already_triggered) {
        // Second signal - terminate immediately
        _exit(EXIT_FAILURE);
    }
    g_te_running.store(false);
}

void* TEBenchRunner::allocateMemoryPool(size_t size, int buffer_id,
                                        bool from_vram) {
#ifdef USE_CUDA
    if (from_vram) {
        int gpu_id = buffer_id;
        void* d_buf = nullptr;
        LOG(INFO) << "Allocating memory on GPU " << gpu_id;

        auto err = cudaSetDevice(gpu_id);
        if (err != cudaSuccess) {
            LOG(ERROR) << "cudaSetDevice failed for GPU " << gpu_id << ": "
                       << cudaGetErrorString(err);
            return nullptr;
        }

#ifdef USE_MNNVL
        d_buf = mooncake::NvlinkTransport::allocatePinnedLocalMemory(size);
#else
        err = cudaMalloc(&d_buf, size);
        if (err != cudaSuccess) {
            LOG(ERROR) << "cudaMalloc failed for GPU " << gpu_id << ": "
                       << cudaGetErrorString(err);
            return nullptr;
        }
#endif

        if (d_buf == nullptr) {
            LOG(ERROR) << "Memory allocation returned nullptr for GPU "
                       << gpu_id;
        }
        return d_buf;
    }
#endif

    void* buf = numa_alloc_onnode(size, buffer_id);
    if (buf == nullptr) {
        LOG(ERROR) << "numa_alloc_onnode failed for node " << buffer_id;
    }
    return buf;
}

void TEBenchRunner::freeMemoryPool(void* addr, size_t size) {
    if (addr == nullptr) {
        LOG(WARNING) << "Attempt to free nullptr";
        return;
    }

#ifdef USE_CUDA
#ifdef USE_MNNVL
    CUmemGenericAllocationHandle handle;
    auto result = cuMemRetainAllocationHandle(&handle, addr);
    if (result == CUDA_SUCCESS) {
        mooncake::NvlinkTransport::freePinnedLocalMemory(addr);
        return;
    }
#endif

    // Check pointer type
    cudaPointerAttributes attributes;
    auto err = cudaPointerGetAttributes(&attributes, addr);

    if (err == cudaSuccess) {
        if (attributes.type == cudaMemoryTypeDevice) {
            auto free_err = cudaFree(addr);
            if (free_err != cudaSuccess) {
                LOG(WARNING)
                    << "cudaFree failed: " << cudaGetErrorString(free_err);
            }
            return;
        } else if (attributes.type == cudaMemoryTypeHost ||
                   attributes.type == cudaMemoryTypeUnregistered) {
            numa_free(addr, size);
            return;
        } else {
            LOG(ERROR) << "Unknown memory type for ptr " << addr
                       << ", type: " << attributes.type;
        }
    } else {
        // cudaPointerGetAttributes failed - assume it's host memory
        LOG(WARNING) << "cudaPointerGetAttributes failed for ptr " << addr
                     << ": " << cudaGetErrorString(err)
                     << ", assuming host memory";
    }
#endif

    numa_free(addr, size);
}

int TEBenchRunner::allocateBuffers() {
    auto total_buffer_size = XferBenchConfig::total_buffer_size;

    if (XferBenchConfig::seg_type == "DRAM") {
        int num_buffers = numa_num_configured_nodes();
        if (num_buffers <= 0) {
            LOG(ERROR) << "Invalid NUMA node count: " << num_buffers;
            return -1;
        }

        pinned_buffer_list_.resize(num_buffers, nullptr);
        for (int i = 0; i < num_buffers; ++i) {
            auto location = "cpu:" + std::to_string(i);
            pinned_buffer_list_[i] =
                allocateMemoryPool(total_buffer_size, i, false);
            if (pinned_buffer_list_[i] == nullptr) {
                LOG(ERROR) << "Failed to allocate buffer for node " << i;
                // Clean up already allocated buffers
                for (int j = 0; j < i; ++j) {
                    engine_->unregisterLocalMemory(pinned_buffer_list_[j]);
                    freeMemoryPool(pinned_buffer_list_[j], total_buffer_size);
                }
                pinned_buffer_list_.clear();
                return -1;
            }

            auto ret = engine_->registerLocalMemory(
                pinned_buffer_list_[i], total_buffer_size, location);
            if (ret != 0) {
                LOG(ERROR) << "Failed to register memory for " << location
                           << ", error code: " << ret;
                freeMemoryPool(pinned_buffer_list_[i], total_buffer_size);
                // Clean up already allocated buffers
                for (int j = 0; j < i; ++j) {
                    engine_->unregisterLocalMemory(pinned_buffer_list_[j]);
                    freeMemoryPool(pinned_buffer_list_[j], total_buffer_size);
                }
                pinned_buffer_list_.clear();
                return -1;
            }
        }

#ifdef USE_CUDA
    } else if (XferBenchConfig::seg_type == "VRAM") {
        int gpu_count = 0;
        auto err = cudaGetDeviceCount(&gpu_count);
        if (err != cudaSuccess || gpu_count <= 0) {
            LOG(ERROR)
                << "Failed to get CUDA device count or no devices available: "
                << (err != cudaSuccess ? cudaGetErrorString(err)
                                       : "no devices");
            return -1;
        }

        int start_gpu = 0, num_buffers = gpu_count;
        if (XferBenchConfig::local_gpu_id != -1) {
            start_gpu = XferBenchConfig::local_gpu_id;
            num_buffers = 1;
            if (start_gpu < 0 || start_gpu >= gpu_count) {
                LOG(ERROR) << "local_gpu_id " << start_gpu
                           << " out of range [0, " << gpu_count << ")";
                return -1;
            }
        }

        pinned_buffer_list_.resize(num_buffers, nullptr);
        for (int i = 0; i < num_buffers; ++i) {
            int gpu_id = start_gpu + i;
            auto location = "cuda:" + std::to_string(gpu_id);
            pinned_buffer_list_[i] =
                allocateMemoryPool(total_buffer_size, gpu_id, true);
            if (pinned_buffer_list_[i] == nullptr) {
                LOG(ERROR) << "Failed to allocate buffer for GPU " << gpu_id;
                // Clean up already allocated buffers
                for (int j = 0; j < i; ++j) {
                    engine_->unregisterLocalMemory(pinned_buffer_list_[j]);
                    freeMemoryPool(pinned_buffer_list_[j], total_buffer_size);
                }
                pinned_buffer_list_.clear();
                return -1;
            }

            auto ret = engine_->registerLocalMemory(
                pinned_buffer_list_[i], total_buffer_size, location);
            if (ret != 0) {
                LOG(ERROR) << "Failed to register memory for " << location
                           << ", error code: " << ret;
                freeMemoryPool(pinned_buffer_list_[i], total_buffer_size);
                // Clean up already allocated buffers
                for (int j = 0; j < i; ++j) {
                    engine_->unregisterLocalMemory(pinned_buffer_list_[j]);
                    freeMemoryPool(pinned_buffer_list_[j], total_buffer_size);
                }
                pinned_buffer_list_.clear();
                return -1;
            }
        }
#endif
    } else {
        LOG(ERROR) << "Unknown seg_type: " << XferBenchConfig::seg_type;
        return -1;
    }

    return 0;
}

int TEBenchRunner::freeBuffers() {
    auto total_buffer_size = XferBenchConfig::total_buffer_size;

    for (size_t i = 0; i < pinned_buffer_list_.size(); ++i) {
        if (pinned_buffer_list_[i] != nullptr && engine_) {
            engine_->unregisterLocalMemory(pinned_buffer_list_[i]);
            freeMemoryPool(pinned_buffer_list_[i], total_buffer_size);
        }
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
    allocateBuffers();
}

TEBenchRunner::~TEBenchRunner() { freeBuffers(); }

int TEBenchRunner::runTarget() {
    while (g_te_running) sleep(1);
    return 0;
}

int TEBenchRunner::startInitiator(int num_threads) {
    handle_ = engine_->openSegment(XferBenchConfig::target_seg_name);
    info_ = engine_->getMetadata()->getSegmentDescByID(handle_);
    std::sort(
        info_->buffers.begin(), info_->buffers.end(),
        [](const TransferMetadata::BufferDesc& a,
           const TransferMetadata::BufferDesc& b) { return a.name < b.name; });
    threads_.resize(num_threads);
    g_te_running.store(true);
    current_task_.resize(threads_.size());
    for (size_t i = 0; i < threads_.size(); ++i)
        threads_[i] = std::thread(&TEBenchRunner::runner, this, i);
    return 0;
}

int TEBenchRunner::stopInitiator() {
    {
        std::unique_lock<std::mutex> lk(mtx_);
        g_te_running.store(false);
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
    uint64_t addr =
        (uint64_t)pinned_buffer_list_[thread_id % pinned_buffer_list_.size()];
    auto result = getMemoryLocation((void*)addr, 1, true);
    if (result[0].location.starts_with("cpu")) {
        auto socket_id = parseIndex(result[0].location);
        bindToSocket(socket_id);
    } else if (result[0].location.starts_with("cuda")) {
        auto device_id = parseIndex(result[0].location);
        auto socket_id = getCudaDeviceNumaID(device_id);
        bindToSocket(socket_id);
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

// Multi-target methods (not supported for classic TE backend)
int TEBenchRunner::publishSegment(const std::string& segment_name) {
    LOG(ERROR) << "Multi-target all-to-all mode is not supported for classic "
                  "TE backend";
    LOG(ERROR)
        << "Please use TENT backend (--backend=tent) for all-to-all testing";
    return -1;
}

int TEBenchRunner::connectToAllTargets(
    const std::vector<std::string>& target_segments, int sync_timeout_sec) {
    LOG(ERROR) << "Multi-target all-to-all mode is not supported for classic "
                  "TE backend";
    LOG(ERROR)
        << "Please use TENT backend (--backend=tent) for all-to-all testing";
    return -1;
}

size_t TEBenchRunner::getTargetCount() const { return 0; }

double TEBenchRunner::runTransferToTarget(uint64_t local_addr,
                                          size_t target_idx,
                                          uint64_t block_size,
                                          uint64_t batch_size, OpCode opcode) {
    LOG(ERROR) << "Multi-target all-to-all mode is not supported for classic "
                  "TE backend";
    return -1.0;
}

}  // namespace tent
}  // namespace mooncake
