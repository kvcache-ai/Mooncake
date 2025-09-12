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

#include "tev0_backend.h"
#include "utils.h"
#include "common.h"

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

namespace mooncake {
namespace v1 {

int getCudaDeviceNumaID(int cuda_id);

volatile bool g_tev0_running = true;
volatile bool g_tev0_triggered_sig = false;

void signalHandlerV0(int signum) {
    if (g_tev0_triggered_sig) {
        LOG(ERROR) << "Received signal " << signum
                   << " again, forcefully terminating...";
        std::exit(EXIT_FAILURE);
    }
    LOG(INFO) << "Received signal " << signum << ", stopping target server...";
    g_tev0_running = false;
    g_tev0_triggered_sig = true;
}

static void *allocateMemoryPool(size_t size, int buffer_id,
                                bool from_vram = false) {
#ifdef USE_CUDA
    if (from_vram) {
        int gpu_id = buffer_id;
        void *d_buf;
        LOG(INFO) << "Allocating memory on GPU " << gpu_id;
        cudaSetDevice(gpu_id);
#ifdef USE_MNNVL
        d_buf = mooncake::NvlinkTransport::allocatePinnedLocalMemory(size);
#else
        cudaMalloc(&d_buf, size);
#endif
        return d_buf;
    }
#endif
    return numa_alloc_onnode(size, buffer_id);
}

static void freeMemoryPool(void *addr, size_t size) {
#ifdef USE_CUDA
#ifdef USE_MNNVL
    CUmemGenericAllocationHandle handle;
    auto result = cuMemRetainAllocationHandle(&handle, addr);
    if (result == CUDA_SUCCESS) {
        mooncake::NvlinkTransport::freePinnedLocalMemory(addr);
        return;
    }
#endif
    // check pointer on GPU
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

int TEv0BenchRunner::allocateBuffers() {
    auto total_buffer_size = XferBenchConfig::total_buffer_size;
    if (XferBenchConfig::seg_type == "DRAM") {
        int num_buffers = numa_num_configured_nodes();
        pinned_buffer_list_.resize(num_buffers, nullptr);
        for (int i = 0; i < num_buffers; ++i) {
            auto location = "cpu:" + std::to_string(i);
            pinned_buffer_list_[i] =
                allocateMemoryPool(total_buffer_size, i, false);
            engine_->registerLocalMemory(pinned_buffer_list_[i],
                                         total_buffer_size, location);
        }
#ifdef USE_CUDA
    } else if (XferBenchConfig::seg_type == "VRAM") {
        int num_buffers = 0;
        cudaGetDeviceCount(&num_buffers);
        pinned_buffer_list_.resize(num_buffers, nullptr);
        for (int i = 0; i < num_buffers; ++i) {
            auto location = "cuda:" + std::to_string(i);
            pinned_buffer_list_[i] =
                allocateMemoryPool(total_buffer_size, i, true);
            engine_->registerLocalMemory(pinned_buffer_list_[i],
                                         total_buffer_size, location);
        }
#endif
    } else {
        LOG(ERROR) << "Unknown seg_type: " << XferBenchConfig::seg_type;
    }
    return 0;
}

int TEv0BenchRunner::freeBuffers() {
    auto total_buffer_size = XferBenchConfig::total_buffer_size;
    for (size_t i = 0; i < pinned_buffer_list_.size(); ++i) {
        engine_->unregisterLocalMemory(pinned_buffer_list_[i]);
        freeMemoryPool(pinned_buffer_list_[i], total_buffer_size);
    }
    pinned_buffer_list_.clear();
    return 0;
}

TEv0BenchRunner::TEv0BenchRunner() {
    signal(SIGINT, signalHandlerV0);
    signal(SIGTERM, signalHandlerV0);
    engine_ = std::make_unique<TransferEngine>(true);
    auto conn_str = XferBenchConfig::metadata_type == "p2p"
                        ? "P2PHANDSHAKE"
                        : XferBenchConfig::metadata_url_list;
    auto seg_name = XferBenchConfig::metadata_type == "p2p"
                        ? mooncake::getHostname()
                        : XferBenchConfig::seg_name;
    engine_->init(conn_str, seg_name);
    allocateBuffers();
}

TEv0BenchRunner::~TEv0BenchRunner() { freeBuffers(); }

int TEv0BenchRunner::runTarget() {
    while (g_tev0_running) sleep(1);
    return 0;
}

int TEv0BenchRunner::startInitiator() {
    handle_ = engine_->openSegment(XferBenchConfig::target_seg_name);
    info_ = engine_->getMetadata()->getSegmentDescByID(handle_);
    // std::sort(info_.buffers.begin(), info_.buffers.end());
    threads_.resize(XferBenchConfig::num_threads);
    current_task_.resize(threads_.size());
    for (size_t i = 0; i < threads_.size(); ++i)
        threads_[i] = std::thread(&TEv0BenchRunner::runner, this, i);
    return 0;
}

int TEv0BenchRunner::stopInitiator() {
    {
        std::unique_lock<std::mutex> lk(mtx_);
        g_tev0_running = false;
        cv_task_.notify_all();
        cv_done_.notify_all();
    }
    for (auto &thread : threads_) {
        thread.join();
    }
    return 0;
}

static int parseIndex(const std::string &loc) {
    auto pos = loc.find(':');
    if (pos == std::string::npos || pos + 1 >= loc.size()) {
        throw std::invalid_argument("Invalid loc format: " + loc);
    }
    return std::stoi(loc.substr(pos + 1));
}

void TEv0BenchRunner::pinThread(int thread_id) {
    uint64_t addr =
        (uint64_t)pinned_buffer_list_[thread_id % pinned_buffer_list_.size()];
    auto result = getMemoryLocation((void *)addr, 1);
    if (result[0].location.starts_with("cpu")) {
        auto socket_id = parseIndex(result[0].location);
        bindToSocket(socket_id);
    } else if (result[0].location.starts_with("cuda")) {
        auto device_id = parseIndex(result[0].location);
        auto socket_id = getCudaDeviceNumaID(device_id);
        bindToSocket(socket_id);
    }
}

int TEv0BenchRunner::runner(int thread_id) {
    while (g_tev0_running) {
        std::function<int(int)> task;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_task_.wait(lk, [&] {
                return !g_tev0_running || current_task_[thread_id];
            });
            if (!g_tev0_running) break;
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

int TEv0BenchRunner::runInitiatorTasks(
    const std::function<int(int /* thread_id */)> &func) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (size_t id = 0; id < current_task_.size(); ++id)
        current_task_[id] = func;
    pending_ = (int)threads_.size();
    cv_task_.notify_all();
    cv_done_.wait(lk, [&] { return g_tev0_running && pending_ == 0; });
    return 0;
}

double TEv0BenchRunner::runSingleTransfer(uint64_t local_addr,
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
        entry.source = (void *)(local_addr + block_size * i);
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

}  // namespace v1
}  // namespace mooncake