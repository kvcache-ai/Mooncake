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

#include "tev1_backend.h"
#include "utils.h"
#include "v1/memory/location.h"
#include "v1/utility/topology.h"

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

namespace mooncake {
namespace v1 {

volatile bool g_tev1_running = true;
volatile bool g_tev1_triggered_sig = false;

void signalHandlerV1(int signum) {
    if (g_tev1_triggered_sig) {
        LOG(ERROR) << "Received signal " << signum
                   << " again, forcefully terminating...";
        std::exit(EXIT_FAILURE);
    }
    LOG(INFO) << "Received signal " << signum << ", stopping target server...";
    g_tev1_running = false;
    g_tev1_triggered_sig = true;
}

std::shared_ptr<ConfigManager> loadConfig() {
    auto config = std::make_shared<ConfigManager>();
    config->set("local_segment_name", XferBenchConfig::seg_name);
    config->set("metadata_type", XferBenchConfig::metadata_type);
    config->set("metadata_servers", XferBenchConfig::metadata_url_list);
    return config;
}

static TransportType getTransportType(const std::string &xport_type) {
    if (xport_type == "rdma") return RDMA;
    if (xport_type == "shm") return SHM;
    if (xport_type == "gds") return GDS;
    if (xport_type == "mnnvl") return MNNVL;
    if (xport_type == "tcp") return TCP;
    if (xport_type == "iouring") return IOURING;
    return UNSPEC;
}

int TEv1BenchRunner::allocateBuffers() {
    auto total_buffer_size = XferBenchConfig::total_buffer_size;
    if (XferBenchConfig::seg_type == "DRAM") {
        int num_buffers = numa_num_configured_nodes();
        pinned_buffer_list_.resize(num_buffers, nullptr);
        for (int i = 0; i < num_buffers; ++i) {
            MemoryOptions options;
            if (!XferBenchConfig::xport_type.empty())
                options.type = getTransportType(XferBenchConfig::xport_type);
            options.location = "cpu:" + std::to_string(i);
            CHECK_FAIL(engine_->allocateLocalMemory(
                &pinned_buffer_list_[i], total_buffer_size, options));
            CHECK_FAIL(engine_->registerLocalMemory(pinned_buffer_list_[i],
                                                    total_buffer_size));
        }
#ifdef USE_CUDA
    } else if (XferBenchConfig::seg_type == "VRAM") {
        int num_buffers = 0;
        cudaGetDeviceCount(&num_buffers);
        pinned_buffer_list_.resize(num_buffers, nullptr);
        for (int i = 0; i < num_buffers; ++i) {
            MemoryOptions options;
            if (!XferBenchConfig::xport_type.empty())
                options.type = getTransportType(XferBenchConfig::xport_type);
            options.location = "cuda:" + std::to_string(i);
            CHECK_FAIL(engine_->allocateLocalMemory(
                &pinned_buffer_list_[i], total_buffer_size, options));
            CHECK_FAIL(engine_->registerLocalMemory(pinned_buffer_list_[i],
                                                    total_buffer_size));
        }
#endif
    } else {
        LOG(ERROR) << "Unknown seg_type: " << XferBenchConfig::seg_type;
    }
    return 0;
}

int TEv1BenchRunner::freeBuffers() {
    auto total_buffer_size = XferBenchConfig::total_buffer_size;
    for (size_t i = 0; i < pinned_buffer_list_.size(); ++i) {
        CHECK_FAIL(engine_->unregisterLocalMemory(pinned_buffer_list_[i],
                                                  total_buffer_size));
        CHECK_FAIL(engine_->freeLocalMemory(pinned_buffer_list_[i]));
    }
    pinned_buffer_list_.clear();
    return 0;
}

TEv1BenchRunner::TEv1BenchRunner() {
    signal(SIGINT, signalHandlerV1);
    signal(SIGTERM, signalHandlerV1);
    engine_ = std::make_unique<TransferEngine>(loadConfig());
    allocateBuffers();
}

TEv1BenchRunner::~TEv1BenchRunner() { freeBuffers(); }

int TEv1BenchRunner::runTarget() {
    while (g_tev1_running) sleep(1);
    return 0;
}

int TEv1BenchRunner::startInitiator() {
    CHECK_FAIL(engine_->openSegment(handle_, XferBenchConfig::target_seg_name));
    CHECK_FAIL(engine_->getSegmentInfo(handle_, info_));
    // std::sort(info_.buffers.begin(), info_.buffers.end());
    threads_.resize(XferBenchConfig::num_threads);
    current_task_.resize(threads_.size());
    for (size_t i = 0; i < threads_.size(); ++i)
        threads_[i] = std::thread(&TEv1BenchRunner::runner, this, i);
    return 0;
}

int TEv1BenchRunner::stopInitiator() {
    {
        std::unique_lock<std::mutex> lk(mtx_);
        g_tev1_running = false;
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

void TEv1BenchRunner::pinThread(int thread_id) {
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

int TEv1BenchRunner::runner(int thread_id) {
    while (g_tev1_running) {
        std::function<int(int)> task;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_task_.wait(lk, [&] {
                return !g_tev1_running || current_task_[thread_id];
            });
            if (!g_tev1_running) break;
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

int TEv1BenchRunner::runInitiatorTasks(
    const std::function<int(int /* thread_id */)> &func) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (size_t id = 0; id < current_task_.size(); ++id)
        current_task_[id] = func;
    pending_ = (int)threads_.size();
    cv_task_.notify_all();
    cv_done_.wait(lk, [&] { return g_tev1_running && pending_ == 0; });
    return 0;
}

double TEv1BenchRunner::runSingleTransfer(uint64_t local_addr,
                                          uint64_t target_addr,
                                          uint64_t block_size,
                                          uint64_t batch_size, OpCode opcode) {
    auto batch_id = engine_->allocateBatch(batch_size);
    std::vector<Request> requests;
    for (uint64_t i = 0; i < batch_size; ++i) {
        Request entry;
        entry.opcode = opcode == READ ? Request::READ : Request::WRITE;
        entry.length = block_size;
        entry.source = (void *)(local_addr + block_size * i);
        entry.target_id = handle_;
        entry.target_offset = target_addr + block_size * i;
        requests.emplace_back(entry);
    }
    XferBenchTimer timer;
    CHECK_FAIL(engine_->submitTransfer(batch_id, requests));
    while (true) {
        TransferStatus overall_status;
        CHECK_FAIL(engine_->getTransferStatus(batch_id, overall_status));
        if (overall_status.s == TransferStatusEnum::COMPLETED) {
            break;
        } else if (overall_status.s == TransferStatusEnum::FAILED) {
            LOG(ERROR) << "Failed transfer detected";
            exit(EXIT_FAILURE);
        }
    }
    auto duration = timer.lap_us();
    CHECK_FAIL(engine_->freeBatch(batch_id));
    return duration;
}

double TEv1BenchRunner::runKVCacheTransfer(uint64_t local_addr,
                                           uint64_t target_addr,
                                           uint64_t nope_block_size,
                                           uint64_t rope_block_size,
                                           uint64_t num_blocks) {
    auto batch_id = engine_->allocateBatch(num_blocks * 2);
    std::vector<Request> requests;
    for (uint64_t i = 0; i < num_blocks; ++i) {
        Request entry;
        entry.opcode = Request::WRITE;
        entry.length = nope_block_size;
        entry.source = (void *)local_addr;
        entry.target_id = handle_;
        entry.target_offset = target_addr;
        local_addr += nope_block_size;
        target_addr += nope_block_size;
        requests.emplace_back(entry);
    }
    for (uint64_t i = 0; i < num_blocks; ++i) {
        Request entry;
        entry.opcode = Request::WRITE;
        entry.length = rope_block_size;
        entry.source = (void *)local_addr;
        entry.target_id = handle_;
        entry.target_offset = target_addr;
        local_addr += rope_block_size;
        target_addr += rope_block_size;
        requests.emplace_back(entry);
    }
    XferBenchTimer timer;
    CHECK_FAIL(engine_->submitTransfer(batch_id, requests));
    while (true) {
        TransferStatus overall_status;
        CHECK_FAIL(engine_->getTransferStatus(batch_id, overall_status));
        if (overall_status.s == TransferStatusEnum::COMPLETED) {
            break;
        } else if (overall_status.s == TransferStatusEnum::FAILED) {
            LOG(ERROR) << "Failed transfer detected";
            exit(EXIT_FAILURE);
        }
    }
    auto duration = timer.lap_us();
    CHECK_FAIL(engine_->freeBatch(batch_id));
    return duration;
}

}  // namespace v1
}  // namespace mooncake