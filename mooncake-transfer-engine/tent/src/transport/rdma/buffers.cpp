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

#include "tent/transport/rdma/buffers.h"
#include "tent/transport/rdma/context.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <future>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {
namespace tent {
namespace {
constexpr size_t kPretouchMinBytes = 4ull * 1024 * 1024 * 1024;
constexpr unsigned kPretouchMaxThreads = 8;
constexpr unsigned kPretouchMaxThreadsHighCore = 16;

unsigned pickPretouchThreads(unsigned hwc) {
    if (hwc == 0) return 0;
    if (hwc > 64) return kPretouchMaxThreadsHighCore;
    return std::min(hwc, kPretouchMaxThreads);
}

RdmaContext* pickPretouchContext(const std::vector<RdmaContext*>& contexts) {
    for (auto* context : contexts) {
        if (context) return context;
    }
    return nullptr;
}

int preTouchMemory(RdmaContext* context, void* addr, size_t length) {
    if (!context || length == 0) return 0;
    unsigned hwc = std::thread::hardware_concurrency();
    unsigned num_threads = pickPretouchThreads(hwc);
    if (num_threads == 0) return 0;
    size_t block_size = length / num_threads;
    if (block_size == 0) return 0;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    std::vector<int> thread_results(num_threads, 0);

    for (unsigned thread_i = 0; thread_i < num_threads; ++thread_i) {
        size_t offset = thread_i * block_size;
        size_t block_len =
            (thread_i == num_threads - 1) ? (length - offset) : block_size;
        void* block_addr = static_cast<char*>(addr) + offset;
        threads.emplace_back(
            [context, thread_i, block_addr, block_len, &thread_results]() {
                thread_results[thread_i] =
                    context->preTouchMemory(block_addr, block_len);
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (auto rc : thread_results) {
        if (rc != 0) return rc;
    }
    return 0;
}
}  // namespace

LocalBufferManager::LocalBufferManager() {}

LocalBufferManager::~LocalBufferManager() { clear(); }

static inline int getAccessFlags(Permission perm) {
    int access = IBV_ACCESS_LOCAL_WRITE;
    if (perm == kGlobalReadWrite) {
        access |= IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    } else if (perm == kGlobalReadOnly) {
        access |= IBV_ACCESS_REMOTE_READ;
    }
    return access;
}

Status LocalBufferManager::addBuffer(BufferDesc& desc,
                                     const MemoryOptions& options) {
    return addBufferInternal(desc, options, false);
}

Status LocalBufferManager::addBufferInternal(BufferDesc& desc,
                                             const MemoryOptions& options,
                                             bool force_sequential) {
    AddressRange range((void*)desc.addr, desc.length);
    BufferEntryForRdma staging;
    auto access = getAccessFlags(options.perm);
    assert(desc.rkey.empty());
    const bool trace_reg = std::getenv("MC_TENT_REGMR_TRACE") != nullptr;
    size_t context_count = 0;
    for (auto* context : context_list_) {
        if (context) ++context_count;
    }

    unsigned hwc = std::thread::hardware_concurrency();
    bool do_pre_touch =
        context_count > 0 && hwc >= 4 && desc.length >= kPretouchMinBytes;
    const bool trace_timing = std::getenv("MC_TENT_REGMR_TIMING") != nullptr;
    const char* pretouch_env = std::getenv("MC_TENT_PRETOUCH_DISABLE");
    if (pretouch_env && *pretouch_env != '\0' && pretouch_env[0] == '1') {
        do_pre_touch = false;
    }
    auto total_start = std::chrono::steady_clock::now();
    long pretouch_ms = 0;
    if (do_pre_touch) {
        auto* pretouch_context = pickPretouchContext(context_list_);
        auto pretouch_start = std::chrono::steady_clock::now();
        int ret =
            preTouchMemory(pretouch_context, (void*)desc.addr, desc.length);
        auto pretouch_end = std::chrono::steady_clock::now();
        pretouch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          pretouch_end - pretouch_start)
                          .count();
        if (ret != 0) {
            return Status::RdmaError(
                "Unable to pre-touch buffer of local memory segment" LOC_MARK);
        }
    }

    std::vector<RdmaContext::MemReg> mem_reg_list(context_list_.size(),
                                                  nullptr);
    bool use_parallel_reg = !force_sequential && context_count > 1;
    const char* parallel_env = std::getenv("MC_TENT_REGMR_PARALLEL");
    if (parallel_env && *parallel_env != '\0') {
        if (parallel_env[0] == '0') {
            use_parallel_reg = false;
        } else {
            use_parallel_reg = context_count > 1 && !force_sequential;
        }
    }
    auto reg_start = std::chrono::steady_clock::now();
    if (use_parallel_reg) {
        std::vector<std::pair<size_t, std::future<void>>> tasks;
        tasks.reserve(context_count);
        void* addr = (void*)desc.addr;
        size_t length = desc.length;
        for (size_t id = 0; id < context_list_.size(); ++id) {
            auto* context = context_list_[id];
            if (!context) continue;
            tasks.emplace_back(
                id, std::async(std::launch::async, [context, &mem_reg_list, id,
                                                    addr, length, access]() {
                    mem_reg_list[id] =
                        context->registerMemReg(addr, length, access);
                }));
        }
        for (auto& task : tasks) task.second.get();
    } else {
        for (size_t id = 0; id < context_list_.size(); ++id) {
            auto* context = context_list_[id];
            if (!context) continue;
            mem_reg_list[id] =
                context->registerMemReg((void*)desc.addr, desc.length, access);
        }
    }
    for (size_t id = 0; id < context_list_.size(); ++id) {
        if (!context_list_[id]) continue;
        if (!mem_reg_list[id]) {
            return Status::RdmaError(
                "Unable to register buffer of local memory segment" LOC_MARK);
        }
        staging.mem_reg_map[context_list_[id]] = mem_reg_list[id];
        auto keys = context_list_[id]->queryMemRegKey(mem_reg_list[id]);
        desc.lkey.push_back(keys.first);
        desc.rkey.push_back(keys.second);
    }
    staging.options = options;
    RWSpinlock::WriteGuard guard(lock_);
    buffer_list_[range] = staging;
    if (trace_reg || trace_timing) {
        auto reg_end = std::chrono::steady_clock::now();
        auto reg_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          reg_end - reg_start)
                          .count();
        auto total_end = std::chrono::steady_clock::now();
        auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            total_end - total_start)
                            .count();
        if (trace_reg) {
            LOG(INFO) << "[TENT][RDMA] register_local_buffer"
                      << " addr=" << (void*)desc.addr << " len=" << desc.length
                      << " contexts=" << context_count
                      << " parallel=" << (use_parallel_reg ? "true" : "false")
                      << " duration_ms=" << reg_ms;
        }
        if (trace_timing) {
            LOG(INFO) << "[TENT][RDMA] register_timing"
                      << " addr=" << (void*)desc.addr << " len=" << desc.length
                      << " contexts=" << context_count
                      << " pretouch=" << (do_pre_touch ? "true" : "false")
                      << " pretouch_ms=" << pretouch_ms << " reg_ms=" << reg_ms
                      << " total_ms=" << total_ms
                      << " parallel=" << (use_parallel_reg ? "true" : "false");
        }
    }
    return Status::OK();
}

Status LocalBufferManager::addBuffer(std::vector<BufferDesc>& desc_list,
                                     const MemoryOptions& options) {
    if (desc_list.empty()) return Status::OK();
    if (desc_list.size() == 1) {
        return addBufferInternal(desc_list.front(), options, false);
    }

    const bool trace_timing = std::getenv("MC_TENT_REGMR_TIMING") != nullptr;
    auto batch_start = std::chrono::steady_clock::now();
    bool use_parallel_batch = true;
    const char* batch_env = std::getenv("MC_TENT_BATCH_REG_PARALLEL");
    if (batch_env && *batch_env != '\0' && batch_env[0] == '0') {
        use_parallel_batch = false;
    }

    if (!use_parallel_batch) {
        for (auto& desc : desc_list) {
            CHECK_STATUS(addBufferInternal(desc, options, false));
        }
        if (trace_timing) {
            auto batch_end = std::chrono::steady_clock::now();
            auto batch_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    batch_end - batch_start)
                    .count();
            LOG(INFO) << "[TENT][RDMA] batch_register_timing"
                      << " buffers=" << desc_list.size() << " parallel=false"
                      << " total_ms=" << batch_ms;
        }
        return Status::OK();
    }

    std::vector<std::future<Status>> tasks;
    tasks.reserve(desc_list.size());
    for (auto& desc : desc_list) {
        auto* desc_ptr = &desc;
        tasks.emplace_back(
            std::async(std::launch::async, [this, desc_ptr, options]() {
                return addBufferInternal(*desc_ptr, options, true);
            }));
    }
    for (auto& task : tasks) {
        auto status = task.get();
        if (!status.ok()) return status;
    }
    if (trace_timing) {
        auto batch_end = std::chrono::steady_clock::now();
        auto batch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            batch_end - batch_start)
                            .count();
        LOG(INFO) << "[TENT][RDMA] batch_register_timing"
                  << " buffers=" << desc_list.size() << " parallel=true"
                  << " total_ms=" << batch_ms;
    }
    return Status::OK();
}

Status LocalBufferManager::removeBuffer(BufferDesc& desc) {
    RWSpinlock::WriteGuard guard(lock_);
    AddressRange range((void*)desc.addr, desc.length);
    auto& item = buffer_list_[range];
    for (auto& elem : item.mem_reg_map) {
        elem.first->unregisterMemReg(elem.second);
    }
    desc.rkey.clear();
    buffer_list_.erase(range);
    return Status::OK();
}

Status LocalBufferManager::addDevice(RdmaContext* context) {
    RWSpinlock::WriteGuard guard(lock_);
    assert(topology_ && context);
    int index = topology_->getNicId(context->name());
    if (index < 0) {
        LOG(ERROR) << "Device " << context->name()
                   << " not found in the local segment";
        return Status::DeviceNotFound(
            "Device not found in the local segment" LOC_MARK);
    }

    if (context_list_[index]) {
        LOG(WARNING) << "Device " << context->name()
                     << " already exists in the local segment";
    }
    context_list_[index] = context;
    for (auto& buffer : buffer_list_) {
        auto range = buffer.first;
        auto& options = buffer.second.options;
        auto access = getAccessFlags(options.perm);
        if (buffer.second.mem_reg_map.count(context)) continue;
        auto mem_reg =
            context->registerMemReg(range.addr, range.length, access);
        if (!mem_reg)
            return Status::RdmaError(
                "Device cannot register memory buffer" LOC_MARK);
        buffer.second.mem_reg_map[context] = mem_reg;
    }
    return Status::OK();
}

Status LocalBufferManager::removeDevice(RdmaContext* context, bool do_unreg) {
    RWSpinlock::WriteGuard guard(lock_);
    assert(topology_ && context);
    auto iter = std::find(context_list_.begin(), context_list_.end(), context);
    if (iter == context_list_.end()) return Status::OK();
    for (auto& buffer : buffer_list_) {
        if (!buffer.second.mem_reg_map.count(context)) continue;
        if (do_unreg)
            context->unregisterMemReg(buffer.second.mem_reg_map[context]);
        buffer.second.mem_reg_map.erase(context);
    }
    *iter = nullptr;
    return Status::OK();
}

Status LocalBufferManager::clear() {
    RWSpinlock::WriteGuard guard(lock_);
    for (auto& buffer : buffer_list_) {
        for (auto& elem : buffer.second.mem_reg_map)
            elem.first->unregisterMemReg(elem.second);
    }
    buffer_list_.clear();
    context_list_.clear();
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
