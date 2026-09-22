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
#include <future>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {
namespace tent {

// MR warm-up is only beneficial for large buffers (>4GB) where the overhead
// of temp registration is amortized by faster actual registration.
const size_t kMrWarmupMinBytes = 4ull * 1024 * 1024 * 1024;

static unsigned pickMrWarmupThreads(unsigned hwc) {
    constexpr unsigned kMrWarmupMaxThreads = 8;
    constexpr unsigned kMrWarmupMaxThreadsHighCore = 16;
    constexpr unsigned kHighCoreCountThreshold = 64;
    if (hwc == 0) hwc = 1;
    if (hwc > kHighCoreCountThreshold) return kMrWarmupMaxThreadsHighCore;
    return std::min(hwc, kMrWarmupMaxThreads);
}

int warmupMrRegistrationParallel(RdmaContext* context, void* addr,
                                 size_t length) {
    if (!context || length == 0) return 0;
    unsigned hwc = std::thread::hardware_concurrency();
    unsigned num_threads = pickMrWarmupThreads(hwc);
    if (num_threads == 0) return 0;
    if (num_threads == 1) {
        return context->warmupMrRegistration(addr, length);
    }
    size_t chunk_size = (length + num_threads - 1) / num_threads;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    std::vector<int> thread_results(num_threads, 0);

    for (unsigned thread_i = 0; thread_i < num_threads; ++thread_i) {
        size_t offset = thread_i * chunk_size;
        if (offset >= length) break;
        size_t block_len = std::min(chunk_size, length - offset);
        void* block_addr = static_cast<char*>(addr) + offset;
        threads.emplace_back(
            [context, thread_i, block_addr, block_len, &thread_results]() {
                thread_results[thread_i] =
                    context->warmupMrRegistration(block_addr, block_len);
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

LocalBufferManager::LocalBufferManager() {}

LocalBufferManager::~LocalBufferManager() { clear(); }

static inline int getAccessFlags(Permission perm,
                                 bool relaxed_ordering = false) {
    int access = IBV_ACCESS_LOCAL_WRITE;
    if (perm == kGlobalReadWrite) {
        access |= IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    } else if (perm == kGlobalReadOnly) {
        access |= IBV_ACCESS_REMOTE_READ;
    }
    if (relaxed_ordering) {
        access |= IBV_ACCESS_RELAXED_ORDERING;
    }
    return access;
}

static void rollbackMemRegistrations(
    const std::vector<RdmaContext*>& contexts,
    const std::vector<RdmaContext::MemReg>& registrations) {
    for (size_t id = 0; id < contexts.size(); ++id) {
        if (registrations[id] &&
            contexts[id]->unregisterMemReg(registrations[id])) {
            LOG(ERROR) << "Failed to roll back RDMA registration";
        }
    }
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
    assert(desc.rkey.empty());
    size_t context_count = 0;
    for (auto* context : context_list_) {
        if (context) ++context_count;
    }

    std::vector<RdmaContext::MemReg> mem_reg_list(context_list_.size(),
                                                  nullptr);
    bool use_parallel_reg = !force_sequential && context_count > 1;
    if (use_parallel_reg) {
        std::vector<std::future<void>> tasks;
        tasks.reserve(context_count);
        void* addr = (void*)desc.addr;
        size_t length = desc.length;
        for (size_t id = 0; id < context_list_.size(); ++id) {
            auto* context = context_list_[id];
            if (!context) continue;
            // Calculate access flags per context (for relaxed ordering support)
            int access = getAccessFlags(options.perm,
                                        context->isRelaxedOrderingEnabled());
            tasks.emplace_back(std::async(
                std::launch::async,
                [context, &mem_reg_list, id, addr, length, access]() {
                    mem_reg_list[id] =
                        context->registerMemReg(addr, length, access);
                }));
        }
        for (auto& task : tasks) task.get();
    } else {
        for (size_t id = 0; id < context_list_.size(); ++id) {
            auto* context = context_list_[id];
            if (!context) continue;
            // Calculate access flags per context (for relaxed ordering support)
            int access = getAccessFlags(options.perm,
                                        context->isRelaxedOrderingEnabled());
            mem_reg_list[id] =
                context->registerMemReg((void*)desc.addr, desc.length, access);
        }
    }
    // If one rail fails, release every MR created for this buffer.
    for (size_t id = 0; id < context_list_.size(); ++id) {
        if (context_list_[id] && !mem_reg_list[id]) {
            rollbackMemRegistrations(context_list_, mem_reg_list);
            return Status::RdmaError(
                "Unable to register buffer of local memory segment" LOC_MARK);
        }
    }
    // NicID-keyed like context_list_, not compacted: slice dispatch subscripts
    // these with a dev_id from the topology, so gaps must stay in place.
    // Devices with no context contribute a zero key that is never selected.
    desc.lkey.assign(context_list_.size(), 0);
    desc.rkey.assign(context_list_.size(), 0);
    for (size_t id = 0; id < context_list_.size(); ++id) {
        if (!context_list_[id]) continue;
        staging.mem_reg_map[context_list_[id]] = mem_reg_list[id];
        auto keys = context_list_[id]->queryMemRegKey(mem_reg_list[id]);
        desc.lkey[id] = keys.first;
        desc.rkey[id] = keys.second;
    }
    staging.options = options;
    {
        RWSpinlock::WriteGuard guard(lock_);
        if (buffer_list_.try_emplace(range, std::move(staging)).second)
            return Status::OK();
    }

    rollbackMemRegistrations(context_list_, mem_reg_list);
    desc.lkey.clear();
    desc.rkey.clear();
    return Status::InvalidArgument(
        "Address region already registered" LOC_MARK);
}

Status LocalBufferManager::addBuffer(std::vector<BufferDesc>& desc_list,
                                     const MemoryOptions& options) {
    if (desc_list.empty()) return Status::OK();
    if (desc_list.size() == 1) {
        return addBufferInternal(desc_list.front(), options, false);
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
    Status result = Status::OK();
    std::vector<bool> registered(tasks.size(), false);
    // Wait for every worker before rolling back successful registrations.
    for (size_t i = 0; i < tasks.size(); ++i) {
        auto status = tasks[i].get();
        registered[i] = status.ok();
        if (!status.ok() && result.ok()) result = status;
    }
    if (!result.ok()) {
        for (size_t i = 0; i < desc_list.size(); ++i) {
            if (!registered[i]) continue;
            auto status = removeBuffer(desc_list[i]);
            if (!status.ok())
                LOG(ERROR) << "Failed to roll back RDMA buffer: "
                           << status.ToString();
        }
    }
    return result;
}

Status LocalBufferManager::removeBuffer(BufferDesc& desc) {
    RWSpinlock::WriteGuard guard(lock_);
    AddressRange range((void*)desc.addr, desc.length);
    auto buffer = buffer_list_.find(range);
    if (buffer == buffer_list_.end()) return Status::OK();

    auto& registrations = buffer->second.mem_reg_map;
    Status result = Status::OK();
    for (auto it = registrations.begin(); it != registrations.end();) {
        if (it->first->unregisterMemReg(it->second)) {
            if (result.ok())
                result = Status::RdmaError(
                    "Unable to unregister buffer of local memory "
                    "segment" LOC_MARK);
            ++it;
        } else {
            it = registrations.erase(it);
        }
    }
    if (!result.ok()) return result;

    desc.lkey.clear();
    desc.rkey.clear();
    buffer_list_.erase(buffer);
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
        auto access =
            getAccessFlags(options.perm, context->isRelaxedOrderingEnabled());
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
