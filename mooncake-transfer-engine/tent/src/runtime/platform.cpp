// Copyright 2024 KVCache.AI
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

#include "tent/runtime/platform.h"

#include <atomic>
#include <chrono>
#include <list>
#include <cstdlib>
#include <thread>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>
#include <unistd.h>

#ifdef USE_CUDA
#include "tent/platform/cuda.h"
#elif defined(USE_HIP)
#include "tent/platform/rocm.h"
#elif defined(USE_SUNRISE)
#include "tent/platform/sunrise.h"
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || \
    defined(USE_MEMFABRIC)
#include "tent/platform/ascend.h"
#elif defined(USE_TPU)
#include "tent/platform/tpu.h"
#elif defined(USE_MLU)
#include "tent/platform/mlu.h"
#elif defined(USE_XPU)
#include "tent/platform/xpu.h"
#else
#include "tent/platform/cpu.h"
#endif

namespace mooncake {
namespace tent {

void DmabufExport::close() {
    if (!close_fd || fd < 0) return;
    const int owned_fd = fd;
    fd = -1;
    close_fd = false;
    if (::close(owned_fd) != 0) PLOG(WARNING) << "Failed to close dma-buf fd";
}

Status Platform::copyWithCompletion(void* dst, void* src, size_t length,
                                    PendingCopy& pending) {
#ifdef USE_CUDA
    if (auto* cuda = dynamic_cast<CudaPlatform*>(this))
        return cuda->copy(dst, src, length, &pending);
#elif defined(USE_HIP)
    if (auto* rocm = dynamic_cast<RocmPlatform*>(this))
        return rocm->copy(dst, src, length, &pending);
#endif
    return copy(dst, src, length);
}

Status CopyCompletion::wait() {
    for (;;) {
        bool finished = false;
        CHECK_STATUS(query(finished));
        if (finished) return Status::OK();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

namespace {
struct CopyQuarantine {
    std::mutex mutex;
    std::list<std::pair<std::shared_ptr<CopyCompletion>, std::function<void()>>>
        copies;
    std::atomic<bool> pending{false};
};

CopyQuarantine& copyQuarantine() {
    // An unconfirmed DMA may outlive all transports and static destructors.
    static auto* quarantine = new CopyQuarantine;
    return *quarantine;
}
}  // namespace

void CopyCompletion::quarantine(std::shared_ptr<CopyCompletion> copy,
                                std::function<void()> release_staging) {
    auto& queue = copyQuarantine();
    std::lock_guard<std::mutex> lock(queue.mutex);
    queue.copies.emplace_back(std::move(copy), std::move(release_staging));
    queue.pending = true;
}

void CopyCompletion::reclaimQuarantined() {
    auto& queue = copyQuarantine();
    if (!queue.pending.load(std::memory_order_relaxed)) return;
    // Query each entry present at the start once, outside the queue lock.
    decltype(queue.copies) copies;
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        copies.swap(queue.copies);
        queue.pending = false;
    }
    for (auto it = copies.begin(); it != copies.end();) {
        bool finished = false;
        try {
            if (it->first->query(finished).ok() && finished) {
                it->second();
                it = copies.erase(it);
                continue;
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "Could not reclaim staging: " << error.what();
        }
        ++it;
    }
    if (copies.empty()) return;
    std::lock_guard<std::mutex> lock(queue.mutex);
    queue.copies.splice(queue.copies.end(), copies);
    queue.pending = true;
}

Platform& Platform::getLoader(std::shared_ptr<Config> conf) {
    static std::shared_ptr<Platform> g_instance;
    static std::once_flag flag;
    std::call_once(flag, [conf = std::move(conf)]() {
        const auto init_conf = conf ? conf : std::make_shared<Config>();
#ifdef USE_CUDA
        g_instance = std::make_shared<CudaPlatform>(init_conf);
#elif defined(USE_HIP)
        g_instance = std::make_shared<RocmPlatform>(init_conf);
#elif defined(USE_SUNRISE)
        g_instance = std::make_shared<SunrisePlatform>(init_conf);
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || \
    defined(USE_MEMFABRIC)
        g_instance = std::make_shared<AscendPlatform>(init_conf);
#elif defined(USE_TPU)
        g_instance = std::make_shared<TpuPlatform>(init_conf);
#elif defined(USE_MLU)
            g_instance = std::make_shared<MluPlatform>(init_conf);
#elif defined(USE_XPU)
            g_instance = std::make_shared<XpuPlatform>(init_conf);
#else
        g_instance = std::make_shared<CpuPlatform>(init_conf);
#endif
    });
    return *g_instance;
}

Status Platform::synchronizeDevices(const Topology* topology) {
    (void)topology;
    return Status::OK();
}

Status Platform::exportDmabuf(void* addr, size_t length, DmabufExport& out) {
    (void)addr;
    (void)length;
    out = DmabufExport{};
    return Status::NotImplemented(
        "dma-buf export is not supported by this platform" LOC_MARK);
}

// Fallback for platforms without a pinned-host API: ordinary pageable
// memory (see platform.h). Page alignment is required for O_DIRECT staging.
Status Platform::allocatePinnedHost(void** pptr, size_t size) {
    if (!pptr || size == 0)
        return Status::InvalidArgument(
            "Invalid pinned host allocation" LOC_MARK);
    if (posix_memalign(pptr, 4096, size) != 0) {
        *pptr = nullptr;
        return Status::InternalError("posix_memalign failed" LOC_MARK);
    }
    return Status::OK();
}

Status Platform::freePinnedHost(void* ptr, size_t size) {
    (void)size;
    std::free(ptr);
    return Status::OK();
}

std::vector<int> Platform::topologyDeviceIndices(const Topology* topology,
                                                 Topology::MemType mem_type) {
    std::vector<int> devices;
    if (!topology) return devices;

    std::unordered_set<int> seen;
    const size_t mem_count = topology->getMemCount();
    for (size_t i = 0; i < mem_count; ++i) {
        const auto* mem =
            topology->getMemEntry(static_cast<Topology::MemID>(i));
        if (!mem || mem->type != mem_type) continue;
        LocationParser parser(mem->name);
        const int device = parser.index();
        if (device < 0 || !seen.insert(device).second) continue;
        devices.push_back(device);
    }
    return devices;
}
}  // namespace tent
}  // namespace mooncake
