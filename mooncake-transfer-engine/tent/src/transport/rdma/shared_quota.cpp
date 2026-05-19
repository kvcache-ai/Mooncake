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

#include "tent/transport/rdma/shared_quota.h"
#include "tent/common/utils/os.h"
#include "tent/common/types.h"

#include <glog/logging.h>
#include <unistd.h>

namespace mooncake {
namespace tent {
SharedSlotManager::SharedSlotManager(DeviceSelector* device_selector)
    : hdr_(nullptr),
      fd_(-1),
      size_(sizeof(SharedHeader)),
      created_(false),
      device_selector_(device_selector),
      background_running_(false) {}

SharedSlotManager::~SharedSlotManager() { detach(); }

bool SharedSlotManager::isPriorityAllowedInSlot(int priority, int slot) const {
    return priority <= slot;
}

Status SharedSlotManager::initializeHeader() {
    hdr_->magic = 0;
    hdr_->version = 0;
    hdr_->current_slot.store(0, std::memory_order_relaxed);

    Status s = initMutex(&hdr_->global_mutex);
    if (!s.ok()) return s;

    hdr_->version = SHM_VERSION;
    hdr_->magic = SHM_MAGIC;
    hdr_->current_slot.store(0, std::memory_order_release);

    return Status::OK();
}

Status SharedSlotManager::initMutex(pthread_mutex_t* m) {
    pthread_mutexattr_t attr;
    if (pthread_mutexattr_init(&attr) != 0) {
        return Status::InternalError("pthread_mutexattr_init failed");
    }
    if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) != 0) {
        pthread_mutexattr_destroy(&attr);
        return Status::InternalError("pthread_mutexattr_setpshared failed");
    }
#if defined(PTHREAD_MUTEX_ROBUST)
    if (pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST) != 0) {
        pthread_mutexattr_destroy(&attr);
        return Status::InternalError("pthread_mutexattr_setrobust failed");
    }
#endif
    if (pthread_mutex_init(m, &attr) != 0) {
        pthread_mutexattr_destroy(&attr);
        return Status::InternalError("pthread_mutex_init failed");
    }
    pthread_mutexattr_destroy(&attr);
    return Status::OK();
}

Status SharedSlotManager::attach(const std::string& shm_name) {
    name_ = shm_name;

    fd_ = shm_open(name_.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd_ < 0) {
        return Status::InternalError("shm_open failed: " +
                                     std::string(std::strerror(errno)));
    }

    if (ftruncate(fd_, static_cast<off_t>(size_)) != 0) {
        int e = errno;
        close(fd_);
        fd_ = -1;
        return Status::InternalError("ftruncate failed: " +
                                     std::string(std::strerror(e)));
    }

    void* ptr =
        mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (ptr == MAP_FAILED) {
        int e = errno;
        close(fd_);
        fd_ = -1;
        return Status::InternalError("mmap failed: " +
                                     std::string(std::strerror(e)));
    }

    hdr_ = reinterpret_cast<SharedHeader*>(ptr);

    if (hdr_->magic != SHM_MAGIC || hdr_->version != SHM_VERSION) {
        created_ = true;
        Status s = initializeHeader();
        if (!s.ok()) {
            munmap(ptr, size_);
            close(fd_);
            hdr_ = nullptr;
            fd_ = -1;
            return s;
        }
    } else {
        created_ = false;
    }

    startBackgroundThread();

    return Status::OK();
}

Status SharedSlotManager::detach() {
    stopBackgroundThread();

    if (hdr_) {
        munmap(hdr_, size_);
        hdr_ = nullptr;
    }

    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }

    return Status::OK();
}

bool SharedSlotManager::canSend() {
    if (!hdr_) return true;

    // Get current global slot
    int current_slot = hdr_->current_slot.load(std::memory_order_acquire);

    // All processes are treated as HIGH priority for global coordination
    // Device-level filtering is handled in buildCandidates()
    return isPriorityAllowedInSlot(PRIO_HIGH, current_slot);
}

void SharedSlotManager::startBackgroundThread() {
    if (background_running_.exchange(true)) return;

    background_thread_ = std::thread([this]() { backgroundThreadLoop(); });
}

void SharedSlotManager::stopBackgroundThread() {
    if (!background_running_.exchange(false)) return;

    if (background_thread_.joinable()) {
        background_thread_.join();
    }
}

// Background thread: advance global slot periodically
void SharedSlotManager::backgroundThreadLoop() {
    const uint64_t SLEEP_INTERVAL_US = 1000;  // 1ms

    while (background_running_.load(std::memory_order_relaxed)) {
        // Calculate base slot from time
        uint64_t now = getCurrentTimeInNano();
        uint64_t base_slot = now / (rotation_interval_ms_ * 1000000ull);

        pthread_mutex_lock(&hdr_->global_mutex);
        // Update global slot
        int global_slot = static_cast<int>(base_slot % NUM_SLOTS);
        hdr_->current_slot.store(global_slot, std::memory_order_release);
        pthread_mutex_unlock(&hdr_->global_mutex);

        usleep(SLEEP_INTERVAL_US);
    }
}

}  // namespace tent
}  // namespace mooncake
