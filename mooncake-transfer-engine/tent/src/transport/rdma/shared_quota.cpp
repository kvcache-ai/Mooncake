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
#ifdef __linux__
    // PTHREAD_MUTEX_ROBUST is an enum constant on glibc, not a macro.
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

    const int error = background_error_.load(std::memory_order_acquire);
    if (error != 0) {
        return Status::InternalError("Shared quota publisher failed: " +
                                     std::string(std::strerror(error)));
    }
    return Status::OK();
}

bool SharedSlotManager::canSend(int priority) {
    if (!hdr_) return true;

    int current_slot;
    if (background_error_.load(std::memory_order_acquire) != 0) {
        // The slot is time-derived. A stopped publisher can still follow the
        // same host-wide arbitration schedule using the local clock.
        const uint64_t now = getCurrentTimeInNano();
        const uint64_t base_slot = now / (rotation_interval_ms_ * 1000000ull);
        current_slot = static_cast<int>(base_slot % NUM_SLOTS);
    } else {
        current_slot = hdr_->current_slot.load(std::memory_order_acquire);
    }

    // Check if the given priority is allowed in current global slot
    // Slot 0: HIGH only (priority 0 <= 0)
    // Slot 1: HIGH + MEDIUM (priority 0 or 1 <= 1)
    // Slot 2: ALL (priority 0, 1, or 2 <= 2)
    return isPriorityAllowedInSlot(priority, current_slot);
}

void SharedSlotManager::startBackgroundThread() {
    if (background_running_.exchange(true)) return;

    background_error_.store(0, std::memory_order_release);
    background_thread_ = std::thread([this]() { backgroundThreadLoop(); });
}

void SharedSlotManager::stopBackgroundThread() {
    background_running_.store(false, std::memory_order_relaxed);

    if (background_thread_.joinable()) {
        background_thread_.join();
    }
}

// Background thread: advance global slot periodically
void SharedSlotManager::backgroundThreadLoop() {
    const uint64_t SLEEP_INTERVAL_US = 1000;  // 1ms
    auto report_error = [this](const char* operation, int error) {
        background_error_.store(error, std::memory_order_release);
        LOG(ERROR) << "Shared quota " << operation
                   << " failed: " << std::strerror(error);
    };

    while (background_running_.load(std::memory_order_relaxed)) {
        // Never block shutdown on another publisher (including old shared
        // objects initialized with a non-robust mutex).
        int error = pthread_mutex_trylock(&hdr_->global_mutex);
        if (error == EBUSY) {
            usleep(SLEEP_INTERVAL_US);
            continue;
        }
        bool owner_died = false;
#ifdef __linux__
        owner_died = error == EOWNERDEAD;
#endif
        if (error != 0 && !owner_died) {
            report_error("pthread_mutex_trylock", error);
            return;
        }

        // The slot is the only protected state. Recompute it after acquiring
        // the mutex, also repairing any update interrupted by owner death.
        uint64_t now = getCurrentTimeInNano();
        uint64_t base_slot = now / (rotation_interval_ms_ * 1000000ull);
        int global_slot = static_cast<int>(base_slot % NUM_SLOTS);
        hdr_->current_slot.store(global_slot, std::memory_order_release);
#ifdef __linux__
        if (owner_died) {
            error = pthread_mutex_consistent(&hdr_->global_mutex);
            if (error != 0) report_error("pthread_mutex_consistent", error);
        }
#endif
        const int unlock_error = pthread_mutex_unlock(&hdr_->global_mutex);
        if (unlock_error != 0) {
            report_error("pthread_mutex_unlock", unlock_error);
            return;
        }
        if (background_error_.load(std::memory_order_acquire) != 0) return;

        usleep(SLEEP_INTERVAL_US);
    }
}

}  // namespace tent
}  // namespace mooncake
