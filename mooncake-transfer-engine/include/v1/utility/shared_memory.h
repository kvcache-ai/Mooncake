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

#ifndef SHARED_MEMORY_H
#define SHARED_MEMORY_H

#include "v1/common/status.h"
#include "topology.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <string>
#include <stdexcept>
#include <cstring>
#include <vector>
#include <atomic>
#include <iostream>
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>

namespace mooncake {
namespace v1 {
static constexpr int MAX_DEVICES = 64;
static constexpr int MAX_PID_SLOTS = 256;
static constexpr uint64_t SHM_MAGIC = 0x2025082772805202ULL;
static constexpr int SHM_VERSION = 1;

#pragma pack(push, 1)
struct PidUsage {
    pid_t pid;             // 0 == free slot
    uint64_t used_bytes;   // used bytes
    uint8_t reserved[56];  // pad -> 64B
};

struct SharedDeviceEntry {
    int32_t dev_id;
    int32_t numa_id;
    double bw_gbps;
    uint8_t reserved0[32];
    // total active bytes (protected by mutex or atomics)
    uint64_t active_bytes;
    uint8_t reserved1[56];
    PidUsage pid_usages[MAX_PID_SLOTS];
    uint8_t reserved2[64];
};

struct SharedHeader {
    uint64_t magic;
    int32_t version;
    int32_t num_devices;
    uint8_t reserved[56];
    pthread_mutex_t global_mutex;  // robust, process-shared
    SharedDeviceEntry devices[MAX_DEVICES];
};
#pragma pack(pop)

class SharedMemoryManager {
   public:
    SharedMemoryManager() : hdr_(nullptr), fd_(-1), size_(0), created_(false) {}

    ~SharedMemoryManager() { detach(); }

    Status createOrAttach(const std::string& shm_name,
                          const std::shared_ptr<Topology>& topology);

    void detach();

    SharedHeader* get() const { return hdr_; }

    // lock global mutex (handles robust EOWNERDEAD)
    // returns 0 on success, or errno on error
    int lock();

    int unlock();

    // find or create pid slot for a device (must be called with lock held)
    PidUsage* findOrCreatePidSlotLocked(int dev_id, pid_t pid) {
        if (!hdr_) return nullptr;
        if (dev_id < 0 || dev_id >= hdr_->num_devices) return nullptr;
        SharedDeviceEntry& dev = hdr_->devices[dev_id];
        PidUsage* empty = nullptr;
        for (int s = 0; s < MAX_PID_SLOTS; ++s) {
            if (dev.pid_usages[s].pid == pid) return &dev.pid_usages[s];
            if (dev.pid_usages[s].pid == 0 && empty == nullptr)
                empty = &dev.pid_usages[s];
        }
        return empty;  // may be nullptr if full
    }

    PidUsage* findPidSlotLocked(int dev_id, pid_t pid) {
        if (!hdr_) return nullptr;
        if (dev_id < 0 || dev_id >= hdr_->num_devices) return nullptr;
        SharedDeviceEntry& dev = hdr_->devices[dev_id];
        for (int s = 0; s < MAX_PID_SLOTS; ++s) {
            if (dev.pid_usages[s].pid == pid) return &dev.pid_usages[s];
        }
        return nullptr;
    }

    // helpers to read/write active_bytes with atomics (can be used without
    // holding lock)
    uint64_t loadActiveBytes(int dev_id) const {
        if (!hdr_) return 0;
        if (dev_id < 0 || dev_id >= hdr_->num_devices) return 0;
        const uint64_t v = __atomic_load_n(&hdr_->devices[dev_id].active_bytes,
                                           __ATOMIC_SEQ_CST);
        return v;
    }
    void atomicAddActiveBytes(int dev_id, uint64_t delta) {
        if (!hdr_) return;
        if (dev_id < 0 || dev_id >= hdr_->num_devices) return;
        __atomic_fetch_add(&hdr_->devices[dev_id].active_bytes, delta,
                           __ATOMIC_SEQ_CST);
    }
    void atomicSubActiveBytes(int dev_id, uint64_t delta) {
        if (!hdr_) return;
        if (dev_id < 0 || dev_id >= hdr_->num_devices) return;
        __atomic_fetch_sub(&hdr_->devices[dev_id].active_bytes, delta,
                           __ATOMIC_SEQ_CST);
    }

    // direct device info accessors (safe to call without lock for readonly
    // fields)
    int numDevices() const { return hdr_ ? hdr_->num_devices : 0; }
    int getDeviceNuma(int dev_id) const {
        if (!hdr_ || dev_id < 0 || dev_id >= hdr_->num_devices) return -1;
        return hdr_->devices[dev_id].numa_id;
    }
    double getDeviceBW(int dev_id) const {
        if (!hdr_ || dev_id < 0 || dev_id >= hdr_->num_devices) return 0.0;
        return hdr_->devices[dev_id].bw_gbps;
    }

   private:
    Status initializeHeader(const std::shared_ptr<Topology>& topology);

    Status initMutex(pthread_mutex_t* m);

    // reclaim implementation: caller must hold the mutex or call via lock()
    void reclaimDeadPidsInternal();

    static bool isPidAlive(pid_t pid) {
        if (pid <= 0) return false;
        // kill(pid, 0) returns 0 if exists and permission ok; ESRCH if not
        // exists
        int r = kill(pid, 0);
        if (r == 0) return true;
        if (errno == ESRCH) return false;
        // if EPERM, process exists but we can't signal it; treat as alive
        return true;
    }

   private:
    std::string name_;
    SharedHeader* hdr_;
    int fd_;
    size_t size_;
    bool created_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // SHARED_MEMORY_H