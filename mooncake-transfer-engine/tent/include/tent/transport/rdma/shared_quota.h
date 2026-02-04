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

#ifndef TENT_SHARED_QUOTA_H
#define TENT_SHARED_QUOTA_H

#include "quota.h"
#include "tent/common/status.h"
#include "tent/runtime/topology.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <string>
#include <cstring>
#include <atomic>
#include <vector>
#include <iostream>
#include <errno.h>

namespace mooncake {
namespace tent {

static constexpr int MAX_DEVICES = 64;
static constexpr int MAX_PID_SLOTS = 256;
static constexpr uint64_t SHM_MAGIC = 0x2025082772805202ULL;
static constexpr int SHM_VERSION = 1;

struct PidUsage {
    pid_t pid;                     // 0 == free slot
    volatile uint64_t used_bytes;  // local used bytes reported by this pid
    uint8_t reserved[56];          // padding -> total 64B
};

struct SharedDeviceEntry {
    char dev_name[56];  // NUL-terminated device name, empty means unused
    volatile uint64_t active_bytes;
    PidUsage pid_usages[MAX_PID_SLOTS];
};

struct SharedHeader {
    uint64_t magic;
    int32_t version;
    int32_t num_devices;
    pthread_mutex_t global_mutex;
    SharedDeviceEntry devices[MAX_DEVICES];
};

class DeviceQuota;
class SharedQuotaManager {
   public:
    explicit SharedQuotaManager(DeviceQuota* local_quota);
    ~SharedQuotaManager();

    Status attach(const std::string& shm_name);
    Status detach();

    Status diffusion();

   private:
    Status attachProcess();
    Status detachProcess();

   private:
    PidUsage* findOrCreatePidSlotLocked(int dev_id, pid_t pid);
    PidUsage* findPidSlotLocked(int dev_id, pid_t pid);
    int findDeviceIdByNameLocked(const std::string& dev_name);
    Status initializeHeader();
    Status initMutex(pthread_mutex_t* m);
    void reclaimDeadPidsInternal();
    static bool isPidAlive(pid_t pid);
    int lock();
    int unlock();

   private:
    std::string name_;
    SharedHeader* hdr_;
    int fd_;
    size_t size_;
    bool created_;
    DeviceQuota* local_quota_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SHARED_QUOTA_H