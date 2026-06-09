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

#ifndef TENT_SHARED_SLOT_H
#define TENT_SHARED_SLOT_H

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
#include <thread>

namespace mooncake {
namespace tent {

static constexpr uint64_t SHM_MAGIC = 0x2025082772805203ULL;
static constexpr int SHM_VERSION = 10;

// Time slice configuration for process-level coordination
// Slot 0:            HIGH only
// Slot 1:            MEDIUM + HIGH
// Slot 2:            ALL (LOW + MEDIUM + HIGH)
// Then repeat
static constexpr int NUM_SLOTS = PRIO_LOW + 1;

struct SharedHeader {
    uint64_t magic;
    int32_t version;
    std::atomic<int> current_slot;
    pthread_mutex_t global_mutex;
};

class DeviceSelector;
class SharedSlotManager {
   public:
    explicit SharedSlotManager(DeviceSelector* local_quota);
    ~SharedSlotManager();

    Status attach(const std::string& shm_name);
    Status detach();

    // Check if current process can send (global slot)
    // Returns true if given priority is allowed in current global slot
    bool canSend(int priority = PRIO_HIGH);

    // Set slot duration in milliseconds (must be > 0)
    void setRotationIntervalMs(int ms) { rotation_interval_ms_ = ms; }
    int getRotationIntervalMs() const { return rotation_interval_ms_; }

   private:
    void startBackgroundThread();
    void stopBackgroundThread();
    void backgroundThreadLoop();
    Status initializeHeader();
    Status initMutex(pthread_mutex_t* m);

    // Check if a priority is allowed in the given slot
    bool isPriorityAllowedInSlot(int priority, int slot) const;

   private:
    std::string name_;
    SharedHeader* hdr_;
    int fd_;
    size_t size_;
    bool created_;
    DeviceSelector* device_selector_;
    int rotation_interval_ms_ = 2;  // Default: 2ms per slot

    // Background thread
    std::thread background_thread_;
    std::atomic<bool> background_running_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SHARED_SLOT_H