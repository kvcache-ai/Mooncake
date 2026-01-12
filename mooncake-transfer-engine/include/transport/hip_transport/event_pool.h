// Copyright(C) 2025 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef EVENT_POOL_H_
#define EVENT_POOL_H_

#include <hip/hip_runtime.h>

#include <mutex>
#include <queue>
#include <unordered_map>

namespace mooncake {

// EventPool manages a pool of HIP events per device for async operations
class EventPool {
   public:
    explicit EventPool(int num_events_per_device);
    ~EventPool();

    // Disable copy (to prevent double-free of HIP resources)
    EventPool(const EventPool &) = delete;
    EventPool &operator=(const EventPool &) = delete;

    // Disable move (std::mutex is not movable)
    EventPool(EventPool &&) = delete;
    EventPool &operator=(EventPool &&) = delete;

    // Get an event from the pool (creates new ones if pool is empty)
    hipEvent_t getEvent(int device_id);

    // Return an event to the pool for reuse
    void putEvent(hipEvent_t event, int device_id);

   private:
    bool initializeEventsForDevice(int device_id);

    // Helper function to create a single event for a device
    hipEvent_t createEventForDevice(int device_id);

    // Helper function to create an event assuming device is already set
    hipEvent_t createEvent();

    int num_events_per_device_;
    std::mutex mutex_;
    std::unordered_map<int, std::queue<hipEvent_t>> event_pools_;
};

}  // namespace mooncake

#endif  // EVENT_POOL_H_
