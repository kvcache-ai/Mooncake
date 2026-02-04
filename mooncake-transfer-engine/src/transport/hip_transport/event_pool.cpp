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

#include "transport/hip_transport/event_pool.h"

#include <glog/logging.h>

namespace mooncake {

EventPool::EventPool(int num_events_per_device)
    : num_events_per_device_(num_events_per_device) {
    if (num_events_per_device_ == 0) {
        num_events_per_device_ = 1;
    }
}

EventPool::~EventPool() {
    for (auto &device_entry : event_pools_) {
        while (!device_entry.second.empty()) {
            hipEvent_t event = device_entry.second.front();
            device_entry.second.pop();
            if (event != nullptr) {
                (void)hipEventDestroy(event);
            }
        }
    }
}

hipEvent_t EventPool::getEvent(int device_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Initialize event pool for this device if not done yet
    if (event_pools_.find(device_id) == event_pools_.end()) {
        event_pools_[device_id] = std::queue<hipEvent_t>();
        if (!initializeEventsForDevice(device_id)) {
            return nullptr;
        }
    }

    auto &pool = event_pools_[device_id];

    // If pool is empty, create a new event on demand
    if (pool.empty()) {
        return createEventForDevice(device_id);
    }

    hipEvent_t event = pool.front();
    pool.pop();
    return event;
}

void EventPool::putEvent(hipEvent_t event, int device_id) {
    if (event == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    event_pools_[device_id].push(event);
}

hipEvent_t EventPool::createEvent() {
    hipEvent_t event;
    hipError_t err = hipEventCreateWithFlags(&event, hipEventDisableTiming);
    if (err != hipSuccess) {
        LOG(ERROR) << "EventPool: Failed to create event: "
                   << hipGetErrorString(err);
        return nullptr;
    }
    return event;
}

hipEvent_t EventPool::createEventForDevice(int device_id) {
    hipError_t err = hipSetDevice(device_id);
    if (err != hipSuccess) {
        LOG(ERROR) << "EventPool: Failed to set device " << device_id << ": "
                   << hipGetErrorString(err);
        return nullptr;
    }

    return createEvent();
}

bool EventPool::initializeEventsForDevice(int device_id) {
    hipError_t err = hipSetDevice(device_id);
    if (err != hipSuccess) {
        LOG(ERROR) << "EventPool: Failed to set device " << device_id << ": "
                   << hipGetErrorString(err);
        return false;
    }

    auto &pool = event_pools_[device_id];
    for (int i = 0; i < num_events_per_device_; ++i) {
        hipEvent_t event = createEvent();
        if (event == nullptr) {
            return false;
        }
        pool.push(event);
    }
    return true;
}

}  // namespace mooncake
