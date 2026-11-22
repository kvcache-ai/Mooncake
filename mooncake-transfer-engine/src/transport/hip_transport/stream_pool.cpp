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

#include "transport/hip_transport/stream_pool.h"

#include <glog/logging.h>

#include <cstdlib>
#include <cstring>
#include <string>

namespace mooncake {

namespace {

// MC_HIP_STREAM_PRIORITY=default|high. Read once before creating the first
// pool. Priority is an opt-in scheduling hint; it does not guarantee
// preemption.
bool useHighPriorityStreams() {
    static const bool high = [] {
        const char *v = std::getenv("MC_HIP_STREAM_PRIORITY");
        bool h = v != nullptr && std::strcmp(v, "high") == 0;
        if (v != nullptr && !h && std::strcmp(v, "default") != 0) {
            LOG(WARNING) << "StreamPool: ignoring MC_HIP_STREAM_PRIORITY=" << v
                         << " (want default|high)";
        }
        return h;
    }();
    return high;
}

}  // namespace

StreamPool::StreamPool(int num_streams_per_device)
    : num_streams_per_device_(num_streams_per_device) {
    if (num_streams_per_device_ == 0) {
        num_streams_per_device_ = 1;
    }
}

StreamPool::~StreamPool() {
    for (auto &device_entry : streams_) {
        for (auto stream : device_entry.second) {
            if (stream != nullptr) {
                (void)hipStreamDestroy(stream);
            }
        }
    }
}

hipStream_t StreamPool::getNextStream(int device_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Initialize streams for this device if not done yet
    if (streams_.find(device_id) == streams_.end()) {
        if (!initializeStreamsForDevice(device_id)) {
            return nullptr;
        }
    }

    auto &device_streams = streams_[device_id];
    if (device_streams.empty()) {
        return nullptr;
    }

    // Round-robin selection
    auto &current_idx = current_stream_idx_[device_id];
    hipStream_t stream = device_streams[current_idx];
    current_idx = (current_idx + 1) % (int)device_streams.size();

    return stream;
}

bool StreamPool::initializeStreamsForDevice(int device_id) {
    hipError_t err = hipSetDevice(device_id);
    if (err != hipSuccess) {
        LOG(ERROR) << "StreamPool: Failed to set device " << device_id << ": "
                   << hipGetErrorString(err);
        return false;
    }

    std::vector<hipStream_t> device_streams;
    device_streams.reserve(num_streams_per_device_);

    int least_priority = 0, greatest_priority = 0;
    const bool high = useHighPriorityStreams();
    if (high) {
        err = hipDeviceGetStreamPriorityRange(&least_priority,
                                              &greatest_priority);
        if (err != hipSuccess) {
            LOG(ERROR) << "StreamPool: hipDeviceGetStreamPriorityRange failed: "
                       << hipGetErrorString(err);
            return false;
        }
    }
    LOG(INFO) << "StreamPool: device " << device_id << ": "
              << num_streams_per_device_ << " non-blocking streams, priority "
              << (high ? "high (" + std::to_string(greatest_priority) + ")"
                       : std::string("default"));

    for (int i = 0; i < num_streams_per_device_; ++i) {
        hipStream_t stream;
        err = high ? hipStreamCreateWithPriority(&stream, hipStreamNonBlocking,
                                                 greatest_priority)
                   : hipStreamCreateWithFlags(&stream, hipStreamNonBlocking);
        if (err != hipSuccess) {
            LOG(ERROR) << "StreamPool: Failed to create stream for device "
                       << device_id << ": " << hipGetErrorString(err);
            // Clean up already created streams
            for (auto s : device_streams) {
                (void)hipStreamDestroy(s);
            }
            return false;
        }
        device_streams.push_back(stream);
    }

    streams_[device_id] = std::move(device_streams);
    current_stream_idx_[device_id] = 0;
    return true;
}

}  // namespace mooncake
