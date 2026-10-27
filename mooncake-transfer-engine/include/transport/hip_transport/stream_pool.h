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

#ifndef STREAM_POOL_H_
#define STREAM_POOL_H_

#include <hip/hip_runtime.h>

#include <mutex>
#include <unordered_map>
#include <vector>

namespace mooncake {

// StreamPool manages a pool of HIP streams per device for async operations
class StreamPool {
   public:
    explicit StreamPool(int num_streams_per_device);
    ~StreamPool();

    // Disable copy (to prevent double-free of HIP resources)
    StreamPool(const StreamPool &) = delete;
    StreamPool &operator=(const StreamPool &) = delete;

    // Disable move (std::mutex is not movable)
    StreamPool(StreamPool &&) = delete;
    StreamPool &operator=(StreamPool &&) = delete;

    // Get next stream for a device in round-robin fashion
    hipStream_t getNextStream(int device_id);

   private:
    bool initializeStreamsForDevice(int device_id);

    int num_streams_per_device_;
    std::mutex mutex_;
    std::unordered_map<int, std::vector<hipStream_t>> streams_;
    std::unordered_map<int, int> current_stream_idx_;
};

}  // namespace mooncake

#endif  // STREAM_POOL_H_
