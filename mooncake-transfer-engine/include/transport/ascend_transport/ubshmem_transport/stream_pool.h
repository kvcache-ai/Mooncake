// Copyright 2025 Huawei Technologies Co., Ltd
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

#ifndef STREAM_POOL_H_
#define STREAM_POOL_H_

#include <acl/acl.h>

#include <mutex>
#include <unordered_map>
#include <vector>

namespace mooncake {

// StreamPool manages a pool of ACL streams for async operations
// Provides stream creation, allocation, and release functionality
class StreamPool {
   public:
    explicit StreamPool(int max_streams);
    ~StreamPool();

    StreamPool(const StreamPool &) = delete;
    StreamPool &operator=(const StreamPool &) = delete;

    StreamPool(StreamPool &&) = delete;
    StreamPool &operator=(StreamPool &&) = delete;

    // Get multiple streams for parallel processing (blocking with timeout)
    bool tryGetStreams(size_t stream_num, std::vector<aclrtStream> &streams,
                       uint64_t timeout_ms = 3000);

    // Try to get streams once without waiting
    bool tryGetStreamsOnce(size_t stream_num,
                           std::vector<aclrtStream> &streams);

    // Release streams back to the pool
    void releaseStreams(std::vector<aclrtStream> &streams);

    // Get total number of streams in pool
    size_t getTotalStreamCount() const;

    // Get number of available streams
    size_t getAvailableStreamCount() const;

   private:
    bool initializeStreams();
    aclrtStream createStream();
    void destroyStream(aclrtStream stream);

    int max_streams_;

    mutable std::mutex mutex_;
    std::vector<aclrtStream> streams_;
    std::unordered_map<aclrtStream, bool> stream_available_;
};

}  // namespace mooncake

#endif  // STREAM_POOL_H_
