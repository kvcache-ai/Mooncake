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

#include "transport/ascend_transport/ubshmem_transport/stream_pool.h"

#include <thread>
#include <glog/logging.h>

namespace mooncake {

StreamPool::StreamPool(int max_streams) : max_streams_(max_streams) {
    if (max_streams_ <= 0) {
        max_streams_ = 32;  // Default: 32 streams total (8 threads × 4 streams)
    }
}

StreamPool::~StreamPool() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto stream : streams_) {
        if (stream != nullptr) {
            destroyStream(stream);
        }
    }
    streams_.clear();
    stream_available_.clear();
}

aclrtStream StreamPool::createStream() {
    aclrtStream stream;
    aclError err = aclrtCreateStreamWithConfig(
        &stream, 0, ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC);
    if (err != ACL_ERROR_NONE) {
        LOG(ERROR) << "StreamPool: Failed to create stream: "
                   << aclGetRecentErrMsg();
        return nullptr;
    }
    return stream;
}

void StreamPool::destroyStream(aclrtStream stream) {
    if (stream != nullptr) {
        (void)aclrtDestroyStream(stream);
    }
}

bool StreamPool::initializeStreams() {
    return true;  // Lazy initialization - streams created on demand
}

bool StreamPool::tryGetStreamsOnce(size_t stream_num,
                                   std::vector<aclrtStream> &streams) {
    std::lock_guard<std::mutex> lock(mutex_);

    streams.clear();
    streams.reserve(stream_num);

    // Find available streams
    for (auto stream : streams_) {
        if (stream_available_[stream] && streams.size() < stream_num) {
            streams.push_back(stream);
            stream_available_[stream] = false;  // mark as used
        }
    }

    // If not enough available streams, create new ones if possible
    while (streams.size() < stream_num &&
           streams_.size() < static_cast<size_t>(max_streams_)) {
        aclrtStream stream = createStream();
        if (stream == nullptr) {
            LOG(ERROR) << "StreamPool: Failed to create new stream";
            // Release already allocated streams
            for (auto s : streams) {
                stream_available_[s] = true;
            }
            streams.clear();
            return false;
        }
        streams_.push_back(stream);
        stream_available_[stream] = false;  // new stream is immediately used
        streams.push_back(stream);
    }

    return streams.size() == stream_num;
}

bool StreamPool::tryGetStreams(size_t stream_num,
                               std::vector<aclrtStream> &streams,
                               uint64_t timeout_ms) {
    auto start = std::chrono::steady_clock::now();
    auto timeout = std::chrono::milliseconds(timeout_ms);

    while (true) {
        if (tryGetStreamsOnce(stream_num, streams)) {
            return true;
        }

        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed >= timeout) {
            LOG(ERROR) << "StreamPool: Timeout waiting for streams (elapsed: "
                       << std::chrono::duration_cast<std::chrono::milliseconds>(
                              elapsed)
                              .count()
                       << "ms)";
            return false;
        }

        // Wait for a short time before retrying to avoid busy loop
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
}

void StreamPool::releaseStreams(std::vector<aclrtStream> &streams) {
    std::lock_guard<std::mutex> lock(mutex_);

    for (auto stream : streams) {
        if (stream_available_.find(stream) != stream_available_.end()) {
            stream_available_[stream] = true;
        }
    }

    streams.clear();
}

size_t StreamPool::getTotalStreamCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return streams_.size();
}

size_t StreamPool::getAvailableStreamCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (auto &entry : stream_available_) {
        if (entry.second) {
            count++;
        }
    }
    return count;
}

}  // namespace mooncake
