// Copyright 2026 Huawei Technologies Co., Ltd
// Copyright 2026 KVCache.AI
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

#ifndef UB_SEGMENT_H_
#define UB_SEGMENT_H_

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {

inline constexpr char NPU_PREFIX[] = "npu:";

class UbSegment {
   public:
    static constexpr const char *kNpuPrefix = NPU_PREFIX;

    // Registering a non-NPU location is a successful no-op. Re-registering
    // the same (device, VA, size) is also an idempotent successful no-op.
    // With USE_ASCEND_RDMA enabled, missing Ascend symbols fail NPU registration.
    int RegUbSegment(const std::string &location, uint64_t va, uint64_t size);

    // Unregistering a non-NPU or unknown location is a successful no-op.
    int UnRegUbSegment(const std::string &location, uint64_t va);

   private:
    struct SegmentInfo {
        int32_t user_device_id;
        uint64_t size;
    };

    std::unordered_map<uint64_t, std::vector<SegmentInfo>> segments_;
    std::mutex mutex_;
};

}  // namespace mooncake

#endif  // UB_SEGMENT_H_
