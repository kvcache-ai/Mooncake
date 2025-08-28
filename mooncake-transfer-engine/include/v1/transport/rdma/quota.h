// Copyright 2025 KVCache.AI
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

#ifndef DEVICE_QUOTA_H_
#define DEVICE_QUOTA_H_

#include <atomic>
#include <vector>
#include <unordered_map>
#include <cmath>
#include <algorithm>
#include <cstdint>
#include <shared_mutex>  // C++17
#include <mutex>

#include "v1/common/status.h"
#include "v1/utility/topology.h"

namespace mooncake {
namespace v1 {
class DeviceQuota {
   public:
    struct AllocPlan {
        int dev_id;
        uint64_t offset;
        uint64_t length;
    };

    struct DeviceInfo {
        int dev_id;
        double bw_gbps;
        int numa_id;
        std::atomic<uint64_t> active_bytes{0};
    };

   public:
    DeviceQuota() = default;

    ~DeviceQuota() = default;

    DeviceQuota(const DeviceQuota &) = delete;

    DeviceQuota &operator=(const DeviceQuota &) = delete;

    Status loadTopology(std::shared_ptr<Topology> &local_topology);

    Status allocate(uint64_t data_size, const std::string &location,
                    std::vector<AllocPlan> &plan_list);

    Status release(int dev_id, uint64_t length);

   private:
    std::shared_ptr<Topology> local_topology_;
    std::unordered_map<int, DeviceInfo> devices_;
    mutable std::shared_mutex rwlock_;
    uint64_t slice_size_ = 64 * 1024;
    bool allow_cross_numa_ = false;
};
}  // namespace v1
}  // namespace mooncake

#endif  // DEVICE_QUOTA_H_