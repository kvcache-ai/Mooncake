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

#include "shared_quota.h"

namespace mooncake {
namespace v1 {
class DeviceQuota {
   public:
    struct DeviceInfo {
        int dev_id;
        double bw_gbps;
        int numa_id;
        uint64_t active_bytes{0};
        uint64_t local_quota{0};  // if shared quota is used
        double avg_latency = 0;
    };

   public:
    DeviceQuota() = default;

    ~DeviceQuota() = default;

    DeviceQuota(const DeviceQuota &) = delete;

    DeviceQuota &operator=(const DeviceQuota &) = delete;

    Status loadTopology(std::shared_ptr<Topology> &local_topology);

    Status enableSharedQuota(const std::string &shm_name);

    Status allocate(uint64_t length, const std::string &location,
                    int &chosen_dev_id);

    Status release(int dev_id, uint64_t length, double latency);

   private:
    std::shared_ptr<Topology> local_topology_;
    std::unordered_map<int, DeviceInfo> devices_;
    mutable std::shared_mutex rwlock_;
    uint64_t slice_size_ = 64 * 1024;
    uint64_t alloc_units_ = 1024;
    bool allow_cross_numa_ = false;
    std::shared_ptr<SharedQuotaManager> shared_quota_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // DEVICE_QUOTA_H_