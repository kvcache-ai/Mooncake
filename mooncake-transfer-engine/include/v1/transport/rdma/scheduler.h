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

#ifndef SCHEDULER_H
#define SCHEDULER_H

#include "v1/common/status.h"

#include <string>
#include <vector>
#include <cstdint>
#include <chrono>
#include <optional>
#include <mutex>
#include <unordered_map>

namespace mooncake {
namespace v1 {
using Clock = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Clock>;
using Duration = std::chrono::milliseconds;
using LeaseId = uint64_t;

struct SharedState;

class Scheduler {
   public:
    Scheduler(const std::string& shm_name = "/mooncake_sched",
              Duration default_ttl = Duration(5000));

    ~Scheduler();

    Status createJob(LeaseId& lease, int& selected_index, uint64_t length,
                     const std::vector<std::string>& device_list);

    bool closeJob(LeaseId lease);

    size_t reclaimExpiredLeases();

    Status getEstimatedDeviceLoads(
        std::unordered_map<std::string, uint64_t>& result);
    
    Status constructState() {
        if (!state_) return Status::InternalError("no shared state");
        return Status::OK();
    }

   private:
    size_t reclaimExpiredLeasesUnlocked();

    SharedState* state_;
    std::string shm_name_;
    Duration default_ttl_;
    std::mutex local_mtx_;
    std::unordered_map<std::string, int> device_name_cache_;
};

}  // namespace v1
}  // namespace mooncake

#endif  // SCHEDULER_H