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

// JobLease returned to caller
struct JobLease {
    LeaseId id;
    std::string nic_name;
    uint64_t bytes;  // allocated bytes
    TimePoint expires_at;
    pid_t owner_pid;
};

struct SharedState;

class Scheduler {
   public:
    // shm_name: shared memory object name (must start with '/')
    // default_ttl: default lease TTL
    Scheduler(const std::string& shm_name = "/mooncake_sched",
              Duration default_ttl = Duration(30000));

    ~Scheduler();

    Status createJobs(uint64_t total_bytes,
                      const std::vector<std::string>& device_list,
                      std::vector<JobLease>& jobs,
                      Duration requested_ttl = Duration(30000));

    // Close/release a lease (pass the JobLease returned earlier)
    // Returns true if found & released.
    bool closeJob(const JobLease& lease);

    // Manually reclaim expired leases; returns number reclaimed.
    size_t reclaimExpiredLeases();

   private:
    int findDeviceIndexByName(const std::string& name);

    size_t reclaimExpiredLeasesUnlocked();

    SharedState* state_;
    std::string shm_name_;
    Duration default_ttl_;
    std::mutex local_mtx_;
};

}  // namespace v1
}  // namespace mooncake

#endif  // SCHEDULER_H