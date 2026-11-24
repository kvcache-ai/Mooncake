// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MOONCAKE_RAIL_METRICS_H
#define MOONCAKE_RAIL_METRICS_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <utility>

namespace mooncake {

struct RailCounters {
    std::atomic<uint64_t> completions{0};
    std::atomic<uint64_t> bytes{0};
    std::atomic<uint64_t> latency_ns{0};
    std::atomic<uint64_t> errors{0};
};

class RailMetricsRegistry {
   public:
    explicit RailMetricsRegistry(size_t max_rails) : max_rails_(max_rails) {}

    static RailMetricsRegistry* instanceIfEnabled();

    // Entries remain valid across endpoint destruction and subsequent inserts.
    RailCounters* getOrAdd(const std::string& local_nic,
                           const std::string& peer_nic_path);
    void appendPrometheus(std::string& out);

   private:
    struct Entry {
        explicit Entry(std::string label_set) : labels(std::move(label_set)) {}

        std::string labels;
        RailCounters counters;
    };

    const size_t max_rails_;
    std::mutex mutex_;
    std::map<std::pair<std::string, std::string>, Entry> rails_;
    Entry overflow_{R"({local_nic="other",peer="other",peer_nic="other"})"};
    bool overflow_used_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_RAIL_METRICS_H
