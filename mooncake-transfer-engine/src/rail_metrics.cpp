// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rail_metrics.h"

#include <charconv>
#include <cstdlib>
#include <cstring>

#include "common.h"

namespace mooncake {
namespace {

size_t railLimitFromEnv() {
    constexpr size_t kDefaultLimit = 4096;
    const char* value = std::getenv("MC_TE_METRIC_RAIL_MAX");
    if (!value) return kDefaultLimit;
    const char* end = value + std::strlen(value);
    size_t limit;
    const auto [ptr, ec] = std::from_chars(value, end, limit);
    if (ec == std::errc() && ptr == end) return limit;
    LOG(WARNING) << "Invalid MC_TE_METRIC_RAIL_MAX; using " << kDefaultLimit;
    return kDefaultLimit;
}

std::string escapeLabel(const std::string& value) {
    std::string escaped;
    for (char ch : value) {
        if (ch == '\\' || ch == '"') escaped += '\\';
        if (ch == '\n') {
            escaped += "\\n";
        } else {
            escaped += ch;
        }
    }
    return escaped;
}

}  // namespace

RailMetricsRegistry* RailMetricsRegistry::instanceIfEnabled() {
    // Endpoint workers and the exporter may be destroyed in either order.
    // Keep the registry and its entries alive until process exit.
    static RailMetricsRegistry* const registry = []() -> RailMetricsRegistry* {
        const char* enabled = std::getenv("MC_TE_METRIC_RAIL");
        return enabled && std::strcmp(enabled, "1") == 0
                   ? new RailMetricsRegistry(railLimitFromEnv())
                   : nullptr;
    }();
    return registry;
}

RailCounters* RailMetricsRegistry::getOrAdd(const std::string& local_nic,
                                            const std::string& peer_nic_path) {
    const auto peer = getServerNameFromNicPath(peer_nic_path);
    const auto peer_nic = getNicNameFromNicPath(peer_nic_path);
    if (peer.empty() || peer_nic.empty()) return nullptr;

    std::lock_guard<std::mutex> lock(mutex_);
    const auto key = std::make_pair(local_nic, peer_nic_path);
    const auto existing = rails_.find(key);
    if (existing != rails_.end()) return &existing->second.counters;
    if (rails_.size() >= max_rails_ ||
        (local_nic == "other" && peer == "other" && peer_nic == "other")) {
        overflow_used_ = true;
        return &overflow_.counters;
    }
    auto labels = "{local_nic=\"" + escapeLabel(local_nic) + "\",peer=\"" +
                  escapeLabel(peer) + "\",peer_nic=\"" + escapeLabel(peer_nic) +
                  "\"}";
    return &rails_.try_emplace(key, std::move(labels)).first->second.counters;
}

void RailMetricsRegistry::appendPrometheus(std::string& out) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto family = [&](const char* name, const char* help,
                            std::atomic<uint64_t> RailCounters::* counter,
                            bool in_microseconds = false) {
        out.append("# HELP ").append(name).append(" ").append(help).append(
            "\n");
        out.append("# TYPE ").append(name).append(" counter\n");
        const auto sample = [&](const Entry& entry) {
            const auto value =
                (entry.counters.*counter).load(std::memory_order_relaxed);
            out.append(name).append(entry.labels).append(" ");
            if (in_microseconds) {
                out.append(std::to_string(value / 1000)).push_back('.');
                out.append(std::to_string(1000 + value % 1000), 1, 3);
            } else {
                out.append(std::to_string(value));
            }
            out.push_back('\n');
        };
        for (const auto& rail : rails_) sample(rail.second);
        if (overflow_used_) sample(overflow_);
    };
    family("mooncake_te_rail_completions_total", "RDMA work requests retired",
           &RailCounters::completions);
    family("mooncake_te_rail_bytes_total",
           "Payload bytes of retired RDMA work requests", &RailCounters::bytes);
    family("mooncake_te_rail_latency_us_sum",
           "Cumulative observed RDMA completion latency in microseconds",
           &RailCounters::latency_ns, true);
    family("mooncake_te_rail_errors_total",
           "RDMA completion errors excluding WR flushes",
           &RailCounters::errors);
}

}  // namespace mooncake
