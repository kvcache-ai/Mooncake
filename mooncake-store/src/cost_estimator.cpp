// Copyright 2025 Mooncake Authors
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

#include "cost_estimator.h"

#include <jsoncpp/json/json.h>

#include <mutex>
#include <algorithm>
#include <cstring>
#include <numeric>
#include <sstream>

namespace mooncake {

namespace {

constexpr double kBytesPerMiB = 1024.0 * 1024.0;

// Strip the ":port" suffix from "host:port" / "[ipv6]:port"; if the input
// has no port it is returned verbatim. Empty string in -> empty string out.
std::string ExtractHost(const std::string& endpoint) {
    if (endpoint.empty()) {
        return {};
    }
    // IPv6 form: "[2001:db8::1]:9000"
    if (endpoint.front() == '[') {
        auto rb = endpoint.find(']');
        if (rb == std::string::npos) {
            return endpoint;
        }
        return endpoint.substr(1, rb - 1);
    }
    auto colon = endpoint.rfind(':');
    if (colon == std::string::npos) {
        return endpoint;
    }
    // Disambiguate: a single colon in an IPv4 endpoint is a port separator;
    // multiple colons + no brackets means raw IPv6, treat the whole thing as
    // host (operators that want disambiguation should bracket it).
    auto first_colon = endpoint.find(':');
    if (first_colon != colon) {
        return endpoint;
    }
    return endpoint.substr(0, colon);
}

}  // namespace

bool ClusterTopology::ParseFromJson(const std::string& json_text,
                                    ClusterTopology* out) {
    if (out == nullptr) {
        return false;
    }
    if (json_text.empty()) {
        // empty input => empty topology, success
        out->zone_to_hosts.clear();
        out->host_to_zone.clear();
        return true;
    }
    Json::CharReaderBuilder builder;
    Json::Value root;
    std::string errs;
    std::istringstream is(json_text);
    if (!Json::parseFromStream(builder, is, &root, &errs)) {
        // Parse failed: leave *out untouched so callers may keep using a
        // previously valid topology.
        return false;
    }
    if (!root.isObject()) {
        return false;
    }
    // Build into a temporary; commit only on full success so a malformed
    // zone in the middle of the JSON cannot leave *out half-populated.
    ClusterTopology tmp;
    for (const auto& zone_name : root.getMemberNames()) {
        const Json::Value& hosts = root[zone_name];
        if (!hosts.isArray()) {
            return false;
        }
        auto& bucket = tmp.zone_to_hosts[zone_name];
        for (const auto& h : hosts) {
            if (!h.isString()) {
                return false;
            }
            const std::string host = h.asString();
            bucket.insert(host);
            // First definition wins on duplicate hosts across zones.
            tmp.host_to_zone.emplace(host, zone_name);
        }
    }
    out->zone_to_hosts = std::move(tmp.zone_to_hosts);
    out->host_to_zone = std::move(tmp.host_to_zone);
    return true;
}

uint32_t SegmentInflightTracker::Begin(const std::string& segment_name) {
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = counters_.find(segment_name);
        if (it != counters_.end()) {
            return it->second->fetch_add(1, std::memory_order_acq_rel) + 1;
        }
    }
    // Slow path: counter does not exist yet.
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = counters_.find(segment_name);
    if (it == counters_.end()) {
        auto inserted = counters_.emplace(
            segment_name, std::make_shared<std::atomic<uint32_t>>(0));
        it = inserted.first;
    }
    return it->second->fetch_add(1, std::memory_order_acq_rel) + 1;
}

uint32_t SegmentInflightTracker::End(const std::string& segment_name) {
    std::shared_ptr<std::atomic<uint32_t>> counter;
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = counters_.find(segment_name);
        if (it == counters_.end()) {
            return 0;
        }
        counter = it->second;
    }
    // CAS loop to clamp at zero on spurious / replayed End.
    uint32_t cur = counter->load(std::memory_order_acquire);
    while (cur > 0) {
        if (counter->compare_exchange_weak(cur, cur - 1,
                                           std::memory_order_acq_rel,
                                           std::memory_order_acquire)) {
            return cur - 1;
        }
    }
    return 0;
}

uint32_t SegmentInflightTracker::Get(const std::string& segment_name) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = counters_.find(segment_name);
    if (it == counters_.end()) {
        return 0;
    }
    return it->second->load(std::memory_order_acquire);
}

std::vector<std::pair<std::string, uint32_t>> SegmentInflightTracker::Snapshot()
    const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<std::pair<std::string, uint32_t>> out;
    out.reserve(counters_.size());
    for (const auto& kv : counters_) {
        out.emplace_back(kv.first, kv.second->load(std::memory_order_acquire));
    }
    return out;
}

uint64_t SegmentInflightTracker::TotalInflight() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    uint64_t total = 0;
    for (const auto& kv : counters_) {
        total += kv.second->load(std::memory_order_acquire);
    }
    return total;
}

void SegmentInflightTracker::ResetForTesting() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    counters_.clear();
}

double CostEstimator::Score(const CostInputs& inputs) const {
    const double link_term =
        weights_.link_weight *
        static_cast<double>(static_cast<int32_t>(inputs.link_class));
    const double tier_term =
        weights_.tier_weight *
        static_cast<double>(static_cast<int32_t>(inputs.storage_tier));
    const double inflight_term =
        weights_.inflight_weight * static_cast<double>(inputs.inflight);
    const double size_term =
        weights_.size_weight *
        (static_cast<double>(inputs.request_size_bytes) / kBytesPerMiB);
    return link_term + tier_term + inflight_term + size_term;
}

LinkClass CostEstimator::ClassifyLink(const std::string& segment_te_endpoint,
                                      const std::string& client_host,
                                      const std::string& client_zone,
                                      const ClusterTopology& topology) {
    const std::string seg_host = ExtractHost(segment_te_endpoint);
    if (seg_host.empty()) {
        return LinkClass::UNKNOWN;
    }
    if (!client_host.empty() && seg_host == client_host) {
        return LinkClass::LOCAL_HOST;
    }
    if (topology.empty()) {
        return LinkClass::UNKNOWN;
    }
    auto it = topology.host_to_zone.find(seg_host);
    if (it == topology.host_to_zone.end()) {
        return LinkClass::UNKNOWN;
    }
    if (!client_zone.empty() && it->second == client_zone) {
        return LinkClass::SAME_ZONE;
    }
    return LinkClass::CROSS_ZONE;
}

StorageTier CostEstimator::ClassifyTier(const std::string& segment_protocol) {
    if (segment_protocol == "local_disk" || segment_protocol == "ssd" ||
        segment_protocol == "nvme") {
        return StorageTier::SSD;
    }
    if (segment_protocol == "file" || segment_protocol == "3fs") {
        return StorageTier::FILE;
    }
    return StorageTier::DRAM;
}

}  // namespace mooncake
