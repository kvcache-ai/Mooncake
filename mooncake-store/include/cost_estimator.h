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

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace mooncake {

// Network proximity class between client and segment.
// Lower ordinal = closer = preferred. Used as one term in the linear cost
// function evaluated by CostEstimator.
enum class LinkClass : int32_t {
    LOCAL_HOST = 0,  // segment.te_endpoint host == client_host (requires
                     // non-empty client_host; empty client_host can never
                     // resolve to LOCAL_HOST)
    SAME_ZONE = 1,   // segment is in the same topology zone as the client
                     // (requires non-empty client_zone + populated topology)
    CROSS_ZONE = 2,  // segment is in a different known zone
    UNKNOWN = 3,     // topology not configured / endpoint not parseable /
                     // segment host not registered in topology
};

inline const char* LinkClassToString(LinkClass cls) {
    switch (cls) {
        case LinkClass::LOCAL_HOST:
            return "LOCAL_HOST";
        case LinkClass::SAME_ZONE:
            return "SAME_ZONE";
        case LinkClass::CROSS_ZONE:
            return "CROSS_ZONE";
        case LinkClass::UNKNOWN:
            return "UNKNOWN";
    }
    return "UNKNOWN";
}

// Storage media tier of the segment. Lower ordinal = faster = preferred.
enum class StorageTier : int32_t {
    DRAM = 0,  // memory-resident segment (default for MountSegment)
    SSD = 1,   // local-disk segment
    FILE = 2,  // file-backed segment (slowest hot tier)
};

inline const char* StorageTierToString(StorageTier tier) {
    switch (tier) {
        case StorageTier::DRAM:
            return "DRAM";
        case StorageTier::SSD:
            return "SSD";
        case StorageTier::FILE:
            return "FILE";
    }
    return "DRAM";
}

// Per-deployment cost-aware tunables. The final cost score is
//   cost = link_weight * static_cast<int32_t>(link_class)
//        + tier_weight * static_cast<int32_t>(storage_tier)
//        + inflight_weight * inflight_count
//        + size_weight * (request_size_bytes / kBytesPerMiB)
// All weights are non-negative.
struct CostWeights {
    double link_weight{100.0};
    double tier_weight{50.0};
    double inflight_weight{10.0};
    double size_weight{0.001};  // per-MiB
};

// Cluster topology metadata used for LinkClass classification.
// Loaded once from MasterConfig.cluster_topology_json at startup; immutable
// for the lifetime of the master process.
struct ClusterTopology {
    // zone name -> set of host identifiers in that zone (typically IPs)
    std::unordered_map<std::string, std::unordered_set<std::string>>
        zone_to_hosts;
    // reverse index built once at construction time
    std::unordered_map<std::string, std::string> host_to_zone;

    // Parse the JSON form
    //   {"zone_a": ["10.0.0.1", "10.0.0.2"], "zone_b": ["10.0.1.1"]}
    // Returns false on parse error; an empty input string is accepted and
    // produces an empty (UNKNOWN-only) topology.
    static bool ParseFromJson(const std::string& json_text,
                              ClusterTopology* out);

    // True if the topology has at least one host registered.
    bool empty() const { return host_to_zone.empty(); }
};

// Tracks how many in-flight prefix-aware fetches are currently routed at
// each segment, so the cost function can spread load instead of dog-piling
// on the cheapest replica.
//
// Ownership: one tracker per master_service. Thread-safety: mutating
// Begin/End take a unique_lock only on the rare slow path (new segment seen
// for the first time); steady-state increments and reads are wait-free
// atomic ops on the per-segment counter.
class SegmentInflightTracker {
   public:
    SegmentInflightTracker() = default;
    SegmentInflightTracker(const SegmentInflightTracker&) = delete;
    SegmentInflightTracker& operator=(const SegmentInflightTracker&) = delete;

    // Increment the in-flight counter for `segment_name` by 1, creating the
    // counter on first sight. Returns the new value.
    uint32_t Begin(const std::string& segment_name);

    // Decrement the in-flight counter for `segment_name` by 1; clamped at 0
    // (so spurious / replayed End calls cannot underflow). Returns the new
    // value, or 0 if the segment has no counter yet.
    uint32_t End(const std::string& segment_name);

    // Read the current in-flight value. Returns 0 if not seen before.
    uint32_t Get(const std::string& segment_name) const;

    // Snapshot the entire (segment_name -> inflight) map. Takes the slow
    // lock once. Used by metrics / debug endpoints.
    std::vector<std::pair<std::string, uint32_t>> Snapshot() const;

    // Total of every segment's in-flight counter (gauge for Prometheus).
    uint64_t TotalInflight() const;

    // Reset all counters to zero (test-only).
    void ResetForTesting();

   private:
    mutable std::shared_mutex mutex_;
    // shared_ptr<atomic> so concurrent readers can hold a stable handle even
    // while the map is being rehashed under the unique_lock above.
    std::unordered_map<std::string, std::shared_ptr<std::atomic<uint32_t>>>
        counters_;
};

// Inputs to the linear cost function for a single (client, segment) pair,
// after LinkClass / StorageTier / inflight have been resolved.
struct CostInputs {
    LinkClass link_class{LinkClass::UNKNOWN};
    StorageTier storage_tier{StorageTier::DRAM};
    uint32_t inflight{0};
    uint64_t request_size_bytes{0};
};

// Pure-function cost estimator. Stateless apart from the deployment-wide
// weights; topology lives alongside it on MasterService.
//
// Kept as a class (not a free function) so that future non-linear refinements
// (e.g. piecewise penalties beyond an inflight threshold) can be added
// without touching every call site.
class CostEstimator {
   public:
    explicit CostEstimator(CostWeights weights = CostWeights{})
        : weights_(weights) {}

    // Compute the cost score for `inputs`. Lower is better.
    double Score(const CostInputs& inputs) const;

    // Resolve the LinkClass for a segment given its te_endpoint and the
    // requesting client's host / zone. Returns UNKNOWN if topology is empty
    // or the endpoint cannot be parsed.
    static LinkClass ClassifyLink(const std::string& segment_te_endpoint,
                                  const std::string& client_host,
                                  const std::string& client_zone,
                                  const ClusterTopology& topology);

    // Map a Segment::protocol string to a StorageTier. Defaults to DRAM.
    static StorageTier ClassifyTier(const std::string& segment_protocol);

    const CostWeights& weights() const { return weights_; }

   private:
    CostWeights weights_;
};

}  // namespace mooncake
