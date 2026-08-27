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

#ifndef TENT_RUNTIME_CAPABILITY_GRAPH_H
#define TENT_RUNTIME_CAPABILITY_GRAPH_H

#include <array>
#include <limits>
#include <string>
#include <vector>

#include "tent/common/types.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

struct StageCandidate {
    std::string location;
    MemoryType memory_type{MTYPE_UNKNOWN};
};

struct TransportCapability {
    bool enabled{false};
    Capabilities caps{};
};

struct CapabilityGraphInput {
    MemoryType local_memory_type{MTYPE_UNKNOWN};
    MemoryType remote_memory_type{MTYPE_UNKNOWN};
    std::string local_location;
    std::string remote_location;
    std::string server_addr;
    std::vector<StageCandidate> local_stage_candidates;
    std::vector<StageCandidate> remote_stage_candidates;
    std::array<TransportCapability, kSupportedTransportTypes> transports{};
};

struct PathSynthesisOptions {
    PathSynthesisOptions();

    // Lower is better. Entries left as infinity use the synthesizer's nominal
    // default for that transport. Callers can override or derive these costs
    // from an intent profile, calibrated bandwidth/latency, or live runtime
    // state.
    std::array<double, kSupportedTransportTypes> transport_cost;

    // Path-shape penalties are additive. They let higher-level policy express
    // preferences such as "avoid extra staging for latency-sensitive traffic"
    // or "prefer high-bandwidth staged paths for throughput traffic".
    double direct_path_penalty{0.0};
    double staged_path_penalty{0.20};
    double stage_hop_penalty{0.20};
};

struct PathScoringState {
    // Generic dispatch pressure in [0, +inf). 0 means idle; 1 means the
    // configured runtime dispatch window is full.
    double runtime_queue_pressure{0.0};

    // RDMA load snapshot, if available. These are aggregated across active
    // rails and let the cost model account for rail inflight and EWMA bandwidth
    // without exposing RDMA internals to the graph.
    bool rdma_available{false};
    uint64_t rdma_inflight_bytes{0};
    double rdma_ewma_bandwidth_bps{0.0};

    // steady_clock nanoseconds used to compute deadline slack. 0 means the
    // builder should sample steady_clock itself.
    uint64_t now_ns{0};
};

PathSynthesisOptions buildPathSynthesisOptions(
    const Request& request, const PathScoringState& state = {});

struct CapabilityVertex {
    enum class Side { Local, Remote };
    enum class Kind { Source, Target, Stage };

    Side side{Side::Local};
    Kind kind{Kind::Source};
    std::string location;
    MemoryType memory_type{MTYPE_UNKNOWN};
};

struct CapabilityEdge {
    size_t from{0};
    size_t to{0};
    TransportType transport{UNSPEC};
    double estimated_cost{0.0};
};

struct CapabilityPathCandidate {
    bool direct{false};
    TransportType local_transport{UNSPEC};
    TransportType cross_transport{UNSPEC};
    TransportType remote_transport{UNSPEC};
    std::string local_stage_location;
    std::string remote_stage_location;
    double estimated_cost{0.0};
    std::string reason;
    std::vector<CapabilityVertex> vertices;
    std::vector<CapabilityEdge> edges;
};

struct SynthesizedPath {
    bool found{false};
    bool direct{false};
    TransportType cross_transport{UNSPEC};
    std::string server_addr;
    std::string local_stage_location;
    std::string remote_stage_location;
    double estimated_cost{0.0};
    std::string reason;
    std::vector<CapabilityVertex> vertices;
    std::vector<CapabilityEdge> edges;
    size_t selected_candidate_index{std::numeric_limits<size_t>::max()};
    std::vector<CapabilityPathCandidate> candidates;
};

class CapabilityPathSynthesizer {
   public:
    static SynthesizedPath synthesize(const CapabilityGraphInput& input);
    static SynthesizedPath synthesize(const CapabilityGraphInput& input,
                                      const PathSynthesisOptions& options);

    static bool canReach(const Capabilities& caps, MemoryType local,
                         MemoryType remote);
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_CAPABILITY_GRAPH_H
