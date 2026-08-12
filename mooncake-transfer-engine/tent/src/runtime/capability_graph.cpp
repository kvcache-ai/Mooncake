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

#include "tent/runtime/capability_graph.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <optional>
#include <sstream>

namespace mooncake {
namespace tent {
namespace {

double defaultTransportCost(TransportType type) {
    switch (type) {
        case SHM:
        case NVLINK:
        case TPU:
        case MNNVL:
            return 0.10;
        case RDMA:
        case UB:
        case SUNRISE_LINK:
        case AscendDirect:
            return 1.00;
        case TCP:
            return 10.00;
        default:
            return 100.00;
    }
}

double transportCost(TransportType type, const PathSynthesisOptions& options) {
    const auto idx = static_cast<int>(type);
    if (idx >= 0 && idx < kSupportedTransportTypes &&
        options.transport_cost[idx] < std::numeric_limits<double>::infinity()) {
        return options.transport_cost[idx];
    }
    return defaultTransportCost(type);
}

uint64_t steadyNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

void addTransportCostDelta(PathSynthesisOptions& options, TransportType type,
                           double delta) {
    const auto idx = static_cast<int>(type);
    if (idx < 0 || idx >= kSupportedTransportTypes) return;
    const double base =
        options.transport_cost[idx] < std::numeric_limits<double>::infinity()
            ? options.transport_cost[idx]
            : defaultTransportCost(type);
    options.transport_cost[idx] = std::max(0.01, base + delta);
}

struct Hop {
    TransportType transport{UNSPEC};
    double cost{0.0};
};

std::vector<Hop> listTransports(
    const std::array<TransportCapability, kSupportedTransportTypes>& transports,
    MemoryType from, MemoryType to, bool cross_node,
    const PathSynthesisOptions& options) {
    std::vector<Hop> result;
    if (!isKnownMemoryType(from) || !isKnownMemoryType(to)) return result;

    for (int i = 0; i < kSupportedTransportTypes; ++i) {
        const auto type = static_cast<TransportType>(i);
        const auto& info = transports[i];
        if (cross_node) {
            if (!info.caps.cross_node_transfer) continue;
        } else {
            if (!info.caps.local_stage_executor) continue;
        }

        if (!info.enabled) continue;
        if (!CapabilityPathSynthesizer::canReach(info.caps, from, to))
            continue;

        result.push_back(Hop{type, transportCost(type, options)});
    }
    return result;
}

struct Candidate {
    bool direct{false};
    TransportType local_transport{UNSPEC};
    TransportType cross_transport{UNSPEC};
    TransportType remote_transport{UNSPEC};
    StageCandidate local_stage{};
    StageCandidate remote_stage{};
    bool has_local_stage{false};
    bool has_remote_stage{false};
    double cost{std::numeric_limits<double>::infinity()};
};

std::string describeCandidate(const CapabilityGraphInput& input,
                              const Candidate& c) {
    std::ostringstream os;
    if (c.direct) {
        os << "direct " << transportTypeName(c.cross_transport) << " "
           << memoryTypeName(input.local_memory_type) << "->"
           << memoryTypeName(input.remote_memory_type);
        return os.str();
    }

    os << "staged";
    if (c.has_local_stage) {
        os << " local=" << c.local_stage.location << " via "
           << transportTypeName(c.local_transport);
    }
    os << " cross=" << transportTypeName(c.cross_transport);
    if (c.has_remote_stage) {
        os << " remote=" << c.remote_stage.location << " via "
           << transportTypeName(c.remote_transport);
    }
    return os.str();
}

CapabilityPathCandidate makePublicCandidate(
    const CapabilityGraphInput& input, const Candidate& c,
    const PathSynthesisOptions& options) {
    CapabilityPathCandidate path;
    path.direct = c.direct;
    path.local_transport = c.local_transport;
    path.cross_transport = c.cross_transport;
    path.remote_transport = c.remote_transport;
    path.local_stage_location =
        c.has_local_stage ? c.local_stage.location : "";
    path.remote_stage_location =
        c.has_remote_stage ? c.remote_stage.location : "";
    path.estimated_cost = c.cost;
    path.reason = describeCandidate(input, c);

    const auto add_vertex = [&](CapabilityVertex::Side side,
                                CapabilityVertex::Kind kind,
                                const std::string& location,
                                MemoryType memory_type) {
        path.vertices.push_back(
            CapabilityVertex{side, kind, location, memory_type});
        return path.vertices.size() - 1;
    };

    size_t current =
        add_vertex(CapabilityVertex::Side::Local,
                   CapabilityVertex::Kind::Source, input.local_location,
                   input.local_memory_type);

    if (c.has_local_stage) {
        const size_t next =
            add_vertex(CapabilityVertex::Side::Local,
                       CapabilityVertex::Kind::Stage, c.local_stage.location,
                       c.local_stage.memory_type);
        path.edges.push_back(
            CapabilityEdge{current, next, c.local_transport,
                           transportCost(c.local_transport, options)});
        current = next;
    }

    size_t remote_current = 0;
    if (c.has_remote_stage) {
        remote_current =
            add_vertex(CapabilityVertex::Side::Remote,
                       CapabilityVertex::Kind::Stage, c.remote_stage.location,
                       c.remote_stage.memory_type);
    } else {
        remote_current =
            add_vertex(CapabilityVertex::Side::Remote,
                       CapabilityVertex::Kind::Target, input.remote_location,
                       input.remote_memory_type);
    }
    path.edges.push_back(CapabilityEdge{current, remote_current,
                                        c.cross_transport,
                                        transportCost(c.cross_transport,
                                                      options)});

    if (c.has_remote_stage) {
        const size_t target =
            add_vertex(CapabilityVertex::Side::Remote,
                       CapabilityVertex::Kind::Target, input.remote_location,
                       input.remote_memory_type);
        path.edges.push_back(
            CapabilityEdge{remote_current, target, c.remote_transport,
                           transportCost(c.remote_transport, options)});
    }
    return path;
}

}  // namespace

PathSynthesisOptions::PathSynthesisOptions() {
    transport_cost.fill(std::numeric_limits<double>::infinity());
}

PathSynthesisOptions buildPathSynthesisOptions(
    const Request& request, const PathScoringState& state) {
    PathSynthesisOptions options;

    switch (request.intent_type) {
        case IntentType::FOREGROUND_GET:
            // Latency-sensitive foreground traffic pays heavily for extra hops.
            options.staged_path_penalty += 1.50;
            options.stage_hop_penalty += 0.35;
            addTransportCostDelta(options, TCP, 1.00);
            break;
        case IntentType::BACKGROUND_PREFETCH:
            // Background prefetch should make progress while avoiding the fast
            // RDMA lane when a slower direct fallback exists.
            options.staged_path_penalty = 0.05;
            options.stage_hop_penalty = 0.05;
            addTransportCostDelta(options, RDMA, 7.00);
            addTransportCostDelta(options, UB, 7.00);
            addTransportCostDelta(options, SUNRISE_LINK, 7.00);
            addTransportCostDelta(options, AscendDirect, 7.00);
            addTransportCostDelta(options, TCP, -3.00);
            break;
        case IntentType::MIGRATION:
        case IntentType::CHECKPOINT:
        case IntentType::WEIGHT_LOADING:
            // Bulk movement prefers high-bandwidth fabrics and accepts staging
            // when it unlocks a faster cross-node hop.
            options.staged_path_penalty = 0.05;
            options.stage_hop_penalty = 0.05;
            addTransportCostDelta(options, RDMA, -0.20);
            addTransportCostDelta(options, MNNVL, -0.05);
            addTransportCostDelta(options, TCP, 3.00);
            break;
        case IntentType::STAGING_INTERNAL:
            // Internal stage copies should not recursively synthesize another
            // staged path unless there is no direct route left.
            options.staged_path_penalty += 100.0;
            options.stage_hop_penalty += 10.0;
            break;
        case IntentType::INTENT_UNSPEC:
            break;
    }

    const double pressure = std::max(0.0, state.runtime_queue_pressure);
    if (pressure > 0.0) {
        options.direct_path_penalty += std::min(1.0, pressure * 0.10);
        options.staged_path_penalty += std::min(2.0, pressure * 0.50);
    }

    if (state.rdma_available) {
        if (state.rdma_ewma_bandwidth_bps > 0.0) {
            const double queued_seconds =
                static_cast<double>(state.rdma_inflight_bytes + request.length) /
                state.rdma_ewma_bandwidth_bps;
            addTransportCostDelta(options, RDMA,
                                  std::min(20.0, queued_seconds * 100.0));
        } else {
            addTransportCostDelta(options, RDMA, 2.0);
        }

        if (state.rdma_inflight_bytes > 0 && request.length > 0) {
            const double ratio =
                static_cast<double>(state.rdma_inflight_bytes) /
                static_cast<double>(request.length);
            addTransportCostDelta(options, RDMA,
                                  std::min(5.0, std::log1p(ratio) * 0.50));
        }
    }

    if (request.deadline_ns != 0) {
        const uint64_t now = state.now_ns != 0 ? state.now_ns : steadyNowNs();
        if (request.deadline_ns <= now) {
            options.staged_path_penalty += 20.0;
            addTransportCostDelta(options, TCP, 20.0);
        } else {
            const double slack_seconds =
                static_cast<double>(request.deadline_ns - now) / 1e9;
            double rdma_seconds = 0.0;
            if (state.rdma_ewma_bandwidth_bps > 0.0) {
                rdma_seconds = static_cast<double>(request.length) /
                               state.rdma_ewma_bandwidth_bps;
            }
            if (slack_seconds < 0.005 ||
                (rdma_seconds > 0.0 && rdma_seconds * 2.0 > slack_seconds)) {
                options.staged_path_penalty += 3.0;
                options.stage_hop_penalty += 0.50;
                addTransportCostDelta(options, TCP, 5.0);
            }
        }
    }

    return options;
}

bool CapabilityPathSynthesizer::canReach(const Capabilities& caps,
                                         MemoryType local,
                                         MemoryType remote) {
    if (local == MTYPE_CPU && remote == MTYPE_CPU) return caps.dram_to_dram;
    if (isDeviceMemoryType(local) && isDeviceMemoryType(remote))
        return caps.gpu_to_gpu;
    if (local == MTYPE_CPU && isDeviceMemoryType(remote))
        return caps.dram_to_gpu;
    if (isDeviceMemoryType(local) && remote == MTYPE_CPU)
        return caps.gpu_to_dram;
    return false;
}

SynthesizedPath CapabilityPathSynthesizer::synthesize(
    const CapabilityGraphInput& input) {
    return synthesize(input, PathSynthesisOptions{});
}

SynthesizedPath CapabilityPathSynthesizer::synthesize(
    const CapabilityGraphInput& input, const PathSynthesisOptions& options) {
    SynthesizedPath result;
    result.server_addr = input.server_addr;

    if (!isKnownMemoryType(input.local_memory_type) ||
        !isKnownMemoryType(input.remote_memory_type)) {
        result.reason = "unknown source or target memory type";
        return result;
    }

    std::vector<Candidate> candidates;

    for (const auto& cross :
         listTransports(input.transports, input.local_memory_type,
                        input.remote_memory_type, true, options)) {
        Candidate c;
        c.direct = true;
        c.cross_transport = cross.transport;
        c.cost = cross.cost + options.direct_path_penalty;
        candidates.push_back(c);
    }

    std::vector<std::optional<StageCandidate>> local_options{std::nullopt};
    for (const auto& stage : input.local_stage_candidates) {
        if (!stage.location.empty() && isKnownMemoryType(stage.memory_type))
            local_options.push_back(stage);
    }

    std::vector<std::optional<StageCandidate>> remote_options{std::nullopt};
    for (const auto& stage : input.remote_stage_candidates) {
        if (!stage.location.empty() && isKnownMemoryType(stage.memory_type))
            remote_options.push_back(stage);
    }

    for (const auto& local_stage : local_options) {
        for (const auto& remote_stage : remote_options) {
            if (!local_stage && !remote_stage) continue;

            const MemoryType cross_from =
                local_stage ? local_stage->memory_type : input.local_memory_type;
            const MemoryType cross_to = remote_stage
                                            ? remote_stage->memory_type
                                            : input.remote_memory_type;

            auto cross_hops = listTransports(input.transports, cross_from,
                                             cross_to, true, options);
            if (cross_hops.empty()) continue;

            std::vector<Hop> local_hops;
            if (local_stage) {
                local_hops =
                    listTransports(input.transports, input.local_memory_type,
                                   local_stage->memory_type, false, options);
                if (local_hops.empty()) continue;
            } else {
                local_hops.push_back(Hop{UNSPEC, 0.0});
            }

            std::vector<Hop> remote_hops;
            if (remote_stage) {
                remote_hops =
                    listTransports(input.transports, remote_stage->memory_type,
                                   input.remote_memory_type, false, options);
                if (remote_hops.empty()) continue;
            } else {
                remote_hops.push_back(Hop{UNSPEC, 0.0});
            }

            for (const auto& cross : cross_hops) {
                for (const auto& local : local_hops) {
                    for (const auto& remote : remote_hops) {
                        Candidate c;
                        c.direct = false;
                        c.cross_transport = cross.transport;
                        c.cost = cross.cost + options.staged_path_penalty;

                        if (local_stage) {
                            c.has_local_stage = true;
                            c.local_stage = *local_stage;
                            c.local_transport = local.transport;
                            c.cost += local.cost + options.stage_hop_penalty;
                        }

                        if (remote_stage) {
                            c.has_remote_stage = true;
                            c.remote_stage = *remote_stage;
                            c.remote_transport = remote.transport;
                            c.cost += remote.cost + options.stage_hop_penalty;
                        }

                        candidates.push_back(c);
                    }
                }
            }
        }
    }

    if (candidates.empty()) {
        result.reason = "no feasible direct or staged path";
        return result;
    }

    auto public_candidates = std::vector<CapabilityPathCandidate>{};
    public_candidates.reserve(candidates.size());
    for (const auto& candidate : candidates) {
        public_candidates.push_back(
            makePublicCandidate(input, candidate, options));
    }

    const auto best =
        std::min_element(public_candidates.begin(), public_candidates.end(),
                         [](const CapabilityPathCandidate& a,
                            const CapabilityPathCandidate& b) {
                             if (a.estimated_cost != b.estimated_cost)
                                 return a.estimated_cost < b.estimated_cost;
                             return a.direct && !b.direct;
                         });
    const size_t selected_index =
        static_cast<size_t>(std::distance(public_candidates.begin(), best));

    result.found = true;
    result.direct = best->direct;
    result.cross_transport = best->cross_transport;
    result.local_stage_location = best->local_stage_location;
    result.remote_stage_location = best->remote_stage_location;
    result.estimated_cost = best->estimated_cost;
    result.reason = best->reason;
    result.vertices = best->vertices;
    result.edges = best->edges;
    result.selected_candidate_index = selected_index;
    result.candidates = std::move(public_candidates);
    return result;
}

}  // namespace tent
}  // namespace mooncake
