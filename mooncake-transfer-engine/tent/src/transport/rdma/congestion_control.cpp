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

#include "tent/transport/rdma/congestion_control.h"

#ifdef MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL

#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <utility>

#include "adaptive_congestion_control_config.h"
#include "rdma_error_classifier.h"
#include "tent/transport/rdma/slice.h"

namespace mooncake::tent {
namespace {

adaptive_congestion_control::Config loadTentCongestionControlConfig() {
    auto result = adaptive_congestion_control::loadConfigFromEnvironment();
    if (result.valid) return result.config;

    static std::once_flag warning_once;
    std::call_once(warning_once, [&] {
        LOG(ERROR) << "Invalid adaptive congestion-control setting: "
                   << result.error << "; TENT RDMA enforcement disabled";
    });
    return adaptive_congestion_control::Config{};
}

uint32_t nextGeneration(
    const adaptive_congestion_control::DomainState& domain) {
    uint32_t next = adaptive_congestion_control::generation(domain) + 1;
    return next == 0 ? 1 : next;
}

}  // namespace

struct TentRdmaCongestionControl::RouteState {
    explicit RouteState(adaptive_congestion_control::Config config)
        : domain(config) {}

    adaptive_congestion_control::DomainState domain;
    std::atomic<RdmaEndPoint*> endpoint{nullptr};
    std::atomic<uint64_t> endpoint_generation{1};
    std::mutex endpoint_mutex;
};

TentRdmaCongestionControl::TentRdmaCongestionControl(size_t device_count)
    : TentRdmaCongestionControl(loadTentCongestionControlConfig(),
                                device_count) {}

TentRdmaCongestionControl::TentRdmaCongestionControl(
    adaptive_congestion_control::Config config, size_t device_count)
    : config_(config), mode_(config.mode), device_count_(device_count) {
    if (!enabled()) return;
    devices_.reserve(device_count_);
    for (size_t i = 0; i < device_count_; ++i) {
        devices_.push_back(
            std::make_unique<adaptive_congestion_control::DomainState>(
                config_));
    }
}

TentRdmaCongestionControl::~TentRdmaCongestionControl() = default;

size_t TentRdmaCongestionControl::RouteKeyHash::operator()(
    const RouteKey& key) const {
    size_t seed = std::hash<std::string>{}(key.peer_machine);
    seed ^= std::hash<int>{}(key.local_device) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    seed ^= std::hash<int>{}(key.remote_device) + 0x9e3779b9 + (seed << 6) +
            (seed >> 2);
    return seed;
}

TentRdmaCongestionControl::RouteState* TentRdmaCongestionControl::route(
    const RouteKey& key) {
    std::lock_guard<std::mutex> lock(routes_mutex_);
    auto& entry = routes_[key];
    if (!entry) entry = std::make_unique<RouteState>(config_);
    return entry.get();
}

TentRdmaCongestionControl::EndpointBinding
TentRdmaCongestionControl::bindEndpoint(RouteState& route,
                                        RdmaEndPoint* endpoint) {
    if (route.endpoint.load(std::memory_order_acquire) == endpoint) {
        const uint64_t endpoint_generation =
            route.endpoint_generation.load(std::memory_order_acquire);
        const uint32_t route_generation =
            adaptive_congestion_control::generation(route.domain);
        // Pair both generations with the endpoint we observed. If a
        // replacement raced this fast path, tryAcquire or complete will fence
        // the old binding.
        if (route.endpoint.load(std::memory_order_acquire) == endpoint &&
            route.endpoint_generation.load(std::memory_order_acquire) ==
                endpoint_generation) {
            return {route_generation, endpoint_generation};
        }
    }
    std::lock_guard<std::mutex> lock(route.endpoint_mutex);
    if (route.endpoint.load(std::memory_order_relaxed) != endpoint) {
        uint64_t endpoint_generation =
            route.endpoint_generation.load(std::memory_order_relaxed) + 1;
        if (endpoint_generation == 0) endpoint_generation = 1;
        route.endpoint_generation.store(endpoint_generation,
                                        std::memory_order_release);
        route.endpoint.store(endpoint, std::memory_order_release);
    }
    return {
        adaptive_congestion_control::generation(route.domain),
        route.endpoint_generation.load(std::memory_order_acquire),
    };
}

size_t TentRdmaCongestionControl::admitPrefix(
    int local_device, const std::string& peer_machine, int remote_device,
    RdmaEndPoint* endpoint, const std::vector<RdmaSlice*>& slices) {
    if (!enabled() || slices.empty()) return slices.size();
    if (local_device < 0 || remote_device < 0 ||
        static_cast<size_t>(local_device) >= device_count_ ||
        endpoint == nullptr) {
        return slices.size();
    }

    RouteState* route_state =
        route({local_device, remote_device, peer_machine});
    const EndpointBinding binding = bindEndpoint(*route_state, endpoint);
    auto& device = *devices_[local_device];

    size_t admitted = 0;
    for (auto* slice : slices) {
        if (slice == nullptr) break;
        auto& permit = slice->rdma_congestion_control_permit;
        if (permit.active() &&
            (!adaptive_congestion_control::isCurrentGeneration(permit) ||
             slice->rdma_congestion_control_route != route_state ||
             slice->rdma_congestion_control_endpoint != endpoint ||
             slice->rdma_congestion_control_endpoint_generation !=
                 binding.endpoint_generation)) {
            releaseUnposted(slice);
        }
        if (permit.active()) {
            ++admitted;
            continue;
        }

        adaptive_congestion_control::PathHandle path{
            &device, &route_state->domain,
            adaptive_congestion_control::generation(device),
            binding.route_generation};
        if (adaptive_congestion_control::tryAcquire(path, slice->length,
                                                    permit) !=
            adaptive_congestion_control::Decision::kAllow) {
            break;
        }
        slice->rdma_congestion_control_route = route_state;
        slice->rdma_congestion_control_endpoint = endpoint;
        slice->rdma_congestion_control_endpoint_generation =
            binding.endpoint_generation;
        ++admitted;
    }
    return admitted;
}

void TentRdmaCongestionControl::complete(RdmaSlice* slice, ibv_wc_status status,
                                         uint32_t vendor_error) {
    if (!slice) return;
    auto& permit = slice->rdma_congestion_control_permit;
    if (!permit.active()) return;

    auto* route =
        static_cast<RouteState*>(slice->rdma_congestion_control_route);
    const bool current_binding =
        adaptive_congestion_control::isCurrentGeneration(permit) &&
        route != nullptr &&
        route->endpoint.load(std::memory_order_acquire) ==
            slice->rdma_congestion_control_endpoint &&
        route->endpoint_generation.load(std::memory_order_acquire) ==
            slice->rdma_congestion_control_endpoint_generation;
    if (!current_binding) {
        adaptive_congestion_control::complete(
            permit, adaptive_congestion_control::OutcomeClass::kDerivedFlush,
            adaptive_congestion_control::FailureScope::kOperation);
    } else {
        const auto classification =
            adaptive_congestion_control::classifyCompletion(status,
                                                            vendor_error);
        adaptive_congestion_control::complete(permit, classification.outcome,
                                              classification.scope);
    }
    slice->rdma_congestion_control_route = nullptr;
    slice->rdma_congestion_control_endpoint = nullptr;
    slice->rdma_congestion_control_endpoint_generation = 0;
}

void TentRdmaCongestionControl::releaseUnposted(RdmaSlice* slice) {
    if (!slice) return;
    adaptive_congestion_control::complete(
        slice->rdma_congestion_control_permit,
        adaptive_congestion_control::OutcomeClass::kDerivedFlush,
        adaptive_congestion_control::FailureScope::kOperation);
    slice->rdma_congestion_control_route = nullptr;
    slice->rdma_congestion_control_endpoint = nullptr;
    slice->rdma_congestion_control_endpoint_generation = 0;
}

void TentRdmaCongestionControl::tick(uint64_t now_ns) {
    if (!enabled()) return;
    for (auto& device : devices_) {
        adaptive_congestion_control::controlTick(*device, now_ns);
    }
    std::lock_guard<std::mutex> lock(routes_mutex_);
    for (auto& [_, route] : routes_) {
        adaptive_congestion_control::controlTick(route->domain, now_ns);
    }
}

void TentRdmaCongestionControl::resetDevice(int local_device) {
    if (!enabled() || local_device < 0 ||
        static_cast<size_t>(local_device) >= device_count_) {
        return;
    }
    auto& device = *devices_[local_device];
    adaptive_congestion_control::resetGeneration(device,
                                                 nextGeneration(device));
}

}  // namespace mooncake::tent

#endif  // MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL
