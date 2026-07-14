// Copyright 2026 Mooncake Authors
//
// Experimental cached signal provider for ReplicaSelector V2. Signal
// collection and RealClient integration deliberately live outside this API.

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "replica_selector.h"

namespace mooncake {

class ReplicaSignalClock {
   public:
    using TimePoint = std::chrono::steady_clock::time_point;

    virtual ~ReplicaSignalClock() = default;
    [[nodiscard]] virtual TimePoint Now() const noexcept = 0;
};

struct ReplicaEndpointProtocolView {
    std::string_view transport_endpoint;
    std::string_view protocol;
};

struct ReplicaEndpointProtocolKey {
    std::string transport_endpoint;
    std::string protocol;

    friend bool operator==(const ReplicaEndpointProtocolKey&,
                           const ReplicaEndpointProtocolKey&) = default;
};

struct ReplicaEndpointProtocolKeyHash {
    using is_transparent = void;

    [[nodiscard]] size_t operator()(
        const ReplicaEndpointProtocolKey& key) const noexcept;
    [[nodiscard]] size_t operator()(
        ReplicaEndpointProtocolView key) const noexcept;
};

struct ReplicaEndpointProtocolKeyEqual {
    using is_transparent = void;

    [[nodiscard]] bool operator()(
        const ReplicaEndpointProtocolKey& lhs,
        const ReplicaEndpointProtocolKey& rhs) const noexcept;
    [[nodiscard]] bool operator()(
        const ReplicaEndpointProtocolKey& lhs,
        ReplicaEndpointProtocolView rhs) const noexcept;
    [[nodiscard]] bool operator()(
        ReplicaEndpointProtocolView lhs,
        const ReplicaEndpointProtocolKey& rhs) const noexcept;
};

struct ReplicaTopologyRouteView {
    std::string_view requester_endpoint;
    std::string_view destination_location;
    std::string_view candidate_endpoint;
    std::string_view protocol;
};

struct ReplicaTopologyRouteKey {
    std::string requester_endpoint;
    std::string destination_location;
    std::string candidate_endpoint;
    std::string protocol;

    friend bool operator==(const ReplicaTopologyRouteKey&,
                           const ReplicaTopologyRouteKey&) = default;
};

struct ReplicaTopologyRouteKeyHash {
    using is_transparent = void;

    [[nodiscard]] size_t operator()(
        const ReplicaTopologyRouteKey& key) const noexcept;
    [[nodiscard]] size_t operator()(
        ReplicaTopologyRouteView key) const noexcept;
};

struct ReplicaTopologyRouteKeyEqual {
    using is_transparent = void;

    [[nodiscard]] bool operator()(
        const ReplicaTopologyRouteKey& lhs,
        const ReplicaTopologyRouteKey& rhs) const noexcept;
    [[nodiscard]] bool operator()(const ReplicaTopologyRouteKey& lhs,
                                  ReplicaTopologyRouteView rhs) const noexcept;
    [[nodiscard]] bool operator()(
        ReplicaTopologyRouteView lhs,
        const ReplicaTopologyRouteKey& rhs) const noexcept;
};

// enabled=false is the disabled state: required must be false and weight must
// be zero. enabled=true, required=false is optional: unavailable data receives
// the component's maximum normalized contribution. enabled=true,
// required=true gates the complete batch with a soft fallback. Policies are
// constructor-owned and cannot be changed by snapshot publishers.
struct ReplicaSignalSourcePolicy {
    bool enabled{false};
    bool required{false};
    std::chrono::nanoseconds ttl{0};
    double weight{0.0};
};

struct ReplicaSignalSourceObservation {
    bool ready{false};
    ReplicaSignalClock::TimePoint observed_at{};
};

struct ReplicaLoadSignal {
    uint64_t queued_bytes{0};
    uint64_t saturation_bytes{0};
    double utilization{0.0};
};

enum class ReplicaHealthState : uint8_t {
    HEALTHY = 0,
    DEGRADED,
    UNHEALTHY,
    CIRCUIT_OPEN,
};

struct ReplicaHealthSignal {
    ReplicaHealthState state{ReplicaHealthState::HEALTHY};
    double cost{0.0};
};

using ReplicaTopologySignalMap =
    std::unordered_map<ReplicaTopologyRouteKey, double,
                       ReplicaTopologyRouteKeyHash,
                       ReplicaTopologyRouteKeyEqual>;
using ReplicaLoadSignalMap =
    std::unordered_map<ReplicaEndpointProtocolKey, ReplicaLoadSignal,
                       ReplicaEndpointProtocolKeyHash,
                       ReplicaEndpointProtocolKeyEqual>;
using ReplicaHealthSignalMap =
    std::unordered_map<ReplicaEndpointProtocolKey, ReplicaHealthSignal,
                       ReplicaEndpointProtocolKeyHash,
                       ReplicaEndpointProtocolKeyEqual>;

// A publish operation takes this value by copy/move, validates it completely,
// then release-publishes one immutable shared instance. No string_view or
// caller-owned map storage is retained. unordered_map also makes duplicate
// route or endpoint/protocol entries unrepresentable in a snapshot.
struct ReplicaSignalSnapshot {
    uint64_t generation{0};

    ReplicaSignalSourceObservation topology_source;
    ReplicaSignalSourceObservation load_source;
    ReplicaSignalSourceObservation health_source;

    ReplicaTopologySignalMap topology;
    ReplicaLoadSignalMap load;
    ReplicaHealthSignalMap health;
};

struct ReplicaSignalLimits {
    size_t max_topology_entries{65536};
    size_t max_load_entries{16384};
    size_t max_health_entries{16384};
    size_t max_total_entries{98304};
    size_t max_string_bytes{1024};
    size_t max_candidates_per_batch{4096};
};

struct ReplicaSignalProviderConfig {
    ReplicaSignalSourcePolicy topology_source;
    ReplicaSignalSourcePolicy load_source;
    ReplicaSignalSourcePolicy health_source;
    double degraded_health_cost_floor{0.5};
    ReplicaSignalLimits limits;
};

enum class ReplicaSignalPublishStatus : uint8_t {
    PUBLISHED = 0,
    INVALID_SNAPSHOT,
    LIMIT_EXCEEDED,
    GENERATION_NOT_INCREASING,
    NOT_ENABLED,
};

class CachedReplicaScoreProvider final : public ReplicaScoreProvider {
   public:
    explicit CachedReplicaScoreProvider(
        ReplicaSignalProviderConfig config,
        std::shared_ptr<const ReplicaSignalClock> clock = nullptr);

    // Validation is intentionally performed before the generation check: an
    // invalid update is always reported as invalid and can never replace or
    // otherwise affect the last valid generation.
    [[nodiscard]] ReplicaSignalPublishStatus PublishSnapshot(
        ReplicaSignalSnapshot snapshot);

    [[nodiscard]] uint64_t CurrentGeneration() const noexcept;

    void ScoreAll(const ReplicaSelectionContext& context,
                  std::span<const ReplicaScoreCandidateView> candidates,
                  std::span<ReplicaScoreVerdict> verdicts) const override;

   private:
    std::shared_ptr<const ReplicaSignalClock> clock_;
    const ReplicaSignalProviderConfig config_;
    std::atomic<std::shared_ptr<const ReplicaSignalSnapshot>> snapshot_;
};

}  // namespace mooncake
