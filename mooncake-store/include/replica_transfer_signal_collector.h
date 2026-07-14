// Copyright 2026 Mooncake Authors

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "cached_replica_score_provider.h"
#include "transfer_signal.h"

namespace mooncake {

struct ReplicaTransferSignalCollectorConfig {
    uint64_t saturation_bytes{64ULL * 1024 * 1024};
    uint32_t unhealthy_failure_streak{3};
    double failure_ewma_alpha{0.25};
    std::chrono::steady_clock::duration min_publish_interval{
        std::chrono::milliseconds(1)};
    std::chrono::steady_clock::duration max_refresh_interval{
        std::chrono::seconds(1)};
};

class ReplicaTransferSignalCollector final
    : public TransferSignalObserver,
      public std::enable_shared_from_this<ReplicaTransferSignalCollector> {
   public:
    explicit ReplicaTransferSignalCollector(
        std::shared_ptr<CachedReplicaScoreProvider> provider,
        ReplicaTransferSignalCollectorConfig config = {});

    [[nodiscard]] std::shared_ptr<TransferSignalReservation> Reserve(
        std::string_view endpoint, std::string_view protocol, uint64_t bytes,
        bool health_attributable) override;

    // Registers a scoreable candidate even when this requester has not yet
    // transferred from it. Such an endpoint starts idle and healthy.
    void ObserveEndpoint(std::string_view endpoint, std::string_view protocol);

    // Publishes one immutable point-in-time view immediately before scoring.
    // topology_source intentionally remains not-ready until a real destination
    // location signal is wired.
    [[nodiscard]] ReplicaSignalPublishStatus PublishSnapshot();

    // Read-path variant: skips publication when state is unchanged and
    // coalesces bursts within min_publish_interval. A skipped call returns
    // PUBLISHED because the currently published snapshot is still valid.
    [[nodiscard]] ReplicaSignalPublishStatus PublishSnapshotIfNeeded();

   private:
    struct EndpointState {
        uint64_t queued_bytes{0};
        uint64_t completed{0};
        uint32_t consecutive_failures{0};
        double failure_ewma{0.0};
    };

    class Reservation;
    void Finish(const ReplicaEndpointProtocolKey& key, uint64_t bytes,
                bool health_attributable,
                std::optional<ErrorCode> result) noexcept;
    ReplicaSignalPublishStatus PublishSnapshotLocked(
        std::chrono::steady_clock::time_point now);

    std::shared_ptr<CachedReplicaScoreProvider> provider_;
    const ReplicaTransferSignalCollectorConfig config_;
    std::mutex mutex_;
    std::unordered_map<ReplicaEndpointProtocolKey, EndpointState,
                       ReplicaEndpointProtocolKeyHash,
                       ReplicaEndpointProtocolKeyEqual>
        endpoints_;
    std::atomic<bool> dirty_{false};
    std::atomic<int64_t> next_publish_ns_{0};
    std::atomic<int64_t> next_refresh_ns_{0};
};

}  // namespace mooncake
