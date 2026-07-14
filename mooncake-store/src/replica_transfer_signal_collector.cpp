// Copyright 2026 Mooncake Authors

#include "replica_transfer_signal_collector.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

namespace mooncake {

class ReplicaTransferSignalCollector::Reservation final
    : public TransferSignalReservation {
   public:
    Reservation(std::weak_ptr<ReplicaTransferSignalCollector> owner,
                ReplicaEndpointProtocolKey key, uint64_t bytes,
                bool health_attributable)
        : owner_(std::move(owner)),
          key_(std::move(key)),
          bytes_(bytes),
          health_attributable_(health_attributable) {}

    ~Reservation() override { Finish(std::nullopt); }

    void Complete(ErrorCode result) noexcept override { Finish(result); }

   private:
    void Finish(std::optional<ErrorCode> result) noexcept {
        if (finished_.exchange(true, std::memory_order_acq_rel)) return;
        if (auto owner = owner_.lock()) {
            owner->Finish(key_, bytes_, health_attributable_, result);
        }
    }

    std::weak_ptr<ReplicaTransferSignalCollector> owner_;
    ReplicaEndpointProtocolKey key_;
    uint64_t bytes_;
    bool health_attributable_;
    std::atomic<bool> finished_{false};
};

ReplicaTransferSignalCollector::ReplicaTransferSignalCollector(
    std::shared_ptr<CachedReplicaScoreProvider> provider,
    ReplicaTransferSignalCollectorConfig config)
    : provider_(std::move(provider)), config_(config) {
    if (!provider_ || config_.saturation_bytes == 0 ||
        config_.unhealthy_failure_streak == 0 ||
        !std::isfinite(config_.failure_ewma_alpha) ||
        config_.failure_ewma_alpha <= 0.0 || config_.failure_ewma_alpha > 1.0 ||
        config_.min_publish_interval <
            std::chrono::steady_clock::duration::zero() ||
        config_.min_publish_interval > std::chrono::hours(24) ||
        config_.max_refresh_interval < config_.min_publish_interval ||
        config_.max_refresh_interval > std::chrono::hours(24)) {
        throw std::invalid_argument(
            "invalid replica transfer collector config");
    }
}

std::shared_ptr<TransferSignalReservation>
ReplicaTransferSignalCollector::Reserve(std::string_view endpoint,
                                        std::string_view protocol,
                                        uint64_t bytes,
                                        bool health_attributable) {
    if (endpoint.empty() || bytes == 0) return nullptr;
    ReplicaEndpointProtocolKey key{std::string(endpoint),
                                   std::string(protocol)};
    {
        std::lock_guard lock(mutex_);
        auto& state = endpoints_[key];
        const bool starts_busy_period = state.queued_bytes == 0;
        state.queued_bytes =
            bytes > std::numeric_limits<uint64_t>::max() - state.queued_bytes
                ? std::numeric_limits<uint64_t>::max()
                : state.queued_bytes + bytes;
        dirty_.store(true, std::memory_order_release);
        if (starts_busy_period) {
            const auto status =
                PublishSnapshotLocked(std::chrono::steady_clock::now());
            if (status != ReplicaSignalPublishStatus::PUBLISHED) {
                LOG_EVERY_N(WARNING, 1000)
                    << "Failed to publish busy-period replica signals: "
                    << static_cast<int>(status);
            }
        }
    }
    return std::make_shared<Reservation>(weak_from_this(), std::move(key),
                                         bytes, health_attributable);
}

void ReplicaTransferSignalCollector::ObserveEndpoint(
    std::string_view endpoint, std::string_view protocol) {
    if (endpoint.empty()) return;
    std::lock_guard lock(mutex_);
    const ReplicaEndpointProtocolView view{endpoint, protocol};
    if (!endpoints_.contains(view)) {
        endpoints_.emplace(ReplicaEndpointProtocolKey{std::string(endpoint),
                                                      std::string(protocol)},
                           EndpointState{});
        dirty_.store(true, std::memory_order_release);
    }
}

void ReplicaTransferSignalCollector::Finish(
    const ReplicaEndpointProtocolKey& key, uint64_t bytes,
    bool health_attributable, std::optional<ErrorCode> result) noexcept {
    std::lock_guard lock(mutex_);
    auto it = endpoints_.find(key);
    if (it == endpoints_.end()) return;
    auto& state = it->second;
    state.queued_bytes =
        bytes >= state.queued_bytes ? 0 : state.queued_bytes - bytes;
    dirty_.store(true, std::memory_order_release);
    if (!health_attributable || !result.has_value()) return;

    const double sample = *result == ErrorCode::OK ? 0.0 : 1.0;
    if (state.completed == 0) {
        state.failure_ewma = sample;
    } else {
        state.failure_ewma +=
            config_.failure_ewma_alpha * (sample - state.failure_ewma);
    }
    ++state.completed;
    state.consecutive_failures =
        *result == ErrorCode::OK ? 0 : state.consecutive_failures + 1;
    if (*result != ErrorCode::OK) {
        next_publish_ns_.store(0, std::memory_order_relaxed);
    }
}

ReplicaSignalPublishStatus ReplicaTransferSignalCollector::PublishSnapshot() {
    std::lock_guard lock(mutex_);
    return PublishSnapshotLocked(std::chrono::steady_clock::now());
}

ReplicaSignalPublishStatus
ReplicaTransferSignalCollector::PublishSnapshotIfNeeded() {
    const bool dirty = dirty_.load(std::memory_order_acquire);
    auto now = std::chrono::steady_clock::now();
    auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      now.time_since_epoch())
                      .count();
    auto deadline_ns = dirty ? next_publish_ns_.load(std::memory_order_relaxed)
                             : next_refresh_ns_.load(std::memory_order_relaxed);
    if (now_ns < deadline_ns) {
        if (dirty || !dirty_.load(std::memory_order_acquire)) {
            return ReplicaSignalPublishStatus::PUBLISHED;
        }
        deadline_ns = next_publish_ns_.load(std::memory_order_relaxed);
        if (now_ns < deadline_ns) {
            return ReplicaSignalPublishStatus::PUBLISHED;
        }
    }

    std::lock_guard lock(mutex_);
    now = std::chrono::steady_clock::now();
    now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                 now.time_since_epoch())
                 .count();
    const bool locked_dirty = dirty_.load(std::memory_order_relaxed);
    const auto locked_deadline_ns =
        locked_dirty ? next_publish_ns_.load(std::memory_order_relaxed)
                     : next_refresh_ns_.load(std::memory_order_relaxed);
    if (now_ns < locked_deadline_ns) {
        return ReplicaSignalPublishStatus::PUBLISHED;
    }
    return PublishSnapshotLocked(now);
}

ReplicaSignalPublishStatus
ReplicaTransferSignalCollector::PublishSnapshotLocked(
    std::chrono::steady_clock::time_point now) {
    ReplicaSignalSnapshot snapshot;
    snapshot.generation = provider_->CurrentGeneration() + 1;
    snapshot.load_source = {true, now};
    snapshot.health_source = {true, now};
    snapshot.load.reserve(endpoints_.size());
    snapshot.health.reserve(endpoints_.size());
    for (const auto& [key, state] : endpoints_) {
        snapshot.load.emplace(
            key, ReplicaLoadSignal{
                     state.queued_bytes, config_.saturation_bytes,
                     std::min(1.0, static_cast<double>(state.queued_bytes) /
                                       config_.saturation_bytes)});

        ReplicaHealthState health = ReplicaHealthState::HEALTHY;
        double cost = std::clamp(state.failure_ewma, 0.0, 1.0);
        if (state.consecutive_failures >= config_.unhealthy_failure_streak) {
            health = ReplicaHealthState::UNHEALTHY;
            cost = 1.0;
        } else if (state.consecutive_failures > 0 || cost > 0.0) {
            health = ReplicaHealthState::DEGRADED;
        }
        snapshot.health.emplace(key, ReplicaHealthSignal{health, cost});
    }
    const auto status = provider_->PublishSnapshot(std::move(snapshot));
    const auto interval_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            config_.min_publish_interval)
            .count();
    const auto refresh_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            config_.max_refresh_interval)
            .count();
    const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            now.time_since_epoch())
                            .count();
    if (status == ReplicaSignalPublishStatus::PUBLISHED) {
        next_publish_ns_.store(now_ns + interval_ns, std::memory_order_relaxed);
        next_refresh_ns_.store(now_ns + refresh_ns, std::memory_order_relaxed);
        dirty_.store(false, std::memory_order_release);
    }
    return status;
}

}  // namespace mooncake
