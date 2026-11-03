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

#ifndef MOONCAKE_TENT_TRANSPORT_UB_QUOTA_H_
#define MOONCAKE_TENT_TRANSPORT_UB_QUOTA_H_

#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "tent/transport/ub/rail_monitor.h"

namespace mooncake::tent::ub {

struct QuotaLimits {
    uint64_t max_inflight_bytes{std::numeric_limits<uint64_t>::max()};
    uint64_t max_outstanding_wrs{std::numeric_limits<uint64_t>::max()};

    bool operator==(const QuotaLimits&) const = default;
};

struct QuotaUsage {
    uint64_t inflight_bytes{0};
    uint64_t outstanding_wrs{0};

    bool operator==(const QuotaUsage&) const = default;
};

struct QuotaStats {
    QuotaLimits limits{};
    QuotaUsage usage{};
    QuotaUsage peak_usage{};
    uint64_t total_acquisitions{0};
    uint64_t total_releases{0};
    uint64_t rejected_acquisitions{0};
};

// A reservation is a copyable release token. QuotaManager retains the
// authoritative path and charge, so changing those descriptive fields cannot
// decrement the wrong counters, and releasing a copied token cannot do so
// twice.
struct QuotaReservation {
    uint64_t id{0};
    UbPostPath path{};
    uint64_t bytes{0};
    uint64_t wrs{0};

    [[nodiscard]] bool valid() const { return id != 0; }
};

struct DeviceQuotaStats : QuotaStats {
    Topology::NicID local_topology_id{-1};
};

struct PathQuotaStats : QuotaStats {
    UbPostPath path{};
};

struct AggregateQuotaStats {
    QuotaUsage usage{};
    QuotaUsage peak_usage{};
    size_t active_reservations{0};
    uint64_t total_acquisitions{0};
    uint64_t total_releases{0};
    uint64_t rejected_acquisitions{0};
    uint64_t duplicate_release_attempts{0};
};

// A lock-consistent capacity snapshot used by path selection. Pressure is the
// projected utilization after charging the requested work and is normalized
// to [0, 1]. Unlimited dimensions contribute no pressure.
struct QuotaAvailability {
    bool can_acquire{false};
    double normalized_inflight{1.0};
    double normalized_outstanding_wrs{1.0};
};

// Atomically enforces both physical-device and posting-path capacity. This is
// an internal sender-side limit and is intentionally independent of TENT's
// receiver-credit protocol.
class QuotaManager {
   public:
    explicit QuotaManager(QuotaLimits default_device_limits = {},
                          QuotaLimits default_path_limits = {});

    QuotaManager(const QuotaManager&) = delete;
    QuotaManager& operator=(const QuotaManager&) = delete;

    void setDefaultDeviceLimits(const QuotaLimits& limits);
    void setDefaultPathLimits(const QuotaLimits& limits);
    [[nodiscard]] QuotaLimits defaultDeviceLimits() const;
    [[nodiscard]] QuotaLimits defaultPathLimits() const;

    bool setDeviceLimits(Topology::NicID local_topology_id,
                         const QuotaLimits& limits);
    bool clearDeviceLimits(Topology::NicID local_topology_id);
    bool setPathLimits(const UbPostPath& path, const QuotaLimits& limits);
    bool clearPathLimits(const UbPostPath& path);

    // Acquires device and path charges as one transaction. A zero-byte work
    // request is supported, but wrs must be nonzero.
    [[nodiscard]] std::optional<QuotaReservation> tryAcquire(
        const UbPostPath& path, uint64_t bytes, uint64_t wrs = 1);

    // Tries paths in caller-provided preference order under one lock. This is
    // the commit point for multi-rail selection: if a preflight snapshot races
    // with another posting worker, later rails are considered before the
    // request is deferred.
    [[nodiscard]] std::optional<QuotaReservation> tryAcquireFirst(
        const std::vector<UbPostPath>& paths, uint64_t bytes, uint64_t wrs = 1);

    // Returns projected device/path pressure without reserving capacity.
    [[nodiscard]] QuotaAvailability availability(const UbPostPath& path,
                                                 uint64_t bytes,
                                                 uint64_t wrs = 1) const;

    // The first release returns true. Releasing the same token again is a
    // harmless no-op and returns false; usage can never underflow.
    bool release(const QuotaReservation& reservation);

    [[nodiscard]] DeviceQuotaStats deviceStats(
        Topology::NicID local_topology_id) const;
    [[nodiscard]] PathQuotaStats pathStats(const UbPostPath& path) const;
    [[nodiscard]] std::vector<DeviceQuotaStats> allDeviceStats() const;
    [[nodiscard]] std::vector<PathQuotaStats> allPathStats() const;
    [[nodiscard]] AggregateQuotaStats aggregateStats() const;
    [[nodiscard]] size_t activeReservationCount() const;

   private:
    struct QuotaRecord {
        std::optional<QuotaLimits> override_limits;
        // Quota is charged to a physical rail, while this preserves the most
        // recent endpoint incarnation for diagnostics.
        UbPostPath latest_path{};
        QuotaUsage usage{};
        QuotaUsage peak_usage{};
        uint64_t total_acquisitions{0};
        uint64_t total_releases{0};
        uint64_t rejected_acquisitions{0};
    };

    struct ActiveReservation {
        UbPostPath path{};
        uint64_t bytes{0};
        uint64_t wrs{0};
    };

    static bool fits(uint64_t current, uint64_t charge, uint64_t limit);
    static double normalizedUsage(uint64_t current, uint64_t charge,
                                  uint64_t limit);
    static uint64_t saturatingAdd(uint64_t lhs, uint64_t rhs);
    static void addUsage(QuotaUsage& usage, uint64_t bytes, uint64_t wrs);
    static void releaseUsage(QuotaUsage& usage, uint64_t bytes, uint64_t wrs);
    static void updatePeak(const QuotaUsage& usage, QuotaUsage& peak);
    static QuotaLimits effectiveLimits(const QuotaRecord& record,
                                       const QuotaLimits& defaults);
    static QuotaStats makeStats(const QuotaRecord& record,
                                const QuotaLimits& defaults);
    std::optional<QuotaReservation> tryAcquireLocked(
        const UbPostPath& path, uint64_t bytes, uint64_t wrs,
        bool count_aggregate_reject);
    QuotaAvailability availabilityLocked(const UbPostPath& path, uint64_t bytes,
                                         uint64_t wrs) const;
    uint64_t nextReservationIdLocked();

    mutable std::mutex mutex_;
    QuotaLimits default_device_limits_;
    QuotaLimits default_path_limits_;
    std::unordered_map<Topology::NicID, QuotaRecord> devices_;
    std::unordered_map<UbRailKey, QuotaRecord, UbRailKeyHash> paths_;
    std::unordered_map<uint64_t, ActiveReservation> active_reservations_;
    AggregateQuotaStats aggregate_stats_{};
    uint64_t next_reservation_id_{1};
};

using UbQuotaManager = QuotaManager;

}  // namespace mooncake::tent::ub

#endif  // MOONCAKE_TENT_TRANSPORT_UB_QUOTA_H_
