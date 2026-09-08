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

#include "tent/transport/ub/quota.h"

#include <algorithm>

namespace mooncake::tent::ub {

QuotaManager::QuotaManager(QuotaLimits default_device_limits,
                           QuotaLimits default_path_limits)
    : default_device_limits_(default_device_limits),
      default_path_limits_(default_path_limits) {}

void QuotaManager::setDefaultDeviceLimits(const QuotaLimits& limits) {
    std::lock_guard<std::mutex> lock(mutex_);
    default_device_limits_ = limits;
}

void QuotaManager::setDefaultPathLimits(const QuotaLimits& limits) {
    std::lock_guard<std::mutex> lock(mutex_);
    default_path_limits_ = limits;
}

QuotaLimits QuotaManager::defaultDeviceLimits() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return default_device_limits_;
}

QuotaLimits QuotaManager::defaultPathLimits() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return default_path_limits_;
}

bool QuotaManager::setDeviceLimits(Topology::NicID local_topology_id,
                                   const QuotaLimits& limits) {
    if (local_topology_id < 0) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    devices_[local_topology_id].override_limits = limits;
    return true;
}

bool QuotaManager::clearDeviceLimits(Topology::NicID local_topology_id) {
    if (local_topology_id < 0) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = devices_.find(local_topology_id);
    if (it == devices_.end() || !it->second.override_limits) return false;
    it->second.override_limits.reset();
    return true;
}

bool QuotaManager::setPathLimits(const UbPostPath& path,
                                 const QuotaLimits& limits) {
    if (!path.valid()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    auto& record = paths_[UbRailKey::fromPath(path)];
    record.latest_path = path;
    record.override_limits = limits;
    return true;
}

bool QuotaManager::clearPathLimits(const UbPostPath& path) {
    if (!path.valid()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = paths_.find(UbRailKey::fromPath(path));
    if (it == paths_.end() || !it->second.override_limits) return false;
    it->second.override_limits.reset();
    return true;
}

std::optional<QuotaReservation> QuotaManager::tryAcquire(const UbPostPath& path,
                                                         uint64_t bytes,
                                                         uint64_t wrs) {
    std::lock_guard<std::mutex> lock(mutex_);
    return tryAcquireLocked(path, bytes, wrs, true);
}

std::optional<QuotaReservation> QuotaManager::tryAcquireFirst(
    const std::vector<UbPostPath>& paths, uint64_t bytes, uint64_t wrs) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& path : paths) {
        auto reservation = tryAcquireLocked(path, bytes, wrs, false);
        if (reservation) return reservation;
    }
    aggregate_stats_.rejected_acquisitions =
        saturatingAdd(aggregate_stats_.rejected_acquisitions, 1);
    return std::nullopt;
}

QuotaAvailability QuotaManager::availability(const UbPostPath& path,
                                             uint64_t bytes,
                                             uint64_t wrs) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return availabilityLocked(path, bytes, wrs);
}

std::optional<QuotaReservation> QuotaManager::tryAcquireLocked(
    const UbPostPath& path, uint64_t bytes, uint64_t wrs,
    bool count_aggregate_reject) {
    if (!path.valid() || wrs == 0) {
        if (count_aggregate_reject) {
            aggregate_stats_.rejected_acquisitions =
                saturatingAdd(aggregate_stats_.rejected_acquisitions, 1);
        }
        return std::nullopt;
    }

    auto& device = devices_[path.local_topology_id];
    auto& rail = paths_[UbRailKey::fromPath(path)];
    if (!rail.latest_path.valid() ||
        path.endpoint_generation > rail.latest_path.endpoint_generation) {
        rail.latest_path = path;
    }
    const auto device_limits = effectiveLimits(device, default_device_limits_);
    const auto path_limits = effectiveLimits(rail, default_path_limits_);
    const bool device_fits = fits(device.usage.inflight_bytes, bytes,
                                  device_limits.max_inflight_bytes) &&
                             fits(device.usage.outstanding_wrs, wrs,
                                  device_limits.max_outstanding_wrs);
    const bool path_fits =
        fits(rail.usage.inflight_bytes, bytes,
             path_limits.max_inflight_bytes) &&
        fits(rail.usage.outstanding_wrs, wrs, path_limits.max_outstanding_wrs);
    if (!device_fits || !path_fits) {
        if (!device_fits) {
            device.rejected_acquisitions =
                saturatingAdd(device.rejected_acquisitions, 1);
        }
        if (!path_fits) {
            rail.rejected_acquisitions =
                saturatingAdd(rail.rejected_acquisitions, 1);
        }
        if (count_aggregate_reject) {
            aggregate_stats_.rejected_acquisitions =
                saturatingAdd(aggregate_stats_.rejected_acquisitions, 1);
        }
        return std::nullopt;
    }

    const uint64_t id = nextReservationIdLocked();
    addUsage(device.usage, bytes, wrs);
    addUsage(rail.usage, bytes, wrs);
    updatePeak(device.usage, device.peak_usage);
    updatePeak(rail.usage, rail.peak_usage);
    device.total_acquisitions = saturatingAdd(device.total_acquisitions, 1);
    rail.total_acquisitions = saturatingAdd(rail.total_acquisitions, 1);

    addUsage(aggregate_stats_.usage, bytes, wrs);
    updatePeak(aggregate_stats_.usage, aggregate_stats_.peak_usage);
    aggregate_stats_.total_acquisitions =
        saturatingAdd(aggregate_stats_.total_acquisitions, 1);
    active_reservations_.emplace(id, ActiveReservation{path, bytes, wrs});
    aggregate_stats_.active_reservations = active_reservations_.size();
    return QuotaReservation{id, path, bytes, wrs};
}

QuotaAvailability QuotaManager::availabilityLocked(const UbPostPath& path,
                                                   uint64_t bytes,
                                                   uint64_t wrs) const {
    if (!path.valid() || wrs == 0) return {};

    const auto device_it = devices_.find(path.local_topology_id);
    const auto path_it = paths_.find(UbRailKey::fromPath(path));
    const QuotaRecord empty;
    const auto& device =
        device_it == devices_.end() ? empty : device_it->second;
    const auto& rail = path_it == paths_.end() ? empty : path_it->second;
    const auto device_limits = effectiveLimits(device, default_device_limits_);
    const auto path_limits = effectiveLimits(rail, default_path_limits_);

    QuotaAvailability result;
    result.can_acquire =
        fits(device.usage.inflight_bytes, bytes,
             device_limits.max_inflight_bytes) &&
        fits(device.usage.outstanding_wrs, wrs,
             device_limits.max_outstanding_wrs) &&
        fits(rail.usage.inflight_bytes, bytes,
             path_limits.max_inflight_bytes) &&
        fits(rail.usage.outstanding_wrs, wrs, path_limits.max_outstanding_wrs);
    result.normalized_inflight =
        std::max(normalizedUsage(device.usage.inflight_bytes, bytes,
                                 device_limits.max_inflight_bytes),
                 normalizedUsage(rail.usage.inflight_bytes, bytes,
                                 path_limits.max_inflight_bytes));
    result.normalized_outstanding_wrs =
        std::max(normalizedUsage(device.usage.outstanding_wrs, wrs,
                                 device_limits.max_outstanding_wrs),
                 normalizedUsage(rail.usage.outstanding_wrs, wrs,
                                 path_limits.max_outstanding_wrs));
    return result;
}

bool QuotaManager::release(const QuotaReservation& reservation) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto active = active_reservations_.find(reservation.id);
    if (reservation.id == 0 || active == active_reservations_.end()) {
        aggregate_stats_.duplicate_release_attempts =
            saturatingAdd(aggregate_stats_.duplicate_release_attempts, 1);
        return false;
    }

    // Always release the manager-owned record. The caller's copy may have
    // been changed or may refer to an earlier copy of this reservation.
    const ActiveReservation charge = active->second;
    active_reservations_.erase(active);

    auto device = devices_.find(charge.path.local_topology_id);
    if (device != devices_.end()) {
        releaseUsage(device->second.usage, charge.bytes, charge.wrs);
        device->second.total_releases =
            saturatingAdd(device->second.total_releases, 1);
    }
    auto path = paths_.find(UbRailKey::fromPath(charge.path));
    if (path != paths_.end()) {
        releaseUsage(path->second.usage, charge.bytes, charge.wrs);
        path->second.total_releases =
            saturatingAdd(path->second.total_releases, 1);
    }

    releaseUsage(aggregate_stats_.usage, charge.bytes, charge.wrs);
    aggregate_stats_.total_releases =
        saturatingAdd(aggregate_stats_.total_releases, 1);
    aggregate_stats_.active_reservations = active_reservations_.size();
    return true;
}

DeviceQuotaStats QuotaManager::deviceStats(
    Topology::NicID local_topology_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    DeviceQuotaStats result;
    result.local_topology_id = local_topology_id;
    auto it = devices_.find(local_topology_id);
    if (it == devices_.end()) {
        static_cast<QuotaStats&>(result).limits = default_device_limits_;
    } else {
        static_cast<QuotaStats&>(result) =
            makeStats(it->second, default_device_limits_);
    }
    return result;
}

PathQuotaStats QuotaManager::pathStats(const UbPostPath& path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    PathQuotaStats result;
    result.path = path;
    auto it = paths_.find(UbRailKey::fromPath(path));
    if (it == paths_.end()) {
        static_cast<QuotaStats&>(result).limits = default_path_limits_;
    } else {
        static_cast<QuotaStats&>(result) =
            makeStats(it->second, default_path_limits_);
    }
    return result;
}

std::vector<DeviceQuotaStats> QuotaManager::allDeviceStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<DeviceQuotaStats> result;
    result.reserve(devices_.size());
    for (const auto& [id, record] : devices_) {
        DeviceQuotaStats stats;
        static_cast<QuotaStats&>(stats) =
            makeStats(record, default_device_limits_);
        stats.local_topology_id = id;
        result.push_back(stats);
    }
    return result;
}

std::vector<PathQuotaStats> QuotaManager::allPathStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PathQuotaStats> result;
    result.reserve(paths_.size());
    for (const auto& [_, record] : paths_) {
        PathQuotaStats stats;
        static_cast<QuotaStats&>(stats) =
            makeStats(record, default_path_limits_);
        stats.path = record.latest_path;
        result.push_back(stats);
    }
    return result;
}

AggregateQuotaStats QuotaManager::aggregateStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return aggregate_stats_;
}

size_t QuotaManager::activeReservationCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return active_reservations_.size();
}

bool QuotaManager::fits(uint64_t current, uint64_t charge, uint64_t limit) {
    return current <= limit && charge <= limit - current;
}

double QuotaManager::normalizedUsage(uint64_t current, uint64_t charge,
                                     uint64_t limit) {
    if (limit == std::numeric_limits<uint64_t>::max()) return 0.0;
    if (limit == 0) return current == 0 && charge == 0 ? 0.0 : 1.0;
    const long double projected = std::min<long double>(
        static_cast<long double>(limit),
        static_cast<long double>(current) + static_cast<long double>(charge));
    return static_cast<double>(projected / static_cast<long double>(limit));
}

uint64_t QuotaManager::saturatingAdd(uint64_t lhs, uint64_t rhs) {
    if (std::numeric_limits<uint64_t>::max() - lhs < rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

void QuotaManager::addUsage(QuotaUsage& usage, uint64_t bytes, uint64_t wrs) {
    usage.inflight_bytes = saturatingAdd(usage.inflight_bytes, bytes);
    usage.outstanding_wrs = saturatingAdd(usage.outstanding_wrs, wrs);
}

void QuotaManager::releaseUsage(QuotaUsage& usage, uint64_t bytes,
                                uint64_t wrs) {
    usage.inflight_bytes =
        bytes >= usage.inflight_bytes ? 0 : usage.inflight_bytes - bytes;
    usage.outstanding_wrs =
        wrs >= usage.outstanding_wrs ? 0 : usage.outstanding_wrs - wrs;
}

void QuotaManager::updatePeak(const QuotaUsage& usage, QuotaUsage& peak) {
    peak.inflight_bytes = std::max(peak.inflight_bytes, usage.inflight_bytes);
    peak.outstanding_wrs =
        std::max(peak.outstanding_wrs, usage.outstanding_wrs);
}

QuotaLimits QuotaManager::effectiveLimits(const QuotaRecord& record,
                                          const QuotaLimits& defaults) {
    return record.override_limits.value_or(defaults);
}

QuotaStats QuotaManager::makeStats(const QuotaRecord& record,
                                   const QuotaLimits& defaults) {
    return QuotaStats{effectiveLimits(record, defaults),
                      record.usage,
                      record.peak_usage,
                      record.total_acquisitions,
                      record.total_releases,
                      record.rejected_acquisitions};
}

uint64_t QuotaManager::nextReservationIdLocked() {
    // IDs are never reused while active, including across uint64_t wrap.
    while (next_reservation_id_ == 0 ||
           active_reservations_.contains(next_reservation_id_)) {
        ++next_reservation_id_;
    }
    const uint64_t result = next_reservation_id_++;
    if (next_reservation_id_ == 0) ++next_reservation_id_;
    return result;
}

}  // namespace mooncake::tent::ub
