#pragma once

#include <chrono>
#include <optional>
#include <vector>

#include "types.h"

namespace mooncake {

/**
 * @brief Metadata for objects stored in the system
 */
struct ObjectMetadata {
    std::vector<Replica> replicas;
    size_t size;
    // Default constructor, creates a time_point representing
    // the Clock's epoch (i.e., time_since_epoch() is zero).
    std::chrono::steady_clock::time_point lease_timeout;

    // Check if there is some replica with a different status than the given
    // value. If there is, return the status of the first replica that is
    // not equal to the given value. Otherwise, return false.
    [[nodiscard]] std::optional<ReplicaStatus> HasDiffRepStatus(
        ReplicaStatus status) const {
        for (const auto& replica : replicas) {
            if (replica.status() != status) {
                return replica.status();
            }
        }
        return {};
    }

    // Grant a lease with timeout as now() + ttl, only update if the new
    // timeout is larger
    void GrantLease(const uint64_t ttl) {
        lease_timeout =
            std::max(lease_timeout, std::chrono::steady_clock::now() +
                                        std::chrono::milliseconds(ttl));
    }

    // Check if the lease has expired
    [[nodiscard]] bool IsLeaseExpired() const {
        return std::chrono::steady_clock::now() >= lease_timeout;
    }

    // Check if the lease has expired
    [[nodiscard]] bool IsLeaseExpired(
        std::chrono::steady_clock::time_point& now) const {
        return now >= lease_timeout;
    }

    // Returns the segment name of the first buffer descriptor in the first
    // replica Returns std::nullopt if no valid segment name is available
    [[nodiscard]] std::optional<std::string> get_primary_segment_name() const {
        if (replicas.empty() ||
            replicas[0].get_descriptor().buffer_descriptors.empty()) {
            return std::nullopt;
        }
        return replicas[0].get_descriptor().buffer_descriptors[0].segment_name_;
    }
};

}  // namespace mooncake
