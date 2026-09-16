#pragma once

// The in-flight replica-action leases for one tenant, keyed by proposal id.
//
// A proposal is not an object: the client asks for a replica to be added or
// removed, the master answers with a lease that the client must act inside, and
// the lease expires if it does not. Several proposals may be in flight for the
// same object key, so the table cannot fold into a per-object entry, and an
// object teardown has to retract whatever is still in flight for its key.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>

#include <boost/functional/hash.hpp>

#include "rpc_types.h"
#include "types.h"

namespace mooncake {

class DynamicReplicationLeaseTable {
   public:
    // std::nullopt when no lease is recorded for the proposal.
    [[nodiscard]] std::optional<ReplicaActionLease> Find(
        const UUID& proposal_id) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        const auto it = leases_.find(proposal_id);
        return it == leases_.end()
                   ? std::nullopt
                   : std::make_optional<ReplicaActionLease>(it->second);
    }

    // Record the lease, replacing any lease already held for the proposal.
    void Put(const UUID& proposal_id, ReplicaActionLease lease) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        leases_[proposal_id] = std::move(lease);
    }

    // True when a lease was recorded for the proposal.
    [[nodiscard]] bool Remove(const UUID& proposal_id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return leases_.erase(proposal_id) > 0;
    }

    // Retract every lease for an object key, for a teardown that must not leave
    // a client acting on a key that is gone. The table is keyed by proposal, so
    // this scans it.
    void EraseForObject(std::string_view key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        std::erase_if(leases_, [key](const auto& entry) {
            return std::string_view(entry.second.key) == key;
        });
    }

    // Drop the leases whose deadline has passed.
    void EraseExpired(std::chrono::system_clock::time_point now) {
        const int64_t now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch())
                .count();
        std::unique_lock<std::shared_mutex> lock(mutex_);
        std::erase_if(leases_, [now_ms](const auto& entry) {
            return entry.second.expire_at_ms_epoch < now_ms;
        });
    }

    [[nodiscard]] bool Empty() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return leases_.empty();
    }

    // Only the suites read a lease by key; production reaches one by proposal.
    [[nodiscard]] bool HasLeaseForObjectForTest(std::string_view key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return std::any_of(leases_.begin(), leases_.end(),
                           [key](const auto& entry) {
                               return std::string_view(entry.second.key) == key;
                           });
    }

   private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<UUID, ReplicaActionLease, boost::hash<UUID>> leases_;
};

}  // namespace mooncake
