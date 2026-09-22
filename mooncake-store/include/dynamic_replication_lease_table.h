#pragma once

// The in-flight replica-action leases for one tenant, keyed by proposal id.
//
// A proposal is not an object: the client asks for a replica to be added or
// removed, the master answers with a lease that the client must act inside, and
// the lease expires if it does not. Several proposals may be in flight for the
// same object key, so the table cannot fold into a per-object entry, and an
// object teardown has to retract whatever is still in flight for its key.
//
// Three indexes share one lock so that no query scans the table: the proposal
// id finds a lease, the object key finds the proposals in flight for it, and a
// deadline heap orders the expiry sweep.
//
// Each proposal records the generation of the object it belongs to, so a
// teardown retracts the proposals of one publication and no others.

#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/functional/hash.hpp>

#include "common/transparent_string_hash.h"
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

    // Record the lease, replacing any lease already held for the proposal, and
    // remember the publication generation of the object it belongs to.
    void Put(const UUID& proposal_id, ReplicaActionLease lease,
             uint64_t generation) {
        assert(generation != 0);
        assert(lease.proposal_id == proposal_id);
        std::unique_lock<std::shared_mutex> lock(mutex_);
        const auto it = leases_.find(proposal_id);
        if (it != leases_.end() && it->second.key != lease.key) {
            Unindex(it->second.key, proposal_id);
        }
        by_key_[lease.key].insert_or_assign(proposal_id, generation);
        const int64_t expire_at = lease.expire_at_ms_epoch;
        leases_.insert_or_assign(proposal_id, std::move(lease));
        // The deadline this replaces stays in the heap and is dropped when it
        // surfaces: a node only erases the lease it still names with the same
        // deadline, and a lease carrying that deadline is expired anyway.
        deadlines_.push(Deadline{expire_at, proposal_id});
    }

    // True when a lease was recorded for the proposal.
    [[nodiscard]] bool Remove(const UUID& proposal_id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        const auto it = leases_.find(proposal_id);
        if (it == leases_.end()) {
            return false;
        }
        Unindex(it->second.key, proposal_id);
        leases_.erase(it);
        return true;
    }

    // Retract every lease for an object key, for a teardown that must not leave
    // a client acting on a key that is gone.
    void EraseForObject(std::string_view key, uint64_t generation) {
        if (generation == 0) {
            return;
        }
        std::unique_lock<std::shared_mutex> lock(mutex_);
        const auto it = by_key_.find(key);
        if (it == by_key_.end()) {
            return;
        }
        auto proposal = it->second.begin();
        while (proposal != it->second.end()) {
            if (proposal->second != generation) {
                ++proposal;
                continue;
            }
            leases_.erase(proposal->first);
            proposal = it->second.erase(proposal);
        }
        if (it->second.empty()) {
            by_key_.erase(it);
        }
    }

    // Drop the leases whose deadline has passed.
    void EraseExpired(std::chrono::system_clock::time_point now) {
        const int64_t now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch())
                .count();
        std::unique_lock<std::shared_mutex> lock(mutex_);
        while (!deadlines_.empty()) {
            const Deadline& next = deadlines_.top();
            if (next.expire_at_ms_epoch >= now_ms) {
                break;
            }
            const Deadline expired = next;
            deadlines_.pop();
            const auto it = leases_.find(expired.proposal_id);
            if (it == leases_.end() ||
                it->second.expire_at_ms_epoch != expired.expire_at_ms_epoch) {
                continue;  // already gone, or superseded by a later deadline
            }
            Unindex(it->second.key, expired.proposal_id);
            leases_.erase(it);
        }
    }

    // True when no lease is in flight.
    [[nodiscard]] bool Empty() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return leases_.empty();
    }

   private:
    // Earliest deadline first.
    struct Deadline {
        int64_t expire_at_ms_epoch;
        UUID proposal_id;

        bool operator>(const Deadline& other) const {
            return expire_at_ms_epoch > other.expire_at_ms_epoch;
        }
    };

    void Unindex(std::string_view key, const UUID& proposal_id) {
        const auto it = by_key_.find(key);
        if (it == by_key_.end()) {
            return;
        }
        it->second.erase(proposal_id);
        if (it->second.empty()) {
            by_key_.erase(it);
        }
    }

    mutable std::shared_mutex mutex_;
    std::unordered_map<UUID, ReplicaActionLease, boost::hash<UUID>> leases_;
    // The proposals in flight per object key: proposal -> the generation it was
    // registered under, so a teardown retracts its own without scanning.
    std::unordered_map<std::string,
                       std::unordered_map<UUID, uint64_t, boost::hash<UUID>>,
                       TransparentStringHash, std::equal_to<>>
        by_key_;
    // One node per Put, so a repeat leaves a superseded node behind.
    std::priority_queue<Deadline, std::vector<Deadline>, std::greater<>>
        deadlines_;
};

}  // namespace mooncake
