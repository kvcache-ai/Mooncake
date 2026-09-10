#pragma once

#include <array>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lease.h"

namespace mooncake {
namespace tenant {

// GroupIndex: group_id -> {shared Lease, member keys}. A group is not a
// container of objects: it is a thin membership table plus a single shared
// Lease. The lease is the all-or-none unit consulted by eviction (the
// per-member ObjectMetadata entry points at it); the read path extends that
// shared lease on a member hit without touching this index.
class GroupIndex {
   public:
    // Return (creating on demand) the single shared Lease for a group_id.
    std::shared_ptr<Lease> LeaseFor(const std::string& group_id) {
        auto& stripe = StripeFor(group_id);
        std::unique_lock<std::shared_mutex> lock(stripe.mutex);
        auto [it, inserted] = stripe.groups.try_emplace(group_id);
        if (inserted) {
            it->second.lease = std::make_shared<Lease>();
        }
        return it->second.lease;
    }

    bool AddMember(const std::string& group_id, const std::string& member_key) {
        auto& stripe = StripeFor(group_id);
        std::unique_lock<std::shared_mutex> lock(stripe.mutex);
        auto it = stripe.groups.find(group_id);
        if (it == stripe.groups.end()) {
            return false;  // group must be materialized via LeaseFor first
        }
        return it->second.member_keys.insert(member_key).second;
    }

    bool RemoveMember(const std::string& group_id,
                      const std::string& member_key) {
        auto& stripe = StripeFor(group_id);
        std::unique_lock<std::shared_mutex> lock(stripe.mutex);
        auto it = stripe.groups.find(group_id);
        if (it == stripe.groups.end()) {
            return false;
        }
        const bool erased = it->second.member_keys.erase(member_key) > 0;
        if (erased && it->second.Empty()) {
            stripe.groups.erase(it);
        }
        return erased;
    }

    std::vector<std::string> Members(const std::string& group_id) const {
        const auto& stripe = StripeFor(group_id);
        std::shared_lock<std::shared_mutex> lock(stripe.mutex);
        auto it = stripe.groups.find(group_id);
        if (it == stripe.groups.end()) {
            return {};
        }
        return {it->second.member_keys.begin(), it->second.member_keys.end()};
    }

    bool Empty() const {
        for (const auto& stripe : stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.mutex);
            if (!stripe.groups.empty()) {
                return false;
            }
        }
        return true;
    }

   private:
    struct GroupState {
        std::unordered_set<std::string> member_keys;
        std::shared_ptr<Lease> lease;

        bool Empty() const { return member_keys.empty(); }
    };

    // Group operations are O(1) map/set updates, so a per-group-id stripe
    // keeps concurrent puts on distinct groups from serializing on one lock;
    // every operation on a group hashes to the same stripe.
    struct Stripe {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, GroupState> groups;
    };

    static constexpr size_t kStripeCount = 64;

    Stripe& StripeFor(const std::string& group_id) {
        return stripes_[std::hash<std::string>{}(group_id) % kStripeCount];
    }
    const Stripe& StripeFor(const std::string& group_id) const {
        return stripes_[std::hash<std::string>{}(group_id) % kStripeCount];
    }

    std::array<Stripe, kStripeCount> stripes_;
};

}  // namespace tenant
}  // namespace mooncake
