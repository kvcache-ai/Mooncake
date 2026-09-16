#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/transparent_string_hash.h"
#include "lease.h"

namespace mooncake {

// GroupIndex: group_id -> {shared Lease, member keys}. A group is not a
// container of objects: it is a membership table plus one shared Lease, and
// that lease is the all-or-none unit eviction reads (every member's
// ObjectMetadata points at it). The read path extends it on a member hit
// without touching this index. A group is dropped with its last member, and a
// later AddMember starts a fresh group rather than reviving the old one.
//
// The table is striped so that writes to distinct groups do not serialize on
// one lock; every operation on a group hashes to the same stripe. A stripe is
// a map plus a lock and every tenant holds one table, so the count trades
// tenant memory for write concurrency and is a template parameter rather than
// a constant.
template <size_t StripeCount>
class StripedGroupIndex {
   public:
    // Materialize the group on demand and register the member under one lock
    // section, returning the group's single shared Lease (nullptr when already
    // a member). One step, so the lease and the membership cannot disagree.
    [[nodiscard]] std::shared_ptr<Lease> AddMember(
        std::string_view group_id, std::string_view member_key) {
        auto& stripe = StripeFor(group_id);
        std::unique_lock<std::shared_mutex> lock(stripe.mutex);
        auto [it, inserted] = stripe.groups.try_emplace(std::string(group_id));
        if (inserted) {
            it->second.lease = std::make_shared<Lease>();
        }
        if (!it->second.member_keys.insert(std::string(member_key)).second) {
            return nullptr;  // already a member
        }
        return it->second.lease;
    }

    [[nodiscard]] bool RemoveMember(std::string_view group_id,
                                    std::string_view member_key) {
        auto& stripe = StripeFor(group_id);
        std::unique_lock<std::shared_mutex> lock(stripe.mutex);
        auto it = stripe.groups.find(group_id);
        if (it == stripe.groups.end()) {
            return false;
        }
        // Heterogeneous lookup, then erase by iterator: the container has no
        // heterogeneous erase of its own.
        const auto member = it->second.member_keys.find(member_key);
        if (member == it->second.member_keys.end()) {
            return false;
        }
        it->second.member_keys.erase(member);
        if (it->second.Empty()) {
            stripe.groups.erase(it);
        }
        return true;
    }

    [[nodiscard]] std::vector<std::string> Members(
        std::string_view group_id) const {
        const auto& stripe = StripeFor(group_id);
        std::shared_lock<std::shared_mutex> lock(stripe.mutex);
        auto it = stripe.groups.find(group_id);
        if (it == stripe.groups.end()) {
            return {};
        }
        return {it->second.member_keys.begin(), it->second.member_keys.end()};
    }

    [[nodiscard]] bool Empty() const {
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
        std::unordered_set<std::string, TransparentStringHash, std::equal_to<>>
            member_keys;
        std::shared_ptr<Lease> lease;

        bool Empty() const { return member_keys.empty(); }
    };

    // Group operations are O(1) map/set updates, so a per-group-id stripe keeps
    // concurrent puts on distinct groups from serializing on one lock.
    struct Stripe {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, GroupState, TransparentStringHash,
                           std::equal_to<>>
            groups;
    };

    Stripe& StripeFor(std::string_view group_id) {
        return stripes_[StripeIndex(group_id)];
    }
    const Stripe& StripeFor(std::string_view group_id) const {
        return stripes_[StripeIndex(group_id)];
    }
    // The same hash the tables use, so a group lands in one stripe no matter
    // whether the caller holds a string or a view.
    static size_t StripeIndex(std::string_view group_id) {
        return TransparentStringHash{}(group_id) % StripeCount;
    }

    std::array<Stripe, StripeCount> stripes_;
};

// 64 stripes: the shipped default. A grouping-heavy workload can raise it by
// instantiating the template with a larger count.
using GroupIndex = StripedGroupIndex<64>;

}  // namespace mooncake
