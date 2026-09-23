#pragma once

#include <array>
#include <atomic>
#include <cassert>
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
//
// Membership is a set of member keys. Which publication a member belongs to is
// settled by the caller (see Tenant::RemoveObject) before a membership is
// dropped, so this table keeps no identity of its own.
template <size_t StripeCount>
class StripedGroupIndex {
   public:
    // Materialize the group on demand, register the member and return the
    // group's single shared Lease under one lock section, so a lease and its
    // membership cannot disagree. Every member of a group shares that one
    // lease, and the metadata that receives it is what keeps it alive: the
    // group itself is dropped with its last member.
    //
    // Re-registering a member returns the same lease, because membership is a
    // set and a repeat is not an error. An empty group_id is the ungrouped
    // case: it joins no group and gets no lease, so no ungrouped object is ever
    // registered here.
    [[nodiscard]] std::shared_ptr<Lease> AddMember(
        std::string_view group_id, std::string_view member_key) {
        if (group_id.empty()) {
            return nullptr;
        }
        auto& stripe = StripeFor(group_id);
        std::unique_lock<std::shared_mutex> lock(stripe.mutex);
        auto [it, inserted] = stripe.groups.try_emplace(std::string(group_id));
        if (inserted) {
            it->second.lease = std::make_shared<Lease>();
            group_count_.fetch_add(1, std::memory_order_relaxed);
        }
        it->second.member_keys.insert(std::string(member_key));
        return it->second.lease;
    }

    // Drops a membership. The caller has already established that the member
    // it is unwinding is the one the route publishes, so a teardown of an older
    // object of the same key never reaches this call.
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
            group_count_.fetch_sub(1, std::memory_order_relaxed);
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
        std::vector<std::string> members;
        members.reserve(it->second.member_keys.size());
        for (const auto& member : it->second.member_keys) {
            members.push_back(member);
        }
        return members;
    }

    // Materialized groups, counted as they are created and dropped, so this
    // does not take one lock per stripe.
    [[nodiscard]] bool Empty() const {
        return group_count_.load(std::memory_order_relaxed) == 0;
    }

   private:
    struct GroupState {
        std::unordered_set<std::string, TransparentStringHash, std::equal_to<>>
            member_keys;
        std::shared_ptr<Lease> lease;

        bool Empty() const { return member_keys.empty(); }
    };

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
    // Groups materialized across every stripe. A count rather than a flag: the
    // stripes that are not being written cannot tell whether they are the last
    // group, so only a count can be lowered again.
    std::atomic<size_t> group_count_{0};
};

// 64 stripes: the shipped default. Striping trades per-tenant memory for write
// concurrency, a stripe costing ~120 bytes: 7.7 kB per tenant here against
// 30.7 kB at 256. One stripe serializes a tenant's grouped writes (about 20x
// fewer member writes per second than 64 stripes at 32 threads), and past 64
// each doubling buys less, about a third from 64 to 128 and a sixth on to 256.
// A grouping-heavy workload can raise the count by instantiating the template
// with a larger one. Measurements are in
// benchmarks/group_index_contention_bench.cpp.
using GroupIndex = StripedGroupIndex<64>;

}  // namespace mooncake
