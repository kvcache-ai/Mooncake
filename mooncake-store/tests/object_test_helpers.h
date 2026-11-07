// object_test_helpers.h
//
// Builders shared by the per-object metadata suites.

#pragma once

#include <chrono>
#include <cstddef>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "object_metadata.h"
#include "metadata/object_route.h"

namespace mooncake {
namespace test {

// Publishes an empty 128 B part under the key `guard` holds, which holds no
// object, and returns its generation. The write time is fixed rather than read
// from the clock, so a suite that ages a lease cannot pick up a moving value.
inline route::Generation PublishEnvelope(const route::WriteGuard& guard,
                                         const std::string& group_id = {}) {
    const auto generation = guard.Publish(
        /*part_count=*/1, TenantId(), guard.key(), group_id,
        ObjectDataType::UNKNOWN, /*hard_pinned=*/false, std::nullopt);
    const auto write_time =
        std::chrono::system_clock::time_point(std::chrono::seconds(1));
    auto begin_write = guard.metadata().BeginWrite(
        /*index=*/0, /*length=*/128, std::vector<Replica>{}, UUID{1, 2},
        /*session_id=*/"", write_time);
    CHECK(begin_write.has_value())
        << "an object published by this helper starts empty";
    return generation;
}

// Publishes that envelope with one replica its writer has not ended, so the
// object carries a primary write in flight until SetWriteInFlight finishes it.
// `replica_id` names that replica, which is what a later guard needs to speak
// of the same write.
inline route::Generation PublishEnvelopeWithOpenWrite(
    const route::WriteGuard& guard, ReplicaID& replica_id,
    const std::string& group_id = {}) {
    const auto generation = guard.Publish(
        /*part_count=*/1, TenantId(), guard.key(), group_id,
        ObjectDataType::UNKNOWN, /*hard_pinned=*/false, std::nullopt);
    Replica replica("open-write.replica", 128, ReplicaStatus::PROCESSING);
    replica_id = replica.id();
    const auto write_time =
        std::chrono::system_clock::time_point(std::chrono::seconds(1));
    std::vector<Replica> replicas;
    replicas.push_back(std::move(replica));
    auto begin_write = guard.metadata().BeginWrite(
        /*index=*/0, /*length=*/128, std::move(replicas), UUID{1, 2},
        /*session_id=*/"", write_time);
    CHECK(begin_write.has_value())
        << "an object published by this helper starts empty";
    return generation;
}

// Starts or finishes the write that left `replica_id` on `metadata`. The
// attempt keeps naming that replica, so the object carries a write in flight
// exactly while the replica is unfinished.
inline void SetWriteInFlight(ObjectMetadata& metadata, ReplicaID replica_id,
                             bool in_flight) {
    const size_t visited = metadata.VisitReplicas(
        [replica_id](const Replica& replica) {
            return replica.id() == replica_id;
        },
        [in_flight](Replica& replica) {
            if (in_flight == replica.is_processing()) {
                return;
            }
            if (in_flight) {
                replica.mark_processing();
            } else {
                replica.mark_complete();
            }
        });
    CHECK_EQ(visited, 1u) << "the write left exactly one replica behind";
}

// Publishes that envelope under `key` and returns its generation, or 0 when
// the key already holds an object (the route never hands out generation 0).
inline route::Generation PublishObject(route::ObjectRoute& route,
                                       const std::string& key,
                                       const std::string& group_id = {}) {
    auto guard = route.WriteKey(key);
    if (guard.has_object()) {
        return 0;
    }
    return PublishEnvelope(guard, group_id);
}

// Reaches beneath the route's public surface: for the route's own suite,
// which counts slots and keeps one referenced, and for fixtures that need a
// restored route's state, such as a group table that starts empty.
struct ObjectRouteTestPeer {
    // Every slot in the stripes, whether or not it holds an object.
    static size_t SlotCount(const route::ObjectRoute& route) {
        size_t count = 0;
        for (const auto& stripe : route.stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.lock);
            count += stripe.slots.size();
        }
        return count;
    }

    // The stripe `key` is filed in, for tests that need two keys visited in
    // a known order.
    static size_t StripeIndex(std::string_view key) {
        return route::ObjectRoute::StripeIndex(key);
    }

    // The bucket count of one stripe's map.
    static size_t StripeBucketCount(const route::ObjectRoute& route,
                                    size_t stripe_idx) {
        const route::Stripe& stripe = route.stripes_[stripe_idx];
        std::shared_lock<std::shared_mutex> lock(stripe.lock);
        return stripe.slots.bucket_count();
    }

    // A handle on `key`'s slot, empty when the route has none. Holding it
    // keeps an empty slot in the map.
    static route::SlotHandle SlotRef(const route::ObjectRoute& route,
                                     std::string_view key) {
        return route.FindSlot(key);
    }

    // Whether `key`'s lock could be taken exclusively right now; a key the
    // route has no slot for is free.
    static bool KeyLockFree(const route::ObjectRoute& route,
                            std::string_view key) {
        const route::SlotHandle slot = route.FindSlot(key);
        if (!slot) {
            return true;
        }
        std::unique_lock<std::shared_mutex> lock(slot->mutex_,
                                                 std::try_to_lock);
        return lock.owns_lock();
    }

    // `key`'s stripe locked exclusively, as a release that drops a slot holds
    // it.
    static std::unique_lock<std::shared_mutex> LockStripe(
        const route::ObjectRoute& route, std::string_view key) {
        return std::unique_lock<std::shared_mutex>(route.StripeOf(key).lock);
    }

    // A handle on `key`'s slot made under a stripe lock the caller already
    // holds, as a lookup does.
    static route::SlotHandle SlotRefLocked(const route::ObjectRoute& route,
                                           std::string_view key) {
        route::Stripe& stripe = route.StripeOf(key);
        const auto it = stripe.slots.find(key);
        return it == stripe.slots.end() ? route::SlotHandle()
                                        : route::SlotHandle(stripe, it->second);
    }

    // Drops every grouped object's membership from the group index, leaving
    // the objects (and the leases their metadata holds) as they are: the
    // state a restore starts from before RebuildGroupState(). The slots are
    // gathered under the stripe locks and locked only after those are let go,
    // since nothing waits for a slot lock while holding a stripe.
    static void DropGroupMemberships(route::ObjectRoute& route) {
        std::vector<route::SlotHandle> slots;
        for (auto& stripe : route.stripes_) {
            std::shared_lock<std::shared_mutex> lock(stripe.lock);
            for (auto& entry : stripe.slots) {
                slots.emplace_back(stripe, entry.second);
            }
        }
        for (const auto& slot : slots) {
            std::unique_lock<std::shared_mutex> lock(slot->mutex_);
            if (!slot->record_.has_value()) {
                continue;
            }
            const std::string& group_id = slot->record_->metadata.group_id;
            if (!group_id.empty()) {
                (void)route.groups_.RemoveMember(group_id, slot->key());
            }
        }
    }
};

}  // namespace test
}  // namespace mooncake
