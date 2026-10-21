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

// One disk replica in `status`: a suite that asks whether an object has work in
// flight needs a replica its write has not completed, and a disk replica needs
// no segment to exist.
inline Replica DiskReplica(ReplicaStatus status, const std::string& path,
                           size_t length = 128) {
    return Replica(path, length, status);
}

// Publishes an empty 128 B part under the key `guard` holds, which holds no
// object, and returns its generation. These suites exercise the object model,
// not replica validity, so the replica-less default sets no real replica; a
// suite that wants work in flight hands it a PROCESSING one. The write time is
// fixed rather than read from the clock so a suite that ages a lease cannot
// pick up a moving value.
inline route::Generation PublishEnvelope(const route::WriteGuard& guard,
                                         const std::string& group_id = {},
                                         std::vector<Replica> replicas = {}) {
    const auto generation = guard.Publish(
        /*part_count=*/1, TenantId(), guard.key(), group_id,
        ObjectDataType::UNKNOWN, /*hard_pinned=*/false, std::nullopt);
    const auto write_time =
        std::chrono::system_clock::time_point(std::chrono::seconds(1));
    auto begin_write = guard.metadata().BeginWrite(
        /*index=*/0, /*length=*/128, std::move(replicas), UUID{1, 2},
        /*session_id=*/"", write_time);
    CHECK(begin_write.has_value())
        << "an object published by this helper starts empty";
    return generation;
}

// Publishes that envelope under `key` and returns its generation, or 0 when
// the key already holds an object (the route never hands out generation 0).
inline route::Generation PublishObject(route::ObjectRoute& route,
                                       const std::string& key,
                                       const std::string& group_id = {},
                                       std::vector<Replica> replicas = {}) {
    auto guard = route.WriteKey(key);
    if (guard.has_object()) {
        return 0;
    }
    return PublishEnvelope(guard, group_id, std::move(replicas));
}

// Completes every replica the write of `guard`'s object handed out, so the
// object no longer has a write in flight.
inline void FinishWriteInFlight(const route::WriteGuard& guard) {
    std::vector<ReplicaID> ids;
    const ObjectPart* part = guard.metadata().GetPart(0);
    CHECK(part != nullptr) << "the write in flight is on part 0";
    for (const auto& replica : part->replicas()) {
        ids.push_back(replica.id());
    }
    auto ended = guard.metadata().EndWrite(0, ids);
    CHECK(ended.has_value()) << "the write in flight closes";
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

    // A handle on `key`'s slot, empty when the route has none. Holding it
    // keeps an empty slot in the map.
    static route::SlotHandle SlotRef(const route::ObjectRoute& route,
                                     std::string_view key) {
        return route.FindSlot(key);
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
