#include "metadata/object_route.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

using route::Generation;
using route::ObjectRef;
using route::ObjectRoute;
using test::ObjectRouteTestPeer;
using test::PublishObject;

// The lease the object under `key` holds, read under the key's read lock and
// the envelope's own lease lock; null when the key holds no object.
std::shared_ptr<Lease> LeaseOf(const ObjectRoute& route, std::string_view key) {
    auto guard = route.Read(key);
    if (!guard) {
        return nullptr;
    }
    SpinLocker locker(&guard->metadata().lock);
    return guard->metadata().lease_;
}

// Tears down whatever `key` holds; false when it holds nothing.
bool TearDownKey(ObjectRoute& route, std::string_view key) {
    auto guard = route.Write(key);
    if (!guard) {
        return false;
    }
    guard->TearDown();
    return true;
}

std::vector<std::string> Sorted(std::vector<std::string> keys) {
    std::sort(keys.begin(), keys.end());
    return keys;
}

std::vector<std::string> SortedMembers(const ObjectRoute& route,
                                       std::string_view group_id) {
    return Sorted(route.GroupMembers(group_id));
}

// --- Publication and generations ---------------------------------------------

TEST(ObjectRouteTest, PublishMakesTheObjectReachableUnderItsKey) {
    ObjectRoute route;
    EXPECT_EQ(route.ObjectCount(), 0u);
    EXPECT_FALSE(route.Read("k1").has_value());

    Generation generation = 0;
    {
        auto guard = route.WriteKey("k1");
        EXPECT_EQ(guard.key(), "k1");
        EXPECT_FALSE(guard.has_object());
        generation = test::PublishEnvelope(guard);
        // The guard that published now holds the object it published.
        ASSERT_TRUE(guard.has_object());
        EXPECT_EQ(guard.generation(), generation);
        EXPECT_EQ(guard.metadata().user_key, "k1");
        EXPECT_FALSE(guard.state().HasWorkInFlight());
    }
    EXPECT_NE(generation, 0u);
    EXPECT_EQ(route.ObjectCount(), 1u);

    auto read = route.Read("k1");
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(read->generation(), generation);
    EXPECT_EQ(read->metadata().TotalLength(), 128u);
    const ObjectRef ref = read->ref();
    EXPECT_EQ(ref.key, "k1");
    EXPECT_EQ(ref.generation, generation);
    read.reset();

    auto write = route.Write("k1");
    ASSERT_TRUE(write.has_value());
    EXPECT_EQ(write->generation(), generation);
    write.reset();

    EXPECT_FALSE(route.Read("missing").has_value());
    EXPECT_FALSE(route.Write("missing").has_value());
}

TEST(ObjectRouteTest, WriteKeyLocksAnAbsentKeyWithoutPublishing) {
    ObjectRoute route;
    {
        // The writer holds the key although it holds no object, so it can
        // decide whether to publish one; until it does, nothing is counted.
        auto guard = route.WriteKey("k1");
        EXPECT_FALSE(guard.has_object());
        EXPECT_EQ(route.ObjectCount(), 0u);
        EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
    }
    // Leaving it empty publishes nothing and leaves no slot behind.
    EXPECT_FALSE(route.Contains("k1"));
    EXPECT_EQ(route.ObjectCount(), 0u);
    EXPECT_TRUE(route.Empty());
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 0u);
}

TEST(ObjectRouteTest, WriteKeyHoldsAnExistingObject) {
    ObjectRoute route;
    const Generation generation = PublishObject(route, "k1");
    ASSERT_NE(generation, 0u);

    // A writer that meets an object already there holds that object, and the
    // helper built on it refuses to publish over it.
    {
        auto guard = route.WriteKey("k1");
        ASSERT_TRUE(guard.has_object());
        EXPECT_EQ(guard.generation(), generation);
    }
    EXPECT_EQ(PublishObject(route, "k1"), 0u);
    EXPECT_EQ(route.ObjectCount(), 1u);
    EXPECT_EQ(route.Read("k1")->generation(), generation);
}

TEST(ObjectRouteTest, GenerationsAreNeverReused) {
    ObjectRoute route;
    const Generation first = PublishObject(route, "k1");
    const Generation other_key = PublishObject(route, "k2");
    EXPECT_NE(first, other_key);

    // A republication of the same key is a new publication with a new name.
    ASSERT_TRUE(TearDownKey(route, "k1"));
    const Generation second = PublishObject(route, "k1");
    EXPECT_NE(second, first);
    EXPECT_NE(second, other_key);
}

TEST(ObjectRouteTest, RefReachesOnlyItsOwnPublication) {
    ObjectRoute route;
    const Generation first = PublishObject(route, "k1");
    const ObjectRef ref{"k1", first};

    EXPECT_TRUE(route.Read(ref).has_value());
    EXPECT_TRUE(route.Write(ref).has_value());
    // A ref to a key that holds nothing, or naming another generation, finds
    // nothing.
    EXPECT_FALSE(route.Read(ObjectRef{"missing", first}).has_value());
    EXPECT_FALSE(route.Read(ObjectRef{"k1", first + 100}).has_value());

    // Torn down: the ref is stale, and so is the key.
    ASSERT_TRUE(TearDownKey(route, "k1"));
    EXPECT_FALSE(route.Read(ref).has_value());
    EXPECT_FALSE(route.Write(ref).has_value());

    // Republished: the key holds an object again, but not the one the ref
    // names, so a caller that judged the first one cannot act on the second.
    const Generation second = PublishObject(route, "k1");
    EXPECT_FALSE(route.Read(ref).has_value());
    EXPECT_FALSE(route.Write(ref).has_value());
    auto current = route.Write(ObjectRef{"k1", second});
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(current->generation(), second);
}

// --- Teardown ----------------------------------------------------------------

TEST(ObjectRouteTest, TearDownRunsOnceAcrossErasers) {
    ObjectRoute route;
    const Generation generation = PublishObject(route, "k1", "g1");
    const ObjectRef ref{"k1", generation};

    // Two erasers judged the same object. The first tears it down; the second
    // comes back with the same ref, or with the bare key, and finds no object.
    {
        auto guard = route.Write(ref);
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
        EXPECT_FALSE(guard->has_object());
    }
    EXPECT_FALSE(route.Write(ref).has_value());
    EXPECT_FALSE(route.Write("k1").has_value());

    EXPECT_FALSE(route.Contains("k1"));
    EXPECT_EQ(route.ObjectCount(), 0u);
    EXPECT_TRUE(route.GroupMembers("g1").empty());
    EXPECT_TRUE(route.Empty());
}

TEST(ObjectRouteTest, PublishAfterTearDownOnTheSameGuardReplacesTheObject) {
    ObjectRoute route;
    const Generation first = PublishObject(route, "k1", "g1");

    Generation second = 0;
    {
        // A replacement under one guard: no reader can find the key absent
        // in between, and the new publication joins its own group.
        auto guard = route.WriteKey("k1");
        ASSERT_TRUE(guard.has_object());
        guard.TearDown();
        EXPECT_FALSE(guard.has_object());
        EXPECT_EQ(route.ObjectCount(), 0u);
        second = test::PublishEnvelope(guard, "g2");
        ASSERT_TRUE(guard.has_object());
        EXPECT_EQ(guard.generation(), second);
        EXPECT_EQ(guard.metadata().group_id, "g2");
    }
    EXPECT_NE(second, first);
    EXPECT_EQ(route.ObjectCount(), 1u);
    EXPECT_TRUE(route.GroupMembers("g1").empty());
    EXPECT_EQ(route.GroupMembers("g2"), (std::vector<std::string>{"k1"}));
    EXPECT_FALSE(route.Read(ObjectRef{"k1", first}).has_value());
    EXPECT_TRUE(route.Read(ObjectRef{"k1", second}).has_value());
}

// --- Groups and leases -------------------------------------------------------

TEST(ObjectRouteTest, PublishJoinsTheGroupOnItsSharedLease) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1", "g1"), 0u);
    ASSERT_NE(PublishObject(route, "k2", "g1"), 0u);

    EXPECT_EQ(route.ObjectCount(), 2u);
    EXPECT_EQ(SortedMembers(route, "g1"),
              (std::vector<std::string>{"k1", "k2"}));

    // Both members point at the one shared lease, so the group has a single
    // deadline.
    const auto first_lease = LeaseOf(route, "k1");
    ASSERT_NE(first_lease, nullptr);
    EXPECT_EQ(first_lease.get(), LeaseOf(route, "k2").get());
}

TEST(ObjectRouteTest, PublishLeavesAnUngroupedObjectOnItsOwnLease) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    ASSERT_NE(PublishObject(route, "k2"), 0u);

    // An ungrouped object joins no group and keeps the never-granted lease
    // its envelope was constructed with: present, its own, and expired.
    EXPECT_TRUE(route.GroupMembers("").empty());
    const auto lease = LeaseOf(route, "k1");
    ASSERT_NE(lease, nullptr);
    EXPECT_NE(lease.get(), LeaseOf(route, "k2").get());
    EXPECT_TRUE(route.Read("k1")->metadata().IsLeaseExpired());
}

TEST(ObjectRouteTest, TearDownDropsOnlyItsOwnMembership) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1", "g1"), 0u);
    ASSERT_NE(PublishObject(route, "k2", "g1"), 0u);
    const auto group_lease = LeaseOf(route, "k1");

    // The group keeps its other member and the one lease both of them held.
    ASSERT_TRUE(TearDownKey(route, "k1"));
    EXPECT_EQ(route.GroupMembers("g1"), (std::vector<std::string>{"k2"}));
    EXPECT_EQ(LeaseOf(route, "k2").get(), group_lease.get());
    EXPECT_FALSE(route.Empty());

    ASSERT_TRUE(TearDownKey(route, "k2"));
    EXPECT_TRUE(route.GroupMembers("g1").empty());
    EXPECT_TRUE(route.Empty());
}

TEST(ObjectRouteTest, AGroupKeepsOneLeaseAcrossReplacementOfItsKeys) {
    // A member's lease is decided by the group it belongs to, and the group is
    // replaced only when its last member leaves.
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1", "g1"), 0u);
    ASSERT_NE(PublishObject(route, "k2", "g1"), 0u);
    const auto group_lease = LeaseOf(route, "k1");
    ASSERT_NE(group_lease, nullptr);

    // Replacing one key of the group re-registers that member and keeps both
    // on the group's one lease.
    ASSERT_TRUE(TearDownKey(route, "k1"));
    ASSERT_NE(PublishObject(route, "k1", "g1"), 0u);
    ASSERT_EQ(route.GroupMembers("g1").size(), 2u);
    EXPECT_EQ(LeaseOf(route, "k1").get(), group_lease.get());
    EXPECT_EQ(LeaseOf(route, "k2").get(), group_lease.get());

    // The group, and its lease, go with the last member; a later publication
    // gets the fresh lease of a new group.
    ASSERT_TRUE(TearDownKey(route, "k1"));
    ASSERT_TRUE(TearDownKey(route, "k2"));
    ASSERT_TRUE(route.GroupMembers("g1").empty());

    ASSERT_NE(PublishObject(route, "k1", "g1"), 0u);
    const auto late_lease = LeaseOf(route, "k1");
    ASSERT_NE(late_lease, nullptr);
    EXPECT_NE(late_lease.get(), group_lease.get());
}

TEST(ObjectRouteTest, RebuildGroupStateRegroupsMembersOnTheLatestDeadline) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1", "g1"), 0u);
    ASSERT_NE(PublishObject(route, "k2", "g1"), 0u);
    ASSERT_NE(PublishObject(route, "k3", "g2"), 0u);
    ASSERT_NE(PublishObject(route, "solo"), 0u);

    // A restored route starts with objects that each carry the deadline they
    // were saved with, on a lease of their own, and no group membership.
    ObjectRouteTestPeer::DropGroupMemberships(route);
    ASSERT_TRUE(route.GroupMembers("g1").empty());
    ASSERT_TRUE(route.GroupMembers("g2").empty());
    const auto at = [](int seconds) {
        return std::chrono::system_clock::time_point(
            std::chrono::seconds(seconds));
    };
    const auto restore_lease = [&](std::string_view key, int seconds) {
        auto guard = route.Write(key);
        ASSERT_TRUE(guard.has_value());
        auto lease = std::make_shared<Lease>();
        lease->ExtendTo(at(seconds));
        SpinLocker locker(&guard->metadata().lock);
        guard->metadata().lease_ = std::move(lease);
    };
    restore_lease("k1", 100);
    restore_lease("k2", 300);
    restore_lease("k3", 200);
    restore_lease("solo", 50);
    const auto solo_lease = LeaseOf(route, "solo");

    route.RebuildGroupState();

    // Membership is back, the members of a group share one lease again, and
    // that lease keeps the latest deadline any member was restored with.
    EXPECT_EQ(SortedMembers(route, "g1"),
              (std::vector<std::string>{"k1", "k2"}));
    EXPECT_EQ(route.GroupMembers("g2"), (std::vector<std::string>{"k3"}));
    const auto g1_lease = LeaseOf(route, "k1");
    ASSERT_NE(g1_lease, nullptr);
    EXPECT_EQ(LeaseOf(route, "k2").get(), g1_lease.get());
    EXPECT_EQ(g1_lease->ExpiresAt(), at(300));
    const auto g2_lease = LeaseOf(route, "k3");
    ASSERT_NE(g2_lease, nullptr);
    EXPECT_NE(g2_lease.get(), g1_lease.get());
    EXPECT_EQ(g2_lease->ExpiresAt(), at(200));
    // An ungrouped object keeps its own lease.
    EXPECT_EQ(LeaseOf(route, "solo").get(), solo_lease.get());
    EXPECT_EQ(solo_lease->ExpiresAt(), at(50));
    EXPECT_EQ(route.ObjectCount(), 4u);
}

// --- Counting ----------------------------------------------------------------

TEST(ObjectRouteTest, ObjectCountAndEmptyTrackPublications) {
    ObjectRoute route;
    EXPECT_TRUE(route.Empty());

    ASSERT_NE(PublishObject(route, "k1", "g1"), 0u);
    ASSERT_NE(PublishObject(route, "k2"), 0u);
    EXPECT_EQ(route.ObjectCount(), 2u);
    EXPECT_FALSE(route.Empty());

    // A failed publication counts nothing.
    EXPECT_EQ(PublishObject(route, "k1", "g1"), 0u);
    EXPECT_EQ(route.ObjectCount(), 2u);

    ASSERT_TRUE(TearDownKey(route, "k1"));
    EXPECT_EQ(route.ObjectCount(), 1u);
    EXPECT_FALSE(route.Empty());
    ASSERT_TRUE(TearDownKey(route, "k2"));
    EXPECT_EQ(route.ObjectCount(), 0u);
    EXPECT_TRUE(route.Empty());
}

TEST(ObjectRouteTest, ContainsSeesOnlyKeysThatHoldAnObject) {
    ObjectRoute route;
    EXPECT_FALSE(route.Contains("k1"));
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    EXPECT_TRUE(route.Contains("k1"));
    EXPECT_FALSE(route.Contains("k2"));

    // A slot kept alive without an object does not count as one.
    const auto kept = ObjectRouteTestPeer::SlotRef(route, "k1");
    ASSERT_TRUE(TearDownKey(route, "k1"));
    ASSERT_EQ(ObjectRouteTestPeer::SlotRef(route, "k1").get(), kept.get());
    EXPECT_FALSE(route.Contains("k1"));
}

// --- Slot lifetime -----------------------------------------------------------

TEST(ObjectRouteTest, EmptySlotGoesWithItsLastWriter) {
    ObjectRoute route;

    // A writer that publishes nothing leaves no slot.
    {
        auto guard = route.WriteKey("k1");
    }
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 0u);

    // A slot with an object stays; the writer that tears the object down
    // takes the slot with it.
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    ASSERT_NE(PublishObject(route, "k2"), 0u);
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 2u);
    ASSERT_TRUE(TearDownKey(route, "k1"));
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
    EXPECT_EQ(ObjectRouteTestPeer::SlotRef(route, "k1").get(), nullptr);
    EXPECT_NE(ObjectRouteTestPeer::SlotRef(route, "k2").get(), nullptr);

    // A replacement under one guard keeps its slot.
    {
        auto guard = route.WriteKey("k2");
        guard.TearDown();
        (void)test::PublishEnvelope(guard);
    }
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
    // So does a read or write of a key that holds an object.
    EXPECT_TRUE(route.Read("k2").has_value());
    EXPECT_TRUE(route.Write("k2").has_value());
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
}

TEST(ObjectRouteTest, WriterWaitingOnAnEmptySlotTakesItOver) {
    ObjectRoute route;
    std::promise<void> held;
    std::promise<void> release;
    std::thread holder([&] {
        auto guard = route.WriteKey("k1");
        held.set_value();
        release.get_future().wait();
    });
    held.get_future().wait();

    // A second writer queues on the slot while the first holds it empty. The
    // first leaves it empty, but the slot is still referenced then, so it
    // stays for the second, which publishes into it.
    std::thread publisher([&] { EXPECT_NE(PublishObject(route, "k1"), 0u); });
    // Give the publisher time to find the slot and wait on its lock; if it
    // has not yet, it finds or creates the slot afterwards, with the same
    // outcome.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    release.set_value();
    holder.join();
    publisher.join();

    EXPECT_TRUE(route.Contains("k1"));
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
}

TEST(ObjectRouteTest, ReferencedEmptySlotGoesWithItsLastHandle) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    ASSERT_NE(PublishObject(route, "k2"), 0u);

    // Someone else still holds k1's slot when its last writer leaves it
    // empty, so the slot stays for that holder.
    auto kept = ObjectRouteTestPeer::SlotRef(route, "k1");
    ASSERT_NE(kept.get(), nullptr);
    ASSERT_TRUE(TearDownKey(route, "k1"));
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 2u);
    EXPECT_EQ(ObjectRouteTestPeer::SlotRef(route, "k1").get(), kept.get());

    // Letting go of the last handle takes the slot, and only it.
    kept.reset();
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
    EXPECT_TRUE(route.Contains("k2"));
}

// The last handle on an empty slot drops it only if it is still the last once
// it holds the stripe: a holder that joined while it waited keeps the slot,
// and that holder's release drops it in turn.
TEST(ObjectRouteTest, LastReleaseKeepsASlotSomeoneJoinedMeanwhile) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    auto kept = ObjectRouteTestPeer::SlotRef(route, "k1");
    ASSERT_TRUE(TearDownKey(route, "k1"));

    auto stripe = ObjectRouteTestPeer::LockStripe(route, "k1");
    std::thread releaser([&] { kept.reset(); });
    // Give the release time to find itself last and wait on the stripe; if it
    // has not yet, it sees the second holder and only counts down.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto joined = ObjectRouteTestPeer::SlotRefLocked(route, "k1");
    ASSERT_NE(joined.get(), nullptr);
    stripe.unlock();
    releaser.join();
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);

    joined.reset();
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 0u);
}

// Many holders letting go of one empty slot at once drop it exactly once.
TEST(ObjectRouteTest, ConcurrentReleasesDropAnEmptySlotOnce) {
    ObjectRoute route;
    constexpr int kRounds = 200;
    constexpr int kReaders = 8;
    for (int round = 0; round < kRounds; ++round) {
        std::promise<void> held;
        std::promise<void> release;
        std::thread writer([&] {
            auto guard = route.WriteKey("k1");
            held.set_value();
            release.get_future().wait();
        });
        held.get_future().wait();
        // Each reader holds the slot while it waits for the writer, which
        // leaves it empty; whichever release comes last drops it.
        std::vector<std::thread> readers;
        for (int r = 0; r < kReaders; ++r) {
            readers.emplace_back(
                [&] { EXPECT_FALSE(route.Read("k1").has_value()); });
        }
        release.set_value();
        writer.join();
        for (auto& reader : readers) {
            reader.join();
        }
        ASSERT_EQ(ObjectRouteTestPeer::SlotCount(route), 0u);
    }
}

TEST(ObjectRouteTest, KeptSlotIsReusedByTheNextPublication) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    auto kept = ObjectRouteTestPeer::SlotRef(route, "k1");
    ASSERT_TRUE(TearDownKey(route, "k1"));

    // While a slot exists it is the only one for its key, so the next
    // publication lands in it rather than in a second one.
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    EXPECT_EQ(ObjectRouteTestPeer::SlotRef(route, "k1").get(), kept.get());
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
    kept.reset();
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
    EXPECT_TRUE(route.Contains("k1"));
}

// --- Cursors -----------------------------------------------------------------

TEST(ObjectRouteTest, ReadCursorVisitsEveryObjectOnce) {
    ObjectRoute route;
    std::vector<ObjectRef> published;
    for (int i = 0; i < 200; ++i) {
        const std::string key = "k" + std::to_string(i);
        published.push_back({key, PublishObject(route, key, i % 3 ? "" : "g")});
    }
    // A slot that holds no object is passed over.
    auto kept = ObjectRouteTestPeer::SlotRef(route, "k0");
    ASSERT_TRUE(TearDownKey(route, "k0"));
    published.erase(published.begin());

    std::vector<std::string> keys;
    std::vector<std::string> expected;
    for (auto object : route.ReadCursor()) {
        // The cursor names the publication it stands on.
        EXPECT_EQ(object.ref().key, object.key());
        EXPECT_EQ(object.ref().generation, object.generation());
        EXPECT_EQ(object.metadata().user_key, object.key());
        keys.push_back(object.key());
    }
    for (const auto& ref : published) {
        expected.push_back(ref.key);
    }
    EXPECT_EQ(Sorted(keys), Sorted(expected));

    // Named, since a temporary route would go before the loop body runs.
    const ObjectRoute empty;
    size_t count = 0;
    for (auto object : empty.ReadCursor()) {
        (void)object;
        ++count;
    }
    EXPECT_EQ(count, 0u);
}

TEST(ObjectRouteTest, WriteCursorChangesMetadataAndState) {
    ObjectRoute route;
    for (const char* key : {"k1", "k2"}) {
        auto guard = route.WriteKey(key);
        test::PublishEnvelope(guard);
        guard.state().offloading_task.emplace();
    }

    // A write cursor may change a task, but not whether the object has work
    // in flight: it does not run the observer.
    for (auto object : route.WriteCursor()) {
        object.metadata().MutablePart(0).SetChecksum(42);
        object.state().offloading_task->mirror_clients.push_back(UUID{3, 4});
    }
    for (const char* key : {"k1", "k2"}) {
        auto guard = route.Read(key);
        ASSERT_TRUE(guard.has_value());
        ASSERT_TRUE(guard->metadata().GetPart(0).checksum().has_value());
        EXPECT_EQ(*guard->metadata().GetPart(0).checksum(), 42u);
        ASSERT_TRUE(guard->state().offloading_task.has_value());
        EXPECT_EQ(guard->state().offloading_task->mirror_clients.size(), 1u);
    }
}

// Leaving the loop early releases the stripe and the slot it stood on, so
// every key is writable again.
TEST(ObjectRouteTest, BreakingOutOfACursorReleasesItsLocks) {
    ObjectRoute route;
    for (int i = 0; i < 8; ++i) {
        ASSERT_NE(PublishObject(route, "k" + std::to_string(i)), 0u);
    }
    for (auto object : route.WriteCursor()) {
        (void)object;
        break;
    }
    for (int i = 0; i < 8; ++i) {
        EXPECT_TRUE(TearDownKey(route, "k" + std::to_string(i)));
    }
    EXPECT_TRUE(route.Empty());
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 0u);
}

// A key locked elsewhere is not waited for under the stripe lock: the cursor
// visits everything else first, then waits for it.
TEST(ObjectRouteTest, CursorVisitsABusyKeyAfterTheOthers) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "busy"), 0u);
    for (int i = 0; i < 8; ++i) {
        ASSERT_NE(PublishObject(route, "k" + std::to_string(i)), 0u);
    }

    std::promise<void> held;
    std::promise<void> release;
    std::thread holder([&] {
        auto guard = route.Write("busy");
        ASSERT_TRUE(guard.has_value());
        held.set_value();
        release.get_future().wait();
    });
    held.get_future().wait();

    std::vector<std::string> keys;
    for (auto object : route.ReadCursor()) {
        keys.push_back(object.key());
        if (keys.size() == 8) {
            release.set_value();
        }
    }
    holder.join();
    ASSERT_EQ(keys.size(), 9u);
    EXPECT_EQ(keys.back(), "busy");
}

// A busy key is visited only while it still holds an object: one torn down
// before the cursor gets its lock is skipped. Whether the cursor had already
// set the key aside when it was torn down depends on the stripe order, so
// only its absence from the visit is checked, and that nothing is left behind
// once the cursor is done.
TEST(ObjectRouteTest, CursorSkipsABusyKeyTornDownMeanwhile) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "busy"), 0u);
    ASSERT_NE(PublishObject(route, "other"), 0u);

    std::promise<void> held;
    std::promise<void> tear_down;
    std::thread holder([&] {
        auto guard = route.Write("busy");
        ASSERT_TRUE(guard.has_value());
        held.set_value();
        tear_down.get_future().wait();
        guard->TearDown();
    });
    held.get_future().wait();

    std::vector<std::string> keys;
    for (auto object : route.ReadCursor()) {
        keys.push_back(object.key());
        if (object.key() == "other") {
            tear_down.set_value();
        }
    }
    holder.join();
    EXPECT_EQ(keys, (std::vector<std::string>{"other"}));
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
    EXPECT_EQ(route.ObjectCount(), 1u);
}

// A key the cursor set aside, then found empty, leaves the map once the cursor
// lets go of it: by then the cursor is its last holder.
TEST(ObjectRouteTest, CursorLeavesNoEmptySlotItSetAside) {
    ObjectRoute route;
    // "busy" must sit in an earlier stripe than the key whose visit tears it
    // down, so the cursor has set it aside by then.
    const std::string busy = "busy";
    std::string later;
    for (int i = 0; later.empty(); ++i) {
        const std::string candidate = "later" + std::to_string(i);
        if (ObjectRouteTestPeer::StripeIndex(candidate) >
            ObjectRouteTestPeer::StripeIndex(busy)) {
            later = candidate;
        }
    }
    ASSERT_NE(PublishObject(route, busy), 0u);
    ASSERT_NE(PublishObject(route, later), 0u);

    std::promise<void> held;
    std::promise<void> tear_down;
    std::thread holder([&] {
        auto guard = route.Write(busy);
        ASSERT_TRUE(guard.has_value());
        held.set_value();
        tear_down.get_future().wait();
        guard->TearDown();
    });
    held.get_future().wait();

    std::vector<std::string> keys;
    for (auto object : route.ReadCursor()) {
        keys.push_back(object.key());
        // The holder's release cannot drop the slot, since the cursor still
        // holds it. It touches only busy's stripe, which the cursor no
        // longer holds, so waiting for it here cannot deadlock.
        tear_down.set_value();
        holder.join();
    }
    EXPECT_EQ(keys, (std::vector<std::string>{later}));

    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 1u);
}

// Misuse that would corrupt the route aborts in release builds too.
TEST(ObjectRouteDeathTest, PublishOverAnObjectAborts) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    EXPECT_DEATH(
        {
            auto guard = route.WriteKey("k1");
            (void)test::PublishEnvelope(guard);
        },
        "already holds an object");
}

TEST(ObjectRouteDeathTest, PublishOfAnotherKeysMetadataAborts) {
    ObjectRoute route;
    const auto write_time =
        std::chrono::system_clock::time_point(std::chrono::seconds(1));
    EXPECT_DEATH(
        {
            auto guard = route.WriteKey("k1");
            (void)guard.Publish(UUID{1, 2}, write_time, 128,
                                std::vector<Replica>{}, std::nullopt, false,
                                ObjectDataType::UNKNOWN, std::string(),
                                TenantId(), std::string("k2"));
        },
        "of an object keyed k2");
}

TEST(ObjectRouteDeathTest, TearDownOfAnEmptyKeyAborts) {
    ObjectRoute route;
    EXPECT_DEATH(route.WriteKey("k1").TearDown(), "holds no object");
}

#ifndef NDEBUG
TEST(ObjectRouteDeathTest, DestroyingARouteUnderAGuardAborts) {
    EXPECT_DEATH(
        {
            auto route = std::make_unique<ObjectRoute>();
            auto guard = route->WriteKey("k1");
            route.reset();
        },
        "is still held");
}

TEST(ObjectRouteDeathTest, DestroyingARouteUnderACursorAborts) {
    EXPECT_DEATH(
        {
            auto route = std::make_unique<ObjectRoute>();
            (void)PublishObject(*route, "k1");
            for (auto object : route->ReadCursor()) {
                (void)object;
                route.reset();
            }
        },
        "under an open cursor");
}

TEST(ObjectRouteDeathTest, RouteAccessFromACursorAsserts) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    EXPECT_DEATH(
        {
            for (auto object : route.ReadCursor()) {
                (void)route.Contains(object.key());
            }
        },
        "route access from inside an object cursor");
}

// The check runs whether the cursor moves on or the loop is left on the
// object that was changed.
TEST(ObjectRouteDeathTest, WriteCursorChangingInFlightWorkAsserts) {
    ObjectRoute route;
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    EXPECT_DEATH(
        {
            for (auto object : route.WriteCursor()) {
                object.state().is_processing = true;
            }
        },
        "changed in-flight work");
    EXPECT_DEATH(
        {
            for (auto object : route.WriteCursor()) {
                object.state().is_processing = true;
                break;
            }
        },
        "changed in-flight work");
}
#endif

// --- Observer ----------------------------------------------------------------

// Records what every released write guard held, as the owner sees it.
class RecordingObserver final : public route::RouteObserver {
   public:
    struct Release {
        std::string key;
        bool has_object;
        bool processing;
    };

    void OnWriteRelease(const route::WriteGuard& guard) override {
        releases.push_back({guard.key(), guard.has_object(),
                            guard.has_object() && guard.state().is_processing});
    }

    std::vector<Release> releases;
};

TEST(ObjectRouteTest, ObserverSeesEveryWriteGuardAsItIsReleased) {
    RecordingObserver observer;
    ObjectRoute route(&observer);

    // A writer that leaves the key empty is reported too.
    {
        auto guard = route.WriteKey("k1");
    }
    ASSERT_EQ(observer.releases.size(), 1u);
    EXPECT_EQ(observer.releases[0].key, "k1");
    EXPECT_FALSE(observer.releases[0].has_object);

    // The observer sees the key as the writer left it, not as it was taken,
    // and only once: the guard's moves are not releases.
    {
        auto guard = route.WriteKey("k1");
        (void)test::PublishEnvelope(guard);
        guard.state().is_processing = true;
        EXPECT_EQ(observer.releases.size(), 1u);
    }
    ASSERT_EQ(observer.releases.size(), 2u);
    EXPECT_TRUE(observer.releases[1].has_object);
    EXPECT_TRUE(observer.releases[1].processing);

    {
        auto guard = route.Write("k1");
        ASSERT_TRUE(guard.has_value());
        guard->state().is_processing = false;
    }
    ASSERT_EQ(observer.releases.size(), 3u);
    EXPECT_TRUE(observer.releases[2].has_object);
    EXPECT_FALSE(observer.releases[2].processing);

    {
        auto guard = route.Write("k1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
    }
    ASSERT_EQ(observer.releases.size(), 4u);
    EXPECT_FALSE(observer.releases[3].has_object);
}

TEST(ObjectRouteTest, ObserverIsNotCalledForReadsOrCursors) {
    RecordingObserver observer;
    ObjectRoute route(&observer);
    ASSERT_NE(PublishObject(route, "k1"), 0u);
    observer.releases.clear();

    EXPECT_TRUE(route.Read("k1").has_value());
    EXPECT_TRUE(route.Contains("k1"));
    // A key with no slot makes no guard at all.
    EXPECT_FALSE(route.Write("missing").has_value());
    for (auto object : route.ReadCursor()) {
        (void)object;
    }
    // A write cursor does not run the observer, which is why it must not
    // start or finish in-flight work.
    for (auto object : route.WriteCursor()) {
        object.metadata().MutablePart(0).SetChecksum(7);
    }
    EXPECT_TRUE(observer.releases.empty());
}

// --- Concurrency -------------------------------------------------------------

TEST(ObjectRouteTest, ChurnKeepsTheRouteAndTheGroupConsistent) {
    // Two keys of one group are published and torn down over and over, while
    // other threads tear down whatever publication they resolved earlier,
    // mutate the current one, watch both keys at once and walk the route with
    // a cursor.
    ObjectRoute route;
    const std::vector<std::string> keys = {"k1", "k2"};
    constexpr int kWriters = 4;
    constexpr int kWriterRounds = 20000;
    // The other threads yield between rounds: std::shared_mutex prefers
    // readers on glibc, and readers that never pause starve the writers on a
    // machine with few cores. The cap only guards against a hang.
    constexpr uint64_t kOtherRounds = 200000;

    std::atomic<bool> writers_done{false};
    std::atomic<int> violations{0};
    std::atomic<uint64_t> published{0};
    std::atomic<uint64_t> torn_down_by_others{0};
    std::atomic<uint64_t> mutations{0};
    std::atomic<uint64_t> pairs_observed{0};
    std::atomic<uint64_t> cursor_passes{0};
    std::vector<std::vector<Generation>> generations(kWriters);
    const auto violation = [&] {
        violations.fetch_add(1, std::memory_order_relaxed);
    };
    const auto is_member = [&](const std::string& key) {
        const auto members = route.GroupMembers("g1");
        return std::find(members.begin(), members.end(), key) != members.end();
    };
    const auto others_running = [&](uint64_t i) {
        return i < kOtherRounds &&
               !writers_done.load(std::memory_order_relaxed);
    };

    std::vector<std::thread> threads;
    // Writers publish into a key that holds nothing and tear their own
    // publication down, unless another thread got to it first. A publication
    // is a member from the moment it is published until it is torn down.
    for (int w = 0; w < kWriters; ++w) {
        threads.emplace_back([&, w] {
            for (int round = 0; round < kWriterRounds; ++round) {
                const std::string& key = keys[(round + w) % 2];
                ObjectRef ref;
                {
                    auto guard = route.WriteKey(key);
                    if (guard.has_object()) {
                        continue;
                    }
                    (void)test::PublishEnvelope(guard, "g1");
                    ref = guard.ref();
                    if (!is_member(key)) {
                        violation();
                    }
                }
                generations[w].push_back(ref.generation);
                published.fetch_add(1, std::memory_order_relaxed);
                // Leave the object published for a moment, so the other
                // threads meet it.
                std::this_thread::yield();
                if (auto guard = route.Write(ref)) {
                    guard->TearDown();
                    if (is_member(key)) {
                        violation();
                    }
                }
            }
        });
    }
    // Stale removers judge a publication under a read guard, let go, and come
    // back by ref: by then it may have been torn down or replaced, and only
    // the publication they judged may be torn down.
    for (int r = 0; r < 2; ++r) {
        threads.emplace_back([&, r] {
            for (uint64_t i = r; others_running(i); ++i) {
                std::this_thread::yield();
                std::optional<ObjectRef> ref;
                if (auto guard = route.Read(keys[i % 2])) {
                    ref = guard->ref();
                }
                if (!ref) {
                    continue;
                }
                std::this_thread::yield();
                if (auto guard = route.Write(*ref)) {
                    if (guard->generation() != ref->generation) {
                        violation();
                    }
                    guard->TearDown();
                    torn_down_by_others.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    // Mutators only ever run on a published object, and that object is a
    // member of its group for as long as they hold its key.
    for (int m = 0; m < 2; ++m) {
        threads.emplace_back([&, m] {
            for (uint64_t i = m; others_running(i); ++i) {
                std::this_thread::yield();
                const std::string& key = keys[i % 2];
                if (auto guard = route.Write(key)) {
                    mutations.fetch_add(1, std::memory_order_relaxed);
                    if (!is_member(key)) {
                        violation();
                    }
                    guard->state().is_processing =
                        !guard->state().is_processing;
                }
            }
        });
    }
    // An observer holds both keys at once. That breaks the one-slot-per-thread
    // rule only in a way that cannot deadlock here: it takes them shared and
    // always k1 before k2, and no other thread holds two slots. Two live
    // members of one group joined the same group instance, so they carry one
    // lease; a publication observed half-wired would carry the lease its
    // envelope was constructed with instead.
    threads.emplace_back([&] {
        for (uint64_t i = 0; others_running(i); ++i) {
            std::this_thread::yield();
            const auto first = route.Read("k1");
            if (!first) {
                continue;
            }
            const auto second = route.Read("k2");
            if (!second) {
                continue;
            }
            pairs_observed.fetch_add(1, std::memory_order_relaxed);
            if (first->metadata().lease_ != second->metadata().lease_ ||
                !is_member("k1") || !is_member("k2")) {
                violation();
            }
        }
    });
    // A cursor visits each key at most once per pass, whatever is published
    // or torn down meanwhile.
    threads.emplace_back([&] {
        for (uint64_t i = 0; others_running(i); ++i) {
            std::this_thread::yield();
            std::vector<std::string> visited;
            for (auto object : route.ReadCursor()) {
                if (object.generation() == 0 ||
                    object.metadata().user_key != object.key()) {
                    violation();
                }
                visited.push_back(object.key());
            }
            std::sort(visited.begin(), visited.end());
            if (std::adjacent_find(visited.begin(), visited.end()) !=
                visited.end()) {
                violation();
            }
            cursor_passes.fetch_add(1, std::memory_order_relaxed);
        }
    });
    for (int w = 0; w < kWriters; ++w) {
        threads[w].join();
    }
    writers_done.store(true, std::memory_order_relaxed);
    for (size_t t = kWriters; t < threads.size(); ++t) {
        threads[t].join();
    }

    EXPECT_EQ(violations.load(), 0);
    // Every racing path actually ran, so the run was not trivially quiet.
    EXPECT_GT(published.load(), 0u);
    EXPECT_GT(torn_down_by_others.load(), 0u);
    EXPECT_GT(mutations.load(), 0u);
    EXPECT_GT(pairs_observed.load(), 0u);
    EXPECT_GT(cursor_passes.load(), 0u);

    // No two publications got the same generation.
    std::vector<Generation> all;
    for (const auto& per_writer : generations) {
        all.insert(all.end(), per_writer.begin(), per_writer.end());
    }
    std::sort(all.begin(), all.end());
    EXPECT_EQ(std::adjacent_find(all.begin(), all.end()), all.end());

    // At rest, the group lists exactly the keys that hold an object, the count
    // agrees, and every slot left holds an object.
    std::vector<std::string> routed;
    for (auto object : route.ReadCursor()) {
        routed.push_back(object.key());
    }
    EXPECT_EQ(SortedMembers(route, "g1"), Sorted(routed));
    EXPECT_EQ(route.ObjectCount(), routed.size());
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), routed.size());

    for (const auto& key : routed) {
        EXPECT_TRUE(TearDownKey(route, key));
    }
    EXPECT_TRUE(route.Empty());
    EXPECT_EQ(ObjectRouteTestPeer::SlotCount(route), 0u);
}

}  // namespace
}  // namespace mooncake
