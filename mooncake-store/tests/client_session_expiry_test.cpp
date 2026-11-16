#include "client_session_registry.h"

#include <chrono>
#include <future>
#include <optional>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "client_session/client_session_registry_test_peer.h"
#include "replica.h"

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;
using State = ClientLivenessState;
using Observation = ClientLivenessObservation;
using Clock = ClientSessionRegistry::Clock;
const UUID kClient{1, 2};
const ClientLivenessRecord::TimePoint kInitial{};

// One deterministic tick: evaluate, then deliver on the caller's thread.
void Poll(ClientSessionRegistry& registry, Clock::time_point now) {
    ClientSessionRegistryTestPeer::Poll(registry, now);
}

// A heartbeat at an explicit time.
ClientStatus Ping(ClientSessionRegistry& registry, Clock::time_point now) {
    return ClientSessionRegistryTestPeer::Ping(registry, kClient, now);
}

ClientSessionRegistryTestPeer::Record Register(ClientSessionRegistry& registry,
                                               const UUID& id = kClient) {
    {
        auto registration = registry.BeginRegistration(id, kInitial);
        EXPECT_TRUE(registration);
        if (registration) {
            registration->Commit();
        }
    }
    return ClientSessionRegistryTestPeer::Find(registry, id);
}

TEST(ClientSessionExpiryTest,
     SessionAdmissionDefersExpiryWithoutBlockingHeartbeat) {
    const auto check = [](auto acquire) {
        ClientSessionRegistry registry(10s, 20s);
        auto record = Register(registry);
        const UUID other{3, 4};
        auto other_record = Register(registry, other);
        {
            auto remount = registry.BeginRemount(kClient);
            remount->Commit();
        }
        {
            auto guard = acquire(registry);
            ASSERT_TRUE(guard);
            auto moved = std::move(*guard);
            guard.reset();
            // A long admitted operation must not stall monitoring other
            // clients, nor prevent its own heartbeat from completing.
            std::async(std::launch::async, [&] {
                Poll(registry, kInitial + 10s);
            }).get();
            EXPECT_EQ(moved.Session()->state(), State::ACTIVE);
            EXPECT_EQ(other_record->state(), State::SUSPECTED);
            auto ping = std::async(std::launch::async,
                                   [&] { return registry.Ping(kClient); });
            const auto status = ping.wait_for(1s);
            EXPECT_EQ(status, std::future_status::ready);
            // Release admission before joining even on failure.
            std::optional<ClientSessionRegistry::SessionGuard> held(
                std::move(moved));
            held.reset();
            EXPECT_EQ(ping.get(), ClientStatus::OK);
        }
        const auto now = Clock::now();
        Poll(registry, now + 10s);
        EXPECT_EQ(record->state(), State::SUSPECTED);
        Poll(registry, now + 30s);
        EXPECT_EQ(record->state(), State::OFFLINE);
    };
    check([](auto& registry) {
        return registry.TryAcquireServingSession(kClient);
    });
    check([](auto& registry) {
        return registry.TryAcquireRetainingSession(kClient);
    });
}

TEST(ClientSessionExpiryTest, RemountOnlySerializesItsOwnIncarnation) {
    ClientSessionRegistry registry(10s, 20s);
    auto record = Register(registry);
    const UUID other{3, 4};
    auto other_record = Register(registry, other);
    std::future<bool> same;
    std::future<bool> different;
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        same = std::async(std::launch::async, [&] {
            return registry.BeginRemount(kClient).has_value();
        });
        different = std::async(std::launch::async, [&] {
            return registry.BeginRemount(other).has_value();
        });
        EXPECT_EQ(same.wait_for(20ms), std::future_status::timeout);
        EXPECT_EQ(different.wait_for(1s), std::future_status::ready);
        // A held remount must neither expire nor stop monitoring other owners.
        std::async(std::launch::async, [&] {
            Poll(registry, kInitial + 10s);
        }).get();
        EXPECT_EQ(record->state(), State::ACTIVE);
        EXPECT_EQ(other_record->state(), State::SUSPECTED);
        std::async(std::launch::async, [&] {
            Poll(registry, kInitial + 30s);
        }).get();
        EXPECT_EQ(record->state(), State::ACTIVE);
        EXPECT_EQ(other_record->state(), State::OFFLINE);
    }
    EXPECT_TRUE(same.get());
    EXPECT_TRUE(different.get());
}

TEST(ClientSessionExpiryTest, ExpiryQueuesBehindPersistentlyAdmittedSlots) {
    ClientSessionRegistry registry(10s, 20s);
    auto record = Register(registry);
    auto guard = registry.TryAcquireRetainingSession(kClient);
    ASSERT_TRUE(guard);
    // A busy slot is skipped for a few passes, so a short operation never
    // pays for a transition it happens to overlap.
    for (int pass = 0; pass < 3; ++pass) {
        Poll(registry, kInitial + 10s);
        ASSERT_EQ(record->state(), State::ACTIVE);
    }
    // Once overdue past the grace passes, the scanner closes this client's
    // admission turnstile and returns immediately without blocking on guard.
    auto drain_poll =
        std::async(std::launch::async, [&] { Poll(registry, kInitial + 10s); });
    ASSERT_EQ(drain_poll.wait_for(1s), std::future_status::ready);
    drain_poll.get();
    EXPECT_EQ(record->state(), State::ACTIVE);
    // New admissions wait behind the closed turnstile, so a steady stream of
    // them cannot keep the slot busy indefinitely.
    auto admitted = std::async(std::launch::async, [&] {
        return registry.TryAcquireRetainingSession(kClient).has_value();
    });
    EXPECT_EQ(admitted.wait_for(50ms), std::future_status::timeout);
    guard.reset();
    Poll(registry, kInitial + 10s);
    EXPECT_EQ(record->state(), State::SUSPECTED);
    ASSERT_EQ(admitted.wait_for(1s), std::future_status::ready);
    EXPECT_TRUE(admitted.get());
}

TEST(ClientSessionExpiryTest,
     BusySlotDoesNotStallOtherClientExpiryOrRestoreAndHeartbeatCancelsDrain) {
    ClientSessionRegistry registry(10s, 20s);
    auto busy_record = Register(registry, kClient);
    const UUID other{3, 4};
    auto other_record = Register(registry, other);

    auto guard = registry.TryAcquireServingSession(kClient);
    ASSERT_TRUE(guard);

    // Client A stays persistently busy for 5 overdue passes and enters drain
    // mode, yet every scan returns without blocking and Client B still suspects
    // and expires independently.
    for (int pass = 0; pass < 5; ++pass) {
        auto poll = std::async(std::launch::async,
                               [&] { Poll(registry, kInitial + 10s); });
        ASSERT_EQ(poll.wait_for(1s), std::future_status::ready);
        poll.get();
        EXPECT_EQ(busy_record->state(), State::ACTIVE);
    }
    EXPECT_EQ(other_record->state(), State::SUSPECTED);

    auto offline_poll =
        std::async(std::launch::async, [&] { Poll(registry, kInitial + 30s); });
    ASSERT_EQ(offline_poll.wait_for(1s), std::future_status::ready);
    offline_poll.get();
    EXPECT_EQ(busy_record->state(), State::ACTIVE);
    EXPECT_EQ(other_record->state(), State::OFFLINE);

    // BeginRestore must not be stalled behind a busy draining slot.
    auto restored = std::async(std::launch::async, [&] {
        auto restore = registry.BeginRestore();
        restore.Commit();
    });
    ASSERT_EQ(restored.wait_for(1s), std::future_status::ready);
    restored.get();

    // New admissions on Client A are parked behind the closed drain gate until
    // a heartbeat cancels the drain — even before guard releases.
    auto waiting_admission = std::async(std::launch::async, [&] {
        return registry.TryAcquireServingSession(kClient).has_value();
    });
    EXPECT_EQ(waiting_admission.wait_for(50ms), std::future_status::timeout);

    EXPECT_EQ(Ping(registry, kInitial + 31s), ClientStatus::NEED_REMOUNT);
    ASSERT_EQ(waiting_admission.wait_for(1s), std::future_status::ready);
    EXPECT_TRUE(waiting_admission.get());

    guard.reset();
}

TEST(ClientSessionExpiryTest, RestoreDefersExpiryWithoutHoldingRegistry) {
    ClientSessionRegistry registry(10s, 20s);
    auto record = Register(registry);
    {
        auto restore = registry.BeginRestore();
        std::async(std::launch::async, [&] {
            Poll(registry, kInitial + 10s);
        }).get();
        EXPECT_EQ(record->state(), State::ACTIVE);
        restore.Commit();
    }
    Poll(registry, kInitial + 10s);
    EXPECT_EQ(record->state(), State::SUSPECTED);
}

TEST(ClientSessionExpiryTest, LocalDiskCleanupUsesBoundSessionState) {
    ClientSessionRegistry registry(10s, 20s);
    auto record = Register(registry);
    Replica replica(kClient, 1024, "disk-endpoint", ReplicaStatus::COMPLETE,
                    record);
    EXPECT_FALSE(replica.has_stale_local_disk_client());
    Poll(registry, kInitial + 10s);
    ASSERT_EQ(record->state(), State::SUSPECTED);
    EXPECT_FALSE(replica.has_stale_local_disk_client());
    Poll(registry, kInitial + 30s);
    ASSERT_EQ(record->state(), State::OFFLINE);
    EXPECT_TRUE(replica.has_stale_local_disk_client());
}

TEST(ClientSessionExpiryTest,
     HealthyAdmittedSessionNeverQueuesExpiryOrBlocksAdmission) {
    ClientSessionRegistry registry(10s, 20s);
    auto record = Register(registry);
    auto guard = registry.TryAcquireServingSession(kClient);
    ASSERT_TRUE(guard);

    // Unexpired slots skip admission_gate/operation_mutex completely, so a
    // healthy busy client never hits kBusyExpiryPassesBeforeWaiting.
    for (int pass = 0; pass < 5; ++pass) {
        auto poll = std::async(std::launch::async,
                               [&] { Poll(registry, kInitial + 5s); });
        ASSERT_EQ(poll.wait_for(1s), std::future_status::ready);
        poll.get();
        EXPECT_EQ(record->state(), State::ACTIVE);
        auto concurrent = std::async(std::launch::async, [&] {
            return registry.TryAcquireServingSession(kClient).has_value();
        });
        ASSERT_EQ(concurrent.wait_for(1s), std::future_status::ready);
        EXPECT_TRUE(concurrent.get());
    }

    // Two overdue skips followed by a heartbeat must reset the busy streak.
    for (int pass = 0; pass < 2; ++pass) {
        Poll(registry, kInitial + 10s);
        ASSERT_EQ(record->state(), State::ACTIVE);
    }
    EXPECT_EQ(Ping(registry, kInitial + 11s), ClientStatus::NEED_REMOUNT);
    Poll(registry, kInitial + 15s);
    // Single overdue pass after the reset still uses try_lock and skips.
    auto skipped =
        std::async(std::launch::async, [&] { Poll(registry, kInitial + 21s); });
    ASSERT_EQ(skipped.wait_for(1s), std::future_status::ready);
    skipped.get();
    EXPECT_EQ(record->state(), State::ACTIVE);
}

}  // namespace
}  // namespace mooncake::test
