#include "client_session_registry.h"

#include <chrono>
#include <future>
#include <optional>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "client_session/client_session_registry_test_peer.h"
#include "client_session_event.h"
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

TEST(ClientSessionExpiryTest, OrderedTransitionsAndMultipleListeners) {
    std::vector<ClientSessionEvent> events;
    std::vector<State> second_listener;
    ClientSessionRegistry registry(10s, 20s);
    ASSERT_TRUE(registry.AddListener(
        [&](const auto& event) { events.push_back(event); }));
    ASSERT_TRUE(registry.AddListener(
        [&](const auto& event) { second_listener.push_back(event.current); }));
    auto record = Register(registry);
    Poll(registry, kInitial + 10s);
    ASSERT_EQ(events.size(), 1);
    EXPECT_EQ(Ping(registry, kInitial + 11s), ClientStatus::NEED_REMOUNT);
    EXPECT_EQ(record->state(), State::ACTIVE);
    // Recovery is queued by the heartbeat itself, before the next expiry.
    Poll(registry, kInitial + 21s);
    Poll(registry, kInitial + 41s);
    Poll(registry, kInitial + 100s);
    EXPECT_EQ(record->Observe(kInitial + 101s), Observation::REJECTED_OFFLINE);
    ASSERT_EQ(events.size(), 4);
    EXPECT_EQ(second_listener,
              (std::vector<State>{State::SUSPECTED, State::ACTIVE,
                                  State::SUSPECTED, State::OFFLINE}));
    EXPECT_EQ(events.front().previous, State::ACTIVE);
    EXPECT_EQ(events[1].previous, State::SUSPECTED);
    for (const auto& event : events) {
        EXPECT_EQ(event.client_id, kClient);
        EXPECT_EQ(event.session, record);
    }
}

TEST(ClientSessionExpiryTest, RefreshEmitsNoEvents) {
    std::vector<State> states;
    ClientSessionRegistry registry(10s, 20s);
    ASSERT_TRUE(registry.AddListener(
        [&](const auto& event) { states.push_back(event.current); }));
    Register(registry);
    EXPECT_EQ(Ping(registry, kInitial + 1s), ClientStatus::NEED_REMOUNT);
    Poll(registry, kInitial + 10s);
    EXPECT_TRUE(states.empty());
    Poll(registry, kInitial + 11s);
    Poll(registry, kInitial + 31s);
    EXPECT_EQ(states, (std::vector<State>{State::SUSPECTED, State::OFFLINE}));
}

TEST(ClientSessionExpiryTest,
     RetiredSessionIsVisibleToListenerAndListenerIsUnlocked) {
    ClientSessionRegistryTestPeer::Record record;
    bool notified = false;
    ClientSessionRegistry registry(10s, 20s);
    ASSERT_TRUE(registry.AddListener([&](const auto& event) {
        // The registry and operation locks must be released before invoking
        // a listener.
        EXPECT_EQ(ClientSessionRegistryTestPeer::Find(registry, kClient),
                  record);
        const bool offline = event.current == State::OFFLINE;
        // OFFLINE is the cleanup barrier, with no separate reservation.
        EXPECT_EQ(registry.HasRetiredSession(), offline);
        EXPECT_EQ(registry.TryAcquireRetainingSession(kClient).has_value(),
                  !offline);
        notified = notified || offline;
    }));
    record = Register(registry);
    Poll(registry, kInitial + 10s);
    Poll(registry, kInitial + 30s);
    EXPECT_TRUE(notified);
    // Only removal lifts the barrier, not event delivery.
    EXPECT_TRUE(registry.HasRetiredSession());
    EXPECT_TRUE(registry.Remove(kClient, record));
    EXPECT_FALSE(registry.HasRetiredSession());
}

TEST(ClientSessionExpiryTest, StopPreservesInFlightRetirementNotification) {
    int offline_events = 0;
    ClientSessionRegistry registry(0s, 0s);
    ASSERT_TRUE(registry.AddListener([&](const auto& event) {
        if (event.current == State::OFFLINE) {
            ++offline_events;
        }
    }));
    auto record = Register(registry);
    Poll(registry, Clock::now());
    ASSERT_EQ(record->state(), State::SUSPECTED);
    auto guard = registry.TryAcquireRetainingSession(kClient);
    ASSERT_TRUE(guard);
    // Make the next pass queue for the slot instead of skipping it, so the
    // retirement below is published from inside a pass that Stop must await.
    for (int pass = 0; pass < 3; ++pass) {
        Poll(registry, Clock::now());
    }
    ASSERT_EQ(record->state(), State::SUSPECTED);
    registry.Start();
    auto stopped = std::async(std::launch::async, [&] { registry.Stop(); });
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    guard.reset();
    stopped.get();
    // Stop drains what the awaited pass published instead of dropping it.
    EXPECT_EQ(record->state(), State::OFFLINE);
    EXPECT_EQ(offline_events, 1);
}

TEST(ClientSessionExpiryTest, OfflineClearsHostHintBeforeNotifyingListeners) {
    bool offline_notified = false;
    ClientSessionRegistry registry(10s, 20s);
    ASSERT_TRUE(registry.AddListener([&](const auto& event) {
        if (event.current == State::OFFLINE) {
            EXPECT_TRUE(registry.GetHostId(kClient).empty());
            offline_notified = true;
        } else {
            EXPECT_EQ(registry.GetHostId(kClient), "host-a");
        }
    }));
    Register(registry);
    registry.UpdateHostId(kClient, "host-a");
    Poll(registry, kInitial + 10s);
    (void)Ping(registry, kInitial + 11s);
    Poll(registry, kInitial + 21s);
    Poll(registry, kInitial + 41s);
    EXPECT_TRUE(offline_notified);
}

TEST(ClientSessionExpiryTest, OfflineDropsReadinessWithoutWaitingForDelivery) {
    ClientSessionRegistry registry(10s, 20s);
    auto record = Register(registry);
    {
        auto remount = registry.BeginRemount(kClient);
        remount->UpdateHostId("host-a");
        remount->Commit();
    }
    // Expire without delivering: the registry's own state for a terminal
    // incarnation must not depend on the delivery thread getting to the event.
    ClientSessionRegistryTestPeer::ExpireSessions(registry, kInitial + 10s);
    EXPECT_EQ(registry.GetHostId(kClient), "host-a");
    ClientSessionRegistryTestPeer::ExpireSessions(registry, kInitial + 30s);
    ASSERT_EQ(record->state(), State::OFFLINE);
    EXPECT_TRUE(registry.GetHostId(kClient).empty());
    ASSERT_TRUE(registry.Remove(kClient, record));
    // The replacement starts without the old incarnation's readiness, and
    // delivering the old OFFLINE event afterwards does not touch it.
    {
        auto remount = registry.BeginRemount(kClient);
        EXPECT_TRUE(remount->NeedsRemount());
        remount->UpdateHostId("new-host");
        remount->Commit();
    }
    ClientSessionRegistryTestPeer::Drain(registry);
    EXPECT_EQ(registry.GetHostId(kClient), "new-host");
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::OK);
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
    // After that the scan queues for the slot instead of skipping forever:
    // otherwise a session admitted without pause -- typically a dead client
    // that others keep holding as a replication source -- never expires.
    auto expiry =
        std::async(std::launch::async, [&] { Poll(registry, kInitial + 10s); });
    EXPECT_EQ(expiry.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(record->state(), State::ACTIVE);
    // New admissions wait behind the queued transition, so a steady stream of
    // them cannot keep the slot busy indefinitely.
    auto admitted = std::async(std::launch::async, [&] {
        return registry.TryAcquireRetainingSession(kClient).has_value();
    });
    EXPECT_EQ(admitted.wait_for(50ms), std::future_status::timeout);
    guard.reset();
    ASSERT_EQ(expiry.wait_for(5s), std::future_status::ready);
    expiry.get();
    EXPECT_EQ(record->state(), State::SUSPECTED);
    EXPECT_TRUE(admitted.get());
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

}  // namespace
}  // namespace mooncake::test
