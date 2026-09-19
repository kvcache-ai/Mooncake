#include "client_session_manager.h"
#include "replica.h"

#include <chrono>
#include <future>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake::test {

class ClientSessionManagerTest : public ::testing::Test {
   protected:
    static void Poll(ClientSessionManager& manager,
                     ClientSessionManager::Clock::time_point now) {
        manager.Poll(now);
    }

    using Slot = ClientSessionManager::Slot;

    static Slot FindSlot(ClientSessionManager& manager, const UUID& client_id) {
        return manager.FindSlot(client_id);
    }

    static bool AcquireCurrentOperation(ClientSessionManager& manager,
                                        const UUID& client_id, const Slot& slot,
                                        bool try_lock = false) {
        return manager.AcquireCurrentOperation(client_id, slot, try_lock)
            .has_value();
    }
};

namespace {

using namespace std::chrono_literals;
using State = ClientLivenessState;
using Observation = ClientLivenessObservation;
const UUID kClient{1, 2};
const ClientLivenessRecord::TimePoint kInitial{};

ClientSessionManager::Record Register(ClientSessionManager& manager,
                                      const UUID& id = kClient) {
    {
        auto registration = manager.BeginRegistration(id, kInitial);
        EXPECT_TRUE(registration);
        if (registration) {
            registration->Commit();
        }
    }
    return manager.Find(id);
}

TEST_F(ClientSessionManagerTest, ListenerRegistrationClosesAtStart) {
    ClientSessionManager manager(10s, 20s);
    EXPECT_FALSE(manager.AddSessionListener({}));
    ASSERT_TRUE(manager.AddSessionListener([](const auto&) {}));
    manager.Start();
    EXPECT_FALSE(manager.AddSessionListener([](const auto&) {}));
    manager.Quiesce();
    EXPECT_FALSE(manager.AddSessionListener([](const auto&) {}));
    manager.Stop();
    EXPECT_FALSE(manager.AddSessionListener([](const auto&) {}));
    manager.Start();
    EXPECT_FALSE(manager.AddSessionListener([](const auto&) {}));
    manager.Stop();
}

TEST_F(ClientSessionManagerTest, ManualDeliveryFreezesListenersInOrder) {
    ClientSessionManager manager(10s, 20s);
    std::vector<int> order;
    ASSERT_TRUE(manager.AddSessionListener([&](const auto&) {
        order.push_back(1);
        // Registration from a callback must reject rather than deadlock or
        // invalidate the listener vector being traversed.
        EXPECT_FALSE(manager.AddSessionListener([](const auto&) {}));
    }));
    ASSERT_TRUE(
        manager.AddSessionListener([&](const auto&) { order.push_back(2); }));
    Register(manager);
    Poll(manager, kInitial + 10s);
    EXPECT_EQ(order, (std::vector<int>{1, 2}));
    EXPECT_FALSE(manager.AddSessionListener([](const auto&) {}));
    manager.Stop();
}

TEST_F(ClientSessionManagerTest, StopBeforeStartAlsoClosesRegistration) {
    ClientSessionManager manager(10s, 20s);
    manager.Stop();
    EXPECT_FALSE(manager.AddSessionListener([](const auto&) {}));
}

TEST_F(ClientSessionManagerTest, OrderedTransitionsAndMultipleListeners) {
    std::vector<ClientSessionEvent> events;
    std::vector<State> second_listener;
    int reservations = 0;
    ClientSessionManager manager(10s, 20s, [&] { ++reservations; });
    ASSERT_TRUE(manager.AddSessionListener(
        [&](const auto& event) { events.push_back(event); }));
    ASSERT_TRUE(manager.AddSessionListener(
        [&](const auto& event) { second_listener.push_back(event.current); }));
    auto record = Register(manager);
    Poll(manager, kInitial + 10s);
    ASSERT_EQ(events.size(), 1);
    EXPECT_EQ(record->Observe(kInitial + 11s), Observation::RECOVERED_ACTIVE);
    // Recovery is queued under the transition lock, before the next expiry.
    Poll(manager, kInitial + 21s);
    Poll(manager, kInitial + 41s);
    Poll(manager, kInitial + 100s);
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
    EXPECT_EQ(reservations, 1);
}

TEST_F(ClientSessionManagerTest, FailedOperationAndRefreshEmitNoEvents) {
    std::vector<State> states;
    ClientSessionManager manager(10s, 20s);
    ASSERT_TRUE(manager.AddSessionListener(
        [&](const auto& event) { states.push_back(event.current); }));
    auto record = Register(manager);
    EXPECT_EQ(record->Observe(kInitial + 1s), Observation::REFRESHED_ACTIVE);
    Poll(manager, kInitial + 11s);
    EXPECT_EQ(record->ObserveAndRun(kInitial + 12s, [] { return false; }),
              Observation::OBSERVATION_WITHHELD);
    Poll(manager, kInitial + 31s);
    EXPECT_EQ(states, (std::vector<State>{State::SUSPECTED, State::OFFLINE}));
}

TEST_F(ClientSessionManagerTest,
       ReservationPrecedesOfflineAndListenerIsUnlocked) {
    ClientSessionManager::Record record;
    ClientSessionManager* manager_ptr = nullptr;
    bool reserved = false;
    bool notified = false;
    ClientSessionManager manager(10s, 20s, [&] {
        EXPECT_EQ(record->state(), State::SUSPECTED);
        reserved = true;
    });
    ASSERT_TRUE(manager.AddSessionListener([&](const auto& event) {
        // Both locks must be released before invoking a listener.
        EXPECT_EQ(manager_ptr->Find(kClient), record);
        if (event.current == State::OFFLINE) {
            EXPECT_TRUE(reserved);
            EXPECT_FALSE(record->TryAcquireRetainingGuard());
            notified = true;
        } else {
            EXPECT_TRUE(record->TryAcquireRetainingGuard());
        }
    }));
    manager_ptr = &manager;
    record = Register(manager);
    Poll(manager, kInitial + 10s);
    Poll(manager, kInitial + 30s);
    EXPECT_TRUE(notified);
}

TEST_F(ClientSessionManagerTest, OldIncarnationCannotRemoveNewRegistration) {
    ClientSessionManager manager(10s, 20s);
    auto old = Register(manager);
    Poll(manager, kInitial + 10s);
    Poll(manager, kInitial + 30s);
    EXPECT_FALSE(manager.Find(kClient)->ShouldRetainResources());
    EXPECT_TRUE(manager.Remove(kClient, old));
    const auto current = Register(manager);
    EXPECT_NE(current, old);
    EXPECT_FALSE(manager.Remove(kClient, old));
    EXPECT_EQ(manager.Find(kClient), current);
    EXPECT_TRUE(manager.Find(kClient)->ShouldRetainResources());
}

TEST_F(ClientSessionManagerTest,
       RestoreBindsRecordsWithoutSyntheticTransitions) {
    std::vector<ClientSessionEvent> events;
    ClientSessionManager manager(10s, 20s);
    ASSERT_TRUE(manager.AddSessionListener(
        [&](const auto& event) { events.push_back(event); }));
    auto old = Register(manager);
    auto restored = std::make_shared<ClientLivenessRecord>(kInitial);
    {
        manager.Reset({{kClient, restored}});
    }
    Poll(manager, kInitial + 1s);
    EXPECT_TRUE(events.empty());
    // Records can outlive the registry, but no longer notify its listeners.
    (void)old->Evaluate(kInitial + 10s, 10s, 20s);
    Poll(manager, kInitial + 10s);
    ASSERT_EQ(events.size(), 1);
    EXPECT_EQ(events.front().session, restored);
}

TEST_F(ClientSessionManagerTest, ThreadDeliversRecoveryAndStopDrainsEvents) {
    std::promise<void> recovered;
    auto delivered = recovered.get_future();
    std::vector<State> states;
    ClientSessionManager manager(10s, 20s);
    ASSERT_TRUE(manager.AddSessionListener([&](const auto& event) {
        states.push_back(event.current);
        if (event.current == State::ACTIVE) {
            recovered.set_value();
        }
    }));
    auto record = Register(manager);
    Poll(manager, kInitial + 10s);
    EXPECT_EQ(record->Observe(ClientLivenessRecord::Clock::now()),
              Observation::RECOVERED_ACTIVE);
    manager.Start();
    EXPECT_EQ(delivered.wait_for(5s), std::future_status::ready);
    manager.Stop();
    EXPECT_EQ(states, (std::vector<State>{State::SUSPECTED, State::ACTIVE}));
    manager.Stop();
}

TEST_F(ClientSessionManagerTest, StopPreservesInFlightRetirementNotification) {
    std::promise<void> entered;
    auto reserved = entered.get_future();
    std::promise<void> release;
    auto proceed = release.get_future();
    int offline_events = 0;
    ClientSessionManager manager(10s, 20s, [&] {
        entered.set_value();
        proceed.wait();
    });
    ASSERT_TRUE(manager.AddSessionListener([&](const auto& event) {
        if (event.current == State::OFFLINE) {
            ++offline_events;
        }
    }));
    Register(manager);
    Poll(manager, kInitial + 10s);
    manager.Start();
    const auto reservation_status = reserved.wait_for(5s);
    if (reservation_status != std::future_status::ready) {
        release.set_value();
        manager.Stop();
        FAIL() << "monitor did not enter retirement";
    }
    auto stopped = std::async(std::launch::async, [&] { manager.Stop(); });
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    stopped.get();
    EXPECT_EQ(offline_events, 1);
}

TEST_F(ClientSessionManagerTest, StopDrainsQueuedEventsAndRejectsLaterOnes) {
    std::vector<State> states;
    ClientSessionManager manager(10s, 20s);
    ASSERT_TRUE(manager.AddSessionListener(
        [&](const auto& event) { states.push_back(event.current); }));
    auto record = Register(manager);
    Poll(manager, kInitial + 10s);
    (void)record->Observe(kInitial + 11s);
    manager.Stop();
    EXPECT_EQ(states, (std::vector<State>{State::SUSPECTED, State::ACTIVE}));
    (void)record->Evaluate(kInitial + 21s, 10s, 20s);
    manager.Stop();
    EXPECT_EQ(states.size(), 2);
}

TEST_F(ClientSessionManagerTest, HostHintsAreInternallySynchronized) {
    ClientSessionManager manager(10s, 20s);
    EXPECT_TRUE(manager.GetHostId(kClient).empty());
    manager.UpdateHostId(kClient, "host-a");
    manager.UpdateHostId(kClient, "");
    EXPECT_EQ(manager.GetHostId(kClient), "host-a");
    // Remount can update a hint without recursively taking the registry lock.
    {
        auto registration = manager.BeginRegistration(kClient);
        ASSERT_TRUE(registration);
        registration->UpdateHostId("host-b");
        registration->UpdateHostId("");
        EXPECT_EQ(manager.GetHostId(kClient), "host-b");
        registration->Commit();
    }
    EXPECT_TRUE(manager.GetHostId(UUID{3, 4}).empty());
    manager.Reset();
    EXPECT_TRUE(manager.GetHostId(kClient).empty());
}

TEST_F(ClientSessionManagerTest,
       MovedRegistrationUpdatesOnlyItsClientHostHint) {
    ClientSessionManager manager(10s, 20s);
    const UUID other{3, 4};
    manager.UpdateHostId(other, "other-host");
    {
        auto original = manager.BeginRemount(kClient);
        auto remount = std::move(original);
        ASSERT_TRUE(remount);
        EXPECT_FALSE(original->Session());
        remount->UpdateHostId("host-a");
        EXPECT_EQ(manager.GetHostId(kClient), "host-a");
        EXPECT_EQ(manager.GetHostId(other), "other-host");
        remount->Commit();
    }
    EXPECT_EQ(manager.GetHostId(kClient), "host-a");
}

TEST_F(ClientSessionManagerTest,
       OfflineClearsHostHintBeforeNotifyingListeners) {
    ClientSessionManager* manager_ptr = nullptr;
    bool offline_notified = false;
    ClientSessionManager manager(10s, 20s);
    ASSERT_TRUE(manager.AddSessionListener([&](const auto& event) {
        if (event.current == State::OFFLINE) {
            EXPECT_TRUE(manager_ptr->GetHostId(kClient).empty());
            offline_notified = true;
        } else {
            EXPECT_EQ(manager_ptr->GetHostId(kClient), "host-a");
        }
    }));
    manager_ptr = &manager;
    auto record = Register(manager);
    manager.UpdateHostId(kClient, "host-a");
    Poll(manager, kInitial + 10s);
    (void)record->Observe(kInitial + 11s);
    Poll(manager, kInitial + 21s);
    Poll(manager, kInitial + 41s);
    EXPECT_TRUE(offline_notified);
}

TEST_F(ClientSessionManagerTest, RemovingOldIncarnationPreservesNewHostHint) {
    ClientSessionManager manager(10s, 20s);
    auto old = Register(manager);
    manager.UpdateHostId(kClient, "old-host");
    EXPECT_TRUE(manager.Remove(kClient, old));
    EXPECT_TRUE(manager.GetHostId(kClient).empty());
    const auto current = Register(manager);
    manager.UpdateHostId(kClient, "new-host");
    EXPECT_FALSE(manager.Remove(kClient, old));
    EXPECT_EQ(manager.GetHostId(kClient), "new-host");
    manager.Reset({{kClient, current}});
    EXPECT_TRUE(manager.GetHostId(kClient).empty());
}

TEST_F(ClientSessionManagerTest, GuardsCombineLookupAndStateAdmission) {
    ClientSessionManager manager(10s, 20s);
    EXPECT_FALSE(manager.TryAcquireRetainingSession(kClient));
    EXPECT_FALSE(manager.TryAcquireServingSession(kClient));
    Register(manager);
    EXPECT_TRUE(manager.TryAcquireRetainingSession(kClient));
    EXPECT_TRUE(manager.TryAcquireServingSession(kClient));
    Poll(manager, kInitial + 10s);
    EXPECT_TRUE(manager.TryAcquireRetainingSession(kClient));
    EXPECT_FALSE(manager.TryAcquireServingSession(kClient));
    Poll(manager, kInitial + 30s);
    EXPECT_FALSE(manager.TryAcquireRetainingSession(kClient));
    EXPECT_FALSE(manager.TryAcquireServingSession(kClient));
}

TEST_F(ClientSessionManagerTest, SessionScopesExposeOnlyReadOnlyIdentity) {
    using Guard = ClientSessionManager::SessionGuard;
    using Registration = ClientSessionManager::Registration;
    using Identity = std::shared_ptr<const ClientLivenessRecord>;
    static_assert(
        std::is_same_v<decltype(std::declval<const Guard&>().Session()),
                       Identity>);
    static_assert(
        std::is_same_v<decltype(std::declval<const Registration&>().Session()),
                       Identity>);
    static_assert(
        !std::is_constructible_v<Guard, ClientSessionManager::Record,
                                 ClientLivenessRecord::RetainingGuard>);
    static_assert(!std::is_copy_constructible_v<Guard>);
    static_assert(!std::is_move_assignable_v<Guard>);
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    auto guard = manager.TryAcquireServingSession(kClient);
    ASSERT_TRUE(guard);
    EXPECT_EQ(guard->Session(), record);
    EXPECT_TRUE(guard->Session()->IsServing());
}

TEST_F(ClientSessionManagerTest,
       SessionAdmissionBlocksTransitionsUntilReleased) {
    const auto check = [](auto acquire) {
        ClientSessionManager manager(10s, 20s);
        auto record = Register(manager);
        std::promise<void> attempted;
        auto started = attempted.get_future();
        std::future<ClientLivenessTransition> transition;
        {
            auto guard = acquire(manager);
            ASSERT_TRUE(guard);
            auto moved = std::move(*guard);
            guard.reset();
            transition = std::async(std::launch::async, [&] {
                attempted.set_value();
                return record->Evaluate(kInitial + 10s, 10s, 20s);
            });
            started.wait();
            EXPECT_EQ(transition.wait_for(20ms), std::future_status::timeout);
            EXPECT_EQ(moved.Session()->state(), State::ACTIVE);
        }
        EXPECT_EQ(transition.wait_for(1s), std::future_status::ready);
        EXPECT_EQ(transition.get(), ClientLivenessTransition::BECAME_SUSPECTED);
    };
    check([](auto& manager) {
        return manager.TryAcquireServingSession(kClient);
    });
    check([](auto& manager) {
        return manager.TryAcquireRetainingSession(kClient);
    });
}

TEST_F(ClientSessionManagerTest,
       RemovalWaitsForAdmissionWithoutHoldingRegistry) {
    const auto check = [](auto acquire) {
        auto manager = std::make_unique<ClientSessionManager>(10s, 20s);
        auto record = Register(*manager);
        const UUID other{3, 4};
        Register(*manager, other);
        std::future<bool> removed;
        std::future<ClientStatus> ping;
        ClientSessionSharedPtr identity;
        {
            auto guard = acquire(*manager);
            ASSERT_TRUE(guard);
            identity = guard->Session();
            removed = std::async(std::launch::async, [&] {
                return manager->Remove(kClient, record);
            });
            EXPECT_EQ(removed.wait_for(20ms), std::future_status::timeout);
            ping = std::async(std::launch::async,
                              [&] { return manager->Ping(other); });
            EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
        }
        EXPECT_TRUE(removed.get());
        EXPECT_EQ(ping.get(), ClientStatus::NEED_REMOUNT);
        EXPECT_FALSE(manager->Find(kClient));
        EXPECT_FALSE(manager->TryAcquireRetainingSession(kClient));
        manager.reset();
        EXPECT_EQ(identity, record);
    };
    check([](auto& manager) {
        return manager.TryAcquireServingSession(kClient);
    });
    check([](auto& manager) {
        return manager.TryAcquireRetainingSession(kClient);
    });
}

TEST_F(ClientSessionManagerTest, LocalDiskCleanupUsesBoundSessionState) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    Replica replica(kClient, 1024, "disk-endpoint", ReplicaStatus::COMPLETE,
                    record);
    EXPECT_FALSE(replica.has_stale_local_disk_client());
    Poll(manager, kInitial + 10s);
    ASSERT_EQ(record->state(), State::SUSPECTED);
    EXPECT_FALSE(replica.has_stale_local_disk_client());
    Poll(manager, kInitial + 30s);
    ASSERT_EQ(record->state(), State::OFFLINE);
    EXPECT_TRUE(replica.has_stale_local_disk_client());
}

TEST_F(ClientSessionManagerTest, LocalDiskCleanupDistinguishesIncarnations) {
    ClientSessionManager manager(10s, 20s);
    auto old_record = Register(manager);
    Replica old_replica(kClient, 1024, "disk-endpoint", ReplicaStatus::COMPLETE,
                        old_record);
    Poll(manager, kInitial + 10s);
    Poll(manager, kInitial + 30s);
    ASSERT_TRUE(manager.Remove(kClient, old_record));
    auto new_record = Register(manager);
    ASSERT_NE(old_record, new_record);
    Replica new_replica(kClient, 1024, "disk-endpoint", ReplicaStatus::COMPLETE,
                        new_record);
    EXPECT_TRUE(old_replica.has_stale_local_disk_client());
    EXPECT_FALSE(new_replica.has_stale_local_disk_client());
}

TEST_F(ClientSessionManagerTest, LocalDiskCleanupNeedsNoRegistryAccess) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    // Hold a registration scope while another thread classifies the newly
    // registered replica. No old client-ID snapshot or registry lookup
    // may be involved in the decision.
    std::future<bool> stale;
    std::future_status status;
    {
        auto registration = manager.BeginRegistration(kClient, kInitial);
        ASSERT_TRUE(registration);
        registration->Commit();
        stale = std::async(std::launch::async, [record] {
            Replica replica(kClient, 1024, "disk-endpoint",
                            ReplicaStatus::COMPLETE, record);
            return replica.has_stale_local_disk_client();
        });
        status = stale.wait_for(1s);
    }
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_FALSE(stale.get());
}

TEST_F(ClientSessionManagerTest, LocalDiskCleanupRejectsMissingSession) {
    Replica replica(kClient, 1024, "disk-endpoint", ReplicaStatus::COMPLETE);
    EXPECT_TRUE(replica.has_stale_local_disk_client());
}

TEST_F(ClientSessionManagerTest, RegistrationRollsBackOnlyProvisionalSessions) {
    ClientSessionManager manager(10s, 20s);
    const auto fail = [&] {
        auto registration = manager.BeginRegistration(kClient);
        EXPECT_TRUE(registration);
        registration->UpdateHostId("failed-host");
        return ErrorCode::INVALID_PARAMS;
    };
    EXPECT_EQ(fail(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(manager.Find(kClient));
    EXPECT_TRUE(manager.GetHostId(kClient).empty());
    auto record = Register(manager);
    Poll(manager, kInitial + 10s);
    EXPECT_EQ(fail(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(manager.Find(kClient), record);
    EXPECT_EQ(record->state(), State::SUSPECTED);
    EXPECT_EQ(manager.GetHostId(kClient), "failed-host");
    {
        auto registration = manager.BeginRegistration(kClient, kInitial + 12s);
        ASSERT_TRUE(registration);
        registration->Commit();
    }
    EXPECT_EQ(record->state(), State::ACTIVE);
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionManagerTest, ExceptionsDiscardOnlyUncommittedNewRecords) {
    ClientSessionManager manager(10s, 20s);
    const auto fail = [&] {
        auto registration = manager.BeginRegistration(kClient);
        EXPECT_TRUE(registration);
        throw std::runtime_error("mount failed");
    };
    EXPECT_THROW(fail(), std::runtime_error);
    EXPECT_FALSE(manager.Find(kClient));
    auto existing = Register(manager);
    EXPECT_THROW(fail(), std::runtime_error);
    EXPECT_EQ(manager.Find(kClient), existing);
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
    const UUID other{3, 4};
    const auto fail_after_commit = [&] {
        auto registration = manager.BeginRegistration(other);
        EXPECT_TRUE(registration);
        registration->Commit();
        throw std::runtime_error("work after commit failed");
    };
    EXPECT_THROW(fail_after_commit(), std::runtime_error);
    EXPECT_TRUE(manager.Find(other));
}

TEST_F(ClientSessionManagerTest,
       RemountCommitsReadinessWithoutRenewingLiveness) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    {
        auto remount = manager.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        EXPECT_EQ(remount->Session(), record);
        EXPECT_TRUE(remount->NeedsRemount());
        remount->Commit();
        EXPECT_FALSE(remount->NeedsRemount());
    }
    Poll(manager, kInitial + 10s);
    EXPECT_EQ(record->state(), State::SUSPECTED);
    {
        auto remount = manager.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        EXPECT_FALSE(remount->NeedsRemount());
        remount->Commit();
    }
    EXPECT_EQ(record->state(), State::SUSPECTED);
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::OK);
    EXPECT_EQ(record->state(), State::ACTIVE);
}

TEST_F(ClientSessionManagerTest, PingCombinesObservationWithRemountReadiness) {
    for (const bool remounted : {false, true}) {
        for (const auto state :
             {State::ACTIVE, State::SUSPECTED, State::OFFLINE}) {
            SCOPED_TRACE(::testing::Message() << "remounted=" << remounted
                                              << ", state=" << toString(state));
            ClientSessionManager manager(10s, 20s);
            EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
            EXPECT_FALSE(manager.Find(kClient));
            auto record = Register(manager);
            if (remounted) {
                auto remount = manager.BeginRemount(kClient);
                ASSERT_TRUE(remount);
                remount->Commit();
            }
            // Queue transitions without dispatch so OFFLINE rejection cannot
            // accidentally rely on the remount flag having been cleared.
            if (state != State::ACTIVE) {
                (void)record->Evaluate(kInitial + 10s, 10s, 20s);
            }
            if (state == State::OFFLINE) {
                (void)record->Evaluate(kInitial + 30s, 10s, 20s);
            }
            ASSERT_EQ(record->state(), state);
            EXPECT_EQ(manager.Ping(kClient),
                      remounted && state != State::OFFLINE
                          ? ClientStatus::OK
                          : ClientStatus::NEED_REMOUNT);
            EXPECT_EQ(record->state(),
                      state == State::OFFLINE ? State::OFFLINE : State::ACTIVE);
            Poll(manager, ClientSessionManager::Clock::now());
            // NEED_REMOUNT still refreshes a live session, but OFFLINE is
            // terminal regardless of whether the handshake was completed.
            EXPECT_EQ(record->state(),
                      state == State::OFFLINE ? State::OFFLINE : State::ACTIVE);
        }
    }
}

TEST_F(ClientSessionManagerTest, AbandonedRemountPreservesExistingReadiness) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    {
        auto remount = manager.BeginRemount(kClient);
        ASSERT_TRUE(remount);
    }
    EXPECT_EQ(manager.Find(kClient), record);
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
    {
        auto remount = manager.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        remount->Commit();
    }
    {
        auto remount = manager.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        EXPECT_FALSE(remount->NeedsRemount());
    }
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::OK);
}

TEST_F(ClientSessionManagerTest, TerminalSessionsRejectRegistrationAndRemount) {
    ClientSessionManager manager(10s, 20s);
    Register(manager);
    {
        auto remount = manager.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        remount->Commit();
    }
    Poll(manager, kInitial + 10s);
    Poll(manager, kInitial + 30s);
    EXPECT_FALSE(manager.BeginRegistration(kClient));
    EXPECT_FALSE(manager.BeginRemount(kClient));
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
    EXPECT_TRUE(manager.Remove(kClient, manager.Find(kClient)));
    Register(manager);
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionManagerTest, MovedRegistrationOwnsRollbackAndCommit) {
    static_assert(
        !std::is_copy_constructible_v<ClientSessionManager::Registration>);
    static_assert(
        !std::is_move_assignable_v<ClientSessionManager::Registration>);
    ClientSessionManager manager(10s, 20s);
    std::optional<ClientSessionManager::Registration> moved;
    std::weak_ptr<const ClientLivenessRecord> provisional;
    {
        auto registration = manager.BeginRegistration(kClient);
        ASSERT_TRUE(registration);
        provisional = registration->Session();
        moved.emplace(std::move(*registration));
        EXPECT_FALSE(registration->Session());
    }
    ASSERT_TRUE(moved->Session());
    EXPECT_FALSE(provisional.expired());
    moved.reset();
    EXPECT_TRUE(provisional.expired());
    EXPECT_FALSE(manager.Find(kClient));
    {
        auto remount = manager.BeginRemount(kClient);
        moved.emplace(std::move(*remount));
        moved->Commit();
    }
    moved.reset();
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::OK);
    {
        auto registration = manager.BeginRegistration(kClient);
        registration->Commit();
        moved.emplace(std::move(*registration));
    }
    moved.reset();
    EXPECT_TRUE(manager.Find(kClient));
}

TEST_F(ClientSessionManagerTest, MovingRegistrationPreservesCommitIntent) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    Poll(manager, kInitial + 10s);
    {
        auto original = manager.BeginRemount(kClient);
        ASSERT_TRUE(original);
        auto moved = std::move(*original);
        moved.Commit();
        EXPECT_FALSE(moved.NeedsRemount());
        EXPECT_EQ(moved.Session()->state(), State::SUSPECTED);
    }
    {
        auto original = manager.BeginRegistration(kClient, kInitial + 11s);
        ASSERT_TRUE(original);
        auto moved = std::move(*original);
        moved.Commit();
        EXPECT_EQ(moved.Session()->state(), State::ACTIVE);
    }
    Poll(manager, kInitial + 20s);
    EXPECT_EQ(record->state(), State::ACTIVE);
    Poll(manager, kInitial + 21s);
    EXPECT_EQ(record->state(), State::SUSPECTED);
}

TEST_F(ClientSessionManagerTest, RegistrationCannotCommitTwice) {
    ClientSessionManager manager(10s, 20s);
    {
        auto registration = manager.BeginRegistration(kClient);
        ASSERT_TRUE(registration);
        registration->Commit();
        EXPECT_DEATH(registration->Commit(), "committed_");
    }
    {
        auto remount = manager.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        remount->Commit();
        EXPECT_DEATH(remount->Commit(), "committed_");
    }
}

TEST_F(ClientSessionManagerTest, RegistrationAllowsPingBeforeScopeExit) {
    ClientSessionManager manager(10s, 20s);
    std::promise<void> attempted;
    auto started = attempted.get_future();
    std::future<ClientStatus> ping;
    {
        auto remount = manager.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        remount->Commit();
        ping = std::async(std::launch::async, [&] {
            attempted.set_value();
            return manager.Ping(kClient);
        });
        started.wait();
        EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
    }
    EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(ping.get(), ClientStatus::OK);
}

TEST_F(ClientSessionManagerTest, RemountOnlySerializesItsOwnIncarnation) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    const UUID other{3, 4};
    auto other_record = Register(manager, other);
    std::future<bool> same;
    std::future<bool> different;
    {
        auto remount = manager.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        same = std::async(std::launch::async, [&] {
            return manager.BeginRemount(kClient).has_value();
        });
        different = std::async(std::launch::async, [&] {
            return manager.BeginRemount(other).has_value();
        });
        EXPECT_EQ(same.wait_for(20ms), std::future_status::timeout);
        EXPECT_EQ(different.wait_for(1s), std::future_status::ready);
        // A held remount must neither expire nor stop monitoring other owners.
        std::async(std::launch::async, [&] {
            Poll(manager, kInitial + 10s);
        }).get();
        EXPECT_EQ(record->state(), State::ACTIVE);
        EXPECT_EQ(other_record->state(), State::SUSPECTED);
        std::async(std::launch::async, [&] {
            Poll(manager, kInitial + 30s);
        }).get();
        EXPECT_EQ(record->state(), State::ACTIVE);
        EXPECT_EQ(other_record->state(), State::OFFLINE);
    }
    EXPECT_TRUE(same.get());
    EXPECT_TRUE(different.get());
}

TEST_F(ClientSessionManagerTest,
       WaitingRemountRetriesAfterProvisionalRollback) {
    ClientSessionManager manager(10s, 20s);
    ClientSessionSharedPtr old;
    std::future<ClientSessionSharedPtr> next;
    {
        auto registration = manager.BeginRegistration(kClient);
        old = registration->Session();
        next = std::async(std::launch::async, [&] {
            auto remount = manager.BeginRemount(kClient);
            if (!remount) return ClientSessionSharedPtr{};
            remount->Commit();
            return remount->Session();
        });
        EXPECT_EQ(next.wait_for(20ms), std::future_status::timeout);
    }
    const auto replacement = next.get();
    ASSERT_TRUE(replacement);
    EXPECT_NE(old, replacement);
    EXPECT_EQ(manager.Find(kClient), replacement);
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::OK);
}

TEST_F(ClientSessionManagerTest, RegistrationCommitPreservesNewerHeartbeat) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    {
        auto registration = manager.BeginRegistration(kClient, kInitial);
        EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
        registration->Commit();
    }
    Poll(manager, ClientSessionManager::Clock::now());
    EXPECT_EQ(record->state(), State::ACTIVE);
}

TEST_F(ClientSessionManagerTest, RemountExcludesResourceAdmissionButNotPing) {
    ClientSessionManager manager(10s, 20s);
    Register(manager);
    std::future<bool> admitted;
    std::future<ClientStatus> ping;
    {
        auto remount = manager.BeginRemount(kClient);
        admitted = std::async(std::launch::async, [&] {
            return manager.TryAcquireRetainingSession(kClient).has_value();
        });
        ping = std::async(std::launch::async,
                          [&] { return manager.Ping(kClient); });
        EXPECT_EQ(admitted.wait_for(20ms), std::future_status::timeout);
        EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
        remount->Commit();
    }
    EXPECT_TRUE(admitted.get());
    EXPECT_EQ(ping.get(), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionManagerTest, RestoreWaitsWithoutBlockingRemountCommit) {
    ClientSessionManager manager(10s, 20s);
    Register(manager);
    const UUID other{3, 4};
    Register(manager, other);
    std::shared_mutex snapshot_mutex;
    std::future<void> restore;
    std::future<ClientStatus> ping;
    {
        auto remount = manager.BeginRemount(kClient);
        std::unique_lock snapshot_lock(snapshot_mutex);
        restore = std::async(std::launch::async, [&] {
            auto batch = manager.BeginRestore();
            std::unique_lock lock(snapshot_mutex);
            batch.Commit();
        });
        EXPECT_EQ(restore.wait_for(20ms), std::future_status::timeout);
        ping =
            std::async(std::launch::async, [&] { return manager.Ping(other); });
        EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
        remount->Commit();
        EXPECT_FALSE(remount->NeedsRemount());
    }
    EXPECT_EQ(restore.wait_for(1s), std::future_status::ready);
    restore.get();
    EXPECT_EQ(ping.get(), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionManagerTest, RestorePublishesOnlyCommittedStagedRecords) {
    std::vector<ClientSessionEvent> events;
    ClientSessionManager manager(10s, 20s);
    ASSERT_TRUE(manager.AddSessionListener(
        [&](const auto& event) { events.push_back(event); }));
    auto existing = Register(manager);
    const UUID other{3, 4};
    {
        auto restore = manager.BeginRestore();
        EXPECT_EQ(restore.FindOrCreate(kClient), existing);
        auto pending = restore.FindOrCreate(other);
        EXPECT_EQ(restore.FindOrCreate(other), pending);
    }
    EXPECT_FALSE(manager.Find(other));
    {
        auto restore = manager.BeginRestore();
        restore.FindOrCreate(other);
        restore.Commit();
    }
    EXPECT_EQ(manager.Find(kClient), existing);
    EXPECT_TRUE(manager.Find(other));
    Poll(manager, kInitial + 1s);
    EXPECT_TRUE(events.empty());
    EXPECT_EQ(manager.Ping(other), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionManagerTest, MovedRestoreOwnsPendingRecordsAndLocks) {
    static_assert(
        !std::is_copy_constructible_v<ClientSessionManager::RestoreBatch>);
    static_assert(
        !std::is_move_assignable_v<ClientSessionManager::RestoreBatch>);
    ClientSessionManager manager(10s, 20s);
    std::optional<ClientSessionManager::RestoreBatch> moved;
    std::weak_ptr<ClientLivenessRecord> provisional;
    {
        auto restore = manager.BeginRestore();
        provisional = restore.FindOrCreate(kClient);
        moved.emplace(std::move(restore));
    }
    EXPECT_FALSE(provisional.expired());
    moved.reset();
    EXPECT_TRUE(provisional.expired());
    EXPECT_FALSE(manager.Find(kClient));
    {
        auto restore = manager.BeginRestore();
        restore.FindOrCreate(kClient);
        moved.emplace(std::move(restore));
    }
    moved->Commit();
    moved.reset();
    EXPECT_TRUE(manager.Find(kClient));
}

TEST_F(ClientSessionManagerTest,
       RestoreExceptionReleasesLocksAndDiscardsStaging) {
    ClientSessionManager manager(10s, 20s);
    auto existing = Register(manager);
    std::shared_mutex snapshot_mutex;
    const UUID other{3, 4};
    const auto fail = [&] {
        auto restore = manager.BeginRestore();
        std::unique_lock snapshot_lock(snapshot_mutex);
        EXPECT_EQ(restore.FindOrCreate(kClient), existing);
        restore.FindOrCreate(other);
        throw std::runtime_error("restore failed");
    };
    EXPECT_THROW(fail(), std::runtime_error);
    EXPECT_FALSE(manager.Find(other));
    EXPECT_EQ(manager.Find(kClient), existing);
    std::unique_lock lock(snapshot_mutex, std::try_to_lock);
    EXPECT_TRUE(lock.owns_lock());
}

TEST_F(ClientSessionManagerTest, RemovalDoesNotAddALivenessState) {
    ClientSessionManager manager(10s, 20s);
    std::vector<ClientSessionEvent> events;
    ASSERT_TRUE(manager.AddSessionListener(
        [&](const auto& event) { events.push_back(event); }));
    auto old = Register(manager);
    {
        auto remount = manager.BeginRemount(kClient);
        remount->Commit();
    }
    ASSERT_EQ(old->Observe(kInitial + 1s), Observation::REFRESHED_ACTIVE);
    ASSERT_TRUE(manager.Remove(kClient, old));
    EXPECT_EQ(old->state(), State::ACTIVE);
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
    EXPECT_FALSE(manager.TryAcquireRetainingSession(kClient));
    // A retained record remains a normal liveness object, but its observer
    // no longer belongs to the manager.
    EXPECT_EQ(old->Observe(kInitial + 2s), Observation::REFRESHED_ACTIVE);
    EXPECT_EQ(old->Evaluate(kInitial + 12s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(old->Observe(kInitial + 13s), Observation::RECOVERED_ACTIVE);
    auto current = Register(manager);
    EXPECT_NE(old, current);
    Poll(manager, kInitial + 1s);
    EXPECT_TRUE(events.empty());
    EXPECT_EQ(manager.TryAcquireServingSession(kClient)->Session(), current);
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionManagerTest, WaitingAdmissionRejectsRolledBackRecord) {
    ClientSessionManager manager(10s, 20s);
    std::promise<void> attempted;
    auto started = attempted.get_future();
    std::future<bool> admitted;
    ClientSessionSharedPtr old;
    {
        auto registration = manager.BeginRegistration(kClient);
        old = registration->Session();
        admitted = std::async(std::launch::async, [&] {
            attempted.set_value();
            return manager.TryAcquireRetainingSession(kClient).has_value();
        });
        started.wait();
        EXPECT_EQ(admitted.wait_for(20ms), std::future_status::timeout);
    }
    EXPECT_FALSE(admitted.get());
    EXPECT_EQ(old->state(), State::ACTIVE);
    EXPECT_FALSE(manager.Find(kClient));
    auto current = Register(manager);
    EXPECT_NE(old, current);
    EXPECT_EQ(manager.TryAcquireRetainingSession(kClient)->Session(), current);
}

TEST_F(ClientSessionManagerTest, CurrentOperationValidatesIdentityNotLiveness) {
    ClientSessionManager manager(10s, 20s);
    EXPECT_FALSE(AcquireCurrentOperation(manager, kClient, {}));
    auto old = Register(manager);
    auto old_slot = FindSlot(manager, kClient);
    EXPECT_TRUE(AcquireCurrentOperation(manager, kClient, old_slot));
    ASSERT_TRUE(manager.Remove(kClient, old));
    EXPECT_FALSE(AcquireCurrentOperation(manager, kClient, old_slot));
    auto current = Register(manager);
    auto current_slot = FindSlot(manager, kClient);
    EXPECT_NE(old_slot, current_slot);
    EXPECT_FALSE(AcquireCurrentOperation(manager, kClient, old_slot));
    EXPECT_FALSE(AcquireCurrentOperation(manager, kClient, old_slot,
                                         /*try_lock=*/true));
    EXPECT_TRUE(AcquireCurrentOperation(manager, kClient, current_slot));
    Poll(manager, kInitial + 10s);
    Poll(manager, kInitial + 30s);
    // OFFLINE slots still belong to the registry until cleanup completes.
    // Operation validation must not prevent their removal.
    EXPECT_EQ(current->state(), State::OFFLINE);
    EXPECT_TRUE(AcquireCurrentOperation(manager, kClient, current_slot));
    EXPECT_FALSE(manager.TryAcquireRetainingSession(kClient));
    EXPECT_TRUE(manager.Remove(kClient, current));
}

TEST_F(ClientSessionManagerTest, CurrentOperationTryLockDoesNotWait) {
    ClientSessionManager manager(10s, 20s);
    Register(manager);
    auto slot = FindSlot(manager, kClient);
    std::future<bool> acquired;
    {
        auto remount = manager.BeginRemount(kClient);
        acquired = std::async(std::launch::async, [&] {
            return AcquireCurrentOperation(manager, kClient, slot,
                                           /*try_lock=*/true);
        });
        EXPECT_EQ(acquired.wait_for(1s), std::future_status::ready);
    }
    EXPECT_FALSE(acquired.get());
    EXPECT_TRUE(AcquireCurrentOperation(manager, kClient, slot,
                                        /*try_lock=*/true));
}

TEST_F(ClientSessionManagerTest, ResetRejectsOldSlotEvenWhenRecordIsReused) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    auto old_slot = FindSlot(manager, kClient);
    std::weak_ptr weak_old_slot = old_slot;
    {
        auto remount = manager.BeginRemount(kClient);
        remount->Commit();
    }
    manager.Reset({{kClient, record}});
    auto current_slot = FindSlot(manager, kClient);
    EXPECT_NE(old_slot, current_slot);
    EXPECT_EQ(old_slot->liveness, current_slot->liveness);
    EXPECT_FALSE(AcquireCurrentOperation(manager, kClient, old_slot));
    EXPECT_FALSE(AcquireCurrentOperation(manager, kClient, old_slot,
                                         /*try_lock=*/true));
    EXPECT_TRUE(AcquireCurrentOperation(manager, kClient, current_slot));
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
    old_slot.reset();
    // Read-only resource identities retain liveness, not obsolete slot locks.
    EXPECT_TRUE(weak_old_slot.expired());
    EXPECT_EQ(manager.Find(kClient), record);
}

TEST_F(ClientSessionManagerTest, RemovedSlotIsReleasedDespiteRetainedRecord) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    std::weak_ptr weak_slot = FindSlot(manager, kClient);
    EXPECT_FALSE(weak_slot.expired());
    ASSERT_TRUE(manager.Remove(kClient, record));
    EXPECT_TRUE(weak_slot.expired());
    EXPECT_EQ(record->state(), State::ACTIVE);
    EXPECT_TRUE(manager.SnapshotRecords().empty());
}

TEST_F(ClientSessionManagerTest, ConcurrentRemovalSucceedsOnlyOnce) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    std::promise<void> start;
    auto gate = start.get_future().share();
    const auto remove = [&] {
        gate.wait();
        return manager.Remove(kClient, record);
    };
    auto first = std::async(std::launch::async, remove);
    auto second = std::async(std::launch::async, remove);
    start.set_value();
    EXPECT_NE(first.get(), second.get());
    EXPECT_FALSE(manager.Find(kClient));
}

TEST_F(ClientSessionManagerTest, RestoreDoesNotBlockLookupOrHeartbeat) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    {
        auto remount = manager.BeginRemount(kClient);
        remount->Commit();
    }
    std::future<bool> lookup;
    const UUID other{3, 4};
    {
        auto restore = manager.BeginRestore();
        restore.FindOrCreate(other);
        lookup = std::async(std::launch::async, [&] {
            return manager.Find(kClient) == record && !manager.Find(other) &&
                   manager.Ping(kClient) == ClientStatus::OK;
        });
        EXPECT_EQ(lookup.wait_for(1s), std::future_status::ready);
        restore.Commit();
    }
    EXPECT_TRUE(lookup.get());
    EXPECT_TRUE(manager.Find(other));
}

TEST_F(ClientSessionManagerTest, RestoreDefersExpiryWithoutHoldingRegistry) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    {
        auto restore = manager.BeginRestore();
        std::async(std::launch::async, [&] {
            Poll(manager, kInitial + 10s);
        }).get();
        EXPECT_EQ(record->state(), State::ACTIVE);
        restore.Commit();
    }
    Poll(manager, kInitial + 10s);
    EXPECT_EQ(record->state(), State::SUSPECTED);
}

TEST_F(ClientSessionManagerTest, ResetPreservesRetainedIncarnationIdentity) {
    ClientSessionManager manager(10s, 20s);
    auto record = Register(manager);
    {
        auto remount = manager.BeginRemount(kClient);
        remount->Commit();
    }
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::OK);
    manager.Reset({{kClient, record}});
    EXPECT_EQ(manager.Find(kClient), record);
    EXPECT_TRUE(manager.TryAcquireServingSession(kClient));
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionManagerTest, OldOfflineEventDoesNotClearNewIncarnation) {
    ClientSessionManager manager(10s, 20s);
    auto old = Register(manager);
    (void)old->Evaluate(kInitial + 10s, 10s, 20s);
    (void)old->Evaluate(kInitial + 30s, 10s, 20s);
    ASSERT_TRUE(manager.Remove(kClient, old));
    {
        auto remount = manager.BeginRemount(kClient);
        remount->UpdateHostId("new-host");
        remount->Commit();
    }
    // Deliver the old record's queued OFFLINE after publishing its replacement.
    Poll(manager, ClientSessionManager::Clock::now());
    EXPECT_EQ(manager.GetHostId(kClient), "new-host");
    EXPECT_EQ(manager.Ping(kClient), ClientStatus::OK);
}

TEST_F(ClientSessionManagerTest, RecordCanOutliveManager) {
    ClientSessionManager::Record record;
    {
        ClientSessionManager manager(10s, 20s);
        record = Register(manager);
    }
    EXPECT_EQ(record->Evaluate(kInitial + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(record->Observe(kInitial + 11s), Observation::RECOVERED_ACTIVE);
}

}  // namespace
}  // namespace mooncake::test
