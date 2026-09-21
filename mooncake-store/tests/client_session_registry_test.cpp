#include "client_session_registry.h"

#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "client_session/client_session_registry_test_peer.h"
#include "replica.h"

namespace mooncake::test {

// Inheriting the peer lets these tests call its accessors unqualified; it
// stays the only boundary to the registry's internals.
class ClientSessionRegistryTest : public ::testing::Test,
                                  protected ClientSessionRegistryTestPeer {};

namespace {

using namespace std::chrono_literals;
using State = ClientLivenessState;
using Observation = ClientLivenessObservation;
using Transition = ClientLivenessTransition;
using Clock = ClientLivenessRecord::Clock;
const UUID kClient{1, 2};
const ClientLivenessRecord::TimePoint kInitial{};

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

// These tests step one record through its states directly, so they need no
// TTLs or scan thread; client_session_expiry_test covers those.
void Suspect(const ClientSessionRegistryTestPeer::Record& record) {
    EXPECT_EQ(record->Evaluate(Clock::now(), 0s, 0s),
              Transition::BECAME_SUSPECTED);
}

void Retire(const ClientSessionRegistryTestPeer::Record& record) {
    EXPECT_EQ(record->Evaluate(Clock::now(), 0s, 0s),
              Transition::BECAME_OFFLINE);
}

TEST_F(ClientSessionRegistryTest, HostHintsBelongToTheSession) {
    ClientSessionRegistry registry;
    EXPECT_TRUE(registry.GetHostId(kClient).empty());
    // A hint has nowhere to live before the client has a session.
    registry.UpdateHostId(kClient, "host-a");
    EXPECT_TRUE(registry.GetHostId(kClient).empty());
    Register(registry);
    registry.UpdateHostId(kClient, "host-a");
    registry.UpdateHostId(kClient, "");
    EXPECT_EQ(registry.GetHostId(kClient), "host-a");
    // Remount can update a hint without recursively taking the registry lock.
    {
        auto registration = registry.BeginRegistration(kClient);
        ASSERT_TRUE(registration);
        registration->UpdateHostId("host-b");
        registration->UpdateHostId("");
        EXPECT_EQ(registry.GetHostId(kClient), "host-b");
        registration->Commit();
    }
    EXPECT_TRUE(registry.GetHostId(UUID{3, 4}).empty());
    registry.Reset();
    EXPECT_TRUE(registry.GetHostId(kClient).empty());
}

TEST_F(ClientSessionRegistryTest,
       MovedRegistrationUpdatesOnlyItsClientHostHint) {
    ClientSessionRegistry registry;
    const UUID other{3, 4};
    Register(registry, other);
    registry.UpdateHostId(other, "other-host");
    {
        auto original = registry.BeginRemount(kClient);
        auto remount = std::move(original);
        ASSERT_TRUE(remount);
        EXPECT_FALSE(original->Session());
        remount->UpdateHostId("host-a");
        EXPECT_EQ(registry.GetHostId(kClient), "host-a");
        EXPECT_EQ(registry.GetHostId(other), "other-host");
        remount->Commit();
    }
    EXPECT_EQ(registry.GetHostId(kClient), "host-a");
}

TEST_F(ClientSessionRegistryTest, RemovingOldIncarnationPreservesNewHostHint) {
    ClientSessionRegistry registry;
    auto old = Register(registry);
    registry.UpdateHostId(kClient, "old-host");
    EXPECT_TRUE(registry.Remove(kClient, old));
    EXPECT_TRUE(registry.GetHostId(kClient).empty());
    const auto current = Register(registry);
    registry.UpdateHostId(kClient, "new-host");
    EXPECT_FALSE(registry.Remove(kClient, old));
    EXPECT_EQ(registry.GetHostId(kClient), "new-host");
    registry.Reset();
    EXPECT_TRUE(registry.GetHostId(kClient).empty());
    EXPECT_FALSE(Find(registry, kClient));
    EXPECT_EQ(current->state(), State::ACTIVE);
}

TEST_F(ClientSessionRegistryTest, OldIncarnationCannotRemoveNewRegistration) {
    ClientSessionRegistry registry;
    auto old = Register(registry);
    Suspect(old);
    Retire(old);
    EXPECT_FALSE(Find(registry, kClient)->ShouldRetainResources());
    EXPECT_TRUE(registry.Remove(kClient, old));
    const auto current = Register(registry);
    EXPECT_NE(current, old);
    EXPECT_FALSE(registry.Remove(kClient, old));
    EXPECT_EQ(Find(registry, kClient), current);
    EXPECT_TRUE(Find(registry, kClient)->ShouldRetainResources());
}

TEST_F(ClientSessionRegistryTest, GuardsCombineLookupAndStateAdmission) {
    ClientSessionRegistry registry;
    EXPECT_FALSE(registry.TryAcquireRetainingSession(kClient));
    EXPECT_FALSE(registry.TryAcquireServingSession(kClient));
    const auto record = Register(registry);
    EXPECT_TRUE(registry.TryAcquireRetainingSession(kClient));
    EXPECT_TRUE(registry.TryAcquireServingSession(kClient));
    Suspect(record);
    EXPECT_TRUE(registry.TryAcquireRetainingSession(kClient));
    EXPECT_FALSE(registry.TryAcquireServingSession(kClient));
    Retire(record);
    EXPECT_FALSE(registry.TryAcquireRetainingSession(kClient));
    EXPECT_FALSE(registry.TryAcquireServingSession(kClient));
}

TEST_F(ClientSessionRegistryTest, OperationsOnOneClientRunConcurrently) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    auto first = registry.TryAcquireServingSession(kClient);
    ASSERT_TRUE(first);
    const auto slot = FindSlot(registry, kClient);
    // A second operation on the same client must not queue behind the first.
    // Only a state transition excludes admission, so requests of one client
    // (and requests that hold it as a replication source) stay parallel.
    auto second = std::async(std::launch::async, [&] {
        return registry.TryAcquireRetainingSession(kClient).has_value() &&
               AcquireSharedOperation(registry, kClient, slot);
    });
    EXPECT_EQ(second.wait_for(1s), std::future_status::ready);
    EXPECT_TRUE(second.get());
    // Transitions still wait for every operation admitted before them.
    EXPECT_FALSE(AcquireExclusiveOperation(registry, kClient, slot,
                                           /*try_lock=*/true));
    first.reset();
    EXPECT_TRUE(AcquireExclusiveOperation(registry, kClient, slot,
                                          /*try_lock=*/true));
    EXPECT_EQ(record->state(), State::ACTIVE);
}

TEST_F(ClientSessionRegistryTest, SessionScopesExposeOnlyReadOnlyIdentity) {
    using Guard = ClientSessionRegistry::SessionGuard;
    using Registration = ClientSessionRegistry::Registration;
    using Identity = std::shared_ptr<const ClientLivenessRecord>;
    static_assert(
        std::is_same_v<decltype(std::declval<const Guard&>().Session()),
                       Identity>);
    static_assert(
        std::is_same_v<decltype(std::declval<const Registration&>().Session()),
                       Identity>);
    static_assert(!std::is_copy_constructible_v<Guard>);
    static_assert(!std::is_move_assignable_v<Guard>);
    ClientSessionRegistry registry;
    auto record = Register(registry);
    auto guard = registry.TryAcquireServingSession(kClient);
    ASSERT_TRUE(guard);
    EXPECT_EQ(guard->Session(), record);
    EXPECT_TRUE(guard->Session()->IsServing());
}

TEST_F(ClientSessionRegistryTest, RetainedSuspectedSessionAllowsRecovery) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    Suspect(record);
    ASSERT_EQ(record->state(), State::SUSPECTED);
    auto guard = registry.TryAcquireRetainingSession(kClient);
    ASSERT_TRUE(guard);
    auto ping =
        std::async(std::launch::async, [&] { return registry.Ping(kClient); });
    EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
    // Release before joining so a regression fails rather than hanging.
    guard.reset();
    EXPECT_EQ(ping.get(), ClientStatus::NEED_REMOUNT);
    EXPECT_EQ(record->state(), State::ACTIVE);
}

TEST_F(ClientSessionRegistryTest,
       RemovalWaitsForAdmissionWithoutHoldingRegistry) {
    const auto check = [](auto acquire) {
        auto registry = std::make_unique<ClientSessionRegistry>();
        auto record = Register(*registry);
        const UUID other{3, 4};
        Register(*registry, other);
        std::future<bool> removed;
        std::future<ClientStatus> ping;
        ClientSessionSharedPtr identity;
        {
            auto guard = acquire(*registry);
            ASSERT_TRUE(guard);
            identity = guard->Session();
            removed = std::async(std::launch::async, [&] {
                return registry->Remove(kClient, record);
            });
            EXPECT_EQ(removed.wait_for(20ms), std::future_status::timeout);
            ping = std::async(std::launch::async,
                              [&] { return registry->Ping(other); });
            EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
        }
        EXPECT_TRUE(removed.get());
        EXPECT_EQ(ping.get(), ClientStatus::NEED_REMOUNT);
        EXPECT_FALSE(Find(*registry, kClient));
        EXPECT_FALSE(registry->TryAcquireRetainingSession(kClient));
        registry.reset();
        EXPECT_EQ(identity, record);
    };
    check([](auto& registry) {
        return registry.TryAcquireServingSession(kClient);
    });
    check([](auto& registry) {
        return registry.TryAcquireRetainingSession(kClient);
    });
}

TEST_F(ClientSessionRegistryTest, LocalDiskCleanupNeedsNoRegistryAccess) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    // Hold a registration scope while another thread classifies the newly
    // registered replica. No old client-ID snapshot or registry lookup
    // may be involved in the decision.
    std::future<bool> stale;
    std::future_status status;
    {
        auto registration = registry.BeginRegistration(kClient, kInitial);
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

TEST_F(ClientSessionRegistryTest, LocalDiskCleanupRejectsMissingSession) {
    Replica replica(kClient, 1024, "disk-endpoint", ReplicaStatus::COMPLETE);
    EXPECT_TRUE(replica.has_stale_local_disk_client());
}

TEST_F(ClientSessionRegistryTest, LocalDiskCleanupDistinguishesIncarnations) {
    ClientSessionRegistry registry;
    auto old_record = Register(registry);
    Replica old_replica(kClient, 1024, "disk-endpoint", ReplicaStatus::COMPLETE,
                        old_record);
    Suspect(old_record);
    Retire(old_record);
    ASSERT_TRUE(registry.Remove(kClient, old_record));
    auto new_record = Register(registry);
    ASSERT_NE(old_record, new_record);
    Replica new_replica(kClient, 1024, "disk-endpoint", ReplicaStatus::COMPLETE,
                        new_record);
    EXPECT_TRUE(old_replica.has_stale_local_disk_client());
    EXPECT_FALSE(new_replica.has_stale_local_disk_client());
}

TEST_F(ClientSessionRegistryTest,
       RegistrationRollsBackOnlyProvisionalSessions) {
    ClientSessionRegistry registry;
    const auto fail = [&] {
        auto registration = registry.BeginRegistration(kClient);
        EXPECT_TRUE(registration);
        registration->UpdateHostId("failed-host");
        return ErrorCode::INVALID_PARAMS;
    };
    EXPECT_EQ(fail(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(Find(registry, kClient));
    EXPECT_TRUE(registry.GetHostId(kClient).empty());
    auto record = Register(registry);
    Suspect(record);
    EXPECT_EQ(fail(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(Find(registry, kClient), record);
    EXPECT_EQ(record->state(), State::SUSPECTED);
    EXPECT_EQ(registry.GetHostId(kClient), "failed-host");
    {
        auto registration = registry.BeginRegistration(kClient, kInitial + 12s);
        ASSERT_TRUE(registration);
        registration->Commit();
    }
    EXPECT_EQ(record->state(), State::ACTIVE);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionRegistryTest, ExceptionsDiscardOnlyUncommittedNewRecords) {
    ClientSessionRegistry registry;
    const auto fail = [&] {
        auto registration = registry.BeginRegistration(kClient);
        EXPECT_TRUE(registration);
        throw std::runtime_error("mount failed");
    };
    EXPECT_THROW(fail(), std::runtime_error);
    EXPECT_FALSE(Find(registry, kClient));
    auto existing = Register(registry);
    EXPECT_THROW(fail(), std::runtime_error);
    EXPECT_EQ(Find(registry, kClient), existing);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
    const UUID other{3, 4};
    const auto fail_after_commit = [&] {
        auto registration = registry.BeginRegistration(other);
        EXPECT_TRUE(registration);
        registration->Commit();
        throw std::runtime_error("work after commit failed");
    };
    EXPECT_THROW(fail_after_commit(), std::runtime_error);
    EXPECT_TRUE(Find(registry, other));
}

TEST_F(ClientSessionRegistryTest,
       RemountCommitsReadinessWithoutRenewingLiveness) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        EXPECT_EQ(remount->Session(), record);
        EXPECT_TRUE(remount->NeedsRemount());
        remount->Commit();
        EXPECT_FALSE(remount->NeedsRemount());
    }
    Suspect(record);
    EXPECT_EQ(record->state(), State::SUSPECTED);
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        EXPECT_FALSE(remount->NeedsRemount());
        remount->Commit();
    }
    EXPECT_EQ(record->state(), State::SUSPECTED);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::OK);
    EXPECT_EQ(record->state(), State::ACTIVE);
}

TEST_F(ClientSessionRegistryTest, PingCombinesObservationWithRemountReadiness) {
    for (const bool remounted : {false, true}) {
        for (const auto state :
             {State::ACTIVE, State::SUSPECTED, State::OFFLINE}) {
            SCOPED_TRACE(::testing::Message() << "remounted=" << remounted
                                              << ", state=" << toString(state));
            ClientSessionRegistry registry;
            EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
            EXPECT_FALSE(Find(registry, kClient));
            auto record = Register(registry);
            if (remounted) {
                auto remount = registry.BeginRemount(kClient);
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
            EXPECT_EQ(registry.Ping(kClient),
                      remounted && state != State::OFFLINE
                          ? ClientStatus::OK
                          : ClientStatus::NEED_REMOUNT);
            EXPECT_EQ(record->state(),
                      state == State::OFFLINE ? State::OFFLINE : State::ACTIVE);
            EXPECT_EQ(record->Evaluate(Clock::now(), 10s, 20s),
                      Transition::NONE);
            // NEED_REMOUNT still refreshes a live session, but OFFLINE is
            // terminal regardless of whether the handshake was completed.
            EXPECT_EQ(record->state(),
                      state == State::OFFLINE ? State::OFFLINE : State::ACTIVE);
        }
    }
}

TEST_F(ClientSessionRegistryTest, AbandonedRemountPreservesExistingReadiness) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
    }
    EXPECT_EQ(Find(registry, kClient), record);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        remount->Commit();
    }
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        EXPECT_FALSE(remount->NeedsRemount());
    }
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::OK);
}

TEST_F(ClientSessionRegistryTest,
       TerminalSessionsRejectRegistrationAndRemount) {
    ClientSessionRegistry registry;
    Register(registry);
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        remount->Commit();
    }
    const auto record = Find(registry, kClient);
    Suspect(record);
    Retire(record);
    EXPECT_FALSE(registry.BeginRegistration(kClient));
    EXPECT_FALSE(registry.BeginRemount(kClient));
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
    EXPECT_TRUE(registry.Remove(kClient, Find(registry, kClient)));
    Register(registry);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionRegistryTest, MovedRegistrationOwnsRollbackAndCommit) {
    static_assert(
        !std::is_copy_constructible_v<ClientSessionRegistry::Registration>);
    static_assert(
        !std::is_move_assignable_v<ClientSessionRegistry::Registration>);
    ClientSessionRegistry registry;
    std::optional<ClientSessionRegistry::Registration> moved;
    std::weak_ptr<const ClientLivenessRecord> provisional;
    {
        auto registration = registry.BeginRegistration(kClient);
        ASSERT_TRUE(registration);
        provisional = registration->Session();
        moved.emplace(std::move(*registration));
        EXPECT_FALSE(registration->Session());
    }
    ASSERT_TRUE(moved->Session());
    EXPECT_FALSE(provisional.expired());
    moved.reset();
    EXPECT_TRUE(provisional.expired());
    EXPECT_FALSE(Find(registry, kClient));
    {
        auto remount = registry.BeginRemount(kClient);
        moved.emplace(std::move(*remount));
        moved->Commit();
    }
    moved.reset();
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::OK);
    {
        auto registration = registry.BeginRegistration(kClient);
        registration->Commit();
        moved.emplace(std::move(*registration));
    }
    moved.reset();
    EXPECT_TRUE(Find(registry, kClient));
}

TEST_F(ClientSessionRegistryTest, MovingRegistrationPreservesCommitIntent) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    Suspect(record);
    {
        auto original = registry.BeginRemount(kClient);
        ASSERT_TRUE(original);
        auto moved = std::move(*original);
        moved.Commit();
        EXPECT_FALSE(moved.NeedsRemount());
        EXPECT_EQ(moved.Session()->state(), State::SUSPECTED);
    }
    {
        auto original = registry.BeginRegistration(kClient, kInitial + 11s);
        ASSERT_TRUE(original);
        auto moved = std::move(*original);
        moved.Commit();
        EXPECT_EQ(moved.Session()->state(), State::ACTIVE);
    }
    EXPECT_EQ(record->Evaluate(kInitial + 20s, 10s, 20s), Transition::NONE);
    EXPECT_EQ(record->Evaluate(kInitial + 21s, 10s, 20s),
              Transition::BECAME_SUSPECTED);
}

TEST_F(ClientSessionRegistryTest, RegistrationCannotCommitTwice) {
    ClientSessionRegistry registry;
    {
        auto registration = registry.BeginRegistration(kClient);
        ASSERT_TRUE(registration);
        registration->Commit();
        EXPECT_DEATH(registration->Commit(), "committed_");
    }
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        remount->Commit();
        EXPECT_DEATH(remount->Commit(), "committed_");
    }
}

TEST_F(ClientSessionRegistryTest, RegistrationAllowsPingBeforeScopeExit) {
    ClientSessionRegistry registry;
    std::promise<void> attempted;
    auto started = attempted.get_future();
    std::future<ClientStatus> ping;
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        remount->Commit();
        ping = std::async(std::launch::async, [&] {
            attempted.set_value();
            return registry.Ping(kClient);
        });
        started.wait();
        EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
    }
    EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(ping.get(), ClientStatus::OK);
}

TEST_F(ClientSessionRegistryTest,
       WaitingRemountRetriesAfterProvisionalRollback) {
    ClientSessionRegistry registry;
    ClientSessionSharedPtr old;
    std::future<ClientSessionSharedPtr> next;
    {
        auto registration = registry.BeginRegistration(kClient);
        old = registration->Session();
        next = std::async(std::launch::async, [&] {
            auto remount = registry.BeginRemount(kClient);
            if (!remount) return ClientSessionSharedPtr{};
            remount->Commit();
            return remount->Session();
        });
        EXPECT_EQ(next.wait_for(20ms), std::future_status::timeout);
    }
    const auto replacement = next.get();
    ASSERT_TRUE(replacement);
    EXPECT_NE(old, replacement);
    EXPECT_EQ(Find(registry, kClient), replacement);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::OK);
}

TEST_F(ClientSessionRegistryTest, RegistrationCommitPreservesNewerHeartbeat) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    {
        auto registration = registry.BeginRegistration(kClient, kInitial);
        EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
        registration->Commit();
    }
    EXPECT_EQ(record->Evaluate(Clock::now(), 10s, 20s), Transition::NONE);
    EXPECT_EQ(record->state(), State::ACTIVE);
}

TEST_F(ClientSessionRegistryTest, RemountExcludesResourceAdmissionButNotPing) {
    ClientSessionRegistry registry;
    Register(registry);
    std::future<bool> admitted;
    std::future<ClientStatus> ping;
    {
        auto remount = registry.BeginRemount(kClient);
        admitted = std::async(std::launch::async, [&] {
            return registry.TryAcquireRetainingSession(kClient).has_value();
        });
        ping = std::async(std::launch::async,
                          [&] { return registry.Ping(kClient); });
        EXPECT_EQ(admitted.wait_for(20ms), std::future_status::timeout);
        EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
        remount->Commit();
    }
    EXPECT_TRUE(admitted.get());
    EXPECT_EQ(ping.get(), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionRegistryTest, RestoreWaitsWithoutBlockingRemountCommit) {
    ClientSessionRegistry registry;
    Register(registry);
    const UUID other{3, 4};
    Register(registry, other);
    std::shared_mutex snapshot_mutex;
    std::future<void> restore;
    std::future<ClientStatus> ping;
    {
        auto remount = registry.BeginRemount(kClient);
        std::unique_lock snapshot_lock(snapshot_mutex);
        restore = std::async(std::launch::async, [&] {
            auto batch = registry.BeginRestore();
            std::unique_lock lock(snapshot_mutex);
            batch.Commit();
        });
        EXPECT_EQ(restore.wait_for(20ms), std::future_status::timeout);
        ping = std::async(std::launch::async,
                          [&] { return registry.Ping(other); });
        EXPECT_EQ(ping.wait_for(1s), std::future_status::ready);
        remount->Commit();
        EXPECT_FALSE(remount->NeedsRemount());
    }
    EXPECT_EQ(restore.wait_for(1s), std::future_status::ready);
    restore.get();
    EXPECT_EQ(ping.get(), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionRegistryTest, RestorePublishesOnlyCommittedStagedRecords) {
    std::vector<ClientSessionEvent> events;
    ClientSessionRegistry registry;
    ASSERT_TRUE(registry.AddListener(
        [&](const ClientSessionEvent& event) { events.push_back(event); }));
    auto existing = Register(registry);
    const UUID other{3, 4};
    {
        auto restore = registry.BeginRestore();
        EXPECT_EQ(restore.FindOrCreate(kClient), existing);
        auto pending = restore.FindOrCreate(other);
        EXPECT_EQ(restore.FindOrCreate(other), pending);
    }
    EXPECT_FALSE(Find(registry, other));
    {
        auto restore = registry.BeginRestore();
        restore.FindOrCreate(other);
        restore.Commit();
    }
    EXPECT_EQ(Find(registry, kClient), existing);
    EXPECT_TRUE(Find(registry, other));
    Drain(registry);
    EXPECT_TRUE(events.empty());
    EXPECT_EQ(registry.Ping(other), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionRegistryTest, MovedRestoreOwnsPendingRecordsAndLocks) {
    static_assert(
        !std::is_copy_constructible_v<ClientSessionRegistry::RestoreBatch>);
    static_assert(
        !std::is_move_assignable_v<ClientSessionRegistry::RestoreBatch>);
    ClientSessionRegistry registry;
    std::optional<ClientSessionRegistry::RestoreBatch> moved;
    std::weak_ptr<const ClientLivenessRecord> provisional;
    {
        auto restore = registry.BeginRestore();
        provisional = restore.FindOrCreate(kClient);
        moved.emplace(std::move(restore));
    }
    EXPECT_FALSE(provisional.expired());
    moved.reset();
    EXPECT_TRUE(provisional.expired());
    EXPECT_FALSE(Find(registry, kClient));
    {
        auto restore = registry.BeginRestore();
        restore.FindOrCreate(kClient);
        moved.emplace(std::move(restore));
    }
    moved->Commit();
    moved.reset();
    EXPECT_TRUE(Find(registry, kClient));
}

TEST_F(ClientSessionRegistryTest,
       RestoreExceptionReleasesLocksAndDiscardsStaging) {
    ClientSessionRegistry registry;
    auto existing = Register(registry);
    std::shared_mutex snapshot_mutex;
    const UUID other{3, 4};
    const auto fail = [&] {
        auto restore = registry.BeginRestore();
        std::unique_lock snapshot_lock(snapshot_mutex);
        EXPECT_EQ(restore.FindOrCreate(kClient), existing);
        restore.FindOrCreate(other);
        throw std::runtime_error("restore failed");
    };
    EXPECT_THROW(fail(), std::runtime_error);
    EXPECT_FALSE(Find(registry, other));
    EXPECT_EQ(Find(registry, kClient), existing);
    std::unique_lock lock(snapshot_mutex, std::try_to_lock);
    EXPECT_TRUE(lock.owns_lock());
}

TEST_F(ClientSessionRegistryTest, RemovalDoesNotAddALivenessState) {
    std::vector<ClientSessionEvent> events;
    ClientSessionRegistry registry;
    ASSERT_TRUE(registry.AddListener(
        [&](const ClientSessionEvent& event) { events.push_back(event); }));
    auto old = Register(registry);
    {
        auto remount = registry.BeginRemount(kClient);
        remount->Commit();
    }
    ASSERT_EQ(old->Observe(kInitial + 1s), Observation::REFRESHED_ACTIVE);
    ASSERT_TRUE(registry.Remove(kClient, old));
    EXPECT_EQ(old->state(), State::ACTIVE);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
    EXPECT_FALSE(registry.TryAcquireRetainingSession(kClient));
    // A retained record remains a normal liveness object, but it no longer
    // belongs to the registry.
    EXPECT_EQ(old->Observe(kInitial + 2s), Observation::REFRESHED_ACTIVE);
    EXPECT_EQ(old->Evaluate(kInitial + 12s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(old->Observe(kInitial + 13s), Observation::RECOVERED_ACTIVE);
    auto current = Register(registry);
    EXPECT_NE(old, current);
    Drain(registry);
    EXPECT_TRUE(events.empty());
    EXPECT_EQ(registry.TryAcquireServingSession(kClient)->Session(), current);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionRegistryTest, WaitingAdmissionRejectsRolledBackRecord) {
    ClientSessionRegistry registry;
    std::promise<void> attempted;
    auto started = attempted.get_future();
    std::future<bool> admitted;
    ClientSessionSharedPtr old;
    {
        auto registration = registry.BeginRegistration(kClient);
        old = registration->Session();
        admitted = std::async(std::launch::async, [&] {
            attempted.set_value();
            return registry.TryAcquireRetainingSession(kClient).has_value();
        });
        started.wait();
        EXPECT_EQ(admitted.wait_for(20ms), std::future_status::timeout);
    }
    EXPECT_FALSE(admitted.get());
    EXPECT_EQ(old->state(), State::ACTIVE);
    EXPECT_FALSE(Find(registry, kClient));
    auto current = Register(registry);
    EXPECT_NE(old, current);
    EXPECT_EQ(registry.TryAcquireRetainingSession(kClient)->Session(), current);
}

TEST_F(ClientSessionRegistryTest,
       CurrentOperationValidatesIdentityNotLiveness) {
    ClientSessionRegistry registry;
    EXPECT_FALSE(AcquireExclusiveOperation(registry, kClient, {}));
    auto old = Register(registry);
    auto old_slot = FindSlot(registry, kClient);
    EXPECT_TRUE(AcquireExclusiveOperation(registry, kClient, old_slot));
    ASSERT_TRUE(registry.Remove(kClient, old));
    EXPECT_FALSE(AcquireExclusiveOperation(registry, kClient, old_slot));
    auto current = Register(registry);
    auto current_slot = FindSlot(registry, kClient);
    EXPECT_NE(old_slot, current_slot);
    EXPECT_FALSE(AcquireExclusiveOperation(registry, kClient, old_slot));
    EXPECT_FALSE(AcquireExclusiveOperation(registry, kClient, old_slot,
                                           /*try_lock=*/true));
    EXPECT_TRUE(AcquireExclusiveOperation(registry, kClient, current_slot));
    Suspect(current);
    Retire(current);
    // OFFLINE slots still belong to the registry until cleanup completes.
    // Operation validation must not prevent their removal.
    EXPECT_EQ(current->state(), State::OFFLINE);
    EXPECT_TRUE(AcquireExclusiveOperation(registry, kClient, current_slot));
    EXPECT_FALSE(registry.TryAcquireRetainingSession(kClient));
    EXPECT_TRUE(registry.Remove(kClient, current));
}

TEST_F(ClientSessionRegistryTest, CurrentOperationTryLockDoesNotWait) {
    ClientSessionRegistry registry;
    Register(registry);
    auto slot = FindSlot(registry, kClient);
    std::future<bool> acquired;
    {
        auto remount = registry.BeginRemount(kClient);
        acquired = std::async(std::launch::async, [&] {
            return AcquireExclusiveOperation(registry, kClient, slot,
                                             /*try_lock=*/true);
        });
        EXPECT_EQ(acquired.wait_for(1s), std::future_status::ready);
    }
    EXPECT_FALSE(acquired.get());
    EXPECT_TRUE(AcquireExclusiveOperation(registry, kClient, slot,
                                          /*try_lock=*/true));
}

TEST_F(ClientSessionRegistryTest, RemovedSlotIsReleasedDespiteRetainedRecord) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    std::weak_ptr weak_slot = FindSlot(registry, kClient);
    EXPECT_FALSE(weak_slot.expired());
    ASSERT_TRUE(registry.Remove(kClient, record));
    EXPECT_TRUE(weak_slot.expired());
    EXPECT_EQ(record->state(), State::ACTIVE);
    EXPECT_TRUE(SnapshotRecords(registry).empty());
}

TEST_F(ClientSessionRegistryTest, ConcurrentRemovalSucceedsOnlyOnce) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    std::promise<void> start;
    auto gate = start.get_future().share();
    const auto remove = [&] {
        gate.wait();
        return registry.Remove(kClient, record);
    };
    auto first = std::async(std::launch::async, remove);
    auto second = std::async(std::launch::async, remove);
    start.set_value();
    EXPECT_NE(first.get(), second.get());
    EXPECT_FALSE(Find(registry, kClient));
}

TEST_F(ClientSessionRegistryTest, RestoreDoesNotBlockLookupOrHeartbeat) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    {
        auto remount = registry.BeginRemount(kClient);
        remount->Commit();
    }
    std::future<bool> lookup;
    const UUID other{3, 4};
    {
        auto restore = registry.BeginRestore();
        restore.FindOrCreate(other);
        lookup = std::async(std::launch::async, [&] {
            return Find(registry, kClient) == record &&
                   !Find(registry, other) &&
                   registry.Ping(kClient) == ClientStatus::OK;
        });
        EXPECT_EQ(lookup.wait_for(1s), std::future_status::ready);
        restore.Commit();
    }
    EXPECT_TRUE(lookup.get());
    EXPECT_TRUE(Find(registry, other));
}

}  // namespace
}  // namespace mooncake::test
