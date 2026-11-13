#include "client_session_registry.h"

#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "replica.h"

namespace mooncake::test {
class ClientSessionRegistryTestPeer {
   public:
    using Record = std::shared_ptr<ClientLivenessRecord>;
    static Record Find(const ClientSessionRegistry& registry, const UUID& id) {
        return registry.Find(id);
    }
};

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
    EXPECT_EQ(guard->Session()->ClientId(), kClient);
    EXPECT_TRUE(guard->Session()->IsServing());
}

// Replica staleness checks bind directly to the retained ClientSessionSharedPtr
// rather than consulting ClientSessionRegistry under its lock.
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

    // 1. Provisional session and its staged host hint are discarded on failure.
    EXPECT_EQ(fail(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(Find(registry, kClient));
    EXPECT_TRUE(registry.GetHostId(kClient).empty());

    // 2. Existing session survives failure, and its committed host hint is not
    // overwritten by an uncommitted registration attempt.
    auto record = Register(registry);
    registry.UpdateHostId(kClient, "committed-host");
    Suspect(record);
    EXPECT_EQ(fail(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(Find(registry, kClient), record);
    EXPECT_EQ(record->state(), State::SUSPECTED);
    EXPECT_EQ(registry.GetHostId(kClient), "committed-host");

    // 3. Committing publishes both the liveness recovery and the staged hint.
    {
        auto registration = registry.BeginRegistration(kClient, kInitial + 12s);
        ASSERT_TRUE(registration);
        registration->UpdateHostId("updated-host");
        registration->Commit();
    }
    EXPECT_EQ(record->state(), State::ACTIVE);
    EXPECT_EQ(registry.GetHostId(kClient), "updated-host");
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionRegistryTest, RegistrationSamplesLivenessAtCommit) {
    constexpr auto active_ttl = 10ms;
    for (const bool existing : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "existing=" << existing);
        ClientSessionRegistry registry;
        if (existing) {
            auto record = Register(registry);
            Suspect(record);
        }

        Clock::time_point before_commit;
        Clock::time_point after_commit;
        {
            auto registration = registry.BeginRegistration(kClient);
            ASSERT_TRUE(registration);
            // Model resource installation taking longer than the active TTL.
            std::this_thread::sleep_for(2 * active_ttl);
            before_commit = Clock::now();
            registration->Commit();
            after_commit = Clock::now();
        }

        const auto record = Find(registry, kClient);
        ASSERT_TRUE(record);
        EXPECT_EQ(record->state(), State::ACTIVE);
        // Sampled bounds keep these checks independent of scheduling delays
        // after Commit(): a full active TTL starts at the commit, not Begin().
        EXPECT_EQ(
            record->Evaluate(before_commit + active_ttl - Clock::duration{1},
                             active_ttl, 20s),
            Transition::NONE);
        EXPECT_TRUE(registry.TryAcquireServingSession(kClient));
        EXPECT_EQ(record->Evaluate(after_commit + active_ttl, active_ttl, 20s),
                  Transition::BECAME_SUSPECTED);
    }
}

TEST_F(ClientSessionRegistryTest, ExceptionsDiscardOnlyUncommittedNewRecords) {
    ClientSessionRegistry registry;
    const auto fail = [&] {
        auto registration = registry.BeginRegistration(kClient);
        EXPECT_TRUE(registration);
        throw std::runtime_error("mount failed");
    };

    // Uncommitted provisional record is rolled back when stack unwinding runs.
    EXPECT_THROW(fail(), std::runtime_error);
    EXPECT_FALSE(Find(registry, kClient));

    // Previously committed record is preserved if a later registration throws.
    auto existing = Register(registry);
    EXPECT_THROW(fail(), std::runtime_error);
    EXPECT_EQ(Find(registry, kClient), existing);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);

    // Exceptions thrown after Commit() keep the newly registered session.
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
       RemountInitializesNewSessionAfterRegistryLockWait) {
    ClientSessionRegistry registry;
    constexpr auto active_ttl = 10ms;
    std::promise<void> started;
    auto started_future = started.get_future();
    std::future<bool> remounted;
    Clock::time_point before_unlock;
    {
        // Block BeginRemount() before it can create the missing session.
        // Keep the future outside this scope so the lock is released before
        // joining the worker, including if an assertion fails.
        auto blocker = registry.BeginRestore();
        remounted = std::async(std::launch::async, [&] {
            started.set_value();
            auto remount = registry.BeginRemount(kClient);
            if (!remount) return false;
            remount->Commit();
            return true;
        });
        started_future.wait();
        EXPECT_EQ(remounted.wait_for(2 * active_ttl),
                  std::future_status::timeout);
        before_unlock = Clock::now();
    }
    ASSERT_TRUE(remounted.get());
    const auto after_remount = Clock::now();

    const auto record = Find(registry, kClient);
    ASSERT_TRUE(record);
    // Use sampled bounds, not wall time after joining, so scheduling delays
    // cannot make the freshly initialized session expire during the check.
    EXPECT_EQ(record->Evaluate(before_unlock + active_ttl - Clock::duration{1},
                               active_ttl, 20s),
              Transition::NONE);
    EXPECT_TRUE(registry.TryAcquireServingSession(kClient));
    EXPECT_EQ(record->Evaluate(after_remount + active_ttl, active_ttl, 20s),
              Transition::BECAME_SUSPECTED);
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
    // Remount must preserve the original registration's expiry deadline.
    EXPECT_EQ(record->Evaluate(kInitial + 10s, 10s, 20s),
              Transition::BECAME_SUSPECTED);
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
            SCOPED_TRACE(::testing::Message()
                         << "remounted=" << remounted
                         << ", state=" << static_cast<int>(state));
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

    // 1. Provisional rollback ownership transfers to the moved-to scope.
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

    // 2. Committing on the moved-to scope publishes remount readiness.
    {
        auto remount = registry.BeginRemount(kClient);
        moved.emplace(std::move(*remount));
        moved->Commit();
    }
    moved.reset();
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::OK);

    // 3. Moving an already-committed scope preserves the committed record.
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

TEST_F(ClientSessionRegistryTest, RestorePublishesOnlyCommittedStagedRecords) {
    ClientSessionRegistry registry;
    auto existing = Register(registry);
    const UUID other{3, 4};
    {
        auto restore = registry.BeginRestore();
        EXPECT_EQ(restore.FindOrCreate(kClient), existing);
        auto pending = restore.FindOrCreate(other);
        EXPECT_EQ(pending->ClientId(), other);
        EXPECT_EQ(restore.FindOrCreate(other), pending);
    }
    EXPECT_FALSE(Find(registry, other));
    {
        auto restore = registry.BeginRestore();
        restore.FindOrCreate(other);
        restore.Commit();
    }
    EXPECT_EQ(Find(registry, kClient), existing);
    const auto restored = Find(registry, other);
    ASSERT_TRUE(restored);
    EXPECT_EQ(restored->ClientId(), other);
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
    ClientSessionRegistry registry;
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
    EXPECT_EQ(registry.TryAcquireServingSession(kClient)->Session(), current);
    EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
}

TEST_F(ClientSessionRegistryTest,
       UpdateHostIdIgnoresMissingAndOfflineSessions) {
    ClientSessionRegistry registry;
    registry.UpdateHostId(kClient, "unregistered-host");
    EXPECT_TRUE(registry.GetHostId(kClient).empty());

    auto old = Register(registry);
    registry.UpdateHostId(kClient, "host-a");
    EXPECT_EQ(registry.GetHostId(kClient), "host-a");
    // Repeated identical updates and empty hints preserve the stored value.
    registry.UpdateHostId(kClient, "host-a");
    registry.UpdateHostId(kClient, "");
    EXPECT_EQ(registry.GetHostId(kClient), "host-a");

    Suspect(old);
    registry.UpdateHostId(kClient, "host-b");
    EXPECT_EQ(registry.GetHostId(kClient), "host-b");

    const UUID retiring{5, 6};
    auto offline = Register(registry, retiring);
    Suspect(offline);
    Retire(offline);
    registry.UpdateHostId(retiring, "late-write-host");
    EXPECT_TRUE(registry.GetHostId(retiring).empty());
    ASSERT_TRUE(registry.Remove(retiring, offline));
    Register(registry, retiring);
    EXPECT_TRUE(registry.GetHostId(retiring).empty());
}

TEST_F(ClientSessionRegistryTest,
       RemoveSerializesAgainstConcurrentSessionAcquisition) {
    ClientSessionRegistry registry;
    auto old = Register(registry);
    auto in_flight = registry.TryAcquireRetainingSession(kClient);
    ASSERT_TRUE(in_flight);

    std::promise<void> started;
    auto started_future = started.get_future();
    auto remover = std::async(std::launch::async, [&] {
        started.set_value();
        return registry.Remove(kClient, old);
    });
    started_future.wait();
    EXPECT_EQ(remover.wait_for(20ms), std::future_status::timeout);

    auto late_acquirer = std::async(std::launch::async, [&] {
        return registry.TryAcquireRetainingSession(kClient).has_value();
    });
    EXPECT_EQ(late_acquirer.wait_for(20ms), std::future_status::timeout);

    in_flight.reset();
    EXPECT_TRUE(remover.get());
    EXPECT_EQ(old->state(), State::ACTIVE);
    EXPECT_FALSE(late_acquirer.get());
}

TEST_F(ClientSessionRegistryTest,
       ExpireSessionsClearsMetadataBeforeRetireCallback) {
    ClientSessionRegistry registry;
    auto record = Register(registry);
    {
        auto remount = registry.BeginRemount(kClient);
        ASSERT_TRUE(remount);
        remount->UpdateHostId("host-a");
        remount->Commit();
    }
    ASSERT_EQ(registry.GetHostId(kClient), "host-a");

    // First expiry pass moves ACTIVE -> SUSPECTED without retiring resources.
    bool reserved = false;
    bool retired = false;
    registry.ExpireSessions(
        kInitial + 10s, 10s, 20s, [&] { reserved = true; },
        [&](const UUID&, const ClientSessionSharedPtr&) { retired = true; });
    EXPECT_EQ(record->state(), State::SUSPECTED);
    EXPECT_FALSE(reserved);
    EXPECT_FALSE(retired);
    EXPECT_EQ(registry.GetHostId(kClient), "host-a");

    // Second expiry pass reserves retirement before OFFLINE, clears readiness
    // and host hints, and passes the terminal session to the retire callback.
    registry.ExpireSessions(
        kInitial + 30s, 10s, 20s,
        [&] {
            EXPECT_EQ(record->state(), State::SUSPECTED);
            reserved = true;
        },
        [&](const UUID& id, const ClientSessionSharedPtr& session) {
            EXPECT_TRUE(reserved);
            EXPECT_EQ(id, kClient);
            EXPECT_EQ(session, record);
            EXPECT_EQ(session->state(), State::OFFLINE);
            EXPECT_TRUE(registry.GetHostId(kClient).empty());
            EXPECT_EQ(registry.Ping(kClient), ClientStatus::NEED_REMOUNT);
            retired = true;
        });
    EXPECT_TRUE(retired);
    EXPECT_EQ(Find(registry, kClient), record);
    EXPECT_TRUE(registry.Remove(kClient, record));
}

}  // namespace
}  // namespace mooncake::test
