#include "client_registry.h"
#include "client_registry_test_peer.h"

#include <future>
#include <stdexcept>
#include <thread>
#include <vector>
#include <gtest/gtest.h>

namespace mooncake::test {
namespace {
using namespace std::chrono_literals;

TEST(ClientRegistryTest, RegistrationRollsBackOnlyNewSessions) {
    ClientRegistry registry(false);
    const UUID id{1, 2};
    const auto now = ClientSession::TimePoint{};
    {
        auto registration = registry.Register(id, now);
        ASSERT_TRUE(registration);
        EXPECT_EQ(registration->session()->client_id(), id);
    }
    EXPECT_FALSE(registry.Find(id));

    const auto session = registry.GetOrCreate(id, now);
    ASSERT_EQ(session->Evaluate(now + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    {
        auto registration = registry.Register(id, now + 11s);
        ASSERT_TRUE(registration);
        EXPECT_EQ(registration->session(), session);
    }
    EXPECT_EQ(registry.Find(id), session);
    EXPECT_EQ(session->state(), ClientLivenessState::SUSPECTED);
    EXPECT_EQ(session->Evaluate(now + 30s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
    EXPECT_FALSE(registry.Register(id, now + 31s));
}

TEST(ClientRegistryTest, SuccessfulMountObservesButRemountOnlyRetains) {
    ClientRegistry registry(false);
    const UUID id{1, 2};
    const auto now = ClientSession::TimePoint{};
    const auto session = registry.GetOrCreate(id, now);
    ASSERT_EQ(session->Evaluate(now + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    {
        auto registration = registry.Register(id);
        ASSERT_TRUE(registration);
        EXPECT_EQ(registration->Commit(false),
                  ClientLivenessObservation::OBSERVATION_WITHHELD);
    }
    EXPECT_EQ(session->state(), ClientLivenessState::SUSPECTED);
    {
        auto registration = registry.Register(id);
        ASSERT_TRUE(registration);
        EXPECT_EQ(registration->Commit(true, now + 11s),
                  ClientLivenessObservation::RECOVERED_ACTIVE);
    }
    EXPECT_TRUE(session->IsServing());
    EXPECT_EQ(session->Evaluate(now + 20s, 10s, 20s),
              ClientLivenessTransition::NONE);
}

TEST(ClientRegistryTest, RetiredIncarnationCannotReviveOrRemoveReplacement) {
    ClientRegistry registry(false);
    const UUID id{1, 2};
    const auto now = ClientSession::TimePoint{};
    auto old = registry.GetOrCreate(id, now);
    ASSERT_EQ(old->Evaluate(now + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    ASSERT_EQ(old->Evaluate(now + 30s, 10s, 20s),
              ClientLivenessTransition::BECAME_OFFLINE);
    ASSERT_TRUE(registry.Remove(old));
    auto replacement = registry.GetOrCreate(id, now + 31s);
    EXPECT_NE(replacement, old);
    EXPECT_EQ(replacement->client_id(), old->client_id());
    EXPECT_FALSE(registry.Remove(old));
    EXPECT_EQ(registry.Find(id), replacement);
    EXPECT_EQ(registry.Observe(old, now + 32s),
              ClientLivenessObservation::REJECTED_OFFLINE);
    EXPECT_TRUE(replacement->IsServing());
}

TEST(ClientRegistryTest, RegistrationSerializesObservationWithExpiry) {
    ClientRegistry registry(false);
    const UUID id{1, 2};
    const auto now = ClientSession::TimePoint{};
    const auto session = registry.GetOrCreate(id, now);
    ASSERT_EQ(session->Evaluate(now + 10s, 10s, 20s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    std::promise<void> started;
    auto ready = started.get_future();
    std::future<ClientLivenessTransition> transition;
    {
        auto registration = registry.Register(id);
        ASSERT_TRUE(registration);
        transition = std::async(std::launch::async, [&] {
            started.set_value();
            return ClientRegistryTestPeer::Evaluate(registry, session,
                                                    now + 30s, 10s, 20s);
        });
        ready.wait();
        EXPECT_EQ(transition.wait_for(10ms), std::future_status::timeout);
        EXPECT_EQ(registration->Commit(true, now + 30s),
                  ClientLivenessObservation::RECOVERED_ACTIVE);
    }
    EXPECT_EQ(transition.get(), ClientLivenessTransition::NONE);
    EXPECT_TRUE(session->IsServing());
}

TEST(ClientRegistryTest, ConcurrentCreationAndSnapshotsNeedNoExternalLock) {
    ClientRegistry registry(false);
    const UUID id{1, 2};
    std::vector<std::future<ClientSessionPtr>> callers;
    for (int i = 0; i < 16; ++i) {
        callers.push_back(std::async(std::launch::async,
                                     [&] { return registry.GetOrCreate(id); }));
    }
    const auto session = callers.front().get();
    for (size_t i = 1; i < callers.size(); ++i)
        EXPECT_EQ(callers[i].get(), session);
    auto snapshot = registry.Snapshot();
    ASSERT_EQ(snapshot.size(), 1U);
    snapshot.clear();
    EXPECT_EQ(registry.Find(id), session);
    EXPECT_EQ(registry.AcquireReadAccess().RetainingClientIds(),
              (ClientRegistry::ClientIds{id}));
}

TEST(ClientRegistryTest, ReadersSeeOnlyCommittedRegistration) {
    for (bool commit : {false, true}) {
        ClientRegistry registry(false);
        const UUID id{1, 2};
        std::promise<void> started;
        auto ready = started.get_future();
        std::future<ClientSessionPtr> reader;
        ClientSessionPtr session;
        {
            auto registration = registry.Register(id);
            ASSERT_TRUE(registration);
            session = registration->session();
            reader = std::async(std::launch::async, [&] {
                started.set_value();
                return registry.Find(id);
            });
            ready.wait();
            EXPECT_EQ(reader.wait_for(10ms), std::future_status::timeout);
            if (commit) registration->Commit();
        }
        EXPECT_EQ(reader.get(), commit ? session : nullptr);
    }
}

TEST(ClientRegistryTest, OwnsReadinessAndHostHintsAcrossRegistrationAndExpiry) {
    ClientRegistry registry(false);
    ClientRegistryTestPeer::StartWorker(
        registry, [](ClientOffboardingJob&) { return false; });
    const UUID id{1, 2};
    EXPECT_EQ(registry.ResolveHostId(id, "writer-host"), "writer-host");
    EXPECT_FALSE(registry.Find(id));
    EXPECT_EQ(registry.Ping(id), ClientStatus::NEED_REMOUNT);
    {
        auto failed = registry.Register(id);
        ASSERT_TRUE(failed);
    }
    EXPECT_EQ(registry.ResolveHostId(id), "writer-host");
    {
        auto registration = registry.Register(id);
        ASSERT_TRUE(registration);
        EXPECT_FALSE(registration->IsRemounted());
        registration->session()->SetHostId("storage-host");
        registration->CommitRemount();
        EXPECT_TRUE(registration->IsRemounted());
    }
    EXPECT_EQ(registry.ResolveHostId(id), "storage-host");
    EXPECT_EQ(registry.Ping(id), ClientStatus::OK);
    const auto session = registry.Find(id);
    const auto now = ClientSession::Clock::now();
    EXPECT_EQ(ClientRegistryTestPeer::Evaluate(registry, session, now, 0s, 0s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    EXPECT_EQ(ClientRegistryTestPeer::Evaluate(registry, session, now, 0s, 0s),
              ClientLivenessTransition::BECAME_OFFLINE);
    EXPECT_EQ(registry.Ping(id), ClientStatus::NEED_REMOUNT);
    EXPECT_TRUE(registry.ResolveHostId(id).empty());
    EXPECT_TRUE(registry.AcquireReadAccess().RetainingClientIds().empty());
}

TEST(ClientRegistryTest,
     HostHintsTransferToSessionsWithoutLeakingAcrossIncarnations) {
    ClientRegistry registry(false);
    const UUID id{1, 2};
    EXPECT_EQ(registry.ResolveHostId(id, "writer"), "writer");
    {
        auto registration = registry.Register(id);
        ASSERT_TRUE(registration);
        EXPECT_EQ(registration->session()->host_id(), "writer");
        registration->session()->SetHostId("abandoned");
    }
    EXPECT_FALSE(registry.Find(id));
    EXPECT_EQ(registry.ResolveHostId(id), "writer");
    ClientSessionPtr old;
    {
        auto registration = registry.Register(id);
        ASSERT_TRUE(registration);
        old = registration->session();
        EXPECT_EQ(old->host_id(), "writer");
        old->SetHostId("mounted");
        registration->Commit();
    }
    EXPECT_EQ(registry.ResolveHostId(id), "mounted");
    EXPECT_EQ(registry.ResolveHostId(id, "new-writer"), "new-writer");
    EXPECT_EQ(old->host_id(), "new-writer");
    ASSERT_TRUE(registry.Remove(old));
    const auto replacement = registry.GetOrCreate(id);
    EXPECT_TRUE(replacement->host_id().empty());
    old->SetHostId("stale");
    EXPECT_TRUE(registry.ResolveHostId(id).empty());
}

TEST(ClientRegistryTest, StaleEvaluationCannotClearReplacementClientState) {
    ClientRegistry registry(false);
    const UUID id{1, 2};
    const auto old = registry.GetOrCreate(id);
    const auto now = ClientSession::Clock::now();
    ASSERT_EQ(ClientRegistryTestPeer::Evaluate(registry, old, now, 0s, 0s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    ASSERT_TRUE(registry.Remove(old));
    {
        auto registration = registry.Register(id);
        ASSERT_TRUE(registration);
        registration->session()->SetHostId("replacement");
        registration->CommitRemount();
    }
    EXPECT_EQ(ClientRegistryTestPeer::Evaluate(registry, old, now, 0s, 0s),
              ClientLivenessTransition::NONE);
    EXPECT_FALSE(registry.HasPendingOffboarding());
    EXPECT_EQ(registry.Ping(id), ClientStatus::OK);
    EXPECT_EQ(registry.ResolveHostId(id), "replacement");
}

TEST(ClientRegistryTest, MonitorReservesBeforeOfflineAndRetiresOutsideLocks) {
    ClientRegistry registry(false);
    const UUID id{1, 2};
    const auto session = registry.GetOrCreate(id);
    std::promise<ClientSessionPtr> retired;
    auto completed = retired.get_future();
    registry.StartMonitoring(
        0s, 0s,
        [&](ClientOffboardingJob& job) {
            EXPECT_EQ(job.liveness, session);
            EXPECT_EQ(session->state(), ClientLivenessState::OFFLINE);
            EXPECT_TRUE(registry.HasPendingOffboarding());
            // Resource cleanup runs outside registry and session locks.
            EXPECT_EQ(registry.Find(id), session);
            EXPECT_EQ(registry.Ping(id), ClientStatus::NEED_REMOUNT);
            EXPECT_FALSE(session->TryAcquireRetainingGuard());
            retired.set_value(session);
            return true;
        },
        1ms);
    EXPECT_EQ(completed.wait_for(2s), std::future_status::ready);
    registry.Stop();
    EXPECT_TRUE(registry.Snapshot().empty());
    EXPECT_FALSE(registry.HasPendingOffboarding());
}

TEST(ClientRegistryTest, StopMonitoringInterruptsLongPollingInterval) {
    ClientRegistry registry(false);
    registry.StartMonitoring(
        1h, 1h, [](ClientOffboardingJob&) { return true; }, 1h);
    const auto start = ClientSession::Clock::now();
    registry.StopMonitoring();
    EXPECT_LT(ClientSession::Clock::now() - start, 2s);
    registry.StopMonitoring();
}

TEST(ClientRegistryTest, ReservationPrecedesOfflinePublicationAndScheduling) {
    std::promise<void> cleaned;
    auto complete = cleaned.get_future();
    ClientRegistry registry(false);
    const auto session = registry.GetOrCreate(UUID{1, 2});
    ClientRegistryTestPeer::StartWorker(registry, [&](ClientOffboardingJob&) {
        cleaned.set_value();
        return true;
    });
    const auto now = ClientSession::Clock::now();
    ASSERT_EQ(ClientRegistryTestPeer::Evaluate(registry, session, now, 0s, 0s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    auto read = registry.AcquireReadAccess();
    auto expiry = std::async(std::launch::async, [&] {
        return ClientRegistryTestPeer::Evaluate(registry, session, now, 0s, 0s);
    });
    const auto deadline = ClientSession::Clock::now() + 2s;
    while (session->state() != ClientLivenessState::OFFLINE &&
           ClientSession::Clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_EQ(session->state(), ClientLivenessState::OFFLINE);
    EXPECT_TRUE(registry.HasPendingOffboarding());
    EXPECT_EQ(complete.wait_for(0s), std::future_status::timeout);
    read.Release();
    EXPECT_EQ(expiry.get(), ClientLivenessTransition::BECAME_OFFLINE);
    EXPECT_EQ(complete.wait_for(2s), std::future_status::ready);
    registry.Stop();
    EXPECT_FALSE(registry.Find(session->client_id()));
    EXPECT_FALSE(registry.HasPendingOffboarding());
}

TEST(ClientRegistryTest, RetryOwnsSessionAndResidualsAfterMonitoringStops) {
    for (bool throws : {false, true}) {
        std::promise<void> attempted, cleaned;
        auto first = attempted.get_future();
        auto complete = cleaned.get_future();
        ClientRegistry registry(false);
        const auto session = registry.GetOrCreate(UUID{1, 2});
        registry.StartMonitoring(
            0s, 0s,
            [&](ClientOffboardingJob& job) {
                EXPECT_TRUE(registry.HasPendingOffboarding());
                EXPECT_EQ(registry.Find(job.client_id), session);
                if (job.retry_count == 0) {
                    job.resources_prepared = true;
                    job.metadata_cleanup_accepted = true;
                    attempted.set_value();
                    if (throws) throw std::runtime_error("retry cleanup");
                    return false;
                }
                EXPECT_EQ(job.retry_count, 1U);
                EXPECT_TRUE(job.resources_prepared);
                EXPECT_TRUE(job.metadata_cleanup_accepted);
                cleaned.set_value();
                return true;
            },
            1ms);
        EXPECT_EQ(first.wait_for(2s), std::future_status::ready);
        registry.StopMonitoring();
        EXPECT_TRUE(registry.HasPendingOffboarding());
        EXPECT_EQ(registry.Find(session->client_id()), session);
        EXPECT_EQ(complete.wait_for(3s), std::future_status::ready);
        registry.Stop();
        EXPECT_FALSE(registry.HasPendingOffboarding());
        EXPECT_FALSE(registry.Find(session->client_id()));
    }
}

TEST(ClientRegistryTest, StopWaitsForCleanupBeforeDroppingRetryBarrier) {
    std::promise<void> entered, release;
    auto started = entered.get_future();
    auto released = release.get_future();
    ClientRegistry registry(false);
    const auto session = registry.GetOrCreate(UUID{1, 2});
    registry.StartMonitoring(
        0s, 0s,
        [&](ClientOffboardingJob&) {
            entered.set_value();
            released.wait();
            return false;
        },
        1ms);
    EXPECT_EQ(started.wait_for(2s), std::future_status::ready);
    auto stopped = std::async(std::launch::async, [&] { registry.Stop(); });
    EXPECT_EQ(stopped.wait_for(20ms), std::future_status::timeout);
    EXPECT_TRUE(registry.HasPendingOffboarding());
    release.set_value();
    stopped.get();
    EXPECT_FALSE(registry.HasPendingOffboarding());
    EXPECT_EQ(registry.Find(session->client_id()), session);
    EXPECT_EQ(session->state(), ClientLivenessState::OFFLINE);
}

TEST(ClientRegistryTest, StaleJobCannotCleanOrRemoveReplacement) {
    ClientRegistry registry(false);
    const UUID id{1, 2};
    ClientOffboardingJob job;
    job.client_id = id;
    job.liveness = registry.GetOrCreate(id);
    ASSERT_TRUE(registry.Remove(job.liveness));
    const auto replacement = registry.GetOrCreate(id);
    // No callback installed: a stale job must be discarded before cleanup.
    EXPECT_TRUE(ClientRegistryTestPeer::ProcessPreparedJob(registry, job));
    EXPECT_EQ(registry.Find(id), replacement);
}

TEST(ClientRegistryTest, RestoreAccessAndReplacementOwnTheirSynchronization) {
    ClientRegistry registry(false);
    ClientRegistry staged(false);
    const UUID id{1, 2};
    const auto session = staged.GetOrCreate(id);
    const auto restored = staged.Snapshot();
    {
        auto access = registry.AcquireWriteAccess();
        EXPECT_FALSE(access.Find(id));
        access.Import(restored);
        EXPECT_EQ(access.Find(id), session);
    }
    const auto retained_snapshot = registry.Snapshot();
    ClientRegistry replacement(false);
    const auto next = replacement.GetOrCreate(id);
    registry.Replace(replacement);
    EXPECT_EQ(registry.Find(id), next);
    EXPECT_EQ(retained_snapshot.at(id), session);
    EXPECT_FALSE(registry.Remove(session));
}

}  // namespace
}  // namespace mooncake::test
