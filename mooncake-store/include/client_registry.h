#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "client_session.h"
#include "client_offboarding.h"

namespace mooncake {

class ClientOffboardingWorker;
class ClientRegistryTestPeer;

// Owns client membership, synchronization, host hints, remount readiness and
// liveness monitoring. Resource retirement is supplied by the caller; it runs
// outside both registry and session locks. No MasterService state is borrowed.
class ClientRegistry final {
   public:
    using Sessions =
        std::unordered_map<UUID, ClientSessionPtr, boost::hash<UUID>>;
    using ClientIds = std::unordered_set<UUID, boost::hash<UUID>>;
    using Clock = ClientSession::Clock;
    using CleanupResources = std::function<bool(ClientOffboardingJob&)>;

    explicit ClientRegistry(bool account_metrics = true);
    ~ClientRegistry();
    ClientRegistry(const ClientRegistry&) = delete;
    ClientRegistry& operator=(const ClientRegistry&) = delete;

    // Explicit access is only needed to compose registry operations with an
    // external barrier. Acquire registry access before snapshot/resource locks;
    // do not reenter the registry while holding access or a Registration.
    class ReadAccess final {
       public:
        ReadAccess(ReadAccess&&) noexcept = default;
        ClientIds RetainingClientIds() const;
        void Release() { lock_.unlock(); }

       private:
        friend class ClientRegistry;
        explicit ReadAccess(const ClientRegistry& registry);
        const ClientRegistry& registry_;
        std::shared_lock<std::shared_mutex> lock_;
    };

    class WriteAccess final {
       public:
        WriteAccess(WriteAccess&&) noexcept = default;
        ClientSessionPtr Find(const UUID& client_id) const;
        // Staged sessions are detached, ACTIVE incarnations, never an exposed
        // reference to a registry's container.
        void Import(const Sessions& staged);
        void Release() { lock_.unlock(); }

       private:
        friend class ClientRegistry;
        WriteAccess(ClientRegistry& registry,
                    std::unique_lock<std::shared_mutex> lock);
        ClientRegistry& registry_;
        std::unique_lock<std::shared_mutex> lock_;
    };

    class Registration final {
       public:
        Registration(Registration&& other) noexcept;
        Registration(const Registration&) = delete;
        Registration& operator=(const Registration&) = delete;
        ~Registration();

        const ClientSessionPtr& session() const { return session_; }
        bool IsRemounted() const;
        // Memory/local-disk mount commits an observation; remount only retains
        // resources and records readiness. Both keep the barrier until scope
        // exit.
        ClientLivenessObservation Commit(bool observe = true,
                                         Clock::time_point now = Clock::now());
        void CommitRemount();

       private:
        friend class ClientRegistry;
        Registration(ClientRegistry& registry,
                     std::unique_lock<std::shared_mutex> registry_lock,
                     ClientSessionPtr session,
                     ClientSession::RetainingGuard guard, bool inserted);
        ClientRegistry* registry_;
        std::unique_lock<std::shared_mutex> registry_lock_;
        ClientSessionPtr session_;
        ClientSession::RetainingGuard guard_;
        bool inserted_;
        bool committed_{false};
    };

    // Registration owns the registry/transition barriers across publication.
    // Abandoning it removes only a newly created session; existing sessions are
    // neither refreshed nor recovered. OFFLINE sessions cannot register again.
    tl::expected<Registration, ErrorCode> Register(
        const UUID& client_id, Clock::time_point now = Clock::now());
    ClientSessionPtr GetOrCreate(const UUID& client_id,
                                 Clock::time_point now = Clock::now());
    ClientSessionPtr Find(const UUID& client_id) const;
    Sessions Snapshot() const;
    bool Remove(const ClientSessionPtr& session);
    void Clear();
    void Replace(ClientRegistry& staged);
    ClientLivenessObservation Observe(const ClientSessionPtr& session,
                                      Clock::time_point now);
    ClientStatus Ping(const UUID& client_id);
    // Writers without mounted storage can have host hints without a session.
    std::string ResolveHostId(const UUID& client_id,
                              const std::string& hint = {});

    ReadAccess AcquireReadAccess() const;
    WriteAccess AcquireWriteAccess();
    std::optional<WriteAccess> TryAcquireWriteAccess();

    // Registry owns reservation, scheduling, retries and session removal. The
    // callback performs one resource-cleanup attempt outside registry/session
    // locks, returning false (or throwing) to retry the same residual job.
    void StartMonitoring(Clock::duration active_ttl,
                         Clock::duration suspicion_ttl,
                         CleanupResources cleanup,
                         Clock::duration interval = std::chrono::seconds(1));
    // First stop producing jobs; pending/in-flight work still blocks snapshots.
    void StopMonitoring();
    // After external snapshot producers have joined, stop the worker and drop
    // queued retries. Joins all callbacks before returning. Start/Stop must be
    // serialized by the owner and must not run from a cleanup callback.
    void Stop();
    bool HasPendingOffboarding() const;

   private:
    friend class ClientRegistryTestPeer;
    void StartOffboarding(CleanupResources cleanup);
    ClientLivenessTransition Evaluate(const ClientSessionPtr& session,
                                      Clock::time_point now,
                                      Clock::duration active_ttl,
                                      Clock::duration suspicion_ttl);
    void ScheduleClientOffboarding(const ClientSessionPtr& session);
    bool ProcessClientOffboardingJob(ClientOffboardingJob& job);
    ClientSessionPtr FindLocked(const UUID& client_id) const;
    ClientSessionPtr GetOrCreateLocked(const UUID& client_id,
                                       Clock::time_point now);
    bool RemoveLocked(const ClientSessionPtr& session,
                      bool clear_client_state = true);
    void ClearLocked();
    void ImportLocked(const Sessions& staged);
    void ClearClientStateLocked(const UUID& client_id);
    void AccountObservation(ClientLivenessObservation observation);
    mutable std::shared_mutex mutex_;
    Sessions sessions_;
    ClientIds remounted_;
    std::unordered_map<UUID, std::string, boost::hash<UUID>> writer_host_hints_;
    const bool account_metrics_;
    CleanupResources cleanup_resources_;
    std::unique_ptr<ClientOffboardingWorker> offboarding_worker_;
    std::jthread monitor_;
};

}  // namespace mooncake
