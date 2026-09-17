#pragma once

#include <boost/functional/hash.hpp>

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "client_liveness.h"
#include "types.h"

namespace mooncake {

namespace test {
class ClientSessionManagerTest;
}

class MasterService;
class ClientOffboardingWorker;
struct ClientOffboardingJob;

// The record is the incarnation identity: an event for an old record must not
// be applied to a new registration with the same client ID.
struct ClientSessionEvent {
    UUID client_id;
    std::shared_ptr<ClientLivenessRecord> session;
    ClientLivenessState previous;
    ClientLivenessState current;
};

class ClientSessionManager {
   private:
    enum class RegistrationKind { Register, Remount };

   public:
    using Record = std::shared_ptr<ClientLivenessRecord>;
    using Records = std::unordered_map<UUID, Record, boost::hash<UUID>>;
    using Listener = std::function<void(const ClientSessionEvent&)>;
    using Clock = ClientLivenessRecord::Clock;

    // Production lifecycle owns offboarding; the service handles resource work
    // through its session listener and must outlive the manager.
    ClientSessionManager(Clock::duration active_ttl,
                         Clock::duration suspicion_ttl, MasterService& service);

    // Standalone lifecycle without a worker, also used for deterministic tests.
    // reserve_retirement runs before OFFLINE under the transition lock; it must
    // not acquire registry, segment, metadata, or snapshot locks.
    ClientSessionManager(Clock::duration active_ttl,
                         Clock::duration suspicion_ttl,
                         std::function<void()> reserve_retirement = {});
    ~ClientSessionManager();
    ClientSessionManager(const ClientSessionManager&) = delete;
    ClientSessionManager& operator=(const ClientSessionManager&) = delete;

    // Register before the first Start/Poll/Stop (including Quiesce). Returns
    // false for an empty listener or once delivery has been enabled; Stop does
    // not reopen registration. Multiple components may register independently.
    [[nodiscard]] bool AddSessionListener(Listener listener);

    // Listeners run in registration order, serially without registry or
    // record locks, and must not throw. Stop drains notifications and joins;
    // listener owners must outlive Stop. Do not call Stop from a listener.
    // Stop rejects subsequent notifications until Start; quiesce RPC producers
    // before stopping so no production transition loses its side effects.
    void Start();
    void Stop();
    // Drain events and stop monitoring, retaining cleanup and its barrier.
    // Join snapshot producers before the final Stop().
    void Quiesce();
    bool HasPendingOffboarding() const;

    // Lookup only: the returned record does not grant operation admission.
    // Use TryAcquire*Session for work that must exclude state transitions.
    Record Find(const UUID& client_id) const;

    // Host hints may arrive before registration. These methods synchronize
    // internally and may also be called within a registry transaction. Empty
    // updates preserve the previous hint; OFFLINE/removal/reset erase it.
    // Prefer Registration::UpdateHostId within a registration scope.
    void UpdateHostId(const UUID& client_id, const std::string& host_id);
    std::string GetHostId(const UUID& client_id) const;

    // Admission owns the session and its transition lock, not the registry
    // lock. Both calls block on that lock; an empty result means the client
    // is missing or its state rejects admission (not lock contention).
    // Serving accepts ACTIVE; retaining accepts ACTIVE and SUSPECTED.
    class SessionGuard;
    [[nodiscard]] std::optional<SessionGuard> TryAcquireServingSession(
        const UUID& client_id) const;
    [[nodiscard]] std::optional<SessionGuard> TryAcquireRetainingSession(
        const UUID& client_id) const;

    bool Remove(const UUID& client_id, const Record& expected);

    // Both scopes hold the WHOLE registry exclusively and the session's
    // transition lock until destruction, including after Commit(). Do not
    // reenter registry APIs or reacquire the session's transition lock.
    // A missing session is provisionally created; OFFLINE rejects admission.
    // The manager must outlive these scopes.
    class Registration;
    // Successful registration renews liveness (and recovers SUSPECTED).
    [[nodiscard]] std::optional<Registration> BeginRegistration(
        const UUID& client_id,
        std::optional<Clock::time_point> observed_at = std::nullopt);
    // Successful remount marks readiness without renewing/reviving liveness.
    [[nodiscard]] std::optional<Registration> BeginRemount(
        const UUID& client_id);

    // Stage restored owners under the exclusive registry lock. Commit adopts
    // them; destruction without Commit discards them. Resource rollback is
    // the caller's job. Commit does not release the lock; manager must outlive
    // the scope. Existing records are reused, not replaced.
    class RestoreBatch;
    [[nodiscard]] RestoreBatch BeginRestore();

    ClientStatus Ping(const UUID& client_id);
    // Copies registry membership, not a frozen snapshot of session state.
    Records SnapshotRecords() const;
    // Whole-registry reset/replacement is only allowed while stopped.
    void Reset(Records records = {});

   private:
    friend class MasterService;
    friend class test::ClientSessionManagerTest;

    // Explicit ticks allow deterministic tests through the friend fixture;
    // do not race with Start/Stop or another Poll. Production uses the owned
    // monitor thread.
    void Poll(Clock::time_point now);
    // Only the resource-owning listener submits the one pre-reserved job.
    void ScheduleOffboarding(ClientOffboardingJob job);

    // Helpers below require mutex_ to be held by a manager operation.
    Record FindRecord(const UUID& client_id) const;
    std::pair<Record, bool> GetOrCreateRecord(const UUID& client_id,
                                              Clock::time_point now);
    bool EraseRecord(const UUID& client_id, const Record& expected);
    void AdoptRecord(const UUID& client_id, Record record);
    void ClearRecords();
    void ClearRemountState(const UUID& client_id);

    struct Notifications {
        std::mutex mutex;
        std::condition_variable cv;
        std::deque<ClientSessionEvent> events;
        bool stopping{false};
        bool accepting{true};
        bool listeners_frozen{false};
    };
    void Bind(const UUID& client_id, const Record& record);
    void EraseHostId(const UUID& client_id);
    void Dispatch();
    void ThreadFunc();

    mutable std::shared_mutex mutex_;
    Records records_;
    std::unordered_set<UUID, boost::hash<UUID>> remount_completed_clients_;
    // Independent of the registry lock; never acquire the registry lock while
    // holding this mutex. Host lookups do not need a session transaction.
    mutable std::shared_mutex host_mutex_;
    std::unordered_map<UUID, std::string, boost::hash<UUID>> host_ids_;
    const Clock::duration active_ttl_;
    const Clock::duration suspicion_ttl_;
    const std::function<void()> reserve_retirement_;
    std::vector<Listener> listeners_;
    std::shared_ptr<Notifications> notifications_ =
        std::make_shared<Notifications>();
    std::unique_ptr<ClientOffboardingWorker> offboarding_worker_;
    std::thread thread_;
};

class ClientSessionManager::SessionGuard {
   public:
    SessionGuard(const SessionGuard&) = delete;
    SessionGuard& operator=(const SessionGuard&) = delete;
    SessionGuard(SessionGuard&&) noexcept = default;
    SessionGuard& operator=(SessionGuard&&) = delete;

    // A read-only identity, not permission to mutate liveness or reacquire its
    // lock. Copying it retains the record, but does not extend admission.
    ClientSessionSharedPtr Session() const { return record_; }

   private:
    friend class ClientSessionManager;
    SessionGuard(Record record, ClientLivenessRecord::RetainingGuard guard)
        : record_(std::move(record)), guard_(std::move(guard)) {}

    // Destruction must unlock before releasing the last record reference.
    Record record_;
    ClientLivenessRecord::RetainingGuard guard_;
};

class ClientSessionManager::Registration {
   public:
    ~Registration();
    Registration(const Registration&) = delete;
    Registration& operator=(const Registration&) = delete;
    Registration(Registration&& other) noexcept;
    Registration& operator=(Registration&&) = delete;

    ClientSessionSharedPtr Session() const { return record_; }
    // Whether this incarnation still owes a successful remount handshake.
    // Independent of ACTIVE/SUSPECTED: a suspected session may need no remount.
    bool NeedsRemount() const;
    // Updates immediately; empty hints preserve the previous value. Rollback
    // erases the hint only when the session itself is provisional.
    void UpdateHostId(const std::string& host_id);
    // Exactly one commit, with semantics fixed by the Begin* call.
    // Destruction without Commit removes only a provisional session, not
    // existing state or external resources. This is not a general rollback
    // transaction. Commit retains both locks until scope exit.
    void Commit();

   private:
    friend class ClientSessionManager;
    Registration(ClientSessionManager& manager, const UUID& client_id,
                 RegistrationKind kind,
                 std::optional<Clock::time_point> observed_at = std::nullopt);

    ClientSessionManager* manager_;
    std::unique_lock<std::shared_mutex> registry_lock_;
    UUID client_id_;
    const RegistrationKind kind_;
    Clock::time_point observed_at_;
    Record record_;
    std::optional<ClientLivenessRecord::RetainingGuard> guard_;
    bool provisional_{false};
    bool committed_{false};
};

class ClientSessionManager::RestoreBatch {
   public:
    ~RestoreBatch() = default;
    RestoreBatch(const RestoreBatch&) = delete;
    RestoreBatch& operator=(const RestoreBatch&) = delete;
    RestoreBatch(RestoreBatch&& other) noexcept;
    RestoreBatch& operator=(RestoreBatch&&) = delete;

    Record FindOrCreate(const UUID& client_id);
    void Commit();

   private:
    friend class ClientSessionManager;
    explicit RestoreBatch(ClientSessionManager& manager);

    ClientSessionManager* manager_;
    std::unique_lock<std::shared_mutex> registry_lock_;
    Records pending_;
    bool committed_{false};
};

}  // namespace mooncake
