#pragma once

#include <boost/functional/hash.hpp>

#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "client_liveness.h"
#include "types.h"

namespace mooncake {
namespace test {
class MasterServiceTestPeer;
class ClientSessionRegistryTestPeer;
}  // namespace test

// Owns client session membership, remount readiness, host hints, registration
// rollback and staged restore. This intermediate step deliberately retains the
// global registration lock and per-record liveness transition guards;
// per-client admission and heartbeat isolation follow separately.
class ClientSessionRegistry {
    using Record = std::shared_ptr<ClientLivenessRecord>;
    using Records = std::unordered_map<UUID, Record, boost::hash<UUID>>;
    enum class RegistrationKind { Register, Remount };

   public:
    using Clock = ClientLivenessRecord::Clock;

    ClientSessionRegistry() = default;
    ~ClientSessionRegistry() { Reset(); }
    ClientSessionRegistry(const ClientSessionRegistry&) = delete;
    ClientSessionRegistry& operator=(const ClientSessionRegistry&) = delete;

    class SessionGuard;
    class Registration;
    class RestoreBatch;

    // Combines lookup and liveness admission. Serving accepts ACTIVE only;
    // retaining accepts ACTIVE and SUSPECTED (rejects missing or OFFLINE).
    [[nodiscard]] std::optional<SessionGuard> TryAcquireServingSession(
        const UUID& id) const;
    [[nodiscard]] std::optional<SessionGuard> TryAcquireRetainingSession(
        const UUID& id) const;

    // Opens an exclusive registration or remount scope, provisionally creating
    // an ACTIVE session if missing, or rejecting admission if OFFLINE.
    // Holds the registry write lock and session retaining guard until the scope
    // exits (including after Commit()).
    // - BeginRegistration: Commit() renews liveness at commit time (recovering
    //   SUSPECTED). An explicit observed_at overrides the clock for tests.
    // - BeginRemount: Commit() marks remount readiness without renewing
    //   liveness.
    [[nodiscard]] std::optional<Registration> BeginRegistration(
        const UUID& id,
        std::optional<Clock::time_point> observed_at = std::nullopt);
    [[nodiscard]] std::optional<Registration> BeginRemount(const UUID& id);

    // Stages restored local-disk owners under the exclusive registry lock until
    // Commit(). Holds the registry lock for the full scope lifetime; destroying
    // without Commit() discards all staged records.
    [[nodiscard]] RestoreBatch BeginRestore();

    // Removes the current session only if it still matches expected, waiting
    // for in-flight transition/retaining guards on that incarnation to exit.
    bool Remove(const UUID& id, const ClientSessionSharedPtr& expected);

    // Host hints live and die with the retaining incarnation. Empty hints
    // preserve the existing value; missing or OFFLINE sessions are ignored.
    void UpdateHostId(const UUID& id, const std::string& host_id);
    std::string GetHostId(const UUID& id) const;

    // Refreshes liveness even before remount completes. Returns OK only when
    // the observation is accepted and remount readiness has been committed.
    ClientStatus Ping(const UUID& id);

    // Evaluates TTL transitions across a snapshot of current sessions. Clears
    // readiness and host hints before invoking retire on BECAME_OFFLINE.
    void ExpireSessions(
        Clock::time_point now, Clock::duration active_ttl,
        Clock::duration suspicion_ttl,
        const std::function<void()>& reserve_retirement = {},
        const std::function<void(const UUID&, const ClientSessionSharedPtr&)>&
            retire = {});

    void Reset();

   private:
    friend class test::MasterServiceTestPeer;
    friend class test::ClientSessionRegistryTestPeer;

    std::optional<SessionGuard> TryAcquireSession(const UUID& id,
                                                  bool require_serving) const;
    std::optional<Registration> Begin(
        const UUID& id, RegistrationKind kind,
        std::optional<Clock::time_point> observed_at);
    Record Find(const UUID& id) const;
    void ClearSessionMetadataLocked(const UUID& id);
    void EraseLocked(const UUID& id, const Record& record);

    mutable std::shared_mutex mutex_;
    Records records_;
    std::unordered_set<UUID, boost::hash<UUID>> ready_;
    std::unordered_map<UUID, std::string, boost::hash<UUID>> host_ids_;
};

class ClientSessionRegistry::SessionGuard {
   public:
    SessionGuard(const SessionGuard&) = delete;
    SessionGuard& operator=(const SessionGuard&) = delete;
    SessionGuard(SessionGuard&&) noexcept = default;
    SessionGuard& operator=(SessionGuard&&) = delete;

    // Read-only session identity; copying the shared_ptr retains the record
    // object but does not extend admission beyond this guard's lifetime.
    ClientSessionSharedPtr Session() const { return record_; }

   private:
    friend class ClientSessionRegistry;
    SessionGuard(Record record, ClientLivenessRecord::RetainingGuard guard)
        : record_(std::move(record)), guard_(std::move(guard)) {}

    // Destruction order is load-bearing: guard_ must unlock transition_mutex_
    // before record_ can drop the last ClientLivenessRecord reference.
    Record record_;
    ClientLivenessRecord::RetainingGuard guard_;
};

class ClientSessionRegistry::Registration {
   public:
    ~Registration();
    Registration(const Registration&) = delete;
    Registration& operator=(const Registration&) = delete;
    Registration(Registration&&) noexcept = default;
    Registration& operator=(Registration&&) = delete;

    ClientSessionSharedPtr Session() const { return record_; }
    bool NeedsRemount() const;
    // Stages a non-empty host hint before Commit() (or applies it directly if
    // already committed while the scope is still held).
    void UpdateHostId(const std::string& host_id);
    // Publishes registration/remount state and any staged host hint. May be
    // called at most once; the registry lock remains held until scope exit.
    // Destroying without Commit() rolls back provisional sessions and discards
    // staged host hints without disturbing an already-registered session.
    void Commit();

   private:
    friend class ClientSessionRegistry;
    Registration(ClientSessionRegistry& registry,
                 std::unique_lock<std::shared_mutex> lock, const UUID& id,
                 RegistrationKind kind,
                 std::optional<Clock::time_point> observed_at, Record record,
                 ClientLivenessRecord::RetainingGuard guard, bool provisional);

    ClientSessionRegistry& registry_;
    // Destruction order is load-bearing: guard_ unlocks transition_mutex_
    // before record_ releases the ClientLivenessRecord, and both complete
    // before lock_ releases registry_.mutex_.
    std::unique_lock<std::shared_mutex> lock_;
    UUID id_;
    RegistrationKind kind_;
    std::optional<Clock::time_point> observed_at_;
    std::optional<std::string> pending_host_id_;
    Record record_;
    ClientLivenessRecord::RetainingGuard guard_;
    bool provisional_{false};
    bool committed_{false};
};

class ClientSessionRegistry::RestoreBatch {
   public:
    ~RestoreBatch() = default;
    RestoreBatch(const RestoreBatch&) = delete;
    RestoreBatch& operator=(const RestoreBatch&) = delete;
    RestoreBatch(RestoreBatch&&) noexcept = default;
    RestoreBatch& operator=(RestoreBatch&&) = delete;

    ClientSessionSharedPtr FindOrCreate(const UUID& id);
    void Commit();

   private:
    friend class ClientSessionRegistry;
    explicit RestoreBatch(ClientSessionRegistry& registry)
        : registry_(registry), lock_(registry.mutex_) {}

    ClientSessionRegistry& registry_;
    std::unique_lock<std::shared_mutex> lock_;
    Records pending_;
    bool committed_{false};
};

}  // namespace mooncake
