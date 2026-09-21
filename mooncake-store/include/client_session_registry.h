#pragma once

#include <boost/functional/hash.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "client_liveness.h"
#include "client_session_event_dispatcher.h"
#include "types.h"

namespace mooncake {

namespace test {
class ClientSessionRegistryTestPeer;
}  // namespace test

// Owns client session identity: which incarnation is registered under a client
// ID, whether it may serve or retain resources, the scopes that keep one
// incarnation stable while an operation runs, and the TTL expiry of sessions
// that stop heartbeating. It also owns the delivery of the resulting liveness
// transitions: publication happens under the record's transition lock, but
// listeners run on the owned delivery thread, outside every registry lock, so
// a listener may take segment, metadata or snapshot locks and may reenter the
// registry.
class ClientSessionRegistry {
   private:
    enum class RegistrationKind { Register, Remount };

   public:
    using Record = std::shared_ptr<ClientLivenessRecord>;
    using Records = std::unordered_map<UUID, Record, boost::hash<UUID>>;
    using Clock = ClientLivenessRecord::Clock;

    // Records may outlive the registry: removal and destruction stop observing
    // them first, so their later transitions are dropped. A session unobserved
    // for active_ttl becomes SUSPECTED, and OFFLINE suspicion_ttl later; the
    // defaults never expire.
    explicit ClientSessionRegistry(
        Clock::duration active_ttl = Clock::duration::max(),
        Clock::duration suspicion_ttl = Clock::duration::max());
    ~ClientSessionRegistry();
    ClientSessionRegistry(const ClientSessionRegistry&) = delete;
    ClientSessionRegistry& operator=(const ClientSessionRegistry&) = delete;

    // Host hints may arrive before registration. These methods synchronize
    // internally and may also be called within a registry transaction. Empty
    // updates preserve the previous hint; OFFLINE/removal/reset erase it.
    // Prefer Registration::UpdateHostId within a registration scope.
    void UpdateHostId(const UUID& client_id, const std::string& host_id);
    std::string GetHostId(const UUID& client_id) const;

    // Admission retains the slot and shares its operation lock, not the
    // registry or liveness transition lock. Operations on one client run
    // concurrently; only a state transition (expiry, registration, removal)
    // excludes them, so admission stays stable for the whole scope. Ping needs
    // neither. Acquisition waits only for such a transition. An empty result
    // means the client is missing or rejects admission (not lock contention).
    // Serving accepts ACTIVE; retaining accepts ACTIVE and SUSPECTED.
    // Do not hold one scope for a client while acquiring another for the same
    // client: a queued transition stops new admissions, so the outer scope
    // would wait for itself.
    class SessionGuard;
    [[nodiscard]] std::optional<SessionGuard> TryAcquireServingSession(
        const UUID& client_id) const;
    [[nodiscard]] std::optional<SessionGuard> TryAcquireRetainingSession(
        const UUID& client_id) const;

    // Waits for this incarnation's registration/resource scopes to exit.
    // Never call while holding one of those scopes for the same session.
    bool Remove(const UUID& client_id, const ClientSessionSharedPtr& expected);

    // Scopes serialize registration/remount for one incarnation only. Ping
    // remains independent; monitoring defers transitions for this incarnation
    // until scope exit. Do not nest registration scopes for the same client.
    // A missing session is provisionally created; OFFLINE rejects admission.
    // The registry must outlive these scopes.
    class Registration;
    // Successful registration renews liveness (and recovers SUSPECTED).
    [[nodiscard]] std::optional<Registration> BeginRegistration(
        const UUID& client_id,
        std::optional<Clock::time_point> observed_at = std::nullopt);
    // Successful remount marks readiness without renewing/reviving liveness.
    [[nodiscard]] std::optional<Registration> BeginRemount(
        const UUID& client_id);

    // Stage restored owners under the exclusive lifecycle lock. Commit adopts
    // them atomically under the registry lock; destruction without Commit
    // discards them. Resource rollback is the caller's job. Commit retains the
    // lifecycle lock; the registry must outlive the scope. Existing records are
    // reused, not replaced. Lookups and heartbeats remain independent.
    class RestoreBatch;
    [[nodiscard]] RestoreBatch BeginRestore();

    // Observes liveness even before remount completes. OK requires an accepted
    // observation, the current registry identity, and a completed remount.
    ClientStatus Ping(const UUID& client_id);
    // Whether a terminal incarnation still awaits Remove, i.e. its resource
    // cleanup has not converged. OFFLINE is itself the barrier: whoever can
    // observe it can also observe this, with no separate reservation.
    bool HasRetiredSession() const;
    // Whole-registry reset/replacement is only allowed once monitoring and
    // event delivery are stopped.
    void Reset(Records records = {});

    // Register before the first Start; a listener added later is rejected.
    // Listeners run serially, in registration order, on the delivery thread,
    // and must not throw. Their owners must outlive Stop.
    [[nodiscard]] bool AddListener(
        ClientSessionEventDispatcher::Listener listener);

    // Enable expiry and delivery. Stop tears them down in the order that
    // preserves side effects: expiry stops publishing first, then queued
    // transitions are delivered and the delivery thread joins. Stop rejects
    // later transitions until Start, and is idempotent. Do not call Stop from
    // a listener. The destructor stops too, but that is too late for listeners
    // that reach state destroyed before this registry: those owners must call
    // Stop themselves, before that state goes away.
    void Start();
    void Stop();

   private:
    // Drop readiness and host state for a terminal incarnation. Runs from this
    // registry's own listener, registered first so no other listener can see
    // the event earlier, outside the record lock, and ignores an event that a
    // newer incarnation has already replaced.
    void OnSessionRetired(const UUID& client_id,
                          const ClientSessionSharedPtr& session);

    friend class test::ClientSessionRegistryTestPeer;

    static constexpr std::chrono::seconds kExpiryScanInterval{1};

    // A replaceable registry entry, not a permanent per-UUID allocation.
    // Scopes retain the slot so its mutex and liveness outlive their guards.
    struct ClientSlot {
        explicit ClientSlot(Record record) : liveness(std::move(record)) {}

        const Record liveness;
        // Exclusive acquirers hold this while they queue for operation_mutex,
        // and shared acquirers pass through it first. A shared_mutex may
        // prefer readers (the default on glibc), so without the gate a stream
        // of overlapping operations could postpone a transition forever.
        std::mutex admission_gate;
        // Shared by operations, exclusive for registration, removal and
        // expiry.
        std::shared_mutex operation_mutex;
        // Consecutive expiry passes that found this slot busy. Monitoring
        // reads and writes it while scanning; nothing else touches it.
        std::atomic<unsigned> busy_expiry_passes{0};
        // Protected by the registry mutex, not operation_mutex: Ping must not
        // wait for long registration/resource work.
        bool remount_completed{false};
    };
    using Slot = std::shared_ptr<ClientSlot>;
    using Slots = std::unordered_map<UUID, Slot, boost::hash<UUID>>;
    using SharedOperation = std::shared_lock<std::shared_mutex>;
    using ExclusiveOperation = std::unique_lock<std::shared_mutex>;

    // Lock order: admission gate -> slot operation -> registry, or slot
    // operation -> transition. Never acquire an operation under the registry
    // lock, or nest registry and transition. The caller retains slot until the
    // returned operation is released. Empty means missing/replaced, or busy
    // when try_lock is requested; these validate slot identity only, not
    // liveness admission.
    std::optional<SharedOperation> AcquireSharedOperation(
        const UUID& client_id, const Slot& slot) const;
    std::optional<ExclusiveOperation> AcquireExclusiveOperation(
        const UUID& client_id, const Slot& slot, bool try_lock = false) const;
    Slot FindSlot(const UUID& client_id) const;
    Slots SnapshotSlots() const;
    std::optional<Registration> Begin(
        const UUID& client_id, RegistrationKind kind,
        std::optional<Clock::time_point> observed_at);

    // These helpers require the registry lock on entry.
    Slot FindSlotLocked(const UUID& client_id) const;
    void AdoptSlot(const UUID& client_id, const Slot& slot);
    void ClearRemountState(const Slot& slot);
    // Requires exclusive lifecycle access; binds before taking registry lock.
    void AdoptRecords(const Records& records);
    // Requires a validated current slot operation and lifecycle access.
    void EraseSlot(const UUID& client_id, const Slot& slot,
                   const ExclusiveOperation& operation);
    void ClearSlots();

    void Bind(const UUID& client_id, const Record& record);
    void EraseHostId(const UUID& client_id);
    // One deterministic expiry pass. Applies the TTLs to every session,
    // excluding its operations for the evaluation. A busy session is retried
    // on the next pass; one that stays busy is eventually waited for, so
    // continuous admission cannot postpone it forever. The whole pass is
    // skipped while restore/reset holds the lifecycle barrier -- expiry is
    // frozen there, heartbeats and lookups are not.
    void ExpireSessions(Clock::time_point now);
    void ExpiryThreadFunc();

    // Membership-changing operations take this before any other lock.
    // Registration/removal share it; restore/reset own it exclusively.
    // Ping/admission never need it; monitoring only tries shared access.
    mutable std::shared_mutex lifecycle_mutex_;
    mutable std::shared_mutex mutex_;
    Slots slots_;
    // Independent of the registry lock; never acquire the registry lock while
    // holding this mutex. Host lookups do not need a session transaction.
    mutable std::shared_mutex host_mutex_;
    std::unordered_map<UUID, std::string, boost::hash<UUID>> host_ids_;
    // Published to by the observer of every bound record. EraseSlot stops
    // observing before a record leaves the registry, and the destructor stops
    // delivery before erasing, so no observer reaches it after destruction.
    ClientSessionEventDispatcher events_;

    const Clock::duration active_ttl_;
    const Clock::duration suspicion_ttl_;
    std::mutex expiry_mutex_;
    std::condition_variable expiry_cv_;
    bool expiry_stopping_{false};
    std::thread expiry_thread_;
};

class ClientSessionRegistry::SessionGuard {
   public:
    SessionGuard(const SessionGuard&) = delete;
    SessionGuard& operator=(const SessionGuard&) = delete;
    SessionGuard(SessionGuard&&) noexcept = default;
    SessionGuard& operator=(SessionGuard&&) = delete;

    // A read-only identity, not permission to mutate liveness or reacquire its
    // lock. Copying it retains the record, but does not extend admission.
    ClientSessionSharedPtr Session() const {
        return slot_ ? slot_->liveness : nullptr;
    }

   private:
    friend class ClientSessionRegistry;
    SessionGuard(Slot slot, SharedOperation operation)
        : slot_(std::move(slot)), operation_(std::move(operation)) {}

    // Destruction unlocks operation before releasing slot.
    Slot slot_;
    SharedOperation operation_;
};

class ClientSessionRegistry::Registration {
   public:
    ~Registration();
    Registration(const Registration&) = delete;
    Registration& operator=(const Registration&) = delete;
    Registration(Registration&&) noexcept = default;
    Registration& operator=(Registration&&) = delete;

    ClientSessionSharedPtr Session() const {
        return slot_ ? slot_->liveness : nullptr;
    }
    // Whether this incarnation still owes a successful remount handshake.
    // Independent of ACTIVE/SUSPECTED: a suspected session may need no remount.
    bool NeedsRemount() const;
    // Updates immediately; empty hints preserve the previous value. Rollback
    // erases the hint only when the session itself is provisional.
    void UpdateHostId(const std::string& host_id);
    // Exactly one commit, with semantics fixed by the Begin* call.
    // Destruction without Commit removes only a provisional session, not
    // existing state or external resources. This is not a general rollback
    // transaction. Commit retains per-incarnation serialization until exit.
    void Commit();

   private:
    friend class ClientSessionRegistry;
    Registration(ClientSessionRegistry& registry, const UUID& client_id,
                 RegistrationKind kind,
                 std::optional<Clock::time_point> observed_at);

    ClientSessionRegistry& registry_;
    std::shared_lock<std::shared_mutex> lifecycle_lock_;
    UUID client_id_;
    const RegistrationKind kind_;
    Clock::time_point observed_at_;
    Slot slot_;
    ExclusiveOperation operation_;
    bool admitted_{false};
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

    Record FindOrCreate(const UUID& client_id);
    void Commit();

   private:
    friend class ClientSessionRegistry;
    explicit RestoreBatch(ClientSessionRegistry& registry);

    ClientSessionRegistry& registry_;
    std::unique_lock<std::shared_mutex> lifecycle_lock_;
    Records pending_;
    bool committed_{false};
};

}  // namespace mooncake
