#pragma once

#include <boost/functional/hash.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

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
// that stop heartbeating. It is the only mutator of the liveness records it
// hands out as read-only identities, so every transition is one it caused
// itself, and it owns what follows from one: its gauges, its own state for a
// terminal incarnation, and delivery to listeners. Listeners run on the owned
// delivery thread, outside every registry lock, so a listener may take
// segment, metadata or snapshot locks and may reenter the registry.
class ClientSessionRegistry {
   private:
    enum class RegistrationKind { Register, Remount };
    using Record = std::shared_ptr<ClientLivenessRecord>;
    using Records = std::unordered_map<UUID, Record, boost::hash<UUID>>;

   public:
    using Clock = ClientLivenessRecord::Clock;

    // A session unobserved for active_ttl becomes SUSPECTED, and OFFLINE
    // suspicion_ttl later; the defaults never expire. Identities may outlive
    // the registry: a record that left it is never mutated again.
    // Optional expiry admission is checked outside registry locks before each
    // pass, so an owner can pause expiry during recovery. It must not throw
    // and its captures must outlive Stop.
    explicit ClientSessionRegistry(
        Clock::duration active_ttl = Clock::duration::max(),
        Clock::duration suspicion_ttl = Clock::duration::max(),
        std::function<bool()> admits_expiry = {});
    ~ClientSessionRegistry();
    ClientSessionRegistry(const ClientSessionRegistry&) = delete;
    ClientSessionRegistry& operator=(const ClientSessionRegistry&) = delete;

    // The host hint is part of the session, so it lives and dies with the
    // incarnation: OFFLINE, removal and reset drop it, and a hint for a client
    // without a session is ignored. Empty updates preserve the previous hint.
    // These synchronize internally and need no session scope; within a
    // registration scope use Registration::UpdateHostId.
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
    // Successful registration renews liveness at commit time (and recovers
    // SUSPECTED). An explicit observed_at overrides the clock for tests.
    [[nodiscard]] std::optional<Registration> BeginRegistration(
        const UUID& client_id,
        std::optional<Clock::time_point> observed_at = std::nullopt);
    // Successful remount marks readiness without renewing/reviving liveness.
    [[nodiscard]] std::optional<Registration> BeginRemount(
        const UUID& client_id);

    // The one way restored resources get an owner session, for snapshot and
    // standby restore alike. Stages owners under the exclusive lifecycle lock.
    // Commit adopts them atomically under the registry lock; destruction
    // without Commit discards them. Resource rollback is the caller's job.
    // Commit retains the lifecycle lock; the registry must outlive the scope.
    // Existing sessions are reused, not replaced. Lookups and heartbeats remain
    // independent.
    class RestoreBatch;
    [[nodiscard]] RestoreBatch BeginRestore();

    // Observes liveness even before remount completes. OK requires an accepted
    // observation, the current registry identity, and a completed remount.
    ClientStatus Ping(const UUID& client_id);
    // Whether a terminal incarnation still awaits Remove, i.e. its resource
    // cleanup has not converged. OFFLINE is itself the barrier: whoever can
    // observe it can also observe this, with no separate reservation.
    bool HasRetiredSession() const;
    // Drops every session. Only allowed once monitoring and event delivery
    // are stopped.
    void Reset();

    // Register before the first Start; a listener added later is rejected.
    // Listeners run serially, in registration order, on the delivery thread,
    // and must not throw. Their owners must outlive Stop. OFFLINE is delivered
    // once per incarnation, after the registry dropped its own readiness and
    // host state for it. A recovery and a suspicion of one session come from
    // different threads and may be delivered out of order; read
    // event.session for the state that holds now.
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
    friend class test::ClientSessionRegistryTestPeer;

    static constexpr std::chrono::seconds kExpiryScanInterval{1};
    // A std::shared_mutex may prefer readers (the default on glibc), which
    // would let overlapping operations postpone a state transition forever.
    // Exclusive acquirers increment waiting_writers_ while queuing for mutex_,
    // and overdue expiry scans close the turnstile via expiry_draining_ without
    // blocking the shared scanner thread so in-flight operations drain in
    // bounded time.
    class TurnstileSharedMutex {
       public:
        TurnstileSharedMutex() = default;
        TurnstileSharedMutex(const TurnstileSharedMutex&) = delete;
        TurnstileSharedMutex& operator=(const TurnstileSharedMutex&) = delete;

        void lock() {
            {
                const std::lock_guard<std::mutex> gate(gate_mutex_);
                ++waiting_writers_;
            }
            mutex_.lock();
            {
                const std::lock_guard<std::mutex> gate(gate_mutex_);
                if (--waiting_writers_ == 0 && !expiry_draining_) {
                    gate_cv_.notify_all();
                }
            }
        }
        bool try_lock() {
            const std::lock_guard<std::mutex> gate(gate_mutex_);
            if (closed_ || waiting_writers_ > 0 || !mutex_.try_lock()) {
                return false;
            }
            if (expiry_draining_) {
                expiry_draining_ = false;
                gate_cv_.notify_all();
            }
            return true;
        }
        bool try_lock_or_drain() {
            const std::lock_guard<std::mutex> gate(gate_mutex_);
            if (closed_) {
                return false;
            }
            if (waiting_writers_ == 0 && mutex_.try_lock()) {
                if (expiry_draining_) {
                    expiry_draining_ = false;
                    gate_cv_.notify_all();
                }
                return true;
            }
            expiry_draining_ = true;
            return false;
        }
        void cancel_drain() {
            const std::lock_guard<std::mutex> gate(gate_mutex_);
            if (expiry_draining_) {
                expiry_draining_ = false;
                if (waiting_writers_ == 0) {
                    gate_cv_.notify_all();
                }
            }
        }
        void close_permanently() {
            const std::lock_guard<std::mutex> gate(gate_mutex_);
            closed_ = true;
            expiry_draining_ = false;
            gate_cv_.notify_all();
        }
        void unlock() { mutex_.unlock(); }

        void lock_shared() {
            {
                std::unique_lock<std::mutex> gate(gate_mutex_);
                gate_cv_.wait(gate, [this] {
                    return closed_ ||
                           (waiting_writers_ == 0 && !expiry_draining_);
                });
            }
            mutex_.lock_shared();
        }
        bool try_lock_shared() {
            const std::lock_guard<std::mutex> gate(gate_mutex_);
            return !closed_ && waiting_writers_ == 0 && !expiry_draining_ &&
                   mutex_.try_lock_shared();
        }
        void unlock_shared() { mutex_.unlock_shared(); }

       private:
        std::mutex gate_mutex_;
        std::condition_variable gate_cv_;
        size_t waiting_writers_{0};
        bool expiry_draining_{false};
        bool closed_{false};
        std::shared_mutex mutex_;
    };

    using SharedOperation = std::shared_lock<TurnstileSharedMutex>;
    using ExclusiveOperation = std::unique_lock<TurnstileSharedMutex>;

    // A replaceable registry entry, not a permanent per-UUID allocation.
    // Scopes retain the slot so its mutex and liveness outlive their guards.
    struct ClientSlot {
        explicit ClientSlot(Record record) : liveness(std::move(record)) {}

        // Empty if erased, or busy when try_lock is requested; validates slot
        // identity only, not liveness admission.
        std::optional<SharedOperation> AcquireShared();
        std::optional<ExclusiveOperation> AcquireExclusive(
            bool try_lock = false);
        std::optional<ExclusiveOperation> AcquireExclusiveForExpiry(
            bool drain_if_busy);
        void CancelExpiryDrain();

        const Record liveness;
        // Shared by operations; exclusive for registration, removal and expiry.
        TurnstileSharedMutex operation_mutex;
        // Set under exclusive operation_mutex in EraseSlot; once erased, a
        // slot never re-enters the registry.
        bool erased{false};
        // Consecutive expiry passes that found this slot busy while expired.
        // Reset on expiry evaluation or liveness observation.
        std::atomic<unsigned> busy_expiry_passes{0};
        // Protected by the registry mutex, not operation_mutex: Ping and host
        // hints must not wait for long registration/resource work.
        bool remount_completed{false};
        std::string host_id;
    };
    using Slot = std::shared_ptr<ClientSlot>;
    using Slots = std::unordered_map<UUID, Slot, boost::hash<UUID>>;
    using SlotSnapshot = std::vector<std::pair<UUID, Slot>>;

    // Lock order: lifecycle -> slot operation -> registry -> transition.
    // Never acquire an operation under the registry lock. A record's
    // transition lock is a leaf.
    std::optional<SessionGuard> TryAcquireSession(const UUID& client_id,
                                                  bool require_serving) const;
    Slot FindSlot(const UUID& client_id) const;
    SlotSnapshot SnapshotSlots() const;
    std::optional<Registration> Begin(
        const UUID& client_id, RegistrationKind kind,
        std::optional<Clock::time_point> observed_at);

    // These helpers require the registry lock on entry.
    Slot FindSlotLocked(const UUID& client_id) const;
    void AdoptSlot(const UUID& client_id, const Slot& slot);
    void SetSlotHostIdLocked(const Slot& slot, std::string host_id);
    void ClearSlotStateLocked(const Slot& slot);
    // Requires exclusive lifecycle access.
    void AdoptRecords(const Records& records);
    // Requires a validated current slot operation and lifecycle access.
    void EraseSlot(const UUID& client_id, const Slot& slot,
                   const ExclusiveOperation& operation);

    // Ping at an explicit time.
    ClientStatus Observe(const UUID& client_id, Clock::time_point now);
    bool ObserveSlot(const UUID& client_id, const Slot& slot,
                     Clock::time_point now);
    // Gauges for a transition this registry just caused. The
    // caller excludes EraseSlot, through the slot's exclusive operation or the
    // registry lock, so a removed session is counted in exactly one state.
    void PublishTransition(const UUID& client_id, const Slot& slot,
                           ClientLivenessState previous,
                           ClientLivenessState current);
    // One deterministic expiry pass. Applies the TTLs to every session,
    // excluding its operations for the evaluation. A busy session is retried
    // on the next pass; a persistently busy slot closes its admission gate
    // without blocking the scanner. The whole pass is
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
    ClientSessionEventDispatcher events_;

    const Clock::duration active_ttl_;
    const Clock::duration suspicion_ttl_;
    const std::function<bool()> admits_expiry_;
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
    // Stages a non-empty host hint before Commit() (or applies it directly if
    // already committed while the scope is still held). Empty hints preserve
    // the previous value. Rolling back a provisional scope or abandoning an
    // uncommitted update discards the staged hint.
    void UpdateHostId(const std::string& host_id);
    // Exactly one commit, with semantics fixed by the Begin* call.
    // Destruction without Commit removes only a provisional session, not
    // existing state or external resources. This is not a general rollback
    // transaction. Commit retains per-incarnation serialization until exit.
    void Commit();

   private:
    friend class ClientSessionRegistry;
    Registration(ClientSessionRegistry& registry,
                 std::shared_lock<std::shared_mutex> lifecycle_lock,
                 const UUID& client_id, RegistrationKind kind,
                 std::optional<Clock::time_point> observed_at, Slot slot,
                 ExclusiveOperation operation, bool provisional);

    ClientSessionRegistry& registry_;
    std::shared_lock<std::shared_mutex> lifecycle_lock_;
    UUID client_id_;
    const RegistrationKind kind_;
    const std::optional<Clock::time_point> observed_at_;
    std::optional<std::string> pending_host_id_;
    Slot slot_;
    ExclusiveOperation operation_;
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

    // The owner identity to bind restored resources to: the registered
    // session, or one staged until Commit.
    ClientSessionSharedPtr FindOrCreate(const UUID& client_id);
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
