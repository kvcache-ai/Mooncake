#include "client_session_registry.h"

#include <glog/logging.h>

#include <algorithm>

#include "master_metric_manager.h"

namespace mooncake {
namespace {

// Passes a busy slot may be skipped before the scan queues for it instead.
constexpr unsigned kBusyExpiryPassesBeforeWaiting = 3;
// Slots that may be waited for in one pass, so a single long operation cannot
// stall the rest of the scan.
constexpr int kMaxWaitingExpiryAcquisitionsPerPass = 4;

}  // namespace

ClientSessionRegistry::ClientSessionRegistry(Clock::duration active_ttl,
                                             Clock::duration suspicion_ttl)
    : active_ttl_(active_ttl), suspicion_ttl_(suspicion_ttl) {
    // Registered first, so the registry has dropped its own state for a
    // terminal incarnation before any resource owner acts on the event.
    CHECK(events_.AddListener([this](const ClientSessionEvent& event) {
        if (event.current == ClientLivenessState::OFFLINE) {
            OnSessionRetired(event.client_id, event.session);
        }
    }));
}

ClientSessionRegistry::~ClientSessionRegistry() {
    Stop();
    std::unique_lock lifecycle_lock(lifecycle_mutex_);
    ClearSlots();
}

bool ClientSessionRegistry::AddListener(
    ClientSessionEventDispatcher::Listener listener) {
    return events_.AddListener(std::move(listener));
}

void ClientSessionRegistry::Start() {
    CHECK(!expiry_thread_.joinable());
    events_.Start();
    {
        std::lock_guard lock(expiry_mutex_);
        expiry_stopping_ = false;
    }
    expiry_thread_ =
        std::thread(&ClientSessionRegistry::ExpiryThreadFunc, this);
}

void ClientSessionRegistry::Stop() {
    // Stop publishing before draining, so no transition loses its listeners.
    {
        std::lock_guard lock(expiry_mutex_);
        expiry_stopping_ = true;
    }
    expiry_cv_.notify_one();
    if (expiry_thread_.joinable()) {
        expiry_thread_.join();
    }
    events_.Stop();
}

void ClientSessionRegistry::Bind(const UUID& client_id, const Record& record) {
    // Capturing this is safe: EraseSlot stops observing before a bound record
    // leaves the registry, and the destructor erases every slot.
    std::weak_ptr<ClientLivenessRecord> session = record;
    record->SetTransitionObserver(
        [this, session, client_id](ClientLivenessState previous,
                                   ClientLivenessState current) {
            auto record = session.lock();
            if (!record) {
                return;
            }
            // Update gauges in transition order, rather than at asynchronous
            // delivery time (a recovered session may already expire again).
            auto& metrics = MasterMetricManager::instance();
            switch (current) {
                case ClientLivenessState::ACTIVE:
                    metrics.client_liveness_recovered();
                    break;
                case ClientLivenessState::SUSPECTED:
                    metrics.client_liveness_became_suspected();
                    break;
                case ClientLivenessState::OFFLINE:
                    metrics.client_liveness_became_offline();
                    break;
            }
            events_.Publish({client_id, std::move(record), previous, current});
        });
}

void ClientSessionRegistry::UpdateHostId(const UUID& client_id,
                                         const std::string& host_id) {
    if (host_id.empty()) {
        return;
    }
    {
        std::shared_lock lock(host_mutex_);
        const auto it = host_ids_.find(client_id);
        if (it != host_ids_.end() && it->second == host_id) {
            return;
        }
    }
    std::unique_lock lock(host_mutex_);
    host_ids_[client_id] = host_id;
}

std::string ClientSessionRegistry::GetHostId(const UUID& client_id) const {
    std::shared_lock lock(host_mutex_);
    const auto it = host_ids_.find(client_id);
    return it == host_ids_.end() ? std::string() : it->second;
}

void ClientSessionRegistry::EraseHostId(const UUID& client_id) {
    std::unique_lock lock(host_mutex_);
    host_ids_.erase(client_id);
}

ClientSessionRegistry::Slot ClientSessionRegistry::FindSlot(
    const UUID& client_id) const {
    std::shared_lock lock(mutex_);
    return FindSlotLocked(client_id);
}

ClientSessionRegistry::Slot ClientSessionRegistry::FindSlotLocked(
    const UUID& client_id) const {
    const auto it = slots_.find(client_id);
    return it == slots_.end() ? nullptr : it->second;
}

std::optional<ClientSessionRegistry::SharedOperation>
ClientSessionRegistry::AcquireSharedOperation(const UUID& client_id,
                                              const Slot& slot) const {
    if (!slot) return std::nullopt;
    {
        // Turnstile: taking and releasing the gate lets an exclusive acquirer
        // queuing behind it through, instead of being starved by a continuous
        // stream of operations.
        const std::lock_guard<std::mutex> gate(slot->admission_gate);
    }
    SharedOperation operation(slot->operation_mutex);
    // Removal holds this lock exclusively through erasure. Revalidate the slot
    // itself, not just the record (Reset may reuse a record in a new slot).
    if (FindSlot(client_id) != slot) return std::nullopt;
    return operation;
}

std::optional<ClientSessionRegistry::ExclusiveOperation>
ClientSessionRegistry::AcquireExclusiveOperation(const UUID& client_id,
                                                 const Slot& slot,
                                                 bool try_lock) const {
    if (!slot) return std::nullopt;
    std::unique_lock<std::mutex> gate(slot->admission_gate, std::defer_lock);
    ExclusiveOperation operation(slot->operation_mutex, std::defer_lock);
    if (try_lock) {
        if (!gate.try_lock() || !operation.try_lock()) return std::nullopt;
    } else {
        // Hold the gate while queuing: no new operation enters, so this waits
        // only for the ones already in flight.
        gate.lock();
        operation.lock();
    }
    if (FindSlot(client_id) != slot) return std::nullopt;
    return operation;
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireRetainingSession(const UUID& client_id) const {
    auto slot = FindSlot(client_id);
    auto operation = AcquireSharedOperation(client_id, slot);
    if (!operation) return std::nullopt;
    if (!slot->liveness->ShouldRetainResources()) return std::nullopt;
    return SessionGuard(std::move(slot), std::move(*operation));
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireServingSession(const UUID& client_id) const {
    auto guard = TryAcquireRetainingSession(client_id);
    // The shared operation lock excludes expiry, so liveness cannot degrade
    // between this check and the caller's operation. Ping may still recover
    // it.
    if (guard && !guard->slot_->liveness->IsServing()) {
        return std::nullopt;
    }
    return guard;
}

bool ClientSessionRegistry::Remove(const UUID& client_id,
                                   const ClientSessionSharedPtr& expected) {
    std::shared_lock lifecycle_lock(lifecycle_mutex_);
    auto slot = FindSlot(client_id);
    if (!slot || slot->liveness != expected) return false;
    auto operation = AcquireExclusiveOperation(client_id, slot);
    if (!operation) return false;
    EraseSlot(client_id, slot, *operation);
    return true;
}

std::optional<ClientSessionRegistry::Registration>
ClientSessionRegistry::BeginRegistration(
    const UUID& client_id, std::optional<Clock::time_point> observed_at) {
    return Begin(client_id, RegistrationKind::Register, observed_at);
}

std::optional<ClientSessionRegistry::Registration>
ClientSessionRegistry::BeginRemount(const UUID& client_id) {
    return Begin(client_id, RegistrationKind::Remount, std::nullopt);
}

std::optional<ClientSessionRegistry::Registration> ClientSessionRegistry::Begin(
    const UUID& client_id, RegistrationKind kind,
    std::optional<Clock::time_point> observed_at) {
    Registration registration(*this, client_id, kind, observed_at);
    if (!registration.admitted_) {
        return std::nullopt;
    }
    return registration;
}

ClientSessionRegistry::Registration::Registration(
    ClientSessionRegistry& registry, const UUID& client_id,
    RegistrationKind kind, std::optional<Clock::time_point> observed_at)
    : registry_(registry),
      lifecycle_lock_(registry.lifecycle_mutex_),
      client_id_(client_id),
      kind_(kind),
      observed_at_(observed_at.value_or(Clock::now())) {
    for (;;) {
        slot_ = registry.FindSlot(client_id);
        provisional_ = !slot_;
        if (provisional_) {
            slot_ = std::make_shared<ClientSlot>(
                std::make_shared<ClientLivenessRecord>(observed_at_));
            registry.Bind(client_id, slot_->liveness);
            // Claim the private slot before publication; never acquire a
            // published slot's operation lock under the registry lock.
            operation_ = ExclusiveOperation(slot_->operation_mutex);
            std::unique_lock lock(registry.mutex_);
            if (registry.FindSlotLocked(client_id)) {
                // Lost the publication race; retry against the winner.
                operation_ = {};
                continue;
            }
            registry.AdoptSlot(client_id, slot_);
        } else {
            auto operation =
                registry.AcquireExclusiveOperation(client_id, slot_);
            // A preceding scope may have removed this slot while we waited.
            if (!operation) continue;
            operation_ = std::move(*operation);
        }
        admitted_ = slot_->liveness->ShouldRetainResources();
        break;
    }
}

ClientSessionRegistry::Registration::~Registration() {
    // A moved-from scope no longer owns the operation.
    if (operation_.owns_lock() && provisional_ && !committed_) {
        registry_.EraseSlot(client_id_, slot_, operation_);
    }
}

bool ClientSessionRegistry::Registration::NeedsRemount() const {
    CHECK(operation_.owns_lock() && admitted_);
    std::shared_lock lock(registry_.mutex_);
    return !slot_->remount_completed;
}

void ClientSessionRegistry::Registration::UpdateHostId(
    const std::string& host_id) {
    CHECK(operation_.owns_lock() && admitted_);
    registry_.UpdateHostId(client_id_, host_id);
}

void ClientSessionRegistry::Registration::Commit() {
    CHECK(operation_.owns_lock() && admitted_);
    CHECK(!committed_);
    if (kind_ == RegistrationKind::Register) {
        (void)slot_->liveness->Observe(observed_at_);
    } else {
        std::unique_lock lock(registry_.mutex_);
        if (slot_->liveness->ShouldRetainResources() &&
            !std::exchange(slot_->remount_completed, true)) {
            MasterMetricManager::instance().inc_active_clients();
        }
    }
    committed_ = true;
}

ClientSessionRegistry::RestoreBatch ClientSessionRegistry::BeginRestore() {
    return RestoreBatch(*this);
}

ClientSessionRegistry::RestoreBatch::RestoreBatch(
    ClientSessionRegistry& registry)
    : registry_(registry), lifecycle_lock_(registry.lifecycle_mutex_) {}

ClientSessionRegistry::Record ClientSessionRegistry::RestoreBatch::FindOrCreate(
    const UUID& client_id) {
    CHECK(lifecycle_lock_.owns_lock() && !committed_);
    if (const auto slot = registry_.FindSlot(client_id)) {
        return slot->liveness;
    }
    if (const auto it = pending_.find(client_id); it != pending_.end()) {
        return it->second;
    }
    auto record = std::make_shared<ClientLivenessRecord>(Clock::now());
    pending_.emplace(client_id, record);
    return record;
}

void ClientSessionRegistry::RestoreBatch::Commit() {
    CHECK(lifecycle_lock_.owns_lock() && !committed_);
    registry_.AdoptRecords(pending_);
    committed_ = true;
}

ClientStatus ClientSessionRegistry::Ping(const UUID& client_id) {
    const auto slot = FindSlot(client_id);
    if (!slot || slot->liveness->Observe(Clock::now()) ==
                     ClientLivenessObservation::REJECTED_OFFLINE) {
        return ClientStatus::NEED_REMOUNT;
    }
    // Read readiness with registry identity, never with the long operation
    // lock. Observe has already released the liveness transition lock.
    std::shared_lock lock(mutex_);
    return FindSlotLocked(client_id) == slot && slot->remount_completed
               ? ClientStatus::OK
               : ClientStatus::NEED_REMOUNT;
}

bool ClientSessionRegistry::HasRetiredSession() const {
    std::shared_lock lock(mutex_);
    return std::any_of(slots_.begin(), slots_.end(), [](const auto& entry) {
        return !entry.second->liveness->ShouldRetainResources();
    });
}

ClientSessionRegistry::Slots ClientSessionRegistry::SnapshotSlots() const {
    std::shared_lock lock(mutex_);
    return slots_;
}

void ClientSessionRegistry::AdoptSlot(const UUID& client_id, const Slot& slot) {
    CHECK(slot && slot->liveness);
    CHECK(slot->liveness->IsServing());
    CHECK(!slots_.contains(client_id));
    slots_.emplace(client_id, slot);
    MasterMetricManager::instance().client_liveness_record_created();
}

void ClientSessionRegistry::ClearRemountState(const Slot& slot) {
    if (std::exchange(slot->remount_completed, false)) {
        MasterMetricManager::instance().dec_active_clients();
    }
}

void ClientSessionRegistry::AdoptRecords(const Records& records) {
    Slots pending;
    for (const auto& [client_id, record] : records) {
        pending.emplace(client_id, std::make_shared<ClientSlot>(record));
        Bind(client_id, record);
    }
    std::unique_lock lock(mutex_);
    for (const auto& [client_id, slot] : pending) {
        AdoptSlot(client_id, slot);
    }
}

void ClientSessionRegistry::EraseSlot(const UUID& client_id, const Slot& slot,
                                      const ExclusiveOperation& operation) {
    CHECK(operation.owns_lock() && operation.mutex() == &slot->operation_mutex);
    const auto last_observed_state = slot->liveness->StopObserving();
    MasterMetricManager::instance().on_client_liveness_record_removed(
        last_observed_state);
    // Transition is unlocked; operation still excludes other removers and
    // waiters cannot validate this slot until erasure completes.
    std::unique_lock lock(mutex_);
    CHECK(FindSlotLocked(client_id) == slot);
    ClearRemountState(slot);
    slots_.erase(client_id);
    EraseHostId(client_id);
}

void ClientSessionRegistry::ClearSlots() {
    const auto slots = SnapshotSlots();
    for (const auto& [client_id, slot] : slots) {
        const std::lock_guard<std::mutex> gate(slot->admission_gate);
        ExclusiveOperation operation(slot->operation_mutex);
        EraseSlot(client_id, slot, operation);
    }
    std::unique_lock host_lock(host_mutex_);
    host_ids_.clear();
}

void ClientSessionRegistry::Reset(Records records) {
    std::unique_lock lifecycle_lock(lifecycle_mutex_);
    ClearSlots();
    AdoptRecords(records);
}

void ClientSessionRegistry::OnSessionRetired(
    const UUID& client_id, const ClientSessionSharedPtr& session) {
    std::unique_lock lock(mutex_);
    const auto slot = FindSlotLocked(client_id);
    if (slot && slot->liveness == session) {
        ClearRemountState(slot);
        EraseHostId(client_id);
    }
}

void ClientSessionRegistry::ExpiryThreadFunc() {
    std::unique_lock lock(expiry_mutex_);
    while (!expiry_stopping_) {
        lock.unlock();
        ExpireSessions(Clock::now());
        lock.lock();
        expiry_cv_.wait_until(lock, Clock::now() + kExpiryScanInterval,
                              [this] { return expiry_stopping_; });
    }
}

void ClientSessionRegistry::ExpireSessions(Clock::time_point now) {
    // Restore/reset freeze expiry, but not heartbeats. Do not wait here:
    // event delivery must remain independent of restore work.
    std::shared_lock lifecycle_lock(lifecycle_mutex_, std::try_to_lock);
    if (!lifecycle_lock.owns_lock()) {
        return;
    }
    const auto slots = SnapshotSlots();
    int waits_left = kMaxWaitingExpiryAcquisitionsPerPass;
    for (const auto& [client_id, slot] : slots) {
        // A continuously admitted session would otherwise postpone its own
        // expiry forever. That matters most when the admissions come from
        // other clients holding it as a replication source, so after a few
        // skipped passes queue behind the operations in flight instead.
        const auto busy_passes =
            slot->busy_expiry_passes.load(std::memory_order_relaxed);
        const bool wait =
            waits_left > 0 && busy_passes >= kBusyExpiryPassesBeforeWaiting;
        auto operation = AcquireExclusiveOperation(client_id, slot,
                                                   /*try_lock=*/!wait);
        if (wait) --waits_left;
        if (!operation) {
            slot->busy_expiry_passes.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        slot->busy_expiry_passes.store(0, std::memory_order_relaxed);
        (void)slot->liveness->Evaluate(now, active_ttl_, suspicion_ttl_);
    }
}

}  // namespace mooncake
