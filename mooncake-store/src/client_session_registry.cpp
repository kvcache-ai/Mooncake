#include "client_session_registry.h"

#include <glog/logging.h>

#include <algorithm>

#include "master_metric_manager.h"

namespace mooncake {
namespace {

// Passes a busy slot may be skipped before expiry closes its admission gate.
constexpr unsigned kBusyExpiryPassesBeforeWaiting = 3;

}  // namespace

ClientSessionRegistry::ClientSessionRegistry(
    Clock::duration active_ttl, Clock::duration suspicion_ttl,
    std::function<bool()> admits_expiry)
    : active_ttl_(active_ttl),
      suspicion_ttl_(suspicion_ttl),
      admits_expiry_(std::move(admits_expiry)) {}

ClientSessionRegistry::~ClientSessionRegistry() {
    Stop();
    Reset();
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

void ClientSessionRegistry::PublishTransition(const UUID& client_id,
                                              const Slot& slot,
                                              ClientLivenessState previous,
                                              ClientLivenessState current) {
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
    events_.Publish({client_id, slot->liveness, previous, current});
}

void ClientSessionRegistry::UpdateHostId(const UUID& client_id,
                                         const std::string& host_id) {
    if (host_id.empty()) {
        return;
    }
    Slot slot;
    {
        std::shared_lock lock(mutex_);
        slot = FindSlotLocked(client_id);
        if (!slot || !slot->liveness->ShouldRetainResources() ||
            slot->host_id == host_id) {
            return;
        }
    }
    // The session may have been replaced or retired between the two locks. A
    // hint seen by an old or offline incarnation must not land on it or its
    // successor.
    std::unique_lock lock(mutex_);
    if (FindSlotLocked(client_id) == slot) {
        SetSlotHostIdLocked(slot, host_id);
    }
}

std::string ClientSessionRegistry::GetHostId(const UUID& client_id) const {
    std::shared_lock lock(mutex_);
    const auto slot = FindSlotLocked(client_id);
    return (slot && slot->liveness->ShouldRetainResources()) ? slot->host_id
                                                             : std::string();
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
ClientSessionRegistry::ClientSlot::AcquireShared() {
    SharedOperation operation(operation_mutex);
    // Removal sets erased under ExclusiveOperation before dropping the slot.
    if (erased) return std::nullopt;
    return operation;
}

std::optional<ClientSessionRegistry::ExclusiveOperation>
ClientSessionRegistry::ClientSlot::AcquireExclusive(bool try_lock) {
    ExclusiveOperation operation(operation_mutex, std::defer_lock);
    if (try_lock) {
        if (!operation.try_lock()) return std::nullopt;
    } else {
        operation.lock();
    }
    if (erased) return std::nullopt;
    return operation;
}

std::optional<ClientSessionRegistry::ExclusiveOperation>
ClientSessionRegistry::ClientSlot::AcquireExclusiveForExpiry(
    bool drain_if_busy) {
    const bool locked = drain_if_busy ? operation_mutex.try_lock_or_drain()
                                      : operation_mutex.try_lock();
    if (!locked) return std::nullopt;
    ExclusiveOperation operation(operation_mutex, std::adopt_lock);
    if (erased) return std::nullopt;
    return operation;
}

void ClientSessionRegistry::ClientSlot::CancelExpiryDrain() {
    busy_expiry_passes.store(0, std::memory_order_relaxed);
    operation_mutex.cancel_drain();
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireSession(const UUID& client_id,
                                         bool require_serving) const {
    auto slot = FindSlot(client_id);
    // OFFLINE is terminal and never recovers; SUSPECTED still acquires the
    // shared lock so it waits for any in-flight registration recovering it.
    if (!slot || !slot->liveness->ShouldRetainResources()) {
        return std::nullopt;
    }
    auto operation = slot->AcquireShared();
    if (!operation) return std::nullopt;
    const bool admitted = require_serving
                              ? slot->liveness->IsServing()
                              : slot->liveness->ShouldRetainResources();
    if (!admitted) return std::nullopt;
    return SessionGuard(std::move(slot), std::move(*operation));
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireRetainingSession(const UUID& client_id) const {
    return TryAcquireSession(client_id, /*require_serving=*/false);
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireServingSession(const UUID& client_id) const {
    return TryAcquireSession(client_id, /*require_serving=*/true);
}

bool ClientSessionRegistry::Remove(const UUID& client_id,
                                   const ClientSessionSharedPtr& expected) {
    std::shared_lock lifecycle_lock(lifecycle_mutex_);
    auto slot = FindSlot(client_id);
    if (!slot || slot->liveness != expected) return false;
    auto operation = slot->AcquireExclusive();
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
    std::shared_lock lifecycle_lock(lifecycle_mutex_);
    for (;;) {
        auto slot = FindSlot(client_id);
        const bool provisional = !slot;
        ExclusiveOperation operation;
        if (provisional) {
            // Sample after acquiring lifecycle_lock so waiting behind restore
            // work does not consume the new session's active TTL.
            slot = std::make_shared<ClientSlot>(
                std::make_shared<ClientLivenessRecord>(
                    client_id, observed_at.value_or(Clock::now())));
            // Claim the private slot before publication; never acquire a
            // published slot's operation lock under the registry lock.
            operation = ExclusiveOperation(slot->operation_mutex);
            std::unique_lock lock(mutex_);
            if (FindSlotLocked(client_id)) {
                // Lost the publication race; retry against the winner.
                continue;
            }
            AdoptSlot(client_id, slot);
        } else {
            if (!slot->liveness->ShouldRetainResources()) {
                return std::nullopt;
            }
            auto acquired = slot->AcquireExclusive();
            // A preceding scope may have removed this slot while we waited.
            if (!acquired) continue;
            if (!slot->liveness->ShouldRetainResources()) {
                return std::nullopt;
            }
            operation = std::move(*acquired);
        }
        return Registration(*this, std::move(lifecycle_lock), client_id, kind,
                            observed_at, std::move(slot), std::move(operation),
                            provisional);
    }
}

ClientSessionRegistry::Registration::Registration(
    ClientSessionRegistry& registry,
    std::shared_lock<std::shared_mutex> lifecycle_lock, const UUID& client_id,
    RegistrationKind kind, std::optional<Clock::time_point> observed_at,
    Slot slot, ExclusiveOperation operation, bool provisional)
    : registry_(registry),
      lifecycle_lock_(std::move(lifecycle_lock)),
      client_id_(client_id),
      kind_(kind),
      observed_at_(observed_at),
      slot_(std::move(slot)),
      operation_(std::move(operation)),
      provisional_(provisional) {}

ClientSessionRegistry::Registration::~Registration() {
    // A moved-from scope no longer owns the operation.
    if (operation_.owns_lock() && provisional_ && !committed_) {
        registry_.EraseSlot(client_id_, slot_, operation_);
    }
}

bool ClientSessionRegistry::Registration::NeedsRemount() const {
    CHECK(operation_.owns_lock());
    std::shared_lock lock(registry_.mutex_);
    return !slot_->remount_completed;
}

void ClientSessionRegistry::Registration::UpdateHostId(
    const std::string& host_id) {
    CHECK(operation_.owns_lock());
    if (host_id.empty()) {
        return;
    }
    if (committed_) {
        std::unique_lock lock(registry_.mutex_);
        registry_.SetSlotHostIdLocked(slot_, host_id);
    } else {
        pending_host_id_ = host_id;
    }
}

void ClientSessionRegistry::Registration::Commit() {
    CHECK(operation_.owns_lock());
    CHECK(!committed_);
    if (kind_ == RegistrationKind::Register) {
        // Sample after lock acquisition and resource installation so neither
        // consumes the successfully registered client's active TTL. Keep an
        // explicit timestamp only for deterministic tests.
        (void)registry_.ObserveSlot(client_id_, slot_,
                                    observed_at_.value_or(Clock::now()));
    }
    if (kind_ == RegistrationKind::Remount || pending_host_id_) {
        std::unique_lock lock(registry_.mutex_);
        if (kind_ == RegistrationKind::Remount &&
            slot_->liveness->ShouldRetainResources() &&
            !std::exchange(slot_->remount_completed, true)) {
            MasterMetricManager::instance().inc_active_clients();
        }
        if (pending_host_id_) {
            registry_.SetSlotHostIdLocked(slot_, std::move(*pending_host_id_));
            pending_host_id_.reset();
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

ClientSessionSharedPtr ClientSessionRegistry::RestoreBatch::FindOrCreate(
    const UUID& client_id) {
    CHECK(lifecycle_lock_.owns_lock() && !committed_);
    if (const auto slot = registry_.FindSlot(client_id)) {
        return slot->liveness;
    }
    if (const auto it = pending_.find(client_id); it != pending_.end()) {
        return it->second;
    }
    auto record =
        std::make_shared<ClientLivenessRecord>(client_id, Clock::now());
    pending_.emplace(client_id, record);
    return record;
}

void ClientSessionRegistry::RestoreBatch::Commit() {
    CHECK(lifecycle_lock_.owns_lock() && !committed_);
    registry_.AdoptRecords(pending_);
    pending_.clear();
    committed_ = true;
}

ClientStatus ClientSessionRegistry::Ping(const UUID& client_id) {
    return Observe(client_id, Clock::now());
}

bool ClientSessionRegistry::ObserveSlot(const UUID& client_id, const Slot& slot,
                                        Clock::time_point now) {
    switch (slot->liveness->Observe(now)) {
        case ClientLivenessObservation::REJECTED_OFFLINE:
            return false;
        case ClientLivenessObservation::RECOVERED_ACTIVE:
            slot->CancelExpiryDrain();
            PublishTransition(client_id, slot, ClientLivenessState::SUSPECTED,
                              ClientLivenessState::ACTIVE);
            return true;
        case ClientLivenessObservation::REFRESHED_ACTIVE:
            slot->CancelExpiryDrain();
            return true;
    }
    return false;
}

ClientStatus ClientSessionRegistry::Observe(const UUID& client_id,
                                            Clock::time_point now) {
    // Never the long operation lock. The registry lock covers the observation
    // too, so a session is either observed before its removal reads the state
    // or not at all.
    std::shared_lock lock(mutex_);
    const auto slot = FindSlotLocked(client_id);
    if (!slot || !ObserveSlot(client_id, slot, now)) {
        return ClientStatus::NEED_REMOUNT;
    }
    return slot->remount_completed ? ClientStatus::OK
                                   : ClientStatus::NEED_REMOUNT;
}

bool ClientSessionRegistry::HasRetiredSession() const {
    std::shared_lock lock(mutex_);
    return std::any_of(slots_.begin(), slots_.end(), [](const auto& entry) {
        return !entry.second->liveness->ShouldRetainResources();
    });
}

ClientSessionRegistry::SlotSnapshot ClientSessionRegistry::SnapshotSlots()
    const {
    std::shared_lock lock(mutex_);
    SlotSnapshot snapshot;
    snapshot.reserve(slots_.size());
    for (const auto& entry : slots_) {
        snapshot.push_back(entry);
    }
    return snapshot;
}

void ClientSessionRegistry::AdoptSlot(const UUID& client_id, const Slot& slot) {
    CHECK(slot && slot->liveness);
    CHECK(slot->liveness->IsServing());
    CHECK(slot->liveness->ClientId() == client_id);
    CHECK(!slots_.contains(client_id));
    slots_.emplace(client_id, slot);
    MasterMetricManager::instance().client_liveness_record_created();
}

void ClientSessionRegistry::SetSlotHostIdLocked(const Slot& slot,
                                                std::string host_id) {
    if (!host_id.empty() && slot->liveness->ShouldRetainResources()) {
        slot->host_id = std::move(host_id);
    }
}

void ClientSessionRegistry::ClearSlotStateLocked(const Slot& slot) {
    if (std::exchange(slot->remount_completed, false)) {
        MasterMetricManager::instance().dec_active_clients();
    }
    slot->host_id.clear();
}

void ClientSessionRegistry::AdoptRecords(const Records& records) {
    std::unique_lock lock(mutex_);
    for (const auto& [client_id, record] : records) {
        AdoptSlot(client_id, std::make_shared<ClientSlot>(record));
    }
}

void ClientSessionRegistry::EraseSlot(const UUID& client_id, const Slot& slot,
                                      const ExclusiveOperation& operation) {
    CHECK(operation.owns_lock() && operation.mutex() == &slot->operation_mutex);
    slot->erased = true;
    slot->operation_mutex.close_permanently();
    // The operation excludes other removers, expiry and registration, and the
    // registry lock excludes Ping, so the state read here is the one the
    // gauges last counted. Waiters cannot enter this slot once erased is set.
    std::unique_lock lock(mutex_);
    CHECK(FindSlotLocked(client_id) == slot);
    MasterMetricManager::instance().on_client_liveness_record_removed(
        slot->liveness->state());
    ClearSlotStateLocked(slot);
    slots_.erase(client_id);
}

void ClientSessionRegistry::Reset() {
    std::unique_lock lifecycle_lock(lifecycle_mutex_);
    for (const auto& [client_id, slot] : SnapshotSlots()) {
        if (auto operation = slot->AcquireExclusive()) {
            EraseSlot(client_id, slot, *operation);
        }
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
    if (admits_expiry_ && !admits_expiry_()) {
        return;
    }
    // Restore/reset freeze expiry, but not heartbeats. Do not wait here:
    // event delivery must remain independent of restore work.
    std::shared_lock lifecycle_lock(lifecycle_mutex_, std::try_to_lock);
    if (!lifecycle_lock.owns_lock()) {
        return;
    }
    for (const auto& [client_id, slot] : SnapshotSlots()) {
        if (!slot->liveness->IsExpiredAt(now, active_ttl_, suspicion_ttl_)) {
            slot->CancelExpiryDrain();
            continue;
        }
        // A continuously admitted session would otherwise postpone its own
        // expiry forever. After a few skipped passes, close its turnstile so
        // new admissions wait and the current operations drain before the next
        // pass evaluates expiry without blocking unrelated clients.
        const bool drain =
            slot->busy_expiry_passes.load(std::memory_order_relaxed) >=
            kBusyExpiryPassesBeforeWaiting;
        auto operation = slot->AcquireExclusiveForExpiry(drain);
        if (!operation) {
            slot->busy_expiry_passes.fetch_add(1, std::memory_order_relaxed);
            if (drain && !slot->liveness->IsExpiredAt(now, active_ttl_,
                                                      suspicion_ttl_)) {
                slot->CancelExpiryDrain();
            }
            continue;
        }
        slot->busy_expiry_passes.store(0, std::memory_order_relaxed);
        switch (slot->liveness->Evaluate(now, active_ttl_, suspicion_ttl_)) {
            case ClientLivenessTransition::NONE:
                break;
            case ClientLivenessTransition::BECAME_SUSPECTED:
                PublishTransition(client_id, slot, ClientLivenessState::ACTIVE,
                                  ClientLivenessState::SUSPECTED);
                break;
            case ClientLivenessTransition::BECAME_OFFLINE: {
                // Drop this registry's own state for the terminal incarnation
                // before any resource owner hears of it. The operation is
                // still held, so the slot is the current one.
                {
                    std::unique_lock lock(mutex_);
                    ClearSlotStateLocked(slot);
                }
                PublishTransition(client_id, slot,
                                  ClientLivenessState::SUSPECTED,
                                  ClientLivenessState::OFFLINE);
                break;
            }
        }
    }
}

}  // namespace mooncake
