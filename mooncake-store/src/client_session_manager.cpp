#include "client_session_manager.h"

#include <glog/logging.h>

#include "client_offboarding.h"
#include "master_metric_manager.h"

namespace mooncake {

ClientSessionManager::ClientSessionManager(
    Clock::duration active_ttl, Clock::duration suspicion_ttl,
    std::function<void()> reserve_retirement)
    : active_ttl_(active_ttl),
      suspicion_ttl_(suspicion_ttl),
      reserve_retirement_(std::move(reserve_retirement)) {}

ClientSessionManager::ClientSessionManager(Clock::duration active_ttl,
                                           Clock::duration suspicion_ttl,
                                           MasterService& service)
    : ClientSessionManager(active_ttl, suspicion_ttl) {
    offboarding_worker_.reset(new ClientOffboardingWorker(&service));
}

bool ClientSessionManager::AddSessionListener(Listener listener) {
    std::lock_guard lock(notifications_->mutex);
    if (!listener || notifications_->listeners_frozen) {
        return false;
    }
    listeners_.push_back(std::move(listener));
    return true;
}

void ClientSessionManager::ScheduleOffboarding(ClientOffboardingJob job) {
    CHECK(offboarding_worker_);
    offboarding_worker_->ScheduleReserved(std::move(job));
}

ClientSessionManager::~ClientSessionManager() {
    Stop();
    std::unique_lock lifecycle_lock(lifecycle_mutex_);
    ClearSlots();
}

void ClientSessionManager::Bind(const UUID& client_id, const Record& record) {
    std::weak_ptr<Notifications> notifications = notifications_;
    std::weak_ptr<ClientLivenessRecord> session = record;
    record->SetTransitionObserver(
        [notifications, session, client_id](ClientLivenessState previous,
                                            ClientLivenessState current) {
            auto queue = notifications.lock();
            auto record = session.lock();
            if (!queue || !record) {
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
            {
                std::lock_guard lock(queue->mutex);
                if (!queue->accepting) {
                    return;
                }
                queue->events.push_back(
                    {client_id, std::move(record), previous, current});
            }
            queue->cv.notify_one();
        });
}

void ClientSessionManager::UpdateHostId(const UUID& client_id,
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

std::string ClientSessionManager::GetHostId(const UUID& client_id) const {
    std::shared_lock lock(host_mutex_);
    const auto it = host_ids_.find(client_id);
    return it == host_ids_.end() ? std::string() : it->second;
}

void ClientSessionManager::EraseHostId(const UUID& client_id) {
    std::unique_lock lock(host_mutex_);
    host_ids_.erase(client_id);
}

ClientSessionManager::Record ClientSessionManager::Find(
    const UUID& client_id) const {
    const auto slot = FindSlot(client_id);
    return slot ? slot->liveness : nullptr;
}

ClientSessionManager::Slot ClientSessionManager::FindSlot(
    const UUID& client_id) const {
    std::shared_lock lock(mutex_);
    return FindSlotLocked(client_id);
}

ClientSessionManager::Slot ClientSessionManager::FindSlotLocked(
    const UUID& client_id) const {
    const auto it = slots_.find(client_id);
    return it == slots_.end() ? nullptr : it->second;
}

std::optional<std::unique_lock<std::mutex>>
ClientSessionManager::AcquireCurrentOperation(const UUID& client_id,
                                              const Slot& slot,
                                              bool try_lock) const {
    if (!slot) return std::nullopt;
    std::unique_lock operation(slot->operation_mutex, std::defer_lock);
    if (try_lock) {
        if (!operation.try_lock()) return std::nullopt;
    } else {
        operation.lock();
    }
    // Removal retains this lock through erasure. Revalidate the slot itself,
    // not just the record (Reset may reuse a record in a new slot).
    if (FindSlot(client_id) != slot) return std::nullopt;
    return operation;
}

std::optional<ClientSessionManager::SessionGuard>
ClientSessionManager::TryAcquireRetainingSession(const UUID& client_id) const {
    auto slot = FindSlot(client_id);
    auto operation = AcquireCurrentOperation(client_id, slot);
    if (!operation) return std::nullopt;
    if (!slot->liveness->ShouldRetainResources()) return std::nullopt;
    return SessionGuard(std::move(slot), std::move(*operation));
}

std::optional<ClientSessionManager::SessionGuard>
ClientSessionManager::TryAcquireServingSession(const UUID& client_id) const {
    auto guard = TryAcquireRetainingSession(client_id);
    // The operation lock excludes Poll, so liveness cannot degrade between
    // this check and the caller's operation. Ping may still recover it.
    if (guard && !guard->slot_->liveness->IsServing()) {
        return std::nullopt;
    }
    return guard;
}

bool ClientSessionManager::Remove(const UUID& client_id,
                                  const ClientSessionSharedPtr& expected) {
    std::shared_lock lifecycle_lock(lifecycle_mutex_);
    auto slot = FindSlot(client_id);
    if (!slot || slot->liveness != expected) return false;
    auto operation = AcquireCurrentOperation(client_id, slot);
    if (!operation) return false;
    EraseSlot(client_id, slot, *operation);
    return true;
}

std::optional<ClientSessionManager::Registration>
ClientSessionManager::BeginRegistration(
    const UUID& client_id, std::optional<Clock::time_point> observed_at) {
    Registration registration(*this, client_id, RegistrationKind::Register,
                              observed_at);
    if (!registration.admitted_) {
        return std::nullopt;
    }
    return registration;
}

std::optional<ClientSessionManager::Registration>
ClientSessionManager::BeginRemount(const UUID& client_id) {
    Registration registration(*this, client_id, RegistrationKind::Remount);
    if (!registration.admitted_) {
        return std::nullopt;
    }
    return registration;
}

ClientSessionManager::Registration::Registration(
    ClientSessionManager& manager, const UUID& client_id, RegistrationKind kind,
    std::optional<Clock::time_point> observed_at)
    : manager_(&manager),
      lifecycle_lock_(manager.lifecycle_mutex_),
      client_id_(client_id),
      kind_(kind),
      observed_at_(observed_at.value_or(Clock::now())) {
    for (;;) {
        slot_ = manager.FindSlot(client_id);
        provisional_ = !slot_;
        if (provisional_) {
            slot_ = std::make_shared<ClientSlot>(
                std::make_shared<ClientLivenessRecord>(observed_at_));
            manager.Bind(client_id, slot_->liveness);
            // Claim the private slot before publication; never acquire a
            // published slot's operation lock under the registry lock.
            operation_ = std::unique_lock(slot_->operation_mutex);
            bool published = false;
            {
                std::unique_lock lock(manager.mutex_);
                if (!manager.FindSlotLocked(client_id)) {
                    manager.AdoptSlot(client_id, slot_);
                    published = true;
                }
            }
            if (!published) {
                operation_ = {};
                continue;
            }
        } else {
            auto operation = manager.AcquireCurrentOperation(client_id, slot_);
            // A preceding scope may have removed this slot while we waited.
            if (!operation) continue;
            operation_ = std::move(*operation);
        }
        admitted_ = slot_->liveness->ShouldRetainResources();
        break;
    }
}

ClientSessionManager::Registration::Registration(Registration&& other) noexcept
    : manager_(std::exchange(other.manager_, nullptr)),
      lifecycle_lock_(std::move(other.lifecycle_lock_)),
      client_id_(other.client_id_),
      kind_(other.kind_),
      observed_at_(other.observed_at_),
      slot_(std::move(other.slot_)),
      operation_(std::move(other.operation_)),
      admitted_(other.admitted_),
      provisional_(other.provisional_),
      committed_(other.committed_) {}

ClientSessionManager::Registration::~Registration() {
    if (manager_ && provisional_ && !committed_) {
        manager_->EraseSlot(client_id_, slot_, operation_);
    }
}

bool ClientSessionManager::Registration::NeedsRemount() const {
    CHECK(manager_ && admitted_);
    std::shared_lock lock(manager_->mutex_);
    return !slot_->remount_completed;
}

void ClientSessionManager::Registration::UpdateHostId(
    const std::string& host_id) {
    CHECK(manager_ && admitted_);
    manager_->UpdateHostId(client_id_, host_id);
}

void ClientSessionManager::Registration::Commit() {
    CHECK(manager_ && admitted_);
    CHECK(!committed_);
    if (kind_ == RegistrationKind::Register) {
        (void)slot_->liveness->Observe(observed_at_);
    } else {
        std::unique_lock lock(manager_->mutex_);
        if (slot_->liveness->ShouldRetainResources() &&
            !std::exchange(slot_->remount_completed, true)) {
            MasterMetricManager::instance().inc_active_clients();
        }
    }
    committed_ = true;
}

ClientSessionManager::RestoreBatch ClientSessionManager::BeginRestore() {
    return RestoreBatch(*this);
}

ClientSessionManager::RestoreBatch::RestoreBatch(ClientSessionManager& manager)
    : manager_(&manager), lifecycle_lock_(manager.lifecycle_mutex_) {}

ClientSessionManager::RestoreBatch::RestoreBatch(RestoreBatch&& other) noexcept
    : manager_(std::exchange(other.manager_, nullptr)),
      lifecycle_lock_(std::move(other.lifecycle_lock_)),
      pending_(std::move(other.pending_)),
      committed_(other.committed_) {}

ClientSessionManager::Record ClientSessionManager::RestoreBatch::FindOrCreate(
    const UUID& client_id) {
    CHECK(manager_);
    CHECK(!committed_);
    if (auto existing = manager_->Find(client_id)) {
        return existing;
    }
    if (const auto it = pending_.find(client_id); it != pending_.end()) {
        return it->second;
    }
    auto record = std::make_shared<ClientLivenessRecord>(Clock::now());
    pending_.emplace(client_id, record);
    return record;
}

void ClientSessionManager::RestoreBatch::Commit() {
    CHECK(manager_);
    CHECK(!committed_);
    manager_->AdoptRecords(pending_);
    committed_ = true;
}

ClientStatus ClientSessionManager::Ping(const UUID& client_id) {
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

ClientSessionManager::Records ClientSessionManager::SnapshotRecords() const {
    std::shared_lock lock(mutex_);
    Records records;
    records.reserve(slots_.size());
    for (const auto& [client_id, slot] : slots_) {
        records.emplace(client_id, slot->liveness);
    }
    return records;
}

ClientSessionManager::Slots ClientSessionManager::SnapshotSlots() const {
    std::shared_lock lock(mutex_);
    return slots_;
}

void ClientSessionManager::AdoptSlot(const UUID& client_id, const Slot& slot) {
    CHECK(slot && slot->liveness);
    CHECK(slot->liveness->IsServing());
    CHECK(!slots_.contains(client_id));
    slots_.emplace(client_id, slot);
    MasterMetricManager::instance().client_liveness_record_created();
}

void ClientSessionManager::ClearRemountState(const Slot& slot) {
    if (std::exchange(slot->remount_completed, false)) {
        MasterMetricManager::instance().dec_active_clients();
    }
}

void ClientSessionManager::AdoptRecords(const Records& records) {
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

void ClientSessionManager::EraseSlot(
    const UUID& client_id, const Slot& slot,
    const std::unique_lock<std::mutex>& operation) {
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

void ClientSessionManager::ClearSlots() {
    CHECK(!thread_.joinable());
    const auto slots = SnapshotSlots();
    for (const auto& [client_id, slot] : slots) {
        std::unique_lock operation(slot->operation_mutex);
        EraseSlot(client_id, slot, operation);
    }
    std::unique_lock host_lock(host_mutex_);
    host_ids_.clear();
}

void ClientSessionManager::Reset(Records records) {
    std::unique_lock lifecycle_lock(lifecycle_mutex_);
    ClearSlots();
    AdoptRecords(records);
}

void ClientSessionManager::Poll(Clock::time_point now) {
    {
        std::lock_guard lock(notifications_->mutex);
        notifications_->listeners_frozen = true;
    }
    {
        // Restore/reset freeze expiry, but not heartbeats. Do not wait here:
        // notification delivery must remain independent of restore work.
        std::shared_lock lifecycle_lock(lifecycle_mutex_, std::try_to_lock);
        if (lifecycle_lock.owns_lock()) {
            const auto slots = SnapshotSlots();
            for (const auto& [client_id, slot] : slots) {
                auto operation = AcquireCurrentOperation(client_id, slot,
                                                         /*try_lock=*/true);
                if (!operation) continue;
                (void)slot->liveness->EvaluateAndRetire(
                    now, active_ttl_, suspicion_ttl_,
                    [this] {
                        if (offboarding_worker_) {
                            offboarding_worker_->ReserveJob();
                        }
                        if (reserve_retirement_) {
                            reserve_retirement_();
                        }
                    },
                    [] {});
            }
        }
    }
    Dispatch();
}

void ClientSessionManager::Dispatch() {
    for (;;) {
        std::deque<ClientSessionEvent> events;
        {
            std::lock_guard lock(notifications_->mutex);
            notifications_->listeners_frozen = true;
            events.swap(notifications_->events);
        }
        if (events.empty()) {
            return;
        }
        for (const auto& event : events) {
            if (event.current == ClientLivenessState::OFFLINE) {
                std::unique_lock lock(mutex_);
                const auto slot = FindSlotLocked(event.client_id);
                if (slot && slot->liveness == event.session) {
                    ClearRemountState(slot);
                    EraseHostId(event.client_id);
                }
            }
            LOG(INFO) << "client_id=" << event.client_id
                      << ", action=client_session_transition, previous="
                      << toString(event.previous)
                      << ", current=" << toString(event.current);
            for (const auto& listener : listeners_) {
                listener(event);
            }
        }
    }
}

void ClientSessionManager::Start() {
    CHECK(!thread_.joinable());
    {
        std::lock_guard lock(notifications_->mutex);
        notifications_->listeners_frozen = true;
    }
    if (offboarding_worker_) {
        offboarding_worker_->Start();
    }
    {
        std::lock_guard lock(notifications_->mutex);
        notifications_->stopping = false;
        notifications_->accepting = true;
    }
    try {
        thread_ = std::thread(&ClientSessionManager::ThreadFunc, this);
    } catch (...) {
        Stop();
        throw;
    }
}

bool ClientSessionManager::HasPendingOffboarding() const {
    return offboarding_worker_ && offboarding_worker_->HasPending();
}

void ClientSessionManager::Stop() {
    Quiesce();
    if (offboarding_worker_) {
        offboarding_worker_->Stop();
    }
}

void ClientSessionManager::Quiesce() {
    {
        std::lock_guard lock(notifications_->mutex);
        notifications_->stopping = true;
    }
    notifications_->cv.notify_one();
    if (thread_.joinable()) {
        thread_.join();
    } else {
        Dispatch();
    }
    // Let an in-flight scan publish and deliver OFFLINE after reserving its
    // retirement job, even when Stop races with that scan.
    std::lock_guard lock(notifications_->mutex);
    notifications_->accepting = false;
}

void ClientSessionManager::ThreadFunc() {
    auto next_scan = Clock::now();
    for (;;) {
        {
            std::unique_lock lock(notifications_->mutex);
            notifications_->cv.wait_until(lock, next_scan, [this] {
                return notifications_->stopping ||
                       !notifications_->events.empty();
            });
            if (notifications_->stopping) {
                lock.unlock();
                Dispatch();
                return;
            }
        }
        const auto now = Clock::now();
        if (now >= next_scan) {
            Poll(now);
            next_scan = Clock::now() + std::chrono::seconds(1);
        } else {
            Dispatch();
        }
    }
}

}  // namespace mooncake
