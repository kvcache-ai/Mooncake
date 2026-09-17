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
    std::unique_lock lock(mutex_);
    ClearRecords();
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
    std::shared_lock lock(mutex_);
    return FindRecord(client_id);
}

ClientSessionManager::Record ClientSessionManager::FindRecord(
    const UUID& client_id) const {
    const auto it = records_.find(client_id);
    return it == records_.end() ? nullptr : it->second;
}

std::optional<ClientSessionManager::SessionGuard>
ClientSessionManager::TryAcquireRetainingSession(const UUID& client_id) const {
    auto record = Find(client_id);
    auto guard = record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!guard) {
        return std::nullopt;
    }
    return SessionGuard(std::move(record), std::move(*guard));
}

std::optional<ClientSessionManager::SessionGuard>
ClientSessionManager::TryAcquireServingSession(const UUID& client_id) const {
    auto guard = TryAcquireRetainingSession(client_id);
    // The transition lock is still held, so admission cannot change between
    // this check and the caller's operation.
    if (guard && !guard->record_->IsServing()) {
        return std::nullopt;
    }
    return guard;
}

bool ClientSessionManager::Remove(const UUID& client_id,
                                  const Record& expected) {
    std::unique_lock lock(mutex_);
    return EraseRecord(client_id, expected);
}

std::optional<ClientSessionManager::Registration>
ClientSessionManager::BeginRegistration(
    const UUID& client_id, std::optional<Clock::time_point> observed_at) {
    Registration registration(*this, client_id, RegistrationKind::Register,
                              observed_at);
    if (!registration.guard_) {
        return std::nullopt;
    }
    return registration;
}

std::optional<ClientSessionManager::Registration>
ClientSessionManager::BeginRemount(const UUID& client_id) {
    Registration registration(*this, client_id, RegistrationKind::Remount);
    if (!registration.guard_) {
        return std::nullopt;
    }
    return registration;
}

ClientSessionManager::Registration::Registration(
    ClientSessionManager& manager, const UUID& client_id, RegistrationKind kind,
    std::optional<Clock::time_point> observed_at)
    : manager_(&manager),
      registry_lock_(manager.mutex_),
      client_id_(client_id),
      kind_(kind),
      observed_at_(observed_at.value_or(Clock::now())) {
    auto [record, inserted] =
        manager.GetOrCreateRecord(client_id, observed_at_);
    record_ = std::move(record);
    provisional_ = inserted;
    try {
        if (auto guard = record_->TryAcquireRetainingGuard()) {
            guard_.emplace(std::move(*guard));
        }
    } catch (...) {
        if (provisional_) {
            manager.EraseRecord(client_id_, record_);
        }
        throw;
    }
}

ClientSessionManager::Registration::Registration(Registration&& other) noexcept
    : manager_(std::exchange(other.manager_, nullptr)),
      registry_lock_(std::move(other.registry_lock_)),
      client_id_(other.client_id_),
      kind_(other.kind_),
      observed_at_(other.observed_at_),
      record_(std::move(other.record_)),
      guard_(std::move(other.guard_)),
      provisional_(other.provisional_),
      committed_(other.committed_) {}

ClientSessionManager::Registration::~Registration() {
    if (manager_ && provisional_ && !committed_) {
        manager_->EraseRecord(client_id_, record_);
    }
    // Members release the record guard before the record and registry lock.
}

bool ClientSessionManager::Registration::NeedsRemount() const {
    CHECK(manager_ && guard_.has_value());
    return !manager_->remount_completed_clients_.contains(client_id_);
}

void ClientSessionManager::Registration::UpdateHostId(
    const std::string& host_id) {
    CHECK(manager_ && guard_.has_value());
    manager_->UpdateHostId(client_id_, host_id);
}

void ClientSessionManager::Registration::Commit() {
    CHECK(manager_ && guard_.has_value());
    CHECK(!committed_);
    if (kind_ == RegistrationKind::Register) {
        (void)guard_->Observe(observed_at_);
    } else if (manager_->remount_completed_clients_.insert(client_id_).second) {
        MasterMetricManager::instance().inc_active_clients();
    }
    committed_ = true;
}

ClientSessionManager::RestoreBatch ClientSessionManager::BeginRestore() {
    return RestoreBatch(*this);
}

ClientSessionManager::RestoreBatch::RestoreBatch(ClientSessionManager& manager)
    : manager_(&manager), registry_lock_(manager.mutex_) {}

ClientSessionManager::RestoreBatch::RestoreBatch(RestoreBatch&& other) noexcept
    : manager_(std::exchange(other.manager_, nullptr)),
      registry_lock_(std::move(other.registry_lock_)),
      pending_(std::move(other.pending_)),
      committed_(other.committed_) {}

ClientSessionManager::Record ClientSessionManager::RestoreBatch::FindOrCreate(
    const UUID& client_id) {
    CHECK(manager_);
    CHECK(!committed_);
    if (auto existing = manager_->FindRecord(client_id)) {
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
    for (auto& [client_id, record] : pending_) {
        manager_->AdoptRecord(client_id, std::move(record));
    }
    committed_ = true;
}

ClientStatus ClientSessionManager::Ping(const UUID& client_id) {
    std::shared_lock lock(mutex_);
    const auto record = FindRecord(client_id);
    const bool accepted =
        record && record->Observe(Clock::now()) !=
                      ClientLivenessObservation::REJECTED_OFFLINE;
    return accepted && remount_completed_clients_.contains(client_id)
               ? ClientStatus::OK
               : ClientStatus::NEED_REMOUNT;
}

ClientSessionManager::Records ClientSessionManager::SnapshotRecords() const {
    std::shared_lock lock(mutex_);
    return records_;
}

void ClientSessionManager::ClearRemountState(const UUID& client_id) {
    if (remount_completed_clients_.erase(client_id) != 0) {
        MasterMetricManager::instance().dec_active_clients();
    }
}

std::pair<ClientSessionManager::Record, bool>
ClientSessionManager::GetOrCreateRecord(const UUID& client_id,
                                        Clock::time_point now) {
    if (auto record = FindRecord(client_id)) {
        return {std::move(record), false};
    }
    auto record = std::make_shared<ClientLivenessRecord>(now);
    AdoptRecord(client_id, record);
    return {std::move(record), true};
}

void ClientSessionManager::AdoptRecord(const UUID& client_id, Record record) {
    CHECK(record);
    CHECK(record->IsServing());
    CHECK(!records_.contains(client_id));
    Bind(client_id, record);
    records_.emplace(client_id, std::move(record));
    MasterMetricManager::instance().client_liveness_record_created();
}

bool ClientSessionManager::EraseRecord(const UUID& client_id,
                                       const Record& expected) {
    const auto it = records_.find(client_id);
    if (it == records_.end() || it->second != expected) {
        return false;
    }
    it->second->DisableTransitionObserver();
    MasterMetricManager::instance().on_client_liveness_record_removed(
        it->second->state());
    records_.erase(it);
    ClearRemountState(client_id);
    EraseHostId(client_id);
    return true;
}

void ClientSessionManager::ClearRecords() {
    CHECK(!thread_.joinable());
    for (const auto& [_, record] : records_) {
        record->DisableTransitionObserver();
        MasterMetricManager::instance().on_client_liveness_record_removed(
            record->state());
    }
    records_.clear();
    MasterMetricManager::instance().dec_active_clients(
        static_cast<int64_t>(remount_completed_clients_.size()));
    remount_completed_clients_.clear();
    std::unique_lock host_lock(host_mutex_);
    host_ids_.clear();
}

void ClientSessionManager::Reset(Records records) {
    std::unique_lock lock(mutex_);
    ClearRecords();
    for (auto& [client_id, record] : records) {
        AdoptRecord(client_id, std::move(record));
    }
}

void ClientSessionManager::Poll(Clock::time_point now) {
    {
        std::lock_guard lock(notifications_->mutex);
        notifications_->listeners_frozen = true;
    }
    const auto records = SnapshotRecords();
    for (const auto& [client_id, record] : records) {
        (void)client_id;
        (void)record->EvaluateAndRetire(
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
                CHECK(FindRecord(event.client_id) == event.session);
                ClearRemountState(event.client_id);
                EraseHostId(event.client_id);
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
