#include "client_session_registry.h"

#include <glog/logging.h>

#include <utility>
#include <vector>

#include "master_metric_manager.h"

namespace mooncake {

ClientSessionRegistry::Record ClientSessionRegistry::Find(
    const UUID& id) const {
    std::shared_lock lock(mutex_);
    const auto it = records_.find(id);
    return it == records_.end() ? nullptr : it->second;
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireSession(const UUID& id,
                                         bool require_serving) const {
    std::shared_lock lock(mutex_);
    const auto it = records_.find(id);
    if (it == records_.end()) {
        return std::nullopt;
    }
    auto record = it->second;
    if (require_serving ? !record->IsServing()
                        : !record->ShouldRetainResources()) {
        return std::nullopt;
    }
    auto guard = record->TryAcquireRetainingGuard();
    if (!guard || (require_serving && !record->IsServing())) {
        return std::nullopt;
    }
    return SessionGuard(std::move(record), std::move(*guard));
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireRetainingSession(const UUID& id) const {
    return TryAcquireSession(id, /*require_serving=*/false);
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireServingSession(const UUID& id) const {
    return TryAcquireSession(id, /*require_serving=*/true);
}

std::optional<ClientSessionRegistry::Registration>
ClientSessionRegistry::BeginRegistration(
    const UUID& id, std::optional<Clock::time_point> observed_at) {
    return Begin(id, RegistrationKind::Register, observed_at);
}

std::optional<ClientSessionRegistry::Registration>
ClientSessionRegistry::BeginRemount(const UUID& id) {
    // Initialize new sessions after acquiring the registry lock; existing
    // sessions keep their liveness timestamp.
    return Begin(id, RegistrationKind::Remount, std::nullopt);
}

std::optional<ClientSessionRegistry::Registration> ClientSessionRegistry::Begin(
    const UUID& id, RegistrationKind kind,
    std::optional<Clock::time_point> observed_at) {
    std::unique_lock lock(mutex_);
    const auto it = records_.find(id);
    const bool provisional = (it == records_.end());
    Record record = provisional ? std::make_shared<ClientLivenessRecord>(
                                      id, observed_at.value_or(Clock::now()))
                                : it->second;
    auto guard = record->TryAcquireRetainingGuard();
    if (!guard) {
        return std::nullopt;
    }
    if (provisional) {
        records_.emplace(id, record);
        MasterMetricManager::instance().client_liveness_record_created();
    }
    return Registration(*this, std::move(lock), id, kind, observed_at,
                        std::move(record), std::move(*guard), provisional);
}

ClientSessionRegistry::Registration::Registration(
    ClientSessionRegistry& registry, std::unique_lock<std::shared_mutex> lock,
    const UUID& id, RegistrationKind kind,
    std::optional<Clock::time_point> observed_at, Record record,
    ClientLivenessRecord::RetainingGuard guard, bool provisional)
    : registry_(registry),
      lock_(std::move(lock)),
      id_(id),
      kind_(kind),
      observed_at_(observed_at),
      record_(std::move(record)),
      guard_(std::move(guard)),
      provisional_(provisional) {}

ClientSessionRegistry::Registration::~Registration() {
    if (lock_.owns_lock() && provisional_ && !committed_) {
        registry_.EraseLocked(id_, record_);
    }
}

bool ClientSessionRegistry::Registration::NeedsRemount() const {
    CHECK(lock_.owns_lock());
    return !registry_.ready_.contains(id_);
}

void ClientSessionRegistry::Registration::UpdateHostId(
    const std::string& host_id) {
    CHECK(lock_.owns_lock());
    if (host_id.empty()) {
        return;
    }
    if (committed_) {
        registry_.host_ids_[id_] = host_id;
    } else {
        pending_host_id_ = host_id;
    }
}

void ClientSessionRegistry::Registration::Commit() {
    CHECK(lock_.owns_lock() && !committed_);
    if (kind_ == RegistrationKind::Register) {
        // Sample after lock acquisition and resource installation so neither
        // consumes the successfully registered client's active TTL. Keep an
        // explicit timestamp only for deterministic tests.
        const auto now = observed_at_.value_or(Clock::now());
        if (record_->CommitObservationLocked(now, record_->state()) ==
            ClientLivenessObservation::RECOVERED_ACTIVE) {
            MasterMetricManager::instance().client_liveness_recovered();
            LOG(INFO) << "client_id=" << id_
                      << ", action=client_liveness_recovered, "
                         "signal=registration";
        }
    } else if (registry_.ready_.insert(id_).second) {
        MasterMetricManager::instance().inc_active_clients();
    }
    if (pending_host_id_) {
        registry_.host_ids_[id_] = std::move(*pending_host_id_);
        pending_host_id_.reset();
    }
    committed_ = true;
}

ClientSessionRegistry::RestoreBatch ClientSessionRegistry::BeginRestore() {
    return RestoreBatch(*this);
}

ClientSessionSharedPtr ClientSessionRegistry::RestoreBatch::FindOrCreate(
    const UUID& id) {
    CHECK(lock_.owns_lock() && !committed_);
    if (const auto it = registry_.records_.find(id);
        it != registry_.records_.end()) {
        return it->second;
    }
    if (const auto it = pending_.find(id); it != pending_.end()) {
        return it->second;
    }
    auto record = std::make_shared<ClientLivenessRecord>(id, Clock::now());
    pending_.emplace(id, record);
    return record;
}

void ClientSessionRegistry::RestoreBatch::Commit() {
    CHECK(lock_.owns_lock() && !committed_);
    for (auto& [id, record] : pending_) {
        const bool inserted =
            registry_.records_.emplace(id, std::move(record)).second;
        CHECK(inserted);
        MasterMetricManager::instance().client_liveness_record_created();
    }
    pending_.clear();
    committed_ = true;
}

void ClientSessionRegistry::ClearSessionMetadataLocked(const UUID& id) {
    if (ready_.erase(id) != 0) {
        MasterMetricManager::instance().dec_active_clients();
    }
    host_ids_.erase(id);
}

void ClientSessionRegistry::EraseLocked(const UUID& id, const Record& record) {
    MasterMetricManager::instance().on_client_liveness_record_removed(
        record->state());
    ClearSessionMetadataLocked(id);
    records_.erase(id);
}

bool ClientSessionRegistry::Remove(const UUID& id,
                                   const ClientSessionSharedPtr& expected) {
    std::unique_lock lock(mutex_);
    const auto it = records_.find(id);
    if (it == records_.end() || it->second != expected) {
        return false;
    }
    const auto record = it->second;
    std::lock_guard transition(record->transition_mutex_);
    EraseLocked(id, record);
    return true;
}

void ClientSessionRegistry::Reset() {
    std::unique_lock lock(mutex_);
    while (!records_.empty()) {
        const auto id = records_.begin()->first;
        const auto record = records_.begin()->second;
        EraseLocked(id, record);
    }
    host_ids_.clear();
    ready_.clear();
}

void ClientSessionRegistry::UpdateHostId(const UUID& id,
                                         const std::string& host_id) {
    if (host_id.empty()) {
        return;
    }
    Record expected;
    {
        std::shared_lock lock(mutex_);
        const auto record_it = records_.find(id);
        if (record_it == records_.end() ||
            !record_it->second->ShouldRetainResources()) {
            return;
        }
        const auto host_it = host_ids_.find(id);
        if (host_it != host_ids_.end() && host_it->second == host_id) {
            return;
        }
        expected = record_it->second;
    }
    std::unique_lock lock(mutex_);
    const auto it = records_.find(id);
    if (it != records_.end() && it->second == expected &&
        it->second->ShouldRetainResources()) {
        host_ids_[id] = host_id;
    }
}

std::string ClientSessionRegistry::GetHostId(const UUID& id) const {
    std::shared_lock lock(mutex_);
    const auto it = host_ids_.find(id);
    return it == host_ids_.end() ? std::string() : it->second;
}

ClientStatus ClientSessionRegistry::Ping(const UUID& id) {
    std::shared_lock lock(mutex_);
    const auto it = records_.find(id);
    if (it == records_.end()) {
        return ClientStatus::NEED_REMOUNT;
    }
    const auto observation = it->second->Observe(Clock::now());
    if (observation == ClientLivenessObservation::REJECTED_OFFLINE) {
        return ClientStatus::NEED_REMOUNT;
    }
    if (observation == ClientLivenessObservation::RECOVERED_ACTIVE) {
        MasterMetricManager::instance().client_liveness_recovered();
        LOG(INFO) << "client_id=" << id
                  << ", action=client_liveness_recovered, signal=ping";
    }
    return ready_.contains(id) ? ClientStatus::OK : ClientStatus::NEED_REMOUNT;
}

void ClientSessionRegistry::ExpireSessions(
    Clock::time_point now, Clock::duration active_ttl,
    Clock::duration suspicion_ttl,
    const std::function<void()>& reserve_retirement,
    const std::function<void(const UUID&, const ClientSessionSharedPtr&)>&
        retire) {
    std::vector<std::pair<UUID, Record>> clients;
    {
        std::shared_lock lock(mutex_);
        clients.reserve(records_.size());
        for (const auto& entry : records_) {
            clients.push_back(entry);
        }
    }

    for (const auto& [id, record] : clients) {
        const auto transition = record->EvaluateAndRetire(
            now, active_ttl, suspicion_ttl,
            [&] {
                if (reserve_retirement) {
                    reserve_retirement();
                }
            },
            [&] {
                {
                    std::unique_lock lock(mutex_);
                    const auto current = records_.find(id);
                    CHECK(current != records_.end());
                    CHECK(current->second == record);
                    ClearSessionMetadataLocked(id);
                }
                MasterMetricManager::instance()
                    .client_liveness_became_offline();
                if (retire) {
                    retire(id, record);
                }
            });

        if (transition == ClientLivenessTransition::BECAME_SUSPECTED) {
            MasterMetricManager::instance().client_liveness_became_suspected();
            LOG(INFO) << "client_id=" << id
                      << ", action=client_liveness_suspected";
        } else if (transition == ClientLivenessTransition::BECAME_OFFLINE) {
            LOG(INFO) << "client_id=" << id
                      << ", action=client_liveness_offline";
        }
    }
}

}  // namespace mooncake
