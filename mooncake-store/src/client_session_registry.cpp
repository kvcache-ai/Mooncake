#include "client_session_registry.h"

#include <glog/logging.h>
#include "master_metric_manager.h"

namespace mooncake {

ClientSessionRegistry::Record ClientSessionRegistry::Find(
    const UUID& id) const {
    std::shared_lock lock(mutex_);
    const auto it = records_.find(id);
    return it == records_.end() ? nullptr : it->second;
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireRetainingSession(const UUID& id) const {
    auto record = Find(id);
    if (!record) return std::nullopt;
    auto guard = record->TryAcquireRetainingGuard();
    if (!guard) return std::nullopt;
    return SessionGuard(std::move(record), std::move(*guard));
}

std::optional<ClientSessionRegistry::SessionGuard>
ClientSessionRegistry::TryAcquireServingSession(const UUID& id) const {
    auto guard = TryAcquireRetainingSession(id);
    if (guard && !guard->Session()->IsServing()) return std::nullopt;
    return guard;
}

std::optional<ClientSessionRegistry::Registration>
ClientSessionRegistry::BeginRegistration(
    const UUID& id, std::optional<Clock::time_point> observed_at) {
    Registration registration(*this, id, RegistrationKind::Register,
                              observed_at.value_or(Clock::now()));
    if (!registration.guard_) return std::nullopt;
    return registration;
}

std::optional<ClientSessionRegistry::Registration>
ClientSessionRegistry::BeginRemount(const UUID& id) {
    Registration registration(*this, id, RegistrationKind::Remount,
                              Clock::now());
    if (!registration.guard_) return std::nullopt;
    return registration;
}

ClientSessionRegistry::Registration::Registration(
    ClientSessionRegistry& registry, const UUID& id, RegistrationKind kind,
    Clock::time_point observed_at)
    : registry_(registry),
      lock_(registry.mutex_),
      id_(id),
      kind_(kind),
      observed_at_(observed_at) {
    auto [it, inserted] = registry_.records_.try_emplace(
        id, std::make_shared<ClientLivenessRecord>(id, observed_at));
    record_ = it->second;
    provisional_ = inserted;
    if (inserted)
        MasterMetricManager::instance().client_liveness_record_created();
    guard_ = record_->TryAcquireRetainingGuard();
}

ClientSessionRegistry::Registration::~Registration() {
    if (lock_.owns_lock() && provisional_ && !committed_) {
        registry_.EraseLocked(id_, record_);
    }
}

bool ClientSessionRegistry::Registration::NeedsRemount() const {
    CHECK(lock_.owns_lock() && guard_);
    return !registry_.ready_.contains(id_);
}

void ClientSessionRegistry::Registration::UpdateHostId(
    const std::string& host_id) {
    CHECK(lock_.owns_lock() && guard_);
    if (!host_id.empty()) registry_.host_ids_[id_] = host_id;
}

void ClientSessionRegistry::Registration::Commit() {
    CHECK(lock_.owns_lock() && guard_ && !committed_);
    if (kind_ == RegistrationKind::Register) {
        // The old transition guard is held throughout resource installation.
        if (record_->CommitObservationLocked(observed_at_, record_->state()) ==
            ClientLivenessObservation::RECOVERED_ACTIVE) {
            MasterMetricManager::instance().client_liveness_recovered();
        }
    } else if (registry_.ready_.insert(id_).second) {
        MasterMetricManager::instance().inc_active_clients();
    }
    committed_ = true;
}

ClientSessionRegistry::RestoreBatch ClientSessionRegistry::BeginRestore() {
    return RestoreBatch(*this);
}

ClientSessionSharedPtr ClientSessionRegistry::RestoreBatch::FindOrCreate(
    const UUID& id) {
    CHECK(lock_.owns_lock() && !committed_);
    if (auto it = registry_.records_.find(id); it != registry_.records_.end())
        return it->second;
    auto [it, inserted] = pending_.try_emplace(
        id, std::make_shared<ClientLivenessRecord>(id, Clock::now()));
    (void)inserted;
    return it->second;
}

void ClientSessionRegistry::RestoreBatch::Commit() {
    CHECK(lock_.owns_lock() && !committed_);
    for (const auto& [id, record] : pending_) {
        const bool inserted = registry_.records_.emplace(id, record).second;
        CHECK(inserted);
        MasterMetricManager::instance().client_liveness_record_created();
    }
    committed_ = true;
}

void ClientSessionRegistry::EraseLocked(const UUID& id, const Record& record) {
    MasterMetricManager::instance().on_client_liveness_record_removed(
        record->state());
    if (ready_.erase(id)) MasterMetricManager::instance().dec_active_clients();
    host_ids_.erase(id);
    records_.erase(id);
}

bool ClientSessionRegistry::Remove(const UUID& id,
                                   const ClientSessionSharedPtr& expected) {
    std::unique_lock lock(mutex_);
    const auto it = records_.find(id);
    if (it == records_.end() || it->second != expected) return false;
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
    if (host_id.empty()) return;
    const auto expected = Find(id);
    if (!expected) return;
    std::unique_lock lock(mutex_);
    const auto it = records_.find(id);
    if (it != records_.end() && it->second == expected) host_ids_[id] = host_id;
}

std::string ClientSessionRegistry::GetHostId(const UUID& id) const {
    std::shared_lock lock(mutex_);
    const auto it = host_ids_.find(id);
    return it == host_ids_.end() ? std::string() : it->second;
}

ClientStatus ClientSessionRegistry::Ping(const UUID& id) {
    std::shared_lock lock(mutex_);
    const auto it = records_.find(id);
    if (it == records_.end()) return ClientStatus::NEED_REMOUNT;
    const auto observation = it->second->Observe(Clock::now());
    if (observation == ClientLivenessObservation::REJECTED_OFFLINE)
        return ClientStatus::NEED_REMOUNT;
    if (observation == ClientLivenessObservation::RECOVERED_ACTIVE) {
        MasterMetricManager::instance().client_liveness_recovered();
    }
    return ready_.contains(id) ? ClientStatus::OK : ClientStatus::NEED_REMOUNT;
}
}  // namespace mooncake
