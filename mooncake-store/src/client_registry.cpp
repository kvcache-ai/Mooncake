#include "client_registry.h"
#include "client_offboarding_internal.h"

#include <condition_variable>
#include <glog/logging.h>

#include "master_metric_manager.h"

namespace mooncake {
namespace {

// Caller holds registry access so the session cannot be replaced mid-lookup.
std::string ResolveSessionHostId(ClientSession& session,
                                 const std::string& hint) {
    // An explicit request hint is still usable for placement, but must not
    // update a retired incarnation or expose its cached host.
    if (!session.ShouldRetainResources()) {
        return hint;
    }
    if (hint.empty()) {
        return session.host_id();
    }
    session.SetHostId(hint);
    return hint;
}

}  // namespace

ClientRegistry::ClientRegistry(bool account_metrics)
    : account_metrics_(account_metrics),
      offboarding_worker_(std::make_unique<ClientOffboardingWorker>(
          [this](ClientOffboardingJob& job) {
              return ProcessClientOffboardingJob(job);
          },
          account_metrics)) {}

ClientRegistry::~ClientRegistry() {
    Stop();
    Clear();
}

ClientRegistry::ReadAccess::ReadAccess(const ClientRegistry& registry)
    : registry_(registry), lock_(registry.mutex_) {}

ClientRegistry::ClientIds ClientRegistry::ReadAccess::RetainingClientIds()
    const {
    ClientIds clients;
    clients.reserve(registry_.sessions_.size());
    for (const auto& [id, session] : registry_.sessions_)
        if (session->ShouldRetainResources()) clients.insert(id);
    return clients;
}

ClientRegistry::WriteAccess::WriteAccess(
    ClientRegistry& registry, std::unique_lock<std::shared_mutex> lock)
    : registry_(registry), lock_(std::move(lock)) {}

ClientSessionPtr ClientRegistry::WriteAccess::Find(
    const UUID& client_id) const {
    return registry_.FindLocked(client_id);
}

void ClientRegistry::WriteAccess::Import(const Sessions& staged) {
    registry_.ImportLocked(staged);
}

ClientRegistry::ReadAccess ClientRegistry::AcquireReadAccess() const {
    return ReadAccess(*this);
}

ClientRegistry::WriteAccess ClientRegistry::AcquireWriteAccess() {
    return WriteAccess(*this, std::unique_lock(mutex_));
}

std::optional<ClientRegistry::WriteAccess>
ClientRegistry::TryAcquireWriteAccess() {
    std::unique_lock lock(mutex_, std::try_to_lock);
    if (!lock.owns_lock()) return std::nullopt;
    return WriteAccess(*this, std::move(lock));
}

ClientRegistry::Registration::Registration(
    ClientRegistry& registry, std::unique_lock<std::shared_mutex> registry_lock,
    ClientSessionPtr session, ClientSession::RetainingGuard guard,
    bool inserted)
    : registry_(&registry),
      registry_lock_(std::move(registry_lock)),
      session_(std::move(session)),
      guard_(std::move(guard)),
      inserted_(inserted) {}

ClientRegistry::Registration::Registration(Registration&& other) noexcept
    : registry_(std::exchange(other.registry_, nullptr)),
      registry_lock_(std::move(other.registry_lock_)),
      session_(std::move(other.session_)),
      guard_(std::move(other.guard_)),
      inserted_(other.inserted_),
      committed_(other.committed_) {}

ClientRegistry::Registration::~Registration() {
    if (registry_ && inserted_ && !committed_)
        registry_->RemoveLocked(session_, false);
}

bool ClientRegistry::Registration::IsRemounted() const {
    return registry_->remounted_.contains(session_->client_id());
}

ClientLivenessObservation ClientRegistry::Registration::Commit(
    bool observe, Clock::time_point now) {
    if (committed_) {
        return ClientLivenessObservation::OBSERVATION_WITHHELD;
    }
    committed_ = true;
    // The session now owns the host; the rollback copy is no longer needed.
    registry_->writer_host_hints_.erase(session_->client_id());
    const auto observation =
        observe ? guard_.Observe(now)
                : ClientLivenessObservation::OBSERVATION_WITHHELD;
    registry_->AccountObservation(observation);
    return observation;
}

void ClientRegistry::Registration::CommitRemount() {
    if (registry_->remounted_.insert(session_->client_id()).second &&
        registry_->account_metrics_)
        MasterMetricManager::instance().inc_active_clients();
    Commit(false);
}

tl::expected<ClientRegistry::Registration, ErrorCode> ClientRegistry::Register(
    const UUID& client_id, Clock::time_point now) {
    std::unique_lock lock(mutex_);
    const bool inserted = !sessions_.contains(client_id);
    auto session = GetOrCreateLocked(client_id, now);
    auto guard = session->TryAcquireRetainingGuard();
    if (!guard) return tl::unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    return Registration(*this, std::move(lock), std::move(session),
                        std::move(*guard), inserted);
}

ClientSessionPtr ClientRegistry::GetOrCreate(const UUID& client_id,
                                             Clock::time_point now) {
    std::unique_lock lock(mutex_);
    auto session = GetOrCreateLocked(client_id, now);
    // Unlike Register(), immediate creation has no rollback phase.
    writer_host_hints_.erase(client_id);
    return session;
}

ClientSessionPtr ClientRegistry::GetOrCreateLocked(const UUID& client_id,
                                                   Clock::time_point now) {
    if (auto session = FindLocked(client_id)) return session;
    auto session = ClientSessionPtr(new ClientSession(client_id, now));
    // Retain the hint until registration commits so rollback preserves a
    // writer that has not successfully mounted storage yet.
    if (const auto hint = writer_host_hints_.find(client_id);
        hint != writer_host_hints_.end()) {
        session->SetHostId(hint->second);
    }
    sessions_.emplace(client_id, session);
    if (account_metrics_)
        MasterMetricManager::instance().client_liveness_record_created();
    return session;
}

ClientSessionPtr ClientRegistry::Find(const UUID& client_id) const {
    std::shared_lock lock(mutex_);
    return FindLocked(client_id);
}

ClientSessionPtr ClientRegistry::FindLocked(const UUID& client_id) const {
    const auto it = sessions_.find(client_id);
    return it == sessions_.end() ? nullptr : it->second;
}

ClientRegistry::Sessions ClientRegistry::Snapshot() const {
    std::shared_lock lock(mutex_);
    return sessions_;
}

bool ClientRegistry::Remove(const ClientSessionPtr& session) {
    std::unique_lock lock(mutex_);
    return RemoveLocked(session);
}

bool ClientRegistry::RemoveLocked(const ClientSessionPtr& session,
                                  bool clear_client_state) {
    if (!session || FindLocked(session->client_id()) != session) return false;
    if (account_metrics_)
        MasterMetricManager::instance().on_client_liveness_record_removed(
            session->state());
    if (clear_client_state) ClearClientStateLocked(session->client_id());
    sessions_.erase(session->client_id());
    return true;
}

void ClientRegistry::ClearClientStateLocked(const UUID& client_id) {
    if (remounted_.erase(client_id) && account_metrics_)
        MasterMetricManager::instance().dec_active_clients();
    writer_host_hints_.erase(client_id);
}

void ClientRegistry::Clear() {
    std::unique_lock lock(mutex_);
    ClearLocked();
}

void ClientRegistry::ClearLocked() {
    if (account_metrics_) {
        for (const auto& [_, session] : sessions_)
            MasterMetricManager::instance().on_client_liveness_record_removed(
                session->state());
        for (size_t i = 0; i < remounted_.size(); ++i)
            MasterMetricManager::instance().dec_active_clients();
    }
    sessions_.clear();
    remounted_.clear();
    writer_host_hints_.clear();
}

void ClientRegistry::ImportLocked(const Sessions& staged) {
    for (const auto& [id, session] : staged) {
        CHECK(!sessions_.contains(id));
        CHECK(session && session->client_id() == id);
        CHECK(session->state() == ClientLivenessState::ACTIVE);
    }
    for (const auto& [id, session] : staged) {
        sessions_.emplace(id, session);
        if (account_metrics_)
            MasterMetricManager::instance().client_liveness_record_created();
    }
}

void ClientRegistry::Replace(ClientRegistry& staged) {
    CHECK(!staged.account_metrics_);
    CHECK(&staged != this);
    const auto sessions = staged.Snapshot();
    std::unique_lock lock(mutex_);
    ClearLocked();
    ImportLocked(sessions);
}

void ClientRegistry::AccountObservation(ClientLivenessObservation observation) {
    if (account_metrics_ &&
        observation == ClientLivenessObservation::RECOVERED_ACTIVE)
        MasterMetricManager::instance().client_liveness_recovered();
}

ClientLivenessObservation ClientRegistry::Observe(
    const ClientSessionPtr& session, Clock::time_point now) {
    std::shared_lock lock(mutex_);
    if (!session || FindLocked(session->client_id()) != session)
        return ClientLivenessObservation::REJECTED_OFFLINE;
    const auto observation = session->Observe(now);
    AccountObservation(observation);
    return observation;
}

ClientStatus ClientRegistry::Ping(const UUID& client_id) {
    std::shared_lock lock(mutex_);
    const auto session = FindLocked(client_id);
    if (!session) return ClientStatus::NEED_REMOUNT;
    const auto observation = session->Observe(Clock::now());
    AccountObservation(observation);
    if (observation == ClientLivenessObservation::RECOVERED_ACTIVE)
        LOG(INFO) << "client_id=" << client_id
                  << ", action=client_liveness_recovered, signal=ping";
    return observation != ClientLivenessObservation::REJECTED_OFFLINE &&
                   remounted_.contains(client_id)
               ? ClientStatus::OK
               : ClientStatus::NEED_REMOUNT;
}

std::string ClientRegistry::ResolveHostId(const UUID& client_id,
                                          const std::string& hint) {
    {
        // Session hosts have their own synchronization. Only changes to the
        // writer-only hint map require exclusive registry access.
        std::shared_lock lock(mutex_);
        if (const auto session = FindLocked(client_id)) {
            return ResolveSessionHostId(*session, hint);
        }

        const auto cached_hint = writer_host_hints_.find(client_id);
        if (hint.empty()) {
            return cached_hint == writer_host_hints_.end()
                       ? std::string{}
                       : cached_hint->second;
        }
        if (cached_hint != writer_host_hints_.end() &&
            cached_hint->second == hint) {
            return hint;
        }
    }

    std::unique_lock lock(mutex_);
    // Registration may have created a session while we changed lock modes.
    // In that case the hint belongs to the session, not the writer-only map.
    if (const auto session = FindLocked(client_id)) {
        return ResolveSessionHostId(*session, hint);
    }
    writer_host_hints_[client_id] = hint;
    return hint;
}

ClientLivenessTransition ClientRegistry::Evaluate(
    const ClientSessionPtr& session, Clock::time_point now,
    Clock::duration active_ttl, Clock::duration suspicion_ttl) {
    ClientLivenessTransition transition;
    {
        std::shared_lock lock(mutex_);
        if (!session || FindLocked(session->client_id()) != session)
            return ClientLivenessTransition::NONE;
        transition = session->EvaluateAndRetire(
            now, active_ttl, suspicion_ttl,
            [this] { offboarding_worker_->ReserveJob(); }, [] {});
        if (account_metrics_) {
            if (transition == ClientLivenessTransition::BECAME_SUSPECTED)
                MasterMetricManager::instance()
                    .client_liveness_became_suspected();
            else if (transition == ClientLivenessTransition::BECAME_OFFLINE)
                MasterMetricManager::instance()
                    .client_liveness_became_offline();
        }
    }
    if (transition == ClientLivenessTransition::BECAME_OFFLINE) {
        {
            std::unique_lock lock(mutex_);
            if (FindLocked(session->client_id()) == session)
                ClearClientStateLocked(session->client_id());
        }
        LOG(INFO) << "client_id=" << session->client_id()
                  << ", action=client_liveness_offline";
        ScheduleClientOffboarding(session);
    } else if (transition == ClientLivenessTransition::BECAME_SUSPECTED) {
        LOG(INFO) << "client_id=" << session->client_id()
                  << ", action=client_liveness_suspected";
    }
    return transition;
}

void ClientRegistry::ScheduleClientOffboarding(
    const ClientSessionPtr& session) {
    ClientOffboardingJob job;
    job.client_id = session->client_id();
    job.liveness = session;
    offboarding_worker_->ScheduleReserved(std::move(job));
}

bool ClientRegistry::ProcessClientOffboardingJob(ClientOffboardingJob& job) {
    if (!job.liveness || Find(job.client_id) != job.liveness) return true;
    if (!cleanup_resources_(job)) return false;
    Remove(job.liveness);
    return true;
}

void ClientRegistry::StartOffboarding(CleanupResources cleanup) {
    CHECK(cleanup);
    CHECK(!offboarding_worker_->HasPending());
    cleanup_resources_ = std::move(cleanup);
    offboarding_worker_->Start();
}

void ClientRegistry::StartMonitoring(Clock::duration active_ttl,
                                     Clock::duration suspicion_ttl,
                                     CleanupResources cleanup,
                                     Clock::duration interval) {
    CHECK(!monitor_.joinable());
    CHECK(interval > Clock::duration::zero());
    StartOffboarding(std::move(cleanup));
    try {
        monitor_ = std::jthread(
            [this, active_ttl, suspicion_ttl, interval](std::stop_token stop) {
                std::mutex wait_mutex;
                std::condition_variable_any wake;
                while (!stop.stop_requested()) {
                    const auto now = Clock::now();
                    for (const auto& [_, session] : Snapshot()) {
                        if (stop.stop_requested()) break;
                        Evaluate(session, now, active_ttl, suspicion_ttl);
                    }
                    std::unique_lock lock(wait_mutex);
                    wake.wait_for(lock, stop, interval, [] { return false; });
                }
            });
    } catch (...) {
        offboarding_worker_->Stop();
        throw;
    }
}

void ClientRegistry::StopMonitoring() {
    if (monitor_.joinable()) {
        monitor_.request_stop();
        monitor_.join();
    }
}

void ClientRegistry::Stop() {
    StopMonitoring();
    offboarding_worker_->Stop();
}

bool ClientRegistry::HasPendingOffboarding() const {
    return offboarding_worker_->HasPending();
}

}  // namespace mooncake
