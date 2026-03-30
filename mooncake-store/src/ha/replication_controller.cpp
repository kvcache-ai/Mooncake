#include "ha/replication_controller.h"

#include <mutex>
#include <memory>
#include <optional>

#include <glog/logging.h>

#include "hot_standby_service.h"

namespace mooncake {
namespace ha {

namespace {

class NoopReplicationController final : public ReplicationController {
   public:
    ErrorCode StartStandby(const std::optional<MasterView>&) override {
        return ErrorCode::OK;
    }

    void StopStandby() override {}

    ErrorCode PrepareToServe() override { return ErrorCode::OK; }

    void UpdateObservedLeader(const std::optional<MasterView>&) override {}

    MasterRuntimeState GetStandbyRuntimeState() const override {
        return MasterRuntimeState::kStandby;
    }

    void SetStandbyRuntimeStateCallback(
        RuntimeStateCallback callback) override {
        callback_ = std::move(callback);
        if (callback_) {
            callback_(MasterRuntimeState::kStandby);
        }
    }

   private:
    RuntimeStateCallback callback_;
};

#ifdef STORE_USE_ETCD
MasterRuntimeState MapStandbyRuntimeState(
    const StandbySyncStatus& status,
    const std::optional<MasterView>& observed_leader) {
    switch (status.state) {
        case StandbyState::STOPPED:
            return MasterRuntimeState::kStandby;
        case StandbyState::CONNECTING:
        case StandbyState::SYNCING:
        case StandbyState::RECOVERING:
        case StandbyState::RECONNECTING:
        case StandbyState::FAILED:
            return MasterRuntimeState::kRecovering;
        case StandbyState::WATCHING:
            if (observed_leader.has_value() && status.lag_entries > 0) {
                return MasterRuntimeState::kCatchingUp;
            }
            return MasterRuntimeState::kStandby;
        case StandbyState::PROMOTING:
        case StandbyState::PROMOTED:
            return MasterRuntimeState::kLeaderWarmup;
    }
    return MasterRuntimeState::kStandby;
}

class EtcdReplicationController final : public ReplicationController {
   public:
    explicit EtcdReplicationController(
        const MasterServiceSupervisorConfig& config)
        : config_(config),
          standby_service_(std::make_unique<HotStandbyService>(HotStandbyConfig{
              .standby_id = config.local_hostname,
              .verification_interval_sec = 30,
              .max_replication_lag_entries = 1000,
              .enable_verification = false,
              .enable_snapshot_bootstrap = config.enable_snapshot_restore,
          })) {
        standby_service_->SetSyncStatusCallback(
            [this](const StandbySyncStatus&) {
                NotifyRuntimeStateIfChanged();
            });
    }

    ErrorCode StartStandby(
        const std::optional<MasterView>& observed_leader) override {
        bool standby_running = false;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            observed_leader_ = observed_leader;
            standby_running = standby_running_;
        }
        if (standby_running) {
            NotifyRuntimeStateIfChanged();
            return ErrorCode::OK;
        }

        ErrorCode err = standby_service_->Start(
            observed_leader.has_value() ? observed_leader->leader_address : "",
            config_.ha_backend_connstring.empty()
                ? config_.etcd_endpoints
                : config_.ha_backend_connstring,
            config_.cluster_id);
        if (err == ErrorCode::OK) {
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                standby_running_ = true;
            }
            NotifyRuntimeStateIfChanged();
        }
        return err;
    }

    void StopStandby() override {
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (!standby_running_) {
                return;
            }
        }

        standby_service_->Stop();
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            standby_running_ = false;
        }
        NotifyRuntimeStateIfChanged();
    }

    ErrorCode PrepareToServe() override {
        StopStandby();
        return ErrorCode::OK;
    }

    void UpdateObservedLeader(
        const std::optional<MasterView>& observed_leader) override {
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            observed_leader_ = observed_leader;
        }
        NotifyRuntimeStateIfChanged();
    }

    MasterRuntimeState GetStandbyRuntimeState() const override {
        std::optional<MasterView> observed_leader;
        bool standby_running = false;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            observed_leader = observed_leader_;
            standby_running = standby_running_;
        }
        if (!standby_running) {
            return MasterRuntimeState::kStandby;
        }
        return MapStandbyRuntimeState(standby_service_->GetSyncStatus(),
                                      observed_leader);
    }

    void SetStandbyRuntimeStateCallback(
        RuntimeStateCallback callback) override {
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            runtime_state_callback_ = std::move(callback);
            last_reported_runtime_state_.reset();
        }
        NotifyRuntimeStateIfChanged();
    }

   private:
    void NotifyRuntimeStateIfChanged() {
        NotifyRuntimeStateIfChanged(GetStandbyRuntimeState());
    }

    void NotifyRuntimeStateIfChanged(MasterRuntimeState runtime_state) {
        RuntimeStateCallback callback;
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            if (!runtime_state_callback_.has_value()) {
                return;
            }
            if (last_reported_runtime_state_.has_value() &&
                last_reported_runtime_state_.value() == runtime_state) {
                return;
            }
            last_reported_runtime_state_ = runtime_state;
            callback = runtime_state_callback_.value();
        }
        callback(runtime_state);
    }

    MasterServiceSupervisorConfig config_;
    std::unique_ptr<HotStandbyService> standby_service_;
    mutable std::mutex state_mutex_;
    std::optional<MasterView> observed_leader_;
    bool standby_running_ = false;
    std::mutex callback_mutex_;
    std::optional<RuntimeStateCallback> runtime_state_callback_;
    std::optional<MasterRuntimeState> last_reported_runtime_state_;
};
#endif

}  // namespace

std::unique_ptr<ReplicationController> CreateReplicationController(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config) {
#ifdef STORE_USE_ETCD
    if (spec.type == HABackendType::ETCD) {
        return std::make_unique<EtcdReplicationController>(config);
    }
#else
    (void)config;
#endif

    LOG(INFO) << "HA replication controller falls back to noop, backend_type="
              << HABackendTypeToString(spec.type);
    return std::make_unique<NoopReplicationController>();
}

}  // namespace ha
}  // namespace mooncake
