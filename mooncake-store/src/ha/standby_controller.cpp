#include "ha/standby_controller.h"

#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <glog/logging.h>

#include "ha/snapshot/catalog_backed_snapshot_provider.h"
#include "hot_standby_service.h"

namespace mooncake {
namespace ha {

namespace {

struct StandbyRuntimeCapabilities {
    bool has_snapshot_bootstrap{false};
    bool has_oplog_following{false};
};

std::unique_ptr<HotStandbyService> CreateStandbyService(
    const MasterServiceSupervisorConfig& config,
    const StandbyRuntimeCapabilities& capabilities) {
    return std::make_unique<HotStandbyService>(HotStandbyConfig{
        .standby_id = config.local_hostname,
        .primary_address = "",
        .verification_interval_sec = 30,
        .max_replication_lag_entries = 1000,
        .enable_verification = false,
        .enable_snapshot_bootstrap = config.enable_snapshot_restore,
        .enable_oplog_following = capabilities.has_oplog_following,
    });
}

StandbyRuntimeCapabilities BuildStandbyRuntimeCapabilities(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config) {
    StandbyRuntimeCapabilities capabilities;
    capabilities.has_snapshot_bootstrap = config.enable_snapshot_restore;
    capabilities.has_oplog_following = spec.type == HABackendType::ETCD;
    return capabilities;
}

MasterRuntimeState MapStandbyRuntimeState(
    const StandbySyncStatus& status,
    const std::optional<MasterView>& observed_leader,
    const StandbyRuntimeCapabilities& capabilities) {
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
            if (capabilities.has_oplog_following &&
                observed_leader.has_value() && status.lag_entries > 0) {
                return MasterRuntimeState::kCatchingUp;
            }
            return MasterRuntimeState::kStandby;
        case StandbyState::PROMOTING:
        case StandbyState::PROMOTED:
            return MasterRuntimeState::kLeaderWarmup;
    }
    return MasterRuntimeState::kStandby;
}

class NoopStandbyController final : public StandbyController {
   public:
    ErrorCode StartStandby(const std::optional<MasterView>&) override {
        return ErrorCode::OK;
    }

    void StopStandby() override {}

    ErrorCode PromoteStandby() override { return ErrorCode::OK; }

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

class CapabilityDrivenStandbyController final : public StandbyController {
   public:
    CapabilityDrivenStandbyController(
        const HABackendSpec& spec, const MasterServiceSupervisorConfig& config)
        : spec_(spec),
          config_(config),
          capabilities_(BuildStandbyRuntimeCapabilities(spec, config)),
          standby_service_(CreateStandbyService(config, capabilities_)) {
        if (capabilities_.has_snapshot_bootstrap) {
            auto snapshot_provider =
                CreateCatalogBackedSnapshotProvider(config_);
            if (!snapshot_provider) {
                dependency_init_error_ = snapshot_provider.error();
                LOG(ERROR) << "Failed to initialize standby snapshot provider, "
                           << "backend=" << HABackendTypeToString(spec_.type)
                           << ", error=" << toString(dependency_init_error_);
            } else {
                standby_service_->SetSnapshotProvider(
                    std::move(snapshot_provider.value()));
            }
        }

        if (capabilities_.has_oplog_following) {
            oplog_connstring_ = config_.ha_backend_connstring.empty()
                                    ? config_.etcd_endpoints
                                    : config_.ha_backend_connstring;
        }

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

        if (dependency_init_error_ != ErrorCode::OK) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            last_standby_error_ = dependency_init_error_;
            return dependency_init_error_;
        }

        ErrorCode err = standby_service_->Start(
            observed_leader.has_value() ? observed_leader->leader_address : "",
            oplog_connstring_, config_.cluster_id);

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            standby_running_ = err == ErrorCode::OK;
            last_standby_error_ = err;
        }
        if (err == ErrorCode::OK) {
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
            last_standby_error_ = ErrorCode::OK;
        }
        NotifyRuntimeStateIfChanged();
    }

    ErrorCode PromoteStandby() override {
        ErrorCode promote_error = ErrorCode::OK;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (!standby_running_) {
                promote_error = last_standby_error_ != ErrorCode::OK
                                    ? last_standby_error_
                                    : ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
            }
        }
        if (promote_error != ErrorCode::OK) {
            return promote_error;
        }

        ErrorCode err = standby_service_->Promote();
        if (err != ErrorCode::OK) {
            standby_service_->Stop();
        }

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            standby_running_ = false;
            last_standby_error_ = err;
        }
        NotifyRuntimeStateIfChanged();
        return err;
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
                                      observed_leader, capabilities_);
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

    HABackendSpec spec_;
    MasterServiceSupervisorConfig config_;
    StandbyRuntimeCapabilities capabilities_;
    std::unique_ptr<HotStandbyService> standby_service_;
    ErrorCode dependency_init_error_{ErrorCode::OK};
    std::string oplog_connstring_;

    mutable std::mutex state_mutex_;
    std::optional<MasterView> observed_leader_;
    bool standby_running_ = false;
    ErrorCode last_standby_error_{ErrorCode::OK};

    std::mutex callback_mutex_;
    std::optional<RuntimeStateCallback> runtime_state_callback_;
    std::optional<MasterRuntimeState> last_reported_runtime_state_;
};

}  // namespace

std::unique_ptr<StandbyController> CreateStandbyController(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config) {
    const auto capabilities = BuildStandbyRuntimeCapabilities(spec, config);
    if (capabilities.has_snapshot_bootstrap ||
        capabilities.has_oplog_following) {
        return std::make_unique<CapabilityDrivenStandbyController>(spec,
                                                                   config);
    }

    LOG(INFO) << "HA standby controller falls back to noop, backend_type="
              << HABackendTypeToString(spec.type);
    return std::make_unique<NoopStandbyController>();
}

}  // namespace ha
}  // namespace mooncake
