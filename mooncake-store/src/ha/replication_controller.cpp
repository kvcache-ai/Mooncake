#include "ha/replication_controller.h"

#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <glog/logging.h>

#include "ha/oplog/oplog_backend_config.h"
#include "ha/snapshot/catalog_backed_snapshot_provider.h"
#include "ha/oplog/oplog_store_factory.h"
#include "ha/standby/hot_standby_service.h"

namespace mooncake {
namespace ha {

namespace {

struct StandbyRuntimeCapabilities {
    bool has_snapshot_bootstrap{false};
    bool has_oplog_following{false};
};

std::unique_ptr<HotStandbyService> CreateStandbyService(
    const MasterServiceSupervisorConfig& config,
    const StandbyRuntimeCapabilities& capabilities,
    ha::HABackendType oplog_backend_type, const HABackendSpec& ha_spec) {
    return std::make_unique<HotStandbyService>(HotStandbyConfig{
        .standby_id = config.local_hostname,
        .primary_address = "",
        .verification_interval_sec = 30,
        .max_replication_lag_entries = 1000,
        .enable_verification = false,
        .enable_snapshot_bootstrap = config.enable_snapshot_restore,
        .enable_oplog_following = capabilities.has_oplog_following,
        .oplog_backend_type = oplog_backend_type,
        .progress_backend_type = ha_spec.type,
        .progress_backend_connstring = ha_spec.connstring,
    });
}

StandbyRuntimeCapabilities BuildStandbyRuntimeCapabilities(
    const HABackendSpec& oplog_spec,
    const MasterServiceSupervisorConfig& config) {
    StandbyRuntimeCapabilities capabilities;
    capabilities.has_snapshot_bootstrap = config.enable_snapshot_restore;
    capabilities.has_oplog_following =
        SupportsOpLogFollowing(oplog_spec.type) &&
        !oplog_spec.connstring.empty();
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

class NoopReplicationController final : public ReplicationController {
   public:
    ErrorCode StartStandby(const std::optional<MasterView>&) override {
        return ErrorCode::OK;
    }

    void StopStandby() override {}

    ErrorCode PromoteStandby() override { return ErrorCode::OK; }

    tl::expected<std::optional<PromotedStandbyState>, ErrorCode>
    TakePromotedStandbyState() override {
        return std::optional<PromotedStandbyState>();
    }

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

class CapabilityDrivenReplicationController final
    : public ReplicationController {
   public:
    CapabilityDrivenReplicationController(
        const HABackendSpec& spec, const MasterServiceSupervisorConfig& config)
        : spec_(spec), config_(config) {
        auto oplog_spec = BuildConfiguredOpLogBackendSpec(
            HABackendTypeToString(spec_.type), spec_.connstring,
            config_.oplog_backend_type, config_.oplog_backend_connstring,
            config_.cluster_id);
        if (!oplog_spec) {
            dependency_init_error_ = oplog_spec.error();
            LOG(ERROR) << "Failed to resolve standby OpLog backend spec, "
                       << "error=" << toString(dependency_init_error_);
            standby_service_ = CreateStandbyService(
                config_, capabilities_, HABackendType::UNKNOWN, spec_);
        } else {
            oplog_spec_ = std::move(oplog_spec.value());
            capabilities_ =
                BuildStandbyRuntimeCapabilities(oplog_spec_.value(), config_);
            standby_service_ = CreateStandbyService(config_, capabilities_,
                                                    oplog_spec_->type, spec_);

            if (SupportsOpLogFollowing(oplog_spec_->type) &&
                oplog_spec_->connstring.empty() &&
                HasExplicitOpLogBackendConfig(
                    config_.oplog_backend_type,
                    config_.oplog_backend_connstring)) {
                dependency_init_error_ = ErrorCode::INVALID_PARAMS;
                LOG(ERROR) << "Standby OpLog backend requires a connection "
                              "string, backend="
                           << HABackendTypeToString(oplog_spec_->type);
            } else if (capabilities_.has_oplog_following) {
                oplog_connstring_ = oplog_spec_->connstring;
            }
        }

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
            promoted_state_.reset();
            standby_running = standby_running_;
        }
        if (standby_running) {
            NotifyRuntimeStateIfChanged();
            return ErrorCode::OK;
        }

        if (dependency_init_error_ != ErrorCode::OK) {
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                standby_last_error_ = dependency_init_error_;
            }
            NotifyRuntimeStateIfChanged();
            return dependency_init_error_;
        }

        ErrorCode err = standby_service_->Start(
            observed_leader.has_value() ? observed_leader->leader_address : "",
            oplog_connstring_, config_.cluster_id);

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            standby_running_ = (err == ErrorCode::OK);
            standby_last_error_ = (err == ErrorCode::OK ? ErrorCode::OK : err);
            if (err == ErrorCode::OK) {
                promoted_state_.reset();
            }
        }
        NotifyRuntimeStateIfChanged();
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
            promoted_state_.reset();
        }
        NotifyRuntimeStateIfChanged();
    }

    ErrorCode PromoteStandby() override {
        bool standby_running = false;
        ErrorCode standby_last_error = ErrorCode::OK;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            standby_running = standby_running_;
            standby_last_error = standby_last_error_;
        }
        if (!standby_running) {
            return standby_last_error == ErrorCode::OK
                       ? ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS
                       : standby_last_error;
        }

        ErrorCode err = standby_service_->Promote();
        std::optional<PromotedStandbyState> promoted_state;
        if (err == ErrorCode::OK) {
            std::vector<std::pair<std::string, StandbyObjectMetadata>>
                metadata_snapshot;
            std::vector<StandbySegmentInfo> segment_registry;
            if (!standby_service_->ExportMetadataSnapshot(metadata_snapshot)) {
                LOG(ERROR) << "Failed to export promoted standby metadata";
                err = ErrorCode::INTERNAL_ERROR;
            } else if (!standby_service_->ExportSegmentRegistry(
                           segment_registry)) {
                LOG(ERROR) << "Failed to export promoted standby segment "
                              "registry";
                err = ErrorCode::INTERNAL_ERROR;
            } else {
                promoted_state = PromotedStandbyState{
                    .metadata_snapshot = std::move(metadata_snapshot),
                    .segment_registry = std::move(segment_registry),
                    .applied_seq_id =
                        standby_service_->GetLatestAppliedSequenceId(),
                };
            }
        }
        if (err != ErrorCode::OK) {
            standby_service_->Stop();
        }

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            standby_running_ = false;
            promoted_state_ =
                err == ErrorCode::OK ? std::move(promoted_state) : std::nullopt;
            standby_last_error_ = (err == ErrorCode::OK)
                                      ? ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS
                                      : err;
        }
        NotifyRuntimeStateIfChanged();
        return err;
    }

    tl::expected<std::optional<PromotedStandbyState>, ErrorCode>
    TakePromotedStandbyState() override {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto promoted_state = std::move(promoted_state_);
        promoted_state_.reset();
        return promoted_state;
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
    std::optional<HABackendSpec> oplog_spec_;
    MasterServiceSupervisorConfig config_;
    StandbyRuntimeCapabilities capabilities_;
    std::unique_ptr<HotStandbyService> standby_service_;
    ErrorCode dependency_init_error_{ErrorCode::OK};
    std::string oplog_connstring_;

    mutable std::mutex state_mutex_;
    std::optional<MasterView> observed_leader_;
    bool standby_running_ = false;
    ErrorCode standby_last_error_ = ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    std::optional<PromotedStandbyState> promoted_state_;

    std::mutex callback_mutex_;
    std::optional<RuntimeStateCallback> runtime_state_callback_;
    std::optional<MasterRuntimeState> last_reported_runtime_state_;
};

}  // namespace

std::unique_ptr<ReplicationController> CreateReplicationController(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config) {
    auto oplog_spec = BuildConfiguredOpLogBackendSpec(
        HABackendTypeToString(spec.type), spec.connstring,
        config.oplog_backend_type, config.oplog_backend_connstring,
        config.cluster_id);
    const auto capabilities =
        oplog_spec
            ? BuildStandbyRuntimeCapabilities(oplog_spec.value(), config)
            : StandbyRuntimeCapabilities{
                  .has_snapshot_bootstrap = config.enable_snapshot_restore,
                  .has_oplog_following = false,
              };
    const bool wants_oplog_following =
        HasExplicitOpLogBackendConfig(config.oplog_backend_type,
                                      config.oplog_backend_connstring) ||
        (oplog_spec.has_value() && capabilities.has_oplog_following);
    if (config.enable_snapshot_restore || wants_oplog_following) {
        return std::make_unique<CapabilityDrivenReplicationController>(spec,
                                                                       config);
    }

    LOG(INFO) << "HA replication controller falls back to noop, backend_type="
              << HABackendTypeToString(spec.type);
    return std::make_unique<NoopReplicationController>();
}

}  // namespace ha
}  // namespace mooncake
