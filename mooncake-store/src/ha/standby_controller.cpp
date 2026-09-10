#include "ha/standby_controller.h"

#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <glog/logging.h>

#include "ha/kv/etcd_ha_kv_backend.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_coordinator.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_provider.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/catalog_backed_snapshot_provider.h"
#include "ha/snapshot/object/snapshot_object_store.h"
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
        .enable_snapshot_bootstrap =
            config.enable_oplog_snapshot || config.enable_snapshot_restore,
        .enable_oplog_following = capabilities.has_oplog_following,
        .oplog_poll_interval_ms = config.oplog_poll_interval_ms,
        .batch_oplog_retry_timeout_sec = config.batch_oplog_retry_timeout_sec,
    });
}

StandbyRuntimeCapabilities BuildStandbyRuntimeCapabilities(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config) {
    StandbyRuntimeCapabilities capabilities;
    capabilities.has_snapshot_bootstrap =
        config.enable_oplog_snapshot || config.enable_snapshot_restore;
    capabilities.has_oplog_following =
        config.enable_oplog && spec.type == HABackendType::ETCD;
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

    tl::expected<PromotionContext, ErrorCode> PromoteStandbyAndExport()
        override {
        return PromotionContext{};
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

class CapabilityDrivenStandbyController final : public StandbyController {
   public:
    CapabilityDrivenStandbyController(
        const HABackendSpec& spec, const MasterServiceSupervisorConfig& config)
        : spec_(spec),
          config_(config),
          capabilities_(BuildStandbyRuntimeCapabilities(spec, config)),
          standby_service_(CreateStandbyService(config, capabilities_)) {
        if (config_.enable_snapshot_restore && !config_.enable_oplog_snapshot) {
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

        if (config_.enable_oplog_snapshot) {
            try {
                if (!capabilities_.has_oplog_following ||
                    config_.snapshot_chunk_object_count == 0) {
                    throw std::invalid_argument(
                        "batch snapshot requires etcd OpLog and positive chunk "
                        "count");
                }
                batch_backend_ = std::make_unique<EtcdHaKvBackend>();
                auto type = ParseSnapshotObjectStoreType(
                    config_.snapshot_object_store_type);
                snapshot_object_store_ = SnapshotObjectStore::Create(type);
                const std::string snapshot_root =
                    BuildBatchOpLogSnapshotRoot(config_.cluster_id);
                if (snapshot_root.empty()) {
                    throw std::invalid_argument(
                        "batch OpLog snapshot requires a valid cluster_id");
                }
                standby_service_->SetBatchOpLogSnapshotProvider(
                    std::make_unique<BatchOpLogSnapshotProvider>(
                        config_.cluster_id, *batch_backend_,
                        *snapshot_object_store_, snapshot_root));
                batch_oplog_snapshot_coordinator_ =
                    std::make_unique<BatchOpLogSnapshotCoordinator>(
                        *standby_service_, *batch_backend_,
                        *snapshot_object_store_, config_.cluster_id,
                        BatchOpLogSnapshotCoordinatorConfig{
                            .snapshot_interval_seconds =
                                config_.snapshot_interval_seconds,
                            .chunk_object_count =
                                config_.snapshot_chunk_object_count,
                            .snapshot_root = snapshot_root,
                            .clock = {},
                        });
            } catch (const std::exception& e) {
                throw std::runtime_error(
                    std::string("Failed to initialize batch OpLog snapshot: ") +
                    e.what());
            }
        }

        if (capabilities_.has_oplog_following) {
            oplog_connstring_ = config_.ha_backend_connstring.empty()
                                    ? config_.etcd_endpoints
                                    : config_.ha_backend_connstring;
        }

        standby_service_->SetSyncStatusCallback(
            [this](const StandbySyncStatus& status) {
                if (status.state == StandbyState::FAILED ||
                    status.state == StandbyState::STOPPED) {
                    std::lock_guard<std::mutex> lock(state_mutex_);
                    standby_running_ = false;
                    last_standby_error_ = status.last_error;
                }
                NotifyRuntimeStateIfChanged();
            });
    }

    ~CapabilityDrivenStandbyController() override {
        standby_service_->SetSyncStatusCallback({});
        standby_service_->Stop();
        batch_oplog_snapshot_coordinator_.reset();
        standby_service_.reset();
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
            const StandbySyncStatus status = standby_service_->GetSyncStatus();
            standby_running_ = err == ErrorCode::OK &&
                               status.state != StandbyState::FAILED &&
                               status.state != StandbyState::STOPPED;
            if (standby_running_) {
                last_standby_error_ = ErrorCode::OK;
            } else if (status.last_error != ErrorCode::OK) {
                last_standby_error_ = status.last_error;
            } else {
                last_standby_error_ = err;
            }
        }
        if (err == ErrorCode::OK) {
            if (batch_oplog_snapshot_coordinator_) {
                try {
                    batch_oplog_snapshot_coordinator_->Start();
                } catch (const std::exception& error) {
                    LOG(ERROR)
                        << "Snapshot worker start failed: " << error.what();
                    StopStandby();
                    return ErrorCode::INTERNAL_ERROR;
                }
            }
            NotifyRuntimeStateIfChanged();
        }
        return err;
    }

    void StopStandby() override {
        if (batch_oplog_snapshot_coordinator_) {
            batch_oplog_snapshot_coordinator_->Stop();
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
        if (err == ErrorCode::OK && batch_oplog_snapshot_coordinator_) {
            batch_oplog_snapshot_coordinator_->Stop();
        }

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            standby_running_ = false;
            last_standby_error_ = err;
        }
        NotifyRuntimeStateIfChanged();
        return err;
    }

    tl::expected<PromotionContext, ErrorCode> PromoteStandbyAndExport()
        override {
        // Verify standby is running first (same check as PromoteStandby)
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
            return tl::unexpected(promote_error);
        }

        if (standby_service_->IsBatchOpLogSnapshotMode()) {
            auto handoff = standby_service_->PromoteAndDetachBatchOpLogStore();
            if (!handoff) {
                standby_service_->Stop();
                {
                    std::lock_guard<std::mutex> lock(state_mutex_);
                    standby_running_ = false;
                    last_standby_error_ = handoff.error();
                }
                NotifyRuntimeStateIfChanged();
                return tl::unexpected(handoff.error());
            }

            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                standby_running_ = false;
                last_standby_error_ = ErrorCode::OK;
            }
            NotifyRuntimeStateIfChanged();

            PromotionContext ctx;
            ctx.applied_seq_id = handoff->applied_cursor.last_seq;
            ctx.metadata_store = std::move(handoff->metadata_store);
            ctx.segments = std::move(handoff->segments);
            ctx.applied_cursor = handoff->applied_cursor;
            ctx.producer_view_version = handoff->producer_view_version;
            ctx.max_replica_id = handoff->max_replica_id;
            if (batch_oplog_snapshot_coordinator_) {
                batch_oplog_snapshot_coordinator_->Stop();
            }
            return ctx;
        }

        // Atomic legacy promote + export (final catch-up happens inside).
        StandbySnapshot snapshot;
        ErrorCode err = standby_service_->PromoteAndExportSnapshot(snapshot);
        if (err != ErrorCode::OK) {
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                standby_running_ = false;
                last_standby_error_ = err;
            }
            NotifyRuntimeStateIfChanged();
            return tl::unexpected(err);
        }

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            standby_running_ = false;
            last_standby_error_ = ErrorCode::OK;
        }
        NotifyRuntimeStateIfChanged();

        PromotionContext ctx;
        ctx.applied_seq_id = snapshot.oplog_sequence_id;
        ctx.objects = std::move(snapshot.objects);
        ctx.segments = std::move(snapshot.segments);

        return ctx;
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
    std::unique_ptr<HaKvBackend> batch_backend_;
    std::unique_ptr<SnapshotObjectStore> snapshot_object_store_;
    std::unique_ptr<BatchOpLogSnapshotCoordinator>
        batch_oplog_snapshot_coordinator_;
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
