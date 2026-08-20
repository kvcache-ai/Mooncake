#include "ha/p2p_standby_controller.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "p2p/ha/oplog/p2p_hot_standby_service.h"
#include "p2p/ha/p2p_promotion_data.h"
#include "p2p/ha/oplog/oplog_store_factory.h"

namespace mooncake {
namespace ha {

namespace {

MasterRuntimeState MapP2PStandbyState(P2PStandbyState state) {
    switch (state) {
        case P2PStandbyState::STOPPED:
            return MasterRuntimeState::kStandby;
        case P2PStandbyState::CONNECTING:
        case P2PStandbyState::SYNCING:
        case P2PStandbyState::RECOVERING:
        case P2PStandbyState::RECONNECTING:
        case P2PStandbyState::FAILED:
            return MasterRuntimeState::kRecovering;
        case P2PStandbyState::WATCHING:
            return MasterRuntimeState::kCatchingUp;
        case P2PStandbyState::PROMOTING:
        case P2PStandbyState::PROMOTED:
            return MasterRuntimeState::kLeaderWarmup;
    }
    return MasterRuntimeState::kStandby;
}

P2PHotStandbyConfig BuildStandbyConfig(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config) {
    P2PHotStandbyConfig standby_config;
    standby_config.cluster_id = config.cluster_id;
    standby_config.oplog_store_type = ParseOpLogStoreType(config.oplog_store_type);
    standby_config.oplog_store_root_dir = config.oplog_data_dir;
    standby_config.redis_endpoint = config.redis_endpoint;
    standby_config.redis_username = config.redis_username;
    standby_config.redis_password = config.redis_password;
    standby_config.redis_db_index = config.redis_db_index;
    standby_config.oplog_poll_interval_ms = config.oplog_poll_interval_ms;
    // Snapshot bootstrap is not yet wired through MasterServiceSupervisorConfig;
    // port=0 disables the snapshot service while oplog following stays active.
    standby_config.snapshot_service_port = 0;
    (void)spec;
    return standby_config;
}

}  // namespace

P2PStandbyController::P2PStandbyController(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config)
    : standby_(std::make_unique<P2PHotStandbyService>(BuildStandbyConfig(spec, config))) {}

P2PStandbyController::~P2PStandbyController() { StopStandby(); }

ErrorCode P2PStandbyController::StartStandby(
    const std::optional<MasterView>& /*observed_leader*/) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto err = standby_->Start();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "P2PStandbyController: failed to start P2P hot standby, error="
                   << toString(err);
    }
    ReportRuntimeState();
    return err;
}

void P2PStandbyController::StopStandby() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (standby_) standby_->Stop();
}

ErrorCode P2PStandbyController::PromoteStandby() {
    auto ctx = PromoteStandbyAndExport();
    if (!ctx) return ctx.error();
    return ErrorCode::OK;
}

tl::expected<PromotionContext, ErrorCode>
P2PStandbyController::PromoteStandbyAndExport() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto promote_err = standby_->Promote();
    if (promote_err != ErrorCode::OK) {
        LOG(ERROR) << "P2PStandbyController: promote failed, error="
                   << toString(promote_err);
        return tl::unexpected(promote_err);
    }
    PromotionContext ctx;
    P2PPromotionData p2p_data;
    p2p_data.metadata = standby_->ExportMetadata();
    p2p_data.applied_sequence_id = standby_->GetLatestAppliedSequenceId();
    ctx.applied_seq_id = p2p_data.applied_sequence_id;
    ctx.p2p_promotion_data = std::move(p2p_data);
    LOG(INFO) << "P2PStandbyController: promoted, applied_seq=" << ctx.applied_seq_id;
    return ctx;
}

void P2PStandbyController::UpdateObservedLeader(
    const std::optional<MasterView>& /*observed_leader*/) {
    // P2P hot standby discovers the primary through its own oplog replicator
    // (redis master registry / localfs polling); the supervisor-provided
    // observed leader is not consumed here.
}

MasterRuntimeState P2PStandbyController::GetStandbyRuntimeState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return MapP2PStandbyState(standby_->GetState());
}

void P2PStandbyController::SetStandbyRuntimeStateCallback(
    RuntimeStateCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = std::move(callback);
    ReportRuntimeState();
}

void P2PStandbyController::ReportRuntimeState() {
    if (callback_) {
        callback_(MapP2PStandbyState(standby_->GetState()));
    }
}

}  // namespace ha
}  // namespace mooncake
