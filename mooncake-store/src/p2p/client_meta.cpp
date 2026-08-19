#include "p2p/client_meta.h"
#include <glog/logging.h>
#include "master_metric_manager.h"

namespace mooncake {

int64_t ClientMeta::disconnect_timeout_sec_ = 0;
int64_t ClientMeta::crash_timeout_sec_ = 0;

ClientMeta::ClientMeta(const UUID& client_id) : client_id_(client_id) {
    health_state_.status = P2PClientStatus::HEALTH;
    health_state_.last_heartbeat = std::chrono::steady_clock::now();
}

tl::expected<void, ErrorCode> ClientMeta::MountSegment(const Segment& segment) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }

    auto ret = GetSegmentManager()->MountSegment(segment);
    if (!ret.has_value()) {
        if (ret.error() == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            LOG(WARNING) << "attempt to mount segment but it already exists"
                         << ", client_id=" << client_id_
                         << ", segment_id =" << segment.id
                         << ", segment_name=" << segment.name
                         << ", ret=" << ret.error();
            return {};
        } else {
            LOG(ERROR) << "fail to mount segment"
                       << ", client_id=" << client_id_
                       << ", segment_id =" << segment.id
                       << ", segment_name=" << segment.name
                       << ", ret=" << ret.error();
            return ret;
        }
    }
    LOG(INFO) << "Mount segment success"
              << ", client_id=" << client_id_ << ", segment_id =" << segment.id
              << ", segment_name=" << segment.name;
    return {};
}

tl::expected<void, ErrorCode> ClientMeta::UnmountSegment(
    const UUID& segment_id) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }
    auto ret = GetSegmentManager()->UnmountSegment(segment_id);
    if (!ret.has_value()) {
        if (ret.error() == ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(WARNING) << "attempt to unmount segment but it does not exist"
                         << ", client_id=" << client_id_
                         << ", segment_id=" << segment_id
                         << ", ret=" << ret.error();
            return {};
        } else {
            LOG(ERROR) << "fail to unmount segment"
                       << ", client_id=" << client_id_
                       << ", segment_id=" << segment_id
                       << ", ret=" << ret.error();
            return ret;
        }
    }
    LOG(INFO) << "Unmount segment success"
              << ", client_id=" << client_id_ << ", segment_id =" << segment_id;
    return {};
}

tl::expected<std::vector<Segment>, ErrorCode> ClientMeta::GetSegments() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return GetSegmentManager()->GetSegments();
}

tl::expected<std::pair<size_t, size_t>, ErrorCode> ClientMeta::QuerySegments(
    const std::string& segment_name) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return GetSegmentManager()->QuerySegments(segment_name);
}

tl::expected<std::shared_ptr<Segment>, ErrorCode> ClientMeta::QuerySegment(
    const UUID& segment_id) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return GetSegmentManager()->QuerySegment(segment_id);
}

void ClientMeta::SetSegmentRemovalCallback(SegmentRemovalCallback cb) {
    GetSegmentManager()->SetSegmentRemovalCallback(std::move(cb));
}

void ClientMeta::SetTimeouts(int64_t disconnect_sec, int64_t crash_sec) {
    disconnect_timeout_sec_ = disconnect_sec;
    crash_timeout_sec_ = crash_sec;
}

ClientHealthState ClientMeta::get_health_state() const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return health_state_;
}

bool ClientMeta::is_health() const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return health_state_.status == P2PClientStatus::HEALTH;
}

std::pair<P2PClientStatus, P2PClientStatus> ClientMeta::Heartbeat() {
    SharedMutexLocker lock(&client_mutex_);
    InnerUpdateHeartbeat();
    return InnerUpdateHealthStatus();
}

std::pair<P2PClientStatus, P2PClientStatus> ClientMeta::CheckHealth() {
    SharedMutexLocker lock(&client_mutex_);
    return InnerUpdateHealthStatus();
}

void ClientMeta::InnerUpdateHeartbeat() {
    if (health_state_.status == P2PClientStatus::CRASHED) {
        LOG(WARNING) << "heartbeat received while in CRASHED state, "
                        "timestamp will not update"
                     << ", client_id=" << client_id_;
        return;
    } else if (health_state_.status == P2PClientStatus::DISCONNECTION) {
        LOG(WARNING) << "heartbeat received while in DISCONNECTION state, "
                        "the state might change to HEALTH as soon as possible"
                     << ", client_id=" << client_id_;
    }
    health_state_.last_heartbeat = std::chrono::steady_clock::now();
}

std::pair<P2PClientStatus, P2PClientStatus>
ClientMeta::InnerUpdateHealthStatus() {
    auto now = std::chrono::steady_clock::now();
    P2PClientStatus old_status = health_state_.status;

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now - health_state_.last_heartbeat)
                          .count();

    int64_t disconnect_timeout_ms = disconnect_timeout_sec_ * 1000;
    int64_t crash_timeout_ms = crash_timeout_sec_ * 1000;

    switch (health_state_.status) {
        case P2PClientStatus::HEALTH: {
            if (elapsed_ms >= disconnect_timeout_ms) {
                if (elapsed_ms >= crash_timeout_ms) {
                    health_state_.status = P2PClientStatus::CRASHED;
                } else {
                    health_state_.status = P2PClientStatus::DISCONNECTION;
                }
            }
            break;
        }
        case P2PClientStatus::DISCONNECTION: {
            if (elapsed_ms < disconnect_timeout_ms) {
                health_state_.status = P2PClientStatus::HEALTH;
            } else if (elapsed_ms >= crash_timeout_ms) {
                health_state_.status = P2PClientStatus::CRASHED;
            }
            break;
        }
        case P2PClientStatus::CRASHED:
        case P2PClientStatus::UNDEFINED:
            break;
    }
    if (health_state_.status != old_status) {
        LOG(INFO) << "Client status changed"
                  << ", client_id=" << client_id_
                  << ", old_status=" << HealthToString(old_status)
                  << ", new_status=" << HealthToString(health_state_.status);
    }
    return {old_status, health_state_.status};
}

tl::expected<void, ErrorCode> ClientMeta::InnerStatusCheck() const {
    if (health_state_.status != P2PClientStatus::HEALTH) {
        LOG(WARNING) << "Client is not HEALTH"
                     << ", client_id=" << client_id_
                     << ", status=" << HealthToString(health_state_.status);
        return tl::make_unexpected(ErrorCode::CLIENT_UNHEALTHY);
    }
    return {};
}

void ClientMeta::OnDisconnected() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    if (health_state_.status == P2PClientStatus::HEALTH) {
        return;
    } else if (health_state_.status != P2PClientStatus::DISCONNECTION) {
        LOG(ERROR) << "unexpected hook calling" << ", client_id=" << client_id_
                   << ", current status="
                   << HealthToString(health_state_.status)
                   << ", expected status="
                   << HealthToString(P2PClientStatus::DISCONNECTION);
        return;
    }
    LOG(INFO) << "the client is disconnected" << ", client_id=" << client_id_;
    DoOnDisconnected();
    MasterMetricManager::instance().dec_active_clients();
    MasterMetricManager::instance().inc_clients_disconnected_total();
}

void ClientMeta::OnRecovered() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    if (health_state_.status != P2PClientStatus::HEALTH) {
        LOG(ERROR) << "unexpected hook calling" << ", client_id=" << client_id_
                   << ", current status="
                   << HealthToString(health_state_.status)
                   << ", expected status="
                   << HealthToString(P2PClientStatus::HEALTH);
        return;
    }
    LOG(INFO) << "the client is recovered" << ", client_id=" << client_id_;
    DoOnRecovered();
    MasterMetricManager::instance().inc_active_clients();
    MasterMetricManager::instance().inc_clients_recovered_total();
}

void ClientMeta::OnCrashed() {
    LOG(INFO) << "the client is crashed" << ", client_id=" << client_id_;
    MasterMetricManager::instance().inc_clients_crashed_total();
    RecycleMeta();
}

void ClientMeta::RecycleMeta() {
    if (recycled_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    LOG(INFO) << "start to recycle client meta" << ", client_id=" << client_id_;
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto segments_res = GetSegmentManager()->GetSegments();
    if (segments_res) {
        for (const auto& seg : *segments_res) {
            auto ret = GetSegmentManager()->UnmountSegment(seg.id);
            if (!ret.has_value()) {
                LOG(ERROR) << "Failed to unmount segment"
                           << ", client_id=" << client_id_
                           << ", segment_id=" << seg.id
                           << " error=" << ret.error();
            }
        }
    }
    LOG(INFO) << "the client meta is recycled over"
              << ", client_id=" << client_id_;
}

std::string ClientMeta::HealthToString(P2PClientStatus status) const {
    switch (status) {
        case P2PClientStatus::HEALTH:
            return "HEALTH";
        case P2PClientStatus::DISCONNECTION:
            return "DISCONNECTION";
        case P2PClientStatus::CRASHED:
            return "CRASHED";
        case P2PClientStatus::UNDEFINED:
            return "UNDEFINED";
    }
    return "UNKNOWN";
}

}  // namespace mooncake