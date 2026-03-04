#include "client_meta.h"
#include <glog/logging.h>
#include "master_metric_manager.h"

namespace mooncake {

// Define static timeout members
int64_t ClientMeta::disconnect_timeout_sec_ = 0;
int64_t ClientMeta::crash_timeout_sec_ = 0;

ClientMeta::ClientMeta(const UUID& client_id) : client_id_(client_id) {
    health_state_.status = ClientStatus::HEALTH;
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
            return {};  // ignore the errcode
        } else {
            LOG(ERROR) << "fail to mount segment"
                       << ", client_id=" << client_id_
                       << ", segment_id =" << segment.id
                       << ", segment_name=" << segment.name
                       << ", ret=" << ret.error();
            return ret;
        }
    }
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
            return {};  // ignore the errcode
        } else {
            LOG(ERROR) << "fail to unmount segment"
                       << ", client_id=" << client_id_
                       << ", segment_id=" << segment_id
                       << ", ret=" << ret.error();
            return ret;
        }
    }
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
    return health_state_.status == ClientStatus::HEALTH;
}

std::pair<ClientStatus, ClientStatus> ClientMeta::Heartbeat() {
    SharedMutexLocker lock(&client_mutex_);
    InnerUpdateHeartbeat();
    return InnerUpdateHealthStatus();
}

std::pair<ClientStatus, ClientStatus> ClientMeta::CheckHealth() {
    SharedMutexLocker lock(&client_mutex_);
    return InnerUpdateHealthStatus();
}

void ClientMeta::InnerUpdateHeartbeat() {
    if (health_state_.status == ClientStatus::CRASHED) {
        LOG(WARNING) << "heartbeat received while in CRASHED state, "
                        "timestamp will not update"
                     << ", client_id=" << client_id_;
        return;
    } else if (health_state_.status == ClientStatus::DISCONNECTION) {
        LOG(WARNING) << "heartbeat received while in DISCONNECTION state, "
                        "the state might change to HEALTH as soon as possible"
                     << ", client_id=" << client_id_;
    }
    health_state_.last_heartbeat = std::chrono::steady_clock::now();
}

std::pair<ClientStatus, ClientStatus> ClientMeta::InnerUpdateHealthStatus() {
    auto now = std::chrono::steady_clock::now();
    ClientStatus old_status = health_state_.status;

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       now - health_state_.last_heartbeat)
                       .count();

    switch (health_state_.status) {
        case ClientStatus::HEALTH: {
            if (elapsed > disconnect_timeout_sec_) {
                if (elapsed > crash_timeout_sec_) {
                    health_state_.status = ClientStatus::CRASHED;
                } else {
                    health_state_.status = ClientStatus::DISCONNECTION;
                }
            }
            break;
        }
        case ClientStatus::DISCONNECTION: {
            if (elapsed <= disconnect_timeout_sec_) {
                health_state_.status = ClientStatus::HEALTH;
            } else if (elapsed > crash_timeout_sec_) {
                health_state_.status = ClientStatus::CRASHED;
            }
            break;
        }
        case ClientStatus::CRASHED:
        case ClientStatus::UNDEFINED:
            // final states, do nothing
            break;
    }
    if (health_state_.status != old_status) {
        // client status changed
        LOG(INFO) << "Client status changed"
                  << ", client_id=" << client_id_
                  << ", old_status=" << HealthToString(old_status)
                  << ", new_status=" << HealthToString(health_state_.status);
    }
    return {old_status, health_state_.status};
}

tl::expected<void, ErrorCode> ClientMeta::InnerStatusCheck() const {
    if (health_state_.status != ClientStatus::HEALTH) {
        LOG(WARNING) << "Client is not HEALTH"
                     << ", client_id=" << client_id_
                     << ", status=" << HealthToString(health_state_.status);
        return tl::make_unexpected(ErrorCode::CLIENT_UNHEALTHY);
    }
    return {};
}

void ClientMeta::OnDisconnected() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    if (health_state_.status == ClientStatus::HEALTH) {
        // concurrent heartbeat might have recovered the client, skip
        return;
    } else if (health_state_.status != ClientStatus::DISCONNECTION) {
        LOG(ERROR) << "unexpected hook calling" << ", client_id=" << client_id_
                   << ", current status="
                   << HealthToString(health_state_.status)
                   << ", expected status="
                   << HealthToString(ClientStatus::DISCONNECTION);
        return;
    }
    LOG(INFO) << "the client is disconnected" << ", client_id=" << client_id_;
    DoOnDisconnected();
    MasterMetricManager::instance().dec_active_clients();
}

void ClientMeta::OnRecovered() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    if (health_state_.status != ClientStatus::HEALTH) {
        LOG(ERROR) << "unexpected hook calling" << ", client_id=" << client_id_
                   << ", current status="
                   << HealthToString(health_state_.status)
                   << ", expected status="
                   << HealthToString(ClientStatus::HEALTH);
        return;
    }
    LOG(INFO) << "the client is recovered" << ", client_id=" << client_id_;
    DoOnRecovered();
    MasterMetricManager::instance().inc_active_clients();
}

void ClientMeta::OnCrashed() {
    LOG(INFO) << "the client is crashed, start to recycle meta"
              << ", client_id=" << client_id_;
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

std::string ClientMeta::HealthToString(ClientStatus status) const {
    switch (status) {
        case ClientStatus::HEALTH:
            return "HEALTH";
        case ClientStatus::DISCONNECTION:
            return "DISCONNECTION";
        case ClientStatus::CRASHED:
            return "CRASHED";
        case ClientStatus::UNDEFINED:
            return "UNDEFINED";
    }
    return "UNKNOWN";
}
}  // namespace mooncake
