#include "p2p/master/p2p_client_meta.h"

#include <algorithm>
#include <glog/logging.h>
#include <limits>
#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {

int64_t P2PClientMeta::disconnect_timeout_sec_ = 0;
int64_t P2PClientMeta::crash_timeout_sec_ = 0;

P2PClientMeta::P2PClientMeta(const UUID& client_id,
                             const std::string& ip_address, uint16_t rpc_port)
    : client_id_(client_id), ip_address_(ip_address), rpc_port_(rpc_port) {
    health_state_.status = P2PClientStatus::HEALTH;
    health_state_.last_heartbeat = std::chrono::steady_clock::now();
}

P2PClientMeta::~P2PClientMeta() {
    if (registered_) {
        P2PMasterMetricManager::instance().OnClientRemoved(client_id_);
    }
}

tl::expected<void, ErrorCode> P2PClientMeta::MountSegment(
    const P2PSegment& segment) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }

    auto ret = segment_manager_.MountSegment(segment);
    if (!ret.has_value()) {
        if (ret.error() == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            LOG(WARNING) << "attempt to mount segment but it already exists"
                         << ", client_id=" << client_id_
                         << ", segment_id =" << segment.id
                         << ", segment_name=" << segment.name
                         << ", ret=" << ret.error();
            return {};
        }
        LOG(ERROR) << "fail to mount segment"
                   << ", client_id=" << client_id_
                   << ", segment_id =" << segment.id
                   << ", segment_name=" << segment.name
                   << ", ret=" << ret.error();
        return ret;
    }
    LOG(INFO) << "Mount segment success"
              << ", client_id=" << client_id_ << ", segment_id =" << segment.id
              << ", segment_name=" << segment.name;
    return {};
}

tl::expected<void, ErrorCode> P2PClientMeta::UnmountSegment(
    const UUID& segment_id) {
    {
        SharedMutexLocker lock(&client_mutex_, shared_lock);
        auto check_ret = InnerStatusCheck();
        if (!check_ret.has_value()) {
            LOG(ERROR) << "fail to inner check client status"
                       << ", client_id=" << client_id_
                       << ", ret=" << check_ret.error();
            return check_ret;
        }

        auto ret = segment_manager_.UnmountSegment(segment_id);
        if (!ret.has_value()) {
            if (ret.error() == ErrorCode::SEGMENT_NOT_FOUND) {
                LOG(WARNING)
                    << "attempt to unmount segment but it does not exist"
                    << ", client_id=" << client_id_
                    << ", segment_id=" << segment_id << ", ret=" << ret.error();
                return {};
            }
            LOG(ERROR) << "fail to unmount segment"
                       << ", client_id=" << client_id_
                       << ", segment_id=" << segment_id
                       << ", ret=" << ret.error();
            return ret;
        }
    }

    LOG(INFO) << "Unmount segment success"
              << ", client_id=" << client_id_ << ", segment_id =" << segment_id;
    if (segment_removal_cb_) {
        segment_removal_cb_(segment_id);
    }
    return {};
}

tl::expected<std::vector<P2PSegment>, ErrorCode> P2PClientMeta::GetSegments() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_.GetSegments();
}

tl::expected<std::pair<size_t, size_t>, ErrorCode> P2PClientMeta::QuerySegments(
    const std::string& segment_name) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_.QuerySegments(segment_name);
}

tl::expected<P2PSegment, ErrorCode> P2PClientMeta::QuerySegment(
    const UUID& segment_id) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_.QuerySegment(segment_id);
}

tl::expected<void, ErrorCode> P2PClientMeta::CheckSegmentAvailable(
    const UUID& segment_id) const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }
    return segment_manager_.CheckSegmentExists(segment_id);
}

void P2PClientMeta::SetSegmentRemovalCallback(SegmentRemovalCallback cb) {
    segment_removal_cb_ = std::move(cb);
}

void P2PClientMeta::SetTimeouts(int64_t disconnect_sec, int64_t crash_sec) {
    disconnect_timeout_sec_ = disconnect_sec;
    crash_timeout_sec_ = crash_sec;
}

P2PClientHealthState P2PClientMeta::get_health_state() const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return health_state_;
}

bool P2PClientMeta::is_health() const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return !recycled_ && health_state_.status == P2PClientStatus::HEALTH;
}

std::pair<P2PClientStatus, P2PClientStatus> P2PClientMeta::Heartbeat() {
    SharedMutexLocker lock(&client_mutex_);
    InnerUpdateHeartbeat();
    auto transition = InnerUpdateHealthStatus();
    ApplyHealthTransition(transition.first, transition.second);
    return transition;
}

std::pair<P2PClientStatus, P2PClientStatus> P2PClientMeta::CheckHealth() {
    SharedMutexLocker lock(&client_mutex_);
    auto transition = InnerUpdateHealthStatus();
    ApplyHealthTransition(transition.first, transition.second);
    return transition;
}

void P2PClientMeta::InnerUpdateHeartbeat() {
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
P2PClientMeta::InnerUpdateHealthStatus() {
    const auto now = std::chrono::steady_clock::now();
    const P2PClientStatus old_status = health_state_.status;
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - health_state_.last_heartbeat)
            .count();
    const int64_t disconnect_timeout_ms = disconnect_timeout_sec_ * 1000;
    const int64_t crash_timeout_ms = crash_timeout_sec_ * 1000;

    switch (health_state_.status) {
        case P2PClientStatus::HEALTH:
            if (elapsed_ms >= disconnect_timeout_ms) {
                if (elapsed_ms >= crash_timeout_ms) {
                    health_state_.status = P2PClientStatus::CRASHED;
                } else {
                    health_state_.status = P2PClientStatus::DISCONNECTION;
                }
            }
            break;
        case P2PClientStatus::DISCONNECTION:
            if (elapsed_ms < disconnect_timeout_ms) {
                health_state_.status = P2PClientStatus::HEALTH;
            } else if (elapsed_ms >= crash_timeout_ms) {
                health_state_.status = P2PClientStatus::CRASHED;
            }
            break;
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

void P2PClientMeta::ApplyHealthTransition(P2PClientStatus old_status,
                                          P2PClientStatus new_status) {
    if (old_status == new_status || !registered_) {
        return;
    }
    auto& metrics = P2PMasterMetricManager::instance();
    if (old_status == P2PClientStatus::HEALTH &&
        new_status == P2PClientStatus::DISCONNECTION) {
        LOG(INFO) << "the client is disconnected"
                  << ", client_id=" << client_id_;
        metrics.dec_active_clients();
        metrics.inc_clients_disconnected_total();
    } else if (old_status == P2PClientStatus::DISCONNECTION &&
               new_status == P2PClientStatus::HEALTH) {
        LOG(INFO) << "the client is recovered"
                  << ", client_id=" << client_id_;
        metrics.inc_active_clients();
        metrics.inc_clients_recovered_total();
    } else if (new_status == P2PClientStatus::CRASHED) {
        LOG(INFO) << "the client is crashed"
                  << ", client_id=" << client_id_;
        if (old_status == P2PClientStatus::HEALTH) {
            metrics.dec_active_clients();
        }
        metrics.inc_clients_crashed_total();
    } else {
        LOG(WARNING) << "unexpected P2P client health transition"
                     << ", client_id=" << client_id_
                     << ", old_status=" << old_status
                     << ", new_status=" << new_status;
    }
}

tl::expected<void, ErrorCode> P2PClientMeta::InnerStatusCheck() const {
    if (recycled_) {
        LOG(WARNING) << "Client metadata has been recycled"
                     << ", client_id=" << client_id_;
        return tl::make_unexpected(ErrorCode::CLIENT_UNHEALTHY);
    }
    if (health_state_.status != P2PClientStatus::HEALTH) {
        LOG(WARNING) << "Client is not HEALTH"
                     << ", client_id=" << client_id_
                     << ", status=" << HealthToString(health_state_.status);
        return tl::make_unexpected(ErrorCode::CLIENT_UNHEALTHY);
    }
    return {};
}

void P2PClientMeta::RecycleMeta() {
    std::vector<UUID> removed_segments;
    {
        SharedMutexLocker lock(&client_mutex_);
        if (recycled_) {
            return;
        }
        // Fence captured client pointers before taking the segment snapshot.
        // A mount either finishes before this lock or observes recycled_.
        recycled_ = true;
        LOG(INFO) << "start to recycle client meta"
                  << ", client_id=" << client_id_;
        auto segments_res = segment_manager_.GetSegments();
        if (segments_res) {
            removed_segments.reserve(segments_res->size());
            for (const auto& segment : *segments_res) {
                auto ret = segment_manager_.UnmountSegment(segment.id);
                if (!ret.has_value()) {
                    LOG(ERROR) << "Failed to unmount segment"
                               << ", client_id=" << client_id_
                               << ", segment_id=" << segment.id
                               << " error=" << ret.error();
                    continue;
                }
                removed_segments.push_back(segment.id);
            }
        } else {
            LOG(ERROR) << "Failed to list segments during client recycling"
                       << ", client_id=" << client_id_
                       << ", error=" << segments_res.error();
        }
    }
    if (segment_removal_cb_) {
        for (const auto& segment_id : removed_segments) {
            segment_removal_cb_(segment_id);
        }
    }
    LOG(INFO) << "the client meta is recycled over"
              << ", client_id=" << client_id_;
}

std::string P2PClientMeta::HealthToString(P2PClientStatus status) const {
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

tl::expected<std::vector<std::string>, ErrorCode> P2PClientMeta::QueryIp() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return std::vector<std::string>{ip_address_};
}

SyncSegmentMetaResult P2PClientMeta::UpdateSegmentUsages(
    const std::vector<TierUsageInfo>& usages) {
    SyncSegmentMetaResult result;
    for (const auto& usage : usages) {
        SyncSegmentMetaResult::SubResult sub_res;
        sub_res.segment_id = usage.segment_id;

        auto old_usage =
            segment_manager_.UpdateSegmentUsage(usage.segment_id, usage.usage);
        if (!old_usage.has_value()) {
            LOG(WARNING) << "fail to update segment usage"
                         << ", client_id: " << client_id_
                         << ", segment_id: " << usage.segment_id
                         << ", usage: " << usage.usage
                         << ", error: " << old_usage.error();
            sub_res.error = old_usage.error();
            result.sub_results.push_back(sub_res);
            continue;
        }
        sub_res.error = ErrorCode::OK;
        result.sub_results.push_back(sub_res);
    }
    return result;
}

size_t P2PClientMeta::GetAvailableCapacity() const {
    const auto [capacity, usage] = segment_manager_.GetCapacityUsage();
    return capacity > usage ? capacity - usage : 0;
}

P2PClientMeta::CapacityStat P2PClientMeta::GetWriteScoreCapacity(
    const std::vector<std::string>& tag_filters, int priority_limit,
    bool top_tier_only) const {
    // A segment is eligible for scoring if it carries no filtered tag and its
    // priority is >= priority_limit.
    auto eligible = [&](const P2PSegment& segment) -> bool {
        if (segment.priority < priority_limit) return false;
        for (const auto& tag : tag_filters) {
            if (std::find(segment.tags.begin(), segment.tags.end(), tag) !=
                segment.tags.end()) {
                return false;
            }
        }
        return true;
    };

    CapacityStat all, top;
    int max_priority = std::numeric_limits<int>::min();
    segment_manager_.ForEachSegment([&](const P2PSegment& seg) -> bool {
        if (!eligible(seg)) return false;
        const size_t free = seg.size > seg.usage ? seg.size - seg.usage : 0;
        all.total += seg.size;
        all.free += free;
        if (seg.priority > max_priority) {
            max_priority = seg.priority;
            top = {free, seg.size};
        } else if (seg.priority == max_priority) {
            top.total += seg.size;
            top.free += free;
        }
        return false;  // always continue to next segment
    });
    return top_tier_only ? top : all;
}

std::optional<P2PWriteCandidate> P2PClientMeta::GetWriteRouteCandidate(
    const P2PWriteRouteConfig& config) const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    if (recycled_) {
        return std::nullopt;
    }
    const CapacityStat capacity = GetWriteScoreCapacity(
        config.tag_filters, config.priority_limit, config.top_tier_only);
    if (capacity.total == 0) {
        return std::nullopt;
    }
    return P2PWriteCandidate{
        .client_id = client_id_,
        .ip_address = ip_address_,
        .rpc_port = rpc_port_,
        .available_capacity = capacity.free,
        .score = static_cast<double>(capacity.free) / capacity.total,
    };
}

}  // namespace mooncake
