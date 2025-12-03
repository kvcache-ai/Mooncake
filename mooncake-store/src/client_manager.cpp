#include "client_manager.h"
#include "master_metric_manager.h"
#include <glog/logging.h>

namespace mooncake {

ClientManager::ClientManager(const int64_t client_live_ttl_sec) :
    client_live_ttl_sec_(client_live_ttl_sec) {
    // Start client monitor thread in all modes so TTL/heartbeat works
    client_monitor_running_ = true;
    client_monitor_thread_ =
        std::thread(&ClientManager::ClientMonitorFunc, this);
    VLOG(1) << "action=start_client_monitor_thread";
}

ClientManager::~ClientManager() {
    client_monitor_running_ = false;
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
}

ErrorCode ClientManager::MountSegment(const Segment& segment, const UUID& client_id) {
    std::function<ErrorCode()> pre_mount = [this, &client_id, &segment]() -> ErrorCode {
        // Tell the client monitor thread to start timing for this client. To
        // avoid the following undesired situations, this message must be sent
        // after locking the segment mutex and before the mounting operation
        // completes:
        // 1. Sending the message before the lock: the client expires and
        // unmouting invokes before this mounting are completed, which prevents
        // this segment being able to be unmounted forever;
        // 2. Sending the message after mounting the segment: After mounting
        // this segment, when trying to push id to the queue, the queue is
        // already full. However, at this point, the message must be sent,
        // otherwise this client cannot be monitored and expired.
        PodUUID pod_client_id{client_id.first, client_id.second};
        if (!client_ping_queue_.push(pod_client_id)) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=client_ping_queue_full";
            return ErrorCode::INTERNAL_ERROR;
        }
        return ErrorCode::OK;
    };
    auto err = GetSegmentManager()->MountSegment(segment, client_id, pre_mount);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return ErrorCode::OK;
    } else if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to mount segment"
                   << ", segment_name=" << segment.name
                   << ", client_id=" << client_id
                   << ", ret=" << err;
        return err;
    }

    MasterMetricManager::instance().inc_total_mem_capacity(segment.name, segment.size);
    return ErrorCode::OK;
}

ErrorCode ClientManager::ReMountSegment(const std::vector<Segment>& segments,
                                        const UUID& client_id) {
    std::unique_lock<std::shared_mutex> lock(client_mutex_);
    if (ok_client_.contains(client_id)) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", warn=client_already_remounted";
        // Return OK because this is an idempotent operation
        return ErrorCode::OK;
    }

    std::function<ErrorCode()> pre_mount = [this, &client_id]() -> ErrorCode {
        // Tell the client monitor thread to start timing for this client. To
        // avoid the following undesired situations, this message must be sent
        // after locking the segment mutex or client mutex and before the remounting
        // operation completes:
        // 1. Sending the message before the lock: the client expires and
        // unmouting invokes before this remounting are completed, which prevents
        // this segment being able to be unmounted forever;
        // 2. Sending the message after remounting the segments: After remounting
        // these segments, when trying to push id to the queue, the queue is
        // already full. However, at this point, the message must be sent,
        // otherwise this client cannot be monitored and expired.
        PodUUID pod_client_id{client_id.first, client_id.second};
        if (!client_ping_queue_.push(pod_client_id)) {
            LOG(ERROR) << "client_id=" << client_id
                       << ", error=client_ping_queue_full";
            return ErrorCode::INTERNAL_ERROR;
        }
        return ErrorCode::OK;
    };

    ErrorCode err = GetSegmentManager()->ReMountSegment(segments, client_id, pre_mount);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=fail_to_remount_segment"
                   << ", ret=" << err;
        return err;
    }

    // Change the client status to OK
    ok_client_.insert(client_id);
    MasterMetricManager::instance().inc_active_clients();

    return ErrorCode::OK;
}

ErrorCode ClientManager::GetAllSegments(std::vector<std::string>& all_segments) {
    ErrorCode ret = GetSegmentManager()->GetAllSegments(all_segments);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to get all segments"
                   << ", ret=" << ret;
        return ret;
    }
    return ErrorCode::OK;
}

ErrorCode ClientManager::QuerySegments(const std::string& segment,
                                       size_t& used, size_t& capacity) {
    ErrorCode ret = GetSegmentManager()->QuerySegments(segment, used, capacity);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to query segments"
                   << ", ret=" << ret;
        return ret;
    }
    return ErrorCode::OK;
}

ErrorCode ClientManager::QueryIp(const UUID& client_id, std::vector<std::string>& result) {
    ErrorCode ret = GetSegmentManager()->QueryIp(client_id, result);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "fail to query ip"
                   << ", ret=" << ret;
        return ret;
    }
    return ErrorCode::OK;
}
}  // namespace mooncake
