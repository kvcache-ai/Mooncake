#include "client_manager.h"
#include "master_metric_manager.h"
#include <glog/logging.h>

namespace mooncake {

ClientManager::ClientManager(const int64_t client_live_ttl_sec)
    : client_live_ttl_sec_(client_live_ttl_sec) {
    // Thread will be started by Start() after object is fully constructed
}

void ClientManager::Start() {
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

auto ClientManager::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::function<ErrorCode()> pre_mount = [this, &client_id,
                                            &segment]() -> ErrorCode {
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
    auto err = InnerMountSegment(segment, client_id, pre_mount);
    if (!err.has_value()) {
        if (err.error() == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            // Return OK because this is an idempotent operation
            return {};
        } else {
            LOG(ERROR) << "fail to mount segment"
                       << ", segment_name=" << segment.name
                       << ", client_id=" << client_id
                       << ", ret=" << err.error();
            return err;
        }
    }

    MasterMetricManager::instance().inc_total_mem_capacity(segment.name,
                                                           segment.size);
    return {};
}

auto ClientManager::ReMountSegment(const std::vector<Segment>& segments,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    SharedMutexLocker lock(&client_mutex_);
    if (ok_client_.contains(client_id)) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", warn=client_already_remounted";
        // Return OK because this is an idempotent operation
        return {};
    }

    std::function<ErrorCode()> pre_mount = [this, &client_id]() -> ErrorCode {
        // Tell the client monitor thread to start timing for this client. To
        // avoid the following undesired situations, this message must be sent
        // after locking the segment mutex or client mutex and before the
        // remounting operation completes:
        // 1. Sending the message before the lock: the client expires and
        // unmouting invokes before this remounting are completed, which
        // prevents this segment being able to be unmounted forever;
        // 2. Sending the message after remounting the segments: After
        // remounting these segments, when trying to push id to the queue, the
        // queue is already full. However, at this point, the message must be
        // sent, otherwise this client cannot be monitored and expired.
        PodUUID pod_client_id{client_id.first, client_id.second};
        if (!client_ping_queue_.push(pod_client_id)) {
            LOG(ERROR) << "client_id=" << client_id
                       << ", error=client_ping_queue_full";
            return ErrorCode::INTERNAL_ERROR;
        }
        return ErrorCode::OK;
    };

    auto ret = InnerReMountSegment(segments, client_id, pre_mount);
    if (!ret.has_value()) {
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=fail_to_remount_segment"
                   << ", ret=" << ret.error();
        return ret;
    }

    // Change the client status to OK
    ok_client_.insert(client_id);
    MasterMetricManager::instance().inc_active_clients();

    return {};
}

}  // namespace mooncake
