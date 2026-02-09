#include "centralized_client_manager.h"
#include "master_metric_manager.h"

namespace mooncake {
CentralizedClientManager::CentralizedClientManager(
    const int64_t client_live_ttl_sec,
    const BufferAllocatorType memory_allocator_type,
    std::function<void()> segment_clean_func)
    : ClientManager(client_live_ttl_sec),
      segment_clean_func_(segment_clean_func) {
    segment_manager_ =
        std::make_shared<CentralizedSegmentManager>(memory_allocator_type);
}

auto CentralizedClientManager::UnmountSegment(const UUID& segment_id,
                                              const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    size_t metrics_dec_capacity = 0;  // to update the metrics
    std::string segment_name;
    // 1. Prepare to unmount the segment by deleting its allocator
    ErrorCode err = segment_manager_->PrepareUnmountSegment(
        segment_id, metrics_dec_capacity, segment_name);
    if (err == ErrorCode::SEGMENT_NOT_FOUND) {
        LOG(INFO) << "segment_id=" << segment_id << ", client_id=" << client_id
                  << ", error=segment_not_found";
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to prepare unmount segment"
                   << "segment_id=" << segment_id << ", client_id=" << client_id
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }

    // 2. Remove the metadata of the related objects
    segment_clean_func_();

    // 3. Commit the unmount operation
    err = segment_manager_->UnmountSegment(segment_id, client_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to unmount segment"
                   << "segment_id=" << segment_id << ", client_id=" << client_id
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }

    // Decrease the total capacity
    MasterMetricManager::instance().dec_total_mem_capacity(
        segment_name, metrics_dec_capacity);
    return {};
}

auto CentralizedClientManager::MountLocalDiskSegment(const UUID& client_id,
                                                     bool enable_offloading)
    -> tl::expected<void, ErrorCode> {
    auto err =
        segment_manager_->MountLocalDiskSegment(client_id, enable_offloading);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to mount local disk segment"
                   << ", client_id=" << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

auto CentralizedClientManager::InnerMountSegment(
    const Segment& segment, const UUID& client_id,
    std::function<ErrorCode()>& pre_func) -> tl::expected<void, ErrorCode> {
    ErrorCode err =
        segment_manager_->MountSegment(segment, client_id, pre_func);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to mount segment"
                   << ", segment_id=" << segment.id
                   << ", client_id=" << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

auto CentralizedClientManager::InnerReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id,
    std::function<ErrorCode()>& pre_func) -> tl::expected<void, ErrorCode> {
    ErrorCode err =
        segment_manager_->ReMountSegment(segments, client_id, pre_func);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to remount segment"
                   << ", segments.size()=" << segments.size()
                   << ", client_id=" << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

auto CentralizedClientManager::OffloadObjectHeartbeat(const UUID& client_id,
                                                      bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    return segment_manager_->OffloadObjectHeartbeat(client_id,
                                                    enable_offloading);
}

auto CentralizedClientManager::PushOffloadingQueue(
    const std::string& key, const int64_t size, const std::string& segment_name)
    -> tl::expected<void, ErrorCode> {
    ErrorCode err =
        segment_manager_->PushOffloadingQueue(key, size, segment_name);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

auto CentralizedClientManager::Ping(const UUID& client_id)
    -> tl::expected<ClientStatus, ErrorCode> {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    ClientStatus client_status;
    auto it = ok_client_.find(client_id);
    if (it != ok_client_.end()) {
        client_status = ClientStatus::OK;
    } else {
        client_status = ClientStatus::NEED_REMOUNT;
    }
    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return client_status;
}

void CentralizedClientManager::ClientMonitorFunc() {
    std::unordered_map<UUID, std::chrono::steady_clock::time_point,
                       boost::hash<UUID>>
        client_ttl;
    while (client_monitor_running_) {
        auto now = std::chrono::steady_clock::now();

        // Update the client ttl
        PodUUID pod_client_id;
        while (client_ping_queue_.pop(pod_client_id)) {
            UUID client_id = {pod_client_id.first, pod_client_id.second};
            client_ttl[client_id] =
                now + std::chrono::seconds(client_live_ttl_sec_);
        }

        // Find out expired clients
        std::vector<UUID> expired_clients;
        for (auto it = client_ttl.begin(); it != client_ttl.end();) {
            if (it->second < now) {
                LOG(INFO) << "client_id=" << it->first
                          << ", action=client_expired";
                expired_clients.push_back(it->first);
                it = client_ttl.erase(it);
            } else {
                ++it;
            }
        }

        // Update the client status to NEED_REMOUNT
        if (!expired_clients.empty()) {
            // Record which segments are unmounted, will be used in the commit
            // phase.
            std::vector<UUID> unmount_segments;
            std::vector<size_t> dec_capacities;
            std::vector<UUID> client_ids;
            std::vector<std::string> segment_names;
            {
                // Lock client_mutex and segment_mutex
                SharedMutexLocker lock(&client_mutex_);
                for (auto& client_id : expired_clients) {
                    auto it = ok_client_.find(client_id);
                    if (it != ok_client_.end()) {
                        ok_client_.erase(it);
                        MasterMetricManager::instance().dec_active_clients();
                    }
                }

                ErrorCode ret =
                    segment_manager_->BatchPrepareUnmountClientSegments(
                        expired_clients, unmount_segments, dec_capacities,
                        client_ids, segment_names);
                if (ret != ErrorCode::OK) {
                    LOG(ERROR)
                        << "Failed to batch prepare unmount client segments: "
                        << toString(ret);
                }
            }  // Release the mutex before long-running ClearInvalidHandles and
               // avoid deadlocks

            if (!unmount_segments.empty()) {
                segment_clean_func_();
                ErrorCode ret = segment_manager_->BatchUnmountSegments(
                    unmount_segments, client_ids, segment_names);
                if (ret != ErrorCode::OK) {
                    LOG(ERROR) << "Failed to batch unmount segments: "
                               << toString(ret);
                }
                for (size_t i = 0; i < unmount_segments.size(); ++i) {
                    MasterMetricManager::instance().dec_total_mem_capacity(
                        segment_names[i], dec_capacities[i]);
                }
            }
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));
    }  // end while
}

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedClientManager::GetAllSegments() {
    std::vector<std::string> all_segments;
    ErrorCode err = segment_manager_->GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to get all segments" << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return all_segments;
}

tl::expected<std::pair<size_t, size_t>, ErrorCode>
CentralizedClientManager::QuerySegments(const std::string& segment) {
    size_t used = 0;
    size_t capacity = 0;
    ErrorCode err = segment_manager_->QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to query segments" << ", segment=" << segment
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedClientManager::QueryIp(const UUID& client_id) {
    std::vector<std::string> result;
    ErrorCode err = segment_manager_->QueryIp(client_id, result);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to query ip" << ", client_id=" << client_id
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return result;
}

}  // namespace mooncake
