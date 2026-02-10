#include "client_manager.h"
#include "master_metric_manager.h"
#include <glog/logging.h>

namespace mooncake {
ClientManager::ClientManager(const int64_t disconnect_timeout_sec,
                             const int64_t crash_timeout_sec,
                             const ViewVersionId view_version)
    : view_version_(view_version) {
    ClientMeta::SetTimeouts(disconnect_timeout_sec, crash_timeout_sec);
}

void ClientManager::Start() {
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

auto ClientManager::GetClient(const UUID& client_id)
    -> std::shared_ptr<ClientMeta> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        return nullptr;
    }
    return it->second;
}

std::vector<std::shared_ptr<ClientMeta>> ClientManager::GetAllClients() {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    std::vector<std::shared_ptr<ClientMeta>> clients;
    clients.reserve(client_metas_.size());
    for (const auto& [id, meta] : client_metas_) {
        clients.push_back(meta);
    }
    return clients;
}

std::unique_ptr<ClientIterator> ClientManager::InnerBuildClientIterator(
    ObjectIterateStrategy strategy) {
    switch (strategy) {
        case ObjectIterateStrategy::ORDERED:
            return std::make_unique<OrderedClientIterator>(client_metas_);
        case ObjectIterateStrategy::RANDOM:
            return std::make_unique<RandomClientIterator>(client_metas_);
        default:
            return nullptr;
    }
}

auto ClientManager::ForEachClient(ObjectIterateStrategy strategy,
                                  const ClientVisitor& visitor)
    -> tl::expected<void, ErrorCode> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto iterator = InnerBuildClientIterator(strategy);
    if (!iterator) {
        LOG(WARNING) << "fail to get client iterator"
                     << ", strategy=" << strategy;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    while (auto client = iterator->Next()) {
        auto ret = visitor(client);
        if (!ret) {
            LOG(WARNING) << "client visitor returned error"
                         << ", strategy=" << strategy
                         << ", client_id=" << client->get_client_id()
                         << ", ret=" << ret.error();
            return tl::make_unexpected(ret.error());
        }
        if (ret.value()) {  // early stop
            break;
        }
    }
    return {};
}

tl::expected<std::vector<std::string>, ErrorCode>
ClientManager::GetAllSegments() {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    std::vector<std::string> all_segments;
    for (const auto& [id, meta] : client_metas_) {
        auto segments_res = meta->GetSegments();
        if (!segments_res) {
            LOG(WARNING) << "GetAllSegments: failed to get segments"
                         << ", client_id=" << id
                         << ", error=" << segments_res.error();
            continue;
        }
        for (const auto& seg : segments_res.value()) {
            all_segments.emplace_back(std::move(seg.name));
        }
    }
    return all_segments;
}

tl::expected<std::pair<size_t, size_t>, ErrorCode> ClientManager::QuerySegments(
    const std::string& segment) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    for (const auto& [id, meta] : client_metas_) {
        auto ret = meta->QuerySegments(segment);
        if (ret.has_value()) {
            return ret;
        } else if (ret.error() != ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(ERROR)
                << "QuerySegments: failed to query segments for client_id="
                << id << ", error=" << ret.error();
            return ret;
        }
    }
    LOG(WARNING) << "QuerySegments: segment not found"
                 << ", segment=" << segment;
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

tl::expected<std::vector<std::string>, ErrorCode> ClientManager::QueryIp(
    const UUID& client_id) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        LOG(WARNING) << "QueryIp: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return it->second->QueryIp(client_id);
}

tl::expected<std::shared_ptr<Segment>, ErrorCode> ClientManager::QuerySegment(
    const UUID& client_id, const UUID& segment_id) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        LOG(WARNING) << "QuerySegment: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return it->second->QuerySegment(segment_id);
}

void ClientManager::SetSegmentRemovalCallback(SegmentRemovalCallback cb) {
    segment_removal_cb_ = std::move(cb);
}

auto ClientManager::RegisterClient(const RegisterClientRequest& req)
    -> tl::expected<RegisterClientResponse, ErrorCode> {
    const auto& client_id = req.client_id;
    auto it = client_metas_.find(client_id);
    if (it != client_metas_.end()) {
        LOG(WARNING) << "RegisterClient: client already exists"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_ALREADY_EXISTS);
    }

    auto meta = CreateClientMeta(req);
    if (segment_removal_cb_) {
        meta->SetSegmentRemovalCallback(segment_removal_cb_);
    }
    for (const auto& segment : req.segments) {
        auto result = meta->MountSegment(segment);
        if (!result) {
            LOG(ERROR) << "RegisterClient: failed to mount segment"
                       << ", segment_name=" << segment.name
                       << ", client_id=" << client_id
                       << ", error=" << result.error();
            return tl::make_unexpected(result.error());
        }
    }

    SharedMutexLocker lock(&clients_mutex_);
    // Write to client_metas_ (overwrites if re-registering after crash)
    client_metas_[client_id] = std::move(meta);

    MasterMetricManager::instance().inc_active_clients();

    RegisterClientResponse response;
    response.view_version = view_version_;

    LOG(INFO) << "RegisterClient: client_id=" << client_id
              << ", segments=" << req.segments.size()
              << ", view_version=" << response.view_version;

    return response;
}

auto ClientManager::Heartbeat(const HeartbeatRequest& req)
    -> tl::expected<HeartbeatResponse, ErrorCode> {
    const auto& client_id = req.client_id;
    HeartbeatResponse response;
    response.view_version = view_version_;

    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        // Client not in client_metas_: master restarted or client heartbeat
        // timed out and the meta of client was cleaned up. Return UNDEFINED +
        // view_version to inform client to re-register.
        response.status = ClientStatus::UNDEFINED;
        return response;
    }

    auto& meta = it->second;

    // Update Heartbeat
    auto [old_status, new_status] = meta->Heartbeat();
    response.status = new_status;
    if (new_status == ClientStatus::HEALTH) {
        if (old_status != new_status) {
            LOG(INFO) << "client recovered" << ", client_id=" << client_id;
            meta->OnRecovered();
        }
        for (const auto& task : req.tasks) {
            response.task_results.push_back(ProcessTask(client_id, task));
        }
    }

    return response;
}

// Monitor Strategy:
// 1. Phase 1 (Shared Lock): Check health status
// 2. Phase 2 (No Lock): Execute crashed client hooks
// 3. Phase 3 (Write Lock): Clean up crashed clients
void ClientManager::ClientMonitorFunc() {
    while (client_monitor_running_) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));

        // Attention:
        // 1. DISCONNECTED is not finnal status. The clients in
        // newly_disconnected might change its status.
        // 2. CRASHED is finnal status. The clients in newly_crashed will always
        // be crashed.
        std::vector<std::shared_ptr<ClientMeta>> newly_disconnected;
        std::vector<std::shared_ptr<ClientMeta>> newly_crashed;

        // Phase 1: Check health status
        {
            SharedMutexLocker lock(&clients_mutex_, shared_lock);
            for (auto& [client_id, meta] : client_metas_) {
                auto [old_status, new_status] = meta->CheckHealth();
                if (old_status != new_status) {
                    if (new_status == ClientStatus::DISCONNECTION) {
                        newly_disconnected.push_back(meta);
                    } else if (new_status == ClientStatus::CRASHED) {
                        newly_crashed.push_back(meta);
                    }
                }
            }
        }

        // Phase 2: Execute hooks (No client_mutex lock)
        // We can safely execute hooks because we hold shared_ptrs to
        // ClientMeta, so they won't be destroyed.
        // And hooks don't need client_mutex because they are protected by
        // client_meta itself
        for (const auto& client : newly_disconnected) {
            // The client might change to Healthy by concurrent heartbeat.
            // So OnDisconnected() need to check the status again.
            client->OnDisconnected();
        }

        for (const auto& client : newly_crashed) {
            client->OnCrashed();
        }

        // Phase 3: Clean up crashed clients (Write Lock)
        if (!newly_crashed.empty()) {
            SharedMutexLocker lock(&clients_mutex_);
            for (const auto& client : newly_crashed) {
                client_metas_.erase(client->get_client_id());
            }
        }
    }
}

}  // namespace mooncake
