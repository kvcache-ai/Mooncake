#include "p2p/master/p2p_client_manager.h"

#include <algorithm>
#include <chrono>
#include <glog/logging.h>
#include <random>
#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {

P2PClientManager::P2PClientManager(int64_t disconnect_timeout_sec,
                                   int64_t crash_timeout_sec,
                                   ViewVersionId view_version)
    : view_version_(view_version) {
    P2PClientMeta::SetTimeouts(disconnect_timeout_sec, crash_timeout_sec);
}

P2PClientManager::~P2PClientManager() { Stop(); }

void P2PClientManager::Start() { StartClientMonitor(); }

void P2PClientManager::StartClientMonitor() {
    client_monitor_thread_ = std::jthread([this](std::stop_token stop_token) {
        while (!stop_token.stop_requested()) {
            constexpr auto kPollInterval = std::chrono::milliseconds(10);
            uint64_t waited_ms = 0;
            while (!stop_token.stop_requested() &&
                   waited_ms < kClientMonitorSleepMs) {
                std::this_thread::sleep_for(kPollInterval);
                waited_ms += kPollInterval.count();
            }
            if (!stop_token.stop_requested()) {
                ClientMonitorFunc();
            }
        }
    });
    VLOG(1) << "action=start_client_monitor_thread";
}

void P2PClientManager::Stop() { StopClientMonitor(); }

void P2PClientManager::StopClientMonitor() {
    if (!client_monitor_thread_.joinable()) {
        return;
    }
    client_monitor_thread_.request_stop();
    client_monitor_thread_.join();
}

auto P2PClientManager::GetClient(const UUID& client_id)
    -> std::shared_ptr<P2PClientMeta> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        return nullptr;
    }
    return it->second;
}

std::vector<std::shared_ptr<P2PClientMeta>> P2PClientManager::GetAllClients() {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    std::vector<std::shared_ptr<P2PClientMeta>> clients;
    clients.reserve(client_metas_.size());
    for (const auto& [id, meta] : client_metas_) {
        clients.push_back(meta);
    }
    return clients;
}

auto P2PClientManager::BuildClientList(
    P2PClientSelectionStrategy strategy) const
    -> std::optional<std::vector<std::shared_ptr<P2PClientMeta>>> {
    std::vector<std::shared_ptr<P2PClientMeta>> clients;
    clients.reserve(client_metas_.size());

    switch (strategy) {
        case P2PClientSelectionStrategy::ORDERED: {
            for (const auto& [id, meta] : client_metas_) {
                clients.emplace_back(meta);
            }
            break;
        }
        case P2PClientSelectionStrategy::RANDOM: {
            for (const auto& [id, meta] : client_metas_) {
                clients.emplace_back(meta);
            }
            std::random_device rd;
            std::mt19937 generator(rd());
            std::shuffle(clients.begin(), clients.end(), generator);
            break;
        }
        case P2PClientSelectionStrategy::CAPACITY_PRIORITY: {
            std::vector<std::pair<size_t, std::shared_ptr<P2PClientMeta>>>
                clients_with_capacity;
            clients_with_capacity.reserve(client_metas_.size());
            for (const auto& [id, meta] : client_metas_) {
                clients_with_capacity.emplace_back(meta->GetAvailableCapacity(),
                                                   meta);
            }
            std::sort(clients_with_capacity.begin(),
                      clients_with_capacity.end(),
                      [](const auto& lhs, const auto& rhs) {
                          return lhs.first > rhs.first;
                      });
            for (auto& [capacity, client] : clients_with_capacity) {
                clients.emplace_back(std::move(client));
            }
            break;
        }
        default:
            return std::nullopt;
    }
    return clients;
}

auto P2PClientManager::ForEachClient(P2PClientSelectionStrategy strategy,
                                     const ClientVisitor& visitor)
    -> tl::expected<void, ErrorCode> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto clients = BuildClientList(strategy);
    if (!clients) {
        LOG(WARNING) << "fail to get client iterator"
                     << ", strategy=" << strategy;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    for (const auto& client : *clients) {
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
P2PClientManager::GetAllSegments() {
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
        for (const auto& segment : segments_res.value()) {
            all_segments.emplace_back(segment.name);
        }
    }
    return all_segments;
}

tl::expected<std::vector<std::string>, ErrorCode>
P2PClientManager::GetClientSegments(const UUID& client_id) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        LOG(WARNING) << "GetClientSegments: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto segments_res = it->second->GetSegments();
    if (!segments_res) {
        LOG(WARNING) << "GetClientSegments: failed to get segments"
                     << ", client_id=" << client_id
                     << ", error=" << segments_res.error();
        return tl::make_unexpected(segments_res.error());
    }
    std::vector<std::string> segment_names;
    segment_names.reserve(segments_res.value().size());
    for (const auto& segment : segments_res.value()) {
        segment_names.emplace_back(segment.name);
    }
    return segment_names;
}

tl::expected<std::pair<size_t, size_t>, ErrorCode>
P2PClientManager::QuerySegments(const std::string& segment) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    for (const auto& [id, meta] : client_metas_) {
        auto ret = meta->QuerySegments(segment);
        if (ret.has_value()) {
            return ret;
        }
        if (ret.error() != ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(ERROR) << "QuerySegments: failed to query segments for "
                          "client_id="
                       << id << ", error=" << ret.error();
            return ret;
        }
    }
    LOG(WARNING) << "QuerySegments: segment not found"
                 << ", segment=" << segment;
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

// TODO: wanyue-wy
// To ensure the compatibility of the code,
// currently we assume that segment_name is globally unique among all clients.
// However, the actual code does not guarantee this premise
// In the future, we need to impose constraints on this premise,
// or replace segment name with segment id
tl::expected<UUID, ErrorCode> P2PClientManager::GetClientIdBySegmentName(
    const std::string& segment_name) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    for (const auto& [client_id, meta] : client_metas_) {
        auto segments = meta->GetSegments();
        if (!segments.has_value()) {
            continue;
        }
        for (const auto& segment : segments.value()) {
            if (segment.name == segment_name) {
                return client_id;
            }
        }
    }
    LOG(WARNING) << "GetClientIdBySegmentName: segment not found"
                 << ", segment_name=" << segment_name;
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

tl::expected<std::vector<std::string>, ErrorCode> P2PClientManager::QueryIp(
    const UUID& client_id) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        LOG(WARNING) << "QueryIp: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return it->second->QueryIp();
}

tl::expected<P2PSegment, ErrorCode> P2PClientManager::QuerySegment(
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

void P2PClientManager::SetSegmentRemovalCallback(SegmentRemovalCallback cb) {
    segment_removal_cb_ = std::move(cb);
}

auto P2PClientManager::RegisterClient(const P2PRegisterClientRequest& req)
    -> tl::expected<ViewVersionId, ErrorCode> {
    const auto& client_id = req.client_id;
    {
        SharedMutexLocker lock(&clients_mutex_, shared_lock);
        auto it = client_metas_.find(client_id);
        if (it != client_metas_.end()) {
            LOG(WARNING) << "RegisterClient: client already exists"
                         << ", client_id=" << client_id;
            return tl::make_unexpected(ErrorCode::CLIENT_ALREADY_EXISTS);
        }
    }

    LOG(INFO) << "RegisterClient(P2P): client_id=" << req.client_id
              << ", ip_address='" << req.ip_address
              << "', rpc_port=" << req.rpc_port
              << ", segments=" << req.segments.size();
    if (req.ip_address.empty()) {
        LOG(ERROR) << "RegisterClient(P2P): rejected, empty ip_address"
                   << ", client_id=" << req.client_id;
        LOG(WARNING) << "RegisterClient: register request failed"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (req.rpc_port == 0) {
        LOG(ERROR) << "RegisterClient(P2P): rejected, invalid rpc_port=0"
                   << ", client_id=" << req.client_id;
        LOG(WARNING) << "RegisterClient: register request failed"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto meta = std::make_shared<P2PClientMeta>(req.client_id, req.ip_address,
                                                req.rpc_port);
    for (const auto& segment : req.segments) {
        auto result = meta->MountSegment(segment);
        if (!result) {
            LOG(ERROR) << "RegisterClient: failed to mount segment"
                       << ", segment_name=" << segment.name
                       << ", client_id=" << client_id
                       << ", error=" << result.error();
            meta->RecycleMeta();
            return tl::make_unexpected(result.error());
        }
    }
    meta->SetSyncing(true);

    bool inserted = false;
    {
        SharedMutexLocker lock(&clients_mutex_);
        auto [it, was_inserted] = client_metas_.emplace(client_id, meta);
        inserted = was_inserted;
        if (inserted) {
            if (segment_removal_cb_) {
                meta->SetSegmentRemovalCallback(
                    [callback = segment_removal_cb_, client_id](
                        const UUID& segment_id) {
                        callback(P2PRouteLocation{.client_id = client_id,
                                                  .segment_id = segment_id});
                    });
            }
            meta->MarkRegistered();
            P2PMasterMetricManager::instance().inc_active_clients();
        }
    }
    if (!inserted) {
        LOG(WARNING)
            << "RegisterClient: client already exists (lost registration race)"
            << ", client_id=" << client_id;
        meta->RecycleMeta();
        return tl::make_unexpected(ErrorCode::CLIENT_ALREADY_EXISTS);
    }

    LOG(INFO) << "RegisterClient: client_id=" << client_id
              << ", segments=" << req.segments.size()
              << ", view_version=" << view_version_;
    return view_version_;
}

auto P2PClientManager::UnregisterClient(const UUID& client_id)
    -> tl::expected<ViewVersionId, ErrorCode> {
    std::shared_ptr<P2PClientMeta> meta;
    bool was_health = false;
    {
        SharedMutexLocker lock(&clients_mutex_);
        auto it = client_metas_.find(client_id);
        if (it == client_metas_.end()) {
            // Idempotent: already absent (crashed-out or double unregister).
            LOG(INFO) << "UnregisterClient: client not found (idempotent ok)"
                      << ", client_id=" << client_id;
            return view_version_;
        }
        meta = std::move(it->second);
        was_health = meta->get_health_state().status == P2PClientStatus::HEALTH;
        client_metas_.erase(it);
    }

    // TODO(C3): Define cleanup completion before reusing a client/segment
    // identity; an old removal callback can otherwise erase newly published
    // routes with the same identity, even when same-key values are immutable.
    // The client is out of client_metas_ now. Recycle its segments WITHOUT
    // crash accounting (this is a proactive unregister, not a crash).
    meta->RecycleMeta();
    // Decrement the active gauge only if the client was still HEALTH: the
    // unhealthy path already decremented it during the status transition.
    if (was_health) {
        P2PMasterMetricManager::instance().dec_active_clients();
    }
    LOG(INFO) << "UnregisterClient: client_id=" << client_id
              << ", was_health=" << was_health;
    return view_version_;
}

auto P2PClientManager::Heartbeat(const P2PHeartbeatRequest& req)
    -> tl::expected<P2PHeartbeatResponse, ErrorCode> {
    const auto& client_id = req.client_id;
    P2PHeartbeatResponse response;
    response.view_version = view_version_;

    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        // Client not in client_metas_: master restarted or client heartbeat
        // timed out and the meta of client was cleaned up. Return UNDEFINED +
        // view_version to inform client to re-register.
        response.status = P2PClientStatus::UNDEFINED;
        return response;
    }

    auto& meta = it->second;
    // Update Heartbeat
    auto [old_status, new_status] = meta->Heartbeat();
    response.status = new_status;
    if (new_status == P2PClientStatus::HEALTH) {
        response.task_results.reserve(req.tasks.size());
        for (const auto& task : req.tasks) {
            response.task_results.push_back(ProcessTask(meta, task));
        }
    }
    return response;
}

auto P2PClientManager::QueryClientStatus(const UUID& client_id)
    -> tl::expected<P2PClientStatus, ErrorCode> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    return it == client_metas_.end() ? P2PClientStatus::UNDEFINED
                                     : it->second->get_health_state().status;
}

HeartbeatTaskResult P2PClientManager::ProcessTask(
    const std::shared_ptr<P2PClientMeta>& client_meta,
    const HeartbeatTask& task) {
    HeartbeatTaskResult result;
    result.type = task.type_;
    switch (task.type_) {
        case HeartbeatTaskType::SYNC_SEGMENT_META: {
            const auto* param = std::get_if<SyncSegmentMetaParam>(&task.param_);
            if (!param) {
                result.error = ErrorCode::INVALID_PARAMS;
                LOG(ERROR) << "SYNC_SEGMENT_META: invalid param"
                           << ", client_id=" << client_meta->get_client_id();
                break;
            }
            auto sync_res =
                client_meta->UpdateSegmentUsages(param->tier_usages);
            result.detail = sync_res;
            for (const auto& sub : sync_res.sub_results) {
                if (sub.error != ErrorCode::OK) {
                    // result.error means the task is failed.
                    // here just sub task error, don't affect task result.
                    LOG(ERROR) << "fail to update segment usages"
                               << ", client_id=" << client_meta->get_client_id()
                               << ", segment_id=" << sub.segment_id
                               << ", error=" << sub.error;
                }
            }
            break;
        }
        case HeartbeatTaskType::SYNC_CLIENT_METRIC: {
            const auto* param =
                std::get_if<SyncClientMetricParam>(&task.param_);
            if (!param) {
                result.error = ErrorCode::INVALID_PARAMS;
                LOG(ERROR) << "SYNC_CLIENT_METRIC: invalid param"
                           << ", client_id=" << client_meta->get_client_id();
                break;
            }
            P2PMasterMetricManager::instance().UpdateClientMetrics(
                client_meta->get_client_id(), param->snapshot);
            break;
        }
        default:
            result.error = ErrorCode::NOT_IMPLEMENTED;
            LOG(WARNING) << "unsupported heartbeat task"
                         << ", client_id=" << client_meta->get_client_id()
                         << ", task_type=" << static_cast<int>(task.type_);
            break;
    }
    return result;
}

// 1. Phase 1 (Shared Lock): Check health status
// 2. Phase 2 (No Lock): Recycle crashed clients
// 3. Phase 3 (Write Lock): Clean up crashed clients
void P2PClientManager::ClientMonitorFunc() {
    // Attention:
    // 1. DISCONNECTION is not final status. A concurrent heartbeat may recover
    // the client after this health check.
    // 2. CRASHED is final status. The clients in newly_crashed will remain
    // crashed.
    std::vector<std::shared_ptr<P2PClientMeta>> newly_crashed;

    // Phase 1: Check health status.
    {
        SharedMutexLocker lock(&clients_mutex_, shared_lock);
        for (const auto& [client_id, meta] : client_metas_) {
            auto [old_status, new_status] = meta->CheckHealth();
            if (old_status != new_status &&
                new_status == P2PClientStatus::CRASHED) {
                newly_crashed.push_back(meta);
            }
        }
    }

    // Phase 2: Recycle segments without clients_mutex_. The shared_ptr keeps
    // the client alive while callbacks remove its routes.
    for (const auto& meta : newly_crashed) {
        meta->RecycleMeta();
    }

    // Phase 3: Remove only the same crashed object observed in phase 1. This
    // prevents an old monitor result from deleting a concurrently re-registered
    // client with the same UUID.
    if (!newly_crashed.empty()) {
        SharedMutexLocker lock(&clients_mutex_);
        for (const auto& meta : newly_crashed) {
            auto it = client_metas_.find(meta->get_client_id());
            if (it != client_metas_.end() && it->second == meta) {
                client_metas_.erase(it);
            }
        }
    }
}

}  // namespace mooncake
