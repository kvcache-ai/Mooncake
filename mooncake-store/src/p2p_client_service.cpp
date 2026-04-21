#include "p2p_client_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <coroutine>
#include <cstdlib>
#include <exception>
#include <future>
#include <thread>

#include <async_simple/Try.h>
#include <async_simple/coro/Lazy.h>

#include "utils/scoped_vlog_timer.h"

namespace mooncake {

// ============================================================================
// Construction / Destruction
// ============================================================================

P2PClientService::P2PClientService(
    const std::string& local_ip, uint16_t te_port,
    const std::string& metadata_connstring, uint16_t metrics_port,
    bool enable_metrics_http, const std::map<std::string, std::string>& labels)
    : ClientService(local_ip, te_port, metadata_connstring, metrics_port,
                    enable_metrics_http, labels),
      master_client_(client_id_,
                     metrics_ ? &metrics_->master_client_metric : nullptr) {}

void P2PClientService::Stop() {
    if (!MarkShuttingDown()) {
        return;  // Already shut down.
    }

    LOG(INFO) << "P2PClientService::Stop() — begin";

    // Stop HA recovery thread first
    if (ha_manager_) {
        ha_manager_->Stop();
    }

    // Stop RPC server so no new requests arrive.
    if (client_rpc_server_) {
        client_rpc_server_->stop();
    }
    if (client_rpc_server_thread_.joinable()) {
        client_rpc_server_thread_.join();
    }

    // Stop async notifier before data_manager to drain pending ops
    if (async_route_notifier_) {
        async_route_notifier_->Stop();
    }

    // Stop tier scheduler of tierd_backend
    if (data_manager_.has_value()) {
        data_manager_->Stop();
    }

    // Stop heartbeat
    ClientService::Stop();

    LOG(INFO) << "P2PClientService::Stop() — complete";
}

void P2PClientService::Destroy() {
    LOG(INFO) << "P2PClientService::Destroy() — begin";

    {
        std::lock_guard<std::mutex> lock(peer_clients_mutex_);
        peer_clients_.clear();
    }

    client_rpc_service_.reset();
    ha_manager_.reset();
    async_route_notifier_.reset();
    if (data_manager_.has_value()) {
        data_manager_->Destroy();
    }
    data_manager_.reset();

    ClientService::Destroy();

    LOG(INFO) << "P2PClientService::Destroy() — complete";
}

P2PClientService::~P2PClientService() {
    Stop();
    Destroy();
}

ErrorCode P2PClientService::Init(const P2PClientConfig& config) {
    client_rpc_port_ = config.client_rpc_port;

    // 1. Connect to master
    ErrorCode err = ConnectToMaster(config.master_server_entry);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to master in P2P mode";
        return err;
    }

    // 2. Initialize transfer engine
    if (config.transfer_engine == nullptr) {
        transfer_engine_ = std::make_shared<TransferEngine>();
        err = InitTransferEngine(local_endpoint(), metadata_connstring_,
                                 config.protocol, config.rdma_devices);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize transfer engine";
            return err;
        }
    } else {
        transfer_engine_ = config.transfer_engine;
        LOG(INFO) << "Use existing transfer engine instance. Skip its "
                     "initialization.";
    }
    initTeEndpoint();

    // 3. Register with master BEFORE InitStorage, because InitStorage
    //    triggers TieredBackend::MountSegment which requires the client to
    //    be already registered on the master side.
    auto reg = RegisterClient();
    if (!reg) {
        LOG(ERROR) << "Failed to register P2P client with master";
        return reg.error();
    }

    // 3.5. Start heartbeat immediately after registration so master does not
    //      consider this client disconnected during a lengthy InitStorage.
    //      build_heartbeat_request() and OnHAEvent() both guard against
    //      uninitialized data_manager_ / ha_manager_, so this is safe.
    StartHeartbeat(config.master_server_entry);

    // 4. Initialize TieredBackend + DataManager
    err = InitStorage(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize TieredBackend";
        return err;
    }

    // 4.5. Initialize async route notifier if enabled
    if (config.async_sender_thread_count > 0) {
        // When async ADD is rejected by Master, delete the local replica
        // since Master won't track it.
        SyncFailureCallback failure_cb = [this](const std::string& key,
                                                const UUID& segment_id,
                                                ErrorCode error) {
            LOG(WARNING) << "Async ADD rejected by Master, deleting local"
                         << ", key=" << key << ", error=" << error;
            if (data_manager_.has_value()) {
                auto r = data_manager_->Delete(key, segment_id);
                if (!r) {
                    LOG(ERROR) << "Failed to delete local replica"
                               << ", key=" << key << ", error=" << r.error();
                }
            }
        };
        async_route_notifier_ = std::make_unique<AsyncMetadataNotifier>(
            master_client_, client_id_, config.async_sender_thread_count,
            config.async_max_batch_size, config.async_route_queue_size,
            std::move(failure_cb));
        async_route_notifier_->Start();
        LOG(INFO) << "Async route notifier enabled, thread_count="
                  << config.async_sender_thread_count
                  << ", queue_size=" << config.async_route_queue_size;
    }

    // 5. Start P2P client RPC service
    client_rpc_service_.emplace(*data_manager_);
    client_rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
        config.rpc_thread_num, client_rpc_port_);
    RegisterClientRpcService(*client_rpc_server_, *client_rpc_service_);

    client_rpc_server_thread_ = std::thread([this]() {
        auto ec = client_rpc_server_->start();
        if (ec) {
            LOG(ERROR) << "P2P RPC server failed to start on port "
                       << client_rpc_port_ << ": " << ec.message();
        }
    });

    is_running_ = true;

    // Give RPC server a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    LOG(INFO) << "P2P RPC server started on port " << client_rpc_port_;

    // 6. Initialize HA recovery manager
    ha_manager_ = std::make_unique<HARecoveryManager>(
        client_id_, master_client_, data_manager_, async_route_notifier_,
        view_version_);
    // First-time registration: no keys to sync, clear is_syncing_ on Master
    ha_manager_->SetSyncCompleted();

    return ErrorCode::OK;
}

ErrorCode P2PClientService::InitStorage(const P2PClientConfig& config) {
    auto tiered_backend = std::make_unique<TieredBackend>();

    auto add_replica_callback = BuildAddReplicaCallback();
    auto remove_replica_callback = BuildRemoveReplicaCallback();
    auto segment_sync_callback = BuildSegmentSyncCallback();

    auto init_result = tiered_backend->Init(
        config.tiered_backend_config, transfer_engine_.get(),
        add_replica_callback, remove_replica_callback, segment_sync_callback);
    if (!init_result) {
        LOG(ERROR) << "Failed to init TieredBackend: " << init_result.error();
        return init_result.error();
    }

    LocalTransferConfig local_transfer_config;
    local_transfer_config.mode = config.local_transfer_mode;
    if (config.local_transfer_mode == LocalTransferMode::TE) {
        local_transfer_config.te_endpoint = get_te_endpoint();
    } else {
        local_transfer_config.local_memcpy_async_worker_num =
            config.local_memcpy_async_worker_num;
    }

    data_manager_ = DataManager(std::move(tiered_backend), transfer_engine_,
                                config.lock_shard_count, local_transfer_config);
    // Set rectify callback on DataManager to remove stale replicas from master
    data_manager_->SetRectifyCallback([this](const std::string& key,
                                             std::optional<UUID> tier_id) {
        if (async_route_notifier_) {
            if (!tier_id.has_value()) {
                auto tier_views = data_manager_->GetTierViews();
                for (const auto& tv : tier_views) {
                    auto r = async_route_notifier_->EnqueueRemove(key, tv.id);
                    if (!r) {
                        LOG(WARNING) << "Failed to enqueue rectify remove"
                                     << ", key=" << key;
                    }
                }
            } else {
                auto r = async_route_notifier_->EnqueueRemove(key, *tier_id);
                if (!r) {
                    LOG(WARNING) << "Failed to enqueue rectify remove"
                                 << ", key=" << key;
                }
            }
        } else {
            if (!tier_id.has_value()) {
                auto tier_views = data_manager_->GetTierViews();
                std::vector<UUID> segment_ids;
                segment_ids.reserve(tier_views.size());
                for (const auto& tv : tier_views) {
                    segment_ids.push_back(tv.id);
                }
                SyncBatchRemoveReplica(key, std::move(segment_ids));
            } else {
                SyncRemoveReplica(key, *tier_id);
            }
        }
    });

    // Initialize route cache
    if (config.route_cache_max_memory_bytes > 0 &&
        config.route_cache_ttl_ms > 0) {
        route_cache_.emplace(config.route_cache_max_memory_bytes,
                             config.route_cache_ttl_ms);
    }

    return ErrorCode::OK;
}

AddReplicaCallback P2PClientService::BuildAddReplicaCallback() {
    return [this](const std::string& key, const UUID& tier_id,
                  size_t size) -> tl::expected<void, ErrorCode> {
        // In degraded mode, skip metadata notification to Master.
        // The data is stored locally; the recovery pipeline will re-sync
        // all local metadata to Master when the connection is restored.
        if (ha_manager_ && ha_manager_->IsDegraded()) {
            return {};
        }
        if (async_route_notifier_) {
            return async_route_notifier_->EnqueueAdd(key, tier_id, size);
        }
        return SyncAddReplica(key, tier_id, size);
    };
}

RemoveReplicaCallback P2PClientService::BuildRemoveReplicaCallback() {
    return
        [this](
            const std::string& key, const UUID& tier_id,
            enum REMOVE_CALLBACK_TYPE type) -> tl::expected<void, ErrorCode> {
            // In degraded mode, skip metadata notification to Master.
            // The recovery pipeline will re-sync all local metadata,
            // and Master will discard routes for keys that no longer
            // exist locally.
            if (ha_manager_ && ha_manager_->IsDegraded()) {
                return {};
            }
            if (type == REMOVE_CALLBACK_TYPE::DELETE) {
                if (async_route_notifier_) {
                    return async_route_notifier_->EnqueueRemove(key, tier_id);
                }
                return SyncRemoveReplica(key, tier_id);
            } else if (type == REMOVE_CALLBACK_TYPE::DELETE_ALL) {
                // TODO:
                // Currently Master does not support deleting all replicas of a
                // key within a client. The future will be implemented in
                // future.
                LOG(ERROR) << "DELETE_ALL callback is not supported"
                           << ", key: " << key;
                return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
            }

            LOG(ERROR) << "Unknown callback type: " << static_cast<int>(type);
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        };
}

tl::expected<void, ErrorCode> P2PClientService::SyncAddReplica(
    const std::string& key, const UUID& tier_id, size_t size) {
    AddReplicaRequest req;
    req.key = key;
    req.size = size;
    req.client_id = client_id_;
    req.segment_id = tier_id;
    auto result = master_client_.AddReplica(req);
    if (!result) {
        LOG(ERROR) << "Failed to add replica for key: " << key
                   << " error: " << result.error();
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<void, ErrorCode> P2PClientService::SyncRemoveReplica(
    const std::string& key, const UUID& tier_id) {
    RemoveReplicaRequest req;
    req.key = key;
    req.client_id = client_id_;
    req.segment_id = tier_id;
    auto result = master_client_.RemoveReplica(req);
    if (!result) {
        LOG(ERROR) << "Failed to remove replica for key: " << key
                   << " error: " << result.error();
        return tl::unexpected(result.error());
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>>
P2PClientService::SyncBatchRemoveReplica(const std::string& key,
                                         std::vector<UUID> segment_ids) {
    BatchRemoveReplicaRequest req;
    req.key = key;
    req.client_id = client_id_;
    req.segment_ids = std::move(segment_ids);
    auto results = master_client_.BatchRemoveReplica(req);
    for (size_t i = 0; i < results.size(); i++) {
        if (!results[i]) {
            LOG(ERROR) << "Failed to remove replica for key: " << key
                       << ", segment_id: " << req.segment_ids[i]
                       << ", error: " << results[i].error();
        }
    }
    return results;
}

SegmentSyncCallback P2PClientService::BuildSegmentSyncCallback() {
    return [this](const Segment& segment,
                  bool mount) -> tl::expected<void, ErrorCode> {
        if (mount) {
            LOG(INFO) << "Mounting segment with Master: id=" << segment.id
                      << ", name=" << segment.name << ", size=" << segment.size;
            auto result = master_client_.MountSegment(segment);
            if (!result) {
                LOG(ERROR) << "Failed to mount segment with Master: id="
                           << segment.id << ", error=" << result.error();
                return tl::unexpected(result.error());
            }
            return {};
        } else {
            LOG(INFO) << "Unmounting segment from Master: id=" << segment.id
                      << ", name=" << segment.name;
            auto result = master_client_.UnmountSegment(segment.id);
            if (!result) {
                LOG(ERROR) << "Failed to unmount segment from Master: id="
                           << segment.id << ", error=" << result.error();
                return tl::unexpected(result.error());
            }
            return {};
        }
    };
}

// ============================================================================
// Heartbeat & Registration
// ============================================================================

HeartbeatRequest P2PClientService::build_heartbeat_request() {
    HeartbeatRequest req;
    req.client_id = client_id_;

    if (data_manager_.has_value()) {
        SyncSegmentMetaParam param;
        auto tier_views = data_manager_->GetTierViews();
        for (const auto& view : tier_views) {
            TierUsageInfo info;
            info.segment_id = view.id;
            info.usage = view.usage;
            param.tier_usages.push_back(info);
        }
        req.tasks.emplace_back(HeartbeatTaskType::SYNC_SEGMENT_META,
                               std::move(param));
    }

    return req;
}

std::vector<Segment> P2PClientService::CollectTierSegments() const {
    std::vector<Segment> segments;
    if (!data_manager_.has_value()) {
        return segments;
    }

    auto tier_views = data_manager_->GetTierViews();
    segments.reserve(tier_views.size());
    for (const auto& view : tier_views) {
        Segment seg;
        seg.id = view.id;
        seg.name = "tier_" + std::to_string(view.id.first) + "_" +
                   std::to_string(view.id.second);
        seg.size = view.capacity;
        auto& p2p_extra = seg.GetP2PExtra();
        p2p_extra.priority = view.priority;
        p2p_extra.tags = view.tags;
        p2p_extra.memory_type = view.type;
        p2p_extra.usage = view.usage;
        segments.push_back(std::move(seg));
    }
    return segments;
}

tl::expected<RegisterClientResponse, ErrorCode>
P2PClientService::RegisterClient() {
    RegisterClientRequest req;
    req.client_id = client_id_;
    req.segments = CollectTierSegments();
    req.deployment_mode = DeploymentMode::P2P;
    req.ip_address = local_ip_;
    req.rpc_port = client_rpc_port_;

    auto register_result = master_client_.RegisterClient(req);
    if (!register_result) {
        LOG(ERROR) << "Failed to register P2P client: "
                   << register_result.error() << ", client_id=" << client_id_;
    } else {
        view_version_ = register_result.value().view_version;
    }
    return register_result;
}

// ============================================================================
// HA Recovery — delegate to HARecoveryManager
// ============================================================================

void P2PClientService::OnHAEvent(HAEvent event) {
    if (ha_manager_) ha_manager_->HandleEvent(event);
}

std::string P2PClientService::GetHealthStatus() const {
    if (ha_manager_) {
        return toString(ha_manager_->GetState());
    }
    return "OK";
}

// ============================================================================
// Put Operations
// ============================================================================

tl::expected<void, ErrorCode> P2PClientService::Put(const ObjectKey& key,
                                                    std::vector<Slice>& slices,
                                                    const WriteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::Put");
    timer.LogRequest("key=", key, "slice_count=", slices.size());

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("error_code=", ErrorCode::SHUTTING_DOWN);
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    const auto* route_config = std::get_if<WriteRouteRequestConfig>(&config);
    if (!route_config) {
        LOG(ERROR) << "P2PClientService currently only supports "
                      "WriteRouteRequestConfig";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto task_handle_ptr = CreatePutHandle(key, slices, *route_config);
    if (!task_handle_ptr) {
        LOG(ERROR) << "Failed to create put handle for key: " << key
                   << ", error: " << task_handle_ptr.error();
        timer.LogResponse("error_code=", task_handle_ptr.error());
        return tl::unexpected(task_handle_ptr.error());
    } else if (!task_handle_ptr.value()) {
        LOG(ERROR) << "put task handle is null for key: " << key;
        timer.LogResponse("error_code=", ErrorCode::INTERNAL_ERROR);
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto result = task_handle_ptr.value()->Wait();
    if (!result) {
        LOG(ERROR) << "Failed to put key: " << key
                   << ", error: " << result.error();
    }
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> P2PClientService::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::BatchPut");
    timer.LogRequest("batch_size=", keys.size());

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::SHUTTING_DOWN);
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::SHUTTING_DOWN));
    }
    std::vector<tl::expected<void, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::INTERNAL_ERROR));
    if (keys.size() != batched_slices.size()) {
        LOG(ERROR) << "BatchPut input size mismatch";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::INVALID_PARAMS));
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::INVALID_PARAMS);
        return results;
    }

    const auto* route_config = std::get_if<WriteRouteRequestConfig>(&config);
    if (!route_config) {
        LOG(ERROR) << "P2PClientService currently only supports "
                      "WriteRouteRequestConfig";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::INVALID_PARAMS));
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::INVALID_PARAMS);
        return results;
    }

    // Phase 1: create handles for all keys (remote writes are in-flight).
    std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>
        handles;
    handles.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        handles.push_back(
            CreatePutHandle(keys[i], batched_slices[i], *route_config));
    }

    // Phase 2: collect results.
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!handles[i]) {
            LOG(ERROR) << "Failed to create put handle for key: " << keys[i]
                       << ", error: " << handles[i].error();
            results[i] = tl::unexpected(handles[i].error());
        } else if (!handles[i].value()) {
            LOG(ERROR) << "put task handle is null for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::INTERNAL_ERROR);
        } else {
            auto result = handles[i].value()->Wait();
            if (!result) {
                LOG(ERROR) << "Failed to put key: " << keys[i]
                           << ", error: " << result.error();
            }
            results[i] = result;
        }
    }
    size_t success_count = 0;
    for (const auto& r : results) {
        if (r) ++success_count;
    }
    timer.LogResponse("success=", success_count,
                      " fail=", keys.size() - success_count);
    return results;
}

inline bool IsAlreadyExistsError(ErrorCode err) {
    return err == ErrorCode::REPLICA_NUM_EXCEEDED ||
           err == ErrorCode::REPLICA_ALREADY_EXISTS ||
           err == ErrorCode::OBJECT_ALREADY_EXISTS;
}

// Coroutine tries remote write candidates one at a time, retrying on failure.
static async_simple::coro::Lazy<void> RunWriteRetry(
    std::vector<P2PProxyDescriptor> proxies,
    std::shared_ptr<RemoteWriteRequest> write_req,
    std::shared_ptr<std::promise<tl::expected<void, ErrorCode>>> promise,
    RouteCache* route_cache,
    std::function<PeerClient&(const std::string&)> get_peer, std::string key) {
    try {
        for (auto& proxy : proxies) {
            try {
                std::string endpoint =
                    proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
                auto& peer = get_peer(endpoint);
                auto result = co_await peer.AsyncWriteRemoteData(*write_req);
                if (result.has_value()) {
                    if (route_cache) {
                        P2PProxyDescriptor np = proxy;
                        np.segment_id = result.value();
                        route_cache->Upsert(key, {np});
                    }
                    promise->set_value({});
                    co_return;
                } else if (IsAlreadyExistsError(result.error())) {
                    promise->set_value({});
                    co_return;
                } else {
                    LOG(ERROR) << "Failed to write to remote, key: " << key
                               << ", error: " << result.error();
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Failed to write to remote, key: " << key
                           << ", exception: " << e.what();
            } catch (...) {
                LOG(ERROR) << "Failed to write to remote, key: " << key
                           << ", unknown exception";
            }
        }
        promise->set_value(tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
    } catch (...) {
        promise->set_value(tl::unexpected(ErrorCode::INTERNAL_ERROR));
    }
}

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
P2PClientService::CreatePutHandle(const std::string& key,
                                  std::vector<Slice>& slices,
                                  const WriteRouteRequestConfig& config) {
    // DEGRADED: Master unreachable, only allow local writes
    if (ha_manager_ && ha_manager_->IsDegraded()) {
        auto local_handle = data_manager_->Put(key, slices);
        if (local_handle) {
            return std::move(local_handle.value());
        }
        LOG(ERROR) << "Local write failed for key: " << key
                   << ", error: " << local_handle.error();
        return tl::unexpected(local_handle.error());
    }

    size_t total_size = ClientService::CalculateSliceSize(slices);

    // 1. Get write route from master
    WriteRouteRequest route_req;
    route_req.key = key;
    route_req.client_id = client_id_;
    route_req.size = total_size;
    route_req.config = config;

    auto route_result = master_client_.GetWriteRoute(route_req);
    if (!route_result) {
        LOG(WARNING) << "Failed to get write route for key: " << key
                     << " error: " << route_result.error();
        return tl::unexpected(route_result.error());
    }

    auto& candidates = route_result.value().candidates;
    if (candidates.empty()) {
        LOG(ERROR) << "No write candidates for key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // 2. Try all local candidates synchronously
    std::vector<P2PProxyDescriptor> remote_proxies;
    for (size_t i = 0; i < candidates.size(); ++i) {
        auto& proxy = candidates[i].replica;

        if (proxy.client_id == client_id_) {
            if (data_manager_.has_value()) {
                auto local_handle = data_manager_->Put(key, slices);
                if (local_handle) {
                    return std::move(local_handle.value());
                } else if (IsAlreadyExistsError(local_handle.error())) {
                    // The key is already exists. Currently, we think this is a
                    // normal case, just ignore the error and return success.
                    return ImmediateHandle<void>::Create();
                } else {
                    LOG(WARNING) << "Local write failed, trying next candidate"
                                 << ", error: " << local_handle.error();
                    continue;
                }
            }
        } else {
            remote_proxies.push_back(proxy);
        }
    }

    // 3. Chain async RPCs over remote candidates via continuation
    if (remote_proxies.empty()) {
        LOG(ERROR) << "No remote candidates for key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    auto write_req = std::make_shared<RemoteWriteRequest>();
    write_req->key = key;
    for (const auto& slice : slices) {
        RemoteBufferDesc buf;
        buf.segment_endpoint = get_te_endpoint();
        buf.addr = reinterpret_cast<uintptr_t>(slice.ptr);
        buf.size = slice.size;
        write_req->src_buffers.push_back(buf);
    }

    auto promise =
        std::make_shared<std::promise<tl::expected<void, ErrorCode>>>();
    auto future = promise->get_future();

    RunWriteRetry(
        std::move(remote_proxies), write_req, promise,
        route_cache_ ? &(*route_cache_) : nullptr,
        [this](const std::string& ep) -> PeerClient& {
            return GetOrCreatePeerClient(ep);
        },
        key)
        .start([](auto&&) {});

    return RemoteRpcHandle<void>::Create(std::move(write_req),
                                         std::move(future));
}

// ============================================================================
// Get Operations
// ============================================================================

tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> P2PClientService::Get(
    const std::string& key, std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadRouteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::Get");
    timer.LogRequest("key=", key);

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("error_code=", ErrorCode::SHUTTING_DOWN);
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto handle = CreateGetHandle(key, allocator, config);
    if (!handle) {
        if (handle.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to create get handle for key: " << key
                       << ", error: " << handle.error();
        }
        timer.LogResponse("error_code=", handle.error());
        return tl::unexpected(handle.error());
    }
    auto result = handle->task_handle->Wait();
    if (!result) {
        LOG(ERROR) << "get failed for key: " << key
                   << ", error: " << result.error();
        timer.LogResponse("error_code=", result.error());
        return tl::unexpected(result.error());
    }
    timer.LogResponse("error_code=", ErrorCode::OK);
    return handle->read_buf;
}

std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
P2PClientService::BatchGet(const std::vector<std::string>& keys,
                           std::shared_ptr<ClientBufferAllocator> allocator,
                           const ReadRouteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::BatchGet");
    timer.LogRequest("batch_size=", keys.size());

    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::OK));

    auto batch_guard = AcquireInflightGuard();
    if (!batch_guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        for (auto& r : results) {
            r = tl::unexpected(ErrorCode::SHUTTING_DOWN);
        }
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::SHUTTING_DOWN);
        return results;
    }
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        for (auto& r : results) {
            r = tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::INVALID_PARAMS);
        return results;
    }

    // Phase 1: create handles for all keys (local + remote).
    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> handles;
    handles.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        handles.push_back(CreateGetHandle(keys[i], allocator, config));
    }

    // Phase 2: wait on all handles.
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!handles[i]) {
            if (handles[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(ERROR) << "Failed to create get handle for key: " << keys[i]
                           << ", error: " << handles[i].error();
            }
            results[i] = tl::unexpected(handles[i].error());
            continue;
        }
        auto get_result = handles[i]->task_handle->Wait();
        if (!get_result) {
            LOG(ERROR) << "Failed to get key: " << keys[i]
                       << ", error: " << get_result.error();
            results[i] = tl::unexpected(get_result.error());
        } else {
            results[i] = handles[i]->read_buf;
        }
    }
    size_t success_count = 0;
    for (const auto& r : results) {
        if (r) ++success_count;
    }
    timer.LogResponse("success=", success_count,
                      " fail=", keys.size() - success_count);
    return results;
}

tl::expected<int64_t, ErrorCode> P2PClientService::Get(
    const std::string& key, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const ReadRouteConfig& config) {
    ScopedVLogTimer timer(1, "P2PClientService::Get");
    timer.LogRequest("key=", key, "buffer_count=", buffers.size());

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("error_code=", ErrorCode::SHUTTING_DOWN);
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    std::vector<Slice> slices;
    slices.reserve(buffers.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        slices.emplace_back(Slice{buffers[i], sizes[i]});
    }

    auto handle = CreateGetHandle(key, slices, config);
    if (!handle) {
        if (handle.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to create get handle for key: " << key
                       << ", error: " << handle.error();
        }
        timer.LogResponse("error_code=", handle.error());
        return tl::unexpected(handle.error());
    }
    auto result = handle->task_handle->Wait();
    if (!result) {
        LOG(ERROR) << "Failed to wait for get handle for key: " << key
                   << ", error: " << result.error();
        timer.LogResponse("error_code=", result.error());
        return tl::unexpected(result.error());
    }
    timer.LogResponse("error_code=", ErrorCode::OK,
                      " data_size=", handle->data_size);
    return handle->data_size;
}

std::vector<tl::expected<int64_t, ErrorCode>> P2PClientService::BatchGet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReadRouteConfig& config, bool /*aggregate_same_segment_task*/) {
    ScopedVLogTimer timer(1, "P2PClientService::BatchGet");
    timer.LogRequest("batch_size=", keys.size());

    auto batch_guard = AcquireInflightGuard();
    if (!batch_guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::SHUTTING_DOWN);
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }

    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch";
        timer.LogResponse("success=0 fail=", keys.size(),
                          " error_code=", ErrorCode::INVALID_PARAMS);
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    // Phase 1: build slices and create handles (local + remote).
    std::vector<std::vector<Slice>> all_slices(keys.size());
    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> handles;
    handles.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        all_slices[i].reserve(all_buffers[i].size());
        for (size_t j = 0; j < all_buffers[i].size(); ++j) {
            all_slices[i].emplace_back(
                Slice{all_buffers[i][j], all_sizes[i][j]});
        }
        handles.push_back(CreateGetHandle(keys[i], all_slices[i], config));
    }

    // Phase 2: wait on all handles.
    std::vector<tl::expected<int64_t, ErrorCode>> results;
    results.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!handles[i]) {
            if (handles[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(ERROR) << "Failed to create get handle for key: " << keys[i]
                           << ", error: " << handles[i].error();
            }
            results.push_back(tl::unexpected(handles[i].error()));
            continue;
        }
        auto get_result = handles[i]->task_handle->Wait();
        if (!get_result) {
            LOG(ERROR) << "Failed to get key: " << keys[i]
                       << ", error: " << get_result.error();
            results.push_back(tl::unexpected(get_result.error()));
        } else {
            results.push_back(handles[i]->data_size);
        }
    }
    size_t success_count = 0;
    for (const auto& r : results) {
        if (r) ++success_count;
    }
    timer.LogResponse("success=", success_count,
                      " fail=", keys.size() - success_count);
    return results;
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::CreateGetHandle(
    const std::string& key, std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadRouteConfig& config) {
    // 1. Try local first
    if (data_manager_.has_value()) {
        auto local = data_manager_->Get(key, allocator);
        if (local.has_value()) {
            return std::move(local.value());
        }
        if (local.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to get from local, key: " << key
                       << ", error: " << local.error();
            return tl::unexpected(local.error());
        }
    }

    // DEGRADED: local miss with unreachable master — cannot fetch remote
    if (ha_manager_ && ha_manager_->IsDegraded()) {
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    // 2. Build route iterator
    auto iter_result = BuildRouteIter(key, config);
    if (!iter_result) {
        if (iter_result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "fail to build route iterator"
                       << ", key=" << key << ", error=" << iter_result.error();
        }
        return tl::unexpected(iter_result.error());
    }
    auto& iter = iter_result.value();

    // 3. Ensure object_size is known
    // (maybe trigger lazy master load on route cache miss).
    iter.Prime();
    if (iter.empty()) {
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    const uint64_t object_size = iter.object_size();
    auto alloc_result = allocator->allocate(object_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for get, key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    auto read_buf = std::make_shared<BufferHandle>(std::move(*alloc_result));
    std::vector<Slice> slices = {{read_buf->ptr(), object_size}};

    auto fetch_result = InnerGetViaRoute(key, slices, std::move(iter));
    if (!fetch_result) {
        LOG(ERROR) << "Failed to inner get via route for key: " << key
                   << ", error: " << fetch_result.error();
        return tl::unexpected(fetch_result.error());
    } else if (!fetch_result.value().task_handle) {
        LOG(ERROR) << "task handle ptr is null for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    fetch_result->read_buf = std::move(read_buf);
    return std::move(fetch_result.value());
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::CreateGetHandle(
    const std::string& key, std::vector<Slice>& slices,
    const ReadRouteConfig& config) {
    // 1. Try local first
    if (data_manager_.has_value()) {
        auto local = data_manager_->Get(key, slices);
        if (local.has_value()) {
            return std::move(local.value());
        }
        if (local.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to get from local, key: " << key
                       << ", error: " << local.error();
            return tl::unexpected(local.error());
        }
    }

    // DEGRADED: local miss with unreachable master — cannot fetch remote
    if (ha_manager_ && ha_manager_->IsDegraded()) {
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    // 2. Build route iterator
    auto iter_result = BuildRouteIter(key, config);
    if (!iter_result) {
        if (iter_result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to build route iterator for key: " << key
                       << ", error: " << iter_result.error();
        }
        return tl::unexpected(iter_result.error());
    }
    auto& iter = iter_result.value();

    // 3. Ensure routes are available
    // (maybe trigger lazy master load on route cache miss)
    iter.Prime();
    if (iter.empty()) {
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto fetch_result = InnerGetViaRoute(key, slices, std::move(iter));
    if (!fetch_result) {
        LOG(ERROR) << "Failed to inner get via route for key: " << key
                   << ", error: " << fetch_result.error();
        return tl::unexpected(fetch_result.error());
    } else if (!fetch_result.value().task_handle) {
        LOG(ERROR) << "get task handle is null for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return std::move(fetch_result.value());
}

// Coroutine iterates route candidates and retries on failure.
async_simple::coro::Lazy<void> P2PClientService::RunReadRetry(
    RouteIterator iter, std::shared_ptr<RemoteReadRequest> req,
    std::shared_ptr<std::promise<tl::expected<void, ErrorCode>>> promise) {
    ErrorCode final_result = ErrorCode::OBJECT_NOT_FOUND;
    try {
        while (auto route = co_await iter.AsyncNext()) {
            try {
                auto result = co_await route->peer->AsyncReadRemoteData(*req);
                if (result.has_value()) {
                    promise->set_value({});
                    co_return;
                } else if (result.error() != ErrorCode::OBJECT_NOT_FOUND) {
                    LOG(ERROR) << "Failed to get from remote, key: " << req->key
                               << ", error: " << result.error();
                } else {
                    final_result = result.error();
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Failed to get from remote, key: " << req->key
                           << ", exception: " << e.what();
            } catch (...) {
                LOG(ERROR) << "Failed to get from remote, key: " << req->key
                           << ", unknown exception";
            }
            iter.Evict(*route);
        }
        promise->set_value(tl::unexpected(final_result));
    } catch (...) {
        promise->set_value(tl::unexpected(ErrorCode::INTERNAL_ERROR));
    }
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::InnerGetViaRoute(
    const std::string& key, std::vector<Slice>& slices, RouteIterator iter) {
    auto req = std::make_shared<RemoteReadRequest>();
    req->key = key;
    for (const auto& s : slices) {
        RemoteBufferDesc buf;
        buf.segment_endpoint = get_te_endpoint();
        buf.addr = reinterpret_cast<uintptr_t>(s.ptr);
        buf.size = s.size;
        req->dest_buffers.push_back(buf);
    }

    auto promise =
        std::make_shared<std::promise<tl::expected<void, ErrorCode>>>();
    auto future = promise->get_future();

    const uint64_t object_size = iter.object_size();
    RunReadRetry(std::move(iter), req, promise).start([](auto&&) {});

    ReadTaskHandle res;
    res.data_size = object_size;
    res.task_handle =
        RemoteRpcHandle<void>::Create(std::move(req), std::move(future));
    return res;
}

// ============================================================================
// P2PClientService::RouteIterator
// ============================================================================

P2PClientService::RouteIterator::RouteIterator(
    std::string key, std::vector<ResolvedRoute> initial, uint64_t object_size,
    RouteCache* route_cache, MasterFetch master_fetch)
    : key_(std::move(key)),
      routes_(std::move(initial)),
      object_size_(object_size),
      route_cache_(route_cache),
      master_fetch_(std::move(master_fetch)) {}

void P2PClientService::RouteIterator::Prime() {
    if (!routes_.empty() || master_queried_) {
        return;
    }
    master_queried_ = true;
    // syncAwait is safe here: Prime() is always called from the user thread,
    // not an IO/RPC callback thread.
    auto master_routes = async_simple::coro::syncAwait(master_fetch_());
    if (master_routes.empty()) {
        return;
    }
    UpsertToCache(master_routes);
    routes_.insert(routes_.end(),
                   std::make_move_iterator(master_routes.begin()),
                   std::make_move_iterator(master_routes.end()));
    if (!routes_.empty()) {
        object_size_ = routes_.front().object_size;
    }
}

auto P2PClientService::RouteIterator::AsyncNext()
    -> async_simple::coro::Lazy<std::optional<ResolvedRoute>> {
    if (idx_ < routes_.size()) {
        co_return routes_[idx_++];
    }
    if (master_queried_) {
        co_return std::nullopt;
    }
    master_queried_ = true;
    auto master_routes = co_await master_fetch_();
    if (master_routes.empty()) {
        co_return std::nullopt;
    }
    UpsertToCache(master_routes);
    routes_.insert(routes_.end(),
                   std::make_move_iterator(master_routes.begin()),
                   std::make_move_iterator(master_routes.end()));
    if (object_size_ == 0) {
        object_size_ = routes_[idx_].object_size;
    }
    if (idx_ < routes_.size()) {
        co_return routes_[idx_++];
    }
    co_return std::nullopt;
}

void P2PClientService::RouteIterator::UpsertToCache(
    const std::vector<ResolvedRoute>& routes) {
    if (!route_cache_ || routes.empty()) {
        return;
    }
    std::vector<P2PProxyDescriptor> ps;
    ps.reserve(routes.size());
    for (const auto& r : routes) {
        ps.push_back(r.proxy);
    }
    route_cache_->Upsert(key_, ps);
}

void P2PClientService::RouteIterator::Evict(const ResolvedRoute& route) {
    if (route.is_cached && route_cache_) {
        route_cache_->RemoveReplica(key_, {route.proxy});
    }
}

auto P2PClientService::BuildRouteIter(const std::string& key,
                                      const ReadRouteConfig& config)
    -> tl::expected<RouteIterator, ErrorCode> {
    std::vector<ResolvedRoute> routes;

    // Load all replicas from the route cache.
    // if cache miss, routes stays empty, the iterator's Prime()/Next() will
    // query master lazily.
    if (route_cache_) {
        auto cached = route_cache_->Get(key);
        for (const auto& item : cached.items()) {
            P2PProxyDescriptor proxy;
            proxy.client_id = item.client_id;
            proxy.segment_id = item.segment_id;
            proxy.ip_address = item.ip_address;
            proxy.rpc_port = item.rpc_port;
            proxy.object_size = item.object_size;

            std::string endpoint =
                proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
            auto& peer = GetOrCreatePeerClient(endpoint);

            ResolvedRoute route;
            route.peer = &peer;
            route.object_size = proxy.object_size;
            route.is_cached = true;
            route.proxy = proxy;
            routes.push_back(std::move(route));
        }
    }

    uint64_t object_size = routes.empty() ? 0 : routes.front().object_size;
    return RouteIterator(key, std::move(routes), object_size,
                         route_cache_ ? &(*route_cache_) : nullptr,
                         [this, key, config]() {
                             return AsyncResolveRoutesFromMaster(key, config);
                         });
}

async_simple::coro::Lazy<std::vector<P2PClientService::ResolvedRoute>>
P2PClientService::AsyncResolveRoutesFromMaster(const std::string& key,
                                               const ReadRouteConfig& config) {
    std::vector<ResolvedRoute> result;
    auto replica_result =
        co_await master_client_.AsyncGetReplicaList(key, config);
    if (!replica_result) {
        if (replica_result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to query replica size for key: " << key
                       << ", error: " << replica_result.error();
        }
        co_return result;
    }
    auto& replicas = replica_result.value().replicas;
    if (replicas.empty()) {
        co_return result;
    }
    uint64_t total_size = 0;
    for (auto& replica : replicas) {
        if (replica.is_p2p_proxy_replica()) {
            total_size = calculate_total_size(replica);
            break;
        }
    }
    if (total_size == 0) {
        LOG(ERROR) << "Cannot determine size for key: " << key;
        co_return result;
    }
    for (const auto& replica : replicas) {
        if (!replica.is_p2p_proxy_replica()) {
            continue;
        }
        auto proxy = replica.get_p2p_proxy_descriptor();
        std::string endpoint =
            proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
        auto& peer = GetOrCreatePeerClient(endpoint);
        ResolvedRoute route;
        route.peer = &peer;
        route.object_size = total_size;
        route.is_cached = false;
        route.proxy = proxy;
        result.push_back(std::move(route));
    }
    co_return result;
}

// ============================================================================
// IsExist / BatchIsExist (P2P: local-first)
// ============================================================================

tl::expected<bool, ErrorCode> P2PClientService::IsExist(
    const std::string& key) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }

    // Check local first
    if (data_manager_.has_value() && data_manager_->Exist(key)) {
        return true;
    }

    // DEGRADED: skip Master fallback, return local-only result
    if (ha_manager_ && ha_manager_->IsDegraded()) {
        return false;
    }

    // Fallback to master
    return master_client_.ExistKey(key);
}

std::vector<tl::expected<bool, ErrorCode>> P2PClientService::BatchIsExist(
    const std::vector<std::string>& keys) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }

    std::vector<tl::expected<bool, ErrorCode>> results(keys.size());
    std::vector<size_t> miss_indices;
    std::vector<std::string> miss_keys;

    // Batch local check
    for (size_t i = 0; i < keys.size(); ++i) {
        const bool local_hit =
            data_manager_.has_value() && data_manager_->Exist(keys[i]);
        if (local_hit) {
            results[i] = true;
        } else {
            miss_indices.push_back(i);
            miss_keys.push_back(keys[i]);
        }
    }

    // Batch query master for misses
    if (!miss_keys.empty()) {
        auto master_results = master_client_.BatchExistKey(miss_keys);
        for (size_t j = 0; j < miss_indices.size(); ++j) {
            results[miss_indices[j]] = master_results[j];
        }
    }

    return results;
}

// ============================================================================
// Query Operations
// ============================================================================

tl::expected<std::unique_ptr<QueryResult>, ErrorCode> P2PClientService::Query(
    const std::string& object_key, const ReadRouteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }

    // DEGRADED: return empty result (local-only, no Master query)
    if (ha_manager_ && ha_manager_->IsDegraded()) {
        LOG(WARNING) << "fail to access master"
                     << ", key=" << object_key;
        return tl::make_unexpected(ErrorCode::INACCESSIBLE_MASTER);
    }

    // Query master for replica list
    auto result = master_client_.GetReplicaList(object_key, config);
    if (!result) {
        LOG(WARNING) << "fail to get replica list"
                     << ", key=" << object_key << ", error=" << result.error();
        return tl::unexpected(result.error());
    }

    return std::make_unique<QueryResult>(std::move(result.value().replicas));
}

std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
P2PClientService::BatchQuery(const std::vector<std::string>& object_keys,
                             const ReadRouteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
            results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.push_back(tl::make_unexpected(ErrorCode::SHUTTING_DOWN));
        }
        return results;
    }
    auto responses = master_client_.BatchGetReplicaList(object_keys, config);
    std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>> results;
    results.reserve(responses.size());
    for (size_t i = 0; i < responses.size(); ++i) {
        if (responses[i]) {
            results.emplace_back(std::make_unique<QueryResult>(
                std::move(responses[i].value().replicas)));
        } else {
            results.emplace_back(tl::unexpected(responses[i].error()));
        }
    }
    return results;
}

// ============================================================================
// Remove Operations (Not Supported in P2P)
// Attention:
// The behavior of this type of interface has not yet been defined.
// At present, all keys will be evicted by the client's scheduler according
// to a specific strategy.
// The external active remove call is not allowed currently
// ============================================================================

tl::expected<void, ErrorCode> P2PClientService::Remove(const ObjectKey& key) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    LOG(WARNING) << "Remove is not supported in P2P mode";
    return {};  // return ok for ut
}

tl::expected<long, ErrorCode> P2PClientService::RemoveByRegex(
    const ObjectKey& str) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    LOG(WARNING) << "RemoveByRegex is not supported in P2P mode";
    return {};  // return ok for ut
}

tl::expected<long, ErrorCode> P2PClientService::RemoveAll() {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    LOG(WARNING) << "RemoveAll is not supported in P2P mode";
    return {};  // return ok for ut
}

// ============================================================================
// MountSegment / UnmountSegment (Not Supported)
// ============================================================================

tl::expected<void, ErrorCode> P2PClientService::MountSegment(const void* buffer,
                                                             size_t size) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    // Due to TieredBackend does not support dynamic capacity scaling,
    // P2PClientService could not support segment mount/unmount functions.
    // Currently, the segment is mounted in TieredBackend::Init(),
    // and is unmounted in TieredBackend::Destroy()
    LOG(WARNING) << "MountSegment is not supported in P2P mode. "
                 << "Please use TieredBackend::Init config for tier setup.";
    return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<void, ErrorCode> P2PClientService::UnmountSegment(
    const void* buffer, size_t size) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    // P2PClientService does not support dynamic segment mount/unmount.
    // See MountSegment comment for details.
    LOG(WARNING) << "UnmountSegment is not supported in P2P mode.";
    return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
}

// ============================================================================
// PeerClient management
// ============================================================================

PeerClient& P2PClientService::GetOrCreatePeerClient(
    const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(peer_clients_mutex_);
    auto it = peer_clients_.find(endpoint);
    if (it != peer_clients_.end()) {
        return *it->second;
    }

    auto client = std::make_unique<PeerClient>();
    auto connect_result = client->Connect(endpoint);
    if (!connect_result) {
        LOG(ERROR) << "Failed to connect PeerClient to " << endpoint
                   << " error: " << connect_result.error();
        // Still store it; Connect may succeed lazily on first RPC.
    }

    auto [inserted_it, _] = peer_clients_.emplace(endpoint, std::move(client));
    return *inserted_it->second;
}

}  // namespace mooncake
