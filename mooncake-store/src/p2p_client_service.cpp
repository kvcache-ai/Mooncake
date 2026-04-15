#include "p2p_client_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <future>
#include <thread>

#include <async_simple/Try.h>
#include <async_simple/coro/Lazy.h>

namespace mooncake {

// ============================================================================
// Construction / Destruction
// ============================================================================

P2PClientService::P2PClientService(
    const std::string& local_ip, uint16_t te_port,
    const std::string& metadata_connstring,
    const std::map<std::string, std::string>& labels)
    : ClientService(local_ip, te_port, metadata_connstring, labels),
      master_client_(client_id_,
                     metrics_ ? &metrics_->master_client_metric : nullptr) {}

void P2PClientService::Stop() {
    if (!MarkShuttingDown()) {
        return;  // Already shut down.
    }

    LOG(INFO) << "P2PClientService::Stop() — begin";

    // Stop RPC server so no new requests arrive.
    if (client_rpc_server_) {
        client_rpc_server_->stop();
    }
    if (client_rpc_server_thread_.joinable()) {
        client_rpc_server_thread_.join();
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

    // 4. Initialize TieredBackend + DataManager
    err = InitStorage(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize TieredBackend";
        return err;
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

    // 6. Start heartbeat AFTER everything is fully initialized
    StartHeartbeat(config.master_server_entry);

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
    if (config.local_transfer_mode == P2PClientConfig::LocalTransferMode::TE) {
        local_transfer_config.te_endpoint = get_te_endpoint();
    } else {
        local_transfer_config.local_memcpy_async_worker_num =
            config.local_memcpy_async_worker_num;
        local_transfer_config.local_memcpy_async_queue_depth =
            config.local_memcpy_async_queue_depth;
    }

    data_manager_ = DataManager(std::move(tiered_backend), transfer_engine_,
                                config.lock_shard_count, local_transfer_config);
    // Set rectify callback on DataManager to remove stale replicas from master
    data_manager_->SetRectifyCallback(
        [this](const std::string& key, std::optional<UUID> tier_id) {
            if (!tier_id) {
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
        return SyncAddReplica(key, tier_id, size);
    };
}

RemoveReplicaCallback P2PClientService::BuildRemoveReplicaCallback() {
    return
        [this](
            const std::string& key, const UUID& tier_id,
            enum REMOVE_CALLBACK_TYPE type) -> tl::expected<void, ErrorCode> {
            if (type == REMOVE_CALLBACK_TYPE::DELETE) {
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
    req.replica.client_id = client_id_;
    req.replica.segment_id = tier_id;
    req.replica.rpc_port = client_rpc_port_;
    req.replica.ip_address = local_ip_;
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
// Put Operations
// ============================================================================

tl::expected<void, ErrorCode> P2PClientService::Put(const ObjectKey& key,
                                                    std::vector<Slice>& slices,
                                                    const WriteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    const auto* route_config = std::get_if<WriteRouteRequestConfig>(&config);
    if (!route_config) {
        LOG(ERROR) << "P2PClientService currently only supports "
                      "WriteRouteRequestConfig";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto task_handle_ptr = CreatePutHandle(key, slices, *route_config);
    if (!task_handle_ptr) {
        LOG(ERROR) << "Failed to create put handle for key: " << key
                   << ", error: " << task_handle_ptr.error();
        return tl::unexpected(task_handle_ptr.error());
    } else if (!task_handle_ptr.value()) {
        LOG(ERROR) << "put task handle is null for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto result = task_handle_ptr.value()->Wait();
    if (!result) {
        LOG(ERROR) << "Failed to put key: " << key
                   << ", error: " << result.error();
    }
    return result;
}

std::vector<tl::expected<void, ErrorCode>> P2PClientService::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::SHUTTING_DOWN));
    }
    std::vector<tl::expected<void, ErrorCode>> results(keys.size(),
                                   tl::unexpected(ErrorCode::INTERNAL_ERROR));
    if (keys.size() != batched_slices.size()) {
        LOG(ERROR) << "BatchPut input size mismatch";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::INVALID_PARAMS));
        return results;
    }

    const auto* route_config = std::get_if<WriteRouteRequestConfig>(&config);
    if (!route_config) {
        LOG(ERROR) << "P2PClientService currently only supports "
                      "WriteRouteRequestConfig";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::INVALID_PARAMS));
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
    return results;
}

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
P2PClientService::CreatePutHandle(const std::string& key,
                                  std::vector<Slice>& slices,
                                  const WriteRouteRequestConfig& config) {
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

    // 2. Try candidates in order
    for (size_t i = 0; i < candidates.size(); ++i) {
        auto& proxy = candidates[i].replica;

        if (proxy.client_id == client_id_) {
            // Write locally via DataManager
            if (data_manager_.has_value()) {
                auto local_handle = data_manager_->Put(key, slices);
                if (local_handle) {
                    return std::move(local_handle.value());
                } else if (local_handle.error() !=
                               ErrorCode::REPLICA_NUM_EXCEEDED &&
                           local_handle.error() !=
                               ErrorCode::OBJECT_ALREADY_EXISTS &&
                           local_handle.error() !=
                               ErrorCode::REPLICA_ALREADY_EXISTS) {
                    LOG(WARNING) << "Local write failed, trying next candidate"
                                 << ", error: " << local_handle.error();
                    continue;
                } else {
                    // ErrorCode::REPLICA_NUM_EXCEEDED or
                    // ErrorCode::OBJECT_ALREADY_EXISTS or
                    // ErrorCode::REPLICA_ALREADY_EXISTS means the key exists.
                    // Currently, we think this is a normal case,
                    // just ignore the error and return success.
                    return ImmediateHandle<void>::Create();
                }
            }
        }

        // Remote write via PeerClient
        std::string endpoint =
            proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
        auto& peer = GetOrCreatePeerClient(endpoint);

        // Build remote write request:
        // We need to provide the src_buffers (our local registered
        // memory) and let the remote side pull data.
        auto write_req = std::make_shared<RemoteWriteRequest>();
        write_req->key = key;
        for (const auto& slice : slices) {
            RemoteBufferDesc buf;
            buf.segment_endpoint = get_te_endpoint();
            buf.addr = reinterpret_cast<uintptr_t>(slice.ptr);
            buf.size = slice.size;
            write_req->src_buffers.push_back(buf);
        }

        auto promise = std::make_shared<std::promise<tl::expected<void, ErrorCode>>>();
        auto future = promise->get_future();

        peer.AsyncWriteRemoteData(*write_req)
            .start(
                [promise, write_req, this, key, proxy](
                    async_simple::Try<tl::expected<UUID, ErrorCode>>&& result) {
                    OnRemoteWriteComplete(promise, std::move(result), key,
                                          proxy);
                });

        return RemoteRpcHandle<void>::Create(std::move(write_req), std::move(future));
    }

    return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
}

void P2PClientService::OnRemoteWriteComplete(
    std::shared_ptr<std::promise<tl::expected<void, ErrorCode>>> promise,
    async_simple::Try<tl::expected<UUID, ErrorCode>> result,
    const std::string& key, const P2PProxyDescriptor& proxy) {
    try {
        auto& val = result.value();
        if (!val) {
            auto err = val.error();
            if (err == ErrorCode::REPLICA_NUM_EXCEEDED ||
                err == ErrorCode::REPLICA_ALREADY_EXISTS ||
                err == ErrorCode::OBJECT_ALREADY_EXISTS) {
                promise->set_value({});
            } else {
                promise->set_value(tl::unexpected(err));
            }
        } else {
            if (route_cache_) {
                P2PProxyDescriptor new_proxy = proxy;
                new_proxy.segment_id = val.value();
                route_cache_->Upsert(key, {new_proxy});
            }
            promise->set_value({});
        }
    } catch (...) {
        promise->set_value(tl::unexpected(ErrorCode::RPC_FAIL));
    }
}

// ============================================================================
// Get Operations
// ============================================================================

tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> P2PClientService::Get(
    const std::string& key, std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadRouteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto handle = CreateGetHandle(key, allocator, config);
    if (!handle) {
        LOG(ERROR) << "Failed to create get handle for key: " << key
                   << ", error: " << handle.error();
        return tl::unexpected(handle.error());
    }
    auto result = handle->task_handle->Wait();
    if (!result) {
        LOG(ERROR) << "get failed for key: " << key
                   << ", error: " << result.error();
        return tl::unexpected(result.error());
    }
    return handle->read_buf;
}

std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
P2PClientService::BatchGet(const std::vector<std::string>& keys,
                           std::shared_ptr<ClientBufferAllocator> allocator,
                           const ReadRouteConfig& config) {
    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::OK));

    auto batch_guard = AcquireInflightGuard();
    if (!batch_guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        for (auto& r : results) r = tl::unexpected(ErrorCode::SHUTTING_DOWN);
        return results;
    }
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        for (auto& r : results) {
            r = tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
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
            LOG(ERROR) << "Failed to create get handle for key: " << keys[i]
                       << ", error: " << handles[i].error();
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
    return results;
}

tl::expected<int64_t, ErrorCode> P2PClientService::Get(
    const std::string& key, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const ReadRouteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    std::vector<Slice> slices;
    slices.reserve(buffers.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        slices.emplace_back(Slice{buffers[i], sizes[i]});
    }

    auto handle = CreateGetHandle(key, slices, config);
    if (!handle) {
        LOG(ERROR) << "Failed to create get handle for key: " << key
                   << ", error: " << handle.error();
        return tl::unexpected(handle.error());
    }
    auto result = handle->task_handle->Wait();
    if (!result) {
        LOG(ERROR) << "Failed to wait for get handle for key: " << key
                   << ", error: " << result.error();
        return tl::unexpected(result.error());
    }
    return handle->data_size;
}

std::vector<tl::expected<int64_t, ErrorCode>> P2PClientService::BatchGet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReadRouteConfig& config, bool /*aggregate_same_segment_task*/) {
    auto batch_guard = AcquireInflightGuard();
    if (!batch_guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }

    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch";
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
            LOG(ERROR) << "Failed to create get handle for key: " << keys[i]
                       << ", error: " << handles[i].error();
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
    return results;
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::CreateGetHandle(
    const std::string& key, std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadRouteConfig& config) {
    // Try local
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

    // Remote: resolve route, allocate, then async RPC
    auto route = ResolveRoute(key, config);
    if (!route) {
        if (route.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "fail to resolve route"
                       << ", key=" << key << ", error=" << route.error();
        }
        return tl::unexpected(route.error());
    }

    auto alloc_result = allocator->allocate(route->object_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for get, key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    auto read_buf = std::make_shared<BufferHandle>(std::move(*alloc_result));
    std::vector<Slice> slices = {{read_buf->ptr(), route->object_size}};

    auto fetch_result = InnerGetViaRoute(key, slices, config, &route.value());
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
    // Try local
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

    // Remote
    auto fetch_result = InnerGetViaRoute(key, slices, config);
    if (!fetch_result) {
        return tl::unexpected(fetch_result.error());
    } else if (!fetch_result.value().task_handle) {
        LOG(ERROR) << "get task handle is null for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return std::move(fetch_result.value());
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::InnerGetViaRoute(
    const std::string& key, std::vector<Slice>& slices,
    const ReadRouteConfig& config, const ResolvedRoute* resolved) {
    // 1. Route resolution
    ResolvedRoute route;
    if (resolved) {
        route = *resolved;
    } else {
        auto route_result = ResolveRoute(key, config);
        if (!route_result) {
            LOG(ERROR) << "Failed to resolve route for key: " << key
                       << ", error: " << route_result.error();
            return tl::unexpected(route_result.error());
        }
        route = std::move(route_result.value());
    }

    // 2. Build request
    auto req = std::make_shared<RemoteReadRequest>();
    req->key = key;
    for (const auto& s : slices) {
        RemoteBufferDesc buf;
        buf.segment_endpoint = get_te_endpoint();
        buf.addr = reinterpret_cast<uintptr_t>(s.ptr);
        buf.size = s.size;
        req->dest_buffers.push_back(buf);
    }

    // 3. Start async RPC
    auto promise = std::make_shared<std::promise<tl::expected<void, ErrorCode>>>();
    auto future = promise->get_future();

    route.peer->AsyncReadRemoteData(*req).start(
        [promise,
         req](async_simple::Try<tl::expected<void, ErrorCode>>&& result) {
            try {
                auto& r = result.value();
                if (!r) {
                    promise->set_value(tl::unexpected(r.error()));
                } else {
                    promise->set_value({});
                }
            } catch (...) {
                promise->set_value(tl::unexpected(ErrorCode::RPC_FAIL));
            }
        });

    ReadTaskHandle res;
    res.data_size = route.object_size;
    res.task_handle = RemoteRpcHandle<void>::Create(std::move(req), std::move(future));
    return res;
}

tl::expected<P2PClientService::ResolvedRoute, ErrorCode>
P2PClientService::ResolveRoute(const std::string& key,
                               const ReadRouteConfig& config) {
    // Step 1: try RouteCache
    if (route_cache_) {
        auto cached = route_cache_->Get(key);
        for (const auto& item : cached.items()) {
            if (item.client_id == client_id_) continue;  // skip local

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
            return route;
        }
    }

    // Step 2: query Master
    auto size_result = QueryReplicaSize(key, config);
    if (!size_result) {
        return tl::unexpected(size_result.error());
    }
    auto& [replicas, total_size] = size_result.value();

    for (const auto& replica : replicas) {
        if (!replica.is_p2p_proxy_replica()) continue;
        auto proxy = replica.get_p2p_proxy_descriptor();
        if (proxy.client_id == client_id_) continue;  // skip local

        std::string endpoint =
            proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
        auto& peer = GetOrCreatePeerClient(endpoint);

        ResolvedRoute route;
        route.peer = &peer;
        route.object_size = total_size;
        route.is_cached = false;
        return route;
    }

    return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
}

tl::expected<std::pair<std::vector<Replica::Descriptor>, uint64_t>, ErrorCode>
P2PClientService::QueryReplicaSize(const std::string& key,
                                   const ReadRouteConfig& config) {
    auto replica_result = master_client_.GetReplicaList(key, config);
    if (!replica_result) {
        return tl::unexpected(replica_result.error());
    }

    auto& replicas = replica_result.value().replicas;
    if (replicas.empty()) {
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
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
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    return std::make_pair(std::move(replicas), total_size);
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
    // Query master for replica list
    auto result = master_client_.GetReplicaList(object_key, config);
    if (!result) {
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
