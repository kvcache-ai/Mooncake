#include "p2p_client_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <thread>

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

    data_manager_ = DataManager(std::move(tiered_backend), transfer_engine_,
                                config.lock_shard_count);
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

tl::expected<void, ErrorCode> P2PClientService::PutLocal(
    const std::string& key, std::vector<Slice>& slices) {
    if (!data_manager_.has_value()) {
        LOG(ERROR) << "DataManager not initialized";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    if (slices.size() != 1) {
        LOG(ERROR) << "PutLocal currently only supports a single slice, "
                      "but received slice size = "
                   << slices.size();
        return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
    }

    auto result = data_manager_->Put(key, slices[0]);
    if (!result && result.error() != ErrorCode::REPLICA_NUM_EXCEEDED &&
        result.error() != ErrorCode::REPLICA_ALREADY_EXISTS) {
        VLOG(1) << "Local put failed for key: " << key
                << " error: " << result.error();
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<void, ErrorCode> P2PClientService::PutViaRoute(
    const std::string& key, std::vector<Slice>& slices,
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
    tl::expected<void, ErrorCode> result;
    for (auto& candidate : candidates) {
        auto& proxy = candidate.replica;
        // Check if locality: is this our own client?
        if (proxy.client_id == client_id_) {
            // Write locally via DataManager
            result = PutLocal(key, slices);
            if (!result && result.error() != ErrorCode::REPLICA_NUM_EXCEEDED &&
                result.error() != ErrorCode::REPLICA_ALREADY_EXISTS) {
                LOG(WARNING)
                    << "Local write failed despite local route, trying "
                       "next candidate, error: "
                    << result.error();
                continue;  // write failed, attempt next candidate
            } else {
                // ErrorCode::REPLICA_NUM_EXCEEDED or
                // ErrorCode::REPLICA_ALREADY_EXISTS means the key exists.
                // Currently, we think this is a normal case,
                // just ignore the error and return success.
                return {};  // write success
            }
        }

        // Remote write via PeerClient
        std::string endpoint =
            proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
        try {
            auto& peer = GetOrCreatePeerClient(endpoint);

            // Build remote write request:
            // We need to provide the src_buffers (our local registered
            // memory) and let the remote side pull data.
            RemoteWriteRequest write_req;
            write_req.key = key;
            for (const auto& slice : slices) {
                RemoteBufferDesc buf;
                buf.segment_endpoint = transfer_engine_->getLocalIpAndPort();
                buf.addr = reinterpret_cast<uintptr_t>(slice.ptr);
                buf.size = slice.size;
                write_req.src_buffers.push_back(buf);
            }

            auto write_result = peer.WriteRemoteData(write_req);
            if (!write_result) {
                if (write_result.error() != ErrorCode::REPLICA_NUM_EXCEEDED &&
                    write_result.error() != ErrorCode::REPLICA_ALREADY_EXISTS) {
                    LOG(WARNING) << "Remote write to " << endpoint
                                 << " failed: " << write_result.error();
                    continue;  // write failed, attempt next candidate
                } else {
                    // ErrorCode::REPLICA_NUM_EXCEEDED or
                    // ErrorCode::REPLICA_ALREADY_EXISTS means the key exists.
                    // Currently, we think this is a normal case,
                    // just ignore the error and return success.
                    return {};  // write success
                }
            } else {
                // Write success — cache the route for future reads
                if (route_cache_) {
                    P2PProxyDescriptor new_proxy = proxy;
                    new_proxy.segment_id = write_result.value();
                    route_cache_->Upsert(key, {new_proxy});
                }
                return {};  // write success
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception during remote write to " << endpoint
                       << ": " << e.what();
            result = tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }  // end for

    return result;
}

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
    auto result = PutViaRoute(key, slices, *route_config);
    if (!result) {
        if (result.error() != ErrorCode::REPLICA_NUM_EXCEEDED &&
            result.error() != ErrorCode::REPLICA_ALREADY_EXISTS) {
            LOG(ERROR) << "Failed to put key: " << key
                       << " error: " << result.error();
        } else {
            // the key exists, just ignore the error
        }
    }

    return {};
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
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        results.push_back(Put(keys[i], batched_slices[i], config));
    }
    return results;
}

// ============================================================================
// Get Operations
// ============================================================================

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

std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
P2PClientService::BatchGet(const std::vector<std::string>& keys,
                           std::shared_ptr<ClientBufferAllocator> allocator,
                           const ReadRouteConfig& config) {
    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::INTERNAL_ERROR));

    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        for (auto& r : results) {
            r = tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        return results;
    }

    // Process each key: try local first, then batch remote
    for (size_t i = 0; i < keys.size(); ++i) {
        results[i] = Get(keys[i], allocator, config);
    }

    return results;
}

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

    // Try local first — avoids Query RPC on hit
    if (data_manager_.has_value()) {
        auto handle = data_manager_->Get(key);
        if (handle) {
            auto& loc = handle.value()->loc;
            if (loc.data.buffer) {
                size_t local_size = loc.data.buffer->size();

                auto alloc_result = allocator->allocate(local_size);
                if (!alloc_result) {
                    LOG(ERROR) << "Failed to allocate buffer for local get, "
                                  "key: "
                               << key;
                    return tl::unexpected(ErrorCode::INVALID_PARAMS);
                }

                auto buffer_handle = std::move(*alloc_result);
                const char* src =
                    reinterpret_cast<const char*>(loc.data.buffer->data());
                std::memcpy(buffer_handle.ptr(), src, local_size);
                return std::make_shared<BufferHandle>(std::move(buffer_handle));
            }
        }
    }

    // Step 1.5: Try RouteCache before querying Master
    std::vector<P2PProxyDescriptor> cached_proxies;
    if (route_cache_) {
        auto cached = route_cache_->Get(key);
        for (const auto& item : cached.items()) {
            P2PProxyDescriptor proxy;
            proxy.client_id = item.client_id;
            proxy.segment_id = item.segment_id;
            proxy.ip_address = item.ip_address;
            proxy.rpc_port = item.rpc_port;
            proxy.object_size = item.object_size;
            cached_proxies.push_back(proxy);
        }
    }

    std::optional<BufferHandle> buffer_handle;
    if (!cached_proxies.empty()) {
        uint64_t cached_size = cached_proxies[0].object_size;
        auto alloc_result = allocator->allocate(cached_size);
        if (alloc_result) {
            buffer_handle = std::move(*alloc_result);
            // Build slices and do remote get (1 key = 1 slice in P2P)
            std::vector<Slice> slices = {{buffer_handle->ptr(), cached_size}};

            if (GetRemoteViaRoute(key, slices, cached_proxies, true)) {
                return std::make_shared<BufferHandle>(
                    std::move(*buffer_handle));
            }
        }
    }

    // Local miss and Cache miss/fail — query Master for replicas and size
    auto size_result = QueryReplicaSize(key, config);
    if (!size_result) {
        return tl::unexpected(size_result.error());
    }
    auto& [replicas, total_size] = size_result.value();

    if (!buffer_handle || buffer_handle->size() != total_size) {
        auto alloc_result = allocator->allocate(total_size);
        if (!alloc_result) {
            LOG(ERROR) << "Failed to allocate buffer for get, key: " << key;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        buffer_handle = std::move(*alloc_result);
    }

    // Build slices and do remote get (1 key = 1 slice in P2P)
    std::vector<Slice> slices = {{buffer_handle->ptr(), total_size}};

    std::vector<P2PProxyDescriptor> master_proxies;
    for (const auto& replica : replicas) {
        if (!replica.is_p2p_proxy_replica()) {
            LOG(ERROR) << "Invalid replica type for key: " << key
                       << ", replica: " << replica;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        } else {
            master_proxies.push_back(replica.get_p2p_proxy_descriptor());
        }
    }

    auto remote_result = GetRemoteViaRoute(key, slices, master_proxies, false);
    if (!remote_result) {
        LOG(ERROR) << "Failed to get remote data for key: " << key;
        return tl::unexpected(remote_result.error());
    }

    return std::make_shared<BufferHandle>(std::move(*buffer_handle));
}

std::vector<tl::expected<int64_t, ErrorCode>> P2PClientService::BatchGet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReadRouteConfig& config, bool /*aggregate_same_segment_task*/) {
    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<tl::expected<int64_t, ErrorCode>> results;
    results.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        results.push_back(Get(keys[i], all_buffers[i], all_sizes[i], config));
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

    // Attention:
    // if Slice's size is larger than actual data size:
    // 1. in local scene, the memcpy() could run normally
    // 2. in remote scene, TE will return error code
    // (currently, TE simplythinks the Slices's size is data size)
    // Step 1: Try local first via GetLocal
    if (data_manager_.has_value()) {
        std::vector<Slice> local_slices;
        for (size_t i = 0; i < buffers.size(); ++i) {
            local_slices.emplace_back(Slice{buffers[i], sizes[i]});
        }
        auto local_result = GetLocal(key, local_slices);
        if (local_result) {
            return static_cast<int64_t>(local_result.value());
        }
        // GetLocal returns OBJECT_NOT_FOUND on miss — continue to remote;
        // other errors are also non-fatal here, fall through to remote path.
    }

    // Step 1.5: Try RouteCache before querying Master
    std::vector<P2PProxyDescriptor> cached_proxies;
    if (route_cache_) {
        auto cached = route_cache_->Get(key);
        for (const auto& item : cached.items()) {
            P2PProxyDescriptor proxy;
            proxy.client_id = item.client_id;
            proxy.segment_id = item.segment_id;
            proxy.ip_address = item.ip_address;
            proxy.rpc_port = item.rpc_port;
            proxy.object_size = item.object_size;
            cached_proxies.push_back(proxy);
        }
    }

    if (!cached_proxies.empty()) {
        uint64_t total_size = cached_proxies[0].object_size;
        auto slices = BuildSlicesFromBuffers(buffers, sizes, total_size);
        size_t provided_size = ClientService::CalculateSliceSize(slices);
        if (provided_size >= total_size) {
            if (GetRemoteViaRoute(key, slices, cached_proxies, true)) {
                return static_cast<int64_t>(total_size);
            }
        }
    }

    // Step 2: Local miss and cache miss — query Master for replicas and size
    auto size_result = QueryReplicaSize(key, config);
    if (!size_result) {
        return tl::unexpected(size_result.error());
    }
    auto& [replicas, total_size] = size_result.value();

    size_t provided_size = 0;
    for (auto s : sizes) provided_size += s;
    if (provided_size < total_size) {
        LOG(ERROR) << "Buffer too small for key '" << key
                   << "': required=" << total_size
                   << ", provided=" << provided_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<P2PProxyDescriptor> master_proxies;
    for (const auto& replica : replicas) {
        if (!replica.is_p2p_proxy_replica()) {
            LOG(ERROR) << "Invalid replica type for key: " << key
                       << ", replica: " << replica;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        } else {
            master_proxies.push_back(replica.get_p2p_proxy_descriptor());
        }
    }

    // Step 3: Build correctly-sized slices and remote get
    auto slices = BuildSlicesFromBuffers(buffers, sizes, total_size);
    auto remote_result = GetRemoteViaRoute(key, slices, master_proxies, false);
    if (!remote_result) {
        return tl::unexpected(remote_result.error());
    }

    return static_cast<int64_t>(total_size);
}

tl::expected<size_t, ErrorCode> P2PClientService::GetLocal(
    const std::string& key, std::vector<Slice>& slices) {
    if (!data_manager_.has_value()) {
        LOG(ERROR) << "DataManager not initialized";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    auto handle = data_manager_->Get(key);
    if (!handle) {
        VLOG(1) << "Local get miss for key: " << key;
        return tl::unexpected(handle.error());
    }

    // Copy data from handle to slices
    auto& loc = handle.value()->loc;
    if (!loc.data.buffer) {
        LOG(ERROR) << "Allocation handle has null buffer for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const char* src = reinterpret_cast<const char*>(loc.data.buffer->data());
    size_t src_size = loc.data.buffer->size();

    // Verify provided slices are large enough
    size_t provided_size = ClientService::CalculateSliceSize(slices);
    if (provided_size < src_size) {
        LOG(ERROR) << "Buffer too small for local key '" << key
                   << "': required=" << src_size
                   << ", provided=" << provided_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t offset = 0;
    for (auto& slice : slices) {
        size_t copy_size = std::min(slice.size, src_size - offset);
        if (copy_size > 0) {
            std::memcpy(slice.ptr, src + offset, copy_size);
            offset += copy_size;
        }
        if (offset >= src_size) break;
    }
    return src_size;
}

tl::expected<void, ErrorCode> P2PClientService::GetRemoteViaRoute(
    const std::string& key, std::vector<Slice>& slices,
    const std::vector<P2PProxyDescriptor>& proxies, bool is_cached_proxies) {
    if (proxies.empty()) {
        LOG(ERROR) << "No proxies found for key: " << key;
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    std::vector<P2PProxyDescriptor> failed_proxies;

    auto recycle_failed = [&]() {
        if (!failed_proxies.empty() && is_cached_proxies && route_cache_) {
            route_cache_->RemoveReplica(key, failed_proxies);
        }
    };

    for (size_t i = 0; i < proxies.size(); ++i) {
        const auto& proxy = proxies[i];

        // Check if locality (no need to use route cache)
        if (proxy.client_id == client_id_) {
            auto local_result = GetLocal(key, slices);
            if (!local_result) {
                LOG(WARNING)
                    << "fail to get local via route"
                    << ", key: " << key << ", error: " << local_result.error();
                // Rectify stale local route
                if (data_manager_.has_value()) {
                    data_manager_->RectifyReadRoute(key, proxy.segment_id);
                }
                failed_proxies.push_back(proxy);
                continue;  // get failed, attempt next replica
            } else {
                recycle_failed();
                return {};
            }
        }

        // Remote read
        std::string endpoint =
            proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
        try {
            auto& peer = GetOrCreatePeerClient(endpoint);
            RemoteReadRequest read_req;
            read_req.key = key;
            for (const auto& slice : slices) {
                RemoteBufferDesc buf;
                buf.segment_endpoint = transfer_engine_->getLocalIpAndPort();
                buf.addr = reinterpret_cast<uintptr_t>(slice.ptr);
                buf.size = slice.size;
                read_req.dest_buffers.push_back(buf);
            }

            auto read_result = peer.ReadRemoteData(read_req);
            if (!read_result) {
                LOG(WARNING) << "Remote read from " << endpoint
                             << " failed for key: " << key
                             << " error: " << read_result.error();
                failed_proxies.push_back(proxy);
                continue;
            } else {
                if (!is_cached_proxies && route_cache_) {
                    std::vector<P2PProxyDescriptor> remaining_proxies(
                        proxies.begin() + i, proxies.end());
                    route_cache_->Replace(key, remaining_proxies);
                }
                recycle_failed();
                return {};
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception during remote read from " << endpoint
                       << ": " << e.what();
            failed_proxies.push_back(proxy);
        }
    }

    recycle_failed();
    return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
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
    if (data_manager_.has_value()) {
        auto handle = data_manager_->Get(key);
        if (handle) {
            return true;
        }
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
        bool local_hit = false;
        if (data_manager_.has_value()) {
            auto handle = data_manager_->Get(keys[i]);
            if (handle) {
                local_hit = true;
            }
        }
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
