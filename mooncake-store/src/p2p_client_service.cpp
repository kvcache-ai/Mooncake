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
#include <async_simple/coro/SyncAwait.h>

#include "utils/scoped_vlog_timer.h"

namespace mooncake {

namespace {

// UnPin after forward read (or cleanup after TE failure): retry only for
// "other" errors. INVALID_READ = token mismatch (treat as released for flow).
// RPC_FAIL = transport/timeout-like (no repeat; owner may TTL-clean). LEASE_EXPIRED
// = server already expired the pin record.
// Same max-attempt count is used for WriteRevoke after forward write TE failure.
constexpr int kForwardReadUnpinMaxAttempts = 3;

bool UnPinErrorTreatAsEffectiveOk(ErrorCode e) {
    switch (e) {
        case ErrorCode::INVALID_READ:
        case ErrorCode::RPC_FAIL:
        case ErrorCode::LEASE_EXPIRED:
            return true;
        default:
            return false;
    }
}

bool WriteRevokeErrorTreatAsEffectiveOk(ErrorCode e) {
    switch (e) {
        case ErrorCode::INVALID_WRITE:
        case ErrorCode::RPC_FAIL:
            return true;
        default:
            return false;
    }
}

bool SlicesAreContiguous(const std::vector<Slice>& slices) {
    if (slices.empty()) {
        return false;
    }
    for (size_t i = 1; i < slices.size(); ++i) {
        const char* prev_end =
            static_cast<const char*>(slices[i - 1].ptr) + slices[i - 1].size;
        if (prev_end != static_cast<const char*>(slices[i].ptr)) {
            return false;
        }
    }
    return true;
}

size_t TotalSliceBytes(const std::vector<Slice>& slices) {
    size_t t = 0;
    for (const auto& s : slices) {
        t += s.size;
    }
    return t;
}

bool RemoteDestBuffersContiguous(const std::vector<RemoteBufferDesc>& bufs) {
    if (bufs.empty()) {
        return false;
    }
    for (size_t i = 1; i < bufs.size(); ++i) {
        uintptr_t prev_end = bufs[i - 1].addr + bufs[i - 1].size;
        if (prev_end != bufs[i].addr) {
            return false;
        }
    }
    return true;
}

}  // namespace

// ============================================================================
// Construction / Destruction
// ============================================================================

P2PClientService::P2PClientService(
    const std::string& metadata_connstring, uint16_t metrics_port,
    bool enable_metrics_http, const std::map<std::string, std::string>& labels)
    : ClientService(metadata_connstring, metrics_port, enable_metrics_http,
                    labels),
      metrics_(P2PClientMetric::Create(labels)),
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

    // 1. Try to connect to master (allow failure for degraded startup)
    bool master_connected = false;
    ErrorCode err = ConnectToMaster(config.master_server_entry);
    if (err == ErrorCode::OK) {
        master_connected = true;
        LOG(INFO) << "Connected to master successfully";
    } else {
        LOG(WARNING)
            << "Failed to connect to master, starting in DEGRADED mode: "
            << err;
    }

    // 2. Try to register with master (allow failure for degraded startup)
    //    Note: RegisterClient before InitStorage sends empty segments.
    //    Segment info will be updated via MountSegment during InitStorage
    //    (if connected) or during recovery (if degraded).
    bool client_registered = false;
    if (master_connected) {
        auto reg = RegisterClient();
        if (reg) {
            client_registered = true;
            LOG(INFO) << "Registered with master successfully";
        } else {
            LOG(WARNING) << "Failed to register with master: " << reg.error()
                         << ", starting in DEGRADED mode";
        }
    }

    // 3. Initialize HA recovery manager with appropriate initial state.
    //    FULL: master connected and client registered.
    //    DEGRADED: connection or registration failed.
    HAClientState initial_state =
        client_registered ? HAClientState::FULL : HAClientState::DEGRADED;
    ha_manager_ = std::make_unique<HARecoveryManager>(
        client_id_, master_client_, data_manager_, async_route_notifier_,
        view_version_, initial_state);

    // 4. Start heartbeat immediately after registration so master does not
    //    consider this client disconnected during a lengthy initialization.
    StartHeartbeat(config.master_server_entry);

    // 5. Initialize transfer engine (local operation, no master dependency)
    if (config.transfer_engine == nullptr) {
        err = InitTransferEngine(config.local_ip, config.te_port,
                                 metadata_connstring_, config.protocol,
                                 config.rdma_devices);
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

    // 6. Initialize TieredBackend + DataManager.
    //    SegmentSyncCallback will check degraded state and skip MountSegment.
    err = InitStorage(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize TieredBackend";
        return err;
    }

    // 7. Initialize async route notifier if enabled
    if (config.async_sender_thread_count > 0) {
        // When async ADD is rejected by Master, delete the local replica
        // since Master won't track it.
        SyncFailureCallback failure_cb = [this](std::string_view key,
                                                const UUID& segment_id,
                                                ErrorCode error) {
            LOG(WARNING) << "Async ADD rejected by Master, deleting local"
                         << ", key=" << key << ", error=" << error;
            if (data_manager_.has_value()) {
                auto r = data_manager_->Delete(key, segment_id,
                                               /*notify_master=*/false);
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

    // 8. Start P2P client RPC service
    client_rpc_service_.emplace(*data_manager_, metrics_.get());
    client_rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
        config.rpc_thread_num, client_rpc_port_);
    RegisterClientRpcService(*client_rpc_server_, *client_rpc_service_);

    auto rpc_start_failed = std::make_shared<std::atomic<bool>>(false);
    client_rpc_server_thread_ = std::thread([this, rpc_start_failed]() {
        auto ec = client_rpc_server_->start();
        if (ec) {
            rpc_start_failed->store(true);
            LOG(ERROR) << "P2P RPC server failed to start on port "
                       << client_rpc_port_ << ": " << ec.message();
        }
    });

    is_running_ = true;

    // Give RPC server a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (rpc_start_failed->load()) {
        LOG(ERROR) << "P2P RPC server could not bind on port "
                   << client_rpc_port_ << "; aborting service initialization.";
        return ErrorCode::INTERNAL_ERROR;
    }
    LOG(INFO) << "P2P RPC server started on port " << client_rpc_port_;

    // 9. If started in FULL state, notify master that sync is complete.
    //    If in DEGRADED state, heartbeat will recover later.
    if (!ha_manager_->IsDegraded()) {
        ha_manager_->SetSyncCompleted();
    } else {
        LOG(INFO) << "P2P client started in DEGRADED mode, heartbeat will "
                  << "establish master connection when available";
    }

    // 10. Mark service as ready for recovery. HA recovery thread can now
    // proceed.
    ha_manager_->SetReadyForRecovery();

    return ErrorCode::OK;
}

ErrorCode P2PClientService::InitStorage(const P2PClientConfig& config) {
    auto tiered_backend =
        std::make_unique<TieredBackend>(config.lock_shard_count);

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
    KeyLeaseConfig key_lease_config;
    key_lease_config.duration_ms = config.p2p_key_lease_duration_ms;
    key_lease_config.scan_interval_ms = config.p2p_key_lease_scan_interval_ms;

    data_manager_.emplace(std::move(tiered_backend), transfer_engine_,
                          config.lock_shard_count, local_transfer_config,
                          key_lease_config);
    // Set rectify callback on DataManager to remove stale replicas from master
    data_manager_->SetRectifyCallback([this](std::string_view key,
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
    return [this](std::string_view key, const UUID& tier_id,
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
            std::string_view key, const UUID& tier_id,
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
    std::string_view key, const UUID& tier_id, size_t size) {
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
    std::string_view key, const UUID& tier_id) {
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
P2PClientService::SyncBatchRemoveReplica(std::string_view key,
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
            // Skip MountSegment in degraded mode (master not connected).
            // Registration during heartbeat recovery will include segment info.
            if (ha_manager_ && ha_manager_->IsDegraded()) {
                LOG(INFO) << "Skipping MountSegment in DEGRADED mode: id="
                          << segment.id << ", name=" << segment.name;
                return {};
            }
            // TODO: There is a race window between the IsDegraded() check above
            // and the MountSegment() call below. During this window, the system
            // could transition to degraded mode (e.g., due to master connection
            // loss), causing the MountSegment RPC to fail. This would result in
            // segment initialization failure even though the segment could be
            // registered later during heartbeat recovery.
            //
            // Future improvement: Remove BuildSegmentSyncCallback function to
            // decouple storage layer initialization from master interaction.
            // The P2PClientService will manage this logic instead, and can
            // directly convert ha_status to degraded upon RPC failure during
            // initialization.
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

inline bool IsAlreadyExistsError(ErrorCode err) {
    return err == ErrorCode::REPLICA_NUM_EXCEEDED ||
           err == ErrorCode::REPLICA_ALREADY_EXISTS ||
           err == ErrorCode::OBJECT_ALREADY_EXISTS;
}

tl::expected<void, ErrorCode> P2PClientService::Put(const ObjectKey& key,
                                                    std::vector<Slice>& slices,
                                                    const WriteConfig& config) {
    std::vector<std::vector<Slice>> batched_slices{std::move(slices)};
    auto result = BatchPut({key}, batched_slices, config);
    if (result.empty()) {
        LOG(ERROR) << "BatchPut returned empty result for key: " << key;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    slices = std::move(batched_slices[0]);  // restore original slices
    return result[0];
}

std::vector<tl::expected<void, ErrorCode>> P2PClientService::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteConfig& config) {
    std::vector<tl::expected<void, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::INTERNAL_ERROR));
    ScopedVLogTimer timer(1, "P2PClientService::BatchPut");
    timer.LogRequest("batch_size=", keys.size());

    Stopwatch stopwatch;
    if (metrics_) {
        metrics_->local_request.put_requests.inc(keys.size());
    }

    auto guard = AcquireInflightGuard();
    const auto* plain_route = std::get_if<WriteRouteRequestConfig>(&config);
    const auto* ext_route = std::get_if<WriteConfigExt>(&config);
    const WriteRouteRequestConfig* route_cfg_ptr = nullptr;
    RdmaDirectionMode rdma_write_mode = RdmaDirectionMode::REVERSE;
    if (plain_route) {
        route_cfg_ptr = plain_route;
    } else if (ext_route) {
        route_cfg_ptr = &ext_route->route_config;
        rdma_write_mode = ext_route->rdma_direction_mode;
    }
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::SHUTTING_DOWN));
    } else if (keys.size() != batched_slices.size()) {
        LOG(ERROR) << "BatchPut input size mismatch";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::INVALID_PARAMS));
    } else if (!route_cfg_ptr) {
        LOG(ERROR) << "P2PClientService expects WriteRouteRequestConfig or "
                      "WriteConfigExt";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::INVALID_PARAMS));
    } else {
        results = InnerBatchPut(keys, batched_slices, *route_cfg_ptr,
                                rdma_write_mode);
    }

    size_t success_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].has_value()) {
            success_count++;
        }
    }

    if (metrics_) {
        const auto elapsed = stopwatch.elapsed_us();
        const double avg_latency =
            keys.empty() ? 0.0 : static_cast<double>(elapsed) / keys.size();
        for (size_t i = 0; i < results.size(); ++i) {
            if (results[i].has_value()) {
                metrics_->local_request.put_bytes.inc(
                    ClientService::CalculateSliceSize(batched_slices[i]));
                metrics_->local_request.put_latency_success.observe(
                    avg_latency);
            } else {
                metrics_->local_request.put_latency_failure.observe(
                    avg_latency);
            }
        }
        const size_t failure_count = keys.size() - success_count;
        if (failure_count > 0) {
            metrics_->local_request.put_failures.inc(failure_count);
        }
    }

    timer.LogResponse("success=", success_count,
                      " fail=", keys.size() - success_count);
    return results;
}

std::vector<tl::expected<void, ErrorCode>> P2PClientService::InnerBatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteRouteRequestConfig& route_config,
    RdmaDirectionMode rdma_direction_mode) {
    if (ha_manager_ && ha_manager_->IsDegraded()) {
        return InnerBatchPutDegraded(keys, batched_slices);
    }
    return InnerBatchPutNormal(keys, batched_slices, route_config,
                               rdma_direction_mode);
}

std::vector<tl::expected<void, ErrorCode>>
P2PClientService::InnerBatchPutDegraded(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices) {
    // Phase 1: dispatch all local writes.
    std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>
        handles;
    handles.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        handles.push_back(CreatePutHandleFromLocal(keys[i], batched_slices[i]));
    }

    // Phase 2: wait each handle and collect results.
    return CollectResults(handles, keys);
}

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
P2PClientService::CreatePutHandleFromLocal(std::string_view key,
                                           std::vector<Slice>& slices) {
    if (!data_manager_.has_value()) {
        LOG(ERROR) << "Data manager not initialized";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto local_handle = data_manager_->Put(key, slices);

    if (local_handle) {
        return std::move(local_handle.value());
    } else if (!IsAlreadyExistsError(local_handle.error())) {
        LOG(ERROR) << "Local write failed for key: " << key
                   << ", error: " << local_handle.error();
    }

    return tl::unexpected(local_handle.error());
}

std::vector<tl::expected<void, ErrorCode>> P2PClientService::CollectResults(
    std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>&
        handles,
    const std::vector<ObjectKey>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::INTERNAL_ERROR));
    for (size_t i = 0; i < handles.size(); ++i) {
        if (!handles[i]) {
            if (!IsAlreadyExistsError(handles[i].error())) {
                LOG(ERROR) << "Failed to put key: " << keys[i]
                           << ", error: " << handles[i].error();
                results[i] = tl::unexpected(handles[i].error());
            } else {
                results[i] = {};
            }
        } else if (!handles[i].value()) {
            LOG(ERROR) << "put task handle is null for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::INTERNAL_ERROR);
        } else {
            auto wait_result = handles[i].value()->Wait();

            if (wait_result || IsAlreadyExistsError(wait_result.error())) {
                results[i] = {};
            } else {
                LOG(ERROR) << "Failed to put key: " << keys[i]
                           << ", error: " << wait_result.error();
                results[i] = tl::unexpected(wait_result.error());
            }
        }
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
P2PClientService::InnerBatchPutNormal(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteRouteRequestConfig& route_config,
    RdmaDirectionMode rdma_direction_mode) {
    // Phase 1: fetch write routes from master.
    auto batch_routes =
        BatchFetchWriteRoutes(keys, batched_slices, route_config);
    if (!batch_routes) {
        LOG(ERROR) << "BatchGetWriteRoute RPC failed: " << batch_routes.error();
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(batch_routes.error()));
    }

    // Phase 2:
    // 2.1: async dispatch first-candidate writes for each key
    // 2.2: wrap each key in a retry chain based on rotute
    auto handles =
        CreatePutHandlesFromRoute(keys, batched_slices, route_config,
                                  rdma_direction_mode, batch_routes.value());

    // Phase 3: wait every retry chain and collect results.
    return CollectResults(handles, keys);
}

tl::expected<BatchGetWriteRouteResponse, ErrorCode>
P2PClientService::BatchFetchWriteRoutes(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices,
    const WriteRouteRequestConfig& config) {
    BatchGetWriteRouteRequest req;
    req.client_id = client_id_;
    req.config = config;
    req.keys.reserve(keys.size());
    req.sizes.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        req.keys.push_back(keys[i]);
        req.sizes.push_back(
            ClientService::CalculateSliceSize(batched_slices[i]));
    }
    auto batch_route_result = master_client_.BatchGetWriteRoute(req);
    if (!batch_route_result) {
        LOG(ERROR) << "BatchGetWriteRoute RPC failed: "
                   << batch_route_result.error();
        return batch_route_result;
    }
    return batch_route_result;
}

std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>
P2PClientService::CreatePutHandlesFromRoute(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteRouteRequestConfig& route_config,
    RdmaDirectionMode rdma_direction_mode,
    BatchGetWriteRouteResponse& batch_resp) {
    struct WriteTask {
        std::unique_ptr<TaskHandle<void>> first_task;
        std::string first_route;
        std::vector<std::unique_ptr<WriteOp>> retry_op_list;
    };

    // Step 1: dispatch first candidate for each key so all writes are
    // in-flight before we build retry chain.
    std::vector<tl::expected<WriteTask, ErrorCode>> tasks;
    tasks.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (batch_resp.error_codes[i] != ErrorCode::OK) {
            tasks.push_back(tl::unexpected(batch_resp.error_codes[i]));
            continue;
        }
        auto ops = BuildWriteOps(keys[i], batched_slices[i], route_config,
                                 rdma_direction_mode,
                                 std::move(batch_resp.responses[i].candidates));
        if (!ops) {
            LOG(ERROR) << "fail to build write ops"
                       << ", key=" << keys[i] << ", error=" << ops.error();
            tasks.push_back(tl::unexpected(ops.error()));
            continue;
        }
        std::string first_route(ops->front()->route());
        std::unique_ptr<TaskHandle<void>> first_task;
        try {
            // start a async write task and generate a wait handle
            first_task = ops->front()->Dispatch();
        } catch (const std::exception& e) {
            LOG(ERROR) << "First-candidate dispatch threw, key: " << keys[i]
                       << ", route: " << first_route << ", what: " << e.what();
            tasks.push_back(tl::unexpected(ErrorCode::INTERNAL_ERROR));
            continue;
        } catch (...) {
            LOG(ERROR) << "First-candidate dispatch threw unknown, key: "
                       << keys[i] << ", route: " << first_route;
            tasks.push_back(tl::unexpected(ErrorCode::INTERNAL_ERROR));
            continue;
        }
        tasks.push_back(WriteTask{std::move(first_task),
                                  std::move(first_route),
                                  {std::make_move_iterator(ops->begin() + 1),
                                   std::make_move_iterator(ops->end())}});
    }

    // Step 2: build a retry chain for each key and wrap it in task_handle
    using Handle = tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>;
    std::vector<Handle> handles;
    handles.reserve(keys.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
        if (!tasks[i]) {
            handles.push_back(tl::unexpected(tasks[i].error()));
            continue;
        }
        auto& task = *tasks[i];
        auto promise = std::make_shared<
            async_simple::Promise<tl::expected<void, ErrorCode>>>();
        auto future = promise->getFuture();
        RunWriteWithRetry(std::move(promise), std::move(task.first_task),
                          std::move(task.first_route),
                          std::move(task.retry_op_list), keys[i])
            .start([](auto&&) {});
        handles.push_back(FutureHandle<void>::Create(std::shared_ptr<void>{},
                                                     std::move(future)));
    }
    return handles;
}

auto P2PClientService::BuildWriteOps(std::string_view key,
                                     std::vector<Slice>& slices,
                                     const WriteRouteRequestConfig& config,
                                     RdmaDirectionMode rdma_direction_mode,
                                     std::vector<WriteCandidate> candidates)
    -> tl::expected<std::vector<std::unique_ptr<WriteOp>>, ErrorCode> {
    if (candidates.empty()) {
        LOG(ERROR) << "No write candidates for key: " << key;
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

    std::vector<std::unique_ptr<WriteOp>> write_ops;
    for (auto& candidate : candidates) {
        auto& proxy = candidate.replica;
        if (proxy.client_id == client_id_) {
            // Defensive check: master should not return local candidates when
            // allow_local=false, but if it does, just skip it.
            if (!config.allow_local) {
                LOG(WARNING) << "Master returned local candidate but "
                                "allow_local=false, skipping";
                continue;
            } else if (!data_manager_.has_value()) {
                LOG(ERROR) << "Data manager not initialized";
                continue;
            }
            write_ops.push_back(
                std::make_unique<LocalWriteOp>(&*data_manager_, key, &slices));
        } else {
            std::string endpoint =
                proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
            DataManager* fwd_dm =
                (rdma_direction_mode == RdmaDirectionMode::FORWARD &&
                 data_manager_.has_value())
                    ? &*data_manager_
                    : nullptr;
            write_ops.push_back(std::make_unique<RemoteWriteOp>(
                this, &GetOrCreatePeerClient(endpoint), write_req,
                std::move(proxy), route_cache_ ? &*route_cache_ : nullptr,
                endpoint, fwd_dm, &slices, rdma_direction_mode));
        }
    }

    if (write_ops.empty()) {
        LOG(ERROR) << "No valid candidates for key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    return write_ops;
}

// TODO (TE mode blocking):
// When local_transfer_mode == TE, data_manager->Put() returns a
// CallableTaskHandle whose WaitAsync() falls back to the base class synchronous
// Wait() — there is no true coroutine suspension point.
// As a result, co_await current_task->WaitAsync() inside RunWriteWithRetry will
// block the coroutine worker thread for the full duration of the TE transfer
// instead of yielding it.
// The retry chain can only advance after that blocking wait returns.
// See the TODO in DataManager::PutViaTe for planned async improvements.
std::unique_ptr<TaskHandle<void>> P2PClientService::LocalWriteOp::Dispatch() {
    if (!data_manager) {
        LOG(ERROR) << "Data manager not initialized";
        return CallableTaskHandle<void>::Create(
            []() -> tl::expected<void, ErrorCode> {
                return tl::unexpected(ErrorCode::INTERNAL_ERROR);
            });
    }
    auto handle = data_manager->Put(key, *slices);
    if (!handle) {
        if (!IsAlreadyExistsError(handle.error())) {
            LOG(ERROR) << "Local write failed (dispatch), key: " << key
                       << ", error: " << handle.error();
        }
        return CallableTaskHandle<void>::Create(
            [e = handle.error()]() -> tl::expected<void, ErrorCode> {
                return tl::make_unexpected(e);
            });
    }
    return std::move(handle.value());
}

std::unique_ptr<TaskHandle<void>> P2PClientService::RemoteWriteOp::Dispatch() {
    if (!owner_service) {
        LOG(ERROR) << "Remote write requires P2PClientService";
        return CallableTaskHandle<void>::Create(
            []() -> tl::expected<void, ErrorCode> {
                return tl::unexpected(ErrorCode::INTERNAL_ERROR);
            });
    }
    if (rdma_direction_mode == RdmaDirectionMode::FORWARD) {
        return owner_service->StartForwardRemotePut(
            peer_ptr, forward_dm, forward_slices, write_req);
    }
    return owner_service->RunReverseRemotePut(peer_ptr, write_req, proxy,
                                              route_cache);
}

std::unique_ptr<TaskHandle<void>> P2PClientService::StartForwardRemotePut(
    PeerClient* peer, DataManager* forward_dm,
    std::vector<Slice>* forward_slices,
    std::shared_ptr<RemoteWriteRequest> write_req) {
    if (!forward_dm || !forward_slices) {
        LOG(ERROR) << "Forward RDMA write requires local DataManager";
        return CallableTaskHandle<void>::Create(
            []() -> tl::expected<void, ErrorCode> {
                return tl::unexpected(ErrorCode::INTERNAL_ERROR);
            });
    }
    auto promise = std::make_shared<
        async_simple::Promise<tl::expected<void, ErrorCode>>>();
    auto future = promise->getFuture();
    RunForwardRemotePut(std::move(promise), peer, forward_dm, write_req,
                        forward_slices)
        .start([](auto&&) {});
    return FutureHandle<void>::Create(write_req, std::move(future));
}

std::unique_ptr<TaskHandle<void>> P2PClientService::RunReverseRemotePut(
    PeerClient* peer, std::shared_ptr<RemoteWriteRequest> write_req,
    const P2PProxyDescriptor& proxy, RouteCache* route_cache) {
    auto promise = std::make_shared<
        async_simple::Promise<tl::expected<void, ErrorCode>>>();
    auto future = promise->getFuture();

    auto req = write_req;
    auto cached_proxy = proxy;
    auto* cache = route_cache;

    peer->AsyncWriteRemoteData(*write_req)
        .start([promise, req, cached_proxy,
                cache](async_simple::Try<tl::expected<UUID, ErrorCode>>&&
                           remote_res) mutable {
            tl::expected<void, ErrorCode> out;
            try {
                auto& result = remote_res.value();
                if (result.has_value()) {
                    if (cache) {
                        P2PProxyDescriptor desc = cached_proxy;
                        desc.segment_id = result.value();
                        cache->Upsert(req->key, {desc});
                    }
                } else {
                    if (!IsAlreadyExistsError(result.error())) {
                        LOG(ERROR)
                            << "Failed to write to remote, key: " << req->key
                            << ", error: " << result.error();
                    }
                    out = tl::make_unexpected(result.error());
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Remote write threw, key: " << req->key
                           << ", what: " << e.what();
                out = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            } catch (...) {
                LOG(ERROR) << "Remote write threw unknown, key: " << req->key;
                out = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            promise->setValue(std::move(out));
        });

    return FutureHandle<void>::Create(req, std::move(future));
}

async_simple::coro::Lazy<void> P2PClientService::RunForwardRemotePut(
    std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
        promise,
    PeerClient* peer, DataManager* dm,
    std::shared_ptr<RemoteWriteRequest> write_req,
    std::vector<Slice>* slices) {
    if (!peer || !dm || !write_req || !slices) {
        promise->setValue(tl::expected<void, ErrorCode>(tl::unexpect, ErrorCode::INTERNAL_ERROR));
        co_return;
    }
    if (!SlicesAreContiguous(*slices)) {
        LOG(ERROR)
            << "Forward RDMA write requires contiguous slice buffers, key="
            << write_req->key;
        tl::expected<void, ErrorCode> err =
            tl::make_unexpected(ErrorCode::NON_CONTIGUOUS_BUFFER_NOT_SUPPORTED);
        promise->setValue(std::move(err));
        co_return;
    }
    PreWriteRequest pre_req;
    pre_req.key = write_req->key;
    pre_req.size_bytes = TotalSliceBytes(*slices);
    pre_req.target_tier_id = write_req->target_tier_id;

    auto pre = co_await peer->AsyncPreWrite(pre_req);
    if (!pre) {
        if (!IsAlreadyExistsError(pre.error())) {
            LOG(ERROR) << "AsyncPreWrite failed, key=" << write_req->key
                       << ", error=" << pre.error();
        }
        promise->setValue(tl::expected<void, ErrorCode>(tl::unexpect, pre.error()));
        co_return;
    }

    std::vector<RemoteBufferDesc> dest{pre.value().remote_buffer};
    void* base = slices->front().ptr;
    auto te = dm->TransferWithTeNoTierStaging(
        base, TotalSliceBytes(*slices), dest,
        Transport::TransferRequest::WRITE);
    if (!te) {
        LOG(ERROR) << "Forward TE write failed, key=" << write_req->key
                   << ", error=" << te.error();
        WriteRevokeRequest revoke_req;
        revoke_req.key = write_req->key;
        revoke_req.write_operation_id = pre.value().write_operation_id;
        tl::expected<void, ErrorCode> revoke_res;
        for (int attempt = 0; attempt < kForwardReadUnpinMaxAttempts; ++attempt) {
            revoke_res = co_await peer->AsyncWriteRevoke(revoke_req);
            if (revoke_res) {
                break;
            }
            if (WriteRevokeErrorTreatAsEffectiveOk(revoke_res.error())) {
                revoke_res = tl::expected<void, ErrorCode>{};
                break;
            }
            if (attempt + 1 < kForwardReadUnpinMaxAttempts) {
                LOG(WARNING) << "AsyncWriteRevoke retry after TE failure, key="
                             << write_req->key << ", attempt=" << (attempt + 1)
                             << ", error=" << revoke_res.error();
            }
        }
        if (!revoke_res) {
            LOG(ERROR) << "AsyncWriteRevoke failed after TE failure, key="
                         << write_req->key << ", error=" << revoke_res.error();
        }
        promise->setValue(tl::expected<void, ErrorCode>(tl::unexpect, te.error()));
        co_return;
    }

    WriteCommitRequest commit;
    commit.key = write_req->key;
    commit.write_operation_id = pre.value().write_operation_id;
    auto cm = co_await peer->AsyncWriteCommit(commit);
    if (!cm) {
        promise->setValue(tl::expected<void, ErrorCode>(tl::unexpect, cm.error()));
        co_return;
    }
    promise->setValue(tl::expected<void, ErrorCode>{});
}

async_simple::coro::Lazy<void> P2PClientService::RunWriteWithRetry(
    std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
        promise,
    std::unique_ptr<TaskHandle<void>> current_task, std::string current_route,
    std::vector<std::unique_ptr<WriteOp>> retry_op_list, std::string_view key) {
    size_t retry_cnt = 0;
    tl::expected<void, ErrorCode> result;
    while (current_task) {
        try {
            result = co_await current_task->WaitAsync();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Wait threw, key: " << key
                       << ", route: " << current_route
                       << ", what: " << e.what();
            result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        } catch (...) {
            LOG(ERROR) << "Wait threw unknown, key: " << key
                       << ", route: " << current_route;
            result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        if (result.has_value() || IsAlreadyExistsError(result.error())) {
            promise->setValue(std::move(result));
            co_return;
        }
        LOG(ERROR) << "Write candidate failed, key: " << key
                   << ", route: " << current_route
                   << ", retry_cnt: " << retry_cnt
                   << ", error: " << result.error();
        current_task.reset();
        while (retry_cnt < retry_op_list.size() && !current_task) {
            auto& op = retry_op_list[retry_cnt];
            current_route = std::string(op->route());
            try {
                current_task = op->Dispatch();
            } catch (const std::exception& e) {
                LOG(ERROR) << "Dispatch threw, key: " << key
                           << ", route: " << current_route
                           << ", retry_cnt: " << retry_cnt
                           << ", what: " << e.what();
            } catch (...) {
                LOG(ERROR) << "Dispatch threw unknown, key: " << key
                           << ", retry_cnt: " << retry_cnt
                           << ", route: " << current_route;
            }
            retry_cnt++;
        }
    }
    if (result.has_value()) {
        result = tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    LOG(ERROR) << "write failed with all retry list"
               << ", key:" << key << ", error: " << result.error();
    promise->setValue(std::move(result));
}

// ============================================================================
// Get Operations
// ============================================================================

tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> P2PClientService::Get(
    const std::string& key, std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadConfigExt& config) {
    return std::move(BatchGet({key}, allocator, config)[0]);
}

tl::expected<int64_t, ErrorCode> P2PClientService::Get(
    const std::string& key, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const ReadConfigExt& config) {
    return std::move(BatchGet({key}, {buffers}, {sizes}, config)[0]);
}

std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
P2PClientService::BatchGet(const std::vector<std::string>& keys,
                           std::shared_ptr<ClientBufferAllocator> allocator,
                           const ReadConfigExt& config) {
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return std::vector<
            tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    auto create_handles = [&] {
        return BatchCreateGetHandles(keys, allocator, config);
    };
    auto extract_buf = [](ReadTaskHandle& h) { return h.read_buf; };

    return BatchGetImpl<std::shared_ptr<BufferHandle>>(keys, create_handles,
                                                       extract_buf);
}

std::vector<tl::expected<int64_t, ErrorCode>> P2PClientService::BatchGet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReadConfigExt& config, bool /*aggregate_same_segment_task*/) {
    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<std::vector<Slice>> all_slices(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        all_slices[i].reserve(all_buffers[i].size());
        for (size_t j = 0; j < all_buffers[i].size(); ++j) {
            all_slices[i].emplace_back(
                Slice{all_buffers[i][j], all_sizes[i][j]});
        }
    }

    auto create_handles = [&] {
        return BatchCreateGetHandles(keys, all_slices, config);
    };
    auto extract_size = [](ReadTaskHandle& h) { return h.data_size; };

    return BatchGetImpl<int64_t>(keys, create_handles, extract_size);
}

template <typename ResultT, typename CreateHandlesFn, typename ExtractFn>
std::vector<tl::expected<ResultT, ErrorCode>> P2PClientService::BatchGetImpl(
    const std::vector<std::string>& keys, CreateHandlesFn&& create_handles,
    ExtractFn&& extract) {
    ScopedVLogTimer timer(1, "P2PClientService::BatchGet");
    timer.LogRequest("batch_size=", keys.size());

    if (metrics_) {
        metrics_->local_request.get_requests.inc(keys.size());
    }

    std::vector<tl::expected<ResultT, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::INTERNAL_ERROR));
    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> handles;

    Stopwatch stopwatch;
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        std::fill(results.begin(), results.end(),
                  tl::unexpected(ErrorCode::SHUTTING_DOWN));
    } else {
        handles = create_handles();
        if (handles.size() != keys.size()) {
            LOG(ERROR) << "handles size mismatch";
            std::fill(results.begin(), results.end(),
                      tl::unexpected(ErrorCode::INTERNAL_ERROR));
        } else {
            for (size_t i = 0; i < handles.size(); ++i) {
                if (!handles[i]) {
                    if (handles[i].error() != ErrorCode::OBJECT_NOT_FOUND) {
                        LOG(ERROR)
                            << "Failed to create get handle for key: "
                            << keys[i] << ", error: " << handles[i].error();
                    }
                    results[i] = tl::unexpected(handles[i].error());
                } else {
                    auto wait_result = handles[i]->task_handle->Wait();
                    if (!wait_result) {
                        LOG(ERROR) << "Failed to get key: " << keys[i]
                                   << ", error: " << wait_result.error();
                        results[i] = tl::unexpected(wait_result.error());
                    } else {
                        results[i] = extract(handles[i].value());
                    }
                }
            }  // end for
        }
    }

    size_t success_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].has_value()) {
            success_count++;
        }
    }

    if (metrics_) {
        const auto elapsed = stopwatch.elapsed_us();
        const double avg_latency =
            keys.empty() ? 0.0 : static_cast<double>(elapsed) / keys.size();
        for (size_t i = 0; i < results.size(); ++i) {
            if (results[i].has_value()) {
                metrics_->local_request.get_bytes.inc(handles[i]->data_size);
                metrics_->local_request.get_latency_success.observe(
                    avg_latency);
            } else {
                metrics_->local_request.get_latency_failure.observe(
                    avg_latency);
            }
        }
        const size_t failure_count = keys.size() - success_count;
        if (failure_count > 0) {
            metrics_->local_request.get_failures.inc(failure_count);
        }
    }

    timer.LogResponse("success=", success_count,
                      " fail=", keys.size() - success_count);
    return results;
}

std::vector<tl::expected<ReadTaskHandle, ErrorCode>>
P2PClientService::BatchCreateGetHandles(
    const std::vector<std::string>& keys,
    std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadConfigExt& config) {
    auto local_get = [&](std::string_view key,
                         size_t) -> tl::expected<ReadTaskHandle, ErrorCode> {
        if (!data_manager_.has_value()) {
            LOG(ERROR) << "Data manager is not initialized";
            return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        return data_manager_->Get(key, allocator);
    };
    auto remote_get = [&](std::string_view key, size_t,
                          std::vector<ResolvedRoute> routes) {
        return CreateRemoteGetHandle(key, allocator, config, std::move(routes));
    };
    return BatchCreateGetHandlesImpl(keys, config, local_get, remote_get);
}

std::vector<tl::expected<ReadTaskHandle, ErrorCode>>
P2PClientService::BatchCreateGetHandles(
    const std::vector<std::string>& keys,
    std::vector<std::vector<Slice>>& all_slices,
    const ReadConfigExt& config) {
    auto local_get = [&](std::string_view key,
                         size_t i) -> tl::expected<ReadTaskHandle, ErrorCode> {
        if (!data_manager_.has_value()) {
            LOG(ERROR) << "Data manager is not initialized";
            return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        return data_manager_->Get(key, all_slices[i]);
    };
    auto remote_get = [&](std::string_view key, size_t i,
                          std::vector<ResolvedRoute> routes) {
        return CreateRemoteGetHandle(key, all_slices[i], config,
                                     std::move(routes));
    };
    return BatchCreateGetHandlesImpl(keys, config, local_get, remote_get);
}

template <typename LocalGetFn, typename RemoteGetFn>
std::vector<tl::expected<ReadTaskHandle, ErrorCode>>
P2PClientService::BatchCreateGetHandlesImpl(
    const std::vector<std::string>& keys, const ReadConfigExt& config,
    LocalGetFn&& local_get, RemoteGetFn&& remote_get) {
    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> handles;
    handles.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        handles.emplace_back(tl::unexpected(ErrorCode::OBJECT_NOT_FOUND));
    }

    // Phase A: try local for all keys; collect indices that need remote fetch.
    std::vector<size_t> miss_indices;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto local = local_get(keys[i], i);
        if (local.has_value()) {
            handles[i] = std::move(local.value());
            // Count local cache hits
            if (metrics_) {
                metrics_->local_request.get_hits.inc();
            }
        } else if (local.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to get from local, key: " << keys[i]
                       << ", error: " << local.error();
            handles[i] = tl::unexpected(local.error());
        } else {
            miss_indices.push_back(i);
            // Count local cache misses
            if (metrics_) {
                metrics_->local_request.get_misses.inc();
            }
        }
    }

    if (miss_indices.empty() || (ha_manager_ && ha_manager_->IsDegraded())) {
        // case 1: All keys are found locally
        // case 2: DEGRADED: master is unreachable
        return handles;
    }

    // Phase B: batch-fetch routes for local-miss keys
    std::vector<std::string_view> miss_key_views;
    miss_key_views.reserve(miss_indices.size());
    for (size_t i : miss_indices) {
        miss_key_views.emplace_back(keys[i]);
    }
    auto routes = BatchFetchReadRoutes(miss_key_views, config);

    // Phase C: remote get for each miss
    for (size_t j = 0; j < miss_indices.size(); ++j) {
        const size_t i = miss_indices[j];
        if (!routes[j]) {
            handles[i] = tl::unexpected(routes[j].error());
        } else {
            handles[i] = remote_get(keys[i], i, std::move(routes[j].value()));
        }
    }
    return handles;
}

std::vector<
    tl::expected<std::vector<P2PClientService::ResolvedRoute>, ErrorCode>>
P2PClientService::BatchFetchReadRoutes(
    const std::vector<std::string_view>& keys, const ReadConfigExt& config) {
    std::vector<tl::expected<std::vector<ResolvedRoute>, ErrorCode>> result(
        keys.size(), std::vector<ResolvedRoute>{});

    // check route cache, collect misses
    std::vector<std::string_view> miss_keys;
    std::vector<size_t> miss_pos;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto cached = LoadCachedRoutes(keys[i]);
        if (!cached.empty()) {
            result[i] = std::move(cached);
        } else {
            miss_keys.push_back(keys[i]);
            miss_pos.push_back(i);
        }
    }
    if (miss_keys.empty()) {
        // all keys are found in route cache
        return result;
    }

    // Single batch RPC to master
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> responses;
    responses = master_client_.BatchGetReplicaList(miss_keys, config.route_config);
    for (size_t k = 0; k < responses.size(); ++k) {
        if (!responses[k]) {
            if (responses[k].error() != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(ERROR) << "BatchFetchReadRoutes failed: "
                           << responses[k].error();
            }
            result[miss_pos[k]] = tl::unexpected(responses[k].error());
            continue;
        }
        auto routes = ReplicasToRoutes(responses[k].value().replicas);
        if (routes.empty()) {
            LOG(ERROR) << "invalid route, key=" << miss_keys[k];
            result[miss_pos[k]] = tl::unexpected(ErrorCode::INTERNAL_ERROR);
            continue;
        }
        if (route_cache_) {
            std::vector<P2PProxyDescriptor> descriptors;
            descriptors.reserve(routes.size());
            for (const auto& r : routes) {
                descriptors.push_back(r.proxy);
            }
            route_cache_->Upsert(miss_keys[k], std::move(descriptors));
        }
        result[miss_pos[k]] = std::move(routes);
    }
    return result;
}

std::vector<P2PClientService::ResolvedRoute> P2PClientService::LoadCachedRoutes(
    std::string_view key) {
    std::vector<ResolvedRoute> routes;
    if (route_cache_) {
        for (const auto& item : route_cache_->Get(key).items()) {
            P2PProxyDescriptor proxy;
            proxy.client_id = item.client_id;
            proxy.segment_id = item.segment_id;
            proxy.ip_address = item.ip_address;
            proxy.rpc_port = item.rpc_port;
            proxy.object_size = item.object_size;
            std::string endpoint =
                proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
            auto& peer = GetOrCreatePeerClient(endpoint);
            routes.push_back(
                {&peer, proxy.object_size, /*is_cached=*/true, proxy});
        }
    }
    return routes;
}

std::vector<P2PClientService::ResolvedRoute> P2PClientService::ReplicasToRoutes(
    const std::vector<Replica::Descriptor>& replicas) {
    std::vector<ResolvedRoute> routes;
    if (replicas.empty()) {
        LOG(WARNING) << "replicas is empty";
        return routes;
    }
    uint64_t total_size = 0;
    for (const auto& replica : replicas) {
        if (!replica.is_p2p_proxy_replica()) {
            LOG(ERROR) << "invalid replica, not p2p proxy replica";
            return routes;
        }
    }
    total_size = calculate_total_size(replicas[0]);
    if (total_size == 0) {
        LOG(ERROR) << "invalid replica, total size is 0";
        return routes;
    }

    for (const auto& replica : replicas) {
        auto proxy = replica.get_p2p_proxy_descriptor();
        std::string endpoint =
            proxy.ip_address + ":" + std::to_string(proxy.rpc_port);
        auto& peer = GetOrCreatePeerClient(endpoint);
        routes.push_back({&peer, total_size, /*is_cached=*/false, proxy});
    }
    return routes;
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::CreateRemoteGetHandle(
    std::string_view key, std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadConfigExt& config, std::vector<ResolvedRoute> pre_fetched) {
    auto iter = BuildRouteIter(key, config, std::move(pre_fetched));
    if (!iter) {
        LOG(ERROR) << "Failed to build route iterator, key=" << key
                   << ", error=" << iter.error();
        return tl::unexpected(iter.error());
    }

    const uint64_t object_size = iter->object_size();
    auto alloc_result = allocator->allocate(object_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for get, key: " << key;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    auto read_buf = std::make_shared<BufferHandle>(std::move(*alloc_result));
    std::vector<Slice> slices = {{read_buf->ptr(), object_size}};

    auto result =
        InnerGetViaRoute(key, slices, std::move(*iter), config.rdma_direction_mode);
    if (!result) {
        LOG(ERROR) << "Failed to get via route, key=" << key
                   << ", error=" << result.error();
        return tl::unexpected(result.error());
    } else {
        // the read_buf is allocated in this function,
        // so we need to guarantee the lifetime of read_buf
        result->read_buf = std::move(read_buf);
    }
    return result;
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::CreateRemoteGetHandle(
    std::string_view key, std::vector<Slice>& slices,
    const ReadConfigExt& config, std::vector<ResolvedRoute> pre_fetched) {
    auto iter = BuildRouteIter(key, config, std::move(pre_fetched));
    if (!iter) {
        if (iter.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to build route iterator, key=" << key
                       << ", error=" << iter.error();
        }
        return tl::unexpected(iter.error());
    }
    auto result =
        InnerGetViaRoute(key, slices, std::move(*iter), config.rdma_direction_mode);
    if (!result) {
        LOG(ERROR) << "Failed to get via route, key=" << key
                   << ", error=" << result.error();
        return tl::unexpected(result.error());
    }
    return result;
}

tl::expected<ReadTaskHandle, ErrorCode> P2PClientService::InnerGetViaRoute(
    std::string_view key, std::vector<Slice>& slices, RouteIterator iter,
    RdmaDirectionMode rdma_direction_mode) {
    auto req = std::make_shared<RemoteReadRequest>();
    req->key = key;
    for (const auto& s : slices) {
        RemoteBufferDesc buf;
        buf.segment_endpoint = get_te_endpoint();
        buf.addr = reinterpret_cast<uintptr_t>(s.ptr);
        buf.size = s.size;
        req->dest_buffers.push_back(buf);
    }

    auto promise = std::make_shared<
        async_simple::Promise<tl::expected<void, ErrorCode>>>();
    auto future = promise->getFuture();

    const uint64_t object_size = iter.object_size();
    RunReadWithRetry(std::move(iter), req, promise, rdma_direction_mode)
        .start([](auto&&) {});

    ReadTaskHandle res;
    res.data_size = object_size;
    res.task_handle =
        FutureHandle<void>::Create(std::move(req), std::move(future));
    return res;
}

async_simple::coro::Lazy<bool> P2PClientService::RunForwardReadOnRoute(
    const ResolvedRoute& route, std::shared_ptr<RemoteReadRequest> req,
    std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
        promise,
    RouteIterator& iter, ErrorCode& final_result) {
    if (!data_manager_.has_value()) {
        LOG(ERROR) << "Forward RDMA read requires DataManager";
        promise->setValue(tl::expected<void, ErrorCode>(tl::unexpect, ErrorCode::INTERNAL_ERROR));
        co_return true;
    }
    if (!RemoteDestBuffersContiguous(req->dest_buffers)) {
        LOG(ERROR) << "Forward RDMA read requires contiguous dest buffers, key="
                   << req->key;
<<<<<<< HEAD
        promise->setValue(tl::expected<void, ErrorCode>(tl::unexpect, ErrorCode::INVALID_PARAMS));
=======
        tl::expected<void, ErrorCode> err2 =
            tl::make_unexpected(ErrorCode::NON_CONTIGUOUS_BUFFER_NOT_SUPPORTED);
        promise->setValue(std::move(err2));
>>>>>>> 92f08293 (add errorcode NON_CONTIGUOUS_BUFFER_NOT_SUPPORTED for non-contiguous buffers check)
        co_return true;
    }
    PinKeyRequest pin_req;
    pin_req.key = req->key;
    auto pin = co_await route.peer->AsyncPinKey(pin_req);
    if (!pin) {
        if (pin.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "AsyncPinKey failed, key=" << req->key
                       << ", error=" << pin.error();
        } else {
            final_result = pin.error();
        }
        iter.Evict(route);
        co_return false;
    }
    void* base = reinterpret_cast<void*>(req->dest_buffers[0].addr);
    size_t total = 0;
    for (const auto& d : req->dest_buffers) {
        total += d.size;
    }
    auto tr = data_manager_->TransferWithTeNoTierStaging(
        base, total, {pin.value().remote_buffer},
        Transport::TransferRequest::READ);
    if (!tr) {
        LOG(ERROR) << "Forward TE read failed, key=" << req->key
                   << ", error=" << tr.error();
        UnPinKeyRequest cleanup;
        cleanup.key = req->key;
        cleanup.read_operation_id = pin.value().read_operation_id;
        tl::expected<void, ErrorCode> cleanup_unpin;
        for (int attempt = 0; attempt < kForwardReadUnpinMaxAttempts;
             ++attempt) {
            cleanup_unpin = co_await route.peer->AsyncUnPinKey(cleanup);
            if (cleanup_unpin) {
                break;
            }
            if (UnPinErrorTreatAsEffectiveOk(cleanup_unpin.error())) {
                cleanup_unpin = tl::expected<void, ErrorCode>{};
                break;
            }
            if (attempt + 1 < kForwardReadUnpinMaxAttempts) {
                LOG(WARNING) << "AsyncUnPinKey retry after TE failure, key="
                             << req->key << ", attempt=" << (attempt + 1)
                             << ", error=" << cleanup_unpin.error();
            }
        }
        if (!cleanup_unpin) {
            LOG(ERROR) << "AsyncUnPinKey failed after TE read failure, key="
                         << req->key << ", error=" << cleanup_unpin.error();
        }
        iter.Evict(route);
        co_return false;
    }
    UnPinKeyRequest unpin_req;
    unpin_req.key = req->key;
    unpin_req.read_operation_id = pin.value().read_operation_id;
    tl::expected<void, ErrorCode> unpin_res;
    for (int attempt = 0; attempt < kForwardReadUnpinMaxAttempts; ++attempt) {
        unpin_res = co_await route.peer->AsyncUnPinKey(unpin_req);
        if (unpin_res) {
            break;
        }
        if (UnPinErrorTreatAsEffectiveOk(unpin_res.error())) {
            unpin_res = tl::expected<void, ErrorCode>{};
            break;
        }
        if (attempt + 1 < kForwardReadUnpinMaxAttempts) {
            LOG(WARNING) << "AsyncUnPinKey retry after forward read, key="
                         << req->key << ", attempt=" << (attempt + 1)
                         << ", error=" << unpin_res.error();
        }
    }
    if (!unpin_res) {
        LOG(ERROR) << "AsyncUnPinKey failed after forward read, key="
                   << req->key << ", error=" << unpin_res.error();
        final_result = unpin_res.error();
        iter.Evict(route);
        co_return false;
    }
    tl::expected<void, ErrorCode> ok;
    promise->setValue(std::move(ok));
    co_return true;
}

// Coroutine iterates route candidates and retries on failure.
async_simple::coro::Lazy<void> P2PClientService::RunReadWithRetry(
    RouteIterator iter, std::shared_ptr<RemoteReadRequest> req,
    std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
        promise,
    RdmaDirectionMode rdma_direction_mode) {
    ErrorCode final_result = ErrorCode::OBJECT_NOT_FOUND;
    try {
        while (auto route = co_await iter.AsyncNext()) {
            try {
                if (rdma_direction_mode == RdmaDirectionMode::FORWARD) {
                    if (co_await RunForwardReadOnRoute(*route, req, promise, iter,
                                                       final_result)) {
                        co_return;
                    }
                    continue;
                }

                auto result = co_await route->peer->AsyncReadRemoteData(*req);
                if (result.has_value()) {
                    tl::expected<void, ErrorCode> ok;
                    promise->setValue(std::move(ok));
                    co_return;
                } else {
                    if (result.error() != ErrorCode::OBJECT_NOT_FOUND) {
                        LOG(ERROR)
                            << "Failed to get from remote, key: " << req->key
                            << ", error: " << result.error()
                            << ", route: " << route->proxy.ip_address << ":"
                            << route->proxy.rpc_port
                            << ", client_id: " << route->proxy.client_id
                            << ", segment_id: " << route->proxy.segment_id
                            << ", is_cached: " << route->is_cached;
                    }
                    final_result = result.error();
                }
            } catch (const std::exception& e) {
                final_result = ErrorCode::INTERNAL_ERROR;
                LOG(ERROR) << "Failed to get from remote, key: " << req->key
                           << ", exception: " << e.what()
                           << ", route: " << route->proxy.ip_address << ":"
                           << route->proxy.rpc_port
                           << ", client_id: " << route->proxy.client_id
                           << ", segment_id: " << route->proxy.segment_id
                           << ", is_cached: " << route->is_cached;
            } catch (...) {
                final_result = ErrorCode::INTERNAL_ERROR;
                LOG(ERROR) << "Failed to get from remote, key: " << req->key
                           << ", unknown exception"
                           << ", route: " << route->proxy.ip_address << ":"
                           << route->proxy.rpc_port
                           << ", client_id: " << route->proxy.client_id
                           << ", segment_id: " << route->proxy.segment_id
                           << ", is_cached: " << route->is_cached;
            }
            iter.Evict(*route);
        }
        tl::expected<void, ErrorCode> err = tl::make_unexpected(final_result);
        promise->setValue(std::move(err));
    } catch (...) {
        tl::expected<void, ErrorCode> internal_err =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        promise->setValue(std::move(internal_err));
    }
}

// ============================================================================
// P2PClientService::RouteIterator
// ============================================================================

P2PClientService::RouteIterator::RouteIterator(
    std::string_view key, std::vector<ResolvedRoute> initial,
    uint64_t object_size, RouteCache* route_cache, MasterFetch master_fetch)
    : key_(key),
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

tl::expected<P2PClientService::RouteIterator, ErrorCode>
P2PClientService::BuildRouteIter(std::string_view key,
                                 const ReadConfigExt& config) {
    return BuildRouteIter(key, config, LoadCachedRoutes(key));
}

tl::expected<P2PClientService::RouteIterator, ErrorCode>
P2PClientService::BuildRouteIter(std::string_view key,
                                 const ReadConfigExt& config,
                                 std::vector<ResolvedRoute> pre_fetched) {
    auto routes = std::move(pre_fetched);
    uint64_t object_size = routes.empty() ? 0 : routes.front().object_size;
    RouteIterator iter(key, std::move(routes), object_size,
                       route_cache_ ? &(*route_cache_) : nullptr,
                       [this, key, config]() {
                           return AsyncResolveRoutesFromMaster(key, config);
                       });
    if (iter.empty()) {
        iter.Prime();
        if (iter.empty()) {
            return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
    }
    return iter;
}

async_simple::coro::Lazy<std::vector<P2PClientService::ResolvedRoute>>
P2PClientService::AsyncResolveRoutesFromMaster(std::string_view key,
                                               const ReadConfigExt& config) {
    auto replica_result =
        co_await master_client_.AsyncGetReplicaList(key, config.route_config);
    if (!replica_result) {
        if (replica_result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to query replica list, key=" << key
                       << ", error=" << replica_result.error();
        }
        co_return std::vector<ResolvedRoute>{};
    }
    auto routes = ReplicasToRoutes(replica_result.value().replicas);
    if (routes.empty()) {
        LOG(ERROR) << "Cannot determine size for key: " << key;
    }
    co_return routes;
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
    std::vector<std::string_view> miss_keys;

    // Batch local check
    for (size_t i = 0; i < keys.size(); ++i) {
        const bool local_hit =
            data_manager_.has_value() && data_manager_->Exist(keys[i]);
        if (local_hit) {
            results[i] = true;
        } else {
            miss_indices.push_back(i);
            miss_keys.emplace_back(keys[i]);
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
    const std::string& object_key, const ReadConfigExt& config) {
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
    auto result = master_client_.GetReplicaList(object_key, config.route_config);
    if (!result) {
        LOG(WARNING) << "fail to get replica list"
                     << ", key=" << object_key << ", error=" << result.error();
        return tl::unexpected(result.error());
    }

    return std::make_unique<QueryResult>(std::move(result.value().replicas));
}

std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
P2PClientService::BatchQuery(const std::vector<std::string>& object_keys,
                             const ReadConfigExt& config) {
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
    std::vector<std::string_view> key_views(object_keys.begin(),
                                            object_keys.end());
    auto responses =
        master_client_.BatchGetReplicaList(key_views, config.route_config);
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
