#include "p2p/client/centralized_client_service.h"

#include <glog/logging.h>

#include <csignal>
#include <algorithm>
#include <cassert>
#include <unordered_set>
#include <string_view>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <span>
#include <vector>
#include <memory>

#include <ylt/struct_json/json_reader.h>

#include "task_manager.h"
#include "transfer_engine.h"
#include "transfer_task.h"
#include "config.h"
#include "types.h"
#include "utils.h"
#include "local_hot_cache.h"
#include "storage/distributed/distributed_storage_backend.h"

namespace mooncake {

static constexpr int kPingIntervalMs = 1000;
static constexpr int kMaxPingFailures = 3;

CentralizedClientService::CentralizedClientService(
    const std::string& metadata_connstring, const std::string& protocol,
    uint16_t http_port, bool enable_http_server,
    const std::map<std::string, std::string>& labels,
    bool enable_metric_collection)
    : ClientService(metadata_connstring, http_port, enable_http_server, labels,
                    enable_metric_collection),
      metrics_(enable_metric_collection ? ClientMetric::Create(labels)
                                        : nullptr),
      protocol_(protocol),
      master_client_(nullptr),
      write_thread_pool_(2) {
    // runtime_config_store_ =
    //     std::make_unique<RuntimeConfigStore>(DeploymentMode::CENTRALIZATION);
}

CentralizedClientService::~CentralizedClientService() {
    Stop();
    Destroy();
}

std::optional<std::shared_ptr<CentralizedClientService>>
CentralizedClientService::Create(const CentralizedClientConfig& config) {
    auto svc = std::make_shared<CentralizedClientService>(
        config.metadata_connstring, config.protocol, config.http_port,
        config.enable_http_server, config.labels,
        config.enable_metric_collection);
    auto err = svc->Init(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to init CentralizedClientService: " << err;
        return std::nullopt;
    }
    return svc;
}

void CentralizedClientService::Stop() {
    {
        MutexLocker lk(&registration_mutex_);
        if (!MarkShuttingDown()) {
            return;
        }
    }
    ClientService::Stop();
}

void CentralizedClientService::Destroy() {
    std::vector<Segment> segments_to_unmount;
    {
        SharedMutexLocker lock(&mounted_segments_mutex_);
        segments_to_unmount.reserve(mounted_segments_.size());
        for (auto& entry : mounted_segments_) {
            segments_to_unmount.emplace_back(entry.second);
        }
    }

    for (auto& segment : segments_to_unmount) {
        if (segment.IsP2PSegment()) {
            LOG(ERROR) << "Segment " << segment.id << " is not centralized";
            continue;
        }
        auto result = InnerUnmountSegment(
            reinterpret_cast<void*>(segment.base), segment.size);
        if (!result) {
            LOG(ERROR) << "Failed to unmount segment: "
                       << toString(result.error());
        }
    }

    {
        SharedMutexLocker lock(&mounted_segments_mutex_);
        mounted_segments_.clear();
    }

    hugepage_segment_ptrs_.clear();
    segment_ptrs_.clear();
    ascend_segment_ptrs_.clear();

    ClientService::Destroy();
}

ErrorCode CentralizedClientService::Init(
    const CentralizedClientConfig& config) {
    master_server_entry_ = config.master_server_entry;

    auto mc = std::make_shared<MasterClient>(client_id_, nullptr, "default");
    master_client_ = std::make_unique<CentralizedMasterClient>(std::move(mc));

    ErrorCode err = ConnectToMaster(master_server_entry_);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to master: " << err;
        return err;
    }

    local_ip_ = config.local_ip;
    if (config.transfer_engine == nullptr) {
        err = InitTransferEngine(config.te_port, metadata_connstring_,
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

    InitTransferSubmitter();

    InitLocalBufferAllocator(config.local_buffer_size, config.protocol);

    auto reg = RegisterClient();
    if (!reg) {
        LOG(ERROR) << "Failed to register centralized client with master: "
                   << toString(reg.error());
        return reg.error();
    }

    if (config.global_segment_size == 0) {
        LOG(INFO) << "Global segment size is 0, skip mounting segment";
    } else if (config.protocol == "cxl") {
        size_t cxl_dev_size = 0;
        const char* env = std::getenv("MC_CXL_DEV_SIZE");
        if (env) {
            char* end = nullptr;
            unsigned long long val = strtoull(env, &end, 10);
            if (end != env && *end == '\0')
                cxl_dev_size = static_cast<size_t>(val);
        } else {
            LOG(FATAL) << "MC_CXL_DEV_SIZE not set";
            return ErrorCode::INVALID_PARAMS;
        }
        void* ptr = GetBaseAddr();
        LOG(INFO) << "Mounting CXL segment: " << cxl_dev_size << " bytes, "
                  << ptr;
        auto mount_result = MountSegment(ptr, cxl_dev_size, config.protocol);
        if (!mount_result.has_value()) {
            LOG(ERROR) << "Failed to mount CXL segment: "
                       << toString(mount_result.error());
            return mount_result.error();
        }
    } else {
        auto max_mr_size = globalConfig().max_mr_size;
        uint64_t total_glbseg_size = config.global_segment_size;
        uint64_t current_glbseg_size = 0;
        uint64_t remaining_size = config.global_segment_size;
        const bool use_hugepage =
            (std::getenv("MC_STORE_USE_HUGEPAGE") != nullptr);
        const bool should_use_hugepage =
            use_hugepage && config.protocol != "ascend";

        while (remaining_size > 0) {
            size_t segment_size =
                std::min(remaining_size, (uint64_t)max_mr_size);
            remaining_size -= segment_size;
            current_glbseg_size += segment_size;
            LOG(INFO) << "Mounting segment: " << segment_size << " bytes, "
                      << current_glbseg_size << " of " << total_glbseg_size;
            size_t mapped_size = segment_size;
            void* ptr = nullptr;
            if (should_use_hugepage) {
                mapped_size =
                    align_up(segment_size, get_hugepage_size_from_env());
                ptr = allocate_buffer_mmap_memory(mapped_size,
                                                  get_hugepage_size_from_env());
            } else {
                ptr = allocate_buffer_allocator_memory(segment_size,
                                                       config.protocol);
            }
            if (!ptr) {
                LOG(ERROR) << "Failed to allocate segment memory";
                return ErrorCode::INTERNAL_ERROR;
            }
            if (config.protocol == "ascend") {
                ascend_segment_ptrs_.emplace_back(ptr);
            } else if (should_use_hugepage) {
                hugepage_segment_ptrs_.emplace_back(
                    ptr, HugepageSegmentDeleter{mapped_size});
            } else {
                segment_ptrs_.emplace_back(ptr);
            }
            auto mount_result = MountSegment(ptr, mapped_size, config.protocol);
            if (!mount_result.has_value()) {
                LOG(ERROR) << "Failed to mount segment: "
                           << toString(mount_result.error());
                return mount_result.error();
            }
        }
    }

    StartKeepalive(master_server_entry_);

    StartHttpServer();

    // runtime_config_store_->loadFromJson(config.runtime_config_json);
    if (false) {
        LOG(ERROR) << "runtime config validation failed during startup, "
                   << "init aborted";
        return ErrorCode::INVALID_PARAMS;
    }

    if (metrics_) {
        metrics_->StartMetricsReportingThread();
    }

    return ErrorCode::OK;
}

void* CentralizedClientService::GetBaseAddr() {
    SharedMutexLocker lock(&mounted_segments_mutex_);
    if (mounted_segments_.empty()) {
        return nullptr;
    }
    return reinterpret_cast<void*>(mounted_segments_.begin()->second.base);
}

void CentralizedClientService::InitTransferSubmitter() {
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *transfer_engine_, storage_backend_, te_endpoint_,
        metrics_ ? &metrics_->transfer_metric : nullptr, 5);
}

void CentralizedClientService::StartKeepalive(const std::string& master_addr) {
    StopHeartbeat();
    ping_running_ = true;
    ping_thread_ = std::thread(&CentralizedClientService::PingThreadMain, this);
    LOG(INFO) << "Ping thread started for master: " << master_addr;
}

void CentralizedClientService::StopHeartbeat() {
    if (!ping_running_.exchange(false)) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(ping_mtx_);
    }
    ping_cv_.notify_all();
    if (ping_thread_.joinable()) {
        ping_thread_.join();
    }
    LOG(INFO) << "Ping thread stopped";
}

void CentralizedClientService::PingThreadMain() {
    int consecutive_failures = 0;

    // Execute one master-assigned replica task (copy/move). Mirrors
    // Client::ExecuteReplicaTransfer / Client::ExecuteTask. Implemented as
    // lambdas so no new class members are required.
    auto execute_replica_transfer =
        [this](const std::string& key, const std::string& action_name,
               const std::function<tl::expected<void, ErrorCode>()>& end_fn,
               const std::function<tl::expected<void, ErrorCode>()>& revoke_fn,
               const Replica::Descriptor& source,
               const std::vector<Replica::Descriptor>& targets)
        -> tl::expected<void, ErrorCode> {
        auto revoke_lambda = [&]() {
            auto revoke_result = revoke_fn();
            if (!revoke_result.has_value()) {
                LOG(WARNING) << "action=replica_" << action_name
                             << "_revoke_failed"
                             << ", key=" << key
                             << ", error_code=" << revoke_result.error();
            }
        };

        // currently only memory source replica is supported
        if (!source.is_memory_replica()) {
            revoke_lambda();
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!IsReplicaOnLocalMemory(source)) {
            revoke_lambda();
            return tl::unexpected(ErrorCode::REPLICA_NOT_IN_LOCAL_MEMORY);
        }

        const auto& buffer_descriptor =
            source.get_memory_descriptor().buffer_descriptor;
        void* buffer =
            reinterpret_cast<void*>(buffer_descriptor.buffer_address_);
        auto slices = split_into_slices(buffer, buffer_descriptor.size_);

        for (const auto& target : targets) {
            if (TransferWrite(target, slices) != ErrorCode::OK) {
                revoke_lambda();
                return tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }

        auto end_result = end_fn();
        if (!end_result.has_value()) {
            revoke_lambda();
            return tl::unexpected(end_result.error());
        }
        return {};
    };

    auto execute_assignment = [this, &execute_replica_transfer](
                                  const TaskAssignment& assignment) {
        ErrorCode result = ErrorCode::OK;
        try {
            switch (assignment.type) {
                case TaskType::REPLICA_COPY: {
                    ReplicaCopyPayload payload;
                    struct_json::from_json(payload, assignment.payload);
                    auto start_result = master_client_->CopyStart(
                        payload.key, payload.source, payload.targets);
                    if (!start_result.has_value()) {
                        result = start_result.error();
                        break;
                    }
                    const auto& response = start_result.value();
                    if (response.targets.empty()) {
                        // Target replicas already exist, consider it success.
                        auto end_result =
                            master_client_->CopyEnd(payload.key);
                        result =
                            end_result.has_value() ? ErrorCode::OK
                                                   : end_result.error();
                        break;
                    }
                    auto copy_result = execute_replica_transfer(
                        payload.key, "copy",
                        [this, &payload]() {
                            return master_client_->CopyEnd(payload.key);
                        },
                        [this, &payload]() {
                            return master_client_->CopyRevoke(payload.key);
                        },
                        response.source, response.targets);
                    result =
                        copy_result.has_value() ? ErrorCode::OK
                                                : copy_result.error();
                    break;
                }
                case TaskType::REPLICA_MOVE: {
                    ReplicaMovePayload payload;
                    struct_json::from_json(payload, assignment.payload);
                    auto start_result = master_client_->MoveStart(
                        payload.key, payload.source, payload.target);
                    if (!start_result.has_value()) {
                        result = start_result.error();
                        break;
                    }
                    const auto& response = start_result.value();
                    if (!response.target.has_value()) {
                        // Target already exists, consider it success.
                        auto end_result =
                            master_client_->MoveEnd(payload.key);
                        result =
                            end_result.has_value() ? ErrorCode::OK
                                                   : end_result.error();
                        break;
                    }
                    std::vector<Replica::Descriptor> targets = {
                        response.target.value()};
                    auto move_result = execute_replica_transfer(
                        payload.key, "move",
                        [this, &payload]() {
                            return master_client_->MoveEnd(payload.key);
                        },
                        [this, &payload]() {
                            return master_client_->MoveRevoke(payload.key);
                        },
                        response.source, targets);
                    result =
                        move_result.has_value() ? ErrorCode::OK
                                                : move_result.error();
                    break;
                }
                default:
                    LOG(ERROR) << "action=task_execution_failed"
                               << ", task_id=" << assignment.id
                               << ", error=unknown_task_type"
                               << ", task_type=" << assignment.type;
                    result = ErrorCode::INVALID_PARAMS;
                    break;
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "action=task_execution_failed"
                       << ", task_id=" << assignment.id
                       << ", error=exception, exception=" << e.what();
            result = ErrorCode::INTERNAL_ERROR;
        }

        TaskCompleteRequest complete_request;
        complete_request.id = assignment.id;
        complete_request.status = result == ErrorCode::OK
                                      ? TaskStatus::SUCCESS
                                      : TaskStatus::FAILED;
        complete_request.message =
            result == ErrorCode::OK
                ? "Task completed successfully"
                : "Task failed, error_code=" + std::to_string(toInt(result));
        auto complete_result = MarkTaskToComplete(complete_request);
        if (!complete_result.has_value()) {
            LOG(WARNING) << "action=task_complete_failed"
                         << ", task_id=" << assignment.id
                         << ", error_code=" << complete_result.error();
        }
    };

    while (ping_running_) {
        {
            std::unique_lock<std::mutex> lk(ping_mtx_);
            ping_cv_.wait_for(lk, std::chrono::milliseconds(kPingIntervalMs));
        }
        if (!ping_running_) break;

        auto ping_result = master_client_->Ping();
        if (!ping_result) {
            consecutive_failures++;
            LOG(WARNING) << "Ping failed (" << consecutive_failures
                         << "/" << kMaxPingFailures
                         << "): " << ping_result.error();
            if (consecutive_failures >= kMaxPingFailures) {
                LOG(ERROR) << "Too many ping failures, reconnecting...";
                auto conn_result = master_client_->Connect(master_server_entry_);
                if (!conn_result) {
                    LOG(ERROR) << "Reconnect failed: " << conn_result.error();
                }
                consecutive_failures = 0;
            }
            continue;
        }

        consecutive_failures = 0;
        auto& resp = ping_result.value();
        if (resp.client_status == ClientStatus::NEED_REMOUNT) {
            LOG(INFO) << "Master requested remount, re-registering...";
            auto reg = RegisterClient();
            if (!reg) {
                LOG(ERROR) << "Remount failed: " << toString(reg.error());
            }
        }

        // Poll for master-assigned replica tasks (copy/move) and execute
        // them on this keepalive thread, mirroring Client's task poll loop.
        constexpr size_t kTaskBatchSize = 16;
        auto fetch_result = FetchTasks(kTaskBatchSize);
        if (fetch_result.has_value()) {
            for (const auto& task_assignment : fetch_result.value()) {
                LOG(INFO) << "action=task_poll_success"
                          << ", task_id=" << task_assignment.id;
                execute_assignment(task_assignment);
            }
        } else {
            ErrorCode error = fetch_result.error();
            if (error != ErrorCode::RPC_FAIL) {
                LOG(WARNING) << "action=task_poll_failed"
                             << ", error_code=" << error;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

tl::expected<std::unique_ptr<QueryResult>, ErrorCode>
CentralizedClientService::Query(const std::string& object_key,
                                const ReadRouteConfig& config) {
    (void)config;
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto result = master_client_->GetReplicaList(object_key);
    if (!result) {
        LOG(ERROR) << "Failed to get replica list: " << result.error();
        return tl::unexpected(result.error());
    }
    uint64_t lease_ttl_ms = result.value().lease_ttl_ms;
    return std::make_unique<QueryResult>(
        std::move(result.value().replicas),
        start_time + std::chrono::milliseconds(lease_ttl_ms));
}

std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
CentralizedClientService::BatchQuery(
    const std::vector<std::string>& object_keys,
    const ReadRouteConfig& config) {
    (void)config;
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
            results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::SHUTTING_DOWN));
        }
        return results;
    }
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto response = master_client_->BatchGetReplicaList(object_keys);

    if (response.size() != object_keys.size()) {
        LOG(ERROR) << "BatchQuery response size mismatch. Expected: "
                   << object_keys.size() << ", Got: " << response.size();
        std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
            results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }
    std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>> results;
    results.reserve(response.size());
    for (size_t i = 0; i < response.size(); ++i) {
        if (response[i]) {
            uint64_t lease_ttl_ms = response[i].value().lease_ttl_ms;
            results.emplace_back(std::make_unique<QueryResult>(
                std::move(response[i].value().replicas),
                start_time + std::chrono::milliseconds(lease_ttl_ms)));
        } else {
            results.emplace_back(tl::unexpected(response[i].error()));
        }
    }
    return results;
}

// ---------------------------------------------------------------------------
// IsExist
// ---------------------------------------------------------------------------

tl::expected<bool, ErrorCode> CentralizedClientService::IsExist(
    const std::string& key) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = master_client_->ExistKey(key);
    if (!result) {
        LOG(ERROR) << "Failed to query key"
                   << ", key:" << key << ", error:" << result.error();
        return tl::unexpected(result.error());
    }
    return result;
}

std::vector<tl::expected<bool, ErrorCode>>
CentralizedClientService::BatchIsExist(const std::vector<std::string>& keys) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    auto results = master_client_->BatchExistKey(keys);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            LOG(ERROR) << "Failed to query key"
                       << ", key:" << keys[i]
                       << ", error:" << results[i].error();
        }
    }
    return results;
}

// ---------------------------------------------------------------------------
// Get (allocator-based)
// ---------------------------------------------------------------------------

tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>
CentralizedClientService::Get(const std::string& key,
                              std::shared_ptr<ClientBufferAllocator> allocator,
                              const ReadRouteConfig& config) {
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto query_result = Query(key, config);
    if (!query_result) {
        LOG(ERROR) << "Failed to query key"
                   << ", key:" << key << ", error:" << query_result.error();
        return tl::unexpected(query_result.error());
    }

    const auto& replica_list = query_result.value()->replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto& replica = replica_list[0];
    uint64_t total_size = calculate_total_size(replica);
    if (total_size == 0) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto alloc_result = allocator->allocate(total_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for get, key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto buffer_handle = std::move(*alloc_result);

    std::vector<Slice> slices;
    allocateSlices(slices, replica, buffer_handle.ptr());

    Replica::Descriptor preferred_replica;
    ErrorCode err =
        GetPreferredReplica(query_result.value()->replicas, preferred_replica);
    if (err != ErrorCode::OK) {
        return tl::unexpected(err);
    }
    err = TransferRead(preferred_replica, slices);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_failed key=" << key;
        return tl::unexpected(err);
    }
    if (query_result.value()->IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }

    return std::make_shared<BufferHandle>(std::move(buffer_handle));
}

std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
CentralizedClientService::BatchGet(
    const std::vector<std::string>& keys,
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

    auto query_results = BatchQuery(keys, config);

    struct KeyOp {
        size_t original_index;
        std::unique_ptr<QueryResult> query_result;
        std::unique_ptr<BufferHandle> buffer_handle;
        std::vector<Slice> slices;
    };
    std::vector<KeyOp> valid_ops;
    valid_ops.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        if (!query_results[i]) {
            auto error = query_results[i].error();
            if (error != ErrorCode::OBJECT_NOT_FOUND &&
                error != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "Query failed for key '" << keys[i]
                           << "': " << toString(error);
            }
            results[i] = tl::unexpected(query_results[i].error());
            continue;
        }

        auto query_ptr = std::move(query_results[i].value());
        if (query_ptr->replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        const auto& replica = query_ptr->replicas[0];
        uint64_t total_size = calculate_total_size(replica);
        if (total_size == 0) {
            LOG(ERROR) << "Empty replica list for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
            continue;
        }

        auto alloc_result = allocator->allocate(total_size);
        if (!alloc_result) {
            LOG(ERROR) << "Failed to allocate buffer for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        auto bh = std::make_unique<BufferHandle>(std::move(*alloc_result));
        std::vector<Slice> slices;
        allocateSlices(slices, replica, bh->ptr());

        valid_ops.push_back(
            {i, std::move(query_ptr), std::move(bh), std::move(slices)});
    }

    if (valid_ops.empty()) {
        return results;
    }

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        for (size_t i = 0; i < keys.size(); ++i) {
            if (!results[i].has_value()) {
                results[i] = tl::unexpected(ErrorCode::SHUTTING_DOWN);
            }
        }
        return results;
    }

    auto t0 = std::chrono::steady_clock::now();
    for (auto& op : valid_ops) {
        Replica::Descriptor preferred_replica;
        ErrorCode err =
            GetPreferredReplica(op.query_result->replicas, preferred_replica);
        if (err != ErrorCode::OK) {
            results[op.original_index] = tl::unexpected(err);
            continue;
        }
        err = TransferRead(preferred_replica, op.slices);
        if (err != ErrorCode::OK) {
            results[op.original_index] = tl::unexpected(err);
        } else {
            results[op.original_index] =
                std::make_shared<BufferHandle>(std::move(*op.buffer_handle));
        }
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_get_latency_us.observe(us);
    }

    return results;
}

// ---------------------------------------------------------------------------
// Get (buffer-based)
// ---------------------------------------------------------------------------

tl::expected<int64_t, ErrorCode> CentralizedClientService::Get(
    const std::string& key, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const ReadRouteConfig& config) {
    auto query_result = Query(key, config);
    if (!query_result) {
        LOG(ERROR) << "Failed to query key: " << key;
        return tl::unexpected(query_result.error());
    }

    const auto& replica_list = query_result.value()->replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto& replica = replica_list[0];
    uint64_t total_size = calculate_total_size(replica);
    if (total_size == 0) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    size_t provided_size = 0;
    for (auto s : sizes) provided_size += s;
    if (provided_size < total_size) {
        LOG(ERROR) << "Buffer too small for key '" << key
                   << "': required=" << total_size
                   << ", provided=" << provided_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<Slice> slices;
    size_t remaining = total_size;
    for (size_t j = 0; j < buffers.size() && remaining > 0; ++j) {
        size_t chunk = std::min(sizes[j], remaining);
        slices.push_back({buffers[j], chunk});
        remaining -= chunk;
    }
    Replica::Descriptor preferred_replica;
    ErrorCode err =
        GetPreferredReplica(query_result.value()->replicas, preferred_replica);
    if (err != ErrorCode::OK) {
        return tl::unexpected(err);
    }
    err = TransferRead(preferred_replica, slices);
    if (err != ErrorCode::OK) {
        return tl::unexpected(err);
    }
    if (query_result.value()->IsLeaseExpired()) {
        LOG(WARNING)
            << "lease_expired_before_data_transfer_completed key=" << key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }

    return static_cast<int64_t>(total_size);
}

std::vector<tl::expected<int64_t, ErrorCode>>
CentralizedClientService::BatchGet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReadRouteConfig& config, bool aggregate_same_segment_task) {
    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    auto query_results = BatchQuery(keys, config);

    std::vector<tl::expected<int64_t, ErrorCode>> results(keys.size());

    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        for (size_t i = 0; i < keys.size(); ++i) {
            results[i] = tl::unexpected(ErrorCode::SHUTTING_DOWN);
        }
        return results;
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        if (!query_results[i]) {
            auto error = query_results[i].error();
            results[i] = tl::unexpected(error);
            if (error != ErrorCode::OBJECT_NOT_FOUND &&
                error != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "Query failed for key '" << keys[i]
                           << "': " << toString(error);
            }
            continue;
        }

        auto query_ptr = std::move(query_results[i].value());
        if (query_ptr->replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        const auto& replica = query_ptr->replicas[0];
        uint64_t total_size = calculate_total_size(replica);
        size_t provided_size = 0;
        for (auto s : all_sizes[i]) provided_size += s;

        if (provided_size < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << keys[i]
                       << "': required=" << total_size
                       << ", available=" << provided_size;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        std::vector<Slice> slices;
        size_t remaining = total_size;
        for (size_t j = 0; j < all_buffers[i].size() && remaining > 0; ++j) {
            size_t chunk = std::min(all_sizes[i][j], remaining);
            slices.push_back({all_buffers[i][j], chunk});
            remaining -= chunk;
        }

        Replica::Descriptor preferred_replica;
        ErrorCode err =
            GetPreferredReplica(query_ptr->replicas, preferred_replica);
        if (err != ErrorCode::OK) {
            results[i] = tl::unexpected(err);
            continue;
        }

        err = TransferRead(preferred_replica, slices);
        if (err != ErrorCode::OK) {
            results[i] = tl::unexpected(err);
        } else if (query_ptr->IsLeaseExpired()) {
            LOG(WARNING)
                << "lease_expired_before_data_transfer_completed key="
                << keys[i];
            results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
        } else {
            results[i] = static_cast<int64_t>(total_size);
        }
    }

    return results;
}

// ---------------------------------------------------------------------------
// Put
// ---------------------------------------------------------------------------

tl::expected<void, ErrorCode> CentralizedClientService::Put(
    const ObjectKey& key, std::vector<Slice>& slices,
    const WriteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    const auto* replicate_config = std::get_if<ReplicateConfig>(&config);
    if (!replicate_config) {
        LOG(ERROR) << "CentralizedClientService currently only supports "
                      "ReplicateConfig";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    ReplicateConfig client_cfg = *replicate_config;
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_endpoint();
    }

    auto start_result = master_client_->PutStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
            VLOG(1) << "object_already_exists key=" << key;
            return {};
        }
        if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
            LOG(WARNING) << "Failed to start put operation for key=" << key
                         << PUT_NO_SPACE_HELPER_STR;
        } else {
            LOG(ERROR) << "Failed to start put operation for key=" << key
                       << ": " << toString(err);
        }
        return tl::unexpected(err);
    }

    auto t0_put = std::chrono::steady_clock::now();

    if (storage_backend_) {
        for (auto it = start_result.value().rbegin();
             it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                auto disk_descriptor = replica.get_disk_descriptor();
                PutToLocalFile(key, slices, disk_descriptor);
                break;
            }
        }
    }

    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica()) {
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                auto revoke_result =
                    master_client_->PutRevoke(key, ReplicaType::MEMORY);
                if (!revoke_result) {
                    LOG(ERROR) << "Failed to revoke put operation";
                    return tl::unexpected(revoke_result.error());
                }
                return tl::unexpected(transfer_err);
            }
        }
    }

    auto us_put = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_put)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us_put);
    }

    auto end_result = master_client_->PutEnd(ObjectMeta{key}, ReplicaType::MEMORY);
    if (!end_result) {
        ErrorCode err = end_result.error();
        LOG(ERROR) << "Failed to end put operation: " << err;
        return tl::unexpected(err);
    }

    return {};
}

// ---------------------------------------------------------------------------
// BatchPut
// ---------------------------------------------------------------------------

std::vector<tl::expected<void, ErrorCode>> CentralizedClientService::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteConfig& config) {
    const auto* replicate_config = std::get_if<ReplicateConfig>(&config);
    if (!replicate_config) {
        LOG(ERROR) << "CentralizedClientService currently only supports "
                      "ReplicateConfig";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        results.push_back(Put(keys[i], batched_slices[i], config));
    }
    return results;
}

// ---------------------------------------------------------------------------
// Remove
// ---------------------------------------------------------------------------

tl::expected<void, ErrorCode> CentralizedClientService::Remove(
    const ObjectKey& key, bool force) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = master_client_->Remove(key, force);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<long, ErrorCode> CentralizedClientService::RemoveByRegex(
    const ObjectKey& str, bool force) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = master_client_->RemoveByRegex(str, force);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return result.value();
}

tl::expected<long, ErrorCode> CentralizedClientService::RemoveAll(bool force) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return master_client_->RemoveAll(force);
}

// ---------------------------------------------------------------------------
// MountSegment / UnmountSegment
// ---------------------------------------------------------------------------

tl::expected<void, ErrorCode> CentralizedClientService::MountSegment(
    const void* buffer, size_t size, const std::string& protocol) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto check_result = CheckRegisterMemoryParams(buffer, size);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }

    SharedMutexLocker lock(&mounted_segments_mutex_);

    for (auto& it : mounted_segments_) {
        auto& mtseg = it.second;
        if (mtseg.IsP2PSegment()) {
            continue;
        }
        uintptr_t l1 = mtseg.base;
        uintptr_t r1 = reinterpret_cast<uintptr_t>(mtseg.size) + l1;
        uintptr_t l2 = reinterpret_cast<uintptr_t>(buffer);
        uintptr_t r2 = reinterpret_cast<uintptr_t>(size) + l2;
        if (std::max(l1, l2) < std::min(r1, r2)) {
            LOG(ERROR) << "segment_overlaps base1=" << mtseg.base
                       << " size1=" << mtseg.size << " base2=" << buffer
                       << " size2=" << size;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    int rc = transfer_engine_->registerLocalMemory(
        (void*)buffer, size, kWildcardLocation, true, true);
    if (rc != 0) {
        LOG(ERROR) << "register_local_memory_failed base=" << buffer
                   << " size=" << size << ", error=" << rc;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    Segment segment;
    segment.id = generate_uuid();
    segment.name = local_endpoint();
    segment.size = size;
    segment.base = reinterpret_cast<uintptr_t>(buffer);
    segment.protocol = protocol;
    segment.te_endpoint = get_te_endpoint();

    auto mount_result = master_client_->MountSegment(segment);
    if (!mount_result) {
        ErrorCode err = mount_result.error();
        LOG(ERROR) << "mount_segment_to_master_failed base=" << buffer
                   << " size=" << size << ", error=" << err;
        return tl::unexpected(err);
    }

    mounted_segments_[segment.id] = segment;
    return {};
}

tl::expected<void, ErrorCode> CentralizedClientService::UnmountSegment(
    const void* buffer, size_t size) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return InnerUnmountSegment(buffer, size);
}

tl::expected<void, ErrorCode> CentralizedClientService::InnerUnmountSegment(
    const void* buffer, size_t size) {
    SharedMutexLocker lock(&mounted_segments_mutex_);
    auto segment = mounted_segments_.end();

    for (auto it = mounted_segments_.begin(); it != mounted_segments_.end();
         ++it) {
        if (it->second.IsP2PSegment()) {
            LOG(ERROR) << "segment_not_found base=" << buffer
                       << " size=" << size;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (it->second.base == reinterpret_cast<uintptr_t>(buffer) &&
            it->second.size == size) {
            segment = it;
            break;
        }
    }
    if (segment == mounted_segments_.end()) {
        LOG(ERROR) << "segment_not_found base=" << buffer << " size=" << size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto unmount_result = master_client_->UnmountSegment(segment->second.id);
    if (!unmount_result) {
        ErrorCode err = unmount_result.error();
        LOG(ERROR) << "Failed to unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    int rc = transfer_engine_->unregisterLocalMemory(
        reinterpret_cast<void*>(segment->second.base));
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister transfer buffer with transfer "
                      "engine ret is "
                   << rc;
        if (rc != ERR_ADDRESS_NOT_REGISTERED) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    mounted_segments_.erase(segment);
    return {};
}

// ---------------------------------------------------------------------------
// Copy/Move Tasks
// ---------------------------------------------------------------------------

tl::expected<UUID, ErrorCode> CentralizedClientService::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    return master_client_->CreateCopyTask(key, targets);
}

tl::expected<UUID, ErrorCode> CentralizedClientService::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return master_client_->CreateMoveTask(key, source, target);
}

tl::expected<QueryTaskResponse, ErrorCode> CentralizedClientService::QueryTask(
    const UUID& task_id) {
    return master_client_->QueryTask(task_id);
}

tl::expected<std::vector<TaskAssignment>, ErrorCode>
CentralizedClientService::FetchTasks(size_t batch_size) {
    return master_client_->FetchTasks(batch_size);
}

tl::expected<void, ErrorCode> CentralizedClientService::MarkTaskToComplete(
    const TaskCompleteRequest& update_request) {
    return master_client_->MarkTaskToComplete(update_request);
}

// ---------------------------------------------------------------------------
// RegisterClient / InnerRegisterClient
// ---------------------------------------------------------------------------

tl::expected<RegisterClientResponse, ErrorCode>
CentralizedClientService::RegisterClient() {
    MutexLocker lk(&registration_mutex_);
    return InnerRegisterClient();
}

tl::expected<RegisterClientResponse, ErrorCode>
CentralizedClientService::InnerRegisterClient() {
    SharedMutexLocker lock(&mounted_segments_mutex_);

    auto ping_result = master_client_->Ping();
    if (!ping_result) {
        LOG(ERROR) << "Failed to ping master during registration: "
                   << ping_result.error();
        return tl::unexpected(ping_result.error());
    }

    for (auto& it : mounted_segments_) {
        auto& segment = it.second;
        auto mount_result = master_client_->MountSegment(segment);
        if (!mount_result) {
            LOG(ERROR) << "Failed to mount segment during registration: "
                       << toString(mount_result.error());
            return tl::unexpected(mount_result.error());
        }
    }

    RegisterClientResponse resp;
    resp.view_version = ping_result.value().view_version_id;
    return resp;
}

// ---------------------------------------------------------------------------
// Transfer helpers
// ---------------------------------------------------------------------------

ErrorCode CentralizedClientService::TransferData(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return ErrorCode::INVALID_PARAMS;
    }

    auto future =
        transfer_submitter_->submit(replica_descriptor, slices, op_code);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

ErrorCode CentralizedClientService::TransferWrite(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices) {
    return TransferData(replica_descriptor, slices, TransferRequest::WRITE);
}

ErrorCode CentralizedClientService::TransferRead(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices) {
    size_t total_size = 0;
    if (replica_descriptor.is_memory_replica()) {
        auto& mem_desc = replica_descriptor.get_memory_descriptor();
        total_size = mem_desc.buffer_descriptor.size_;
    } else {
        auto& disk_desc = replica_descriptor.get_disk_descriptor();
        total_size = disk_desc.object_size;
    }

    size_t slices_size = ClientService::CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    return TransferData(replica_descriptor, slices, TransferRequest::READ);
}

// ---------------------------------------------------------------------------
// GetPreferredReplica
// ---------------------------------------------------------------------------

ErrorCode CentralizedClientService::GetPreferredReplica(
    const std::vector<Replica::Descriptor>& replica_list,
    Replica::Descriptor& replica) {
    std::unordered_set<std::string> local_endpoints;
    {
        SharedMutexLocker lock(&mounted_segments_mutex_, shared_lock);
        local_endpoints.reserve(mounted_segments_.size());
        for (const auto& [uuid, seg] : mounted_segments_) {
            local_endpoints.insert(seg.te_endpoint);
        }
    }

    size_t first_complete_idx = replica_list.size();
    for (size_t i = 0; i < replica_list.size(); ++i) {
        const auto& rep = replica_list[i];
        if (rep.status != ReplicaStatus::COMPLETE) {
            continue;
        }
        if (first_complete_idx == replica_list.size()) {
            first_complete_idx = i;
        }
        if (rep.is_memory_replica() && !local_endpoints.empty()) {
            const auto& ep = rep.get_memory_descriptor()
                                 .buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(ep)) {
                replica = replica_list[i];
                return ErrorCode::OK;
            }
        }
    }

    if (first_complete_idx < replica_list.size()) {
        replica = replica_list[first_complete_idx];
        return ErrorCode::OK;
    }

    return ErrorCode::INVALID_REPLICA;
}

// ---------------------------------------------------------------------------
// Disk helpers (minimal implementation)
// ---------------------------------------------------------------------------

void CentralizedClientService::PrepareStorageBackend(
    const std::string& storage_root_dir, const std::string& fsdir,
    bool enable_eviction, uint64_t quota_bytes) {
    (void)storage_root_dir;
    (void)fsdir;
    (void)enable_eviction;
    (void)quota_bytes;
}

void CentralizedClientService::PutToLocalFile(
    const std::string& key, const std::vector<Slice>& slices,
    const DiskDescriptor& disk_descriptor) {
    (void)key;
    (void)slices;
    (void)disk_descriptor;
}

// ---------------------------------------------------------------------------
// Offloading / Batch
// ---------------------------------------------------------------------------

tl::expected<void, ErrorCode>
CentralizedClientService::MountLocalDiskSegment(bool enable_offloading) {
    if (!master_client_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return master_client_->MountLocalDiskSegment(client_id_, enable_offloading);
}

tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>
CentralizedClientService::OffloadObjectHeartbeat(bool enable_offloading) {
    (void)enable_offloading;
    return std::unordered_map<std::string, int64_t>{};
}

tl::expected<void, ErrorCode>
CentralizedClientService::NotifyOffloadSuccess(
    const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    if (!master_client_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return master_client_->NotifyOffloadSuccess(client_id_, keys, metadatas);
}

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedClientService::BatchReplicaClear(
    const std::vector<std::string>& object_keys,
    const UUID& client_id, const std::string& segment_name) {
    if (!master_client_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return master_client_->BatchReplicaClear(object_keys, client_id, segment_name);
}

tl::expected<bool, ErrorCode> CentralizedClientService::PollRemoveAll() {
    if (!master_client_) return tl::unexpected(ErrorCode::INVALID_PARAMS);
    return master_client_->PollRemoveAll();
}

tl::expected<void, ErrorCode>
CentralizedClientService::BatchGetOffloadObject(
    const std::vector<std::string>& keys,
    const std::vector<int64_t>& sizes) {
    return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
}

// ============================================================================
// Value-based Get (Slice-based)
// ============================================================================

tl::expected<void, ErrorCode> CentralizedClientService::Get(
    const std::string& key, std::vector<Slice>& slices,
    const ReadRouteConfig& config) {
    auto query_result = Query(key, config);
    if (!query_result) return tl::unexpected(query_result.error());
    if ((*query_result)->replicas.empty())
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    Replica::Descriptor replica;
    auto err = GetPreferredReplica((*query_result)->replicas, replica);
    if (err != ErrorCode::OK) return tl::unexpected(err);
    auto t0_get = std::chrono::steady_clock::now();
    err = TransferRead(replica, slices);
    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }
    if (err != ErrorCode::OK) return tl::unexpected(err);
    if ((*query_result)->IsLeaseExpired()) {
        LOG(WARNING)
            << "lease_expired_before_data_transfer_completed key=" << key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> CentralizedClientService::BatchGet(
    const std::vector<std::string>& keys,
    std::unordered_map<std::string, std::vector<Slice>>& slices,
    const ReadRouteConfig& config) {
    slices.clear();
    auto query_results = BatchQuery(keys, config);
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!query_results[i]) {
            results[i] = tl::unexpected(query_results[i].error());
            continue;
        }
        auto& qr = query_results[i].value();
        if (qr->replicas.empty()) {
            results[i] = tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
            continue;
        }
        Replica::Descriptor replica;
        auto err = GetPreferredReplica(qr->replicas, replica);
        if (err != ErrorCode::OK) {
            results[i] = tl::unexpected(err);
            continue;
        }
        const auto& first_replica = qr->replicas[0];
        uint64_t total_size = calculate_total_size(first_replica);
        std::vector<Slice> key_slices;
        allocateSlices(key_slices, first_replica, nullptr);
        for (auto& s : key_slices) {
            s.ptr = nullptr;
        }
        err = TransferRead(replica, key_slices);
        if (err != ErrorCode::OK) {
            results[i] = tl::unexpected(err);
        } else if (qr->IsLeaseExpired()) {
            results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
        } else {
            slices[keys[i]] = std::move(key_slices);
            results[i] = {};
        }
    }
    return results;
}

// ============================================================================
// QueryByRegex
// ============================================================================

tl::expected<std::vector<Replica>, ErrorCode>
CentralizedClientService::QueryByRegex(const std::string& str) {
    if (!master_client_) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return master_client_->GetReplicaListByRegex(str);
}

// ============================================================================
// VerifyChecksum — stub
// ============================================================================

tl::expected<void, ErrorCode> CentralizedClientService::VerifyObjectChecksum(
    const std::string& key, const std::vector<Slice>& slices,
    size_t object_size, std::optional<uint64_t> expected_checksum) {
    (void)key;
    (void)slices;
    (void)object_size;
    (void)expected_checksum;
    return tl::unexpected(ErrorCode::NOT_IMPLEMENTED);
}

// ============================================================================
// BatchRemove
// ============================================================================

std::vector<tl::expected<void, ErrorCode>> CentralizedClientService::BatchRemove(
    const std::vector<ObjectKey>& keys, bool force) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    std::vector<std::string> str_keys;
    str_keys.reserve(keys.size());
    for (auto& k : keys) str_keys.push_back(k);
    return master_client_->BatchRemove(str_keys, force);
}

// ============================================================================
// Eviction
// ============================================================================

tl::expected<void, ErrorCode> CentralizedClientService::EvictDiskReplica(
    const std::string& key, ReplicaType replica_type) {
    return master_client_->EvictDiskReplica(key, replica_type);
}

std::vector<tl::expected<void, ErrorCode>>
CentralizedClientService::BatchEvictDiskReplica(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    return master_client_->BatchEvictDiskReplica(keys, replica_type);
}

// ============================================================================
// Upsert — delegates to Put
// ============================================================================

tl::expected<void, ErrorCode> CentralizedClientService::Upsert(
    const ObjectKey& key, std::vector<Slice>& slices,
    const WriteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    const auto* replicate_config = std::get_if<ReplicateConfig>(&config);
    if (!replicate_config) {
        LOG(ERROR) << "CentralizedClientService currently only supports "
                      "ReplicateConfig";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    ReplicateConfig client_cfg = *replicate_config;
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_endpoint();
    }

    // Upsert replaces an existing object instead of failing with
    // OBJECT_ALREADY_EXISTS (which Put treats as a silent success).
    auto start_result =
        master_client_->UpsertStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
            LOG(WARNING) << "Failed to start upsert operation for key=" << key
                         << PUT_NO_SPACE_HELPER_STR;
        } else {
            LOG(ERROR) << "Failed to start upsert operation for key=" << key
                       << ": " << toString(err);
        }
        return tl::unexpected(err);
    }

    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica()) {
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                auto revoke_result =
                    master_client_->UpsertRevoke(key, ReplicaType::MEMORY);
                if (!revoke_result) {
                    LOG(ERROR) << "Failed to revoke upsert operation";
                    return tl::unexpected(revoke_result.error());
                }
                return tl::unexpected(transfer_err);
            }
        }
    }

    auto end_result =
        master_client_->UpsertEnd(ObjectMeta{key}, ReplicaType::MEMORY);
    if (!end_result) {
        LOG(ERROR) << "Failed to end upsert operation: " << end_result.error();
        return tl::unexpected(end_result.error());
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> CentralizedClientService::BatchUpsert(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteConfig& config) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        results.push_back(Upsert(keys[i], batched_slices[i], config));
    }
    return results;
}

// ============================================================================
// Register/Unregister memory
// ============================================================================

tl::expected<void, ErrorCode> CentralizedClientService::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    auto result = transfer_engine_->registerLocalMemory(
        addr, length, location, remote_accessible, update_metadata);
    if (result != 0) return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    return {};
}

tl::expected<void, ErrorCode> CentralizedClientService::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    auto result = transfer_engine_->unregisterLocalMemory(addr, update_metadata);
    if (result != 0) return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    return {};
}

// ============================================================================
// MountSegmentAndGetId / UnmountSegmentById
// ============================================================================

tl::expected<UUID, ErrorCode> CentralizedClientService::MountSegmentAndGetId(
    const void* buffer, size_t size, const std::string& protocol,
    const std::string& location) {
    auto mount_result = MountSegment(buffer, size, protocol);
    if (!mount_result) return tl::unexpected(mount_result.error());
    SharedMutexLocker lock(&mounted_segments_mutex_, shared_lock);
    for (auto& [uuid, seg] : mounted_segments_) {
        if (seg.base == reinterpret_cast<uintptr_t>(buffer) &&
            seg.size == size) {
            return uuid;
        }
    }
    return tl::unexpected(ErrorCode::INTERNAL_ERROR);
}

tl::expected<void, ErrorCode> CentralizedClientService::UnmountSegmentById(
    const UUID& segment_id, uint64_t grace_period_ms) {
    (void)grace_period_ms;
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    SharedMutexLocker lock(&mounted_segments_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto unmount_result = master_client_->UnmountSegment(segment_id);
    if (!unmount_result) return tl::unexpected(unmount_result.error());
    int rc = transfer_engine_->unregisterLocalMemory(
        reinterpret_cast<void*>(it->second.base));
    if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    mounted_segments_.erase(it);
    return {};
}

// ============================================================================
// BatchPut session methods
// ============================================================================

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
CentralizedClientService::StartBatchPutForSizes(
    const std::vector<std::string>& keys,
    const std::vector<uint64_t>& object_sizes,
    const ReplicateConfig& config) {
    std::vector<std::vector<uint64_t>> slice_lengths;
    slice_lengths.reserve(keys.size());
    for (auto sz : object_sizes) slice_lengths.push_back({sz});
    return master_client_->BatchPutStart(keys, slice_lengths, config);
}

std::vector<tl::expected<void, ErrorCode>> CentralizedClientService::BatchPutEnd(
    const std::vector<ObjectMeta>& object_metas, ReplicaType replica_type) {
    return master_client_->BatchPutEnd(object_metas, replica_type);
}

std::vector<tl::expected<void, ErrorCode>>
CentralizedClientService::BatchPutRevoke(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    return master_client_->BatchPutRevoke(keys, replica_type);
}

// ============================================================================
// SubmitScatter
// ============================================================================

namespace {

ErrorCode ScatterFragmentError(const Status& status) {
    return status.IsInvalidArgument() ? ErrorCode::INVALID_PARAMS
                                      : ErrorCode::TRANSFER_FAIL;
}

class ScatterRangeBuilder {
   public:
    explicit ScatterRangeBuilder(size_t fragment_count)
        : zero_offsets_(fragment_count, 0) {
        remote_offsets_.reserve(fragment_count);
        lengths_.reserve(fragment_count);
        ranges_.reserve(fragment_count);
    }

    void Add(TransferRequest::OpCode opcode,
             const AllocatedBuffer::Descriptor& handle, const Slice& slice,
             uint64_t remote_offset, std::optional<ErrorCode>* error_slot) {
        const size_t index = remote_offsets_.size();
        remote_offsets_.push_back(static_cast<size_t>(remote_offset));
        lengths_.push_back(slice.size);
        ranges_.push_back(TransferEngine::ScatterTransferRange{
            .opcode = opcode,
            .remote_segment = handle.transport_endpoint_,
            .remote_base_offset = handle.buffer_address_,
            .remote_size = static_cast<size_t>(handle.size_),
            .local_buffer = slice.ptr,
            .local_capacity = slice.size,
            .local_offsets = std::span<const size_t>(&zero_offsets_[index], 1),
            .remote_offsets =
                std::span<const size_t>(&remote_offsets_[index], 1),
            .lengths = std::span<const size_t>(&lengths_[index], 1),
            .on_fragment_complete =
                [error_slot](size_t, const Status& status) {
                    if (!status.ok() && !error_slot->has_value()) {
                        *error_slot = ScatterFragmentError(status);
                    }
                },
        });
    }

    bool empty() const { return ranges_.empty(); }

    const std::vector<TransferEngine::ScatterTransferRange>& ranges() const {
        return ranges_;
    }

   private:
    std::vector<size_t> zero_offsets_;
    std::vector<size_t> remote_offsets_;
    std::vector<size_t> lengths_;
    std::vector<TransferEngine::ScatterTransferRange> ranges_;
};

}  // namespace

/*
std::optional<TransferEngine::ScatterTransferOperation>
CentralizedClientService::SubmitScatter(
    const std::string& key, std::vector<Slice>& slices,
    const Replica::Descriptor& replica_descriptor, uint64_t src_offset) {
    (void)key;
    if (!transfer_submitter_) return std::nullopt;
    if (!replica_descriptor.is_memory_replica()) return std::nullopt;
    const auto& handle =
        replica_descriptor.get_memory_descriptor().buffer_descriptor;
    size_t fragment_count = slices.size();
    ScatterRangeBuilder builder(fragment_count);
    std::optional<ErrorCode> error_slot;
    uint64_t cumulative_offset = src_offset;
    for (size_t j = 0; j < slices.size(); ++j) {
        builder.Add(TransferRequest::READ, handle, slices[j],
                    cumulative_offset, &error_slot);
        cumulative_offset += slices[j].size;
    }
    return transfer_submitter_->submitScatter(builder.ranges());
}
*/

// ============================================================================
// Transfer methods
// ============================================================================

ErrorCode CentralizedClientService::TransferWriteRange(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t dst_offset) {
    if (!transfer_submitter_) return ErrorCode::INVALID_PARAMS;
    auto future =
        transfer_submitter_->submitRangeWrite(replica_descriptor, slices,
                                                dst_offset);
    if (!future) return ErrorCode::TRANSFER_FAIL;
    return future->get();
}

std::vector<tl::expected<int64_t, ErrorCode>>
CentralizedClientService::BatchTransferReadRanges(
    const std::vector<Replica::Descriptor>& replicas,
    std::vector<std::vector<Slice>>& all_slices,
    const std::vector<std::vector<uint64_t>>& src_offsets) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas.size() != all_slices.size() ||
        replicas.size() != src_offsets.size()) {
        return results;
    }
    if (!transfer_submitter_) return results;

    size_t fragment_count = 0;
    for (const auto& entry : all_slices) fragment_count += entry.size();

    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(replicas.size());
    for (size_t i = 0; i < replicas.size(); ++i) {
        if (all_slices[i].size() != src_offsets[i].size()) continue;
        if (!replicas[i].is_memory_replica()) continue;
        const auto& handle =
            replicas[i].get_memory_descriptor().buffer_descriptor;
        int64_t transferred = 0;
        for (size_t j = 0; j < all_slices[i].size(); ++j) {
            builder.Add(TransferRequest::READ, handle, all_slices[i][j],
                        src_offsets[i][j], &entry_errors[i]);
            transferred += static_cast<int64_t>(all_slices[i][j].size);
        }
        results[i] = transferred;
    }

    if (builder.empty()) return results;

    auto operation = transfer_submitter_->submitScatter(builder.ranges());
    operation.wait();

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) continue;
        results[i] = tl::unexpected(entry_errors[i].value());
    }
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>>
CentralizedClientService::BatchTransferWriteRanges(
    const std::vector<Replica::Descriptor>& replicas,
    std::vector<std::vector<Slice>>& all_slices,
    const std::vector<std::vector<uint64_t>>& dst_offsets) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas.size() != all_slices.size() ||
        replicas.size() != dst_offsets.size()) {
        return results;
    }
    if (!transfer_submitter_) return results;

    size_t fragment_count = 0;
    for (const auto& entry : all_slices) fragment_count += entry.size();

    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(replicas.size());
    for (size_t i = 0; i < replicas.size(); ++i) {
        if (all_slices[i].size() != dst_offsets[i].size()) continue;
        if (!replicas[i].is_memory_replica()) continue;
        const auto& handle =
            replicas[i].get_memory_descriptor().buffer_descriptor;
        int64_t transferred = 0;
        for (size_t j = 0; j < all_slices[i].size(); ++j) {
            builder.Add(TransferRequest::WRITE, handle, all_slices[i][j],
                        dst_offsets[i][j], &entry_errors[i]);
            transferred += static_cast<int64_t>(all_slices[i][j].size);
        }
        results[i] = transferred;
    }

    if (builder.empty()) return results;

    auto operation = transfer_submitter_->submitScatter(builder.ranges());
    operation.wait();

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) continue;
        results[i] = tl::unexpected(entry_errors[i].value());
    }
    return results;
}

// ============================================================================
// Metrics & Accessors
// ============================================================================

tl::expected<std::string, ErrorCode>
CentralizedClientService::GetSummaryMetrics() {
    if (metrics_ == nullptr)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return metrics_->summary_metrics();
}

CacheStats CentralizedClientService::CalcCacheStats() {
    return CacheStats{};
}

tl::expected<std::string, ErrorCode>
CentralizedClientService::SerializeMetrics() {
    if (metrics_ == nullptr)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    std::string str;
    metrics_->serialize(str);
    return str;
}

void CentralizedClientService::ObserveTransferOperation(
    TransferOperationKind kind, const std::string& op_name, uint64_t bytes,
    uint64_t latency_us) {
    if (metrics_ != nullptr)
        metrics_->ObserveTransferOperation(kind, op_name, bytes, latency_us);
}

SsdMetric* CentralizedClientService::GetSsdMetricPtr() {
    return metrics_ ? &metrics_->ssd_metric : nullptr;
}

std::vector<int> CentralizedClientService::GetNicNumaNodes() const {
    return {};
}

tl::expected<Replica::Descriptor, ErrorCode>
CentralizedClientService::GetPreferredReplica(
    const std::vector<Replica::Descriptor>& replica_list) {
    Replica::Descriptor replica;
    auto err = GetPreferredReplica(replica_list, replica);
    if (err != ErrorCode::OK) return tl::unexpected(err);
    return replica;
}

bool CentralizedClientService::IsReplicaOnLocalMemory(
    const Replica::Descriptor& replica) {
    if (!replica.is_memory_replica()) return false;
    auto& mem_desc = replica.get_memory_descriptor();
    const auto address = mem_desc.buffer_descriptor.buffer_address_;
    const auto size = mem_desc.buffer_descriptor.size_;
    // A replica handle is a sub-range of a mounted segment, so check whether
    // it falls inside any locally mounted segment (an exact base/size match
    // only covers whole-segment replicas).
    SharedMutexLocker lock(&mounted_segments_mutex_, shared_lock);
    for (auto& [uuid, seg] : mounted_segments_) {
        if (address >= seg.base && size <= seg.size &&
            address <= seg.base + (seg.size - size)) {
            return true;
        }
    }
    return false;
}

tl::expected<void, ErrorCode> CentralizedClientService::ReportSsdCapacity(
    int64_t ssd_total_capacity_bytes) {
    return master_client_->ReportSsdCapacity(client_id_,
                                              ssd_total_capacity_bytes);
}

// ============================================================================
// Promotion — delegates to master_client_
// ============================================================================

tl::expected<void, ErrorCode>
CentralizedClientService::PromotionObjectHeartbeat(
    std::vector<PromotionTaskItem>& promotion_objects) {
    auto result = master_client_->PromotionObjectHeartbeat(client_id_);
    if (!result) return tl::unexpected(result.error());
    promotion_objects = std::move(result.value());
    return {};
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
CentralizedClientService::PromotionAllocStart(
    const std::string& key, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return master_client_->PromotionAllocStart(client_id_, key, size,
                                                preferred_segments);
}

tl::expected<void, ErrorCode> CentralizedClientService::NotifyPromotionSuccess(
    const std::string& key) {
    return master_client_->NotifyPromotionSuccess(client_id_, key);
}

tl::expected<void, ErrorCode> CentralizedClientService::NotifyPromotionFailure(
    const std::string& key) {
    return master_client_->NotifyPromotionFailure(client_id_, key);
}

ErrorCode CentralizedClientService::PromotionWrite(
    const Replica::Descriptor& memory_descriptor,
    std::vector<Slice>& slices) {
    (void)memory_descriptor;
    (void)slices;
    return ErrorCode::NOT_IMPLEMENTED;
}

// ============================================================================
// Offload (complex version) — stub
// ============================================================================

tl::expected<void, ErrorCode> CentralizedClientService::BatchGetOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_slices) {
    auto future = transfer_submitter_->submit_batch_get_offload_object(
        transfer_engine_addr, keys, pointers, batch_slices,
        OffloadBufferAccess::kTransferEngine);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }
    VLOG(1) << "Using transfer strategy: " << future->strategy();
    auto result = future->get();
    if (result != ErrorCode::OK) {
        LOG(ERROR) << "Transfer failed, error code is " << result;
        return tl::make_unexpected(result);
    }
    return {};
}

}  // namespace mooncake