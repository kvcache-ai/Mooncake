#include "client.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <optional>
#include <ranges>

#include "transfer_engine.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "types.h"

namespace mooncake {

[[nodiscard]] size_t CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

[[nodiscard]] size_t CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

Client::Client(const std::string& local_hostname,
               const std::string& metadata_connstring,
               const std::string& storage_root_dir)
    : local_hostname_(local_hostname),
      metadata_connstring_(metadata_connstring),
      storage_root_dir_(storage_root_dir),
      write_thread_pool_(2) {
    client_id_ = generate_uuid();
    LOG(INFO) << "client_id=" << client_id_;
}

Client::~Client() {
    // Make a copy of mounted_segments_ to avoid modifying while iterating
    std::vector<Segment> segments_to_unmount;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        segments_to_unmount.reserve(mounted_segments_.size());
        for (auto& entry : mounted_segments_) {
            segments_to_unmount.emplace_back(entry.second);
        }
    }

    for (auto& segment : segments_to_unmount) {
        auto result =
            UnmountSegment(reinterpret_cast<void*>(segment.base), segment.size);
        if (!result) {
            LOG(ERROR) << "Failed to unmount segment: "
                       << toString(result.error());
        }
    }

    // Clear any remaining segments
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_.clear();
    }

    // Stop ping thread only after no need to contact master anymore
    if (ping_running_) {
        ping_running_ = false;
        if (ping_thread_.joinable()) {
            ping_thread_.join();
        }
    }
}

static bool get_auto_discover() {
    const char* ev_ad = std::getenv("MC_MS_AUTO_DISC");
    if (ev_ad) {
        int iv = std::stoi(ev_ad);
        if (iv == 1) {
            LOG(INFO) << "auto discovery set by env MC_MS_AUTO_DISC";
            return true;
        }
    }
    return false;
}

static inline void ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                return !std::isspace(ch);
            }));
}

static inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(),
                         [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            s.end());
}

static std::vector<std::string> get_auto_discover_filters(bool auto_discover) {
    std::vector<std::string> whitelst_filters;
    char* ev_ad = std::getenv("MC_MS_FILTERS");
    if (ev_ad) {
        if (!auto_discover) {
            LOG(WARNING)
                << "auto discovery not set, but find whitelist filters: "
                << ev_ad;
            return whitelst_filters;
        }
        LOG(INFO) << "whitelist filters: " << ev_ad;
        char delimiter = ',';
        char* end = ev_ad + std::strlen(ev_ad);
        char *start = ev_ad, *pos = ev_ad;
        while ((pos = std::find(start, end, delimiter)) != end) {
            std::string str(start, pos);
            ltrim(str);
            rtrim(str);
            whitelst_filters.emplace_back(std::move(str));
            start = pos + 1;
        }
        if (start != (end + 1)) {
            std::string str(start, end);
            ltrim(str);
            rtrim(str);
            whitelst_filters.emplace_back(std::move(str));
        }
    }
    return whitelst_filters;
}

ErrorCode Client::ConnectToMaster(const std::string& master_server_entry) {
    if (master_server_entry.find("etcd://") == 0) {
        std::string etcd_entry = master_server_entry.substr(strlen("etcd://"));

        // Get master address from etcd
        auto err = master_view_helper_.ConnectToEtcd(etcd_entry);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd";
            return err;
        }
        std::string master_address;
        ViewVersionId master_version = 0;
        err = master_view_helper_.GetMasterView(master_address, master_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get master address";
            return err;
        }

        err = master_client_.Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        // Start Ping thread to monitor master view changes and remount segments
        // if needed
        ping_running_ = true;
        ping_thread_ = std::thread(&Client::PingThreadFunc, this);

        return ErrorCode::OK;
    } else {
        return master_client_.Connect(master_server_entry);
    }
}

ErrorCode Client::InitTransferEngine(const std::string& local_hostname,
                                     const std::string& metadata_connstring,
                                     const std::string& protocol,
                                     void** protocol_args) {
    // get auto_discover and filters from env
    bool auto_discover = get_auto_discover();
    transfer_engine_.setAutoDiscover(auto_discover);
    transfer_engine_.setWhitelistFilters(
        get_auto_discover_filters(auto_discover));

    auto [hostname, port] = parseHostNameWithPort(local_hostname);
    int rc = transfer_engine_.init(metadata_connstring, local_hostname,
                                   hostname, port);
    CHECK_EQ(rc, 0) << "Failed to initialize transfer engine";

    Transport* transport = nullptr;
    if (protocol == "rdma") {
        LOG(INFO) << "transport_type=rdma";
        transport = transfer_engine_.installTransport("rdma", protocol_args);
    } else if (protocol == "tcp") {
        LOG(INFO) << "transport_type=tcp";
        try {
            transport = transfer_engine_.installTransport("tcp", protocol_args);
        } catch (std::exception& e) {
            LOG(ERROR) << "tcp_transport_install_failed error_message=\""
                       << e.what() << "\"";
            return ErrorCode::INTERNAL_ERROR;
        }
    } else {
        LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
        return ErrorCode::INVALID_PARAMS;
    }
    CHECK(transport) << "Failed to install transport";

    // Initialize TransferSubmitter after transfer engine is ready
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        transfer_engine_, local_hostname, storage_backend_);

    return ErrorCode::OK;
}

std::optional<std::shared_ptr<Client>> Client::Create(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol, void** protocol_args,
    const std::string& master_server_entry) {
    // If MOONCAKE_STORAGE_ROOT_DIR is set, use it as the storage root directory
    std::string storage_root_dir =
        std::getenv("MOONCAKE_STORAGE_ROOT_DIR")
            ? std::getenv("MOONCAKE_STORAGE_ROOT_DIR")
            : "";

    auto client = std::shared_ptr<Client>(
        new Client(local_hostname, metadata_connstring, storage_root_dir));

    ErrorCode err = client->ConnectToMaster(master_server_entry);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    // Initialize storage backend if storage_root_dir is provided
    auto response = client->master_client_.GetFsdir();
    if (!response) {
        LOG(ERROR) << "Failed to get fsdir from master";
    } else if (storage_root_dir.empty()) {
        LOG(INFO) << "Storage root directory is not set. persisting data is "
                     "disabled.";
    } else {
        LOG(INFO) << "Storage root directory is: " << storage_root_dir;
        // Initialize storage backend
        client->PrepareStorageBackend(storage_root_dir, response.value());
    }

    // Initialize transfer engine
    err = client->InitTransferEngine(local_hostname, metadata_connstring,
                                     protocol, protocol_args);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize transfer engine";
        return std::nullopt;
    }

    return client;
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          std::vector<Slice>& slices) {
    auto query_result = Query(object_key);
    if (!query_result) {
        return tl::unexpected(query_result.error());
    }
    return Get(object_key, query_result.value(), slices);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    auto batched_query_results = BatchQuery(object_keys);

    // If any queries failed, return error results immediately for failed
    // queries
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_keys.size());

    std::vector<std::vector<Replica::Descriptor>> valid_replica_lists;
    std::vector<size_t> valid_indices;
    std::vector<std::string> valid_keys;

    for (size_t i = 0; i < batched_query_results.size(); ++i) {
        if (batched_query_results[i]) {
            valid_replica_lists.emplace_back(batched_query_results[i].value());
            valid_indices.emplace_back(i);
            valid_keys.emplace_back(object_keys[i]);
            results.emplace_back();  // placeholder for successful results
        } else {
            results.emplace_back(
                tl::unexpected(batched_query_results[i].error()));
        }
    }

    // If we have any valid queries, process them
    if (!valid_keys.empty()) {
        std::unordered_map<std::string, std::vector<Slice>> valid_slices;
        for (const auto& key : valid_keys) {
            auto it = slices.find(key);
            if (it != slices.end()) {
                valid_slices[key] = it->second;
            }
        }

        auto valid_results =
            BatchGet(valid_keys, valid_replica_lists, valid_slices);

        // Merge results back
        for (size_t i = 0; i < valid_indices.size(); ++i) {
            results[valid_indices[i]] = valid_results[i];
        }
    }

    return results;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode> Client::Query(
    const std::string& object_key) {
    auto result = master_client_.GetReplicaList(object_key);
    if (!result) {
        // Check storage backend if master query fails
        if (storage_backend_) {
            if (auto desc_opt = storage_backend_->Querykey(object_key)) {
                return std::vector<Replica::Descriptor>{std::move(*desc_opt)};
            }
        }
        return tl::unexpected(result.error());
    }
    return result.value();
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
Client::BatchQuery(const std::vector<std::string>& object_keys) {
    auto response = master_client_.BatchGetReplicaList(object_keys);

    // Check if we got the expected number of responses
    if (response.size() != object_keys.size()) {
        LOG(ERROR) << "BatchQuery response size mismatch. Expected: "
                   << object_keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
            results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    // For failed queries, check storage backend if available
    if (storage_backend_) {
        for (size_t i = 0; i < response.size(); ++i) {
            if (!response[i]) {
                if (auto desc_opt =
                        storage_backend_->Querykey(object_keys[i])) {
                    response[i] =
                        std::vector<Replica::Descriptor>{std::move(*desc_opt)};
                }
            }
        }
    }

    return response;
}

tl::expected<void, ErrorCode> Client::Get(
    const std::string& object_key,
    const std::vector<Replica::Descriptor>& replica_list,
    std::vector<Slice>& slices) {
    // Find the first complete replica
    Replica::Descriptor replica;
    ErrorCode err = FindFirstCompleteReplica(replica_list, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }

    err = TransferRead(replica, slices);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_failed key=" << object_key;
        return tl::unexpected(err);
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    const std::vector<std::vector<Replica::Descriptor>>& replica_lists,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    CHECK(transfer_submitter_) << "TransferSubmitter not initialized";

    // Validate input size consistency
    if (replica_lists.size() != object_keys.size()) {
        LOG(ERROR) << "Replica lists size (" << replica_lists.size()
                   << ") doesn't match object keys size (" << object_keys.size()
                   << ")";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    // Collect all transfer operations for parallel execution
    std::vector<std::tuple<size_t, std::string, TransferFuture>>
        pending_transfers;
    std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());

    // Submit all transfers in parallel
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& replica_list = replica_lists[i];

        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        // Find the first complete replica for this key
        Replica::Descriptor replica;
        ErrorCode err = FindFirstCompleteReplica(replica_list, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }

        // Submit transfer operation asynchronously
        auto future = transfer_submitter_->submit(replica, slices_it->second,
                                                  TransferRequest::READ);
        if (!future) {
            LOG(ERROR) << "Failed to submit transfer operation for key: "
                       << key;
            results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            continue;
        }

        VLOG(1) << "Submitted transfer for key " << key
                << " using strategy: " << static_cast<int>(future->strategy());

        pending_transfers.emplace_back(i, key, std::move(*future));
    }

    // Wait for all transfers to complete
    for (auto& [index, key, future] : pending_transfers) {
        ErrorCode result = future.get();
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Transfer failed for key: " << key
                       << " with error: " << static_cast<int>(result);
            results[index] = tl::unexpected(result);
        } else {
            VLOG(1) << "Transfer completed successfully for key: " << key;
            results[index] = {};
        }
    }

    VLOG(1) << "BatchGet completed for " << object_keys.size() << " keys";
    return results;
}

tl::expected<void, ErrorCode> Client::Put(const ObjectKey& key,
                                          std::vector<Slice>& slices,
                                          const ReplicateConfig& config) {
    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    size_t slice_size = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
        slice_size += slices[i].size;
    }

    // Start put operation
    auto start_result =
        master_client_.PutStart(key, slice_lengths, slice_size, config);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
            VLOG(1) << "object_already_exists key=" << key;
            return {};
        }
        LOG(ERROR) << "Failed to start put operation: " << err;
        return tl::unexpected(err);
    }

    // Transfer data using allocated handles from all replicas
    for (const auto& replica : start_result.value()) {
        ErrorCode transfer_err = TransferWrite(replica, slices);
        if (transfer_err != ErrorCode::OK) {
            // Revoke put operation
            auto revoke_result = master_client_.PutRevoke(key);
            if (!revoke_result) {
                LOG(ERROR) << "Failed to revoke put operation";
                return tl::unexpected(revoke_result.error());
            }
            return tl::unexpected(transfer_err);
        }
    }

    // End put operation
    auto end_result = master_client_.PutEnd(key);
    if (!end_result) {
        ErrorCode err = end_result.error();
        LOG(ERROR) << "Failed to end put operation: " << err;
        return tl::unexpected(err);
    }

    // Store to local file if storage backend is available
    PutToLocalFile(key, slices);

    return {};
}

// TODO: `client.cpp` is too long, consider split it into multiple files
enum class PutOperationState {
    PENDING,
    MASTER_FAILED,
    TRANSFER_FAILED,
    FINALIZE_FAILED,
    SUCCESS
};

class PutOperation {
   public:
    PutOperation(std::string_view k, const std::vector<Slice>& s)
        : key(k), slices(s) {
        value_length = CalculateSliceSize(slices);
        // Initialize with a pending error state to ensure result is always set
        result = tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::string key;
    std::vector<Slice> slices;
    size_t value_length;

    // Enhanced state tracking
    PutOperationState state = PutOperationState::PENDING;
    tl::expected<void, ErrorCode> result;
    std::vector<Replica::Descriptor> replicas;
    std::vector<TransferFuture> pending_transfers;

    // Error context for debugging
    std::optional<std::string> failure_context;

    // Helper methods for robust state management
    void SetSuccess() {
        state = PutOperationState::SUCCESS;
        result = {};
        failure_context.reset();
    }

    void SetError(ErrorCode error, const std::string& context = "") {
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = context;
        }

        // Update state based on current processing stage
        if (replicas.empty()) {
            state = PutOperationState::MASTER_FAILED;
        } else if (pending_transfers.empty()) {
            state = PutOperationState::TRANSFER_FAILED;
        } else {
            state = PutOperationState::FINALIZE_FAILED;
        }
    }

    bool IsResolved() const { return state != PutOperationState::PENDING; }

    bool IsSuccessful() const {
        return state == PutOperationState::SUCCESS && result.has_value();
    }
};

std::vector<PutOperation> Client::CreatePutOperations(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    std::vector<PutOperation> ops;
    ops.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ops.emplace_back(keys[i], batched_slices[i]);
    }
    return ops;
}

void Client::StartBatchPut(std::vector<PutOperation>& ops,
                           const ReplicateConfig& config) {
    std::vector<std::string> keys;
    std::vector<size_t> value_lengths;
    std::vector<std::vector<uint64_t>> slice_lengths;

    keys.reserve(ops.size());
    value_lengths.reserve(ops.size());
    slice_lengths.reserve(ops.size());

    for (const auto& op : ops) {
        keys.emplace_back(op.key);
        value_lengths.emplace_back(op.value_length);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    auto start_responses = master_client_.BatchPutStart(keys, value_lengths,
                                                        slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != ops.size()) {
        LOG(ERROR) << "BatchPutStart response size mismatch: expected "
                   << ops.size() << ", got " << start_responses.size();
        for (auto& op : ops) {
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchPutStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    for (size_t i = 0; i < ops.size(); ++i) {
        if (!start_responses[i]) {
            ops[i].SetError(start_responses[i].error(),
                            "Master failed to start put operation");
        } else {
            ops[i].replicas = start_responses[i].value();
            // Operation continues to next stage - result remains INTERNAL_ERROR
            // until fully successful
            VLOG(1) << "Successfully started put for key " << ops[i].key
                    << " with " << ops[i].replicas.size() << " replicas";
        }
    }
}

void Client::SubmitTransfers(std::vector<PutOperation>& ops) {
    CHECK(transfer_submitter_) << "TransferSubmitter not initialized";

    for (auto& op : ops) {
        // Skip operations that already failed in previous stages
        if (op.IsResolved()) {
            continue;
        }

        // Skip operations that don't have replicas (failed in StartBatchPut)
        if (op.replicas.empty()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "No replicas available for transfer");
            continue;
        }

        bool all_transfers_submitted = true;
        std::string failure_context;

        for (size_t replica_idx = 0; replica_idx < op.replicas.size();
             ++replica_idx) {
            const auto& replica = op.replicas[replica_idx];

            auto submit_result = transfer_submitter_->submit(
                replica, op.slices, TransferRequest::WRITE);

            if (!submit_result) {
                failure_context = "Failed to submit transfer for replica " +
                                  std::to_string(replica_idx);
                all_transfers_submitted = false;
                break;
            }

            op.pending_transfers.emplace_back(std::move(submit_result.value()));
        }

        if (!all_transfers_submitted) {
            LOG(ERROR) << "Transfer submission failed for key " << op.key
                       << ": " << failure_context;
            op.SetError(ErrorCode::TRANSFER_FAIL, failure_context);
            op.pending_transfers.clear();
        } else {
            VLOG(1) << "Successfully submitted " << op.pending_transfers.size()
                    << " transfers for key " << op.key;
        }
    }
}

void Client::WaitForTransfers(std::vector<PutOperation>& ops) {
    for (auto& op : ops) {
        // Skip operations that already failed or completed
        if (op.IsResolved()) {
            continue;
        }

        // Skip operations with no pending transfers (failed in SubmitTransfers)
        if (op.pending_transfers.empty()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "No pending transfers to wait for");
            continue;
        }

        bool all_transfers_succeeded = true;
        ErrorCode first_error = ErrorCode::OK;
        size_t failed_transfer_idx = 0;

        for (size_t i = 0; i < op.pending_transfers.size(); ++i) {
            ErrorCode transfer_result = op.pending_transfers[i].get();
            if (transfer_result != ErrorCode::OK) {
                if (all_transfers_succeeded) {
                    // Record the first error for reporting
                    first_error = transfer_result;
                    failed_transfer_idx = i;
                    all_transfers_succeeded = false;
                }
                // Continue waiting for all transfers to avoid resource leaks
            }
        }

        if (all_transfers_succeeded) {
            VLOG(1) << "All transfers completed successfully for key "
                    << op.key;
            // Transfer phase successful - continue to finalization
            // Note: Don't mark as SUCCESS yet, need to complete finalization
        } else {
            std::string error_context =
                "Transfer " + std::to_string(failed_transfer_idx) + " failed";
            LOG(ERROR) << "Transfer failed for key " << op.key << ": "
                       << toString(first_error) << " (" << error_context << ")";
            op.SetError(first_error, error_context);
        }
    }
}

void Client::FinalizeBatchPut(std::vector<PutOperation>& ops) {
    // For each operation,
    // If transfers completed successfully, we need to call BatchPutEnd
    // If the operation failed but has allocated replicas, we need to call
    // BatchPutRevoke

    std::vector<std::string> successful_keys;
    std::vector<size_t> successful_indices;
    std::vector<std::string> failed_keys;
    std::vector<size_t> failed_indices;

    // Reserve space to avoid reallocations
    successful_keys.reserve(ops.size());
    successful_indices.reserve(ops.size());
    failed_keys.reserve(ops.size());
    failed_indices.reserve(ops.size());

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];

        // Check if operation completed transfers successfully and needs
        // finalization
        if (!op.IsResolved() && !op.replicas.empty() &&
            !op.pending_transfers.empty()) {
            // Transfers completed, needs BatchPutEnd
            successful_keys.emplace_back(op.key);
            successful_indices.emplace_back(i);
        } else if (op.state != PutOperationState::PENDING &&
                   !op.replicas.empty()) {
            // Operation failed but has allocated replicas, needs BatchPutRevoke
            failed_keys.emplace_back(op.key);
            failed_indices.emplace_back(i);
        }
        // Operations without replicas (early failures) don't need finalization
    }

    // Process successful operations
    if (!successful_keys.empty()) {
        auto end_responses = master_client_.BatchPutEnd(successful_keys);
        if (end_responses.size() != successful_keys.size()) {
            LOG(ERROR) << "BatchPutEnd response size mismatch: expected "
                       << successful_keys.size() << ", got "
                       << end_responses.size();
            for (size_t idx : successful_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchPutEnd response size mismatch");
            }
        } else {
            // Process individual responses
            for (size_t i = 0; i < end_responses.size(); ++i) {
                const size_t op_idx = successful_indices[i];
                if (!end_responses[i]) {
                    LOG(ERROR) << "Failed to finalize put for key "
                               << successful_keys[i] << ": "
                               << toString(end_responses[i].error());
                    ops[op_idx].SetError(end_responses[i].error(),
                                         "BatchPutEnd failed");
                } else {
                    // Operation fully successful
                    ops[op_idx].SetSuccess();
                    VLOG(1) << "Successfully completed put for key "
                            << successful_keys[i];
                }
            }
        }
    }

    // Process failed operations that need cleanup
    if (!failed_keys.empty()) {
        auto revoke_responses = master_client_.BatchPutRevoke(failed_keys);
        if (revoke_responses.size() != failed_keys.size()) {
            LOG(ERROR) << "BatchPutRevoke response size mismatch: expected "
                       << failed_keys.size() << ", got "
                       << revoke_responses.size();
            // Mark all failed operations with revoke RPC failure
            for (size_t idx : failed_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchPutRevoke response size mismatch");
            }
        } else {
            // Process individual revoke responses
            for (size_t i = 0; i < revoke_responses.size(); ++i) {
                const size_t op_idx = failed_indices[i];
                if (!revoke_responses[i]) {
                    LOG(ERROR)
                        << "Failed to revoke put for key " << failed_keys[i]
                        << ": " << toString(revoke_responses[i].error());
                    // Preserve original error but note revoke failure in
                    // context
                    std::string original_context =
                        ops[op_idx].failure_context.value_or("unknown error");
                    ops[op_idx].failure_context =
                        original_context + "; revoke also failed";
                } else {
                    LOG(INFO) << "Successfully revoked failed put for key "
                              << failed_keys[i];
                }
            }
        }
    }

    // Ensure all operations have definitive results
    for (auto& op : ops) {
        if (!op.IsResolved()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "Operation not resolved after finalization");
            LOG(ERROR) << "Operation for key " << op.key
                       << " was not properly resolved";
        }
    }
}

std::vector<tl::expected<void, ErrorCode>> Client::CollectResults(
    const std::vector<PutOperation>& ops) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(ops.size());

    for (const auto& op : ops) {
        // With the new structure, result is always set (never nullopt)
        results.emplace_back(op.result);

        // Additional validation and logging for debugging
        if (!op.result.has_value()) {
            // if error == object already exist, consider as ok
            if (op.result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                results.back() = {};
                continue;
            }
            LOG(ERROR) << "Operation for key " << op.key
                       << " failed: " << toString(op.result.error())
                       << (op.failure_context
                               ? (" (" + *op.failure_context + ")")
                               : "");
        } else {
            VLOG(1) << "Operation for key " << op.key
                    << " completed successfully";
        }
    }

    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices, ReplicateConfig& config) {
    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    StartBatchPut(ops, config);
    SubmitTransfers(ops);
    WaitForTransfers(ops);
    FinalizeBatchPut(ops);
    return CollectResults(ops);
}

tl::expected<void, ErrorCode> Client::Remove(const ObjectKey& key) {
    auto result = master_client_.Remove(key);
    if(storage_backend_) {
        storage_backend_->RemoveFile(key);
    }
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<long, ErrorCode> Client::RemoveAll() {
    if(storage_backend_) {
        storage_backend_->RemoveAll();
    }
    return master_client_.RemoveAll();
}

tl::expected<void, ErrorCode> Client::MountSegment(const void* buffer,
                                                   size_t size) {
    if (buffer == nullptr || size == 0 ||
        reinterpret_cast<uintptr_t>(buffer) % facebook::cachelib::Slab::kSize ||
        size % facebook::cachelib::Slab::kSize) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);

    // Check if the segment overlaps with any existing segment
    for (auto& it : mounted_segments_) {
        auto& mtseg = it.second;
        uintptr_t l1 = reinterpret_cast<uintptr_t>(mtseg.base);
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

    int rc = transfer_engine_.registerLocalMemory(
        (void*)buffer, size, kWildcardLocation, true, true);
    if (rc != 0) {
        LOG(ERROR) << "register_local_memory_failed base=" << buffer
                   << " size=" << size << ", error=" << rc;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    Segment segment(generate_uuid(), local_hostname_,
                    reinterpret_cast<uintptr_t>(buffer), size);

    auto mount_result = master_client_.MountSegment(segment, client_id_);
    if (!mount_result) {
        ErrorCode err = mount_result.error();
        LOG(ERROR) << "mount_segment_to_master_failed base=" << buffer
                   << " size=" << size << ", error=" << err;
        return tl::unexpected(err);
    }

    mounted_segments_[segment.id] = segment;
    return {};
}

tl::expected<void, ErrorCode> Client::UnmountSegment(const void* buffer,
                                                     size_t size) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.end();

    for (auto it = mounted_segments_.begin(); it != mounted_segments_.end();
         ++it) {
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

    auto unmount_result =
        master_client_.UnmountSegment(segment->second.id, client_id_);
    if (!unmount_result) {
        ErrorCode err = unmount_result.error();
        LOG(ERROR) << "Failed to unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    int rc = transfer_engine_.unregisterLocalMemory(
        reinterpret_cast<void*>(segment->second.base));
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister transfer buffer with transfer "
                      "engine ret is "
                   << rc;
        if (rc != ERR_ADDRESS_NOT_REGISTERED) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        // Otherwise, the segment is already unregistered from transfer
        // engine, we can continue
    }

    mounted_segments_.erase(segment);
    return {};
}

tl::expected<void, ErrorCode> Client::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    if (this->transfer_engine_.registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    if (this->transfer_engine_.unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<bool, ErrorCode> Client::IsExist(const std::string& key) {
    auto result = master_client_.ExistKey(key);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return result.value();
}

std::vector<tl::expected<bool, ErrorCode>> Client::BatchIsExist(
    const std::vector<std::string>& keys) {
    auto response = master_client_.BatchExistKey(keys);

    // Check if we got the expected number of responses
    if (response.size() != keys.size()) {
        LOG(ERROR) << "BatchExistKey response size mismatch. Expected: "
                   << keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<bool, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    // Return the response directly as it's already in the correct
    // format
    return response;
}

void Client::PrepareStorageBackend(const std::string& storage_root_dir,
                                   const std::string& fsdir) {
    // Initialize storage backend
    storage_backend_ = StorageBackend::Create(storage_root_dir, fsdir);
    if (!storage_backend_) {
        LOG(INFO) << "Failed to initialize storage backend";
    }
}

ErrorCode Client::GetFromLocalFile(const std::string& object_key,
                                   std::vector<Slice>& slices,
                                   std::vector<Replica::Descriptor>& replicas) {
    if (!storage_backend_) {
        return ErrorCode::FILE_READ_FAIL;
    }

    ErrorCode err = storage_backend_->LoadObject(object_key, slices);
    if (err != ErrorCode::OK) {
        return err;
    }

    return ErrorCode::OK;
}

void Client::PutToLocalFile(const std::string& key,
                            std::vector<Slice>& slices) {
    if (!storage_backend_) return;

    size_t total_size = 0;
    for (const auto& slice : slices) {
        total_size += slice.size;
    }

    std::string value;
    value.reserve(total_size);
    for (const auto& slice : slices) {
        value.append(static_cast<char*>(slice.ptr), slice.size);
    }

    write_thread_pool_.enqueue(
        [backend = storage_backend_, key, value = std::move(value)] {
            backend->StoreObject(key, value);
        });
}

ErrorCode Client::TransferData(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices,
                               TransferRequest::OpCode op_code) {
    CHECK(transfer_submitter_) << "TransferSubmitter not initialized";

    auto future =
        transfer_submitter_->submit(replica_descriptor, slices, op_code);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

ErrorCode Client::TransferWrite(const Replica::Descriptor& replica_descriptor,
                                std::vector<Slice>& slices) {
    return TransferData(replica_descriptor, slices, TransferRequest::WRITE);
}

ErrorCode Client::TransferRead(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices) {
    size_t total_size = 0;
    if (replica_descriptor.is_memory_replica()) {
        auto& mem_desc = replica_descriptor.get_memory_descriptor();
        for (const auto& handle : mem_desc.buffer_descriptors) {
            total_size += handle.size_;
        }
    } else {
        auto& disk_desc = replica_descriptor.get_disk_descriptor();
        total_size = disk_desc.file_size;
    }

    size_t slices_size = CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    return TransferData(replica_descriptor, slices, TransferRequest::READ);
}

void Client::PingThreadFunc() {
    // How many failed pings before getting latest master view from etcd
    const int max_ping_fail_count = 3;
    // How long to wait for next ping after success
    const int success_ping_interval_ms = 1000;
    // How long to wait for next ping after failure
    const int fail_ping_interval_ms = 1000;
    // Increment after a ping failure, reset after a ping success
    int ping_fail_count = 0;

    auto remount_segment = [this]() {
        // This lock must be held until the remount rpc is finished,
        // otherwise there will be corner cases, e.g., a segment is
        // unmounted successfully first, and then remounted again in
        // this thread.
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        std::vector<Segment> segments;
        for (auto it : mounted_segments_) {
            auto& segment = it.second;
            segments.emplace_back(segment);
        }
        auto remount_result =
            master_client_.ReMountSegment(segments, client_id_);
        if (!remount_result) {
            ErrorCode err = remount_result.error();
            LOG(ERROR) << "Failed to remount segments: " << err;
        }
    };
    // Use another thread to remount segments to avoid blocking the ping
    // thread
    std::future<void> remount_segment_future;

    while (ping_running_) {
        // Join the remount segment thread if it is ready
        if (remount_segment_future.valid() &&
            remount_segment_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            remount_segment_future = std::future<void>();
        }

        // Ping master
        auto ping_result = master_client_.Ping(client_id_);
        if (ping_result) {
            // Reset ping failure count
            ping_fail_count = 0;
            auto [view_version, client_status] = ping_result.value();
            if (client_status == ClientStatus::NEED_REMOUNT &&
                !remount_segment_future.valid()) {
                // Ensure at most one remount segment thread is running
                remount_segment_future =
                    std::async(std::launch::async, remount_segment);
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(success_ping_interval_ms));
            continue;
        }

        ping_fail_count++;
        if (ping_fail_count < max_ping_fail_count) {
            LOG(ERROR) << "Failed to ping master";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        // Too many ping failures, we need to check if the master view
        // has changed
        LOG(ERROR) << "Failed to ping master for " << ping_fail_count
                   << " times, try to get latest master view and reconnect";
        std::string master_address;
        ViewVersionId next_version = 0;
        auto err =
            master_view_helper_.GetMasterView(master_address, next_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get new master view: " << toString(err);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        err = master_client_.Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master " << master_address
                       << ": " << toString(err);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        LOG(INFO) << "Reconnected to master " << master_address;
        ping_fail_count = 0;
    }
    // Explicitly wait for the remount segment thread to finish
    if (remount_segment_future.valid()) {
        remount_segment_future.wait();
    }
}

ErrorCode Client::FindFirstCompleteReplica(
    const std::vector<Replica::Descriptor>& replica_list,
    Replica::Descriptor& replica) {
    // Find the first complete replica
    for (size_t i = 0; i < replica_list.size(); ++i) {
        if (replica_list[i].status == ReplicaStatus::COMPLETE) {
            replica = replica_list[i];
            return ErrorCode::OK;
        }
    }

    // No complete replica found
    return ErrorCode::INVALID_REPLICA;
}

}  // namespace mooncake
