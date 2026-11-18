#include "client_service.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "ascii_string.h"
#include "allocator.h"
#include "segment.h"
#include "nof/nof_runtime.h"

#include <csignal>
#include <algorithm>
#include <cassert>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <future>
#include <iomanip>
#include <limits>
#if defined(__linux__)
#include <numa.h>
#endif
#include <optional>
#include <ranges>
#include <span>
#include <sched.h>
#include <thread>
#include <set>
#include <utility>
#include <vector>
#include <ylt/struct_json/json_reader.h>

#include "common.h"
#include "transfer_engine.h"
#include "topology.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "types.h"
#include "client_buffer.h"
#include "common/network.h"
#include "config/client_host_identity_config.h"
#include "config/client_object_checksum_config.h"
#include "replica_selection.h"
#include "rpc_types.h"
#include "local_hot_cache.h"
#include "config/client_auto_discovery_config.h"
#include "device/accelerator_registry.h"
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#include "crc_checksum.h"
#include "config/client_numa_config.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "client_service_internal.h"

namespace mooncake {

using detail::CalculateSliceSize;
using detail::GetContiguousSliceRange;

namespace {

struct ReplicaTransferSummary {
    size_t allocated_memory_replicas = 0;
    size_t allocated_nof_replicas = 0;
    size_t allocated_dfs_replicas = 0;
    size_t successful_memory_transfers = 0;
    size_t successful_nof_transfers = 0;
    size_t successful_dfs_transfers = 0;
    size_t failed_memory_transfers = 0;
    size_t failed_nof_transfers = 0;
    size_t failed_dfs_transfers = 0;
    ErrorCode first_error = ErrorCode::OK;

    void RecordAllocatedReplica(const Replica::Descriptor& replica) {
        if (replica.is_memory_replica()) {
            ++allocated_memory_replicas;
        } else if (replica.is_nof_replica()) {
            ++allocated_nof_replicas;
        } else if (replica.is_dfs_replica()) {
            ++allocated_dfs_replicas;
        }
    }

    void RecordSuccess(ReplicaType replica_type) {
        if (replica_type == ReplicaType::MEMORY) {
            ++successful_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++successful_nof_transfers;
        } else if (replica_type == ReplicaType::DFS) {
            ++successful_dfs_transfers;
        }
    }

    void RecordFailure(ReplicaType replica_type, ErrorCode error) {
        if (replica_type == ReplicaType::MEMORY) {
            ++failed_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++failed_nof_transfers;
        } else if (replica_type == ReplicaType::DFS) {
            ++failed_dfs_transfers;
        }
        if (first_error == ErrorCode::OK) {
            first_error = error;
        }
    }
};

bool NonDfsTransfersSucceeded(const ReplicaTransferSummary& summary) {
    return summary.successful_memory_transfers ==
               summary.allocated_memory_replicas &&
           summary.successful_nof_transfers == summary.allocated_nof_replicas &&
           summary.failed_memory_transfers == 0 &&
           summary.failed_nof_transfers == 0;
}

bool AllAllocatedTransfersSucceeded(const ReplicaTransferSummary& summary) {
    return NonDfsTransfersSucceeded(summary) &&
           summary.successful_dfs_transfers == summary.allocated_dfs_replicas &&
           summary.failed_dfs_transfers == 0;
}

bool HasExpectedReplicaAllocation(const ReplicateConfig& config,
                                  const ReplicaTransferSummary& summary) {
    if (config.nof_replica_num == 0 && config.dfs_replica_num == 0) {
        return summary.allocated_memory_replicas > 0;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        return summary.allocated_memory_replicas +
                   summary.allocated_nof_replicas >
               0;
    }
    return summary.allocated_memory_replicas == config.replica_num &&
           summary.allocated_nof_replicas == config.nof_replica_num &&
           summary.allocated_dfs_replicas == config.dfs_replica_num;
}

std::optional<ErrorCode> ValidatePreferSameNodeWriteConfig(
    const ReplicateConfig& config) {
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                      "NoF replicas";
        return ErrorCode::INVALID_PARAMS;
    }
    if (config.replica_num != 1) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                      "replica_num != 1";
        return ErrorCode::INVALID_PARAMS;
    }

    return std::nullopt;
}

// success describes whether the overall put should succeed. Reliable modes
// require all allocated replicas to complete. Flexible dual-replica mode only
// requires one replica type to succeed, so success may be true while
// revoke_type is also set for the failed side.
struct FinalizeDecision {
    std::optional<ReplicaType> end_type;
    std::optional<ReplicaType> revoke_type;
    bool success = false;
    ErrorCode error = ErrorCode::OK;
};

FinalizeDecision DetermineFinalizeDecision(
    const ReplicateConfig& config, const ReplicaTransferSummary& summary) {
    const auto write_mode = DetermineReplicaWriteMode(config);
    const bool allocation_satisfied =
        HasExpectedReplicaAllocation(config, summary);

    if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        const bool all_transfers_succeeded =
            AllAllocatedTransfersSucceeded(summary);
        if (allocation_satisfied && all_transfers_succeeded) {
            return {.end_type = ReplicaType::ALL,
                    .revoke_type = std::nullopt,
                    .success = true,
                    .error = ErrorCode::OK};
        }
        return {.end_type = std::nullopt,
                .revoke_type = ReplicaType::ALL,
                .success = false,
                .error = allocation_satisfied
                             ? (summary.first_error == ErrorCode::OK
                                    ? ErrorCode::TRANSFER_FAIL
                                    : summary.first_error)
                             : ErrorCode::NO_AVAILABLE_HANDLE};
    }

    const bool memory_succeeded = summary.successful_memory_transfers > 0;
    const bool nof_succeeded = summary.successful_nof_transfers > 0;

    if (memory_succeeded && nof_succeeded) {
        return {.end_type = ReplicaType::ALL,
                .revoke_type = std::nullopt,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (memory_succeeded) {
        return {.end_type = ReplicaType::MEMORY,
                .revoke_type = ReplicaType::NOF_SSD,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (nof_succeeded) {
        return {.end_type = ReplicaType::NOF_SSD,
                .revoke_type = ReplicaType::MEMORY,
                .success = true,
                .error = ErrorCode::OK};
    }

    return {.end_type = std::nullopt,
            .revoke_type = ReplicaType::ALL,
            .success = false,
            .error = summary.first_error == ErrorCode::OK
                         ? ErrorCode::NO_AVAILABLE_HANDLE
                         : summary.first_error};
}

// Every COMPLETE replica in the list is a LOCAL_DISK replica owned by this
// client, so nothing else can serve reads for the key.
bool HasOnlyClientLocalDiskReplicas(
    const std::vector<Replica::Descriptor>& replicas, const UUID& client_id) {
    bool has_local_disk = false;
    for (const auto& replica : replicas) {
        if (replica.status != ReplicaStatus::COMPLETE) {
            continue;
        }
        if (replica.is_local_disk_replica() &&
            replica.get_local_disk_descriptor().client_id == client_id) {
            has_local_disk = true;
            continue;
        }
        // A completed replica owned elsewhere (memory, remote disk, DFS) can
        // still serve reads, so there is nothing for this client to heal.
        return false;
    }
    return has_local_disk;
}

}  // namespace

tl::expected<void, ErrorCode> Client::Put(const ObjectKey& key,
                                          std::vector<Slice>& slices,
                                          const ReplicateConfig& config) {
    std::optional<uint64_t> object_checksum;
    if (object_checksum_enabled_) {
        auto checksum_result = ComputeObjectChecksumForSlices(
            key, slices, CalculateSliceSize(slices));
        if (!checksum_result) {
            return tl::unexpected(checksum_result.error());
        }
        object_checksum = *checksum_result;
    }

    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    ReplicateConfig client_cfg = AttachHostId(config);
    AttachHcaAffinity(client_cfg, slices);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    // Start put operation
    auto start_result = master_client_.PutStart(key, slice_lengths, client_cfg);
    if (!start_result &&
        start_result.error() == ErrorCode::OBJECT_ALREADY_EXISTS &&
        healDanglingLocalDiskReplica(key) == DiskReplicaHealResult::kEvicted) {
        start_result = master_client_.PutStart(key, slice_lengths, client_cfg);
    }
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

    ReplicaTransferSummary transfer_summary;
    for (const auto& replica : start_result.value()) {
        transfer_summary.RecordAllocatedReplica(replica);
    }

    // Record Put transfer latency (all replicas)
    auto t0_put = std::chrono::steady_clock::now();

    // We must deal with disk replica first, then the disk putrevoke/putend can
    // be called surely
    if (storage_backend_) {
        for (auto it = start_result.value().rbegin();
             it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                // Store to local file if storage backend is available
                auto disk_descriptor = replica.get_disk_descriptor();
                PutToLocalFile(key, slices, disk_descriptor);
                break;  // Only one disk replica is needed
            }
        }
    }

    for (const auto& [replica_type, transfer_err] :
         WriteMemoryAndNofReplicas(start_result.value(), slices)) {
        if (transfer_err != ErrorCode::OK) {
            transfer_summary.RecordFailure(replica_type, transfer_err);
            continue;
        }
        transfer_summary.RecordSuccess(replica_type);
    }

    if (transfer_summary.allocated_dfs_replicas > 0 &&
        NonDfsTransfersSucceeded(transfer_summary)) {
        std::vector<std::string> dfs_keys;
        std::vector<const std::vector<Slice>*> dfs_slices;
        std::vector<DistributedFSDescriptor> dfs_descriptors;
        for (const auto& replica : start_result.value()) {
            if (!replica.is_dfs_replica()) {
                continue;
            }
            dfs_keys.push_back(key);
            dfs_slices.push_back(&slices);
            dfs_descriptors.push_back(replica.get_dfs_descriptor());
        }
        for (auto dfs_result :
             WriteDfsReplicas(dfs_keys, dfs_slices, dfs_descriptors)) {
            if (dfs_result == ErrorCode::OK) {
                transfer_summary.RecordSuccess(ReplicaType::DFS);
            } else {
                transfer_summary.RecordFailure(ReplicaType::DFS, dfs_result);
            }
        }
    } else if (transfer_summary.allocated_dfs_replicas > 0) {
        // A DFS replica was allocated but is abandoned because a peer replica
        // failed first. Without this counter the write simply disappears: no
        // error is recorded against DFS and no I/O is attempted. Counted per
        // key, matching SubmitDfsWrites.
        VLOG(1) << "Skipping DFS write, action=skip, key=" << key
                << ", reason=non_dfs_transfer_failed";
        if (auto* dfs_metric = GetDfsMetricPtr()) {
            dfs_metric->RecordSkippedWrites(1);
        }
    }

    auto us_put = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_put)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us_put);
    }

    const auto finalize_decision =
        DetermineFinalizeDecision(client_cfg, transfer_summary);

    if (finalize_decision.end_type.has_value()) {
        auto end_result = master_client_.PutEnd(
            ObjectMeta{key, object_checksum}, *finalize_decision.end_type);
        if (!end_result) {
            ErrorCode err = end_result.error();
            LOG(ERROR) << "Failed to end put operation: " << err;
            return tl::unexpected(err);
        }
    }

    if (finalize_decision.revoke_type.has_value()) {
        auto revoke_result =
            master_client_.PutRevoke(key, *finalize_decision.revoke_type);
        if (!revoke_result) {
            LOG(ERROR) << "Failed to revoke put operation";
            return tl::unexpected(revoke_result.error());
        }
    }

    if (!finalize_decision.success) {
        return tl::unexpected(finalize_decision.error);
    }

    return {};
}

Client::DiskReplicaHealResult Client::healDanglingLocalDiskReplica(
    const ObjectKey& key) {
    auto replicas = master_client_.GetReplicaList(key);
    if (!replicas ||
        !HasOnlyClientLocalDiskReplicas(replicas->replicas, client_id_) ||
        !local_disk_probe_fn_) {
        return DiskReplicaHealResult::kNotDangling;
    }
    // A physical SSD wipe can leave master metadata naming a completed entry
    // whose backing file is gone (issue #3709).
    // Reporting success for a Put on top of it would store nothing. Ask the
    // installed probe (the offload file storage owned by RealClient) whether
    // the file is still there. Only a proven-gone answer evicts; present or
    // unknown (transport/config trouble) keeps the old idempotent path, so a
    // healthy replica can never be evicted by mistake.
    const std::optional<bool> file_gone = local_disk_probe_fn_(key);
    if (file_gone == false) {
        return DiskReplicaHealResult::kPresent;
    }
    if (file_gone != true) {
        return DiskReplicaHealResult::kUnknown;
    }
    auto evicted =
        master_client_.EvictDiskReplica(key, ReplicaType::LOCAL_DISK);
    if (!evicted) {
        LOG(WARNING) << "Failed to evict dangling LOCAL_DISK replica for key="
                     << key << ": " << toString(evicted.error());
        return DiskReplicaHealResult::kUnknown;
    }
    LOG(WARNING) << "Evicted dangling LOCAL_DISK replica for key=" << key
                 << " (backing file already gone)";
    return DiskReplicaHealResult::kEvicted;
}

tl::expected<void, ErrorCode> Client::Upsert(const ObjectKey& key,
                                             std::vector<Slice>& slices,
                                             const ReplicateConfig& config) {
    std::optional<uint64_t> object_checksum;
    if (object_checksum_enabled_) {
        auto checksum_result = ComputeObjectChecksumForSlices(
            key, slices, CalculateSliceSize(slices));
        if (!checksum_result) {
            return tl::unexpected(checksum_result.error());
        }
        object_checksum = *checksum_result;
    }

    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    ReplicateConfig client_cfg = AttachHostId(config);
    AttachHcaAffinity(client_cfg, slices);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    // Start upsert operation
    auto start_result =
        master_client_.UpsertStart(key, slice_lengths, client_cfg);
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

    ReplicaTransferSummary transfer_summary;
    for (const auto& replica : start_result.value()) {
        transfer_summary.RecordAllocatedReplica(replica);
    }

    // Record transfer latency
    auto t0 = std::chrono::steady_clock::now();

    // Handle disk replicas first
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

    // Transfer to memory and NoF replicas first.
    for (const auto& [replica_type, transfer_err] :
         WriteMemoryAndNofReplicas(start_result.value(), slices)) {
        if (transfer_err != ErrorCode::OK) {
            transfer_summary.RecordFailure(replica_type, transfer_err);
            continue;
        }
        transfer_summary.RecordSuccess(replica_type);
    }

    if (transfer_summary.allocated_dfs_replicas > 0 &&
        NonDfsTransfersSucceeded(transfer_summary)) {
        std::vector<std::string> dfs_keys;
        std::vector<const std::vector<Slice>*> dfs_slices;
        std::vector<DistributedFSDescriptor> dfs_descriptors;
        for (const auto& replica : start_result.value()) {
            if (!replica.is_dfs_replica()) {
                continue;
            }
            dfs_keys.push_back(key);
            dfs_slices.push_back(&slices);
            dfs_descriptors.push_back(replica.get_dfs_descriptor());
        }
        for (auto dfs_result :
             WriteDfsReplicas(dfs_keys, dfs_slices, dfs_descriptors)) {
            if (dfs_result == ErrorCode::OK) {
                transfer_summary.RecordSuccess(ReplicaType::DFS);
            } else {
                transfer_summary.RecordFailure(ReplicaType::DFS, dfs_result);
            }
        }
    } else if (transfer_summary.allocated_dfs_replicas > 0) {
        // Same abandoned-write case as in Put: the DFS replica was allocated
        // but a peer replica failed first, so no I/O is attempted and no DFS
        // error is recorded. Counted per key, matching SubmitDfsWrites.
        VLOG(1) << "Skipping DFS write, action=skip, key=" << key
                << ", reason=non_dfs_transfer_failed";
        if (auto* dfs_metric = GetDfsMetricPtr()) {
            dfs_metric->RecordSkippedWrites(1);
        }
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us);
    }

    const auto finalize_decision =
        DetermineFinalizeDecision(client_cfg, transfer_summary);
    if (finalize_decision.end_type.has_value()) {
        auto end_result = master_client_.UpsertEnd(
            ObjectMeta{key, object_checksum}, *finalize_decision.end_type);
        if (!end_result) {
            ErrorCode err = end_result.error();
            LOG(ERROR) << "Failed to end upsert operation: " << err;
            return tl::unexpected(err);
        }
    }
    if (finalize_decision.revoke_type.has_value()) {
        auto revoke_result =
            master_client_.UpsertRevoke(key, *finalize_decision.revoke_type);
        if (!revoke_result) {
            LOG(ERROR) << "Failed to revoke upsert operation";
            return tl::unexpected(revoke_result.error());
        }
    }
    if (!finalize_decision.success) {
        return tl::unexpected(finalize_decision.error);
    }

    // Success-side invalidation: a concurrent read between the pre-upsert
    // RemoveHotKey() and UpsertEnd could have read the old value and submitted
    // an async hot-cache fill with a still-valid token. Invalidate again now
    // that the new value is committed so that stale fill cannot publish.
    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

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
    struct PendingTransferRecord {
        ReplicaType replica_type;
        TransferFuture future;

        PendingTransferRecord(ReplicaType type,
                              TransferFuture&& transfer_future)
            : replica_type(type), future(std::move(transfer_future)) {}
    };

    PutOperation(std::string_view k, const std::vector<Slice>& s)
        : key(k), slices(s) {
        // Initialize with a pending error state to ensure result is always set
        result = tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::string key;
    std::vector<Slice> slices;
    std::optional<uint64_t> object_checksum;
    std::vector<std::vector<Slice>> batched_slices;

    // Enhanced state tracking
    PutOperationState state = PutOperationState::PENDING;
    tl::expected<void, ErrorCode> result;
    std::vector<Replica::Descriptor> replicas;
    std::vector<PendingTransferRecord> pending_transfers;

    size_t requested_memory_replicas = 0;
    size_t requested_nof_replicas = 0;
    size_t requested_dfs_replicas = 0;
    ReplicaTransferSummary transfer_summary;

    // Error context for debugging
    std::optional<std::string> failure_context;

    // Helper methods for robust state management
    void SetSuccess() {
        state = PutOperationState::SUCCESS;
        result = {};
        failure_context.reset();
    }

    void SetTerminalError(ErrorCode error, PutOperationState terminal_state,
                          const std::string& context = "") {
        state = terminal_state;
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = context;
        }
    }

    void SetError(ErrorCode error, const std::string& context = "") {
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = toString(error) + ": " + context + "; " +
                              failure_context.value_or("");
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

    void AppendFailureContext(const std::string& context) {
        if (context.empty()) {
            return;
        }
        if (!failure_context.has_value()) {
            failure_context = context;
            return;
        }
        failure_context = *failure_context + "; " + context;
    }

    void InitializeRequestedReplicas(const ReplicateConfig& config) {
        requested_memory_replicas = config.replica_num;
        requested_nof_replicas = config.nof_replica_num;
        requested_dfs_replicas = config.dfs_replica_num;
    }

    ReplicateConfig ToReplicateConfig() const {
        ReplicateConfig config;
        config.replica_num = requested_memory_replicas;
        config.nof_replica_num = requested_nof_replicas;
        config.dfs_replica_num = requested_dfs_replicas;
        return config;
    }

    void RecordAllocatedReplicas() {
        transfer_summary = ReplicaTransferSummary{};
        for (const auto& replica : replicas) {
            transfer_summary.RecordAllocatedReplica(replica);
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

void Client::ComputeBatchObjectChecksums(std::vector<PutOperation>& ops) {
    if (!object_checksum_enabled_) {
        return;
    }
    for (auto& op : ops) {
        auto checksum_result = ComputeObjectChecksumForSlices(
            op.key, op.slices, CalculateSliceSize(op.slices));
        if (!checksum_result) {
            op.SetTerminalError(checksum_result.error(),
                                PutOperationState::MASTER_FAILED,
                                "Object checksum calculation failed");
            continue;
        }
        op.object_checksum = *checksum_result;
    }
}

ReplicateConfig Client::AttachBatchHcaAffinity(
    const ReplicateConfig& config, const std::vector<PutOperation>& ops,
    const std::vector<size_t>& indices) const {
    if (!StoreHcaAffinityEnabled()) return config;
    auto request = config.ForKeySubset(indices);
    if (request.preferred_hca_names || request.per_key_preferred_hca_names)
        return request;
    if (auto infer = MakeHcaAffinity()) {
        std::vector<std::vector<std::string>> hints;
        bool has_hint = false;
        for (auto index : indices) {
            hints.push_back(infer(ops[index].slices));
            has_hint |= !hints.back().empty();
        }
        if (has_hint) request.per_key_preferred_hca_names = std::move(hints);
    }
    return request;
}

void Client::healDanglingLocalDiskBatchStarts(
    std::vector<PutOperation>& ops, const std::vector<size_t>& active_indices,
    const std::vector<size_t>& already_exists,
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint64_t>>& slice_lengths,
    const ReplicateConfig& config) {
    if (already_exists.empty() || !local_disk_probe_fn_) {
        return;
    }
    std::vector<std::string> candidate_keys;
    candidate_keys.reserve(already_exists.size());
    for (size_t i : already_exists) {
        candidate_keys.emplace_back(keys[i]);
    }
    // One round trip for the whole subset: a healthy idempotent batch pays a
    // single batched query and no evictions, and the per-key work left over
    // is a local filesystem probe.
    auto replica_lists = master_client_.BatchGetReplicaList(candidate_keys);
    if (replica_lists.size() != candidate_keys.size()) {
        return;
    }
    std::vector<std::string> evict_keys;
    std::vector<size_t> evict_positions;
    for (size_t j = 0; j < candidate_keys.size(); ++j) {
        if (!replica_lists[j] || !HasOnlyClientLocalDiskReplicas(
                                     replica_lists[j]->replicas, client_id_)) {
            continue;
        }
        const std::optional<bool> file_gone =
            local_disk_probe_fn_(candidate_keys[j]);
        if (file_gone != true) {
            continue;
        }
        evict_keys.emplace_back(candidate_keys[j]);
        evict_positions.emplace_back(j);
    }
    if (evict_keys.empty()) {
        return;
    }
    auto evicted = master_client_.BatchEvictDiskReplica(
        evict_keys, ReplicaType::LOCAL_DISK);
    if (evicted.size() != evict_keys.size()) {
        return;
    }
    std::vector<std::string> retry_keys;
    std::vector<std::vector<uint64_t>> retry_slices;
    std::vector<size_t> retry_positions;
    std::vector<size_t> retry_config_indices;
    for (size_t j = 0; j < evict_keys.size(); ++j) {
        if (!evicted[j]) {
            LOG(WARNING) << "Failed to evict dangling LOCAL_DISK replica for "
                         << "key=" << evict_keys[j] << ": "
                         << toString(evicted[j].error());
            continue;
        }
        LOG(WARNING) << "Evicted dangling LOCAL_DISK replica for key="
                     << evict_keys[j] << " (backing file already gone)";
        retry_positions.emplace_back(evict_positions[j]);
        retry_config_indices.push_back(already_exists[evict_positions[j]]);
        retry_keys.emplace_back(keys[already_exists[evict_positions[j]]]);
        retry_slices.emplace_back(
            slice_lengths[already_exists[evict_positions[j]]]);
    }
    if (retry_keys.empty()) {
        return;
    }
    const auto retry_config =
        config.group_ids || config.session_ids ||
                config.residency_affinity_ids ||
                config.per_key_preferred_hca_names
            ? std::make_optional(config.ForKeySubset(retry_config_indices))
            : std::nullopt;
    auto retry_responses = master_client_.BatchPutStart(
        retry_keys, retry_slices, retry_config ? *retry_config : config);
    if (retry_responses.size() != retry_keys.size()) {
        return;
    }
    for (size_t r = 0; r < retry_keys.size(); ++r) {
        auto& op = ops[active_indices[already_exists[retry_positions[r]]]];
        if (!retry_responses[r]) {
            // OBJECT_ALREADY_EXISTS again means another writer won the race;
            // the idempotent skip applies. Any other error is reported as is.
            if (retry_responses[r].error() !=
                ErrorCode::OBJECT_ALREADY_EXISTS) {
                op.SetTerminalError(retry_responses[r].error(),
                                    PutOperationState::MASTER_FAILED,
                                    "Master failed to start put operation");
            }
            continue;
        }
        op.replicas = std::move(retry_responses[r].value());
        op.RecordAllocatedReplicas();
        if (!HasExpectedReplicaAllocation(config, op.transfer_summary)) {
            op.SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                PutOperationState::MASTER_FAILED,
                                "Allocated replicas do not satisfy "
                                "requested replica policy");
        }
    }
}

void Client::StartBatchPut(std::vector<PutOperation>& ops,
                           const ReplicateConfig& base_config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;
    std::vector<size_t> active_indices;

    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());
    active_indices.reserve(ops.size());

    if (hot_cache_) {
        std::vector<std::string> hot_keys;
        hot_keys.reserve(ops.size());
        for (const auto& op : ops) hot_keys.emplace_back(op.key);
        hot_cache_->RemoveHotKeys(hot_keys);
    }

    for (size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];
        if (op.IsResolved()) {
            continue;
        }
        active_indices.emplace_back(i);
        keys.emplace_back(op.key);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    if (active_indices.empty()) {
        return;
    }

    std::optional<ReplicateConfig> active_config;
    if (StoreHcaAffinityEnabled()) {
        active_config =
            AttachBatchHcaAffinity(base_config, ops, active_indices);
    } else if (active_indices.size() != ops.size() &&
               (base_config.group_ids || base_config.session_ids ||
                base_config.residency_affinity_ids ||
                base_config.per_key_preferred_hca_names)) {
        active_config = base_config.ForKeySubset(active_indices);
    }
    const auto& config = active_config ? *active_config : base_config;
    auto start_responses =
        master_client_.BatchPutStart(keys, slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != active_indices.size()) {
        LOG(ERROR) << "BatchPutStart response size mismatch: expected "
                   << active_indices.size() << ", got "
                   << start_responses.size();
        for (size_t index : active_indices) {
            auto& op = ops[index];
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchPutStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    std::vector<size_t> already_exists;
    for (size_t i = 0; i < active_indices.size(); ++i) {
        auto& op = ops[active_indices[i]];
        op.InitializeRequestedReplicas(config);
        if (!start_responses[i]) {
            if (start_responses[i].error() ==
                ErrorCode::OBJECT_ALREADY_EXISTS) {
                // Decided after the heal pass below: retried when the
                // replica proves dangling, or the idempotent skip that
                // CollectResults already grants.
                already_exists.emplace_back(i);
                continue;
            }
            op.SetTerminalError(start_responses[i].error(),
                                PutOperationState::MASTER_FAILED,
                                "Master failed to start put operation");
        } else {
            op.replicas = std::move(start_responses[i].value());
            op.RecordAllocatedReplicas();
            if (!HasExpectedReplicaAllocation(config, op.transfer_summary)) {
                op.SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                    PutOperationState::MASTER_FAILED,
                                    "Allocated replicas do not satisfy "
                                    "requested replica policy");
                continue;
            }
            // Operation continues to next stage - result remains INTERNAL_ERROR
            // until fully successful
            VLOG(1) << "Successfully started put for key " << op.key << " with "
                    << op.replicas.size() << " replicas";
        }
    }

    healDanglingLocalDiskBatchStarts(ops, active_indices, already_exists, keys,
                                     slice_lengths, config);
    for (size_t i : already_exists) {
        auto& op = ops[active_indices[i]];
        if (!op.IsResolved() && op.replicas.empty()) {
            op.SetTerminalError(ErrorCode::OBJECT_ALREADY_EXISTS,
                                PutOperationState::MASTER_FAILED,
                                "Master failed to start put operation");
        }
    }
}

void Client::StartBatchUpsert(std::vector<PutOperation>& ops,
                              const ReplicateConfig& base_config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;
    std::vector<size_t> active_indices;

    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());
    active_indices.reserve(ops.size());

    if (hot_cache_) {
        std::vector<std::string> hot_keys;
        hot_keys.reserve(ops.size());
        for (const auto& op : ops) hot_keys.emplace_back(op.key);
        hot_cache_->RemoveHotKeys(hot_keys);
    }

    for (size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];
        if (op.IsResolved()) {
            continue;
        }
        active_indices.emplace_back(i);
        keys.emplace_back(op.key);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    if (active_indices.empty()) {
        return;
    }

    std::optional<ReplicateConfig> active_config;
    if (StoreHcaAffinityEnabled()) {
        active_config =
            AttachBatchHcaAffinity(base_config, ops, active_indices);
    } else if (active_indices.size() != ops.size() &&
               (base_config.group_ids || base_config.session_ids ||
                base_config.residency_affinity_ids ||
                base_config.per_key_preferred_hca_names)) {
        active_config = base_config.ForKeySubset(active_indices);
    }
    const auto& config = active_config ? *active_config : base_config;
    auto start_responses =
        master_client_.BatchUpsertStart(keys, slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != active_indices.size()) {
        LOG(ERROR) << "BatchUpsertStart response size mismatch: expected "
                   << active_indices.size() << ", got "
                   << start_responses.size();
        for (size_t index : active_indices) {
            auto& op = ops[index];
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchUpsertStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    for (size_t i = 0; i < active_indices.size(); ++i) {
        auto& op = ops[active_indices[i]];
        op.InitializeRequestedReplicas(config);
        if (!start_responses[i]) {
            op.SetTerminalError(start_responses[i].error(),
                                PutOperationState::MASTER_FAILED,
                                "Master failed to start upsert operation");
        } else {
            op.replicas = std::move(start_responses[i].value());
            op.RecordAllocatedReplicas();
            if (!HasExpectedReplicaAllocation(config, op.transfer_summary)) {
                op.SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                    PutOperationState::MASTER_FAILED,
                                    "Allocated replicas do not satisfy "
                                    "requested replica policy");
                continue;
            }
            VLOG(1) << "Successfully started upsert for key " << op.key
                    << " with " << op.replicas.size() << " replicas";
        }
    }
}

void Client::SubmitTransfers(std::vector<PutOperation>& ops,
                             bool group_by_target) {
    if (std::all_of(ops.begin(), ops.end(),
                    [](const PutOperation& op) { return op.IsResolved(); })) {
        return;
    }
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        for (auto& op : ops) {
            if (op.IsResolved()) {
                continue;
            }
            op.SetTerminalError(ErrorCode::INVALID_PARAMS,
                                PutOperationState::TRANSFER_FAILED,
                                "TransferSubmitter not initialized");
        }
        return;
    }

    struct TargetWriteGroup {
        std::vector<Replica::Descriptor> replicas;
        std::vector<std::vector<Slice>> slices;
        std::vector<PutOperation*> ops;
    };
    std::unordered_map<std::string, TargetWriteGroup> target_groups;

    for (auto& op : ops) {
        // Skip operations that already failed in previous stages
        if (op.IsResolved()) {
            continue;
        }

        // Skip operations that don't have replicas (failed in StartBatchPut)
        if (op.replicas.empty()) {
            op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                PutOperationState::MASTER_FAILED,
                                "No replicas available for transfer");
            continue;
        }

        // We must deal with disk replica first, then the disk putrevoke/putend
        // can be called surely
        if (storage_backend_) {
            for (auto it = op.replicas.rbegin(); it != op.replicas.rend();
                 ++it) {
                const auto& replica = *it;
                if (replica.is_disk_replica()) {
                    auto disk_descriptor = replica.get_disk_descriptor();
                    PutToLocalFile(op.key, op.slices, disk_descriptor);
                    break;  // Only one disk replica is needed
                }
            }
        }

        for (size_t replica_idx = 0; replica_idx < op.replicas.size();
             ++replica_idx) {
            const auto& replica = op.replicas[replica_idx];
            if (group_by_target && replica.is_memory_replica() &&
                CalculateSliceSize(op.slices) ==
                    replica.get_memory_descriptor().buffer_descriptor.size_) {
                // A size mismatch would fail the whole group in submit_batch,
                // so such a replica stays on the per-key path and fails alone.
                auto& group =
                    target_groups[replica.get_memory_descriptor()
                                      .buffer_descriptor.transport_endpoint_];
                group.replicas.push_back(replica);
                group.slices.push_back(op.slices);
                group.ops.push_back(&op);
                continue;
            }
            if (replica.is_memory_replica() || replica.is_nof_replica()) {
                const auto replica_type = replica.is_memory_replica()
                                              ? ReplicaType::MEMORY
                                              : ReplicaType::NOF_SSD;
                std::optional<TransferFuture> submit_result;
                if (replica.is_nof_replica()) {
                    auto contiguous_range = GetContiguousSliceRange(op.slices);
                    if (!contiguous_range.has_value()) {
                        std::string failure_context =
                            "NoF transfer requires contiguous slices for "
                            "replica " +
                            std::to_string(replica_idx);
                        op.transfer_summary.RecordFailure(
                            replica_type, ErrorCode::INVALID_PARAMS);
                        op.AppendFailureContext(failure_context);
                        continue;
                    }
                    submit_result = transfer_submitter_->submit(
                        replica, op.slices, TransferRequest::WRITE,
                        contiguous_range->ptr, contiguous_range->size,
                        TransferIntent::kUnspecified);
                } else {
                    submit_result = transfer_submitter_->submit(
                        replica, op.slices, TransferRequest::WRITE, nullptr, 0,
                        TransferIntent::kUnspecified);
                }

                if (!submit_result) {
                    std::string failure_context =
                        "Failed to submit transfer for replica " +
                        std::to_string(replica_idx);
                    op.transfer_summary.RecordFailure(replica_type,
                                                      ErrorCode::TRANSFER_FAIL);
                    op.AppendFailureContext(failure_context);
                    continue;
                }

                op.pending_transfers.emplace_back(
                    replica_type, std::move(submit_result.value()));
            }
        }

        VLOG(1) << "Submitted " << op.pending_transfers.size()
                << " transfers for key " << op.key;
    }

    for (auto& [endpoint, group] : target_groups) {
        auto submit_result = transfer_submitter_->submit_batch(
            group.replicas, group.slices, TransferRequest::WRITE,
            TransferIntent::kUnspecified);
        for (auto* op : group.ops) {
            if (!submit_result) {
                op->transfer_summary.RecordFailure(ReplicaType::MEMORY,
                                                   ErrorCode::TRANSFER_FAIL);
                op->AppendFailureContext("Failed to submit batch transfer to " +
                                         endpoint);
                continue;
            }
            op->pending_transfers.emplace_back(ReplicaType::MEMORY,
                                               submit_result->share());
        }
        VLOG(1) << "Submitted batch write of " << group.ops.size()
                << " replicas to " << endpoint;
    }
}

void Client::WaitForTransfers(std::vector<PutOperation>& ops) {
    for (auto& op : ops) {
        // Skip operations that already failed or completed
        if (op.IsResolved()) {
            continue;
        }

        for (size_t i = 0; i < op.pending_transfers.size(); ++i) {
            auto& pending_transfer = op.pending_transfers[i];
            ErrorCode transfer_result = pending_transfer.future.get();
            if (transfer_result != ErrorCode::OK) {
                op.transfer_summary.RecordFailure(pending_transfer.replica_type,
                                                  transfer_result);
                std::string error_context =
                    "Transfer " + std::to_string(i) + " failed";
                op.AppendFailureContext(error_context);
            } else {
                op.transfer_summary.RecordSuccess(
                    pending_transfer.replica_type);
            }
        }

        VLOG(1) << "Transfers finished for key " << op.key << ", success(mem="
                << op.transfer_summary.successful_memory_transfers
                << ", nof=" << op.transfer_summary.successful_nof_transfers
                << "), fail(mem=" << op.transfer_summary.failed_memory_transfers
                << ", nof=" << op.transfer_summary.failed_nof_transfers << ")";
    }
}

std::vector<ErrorCode> Client::WriteDfsReplicas(
    const std::vector<std::string>& keys,
    const std::vector<const std::vector<Slice>*>& slice_lists,
    const std::vector<DistributedFSDescriptor>& descriptors) {
    if (keys.size() != slice_lists.size() ||
        keys.size() != descriptors.size()) {
        return std::vector<ErrorCode>(keys.size(), ErrorCode::INVALID_PARAMS);
    }
    if (keys.empty()) {
        return {};
    }
    if (!dfs_storage_backend_) {
        LOG(ERROR) << "DFS backend is unavailable for synchronous write";
        if (auto* dfs_metric = GetDfsMetricPtr()) {
            dfs_metric->RecordWriteErrors(
                toString(ErrorCode::DFS_SERVICE_UNAVAILABLE),
                static_cast<int64_t>(keys.size()));
        }
        return std::vector<ErrorCode>(keys.size(),
                                      ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    auto* dfs_metric = GetDfsMetricPtr();
    std::vector<ErrorCode> results(keys.size(), ErrorCode::OK);
    std::vector<DfsWriteRequest> requests;
    std::vector<size_t> request_indices;
    // Bytes per request, so the success accounting after BatchWrite does not
    // have to walk the slice lists a second time.
    std::vector<uint64_t> request_bytes;
    std::vector<PinnedBufferPool::Buffer> staging_buffers;
    requests.reserve(keys.size());
    request_indices.reserve(keys.size());
    request_bytes.reserve(keys.size());

    // Staging is the device-to-host copy below; it is timed separately from the
    // BatchWrite call so a slow put can be attributed to the right side of the
    // boundary between them.
    const auto staging_start = std::chrono::steady_clock::now();
    bool staging_performed = false;
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    for (size_t i = 0; i < keys.size(); ++i) {
        if (slice_lists[i] == nullptr) {
            results[i] = ErrorCode::INVALID_PARAMS;
            if (dfs_metric) {
                dfs_metric->RecordWriteErrors(
                    toString(ErrorCode::INVALID_PARAMS));
            }
            continue;
        }

        std::vector<Slice> host_slices;
        host_slices.reserve(slice_lists[i]->size());
        uint64_t key_bytes = 0;
        bool staging_succeeded = true;
        for (const auto& slice : *slice_lists[i]) {
            device::PointerInfo info{};
            auto* device = slice.ptr == nullptr
                               ? nullptr
                               : runtime_accelerator.FindDeviceForPointer(
                                     slice.ptr, &info);
            if (device == nullptr) {
                key_bytes += slice.size;
                host_slices.push_back(slice);
                continue;
            }

            staging_performed = true;
            device->SetContext(info.device_id);
            auto buffer = pinned_buffer_pool_->Acquire(slice.size);
            if (!device->Copy(buffer.data, slice.ptr, slice.size,
                              device::CopyDirection::kDeviceToHost)) {
                LOG(ERROR) << "DFS D2H staging failed, action=stage, key="
                           << keys[i] << ", size=" << slice.size
                           << ", device_id=" << info.device_id;
                pinned_buffer_pool_->Release(std::move(buffer));
                results[i] = ErrorCode::TRANSFER_FAIL;
                staging_succeeded = false;
                break;
            }
            key_bytes += slice.size;
            host_slices.emplace_back(Slice{buffer.data, slice.size});
            staging_buffers.push_back(std::move(buffer));
        }
        if (!staging_succeeded) {
            // Counted as a DFS write failure: the key was destined for DFS and
            // never got there, even though the backend was never reached.
            if (dfs_metric) {
                dfs_metric->RecordWriteErrors(
                    toString(ErrorCode::TRANSFER_FAIL));
            }
            continue;
        }

        requests.push_back(
            DfsWriteRequest{keys[i], descriptors[i], std::move(host_slices)});
        request_indices.push_back(i);
        request_bytes.push_back(key_bytes);
    }
    if (dfs_metric && staging_performed) {
        dfs_metric->ObserveWriteStaging(elapsed_us_since(staging_start));
    }

    const auto write_start = std::chrono::steady_clock::now();
    auto write_results = dfs_storage_backend_->BatchWrite(requests);
    const auto write_latency_us = elapsed_us_since(write_start);
    int64_t success_keys = 0;
    uint64_t success_bytes = 0;
    if (write_results.size() != requests.size()) {
        LOG(ERROR) << "DFS BatchWrite response size mismatch: expected "
                   << requests.size() << ", got " << write_results.size();
        for (size_t index : request_indices) {
            results[index] = ErrorCode::INTERNAL_ERROR;
        }
        if (dfs_metric && !request_indices.empty()) {
            dfs_metric->RecordWriteErrors(
                toString(ErrorCode::INTERNAL_ERROR),
                static_cast<int64_t>(request_indices.size()));
        }
    } else {
        for (size_t i = 0; i < write_results.size(); ++i) {
            results[request_indices[i]] =
                write_results[i] ? ErrorCode::OK : write_results[i].error();
            if (write_results[i]) {
                ++success_keys;
                success_bytes += request_bytes[i];
            } else if (dfs_metric) {
                dfs_metric->RecordWriteErrors(
                    toString(write_results[i].error()));
            }
        }
    }
    // Latency is per batch, so it is only observed when the batch delivered
    // something; a fully failed batch would otherwise pollute the histogram
    // with error-path timings.
    if (dfs_metric && success_keys > 0) {
        dfs_metric->ObserveWrite(success_keys,
                                 static_cast<int64_t>(success_bytes),
                                 write_latency_us);
    }
    VLOG(1) << "DFS batch write finished, action=write, requested="
            << requests.size() << ", succeeded=" << success_keys
            << ", bytes=" << success_bytes
            << ", latency_us=" << write_latency_us;

    for (auto& buffer : staging_buffers) {
        pinned_buffer_pool_->Release(std::move(buffer));
    }
    return results;
}

void Client::SubmitDfsWrites(std::vector<PutOperation>& ops) {
    std::vector<std::string> keys;
    std::vector<const std::vector<Slice>*> slice_lists;
    std::vector<DistributedFSDescriptor> descriptors;
    std::vector<size_t> op_indices;
    auto* dfs_metric = GetDfsMetricPtr();
    int64_t skipped_writes = 0;

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved() ||
            op.transfer_summary.allocated_dfs_replicas == 0) {
            continue;
        }
        // A DFS replica was allocated but is abandoned because a peer replica
        // failed first. Without this counter the write simply disappears: no
        // error is recorded against DFS and no I/O is attempted.
        if (!NonDfsTransfersSucceeded(op.transfer_summary)) {
            ++skipped_writes;
            VLOG(1) << "Skipping DFS write, action=skip, key=" << op.key
                    << ", reason=non_dfs_transfer_failed";
            continue;
        }

        auto dfs_it = std::find_if(op.replicas.begin(), op.replicas.end(),
                                   [](const Replica::Descriptor& replica) {
                                       return replica.is_dfs_replica();
                                   });
        if (dfs_it == op.replicas.end()) {
            op.transfer_summary.RecordFailure(ReplicaType::DFS,
                                              ErrorCode::INVALID_REPLICA);
            op.AppendFailureContext("Allocated DFS replica has no descriptor");
            if (dfs_metric) {
                dfs_metric->RecordWriteErrors(
                    toString(ErrorCode::INVALID_REPLICA));
            }
            continue;
        }
        keys.push_back(op.key);
        slice_lists.push_back(&op.slices);
        descriptors.push_back(dfs_it->get_dfs_descriptor());
        op_indices.push_back(i);
    }
    if (dfs_metric && skipped_writes > 0) {
        dfs_metric->RecordSkippedWrites(skipped_writes);
    }

    auto results = WriteDfsReplicas(keys, slice_lists, descriptors);
    for (size_t i = 0; i < results.size(); ++i) {
        auto& op = ops[op_indices[i]];
        if (results[i] == ErrorCode::OK) {
            op.transfer_summary.RecordSuccess(ReplicaType::DFS);
        } else {
            op.transfer_summary.RecordFailure(ReplicaType::DFS, results[i]);
            op.AppendFailureContext("Synchronous DFS write failed: " +
                                    toString(results[i]));
        }
    }
}

void Client::FinalizeBatchPut(std::vector<PutOperation>& ops) {
    struct BatchFinalizeGroup {
        std::vector<std::string> keys;
        std::vector<size_t> indices;
    };

    BatchFinalizeGroup end_all_group;
    BatchFinalizeGroup end_memory_group;
    BatchFinalizeGroup end_nof_group;
    BatchFinalizeGroup revoke_all_group;
    BatchFinalizeGroup revoke_memory_group;
    BatchFinalizeGroup revoke_nof_group;

    std::vector<size_t> pending_finalize_actions(ops.size(), 0);
    std::vector<bool> should_succeed(ops.size(), false);
    std::vector<ErrorCode> terminal_errors(ops.size(), ErrorCode::OK);
    std::vector<std::optional<ErrorCode>> finalize_rpc_errors(ops.size());

    auto add_group_entry = [](BatchFinalizeGroup& group, const std::string& key,
                              size_t index) {
        group.keys.emplace_back(key);
        group.indices.emplace_back(index);
    };

    auto add_finalize_action =
        [&](const std::optional<ReplicaType>& replica_type, bool is_end,
            const std::string& key, size_t index) {
            if (!replica_type.has_value()) {
                return;
            }
            switch (*replica_type) {
                case ReplicaType::ALL:
                    add_group_entry(is_end ? end_all_group : revoke_all_group,
                                    key, index);
                    ++pending_finalize_actions[index];
                    break;
                case ReplicaType::MEMORY:
                    add_group_entry(
                        is_end ? end_memory_group : revoke_memory_group, key,
                        index);
                    ++pending_finalize_actions[index];
                    break;
                case ReplicaType::NOF_SSD:
                    add_group_entry(is_end ? end_nof_group : revoke_nof_group,
                                    key, index);
                    ++pending_finalize_actions[index];
                    break;
                default:
                    LOG(ERROR) << "Unexpected replica type in batch finalize: "
                               << *replica_type;
                    finalize_rpc_errors[index] = ErrorCode::INVALID_PARAMS;
                    break;
            }
        };

    auto complete_finalize_action = [&](size_t index) {
        if (pending_finalize_actions[index] > 0) {
            --pending_finalize_actions[index];
        }
    };

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved()) {
            if (!op.IsSuccessful() && !op.replicas.empty()) {
                terminal_errors[i] = op.result.has_value()
                                         ? ErrorCode::INTERNAL_ERROR
                                         : op.result.error();
                add_finalize_action(ReplicaType::ALL, false, op.key, i);
            }
            continue;
        }
        if (op.replicas.empty()) {
            op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                PutOperationState::MASTER_FAILED,
                                "Operation has no replicas to finalize");
            continue;
        }

        const auto finalize_decision = DetermineFinalizeDecision(
            op.ToReplicateConfig(), op.transfer_summary);
        should_succeed[i] = finalize_decision.success;
        terminal_errors[i] = finalize_decision.error;
        add_finalize_action(finalize_decision.end_type, true, op.key, i);
        add_finalize_action(finalize_decision.revoke_type, false, op.key, i);
    }

    auto process_end_group = [&](BatchFinalizeGroup& group,
                                 ReplicaType replica_type) {
        if (group.keys.empty()) {
            return;
        }
        std::vector<ObjectMeta> object_metas;
        object_metas.reserve(group.indices.size());
        for (size_t i = 0; i < group.indices.size(); ++i) {
            object_metas.emplace_back(ObjectMeta{
                group.keys[i], ops[group.indices[i]].object_checksum});
        }
        auto responses = master_client_.BatchPutEnd(object_metas, replica_type);
        if (responses.size() != group.keys.size()) {
            for (size_t idx : group.indices) {
                finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                complete_finalize_action(idx);
            }
            return;
        }
        for (size_t i = 0; i < responses.size(); ++i) {
            const size_t op_idx = group.indices[i];
            if (!responses[i]) {
                finalize_rpc_errors[op_idx] = responses[i].error();
                LOG(ERROR) << "Failed to BatchPutEnd key " << group.keys[i]
                           << ": " << toString(responses[i].error());
                complete_finalize_action(op_idx);
                continue;
            }
            complete_finalize_action(op_idx);
        }
    };

    auto process_revoke_group = [&](BatchFinalizeGroup& group,
                                    ReplicaType replica_type) {
        if (group.keys.empty()) {
            return;
        }
        auto responses =
            master_client_.BatchPutRevoke(group.keys, replica_type);
        if (responses.size() != group.keys.size()) {
            for (size_t idx : group.indices) {
                finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                complete_finalize_action(idx);
            }
            return;
        }
        for (size_t i = 0; i < responses.size(); ++i) {
            const size_t op_idx = group.indices[i];
            if (!responses[i]) {
                finalize_rpc_errors[op_idx] = responses[i].error();
                LOG(ERROR) << "Failed to BatchPutRevoke key " << group.keys[i]
                           << ": " << toString(responses[i].error());
                complete_finalize_action(op_idx);
                continue;
            }
            complete_finalize_action(op_idx);
        }
    };

    process_end_group(end_all_group, ReplicaType::ALL);
    process_end_group(end_memory_group, ReplicaType::MEMORY);
    process_end_group(end_nof_group, ReplicaType::NOF_SSD);
    process_revoke_group(revoke_all_group, ReplicaType::ALL);
    process_revoke_group(revoke_memory_group, ReplicaType::MEMORY);
    process_revoke_group(revoke_nof_group, ReplicaType::NOF_SSD);

    auto append_finalize_error_context = [&](PutOperation& op, size_t index) {
        if (finalize_rpc_errors[index].has_value()) {
            op.AppendFailureContext("Batch finalization RPC failed: " +
                                    toString(*finalize_rpc_errors[index]));
        }
        if (pending_finalize_actions[index] != 0) {
            op.AppendFailureContext(
                "Operation has unfinished finalize actions");
        }
    };

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved()) {
            if (!op.IsSuccessful()) {
                append_finalize_error_context(op, i);
            }
            continue;
        }
        if (finalize_rpc_errors[i].has_value() ||
            pending_finalize_actions[i] != 0) {
            if (!should_succeed[i] && terminal_errors[i] != ErrorCode::OK) {
                append_finalize_error_context(op, i);
                op.SetTerminalError(
                    terminal_errors[i], PutOperationState::TRANSFER_FAILED,
                    op.failure_context.value_or(
                        "Replica transfer failed before finalize"));
            } else if (finalize_rpc_errors[i].has_value()) {
                op.SetTerminalError(*finalize_rpc_errors[i],
                                    PutOperationState::FINALIZE_FAILED,
                                    "Batch finalization RPC failed");
            } else {
                op.SetTerminalError(
                    ErrorCode::INTERNAL_ERROR,
                    PutOperationState::FINALIZE_FAILED,
                    "Operation has unfinished finalize actions");
            }
            continue;
        }
        if (should_succeed[i]) {
            op.SetSuccess();
            continue;
        }
        op.SetTerminalError(terminal_errors[i],
                            PutOperationState::TRANSFER_FAILED,
                            op.failure_context.value_or(
                                "Replica transfer failed before finalize"));
    }
}

void Client::FinalizeBatchUpsert(std::vector<PutOperation>& ops) {
    std::vector<ObjectMeta> successful_object_metas;
    std::vector<size_t> successful_indices;
    std::vector<std::string> failed_keys;
    std::vector<size_t> failed_indices;

    successful_object_metas.reserve(ops.size());
    successful_indices.reserve(ops.size());
    failed_keys.reserve(ops.size());
    failed_indices.reserve(ops.size());

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (!op.IsResolved() && !op.replicas.empty() &&
            HasExpectedReplicaAllocation(op.ToReplicateConfig(),
                                         op.transfer_summary) &&
            AllAllocatedTransfersSucceeded(op.transfer_summary)) {
            successful_object_metas.emplace_back(
                ObjectMeta{op.key, op.object_checksum});
            successful_indices.emplace_back(i);
            continue;
        }

        if (!op.IsResolved() && !op.replicas.empty()) {
            const auto error = op.transfer_summary.first_error == ErrorCode::OK
                                   ? ErrorCode::TRANSFER_FAIL
                                   : op.transfer_summary.first_error;
            op.SetTerminalError(
                error, PutOperationState::TRANSFER_FAILED,
                op.failure_context.value_or(
                    "Replica transfer failed before upsert finalize"));
        }
        if (!op.IsSuccessful() && !op.replicas.empty()) {
            failed_keys.emplace_back(op.key);
            failed_indices.emplace_back(i);
        }
    }

    // Process successful operations
    std::vector<std::string> finalized_keys;
    if (!successful_object_metas.empty()) {
        finalized_keys.reserve(successful_object_metas.size());
        auto end_responses =
            master_client_.BatchUpsertEnd(successful_object_metas);
        if (end_responses.size() != successful_object_metas.size()) {
            LOG(ERROR) << "BatchUpsertEnd response size mismatch: expected "
                       << successful_object_metas.size() << ", got "
                       << end_responses.size();
            for (size_t idx : successful_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchUpsertEnd response size mismatch");
            }
        } else {
            for (size_t i = 0; i < end_responses.size(); ++i) {
                const size_t op_idx = successful_indices[i];
                if (!end_responses[i]) {
                    LOG(ERROR) << "Failed to finalize upsert for key "
                               << successful_object_metas[i].key << ": "
                               << toString(end_responses[i].error());
                    ops[op_idx].SetError(end_responses[i].error(),
                                         "BatchUpsertEnd failed");
                } else {
                    ops[op_idx].SetSuccess();
                    finalized_keys.emplace_back(successful_object_metas[i].key);
                    VLOG(1) << "Successfully completed upsert for key "
                            << successful_object_metas[i].key;
                }
            }
        }
    }

    // Success-side invalidation for finalized upserts only: a concurrent read
    // between StartBatchUpsert()'s pre-invalidation and BatchUpsertEnd could
    // have read the old value and submitted an async hot-cache fill with a
    // still-valid token. Invalidate again now that the new values are
    // committed so those stale fills cannot publish.
    if (hot_cache_ && !finalized_keys.empty()) {
        hot_cache_->RemoveHotKeys(finalized_keys);
    }

    // Process failed operations that need cleanup
    if (!failed_keys.empty()) {
        auto revoke_responses = master_client_.BatchUpsertRevoke(failed_keys);
        if (revoke_responses.size() != failed_keys.size()) {
            LOG(ERROR) << "BatchUpsertRevoke response size mismatch: expected "
                       << failed_keys.size() << ", got "
                       << revoke_responses.size();
            for (size_t idx : failed_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchUpsertRevoke response size mismatch");
            }
        } else {
            for (size_t i = 0; i < revoke_responses.size(); ++i) {
                const size_t op_idx = failed_indices[i];
                if (!revoke_responses[i]) {
                    LOG(ERROR)
                        << "Failed to revoke upsert for key " << failed_keys[i]
                        << ": " << toString(revoke_responses[i].error());
                    std::string original_context =
                        ops[op_idx].failure_context.value_or("unknown error");
                    ops[op_idx].failure_context =
                        original_context + "; revoke also failed";
                } else {
                    LOG(INFO) << "Successfully revoked failed upsert for key "
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

    int no_available_handle_count = 0;
    for (const auto& op : ops) {
        // With the new structure, result is always set (never nullopt)
        results.emplace_back(op.result);

        // Additional validation and logging for debugging
        if (!op.result.has_value()) {
            // If error == object already exist, consider as ok (Put semantics).
            // UpsertStart never returns this error, so this branch is
            // unreachable for BatchUpsert — kept for BatchPut compatibility.
            if (op.result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                results.back() = {};
                continue;
            }
            if (op.result.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                no_available_handle_count++;
            } else {
                LOG(ERROR) << "Operation for key " << op.key
                           << " failed: " << toString(op.result.error())
                           << (op.failure_context
                                   ? (" (" + *op.failure_context + ")")
                                   : "");
            }
        } else {
            VLOG(1) << "Operation for key " << op.key
                    << " completed successfully";
        }
    }
    if (no_available_handle_count > 0) {
        LOG(WARNING) << "BatchPut failed for " << no_available_handle_count
                     << " keys" << PUT_NO_SPACE_HELPER_STR;
    }

    return results;
}

void Client::StageWriteBuffersForRemoteReplicas(
    std::vector<PutOperation>& ops, const WriteBufferStager& stager) {
    if (!stager) return;

    for (auto& op : ops) {
        if (op.IsResolved() || op.replicas.empty() ||
            std::all_of(op.replicas.begin(), op.replicas.end(),
                        [this](const Replica::Descriptor& replica) {
                            return CanUseLocalMemcpy(replica);
                        })) {
            continue;
        }
        auto staged_slices = stager(op.slices);
        if (!staged_slices) {
            op.SetError(staged_slices.error(),
                        "Failed to stage non-local write buffers");
            continue;
        }
        op.slices = std::move(*staged_slices);
        VLOG(1) << "Staged write buffers for non-local replica, key=" << op.key;
    }
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    return BatchPut(keys, batched_slices, config, {});
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config, const WriteBufferStager& stager,
    bool group_by_target) {
    if (!config.ValidSessionIds(keys.size()) ||
        !config.HasValidKeyHints(keys.size())) {
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    ReplicateConfig client_cfg = AttachHostId(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    ComputeBatchObjectChecksums(ops);
    if (client_cfg.prefer_alloc_in_same_node) {
        if (auto err = ValidatePreferSameNodeWriteConfig(client_cfg)) {
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(*err));
        }
    }
    StartBatchPut(ops, client_cfg);
    StageWriteBuffersForRemoteReplicas(ops, stager);

    auto t0 = std::chrono::steady_clock::now();
    SubmitTransfers(ops,
                    group_by_target || client_cfg.prefer_alloc_in_same_node);
    WaitForTransfers(ops);
    SubmitDfsWrites(ops);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }

    FinalizeBatchPut(ops);
    return CollectResults(ops);
}

// Defined after PutOperation: std::vector<PutOperation> must be instantiated
// on a complete type (clang-based compilers diagnose this; GCC defers it).
std::vector<tl::expected<void, ErrorCode>> Client::BatchUpsert(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    return BatchUpsert(keys, batched_slices, config, {});
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchUpsert(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config, const WriteBufferStager& stager,
    bool group_by_target) {
    if (!config.ValidSessionIds(keys.size()) ||
        !config.HasValidKeyHints(keys.size())) {
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    ReplicateConfig client_cfg = AttachHostId(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    ComputeBatchObjectChecksums(ops);
    if (client_cfg.prefer_alloc_in_same_node) {
        if (auto err = ValidatePreferSameNodeWriteConfig(client_cfg)) {
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(*err));
        }
    }

    StartBatchUpsert(ops, client_cfg);
    StageWriteBuffersForRemoteReplicas(ops, stager);
    auto t0 = std::chrono::steady_clock::now();
    SubmitTransfers(ops,
                    group_by_target || client_cfg.prefer_alloc_in_same_node);
    WaitForTransfers(ops);
    SubmitDfsWrites(ops);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }

    FinalizeBatchUpsert(ops);
    return CollectResults(ops);
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
Client::StartBatchPutForSizes(const std::vector<std::string>& keys,
                              const std::vector<uint64_t>& object_sizes,
                              const ReplicateConfig& config) {
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results(keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (!config.ValidSessionIds(keys.size()) ||
        !config.HasValidKeyHints(keys.size())) {
        return results;
    }
    if (keys.size() != object_sizes.size()) {
        LOG(ERROR) << "StartBatchPutForSizes size mismatch: keys="
                   << keys.size() << ", sizes=" << object_sizes.size();
        return results;
    }

    ReplicateConfig client_cfg = AttachHostId(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    std::vector<std::vector<Slice>> batched_slices(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        batched_slices[i] = {Slice{nullptr, object_sizes[i]}};
    }
    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    StartBatchPut(ops, client_cfg);

    for (size_t i = 0; i < ops.size(); ++i) {
        if (ops[i].IsResolved()) {
            results[i] = tl::unexpected(ops[i].result.error());
            continue;
        }
        results[i] = std::move(ops[i].replicas);
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPutEnd(
    const std::vector<ObjectMeta>& object_metas, ReplicaType replica_type) {
    return master_client_.BatchPutEnd(object_metas, replica_type);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPutRevoke(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    return master_client_.BatchPutRevoke(keys, replica_type);
}

}  // namespace mooncake
