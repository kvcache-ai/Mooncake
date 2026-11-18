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

ErrorCode ScatterFragmentError(const Status& status) {
    return status.IsInvalidArgument() ? ErrorCode::INVALID_PARAMS
                                      : ErrorCode::TRANSFER_FAIL;
}

void MarkScatterOperationFailure(
    std::vector<tl::expected<int64_t, ErrorCode>>& results,
    const std::vector<std::optional<ErrorCode>>& entry_errors,
    const Status& status) {
    const auto error = ScatterFragmentError(status);
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].has_value() && !entry_errors[i].has_value()) {
            results[i] = tl::unexpected(error);
        }
    }
}

// Collects the fragments of many ranged entries into a single scatter submit.
//
// One submit per entry would hand the transport one tiny transfer per key per
// layer; batching them lets the transport coalesce every fragment bound for the
// same segment into one transfer, which is what keeps layer-wise sessions at
// full bandwidth.
//
// ScatterTransferRange holds non-owning spans, so the offset/length storage
// lives here and must outlive the submit. Both vectors are reserved to the
// exact fragment count up front so pushes never reallocate under a live span.
class ScatterRangeBuilder {
   public:
    explicit ScatterRangeBuilder(size_t fragment_count)
        : zero_offsets_(fragment_count, 0) {
        remote_offsets_.reserve(fragment_count);
        lengths_.reserve(fragment_count);
        ranges_.reserve(fragment_count);
    }

    // `error_slot` is shared by every fragment of one entry and keeps the first
    // failure. Callbacks run on the waiting thread, so no locking is needed.
    void Add(TransferRequest::OpCode opcode, const std::string& remote_segment,
             uint64_t remote_base, size_t remote_size, const Slice& slice,
             uint64_t remote_offset, std::optional<ErrorCode>* error_slot) {
        const size_t index = remote_offsets_.size();
        remote_offsets_.push_back(static_cast<size_t>(remote_offset));
        lengths_.push_back(slice.size);
        ranges_.push_back(TransferEngine::ScatterTransferRange{
            .opcode = opcode,
            .remote_segment = remote_segment,
            .remote_base_offset = remote_base,
            .remote_size = remote_size,
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

    void Add(TransferRequest::OpCode opcode,
             const AllocatedBuffer::Descriptor& handle, const Slice& slice,
             uint64_t remote_offset, std::optional<ErrorCode>* error_slot) {
        Add(opcode, handle.transport_endpoint_, handle.buffer_address_,
            static_cast<size_t>(handle.size_), slice, remote_offset,
            error_slot);
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

void CompleteScatterRanges(
    Client& client, ScatterRangeBuilder& builder,
    std::vector<tl::expected<int64_t, ErrorCode>>& results,
    const std::vector<std::optional<ErrorCode>>& entry_errors,
    const char* submit_fail_msg, const char* entry_fail_prefix) {
    if (builder.empty()) {
        return;
    }
    auto operation = client.SubmitScatter(builder.ranges());
    if (!operation) {
        LOG(ERROR) << submit_fail_msg;
        for (auto& result : results) {
            if (result.has_value()) {
                result = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return;
    }
    (void)operation->wait();
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) {
            continue;
        }
        LOG(ERROR) << entry_fail_prefix << ", entry=" << i
                   << ", error=" << static_cast<int>(entry_errors[i].value());
        results[i] = tl::unexpected(entry_errors[i].value());
    }
}

}  // namespace

std::optional<TransferEngine::ScatterTransferOperation> Client::SubmitScatter(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers,
    TransferIntent intent) {
    if (!transfer_engine_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    auto mutable_transfers = transfers;
    for (auto& transfer : mutable_transfers)
        transfer.intent_type = static_cast<int>(intent);
    return transfer_engine_->submitScatter(mutable_transfers);
}

std::optional<StoreScatterTransferOperation> Client::SubmitScatterNative(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers,
    TransferIntent intent) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitNativeScatter(transfers, intent);
}

tl::expected<TransferFuture, ErrorCode> Client::SubmitSingleTransfer(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code, TransferIntent intent) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::optional<TransferFuture> future;
    if (replica_descriptor.is_nof_replica()) {
        auto contiguous_range = GetContiguousSliceRange(slices);
        if (!contiguous_range.has_value()) {
            LOG(ERROR) << "NoF transfer requires contiguous slices";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        future = transfer_submitter_->submit(replica_descriptor, slices,
                                             op_code, contiguous_range->ptr,
                                             contiguous_range->size, intent);
    } else {
        future = transfer_submitter_->submit(replica_descriptor, slices,
                                             op_code, nullptr, 0, intent);
    }
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return tl::unexpected(ErrorCode::TRANSFER_FAIL);
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return std::move(*future);
}

ErrorCode Client::TransferData(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices,
                               TransferRequest::OpCode op_code,
                               TransferIntent intent) {
    auto future =
        SubmitSingleTransfer(replica_descriptor, slices, op_code, intent);
    if (!future) {
        return future.error();
    }
    return future->get();
}

std::optional<TransferFuture> Client::SubmitRangeRead(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitRangeRead(
        replica_descriptor, slices, src_offset, TransferIntent::kForegroundGet);
}

std::optional<TransferFuture> Client::SubmitRangeWrite(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t dst_offset, TransferIntent intent) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitRangeWrite(replica_descriptor, slices,
                                                 dst_offset, intent);
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchTransferReadRanges(
    const std::vector<Replica::Descriptor>& replicas,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& src_offsets,
    TransferIntent intent, std::span<const QueryResult* const> query_results) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas.size() != slices.size() ||
        replicas.size() != src_offsets.size() ||
        (!query_results.empty() && query_results.size() != replicas.size())) {
        LOG(ERROR) << "BatchTransferReadRanges size mismatch: replicas="
                   << replicas.size() << ", slices=" << slices.size()
                   << ", offsets=" << src_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (const auto& entry : slices) {
        fragment_count += entry.size();
    }

    auto select_replica = MakeReadReplicaSelector();
    // Every fragment of every entry goes into one scatter submit so the
    // transport sees the whole layer at once instead of one transfer per key.
    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(replicas.size());
    for (size_t i = 0; i < replicas.size(); ++i) {
        if (slices[i].size() != src_offsets[i].size()) {
            LOG(ERROR) << "BatchTransferReadRanges fragment count mismatch, "
                       << "entry=" << i << ", slices=" << slices[i].size()
                       << ", offsets=" << src_offsets[i].size();
            continue;  // results[i] stays INVALID_PARAMS
        }
        if (!replicas[i].is_memory_replica()) {
            LOG(ERROR) << "Range read requires a memory replica, entry=" << i;
            continue;
        }

        Replica::Descriptor selected;
        const auto* selected_replica = &replicas[i];
        int64_t transferred = 0;
        for (size_t j = 0; j < slices[i].size(); ++j) {
            if (!query_results.empty() && query_results[i]) {
                if (select_replica(query_results[i]->replicas,
                                   std::span<const Slice>(&slices[i][j], 1),
                                   selected) != ErrorCode::OK ||
                    !selected.is_memory_replica()) {
                    entry_errors[i] = ErrorCode::INVALID_REPLICA;
                    break;
                }
                selected_replica = &selected;
            }
            const auto& handle =
                selected_replica->get_memory_descriptor().buffer_descriptor;
            builder.Add(TransferRequest::READ, handle, slices[i][j],
                        src_offsets[i][j], &entry_errors[i]);
            transferred += static_cast<int64_t>(slices[i][j].size);
        }
        results[i] = entry_errors[i]
                         ? tl::expected<int64_t, ErrorCode>(
                               tl::unexpected(*entry_errors[i]))
                         : tl::expected<int64_t, ErrorCode>(transferred);
    }

    CompleteScatterRanges(*this, builder, results, entry_errors,
                          "Failed to submit batch range read",
                          "Range read failed");
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>>
Client::BatchTransferReadOffloadRanges(
    const std::string& transfer_engine_addr,
    const std::vector<uint64_t>& remote_bases,
    const std::vector<size_t>& remote_sizes,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& src_offsets) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        remote_bases.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (remote_bases.size() != remote_sizes.size() ||
        remote_bases.size() != slices.size() ||
        remote_bases.size() != src_offsets.size()) {
        LOG(ERROR) << "BatchTransferReadOffloadRanges size mismatch: bases="
                   << remote_bases.size() << ", sizes=" << remote_sizes.size()
                   << ", slices=" << slices.size()
                   << ", offsets=" << src_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (const auto& entry : slices) {
        fragment_count += entry.size();
    }

    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(remote_bases.size());
    for (size_t i = 0; i < remote_bases.size(); ++i) {
        if (slices[i].size() != src_offsets[i].size()) {
            LOG(ERROR)
                << "BatchTransferReadOffloadRanges fragment count mismatch, "
                << "entry=" << i << ", slices=" << slices[i].size()
                << ", offsets=" << src_offsets[i].size();
            continue;
        }
        int64_t transferred = 0;
        for (size_t j = 0; j < slices[i].size(); ++j) {
            builder.Add(TransferRequest::READ, transfer_engine_addr,
                        remote_bases[i], remote_sizes[i], slices[i][j],
                        src_offsets[i][j], &entry_errors[i]);
            transferred += static_cast<int64_t>(slices[i][j].size);
        }
        results[i] = transferred;  // optimistic; corrected on await
    }

    if (builder.empty()) {
        return results;
    }

    auto operation = SubmitScatterNative(builder.ranges());
    if (!operation) {
        LOG(ERROR) << "Failed to submit batch offload range read";
        for (auto& result : results) {
            if (result.has_value()) {
                result = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return results;
    }
    const auto status = operation->wait();
    if (!status.ok()) {
        LOG(ERROR) << "Batch offload range read scatter operation failed: "
                   << status.ToString();
        MarkScatterOperationFailure(results, entry_errors, status);
    }

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) {
            continue;
        }
        LOG(ERROR) << "Offload range read failed, entry=" << i
                   << ", error=" << static_cast<int>(entry_errors[i].value());
        results[i] = tl::unexpected(entry_errors[i].value());
    }
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchTransferWriteRanges(
    const std::vector<std::vector<Replica::Descriptor>>& replicas_per_entry,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& dst_offsets,
    TransferIntent intent) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas_per_entry.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas_per_entry.size() != slices.size() ||
        replicas_per_entry.size() != dst_offsets.size()) {
        LOG(ERROR) << "BatchTransferWriteRanges size mismatch: entries="
                   << replicas_per_entry.size() << ", slices=" << slices.size()
                   << ", offsets=" << dst_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (size_t i = 0; i < replicas_per_entry.size(); ++i) {
        for (const auto& replica : replicas_per_entry[i]) {
            if (replica.is_memory_replica()) {
                fragment_count += slices[i].size();
            }
        }
    }

    // One scatter submit covers every replica of every entry, so replication
    // fans out inside a single transfer instead of one submit per fragment.
    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(
        replicas_per_entry.size());
    for (size_t i = 0; i < replicas_per_entry.size(); ++i) {
        if (slices[i].size() != dst_offsets[i].size()) {
            LOG(ERROR) << "BatchTransferWriteRanges fragment count mismatch, "
                       << "entry=" << i << ", slices=" << slices[i].size()
                       << ", offsets=" << dst_offsets[i].size();
            continue;  // results[i] stays INVALID_PARAMS
        }

        // Logical bytes: counted once per fragment, not per replica.
        int64_t transferred = 0;
        for (const auto& slice : slices[i]) {
            transferred += static_cast<int64_t>(slice.size);
        }
        bool submitted = false;
        for (const auto& replica : replicas_per_entry[i]) {
            if (!replica.is_memory_replica()) {
                continue;
            }
            const auto& handle =
                replica.get_memory_descriptor().buffer_descriptor;
            for (size_t j = 0; j < slices[i].size(); ++j) {
                builder.Add(TransferRequest::WRITE, handle, slices[i][j],
                            dst_offsets[i][j], &entry_errors[i]);
                submitted = true;
            }
        }
        if (!submitted) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        results[i] = transferred;  // optimistic; corrected on await
    }

    if (builder.empty()) {
        return results;
    }

    auto operation = SubmitScatterNative(builder.ranges(), intent);
    if (!operation) {
        LOG(ERROR) << "Failed to submit batch range write";
        for (auto& result : results) {
            if (result.has_value()) {
                result = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return results;
    }
    const auto status = operation->wait();
    if (!status.ok()) {
        LOG(ERROR) << "Batch range write scatter operation failed: "
                   << status.ToString();
        MarkScatterOperationFailure(results, entry_errors, status);
    }

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) {
            continue;
        }
        LOG(ERROR) << "Range write failed, entry=" << i
                   << ", error=" << static_cast<int>(entry_errors[i].value());
        results[i] = tl::unexpected(entry_errors[i].value());
    }
    return results;
}

ErrorCode Client::TransferReadInternal(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    auto future = SubmitRangeRead(replica_descriptor, slices, src_offset);
    if (!future) {
        LOG(ERROR) << "Failed to submit range read operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

std::vector<std::pair<ReplicaType, ErrorCode>>
Client::WriteMemoryAndNofReplicas(
    const std::vector<Replica::Descriptor>& replicas,
    std::vector<Slice>& slices) {
    // Submit all before waiting, as BatchPut does, so the writes overlap.
    std::vector<std::pair<ReplicaType, tl::expected<TransferFuture, ErrorCode>>>
        pending;
    pending.reserve(replicas.size());
    std::vector<std::pair<ReplicaType, ErrorCode>> results;
    results.reserve(replicas.size());
    for (const auto& replica : replicas) {
        if (replica.is_memory_replica() || replica.is_nof_replica()) {
            pending.emplace_back(
                replica.is_memory_replica() ? ReplicaType::MEMORY
                                            : ReplicaType::NOF_SSD,
                SubmitSingleTransfer(replica, slices, TransferRequest::WRITE));
        }
    }
    for (auto& [replica_type, future] : pending) {
        results.emplace_back(replica_type,
                             future ? future->get() : future.error());
    }
    return results;
}

ErrorCode Client::TransferWrite(const Replica::Descriptor& replica_descriptor,
                                std::vector<Slice>& slices,
                                TransferIntent intent) {
    return TransferData(replica_descriptor, slices, TransferRequest::WRITE,
                        intent);
}

ErrorCode Client::TransferWriteRange(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t dst_offset, TransferIntent intent) {
    auto future =
        SubmitRangeWrite(replica_descriptor, slices, dst_offset, intent);
    if (!future) {
        LOG(ERROR) << "Failed to submit range write operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();
    return future->get();
}

ErrorCode Client::TransferRead(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices,
                               TransferIntent intent) {
    size_t total_size = 0;
    if (replica_descriptor.is_memory_replica()) {
        auto& mem_desc = replica_descriptor.get_memory_descriptor();
        total_size = mem_desc.buffer_descriptor.size_;
    } else if (replica_descriptor.is_nof_replica()) {
        auto& nof_desc = replica_descriptor.get_nof_descriptor();
        total_size = nof_desc.buffer_descriptor.size_;
    } else if (replica_descriptor.is_disk_replica()) {
        auto& disk_desc = replica_descriptor.get_disk_descriptor();
        total_size = disk_desc.object_size;
    } else if (replica_descriptor.is_local_disk_replica()) {
        auto& disk_desc = replica_descriptor.get_local_disk_descriptor();
        total_size = disk_desc.object_size;
    } else if (replica_descriptor.is_dfs_replica()) {
        total_size = replica_descriptor.get_dfs_descriptor().object_size;
    }

    size_t slices_size = CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    if (replica_descriptor.is_dfs_replica()) {
        LOG(ERROR) << "DFS reads require object key context";
        return ErrorCode::INVALID_REPLICA;
    }

    return TransferData(replica_descriptor, slices, TransferRequest::READ,
                        intent);
}

ErrorCode Client::ReadDfsReplica(const std::string& key,
                                 const Replica::Descriptor& replica_descriptor,
                                 std::vector<Slice>& slices) {
    if (!replica_descriptor.is_dfs_replica()) {
        return ErrorCode::INVALID_REPLICA;
    }
    if (!dfs_storage_backend_) {
        LOG(ERROR) << "DFS backend is not initialized";
        if (auto* dfs_metric = GetDfsMetricPtr()) {
            dfs_metric->RecordReadErrors(
                toString(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        }
        return ErrorCode::DFS_SERVICE_UNAVAILABLE;
    }

    auto* dfs_metric = GetDfsMetricPtr();
    const auto& desc = replica_descriptor.get_dfs_descriptor();
    std::vector<DfsReadRequest> requests{
        DfsReadRequest{.key = key,
                       .descriptor = desc,
                       .slices = slices,
                       .source_replica_id = replica_descriptor.id}};
    const auto read_start = std::chrono::steady_clock::now();
    auto results = dfs_storage_backend_->BatchRead(requests);
    const auto read_latency_us = elapsed_us_since(read_start);
    if (results.size() != 1) {
        LOG(ERROR) << "DFS BatchRead response size mismatch for key " << key;
        if (dfs_metric) {
            dfs_metric->RecordReadErrors(toString(ErrorCode::INTERNAL_ERROR));
        }
        return ErrorCode::INTERNAL_ERROR;
    }
    if (!results[0]) {
        LOG(ERROR) << "DFS read failed, action=read, key=" << key
                   << ", offset=" << desc.offset
                   << ", size=" << desc.object_size
                   << ", latency_us=" << read_latency_us
                   << ", error=" << results[0].error();
        if (dfs_metric) {
            dfs_metric->RecordReadErrors(toString(results[0].error()));
        }
        return results[0].error();
    }
    if (dfs_metric) {
        dfs_metric->ObserveRead(1, static_cast<int64_t>(desc.object_size),
                                read_latency_us);
    }
    VLOG(1) << "DFS read finished, action=read, key=" << key
            << ", offset=" << desc.offset << ", size=" << desc.object_size
            << ", latency_us=" << read_latency_us;
    return ErrorCode::OK;
}

void Client::ReconcileMissingKvcsReplica(
    const std::string& key, const QueryResult& query_result,
    const Replica::Descriptor& replica_descriptor) {
    if (!dfs_storage_backend_ || !dfs_storage_backend_->UsesKvcs() ||
        !replica_descriptor.is_dfs_replica()) {
        return;
    }
    if (!query_result.resolved_tenant_id.has_value()) {
        LOG(WARNING) << "Cannot reconcile missing KVCS replica without "
                        "resolved tenant, key="
                     << key;
        return;
    }

    auto evicted = master_client_.EvictDiskReplicaIfCurrent(
        key, *query_result.resolved_tenant_id, ReplicaType::DFS,
        replica_descriptor.id);
    if (!evicted) {
        LOG(WARNING) << "Failed to reconcile missing KVCS replica, key=" << key
                     << ", replica_id=" << replica_descriptor.id
                     << ", error=" << toString(evicted.error());
    } else if (!*evicted) {
        VLOG(1) << "KVCS replica was replaced before miss reconciliation, key="
                << key << ", replica_id=" << replica_descriptor.id;
    }
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchReadFileReplicaRanges(
    const std::vector<std::string>& keys,
    const std::vector<Replica::Descriptor>& replicas,
    const std::vector<Slice>& windows,
    const std::vector<uint64_t>& value_offsets) {
    std::vector<tl::expected<void, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (keys.size() != replicas.size() || keys.size() != windows.size() ||
        keys.size() != value_offsets.size()) {
        LOG(ERROR) << "BatchReadFileReplicaRanges size mismatch";
        return results;
    }

    std::vector<DfsReadRequest> dfs_requests;
    std::vector<size_t> dfs_indexes;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (windows[i].size >
            static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            continue;
        }
        if (replicas[i].is_disk_replica()) {
            if (!storage_backend_) {
                LOG(ERROR) << "Storage backend is not initialized";
                results[i] = tl::unexpected(ErrorCode::INTERNAL_ERROR);
                continue;
            }
            if (value_offsets[i] >
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                continue;
            }
            std::vector<Slice> slices{windows[i]};
            auto loaded = storage_backend_->LoadObject(
                replicas[i].get_disk_descriptor().file_path, slices,
                static_cast<int64_t>(windows[i].size),
                static_cast<int64_t>(value_offsets[i]));
            results[i] = loaded ? tl::expected<void, ErrorCode>{}
                                : tl::unexpected(loaded.error());
            continue;
        }
        if (!replicas[i].is_dfs_replica()) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        if (!dfs_storage_backend_) {
            LOG(ERROR) << "DFS backend is not initialized";
            results[i] = tl::unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
            if (auto* dfs_metric = GetDfsMetricPtr()) {
                dfs_metric->RecordReadErrors(
                    toString(ErrorCode::DFS_SERVICE_UNAVAILABLE));
            }
            continue;
        }
        dfs_requests.push_back(DfsReadRequest{keys[i],
                                              replicas[i].get_dfs_descriptor(),
                                              {windows[i]},
                                              value_offsets[i],
                                              true,
                                              replicas[i].id});
        dfs_indexes.push_back(i);
        results[i] = {};
    }

    if (dfs_requests.empty()) return results;

    // Same counters as Get/BatchGet. Bytes are the window that was read,
    // not the full object: a layer transfers only that window.
    auto* dfs_metric = GetDfsMetricPtr();
    const auto dfs_read_start = std::chrono::steady_clock::now();
    auto dfs_results = dfs_storage_backend_->BatchRead(dfs_requests);
    const auto dfs_read_latency_us = elapsed_us_since(dfs_read_start);
    if (dfs_results.size() != dfs_requests.size()) {
        LOG(ERROR) << "DFS BatchRead response size mismatch: expected "
                   << dfs_requests.size() << ", got " << dfs_results.size();
        for (size_t index : dfs_indexes) {
            results[index] = tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        if (dfs_metric) {
            dfs_metric->RecordReadErrors(
                toString(ErrorCode::INTERNAL_ERROR),
                static_cast<int64_t>(dfs_indexes.size()));
        }
        return results;
    }

    int64_t dfs_success_keys = 0;
    int64_t dfs_success_bytes = 0;
    for (size_t j = 0; j < dfs_results.size(); ++j) {
        const size_t index = dfs_indexes[j];
        if (!dfs_results[j]) {
            LOG(ERROR) << "DFS range read failed, action=range_read, key="
                       << keys[index] << ", error=" << dfs_results[j].error();
            results[index] = tl::unexpected(dfs_results[j].error());
            if (dfs_metric) {
                dfs_metric->RecordReadErrors(toString(dfs_results[j].error()));
            }
            continue;
        }
        ++dfs_success_keys;
        dfs_success_bytes += static_cast<int64_t>(windows[index].size);
    }
    if (dfs_metric && dfs_success_keys > 0) {
        dfs_metric->ObserveRead(dfs_success_keys, dfs_success_bytes,
                                dfs_read_latency_us);
    }
    return results;
}

ErrorCode Client::TransferReadRange(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    return TransferReadInternal(replica_descriptor, slices, src_offset);
}

}  // namespace mooncake
