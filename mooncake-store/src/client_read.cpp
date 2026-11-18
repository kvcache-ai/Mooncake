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

    std::vector<QueryResult> valid_query_results;
    std::vector<size_t> valid_indices;
    std::vector<std::string> valid_keys;

    for (size_t i = 0; i < batched_query_results.size(); ++i) {
        if (batched_query_results[i]) {
            valid_query_results.emplace_back(batched_query_results[i].value());
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
            BatchGet(valid_keys, valid_query_results, valid_slices);

        // Merge results back
        for (size_t i = 0; i < valid_indices.size(); ++i) {
            results[valid_indices[i]] = valid_results[i];
        }
    }

    return results;
}
tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          const QueryResult& query_result,
                                          std::vector<Slice>& slices) {
    if (query_result.IsLeaseExpired()) {
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    // Select a complete replica for this request's destination.
    Replica::Descriptor replica;
    ErrorCode err =
        MakeReadReplicaSelector()(query_result.replicas, slices, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }

    // Check local hot cache and update replica descriptor if cache hit
    bool cache_used = false;
    if (hot_cache_ && replica.is_memory_replica()) {
        cache_used = RedirectToHotCache(object_key, replica);
    }

    auto t0_get = std::chrono::steady_clock::now();
    if (replica.is_dfs_replica()) {
        err = ReadDfsReplica(object_key, replica, slices);
    } else {
        err = TransferRead(replica, slices);
    }

    // Release the cache block after transfer completes (memcpy is done)
    if (hot_cache_ && cache_used) {
        hot_cache_->ReleaseHotKey(object_key);
    }

    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }

    if (err != ErrorCode::OK) {
        if (err == ErrorCode::OBJECT_NOT_FOUND) {
            ReconcileMissingKvcsReplica(object_key, query_result, replica);
        }
        LOG(ERROR) << "transfer_read_failed key=" << object_key;
        return tl::unexpected(err);
    }

    auto checksum_result =
        VerifyObjectChecksum(object_key, slices, calculate_total_size(replica),
                             query_result.object_checksum);
    if (!checksum_result) {
        // add checksum err for single-key
        auto* dfs_metric = GetDfsMetricPtr();
        if (replica.is_dfs_replica() && dfs_metric) {
            dfs_metric->RecordReadErrors(toString(checksum_result.error()));
        }
        return tl::unexpected(checksum_result.error());
    }

    // Frequency admission: only promote frequently accessed keys to hot cache.
    // Skip when cache_used — data was already served from local cache, no need
    // to re-promote or increment the CMS counter.
    if (ShouldAdmitToHotCache(object_key, cache_used)) {
        ProcessSlicesAsync(object_key, slices, replica);
    }

    if (query_result.IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << object_key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    // Log cache hit statistics
    if (hot_cache_ && replica.is_memory_replica()) {
        VLOG(1) << "Get completed: key=" << object_key
                << " cache_hit=" << (cache_used ? 1 : 0);
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          const QueryResult& query_result,
                                          std::vector<Slice>& slices,
                                          uint64_t src_offset) {
    if (query_result.IsLeaseExpired()) {
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    Replica::Descriptor replica;
    ErrorCode err =
        MakeReadReplicaSelector()(query_result.replicas, slices, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }
    if (!replica.is_memory_replica()) {
        LOG(ERROR) << "Range read only supported for memory replicas, key="
                   << object_key;
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }

    auto t0_get = std::chrono::steady_clock::now();
    err = TransferReadRange(replica, slices, src_offset);
    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }

    if (err != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_range_failed key=" << object_key;
        return tl::unexpected(err);
    }
    if (query_result.IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << object_key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices,
    bool group_by_target) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    // Validate input size consistency
    if (query_results.size() != object_keys.size()) {
        LOG(ERROR) << "Query results size (" << query_results.size()
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
    // Tuple: (index, key, future, replica, cache_used)
    std::vector<std::tuple<size_t, std::string, TransferFuture,
                           Replica::Descriptor, bool>>
        pending_transfers;
    struct TargetReadGroup {
        std::vector<size_t> indexes;
        std::vector<Replica::Descriptor> replicas;
        std::vector<std::vector<Slice>> slices;
        std::vector<bool> cache_used;
    };
    std::unordered_map<std::string, TargetReadGroup> target_groups;
    std::vector<DfsReadRequest> dfs_read_requests;
    std::vector<size_t> dfs_read_indices;
    std::vector<Replica::Descriptor> dfs_read_replicas;
    std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());
    // Record batch get transfer latency (Submit + Wait)
    auto t0_batch_get = std::chrono::steady_clock::now();

    // Collect cache hit statistics for the entire batch
    size_t total_cache_hits = 0;

    auto select_replica = MakeReadReplicaSelector();
    // Submit all transfers in parallel
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& query_result = query_results[i];

        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        // Select a complete replica for this key's destination.
        Replica::Descriptor replica;
        ErrorCode err =
            select_replica(query_result.replicas, slices_it->second, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }

        // Validate the requested length without accessing the replica's
        // buffer. A malformed request keeps its transfer error even when the
        // lease has expired.
        if (replica.is_memory_replica() &&
            CalculateSliceSize(slices_it->second) !=
                replica.get_memory_descriptor().buffer_descriptor.size_) {
            LOG(ERROR) << "Transfer length mismatch for key: " << key;
            results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            continue;
        }
        if (query_result.IsLeaseExpired()) {
            results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
            continue;
        }

        bool cache_used = false;
        if (hot_cache_ && replica.is_memory_replica()) {
            cache_used = RedirectToHotCache(key, replica);
            if (cache_used) {
                total_cache_hits++;
            }
        }

        // Submit transfer operation asynchronously
        std::optional<TransferFuture> future;
        if (replica.is_dfs_replica()) {
            if (!dfs_storage_backend_) {
                LOG(ERROR) << "DFS backend is not initialized";
                if (auto* dfs_metric = GetDfsMetricPtr()) {
                    dfs_metric->RecordReadErrors(
                        toString(ErrorCode::DFS_SERVICE_UNAVAILABLE));
                }
                results[i] = tl::unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
                continue;
            }
            const auto& desc = replica.get_dfs_descriptor();
            dfs_read_requests.push_back(
                DfsReadRequest{.key = key,
                               .descriptor = desc,
                               .slices = slices_it->second,
                               .source_replica_id = replica.id});
            dfs_read_indices.push_back(i);
            dfs_read_replicas.push_back(replica);
            continue;
        } else if (replica.is_nof_replica()) {
            auto contiguous_range = GetContiguousSliceRange(slices_it->second);
            if (!contiguous_range.has_value()) {
                LOG(ERROR) << "NoF transfer requires contiguous slices";
                results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            future = transfer_submitter_->submit(
                replica, slices_it->second, TransferRequest::READ,
                contiguous_range->ptr, contiguous_range->size,
                TransferIntent::kForegroundGet);
        } else if (group_by_target && replica.is_memory_replica()) {
            auto& group =
                target_groups[replica.get_memory_descriptor()
                                  .buffer_descriptor.transport_endpoint_];
            group.indexes.push_back(i);
            group.replicas.push_back(replica);
            group.slices.push_back(slices_it->second);
            group.cache_used.push_back(cache_used);
            continue;
        } else {
            future = transfer_submitter_->submit(
                replica, slices_it->second, TransferRequest::READ, nullptr, 0,
                TransferIntent::kForegroundGet);
        }
        if (!future) {
            // Release cache block if submit failed
            if (hot_cache_ && cache_used) {
                hot_cache_->ReleaseHotKey(key);
            }
            LOG(ERROR) << "Failed to submit transfer operation for key: "
                       << key;
            results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            continue;
        }

        VLOG(1) << "Submitted transfer for key " << key
                << " using strategy: " << static_cast<int>(future->strategy());

        pending_transfers.emplace_back(i, key, std::move(*future), replica,
                                       cache_used);
    }

    for (auto& [endpoint, group] : target_groups) {
        auto future = transfer_submitter_->submit_batch(
            group.replicas, group.slices, TransferRequest::READ,
            TransferIntent::kForegroundGet);
        for (size_t j = 0; j < group.indexes.size(); ++j) {
            const size_t index = group.indexes[j];
            const auto& key = object_keys[index];
            if (!future) {
                if (hot_cache_ && group.cache_used[j]) {
                    hot_cache_->ReleaseHotKey(key);
                }
                LOG(ERROR) << "Failed to submit batch transfer to " << endpoint
                           << " for key: " << key;
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                continue;
            }
            pending_transfers.emplace_back(index, key, future->share(),
                                           group.replicas[j],
                                           group.cache_used[j]);
        }
        VLOG(1) << "Submitted batch read of " << group.indexes.size()
                << " keys from " << endpoint;
    }

    if (!dfs_read_requests.empty()) {
        auto* dfs_metric = GetDfsMetricPtr();
        const auto dfs_read_start = std::chrono::steady_clock::now();
        auto dfs_results = dfs_storage_backend_->BatchRead(dfs_read_requests);
        const auto dfs_read_latency_us = elapsed_us_since(dfs_read_start);
        int64_t dfs_success_keys = 0;
        uint64_t dfs_success_bytes = 0;
        if (dfs_results.size() != dfs_read_requests.size()) {
            LOG(ERROR) << "DFS BatchRead response size mismatch: expected "
                       << dfs_read_requests.size() << ", got "
                       << dfs_results.size();
            for (size_t index : dfs_read_indices) {
                results[index] = tl::unexpected(ErrorCode::INTERNAL_ERROR);
            }
            if (dfs_metric) {
                dfs_metric->RecordReadErrors(
                    toString(ErrorCode::INTERNAL_ERROR),
                    static_cast<int64_t>(dfs_read_indices.size()));
            }
        } else {
            for (size_t i = 0; i < dfs_results.size(); ++i) {
                const size_t index = dfs_read_indices[i];
                const auto& request = dfs_read_requests[i];
                if (!dfs_results[i]) {
                    LOG(WARNING) << "DFS read failed, action=batch_read, key="
                                 << request.key
                                 << ", offset=" << request.descriptor.offset
                                 << ", size=" << request.descriptor.object_size
                                 << ", error=" << dfs_results[i].error();
                    if (dfs_metric) {
                        dfs_metric->RecordReadErrors(
                            toString(dfs_results[i].error()));
                    }
                    results[index] = tl::unexpected(dfs_results[i].error());
                    if (dfs_results[i].error() == ErrorCode::OBJECT_NOT_FOUND) {
                        ReconcileMissingKvcsReplica(request.key,
                                                    query_results[index],
                                                    dfs_read_replicas[i]);
                    }
                    continue;
                }
                // The I/O itself delivered these bytes, so they count even if
                // the checksum below rejects the payload; a checksum mismatch
                // is additionally recorded as an error so the two are
                // distinguishable.
                ++dfs_success_keys;
                dfs_success_bytes += request.descriptor.object_size;
                auto checksum_result = VerifyObjectChecksum(
                    request.key, request.slices, request.descriptor.object_size,
                    query_results[index].object_checksum);
                if (!checksum_result) {
                    if (dfs_metric) {
                        dfs_metric->RecordReadErrors(
                            toString(checksum_result.error()));
                    }
                    results[index] = tl::unexpected(checksum_result.error());
                    continue;
                }
                results[index] = {};
            }
        }
        if (dfs_metric && dfs_success_keys > 0) {
            dfs_metric->ObserveRead(dfs_success_keys,
                                    static_cast<int64_t>(dfs_success_bytes),
                                    dfs_read_latency_us);
        }
        VLOG(1) << "DFS batch read finished, action=batch_read, requested="
                << dfs_read_requests.size()
                << ", succeeded=" << dfs_success_keys
                << ", bytes=" << dfs_success_bytes
                << ", latency_us=" << dfs_read_latency_us;
    }

    // Wait for all transfers to complete
    for (auto& [index, key, future, stored_replica, cache_used] :
         pending_transfers) {
        ErrorCode result = future.get();

        // Release the cache block after transfer completes (memcpy is done)
        if (hot_cache_ && cache_used) {
            hot_cache_->ReleaseHotKey(key);
        }
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Transfer failed for key: " << key
                       << " with error: " << static_cast<int>(result);
            results[index] = tl::unexpected(result);
        } else {
            VLOG(1) << "Transfer completed successfully for key: " << key;

            auto slices_it = slices.find(key);
            if (slices_it == slices.end()) {
                results[index] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            auto checksum_result = VerifyObjectChecksum(
                key, slices_it->second, calculate_total_size(stored_replica),
                query_results[index].object_checksum);
            if (!checksum_result) {
                results[index] = tl::unexpected(checksum_result.error());
                continue;
            }
            results[index] = {};

            // Frequency admission: only promote frequently accessed keys.
            // Skip when cache was used (data served from local cache).
            if (hot_cache_) {
                auto slices_it = slices.find(key);
                if (slices_it != slices.end() &&
                    ShouldAdmitToHotCache(key, cache_used)) {
                    ProcessSlicesAsync(key, slices_it->second, stored_replica);
                }
            }
        }
    }

    // As lease expired is a rare case, we check all the results with the same
    // time_point to avoid too many syscalls
    std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    for (size_t i = 0; i < object_keys.size(); ++i) {
        if (results[i].has_value() && query_results[i].IsLeaseExpired(now)) {
            LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                         << object_keys[i];
            results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
        }
    }

    auto us_batch_get = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - t0_batch_get)
                            .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_get_latency_us.observe(us_batch_get);
    }

    // Log overall cache hit statistics for the entire batch
    if (hot_cache_) {
        VLOG(1) << "BatchGet completed: num_keys=" << object_keys.size()
                << " total_cache_hits=" << total_cache_hits;
    } else {
        VLOG(1) << "BatchGet completed for " << object_keys.size() << " keys";
    }
    return results;
}

bool Client::RedirectToHotCache(const std::string& key,
                                Replica::Descriptor& replica) {
    if (!replica.is_memory_replica() || !hot_cache_) {
        return false;
    }

    auto& mem_desc = replica.get_memory_descriptor();
    HotMemBlock* blk = hot_cache_->GetHotKey(key);
    if (blk == nullptr) {
        return false;
    }

    if (mem_desc.buffer_descriptor.size_ != blk->size) {
        LOG(ERROR) << "Cache hit but size mismatch for key: " << key;
        return false;
    }

    mem_desc.buffer_descriptor.transport_endpoint_ =
        (metadata_connstring_ == P2PHANDSHAKE) ? GetTransportEndpoint()
                                               : local_hostname_;
    mem_desc.buffer_descriptor.buffer_address_ =
        reinterpret_cast<uintptr_t>(blk->addr);
    return true;
}

void Client::ProcessSlicesAsync(const std::string& key,
                                const std::vector<Slice>& slices,
                                const Replica::Descriptor& replica) {
    if (!(hot_cache_ && replica.is_memory_replica())) {
        return;
    }

    // Skip local data unless hot cache is in shm mode (shared with dummy
    // clients who need local data cached for zero-copy access).
    if (!hot_cache_->IsShm() && IsReplicaOnLocalMemory(replica)) {
        return;
    }

    // The writeback copies out of the read destination with a host memcpy
    // (LocalHotCacheHandler::SubmitPutTask), so a slice that landed in device
    // memory (e.g. batch_get_into into CUDA buffers) would fault the reader.
    // Device-resident results are not cached through this path.
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    size_t device_dest_slices = 0;
    // Identify TE transfer slices (non-local) and submit async put tasks
    for (size_t i = 0; i < slices.size(); ++i) {
        if (runtime_accelerator.FindDeviceForPointer(slices[i].ptr) !=
            nullptr) {
            ++device_dest_slices;
            continue;
        }
        if (!hot_cache_handler_->SubmitPutTask(key, slices[i])) {
            LOG(ERROR) << "Failed to submit hot cache put task for key=" << key
                       << " slice_idx=" << i;
            return;
        }
    }
    if (device_dest_slices > 0) {
        LOG(INFO) << "Skipped hot-cache writeback for " << device_dest_slices
                  << " device-resident slice(s), key=" << key;
    }
}

bool Client::IsReplicaOnLocalMemory(const Replica::Descriptor& replica) {
    if (!replica.is_memory_replica()) {
        return false;
    }
    const auto replica_transfer_endpoint =
        replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    // Fast paths first: lock-free comparisons against this client's own
    // endpoint/hostname.
    if (metadata_connstring_ == P2PHANDSHAKE) {
        if (replica_transfer_endpoint == GetTransportEndpoint()) {
            return true;
        }
    } else if (local_hostname_ == replica_transfer_endpoint) {
        return true;
    }
    // Fallback: the replica's endpoint may address one of this client's
    // mounted segments by segment name or TE endpoint (e.g. when the writer
    // published the segment under a name different from local_hostname_).
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    for (const auto& [segment_id, segment] : mounted_segments_) {
        if (replica_transfer_endpoint == segment.name ||
            replica_transfer_endpoint == segment.te_endpoint) {
            return true;
        }
    }
    return false;
}

std::optional<store::GatherReadOperation> Client::SubmitGatherRead(
    const std::string& endpoint,
    const std::vector<store::GatherReadRange>& ranges, void* destination,
    size_t capacity) {
    if (!SupportsGatherRead() || endpoint.empty()) return std::nullopt;
    std::shared_ptr<GatherPeer> peer_ptr;
    {
        std::lock_guard lock(gather_clients_mutex_);
        auto& slot = gather_clients_[endpoint];
        if (!slot) slot = std::make_shared<GatherPeer>();
        peer_ptr = slot;
    }
    auto& peer = *peer_ptr;
    std::lock_guard lock(peer.mutex);
    const auto now = std::chrono::steady_clock::now();
    if (now >= peer.refresh_at) {
        auto discovered = master_client_.ResolveGatherEndpoint(endpoint);
        const std::string resolved = discovered ? *discovered : "";
        if (resolved != peer.endpoint) peer.clients.clear();
        peer.endpoint = resolved;
        peer.refresh_at = now + std::chrono::seconds(resolved.empty() ? 5 : 30);
    }
    if (peer.endpoint.empty()) return std::nullopt;
    auto& clients = peer.clients;
    std::erase_if(clients,
                  [](const auto& client) { return client->retired(); });
    auto it =
        std::find_if(clients.begin(), clients.end(),
                     [](const auto& client) { return client->available(); });
    if (it == clients.end()) {
        if (clients.size() >= 64) return std::nullopt;
        try {
            clients.push_back(std::make_unique<store::GatherReadClient>(
                transfer_engine_, peer.endpoint, std::chrono::seconds(60),
                endpoint));
        } catch (const std::exception& e) {
            peer.refresh_at = now + std::chrono::seconds(5);
            peer.endpoint.clear();
            clients.clear();
            LOG(WARNING) << "Store gather connection unavailable: " << e.what();
            return std::nullopt;
        }
        it = std::prev(clients.end());
    }
    // The synchronous get_into_ranges caller owns the destination and its
    // registration until all operations (including recovery fences) finish.
    return (*it)->submitGatherRead(ranges, destination, capacity,
                                   transfer_engine_);
}

}  // namespace mooncake
