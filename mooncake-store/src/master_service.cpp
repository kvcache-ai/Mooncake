#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <shared_mutex>
#include <regex>
#include <unordered_set>
#include <ylt/util/tl/expected.hpp>

#include "client_manager.h"
#include "master_metric_manager.h"
#include "types.h"

namespace mooncake {

MasterService::MasterService(const MasterServiceConfig& config)
    : enable_ha_(config.enable_ha) {
}

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    ErrorCode err = GetClientManager().UnmountSegment(segment_id, client_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to unmount segment"
                   << ", segment_id=" << segment_id
                   << ", client_id=" << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

auto MasterService::ExistKey(const std::string& key)
    -> tl::expected<bool, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    auto& metadata = accessor.Get();
    for (const auto& replica : metadata.replicas) {
        if (replica.status() == ReplicaStatus::COMPLETE) {
            // Grant a lease to the object as it may be further used by the
            // client.
            metadata.GrantLease(default_kv_lease_ttl_,
                                default_kv_soft_pin_ttl_);
            return true;
        }
    }

    return false;  // If no complete replica is found, return false
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKey(
    const std::vector<std::string>& keys) {
    std::vector<tl::expected<bool, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(ExistKey(key));
    }
    return results;
}

auto MasterService::GetAllKeys()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    for (size_t i = 0; i < kNumShards; i++) {
        MutexLocker lock(&metadata_shards_[i].mutex);
        for (const auto& item : metadata_shards_[i].metadata) {
            all_keys.push_back(item.first);
        }
    }
    return all_keys;
}

auto MasterService::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_segments;
    auto err = GetClientManager().GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to get all segments"
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return all_segments;
}

auto MasterService::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    size_t used, capacity;
    auto err = GetClientManager().QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to query segment"
                   << ", segment=" << segment << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

auto MasterService::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> result;
    auto err = GetClientManager().QueryIp(client_id, result);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to query ip"
                   << ", client_id=" << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return result;
}

auto MasterService::BatchQueryIp(const std::vector<UUID>& client_ids)
    -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode> {
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>
        results;
    results.reserve(client_ids.size());
    for (const auto& client_id : client_ids) {
        auto ip_result = QueryIp(client_id);
        if (ip_result.has_value()) {
            results.emplace(client_id, std::move(ip_result.value()));
        }
    }
    return results;
}

auto MasterService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> cleared_keys;
    cleared_keys.reserve(object_keys.size());
    const bool clear_all_segments = segment_name.empty();

    for (const auto& key : object_keys) {
        if (key.empty()) {
            LOG(WARNING) << "BatchReplicaClear: empty key, skipping";
            continue;
        }
        MetadataAccessor accessor(this, key);
        if (!accessor.Exists()) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " not found, skipping";
            continue;
        }

        auto& metadata = accessor.Get();

        // Security check: Ensure the requesting client owns the object.
        if (metadata.client_id != client_id) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " belongs to different client_id="
                         << metadata.client_id << ", expected=" << client_id
                         << ", skipping";
            continue;
        }

        // Safety check: Do not clear an object that has an active lease.
        if (!metadata.IsLeaseExpired()) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " has active lease, skipping";
            continue;
        }

        if (clear_all_segments) {
            // Check if all replicas are complete. Incomplete replicas could
            // indicate an ongoing Put operation, and clearing during this time
            // could lead to an inconsistent state or interfere with the write.
            if (!metadata.IsAllReplicasComplete()) {
                LOG(WARNING) << "BatchReplicaClear: key=" << key
                             << " has incomplete replicas, skipping";
                continue;
            }

            // Before erasing, decrement cache metrics for each COMPLETE replica
            for (const auto& replica : metadata.replicas) {
                if (replica.status() == ReplicaStatus::COMPLETE) {
                    if (replica.is_memory_replica()) {
                        MasterMetricManager::instance().dec_mem_cache_nums();
                    } else if (replica.is_disk_replica()) {
                        MasterMetricManager::instance().dec_file_cache_nums();
                    }
                }
            }

            // Erase the entire metadata (all replicas will be deallocated)
            accessor.Erase();
            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: successfully cleared all replicas "
                       "for key="
                    << key << " for client_id=" << client_id;
        } else {
            // Clear only replicas on the specified segment_name
            bool has_replica_on_segment = false;
            std::vector<size_t> replicas_to_remove;

            for (size_t i = 0; i < metadata.replicas.size(); ++i) {
                const auto& replica = metadata.replicas[i];
                if (replica.status() != ReplicaStatus::COMPLETE) {
                    continue;
                }
                auto segment_names = replica.get_segment_names();
                for (const auto& seg_name : segment_names) {
                    if (seg_name.has_value() &&
                        seg_name.value() == segment_name) {
                        has_replica_on_segment = true;
                        replicas_to_remove.emplace_back(i);
                        break;
                    }
                }
            }

            if (!has_replica_on_segment) {
                LOG(WARNING)
                    << "BatchReplicaClear: key=" << key
                    << " has no replica on segment_name=" << segment_name
                    << ", skipping";
                continue;
            }

            // Remove replicas on the specified segment (in reverse order to
            // maintain indices)
            for (auto it = replicas_to_remove.rbegin();
                 it != replicas_to_remove.rend(); ++it) {
                size_t idx = *it;
                const auto& replica = metadata.replicas[idx];

                if (replica.is_memory_replica()) {
                    MasterMetricManager::instance().dec_mem_cache_nums();
                } else if (replica.is_disk_replica()) {
                    MasterMetricManager::instance().dec_file_cache_nums();
                }
                metadata.replicas.erase(metadata.replicas.begin() + idx);
            }

            // If no valid replicas remain, erase the entire metadata
            if (metadata.replicas.empty() || !metadata.IsValid()) {
                accessor.Erase();
            }

            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: successfully cleared replicas on "
                       "segment_name="
                    << segment_name << " for key=" << key
                    << " for client_id=" << client_id;
        }
    }

    return cleared_keys;
}

auto MasterService::GetReplicaListByRegex(const std::string& regex_pattern)
    -> tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode> {
    std::unordered_map<std::string, std::vector<Replica::Descriptor>> results;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (size_t i = 0; i < kNumShards; ++i) {
        MutexLocker lock(&metadata_shards_[i].mutex);

        for (auto& [key, metadata] : metadata_shards_[i].metadata) {
            if (std::regex_search(key, pattern)) {
                std::vector<Replica::Descriptor> replica_list;
                replica_list.reserve(metadata.replicas.size());
                for (const auto& replica : metadata.replicas) {
                    if (replica.status() == ReplicaStatus::COMPLETE) {
                        replica_list.emplace_back(replica.get_descriptor());
                    }
                }
                if (replica_list.empty()) {
                    LOG(WARNING)
                        << "key=" << key
                        << " matched by regex, but has no complete replicas.";
                    continue;
                }

                results.emplace(key, std::move(replica_list));
                metadata.GrantLease(default_kv_lease_ttl_,
                                    default_kv_soft_pin_ttl_);
            }
        }
    }

    return results;
}

auto MasterService::GetReplicaList(std::string_view key)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    MetadataAccessor accessor(this, std::string(key));

    MasterMetricManager::instance().inc_total_get_nums();

    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();

    std::vector<Replica::Descriptor> replica_list;
    replica_list.reserve(metadata.replicas.size());
    for (const auto& replica : metadata.replicas) {
        if (replica.status() == ReplicaStatus::COMPLETE) {
            replica_list.emplace_back(replica.get_descriptor());
        }
    }

    if (replica_list.empty()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    if (replica_list[0].is_memory_replica()) {
        MasterMetricManager::instance().inc_mem_cache_hit_nums();
    } else if (replica_list[0].is_disk_replica()) {
        MasterMetricManager::instance().inc_file_cache_hit_nums();
    }
    MasterMetricManager::instance().inc_valid_get_nums();
    // Grant a lease to the object so it will not be removed
    // when the client is reading it.
    metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);

    return GetReplicaListResponse(std::move(replica_list),
                                  default_kv_lease_ttl_);
}

auto MasterService::Remove(const std::string& key)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    if (!metadata.IsLeaseExpired()) {
        VLOG(1) << "key=" << key << ", error=object_has_lease";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }

    if (!metadata.IsAllReplicasComplete()) {
        LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Remove object metadata
    accessor.Erase();
    return {};
}

auto MasterService::RemoveByRegex(const std::string& regex_pattern)
    -> tl::expected<long, ErrorCode> {
    long removed_count = 0;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (size_t i = 0; i < kNumShards; ++i) {
        MutexLocker lock(&metadata_shards_[i].mutex);

        for (auto it = metadata_shards_[i].metadata.begin();
             it != metadata_shards_[i].metadata.end();) {
            if (std::regex_search(it->first, pattern)) {
                if (!it->second.IsLeaseExpired()) {
                    VLOG(1) << "key=" << it->first
                            << " matched by regex, but has lease. Skipping "
                            << "removal.";
                    ++it;
                    continue;
                }
                if (!it->second.IsAllReplicasComplete()) {
                    LOG(WARNING) << "key=" << it->first
                                 << " matched by regex, but not all replicas "
                                    "are complete. Skipping removal.";
                    ++it;
                    continue;
                }

                VLOG(1) << "key=" << it->first
                        << " matched by regex. Removing.";
                it = metadata_shards_[i].metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << removed_count;
    return removed_count;
}

long MasterService::RemoveAll() {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    // Store the current time to avoid repeatedly
    // calling std::chrono::steady_clock::now()
    auto now = std::chrono::steady_clock::now();

    for (auto& shard : metadata_shards_) {
        MutexLocker lock(&shard.mutex);
        if (shard.metadata.empty()) {
            continue;
        }

        // Only remove completed objects with expired leases
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            if (it->second.IsLeaseExpired(now) &&
                it->second.IsAllReplicasComplete()) {
                total_freed_size +=
                    it->second.size * it->second.GetMemReplicaCount();
                it = shard.metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    for (const auto& shard : metadata_shards_) {
        MutexLocker lock(&shard.mutex);
        total += shard.metadata.size();
    }
    return total;
}

auto MasterService::Ping(const UUID& client_id)
    -> tl::expected<PingResponse, ErrorCode> {
    auto res = GetClientManager().Ping(client_id);
    if (!res.has_value()) {
        LOG(ERROR) << "fail to ping client"
                   << ", client_id=" << client_id << ", ret=" << res.error();
        return tl::make_unexpected(res.error());
    }

    return PingResponse(view_version_, res.value());
}

}  // namespace mooncake