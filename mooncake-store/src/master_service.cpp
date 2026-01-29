#include "master_service.h"

#include <cassert>
#include <regex>
#include <ylt/util/tl/expected.hpp>

#include "client_manager.h"
#include "master_metric_manager.h"
#include "types.h"

namespace mooncake {

// RAII-style metric management
MasterService::ObjectMetadata::~ObjectMetadata() {
    MasterMetricManager::instance().dec_key_count(1);
}

MasterService::ObjectMetadata::ObjectMetadata(const UUID& client_id,
                                              size_t value_length,
                                              std::vector<Replica>&& reps)
    : client_id_(client_id), replicas_(std::move(reps)), size_(value_length) {
    MasterMetricManager::instance().inc_key_count(1);
    MasterMetricManager::instance().observe_value_size(value_length);
}

MasterService::MasterService(const MasterServiceConfig& config)
    : enable_ha_(config.enable_ha), view_version_(config.view_version) {}

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    auto result = GetClientManager().UnmountSegment(segment_id, client_id);
    if (!result) {
        LOG(ERROR) << "fail to unmount segment"
                   << ", segment_id=" << segment_id
                   << ", client_id=" << client_id << ", ret=" << result.error();
        return result;
    }
    return {};
}

auto MasterService::ExistKey(const std::string& key)
    -> tl::expected<bool, ErrorCode> {
    auto accessor = GetMetadataAccessor(key);
    if (!accessor->Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    auto& metadata = accessor->Get();
    if (metadata.IsObjectAccessible()) {
        OnObjectAccessed(metadata);
        return true;
    }

    return false;
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
    for (size_t i = 0; i < GetShardCount(); i++) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);
        for (const auto& item : shard.metadata) {
            all_keys.push_back(item.first);
        }
    }
    return all_keys;
}

auto MasterService::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto result = GetClientManager().GetAllSegments();
    if (!result.has_value()) {
        LOG(ERROR) << "fail to get all segments"
                   << ", ret=" << result.error();
    }
    return result;
}

auto MasterService::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    auto result = GetClientManager().QuerySegments(segment);
    if (!result.has_value()) {
        LOG(ERROR) << "fail to query segment"
                   << ", segment=" << segment << ", ret=" << result.error();
    }
    return result;
}

auto MasterService::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto result = GetClientManager().QueryIp(client_id);
    if (!result.has_value()) {
        LOG(ERROR) << "fail to query ip"
                   << ", client_id=" << client_id << ", ret=" << result.error();
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
        } else {
            LOG(WARNING) << "fail to query ip"
                         << ", client_id=" << client_id
                         << ", ret=" << ip_result.error();
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
        auto accessor = GetMetadataAccessor(key);
        if (!accessor->Exists()) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " not found, skipping";
            continue;
        }

        auto& metadata = accessor->Get();

        // Security check: Ensure the requesting client owns the object.
        if (metadata.client_id_ != client_id) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " belongs to different client_id="
                         << metadata.client_id_ << ", expected=" << client_id
                         << ", skipping";
            continue;
        }

        if (clear_all_segments) {
            if (auto res = metadata.IsObjectRemovable(); !res) {
                LOG(WARNING) << "BatchReplicaClear: key=" << key
                             << " cannot be removed, reason=" << res.error();
                continue;
            }
            OnObjectRemoved(metadata);

            // Erase the entire metadata (all replicas will be deallocated)
            accessor->Erase();
            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: successfully cleared all replicas "
                       "for key="
                    << key << " for client_id=" << client_id;
        } else {
            // Clear only replicas on the specified segment_name
            bool has_replica_on_segment = false;
            std::vector<size_t> replicas_to_remove;

            for (size_t i = 0; i < metadata.replicas_.size(); ++i) {
                const auto& replica = metadata.replicas_[i];
                if (auto res = metadata.IsReplicaRemovable(replica); !res) {
                    LOG(WARNING)
                        << "BatchReplicaClear: key=" << key
                        << " cannot be removed, reason=" << res.error();
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

            for (auto it = replicas_to_remove.rbegin();
                 it != replicas_to_remove.rend(); ++it) {
                size_t idx = *it;
                const auto& replica = metadata.replicas_[idx];
                OnReplicaRemoved(replica);
                metadata.replicas_.erase(metadata.replicas_.begin() + idx);
            }

            if (metadata.replicas_.empty()) {
                accessor->Erase();
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

    for (size_t i = 0; i < GetShardCount(); ++i) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);

        for (auto& [key, metadata] : shard.metadata) {
            if (std::regex_search(key, pattern)) {
                std::vector<Replica::Descriptor> replica_list;
                replica_list.reserve(metadata->replicas_.size());
                for (const auto& replica : metadata->replicas_) {
                    if (metadata->IsReplicaAccessible(replica)) {
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
                OnObjectHit(*metadata);
                OnObjectAccessed(*metadata);
            }
        }
    }

    return results;
}

auto MasterService::GetReplicaList(std::string_view key)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    auto accessor = GetMetadataAccessor(std::string(key));

    MasterMetricManager::instance().inc_total_get_nums();

    if (!accessor->Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor->Get();

    std::vector<Replica::Descriptor> replica_list;
    replica_list.reserve(metadata.replicas_.size());
    for (const auto& replica : metadata.replicas_) {
        if (metadata.IsReplicaAccessible(replica)) {
            replica_list.emplace_back(replica.get_descriptor());
        }
    }

    if (replica_list.empty()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    OnObjectHit(metadata);
    OnObjectAccessed(metadata);

    return replica_list;
}

auto MasterService::Remove(const std::string& key)
    -> tl::expected<void, ErrorCode> {
    auto accessor = GetMetadataAccessor(key);
    if (!accessor->Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor->Get();

    if (auto res = metadata.IsObjectRemovable(); !res) {
        VLOG(1) << "key=" << key << ", error=" << res.error();
        return tl::make_unexpected(res.error());
    }

    // Remove object metadata
    OnObjectRemoved(metadata);
    accessor->Erase();
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

    for (size_t i = 0; i < GetShardCount(); ++i) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);

        for (auto it = shard.metadata.begin(); it != shard.metadata.end();) {
            if (std::regex_search(it->first, pattern)) {
                if (!it->second->IsObjectRemovable()) {
                    VLOG(1) << "key=" << it->first
                            << " matched by regex, but object is not removable";
                    ++it;
                    continue;
                }

                VLOG(1) << "key=" << it->first
                        << " matched by regex. Removing.";
                OnObjectRemoved(*it->second);
                it = shard.metadata.erase(it);
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

    for (size_t i = 0; i < GetShardCount(); ++i) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            if (it->second->IsObjectRemovable()) {
                OnObjectRemoved(*it->second);
                it = shard.metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count;
    return removed_count;
}

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    for (size_t i = 0; i < GetShardCount(); ++i) {
        const auto& shard = GetShard(i);
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