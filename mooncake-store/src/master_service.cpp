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

MasterService::ObjectMetadata::ObjectMetadata(size_t value_length,
                                              std::vector<Replica>&& reps)
    : replicas_(std::move(reps)), size_(value_length) {
    MasterMetricManager::instance().inc_key_count(1);
    MasterMetricManager::instance().observe_value_size(value_length);
}

MasterService::MasterService(const MasterServiceConfig& config)
    : enable_ha_(config.enable_ha), view_version_(config.view_version) {}

void MasterService::InitializeClientManager() {
    GetClientManager().SetSegmentRemovalCallback(
        [this](const UUID& segment_id) { this->OnSegmentRemoved(segment_id); });
}

auto MasterService::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    auto client = GetClientManager().GetClient(client_id);
    if (!client) {
        LOG(ERROR) << "MountSegment: client not found"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }

    auto result = client->MountSegment(segment);
    if (!result) {
        LOG(ERROR) << "fail to mount segment"
                   << ", segment=" << segment.name
                   << ", client_id=" << client_id << ", ret=" << result.error();
        return result;
    }
    return {};
}

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    auto client = GetClientManager().GetClient(client_id);
    if (!client) {
        LOG(ERROR) << "UnmountSegment: client not found"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }

    auto result = client->UnmountSegment(segment_id);
    if (!result) {
        LOG(ERROR) << "fail to unmount segment"
                   << ", segment_id=" << segment_id
                   << ", client_id=" << client_id << ", ret=" << result.error();
        return result;
    }
    return {};
}

void MasterService::OnObjectRemoved(ObjectMetadata& metadata) {
    for (auto& replica : metadata.replicas_) {
        OnReplicaRemoved(replica);
    }
}

// As a callback, called by SegmentManager when a segment is removed:
// 1. remove reverse index of segment
// 2. remove the replica in metadata about the segment
void MasterService::OnSegmentRemoved(const UUID& segment_id) {
    for (size_t i = 0; i < GetShardCount(); ++i) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);

        auto idx_it = shard.segment_key_index.find(segment_id);
        if (idx_it == shard.segment_key_index.end()) {
            continue;
        }

        // the reverse index using string_view acquired from metadata.
        // before remove the reverse index, we should acquire the key copier.
        std::vector<std::string> affected_keys;
        affected_keys.reserve(idx_it->second.size());
        for (const auto& item : idx_it->second) {
            affected_keys.emplace_back(item.first);
        }

        // 1. Remove the segment from the reverse index
        shard.segment_key_index.erase(idx_it);

        // 2. Remove the replica in metadata about the segment
        for (const auto& key : affected_keys) {
            auto meta_it = shard.metadata.find(key);
            if (meta_it == shard.metadata.end()) {
                continue;
            }

            auto& metadata = *meta_it->second;
            auto& replicas = metadata.replicas_;

            for (int k = replicas.size() - 1; k >= 0; --k) {
                auto id = replicas[k].get_segment_id();
                if (id.has_value() && id.value() == segment_id) {
                    OnReplicaRemoved(replicas[k]);
                    replicas.erase(replicas.begin() + k);
                    break;
                }
            }

            if (replicas.empty()) {
                OnObjectRemoved(metadata);
                shard.metadata.erase(meta_it);
            }
        }
    }  // end for
}

void MasterService::AddReplicaToSegmentIndex(MetadataShard& shard,
                                             const std::string& key,
                                             const Replica& replica) {
    if (replica.status() != ReplicaStatus::COMPLETE) {
        return;
    }
    auto seg_id = replica.get_segment_id();
    if (seg_id.has_value()) {
        shard.segment_key_index[seg_id.value()][std::string_view(key)]++;
    }
}

void MasterService::RemoveReplicaFromSegmentIndex(
    MetadataShard& shard, const std::string& key,
    const std::vector<Replica>& replicas) {
    for (const auto& replica : replicas) {
        RemoveReplicaFromSegmentIndex(shard, key, replica);
    }
}

void MasterService::RemoveReplicaFromSegmentIndex(MetadataShard& shard,
                                                  const std::string& key,
                                                  const Replica& replica) {
    if (replica.status() != ReplicaStatus::COMPLETE) {
        return;
    }

    auto seg_id = replica.get_segment_id();
    if (seg_id.has_value()) {
        auto seg_it = shard.segment_key_index.find(seg_id.value());
        if (seg_it != shard.segment_key_index.end()) {
            auto key_it = seg_it->second.find(key);
            if (key_it != seg_it->second.end()) {
                if (--key_it->second == 0) {
                    seg_it->second.erase(key_it);
                }
                if (seg_it->second.empty()) {
                    shard.segment_key_index.erase(seg_it);
                }
            } else {
                LOG(WARNING)
                    << "RemoveReplicaFromSegmentIndex: key not found"
                    << ", segment_id=" << seg_id.value() << ", key=" << key;
            }
        } else {
            LOG(WARNING) << "RemoveReplicaFromSegmentIndex: segment not found"
                         << ", segment_id=" << seg_id.value()
                         << ", key=" << key;
        }
    }
}

auto MasterService::RegisterClient(const RegisterClientRequest& req)
    -> tl::expected<RegisterClientResponse, ErrorCode> {
    return GetClientManager().RegisterClient(req);
}

auto MasterService::Heartbeat(const HeartbeatRequest& req)
    -> tl::expected<HeartbeatResponse, ErrorCode> {
    return GetClientManager().Heartbeat(req);
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

auto MasterService::GetReplicaList(const std::string& key,
                                   const GetReplicaListRequestConfig& config)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    auto accessor = GetMetadataAccessor(std::string(key));

    MasterMetricManager::instance().inc_total_get_nums();

    if (!accessor->Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor->Get();

    std::vector<Replica::Descriptor> replica_list =
        FilterReplicas(config, metadata);

    if (replica_list.empty()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // TODO: wanyue-wy
    // Currently, the hit statistics is maintained in object level in master.
    // If `max_candidates` of config is larger than 1, the statistics is not
    // accurate. (Because we don't know which replica is chosen in client)
    // For more accurate recording, we should maintain it in client in replica
    // level and sync it to master.
    OnObjectHit(metadata);
    OnObjectAccessed(metadata);

    GetReplicaListResponse resp;
    resp.replicas = std::move(replica_list);
    return resp;
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
                RemoveReplicaFromSegmentIndex(shard, it->first,
                                              it->second->replicas_);
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
                RemoveReplicaFromSegmentIndex(shard, it->first,
                                              it->second->replicas_);
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

}  // namespace mooncake