#include "p2p_master_service.h"

#include <glog/logging.h>
#include <algorithm>
#include "p2p_client_meta.h"

namespace mooncake {

P2PMasterService::P2PMasterService(const MasterServiceConfig& config)
    : MasterService(config),
      max_replicas_per_key_(config.max_replicas_per_key) {
    client_manager_ = std::make_shared<P2PClientManager>(
        config.client_live_ttl_sec, config.client_crashed_ttl_sec,
        config.view_version);
    InitializeClientManager();
}

std::vector<Replica::Descriptor> P2PMasterService::FilterReplicas(
    const GetReplicaListRequestConfig& config, const ObjectMetadata& metadata) {
    const auto& p2p_config = config.p2p_config ? config.p2p_config.value()
                                               : P2PGetReplicaListConfigExtra();
    std::vector<std::pair<uint32_t, Replica::Descriptor>> candidates;
    // 1. filter qualified replicas
    for (const auto& replica : metadata.replicas_) {
        if (!replica.is_p2p_proxy_replica()) {
            LOG(ERROR) << "invalid replica type"
                       << ", replica: " << replica;
            continue;
        } else if (!replica.get_p2p_client()->is_health()) {
            // The client of the replica might be disconnected, just skip it.
            // Moreover, it is no need to check health status with client_lock.
            // Although a health client is to be unhealthy in following code,
            // the wrong route will not result in acquiring incorrect data.
            // Because the read is based on client rpc, a disconnected client
            // can't be accessed
            continue;
        }

        // filter with config
        // 1.1 tag filter: exclude replicas whose segment contains
        // any tag listed in tag_filters.
        bool excluded_by_tag = false;
        const auto& p2p_tags = replica.get_p2p_tags();
        for (const auto& tag : p2p_config.tag_filters) {
            if (std::find(p2p_tags.begin(), p2p_tags.end(), tag) !=
                p2p_tags.end()) {
                excluded_by_tag = true;
                break;
            }
        }
        if (excluded_by_tag) continue;

        // 1.2 priority filter
        auto priority_opt = replica.get_p2p_priority();
        if (!priority_opt) {
            LOG(ERROR) << "invalid priority" << ", replica: " << replica;
            continue;
        }
        if (*priority_opt < p2p_config.priority_limit) continue;

        candidates.push_back({*priority_opt, replica.get_descriptor()});
    }  // iter replicas over

    if (config.max_candidates ==
            GetReplicaListRequestConfig::RETURN_ALL_CANDIDATES ||
        config.max_candidates >= candidates.size() || candidates.empty()) {
        // return all candidates
        std::vector<Replica::Descriptor> result;
        result.reserve(candidates.size());
        for (const auto& p : candidates) {
            result.push_back(p.second);
        }
        return result;
    }

    // 2. the number of qualified replicas is larger than limit,
    // choose the best ones.
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    std::vector<Replica::Descriptor> result;
    result.reserve(config.max_candidates);
    for (size_t i = 0; i < config.max_candidates; ++i) {
        result.push_back(candidates[i].second);
    }
    return result;
}

auto P2PMasterService::GetWriteRoute(const WriteRouteRequest& req)
    -> tl::expected<WriteRouteResponse, ErrorCode> {
    // pre check replica num.
    // it might happen that concurrent write for same key.
    // for this case, we will finally check it when add route replica.
    if (!req.key.empty() && max_replicas_per_key_ > 0) {
        auto accessor = GetMetadataAccessor(req.key);
        if (accessor->Exists()) {
            auto& metadata = accessor->Get();
            if (metadata.replicas_.size() >= max_replicas_per_key_) {
                LOG(WARNING)
                    << "replica num exceeded" << ", key: " << req.key
                    << ", client_id: " << req.client_id
                    << ", current replica num:" << metadata.replicas_.size()
                    << ", max replica num: " << max_replicas_per_key_;
                return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
            }
        }
    }

    std::vector<WriteCandidate> candidates;
    // find qualified segments across all clients
    client_manager_->ForEachClient(
        req.config.strategy,
        [&](const std::shared_ptr<ClientMeta>& client)
            -> tl::expected<bool, ErrorCode> {
            auto p2p_client = std::dynamic_pointer_cast<P2PClientMeta>(client);
            if (!p2p_client) {
                LOG(ERROR) << "unexpected client meta type";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            return p2p_client->CollectWriteRouteCandidates(req, candidates);
        });

    WriteRouteResponse response;
    if (candidates.empty()) {
        LOG(ERROR) << "no candidate found for key: " << req.key
                   << ", client_id: " << req.client_id
                   << ", size: " << req.size;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    } else if (req.config.max_candidates ==
                   WriteRouteRequestConfig::RETURN_ALL_CANDIDATES ||
               candidates.size() <= req.config.max_candidates) {
        // return all candidates
        response.candidates = std::move(candidates);
    } else {
        // return top max_candidates candidates
        std::sort(candidates.begin(), candidates.end(),
                  [](const auto& a, const auto& b) {
                      return a.priority > b.priority;
                  });
        candidates.resize(req.config.max_candidates);
        response.candidates = std::move(candidates);
    }
    return response;
}

auto P2PMasterService::AddReplica(const AddReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    auto accessor = GetMetadataAccessor(req.key);
    auto client = std::dynamic_pointer_cast<P2PClientMeta>(
        client_manager_->GetClient(req.replica.client_id));
    if (!client) {
        LOG(ERROR) << "client not found"
                   << ", client_id: " << req.replica.client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto segment_res = client->QuerySegment(req.replica.segment_id);
    if (!segment_res.has_value()) {
        LOG(ERROR) << "fail to query segment"
                   << ", client_id: " << req.replica.client_id
                   << ", segment_id: " << req.replica.segment_id;
        return tl::make_unexpected(segment_res.error());
    }

    // Construct Replica from resolved pointers
    Replica new_replica(P2PProxyReplicaData(client, segment_res.value()),
                        ReplicaStatus::COMPLETE);

    if (accessor->Exists()) {
        auto& metadata = accessor->Get();
        if (max_replicas_per_key_ > 0 &&
            metadata.replicas_.size() >= max_replicas_per_key_) {
            LOG(WARNING) << "replica num exceeded" << ", key: " << req.key
                         << ", client_id: " << req.replica.client_id
                         << ", segment_id: " << req.replica.segment_id
                         << ", current replica num:" << max_replicas_per_key_;
            return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
        }
        // Skip duplicate
        bool exist = false;
        for (const auto& replica : metadata.replicas_) {
            if (!replica.is_p2p_proxy_replica()) {
                LOG(ERROR) << "unexpected replica type" << ", key: " << req.key
                           << ", request client_id: " << req.replica.client_id
                           << ", request segment_id: " << req.replica.segment_id
                           << ", replica:" << replica;
                return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
            }
            auto seg_id = replica.get_segment_id();
            auto cli_id = replica.get_p2p_client_id();
            if (cli_id && seg_id && cli_id == req.replica.client_id &&
                *seg_id == req.replica.segment_id) {
                exist = true;
                break;
            }
        }
        if (exist) {
            LOG(WARNING) << "replica has existed" << ", key: " << req.key
                         << ", client_id: " << req.replica.client_id
                         << ", segment_id: " << req.replica.segment_id;
            return tl::make_unexpected(ErrorCode::REPLICA_ALREADY_EXISTS);
        } else {
            AddReplicaToSegmentIndex(accessor->GetShard(), req.key,
                                     new_replica);
            OnReplicaAdded(new_replica);
            metadata.replicas_.push_back(std::move(new_replica));
        }
    } else {
        std::vector<Replica> replicas;
        replicas.push_back(std::move(new_replica));

        auto new_meta =
            std::make_unique<ObjectMetadata>(req.size, std::move(replicas));

        auto& shard = accessor->GetShard();
        auto it = shard.metadata.emplace(req.key, std::move(new_meta)).first;
        AddReplicaToSegmentIndex(shard, it->first, it->second->replicas_[0]);
        OnReplicaAdded(it->second->replicas_[0]);
    }

    return {};
}

auto P2PMasterService::RemoveReplica(const RemoveReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    auto accessor = GetMetadataAccessor(req.key);
    if (!accessor->Exists()) {
        LOG(WARNING) << "object not found" << ", key: " << req.key
                     << ", client_id: " << req.replica.client_id
                     << ", segment_id: " << req.replica.segment_id;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor->Get();

    bool removed = false;
    for (auto it = metadata.replicas_.begin(); it != metadata.replicas_.end();
         ++it) {
        if (!it->is_p2p_proxy_replica()) {
            LOG(ERROR) << "unexpected replica type" << ", key: " << req.key
                       << ", client_id: " << req.replica.client_id
                       << ", segment_id: " << req.replica.segment_id
                       << ", replica: " << *it;
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        } else {
            auto seg_id = it->get_segment_id();
            auto cli_id = it->get_p2p_client_id();
            if (cli_id && seg_id && cli_id == req.replica.client_id &&
                *seg_id == req.replica.segment_id) {
                RemoveReplicaFromSegmentIndex(accessor->GetShard(), req.key,
                                              *it);
                OnReplicaRemoved(*it);
                metadata.replicas_.erase(it);
                removed = true;
                break;
            }
        }
    }

    if (!removed) {
        LOG(WARNING) << "replica not found" << ", key: " << req.key
                     << ", client_id: " << req.replica.client_id
                     << ", segment_id: " << req.replica.segment_id;
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    } else if (metadata.replicas_.empty()) {
        OnObjectRemoved(metadata);
        accessor->Erase();
    }

    return {};
}

void P2PMasterService::OnObjectAccessed(ObjectMetadata& metadata) {
    // do nothing
}

// TODO: wanyue-wy
// For P2P structure, if a object has multiple replicas,
// we don't know which replica is hit.
// The detailed hit statistic of replica should be synced from
// client to master.
void P2PMasterService::OnObjectHit(const ObjectMetadata& metadata) {
    MasterMetricManager::instance().inc_valid_get_nums();
}

void P2PMasterService::OnReplicaRemoved(const Replica& replica) {
    if (replica.is_p2p_proxy_replica()) {
        auto type = replica.get_p2p_memory_type();
        if (!type) {
            LOG(ERROR) << "invalid memory type"
                       << ", replica: " << replica;
        } else if (*type == MemoryType::DRAM) {
            MasterMetricManager::instance().dec_mem_cache_nums();
        } else if (*type == MemoryType::NVME) {
            MasterMetricManager::instance().dec_file_cache_nums();
        }
    }
}

void P2PMasterService::OnReplicaAdded(const Replica& replica) {
    if (replica.is_p2p_proxy_replica()) {
        auto type = replica.get_p2p_memory_type();
        if (!type) {
            LOG(ERROR) << "invalid memory type"
                       << ", replica: " << replica;
        } else if (*type == MemoryType::DRAM) {
            MasterMetricManager::instance().inc_mem_cache_nums();
        } else if (*type == MemoryType::NVME) {
            MasterMetricManager::instance().inc_file_cache_nums();
        }
    }
}

}  // namespace mooncake
