#include "p2p_master_service.h"

#include <glog/logging.h>
#include <algorithm>
#include <tuple>
#include <unordered_map>
#include <variant>

#include "ha/oplog/p2p_oplog_types.h"
#include "ha/oplog/oplog_store_factory.h"
#include "p2p_client_meta.h"

namespace mooncake {

P2PMasterService::P2PMasterService(const MasterServiceConfig& config)
    : MasterService(config),
      max_client_per_key_(config.max_client_per_key),
      enable_async_oplog_write_(ParseOpLogStoreType(config.oplog_store_type) ==
                                OpLogStoreType::REDIS) {
    client_manager_ = std::make_shared<P2PClientManager>(
        config.client_live_ttl_sec, config.client_crashed_ttl_sec,
        config.view_version);
    InitializeClientManager();
    client_manager_->Start();
}

ErrorCode P2PMasterService::RecordOplog(OpType type, const std::string& key,
                                        const std::string& payload) {
    // TODO: Record remaining failover-visible P2P mutations: client crash
    // cleanup, heartbeat state transitions, replica eviction/rebalance, and
    // task metadata.
    auto* manager = GetOpLogManager();
    if (manager == nullptr) {
        return ErrorCode::OK;
    }

    auto result = manager->AppendAndPersist(
        type, key, payload, /*sync=*/!enable_async_oplog_write_);
    if (!result.has_value()) {
        LOG(ERROR) << "P2PMasterService: failed to persist oplog"
                   << ", op_type=" << static_cast<int>(type) << ", key=" << key
                   << ", error=" << toString(result.error());
        return result.error();
    }
    return ErrorCode::OK;
}

auto P2PMasterService::RegisterClient(const RegisterClientRequest& req)
    -> tl::expected<RegisterClientResponse, ErrorCode> {
    if (GetClientManager().GetClient(req.client_id)) {
        LOG(WARNING) << "RegisterClient(P2P): client already exists"
                     << ", client_id=" << req.client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_ALREADY_EXISTS);
    }

    if (!req.ip_address || !req.rpc_port) {
        LOG(ERROR) << "RegisterClient(P2P): missing endpoint"
                   << ", client_id=" << req.client_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result = MasterService::RegisterClient(req);
    if (!result.has_value()) {
        LOG(ERROR) << "RegisterClient(P2P): failed"
                   << ", client_id=" << req.client_id
                   << ", error=" << result.error();
        return result;
    }

    RegisterClientPayload payload;
    payload.client_id = req.client_id;
    payload.ip_address = *req.ip_address;
    payload.rpc_port = *req.rpc_port;
    payload.segments = req.segments;
    auto err =
        RecordOplog(OpType_REGISTER_CLIENT, "", SerializeP2PPayload(payload));
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RegisterClient(P2P): failed to record oplog"
                   << ", client_id=" << req.client_id
                   << ", error=" << toString(err);
        return tl::make_unexpected(err);
    }
    return result;
}

auto P2PMasterService::UnregisterClient(const UnregisterClientRequest& req)
    -> tl::expected<UnregisterClientResponse, ErrorCode> {
    auto result = MasterService::UnregisterClient(req);
    if (!result.has_value()) {
        LOG(ERROR) << "UnregisterClient(P2P): failed"
                   << ", client_id=" << req.client_id
                   << ", error=" << result.error();
        return result;
    }

    UnregisterClientPayload payload;
    payload.client_id = req.client_id;
    auto err =
        RecordOplog(OpType_UNREGISTER_CLIENT, "", SerializeP2PPayload(payload));
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "UnregisterClient(P2P): failed to record oplog"
                   << ", client_id=" << req.client_id
                   << ", error=" << toString(err);
        return tl::make_unexpected(err);
    }
    return result;
}

auto P2PMasterService::MountSegment(const Segment& segment,
                                    const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    auto result = MasterService::MountSegment(segment, client_id);
    if (!result.has_value()) {
        LOG(ERROR) << "MountSegment(P2P): failed"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment.id
                   << ", segment_name=" << segment.name
                   << ", error=" << result.error();
        return result;
    }

    MountSegmentPayload payload;
    payload.client_id = client_id;
    payload.segment = segment;
    auto err =
        RecordOplog(OpType_MOUNT_SEGMENT, "", SerializeP2PPayload(payload));
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "MountSegment(P2P): failed to record oplog"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment.id
                   << ", segment_name=" << segment.name
                   << ", error=" << toString(err);
        return tl::make_unexpected(err);
    }
    return {};
}

auto P2PMasterService::UnmountSegment(const UUID& segment_id,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    auto result = MasterService::UnmountSegment(segment_id, client_id);
    if (!result.has_value()) {
        LOG(ERROR) << "UnmountSegment(P2P): failed"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment_id
                   << ", error=" << result.error();
        return result;
    }

    UnmountSegmentPayload payload;
    payload.segment_id = segment_id;
    payload.client_id = client_id;
    auto err =
        RecordOplog(OpType_UNMOUNT_SEGMENT, "", SerializeP2PPayload(payload));
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "UnmountSegment(P2P): failed to record oplog"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment_id
                   << ", error=" << toString(err);
        return tl::make_unexpected(err);
    }
    return {};
}

auto P2PMasterService::CollectReplicaOwnerClients(
    const ObjectMetadata& metadata, std::string_view key)
    -> tl::expected<std::unordered_set<UUID, boost::hash<UUID>>, ErrorCode> {
    std::unordered_set<UUID, boost::hash<UUID>> owner_clients;
    for (const auto& replica : metadata.replicas_) {
        if (!replica.is_p2p_proxy_replica()) {
            LOG(ERROR) << "unexpected replica type"
                       << ", key: " << key << ", replica:" << replica;
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        auto client_id = replica.get_p2p_client_id();
        if (!client_id) {
            LOG(ERROR) << "invalid p2p replica"
                       << ", key: " << key << ", replica:" << replica;
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        owner_clients.insert(*client_id);
    }
    return owner_clients;
}

std::vector<Replica::Descriptor> P2PMasterService::FilterReplicas(
    const GetReplicaListRequestConfig& config, const ObjectMetadata& metadata) {
    const auto& p2p_config = config.p2p_config ? config.p2p_config.value()
                                               : P2PGetReplicaListConfigExtra();
    // candidates kept at client granularity
    std::vector<std::pair<uint32_t, Replica::Descriptor>> candidates;
    std::unordered_map<UUID, size_t, boost::hash<UUID>> best_by_client;

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
            LOG(ERROR) << "invalid priority"
                       << ", replica: " << replica;
            continue;
        }
        if (*priority_opt < p2p_config.priority_limit) continue;

        // 1.3 client-granularity: keep the highest-priority replica
        auto cid_opt = replica.get_p2p_client_id();
        if (!cid_opt) continue;
        const UUID cid = *cid_opt;
        auto it = best_by_client.find(cid);
        if (it == best_by_client.end()) {
            best_by_client[cid] = candidates.size();
            candidates.push_back({*priority_opt, replica.get_descriptor()});
        } else if (*priority_opt > candidates[it->second].first) {
            candidates[it->second] = {*priority_opt, replica.get_descriptor()};
        }
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

    // 3. the number of qualified replicas is larger than limit,
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
    if (!req.config.IsValid()) {
        LOG(ERROR) << "invalid write route config: " << req.config
                   << ", client_id: " << req.client_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // 1. Collect existing replica owners and enforce the client limit.
    OwnerClientSet owners;
    if (!req.key.empty()) {
        MetadataAccessorRO accessor(this, req.key);
        if (accessor.Exists()) {
            auto res = CollectReplicaOwnerClients(accessor.Get(), req.key);
            if (!res) {
                LOG(ERROR) << "failed to collect replica owner clients"
                           << ", key: " << req.key
                           << ", error: " << res.error();
                return tl::make_unexpected(res.error());
            }
            if (max_client_per_key_ > 0 && res->size() >= max_client_per_key_) {
                LOG(WARNING)
                    << "replica owner client num exceeded"
                    << ", key: " << req.key << ", client_id: " << req.client_id
                    << ", current: " << res->size()
                    << ", max: " << max_client_per_key_;
                return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
            }
            owners = std::move(*res);
        }
    }

    // 2. Single pass: collect and score all candidates.
    //    score = free_ratio * (is_local ? (1 - remote_weight) : remote_weight)
    const double remote_weight = std::clamp(req.config.remote_weight, 0.0, 1.0);
    std::vector<WriteCandidate> candidates;
    const bool can_early_stop =
        req.config.early_return &&
        req.config.max_candidates !=
            WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;

    client_manager_->ForEachClient(
        req.config.strategy,
        [&](const std::shared_ptr<ClientMeta>& client)
            -> tl::expected<bool, ErrorCode> {
            auto p2p = std::static_pointer_cast<P2PClientMeta>(client);
            if (!p2p) {
                LOG(ERROR) << "unexpected client meta type";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            const UUID cid = p2p->get_client_id();
            if (owners.count(cid)) {
                return false;
            }
            const bool is_local = (cid == req.client_id);
            const double weight =
                is_local ? (1.0 - remote_weight) : remote_weight;
            if (weight <= 0.0) {
                return false;
            }

            if (auto cand = p2p->GetWriteRouteCandidate(req)) {
                cand->score *= weight;
                candidates.push_back(std::move(*cand));
                if (can_early_stop &&
                    candidates.size() >= req.config.max_candidates)
                    return true;
            }
            return false;
        });

    // 3. Sort by score desc (capacity desc as tiebreaker), then truncate.
    if (candidates.empty()) {
        LOG(ERROR) << "no candidate found for key: " << req.key
                   << ", client_id: " << req.client_id
                   << ", size: " << req.size;
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_CANDIDATE);
    }
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) {
                  return std::tie(b.score, b.available_capacity) <
                         std::tie(a.score, a.available_capacity);
              });
    if (req.config.max_candidates !=
            WriteRouteRequestConfig::RETURN_ALL_CANDIDATES &&
        candidates.size() > req.config.max_candidates) {
        candidates.resize(req.config.max_candidates);
    }
    WriteRouteResponse response;
    response.candidates = std::move(candidates);
    return response;
}

auto P2PMasterService::BatchGetWriteRoute(const BatchGetWriteRouteRequest& req)
    -> BatchGetWriteRouteResponse {
    const size_t n = req.keys.size();
    BatchGetWriteRouteResponse response;
    response.responses.resize(n);
    response.error_codes.resize(n, ErrorCode::OK);

    if (req.keys.size() != req.sizes.size()) {
        std::fill(response.error_codes.begin(), response.error_codes.end(),
                  ErrorCode::INVALID_PARAMS);
        return response;
    }

    WriteRouteRequest single_req;
    single_req.client_id = req.client_id;
    single_req.config = req.config;
    for (size_t i = 0; i < n; ++i) {
        single_req.key = req.keys[i];
        single_req.size = req.sizes[i];
        auto result = GetWriteRoute(single_req);
        if (result.has_value()) {
            response.responses[i] = std::move(*result);
        } else {
            response.error_codes[i] = result.error();
        }
    }
    return response;
}

auto P2PMasterService::AddReplica(const AddReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessorRW accessor(this, req.key);
    auto client = std::static_pointer_cast<P2PClientMeta>(
        client_manager_->GetClient(req.client_id));
    if (!client) {
        LOG(ERROR) << "client not found"
                   << ", client_id: " << req.client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return InnerAddReplica(accessor.GetShard().GetRef(), req.key, req.client_id,
                           req.segment_id, req.size, client);
}

tl::expected<void, ErrorCode> P2PMasterService::InnerAddReplica(
    MetadataShard& shard, std::string_view key, const UUID& client_id,
    const UUID& segment_id, size_t size,
    const std::shared_ptr<P2PClientMeta>& client) {
    auto segment_res = client->QuerySegment(segment_id);
    if (!segment_res.has_value()) {
        LOG(ERROR) << "fail to query segment"
                   << ", client_id: " << client_id
                   << ", segment_id: " << segment_id;
        return tl::make_unexpected(segment_res.error());
    }

    Replica new_replica(P2PProxyReplicaData(client, segment_res.value(), size),
                        ReplicaStatus::COMPLETE);

    // AddReplica commits the in-memory route first. OpLog is best-effort;
    // returning an OpLog error could make the client delete its local replica.
    auto it = shard.metadata.find(key);
    if (it != shard.metadata.end()) {
        auto& metadata = *it->second;
        auto owner_clients_res = CollectReplicaOwnerClients(metadata, key);
        if (!owner_clients_res.has_value()) {
            LOG(ERROR) << "failed to collect replica owner clients"
                       << ", key: " << key
                       << ", error: " << owner_clients_res.error();
            return tl::make_unexpected(owner_clients_res.error());
        }
        const auto& owner_clients = owner_clients_res.value();
        for (const auto& replica : metadata.replicas_) {
            auto seg_id = replica.get_segment_id();
            auto cli_id = replica.get_p2p_client_id();
            if (cli_id && seg_id && *cli_id == client_id &&
                *seg_id == segment_id) {
                LOG(WARNING) << "replica has existed"
                             << ", key: " << key << ", client_id: " << client_id
                             << ", segment_id: " << segment_id;
                return tl::make_unexpected(ErrorCode::REPLICA_ALREADY_EXISTS);
            }
        }
        // AddReplica is also used by in-client tier migration to publish a new
        // physical replica. Existing owner clients may add replicas on new
        // segments, but a new owner client must honor max_client_per_key_.
        if (max_client_per_key_ > 0 &&
            owner_clients.find(client_id) == owner_clients.end() &&
            owner_clients.size() >= max_client_per_key_) {
            LOG(WARNING) << "replica owner client num exceeded"
                         << ", key: " << key << ", client_id: " << client_id
                         << ", segment_id: " << segment_id
                         << ", current owner client num:"
                         << owner_clients.size()
                         << ", max owner client num: " << max_client_per_key_;
            return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
        }
        AddReplicaPayload payload;
        payload.object_key = std::string(key);
        payload.client_id = client_id;
        payload.segment_id = segment_id;
        payload.size = size;
        AddReplicaToSegmentIndex(shard, it->first, new_replica);
        OnReplicaAdded(new_replica);
        metadata.replicas_.push_back(std::move(new_replica));
        ErrorCode record_err =
            RecordOplog(OpType_ADD_REPLICA, payload.object_key,
                        SerializeP2PPayload(payload));
        if (record_err != ErrorCode::OK) {
            LOG(ERROR) << "AddReplica(P2P): failed to record oplog"
                       << ", client_id=" << client_id
                       << ", segment_id=" << segment_id
                       << ", error=" << toString(record_err)
                       << "; keeping the in-memory route";
        }
    } else {
        std::vector<Replica> replicas;
        replicas.push_back(std::move(new_replica));
        auto new_meta =
            std::make_unique<ObjectMetadata>(size, std::move(replicas));
        AddReplicaPayload payload;
        payload.object_key = std::string(key);
        payload.client_id = client_id;
        payload.segment_id = segment_id;
        payload.size = size;
        auto emplace_it =
            shard.metadata.emplace(std::string(key), std::move(new_meta)).first;
        AddReplicaToSegmentIndex(shard, emplace_it->first,
                                 emplace_it->second->replicas_[0]);
        OnReplicaAdded(emplace_it->second->replicas_[0]);
        ErrorCode record_err =
            RecordOplog(OpType_ADD_REPLICA, payload.object_key,
                        SerializeP2PPayload(payload));
        if (record_err != ErrorCode::OK) {
            LOG(ERROR) << "AddReplica(P2P): failed to record oplog"
                       << ", client_id=" << client_id
                       << ", segment_id=" << segment_id
                       << ", error=" << toString(record_err)
                       << "; keeping the in-memory route";
        }
    }
    return {};
}

auto P2PMasterService::RemoveReplica(const RemoveReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessorRW accessor(this, req.key);
    return InnerRemoveReplica(accessor.GetShard().GetRef(), req.key,
                              req.client_id, req.segment_id);
}

tl::expected<void, ErrorCode> P2PMasterService::InnerRemoveReplica(
    MetadataShard& shard, std::string_view key, const UUID& client_id,
    const UUID& segment_id) {
    auto it = shard.metadata.find(key);
    if (it == shard.metadata.end()) {
        LOG(WARNING) << "object not found"
                     << ", key: " << key << ", client_id: " << client_id
                     << ", segment_id: " << segment_id;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = *it->second;
    for (auto rit = metadata.replicas_.begin(); rit != metadata.replicas_.end();
         ++rit) {
        if (!rit->is_p2p_proxy_replica()) {
            LOG(ERROR) << "unexpected replica type"
                       << ", key: " << key << ", client_id: " << client_id
                       << ", segment_id: " << segment_id
                       << ", replica: " << *rit;
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        auto seg_id = rit->get_segment_id();
        auto cli_id = rit->get_p2p_client_id();
        if (cli_id && seg_id && cli_id == client_id && *seg_id == segment_id) {
            RemoveReplicaPayload payload;
            payload.object_key = std::string(key);
            payload.client_id = client_id;
            payload.segment_id = segment_id;
            ErrorCode record_err =
                RecordOplog(OpType_REMOVE_REPLICA, payload.object_key,
                            SerializeP2PPayload(payload));
            if (record_err != ErrorCode::OK) {
                LOG(ERROR) << "RemoveReplica(P2P): failed to record oplog"
                           << ", client_id=" << client_id
                           << ", segment_id=" << segment_id
                           << ", error=" << toString(record_err);
                return tl::make_unexpected(record_err);
            }
            RemoveReplicaFromSegmentIndex(shard, it->first, *rit);
            OnReplicaRemoved(*rit);
            metadata.replicas_.erase(rit);
            if (metadata.replicas_.empty()) {
                OnObjectRemoved(metadata);
                shard.metadata.erase(it);
            }
            return {};
        }
    }

    LOG(WARNING) << "replica not found"
                 << ", key: " << key << ", client_id: " << client_id
                 << ", segment_id: " << segment_id;
    return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
}

auto P2PMasterService::BatchRemoveReplica(const BatchRemoveReplicaRequest& req)
    -> std::vector<tl::expected<void, ErrorCode>> {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(req.segment_ids.size());

    RemoveReplicaRequest single_req;
    single_req.key = req.key;
    single_req.client_id = req.client_id;
    for (const auto& segment_id : req.segment_ids) {
        single_req.segment_id = segment_id;
        auto result = RemoveReplica(single_req);
        if (!result.has_value()) {
            if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
                // This may happen if the object is removed by another thread
                LOG(INFO) << "object not found when batch remove replica"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
                results.push_back({});
            } else if (result.error() == ErrorCode::REPLICA_NOT_FOUND) {
                // This may happen if the replica is removed by another thread
                LOG(INFO) << "replica not found when batch remove replica"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
                results.push_back({});
            } else {
                LOG(ERROR) << "failed to remove replica"
                           << ", key: " << req.key
                           << ", client_id: " << req.client_id
                           << ", segment_id: " << segment_id
                           << ", error: " << toString(result.error());
                results.push_back(tl::make_unexpected(result.error()));
            }
        } else {
            results.push_back({});
        }
    }
    return results;
}

auto P2PMasterService::BatchSyncReplica(const BatchSyncReplicaRequest& req)
    -> BatchSyncReplicaResponse {
    // Validate SoA array lengths are consistent
    if (req.add_keys.size() != req.add_sizes.size() ||
        req.add_keys.size() != req.add_segment_ids.size() ||
        req.remove_keys.size() != req.remove_segment_ids.size()) {
        LOG(ERROR) << "BatchSyncReplica: mismatched array sizes"
                   << ", add_keys=" << req.add_keys.size()
                   << ", add_sizes=" << req.add_sizes.size()
                   << ", add_segment_ids=" << req.add_segment_ids.size()
                   << ", remove_keys=" << req.remove_keys.size()
                   << ", remove_segment_ids=" << req.remove_segment_ids.size();
        BatchSyncReplicaResponse err_resp;
        err_resp.add_results.assign(req.add_keys.size(),
                                    ErrorCode::INVALID_PARAMS);
        err_resp.remove_results.assign(req.remove_keys.size(),
                                       ErrorCode::INVALID_PARAMS);
        return err_resp;
    }

    BatchSyncReplicaResponse response;
    response.add_results.resize(req.add_keys.size(), ErrorCode::OK);
    response.remove_results.resize(req.remove_keys.size(), ErrorCode::OK);

    // Resolve client once for all operations
    auto client = std::static_pointer_cast<P2PClientMeta>(
        client_manager_->GetClient(req.client_id));
    if (!client) {
        LOG(ERROR) << "BatchSyncReplica: client not found"
                   << ", client_id=" << req.client_id;
        std::fill(response.add_results.begin(), response.add_results.end(),
                  ErrorCode::CLIENT_NOT_FOUND);
        std::fill(response.remove_results.begin(),
                  response.remove_results.end(), ErrorCode::CLIENT_NOT_FOUND);
        return response;
    }

    // Group operations by shard index.
    // Each entry: (original_index, is_add=true/false)
    std::unordered_map<size_t, std::vector<std::pair<size_t, bool>>>
        shard_groups;

    for (size_t i = 0; i < req.add_keys.size(); ++i) {
        size_t shard_idx = GetShardIndex(req.add_keys[i]);
        shard_groups[shard_idx].emplace_back(i, true);
    }
    for (size_t i = 0; i < req.remove_keys.size(); ++i) {
        size_t shard_idx = GetShardIndex(req.remove_keys[i]);
        shard_groups[shard_idx].emplace_back(i, false);
    }

    // Process each shard group with one lock acquisition
    for (auto& [shard_idx, ops] : shard_groups) {
        MetadataShardAccessorRW shard_rw(this, shard_idx);
        auto& shard = shard_rw.GetRef();

        for (auto& [idx, is_add] : ops) {
            if (is_add) {
                auto result = InnerAddReplica(
                    shard, req.add_keys[idx], req.client_id,
                    req.add_segment_ids[idx], req.add_sizes[idx], client);
                if (!result.has_value()) {
                    response.add_results[idx] = result.error();
                }
            } else {
                auto result = InnerRemoveReplica(shard, req.remove_keys[idx],
                                                 req.client_id,
                                                 req.remove_segment_ids[idx]);
                if (!result.has_value() &&
                    result.error() != ErrorCode::OBJECT_NOT_FOUND &&
                    result.error() != ErrorCode::REPLICA_NOT_FOUND) {
                    response.remove_results[idx] = result.error();
                }
            }
        }
    }

    return response;
}

auto P2PMasterService::SetSyncCompleted(UUID client_id)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(client_id);
    if (!client) {
        LOG(WARNING) << "SetSyncCompleted: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto p2p_client = std::dynamic_pointer_cast<P2PClientMeta>(client);
    if (!p2p_client) {
        LOG(ERROR) << "SetSyncCompleted: client is not P2PClientMeta"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    p2p_client->SetSyncing(false);
    LOG(INFO) << "SetSyncCompleted: client_id=" << client_id;
    return {};
}

ErrorCode P2PMasterService::RestoreFromStandbyMetadata(
    const P2PStandbyMetadataStore::ExportedMetadata& metadata,
    uint64_t last_applied_sequence_id) {
    if (GetKeyCount() != 0 || !client_manager_->GetAllClients().empty()) {
        LOG(ERROR) << "RestoreFromStandbyMetadata: target service is not empty"
                   << ", existing_keys=" << GetKeyCount()
                   << ", existing_clients="
                   << client_manager_->GetAllClients().size();
        return ErrorCode::INVALID_PARAMS;
    }

    if (last_applied_sequence_id > 0) {
        auto* manager = GetOpLogManager();
        if (manager) {
            manager->SetInitialSequenceId(last_applied_sequence_id);
        } else {
            LOG(WARNING)
                << "RestoreFromStandbyMetadata: cannot set initial OpLog "
                   "sequence without OpLogManager"
                << ", last_applied_sequence_id=" << last_applied_sequence_id;
        }
    }

    size_t restored_clients = 0;
    size_t restored_objects = 0;
    size_t restored_replicas = 0;
    size_t skipped_replicas = 0;

    for (const auto& [client_id, client_info] : metadata.clients) {
        RegisterClientRequest req;
        req.client_id = client_id;
        req.ip_address = client_info.ip_address;
        req.rpc_port = client_info.rpc_port;
        req.segments = client_info.segments;
        req.deployment_mode = DeploymentMode::P2P;

        auto result = MasterService::RegisterClient(req);
        if (!result.has_value()) {
            LOG(ERROR) << "RestoreFromStandbyMetadata: failed to restore client"
                       << ", client_id=" << client_id
                       << ", error=" << toString(result.error());
            return result.error();
        }

        auto p2p_client = std::dynamic_pointer_cast<P2PClientMeta>(
            client_manager_->GetClient(client_id));
        if (p2p_client) {
            p2p_client->SetSyncing(false);
        }
        ++restored_clients;
    }

    for (const auto& [key, standby_metadata] : metadata.objects) {
        std::vector<Replica> replicas;
        replicas.reserve(standby_metadata.replicas.size());

        // TODO: Make promotion restore strict once gap/out-of-order handling is
        // hardened. Missing clients/segments should follow an explicit policy
        // instead of silently producing a partially restored object.
        for (const auto& desc : standby_metadata.replicas) {
            if (!std::holds_alternative<P2PProxyDescriptor>(
                    desc.descriptor_variant)) {
                LOG(WARNING)
                    << "RestoreFromStandbyMetadata: skip non-P2P replica"
                    << ", key=" << key << ", replica=" << desc;
                ++skipped_replicas;
                continue;
            }

            const auto& p2p_desc =
                std::get<P2PProxyDescriptor>(desc.descriptor_variant);
            auto client = std::dynamic_pointer_cast<P2PClientMeta>(
                client_manager_->GetClient(p2p_desc.client_id));
            if (!client) {
                LOG(WARNING)
                    << "RestoreFromStandbyMetadata: skip replica with missing "
                       "client"
                    << ", key=" << key << ", client_id=" << p2p_desc.client_id
                    << ", segment_id=" << p2p_desc.segment_id;
                ++skipped_replicas;
                continue;
            }

            auto segment = client->QuerySegment(p2p_desc.segment_id);
            if (!segment.has_value()) {
                LOG(WARNING)
                    << "RestoreFromStandbyMetadata: skip replica with missing "
                       "segment"
                    << ", key=" << key << ", client_id=" << p2p_desc.client_id
                    << ", segment_id=" << p2p_desc.segment_id
                    << ", error=" << toString(segment.error());
                ++skipped_replicas;
                continue;
            }

            const uint64_t object_size = p2p_desc.object_size != 0
                                             ? p2p_desc.object_size
                                             : standby_metadata.size;
            replicas.emplace_back(
                P2PProxyReplicaData(client, segment.value(), object_size),
                ReplicaStatus::COMPLETE);
        }

        if (replicas.empty()) {
            LOG(WARNING)
                << "RestoreFromStandbyMetadata: skip object with no restorable "
                   "replicas"
                << ", key=" << key;
            continue;
        }

        MetadataAccessorRW accessor(this, key);
        auto& shard = accessor.GetShard().GetRef();
        auto new_meta = std::make_unique<ObjectMetadata>(standby_metadata.size,
                                                         std::move(replicas));
        auto [it, inserted] = shard.metadata.emplace(key, std::move(new_meta));
        if (!inserted) {
            LOG(ERROR)
                << "RestoreFromStandbyMetadata: object already exists despite "
                   "empty-target check"
                << ", key=" << key;
            return ErrorCode::INTERNAL_ERROR;
        }

        for (const auto& replica : it->second->replicas_) {
            AddReplicaToSegmentIndex(shard, it->first, replica);
            OnReplicaAdded(replica);
            ++restored_replicas;
        }
        ++restored_objects;
    }

    LOG(INFO) << "RestoreFromStandbyMetadata: restored"
              << ", clients=" << restored_clients
              << ", objects=" << restored_objects
              << ", replicas=" << restored_replicas
              << ", skipped_replicas=" << skipped_replicas
              << ", last_applied_sequence_id=" << last_applied_sequence_id;
    return ErrorCode::OK;
}

void P2PMasterService::OnObjectAccessed(const ObjectMetadata& metadata) {
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
