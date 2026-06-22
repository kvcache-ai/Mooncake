#include "native_metadata_backend.h"

#include "master_metric_manager.h"

namespace mooncake {

namespace {

tl::expected<std::string, ErrorCode> GetGroupIdForKey(
    const ReplicateConfig& config, size_t key_count, size_t key_index) {
    if (!config.group_ids.has_value()) {
        return "";
    }
    if (config.group_ids->size() != key_count || key_index >= key_count) {
        LOG(ERROR) << "group_ids.size()=" << config.group_ids->size()
                   << ", key_count=" << key_count
                   << ", error=invalid_group_ids";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return config.group_ids->at(key_index);
}

}  // namespace

// ---------------------------------------------------------------------------
// Key existence and enumeration
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::ExistKey(const std::string& key,
                                     const std::string& tenant_id)
    -> tl::expected<bool, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    MasterService::MetadataAccessorRO accessor(master_, MasterService::MakeObjectIdentity(key, tenant_id));
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    const auto& metadata = accessor.Get();
    if (metadata.HasReplica(&Replica::fn_is_completed)) {
        // Grant a lease to the object as it may be further used by the
        // client.
        auto* ts = accessor.GetTenantState();
        if (ts) {
            master_->GrantLeaseForGroup(*ts, key, metadata);
        } else {
            metadata.GrantLease(master_->default_kv_lease_ttl_,
                                master_->default_kv_soft_pin_ttl_);
        }
        return true;
    }

    return false;  // If no complete replica is found, return false
}

std::vector<tl::expected<bool, ErrorCode>>
NativeMetadataBackend::BatchExistKey(const std::vector<std::string>& keys,
                                     const std::string& tenant_id) {
    std::vector<tl::expected<bool, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(ExistKey(key, tenant_id));
    }
    return results;
}

auto NativeMetadataBackend::GetAllKeys(const std::string& tenant_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    for (size_t i = 0; i < MasterService::kNumShards; i++) {
        MasterService::MetadataShardAccessorRO shard(master_, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        for (const auto& item : tenant_it->second.metadata) {
            all_keys.push_back(item.second.user_key.empty()
                                   ? item.first
                                   : item.second.user_key);
        }
    }
    return all_keys;
}

size_t NativeMetadataBackend::GetKeyCount() const {
    size_t count = 0;
    for (size_t i = 0; i < MasterService::kNumShards; i++) {
        MasterService::MetadataShardAccessorRO shard(master_, i);
        for (const auto& [tenant_id, tenant_state] : shard->tenants) {
            count += tenant_state.metadata.size();
        }
    }
    return count;
}

// ---------------------------------------------------------------------------
// Replica queries
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::GetReplicaList(const std::string& key,
                                           const std::string& tenant_id)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);

    GetReplicaListResponse resp({}, master_->default_kv_lease_ttl_);
    bool promotion_eligible = false;
    {
        MasterService::MetadataAccessorRO accessor(master_, object_id);

        MasterMetricManager::instance().inc_total_get_nums();

        if (!accessor.Exists()) {
            VLOG(1) << "key=" << key << ", info=object_not_found";
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        const auto& metadata = accessor.Get();

        std::vector<Replica::Descriptor> replica_list;
        metadata.VisitReplicas(
            &Replica::fn_is_completed, [&replica_list](const Replica& replica) {
                replica_list.emplace_back(replica.get_descriptor());
            });

        if (replica_list.empty()) {
            LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
            return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
        }

        // TODO: NoF SSD support (ranhaojia)
        if (replica_list[0].is_memory_replica()) {
            MasterMetricManager::instance().inc_mem_cache_hit_nums();
        } else if (replica_list[0].is_disk_replica()) {
            MasterMetricManager::instance().inc_file_cache_hit_nums();
        }
        MasterMetricManager::instance().inc_valid_get_nums();
        // Grant a lease to the object so it will not be removed
        // when the client is reading it.
        auto* ts = accessor.GetTenantState();
        if (ts) {
            master_->GrantLeaseForGroup(*ts, key, metadata);
        } else {
            metadata.GrantLease(master_->default_kv_lease_ttl_,
                                master_->default_kv_soft_pin_ttl_);
        }

        // Promotion-on-hit eligibility: only when no MEMORY replica is
        // present but at least one LOCAL_DISK replica is. Decided here while
        // we hold the RO accessor; the actual enqueue happens after we
        // release the accessor below to avoid lock-upgrade complexity.
        if (master_->promotion_on_hit_) {
            const bool any_memory =
                metadata.HasReplica(&Replica::fn_is_memory_replica);
            const bool any_local_disk =
                metadata.HasReplica(&Replica::fn_is_local_disk_replica);
            promotion_eligible = !any_memory && any_local_disk;
        }

        resp = GetReplicaListResponse(std::move(replica_list),
                                      master_->default_kv_lease_ttl_);
    }
    // RO accessor released. Safe to take a fresh RW accessor now.
    if (promotion_eligible) {
        master_->TryPushPromotionQueue(object_id);
    }
    return resp;
}

auto NativeMetadataBackend::GetReplicaListByRegex(
    const std::string& regex_pattern, const std::string& tenant_id)
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

    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    for (size_t i = 0; i < MasterService::kNumShards; ++i) {
        MasterService::MetadataShardAccessorRO shard(master_, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        for (const auto& [key, metadata] : tenant_it->second.metadata) {
            if (std::regex_search(key, pattern)) {
                std::vector<Replica::Descriptor> replica_list;
                metadata.VisitReplicas(
                    &Replica::fn_is_completed,
                    [&replica_list](const Replica& replica) {
                        replica_list.emplace_back(replica.get_descriptor());
                    });

                if (replica_list.empty()) {
                    LOG(WARNING)
                        << "key=" << key
                        << " matched by regex, but has no complete replicas.";
                    continue;
                }

                results.emplace(key, std::move(replica_list));
                master_->GrantLeaseForGroup(tenant_it->second, key, metadata);
            }
        }
    }

    return results;
}

// ---------------------------------------------------------------------------
// Put lifecycle
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::PutStart(const UUID& client_id,
                                     const std::string& key,
                                     const std::string& tenant_id,
                                     const uint64_t slice_length,
                                     const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    if ((config.replica_num == 0 && config.nof_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.prefer_alloc_in_same_node && config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", prefer_alloc_in_same_node="
                   << config.prefer_alloc_in_same_node
                   << ", error=nof_not_supported_with_prefer_same_node";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#ifndef USE_NOF
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", error=nof_pool_disabled";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#endif

    if ((master_->memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        LOG(ERROR) << "key=" << key << ", slice_length=" << slice_length
                   << ", max_size=" << kMaxSliceSize
                   << ", error=invalid_slice_size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    VLOG(1) << "key=" << key << ", value_length=" << slice_length
            << ", config=" << config << ", action=put_start_begin";

    auto group_id_result = GetGroupIdForKey(config, 1, 0);
    if (!group_id_result) {
        return tl::make_unexpected(group_id_result.error());
    }
    const std::string group_id = group_id_result.value();

    [[maybe_unused]] auto object_operation_lock =
        master_->AcquireObjectOperationLock(object_id.tenant_id,
                                            object_id.user_key);
    const auto now = std::chrono::system_clock::now();
    std::optional<size_t> retry_shard_idx;
    {
        auto alive_clients = master_->getAliveClientsSnapshot();
        std::shared_lock<std::shared_mutex> shared_lock(
            master_->snapshot_mutex_);
        const size_t lookup_shard_idx = master_->getMetadataShardIndex(
            object_id.tenant_id, object_id.user_key);
        MasterService::MetadataShardAccessorRW shard(master_, lookup_shard_idx);
        auto& tenant_state = shard->tenants[object_id.tenant_id];

        auto it = tenant_state.metadata.find(key);
        if (it != tenant_state.metadata.end()) {
            if (master_->CleanupStaleHandles(it->second, alive_clients)) {
                tenant_state.processing_keys.erase(key);
                tenant_state.replication_tasks.erase(key);
                tenant_state.offloading_tasks.erase(key);
                master_->ErasePromotionTaskIfPresent(tenant_state, key);
                master_->EraseMetadata(tenant_state, it, object_id.tenant_id);
                it = tenant_state.metadata.end();
            } else {
                auto& metadata = it->second;
                if (metadata.HasReplica(&Replica::fn_is_completed) ||
                    metadata.put_start_time +
                            master_->put_start_discard_timeout_sec_ >=
                        now) {
                    LOG(INFO)
                        << "key=" << key << ", info=object_already_exists";
                    return tl::make_unexpected(
                        ErrorCode::OBJECT_ALREADY_EXISTS);
                }
                auto replicas =
                    metadata.PopReplicas(&Replica::fn_is_processing);
                if (!replicas.empty()) {
                    std::lock_guard lock(master_->discarded_replicas_mutex_);
                    master_->discarded_replicas_.emplace_back(
                        std::move(replicas),
                        metadata.put_start_time +
                            master_->put_start_release_timeout_sec_);
                }
                tenant_state.processing_keys.erase(key);
                master_->EraseMetadata(tenant_state, it, object_id.tenant_id);
                it = tenant_state.metadata.end();
            }
        }

        if (it == tenant_state.metadata.end()) {
            const size_t target_shard_idx =
                group_id.empty()
                    ? master_->getShardIndex(object_id.tenant_id,
                                             object_id.user_key)
                    : master_->getShardIndex(group_id);
            if (target_shard_idx != lookup_shard_idx) {
                retry_shard_idx = target_shard_idx;
                if (tenant_state.Empty()) {
                    shard->tenants.erase(object_id.tenant_id);
                }
            } else {
                return master_->AllocateAndInsertMetadata(
                    shard, client_id, key, slice_length, config, group_id,
                    object_id.tenant_id, now);
            }
        }
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    MasterService::MetadataShardAccessorRW shard(master_,
                                                 retry_shard_idx.value());
    auto& retry_tenant_state = shard->tenants[object_id.tenant_id];
    if (master_->GetGroupRoute(object_id.tenant_id, object_id.user_key)
            .has_value() ||
        retry_tenant_state.metadata.contains(key)) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    return master_->AllocateAndInsertMetadata(shard, client_id, key,
                                              slice_length, config, group_id,
                                              object_id.tenant_id, now);
}

auto NativeMetadataBackend::PutEnd(const UUID& client_id,
                                   const std::string& key,
                                   const std::string& tenant_id,
                                   ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    MasterService::MetadataAccessorRW accessor(master_, object_id);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutEnd key " << key
                   << ", was PutStart-ed by " << metadata.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    metadata.VisitReplicas(
        [replica_type](const Replica& replica) {
            if (replica_type == ReplicaType::ALL) {
                return (replica.is_memory_replica() &&
                        !replica.has_invalid_mem_handle()) ||
                       (replica.is_nof_replica() &&
                        !replica.has_invalid_nof_handle());
            }
            if (replica_type == ReplicaType::MEMORY) {
                return replica.is_memory_replica() &&
                       !replica.has_invalid_mem_handle();
            }
            if (replica_type == ReplicaType::NOF_SSD) {
                return replica.is_nof_replica() &&
                       !replica.has_invalid_nof_handle();
            }
            return replica.type() == replica_type;
        },
        [](Replica& replica) { replica.mark_complete(); });

    if (master_->enable_offload_ && !master_->offload_on_evict_) {
        auto& tenant_state = accessor.GetTenantState();
        metadata.VisitReplicas(
            [](const Replica& replica) {
                return replica.is_completed() && replica.is_memory_replica();
            },
            [this, &object_id, &tenant_state](Replica& replica) {
                auto result =
                    master_->PushOffloadingQueue(object_id, replica);
                if (result) {
                    replica.inc_refcnt();
                    tenant_state.offloading_tasks.emplace(
                        object_id.user_key,
                        MasterService::OffloadingTask{
                            replica.id(),
                            std::chrono::system_clock::now()});
                }
            });
    }

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    if (replica_type == ReplicaType::MEMORY ||
        (replica_type == ReplicaType::ALL && metadata.HasMemReplica())) {
        MasterMetricManager::instance().inc_mem_cache_nums();
    } else if (replica_type == ReplicaType::DISK) {
        MasterMetricManager::instance().inc_file_cache_nums();
    }  // TODO: add inc_nof_cache_nums() (ranhaojia)
    // 1. Set lease timeout to now, indicating that the object has no lease
    // at beginning. 2. If this object has soft pin enabled, set it to be soft
    // pinned.
    metadata.GrantLease(0, master_->default_kv_soft_pin_ttl_);
    return {};
}

auto NativeMetadataBackend::AddReplica(const UUID& client_id,
                                       const std::string& key,
                                       const std::string& tenant_id,
                                       Replica& replica)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    MasterService::MetadataAccessorRW accessor(
        master_, MasterService::MakeObjectIdentity(key, tenant_id));
    if (!accessor.Exists()) {
        accessor.Create(
            client_id,
            replica.get_descriptor().get_local_disk_descriptor().object_size,
            std::vector<Replica>{}, false);
    }
    auto& metadata = accessor.Get();
    if (replica.type() != ReplicaType::LOCAL_DISK) {
        LOG(ERROR) << "Invalid replica type: " << replica.type()
                   << ". Expected ReplicaType::LOCAL_DISK.";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (!metadata.HasReplica(&Replica::fn_is_local_disk_replica)) {
        std::vector<Replica> replicas;
        replicas.emplace_back(std::move(replica));
        metadata.AddReplicas(std::move(replicas));
        return {};
    }

    metadata.VisitReplicas(
        [client_id](const Replica& rep) {
            return rep.type() == ReplicaType::LOCAL_DISK &&
                   rep.get_descriptor().get_local_disk_descriptor().client_id ==
                       client_id;
        },
        [&replica](Replica& rep) {
            rep.get_descriptor()
                .get_local_disk_descriptor()
                .transport_endpoint = replica.get_descriptor()
                                          .get_local_disk_descriptor()
                                          .transport_endpoint;
            rep.get_descriptor().get_local_disk_descriptor().object_size =
                replica.get_descriptor()
                    .get_local_disk_descriptor()
                    .object_size;
        });
    return {};
}

auto NativeMetadataBackend::PutRevoke(const UUID& client_id,
                                      const std::string& key,
                                      const std::string& tenant_id,
                                      ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    MasterService::MetadataAccessorRW accessor(
        master_, MasterService::MakeObjectIdentity(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutRevoke key "
                   << key << ", was PutStart-ed by " << metadata.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    auto processing_rep = metadata.GetFirstReplica([replica_type](
                                                       const Replica& replica) {
        if (replica_type == ReplicaType::ALL) {
            return (replica.is_memory_replica() || replica.is_nof_replica()) &&
                   !replica.is_processing();
        }
        return replica.type() == replica_type && !replica.is_processing();
    });
    if (processing_rep != nullptr) {
        LOG(ERROR) << "key=" << key << ", status=" << processing_rep->status()
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    if (replica_type == ReplicaType::MEMORY ||
        (replica_type == ReplicaType::ALL && metadata.HasMemReplica())) {
        MasterMetricManager::instance().dec_mem_cache_nums();
    } else if (replica_type == ReplicaType::DISK) {
        MasterMetricManager::instance().dec_file_cache_nums();
    }

    metadata.EraseReplicas([replica_type](const Replica& replica) {
        if (replica_type == ReplicaType::ALL) {
            return replica.is_memory_replica() || replica.is_nof_replica();
        }
        return replica.type() == replica_type;
    });

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    if (metadata.IsValid() == false) {
        accessor.Erase();
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>>
NativeMetadataBackend::BatchPutEnd(const UUID& client_id,
                                   const std::vector<std::string>& keys,
                                   const std::string& tenant_id,
                                   ReplicaType replica_type) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutEnd(client_id, key, tenant_id, replica_type));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
NativeMetadataBackend::BatchPutRevoke(const UUID& client_id,
                                      const std::vector<std::string>& keys,
                                      const std::string& tenant_id,
                                      ReplicaType replica_type) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(
            PutRevoke(client_id, key, tenant_id, replica_type));
    }
    return results;
}

// ---------------------------------------------------------------------------
// Upsert lifecycle
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::UpsertStart(const UUID& client_id,
                                        const std::string& key,
                                        const std::string& tenant_id,
                                        const uint64_t slice_length,
                                        const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    // --- Parameter validation (same as PutStart) ---
    if ((config.replica_num == 0 && config.nof_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.prefer_alloc_in_same_node && config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", prefer_alloc_in_same_node="
                   << config.prefer_alloc_in_same_node
                   << ", error=nof_not_supported_with_prefer_same_node";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#ifndef USE_NOF
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", error=nof_pool_disabled";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#endif

    if ((master_->memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        LOG(ERROR) << "key=" << key << ", slice_length=" << slice_length
                   << ", max_size=" << kMaxSliceSize
                   << ", error=invalid_slice_size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    VLOG(1) << "key=" << key << ", value_length=" << slice_length
            << ", config=" << config << ", action=upsert_start_begin";

    auto group_id_result = GetGroupIdForKey(config, 1, 0);
    if (!group_id_result) {
        return tl::make_unexpected(group_id_result.error());
    }
    const std::string group_id = group_id_result.value();

    [[maybe_unused]] auto object_operation_lock =
        master_->AcquireObjectOperationLock(object_id.tenant_id,
                                            object_id.user_key);
    const auto now = std::chrono::system_clock::now();
    std::optional<size_t> case_a_retry_shard_idx;
    {
        // --- Lock acquisition ---
        auto alive_clients = master_->getAliveClientsSnapshot();
        std::shared_lock<std::shared_mutex> shared_lock(
            master_->snapshot_mutex_);
        // Use getMetadataShardIndex to find the object at its current shard
        // (handles both grouped and ungrouped routing).
        const size_t lookup_shard_idx = master_->getMetadataShardIndex(
            object_id.tenant_id, object_id.user_key);
        MasterService::MetadataShardAccessorRW shard(master_, lookup_shard_idx);
        auto& tenant_state = shard->tenants[object_id.tenant_id];

        auto it = tenant_state.metadata.find(key);

        // --- Step 0: stale handle cleanup ---
        if (it != tenant_state.metadata.end() &&
            master_->CleanupStaleHandles(it->second, alive_clients)) {
            tenant_state.processing_keys.erase(key);
            master_->ErasePromotionTaskIfPresent(tenant_state, key);
            master_->EraseMetadata(tenant_state, it, object_id.tenant_id);
            it = tenant_state.metadata.end();
        }

        // --- Step 1: safety checks and preemption (only if key exists) ---
        if (it != tenant_state.metadata.end()) {
            auto& metadata = it->second;

            // Reject if the caller tries to change group membership.
            // Group membership is immutable while an object exists.
            if (config.group_ids.has_value() && metadata.group_id != group_id) {
                LOG(ERROR) << "key=" << key
                           << ", error=group_membership_is_immutable";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            // Reject if a Copy/Move task is actively reading this key's
            // replicas.
            if (tenant_state.replication_tasks.count(key) > 0) {
                LOG(INFO) << "key=" << key
                          << ", error=object_has_replication_task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_HAS_REPLICATION_TASK);
            }

            // Reject if an offload-to-disk task is in progress (same reason).
            if (tenant_state.offloading_tasks.count(key) > 0) {
                LOG(INFO) << "key=" << key
                          << ", error=object_has_offloading_task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_HAS_REPLICATION_TASK);
            }

            // Preempt an in-progress Put/Upsert on the same key.  The previous
            // writer's PROCESSING replicas are moved to discarded_replicas_
            // with a TTL so they are not freed while the old writer may still
            // be doing RDMA writes.  Unlike PutStart (which only preempts after
            // a timeout), UpsertStart preempts immediately.
            if (tenant_state.processing_keys.count(key) > 0) {
                auto processing_replicas =
                    metadata.PopReplicas(&Replica::fn_is_processing);
                if (!processing_replicas.empty()) {
                    std::lock_guard lock(master_->discarded_replicas_mutex_);
                    master_->discarded_replicas_.emplace_back(
                        std::move(processing_replicas),
                        now + master_->put_start_release_timeout_sec_);
                }
                tenant_state.processing_keys.erase(key);

                // If no COMPLETE replicas survive the preemption, this key
                // effectively does not exist — fall through to Case A.
                if (!metadata.HasReplica(&Replica::fn_is_completed)) {
                    master_->ErasePromotionTaskIfPresent(tenant_state, key);
                    master_->EraseMetadata(tenant_state, it,
                                           object_id.tenant_id);
                    it = tenant_state.metadata.end();
                }
            }
        }

        // --- Case A: key does not exist (or was erased above) ---
        // Allocate fresh buffers, identical to PutStart.
        if (it == tenant_state.metadata.end()) {
            VLOG(1) << "key=" << key << ", action=upsert_start_case_a";
            const size_t case_a_shard_idx =
                group_id.empty()
                    ? master_->getShardIndex(object_id.tenant_id,
                                             object_id.user_key)
                    : master_->getShardIndex(group_id);
            if (case_a_shard_idx != lookup_shard_idx) {
                case_a_retry_shard_idx = case_a_shard_idx;
                if (tenant_state.Empty()) {
                    shard->tenants.erase(object_id.tenant_id);
                }
            } else {
                return master_->AllocateAndInsertMetadata(
                    shard, client_id, key, slice_length, config, group_id,
                    object_id.tenant_id, now);
            }
        } else {
            // --- Step 2: key exists with COMPLETE replicas -> Case B or C ---
            auto& metadata = it->second;

            // Reject if any reader holds a reference (refcnt > 0).  Overwriting
            // a buffer that an RDMA read is streaming from would cause data
            // corruption. The client should retry after readers finish.
            if (metadata.HasReplica(&Replica::fn_is_busy)) {
                LOG(INFO) << "key=" << key << ", error=object_replica_busy";
                return tl::make_unexpected(ErrorCode::OBJECT_REPLICA_BUSY);
            }

            if (metadata.size == slice_length) {
                // --- Case B: same size — in-place update ---
                metadata.client_id = client_id;
                metadata.put_start_time = now;

                // Reconcile soft_pin state with the incoming config.
                {
                    SpinLocker locker(&metadata.lock);
                    if (config.with_soft_pin && !metadata.soft_pin_timeout) {
                        metadata.soft_pin_timeout.emplace();
                        MasterMetricManager::instance().inc_soft_pin_key_count(
                            1);
                    } else if (!config.with_soft_pin &&
                               metadata.soft_pin_timeout) {
                        metadata.soft_pin_timeout.reset();
                        MasterMetricManager::instance().dec_soft_pin_key_count(
                            1);
                    }
                }

                // Mark COMPLETE -> PROCESSING so readers won't see stale data
                // mid-transfer.  The key becomes unreadable until UpsertEnd.
                metadata.VisitReplicas(
                    &Replica::fn_is_completed,
                    [](Replica& replica) { replica.mark_processing(); });

                tenant_state.processing_keys.insert(key);

                // Return the existing descriptors — same buffer addresses as
                // before.
                std::vector<Replica::Descriptor> replica_list;
                const auto& all_replicas = metadata.GetAllReplicas();
                replica_list.reserve(all_replicas.size());
                for (const auto& replica : all_replicas) {
                    replica_list.emplace_back(replica.get_descriptor());
                }

                VLOG(1) << "key=" << key
                        << ", action=upsert_start_case_b_inplace";
                return replica_list;
            }

            // --- Case C: different size — discard old replicas and reallocate
            ReplicateConfig merged_config = config;
            merged_config.with_hard_pin =
                merged_config.with_hard_pin || metadata.IsHardPinned();
            merged_config.with_soft_pin =
                merged_config.with_soft_pin || metadata.IsSoftPinned();

            const std::string existing_group_id = metadata.group_id;
            auto old_replicas = metadata.PopReplicas();
            if (!old_replicas.empty()) {
                std::lock_guard lock(master_->discarded_replicas_mutex_);
                master_->discarded_replicas_.emplace_back(
                    std::move(old_replicas),
                    now + master_->put_start_release_timeout_sec_);
            }
            master_->EraseMetadata(tenant_state, it, object_id.tenant_id);

            VLOG(1) << "key=" << key
                    << ", action=upsert_start_case_c_reallocate";
            return master_->AllocateAndInsertMetadata(
                shard, client_id, key, slice_length, merged_config,
                existing_group_id, object_id.tenant_id, now);
        }
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    MasterService::MetadataShardAccessorRW shard(
        master_, case_a_retry_shard_idx.value());
    auto& retry_tenant_state = shard->tenants[object_id.tenant_id];
    const auto current_route =
        master_->GetGroupRoute(object_id.tenant_id, object_id.user_key);
    if (current_route.has_value() ||
        retry_tenant_state.metadata.contains(key)) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    return master_->AllocateAndInsertMetadata(shard, client_id, key,
                                              slice_length, config, group_id,
                                              object_id.tenant_id, now);
}

auto NativeMetadataBackend::UpsertEnd(const UUID& client_id,
                                      const std::string& key,
                                      const std::string& tenant_id,
                                      ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutEnd(client_id, key, tenant_id, replica_type);
}

auto NativeMetadataBackend::UpsertRevoke(const UUID& client_id,
                                         const std::string& key,
                                         const std::string& tenant_id,
                                         ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutRevoke(client_id, key, tenant_id, replica_type);
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
NativeMetadataBackend::BatchUpsertStart(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::string& tenant_id,
    const std::vector<uint64_t>& slice_lengths,
    const ReplicateConfig& config) {
    if (keys.size() != slice_lengths.size()) {
        LOG(ERROR) << "BatchUpsertStart: keys.size()=" << keys.size()
                   << " != slice_lengths.size()=" << slice_lengths.size();
        return std::vector<
            tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (config.group_ids.has_value() &&
        config.group_ids->size() != keys.size()) {
        LOG(ERROR) << "BatchUpsertStart: group_ids.size()="
                   << config.group_ids->size()
                   << " != keys.size()=" << keys.size();
        return std::vector<
            tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results;
    results.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key_config = config.ForSingleKey(i);
        results.emplace_back(UpsertStart(client_id, keys[i], tenant_id,
                                         slice_lengths[i], key_config));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
NativeMetadataBackend::BatchUpsertEnd(const UUID& client_id,
                                      const std::vector<std::string>& keys,
                                      const std::string& tenant_id) {
    return BatchPutEnd(client_id, keys, tenant_id);
}

std::vector<tl::expected<void, ErrorCode>>
NativeMetadataBackend::BatchUpsertRevoke(const UUID& client_id,
                                         const std::vector<std::string>& keys,
                                         const std::string& tenant_id) {
    return BatchPutRevoke(client_id, keys, tenant_id);
}

// ---------------------------------------------------------------------------
// Disk replica eviction
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::EvictDiskReplica(const UUID& client_id,
                                             const std::string& key,
                                             const std::string& tenant_id,
                                             ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    MasterService::MetadataAccessorRW accessor(master_, object_id);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", tenant_id=" << object_id.tenant_id
                  << ", info=object_not_found_for_eviction";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    if (replica_type == ReplicaType::DISK) {
        metadata.EraseReplicas(
            [](const Replica& replica) { return replica.is_disk_replica(); });
        MasterMetricManager::instance().dec_file_cache_nums();
    } else if (replica_type == ReplicaType::LOCAL_DISK) {
        metadata.EraseReplicas([&client_id](const Replica& replica) {
            return replica.is_local_disk_replica() &&
                   replica.get_descriptor()
                           .get_local_disk_descriptor()
                           .client_id == client_id;
        });
    } else {
        LOG(ERROR) << "key=" << key
                   << ", error=invalid_replica_type_for_eviction";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (!metadata.IsValid()) {
        accessor.Erase();
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>>
NativeMetadataBackend::BatchEvictDiskReplica(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::string& tenant_id, ReplicaType replica_type) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.push_back(
            EvictDiskReplica(client_id, key, tenant_id, replica_type));
    }
    return results;
}

// ---------------------------------------------------------------------------
// Replica clear
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    std::vector<std::string> cleared_keys;
    cleared_keys.reserve(object_keys.size());
    const bool clear_all_segments = segment_name.empty();

    for (const auto& key : object_keys) {
        if (key.empty()) {
            LOG(WARNING) << "BatchReplicaClear: empty key, skipping";
            continue;
        }
        // BatchReplicaClear is a default-tenant compatibility/admin helper.
        MasterService::MetadataAccessorRW accessor(
            master_, MasterService::MakeObjectIdentity(key, "default"));
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
            if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                LOG(WARNING) << "BatchReplicaClear: key=" << key
                             << " has incomplete replicas, skipping";
                continue;
            }

            metadata.VisitReplicas(
                &Replica::fn_is_completed, [](Replica& replica) {
                    if (replica.is_memory_replica()) {
                        MasterMetricManager::instance().dec_mem_cache_nums();
                    } else if (replica.is_disk_replica()) {
                        MasterMetricManager::instance().dec_file_cache_nums();
                    }
                });

            // Erase the entire metadata (all replicas will be deallocated)
            accessor.Erase();
            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: successfully cleared all replicas "
                       "for key="
                    << key << " for client_id=" << client_id;
        } else {
            // Clear only replicas on the specified segment_name
            bool has_replica_on_segment = false;
            const auto match_replica_on_segment =
                [&](const Replica& replica) -> bool {
                if (!replica.is_completed()) {
                    return false;
                }
                const auto segment_names = replica.get_segment_names();
                for (const auto& seg_name : segment_names) {
                    if (seg_name.has_value() &&
                        seg_name.value() == segment_name) {
                        return true;
                    }
                }
                return false;
            };

            metadata.VisitReplicas(
                match_replica_on_segment, [&](Replica& replica) {
                    has_replica_on_segment = true;
                    if (replica.is_memory_replica()) {
                        MasterMetricManager::instance().dec_mem_cache_nums();
                    } else if (replica.is_disk_replica()) {
                        MasterMetricManager::instance().dec_file_cache_nums();
                    }
                });

            if (!has_replica_on_segment) {
                LOG(WARNING)
                    << "BatchReplicaClear: key=" << key
                    << " has no replica on segment_name=" << segment_name
                    << ", skipping";
                continue;
            }

            metadata.EraseReplicas(match_replica_on_segment);

            // If no valid replicas remain, erase the entire metadata
            if (!metadata.IsValid()) {
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

// ---------------------------------------------------------------------------
// Copy operations
// ---------------------------------------------------------------------------

tl::expected<CopyStartResponse, ErrorCode>
NativeMetadataBackend::CopyStart(const UUID& client_id, const std::string& key,
                                 const std::string& tenant_id,
                                 const std::string& src_segment,
                                 const std::vector<std::string>& tgt_segments) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    {
        ScopedSegmentAccess segment_access =
            master_->segment_manager_.getSegmentAccess();
        for (const auto& tgt_segment : tgt_segments) {
            if (!segment_access.ExistsSegmentName(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_found";
                return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
            }
            if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_allocatable";
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
        }
    }
    MasterService::MetadataAccessorRW accessor(master_, object_id);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not valid";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    std::vector<Replica> replicas;
    replicas.reserve(tgt_segments.size());
    {
        ScopedAllocatorAccess allocator_access =
            master_->segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        for (auto& tgt_segment : tgt_segments) {
            if (metadata.GetReplicaBySegmentName(tgt_segment) != nullptr) {
                // Skip used segments.
                continue;
            }

            auto replica = master_->allocation_strategy_->AllocateFrom(
                allocator_manager, metadata.size, tgt_segment);
            if (!replica.has_value()) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", failed to allocate replica";
                return tl::make_unexpected(replica.error());
            }
            replicas.push_back(std::move(*replica));
        }
    }

    CopyStartResponse response;
    response.targets.reserve(replicas.size());
    std::vector<ReplicaID> replica_ids;
    replica_ids.reserve(replicas.size());

    response.source = source->get_descriptor();
    for (const auto& replica : replicas) {
        replica_ids.push_back(replica.id());
        response.targets.emplace_back(replica.get_descriptor());
    }

    // Create replication task for tracking.
    auto& tenant_state = accessor.GetTenantState();
    tenant_state.replication_tasks.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, std::chrono::system_clock::now(),
                              ReplicationTask::Type::COPY, source->id(),
                              std::move(replica_ids)));

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> NativeMetadataBackend::CopyEnd(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    MasterService::MetadataAccessorRW accessor(
        master_, MasterService::MakeObjectIdentity(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyEnd key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", copy source becomes invalid during data transfer";
        // Discard target replicas and clear the replication task.
        metadata.EraseReplicas([&task](const Replica& replica) {
            return std::find(task.replica_ids.begin(), task.replica_ids.end(),
                             replica.id()) != task.replica_ids.end();
        });
        accessor.EraseReplicationTask();
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // Decrement source reference count
    source->dec_refcnt();

    // Mark all replica_ids as complete
    bool all_complete = true;
    for (const auto& replica_id : task.replica_ids) {
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << replica_id
                << ", copy target becomes invalid during data transfer";
            all_complete = false;
        } else {
            replica->mark_complete();
        }
    }

    accessor.EraseReplicationTask();

    return all_complete ? tl::expected<void, ErrorCode>()
                        : tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
}

tl::expected<void, ErrorCode> NativeMetadataBackend::CopyRevoke(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    MasterService::MetadataAccessorRW accessor(
        master_, MasterService::MakeObjectIdentity(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyRevoke key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", copy source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids
    for (const auto& replica_id : task.replica_ids) {
        metadata.EraseReplicaByID(replica_id);
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

// ---------------------------------------------------------------------------
// Move operations
// ---------------------------------------------------------------------------

tl::expected<MoveStartResponse, ErrorCode>
NativeMetadataBackend::MoveStart(const UUID& client_id, const std::string& key,
                                 const std::string& tenant_id,
                                 const std::string& src_segment,
                                 const std::string& tgt_segment) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    if (src_segment == tgt_segment) {
        LOG(ERROR) << "key=" << key << ", move_tgt=" << tgt_segment
                   << " cannot be the same as move_src=" << src_segment;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    {
        ScopedSegmentAccess segment_access =
            master_->segment_manager_.getSegmentAccess();
        if (!segment_access.ExistsSegmentName(tgt_segment)) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", error=target_segment_not_found";
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", error=target_segment_not_allocatable";
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    MasterService::MetadataAccessorRW accessor(master_, object_id);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not completed";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    std::vector<Replica> replicas;
    if (metadata.GetReplicaBySegmentName(tgt_segment) == nullptr) {
        ScopedAllocatorAccess allocator_access =
            master_->segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        auto replica = master_->allocation_strategy_->AllocateFrom(
            allocator_manager, metadata.size, tgt_segment);
        if (!replica.has_value()) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", failed to allocate replica";
            return tl::make_unexpected(replica.error());
        }
        replicas.push_back(std::move(*replica));
    }

    MoveStartResponse response;
    std::vector<ReplicaID> replica_ids;

    response.source = source->get_descriptor();
    if (!replicas.empty()) {
        replica_ids.push_back(replicas[0].id());
        response.target = replicas[0].get_descriptor();
    } else {
        response.target = std::nullopt;
    }

    // Create replication task for tracking.
    auto& tenant_state = accessor.GetTenantState();
    tenant_state.replication_tasks.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, std::chrono::system_clock::now(),
                              ReplicationTask::Type::MOVE, source->id(),
                              std::move(replica_ids)));

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> NativeMetadataBackend::MoveEnd(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    MasterService::MetadataAccessorRW accessor(
        master_, MasterService::MakeObjectIdentity(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveEnd key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", move source becomes invalid during data transfer";
        // Discard target replica and clear the replication task.
        metadata.EraseReplicas([&task](const Replica& replica) {
            return std::find(task.replica_ids.begin(), task.replica_ids.end(),
                             replica.id()) != task.replica_ids.end();
        });
        accessor.EraseReplicationTask();
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // Decrement source reference count
    source->dec_refcnt();

    // If the move target has already existed on MoveStart, task.replica_ids
    // will be empty. Thus we need to check whether we have replica_ids to
    // process.
    if (!task.replica_ids.empty()) {
        auto replica_id = task.replica_ids[0];
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << replica_id
                << ", move target becomes invalid during data transfer";
            accessor.EraseReplicationTask();
            return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
        }

        // Mark replica as complete
        replica->mark_complete();
    }

    // Remove the source replica and release its space later.
    auto source_replica =
        metadata.PopReplicas([&source_id](const Replica& replica) {
            return replica.id() == source_id;
        });
    if (!source_replica.empty()) {
        std::lock_guard lock(master_->discarded_replicas_mutex_);
        master_->discarded_replicas_.emplace_back(
            std::move(source_replica),
            std::chrono::system_clock::now() +
                master_->put_start_release_timeout_sec_);
    }

    accessor.EraseReplicationTask();

    return {};
}

tl::expected<void, ErrorCode> NativeMetadataBackend::MoveRevoke(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    MasterService::MetadataAccessorRW accessor(
        master_, MasterService::MakeObjectIdentity(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveRevoke key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", move source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids (in MOVE operation, there should be at most one)
    for (const auto& replica_id : task.replica_ids) {
        metadata.EraseReplicaByID(replica_id);
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

// ---------------------------------------------------------------------------
// Object removal
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::Remove(const std::string& key,
                                   const std::string& tenant_id, bool force)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    MasterService::MetadataAccessorRW accessor(master_, object_id);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    if (!force && !metadata.IsLeaseExpired()) {
        VLOG(1) << "key=" << key << ", error=object_has_lease";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }

    /**
     * The reason the force operation here does not bypass the replica
     * check is that put operations (which could also be copy or move)
     * and remove operations might be happening concurrently, making it
     * extremely dangerous to perform a direct removal at this point.
     */
    if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
        LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key << ", error=object_has_replication_task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& tenant_state = accessor.GetTenantState();
    master_->ErasePromotionTaskIfPresent(tenant_state, key);
    accessor.Erase();
    return {};
}

auto NativeMetadataBackend::RemoveByRegex(const std::string& str,
                                          const std::string& tenant_id,
                                          bool force)
    -> tl::expected<long, ErrorCode> {
    long removed_count = 0;
    std::regex pattern;

    try {
        pattern = std::regex(str, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << str
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    for (size_t i = 0; i < MasterService::kNumShards; ++i) {
        MasterService::MetadataShardAccessorRW shard(master_, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        auto& tenant_state = tenant_it->second;

        for (auto it = tenant_state.metadata.begin();
             it != tenant_state.metadata.end();) {
            if (std::regex_search(it->first, pattern)) {
                if (!force && !it->second.IsLeaseExpired()) {
                    VLOG(1) << "key=" << it->first
                            << " matched by regex, but has lease. Skipping "
                            << "removal.";
                    ++it;
                    continue;
                }
                /**
                 * The reason the force operation here does not bypass the
                 * replica check is that put operations (which could also be
                 * copy or move) and remove operations might be happening
                 * concurrently, making it extremely dangerous to perform a
                 * direct removal at this point.
                 */
                if (!it->second.AllReplicas(&Replica::fn_is_completed)) {
                    LOG(WARNING) << "key=" << it->first
                                 << " matched by regex, but not all replicas "
                                    "are complete. Skipping removal.";
                    ++it;
                    continue;
                }
                if (tenant_state.replication_tasks.contains(it->first)) {
                    LOG(WARNING) << "key=" << it->first
                                 << ", matched by regex, but has replication "
                                    "task. Skipping removal.";
                    ++it;
                    continue;
                }

                VLOG(1) << "key=" << it->first
                        << " matched by regex. Removing.";
                master_->ErasePromotionTaskIfPresent(tenant_state, it->first);
                it = master_->EraseMetadata(tenant_state, it, normalized_tenant);
                removed_count++;
            } else {
                ++it;
            }
        }
        if (tenant_state.Empty()) {
            shard->tenants.erase(tenant_it);
        }
    }

    VLOG(1) << "action=remove_by_regex, pattern=" << str
            << ", removed_count=" << removed_count;
    return removed_count;
}

auto NativeMetadataBackend::RemoveAll(bool force)
    -> tl::expected<long, ErrorCode> {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    auto now = std::chrono::system_clock::now();

    for (size_t i = 0; i < MasterService::kNumShards; i++) {
        MasterService::MetadataShardAccessorRW shard(master_, i);
        for (auto tenant_it = shard->tenants.begin();
             tenant_it != shard->tenants.end();) {
            auto& tenant_state = tenant_it->second;
            auto it = tenant_state.metadata.begin();
            while (it != tenant_state.metadata.end()) {
                if ((force || it->second.IsLeaseExpired(now)) &&
                    it->second.AllReplicas(&Replica::fn_is_completed) &&
                    !tenant_state.replication_tasks.contains(it->first)) {
                    auto mem_rep_count = it->second.CountReplicas(
                        &Replica::fn_is_memory_replica);
                    total_freed_size += it->second.size * mem_rep_count;
                    master_->ErasePromotionTaskIfPresent(tenant_state,
                                                         it->first);
                    it = master_->EraseMetadata(tenant_state, it,
                                                tenant_it->first);
                    removed_count++;
                } else {
                    ++it;
                }
            }
            if (tenant_state.Empty()) {
                tenant_it = shard->tenants.erase(tenant_it);
            } else {
                ++tenant_it;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

auto NativeMetadataBackend::RemoveAll(const std::string& tenant_id, bool force)
    -> tl::expected<long, ErrorCode> {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    // Store the current time to avoid repeatedly
    // calling std::chrono::steady_clock::now()
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    auto now = std::chrono::system_clock::now();
    const auto normalized_tenant = NormalizeTenantId(tenant_id);

    for (size_t i = 0; i < MasterService::kNumShards; i++) {
        MasterService::MetadataShardAccessorRW shard(master_, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        auto& tenant_state = tenant_it->second;
        auto it = tenant_state.metadata.begin();
        while (it != tenant_state.metadata.end()) {
            if ((force || it->second.IsLeaseExpired(now)) &&
                it->second.AllReplicas(&Replica::fn_is_completed) &&
                !tenant_state.replication_tasks.contains(it->first)) {
                auto mem_rep_count =
                    it->second.CountReplicas(&Replica::fn_is_memory_replica);
                total_freed_size += it->second.size * mem_rep_count;
                master_->ErasePromotionTaskIfPresent(tenant_state, it->first);
                it = master_->EraseMetadata(tenant_state, it, normalized_tenant);
                removed_count++;
            } else {
                ++it;
            }
        }
        if (tenant_state.Empty()) {
            shard->tenants.erase(tenant_it);
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", tenant_id=" << normalized_tenant
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

auto NativeMetadataBackend::BatchRemove(const std::vector<std::string>& keys,
                                        const std::string& tenant_id,
                                        bool force)
    -> std::vector<tl::expected<void, ErrorCode>> {
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    const auto normalized_tenant = NormalizeTenantId(tenant_id);

    // Group keys by shard to reduce lock contention
    std::unordered_map<size_t,
                       std::vector<std::pair<size_t, const std::string*>>>
        keys_by_shard;
    keys_by_shard.reserve(
        std::min(keys.size(), static_cast<size_t>(MasterService::kNumShards)));

    for (size_t i = 0; i < keys.size(); ++i) {
        size_t shard_idx = master_->getMetadataShardIndex(normalized_tenant, keys[i]);
        keys_by_shard[shard_idx].emplace_back(i, &keys[i]);
    }

    std::shared_lock<std::shared_mutex> snapshot_lock(master_->snapshot_mutex_);

    auto alive_clients = master_->getAliveClientsSnapshot();

    // Process each shard once, acquiring lock per shard
    for (auto& [shard_idx, key_group] : keys_by_shard) {
        MasterService::MetadataShardAccessorRW shard(master_, shard_idx);
        auto now = std::chrono::system_clock::now();

        for (const auto& [original_idx, key_ptr] : key_group) {
            const std::string& key = *key_ptr;
            auto tenant_it = shard->tenants.find(normalized_tenant);
            if (tenant_it == shard->tenants.end()) {
                VLOG(1) << "key=" << key << ", error=object_not_found";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }
            auto& tenant_state = tenant_it->second;
            auto it = tenant_state.metadata.find(key);

            if (it == tenant_state.metadata.end()) {
                VLOG(1) << "key=" << key << ", error=object_not_found";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }

            // Clean up stale replica handles (consistent with single Remove)
            if (master_->CleanupStaleHandles(it->second, alive_clients)) {
                tenant_state.processing_keys.erase(key);
                tenant_state.replication_tasks.erase(key);
                tenant_state.offloading_tasks.erase(key);
                master_->ErasePromotionTaskIfPresent(tenant_state, key);
                master_->EraseMetadata(tenant_state, it, normalized_tenant);
                if (tenant_state.Empty()) {
                    shard->tenants.erase(tenant_it);
                }
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }
            if (!it->second.IsValid()) {
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }

            auto& metadata = it->second;

            if (!force && !metadata.IsLeaseExpired(now)) {
                VLOG(1) << "key=" << key << ", error=object_has_lease";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
                continue;
            }

            if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
                continue;
            }

            if (tenant_state.replication_tasks.contains(key)) {
                LOG(ERROR) << "key=" << key
                           << ", error=object_has_replication_task";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                continue;
            }

            // Remove object metadata
            master_->ErasePromotionTaskIfPresent(tenant_state, key);
            master_->EraseMetadata(tenant_state, it, normalized_tenant);
            if (tenant_state.Empty()) {
                shard->tenants.erase(tenant_it);
            }
            results[original_idx] = {};  // Success
        }
    }

    return results;
}

// ---------------------------------------------------------------------------
// Segment management
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::MountSegment(const Segment& segment,
                                         const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();

    // Tell the client monitor thread to start timing for this client. To
    // avoid the following undesired situations, this message must be sent
    // after locking the segment mutex and before the mounting operation
    // completes:
    // 1. Sending the message before the lock: the client expires and
    // unmouting invokes before this mounting are completed, which prevents
    // this segment being able to be unmounted forever;
    // 2. Sending the message after mounting the segment: After mounting
    // this segment, when trying to push id to the queue, the queue is
    // already full. However, at this point, the message must be sent,
    // otherwise this client cannot be monitored and expired.
    {
        MasterService::PodUUID pod_client_id;
        pod_client_id.first = client_id.first;
        pod_client_id.second = client_id.second;
        if (!master_->client_ping_queue_.push(pod_client_id)) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=client_ping_queue_full";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    LOG(INFO) << "client_id=" << client_id
              << ", action=mount_segment, segment_name=" << segment.name;

    auto err = segment_access.MountSegment(segment, client_id);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

auto NativeMetadataBackend::MountNoFSegment(const NoFSegment& segment,
                                            const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id << ", segment_name=" << segment.name
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    ScopedNoFSegmentAccess nof_segment_access =
        master_->nof_segment_manager_.getNoFSegmentAccess();

    LOG(INFO) << "NoF segment mount: "
              << "client_id=" << client_id
              << ", action=mount_segment, segment_name=" << segment.name;

    auto err = nof_segment_access.MountSegment(segment, client_id);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
#endif
}

auto NativeMetadataBackend::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    std::unique_lock<std::shared_mutex> lock(master_->client_mutex_);
    if (master_->ok_client_.contains(client_id)) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", warn=client_already_remounted";
        // Return OK because this is an idempotent operation
        return {};
    }

    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();

    // Tell the client monitor thread to start timing for this client. To
    // avoid the following undesired situations, this message must be sent
    // after locking the segment mutex or client mutex and before the remounting
    // operation completes:
    // 1. Sending the message before the lock: the client expires and
    // unmouting invokes before this remounting are completed, which prevents
    // this segment being able to be unmounted forever;
    // 2. Sending the message after remounting the segments: After remounting
    // these segments, when trying to push id to the queue, the queue is
    // already full. However, at this point, the message must be sent,
    // otherwise this client cannot be monitored and expired.
    MasterService::PodUUID pod_client_id;
    pod_client_id.first = client_id.first;
    pod_client_id.second = client_id.second;
    if (!master_->client_ping_queue_.push(pod_client_id)) {
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    ErrorCode err = segment_access.ReMountSegment(segments, client_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    // Change the client status to OK
    master_->ok_client_.insert(client_id);
    MasterMetricManager::instance().inc_active_clients();

    return {};
}

auto NativeMetadataBackend::ReMountNoFSegment(
    const std::vector<NoFSegment>& segments, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id
               << ", segments_count=" << segments.size()
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    ScopedNoFSegmentAccess nof_segment_access =
        master_->nof_segment_manager_.getNoFSegmentAccess();
    ErrorCode err = nof_segment_access.ReMountSegment(segments, client_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
#endif
}

auto NativeMetadataBackend::UnmountSegment(const UUID& segment_id,
                                           const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    size_t metrics_dec_capacity = 0;  // to update the metrics

    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedSegmentAccess segment_access =
            master_->segment_manager_.getSegmentAccess();
        ErrorCode err = segment_access.PrepareUnmountSegment(
            segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            // Return OK because this is an idempotent operation
            return {};
        }
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }  // Release the segment mutex before long-running step 2 and avoid
       // deadlocks

    // 2. Remove the metadata of the related objects
    master_->ClearInvalidHandles();

    // 3. Commit the unmount operation
    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();
    auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                   metrics_dec_capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

auto NativeMetadataBackend::GracefulUnmountSegment(const UUID& segment_id,
                                                   const UUID& client_id,
                                                   uint64_t grace_period_ms)
    -> tl::expected<void, ErrorCode> {
    std::unique_lock<std::shared_mutex> lock(master_->snapshot_mutex_);
    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();

    // Verify ownership: the segment must belong to the calling client
    std::vector<Segment> client_segments;
    auto err = segment_access.GetClientSegments(client_id, client_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    bool owned = false;
    for (auto& seg : client_segments) {
        if (seg.id == segment_id) {
            owned = true;
            break;
        }
    }
    if (!owned) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    err = segment_access.PrepareGracefulUnmountSegment(segment_id);
    if (err == ErrorCode::SEGMENT_NOT_FOUND) {
        return {};
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    auto expire_time = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(grace_period_ms);
    master_->graceful_unmount_scheduler_.Schedule(segment_id, client_id,
                                                  expire_time);
    return {};
}

auto NativeMetadataBackend::UnmountNoFSegment(const UUID& segment_id,
                                              const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id << ", segment_id=" << segment_id
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    size_t metrics_dec_capacity = 0;  // to update the metrics

    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedNoFSegmentAccess segment_access =
            master_->nof_segment_manager_.getNoFSegmentAccess();
        ErrorCode err = segment_access.PrepareUnmountSegment(
            segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            // Return OK because this is an idempotent operation
            return {};
        }
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }  // Release the segment mutex before long-running step 2 and avoid
       // deadlocks

    // 2. Remove the metadata of the related objects
    master_->ClearInvalidHandles();

    // 3. Commit the unmount operation
    ScopedNoFSegmentAccess segment_access =
        master_->nof_segment_manager_.getNoFSegmentAccess();
    auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                   metrics_dec_capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    {
        std::lock_guard<std::mutex> lock(master_->nof_heartbeat_mutex_);
        master_->nof_heartbeat_states_.erase(segment_id);
    }
    return {};
#endif
}

auto NativeMetadataBackend::MountLocalDiskSegment(const UUID& client_id,
                                                  bool enable_offloading)
    -> tl::expected<void, ErrorCode> {
    if (!master_->enable_offload_) {
        LOG(ERROR) << "	The offload functionality is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOAD);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();

    auto err =
        segment_access.MountLocalDiskSegment(client_id, enable_offloading);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    // Notify the client monitor thread to start tracking this client's TTL.
    // Without this, a client that only mounts a LOCAL_DISK segment (and
    // doesn't ping) would be considered expired by ClientMonitorFunc, which
    // would then clear all its LOCAL_DISK replicas.
    MasterService::PodUUID pod_client_id;
    pod_client_id.first = client_id.first;
    pod_client_id.second = client_id.second;
    if (!master_->client_ping_queue_.push(pod_client_id)) {
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

auto NativeMetadataBackend::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();
    std::vector<std::string> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return all_segments;
}

auto NativeMetadataBackend::GetAllNoFSegments()
    -> tl::expected<std::vector<NoFSegment>, ErrorCode> {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    master_->nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);

    std::vector<NoFSegment> result;
    for (const auto& segment : mounted_segments) {
        result.push_back(segment.segment);
    }

    return result;
}

auto NativeMetadataBackend::GetNoFSegmentsByName(
    const std::string& segment_name)
    -> tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode> {
    return master_->nof_segment_manager_.GetSegmentsByName(segment_name);
}

auto NativeMetadataBackend::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();
    size_t used, capacity;
    auto err = segment_access.QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

auto NativeMetadataBackend::GetSegmentsDetail()
    -> tl::expected<std::vector<SegmentDetailInfo>, ErrorCode> {
    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();

    // Get full info of all segments (including Segment and client_id)
    std::vector<std::pair<Segment, UUID>> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::vector<SegmentDetailInfo> result;
    result.reserve(all_segments.size());

    for (const auto& [segment, client_id] : all_segments) {
        SegmentDetailInfo info;
        info.segment_name = segment.name;
        info.segment_id = segment.id;
        info.client_id = client_id;
        info.base_address = segment.base;
        info.size_bytes = segment.size;
        info.te_endpoint = segment.te_endpoint;
        info.protocol = segment.protocol;

        // Query segment status
        segment_access.GetSegmentStatusByName(segment.name, info.status);

        // Query allocator used/capacity
        size_t used = 0, capacity = 0;
        segment_access.QuerySegments(segment.name, used, capacity);
        info.allocator_used_bytes = used;
        info.allocator_capacity_bytes = capacity;

        result.push_back(std::move(info));
    }

    return result;
}

tl::expected<SegmentStatus, ErrorCode>
NativeMetadataBackend::QuerySegmentStatus(const std::string& segment_name) {
    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();
    SegmentStatus status = SegmentStatus::UNDEFINED;
    auto err = segment_access.GetSegmentStatusByName(segment_name, status);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return status;
}

tl::expected<SegmentStatus, ErrorCode>
NativeMetadataBackend::QuerySegmentStatusById(const UUID& segment_id) {
    ScopedSegmentAccess segment_access =
        master_->segment_manager_.getSegmentAccess();
    SegmentStatus status = SegmentStatus::UNDEFINED;
    auto err = segment_access.GetSegmentStatusById(segment_id, status);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return status;
}

// ---------------------------------------------------------------------------
// Client identity and IP lookup
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = master_->segment_manager_.getSegmentAccess();
    std::vector<Segment> segments;
    ErrorCode err = segment_access.GetClientSegments(client_id, segments);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            VLOG(1) << "QueryIp: client_id=" << client_id
                    << " not found or has no segments";
            return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
        }

        LOG(ERROR) << "QueryIp: failed to get segments for client_id="
                   << client_id << ", error=" << toString(err);

        return tl::make_unexpected(err);
    }

    std::unordered_set<std::string> unique_ips;
    unique_ips.reserve(segments.size());
    for (const auto& segment : segments) {
        if (!segment.te_endpoint.empty()) {
            unique_ips.emplace(MasterService::getHostNameWithoutPort(segment.te_endpoint));
        }
    }

    if (unique_ips.empty()) {
        LOG(WARNING) << "QueryIp: client_id=" << client_id
                     << " has no valid IP addresses";
        return {};
    }
    std::vector<std::string> result(unique_ips.begin(), unique_ips.end());
    return result;
}

auto NativeMetadataBackend::BatchQueryIp(const std::vector<UUID>& client_ids)
    -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, UUIDHash>,
        ErrorCode> {
    std::unordered_map<UUID, std::vector<std::string>, UUIDHash> results;
    results.reserve(client_ids.size());
    for (const auto& client_id : client_ids) {
        auto ip_result = QueryIp(client_id);
        if (ip_result.has_value()) {
            results.emplace(client_id, std::move(ip_result.value()));
        }
    }
    return results;
}

// ---------------------------------------------------------------------------
// Client heartbeat
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::Ping(const UUID& client_id)
    -> tl::expected<PingResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> lock(master_->client_mutex_);
    ClientStatus client_status;
    auto it = master_->ok_client_.find(client_id);
    if (it != master_->ok_client_.end()) {
        client_status = ClientStatus::OK;
    } else {
        client_status = ClientStatus::NEED_REMOUNT;
    }
    MasterService::PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!master_->client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return PingResponse(master_->view_version_, client_status);
}

// ---------------------------------------------------------------------------
// Configuration queries
// ---------------------------------------------------------------------------

tl::expected<std::string, ErrorCode> NativeMetadataBackend::GetFsdir() const {
    if (master_->root_fs_dir_.empty() || master_->cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return std::string();
    }
    return master_->root_fs_dir_ + "/" + master_->cluster_id_;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
NativeMetadataBackend::GetStorageConfig() const {
    if (master_->root_fs_dir_.empty() || master_->cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return GetStorageConfigResponse("", master_->enable_disk_eviction_,
                                        master_->quota_bytes_);
    }
    std::string fsdir = master_->root_fs_dir_ + "/" + master_->cluster_id_;
    return GetStorageConfigResponse(fsdir, master_->enable_disk_eviction_, master_->quota_bytes_);
}

// ---------------------------------------------------------------------------
// Offload workflow
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::OffloadObjectHeartbeat(const UUID& client_id,
                                                   bool enable_offloading)
    -> tl::expected<std::vector<OffloadTaskItem>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        master_->segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        LOG(ERROR) << "Local disk segment not found with client id = "
                   << client_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    std::unordered_map<std::string, OffloadTaskItem> offloading_objects_copy;
    {
        MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
        local_disk_segment_it->second->enable_offloading = enable_offloading;
        if (enable_offloading) {
            std::vector<OffloadTaskItem> result;
            result.reserve(
                local_disk_segment_it->second->offloading_objects.size());
            for (const auto& [_, task] :
                 local_disk_segment_it->second->offloading_objects) {
                result.push_back(task);
            }
            local_disk_segment_it->second->offloading_objects.clear();
            return result;
        }
        // Offloading is disabled: clear the pending queue to prevent
        // unbounded growth that would trigger KEYS_ULTRA_LIMIT in
        // PushOffloadingQueue. We must also clean up corresponding
        // offloading_tasks and decrement source replica refcounts to avoid
        // resource leaks and blocked writes (OBJECT_HAS_REPLICATION_TASK).
        // Copy keys out before releasing the mutex to avoid lock order
        // violation: the lock order is Shard Lock -> offloading_mutex_, so we
        // must release offloading_mutex_ before taking shard locks via
        // MasterService::MetadataAccessorRW.
        offloading_objects_copy =
            std::move(local_disk_segment_it->second->offloading_objects);
    }

    for (auto& [_, task] : offloading_objects_copy) {
        const auto object_id = MasterService::MakeObjectIdentity(task.key, task.tenant_id);
        MasterService::MetadataAccessorRW accessor(master_, object_id);
        if (accessor.Exists()) {
            auto& tenant_state = accessor.GetTenantState();
            auto task_it =
                tenant_state.offloading_tasks.find(object_id.user_key);
            if (task_it != tenant_state.offloading_tasks.end()) {
                auto source =
                    accessor.Get().GetReplicaByID(task_it->second.source_id);
                if (source) {
                    source->dec_refcnt();
                }
                tenant_state.offloading_tasks.erase(task_it);
            }
        }
    }
    return {};
}

auto NativeMetadataBackend::ReportSsdCapacity(const UUID& client_id,
                                              int64_t ssd_total_capacity_bytes)
    -> tl::expected<void, ErrorCode> {
    if (ssd_total_capacity_bytes < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        master_->segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        LOG(ERROR) << "Local disk segment not found with client id = "
                   << client_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    int64_t old_capacity =
        local_disk_segment_it->second->ssd_total_capacity_bytes;
    if (ssd_total_capacity_bytes != old_capacity) {
        local_disk_segment_it->second->ssd_total_capacity_bytes =
            ssd_total_capacity_bytes;
        if (old_capacity > 0) {
            MasterMetricManager::instance().dec_total_file_capacity(
                old_capacity);
        }
        if (ssd_total_capacity_bytes > 0) {
            MasterMetricManager::instance().inc_total_file_capacity(
                ssd_total_capacity_bytes);
        }
    }
    return {};
}

auto NativeMetadataBackend::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas)
    -> tl::expected<void, ErrorCode> {
    if (tasks.size() != metadatas.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (size_t i = 0; i < tasks.size(); ++i) {
        const auto& task = tasks[i];
        const auto& metadata = metadatas[i];
        const auto object_id = MasterService::MakeObjectIdentity(task.key, task.tenant_id);

        // Release refcnt and clear offloading task.
        {
            MasterService::MetadataAccessorRW accessor(master_, object_id);
            if (accessor.Exists()) {
                auto& obj_metadata = accessor.Get();
                auto& tenant_state = accessor.GetTenantState();
                auto task_it =
                    tenant_state.offloading_tasks.find(object_id.user_key);
                if (task_it != tenant_state.offloading_tasks.end()) {
                    auto source =
                        obj_metadata.GetReplicaByID(task_it->second.source_id);
                    if (source != nullptr) {
                        source->dec_refcnt();
                    }
                    tenant_state.offloading_tasks.erase(task_it);
                }
            }
        }

        // Add LOCAL_DISK replica.
        Replica replica(client_id, metadata.data_size,
                        metadata.transport_endpoint, ReplicaStatus::COMPLETE);
        auto res = master_->AddReplica(client_id, object_id.user_key,
                              object_id.tenant_id, replica);
        if (!res && res.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to add replica: error=" << res.error()
                       << ", client_id=" << client_id
                       << ", tenant_id=" << object_id.tenant_id
                       << ", key=" << object_id.user_key;
            return tl::make_unexpected(res.error());
        }
    }
    return {};
}

// ---------------------------------------------------------------------------
// Promotion workflow
// ---------------------------------------------------------------------------

auto NativeMetadataBackend::PromotionObjectHeartbeat(const UUID& client_id)
    -> tl::expected<std::vector<PromotionTaskItem>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        master_->segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    // Return at most promotion_max_per_heartbeat_ tasks. Each task does
    // a synchronous SSD read + RDMA write on the client side; allowing
    // more than one per heartbeat risks blocking past the client-
    // liveness window and the master marking the client dead. The rest
    // stay queued in promotion_objects for subsequent heartbeats. The
    // cap must live here (server side) rather than on the client so
    // leftover work isn't silently dropped.
    auto& src = local_disk_segment_it->second->promotion_objects;
    std::vector<PromotionTaskItem> result;
    while (result.size() < master_->promotion_max_per_heartbeat_ && !src.empty()) {
        auto node = src.extract(src.begin());
        result.push_back(std::move(node.mapped()));
    }
    return result;
}

auto NativeMetadataBackend::PromotionAllocStart(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id, uint64_t size,
    const std::vector<std::string>& preferred_segments)
    -> tl::expected<PromotionAllocStartResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    MasterService::MetadataAccessorRW accessor(master_, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();

    // Verify the in-flight task still exists before allocating. The
    // reaper can sweep it between the holder's heartbeat and this
    // AllocStart call (a hung client, GC pause, or HA failover can
    // stall AllocStart past put_start_release_timeout_sec_). If we
    // allocated and AddReplicas'd anyway, the staged PROCESSING MEMORY
    // replica would have no PromotionTask pointing at it: the generic
    // PROCESSING reaper iterates tenant_state.processing_keys (never
    // populated by promotion) and the promotion-task reaper would have
    // nothing left to iterate, leaking the buffer until the object is
    // removed or evicted. The shard mutex is held for the rest of this
    // function, so the iterator stays valid across the allocation step.
    auto& tenant_state = accessor.GetTenantState();
    auto task_it = tenant_state.promotion_tasks.find(object_id.user_key);
    if (task_it == tenant_state.promotion_tasks.end()) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (task_it->second.holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Defensive size check: must match the source LOCAL_DISK
    // descriptor's object_size captured at admission. A mismatch would
    // let a buggy caller request a wrong-sized allocation — smaller
    // risks RDMA overflow, larger wastes DRAM pinned until reaper TTL.
    if (task_it->second.object_size != size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Allocate a single MEMORY replica via the existing strategy, biased to
    // the holder's mem segment when possible.
    ReplicateConfig config;
    config.replica_num = 1;
    if (!preferred_segments.empty()) {
        config.preferred_segments = preferred_segments;
    }

    std::vector<Replica> staged_replicas;
    {
        ScopedAllocatorAccess allocator_access =
            master_->segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();
        auto allocation_result = master_->allocation_strategy_->Allocate(
            allocator_manager, size, config.replica_num, preferred_segments);
        if (!allocation_result) {
            return tl::make_unexpected(allocation_result.error());
        }
        staged_replicas = std::move(allocation_result.value());
    }
    if (staged_replicas.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Append the new PROCESSING MEMORY replica to the existing object's
    // metadata. Visible only after NotifyPromotionSuccess flips it COMPLETE.
    Replica::Descriptor desc = staged_replicas[0].get_descriptor();
    const ReplicaID new_id = staged_replicas[0].id();
    std::vector<Replica> to_add;
    to_add.push_back(std::move(staged_replicas[0]));
    metadata.AddReplicas(std::move(to_add));

    // Record the new replica's ID on the in-flight PromotionTask so
    // NotifyPromotionSuccess knows exactly which replica to commit. A
    // concurrent Put on this key may stage other PROCESSING MEMORY
    // replicas; using alloc_id avoids the "first PROCESSING memory"
    // ambiguity.
    //
    // Also reset start_time so the reaper TTL covers the active-
    // transfer phase (AllocStart -> SSD read -> RDMA write -> Notify)
    // measured from when a master-allocated buffer becomes vulnerable,
    // rather than being consumed by queue-waiting. Without the reset,
    // a backlogged task could enter active transfer with little TTL
    // remaining and the reaper could free the staged replica via
    // EraseReplicaByID mid-RDMA-write. The queue-waiting phase
    // (alloc_id == 0) is bounded by its own original start_time window
    // during which the reaper's EraseReplicaByID branch is a no-op.
    task_it->second.alloc_id = new_id;
    task_it->second.start_time = std::chrono::system_clock::now();
    return PromotionAllocStartResponse{std::move(desc)};
}

auto NativeMetadataBackend::NotifyPromotionSuccess(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    MasterService::MetadataAccessorRW accessor(master_, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();
    auto& tenant_state = accessor.GetTenantState();

    // Look up the in-flight task to find the exact replica we staged. A
    // concurrent Put on this key may have created other PROCESSING MEMORY
    // replicas, so we must not just "mark first PROCESSING memory
    // complete" — that would risk committing someone else's half-written
    // replica.
    auto task_it = tenant_state.promotion_tasks.find(object_id.user_key);
    if (task_it == tenant_state.promotion_tasks.end() ||
        task_it->second.alloc_id == 0) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (task_it->second.holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    bool committed = false;
    Replica* staged = metadata.GetReplicaByID(task_it->second.alloc_id);
    if (staged != nullptr && staged->is_memory_replica() &&
        staged->is_processing()) {
        staged->mark_complete();
        committed = true;
    }

    // Drop the source LOCAL_DISK replica's refcnt and erase the task.
    auto* source = metadata.GetReplicaByID(task_it->second.source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    const uint64_t completed_bytes = task_it->second.object_size;
    tenant_state.promotion_tasks.erase(task_it);
    master_->promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
    MasterMetricManager::instance().dec_promotion_in_flight();
    if (committed) {
        MasterMetricManager::instance().inc_promotion_completed();
        MasterMetricManager::instance().inc_promotion_completed_bytes(
            static_cast<int64_t>(completed_bytes));
    } else {
        MasterMetricManager::instance().inc_promotion_cancelled();
    }

    // Erase the per-client promotion_objects entry (best-effort; the
    // heartbeat may have already drained it).
    {
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            master_->segment_manager_.getLocalDiskSegmentAccess();
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto it = client_local_disk_segment.find(client_id);
        if (it != client_local_disk_segment.end()) {
            MutexLocker locker(&it->second->offloading_mutex_);
            it->second->promotion_objects.erase(MakeTenantScopedStorageKey(
                object_id.tenant_id, object_id.user_key));
        }
    }

    if (!committed) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    return {};
}

auto NativeMetadataBackend::NotifyPromotionFailure(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    MasterService::MetadataAccessorRW accessor(master_, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();
    auto& tenant_state = accessor.GetTenantState();

    auto task_it = tenant_state.promotion_tasks.find(object_id.user_key);
    if (task_it == tenant_state.promotion_tasks.end()) {
        // No task to release. Either the reaper already swept it, or the
        // client never had a task here. Return OK to keep this RPC
        // idempotent — repeated failure notifications on the same key
        // should be safe.
        return {};
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (task_it->second.holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Mirror the reaper's expiry path; see DiscardExpiredProcessingReplicas
    // Part 4 for the full rationale on each step.
    auto* source = metadata.GetReplicaByID(task_it->second.source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    if (task_it->second.alloc_id != 0) {
        metadata.EraseReplicaByID(task_it->second.alloc_id);
    }
    tenant_state.promotion_tasks.erase(task_it);
    master_->promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
    MasterMetricManager::instance().dec_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_failed();

    // Clear the holder's per-client promotion_objects entry. Same
    // best-effort cleanup pattern as NotifyPromotionSuccess — the
    // heartbeat may have already drained it.
    {
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            master_->segment_manager_.getLocalDiskSegmentAccess();
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto it = client_local_disk_segment.find(client_id);
        if (it != client_local_disk_segment.end()) {
            MutexLocker locker(&it->second->offloading_mutex_);
            it->second->promotion_objects.erase(MakeTenantScopedStorageKey(
                object_id.tenant_id, object_id.user_key));
        }
    }

    return {};
}

// ---------------------------------------------------------------------------
// Replication tasks
// ---------------------------------------------------------------------------

tl::expected<UUID, ErrorCode> NativeMetadataBackend::CreateCopyTask(
    const std::string& key, const std::string& tenant_id,
    const std::vector<std::string>& targets) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    if (targets.empty()) {
        LOG(ERROR) << "key=" << key << ", error=empty_targets";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    MasterService::MetadataAccessorRO accessor(master_, object_id);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    ScopedSegmentAccess segment_accessor = master_->segment_manager_.getSegmentAccess();
    for (const auto& target : targets) {
        if (!segment_accessor.ExistsSegmentName(target)) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_mounted";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!segment_accessor.IsSegmentAllocatable(target)) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_allocatable";
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    const auto& metadata = accessor.Get();
    const auto& segment_names = metadata.GetReplicaSegmentNames();
    if (segment_names.empty()) {
        LOG(ERROR) << "key=" << key << ", error=no_valid_source_replicas";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Randomly pick a segment from the source replicas
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<size_t> dis(0, segment_names.size() - 1);
    std::string selected_source_segment = segment_names[dis(gen)];
    UUID select_client;
    ErrorCode error = segment_accessor.GetClientIdBySegmentName(
        selected_source_segment, select_client);
    if (error != ErrorCode::OK) {
        LOG(ERROR) << "key=" << key
                   << ", segment_name=" << selected_source_segment
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return master_->task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_COPY>(
            select_client, {.tenant_id = object_id.tenant_id,
                            .key = object_id.user_key,
                            .source = selected_source_segment,
                            .targets = targets});
}

tl::expected<UUID, ErrorCode> NativeMetadataBackend::CreateMoveTask(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::string& target) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto object_id = MasterService::MakeObjectIdentity(key, tenant_id);
    MasterService::MetadataAccessorRO accessor(master_, object_id);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (source == target) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", target_segment=" << target
                   << ", error=source_target_segments_are_same";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ScopedSegmentAccess segment_accessor = master_->segment_manager_.getSegmentAccess();
    if (!segment_accessor.ExistsSegmentName(target)) {
        LOG(ERROR) << "key=" << key << ", target_segment=" << target
                   << ", error=target_segment_not_mounted";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!segment_accessor.IsSegmentAllocatable(target)) {
        LOG(ERROR) << "key=" << key << ", target_segment=" << target
                   << ", error=target_segment_not_allocatable";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    const auto& metadata = accessor.Get();
    const auto& segment_names = metadata.GetReplicaSegmentNames();
    if (std::find(segment_names.begin(), segment_names.end(), source) ==
        segment_names.end()) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", error=source_segment_not_found";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    UUID select_client;
    ErrorCode error =
        segment_accessor.GetClientIdBySegmentName(source, select_client);

    if (error != ErrorCode::OK) {
        LOG(ERROR) << "key=" << key << ", segment_name=" << source
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return master_->task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_MOVE>(
            select_client, {.tenant_id = object_id.tenant_id,
                            .key = object_id.user_key,
                            .source = source,
                            .target = target});
}

tl::expected<UUID, ErrorCode> NativeMetadataBackend::CreateDrainJob(
    const CreateDrainJobRequest& request) {
    std::vector<std::string> draining_segments;
    {
        ScopedSegmentAccess segment_access =
            master_->segment_manager_.getSegmentAccess();
        auto valid = MasterService::ValidateDrainRequestLocked(segment_access, request);
        if (!valid.has_value()) {
            return tl::make_unexpected(valid.error());
        }

        draining_segments.reserve(request.segments.size());
        for (const auto& segment_name : request.segments) {
            auto err = segment_access.SetSegmentStatusByName(
                segment_name, SegmentStatus::DRAINING);
            if (err != ErrorCode::OK) {
                for (const auto& updated_segment : draining_segments) {
                    (void)segment_access.SetSegmentStatusByName(
                        updated_segment, SegmentStatus::OK);
                }
                return tl::make_unexpected(err);
            }
            draining_segments.push_back(segment_name);
        }
    }

    auto job = std::make_shared<MasterService::DrainJob>();
    job->id = generate_uuid();
    job->request = request;
    job->created_at = std::chrono::system_clock::now();
    job->last_updated_at = job->created_at;
    job->status = MasterService::JobStatus::CREATED;
    job->message = "Drain job created";

    {
        std::lock_guard<std::mutex> lock(master_->job_mutex_);
        master_->drain_jobs_.emplace(job->id, job);
    }

    return job->id;
}

tl::expected<QueryJobResponse, ErrorCode>
NativeMetadataBackend::QueryDrainJob(const UUID& job_id) {
    std::shared_ptr<MasterService::DrainJob> job;
    {
        std::lock_guard<std::mutex> lock(master_->job_mutex_);
        auto it = master_->drain_jobs_.find(job_id);
        if (it == master_->drain_jobs_.end()) {
            return tl::make_unexpected(ErrorCode::JOB_NOT_FOUND);
        }
        job = it->second;
    }

    std::lock_guard<std::mutex> job_lock(job->mutex);
    QueryJobResponse response;
    response.id = job->id;
    response.type = job->type;
    response.status = job->status;
    response.created_at_ms_epoch = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            job->created_at.time_since_epoch())
            .count());
    response.last_updated_at_ms_epoch = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            job->last_updated_at.time_since_epoch())
            .count());
    response.segments = job->request.segments;
    response.succeeded_units = job->succeeded_units;
    response.failed_units = job->failed_units;
    response.blocked_units = job->blocked_units;
    response.active_units = static_cast<uint64_t>(job->active_tasks.size());
    response.migrated_bytes = job->migrated_bytes;
    response.message = job->message;
    return response;
}

tl::expected<void, ErrorCode> NativeMetadataBackend::CancelDrainJob(
    const UUID& job_id) {
    std::shared_ptr<MasterService::DrainJob> job;
    {
        std::lock_guard<std::mutex> lock(master_->job_mutex_);
        auto it = master_->drain_jobs_.find(job_id);
        if (it == master_->drain_jobs_.end()) {
            return tl::make_unexpected(ErrorCode::JOB_NOT_FOUND);
        }
        job = it->second;
    }

    std::vector<std::string> segments_to_restore;
    {
        std::lock_guard<std::mutex> job_lock(job->mutex);
        if (job->status == MasterService::JobStatus::SUCCEEDED ||
            job->status == MasterService::JobStatus::FAILED ||
            job->status == MasterService::JobStatus::CANCELED || !job->active_tasks.empty()) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }

        job->status = MasterService::JobStatus::CANCELED;
        job->last_updated_at = std::chrono::system_clock::now();
        job->message = "Drain job canceled";
        segments_to_restore = job->request.segments;
    }

    ScopedSegmentAccess segment_access = master_->segment_manager_.getSegmentAccess();
    for (const auto& segment_name : segments_to_restore) {
        SegmentStatus status = SegmentStatus::UNDEFINED;
        if (segment_access.GetSegmentStatusByName(segment_name, status) ==
                ErrorCode::OK &&
            status != SegmentStatus::UNMOUNTING) {
            (void)segment_access.SetSegmentStatusByName(segment_name,
                                                        SegmentStatus::OK);
        }
    }
    return {};
}

tl::expected<QueryTaskResponse, ErrorCode>
NativeMetadataBackend::QueryTask(const UUID& task_id) {
    const auto& task_option =
        master_->task_manager_.get_read_access().find_task_by_id(task_id);
    if (!task_option.has_value()) {
        LOG(ERROR) << "task_id=" << task_id << ", error=task_not_found";
        return tl::make_unexpected(ErrorCode::TASK_NOT_FOUND);
    }
    return QueryTaskResponse(task_option.value());
}

tl::expected<std::vector<TaskAssignment>, ErrorCode>
NativeMetadataBackend::FetchTasks(const UUID& client_id, size_t batch_size) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    const auto& tasks =
        master_->task_manager_.get_write_access().pop_tasks(client_id, batch_size);
    std::vector<TaskAssignment> assignments;
    for (const auto& task : tasks) {
        assignments.emplace_back(task);
    }
    return assignments;
}

tl::expected<void, ErrorCode> NativeMetadataBackend::MarkTaskToComplete(
    const UUID& client_id, const TaskCompleteRequest& request) {
    std::shared_lock<std::shared_mutex> shared_lock(master_->snapshot_mutex_);
    auto write_access = master_->task_manager_.get_write_access();
    ErrorCode err = write_access.complete_task(client_id, request.id,
                                               request.status, request.message);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "task_id=" << request.id
                   << ", error=complete_task_failed";
        return tl::make_unexpected(err);
    }
    return {};
}

}  // namespace mooncake
