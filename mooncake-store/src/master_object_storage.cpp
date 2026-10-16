#include "master_service.h"

#include <chrono>
#include <stdexcept>

#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#include "storage/distributed/object_storage_backend.h"

namespace mooncake {
namespace {
uint64_t ObjectNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
}  // namespace

void MasterService::InitObjectStorage(const MasterServiceConfig& config) {
    if (!config.object_storage.enabled) return;
    if (!config.object_storage.Validate() ||
        config.ha_backend_connstring.empty())
        throw std::invalid_argument(
            "Object storage requires valid configuration and "
            "ha_backend_connstring for its durable catalog");
#ifndef STORE_USE_ETCD
    throw std::invalid_argument(
        "Object storage catalog requires STORE_USE_ETCD");
#else
    auto error =
        EtcdHelper::ConnectToEtcdStoreClient(config.ha_backend_connstring);
    if (error != ErrorCode::OK)
        throw std::runtime_error("Cannot connect object storage catalog");
    auto adapter = CreateObjectStorageAdapter(config.object_storage);
    if (!adapter)
        throw std::runtime_error("Cannot initialize object storage adapter");
    auto catalog = std::make_unique<ObjectStorageCatalog>(
        config.object_storage, std::make_shared<EtcdHaKvBackend>(),
        config.cluster_id);
    error = catalog->Open(config.view_version);
    if (error != ErrorCode::OK)
        throw std::runtime_error("Cannot acquire object storage catalog: " +
                                 toString(error));
    object_storage_adapter_ = *adapter;
    object_storage_catalog_ = std::move(catalog);
    if (config.enable_ha) return;
    error = RecoverObjectStorage();
    if (error != ErrorCode::OK)
        throw std::runtime_error("Cannot recover object storage catalog: " +
                                 toString(error));
#endif
}

tl::expected<ObjectStorageConfig, ErrorCode>
MasterService::GetObjectStorageConfig() {
    return object_storage_catalog_ ? object_storage_catalog_->config()
                                   : ObjectStorageConfig{};
}

tl::expected<void, ErrorCode> MasterService::RegisterObjectStorageClient(
    const UUID& client_id) {
    if (!object_storage_catalog_)
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    std::lock_guard lock(object_storage_clients_mutex_);
    object_storage_clients_.insert(client_id);
    return {};
}

ErrorCode MasterService::PrepareObjectStorage(ObjectMetadata& metadata) {
    auto* target =
        metadata.GetFirstReplica(&Replica::fn_is_object_storage_replica);
    if (!target) return ErrorCode::OK;
    if (!object_storage_catalog_) return ErrorCode::NOT_SUPPORTED;
    const auto mode = target->get_object_storage_descriptor().write_mode;
    ObjectStorageRecord record;
    record.tenant_id = metadata.tenant_id.value();
    record.key = metadata.user_key;
    record.target = target->get_descriptor();
    record.writer = metadata.client_id;
    record.uploader = metadata.client_id;
    record.group_id = metadata.group_id;
    record.hard_pinned = metadata.IsHardPinned();
    record.data_type = metadata.data_type;
    if (mode != ObjectStorageWriteMode::SYNC) {
        bool found = false;
        for (const auto& source : metadata.GetAllReplicas()) {
            if (!source.is_memory_replica()) continue;
            for (const auto& segment : source.get_segment_names()) {
                if (!segment) continue;
                std::optional<UUID> owner;
                {
                    auto view = segment_manager_.getAllocatorAccess();
                    owner = view.GetOwnerClientId(*segment);
                }
                if (!owner) continue;
                std::lock_guard lock(object_storage_clients_mutex_);
                if (!object_storage_clients_.contains(*owner)) continue;
                record.source = source.get_descriptor();
                record.uploader = *owner;
                found = true;
                break;
            }
            if (found) break;
        }
        if (!found) return ErrorCode::UNABLE_OFFLOADING;
    }
    return object_storage_catalog_->Save(record);
}

void MasterService::UpdateObjectStorageSourceReference(
    ObjectMetadata& metadata, const ObjectStorageRecord& record, bool acquire) {
    auto* source = metadata.GetReplicaByID(record.source.id);
    std::lock_guard lock(object_storage_refs_mutex_);
    if (acquire) {
        if (source && object_storage_refs_.insert(record.target.id).second)
            source->inc_refcnt();
    } else if (object_storage_refs_.erase(record.target.id) && source)
        source->dec_refcnt();
}

ErrorCode MasterService::CommitObjectStorageForeground(ObjectMetadata& metadata,
                                                       const ObjectMeta& meta) {
    auto* target =
        metadata.GetFirstReplica(&Replica::fn_is_object_storage_replica);
    if (!target)
        return meta.object_storage_target ? ErrorCode::INVALID_WRITE
                                          : ErrorCode::OK;
    if (target->status() == ReplicaStatus::REMOVED ||
        !meta.object_storage_target ||
        *meta.object_storage_target != target->id())
        return ErrorCode::INVALID_WRITE;
    auto record = object_storage_catalog_->Find(
        target->get_object_storage_descriptor().object_key);
    if (!record || record->phase == ObjectStoragePhase::RETIRED)
        return ErrorCode::INVALID_WRITE;
    if (record->foreground_complete) {
        if (record->descriptor().write_mode == ObjectStorageWriteMode::ASYNC &&
            !record->source_released)
            UpdateObjectStorageSourceReference(metadata, *record, true);
        return ErrorCode::OK;
    }
    const auto mode = record->descriptor().write_mode;
    if (mode == ObjectStorageWriteMode::SYNC && !record->upload_finished)
        return ErrorCode::REPLICA_IS_NOT_READY;
    auto* source = mode == ObjectStorageWriteMode::SYNC
                       ? nullptr
                       : metadata.GetReplicaByID(record->source.id);
    if (mode != ObjectStorageWriteMode::SYNC && !source)
        return ErrorCode::REPLICA_IS_NOT_READY;
    record->foreground_complete = true;
    record->checksum = meta.object_checksum;
    record->phase =
        mode == ObjectStorageWriteMode::SYNC    ? ObjectStoragePhase::COMPLETE
        : mode == ObjectStorageWriteMode::ASYNC ? ObjectStoragePhase::READY
                                                : ObjectStoragePhase::DEFERRED;
    if (mode == ObjectStorageWriteMode::SYNC)
        record->target.status = ReplicaStatus::COMPLETE;
    const auto error = object_storage_catalog_->Save(*record);
    if (error != ErrorCode::OK) return error;
    if (mode == ObjectStorageWriteMode::ASYNC)
        UpdateObjectStorageSourceReference(metadata, *record, true);
    return ErrorCode::OK;
}

bool MasterService::HasPendingObjectStorage(
    const ObjectMetadata& metadata) const {
    return metadata.HasReplica([](const Replica& r) {
        return r.is_object_storage_replica() && !r.is_completed();
    });
}

void MasterService::ReconcileObjectStorageTarget(ObjectMetadata& metadata) {
    auto* target =
        metadata.GetFirstReplica(&Replica::fn_is_object_storage_replica);
    if (!target || target->status() == ReplicaStatus::REMOVED) return;
    auto record = object_storage_catalog_->Find(
        target->get_object_storage_descriptor().object_key);
    const auto head = object_storage_catalog_->Head(metadata.tenant_id.value(),
                                                    metadata.user_key);
    if (head && head->target.id > target->id() && !head->object_deleted &&
        head->descriptor().object_size == metadata.size &&
        metadata.GetReplicaByID(head->source.id)) {
        // The replacement transaction may have committed even when both its
        // reply and reconciliation failed. Adopt that exact durable attempt.
        if (record)
            UpdateObjectStorageSourceReference(metadata, *record, false);
        metadata.EraseReplicaByID(target->id());
        std::vector<Replica> replicas;
        replicas.push_back(
            Replica(head->target.id, head->descriptor(), head->target.status));
        metadata.AddReplicas(std::move(replicas));
        if (!head->source_released &&
            head->phase != ObjectStoragePhase::DEFERRED)
            UpdateObjectStorageSourceReference(metadata, *head, true);
    }
}

bool MasterService::ScheduleObjectStorage(ObjectMetadata& metadata) {
    ReconcileObjectStorageTarget(metadata);
    auto* target =
        metadata.GetFirstReplica(&Replica::fn_is_object_storage_replica);
    if (!target || target->is_completed()) return false;
    if (target->status() == ReplicaStatus::REMOVED) return true;
    auto record = object_storage_catalog_->Find(
        target->get_object_storage_descriptor().object_key);
    if (!record) return true;
    if (record->source_released)
        UpdateObjectStorageSourceReference(metadata, *record, false);
    else if (record->foreground_complete &&
             record->phase != ObjectStoragePhase::DEFERRED)
        UpdateObjectStorageSourceReference(metadata, *record, true);
    auto* old_source = metadata.GetReplicaByID(record->source.id);
    if (record->foreground_complete &&
        record->phase != ObjectStoragePhase::RETIRED &&
        record->phase != ObjectStoragePhase::COMPLETE &&
        (!old_source || old_source->has_invalid_mem_handle())) {
        record->phase = ObjectStoragePhase::RETIRED;
        // BeginObjectStorageUpload uses the same metadata shard lock. An
        // unclaimed attempt cannot issue a PUT after this retirement.
        if (!record->upload_started) record->upload_finished = true;
        record->source_released = true;
        record->retire_after_ms = ObjectNowMs() + default_kv_lease_ttl_;
        if (object_storage_catalog_->Save(*record) != ErrorCode::OK)
            return true;
        UpdateObjectStorageSourceReference(metadata, *record, false);
        record = object_storage_catalog_->Find(record->descriptor().object_key);
    }
    if (record->phase == ObjectStoragePhase::RETIRED &&
        record->foreground_complete) {
        // Retry with a fresh immutable name; the old name stays on the GC list.
        auto* source = metadata.GetReplicaByID(record->source.id);
        UUID uploader = record->uploader;
        if (!source || !source->is_completed() ||
            source->has_invalid_mem_handle()) {
            source = nullptr;
            for (const auto& candidate : metadata.GetAllReplicas()) {
                if (!candidate.is_memory_replica() ||
                    !candidate.is_completed() ||
                    candidate.has_invalid_mem_handle())
                    continue;
                for (const auto& segment : candidate.get_segment_names()) {
                    if (!segment) continue;
                    auto view = segment_manager_.getAllocatorAccess();
                    auto owner = view.GetOwnerClientId(*segment);
                    if (!owner) continue;
                    std::lock_guard lock(object_storage_clients_mutex_);
                    if (!object_storage_clients_.contains(*owner)) continue;
                    uploader = *owner;
                    source = metadata.GetReplicaByID(candidate.id());
                    break;
                }
                if (source) break;
            }
            if (!source) {
                // Failure of this upload attempt must not delete readable
                // replicas in other tiers. Keep the intent for a future source.
                if (!HasReadableReplica(metadata))
                    RetireObjectStorage(metadata);
                return true;
            }
        }
        auto next = *record;
        Replica replacement(
            ObjectStorageDescriptor{UuidToString(generate_uuid()),
                                    metadata.size,
                                    record->descriptor().write_mode,
                                    record->descriptor().namespace_id},
            ReplicaStatus::PROCESSING);
        next.target = replacement.get_descriptor();
        next.source = source->get_descriptor();
        next.uploader = uploader;
        next.phase = ObjectStoragePhase::READY;
        next.upload_started = next.upload_finished = next.source_released =
            next.object_deleted = false;
        next.upload_started_ms = next.retire_after_ms = next.revision = 0;
        if (object_storage_catalog_->Save(next) != ErrorCode::OK) return true;
        const auto old_id = target->id();
        metadata.EraseReplicaByID(old_id);
        std::vector<Replica> replicas;
        replicas.push_back(std::move(replacement));
        metadata.AddReplicas(std::move(replicas));
        UpdateObjectStorageSourceReference(metadata, next, true);
        object_storage_catalog_->Retire(record->descriptor().object_key,
                                        ObjectNowMs() + default_kv_lease_ttl_);
        return true;
    }
    if (record->phase == ObjectStoragePhase::DEFERRED) {
        auto* source = metadata.GetReplicaByID(record->source.id);
        if (!source || !source->is_completed()) return true;
        record->phase = ObjectStoragePhase::READY;
        if (object_storage_catalog_->Save(*record) == ErrorCode::OK)
            UpdateObjectStorageSourceReference(metadata, *record, true);
    }
    return true;
}

tl::expected<std::vector<ObjectStorageRecord>, ErrorCode>
MasterService::PollObjectStorageTasks(const UUID& client_id) {
    if (!object_storage_catalog_)
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    auto registered = RegisterObjectStorageClient(client_id);
    if (!registered) return tl::make_unexpected(registered.error());
    return object_storage_catalog_->TasksFor(client_id);
}

tl::expected<void, ErrorCode> MasterService::BeginObjectStorageUpload(
    const UUID& client_id, const std::string& object_key) {
    if (!object_storage_catalog_)
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    auto record = object_storage_catalog_->Find(object_key);
    if (!record || record->uploader != client_id)
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    std::shared_lock snapshot_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(
        this,
        MakeObjectIdentityForRequest(record->key, TenantId(record->tenant_id)));
    if (accessor.Exists()) ReconcileObjectStorageTarget(accessor.Get());
    if (!accessor.Exists() || !accessor.Get().GetReplicaByID(record->target.id))
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    if (accessor.Get().GetReplicaByID(record->target.id)->status() ==
        ReplicaStatus::REMOVED)
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    // Re-read after taking the shard: every transition uses that same lock.
    record = object_storage_catalog_->Find(object_key);
    if (!record || record->upload_started || record->upload_finished ||
        (record->phase != ObjectStoragePhase::PREPARED &&
         record->phase != ObjectStoragePhase::READY))
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    if (record->descriptor().write_mode != ObjectStorageWriteMode::SYNC) {
        if (accessor.HasReplicationTask())
            return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
        auto* source = accessor.Get().GetReplicaByID(record->source.id);
        if (!source || !source->is_completed() ||
            source->has_invalid_mem_handle())
            return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    if (record->descriptor().write_mode != ObjectStorageWriteMode::SYNC)
        UpdateObjectStorageSourceReference(accessor.Get(), *record, true);
    record->upload_started = true;
    record->upload_started_ms = ObjectNowMs();
    record->phase = ObjectStoragePhase::UPLOADING;
    const auto error = object_storage_catalog_->Save(*record);
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return {};
}

tl::expected<void, ErrorCode> MasterService::FinishObjectStorageUpload(
    const UUID& client_id, const std::string& object_key,
    ObjectUploadOutcome outcome) {
    if (!object_storage_catalog_)
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    auto record = object_storage_catalog_->Find(object_key);
    if (!record)
        return {};  // Immutable names are never reused; GC is terminal.
    if (record->uploader != client_id)
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    std::shared_lock snapshot_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(
        this,
        MakeObjectIdentityForRequest(record->key, TenantId(record->tenant_id)));
    record = object_storage_catalog_->Find(object_key);
    if (!record) return {};
    if (!record->upload_started && outcome != ObjectUploadOutcome::NOT_STARTED)
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    if (outcome != ObjectUploadOutcome::COMPLETE &&
        outcome != ObjectUploadOutcome::NOT_STARTED &&
        outcome != ObjectUploadOutcome::UNKNOWN)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    if (record->upload_finished) {
        if (accessor.Exists()) {
            UpdateObjectStorageSourceReference(accessor.Get(), *record, false);
            auto* completed = accessor.Get().GetReplicaByID(record->target.id);
            if (completed && record->phase == ObjectStoragePhase::COMPLETE &&
                completed->is_processing()) {
                completed->mark_complete();
                SyncCacheTotalAccounting(accessor.Get());
                PublishKvStored(record->key, accessor.Get(),
                                TenantId(record->tenant_id));
            }
        }
        if (accessor.Exists() && record->foreground_complete &&
            !record->object_deleted &&
            record->phase == ObjectStoragePhase::RETIRED &&
            accessor.Get().GetReplicaByID(record->target.id))
            ScheduleObjectStorage(accessor.Get());
        return {};
    }
    auto* target = accessor.Exists()
                       ? accessor.Get().GetReplicaByID(record->target.id)
                       : nullptr;
    const bool success = outcome == ObjectUploadOutcome::COMPLETE;
    record->upload_finished = outcome != ObjectUploadOutcome::UNKNOWN;
    if (outcome == ObjectUploadOutcome::NOT_STARTED)
        record->upload_started = false;
    const bool retired = record->phase == ObjectStoragePhase::RETIRED ||
                         !target || target->status() == ReplicaStatus::REMOVED;
    if (retired || !success) {
        record->phase = ObjectStoragePhase::RETIRED;
        record->retire_after_ms = std::max(
            record->retire_after_ms, ObjectNowMs() + default_kv_lease_ttl_);
    } else {
        record->phase = record->foreground_complete
                            ? ObjectStoragePhase::COMPLETE
                            : ObjectStoragePhase::PREPARED;
        if (record->foreground_complete)
            record->target.status = ReplicaStatus::COMPLETE;
    }
    record->source_released = true;
    const auto error = object_storage_catalog_->Save(*record);
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    if (accessor.Exists())
        UpdateObjectStorageSourceReference(accessor.Get(), *record, false);
    if (!success && target && record->foreground_complete &&
        !record->object_deleted)
        ScheduleObjectStorage(accessor.Get());
    if (!retired && success && record->foreground_complete) {
        target->mark_complete();
        SyncCacheTotalAccounting(accessor.Get());
        PublishKvStored(record->key, accessor.Get(),
                        TenantId(record->tenant_id));
        if (enable_oplog_ && ordered_oplog_writer_) {
            auto logged = AppendOpLogVisibleBeforeDurable(
                OpType::PUT_END, record->tenant_id, record->key,
                SerializeMetadataForOpLog(accessor.Get()));
            if (!logged) return tl::make_unexpected(logged.error());
        }
    }
    return {};
}

ErrorCode MasterService::RetireObjectStorage(ObjectMetadata& metadata) {
    if (!object_storage_catalog_) return ErrorCode::OK;
    auto records = object_storage_catalog_->ForObject(
        metadata.tenant_id.value(), metadata.user_key);
    for (const auto& record : records) {
        const auto* source = metadata.GetReplicaByID(record.source.id);
        if (record.foreground_complete && record.upload_started &&
            !record.source_released && source &&
            !source->has_invalid_mem_handle())
            return ErrorCode::OBJECT_HAS_REPLICATION_TASK;
    }
    // Stop new leases before a retirement whose outcome may be unknown.
    // An error requires retry; allowing reads could outlive the GC deadline.
    if (metadata.HasReplica(&Replica::fn_is_object_storage_replica)) {
        metadata.VisitReplicas(
            [](const Replica& r) {
                return r.status() != ReplicaStatus::REMOVED;
            },
            [](Replica& r) { r.mark_removed(); });
    }
    for (const auto& record : records)
        UpdateObjectStorageSourceReference(metadata, record, false);
    // Retire every prior attempt first. The current logical head commits last;
    // recovery must never fall back to an older attempt after deleting the
    // head.
    std::sort(records.begin(), records.end(), [](const auto& a, const auto& b) {
        return a.target.id < b.target.id;
    });
    const auto lease = std::chrono::duration_cast<std::chrono::milliseconds>(
                           metadata.lease_->ExpiresAt().time_since_epoch())
                           .count();
    const auto deadline = std::max<uint64_t>(
        ObjectNowMs() + default_kv_lease_ttl_, std::max<int64_t>(0, lease));
    for (const auto& record : records) {
        if (record.object_deleted) continue;
        const auto error = object_storage_catalog_->Retire(
            record.descriptor().object_key, deadline);
        if (error != ErrorCode::OK) return error;
    }
    return ErrorCode::OK;
}

tl::expected<void, ErrorCode> MasterService::RevokeObjectStorageWrite(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    ReplicaID target_id) {
    std::shared_lock snapshot_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) return {};
    auto& metadata = accessor.Get();
    const auto* target = metadata.GetReplicaByID(target_id);
    if (!target || !target->is_object_storage_replica() ||
        metadata.client_id != client_id)
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    auto record = object_storage_catalog_->Find(
        target->get_object_storage_descriptor().object_key);
    if (record && record->foreground_complete)
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    const auto error = RetireObjectStorage(metadata);
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    auto replicas = metadata.PopReplicas();
    FreeDfsReplicas(key, replicas);
    {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.emplace_back(
            std::move(replicas),
            std::chrono::system_clock::now() + put_start_release_timeout_sec_);
    }
    auto& tenant = accessor.GetTenantState();
    const auto erased =
        EraseMetadata(tenant, tenant.metadata.find(key), tenant_id);
    return erased ? tl::expected<void, ErrorCode>{}
                  : tl::make_unexpected(erased.error());
}

void MasterService::ReclaimObjectStorage() {
    if (!object_storage_catalog_ || !object_storage_recovery_complete_.load())
        return;
    std::unique_lock maintenance(object_storage_maintenance_mutex_,
                                 std::try_to_lock);
    if (!maintenance.owns_lock()) return;
    for (const auto& record : object_storage_catalog_->NextRecords(
             object_storage_maintenance_cursor_)) {
        if (record.phase == ObjectStoragePhase::COMPLETE) continue;
        std::shared_lock snapshot_lock(snapshot_mutex_);
        MetadataAccessorRW accessor(
            this, MakeObjectIdentityForRequest(record.key,
                                               TenantId(record.tenant_id)));
        if (accessor.Exists()) ReconcileObjectStorageTarget(accessor.Get());
        if (!accessor.Exists() ||
            !accessor.Get().GetReplicaByID(record.target.id)) {
            // A crash after saving a replacement can leave its old intent
            // behind. Logical head fencing makes that name unreachable.
            const auto retired = object_storage_catalog_->Retire(
                record.descriptor().object_key,
                ObjectNowMs() + default_kv_lease_ttl_);
            if (retired != ErrorCode::OK)
                LOG(WARNING)
                    << "Cannot retire abandoned object intent: " << retired;
            continue;
        }
        if (record.object_deleted) {
            const auto erased = accessor.Erase();
            if (!erased)
                LOG(WARNING) << "Cannot finish object storage retirement: "
                             << erased.error();
            continue;
        }
        if (!record.foreground_complete) continue;
        // DEFERRED is scheduled exclusively by pressure, not by maintenance.
        auto* source = accessor.Get().GetReplicaByID(record.source.id);
        if (record.phase == ObjectStoragePhase::RETIRED || !source ||
            source->has_invalid_mem_handle())
            ScheduleObjectStorage(accessor.Get());
    }
    const auto error = object_storage_catalog_->Reclaim(
        *object_storage_adapter_, ObjectNowMs());
    if (error != ErrorCode::OK)
        LOG(WARNING) << "Object storage reclamation failed: " << error;
}

ErrorCode MasterService::RecoverObjectStorage() {
    if (!object_storage_catalog_) return ErrorCode::OK;
    // The catalogue is authoritative for immutable names and foreground
    // commits. Snapshot/oplog records can lag a successful catalogue commit.
    auto records = object_storage_catalog_->Records();
    std::map<std::pair<std::string, std::string>, ObjectStorageRecord> latest;
    ReplicaID maximum_id = 0;
    for (const auto& record : object_storage_catalog_->Heads()) {
        maximum_id = std::max(maximum_id, record.target.id);
        if (!record.foreground_complete) continue;
        auto identity = std::make_pair(record.tenant_id, record.key);
        auto it = latest.find(identity);
        if (it == latest.end() || it->second.target.id < record.target.id)
            latest.insert_or_assign(std::move(identity), record);
    }
    for (const auto& record : records)
        maximum_id = std::max(maximum_id, record.target.id);
    auto next_id = Replica::next_id_.load();
    while (next_id <= maximum_id &&
           !Replica::next_id_.compare_exchange_weak(next_id, maximum_id + 1)) {
    }

    for (auto& shard : metadata_shards_) {
        for (auto& [tenant_id, tenant] : shard.tenants) {
            for (auto it = tenant.metadata.begin();
                 it != tenant.metadata.end();) {
                auto& metadata = it->second;
                auto* target = metadata.GetFirstReplica(
                    &Replica::fn_is_object_storage_replica);
                auto found = latest.find({tenant_id.value(), it->first});
                if (!target && found == latest.end()) {
                    ++it;
                    continue;
                }
                if (found != latest.end() && found->second.object_deleted &&
                    !target && metadata.AllReplicas([&](const Replica& r) {
                        return r.id() > found->second.target.id;
                    })) {
                    // A later MEMORY-only write of the same key is independent.
                    ++it;
                    continue;
                }
                const bool exact_target =
                    found != latest.end() && target &&
                    target->id() == found->second.target.id;
                const bool exact_source =
                    found != latest.end() &&
                    metadata.GetReplicaByID(found->second.source.id) != nullptr;
                if (found == latest.end() || found->second.object_deleted ||
                    (!exact_target && !exact_source)) {
                    AccountCacheTotalRemoval(metadata);
                    tenant.processing_keys.erase(it->first);
                    if (metadata.GetCommittedSoftPinTimeout())
                        soft_pin_deadline_index_.Remove(
                            tenant_id.MakeScopedKey(it->first));
                    it = tenant.metadata.erase(it);
                    continue;
                }
                const auto& record = found->second;
                if (!exact_target) {
                    metadata.EraseReplicas(
                        &Replica::fn_is_object_storage_replica);
                    std::vector<Replica> targets;
                    targets.push_back(
                        Replica(record.target.id, record.descriptor(),
                                record.phase == ObjectStoragePhase::COMPLETE
                                    ? ReplicaStatus::COMPLETE
                                    : ReplicaStatus::PROCESSING));
                    metadata.AddReplicas(std::move(targets));
                    target = metadata.GetReplicaByID(record.target.id);
                }
                metadata.object_checksum = record.checksum;
                if (record.phase == ObjectStoragePhase::COMPLETE &&
                    !target->is_completed())
                    target->mark_complete();
                tenant.processing_keys.erase(it->first);
                ++it;
            }
        }
    }
    // Restore every source guard, including old attempts whose PUT/copy has
    // not reported completion. Their pins must survive a leader transition.
    for (const auto& record : records) {
        const TenantId tenant_id(record.tenant_id);
        auto& shard = metadata_shards_[getShardIndex(tenant_id, record.key)];
        auto tenant_it = shard.tenants.find(tenant_id);
        if (tenant_it != shard.tenants.end() && record.foreground_complete &&
            !record.source_released &&
            (record.phase == ObjectStoragePhase::READY ||
             record.phase == ObjectStoragePhase::UPLOADING ||
             record.phase == ObjectStoragePhase::RETIRED)) {
            auto it = tenant_it->second.metadata.find(record.key);
            if (it != tenant_it->second.metadata.end()) {
                auto* source = it->second.GetReplicaByID(record.source.id);
                if (source)
                    UpdateObjectStorageSourceReference(it->second, record,
                                                       true);
            }
        }
        if (!record.foreground_complete && !record.object_deleted) {
            const auto error = object_storage_catalog_->Retire(
                record.descriptor().object_key,
                ObjectNowMs() + default_kv_lease_ttl_);
            if (error != ErrorCode::OK) return error;
        }
    }
    for (const auto& [identity, record] : latest) {
        if (record.object_deleted) continue;
        const TenantId tenant_id(record.tenant_id);
        auto& shard = metadata_shards_[getShardIndex(tenant_id, record.key)];
        auto& tenant = GetOrCreateTenantState(shard, tenant_id);
        if (tenant.metadata.contains(record.key)) continue;
        if (record.phase != ObjectStoragePhase::COMPLETE) {
            // No surviving memory source: async has not promised durability
            // before this state. Cancel the unreachable intent, never publish
            // it.
            const auto error = object_storage_catalog_->Retire(
                record.descriptor().object_key,
                ObjectNowMs() + default_kv_lease_ttl_);
            if (error != ErrorCode::OK) return error;
            continue;
        }
        std::vector<Replica> replicas;
        replicas.push_back(Replica(record.target.id, record.descriptor(),
                                   ReplicaStatus::COMPLETE));
        auto [it, inserted] = tenant.metadata.emplace(
            std::piecewise_construct, std::forward_as_tuple(record.key),
            std::forward_as_tuple(
                record.writer, std::chrono::system_clock::now(),
                record.descriptor().object_size, std::move(replicas),
                std::nullopt, record.hard_pinned, record.data_type,
                record.group_id, tenant_id, record.key));
        it->second.object_checksum = record.checksum;
    }
    RebuildGroupState();
    RebuildCacheTotalAccounting();
    if (enable_multi_tenants_) RebuildTenantQuotaUsageFromMetadata();
    object_storage_recovery_complete_.store(true);
    return ErrorCode::OK;
}

}  // namespace mooncake
