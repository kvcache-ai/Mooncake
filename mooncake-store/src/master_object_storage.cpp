#include "master_service.h"

#include <future>
#include <stdexcept>

#include "config/dfs_enablement_config.h"
#include "config/distributed_storage_config.h"
#include "ha/ha_types.h"
#include "ha/standby_metadata_store.h"
#include "ha/oplog/oplog_test_failpoint.h"
#include "storage/distributed/object_replica_io.h"

namespace mooncake {
namespace {
uint64_t ObjectNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
uint64_t ObjectLeaseTtl(const ObjectUploadRecord& record, uint64_t current) {
    return std::max(current, record.read_lease_ttl_ms.value_or(0));
}
void RetireObject(ObjectUploadRecord& record, uint64_t ttl, uint64_t now) {
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms =
        std::max(record.not_before_ms, now + ObjectLeaseTtl(record, ttl));
}
std::string EncodeObjectMutation(const ObjectReplicaMutation& mutation) {
    auto bytes = struct_pack::serialize(mutation);
    return {bytes.begin(), bytes.end()};
}
struct ObjectCompletion {
    std::promise<ErrorCode> promise;
    std::atomic<bool> completed{false};
    void Finish(ErrorCode error) {
        if (!completed.exchange(true)) promise.set_value(error);
    }
};
}  // namespace

std::shared_ptr<ObjectStorageAdapter>
MasterService::PrepareObjectStorageAdapter(const MasterServiceConfig& config) {
    if (!config.object_storage.Validate() || !config.enable_ha ||
        !config.enable_oplog || config.cluster_id.empty() ||
        !ha::HaBackendSupportsOpLog(config.ha_backend_type) ||
        DfsEnablementConfig::FromEnvironment().enabled ||
        DistributedStorageConfig::FromEnvironment().UsesKvcs() ||
        !config.root_fs_dir.empty()) {
        throw std::invalid_argument(
            "Object replicas require a valid target including an explicit "
            "object_storage_region, a nonempty cluster_id, and fenced HA "
            "ordered-log recovery with DFS "
            "and legacy shared-file replicas disabled");
    }
    auto adapter = CreateObjectReplicaAdapter(config.object_storage);
    if (!adapter)
        throw std::runtime_error("Cannot initialize object replica adapter: " +
                                 toString(adapter.error()));
    return *adapter;
}

void MasterService::InitObjectStorage(const MasterServiceConfig& config) {
    object_storage_config_ = config.object_storage;
    if (!object_storage_config_.enabled) return;
    if (!ordered_oplog_writer_ || view_version_ == 0)
        throw std::invalid_argument("Object replicas require a fenced writer");
    object_storage_adapter_ = PrepareObjectStorageAdapter(config);
    object_uploads_ = std::make_unique<StandbyMetadataStore>();
}

tl::expected<ObjectStorageConfig, ErrorCode>
MasterService::GetObjectStorageConfig() {
    return object_storage_config_;
}

tl::expected<void, ErrorCode> MasterService::WaitObjectDurable(
    uint64_t sequence, uint64_t timeout_ms) {
    auto done = std::make_shared<std::promise<ErrorCode>>();
    auto result = done->get_future();
    [[maybe_unused]] auto waiter =
        ordered_oplog_writer_->AwaitDurable(sequence).thenValue(
            [done](ErrorCode error) { done->set_value(error); });
    if (timeout_ms && result.wait_for(std::chrono::milliseconds(timeout_ms)) !=
                          std::future_status::ready) {
        LOG(ERROR) << "Timed out waiting for durable object repair, sequence="
                   << sequence;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto error = result.get();
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return {};
}

// The caller holds its metadata key. This mutex serializes record preparation
// with log ordering across keys; no network I/O or durability wait occurs here.
tl::expected<OrderedOpLogWriter::PendingHandle, ErrorCode>
MasterService::CommitObjectAware(OrderedOpLogWriter::Reservation&& reservation,
                                 OpLogEntry entry,
                                 DurableFinalizeCallback callback,
                                 uint64_t* object_sequence) {
    if (!object_uploads_ ||
        (entry.op_type != OpType::OBJECT_STORAGE_MUTATION &&
         entry.op_type != OpType::REMOVE && entry.op_type != OpType::PUT_END &&
         entry.op_type != OpType::PUT_REVOKE)) {
        return ordered_oplog_writer_->Commit(
            std::move(reservation), std::move(entry), std::move(callback));
    }
    std::lock_guard lock(object_storage_mutex_);
    auto previous = object_uploads_->ActiveObjectUploadsForKey(
        entry.tenant_id, entry.object_key);
    if (previous.empty()) {
        auto retired = object_uploads_->FirstObjectUploadForKey(
            entry.tenant_id, entry.object_key);
        if (retired) previous.push_back(*retired);
    }
    ObjectReplicaMutation mutation;
    if (entry.op_type == OpType::OBJECT_STORAGE_MUTATION) {
        if (struct_pack::deserialize_to(mutation, entry.payload) !=
            struct_pack::errc::ok)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        std::unordered_set<std::string> included;
        for (const auto& record : previous)
            included.insert(record.object.object_key);
        auto include = [&](const std::string& name) {
            if (included.insert(name).second) {
                auto old = object_uploads_->FindObjectUpload(name);
                if (old) previous.push_back(*old);
            }
        };
        for (const auto& record : mutation.records)
            include(record.object.object_key);
        for (const auto& name : mutation.forget) include(name);
        if (mutation.expired_forget)
            for (const auto& name : *mutation.expired_forget) include(name);
        if (mutation.orphan_deletes)
            for (const auto& name : *mutation.orphan_deletes)
                if (!name.starts_with(ObjectStorageGcPrefix(cluster_id_)) ||
                    object_uploads_->FindObjectUpload(name))
                    return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    } else {
        if (previous.empty())
            return ordered_oplog_writer_->Commit(
                std::move(reservation), std::move(entry), std::move(callback));
        mutation.target = object_storage_config_.target;
        mutation.remove_value = entry.op_type == OpType::REMOVE ||
                                entry.op_type == OpType::PUT_REVOKE;
        std::unordered_set<std::string> retained;
        if (!mutation.remove_value) {
            MetadataPayload payload;
            if (struct_pack::deserialize_to(payload, entry.payload) !=
                struct_pack::errc::ok)
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            mutation.metadata_payload = entry.payload;
            for (const auto& replica : payload.replicas)
                if (replica.is_object_storage_replica())
                    retained.insert(
                        replica.get_object_storage_descriptor().object_key);
        }
        for (auto record : previous) {
            if (!retained.contains(record.object.object_key) &&
                record.phase != ObjectUploadPhase::RETIRED) {
                RetireObject(record, default_kv_lease_ttl_, ObjectNowMs());
            }
            mutation.records.push_back(std::move(record));
        }
        entry.op_type = OpType::OBJECT_STORAGE_MUTATION;
        entry.payload = EncodeObjectMutation(mutation);
    }
    if (!mutation.metadata_payload.empty() || mutation.remove_value) {
        MetadataPayload metadata;
        std::unordered_set<std::string> retained, changed;
        if (!mutation.metadata_payload.empty()) {
            if (struct_pack::deserialize_to(metadata,
                                            mutation.metadata_payload) !=
                struct_pack::errc::ok)
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            for (const auto& replica : metadata.replicas)
                if (replica.is_object_storage_replica())
                    retained.insert(
                        replica.get_object_storage_descriptor().object_key);
        }
        for (const auto& record : mutation.records)
            changed.insert(record.object.object_key);
        for (auto record : previous) {
            if (changed.contains(record.object.object_key)) continue;
            if (!retained.contains(record.object.object_key) &&
                record.phase != ObjectUploadPhase::RETIRED) {
                RetireObject(record, default_kv_lease_ttl_, ObjectNowMs());
            }
            mutation.records.push_back(std::move(record));
        }
        entry.payload = EncodeObjectMutation(mutation);
    }
    auto forget = mutation.forget;
    if (mutation.expired_forget)
        forget.insert(forget.end(), mutation.expired_forget->begin(),
                      mutation.expired_forget->end());
    size_t new_records = 0;
    for (const auto& record : mutation.records)
        if (!object_uploads_->FindObjectUpload(record.object.object_key))
            ++new_records;
    const auto pending_writes =
        object_pending_deadlines_.size() - object_gc_records_.size();
    if (new_records &&
        (object_gc_records_.size() >= object_storage_config_.max_gc_records ||
         pending_writes >= object_storage_config_.max_pending_records ||
         new_records >
             object_storage_config_.max_pending_records - pending_writes))
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    // Remember transition boundaries before applying the candidate. Repeated
    // notifications must not postpone an existing cleanup/recheck deadline.
    std::unordered_map<std::string, ObjectUploadRecord> old_records;
    for (const auto& record : mutation.records) {
        auto old = object_uploads_->FindObjectUpload(record.object.object_key);
        if (old) old_records.emplace(record.object.object_key, std::move(*old));
    }
    // Validate before enqueue without publishing candidate logical metadata.
    StandbyMetadataStore validation;
    if (!validation.RestoreObjectReplicas({mutation.target, previous}) ||
        !validation.ApplyObjectReplicaMutation(entry.tenant_id,
                                               entry.object_key, mutation))
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    auto pending = ordered_oplog_writer_->Commit(std::move(reservation), entry,
                                                 std::move(callback));
    if (!pending) return pending;
    // Materialize the same ordered log as the standby, for cleanup
    // reachability. Serving visibility still comes only from ordinary Master
    // metadata. A temporarily hidden old Upsert value stays reachable here
    // until its replacement or removal is logged. Cleanup waits for this
    // sequence.
    if (!object_uploads_->ApplyObjectReplicaMutation(
            entry.tenant_id, entry.object_key, mutation))
        throw std::logic_error("Validated object record transition failed");
    if (object_sequence) *object_sequence = pending->sequence_id();
    for (const auto& record : mutation.records)
        object_upload_sequences_[record.object.object_key] =
            pending->sequence_id();
    const auto logical =
        object_uploads_->GetMetadata(entry.tenant_id, entry.object_key);
    const auto now = BackgroundWorker::Clock::now();
    for (const auto& record : mutation.records) {
        const auto& name = record.object.object_key;
        const bool committed =
            record.phase == ObjectUploadPhase::UPLOADED && logical &&
            std::any_of(
                logical->replicas.begin(), logical->replicas.end(),
                [&](const auto& replica) {
                    return replica.is_object_storage_replica() &&
                           replica.status == ReplicaStatus::COMPLETE &&
                           replica.id == record.replica_id &&
                           replica.get_object_storage_descriptor().object_key ==
                               name;
                });
        if (committed) {
            SetObjectWorkLocked(name, std::nullopt);
            continue;
        }
        const auto old = old_records.find(name);
        const auto work = object_pending_deadlines_.find(name);
        if (work == object_pending_deadlines_.end() ||
            old == old_records.end() || old->second.phase != record.phase ||
            old->second.not_before_ms != record.not_before_ms ||
            old->second.upload_finished != record.upload_finished) {
            if (record.phase != ObjectUploadPhase::RETIRED) {
                SetObjectWorkLocked(
                    name,
                    now + std::chrono::milliseconds(
                              object_storage_config_.cleanup_interval_ms));
                continue;
            }
            // Preserve the original lease window on completion notifications.
            // Recovered retirements wait out the previous leader's saved TTL.
            std::chrono::steady_clock::time_point lease_deadline;
            if (old != old_records.end() &&
                old->second.phase == ObjectUploadPhase::RETIRED) {
                lease_deadline =
                    work != object_pending_deadlines_.end()
                        ? work->second.lease_deadline
                        : object_restore_time_ +
                              std::chrono::milliseconds(
                                  record.read_lease_ttl_ms.value_or(
                                      default_kv_lease_ttl_));
            } else {
                lease_deadline = now + std::chrono::milliseconds(ObjectLeaseTtl(
                                           record, default_kv_lease_ttl_));
            }
            SetObjectWorkLocked(name, lease_deadline, lease_deadline);
        }
    }
    for (const auto& name : forget) {
        object_upload_sequences_.erase(name);
        SetObjectWorkLocked(name, std::nullopt);
    }
    ScheduleObjectWorkLocked();
    if (!forget.empty() && !object_uploads_->FirstObjectUploadForKey(
                               entry.tenant_id, entry.object_key))
        object_uploads_->Remove(entry.tenant_id, entry.object_key);
    return pending;
}

tl::expected<void, ErrorCode> MasterService::AuthorizeObjectStorage(
    const UUID& writer, const TenantId& tenant, const std::string& key,
    uint64_t size, std::vector<Replica>& replicas) {
    if (!object_uploads_) return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    auto replica =
        std::find_if(replicas.begin(), replicas.end(), [](const auto& replica) {
            return replica.is_object_storage_replica();
        });
    if (replica == replicas.end()) {
        replicas.emplace_back(
            ObjectStorageDescriptor{ObjectStorageGcPrefix(cluster_id_) +
                                        UuidToString(generate_uuid()),
                                    size, ObjectStorageWriteMode::SYNC,
                                    object_storage_config_.NamespaceId()},
            ReplicaStatus::PROCESSING);
        replica = std::prev(replicas.end());
    }
    const auto& descriptor = replica->get_object_storage_descriptor();
    // A replacement reserves its authorization before detaching the old value.
    // InsertMetadata then consumes those exact replicas without authorizing a
    // second PUT.
    auto existing = object_uploads_->FindObjectUpload(descriptor.object_key);
    if (existing) {
        if (existing->writer != writer ||
            existing->tenant_id != tenant.value() || existing->key != key ||
            existing->replica_id != replica->id() ||
            existing->phase != ObjectUploadPhase::AUTHORIZED)
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        return {};
    }
    ObjectUploadRecord record;
    record.target = object_storage_config_.target;
    record.read_lease_ttl_ms = default_kv_lease_ttl_;
    record.tenant_id = tenant.value();
    record.key = key;
    record.writer = writer;
    record.replica_id = replica->id();
    record.object = {descriptor.object_key, size};
    ObjectReplicaMutation mutation;
    mutation.target = record.target;
    mutation.records.push_back(record);
    auto appended = AppendOpLogWithDurableFinalize(
        OpType::OBJECT_STORAGE_MUTATION, record.tenant_id, record.key,
        EncodeObjectMutation(mutation), {});
    if (!appended) return tl::make_unexpected(appended.error());
    return {};
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::AwaitObjectAuthorization(
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> result) {
    if (!result) return result;
    for (const auto& replica : *result) {
        if (!replica.is_object_storage_replica()) continue;
        uint64_t sequence;
        {
            std::lock_guard lock(object_storage_mutex_);
            const auto& name =
                replica.get_object_storage_descriptor().object_key;
            auto it = object_upload_sequences_.find(name);
            if (it == object_upload_sequences_.end())
                return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            sequence = it->second;
        }
        auto durable = WaitObjectDurable(sequence);
        if (!durable) return tl::make_unexpected(durable.error());
    }
    return result;
}

auto MasterService::PutStart(const UUID& writer, const std::string& key,
                             const TenantId& tenant, uint64_t size,
                             const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    return AwaitObjectAuthorization(
        PutStartImpl(writer, key, tenant, size, config));
}
auto MasterService::UpsertStart(const UUID& writer, const std::string& key,
                                const TenantId& tenant, uint64_t size,
                                const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    return AwaitObjectAuthorization(
        UpsertStartImpl(writer, key, tenant, size, config));
}

tl::expected<void, ErrorCode> MasterService::FinishObjectStorageUpload(
    const UUID& writer, const std::string& name, ObjectUploadOutcome outcome,
    const TenantId& tenant) {
    if (outcome != ObjectUploadOutcome::COMPLETE &&
        outcome != ObjectUploadOutcome::NOT_WRITTEN &&
        outcome != ObjectUploadOutcome::UNKNOWN)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    // A leader with object storage disabled can serve only after recovery
    // confirms that there are no uploads left to finish.
    if (!object_uploads_) return {};
    auto record = object_uploads_->FindObjectUpload(name);
    // Forgotten immutable attempts are terminal. A lost completion reply may
    // be retried after GC; acknowledging it cannot authorize another PUT.
    if (!record) return {};
    if (record->writer != writer || record->tenant_id != tenant.value())
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    uint64_t sequence;
    {
        std::shared_lock state_lock(master_state_mutex_);
        MetadataAccessorRW accessor(this, {tenant, record->key});
        record = object_uploads_->FindObjectUpload(name);
        if (!record) return {};
        const auto old_phase = record->phase;
        const auto old_finished = record->upload_finished;
        if (outcome == ObjectUploadOutcome::COMPLETE) {
            if (record->phase != ObjectUploadPhase::RETIRED)
                record->phase = ObjectUploadPhase::UPLOADED;
            record->upload_finished = true;
        } else if (!record->upload_finished) {
            if (record->phase != ObjectUploadPhase::RETIRED) {
                RetireObject(*record, default_kv_lease_ttl_, ObjectNowMs());
            }
            record->upload_finished =
                outcome == ObjectUploadOutcome::NOT_WRITTEN;
        }
        if (record->phase == old_phase &&
            record->upload_finished == old_finished) {
            // Records are applied at enqueue time, before durability. A racing
            // duplicate still owes the caller the original durability barrier.
            std::lock_guard lock(object_storage_mutex_);
            auto it = object_upload_sequences_.find(name);
            // Live records restored from a durable snapshot may have no entry.
            sequence = it == object_upload_sequences_.end() ? 0 : it->second;
        } else {
            ObjectReplicaMutation mutation;
            mutation.target = record->target;
            mutation.records.push_back(*record);
            auto appended = AppendOpLogWithDurableFinalize(
                OpType::OBJECT_STORAGE_MUTATION, tenant.value(), record->key,
                EncodeObjectMutation(mutation), {});
            if (!appended) return tl::make_unexpected(appended.error());
            sequence = appended->sequence_id;
        }
    }
    return WaitObjectDurable(sequence);
}

auto MasterService::PutEnd(const UUID& writer, const ObjectMeta& meta,
                           const TenantId& tenant, ReplicaType type)
    -> tl::expected<void, ErrorCode> {
    if (meta.object_storage_target)
        return CommitObjectStorage(writer, meta, tenant, type);
    return PutEndImpl(writer, meta, tenant, type);
}

tl::expected<void, ErrorCode> MasterService::CommitObjectStorage(
    const UUID& writer, const ObjectMeta& meta, const TenantId& tenant,
    ReplicaType type, std::future<ErrorCode>* deferred) {
    TestFailPoint::Wait("object_before_commit_append");
    auto completion = std::make_shared<ObjectCompletion>();
    auto result = completion->promise.get_future();
    uint64_t sequence;
    {
        std::shared_lock lock(master_state_mutex_);
        MetadataAccessorRW accessor(
            this, MakeObjectIdentityForRequest(meta.key, tenant));
        if (!accessor.Exists())
            return tl::make_unexpected(object_uploads_
                                           ? ErrorCode::OBJECT_NOT_FOUND
                                           : ErrorCode::INVALID_WRITE);
        auto& metadata = accessor.Get();
        if (metadata.client_id != writer ||
            !metadata.object_storage_write_attempt ||
            *metadata.object_storage_write_attempt !=
                *meta.object_storage_target ||
            type != ReplicaType::ALL)
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        if (accessor.GetState().object_storage_commit_pending)
            return tl::make_unexpected(ErrorCode::OBJECT_REPLICA_BUSY);
        if (!accessor.InProcessing()) {
            // The token identifies the committed write even after its first
            // memory replica is unmounted or the new leader disables storage.
            if (metadata.HasReplica(&Replica::fn_is_completed)) return {};
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        auto* replica =
            metadata.GetReplicaByID(meta.object_storage_target->replica_id);
        if (!object_uploads_ || !replica)
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        // Reserve import admission before the durable commit. Abort/timeout may
        // close the group while the upload is in flight; publication must not
        // perform another fallible admission after its metadata is durable.
        auto tracked =
            weight_manager_.TrackImportWrite(tenant, metadata.group_id);
        if (!tracked) return tl::make_unexpected(tracked.error());
        auto admission = std::make_shared<WeightStoreManager::ImportWriteGuard>(
            std::move(*tracked));
        TestFailPoint::Wait("object_after_import_admission");
        std::optional<ObjectUploadRecord> record;
        if (replica->is_object_storage_replica()) {
            record = object_uploads_->FindObjectUpload(
                replica->get_object_storage_descriptor().object_key);
            if (!record || record->phase == ObjectUploadPhase::RETIRED)
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            record->phase = ObjectUploadPhase::UPLOADED;
            record->upload_finished = true;
        }
        std::vector<Replica::Descriptor> descriptors;
        std::unordered_set<ReplicaID> committed_ids;
        for (const auto& target : metadata.GetAllReplicas()) {
            if (target.has_invalid_mem_handle() ||
                target.has_invalid_nof_handle())
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            if (target.status() != ReplicaStatus::PROCESSING)
                return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            auto descriptor = target.get_descriptor();
            descriptor.status = ReplicaStatus::COMPLETE;
            committed_ids.insert(target.id());
            descriptors.push_back(std::move(descriptor));
        }
        MetadataPayload payload;
        auto encoded = SerializeMetadataForOpLogFromReplicaDescriptors(
            metadata, descriptors);
        if (struct_pack::deserialize_to(payload, encoded) !=
            struct_pack::errc::ok)
            throw std::logic_error("Cannot decode own metadata");
        payload.object_checksum = meta.object_checksum;
        if (metadata.pending_session_tag &&
            std::any_of(descriptors.begin(), descriptors.end(),
                        [&](const auto& descriptor) {
                            return metadata.pending_session_tag
                                ->eligible_replica_ids.contains(descriptor.id);
                        }))
            payload.session_id = metadata.pending_session_tag->session_id;
        auto bytes = struct_pack::serialize(payload);
        OpType commit_type = OpType::PUT_END;
        std::string commit_payload(bytes.begin(), bytes.end());
        if (record) {
            ObjectReplicaMutation mutation;
            mutation.target = record->target;
            mutation.records.push_back(*record);
            mutation.metadata_payload = std::move(commit_payload);
            commit_type = OpType::OBJECT_STORAGE_MUTATION;
            commit_payload = EncodeObjectMutation(mutation);
        }
        // Memory-only attempts remain valid after the old cleanup record is
        // forgotten. The ordinary log decorator retires any remaining history
        // atomically, without making completion depend on that history.
        auto appended = AppendOpLogWithDurableFinalize(
            commit_type, tenant.value(), meta.key, std::move(commit_payload),
            [this, writer, meta, tenant, completion, admission,
             committed_ids = std::move(committed_ids)](const OpLogEntry&) {
                TestFailPoint::Wait("object_commit_durable_before_publish");
                auto published =
                    PutEndImpl(writer, meta, tenant, ReplicaType::ALL,
                               &committed_ids, admission.get());
                TestFailPoint::Wait("object_commit_after_publish_before_reply");
                completion->Finish(published ? ErrorCode::OK
                                             : published.error());
            });
        if (!appended) return tl::make_unexpected(appended.error());
        accessor.GetState().object_storage_commit_pending = true;
        sequence = appended->sequence_id;
    }
    [[maybe_unused]] auto failure =
        ordered_oplog_writer_->AwaitDurable(sequence).thenValue(
            [completion](ErrorCode error) {
                if (error != ErrorCode::OK) completion->Finish(error);
            });
    if (deferred) {
        *deferred = std::move(result);
        return {};
    }
    auto error = result.get();
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return {};
}

tl::expected<void, ErrorCode> MasterService::RevokeObjectStorageWrite(
    const UUID& writer, const std::string& key,
    const ObjectWriteAttempt& attempt, const TenantId& tenant) {
    if (!object_uploads_) return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    auto revoke = [&]() -> tl::expected<std::future<ErrorCode>, ErrorCode> {
        std::shared_lock lock(master_state_mutex_);
        MetadataAccessorRW accessor(this,
                                    MakeObjectIdentityForRequest(key, tenant));
        if (!accessor.IsPublished())
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        auto& metadata = accessor.Get();
        if (metadata.client_id != writer ||
            !metadata.GetReplicaByID(attempt.replica_id) ||
            !metadata.object_storage_write_attempt ||
            *metadata.object_storage_write_attempt != attempt ||
            !accessor.InProcessing() ||
            accessor.GetState().object_storage_commit_pending)
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        return BeginObjectWriteRevocation(
            accessor,
            std::chrono::system_clock::now() + put_start_release_timeout_sec_);
    }();
    if (!revoke) return tl::make_unexpected(revoke.error());
    auto error = revoke->get();
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return {};
}

tl::expected<std::future<ErrorCode>, ErrorCode>
MasterService::BeginObjectWriteRevocation(
    MetadataAccessorRW& accessor,
    std::chrono::system_clock::time_point release_at) {
    auto completion = std::make_shared<ObjectCompletion>();
    auto result = completion->promise.get_future();
    // CommitObjectAware retires every active upload for this logical key,
    // including a committed value hidden by a memory-only replacement.
    auto appended = AppendOpLogWithDurableFinalize(
        OpType::PUT_REVOKE, accessor.GetTenantId().value(), accessor.GetKey(),
        {},
        [this, object = accessor.GetRef(), release_at,
         completion](const OpLogEntry& entry) {
            std::shared_lock lock(master_state_mutex_);
            MetadataAccessorRW current(this, TenantId(entry.tenant_id), object);
            if (current.IsPublished()) {
                auto replicas =
                    current.Get().PopReplicas(&Replica::fn_is_processing);
                if (!replicas.empty()) {
                    std::lock_guard discarded_lock(discarded_replicas_mutex_);
                    discarded_replicas_.emplace_back(std::move(replicas),
                                                     release_at);
                }
                current.Erase();
            }
            completion->Finish(ErrorCode::OK);
        });
    if (!appended) return tl::make_unexpected(appended.error());
    accessor.GetState().object_storage_commit_pending = true;
    [[maybe_unused]] auto failure =
        ordered_oplog_writer_->AwaitDurable(appended->sequence_id)
            .thenValue([completion](ErrorCode error) {
                if (error != ErrorCode::OK) completion->Finish(error);
            });
    return result;
}

// object_storage_mutex_ protects admission and both scheduling indexes.
void MasterService::SetObjectWorkLocked(
    const std::string& name,
    std::optional<BackgroundWorker::Clock::time_point> run_at,
    BackgroundWorker::Clock::time_point lease_deadline) {
    auto existing = object_pending_deadlines_.find(name);
    uint64_t retry_delay = object_storage_config_.cleanup_interval_ms;
    if (existing != object_pending_deadlines_.end()) {
        retry_delay = existing->second.retry_delay_ms;
        if (existing->second.retired)
            object_gc_records_.erase(
                {existing->second.retired_not_before_ms, name});
        object_work_.erase({existing->second.run_at, name});
        object_pending_deadlines_.erase(existing);
    }
    if (run_at) {
        const auto record = object_uploads_->FindObjectUpload(name);
        const bool retired =
            record && record->phase == ObjectUploadPhase::RETIRED;
        const uint64_t not_before = retired ? record->not_before_ms : 0;
        if (retired) object_gc_records_.emplace(not_before, name);
        object_pending_deadlines_.emplace(
            name, ObjectPendingWork{*run_at, ++object_work_generation_,
                                    lease_deadline, retry_delay, not_before,
                                    retired});
        object_work_.emplace(*run_at, name);
    }
    UpdateObjectGcMetricsLocked();
}

void MasterService::UpdateObjectGcMetricsLocked() {
    const auto now = ObjectNowMs();
    const auto oldest =
        object_gc_records_.empty() ? now : object_gc_records_.begin()->first;
    MasterMetricManager::instance().set_object_gc_stats(
        object_pending_deadlines_.size() - object_gc_records_.size(),
        object_gc_records_.size(), now > oldest ? now - oldest : 0);
}

void MasterService::ScheduleObjectWorkLocked() {
    auto next = object_gc_scan_at_;
    if (!object_work_.empty())
        next = std::min(next, object_work_.begin()->first);
    object_storage_worker_.ScheduleAt(next);
}

bool MasterService::CanForgetUncertainObject(
    const ObjectUploadRecord& record) const {
    const auto now = ObjectNowMs();
    return object_storage_adapter_ &&
           object_storage_adapter_->SupportsGcListing() &&
           record.object.object_key.starts_with(
               ObjectStorageGcPrefix(cluster_id_)) &&
           now >= record.not_before_ms &&
           now - record.not_before_ms >=
               object_storage_config_.unknown_upload_retention_ms;
}

void MasterService::SweepObjectStorageOrphans() {
    std::string token;
    {
        std::lock_guard lock(object_storage_mutex_);
        if (BackgroundWorker::Clock::now() < object_gc_scan_at_) return;
        token = object_gc_scan_token_;
        object_gc_scan_at_ = BackgroundWorker::Clock::now() +
                             std::chrono::milliseconds(
                                 object_storage_config_.cleanup_max_backoff_ms);
    }
    if (!object_storage_adapter_ ||
        !object_storage_adapter_->SupportsGcListing() ||
        GetRestoreLifecycleState() != RestoreLifecycleState::kServing)
        return;
    const auto prefix = ObjectStorageGcPrefix(cluster_id_);
    auto page = object_storage_adapter_->ListPage(
        prefix, token,
        std::min<uint64_t>(1000,
                           object_storage_config_.maintenance_batch_size));
    if (!page) {
        MasterMetricManager::instance().inc_object_gc_failures();
        LOG(WARNING) << "Object orphan listing failed: "
                     << toString(page.error());
        return;
    }
    ObjectReplicaMutation mutation;
    mutation.target = object_storage_config_.target;
    mutation.orphan_deletes = std::vector<std::string>{};
    const auto now = ObjectNowMs();
    for (const auto& object : page->objects) {
        if (object.logical_key.starts_with(prefix) && object.last_modified_ms &&
            now >= object.last_modified_ms &&
            now - object.last_modified_ms >=
                object_storage_config_.gc_min_object_age_ms &&
            !object_uploads_->FindObjectUpload(object.logical_key))
            mutation.orphan_deletes->push_back(object.logical_key);
    }
    if (!mutation.orphan_deletes->empty()) {
        uint64_t sequence;
        {
            std::shared_lock lock(master_state_mutex_);
            if (GetRestoreLifecycleState() != RestoreLifecycleState::kServing)
                return;
            // CommitObjectAware rechecks the live registry under its ordering
            // mutex. Every valid object replica has a record, including hidden
            // Upsert values. The fenced log prevents a stale leader from using
            // an incomplete registry; immutable names are never reauthorized.
            auto appended = AppendOpLogWithDurableFinalize(
                OpType::OBJECT_STORAGE_MUTATION,
                std::string(TenantId::kDefaultValue), "__object_orphan_gc__",
                EncodeObjectMutation(mutation), {});
            if (!appended) return;
            sequence = appended->sequence_id;
        }
        if (!WaitObjectDurable(sequence)) return;
        for (const auto& name : *mutation.orphan_deletes) {
            if (!AdmitsBusinessWorkers()) return;
            auto removed = object_storage_adapter_->Delete(name);
            if (!removed && removed.error() != ErrorCode::FILE_NOT_FOUND &&
                removed.error() != ErrorCode::OBJECT_NOT_FOUND) {
                MasterMetricManager::instance().inc_object_gc_failures();
                return;
            }
        }
    }
    std::lock_guard lock(object_storage_mutex_);
    object_gc_scan_token_ = std::move(page->next_token);
    object_gc_scan_at_ = BackgroundWorker::Clock::now() +
                         std::chrono::milliseconds(
                             object_gc_scan_token_.empty()
                                 ? object_storage_config_.gc_scan_interval_ms
                                 : 0);
}

void MasterService::MaintainObjectStorage() {
    if (!AdmitsBusinessWorkers()) {
        object_storage_worker_.ScheduleAt(
            BackgroundWorker::Clock::now() +
            std::chrono::milliseconds(
                object_storage_config_.cleanup_interval_ms));
        return;
    }
    for (uint64_t i = 0;
         i < object_storage_config_.maintenance_batch_size &&
         object_storage_worker_.IsRunning() && AdmitsBusinessWorkers();
         ++i) {
        std::string name;
        uint64_t sequence;
        uint64_t generation;
        {
            std::lock_guard lock(object_storage_mutex_);
            if (object_work_.empty() ||
                object_work_.begin()->first > BackgroundWorker::Clock::now())
                break;
            name = object_work_.begin()->second;
            sequence = object_upload_sequences_.at(name);
            generation = object_pending_deadlines_.at(name).generation;
        }
        auto process = [&] {
            auto record = object_uploads_->FindObjectUpload(name);
            if (!record) return;
            const TenantId tenant(record->tenant_id);
            {
                std::shared_lock lock(master_state_mutex_);
                if (!AdmitsBusinessWorkers()) return;
                MetadataAccessorRW accessor(this, {tenant, record->key});
                record = object_uploads_->FindObjectUpload(name);
                if (!record) return;
                bool live = accessor.Exists() &&
                            accessor.Get().GetReplicaByID(record->replica_id);
                auto logical = object_uploads_->GetMetadata(record->tenant_id,
                                                            record->key);
                if (logical) {
                    for (const auto& replica : logical->replicas)
                        if (replica.id == record->replica_id) live = true;
                }
                if (record->phase != ObjectUploadPhase::RETIRED) {
                    if (!live) {
                        RetireObject(*record, default_kv_lease_ttl_,
                                     ObjectNowMs());
                        ObjectReplicaMutation mutation;
                        mutation.target = record->target;
                        mutation.records.push_back(*record);
                        auto appended = AppendOpLogWithDurableFinalize(
                            OpType::OBJECT_STORAGE_MUTATION, record->tenant_id,
                            record->key, EncodeObjectMutation(mutation), {});
                        if (!appended)
                            LOG(WARNING) << "Cannot retire object: "
                                         << toString(appended.error());
                    }
                    return;
                }
                std::lock_guard object_lock(object_storage_mutex_);
                const auto lease_deadline =
                    object_pending_deadlines_.at(name).lease_deadline;
                const auto steady_now = BackgroundWorker::Clock::now();
                // A retirement racing with work selection installed a new
                // deadline.
                if (steady_now < lease_deadline) return;
                const auto wall_now = ObjectNowMs();
                if (record->not_before_ms > wall_now) {
                    // Recheck the persisted deadline if the wall clock was set
                    // back.
                    const auto delay =
                        std::min(record->not_before_ms - wall_now,
                                 object_storage_config_.cleanup_interval_ms);
                    SetObjectWorkLocked(
                        name, steady_now + std::chrono::milliseconds(delay),
                        lease_deadline);
                    return;
                }
                // Capture the fresh retirement sequence under the key lock.
                sequence = object_upload_sequences_.at(name);
            }
            // No destructive side effect for an undurable retirement, and no
            // business locks held across a durability wait or remote DELETE.
            if (!WaitObjectDurable(sequence)) return;
            auto deleted = object_storage_adapter_->Delete(name);
            if (!deleted && deleted.error() != ErrorCode::FILE_NOT_FOUND &&
                deleted.error() != ErrorCode::OBJECT_NOT_FOUND) {
                MasterMetricManager::instance().inc_object_gc_failures();
                LOG(WARNING)
                    << "Object cleanup failed: " << toString(deleted.error());
                return;
            }
            if (!record->upload_finished && !CanForgetUncertainObject(*record))
                return;
            std::shared_lock lock(master_state_mutex_);
            if (!AdmitsBusinessWorkers()) return;
            MetadataAccessorRW accessor(this, {tenant, record->key});
            auto current = object_uploads_->FindObjectUpload(name);
            if (!current || current->phase != ObjectUploadPhase::RETIRED ||
                (!current->upload_finished &&
                 !CanForgetUncertainObject(*current)) ||
                current->not_before_ms > ObjectNowMs())
                return;
            ObjectReplicaMutation mutation;
            mutation.target = current->target;
            if (current->upload_finished)
                mutation.forget.push_back(name);
            else
                mutation.expired_forget = std::vector<std::string>{name};
            auto appended = AppendOpLogWithDurableFinalize(
                OpType::OBJECT_STORAGE_MUTATION, current->tenant_id,
                current->key, EncodeObjectMutation(mutation),
                [](const OpLogEntry&) {
                    TestFailPoint::Wait("object_cleanup_forgotten");
                });
            if (!appended)
                LOG(WARNING) << "Cannot persist object cleanup: "
                             << toString(appended.error());
        };
        process();
        {
            std::lock_guard lock(object_storage_mutex_);
            const auto current = object_pending_deadlines_.find(name);
            // A concurrent completion/retirement has its own new deadline.
            // Do not overwrite it with this older attempt's retry schedule.
            // A duplicate notification can advance the log sequence without
            // changing the work. It must not bypass a failed DELETE's backoff.
            if (current != object_pending_deadlines_.end() &&
                current->second.generation == generation) {
                const auto delay =
                    current->second.retired
                        ? current->second.retry_delay_ms
                        : object_storage_config_.cleanup_interval_ms;
                const auto lease_deadline = current->second.lease_deadline;
                SetObjectWorkLocked(
                    name,
                    std::max(current->second.run_at,
                             BackgroundWorker::Clock::now() +
                                 std::chrono::milliseconds(delay)),
                    lease_deadline);
                object_pending_deadlines_.at(name).retry_delay_ms = std::min(
                    object_storage_config_.cleanup_max_backoff_ms, delay * 2);
            }
        }
    }
    SweepObjectStorageOrphans();
    std::lock_guard lock(object_storage_mutex_);
    UpdateObjectGcMetricsLocked();
    ScheduleObjectWorkLocked();
}

tl::expected<void, ErrorCode> MasterService::RestoreObjectStorageLocked(
    const StandbyMetadataStore* source, const ObjectReplicaSnapshot* snapshot,
    size_t chunk_size) {
    if (!object_uploads_) {
        // The log retains the target identity even after the last forget.
        // Live object descriptors are rejected during metadata restoration.
        if (source ? source->ObjectUploadCount() != 0
                   : !snapshot->records.empty())
            return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
        return {};
    }
    object_uploads_->Clear();
    const auto& target = object_storage_config_.target;
    if (source) {
        if (!object_uploads_->RestoreObjectReplicas(*source, target))
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    } else if ((snapshot->target && *snapshot->target != target) ||
               !object_uploads_->RestoreObjectReplicas(*snapshot)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    bool valid = true;
    VisitNamespaces([&](const TenantId& tenant, metadata::Namespace& ns) {
        for (auto object : ns.objects.ReadCursor()) {
            const auto& metadata = object.metadata();
            for (const auto& replica : metadata.GetAllReplicas()) {
                if (!replica.is_object_storage_replica()) continue;
                const auto& descriptor =
                    replica.get_object_storage_descriptor();
                auto record =
                    object_uploads_->FindObjectUpload(descriptor.object_key);
                const auto& attempt = metadata.object_storage_write_attempt;
                if (record && !record->read_lease_ttl_ms.has_value()) {
                    LOG(ERROR) << "Cannot restore a live object without its "
                                  "prior read lease TTL";
                    valid = false;
                }
                if (!record || record->tenant_id != tenant.value() ||
                    record->key != object.key() ||
                    record->writer != metadata.client_id ||
                    record->replica_id != replica.id() ||
                    record->object.object_size != descriptor.object_size ||
                    record->phase != ObjectUploadPhase::UPLOADED ||
                    !record->upload_finished || !attempt ||
                    attempt->replica_id != replica.id() ||
                    attempt->token != descriptor.object_key)
                    valid = false;
            }
        }
    });
    if (!valid) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    {
        std::lock_guard lock(object_storage_mutex_);
        object_restore_time_ = std::chrono::steady_clock::now();
        object_upload_sequences_.clear();
        object_pending_deadlines_.clear();
        object_work_.clear();
        object_gc_records_.clear();
        object_gc_scan_token_.clear();
        object_gc_scan_at_ = BackgroundWorker::Clock::now();
        UpdateObjectGcMetricsLocked();
    }
    const auto now = ObjectNowMs();
    uint64_t pending_sequence = 0;
    size_t pending_count = 0;
    std::string after;
    for (;;) {
        auto records = object_uploads_->ObjectUploadChunk(after, chunk_size);
        if (records.empty()) break;
        after = records.back().object.object_key;
        for (auto& record : records) {
            if (pending_count && pending_count >= oplog_batch_max_entries_) {
                auto durable = WaitObjectDurable(
                    pending_sequence, restore_repair_durable_timeout_ms_);
                if (!durable) return durable;
                pending_count = 0;
            }
            MetadataAccessorRW accessor(
                this, {TenantId(record.tenant_id), record.key});
            const auto* replica =
                accessor.Exists()
                    ? accessor.Get().GetReplicaByID(record.replica_id)
                    : nullptr;
            if (replica && replica->is_completed()) {
                if (!replica->is_object_storage_replica() ||
                    record.phase != ObjectUploadPhase::UPLOADED ||
                    replica->get_object_storage_descriptor().object_key !=
                        record.object.object_key ||
                    replica->get_object_storage_descriptor().object_size !=
                        record.object.object_size)
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                accessor.Get().GrantReadLease(std::chrono::milliseconds(
                    ObjectLeaseTtl(record, default_kv_lease_ttl_)));
                auto encoded = SerializeMetadataForOpLog(accessor.Get());
                MetadataPayload payload;
                if (struct_pack::deserialize_to(payload, encoded) !=
                        struct_pack::errc::ok ||
                    !object_uploads_->PutMetadata(record.tenant_id, record.key,
                                                  payload.ToStandbyMetadata()))
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                if (*record.read_lease_ttl_ms >= default_kv_lease_ttl_)
                    continue;
                // Persist the larger ceiling before this leader can grant it.
                record.read_lease_ttl_ms = default_kv_lease_ttl_;
            } else if (record.phase == ObjectUploadPhase::RETIRED) {
                // The handoff already includes the durable retirement.
                // Reinstate the previous leader's lease window before allowing
                // deletion.
                std::lock_guard lock(object_storage_mutex_);
                const auto deadline =
                    object_restore_time_ +
                    std::chrono::milliseconds(record.read_lease_ttl_ms.value_or(
                        default_kv_lease_ttl_));
                object_upload_sequences_[record.object.object_key] = 0;
                SetObjectWorkLocked(record.object.object_key, deadline,
                                    deadline);
                continue;
            } else {
                RetireObject(record, default_kv_lease_ttl_, now);
            }
            ObjectReplicaMutation mutation;
            mutation.target = record.target;
            mutation.records.push_back(record);
            auto appended = AppendOpLogWithDurableFinalize(
                OpType::OBJECT_STORAGE_MUTATION, record.tenant_id, record.key,
                EncodeObjectMutation(mutation), {});
            if (!appended) return tl::make_unexpected(appended.error());
            pending_sequence = appended->sequence_id;
            ++pending_count;
        }
    }
    if (pending_count)
        return WaitObjectDurable(pending_sequence,
                                 restore_repair_durable_timeout_ms_);
    return {};
}
tl::expected<OpLogEntry, ErrorCode>
MasterService::AppendReplicaRemovalWithCompletion(
    OrderedOpLogWriter::Reservation&& reservation, OpType type,
    const std::string& tenant, const std::string& key,
    const std::string& payload, std::vector<ReplicaID> removed_ids,
    std::vector<std::shared_future<ErrorCode>>& completions) {
    auto completion = std::make_shared<ObjectCompletion>();
    auto future = completion->promise.get_future().share();
    uint64_t object_sequence = 0;
    auto appended = AppendReservedOpLogWithDurableFinalize(
        std::move(reservation), type, tenant, key, payload,
        [this, completion,
         removed_ids = std::move(removed_ids)](const OpLogEntry& entry) {
            FinalizeRemovedReplicasAfterDurable(entry, removed_ids);
            completion->Finish(ErrorCode::OK);
        },
        &object_sequence);
    if (!appended) return appended;
    // Preserve ordinary HA removals' asynchronous behavior. Object-aware
    // removals already promised a durable result; also wait for local erase.
    if (object_sequence) {
        [[maybe_unused]] auto failure =
            ordered_oplog_writer_->AwaitDurable(object_sequence)
                .thenValue([completion](ErrorCode error) {
                    if (error != ErrorCode::OK) completion->Finish(error);
                });
        completions.push_back(std::move(future));
    }
    return appended;
}

tl::expected<void, ErrorCode> MasterService::WaitReplicaRemovals(
    const std::vector<std::shared_future<ErrorCode>>& completions) {
    auto error = ErrorCode::OK;
    for (const auto& completion : completions) {
        const auto result = completion.get();
        if (error == ErrorCode::OK) error = result;
    }
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return {};
}

auto MasterService::RemoveObject(const std::string& key, const TenantId& tenant,
                                 bool force, bool allow_managed)
    -> tl::expected<void, ErrorCode> {
    std::vector<std::shared_future<ErrorCode>> completions;
    auto result =
        RemoveObjectImpl(key, tenant, force, allow_managed, completions);
    if (!result) return result;
    return WaitReplicaRemovals(completions);
}
}  // namespace mooncake
