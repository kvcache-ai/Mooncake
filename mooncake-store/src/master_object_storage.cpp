#include "master_service.h"

#include <future>
#include <stdexcept>

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

void MasterService::InitObjectStorage(const MasterServiceConfig& config) {
    object_storage_config_ = config.object_storage;
    if (!object_storage_config_.enabled) return;
    if (!object_storage_config_.Validate() || !enable_ha_ || !enable_oplog_ ||
        !ordered_oplog_writer_ || view_version_ == 0 || enable_dfs_ ||
        !root_fs_dir_.empty()) {
        throw std::invalid_argument(
            "Object replicas require a valid target including an explicit "
            "object_storage_region, fenced HA ordered-log recovery with DFS "
            "and legacy shared-file replicas disabled");
    }
    auto adapter = CreateObjectReplicaAdapter(object_storage_config_);
    if (!adapter)
        throw std::runtime_error("Cannot initialize object replica adapter: " +
                                 toString(adapter.error()));
    object_storage_adapter_ = *adapter;
    object_uploads_ = std::make_unique<StandbyMetadataStore>();
}

tl::expected<ObjectStorageConfig, ErrorCode>
MasterService::GetObjectStorageConfig() {
    return object_storage_config_;
}

tl::expected<void, ErrorCode> MasterService::WaitObjectDurable(
    uint64_t sequence) {
    auto done = std::make_shared<std::promise<ErrorCode>>();
    auto result = done->get_future();
    [[maybe_unused]] auto waiter =
        ordered_oplog_writer_->AwaitDurable(sequence).thenValue(
            [done](ErrorCode error) { done->set_value(error); });
    auto error = result.get();
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return {};
}

// The caller holds its metadata key. This mutex serializes record preparation
// with log ordering across keys; no network I/O or durability wait occurs here.
tl::expected<OrderedOpLogWriter::PendingHandle, ErrorCode>
MasterService::CommitObjectAware(OrderedOpLogWriter::Reservation&& reservation,
                                 OpLogEntry entry,
                                 DurableFinalizeCallback callback) {
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
                record.phase = ObjectUploadPhase::RETIRED;
                record.not_before_ms =
                    std::max(record.not_before_ms,
                             ObjectNowMs() + default_kv_lease_ttl_);
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
                record.phase = ObjectUploadPhase::RETIRED;
                record.not_before_ms =
                    std::max(record.not_before_ms,
                             ObjectNowMs() + default_kv_lease_ttl_);
            }
            mutation.records.push_back(std::move(record));
        }
        entry.payload = EncodeObjectMutation(mutation);
    }
    size_t new_records = 0;
    for (const auto& record : mutation.records)
        if (!object_uploads_->FindObjectUpload(record.object.object_key))
            ++new_records;
    if (new_records &&
        (object_pending_deadlines_.size() >=
             object_storage_config_.max_pending_records ||
         new_records > object_storage_config_.max_pending_records -
                           object_pending_deadlines_.size()))
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
    object_storage_last_sequence_ = pending->sequence_id();
    for (const auto& record : mutation.records)
        object_upload_sequences_[record.object.object_key] =
            pending->sequence_id();
    const auto logical =
        object_uploads_->GetMetadata(entry.tenant_id, entry.object_key);
    const auto now = ObjectNowMs();
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
        if (!object_pending_deadlines_.contains(name) ||
            old == old_records.end() || old->second.phase != record.phase ||
            old->second.not_before_ms != record.not_before_ms ||
            old->second.upload_finished != record.upload_finished) {
            SetObjectWorkLocked(
                name, record.phase == ObjectUploadPhase::RETIRED
                          ? std::max(now, record.not_before_ms)
                          : now + object_storage_config_.cleanup_interval_ms);
        }
    }
    for (const auto& name : mutation.forget) {
        object_upload_sequences_.erase(name);
        SetObjectWorkLocked(name, std::nullopt);
    }
    ScheduleObjectWorkLocked();
    if (!mutation.forget.empty() && !object_uploads_->FirstObjectUploadForKey(
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
            ObjectStorageDescriptor{UuidToString(generate_uuid()), size,
                                    ObjectStorageWriteMode::SYNC,
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
    if (!object_uploads_) return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    if (outcome != ObjectUploadOutcome::COMPLETE &&
        outcome != ObjectUploadOutcome::NOT_STARTED &&
        outcome != ObjectUploadOutcome::UNKNOWN)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
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
        if (outcome == ObjectUploadOutcome::COMPLETE) {
            if (record->phase != ObjectUploadPhase::RETIRED)
                record->phase = ObjectUploadPhase::UPLOADED;
            record->upload_finished = true;
        } else if (!record->upload_finished) {
            if (record->phase != ObjectUploadPhase::RETIRED) {
                record->phase = ObjectUploadPhase::RETIRED;
                record->not_before_ms =
                    std::max(record->not_before_ms,
                             ObjectNowMs() + default_kv_lease_ttl_);
            }
            record->upload_finished =
                outcome == ObjectUploadOutcome::NOT_STARTED;
        }
        ObjectReplicaMutation mutation;
        mutation.target = record->target;
        mutation.records.push_back(*record);
        auto appended = AppendOpLogWithDurableFinalize(
            OpType::OBJECT_STORAGE_MUTATION, tenant.value(), record->key,
            EncodeObjectMutation(mutation), {});
        if (!appended) return tl::make_unexpected(appended.error());
        sequence = appended->sequence_id;
    }
    return WaitObjectDurable(sequence);
}

auto MasterService::PutEnd(const UUID& writer, const ObjectMeta& meta,
                           const TenantId& tenant, ReplicaType type)
    -> tl::expected<void, ErrorCode> {
    if (meta.object_storage_target)
        return CommitObjectStorage(writer, meta, tenant, type);
    // Object writes cannot bypass their exact-attempt completion protocol.
    if (object_uploads_) {
        std::shared_lock lock(master_state_mutex_);
        MetadataAccessorRW accessor(
            this, MakeObjectIdentityForRequest(meta.key, tenant));
        if (accessor.Exists() && accessor.Get().object_storage_write_attempt)
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }
    return PutEndImpl(writer, meta, tenant, type);
}

tl::expected<void, ErrorCode> MasterService::CommitObjectStorage(
    const UUID& writer, const ObjectMeta& meta, const TenantId& tenant,
    ReplicaType type) {
    if (!object_uploads_) return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    TestFailPoint::Wait("object_before_commit_append");
    auto completion = std::make_shared<ObjectCompletion>();
    auto result = completion->promise.get_future();
    uint64_t sequence;
    {
        std::shared_lock lock(master_state_mutex_);
        MetadataAccessorRW accessor(
            this, MakeObjectIdentityForRequest(meta.key, tenant));
        if (!accessor.Exists())
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        auto& metadata = accessor.Get();
        auto* replica =
            metadata.GetReplicaByID(meta.object_storage_target->replica_id);
        if (metadata.client_id != writer || !replica ||
            !metadata.object_storage_write_attempt ||
            *metadata.object_storage_write_attempt !=
                *meta.object_storage_target ||
            type != ReplicaType::ALL)
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        if (accessor.GetState().object_storage_commit_pending)
            return tl::make_unexpected(ErrorCode::OBJECT_REPLICA_BUSY);
        if (replica->is_completed()) return {};
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
        for (const auto& target : metadata.GetAllReplicas()) {
            if (target.has_invalid_mem_handle() ||
                target.has_invalid_nof_handle())
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            if (target.status() != ReplicaStatus::PROCESSING)
                return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            auto descriptor = target.get_descriptor();
            descriptor.status = ReplicaStatus::COMPLETE;
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
            [this, writer, meta, tenant, completion,
             admission](const OpLogEntry&) {
                TestFailPoint::Wait("object_commit_durable_before_publish");
                auto published =
                    PutEndImpl(writer, meta, tenant, ReplicaType::ALL, false,
                               admission.get());
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
    auto error = result.get();
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return {};
}

tl::expected<void, ErrorCode> MasterService::RevokeObjectStorageWrite(
    const UUID& writer, const std::string& key,
    const ObjectWriteAttempt& attempt, const TenantId& tenant) {
    const auto replica_id = attempt.replica_id;
    if (!object_uploads_) return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    auto completion = std::make_shared<ObjectCompletion>();
    auto result = completion->promise.get_future();
    uint64_t sequence;
    {
        std::shared_lock lock(master_state_mutex_);
        MetadataAccessorRW accessor(this,
                                    MakeObjectIdentityForRequest(key, tenant));
        if (!accessor.Exists())
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        auto& metadata = accessor.Get();
        auto* replica = metadata.GetReplicaByID(replica_id);
        if (metadata.client_id != writer || !replica ||
            !metadata.object_storage_write_attempt ||
            *metadata.object_storage_write_attempt != attempt ||
            !accessor.InProcessing() ||
            accessor.GetState().object_storage_commit_pending)
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        OpType revoke_type = OpType::PUT_REVOKE;
        std::string revoke_payload;
        if (replica->is_object_storage_replica()) {
            auto record = object_uploads_->FindObjectUpload(
                replica->get_object_storage_descriptor().object_key);
            if (!record) return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            if (record->phase != ObjectUploadPhase::RETIRED) {
                record->phase = ObjectUploadPhase::RETIRED;
                record->not_before_ms =
                    std::max(record->not_before_ms,
                             ObjectNowMs() + default_kv_lease_ttl_);
            }
            ObjectReplicaMutation mutation;
            mutation.target = record->target;
            mutation.records.push_back(*record);
            mutation.remove_value = true;
            revoke_type = OpType::OBJECT_STORAGE_MUTATION;
            revoke_payload = EncodeObjectMutation(mutation);
        }
        auto appended = AppendOpLogWithDurableFinalize(
            revoke_type, tenant.value(), key, std::move(revoke_payload),
            [this, tenant, key, replica_id, completion](const OpLogEntry&) {
                std::shared_lock lock(master_state_mutex_);
                MetadataAccessorRW current(
                    this, MakeObjectIdentityForRequest(key, tenant));
                if (current.Exists() &&
                    current.Get().GetReplicaByID(replica_id)) {
                    auto replicas =
                        current.Get().PopReplicas(&Replica::fn_is_processing);
                    if (!replicas.empty()) {
                        std::lock_guard discarded_lock(
                            discarded_replicas_mutex_);
                        discarded_replicas_.emplace_back(
                            std::move(replicas),
                            std::chrono::system_clock::now() +
                                put_start_release_timeout_sec_);
                    }
                    current.Erase();
                }
                completion->Finish(ErrorCode::OK);
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
    auto error = result.get();
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return {};
}

// object_storage_mutex_ protects admission and both scheduling indexes.
void MasterService::SetObjectWorkLocked(const std::string& name,
                                        std::optional<uint64_t> deadline) {
    auto existing = object_pending_deadlines_.find(name);
    if (existing != object_pending_deadlines_.end()) {
        object_work_.erase({existing->second.deadline_ms, name});
        object_pending_deadlines_.erase(existing);
    }
    if (deadline) {
        object_pending_deadlines_.emplace(
            name, ObjectPendingWork{*deadline, ++object_work_generation_});
        object_work_.emplace(*deadline, name);
    }
}

void MasterService::ScheduleObjectWorkLocked() {
    if (object_work_.empty()) return;
    const auto now = ObjectNowMs();
    const auto due = object_work_.begin()->first;
    const auto delay = due > now ? due - now : 0;
    object_storage_worker_.ScheduleAt(BackgroundWorker::Clock::now() +
                                      std::chrono::milliseconds(delay));
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
                object_work_.begin()->first > ObjectNowMs())
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
                        record->phase = ObjectUploadPhase::RETIRED;
                        record->not_before_ms =
                            std::max(record->not_before_ms,
                                     ObjectNowMs() + default_kv_lease_ttl_);
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
                if (record->not_before_ms > ObjectNowMs()) return;
                // Capture the fresh retirement sequence under the key lock.
                std::lock_guard object_lock(object_storage_mutex_);
                sequence = object_upload_sequences_.at(name);
            }
            // No destructive side effect for an undurable retirement, and no
            // business locks held across a durability wait or remote DELETE.
            if (!WaitObjectDurable(sequence)) return;
            auto deleted = object_storage_adapter_->Delete(name);
            if (!deleted && deleted.error() != ErrorCode::FILE_NOT_FOUND &&
                deleted.error() != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(WARNING)
                    << "Object cleanup failed: " << toString(deleted.error());
                return;
            }
            if (!record->upload_finished) return;
            std::shared_lock lock(master_state_mutex_);
            if (!AdmitsBusinessWorkers()) return;
            MetadataAccessorRW accessor(this, {tenant, record->key});
            auto current = object_uploads_->FindObjectUpload(name);
            if (!current || current->phase != ObjectUploadPhase::RETIRED ||
                !current->upload_finished ||
                current->not_before_ms > ObjectNowMs())
                return;
            ObjectReplicaMutation mutation;
            mutation.target = current->target;
            mutation.forget.push_back(name);
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
                current->second.generation == generation)
                SetObjectWorkLocked(
                    name,
                    std::max(current->second.deadline_ms,
                             ObjectNowMs() +
                                 object_storage_config_.cleanup_interval_ms));
        }
    }
    std::lock_guard lock(object_storage_mutex_);
    ScheduleObjectWorkLocked();
}

tl::expected<void, ErrorCode> MasterService::RestoreObjectStorage(
    const ObjectReplicaSnapshot& snapshot) {
    if (!object_uploads_)
        return snapshot.target
                   ? tl::expected<void, ErrorCode>(
                         tl::make_unexpected(ErrorCode::NOT_SUPPORTED))
                   : tl::expected<void, ErrorCode>{};
    if (snapshot.target && *snapshot.target != object_storage_config_.target)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    if (!object_uploads_->RestoreObjectReplicas(snapshot))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
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
        object_upload_sequences_.clear();
        object_storage_last_sequence_ = 0;
        object_pending_deadlines_.clear();
        object_work_.clear();
    }
    if (!snapshot.target) return {};
    const auto deadline = ObjectNowMs() + default_kv_lease_ttl_;
    for (auto record : snapshot.records) {
        std::shared_lock lock(master_state_mutex_);
        MetadataAccessorRW accessor(this,
                                    {TenantId(record.tenant_id), record.key});
        const auto* replica =
            accessor.Exists() ? accessor.Get().GetReplicaByID(record.replica_id)
                              : nullptr;
        if (replica && replica->is_completed()) {
            accessor.Get().GrantReadLease(
                std::chrono::milliseconds(default_kv_lease_ttl_));
            if (!replica->is_object_storage_replica() ||
                record.phase != ObjectUploadPhase::UPLOADED ||
                replica->get_object_storage_descriptor().object_key !=
                    record.object.object_key ||
                replica->get_object_storage_descriptor().object_size !=
                    record.object.object_size)
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            auto encoded = SerializeMetadataForOpLog(accessor.Get());
            MetadataPayload payload;
            if (struct_pack::deserialize_to(payload, encoded) !=
                    struct_pack::errc::ok ||
                !object_uploads_->PutMetadata(record.tenant_id, record.key,
                                              payload.ToStandbyMetadata()))
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        record.phase = ObjectUploadPhase::RETIRED;
        record.not_before_ms = std::max(record.not_before_ms, deadline);
        ObjectReplicaMutation mutation;
        mutation.target = record.target;
        mutation.records.push_back(record);
        auto appended = AppendOpLogWithDurableFinalize(
            OpType::OBJECT_STORAGE_MUTATION, record.tenant_id, record.key,
            EncodeObjectMutation(mutation), {});
        if (!appended) return tl::make_unexpected(appended.error());
    }
    return {};
}
tl::expected<void, ErrorCode> MasterService::AwaitObjectMutations() {
    uint64_t sequence;
    {
        std::lock_guard lock(object_storage_mutex_);
        sequence = object_storage_last_sequence_;
    }
    return sequence ? WaitObjectDurable(sequence)
                    : tl::expected<void, ErrorCode>{};
}

auto MasterService::RemoveObject(const std::string& key, const TenantId& tenant,
                                 bool force, bool allow_managed)
    -> tl::expected<void, ErrorCode> {
    auto result = RemoveObjectImpl(key, tenant, force, allow_managed);
    if (result && object_uploads_) return AwaitObjectMutations();
    return result;
}
}  // namespace mooncake
