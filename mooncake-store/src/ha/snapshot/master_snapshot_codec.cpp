#include "ha/snapshot/master_snapshot_codec.h"

#include <string>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "master_service.h"
#include "segment.h"
#include "serialize/serializer.h"
#include "task_manager.h"

namespace mooncake::ha {

std::vector<uint8_t> MasterSnapshotCodec::EncodeManifest(
    const std::string& type, const std::string& version,
    const std::string& snapshot_id) {
    std::string manifest = type + "|" + version + "|" + snapshot_id;
    return std::vector<uint8_t>(manifest.begin(), manifest.end());
}

tl::expected<MasterSnapshotPayloads, SerializationError>
MasterSnapshotCodec::Encode(
    MasterSnapshotStateView& state_view,
    const WeightMetadataSnapshot* frozen_weight_metadata,
    const std::vector<uint8_t>* frozen_drain_jobs) const {
    MasterSnapshotPayloads payloads;

    // 1. Encode metadata (shards, discarded replicas, replica_next_id)
    auto metadata_result =
        EncodeMetadata(state_view.master_service, frozen_weight_metadata);
    if (!metadata_result) {
        return tl::make_unexpected(metadata_result.error());
    }
    payloads.metadata = std::move(metadata_result.value());

    // 2. Encode segments (memory segments + NoF segments)
    auto segments_result =
        EncodeSegments(state_view.segment_manager, state_view.local_ssd_manager,
                       state_view.nof_segment_manager);
    if (!segments_result) {
        return tl::make_unexpected(segments_result.error());
    }
    payloads.segments = std::move(segments_result.value());

    // 3. Encode task manager
    auto task_manager_result = EncodeTaskManager(state_view.task_manager);
    if (!task_manager_result) {
        return tl::make_unexpected(task_manager_result.error());
    }
    payloads.task_manager = std::move(task_manager_result.value());

    payloads.drain_jobs = frozen_drain_jobs
                              ? *frozen_drain_jobs
                              : EncodeDrainJobs(state_view.master_service);
    return payloads;
}

tl::expected<void, SerializationError> MasterSnapshotCodec::Decode(
    MasterService* master_service,
    const MasterSnapshotPayloads& payloads) const {
    if (master_service == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::INVALID_PARAMS, "master_service is null"));
    }

    // Decode() is the codec-level exception boundary for restore. Most
    // serializer failures are already reported as SerializationError, but a
    // few MessagePack conversions (e.g. TaskManagerSerializer::Deserialize()
    // calling arr[0].as<std::string>() on a structurally valid but
    // wrongly-typed field) can still throw msgpack::type_error outside their
    // local try blocks. Since the caller RestoreState() no longer wraps each
    // candidate in a try/catch, any escaping exception would abort restore and
    // prevent fallback to an older healthy snapshot. Converting all exceptions
    // here into SerializationError preserves that per-candidate fallback.
    try {
        // 1. Decode segments first. A MEMORY replica's allocator is bound to
        //    its mounted segment, so the segment/allocator must be restored
        //    before metadata; otherwise GetMountedSegment() returns
        //    SEGMENT_NOT_FOUND while deserializing the replica.
        auto segments_result =
            DecodeSegments(master_service, payloads.segments);
        if (!segments_result) {
            return tl::make_unexpected(segments_result.error());
        }

        // 2. Decode metadata (shards, discarded replicas, replica_next_id)
        auto metadata_result =
            DecodeMetadata(master_service, payloads.metadata);
        if (!metadata_result) {
            return tl::make_unexpected(metadata_result.error());
        }

        // 3. Decode task manager
        auto task_manager_result =
            DecodeTaskManager(master_service, payloads.task_manager);
        if (!task_manager_result) {
            return tl::make_unexpected(task_manager_result.error());
        }

        DecodeDrainJobs(*master_service, payloads.drain_jobs);
        return {};
    } catch (const std::exception& e) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            std::string("exception during snapshot decode: ") + e.what()));
    } catch (...) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "unknown exception during snapshot decode"));
    }
}

tl::expected<std::vector<uint8_t>, SerializationError>
MasterSnapshotCodec::EncodeMetadata(
    MasterService& master_service,
    const WeightMetadataSnapshot* frozen_weight_metadata) const {
    // Delegate to the existing MetadataSerializer for now.
    // This maintains the exact same format as before.
    MasterService::MetadataSerializer serializer(&master_service);
    return serializer.Serialize(frozen_weight_metadata);
}

tl::expected<void, SerializationError> MasterSnapshotCodec::DecodeMetadata(
    MasterService* master_service, const std::vector<uint8_t>& data) const {
    // Delegate to the existing MetadataSerializer for now.
    MasterService::MetadataSerializer serializer(master_service);
    return serializer.Deserialize(data);
}

tl::expected<std::vector<uint8_t>, SerializationError>
MasterSnapshotCodec::EncodeSegments(
    SegmentManager& segment_manager, LocalSsdManager& local_ssd_manager,
    NoFSegmentManager& nof_segment_manager) const {
    // Note: NoFSegmentManager is not currently serialized in snapshots
    SegmentSerializer serializer(&segment_manager);
    return serializer.Serialize(local_ssd_manager.ExportPersistedState());
}

tl::expected<void, SerializationError> MasterSnapshotCodec::DecodeSegments(
    MasterService* master_service, const std::vector<uint8_t>& data) const {
    SegmentSerializer serializer(&master_service->segment_manager_);
    auto local_ssd_state = serializer.Deserialize(data);
    if (!local_ssd_state) {
        return tl::unexpected(local_ssd_state.error());
    }
    master_service->local_ssd_manager_.RestorePersistedState(
        std::move(*local_ssd_state));
    return {};
}

tl::expected<std::vector<uint8_t>, SerializationError>
MasterSnapshotCodec::EncodeTaskManager(ClientTaskManager& task_manager) const {
    // Use the existing TaskManagerSerializer
    TaskManagerSerializer serializer(&task_manager);
    return serializer.Serialize();
}

tl::expected<void, SerializationError> MasterSnapshotCodec::DecodeTaskManager(
    MasterService* master_service, const std::vector<uint8_t>& data) const {
    // Access the task manager from MasterService
    TaskManagerSerializer serializer(&master_service->task_manager_);
    return serializer.Deserialize(data);
}

namespace {
// Snapshot records contain values only; never mutexes or allocator pointers.
struct DrainReplicationRecord {
    std::string tenant, key;
    int32_t type;
    std::string client;
    int64_t started;
    ReplicaID source;
    std::vector<ReplicaID> targets;
    uint64_t pending_bytes;
    std::string dynamic_lease;
    uint64_t dynamic_version;
    bool cleanup_pending;
    MSGPACK_DEFINE(tenant, key, type, client, started, source, targets,
                   pending_bytes, dynamic_lease, dynamic_version,
                   cleanup_pending);
};
struct DrainTaskRecord {
    std::string id, tenant, key, source, target;
    uint64_t bytes;
    std::string unit;
    MSGPACK_DEFINE(id, tenant, key, source, target, bytes, unit);
};
struct DrainJobRecord {
    std::string id;
    int32_t type, status;
    std::vector<std::string> sources, targets;
    uint32_t concurrency;
    int64_t created, updated;
    std::string message;
    uint64_t succeeded, failed, blocked, bytes;
    std::vector<DrainTaskRecord> active;
    std::unordered_set<std::string> completed;
    std::unordered_map<std::string, uint32_t> retries;
    std::unordered_set<std::string> terminal;
    MSGPACK_DEFINE(id, type, status, sources, targets, concurrency, created,
                   updated, message, succeeded, failed, blocked, bytes, active,
                   completed, retries, terminal);
};

int64_t DrainTimeMs(std::chrono::system_clock::time_point time) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               time.time_since_epoch())
        .count();
}
void RequireDrain(bool condition) {
    if (!condition) throw std::runtime_error("invalid drain job snapshot");
}
UUID DrainUuid(const std::string& value, bool allow_nil = false) {
    UUID id;
    RequireDrain(StringToUuid(value, id) && (allow_nil || id != UUID{}));
    return id;
}
std::chrono::system_clock::time_point DrainTime(int64_t value) {
    RequireDrain(value >= 0 &&
                 value <=
                     DrainTimeMs(std::chrono::system_clock::time_point::max()));
    return std::chrono::system_clock::time_point(
        std::chrono::milliseconds(value));
}
}  // namespace

std::vector<uint8_t> MasterSnapshotCodec::EncodeDrainJobs(
    MasterService& service) const {
    std::vector<DrainJobRecord> records;
    std::unordered_set<std::string> active_objects;
    std::lock_guard lock(service.job_mutex_);
    for (const auto& [id, job] : service.drain_jobs_) {
        std::lock_guard job_lock(job->mutex);
        DrainJobRecord record{UuidToString(id),
                              static_cast<int32_t>(job->type),
                              static_cast<int32_t>(job->status),
                              job->request.segments,
                              job->request.target_segments,
                              job->request.max_concurrency,
                              DrainTimeMs(job->created_at),
                              DrainTimeMs(job->last_updated_at),
                              job->message,
                              job->succeeded_units,
                              job->failed_units,
                              job->blocked_units,
                              job->migrated_bytes,
                              {},
                              job->completed_unit_keys,
                              job->retry_counts,
                              job->terminal_failed_unit_keys};
        for (const auto& [task_id, task] : job->active_tasks) {
            record.active.push_back({UuidToString(task_id),
                                     task.tenant_id.value(), task.key,
                                     task.source_segment, task.target_segment,
                                     task.bytes, task.unit_key});
            active_objects.insert(
                service.MakeDrainUnitKey(task.tenant_id, task.key, ""));
        }
        records.push_back(std::move(record));
    }
    // A client can issue CopyStart/MoveStart independently of the queued
    // drain task, including before any unit is scheduled. Preserve per-object
    // runtime for every retained draining object, including orphaned sources.
    std::unordered_set<std::string> draining_segments;
    {
        auto segments = service.segment_manager_.getSegmentAccess();
        std::vector<std::string> names;
        segments.GetAllSegmentNames(names);
        for (const auto& name : names) {
            SegmentStatus status;
            if (segments.GetSegmentStatusByName(name, status) ==
                    ErrorCode::OK &&
                status == SegmentStatus::DRAINING)
                draining_segments.insert(name);
        }
    }
    std::vector<DrainReplicationRecord> replication;
    for (size_t i = 0; i < MasterService::kNumShards; ++i) {
        MasterService::MetadataShardAccessorRO shard(&service, i);
        for (const auto& [tenant, state] : shard->tenants) {
            for (const auto& [key, runtime] : state.replication_tasks) {
                const auto metadata = state.metadata.find(key);
                if (metadata == state.metadata.end()) continue;
                const auto names = metadata->second.GetReplicaSegmentNames();
                if (!active_objects.contains(
                        service.MakeDrainUnitKey(tenant, key, "")) &&
                    std::none_of(names.begin(), names.end(),
                                 [&](const auto& name) {
                                     return draining_segments.contains(name);
                                 }))
                    continue;
                replication.push_back(
                    {tenant.value(), key, static_cast<int32_t>(runtime.type),
                     UuidToString(runtime.client_id),
                     DrainTimeMs(runtime.start_time), runtime.source_id,
                     runtime.replica_ids, runtime.pending_quota_charge_bytes,
                     UuidToString(runtime.dynamic_replication_lease_id),
                     runtime.dynamic_replication_version_epoch,
                     runtime.durable_cleanup_pending});
            }
        }
    }
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_array(2);
    packer.pack(records);
    packer.pack(replication);
    return {reinterpret_cast<const uint8_t*>(buffer.data()),
            reinterpret_cast<const uint8_t*>(buffer.data()) + buffer.size()};
}

void MasterSnapshotCodec::DecodeDrainJobs(
    MasterService& service,
    const std::optional<std::vector<uint8_t>>& data) const {
    decltype(service.drain_jobs_) jobs;
    struct ReplicationRestore {
        TenantId tenant;
        std::string key;
        ReplicationTask runtime;
    };
    std::vector<ReplicationRestore> replication;
    std::unordered_set<UUID, boost::hash<UUID>> task_ids;
    std::unordered_set<std::string> owned_sources;
    std::unordered_set<std::string> active_objects;
    std::unordered_set<std::string> replication_objects;
    if (data) {
        size_t offset = 0;
        auto root = msgpack::unpack(
            reinterpret_cast<const char*>(data->data()), data->size(), offset,
            nullptr, nullptr,
            msgpack::unpack_limit(data->size(), data->size(), data->size(),
                                  data->size(), data->size(), 16));
        RequireDrain(offset == data->size() &&
                     root.get().type == msgpack::type::ARRAY &&
                     root.get().via.array.size == 2);
        const auto* sections = root.get().via.array.ptr;
        RequireDrain(sections[0].type == msgpack::type::ARRAY &&
                     sections[1].type == msgpack::type::ARRAY);
        for (const auto& object :
             sections[0].as<std::vector<msgpack::object>>()) {
            RequireDrain(object.type == msgpack::type::ARRAY &&
                         object.via.array.size == 17);
            const auto* fields = object.via.array.ptr;
            RequireDrain(fields[13].type == msgpack::type::ARRAY);
            for (uint32_t i = 0; i < fields[13].via.array.size; ++i) {
                const auto& active = fields[13].via.array.ptr[i];
                RequireDrain(active.type == msgpack::type::ARRAY &&
                             active.via.array.size == 7);
            }
            const auto record = object.as<DrainJobRecord>();
            RequireDrain(fields[14].type == msgpack::type::ARRAY &&
                         fields[14].via.array.size == record.completed.size() &&
                         fields[15].type == msgpack::type::MAP &&
                         fields[15].via.map.size == record.retries.size() &&
                         fields[16].type == msgpack::type::ARRAY &&
                         fields[16].via.array.size == record.terminal.size());
            auto job = std::make_shared<MasterService::DrainJob>();
            job->id = DrainUuid(record.id);
            RequireDrain(
                record.type == static_cast<int32_t>(JobType::DRAIN) &&
                record.status >= static_cast<int32_t>(JobStatus::CREATED) &&
                record.status <= static_cast<int32_t>(JobStatus::CANCELED));
            job->status = static_cast<JobStatus>(record.status);
            job->request = {record.sources, record.targets, record.concurrency};
            const std::unordered_set<std::string> sources(
                record.sources.begin(), record.sources.end());
            RequireDrain(!sources.empty() &&
                         sources.size() == record.sources.size() &&
                         !sources.contains("") && record.concurrency > 0);
            for (const auto& target : record.targets)
                RequireDrain(!target.empty() && !sources.contains(target));
            const bool terminal = job->status >= JobStatus::SUCCEEDED;
            if (!terminal) {
                for (const auto& source : sources)
                    RequireDrain(owned_sources.insert(source).second);
            }
            job->created_at = DrainTime(record.created);
            job->last_updated_at = DrainTime(record.updated);
            job->message = record.message;
            job->succeeded_units = record.succeeded;
            job->failed_units = record.failed;
            job->blocked_units = record.blocked;
            job->migrated_bytes = record.bytes;
            job->completed_unit_keys = record.completed;
            job->retry_counts = record.retries;
            job->terminal_failed_unit_keys = record.terminal;
            RequireDrain(record.succeeded == record.completed.size() &&
                         record.failed >= record.terminal.size() &&
                         (!terminal || record.active.empty()) &&
                         record.active.size() <= record.concurrency);
            for (const auto& key : record.terminal)
                RequireDrain(!record.completed.contains(key));
            uint64_t retries = 0;
            for (const auto& [key, count] : record.retries) {
                RequireDrain(count > 0 &&
                             count <= MasterService::kMaxDrainUnitRetries &&
                             count <= record.failed - retries);
                retries += count;
            }
            std::unordered_set<std::string> units;
            for (const auto& active : record.active) {
                const auto task_id = DrainUuid(active.id);
                const TenantId tenant(active.tenant);
                RequireDrain(tenant.IsValid());
                RequireDrain(
                    task_ids.insert(task_id).second &&
                    units.insert(active.unit).second &&
                    sources.contains(active.source) &&
                    active.target != active.source && !active.target.empty() &&
                    active.unit == service.MakeDrainUnitKey(tenant, active.key,
                                                            active.source) &&
                    !record.completed.contains(active.unit) &&
                    !record.terminal.contains(active.unit));
                active_objects.insert(
                    service.MakeDrainUnitKey(tenant, active.key, ""));
                RequireDrain(record.targets.empty() ||
                             std::find(record.targets.begin(),
                                       record.targets.end(),
                                       active.target) != record.targets.end());
                const auto task =
                    service.task_manager_.get_read_access().find_task_by_id(
                        task_id);
                if (task) {
                    RequireDrain(task->type == TaskType::REPLICA_MOVE &&
                                 task->status >= TaskStatus::PENDING &&
                                 task->status <= TaskStatus::SUCCESS);
                    ReplicaMovePayload payload;
                    struct_json::from_json(payload, task->payload);
                    RequireDrain(payload.tenant_id == active.tenant &&
                                 payload.key == active.key &&
                                 payload.source == active.source &&
                                 payload.target == active.target);
                }
                job->active_tasks.emplace(
                    task_id,
                    MasterService::ActiveDrainTask{
                        task_id, tenant, active.key, active.source,
                        active.target, static_cast<size_t>(active.bytes),
                        active.unit});
            }
            RequireDrain(jobs.emplace(job->id, std::move(job)).second);
        }
        for (const auto& object :
             sections[1].as<std::vector<msgpack::object>>()) {
            RequireDrain(object.type == msgpack::type::ARRAY &&
                         object.via.array.size == 11);
            const auto saved = object.as<DrainReplicationRecord>();
            const TenantId tenant(saved.tenant);
            RequireDrain(tenant.IsValid());
            const auto identity =
                service.MakeDrainUnitKey(tenant, saved.key, "");
            RequireDrain(replication_objects.insert(identity).second &&
                         (saved.type == static_cast<int32_t>(
                                            ReplicationTask::Type::COPY) ||
                          saved.type == static_cast<int32_t>(
                                            ReplicationTask::Type::MOVE)));
            MasterService::MetadataAccessorRW metadata(&service,
                                                       {tenant, saved.key});
            // Metadata may have disappeared when invalid replicas were
            // pruned during decode. There is no retained object to resume.
            if (!metadata.Exists()) continue;
            RequireDrain(!metadata.HasReplicationTask());
            const auto names = metadata.Get().GetReplicaSegmentNames();
            RequireDrain(
                active_objects.contains(identity) ||
                std::any_of(names.begin(), names.end(), [&](const auto& name) {
                    auto status = service.QuerySegmentStatus(name);
                    return status && *status == SegmentStatus::DRAINING;
                }));
            RequireDrain(saved.type != static_cast<int32_t>(
                                           ReplicationTask::Type::MOVE) ||
                         saved.targets.size() <= 1);
            std::unordered_set<ReplicaID> targets;
            for (const auto id : saved.targets) {
                // A target (or source) can disappear while a replication
                // task remains live. CopyEnd/MoveEnd already handle that
                // partial failure; retain IDs and pending charge for them.
                RequireDrain(targets.insert(id).second && id != saved.source);
            }
            const auto size = metadata.Get().size;
            RequireDrain(size != 0 &&
                         saved.targets.size() <=
                             std::numeric_limits<uint64_t>::max() / size &&
                         saved.pending_bytes == saved.targets.size() * size);
            replication.push_back(
                {tenant,
                 saved.key,
                 {DrainUuid(saved.client, true), DrainTime(saved.started),
                  static_cast<ReplicationTask::Type>(saved.type), saved.source,
                  saved.targets, saved.pending_bytes,
                  DrainUuid(saved.dynamic_lease, true), saved.dynamic_version,
                  saved.cleanup_pending}});
        }
    }
    // All records are validated before publication. Restore runs before
    // workers.
    for (auto& restored : replication) {
        MasterService::MetadataAccessorRW metadata(
            &service, {restored.tenant, restored.key});
        RequireDrain(!metadata.HasReplicationTask());
        if (auto* source =
                metadata.Get().GetReplicaByID(restored.runtime.source_id)) {
            source->inc_refcnt();
        }
        metadata.GetTenantState().replication_tasks.emplace(
            restored.key, std::move(restored.runtime));
    }
    std::lock_guard lock(service.job_mutex_);
    service.drain_jobs_ = std::move(jobs);
}

}  // namespace mooncake::ha
