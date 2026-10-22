#include "master_service.h"

#include <algorithm>
#include <async_simple/coro/SyncAwait.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/FutureAwaiter.h>

#include "ha/standby_metadata_store.h"
#include "master_metric_manager.h"

namespace mooncake {
ErrorCode MasterService::AwaitRecoveryDurability(uint64_t sequence) {
    return async_simple::coro::syncAwait(
        [this, sequence]() -> async_simple::coro::Lazy<ErrorCode> {
            co_return co_await ordered_oplog_writer_->AwaitDurable(sequence);
        }());
}

ErrorCode MasterService::PersistRecoveryEntry(OpType type,
                                              const std::string& payload,
                                              const std::string& key,
                                              const std::string& tenant) {
    if (!enable_oplog_) return ErrorCode::OK;
    auto sequence = AppendOpLogVisibleBeforeDurable(type, tenant, key, payload);
    if (!sequence) return sequence.error();
    // AwaitDurable completes independently of finalizer callbacks. Callers may
    // hold a task/job/shard lock that an earlier finalizer still needs.
    return AwaitRecoveryDurability(*sequence);
}

ErrorCode MasterService::PersistDrainJobUpdate(const DrainJobUpdate& update) {
    const auto key = update.job ? UuidToString(update.job->id)
                                : update.segments.front().name;
    return PersistRecoveryEntry(OpType::DRAIN_JOB_UPDATE,
                                EncodeRecoveryValue(update), key);
}

ErrorCode MasterService::PersistRecoveryTasks(
    std::span<const DurableTaskRecord> tasks) {
    if (!enable_oplog_ || tasks.empty()) return ErrorCode::OK;
    std::vector<OpLogEntry> entries;
    entries.reserve(tasks.size());
    for (const auto& task : tasks) {
        OpLogEntry entry;
        entry.op_type = OpType::TASK_UPDATE;
        entry.object_key = UuidToString(task.id);
        entry.payload = EncodeRecoveryValue(task);
        entries.push_back(std::move(entry));
    }
    auto sequence =
        AppendOpLogBatchWithDurableFinalize(std::move(entries), nullptr);
    return sequence ? AwaitRecoveryDurability(*sequence) : sequence.error();
}

ErrorCode MasterService::PersistReplicationStart(
    MetadataAccessorRW& accessor,
    std::optional<OrderedOpLogWriter::Reservation>& reservation) {
    if (!enable_oplog_) return ErrorCode::OK;
    auto& metadata = accessor.Get();
    const auto& task = accessor.GetReplicationTask();
    ReplicationRecoveryRecord record{
        metadata.tenant_id.value(),
        metadata.user_key,
        task.client_id,
        static_cast<int32_t>(task.type),
        std::chrono::duration_cast<std::chrono::milliseconds>(
            task.start_time.time_since_epoch())
            .count(),
        task.source_id,
        task.replica_ids,
        task.pending_quota_charge_bytes,
        task.dynamic_replication_lease_id,
        task.dynamic_replication_version_epoch};
    std::vector<DrainSegmentRecord> segments;
    {
        auto access = segment_manager_.getSegmentAccess();
        segments = DescribeDrainSegments(
            access, metadata.GetReplicaSegmentNames(), std::nullopt);
    }
    auto persisted = AppendReservedOpLogWithDurableFinalize(
        std::move(*reservation), OpType::REPLICATION_START,
        metadata.tenant_id.value(), metadata.user_key,
        EncodeRecoveryValue(ReplicationStartRecord{
            record, SerializeMetadataForOpLog(metadata), std::move(segments)}),
        nullptr);
    if (!persisted) {
        // Submission was rejected: no client has received the new targets.
        const auto targets = task.replica_ids;
        const auto pending = task.pending_quota_charge_bytes;
        if (auto* source = metadata.GetReplicaByID(task.source_id))
            source->dec_refcnt();
        metadata.ForgetDynamicReplicas(targets);
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&](const Replica& replica) {
                return std::find(targets.begin(), targets.end(),
                                 replica.id()) != targets.end();
            });
        ReleaseTenantQuota(accessor.QuotaAccount(), pending);
        accessor.EraseReplicationTask();
        dynamic_replication_.ClearPendingLocked(
            accessor.GetTenantId(), metadata.user_key, accessor.GetState());
        return persisted.error();
    }
    // A failed admitted write can have an uncertain outcome. Retain its
    // buffers on the failed leader rather than allowing them to be reused.
    return AwaitRecoveryDurability(persisted->sequence_id);
}

ErrorCode MasterService::PersistReplicationRevoke(
    MetadataAccessorRW& accessor) {
    if (!enable_oplog_) return ErrorCode::OK;
    auto& metadata = accessor.Get();
    const auto& task = accessor.GetReplicationTask();
    std::vector<Replica::Descriptor> remaining;
    metadata.VisitReplicas(
        [&](const Replica& replica) {
            return replica.is_completed() &&
                   !replica.has_invalid_mem_handle() &&
                   std::find(task.replica_ids.begin(), task.replica_ids.end(),
                             replica.id()) == task.replica_ids.end();
        },
        [&](const Replica& replica) {
            remaining.push_back(replica.get_descriptor());
        });
    return PersistRecoveryEntry(
        remaining.empty() ? OpType::REMOVE : OpType::PUT_END,
        remaining.empty() ? std::string()
                          : SerializeMetadataForOpLogFromReplicaDescriptors(
                                metadata, remaining, {}, true),
        metadata.user_key, metadata.tenant_id.value());
}

DrainJobRecord MasterService::DescribeDrainJob(const DrainJob& job) {
    const auto milliseconds = [](auto time) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   time.time_since_epoch())
            .count();
    };
    return {job.id,
            static_cast<int32_t>(job.status),
            job.request.segments,
            job.request.target_segments,
            job.request.max_concurrency,
            milliseconds(job.created_at),
            milliseconds(job.last_updated_at),
            job.message,
            job.succeeded_units,
            job.failed_units,
            job.blocked_units,
            job.migrated_bytes};
}

void MasterService::PublishDrainJob(DrainJob& job,
                                    const DrainJobRecord& record) {
    job.id = record.id;
    job.status = static_cast<JobStatus>(record.status);
    job.request = {record.sources, record.targets, record.concurrency};
    job.created_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(record.created_ms));
    job.last_updated_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(record.updated_ms));
    job.message = record.message;
    job.succeeded_units = record.succeeded;
    job.failed_units = record.failed;
    job.blocked_units = record.blocked;
    job.migrated_bytes = record.migrated_bytes;
}

std::vector<DrainSegmentRecord> MasterService::DescribeDrainSegments(
    ScopedSegmentAccess& access, const std::vector<std::string>& names,
    std::optional<SegmentStatus> status) {
    std::vector<std::pair<Segment, UUID>> segments;
    access.GetAllSegments(segments);
    std::vector<DrainSegmentRecord> records;
    for (const auto& [segment, client] : segments) {
        if (std::find(names.begin(), names.end(), segment.name) !=
            names.end()) {
            SegmentStatus current;
            access.GetSegmentStatusById(segment.id, current);
            if (status == SegmentStatus::OK &&
                current >= SegmentStatus::GRACEFULLY_UNMOUNTING)
                continue;
            records.push_back({segment.id, client, segment.name,
                               segment.te_endpoint, segment.base, segment.size,
                               static_cast<int32_t>(
                                   status.value_or(SegmentStatus::UNDEFINED))});
        }
    }
    for (const auto& name : names) {
        if (std::any_of(
                records.begin(), records.end(),
                [&](const auto& record) { return record.name == name; }))
            continue;
        if (auto previous = restored_drain_segments_.find(name);
            previous != restored_drain_segments_.end()) {
            if (status == SegmentStatus::OK &&
                previous->second.status >=
                    static_cast<int32_t>(SegmentStatus::GRACEFULLY_UNMOUNTING))
                continue;
            DrainSegmentRecord record = previous->second;
            record.status =
                static_cast<int32_t>(status.value_or(SegmentStatus::UNDEFINED));
            records.push_back(std::move(record));
        }
    }
    return records;
}

ErrorCode MasterService::SetDrainSegmentStatus(ScopedSegmentAccess& access,
                                               const std::string& name,
                                               SegmentStatus status) {
    const auto error = access.SetSegmentStatusByName(name, status);
    auto restored = restored_drain_segments_.find(name);
    if (restored != restored_drain_segments_.end() &&
        (error == ErrorCode::OK || error == ErrorCode::SEGMENT_NOT_FOUND)) {
        restored->second.status = static_cast<int32_t>(status);
        return ErrorCode::OK;
    }
    return error;
}

ErrorCode MasterService::GetDrainSegmentStatus(ScopedSegmentAccess& access,
                                               const std::string& name,
                                               SegmentStatus& status) const {
    const auto error = access.GetSegmentStatusByName(name, status);
    if (error != ErrorCode::OK) {
        if (auto restored = restored_drain_segments_.find(name);
            restored != restored_drain_segments_.end()) {
            status = static_cast<SegmentStatus>(restored->second.status);
            return ErrorCode::OK;
        }
    }
    return error;
}

tl::expected<void, ErrorCode> MasterService::RestoreReplicationRecovery(
    const StandbyMetadataStore& store) {
    for (const auto& [key, value] : store.DrainRecovery().records()) {
        if (value.kind != RecoveryRecordKind::Replication) continue;
        ReplicationRecoveryRecord runtime;
        if (!DecodeRecoveryValue(value.payload, runtime)) {
            LOG(ERROR) << "Invalid replication recovery record: " << key;
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }
        auto standby_metadata = store.GetMetadata(runtime.tenant, runtime.key);
        if (!standby_metadata ||
            !ValidateReplicationRecovery(runtime, standby_metadata->replicas,
                                         standby_metadata->size, true)) {
            LOG(ERROR) << "Invalid replication source/targets/quota: " << key;
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }
        MetadataAccessorRW accessor(
            this, MakeObjectIdentityForRequest(runtime.key,
                                               TenantId(runtime.tenant)));
        // An authoritative unmount can remove every replica of this object.
        // Its existing task history still drives normal job failure/retry.
        if (!accessor.Exists()) continue;
        auto& metadata = accessor.Get();
        accessor.GetState().replication_task.emplace(
            runtime.client_id,
            std::chrono::system_clock::time_point(
                std::chrono::milliseconds(runtime.started_ms)),
            static_cast<ReplicationTask::Type>(runtime.type), runtime.source_id,
            runtime.targets, runtime.pending_bytes, runtime.dynamic_lease,
            runtime.dynamic_version);
        if (auto* source = metadata.GetReplicaByID(runtime.source_id)) {
            source->inc_refcnt();
        }
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::RestoreDrainRecovery(
    const DrainRecoveryState& state, int64_t restore_epoch_ms) {
    decltype(drain_jobs_) jobs;
    std::vector<Task> tasks;
    std::vector<
        std::pair<std::vector<Replica>, std::chrono::system_clock::time_point>>
        retired;
    for (const auto& [key, value] : state.records()) {
        if (value.kind == RecoveryRecordKind::Job) {
            DrainJobRecord record;
            if (!DecodeRecoveryValue(value.payload, record) ||
                !ValidateDrainJob(record)) {
                LOG(ERROR) << "Invalid drain job header: " << key;
                return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
            }
            auto job = std::make_shared<DrainJob>();
            PublishDrainJob(*job, record);
            jobs.emplace(record.id, std::move(job));
        } else if (value.kind == RecoveryRecordKind::Task) {
            DurableTaskRecord record;
            if (!DecodeRecoveryValue(value.payload, record) ||
                !ValidateDurableTask(record)) {
                LOG(ERROR) << "Invalid task history or drain payload: " << key;
                return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
            }
            tasks.push_back(FromDurableTask(record));
        } else if (value.kind == RecoveryRecordKind::RetiredReplica) {
            RetiredReplicaRecord record;
            if (!DecodeRecoveryValue(value.payload, record) ||
                !record.replica.is_memory_replica()) {
                LOG(ERROR) << "Invalid retired source replica: " << key;
                return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
            }
            if (record.release_ms <= restore_epoch_ms) continue;
            const auto& descriptor =
                record.replica.get_memory_descriptor().buffer_descriptor;
            auto allocator = standby_allocator_keepalive_.find(
                descriptor.transport_endpoint_);
            if (allocator == standby_allocator_keepalive_.end()) continue;
            auto owner = std::find_if(
                restored_drain_segments_.begin(),
                restored_drain_segments_.end(), [&](const auto& segment) {
                    return segment.second.endpoint ==
                               descriptor.transport_endpoint_ ||
                           segment.second.name ==
                               descriptor.transport_endpoint_;
                });
            if (owner == restored_drain_segments_.end()) {
                return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
            }
            auto buffer = std::make_unique<AllocatedBuffer>(allocator->second,
                                                            descriptor);
            buffer->bindSegmentLifetime(owner->second.lifetime);
            buffer->bindClientLiveness(
                client_liveness_records_.at(owner->second.client_id));
            std::vector<Replica> replicas;
            replicas.push_back(Replica(record.replica.id, std::move(buffer),
                                       record.replica.status));
            retired.emplace_back(
                std::move(replicas),
                std::chrono::system_clock::time_point(
                    std::chrono::milliseconds(record.release_ms)));
        }
    }
    for (const auto& [key, value] : state.records()) {
        if (value.kind != RecoveryRecordKind::Unit) continue;
        DrainUnitRecord unit;
        if (!DecodeRecoveryValue(value.payload, unit) ||
            !ValidateDrainUnit(unit) || !jobs.contains(unit.job_id)) {
            LOG(ERROR) << "Invalid drain unit or missing parent job: " << key;
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }
        auto& job = *jobs.at(unit.job_id);
        if (std::find(job.request.segments.begin(), job.request.segments.end(),
                      unit.source) == job.request.segments.end() ||
            (!job.request.target_segments.empty() &&
             std::find(job.request.target_segments.begin(),
                       job.request.target_segments.end(),
                       unit.target) == job.request.target_segments.end())) {
            LOG(ERROR) << "Drain unit source/target outside job request: "
                       << key;
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }
        if (unit.completed) job.completed_unit_keys.insert(unit.unit_key);
        if (unit.terminal_failed) {
            job.terminal_failed_unit_keys.insert(unit.unit_key);
        }
        if (unit.retries != 0)
            job.retry_counts.emplace(unit.unit_key, unit.retries);
        if (unit.task_id != UUID{}) {
            job.active_tasks.emplace(
                unit.task_id,
                ActiveDrainTask{unit.task_id, TenantId(unit.tenant), unit.key,
                                unit.source, unit.target, unit.bytes,
                                unit.unit_key});
        }
    }
    for (const auto& [id, job] : jobs) {
        uint64_t retries = 0;
        for (const auto& [key, count] : job->retry_counts) retries += count;
        if (job->succeeded_units != job->completed_unit_keys.size() ||
            job->failed_units < job->terminal_failed_unit_keys.size() ||
            retries > job->failed_units ||
            job->active_tasks.size() > job->request.max_concurrency ||
            (job->status >= JobStatus::SUCCEEDED &&
             !job->active_tasks.empty())) {
            LOG(ERROR) << "Inconsistent drain job counters or active tasks: "
                       << UuidToString(id);
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }
    }
    std::unordered_set<std::string> owned_sources;
    for (const auto& [id, job] : jobs) {
        if (job->status < JobStatus::SUCCEEDED) {
            owned_sources.insert(job->request.segments.begin(),
                                 job->request.segments.end());
        }
    }
    for (const auto& [name, segment] : restored_drain_segments_) {
        if (segment.status == static_cast<int32_t>(SegmentStatus::DRAINING) &&
            !owned_sources.contains(name)) {
            LOG(WARNING)
                << "Recovered orphaned DRAINING segment=" << name
                << "; kept non-allocatable. Finish outstanding moves before "
                   "PUT /api/v1/segments/status.";
            MasterMetricManager::instance().inc_orphaned_draining_restore();
        }
    }
    std::sort(tasks.begin(), tasks.end(), [](const Task& a, const Task& b) {
        return std::tie(a.last_updated_at, a.id) <
               std::tie(b.last_updated_at, b.id);
    });
    {
        std::lock_guard lock(discarded_replicas_mutex_);
        for (auto& [replicas, release_at] : retired) {
            discarded_replicas_.emplace_back(std::move(replicas), release_at);
        }
    }
    std::lock_guard lock(job_mutex_);
    auto access = task_manager_.get_write_access();
    access.clear_all();
    for (auto& task : tasks) access.restore_task(std::move(task));
    drain_jobs_ = std::move(jobs);
    return {};
}

void MasterService::WarnOrphanedDrainingSegments() {
    std::unordered_set<std::string> owned;
    {
        std::lock_guard lock(job_mutex_);
        for (const auto& [id, job] : drain_jobs_) {
            std::lock_guard job_lock(job->mutex);
            if (job->status < JobStatus::SUCCEEDED) {
                owned.insert(job->request.segments.begin(),
                             job->request.segments.end());
            }
        }
    }
    const auto warn = [&](const std::string& name, uint64_t size) {
        if (!owned.contains(name)) {
            LOG(WARNING) << "Orphaned DRAINING segment=" << name
                         << " stranded_capacity_bytes=" << size
                         << "; finish outstanding transfers and use PUT "
                            "/api/v1/segments/status to resume allocation.";
        }
    };
    auto access = segment_manager_.getSegmentAccess();
    std::vector<std::pair<Segment, UUID>> mounted;
    access.GetAllSegments(mounted);
    std::unordered_set<std::string> names;
    for (const auto& [segment, client] : mounted) {
        names.insert(segment.name);
        SegmentStatus status;
        access.GetSegmentStatusById(segment.id, status);
        if (status == SegmentStatus::DRAINING) warn(segment.name, segment.size);
    }
    for (const auto& [name, segment] : restored_drain_segments_) {
        if (!names.contains(name) &&
            segment.status == static_cast<int32_t>(SegmentStatus::DRAINING)) {
            warn(name, segment.size);
        }
    }
}

void MasterService::ForgetRestoredSegments(
    const std::vector<DrainSegmentRecord>& segments) {
    if (segments.empty()) return;
    std::unique_lock lock(master_state_mutex_);
    for (const auto& segment : segments) {
        if (auto accounted = standby_accounted_memory_bytes_.find(segment.name);
            accounted != standby_accounted_memory_bytes_.end()) {
            MasterMetricManager::instance().dec_allocated_mem_size(
                segment.name, static_cast<int64_t>(accounted->second));
            standby_accounted_memory_bytes_.erase(accounted);
        }
        standby_allocator_keepalive_.erase(segment.name);
        standby_allocator_keepalive_.erase(segment.endpoint);
        invalid_replica_endpoints_.erase(segment.name);
        invalid_replica_endpoints_.erase(segment.endpoint);
        std::erase_if(standby_memory_segments_, [&](const auto& candidate) {
            return candidate.segment_name == segment.name &&
                   candidate.transport_endpoint == segment.endpoint;
        });
    }
}

}  // namespace mooncake
