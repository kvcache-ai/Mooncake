#include "drain_recovery.h"

#include <algorithm>
#include <chrono>
#include <unordered_set>

#include "rpc_types.h"
#include "segment/status.h"
#include "task_manager.h"

namespace mooncake {
namespace {
bool ValidTime(int64_t value) {
    return value >= 0 &&
           value <= std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::time_point::max()
                            .time_since_epoch())
                        .count();
}
}  // namespace

std::string DrainRecoveryKey(RecoveryRecordKind kind, const std::string& key) {
    return std::to_string(static_cast<int>(kind)) + ":" + key;
}

std::string DrainUnitKey(const std::string& tenant, const std::string& key,
                         const std::string& source) {
    return std::to_string(tenant.size()) + ":" + tenant + ":" +
           std::to_string(key.size()) + ":" + key + ":" + source;
}

bool ValidateDrainJob(const DrainJobRecord& job) {
    if (job.id == UUID{} || job.sources.empty() || job.concurrency == 0 ||
        job.status < static_cast<int32_t>(JobStatus::CREATED) ||
        job.status > static_cast<int32_t>(JobStatus::CANCELED) ||
        !ValidTime(job.created_ms) || !ValidTime(job.updated_ms)) {
        return false;
    }
    const std::unordered_set<std::string> sources(job.sources.begin(),
                                                  job.sources.end());
    if (sources.size() != job.sources.size()) return false;
    return std::none_of(
        job.targets.begin(), job.targets.end(),
        [&](const auto& target) { return sources.contains(target); });
}

bool ValidateDrainUnit(const DrainUnitRecord& unit) {
    return unit.job_id != UUID{} && TenantId(unit.tenant).IsValid() &&
           unit.source != unit.target &&
           unit.unit_key == DrainUnitKey(unit.tenant, unit.key, unit.source) &&
           unit.retries <= kMaxDrainUnitRetries &&
           !(unit.completed && unit.terminal_failed) &&
           (!(unit.completed || unit.terminal_failed) ||
            unit.task_id == UUID{});
}

bool ValidateDurableTask(const DurableTaskRecord& task) {
    if (task.id == UUID{}) return false;
    if (task.removed) return true;
    if (!ValidTime(task.created_ms) || !ValidTime(task.updated_ms) ||
        task.type < static_cast<int32_t>(TaskType::REPLICA_COPY) ||
        task.type > static_cast<int32_t>(TaskType::REPLICA_MOVE) ||
        task.status < static_cast<int32_t>(TaskStatus::PENDING) ||
        task.status > static_cast<int32_t>(TaskStatus::SUCCESS))
        return false;
    if (!task.drain) return true;
    const auto& unit = *task.drain;
    if (!ValidateDrainUnit(unit) || unit.task_id != task.id ||
        task.type != static_cast<int32_t>(TaskType::REPLICA_MOVE))
        return false;
    ReplicaMovePayload payload;
    try {
        struct_json::from_json(payload, task.payload);
    } catch (const std::runtime_error&) {
        return false;
    }
    return payload.tenant_id == unit.tenant && payload.key == unit.key &&
           payload.source == unit.source && payload.target == unit.target;
}

bool ValidateReplicationRecovery(
    const ReplicationRecoveryRecord& runtime,
    const std::vector<Replica::Descriptor>& replicas, uint64_t object_size,
    bool allow_missing_replicas) {
    if (!TenantId(runtime.tenant).IsValid() || object_size == 0 ||
        !ValidTime(runtime.started_ms) || runtime.source_id == 0 ||
        runtime.type < 0 || runtime.type > 1 ||
        (runtime.type == 1 && runtime.targets.size() > 1))
        return false;
    std::unordered_set<ReplicaID> targets(runtime.targets.begin(),
                                          runtime.targets.end());
    if (targets.size() != runtime.targets.size() || targets.contains(0) ||
        targets.contains(runtime.source_id))
        return false;
    if (!targets.empty() && object_size > UINT64_MAX / targets.size())
        return false;
    // Full target allocation is charged until COPY/MOVE settles. Changing
    // this billing model requires translating these versioned records.
    if (runtime.pending_bytes != object_size * targets.size()) return false;
    bool source_found = false;
    size_t targets_found = 0;
    for (const auto& replica : replicas) {
        if (replica.id == runtime.source_id) {
            if (replica.status != ReplicaStatus::COMPLETE) return false;
            source_found = true;
        }
        if (targets.contains(replica.id)) {
            if (!replica.is_memory_replica() ||
                replica.status != ReplicaStatus::PROCESSING ||
                replica.get_memory_descriptor().buffer_descriptor.size_ !=
                    object_size)
                return false;
            ++targets_found;
        }
    }
    return allow_missing_replicas ||
           (source_found && targets_found == targets.size());
}

bool DrainRecoveryState::Restore(const DrainRecoveryRecord& record) {
    bool valid = false;
    std::string identity;
    switch (record.kind) {
        case RecoveryRecordKind::Job: {
            DrainJobRecord job;
            valid = DecodeRecoveryValue(record.payload, job) &&
                    ValidateDrainJob(job);
            identity = UuidToString(job.id);
            break;
        }
        case RecoveryRecordKind::Unit: {
            DrainUnitRecord unit;
            valid = DecodeRecoveryValue(record.payload, unit) &&
                    ValidateDrainUnit(unit);
            identity = UuidToString(unit.job_id) + ":" + unit.unit_key;
            break;
        }
        case RecoveryRecordKind::Task: {
            DurableTaskRecord task;
            valid = DecodeRecoveryValue(record.payload, task) &&
                    !task.removed && ValidateDurableTask(task);
            identity = UuidToString(task.id);
            break;
        }
        case RecoveryRecordKind::Segment: {
            DrainSegmentRecord segment;
            valid =
                DecodeRecoveryValue(record.payload, segment) &&
                !segment.endpoint.empty() && segment.size > 0 &&
                segment.status >= static_cast<int32_t>(SegmentStatus::OK) &&
                segment.status <= static_cast<int32_t>(SegmentStatus::DRAINED);
            identity = segment.name;
            break;
        }
        case RecoveryRecordKind::Replication: {
            ReplicationRecoveryRecord replication;
            valid = DecodeRecoveryValue(record.payload, replication) &&
                    TenantId(replication.tenant).IsValid() &&
                    replication.source_id != 0 && replication.type >= 0 &&
                    replication.type <= 1 && ValidTime(replication.started_ms);
            identity =
                TenantId(replication.tenant).MakeScopedKey(replication.key);
            break;
        }
        case RecoveryRecordKind::RetiredReplica: {
            RetiredReplicaRecord retired;
            valid = DecodeRecoveryValue(record.payload, retired) &&
                    retired.replica.id != 0 && ValidTime(retired.release_ms);
            identity = std::to_string(retired.replica.id);
            break;
        }
    }
    return valid && record.key == DrainRecoveryKey(record.kind, identity) &&
           records_.emplace(record.key, record).second;
}

bool DrainRecoveryState::Apply(const DrainJobUpdate& update) {
    // Validate every value before publishing any part of this event.
    DrainRecoveryState prepared;
    if (update.job && !prepared.Restore(MakeRecoveryRecord(
                          RecoveryRecordKind::Job, UuidToString(update.job->id),
                          *update.job)))
        return false;
    if (update.unit &&
        (!prepared.Restore(MakeRecoveryRecord(
             RecoveryRecordKind::Unit,
             UuidToString(update.unit->job_id) + ":" + update.unit->unit_key,
             *update.unit)) ||
         (update.job && update.job->id != update.unit->job_id)))
        return false;
    for (auto segment : update.segments) {
        if (segment.status == static_cast<int32_t>(SegmentStatus::UNDEFINED)) {
            segment.status = static_cast<int32_t>(SegmentStatus::OK);
            const auto key =
                DrainRecoveryKey(RecoveryRecordKind::Segment, segment.name);
            if (auto previous = records_.find(key);
                previous != records_.end()) {
                DrainSegmentRecord old;
                if (!DecodeRecoveryValue(previous->second.payload, old))
                    return false;
                segment.status = old.status;
            }
        }
        if (!prepared.Restore(MakeRecoveryRecord(RecoveryRecordKind::Segment,
                                                 segment.name, segment))) {
            return false;
        }
    }
    for (auto& [key, record] : prepared.records_) {
        records_.insert_or_assign(key, std::move(record));
    }
    return true;
}

bool DrainRecoveryState::Apply(const DurableTaskRecord& task) {
    if (!ValidateDurableTask(task)) return false;
    const auto key =
        DrainRecoveryKey(RecoveryRecordKind::Task, UuidToString(task.id));
    if (task.removed) {
        records_.erase(key);
        return true;
    }
    const bool created = !records_.contains(key);
    if (created && task.drain) {
        const auto job_key = DrainRecoveryKey(RecoveryRecordKind::Job,
                                              UuidToString(task.drain->job_id));
        if (!records_.contains(job_key)) return false;
        DrainJobRecord job;
        if (!DecodeRecoveryValue(records_.at(job_key).payload, job) ||
            job.status >= static_cast<int32_t>(JobStatus::SUCCEEDED))
            return false;
        auto unit = MakeRecoveryRecord(
            RecoveryRecordKind::Unit,
            UuidToString(task.drain->job_id) + ":" + task.drain->unit_key,
            *task.drain);
        if (auto previous = records_.find(unit.key);
            previous != records_.end()) {
            DrainUnitRecord old;
            if (!DecodeRecoveryValue(previous->second.payload, old) ||
                old.task_id != UUID{} || old.completed || old.terminal_failed ||
                old.retries != task.drain->retries)
                return false;
        }
        if (job.status < static_cast<int32_t>(JobStatus::RUNNING)) {
            job.status = static_cast<int32_t>(JobStatus::RUNNING);
            job.updated_ms = task.created_ms;
            job.message = "Drain job running";
            records_.at(job_key) = MakeRecoveryRecord(
                RecoveryRecordKind::Job, UuidToString(job.id), job);
        }
        records_.insert_or_assign(unit.key, std::move(unit));
    }
    records_.insert_or_assign(
        key, MakeRecoveryRecord(RecoveryRecordKind::Task, UuidToString(task.id),
                                task));
    return true;
}

bool DrainRecoveryState::Apply(const ReplicationRecoveryRecord& replication) {
    auto record = MakeRecoveryRecord(
        RecoveryRecordKind::Replication,
        TenantId(replication.tenant).MakeScopedKey(replication.key),
        replication);
    DrainRecoveryState prepared;
    if (!prepared.Restore(record)) return false;
    records_.insert_or_assign(record.key, std::move(record));
    return true;
}

void DrainRecoveryState::EraseReplication(const std::string& tenant,
                                          const std::string& key) {
    records_.erase(DrainRecoveryKey(RecoveryRecordKind::Replication,
                                    TenantId(tenant).MakeScopedKey(key)));
}

void DrainRecoveryState::EraseSegment(const std::string& name,
                                      const std::string& endpoint) {
    records_.erase(DrainRecoveryKey(RecoveryRecordKind::Segment, name));
    const auto prefix =
        DrainRecoveryKey(RecoveryRecordKind::RetiredReplica, "");
    for (auto it = records_.lower_bound(prefix);
         it != records_.end() && it->first.starts_with(prefix);) {
        RetiredReplicaRecord retired;
        if (DecodeRecoveryValue(it->second.payload, retired) &&
            retired.replica.is_memory_replica() &&
            retired.replica.get_memory_descriptor()
                    .buffer_descriptor.transport_endpoint_ == endpoint) {
            it = records_.erase(it);
        } else
            ++it;
    }
}

bool DrainRecoveryState::Retire(const RetiredReplicaRecord& replica) {
    auto record =
        MakeRecoveryRecord(RecoveryRecordKind::RetiredReplica,
                           std::to_string(replica.replica.id), replica);
    DrainRecoveryState prepared;
    if (!prepared.Restore(record)) return false;
    records_.insert_or_assign(record.key, std::move(record));
    return true;
}

void DrainRecoveryState::PruneRetired(int64_t now_ms) {
    const auto prefix =
        DrainRecoveryKey(RecoveryRecordKind::RetiredReplica, "");
    for (auto it = records_.lower_bound(prefix);
         it != records_.end() && it->first.starts_with(prefix);) {
        RetiredReplicaRecord retired;
        if (DecodeRecoveryValue(it->second.payload, retired) &&
            retired.release_ms <= now_ms)
            it = records_.erase(it);
        else
            ++it;
    }
}

}  // namespace mooncake
