#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake {

// Persist values only. Runtime locks, callbacks and allocator pointers stay
// local.
struct DrainJobRecord {
    UUID id{};
    int32_t status{0};
    std::vector<std::string> sources, targets;
    uint32_t concurrency{0};
    int64_t created_ms{0}, updated_ms{0};
    std::string message;
    uint64_t succeeded{0}, failed{0}, blocked{0}, migrated_bytes{0};
    YLT_REFL(DrainJobRecord, id, status, sources, targets, concurrency,
             created_ms, updated_ms, message, succeeded, failed, blocked,
             migrated_bytes);
};

struct DrainUnitRecord {
    UUID job_id{}, task_id{};
    std::string unit_key, tenant, key, source, target;
    uint64_t bytes{0};
    uint32_t retries{0};
    bool completed{false}, terminal_failed{false};
    YLT_REFL(DrainUnitRecord, job_id, task_id, unit_key, tenant, key, source,
             target, bytes, retries, completed, terminal_failed);
};

struct DrainSegmentRecord {
    UUID id{}, client_id{};
    std::string name, endpoint;
    uint64_t base{0}, size{0};
    // UNDEFINED updates registration identity without changing durable status.
    int32_t status{0};
    YLT_REFL(DrainSegmentRecord, id, client_id, name, endpoint, base, size,
             status);
};

struct DrainJobUpdate {
    std::optional<DrainJobRecord> job;
    std::optional<DrainUnitRecord> unit;
    std::vector<DrainSegmentRecord> segments;
    YLT_REFL(DrainJobUpdate, job, unit, segments);
};

struct DurableTaskRecord {
    UUID id{}, client_id{};
    int32_t type{0}, status{0};
    std::string payload, message;
    int64_t created_ms{0}, updated_ms{0};
    uint32_t max_retries{0};
    std::optional<DrainUnitRecord> drain;
    bool removed{false};
    YLT_REFL(DurableTaskRecord, id, client_id, type, status, payload, message,
             created_ms, updated_ms, max_retries, drain, removed);
};

struct ReplicationRecoveryRecord {
    std::string tenant, key;
    UUID client_id{};
    int32_t type{0};
    int64_t started_ms{0};
    ReplicaID source_id{0};
    std::vector<ReplicaID> targets;
    uint64_t pending_bytes{0};
    UUID dynamic_lease{};
    uint64_t dynamic_version{0};
    YLT_REFL(ReplicationRecoveryRecord, tenant, key, client_id, type,
             started_ms, source_id, targets, pending_bytes, dynamic_lease,
             dynamic_version);
};

struct ReplicationStartRecord {
    ReplicationRecoveryRecord runtime;
    std::string metadata_payload;
    std::vector<DrainSegmentRecord> segments;
    YLT_REFL(ReplicationStartRecord, runtime, metadata_payload, segments);
};

enum class RecoveryRecordKind : uint8_t {
    Job,
    Unit,
    Task,
    Segment,
    Replication,
    RetiredReplica,
};

struct RetiredReplicaRecord {
    Replica::Descriptor replica;
    int64_t release_ms{0};
    YLT_REFL(RetiredReplicaRecord, replica, release_ms);
};

struct DrainRecoveryRecord {
    RecoveryRecordKind kind;
    std::string key, payload;
    YLT_REFL(DrainRecoveryRecord, kind, key, payload);
};

using DrainRecoveryRecords = std::map<std::string, DrainRecoveryRecord>;

inline constexpr uint32_t kMaxDrainUnitRetries = 3;

std::string DrainRecoveryKey(RecoveryRecordKind kind, const std::string& key);
std::string DrainUnitKey(const std::string& tenant, const std::string& key,
                         const std::string& source);
bool ValidateDrainJob(const DrainJobRecord& job);
bool ValidateDrainUnit(const DrainUnitRecord& unit);
bool ValidateDurableTask(const DurableTaskRecord& task);
bool ValidateReplicationRecovery(
    const ReplicationRecoveryRecord& runtime,
    const std::vector<Replica::Descriptor>& replicas, uint64_t object_size,
    bool allow_missing_replicas = false);

// Updated by standby replay; snapshot capture and promotion freeze replay.
// Ordered keys permit bounded traversal without a second full state copy.
class DrainRecoveryState {
   public:
    bool Apply(const DrainJobUpdate& update);
    bool Apply(const DurableTaskRecord& task);
    bool Apply(const ReplicationRecoveryRecord& replication);
    bool Restore(const DrainRecoveryRecord& record);
    void EraseReplication(const std::string& tenant, const std::string& key);
    void EraseSegment(const std::string& name, const std::string& endpoint);
    bool Retire(const RetiredReplicaRecord& replica);
    void PruneRetired(int64_t now_ms);
    const DrainRecoveryRecords& records() const { return records_; }
    void Clear() { records_.clear(); }

   private:
    DrainRecoveryRecords records_;
};

template <typename T>
std::string EncodeRecoveryValue(const T& value) {
    const auto encoded = struct_pack::serialize(value);
    return {encoded.begin(), encoded.end()};
}

template <typename T>
bool DecodeRecoveryValue(const std::string& encoded, T& value) {
    return struct_pack::deserialize_to(value, encoded) == struct_pack::errc::ok;
}

template <typename T>
DrainRecoveryRecord MakeRecoveryRecord(RecoveryRecordKind kind,
                                       const std::string& key, const T& value) {
    return {kind, DrainRecoveryKey(kind, key), EncodeRecoveryValue(value)};
}

}  // namespace mooncake
