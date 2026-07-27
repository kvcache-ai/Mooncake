#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "master_service.h"

namespace mooncake::test {

constexpr uint64_t operator""_KB(unsigned long long value) {
    return value * 1024;
}

constexpr uint64_t operator""_MB(unsigned long long value) {
    return value * 1024 * 1024;
}

struct MemoryNodeSpec {
    std::string name;
    uint64_t capacity{16_MB};
    std::string host_id;
    std::string owner;
    std::optional<std::string> endpoint;
    bool mount_local_disk{false};
    bool local_disk_enable_offload{false};

    MemoryNodeSpec& Capacity(uint64_t value) {
        capacity = value;
        return *this;
    }

    MemoryNodeSpec& Host(std::string value) {
        host_id = std::move(value);
        return *this;
    }

    MemoryNodeSpec& OwnedBy(std::string value) {
        owner = std::move(value);
        return *this;
    }

    MemoryNodeSpec& Endpoint(std::string value) {
        endpoint = std::move(value);
        return *this;
    }

    MemoryNodeSpec& LocalDisk(bool enable_offload = false) {
        mount_local_disk = true;
        local_disk_enable_offload = enable_offload;
        return *this;
    }
};

MemoryNodeSpec MemoryNode(std::string name);

struct ScenarioServiceConfig {
    MasterServiceConfig value;

    ScenarioServiceConfig& DefaultLeaseTtl(uint64_t milliseconds) {
        value.default_kv_lease_ttl = milliseconds;
        return *this;
    }

    ScenarioServiceConfig& DefaultSoftPinTtl(uint64_t milliseconds) {
        value.default_kv_soft_pin_ttl = milliseconds;
        return *this;
    }

    ScenarioServiceConfig& AllowEvictSoftPinned(bool enabled) {
        value.allow_evict_soft_pinned_objects = enabled;
        return *this;
    }

    ScenarioServiceConfig& EvictionRatio(double ratio) {
        value.eviction_ratio = ratio;
        return *this;
    }

    ScenarioServiceConfig& AllocationStrategy(AllocationStrategyType type) {
        value.allocation_strategy_type = type;
        return *this;
    }

    ScenarioServiceConfig& PutStartDiscardTimeout(uint64_t seconds) {
        value.put_start_discard_timeout_sec = seconds;
        return *this;
    }

    ScenarioServiceConfig& PutStartReleaseTimeout(uint64_t seconds) {
        value.put_start_release_timeout_sec = seconds;
        return *this;
    }

    ScenarioServiceConfig& EnableOffload(bool enabled = true) {
        value.enable_offload = enabled;
        return *this;
    }
};

ScenarioServiceConfig ServiceConfig();

struct TenantSpec {
    std::string name;
    std::optional<uint64_t> requested_quota;
    std::optional<bool> expected_exists;
    std::optional<uint64_t> expected_used;
    std::optional<uint64_t> expected_reserved;
    std::optional<uint64_t> expected_committed_count;
    std::optional<uint64_t> expected_effective_quota;

    TenantSpec& Quota(uint64_t value) {
        requested_quota = value;
        return *this;
    }

    TenantSpec& DoesNotExist() {
        expected_exists = false;
        return *this;
    }

    TenantSpec& UsedBytes(uint64_t value) {
        expected_used = value;
        return *this;
    }

    TenantSpec& ReservedBytes(uint64_t value) {
        expected_reserved = value;
        return *this;
    }

    TenantSpec& CommittedCount(uint64_t value) {
        expected_committed_count = value;
        return *this;
    }

    TenantSpec& EffectiveQuota(uint64_t value) {
        expected_effective_quota = value;
        return *this;
    }
};

TenantSpec Tenant(std::string name);

enum class ScenarioActionKind {
    PUT_START,
    PUT_END,
    PUT_REVOKE,
    PUT,
    READ,
    UPSERT_START,
    UPSERT_END,
    UPSERT_REVOKE,
    BATCH_UPSERT_START,
    BATCH_UPSERT_END,
    BATCH_UPSERT_REVOKE,
    COPY_START,
    COPY_END,
    COPY_REVOKE,
    MOVE_START,
    MOVE_END,
    MOVE_REVOKE,
    ADD_REPLICA,
    NOTIFY_OFFLOAD_SUCCESS,
    UPSERT_TENANT_POLICY,
    DELETE_TENANT_POLICY,
    REMOVE,
    BATCH_REMOVE,
    BATCH_REPLICA_CLEAR,
    CREATE_COPY_TASK,
    CREATE_MOVE_TASK,
    FETCH_TASKS,
    COMPLETE_TASK,
    QUERY_TASK,
    CREATE_DRAIN_JOB,
    CANCEL_DRAIN_JOB,
    EXECUTE_NEXT_MOVE_TASK,
    REMOVE_BY_REGEX,
    REMOVE_ALL,
    REMOVE_ALL_TENANT,
    UNMOUNT_NODE,
    GRACEFUL_UNMOUNT_NODE,
    WAIT,
};

struct ScenarioAction {
    ScenarioActionKind kind;
    std::string actor{"default"};
    std::string tenant{TenantId::kDefaultValue};
    std::string key;
    std::vector<std::string> keys;
    uint64_t size{0};
    std::vector<uint64_t> sizes;
    uint32_t replica_count{1};
    uint32_t nof_replica_count{0};
    bool hard_pin{false};
    bool soft_pin{false};
    bool prefer_same_node{false};
    bool force{false};
    ReplicaType replica_type{ReplicaType::MEMORY};
    std::string preferred_segment;
    std::vector<std::string> preferred_segments;
    std::string host_id;
    std::string source_segment;
    std::vector<std::string> target_segments;
    std::optional<std::vector<std::string>> group_ids;
    std::string node;
    std::chrono::milliseconds wait{0};
    std::string alias;
    std::optional<ErrorCode> expected_error;
    std::optional<size_t> expected_replica_count;
    std::optional<size_t> expected_target_count;
    std::optional<std::string> expected_source_segment;
    std::optional<std::string> expected_same_allocation;
    std::optional<std::string> expected_different_allocation;
    std::optional<size_t> expected_affected_count;
    std::string task_alias;
    std::optional<std::vector<std::string>> expected_task_aliases;
    std::optional<TaskType> expected_task_type;
    std::optional<TaskStatus> expected_task_status;
    std::optional<std::string> expected_assigned_actor;
    std::string message;
    std::string transport_endpoint{"disk-endpoint"};
    uint32_t max_concurrency{1};
    bool retry_until_nonempty{false};
    TaskStatus completion_status{TaskStatus::SUCCESS};
    std::optional<std::string> expected_payload_tenant;
    std::optional<std::string> expected_payload_key;

    ScenarioAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    ScenarioAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    ScenarioAction& Replicas(uint32_t value) {
        replica_count = value;
        return *this;
    }

    ScenarioAction& NoFReplicas(uint32_t value) {
        nof_replica_count = value;
        return *this;
    }

    ScenarioAction& PreferSameNode() {
        prefer_same_node = true;
        return *this;
    }

    ScenarioAction& HardPinned() {
        hard_pin = true;
        return *this;
    }

    ScenarioAction& SoftPinned() {
        soft_pin = true;
        return *this;
    }

    ScenarioAction& PreferredSegment(std::string value) {
        preferred_segment = std::move(value);
        return *this;
    }

    ScenarioAction& PreferredSegments(std::vector<std::string> value) {
        preferred_segments = std::move(value);
        return *this;
    }

    ScenarioAction& FromHost(std::string value) {
        host_id = std::move(value);
        return *this;
    }

    ScenarioAction& From(std::string value) {
        source_segment = std::move(value);
        return *this;
    }

    ScenarioAction& To(std::vector<std::string> value) {
        target_segments = std::move(value);
        return *this;
    }

    ScenarioAction& InGroup(std::string value) {
        group_ids = std::vector<std::string>{std::move(value)};
        return *this;
    }

    ScenarioAction& InGroups(std::vector<std::string> value) {
        group_ids = std::move(value);
        return *this;
    }

    ScenarioAction& SaveAs(std::string value) {
        alias = std::move(value);
        return *this;
    }

    ScenarioAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }

    ScenarioAction& ExpectTargets(size_t value) {
        expected_target_count = value;
        return *this;
    }

    ScenarioAction& ExpectReplicas(size_t value) {
        expected_replica_count = value;
        return *this;
    }

    ScenarioAction& ExpectSource(std::string value) {
        expected_source_segment = std::move(value);
        return *this;
    }

    ScenarioAction& ExpectSameAllocationAs(std::string value) {
        expected_same_allocation = std::move(value);
        return *this;
    }

    ScenarioAction& ExpectDifferentAllocationFrom(std::string value) {
        expected_different_allocation = std::move(value);
        return *this;
    }

    ScenarioAction& ExpectAffected(size_t value) {
        expected_affected_count = value;
        return *this;
    }

    ScenarioAction& BatchSize(size_t value) {
        size = value;
        return *this;
    }

    ScenarioAction& ExpectTasks(std::vector<std::string> value) {
        expected_task_aliases = std::move(value);
        return *this;
    }

    ScenarioAction& ExpectTaskType(TaskType value) {
        expected_task_type = value;
        return *this;
    }

    ScenarioAction& ExpectTaskStatus(TaskStatus value) {
        expected_task_status = value;
        return *this;
    }

    ScenarioAction& ExpectAssignedTo(std::string value) {
        expected_assigned_actor = std::move(value);
        return *this;
    }

    ScenarioAction& WithMessage(std::string value) {
        message = std::move(value);
        return *this;
    }

    ScenarioAction& AtEndpoint(std::string value) {
        transport_endpoint = std::move(value);
        return *this;
    }

    ScenarioAction& MaxConcurrency(uint32_t value) {
        max_concurrency = value;
        return *this;
    }

    ScenarioAction& WaitUntilAvailable() {
        retry_until_nonempty = true;
        return *this;
    }

    ScenarioAction& ExpectPayloadTenant(std::string value) {
        expected_payload_tenant = std::move(value);
        return *this;
    }

    ScenarioAction& ExpectPayloadKey(std::string value) {
        expected_payload_key = std::move(value);
        return *this;
    }

    ScenarioAction& Force() {
        force = true;
        return *this;
    }
};

ScenarioAction PutStart(std::string key, uint64_t size);
ScenarioAction PutEnd(std::string key, ReplicaType type = ReplicaType::MEMORY);
ScenarioAction PutRevoke(std::string key,
                         ReplicaType type = ReplicaType::MEMORY);
ScenarioAction Put(std::string key, uint64_t size);
ScenarioAction Read(std::string key);
ScenarioAction UpsertStart(std::string key, uint64_t size);
ScenarioAction UpsertEnd(std::string key,
                         ReplicaType type = ReplicaType::MEMORY);
ScenarioAction UpsertRevoke(std::string key,
                            ReplicaType type = ReplicaType::MEMORY);
ScenarioAction BatchUpsertStart(std::vector<std::string> keys,
                                std::vector<uint64_t> sizes);
ScenarioAction BatchUpsertEnd(std::vector<std::string> keys);
ScenarioAction BatchUpsertRevoke(std::vector<std::string> keys);
ScenarioAction CopyStart(std::string key);
ScenarioAction CopyEnd(std::string key);
ScenarioAction CopyRevoke(std::string key);
ScenarioAction MoveStart(std::string key);
ScenarioAction MoveEnd(std::string key);
ScenarioAction MoveRevoke(std::string key);
ScenarioAction AddReplica(std::string key, uint64_t size);
ScenarioAction NotifyOffloadSuccess(std::string key, uint64_t size);
ScenarioAction UpsertTenantPolicy(std::string tenant, uint64_t quota);
ScenarioAction DeleteTenantPolicy(std::string tenant);
ScenarioAction Remove(std::string key);
ScenarioAction BatchRemove(std::vector<std::string> keys);
ScenarioAction BatchReplicaClear(std::vector<std::string> keys,
                                 std::string segment = "");
ScenarioAction CreateCopyTask(std::string key);
ScenarioAction CreateMoveTask(std::string key);
ScenarioAction FetchTasks(size_t batch_size = 16);
ScenarioAction CompleteTask(std::string task_alias, TaskStatus status);
ScenarioAction QueryTask(std::string task_alias);
ScenarioAction CreateDrainJob(std::vector<std::string> segments);
ScenarioAction CancelDrainJob(std::string job_alias);
ScenarioAction ExecuteNextMoveTask(TaskStatus status = TaskStatus::SUCCESS);
ScenarioAction RemoveByRegex(std::string pattern);
ScenarioAction RemoveAll();
ScenarioAction RemoveAllForTenant(std::string tenant);
ScenarioAction UnmountNode(std::string node);
ScenarioAction GracefulUnmountNode(std::string node,
                                   std::chrono::milliseconds grace_period);
ScenarioAction WaitFor(std::chrono::milliseconds duration);

struct ObjectSpec {
    enum class Readability {
        UNSPECIFIED,
        READABLE,
        NOT_READY,
    };

    std::string key;
    std::string tenant{TenantId::kDefaultValue};
    std::optional<bool> expected_exists;
    Readability readability{Readability::UNSPECIFIED};
    std::optional<size_t> complete_replicas;
    std::optional<uint64_t> size;
    std::optional<std::vector<std::string>> replica_segments;
    std::optional<size_t> distinct_segments;

    ObjectSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    ObjectSpec& Exists() {
        expected_exists = true;
        return *this;
    }

    ObjectSpec& DoesNotExist() {
        expected_exists = false;
        return *this;
    }

    ObjectSpec& IsReadable() {
        readability = Readability::READABLE;
        return *this;
    }

    ObjectSpec& IsNotReady() {
        readability = Readability::NOT_READY;
        return *this;
    }

    ObjectSpec& HasCompleteReplicas(size_t value) {
        complete_replicas = value;
        return *this;
    }

    ObjectSpec& HasSize(uint64_t value) {
        size = value;
        return *this;
    }

    ObjectSpec& HasReplicasOn(std::vector<std::string> value) {
        replica_segments = std::move(value);
        return *this;
    }

    ObjectSpec& HasDistinctSegments(size_t value) {
        distinct_segments = value;
        return *this;
    }
};

ObjectSpec Object(std::string key);

struct MatchingObjectsSpec {
    std::string pattern;
    std::string tenant{TenantId::kDefaultValue};
    std::optional<size_t> expected_count;
    std::optional<std::vector<std::string>> expected_keys;

    MatchingObjectsSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    MatchingObjectsSpec& HasCount(size_t value) {
        expected_count = value;
        return *this;
    }

    MatchingObjectsSpec& HasKeys(std::vector<std::string> value) {
        expected_keys = std::move(value);
        return *this;
    }
};

MatchingObjectsSpec MatchingObjects(std::string pattern);

struct ObjectExistenceSpec {
    std::vector<std::string> keys;
    std::string tenant{TenantId::kDefaultValue};
    std::vector<bool> expected;

    ObjectExistenceSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    ObjectExistenceSpec& Is(std::vector<bool> value) {
        expected = std::move(value);
        return *this;
    }
};

ObjectExistenceSpec ObjectExistence(std::vector<std::string> keys);

struct BatchObjectsSpec {
    enum class State {
        READABLE,
        MISSING,
        NOT_READY,
    };

    std::vector<std::string> keys;
    std::string tenant{TenantId::kDefaultValue};
    std::vector<State> expected;

    BatchObjectsSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    BatchObjectsSpec& Are(std::vector<State> value) {
        expected = std::move(value);
        return *this;
    }
};

BatchObjectsSpec BatchObjects(std::vector<std::string> keys);

struct AllKeysSpec {
    std::string tenant{TenantId::kDefaultValue};
    std::vector<std::string> expected;

    AllKeysSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    AllKeysSpec& Are(std::vector<std::string> value) {
        expected = std::move(value);
        return *this;
    }
};

AllKeysSpec AllKeys();

struct ClientIpsSpec {
    std::vector<std::string> actors;
    std::map<std::string, std::vector<std::string>> expected;

    ClientIpsSpec& Are(std::map<std::string, std::vector<std::string>> value) {
        expected = std::move(value);
        return *this;
    }
};

ClientIpsSpec ClientIps(std::vector<std::string> actors);

struct SegmentSpec {
    std::string name;
    std::optional<SegmentStatus> expected_status;
    bool expected_unmounted{false};

    SegmentSpec& HasStatus(SegmentStatus value) {
        expected_status = value;
        return *this;
    }

    SegmentSpec& IsUnmounted() {
        expected_unmounted = true;
        return *this;
    }
};

SegmentSpec SegmentState(std::string name);

struct JobSpec {
    std::string alias;
    JobStatus expected_status{JobStatus::CREATED};
    std::optional<uint64_t> expected_active_units;
    std::optional<uint64_t> minimum_succeeded_units;
    std::optional<uint64_t> minimum_failed_units;

    JobSpec& HasStatus(JobStatus value) {
        expected_status = value;
        return *this;
    }

    JobSpec& HasActiveUnits(uint64_t value) {
        expected_active_units = value;
        return *this;
    }

    JobSpec& HasAtLeastSucceededUnits(uint64_t value) {
        minimum_succeeded_units = value;
        return *this;
    }

    JobSpec& HasAtLeastFailedUnits(uint64_t value) {
        minimum_failed_units = value;
        return *this;
    }
};

JobSpec Job(std::string alias);

enum class InterleaveCommandKind {
    RUN_UNTIL,
    START,
    RESUME,
    JOIN,
};

struct InterleaveCommand {
    InterleaveCommandKind kind;
    std::string actor;
    std::optional<ScenarioAction> action;
    std::optional<MasterTestCheckpoint> checkpoint;
};

InterleaveCommand RunUntil(std::string actor, ScenarioAction action,
                           MasterTestCheckpoint checkpoint);
InterleaveCommand Start(std::string actor, ScenarioAction action);
InterleaveCommand Resume(std::string actor);
InterleaveCommand Join(std::string actor);

struct ScenarioOperationResult {
    bool ok{false};
    ErrorCode error{ErrorCode::OK};
    size_t affected_count{0};
    std::optional<UUID> task_id;
    std::vector<UUID> task_ids;
    std::optional<QueryTaskResponse> task;
    std::vector<TaskAssignment> task_assignments;
    std::optional<UUID> job_id;
    std::vector<Replica::Descriptor> replicas;
    std::string source_segment;
    std::vector<std::string> target_segments;
    std::string detail;
};

struct ScenarioTraceEvent {
    uint64_t sequence{0};
    std::string phase;
    std::string actor;
    std::string operation;
    std::string tenant;
    std::string key;
    std::string client_id;
    std::string checkpoint;
    uint64_t occurrence{0};
    std::string result;
};

struct ScenarioReplayRelease {
    std::string actor;
    std::string checkpoint;
    uint64_t occurrence{0};
};

struct ScenarioReplayArtifact {
    uint32_t version{1};
    std::string scenario;
    std::string test;
    std::vector<std::string> choreography;
    std::vector<ScenarioReplayRelease> releases;
    std::vector<ScenarioTraceEvent> trace;
    std::string failure;
};

std::optional<std::string> ValidateScenarioReplayArtifact(
    const ScenarioReplayArtifact& artifact, const std::string& scenario,
    const std::string& test);

class ScenarioCheckpointScheduler : public MasterTestCheckpointSink {
   public:
    explicit ScenarioCheckpointScheduler(
        std::chrono::milliseconds timeout = std::chrono::seconds(5),
        bool pause_unarmed = true);

    void RegisterActor(const std::string& actor, const UUID& client_id);
    void Arm(const std::string& actor, MasterTestCheckpoint checkpoint);
    void Reach(const MasterTestCheckpointEvent& event) override;
    bool WaitUntilReached(const std::string& actor,
                          MasterTestCheckpoint checkpoint,
                          std::string* error = nullptr);
    bool Resume(const std::string& actor, std::string* error = nullptr);
    void RecordOperation(const std::string& phase, const std::string& actor,
                         const std::string& operation,
                         const std::string& tenant, const std::string& key,
                         const UUID& client_id, const std::string& result = "");
    void Cancel(std::string reason);

    void SetReplay(std::vector<ScenarioReplayRelease> releases);
    bool ValidateReplayComplete(std::string* error) const;

    std::vector<ScenarioReplayRelease> Releases() const;
    std::vector<ScenarioTraceEvent> Trace() const;
    std::string Failure() const;

   private:
    struct WaitState {
        uint64_t armed{0};
        uint64_t arrived{0};
        uint64_t released{0};
        std::string tenant;
        std::string key;
        std::string client_id;
    };

    static std::string Key(const std::string& actor,
                           MasterTestCheckpoint checkpoint);
    void AppendTraceLocked(std::string phase, std::string actor,
                           std::string operation, std::string tenant,
                           std::string key, std::string client_id,
                           std::string checkpoint, uint64_t occurrence,
                           std::string result);
    std::string WaitingSummaryLocked() const;

    std::chrono::milliseconds timeout_;
    bool pause_unarmed_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool cancelled_{false};
    std::string failure_;
    uint64_t sequence_{0};
    std::unordered_map<UUID, std::string, boost::hash<UUID>> actors_;
    std::unordered_map<std::string, WaitState> waits_;
    std::unordered_map<std::string, MasterTestCheckpoint> waiting_checkpoint_;
    std::vector<ScenarioReplayRelease> releases_;
    std::vector<ScenarioReplayRelease> replay_;
    size_t replay_index_{0};
    std::vector<ScenarioTraceEvent> trace_;
};

class MasterScenario {
   public:
    explicit MasterScenario(std::string name);
    ~MasterScenario();

    MasterScenario(const MasterScenario&) = delete;
    MasterScenario& operator=(const MasterScenario&) = delete;

    MasterScenario& Given(MemoryNodeSpec node);
    MasterScenario& Given(TenantSpec tenant);
    MasterScenario& Configured(ScenarioServiceConfig config);
    MasterScenario& When(ScenarioAction action);
    MasterScenario& Parallel(std::initializer_list<ScenarioAction> actions);
    MasterScenario& Then(ObjectSpec object);
    MasterScenario& Eventually(
        ObjectSpec object,
        std::chrono::milliseconds timeout = std::chrono::seconds(4));
    MasterScenario& Then(MatchingObjectsSpec objects);
    MasterScenario& Then(ObjectExistenceSpec objects);
    MasterScenario& Then(BatchObjectsSpec objects);
    MasterScenario& Then(AllKeysSpec keys);
    MasterScenario& Then(ClientIpsSpec clients);
    MasterScenario& Then(SegmentSpec segment);
    MasterScenario& Then(JobSpec job);
    MasterScenario& Eventually(JobSpec job, std::chrono::milliseconds timeout =
                                                std::chrono::seconds(4));
    MasterScenario& Then(TenantSpec tenant);
    MasterScenario& Interleave(
        std::initializer_list<InterleaveCommand> commands);

    const ScenarioOperationResult* Result(const std::string& alias) const;
    MasterService& ServiceForTesting();
    ScenarioCheckpointScheduler& SchedulerForTesting();

   private:
    struct ActorThread {
        std::thread thread;
        std::optional<ScenarioOperationResult> result;
        ScenarioAction action;
    };

    void EnsureService();
    UUID ActorId(const std::string& actor);
    ScenarioOperationResult Execute(const ScenarioAction& action);
    bool ValidateResult(const ScenarioAction& action,
                        const ScenarioOperationResult& result);
    void StartActor(std::string actor, ScenarioAction action);
    void JoinActor(const std::string& actor);
    void JoinAllActors();
    void ReportFailure(const std::string& message);
    void LoadReplayIfRequested();
    void DumpFailureArtifact();
    void ValidateReplay();
    void RecordChoreography(const std::string& entry);
    std::string JobMismatch(const JobSpec& job);
    static std::string DescribeAction(const ScenarioAction& action);
    static std::string OperationName(ScenarioActionKind kind);
    static std::string CheckpointName(MasterTestCheckpoint checkpoint);

    std::string name_;
    std::string test_name_;
    bool frozen_{false};
    bool artifact_dumped_{false};
    uintptr_t next_segment_base_{0x300000000};
    std::vector<MemoryNodeSpec> nodes_;
    std::vector<TenantSpec> tenants_;
    std::optional<MasterServiceConfig> service_config_;
    std::filesystem::path policy_path_;
    std::unique_ptr<MasterService> service_;
    std::shared_ptr<ScenarioCheckpointScheduler> scheduler_;
    std::mutex actor_mutex_;
    std::unordered_map<std::string, UUID> actor_ids_;
    std::unordered_map<std::string, UUID> node_clients_;
    std::unordered_map<std::string, UUID> node_segments_;
    std::unordered_map<std::string, ScenarioOperationResult> results_;
    std::unordered_map<std::string, std::unique_ptr<ActorThread>> threads_;
    std::vector<std::string> choreography_;
    std::optional<std::vector<std::string>> replay_choreography_;
};

}  // namespace mooncake::test

YLT_REFL(mooncake::test::ScenarioTraceEvent, sequence, phase, actor, operation,
         tenant, key, client_id, checkpoint, occurrence, result);
YLT_REFL(mooncake::test::ScenarioReplayRelease, actor, checkpoint, occurrence);
YLT_REFL(mooncake::test::ScenarioReplayArtifact, version, scenario, test,
         choreography, releases, trace, failure);
