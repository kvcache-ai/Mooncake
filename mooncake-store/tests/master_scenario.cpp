#include "master_scenario.h"

#include <glog/logging.h>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <sstream>
#include <unordered_set>

#include "master_metric_manager.h"
#include "tenant_quota_policy_store.h"
#include "utils.h"

namespace mooncake::test {
namespace {

std::atomic<uint64_t> g_scenario_file_counter{0};

std::string CheckpointToString(MasterTestCheckpoint checkpoint) {
    switch (checkpoint) {
        case MasterTestCheckpoint::UPSERT_AFTER_PREEMPT:
            return "UPSERT_AFTER_PREEMPT";
        case MasterTestCheckpoint::ADD_REPLICA_AFTER_TENANT_VALIDATION:
            return "ADD_REPLICA_AFTER_TENANT_VALIDATION";
    }
    return "UNKNOWN";
}

std::optional<MasterTestCheckpoint> CheckpointFromString(
    const std::string& value) {
    if (value == "UPSERT_AFTER_PREEMPT") {
        return MasterTestCheckpoint::UPSERT_AFTER_PREEMPT;
    }
    if (value == "ADD_REPLICA_AFTER_TENANT_VALIDATION") {
        return MasterTestCheckpoint::ADD_REPLICA_AFTER_TENANT_VALIDATION;
    }
    return std::nullopt;
}

std::string SanitizeFilename(std::string value) {
    for (char& ch : value) {
        if (!std::isalnum(static_cast<unsigned char>(ch)) && ch != '-' &&
            ch != '_') {
            ch = '_';
        }
    }
    return value;
}

std::string ClientIdToString(const UUID& client_id) {
    return std::to_string(client_id.first) + "-" +
           std::to_string(client_id.second);
}

std::string CurrentGTestName() {
    const auto* test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    if (test_info == nullptr) {
        return "";
    }
    return std::string(test_info->test_suite_name()) + "." + test_info->name();
}

uint64_t DescriptorSize(const Replica::Descriptor& descriptor) {
    if (descriptor.is_memory_replica()) {
        return descriptor.get_memory_descriptor().buffer_descriptor.size_;
    }
    if (descriptor.is_nof_replica()) {
        return descriptor.get_nof_descriptor().buffer_descriptor.size_;
    }
    if (descriptor.is_disk_replica()) {
        return descriptor.get_disk_descriptor().object_size;
    }
    if (descriptor.is_local_disk_replica()) {
        return descriptor.get_local_disk_descriptor().object_size;
    }
    return 0;
}

std::string DescriptorSegment(const Replica::Descriptor& descriptor) {
    if (descriptor.is_memory_replica()) {
        return descriptor.get_memory_descriptor()
            .buffer_descriptor.transport_endpoint_;
    }
    if (descriptor.is_nof_replica()) {
        return descriptor.get_nof_descriptor()
            .buffer_descriptor.transport_endpoint_;
    }
    if (descriptor.is_disk_replica()) {
        return descriptor.get_disk_descriptor().file_path;
    }
    if (descriptor.is_local_disk_replica()) {
        return descriptor.get_local_disk_descriptor().transport_endpoint;
    }
    return "";
}

std::string AllocationIdentity(const Replica::Descriptor& descriptor) {
    if (descriptor.is_memory_replica()) {
        const auto& buffer =
            descriptor.get_memory_descriptor().buffer_descriptor;
        return buffer.transport_endpoint_ + ":" +
               std::to_string(buffer.buffer_address_) + ":" +
               std::to_string(buffer.size_);
    }
    if (descriptor.is_nof_replica()) {
        const auto& buffer = descriptor.get_nof_descriptor().buffer_descriptor;
        return buffer.transport_endpoint_ + ":" +
               std::to_string(buffer.buffer_address_) + ":" +
               std::to_string(buffer.size_);
    }
    return DescriptorSegment(descriptor) + ":" +
           std::to_string(DescriptorSize(descriptor));
}

}  // namespace

MemoryNodeSpec MemoryNode(std::string name) {
    MemoryNodeSpec spec;
    spec.name = std::move(name);
    return spec;
}

ScenarioServiceConfig ServiceConfig() { return {}; }

TenantSpec Tenant(std::string name) {
    TenantSpec spec;
    spec.name = std::move(name);
    return spec;
}

ScenarioAction PutStart(std::string key, uint64_t size) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::PUT_START;
    action.key = std::move(key);
    action.size = size;
    return action;
}

ScenarioAction PutEnd(std::string key, ReplicaType type) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::PUT_END;
    action.key = std::move(key);
    action.replica_type = type;
    return action;
}

ScenarioAction PutRevoke(std::string key, ReplicaType type) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::PUT_REVOKE;
    action.key = std::move(key);
    action.replica_type = type;
    return action;
}

ScenarioAction Put(std::string key, uint64_t size) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::PUT;
    action.key = std::move(key);
    action.size = size;
    return action;
}

ScenarioAction Read(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::READ;
    action.key = std::move(key);
    return action;
}

ScenarioAction UpsertStart(std::string key, uint64_t size) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::UPSERT_START;
    action.key = std::move(key);
    action.size = size;
    return action;
}

ScenarioAction UpsertEnd(std::string key, ReplicaType type) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::UPSERT_END;
    action.key = std::move(key);
    action.replica_type = type;
    return action;
}

ScenarioAction UpsertRevoke(std::string key, ReplicaType type) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::UPSERT_REVOKE;
    action.key = std::move(key);
    action.replica_type = type;
    return action;
}

ScenarioAction BatchUpsertStart(std::vector<std::string> keys,
                                std::vector<uint64_t> sizes) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::BATCH_UPSERT_START;
    action.keys = std::move(keys);
    action.sizes = std::move(sizes);
    return action;
}

ScenarioAction BatchUpsertEnd(std::vector<std::string> keys) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::BATCH_UPSERT_END;
    action.keys = std::move(keys);
    return action;
}

ScenarioAction BatchUpsertRevoke(std::vector<std::string> keys) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::BATCH_UPSERT_REVOKE;
    action.keys = std::move(keys);
    return action;
}

ScenarioAction CopyStart(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::COPY_START;
    action.key = std::move(key);
    return action;
}

ScenarioAction CopyEnd(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::COPY_END;
    action.key = std::move(key);
    return action;
}

ScenarioAction CopyRevoke(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::COPY_REVOKE;
    action.key = std::move(key);
    return action;
}

ScenarioAction MoveStart(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::MOVE_START;
    action.key = std::move(key);
    return action;
}

ScenarioAction MoveEnd(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::MOVE_END;
    action.key = std::move(key);
    return action;
}

ScenarioAction MoveRevoke(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::MOVE_REVOKE;
    action.key = std::move(key);
    return action;
}

ScenarioAction AddReplica(std::string key, uint64_t size) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::ADD_REPLICA;
    action.key = std::move(key);
    action.size = size;
    return action;
}

ScenarioAction NotifyOffloadSuccess(std::string key, uint64_t size) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::NOTIFY_OFFLOAD_SUCCESS;
    action.key = std::move(key);
    action.size = size;
    return action;
}

ScenarioAction UpsertTenantPolicy(std::string tenant, uint64_t quota) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::UPSERT_TENANT_POLICY;
    action.tenant = std::move(tenant);
    action.size = quota;
    return action;
}

ScenarioAction DeleteTenantPolicy(std::string tenant) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::DELETE_TENANT_POLICY;
    action.tenant = std::move(tenant);
    return action;
}

ScenarioAction Remove(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::REMOVE;
    action.key = std::move(key);
    return action;
}

ScenarioAction BatchRemove(std::vector<std::string> keys) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::BATCH_REMOVE;
    action.keys = std::move(keys);
    return action;
}

ScenarioAction BatchReplicaClear(std::vector<std::string> keys,
                                 std::string segment) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::BATCH_REPLICA_CLEAR;
    action.keys = std::move(keys);
    action.source_segment = std::move(segment);
    return action;
}

ScenarioAction CreateCopyTask(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::CREATE_COPY_TASK;
    action.key = std::move(key);
    return action;
}

ScenarioAction CreateMoveTask(std::string key) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::CREATE_MOVE_TASK;
    action.key = std::move(key);
    return action;
}

ScenarioAction FetchTasks(size_t batch_size) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::FETCH_TASKS;
    action.size = batch_size;
    return action;
}

ScenarioAction CompleteTask(std::string task_alias, TaskStatus status) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::COMPLETE_TASK;
    action.task_alias = std::move(task_alias);
    action.expected_task_status = status;
    return action;
}

ScenarioAction QueryTask(std::string task_alias) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::QUERY_TASK;
    action.task_alias = std::move(task_alias);
    return action;
}

ScenarioAction CreateDrainJob(std::vector<std::string> segments) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::CREATE_DRAIN_JOB;
    action.keys = std::move(segments);
    return action;
}

ScenarioAction CancelDrainJob(std::string job_alias) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::CANCEL_DRAIN_JOB;
    action.task_alias = std::move(job_alias);
    return action;
}

ScenarioAction ExecuteNextMoveTask(TaskStatus status) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::EXECUTE_NEXT_MOVE_TASK;
    action.completion_status = status;
    action.retry_until_nonempty = true;
    action.wait = std::chrono::seconds(4);
    return action;
}

ScenarioAction RemoveByRegex(std::string pattern) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::REMOVE_BY_REGEX;
    action.key = std::move(pattern);
    return action;
}

ScenarioAction RemoveAll() {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::REMOVE_ALL;
    return action;
}

ScenarioAction RemoveAllForTenant(std::string tenant) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::REMOVE_ALL_TENANT;
    action.tenant = std::move(tenant);
    return action;
}

ScenarioAction UnmountNode(std::string node) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::UNMOUNT_NODE;
    action.actor = node;
    action.node = std::move(node);
    return action;
}

ScenarioAction GracefulUnmountNode(std::string node,
                                   std::chrono::milliseconds grace_period) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::GRACEFUL_UNMOUNT_NODE;
    action.actor = node;
    action.node = std::move(node);
    action.wait = grace_period;
    return action;
}

ScenarioAction WaitFor(std::chrono::milliseconds duration) {
    ScenarioAction action{};
    action.kind = ScenarioActionKind::WAIT;
    action.wait = duration;
    return action;
}

ObjectSpec Object(std::string key) {
    ObjectSpec spec;
    spec.key = std::move(key);
    return spec;
}

MatchingObjectsSpec MatchingObjects(std::string pattern) {
    MatchingObjectsSpec spec;
    spec.pattern = std::move(pattern);
    return spec;
}

ObjectExistenceSpec ObjectExistence(std::vector<std::string> keys) {
    ObjectExistenceSpec spec;
    spec.keys = std::move(keys);
    return spec;
}

BatchObjectsSpec BatchObjects(std::vector<std::string> keys) {
    BatchObjectsSpec spec;
    spec.keys = std::move(keys);
    return spec;
}

AllKeysSpec AllKeys() { return {}; }

ClientIpsSpec ClientIps(std::vector<std::string> actors) {
    ClientIpsSpec spec;
    spec.actors = std::move(actors);
    return spec;
}

SegmentSpec SegmentState(std::string name) {
    SegmentSpec spec;
    spec.name = std::move(name);
    return spec;
}

JobSpec Job(std::string alias) {
    JobSpec spec;
    spec.alias = std::move(alias);
    return spec;
}

InterleaveCommand RunUntil(std::string actor, ScenarioAction action,
                           MasterTestCheckpoint checkpoint) {
    action.By(actor);
    InterleaveCommand command{};
    command.kind = InterleaveCommandKind::RUN_UNTIL;
    command.actor = std::move(actor);
    command.action = std::move(action);
    command.checkpoint = checkpoint;
    return command;
}

InterleaveCommand Start(std::string actor, ScenarioAction action) {
    action.By(actor);
    InterleaveCommand command{};
    command.kind = InterleaveCommandKind::START;
    command.actor = std::move(actor);
    command.action = std::move(action);
    return command;
}

InterleaveCommand Resume(std::string actor) {
    InterleaveCommand command{};
    command.kind = InterleaveCommandKind::RESUME;
    command.actor = std::move(actor);
    return command;
}

InterleaveCommand Join(std::string actor) {
    InterleaveCommand command{};
    command.kind = InterleaveCommandKind::JOIN;
    command.actor = std::move(actor);
    return command;
}

std::optional<std::string> ValidateScenarioReplayArtifact(
    const ScenarioReplayArtifact& artifact, const std::string& scenario,
    const std::string& test) {
    if (artifact.version != 1) {
        return "unsupported replay artifact version " +
               std::to_string(artifact.version);
    }
    if (artifact.scenario != scenario) {
        return "replay scenario mismatch: expected " + scenario + ", got " +
               artifact.scenario;
    }
    if (artifact.test != test) {
        return "replay test mismatch: expected " + test + ", got " +
               artifact.test;
    }
    for (const auto& release : artifact.releases) {
        if (!CheckpointFromString(release.checkpoint)) {
            return "unknown checkpoint in replay: " + release.checkpoint;
        }
    }
    return std::nullopt;
}

ScenarioCheckpointScheduler::ScenarioCheckpointScheduler(
    std::chrono::milliseconds timeout, bool pause_unarmed)
    : timeout_(timeout), pause_unarmed_(pause_unarmed) {}

std::string ScenarioCheckpointScheduler::Key(const std::string& actor,
                                             MasterTestCheckpoint checkpoint) {
    return actor + "\n" + CheckpointToString(checkpoint);
}

void ScenarioCheckpointScheduler::RegisterActor(const std::string& actor,
                                                const UUID& client_id) {
    std::lock_guard lock(mutex_);
    actors_[client_id] = actor;
}

void ScenarioCheckpointScheduler::Arm(const std::string& actor,
                                      MasterTestCheckpoint checkpoint) {
    std::lock_guard lock(mutex_);
    auto& state = waits_[Key(actor, checkpoint)];
    state.armed = std::max(state.armed, state.arrived) + 1;
}

void ScenarioCheckpointScheduler::AppendTraceLocked(
    std::string phase, std::string actor, std::string operation,
    std::string tenant, std::string key, std::string client_id,
    std::string checkpoint, uint64_t occurrence, std::string result) {
    trace_.push_back({++sequence_, std::move(phase), std::move(actor),
                      std::move(operation), std::move(tenant), std::move(key),
                      std::move(client_id), std::move(checkpoint), occurrence,
                      std::move(result)});
}

std::string ScenarioCheckpointScheduler::WaitingSummaryLocked() const {
    if (waiting_checkpoint_.empty()) {
        return "no actors paused";
    }
    std::vector<std::string> waiting;
    waiting.reserve(waiting_checkpoint_.size());
    for (const auto& [actor, checkpoint] : waiting_checkpoint_) {
        waiting.push_back(actor + "@" + CheckpointToString(checkpoint));
    }
    std::sort(waiting.begin(), waiting.end());
    std::ostringstream stream;
    stream << "paused actors: ";
    for (size_t index = 0; index < waiting.size(); ++index) {
        if (index != 0) {
            stream << ", ";
        }
        stream << waiting[index];
    }
    return stream.str();
}

void ScenarioCheckpointScheduler::Reach(
    const MasterTestCheckpointEvent& event) {
    std::unique_lock lock(mutex_);
    auto actor_it = actors_.find(event.client_id);
    if (actor_it == actors_.end()) {
        if (failure_.empty()) {
            failure_ = "checkpoint reached by an unregistered client; " +
                       WaitingSummaryLocked();
        }
        cancelled_ = true;
        cv_.notify_all();
        return;
    }

    const std::string actor = actor_it->second;
    const std::string key = Key(actor, event.checkpoint);
    auto& state = waits_[key];
    const uint64_t occurrence = ++state.arrived;
    state.tenant = event.tenant_id.value();
    state.key = event.key;
    state.client_id = ClientIdToString(event.client_id);
    waiting_checkpoint_[actor] = event.checkpoint;
    AppendTraceLocked("CHECKPOINT_ARRIVE", actor, "", state.tenant, state.key,
                      state.client_id, CheckpointToString(event.checkpoint),
                      occurrence, "");
    if (!pause_unarmed_ && occurrence > state.armed) {
        state.released = occurrence;
        AppendTraceLocked("CHECKPOINT_BYPASS", actor, "", state.tenant,
                          state.key, state.client_id,
                          CheckpointToString(event.checkpoint), occurrence, "");
        cv_.notify_all();
        return;
    }
    cv_.notify_all();

    const bool released = cv_.wait_for(lock, timeout_, [&] {
        return cancelled_ || state.released >= occurrence;
    });
    if (!released && !cancelled_) {
        cancelled_ = true;
        failure_ = "timeout waiting to resume actor " + actor + " at " +
                   CheckpointToString(event.checkpoint) + "; " +
                   WaitingSummaryLocked();
        AppendTraceLocked("CHECKPOINT_TIMEOUT", actor, "", state.tenant,
                          state.key, state.client_id,
                          CheckpointToString(event.checkpoint), occurrence,
                          failure_);
        cv_.notify_all();
    }
    if (state.released >= occurrence) {
        AppendTraceLocked("CHECKPOINT_CONTINUE", actor, "", state.tenant,
                          state.key, state.client_id,
                          CheckpointToString(event.checkpoint), occurrence, "");
    }
    auto waiting = waiting_checkpoint_.find(actor);
    if (waiting != waiting_checkpoint_.end() &&
        waiting->second == event.checkpoint) {
        waiting_checkpoint_.erase(waiting);
    }
}

bool ScenarioCheckpointScheduler::WaitUntilReached(
    const std::string& actor, MasterTestCheckpoint checkpoint,
    std::string* error) {
    std::unique_lock lock(mutex_);
    const std::string key = Key(actor, checkpoint);
    const bool reached = cv_.wait_for(lock, timeout_, [&] {
        auto it = waits_.find(key);
        return cancelled_ ||
               (it != waits_.end() && it->second.arrived > it->second.released);
    });
    if (!reached && !cancelled_) {
        cancelled_ = true;
        failure_ = "timeout waiting for actor " + actor + " to reach " +
                   CheckpointToString(checkpoint) + "; " +
                   WaitingSummaryLocked();
        cv_.notify_all();
    }
    if (!reached || cancelled_) {
        if (error) {
            *error = failure_;
        }
        return false;
    }
    return true;
}

bool ScenarioCheckpointScheduler::Resume(const std::string& actor,
                                         std::string* error) {
    std::lock_guard lock(mutex_);
    auto checkpoint_it = waiting_checkpoint_.find(actor);
    if (checkpoint_it == waiting_checkpoint_.end()) {
        if (error) {
            *error = "actor " + actor + " is not paused";
        }
        return false;
    }

    const MasterTestCheckpoint checkpoint = checkpoint_it->second;
    auto& state = waits_[Key(actor, checkpoint)];
    const uint64_t occurrence = state.released + 1;
    if (occurrence > state.arrived) {
        if (error) {
            *error = "actor " + actor + " has no checkpoint to release";
        }
        return false;
    }

    ScenarioReplayRelease release{actor, CheckpointToString(checkpoint),
                                  occurrence};
    if (!replay_.empty()) {
        if (replay_index_ >= replay_.size()) {
            failure_ = "replay exhausted before release " + actor + "/" +
                       release.checkpoint + "; " + WaitingSummaryLocked();
            cancelled_ = true;
            cv_.notify_all();
            if (error) {
                *error = failure_;
            }
            return false;
        }
        const auto& expected = replay_[replay_index_];
        if (expected.actor != release.actor ||
            expected.checkpoint != release.checkpoint ||
            expected.occurrence != release.occurrence) {
            std::ostringstream stream;
            stream << "replay mismatch at release " << replay_index_
                   << ": expected " << expected.actor << "/"
                   << expected.checkpoint << "#" << expected.occurrence
                   << ", observed " << release.actor << "/"
                   << release.checkpoint << "#" << release.occurrence << "; "
                   << WaitingSummaryLocked();
            failure_ = stream.str();
            cancelled_ = true;
            cv_.notify_all();
            if (error) {
                *error = failure_;
            }
            return false;
        }
        ++replay_index_;
    }

    state.released = occurrence;
    releases_.push_back(release);
    AppendTraceLocked("CHECKPOINT_RELEASE", actor, "", state.tenant, state.key,
                      state.client_id, release.checkpoint, occurrence, "");
    cv_.notify_all();
    return true;
}

void ScenarioCheckpointScheduler::RecordOperation(
    const std::string& phase, const std::string& actor,
    const std::string& operation, const std::string& tenant,
    const std::string& key, const UUID& client_id, const std::string& result) {
    std::lock_guard lock(mutex_);
    AppendTraceLocked(phase, actor, operation, tenant, key,
                      ClientIdToString(client_id), "", 0, result);
    cv_.notify_all();
}

void ScenarioCheckpointScheduler::Cancel(std::string reason) {
    std::lock_guard lock(mutex_);
    cancelled_ = true;
    if (failure_.empty() && !reason.empty()) {
        failure_ = std::move(reason) + "; " + WaitingSummaryLocked();
    }
    cv_.notify_all();
}

void ScenarioCheckpointScheduler::SetReplay(
    std::vector<ScenarioReplayRelease> releases) {
    std::lock_guard lock(mutex_);
    replay_ = std::move(releases);
    replay_index_ = 0;
}

bool ScenarioCheckpointScheduler::ValidateReplayComplete(
    std::string* error) const {
    std::lock_guard lock(mutex_);
    if (!failure_.empty()) {
        if (error) {
            *error = failure_;
        }
        return false;
    }
    if (!replay_.empty() && replay_index_ != replay_.size()) {
        if (error) {
            *error = "replay has " +
                     std::to_string(replay_.size() - replay_index_) +
                     " unconsumed releases";
        }
        return false;
    }
    return true;
}

std::vector<ScenarioReplayRelease> ScenarioCheckpointScheduler::Releases()
    const {
    std::lock_guard lock(mutex_);
    return releases_;
}

std::vector<ScenarioTraceEvent> ScenarioCheckpointScheduler::Trace() const {
    std::lock_guard lock(mutex_);
    return trace_;
}

std::string ScenarioCheckpointScheduler::Failure() const {
    std::lock_guard lock(mutex_);
    return failure_;
}

MasterScenario::MasterScenario(std::string name)
    : name_(std::move(name)),
      test_name_(CurrentGTestName()),
      scheduler_(std::make_shared<ScenarioCheckpointScheduler>(
          std::chrono::seconds(5), false)) {
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();
    MasterMetricManager::instance().reset_cache_total_nums();
    LoadReplayIfRequested();
}

MasterScenario::~MasterScenario() {
    if (!threads_.empty()) {
        std::vector<std::string> actors;
        actors.reserve(threads_.size());
        for (const auto& [actor, _] : threads_) {
            actors.push_back(actor);
        }
        std::sort(actors.begin(), actors.end());
        std::ostringstream stream;
        stream << "scenario ended before joining actors: ";
        for (size_t index = 0; index < actors.size(); ++index) {
            if (index != 0) {
                stream << ", ";
            }
            stream << actors[index];
        }
        scheduler_->Cancel(stream.str());
        ReportFailure(stream.str());
    }
    ValidateReplay();
    scheduler_->Cancel("");
    JoinAllActors();
    if (service_) {
        service_->SetCheckpointSinkForTesting(nullptr);
    }

    const auto* test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    if (!scheduler_->Failure().empty() ||
        (test_info != nullptr && test_info->result()->Failed())) {
        DumpFailureArtifact();
    }

    if (!policy_path_.empty()) {
        std::error_code error;
        std::filesystem::remove(policy_path_, error);
    }
}

MasterScenario& MasterScenario::Given(MemoryNodeSpec node) {
    if (frozen_) {
        ReportFailure("Given(MemoryNode) must appear before runtime actions");
        return *this;
    }
    std::ostringstream entry;
    entry << "Given MemoryNode(" << node.name << ", capacity=" << node.capacity
          << ", host=" << node.host_id << ", owner=" << node.owner;
    if (node.endpoint.has_value()) {
        entry << ", endpoint=" << *node.endpoint;
    }
    if (node.mount_local_disk) {
        entry << ", local_disk=true, local_disk_enable_offload="
              << node.local_disk_enable_offload;
    }
    entry << ")";
    RecordChoreography(entry.str());
    nodes_.push_back(std::move(node));
    return *this;
}

MasterScenario& MasterScenario::Given(TenantSpec tenant) {
    if (frozen_) {
        ReportFailure("Given(Tenant) must appear before runtime actions");
        return *this;
    }
    if (!tenant.requested_quota.has_value()) {
        ReportFailure("Given(Tenant) requires Quota(bytes)");
        return *this;
    }
    RecordChoreography("Given Tenant(" + tenant.name + ", quota=" +
                       std::to_string(*tenant.requested_quota) + ")");
    tenants_.push_back(std::move(tenant));
    return *this;
}

MasterScenario& MasterScenario::Configured(ScenarioServiceConfig config) {
    if (frozen_) {
        ReportFailure("Configured must appear before runtime actions");
        return *this;
    }
    RecordChoreography(
        "Configured(default_lease_ttl=" +
        std::to_string(config.value.default_kv_lease_ttl) +
        ", allocation_strategy=" +
        std::to_string(
            static_cast<int>(config.value.allocation_strategy_type)) +
        ", default_soft_pin_ttl=" +
        std::to_string(config.value.default_kv_soft_pin_ttl) +
        ", allow_evict_soft_pinned=" +
        std::to_string(config.value.allow_evict_soft_pinned_objects) +
        ", eviction_ratio=" + std::to_string(config.value.eviction_ratio) +
        ", enable_offload=" + std::to_string(config.value.enable_offload) +
        ", put_start_discard_timeout=" +
        std::to_string(config.value.put_start_discard_timeout_sec) +
        ", put_start_release_timeout=" +
        std::to_string(config.value.put_start_release_timeout_sec) + ")");
    service_config_ = std::move(config.value);
    return *this;
}

void MasterScenario::EnsureService() {
    if (service_) {
        return;
    }
    frozen_ = true;
    MasterServiceConfig config =
        service_config_.value_or(MasterServiceConfig{});
    if (!tenants_.empty()) {
        config.enable_multi_tenants = true;
        config.tenant_quota_connector_type = "file";

        TenantQuotaPolicySnapshot snapshot;
        for (const auto& tenant : tenants_) {
            snapshot.tenant_quotas.emplace(tenant.name,
                                           *tenant.requested_quota);
        }
        policy_path_ =
            std::filesystem::temp_directory_path() /
            ("mooncake_master_scenario_" +
             std::to_string(g_scenario_file_counter.fetch_add(1)) + ".yaml");
        std::ofstream output(policy_path_);
        output << FormatTenantQuotaPolicyYaml(snapshot);
        output.close();
        config.tenant_quota_connector_uri = policy_path_.string();
    }

    service_ = std::make_unique<MasterService>(config);
    service_->SetCheckpointSinkForTesting(scheduler_);

    if (nodes_.empty()) {
        ReportFailure("scenario requires at least one MemoryNode");
        return;
    }
    for (const auto& node : nodes_) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = node.name;
        segment.base = next_segment_base_;
        segment.size = node.capacity;
        segment.te_endpoint = node.endpoint.value_or(node.name);
        segment.host_id = node.host_id;
        next_segment_base_ += node.capacity + 4096;
        const std::string owner = node.owner.empty() ? node.name : node.owner;
        UUID client_id = ActorId(owner);
        auto result = service_->MountSegment(segment, client_id);
        if (!result) {
            ReportFailure("failed to mount node " + node.name + ": " +
                          toString(result.error()));
            return;
        }
        node_clients_[node.name] = client_id;
        node_segments_[node.name] = segment.id;
        if (node.mount_local_disk) {
            auto local_disk = service_->MountLocalDiskSegment(
                client_id, node.local_disk_enable_offload);
            if (!local_disk) {
                ReportFailure("failed to mount local disk for node " +
                              node.name + ": " + toString(local_disk.error()));
                return;
            }
        }
        if (node.owner.empty()) {
            actor_ids_[node.name] = client_id;
        }
    }
}

UUID MasterScenario::ActorId(const std::string& actor) {
    std::lock_guard lock(actor_mutex_);
    auto it = actor_ids_.find(actor);
    if (it != actor_ids_.end()) {
        return it->second;
    }
    UUID id = generate_uuid();
    actor_ids_.emplace(actor, id);
    scheduler_->RegisterActor(actor, id);
    return id;
}

ScenarioOperationResult MasterScenario::Execute(const ScenarioAction& action) {
    EnsureService();
    ScenarioOperationResult result;
    if (!service_) {
        result.error = ErrorCode::INTERNAL_ERROR;
        result.detail = "service initialization failed";
        return result;
    }

    const UUID client_id = ActorId(action.actor);
    const TenantId tenant(action.tenant);
    ReplicateConfig config;
    config.replica_num = action.replica_count;
    config.nof_replica_num = action.nof_replica_count;
    config.with_soft_pin = action.soft_pin;
    config.with_hard_pin = action.hard_pin;
    config.prefer_alloc_in_same_node = action.prefer_same_node;
    config.preferred_segment = action.preferred_segment;
    config.preferred_segments = action.preferred_segments;
    config.host_id = action.host_id;
    config.group_ids = action.group_ids;

    switch (action.kind) {
        case ScenarioActionKind::PUT_START: {
            auto value = service_->PutStart(client_id, action.key, tenant,
                                            action.size, config);
            if (value) {
                result.ok = true;
                result.replicas = std::move(value.value());
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::PUT_END: {
            auto value = service_->PutEnd(client_id, action.key, tenant,
                                          action.replica_type);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::PUT_REVOKE: {
            auto value = service_->PutRevoke(client_id, action.key, tenant,
                                             action.replica_type);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::PUT: {
            auto start = service_->PutStart(client_id, action.key, tenant,
                                            action.size, config);
            if (!start) {
                result.error = start.error();
                break;
            }
            result.replicas = std::move(start.value());
            auto end = service_->PutEnd(client_id, action.key, tenant,
                                        action.replica_type);
            result.ok = end.has_value();
            if (!end) result.error = end.error();
            break;
        }
        case ScenarioActionKind::READ: {
            auto value = service_->GetReplicaList(action.key, tenant);
            if (value) {
                result.ok = true;
                result.replicas = std::move(value->replicas);
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::UPSERT_START: {
            auto value = service_->UpsertStart(client_id, action.key, tenant,
                                               action.size, config);
            if (value) {
                result.ok = true;
                result.replicas = std::move(value.value());
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::UPSERT_END: {
            auto value = service_->UpsertEnd(client_id, action.key, tenant,
                                             action.replica_type);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::UPSERT_REVOKE: {
            auto value = service_->UpsertRevoke(client_id, action.key, tenant,
                                                action.replica_type);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::BATCH_UPSERT_START: {
            auto values = service_->BatchUpsertStart(
                client_id, action.keys, tenant, action.sizes, config);
            result.ok = values.size() == action.keys.size();
            for (auto& value : values) {
                if (value) {
                    auto replicas = std::move(value.value());
                    result.replicas.insert(
                        result.replicas.end(),
                        std::make_move_iterator(replicas.begin()),
                        std::make_move_iterator(replicas.end()));
                } else {
                    result.ok = false;
                    result.error = value.error();
                }
            }
            break;
        }
        case ScenarioActionKind::BATCH_UPSERT_END: {
            auto values =
                service_->BatchUpsertEnd(client_id, action.keys, tenant);
            result.ok = values.size() == action.keys.size();
            for (const auto& value : values) {
                if (!value) {
                    result.ok = false;
                    result.error = value.error();
                }
            }
            break;
        }
        case ScenarioActionKind::BATCH_UPSERT_REVOKE: {
            auto values =
                service_->BatchUpsertRevoke(client_id, action.keys, tenant);
            result.ok = values.size() == action.keys.size();
            for (const auto& value : values) {
                if (!value) {
                    result.ok = false;
                    result.error = value.error();
                }
            }
            break;
        }
        case ScenarioActionKind::COPY_START: {
            auto value = service_->CopyStart(client_id, action.key, tenant,
                                             action.source_segment,
                                             action.target_segments);
            if (value) {
                result.ok = true;
                result.source_segment = DescriptorSegment(value->source);
                for (const auto& target : value->targets) {
                    result.target_segments.push_back(DescriptorSegment(target));
                }
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::COPY_END: {
            auto value = service_->CopyEnd(client_id, action.key, tenant);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::COPY_REVOKE: {
            auto value = service_->CopyRevoke(client_id, action.key, tenant);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::MOVE_START: {
            const std::string target = action.target_segments.empty()
                                           ? ""
                                           : action.target_segments.front();
            auto value = service_->MoveStart(client_id, action.key, tenant,
                                             action.source_segment, target);
            if (value) {
                result.ok = true;
                result.source_segment = DescriptorSegment(value->source);
                if (value->target.has_value()) {
                    result.target_segments.push_back(
                        DescriptorSegment(*value->target));
                }
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::MOVE_END: {
            auto value = service_->MoveEnd(client_id, action.key, tenant);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::MOVE_REVOKE: {
            auto value = service_->MoveRevoke(client_id, action.key, tenant);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::ADD_REPLICA: {
            Replica replica(client_id, action.size,
                            "scenario-local-disk-endpoint",
                            ReplicaStatus::COMPLETE);
            auto value =
                service_->AddReplica(client_id, action.key, tenant, replica);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::NOTIFY_OFFLOAD_SUCCESS: {
            std::vector<OffloadTaskItem> tasks{
                {.tenant_id = tenant.value(),
                 .key = action.key,
                 .size = static_cast<int64_t>(action.size)}};
            StorageObjectMetadata metadata;
            metadata.data_size = action.size;
            metadata.transport_endpoint = action.transport_endpoint;
            auto value =
                service_->NotifyOffloadSuccess(client_id, tasks, {metadata});
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::UPSERT_TENANT_POLICY: {
            auto value = service_->UpsertTenantQuotaPolicy(tenant, action.size);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::DELETE_TENANT_POLICY: {
            auto value = service_->DeleteTenantQuotaPolicy(tenant);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::REMOVE: {
            auto value = service_->Remove(action.key, tenant, action.force);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::BATCH_REMOVE: {
            auto values =
                service_->BatchRemove(action.keys, tenant, action.force);
            result.ok = values.size() == action.keys.size();
            for (const auto& value : values) {
                if (value) {
                    ++result.affected_count;
                } else {
                    result.ok = false;
                    result.error = value.error();
                }
            }
            break;
        }
        case ScenarioActionKind::BATCH_REPLICA_CLEAR: {
            auto value = service_->BatchReplicaClear(action.keys, client_id,
                                                     action.source_segment);
            if (value) {
                result.ok = true;
                result.affected_count = value->size();
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::CREATE_COPY_TASK: {
            auto value = service_->CreateCopyTask(action.key, tenant,
                                                  action.target_segments);
            if (value) {
                result.ok = true;
                result.task_id = value.value();
                auto task = service_->QueryTask(value.value());
                if (task) {
                    result.task = std::move(task.value());
                }
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::CREATE_MOVE_TASK: {
            const std::string target = action.target_segments.empty()
                                           ? ""
                                           : action.target_segments.front();
            auto value = service_->CreateMoveTask(
                action.key, tenant, action.source_segment, target);
            if (value) {
                result.ok = true;
                result.task_id = value.value();
                auto task = service_->QueryTask(value.value());
                if (task) {
                    result.task = std::move(task.value());
                }
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::FETCH_TASKS: {
            tl::expected<std::vector<TaskAssignment>, ErrorCode> value =
                std::vector<TaskAssignment>{};
            const auto timeout =
                action.wait.count() > 0 ? action.wait : std::chrono::seconds(4);
            const auto deadline = std::chrono::steady_clock::now() + timeout;
            do {
                value = service_->FetchTasks(client_id, action.size);
                if (!value || !value->empty() || !action.retry_until_nonempty) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            } while (std::chrono::steady_clock::now() < deadline);
            if (value) {
                result.ok = true;
                result.affected_count = value->size();
                for (const auto& assignment : value.value()) {
                    result.task_ids.push_back(assignment.id);
                    result.task_assignments.push_back(assignment);
                }
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::COMPLETE_TASK:
        case ScenarioActionKind::QUERY_TASK: {
            auto aliased = results_.find(action.task_alias);
            if (aliased == results_.end() ||
                !aliased->second.task_id.has_value()) {
                result.error = ErrorCode::TASK_NOT_FOUND;
                break;
            }
            if (action.kind == ScenarioActionKind::COMPLETE_TASK) {
                TaskCompleteRequest request{};
                request.id = *aliased->second.task_id;
                request.status =
                    action.expected_task_status.value_or(TaskStatus::SUCCESS);
                request.message = action.message;
                auto completed =
                    service_->MarkTaskToComplete(client_id, request);
                if (!completed) {
                    result.error = completed.error();
                    break;
                }
            }
            auto task = service_->QueryTask(*aliased->second.task_id);
            if (task) {
                result.ok = true;
                result.task_id = *aliased->second.task_id;
                result.task = std::move(task.value());
            } else {
                result.error = task.error();
            }
            break;
        }
        case ScenarioActionKind::CREATE_DRAIN_JOB: {
            CreateDrainJobRequest request;
            request.segments = action.keys;
            request.target_segments = action.target_segments;
            request.max_concurrency = action.max_concurrency;
            auto value = service_->CreateDrainJob(request);
            if (value) {
                result.ok = true;
                result.job_id = value.value();
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::CANCEL_DRAIN_JOB: {
            auto aliased = results_.find(action.task_alias);
            if (aliased == results_.end() ||
                !aliased->second.job_id.has_value()) {
                result.error = ErrorCode::TASK_NOT_FOUND;
                break;
            }
            auto value = service_->CancelDrainJob(*aliased->second.job_id);
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            result.job_id = aliased->second.job_id;
            break;
        }
        case ScenarioActionKind::EXECUTE_NEXT_MOVE_TASK: {
            const auto timeout =
                action.wait.count() > 0 ? action.wait : std::chrono::seconds(4);
            const auto deadline = std::chrono::steady_clock::now() + timeout;
            tl::expected<std::vector<TaskAssignment>, ErrorCode> fetched =
                std::vector<TaskAssignment>{};
            do {
                fetched = service_->FetchTasks(client_id, 16);
                if (!fetched || !fetched->empty()) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            } while (std::chrono::steady_clock::now() < deadline);
            if (!fetched) {
                result.error = fetched.error();
                break;
            }
            for (const auto& assignment : fetched.value()) {
                if (assignment.type != TaskType::REPLICA_MOVE) {
                    continue;
                }
                if (action.completion_status == TaskStatus::SUCCESS) {
                    ReplicaMovePayload payload;
                    struct_json::from_json(payload, assignment.payload);
                    auto started = service_->MoveStart(
                        client_id, payload.key, TenantId(payload.tenant_id),
                        payload.source, payload.target);
                    if (!started) {
                        result.error = started.error();
                        break;
                    }
                    auto ended = service_->MoveEnd(client_id, payload.key,
                                                   TenantId(payload.tenant_id));
                    if (!ended) {
                        result.error = ended.error();
                        break;
                    }
                }
                TaskCompleteRequest request{};
                request.id = assignment.id;
                request.status = action.completion_status;
                request.message =
                    action.completion_status == TaskStatus::SUCCESS
                        ? "move_done"
                        : "move_failed";
                auto completed =
                    service_->MarkTaskToComplete(client_id, request);
                if (!completed) {
                    result.error = completed.error();
                    break;
                }
                result.task_ids.push_back(assignment.id);
                result.task_assignments.push_back(assignment);
                ++result.affected_count;
            }
            result.ok =
                result.error == ErrorCode::OK && result.affected_count > 0;
            if (!result.ok && result.error == ErrorCode::OK) {
                result.error = ErrorCode::TASK_NOT_FOUND;
            }
            break;
        }
        case ScenarioActionKind::REMOVE_BY_REGEX: {
            auto value =
                service_->RemoveByRegex(action.key, tenant, action.force);
            if (value) {
                result.ok = true;
                result.affected_count = value.value();
            } else {
                result.error = value.error();
            }
            break;
        }
        case ScenarioActionKind::REMOVE_ALL:
            result.affected_count = service_->RemoveAll(action.force);
            result.ok = true;
            break;
        case ScenarioActionKind::REMOVE_ALL_TENANT:
            result.affected_count = service_->RemoveAll(tenant, action.force);
            result.ok = true;
            break;
        case ScenarioActionKind::UNMOUNT_NODE: {
            auto segment = node_segments_.find(action.node);
            if (segment == node_segments_.end()) {
                result.error = ErrorCode::SEGMENT_NOT_FOUND;
                break;
            }
            auto value = service_->UnmountSegment(segment->second,
                                                  ActorId(action.actor));
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::GRACEFUL_UNMOUNT_NODE: {
            auto segment = node_segments_.find(action.node);
            if (segment == node_segments_.end()) {
                result.error = ErrorCode::SEGMENT_NOT_FOUND;
                break;
            }
            auto value = service_->GracefulUnmountSegment(
                segment->second, ActorId(action.actor), action.wait.count());
            result.ok = value.has_value();
            if (!value) result.error = value.error();
            break;
        }
        case ScenarioActionKind::WAIT:
            std::this_thread::sleep_for(action.wait);
            result.ok = true;
            break;
    }
    result.detail = result.ok ? "OK" : toString(result.error);
    return result;
}

bool MasterScenario::ValidateResult(const ScenarioAction& action,
                                    const ScenarioOperationResult& result) {
    std::string failure;
    if (action.expected_error.has_value()) {
        if (result.ok || result.error != *action.expected_error) {
            std::ostringstream stream;
            stream << OperationName(action.kind) << "(" << action.key << ") by "
                   << action.actor << " expected "
                   << toString(*action.expected_error) << " but got "
                   << result.detail;
            failure = stream.str();
        }
    } else if (!result.ok) {
        failure = OperationName(action.kind) + "(" + action.key + ") by " +
                  action.actor + " failed: " + result.detail;
    }
    if (failure.empty() && result.ok &&
        action.expected_replica_count.has_value() &&
        result.replicas.size() != *action.expected_replica_count) {
        failure = OperationName(action.kind) + "(" + action.key +
                  ") expected " +
                  std::to_string(*action.expected_replica_count) +
                  " replicas, got " + std::to_string(result.replicas.size());
    }
    if (failure.empty() && result.ok &&
        action.expected_replica_count.has_value()) {
        for (const auto& replica : result.replicas) {
            if (replica.status != ReplicaStatus::PROCESSING ||
                DescriptorSize(replica) != action.size) {
                failure = OperationName(action.kind) + "(" + action.key +
                          ") returned an invalid processing replica";
                break;
            }
        }
    }
    if (failure.empty() && result.ok &&
        action.expected_target_count.has_value() &&
        result.target_segments.size() != *action.expected_target_count) {
        failure =
            OperationName(action.kind) + "(" + action.key + ") expected " +
            std::to_string(*action.expected_target_count) + " targets, got " +
            std::to_string(result.target_segments.size());
    }
    if (failure.empty() && result.ok &&
        action.expected_source_segment.has_value() &&
        result.source_segment != *action.expected_source_segment) {
        failure = OperationName(action.kind) + "(" + action.key +
                  ") expected source " + *action.expected_source_segment +
                  ", got " + result.source_segment;
    }
    if (failure.empty() && result.ok &&
        action.expected_affected_count.has_value() &&
        result.affected_count != *action.expected_affected_count) {
        failure = OperationName(action.kind) + "(" + action.key +
                  ") expected affected count " +
                  std::to_string(*action.expected_affected_count) + ", got " +
                  std::to_string(result.affected_count);
    }
    if (failure.empty() && result.ok &&
        action.kind == ScenarioActionKind::FETCH_TASKS &&
        action.expected_task_aliases.has_value()) {
        std::vector<UUID> expected;
        for (const auto& alias : *action.expected_task_aliases) {
            auto task = results_.find(alias);
            if (task == results_.end() || !task->second.task_id.has_value()) {
                failure = "unknown task result alias " + alias;
                break;
            }
            expected.push_back(*task->second.task_id);
        }
        auto observed = result.task_ids;
        std::sort(expected.begin(), expected.end());
        std::sort(observed.begin(), observed.end());
        if (failure.empty() && expected != observed) {
            failure =
                "FetchTasks by " + action.actor + " returned unexpected tasks";
        }
    }
    if (failure.empty() && result.ok &&
        (action.expected_payload_tenant.has_value() ||
         action.expected_payload_key.has_value())) {
        for (const auto& assignment : result.task_assignments) {
            std::string tenant;
            std::string key;
            if (assignment.type == TaskType::REPLICA_COPY) {
                ReplicaCopyPayload payload;
                struct_json::from_json(payload, assignment.payload);
                tenant = std::move(payload.tenant_id);
                key = std::move(payload.key);
            } else if (assignment.type == TaskType::REPLICA_MOVE) {
                ReplicaMovePayload payload;
                struct_json::from_json(payload, assignment.payload);
                tenant = std::move(payload.tenant_id);
                key = std::move(payload.key);
            } else {
                failure = OperationName(action.kind) +
                          " returned an unsupported task payload type";
                break;
            }
            if (action.expected_payload_tenant.has_value() &&
                tenant != *action.expected_payload_tenant) {
                failure = OperationName(action.kind) +
                          " task payload tenant mismatch";
                break;
            }
            if (action.expected_payload_key.has_value() &&
                key != *action.expected_payload_key) {
                failure =
                    OperationName(action.kind) + " task payload key mismatch";
                break;
            }
        }
    }
    if (failure.empty() && result.ok && action.expected_task_type.has_value() &&
        (!result.task.has_value() ||
         result.task->type != *action.expected_task_type)) {
        failure = OperationName(action.kind) + " expected task type mismatch";
    }
    if (failure.empty() && result.ok &&
        action.expected_task_status.has_value() &&
        (!result.task.has_value() ||
         result.task->status != *action.expected_task_status)) {
        failure = OperationName(action.kind) + " expected task status mismatch";
    }
    if (failure.empty() && result.ok &&
        action.expected_assigned_actor.has_value()) {
        if (!result.task.has_value() ||
            result.task->assigned_client !=
                ActorId(*action.expected_assigned_actor)) {
            failure = OperationName(action.kind) +
                      " expected assigned actor mismatch";
        }
    }
    if (failure.empty() && result.ok && !action.message.empty() &&
        (!result.task.has_value() || result.task->message != action.message)) {
        failure = OperationName(action.kind) + " task message mismatch";
    }
    const auto validate_allocation_relation =
        [&](const std::optional<std::string>& alias,
            bool expect_same) -> std::string {
        if (!alias.has_value()) {
            return "";
        }
        auto previous = results_.find(*alias);
        if (previous == results_.end()) {
            return "unknown result alias " + *alias;
        }
        if (result.replicas.empty() || previous->second.replicas.empty()) {
            return "allocation comparison requires replica results";
        }
        const bool same = AllocationIdentity(result.replicas.front()) ==
                          AllocationIdentity(previous->second.replicas.front());
        if (same != expect_same) {
            return OperationName(action.kind) + "(" + action.key +
                   ") allocation was unexpectedly " +
                   (same ? "reused" : "reallocated");
        }
        return "";
    };
    if (failure.empty()) {
        failure =
            validate_allocation_relation(action.expected_same_allocation, true);
    }
    if (failure.empty()) {
        failure = validate_allocation_relation(
            action.expected_different_allocation, false);
    }
    if (!failure.empty()) {
        scheduler_->Cancel(failure);
        ReportFailure(failure);
        return false;
    }
    return true;
}

MasterScenario& MasterScenario::When(ScenarioAction action) {
    RecordChoreography("When " + DescribeAction(action));
    const auto result = Execute(action);
    scheduler_->RecordOperation(
        "OPERATION_SYNC", action.actor, OperationName(action.kind),
        action.tenant, action.key, ActorId(action.actor), result.detail);
    ValidateResult(action, result);
    if (!action.alias.empty()) {
        results_[action.alias] = result;
    }
    return *this;
}

MasterScenario& MasterScenario::Parallel(
    std::initializer_list<ScenarioAction> actions) {
    EnsureService();
    std::vector<std::string> actors;
    actors.reserve(actions.size());
    for (const auto& action : actions) {
        RecordChoreography("Parallel " + DescribeAction(action));
        actors.push_back(action.actor);
        StartActor(action.actor, action);
    }
    for (const auto& actor : actors) {
        JoinActor(actor);
    }
    return *this;
}

MasterScenario& MasterScenario::Then(ObjectSpec object) {
    std::ostringstream entry;
    entry << "Then Object(" << object.tenant << "/" << object.key;
    if (object.expected_exists.has_value()) {
        entry << ", exists=" << (*object.expected_exists ? "true" : "false");
    }
    entry << ", readability=" << static_cast<int>(object.readability);
    if (object.complete_replicas.has_value()) {
        entry << ", complete_replicas=" << *object.complete_replicas;
    }
    if (object.size.has_value()) {
        entry << ", size=" << *object.size;
    }
    if (object.replica_segments.has_value()) {
        entry << ", segments=[";
        for (size_t index = 0; index < object.replica_segments->size();
             ++index) {
            if (index != 0) {
                entry << ",";
            }
            entry << (*object.replica_segments)[index];
        }
        entry << "]";
    }
    if (object.distinct_segments.has_value()) {
        entry << ", distinct_segments=" << *object.distinct_segments;
    }
    entry << ")";
    RecordChoreography(entry.str());
    EnsureService();
    const TenantId tenant(object.tenant);
    if (object.expected_exists.has_value()) {
        auto exists = service_->ExistKey(object.key, tenant);
        if (!exists || exists.value() != *object.expected_exists) {
            ReportFailure("object " + object.tenant + "/" + object.key +
                          " existence mismatch");
        }
    }

    auto replicas = service_->GetReplicaListForAdmin(object.key, tenant);
    if (object.readability != ObjectSpec::Readability::UNSPECIFIED) {
        auto readable = service_->GetReplicaList(object.key, tenant);
        if (object.readability == ObjectSpec::Readability::READABLE) {
            if (!readable) {
                ReportFailure(
                    "object " + object.tenant + "/" + object.key +
                    " is not readable: " + toString(readable.error()));
                return *this;
            }
            replicas = std::move(readable);
        } else if (readable ||
                   readable.error() != ErrorCode::REPLICA_IS_NOT_READY) {
            ReportFailure("object " + object.tenant + "/" + object.key +
                          " was expected to be not ready");
            return *this;
        }
    }
    if (!replicas) {
        return *this;
    }

    if (object.complete_replicas.has_value()) {
        const size_t complete =
            std::count_if(replicas->replicas.begin(), replicas->replicas.end(),
                          [](const Replica::Descriptor& replica) {
                              return replica.status == ReplicaStatus::COMPLETE;
                          });
        if (complete != *object.complete_replicas) {
            ReportFailure(
                "object " + object.tenant + "/" + object.key + " expected " +
                std::to_string(*object.complete_replicas) +
                " complete replicas, got " + std::to_string(complete));
        }
    }
    if (object.size.has_value() &&
        (replicas->replicas.empty() ||
         DescriptorSize(replicas->replicas.front()) != *object.size)) {
        ReportFailure("object " + object.tenant + "/" + object.key +
                      " size mismatch");
    }
    if (object.replica_segments.has_value()) {
        std::vector<std::string> observed;
        observed.reserve(replicas->replicas.size());
        for (const auto& replica : replicas->replicas) {
            observed.push_back(DescriptorSegment(replica));
        }
        auto expected = *object.replica_segments;
        std::sort(observed.begin(), observed.end());
        std::sort(expected.begin(), expected.end());
        if (observed != expected) {
            ReportFailure("object " + object.tenant + "/" + object.key +
                          " replica segments mismatch");
        }
    }
    if (object.distinct_segments.has_value()) {
        std::unordered_set<std::string> segments;
        for (const auto& replica : replicas->replicas) {
            segments.insert(DescriptorSegment(replica));
        }
        if (segments.size() != *object.distinct_segments) {
            ReportFailure("object " + object.tenant + "/" + object.key +
                          " distinct replica segment count mismatch");
        }
    }
    return *this;
}

MasterScenario& MasterScenario::Eventually(ObjectSpec object,
                                           std::chrono::milliseconds timeout) {
    RecordChoreography("Eventually Object(" + object.tenant + "/" + object.key +
                       ", timeout_ms=" + std::to_string(timeout.count()) + ")");
    EnsureService();
    const TenantId tenant(object.tenant);
    const auto matches = [&]() {
        if (object.expected_exists.has_value()) {
            // ExistKey grants a lease. Polling it would change the eviction
            // behavior that an Eventually(DoesNotExist) assertion observes.
            const bool exists =
                service_->GetReplicaListForAdmin(object.key, tenant)
                    .has_value();
            if (exists != *object.expected_exists) {
                return false;
            }
        }
        if (object.readability == ObjectSpec::Readability::READABLE) {
            return service_->GetReplicaList(object.key, tenant).has_value();
        }
        if (object.readability == ObjectSpec::Readability::NOT_READY) {
            auto replicas = service_->GetReplicaList(object.key, tenant);
            return !replicas &&
                   replicas.error() == ErrorCode::REPLICA_IS_NOT_READY;
        }
        return true;
    };
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline && !matches()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return Then(std::move(object));
}

MasterScenario& MasterScenario::Then(MatchingObjectsSpec objects) {
    std::ostringstream entry;
    entry << "Then MatchingObjects(" << objects.tenant << "/"
          << objects.pattern;
    if (objects.expected_count.has_value()) {
        entry << ", count=" << *objects.expected_count;
    }
    entry << ")";
    RecordChoreography(entry.str());
    EnsureService();
    auto result = service_->GetReplicaListByRegex(objects.pattern,
                                                  TenantId(objects.tenant));
    if (!result) {
        ReportFailure("regex lookup " + objects.tenant + "/" + objects.pattern +
                      " failed: " + toString(result.error()));
        return *this;
    }
    if (objects.expected_count.has_value() &&
        result->size() != *objects.expected_count) {
        ReportFailure("regex lookup " + objects.tenant + "/" + objects.pattern +
                      " count mismatch");
    }
    if (objects.expected_keys.has_value()) {
        std::vector<std::string> observed;
        observed.reserve(result->size());
        for (const auto& [key, _] : result.value()) {
            observed.push_back(key);
        }
        auto expected = *objects.expected_keys;
        std::sort(observed.begin(), observed.end());
        std::sort(expected.begin(), expected.end());
        if (observed != expected) {
            ReportFailure("regex lookup " + objects.tenant + "/" +
                          objects.pattern + " keys mismatch");
        }
    }
    return *this;
}

MasterScenario& MasterScenario::Then(ObjectExistenceSpec objects) {
    RecordChoreography("Then ObjectExistence(" + objects.tenant +
                       ", count=" + std::to_string(objects.keys.size()) + ")");
    EnsureService();
    if (objects.expected.size() != objects.keys.size()) {
        ReportFailure("object existence assertion size mismatch");
        return *this;
    }
    auto result =
        service_->BatchExistKey(objects.keys, TenantId(objects.tenant));
    if (result.size() != objects.expected.size()) {
        ReportFailure("batch existence result size mismatch");
        return *this;
    }
    for (size_t index = 0; index < result.size(); ++index) {
        if (!result[index] ||
            result[index].value() != objects.expected[index]) {
            ReportFailure("batch existence mismatch at index " +
                          std::to_string(index) + " for key " +
                          objects.keys[index]);
            break;
        }
    }
    return *this;
}

MasterScenario& MasterScenario::Then(BatchObjectsSpec objects) {
    RecordChoreography("Then BatchObjects(" + objects.tenant +
                       ", count=" + std::to_string(objects.keys.size()) + ")");
    EnsureService();
    if (objects.expected.size() != objects.keys.size()) {
        ReportFailure("batch objects assertion size mismatch");
        return *this;
    }
    auto result =
        service_->BatchGetReplicaList(objects.keys, TenantId(objects.tenant));
    if (result.size() != objects.expected.size()) {
        ReportFailure("batch get result size mismatch");
        return *this;
    }
    for (size_t index = 0; index < result.size(); ++index) {
        bool matches = false;
        switch (objects.expected[index]) {
            case BatchObjectsSpec::State::READABLE:
                matches = result[index].has_value() &&
                          !result[index]->replicas.empty();
                break;
            case BatchObjectsSpec::State::MISSING:
                matches = !result[index].has_value() &&
                          result[index].error() == ErrorCode::OBJECT_NOT_FOUND;
                break;
            case BatchObjectsSpec::State::NOT_READY:
                matches =
                    !result[index].has_value() &&
                    result[index].error() == ErrorCode::REPLICA_IS_NOT_READY;
                break;
        }
        if (!matches) {
            ReportFailure("batch get mismatch at index " +
                          std::to_string(index) + " for key " +
                          objects.keys[index]);
            break;
        }
    }
    return *this;
}

MasterScenario& MasterScenario::Then(AllKeysSpec keys) {
    RecordChoreography("Then AllKeys(" + keys.tenant +
                       ", count=" + std::to_string(keys.expected.size()) + ")");
    EnsureService();
    auto result = service_->GetAllKeys(TenantId(keys.tenant));
    if (!result) {
        ReportFailure("get all keys for tenant " + keys.tenant +
                      " failed: " + toString(result.error()));
        return *this;
    }
    auto observed = std::move(result.value());
    auto expected = std::move(keys.expected);
    std::sort(observed.begin(), observed.end());
    std::sort(expected.begin(), expected.end());
    if (observed != expected) {
        ReportFailure("get all keys for tenant " + keys.tenant + " mismatch");
    }
    return *this;
}

MasterScenario& MasterScenario::Then(ClientIpsSpec clients) {
    RecordChoreography(
        "Then ClientIps(count=" + std::to_string(clients.actors.size()) + ")");
    EnsureService();
    std::vector<UUID> client_ids;
    std::map<std::string, UUID> ids_by_actor;
    client_ids.reserve(clients.actors.size());
    for (const auto& actor : clients.actors) {
        UUID id = ActorId(actor);
        client_ids.push_back(id);
        ids_by_actor.emplace(actor, id);
    }
    auto result = service_->BatchQueryIp(client_ids);
    if (!result) {
        ReportFailure("batch query IP failed: " + toString(result.error()));
        return *this;
    }
    for (const auto& actor : clients.actors) {
        auto expected = clients.expected.find(actor);
        auto id = ids_by_actor.find(actor);
        auto observed = result->find(id->second);
        if (expected == clients.expected.end()) {
            if (observed != result->end()) {
                ReportFailure("unexpected IP result for actor " + actor);
            }
            continue;
        }
        if (observed == result->end()) {
            ReportFailure("missing IP result for actor " + actor);
            continue;
        }
        auto expected_ips = expected->second;
        auto observed_ips = observed->second;
        std::sort(expected_ips.begin(), expected_ips.end());
        std::sort(observed_ips.begin(), observed_ips.end());
        if (expected_ips != observed_ips) {
            ReportFailure("IP result mismatch for actor " + actor);
        }
    }
    if (result->size() != clients.expected.size()) {
        ReportFailure("batch query IP result count mismatch");
    }
    return *this;
}

MasterScenario& MasterScenario::Then(SegmentSpec segment) {
    RecordChoreography("Then SegmentState(" + segment.name + ")");
    EnsureService();
    auto status = service_->QuerySegmentStatus(segment.name);
    if (segment.expected_unmounted) {
        if (status.has_value() && status.value() != SegmentStatus::UNDEFINED) {
            ReportFailure("segment " + segment.name +
                          " was expected to be unmounted");
        }
        return *this;
    }
    if (!segment.expected_status.has_value()) {
        ReportFailure("segment assertion requires a status");
        return *this;
    }
    if (!status || status.value() != *segment.expected_status) {
        ReportFailure("segment " + segment.name + " status mismatch");
    }
    return *this;
}

MasterScenario& MasterScenario::Then(JobSpec job) {
    RecordChoreography("Then Job(" + job.alias + ")");
    EnsureService();
    const std::string mismatch = JobMismatch(job);
    if (!mismatch.empty()) {
        ReportFailure(mismatch);
    }
    return *this;
}

MasterScenario& MasterScenario::Eventually(JobSpec job,
                                           std::chrono::milliseconds timeout) {
    RecordChoreography("Eventually Job(" + job.alias +
                       ", timeout_ms=" + std::to_string(timeout.count()) + ")");
    EnsureService();
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string mismatch;
    do {
        mismatch = JobMismatch(job);
        if (mismatch.empty()) {
            return *this;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    } while (std::chrono::steady_clock::now() < deadline);
    mismatch = JobMismatch(job);
    if (!mismatch.empty()) {
        ReportFailure(mismatch);
    }
    return *this;
}

std::string MasterScenario::JobMismatch(const JobSpec& job) {
    auto aliased = results_.find(job.alias);
    if (aliased == results_.end() || !aliased->second.job_id.has_value()) {
        return "unknown job result alias " + job.alias;
    }
    auto result = service_->QueryDrainJob(*aliased->second.job_id);
    if (!result) {
        return "query job " + job.alias +
               " failed: " + toString(result.error());
    }
    if (result->status != job.expected_status) {
        return "job " + job.alias + " status mismatch";
    }
    if (job.expected_active_units.has_value() &&
        result->active_units != *job.expected_active_units) {
        return "job " + job.alias + " active unit count mismatch";
    }
    if (job.minimum_succeeded_units.has_value() &&
        result->succeeded_units < *job.minimum_succeeded_units) {
        return "job " + job.alias + " succeeded unit count mismatch";
    }
    if (job.minimum_failed_units.has_value() &&
        result->failed_units < *job.minimum_failed_units) {
        return "job " + job.alias + " failed unit count mismatch";
    }
    return "";
}

MasterScenario& MasterScenario::Then(TenantSpec tenant) {
    std::ostringstream entry;
    entry << "Then Tenant(" << tenant.name;
    if (tenant.expected_exists.has_value()) {
        entry << ", exists=" << (*tenant.expected_exists ? "true" : "false");
    }
    if (tenant.expected_used.has_value()) {
        entry << ", used=" << *tenant.expected_used;
    }
    if (tenant.expected_reserved.has_value()) {
        entry << ", reserved=" << *tenant.expected_reserved;
    }
    if (tenant.expected_committed_count.has_value()) {
        entry << ", committed=" << *tenant.expected_committed_count;
    }
    if (tenant.expected_effective_quota.has_value()) {
        entry << ", effective_quota=" << *tenant.expected_effective_quota;
    }
    entry << ")";
    RecordChoreography(entry.str());
    EnsureService();
    auto snapshot = service_->GetTenantQuotaSnapshot(TenantId(tenant.name));
    if (tenant.expected_exists == false) {
        if (snapshot.has_value()) {
            ReportFailure("tenant " + tenant.name +
                          " unexpectedly has a quota snapshot");
        }
        return *this;
    }
    if (!snapshot) {
        ReportFailure("tenant " + tenant.name + " has no quota snapshot");
        return *this;
    }
    if (tenant.expected_used && snapshot->used_bytes != *tenant.expected_used) {
        ReportFailure("tenant " + tenant.name + " used bytes mismatch");
    }
    if (tenant.expected_reserved &&
        snapshot->reserved_bytes != *tenant.expected_reserved) {
        ReportFailure("tenant " + tenant.name + " reserved bytes mismatch");
    }
    if (tenant.expected_committed_count &&
        snapshot->committed_count != *tenant.expected_committed_count) {
        ReportFailure("tenant " + tenant.name + " committed count mismatch");
    }
    if (tenant.expected_effective_quota &&
        snapshot->effective_quota_bytes != *tenant.expected_effective_quota) {
        ReportFailure("tenant " + tenant.name + " effective quota mismatch");
    }
    return *this;
}

void MasterScenario::StartActor(std::string actor, ScenarioAction action) {
    EnsureService();
    if (threads_.contains(actor)) {
        const std::string failure =
            "actor " + actor + " already has a running operation";
        scheduler_->Cancel(failure);
        ReportFailure(failure);
        return;
    }
    action.By(actor);
    const UUID client_id = ActorId(actor);
    auto state = std::make_unique<ActorThread>();
    state->action = action;
    ActorThread* state_ptr = state.get();
    state->thread = std::thread([this, actor, client_id, state_ptr] {
        scheduler_->RecordOperation(
            "OPERATION_BEGIN", actor, OperationName(state_ptr->action.kind),
            state_ptr->action.tenant, state_ptr->action.key, client_id);
        try {
            state_ptr->result = Execute(state_ptr->action);
        } catch (const std::exception& error) {
            ScenarioOperationResult result;
            result.error = ErrorCode::INTERNAL_ERROR;
            result.detail = std::string("exception: ") + error.what();
            state_ptr->result = std::move(result);
        } catch (...) {
            ScenarioOperationResult result;
            result.error = ErrorCode::INTERNAL_ERROR;
            result.detail = "unknown exception";
            state_ptr->result = std::move(result);
        }
        scheduler_->RecordOperation(
            "OPERATION_END", actor, OperationName(state_ptr->action.kind),
            state_ptr->action.tenant, state_ptr->action.key, client_id,
            state_ptr->result ? state_ptr->result->detail : "no result");
    });
    threads_.emplace(std::move(actor), std::move(state));
}

void MasterScenario::JoinActor(const std::string& actor) {
    auto it = threads_.find(actor);
    if (it == threads_.end()) {
        const std::string failure =
            "actor " + actor + " has no running operation";
        scheduler_->Cancel(failure);
        ReportFailure(failure);
        return;
    }
    if (it->second->thread.joinable()) {
        it->second->thread.join();
    }
    if (!it->second->result) {
        ReportFailure("actor " + actor + " completed without a result");
    } else {
        ValidateResult(it->second->action, *it->second->result);
        if (!it->second->action.alias.empty()) {
            results_[it->second->action.alias] = *it->second->result;
        }
    }
    threads_.erase(it);
}

void MasterScenario::JoinAllActors() {
    for (auto& [_, actor] : threads_) {
        if (actor->thread.joinable()) {
            actor->thread.join();
        }
    }
    threads_.clear();
}

MasterScenario& MasterScenario::Interleave(
    std::initializer_list<InterleaveCommand> commands) {
    EnsureService();
    for (const auto& command : commands) {
        if (!scheduler_->Failure().empty()) {
            break;
        }
        switch (command.kind) {
            case InterleaveCommandKind::RUN_UNTIL: {
                RecordChoreography("RunUntil " +
                                   DescribeAction(*command.action) + " @ " +
                                   CheckpointName(*command.checkpoint));
                scheduler_->Arm(command.actor, *command.checkpoint);
                StartActor(command.actor, *command.action);
                std::string error;
                if (!scheduler_->WaitUntilReached(
                        command.actor, *command.checkpoint, &error)) {
                    ReportFailure(error);
                }
                break;
            }
            case InterleaveCommandKind::START:
                RecordChoreography("Start " + DescribeAction(*command.action));
                StartActor(command.actor, *command.action);
                break;
            case InterleaveCommandKind::RESUME: {
                RecordChoreography("Resume " + command.actor);
                std::string error;
                if (!scheduler_->Resume(command.actor, &error)) {
                    scheduler_->Cancel(error);
                    ReportFailure(error);
                }
                break;
            }
            case InterleaveCommandKind::JOIN:
                RecordChoreography("Join " + command.actor);
                JoinActor(command.actor);
                break;
        }
    }

    if (!scheduler_->Failure().empty()) {
        scheduler_->Cancel(scheduler_->Failure());
        JoinAllActors();
        ReportFailure(scheduler_->Failure());
    }
    return *this;
}

const ScenarioOperationResult* MasterScenario::Result(
    const std::string& alias) const {
    auto it = results_.find(alias);
    return it == results_.end() ? nullptr : &it->second;
}

MasterService& MasterScenario::ServiceForTesting() {
    EnsureService();
    return *service_;
}

ScenarioCheckpointScheduler& MasterScenario::SchedulerForTesting() {
    return *scheduler_;
}

void MasterScenario::ReportFailure(const std::string& message) {
    ADD_FAILURE() << "MasterScenario[" << name_ << "]: " << message;
}

void MasterScenario::LoadReplayIfRequested() {
    const char* path = std::getenv("MOONCAKE_SCENARIO_REPLAY");
    if (!path || *path == '\0') {
        return;
    }
    try {
        std::ifstream input(path);
        if (!input) {
            ReportFailure(std::string("cannot open replay artifact: ") + path);
            return;
        }
        std::stringstream buffer;
        buffer << input.rdbuf();
        ScenarioReplayArtifact artifact;
        struct_json::from_json(artifact, buffer.str());
        if (auto error =
                ValidateScenarioReplayArtifact(artifact, name_, test_name_)) {
            ReportFailure(*error);
            return;
        }
        replay_choreography_ = std::move(artifact.choreography);
        scheduler_->SetReplay(std::move(artifact.releases));
    } catch (const std::exception& error) {
        ReportFailure(std::string("failed to parse replay artifact: ") +
                      error.what());
    }
}

void MasterScenario::DumpFailureArtifact() {
    if (artifact_dumped_) {
        return;
    }
    artifact_dumped_ = true;

    const char* output_dir = std::getenv("TEST_UNDECLARED_OUTPUTS_DIR");
    std::filesystem::path directory =
        output_dir && *output_dir ? std::filesystem::path(output_dir)
                                  : std::filesystem::temp_directory_path() /
                                        "mooncake-master-scenario";
    std::error_code error;
    std::filesystem::create_directories(directory, error);
    if (error) {
        LOG(ERROR) << "failed to create scenario artifact directory: "
                   << error.message();
        return;
    }

    ScenarioReplayArtifact artifact;
    artifact.scenario = name_;
    artifact.test = test_name_;
    artifact.choreography = choreography_;
    artifact.releases = scheduler_->Releases();
    artifact.trace = scheduler_->Trace();
    artifact.failure = scheduler_->Failure();
    if (artifact.failure.empty()) {
        artifact.failure = "gtest assertion failure";
    }
    std::string json;
    struct_json::to_json(artifact, json);

    const auto path =
        directory / (SanitizeFilename(name_) + "_" +
                     std::to_string(g_scenario_file_counter.fetch_add(1)) +
                     ".schedule.json");
    std::ofstream output(path);
    output << json;
    output.close();
    LOG(ERROR) << "MasterScenario replay artifact: " << path;
}

void MasterScenario::ValidateReplay() {
    if (replay_choreography_.has_value() &&
        *replay_choreography_ != choreography_) {
        const size_t common =
            std::min(replay_choreography_->size(), choreography_.size());
        size_t mismatch = 0;
        while (mismatch < common &&
               (*replay_choreography_)[mismatch] == choreography_[mismatch]) {
            ++mismatch;
        }
        std::ostringstream stream;
        stream << "replay choreography mismatch at entry " << mismatch;
        if (mismatch < replay_choreography_->size()) {
            stream << ": expected " << (*replay_choreography_)[mismatch];
        }
        if (mismatch < choreography_.size()) {
            stream << ", observed " << choreography_[mismatch];
        }
        ReportFailure(stream.str());
    }

    std::string replay_error;
    if (!scheduler_->ValidateReplayComplete(&replay_error)) {
        ReportFailure(replay_error);
    }
}

void MasterScenario::RecordChoreography(const std::string& entry) {
    choreography_.push_back(entry);
}

std::string MasterScenario::DescribeAction(const ScenarioAction& action) {
    std::ostringstream stream;
    stream << OperationName(action.kind) << "(" << action.tenant;
    if (!action.key.empty()) {
        stream << "/" << action.key;
    }
    if (!action.keys.empty()) {
        stream << "/[";
        for (size_t index = 0; index < action.keys.size(); ++index) {
            if (index != 0) {
                stream << ",";
            }
            stream << action.keys[index];
        }
        stream << "]";
    }
    if (action.size != 0) {
        stream << ", " << action.size << " bytes";
    }
    stream << ", replicas=" << action.replica_count
           << ", nof_replicas=" << action.nof_replica_count
           << ", soft_pin=" << action.soft_pin
           << ", hard_pin=" << action.hard_pin
           << ", prefer_same_node=" << action.prefer_same_node
           << ", force=" << action.force
           << ", replica_type=" << static_cast<int>(action.replica_type);
    if (!action.preferred_segment.empty()) {
        stream << ", preferred_segment=" << action.preferred_segment;
    }
    if (!action.preferred_segments.empty()) {
        stream << ", preferred_segments=[";
        for (size_t index = 0; index < action.preferred_segments.size();
             ++index) {
            if (index != 0) {
                stream << ",";
            }
            stream << action.preferred_segments[index];
        }
        stream << "]";
    }
    if (!action.host_id.empty()) {
        stream << ", host=" << action.host_id;
    }
    if (!action.source_segment.empty()) {
        stream << ", source=" << action.source_segment;
    }
    if (action.group_ids.has_value()) {
        stream << ", groups=[";
        for (size_t index = 0; index < action.group_ids->size(); ++index) {
            if (index != 0) {
                stream << ",";
            }
            stream << (*action.group_ids)[index];
        }
        stream << "]";
    }
    if (!action.target_segments.empty()) {
        stream << ", targets=[";
        for (size_t index = 0; index < action.target_segments.size(); ++index) {
            if (index != 0) {
                stream << ",";
            }
            stream << action.target_segments[index];
        }
        stream << "]";
    }
    if (!action.node.empty()) {
        stream << ", node=" << action.node;
    }
    if (action.kind == ScenarioActionKind::NOTIFY_OFFLOAD_SUCCESS) {
        stream << ", transport_endpoint=" << action.transport_endpoint;
    }
    if (action.wait.count() != 0) {
        stream << ", wait_ms=" << action.wait.count();
    }
    if (action.retry_until_nonempty) {
        stream << ", retry_until_nonempty=true";
    }
    if (action.kind == ScenarioActionKind::EXECUTE_NEXT_MOVE_TASK) {
        stream << ", completion_status="
               << static_cast<int>(action.completion_status);
    }
    if (action.expected_payload_tenant.has_value()) {
        stream << ", expected_payload_tenant="
               << *action.expected_payload_tenant;
    }
    if (action.expected_payload_key.has_value()) {
        stream << ", expected_payload_key=" << *action.expected_payload_key;
    }
    if (action.expected_target_count.has_value()) {
        stream << ", expected_targets=" << *action.expected_target_count;
    }
    if (action.expected_replica_count.has_value()) {
        stream << ", expected_replicas=" << *action.expected_replica_count;
    }
    if (action.expected_source_segment.has_value()) {
        stream << ", expected_source=" << *action.expected_source_segment;
    }
    if (action.expected_same_allocation.has_value()) {
        stream << ", same_allocation=" << *action.expected_same_allocation;
    }
    if (action.expected_different_allocation.has_value()) {
        stream << ", different_allocation="
               << *action.expected_different_allocation;
    }
    if (action.expected_affected_count.has_value()) {
        stream << ", affected=" << *action.expected_affected_count;
    }
    if (!action.task_alias.empty()) {
        stream << ", task=" << action.task_alias;
    }
    if (action.expected_task_aliases.has_value()) {
        stream << ", expected_tasks=[";
        for (size_t index = 0; index < action.expected_task_aliases->size();
             ++index) {
            if (index != 0) {
                stream << ",";
            }
            stream << (*action.expected_task_aliases)[index];
        }
        stream << "]";
    }
    if (action.expected_task_type.has_value()) {
        stream << ", task_type="
               << static_cast<int>(*action.expected_task_type);
    }
    if (action.expected_task_status.has_value()) {
        stream << ", task_status="
               << static_cast<int>(*action.expected_task_status);
    }
    if (action.expected_assigned_actor.has_value()) {
        stream << ", assigned=" << *action.expected_assigned_actor;
    }
    stream << ") by " << action.actor;
    if (action.expected_error.has_value()) {
        stream << " expects " << toString(*action.expected_error);
    }
    return stream.str();
}

std::string MasterScenario::OperationName(ScenarioActionKind kind) {
    switch (kind) {
        case ScenarioActionKind::PUT_START:
            return "PutStart";
        case ScenarioActionKind::PUT_END:
            return "PutEnd";
        case ScenarioActionKind::PUT_REVOKE:
            return "PutRevoke";
        case ScenarioActionKind::PUT:
            return "Put";
        case ScenarioActionKind::READ:
            return "Read";
        case ScenarioActionKind::UPSERT_START:
            return "UpsertStart";
        case ScenarioActionKind::UPSERT_END:
            return "UpsertEnd";
        case ScenarioActionKind::UPSERT_REVOKE:
            return "UpsertRevoke";
        case ScenarioActionKind::BATCH_UPSERT_START:
            return "BatchUpsertStart";
        case ScenarioActionKind::BATCH_UPSERT_END:
            return "BatchUpsertEnd";
        case ScenarioActionKind::BATCH_UPSERT_REVOKE:
            return "BatchUpsertRevoke";
        case ScenarioActionKind::COPY_START:
            return "CopyStart";
        case ScenarioActionKind::COPY_END:
            return "CopyEnd";
        case ScenarioActionKind::COPY_REVOKE:
            return "CopyRevoke";
        case ScenarioActionKind::MOVE_START:
            return "MoveStart";
        case ScenarioActionKind::MOVE_END:
            return "MoveEnd";
        case ScenarioActionKind::MOVE_REVOKE:
            return "MoveRevoke";
        case ScenarioActionKind::ADD_REPLICA:
            return "AddReplica";
        case ScenarioActionKind::NOTIFY_OFFLOAD_SUCCESS:
            return "NotifyOffloadSuccess";
        case ScenarioActionKind::UPSERT_TENANT_POLICY:
            return "UpsertTenantPolicy";
        case ScenarioActionKind::DELETE_TENANT_POLICY:
            return "DeleteTenantPolicy";
        case ScenarioActionKind::REMOVE:
            return "Remove";
        case ScenarioActionKind::BATCH_REMOVE:
            return "BatchRemove";
        case ScenarioActionKind::BATCH_REPLICA_CLEAR:
            return "BatchReplicaClear";
        case ScenarioActionKind::CREATE_COPY_TASK:
            return "CreateCopyTask";
        case ScenarioActionKind::CREATE_MOVE_TASK:
            return "CreateMoveTask";
        case ScenarioActionKind::FETCH_TASKS:
            return "FetchTasks";
        case ScenarioActionKind::COMPLETE_TASK:
            return "CompleteTask";
        case ScenarioActionKind::QUERY_TASK:
            return "QueryTask";
        case ScenarioActionKind::CREATE_DRAIN_JOB:
            return "CreateDrainJob";
        case ScenarioActionKind::CANCEL_DRAIN_JOB:
            return "CancelDrainJob";
        case ScenarioActionKind::EXECUTE_NEXT_MOVE_TASK:
            return "ExecuteNextMoveTask";
        case ScenarioActionKind::REMOVE_BY_REGEX:
            return "RemoveByRegex";
        case ScenarioActionKind::REMOVE_ALL:
            return "RemoveAll";
        case ScenarioActionKind::REMOVE_ALL_TENANT:
            return "RemoveAllForTenant";
        case ScenarioActionKind::UNMOUNT_NODE:
            return "UnmountNode";
        case ScenarioActionKind::GRACEFUL_UNMOUNT_NODE:
            return "GracefulUnmountNode";
        case ScenarioActionKind::WAIT:
            return "Wait";
    }
    return "Unknown";
}

std::string MasterScenario::CheckpointName(MasterTestCheckpoint checkpoint) {
    return CheckpointToString(checkpoint);
}

}  // namespace mooncake::test
