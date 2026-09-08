#include "master_service/dsl/scenario.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <thread>
#include <utility>

#include <unistd.h>

#include "mutex.h"
#include "tenant_quota_policy_store.h"
#include "types.h"

namespace mooncake::test {
namespace {

constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;

uint64_t StableHash(std::string_view kind, std::string_view name,
                    uint64_t seed) {
    uint64_t hash = seed;
    for (const char value : kind) {
        hash = (hash ^ static_cast<unsigned char>(value)) * kFnvPrime;
    }
    for (const char value : name) {
        hash = (hash ^ static_cast<unsigned char>(value)) * kFnvPrime;
    }
    return hash;
}

UUID StableUuid(std::string_view kind, std::string_view name) {
    return {StableHash(kind, name, kFnvOffset),
            StableHash(kind, name, kFnvOffset ^ 0x9e3779b97f4a7c15ULL)};
}

}  // namespace

MemoryNodeSpec MemoryNode(std::string name) {
    MemoryNodeSpec node;
    node.name = std::move(name);
    return node;
}

NoFNodeSpec NoFNode(std::string name) {
    NoFNodeSpec node;
    node.name = std::move(name);
    node.endpoint = node.name + "-endpoint";
    return node;
}

TenantSpec Tenant(std::string name) { return {.name = std::move(name)}; }

PutStartAction<> PutStart(std::string key, uint64_t size) {
    return PutStartAction<>(std::move(key), size);
}

UpsertStartAction<> UpsertStart(std::string key, uint64_t size) {
    return UpsertStartAction<>(std::move(key), size);
}

PutEndAction PutEnd(std::string key) { return {.key = std::move(key)}; }

UpsertEndAction UpsertEnd(std::string key) { return {.key = std::move(key)}; }

PutRevokeAction PutRevoke(std::string key) { return {.key = std::move(key)}; }

UpsertRevokeAction UpsertRevoke(std::string key) {
    return {.key = std::move(key)};
}

BatchUpsertStartAction BatchUpsertStart(
    std::initializer_list<std::pair<std::string, uint64_t>> objects) {
    BatchUpsertStartAction action;
    action.keys.reserve(objects.size());
    action.sizes.reserve(objects.size());
    for (const auto& [key, size] : objects) {
        action.keys.push_back(key);
        action.sizes.push_back(size);
    }
    return action;
}

BatchUpsertEndAction BatchUpsertEnd(std::initializer_list<std::string> keys) {
    BatchUpsertEndAction action;
    action.keys.assign(keys.begin(), keys.end());
    return action;
}

BatchRemoveAction BatchRemove(std::initializer_list<std::string> keys) {
    BatchRemoveAction action;
    action.keys.assign(keys.begin(), keys.end());
    return action;
}

RemoveAction Remove(std::string key) { return {.key = std::move(key)}; }

ClearReplicasAction ClearReplicas(std::initializer_list<std::string> keys) {
    ClearReplicasAction action;
    action.keys.assign(keys.begin(), keys.end());
    return action;
}

CopyStartAction CopyStart(std::string key) { return {.key = std::move(key)}; }

CopyEndAction CopyEnd(std::string key) { return {.key = std::move(key)}; }

CopyRevokeAction CopyRevoke(std::string key) { return {.key = std::move(key)}; }

MoveStartAction MoveStart(std::string key) { return {.key = std::move(key)}; }

MoveEndAction MoveEnd(std::string key) { return {.key = std::move(key)}; }

MoveRevokeAction MoveRevoke(std::string key) { return {.key = std::move(key)}; }

CreateCopyTaskAction CreateCopyTask(std::string name, std::string key) {
    return {.name = std::move(name), .key = std::move(key)};
}

CreateMoveTaskAction CreateMoveTask(std::string name, std::string key) {
    return {.name = std::move(name), .key = std::move(key)};
}

FetchTasksAction FetchTasks(std::string actor) {
    return {.actor = std::move(actor)};
}

CompleteTaskAction CompleteTask(std::string name) {
    return {.name = std::move(name)};
}

CompleteUnknownTaskAction CompleteUnknownTask(std::string name) {
    return {.name = std::move(name)};
}

RemoveByRegexAction RemoveByRegex(std::string pattern) {
    return {.pattern = std::move(pattern)};
}

UnmountMemoryNodeAction UnmountMemoryNode(std::string node) {
    UnmountMemoryNodeAction action;
    action.node = std::move(node);
    action.actor = action.node;
    return action;
}

GracefulUnmountMemoryNodeAction GracefullyUnmountMemoryNode(std::string node) {
    GracefulUnmountMemoryNodeAction action;
    action.node = std::move(node);
    action.actor = action.node;
    return action;
}

MountMemorySegmentAction MountMemorySegment(std::string alias) {
    MountMemorySegmentAction action;
    action.alias = std::move(alias);
    action.name = action.alias;
    action.endpoint = action.name;
    action.actor = action.alias;
    return action;
}

MountUnmountMemoryCapacitiesAction MountUnmountMemoryCapacities(
    std::string name, std::initializer_list<uint64_t> capacities) {
    return {.name = std::move(name),
            .capacities = {capacities.begin(), capacities.end()}};
}

RemoveAllAction RemoveAll() { return {}; }

ExpireAtAction ExpireAt(std::string key,
                        std::chrono::system_clock::time_point lease_timeout) {
    return {.key = std::move(key), .lease_timeout = lease_timeout};
}

MemoryEvictAction EvictMemory(double target_ratio) {
    return {.target_ratio = target_ratio, .lower_bound_ratio = target_ratio};
}

DfsEvictAction EvictDfs() { return {}; }

ObjectSpec<> Object(std::string key) { return ObjectSpec<>(std::move(key)); }

ObjectsSpec<> Objects(size_t begin, size_t end) {
    return ObjectsSpec<>(begin, end);
}

ObjectsSpec<> Objects(std::initializer_list<std::string> keys) {
    return ObjectsSpec<>(std::vector<std::string>(keys));
}

ReadableObjectCountSpec ReadableCount(ObjectsSpec<> objects, size_t expected) {
    return {.objects = std::move(objects), .expected = expected};
}

KeyExistsSpec KeyExists(std::string key) { return {.key = std::move(key)}; }

BatchExistenceSpec BatchExistence(std::initializer_list<std::string> keys) {
    BatchExistenceSpec existence;
    existence.keys.assign(keys.begin(), keys.end());
    return existence;
}

BatchReplicaListsSpec BatchReplicaLists(
    std::initializer_list<std::string> keys) {
    BatchReplicaListsSpec replica_lists;
    replica_lists.keys.assign(keys.begin(), keys.end());
    return replica_lists;
}

NamedTaskSpec NamedTask(std::string name) { return {.name = std::move(name)}; }

UnknownTaskSpec UnknownTask(std::string name) {
    return {.name = std::move(name)};
}

MatchingKeysSpec MatchingKeys(std::string pattern) {
    MatchingKeysSpec keys;
    keys.pattern = std::move(pattern);
    return keys;
}

ClientIpsSpec ClientIps(std::initializer_list<std::string> actors) {
    ClientIpsSpec ips;
    ips.actors.assign(actors.begin(), actors.end());
    return ips;
}

MemoryNodeStatusSpec MemoryNodeStatus(std::string node) {
    return {.node = std::move(node)};
}

std::string GroupOnDifferentShard(std::string_view key) {
    constexpr size_t kMetadataShardCount = 1024;
    const size_t key_shard =
        std::hash<std::string>{}(std::string(key)) % kMetadataShardCount;
    for (size_t suffix = 0; suffix < 10000; ++suffix) {
        std::string group =
            std::string(key) + "_group_" + std::to_string(suffix);
        if (std::hash<std::string>{}(group) % kMetadataShardCount !=
            key_shard) {
            return group;
        }
    }
    return std::string(key) + "_fallback_group";
}

WaitAction WaitFor(std::chrono::milliseconds duration) { return {duration}; }

WaitForOpLogFailureAction WaitForOpLogFailure() { return {}; }

PingAction Ping(std::string actor) { return {.actor = std::move(actor)}; }

MountLocalDiskAction MountLocalDisk(std::string actor) {
    return {.actor = std::move(actor)};
}

UnmountLocalDiskAction UnmountLocalDisk(std::string actor) {
    return {.actor = std::move(actor)};
}

ReportSsdCapacityAction ReportSsdCapacity(std::string actor,
                                          int64_t capacity_bytes) {
    return {.actor = std::move(actor), .capacity_bytes = capacity_bytes};
}

OffloadHeartbeatAction OffloadHeartbeat(std::string actor) {
    return {.actor = std::move(actor)};
}

CompleteOffloadAction CompleteOffload(std::initializer_list<std::string> keys) {
    CompleteOffloadAction action;
    action.keys.assign(keys.begin(), keys.end());
    return action;
}

EvictDiskReplicaAction EvictDiskReplica(std::string key) {
    return {.key = std::move(key)};
}

MasterScenario::MasterScenario(std::string name) : name_(std::move(name)) {}

MasterScenario::MasterScenario(std::string name, MasterServiceConfig config,
                               std::shared_ptr<HaKvBackend> batch_oplog_backend)
    : name_(std::move(name)),
      config_(std::move(config)),
      batch_oplog_backend_(std::move(batch_oplog_backend)) {}

MasterScenario::~MasterScenario() {
    service_.reset();
    if (!tenant_policy_path_.empty()) {
        std::error_code error;
        std::filesystem::remove(tenant_policy_path_, error);
    }
}

MasterScenario& MasterScenario::Given(MemoryNodeSpec node) {
    if (declarations_frozen_) {
        Fail("MemoryNode declarations must precede actions and assertions");
        return *this;
    }
    if (node.name.empty()) {
        Fail("MemoryNode requires a name");
        return *this;
    }
    if (node.capacity == 0) {
        Fail("MemoryNode " + node.name + " requires non-zero capacity");
        return *this;
    }
    const auto duplicate = std::find_if(
        nodes_.begin(), nodes_.end(),
        [&](const auto& existing) { return existing.name == node.name; });
    if (duplicate != nodes_.end()) {
        Fail("duplicate MemoryNode " + node.name);
        return *this;
    }
    nodes_.push_back(std::move(node));
    return *this;
}

MasterScenario& MasterScenario::Given(NoFNodeSpec node) {
    if (declarations_frozen_) {
        Fail("NoFNode declarations must precede actions and assertions");
        return *this;
    }
    if (node.name.empty() || node.endpoint.empty() || node.capacity == 0) {
        Fail("NoFNode requires name, endpoint, and non-zero capacity");
        return *this;
    }
    const auto duplicate = std::find_if(
        nof_nodes_.begin(), nof_nodes_.end(),
        [&](const auto& existing) { return existing.name == node.name; });
    if (duplicate != nof_nodes_.end()) {
        Fail("duplicate NoFNode " + node.name);
        return *this;
    }
    nof_nodes_.push_back(std::move(node));
    return *this;
}

MasterScenario& MasterScenario::Given(TenantSpec tenant) {
    if (declarations_frozen_) {
        Fail("Tenant declarations must precede actions and assertions");
        return *this;
    }
    if (tenant.name.empty() || !TenantId(tenant.name).IsValid()) {
        Fail("Tenant requires a valid name");
        return *this;
    }
    if (tenant.quota_bytes == 0) {
        Fail("Tenant " + tenant.name + " requires non-zero quota");
        return *this;
    }
    const auto duplicate = std::find_if(
        tenants_.begin(), tenants_.end(),
        [&](const auto& existing) { return existing.name == tenant.name; });
    if (duplicate != tenants_.end()) {
        Fail("duplicate Tenant " + tenant.name);
        return *this;
    }
    tenants_.push_back(std::move(tenant));
    return *this;
}

MasterScenario& MasterScenario::Given(ObjectsSpec<> objects) {
    if (objects.keys.empty()) {
        Fail(
            "Objects requires at least one key; indexed ranges must use "
            "NamedBy");
        return *this;
    }
    if (objects.size == 0) {
        Fail("Objects requires non-zero Size");
        return *this;
    }
    if (objects.preferred_node.empty()) {
        Fail("Objects requires CompleteOn");
        return *this;
    }

    for (size_t offset = 0; offset < objects.keys.size(); ++offset) {
        const auto& key = objects.keys[offset];
        auto put = PutStart(key, objects.size)
                       .By(objects.actor)
                       .ForTenant(objects.tenant)
                       .OnNode(objects.preferred_node);
        if (!objects.group_id.empty()) {
            put.InGroup(objects.group_id);
        }
        if (objects.with_soft_pin) {
            put.WithSoftPin();
        }
        if (objects.with_hard_pin) {
            put.WithHardPin();
        }
        When(std::move(put));
        When(PutEnd(key).By(objects.actor).ForTenant(objects.tenant));

        if (objects.lease_timeout_base.has_value()) {
            auto expire = ExpireAt(key, *objects.lease_timeout_base +
                                            objects.lease_timeout_step * offset)
                              .ForTenant(objects.tenant);
            if (objects.soft_pin_timeout.has_value()) {
                expire.SoftPinnedUntil(*objects.soft_pin_timeout);
            }
            When(std::move(expire));
        }
    }
    return *this;
}

MasterScenario& MasterScenario::WhenPutStart(PutStartActionData action) {
    if (!EnsureService()) {
        return *this;
    }

    ReplicateConfig config;
    config.replica_num = action.requested_replica_count;
    config.nof_replica_num = action.requested_nof_replica_count;
    config.dfs_replica_num = action.requested_dfs_replica_count;
    config.preferred_segment = action.preferred_node;
    config.preferred_segments = action.preferred_nodes;
    config.host_id = action.host_id;
    config.prefer_alloc_in_same_node = action.prefer_same_node;
    config.soft_pin_action = action.soft_pin_action;
    config.soft_pin_ttl_ms = action.soft_pin_ttl_ms;
    config.with_hard_pin = action.with_hard_pin;
    if (action.group_ids.has_value()) {
        config.group_ids = std::move(action.group_ids);
    }
    const auto actor_id = ActorId(action.actor);
    const TenantId tenant_id(action.tenant);
    const auto deadline =
        std::chrono::steady_clock::now() + action.eventual_timeout;
    auto result = service_->PutStart(actor_id, action.key, tenant_id,
                                     action.size, config);
    while (!result && action.eventual_timeout.count() > 0 &&
           (result.error() == ErrorCode::TENANT_QUOTA_EXCEEDED ||
            result.error() == ErrorCode::NO_AVAILABLE_HANDLE) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        result = service_->PutStart(actor_id, action.key, tenant_id,
                                    action.size, config);
    }
    ValidateStartResult("PutStart(" + action.key + ")", action.expected_error,
                        action.expected_replica_count,
                        action.expected_replica_status, result,
                        action.expected_memory_nodes);
    if (result) {
        last_start_results_[action.tenant + "\n" + action.key] = *result;
    }
    return *this;
}

MasterScenario& MasterScenario::WhenUpsertStart(UpsertStartActionData action) {
    if (!EnsureService()) {
        return *this;
    }

    ReplicateConfig config;
    config.replica_num = action.requested_replica_count;
    config.preferred_segment = action.preferred_node;
    if (action.group_id.has_value()) {
        config.group_ids = {*action.group_id};
    }
    const auto result =
        service_->UpsertStart(ActorId(action.actor), action.key,
                              TenantId(action.tenant), action.size, config);
    ValidateStartResult("UpsertStart(" + action.key + ")",
                        action.expected_error, action.expected_replica_count,
                        action.expected_replica_status, result);
    if (result && action.expected_buffer_reuse.has_value()) {
        const auto previous =
            last_start_results_.find(action.tenant + "\n" + action.key);
        if (previous == last_start_results_.end()) {
            Fail("UpsertStart(" + action.key +
                 ") cannot check buffer reuse without a prior start result");
        } else {
            const auto memory_address =
                [](const auto& replicas) -> std::optional<uintptr_t> {
                const auto replica = std::find_if(
                    replicas.begin(), replicas.end(),
                    [](const auto& item) { return item.is_memory_replica(); });
                if (replica == replicas.end()) {
                    return std::nullopt;
                }
                return replica->get_memory_descriptor()
                    .buffer_descriptor.buffer_address_;
            };
            const auto old_address = memory_address(previous->second);
            const auto new_address = memory_address(*result);
            if (!old_address.has_value() || !new_address.has_value()) {
                Fail("UpsertStart(" + action.key +
                     ") buffer reuse requires memory replicas");
            } else if ((*old_address == *new_address) !=
                       *action.expected_buffer_reuse) {
                Fail("UpsertStart(" + action.key + ") " +
                     (*action.expected_buffer_reuse
                          ? "did not reuse the previous buffer"
                          : "reused the previous buffer"));
            }
        }
    }
    if (result) {
        last_start_results_[action.tenant + "\n" + action.key] = *result;
    }
    return *this;
}

MasterScenario& MasterScenario::When(PutEndAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->PutEnd(
        ActorId(action.actor), ObjectMeta{action.key, action.checksum},
        TenantId(action.tenant), action.replica_type);
    ValidateActionResult("PutEnd(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(UpsertEndAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->UpsertEnd(
        ActorId(action.actor), ObjectMeta{action.key, action.checksum},
        TenantId(action.tenant), action.replica_type);
    ValidateActionResult("UpsertEnd(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(PutRevokeAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->PutRevoke(ActorId(action.actor), action.key,
                            TenantId(action.tenant), action.replica_type);
    ValidateActionResult("PutRevoke(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(UpsertRevokeAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->UpsertRevoke(ActorId(action.actor), action.key,
                               TenantId(action.tenant), action.replica_type);
    ValidateActionResult("UpsertRevoke(" + action.key + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(BatchUpsertStartAction action) {
    if (!EnsureService()) {
        return *this;
    }

    ReplicateConfig config;
    config.replica_num = action.requested_replica_count;
    config.preferred_segment = action.preferred_node;
    config.group_ids = action.group_ids;
    const auto results = service_->BatchUpsertStart(
        ActorId(action.actor), action.keys, TenantId(action.tenant),
        action.sizes, config);
    if (results.size() != action.keys.size()) {
        Fail("BatchUpsertStart returned " + std::to_string(results.size()) +
             " results; expected " + std::to_string(action.keys.size()));
        return *this;
    }
    for (size_t index = 0; index < results.size(); ++index) {
        ValidateStartResult("BatchUpsertStart(" + action.keys[index] + ")",
                            action.expected_error,
                            action.expected_replica_count,
                            action.expected_replica_status, results[index]);
    }
    return *this;
}

MasterScenario& MasterScenario::When(BatchRemoveAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto results = service_->BatchRemove(
        action.keys, TenantId(action.tenant), action.force);
    if (results.size() != action.keys.size()) {
        Fail("BatchRemove returned " + std::to_string(results.size()) +
             " results; expected " + std::to_string(action.keys.size()));
        return *this;
    }
    for (size_t index = 0; index < results.size(); ++index) {
        ValidateActionResult(
            "BatchRemove(" + action.keys[index] + ")", action.expected_error,
            results[index].has_value(),
            results[index] ? ErrorCode::OK : results[index].error());
    }
    return *this;
}

MasterScenario& MasterScenario::When(BatchUpsertEndAction action) {
    if (!EnsureService()) {
        return *this;
    }

    std::vector<ObjectMeta> objects;
    objects.reserve(action.keys.size());
    for (const auto& key : action.keys) {
        objects.push_back({.key = key, .object_checksum = std::nullopt});
    }
    const auto results = service_->BatchUpsertEnd(
        ActorId(action.actor), objects, TenantId(action.tenant));
    if (results.size() != action.keys.size()) {
        Fail("BatchUpsertEnd returned " + std::to_string(results.size()) +
             " results; expected " + std::to_string(action.keys.size()));
        return *this;
    }
    for (size_t index = 0; index < results.size(); ++index) {
        ValidateActionResult(
            "BatchUpsertEnd(" + action.keys[index] + ")", std::nullopt,
            results[index].has_value(),
            results[index] ? ErrorCode::OK : results[index].error());
    }
    return *this;
}

MasterScenario& MasterScenario::When(RemoveAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->Remove(action.key, TenantId(action.tenant), action.force);
    ValidateActionResult("Remove(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(ClearReplicasAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->BatchReplicaClear(
        action.keys, ActorId(action.actor), action.node, action.tenant);
    ValidateActionResult("ClearReplicas", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (!result || action.expected_error.has_value() ||
        !action.expected_cleared.has_value()) {
        return *this;
    }
    if (*result != *action.expected_cleared) {
        Fail("ClearReplicas cleared an unexpected set of keys");
    }
    return *this;
}

MasterScenario& MasterScenario::When(CopyStartAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->CopyStart(ActorId(action.actor), action.key,
                                            TenantId(action.tenant),
                                            action.source, action.targets);
    ValidateActionResult("CopyStart(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (!result || action.expected_error.has_value()) {
        return *this;
    }

    const auto& source_endpoint = result->source.get_memory_descriptor()
                                      .buffer_descriptor.transport_endpoint_;
    if (source_endpoint != action.source) {
        Fail("CopyStart(" + action.key + ") returned source " +
             source_endpoint + "; expected " + action.source);
    }
    if (action.expected_allocated_targets.has_value()) {
        std::vector<std::string> actual;
        actual.reserve(result->targets.size());
        for (const auto& target : result->targets) {
            actual.push_back(target.get_memory_descriptor()
                                 .buffer_descriptor.transport_endpoint_);
        }
        auto expected = *action.expected_allocated_targets;
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        if (actual != expected) {
            Fail("CopyStart(" + action.key +
                 ") allocated an unexpected set of targets");
        }
    }
    return *this;
}

MasterScenario& MasterScenario::When(CopyEndAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->CopyEnd(ActorId(action.actor), action.key,
                                          TenantId(action.tenant));
    ValidateActionResult("CopyEnd(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(CopyRevokeAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->CopyRevoke(ActorId(action.actor), action.key,
                                             TenantId(action.tenant));
    ValidateActionResult("CopyRevoke(" + action.key + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(MoveStartAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->MoveStart(ActorId(action.actor), action.key,
                                            TenantId(action.tenant),
                                            action.source, action.target);
    ValidateActionResult("MoveStart(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (!result || action.expected_error.has_value()) {
        return *this;
    }

    const auto& source_endpoint = result->source.get_memory_descriptor()
                                      .buffer_descriptor.transport_endpoint_;
    if (source_endpoint != action.source) {
        Fail("MoveStart(" + action.key + ") returned source " +
             source_endpoint + "; expected " + action.source);
    }
    if (result->target.has_value()) {
        const auto& target_endpoint =
            result->target->get_memory_descriptor()
                .buffer_descriptor.transport_endpoint_;
        if (target_endpoint != action.target) {
            Fail("MoveStart(" + action.key + ") returned target " +
                 target_endpoint + "; expected " + action.target);
        }
    }
    if (action.expected_target_allocation.has_value() &&
        result->target.has_value() != *action.expected_target_allocation) {
        Fail("MoveStart(" + action.key +
             ") target allocation did not match expectation");
    }
    return *this;
}

MasterScenario& MasterScenario::When(MoveEndAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->MoveEnd(ActorId(action.actor), action.key,
                                          TenantId(action.tenant));
    ValidateActionResult("MoveEnd(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(MoveRevokeAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->MoveRevoke(ActorId(action.actor), action.key,
                                             TenantId(action.tenant));
    ValidateActionResult("MoveRevoke(" + action.key + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(CreateCopyTaskAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->CreateCopyTask(
        action.key, TenantId(action.tenant), action.targets);
    ValidateActionResult("CreateCopyTask(" + action.name + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (result && !action.expected_error.has_value()) {
        const bool inserted = task_ids_.emplace(action.name, *result).second;
        if (!inserted) {
            Fail("duplicate task name " + action.name);
        }
    }
    return *this;
}

MasterScenario& MasterScenario::When(CreateMoveTaskAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->CreateMoveTask(
        action.key, TenantId(action.tenant), action.source, action.target);
    ValidateActionResult("CreateMoveTask(" + action.name + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (result && !action.expected_error.has_value()) {
        const bool inserted = task_ids_.emplace(action.name, *result).second;
        if (!inserted) {
            Fail("duplicate task name " + action.name);
        }
    }
    return *this;
}

MasterScenario& MasterScenario::When(FetchTasksAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->FetchTasks(ActorId(action.actor), action.batch_size);
    ValidateActionResult("FetchTasks(" + action.actor + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (result && !action.expected_error.has_value() &&
        action.expected_count.has_value() &&
        result->size() != *action.expected_count) {
        Fail("FetchTasks(" + action.actor + ") returned " +
             std::to_string(result->size()) + "; expected " +
             std::to_string(*action.expected_count));
    }
    return *this;
}

MasterScenario& MasterScenario::When(CompleteTaskAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto task = task_ids_.find(action.name);
    if (task == task_ids_.end()) {
        Fail("CompleteTask references unknown task " + action.name);
        return *this;
    }

    TaskCompleteRequest request;
    request.id = task->second;
    request.status = action.status;
    request.message = std::move(action.message);
    const auto result =
        service_->MarkTaskToComplete(ActorId(action.actor), request);
    ValidateActionResult("CompleteTask(" + action.name + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(CompleteUnknownTaskAction action) {
    if (!EnsureService()) {
        return *this;
    }

    TaskCompleteRequest request;
    request.id = StableUuid("unknown-task", action.name);
    request.status = action.status;
    request.message = std::move(action.message);
    const auto result =
        service_->MarkTaskToComplete(ActorId(action.actor), request);
    ValidateActionResult("CompleteUnknownTask(" + action.name + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(RemoveByRegexAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->RemoveByRegex(
        action.pattern, TenantId(action.tenant), action.force);
    ValidateActionResult("RemoveByRegex(" + action.pattern + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (result && action.expected_removed.has_value() &&
        *result != static_cast<long>(*action.expected_removed)) {
        Fail("RemoveByRegex(" + action.pattern + ") removed " +
             std::to_string(*result) + "; expected " +
             std::to_string(*action.expected_removed));
    }
    return *this;
}

MasterScenario& MasterScenario::When(UnmountMemoryNodeAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto segment = segment_ids_.find(action.node);
    if (segment == segment_ids_.end()) {
        if (!action.unknown) {
            Fail("UnmountMemoryNode references undeclared node " + action.node);
            return *this;
        }
    }
    const UUID segment_id = segment == segment_ids_.end()
                                ? StableUuid("unknown-segment", action.node)
                                : segment->second;
    const auto actor = action.actor.empty() ? action.node : action.actor;
    const auto started = std::chrono::steady_clock::now();
    const auto result = service_->UnmountSegment(segment_id, ActorId(actor));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started);
    ValidateActionResult("UnmountMemoryNode(" + action.node + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (action.maximum_duration.has_value() &&
        elapsed > *action.maximum_duration) {
        Fail("UnmountMemoryNode(" + action.node + ") took " +
             std::to_string(elapsed.count()) + "ms; expected at most " +
             std::to_string(action.maximum_duration->count()) + "ms");
    }
    return *this;
}

MasterScenario& MasterScenario::When(GracefulUnmountMemoryNodeAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto segment = segment_ids_.find(action.node);
    if (segment == segment_ids_.end()) {
        Fail("GracefullyUnmountMemoryNode references undeclared node " +
             action.node);
        return *this;
    }
    const auto actor = action.actor.empty() ? action.node : action.actor;
    const auto result = service_->GracefulUnmountSegment(
        segment->second, ActorId(actor), action.grace_period_ms);
    ValidateActionResult("GracefullyUnmountMemoryNode(" + action.node + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(MountMemorySegmentAction action) {
    if (!EnsureService()) {
        return *this;
    }
    Segment segment;
    const auto existing = segments_.find(action.alias);
    segment.id = existing == segments_.end()
                     ? StableUuid("dynamic-segment", action.alias)
                     : existing->second.id;
    segment.name = action.name;
    segment.base = action.base;
    segment.size = action.capacity;
    segment.te_endpoint = action.endpoint;
    const auto result = service_->MountSegment(segment, ActorId(action.actor));
    ValidateActionResult("MountMemorySegment(" + action.alias + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (result) {
        segment_ids_[action.alias] = segment.id;
        segments_[action.alias] = std::move(segment);
    }
    return *this;
}

MasterScenario& MasterScenario::When(
    MountUnmountMemoryCapacitiesAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (action.capacities.empty()) {
        Fail("MountUnmountMemoryCapacities requires capacities");
        return *this;
    }
    Segment segment;
    segment.id = StableUuid("variable-segment", action.name);
    segment.name = action.name;
    segment.base = 0x500000000;
    segment.te_endpoint = action.name;
    const UUID actor = ActorId(action.name);
    for (const uint64_t capacity : action.capacities) {
        segment.size = capacity;
        const auto mount = service_->MountSegment(segment, actor);
        if (!mount) {
            Fail("MountUnmountMemoryCapacities mount failed: " +
                 toString(mount.error()));
            continue;
        }
        const auto unmount = service_->UnmountSegment(segment.id, actor);
        if (!unmount) {
            Fail("MountUnmountMemoryCapacities unmount failed: " +
                 toString(unmount.error()));
        }
    }
    return *this;
}

MasterScenario& MasterScenario::When(RemoveAllAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const long removed =
        action.tenant.has_value()
            ? service_->RemoveAll(TenantId(*action.tenant), action.force)
            : service_->RemoveAll(action.force);
    if (action.expected_removed.has_value() &&
        removed != static_cast<long>(*action.expected_removed)) {
        Fail("RemoveAll removed " + std::to_string(removed) +
             " objects; expected " + std::to_string(*action.expected_removed));
    }
    return *this;
}

MasterScenario& MasterScenario::When(ExpireAtAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const TenantId tenant(action.tenant);
    auto update = [&](size_t shard_idx) {
        MasterService::MetadataShardAccessorRW shard(service_.get(), shard_idx);
        auto tenant_it = shard->tenants.find(tenant);
        if (tenant_it == shard->tenants.end()) {
            return false;
        }
        auto metadata_it = tenant_it->second.metadata.find(action.key);
        if (metadata_it == tenant_it->second.metadata.end()) {
            return false;
        }
        SpinLocker locker(&metadata_it->second.lock);
        metadata_it->second.lease_->SetDeadline(action.lease_timeout);
        metadata_it->second.soft_pin_timeout = action.soft_pin_timeout;
        return true;
    };

    const size_t routed = service_->getShardIndex(tenant, action.key);
    if (update(routed)) {
        return *this;
    }
    for (size_t shard_idx = 0; shard_idx < MasterService::kNumShards;
         ++shard_idx) {
        if (shard_idx != routed && update(shard_idx)) {
            return *this;
        }
    }
    Fail("ExpireAt(" + action.key + ") could not find object");
    return *this;
}

MasterScenario& MasterScenario::When(MemoryEvictAction action) {
    if (!EnsureService()) {
        return *this;
    }
    service_->RunBatchEvictForTesting(action.target_ratio,
                                      action.lower_bound_ratio);
    return *this;
}

MasterScenario& MasterScenario::When(DfsEvictAction) {
    if (!EnsureService()) {
        return *this;
    }
    service_->RunDfsEvictionForTesting();
    return *this;
}

MasterScenario& MasterScenario::When(WaitAction action) {
    if (action.duration < std::chrono::milliseconds::zero()) {
        Fail("WaitFor requires a non-negative duration");
        return *this;
    }
    std::this_thread::sleep_for(action.duration);
    return *this;
}

MasterScenario& MasterScenario::When(WaitForOpLogFailureAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (!service_->ordered_oplog_writer_) {
        Fail("OpLog writer is not configured");
        return *this;
    }
    const auto deadline = std::chrono::steady_clock::now() + action.timeout;
    while (service_->ordered_oplog_writer_->IsAccepting() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (service_->ordered_oplog_writer_->IsAccepting()) {
        Fail("OpLog writer was expected to be unavailable");
    }
    return *this;
}

MasterScenario& MasterScenario::When(PingAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto result = service_->Ping(ActorId(action.actor));
    ValidateActionResult("Ping(" + action.actor + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(MountLocalDiskAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto result = service_->MountLocalDiskSegment(
        ActorId(action.actor), action.enable_offloading);
    ValidateActionResult("MountLocalDisk(" + action.actor + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(UnmountLocalDiskAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto result =
        service_->UnmountLocalDiskSegment(ActorId(action.actor));
    ValidateActionResult("UnmountLocalDisk(" + action.actor + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(ReportSsdCapacityAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto result = service_->ReportSsdCapacity(ActorId(action.actor),
                                                    action.capacity_bytes);
    ValidateActionResult("ReportSsdCapacity(" + action.actor + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(OffloadHeartbeatAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto result = service_->OffloadObjectHeartbeat(
        ActorId(action.actor), action.enable_offloading);
    ValidateActionResult("OffloadHeartbeat(" + action.actor + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (!result || !action.expected_task_keys.has_value()) {
        return *this;
    }
    if (result->size() != action.expected_task_keys->size()) {
        Fail("OffloadHeartbeat(" + action.actor + ") returned " +
             std::to_string(result->size()) + " offload tasks; expected " +
             std::to_string(action.expected_task_keys->size()));
        return *this;
    }
    for (const auto& key : *action.expected_task_keys) {
        const auto match = std::find_if(
            result->begin(), result->end(), [&](const OffloadTaskItem& task) {
                return task.tenant_id == action.tenant && task.key == key &&
                       task.size == action.expected_task_size;
            });
        if (match == result->end()) {
            Fail("OffloadHeartbeat(" + action.actor +
                 ") is missing an offload task for " + key);
        }
    }
    return *this;
}

MasterScenario& MasterScenario::When(CompleteOffloadAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (action.keys.empty()) {
        Fail("CompleteOffload requires at least one key");
        return *this;
    }
    if (action.node.empty()) {
        Fail("CompleteOffload requires OnNode");
        return *this;
    }
    const auto segment = segments_.find(action.node);
    const std::string endpoint =
        segment != segments_.end() ? segment->second.te_endpoint : action.node;
    std::vector<OffloadTaskItem> tasks;
    std::vector<StorageObjectMetadata> metadatas;
    tasks.reserve(action.keys.size());
    metadatas.reserve(action.keys.size());
    for (const auto& key : action.keys) {
        int64_t size = 0;
        if (action.size.has_value()) {
            size = *action.size;
        } else {
            const auto record =
                last_start_results_.find(action.tenant + "\n" + key);
            const Replica::Descriptor* descriptor = nullptr;
            if (record != last_start_results_.end()) {
                for (const auto& candidate : record->second) {
                    if (candidate.is_memory_replica()) {
                        descriptor = &candidate;
                        break;
                    }
                }
            }
            if (descriptor == nullptr) {
                Fail("CompleteOffload(" + key +
                     ") has no PutStart-recorded size; use OfSize");
                return *this;
            }
            size = static_cast<int64_t>(
                descriptor->get_memory_descriptor().buffer_descriptor.size_);
        }
        tasks.push_back(OffloadTaskItem{
            .tenant_id = action.tenant, .key = key, .size = size});
        StorageObjectMetadata metadata{};
        metadata.data_size = size;
        metadata.transport_endpoint = endpoint;
        metadatas.push_back(std::move(metadata));
    }
    const auto result =
        service_->NotifyOffloadSuccess(ActorId(action.actor), tasks, metadatas);
    ValidateActionResult("CompleteOffload(" + action.keys.front() + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(EvictDiskReplicaAction action) {
    if (!EnsureService()) {
        return *this;
    }
    const auto result = service_->EvictDiskReplica(
        ActorId(action.actor), action.key, TenantId(action.tenant),
        action.replica_type);
    ValidateActionResult("EvictDiskReplica(" + action.key + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::ThenObject(ObjectSpecData object,
                                           ObjectExpectation expectation) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->GetReplicaList(object.key, TenantId(object.tenant));
    if (expectation == ObjectExpectation::MISSING) {
        if (result) {
            Fail("Object(" + object.key + ") exists; expected it not to exist");
        } else if (result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            Fail("Object(" + object.key + ") lookup failed with " +
                 toString(result.error()) + "; expected OBJECT_NOT_FOUND");
        }
        return *this;
    }
    if (expectation == ObjectExpectation::NOT_READY) {
        if (result || result.error() != ErrorCode::REPLICA_IS_NOT_READY) {
            Fail("Object(" + object.key + ") was expected to be not ready");
        }
        return *this;
    }
    if (!result) {
        Fail("Object(" + object.key +
             ") is not readable: " + toString(result.error()));
        return *this;
    }
    if (result->replicas.empty()) {
        Fail("Object(" + object.key + ") has no readable replicas");
    }
    if (object.expected_replica_count.has_value() &&
        result->replicas.size() != *object.expected_replica_count) {
        Fail("Object(" + object.key + ") has " +
             std::to_string(result->replicas.size()) + " replicas; expected " +
             std::to_string(*object.expected_replica_count));
    }
    if (object.expected_complete_replica_count.has_value()) {
        const size_t complete =
            std::count_if(result->replicas.begin(), result->replicas.end(),
                          [](const auto& replica) {
                              return replica.status == ReplicaStatus::COMPLETE;
                          });
        if (complete != *object.expected_complete_replica_count) {
            Fail("Object(" + object.key + ") has " + std::to_string(complete) +
                 " complete replicas; expected " +
                 std::to_string(*object.expected_complete_replica_count));
        }
    }
    if (object.expected_checksum.has_value() &&
        result->object_checksum != *object.expected_checksum) {
        const auto format_checksum = [](const std::optional<uint64_t>& value) {
            return value.has_value() ? std::to_string(*value) : "none";
        };
        Fail("Object(" + object.key + ") has checksum " +
             format_checksum(result->object_checksum) + "; expected " +
             format_checksum(*object.expected_checksum));
    }
    std::vector<std::string> memory_nodes;
    size_t memory_replicas = 0;
    size_t nof_replicas = 0;
    size_t dfs_replicas = 0;
    size_t local_disk_replicas = 0;
    size_t disk_replicas = 0;
    size_t complete_memory_replicas = 0;
    size_t complete_nof_replicas = 0;
    size_t complete_dfs_replicas = 0;
    size_t complete_local_disk_replicas = 0;
    size_t complete_disk_replicas = 0;
    for (const auto& replica : result->replicas) {
        if (replica.is_nof_replica()) {
            ++nof_replicas;
            if (replica.status == ReplicaStatus::COMPLETE) {
                ++complete_nof_replicas;
            }
        }
        if (replica.is_dfs_replica()) {
            ++dfs_replicas;
            if (replica.status == ReplicaStatus::COMPLETE) {
                ++complete_dfs_replicas;
            }
        }
        if (replica.is_local_disk_replica()) {
            ++local_disk_replicas;
            if (replica.status == ReplicaStatus::COMPLETE) {
                ++complete_local_disk_replicas;
            }
        }
        if (replica.is_disk_replica()) {
            ++disk_replicas;
            if (replica.status == ReplicaStatus::COMPLETE) {
                ++complete_disk_replicas;
            }
        }
        if (!replica.is_memory_replica()) {
            continue;
        }
        ++memory_replicas;
        if (replica.status == ReplicaStatus::COMPLETE) {
            ++complete_memory_replicas;
        }
        const auto& descriptor =
            replica.get_memory_descriptor().buffer_descriptor;
        memory_nodes.push_back(descriptor.transport_endpoint_);
        if (object.expected_memory_replica_size.has_value() &&
            descriptor.size_ != *object.expected_memory_replica_size) {
            Fail("Object(" + object.key + ") has memory replica size " +
                 std::to_string(descriptor.size_) + "; expected " +
                 std::to_string(*object.expected_memory_replica_size));
        }
        if (object.expected_memory_node.has_value() &&
            descriptor.transport_endpoint_ != *object.expected_memory_node) {
            Fail("Object(" + object.key + ") has a memory replica on " +
                 descriptor.transport_endpoint_ + "; expected " +
                 *object.expected_memory_node);
        }
    }
    const bool expects_memory_replica =
        object.expected_memory_replica_size.has_value() ||
        object.expected_memory_node.has_value() ||
        object.expected_memory_nodes.has_value() ||
        object.expect_distinct_memory_nodes;
    if (expects_memory_replica && memory_nodes.empty()) {
        Fail("Object(" + object.key +
             ") has no memory replicas; expected at least one");
    }
    if (object.expect_distinct_memory_nodes) {
        std::sort(memory_nodes.begin(), memory_nodes.end());
        const auto duplicate =
            std::adjacent_find(memory_nodes.begin(), memory_nodes.end());
        if (duplicate != memory_nodes.end()) {
            Fail("Object(" + object.key +
                 ") has duplicate memory replicas on " + *duplicate);
        }
    }
    if (object.expected_memory_nodes.has_value()) {
        std::sort(memory_nodes.begin(), memory_nodes.end());
        auto expected = *object.expected_memory_nodes;
        std::sort(expected.begin(), expected.end());
        if (memory_nodes != expected) {
            Fail("Object(" + object.key +
                 ") is on an unexpected set of memory nodes");
        }
    }
    const auto validate_count = [&](std::string_view kind, size_t actual,
                                    const std::optional<size_t>& expected) {
        if (expected.has_value() && actual != *expected) {
            Fail("Object(" + object.key + ") has " + std::to_string(actual) +
                 " " + std::string(kind) + "; expected " +
                 std::to_string(*expected));
        }
    };
    validate_count("memory replicas", memory_replicas,
                   object.expected_memory_replica_count);
    validate_count("NoF replicas", nof_replicas,
                   object.expected_nof_replica_count);
    validate_count("DFS replicas", dfs_replicas,
                   object.expected_dfs_replica_count);
    validate_count("local-disk replicas", local_disk_replicas,
                   object.expected_local_disk_replica_count);
    validate_count("disk replicas", disk_replicas,
                   object.expected_disk_replica_count);
    validate_count("complete memory replicas", complete_memory_replicas,
                   object.expected_complete_memory_replica_count);
    validate_count("complete NoF replicas", complete_nof_replicas,
                   object.expected_complete_nof_replica_count);
    validate_count("complete DFS replicas", complete_dfs_replicas,
                   object.expected_complete_dfs_replica_count);
    validate_count("complete local-disk replicas", complete_local_disk_replicas,
                   object.expected_complete_local_disk_replica_count);
    validate_count("complete disk replicas", complete_disk_replicas,
                   object.expected_complete_disk_replica_count);
    return *this;
}

MasterScenario& MasterScenario::ThenObjects(ObjectsSpecData objects,
                                            ObjectExpectation expectation) {
    if (objects.keys.empty()) {
        Fail(
            "Objects requires at least one key; indexed ranges must use "
            "NamedBy");
        return *this;
    }
    for (auto& key : objects.keys) {
        ObjectSpecData object(std::move(key));
        object.tenant = objects.tenant;
        ThenObject(std::move(object), expectation);
    }
    return *this;
}

MasterScenario& MasterScenario::Then(ReadableObjectCountSpec objects) {
    if (!EnsureService()) {
        return *this;
    }
    size_t readable = 0;
    for (const auto& key : objects.objects.keys) {
        const auto result =
            service_->GetReplicaList(key, TenantId(objects.objects.tenant));
        if (result) {
            ++readable;
        } else if (result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            Fail("ReadableCount lookup for " + key +
                 " failed: " + toString(result.error()));
        }
    }
    if (readable != objects.expected) {
        Fail("ReadableCount is " + std::to_string(readable) + "; expected " +
             std::to_string(objects.expected));
    }
    return *this;
}

MasterScenario& MasterScenario::Then(KeyExistsSpec key_exists) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->ExistKey(key_exists.key, TenantId(key_exists.tenant));
    if (!result) {
        Fail("KeyExists(" + key_exists.key +
             ") failed: " + toString(result.error()));
    } else if (!*result) {
        Fail("KeyExists(" + key_exists.key + ") returned false");
    }
    return *this;
}

MasterScenario& MasterScenario::Then(BatchExistenceSpec batch_existence) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->BatchExistKey(
        batch_existence.keys, TenantId(batch_existence.tenant));
    if (result.size() != batch_existence.expected.size()) {
        Fail("BatchExistence returned " + std::to_string(result.size()) +
             " results; expected " +
             std::to_string(batch_existence.expected.size()));
        return *this;
    }
    for (size_t index = 0; index < result.size(); ++index) {
        if (!result[index]) {
            Fail("BatchExistence failed for key " +
                 batch_existence.keys[index] + ": " +
                 toString(result[index].error()));
        } else if (*result[index] != batch_existence.expected[index]) {
            Fail("BatchExistence returned an unexpected result for key " +
                 batch_existence.keys[index]);
        }
    }
    return *this;
}

MasterScenario& MasterScenario::Then(
    BatchReplicaListsSpec batch_replica_lists) {
    if (!EnsureService()) {
        return *this;
    }

    const auto results = service_->BatchGetReplicaList(
        batch_replica_lists.keys, TenantId(batch_replica_lists.tenant));
    if (results.size() != batch_replica_lists.expected.size()) {
        Fail("BatchReplicaLists returned " + std::to_string(results.size()) +
             " results; expected " +
             std::to_string(batch_replica_lists.expected.size()));
        return *this;
    }
    for (size_t index = 0; index < results.size(); ++index) {
        const ErrorCode expected = batch_replica_lists.expected[index];
        if (expected == ErrorCode::OK) {
            if (!results[index]) {
                Fail("BatchReplicaLists(" + batch_replica_lists.keys[index] +
                     ") failed: " + toString(results[index].error()));
            } else if (results[index]->replicas.empty()) {
                Fail("BatchReplicaLists(" + batch_replica_lists.keys[index] +
                     ") returned no replicas");
            }
        } else if (results[index]) {
            Fail("BatchReplicaLists(" + batch_replica_lists.keys[index] +
                 ") succeeded; expected " + toString(expected));
        } else if (results[index].error() != expected) {
            Fail("BatchReplicaLists(" + batch_replica_lists.keys[index] +
                 ") failed with " + toString(results[index].error()) +
                 "; expected " + toString(expected));
        }
    }
    return *this;
}

MasterScenario& MasterScenario::Then(NamedTaskSpec task) {
    if (!EnsureService()) {
        return *this;
    }
    const auto task_id = task_ids_.find(task.name);
    if (task_id == task_ids_.end()) {
        Fail("NamedTask references unknown task " + task.name);
        return *this;
    }

    const auto result = service_->QueryTask(task_id->second);
    if (!result) {
        Fail("NamedTask(" + task.name +
             ") failed: " + toString(result.error()));
        return *this;
    }
    if (task.expected_type.has_value() && result->type != *task.expected_type) {
        Fail("NamedTask(" + task.name + ") has an unexpected type");
    }
    if (task.expected_status.has_value() &&
        result->status != *task.expected_status) {
        Fail("NamedTask(" + task.name + ") has an unexpected status");
    }
    if (task.expected_actor.has_value() &&
        result->assigned_client != ActorId(*task.expected_actor)) {
        Fail("NamedTask(" + task.name + ") has an unexpected assignee");
    }
    if (task.expected_message.has_value() &&
        result->message != *task.expected_message) {
        Fail("NamedTask(" + task.name + ") has message " + result->message +
             "; expected " + *task.expected_message);
    }
    return *this;
}

MasterScenario& MasterScenario::Then(UnknownTaskSpec task) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->QueryTask(StableUuid("unknown-task", task.name));
    if (result) {
        Fail("UnknownTask(" + task.name + ") unexpectedly exists");
    } else if (result.error() != task.expected_error) {
        Fail("UnknownTask(" + task.name + ") failed with " +
             toString(result.error()) + "; expected " +
             toString(task.expected_error));
    }
    return *this;
}

MasterScenario& MasterScenario::Then(MatchingKeysSpec matching_keys) {
    if (!EnsureService()) {
        return *this;
    }
    const auto result = service_->GetReplicaListByRegex(
        matching_keys.pattern, TenantId(matching_keys.tenant));
    if (!result) {
        Fail("MatchingKeys(" + matching_keys.pattern +
             ") failed: " + toString(result.error()));
        return *this;
    }
    if (matching_keys.expected_count.has_value() &&
        result->size() != *matching_keys.expected_count) {
        Fail("MatchingKeys(" + matching_keys.pattern + ") returned " +
             std::to_string(result->size()) + "; expected " +
             std::to_string(*matching_keys.expected_count));
    }
    for (const auto& key : matching_keys.expected_keys) {
        if (!result->contains(key)) {
            Fail("MatchingKeys(" + matching_keys.pattern + ") is missing " +
                 key);
        }
    }
    return *this;
}

MasterScenario& MasterScenario::Then(ClientIpsSpec client_ips) {
    if (!EnsureService()) {
        return *this;
    }
    std::vector<UUID> clients;
    clients.reserve(client_ips.actors.size());
    for (const auto& actor : client_ips.actors) {
        clients.push_back(ActorId(actor));
    }
    const auto result = service_->BatchQueryIp(clients);
    if (!result) {
        Fail("ClientIps query failed: " + toString(result.error()));
        return *this;
    }
    if (client_ips.actors.empty() && !result->empty()) {
        Fail("ClientIps for an empty actor list returned entries");
    }
    for (const auto& [actor, expected_values] : client_ips.expected) {
        const auto found = result->find(ActorId(actor));
        if (found == result->end()) {
            Fail("ClientIps omitted expected actor " + actor);
            continue;
        }
        auto actual = found->second;
        auto expected = expected_values;
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        if (actual != expected) {
            Fail("ClientIps for " + actor + " differ from expectation");
        }
    }
    for (const auto& actor : client_ips.omitted) {
        if (result->contains(ActorId(actor))) {
            Fail("ClientIps unexpectedly returned actor " + actor);
        }
    }
    return *this;
}

MasterScenario& MasterScenario::Then(MemoryNodeStatusSpec node_status) {
    if (!EnsureService()) {
        return *this;
    }
    const auto deadline =
        std::chrono::steady_clock::now() + node_status.eventual_timeout;
    auto matches = [&]() {
        tl::expected<SegmentStatus, ErrorCode> result =
            tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        if (node_status.query_by_id) {
            const auto segment = segment_ids_.find(node_status.node);
            if (segment == segment_ids_.end()) {
                Fail("MemoryNodeStatus references undeclared node " +
                     node_status.node);
                return false;
            }
            result = service_->QuerySegmentStatusById(segment->second);
        } else {
            result = service_->QuerySegmentStatus(node_status.node);
        }
        if (node_status.expected_missing) {
            return !result.has_value() || *result == SegmentStatus::UNDEFINED;
        }
        return result.has_value() && node_status.expected_status.has_value() &&
               *result == *node_status.expected_status;
    };
    while (!matches() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!matches()) {
        Fail("MemoryNodeStatus(" + node_status.node + ") mismatch");
    }
    return *this;
}

bool MasterScenario::EnsureService() {
    if (service_) {
        return true;
    }
    declarations_frozen_ = true;
    if (nodes_.empty() && nof_nodes_.empty()) {
        Fail("scenario requires at least one storage node");
        return false;
    }

    if (!tenants_.empty()) {
        TenantQuotaPolicySnapshot snapshot;
        for (const auto& tenant : tenants_) {
            snapshot.tenant_quotas.emplace(tenant.name, tenant.quota_bytes);
        }
        tenant_policy_path_ =
            (std::filesystem::temp_directory_path() /
             ("mooncake_master_scenario_tenants_" + std::to_string(::getpid()) +
              "_" + std::to_string(reinterpret_cast<uintptr_t>(this)) +
              ".yaml"))
                .string();
        std::ofstream output(tenant_policy_path_);
        output << FormatTenantQuotaPolicyYaml(snapshot);
        output.close();
        config_.enable_multi_tenants = true;
        config_.tenant_quota_connector_type = "file";
        config_.tenant_quota_connector_uri = tenant_policy_path_;
    }

    service_ = std::make_unique<MasterService>(config_);
    if (batch_oplog_backend_) {
        const auto result =
            service_->SetBatchOpLogBackendForTesting(batch_oplog_backend_);
        if (result != ErrorCode::OK) {
            Fail("failed to install batch OpLog backend: " + toString(result));
            service_.reset();
            return false;
        }
    }
    for (const auto& node : nodes_) {
        Segment segment;
        segment.id = StableUuid("segment", node.name);
        segment.name = node.name;
        segment.base = next_segment_base_;
        segment.size = node.capacity;
        segment.te_endpoint = node.endpoint.value_or(node.name);
        segment.host_id = node.host_id;
        next_segment_base_ += node.capacity + 4096;

        const auto owner = node.owner.empty() ? node.name : node.owner;
        const auto result = service_->MountSegment(segment, ActorId(owner));
        if (!result) {
            Fail("failed to mount MemoryNode " + node.name + ": " +
                 toString(result.error()));
            service_.reset();
            return false;
        }
        segment_ids_.emplace(node.name, segment.id);
        segments_.emplace(node.name, std::move(segment));
    }
    for (const auto& node : nof_nodes_) {
        NoFSegment segment;
        segment.id = StableUuid("nof-segment", node.name);
        segment.name = node.name;
        segment.base = next_segment_base_;
        segment.size = node.capacity;
        segment.te_endpoint = node.endpoint;
        next_segment_base_ += node.capacity + 4096;

        const auto owner = node.owner.empty() ? node.name : node.owner;
        const auto result = service_->MountNoFSegment(segment, ActorId(owner));
        if (!result) {
            Fail("failed to mount NoFNode " + node.name + ": " +
                 toString(result.error()));
            service_.reset();
            return false;
        }
        nof_segments_.emplace(node.name, std::move(segment));
    }
    return true;
}

UUID MasterScenario::ActorId(std::string_view actor) {
    const std::string name(actor);
    const auto result = actor_ids_.emplace(name, StableUuid("actor", name));
    return result.first->second;
}

void MasterScenario::ValidateActionResult(
    std::string_view action, const std::optional<ErrorCode>& expected_error,
    bool succeeded, ErrorCode error) {
    if (!expected_error.has_value()) {
        if (!succeeded) {
            Fail(std::string(action) + " failed: " + toString(error));
        }
        return;
    }
    if (succeeded) {
        Fail(std::string(action) + " succeeded; expected " +
             toString(*expected_error));
    } else if (error != *expected_error) {
        Fail(std::string(action) + " failed with " + toString(error) +
             "; expected " + toString(*expected_error));
    }
}

void MasterScenario::ValidateStartResult(
    std::string_view action, const std::optional<ErrorCode>& expected_error,
    const std::optional<size_t>& expected_replica_count,
    const std::optional<ReplicaStatus>& expected_replica_status,
    const StartResult& result,
    const std::optional<std::vector<std::string>>& expected_memory_nodes) {
    ValidateActionResult(action, expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (!result) {
        return;
    }
    if (expected_replica_count.has_value() &&
        result->size() != *expected_replica_count) {
        Fail(std::string(action) + " returned " +
             std::to_string(result->size()) + " replicas; expected " +
             std::to_string(*expected_replica_count));
    }
    if (expected_replica_status.has_value() &&
        std::any_of(result->begin(), result->end(), [&](const auto& replica) {
            return replica.status != *expected_replica_status;
        })) {
        Fail(std::string(action) + " replica status mismatch");
    }
    if (expected_memory_nodes.has_value()) {
        std::vector<std::string> actual;
        for (const auto& replica : *result) {
            if (replica.is_memory_replica()) {
                actual.push_back(replica.get_memory_descriptor()
                                     .buffer_descriptor.transport_endpoint_);
            }
        }
        auto expected = *expected_memory_nodes;
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        if (actual != expected) {
            Fail(std::string(action) + " memory-node allocation mismatch");
        }
    }
}

void MasterScenario::Fail(std::string message) const {
    ADD_FAILURE() << "MasterScenario[" << name_ << "]: " << message;
}

}  // namespace mooncake::test
