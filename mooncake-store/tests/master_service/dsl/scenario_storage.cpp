#include "master_service/dsl/scenario.h"

#include <atomic>
#include <thread>
#include <unordered_set>

namespace mooncake::test {

MountLocalDiskAction MountLocalDisk(std::string actor) {
    MountLocalDiskAction action{};
    action.actor = std::move(actor);
    return action;
}

UnmountLocalDiskAction UnmountLocalDisk(std::string actor) {
    UnmountLocalDiskAction action{};
    action.actor = std::move(actor);
    return action;
}

ConcurrentMountLocalDisksAction ConcurrentMountLocalDisks() { return {}; }

OffloadHeartbeatAction OffloadHeartbeat(std::string actor) {
    OffloadHeartbeatAction action{};
    action.actor = std::move(actor);
    return action;
}

NotifyOffloadSuccessAction NotifyOffloadSuccess(std::string actor,
                                                std::string tenant,
                                                std::string key,
                                                int64_t object_size) {
    NotifyOffloadSuccessAction action{};
    action.actor = std::move(actor);
    action.tenant = std::move(tenant);
    action.key = std::move(key);
    action.object_size = object_size;
    return action;
}

ReportSsdCapacityAction ReportSsdCapacity(std::string actor,
                                          int64_t capacity_bytes) {
    ReportSsdCapacityAction action{};
    action.actor = std::move(actor);
    action.capacity_bytes = capacity_bytes;
    return action;
}

EvictDiskReplicaAction EvictDiskReplica(std::string actor, std::string key) {
    EvictDiskReplicaAction action{};
    action.actor = std::move(actor);
    action.key = std::move(key);
    return action;
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

MasterScenario& MasterScenario::When(ConcurrentMountLocalDisksAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (action.client_count == 0) {
        Fail("ConcurrentMountLocalDisks requires at least one client");
        return *this;
    }
    std::atomic<size_t> successes{0};
    std::vector<std::thread> threads;
    threads.reserve(action.client_count);
    for (size_t index = 0; index < action.client_count; ++index) {
        threads.emplace_back([&] {
            if (service_->MountLocalDiskSegment(generate_uuid(),
                                                action.enable_offloading)) {
                successes.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    if (successes.load() != action.client_count) {
        Fail("ConcurrentMountLocalDisks succeeded " +
             std::to_string(successes.load()) + " times; expected " +
             std::to_string(action.client_count));
    }
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
    if (!result || action.expected_error.has_value()) {
        return *this;
    }
    if (action.expected_task_count.has_value() &&
        result->size() != *action.expected_task_count) {
        Fail("OffloadHeartbeat(" + action.actor + ") returned " +
             std::to_string(result->size()) + " tasks; expected " +
             std::to_string(*action.expected_task_count));
    }
    std::unordered_set<std::string> actual_keys;
    for (const auto& task : *result) {
        actual_keys.insert(task.key);
        if (action.expected_object_size.has_value() &&
            task.size != *action.expected_object_size) {
            Fail("OffloadHeartbeat task " + task.key + " has size " +
                 std::to_string(task.size) + "; expected " +
                 std::to_string(*action.expected_object_size));
        }
    }
    if (!action.expected_saved_set.empty()) {
        const auto expected = saved_key_sets_.find(action.expected_saved_set);
        if (expected == saved_key_sets_.end()) {
            Fail("OffloadHeartbeat references unknown saved set " +
                 action.expected_saved_set);
        } else {
            const std::unordered_set<std::string> expected_keys(
                expected->second.begin(), expected->second.end());
            if (actual_keys != expected_keys) {
                Fail("OffloadHeartbeat task keys differ from saved set " +
                     action.expected_saved_set);
            }
        }
    }
    return *this;
}

MasterScenario& MasterScenario::When(NotifyOffloadSuccessAction action) {
    if (!EnsureService()) {
        return *this;
    }
    std::vector<OffloadTaskItem> tasks{OffloadTaskItem{
        .tenant_id = action.tenant,
        .key = action.key,
        .size = action.object_size,
    }};
    StorageObjectMetadata metadata;
    metadata.data_size = action.object_size;
    metadata.transport_endpoint = action.endpoint;
    const auto result = service_->NotifyOffloadSuccess(ActorId(action.actor),
                                                       tasks, {metadata});
    ValidateActionResult("NotifyOffloadSuccess(" + action.key + ")",
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

}  // namespace mooncake::test
