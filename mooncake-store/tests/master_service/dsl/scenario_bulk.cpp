#include "master_service/dsl/scenario.h"

#include <thread>

namespace mooncake::test {

PutManyAction PutMany(std::string key_prefix, size_t count,
                      uint64_t object_size) {
    PutManyAction action{};
    action.key_prefix = std::move(key_prefix);
    action.count = count;
    action.object_size = object_size;
    return action;
}

SavedObjectsSpec SavedObjects(std::string name) {
    SavedObjectsSpec objects{};
    objects.name = std::move(name);
    return objects;
}

MasterScenario& MasterScenario::When(PutManyAction action) {
    if (!EnsureService()) {
        return *this;
    }
    if (action.count == 0 || action.object_size == 0 ||
        action.replica_count == 0) {
        Fail("PutMany requires non-zero count, size, and replicas");
        return *this;
    }
    const UUID actor = ActorId(action.actor);
    ReplicateConfig config;
    config.replica_num = action.replica_count;
    std::vector<std::string> successful_keys;
    size_t failures = 0;
    for (size_t index = 0; index < action.count; ++index) {
        const std::string key = action.key_prefix + std::to_string(index);
        const auto start = service_->PutStart(actor, key, TenantId::Default(),
                                              action.object_size, config);
        if (!start) {
            if (start.error() != ErrorCode::NO_AVAILABLE_HANDLE) {
                Fail("PutMany(" + key + ") failed: " + toString(start.error()));
            }
            ++failures;
            std::this_thread::sleep_for(action.wait_after_failure);
            continue;
        }
        bool completed = true;
        for (const auto replica_type : action.completion_types) {
            const auto end =
                service_->PutEnd(actor, key, TenantId::Default(), replica_type);
            if (!end) {
                Fail("PutMany(" + key +
                     ") completion failed: " + toString(end.error()));
                completed = false;
                break;
            }
        }
        if (!completed) {
            continue;
        }
        if (action.read_after_write) {
            const auto read =
                service_->GetReplicaList(key, TenantId::Default());
            if (!read) {
                Fail("PutMany(" + key +
                     ") read failed: " + toString(read.error()));
            }
        }
        successful_keys.push_back(key);
    }
    if (action.minimum_successes.has_value() &&
        successful_keys.size() < *action.minimum_successes) {
        Fail("PutMany succeeded " + std::to_string(successful_keys.size()) +
             " times; expected at least " +
             std::to_string(*action.minimum_successes));
    }
    if (action.minimum_failures.has_value() &&
        failures < *action.minimum_failures) {
        Fail("PutMany failed " + std::to_string(failures) +
             " times; expected at least " +
             std::to_string(*action.minimum_failures));
    }
    if (!action.saved_set.empty()) {
        saved_key_sets_[action.saved_set] = std::move(successful_keys);
    }
    return *this;
}

MasterScenario& MasterScenario::Then(SavedObjectsSpec objects) {
    if (!EnsureService()) {
        return *this;
    }
    const auto saved = saved_key_sets_.find(objects.name);
    if (saved == saved_key_sets_.end()) {
        Fail("SavedObjects references unknown set " + objects.name);
        return *this;
    }
    for (const auto& key : saved->second) {
        const auto result = service_->GetReplicaList(key, TenantId::Default());
        if (objects.expected_missing) {
            if (result || result.error() != ErrorCode::OBJECT_NOT_FOUND) {
                Fail("SavedObjects(" + objects.name + ") still contains " +
                     key);
            }
        } else if (!result) {
            Fail("SavedObjects(" + objects.name + ") cannot read " + key +
                 ": " + toString(result.error()));
        }
    }
    return *this;
}

}  // namespace mooncake::test
