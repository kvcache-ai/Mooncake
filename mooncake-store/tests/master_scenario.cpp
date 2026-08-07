#include "master_scenario.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <utility>

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
    return {.name = std::move(name)};
}

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

RemoveAction Remove(std::string key) { return {.key = std::move(key)}; }

ObjectSpec<> Object(std::string key) { return ObjectSpec<>(std::move(key)); }

MasterScenario::MasterScenario(std::string name) : name_(std::move(name)) {}

MasterScenario::~MasterScenario() = default;

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

MasterScenario& MasterScenario::WhenPutStart(PutStartActionData action) {
    if (!EnsureService()) {
        return *this;
    }

    ReplicateConfig config;
    config.replica_num = 1;
    const auto result =
        service_->PutStart(ActorId(action.actor), action.key,
                           TenantId::Default(), action.size, config);
    ValidateStartResult("PutStart(" + action.key + ")", action.expected_error,
                        action.expected_replica_count,
                        action.expected_replica_status, result);
    return *this;
}

MasterScenario& MasterScenario::WhenUpsertStart(UpsertStartActionData action) {
    if (!EnsureService()) {
        return *this;
    }

    ReplicateConfig config;
    config.replica_num = 1;
    const auto result =
        service_->UpsertStart(ActorId(action.actor), action.key,
                              TenantId::Default(), action.size, config);
    ValidateStartResult("UpsertStart(" + action.key + ")",
                        action.expected_error, action.expected_replica_count,
                        action.expected_replica_status, result);
    return *this;
}

MasterScenario& MasterScenario::When(PutEndAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->PutEnd(ActorId(action.actor), action.key, TenantId::Default(),
                         ReplicaType::MEMORY);
    ValidateActionResult("PutEnd(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(UpsertEndAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->UpsertEnd(ActorId(action.actor), action.key,
                            TenantId::Default(), ReplicaType::MEMORY);
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
                            TenantId::Default(), ReplicaType::MEMORY);
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
                               TenantId::Default(), ReplicaType::MEMORY);
    ValidateActionResult("UpsertRevoke(" + action.key + ")",
                         action.expected_error, result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(RemoveAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->Remove(action.key, TenantId::Default());
    ValidateActionResult("Remove(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::ThenObject(ObjectSpecData object,
                                           ObjectExpectation expectation) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->GetReplicaList(object.key, TenantId::Default());
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
    return *this;
}

bool MasterScenario::EnsureService() {
    if (service_) {
        return true;
    }
    declarations_frozen_ = true;
    if (nodes_.empty()) {
        Fail("scenario requires at least one MemoryNode");
        return false;
    }

    service_ = std::make_unique<MasterService>();
    for (const auto& node : nodes_) {
        Segment segment;
        segment.id = StableUuid("segment", node.name);
        segment.name = node.name;
        segment.base = next_segment_base_;
        segment.size = node.capacity;
        segment.te_endpoint = node.name;
        next_segment_base_ += node.capacity + 4096;

        const auto result = service_->MountSegment(segment, ActorId(node.name));
        if (!result) {
            Fail("failed to mount MemoryNode " + node.name + ": " +
                 toString(result.error()));
            service_.reset();
            return false;
        }
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
    const StartResult& result) {
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
}

void MasterScenario::Fail(std::string message) const {
    ADD_FAILURE() << "MasterScenario[" << name_ << "]: " << message;
}

}  // namespace mooncake::test
