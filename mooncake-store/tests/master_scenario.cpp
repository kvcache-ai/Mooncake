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

PutStartAction PutStart(std::string key, uint64_t size) {
    return {.key = std::move(key), .size = size};
}

PutEndAction PutEnd(std::string key) { return {.key = std::move(key)}; }

ObjectSpec Object(std::string key) { return {.key = std::move(key)}; }

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

MasterScenario& MasterScenario::When(PutStartAction action) {
    if (!EnsureService()) {
        return *this;
    }

    ReplicateConfig config;
    config.replica_num = 1;
    const auto result =
        service_->PutStart(ActorId(action.actor), action.key,
                           TenantId::Default(), action.size, config);
    ValidateActionResult("PutStart(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
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

MasterScenario& MasterScenario::Then(ObjectSpec object) {
    if (!EnsureService()) {
        return *this;
    }
    if (!object.expected_readable) {
        Fail("Object(" + object.key + ") has no assertion");
        return *this;
    }

    const auto result =
        service_->GetReplicaList(object.key, TenantId::Default());
    if (!result) {
        Fail("Object(" + object.key +
             ") is not readable: " + toString(result.error()));
    } else if (result->replicas.empty()) {
        Fail("Object(" + object.key + ") has no readable replicas");
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

void MasterScenario::Fail(std::string message) const {
    ADD_FAILURE() << "MasterScenario[" << name_ << "]: " << message;
}

}  // namespace mooncake::test
