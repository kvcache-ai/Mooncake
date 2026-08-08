#include "master_scenario.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <thread>
#include <utility>

#include "mutex.h"
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

PutEndAction PutEnd(std::string key) { return {.key = std::move(key)}; }

PutRevokeAction PutRevoke(std::string key) { return {.key = std::move(key)}; }

RemoveAction Remove(std::string key) { return {.key = std::move(key)}; }

ExpireAtAction ExpireAt(std::string key,
                        std::chrono::system_clock::time_point lease_timeout) {
    return {.key = std::move(key), .lease_timeout = lease_timeout};
}

MemoryEvictAction EvictMemory(double target_ratio) {
    return {.target_ratio = target_ratio, .lower_bound_ratio = target_ratio};
}

ObjectSpec<> Object(std::string key) { return ObjectSpec<>(std::move(key)); }

ObjectsSpec<> Objects(size_t begin, size_t end) {
    return ObjectsSpec<>(begin, end);
}

ObjectsSpec<> Objects(std::initializer_list<std::string> keys) {
    return ObjectsSpec<>(std::vector<std::string>(keys));
}

KeyCountSpec KeyCount(size_t value) { return {.value = value}; }

TenantQuotaSpec TenantQuota(std::string tenant) {
    return {.tenant = std::move(tenant)};
}

OpLogUnavailableSpec OpLogUnavailable() { return {}; }

MasterScenario::MasterScenario(std::string name) : name_(std::move(name)) {}

MasterScenario::MasterScenario(std::string name, MasterServiceConfig config,
                               std::shared_ptr<HaKvBackend> batch_oplog_backend)
    : name_(std::move(name)),
      config_(std::move(config)),
      batch_oplog_backend_(std::move(batch_oplog_backend)) {}

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
    config.replica_num = 1;
    config.preferred_segment = action.preferred_node;
    config.with_soft_pin = action.with_soft_pin;
    config.with_hard_pin = action.with_hard_pin;
    if (!action.group_id.empty()) {
        config.group_ids = {action.group_id};
    }
    const auto result =
        service_->PutStart(ActorId(action.actor), action.key,
                           TenantId(action.tenant), action.size, config);
    ValidateActionResult("PutStart(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    if (!result) {
        return *this;
    }
    if (action.expected_replica_count.has_value() &&
        result->size() != *action.expected_replica_count) {
        Fail("PutStart(" + action.key + ") returned " +
             std::to_string(result->size()) + " replicas; expected " +
             std::to_string(*action.expected_replica_count));
    }
    if (action.expected_replica_status.has_value() &&
        std::any_of(result->begin(), result->end(), [&](const auto& replica) {
            return replica.status != *action.expected_replica_status;
        })) {
        Fail("PutStart(" + action.key + ") replica status mismatch");
    }
    return *this;
}

MasterScenario& MasterScenario::When(PutEndAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->PutEnd(ActorId(action.actor), action.key,
                         TenantId(action.tenant), ReplicaType::MEMORY);
    ValidateActionResult("PutEnd(" + action.key + ")", action.expected_error,
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
                            TenantId(action.tenant), ReplicaType::MEMORY);
    ValidateActionResult("PutRevoke(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
    return *this;
}

MasterScenario& MasterScenario::When(RemoveAction action) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result = service_->Remove(action.key, TenantId(action.tenant));
    ValidateActionResult("Remove(" + action.key + ")", action.expected_error,
                         result.has_value(),
                         result ? ErrorCode::OK : result.error());
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
        metadata_it->second.lease_timeout = action.lease_timeout;
        metadata_it->second.soft_pin_timeout = action.soft_pin_timeout;
        return true;
    };

    const size_t routed = service_->getMetadataShardIndex(tenant, action.key);
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

MasterScenario& MasterScenario::ThenObject(ObjectSpecData object,
                                           ObjectExpectation expectation) {
    if (!EnsureService()) {
        return *this;
    }

    const auto result =
        service_->GetReplicaList(object.key, TenantId(object.tenant));
    if (expectation == ObjectExpectation::MISSING) {
        if (result || result.error() != ErrorCode::OBJECT_NOT_FOUND) {
            Fail("Object(" + object.key + ") was expected to be missing");
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

MasterScenario& MasterScenario::ThenObjects(ObjectsSpecData objects,
                                            ObjectExpectation expectation) {
    if (objects.keys.empty()) {
        Fail(
            "Objects requires at least one key; indexed ranges must use "
            "NamedBy");
        return *this;
    }
    for (auto& key : objects.keys) {
        ThenObject(
            ObjectSpecData{.key = std::move(key), .tenant = objects.tenant},
            expectation);
    }
    return *this;
}

MasterScenario& MasterScenario::Then(KeyCountSpec key_count) {
    if (!EnsureService()) {
        return *this;
    }
    const size_t actual = service_->GetKeyCount();
    if (actual != key_count.value) {
        Fail("KeyCount is " + std::to_string(actual) + "; expected " +
             std::to_string(key_count.value));
    }
    return *this;
}

MasterScenario& MasterScenario::Then(TenantQuotaSpec tenant_quota) {
    if (!EnsureService()) {
        return *this;
    }

    auto snapshot =
        service_->GetTenantQuotaSnapshot(TenantId(tenant_quota.tenant));
    if (!snapshot.has_value()) {
        Fail("TenantQuota(" + tenant_quota.tenant + ") is not registered");
        return *this;
    }

    const auto matches = [&tenant_quota](const TenantQuotaSnapshot& value) {
        return (!tenant_quota.used_bytes.has_value() ||
                value.used_bytes == *tenant_quota.used_bytes) &&
               (!tenant_quota.reserved_bytes.has_value() ||
                value.reserved_bytes == *tenant_quota.reserved_bytes);
    };
    const auto deadline =
        std::chrono::steady_clock::now() + tenant_quota.eventual_timeout;
    while (!matches(*snapshot) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        snapshot =
            service_->GetTenantQuotaSnapshot(TenantId(tenant_quota.tenant));
        if (!snapshot.has_value()) {
            Fail("TenantQuota(" + tenant_quota.tenant + ") is not registered");
            return *this;
        }
    }

    if (tenant_quota.used_bytes.has_value() &&
        snapshot->used_bytes != *tenant_quota.used_bytes) {
        Fail("TenantQuota(" + tenant_quota.tenant + ") uses " +
             std::to_string(snapshot->used_bytes) + "; expected " +
             std::to_string(*tenant_quota.used_bytes));
    }
    if (tenant_quota.reserved_bytes.has_value() &&
        snapshot->reserved_bytes != *tenant_quota.reserved_bytes) {
        Fail("TenantQuota(" + tenant_quota.tenant + ") reserves " +
             std::to_string(snapshot->reserved_bytes) + "; expected " +
             std::to_string(*tenant_quota.reserved_bytes));
    }
    return *this;
}

MasterScenario& MasterScenario::Then(OpLogUnavailableSpec oplog) {
    if (!EnsureService()) {
        return *this;
    }
    if (!service_->ordered_oplog_writer_) {
        Fail("OpLog writer is not configured");
        return *this;
    }
    const auto deadline = std::chrono::steady_clock::now() + oplog.timeout;
    while (service_->ordered_oplog_writer_->IsAccepting() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (service_->ordered_oplog_writer_->IsAccepting()) {
        Fail("OpLog writer was expected to be unavailable");
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
