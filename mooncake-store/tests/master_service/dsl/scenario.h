#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "master_service.h"

namespace mooncake::test {

class MasterScenario;

constexpr uint64_t operator""_KB(unsigned long long value) {
    return value * 1024;
}

struct MemoryNodeSpec {
    std::string name;
    uint64_t capacity{16 * 1024 * 1024};
    std::optional<std::string> endpoint{};
    std::string host_id;
    std::string owner;

    MemoryNodeSpec& Capacity(uint64_t value) {
        capacity = value;
        return *this;
    }

    MemoryNodeSpec& Endpoint(std::string value) {
        endpoint = std::move(value);
        return *this;
    }

    MemoryNodeSpec& OnHost(std::string value) {
        host_id = std::move(value);
        return *this;
    }

    MemoryNodeSpec& OwnedBy(std::string value) {
        owner = std::move(value);
        return *this;
    }
};

MemoryNodeSpec MemoryNode(std::string name);

struct NoFNodeSpec {
    std::string name;
    std::string endpoint;
    uint64_t capacity{16 * 1024 * 1024};
    std::string owner;

    NoFNodeSpec& Endpoint(std::string value) {
        endpoint = std::move(value);
        return *this;
    }

    NoFNodeSpec& Capacity(uint64_t value) {
        capacity = value;
        return *this;
    }

    NoFNodeSpec& OwnedBy(std::string value) {
        owner = std::move(value);
        return *this;
    }
};

NoFNodeSpec NoFNode(std::string name);

struct TenantSpec {
    std::string name;
    uint64_t quota_bytes{64 * 1024 * 1024};

    TenantSpec& Quota(uint64_t value) {
        quota_bytes = value;
        return *this;
    }
};

TenantSpec Tenant(std::string name);

enum class PutStartExpectation {
    UNSPECIFIED,
    SUCCESS,
    ERROR,
};

template <PutStartExpectation expectation = PutStartExpectation::UNSPECIFIED>
struct PutStartAction;

struct PutStartActionData {
    PutStartActionData(const PutStartActionData&) = default;

   private:
    PutStartActionData(std::string value, uint64_t object_size)
        : key(std::move(value)), size(object_size) {}

    std::string key;
    uint64_t size;
    std::string actor{"default"};
    size_t requested_replica_count{1};
    size_t requested_nof_replica_count{0};
    size_t requested_dfs_replica_count{0};
    std::string tenant{TenantId::Default().value()};
    std::string preferred_node;
    std::vector<std::string> preferred_nodes;
    std::string host_id;
    std::optional<std::vector<std::string>> group_ids{};
    bool prefer_same_node{false};
    SoftPinAction soft_pin_action{SoftPinAction::PRESERVE};
    std::optional<uint64_t> soft_pin_ttl_ms{};
    bool with_hard_pin{false};
    std::optional<ErrorCode> expected_error{};
    std::optional<size_t> expected_replica_count{};
    std::optional<ReplicaStatus> expected_replica_status{};
    std::optional<std::vector<std::string>> expected_memory_nodes{};

    template <PutStartExpectation>
    friend struct PutStartAction;
    friend class MasterScenario;
};

template <PutStartExpectation expectation>
struct PutStartAction : PutStartActionData {
    PutStartAction(std::string key, uint64_t size)
        requires(expectation == PutStartExpectation::UNSPECIFIED)
        : PutStartActionData(std::move(key), size) {}

    PutStartAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    PutStartAction& Replicas(size_t value) {
        requested_replica_count = value;
        return *this;
    }

    PutStartAction& NofReplicas(size_t value) {
        requested_nof_replica_count = value;
        return *this;
    }

    PutStartAction& DfsReplicas(size_t value) {
        requested_dfs_replica_count = value;
        return *this;
    }

    PutStartAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    PutStartAction& OnNode(std::string value) {
        preferred_node = std::move(value);
        return *this;
    }

    PutStartAction& OnNodes(std::initializer_list<std::string> values) {
        preferred_nodes.assign(values.begin(), values.end());
        return *this;
    }

    PutStartAction& FromHost(std::string value) {
        host_id = std::move(value);
        return *this;
    }

    PutStartAction& PreferSameNode() {
        prefer_same_node = true;
        return *this;
    }

    PutStartAction& InGroup(std::string value) {
        group_ids = std::vector<std::string>{std::move(value)};
        return *this;
    }

    PutStartAction& InGroups(std::initializer_list<std::string> values) {
        group_ids.emplace(values.begin(), values.end());
        return *this;
    }

    PutStartAction& WithSoftPin() {
        soft_pin_action = SoftPinAction::ENABLE;
        return *this;
    }

    PutStartAction& WithSoftPinAction(SoftPinAction value) {
        soft_pin_action = value;
        return *this;
    }

    PutStartAction& WithSoftPinTtl(uint64_t value) {
        soft_pin_ttl_ms = value;
        return *this;
    }

    PutStartAction& WithHardPin() {
        with_hard_pin = true;
        return *this;
    }

    auto ExpectError(ErrorCode value) const
        requires(expectation == PutStartExpectation::UNSPECIFIED)
    {
        PutStartAction<PutStartExpectation::ERROR> action(*this);
        action.expected_error = value;
        return action;
    }

    auto ExpectReplicas(size_t value) const
        requires(expectation != PutStartExpectation::ERROR)
    {
        PutStartAction<PutStartExpectation::SUCCESS> action(*this);
        action.expected_replica_count = value;
        return action;
    }

    auto ExpectStatus(ReplicaStatus value) const
        requires(expectation != PutStartExpectation::ERROR)
    {
        PutStartAction<PutStartExpectation::SUCCESS> action(*this);
        action.expected_replica_status = value;
        return action;
    }

    auto ExpectMemoryNodes(std::initializer_list<std::string> values) const
        requires(expectation != PutStartExpectation::ERROR)
    {
        PutStartAction<PutStartExpectation::SUCCESS> action(*this);
        action.expected_memory_nodes.emplace(values.begin(), values.end());
        return action;
    }

   private:
    template <PutStartExpectation other>
    friend struct PutStartAction;

    template <PutStartExpectation other>
    PutStartAction(const PutStartAction<other>& action)
        : PutStartActionData(action) {}
};

PutStartAction<> PutStart(std::string key, uint64_t size);

enum class UpsertStartExpectation {
    UNSPECIFIED,
    SUCCESS,
    ERROR,
};

template <UpsertStartExpectation expectation =
              UpsertStartExpectation::UNSPECIFIED>
struct UpsertStartAction;

struct UpsertStartActionData {
    UpsertStartActionData(const UpsertStartActionData&) = default;

   private:
    UpsertStartActionData(std::string value, uint64_t object_size)
        : key(std::move(value)), size(object_size) {}

    std::string key;
    uint64_t size;
    std::string actor{"default"};
    size_t requested_replica_count{1};
    std::string tenant{TenantId::Default().value()};
    std::string preferred_node;
    std::optional<std::string> group_id{};
    std::optional<ErrorCode> expected_error{};
    std::optional<size_t> expected_replica_count{};
    std::optional<ReplicaStatus> expected_replica_status{};
    std::optional<bool> expected_buffer_reuse{};

    template <UpsertStartExpectation>
    friend struct UpsertStartAction;
    friend class MasterScenario;
};

template <UpsertStartExpectation expectation>
struct UpsertStartAction : UpsertStartActionData {
    UpsertStartAction(std::string key, uint64_t size)
        requires(expectation == UpsertStartExpectation::UNSPECIFIED)
        : UpsertStartActionData(std::move(key), size) {}

    UpsertStartAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    UpsertStartAction& Replicas(size_t value) {
        requested_replica_count = value;
        return *this;
    }

    UpsertStartAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    UpsertStartAction& OnNode(std::string value) {
        preferred_node = std::move(value);
        return *this;
    }

    UpsertStartAction& InGroup(std::string value) {
        group_id = std::move(value);
        return *this;
    }

    auto ExpectError(ErrorCode value) const
        requires(expectation == UpsertStartExpectation::UNSPECIFIED)
    {
        UpsertStartAction<UpsertStartExpectation::ERROR> action(*this);
        action.expected_error = value;
        return action;
    }

    auto ExpectReplicas(size_t value) const
        requires(expectation != UpsertStartExpectation::ERROR)
    {
        UpsertStartAction<UpsertStartExpectation::SUCCESS> action(*this);
        action.expected_replica_count = value;
        return action;
    }

    auto ExpectStatus(ReplicaStatus value) const
        requires(expectation != UpsertStartExpectation::ERROR)
    {
        UpsertStartAction<UpsertStartExpectation::SUCCESS> action(*this);
        action.expected_replica_status = value;
        return action;
    }

    auto ExpectBufferReuse() const
        requires(expectation != UpsertStartExpectation::ERROR)
    {
        UpsertStartAction<UpsertStartExpectation::SUCCESS> action(*this);
        action.expected_buffer_reuse = true;
        return action;
    }

    auto ExpectNewBuffer() const
        requires(expectation != UpsertStartExpectation::ERROR)
    {
        UpsertStartAction<UpsertStartExpectation::SUCCESS> action(*this);
        action.expected_buffer_reuse = false;
        return action;
    }

   private:
    template <UpsertStartExpectation other>
    friend struct UpsertStartAction;

    template <UpsertStartExpectation other>
    UpsertStartAction(const UpsertStartAction<other>& action)
        : UpsertStartActionData(action) {}
};

UpsertStartAction<> UpsertStart(std::string key, uint64_t size);

struct PutEndAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<uint64_t> checksum{};
    std::optional<ErrorCode> expected_error{};
    ReplicaType replica_type{ReplicaType::MEMORY};

    PutEndAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    PutEndAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    PutEndAction& WithChecksum(uint64_t value) {
        checksum = value;
        return *this;
    }

    PutEndAction& OfType(ReplicaType value) {
        replica_type = value;
        return *this;
    }

    PutEndAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

PutEndAction PutEnd(std::string key);

struct UpsertEndAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<uint64_t> checksum{};
    std::optional<ErrorCode> expected_error{};
    ReplicaType replica_type{ReplicaType::MEMORY};

    UpsertEndAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    UpsertEndAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    UpsertEndAction& WithChecksum(uint64_t value) {
        checksum = value;
        return *this;
    }

    UpsertEndAction& OfType(ReplicaType value) {
        replica_type = value;
        return *this;
    }

    UpsertEndAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

UpsertEndAction UpsertEnd(std::string key);

struct PutRevokeAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};
    ReplicaType replica_type{ReplicaType::MEMORY};

    PutRevokeAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    PutRevokeAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    PutRevokeAction& OfType(ReplicaType value) {
        replica_type = value;
        return *this;
    }

    PutRevokeAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

PutRevokeAction PutRevoke(std::string key);

struct UpsertRevokeAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};
    ReplicaType replica_type{ReplicaType::MEMORY};

    UpsertRevokeAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    UpsertRevokeAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    UpsertRevokeAction& OfType(ReplicaType value) {
        replica_type = value;
        return *this;
    }

    UpsertRevokeAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

UpsertRevokeAction UpsertRevoke(std::string key);

struct BatchUpsertStartAction {
    std::vector<std::string> keys;
    std::vector<uint64_t> sizes;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    size_t requested_replica_count{1};
    std::string preferred_node;
    std::optional<std::vector<std::string>> group_ids{};
    std::optional<ErrorCode> expected_error{};
    std::optional<size_t> expected_replica_count{};
    std::optional<ReplicaStatus> expected_replica_status{};

    BatchUpsertStartAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    BatchUpsertStartAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    BatchUpsertStartAction& Replicas(size_t value) {
        requested_replica_count = value;
        return *this;
    }

    BatchUpsertStartAction& OnNode(std::string value) {
        preferred_node = std::move(value);
        return *this;
    }

    BatchUpsertStartAction& InGroups(
        std::initializer_list<std::string> values) {
        group_ids.emplace(values.begin(), values.end());
        return *this;
    }

    BatchUpsertStartAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }

    BatchUpsertStartAction& ExpectReplicas(size_t value) {
        expected_replica_count = value;
        return *this;
    }

    BatchUpsertStartAction& ExpectStatus(ReplicaStatus value) {
        expected_replica_status = value;
        return *this;
    }
};

BatchUpsertStartAction BatchUpsertStart(
    std::initializer_list<std::pair<std::string, uint64_t>> objects);

struct BatchUpsertEndAction {
    std::vector<std::string> keys;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};

    BatchUpsertEndAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    BatchUpsertEndAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }
};

BatchUpsertEndAction BatchUpsertEnd(std::initializer_list<std::string> keys);

struct BatchRemoveAction {
    std::vector<std::string> keys;
    std::string tenant{TenantId::Default().value()};
    bool force{false};
    std::optional<ErrorCode> expected_error{};

    BatchRemoveAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    BatchRemoveAction& Force() {
        force = true;
        return *this;
    }

    BatchRemoveAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

BatchRemoveAction BatchRemove(std::initializer_list<std::string> keys);

struct RemoveAction {
    std::string key;
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};
    bool force{false};

    RemoveAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }

    RemoveAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    RemoveAction& Force() {
        force = true;
        return *this;
    }
};

RemoveAction Remove(std::string key);

struct ClearReplicasAction {
    std::vector<std::string> keys;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::string node;
    std::optional<std::vector<std::string>> expected_cleared{};
    std::optional<ErrorCode> expected_error{};

    ClearReplicasAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    ClearReplicasAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    ClearReplicasAction& FromNode(std::string value) {
        node = std::move(value);
        return *this;
    }

    ClearReplicasAction& ExpectCleared(
        std::initializer_list<std::string> values) {
        expected_cleared.emplace(values.begin(), values.end());
        return *this;
    }

    ClearReplicasAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

ClearReplicasAction ClearReplicas(std::initializer_list<std::string> keys);

struct CopyStartAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::string source{};
    std::vector<std::string> targets{};
    std::optional<std::vector<std::string>> expected_allocated_targets{};
    std::optional<ErrorCode> expected_error{};

    CopyStartAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    CopyStartAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    CopyStartAction& From(std::string value) {
        source = std::move(value);
        return *this;
    }

    CopyStartAction& To(std::initializer_list<std::string> values) {
        targets.assign(values.begin(), values.end());
        return *this;
    }

    CopyStartAction& ExpectAllocatedTargets(
        std::initializer_list<std::string> values) {
        expected_allocated_targets.emplace(values.begin(), values.end());
        return *this;
    }

    CopyStartAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

CopyStartAction CopyStart(std::string key);

struct CopyEndAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};

    CopyEndAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    CopyEndAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    CopyEndAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

CopyEndAction CopyEnd(std::string key);

struct CopyRevokeAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};

    CopyRevokeAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    CopyRevokeAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    CopyRevokeAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

CopyRevokeAction CopyRevoke(std::string key);

struct MoveStartAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::string source{};
    std::string target{};
    std::optional<bool> expected_target_allocation{};
    std::optional<ErrorCode> expected_error{};

    MoveStartAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    MoveStartAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    MoveStartAction& From(std::string value) {
        source = std::move(value);
        return *this;
    }

    MoveStartAction& To(std::string value) {
        target = std::move(value);
        return *this;
    }

    MoveStartAction& ExpectTargetAllocation() {
        expected_target_allocation = true;
        return *this;
    }

    MoveStartAction& ExpectNoTargetAllocation() {
        expected_target_allocation = false;
        return *this;
    }

    MoveStartAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

MoveStartAction MoveStart(std::string key);

struct MoveEndAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};

    MoveEndAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    MoveEndAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    MoveEndAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

MoveEndAction MoveEnd(std::string key);

struct MoveRevokeAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};

    MoveRevokeAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    MoveRevokeAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    MoveRevokeAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

MoveRevokeAction MoveRevoke(std::string key);

struct CreateCopyTaskAction {
    std::string name;
    std::string key;
    std::string tenant{TenantId::Default().value()};
    std::vector<std::string> targets{};
    std::optional<ErrorCode> expected_error{};

    CreateCopyTaskAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    CreateCopyTaskAction& To(std::initializer_list<std::string> values) {
        targets.assign(values.begin(), values.end());
        return *this;
    }

    CreateCopyTaskAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

CreateCopyTaskAction CreateCopyTask(std::string name, std::string key);

struct CreateMoveTaskAction {
    std::string name;
    std::string key;
    std::string tenant{TenantId::Default().value()};
    std::string source{};
    std::string target{};
    std::optional<ErrorCode> expected_error{};

    CreateMoveTaskAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    CreateMoveTaskAction& From(std::string value) {
        source = std::move(value);
        return *this;
    }

    CreateMoveTaskAction& To(std::string value) {
        target = std::move(value);
        return *this;
    }

    CreateMoveTaskAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

CreateMoveTaskAction CreateMoveTask(std::string name, std::string key);

struct FetchTasksAction {
    std::string actor{"default"};
    size_t batch_size{16};
    std::optional<size_t> expected_count{};
    std::optional<ErrorCode> expected_error{};

    FetchTasksAction& Limit(size_t value) {
        batch_size = value;
        return *this;
    }

    FetchTasksAction& ExpectCount(size_t value) {
        expected_count = value;
        return *this;
    }

    FetchTasksAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

FetchTasksAction FetchTasks(std::string actor);

struct CompleteTaskAction {
    std::string name;
    std::string actor{"default"};
    TaskStatus status{TaskStatus::SUCCESS};
    std::string message{};
    std::optional<ErrorCode> expected_error{};

    CompleteTaskAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    CompleteTaskAction& WithStatus(TaskStatus value) {
        status = value;
        return *this;
    }

    CompleteTaskAction& WithMessage(std::string value) {
        message = std::move(value);
        return *this;
    }

    CompleteTaskAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

CompleteTaskAction CompleteTask(std::string name);

struct CompleteUnknownTaskAction {
    std::string name;
    std::string actor{"default"};
    TaskStatus status{TaskStatus::FAILED};
    std::string message{};
    std::optional<ErrorCode> expected_error{};

    CompleteUnknownTaskAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    CompleteUnknownTaskAction& WithStatus(TaskStatus value) {
        status = value;
        return *this;
    }

    CompleteUnknownTaskAction& WithMessage(std::string value) {
        message = std::move(value);
        return *this;
    }

    CompleteUnknownTaskAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

CompleteUnknownTaskAction CompleteUnknownTask(std::string name);

struct RemoveByRegexAction {
    std::string pattern;
    std::string tenant{TenantId::Default().value()};
    bool force{false};
    std::optional<size_t> expected_removed{};
    std::optional<ErrorCode> expected_error{};

    RemoveByRegexAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    RemoveByRegexAction& Force() {
        force = true;
        return *this;
    }

    RemoveByRegexAction& ExpectRemoved(size_t value) {
        expected_removed = value;
        return *this;
    }

    RemoveByRegexAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

RemoveByRegexAction RemoveByRegex(std::string pattern);

struct UnmountMemoryNodeAction {
    std::string node;
    std::string actor;
    std::optional<ErrorCode> expected_error{};
    bool unknown{false};
    std::optional<std::chrono::milliseconds> maximum_duration{};

    UnmountMemoryNodeAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    UnmountMemoryNodeAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }

    UnmountMemoryNodeAction& Unknown() {
        unknown = true;
        return *this;
    }

    UnmountMemoryNodeAction& Within(std::chrono::milliseconds value) {
        maximum_duration = value;
        return *this;
    }
};

UnmountMemoryNodeAction UnmountMemoryNode(std::string node);

struct GracefulUnmountMemoryNodeAction {
    std::string node;
    std::string actor;
    uint64_t grace_period_ms{1000};
    std::optional<ErrorCode> expected_error{};

    GracefulUnmountMemoryNodeAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    GracefulUnmountMemoryNodeAction& After(std::chrono::milliseconds value) {
        grace_period_ms = static_cast<uint64_t>(value.count());
        return *this;
    }

    GracefulUnmountMemoryNodeAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

GracefulUnmountMemoryNodeAction GracefullyUnmountMemoryNode(std::string node);

struct MountMemorySegmentAction {
    std::string alias;
    std::string name;
    uintptr_t base{0x400000000};
    uint64_t capacity{16 * 1024 * 1024};
    std::string endpoint;
    std::string actor;
    std::optional<ErrorCode> expected_error{};

    MountMemorySegmentAction& Named(std::string value) {
        name = std::move(value);
        return *this;
    }

    MountMemorySegmentAction& Base(uintptr_t value) {
        base = value;
        return *this;
    }

    MountMemorySegmentAction& Capacity(uint64_t value) {
        capacity = value;
        return *this;
    }

    MountMemorySegmentAction& Endpoint(std::string value) {
        endpoint = std::move(value);
        return *this;
    }

    MountMemorySegmentAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    MountMemorySegmentAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

MountMemorySegmentAction MountMemorySegment(std::string alias);

struct MountUnmountMemoryCapacitiesAction {
    std::string name;
    std::vector<uint64_t> capacities;
};

MountUnmountMemoryCapacitiesAction MountUnmountMemoryCapacities(
    std::string name, std::initializer_list<uint64_t> capacities);

struct RemoveAllAction {
    std::optional<std::string> tenant{};
    bool force{false};
    std::optional<size_t> expected_removed{};

    RemoveAllAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    RemoveAllAction& Force() {
        force = true;
        return *this;
    }

    RemoveAllAction& ExpectRemoved(size_t value) {
        expected_removed = value;
        return *this;
    }
};

RemoveAllAction RemoveAll();

struct ExpireAtAction {
    std::string key;
    std::chrono::system_clock::time_point lease_timeout;
    std::string tenant{TenantId::Default().value()};
    std::optional<std::chrono::system_clock::time_point> soft_pin_timeout{};

    ExpireAtAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    ExpireAtAction& SoftPinnedUntil(
        std::chrono::system_clock::time_point value) {
        soft_pin_timeout = value;
        return *this;
    }
};

ExpireAtAction ExpireAt(std::string key,
                        std::chrono::system_clock::time_point lease_timeout);

struct MemoryEvictAction {
    double target_ratio;
    double lower_bound_ratio;

    MemoryEvictAction& ToLowerBound(double value) {
        lower_bound_ratio = value;
        return *this;
    }
};

MemoryEvictAction EvictMemory(double target_ratio);

struct DfsEvictAction {};

DfsEvictAction EvictDfs();

struct WaitAction {
    std::chrono::milliseconds duration;
};

WaitAction WaitFor(std::chrono::milliseconds duration);

struct WaitForOpLogFailureAction {
    std::chrono::milliseconds timeout{std::chrono::seconds(1)};
};

WaitForOpLogFailureAction WaitForOpLogFailure();

struct PingAction {
    std::string actor;
    std::optional<ErrorCode> expected_error{};

    PingAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

PingAction Ping(std::string actor);

struct MountMemoryNodeAction {
    std::string node;
    std::string actor;
    std::optional<ErrorCode> expected_error{};

    MountMemoryNodeAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    MountMemoryNodeAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

MountMemoryNodeAction MountMemoryNode(std::string node);

struct RacePutStartAction {
    std::string key;
    uint64_t size;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::vector<std::string> group_ids;
    size_t thread_count{16};
    size_t expected_successes{1};
    size_t expected_completions{1};

    RacePutStartAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    RacePutStartAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    RacePutStartAction& AcrossGroups(
        std::initializer_list<std::string> values) {
        group_ids.assign(values.begin(), values.end());
        return *this;
    }

    RacePutStartAction& Threads(size_t value) {
        thread_count = value;
        return *this;
    }

    RacePutStartAction& ExpectSuccesses(size_t value) {
        expected_successes = value;
        return *this;
    }

    RacePutStartAction& ExpectCompletions(size_t value) {
        expected_completions = value;
        return *this;
    }
};

RacePutStartAction RacePutStart(std::string key, uint64_t size);

struct ConcurrentMountUnmountAction {
    std::string segment_prefix;
    size_t thread_count{4};
    size_t iterations{100};
    uint64_t capacity{16 * 1024 * 1024};

    ConcurrentMountUnmountAction& Threads(size_t value) {
        thread_count = value;
        return *this;
    }

    ConcurrentMountUnmountAction& Iterations(size_t value) {
        iterations = value;
        return *this;
    }

    ConcurrentMountUnmountAction& Capacity(uint64_t value) {
        capacity = value;
        return *this;
    }
};

ConcurrentMountUnmountAction ConcurrentMountUnmount(std::string segment_prefix);

struct ConcurrentWriteAndRemoveAllAction {
    std::string key_prefix;
    size_t writer_count{4};
    size_t objects_per_writer{100};
    uint64_t object_size{1_KB};
    std::chrono::milliseconds remove_delay{50};

    ConcurrentWriteAndRemoveAllAction& Writers(size_t value) {
        writer_count = value;
        return *this;
    }

    ConcurrentWriteAndRemoveAllAction& ObjectsPerWriter(size_t value) {
        objects_per_writer = value;
        return *this;
    }

    ConcurrentWriteAndRemoveAllAction& ObjectSize(uint64_t value) {
        object_size = value;
        return *this;
    }

    ConcurrentWriteAndRemoveAllAction& RemoveAfter(
        std::chrono::milliseconds value) {
        remove_delay = value;
        return *this;
    }
};

ConcurrentWriteAndRemoveAllAction ConcurrentWriteAndRemoveAll(
    std::string key_prefix);

struct ConcurrentReadAndRemoveAllAction {
    std::string key_prefix;
    size_t object_count;
    size_t reader_count{4};
    std::chrono::milliseconds remove_delay{10};

    ConcurrentReadAndRemoveAllAction& Readers(size_t value) {
        reader_count = value;
        return *this;
    }

    ConcurrentReadAndRemoveAllAction& RemoveAfter(
        std::chrono::milliseconds value) {
        remove_delay = value;
        return *this;
    }
};

ConcurrentReadAndRemoveAllAction ConcurrentReadAndRemoveAll(
    std::string key_prefix, size_t object_count);

struct ConcurrentRemoveAllAction {
    size_t thread_count{2};
    size_t expected_total_removed;

    ConcurrentRemoveAllAction& Threads(size_t value) {
        thread_count = value;
        return *this;
    }
};

ConcurrentRemoveAllAction ConcurrentRemoveAll(size_t expected_total_removed);

struct PutManyAction {
    std::string key_prefix;
    size_t count;
    uint64_t object_size;
    std::string actor{"default"};
    size_t replica_count{1};
    bool read_after_write{false};
    std::string saved_set;
    std::optional<size_t> minimum_successes{};
    std::optional<size_t> minimum_failures{};
    std::chrono::milliseconds wait_after_failure{};
    std::vector<ReplicaType> completion_types{ReplicaType::MEMORY};

    PutManyAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    PutManyAction& Replicas(size_t value) {
        replica_count = value;
        return *this;
    }

    PutManyAction& ReadAfterWrite() {
        read_after_write = true;
        return *this;
    }

    PutManyAction& SaveSuccessfulAs(std::string value) {
        saved_set = std::move(value);
        return *this;
    }

    PutManyAction& ExpectMoreThanSuccesses(size_t value) {
        minimum_successes = value + 1;
        return *this;
    }

    PutManyAction& ExpectMoreThanFailures(size_t value) {
        minimum_failures = value + 1;
        return *this;
    }

    PutManyAction& WaitAfterFailure(std::chrono::milliseconds value) {
        wait_after_failure = value;
        return *this;
    }

    PutManyAction& CompleteWith(ReplicaType value) {
        completion_types = {value};
        return *this;
    }

    PutManyAction& CompleteWith(std::initializer_list<ReplicaType> values) {
        completion_types.assign(values.begin(), values.end());
        return *this;
    }
};

PutManyAction PutMany(std::string key_prefix, size_t count,
                      uint64_t object_size);

struct MountLocalDiskAction {
    std::string actor;
    bool enable_offloading{false};
    std::optional<ErrorCode> expected_error{};

    MountLocalDiskAction& WithOffloading(bool value = true) {
        enable_offloading = value;
        return *this;
    }

    MountLocalDiskAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

MountLocalDiskAction MountLocalDisk(std::string actor);

struct UnmountLocalDiskAction {
    std::string actor;
    std::optional<ErrorCode> expected_error{};

    UnmountLocalDiskAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

UnmountLocalDiskAction UnmountLocalDisk(std::string actor);

struct ConcurrentMountLocalDisksAction {
    size_t client_count{100};
    bool enable_offloading{true};

    ConcurrentMountLocalDisksAction& Clients(size_t value) {
        client_count = value;
        return *this;
    }

    ConcurrentMountLocalDisksAction& WithOffloading(bool value = true) {
        enable_offloading = value;
        return *this;
    }
};

ConcurrentMountLocalDisksAction ConcurrentMountLocalDisks();

struct OffloadHeartbeatAction {
    std::string actor;
    bool enable_offloading{true};
    std::optional<size_t> expected_task_count{};
    std::string expected_saved_set;
    std::optional<int64_t> expected_object_size{};
    std::optional<ErrorCode> expected_error{};

    OffloadHeartbeatAction& WithOffloading(bool value) {
        enable_offloading = value;
        return *this;
    }

    OffloadHeartbeatAction& ExpectTasks(size_t value) {
        expected_task_count = value;
        return *this;
    }

    OffloadHeartbeatAction& ExpectSavedObjects(std::string value) {
        expected_saved_set = std::move(value);
        return *this;
    }

    OffloadHeartbeatAction& ExpectObjectSize(int64_t value) {
        expected_object_size = value;
        return *this;
    }

    OffloadHeartbeatAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

OffloadHeartbeatAction OffloadHeartbeat(std::string actor);

struct NotifyOffloadSuccessAction {
    std::string actor;
    std::string tenant;
    std::string key;
    int64_t object_size;
    std::string endpoint{"disk-endpoint"};
    std::optional<ErrorCode> expected_error{};

    NotifyOffloadSuccessAction& At(std::string value) {
        endpoint = std::move(value);
        return *this;
    }

    NotifyOffloadSuccessAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

NotifyOffloadSuccessAction NotifyOffloadSuccess(std::string actor,
                                                std::string tenant,
                                                std::string key,
                                                int64_t object_size);

struct ReportSsdCapacityAction {
    std::string actor;
    int64_t capacity_bytes;
    std::optional<ErrorCode> expected_error{};

    ReportSsdCapacityAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

ReportSsdCapacityAction ReportSsdCapacity(std::string actor,
                                          int64_t capacity_bytes);

struct EvictDiskReplicaAction {
    std::string actor;
    std::string key;
    ReplicaType replica_type{ReplicaType::DISK};
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};

    EvictDiskReplicaAction& OfType(ReplicaType value) {
        replica_type = value;
        return *this;
    }

    EvictDiskReplicaAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    EvictDiskReplicaAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

EvictDiskReplicaAction EvictDiskReplica(std::string actor, std::string key);

enum class ObjectExpectation {
    UNSPECIFIED,
    READABLE,
    NOT_READY,
    MISSING,
};

template <ObjectExpectation expectation = ObjectExpectation::UNSPECIFIED>
struct ObjectSpec;

struct ObjectSpecData {
    ObjectSpecData(const ObjectSpecData&) = default;

   private:
    explicit ObjectSpecData(std::string value) : key(std::move(value)) {}

    std::string key;
    std::string tenant{TenantId::Default().value()};
    std::optional<size_t> expected_replica_count{};
    std::optional<size_t> expected_complete_replica_count{};
    std::optional<std::optional<uint64_t>> expected_checksum{};
    std::optional<uint64_t> expected_memory_replica_size{};
    std::optional<size_t> expected_memory_replica_count{};
    std::optional<size_t> expected_nof_replica_count{};
    std::optional<size_t> expected_dfs_replica_count{};
    std::optional<size_t> expected_local_disk_replica_count{};
    std::optional<size_t> expected_disk_replica_count{};
    std::optional<size_t> expected_complete_memory_replica_count{};
    std::optional<size_t> expected_complete_nof_replica_count{};
    std::optional<size_t> expected_complete_dfs_replica_count{};
    std::optional<size_t> expected_complete_local_disk_replica_count{};
    std::optional<size_t> expected_complete_disk_replica_count{};
    std::optional<std::string> expected_memory_node{};
    std::optional<std::vector<std::string>> expected_memory_nodes{};
    bool expect_distinct_memory_nodes{false};

    template <ObjectExpectation>
    friend struct ObjectSpec;
    friend class MasterScenario;
};

template <ObjectExpectation expectation>
struct ObjectSpec : ObjectSpecData {
    explicit ObjectSpec(std::string key)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
        : ObjectSpecData(std::move(key)) {}

    auto IsReadable() const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        return ObjectSpec<ObjectExpectation::READABLE>(*this);
    }

    auto IsNotReady() const
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        return ObjectSpec<ObjectExpectation::NOT_READY>(*this);
    }

    auto DoesNotExist() const
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        return ObjectSpec<ObjectExpectation::MISSING>(*this);
    }

    ObjectSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    auto HasReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_replica_count = value;
        return object;
    }

    auto HasCompleteReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_complete_replica_count = value;
        return object;
    }

    auto HasChecksum(uint64_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_checksum.emplace(value);
        return object;
    }

    auto HasNoChecksum() const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_checksum.emplace(std::nullopt);
        return object;
    }

    auto HasMemoryReplicaSize(uint64_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_memory_replica_size = value;
        return object;
    }

    auto HasMemoryReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_memory_replica_count = value;
        return object;
    }

    auto HasNoFReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_nof_replica_count = value;
        return object;
    }

    auto HasDfsReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_dfs_replica_count = value;
        return object;
    }

    auto HasLocalDiskReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_local_disk_replica_count = value;
        return object;
    }

    auto HasDiskReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_disk_replica_count = value;
        return object;
    }

    auto HasCompleteMemoryReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_complete_memory_replica_count = value;
        return object;
    }

    auto HasCompleteNoFReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_complete_nof_replica_count = value;
        return object;
    }

    auto HasCompleteDfsReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_complete_dfs_replica_count = value;
        return object;
    }

    auto HasCompleteLocalDiskReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_complete_local_disk_replica_count = value;
        return object;
    }

    auto HasCompleteDiskReplicas(size_t value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_complete_disk_replica_count = value;
        return object;
    }

    auto IsOnMemoryNode(std::string value) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_memory_node = std::move(value);
        return object;
    }

    auto HasMemoryNodes(std::initializer_list<std::string> values) const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_memory_nodes.emplace(values.begin(), values.end());
        return object;
    }

    auto HasDistinctMemoryNodes() const
        requires(expectation != ObjectExpectation::NOT_READY &&
                 expectation != ObjectExpectation::MISSING)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expect_distinct_memory_nodes = true;
        return object;
    }

   private:
    template <ObjectExpectation other>
    friend struct ObjectSpec;

    template <ObjectExpectation other>
    ObjectSpec(const ObjectSpec<other>& object) : ObjectSpecData(object) {}
};

ObjectSpec<> Object(std::string key);

struct ObjectsSpecData {
    std::vector<size_t> indices;
    std::vector<std::string> keys;
    uint64_t size{0};
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::string preferred_node;
    std::string group_id;
    bool with_soft_pin{false};
    bool with_hard_pin{false};
    std::optional<std::chrono::system_clock::time_point> lease_timeout_base{};
    std::chrono::nanoseconds lease_timeout_step{1};
    std::optional<std::chrono::system_clock::time_point> soft_pin_timeout{};
};

template <ObjectExpectation expectation = ObjectExpectation::UNSPECIFIED>
struct ObjectsSpec : ObjectsSpecData {
    ObjectsSpec(size_t begin, size_t end)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        indices.reserve(end > begin ? end - begin : 0);
        for (size_t index = begin; index < end; ++index) {
            indices.push_back(index);
        }
    }

    explicit ObjectsSpec(std::vector<std::string> values)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        keys = std::move(values);
    }

    template <typename KeyFactory>
    ObjectsSpec& NamedBy(KeyFactory&& key_factory)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        keys.clear();
        keys.reserve(indices.size());
        for (const size_t index : indices) {
            keys.push_back(std::invoke(key_factory, index));
        }
        return *this;
    }

    ObjectsSpec& Size(uint64_t value)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        size = value;
        return *this;
    }

    ObjectsSpec& By(std::string value)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        actor = std::move(value);
        return *this;
    }

    ObjectsSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    ObjectsSpec& CompleteOn(std::string value)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        preferred_node = std::move(value);
        return *this;
    }

    ObjectsSpec& InGroup(std::string value)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        group_id = std::move(value);
        return *this;
    }

    ObjectsSpec& WithSoftPin()
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        with_soft_pin = true;
        return *this;
    }

    ObjectsSpec& WithHardPin()
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        with_hard_pin = true;
        return *this;
    }

    ObjectsSpec& ExpiredFrom(
        std::chrono::system_clock::time_point value,
        std::chrono::nanoseconds step = std::chrono::nanoseconds(1))
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        lease_timeout_base = value;
        lease_timeout_step = step;
        return *this;
    }

    ObjectsSpec& ExpiresAt(std::chrono::system_clock::time_point value)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        lease_timeout_base = value;
        lease_timeout_step = std::chrono::nanoseconds::zero();
        return *this;
    }

    ObjectsSpec& SoftPinnedUntil(std::chrono::system_clock::time_point value)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        with_soft_pin = true;
        soft_pin_timeout = value;
        return *this;
    }

    auto AreReadable() const
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        return ObjectsSpec<ObjectExpectation::READABLE>(*this);
    }

    auto AreNotReady() const
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        return ObjectsSpec<ObjectExpectation::NOT_READY>(*this);
    }

    auto DoNotExist() const
        requires(expectation == ObjectExpectation::UNSPECIFIED)
    {
        return ObjectsSpec<ObjectExpectation::MISSING>(*this);
    }

   private:
    template <ObjectExpectation other>
    friend struct ObjectsSpec;

    template <ObjectExpectation other>
    ObjectsSpec(const ObjectsSpec<other>& objects) : ObjectsSpecData(objects) {}
};

ObjectsSpec<> Objects(size_t begin, size_t end);
ObjectsSpec<> Objects(std::initializer_list<std::string> keys);

struct ReadableObjectCountSpec {
    ObjectsSpecData objects;
    size_t expected;
};

ReadableObjectCountSpec ReadableCount(ObjectsSpec<> objects, size_t expected);

struct SavedObjectsSpec {
    std::string name;
    bool expected_missing{false};

    SavedObjectsSpec& DoNotExist() {
        expected_missing = true;
        return *this;
    }
};

SavedObjectsSpec SavedObjects(std::string name);

struct KeyExistsSpec {
    std::string key;
    std::string tenant{TenantId::Default().value()};

    KeyExistsSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }
};

KeyExistsSpec KeyExists(std::string key);

struct BatchExistenceSpec {
    std::vector<std::string> keys;
    std::string tenant{TenantId::Default().value()};
    std::vector<bool> expected;

    BatchExistenceSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    BatchExistenceSpec& Returns(std::initializer_list<bool> values) {
        expected.assign(values.begin(), values.end());
        return *this;
    }
};

BatchExistenceSpec BatchExistence(std::initializer_list<std::string> keys);

struct BatchReplicaListsSpec {
    std::vector<std::string> keys;
    std::string tenant{TenantId::Default().value()};
    std::vector<ErrorCode> expected;

    BatchReplicaListsSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    BatchReplicaListsSpec& Returns(std::initializer_list<ErrorCode> values) {
        expected.assign(values.begin(), values.end());
        return *this;
    }
};

BatchReplicaListsSpec BatchReplicaLists(
    std::initializer_list<std::string> keys);

struct NamedTaskSpec {
    std::string name;
    std::optional<TaskType> expected_type{};
    std::optional<TaskStatus> expected_status{};
    std::optional<std::string> expected_actor{};
    std::optional<std::string> expected_message{};

    NamedTaskSpec& HasType(TaskType value) {
        expected_type = value;
        return *this;
    }

    NamedTaskSpec& HasStatus(TaskStatus value) {
        expected_status = value;
        return *this;
    }

    NamedTaskSpec& IsAssignedTo(std::string value) {
        expected_actor = std::move(value);
        return *this;
    }

    NamedTaskSpec& HasMessage(std::string value) {
        expected_message = std::move(value);
        return *this;
    }
};

NamedTaskSpec NamedTask(std::string name);

struct UnknownTaskSpec {
    std::string name;
    ErrorCode expected_error{ErrorCode::TASK_NOT_FOUND};
};

UnknownTaskSpec UnknownTask(std::string name);

struct MatchingKeysSpec {
    std::string pattern;
    std::string tenant{TenantId::Default().value()};
    std::optional<size_t> expected_count{};
    std::vector<std::string> expected_keys;

    MatchingKeysSpec& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    MatchingKeysSpec& HasCount(size_t value) {
        expected_count = value;
        return *this;
    }

    MatchingKeysSpec& HasKeys(std::initializer_list<std::string> values) {
        expected_keys.assign(values.begin(), values.end());
        return *this;
    }
};

MatchingKeysSpec MatchingKeys(std::string pattern);

struct ClientIpsSpec {
    std::vector<std::string> actors;
    std::unordered_map<std::string, std::vector<std::string>> expected;
    std::vector<std::string> omitted;

    ClientIpsSpec& Returns(std::string actor,
                           std::initializer_list<std::string> values) {
        expected.emplace(std::move(actor), std::vector<std::string>(
                                               values.begin(), values.end()));
        return *this;
    }

    ClientIpsSpec& Omits(std::string actor) {
        omitted.push_back(std::move(actor));
        return *this;
    }
};

ClientIpsSpec ClientIps(std::initializer_list<std::string> actors);

struct MemoryNodeStatusSpec {
    std::string node;
    std::optional<SegmentStatus> expected_status{};
    bool expected_missing{false};
    bool query_by_id{false};
    std::chrono::milliseconds eventual_timeout{};

    MemoryNodeStatusSpec& Is(SegmentStatus value) {
        expected_status = value;
        return *this;
    }

    MemoryNodeStatusSpec& DoesNotExist() {
        expected_missing = true;
        return *this;
    }

    MemoryNodeStatusSpec& ById() {
        query_by_id = true;
        return *this;
    }

    MemoryNodeStatusSpec& Eventually(
        std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
        eventual_timeout = timeout;
        return *this;
    }
};

MemoryNodeStatusSpec MemoryNodeStatus(std::string node);

std::string GroupOnDifferentShard(std::string_view key);

class MasterScenario {
   public:
    explicit MasterScenario(std::string name);
    MasterScenario(std::string name, MasterServiceConfig config,
                   std::shared_ptr<HaKvBackend> batch_oplog_backend = nullptr);
    ~MasterScenario();

    MasterScenario(const MasterScenario&) = delete;
    MasterScenario& operator=(const MasterScenario&) = delete;

    MasterScenario& Given(MemoryNodeSpec node);
    MasterScenario& Given(NoFNodeSpec node);
    MasterScenario& Given(TenantSpec tenant);
    MasterScenario& Given(ObjectsSpec<> objects);
    template <PutStartExpectation expectation>
    MasterScenario& When(PutStartAction<expectation> action) {
        return WhenPutStart(std::move(action));
    }
    template <UpsertStartExpectation expectation>
    MasterScenario& When(UpsertStartAction<expectation> action) {
        return WhenUpsertStart(std::move(action));
    }

    MasterScenario& When(PutEndAction action);
    MasterScenario& When(UpsertEndAction action);
    MasterScenario& When(PutRevokeAction action);
    MasterScenario& When(UpsertRevokeAction action);
    MasterScenario& When(BatchUpsertStartAction action);
    MasterScenario& When(BatchUpsertEndAction action);
    MasterScenario& When(BatchRemoveAction action);
    MasterScenario& When(RemoveAction action);
    MasterScenario& When(ClearReplicasAction action);
    MasterScenario& When(CopyStartAction action);
    MasterScenario& When(CopyEndAction action);
    MasterScenario& When(CopyRevokeAction action);
    MasterScenario& When(MoveStartAction action);
    MasterScenario& When(MoveEndAction action);
    MasterScenario& When(MoveRevokeAction action);
    MasterScenario& When(CreateCopyTaskAction action);
    MasterScenario& When(CreateMoveTaskAction action);
    MasterScenario& When(FetchTasksAction action);
    MasterScenario& When(CompleteTaskAction action);
    MasterScenario& When(CompleteUnknownTaskAction action);
    MasterScenario& When(RemoveByRegexAction action);
    MasterScenario& When(UnmountMemoryNodeAction action);
    MasterScenario& When(GracefulUnmountMemoryNodeAction action);
    MasterScenario& When(MountMemorySegmentAction action);
    MasterScenario& When(MountUnmountMemoryCapacitiesAction action);
    MasterScenario& When(RemoveAllAction action);
    MasterScenario& When(ExpireAtAction action);
    MasterScenario& When(MemoryEvictAction action);
    MasterScenario& When(DfsEvictAction action);
    MasterScenario& When(WaitAction action);
    MasterScenario& When(WaitForOpLogFailureAction action);
    MasterScenario& When(PingAction action);
    MasterScenario& When(MountMemoryNodeAction action);
    MasterScenario& When(RacePutStartAction action);
    MasterScenario& When(ConcurrentMountUnmountAction action);
    MasterScenario& When(ConcurrentWriteAndRemoveAllAction action);
    MasterScenario& When(ConcurrentReadAndRemoveAllAction action);
    MasterScenario& When(ConcurrentRemoveAllAction action);
    MasterScenario& When(PutManyAction action);
    MasterScenario& When(MountLocalDiskAction action);
    MasterScenario& When(UnmountLocalDiskAction action);
    MasterScenario& When(ConcurrentMountLocalDisksAction action);
    MasterScenario& When(OffloadHeartbeatAction action);
    MasterScenario& When(NotifyOffloadSuccessAction action);
    MasterScenario& When(ReportSsdCapacityAction action);
    MasterScenario& When(EvictDiskReplicaAction action);

    template <ObjectExpectation expectation>
        requires(expectation != ObjectExpectation::UNSPECIFIED)
    MasterScenario& Then(ObjectSpec<expectation> object) {
        return ThenObject(std::move(object), expectation);
    }
    template <ObjectExpectation expectation>
        requires(expectation != ObjectExpectation::UNSPECIFIED)
    MasterScenario& Then(ObjectsSpec<expectation> objects) {
        return ThenObjects(std::move(objects), expectation);
    }
    MasterScenario& Then(ReadableObjectCountSpec objects);
    MasterScenario& Then(SavedObjectsSpec objects);
    MasterScenario& Then(KeyExistsSpec key_exists);
    MasterScenario& Then(BatchExistenceSpec batch_existence);
    MasterScenario& Then(BatchReplicaListsSpec batch_replica_lists);
    MasterScenario& Then(NamedTaskSpec task);
    MasterScenario& Then(UnknownTaskSpec task);
    MasterScenario& Then(MatchingKeysSpec matching_keys);
    MasterScenario& Then(ClientIpsSpec client_ips);
    MasterScenario& Then(MemoryNodeStatusSpec node_status);

   private:
    using StartResult =
        tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    MasterScenario& WhenPutStart(PutStartActionData action);
    MasterScenario& WhenUpsertStart(UpsertStartActionData action);
    MasterScenario& ThenObject(ObjectSpecData object,
                               ObjectExpectation expectation);
    MasterScenario& ThenObjects(ObjectsSpecData objects,
                                ObjectExpectation expectation);
    bool EnsureService();
    UUID ActorId(std::string_view actor);
    void ValidateActionResult(std::string_view action,
                              const std::optional<ErrorCode>& expected_error,
                              bool succeeded, ErrorCode error);
    void ValidateStartResult(
        std::string_view action, const std::optional<ErrorCode>& expected_error,
        const std::optional<size_t>& expected_replica_count,
        const std::optional<ReplicaStatus>& expected_replica_status,
        const StartResult& result,
        const std::optional<std::vector<std::string>>& expected_memory_nodes =
            std::nullopt);
    void Fail(std::string message) const;

    std::string name_;
    MasterServiceConfig config_;
    std::shared_ptr<HaKvBackend> batch_oplog_backend_;
    bool declarations_frozen_{false};
    uintptr_t next_segment_base_{0x300000000};
    std::vector<MemoryNodeSpec> nodes_;
    std::vector<NoFNodeSpec> nof_nodes_;
    std::vector<TenantSpec> tenants_;
    std::string tenant_policy_path_;
    std::unique_ptr<MasterService> service_;
    std::unordered_map<std::string, UUID> actor_ids_;
    std::unordered_map<std::string, UUID> segment_ids_;
    std::unordered_map<std::string, Segment> segments_;
    std::unordered_map<std::string, NoFSegment> nof_segments_;
    std::unordered_map<std::string, UUID> task_ids_;
    std::unordered_map<std::string, std::vector<Replica::Descriptor>>
        last_start_results_;
    std::unordered_map<std::string, std::vector<std::string>> saved_key_sets_;
};

}  // namespace mooncake::test
