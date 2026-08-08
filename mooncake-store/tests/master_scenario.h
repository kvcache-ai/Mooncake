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

constexpr uint64_t operator""_KB(unsigned long long value) {
    return value * 1024;
}

struct MemoryNodeSpec {
    std::string name;
    uint64_t capacity{16 * 1024 * 1024};

    MemoryNodeSpec& Capacity(uint64_t value) {
        capacity = value;
        return *this;
    }
};

MemoryNodeSpec MemoryNode(std::string name);

enum class PutStartExpectation {
    UNSPECIFIED,
    SUCCESS,
    ERROR,
};

struct PutStartActionData {
    std::string key;
    uint64_t size;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::string preferred_node;
    std::string group_id;
    bool with_soft_pin{false};
    bool with_hard_pin{false};
    std::optional<ErrorCode> expected_error{};
    std::optional<size_t> expected_replica_count{};
    std::optional<ReplicaStatus> expected_replica_status{};
};

template <PutStartExpectation expectation = PutStartExpectation::UNSPECIFIED>
struct PutStartAction : PutStartActionData {
    PutStartAction(std::string key, uint64_t size)
        requires(expectation == PutStartExpectation::UNSPECIFIED)
        : PutStartActionData{.key = std::move(key),
                             .size = size,
                             .actor = "default",
                             .tenant = TenantId::Default().value(),
                             .preferred_node = {},
                             .group_id = {},
                             .with_soft_pin = false,
                             .with_hard_pin = false,
                             .expected_error = std::nullopt,
                             .expected_replica_count = std::nullopt,
                             .expected_replica_status = std::nullopt} {}

    PutStartAction& By(std::string value) {
        actor = std::move(value);
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

    PutStartAction& InGroup(std::string value) {
        group_id = std::move(value);
        return *this;
    }

    PutStartAction& WithSoftPin() {
        with_soft_pin = true;
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

   private:
    template <PutStartExpectation other>
    friend struct PutStartAction;

    template <PutStartExpectation other>
    PutStartAction(const PutStartAction<other>& action)
        : PutStartActionData(action) {}
};

PutStartAction<> PutStart(std::string key, uint64_t size);

struct PutEndAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};

    PutEndAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    PutEndAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    PutEndAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

PutEndAction PutEnd(std::string key);

struct PutRevokeAction {
    std::string key;
    std::string actor{"default"};
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};

    PutRevokeAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    PutRevokeAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }

    PutRevokeAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

PutRevokeAction PutRevoke(std::string key);

struct RemoveAction {
    std::string key;
    std::string tenant{TenantId::Default().value()};
    std::optional<ErrorCode> expected_error{};

    RemoveAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }

    RemoveAction& ForTenant(std::string value) {
        tenant = std::move(value);
        return *this;
    }
};

RemoveAction Remove(std::string key);

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

enum class ObjectExpectation {
    UNSPECIFIED,
    READABLE,
    NOT_READY,
    MISSING,
};

struct ObjectSpecData {
    std::string key;
    std::string tenant{TenantId::Default().value()};
    std::optional<size_t> expected_replica_count{};
    std::optional<size_t> expected_complete_replica_count{};
};

template <ObjectExpectation expectation = ObjectExpectation::UNSPECIFIED>
struct ObjectSpec : ObjectSpecData {
    explicit ObjectSpec(std::string key)
        requires(expectation == ObjectExpectation::UNSPECIFIED)
        : ObjectSpecData{.key = std::move(key)} {}

    auto IsReadable() const
        requires(expectation == ObjectExpectation::UNSPECIFIED ||
                 expectation == ObjectExpectation::READABLE)
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
        requires(expectation == ObjectExpectation::UNSPECIFIED ||
                 expectation == ObjectExpectation::READABLE)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_replica_count = value;
        return object;
    }

    auto HasCompleteReplicas(size_t value) const
        requires(expectation == ObjectExpectation::UNSPECIFIED ||
                 expectation == ObjectExpectation::READABLE)
    {
        ObjectSpec<ObjectExpectation::READABLE> object(*this);
        object.expected_complete_replica_count = value;
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

struct KeyCountSpec {
    size_t value;
};

KeyCountSpec KeyCount(size_t value);

struct TenantQuotaSpec {
    std::string tenant;
    std::optional<uint64_t> used_bytes{};
    std::optional<uint64_t> reserved_bytes{};
    std::chrono::milliseconds eventual_timeout{};

    TenantQuotaSpec& Uses(uint64_t value) {
        used_bytes = value;
        return *this;
    }

    TenantQuotaSpec& Reserves(uint64_t value) {
        reserved_bytes = value;
        return *this;
    }

    TenantQuotaSpec& Eventually(
        std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
        eventual_timeout = timeout;
        return *this;
    }
};

TenantQuotaSpec TenantQuota(std::string tenant);

struct OpLogUnavailableSpec {
    std::chrono::milliseconds timeout{std::chrono::seconds(1)};
};

OpLogUnavailableSpec OpLogUnavailable();

class MasterScenario {
   public:
    explicit MasterScenario(std::string name);
    MasterScenario(std::string name, MasterServiceConfig config,
                   std::shared_ptr<HaKvBackend> batch_oplog_backend = nullptr);
    ~MasterScenario();

    MasterScenario(const MasterScenario&) = delete;
    MasterScenario& operator=(const MasterScenario&) = delete;

    MasterScenario& Given(MemoryNodeSpec node);
    MasterScenario& Given(ObjectsSpec<> objects);
    template <PutStartExpectation expectation>
    MasterScenario& When(PutStartAction<expectation> action) {
        return WhenPutStart(std::move(action));
    }

    MasterScenario& When(PutEndAction action);
    MasterScenario& When(PutRevokeAction action);
    MasterScenario& When(RemoveAction action);
    MasterScenario& When(ExpireAtAction action);
    MasterScenario& When(MemoryEvictAction action);

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
    MasterScenario& Then(KeyCountSpec key_count);
    MasterScenario& Then(TenantQuotaSpec tenant_quota);
    MasterScenario& Then(OpLogUnavailableSpec oplog);

   private:
    MasterScenario& WhenPutStart(PutStartActionData action);
    MasterScenario& ThenObject(ObjectSpecData object,
                               ObjectExpectation expectation);
    MasterScenario& ThenObjects(ObjectsSpecData objects,
                                ObjectExpectation expectation);
    bool EnsureService();
    UUID ActorId(std::string_view actor);
    void ValidateActionResult(std::string_view action,
                              const std::optional<ErrorCode>& expected_error,
                              bool succeeded, ErrorCode error);
    void Fail(std::string message) const;

    std::string name_;
    MasterServiceConfig config_;
    std::shared_ptr<HaKvBackend> batch_oplog_backend_;
    bool declarations_frozen_{false};
    uintptr_t next_segment_base_{0x300000000};
    std::vector<MemoryNodeSpec> nodes_;
    std::unique_ptr<MasterService> service_;
    std::unordered_map<std::string, UUID> actor_ids_;
};

}  // namespace mooncake::test
