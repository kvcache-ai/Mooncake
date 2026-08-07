#pragma once

#include <cstddef>
#include <cstdint>
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
    std::optional<ErrorCode> expected_error{};
    std::optional<size_t> expected_replica_count{};
    std::optional<ReplicaStatus> expected_replica_status{};

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
    std::optional<ErrorCode> expected_error{};
    std::optional<size_t> expected_replica_count{};
    std::optional<ReplicaStatus> expected_replica_status{};

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
    std::optional<ErrorCode> expected_error{};

    PutEndAction& By(std::string value) {
        actor = std::move(value);
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
    std::optional<ErrorCode> expected_error{};

    UpsertEndAction& By(std::string value) {
        actor = std::move(value);
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
    std::optional<ErrorCode> expected_error{};

    PutRevokeAction& By(std::string value) {
        actor = std::move(value);
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
    std::optional<ErrorCode> expected_error{};

    UpsertRevokeAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    UpsertRevokeAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

UpsertRevokeAction UpsertRevoke(std::string key);

struct RemoveAction {
    std::string key;
    std::optional<ErrorCode> expected_error{};

    RemoveAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

RemoveAction Remove(std::string key);

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
    std::optional<size_t> expected_replica_count{};
    std::optional<size_t> expected_complete_replica_count{};

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

   private:
    template <ObjectExpectation other>
    friend struct ObjectSpec;

    template <ObjectExpectation other>
    ObjectSpec(const ObjectSpec<other>& object) : ObjectSpecData(object) {}
};

ObjectSpec<> Object(std::string key);

class MasterScenario {
   public:
    explicit MasterScenario(std::string name);
    ~MasterScenario();

    MasterScenario(const MasterScenario&) = delete;
    MasterScenario& operator=(const MasterScenario&) = delete;

    MasterScenario& Given(MemoryNodeSpec node);
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
    MasterScenario& When(RemoveAction action);

    template <ObjectExpectation expectation>
        requires(expectation != ObjectExpectation::UNSPECIFIED)
    MasterScenario& Then(ObjectSpec<expectation> object) {
        return ThenObject(std::move(object), expectation);
    }

   private:
    using StartResult =
        tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    MasterScenario& WhenPutStart(PutStartActionData action);
    MasterScenario& WhenUpsertStart(UpsertStartActionData action);
    MasterScenario& ThenObject(ObjectSpecData object,
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
        const StartResult& result);
    void Fail(std::string message) const;

    std::string name_;
    bool declarations_frozen_{false};
    uintptr_t next_segment_base_{0x300000000};
    std::vector<MemoryNodeSpec> nodes_;
    std::unique_ptr<MasterService> service_;
    std::unordered_map<std::string, UUID> actor_ids_;
};

}  // namespace mooncake::test
