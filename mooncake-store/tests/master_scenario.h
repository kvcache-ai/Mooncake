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

struct PutStartAction {
    std::string key;
    uint64_t size;
    std::string actor{"default"};
    std::optional<ErrorCode> expected_error{};
    std::optional<size_t> expected_replica_count{};
    std::optional<ReplicaStatus> expected_replica_status{};

    PutStartAction& By(std::string value) {
        actor = std::move(value);
        return *this;
    }

    PutStartAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }

    PutStartAction& ExpectReplicas(size_t value) {
        expected_replica_count = value;
        return *this;
    }

    PutStartAction& ExpectStatus(ReplicaStatus value) {
        expected_replica_status = value;
        return *this;
    }
};

PutStartAction PutStart(std::string key, uint64_t size);

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

struct RemoveAction {
    std::string key;
    std::optional<ErrorCode> expected_error{};

    RemoveAction& ExpectError(ErrorCode value) {
        expected_error = value;
        return *this;
    }
};

RemoveAction Remove(std::string key);

struct ObjectSpec {
    enum class Readability {
        UNSPECIFIED,
        READABLE,
        NOT_READY,
    };

    std::string key;
    Readability readability{Readability::UNSPECIFIED};
    std::optional<size_t> expected_replica_count{};
    std::optional<size_t> expected_complete_replica_count{};

    ObjectSpec& IsReadable() {
        readability = Readability::READABLE;
        return *this;
    }

    ObjectSpec& IsNotReady() {
        readability = Readability::NOT_READY;
        return *this;
    }

    ObjectSpec& HasReplicas(size_t value) {
        expected_replica_count = value;
        return *this;
    }

    ObjectSpec& HasCompleteReplicas(size_t value) {
        expected_complete_replica_count = value;
        return *this;
    }
};

ObjectSpec Object(std::string key);

class MasterScenario {
   public:
    explicit MasterScenario(std::string name);
    ~MasterScenario();

    MasterScenario(const MasterScenario&) = delete;
    MasterScenario& operator=(const MasterScenario&) = delete;

    MasterScenario& Given(MemoryNodeSpec node);
    MasterScenario& When(PutStartAction action);
    MasterScenario& When(PutEndAction action);
    MasterScenario& When(PutRevokeAction action);
    MasterScenario& When(RemoveAction action);
    MasterScenario& Then(ObjectSpec object);

   private:
    bool EnsureService();
    UUID ActorId(std::string_view actor);
    void ValidateActionResult(std::string_view action,
                              const std::optional<ErrorCode>& expected_error,
                              bool succeeded, ErrorCode error);
    void Fail(std::string message) const;

    std::string name_;
    bool declarations_frozen_{false};
    uintptr_t next_segment_base_{0x300000000};
    std::vector<MemoryNodeSpec> nodes_;
    std::unique_ptr<MasterService> service_;
    std::unordered_map<std::string, UUID> actor_ids_;
};

}  // namespace mooncake::test
