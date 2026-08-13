#include "master_scenario.h"

#include <gtest/gtest-spi.h>
#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

template <typename T>
concept SupportsExpectError =
    requires(T value) { value.ExpectError(ErrorCode::INTERNAL_ERROR); };

template <typename T>
concept SupportsExpectReplicas = requires(T value) { value.ExpectReplicas(1); };

template <typename T>
concept SupportsExpectStatus =
    requires(T value) { value.ExpectStatus(ReplicaStatus::COMPLETE); };

template <typename T>
concept SupportsIsReadable = requires(T value) { value.IsReadable(); };

template <typename T>
concept SupportsIsNotReady = requires(T value) { value.IsNotReady(); };

template <typename T>
concept SupportsDoesNotExist = requires(T value) { value.DoesNotExist(); };

template <typename T>
concept SupportsHasReplicas = requires(T value) { value.HasReplicas(1); };

template <typename T>
concept SupportsHasCompleteReplicas =
    requires(T value) { value.HasCompleteReplicas(1); };

template <typename T>
concept SupportsExpectedErrorMutation =
    requires(T value) { value.expected_error = ErrorCode::INTERNAL_ERROR; };

template <typename T>
concept SupportsExpectedReplicaCountMutation =
    requires(T value) { value.expected_replica_count = 1; };

template <typename T>
concept SupportsExpectedCompleteReplicaCountMutation =
    requires(T value) { value.expected_complete_replica_count = 1; };

template <typename T>
concept SupportsThen =
    requires(MasterScenario& scenario, T value) { scenario.Then(value); };

using ErrorExpectedPutStart =
    decltype(PutStart("compile-time", 1_KB)
                 .ExpectError(ErrorCode::INTERNAL_ERROR));
using SuccessExpectedPutStart =
    decltype(PutStart("compile-time", 1_KB).ExpectReplicas(1));
using ErrorExpectedUpsertStart =
    decltype(UpsertStart("compile-time", 1_KB)
                 .ExpectError(ErrorCode::INTERNAL_ERROR));
using SuccessExpectedUpsertStart =
    decltype(UpsertStart("compile-time", 1_KB).ExpectReplicas(1));
using UnspecifiedObject = decltype(Object("compile-time"));
using NotReadyObject = decltype(Object("compile-time").IsNotReady());
using MissingObject = decltype(Object("compile-time").DoesNotExist());
using ReadableObject = decltype(Object("compile-time").HasReplicas(1));
using UnspecifiedObjects = decltype(Objects(0, 1));
using MissingObjects = decltype(Objects(0, 1).DoNotExist());
using ReadableObjects = decltype(Objects(0, 1).AreReadable());

static_assert(!SupportsExpectReplicas<ErrorExpectedPutStart>);
static_assert(!SupportsExpectStatus<ErrorExpectedPutStart>);
static_assert(!SupportsExpectError<SuccessExpectedPutStart>);
static_assert(!SupportsExpectedReplicaCountMutation<ErrorExpectedPutStart>);
static_assert(!SupportsExpectedErrorMutation<SuccessExpectedPutStart>);
static_assert(!SupportsExpectReplicas<ErrorExpectedUpsertStart>);
static_assert(!SupportsExpectStatus<ErrorExpectedUpsertStart>);
static_assert(!SupportsExpectError<SuccessExpectedUpsertStart>);
static_assert(!SupportsExpectedReplicaCountMutation<ErrorExpectedUpsertStart>);
static_assert(!SupportsExpectedErrorMutation<SuccessExpectedUpsertStart>);
static_assert(!SupportsIsReadable<NotReadyObject>);
static_assert(!SupportsHasReplicas<NotReadyObject>);
static_assert(!SupportsIsNotReady<ReadableObject>);
static_assert(!SupportsDoesNotExist<ReadableObject>);
static_assert(!SupportsDoesNotExist<NotReadyObject>);
static_assert(!SupportsIsReadable<MissingObject>);
static_assert(!SupportsIsNotReady<MissingObject>);
static_assert(!SupportsHasReplicas<MissingObject>);
static_assert(!SupportsHasCompleteReplicas<MissingObject>);
static_assert(!SupportsExpectedReplicaCountMutation<MissingObject>);
static_assert(!SupportsExpectedCompleteReplicaCountMutation<MissingObject>);
static_assert(!SupportsThen<UnspecifiedObject>);
static_assert(SupportsThen<NotReadyObject>);
static_assert(SupportsThen<MissingObject>);
static_assert(SupportsThen<ReadableObject>);
static_assert(!SupportsThen<UnspecifiedObjects>);
static_assert(SupportsThen<MissingObjects>);
static_assert(SupportsThen<ReadableObjects>);

}  // namespace

TEST(MasterScenarioContractTest, HonorsRequestedPutStartReplicaCount) {
    MasterScenario("requested put start replica count")
        .Given(MemoryNode("memory-1"))
        .Given(MemoryNode("memory-2"))
        .When(PutStart("key", 1_KB).Replicas(2).ExpectReplicas(2));
}

TEST(MasterScenarioContractTest, HonorsRequestedUpsertStartReplicaCount) {
    MasterScenario("requested upsert start replica count")
        .Given(MemoryNode("memory-1"))
        .Given(MemoryNode("memory-2"))
        .When(UpsertStart("key", 1_KB).Replicas(2).ExpectReplicas(2));
}

TEST(MasterScenarioContractTest, ReportsUnexpectedActionError) {
    EXPECT_NONFATAL_FAILURE(MasterScenario("unexpected action error")
                                .Given(MemoryNode("memory"))
                                .When(PutEnd("missing")),
                            "PutEnd(missing) failed: OBJECT_NOT_FOUND");
}

TEST(MasterScenarioContractTest, ReportsUnexpectedActionSuccess) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("unexpected action success")
            .Given(MemoryNode("memory"))
            .When(PutStart("key", 1_KB)
                      .ExpectError(ErrorCode::OBJECT_ALREADY_EXISTS)),
        "PutStart(key) succeeded; expected OBJECT_ALREADY_EXISTS");
}

TEST(MasterScenarioContractTest, ReportsWrongErrorCode) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("wrong error code")
            .Given(MemoryNode("memory"))
            .When(PutEnd("missing").ExpectError(ErrorCode::ILLEGAL_CLIENT)),
        "PutEnd(missing) failed with OBJECT_NOT_FOUND; expected "
        "ILLEGAL_CLIENT");
}

TEST(MasterScenarioContractTest, ReportsPutStartReplicaCountMismatch) {
    EXPECT_NONFATAL_FAILURE(MasterScenario("put start replica count mismatch")
                                .Given(MemoryNode("memory"))
                                .When(PutStart("key", 1_KB).ExpectReplicas(2)),
                            "PutStart(key) returned 1 replicas; expected 2");
}

TEST(MasterScenarioContractTest, ReportsPutStartReplicaStatusMismatch) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("put start replica status mismatch")
            .Given(MemoryNode("memory"))
            .When(PutStart("key", 1_KB).ExpectStatus(ReplicaStatus::COMPLETE)),
        "PutStart(key) replica status mismatch");
}

TEST(MasterScenarioContractTest, ReportsUpsertStartReplicaCountMismatch) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("upsert start replica count mismatch")
            .Given(MemoryNode("memory"))
            .When(UpsertStart("key", 1_KB).ExpectReplicas(2)),
        "UpsertStart(key) returned 1 replicas; expected 2");
}

TEST(MasterScenarioContractTest, ReportsUpsertStartReplicaStatusMismatch) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("upsert start replica status mismatch")
            .Given(MemoryNode("memory"))
            .When(
                UpsertStart("key", 1_KB).ExpectStatus(ReplicaStatus::COMPLETE)),
        "UpsertStart(key) replica status mismatch");
}

TEST(MasterScenarioContractTest, ReportsUnexpectedUpsertStartError) {
    EXPECT_NONFATAL_FAILURE(MasterScenario("unexpected upsert start error")
                                .Given(MemoryNode("memory").Capacity(1_KB))
                                .When(UpsertStart("key", 2_KB)),
                            "UpsertStart(key) failed: NO_AVAILABLE_HANDLE");
}

TEST(MasterScenarioContractTest, ReportsUnexpectedUpsertStartSuccess) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("unexpected upsert start success")
            .Given(MemoryNode("memory"))
            .When(UpsertStart("key", 1_KB)
                      .ExpectError(ErrorCode::OBJECT_ALREADY_EXISTS)),
        "UpsertStart(key) succeeded; expected OBJECT_ALREADY_EXISTS");
}

TEST(MasterScenarioContractTest, ReportsWrongUpsertEndErrorCode) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("wrong upsert end error code")
            .Given(MemoryNode("memory"))
            .When(UpsertEnd("missing").ExpectError(ErrorCode::ILLEGAL_CLIENT)),
        "UpsertEnd(missing) failed with OBJECT_NOT_FOUND; expected "
        "ILLEGAL_CLIENT");
}

TEST(MasterScenarioContractTest, ReportsWrongUpsertRevokeErrorCode) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("wrong upsert revoke error code")
            .Given(MemoryNode("memory"))
            .When(
                UpsertRevoke("missing").ExpectError(ErrorCode::ILLEGAL_CLIENT)),
        "UpsertRevoke(missing) failed with OBJECT_NOT_FOUND; expected "
        "ILLEGAL_CLIENT");
}

TEST(MasterScenarioContractTest, ReportsUnreadableObject) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("unreadable object")
            .Given(MemoryNode("memory"))
            .Then(Object("missing").IsReadable()),
        "Object(missing) is not readable: OBJECT_NOT_FOUND");
}

TEST(MasterScenarioContractTest, ReportsExistingObjectWhenAbsenceExpected) {
    EXPECT_NONFATAL_FAILURE(MasterScenario("existing object expected absent")
                                .Given(MemoryNode("memory"))
                                .When(PutStart("key", 1_KB))
                                .When(PutEnd("key"))
                                .Then(Object("key").DoesNotExist()),
                            "Object(key) exists; expected it not to exist");
}

TEST(MasterScenarioContractTest, ReportsNotReadyObjectWhenAbsenceExpected) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("not-ready object expected absent")
            .Given(MemoryNode("memory"))
            .When(PutStart("key", 1_KB))
            .Then(Object("key").DoesNotExist()),
        "Object(key) lookup failed with REPLICA_IS_NOT_READY; expected "
        "OBJECT_NOT_FOUND");
}

TEST(MasterScenarioContractTest, ReportsKeyCountMismatch) {
    EXPECT_NONFATAL_FAILURE(MasterScenario("key count mismatch")
                                .Given(MemoryNode("memory"))
                                .When(PutStart("key", 1_KB))
                                .Then(KeyCount(0)),
                            "KeyCount is 1; expected 0");
}

TEST(MasterScenarioContractTest, ReportsObjectReplicaCountMismatch) {
    EXPECT_NONFATAL_FAILURE(MasterScenario("object replica count mismatch")
                                .Given(MemoryNode("memory"))
                                .When(PutStart("key", 1_KB))
                                .When(PutEnd("key"))
                                .Then(Object("key").HasReplicas(2)),
                            "Object(key) has 1 replicas; expected 2");
}

TEST(MasterScenarioContractTest, ReportsCompleteReplicaCountMismatch) {
    EXPECT_NONFATAL_FAILURE(MasterScenario("complete replica count mismatch")
                                .Given(MemoryNode("memory"))
                                .When(PutStart("key", 1_KB))
                                .When(PutEnd("key"))
                                .Then(Object("key").HasCompleteReplicas(0)),
                            "Object(key) has 1 complete replicas; expected 0");
}

TEST(MasterScenarioContractTest, CreatesAndChecksObjectCollections) {
    MasterScenario("object collections")
        .Given(MemoryNode("memory"))
        .Given(Objects(2, 5)
                   .NamedBy([](size_t index) {
                       return "collection-" + std::to_string(index);
                   })
                   .Size(1_KB)
                   .CompleteOn("memory"))
        .Then(Objects(2, 5)
                  .NamedBy([](size_t index) {
                      return "collection-" + std::to_string(index);
                  })
                  .AreReadable())
        .Then(KeyCount(3));
}

TEST(MasterScenarioContractTest, CollectionFailureIdentifiesObjectKey) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("collection failure")
            .Given(MemoryNode("memory"))
            .Given(Objects({"present"}).Size(1_KB).CompleteOn("memory"))
            .Then(Objects({"present", "missing"}).AreReadable()),
        "Object(missing) is not readable: OBJECT_NOT_FOUND");
}

}  // namespace mooncake::test
