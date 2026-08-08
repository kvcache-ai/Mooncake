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
concept SupportsThen =
    requires(MasterScenario& scenario, T value) { scenario.Then(value); };

using ErrorExpectedPutStart =
    decltype(PutStart("compile-time", 1_KB)
                 .ExpectError(ErrorCode::INTERNAL_ERROR));
using SuccessExpectedPutStart =
    decltype(PutStart("compile-time", 1_KB).ExpectReplicas(1));
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
static_assert(!SupportsIsReadable<NotReadyObject>);
static_assert(!SupportsHasReplicas<NotReadyObject>);
static_assert(!SupportsIsNotReady<ReadableObject>);
static_assert(!SupportsIsReadable<MissingObject>);
static_assert(!SupportsHasReplicas<MissingObject>);
static_assert(!SupportsDoesNotExist<ReadableObject>);
static_assert(!SupportsThen<UnspecifiedObject>);
static_assert(SupportsThen<NotReadyObject>);
static_assert(SupportsThen<MissingObject>);
static_assert(SupportsThen<ReadableObject>);
static_assert(!SupportsThen<UnspecifiedObjects>);
static_assert(SupportsThen<MissingObjects>);
static_assert(SupportsThen<ReadableObjects>);

}  // namespace

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

TEST(MasterScenarioContractTest, ReportsUnreadableObject) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("unreadable object")
            .Given(MemoryNode("memory"))
            .Then(Object("missing").IsReadable()),
        "Object(missing) is not readable: OBJECT_NOT_FOUND");
}

TEST(MasterScenarioContractTest, ReportsObjectExpectedMissing) {
    EXPECT_NONFATAL_FAILURE(MasterScenario("object expected missing")
                                .Given(MemoryNode("memory"))
                                .When(PutStart("key", 1_KB))
                                .When(PutEnd("key"))
                                .Then(Object("key").DoesNotExist()),
                            "Object(key) was expected to be missing");
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
