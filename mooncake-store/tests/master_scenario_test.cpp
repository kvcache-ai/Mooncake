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
using ReadableObject = decltype(Object("compile-time").HasReplicas(1));

static_assert(!SupportsExpectReplicas<ErrorExpectedPutStart>);
static_assert(!SupportsExpectStatus<ErrorExpectedPutStart>);
static_assert(!SupportsExpectError<SuccessExpectedPutStart>);
static_assert(!SupportsIsReadable<NotReadyObject>);
static_assert(!SupportsHasReplicas<NotReadyObject>);
static_assert(!SupportsIsNotReady<ReadableObject>);
static_assert(!SupportsThen<UnspecifiedObject>);
static_assert(SupportsThen<NotReadyObject>);
static_assert(SupportsThen<ReadableObject>);

}  // namespace

TEST(MasterServiceTest, PutStartEndFlow) {
    MasterScenario("put start/end flow")
        .Given(MemoryNode("memory"))
        .When(PutStart("test_key", 1_KB)
                  .By("writer")
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .Then(Object("test_key").IsNotReady())
        .When(Remove("test_key").ExpectError(ErrorCode::REPLICA_IS_NOT_READY))
        .When(PutEnd("test_key")
                  .By("other-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(PutRevoke("test_key")
                  .By("other-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(PutEnd("test_key").By("writer"))
        .Then(Object("test_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .HasCompleteReplicas(1));
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

TEST(MasterScenarioContractTest, ReportsUnreadableObject) {
    EXPECT_NONFATAL_FAILURE(
        MasterScenario("unreadable object")
            .Given(MemoryNode("memory"))
            .Then(Object("missing").IsReadable()),
        "Object(missing) is not readable: OBJECT_NOT_FOUND");
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

}  // namespace mooncake::test
