#include "master_scenario.h"

namespace mooncake::test {

TEST(MasterScenarioCopyMoveTest, CopyStart) {
    MasterScenario("copy start contract")
        .Configured(ServiceConfig().DefaultLeaseTtl(50))
        .Given(MemoryNode("segment_1"))
        .Given(MemoryNode("segment_2"))
        .Given(MemoryNode("segment_3"))
        .Given(MemoryNode("segment_4"))
        .When(CopyStart("missing")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_2"})
                  .ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(PutStart("key", 1_KB).By("writer").PreferredSegment("segment_1"))
        .When(CopyStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_2", "segment_3"})
                  .ExpectError(ErrorCode::REPLICA_NOT_FOUND))
        .When(PutEnd("key").By("writer"))
        .When(CopyStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_2", "segment_3"})
                  .ExpectSource("segment_1")
                  .ExpectTargets(2))
        .When(Remove("key").ExpectError(ErrorCode::REPLICA_IS_NOT_READY))
        .When(CopyStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_4"})
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK))
        .When(CopyEnd("key").By("writer"))
        .Then(Object("key").HasReplicasOn(
            {"segment_1", "segment_2", "segment_3"}))
        .When(CopyStart("key")
                  .By("writer")
                  .From("missing")
                  .To({"segment_4"})
                  .ExpectError(ErrorCode::REPLICA_NOT_FOUND))
        .When(CopyStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_4", "missing"})
                  .ExpectError(ErrorCode::SEGMENT_NOT_FOUND))
        .When(CopyStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_3", "segment_4"})
                  .ExpectSource("segment_1")
                  .ExpectTargets(1))
        .When(CopyEnd("key").By("writer"))
        .Then(Object("key").HasReplicasOn(
            {"segment_1", "segment_2", "segment_3", "segment_4"}))
        .When(CopyStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_4"})
                  .ExpectTargets(0))
        .When(WaitFor(std::chrono::milliseconds(100)))
        .When(Remove("key").ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK))
        .When(CopyEnd("key").By("writer"))
        .When(WaitFor(std::chrono::milliseconds(100)))
        .When(Remove("key"))
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioCopyMoveTest, CopyEnd) {
    MasterScenario("copy end contract")
        .Given(MemoryNode("segment_1"))
        .Given(MemoryNode("segment_2"))
        .Given(MemoryNode("segment_3"))
        .When(CopyEnd("missing").By("writer").ExpectError(
            ErrorCode::OBJECT_NOT_FOUND))
        .When(Put("key", 1_KB).By("writer").PreferredSegment("segment_1"))
        .When(CopyEnd("key").By("writer").ExpectError(
            ErrorCode::OBJECT_NO_REPLICATION_TASK))
        .When(CopyStart("key").By("writer").From("segment_1").To({"segment_2"}))
        .When(CopyEnd("key").By("other").ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(
            MoveEnd("key").By("writer").ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CopyEnd("key").By("writer"))
        .Then(Object("key").HasReplicasOn({"segment_1", "segment_2"}))
        .When(CopyStart("key").By("writer").From("segment_1").To({"segment_3"}))
        .When(UnmountNode("segment_1"))
        .When(
            CopyEnd("key").By("writer").ExpectError(ErrorCode::REPLICA_IS_GONE))
        .Then(Object("key").HasReplicasOn({"segment_2"}))
        .When(CopyStart("key").By("writer").From("segment_2").To({"segment_3"}))
        .When(UnmountNode("segment_3"))
        .When(
            CopyEnd("key").By("writer").ExpectError(ErrorCode::REPLICA_IS_GONE))
        .Then(Object("key").HasReplicasOn({"segment_2"}));
}

TEST(MasterScenarioCopyMoveTest, CopyRevoke) {
    MasterScenario("copy revoke contract")
        .Given(MemoryNode("segment_1"))
        .Given(MemoryNode("segment_2"))
        .When(CopyRevoke("missing").By("writer").ExpectError(
            ErrorCode::OBJECT_NOT_FOUND))
        .When(Put("key", 1_KB).By("writer").PreferredSegment("segment_1"))
        .When(CopyRevoke("key").By("writer").ExpectError(
            ErrorCode::OBJECT_NO_REPLICATION_TASK))
        .When(CopyStart("key").By("writer").From("segment_1").To({"segment_2"}))
        .When(CopyRevoke("key").By("other").ExpectError(
            ErrorCode::ILLEGAL_CLIENT))
        .When(MoveRevoke("key").By("writer").ExpectError(
            ErrorCode::INVALID_PARAMS))
        .When(CopyRevoke("key").By("writer"))
        .Then(Object("key").HasReplicasOn({"segment_1"}))
        .When(CopyStart("key").By("writer").From("segment_1").To({"segment_2"}))
        .When(UnmountNode("segment_1"))
        .When(CopyRevoke("key").By("writer"))
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioCopyMoveTest, MoveStart) {
    MasterScenario("move start contract")
        .Configured(ServiceConfig().DefaultLeaseTtl(50))
        .Given(MemoryNode("segment_1"))
        .Given(MemoryNode("segment_2"))
        .Given(MemoryNode("segment_3"))
        .When(MoveStart("missing")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_2"})
                  .ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(PutStart("key", 1_KB).By("writer").PreferredSegment("segment_1"))
        .When(MoveStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_2"})
                  .ExpectError(ErrorCode::REPLICA_NOT_FOUND))
        .When(PutEnd("key").By("writer"))
        .When(CopyStart("key").By("writer").From("segment_1").To({"segment_3"}))
        .When(CopyEnd("key").By("writer"))
        .When(MoveStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_1"})
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(MoveStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_2"})
                  .ExpectSource("segment_1")
                  .ExpectTargets(1))
        .When(Remove("key").ExpectError(ErrorCode::REPLICA_IS_NOT_READY))
        .When(MoveStart("key")
                  .By("writer")
                  .From("segment_1")
                  .To({"segment_3"})
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK))
        .When(MoveEnd("key").By("writer"))
        .Then(Object("key").HasReplicasOn({"segment_2", "segment_3"}))
        .When(MoveStart("key")
                  .By("writer")
                  .From("missing")
                  .To({"segment_1"})
                  .ExpectError(ErrorCode::REPLICA_NOT_FOUND))
        .When(MoveStart("key")
                  .By("writer")
                  .From("segment_2")
                  .To({"missing"})
                  .ExpectError(ErrorCode::SEGMENT_NOT_FOUND))
        .When(MoveStart("key")
                  .By("writer")
                  .From("segment_2")
                  .To({"segment_3"})
                  .ExpectSource("segment_2")
                  .ExpectTargets(0))
        .When(WaitFor(std::chrono::milliseconds(100)))
        .When(Remove("key").ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK))
        .When(MoveEnd("key").By("writer"))
        .Then(Object("key").HasReplicasOn({"segment_3"}))
        .When(WaitFor(std::chrono::milliseconds(100)))
        .When(Remove("key"));
}

TEST(MasterScenarioCopyMoveTest, MoveEnd) {
    MasterScenario("move end contract")
        .Given(MemoryNode("segment_1"))
        .Given(MemoryNode("segment_2"))
        .When(MoveEnd("missing").By("writer").ExpectError(
            ErrorCode::OBJECT_NOT_FOUND))
        .When(Put("key", 1_KB).By("writer").PreferredSegment("segment_1"))
        .When(MoveEnd("key").By("writer").ExpectError(
            ErrorCode::OBJECT_NO_REPLICATION_TASK))
        .When(MoveStart("key").By("writer").From("segment_1").To({"segment_2"}))
        .When(MoveEnd("key").By("other").ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(
            CopyEnd("key").By("writer").ExpectError(ErrorCode::INVALID_PARAMS))
        .When(MoveEnd("key").By("writer"))
        .Then(Object("key").HasReplicasOn({"segment_2"}))
        .When(MoveStart("key").By("writer").From("segment_2").To({"segment_1"}))
        .When(UnmountNode("segment_2"))
        .When(MoveEnd("key").By("writer").ExpectError(
            ErrorCode::REPLICA_IS_GONE));
}

TEST(MasterScenarioCopyMoveTest, MoveRevoke) {
    MasterScenario("move revoke contract")
        .Given(MemoryNode("segment_1"))
        .Given(MemoryNode("segment_2"))
        .When(MoveRevoke("missing").By("writer").ExpectError(
            ErrorCode::OBJECT_NOT_FOUND))
        .When(Put("key", 1_KB).By("writer").PreferredSegment("segment_1"))
        .When(MoveRevoke("key").By("writer").ExpectError(
            ErrorCode::OBJECT_NO_REPLICATION_TASK))
        .When(MoveStart("key").By("writer").From("segment_1").To({"segment_2"}))
        .When(MoveRevoke("key").By("other").ExpectError(
            ErrorCode::ILLEGAL_CLIENT))
        .When(CopyRevoke("key").By("writer").ExpectError(
            ErrorCode::INVALID_PARAMS))
        .When(MoveRevoke("key").By("writer"))
        .Then(Object("key").HasReplicasOn({"segment_1"}))
        .When(MoveStart("key").By("writer").From("segment_1").To({"segment_2"}))
        .When(UnmountNode("segment_1"))
        .When(MoveRevoke("key").By("writer"))
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioCopyMoveTest, SingleSliceMultiReplicaFlow) {
    MasterScenario("single slice multi replica lifecycle")
        .Configured(ServiceConfig().DefaultLeaseTtl(50))
        .Given(MemoryNode("segment_0").Capacity(64_MB))
        .Given(MemoryNode("segment_1").Capacity(64_MB))
        .Given(MemoryNode("segment_2").Capacity(64_MB))
        .When(PutStart("multi-slice-object", 5_MB)
                  .By("writer")
                  .Replicas(3)
                  .ExpectReplicas(3))
        .Then(Object("multi-slice-object").IsNotReady())
        .When(PutEnd("multi-slice-object").By("writer"))
        .Then(Object("multi-slice-object")
                  .IsReadable()
                  .HasCompleteReplicas(3)
                  .HasSize(5_MB)
                  .HasReplicasOn({"segment_0", "segment_1", "segment_2"}));
}

}  // namespace mooncake::test
