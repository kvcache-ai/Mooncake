#include "master_scenario.h"

#include <gtest/gtest.h>

namespace mooncake::test {

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

}  // namespace mooncake::test
