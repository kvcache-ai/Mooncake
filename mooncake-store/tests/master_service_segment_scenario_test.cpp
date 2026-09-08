#include "master_service/dsl/scenario.h"

#include <gtest/gtest.h>

#include <chrono>

namespace mooncake::test {

using std::chrono::milliseconds;

TEST(MasterServiceSegmentScenarioTest, GracefulUnmountSetsCorrectStatus) {
    MasterScenario("graceful unmount marks the node gracefully unmounting")
        .Given(MemoryNode("graceful-node"))
        .Then(MemoryNodeStatus("graceful-node").Is(SegmentStatus::OK))
        .When(GracefullyUnmountMemoryNode("graceful-node")
                  .After(milliseconds(10000)))
        .Then(MemoryNodeStatus("graceful-node")
                  .Is(SegmentStatus::GRACEFULLY_UNMOUNTING));
}

TEST(MasterServiceSegmentScenarioTest, GracefulUnmountRejectsWrongClient) {
    MasterScenario("only the owner may gracefully unmount a node")
        .Given(MemoryNode("owned-node"))
        .When(GracefullyUnmountMemoryNode("owned-node")
                  .By("stranger")
                  .After(milliseconds(10000))
                  .ExpectError(ErrorCode::SEGMENT_NOT_FOUND))
        .Then(MemoryNodeStatus("owned-node").Is(SegmentStatus::OK))
        .When(GracefullyUnmountMemoryNode("owned-node")
                  .After(milliseconds(10000)))
        .Then(MemoryNodeStatus("owned-node")
                  .Is(SegmentStatus::GRACEFULLY_UNMOUNTING));
}

TEST(MasterServiceSegmentScenarioTest, GracefulUnmountIsIdempotent) {
    MasterScenario("repeated graceful unmounts succeed")
        .Given(MemoryNode("idempotent-node"))
        .When(GracefullyUnmountMemoryNode("idempotent-node")
                  .After(milliseconds(10000)))
        .When(GracefullyUnmountMemoryNode("idempotent-node")
                  .After(milliseconds(10000)))
        .Then(MemoryNodeStatus("idempotent-node")
                  .Is(SegmentStatus::GRACEFULLY_UNMOUNTING));
}

TEST(MasterServiceSegmentScenarioTest, GracefulUnmountTimerExpiresAndUnmounts) {
    MasterScenario("the grace timer eventually unmounts the node")
        .Given(MemoryNode("timer-node"))
        .When(
            GracefullyUnmountMemoryNode("timer-node").After(milliseconds(200)))
        .Then(MemoryNodeStatus("timer-node")
                  .Is(SegmentStatus::GRACEFULLY_UNMOUNTING))
        .Then(MemoryNodeStatus("timer-node").DoesNotExist().Eventually());
}

TEST(MasterServiceSegmentScenarioTest, GracefulUnmountQueryByIdWithReusedName) {
    MasterScenario("status by id separates a reused node name")
        .Given(MemoryNode("reused-name"))
        .When(
            GracefullyUnmountMemoryNode("reused-name").After(milliseconds(200)))
        .When(MountMemorySegment("reused-replacement")
                  .Named("reused-name")
                  .Base(0x400000000)
                  .Endpoint("reused-name")
                  .By("reused-name"))
        .Then(MemoryNodeStatus("reused-name")
                  .ById()
                  .Is(SegmentStatus::GRACEFULLY_UNMOUNTING))
        .Then(
            MemoryNodeStatus("reused-replacement").ById().Is(SegmentStatus::OK))
        .Then(
            MemoryNodeStatus("reused-name").ById().DoesNotExist().Eventually())
        .Then(
            MemoryNodeStatus("reused-replacement").ById().Is(SegmentStatus::OK))
        .Then(MemoryNodeStatus("reused-name").Is(SegmentStatus::OK));
}

TEST(MasterServiceSegmentScenarioTest, GracefulUnmountEarlierTimerPreempts) {
    MasterScenario("a shorter grace period fires before a longer one")
        .Given(MemoryNode("long-timer-node"))
        .Given(MemoryNode("short-timer-node"))
        .When(GracefullyUnmountMemoryNode("long-timer-node")
                  .After(milliseconds(10000)))
        .When(WaitFor(milliseconds(20)))
        .When(GracefullyUnmountMemoryNode("short-timer-node")
                  .After(milliseconds(50)))
        .Then(MemoryNodeStatus("short-timer-node").DoesNotExist().Eventually())
        .Then(MemoryNodeStatus("long-timer-node")
                  .Is(SegmentStatus::GRACEFULLY_UNMOUNTING));
}

TEST(MasterServiceSegmentScenarioTest, GracefulUnmountPreventsNewAllocation) {
    MasterScenario("a gracefully unmounting node stays readable but full")
        .Given(MemoryNode("draining-node"))
        .Given(MemoryNode("healthy-node"))
        .When(PutStart("existing_key", 1_KB).OnNode("draining-node"))
        .When(PutEnd("existing_key"))
        .When(GracefullyUnmountMemoryNode("draining-node")
                  .After(milliseconds(10000)))
        .Then(MemoryNodeStatus("draining-node")
                  .Is(SegmentStatus::GRACEFULLY_UNMOUNTING))
        .Then(Object("existing_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .IsOnMemoryNode("draining-node"))
        .Then(MemoryNodeStatus("healthy-node").Is(SegmentStatus::OK))
        .When(PutStart("new_key", 1_KB)
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"healthy-node"}))
        .When(PutEnd("new_key"));
}

TEST(MasterServiceSegmentScenarioTest, ClientIpsFromSingleEndpoint) {
    MasterScenario("client IPs derive from the owned node endpoint")
        .Given(MemoryNode("segment-a")
                   .Endpoint("127.0.0.1:12345")
                   .OwnedBy("client-a"))
        .Then(ClientIps({"client-a"}).Returns("client-a", {"127.0.0.1"}))
        .Then(ClientIps({"client-a", "ghost-client"})
                  .Returns("client-a", {"127.0.0.1"})
                  .Omits("ghost-client"));
}

TEST(MasterServiceSegmentScenarioTest, ClientIpsAreUniqueAcrossNodes) {
    MasterScenario("client IPs are deduplicated across owned nodes")
        .Given(MemoryNode("segment-a")
                   .Endpoint("127.0.0.1:12345")
                   .OwnedBy("client-a"))
        .Given(MemoryNode("segment-b")
                   .Endpoint("127.0.0.1:12346")
                   .OwnedBy("client-a"))
        .Given(MemoryNode("segment-c")
                   .Endpoint("192.168.1.1:12345")
                   .OwnedBy("client-a"))
        .Then(ClientIps({"client-a"})
                  .Returns("client-a", {"127.0.0.1", "192.168.1.1"}));
}

TEST(MasterServiceSegmentScenarioTest, ClientIpsForNoActors) {
    MasterScenario("an empty client list returns no entries")
        .Given(MemoryNode("segment-a"))
        .Then(ClientIps({}));
}

TEST(MasterServiceSegmentScenarioTest, ClientIpsWithEmptyEndpoints) {
    MasterScenario("empty endpoints yield a client entry with no IPs")
        .Given(MemoryNode("segment-a").Endpoint("").OwnedBy("client-a"))
        .Given(MemoryNode("segment-b").Endpoint("").OwnedBy("client-a"))
        .Then(ClientIps({"client-a"}).Returns("client-a", {}));
}

TEST(MasterServiceSegmentScenarioTest, ClientIpsParseBracketedIpv6) {
    MasterScenario("bracketed IPv6 endpoints lose brackets and port")
        .Given(
            MemoryNode("segment-a").Endpoint("[::1]:17813").OwnedBy("client-a"))
        .Then(ClientIps({"client-a"}).Returns("client-a", {"::1"}));
}

TEST(MasterServiceSegmentScenarioTest, ClientIpsKeepLinkLocalIpv6Scope) {
    MasterScenario("link-local IPv6 endpoints keep their scope id")
        .Given(MemoryNode("segment-a")
                   .Endpoint("fe80::a236:bcff:fecb:a1be%eno2:15773")
                   .OwnedBy("client-a"))
        .Then(ClientIps({"client-a"})
                  .Returns("client-a", {"fe80::a236:bcff:fecb:a1be%eno2"}));
}

TEST(MasterServiceSegmentScenarioTest, ClientIpsParseIpv6WithoutPort) {
    MasterScenario("a portless IPv6 endpoint is returned as-is")
        .Given(MemoryNode("segment-a").Endpoint("::1").OwnedBy("client-a"))
        .Then(ClientIps({"client-a"}).Returns("client-a", {"::1"}));
}

TEST(MasterServiceSegmentScenarioTest, ClientIpsMixIpv4AndIpv6) {
    MasterScenario("IPv4 and IPv6 endpoints combine per client")
        .Given(MemoryNode("segment-a")
                   .Endpoint("192.168.1.1:12345")
                   .OwnedBy("client-a"))
        .Given(
            MemoryNode("segment-b").Endpoint("[::1]:17813").OwnedBy("client-a"))
        .Then(ClientIps({"client-a"})
                  .Returns("client-a", {"192.168.1.1", "::1"}));
}

}  // namespace mooncake::test
