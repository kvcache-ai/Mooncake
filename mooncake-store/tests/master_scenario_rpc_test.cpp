#include "master_scenario.h"

namespace mooncake::test {

TEST(MasterScenarioRpcTest, BatchQueryIpSkipsUnknownClients) {
    MasterScenario("batch query IP skips unknown clients")
        .Given(
            MemoryNode("segment").OwnedBy("client").Endpoint("127.0.0.1:12345"))
        .Then(
            ClientIps({"client", "missing"}).Are({{"client", {"127.0.0.1"}}}));
}

TEST(MasterScenarioRpcTest, BatchQueryIpDeduplicatesMultipleSegments) {
    MasterScenario("batch query IP deduplicates multiple segments")
        .Given(MemoryNode("segment-1")
                   .OwnedBy("client")
                   .Endpoint("127.0.0.1:12345"))
        .Given(MemoryNode("segment-2")
                   .OwnedBy("client")
                   .Endpoint("127.0.0.1:12346"))
        .Given(MemoryNode("segment-3")
                   .OwnedBy("client")
                   .Endpoint("192.168.1.1:12345"))
        .Then(ClientIps({"client"})
                  .Are({{"client", {"127.0.0.1", "192.168.1.1"}}}));
}

TEST(MasterScenarioRpcTest, BatchQueryIpAcceptsEmptyInput) {
    MasterScenario("batch query IP accepts empty input")
        .Given(MemoryNode("segment"))
        .Then(ClientIps({}).Are({}));
}

TEST(MasterScenarioRpcTest, EmptyEndpointsProduceEmptyIpList) {
    MasterScenario("empty endpoints produce an empty IP list")
        .Given(MemoryNode("segment-1").OwnedBy("client").Endpoint(""))
        .Given(MemoryNode("segment-2").OwnedBy("client").Endpoint(""))
        .Then(ClientIps({"client"}).Are({{"client", {}}}));
}

TEST(MasterScenarioRpcTest, BatchQueryIpParsesIpv6Forms) {
    MasterScenario("batch query IP parses IPv6 forms")
        .Given(MemoryNode("bracketed")
                   .OwnedBy("bracketed-client")
                   .Endpoint("[::1]:17813"))
        .Given(MemoryNode("link-local")
                   .OwnedBy("link-local-client")
                   .Endpoint("fe80::a236:bcff:fecb:a1be%eno2:15773"))
        .Given(MemoryNode("no-port").OwnedBy("no-port-client").Endpoint("::1"))
        .Then(
            ClientIps(
                {"bracketed-client", "link-local-client", "no-port-client"})
                .Are({{"bracketed-client", {"::1"}},
                      {"link-local-client", {"fe80::a236:bcff:fecb:a1be%eno2"}},
                      {"no-port-client", {"::1"}}}));
}

TEST(MasterScenarioRpcTest, BatchQueryIpHandlesMixedIpv4AndIpv6) {
    MasterScenario("batch query IP handles mixed IPv4 and IPv6")
        .Given(
            MemoryNode("ipv4").OwnedBy("client").Endpoint("192.168.1.1:12345"))
        .Given(MemoryNode("ipv6").OwnedBy("client").Endpoint("[::1]:17813"))
        .Then(ClientIps({"client"}).Are({{"client", {"192.168.1.1", "::1"}}}));
}

}  // namespace mooncake::test
