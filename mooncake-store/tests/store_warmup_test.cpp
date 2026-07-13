#include "master_service.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_set>
#include <vector>

#include "types.h"

namespace mooncake::test {

class StoreWarmupTest : public ::testing::Test {
   protected:
    static Segment MakeSegment(std::string name, uintptr_t base,
                               std::string protocol,
                               std::string te_endpoint = "") {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = 1024 * 1024;
        segment.protocol = std::move(protocol);
        segment.te_endpoint = std::move(te_endpoint);
        return segment;
    }

    static void Mount(MasterService& service, const Segment& segment,
                      const UUID& client_id) {
        auto result = service.MountSegment(segment, client_id);
        ASSERT_TRUE(result.has_value());
    }

    static std::unordered_set<std::string> TargetNames(
        const std::vector<WarmupTarget>& targets) {
        std::unordered_set<std::string> names;
        for (const auto& target : targets) {
            names.insert(target.segment_name);
        }
        return names;
    }

    static void ExpectSequentialPriority(
        const std::vector<WarmupTarget>& targets) {
        for (size_t i = 0; i < targets.size(); ++i) {
            EXPECT_EQ(targets[i].priority, i);
        }
    }
};

TEST_F(StoreWarmupTest, SingleNodeReturnsNoWarmupTargets) {
    MasterService service;
    const UUID requester{1, 1};

    Mount(service,
          MakeSegment("local-segment-1", 0x10000000, "tcp", "127.0.0.1:18000"),
          requester);
    Mount(service, MakeSegment("127.0.0.1:18001", 0x20000000, "tcp"),
          requester);

    auto targets = service.ListWarmupTargets(requester, 0, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    EXPECT_TRUE(targets->empty());

    targets = service.ListWarmupTargets(requester, 1, {});
    ASSERT_TRUE(targets.has_value());
    EXPECT_TRUE(targets->empty());
}

TEST_F(StoreWarmupTest, DifferentClientsReturnRemoteWarmupTargets) {
    MasterService service;
    const UUID requester{2, 1};
    const UUID remote_client_1{3, 1};
    const UUID remote_client_2{4, 1};

    auto local_segment =
        MakeSegment("local-segment", 0x10000000, "tcp", "127.0.0.1:18000");
    auto remote_segment_1 = MakeSegment(
        "remote-segment-1", 0x20000000, "tcp", "127.0.0.1:18001");
    auto remote_segment_2 =
        MakeSegment("127.0.0.1:18002", 0x30000000, "tcp");
    local_segment.host_id = "same-host";
    remote_segment_1.host_id = "same-host";
    remote_segment_2.host_id = "same-host";

    Mount(service, local_segment, requester);
    Mount(service, remote_segment_1, remote_client_1);
    Mount(service, remote_segment_2, remote_client_2);

    auto targets = service.ListWarmupTargets(requester, 1, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 1);
    EXPECT_FALSE(targets->at(0).is_local);
    EXPECT_TRUE(targets->at(0).allow_warmup);
    EXPECT_NE(targets->at(0).client_id, requester);

    targets = service.ListWarmupTargets(requester, 2, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 2);
    for (const auto& target : *targets) {
        EXPECT_FALSE(target.is_local);
        EXPECT_TRUE(target.allow_warmup);
        EXPECT_NE(target.client_id, requester);
    }
    const auto target_names = TargetNames(*targets);
    EXPECT_TRUE(target_names.contains("127.0.0.1:18001"));
    EXPECT_TRUE(target_names.contains("127.0.0.1:18002"));
}

TEST_F(StoreWarmupTest, ListWarmupTargetsUsesExactProtocolTokens) {
    MasterService service;
    const UUID requester{5, 1};
    const UUID remote_client_1{6, 1};
    const UUID remote_client_2{7, 1};
    const UUID remote_client_3{8, 1};

    Mount(service,
          MakeSegment("remote-token-list", 0x40000000, "tcp,rdma",
                      "127.0.0.1:18003"),
          remote_client_1);
    Mount(service,
          MakeSegment("remote-substring", 0x50000000, "tcp_like",
                      "127.0.0.1:18004"),
          remote_client_2);
    Mount(service,
          MakeSegment("remote-rdma", 0x60000000, "rdma", "127.0.0.1:18005"),
          remote_client_3);

    auto targets = service.ListWarmupTargets(requester, 0, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 1);
    EXPECT_EQ(targets->at(0).segment_name, "127.0.0.1:18003");
    EXPECT_EQ(targets->at(0).protocol, "tcp,rdma");
}

TEST_F(StoreWarmupTest, AbnormalSegmentsAreSkipped) {
    MasterService service;
    const UUID requester{9, 1};
    const UUID remote_client{10, 1};

    auto ok_segment =
        MakeSegment("remote-ok", 0x70000000, "tcp", "127.0.0.1:18006");
    auto graceful_segment =
        MakeSegment("remote-graceful", 0x80000000, "tcp", "127.0.0.1:18007");
    auto unmounted_segment =
        MakeSegment("remote-unmounted", 0x90000000, "tcp", "127.0.0.1:18008");
    auto empty_name_segment = MakeSegment("", 0xA0000000, "tcp");

    Mount(service, ok_segment, remote_client);
    Mount(service, graceful_segment, remote_client);
    Mount(service, unmounted_segment, remote_client);
    Mount(service, empty_name_segment, remote_client);

    ASSERT_TRUE(service
                    .GracefulUnmountSegment(graceful_segment.id, remote_client,
                                            60 * 1000)
                    .has_value());
    ASSERT_TRUE(service.UnmountSegment(unmounted_segment.id, remote_client)
                    .has_value());

    auto targets = service.ListWarmupTargets(requester, 0, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 1);
    EXPECT_EQ(targets->at(0).segment_name, "127.0.0.1:18006");
    EXPECT_TRUE(targets->at(0).allow_warmup);
    EXPECT_FALSE(targets->at(0).is_local);
}

TEST_F(StoreWarmupTest, DeduplicatesWarmupTargetsByEndpoint) {
    MasterService service;
    const UUID requester{11, 1};
    const UUID remote_client{12, 1};

    Mount(service,
          MakeSegment("remote-dup-a", 0xB0000000, "tcp", "127.0.0.1:18009"),
          remote_client);
    Mount(service,
          MakeSegment("remote-dup-b", 0xC0000000, "tcp", "127.0.0.1:18009"),
          remote_client);
    Mount(service,
          MakeSegment("remote-distinct", 0xD0000000, "tcp", "127.0.0.1:18010"),
          remote_client);

    auto targets = service.ListWarmupTargets(requester, 0, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 2);
    const auto target_names = TargetNames(*targets);
    EXPECT_TRUE(target_names.contains("127.0.0.1:18009"));
    EXPECT_TRUE(target_names.contains("127.0.0.1:18010"));
    ExpectSequentialPriority(*targets);
}

TEST_F(StoreWarmupTest, MaxTargetsUsesRequesterStableRotation) {
    MasterService service;
    const UUID requester_a{0, 0};
    const UUID requester_b{1, 0};
    const UUID remote_client{13, 1};

    Mount(service, MakeSegment("remote-a", 0xE0000000, "tcp", "target-a"),
          remote_client);
    Mount(service, MakeSegment("remote-b", 0xF0000000, "tcp", "target-b"),
          remote_client);
    Mount(service, MakeSegment("remote-c", 0x110000000, "tcp", "target-c"),
          remote_client);
    Mount(service, MakeSegment("remote-d", 0x120000000, "tcp", "target-d"),
          remote_client);

    auto targets_a = service.ListWarmupTargets(requester_a, 2, {"tcp"});
    auto targets_a_again =
        service.ListWarmupTargets(requester_a, 2, {"tcp"});
    auto targets_b = service.ListWarmupTargets(requester_b, 2, {"tcp"});
    ASSERT_TRUE(targets_a.has_value());
    ASSERT_TRUE(targets_a_again.has_value());
    ASSERT_TRUE(targets_b.has_value());
    ASSERT_EQ(targets_a->size(), 2);
    ASSERT_EQ(targets_a_again->size(), targets_a->size());
    ASSERT_EQ(targets_b->size(), 2);
    ExpectSequentialPriority(*targets_a);
    ExpectSequentialPriority(*targets_a_again);
    ExpectSequentialPriority(*targets_b);
    for (size_t i = 0; i < targets_a->size(); ++i) {
        EXPECT_EQ(targets_a->at(i).segment_name,
                  targets_a_again->at(i).segment_name);
    }
    EXPECT_NE(TargetNames(*targets_a), TargetNames(*targets_b));
}

}  // namespace mooncake::test
