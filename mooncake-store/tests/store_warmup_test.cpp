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
};

TEST_F(StoreWarmupTest, ListWarmupTargetsFiltersLocalBeforeMaxTargets) {
    MasterService service;
    const UUID requester{1, 1};
    const UUID remote_client_1{2, 1};
    const UUID remote_client_2{3, 1};

    Mount(service,
          MakeSegment("local-segment", 0x10000000, "tcp", "127.0.0.1:18000"),
          requester);
    Mount(service,
          MakeSegment("remote-segment-1", 0x20000000, "tcp", "127.0.0.1:18001"),
          remote_client_1);
    Mount(service, MakeSegment("127.0.0.1:18002", 0x30000000, "tcp"),
          remote_client_2);

    auto targets = service.ListWarmupTargets(requester, 1, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 1);
    EXPECT_FALSE(targets->at(0).is_local);
    EXPECT_TRUE(targets->at(0).allow_warmup);
    EXPECT_NE(targets->at(0).client_id, requester);

    targets = service.ListWarmupTargets(requester, 2, {"tcp"});
    ASSERT_TRUE(targets.has_value());
    ASSERT_EQ(targets->size(), 2);
    std::unordered_set<std::string> target_names;
    for (const auto& target : *targets) {
        EXPECT_FALSE(target.is_local);
        EXPECT_TRUE(target.allow_warmup);
        EXPECT_NE(target.client_id, requester);
        target_names.insert(target.segment_name);
    }
    EXPECT_TRUE(target_names.contains("127.0.0.1:18001"));
    EXPECT_TRUE(target_names.contains("127.0.0.1:18002"));
}

TEST_F(StoreWarmupTest, ListWarmupTargetsUsesExactProtocolTokens) {
    MasterService service;
    const UUID requester{4, 1};
    const UUID remote_client_1{5, 1};
    const UUID remote_client_2{6, 1};
    const UUID remote_client_3{7, 1};

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

}  // namespace mooncake::test
