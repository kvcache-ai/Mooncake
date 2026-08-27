// Verifies P0 RPC handlers from RFC #3477 are registered on production paths.
//
// These methods already exist on WrappedMasterService / RealClient but were
// missing from RegisterRpcService / RegisterClientRpcService, causing
// "function not registered" failures at runtime.

#include <gtest/gtest.h>

#include "master_client.h"
#include "master_metric_manager.h"
#include "test_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {
namespace {

class RpcHandlerRegistrationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        InProcMasterConfig config;
        ASSERT_TRUE(master_.Start(config));
    }

    void TearDown() override { master_.Stop(); }

    InProcMaster master_;
};

TEST_F(RpcHandlerRegistrationTest, CalcCacheStatsRpcIsRegistered) {
    MasterClient client(generate_uuid());
    ASSERT_EQ(client.Connect(master_.master_address()), ErrorCode::OK);

    auto stats = client.CalcCacheStats();
    ASSERT_TRUE(stats.has_value()) << toString(stats.error());
    EXPECT_TRUE(stats->count(MasterMetricManager::CacheHitStat::MEMORY_TOTAL) >
                0);
}

}  // namespace
}  // namespace testing
}  // namespace mooncake
