#include "conductor/client/types.h"

#include <gtest/gtest.h>

namespace mooncake::conductor {
namespace {

TEST(ClientTypesTest, ErrorCodeValuesAlignWithStore) {
    // Values must match mooncake-store/include/types.h where meaning overlap.
    EXPECT_EQ(static_cast<int32_t>(ErrorCode::OK), 0);
    EXPECT_EQ(static_cast<int32_t>(ErrorCode::INTERNAL_ERROR), -1);
    EXPECT_EQ(static_cast<int32_t>(ErrorCode::INVALID_PARAMS), -600);
    EXPECT_EQ(static_cast<int32_t>(ErrorCode::RPC_FAIL), -900);
    EXPECT_EQ(static_cast<int32_t>(ErrorCode::RPC_TIMEOUT), -901);
}

TEST(ClientTypesTest, ErrorCodeNameCoversAllCodes) {
    EXPECT_STREQ(ErrorCodeName(ErrorCode::OK), "OK");
    EXPECT_STREQ(ErrorCodeName(ErrorCode::CONDUCTOR_UNAVAILABLE),
                 "CONDUCTOR_UNAVAILABLE");
    EXPECT_STREQ(ErrorCodeName(ErrorCode::SERVICE_NOT_FOUND),
                 "SERVICE_NOT_FOUND");
}

TEST(ClientTypesTest, QueryRequestDefaults) {
    QueryRequest req;
    EXPECT_TRUE(req.token_ids.empty());
    EXPECT_FALSE(req.cache_salt.has_value());
    EXPECT_FALSE(req.instance_filter.has_value());
}

}  // namespace
}  // namespace mooncake::conductor
