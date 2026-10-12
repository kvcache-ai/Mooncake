#include <gtest/gtest.h>
#include <ylt/struct_pack.hpp>

#include "conductor/client/types.h"

namespace mooncake::conductor {
namespace {

template <typename T>
void ExpectRoundtrip(const T& value) {
    auto buf = struct_pack::serialize(value);
    auto decoded = struct_pack::deserialize<T>(buf);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, value);
}

TEST(WireTypesTest, QueryRequestRoundtrip) {
    QueryRequest req;
    req.token_ids = {1, -2, 3000000};
    req.cache_salt = "salt";
    // instance_filter stays empty while cache_salt is set, covering both
    // states of the optional fields.
    ExpectRoundtrip(req);
}

TEST(WireTypesTest, QueryResultRoundtrip) {
    QueryResult result;
    prefixindex::CacheHitResult hit;
    hit.longest_match_tokens = 42;
    hit.dp = {{0, 10}, {1, 20}};
    hit.npu = 5;
    hit.cpu_share = 7;
    result.instances.emplace("inst-1", hit);
    ExpectRoundtrip(result);
}

TEST(WireTypesTest, ServiceConfigRoundtrip) {
    common::ServiceConfig svc;
    svc.endpoint = "tcp://127.0.0.1:5555";
    svc.instance_id = "inst-1";
    svc.block_size = 16;
    svc.cache_group = 3;
    svc.publisher_kind = common::PublisherKind::kSglang;
    svc.hash_profile.strategy = "vllm";
    svc.hash_profile.algorithm = "sha256";
    ExpectRoundtrip(svc);
}

TEST(WireTypesTest, RegisterUnregisterResultRoundtrip) {
    ExpectRoundtrip(RegisterResult{"inst-1", true});
    ExpectRoundtrip(UnregisterResult{"inst-1:default:0"});
}

}  // namespace
}  // namespace mooncake::conductor
