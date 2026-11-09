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
    hit.device = 5;
    hit.cpu_share = 7;
    result.instances.emplace("inst-1", hit);
    ExpectRoundtrip(result);
}

// Retain the previous result layout only as a wire-compatibility fixture.
// Production exposes device, without a second npu member or response alias.
TEST(WireTypesTest, DeviceRenamePreservesResultWireLayout) {
    struct PreviousRankMatch {
        int64_t npu = 0;
        int64_t cpu_local = 0;
        int64_t cpu_share = 0;
        int64_t disk = 0;
    };
    struct PreviousHit {
        int64_t longest_match_tokens = 0;
        std::map<int64_t, int64_t> dp;
        std::map<int64_t, PreviousRankMatch> rank_matches;
        int64_t npu = 0;
        int64_t cpu_local = 0;
        int64_t cpu_share = 0;
        int64_t disk = 0;
    };
    struct PreviousResult {
        std::map<std::string, PreviousHit> instances;
    };
    PreviousResult previous;
    previous.instances.emplace(
        "engine",
        PreviousHit{64, {{0, 16}}, {{0, {16, 32, 48, 64}}}, 16, 32, 48, 64});
    QueryResult current;
    current.instances.emplace(
        "engine", prefixindex::CacheHitResult{
                      64, {{0, 16}}, {{0, {16, 32, 48, 64}}}, 16, 32, 48, 64});
    const auto old_bytes = struct_pack::serialize(previous);
    const auto new_bytes = struct_pack::serialize(current);
    EXPECT_EQ(old_bytes, new_bytes);
    auto decoded = struct_pack::deserialize<QueryResult>(old_bytes);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, current);
    auto old_decoded = struct_pack::deserialize<PreviousResult>(new_bytes);
    ASSERT_TRUE(old_decoded.has_value());
    const auto& hit = old_decoded->instances.at("engine");
    EXPECT_EQ(hit.npu, current.instances.at("engine").device);
    EXPECT_EQ(hit.rank_matches.at(0).npu,
              current.instances.at("engine").rank_matches.at(0).device);
    EXPECT_EQ(struct_pack::serialize(*old_decoded), old_bytes);
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
