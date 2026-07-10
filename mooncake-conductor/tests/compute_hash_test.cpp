// Bit-level golden-vector tests for ComputeBlockHash / Sum64String /
// ComputePrefixHash: they pin the exact hash byte layout and 64-bit
// output values, so any change to the hashing wire contract is caught.

#include <gtest/gtest.h>

#include <vector>

#include "conductor/prefixindex/prefix_indexer.h"
#include "test_fixtures.h"

namespace {

using conductor::prefixindex::ComputeBlockHash;
using conductor::prefixindex::ModelContext;
using conductor::prefixindex::PrefixCacheTable;
using conductor::prefixindex::Sum64String;
using conductor_test::LoadJsonFixture;
using conductor_test::ParseU64;

std::vector<int32_t> TokensFrom(const Json::Value& arr) {
    std::vector<int32_t> tokens;
    tokens.reserve(arr.size());
    for (const auto& t : arr) {
        tokens.push_back(t.asInt());
    }
    return tokens;
}

TEST(Sum64StringGolden, MatchesGoXxhashSum64String) {
    const auto doc = LoadJsonFixture("seed_golden_vectors.json");
    const auto& cases = doc["cases"];
    ASSERT_GT(cases.size(), 0u);
    for (const auto& c : cases) {
        const std::string input = c["input"].asString();
        EXPECT_EQ(Sum64String(input), ParseU64(c["expected"]))
            << "input=" << input;
    }
}

TEST(ComputeHashGolden, MatchesGoComputeHash) {
    const auto doc = LoadJsonFixture("hash_golden_vectors.json");
    const auto& cases = doc["compute_hash_cases"];
    ASSERT_GT(cases.size(), 0u);
    for (const auto& c : cases) {
        const auto tokens = TokensFrom(c["token_ids"]);
        EXPECT_EQ(ComputeBlockHash(ParseU64(c["parent_hash"]), tokens.data(),
                                   tokens.size()),
                  ParseU64(c["expected"]))
            << "case=" << c["name"].asString();
    }
}

TEST(ComputeHashGolden, PrefixChainMatchesGoComputePrefixHash) {
    const auto doc = LoadJsonFixture("hash_golden_vectors.json");
    const auto& cases = doc["prefix_chain_cases"];
    ASSERT_GT(cases.size(), 0u);

    PrefixCacheTable table;
    for (const auto& c : cases) {
        ModelContext ctx;
        ctx.model_name = "golden-model";
        ctx.lora_name = "";
        ctx.block_size = c["block_size"].asInt64();
        ctx.additional_salt = c["additional_salt"].asString();
        ctx.tenant_id = "default";
        ctx.instance_id = "golden-instance";

        const auto tokens = TokensFrom(c["token_ids"]);
        const auto chain =
            table.ComputePrefixHash(ctx, tokens, ParseU64(c["cache_salt"]));

        const auto& expected = c["expected_chain"];
        ASSERT_EQ(chain.size(), expected.size())
            << "case=" << c["name"].asString();
        for (Json::ArrayIndex i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(chain[i], ParseU64(expected[i]))
                << "case=" << c["name"].asString() << " index=" << i;
        }
    }
}

// Spec scenario: ComputePrefixHash with BlockSize <= 0 warns and returns
// an empty result (no crash).
TEST(ComputeHash, InvalidBlockSizeReturnsEmpty) {
    PrefixCacheTable table;
    ModelContext ctx;
    ctx.model_name = "m";
    ctx.block_size = 0;
    EXPECT_TRUE(table.ComputePrefixHash(ctx, {1, 2, 3, 4}, 0).empty());
    ctx.block_size = -4;
    EXPECT_TRUE(table.ComputePrefixHash(ctx, {1, 2, 3, 4}, 0).empty());
}

}  // namespace
