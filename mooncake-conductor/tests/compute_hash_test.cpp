#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "conductor/prefixindex/hash_strategy.h"
#include "test_fixtures.h"

namespace {

using conductor::prefixindex::ContextKey;
using conductor::prefixindex::CreateHashStrategy;
using conductor::prefixindex::DigestToHex;
using conductor::prefixindex::HashBlock;
using conductor::prefixindex::HashProfile;
using conductor::prefixindex::ValidateHashProfile;
using conductor_test::LoadJsonFixture;
using conductor_test::ParseU64;

HashProfile ProfileFrom(const Json::Value& value) {
    HashProfile profile;
    profile.strategy = value["strategy"].asString();
    profile.algorithm = value["algorithm"].asString();
    profile.root_digest = value["root_digest"].asString();
    profile.index_projection = value["index_projection"].asString();
    return profile;
}

HashProfile ValidProfile() {
    return HashProfile{
        .strategy = "vllm_v1",
        .algorithm = "sha256_cbor",
        .root_digest =
            "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e",
        .index_projection = "low64_be",
    };
}

std::vector<int32_t> TokensFrom(const Json::Value& values) {
    std::vector<int32_t> tokens;
    tokens.reserve(values.size());
    for (const auto& value : values) {
        tokens.push_back(static_cast<int32_t>(value.asInt64()));
    }
    return tokens;
}

std::optional<std::string> SaltFrom(const Json::Value& value) {
    if (value.isNull()) {
        return std::nullopt;
    }
    return value.asString();
}

TEST(HashProfile, AcceptsOnlyTheSupportedProfile) {
    HashProfile profile = ValidProfile();
    EXPECT_TRUE(ValidateHashProfile(profile).empty());

    std::string error = "stale error";
    auto strategy = CreateHashStrategy(profile, &error);
    EXPECT_NE(strategy, nullptr);
    EXPECT_TRUE(error.empty());
    EXPECT_NE(CreateHashStrategy(profile, nullptr), nullptr);
}

TEST(HashProfile, RejectsUnsupportedAndMalformedProfiles) {
    std::vector<std::pair<std::string, HashProfile>> cases;

    HashProfile profile = ValidProfile();
    profile.strategy = "vllm_v2";
    cases.emplace_back("strategy", profile);

    profile = ValidProfile();
    profile.algorithm = "sha256";
    cases.emplace_back("algorithm", profile);

    profile = ValidProfile();
    profile.index_projection = "high64_be";
    cases.emplace_back("projection", profile);

    profile = ValidProfile();
    profile.root_digest.pop_back();
    cases.emplace_back("short root", profile);

    profile = ValidProfile();
    profile.root_digest.push_back('0');
    cases.emplace_back("long root", profile);

    profile = ValidProfile();
    profile.root_digest[1] = 'E';
    cases.emplace_back("uppercase root", profile);

    profile = ValidProfile();
    profile.root_digest[0] = 'g';
    cases.emplace_back("non-hex root", profile);

    for (const auto& [name, candidate] : cases) {
        SCOPED_TRACE(name);
        const std::string validation_error = ValidateHashProfile(candidate);
        EXPECT_FALSE(validation_error.empty());

        std::string factory_error;
        EXPECT_EQ(CreateHashStrategy(candidate, &factory_error), nullptr);
        EXPECT_EQ(factory_error, validation_error);
    }
}

TEST(HashStrategyGolden, MatchesVllmAndCbor2Vectors) {
    const Json::Value fixture = LoadJsonFixture("hash_golden_vectors.json");
    const HashProfile profile = ProfileFrom(fixture["profile"]);

    std::string factory_error;
    auto strategy = CreateHashStrategy(profile, &factory_error);
    ASSERT_NE(strategy, nullptr) << factory_error;

    const Json::Value& cases = fixture["cases"];
    ASSERT_GT(cases.size(), 0u);
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case["name"].asString());
        ContextKey context{
            .tenant_id = "default",
            .model_name = "golden-model",
            .lora_name = test_case["lora_name"].asString(),
            .block_size = test_case["block_size"].asInt64(),
        };
        const std::vector<int32_t> tokens = TokensFrom(test_case["token_ids"]);

        std::vector<HashBlock> blocks;
        const std::string error = strategy->Compute(
            context, tokens, SaltFrom(test_case["cache_salt"]), &blocks);
        ASSERT_TRUE(error.empty()) << error;

        const Json::Value& expected = test_case["expected"];
        ASSERT_EQ(blocks.size(), expected.size());
        for (Json::ArrayIndex index = 0; index < expected.size(); ++index) {
            const std::string digest = DigestToHex(blocks[index].digest);
            EXPECT_EQ(digest, expected[index]["digest"].asString())
                << "block=" << index;
            EXPECT_EQ(digest.substr(48),
                      expected[index]["projected_hex"].asString())
                << "block=" << index;
            EXPECT_EQ(blocks[index].projected.value,
                      ParseU64(expected[index]["projected_decimal"]))
                << "block=" << index;
        }
    }
}

TEST(HashStrategyGolden, MultiBlockChainDoesNotReuseLow64AsParent) {
    const Json::Value fixture = LoadJsonFixture("hash_golden_vectors.json");
    const Json::Value& test_case = fixture["cases"][0];
    ASSERT_EQ(test_case["name"].asString(), "spec_unsalted");

    std::string factory_error;
    auto strategy =
        CreateHashStrategy(ProfileFrom(fixture["profile"]), &factory_error);
    ASSERT_NE(strategy, nullptr) << factory_error;

    ContextKey context{
        .tenant_id = "default",
        .model_name = "golden-model",
        .lora_name = "",
        .block_size = test_case["block_size"].asInt64(),
    };
    const std::vector<int32_t> tokens = TokensFrom(test_case["token_ids"]);
    std::vector<HashBlock> blocks;
    ASSERT_TRUE(
        strategy->Compute(context, tokens, std::nullopt, &blocks).empty());
    ASSERT_EQ(blocks.size(), 2u);

    const std::string second_digest = DigestToHex(blocks[1].digest);
    EXPECT_EQ(second_digest, test_case["expected"][1]["digest"].asString());
    EXPECT_NE(second_digest,
              test_case["incorrect_low64_parent_digest"].asString());
}

TEST(HashStrategy, EmptySaltHasNoExtraKey) {
    std::string factory_error;
    auto strategy = CreateHashStrategy(ValidProfile(), &factory_error);
    ASSERT_NE(strategy, nullptr) << factory_error;

    ContextKey context{
        .tenant_id = "default",
        .model_name = "model",
        .lora_name = "",
        .block_size = 4,
    };
    const std::vector<int32_t> tokens{1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<HashBlock> omitted_salt;
    std::vector<HashBlock> empty_salt;
    ASSERT_TRUE(strategy->Compute(context, tokens, std::nullopt, &omitted_salt)
                    .empty());
    ASSERT_TRUE(
        strategy->Compute(context, tokens, std::string{}, &empty_salt).empty());
    EXPECT_EQ(empty_salt, omitted_salt);
}

TEST(HashStrategy, RejectsInvalidComputeInputsWithoutPartialOutput) {
    std::string factory_error;
    auto strategy = CreateHashStrategy(ValidProfile(), &factory_error);
    ASSERT_NE(strategy, nullptr) << factory_error;

    ContextKey context{
        .tenant_id = "default",
        .model_name = "model",
        .lora_name = "",
        .block_size = 0,
    };
    const std::vector<int32_t> tokens{1, 2, 3, 4};
    std::vector<HashBlock> blocks(1);
    EXPECT_FALSE(
        strategy->Compute(context, tokens, std::nullopt, &blocks).empty());
    EXPECT_TRUE(blocks.empty());

    context.block_size = -4;
    blocks.resize(1);
    EXPECT_FALSE(
        strategy->Compute(context, tokens, std::nullopt, &blocks).empty());
    EXPECT_TRUE(blocks.empty());

    context.block_size = 4;
    context.lora_name = std::string("\xc0\xaf", 2);
    blocks.resize(1);
    EXPECT_FALSE(
        strategy->Compute(context, tokens, std::nullopt, &blocks).empty());
    EXPECT_TRUE(blocks.empty());

    context.lora_name.clear();
    const std::string invalid_salt("\xed\xa0\x80", 3);
    blocks.resize(1);
    EXPECT_FALSE(
        strategy->Compute(context, tokens, invalid_salt, &blocks).empty());
    EXPECT_TRUE(blocks.empty());

    EXPECT_FALSE(
        strategy->Compute(context, tokens, std::nullopt, nullptr).empty());
}

TEST(HashStrategy, RegisteredRootDigestChangesTheChain) {
    HashProfile alternate_profile = ValidProfile();
    alternate_profile.root_digest = std::string(64, '0');

    std::string factory_error;
    auto default_strategy = CreateHashStrategy(ValidProfile(), &factory_error);
    ASSERT_NE(default_strategy, nullptr) << factory_error;
    auto alternate_strategy =
        CreateHashStrategy(alternate_profile, &factory_error);
    ASSERT_NE(alternate_strategy, nullptr) << factory_error;

    ContextKey context{
        .tenant_id = "default",
        .model_name = "model",
        .lora_name = "",
        .block_size = 4,
    };
    const std::vector<int32_t> tokens{1, 2, 3, 4};
    std::vector<HashBlock> default_blocks;
    std::vector<HashBlock> alternate_blocks;
    ASSERT_TRUE(default_strategy
                    ->Compute(context, tokens, std::nullopt, &default_blocks)
                    .empty());
    ASSERT_TRUE(alternate_strategy
                    ->Compute(context, tokens, std::nullopt, &alternate_blocks)
                    .empty());
    ASSERT_EQ(default_blocks.size(), 1u);
    ASSERT_EQ(alternate_blocks.size(), 1u);
    EXPECT_NE(default_blocks, alternate_blocks);
}

TEST(HashStrategy, DigestToHexPreservesLeadingZerosAndUsesLowercase) {
    std::array<uint8_t, 32> digest{};
    digest[0] = 0x01;
    digest[30] = 0xcd;
    digest[31] = 0xef;

    const std::string encoded = DigestToHex(digest);
    ASSERT_EQ(encoded.size(), 64u);
    EXPECT_EQ(encoded.substr(0, 4), "0100");
    EXPECT_EQ(encoded.substr(60), "cdef");
}

}  // namespace
