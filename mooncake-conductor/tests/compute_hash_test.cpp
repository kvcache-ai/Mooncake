#include <gtest/gtest.h>

#include <openssl/evp.h>

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "conductor/prefixindex/hash_strategy.h"
#include "test_fixtures.h"

namespace {

using conductor::common::HashProfileConfig;
using conductor::prefixindex::ContextKey;
using conductor::prefixindex::CreateHashStrategy;
using conductor::prefixindex::DigestToHex;
using conductor::prefixindex::HashBlock;
using conductor::prefixindex::HashProfile;
using conductor::prefixindex::ResolveHashProfile;
using conductor::prefixindex::ValidateHashProfile;
using conductor_test::LoadJsonFixture;
using conductor_test::ParseU64;

constexpr char kSeedZeroRoot[] =
    "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e";
constexpr char kPaddedSeedZeroRoot[] =
    "8d912e4e62b3cc377b1d1c7a14ef61dffbdaa0990237035c05401c29414c4172";
constexpr char kPickleSeedZeroRoot[] =
    "1973e23848344dc43a988a9b478663803cfffe1243480253f9a3cf004b14aa7c";

HashProfile ProfileFrom(const Json::Value& value) {
    HashProfile profile;
    profile.strategy = value["strategy"].asString();
    profile.algorithm = value["algorithm"].asString();
    profile.python_hash_seed = value["python_hash_seed"].asString();
    profile.root_digest = value["root_digest"].asString();
    profile.index_projection = value["index_projection"].asString();
    return profile;
}

HashProfileConfig SourceProfile(std::string python_hash_seed = "0",
                                std::string algorithm = "sha256_cbor") {
    return {.strategy = "vllm_v1",
            .algorithm = std::move(algorithm),
            .python_hash_seed = std::move(python_hash_seed),
            .index_projection = "low64_be"};
}

HashProfile ValidProfile() {
    return HashProfile{
        .strategy = "vllm_v1",
        .algorithm = "sha256_cbor",
        .python_hash_seed = "0",
        .root_digest = kSeedZeroRoot,
        .index_projection = "low64_be",
    };
}

HashProfile ValidPickleProfile() {
    return HashProfile{
        .strategy = "vllm_v1",
        .algorithm = "sha256",
        .python_hash_seed = "0",
        .root_digest = kPickleSeedZeroRoot,
        .index_projection = "low64_be",
    };
}

HashProfile ResolvedProfile(std::string python_hash_seed,
                            std::string algorithm = "sha256_cbor") {
    HashProfile profile;
    const std::string error = ResolveHashProfile(
        SourceProfile(std::move(python_hash_seed), std::move(algorithm)),
        &profile);
    EXPECT_TRUE(error.empty()) << error;
    return profile;
}

std::vector<int32_t> TokensFrom(const Json::Value& values) {
    std::vector<int32_t> tokens;
    tokens.reserve(values.size());
    for (const auto& value : values) {
        tokens.push_back(static_cast<int32_t>(value.asInt64()));
    }
    return tokens;
}

// Expands the compact repeat encoding used by large fixture cases.
std::vector<int32_t> CaseTokens(const Json::Value& test_case) {
    if (test_case.isMember("token_ids_repeat")) {
        const Json::Value& repeat = test_case["token_ids_repeat"];
        return std::vector<int32_t>(
            static_cast<size_t>(repeat["count"].asUInt64()),
            static_cast<int32_t>(repeat["value"].asInt64()));
    }
    return TokensFrom(test_case["token_ids"]);
}

std::string CaseLora(const Json::Value& test_case) {
    if (test_case.isMember("lora_name_repeat")) {
        const Json::Value& repeat = test_case["lora_name_repeat"];
        return std::string(static_cast<size_t>(repeat["count"].asUInt64()),
                           repeat["value"].asString().front());
    }
    return test_case["lora_name"].asString();
}

std::optional<std::string> SaltFrom(const Json::Value& value) {
    if (value.isNull()) {
        return std::nullopt;
    }
    return value.asString();
}

std::vector<uint8_t> HexToBytes(const std::string& hex) {
    std::vector<uint8_t> bytes;
    bytes.reserve(hex.size() / 2);
    auto nibble = [](char value) -> int {
        if (value >= '0' && value <= '9') {
            return value - '0';
        }
        if (value >= 'a' && value <= 'f') {
            return value - 'a' + 10;
        }
        return -1;
    };
    for (size_t index = 0; index + 1 < hex.size(); index += 2) {
        const int high = nibble(hex[index]);
        const int low = nibble(hex[index + 1]);
        EXPECT_GE(high, 0);
        EXPECT_GE(low, 0);
        bytes.push_back(static_cast<uint8_t>((high << 4) | low));
    }
    return bytes;
}

// Recomputes SHA-256 over the fixture's pinned serialized bytes so the test
// compares serialized bytes as well as digests.
std::string Sha256Hex(const std::vector<uint8_t>& input) {
    std::array<uint8_t, 32> digest{};
    unsigned int digest_size = 0;
    EVP_MD_CTX* context = EVP_MD_CTX_new();
    EXPECT_NE(context, nullptr);
    EXPECT_EQ(EVP_DigestInit_ex(context, EVP_sha256(), nullptr), 1);
    EXPECT_EQ(EVP_DigestUpdate(context, input.data(), input.size()), 1);
    EXPECT_EQ(EVP_DigestFinal_ex(context, digest.data(), &digest_size), 1);
    EVP_MD_CTX_free(context);
    EXPECT_EQ(digest_size, digest.size());
    return DigestToHex(digest);
}

TEST(HashProfileResolver, MatchesSeedRootGoldenVectors) {
    const Json::Value fixture = LoadJsonFixture("hash_golden_vectors.json");
    const Json::Value& vectors = fixture["seed_root_vectors"];
    ASSERT_TRUE(vectors.isArray());
    ASSERT_GE(vectors.size(), 2u);

    for (const auto& vector : vectors) {
        const std::string seed = vector["python_hash_seed"].asString();
        SCOPED_TRACE(seed);
        HashProfile resolved;
        const std::string error =
            ResolveHashProfile(SourceProfile(seed), &resolved);
        ASSERT_TRUE(error.empty()) << error;
        EXPECT_EQ(resolved.python_hash_seed, seed);
        EXPECT_EQ(resolved.root_digest, vector["root_digest"].asString());
    }
}

TEST(HashProfileResolver, MatchesPickleSeedRootGoldenVectors) {
    const Json::Value fixture =
        LoadJsonFixture("hash_golden_vectors_sha256.json");
    const Json::Value& vectors = fixture["seed_root_vectors"];
    ASSERT_TRUE(vectors.isArray());
    ASSERT_GE(vectors.size(), 4u);

    for (const auto& vector : vectors) {
        const std::string seed = vector["python_hash_seed"].asString();
        SCOPED_TRACE(seed);
        HashProfile resolved;
        const std::string error =
            ResolveHashProfile(SourceProfile(seed, "sha256"), &resolved);
        ASSERT_TRUE(error.empty()) << error;
        EXPECT_EQ(resolved.algorithm, "sha256");
        EXPECT_EQ(resolved.python_hash_seed, seed);
        EXPECT_EQ(resolved.root_digest, vector["root_digest"].asString());
        // The serialized seed bytes pinned by the fixture must be exactly
        // what the digest was computed over.
        EXPECT_EQ(Sha256Hex(HexToBytes(vector["pickle_hex"].asString())),
                  vector["root_digest"].asString());
    }
}

TEST(HashProfileResolver, AcceptsSupportedSeedsAndPreservesExactText) {
    struct SeedCase {
        const char* seed;
        const char* root_digest;
    };
    const SeedCase cases[] = {
        {"0", kSeedZeroRoot},
        {"00", kPaddedSeedZeroRoot},
        {"random",
         "78d6ac7e28de859e492449dcea03e3807377d69998c5af819fed33a6df490cad"},
        {"4294967295",
         "177f280a5695322a18f16c96a26dc99d9c03f905940103dfe24a9c646fe446a8"},
    };

    for (const SeedCase& test_case : cases) {
        SCOPED_TRACE(test_case.seed);
        const HashProfile resolved = ResolvedProfile(test_case.seed);
        EXPECT_EQ(resolved.strategy, "vllm_v1");
        EXPECT_EQ(resolved.algorithm, "sha256_cbor");
        EXPECT_EQ(resolved.python_hash_seed, test_case.seed);
        EXPECT_EQ(resolved.root_digest, test_case.root_digest);
        EXPECT_EQ(resolved.index_projection, "low64_be");
        EXPECT_TRUE(ValidateHashProfile(resolved).empty());
    }

    EXPECT_NE(ResolvedProfile("0"), ResolvedProfile("00"));
}

TEST(HashProfileResolver, AcceptsPickleSeedsAndPreservesExactText) {
    const HashProfile resolved = ResolvedProfile("0", "sha256");
    EXPECT_EQ(resolved.strategy, "vllm_v1");
    EXPECT_EQ(resolved.algorithm, "sha256");
    EXPECT_EQ(resolved.python_hash_seed, "0");
    EXPECT_EQ(resolved.root_digest, kPickleSeedZeroRoot);
    EXPECT_EQ(resolved.index_projection, "low64_be");
    EXPECT_TRUE(ValidateHashProfile(resolved).empty());

    // Seed text is never normalized, under either supported algorithm.
    EXPECT_NE(ResolvedProfile("0", "sha256"), ResolvedProfile("00", "sha256"));
    // Identical seed text under different algorithms yields different roots.
    EXPECT_NE(ResolvedProfile("0", "sha256").root_digest,
              ResolvedProfile("0", "sha256_cbor").root_digest);
}

TEST(HashProfileResolver, RejectsMalformedSeedTextAndClearsOutput) {
    const std::vector<std::string> invalid = {
        "",    "+1",     "-1",      " 0",         "0 ",         "0\n",
        "1.0", "Random", "random ", "4294967296", "not-a-seed", "\xe9\x9b\xb6",
    };

    for (const std::string& seed : invalid) {
        SCOPED_TRACE(seed);
        for (const std::string& algorithm : {"sha256_cbor", "sha256"}) {
            HashProfile resolved = ValidProfile();
            EXPECT_FALSE(
                ResolveHashProfile(SourceProfile(seed, algorithm), &resolved)
                    .empty());
            EXPECT_EQ(resolved, HashProfile{});
        }
    }

    EXPECT_FALSE(ResolveHashProfile(SourceProfile(), nullptr).empty());
}

TEST(HashProfileResolver, RejectsInvalidUtf8AndUnsupportedSelectors) {
    for (const std::string seed :
         {std::string("\xc0\xaf", 2), std::string("\xed\xa0\x80", 3)}) {
        HashProfile resolved;
        const std::string error =
            ResolveHashProfile(SourceProfile(seed), &resolved);
        EXPECT_NE(error.find("valid UTF-8"), std::string::npos);
    }

    std::vector<HashProfileConfig> unsupported;
    auto source = SourceProfile();
    source.strategy = "vllm_v2";
    unsupported.push_back(source);
    source = SourceProfile();
    source.algorithm = "md5";
    unsupported.push_back(source);
    source = SourceProfile();
    source.algorithm = "sha512";
    unsupported.push_back(source);
    source = SourceProfile();
    source.algorithm = "xxhash";
    unsupported.push_back(source);
    source = SourceProfile();
    source.algorithm = "SHA256";
    unsupported.push_back(source);
    source = SourceProfile();
    source.algorithm = "";
    unsupported.push_back(source);
    source = SourceProfile();
    source.index_projection = "high64_be";
    unsupported.push_back(source);

    for (const HashProfileConfig& candidate : unsupported) {
        HashProfile resolved;
        EXPECT_FALSE(ResolveHashProfile(candidate, &resolved).empty());
        EXPECT_EQ(resolved, HashProfile{});
    }
}

TEST(HashProfile, AcceptsOnlyTheSupportedResolvedProfile) {
    for (const HashProfile& profile : {ValidProfile(), ValidPickleProfile()}) {
        SCOPED_TRACE(profile.algorithm);
        EXPECT_TRUE(ValidateHashProfile(profile).empty());

        std::string error = "stale error";
        auto strategy = CreateHashStrategy(profile, &error);
        EXPECT_NE(strategy, nullptr);
        EXPECT_TRUE(error.empty());
        EXPECT_NE(CreateHashStrategy(profile, nullptr), nullptr);
    }
}

TEST(HashProfile, RejectsUnsupportedAndMalformedResolvedShapes) {
    std::vector<std::pair<std::string, HashProfile>> cases;

    HashProfile profile = ValidProfile();
    profile.strategy = "vllm_v2";
    cases.emplace_back("strategy", profile);

    profile = ValidProfile();
    profile.algorithm = "md5";
    cases.emplace_back("algorithm", profile);

    profile = ValidProfile();
    profile.index_projection = "high64_be";
    cases.emplace_back("projection", profile);

    profile = ValidProfile();
    profile.python_hash_seed.clear();
    cases.emplace_back("empty seed", profile);

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

TEST(HashProfile, SemanticValidationRejectsForgedSeedRootPair) {
    HashProfile forged = ValidProfile();
    forged.root_digest = kPaddedSeedZeroRoot;

    const std::string validation_error = ValidateHashProfile(forged);
    EXPECT_NE(validation_error.find("does not match"), std::string::npos);

    std::string factory_error = "stale error";
    EXPECT_NE(CreateHashStrategy(forged, &factory_error), nullptr);
    EXPECT_TRUE(factory_error.empty());
}

TEST(HashProfile, SemanticValidationUsesTheSelectedRecipe) {
    // A root derived with one recipe must not validate under the other
    // algorithm even when every other field is well formed.
    HashProfile mismatched = ValidPickleProfile();
    mismatched.algorithm = "sha256_cbor";
    EXPECT_NE(ValidateHashProfile(mismatched).find("does not match"),
              std::string::npos);

    mismatched = ValidProfile();
    mismatched.algorithm = "sha256";
    EXPECT_NE(ValidateHashProfile(mismatched).find("does not match"),
              std::string::npos);
}

TEST(HashStrategyGolden, MatchesVllmAndCbor2Vectors) {
    const Json::Value fixture = LoadJsonFixture("hash_golden_vectors.json");
    const HashProfile profile = ProfileFrom(fixture["profile"]);
    ASSERT_TRUE(ValidateHashProfile(profile).empty());

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

TEST(HashStrategyGolden, MatchesVllmPickleVectors) {
    const Json::Value fixture =
        LoadJsonFixture("hash_golden_vectors_sha256.json");
    const HashProfile profile = ProfileFrom(fixture["profile"]);
    ASSERT_EQ(profile.algorithm, "sha256");
    ASSERT_TRUE(ValidateHashProfile(profile).empty());

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
            .lora_name = CaseLora(test_case),
            .block_size = test_case["block_size"].asInt64(),
        };
        const std::vector<int32_t> tokens = CaseTokens(test_case);

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
            if (expected[index].isMember("pickle_hex")) {
                // The fixture pins the exact serialized Pickle bytes; their
                // SHA-256 must reproduce the pinned digest.
                EXPECT_EQ(Sha256Hex(HexToBytes(
                              expected[index]["pickle_hex"].asString())),
                          expected[index]["digest"].asString())
                    << "block=" << index;
            }
        }
    }
}

TEST(HashStrategyGolden, PickleMultiBlockChainDoesNotReuseLow64AsParent) {
    const Json::Value fixture =
        LoadJsonFixture("hash_golden_vectors_sha256.json");
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
    const std::vector<int32_t> tokens = CaseTokens(test_case);
    std::vector<HashBlock> blocks;
    ASSERT_TRUE(
        strategy->Compute(context, tokens, std::nullopt, &blocks).empty());
    ASSERT_EQ(blocks.size(), 2u);

    const std::string second_digest = DigestToHex(blocks[1].digest);
    EXPECT_EQ(second_digest, test_case["expected"][1]["digest"].asString());
    EXPECT_NE(second_digest,
              test_case["incorrect_low64_parent_digest"].asString());
}

TEST(HashStrategyGolden, AlgorithmsProduceDistinctDigestsForIdenticalTokens) {
    const Json::Value cbor_fixture =
        LoadJsonFixture("hash_golden_vectors.json");
    const Json::Value pickle_fixture =
        LoadJsonFixture("hash_golden_vectors_sha256.json");

    std::string factory_error;
    auto cbor_strategy = CreateHashStrategy(
        ProfileFrom(cbor_fixture["profile"]), &factory_error);
    ASSERT_NE(cbor_strategy, nullptr) << factory_error;
    auto pickle_strategy = CreateHashStrategy(
        ProfileFrom(pickle_fixture["profile"]), &factory_error);
    ASSERT_NE(pickle_strategy, nullptr) << factory_error;

    ContextKey context{
        .tenant_id = "default",
        .model_name = "golden-model",
        .lora_name = "",
        .block_size = 4,
    };
    const std::vector<int32_t> tokens{1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<HashBlock> cbor_blocks;
    std::vector<HashBlock> pickle_blocks;
    ASSERT_TRUE(
        cbor_strategy->Compute(context, tokens, std::nullopt, &cbor_blocks)
            .empty());
    ASSERT_TRUE(
        pickle_strategy->Compute(context, tokens, std::nullopt, &pickle_blocks)
            .empty());
    ASSERT_EQ(cbor_blocks.size(), pickle_blocks.size());

    // Identical tokens under the two recipes must produce distinct digests
    // while each recipe matches its own vLLM reference vectors.
    for (size_t index = 0; index < cbor_blocks.size(); ++index) {
        EXPECT_NE(cbor_blocks[index], pickle_blocks[index])
            << "block=" << index;
        EXPECT_EQ(DigestToHex(cbor_blocks[index].digest),
                  cbor_fixture["cases"][0]["expected"]
                              [static_cast<Json::ArrayIndex>(index)]["digest"]
                                  .asString());
        EXPECT_EQ(DigestToHex(pickle_blocks[index].digest),
                  pickle_fixture["cases"][0]["expected"]
                                [static_cast<Json::ArrayIndex>(index)]["digest"]
                                    .asString());
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
    for (const HashProfile& profile : {ValidProfile(), ValidPickleProfile()}) {
        SCOPED_TRACE(profile.algorithm);
        std::string factory_error;
        auto strategy = CreateHashStrategy(profile, &factory_error);
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
        ASSERT_TRUE(
            strategy->Compute(context, tokens, std::nullopt, &omitted_salt)
                .empty());
        ASSERT_TRUE(
            strategy->Compute(context, tokens, std::string{}, &empty_salt)
                .empty());
        EXPECT_EQ(empty_salt, omitted_salt);
    }
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

TEST(HashStrategy, ResolvedSeedChangesTheChain) {
    const HashProfile alternate_profile = ResolvedProfile("00");

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

TEST(HashChain, MatchesEagerCompute) {
    for (const HashProfile& profile : {ValidProfile(), ValidPickleProfile()}) {
        SCOPED_TRACE(profile.algorithm);
        std::string factory_error;
        auto strategy = CreateHashStrategy(profile, &factory_error);
        ASSERT_NE(strategy, nullptr) << factory_error;

        ContextKey context{
            .tenant_id = "default",
            .model_name = "model",
            .lora_name = "lora-a",
            .block_size = 4,
        };
        const std::vector<int32_t> tokens{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        const std::string salt = "salty";

        std::vector<HashBlock> eager;
        ASSERT_TRUE(strategy->Compute(context, tokens, salt, &eager).empty());
        ASSERT_EQ(eager.size(), 2u);

        std::string chain_error;
        auto chain = strategy->CreateChain(context, tokens, salt, &chain_error);
        ASSERT_NE(chain, nullptr) << chain_error;
        EXPECT_EQ(chain->BlockCount(), eager.size());
        for (size_t index = 0; index < eager.size(); ++index) {
            const HashBlock* block = chain->At(index, &chain_error);
            ASSERT_NE(block, nullptr) << chain_error;
            EXPECT_EQ(*block, eager[index]) << "block=" << index;
        }
    }
}

TEST(HashChain, ComputesOnlyRequestedPrefix) {
    std::string factory_error;
    auto strategy = CreateHashStrategy(ValidProfile(), &factory_error);
    ASSERT_NE(strategy, nullptr) << factory_error;

    ContextKey context{
        .tenant_id = "default",
        .model_name = "model",
        .lora_name = "",
        .block_size = 4,
    };
    const std::vector<int32_t> tokens(400, 7);  // 100 complete blocks

    std::string chain_error;
    auto chain =
        strategy->CreateChain(context, tokens, std::nullopt, &chain_error);
    ASSERT_NE(chain, nullptr) << chain_error;
    EXPECT_EQ(chain->BlockCount(), 100u);
    EXPECT_EQ(chain->ComputedCount(), 0u);

    ASSERT_NE(chain->At(0, &chain_error), nullptr) << chain_error;
    EXPECT_EQ(chain->ComputedCount(), 1u);

    ASSERT_NE(chain->At(2, &chain_error), nullptr) << chain_error;
    EXPECT_EQ(chain->ComputedCount(), 3u);

    // Already-computed blocks must not be rehashed.
    ASSERT_NE(chain->At(2, &chain_error), nullptr) << chain_error;
    EXPECT_EQ(chain->ComputedCount(), 3u);

    EXPECT_EQ(chain->At(100, &chain_error), nullptr);
    EXPECT_FALSE(chain_error.empty());
    EXPECT_EQ(chain->ComputedCount(), 3u);
}

TEST(HashChain, RejectsInvalidInputsAtSetup) {
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

    std::string chain_error;
    EXPECT_EQ(
        strategy->CreateChain(context, tokens, std::nullopt, &chain_error),
        nullptr);
    EXPECT_FALSE(chain_error.empty());

    context.block_size = 4;
    const std::string invalid_salt("\xed\xa0\x80", 3);
    EXPECT_EQ(
        strategy->CreateChain(context, tokens, invalid_salt, &chain_error),
        nullptr);
}

TEST(SglangHashChain, MatchesNativeSha256TokenChain) {
    HashProfile profile;
    const HashProfileConfig source{
        .strategy = "sglang",
        .algorithm = "sha256_raw",
        .python_hash_seed = "0",
        .index_projection = "first64_be",
    };
    ASSERT_TRUE(ResolveHashProfile(source, &profile).empty());
    ASSERT_TRUE(ValidateHashProfile(profile).empty());

    std::string error;
    auto strategy = CreateHashStrategy(profile, &error);
    ASSERT_NE(strategy, nullptr) << error;
    const ContextKey context{.tenant_id = "default",
                             .model_name = "model",
                             .lora_name = "",
                             .block_size = 4};
    const std::vector<int32_t> tokens{1, 2, 3, 4};
    std::vector<HashBlock> blocks;
    ASSERT_TRUE(strategy->Compute(context, tokens, std::nullopt, &blocks)
                    .empty());
    ASSERT_EQ(blocks.size(), 1u);
    EXPECT_EQ(blocks[0].projected.value, 0xcf97adeedb59e05bULL);

    blocks.clear();
    ASSERT_TRUE(strategy->Compute(context, tokens, std::string("salty"),
                                   &blocks)
                    .empty());
    ASSERT_EQ(blocks.size(), 1u);
    EXPECT_EQ(blocks[0].projected.value, 0xd98f7292c18ec8ddULL);
}

TEST(SglangHashChain, IncludesPartialFinalBlock) {
    HashProfile profile;
    const HashProfileConfig source{
        .strategy = "sglang",
        .algorithm = "sha256_raw",
        .python_hash_seed = "0",
        .index_projection = "first64_be",
    };
    ASSERT_TRUE(ResolveHashProfile(source, &profile).empty());

    std::string error;
    auto strategy = CreateHashStrategy(profile, &error);
    ASSERT_NE(strategy, nullptr) << error;
    const ContextKey context{.tenant_id = "default",
                             .model_name = "model",
                             .lora_name = "",
                             .block_size = 2};
    const std::vector<int32_t> tokens{1, 2, 3};
    std::vector<HashBlock> blocks;
    ASSERT_TRUE(strategy->Compute(context, tokens, std::nullopt, &blocks)
                    .empty());
    EXPECT_EQ(blocks.size(), 2u);
}

TEST(SglangHashChain, MatchesBigramGolden) {
    HashProfile profile;
    const HashProfileConfig source{
        .strategy = "sglang_bigram",
        .algorithm = "sha256_raw",
        .python_hash_seed = "0",
        .index_projection = "first64_be",
    };
    ASSERT_TRUE(ResolveHashProfile(source, &profile).empty());
    ASSERT_TRUE(ValidateHashProfile(profile).empty());

    std::string error;
    auto strategy = CreateHashStrategy(profile, &error);
    ASSERT_NE(strategy, nullptr) << error;
    const ContextKey context{.tenant_id = "default",
                             .model_name = "model",
                             .lora_name = "",
                             .block_size = 4};
    const std::vector<int32_t> tokens{10, 20, 30, 40};
    std::vector<HashBlock> blocks;
    ASSERT_TRUE(strategy->Compute(context, tokens, std::nullopt, &blocks)
                    .empty());
    ASSERT_EQ(blocks.size(), 1u);
    EXPECT_EQ(blocks[0].projected.value, 15710792592378487421ULL);
}

}  // namespace
