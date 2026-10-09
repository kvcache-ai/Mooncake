#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "conductor/prefixindex/hash_strategy.h"
#include "conductor/prefixindex/prefix_indexer.h"
#include "prefix_indexer_test_peer.h"

namespace {

using mooncake::conductor::prefixindex::BlockPresenceSnapshot;
using mooncake::conductor::prefixindex::CacheHitResult;
using mooncake::conductor::prefixindex::ContextKey;
using mooncake::conductor::prefixindex::EngineClear;
using mooncake::conductor::prefixindex::EngineMutation;
using mooncake::conductor::prefixindex::EngineOwner;
using mooncake::conductor::prefixindex::EngineRegistration;
using mooncake::conductor::prefixindex::HashBlock;
using mooncake::conductor::prefixindex::HashProfile;
using mooncake::conductor::prefixindex::PrefixCacheTable;
using mooncake::conductor::prefixindex::PrefixCacheTableSnapshot;
using mooncake::conductor::prefixindex::PrefixCacheTableTestPeer;
using mooncake::conductor::prefixindex::ProjectedPrefix;
using mooncake::conductor::prefixindex::RankCacheHitResult;
using mooncake::conductor::prefixindex::SharedClear;
using mooncake::conductor::prefixindex::SharedMutation;
using mooncake::conductor::prefixindex::SharedObjectOwner;
using mooncake::conductor::prefixindex::StorageTier;

constexpr char kRootDigest[] =
    "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e";
constexpr char kPaddedSeedRootDigest[] =
    "8d912e4e62b3cc377b1d1c7a14ef61dffbdaa0990237035c05401c29414c4172";
constexpr char kPickleRootDigest[] =
    "1973e23848344dc43a988a9b478663803cfffe1243480253f9a3cf004b14aa7c";

ContextKey TestContext(int64_t block_size = 16) {
    return {.tenant_id = "tenant-a",
            .model_name = "model-a",
            .lora_name = "",
            .block_size = block_size};
}

HashProfile TestProfile() {
    return {.strategy = "vllm_v1",
            .algorithm = "sha256_cbor",
            .python_hash_seed = "0",
            .root_digest = kRootDigest,
            .index_projection = "low64_be"};
}

HashProfile PaddedSeedProfile() {
    return {.strategy = "vllm_v1",
            .algorithm = "sha256_cbor",
            .python_hash_seed = "00",
            .root_digest = kPaddedSeedRootDigest,
            .index_projection = "low64_be"};
}

HashProfile PickleProfile() {
    return {.strategy = "vllm_v1",
            .algorithm = "sha256",
            .python_hash_seed = "0",
            .root_digest = kPickleRootDigest,
            .index_projection = "low64_be"};
}

// SGLang chains carry no Python seed root, so the resolver pins an all-zero
// sentinel digest for the registration wire contract.
HashProfile SglangProfile() {
    return {.strategy = "sglang",
            .algorithm = "sha256_raw",
            .python_hash_seed = "0",
            .root_digest = std::string(64, '0'),
            .index_projection = "first64_be"};
}

// The bigram chains hash token pairs, so they carry one fewer logical position
// than the prompt has tokens.
HashProfile SglangBigramProfile() {
    return {.strategy = "sglang_bigram",
            .algorithm = "sha256_raw",
            .python_hash_seed = "0",
            .root_digest = std::string(64, '0'),
            .index_projection = "first64_be"};
}

EngineRegistration Registration(const std::string& instance_id = "instance-a",
                                int64_t dp_rank = 0) {
    const ContextKey context = TestContext();
    return {.context = context,
            .profile = TestProfile(),
            .instance_id = instance_id,
            .dp_rank = dp_rank,
            .effective_block_size = context.block_size,
            .cache_group = 0};
}

EngineOwner GpuOwner(const std::string& instance_id = "instance-a",
                     int64_t dp_rank = 0,
                     const std::string& stream = "stream-a") {
    return {.source_stream = stream,
            .instance_id = instance_id,
            .dp_rank = dp_rank};
}

SharedObjectOwner SharedOwner(const std::string& object_id = "object-a",
                              const std::string& stream = "pool-stream",
                              const std::string& backend = "backend-a") {
    return {
        .source_stream = stream, .backend_id = backend, .object_id = object_id};
}

ProjectedPrefix Prefix(uint64_t value) { return {.value = value}; }

RankCacheHitResult RankMatch(int64_t npu, int64_t local, int64_t shared,
                             int64_t disk) {
    return {.npu = npu, .cpu_local = local, .cpu_share = shared, .disk = disk};
}

EngineMutation Gpu(const std::vector<ProjectedPrefix>& prefixes,
                   EngineOwner owner = GpuOwner()) {
    const ContextKey context = TestContext();
    return {.context = context,
            .prefixes = prefixes,
            .owner = std::move(owner),
            .effective_block_size = context.block_size,
            .cache_group = 0};
}

SharedMutation Shared(const std::vector<ProjectedPrefix>& prefixes,
                      StorageTier tier,
                      SharedObjectOwner owner = SharedOwner()) {
    const ContextKey context = TestContext();
    return {.context = context,
            .prefixes = prefixes,
            .tier = tier,
            .owner = std::move(owner),
            .effective_block_size = context.block_size,
            .cache_group = 0};
}

EngineClear ClearFor(EngineOwner owner = GpuOwner()) {
    const ContextKey context = TestContext();
    return {.context = context,
            .owner = std::move(owner),
            .effective_block_size = context.block_size,
            .cache_group = 0};
}

SharedClear ClearFor(SharedObjectOwner owner,
                     std::optional<StorageTier> tier = std::nullopt) {
    const ContextKey context = TestContext();
    return {.context = context,
            .owner = std::move(owner),
            .tier = tier,
            .effective_block_size = context.block_size,
            .cache_group = 0};
}

std::vector<int32_t> Tokens(size_t count) {
    std::vector<int32_t> tokens;
    tokens.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        tokens.push_back(static_cast<int32_t>(i + 1));
    }
    return tokens;
}

std::vector<ProjectedPrefix> Hashes(
    const std::vector<int32_t>& tokens,
    std::optional<std::string> cache_salt = std::nullopt) {
    std::string error;
    auto strategy = mooncake::conductor::prefixindex::CreateHashStrategy(
        TestProfile(), &error);
    EXPECT_TRUE(error.empty()) << error;
    if (!strategy) {
        return {};
    }

    std::vector<HashBlock> blocks;
    error = strategy->Compute(TestContext(), tokens, std::move(cache_salt),
                              &blocks);
    EXPECT_TRUE(error.empty()) << error;

    std::vector<ProjectedPrefix> prefixes;
    prefixes.reserve(blocks.size());
    for (const HashBlock& block : blocks) {
        prefixes.push_back(block.projected);
    }
    return prefixes;
}

std::vector<ProjectedPrefix> SglangHashes(const std::vector<int32_t>& tokens) {
    std::string error;
    auto strategy = mooncake::conductor::prefixindex::CreateHashStrategy(
        SglangProfile(), &error);
    EXPECT_TRUE(error.empty()) << error;
    if (!strategy) {
        return {};
    }

    std::vector<HashBlock> blocks;
    error = strategy->Compute(TestContext(), tokens, std::nullopt, &blocks);
    EXPECT_TRUE(error.empty()) << error;

    std::vector<ProjectedPrefix> prefixes;
    prefixes.reserve(blocks.size());
    for (const HashBlock& block : blocks) {
        prefixes.push_back(block.projected);
    }
    return prefixes;
}

std::vector<ProjectedPrefix> SglangBigramHashes(
    const std::vector<int32_t>& tokens) {
    std::string error;
    auto strategy = mooncake::conductor::prefixindex::CreateHashStrategy(
        SglangBigramProfile(), &error);
    EXPECT_TRUE(error.empty()) << error;
    if (!strategy) {
        return {};
    }

    std::vector<HashBlock> blocks;
    error = strategy->Compute(TestContext(), tokens, std::nullopt, &blocks);
    EXPECT_TRUE(error.empty()) << error;

    std::vector<ProjectedPrefix> prefixes;
    prefixes.reserve(blocks.size());
    for (const HashBlock& block : blocks) {
        prefixes.push_back(block.projected);
    }
    return prefixes;
}

void RegisterOrFail(PrefixCacheTable& table,
                    const EngineRegistration& registration) {
    const auto result = table.Register(registration);
    ASSERT_TRUE(result.error.empty()) << result.error;
}

BlockPresenceSnapshot Presence(const PrefixCacheTable& table,
                               ProjectedPrefix prefix) {
    const PrefixCacheTableSnapshot table_snapshot =
        PrefixCacheTableTestPeer::Snapshot(table);
    return table_snapshot.contexts.at(TestContext()).blocks.at(prefix);
}

TEST(Registration, InvalidInputsDoNotCreateContextState) {
    std::vector<EngineRegistration> invalid;

    auto non_positive = Registration();
    non_positive.context.block_size = 0;
    non_positive.effective_block_size = 0;
    invalid.push_back(non_positive);

    auto mismatch = Registration();
    mismatch.effective_block_size = 8;
    invalid.push_back(mismatch);

    auto unsupported_group = Registration();
    unsupported_group.cache_group = 1;
    invalid.push_back(unsupported_group);

    auto empty_instance = Registration();
    empty_instance.instance_id.clear();
    invalid.push_back(empty_instance);

    auto negative_rank = Registration();
    negative_rank.dp_rank = -1;
    invalid.push_back(negative_rank);

    auto malformed_profile = Registration();
    malformed_profile.profile.root_digest = "not-a-digest";
    invalid.push_back(malformed_profile);

    PrefixCacheTable table;
    for (const auto& registration : invalid) {
        SCOPED_TRACE(registration.instance_id);
        const auto validation =
            PrefixCacheTable::ValidateRegistration(registration);
        EXPECT_FALSE(validation.error.empty());
        const auto result = table.Register(registration);
        EXPECT_FALSE(result.error.empty());
        EXPECT_FALSE(result.inserted);
    }
    EXPECT_EQ(table.GetGlobalView().context_count, 0);
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table).contexts.empty());
}

TEST(Registration, ForgedSeedRootPairIsRejectedWithoutMutation) {
    PrefixCacheTable table;
    auto forged = Registration();
    forged.profile.root_digest = kPaddedSeedRootDigest;

    const auto validation = PrefixCacheTable::ValidateRegistration(forged);
    EXPECT_NE(validation.error.find("does not match"), std::string::npos);
    const auto rejected = table.Register(forged);
    EXPECT_NE(rejected.error.find("does not match"), std::string::npos);
    EXPECT_FALSE(rejected.inserted);
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table).contexts.empty());

    RegisterOrFail(table, Registration());
    const auto registered = PrefixCacheTableTestPeer::Snapshot(table);
    EXPECT_NE(table.ValidateProfileBinding(TestContext(), forged.profile)
                  .find("does not match"),
              std::string::npos);
    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), registered);

    forged.instance_id = "instance-b";
    const auto conflicting = table.Register(forged);
    EXPECT_NE(conflicting.error.find("does not match"), std::string::npos);
    EXPECT_FALSE(conflicting.inserted);
    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), registered);
}

TEST(Registration, SglangSeedVariantsShareContextAndBinding) {
    for (const auto& profile : {SglangProfile(), SglangBigramProfile()}) {
        PrefixCacheTable table;
        auto first = Registration();
        first.profile = profile;
        ASSERT_TRUE(table.Register(first).error.empty());
        auto second = Registration("instance-b", 1);
        second.profile = profile;
        second.profile.python_hash_seed = "different-unused-seed";
        ASSERT_TRUE(table.Register(second).error.empty());
        EXPECT_TRUE(table.ValidateProfileBinding(TestContext(), second.profile)
                        .empty());
        auto snapshot = PrefixCacheTableTestPeer::Snapshot(table);
        EXPECT_EQ(snapshot.contexts.size(), 1u);
        EXPECT_EQ(snapshot.contexts.at(TestContext()).instance_ranks.size(),
                  2u);
        auto conflict = second;
        conflict.profile.root_digest = std::string(64, '1');
        EXPECT_FALSE(table.Register(conflict).error.empty());
        EXPECT_FALSE(
            table.ValidateProfileBinding(TestContext(), conflict.profile)
                .empty());
        EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), snapshot);
    }
}

TEST(Registration, TracksEveryInstanceAndRankIdempotently) {
    PrefixCacheTable table;

    auto first = table.Register(Registration("instance-a", 0));
    ASSERT_TRUE(first.error.empty()) << first.error;
    EXPECT_TRUE(first.inserted);

    auto duplicate = table.Register(Registration("instance-a", 0));
    ASSERT_TRUE(duplicate.error.empty()) << duplicate.error;
    EXPECT_FALSE(duplicate.inserted);

    auto omitted_group = Registration("instance-a", 0);
    omitted_group.cache_group.reset();
    auto omitted_duplicate = table.Register(omitted_group);
    ASSERT_TRUE(omitted_duplicate.error.empty()) << omitted_duplicate.error;
    EXPECT_FALSE(omitted_duplicate.inserted);

    auto second_rank = table.Register(Registration("instance-a", 2));
    ASSERT_TRUE(second_rank.error.empty()) << second_rank.error;
    EXPECT_TRUE(second_rank.inserted);

    auto second_instance = table.Register(Registration("instance-b", 1));
    ASSERT_TRUE(second_instance.error.empty()) << second_instance.error;
    EXPECT_TRUE(second_instance.inserted);

    const auto snapshot = PrefixCacheTableTestPeer::Snapshot(table);
    ASSERT_EQ(snapshot.contexts.size(), 1u);
    const auto& state = snapshot.contexts.at(TestContext());
    EXPECT_EQ(state.profile, TestProfile());
    EXPECT_EQ(state.instance_ranks.at("instance-a"), (std::set<int64_t>{0, 2}));
    EXPECT_EQ(state.instance_ranks.at("instance-b"), (std::set<int64_t>{1}));
    EXPECT_TRUE(state.blocks.empty());
}

TEST(Registration, ConflictingProfilePreservesCompleteState) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    ASSERT_EQ(table.StoreEngine(Gpu({Prefix(1)})), "");
    const auto before = PrefixCacheTableTestPeer::Snapshot(table);

    auto conflicting = Registration("instance-b", 1);
    conflicting.profile = PaddedSeedProfile();
    const auto result = table.Register(conflicting);

    EXPECT_FALSE(result.error.empty());
    EXPECT_FALSE(result.inserted);
    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), before);
}

TEST(Registration, ProfileBindingValidationIsExactAndLookupOnly) {
    PrefixCacheTable table;
    const auto empty_before = PrefixCacheTableTestPeer::Snapshot(table);

    EXPECT_FALSE(
        table.ValidateProfileBinding(TestContext(), TestProfile()).empty());
    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), empty_before);

    RegisterOrFail(table, Registration());
    const auto registered = PrefixCacheTableTestPeer::Snapshot(table);
    EXPECT_EQ(table.ValidateProfileBinding(TestContext(), TestProfile()), "");

    const HashProfile conflict = PaddedSeedProfile();
    EXPECT_FALSE(table.ValidateProfileBinding(TestContext(), conflict).empty());
    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), registered);
}

TEST(Registration, MixedAlgorithmsUnderOneContextAreRejected) {
    // The resolved profile is immutable per ContextKey: the same seed under
    // the other supported algorithm is still a conflict, in both orders.
    {
        PrefixCacheTable table;
        RegisterOrFail(table, Registration());
        const auto before = PrefixCacheTableTestPeer::Snapshot(table);

        auto conflicting = Registration("instance-b", 1);
        conflicting.profile = PickleProfile();
        const auto result = table.Register(conflicting);
        EXPECT_FALSE(result.error.empty());
        EXPECT_FALSE(result.inserted);
        EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), before);

        EXPECT_FALSE(
            table.ValidateProfileBinding(TestContext(), PickleProfile())
                .empty());
        EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), before);
    }
    {
        PrefixCacheTable table;
        auto pickle_registration = Registration();
        pickle_registration.profile = PickleProfile();
        RegisterOrFail(table, pickle_registration);
        const auto before = PrefixCacheTableTestPeer::Snapshot(table);
        EXPECT_EQ(table.ValidateProfileBinding(TestContext(), PickleProfile()),
                  "");

        auto conflicting = Registration("instance-b", 1);
        const auto result = table.Register(conflicting);
        EXPECT_FALSE(result.error.empty());
        EXPECT_FALSE(result.inserted);
        EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), before);
    }
}

TEST(Mutations, StoreRequiresKnownContextAndRegisteredGpuRank) {
    PrefixCacheTable table;
    const auto gpu = Gpu({Prefix(1)});
    const auto shared = Shared({Prefix(1)}, StorageTier::kCpuShare);

    EXPECT_FALSE(table.StoreEngine(gpu).empty());
    EXPECT_FALSE(table.StoreShared(shared).empty());
    EXPECT_EQ(table.GetGlobalView().context_count, 0);

    RegisterOrFail(table, Registration("instance-a", 1));
    EXPECT_FALSE(table.StoreEngine(gpu).empty());
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table)
                    .contexts.at(TestContext())
                    .blocks.empty());
}

TEST(Mutations, InvalidGroupTierAndOwnersPreserveState) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    ASSERT_EQ(table.StoreEngine(Gpu({Prefix(1)})), "");
    const auto before = PrefixCacheTableTestPeer::Snapshot(table);

    auto bad_group = Gpu({Prefix(2)});
    bad_group.cache_group = 3;
    EXPECT_FALSE(table.StoreEngine(bad_group).empty());

    auto bad_owner = Gpu({Prefix(2)});
    bad_owner.owner.source_stream.clear();
    EXPECT_FALSE(table.StoreEngine(bad_owner).empty());

    auto bad_tier = Shared({Prefix(2)}, StorageTier::kNpu);
    EXPECT_FALSE(table.StoreShared(bad_tier).empty());

    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), before);
}

TEST(Mutations, DuplicateGpuStoreAndRemoveAreIdempotent) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const ProjectedPrefix prefix = Prefix(7);
    const auto mutation = Gpu({prefix});

    ASSERT_EQ(table.StoreEngine(mutation), "");
    ASSERT_EQ(table.StoreEngine(mutation), "");
    EXPECT_EQ(Presence(table, prefix).npu_owners,
              (std::set<EngineOwner>{GpuOwner()}));

    auto absent_owner = Gpu({prefix}, GpuOwner("instance-b", 0, "stream-b"));
    ASSERT_EQ(table.RemoveEngine(absent_owner), "");
    EXPECT_EQ(Presence(table, prefix).npu_owners,
              (std::set<EngineOwner>{GpuOwner()}));

    ASSERT_EQ(table.RemoveEngine(mutation), "");
    ASSERT_EQ(table.RemoveEngine(mutation), "");
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table)
                    .contexts.at(TestContext())
                    .blocks.empty());
}

TEST(Mutations, CollidingSharedOwnersRemainIndependentlyRemovable) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const ProjectedPrefix collision = Prefix(0x123456789abcdef0ULL);
    const SharedObjectOwner first = SharedOwner("object-a");
    const SharedObjectOwner second = SharedOwner("object-b");

    ASSERT_EQ(
        table.StoreShared(Shared({collision}, StorageTier::kCpuShare, first)),
        "");
    ASSERT_EQ(
        table.StoreShared(Shared({collision}, StorageTier::kCpuShare, second)),
        "");
    ASSERT_EQ(
        table.StoreShared(Shared({collision}, StorageTier::kCpuShare, first)),
        "");
    EXPECT_EQ(Presence(table, collision).cpu_share_owners,
              (std::set<SharedObjectOwner>{first, second}));

    ASSERT_EQ(
        table.RemoveShared(Shared({collision}, StorageTier::kCpuShare, first)),
        "");
    EXPECT_EQ(Presence(table, collision).cpu_share_owners,
              (std::set<SharedObjectOwner>{second}));

    ASSERT_EQ(
        table.RemoveShared(Shared({collision}, StorageTier::kCpuShare, second)),
        "");
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table)
                    .contexts.at(TestContext())
                    .blocks.empty());
}

TEST(Mutations, BlockLivesUntilEveryTierOwnerSetIsEmpty) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const ProjectedPrefix prefix = Prefix(11);
    const auto gpu = Gpu({prefix});
    const auto cpu =
        Shared({prefix}, StorageTier::kCpuShare, SharedOwner("cpu"));
    const auto disk = Shared({prefix}, StorageTier::kDisk, SharedOwner("disk"));

    ASSERT_EQ(table.StoreEngine(gpu), "");
    ASSERT_EQ(table.StoreShared(cpu), "");
    ASSERT_EQ(table.StoreShared(disk), "");
    ASSERT_EQ(table.RemoveEngine(gpu), "");
    EXPECT_TRUE(Presence(table, prefix).npu_owners.empty());
    EXPECT_FALSE(Presence(table, prefix).cpu_share_owners.empty());
    EXPECT_FALSE(Presence(table, prefix).disk_owners.empty());

    ASSERT_EQ(table.RemoveShared(cpu), "");
    EXPECT_TRUE(Presence(table, prefix).cpu_share_owners.empty());
    EXPECT_FALSE(Presence(table, prefix).disk_owners.empty());

    ASSERT_EQ(table.RemoveShared(disk), "");
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table)
                    .contexts.at(TestContext())
                    .blocks.empty());
}

TEST(Mutations, GpuAndSharedClearAreExactlyOwnerScoped) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-a", 0));
    RegisterOrFail(table, Registration("instance-b", 1));
    const ProjectedPrefix prefix = Prefix(21);
    const EngineOwner engine_a = GpuOwner("instance-a", 0, "stream-a");
    const EngineOwner engine_a_other_stream =
        GpuOwner("instance-a", 0, "stream-a-other");
    const EngineOwner engine_b = GpuOwner("instance-b", 1, "stream-b");
    const SharedObjectOwner shared_a = SharedOwner("object-a");
    const SharedObjectOwner shared_b = SharedOwner("object-b");

    ASSERT_EQ(table.StoreEngine(Gpu({prefix}, engine_a)), "");
    ASSERT_EQ(table.StoreEngine(Gpu({prefix}, engine_a_other_stream)), "");
    ASSERT_EQ(table.StoreEngine(Gpu({prefix}, engine_b)), "");
    ASSERT_EQ(
        table.StoreShared(Shared({prefix}, StorageTier::kCpuShare, shared_a)),
        "");
    ASSERT_EQ(table.StoreShared(Shared({prefix}, StorageTier::kDisk, shared_a)),
              "");
    ASSERT_EQ(
        table.StoreShared(Shared({prefix}, StorageTier::kCpuShare, shared_b)),
        "");

    ASSERT_EQ(table.ClearEngine(ClearFor(engine_a)), "");
    EXPECT_EQ(Presence(table, prefix).npu_owners,
              (std::set<EngineOwner>{engine_a_other_stream, engine_b}));
    EXPECT_EQ(Presence(table, prefix).cpu_share_owners,
              (std::set<SharedObjectOwner>{shared_a, shared_b}));

    ASSERT_EQ(table.ClearShared(ClearFor(shared_a, StorageTier::kCpuShare)),
              "");
    EXPECT_EQ(Presence(table, prefix).cpu_share_owners,
              (std::set<SharedObjectOwner>{shared_b}));
    EXPECT_EQ(
        Presence(table, prefix).disk_owners,
        (std::set<mooncake::conductor::prefixindex::TierOwner>{shared_a}));
    EXPECT_EQ(Presence(table, prefix).npu_owners,
              (std::set<EngineOwner>{engine_a_other_stream, engine_b}));

    ASSERT_EQ(table.ClearShared(ClearFor(shared_a)), "");
    EXPECT_TRUE(Presence(table, prefix).disk_owners.empty());
    EXPECT_EQ(Presence(table, prefix).npu_owners,
              (std::set<EngineOwner>{engine_a_other_stream, engine_b}));
}

TEST(Mutations, UnknownRemoveClearAndUnregisterNeverCreateState) {
    PrefixCacheTable table;
    const ContextKey context = TestContext();

    EXPECT_EQ(table.RemoveEngine(Gpu({Prefix(1)})), "");
    EXPECT_EQ(table.ClearEngine(ClearFor()), "");
    EXPECT_EQ(table.RemoveShared(Shared({Prefix(1)}, StorageTier::kCpuShare)),
              "");
    EXPECT_EQ(table.ClearShared(ClearFor(SharedOwner())), "");
    EXPECT_EQ(table.Unregister(context, "instance-a", 0), "");

    EXPECT_FALSE(PrefixCacheTableTestPeer::ContextExists(table, context));
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table).contexts.empty());
}

TEST(Unregister, RemovesOnlySelectedRankGpuOwners) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-a", 0));
    RegisterOrFail(table, Registration("instance-a", 1));
    RegisterOrFail(table, Registration("instance-b", 0));
    const ProjectedPrefix prefix = Prefix(31);
    const EngineOwner a0 = GpuOwner("instance-a", 0, "stream-a0");
    const EngineOwner a0_second_stream =
        GpuOwner("instance-a", 0, "stream-a0-second");
    const EngineOwner a1 = GpuOwner("instance-a", 1, "stream-a1");
    const EngineOwner b0 = GpuOwner("instance-b", 0, "stream-b0");
    const SharedObjectOwner shared = SharedOwner();

    ASSERT_EQ(table.StoreEngine(Gpu({prefix}, a0)), "");
    ASSERT_EQ(table.StoreEngine(Gpu({prefix}, a0_second_stream)), "");
    ASSERT_EQ(table.StoreEngine(Gpu({prefix}, a1)), "");
    ASSERT_EQ(table.StoreEngine(Gpu({prefix}, b0)), "");
    ASSERT_EQ(
        table.StoreShared(Shared({prefix}, StorageTier::kCpuShare, shared)),
        "");

    ASSERT_EQ(table.Unregister(TestContext(), "instance-a", 0), "");
    auto snapshot = PrefixCacheTableTestPeer::Snapshot(table);
    const auto& state = snapshot.contexts.at(TestContext());
    EXPECT_EQ(state.instance_ranks.at("instance-a"), (std::set<int64_t>{1}));
    EXPECT_EQ(state.instance_ranks.at("instance-b"), (std::set<int64_t>{0}));
    EXPECT_EQ(state.blocks.at(prefix).npu_owners,
              (std::set<EngineOwner>{a1, b0}));
    EXPECT_EQ(state.blocks.at(prefix).cpu_share_owners,
              (std::set<SharedObjectOwner>{shared}));

    ASSERT_EQ(table.Unregister(TestContext(), "instance-a", 1), "");
    snapshot = PrefixCacheTableTestPeer::Snapshot(table);
    EXPECT_FALSE(snapshot.contexts.at(TestContext())
                     .instance_ranks.contains("instance-a"));
    EXPECT_EQ(snapshot.contexts.at(TestContext()).blocks.at(prefix).npu_owners,
              (std::set<EngineOwner>{b0}));
    EXPECT_EQ(
        snapshot.contexts.at(TestContext()).blocks.at(prefix).cpu_share_owners,
        (std::set<SharedObjectOwner>{shared}));
}

TEST(Query, ExactTwoInstanceSharedCacheExample) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-1", 0));
    RegisterOrFail(table, Registration("instance-2", 1));
    const auto tokens = Tokens(48);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    ASSERT_EQ(table.StoreEngine(Gpu({hashes[0], hashes[1]},
                                    GpuOwner("instance-1", 0, "engine-1"))),
              "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kCpuShare,
                                       SharedOwner("cpu-object"))),
              "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kDisk,
                                       SharedOwner("disk-object"))),
              "");

    const auto results = table.Query(TestContext(), tokens);
    ASSERT_EQ(results.size(), 2u);

    const CacheHitResult& first = results.at("instance-1");
    EXPECT_EQ(first.longest_match_tokens, 48);
    EXPECT_EQ(first.npu, 32);
    EXPECT_EQ(first.dp, (std::map<int64_t, int64_t>{{0, 32}}));
    EXPECT_EQ(first.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                      {0, RankMatch(32, 32, 48, 48)}}));
    EXPECT_EQ(first.cpu_share, 48);
    EXPECT_EQ(first.disk, 48);

    const CacheHitResult& second = results.at("instance-2");
    EXPECT_EQ(second.longest_match_tokens, 48);
    EXPECT_EQ(second.npu, 0);
    EXPECT_EQ(second.dp, (std::map<int64_t, int64_t>{{1, 0}}));
    EXPECT_EQ(
        second.rank_matches,
        (std::map<int64_t, RankCacheHitResult>{{1, RankMatch(0, 0, 48, 48)}}));
    EXPECT_EQ(second.cpu_share, 48);
    EXPECT_EQ(second.disk, 48);
}

TEST(Query, TrailingPartialBlockNeverReportsMoreThanPromptTokens) {
    PrefixCacheTable table;
    auto registration = Registration();
    registration.profile = SglangProfile();
    RegisterOrFail(table, registration);

    // 40 tokens over a 16-token block size: two whole blocks plus a trailing
    // block that only covers 8 tokens.
    const auto tokens = Tokens(40);
    const auto hashes = SglangHashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    ASSERT_EQ(table.StoreEngine(Gpu({hashes[0], hashes[1], hashes[2]})), "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kCpuShare)), "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kDisk)), "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 40);
    EXPECT_EQ(result.npu, 40);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 40}}));
    EXPECT_EQ(result.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                       {0, RankMatch(40, 40, 40, 40)}}));
    EXPECT_EQ(result.cpu_share, 40);
    EXPECT_EQ(result.disk, 40);
}

TEST(Query, WholeBlockRunIsUnaffectedByThePromptLengthClamp) {
    PrefixCacheTable table;
    auto registration = Registration();
    registration.profile = SglangProfile();
    RegisterOrFail(table, registration);

    const auto tokens = Tokens(40);
    const auto hashes = SglangHashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    // Only the two whole blocks are indexed; the partial tail misses.
    ASSERT_EQ(
        table.StoreShared(Shared({hashes[0], hashes[1]}, StorageTier::kDisk)),
        "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 32);
    EXPECT_EQ(result.npu, 0);
    EXPECT_EQ(result.cpu_share, 0);
    EXPECT_EQ(result.disk, 32);
}

TEST(Query, BigramFullMatchReportsOneFewerPositionThanPromptTokens) {
    PrefixCacheTable table;
    auto registration = Registration();
    registration.profile = SglangBigramProfile();
    RegisterOrFail(table, registration);

    // 40 raw tokens over a 16-token block size. The bigram chain covers 39
    // logical positions, so it is two whole blocks plus a trailing block of 7.
    const auto tokens = Tokens(40);
    const auto hashes = SglangBigramHashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    ASSERT_EQ(table.StoreEngine(Gpu({hashes[0], hashes[1], hashes[2]})), "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kCpuShare)), "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kDisk)), "");

    // 39, not 40 and not the 48 a whole-block count would report.
    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 39);
    EXPECT_EQ(result.npu, 39);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 39}}));
    EXPECT_EQ(result.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                       {0, RankMatch(39, 39, 39, 39)}}));
    EXPECT_EQ(result.cpu_share, 39);
    EXPECT_EQ(result.disk, 39);
}

TEST(Query, BigramWholeBlockRunIsUnaffectedByTheLogicalLengthClamp) {
    PrefixCacheTable table;
    auto registration = Registration();
    registration.profile = SglangBigramProfile();
    RegisterOrFail(table, registration);

    const auto tokens = Tokens(40);
    const auto hashes = SglangBigramHashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    // Only the two whole blocks are indexed; the partial tail misses. The
    // clamp must not pull this below the 32 positions those blocks hold.
    ASSERT_EQ(
        table.StoreShared(Shared({hashes[0], hashes[1]}, StorageTier::kDisk)),
        "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 32);
    EXPECT_EQ(result.npu, 0);
    EXPECT_EQ(result.cpu_share, 0);
    EXPECT_EQ(result.disk, 32);
}

TEST(Query, GpuCpuAndDiskExtendOneCumulativePrefix) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const auto tokens = Tokens(64);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 4u);

    ASSERT_EQ(table.StoreEngine(Gpu({hashes[0], hashes[1]})), "");
    ASSERT_EQ(table.StoreShared(Shared({hashes[2]}, StorageTier::kCpuShare)),
              "");
    ASSERT_EQ(table.StoreShared(Shared({hashes[3]}, StorageTier::kDisk)), "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 64);
    EXPECT_EQ(result.npu, 32);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 32}}));
    EXPECT_EQ(result.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                       {0, RankMatch(32, 32, 48, 64)}}));
    EXPECT_EQ(result.cpu_share, 48);
    EXPECT_EQ(result.disk, 64);
}

TEST(Query, EmptyCpuPhaseFallsThroughToDiskAtSameBlock) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const auto tokens = Tokens(48);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    ASSERT_EQ(table.StoreEngine(Gpu({hashes[0]})), "");
    ASSERT_EQ(
        table.StoreShared(Shared({hashes[1], hashes[2]}, StorageTier::kDisk)),
        "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 48);
    EXPECT_EQ(result.npu, 16);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 16}}));
    EXPECT_EQ(result.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                       {0, RankMatch(16, 16, 16, 48)}}));
    EXPECT_EQ(result.cpu_share, 16);
    EXPECT_EQ(result.disk, 48);
}

TEST(Query, CompleteGpuCoverageCarriesThroughLowerTierBoundaries) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const auto tokens = Tokens(48);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    ASSERT_EQ(table.StoreEngine(Gpu(hashes)), "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 48);
    EXPECT_EQ(result.npu, 48);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 48}}));
    EXPECT_EQ(result.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                       {0, RankMatch(48, 48, 48, 48)}}));
    EXPECT_EQ(result.cpu_share, 48);
    EXPECT_EQ(result.disk, 48);
}

TEST(Query, DuplicateTierPresenceIsAttributedOnce) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const auto tokens = Tokens(48);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    ASSERT_EQ(table.StoreEngine(Gpu({hashes[0]})), "");
    ASSERT_EQ(table.StoreShared(
                  Shared({hashes[0], hashes[1]}, StorageTier::kCpuShare)),
              "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kDisk)), "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 48);
    EXPECT_EQ(result.npu, 16);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 16}}));
    EXPECT_EQ(result.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                       {0, RankMatch(16, 16, 32, 48)}}));
    EXPECT_EQ(result.cpu_share, 32);
    EXPECT_EQ(result.disk, 48);
}

TEST(Query, CumulativeTierIncludesHigherTierAfterLowerTierBlocks) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const auto tokens = Tokens(64);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 4u);

    ASSERT_EQ(table.StoreEngine(Gpu({hashes[0]})), "");
    ASSERT_EQ(table.StoreShared(Shared({hashes[2]}, StorageTier::kCpuShare)),
              "");
    ASSERT_EQ(
        table.StoreShared(Shared({hashes[1], hashes[3]}, StorageTier::kDisk)),
        "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 64);
    EXPECT_EQ(result.npu, 16);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 16}}));
    EXPECT_EQ(result.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                       {0, RankMatch(16, 16, 16, 64)}}));
    EXPECT_EQ(result.cpu_share, 16);
    EXPECT_EQ(result.disk, 64);
}

TEST(Query, LocalAndSharedTiersRespectRankAndContinuousPrefix) {
    PrefixCacheTable table;
    for (const auto& registration :
         {Registration(), Registration("instance-a", 1),
          Registration("instance-b")}) {
        RegisterOrFail(table, registration);
    }
    const auto tokens = Tokens(80);
    const auto hashes = Hashes(tokens);
    auto host = Gpu({hashes[0], hashes[2]});
    host.tier = StorageTier::kCpuLocal;
    ASSERT_EQ(table.StoreEngine(host), "");
    ASSERT_EQ(table.StoreEngine(host), "");
    ASSERT_EQ(table.StoreEngine(Gpu({hashes[1]})), "");
    auto disk = Gpu({hashes[3]});
    disk.tier = StorageTier::kDisk;
    ASSERT_EQ(table.StoreEngine(disk), "");
    auto hits = table.Query(TestContext(), tokens);
    EXPECT_EQ(hits.at("instance-a").rank_matches.at(0),
              RankMatch(0, 48, 48, 64));
    EXPECT_EQ(hits.at("instance-a").rank_matches.at(1), RankMatch(0, 0, 0, 0));
    EXPECT_EQ(hits.at("instance-b").cpu_share, 0);
    auto shared = Shared(hashes, StorageTier::kCpuShare);
    ASSERT_EQ(table.StoreShared(shared), "");
    EXPECT_EQ(table.Query(TestContext(), tokens).at("instance-b").cpu_share,
              80);
    ASSERT_EQ(table.RemoveShared(shared), "");
    EXPECT_EQ(table.Query(TestContext(), tokens).at("instance-a").cpu_share,
              48);
    EXPECT_EQ(table.Query(TestContext(), tokens).at("instance-b").cpu_share, 0);
    ASSERT_EQ(table.Unregister(TestContext(), "instance-a", 0), "");
    RegisterOrFail(table, Registration());
    hits = table.Query(TestContext(), tokens);
    EXPECT_EQ(hits.at("instance-a").rank_matches.at(0), RankMatch(0, 0, 0, 0));
    EXPECT_EQ(table.GetGlobalView().contexts.at(0).prefix_count, 0u);
}

TEST(Query, FourTierInterleavedPrefixesAndGaps) {
    struct Case {
        // NPU, local CPU, shared CPU, local disk, shared disk, absent.
        std::string tiers;
        RankCacheHitResult expected;
    };
    for (const auto& example :
         std::vector<Case>{{"LNSD", RankMatch(0, 32, 48, 64)},
                           {"SLND", RankMatch(0, 0, 48, 64)},
                           {"NSL-", RankMatch(16, 16, 48, 48)},
                           {"S-SN", RankMatch(0, 0, 16, 16)},
                           {"SSSS", RankMatch(0, 0, 64, 64)},
                           {"LLLL", RankMatch(0, 64, 64, 64)},
                           {"SLDS", RankMatch(0, 0, 32, 64)}}) {
        SCOPED_TRACE(example.tiers);
        PrefixCacheTable table;
        RegisterOrFail(table, Registration());
        const auto tokens = Tokens(64);
        const auto hashes = Hashes(tokens);
        for (size_t i = 0; i < hashes.size(); ++i) {
            const char tier = example.tiers[i];
            if (tier == '-') continue;
            if (tier == 'S') {
                ASSERT_EQ(table.StoreShared(
                              Shared({hashes[i]}, StorageTier::kCpuShare)),
                          "");
            } else {
                auto mutation = Gpu({hashes[i]});
                mutation.tier = tier == 'N'   ? StorageTier::kNpu
                                : tier == 'L' ? StorageTier::kCpuLocal
                                              : StorageTier::kDisk;
                ASSERT_EQ(table.StoreEngine(mutation), "");
            }
        }
        const auto result = table.Query(TestContext(), tokens).at("instance-a");
        EXPECT_EQ(result.rank_matches.at(0), example.expected);
        EXPECT_EQ(result.dp.at(0), example.expected.npu);
        EXPECT_EQ(result.npu, example.expected.npu);
        EXPECT_EQ(result.cpu_local, example.expected.cpu_local);
        EXPECT_EQ(result.cpu_share, example.expected.cpu_share);
        EXPECT_EQ(result.longest_match_tokens, example.expected.disk);
        // NSL- extends by 32 at the shared boundary, but only 16 tokens live
        // in Store: cumulative boundaries are not per-tier transfer volumes.
    }
}

TEST(Query, FourTierOwnersRemainIndependentAcrossRanksAndLifecycle) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    RegisterOrFail(table, Registration("instance-a", 1));
    RegisterOrFail(table, Registration("instance-b"));
    const auto tokens = Tokens(48);
    const auto hashes = Hashes(tokens);
    auto local = Gpu(hashes);
    local.tier = StorageTier::kCpuLocal;
    const auto shared = Shared(hashes, StorageTier::kCpuShare);
    ASSERT_EQ(table.StoreEngine(local), "");
    ASSERT_EQ(table.StoreEngine(local), "");
    ASSERT_EQ(table.StoreShared(shared), "");
    const auto presence = Presence(table, hashes[0]);
    EXPECT_EQ(presence.cpu_local_owners, (std::set<EngineOwner>{local.owner}));
    EXPECT_EQ(presence.cpu_share_owners,
              (std::set<SharedObjectOwner>{shared.owner}));
    auto hits = table.Query(TestContext(), tokens);
    EXPECT_EQ(hits.at("instance-a").rank_matches.at(0),
              RankMatch(0, 48, 48, 48));
    EXPECT_EQ(hits.at("instance-a").rank_matches.at(1),
              RankMatch(0, 0, 48, 48));
    EXPECT_EQ(hits.at("instance-b").rank_matches.at(0),
              RankMatch(0, 0, 48, 48));
    ASSERT_EQ(table.ClearShared(ClearFor(shared.owner)), "");
    hits = table.Query(TestContext(), tokens);
    EXPECT_EQ(hits.at("instance-a").rank_matches.at(0),
              RankMatch(0, 48, 48, 48));
    EXPECT_EQ(hits.at("instance-a").rank_matches.at(1), RankMatch(0, 0, 0, 0));
    EXPECT_EQ(hits.at("instance-b").disk, 0);
    ASSERT_EQ(table.StoreShared(shared), "");
    ASSERT_EQ(table.RemoveEngine(local), "");
    hits = table.Query(TestContext(), tokens);
    EXPECT_EQ(hits.at("instance-a").rank_matches.at(0),
              RankMatch(0, 0, 48, 48));
    ASSERT_EQ(table.StoreEngine(local), "");
    ASSERT_EQ(table.Unregister(TestContext(), "instance-a", 0), "");
    RegisterOrFail(table, Registration());
    EXPECT_EQ(
        table.Query(TestContext(), tokens).at("instance-a").rank_matches.at(0),
        RankMatch(0, 0, 48, 48));
}

TEST(Mutations, FourTierMutationsRejectWrongOwnerFamily) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    auto engine = Gpu({Prefix(1)});
    engine.tier = StorageTier::kCpuShare;
    EXPECT_FALSE(table.StoreEngine(engine).empty());
    EXPECT_FALSE(table.RemoveEngine(engine).empty());
    auto clear = ClearFor();
    clear.tier = StorageTier::kCpuShare;
    EXPECT_FALSE(table.ClearEngine(clear).empty());
    for (auto tier : {StorageTier::kNpu, StorageTier::kCpuLocal}) {
        auto shared = Shared({Prefix(1)}, tier);
        EXPECT_FALSE(table.StoreShared(shared).empty());
        EXPECT_FALSE(table.RemoveShared(shared).empty());
        EXPECT_FALSE(table.ClearShared(ClearFor(shared.owner, tier)).empty());
    }
    EXPECT_EQ(table.GetGlobalView().contexts.at(0).prefix_count, 0u);
}

TEST(Query, FourTierLocalPartialTailUsesProfileLogicalPositions) {
    const auto tokens = Tokens(40);
    for (bool bigram : {false, true}) {
        SCOPED_TRACE(bigram);
        PrefixCacheTable table;
        auto registration = Registration();
        registration.profile = bigram ? SglangBigramProfile() : SglangProfile();
        RegisterOrFail(table, registration);
        const auto hashes =
            bigram ? SglangBigramHashes(tokens) : SglangHashes(tokens);
        ASSERT_EQ(hashes.size(), 3u);
        auto local = Gpu(hashes);
        local.tier = StorageTier::kCpuLocal;
        ASSERT_EQ(table.StoreEngine(local), "");
        const int64_t positions = bigram ? 39 : 40;
        EXPECT_EQ(table.Query(TestContext(), tokens)
                      .at("instance-a")
                      .rank_matches.at(0),
                  RankMatch(0, positions, positions, positions));
        local.prefixes = {hashes[2]};
        ASSERT_EQ(table.RemoveEngine(local), "");
        ASSERT_EQ(
            table.StoreShared(Shared({hashes[2]}, StorageTier::kCpuShare)), "");
        EXPECT_EQ(table.Query(TestContext(), tokens)
                      .at("instance-a")
                      .rank_matches.at(0),
                  RankMatch(0, 32, positions, positions));
    }
}

TEST(Query, DiskKeepsLocalRankAndSharedOwnershipInFourTierResults) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    RegisterOrFail(table, Registration("instance-a", 1));
    const auto tokens = Tokens(48);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(table.StoreShared(Shared({hashes[0]}, StorageTier::kCpuShare)),
              "");
    auto local_disk = Gpu({hashes[1]}, GpuOwner("instance-a", 1));
    local_disk.tier = StorageTier::kDisk;
    ASSERT_EQ(table.StoreEngine(local_disk), "");
    ASSERT_EQ(table.StoreShared(Shared({hashes[2]}, StorageTier::kDisk)), "");
    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.rank_matches.at(0), RankMatch(0, 0, 16, 16));
    EXPECT_EQ(result.rank_matches.at(1), RankMatch(0, 0, 16, 48));
    EXPECT_EQ(result.cpu_share, 16);
    EXPECT_EQ(result.disk, 48);
    EXPECT_EQ(result.longest_match_tokens, 48);
}

TEST(Mutations, ClearLocalTiersKeepsOtherSourcesAndSharedStore) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    RegisterOrFail(table, Registration("instance-b"));
    const auto tokens = Tokens(16);
    const auto hashes = Hashes(tokens);
    auto local = Gpu(hashes);
    local.tier = StorageTier::kCpuLocal;
    ASSERT_EQ(table.StoreEngine(local), "");
    auto other = Gpu(hashes, GpuOwner("instance-b"));
    other.tier = StorageTier::kDisk;
    ASSERT_EQ(table.StoreEngine(other), "");
    const auto shared = Shared(hashes, StorageTier::kCpuShare);
    ASSERT_EQ(table.StoreShared(shared), "");
    auto clear = ClearFor();
    clear.tier = std::nullopt;
    ASSERT_EQ(table.ClearEngine(clear), "");
    EXPECT_EQ(table.Query(TestContext(), tokens).at("instance-a").cpu_share,
              16);
    ASSERT_EQ(table.RemoveShared(shared), "");
    const auto hits = table.Query(TestContext(), tokens);
    EXPECT_EQ(hits.at("instance-a").disk, 0);
    EXPECT_EQ(hits.at("instance-b").rank_matches.at(0), RankMatch(0, 0, 0, 16));
    local.owner.dp_rank = 2;
    EXPECT_NE(table.StoreEngine(local), "");
    local.owner.dp_rank = 0;
    local.tier = static_cast<StorageTier>(99);
    EXPECT_NE(table.StoreEngine(local), "");
}

TEST(Query, DiskMissIgnoresAllLaterIsolatedBlocks) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const auto tokens = Tokens(80);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 5u);

    ASSERT_EQ(table.StoreEngine(Gpu({hashes[0], hashes[4]})), "");
    ASSERT_EQ(table.StoreShared(
                  Shared({hashes[1], hashes[4]}, StorageTier::kCpuShare)),
              "");
    ASSERT_EQ(
        table.StoreShared(Shared({hashes[3], hashes[4]}, StorageTier::kDisk)),
        "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 32);
    EXPECT_EQ(result.npu, 16);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 16}}));
    EXPECT_EQ(result.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                       {0, RankMatch(16, 16, 32, 32)}}));
    EXPECT_EQ(result.cpu_share, 32);
    EXPECT_EQ(result.disk, 32);
}

TEST(Query, DifferentRanksNeverFabricateOneGpuPrefix) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-a", 0));
    RegisterOrFail(table, Registration("instance-a", 1));
    const auto tokens = Tokens(32);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 2u);

    ASSERT_EQ(table.StoreEngine(
                  Gpu({hashes[0]}, GpuOwner("instance-a", 0, "rank-0"))),
              "");
    ASSERT_EQ(table.StoreEngine(
                  Gpu({hashes[1]}, GpuOwner("instance-a", 1, "rank-1"))),
              "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 16);
    EXPECT_EQ(result.npu, 16);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 16}, {1, 0}}));
    EXPECT_EQ(result.rank_matches,
              (std::map<int64_t, RankCacheHitResult>{
                  {0, RankMatch(16, 16, 16, 16)}, {1, RankMatch(0, 0, 0, 0)}}));
    EXPECT_EQ(result.dp.size(), result.rank_matches.size());
    EXPECT_EQ(result.cpu_share, 16);
    EXPECT_EQ(result.disk, 16);
}

TEST(Query, InstanceSummaryIsRealizedByMaximumGpuRank) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-a", 0));
    RegisterOrFail(table, Registration("instance-a", 1));
    const auto tokens = Tokens(64);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 4u);

    ASSERT_EQ(table.StoreEngine(Gpu({hashes[0], hashes[1]},
                                    GpuOwner("instance-a", 0, "rank-0"))),
              "");
    ASSERT_EQ(table.StoreEngine(
                  Gpu({hashes[0]}, GpuOwner("instance-a", 1, "rank-1"))),
              "");
    ASSERT_EQ(table.StoreShared(Shared({hashes[2]}, StorageTier::kCpuShare)),
              "");
    ASSERT_EQ(table.StoreShared(Shared({hashes[3]}, StorageTier::kDisk)), "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 32}, {1, 16}}));
    EXPECT_EQ(result.rank_matches, (std::map<int64_t, RankCacheHitResult>{
                                       {0, RankMatch(32, 32, 48, 64)},
                                       {1, RankMatch(16, 16, 16, 16)}}));
    EXPECT_EQ(result.dp.size(), result.rank_matches.size());
    for (const auto& [rank, gpu] : result.dp) {
        ASSERT_TRUE(result.rank_matches.contains(rank));
        EXPECT_EQ(gpu, result.rank_matches.at(rank).npu);
    }
    EXPECT_EQ(result.npu, result.rank_matches.at(0).npu);
    EXPECT_EQ(result.cpu_share, result.rank_matches.at(0).cpu_share);
    EXPECT_EQ(result.disk, result.rank_matches.at(0).disk);
    EXPECT_EQ(result.longest_match_tokens, result.rank_matches.at(0).disk);
}

TEST(Query, RegisteredZeroHitRanksAndIncompleteTailAreRetained) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-a", 0));
    RegisterOrFail(table, Registration("instance-a", 2));
    const auto incomplete_tokens = Tokens(31);

    const auto results = table.Query(TestContext(), incomplete_tokens);
    ASSERT_EQ(results.size(), 1u);
    const auto& result = results.at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 0);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 0}, {2, 0}}));
    EXPECT_EQ(result.rank_matches,
              (std::map<int64_t, RankCacheHitResult>{
                  {0, RankMatch(0, 0, 0, 0)}, {2, RankMatch(0, 0, 0, 0)}}));
    EXPECT_EQ(result.npu, 0);
    EXPECT_EQ(result.cpu_share, 0);
    EXPECT_EQ(result.disk, 0);
}

TEST(Query, InstanceFilterAndUnknownContextAreLookupOnly) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-a", 0));
    RegisterOrFail(table, Registration("instance-b", 1));
    const auto before = PrefixCacheTableTestPeer::Snapshot(table);

    const auto filtered =
        table.Query(TestContext(), Tokens(16), std::nullopt, "instance-b");
    ASSERT_EQ(filtered.size(), 1u);
    EXPECT_TRUE(filtered.contains("instance-b"));

    EXPECT_TRUE(
        table.Query(TestContext(), Tokens(16), std::nullopt, "unknown-instance")
            .empty());
    ContextKey unknown = TestContext();
    unknown.model_name = "missing";
    EXPECT_TRUE(table.Query(unknown, Tokens(16)).empty());
    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), before);
}

TEST(Query, CacheSaltChangesHashesWithoutChangingContextIdentity) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const auto tokens = Tokens(16);
    const auto unsalted = Hashes(tokens);
    ASSERT_EQ(table.StoreEngine(Gpu(unsalted)), "");

    const auto hit = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(hit.longest_match_tokens, 16);

    const auto salted =
        table.Query(TestContext(), tokens, std::string("request-salt"))
            .at("instance-a");
    EXPECT_EQ(salted.longest_match_tokens, 0);
    EXPECT_EQ(table.GetGlobalView().context_count, 1);
}

TEST(GlobalView, ReportsProfileRegistrationAndOwnerMapSize) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-a", 0));
    RegisterOrFail(table, Registration("instance-b", 1));
    ASSERT_EQ(table.StoreEngine(Gpu({Prefix(1), Prefix(2)})), "");

    const auto view = table.GetGlobalView();
    ASSERT_EQ(view.context_count, 1);
    ASSERT_EQ(view.contexts.size(), 1u);
    EXPECT_EQ(view.contexts[0].context, TestContext());
    EXPECT_EQ(view.contexts[0].profile, TestProfile());
    EXPECT_EQ(view.contexts[0].instance_ranks.at("instance-a"),
              (std::set<int64_t>{0}));
    EXPECT_EQ(view.contexts[0].instance_ranks.at("instance-b"),
              (std::set<int64_t>{1}));
    EXPECT_EQ(view.contexts[0].prefix_count, 2u);
}

TEST(Capacity, EvictsOldestWrittenPrefixesWhenOverLimit) {
    PrefixCacheTable table(10);
    RegisterOrFail(table, Registration());

    for (uint64_t i = 1; i <= 14; ++i) {
        ASSERT_EQ(table.StoreEngine(Gpu({Prefix(i)})), "");
    }

    const auto snapshot = PrefixCacheTableTestPeer::Snapshot(table);
    const auto& blocks = snapshot.contexts.at(TestContext()).blocks;
    EXPECT_LE(blocks.size(), 10u);
    EXPECT_TRUE(blocks.contains(Prefix(14)));
    EXPECT_FALSE(blocks.contains(Prefix(1)));
}

TEST(Capacity, UnlimitedWhenBlockLimitIsZero) {
    PrefixCacheTable table(0);
    RegisterOrFail(table, Registration());
    for (uint64_t i = 1; i <= 50; ++i) {
        ASSERT_EQ(table.StoreEngine(Gpu({Prefix(i)})), "");
    }
    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table)
                  .contexts.at(TestContext())
                  .blocks.size(),
              50u);
}

TEST(Capacity, OrderTrackingStaysInSyncWithBlocks) {
    PrefixCacheTable table(10);
    RegisterOrFail(table, Registration());
    const ContextKey context = TestContext();

    for (uint64_t i = 1; i <= 6; ++i) {
        ASSERT_EQ(table.StoreEngine(Gpu({Prefix(i)})), "");
    }
    auto sizes = PrefixCacheTableTestPeer::Order(table, context);
    EXPECT_EQ(sizes.blocks, 6u);
    EXPECT_EQ(sizes.write_order, 6u);
    EXPECT_EQ(sizes.order_pos, 6u);

    for (uint64_t i = 1; i <= 3; ++i) {
        ASSERT_EQ(table.RemoveEngine(Gpu({Prefix(i)})), "");
    }
    sizes = PrefixCacheTableTestPeer::Order(table, context);
    EXPECT_EQ(sizes.blocks, 3u);
    EXPECT_EQ(sizes.write_order, 3u);
    EXPECT_EQ(sizes.order_pos, 3u);

    ASSERT_EQ(table.ClearEngine(ClearFor()), "");
    sizes = PrefixCacheTableTestPeer::Order(table, context);
    EXPECT_EQ(sizes.blocks, 0u);
    EXPECT_EQ(sizes.write_order, 0u);
    EXPECT_EQ(sizes.order_pos, 0u);

    for (uint64_t i = 20; i <= 40; ++i) {
        ASSERT_EQ(table.StoreEngine(Gpu({Prefix(i)})), "");
    }
    sizes = PrefixCacheTableTestPeer::Order(table, context);
    EXPECT_LE(sizes.blocks, 10u);
    EXPECT_EQ(sizes.write_order, sizes.blocks);
    EXPECT_EQ(sizes.order_pos, sizes.blocks);
    EXPECT_GT(sizes.evicted_by_capacity, 0);
}

}  // namespace
