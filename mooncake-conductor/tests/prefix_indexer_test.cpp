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

using conductor::prefixindex::BlockPresenceSnapshot;
using conductor::prefixindex::CacheHitResult;
using conductor::prefixindex::ContextKey;
using conductor::prefixindex::EngineOwner;
using conductor::prefixindex::EngineRegistration;
using conductor::prefixindex::GpuClear;
using conductor::prefixindex::GpuMutation;
using conductor::prefixindex::HashBlock;
using conductor::prefixindex::HashProfile;
using conductor::prefixindex::PrefixCacheTable;
using conductor::prefixindex::PrefixCacheTableSnapshot;
using conductor::prefixindex::PrefixCacheTableTestPeer;
using conductor::prefixindex::ProjectedPrefix;
using conductor::prefixindex::SharedClear;
using conductor::prefixindex::SharedMutation;
using conductor::prefixindex::SharedObjectOwner;
using conductor::prefixindex::StorageTier;

constexpr char kRootDigest[] =
    "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e";
constexpr char kPaddedSeedRootDigest[] =
    "8d912e4e62b3cc377b1d1c7a14ef61dffbdaa0990237035c05401c29414c4172";

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

GpuMutation Gpu(const std::vector<ProjectedPrefix>& prefixes,
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

GpuClear ClearFor(EngineOwner owner = GpuOwner()) {
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
    auto strategy =
        conductor::prefixindex::CreateHashStrategy(TestProfile(), &error);
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
    ASSERT_EQ(table.StoreGpu(Gpu({Prefix(1)})), "");
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

TEST(Mutations, StoreRequiresKnownContextAndRegisteredGpuRank) {
    PrefixCacheTable table;
    const auto gpu = Gpu({Prefix(1)});
    const auto shared = Shared({Prefix(1)}, StorageTier::kCpu);

    EXPECT_FALSE(table.StoreGpu(gpu).empty());
    EXPECT_FALSE(table.StoreShared(shared).empty());
    EXPECT_EQ(table.GetGlobalView().context_count, 0);

    RegisterOrFail(table, Registration("instance-a", 1));
    EXPECT_FALSE(table.StoreGpu(gpu).empty());
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table)
                    .contexts.at(TestContext())
                    .blocks.empty());
}

TEST(Mutations, InvalidGroupTierAndOwnersPreserveState) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    ASSERT_EQ(table.StoreGpu(Gpu({Prefix(1)})), "");
    const auto before = PrefixCacheTableTestPeer::Snapshot(table);

    auto bad_group = Gpu({Prefix(2)});
    bad_group.cache_group = 3;
    EXPECT_FALSE(table.StoreGpu(bad_group).empty());

    auto bad_owner = Gpu({Prefix(2)});
    bad_owner.owner.source_stream.clear();
    EXPECT_FALSE(table.StoreGpu(bad_owner).empty());

    auto bad_tier = Shared({Prefix(2)}, StorageTier::kGpu);
    EXPECT_FALSE(table.StoreShared(bad_tier).empty());

    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(table), before);
}

TEST(Mutations, DuplicateGpuStoreAndRemoveAreIdempotent) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const ProjectedPrefix prefix = Prefix(7);
    const auto mutation = Gpu({prefix});

    ASSERT_EQ(table.StoreGpu(mutation), "");
    ASSERT_EQ(table.StoreGpu(mutation), "");
    EXPECT_EQ(Presence(table, prefix).gpu_owners,
              (std::set<EngineOwner>{GpuOwner()}));

    auto absent_owner = Gpu({prefix}, GpuOwner("instance-b", 0, "stream-b"));
    ASSERT_EQ(table.RemoveGpu(absent_owner), "");
    EXPECT_EQ(Presence(table, prefix).gpu_owners,
              (std::set<EngineOwner>{GpuOwner()}));

    ASSERT_EQ(table.RemoveGpu(mutation), "");
    ASSERT_EQ(table.RemoveGpu(mutation), "");
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

    ASSERT_EQ(table.StoreShared(Shared({collision}, StorageTier::kCpu, first)),
              "");
    ASSERT_EQ(table.StoreShared(Shared({collision}, StorageTier::kCpu, second)),
              "");
    ASSERT_EQ(table.StoreShared(Shared({collision}, StorageTier::kCpu, first)),
              "");
    EXPECT_EQ(Presence(table, collision).cpu_owners,
              (std::set<SharedObjectOwner>{first, second}));

    ASSERT_EQ(table.RemoveShared(Shared({collision}, StorageTier::kCpu, first)),
              "");
    EXPECT_EQ(Presence(table, collision).cpu_owners,
              (std::set<SharedObjectOwner>{second}));

    ASSERT_EQ(
        table.RemoveShared(Shared({collision}, StorageTier::kCpu, second)), "");
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table)
                    .contexts.at(TestContext())
                    .blocks.empty());
}

TEST(Mutations, BlockLivesUntilEveryTierOwnerSetIsEmpty) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const ProjectedPrefix prefix = Prefix(11);
    const auto gpu = Gpu({prefix});
    const auto cpu = Shared({prefix}, StorageTier::kCpu, SharedOwner("cpu"));
    const auto disk = Shared({prefix}, StorageTier::kDisk, SharedOwner("disk"));

    ASSERT_EQ(table.StoreGpu(gpu), "");
    ASSERT_EQ(table.StoreShared(cpu), "");
    ASSERT_EQ(table.StoreShared(disk), "");
    ASSERT_EQ(table.RemoveGpu(gpu), "");
    EXPECT_TRUE(Presence(table, prefix).gpu_owners.empty());
    EXPECT_FALSE(Presence(table, prefix).cpu_owners.empty());
    EXPECT_FALSE(Presence(table, prefix).disk_owners.empty());

    ASSERT_EQ(table.RemoveShared(cpu), "");
    EXPECT_TRUE(Presence(table, prefix).cpu_owners.empty());
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

    ASSERT_EQ(table.StoreGpu(Gpu({prefix}, engine_a)), "");
    ASSERT_EQ(table.StoreGpu(Gpu({prefix}, engine_a_other_stream)), "");
    ASSERT_EQ(table.StoreGpu(Gpu({prefix}, engine_b)), "");
    ASSERT_EQ(table.StoreShared(Shared({prefix}, StorageTier::kCpu, shared_a)),
              "");
    ASSERT_EQ(table.StoreShared(Shared({prefix}, StorageTier::kDisk, shared_a)),
              "");
    ASSERT_EQ(table.StoreShared(Shared({prefix}, StorageTier::kCpu, shared_b)),
              "");

    ASSERT_EQ(table.ClearGpu(ClearFor(engine_a)), "");
    EXPECT_EQ(Presence(table, prefix).gpu_owners,
              (std::set<EngineOwner>{engine_a_other_stream, engine_b}));
    EXPECT_EQ(Presence(table, prefix).cpu_owners,
              (std::set<SharedObjectOwner>{shared_a, shared_b}));

    ASSERT_EQ(table.ClearShared(ClearFor(shared_a, StorageTier::kCpu)), "");
    EXPECT_EQ(Presence(table, prefix).cpu_owners,
              (std::set<SharedObjectOwner>{shared_b}));
    EXPECT_EQ(Presence(table, prefix).disk_owners,
              (std::set<SharedObjectOwner>{shared_a}));
    EXPECT_EQ(Presence(table, prefix).gpu_owners,
              (std::set<EngineOwner>{engine_a_other_stream, engine_b}));

    ASSERT_EQ(table.ClearShared(ClearFor(shared_a)), "");
    EXPECT_TRUE(Presence(table, prefix).disk_owners.empty());
    EXPECT_EQ(Presence(table, prefix).gpu_owners,
              (std::set<EngineOwner>{engine_a_other_stream, engine_b}));
}

TEST(Mutations, UnknownRemoveClearAndUnregisterNeverCreateState) {
    PrefixCacheTable table;
    const ContextKey context = TestContext();

    EXPECT_EQ(table.RemoveGpu(Gpu({Prefix(1)})), "");
    EXPECT_EQ(table.ClearGpu(ClearFor()), "");
    EXPECT_EQ(table.RemoveShared(Shared({Prefix(1)}, StorageTier::kCpu)), "");
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

    ASSERT_EQ(table.StoreGpu(Gpu({prefix}, a0)), "");
    ASSERT_EQ(table.StoreGpu(Gpu({prefix}, a0_second_stream)), "");
    ASSERT_EQ(table.StoreGpu(Gpu({prefix}, a1)), "");
    ASSERT_EQ(table.StoreGpu(Gpu({prefix}, b0)), "");
    ASSERT_EQ(table.StoreShared(Shared({prefix}, StorageTier::kCpu, shared)),
              "");

    ASSERT_EQ(table.Unregister(TestContext(), "instance-a", 0), "");
    auto snapshot = PrefixCacheTableTestPeer::Snapshot(table);
    const auto& state = snapshot.contexts.at(TestContext());
    EXPECT_EQ(state.instance_ranks.at("instance-a"), (std::set<int64_t>{1}));
    EXPECT_EQ(state.instance_ranks.at("instance-b"), (std::set<int64_t>{0}));
    EXPECT_EQ(state.blocks.at(prefix).gpu_owners,
              (std::set<EngineOwner>{a1, b0}));
    EXPECT_EQ(state.blocks.at(prefix).cpu_owners,
              (std::set<SharedObjectOwner>{shared}));

    ASSERT_EQ(table.Unregister(TestContext(), "instance-a", 1), "");
    snapshot = PrefixCacheTableTestPeer::Snapshot(table);
    EXPECT_FALSE(snapshot.contexts.at(TestContext())
                     .instance_ranks.contains("instance-a"));
    EXPECT_EQ(snapshot.contexts.at(TestContext()).blocks.at(prefix).gpu_owners,
              (std::set<EngineOwner>{b0}));
    EXPECT_EQ(snapshot.contexts.at(TestContext()).blocks.at(prefix).cpu_owners,
              (std::set<SharedObjectOwner>{shared}));
}

TEST(Query, ExactTwoInstanceSharedCacheExample) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-1", 0));
    RegisterOrFail(table, Registration("instance-2", 1));
    const auto tokens = Tokens(48);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    ASSERT_EQ(table.StoreGpu(Gpu({hashes[0], hashes[1]},
                                 GpuOwner("instance-1", 0, "engine-1"))),
              "");
    ASSERT_EQ(table.StoreShared(
                  Shared(hashes, StorageTier::kCpu, SharedOwner("cpu-object"))),
              "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kDisk,
                                       SharedOwner("disk-object"))),
              "");

    const auto results = table.Query(TestContext(), tokens);
    ASSERT_EQ(results.size(), 2u);

    const CacheHitResult& first = results.at("instance-1");
    EXPECT_EQ(first.longest_match_tokens, 48);
    EXPECT_EQ(first.gpu, 32);
    EXPECT_EQ(first.dp, (std::map<int64_t, int64_t>{{0, 32}}));
    EXPECT_EQ(first.cpu, 48);
    EXPECT_EQ(first.disk, 48);

    const CacheHitResult& second = results.at("instance-2");
    EXPECT_EQ(second.longest_match_tokens, 48);
    EXPECT_EQ(second.gpu, 0);
    EXPECT_EQ(second.dp, (std::map<int64_t, int64_t>{{1, 0}}));
    EXPECT_EQ(second.cpu, 48);
    EXPECT_EQ(second.disk, 48);
}

TEST(Query, GpuPrefixComposesWithSharedTail) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const auto tokens = Tokens(48);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 3u);

    ASSERT_EQ(table.StoreGpu(Gpu({hashes[0], hashes[1]})), "");
    ASSERT_EQ(table.StoreShared(Shared({hashes[2]}, StorageTier::kCpu)), "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 48);
    EXPECT_EQ(result.gpu, 32);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 32}}));
    EXPECT_EQ(result.cpu, 0);
    EXPECT_EQ(result.disk, 0);
}

TEST(Query, DuplicateTierPresenceCountsOnceInLongestMatch) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration());
    const auto tokens = Tokens(16);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 1u);

    ASSERT_EQ(table.StoreGpu(Gpu(hashes)), "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kCpu)), "");
    ASSERT_EQ(table.StoreShared(Shared(hashes, StorageTier::kDisk)), "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 16);
    EXPECT_EQ(result.gpu, 16);
    EXPECT_EQ(result.cpu, 16);
    EXPECT_EQ(result.disk, 16);
}

TEST(Query, DifferentRanksNeverFabricateOneGpuPrefix) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-a", 0));
    RegisterOrFail(table, Registration("instance-a", 1));
    const auto tokens = Tokens(32);
    const auto hashes = Hashes(tokens);
    ASSERT_EQ(hashes.size(), 2u);

    ASSERT_EQ(
        table.StoreGpu(Gpu({hashes[0]}, GpuOwner("instance-a", 0, "rank-0"))),
        "");
    ASSERT_EQ(
        table.StoreGpu(Gpu({hashes[1]}, GpuOwner("instance-a", 1, "rank-1"))),
        "");

    const auto result = table.Query(TestContext(), tokens).at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 16);
    EXPECT_EQ(result.gpu, 16);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 16}, {1, 0}}));
    EXPECT_EQ(result.cpu, 0);
    EXPECT_EQ(result.disk, 0);
}

TEST(Query, RegisteredZeroHitRanksAndIncompleteTailAreRetained) {
    PrefixCacheTable table;
    RegisterOrFail(table, Registration("instance-a", 0));
    RegisterOrFail(table, Registration("instance-a", 2));
    const auto incomplete_tokens = Tokens(15);

    const auto results = table.Query(TestContext(), incomplete_tokens);
    ASSERT_EQ(results.size(), 1u);
    const auto& result = results.at("instance-a");
    EXPECT_EQ(result.longest_match_tokens, 0);
    EXPECT_EQ(result.dp, (std::map<int64_t, int64_t>{{0, 0}, {2, 0}}));
    EXPECT_EQ(result.gpu, 0);
    EXPECT_EQ(result.cpu, 0);
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
    ASSERT_EQ(table.StoreGpu(Gpu(unsalted)), "");

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
    ASSERT_EQ(table.StoreGpu(Gpu({Prefix(1), Prefix(2)})), "");

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

}  // namespace
