#include <gtest/gtest.h>
#include <json/json.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "conductor/kvevent/conductor_service.h"
#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/cache_layout.h"
#include "conductor/prefixindex/hash_strategy.h"
#include "conductor/prefixindex/hybrid_index.h"
#include "conductor/prefixindex/layout_parser.h"
#include "conductor/zmq/msg_decoder.h"
#include "event_manager_test_peer.h"
#include "prefix_indexer_test_peer.h"
#include "test_fixtures.h"

namespace mooncake::conductor {
namespace {

using namespace prefixindex;
using common::PublisherKind;
using kvevent::EventManager;
using kvevent::KVEventHandler;

ContextKey Context() { return {"default", "hybrid-model", "", 16}; }

HashProfile Profile(bool sglang = false) {
    common::HashProfileConfig config{sglang ? "sglang" : "vllm_v1",
                                     sglang ? "sha256_raw" : "sha256_cbor", "0",
                                     sglang ? "first64_be" : "low64_be"};
    HashProfile profile;
    EXPECT_EQ(ResolveHashProfile(config, &profile), "");
    return profile;
}

std::vector<int32_t> Tokens(size_t count = 160) {
    std::vector<int32_t> tokens(count);
    std::iota(tokens.begin(), tokens.end(), 1);
    return tokens;
}

std::vector<HashBlock> Hashes(const HashProfile& profile = Profile(),
                              HashDomain domain = HashDomain::kRaw,
                              std::optional<std::string> salt = std::nullopt) {
    std::vector<HashBlock> blocks;
    std::string error;
    auto strategy = CreateHashStrategy(profile, &error, domain);
    EXPECT_NE(strategy, nullptr) << error;
    if (strategy)
        EXPECT_EQ(strategy->Compute(Context(), Tokens(), salt, &blocks), "");
    return blocks;
}

CacheGroupLayout Group(const std::string& id, MatchPolicy policy,
                       int64_t span = 16, int64_t window = 0) {
    CacheGroupLayout group;
    group.id = id;
    group.match = policy;
    group.block_tokens = span;
    group.alignment_tokens = span;
    group.window_tokens = window;
    return group;
}

std::shared_ptr<const CacheLayout> Layout(
    std::vector<CacheGroupLayout> groups = {
        Group("full", MatchPolicy::kDense),
        Group("state", MatchPolicy::kCheckpoint)}) {
    auto layout = std::make_shared<CacheLayout>();
    layout->id = "hybrid";
    layout->hash_unit_tokens = 16;
    layout->alignment_tokens = 16;
    layout->groups = std::move(groups);
    EXPECT_EQ(ValidateCacheLayout(*layout), "");
    return layout;
}

EngineRegistration Registration(std::shared_ptr<const CacheLayout> layout,
                                const std::string& instance = "p0",
                                int64_t rank = 0,
                                HashProfile profile = Profile()) {
    return {.context = Context(),
            .profile = std::move(profile),
            .instance_id = instance,
            .dp_rank = rank,
            .effective_block_size = 16,
            .layout = std::move(layout),
            .readable_domains = {"pool"}};
}

GroupMutation Engine(size_t group, std::initializer_list<size_t> endpoints,
                     StorageTier tier = StorageTier::kDevice,
                     const std::string& instance = "p0", int64_t rank = 0) {
    GroupMutation mutation{
        .context = Context(),
        .group = group,
        .tier = tier,
        .owner = EngineOwner{"stream-" + instance + "-" + std::to_string(rank),
                             instance, rank}};
    const auto hashes = Hashes();
    for (auto end : endpoints)
        mutation.prefixes.push_back(hashes.at(end / 16 - 1).projected);
    return mutation;
}

GroupMutation Shared(size_t group, size_t endpoint, size_t part,
                     const std::string& object,
                     const std::string& replica = "copy-a") {
    GroupMutation mutation{
        .context = Context(),
        .group = group,
        .part = part,
        .tier = StorageTier::kCpuShare,
        .owner = SharedObjectOwner{"store", "backend", object},
        .storage_domain = "pool",
        .replica = replica};
    mutation.prefixes = {Hashes().at(endpoint / 16 - 1).projected};
    return mutation;
}

CacheHitResult Query(const PrefixCacheTable& table,
                     const std::string& instance = "p0") {
    const auto result = table.Query(Context(), Tokens());
    EXPECT_TRUE(result.contains(instance));
    return result.contains(instance) ? result.at(instance) : CacheHitResult{};
}

void StoreFull(PrefixCacheTable& table) {
    ASSERT_EQ(table.StoreGrouped(
                  Engine(0, {16, 32, 48, 64, 80, 96, 112, 128, 144, 160})),
              "");
}

TEST(SpecialKV, CheckpointNeedsOnlyEndpointAndRemovalPreservesFullGroup) {
    PrefixCacheTable table;
    ASSERT_TRUE(table.Register(Registration(Layout())).error.empty());
    StoreFull(table);
    EXPECT_EQ(Query(table).device, 0);
    ASSERT_EQ(table.StoreGrouped(Engine(1, {160})), "");
    EXPECT_EQ(Query(table).device, 160);
    ASSERT_EQ(table.RemoveGrouped(Engine(1, {160})), "");
    EXPECT_EQ(Query(table).device, 0);
    ASSERT_EQ(table.StoreGrouped(Engine(1, {64})), "");
    EXPECT_EQ(Query(table).device, 64);
}

TEST(SpecialKV, ColdProbeRespectsDenseStrideAndReadableOwners) {
    PrefixCacheTable table;
    const auto layout = Layout({Group("full", MatchPolicy::kDense, 32),
                                Group("state", MatchPolicy::kCheckpoint)});
    ASSERT_TRUE(table.Register(Registration(layout)).error.empty());
    ASSERT_TRUE(table.Register(Registration(layout, "p1")).error.empty());
    EXPECT_EQ(Query(table).disk, 0);
    // A later checkpoint cannot recover a missing first dense block.
    ASSERT_EQ(table.StoreGrouped(Engine(1, {64})), "");
    EXPECT_EQ(Query(table).disk, 0);
    ASSERT_EQ(table.StoreGrouped(Engine(0, {32}, StorageTier::kDevice, "p1")),
              "");
    ASSERT_EQ(table.StoreGrouped(Engine(1, {32}, StorageTier::kDevice, "p1")),
              "");
    EXPECT_EQ(Query(table, "p1").device, 32);
    EXPECT_EQ(
        table.Query(Context(), Tokens(), std::nullopt, "p0").at("p0").disk, 0);
    EXPECT_EQ(table.Query(Context(), Tokens(16)).at("p1").disk, 0);
    auto unrelated = Tokens();
    unrelated.front() = 9000;
    EXPECT_EQ(table.Query(Context(), unrelated).at("p1").disk, 0);
}

TEST(SpecialKV, JointBoundaryIsNotMinimumOfIndependentMaxima) {
    PrefixCacheTable table;
    const auto layout = Layout({Group("full", MatchPolicy::kDense),
                                Group("state", MatchPolicy::kCheckpoint),
                                Group("window", MatchPolicy::kWindow, 16, 33)});
    ASSERT_TRUE(table.Register(Registration(layout)).error.empty());
    StoreFull(table);
    ASSERT_EQ(table.StoreGrouped(Engine(1, {64, 160})), "");
    ASSERT_EQ(table.StoreGrouped(Engine(2, {48, 64, 112, 128})), "");
    EXPECT_EQ(Query(table).device, 64);
}

TEST(SpecialKV, WindowCanRecoverAfterAnEarlierHole) {
    PrefixCacheTable table;
    ASSERT_TRUE(table
                    .Register(Registration(Layout(
                        {Group("window", MatchPolicy::kWindow, 16, 33)})))
                    .error.empty());
    ASSERT_EQ(table.StoreGrouped(Engine(0, {48, 64})), "");
    EXPECT_EQ(Query(table).device, 64);
    ASSERT_EQ(table.RemoveGrouped(Engine(0, {48})), "");
    EXPECT_EQ(Query(table).device, 0);
    ASSERT_EQ(table.StoreGrouped(Engine(0, {144, 160})), "");
    EXPECT_EQ(Query(table).device, 160);
}

TEST(SpecialKV, FourTiersAreCumulativeAndLocalOwnersDoNotMix) {
    PrefixCacheTable table;
    const auto layout = Layout();
    ASSERT_TRUE(table.Register(Registration(layout)).error.empty());
    ASSERT_TRUE(table.Register(Registration(layout, "p1")).error.empty());
    StoreFull(table);
    ASSERT_EQ(table.StoreGrouped(Engine(1, {32})), "");
    ASSERT_EQ(table.StoreGrouped(Engine(1, {64}, StorageTier::kCpuLocal)), "");
    ASSERT_EQ(table.StoreGrouped(Shared(1, 128, 0, "checkpoint")), "");
    auto disk = Shared(1, 160, 0, "disk-checkpoint");
    disk.tier = StorageTier::kDisk;
    ASSERT_EQ(table.StoreGrouped(disk), "");
    const auto hit = Query(table);
    EXPECT_EQ(hit.rank_matches.at(0), (RankCacheHitResult{32, 64, 128, 160}));
    EXPECT_EQ(Query(table, "p1").disk, 0);
    EXPECT_EQ(table.Query(Context(), Tokens(), std::nullopt, "absent").size(),
              0u);
}

TEST(SpecialKV, RequiredPartsAreAndAndCompleteCopiesAreOr) {
    auto state = Group("state", MatchPolicy::kCheckpoint);
    state.parts = {"k", "v"};
    PrefixCacheTable table;
    ASSERT_TRUE(table
                    .Register(Registration(
                        Layout({Group("full", MatchPolicy::kDense), state})))
                    .error.empty());
    StoreFull(table);
    const auto key = Shared(1, 160, 0, "k");
    const auto value = Shared(1, 160, 1, "v");
    ASSERT_EQ(table.StoreGrouped(key), "");
    EXPECT_EQ(Query(table).cpu_share, 0);
    ASSERT_EQ(table.StoreGrouped(value), "");
    EXPECT_EQ(Query(table).cpu_share, 160);
    ASSERT_EQ(table.StoreGrouped(Shared(1, 160, 0, "k-b", "copy-b")), "");
    EXPECT_EQ(Query(table).cpu_share, 160);
    ASSERT_EQ(table.RemoveGrouped(key), "");
    EXPECT_EQ(Query(table).cpu_share,
              0);  // copy-b has no V; never combine copies
    ASSERT_EQ(table.StoreGrouped(Shared(1, 160, 1, "v-b", "copy-b")), "");
    EXPECT_EQ(Query(table).cpu_share, 160);
    ASSERT_EQ(table.RemoveGrouped(value), "");
    EXPECT_EQ(Query(table).cpu_share, 160);
}

TEST(SpecialKV, DuplicateStoresDoNotCreateReferencesAndDomainsRestrictReaders) {
    PrefixCacheTable table;
    ASSERT_TRUE(table.Register(Registration(Layout())).error.empty());
    StoreFull(table);
    auto checkpoint = Shared(1, 160, 0, "checkpoint");
    ASSERT_EQ(table.StoreGrouped(checkpoint), "");
    ASSERT_EQ(table.StoreGrouped(checkpoint), "");
    ASSERT_EQ(table.RemoveGrouped(checkpoint), "");
    EXPECT_EQ(Query(table).cpu_share, 0);
    checkpoint.storage_domain = "unreadable-pool";
    ASSERT_EQ(table.StoreGrouped(checkpoint), "");
    EXPECT_EQ(Query(table).cpu_share, 0);
}

TEST(SpecialKV, ClearAndUnregisterRetractEveryGroupWithoutRemovingOtherRanks) {
    PrefixCacheTable table;
    const auto layout = Layout();
    ASSERT_TRUE(table.Register(Registration(layout)).error.empty());
    ASSERT_TRUE(table.Register(Registration(layout, "p0", 1)).error.empty());
    StoreFull(table);
    ASSERT_EQ(table.StoreGrouped(Engine(1, {160})), "");
    ASSERT_EQ(
        table.StoreGrouped(Engine(0, {16}, StorageTier::kDevice, "p0", 1)), "");
    ASSERT_EQ(
        table.StoreGrouped(Engine(1, {16}, StorageTier::kDevice, "p0", 1)), "");
    ASSERT_EQ(table.ClearGroupedSource(Context(), "stream-p0-0"), "");
    EXPECT_EQ(Query(table).rank_matches.at(0).device, 0);
    EXPECT_EQ(Query(table).rank_matches.at(1).device, 16);
    ASSERT_EQ(table.Unregister(Context(), "p0", 1), "");
    EXPECT_FALSE(Query(table).rank_matches.contains(1));
}

TEST(SpecialKV, LayoutConflictsAndUngroupedWritesAreRejected) {
    PrefixCacheTable table;
    auto registration = Registration(Layout());
    ASSERT_TRUE(table.Register(registration).error.empty());
    auto changed = std::make_shared<CacheLayout>(*registration.layout);
    changed->groups[1].alignment_tokens = 32;
    registration.layout = changed;
    EXPECT_FALSE(table.Register(registration).error.empty());
    EXPECT_FALSE(table
                     .StoreEngine({.context = Context(),
                                   .prefixes = {{1}},
                                   .owner = {"stream-p0-0", "p0", 0},
                                   .effective_block_size = 16})
                     .empty());
    auto invalid = Engine(0, {16});
    invalid.group = 100;
    EXPECT_FALSE(table.StoreGrouped(invalid).empty());
}

TEST(SpecialKV, GroupSpansUseProducerChainAndDoNotMultiplyCompressionRatio) {
    PrefixCacheTable table;
    ASSERT_TRUE(table
                    .Register(Registration(
                        Layout({Group("compressed", MatchPolicy::kDense, 32)})))
                    .error.empty());
    ASSERT_EQ(table.StoreGrouped(Engine(0, {32, 64, 96, 128, 160})), "");
    EXPECT_EQ(Query(table).device, 160);
    ASSERT_EQ(table.RemoveGrouped(Engine(0, {64})), "");
    EXPECT_EQ(Query(table).device, 32);
}

TEST(SpecialKV, CapacityBoundsAllPhysicalPartReferences) {
    auto group = Group("state", MatchPolicy::kCheckpoint);
    group.parts = {"k", "v"};
    auto layout = std::make_shared<CacheLayout>(*Layout({group}));
    layout->max_records = 2;
    PrefixCacheTable table;
    ASSERT_TRUE(table.Register(Registration(layout)).error.empty());
    ASSERT_EQ(table.StoreGrouped(Shared(0, 160, 0, "k")), "");
    ASSERT_EQ(table.StoreGrouped(Shared(0, 160, 1, "v")), "");
    EXPECT_EQ(Query(table).cpu_share, 160);
    ASSERT_EQ(table.StoreGrouped(Shared(0, 128, 0, "another")), "");
    EXPECT_EQ(Query(table).cpu_share, 0);
    EXPECT_LE(table.GetGlobalView().contexts.at(0).prefix_count, 1u);
}

SourceLayout Binding() {
    return {.layout = Layout(),
            .source = LayoutSource::kVllm,
            .groups = {{0, 0}, {1, 1}}};
}

common::ServiceConfig Service(PublisherKind kind = PublisherKind::kVllm) {
    return {.endpoint = "tcp://127.0.0.1:19876",
            .publisher_kind = kind,
            .model_name = "hybrid-model",
            .tenant_id = "default",
            .instance_id = "p0",
            .block_size = 16,
            .hash_profile = Profile(kind == PublisherKind::kSglang)};
}

TEST(CacheLayouts, UnknownAndDenseModelsKeepOrdinaryPrefixMatching) {
    for (const char* model : {"unknown-model", "GLM-5.2", "zai-org/GLM-5.3"}) {
        SCOPED_TRACE(model);
        auto service = Service();
        service.model_name = model;
        const auto binding = ResolveModelLayout(service);
        ASSERT_EQ(binding.layout, nullptr);
        auto registration = Registration(nullptr);
        registration.context.model_name = model;
        PrefixCacheTable table;
        ASSERT_TRUE(table.Register(registration).error.empty());
        std::string error;
        auto strategy = CreateHashStrategy(service.hash_profile, &error);
        ASSERT_NE(strategy, nullptr) << error;
        std::vector<HashBlock> hashes;
        ASSERT_EQ(strategy->Compute(registration.context, Tokens(),
                                    std::nullopt, &hashes),
                  "");
        std::vector<ProjectedPrefix> prefixes;
        for (const auto& hash : hashes) prefixes.push_back(hash.projected);
        ASSERT_EQ(table.StoreEngine({.context = registration.context,
                                     .prefixes = prefixes,
                                     .owner = {service.endpoint, "p0", 0},
                                     .effective_block_size = 16}),
                  "");
        EXPECT_EQ(table.Query(registration.context, Tokens()).at("p0").device,
                  160);
    }
}

TEST(CacheLayouts, KnownSpecialModelsDoNotGuessMissingDeploymentLayout) {
    for (auto publisher : {PublisherKind::kVllm, PublisherKind::kSglang,
                           PublisherKind::kMooncake}) {
        for (const char* model : {"DeepSeek-V4-Flash", "DeepSeek-V4.1-Flash",
                                  "GLM-5.3-Flash", "Kimi-K3"}) {
            SCOPED_TRACE(model);
            auto service = Service(publisher);
            service.model_name = model;
            const auto binding = ResolveModelLayout(service);
            ASSERT_NE(binding.layout, nullptr);
            EXPECT_EQ(binding.layout->hash_unit_tokens, service.block_size);
            EXPECT_FALSE(binding.layout->unavailable_reason.empty());
            EXPECT_EQ(ValidateCacheLayout(*binding.layout), "");
            EXPECT_EQ(
                ValidateLayoutProfile(*binding.layout, service.hash_profile),
                "");
            auto registration =
                Registration(binding.layout, "p0", 0, service.hash_profile);
            registration.context.model_name = model;
            registration.publisher_kind = publisher == PublisherKind::kSglang
                                              ? publisher
                                              : PublisherKind::kVllm;
            PrefixCacheTable table;
            ASSERT_TRUE(table.Register(registration).error.empty());
            // Ungrouped writes cannot bypass special-model completeness.
            EXPECT_FALSE(table
                             .StoreEngine({.context = registration.context,
                                           .prefixes = {{1}},
                                           .owner = {service.endpoint, "p0", 0},
                                           .effective_block_size = 16})
                             .empty());
            const auto result =
                table.Query(registration.context, Tokens()).at("p0");
            EXPECT_EQ(result.device, 0);
            EXPECT_EQ(result.cpu_local, 0);
            EXPECT_EQ(result.cpu_share, 0);
            EXPECT_EQ(result.disk, 0);
            ASSERT_TRUE(result.rank_matches.contains(0));
            EXPECT_EQ(result.rank_matches.at(0).device, 0);
            EXPECT_TRUE(table
                            .Query(registration.context, Tokens(), std::nullopt,
                                   "other-instance")
                            .empty());
        }
    }
}

TEST(CacheLayouts, ResolvesOnlyKnownModelAliasesAndWeightFormats) {
    const std::pair<const char*, const char*> aliases[] = {
        {"deepseekv4", "DeepSeek-V4"},
        {"deepseek-ai/DeepSeek-V4-Flash", "DeepSeek-V4"},
        {"deepseek-ai/DeepSeek-V4-Pro", "DeepSeek-V4"},
        {"deepseek-ai/DeepSeek-V4.1-Flash", "DeepSeek-V4.1-Flash"},
        {"zai-org/GLM-5.2-FP8", "GLM-5.2"},
        {"zai-org/GLM-5.3", "GLM-5.3"},
        {"zai-org/GLM-5.3-Flash", "GLM-5.3-Flash"},
        {"/models/GLM-5.3-Flash-W4A8/", "GLM-5.3-Flash"},
        {"D:\\models\\GLM-5.2-W8A8", "GLM-5.2"},
        {"moonshotai/Kimi-K3", "Kimi-K3"},
        {"kimi k3", "Kimi-K3"},
    };
    for (const auto& [name, expected] : aliases) {
        SCOPED_TRACE(name);
        const auto* spec = FindModelCacheSpec(name);
        ASSERT_NE(spec, nullptr);
        EXPECT_EQ(spec->name, expected);
    }
    for (const char* name :
         {"", "my-GLM-5.3-Flash", "GLM-5.3-Flash-custom", "Kimi-K3.1",
          "DeepSeek-V4.1-Pro", "Qwen3", "deepseekv40", "GLM-5.3-Flash-draft"}) {
        SCOPED_TRACE(name);
        EXPECT_EQ(FindModelCacheSpec(name), nullptr);
    }
}

TEST(CacheLayouts, ModelRulesDistinguishDenseDsaFromHybridAndCompressedKv) {
    for (const char* name : {"GLM-5.2", "GLM-5.3"}) {
        const auto* spec = FindModelCacheSpec(name);
        ASSERT_NE(spec, nullptr);
        EXPECT_FALSE(spec->RequiresGroupedMatching());
    }
    const auto* flash = FindModelCacheSpec("GLM-5.3-Flash");
    ASSERT_NE(flash, nullptr);
    EXPECT_TRUE(flash->checkpoints);
    EXPECT_TRUE(flash->token_compression);
    const auto* kimi = FindModelCacheSpec("Kimi-K3");
    ASSERT_NE(kimi, nullptr);
    EXPECT_TRUE(kimi->checkpoints);
    EXPECT_FALSE(kimi->token_compression);
    for (const char* name : {"DeepSeek-V4", "DeepSeek-V4.1-Flash"}) {
        const auto* spec = FindModelCacheSpec(name);
        ASSERT_NE(spec, nullptr);
        EXPECT_TRUE(spec->sliding_window);
        EXPECT_TRUE(spec->token_compression);
    }
}

TEST(CacheLayouts, RejectsMalformedInternalGeometry) {
    auto layout = *Layout();
    layout.groups[1].id = "full";
    EXPECT_FALSE(ValidateCacheLayout(layout).empty());
    layout = *Layout();
    layout.groups[0].block_tokens = 17;
    EXPECT_FALSE(ValidateCacheLayout(layout).empty());
    layout = *Layout();
    layout.groups.clear();
    EXPECT_FALSE(ValidateCacheLayout(layout).empty());
    layout = *Layout();
    layout.groups[1].match = MatchPolicy::kWindow;
    EXPECT_FALSE(ValidateCacheLayout(layout).empty());
}

TEST(CacheLayouts, OrdinaryServiceDoesNotAllocateHybridState) {
    PrefixCacheTable table;
    auto registration = Registration(nullptr);
    ASSERT_TRUE(table.Register(registration).error.empty());
    ASSERT_EQ(table.StoreEngine({.context = Context(),
                                 .prefixes = {Hashes()[0].projected},
                                 .owner = {"stream", "p0", 0},
                                 .effective_block_size = 16}),
              "");
    EXPECT_EQ(Query(table).device, 16);
    const auto snapshot = PrefixCacheTableTestPeer::Snapshot(table);
    EXPECT_EQ(snapshot.contexts.at(Context()).blocks.size(), 1u);
}

TEST(SpecialKV, MatchesPinnedVllmCheckpointAndWindowDecisions) {
    const auto fixtures = test::LoadJsonFixture("special_kv_vectors.json");
    for (const auto& item : fixtures["matcher_cases"]) {
        SCOPED_TRACE(item.toStyledString());
        const bool window = item["policy"].asString() == "window";
        PrefixCacheTable table;
        ASSERT_TRUE(
            table
                .Register(Registration(Layout({Group(
                    "component",
                    window ? MatchPolicy::kWindow : MatchPolicy::kCheckpoint,
                    16, window ? 33 : 0)})))
                .error.empty());
        GroupMutation mutation{.context = Context(),
                               .group = 0,
                               .owner = EngineOwner{"stream-p0-0", "p0", 0}};
        const auto hashes = Hashes();
        for (const auto& endpoint : item["present"])
            mutation.prefixes.push_back(
                hashes.at(endpoint.asUInt() / 16 - 1).projected);
        ASSERT_EQ(table.StoreGrouped(mutation), "");
        EXPECT_EQ(table.Query(Context(), Tokens(128)).at("p0").device,
                  item["expected"].asInt64());
    }
}

TEST(SpecialKV, ExplicitL3TrailingPagesAndOptionalSidecars) {
    auto window = Group("tail", MatchPolicy::kWindow, 16, 0);
    window.trailing_blocks = 3;
    auto optional = Group("compressed-alternative", MatchPolicy::kDense);
    optional.required = false;
    PrefixCacheTable table;
    ASSERT_TRUE(
        table.Register(Registration(Layout({window, optional}))).error.empty());
    ASSERT_EQ(table.StoreGrouped(Shared(0, 128, 0, "tail-a")), "");
    ASSERT_EQ(table.StoreGrouped(Shared(0, 144, 0, "tail-b")), "");
    EXPECT_EQ(Query(table).cpu_share, 0);
    ASSERT_EQ(table.StoreGrouped(Shared(0, 160, 0, "tail-c")), "");
    EXPECT_EQ(Query(table).cpu_share, 160);
    ASSERT_EQ(table.RemoveGrouped(Shared(0, 144, 0, "tail-b")), "");
    EXPECT_EQ(Query(table).cpu_share, 0);
}

TEST(SpecialKVHashes, DistinctStorageAndEventDomainsMeetAtLogicalBoundary) {
    auto layout = std::make_shared<CacheLayout>(
        *Layout({Group("full", MatchPolicy::kDense),
                 Group("window", MatchPolicy::kWindow, 16, 33)}));
    layout->hash_domains = {HashDomain::kSglangStorage,
                            HashDomain::kSglangDynamo};
    const auto profile = Profile(true);
    auto registration = Registration(layout, "p0", 0, profile);
    registration.publisher_kind = PublisherKind::kSglang;
    PrefixCacheTable table;
    ASSERT_TRUE(table.Register(registration).error.empty());
    GroupMutation full{.context = Context(),
                       .group = 0,
                       .owner = EngineOwner{"native", "p0", 0},
                       .hash_domain = HashDomain::kSglangDynamo};
    for (const auto& hash : Hashes(profile, HashDomain::kSglangDynamo, "salt"))
        full.prefixes.push_back(hash.projected);
    ASSERT_EQ(table.StoreGrouped(full), "");
    const auto storage = Hashes(profile, HashDomain::kSglangStorage, "salt");
    GroupMutation window{
        .context = Context(),
        .prefixes = {storage[8].projected, storage[9].projected},
        .group = 1,
        .part = 0,
        .tier = StorageTier::kCpuShare,
        .owner = SharedObjectOwner{"raw", "backend", "window"},
        .storage_domain = "pool",
        .hash_domain = HashDomain::kSglangStorage};
    ASSERT_EQ(table.StoreGrouped(window), "");
    const auto matched = table.Query(Context(), Tokens(), "salt").at("p0");
    EXPECT_EQ(matched.device, 0);
    EXPECT_EQ(matched.cpu_share, 160);
    EXPECT_EQ(table.Query(Context(), Tokens(), "wrong-salt").at("p0").cpu_share,
              0);
}

std::string FromHex(const std::string& text) {
    std::string data;
    for (size_t i = 0; i < text.size(); i += 2)
        data.push_back(
            static_cast<char>(std::stoul(text.substr(i, 2), nullptr, 16)));
    return data;
}

TEST(SpecialKVEvents, DecodesUnmodifiedProducerSerializerFixtures) {
    const auto fixture = test::LoadJsonFixture("special_kv_vectors.json");
    const auto vl_bytes =
        FromHex(fixture["vllm_checkpoint_msgpack"].asString());
    const auto vl = zmq::DecodeVllmEventBatch(vl_bytes.data(), vl_bytes.size());
    ASSERT_TRUE(vl.ok) << vl.error;
    ASSERT_EQ(vl.batch.events.size(), 1u);
    ASSERT_TRUE(vl.batch.events[0].ok()) << vl.batch.events[0].error;
    const auto& checkpoint =
        std::get<zmq::VllmStoredEvent>(*vl.batch.events[0].event);
    EXPECT_EQ(checkpoint.group_idx, 1);
    EXPECT_EQ(checkpoint.kv_cache_spec_kind, "mamba");
    ASSERT_TRUE(checkpoint.token_ids);
    EXPECT_TRUE(checkpoint.token_ids->empty());
    const auto sg_bytes = FromHex(fixture["sglang_page_msgpack"].asString());
    const auto sg =
        zmq::DecodeSglangEventBatch(sg_bytes.data(), sg_bytes.size());
    ASSERT_TRUE(sg.ok) << sg.error;
    ASSERT_EQ(sg.batch.events.size(), 1u);
    ASSERT_TRUE(sg.batch.events[0].ok()) << sg.batch.events[0].error;
    const auto& page =
        std::get<zmq::SglangStoredEvent>(*sg.batch.events[0].event);
    EXPECT_EQ(page.block_hashes.at(0), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(page.cache_salt, "tenant-A");
}

zmq::MessageMetadata Metadata(const common::ServiceConfig& service) {
    zmq::MessageMetadata metadata;
    metadata.endpoint = service.endpoint;
    metadata.publisher_kind = service.publisher_kind;
    return metadata;
}

zmq::VllmEventBatch Batch(zmq::VllmEvent event) {
    zmq::VllmEventBatch batch;
    zmq::DecodedEvent<zmq::VllmEvent> decoded;
    decoded.event = std::move(event);
    batch.events.push_back(std::move(decoded));
    return batch;
}

TEST(SpecialKVEvents, VllmCheckpointEmptyTokensAndStaleRetraction) {
    const auto service = Service();
    const auto binding = Binding();
    EventManager manager({}, 0);
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(Registration(binding.layout))
                    .error.empty());
    KVEventHandler handler(&manager, service, binding);
    zmq::VllmStoredEvent event;
    event.block_size = 16;
    event.medium = "gpu";
    event.group_idx = 0;
    event.kv_cache_spec_kind = "full_attention";
    for (const auto& hash : Hashes())
        event.block_hashes.emplace_back(hash.projected.value);
    ASSERT_EQ(handler.HandleBatch(Batch(event), Metadata(service)), "");
    event.block_hashes = {Hashes().back().projected.value};
    event.group_idx = 1;
    event.kv_cache_spec_kind = "mamba";
    event.token_ids = std::vector<int32_t>{};
    ASSERT_EQ(handler.HandleBatch(Batch(event), Metadata(service)), "");
    EXPECT_EQ(Query(*manager.GetIndexer()).device, 160);
    handler.OnSourceStale("p0", Metadata(service), "unrecoverable gap");
    EXPECT_EQ(Query(*manager.GetIndexer()).device, 0);
}

TEST(SpecialKVEvents, PlaceholderAndUnknownGroupCannotContributeHits) {
    const auto service = Service();
    const auto binding = Binding();
    EventManager manager({}, 0);
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(Registration(binding.layout))
                    .error.empty());
    KVEventHandler handler(&manager, service, binding);
    zmq::VllmStoredEvent event;
    event.block_size = 0;
    event.medium = "gpu";
    event.group_idx = 1;
    event.kv_cache_spec_kind = "mamba";
    event.block_hashes = {Hashes().back().projected.value};
    ASSERT_EQ(handler.HandleBatch(Batch(event), Metadata(service)), "");
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().contexts.at(0).prefix_count,
              0u);
    event.block_size = 16;
    event.group_idx = 9;
    ASSERT_EQ(handler.HandleBatch(Batch(event), Metadata(service)), "");
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().contexts.at(0).prefix_count,
              0u);
}

TEST(SpecialKVEvents, NativeDenseAliasesRetainPhysicalRecoveryBoundary) {
    const auto service = Service();
    auto binding = Binding();
    binding.layout = Layout({Group("full", MatchPolicy::kDense, 32),
                             Group("state", MatchPolicy::kCheckpoint)});
    EventManager manager({}, 0);
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(Registration(binding.layout))
                    .error.empty());
    KVEventHandler handler(&manager, service, binding);
    zmq::VllmStoredEvent event;
    event.block_size = 16;
    event.medium = "gpu";
    event.group_idx = 0;
    event.kv_cache_spec_kind = "full_attention";
    event.block_hashes = {Hashes().at(1).projected.value};
    ASSERT_EQ(handler.HandleBatch(Batch(event), Metadata(service)), "");
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().contexts.at(0).prefix_count,
              1u);
    ASSERT_EQ(manager.GetIndexer()->StoreGrouped(Engine(1, {16, 32})), "");
    EXPECT_EQ(Query(*manager.GetIndexer()).device, 32);
    EXPECT_EQ(
        manager.GetIndexer()->Query(Context(), Tokens(16)).at("p0").device, 0);
}

TEST(SpecialKVEvents, NativeSglangPagesCannotManufactureIndependentComponents) {
    auto layout = std::make_shared<CacheLayout>(*Layout());
    layout->hash_domains = {HashDomain::kSglangEvent,
                            HashDomain::kSglangStorage};
    layout->groups[1] = Group("state", MatchPolicy::kWindow, 16, 33);
    SourceLayout binding{.layout = layout,
                         .source = LayoutSource::kSglang,
                         .groups = {{0, 0}},
                         .hash_domain = HashDomain::kSglangEvent};
    auto service = Service(PublisherKind::kSglang);
    auto registration =
        Registration(binding.layout, "p0", 0, service.hash_profile);
    registration.publisher_kind = PublisherKind::kSglang;
    EventManager manager({}, 0);
    ASSERT_TRUE(manager.GetIndexer()->Register(registration).error.empty());
    KVEventHandler handler(&manager, service, binding);
    zmq::SglangStoredEvent event;
    event.block_size = 16;
    event.medium = "gpu";
    for (const auto& hash :
         Hashes(service.hash_profile, HashDomain::kSglangEvent))
        event.block_hashes.push_back(hash.projected.value);
    zmq::SglangEventBatch batch;
    batch.events.push_back({.event = event});
    ASSERT_EQ(handler.HandleBatch(batch, Metadata(service)), "");
    EXPECT_EQ(Query(*manager.GetIndexer()).disk, 0);
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().contexts.at(0).prefix_count,
              10u);
    handler.OnSourceStale("p0", Metadata(service), "unrecoverable gap");
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().contexts.at(0).prefix_count,
              0u);
}

TEST(SpecialKVEvents, ConnectorCpuAnnouncementsDoNotChangeGroupedResidency) {
    const auto binding = Binding();
    const auto service = Service();
    EventManager manager({}, 0);
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(Registration(binding.layout))
                    .error.empty());
    StoreFull(*manager.GetIndexer());
    KVEventHandler handler(&manager, service, binding);
    zmq::VllmStoredEvent event;
    event.block_size = 16;
    event.medium = "cpu";
    event.block_hashes = {Hashes().back().projected.value};
    for (int64_t group : {0, 1}) {
        event.group_idx = group;
        ASSERT_EQ(handler.HandleBatch(Batch(event), Metadata(service)), "");
    }
    EXPECT_EQ(Query(*manager.GetIndexer()).device, 0);
    EXPECT_EQ(Query(*manager.GetIndexer()).disk, 0);
    ASSERT_EQ(manager.GetIndexer()->StoreGrouped(Engine(1, {160})), "");
    zmq::VllmRemovedEvent removed;
    removed.medium = "cpu";
    removed.group_idx = 1;
    removed.block_hashes = event.block_hashes;
    ASSERT_EQ(handler.HandleBatch(Batch(removed), Metadata(service)), "");
    EXPECT_EQ(Query(*manager.GetIndexer()).device, 160);
}

TEST(SpecialKVEvents, MooncakeMinimalRemoveRetractsOneRequiredObject) {
    auto layout = std::make_shared<CacheLayout>(*Layout());
    layout->groups[1].parts = {"k", "v"};
    SourceLayout binding{.layout = layout,
                         .source = LayoutSource::kMooncake,
                         .objects = {{"namespace/", "_k", 1, 0, "pool-copy"},
                                     {"namespace/", "_v", 1, 1, "pool-copy"}},
                         .storage_domain = "pool"};
    auto service = Service(PublisherKind::kMooncake);
    EventManager manager({}, 0);
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(Registration(binding.layout))
                    .error.empty());
    StoreFull(*manager.GetIndexer());
    KVEventHandler handler(&manager, service, binding);
    const std::string key = "namespace/" + DigestToHex(Hashes().back().digest);
    auto send = [&](auto event) {
        zmq::MooncakeEventBatch batch;
        zmq::DecodedEvent<zmq::MooncakeEvent> decoded;
        decoded.event = std::move(event);
        batch.events.push_back(std::move(decoded));
        EXPECT_EQ(handler.HandleBatch(batch, Metadata(service)), "");
    };
    zmq::MooncakeStoredEvent stored;
    stored.fields.backend_id = "backend";
    stored.fields.tenant_id = "default";
    stored.fields.medium = "cpu";
    stored.object.object_key = key + "_k";
    send(stored);
    EXPECT_EQ(Query(*manager.GetIndexer()).cpu_share, 0);
    stored.object.object_key = key + "_v";
    send(stored);
    EXPECT_EQ(Query(*manager.GetIndexer()).cpu_share, 160);
    zmq::MooncakeRemovedEvent removed;
    removed.fields.backend_id = "backend";
    removed.fields.tenant_id = "default";
    removed.object.object_key =
        key + "_k";  // no model/group/block-size/medium assertions
    send(removed);
    EXPECT_EQ(Query(*manager.GetIndexer()).cpu_share, 0);
}

Json::Value AscendDeployment(
    bool layerwise = false,
    const std::string& physical_model = "GLM-5.3-Flash") {
    Json::Value root;
    root["format"] = "ascend-store-v1";
    root["model_name"] = physical_model;
    root["hash_unit_tokens"] = 16;
    root["alignment_tokens"] = 64;
    root["storage_domain"] = "ascend://master:1";
    root["transient_alignment_tokens"] = 4;
    if (layerwise) root["object_key_format"] = "mooncake-layerwise-v1";
    root["groups"] = Json::arrayValue;
    for (int id = 0; id < 2; ++id) {
        Json::Value group;
        group["group_idx"] = id;
        group["kind"] = id == 0 ? "mla_attention" : "mamba";
        group["block_tokens"] = id == 0 ? 16 : 64;
        if (id == 1) group["mamba_cache_mode"] = "align";
        group["key_prefixes"] = Json::arrayValue;
        for (int rank = 0; rank < (id == 0 ? 1 : 2); ++rank) {
            if (layerwise) {
                group["key_prefixes"].append(
                    physical_model + "@mooncake_hybrid_v1:" +
                    std::string(64, 'a') + "@group:" + std::to_string(id) +
                    "@block:" + std::to_string(id == 0 ? 16 : 64) + "@");
                group["key_suffixes"].append("@" + std::to_string(rank));
            } else {
                group["key_prefixes"].append(
                    physical_model +
                    "@dcp:0@head_or_tp_rank:" + std::to_string(rank) +
                    "@pp_rank:0@group:" + std::to_string(id) +
                    "@cache_role:kv@cache_family:c1@");
            }
        }
        root["groups"].append(group);
    }
    return root;
}

class SpecialKVDeploymentFormats : public ::testing::TestWithParam<bool> {};

TEST_P(SpecialKVDeploymentFormats, BindsOnlyAConsistentPhysicalModelNamespace) {
    auto service = Service(PublisherKind::kVllm);
    service.model_name = "GLM-5.3-Flash";
    const std::string physical = "GLM-5.3-Flash-w8a8-4layers-rand";
    auto declaration = AscendDeployment(GetParam(), physical);
    SourceLayout binding;
    auto resolve = [&] {
        return ResolveVllmDeployment(service, declaration.toStyledString(),
                                     &binding);
    };
    ASSERT_EQ(resolve(), "");
    EXPECT_EQ(binding.layout->id, service.model_name);
    EXPECT_EQ(binding.physical_model_name, physical);

    declaration["model_name"] = "other-weights";
    EXPECT_FALSE(resolve().empty());
    declaration["model_name"] = "";
    if (GetParam()) {
        EXPECT_FALSE(resolve().empty());
        return;
    }
    ASSERT_EQ(resolve(), "");
    EXPECT_EQ(binding.physical_model_name, physical);
    auto& last_prefix = declaration["groups"][1]["key_prefixes"][1];
    const auto suffix = last_prefix.asString().substr(physical.size());
    last_prefix = "other-weights" + suffix;
    EXPECT_FALSE(resolve().empty());
    last_prefix = suffix;  // No model component at all.
    EXPECT_FALSE(resolve().empty());
}

TEST_P(SpecialKVDeploymentFormats,
       CompleteDeclarationIsValidatedBeforeActivation) {
    auto service = Service(PublisherKind::kVllm);
    service.model_name = "GLM-5.3-Flash";
    SourceLayout binding;
    const auto valid = AscendDeployment(GetParam());
    ASSERT_EQ(ResolveVllmDeployment(service, valid.toStyledString(), &binding),
              "");
    EXPECT_EQ(binding.layout->groups.size(), 2u);
    EXPECT_EQ(binding.layout->groups[1].match, MatchPolicy::kCheckpoint);
    EXPECT_EQ(binding.layout->groups[1].parts.size(), 2u);
    for (const auto* field : {"format", "model_name", "hash_unit_tokens",
                              "storage_domain", "groups"}) {
        auto invalid = valid;
        invalid[field] = Json::arrayValue;
        EXPECT_FALSE(
            ResolveVllmDeployment(service, invalid.toStyledString(), &binding)
                .empty())
            << field;
    }
    auto invalid = valid;
    invalid["groups"][1]["mamba_cache_mode"] = "none";
    EXPECT_FALSE(
        ResolveVllmDeployment(service, invalid.toStyledString(), &binding)
            .empty());
    invalid = valid;
    invalid["groups"][1]["key_prefixes"][1] =
        invalid["groups"][1]["key_prefixes"][0];
    if (GetParam()) invalid["groups"][1]["key_suffixes"][1] = "@0";
    EXPECT_FALSE(
        ResolveVllmDeployment(service, invalid.toStyledString(), &binding)
            .empty());
    if (GetParam()) {
        for (const auto& suffix : {Json::Value("@2"), Json::Value(0)}) {
            invalid = valid;
            invalid["groups"][1]["key_suffixes"][1] = suffix;
            EXPECT_FALSE(ResolveVllmDeployment(
                             service, invalid.toStyledString(), &binding)
                             .empty());
        }
        invalid = valid;
        invalid["groups"][1].removeMember("key_suffixes");
        EXPECT_FALSE(
            ResolveVllmDeployment(service, invalid.toStyledString(), &binding)
                .empty());
        invalid = valid;
        invalid.removeMember("object_key_format");
        EXPECT_FALSE(
            ResolveVllmDeployment(service, invalid.toStyledString(), &binding)
                .empty());
    }
    invalid = valid;
    invalid["groups"].resize(1);
    EXPECT_FALSE(
        ResolveVllmDeployment(service, invalid.toStyledString(), &binding)
            .empty());
}

TEST(SpecialKVDeployment, UnrelatedStoreCannotInheritAnotherPoolsReadDomain) {
    EventManager manager({}, 0);
    auto store = Service(PublisherKind::kMooncake);
    store.model_name = "GLM-5.3-Flash";
    const auto key = kvevent::MakeServiceKey(store.instance_id, store.tenant_id,
                                             store.dp_rank);
    kvevent::EventManagerTestPeer::FakeActiveService(manager, key, store);
    auto other = store;
    other.instance_id = "unrelated-store";
    other.endpoint = "tcp://127.0.0.1:49003";
    const auto [inserted, error] =
        kvevent::EventManagerTestPeer::Subscribe(manager, other);
    EXPECT_FALSE(inserted);
    EXPECT_NE(error.find("unambiguous Mooncake publisher"), std::string::npos);
    EXPECT_EQ(kvevent::EventManagerTestPeer::ActiveConfigCount(manager), 1u);
}

TEST_P(SpecialKVDeploymentFormats,
       EarlyObjectEventsReplayAndRequiredStateShardRemovalRetractsHit) {
    auto service = Service(PublisherKind::kVllm);
    service.model_name = "GLM-5.3-Flash";
    const ContextKey context{"default", service.model_name, "", 16};
    EventManager manager({}, 0);
    const auto placeholder = ResolveModelLayout(service);
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register({.context = context,
                                .profile = service.hash_profile,
                                .instance_id = service.instance_id,
                                .effective_block_size = 16,
                                .layout = placeholder.layout})
                    .error.empty());
    KVEventHandler engine(&manager, service, placeholder);
    auto store_service = service;
    store_service.publisher_kind = PublisherKind::kMooncake;
    store_service.endpoint = "tcp://127.0.0.1:49002";
    store_service.instance_id = "master";
    const auto key = kvevent::MakeServiceKey("master", "default", 0);
    kvevent::EventManagerTestPeer::FakeActiveService(
        manager, key, store_service, ResolveModelLayout(store_service));
    const auto store = kvevent::EventManagerTestPeer::HandlerFor(manager, key);
    std::string error;
    auto strategy = CreateHashStrategy(service.hash_profile, &error);
    ASSERT_NE(strategy, nullptr) << error;
    std::vector<HashBlock> hashes;
    ASSERT_EQ(strategy->Compute(context, Tokens(), std::nullopt, &hashes), "");
    const std::string physical = "GLM-5.3-Flash-w8a8-4layers-rand";
    auto declaration = AscendDeployment(GetParam(), physical);
    if (!GetParam()) declaration["model_name"] = "";
    auto object_key = [&](int group, int part, size_t index) {
        return declaration["groups"][group]["key_prefixes"][part].asString() +
               DigestToHex(hashes.at(index).digest) +
               (GetParam() ? declaration["groups"][group]["key_suffixes"][part]
                                 .asString()
                           : "");
    };
    auto send = [&](auto event) {
        zmq::MooncakeEventBatch batch;
        batch.events.push_back({.event = zmq::MooncakeEvent(std::move(event))});
        EXPECT_EQ(store->HandleBatch(batch, Metadata(store_service)), "");
    };
    zmq::MooncakeStoredEvent stored;
    stored.fields.backend_id = "master";
    stored.fields.tenant_id = "default";
    stored.fields.medium = "cpu";
    stored.fields.model_name = service.model_name;
    for (size_t i = 0; i < hashes.size(); ++i) {
        stored.object.object_key = object_key(0, 0, i);
        send(stored);
    }
    stored.fields.model_name.reset();
    for (int part = 0; part < 2; ++part) {
        stored.object.object_key = object_key(1, part, 7);
        send(stored);
    }
    zmq::MooncakeRemovedEvent removed;
    removed.fields.backend_id = "master";
    removed.fields.tenant_id = "default";
    removed.fields.model_name = physical;
    removed.object.object_key = object_key(1, 0, 7);
    send(removed);  // Preserve Store/Remove order even before declaration.
    auto hit = [&] {
        return manager.GetIndexer()
            ->Query(context, Tokens())
            .at(service.instance_id)
            .cpu_share;
    };
    EXPECT_EQ(hit(), 0);
    zmq::VllmStoredEvent announcement;
    announcement.block_size = 16;
    announcement.medium = "cpu";
    announcement.kv_cache_layout = declaration.toStyledString();
    zmq::VllmEventBatch batch;
    batch.events.push_back({.event = zmq::VllmEvent(announcement)});
    ASSERT_EQ(engine.HandleBatch(batch, Metadata(service)), "");
    EXPECT_EQ(hit(), 0);
    stored.fields.model_name = service.model_name;
    stored.object.object_key = "other-" + object_key(1, 0, 7);
    send(stored);  // Same hash and claimed context, different physical key.
    EXPECT_EQ(hit(), 0);
    stored.object.object_key = object_key(1, 0, 7);
    stored.fields.model_name = "other-weights";
    send(stored);  // Matching key but conflicting model assertion.
    EXPECT_EQ(hit(), 0);
    stored.fields.model_name = physical;
    send(stored);
    EXPECT_EQ(hit(), 128);
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register({.context = context,
                                .profile = service.hash_profile,
                                .instance_id = "unannounced-engine",
                                .effective_block_size = 16,
                                .layout = placeholder.layout})
                    .error.empty());
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(context, Tokens())
                  .at("unannounced-engine")
                  .cpu_share,
              0);
    send(stored);  // Upsert, not an extra lifetime reference.
    send(removed);
    EXPECT_EQ(hit(), 0);
    auto conflict = declaration;
    conflict["storage_domain"] = "ascend://other-master:1";
    announcement.kv_cache_layout = conflict.toStyledString();
    batch.events[0].event = zmq::VllmEvent(announcement);
    EXPECT_FALSE(engine.HandleBatch(batch, Metadata(service)).empty());
    EXPECT_EQ(hit(), 0);
}

INSTANTIATE_TEST_SUITE_P(StoreKeyFormats, SpecialKVDeploymentFormats,
                         ::testing::Bool());


// Regression guard for deployment activation across instances of one ContextKey.
// Only the instance that publishes the AscendStore declaration runs
// ActivateDeployment(); before the propagation fix every other instance kept an
// empty readable-domain set in its HybridIndex reader, so the shared pool copy
// stayed invisible to it and /query reported cpu_share = 0.
TEST(SpecialKVDeployment, DeclarationFromOneInstanceMakesPoolCopyVisibleToPeers) {
    EventManager manager({}, 0);
    PrefixCacheTable* index = manager.GetIndexer();
    ASSERT_NE(index, nullptr);

    auto p0 = Service();
    p0.model_name = "GLM-5.3-Flash";   // in the cache-spec table: needs grouping
    auto p1 = p0;
    p1.instance_id = "p1";
    p1.endpoint = "tcp://127.0.0.1:49001";
    auto p2 = p0;
    p2.instance_id = "p2";
    p2.endpoint = "tcp://127.0.0.1:49002";
    const ContextKey context{"default", p0.model_name, "", 16};

    // Every instance joins the same ContextKey with the "unavailable" placeholder
    // whose readable_domains is empty: the state a non-declaring instance is
    // stuck in without the fix.
    for (const auto& service : {p0, p1, p2}) {
        const auto placeholder = ResolveModelLayout(service);
        ASSERT_NE(placeholder.layout, nullptr);
        ASSERT_FALSE(placeholder.layout->unavailable_reason.empty());
        ASSERT_TRUE(placeholder.readable_domains.empty());
        const auto registered = index->Register(
            {.context = context,
             .profile = service.hash_profile,
             .instance_id = service.instance_id,
             .dp_rank = service.dp_rank,
             .effective_block_size = 16,
             .layout = placeholder.layout,
             .readable_domains = placeholder.readable_domains});
        ASSERT_EQ(registered.error, "");
    }

    // p0/p1 are wired into the manager; p2 deliberately is not, so no declaration
    // can ever reach it (the pre-fix situation of every peer).
    for (const auto& service : {p0, p1}) {
        kvevent::EventManagerTestPeer::FakeActiveService(
            manager,
            kvevent::MakeServiceKey(service.instance_id, service.tenant_id,
                                    service.dp_rank),
            service);
    }
    auto handler = kvevent::EventManagerTestPeer::HandlerFor(
        manager, kvevent::MakeServiceKey(p0.instance_id, p0.tenant_id,
                                         p0.dp_rank));
    ASSERT_NE(handler, nullptr);

    // AscendStore declaration: one dense attention group plus one aligned
    // checkpoint (KDA) group. GLM Flash also verifies the pooled-indexer tail.
    Json::Value root;
    root["format"] = "ascend-store-v1";
    root["model_name"] = "GLM-5.3-Flash-w8a8";
    root["hash_unit_tokens"] = 16;
    root["alignment_tokens"] = 16;
    root["transient_alignment_tokens"] = 4;
    root["storage_domain"] = "ascend://pool:1";
    const auto append_group = [&root](int64_t id, const char* kind,
                                      bool align) {
        Json::Value group;
        group["group_idx"] = id;
        group["kind"] = kind;
        group["block_tokens"] = 16;
        if (align) group["mamba_cache_mode"] = "align";
        group["key_prefixes"].append(
            std::string("GLM-5.3-Flash-w8a8") +
            "@dcp:0@head_or_tp_rank:0@pp_rank:0@group:" + std::to_string(id) +
            "@cache_role:kv@cache_family:default@");
        root["groups"].append(group);
    };
    append_group(0, "mla_attention", false);
    append_group(2, "mamba", true);

    zmq::VllmStoredEvent event;
    event.block_size = 16;
    event.medium = "GPU";
    event.group_idx = 0;
    event.kv_cache_spec_kind = "mla_attention";
    event.block_hashes = {Hashes().front().projected.value};
    event.kv_cache_layout = root.toStyledString();
    ASSERT_EQ(handler->HandleBatch(Batch(event), Metadata(p0)), "");

    // The declaration supplies the group geometry, so shared objects can be
    // recorded now: one copy for each required group, first hash unit only.
    for (size_t slot = 0; slot < 2; ++slot) {
        GroupMutation shared{
            .context = context,
            .prefixes = {Hashes().front().projected},
            .group = slot,
            .part = size_t{0},
            .tier = StorageTier::kCpuShare,
            .owner = SharedObjectOwner{.source_stream = "store",
                                       .backend_id = "backend",
                                       .object_id =
                                           "object-" + std::to_string(slot)},
            .storage_domain = "ascend://pool:1"};
        ASSERT_EQ(index->StoreGrouped(shared), "") << "slot " << slot;
    }

    const auto hits = index->Query(context, Tokens());
    ASSERT_EQ(hits.count("p0"), 1u);
    ASSERT_EQ(hits.count("p1"), 1u);
    ASSERT_EQ(hits.count("p2"), 1u);
    // Only the first hash unit was published, so the joint prefix ends there.
    // Both instances reached by the propagation resolve the shared copy without
    // ever storing it themselves.
    EXPECT_EQ(hits.at("p0").cpu_share, 16);
    EXPECT_EQ(hits.at("p1").cpu_share, 16);
    EXPECT_EQ(hits.at("p0").device, 0);
    EXPECT_EQ(hits.at("p1").device, 0);
    // p2 never received the deployment: its reader keeps an empty domain set, so
    // the same shared copy stays invisible. This pins the pre-fix behaviour.
    EXPECT_EQ(hits.at("p2").cpu_share, 0);
}

}  // namespace
}  // namespace mooncake::conductor
