// Configuration parsing and validation for V2.
//
// Two things here are load-bearing rather than cosmetic:
//
//  - An ASCEND tier must be a hard error, never a skipped entry. Skipping it
//    would leave an operator believing data lives on the NPU while it silently
//    landed in DRAM.
//  - Every tiers[] entry becomes one independent logical tiler with its own
//    UUID and its own Master segment. Merging entries by type would change the
//    topology reported to Master, so entries that look alike must stay apart.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "p2p/client/data_manager_factory.h"
#include "p2p/client/v2/block_index.h"
#include "p2p/client/v2/data_manager_v2.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

Json::Value ParseJson(const std::string& text) {
    Json::Value value;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;
    EXPECT_TRUE(
        reader->parse(text.data(), text.data() + text.size(), &value, &errs))
        << errs;
    return value;
}

tl::expected<DataManagerV2Config, ErrorCode> Parse(const std::string& text) {
    return ParseDataManagerV2Config(ParseJson(text), LocalTransferConfig{},
                                    KeyLeaseConfig{});
}

}  // namespace

class V2ConfigTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("V2ConfigTest");
            FLAGS_logtostderr = 1;
        });
    }
};

// ---------------------------------------------------------------------------
// Version selection
// ---------------------------------------------------------------------------

TEST_F(V2ConfigTest, VersionParsingAcceptsBothSpellingsAndRejectsGarbage) {
    EXPECT_EQ(ParseDataManagerVersion("v1"), DataManagerVersion::kV1);
    EXPECT_EQ(ParseDataManagerVersion("V1"), DataManagerVersion::kV1);
    EXPECT_EQ(ParseDataManagerVersion("1"), DataManagerVersion::kV1);
    EXPECT_EQ(ParseDataManagerVersion("v2"), DataManagerVersion::kV2);
    EXPECT_EQ(ParseDataManagerVersion("V2"), DataManagerVersion::kV2);
    EXPECT_FALSE(ParseDataManagerVersion("v3").has_value());
    EXPECT_FALSE(ParseDataManagerVersion("").has_value());

    // The default must stay v1 until V2 clears its acceptance criteria.
    EXPECT_EQ(DataManagerConfig{}.version, DataManagerVersion::kV1);
}

// ---------------------------------------------------------------------------
// Tier mapping
// ---------------------------------------------------------------------------

TEST_F(V2ConfigTest, EachTierEntryBecomesOneIndependentTiler) {
    auto config = Parse(R"({
        "tiers": [
            {"type": "DRAM", "capacity": 1048576, "priority": 100,
             "tags": ["a"], "numa_node": 0},
            {"type": "DRAM", "capacity": 2097152, "priority": 50,
             "tags": ["b"]},
            {"type": "STORAGE", "capacity": 4194304, "priority": 10,
             "file_path": "/tmp/mooncake_v2_config_test.data"}
        ]
    })");
    ASSERT_TRUE(config.has_value()) << toString(config.error());
    ASSERT_EQ(config->tilers.size(), 3U);

    // Same medium, same shape: still two separate layers with distinct ids.
    EXPECT_EQ(config->tilers[0].logical.memory_type, MemoryType::DRAM);
    EXPECT_EQ(config->tilers[1].logical.memory_type, MemoryType::DRAM);
    EXPECT_NE(config->tilers[0].logical.tiler_id,
              config->tilers[1].logical.tiler_id);
    EXPECT_EQ(config->tilers[0].logical.priority, 100);
    EXPECT_EQ(config->tilers[1].logical.priority, 50);
    ASSERT_EQ(config->tilers[0].logical.tags.size(), 1U);
    EXPECT_EQ(config->tilers[0].logical.tags[0], "a");

    ASSERT_TRUE(
        std::holds_alternative<DramBlockPoolConfig>(config->tilers[0].pool));
    const auto& dram = std::get<DramBlockPoolConfig>(config->tilers[0].pool);
    ASSERT_EQ(dram.arenas.size(), 1U);
    EXPECT_EQ(dram.arenas[0].capacity_bytes, 1048576U);
    ASSERT_TRUE(dram.arenas[0].numa_node.has_value());
    EXPECT_EQ(*dram.arenas[0].numa_node, 0);

    EXPECT_EQ(config->tilers[2].logical.memory_type, MemoryType::NVME);
    ASSERT_TRUE(
        std::holds_alternative<SSDBlockPoolConfig>(config->tilers[2].pool));
    const auto& ssd = std::get<SSDBlockPoolConfig>(config->tilers[2].pool);
    ASSERT_EQ(ssd.devices.size(), 1U);
    EXPECT_EQ(ssd.devices[0].capacity_bytes, 4194304U);
    EXPECT_EQ(ssd.devices[0].file_path, "/tmp/mooncake_v2_config_test.data");
}

// Skipping the tier would be the dangerous outcome: the deployment would look
// healthy while its data quietly landed somewhere else.
TEST_F(V2ConfigTest, AscendTiersAreRejectedRatherThanSkipped) {
    for (const char* type : {"ASCEND_NPU", "ASCEND"}) {
        auto config = Parse(std::string(R"({"tiers": [
            {"type": "DRAM", "capacity": 1048576, "priority": 100},
            {"type": ")") + type +
                            R"(", "capacity": 1048576, "priority": 50}
        ]})");
        ASSERT_FALSE(config.has_value()) << type << " was accepted";
        EXPECT_EQ(config.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST_F(V2ConfigTest, MalformedTierEntriesAreRejected) {
    EXPECT_EQ(Parse(R"({})").error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(Parse(R"({"tiers": []})").error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(Parse(R"({"tiers": [{"type": "DRAM", "priority": 1}]})").error(),
              ErrorCode::INVALID_PARAMS)
        << "a tier with no capacity must be rejected";
    EXPECT_EQ(
        Parse(R"({"tiers": [{"type": "TAPE", "capacity": 1024}]})").error(),
        ErrorCode::INVALID_PARAMS)
        << "an unknown medium must be rejected";
}

// A storage tier with no file_path and no MOONCAKE_OFFLOAD_FILE_STORAGE_PATH
// has nowhere to put its data; guessing would risk colliding with V1's files,
// whose space management is incompatible.
TEST_F(V2ConfigTest, StorageTierWithoutAPathIsRejected) {
    const char* previous = std::getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH");
    const std::string saved = previous == nullptr ? "" : previous;
    unsetenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH");

    auto config =
        Parse(R"({"tiers": [{"type": "STORAGE", "capacity": 1048576}]})");
    EXPECT_FALSE(config.has_value());

    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "/tmp/mooncake_v2_cfg", 1);
    auto derived =
        Parse(R"({"tiers": [{"type": "STORAGE", "capacity": 1048576}]})");
    ASSERT_TRUE(derived.has_value()) << toString(derived.error());
    const auto& ssd = std::get<SSDBlockPoolConfig>(derived->tilers[0].pool);
    // The v2 prefix is what keeps V2 off V1's data files.
    EXPECT_NE(ssd.devices[0].file_path.find("mooncake_v2_tier_"),
              std::string::npos);

    if (saved.empty()) {
        unsetenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH");
    } else {
        setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", saved.c_str(), 1);
    }
}

// ---------------------------------------------------------------------------
// v2 block
// ---------------------------------------------------------------------------

TEST_F(V2ConfigTest, GlobalDefaultsAreOverriddenByTheV2Block) {
    auto defaults =
        Parse(R"({"tiers": [{"type": "DRAM", "capacity": 1048576}]})");
    ASSERT_TRUE(defaults.has_value());
    EXPECT_EQ(defaults->registry.shard_count, 64U);
    EXPECT_EQ(defaults->block_index.shard_count, 64U);
    EXPECT_TRUE(defaults->allocation_failure.try_evict);
    EXPECT_EQ(defaults->allocation_failure.max_evict_rounds, 1U);

    auto tuned = Parse(R"({
        "tiers": [{"type": "DRAM", "capacity": 1048576}],
        "v2": {
            "block_index": {"shard_count": 128, "max_load_factor": 0.5},
            "block_registry": {"shard_count": 32},
            "allocation_failure": {
                "try_evict": false, "max_evict_rounds": 3,
                "evict_timeout_ms": 25, "reclaim_margin_bytes": 4096
            }
        }
    })");
    ASSERT_TRUE(tuned.has_value()) << toString(tuned.error());
    EXPECT_EQ(tuned->block_index.shard_count, 128U);
    EXPECT_FLOAT_EQ(tuned->block_index.max_load_factor, 0.5F);
    EXPECT_EQ(tuned->registry.shard_count, 32U);
    EXPECT_FALSE(tuned->allocation_failure.try_evict);
    EXPECT_EQ(tuned->allocation_failure.reclaim_margin_bytes, 4096U);
}

// A per-tier override replaces the global policy wholesale. There is
// deliberately no field-level merge: a half-inherited policy is very hard to
// reason about when you are looking at an eviction storm.
TEST_F(V2ConfigTest, PerTierAllocationFailureOverrideReplacesTheGlobalOne) {
    auto config = Parse(R"({
        "tiers": [
            {"type": "DRAM", "capacity": 1048576, "priority": 100},
            {"type": "DRAM", "capacity": 1048576, "priority": 50,
             "allocation_failure": {"try_evict": false}}
        ],
        "v2": {"allocation_failure": {"try_evict": true,
                                      "max_evict_rounds": 2}}
    })");
    ASSERT_TRUE(config.has_value()) << toString(config.error());
    EXPECT_FALSE(config->tilers[0].allocation_failure_override.has_value());
    ASSERT_TRUE(config->tilers[1].allocation_failure_override.has_value());
    EXPECT_FALSE(config->tilers[1].allocation_failure_override->try_evict);
    EXPECT_TRUE(config->allocation_failure.try_evict);
    EXPECT_EQ(config->allocation_failure.max_evict_rounds, 2U);
}

TEST_F(V2ConfigTest, InvalidShardCountsAndLoadFactorsAreRejected) {
    EXPECT_EQ(Parse(R"({
        "tiers": [{"type": "DRAM", "capacity": 1048576}],
        "v2": {"block_registry": {"shard_count": 0}}
    })")
                  .error(),
              ErrorCode::INVALID_PARAMS);

    EXPECT_EQ(Parse(R"({
        "tiers": [{"type": "DRAM", "capacity": 1048576}],
        "v2": {"block_index": {"shard_count": 0}}
    })")
                  .error(),
              ErrorCode::INVALID_PARAMS);

    EXPECT_EQ(Parse(R"({
        "tiers": [{"type": "DRAM", "capacity": 1048576}],
        "v2": {"block_index": {"max_load_factor": 0.0}}
    })")
                  .error(),
              ErrorCode::INVALID_PARAMS);

    EXPECT_EQ(Parse(R"({
        "tiers": [{"type": "DRAM", "capacity": 1048576}],
        "v2": {"block_index": {"max_load_factor": 1.5}}
    })")
                  .error(),
              ErrorCode::INVALID_PARAMS);
}

// ---------------------------------------------------------------------------
// Allocation-failure policy validation
// ---------------------------------------------------------------------------

TEST_F(V2ConfigTest, AllocationFailurePolicyBoundsAreEnforced) {
    AllocationFailurePolicyConfig policy;
    EXPECT_TRUE(ValidateAllocationFailurePolicy(policy).has_value());

    policy.max_evict_rounds = 0;
    EXPECT_EQ(ValidateAllocationFailurePolicy(policy).error(),
              ErrorCode::INVALID_PARAMS)
        << "try_evict with zero rounds would evict nothing and retry nothing";

    policy.max_evict_rounds = 1000;
    EXPECT_EQ(ValidateAllocationFailurePolicy(policy).error(),
              ErrorCode::INVALID_PARAMS)
        << "unbounded rounds turn an allocation failure into a latency cliff";

    policy.max_evict_rounds = 1;
    policy.evict_timeout = std::chrono::milliseconds(0);
    EXPECT_EQ(ValidateAllocationFailurePolicy(policy).error(),
              ErrorCode::INVALID_PARAMS);

    policy.evict_timeout = std::chrono::milliseconds(60000);
    EXPECT_EQ(ValidateAllocationFailurePolicy(policy).error(),
              ErrorCode::INVALID_PARAMS)
        << "a minute of synchronous reclaim on the request path is not a bound";

    // With eviction disabled the remaining fields are unused, so a stale value
    // there cannot change behaviour and must not fail startup.
    AllocationFailurePolicyConfig disabled;
    disabled.try_evict = false;
    disabled.max_evict_rounds = 0;
    disabled.evict_timeout = std::chrono::milliseconds(0);
    EXPECT_TRUE(ValidateAllocationFailurePolicy(disabled).has_value());
}

TEST_F(V2ConfigTest, BlockIndexConfigValidationMatchesTheDocumentedRange) {
    EXPECT_TRUE(ValidateBlockIndexConfig(BlockIndexConfig{}).has_value());
    EXPECT_TRUE(
        ValidateBlockIndexConfig(BlockIndexConfig{64, 1.0F}).has_value());
    EXPECT_EQ(ValidateBlockIndexConfig(BlockIndexConfig{0, 0.5F}).error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ValidateBlockIndexConfig(BlockIndexConfig{64, -0.1F}).error(),
              ErrorCode::INVALID_PARAMS);
}

// ---------------------------------------------------------------------------
// The factory must not overwrite what the file said
// ---------------------------------------------------------------------------

// `v2.stop_drain_timeout_ms` was parsed, validated, and then assigned over by
// CreateV2 on every call -- so the knob existed in the schema, in the parser
// and in the tests, and did nothing in production. This is the end-to-end
// check that it reaches the running manager, and the duration is the only
// observable that distinguishes "the file was honoured" from "the default
// was". Before the fix Stop() would wait the 5s default here.
TEST(DataManagerFactoryConfigTest, TheTierFileSetsTheStopDrainTimeout) {
    static constexpr const char* kJson = R"({
  "v2": {"stop_drain_timeout_ms": 100},
  "tiers": [{"type": "DRAM", "capacity": 4194304, "priority": 10}]
})";

    DataManagerConfig config;
    config.version = DataManagerVersion::kV2;
    config.tier_config = ParseJson(kJson);
    // Local-only: no engine to register pool memory with.
    config.register_tiers_with_transfer_engine = false;
    // Deliberately left unset -- that is what "the file decides" looks like.
    ASSERT_FALSE(config.stop_drain_timeout.has_value());

    auto engine = std::make_shared<TransferEngine>();
    auto manager = CreateDataManager(config, engine, MetadataCallbacks{},
                                     DataManagerMetrics{});
    ASSERT_TRUE(manager.has_value()) << toString(manager.error());

    // An unwaited handle keeps one guard in flight, which is exactly the case
    // the timeout exists for: Stop() cannot wait for a caller that may never
    // call Wait().
    const std::string payload(1024, 'x');
    std::vector<Slice> slices{
        Slice{const_cast<char*>(payload.data()), payload.size()}};
    auto handle = manager.value()->Put("factory/stop", slices);
    ASSERT_TRUE(handle.has_value()) << toString(handle.error());

    const auto started = std::chrono::steady_clock::now();
    manager.value()->Stop();
    const auto elapsed = std::chrono::steady_clock::now() - started;

    auto* v2 = dynamic_cast<DataManagerV2*>(manager.value().get());
    ASSERT_NE(v2, nullptr);
    EXPECT_EQ(v2->Metrics().stop_drain_timeout_hit, 1U)
        << "the outstanding handle should have forced the timeout";
    EXPECT_LT(elapsed, std::chrono::seconds(2))
        << "Stop waited the built-in default, so the tier file's "
           "stop_drain_timeout_ms was ignored";

    // Ordered so the handle is released before the manager it points into.
    handle.value().reset();
    manager.value()->Destroy();
}

// ---------------------------------------------------------------------------
// The copy layer's own knobs
// ---------------------------------------------------------------------------

TEST(DataManagerV2ConfigTest, TheCopierBlockIsParsed) {
    auto config = Parse(R"({
  "v2": {"copier": {"staging_buffer_bytes": "8MB", "copy_timeout_ms": 750}},
  "tiers": [{"type": "DRAM", "capacity": 1048576, "priority": 10}]
})");
    ASSERT_TRUE(config.has_value()) << toString(config.error());
    EXPECT_EQ(config->copier.staging_buffer_bytes, 8ULL * 1024 * 1024);
    EXPECT_EQ(config->copier.copy_timeout, std::chrono::milliseconds(750));
}

TEST(DataManagerV2ConfigTest, TheCopierBlockHasWorkingDefaults) {
    auto config = Parse(R"({
  "tiers": [{"type": "DRAM", "capacity": 1048576, "priority": 10}]
})");
    ASSERT_TRUE(config.has_value());
    EXPECT_GT(config->copier.staging_buffer_bytes, 0U);
    // Unbounded by default: a background migration has nobody waiting on it,
    // and inventing a timeout would cancel work the caller never limited.
    EXPECT_EQ(config->copier.copy_timeout, std::chrono::milliseconds(0));
}

TEST(DataManagerV2ConfigTest, ANegativeCopyTimeoutIsRejected) {
    auto config = Parse(R"({
  "v2": {"copier": {"copy_timeout_ms": -1}},
  "tiers": [{"type": "DRAM", "capacity": 1048576, "priority": 10}]
})");
    ASSERT_FALSE(config.has_value());
    EXPECT_EQ(config.error(), ErrorCode::INVALID_PARAMS);
}

// The eviction ordering was reachable only from C++ until now: every tier got
// the built-in default whatever the file said.
TEST(DataManagerV2ConfigTest, TheEvictionOrderingReachesEveryTier) {
    auto config = Parse(R"({
  "v2": {"eviction_index": {"type": "lru", "max_victim_candidates": 32}},
  "tiers": [
    {"type": "DRAM", "capacity": 1048576, "priority": 10},
    {"type": "DRAM", "capacity": 1048576, "priority": 5}
  ]
})");
    ASSERT_TRUE(config.has_value()) << toString(config.error());
    ASSERT_EQ(config->tilers.size(), 2U);
    for (const auto& tiler : config->tilers) {
        EXPECT_EQ(tiler.eviction.type, "lru");
        EXPECT_EQ(tiler.eviction.max_victim_candidates, 32U);
    }
}

TEST(DataManagerV2ConfigTest, AnUnknownEvictionOrderingIsRejected) {
    auto config = Parse(R"({
  "v2": {"eviction_index": {"type": "random"}},
  "tiers": [{"type": "DRAM", "capacity": 1048576, "priority": 10}]
})");
    ASSERT_FALSE(config.has_value());
    EXPECT_EQ(config.error(), ErrorCode::INVALID_PARAMS);
}

// ---------------------------------------------------------------------------
// Validation is not a property of the parser
// ---------------------------------------------------------------------------

// A configuration assembled in C++ used to reach Init completely unchecked --
// only the JSON path was validated. Every component test builds one that way,
// and so would any embedder, so a zero shard count became a division by zero
// at run time rather than a refusal at start-up.
TEST(DataManagerV2ConfigTest, InitRefusesAConfigTheParserWouldHaveRejected) {
    auto parsed = Parse(R"({
  "tiers": [{"type": "DRAM", "capacity": 4194304, "priority": 10}]
})");
    ASSERT_TRUE(parsed.has_value()) << toString(parsed.error());

    DataManagerV2Config broken = parsed.value();
    broken.register_tiers_with_transfer_engine = false;
    broken.registry.shard_count = 0;  // never reachable from JSON
    ASSERT_FALSE(ValidateDataManagerV2Config(broken).has_value());

    DataManagerV2 manager(broken, std::make_shared<TransferEngine>(),
                          MetadataCallbacks{});
    auto initialized = manager.Init();
    ASSERT_FALSE(initialized.has_value());
    EXPECT_EQ(initialized.error(), ErrorCode::INVALID_PARAMS);
}

// The same config without the corruption still starts, so the case above is
// about the check and not about the fixture being unbuildable.
TEST(DataManagerV2ConfigTest, AValidHandBuiltConfigStillStarts) {
    auto parsed = Parse(R"({
  "tiers": [{"type": "DRAM", "capacity": 4194304, "priority": 10}]
})");
    ASSERT_TRUE(parsed.has_value());
    DataManagerV2Config config = parsed.value();
    config.register_tiers_with_transfer_engine = false;

    DataManagerV2 manager(config, std::make_shared<TransferEngine>(),
                          MetadataCallbacks{});
    EXPECT_TRUE(manager.Init().has_value());
    manager.Stop();
    manager.Destroy();
}

}  // namespace mooncake::v2
