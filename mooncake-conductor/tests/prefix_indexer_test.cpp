// Tests for the prefix index: block-hash computation, store/remove/query
// flows, cache-hit accounting, and the /global_view aggregation.

#include <gtest/gtest.h>

#include <barrier>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "conductor/common/types.h"
#include "conductor/prefixindex/prefix_indexer.h"
#include "prefix_indexer_test_peer.h"

namespace {

using conductor::common::RemovedEvent;
using conductor::common::StoredEvent;
using conductor::prefixindex::ModelContext;
using conductor::prefixindex::PrefixCacheTable;
using conductor::prefixindex::PrefixCacheTableSnapshot;
using conductor::prefixindex::PrefixCacheTableTestPeer;

std::vector<int32_t> Seq(int32_t from, int32_t to) {
    std::vector<int32_t> out;
    for (int32_t i = from; i <= to; ++i) out.push_back(i);
    return out;
}

ModelContext TestContext(const std::string& instance_id = "instance-1",
                         int64_t block_size = 4) {
    ModelContext ctx;
    ctx.model_name = "test-model";
    ctx.lora_name = "none";
    ctx.block_size = block_size;
    ctx.tenant_id = "default";
    ctx.additional_salt = "";
    ctx.instance_id = instance_id;
    return ctx;
}

StoredEvent TestStoredEvent() {
    StoredEvent event;
    event.block_hashes = {100, 200};
    event.block_size = 4;
    event.model_name = "test-model";
    event.lora_name = "none";
    event.instance_id = "instance-1";
    event.parent_block_hash = 0;
    event.token_ids = Seq(1, 8);
    event.medium = "cpu";
    return event;
}

ModelContext ContextForEvent(const StoredEvent& event) {
    ModelContext context;
    context.model_name = event.model_name;
    context.lora_name = event.lora_name;
    context.block_size = event.block_size;
    context.tenant_id = "default";
    context.additional_salt = "";
    context.instance_id = event.instance_id;
    return context;
}

struct InvalidStoreCase {
    const char* name;
    StoredEvent event;
    std::optional<size_t> expected_token_count;
    const char* error_fragment;
};

StoredEvent StoreEventWithLayout(size_t hash_count, size_t token_count,
                                 int64_t block_size = 4) {
    StoredEvent event = TestStoredEvent();
    event.block_hashes.resize(hash_count);
    for (size_t i = 0; i < hash_count; ++i) {
        event.block_hashes[i] = 100 + i;
    }
    event.token_ids.resize(token_count);
    for (size_t i = 0; i < token_count; ++i) {
        event.token_ids[i] = static_cast<int32_t>(i + 1);
    }
    event.block_size = block_size;
    return event;
}

StoredEvent MultiplicationOverflowEvent() {
    constexpr size_t kHashCount = 3;
    const size_t block_size =
        std::numeric_limits<size_t>::max() / kHashCount + 1;
    EXPECT_LE(block_size,
              static_cast<size_t>(std::numeric_limits<int64_t>::max()));

    StoredEvent event = StoreEventWithLayout(kHashCount, 1);
    event.block_size = static_cast<int64_t>(block_size);
    return event;
}

std::vector<InvalidStoreCase> InvalidStoreCases() {
    return {
        {"single block short", StoreEventWithLayout(1, 3), 4,
         "token count mismatch"},
        {"single block long", StoreEventWithLayout(1, 5), 4,
         "token count mismatch"},
        {"multiple blocks short", StoreEventWithLayout(2, 7), 8,
         "token count mismatch"},
        {"multiple blocks long", StoreEventWithLayout(2, 9), 8,
         "token count mismatch"},
        {"hashes only", StoreEventWithLayout(1, 0), 4,
         "must both be empty or both be non-empty"},
        {"tokens only", StoreEventWithLayout(0, 4), 0,
         "must both be empty or both be non-empty"},
        {"zero block size", StoreEventWithLayout(1, 1, 0), std::nullopt,
         "greater than zero"},
        {"negative block size", StoreEventWithLayout(1, 1, -4), std::nullopt,
         "greater than zero"},
        {"token count multiplication overflow", MultiplicationOverflowEvent(),
         std::nullopt, "overflow"},
    };
}

void ExpectErrorContains(const std::string& error, const std::string& value) {
    EXPECT_NE(error.find(value), std::string::npos) << "error=" << error;
}

void ExpectRejectedWithoutCreatingContext(const InvalidStoreCase& test_case) {
    PrefixCacheTable table;
    const std::string error = table.ProcessStoreEvent(test_case.event, 0);

    ASSERT_FALSE(error.empty()) << "case=" << test_case.name;
    ExpectErrorContains(error, test_case.error_fragment);
    ExpectErrorContains(
        error,
        "hash_count=" + std::to_string(test_case.event.block_hashes.size()));
    ExpectErrorContains(
        error, "block_size=" + std::to_string(test_case.event.block_size));
    ExpectErrorContains(
        error, "actual=" + std::to_string(test_case.event.token_ids.size()));
    if (test_case.expected_token_count.has_value()) {
        ExpectErrorContains(
            error,
            "expected=" + std::to_string(*test_case.expected_token_count));
    } else {
        EXPECT_EQ(error.find("expected="), std::string::npos)
            << "error=" << error;
    }

    EXPECT_FALSE(PrefixCacheTableTestPeer::ContextExists(
        table, ContextForEvent(test_case.event)));
    const PrefixCacheTableSnapshot snapshot =
        PrefixCacheTableTestPeer::Snapshot(table);
    EXPECT_EQ(snapshot.global_view.context_count, 0);
    EXPECT_TRUE(snapshot.global_view.model_contexts.empty());
    EXPECT_TRUE(snapshot.global_view.proxy_hash_map.empty());
    EXPECT_TRUE(snapshot.contexts.empty());
}

void PopulateCompleteTableState(PrefixCacheTable& table) {
    StoredEvent event = TestStoredEvent();
    ASSERT_EQ(table.ProcessStoreEvent(event, 0), "");

    event.medium = "GPU";
    ASSERT_EQ(table.ProcessStoreEvent(event, 2), "");
    table.AddDpSize(TestContext(), 0);
    table.AddDpSize(TestContext(), 2);

    StoredEvent other = TestStoredEvent();
    other.model_name = "other-model";
    other.instance_id = "instance-2";
    other.block_hashes = {300};
    other.token_ids = Seq(9, 12);
    ASSERT_EQ(table.ProcessStoreEvent(other, 7), "");
    table.AddDpSize(ContextForEvent(other), 7);

    PrefixCacheTableTestPeer::SetLastAccess(table, TestContext(), 123456789);
    PrefixCacheTableTestPeer::SetLastAccess(table, ContextForEvent(other),
                                            987654321);
}

// --- ComputePrefixHash ---------------------------------------------------

TEST(ComputePrefixHash, BlockCounts) {
    PrefixCacheTable table;
    struct Case {
        const char* name;
        int64_t block_size;
        std::vector<int32_t> tokens;
        uint64_t cache_salt;
        size_t want_len;
    };
    const Case cases[] = {
        {"single block", 4, Seq(1, 4), 0, 1},
        {"multiple blocks", 4, Seq(1, 8), 0, 2},
        {"partial block not counted", 4, Seq(1, 5), 0, 1},
        {"empty token ids", 4, {}, 0, 0},
        {"with non-zero cache salt", 4, Seq(1, 4), 12345, 1},
        {"zero block size returns empty", 0, Seq(1, 4), 0, 0},
    };
    for (const auto& c : cases) {
        ModelContext ctx = TestContext("", c.block_size);
        ctx.additional_salt = "test-salt";
        const auto got = table.ComputePrefixHash(ctx, c.tokens, c.cache_salt);
        EXPECT_EQ(got.size(), c.want_len) << "case=" << c.name;
        for (size_t i = 0; i < got.size(); ++i) {
            EXPECT_NE(got[i], 0u) << "case=" << c.name << " index=" << i;
        }
    }
}

// --- ProcessStoreEvent ---------------------------------------------------

TEST(ProcessStoreEvent, CreatesContextData) {
    PrefixCacheTable table;
    EXPECT_EQ(table.ProcessStoreEvent(TestStoredEvent(), 0), "");
    EXPECT_TRUE(PrefixCacheTableTestPeer::ContextExists(table, TestContext()));
}

TEST(ProcessStoreEvent, ConcurrentFirstAccessUsesCanonicalContextData) {
    PrefixCacheTable table;
    ModelContext ctx = TestContext();
    // Make candidate initialisation long enough for all barrier-released
    // workers to observe the initially empty map before insertion.
    ctx.additional_salt.assign(1 << 20, 's');

    constexpr int kThreads = 16;
    std::barrier start(kThreads);
    std::vector<std::shared_ptr<conductor::prefixindex::ContextData>> returned(
        kThreads);
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        workers.emplace_back([&table, &ctx, &start, &returned, i] {
            start.arrive_and_wait();
            auto data = PrefixCacheTableTestPeer::GetContextData(table, ctx);
            {
                std::unique_lock lock(data->hashmap_mu);
                data->dp_size.insert(i);
            }
            returned[i] = std::move(data);
        });
    }
    for (auto& worker : workers) worker.join();

    const auto canonical = PrefixCacheTableTestPeer::GetContextData(table, ctx);
    for (const auto& data : returned) {
        EXPECT_EQ(data, canonical);
    }

    const auto dp_size = PrefixCacheTableTestPeer::DpSize(table, ctx);
    ASSERT_EQ(dp_size.size(), static_cast<size_t>(kThreads));
    for (int i = 0; i < kThreads; ++i) {
        EXPECT_TRUE(dp_size.contains(i));
    }
    EXPECT_EQ(table.GetGlobalView().context_count, 1);
}

TEST(ProcessStoreEvent, FullyEmptyEventIsNoopBeforeBlockSizeValidation) {
    PrefixCacheTable table;
    PopulateCompleteTableState(table);
    const auto before = PrefixCacheTableTestPeer::Snapshot(table);

    StoredEvent event = TestStoredEvent();
    event.instance_id = "never-created";
    event.block_hashes.clear();
    event.token_ids.clear();
    event.block_size = std::numeric_limits<int64_t>::min();

    EXPECT_EQ(table.ProcessStoreEvent(event, 0), "");
    EXPECT_FALSE(
        PrefixCacheTableTestPeer::ContextExists(table, ContextForEvent(event)));
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(table) == before);
}

TEST(ProcessStoreEvent, SingleBlockShortTokensRejected) {
    ExpectRejectedWithoutCreatingContext(InvalidStoreCases()[0]);
}

TEST(ProcessStoreEvent, SingleBlockLongTokensRejected) {
    ExpectRejectedWithoutCreatingContext(InvalidStoreCases()[1]);
}

TEST(ProcessStoreEvent, MultipleBlocksShortTokensRejected) {
    ExpectRejectedWithoutCreatingContext(InvalidStoreCases()[2]);
}

TEST(ProcessStoreEvent, MultipleBlocksLongTokensRejected) {
    ExpectRejectedWithoutCreatingContext(InvalidStoreCases()[3]);
}

TEST(ProcessStoreEvent, HashesWithoutTokensRejected) {
    ExpectRejectedWithoutCreatingContext(InvalidStoreCases()[4]);
}

TEST(ProcessStoreEvent, TokensWithoutHashesRejected) {
    ExpectRejectedWithoutCreatingContext(InvalidStoreCases()[5]);
}

TEST(ProcessStoreEvent, ZeroBlockSizeRejected) {
    ExpectRejectedWithoutCreatingContext(InvalidStoreCases()[6]);
}

TEST(ProcessStoreEvent, NegativeBlockSizeRejected) {
    ExpectRejectedWithoutCreatingContext(InvalidStoreCases()[7]);
}

TEST(ProcessStoreEvent, TokenCountMultiplicationOverflowRejected) {
    ExpectRejectedWithoutCreatingContext(InvalidStoreCases()[8]);
}

TEST(ProcessStoreEvent, InvalidLayoutsPreserveCompleteExistingState) {
    PrefixCacheTable table;
    PopulateCompleteTableState(table);
    const PrefixCacheTableSnapshot before =
        PrefixCacheTableTestPeer::Snapshot(table);

    ASSERT_EQ(before.global_view.context_count, 2);
    ASSERT_EQ(before.global_view.model_contexts.size(), 2u);
    ASSERT_EQ(before.global_view.proxy_hash_map.size(), 2u);
    ASSERT_EQ(before.contexts.size(), 2u);

    const auto target = before.contexts.find(TestContext());
    ASSERT_NE(target, before.contexts.end());
    EXPECT_EQ(target->second.proxy_hash_mapping.size(), 2u);
    EXPECT_EQ(target->second.prefix_entries.size(), 2u);
    EXPECT_EQ(target->second.total_prefixes, 2);
    EXPECT_EQ(target->second.dp_size, (std::set<int64_t>{0, 2}));
    EXPECT_EQ(target->second.last_access, 123456789);
    for (const auto& [hash, entry] : target->second.prefix_entries) {
        SCOPED_TRACE(hash);
        EXPECT_EQ(entry.total_replica_nums, 2);
        EXPECT_EQ(entry.medium_set, (std::set<std::string>{"GPU", "cpu"}));
        EXPECT_EQ(entry.dp_rank_set, (std::set<int64_t>{0, 2}));
        EXPECT_EQ(entry.engine_last_access_time.size(), 1u);
        EXPECT_TRUE(entry.engine_last_access_time.contains("instance-1"));
    }

    ModelContext other_context = TestContext("instance-2");
    other_context.model_name = "other-model";
    const auto other = before.contexts.find(other_context);
    ASSERT_NE(other, before.contexts.end());
    EXPECT_EQ(other->second.last_access, 987654321);

    for (const auto& test_case : InvalidStoreCases()) {
        SCOPED_TRACE(test_case.name);
        const std::string error = table.ProcessStoreEvent(test_case.event, 0);
        ASSERT_FALSE(error.empty());

        const PrefixCacheTableSnapshot after =
            PrefixCacheTableTestPeer::Snapshot(table);
        EXPECT_TRUE(after == before);
        EXPECT_EQ(PrefixCacheTableTestPeer::GetLastAccess(table, TestContext()),
                  123456789);
    }
}

// --- ProcessRemoveEvent --------------------------------------------------

TEST(ProcessRemoveEvent, ClearsProxyHashMapping) {
    PrefixCacheTable table;
    ASSERT_EQ(table.ProcessStoreEvent(TestStoredEvent(), 0), "");

    RemovedEvent remove;
    remove.block_hashes = {100, 200};
    remove.model_name = "test-model";
    remove.lora_name = "none";
    remove.instance_id = "instance-1";
    remove.block_size = 4;
    remove.medium = "cpu";
    EXPECT_EQ(table.ProcessRemoveEvent(remove, 0, "instance-1"), "");

    EXPECT_TRUE(PrefixCacheTableTestPeer::ContextExists(table, TestContext()));
    EXPECT_EQ(
        PrefixCacheTableTestPeer::ProxyHashMappingSize(table, TestContext()),
        0u);
}

TEST(ProcessRemoveEvent, EmptyBlockHashesIsNoop) {
    PrefixCacheTable table;
    RemovedEvent remove;
    remove.block_hashes = {};
    remove.model_name = "test-model";
    remove.lora_name = "none";
    remove.instance_id = "instance-1";
    remove.block_size = 4;
    remove.medium = "cpu";
    EXPECT_EQ(table.ProcessRemoveEvent(remove, 0, "instance-1"), "");
}

// Spec scenario: after all replicas removed, block no longer hits.
TEST(ProcessRemoveEvent, AllReplicasRemovedNoLongerHits) {
    PrefixCacheTable table;
    ASSERT_EQ(table.ProcessStoreEvent(TestStoredEvent(), 0), "");
    ASSERT_GT(
        table.CacheHitCompute(TestContext(), Seq(1, 8)).longest_match_tokens,
        0);

    RemovedEvent remove;
    remove.block_hashes = {100, 200};
    remove.model_name = "test-model";
    remove.lora_name = "none";
    remove.instance_id = "instance-1";
    remove.block_size = 4;
    ASSERT_EQ(table.ProcessRemoveEvent(remove, 0, "instance-1"), "");

    const auto result = table.CacheHitCompute(TestContext(), Seq(1, 8));
    EXPECT_EQ(result.longest_match_tokens, 0);
    EXPECT_EQ(result.cpu, 0);
}

// Spec scenario (bug-for-bug): mediumSet keeps GPU after the GPU replica
// is removed, as long as the replica count stays positive.
TEST(ProcessRemoveEvent, MediumSetRetainedBugForBug) {
    PrefixCacheTable table;

    // Two replicas of the same engine block hash: GPU then cpu.
    StoredEvent gpu_event = TestStoredEvent();
    gpu_event.block_hashes = {100};
    gpu_event.token_ids = Seq(1, 4);
    gpu_event.medium = "GPU";
    ASSERT_EQ(table.ProcessStoreEvent(gpu_event, 0), "");

    StoredEvent cpu_event = gpu_event;
    cpu_event.medium = "cpu";
    ASSERT_EQ(table.ProcessStoreEvent(cpu_event, 1), "");

    // Remove one replica; count stays > 0.
    // NOTE: removing by the engine hash also deletes the proxy mapping
    // (bug-for-bug), but the prefix entry survives with count 1.
    RemovedEvent remove;
    remove.block_hashes = {100};
    remove.model_name = "test-model";
    remove.lora_name = "none";
    remove.instance_id = "instance-1";
    remove.block_size = 4;
    remove.medium = "GPU";
    ASSERT_EQ(table.ProcessRemoveEvent(remove, 0, "instance-1"), "");

    // Bug-for-bug: GPU still reported (dirty read) because mediumSet is
    // never pruned; CPU also still counted.
    const auto result = table.CacheHitCompute(TestContext(), Seq(1, 4));
    EXPECT_EQ(result.longest_match_tokens, 4);
    EXPECT_EQ(result.gpu, 4);
    EXPECT_EQ(result.cpu, 4);
}

// Bug-for-bug: replica count may go negative; entry is erased at <= 0 and
// a later remove of the same conductor hash is a silent no-op.
TEST(ProcessRemoveEvent, ReplicaCountCanGoNegativePathIsSafe) {
    PrefixCacheTable table;
    StoredEvent event = TestStoredEvent();
    event.block_hashes = {100};
    event.token_ids = Seq(1, 4);
    ASSERT_EQ(table.ProcessStoreEvent(event, 0), "");

    RemovedEvent remove;
    remove.block_hashes = {100};
    remove.model_name = "test-model";
    remove.lora_name = "none";
    remove.instance_id = "instance-1";
    remove.block_size = 4;
    ASSERT_EQ(table.ProcessRemoveEvent(remove, 0, "instance-1"), "");
    // Second remove: mapping already gone, must be a safe no-op.
    ASSERT_EQ(table.ProcessRemoveEvent(remove, 0, "instance-1"), "");
    EXPECT_EQ(
        table.CacheHitCompute(TestContext(), Seq(1, 4)).longest_match_tokens,
        0);
}

// --- CacheHitCompute -----------------------------------------------------

TEST(CacheHitCompute, HitAfterStore) {
    PrefixCacheTable table;
    StoredEvent event = TestStoredEvent();
    event.block_hashes = {100};
    event.token_ids = Seq(1, 4);
    ASSERT_EQ(table.ProcessStoreEvent(event, 0), "");

    const auto result = table.CacheHitCompute(TestContext(), Seq(1, 4));
    EXPECT_GT(result.longest_match_tokens, 0);
    EXPECT_GT(result.cpu, 0);
}

TEST(CacheHitCompute, NonExistentContextReturnsZeros) {
    PrefixCacheTable table;
    ModelContext ctx = TestContext();
    ctx.model_name = "non-existent-model";
    ctx.additional_salt = "test-salt";
    ctx.instance_id = "";

    const auto result = table.CacheHitCompute(ctx, Seq(1, 4));
    EXPECT_EQ(result.longest_match_tokens, 0);
    EXPECT_EQ(result.cpu, 0);
    EXPECT_EQ(result.gpu, 0);
}

TEST(CacheHitCompute, ZeroBlockSizeReturnsZerosWithoutCrash) {
    PrefixCacheTable table;
    // Materialize the context with a store first so the zero-BlockSize
    // path reaches ComputePrefixHash.
    ModelContext ctx = TestContext("instance-1", 0);
    ctx.additional_salt = "test-salt";
    const auto result = table.CacheHitCompute(ctx, Seq(1, 4));
    EXPECT_EQ(result.longest_match_tokens, 0);
}

// Spec scenario: different InstanceID contexts are fully isolated.
TEST(CacheHitCompute, DifferentInstanceIdIsolated) {
    PrefixCacheTable table;
    ASSERT_EQ(table.ProcessStoreEvent(TestStoredEvent(), 0), "");

    const auto result =
        table.CacheHitCompute(TestContext("instance-B"), Seq(1, 8));
    EXPECT_EQ(result.longest_match_tokens, 0);
}

// Spec scenario: partial prefix hit — only stored leading blocks count.
TEST(CacheHitCompute, PartialPrefixHit) {
    PrefixCacheTable table;
    // Store only the first 2 blocks (tokens 1..8).
    ASSERT_EQ(table.ProcessStoreEvent(TestStoredEvent(), 0), "");

    // Query 4 blocks (tokens 1..16): only the stored prefix matches.
    const auto result = table.CacheHitCompute(TestContext(), Seq(1, 16));
    EXPECT_EQ(result.longest_match_tokens, 2 * 4);
    EXPECT_EQ(result.cpu, 2 * 4);
    EXPECT_EQ(result.gpu, 0);
}

// Spec scenario: matching stops at the first gap even if later blocks
// exist (prefix semantics).
TEST(CacheHitCompute, StopsAtFirstMissingBlock) {
    PrefixCacheTable table;
    ASSERT_EQ(table.ProcessStoreEvent(TestStoredEvent(), 0), "");

    // Query where the first block differs: nothing may match.
    auto tokens = Seq(1, 8);
    tokens[0] = 999;
    const auto result = table.CacheHitCompute(TestContext(), tokens);
    EXPECT_EQ(result.longest_match_tokens, 0);
}

// Spec scenario (bug-for-bug): unknown medium (empty string, "disk", ...)
// logs a warning and does not count as a hit; DISK stays 0.
TEST(CacheHitCompute, UnknownMediumNotCountedBugForBug) {
    PrefixCacheTable table;
    for (const std::string medium : {"", "disk", "DISK", "Cpu", "gpu"}) {
        StoredEvent event = TestStoredEvent();
        event.instance_id = "inst-" + medium;
        event.block_hashes = {100};
        event.token_ids = Seq(1, 4);
        event.medium = medium;
        ASSERT_EQ(table.ProcessStoreEvent(event, 0), "");

        const auto result =
            table.CacheHitCompute(TestContext("inst-" + medium), Seq(1, 4));
        EXPECT_EQ(result.longest_match_tokens, 0) << "medium=" << medium;
        EXPECT_EQ(result.gpu, 0) << "medium=" << medium;
        EXPECT_EQ(result.cpu, 0) << "medium=" << medium;
        EXPECT_EQ(result.disk, 0) << "medium=" << medium;
    }
}

// DP counting: each hit block contributes block_size per dp_rank present.
TEST(CacheHitCompute, DpRankCounting) {
    PrefixCacheTable table;
    StoredEvent event = TestStoredEvent();
    event.block_hashes = {100};
    event.token_ids = Seq(1, 4);
    event.medium = "GPU";
    ASSERT_EQ(table.ProcessStoreEvent(event, 0), "");
    // Same engine hash stored again from dp_rank 2.
    ASSERT_EQ(table.ProcessStoreEvent(event, 2), "");

    const auto result = table.CacheHitCompute(TestContext(), Seq(1, 4));
    EXPECT_EQ(result.longest_match_tokens, 4);
    ASSERT_EQ(result.dp.size(), 2u);
    EXPECT_EQ(result.dp.at(0), 4);
    EXPECT_EQ(result.dp.at(2), 4);
}

// --- GetGlobalView (spec: 全局视图导出) ----------------------------------

TEST(GetGlobalView, ContainsAllContexts) {
    PrefixCacheTable table;
    ASSERT_EQ(table.ProcessStoreEvent(TestStoredEvent(), 0), "");
    StoredEvent other = TestStoredEvent();
    other.instance_id = "instance-2";
    ASSERT_EQ(table.ProcessStoreEvent(other, 0), "");

    const auto view = table.GetGlobalView();
    EXPECT_EQ(view.context_count, 2);
    ASSERT_EQ(view.model_contexts.size(), 2u);
    ASSERT_EQ(view.proxy_hash_map.size(), 2u);
    for (const auto& mapping : view.proxy_hash_map) {
        EXPECT_EQ(mapping.size(), 2u);  // engine hashes 100 and 200
    }
}

TEST(GetGlobalView, EmptyTable) {
    PrefixCacheTable table;
    const auto view = table.GetGlobalView();
    EXPECT_EQ(view.context_count, 0);
    EXPECT_TRUE(view.model_contexts.empty());
    EXPECT_TRUE(view.proxy_hash_map.empty());
}

// --- AddDpSize ------------------------------------------------------------

TEST(AddDpSize, CreatesContextAndRecordsRank) {
    PrefixCacheTable table;
    ModelContext ctx = TestContext();
    table.AddDpSize(ctx, 0);
    table.AddDpSize(ctx, 3);
    table.AddDpSize(ctx, 0);  // idempotent
    EXPECT_TRUE(PrefixCacheTableTestPeer::ContextExists(table, ctx));
    EXPECT_EQ(table.GetGlobalView().context_count, 1);
    EXPECT_EQ(table.GetGlobalView().model_contexts.size(), 1u);
}

}  // namespace
