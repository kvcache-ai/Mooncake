// Tests for the prefix index: block-hash computation, store/remove/query
// flows, cache-hit accounting, and the /global_view aggregation.

#include <gtest/gtest.h>

#include <barrier>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "conductor/common/types.h"
#include "conductor/prefixindex/prefix_indexer.h"

namespace conductor {
namespace prefixindex {

// Test-only access to the table's internal context map / hash mapping.
class PrefixCacheTableTestPeer {
   public:
    static bool ContextExists(PrefixCacheTable& table,
                              const ModelContext& ctx) {
        return table.LoadContextData(ctx) != nullptr;
    }

    static size_t ProxyHashMappingSize(PrefixCacheTable& table,
                                       const ModelContext& ctx) {
        auto data = table.LoadContextData(ctx);
        if (!data) return 0;
        std::shared_lock lock(data->hashmap_mu);
        return data->proxy_hash_mapping.size();
    }

    static std::shared_ptr<ContextData> GetContextData(
        PrefixCacheTable& table, const ModelContext& ctx) {
        return table.GetContextData(ctx);
    }

    static std::set<int64_t> DpSize(PrefixCacheTable& table,
                                    const ModelContext& ctx) {
        auto data = table.LoadContextData(ctx);
        if (!data) return {};
        std::shared_lock lock(data->hashmap_mu);
        return data->dp_size;
    }
};

}  // namespace prefixindex
}  // namespace conductor

namespace {

using conductor::common::RemovedEvent;
using conductor::common::StoredEvent;
using conductor::prefixindex::ModelContext;
using conductor::prefixindex::PrefixCacheTable;
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

TEST(ProcessStoreEvent, EmptyBlockHashesIsNoop) {
    PrefixCacheTable table;
    StoredEvent event = TestStoredEvent();
    event.block_hashes = {};
    event.token_ids = Seq(1, 4);
    EXPECT_EQ(table.ProcessStoreEvent(event, 0), "");
}

// Spec scenario: length mismatch (and more than one block hash) rejected.
TEST(ProcessStoreEvent, LengthMismatchRejected) {
    PrefixCacheTable table;
    StoredEvent event = TestStoredEvent();
    event.token_ids = Seq(1, 7);  // 2 blocks * 4 != 7
    EXPECT_NE(table.ProcessStoreEvent(event, 0), "");
    // Index unchanged: nothing stored for these tokens.
    const auto result = table.CacheHitCompute(TestContext(), Seq(1, 8));
    EXPECT_EQ(result.longest_match_tokens, 0);
}

// Quirk retained: a single block hash bypasses the length check.
TEST(ProcessStoreEvent, SingleBlockHashBypassesLengthCheck) {
    PrefixCacheTable table;
    StoredEvent event = TestStoredEvent();
    event.block_hashes = {100};
    event.token_ids = Seq(1, 8);  // 1 block * 4 != 8, but len==1 -> allowed
    EXPECT_EQ(table.ProcessStoreEvent(event, 0), "");
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
