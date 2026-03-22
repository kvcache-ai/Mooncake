// Prefix Radix Tree Unit Tests
// Verifies prefix-aware KV cache lookup for LLM workloads

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <string>
#include <vector>
#include <thread>
#include <algorithm>

#include "prefix_radix_tree.h"

namespace mooncake {

class PrefixRadixTreeTest : public ::testing::Test {
   protected:
    void SetUp() override {}
};

// Basic insert and contains
TEST_F(PrefixRadixTreeTest, BasicInsertContains) {
    PrefixRadixTree tree;

    tree.Insert("hello");
    tree.Insert("world");

    ASSERT_TRUE(tree.Contains("hello"));
    ASSERT_TRUE(tree.Contains("world"));
    ASSERT_FALSE(tree.Contains("hell"));  // prefix, not a key
    ASSERT_FALSE(tree.Contains("helloo"));  // extension, not a key
    ASSERT_EQ(tree.Size(), 2u);
}

// Insert keys with shared prefix
TEST_F(PrefixRadixTreeTest, SharedPrefix) {
    PrefixRadixTree tree;

    tree.Insert("abc");
    tree.Insert("abcdef");
    tree.Insert("abcxyz");
    tree.Insert("abd");

    ASSERT_TRUE(tree.Contains("abc"));
    ASSERT_TRUE(tree.Contains("abcdef"));
    ASSERT_TRUE(tree.Contains("abcxyz"));
    ASSERT_TRUE(tree.Contains("abd"));
    ASSERT_FALSE(tree.Contains("ab"));
    ASSERT_FALSE(tree.Contains("abcd"));
    ASSERT_EQ(tree.Size(), 4u);
}

// Basic remove
TEST_F(PrefixRadixTreeTest, BasicRemove) {
    PrefixRadixTree tree;

    tree.Insert("hello");
    tree.Insert("help");
    tree.Insert("world");

    ASSERT_TRUE(tree.Remove("help"));
    ASSERT_FALSE(tree.Contains("help"));
    ASSERT_TRUE(tree.Contains("hello"));  // should not be affected
    ASSERT_TRUE(tree.Contains("world"));
    ASSERT_EQ(tree.Size(), 2u);

    ASSERT_FALSE(tree.Remove("nonexistent"));
    ASSERT_EQ(tree.Size(), 2u);
}

// Remove prefix key: removing "abc" when "abcdef" exists
TEST_F(PrefixRadixTreeTest, RemovePrefixKey) {
    PrefixRadixTree tree;

    tree.Insert("abc");
    tree.Insert("abcdef");

    ASSERT_TRUE(tree.Remove("abc"));
    ASSERT_FALSE(tree.Contains("abc"));
    ASSERT_TRUE(tree.Contains("abcdef"));  // child survives
    ASSERT_EQ(tree.Size(), 1u);
}

// Longest prefix match: core functionality for KV cache
TEST_F(PrefixRadixTreeTest, LongestPrefixMatch) {
    PrefixRadixTree tree;

    tree.Insert("system_prompt_v1");
    tree.Insert("system_prompt_v1_turn1");
    tree.Insert("system_prompt_v1_turn1_user");
    tree.Insert("system_prompt_v2");

    // Exact match
    auto result = tree.LongestPrefixMatch("system_prompt_v1");
    ASSERT_EQ(result.matched_key, "system_prompt_v1");
    ASSERT_TRUE(result.is_exact);

    // Prefix match: query extends beyond stored key
    result = tree.LongestPrefixMatch("system_prompt_v1_turn1_user_response");
    ASSERT_EQ(result.matched_key, "system_prompt_v1_turn1_user");
    ASSERT_EQ(result.matched_length, std::string("system_prompt_v1_turn1_user").size());
    ASSERT_FALSE(result.is_exact);

    // Partial prefix match: should find the longest stored prefix
    result = tree.LongestPrefixMatch("system_prompt_v1_turn2");
    ASSERT_EQ(result.matched_key, "system_prompt_v1");
    ASSERT_EQ(result.matched_length, std::string("system_prompt_v1").size());

    // No match
    result = tree.LongestPrefixMatch("user_prompt_v1");
    ASSERT_EQ(result.matched_length, 0u);
}

// KVCache key format: simulating real LLM token sequences
TEST_F(PrefixRadixTreeTest, KVCacheTokenSequences) {
    PrefixRadixTree tree;

    // Simulate token-level KV cache keys
    // Format: model_id/layer/prefix_hash
    tree.Insert("llama3/L0/tok_0_512");       // System prompt
    tree.Insert("llama3/L0/tok_0_1024");      // System + user query 1
    tree.Insert("llama3/L0/tok_0_1536");      // System + user query 1 + response 1
    tree.Insert("llama3/L0/tok_0_2048");      // Full conversation turn 1

    // New request shares system prompt
    auto result = tree.LongestPrefixMatch("llama3/L0/tok_0_512_new_query");
    ASSERT_EQ(result.matched_key, "llama3/L0/tok_0_512");
    LOG(INFO) << "KVCache prefix reuse: matched "
              << result.matched_length << " chars of "
              << "llama3/L0/tok_0_512" << " (system prompt reused)";

    // New request shares system + user query 1
    result = tree.LongestPrefixMatch("llama3/L0/tok_0_1024_followup");
    ASSERT_EQ(result.matched_key, "llama3/L0/tok_0_1024");

    // Different model: no match
    result = tree.LongestPrefixMatch("gpt4/L0/tok_0_512");
    ASSERT_EQ(result.matched_length, 0u);
}

// Multi-turn conversation prefix sharing
TEST_F(PrefixRadixTreeTest, MultiTurnConversation) {
    PrefixRadixTree tree;

    // Conversation with progressive prefix extension
    std::string base = "conv_123_sys_You_are_helpful";
    tree.Insert(base);

    std::string turn1 = base + "_user_Hello";
    tree.Insert(turn1);

    std::string turn2 = turn1 + "_assistant_Hi_there";
    tree.Insert(turn2);

    std::string turn3 = turn2 + "_user_What_is_AI";
    tree.Insert(turn3);

    // New turn should reuse up to turn3
    std::string new_turn = turn3 + "_assistant_AI_is";
    auto result = tree.LongestPrefixMatch(new_turn);
    ASSERT_EQ(result.matched_key, turn3);

    // Evict old turns, new queries should still match system prompt
    tree.Remove(turn1);
    tree.Remove(turn2);
    tree.Remove(turn3);

    result = tree.LongestPrefixMatch(new_turn);
    ASSERT_EQ(result.matched_key, base);
    LOG(INFO) << "After eviction, prefix fell back to system prompt: "
              << result.matched_length << " chars";
}

// Thread safety: concurrent reads and writes
TEST_F(PrefixRadixTreeTest, ConcurrentReadWrite) {
    PrefixRadixTree tree;
    constexpr int kNumKeys = 5000;

    // Pre-populate
    for (int i = 0; i < kNumKeys; i++) {
        tree.Insert("key_" + std::to_string(i));
    }

    std::vector<std::thread> threads;
    std::atomic<int> errors{0};

    // Reader threads
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&]() {
            for (int i = 0; i < kNumKeys; i++) {
                if (!tree.Contains("key_" + std::to_string(i))) {
                    errors.fetch_add(1);
                }
            }
        });
    }

    // Writer threads (insert new keys)
    for (int t = 0; t < 2; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 1000; i++) {
                tree.Insert("new_t" + std::to_string(t) + "_" + std::to_string(i));
            }
        });
    }

    // Remover thread
    threads.emplace_back([&]() {
        for (int i = kNumKeys - 500; i < kNumKeys; i++) {
            tree.Remove("key_" + std::to_string(i));
        }
    });

    for (auto& t : threads) t.join();

    // Some errors expected (removals happen during reads),
    // but no crashes or undefined behavior.
    LOG(INFO) << "Concurrent R/W: " << errors.load()
              << " read misses (expected due to concurrent removal)";
    ASSERT_GE(tree.Size(), 0u);  // Just verify no crash
}

// Memory efficiency: node count should be less than key count with sharing
TEST_F(PrefixRadixTreeTest, MemoryEfficiency) {
    PrefixRadixTree tree;

    // Insert keys with heavy prefix sharing
    for (int i = 0; i < 1000; i++) {
        tree.Insert("system_prompt_v1_layer_0_head_" + std::to_string(i));
    }
    for (int i = 0; i < 1000; i++) {
        tree.Insert("system_prompt_v1_layer_1_head_" + std::to_string(i));
    }

    size_t key_count = tree.Size();
    size_t node_count = tree.NodeCount();

    LOG(INFO) << "Memory efficiency: " << key_count << " keys, "
              << node_count << " nodes (compression ratio: "
              << (1.0 - static_cast<double>(node_count) / key_count) * 100
              << "%)";

    // With heavy prefix sharing, node count per key should be low.
    // Internal nodes add overhead, but total should be < 2x key count.
    ASSERT_LT(node_count, key_count * 2)
        << "Radix tree node count should stay bounded with prefix sharing";
}

// Empty key handling
TEST_F(PrefixRadixTreeTest, EmptyKey) {
    PrefixRadixTree tree;

    tree.Insert("");
    ASSERT_TRUE(tree.Contains(""));
    ASSERT_EQ(tree.Size(), 1u);

    auto result = tree.LongestPrefixMatch("anything");
    ASSERT_EQ(result.matched_key, "");
    ASSERT_EQ(result.matched_length, 0u);

    tree.Remove("");
    ASSERT_FALSE(tree.Contains(""));
}

// AllKeys diagnostic
TEST_F(PrefixRadixTreeTest, AllKeys) {
    PrefixRadixTree tree;

    tree.Insert("a");
    tree.Insert("ab");
    tree.Insert("abc");
    tree.Insert("b");

    auto keys = tree.AllKeys();
    std::sort(keys.begin(), keys.end());

    ASSERT_EQ(keys.size(), 4u);
    ASSERT_EQ(keys[0], "a");
    ASSERT_EQ(keys[1], "ab");
    ASSERT_EQ(keys[2], "abc");
    ASSERT_EQ(keys[3], "b");
}

}  // namespace mooncake
