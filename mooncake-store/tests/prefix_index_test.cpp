#include "prefix_index.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <set>
#include <thread>
#include <vector>

namespace mooncake::test {

static const UUID kClient1 = {1, 1};
static const UUID kClient2 = {2, 2};

// ---------------------------------------------------------------------------
// Insert: basic
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, InsertSingleKey) {
    PrefixIndex idx;
    idx.insert("llama-70b/layer.0/q_proj.weight", 1024, kClient1);

    EXPECT_EQ(idx.size(), 1);
    EXPECT_EQ(idx.count_by_prefix("llama-70b/"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("llama-70b/"), 1024);
}

TEST(PrefixIndexTest, InsertMultipleKeysSharedPrefix) {
    PrefixIndex idx;
    idx.insert("llama-70b/layer.0/q_proj.weight", 1024, kClient1);
    idx.insert("llama-70b/layer.0/k_proj.weight", 2048, kClient1);
    idx.insert("llama-70b/layer.1/q_proj.weight", 1024, kClient1);

    EXPECT_EQ(idx.size(), 3);
    EXPECT_EQ(idx.count_by_prefix("llama-70b/"), 3);
    EXPECT_EQ(idx.count_by_prefix("llama-70b/layer.0/"), 2);
    EXPECT_EQ(idx.count_by_prefix("llama-70b/layer.1/"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("llama-70b/"), 1024 + 2048 + 1024);
    EXPECT_EQ(idx.bytes_by_prefix("llama-70b/layer.0/"), 1024 + 2048);
}

TEST(PrefixIndexTest, InsertDisjointPrefixes) {
    PrefixIndex idx;
    idx.insert("llama-70b/layer.0/q_proj.weight", 1000, kClient1);
    idx.insert("mistral-7b/layer.0/q_proj.weight", 500, kClient2);

    EXPECT_EQ(idx.size(), 2);
    EXPECT_EQ(idx.count_by_prefix("llama-70b/"), 1);
    EXPECT_EQ(idx.count_by_prefix("mistral-7b/"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("llama-70b/"), 1000);
    EXPECT_EQ(idx.bytes_by_prefix("mistral-7b/"), 500);
}

TEST(PrefixIndexTest, InsertDuplicateKeyUpdatesLeafData) {
    PrefixIndex idx;
    idx.insert("key1", 100, kClient1);
    idx.insert("key1", 200, kClient2);

    // Count stays 1 (no new key added).
    EXPECT_EQ(idx.size(), 1);
    EXPECT_EQ(idx.count_by_prefix(""), 1);
    // Bytes reflect the SECOND insert's size (upsert semantics).
    EXPECT_EQ(idx.bytes_by_prefix(""), 200);
}

TEST(PrefixIndexTest, InsertDuplicateKeySameSizeNoStatsDrift) {
    PrefixIndex idx;
    idx.insert("key1", 100, kClient1);
    idx.insert("key1", 100, kClient2);

    EXPECT_EQ(idx.size(), 1);
    EXPECT_EQ(idx.count_by_prefix(""), 1);
    // Same size — bytes unchanged, no drift.
    EXPECT_EQ(idx.bytes_by_prefix(""), 100);
}

TEST(PrefixIndexTest, InsertEmptyKeyIsNoop) {
    PrefixIndex idx;
    idx.insert("", 100, kClient1);
    EXPECT_EQ(idx.size(), 0);
}

// ---------------------------------------------------------------------------
// Split on insert (partial edge match)
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, SplitOnPartialEdgeMatch) {
    PrefixIndex idx;
    idx.insert("abcdef", 100, kClient1);
    idx.insert("abcxyz", 200, kClient1);

    EXPECT_EQ(idx.size(), 2);
    EXPECT_EQ(idx.count_by_prefix("abc"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("abc"), 300);
    EXPECT_EQ(idx.count_by_prefix("abcd"), 1);
    EXPECT_EQ(idx.count_by_prefix("abcx"), 1);
}

TEST(PrefixIndexTest, SplitPreservesExistingSubtreeStats) {
    PrefixIndex idx;
    idx.insert("abc/one", 100, kClient1);
    idx.insert("abc/two", 200, kClient1);

    // Now insert a key that forces a split on "abc/"
    idx.insert("abd/three", 300, kClient1);

    EXPECT_EQ(idx.size(), 3);
    EXPECT_EQ(idx.count_by_prefix("abc/"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("abc/"), 300);
    EXPECT_EQ(idx.count_by_prefix("abd/"), 1);
    EXPECT_EQ(idx.count_by_prefix("ab"), 3);
    EXPECT_EQ(idx.bytes_by_prefix("ab"), 600);
}

// ---------------------------------------------------------------------------
// Insert: shorter key after longer key (split where key ends at mid-node)
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, InsertShorterKeyAfterLongerKey) {
    PrefixIndex idx;
    idx.insert("abcdef", 200, kClient1);
    idx.insert("abc", 100, kClient1);

    EXPECT_EQ(idx.size(), 2);
    EXPECT_EQ(idx.count_by_prefix("abc"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("abc"), 300);

    auto keys = idx.list_keys_by_prefix("abc");
    ASSERT_EQ(keys.size(), 2);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "abc") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "abcdef") != keys.end());
}

// ---------------------------------------------------------------------------
// Terminal node that is also a prefix of longer keys
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, NodeIsBothTerminalAndInternal) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);
    idx.insert("abcdef", 200, kClient1);

    EXPECT_EQ(idx.size(), 2);
    EXPECT_EQ(idx.count_by_prefix("abc"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("abc"), 300);

    auto keys = idx.list_keys_by_prefix("abc");
    EXPECT_EQ(keys.size(), 2);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "abc") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "abcdef") != keys.end());
}

// ---------------------------------------------------------------------------
// Remove tests
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, RemoveSingleKey) {
    PrefixIndex idx;
    idx.insert("key1", 100, kClient1);
    idx.remove("key1");

    EXPECT_EQ(idx.size(), 0);
    EXPECT_EQ(idx.count_by_prefix(""), 0);
    EXPECT_EQ(idx.bytes_by_prefix(""), 0);
}

TEST(PrefixIndexTest, RemoveNonexistentKeyIsNoop) {
    PrefixIndex idx;
    idx.insert("key1", 100, kClient1);
    idx.remove("key2");

    EXPECT_EQ(idx.size(), 1);
}

TEST(PrefixIndexTest, RemoveEmptyKeyIsNoop) {
    PrefixIndex idx;
    idx.insert("key1", 100, kClient1);
    idx.remove("");

    EXPECT_EQ(idx.size(), 1);
}

TEST(PrefixIndexTest, RemoveTerminalPrefixKeepsLongerKey) {
    PrefixIndex idx;
    idx.insert("abcdef", 200, kClient1);
    idx.insert("abc", 100, kClient1);

    idx.remove("abc");

    EXPECT_EQ(idx.size(), 1);
    EXPECT_EQ(idx.count_by_prefix("abc"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("abc"), 200);
    auto keys = idx.list_keys_by_prefix("");
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys[0], "abcdef");
}

TEST(PrefixIndexTest, RemoveLongerKeyKeepsShorterTerminal) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);
    idx.insert("abcdef", 200, kClient1);

    idx.remove("abcdef");

    EXPECT_EQ(idx.size(), 1);
    EXPECT_EQ(idx.count_by_prefix("abc"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("abc"), 100);
    auto keys = idx.list_keys_by_prefix("");
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys[0], "abc");
}

TEST(PrefixIndexTest, RemoveUpdatesSubtreeStats) {
    PrefixIndex idx;
    idx.insert("llama-70b/layer.0/q_proj.weight", 1024, kClient1);
    idx.insert("llama-70b/layer.0/k_proj.weight", 2048, kClient1);
    idx.insert("llama-70b/layer.1/q_proj.weight", 512, kClient1);

    idx.remove("llama-70b/layer.0/q_proj.weight");

    EXPECT_EQ(idx.size(), 2);
    EXPECT_EQ(idx.count_by_prefix("llama-70b/"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("llama-70b/"), 2048 + 512);
    EXPECT_EQ(idx.count_by_prefix("llama-70b/layer.0/"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("llama-70b/layer.0/"), 2048);
}

// ---------------------------------------------------------------------------
// Self-cleaning cascade: leaf deletion
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, CascadeDeletesEmptyLeaf) {
    PrefixIndex idx;
    idx.insert("abc/one", 100, kClient1);
    idx.insert("abc/two", 200, kClient1);

    idx.remove("abc/one");
    idx.remove("abc/two");

    EXPECT_EQ(idx.size(), 0);
    // After removing all keys, the trie should be clean (no leftover
    // internal nodes — only root remains).
    auto keys = idx.list_keys_by_prefix("");
    EXPECT_TRUE(keys.empty());
}

// ---------------------------------------------------------------------------
// Self-cleaning cascade: no merge (delete-only), single-child nodes persist
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, CascadeLeavesInternalSingleChildNode) {
    PrefixIndex idx;
    // Creates tree: root -> "ab" -> "c" (terminal) and "d" (terminal)
    idx.insert("abc", 100, kClient1);
    idx.insert("abd", 200, kClient1);

    // Remove "abd" — "ab" now has only one child "c", but we do NOT
    // merge (future-proofing for fine-grained locking). The key "abc"
    // must still be retrievable.
    idx.remove("abd");

    EXPECT_EQ(idx.size(), 1);
    auto keys = idx.list_keys_by_prefix("");
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys[0], "abc");

    EXPECT_EQ(idx.count_by_prefix("ab"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("ab"), 100);
}

TEST(PrefixIndexTest, CascadeDeletesEmptyButKeepsTerminal) {
    PrefixIndex idx;
    idx.insert("ab", 100, kClient1);
    idx.insert("abc", 200, kClient1);
    idx.insert("abd", 300, kClient1);

    // Remove "abd". The leaf for "abd" is deleted (no children, no
    // leaf_data). "ab" is still terminal, so it stays.
    idx.remove("abd");

    EXPECT_EQ(idx.size(), 2);
    auto keys = idx.list_keys_by_prefix("");
    ASSERT_EQ(keys.size(), 2);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "ab") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "abc") != keys.end());
}

// ---------------------------------------------------------------------------
// list_keys_by_prefix
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListKeysByPrefixReturnsAllMatches) {
    PrefixIndex idx;
    idx.insert("llama-70b/layer.0/q_proj.weight", 1024, kClient1);
    idx.insert("llama-70b/layer.0/k_proj.weight", 2048, kClient1);
    idx.insert("llama-70b/layer.1/q_proj.weight", 512, kClient1);
    idx.insert("mistral-7b/layer.0/q_proj.weight", 100, kClient2);

    auto keys = idx.list_keys_by_prefix("llama-70b/");
    EXPECT_EQ(keys.size(), 3);

    auto keys2 = idx.list_keys_by_prefix("llama-70b/layer.0/");
    EXPECT_EQ(keys2.size(), 2);

    auto keys3 = idx.list_keys_by_prefix("mistral");
    EXPECT_EQ(keys3.size(), 1);
}

TEST(PrefixIndexTest, ListKeysByEmptyPrefixReturnsAll) {
    PrefixIndex idx;
    idx.insert("a", 10, kClient1);
    idx.insert("b", 20, kClient1);
    idx.insert("c", 30, kClient1);

    auto keys = idx.list_keys_by_prefix("");
    EXPECT_EQ(keys.size(), 3);
}

TEST(PrefixIndexTest, ListKeysByPrefixReturnsSortedOrder) {
    PrefixIndex idx;
    idx.insert("c/z", 1, kClient1);
    idx.insert("a/x", 1, kClient1);
    idx.insert("b/y", 1, kClient1);

    auto keys = idx.list_keys_by_prefix("");
    ASSERT_EQ(keys.size(), 3);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));
}

TEST(PrefixIndexTest, ListKeysByPrefixMidEdgeReturnsFullKeys) {
    PrefixIndex idx;
    idx.insert("mistral-7b/layer.0/q_proj.weight", 1024, kClient1);

    // "mistral" ends in the middle of the compressed edge
    // "mistral-7b/layer.0/q_proj.weight". The returned key must be
    // the full stored key, NOT the truncated prefix "mistral".
    auto keys = idx.list_keys_by_prefix("mistral");
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys[0], "mistral-7b/layer.0/q_proj.weight");
}

TEST(PrefixIndexTest, ListKeysByPrefixMidEdgeMultipleKeys) {
    PrefixIndex idx;
    idx.insert("mistral-7b/layer.0/q_proj.weight", 100, kClient1);
    idx.insert("mistral-7b/layer.1/q_proj.weight", 200, kClient1);

    // "mistral-7b/layer." ends mid-edge (the split created
    // "mistral-7b/layer." as an internal node with children "0/..."
    // and "1/..."). Both full keys must be returned.
    auto keys = idx.list_keys_by_prefix("mistral-7b/layer.");
    ASSERT_EQ(keys.size(), 2);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(),
                          "mistral-7b/layer.0/q_proj.weight") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(),
                          "mistral-7b/layer.1/q_proj.weight") != keys.end());
}

TEST(PrefixIndexTest, ListKeysByNonexistentPrefixReturnsEmpty) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);

    auto keys = idx.list_keys_by_prefix("xyz");
    EXPECT_TRUE(keys.empty());
}

// ---------------------------------------------------------------------------
// list_prefix_continuations
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListPrefixContinuationsReturnsDirectChildrenWithValues) {
    PrefixIndex idx;
    idx.insert("llama-70b/layer.0/q_proj.weight", 1024, kClient1);
    idx.insert("llama-70b/layer.1/q_proj.weight", 2048, kClient1);
    idx.insert("mistral-7b/layer.0/q_proj.weight", 100, kClient2);

    auto children = idx.list_prefix_continuations("");
    ASSERT_EQ(children.size(), 2);
    // Verify actual edge labels, not just count. std::map ordering
    // guarantees 'l' < 'm'.
    EXPECT_TRUE(children[0].find("llama") != std::string::npos);
    EXPECT_TRUE(children[1].find("mistral") != std::string::npos);
}

TEST(PrefixIndexTest, ListPrefixContinuationsMidEdgePrependsUnmatchedSuffix) {
    PrefixIndex idx;
    idx.insert("abc/one", 100, kClient1);
    idx.insert("abc/two", 200, kClient1);

    // "abc" ends mid-edge on the "abc/" node. The unmatched suffix
    // "/" is prepended to each child's edge_label, so results are
    // relative to the user's prefix "abc", not the internal node "abc/".
    auto children = idx.list_prefix_continuations("abc");
    ASSERT_EQ(children.size(), 2);
    EXPECT_TRUE(std::find(children.begin(), children.end(), "/one") !=
                children.end());
    EXPECT_TRUE(std::find(children.begin(), children.end(), "/two") !=
                children.end());

    // Same query at the exact edge boundary gives raw child labels.
    auto children_exact = idx.list_prefix_continuations("abc/");
    ASSERT_EQ(children_exact.size(), 2);
    EXPECT_TRUE(std::find(children_exact.begin(), children_exact.end(),
                          "one") != children_exact.end());
    EXPECT_TRUE(std::find(children_exact.begin(), children_exact.end(),
                          "two") != children_exact.end());
}

TEST(PrefixIndexTest, ListPrefixContinuationsOfTerminalLeafReturnsEmpty) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);

    // "abc" is a terminal leaf with no children.
    auto children = idx.list_prefix_continuations("abc");
    EXPECT_TRUE(children.empty());
}

TEST(PrefixIndexTest, ListPrefixContinuationsOfNonexistentPrefixReturnsEmpty) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);

    auto children = idx.list_prefix_continuations("xyz");
    EXPECT_TRUE(children.empty());
}

// ---------------------------------------------------------------------------
// count_by_prefix
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, CountByPrefixIsO1) {
    PrefixIndex idx;
    for (int i = 0; i < 1000; ++i) {
        idx.insert("prefix/" + std::to_string(i), 10, kClient1);
    }

    EXPECT_EQ(idx.count_by_prefix("prefix/"), 1000);
    EXPECT_EQ(idx.count_by_prefix(""), 1000);
}

TEST(PrefixIndexTest, CountByNonexistentPrefixReturnsZero) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);

    EXPECT_EQ(idx.count_by_prefix("xyz"), 0);
}

// ---------------------------------------------------------------------------
// bytes_by_prefix
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, BytesByPrefixMatchesSumOfInsertedSizes) {
    PrefixIndex idx;
    uint64_t total = 0;
    for (int i = 0; i < 100; ++i) {
        uint64_t sz = (i + 1) * 100;
        idx.insert("model/tensor_" + std::to_string(i), sz, kClient1);
        total += sz;
    }

    EXPECT_EQ(idx.bytes_by_prefix("model/"), total);
    EXPECT_EQ(idx.bytes_by_prefix(""), total);
}

TEST(PrefixIndexTest, BytesByNonexistentPrefixReturnsZero) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);

    EXPECT_EQ(idx.bytes_by_prefix("xyz"), 0);
}

// ---------------------------------------------------------------------------
// clear
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ClearRemovesEverything) {
    PrefixIndex idx;
    idx.insert("a", 10, kClient1);
    idx.insert("b", 20, kClient1);
    idx.insert("c", 30, kClient1);

    idx.clear();

    EXPECT_EQ(idx.size(), 0);
    EXPECT_EQ(idx.count_by_prefix(""), 0);
    EXPECT_EQ(idx.bytes_by_prefix(""), 0);
    EXPECT_TRUE(idx.list_keys_by_prefix("").empty());
}

// ---------------------------------------------------------------------------
// Snapshot rebuild: insert all -> clear -> re-insert -> verify
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, RebuildAfterClear) {
    PrefixIndex idx;
    idx.insert("a/1", 100, kClient1);
    idx.insert("a/2", 200, kClient1);
    idx.insert("b/1", 300, kClient2);

    idx.clear();

    idx.insert("a/1", 100, kClient1);
    idx.insert("a/2", 200, kClient1);
    idx.insert("b/1", 300, kClient2);

    EXPECT_EQ(idx.size(), 3);
    EXPECT_EQ(idx.count_by_prefix("a/"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("a/"), 300);
    EXPECT_EQ(idx.count_by_prefix("b/"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("b/"), 300);
}

// ---------------------------------------------------------------------------
// Churn: repeated insert/remove to catch subtree stats drift
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ChurnSubtreeStatsStayConsistent) {
    PrefixIndex idx;

    // Round 1: insert overlapping prefixes
    idx.insert("a/b/c", 100, kClient1);
    idx.insert("a/b/d", 200, kClient1);
    idx.insert("a/x/y", 300, kClient2);

    EXPECT_EQ(idx.size(), 3);
    EXPECT_EQ(idx.count_by_prefix("a/"), 3);
    EXPECT_EQ(idx.bytes_by_prefix("a/"), 600);
    EXPECT_EQ(idx.count_by_prefix("a/b/"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("a/b/"), 300);

    // Round 2: remove and re-insert
    idx.remove("a/b/c");
    EXPECT_EQ(idx.count_by_prefix("a/b/"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("a/b/"), 200);

    idx.insert("a/b/c", 150, kClient1);
    EXPECT_EQ(idx.count_by_prefix("a/b/"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("a/b/"), 350);

    // Round 3: remove everything under a/b/ one by one
    idx.remove("a/b/c");
    idx.remove("a/b/d");
    EXPECT_EQ(idx.count_by_prefix("a/b/"), 0);
    EXPECT_EQ(idx.bytes_by_prefix("a/b/"), 0);
    EXPECT_EQ(idx.count_by_prefix("a/"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("a/"), 300);

    // Round 4: remove last key
    idx.remove("a/x/y");
    EXPECT_EQ(idx.size(), 0);
    EXPECT_EQ(idx.count_by_prefix(""), 0);
    EXPECT_EQ(idx.bytes_by_prefix(""), 0);

    // Round 5: re-insert after full drain
    idx.insert("a/b/c", 100, kClient1);
    EXPECT_EQ(idx.size(), 1);
    EXPECT_EQ(idx.count_by_prefix("a/"), 1);
    EXPECT_EQ(idx.bytes_by_prefix("a/"), 100);
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ConcurrentInsertsAndReads) {
    PrefixIndex idx;
    constexpr int kNumWriters = 4;
    constexpr int kKeysPerWriter = 500;
    constexpr int kNumReaders = 4;

    std::vector<std::thread> threads;

    // Writers
    for (int w = 0; w < kNumWriters; ++w) {
        threads.emplace_back([&idx, w]() {
            for (int i = 0; i < kKeysPerWriter; ++i) {
                std::string key = "w" + std::to_string(w) + "/key_" +
                                  std::to_string(i);
                idx.insert(key, 10, kClient1);
            }
        });
    }

    // Readers (run concurrently with writers)
    for (int r = 0; r < kNumReaders; ++r) {
        threads.emplace_back([&idx]() {
            for (int i = 0; i < 100; ++i) {
                idx.count_by_prefix("");
                idx.list_keys_by_prefix("w0/");
                idx.bytes_by_prefix("w1/");
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(idx.size(), kNumWriters * kKeysPerWriter);
}

TEST(PrefixIndexTest, ConcurrentInsertsAndRemoves) {
    PrefixIndex idx;
    constexpr int kNumKeys = 1000;

    // Pre-populate
    for (int i = 0; i < kNumKeys; ++i) {
        idx.insert("key_" + std::to_string(i), 10, kClient1);
    }
    EXPECT_EQ(idx.size(), kNumKeys);

    std::vector<std::thread> threads;

    // Remove even keys
    threads.emplace_back([&idx]() {
        for (int i = 0; i < kNumKeys; i += 2) {
            idx.remove("key_" + std::to_string(i));
        }
    });

    // Insert new keys concurrently
    threads.emplace_back([&idx]() {
        for (int i = kNumKeys; i < kNumKeys + 500; ++i) {
            idx.insert("key_" + std::to_string(i), 20, kClient2);
        }
    });

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(idx.size(), kNumKeys / 2 + 500);
}

TEST(PrefixIndexTest, ConcurrentReadsAndRemovesSamePrefix) {
    PrefixIndex idx;
    constexpr int kNumKeys = 500;

    // Pre-populate under a shared prefix
    for (int i = 0; i < kNumKeys; ++i) {
        idx.insert("shared/key_" + std::to_string(i), 10, kClient1);
    }
    EXPECT_EQ(idx.count_by_prefix("shared/"), kNumKeys);

    std::vector<std::thread> threads;

    // Remover: deletes all keys under the prefix
    threads.emplace_back([&idx]() {
        for (int i = 0; i < kNumKeys; ++i) {
            idx.remove("shared/key_" + std::to_string(i));
        }
    });

    // Readers: query the same prefix being drained
    for (int r = 0; r < 4; ++r) {
        threads.emplace_back([&idx]() {
            for (int i = 0; i < 200; ++i) {
                auto count = idx.count_by_prefix("shared/");
                (void)count;  // just exercising thread safety
                auto keys = idx.list_keys_by_prefix("shared/");
                (void)keys;
                auto bytes = idx.bytes_by_prefix("shared/");
                (void)bytes;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(idx.count_by_prefix("shared/"), 0);
    EXPECT_EQ(idx.size(), 0);
}

// ---------------------------------------------------------------------------
// Mid-edge list_prefix_continuations edge cases
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListPrefixContinuationsMidEdgeOnLeafReturnsSuffix) {
    PrefixIndex idx;
    idx.insert("mistral-7b/layer.0/q_proj.weight", 1024, kClient1);

    // "mistral" ends mid-edge on the terminal leaf. The unmatched
    // suffix "-7b/layer.0/q_proj.weight" is the continuation.
    auto children = idx.list_prefix_continuations("mistral");
    ASSERT_EQ(children.size(), 1);
    EXPECT_EQ(children[0], "-7b/layer.0/q_proj.weight");
}

TEST(PrefixIndexTest, ListPrefixContinuationsMidEdgeShortPrefix) {
    PrefixIndex idx;
    idx.insert("abc/one", 100, kClient1);
    idx.insert("abc/two", 200, kClient1);

    // "ab" ends mid-edge on the "abc/" node. Unmatched suffix "c/"
    // is prepended to children.
    auto children = idx.list_prefix_continuations("ab");
    ASSERT_EQ(children.size(), 2);
    EXPECT_TRUE(std::find(children.begin(), children.end(), "c/one") !=
                children.end());
    EXPECT_TRUE(std::find(children.begin(), children.end(), "c/two") !=
                children.end());
}

// ---------------------------------------------------------------------------
// list_prefix_continuations: freeze compressed-edge semantics
// (returns full trie edges, not delimiter-split segments)
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListPrefixContinuationsMidEdgeSingleStoredKey) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);

    // "ab" ends mid-edge on the terminal leaf "abc". The unmatched
    // suffix "c" is returned as the continuation.
    auto children = idx.list_prefix_continuations("ab");
    ASSERT_EQ(children.size(), 1);
    EXPECT_EQ(children[0], "c");
}

TEST(PrefixIndexTest, ListPrefixContinuationsExposesCompressedTrieEdges) {
    PrefixIndex idx;
    idx.insert("ab/one", 100, kClient1);
    idx.insert("ac/two", 200, kClient1);

    // Tree: root → "a" (internal) → "b/one" (terminal), "c/two" (terminal)
    // list_prefix_continuations("a") returns the FULL compressed edges from the "a"
    // node, not truncated-at-delimiter immediate children. This is the
    // intended radix tree behavior — edges can span multiple "levels."
    auto children = idx.list_prefix_continuations("a");
    ASSERT_EQ(children.size(), 2);
    EXPECT_EQ(children[0], "b/one");
    EXPECT_EQ(children[1], "c/two");
}

TEST(PrefixIndexTest, ListPrefixContinuationsAtExactBranchPoint) {
    PrefixIndex idx;
    idx.insert("ab/one", 100, kClient1);
    idx.insert("ab/two", 200, kClient1);
    idx.insert("ac/three", 300, kClient1);

    // Tree: root → "a" (internal)
    //                ├── "b/" (internal) → "one", "two"
    //                └── "c/three" (terminal)
    auto children = idx.list_prefix_continuations("a");
    ASSERT_EQ(children.size(), 2);
    EXPECT_EQ(children[0], "b/");       // compressed edge to branch node
    EXPECT_EQ(children[1], "c/three");  // compressed edge to leaf

    // Drilling into "ab/" gives the next level.
    auto children2 = idx.list_prefix_continuations("ab/");
    ASSERT_EQ(children2.size(), 2);
    EXPECT_EQ(children2[0], "one");
    EXPECT_EQ(children2[1], "two");
}

TEST(PrefixIndexTest, ListPrefixContinuationsMidEdgeTerminalAndInternal) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);
    idx.insert("abc/one", 200, kClient1);

    // Tree: root → "abc" (terminal) → "/one" (terminal)
    // list_prefix_continuations("ab") lands mid-edge on "abc". Unmatched suffix
    // is "c". The node is terminal (so "c" is a continuation) AND has
    // children (so "c/one" is also a continuation).
    auto children = idx.list_prefix_continuations("ab");
    ASSERT_EQ(children.size(), 2);
    EXPECT_EQ(children[0], "c");      // terminal continuation
    EXPECT_EQ(children[1], "c/one");  // child continuation
}

// ---------------------------------------------------------------------------
// Upsert: ownership change on re-insert
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, UpsertChangesClientAndAdjustsBytes) {
    PrefixIndex idx;
    idx.insert("tenant-a/obj1", 100, kClient1);
    idx.insert("tenant-a/obj2", 200, kClient1);

    EXPECT_EQ(idx.bytes_by_prefix("tenant-a/"), 300);

    // Re-register obj1 under a different client with different size.
    idx.insert("tenant-a/obj1", 150, kClient2);

    // Count unchanged, bytes adjusted.
    EXPECT_EQ(idx.size(), 2);
    EXPECT_EQ(idx.count_by_prefix("tenant-a/"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("tenant-a/"), 350);
    EXPECT_EQ(idx.bytes_by_prefix(""), 350);
}

TEST(PrefixIndexTest, UpsertOnNodeThatIsAlsoPrefix) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);
    idx.insert("abcdef", 200, kClient1);

    EXPECT_EQ(idx.bytes_by_prefix("abc"), 300);

    // Re-insert "abc" with different size.
    idx.insert("abc", 50, kClient2);

    EXPECT_EQ(idx.size(), 2);
    EXPECT_EQ(idx.count_by_prefix("abc"), 2);
    EXPECT_EQ(idx.bytes_by_prefix("abc"), 250);
}

// ---------------------------------------------------------------------------
// Invariant: list_keys_by_prefix results must start with the prefix,
// and count_by_prefix must equal the number of returned keys.
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, InvariantPrefixConsistency) {
    PrefixIndex idx;
    idx.insert("aaa/1", 10, kClient1);
    idx.insert("aaa/2", 20, kClient1);
    idx.insert("aab/3", 30, kClient1);
    idx.insert("bbb/4", 40, kClient2);
    idx.insert("bbb/5", 50, kClient2);

    std::vector<std::string> prefixes = {"", "a", "aa", "aaa", "aaa/",
                                         "aab", "b", "bbb", "bbb/", "xyz"};

    for (const auto& p : prefixes) {
        auto keys = idx.list_keys_by_prefix(p);
        auto count = idx.count_by_prefix(p);

        // count must match number of returned keys.
        EXPECT_EQ(count, keys.size())
            << "Mismatch for prefix \"" << p << "\"";

        // Every returned key must start with the prefix.
        for (const auto& k : keys) {
            EXPECT_TRUE(k.substr(0, p.size()) == p)
                << "Key \"" << k << "\" does not start with prefix \""
                << p << "\"";
        }
    }
}

// ---------------------------------------------------------------------------
// Upsert: size decrease then remove (exercises negative size_delta)
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, UpsertSizeDecreaseThenRemove) {
    PrefixIndex idx;
    idx.insert("obj", 500, kClient1);
    EXPECT_EQ(idx.bytes_by_prefix(""), 500);

    // Upsert with smaller size (negative size_delta in update_ancestors).
    idx.insert("obj", 200, kClient2);
    EXPECT_EQ(idx.size(), 1);
    EXPECT_EQ(idx.bytes_by_prefix(""), 200);

    // Remove should bring everything to zero without underflow.
    idx.remove("obj");
    EXPECT_EQ(idx.size(), 0);
    EXPECT_EQ(idx.bytes_by_prefix(""), 0);
}

// ---------------------------------------------------------------------------
// Repeated upsert churn on same key with varying size/client
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, RepeatedUpsertChurn) {
    PrefixIndex idx;
    idx.insert("churn/key", 100, kClient1);
    idx.insert("churn/other", 50, kClient1);

    // Upsert the same key many times with varying sizes.
    uint64_t last_size = 100;
    for (int i = 0; i < 50; ++i) {
        uint64_t new_size = (i + 1) * 7;
        UUID client = (i % 2 == 0) ? kClient1 : kClient2;
        idx.insert("churn/key", new_size, client);
        last_size = new_size;
    }

    EXPECT_EQ(idx.size(), 2);
    EXPECT_EQ(idx.count_by_prefix("churn/"), 2);
    // "churn/other" is 50, "churn/key" is the last upserted size.
    EXPECT_EQ(idx.bytes_by_prefix("churn/"), last_size + 50);
    EXPECT_EQ(idx.bytes_by_prefix(""), last_size + 50);

    // Remove both and verify clean state.
    idx.remove("churn/key");
    idx.remove("churn/other");
    EXPECT_EQ(idx.size(), 0);
    EXPECT_EQ(idx.bytes_by_prefix(""), 0);
}

// ---------------------------------------------------------------------------
// Extended invariant: bytes_by_prefix equals sum of listed key sizes
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, InvariantBytesSumConsistency) {
    PrefixIndex idx;
    // Insert keys with known sizes under two prefixes.
    idx.insert("x/a", 10, kClient1);
    idx.insert("x/b", 20, kClient1);
    idx.insert("x/c", 30, kClient1);
    idx.insert("y/d", 40, kClient2);
    idx.insert("y/e", 50, kClient2);

    // Upsert a key to change its size.
    idx.insert("x/a", 15, kClient2);

    // Remove one key.
    idx.remove("y/d");

    // Verify bytes_by_prefix("") == sum of all remaining sizes.
    // Remaining: x/a=15, x/b=20, x/c=30, y/e=50
    EXPECT_EQ(idx.bytes_by_prefix(""), 15 + 20 + 30 + 50);
    EXPECT_EQ(idx.bytes_by_prefix("x/"), 15 + 20 + 30);
    EXPECT_EQ(idx.bytes_by_prefix("y/"), 50);
    EXPECT_EQ(idx.count_by_prefix(""), 4);
    EXPECT_EQ(idx.count_by_prefix("x/"), 3);
    EXPECT_EQ(idx.count_by_prefix("y/"), 1);
}

// ---------------------------------------------------------------------------
// Stress: churn with periodic clear
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, StressChurnWithPeriodicClear) {
    PrefixIndex idx;
    constexpr int kRounds = 5;
    constexpr int kKeysPerRound = 200;

    for (int round = 0; round < kRounds; ++round) {
        // Insert keys
        uint64_t expected_bytes = 0;
        for (int i = 0; i < kKeysPerRound; ++i) {
            uint64_t sz = (i + 1) * 10;
            idx.insert("r" + std::to_string(round) + "/k" +
                           std::to_string(i),
                       sz, kClient1);
            expected_bytes += sz;
        }

        std::string prefix = "r" + std::to_string(round) + "/";
        EXPECT_EQ(idx.count_by_prefix(prefix), kKeysPerRound);
        EXPECT_EQ(idx.bytes_by_prefix(prefix), expected_bytes);

        // Remove half
        for (int i = 0; i < kKeysPerRound; i += 2) {
            uint64_t sz = (i + 1) * 10;
            idx.remove("r" + std::to_string(round) + "/k" +
                       std::to_string(i));
            expected_bytes -= sz;
        }

        EXPECT_EQ(idx.count_by_prefix(prefix), kKeysPerRound / 2);
        EXPECT_EQ(idx.bytes_by_prefix(prefix), expected_bytes);

        // Periodic clear every other round
        if (round % 2 == 1) {
            idx.clear();
            EXPECT_EQ(idx.size(), 0);
            EXPECT_EQ(idx.bytes_by_prefix(""), 0);
        }
    }
}

// ===========================================================================
// list_entries_by_prefix
// ===========================================================================

// ---------------------------------------------------------------------------
// Basic functionality
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListEntriesEmptyTrieReturnsEmpty) {
    PrefixIndex idx;
    auto entries = idx.list_entries_by_prefix("anything");
    EXPECT_TRUE(entries.empty());

    entries = idx.list_entries_by_prefix("");
    EXPECT_TRUE(entries.empty());
}

TEST(PrefixIndexTest, ListEntriesSingleKeyExactPrefix) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);

    auto entries = idx.list_entries_by_prefix("abc");
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries[0].first, "abc");
    EXPECT_EQ(entries[0].second.size, 100);
    EXPECT_EQ(entries[0].second.client_id, kClient1);
}

TEST(PrefixIndexTest, ListEntriesSingleKeyStrictPrefix) {
    PrefixIndex idx;
    idx.insert("abc/def", 256, kClient2);

    auto entries = idx.list_entries_by_prefix("abc/");
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries[0].first, "abc/def");
    EXPECT_EQ(entries[0].second.size, 256);
    EXPECT_EQ(entries[0].second.client_id, kClient2);
}

TEST(PrefixIndexTest, ListEntriesMultipleKeysSharedPrefix) {
    PrefixIndex idx;
    idx.insert("llama-70b/layer.0/q_proj.weight", 1024, kClient1);
    idx.insert("llama-70b/layer.0/k_proj.weight", 2048, kClient1);
    idx.insert("llama-70b/layer.1/q_proj.weight", 512, kClient2);

    auto entries = idx.list_entries_by_prefix("llama-70b/");
    ASSERT_EQ(entries.size(), 3);

    std::map<std::string, TrieLeafData> by_key;
    for (const auto& [k, v] : entries) by_key[k] = v;

    EXPECT_EQ(by_key["llama-70b/layer.0/q_proj.weight"].size, 1024);
    EXPECT_EQ(by_key["llama-70b/layer.0/q_proj.weight"].client_id, kClient1);
    EXPECT_EQ(by_key["llama-70b/layer.0/k_proj.weight"].size, 2048);
    EXPECT_EQ(by_key["llama-70b/layer.1/q_proj.weight"].size, 512);
    EXPECT_EQ(by_key["llama-70b/layer.1/q_proj.weight"].client_id, kClient2);
}

TEST(PrefixIndexTest, ListEntriesEmptyPrefixReturnsAll) {
    PrefixIndex idx;
    idx.insert("a/x", 10, kClient1);
    idx.insert("b/y", 20, kClient2);
    idx.insert("c/z", 30, kClient1);

    auto entries = idx.list_entries_by_prefix("");
    ASSERT_EQ(entries.size(), 3);
}

TEST(PrefixIndexTest, ListEntriesDisjointPrefixReturnsOnlyMatching) {
    PrefixIndex idx;
    idx.insert("a/one", 100, kClient1);
    idx.insert("a/two", 200, kClient1);
    idx.insert("b/three", 300, kClient2);

    auto entries = idx.list_entries_by_prefix("a/");
    ASSERT_EQ(entries.size(), 2);
    for (const auto& [k, v] : entries) {
        EXPECT_TRUE(k.substr(0, 2) == "a/");
    }
}

// ---------------------------------------------------------------------------
// Leaf data correctness
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListEntriesVerifiesSizeAndClientId) {
    PrefixIndex idx;
    idx.insert("tenant-a/obj1", 100, kClient1);
    idx.insert("tenant-a/obj2", 200, kClient2);
    idx.insert("tenant-b/obj3", 300, kClient1);

    auto entries = idx.list_entries_by_prefix("tenant-a/");
    ASSERT_EQ(entries.size(), 2);

    std::map<std::string, TrieLeafData> by_key;
    for (const auto& [k, v] : entries) by_key[k] = v;

    EXPECT_EQ(by_key["tenant-a/obj1"].size, 100);
    EXPECT_EQ(by_key["tenant-a/obj1"].client_id, kClient1);
    EXPECT_EQ(by_key["tenant-a/obj2"].size, 200);
    EXPECT_EQ(by_key["tenant-a/obj2"].client_id, kClient2);
}

TEST(PrefixIndexTest, ListEntriesAfterUpsertReflectsLatestData) {
    PrefixIndex idx;
    idx.insert("key", 100, kClient1);
    idx.insert("key", 200, kClient2);

    auto entries = idx.list_entries_by_prefix("key");
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries[0].second.size, 200);
    EXPECT_EQ(entries[0].second.client_id, kClient2);
}

TEST(PrefixIndexTest, ListEntriesMultipleClientsUnderPrefix) {
    PrefixIndex idx;
    idx.insert("shared/obj1", 10, kClient1);
    idx.insert("shared/obj2", 20, kClient2);
    idx.insert("shared/obj3", 30, kClient1);

    auto entries = idx.list_entries_by_prefix("shared/");
    ASSERT_EQ(entries.size(), 3);

    std::map<std::string, TrieLeafData> by_key;
    for (const auto& [k, v] : entries) by_key[k] = v;

    EXPECT_EQ(by_key["shared/obj1"].client_id, kClient1);
    EXPECT_EQ(by_key["shared/obj2"].client_id, kClient2);
    EXPECT_EQ(by_key["shared/obj3"].client_id, kClient1);
}

// ---------------------------------------------------------------------------
// Mid-edge prefix cases
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListEntriesMidEdgeSingleKey) {
    PrefixIndex idx;
    idx.insert("mistral-7b/layer.0/q_proj.weight", 1024, kClient1);

    auto entries = idx.list_entries_by_prefix("mistral");
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries[0].first, "mistral-7b/layer.0/q_proj.weight");
    EXPECT_EQ(entries[0].second.size, 1024);
    EXPECT_EQ(entries[0].second.client_id, kClient1);
}

TEST(PrefixIndexTest, ListEntriesMidEdgeMultipleKeys) {
    PrefixIndex idx;
    idx.insert("mistral-7b/layer.0/q_proj.weight", 100, kClient1);
    idx.insert("mistral-7b/layer.1/q_proj.weight", 200, kClient2);

    auto entries = idx.list_entries_by_prefix("mistral-7b/layer.");
    ASSERT_EQ(entries.size(), 2);

    std::map<std::string, TrieLeafData> by_key;
    for (const auto& [k, v] : entries) by_key[k] = v;

    EXPECT_EQ(by_key["mistral-7b/layer.0/q_proj.weight"].size, 100);
    EXPECT_EQ(by_key["mistral-7b/layer.1/q_proj.weight"].size, 200);
    EXPECT_EQ(by_key["mistral-7b/layer.1/q_proj.weight"].client_id, kClient2);
}

// ---------------------------------------------------------------------------
// Structural edge cases
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListEntriesTerminalAndInternalNode) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);
    idx.insert("abc/def", 200, kClient2);

    auto entries = idx.list_entries_by_prefix("abc");
    ASSERT_EQ(entries.size(), 2);

    std::map<std::string, TrieLeafData> by_key;
    for (const auto& [k, v] : entries) by_key[k] = v;

    EXPECT_EQ(by_key["abc"].size, 100);
    EXPECT_EQ(by_key["abc"].client_id, kClient1);
    EXPECT_EQ(by_key["abc/def"].size, 200);
    EXPECT_EQ(by_key["abc/def"].client_id, kClient2);
}

TEST(PrefixIndexTest, ListEntriesAfterRemoveExcludesDeletedKey) {
    PrefixIndex idx;
    idx.insert("p/a", 10, kClient1);
    idx.insert("p/b", 20, kClient1);
    idx.insert("p/c", 30, kClient2);

    idx.remove("p/b");

    auto entries = idx.list_entries_by_prefix("p/");
    ASSERT_EQ(entries.size(), 2);

    std::set<std::string> keys;
    for (const auto& [k, v] : entries) keys.insert(k);
    EXPECT_TRUE(keys.count("p/a"));
    EXPECT_FALSE(keys.count("p/b"));
    EXPECT_TRUE(keys.count("p/c"));
}

TEST(PrefixIndexTest, ListEntriesAfterClearReturnsEmpty) {
    PrefixIndex idx;
    idx.insert("a", 10, kClient1);
    idx.insert("b", 20, kClient1);

    idx.clear();

    auto entries = idx.list_entries_by_prefix("");
    EXPECT_TRUE(entries.empty());
}

TEST(PrefixIndexTest, ListEntriesAfterClearAndRebuild) {
    PrefixIndex idx;
    idx.insert("x/1", 100, kClient1);
    idx.insert("x/2", 200, kClient2);

    idx.clear();

    idx.insert("x/1", 150, kClient2);
    idx.insert("x/2", 250, kClient1);

    auto entries = idx.list_entries_by_prefix("x/");
    ASSERT_EQ(entries.size(), 2);

    std::map<std::string, TrieLeafData> by_key;
    for (const auto& [k, v] : entries) by_key[k] = v;

    EXPECT_EQ(by_key["x/1"].size, 150);
    EXPECT_EQ(by_key["x/1"].client_id, kClient2);
    EXPECT_EQ(by_key["x/2"].size, 250);
    EXPECT_EQ(by_key["x/2"].client_id, kClient1);
}

// ---------------------------------------------------------------------------
// Nonexistent / miss cases
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListEntriesNonexistentPrefixReturnsEmpty) {
    PrefixIndex idx;
    idx.insert("abc", 100, kClient1);

    auto entries = idx.list_entries_by_prefix("xyz");
    EXPECT_TRUE(entries.empty());
}

TEST(PrefixIndexTest, ListEntriesPrefixDivergesMidEdgeReturnsEmpty) {
    PrefixIndex idx;
    idx.insert("abcdef", 100, kClient1);

    auto entries = idx.list_entries_by_prefix("abx");
    EXPECT_TRUE(entries.empty());
}

// ---------------------------------------------------------------------------
// Sorted order
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListEntriesReturnsSortedByKey) {
    PrefixIndex idx;
    idx.insert("c/z", 30, kClient1);
    idx.insert("a/x", 10, kClient1);
    idx.insert("b/y", 20, kClient2);

    auto entries = idx.list_entries_by_prefix("");
    ASSERT_EQ(entries.size(), 3);

    std::vector<std::string> keys;
    for (const auto& [k, v] : entries) keys.push_back(k);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));
}

// ---------------------------------------------------------------------------
// Cross-consistency invariants
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ListEntriesCountMatchesCountByPrefix) {
    PrefixIndex idx;
    idx.insert("aaa/1", 10, kClient1);
    idx.insert("aaa/2", 20, kClient1);
    idx.insert("aab/3", 30, kClient2);
    idx.insert("bbb/4", 40, kClient2);

    std::vector<std::string> prefixes = {"", "a", "aa", "aaa", "aaa/",
                                         "aab", "b", "bbb", "xyz"};
    for (const auto& p : prefixes) {
        auto entries = idx.list_entries_by_prefix(p);
        auto count = idx.count_by_prefix(p);
        EXPECT_EQ(entries.size(), count)
            << "Mismatch for prefix \"" << p << "\"";
    }
}

TEST(PrefixIndexTest, ListEntriesBytesSumMatchesBytesByPrefix) {
    PrefixIndex idx;
    idx.insert("aaa/1", 10, kClient1);
    idx.insert("aaa/2", 20, kClient1);
    idx.insert("aab/3", 30, kClient2);
    idx.insert("bbb/4", 40, kClient2);

    std::vector<std::string> prefixes = {"", "a", "aa", "aaa", "aaa/",
                                         "aab", "b", "bbb", "xyz"};
    for (const auto& p : prefixes) {
        auto entries = idx.list_entries_by_prefix(p);
        uint64_t sum = 0;
        for (const auto& [k, v] : entries) sum += v.size;
        EXPECT_EQ(sum, idx.bytes_by_prefix(p))
            << "Mismatch for prefix \"" << p << "\"";
    }
}

TEST(PrefixIndexTest, ListEntriesKeysMatchListKeysByPrefix) {
    PrefixIndex idx;
    idx.insert("aaa/1", 10, kClient1);
    idx.insert("aaa/2", 20, kClient1);
    idx.insert("aab/3", 30, kClient2);
    idx.insert("bbb/4", 40, kClient2);

    std::vector<std::string> prefixes = {"", "a", "aa", "aaa", "aaa/",
                                         "aab", "b", "bbb", "xyz"};
    for (const auto& p : prefixes) {
        auto entries = idx.list_entries_by_prefix(p);
        auto keys_only = idx.list_keys_by_prefix(p);

        std::vector<std::string> entry_keys;
        for (const auto& [k, v] : entries) entry_keys.push_back(k);

        EXPECT_EQ(entry_keys, keys_only)
            << "Key mismatch for prefix \"" << p << "\"";
    }
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

TEST(PrefixIndexTest, ConcurrentInsertsAndListEntries) {
    PrefixIndex idx;
    constexpr int kNumWriters = 4;
    constexpr int kKeysPerWriter = 500;
    constexpr int kNumReaders = 4;

    std::vector<std::thread> threads;

    for (int w = 0; w < kNumWriters; ++w) {
        threads.emplace_back([&idx, w]() {
            for (int i = 0; i < kKeysPerWriter; ++i) {
                std::string key = "w" + std::to_string(w) + "/key_" +
                                  std::to_string(i);
                idx.insert(key, 10, kClient1);
            }
        });
    }

    for (int r = 0; r < kNumReaders; ++r) {
        threads.emplace_back([&idx]() {
            for (int i = 0; i < 100; ++i) {
                auto entries = idx.list_entries_by_prefix("w0/");
                (void)entries;
                auto all = idx.list_entries_by_prefix("");
                (void)all;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto all_entries = idx.list_entries_by_prefix("");
    EXPECT_EQ(all_entries.size(), kNumWriters * kKeysPerWriter);

    for (const auto& [k, v] : all_entries) {
        EXPECT_EQ(v.size, 10);
        EXPECT_EQ(v.client_id, kClient1);
    }
}

}  // namespace mooncake::test
