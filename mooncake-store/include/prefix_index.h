#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "mutex.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Summary fields stored at terminal radix nodes (not full metadata).
 *
 * Contract: written on first insert for a key; on duplicate insert the
 * entire struct is replaced (upsert). `size` and `client_id` may both
 * change when the same key is re-registered (e.g. different tenant).
 * This is not a snapshot of ObjectMetadata at a single point in time —
 * it tracks whatever the trie was told last. No replica data; replicas
 * always come from the hash-sharded index.
 */
struct TrieLeafData {
    /// Object size in bytes from the last successful insert/upsert.
    uint64_t size;

    /// Client UUID from the last successful insert/upsert.
    UUID client_id;
};

/**
 * @brief A node in the compressed radix tree (Patricia trie).
 *
 * Each node represents a compressed edge from its parent. The full key
 * for a terminal node is reconstructed by concatenating edge_label values
 * from root to that node.
 *
 * Example tree for keys "llama-70b/layer.0/q_proj.weight" and
 * "llama-70b/layer.1/q_proj.weight":
 *
 *   root
 *    └── "llama-70b/layer." (internal, subtree_count=2)
 *          ├── "0/q_proj.weight" (terminal)
 *          └── "1/q_proj.weight" (terminal)
 */
struct RadixNode {
    /// The compressed key segment on the edge FROM parent TO this node.
    /// For the root node this is always empty.
    std::string edge_label;

    /// Present (has_value() == true) only when a complete stored key ends
    /// at this node. A node can be both terminal and have children (e.g.
    /// "llama-70b/" is a stored key and also a prefix of longer keys).
    std::optional<TrieLeafData> leaf_data;

    /// Number of terminal nodes in this subtree (including self).
    /// Enables O(prefix_length) CountByPrefix.
    size_t subtree_count{0};

    /// Sum of TrieLeafData::size across all terminal nodes in this subtree.
    /// Enables O(prefix_length) BytesByPrefix for quota checks.
    uint64_t subtree_bytes{0};

    /// Weak pointer to the parent node. Used for self-cleaning cascade
    /// on remove. Root node's parent is expired (no parent).
    std::weak_ptr<RadixNode> parent;

    /// Child nodes keyed on the first character of the child's edge_label.
    /// In a radix tree no two children can share the same first character.
    /// std::map keeps children sorted for deterministic iteration order.
    std::map<char, std::shared_ptr<RadixNode>> children;

    /// @return true if a complete stored key ends at this node.
    bool is_terminal() const { return leaf_data.has_value(); }
};

/**
 * @brief A compressed radix tree (Patricia trie) index for efficient
 * prefix-based key enumeration, counting, and quota aggregation.
 *
 * This is a secondary index alongside the existing hash-sharded
 * metadata_shards_ in MasterService. It stores only lightweight
 * TrieLeafData at terminal nodes — no replica information. Replica
 * data is always read from the authoritative hash-sharded index.
 *
 * Thread safety: All public methods are thread-safe. A single SharedMutex
 * protects the entire trie. Write operations (insert, remove, clear) take
 * an exclusive lock. Read operations (list_keys_by_prefix,
 * list_prefix_continuations, count_by_prefix, bytes_by_prefix) take a
 * shared lock.
 *
 * Intended future service/API mapping:
 * - ListByPrefix            -> list_keys_by_prefix
 * - GetByPrefix            -> list_entries_by_prefix
 * - CountByPrefix          -> count_by_prefix
 * - BytesByPrefix          -> bytes_by_prefix
 * - ListPrefixContinuations -> list_prefix_continuations
 *
 * Lock ordering when used with MasterService:
 *   1. metadata_shards_[i].mutex  (shard lock, acquired first)
 *   2. trie_mutex_                (trie lock, acquired inside PrefixIndex)
 */
class PrefixIndex {
   public:
    PrefixIndex();

    /**
     * @brief Insert a key into the trie with associated leaf data.
     *
     * Handles three cases during traversal:
     * - Full edge match: consume edge, recurse into matching child.
     * - Partial edge match: split the existing edge, create intermediate
     *   node that inherits subtree stats from the split node.
     * - No match: create a new terminal leaf as a child.
     *
     * Increments subtree_count by 1 and adds size to subtree_bytes
     * on the new terminal node and all ancestors up to root.
     *
     * If the key already exists, leaf_data is updated to the new
     * values (upsert semantics). This ensures client_id stays in
     * sync when ownership changes on re-register. subtree_bytes
     * is adjusted if size differs; subtree_count stays unchanged.
     *
     * @param key    The full object key string. Must not be empty.
     * @param size   Object size in bytes (from ObjectMetadata on insert).
     * @param client_id  Client UUID for this registration (may change on
     * upsert).
     */
    void insert(const std::string& key, uint64_t size, const UUID& client_id);

    /**
     * @brief Remove a key from the trie and trigger self-cleaning cascade.
     *
     * Clears leaf_data on the terminal node, decrements subtree_count
     * and subtree_bytes on all ancestors, then walks upward deleting
     * any node that is both non-terminal and childless. Stops as soon
     * as a useful node (has children or leaf_data) is reached.
     *
     * Single-child internal nodes are intentionally NOT merged — this
     * keeps cascade logic simple and future-compatible with fine-grained
     * locking.
     *
     * If the key does not exist, this is a no-op.
     *
     * @param key  The full object key string to remove. Must not be empty.
     */
    void remove(const std::string& key);

    /**
     * @brief Remove all keys from the trie.
     *
     * Replaces the root with a fresh empty node. All shared_ptr references
     * to old nodes are released.
     */
    void clear();

    /**
     * @brief Enumerate all stored keys that start with the given prefix.
     *
     * Walks the trie to the prefix node, then performs a DFS collecting
     * all terminal node keys by concatenating edge_labels along the path.
     *
     * @param prefix  The prefix to search for. Empty string returns all keys.
     * @return Vector of full key strings under the prefix, in sorted order
     *         (due to std::map ordering of children).
     */
    std::vector<std::string> list_keys_by_prefix(
        const std::string& prefix) const;

    /**
     * @brief Enumerate all stored entries (key + leaf data) under a prefix.
     *
     * Same traversal as list_keys_by_prefix but also captures the
     * TrieLeafData at each terminal node. Collected in a single DFS pass
     * under one shared-lock acquisition, so the result is a consistent
     * snapshot of the trie at the time of the call.
     *
     * @param prefix  The prefix to search for. Empty string returns all
     * entries.
     * @return Vector of (key, TrieLeafData) pairs, sorted by key.
     */
    std::vector<std::pair<std::string, TrieLeafData>> list_entries_by_prefix(
        const std::string& prefix) const;

    /**
     * @brief Enumerate ways to extend `prefix` toward stored keys.
     *
     * This is NOT "one output per trie child node" in all cases. Each
     * returned string `s` is a continuation: `prefix + s` is a prefix of
     * at least one stored key (or equals a stored key when `s` is the
     * sole terminal continuation). Results are sorted lexicographically.
     *
     * At an internal node boundary, outputs are typically each child's
     * `edge_label` (possibly with a mid-edge prefix prepended). On a
     * mid-edge match, the implementation may synthesize continuations that
     * are not raw child edge labels (e.g. terminal suffix `"c"` when
     * only `"abc"` exists and the query is `"ab"`).
     *
     * Examples:
     *   list_prefix_continuations("llama-70b/") → ["layer.0/...", ...]
     *   list_prefix_continuations("ab") with keys "abc/one","abc/two"
     *     → ["c/one", "c/two"]
     *   list_prefix_continuations("ab") with only "abc" → ["c"]
     *
     * @param prefix  Prefix to extend from (empty = from the root).
     */
    std::vector<std::string> list_prefix_continuations(
        const std::string& prefix) const;

    /**
     * @brief Count the number of stored keys under a prefix.
     *
     * O(prefix_length) — reads subtree_count directly from the prefix node.
     *
     * @param prefix  The prefix to count under. Empty string counts all keys.
     * @return Number of terminal nodes (stored keys) under the prefix.
     */
    size_t count_by_prefix(const std::string& prefix) const;

    /**
     * @brief Get the total bytes of all stored objects under a prefix.
     *
     * O(prefix_length) — reads subtree_bytes directly from the prefix node.
     * Useful for multi-tenant quota checks.
     *
     * @param prefix  The prefix to aggregate under.
     * @return Sum of TrieLeafData::size for all terminal nodes under prefix.
     */
    uint64_t bytes_by_prefix(const std::string& prefix) const;

    /**
     * @brief Get the total number of keys in the trie.
     * @return The root's subtree_count.
     */
    size_t size() const;

   private:
    /// Root node of the trie. Always non-null. Has empty edge_label
    /// and expired parent weak_ptr.
    std::shared_ptr<RadixNode> root_;

    /// Protects the entire trie. Write ops take exclusive lock, read ops
    /// take shared lock.
    mutable SharedMutex trie_mutex_;

    /**
     * @brief Navigate the trie to find the node matching a prefix.
     *
     * Handles partial edge matches: if the prefix ends in the middle of
     * an edge_label, the node owning that edge is returned (the prefix
     * is a valid prefix of keys under that node).
     *
     * @param prefix     The prefix string to navigate to.
     * @param node_path  If non-null, set to the full concatenation of
     *                   edge_labels from root to the returned node. This
     *                   may be longer than prefix when the prefix ends
     *                   mid-edge (e.g. prefix="mistral" → node_path=
     *                   "mistral-7b/layer.0/q_proj.weight").
     * @return The node at the end of the prefix path, or nullptr if not found.
     */
    std::shared_ptr<RadixNode> find_prefix_node(
        const std::string& prefix,
        std::string* node_path = nullptr) const NO_THREAD_SAFETY_ANALYSIS;

    /**
     * @brief Recursively collect all terminal key strings under a node.
     *
     * @param node         Current node being visited.
     * @param accumulated  Key string built so far (concatenation of edge_labels
     *                     from root to current node).
     * @param out          Output vector to append found keys to.
     */
    void dfs_collect_keys(const std::shared_ptr<RadixNode>& node,
                          const std::string& accumulated,
                          std::vector<std::string>& out) const;

    /**
     * @brief Recursively collect all terminal entries (key + leaf data).
     *
     * @param node         Current node being visited.
     * @param accumulated  Key string built so far (concatenation of edge_labels
     *                     from root to current node).
     * @param out          Output vector to append (key, TrieLeafData) pairs to.
     */
    void dfs_collect_entries(
        const std::shared_ptr<RadixNode>& node, const std::string& accumulated,
        std::vector<std::pair<std::string, TrieLeafData>>& out) const;

    /**
     * @brief Insert implementation without locking (caller must hold
     *        exclusive lock).
     *
     * @param key        The full key to insert.
     * @param size       Object size in bytes.
     * @param client_id  Client UUID.
     */
    void insert_locked(const std::string& key, uint64_t size,
                       const UUID& client_id) NO_THREAD_SAFETY_ANALYSIS;

    /**
     * @brief Remove implementation without locking (caller must hold
     *        exclusive lock).
     *
     * @param key  The full key to remove.
     */
    void remove_locked(const std::string& key) NO_THREAD_SAFETY_ANALYSIS;

    /**
     * @brief Walk upward from a node deleting empty childless nodes.
     *
     * No merge of single-child nodes — this keeps the logic simple and
     * future-compatible with fine-grained (hand-over-hand) locking,
     * where merge requires locking parent + node + child simultaneously.
     * The trie may remain slightly less compressed, but correctness
     * and traversal are unaffected.
     *
     * @param node  The node to start cleanup from (the node whose leaf_data
     *              was just cleared).
     */
    void cascade_cleanup(std::shared_ptr<RadixNode> node)
        NO_THREAD_SAFETY_ANALYSIS;

    /**
     * @brief Add count and bytes to every ancestor from node up to root.
     *
     * All arithmetic is unsigned — no signed-to-unsigned casts needed.
     *
     * @param node   The node to start propagation from (inclusive).
     * @param count  Number of keys added (typically 0 or 1).
     * @param bytes  Total bytes added.
     */
    void increment_ancestors(std::shared_ptr<RadixNode> node, size_t count,
                             uint64_t bytes) NO_THREAD_SAFETY_ANALYSIS;

    /**
     * @brief Subtract count and bytes from every ancestor from node up
     *        to root.
     *
     * Asserts that each node's counters are large enough before
     * subtracting. A firing assert indicates a double-remove or
     * mismatched size bug.
     *
     * @param node   The node to start propagation from (inclusive).
     * @param count  Number of keys removed (typically 0 or 1).
     * @param bytes  Total bytes removed.
     */
    void decrement_ancestors(std::shared_ptr<RadixNode> node, size_t count,
                             uint64_t bytes) NO_THREAD_SAFETY_ANALYSIS;
};

}  // namespace mooncake
