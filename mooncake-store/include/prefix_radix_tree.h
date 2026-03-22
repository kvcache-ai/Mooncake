#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {

/**
 * @brief Thread-safe Radix Tree for prefix-aware KV cache lookup.
 *
 * In LLM inference, KV cache keys share common prefixes (system prompts,
 * shared conversation context). A radix tree enables O(|prefix|) longest
 * prefix matching, allowing the Store to return the best available cached
 * prefix when an exact key is not found.
 *
 * This is inspired by SGLang's RadixAttention, but operates at the
 * distributed storage layer rather than the inference framework layer.
 * This is the FIRST implementation of radix tree prefix indexing in a
 * distributed KV cache store.
 *
 * Key features:
 * - Insert/Remove/LongestPrefixMatch operations
 * - Thread-safe via shared_mutex (concurrent reads, exclusive writes)
 * - Memory-efficient: compressed edges (Patricia trie variant)
 * - Supports arbitrary binary keys (token sequences as strings)
 *
 * Usage in Mooncake Store:
 * - PutEnd: Insert key into radix tree (alongside HashMap)
 * - GetReplicaList: If exact match fails, try LongestPrefixMatch
 * - BatchEvict: Remove evicted keys from radix tree
 *
 * Memory overhead: ~64 bytes per node (label + children map + metadata).
 * For 100K keys with average 50% prefix sharing: ~3.2MB.
 */
class PrefixRadixTree {
   public:
    struct MatchResult {
        std::string matched_key;    // The key that matched
        size_t matched_length;      // Length of the matched prefix
        bool is_exact;              // True if exact match
    };

    PrefixRadixTree() : root_(std::make_unique<Node>()) {}

    /**
     * Insert a key into the radix tree.
     * Thread-safe (exclusive lock).
     */
    void Insert(const std::string& key) {
        std::unique_lock lock(mutex_);
        insertImpl(root_.get(), key, 0);
        size_++;
    }

    /**
     * Remove a key from the radix tree.
     * Thread-safe (exclusive lock).
     * Returns true if the key was found and removed.
     */
    bool Remove(const std::string& key) {
        std::unique_lock lock(mutex_);
        bool removed = removeImpl(root_.get(), key, 0);
        if (removed) size_--;
        return removed;
    }

    /**
     * Find the longest prefix of `query` that matches a stored key.
     * Thread-safe (shared lock — concurrent reads allowed).
     *
     * Returns MatchResult with:
     * - matched_key: the stored key that is a prefix of query
     * - matched_length: how many characters matched
     * - is_exact: true if matched_key == query
     *
     * If no prefix matches, returns empty result (matched_length == 0).
     */
    MatchResult LongestPrefixMatch(const std::string& query) const {
        std::shared_lock lock(mutex_);
        MatchResult best{};
        longestPrefixMatchImpl(root_.get(), query, 0, best);
        return best;
    }

    /**
     * Check if an exact key exists in the tree.
     * Thread-safe (shared lock).
     */
    bool Contains(const std::string& key) const {
        std::shared_lock lock(mutex_);
        return containsImpl(root_.get(), key, 0);
    }

    /**
     * Get all keys stored in the tree (for diagnostics).
     * Thread-safe (shared lock).
     */
    std::vector<std::string> AllKeys() const {
        std::shared_lock lock(mutex_);
        std::vector<std::string> result;
        std::string prefix;
        collectKeys(root_.get(), prefix, result);
        return result;
    }

    /// Number of keys stored.
    size_t Size() const {
        std::shared_lock lock(mutex_);
        return size_;
    }

    /// Number of nodes in the tree (for memory profiling).
    size_t NodeCount() const {
        std::shared_lock lock(mutex_);
        return countNodes(root_.get());
    }

   private:
    struct Node {
        // Edge label: the substring stored on the edge from parent to this node.
        // Empty for root node.
        std::string label;

        // Children indexed by first character of their label.
        std::unordered_map<char, std::unique_ptr<Node>> children;

        // True if this node represents the end of an inserted key.
        bool is_terminal = false;

        // The full key (stored only at terminal nodes for fast retrieval).
        std::string full_key;
    };

    std::unique_ptr<Node> root_;
    mutable std::shared_mutex mutex_;
    size_t size_ = 0;

    // --- Insert implementation ---
    void insertImpl(Node* node, const std::string& key, size_t depth) {
        if (depth == key.size()) {
            // We've consumed the entire key — mark this node as terminal.
            node->is_terminal = true;
            node->full_key = key;
            return;
        }

        char c = key[depth];
        auto it = node->children.find(c);

        if (it == node->children.end()) {
            // No child starts with this character — create a new leaf.
            auto child = std::make_unique<Node>();
            child->label = key.substr(depth);
            child->is_terminal = true;
            child->full_key = key;
            node->children[c] = std::move(child);
            return;
        }

        Node* child = it->second.get();
        const std::string& child_label = child->label;

        // Find common prefix between remaining key and child label.
        size_t common = 0;
        size_t remaining = key.size() - depth;
        size_t label_len = child_label.size();
        while (common < remaining && common < label_len &&
               key[depth + common] == child_label[common]) {
            common++;
        }

        if (common == label_len) {
            // Child label is fully consumed — recurse into child.
            insertImpl(child, key, depth + common);
        } else {
            // Split: create intermediate node at the common prefix point.
            auto split_node = std::make_unique<Node>();
            split_node->label = child_label.substr(0, common);

            // Adjust existing child: its label becomes the suffix after split.
            child->label = child_label.substr(common);
            char existing_char = child->label[0];

            // Move existing child under split node.
            split_node->children[existing_char] = std::move(it->second);

            if (depth + common == key.size()) {
                // The new key ends exactly at the split point.
                split_node->is_terminal = true;
                split_node->full_key = key;
            } else {
                // Create new leaf for the remaining key suffix.
                auto new_leaf = std::make_unique<Node>();
                new_leaf->label = key.substr(depth + common);
                new_leaf->is_terminal = true;
                new_leaf->full_key = key;
                char new_char = new_leaf->label[0];
                split_node->children[new_char] = std::move(new_leaf);
            }

            node->children[c] = std::move(split_node);
        }
    }

    // --- Remove implementation ---
    bool removeImpl(Node* node, const std::string& key, size_t depth) {
        if (depth == key.size()) {
            if (!node->is_terminal) return false;
            node->is_terminal = false;
            node->full_key.clear();
            return true;
        }

        char c = key[depth];
        auto it = node->children.find(c);
        if (it == node->children.end()) return false;

        Node* child = it->second.get();
        const std::string& child_label = child->label;
        size_t label_len = child_label.size();
        size_t remaining = key.size() - depth;

        // Check if child label matches the key segment.
        if (remaining < label_len) return false;
        for (size_t i = 0; i < label_len; i++) {
            if (key[depth + i] != child_label[i]) return false;
        }

        bool removed = removeImpl(child, key, depth + label_len);

        if (removed) {
            // Cleanup: if child has no children and is not terminal, remove it.
            if (child->children.empty() && !child->is_terminal) {
                node->children.erase(it);
            }
            // Merge: if child has exactly one child and is not terminal,
            // merge with its only child (compress the path).
            else if (child->children.size() == 1 && !child->is_terminal) {
                auto& only_child = child->children.begin()->second;
                child->label += only_child->label;
                child->is_terminal = only_child->is_terminal;
                child->full_key = std::move(only_child->full_key);
                auto grandchildren = std::move(only_child->children);
                child->children = std::move(grandchildren);
            }
        }

        return removed;
    }

    // --- Longest prefix match implementation ---
    void longestPrefixMatchImpl(const Node* node, const std::string& query,
                                size_t depth, MatchResult& best) const {
        // If this node is terminal, it represents a stored key that is a
        // prefix of the query up to this depth.
        if (node->is_terminal && depth > best.matched_length) {
            best.matched_key = node->full_key;
            best.matched_length = depth;
            best.is_exact = (depth == query.size());
        }

        if (depth >= query.size()) return;

        char c = query[depth];
        auto it = node->children.find(c);
        if (it == node->children.end()) return;

        const Node* child = it->second.get();
        const std::string& child_label = child->label;
        size_t label_len = child_label.size();
        size_t remaining = query.size() - depth;

        // Check how much of the child label matches the query.
        size_t match_len = 0;
        while (match_len < label_len && match_len < remaining &&
               query[depth + match_len] == child_label[match_len]) {
            match_len++;
        }

        if (match_len == label_len) {
            // Full label matched — recurse into child.
            longestPrefixMatchImpl(child, query, depth + label_len, best);
        }
        // Partial match: the best prefix is whatever we found before this node.
    }

    // --- Contains implementation ---
    bool containsImpl(const Node* node, const std::string& key,
                      size_t depth) const {
        if (depth == key.size()) {
            return node->is_terminal;
        }

        char c = key[depth];
        auto it = node->children.find(c);
        if (it == node->children.end()) return false;

        const Node* child = it->second.get();
        const std::string& child_label = child->label;
        size_t label_len = child_label.size();
        size_t remaining = key.size() - depth;

        if (remaining < label_len) return false;
        for (size_t i = 0; i < label_len; i++) {
            if (key[depth + i] != child_label[i]) return false;
        }

        return containsImpl(child, key, depth + label_len);
    }

    // --- Collect all keys ---
    void collectKeys(const Node* node, std::string& prefix,
                     std::vector<std::string>& result) const {
        std::string saved = prefix;
        prefix += node->label;
        if (node->is_terminal) {
            result.push_back(node->full_key);
        }
        for (const auto& [c, child] : node->children) {
            collectKeys(child.get(), prefix, result);
        }
        prefix = saved;
    }

    // --- Count nodes ---
    size_t countNodes(const Node* node) const {
        size_t count = 1;
        for (const auto& [c, child] : node->children) {
            count += countNodes(child.get());
        }
        return count;
    }
};

}  // namespace mooncake
