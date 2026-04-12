#include "prefix_index.h"

#include <algorithm>
#include <cassert>

namespace mooncake {

PrefixIndex::PrefixIndex() : root_(std::make_shared<RadixNode>()) {}

void PrefixIndex::insert(const std::string& key, uint64_t size,
                         const UUID& client_id) {
    if (key.empty()) return;
    SharedMutexLocker lock(&trie_mutex_);
    insert_locked(key, size, client_id);
}

void PrefixIndex::remove(const std::string& key) {
    if (key.empty()) return;
    SharedMutexLocker lock(&trie_mutex_);
    remove_locked(key);
}

void PrefixIndex::clear() {
    SharedMutexLocker lock(&trie_mutex_);
    // Discard entire tree; old nodes are freed when last shared_ptr drops.
    root_ = std::make_shared<RadixNode>();
}

std::vector<std::string> PrefixIndex::list_keys_by_prefix(
    const std::string& prefix) const {
    SharedMutexLocker lock(&trie_mutex_, shared_lock);
    std::vector<std::string> result;

    if (prefix.empty()) {
        dfs_collect_keys(root_, "", result);
        return result;
    }

    // find_prefix_node may land mid-edge (e.g. prefix="mistral" lands
    // on node with edge="mistral-7b/..."). node_path is the full
    // concatenation of edge_labels from root to that node, which may
    // be longer than prefix. We must use node_path (not prefix) as
    // the DFS accumulator so emitted keys are reconstructed correctly.
    std::string node_path;
    auto node = find_prefix_node(prefix, &node_path);
    if (!node) return result;
    dfs_collect_keys(node, node_path, result);
    return result;
}

std::vector<std::pair<std::string, TrieLeafData>>
PrefixIndex::list_entries_by_prefix(const std::string& prefix) const {
    SharedMutexLocker lock(&trie_mutex_, shared_lock);
    std::vector<std::pair<std::string, TrieLeafData>> result;

    if (prefix.empty()) {
        dfs_collect_entries(root_, "", result);
        return result;
    }

    std::string node_path;
    auto node = find_prefix_node(prefix, &node_path);
    if (!node) return result;
    dfs_collect_entries(node, node_path, result);
    return result;
}

std::vector<std::string> PrefixIndex::list_prefix_continuations(
    const std::string& prefix) const {
    SharedMutexLocker lock(&trie_mutex_, shared_lock);

    std::shared_ptr<RadixNode> node;
    std::string unmatched_suffix;

    if (prefix.empty()) {
        node = root_;
    } else {
        std::string node_path;
        node = find_prefix_node(prefix, &node_path);
        if (!node) return {};
        // When the prefix ends mid-edge (e.g. prefix="ab", node edge=
        // "abc/"), node_path is "abc/" and the unmatched suffix is "c/".
        // We prepend this suffix to each child's edge_label so results
        // are relative to the user's prefix, not the internal node.
        if (node_path.size() > prefix.size()) {
            unmatched_suffix = node_path.substr(prefix.size());
        }
    }
    if (!node) return {};

    std::vector<std::string> result;

    // When the prefix lands mid-edge on a terminal node, the unmatched
    // suffix itself is a continuation (it represents a stored key that
    // extends the user's prefix). For example, with only "abc" stored,
    // list_prefix_continuations("ab") should return ["c"] because the key "abc"
    // continues the prefix "ab" by "c".
    if (!unmatched_suffix.empty() && node->is_terminal()) {
        result.push_back(unmatched_suffix);
    }

    result.reserve(result.size() + node->children.size());
    for (const auto& [ch, child] : node->children) {
        result.push_back(unmatched_suffix + child->edge_label);
    }
    return result;
}

size_t PrefixIndex::count_by_prefix(const std::string& prefix) const {
    SharedMutexLocker lock(&trie_mutex_, shared_lock);

    // O(1) read from root for the empty-prefix case.
    if (prefix.empty()) return root_->subtree_count;

    auto node = find_prefix_node(prefix);
    if (!node) return 0;
    // subtree_count is maintained incrementally by insert/remove,
    // so this is O(prefix_length) for traversal, O(1) for the read.
    return node->subtree_count;
}

uint64_t PrefixIndex::bytes_by_prefix(const std::string& prefix) const {
    SharedMutexLocker lock(&trie_mutex_, shared_lock);

    if (prefix.empty()) return root_->subtree_bytes;

    auto node = find_prefix_node(prefix);
    if (!node) return 0;
    return node->subtree_bytes;
}

size_t PrefixIndex::size() const {
    SharedMutexLocker lock(&trie_mutex_, shared_lock);
    return root_->subtree_count;
}

std::shared_ptr<RadixNode> PrefixIndex::find_prefix_node(
    const std::string& prefix, std::string* node_path) const {
    auto current = root_;
    size_t pos = 0;
    // Tracks the full concatenation of edge_labels from root to the
    // current node. Needed because the prefix may end mid-edge, and
    // callers (list_keys_by_prefix) need the true path, not the
    // truncated user prefix.
    std::string path;

    while (pos < prefix.size()) {
        // Radix tree invariant: no two children share the same first
        // character, so lookup by prefix[pos] is unambiguous.
        char next_char = prefix[pos];
        auto it = current->children.find(next_char);
        if (it == current->children.end()) return nullptr;

        auto& child = it->second;
        const auto& label = child->edge_label;

        // Compare as many characters as both the edge and the remaining
        // prefix have in common.
        size_t remaining = prefix.size() - pos;
        size_t match_len = std::min(label.size(), remaining);

        for (size_t i = 0; i < match_len; ++i) {
            if (label[i] != prefix[pos + i]) {
                // Mismatch inside the edge — no node for this prefix.
                return nullptr;
            }
        }

        if (remaining <= label.size()) {
            // Prefix is fully consumed within (or exactly at the end of)
            // this edge. The child is the root of the prefix subtree.
            // node_path includes the FULL edge_label, not just the
            // matched portion — all keys below this node share it.
            if (node_path) *node_path = path + label;
            return child;
        }

        // Edge fully matched but prefix has more characters — descend.
        path += label;
        pos += label.size();
        current = child;
    }

    if (node_path) *node_path = path;
    return current;
}

void PrefixIndex::dfs_collect_keys(const std::shared_ptr<RadixNode>& node,
                                   const std::string& accumulated,
                                   std::vector<std::string>& out) const {
    if (node->is_terminal()) {
        out.push_back(accumulated);
    }
    for (const auto& [ch, child] : node->children) {
        dfs_collect_keys(child, accumulated + child->edge_label, out);
    }
}

void PrefixIndex::dfs_collect_entries(
    const std::shared_ptr<RadixNode>& node, const std::string& accumulated,
    std::vector<std::pair<std::string, TrieLeafData>>& out) const {
    if (node->is_terminal()) {
        out.emplace_back(accumulated, node->leaf_data.value());
    }
    for (const auto& [ch, child] : node->children) {
        dfs_collect_entries(child, accumulated + child->edge_label, out);
    }
}

void PrefixIndex::increment_ancestors(std::shared_ptr<RadixNode> node,
                                      size_t count, uint64_t bytes) {
    while (node) {
        node->subtree_count += count;
        node->subtree_bytes += bytes;
        node = node->parent.lock();
    }
}

void PrefixIndex::decrement_ancestors(std::shared_ptr<RadixNode> node,
                                      size_t count, uint64_t bytes) {
    while (node) {
        assert(node->subtree_count >= count);
        assert(node->subtree_bytes >= bytes);
        node->subtree_count -= count;
        node->subtree_bytes -= bytes;
        node = node->parent.lock();
    }
}

void PrefixIndex::insert_locked(const std::string& key, uint64_t size,
                                const UUID& client_id) {
    auto current = root_;
    size_t pos = 0;  // characters of `key` consumed so far

    while (pos < key.size()) {
        char next_char = key[pos];
        auto it = current->children.find(next_char);

        if (it == current->children.end()) {
            // --- Case C: no child starts with this character ---
            // Create a new leaf whose edge_label is the entire remaining
            // key suffix. This is the common fast path for disjoint keys.
            auto leaf = std::make_shared<RadixNode>();
            leaf->edge_label = key.substr(pos);
            leaf->leaf_data = TrieLeafData{size, client_id};
            leaf->subtree_count = 1;
            leaf->subtree_bytes = size;
            leaf->parent = current;
            current->children[next_char] = leaf;
            increment_ancestors(current, 1, size);
            return;
        }

        auto& child = it->second;
        const auto& label = child->edge_label;
        size_t remaining = key.size() - pos;

        // Count how many characters the edge and remaining key share.
        size_t common = 0;
        size_t limit = std::min(label.size(), remaining);
        while (common < limit && label[common] == key[pos + common]) {
            ++common;
        }

        if (common == label.size()) {
            if (common == remaining) {
                // Key ends exactly at this existing node.
                if (child->is_terminal()) {
                    // Key already exists — update leaf_data to reflect
                    // current ownership. client_id can change when a
                    // different tenant re-registers the same key.
                    uint64_t old_size = child->leaf_data->size;
                    child->leaf_data = TrieLeafData{size, client_id};
                    if (size > old_size) {
                        increment_ancestors(child, 0, size - old_size);
                    } else if (size < old_size) {
                        decrement_ancestors(child, 0, old_size - size);
                    }
                    return;
                }
                // Node exists as internal; promote it to terminal.
                child->leaf_data = TrieLeafData{size, client_id};
                increment_ancestors(child, 1, size);
                return;
            }
            // --- Case A: full edge consumed, key continues ---
            // Advance past this edge and keep walking.
            pos += label.size();
            current = child;
            continue;
        }

        // --- Case B: partial edge match — split required ---
        //
        // The edge and the remaining key diverge at position `common`.
        // We must split the existing edge into a shared prefix (mid)
        // and the diverging suffix (child keeps the tail).

        // Create the intermediate node for the shared prefix.
        auto mid = std::make_shared<RadixNode>();
        mid->edge_label = label.substr(0, common);
        mid->parent = current;
        // mid inherits the existing subtree stats — the original child's
        // entire subtree still hangs below mid.
        mid->subtree_count = child->subtree_count;
        mid->subtree_bytes = child->subtree_bytes;

        // Shrink the original child's edge to only the diverging suffix.
        child->edge_label = label.substr(common);
        child->parent = mid;
        mid->children[child->edge_label[0]] = child;

        if (common == remaining) {
            // The new key ends exactly at the split point — mid itself
            // becomes the terminal node. No separate leaf needed.
            //
            // Example: existing edge "abcdef", insert key "abc"
            //   Before: current --> child("abcdef")
            //   After:  current --> mid("abc", terminal)
            //                         └── child("def")
            mid->leaf_data = TrieLeafData{size, client_id};
        } else {
            // The new key diverges — create a separate leaf for the
            // remaining suffix after the split point.
            //
            // Example: existing edge "abcdef", insert key "abcxyz"
            //   Before: current --> child("abcdef")
            //   After:  current --> mid("abc")
            //                         ├── child("def")
            //                         └── leaf("xyz", terminal)
            auto leaf = std::make_shared<RadixNode>();
            leaf->edge_label = key.substr(pos + common);
            leaf->leaf_data = TrieLeafData{size, client_id};
            leaf->subtree_count = 1;
            leaf->subtree_bytes = size;
            leaf->parent = mid;
            mid->children[leaf->edge_label[0]] = leaf;
        }

        // Swap mid into parent's children map (replaces old child entry).
        current->children[mid->edge_label[0]] = mid;

        // Propagate the new terminal's stats from mid upward to root.
        increment_ancestors(mid, 1, size);
        return;
    }

}

void PrefixIndex::remove_locked(const std::string& key) {
    auto current = root_;
    size_t pos = 0;

    while (pos < key.size()) {
        char next_char = key[pos];
        auto it = current->children.find(next_char);
        if (it == current->children.end()) return;  // key not in trie

        auto& child = it->second;
        const auto& label = child->edge_label;
        size_t remaining = key.size() - pos;

        // Key is shorter than this edge — can't match any stored key.
        if (remaining < label.size()) return;

        // Verify the edge label matches the corresponding key segment.
        if (key.compare(pos, label.size(), label) != 0) return;

        if (label.size() == remaining) {
            // Reached the node for this key.
            if (!child->is_terminal()) return;  // internal-only node

            uint64_t removed_size = child->leaf_data->size;
            // Clear terminal status — node becomes internal-only.
            child->leaf_data.reset();
            decrement_ancestors(child, 1, removed_size);
            // Clean up any now-empty childless nodes upward.
            cascade_cleanup(child);
            return;
        }

        // Edge fully matched but key has more characters — descend.
        pos += label.size();
        current = child;
    }

}

void PrefixIndex::cascade_cleanup(std::shared_ptr<RadixNode> node) {
    // Walk upward, deleting nodes that are both non-terminal and
    // childless. Stop as soon as a node is useful (has children or
    // leaf_data). No single-child merge — intentionally omitted so
    // future fine-grained locking only needs parent → delete-child,
    // not the three-node restructure that merge requires.
    while (node && !node->parent.expired()) {
        if (node->is_terminal() || !node->children.empty()) break;

        auto parent = node->parent.lock();
        if (!parent) break;

        // Remove this empty node from parent's children map.
        parent->children.erase(node->edge_label[0]);
        // Continue checking parent — it may now be empty too.
        node = parent;
    }
}

}  // namespace mooncake
