#pragma once

// Known defects are preserved intentionally and marked "BUG:".
// See docs/KNOWN_ISSUES.md for the full list and fix guidance.

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace conductor {
namespace prefixindex {

struct ModelContext {
    std::string model_name;
    std::string lora_name;  // empty represents no LoRA adapter
    int64_t block_size = 0;
    // TODO @yejj710
    // Confirm the difference between the previously discussed cache_salt
    // and additionalSalt. The current understanding is that cache_salt is
    // used to ensure data isolation between different customers, and it
    // seems it can be directly added to additionalSalt.
    std::string additional_salt;
    std::string tenant_id;
    std::string instance_id;  // unique identifier for each API server

    // All six fields participate — a missed field here would cause
    // silent cross-context cache misses.
    bool operator==(const ModelContext& other) const = default;
};

}  // namespace prefixindex
}  // namespace conductor

template <>
struct std::hash<conductor::prefixindex::ModelContext> {
    size_t operator()(
        const conductor::prefixindex::ModelContext& ctx) const noexcept {
        // Field-by-field hash-combine (boost::hash_combine recipe) over all
        // six fields.
        size_t seed = 0;
        auto combine = [&seed](size_t h) {
            seed ^= h + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<std::string>{}(ctx.model_name));
        combine(std::hash<std::string>{}(ctx.lora_name));
        combine(std::hash<int64_t>{}(ctx.block_size));
        combine(std::hash<std::string>{}(ctx.additional_salt));
        combine(std::hash<std::string>{}(ctx.tenant_id));
        combine(std::hash<std::string>{}(ctx.instance_id));
        return seed;
    }
};

namespace conductor {
namespace prefixindex {

struct CacheStoreInfo {
    // TODO Currently, the KV cache at different
    // levels is not distinguished. In the future, the caches of Mooncake
    // and inference engines (vLLM, SGLang) should be handled separately.
    std::unordered_map<std::string, int64_t> engine_last_access_time;
    int64_t total_replica_nums = 0;
    std::set<std::string> medium_set;
    std::set<int64_t> dp_rank_set;  // dp_ranks the block is cached on
};

struct HashMapStore {
    // conductor prefixHash -> cachestore
    std::unordered_map<uint64_t, std::unique_ptr<CacheStoreInfo>> prefix_map;
    std::atomic<int64_t> last_access{0};
    int64_t total_prefixes = 0;
};

struct ContextData {
    // Lock order: hashmap_mu -> prefix_mu.
    // ProcessStoreEvent/ProcessRemoveEvent take hashmap_mu first and then
    // prefix_mu while still holding hashmap_mu; never acquire in the
    // opposite order.
    std::shared_mutex prefix_mu;
    std::shared_mutex hashmap_mu;

    HashMapStore prefix_store;
    uint64_t seed = 0;  // XXH64(additional_salt, 0)

    std::set<int64_t> dp_size;

    // engine block hash -> conductor prefix hash
    std::unordered_map<uint64_t, uint64_t> proxy_hash_mapping;
};

struct CacheHitResult {
    int64_t longest_match_tokens = 0;  // JSON: longest_matched
    std::map<int64_t, int64_t> dp;     // JSON: DP (string keys)
    int64_t gpu = 0;                   // JSON: GPU
    int64_t cpu = 0;                   // JSON: CPU
    int64_t disk = 0;                  // JSON: DISK
};

struct ModelContextView {
    std::string model_name;
    std::string lora_name;
    int64_t block_size = 0;
    std::string additional_salt;
    std::string tenant_id;
    std::string instance_id;
};

struct GlobalView {
    int32_t context_count = 0;
    std::vector<ModelContextView> model_contexts;
    std::vector<std::unordered_map<uint64_t, uint64_t>> proxy_hash_map;
};

// computeHash building block, exposed for golden-vector tests:
// XXH64(seed=0) over parent_hash (8B little-endian) then each token id
// (4B little-endian). Hash byte layout is a fixed wire contract.
uint64_t ComputeBlockHash(uint64_t parent_hash, const int32_t* token_ids,
                          size_t token_count);

// XXH64 of a UTF-8 string with seed 0, matching xxhash.Sum64String.
uint64_t Sum64String(const std::string& s);

// Forward declarations for events (defined in conductor/common/types.h).
}  // namespace prefixindex
}  // namespace conductor

#include "conductor/common/types.h"

namespace conductor {
namespace prefixindex {

class PrefixCacheTable {
   public:
    PrefixCacheTable() = default;
    PrefixCacheTable(const PrefixCacheTable&) = delete;
    PrefixCacheTable& operator=(const PrefixCacheTable&) = delete;

    void AddDpSize(const ModelContext& model_context, int64_t dp_rank);

    // Returns the prefix hash chain for token_ids. cache_salt seeds the
    // chain (separates hashes from different customers). BlockSize <= 0
    // logs a warning and returns an empty vector.
    std::vector<uint64_t> ComputePrefixHash(
        const ModelContext& model_context,
        const std::vector<int32_t>& token_ids, uint64_t cache_salt);

    CacheHitResult CacheHitCompute(const ModelContext& model_context,
                                   const std::vector<int32_t>& token_ids);

    // Returns empty string on success, error message otherwise.
    std::string ProcessStoreEvent(const common::StoredEvent& event,
                                  int64_t dp_rank);

    std::string ProcessRemoveEvent(const common::RemovedEvent& event,
                                   int64_t dp_rank,
                                   const std::string& instance_id);

    GlobalView GetGlobalView();

   private:
    friend class PrefixCacheTableTestPeer;

    std::shared_ptr<ContextData> GetContextData(
        const ModelContext& model_context);

    // Looks up context without creating it; nullptr when absent.
    std::shared_ptr<ContextData> LoadContextData(
        const ModelContext& model_context);

    void AddNewPrefixStore(HashMapStore* prefix_store, uint64_t hash_value,
                           const std::string& instance_id,
                           const std::string& medium, int64_t dp_rank);

    // ModelContext -> ContextData, guarded by context_map_mu_.
    std::shared_mutex context_map_mu_;
    std::unordered_map<ModelContext, std::shared_ptr<ContextData>> context_map_;

    std::atomic<int32_t> context_count_{0};
};

}  // namespace prefixindex
}  // namespace conductor
