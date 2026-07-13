// Known defects are preserved intentionally and marked with BUG:
// comments. See docs/KNOWN_ISSUES.md.

#include "conductor/prefixindex/prefix_indexer.h"

#include <glog/logging.h>

#define XXH_INLINE_ALL
#include <xxhash.h>

#include <chrono>
#include <cstring>
#include <limits>
#include <optional>
#include <sstream>
#include <utility>

namespace conductor {
namespace prefixindex {

namespace {

int64_t NowUnixSeconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::optional<size_t> SafeExpectedTokenCount(const common::StoredEvent& event) {
    if (event.block_size <= 0 || !std::in_range<size_t>(event.block_size)) {
        return std::nullopt;
    }

    const size_t block_size = static_cast<size_t>(event.block_size);
    if (event.block_hashes.size() >
        std::numeric_limits<size_t>::max() / block_size) {
        return std::nullopt;
    }
    return event.block_hashes.size() * block_size;
}

std::string StoreEventLayoutError(
    const char* reason, const common::StoredEvent& event,
    std::optional<size_t> expected_token_count = std::nullopt) {
    std::ostringstream error;
    error << "StoredEvent layout invalid: " << reason
          << " hash_count=" << event.block_hashes.size()
          << " block_size=" << event.block_size;
    if (expected_token_count.has_value()) {
        error << " expected=" << *expected_token_count;
    }
    error << " actual=" << event.token_ids.size();
    return error.str();
}

}  // namespace

uint64_t Sum64String(const std::string& s) {
    return XXH64(s.data(), s.size(), 0);
}

uint64_t ComputeBlockHash(uint64_t parent_hash, const int32_t* token_ids,
                          size_t token_count) {
    // XXH64 streaming with seed 0 over a fixed byte sequence (wire
    // contract): parent hash as 8 bytes little-endian, then each token
    // id as 4 bytes little-endian (int32 -> uint32 two's-complement).
    XXH64_state_t state;
    XXH64_reset(&state, 0);

    unsigned char parent_bytes[8];
    for (int i = 0; i < 8; ++i) {
        parent_bytes[i] =
            static_cast<unsigned char>((parent_hash >> (8 * i)) & 0xFF);
    }
    XXH64_update(&state, parent_bytes, sizeof(parent_bytes));

    unsigned char token_bytes[4];
    for (size_t t = 0; t < token_count; ++t) {
        const uint32_t v = static_cast<uint32_t>(token_ids[t]);
        for (int i = 0; i < 4; ++i) {
            token_bytes[i] = static_cast<unsigned char>((v >> (8 * i)) & 0xFF);
        }
        XXH64_update(&state, token_bytes, sizeof(token_bytes));
    }
    return XXH64_digest(&state);
}

std::shared_ptr<ContextData> PrefixCacheTable::LoadContextData(
    const ModelContext& model_context) {
    std::shared_lock lock(context_map_mu_);
    auto it = context_map_.find(model_context);
    if (it == context_map_.end()) {
        return nullptr;
    }
    return it->second;
}

std::shared_ptr<ContextData> PrefixCacheTable::GetContextData(
    const ModelContext& model_context) {
    // Fast path: already exists in the context map.
    if (auto existing = LoadContextData(model_context)) {
        return existing;
    }

    auto new_context_data = std::make_shared<ContextData>();
    new_context_data->seed = Sum64String(model_context.additional_salt);
    new_context_data->prefix_store.last_access.store(NowUnixSeconds());

    // Concurrent insert: another thread may have beaten us to the map.
    std::unique_lock lock(context_map_mu_);
    auto [it, inserted] =
        context_map_.try_emplace(model_context, new_context_data);
    VLOG(1) << "in getContextData modelcontext model="
            << model_context.model_name
            << " instance=" << model_context.instance_id;
    return inserted ? new_context_data : it->second;
}

void PrefixCacheTable::AddDpSize(const ModelContext& model_context,
                                 int64_t dp_rank) {
    auto context_data = GetContextData(model_context);
    // NOTE: DpSize mutated without lock — single-writer, safe by construction.
    std::unique_lock lock(context_data->hashmap_mu);
    context_data->dp_size.insert(dp_rank);
}

std::vector<uint64_t> PrefixCacheTable::ComputePrefixHash(
    const ModelContext& model_context, const std::vector<int32_t>& token_ids,
    uint64_t cache_salt) {
    // cache_salt is used to separate hash from different customers
    if (model_context.block_size <= 0) {
        LOG(WARNING) << "ComputePrefixHash: BlockSize must be greater than "
                        "zero blockSize="
                     << model_context.block_size;
        return {};
    }
    const size_t block_size = static_cast<size_t>(model_context.block_size);
    const size_t num_blocks = token_ids.size() / block_size;

    std::vector<uint64_t> prefix_hashes;
    prefix_hashes.reserve(num_blocks);

    uint64_t parent_hash = cache_salt;
    for (size_t i = 0; i < num_blocks; ++i) {
        const size_t start = i * block_size;
        const uint64_t hash_value =
            ComputeBlockHash(parent_hash, token_ids.data() + start, block_size);
        prefix_hashes.push_back(hash_value);
        parent_hash = hash_value;
    }
    return prefix_hashes;
}

CacheHitResult PrefixCacheTable::CacheHitCompute(
    const ModelContext& model_context, const std::vector<int32_t>& token_ids) {
    CacheHitResult prefix_match_result;

    auto context_data = LoadContextData(model_context);
    if (!context_data) {
        LOG(ERROR) << "In CacheHitCompute, contextData not found";
        return prefix_match_result;
    }

    const uint64_t cache_salt = Sum64String(model_context.additional_salt);

    auto prefix_hashes =
        ComputePrefixHash(model_context, token_ids, cache_salt);
    if (prefix_hashes.empty() && model_context.block_size <= 0) {
        // Only an invalid BlockSize hits this error path;
        // an empty-but-valid chain proceeds and naturally matches nothing.
        LOG(ERROR) << "CacheHitCompute: ComputePrefixHash returned nil, "
                      "likely due to invalid BlockSize modelName="
                   << model_context.model_name
                   << " instanceID=" << model_context.instance_id
                   << " blockSize=" << model_context.block_size;
        return prefix_match_result;
    }

    // TODO @yejj710:
    // When there is no data in contextData, what information should be
    // returned for the matched modelcontext. This is related to `AddDpSize`.

    std::shared_lock prefix_lock(context_data->prefix_mu);
    HashMapStore& prefix_store = context_data->prefix_store;

    for (const uint64_t prefix_hash : prefix_hashes) {
        auto it = prefix_store.prefix_map.find(prefix_hash);
        if (it == prefix_store.prefix_map.end() ||
            it->second->total_replica_nums == 0) {
            break;
        }
        const CacheStoreInfo& cache_store_info = *it->second;

        bool cache_hit = false;
        for (const std::string& key : cache_store_info.medium_set) {
            // BUG: medium matching only recognises the exact literals "cpu"
            // and "GPU" (inconsistent casing); any other value — including
            // the empty string that Mooncake-source and nil-medium vLLM
            // blocks carry — logs a warning and does not count as a hit.
            // The DISK field has no assignment path and stays 0 forever.
            // See docs/KNOWN_ISSUES.md issue 1.
            if (key == "cpu") {
                prefix_match_result.cpu += model_context.block_size;
                cache_hit = true;
            } else if (key == "GPU") {
                prefix_match_result.gpu += model_context.block_size;
                cache_hit = true;
            } else {
                LOG(WARNING)
                    << "In CacheHitCompute, unknown medium type medium=" << key;
            }
        }
        if (cache_hit) {
            prefix_match_result.longest_match_tokens +=
                model_context.block_size;
            for (const int64_t dp_rank : cache_store_info.dp_rank_set) {
                prefix_match_result.dp[dp_rank] += model_context.block_size;
            }
        }
    }

    prefix_store.last_access.store(NowUnixSeconds());

    return prefix_match_result;
}

std::string PrefixCacheTable::ProcessStoreEvent(
    const common::StoredEvent& event, int64_t dp_rank) {
    if (event.block_hashes.empty() && event.token_ids.empty()) {
        return "";
    }

    if (event.block_hashes.empty() != event.token_ids.empty()) {
        return StoreEventLayoutError(
            "block_hashes and token_ids must both be empty or both be "
            "non-empty",
            event, SafeExpectedTokenCount(event));
    }
    if (event.block_size <= 0) {
        return StoreEventLayoutError("block_size must be greater than zero",
                                     event);
    }
    if (!std::in_range<size_t>(event.block_size)) {
        return StoreEventLayoutError(
            "token count overflow: block_size is not representable by "
            "size_t",
            event);
    }

    const size_t block_size = static_cast<size_t>(event.block_size);
    const size_t hash_count = event.block_hashes.size();
    if (hash_count > std::numeric_limits<size_t>::max() / block_size) {
        return StoreEventLayoutError(
            "token count overflow: hash_count * block_size exceeds size_t",
            event);
    }

    const size_t expected_token_count = hash_count * block_size;
    if (event.token_ids.size() != expected_token_count) {
        return StoreEventLayoutError("token count mismatch", event,
                                     expected_token_count);
    }

    const std::string tenant_id = "default";

    VLOG(1) << "In ProcessStoreEvent modelName=" << event.model_name
            << " instanceID=" << event.instance_id << " dpRank=" << dp_rank;

    ModelContext model_context;
    model_context.model_name = event.model_name;
    model_context.lora_name = event.lora_name;
    model_context.block_size = event.block_size;
    model_context.tenant_id = tenant_id;
    model_context.additional_salt = "";
    model_context.instance_id = event.instance_id;
    auto context_data = GetContextData(model_context);

    // Lock order: hashmap_mu, then prefix_mu below.
    std::unique_lock hashmap_lock(context_data->hashmap_mu);
    auto& proxy_hash_map = context_data->proxy_hash_mapping;

    struct NewPrefix {
        uint64_t hash_value;
        std::string engine_id;
    };
    std::vector<NewPrefix> new_prefix_store;

    uint64_t parent_hash = context_data->seed;
    VLOG(1) << "In ProcessStoreEvent seed=" << parent_hash;

    // TODO: ParentBlockHash==0 is ambiguous with root block — no-parent
    // sentinel needed. BUG: ParentBlockHash==0 is indistinguishable from root
    // block.
    if (event.parent_block_hash != 0) {
        VLOG(1) << "parent Block HASH is not None.";
        auto pbh = proxy_hash_map.find(event.parent_block_hash);
        if (pbh != proxy_hash_map.end()) {
            parent_hash = pbh->second;
        }
    }

    for (size_t i = 0; i < event.block_hashes.size(); ++i) {
        const uint64_t block_hash = event.block_hashes[i];
        // cache already exists, add engine info and continue
        auto existing = proxy_hash_map.find(block_hash);
        if (existing != proxy_hash_map.end()) {
            new_prefix_store.push_back({existing->second, event.instance_id});
            continue;
        }
        // if not exists, compute hash
        const uint64_t hash_value = ComputeBlockHash(
            parent_hash, event.token_ids.data() + i * block_size, block_size);
        parent_hash = hash_value;

        proxy_hash_map[block_hash] = hash_value;
        new_prefix_store.push_back({hash_value, event.instance_id});
    }

    if (!new_prefix_store.empty()) {
        std::unique_lock prefix_lock(context_data->prefix_mu);
        HashMapStore* prefix_store = &context_data->prefix_store;
        for (const auto& new_prefix : new_prefix_store) {
            VLOG(1) << "show new prefix data hash=" << new_prefix.hash_value;
            AddNewPrefixStore(prefix_store, new_prefix.hash_value,
                              new_prefix.engine_id, event.medium, dp_rank);
        }
    }

    return "";
}

std::string PrefixCacheTable::ProcessRemoveEvent(
    const common::RemovedEvent& event, int64_t dp_rank,
    const std::string& instance_id) {
    if (event.block_hashes.empty()) {
        return "";
    }
    // TODO @yejj710: debug remove_event.

    ModelContext model_context;
    model_context.model_name = event.model_name;
    model_context.lora_name = event.lora_name;
    model_context.block_size = event.block_size;
    model_context.tenant_id = "default";
    model_context.additional_salt = "";
    model_context.instance_id = instance_id;
    auto context_data = GetContextData(model_context);

    // Lock order: hashmap_mu, then prefix_mu.
    std::unique_lock hashmap_lock(context_data->hashmap_mu);
    auto& proxy_hash_map = context_data->proxy_hash_mapping;

    std::vector<uint64_t> remove_conductor_hash;
    remove_conductor_hash.reserve(event.block_hashes.size());

    // delete proxyHashMapping
    for (const uint64_t block_hash : event.block_hashes) {
        auto it = proxy_hash_map.find(block_hash);
        if (it != proxy_hash_map.end()) {
            remove_conductor_hash.push_back(it->second);
            proxy_hash_map.erase(it);
        }
    }

    std::unique_lock prefix_lock(context_data->prefix_mu);
    HashMapStore& prefix_store = context_data->prefix_store;
    for (const uint64_t conductor_hash : remove_conductor_hash) {
        auto it = prefix_store.prefix_map.find(conductor_hash);
        if (it == prefix_store.prefix_map.end()) {
            continue;
        }
        CacheStoreInfo& cache_store_info = *it->second;

        // Decrement replica count.
        // BUG: TotalReplicaNums not clamped — duplicate removes drive count
        // negative.
        cache_store_info.total_replica_nums -= 1;

        // Remove per-instance metadata.
        // BUG: medium_set / dpRankSet / engineLastAccessTime use inconsistent
        // delete semantics (see docs/KNOWN_ISSUES.md issue 3).
        cache_store_info.engine_last_access_time.erase(instance_id);
        cache_store_info.dp_rank_set.erase(dp_rank);

        // Only delete entry when all replicas are removed
        if (cache_store_info.total_replica_nums <= 0) {
            prefix_store.prefix_map.erase(it);
            prefix_store.total_prefixes--;
        }
    }

    return "";
}

void PrefixCacheTable::AddNewPrefixStore(HashMapStore* prefix_store,
                                         uint64_t hash_value,
                                         const std::string& instance_id,
                                         const std::string& medium,
                                         int64_t dp_rank) {
    const int64_t now = NowUnixSeconds();
    auto it = prefix_store->prefix_map.find(hash_value);
    if (it == prefix_store->prefix_map.end()) {
        VLOG(1) << "in addNewPrefixStore, prefixMap[hashValue] is nil "
                   "hashValue="
                << hash_value;
        it = prefix_store->prefix_map
                 .emplace(hash_value, std::make_unique<CacheStoreInfo>())
                 .first;
        prefix_store->total_prefixes++;
    }
    CacheStoreInfo& cache_store_info = *it->second;

    cache_store_info.engine_last_access_time[instance_id] = now;
    cache_store_info.total_replica_nums += 1;
    // BUG: medium_set has no refcount — dirty read after block eviction (see
    // docs/KNOWN_ISSUES.md issue 3).
    cache_store_info.medium_set.insert(medium);
    cache_store_info.dp_rank_set.insert(dp_rank);
    VLOG(1) << "in addNewPrefixStore conductor_hash=" << hash_value;
}

GlobalView PrefixCacheTable::GetGlobalView() {
    GlobalView view;

    // Snapshot context pointers first (iterate under map lock), then
    // copy each context's mapping under its own locks. We copy under lock
    // for memory safety — the serialised output is identical.
    std::vector<std::pair<ModelContext, std::shared_ptr<ContextData>>> contexts;
    {
        std::shared_lock lock(context_map_mu_);
        contexts.reserve(context_map_.size());
        for (const auto& [ctx, data] : context_map_) {
            contexts.emplace_back(ctx, data);
        }
        view.context_count = static_cast<int32_t>(contexts.size());
    }

    for (auto& [ctx, context_data] : contexts) {
        ModelContextView ctx_view;
        ctx_view.model_name = ctx.model_name;
        ctx_view.lora_name = ctx.lora_name;
        ctx_view.block_size = ctx.block_size;
        ctx_view.additional_salt = ctx.additional_salt;
        ctx_view.tenant_id = ctx.tenant_id;
        ctx_view.instance_id = ctx.instance_id;

        // Lock order: hashmap_mu → prefix_mu (see
        // include/conductor/prefixindex/prefix_indexer.h).
        std::shared_lock hashmap_lock(context_data->hashmap_mu);
        std::shared_lock prefix_lock(context_data->prefix_mu);

        view.proxy_hash_map.push_back(context_data->proxy_hash_mapping);
        view.model_contexts.push_back(std::move(ctx_view));
    }

    return view;
}

}  // namespace prefixindex
}  // namespace conductor
