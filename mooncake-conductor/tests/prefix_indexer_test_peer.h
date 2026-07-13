#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "conductor/prefixindex/prefix_indexer.h"

namespace conductor {
namespace prefixindex {

struct PrefixEntrySnapshot {
    std::unordered_map<std::string, int64_t> engine_last_access_time;
    int64_t total_replica_nums = 0;
    std::set<std::string> medium_set;
    std::set<int64_t> dp_rank_set;

    bool operator==(const PrefixEntrySnapshot&) const = default;
};

struct ContextDataSnapshot {
    uint64_t seed = 0;
    std::set<int64_t> dp_size;
    std::unordered_map<uint64_t, uint64_t> proxy_hash_mapping;
    std::unordered_map<uint64_t, PrefixEntrySnapshot> prefix_entries;
    int64_t total_prefixes = 0;
    int64_t last_access = 0;

    bool operator==(const ContextDataSnapshot&) const = default;
};

struct GlobalViewSnapshot {
    int32_t context_count = 0;
    std::vector<ModelContext> model_contexts;
    std::vector<std::unordered_map<uint64_t, uint64_t>> proxy_hash_map;

    bool operator==(const GlobalViewSnapshot&) const = default;
};

struct PrefixCacheTableSnapshot {
    GlobalViewSnapshot global_view;
    std::unordered_map<ModelContext, ContextDataSnapshot> contexts;

    bool operator==(const PrefixCacheTableSnapshot&) const = default;
};

// Test-only access to the table's internal context and prefix state.
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

    static int64_t GetLastAccess(PrefixCacheTable& table,
                                 const ModelContext& ctx) {
        auto data = table.LoadContextData(ctx);
        return data ? data->prefix_store.last_access.load() : 0;
    }

    static void SetLastAccess(PrefixCacheTable& table, const ModelContext& ctx,
                              int64_t value) {
        table.GetContextData(ctx)->prefix_store.last_access.store(value);
    }

    static PrefixCacheTableSnapshot Snapshot(PrefixCacheTable& table) {
        PrefixCacheTableSnapshot snapshot;
        const GlobalView view = table.GetGlobalView();
        snapshot.global_view.context_count = view.context_count;
        snapshot.global_view.proxy_hash_map = view.proxy_hash_map;
        snapshot.global_view.model_contexts.reserve(view.model_contexts.size());
        for (const auto& view_context : view.model_contexts) {
            ModelContext context;
            context.model_name = view_context.model_name;
            context.lora_name = view_context.lora_name;
            context.block_size = view_context.block_size;
            context.additional_salt = view_context.additional_salt;
            context.tenant_id = view_context.tenant_id;
            context.instance_id = view_context.instance_id;
            snapshot.global_view.model_contexts.push_back(std::move(context));
        }

        std::vector<std::pair<ModelContext, std::shared_ptr<ContextData>>>
            contexts;
        {
            std::shared_lock map_lock(table.context_map_mu_);
            contexts.reserve(table.context_map_.size());
            for (const auto& [context, data] : table.context_map_) {
                contexts.emplace_back(context, data);
            }
        }

        for (const auto& [context, data] : contexts) {
            ContextDataSnapshot context_snapshot;
            std::shared_lock hashmap_lock(data->hashmap_mu);
            std::shared_lock prefix_lock(data->prefix_mu);

            context_snapshot.seed = data->seed;
            context_snapshot.dp_size = data->dp_size;
            context_snapshot.proxy_hash_mapping = data->proxy_hash_mapping;
            context_snapshot.total_prefixes = data->prefix_store.total_prefixes;
            context_snapshot.last_access =
                data->prefix_store.last_access.load();
            for (const auto& [hash, info] : data->prefix_store.prefix_map) {
                PrefixEntrySnapshot entry;
                entry.engine_last_access_time = info->engine_last_access_time;
                entry.total_replica_nums = info->total_replica_nums;
                entry.medium_set = info->medium_set;
                entry.dp_rank_set = info->dp_rank_set;
                context_snapshot.prefix_entries.emplace(hash, std::move(entry));
            }
            snapshot.contexts.emplace(context, std::move(context_snapshot));
        }
        return snapshot;
    }
};

}  // namespace prefixindex
}  // namespace conductor
