#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "conductor/prefixindex/prefix_indexer.h"

namespace conductor {
namespace prefixindex {

struct BlockPresenceSnapshot {
    std::set<EngineOwner> gpu_owners;
    std::set<SharedObjectOwner> cpu_owners;
    std::set<SharedObjectOwner> disk_owners;

    bool operator==(const BlockPresenceSnapshot&) const = default;
};

struct ContextStateSnapshot {
    HashProfile profile;
    std::map<std::string, std::set<int64_t>> instance_ranks;
    std::unordered_map<ProjectedPrefix, BlockPresenceSnapshot> blocks;

    bool operator==(const ContextStateSnapshot&) const = default;
};

struct PrefixCacheTableSnapshot {
    std::unordered_map<ContextKey, ContextStateSnapshot> contexts;

    bool operator==(const PrefixCacheTableSnapshot&) const = default;
};

class PrefixCacheTableTestPeer {
   public:
    static bool ContextExists(const PrefixCacheTable& table,
                              const ContextKey& context) {
        return table.LoadContextState(context) != nullptr;
    }

    static std::unique_lock<std::shared_mutex> LockContextState(
        const PrefixCacheTable& table, const ContextKey& context) {
        auto state = table.LoadContextState(context);
        if (state == nullptr) return {};
        return std::unique_lock<std::shared_mutex>(state->mutex);
    }

    static PrefixCacheTableSnapshot Snapshot(const PrefixCacheTable& table) {
        PrefixCacheTableSnapshot snapshot;
        std::vector<std::pair<ContextKey, std::shared_ptr<ContextState>>>
            contexts;
        {
            std::shared_lock map_lock(table.context_map_mutex_);
            contexts.reserve(table.contexts_.size());
            for (const auto& item : table.contexts_) {
                contexts.push_back(item);
            }
        }

        for (const auto& [context, state] : contexts) {
            std::shared_lock state_lock(state->mutex);
            ContextStateSnapshot state_snapshot;
            state_snapshot.profile = state->profile;
            state_snapshot.instance_ranks = state->instance_ranks;
            for (const auto& [prefix, presence] : state->blocks) {
                state_snapshot.blocks.emplace(
                    prefix, BlockPresenceSnapshot{presence.gpu_owners,
                                                  presence.cpu_owners,
                                                  presence.disk_owners});
            }
            snapshot.contexts.emplace(context, std::move(state_snapshot));
        }
        return snapshot;
    }
};

}  // namespace prefixindex
}  // namespace conductor
