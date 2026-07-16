#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <shared_mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "conductor/prefixindex/types.h"

namespace conductor {
namespace prefixindex {

struct RegistrationResult {
    bool inserted = false;
    std::string error;
};

struct BlockPresence {
    std::set<EngineOwner> gpu_owners;
    std::set<SharedObjectOwner> cpu_owners;
    std::set<SharedObjectOwner> disk_owners;

    bool Empty() const {
        return gpu_owners.empty() && cpu_owners.empty() && disk_owners.empty();
    }
};

struct ContextState {
    explicit ContextState(HashProfile registered_profile)
        : profile(std::move(registered_profile)) {}

    // Lock order is global context-map mutex, then this mutex. Code holding
    // this mutex must never reacquire the global mutex.
    mutable std::shared_mutex mutex;
    const HashProfile profile;
    std::map<std::string, std::set<int64_t>> instance_ranks;
    std::unordered_map<ProjectedPrefix, BlockPresence> blocks;
};

struct CacheHitResult {
    int64_t longest_match_tokens = 0;
    std::map<int64_t, int64_t> dp;
    int64_t gpu = 0;
    int64_t cpu = 0;
    int64_t disk = 0;
};

struct ContextView {
    ContextKey context;
    HashProfile profile;
    std::map<std::string, std::set<int64_t>> instance_ranks;
    size_t prefix_count = 0;
};

struct GlobalView {
    int32_t context_count = 0;
    std::vector<ContextView> contexts;
};

class PrefixCacheTable {
   public:
    PrefixCacheTable() = default;
    PrefixCacheTable(const PrefixCacheTable&) = delete;
    PrefixCacheTable& operator=(const PrefixCacheTable&) = delete;

    static RegistrationResult ValidateRegistration(
        const EngineRegistration& registration);

    RegistrationResult Register(const EngineRegistration& registration);
    std::string ValidateProfileBinding(const ContextKey& context,
                                       const HashProfile& profile) const;
    std::string Unregister(const ContextKey& context,
                           const std::string& instance_id, int64_t dp_rank);

    std::string StoreGpu(const GpuMutation& mutation);
    std::string RemoveGpu(const GpuMutation& mutation);
    std::string ClearGpu(const GpuClear& clear);

    std::string StoreShared(const SharedMutation& mutation);
    std::string RemoveShared(const SharedMutation& mutation);
    std::string ClearShared(const SharedClear& clear);

    std::map<std::string, CacheHitResult> Query(
        const ContextKey& context, std::span<const int32_t> token_ids,
        std::optional<std::string> cache_salt = std::nullopt,
        std::optional<std::string> instance_filter = std::nullopt) const;

    GlobalView GetGlobalView() const;

   private:
    friend class PrefixCacheTableTestPeer;

    std::shared_ptr<ContextState> LoadContextState(
        const ContextKey& context) const;

    mutable std::shared_mutex context_map_mutex_;
    std::unordered_map<ContextKey, std::shared_ptr<ContextState>> contexts_;
};

}  // namespace prefixindex
}  // namespace conductor
