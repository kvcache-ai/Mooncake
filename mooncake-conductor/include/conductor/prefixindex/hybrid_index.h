#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "conductor/prefixindex/cache_layout.h"
#include "conductor/prefixindex/types.h"

namespace mooncake::conductor::prefixindex {

struct ContextState;
struct CacheHitResult;

struct GroupMutation {
    ContextKey context;
    std::vector<ProjectedPrefix> prefixes;
    size_t group = 0;
    // nullopt denotes one atomic engine group; a raw object names one part.
    std::optional<size_t> part;
    StorageTier tier = StorageTier::kDevice;
    TierOwner owner;
    std::string storage_domain;
    std::string replica = "default";
    HashDomain hash_domain = HashDomain::kRaw;
};

// All operations are guarded by the containing ContextState's mutex. Ordinary
// contexts do not allocate this index or add fields to BlockPresence.
class HybridIndex {
   public:
    explicit HybridIndex(std::shared_ptr<const CacheLayout> layout)
        : layout_(std::move(layout)) {}

    const CacheLayout& Layout() const { return *layout_; }
    std::string ActivateDeployment(const SourceLayout& binding,
                                   const std::string& instance, int64_t rank);
    std::optional<SourceLayout> Deployment() const { return deployment_; }
    std::string Register(const std::string& instance, int64_t rank,
                         const std::set<std::string>& readable_domains);
    void Unregister(const std::string& instance, int64_t rank);
    std::string Validate(const GroupMutation& mutation) const;
    void Store(const GroupMutation& mutation);
    void Remove(const GroupMutation& mutation);
    void Clear(const std::string& source,
               const std::optional<std::string>& backend = std::nullopt,
               std::optional<StorageTier> tier = std::nullopt);
    size_t PrefixCount() const { return blocks_.size(); }
    size_t RecordCount() const { return records_; }

    std::map<std::string, CacheHitResult> Query(
        const ContextState& state, const ContextKey& context,
        std::span<const int32_t> token_ids,
        const std::optional<std::string>& salt,
        const std::optional<std::string>& instance_filter) const;

   private:
    struct DomainPrefix {
        HashDomain domain;
        ProjectedPrefix hash;
        bool operator==(const DomainPrefix&) const = default;
    };
    struct DomainPrefixHash {
        size_t operator()(const DomainPrefix& key) const noexcept {
            return std::hash<ProjectedPrefix>{}(key.hash) ^
                   (static_cast<size_t>(key.domain) * 0x9e3779b9U);
        }
    };
    struct ReplicaKey {
        StorageTier tier;
        TierOwner owner;  // shared object_id is stripped; parts retain it below
        std::string storage_domain;
        std::string replica;
        auto operator<=>(const ReplicaKey&) const = default;
    };
    struct Replica {
        std::vector<std::set<std::string>> parts;
        size_t present_parts = 0;
    };
    struct PrefixState {
        std::vector<std::map<ReplicaKey, Replica>> groups;
        size_t records = 0;
        std::list<DomainPrefix>::iterator order;
    };

    static ReplicaKey KeyFor(const GroupMutation& mutation);
    template <typename Predicate>
    void EraseIf(Predicate matches);
    void Erase(DomainPrefix prefix);
    uint8_t Presence(ProjectedPrefix prefix, HashDomain domain, size_t group,
                     const std::string& instance, int64_t rank,
                     const std::set<std::string>& domains) const;

    std::shared_ptr<const CacheLayout> layout_;
    std::optional<SourceLayout> deployment_;
    std::map<std::pair<std::string, int64_t>, std::set<std::string>> readers_;
    std::unordered_map<DomainPrefix, PrefixState, DomainPrefixHash> blocks_;
    std::list<DomainPrefix> write_order_;
    size_t records_ = 0;
};

}  // namespace mooncake::conductor::prefixindex
