#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "conductor/common/types.h"
#include "conductor/prefixindex/hash_strategy.h"

namespace mooncake::conductor::prefixindex {

enum class MatchPolicy { kDense, kCheckpoint, kWindow };
enum class LayoutSource { kVllm, kVllmAnnouncement, kSglang, kMooncake };

struct CacheGroupLayout {
    std::string id;
    MatchPolicy match = MatchPolicy::kDense;
    int64_t block_tokens = 0;
    int64_t window_tokens = 0;
    // Exact retained Store-page tail from a verified L3 layout; zero uses
    // the normal sliding-attention window formula.
    int64_t trailing_blocks = 0;
    int64_t alignment_tokens = 0;
    bool required = true;
    std::vector<std::string> parts{"default"};
    std::string event_kind;

    bool operator==(const CacheGroupLayout&) const = default;
};

// Read-only geometry; deployment activation replaces the initial placeholder.
// Token counts refer to logical KV positions.
struct CacheLayout {
    std::string id;
    int64_t hash_unit_tokens = 0;
    int64_t alignment_tokens = 0;
    size_t max_records = 200000;
    std::vector<HashDomain> hash_domains{HashDomain::kRaw};
    std::optional<std::string> sglang_extra_key;
    std::vector<CacheGroupLayout> groups;
    // Nonempty when existing producer metadata cannot certify the deployment's
    // complete recovery layout. Such contexts return zero hits, never ordinary
    // Full-attention hits inferred from a subset of the required components.
    std::string unavailable_reason;

    bool operator==(const CacheLayout&) const = default;
};

struct ObjectPartRule {
    std::string key_prefix;
    std::string key_suffix;
    size_t group = 0;
    size_t part = 0;
    std::string replica = "default";
    bool operator==(const ObjectPartRule&) const = default;
};

// Internal binding derived from model rules and validated deployment metadata.
// Never serialized in ServiceConfig or client RPCs.
struct SourceLayout {
    std::shared_ptr<const CacheLayout> layout;
    LayoutSource source = LayoutSource::kVllm;
    std::map<int64_t, size_t> groups;
    std::vector<ObjectPartRule> objects;
    // Model component of the declared AscendStore keys, not the query context.
    std::optional<std::string> physical_model_name;
    std::string storage_domain;
    std::set<std::string> readable_domains;
    HashDomain hash_domain = HashDomain::kRaw;
};

struct ModelCacheSpec {
    std::string_view name;
    bool checkpoints;
    bool sliding_window;
    bool token_compression;

    bool RequiresGroupedMatching() const {
        return checkpoints || sliding_window || token_compression;
    }
};

// Exact normalized model aliases only. Unknown/custom served names keep the
// ordinary path; model names and ContextKeys are never rewritten.
const ModelCacheSpec* FindModelCacheSpec(std::string_view model_name);
SourceLayout ResolveModelLayout(const common::ServiceConfig& service);
std::string ValidateCacheLayout(const CacheLayout& layout);
std::string ValidateLayoutProfile(const CacheLayout& layout,
                                  const common::ResolvedHashProfile& profile);

}  // namespace mooncake::conductor::prefixindex
