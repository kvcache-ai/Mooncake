#include "conductor/prefixindex/cache_layout.h"

#include <array>
#include <string_view>

namespace mooncake::conductor::prefixindex {
namespace {

// HF checkpoint basenames used by vLLM, vLLM-Ascend and SGLang. GLM-5.2/5.3
// use DSA/MLA with ordinary logical-prefix semantics; only the Flash variant
// mixes KDA with attention and pooled indexer state.
constexpr std::array<ModelCacheSpec, 6> kModels{{
    {"DeepSeek-V4", false, true, true},
    {"DeepSeek-V4.1-Flash", false, true, true},
    {"GLM-5.2", false, false, false},
    {"GLM-5.3", false, false, false},
    {"GLM-5.3-Flash", true, false, true},
    {"Kimi-K3", true, false, false},
}};

struct ModelAlias {
    std::string_view name;
    size_t model;
};

constexpr std::array<ModelAlias, 8> kAliases{{
    {"deepseekv4", 0},
    {"deepseekv4flash", 0},
    {"deepseekv4pro", 0},
    {"deepseekv41flash", 1},
    {"glm52", 2},
    {"glm53", 3},
    {"glm53flash", 4},
    {"kimik3", 5},
}};

std::string NormalizeModelName(std::string_view name) {
    while (!name.empty() && (name.back() == '/' || name.back() == '\\'))
        name.remove_suffix(1);
    if (const auto slash = name.find_last_of("/\\");
        slash != std::string_view::npos)
        name.remove_prefix(slash + 1);
    std::string result;
    for (char ch : name) {
        if (ch == '-' || ch == '_' || ch == '.' || ch == ' ') continue;
        result.push_back(ch >= 'A' && ch <= 'Z' ? ch + ('a' - 'A') : ch);
    }
    // Known weight formats preserve the cache algorithm. Arbitrary suffix or
    // substring matching could incorrectly recognize a different model.
    for (auto suffix : {"fp8", "bf16", "w4a8", "w8a8"}) {
        if (result.ends_with(suffix)) {
            result.resize(result.size() - std::string_view(suffix).size());
            break;
        }
    }
    return result;
}

}  // namespace

const ModelCacheSpec* FindModelCacheSpec(std::string_view model_name) {
    const auto name = NormalizeModelName(model_name);
    for (const auto& alias : kAliases) {
        if (alias.name == name) return &kModels[alias.model];
    }
    return nullptr;
}

SourceLayout ResolveModelLayout(const common::ServiceConfig& service) {
    const auto* model = FindModelCacheSpec(service.model_name);
    if (!model || !model->RequiresGroupedMatching()) return {};

    auto layout = std::make_shared<CacheLayout>();
    layout->id = std::string(model->name);
    // Registration supplies the actual producer hash unit. Do not synthesize
    // a different hash chain from physical compression ratios or page sizes.
    layout->hash_unit_tokens = service.block_size;
    layout->alignment_tokens = service.block_size;
    const bool sglang = service.hash_profile.strategy == "sglang" ||
                        service.hash_profile.strategy == "sglang_bigram";
    if (sglang)
        layout->hash_domains = {HashDomain::kSglangStorage,
                                HashDomain::kSglangEvent};

    // A model identifies required algorithms, not deployed layer-to-group or
    // shard coverage. vLLM can split one attention type into multiple groups;
    // SGLang pages omit independent state changes; Mooncake keys do not list
    // all required parts/rank namespaces. Observed groups are not an inventory.
    layout->unavailable_reason =
        "existing events do not certify the complete deployed cache-group "
        "and recovery layout";

    SourceLayout result;
    result.layout = std::move(layout);
    switch (service.publisher_kind) {
        case common::PublisherKind::kVllm:
            result.source = LayoutSource::kVllm;
            break;
        case common::PublisherKind::kSglang:
            result.source = LayoutSource::kSglang;
            result.hash_domain = HashDomain::kSglangEvent;
            break;
        case common::PublisherKind::kMooncake:
            result.source = LayoutSource::kMooncake;
            result.hash_domain =
                sglang ? HashDomain::kSglangStorage : HashDomain::kRaw;
            break;
    }
    return result;
}

std::string ValidateCacheLayout(const CacheLayout& layout) {
    if (layout.id.empty() || layout.hash_unit_tokens <= 0 ||
        layout.alignment_tokens <= 0 ||
        layout.alignment_tokens % layout.hash_unit_tokens != 0 ||
        layout.max_records == 0 ||
        (layout.groups.empty() && layout.unavailable_reason.empty()))
        return "invalid cache layout geometry";
    if (layout.hash_domains.empty() ||
        std::set<HashDomain>(layout.hash_domains.begin(),
                             layout.hash_domains.end())
                .size() != layout.hash_domains.size())
        return "empty or duplicate hash domains";
    for (auto domain : layout.hash_domains) {
        if (domain != HashDomain::kRaw &&
            domain != HashDomain::kSglangStorage &&
            domain != HashDomain::kSglangEvent &&
            domain != HashDomain::kSglangDynamo)
            return "unsupported hash domain";
    }
    if (!layout.unavailable_reason.empty()) return "";
    std::set<std::string> ids;
    bool required = false;
    for (const auto& group : layout.groups) {
        if (group.id.empty() || !ids.insert(group.id).second)
            return "duplicate or empty group id";
        if (group.block_tokens <= 0 ||
            group.block_tokens % layout.hash_unit_tokens != 0 ||
            group.alignment_tokens <= 0 ||
            group.alignment_tokens % layout.hash_unit_tokens != 0)
            return "group geometry must align with the producer hash unit";
        if (group.window_tokens < 0 || group.trailing_blocks < 0 ||
            (group.match == MatchPolicy::kWindow) !=
                (group.window_tokens > 0 || group.trailing_blocks > 0))
            return "window groups require window_tokens or trailing_blocks";
        if (group.parts.empty()) return "group parts must not be empty";
        std::set<std::string> parts;
        for (const auto& part : group.parts) {
            if (part.empty() || !parts.insert(part).second)
                return "duplicate or empty part";
        }
        required = required || group.required;
    }
    return required ? "" : "layout has no required groups";
}

std::string ValidateLayoutProfile(const CacheLayout& layout,
                                  const common::ResolvedHashProfile& profile) {
    const bool sglang =
        profile.strategy == "sglang" || profile.strategy == "sglang_bigram";
    for (auto domain : layout.hash_domains) {
        if (sglang == (domain == HashDomain::kRaw))
            return "layout hash domains conflict with the registered hash "
                   "strategy";
    }
    return "";
}

}  // namespace mooncake::conductor::prefixindex
