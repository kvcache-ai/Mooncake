#pragma once

// KV cache kinds split ObjectDataType::KVCACHE into the layouts a hybrid
// model actually stores. The kind rides with the object so later admission,
// replication, and eviction can tell a full-attention page from a sliding
// window, a DSA latent, its indexer, or a recurrent state.
//
// UNSPECIFIED is the legacy bucket: objects written before this field, and
// any put that does not name a kind, keep today's eviction behavior.

#include <array>
#include <cctype>
#include <cstdint>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace mooncake {

enum class KVCacheKind : uint8_t {
    UNSPECIFIED = 0,
    FULL = 1,     // Full-attention KV.
    SWA = 2,      // Sliding-window attention KV.
    DSA = 3,      // DeepSeek sparse-attention latent KV.
    INDEXER = 4,  // DSA indexer vectors that address DSA KV.
    MLA = 5,      // Multi-head latent attention cache that is not DSA.
    MAMBA = 6,    // Mamba / SSM recurrent state.
    LINEAR = 7,   // Other linear-attention recurrent state.
};

enum class KVCacheEvictControl : uint8_t {
    // Evict in the ordinary lease order.
    NORMAL = 0,
    // Evict only in the fallback pass, after ordinary objects.
    DEFER = 1,
    // Leave the object in place after its lease expires. A hard pin is
    // still stronger: the evictor never asks this policy about one.
    PROTECT = 2,
};

inline constexpr bool IsKnownKVCacheKind(KVCacheKind kind) noexcept {
    switch (kind) {
        case KVCacheKind::UNSPECIFIED:
        case KVCacheKind::FULL:
        case KVCacheKind::SWA:
        case KVCacheKind::DSA:
        case KVCacheKind::INDEXER:
        case KVCacheKind::MLA:
        case KVCacheKind::MAMBA:
        case KVCacheKind::LINEAR:
            return true;
    }
    return false;
}

inline std::string_view KVCacheKindName(KVCacheKind kind) noexcept {
    switch (kind) {
        case KVCacheKind::UNSPECIFIED:
            return "UNSPECIFIED";
        case KVCacheKind::FULL:
            return "FULL";
        case KVCacheKind::SWA:
            return "SWA";
        case KVCacheKind::DSA:
            return "DSA";
        case KVCacheKind::INDEXER:
            return "INDEXER";
        case KVCacheKind::MLA:
            return "MLA";
        case KVCacheKind::MAMBA:
            return "MAMBA";
        case KVCacheKind::LINEAR:
            return "LINEAR";
    }
    return "UNSPECIFIED";
}

inline std::optional<KVCacheKind> ParseKVCacheKind(std::string_view name) {
    if (name == "UNSPECIFIED") return KVCacheKind::UNSPECIFIED;
    if (name == "FULL") return KVCacheKind::FULL;
    if (name == "SWA") return KVCacheKind::SWA;
    if (name == "DSA") return KVCacheKind::DSA;
    if (name == "INDEXER") return KVCacheKind::INDEXER;
    if (name == "MLA") return KVCacheKind::MLA;
    if (name == "MAMBA") return KVCacheKind::MAMBA;
    if (name == "LINEAR") return KVCacheKind::LINEAR;
    return std::nullopt;
}

inline std::string_view KVCacheEvictControlName(
    KVCacheEvictControl control) noexcept {
    switch (control) {
        case KVCacheEvictControl::NORMAL:
            return "NORMAL";
        case KVCacheEvictControl::DEFER:
            return "DEFER";
        case KVCacheEvictControl::PROTECT:
            return "PROTECT";
    }
    return "NORMAL";
}

inline std::optional<KVCacheEvictControl> ParseKVCacheEvictControl(
    std::string_view name) {
    if (name == "NORMAL") return KVCacheEvictControl::NORMAL;
    if (name == "DEFER") return KVCacheEvictControl::DEFER;
    if (name == "PROTECT") return KVCacheEvictControl::PROTECT;
    return std::nullopt;
}

inline std::ostream& operator<<(std::ostream& os, KVCacheKind kind) {
    return os << KVCacheKindName(kind);
}

// What eviction may do with one kind. Missing kinds are NORMAL, which is
// also the policy of an empty configuration.
class KVCacheKindPolicy {
   public:
    [[nodiscard]] KVCacheEvictControl ControlFor(KVCacheKind kind) const {
        const auto index = static_cast<uint8_t>(kind);
        if (index >= controls_.size()) return KVCacheEvictControl::NORMAL;
        return controls_[index];
    }

    // "MAMBA=PROTECT,INDEXER=DEFER". Empty is a valid policy. Names are
    // case-sensitive. A kind may appear once.
    [[nodiscard]] static std::optional<KVCacheKindPolicy> Parse(
        std::string_view spec, std::string* error) {
        KVCacheKindPolicy policy;
        std::array<bool, kKindCount> seen{};
        std::string_view rest = spec;
        while (!rest.empty()) {
            const auto comma = rest.find(',');
            auto token = Trim(rest.substr(0, comma));
            rest = comma == std::string_view::npos ? std::string_view{}
                                                   : rest.substr(comma + 1);
            if (comma != std::string_view::npos && Trim(rest).empty()) {
                if (error) *error = "empty kv_cache_kind_evict entry";
                return std::nullopt;
            }
            if (token.empty()) {
                if (error) *error = "empty kv_cache_kind_evict entry";
                return std::nullopt;
            }
            const auto eq = token.find('=');
            if (eq == std::string_view::npos) {
                if (error) {
                    *error = "kv_cache_kind_evict entry '" +
                             std::string(token) +
                             "' must look like KIND=CONTROL";
                }
                return std::nullopt;
            }
            const auto kind_name = Trim(token.substr(0, eq));
            const auto control_name = Trim(token.substr(eq + 1));
            const auto kind = ParseKVCacheKind(kind_name);
            const auto control = ParseKVCacheEvictControl(control_name);
            if (!kind || !control) {
                if (error) {
                    *error = "unknown kv_cache_kind_evict entry '" +
                             std::string(token) + "'";
                }
                return std::nullopt;
            }
            const auto index = static_cast<uint8_t>(*kind);
            if (seen[index]) {
                if (error) {
                    *error = "duplicate kv_cache_kind_evict kind " +
                             std::string(kind_name);
                }
                return std::nullopt;
            }
            seen[index] = true;
            policy.controls_[index] = *control;
        }
        return policy;
    }

   private:
    static constexpr size_t kKindCount =
        static_cast<size_t>(KVCacheKind::LINEAR) + 1;

    static std::string_view Trim(std::string_view text) {
        while (!text.empty() &&
               std::isspace(static_cast<unsigned char>(text.front()))) {
            text.remove_prefix(1);
        }
        while (!text.empty() &&
               std::isspace(static_cast<unsigned char>(text.back()))) {
            text.remove_suffix(1);
        }
        return text;
    }

    std::array<KVCacheEvictControl, kKindCount> controls_{};
};

inline KVCacheKindPolicy ParseKVCacheKindPolicyOrDie(std::string_view spec) {
    std::string error;
    auto policy = KVCacheKindPolicy::Parse(spec, &error);
    if (!policy) {
        throw std::invalid_argument(error);
    }
    return *policy;
}

}  // namespace mooncake
