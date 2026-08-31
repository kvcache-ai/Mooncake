#pragma once

#include <cstdint>
#include <string>

#include "conductor/prefixindex/types.h"

namespace conductor {
namespace kvevent {

struct ParsedSglangObjectKey {
    // The key prefix through the logical hash, excluding physical suffixes
    // such as _0_k/_0_v or _0_temporal.
    std::string logical_key;
    std::string full_hash;
    std::string namespace_prefix;
    std::string component_suffix;
    prefixindex::ProjectedPrefix prefix;
};

// Parse a SGLang Mooncake object key.  SGLang keys contain a complete
// lower/upper-case SHA-256 digest followed by an optional physical component
// suffix.  The projected hash is the first 64 bits of the digest, represented
// as an unsigned bit pattern.
std::string ParseSglangObjectKey(const std::string& object_key,
                                 ParsedSglangObjectKey* result);

// Compatibility parser for vLLM and vLLM-Ascend connector keys when a
// Mooncake publisher has been configured to forward only the raw key.
std::string ParseVllmObjectKey(const std::string& object_key,
                               ParsedSglangObjectKey* result);

}  // namespace kvevent
}  // namespace conductor
