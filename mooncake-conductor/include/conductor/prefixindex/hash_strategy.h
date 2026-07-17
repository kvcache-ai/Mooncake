#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "conductor/prefixindex/types.h"

namespace conductor {
namespace prefixindex {

struct HashBlock {
    std::array<uint8_t, 32> digest{};
    ProjectedPrefix projected;

    bool operator==(const HashBlock&) const = default;
};

class HashStrategy {
   public:
    virtual ~HashStrategy() = default;

    // Computes hashes only for complete blocks. Returns an empty string on
    // success and leaves out empty on failure.
    virtual std::string Compute(const ContextKey& context,
                                std::span<const int32_t> token_ids,
                                std::optional<std::string> cache_salt,
                                std::vector<HashBlock>* out) const = 0;
};

// Resolves a supported source profile and derives its root digest. Returns an
// empty string on success.
std::string ResolveHashProfile(const common::HashProfileConfig& config,
                               HashProfile* out);

// Returns an empty string when the resolved profile is supported, well formed,
// and its root digest matches a fresh derivation from python_hash_seed.
std::string ValidateHashProfile(const HashProfile& profile);

// Returns nullptr and sets error when the resolved profile shape is invalid or
// unsupported. This consumes the derived root without hashing the seed again.
std::unique_ptr<HashStrategy> CreateHashStrategy(const HashProfile& profile,
                                                 std::string* error);

std::string DigestToHex(const std::array<uint8_t, 32>& digest);

}  // namespace prefixindex
}  // namespace conductor
