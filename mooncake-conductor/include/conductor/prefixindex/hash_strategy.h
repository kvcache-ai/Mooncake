#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "conductor/prefixindex/types.h"

namespace mooncake::conductor::prefixindex {

struct HashBlock {
    std::array<uint8_t, 32> digest{};
    ProjectedPrefix projected;

    bool operator==(const HashBlock&) const = default;
};

// A lazily-evaluated block-hash chain. Blocks are hashed on demand: asking
// for block i hashes (and caches) every block up to i, so a caller that
// stops early — e.g. a prefix-index walk whose cursors have all stalled —
// never pays for the untouched tail.
class HashChain {
   public:
    virtual ~HashChain() = default;

    // Number of logical blocks the chain can produce. The SGLang strategies
    // include their final partial block; vLLM keeps its complete-block rule.
    virtual size_t BlockCount() const = 0;

    // Number of blocks hashed so far (observability/testing hook).
    virtual size_t ComputedCount() const = 0;

    // Returns block index, hashing any uncomputed prefix first. Returns
    // nullptr and sets error on failure; the error is sticky across calls.
    virtual const HashBlock* At(size_t index, std::string* error) = 0;
};

class HashStrategy {
   public:
    virtual ~HashStrategy() = default;

    // Computes hashes for the blocks defined by the selected engine strategy.
    // Returns an empty string on success and leaves out empty on failure.
    virtual std::string Compute(const ContextKey& context,
                                std::span<const int32_t> token_ids,
                                std::optional<std::string> cache_salt,
                                std::vector<HashBlock>* out) const = 0;

    // Creates a lazy hash chain over the same inputs as Compute. The chain
    // borrows token_ids; the caller must keep it alive for the chain's
    // lifetime. Returns nullptr and sets error when the inputs are invalid.
    virtual std::unique_ptr<HashChain> CreateChain(
        const ContextKey& context, std::span<const int32_t> token_ids,
        std::optional<std::string> cache_salt, std::string* error) const = 0;
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

}  // namespace mooncake::conductor::prefixindex
