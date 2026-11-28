#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "conductor/prefixindex/types.h"

namespace mooncake::conductor::prefixindex {

// Producer hash namespaces are independent of the cache recovery policy.
enum class HashDomain { kRaw, kSglangStorage, kSglangEvent, kSglangDynamo };

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
// empty string on success. When provided, error_field identifies an invalid
// configuration field and is cleared on success or non-field errors.
std::string ResolveHashProfile(const common::HashProfileConfig& config,
                               HashProfile* out,
                               std::string* error_field = nullptr);

// Returns an empty string when the resolved profile is supported, well formed,
// and its root digest matches a fresh derivation from python_hash_seed.
std::string ValidateHashProfile(const HashProfile& profile);

// Returns nullptr and sets error when the resolved profile shape is invalid or
// unsupported. This consumes the derived root without hashing the seed again.
// The default preserves native hashes. SGLang storage/event namespaces select
// their producer's root/projection rules; they do not depend on cache layout.
std::unique_ptr<HashStrategy> CreateHashStrategy(
    const HashProfile& profile, std::string* error,
    HashDomain domain = HashDomain::kRaw,
    std::optional<std::string> extra_key = std::nullopt);

std::string DigestToHex(const std::array<uint8_t, 32>& digest);

// "vllm_v1" hashes vLLM's untagged extra keys (bare LoRA name and cache salt,
// vLLM < v0.31). "vllm_v1_tagged" hashes the tagged tuples vLLM >= v0.31
// uses. Both share the vLLM v1 chain, root, and low64_be projection.
inline constexpr std::string_view kVllmV1Strategy = "vllm_v1";
inline constexpr std::string_view kVllmV1TaggedStrategy = "vllm_v1_tagged";

// Returns true for every strategy that uses the vLLM v1 block-hash chain.
bool IsVllmV1Strategy(std::string_view strategy);

// Returns an empty string when the profile can reproduce the publisher's
// block hashes for this context. vllm_v1_tagged cannot hash LoRA blocks:
// vLLM keys them by ("lora", name, path) and registration carries no path.
std::string ValidateHashProfileForContext(const HashProfile& profile,
                                          const ContextKey& context);

}  // namespace mooncake::conductor::prefixindex
