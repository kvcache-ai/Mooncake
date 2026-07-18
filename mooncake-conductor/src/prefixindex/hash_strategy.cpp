#include "conductor/prefixindex/hash_strategy.h"

#include <openssl/evp.h>

#include <cstddef>
#include <limits>
#include <memory>
#include <string_view>
#include <utility>

namespace conductor {
namespace prefixindex {

namespace {

constexpr size_t kSha256DigestSize = 32;
constexpr uint64_t kMaxPythonHashSeed = std::numeric_limits<uint32_t>::max();

void AppendTypeAndLength(uint8_t major_type, uint64_t value,
                         std::vector<uint8_t>* out) {
    const uint8_t initial = static_cast<uint8_t>(major_type << 5);
    if (value < 24) {
        out->push_back(static_cast<uint8_t>(initial | value));
        return;
    }
    if (value <= std::numeric_limits<uint8_t>::max()) {
        out->push_back(static_cast<uint8_t>(initial | 24));
        out->push_back(static_cast<uint8_t>(value));
        return;
    }
    if (value <= std::numeric_limits<uint16_t>::max()) {
        out->push_back(static_cast<uint8_t>(initial | 25));
        for (int shift = 8; shift >= 0; shift -= 8) {
            out->push_back(static_cast<uint8_t>(value >> shift));
        }
        return;
    }
    if (value <= std::numeric_limits<uint32_t>::max()) {
        out->push_back(static_cast<uint8_t>(initial | 26));
        for (int shift = 24; shift >= 0; shift -= 8) {
            out->push_back(static_cast<uint8_t>(value >> shift));
        }
        return;
    }

    out->push_back(static_cast<uint8_t>(initial | 27));
    for (int shift = 56; shift >= 0; shift -= 8) {
        out->push_back(static_cast<uint8_t>(value >> shift));
    }
}

void AppendArrayHeader(size_t size, std::vector<uint8_t>* out) {
    AppendTypeAndLength(4, static_cast<uint64_t>(size), out);
}

void AppendBytes(std::span<const uint8_t> value, std::vector<uint8_t>* out) {
    AppendTypeAndLength(2, static_cast<uint64_t>(value.size()), out);
    out->insert(out->end(), value.begin(), value.end());
}

void AppendText(std::string_view value, std::vector<uint8_t>* out) {
    AppendTypeAndLength(3, static_cast<uint64_t>(value.size()), out);
    out->insert(out->end(), value.begin(), value.end());
}

void AppendSignedInteger(int32_t value, std::vector<uint8_t>* out) {
    if (value >= 0) {
        AppendTypeAndLength(0, static_cast<uint64_t>(value), out);
        return;
    }
    const int64_t signed_value = value;
    AppendTypeAndLength(1, static_cast<uint64_t>(-1 - signed_value), out);
}

bool IsContinuationByte(uint8_t value) { return (value & 0xc0U) == 0x80U; }

bool IsValidUtf8(std::string_view value) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(value.data());
    size_t index = 0;
    while (index < value.size()) {
        const uint8_t first = bytes[index];
        if (first <= 0x7fU) {
            ++index;
            continue;
        }

        if (first >= 0xc2U && first <= 0xdfU) {
            if (index + 1 >= value.size() ||
                !IsContinuationByte(bytes[index + 1])) {
                return false;
            }
            index += 2;
            continue;
        }

        if (first >= 0xe0U && first <= 0xefU) {
            if (index + 2 >= value.size() ||
                !IsContinuationByte(bytes[index + 1]) ||
                !IsContinuationByte(bytes[index + 2])) {
                return false;
            }
            if ((first == 0xe0U && bytes[index + 1] < 0xa0U) ||
                (first == 0xedU && bytes[index + 1] > 0x9fU)) {
                return false;
            }
            index += 3;
            continue;
        }

        if (first >= 0xf0U && first <= 0xf4U) {
            if (index + 3 >= value.size() ||
                !IsContinuationByte(bytes[index + 1]) ||
                !IsContinuationByte(bytes[index + 2]) ||
                !IsContinuationByte(bytes[index + 3])) {
                return false;
            }
            if ((first == 0xf0U && bytes[index + 1] < 0x90U) ||
                (first == 0xf4U && bytes[index + 1] > 0x8fU)) {
                return false;
            }
            index += 4;
            continue;
        }

        return false;
    }
    return true;
}

std::string Sha256(std::span<const uint8_t> input,
                   std::array<uint8_t, kSha256DigestSize>* digest) {
    using EvpContext = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;
    EvpContext context(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!context ||
        EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(context.get(), input.data(), input.size()) != 1) {
        return "OpenSSL EVP SHA-256 initialization failed";
    }

    unsigned int digest_size = 0;
    if (EVP_DigestFinal_ex(context.get(), digest->data(), &digest_size) != 1 ||
        digest_size != digest->size()) {
        return "OpenSSL EVP SHA-256 finalization failed";
    }
    return "";
}

int LowerHexValue(char value) {
    if (value >= '0' && value <= '9') {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
    }
    return -1;
}

std::string ValidateProfileSelectors(std::string_view strategy,
                                     std::string_view algorithm,
                                     std::string_view index_projection) {
    if (strategy != "vllm_v1") {
        return "unsupported hash strategy: " + std::string(strategy);
    }
    if (algorithm != "sha256_cbor") {
        return "unsupported hash algorithm: " + std::string(algorithm);
    }
    if (index_projection != "low64_be") {
        return "unsupported index projection: " + std::string(index_projection);
    }
    return "";
}

std::string ValidatePythonHashSeed(std::string_view seed) {
    if (!IsValidUtf8(seed)) {
        return "python_hash_seed must contain valid UTF-8";
    }
    if (seed == "random") {
        return "";
    }
    if (seed.empty()) {
        return "python_hash_seed must be \"random\" or ASCII decimal text in "
               "0..4294967295";
    }

    uint64_t value = 0;
    for (const char character : seed) {
        if (character < '0' || character > '9') {
            return "python_hash_seed must be \"random\" or ASCII decimal text "
                   "in 0..4294967295";
        }
        const uint64_t digit = static_cast<uint64_t>(character - '0');
        if (value > (kMaxPythonHashSeed - digit) / 10) {
            return "python_hash_seed must be in range 0..4294967295";
        }
        value = value * 10 + digit;
    }
    return "";
}

std::string ValidateRootDigest(std::string_view root_digest) {
    if (root_digest.size() != kSha256DigestSize * 2) {
        return "root_digest must contain exactly 64 lowercase hex characters";
    }
    for (const char value : root_digest) {
        if (LowerHexValue(value) < 0) {
            return "root_digest must contain exactly 64 lowercase hex "
                   "characters";
        }
    }
    return "";
}

std::string ValidateResolvedHashProfileShape(const HashProfile& profile) {
    if (auto error = ValidateProfileSelectors(
            profile.strategy, profile.algorithm, profile.index_projection);
        !error.empty()) {
        return error;
    }
    if (auto error = ValidatePythonHashSeed(profile.python_hash_seed);
        !error.empty()) {
        return error;
    }
    return ValidateRootDigest(profile.root_digest);
}

std::array<uint8_t, kSha256DigestSize> DecodeRootDigest(
    std::string_view root_digest) {
    std::array<uint8_t, kSha256DigestSize> result{};
    for (size_t index = 0; index < result.size(); ++index) {
        const int high = LowerHexValue(root_digest[index * 2]);
        const int low = LowerHexValue(root_digest[index * 2 + 1]);
        result[index] = static_cast<uint8_t>((high << 4) | low);
    }
    return result;
}

ProjectedPrefix ProjectDigest(
    const std::array<uint8_t, kSha256DigestSize>& digest) {
    uint64_t value = 0;
    for (size_t index = digest.size() - sizeof(value); index < digest.size();
         ++index) {
        value = (value << 8) | digest[index];
    }
    return ProjectedPrefix{value};
}

class VllmV1HashStrategy final : public HashStrategy {
   public:
    explicit VllmV1HashStrategy(
        std::array<uint8_t, kSha256DigestSize> root_digest)
        : root_digest_(std::move(root_digest)) {}

    std::string Compute(const ContextKey& context,
                        std::span<const int32_t> token_ids,
                        std::optional<std::string> cache_salt,
                        std::vector<HashBlock>* out) const override {
        if (out == nullptr) {
            return "hash output must not be null";
        }
        out->clear();

        if (context.block_size <= 0 ||
            static_cast<uint64_t>(context.block_size) >
                std::numeric_limits<size_t>::max()) {
            return "block_size must be a positive size_t value";
        }
        if (!IsValidUtf8(context.lora_name)) {
            return "lora_name must contain valid UTF-8";
        }
        if (cache_salt.has_value() && !IsValidUtf8(*cache_salt)) {
            return "cache_salt must contain valid UTF-8";
        }

        const size_t block_size = static_cast<size_t>(context.block_size);
        const size_t block_count = token_ids.size() / block_size;
        std::vector<HashBlock> computed;
        computed.reserve(block_count);

        std::array<uint8_t, kSha256DigestSize> parent = root_digest_;
        for (size_t block_index = 0; block_index < block_count; ++block_index) {
            std::vector<uint8_t> encoded;
            AppendArrayHeader(3, &encoded);
            AppendBytes(parent, &encoded);

            AppendArrayHeader(block_size, &encoded);
            const size_t token_offset = block_index * block_size;
            for (size_t token_index = 0; token_index < block_size;
                 ++token_index) {
                AppendSignedInteger(token_ids[token_offset + token_index],
                                    &encoded);
            }

            const bool has_lora = !context.lora_name.empty();
            const bool has_salt = block_index == 0 && cache_salt.has_value() &&
                                  !cache_salt->empty();
            if (!has_lora && !has_salt) {
                encoded.push_back(0xf6U);
            } else {
                AppendArrayHeader(static_cast<size_t>(has_lora) +
                                      static_cast<size_t>(has_salt),
                                  &encoded);
                if (has_lora) {
                    AppendText(context.lora_name, &encoded);
                }
                if (has_salt) {
                    AppendText(*cache_salt, &encoded);
                }
            }

            HashBlock block;
            if (std::string error = Sha256(encoded, &block.digest);
                !error.empty()) {
                return error;
            }
            block.projected = ProjectDigest(block.digest);
            parent = block.digest;
            computed.push_back(std::move(block));
        }

        *out = std::move(computed);
        return "";
    }

   private:
    std::array<uint8_t, kSha256DigestSize> root_digest_;
};

}  // namespace

std::string ResolveHashProfile(const common::HashProfileConfig& config,
                               HashProfile* out) {
    if (out == nullptr) {
        return "resolved hash profile output must not be null";
    }
    *out = {};

    if (auto error = ValidateProfileSelectors(config.strategy, config.algorithm,
                                              config.index_projection);
        !error.empty()) {
        return error;
    }
    if (auto error = ValidatePythonHashSeed(config.python_hash_seed);
        !error.empty()) {
        return error;
    }

    std::vector<uint8_t> encoded_seed;
    AppendText(config.python_hash_seed, &encoded_seed);
    std::array<uint8_t, kSha256DigestSize> root_digest{};
    if (auto error = Sha256(encoded_seed, &root_digest); !error.empty()) {
        return error;
    }

    *out = {.strategy = config.strategy,
            .algorithm = config.algorithm,
            .python_hash_seed = config.python_hash_seed,
            .root_digest = DigestToHex(root_digest),
            .index_projection = config.index_projection};
    return "";
}

std::string ValidateHashProfile(const HashProfile& profile) {
    if (auto error = ValidateResolvedHashProfileShape(profile);
        !error.empty()) {
        return error;
    }

    HashProfile expected;
    const common::HashProfileConfig source{
        .strategy = profile.strategy,
        .algorithm = profile.algorithm,
        .python_hash_seed = profile.python_hash_seed,
        .index_projection = profile.index_projection,
    };
    if (auto error = ResolveHashProfile(source, &expected); !error.empty()) {
        return error;
    }
    if (profile.root_digest != expected.root_digest) {
        return "root_digest does not match python_hash_seed and hash selectors";
    }
    return "";
}

std::unique_ptr<HashStrategy> CreateHashStrategy(const HashProfile& profile,
                                                 std::string* error) {
    const std::string validation_error =
        ValidateResolvedHashProfileShape(profile);
    if (error != nullptr) {
        *error = validation_error;
    }
    if (!validation_error.empty()) {
        return nullptr;
    }
    return std::make_unique<VllmV1HashStrategy>(
        DecodeRootDigest(profile.root_digest));
}

std::string DigestToHex(const std::array<uint8_t, 32>& digest) {
    static constexpr char kHexDigits[] = "0123456789abcdef";
    std::string result;
    result.resize(digest.size() * 2);
    for (size_t index = 0; index < digest.size(); ++index) {
        result[index * 2] = kHexDigits[digest[index] >> 4];
        result[index * 2 + 1] = kHexDigits[digest[index] & 0x0fU];
    }
    return result;
}

}  // namespace prefixindex
}  // namespace conductor
