#include "conductor/prefixindex/hash_strategy.h"

// Two SHA-256 backends, selected by CONDUCTOR_HAS_LOWLEVEL_SHA256 (probed by
// CMake): the low-level SHA256_* API with a stack context is ~3x cheaper per
// call than EVP per-call setup on ~100-byte block encodings, but its
// declarations are hidden in OpenSSL no-deprecated builds and it bypasses
// the provider framework required by FIPS.  Both backends dispatch to the
// same OpenSSL implementation and produce byte-identical digests.
#if CONDUCTOR_HAS_LOWLEVEL_SHA256
#define OPENSSL_SUPPRESS_DEPRECATED
#include <openssl/sha.h>
#else
#include <openssl/evp.h>
#endif

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

// ---------------------------------------------------------------------------
// Recipe boundary
//
// The vLLM v1 chain (complete-block selection, full-32-byte parent
// advancement, SHA-256 digesting, and the low64_be projection) is shared by
// every supported algorithm.  Each algorithm name selects exactly one value
// codec that owns serialization of the seed root and of each
// (parent, token tuple, extra keys) block value.  The codec receives the
// already-computed extra-key ordering (non-empty LoRA on every block,
// non-empty cache salt after LoRA only on the first block) and never sees
// value shapes outside the Conductor query contract.
//
// Multimodal and prompt-embedding vLLM extra keys are deliberately NOT
// representable here: the Conductor query API cannot express them, so the
// codecs reject that shape by construction instead of approximating it.
// ---------------------------------------------------------------------------

struct VllmBlockValues {
    std::span<const uint8_t> parent_digest;  // full 32-byte parent digest
    std::span<const int32_t> token_ids;      // exactly one complete block
    const std::string* lora_name;            // nullptr when no LoRA extra key
    const std::string* cache_salt;           // nullptr when no salt extra key
};

class VllmValueCodec {
   public:
    virtual ~VllmValueCodec() = default;

    virtual void EncodeSeed(std::string_view seed,
                            std::vector<uint8_t>* out) const = 0;
    virtual void EncodeBlock(const VllmBlockValues& values,
                             std::vector<uint8_t>* out) const = 0;
};

// ---------------------------------------------------------------------------
// Canonical-CBOR codec (sha256_cbor)
// ---------------------------------------------------------------------------

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

class CborVllmCodec final : public VllmValueCodec {
   public:
    void EncodeSeed(std::string_view seed,
                    std::vector<uint8_t>* out) const override {
        out->clear();
        AppendText(seed, out);
    }

    void EncodeBlock(const VllmBlockValues& values,
                     std::vector<uint8_t>* out) const override {
        out->clear();
        AppendArrayHeader(3, out);
        AppendBytes(values.parent_digest, out);

        AppendArrayHeader(values.token_ids.size(), out);
        for (const int32_t token : values.token_ids) {
            AppendSignedInteger(token, out);
        }

        const bool has_lora = values.lora_name != nullptr;
        const bool has_salt = values.cache_salt != nullptr;
        if (!has_lora && !has_salt) {
            out->push_back(0xf6U);
        } else {
            AppendArrayHeader(
                static_cast<size_t>(has_lora) + static_cast<size_t>(has_salt),
                out);
            if (has_lora) {
                AppendText(*values.lora_name, out);
            }
            if (has_salt) {
                AppendText(*values.cache_salt, out);
            }
        }
    }
};

// ---------------------------------------------------------------------------
// CPython Pickle protocol-5 codec (sha256)
//
// Restricted encoder for the value types the Conductor query contract can
// express: UTF-8 seed strings, full parent bytes, signed int32 token IDs,
// None, tuple containers, and LoRA/cache-salt strings.  It reproduces the
// CPython pickler byte-for-byte for those shapes:
//   * \x80\x05 protocol header and protocol-4+ framing (frames committed at
//     the start of every object save once the pending frame reaches 64 KiB,
//     and one final forced frame before/around STOP);
//   * SHORT_BINUNICODE/BINUNICODE/BINUNICODE8 and SHORT_BINBYTES/BINBYTES/
//     BINBYTES8 length thresholds;
//   * BININT1/BININT2/BININT integer thresholds;
//   * EMPTY_TUPLE/TUPLE1/TUPLE2/TUPLE3/MARK+TUPLE arity opcodes;
//   * MEMOIZE markers after every non-empty bytes/str/tuple object;
//   * the \x2e STOP terminator inside the final frame.
//
// Values are always emitted fresh (no BINGET back-references).  That matches
// CPython whenever the memoized objects are distinct, which is the only case
// the Conductor contract can produce; Python object-identity aliasing between
// equal strings is an explicitly unsupported shape, as are multimodal and
// prompt-embedding extra-key object graphs.
// ---------------------------------------------------------------------------

constexpr uint8_t kPickleMark = 0x28;             // MARK
constexpr uint8_t kPickleStop = 0x2e;             // STOP
constexpr uint8_t kPickleEmptyTuple = 0x29;       // EMPTY_TUPLE
constexpr uint8_t kPickleBinbytes = 0x42;         // BINBYTES
constexpr uint8_t kPickleShortBinbytes = 0x43;    // SHORT_BINBYTES
constexpr uint8_t kPickleBinint = 0x4a;           // BININT
constexpr uint8_t kPickleBinint1 = 0x4b;          // BININT1
constexpr uint8_t kPickleBinint2 = 0x4d;          // BININT2
constexpr uint8_t kPickleNone = 0x4e;             // NONE
constexpr uint8_t kPickleBinunicode = 0x58;       // BINUNICODE
constexpr uint8_t kPickleTuple = 0x74;            // TUPLE
constexpr uint8_t kPickleProto = 0x80;            // PROTO
constexpr uint8_t kPickleTuple1 = 0x85;           // TUPLE1
constexpr uint8_t kPickleTuple2 = 0x86;           // TUPLE2
constexpr uint8_t kPickleTuple3 = 0x87;           // TUPLE3
constexpr uint8_t kPickleShortBinunicode = 0x8c;  // SHORT_BINUNICODE
constexpr uint8_t kPickleBinunicode8 = 0x8d;      // BINUNICODE8
constexpr uint8_t kPickleBinbytes8 = 0x8e;        // BINBYTES8
constexpr uint8_t kPickleMemoize = 0x94;          // MEMOIZE
constexpr uint8_t kPickleFrame = 0x95;            // FRAME
constexpr uint8_t kPickleProtocol5 = 0x05;

// CPython _Framer._FRAME_SIZE_TARGET: a pending frame is committed before the
// next object save once it reaches this size.
constexpr size_t kPickleFrameTarget = 64 * 1024;

// Emulates CPython's protocol-4+ _Framer: object bytes accumulate in
// frame_bytes_; Checkpoint() flushes a full frame at the start of each object
// save, and Finish() emits the final forced frame.
class PickleStream {
   public:
    // Bytes written before framing starts (the protocol header).
    void WriteRaw(uint8_t value) { out_.push_back(value); }

    void Write(uint8_t value) { frame_.push_back(value); }

    void Write(std::span<const uint8_t> bytes) {
        frame_.insert(frame_.end(), bytes.begin(), bytes.end());
    }

    // Mirrors _Framer.commit_frame() at the start of Pickler.save().
    void Checkpoint() {
        if (frame_.size() >= kPickleFrameTarget) {
            FlushFrame();
        }
    }

    // Mirrors _Framer.write_large_bytes: the current frame is force-committed
    // and the large payload is written with its length header but without a
    // frame opcode.  `header` is the already-packed little-endian length.
    void WriteLargePayload(uint8_t opcode, std::span<const uint8_t> header,
                           std::span<const uint8_t> payload) {
        FlushFrame();
        out_.push_back(opcode);
        out_.insert(out_.end(), header.begin(), header.end());
        out_.insert(out_.end(), payload.begin(), payload.end());
    }

    // Mirrors _Framer.end_framing(): force-commit whatever remains.
    void Finish() { FlushFrame(); }

    const std::vector<uint8_t>& bytes() const { return out_; }

   private:
    void FlushFrame() {
        if (frame_.empty()) {
            return;
        }
        out_.push_back(kPickleFrame);
        AppendLittleEndian(static_cast<uint64_t>(frame_.size()), &out_);
        out_.insert(out_.end(), frame_.begin(), frame_.end());
        frame_.clear();
    }

    static void AppendLittleEndian(uint64_t value, std::vector<uint8_t>* out) {
        for (int shift = 0; shift < 64; shift += 8) {
            out->push_back(static_cast<uint8_t>(value >> shift));
        }
    }

    std::vector<uint8_t> out_;
    std::vector<uint8_t> frame_;
};

void PickleAppendLittleEndian(uint64_t value, size_t byte_count,
                              std::vector<uint8_t>* out) {
    for (size_t index = 0; index < byte_count; ++index) {
        out->push_back(static_cast<uint8_t>(value >> (index * 8)));
    }
}

void PickleEncodeBytes(std::span<const uint8_t> value, PickleStream* stream) {
    const uint64_t length = value.size();
    if (length <= 0xffU) {
        stream->Write(kPickleShortBinbytes);
        stream->Write(static_cast<uint8_t>(length));
        stream->Write(value);
    } else if (length > std::numeric_limits<uint32_t>::max()) {
        std::vector<uint8_t> header;
        PickleAppendLittleEndian(length, 8, &header);
        stream->WriteLargePayload(kPickleBinbytes8, header, value);
    } else if (length >= kPickleFrameTarget) {
        std::vector<uint8_t> header;
        PickleAppendLittleEndian(length, 4, &header);
        stream->WriteLargePayload(kPickleBinbytes, header, value);
    } else {
        stream->Write(kPickleBinbytes);
        for (int shift = 0; shift < 32; shift += 8) {
            stream->Write(static_cast<uint8_t>(length >> shift));
        }
        stream->Write(value);
    }
    stream->Write(kPickleMemoize);
}

void PickleEncodeString(std::string_view value, PickleStream* stream) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(value.data());
    const std::span<const uint8_t> encoded(bytes, value.size());
    const uint64_t length = encoded.size();
    if (length <= 0xffU) {
        stream->Write(kPickleShortBinunicode);
        stream->Write(static_cast<uint8_t>(length));
        stream->Write(encoded);
    } else if (length > std::numeric_limits<uint32_t>::max()) {
        std::vector<uint8_t> header;
        PickleAppendLittleEndian(length, 8, &header);
        stream->WriteLargePayload(kPickleBinunicode8, header, encoded);
    } else if (length >= kPickleFrameTarget) {
        std::vector<uint8_t> header;
        PickleAppendLittleEndian(length, 4, &header);
        stream->WriteLargePayload(kPickleBinunicode, header, encoded);
    } else {
        stream->Write(kPickleBinunicode);
        for (int shift = 0; shift < 32; shift += 8) {
            stream->Write(static_cast<uint8_t>(length >> shift));
        }
        stream->Write(encoded);
    }
    stream->Write(kPickleMemoize);
}

void PickleEncodeInt(int32_t value, PickleStream* stream) {
    if (value >= 0 && value <= 0xff) {
        stream->Write(kPickleBinint1);
        stream->Write(static_cast<uint8_t>(value));
        return;
    }
    if (value >= 0 && value <= 0xffff) {
        stream->Write(kPickleBinint2);
        const uint16_t narrow = static_cast<uint16_t>(value);
        stream->Write(static_cast<uint8_t>(narrow));
        stream->Write(static_cast<uint8_t>(narrow >> 8));
        return;
    }
    stream->Write(kPickleBinint);
    const uint32_t bits = static_cast<uint32_t>(value);
    for (int shift = 0; shift < 32; shift += 8) {
        stream->Write(static_cast<uint8_t>(bits >> shift));
    }
}

// Encodes the token tuple: per-element save checkpoints, the CPython tuple
// arity opcodes, and the trailing MEMOIZE marker for non-empty tuples.
void PickleEncodeTokenTuple(std::span<const int32_t> tokens,
                            PickleStream* stream) {
    if (tokens.empty()) {
        stream->Write(kPickleEmptyTuple);
        return;
    }
    if (tokens.size() > 3) {
        stream->Write(kPickleMark);
    }
    for (const int32_t token : tokens) {
        stream->Checkpoint();
        PickleEncodeInt(token, stream);
    }
    switch (tokens.size()) {
        case 1:
            stream->Write(kPickleTuple1);
            break;
        case 2:
            stream->Write(kPickleTuple2);
            break;
        case 3:
            stream->Write(kPickleTuple3);
            break;
        default:
            stream->Write(kPickleTuple);
            break;
    }
    stream->Write(kPickleMemoize);
}

// Encodes the extras slot: Python None when there are no extra keys,
// otherwise the (LoRA, salt) string tuple in vLLM's ordering.  The caller
// must have executed the save-entry checkpoint for this value already.
void PickleEncodeExtras(const VllmBlockValues& values, PickleStream* stream) {
    const bool has_lora = values.lora_name != nullptr;
    const bool has_salt = values.cache_salt != nullptr;
    if (!has_lora && !has_salt) {
        stream->Write(kPickleNone);
        return;
    }
    if (has_lora) {
        stream->Checkpoint();
        PickleEncodeString(*values.lora_name, stream);
    }
    if (has_salt) {
        stream->Checkpoint();
        PickleEncodeString(*values.cache_salt, stream);
    }
    stream->Write(has_lora && has_salt ? kPickleTuple2 : kPickleTuple1);
    stream->Write(kPickleMemoize);
}

class PickleVllmCodec final : public VllmValueCodec {
   public:
    void EncodeSeed(std::string_view seed,
                    std::vector<uint8_t>* out) const override {
        PickleStream stream;
        stream.WriteRaw(kPickleProto);
        stream.WriteRaw(kPickleProtocol5);
        stream.Checkpoint();
        PickleEncodeString(seed, &stream);
        stream.Write(kPickleStop);
        stream.Finish();
        *out = stream.bytes();
    }

    void EncodeBlock(const VllmBlockValues& values,
                     std::vector<uint8_t>* out) const override {
        PickleStream stream;
        stream.WriteRaw(kPickleProto);
        stream.WriteRaw(kPickleProtocol5);

        // Outer (parent, tokens, extras) tuple: TUPLE3.  Each Checkpoint()
        // mirrors Pickler.save() entry for the corresponding value.
        stream.Checkpoint();  // save((parent, tokens, extras))
        stream.Checkpoint();  // save(parent bytes)
        PickleEncodeBytes(values.parent_digest, &stream);
        stream.Checkpoint();  // save(token tuple)
        PickleEncodeTokenTuple(values.token_ids, &stream);
        stream.Checkpoint();  // save(extras)
        PickleEncodeExtras(values, &stream);
        stream.Write(kPickleTuple3);
        stream.Write(kPickleMemoize);

        stream.Write(kPickleStop);
        stream.Finish();
        *out = stream.bytes();
    }
};

const VllmValueCodec* CodecForAlgorithm(std::string_view algorithm) {
    static const CborVllmCodec kCborCodec;
    static const PickleVllmCodec kPickleCodec;
    if (algorithm == "sha256_cbor") {
        return &kCborCodec;
    }
    if (algorithm == "sha256") {
        return &kPickleCodec;
    }
    return nullptr;
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

#if CONDUCTOR_HAS_LOWLEVEL_SHA256

std::string Sha256(std::span<const uint8_t> input,
                   std::array<uint8_t, kSha256DigestSize>* digest) {
    SHA256_CTX context;
    if (SHA256_Init(&context) != 1 ||
        SHA256_Update(&context, input.data(), input.size()) != 1 ||
        SHA256_Final(digest->data(), &context) != 1) {
        return "OpenSSL SHA-256 computation failed";
    }
    return "";
}

#else  // EVP backend: no-deprecated builds and FIPS-forced configurations.

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

// Same as Sha256 but reuses a caller-owned context across invocations,
// avoiding an EVP_MD_CTX allocation per hashed block in long hash chains.
std::string Sha256Reuse(EVP_MD_CTX* context, std::span<const uint8_t> input,
                        std::array<uint8_t, kSha256DigestSize>* digest) {
    if (EVP_MD_CTX_reset(context) != 1 ||
        EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(context, input.data(), input.size()) != 1) {
        return "OpenSSL EVP SHA-256 initialization failed";
    }

    unsigned int digest_size = 0;
    if (EVP_DigestFinal_ex(context, digest->data(), &digest_size) != 1 ||
        digest_size != digest->size()) {
        return "OpenSSL EVP SHA-256 finalization failed";
    }
    return "";
}

#endif

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
    if (algorithm != "sha256" && algorithm != "sha256_cbor") {
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

// Lazily-hashed vLLM v1 block chain. Hashing is incremental: block i is
// computed only when first requested via At(), and every block up to i is
// cached, so prefix-index walks that stall early never hash the tail.
class VllmV1HashChain final : public HashChain {
   public:
    VllmV1HashChain(const VllmValueCodec* codec,
                    std::array<uint8_t, kSha256DigestSize> root_digest,
                    const ContextKey& context,
                    std::span<const int32_t> token_ids,
                    std::optional<std::string> cache_salt)
        : codec_(codec),
          parent_(std::move(root_digest)),
          lora_name_(context.lora_name),
          cache_salt_(std::move(cache_salt)),
          token_ids_(token_ids),
          block_size_(static_cast<size_t>(context.block_size)),
          block_count_(token_ids.size() / block_size_) {
        computed_.reserve(block_count_);
        // Reused across blocks: every codec clears the buffer at the start of
        // EncodeBlock, so a single allocation with worst-case capacity avoids
        // a malloc/free per block (dominant cost at large block counts).
        encoded_.reserve(64 + block_size_ * 9);
    }

    // Validates inputs eagerly (same contract as Compute). Returns an empty
    // string on success.
    static std::string ValidateInputs(const ContextKey& context,
                                      std::optional<std::string> cache_salt) {
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
        return "";
    }

    size_t BlockCount() const override { return block_count_; }

    size_t ComputedCount() const override { return computed_.size(); }

    const HashBlock* At(size_t index, std::string* error) override {
        if (index >= block_count_) {
            if (error != nullptr) {
                *error = "hash chain index out of range";
            }
            return nullptr;
        }
        if (!sticky_error_.empty()) {
            if (error != nullptr) {
                *error = sticky_error_;
            }
            return nullptr;
        }
#if !CONDUCTOR_HAS_LOWLEVEL_SHA256
        if (!EnsureEvp()) {
            if (error != nullptr) {
                *error = sticky_error_;
            }
            return nullptr;
        }
#endif
        while (computed_.size() <= index) {
            const size_t block_index = computed_.size();
            const bool has_lora = !lora_name_.empty();
            const bool has_salt = block_index == 0 && cache_salt_.has_value() &&
                                  !cache_salt_->empty();
            const VllmBlockValues values{
                .parent_digest = parent_,
                .token_ids =
                    token_ids_.subspan(block_index * block_size_, block_size_),
                .lora_name = has_lora ? &lora_name_ : nullptr,
                .cache_salt = has_salt ? &*cache_salt_ : nullptr,
            };

            codec_->EncodeBlock(values, &encoded_);

            HashBlock block;
#if CONDUCTOR_HAS_LOWLEVEL_SHA256
            std::string hash_error = Sha256(encoded_, &block.digest);
#else
            std::string hash_error =
                Sha256Reuse(evp_.get(), encoded_, &block.digest);
#endif
            if (!hash_error.empty()) {
                sticky_error_ = std::move(hash_error);
                if (error != nullptr) {
                    *error = sticky_error_;
                }
                return nullptr;
            }
            block.projected = ProjectDigest(block.digest);
            parent_ = block.digest;
            computed_.push_back(std::move(block));
        }
        return &computed_[index];
    }

   private:
#if !CONDUCTOR_HAS_LOWLEVEL_SHA256
    bool EnsureEvp() {
        if (evp_) {
            return true;
        }
        evp_ = EvpContext(EVP_MD_CTX_new(), EVP_MD_CTX_free);
        if (!evp_) {
            sticky_error_ = "OpenSSL EVP MD context allocation failed";
            return false;
        }
        return true;
    }

    using EvpContext = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;
#endif

    const VllmValueCodec* codec_;
    std::array<uint8_t, kSha256DigestSize> parent_;
    std::string lora_name_;
    std::optional<std::string> cache_salt_;
    std::span<const int32_t> token_ids_;
    size_t block_size_;
    size_t block_count_;
    std::vector<HashBlock> computed_;
    std::vector<uint8_t> encoded_;
#if !CONDUCTOR_HAS_LOWLEVEL_SHA256
    EvpContext evp_{nullptr, EVP_MD_CTX_free};
#endif
    std::string sticky_error_;
};

class VllmV1HashStrategy final : public HashStrategy {
   public:
    VllmV1HashStrategy(const VllmValueCodec* codec,
                       std::array<uint8_t, kSha256DigestSize> root_digest)
        : codec_(codec), root_digest_(std::move(root_digest)) {}

    std::string Compute(const ContextKey& context,
                        std::span<const int32_t> token_ids,
                        std::optional<std::string> cache_salt,
                        std::vector<HashBlock>* out) const override {
        if (out == nullptr) {
            return "hash output must not be null";
        }
        out->clear();

        std::string error;
        auto chain =
            CreateChain(context, token_ids, std::move(cache_salt), &error);
        if (!chain) {
            return error;
        }
        std::vector<HashBlock> computed;
        computed.reserve(chain->BlockCount());
        for (size_t index = 0; index < chain->BlockCount(); ++index) {
            const HashBlock* block = chain->At(index, &error);
            if (block == nullptr) {
                return error;
            }
            computed.push_back(*block);
        }

        *out = std::move(computed);
        return "";
    }

    std::unique_ptr<HashChain> CreateChain(
        const ContextKey& context, std::span<const int32_t> token_ids,
        std::optional<std::string> cache_salt,
        std::string* error) const override {
        if (std::string validation_error =
                VllmV1HashChain::ValidateInputs(context, cache_salt);
            !validation_error.empty()) {
            if (error != nullptr) {
                *error = std::move(validation_error);
            }
            return nullptr;
        }
        return std::make_unique<VllmV1HashChain>(
            codec_, root_digest_, context, token_ids, std::move(cache_salt));
    }

   private:
    const VllmValueCodec* codec_;
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

    const VllmValueCodec* codec = CodecForAlgorithm(config.algorithm);
    if (codec == nullptr) {
        return "unsupported hash algorithm: " + config.algorithm;
    }

    std::vector<uint8_t> encoded_seed;
    codec->EncodeSeed(config.python_hash_seed, &encoded_seed);
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
    const VllmValueCodec* codec = CodecForAlgorithm(profile.algorithm);
    if (codec == nullptr) {
        if (error != nullptr) {
            *error = "unsupported hash algorithm: " + profile.algorithm;
        }
        return nullptr;
    }
    return std::make_unique<VllmV1HashStrategy>(
        codec, DecodeRootDigest(profile.root_digest));
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
