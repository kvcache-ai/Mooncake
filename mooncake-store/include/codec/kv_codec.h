#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace mooncake::codec {

enum class Dtype : uint8_t { Float16 = 1, BFloat16 = 2 };

// Contiguous, row-major CPU memory. Shape includes any K/V/component axes;
// their meaning and the logical cache identity remain the caller's concern.
struct TensorDesc {
    Dtype dtype;
    std::vector<uint64_t> shape;
    bool operator==(const TensorDesc&) const = default;
};

class InvalidRecord : public std::runtime_error {
   public:
    using std::runtime_error::runtime_error;
};

// Synchronous transforms. Buffers must not overlap and must remain alive and
// unmodified by other threads until return. Encode returns actual record bytes.
// Invalid inputs throw invalid_argument; corrupt records throw InvalidRecord.
class KVCodec {
   public:
    virtual ~KVCodec() = default;
    virtual std::string FormatId() const = 0;
    virtual size_t EncodedSize(const TensorDesc& desc) const = 0;
    virtual size_t Encode(const TensorDesc& desc, std::span<const uint8_t> src,
                          std::span<uint8_t> dst) const = 0;
    virtual void Decode(std::span<const uint8_t> src, const TensorDesc& desc,
                        std::span<uint8_t> dst) const = 0;
};

// E4M3FN, round-to-nearest-even, one FP32 scale per consecutive group of
// flattened elements (including a short final group). No calibration state.
// CPU reference implementation; does not reduce GPU offload traffic.
class ScaledFp8Codec final : public KVCodec {
   public:
    explicit ScaledFp8Codec(uint32_t group_size = 128);
    std::string FormatId() const override;
    size_t EncodedSize(const TensorDesc& desc) const override;
    size_t Encode(const TensorDesc& desc, std::span<const uint8_t> src,
                  std::span<uint8_t> dst) const override;
    void Decode(std::span<const uint8_t> src, const TensorDesc& desc,
                std::span<uint8_t> dst) const override;

   private:
    uint32_t group_size_;
};

}  // namespace mooncake::codec
