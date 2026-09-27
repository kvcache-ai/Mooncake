#include "codec/kv_codec.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>

namespace mooncake::codec {
namespace {

// v1, little-endian: magic[8], version:u16, dtype:u8, rank:u8,
// group_size:u32, numel:u64, crc32:u32, reserved:u32, shape[rank]:u64,
// scales[ceil(numel/group_size)]:f32, payload[numel]:e4m3fn.
// CRC32/IEEE covers the entire record with bytes 24..27 treated as zero.
constexpr size_t kHeader = 32;
constexpr std::array<uint8_t, 8> kMagic{'M', 'C', 'K', 'V', 'F', 'P', '8', 0};
static_assert(sizeof(float) == 4 && std::numeric_limits<float>::is_iec559);

void Write(std::span<uint8_t> dst, size_t offset, uint64_t value, size_t n) {
    for (size_t i = 0; i < n; ++i) dst[offset + i] = value >> (8 * i);
}

uint64_t Read(std::span<const uint8_t> src, size_t offset, size_t n) {
    uint64_t value = 0;
    for (size_t i = 0; i < n; ++i)
        value |= uint64_t(src[offset + i]) << (8 * i);
    return value;
}

uint32_t Checksum(std::span<const uint8_t> bytes) {
    uint32_t crc = 0xffffffff;
    for (size_t i = 0; i < bytes.size(); ++i) {
        crc ^= (i >= 24 && i < 28) ? 0 : bytes[i];
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1)));
    }
    return ~crc;
}

size_t Numel(const TensorDesc& desc) {
    if (desc.dtype != Dtype::Float16 && desc.dtype != Dtype::BFloat16)
        throw std::invalid_argument("Only FP16/BF16 tensors are supported");
    if (desc.shape.empty() || desc.shape.size() > 8)
        throw std::invalid_argument("Tensor rank must be between 1 and 8");
    size_t n = 1;
    for (auto dim : desc.shape) {
        if (!dim || dim > std::numeric_limits<size_t>::max() / 2 / n)
            throw std::invalid_argument("Empty or overflowing tensor shape");
        n *= dim;
    }
    return n;
}

void CheckOverlap(std::span<const uint8_t> src, std::span<uint8_t> dst) {
    const auto a = reinterpret_cast<uintptr_t>(src.data());
    const auto b = reinterpret_cast<uintptr_t>(dst.data());
    if (!src.empty() && !dst.empty() &&
        (a <= b ? b - a < src.size() : a - b < dst.size()))
        throw std::invalid_argument("Codec buffers must not overlap");
}

float Load(std::span<const uint8_t> src, size_t index, Dtype dtype) {
    uint16_t bits;
    std::memcpy(&bits, src.data() + index * 2, 2);
    if (dtype == Dtype::BFloat16)
        return std::bit_cast<float>(uint32_t(bits) << 16);
    const int exponent = (bits >> 10) & 31;
    const int mantissa = bits & 1023;
    float value = exponent == 0
                      ? std::ldexp(float(mantissa), -24)
                      : std::ldexp(float(1024 + mantissa), exponent - 25);
    if (exponent == 31) value = std::numeric_limits<float>::infinity();
    return bits & 0x8000 ? -value : value;
}

// Round an unsigned integer right shift to nearest, ties to even.
uint32_t RoundShift(uint32_t bits, unsigned shift) {
    const uint32_t base = bits >> shift;
    const uint32_t remainder = bits & ((uint32_t{1} << shift) - 1);
    const uint32_t half = uint32_t{1} << (shift - 1);
    return base + (remainder > half || (remainder == half && (base & 1)));
}

void Store(std::span<uint8_t> dst, size_t index, float value, Dtype dtype) {
    const uint32_t bits = std::bit_cast<uint32_t>(value);
    uint16_t result;
    if (dtype == Dtype::BFloat16) {
        result = RoundShift(bits, 16);
    } else {
        const uint32_t magnitude = bits & 0x7fffffff;
        const int exponent = int(magnitude >> 23) - 127;
        uint32_t rounded = 0;
        if (exponent >= -14) {
            rounded = RoundShift(magnitude - (112U << 23), 13);
        } else if (exponent >= -25) {
            rounded = RoundShift((magnitude & 0x7fffff) | 0x800000,
                                 unsigned(-exponent - 1));
        }
        result = uint16_t((bits >> 16) & 0x8000) | uint16_t(rounded);
    }
    std::memcpy(dst.data() + index * 2, &result, 2);
}

const std::array<float, 127>& Fp8Values() {
    static const auto values = [] {
        std::array<float, 127> table{};
        for (int i = 0; i < 127; ++i)
            table[i] = i < 8 ? std::ldexp(float(i), -9)
                             : std::ldexp(float(8 + (i & 7)), (i >> 3) - 10);
        return table;
    }();
    return values;
}

uint8_t Quantize(float value, float scale) {
    const auto& values = Fp8Values();
    // Normalize in FP32 before E4M3FN rounding, matching tensor FP8 casts.
    const double magnitude = std::min(std::abs(value / scale), 448.0f);
    auto upper = std::lower_bound(values.begin(), values.end(), magnitude);
    size_t code = size_t(upper - values.begin());
    if (code > 0) {
        const double low_distance = magnitude - values[code - 1];
        const double high_distance = values[code] - magnitude;
        if (low_distance < high_distance ||
            (low_distance == high_distance && (code & 1)))
            --code;
    }
    return uint8_t(code) | (std::signbit(value) ? 0x80 : 0);
}

}  // namespace

ScaledFp8Codec::ScaledFp8Codec(uint32_t group_size) : group_size_(group_size) {
    if (!group_size_)
        throw std::invalid_argument("group_size must be positive");
}

std::string ScaledFp8Codec::FormatId() const {
    return "fp8-e4m3fn-v1-g" + std::to_string(group_size_);
}

size_t ScaledFp8Codec::EncodedSize(const TensorDesc& desc) const {
    const size_t n = Numel(desc);
    const size_t groups = (n - 1) / group_size_ + 1;
    const size_t header = kHeader + desc.shape.size() * 8;
    if (groups > (std::numeric_limits<size_t>::max() - header - n) / 4)
        throw std::invalid_argument("Encoded tensor size overflows");
    return header + groups * 4 + n;
}

size_t ScaledFp8Codec::Encode(const TensorDesc& desc,
                              std::span<const uint8_t> src,
                              std::span<uint8_t> dst) const {
    const size_t size = EncodedSize(desc);
    const size_t n = Numel(desc);
    if (src.size() != n * 2 || dst.size() < size)
        throw std::invalid_argument("Invalid source size or output capacity");
    CheckOverlap(src, dst);
    dst = dst.first(size);
    std::fill(dst.begin(), dst.begin() + kHeader, 0);
    std::copy(kMagic.begin(), kMagic.end(), dst.begin());
    Write(dst, 8, 1, 2);
    dst[10] = uint8_t(desc.dtype);
    dst[11] = uint8_t(desc.shape.size());
    Write(dst, 12, group_size_, 4);
    Write(dst, 16, n, 8);
    for (size_t d = 0; d < desc.shape.size(); ++d)
        Write(dst, kHeader + d * 8, desc.shape[d], 8);
    const size_t scales = kHeader + desc.shape.size() * 8;
    const size_t groups = (n - 1) / group_size_ + 1;
    const size_t payload = scales + groups * 4;
    for (size_t group = 0; group < groups; ++group) {
        const size_t begin = group * size_t(group_size_);
        const size_t end = begin + std::min(size_t(group_size_), n - begin);
        float maximum = 0;
        for (size_t i = begin; i < end; ++i) {
            const float value = Load(src, i, desc.dtype);
            if (!std::isfinite(value))
                throw std::invalid_argument("Non-finite input is unsupported");
            maximum = std::max(maximum, std::abs(value));
        }
        const float scale = maximum == 0 ? 1.0f : maximum / 448.0f;
        Write(dst, scales + group * 4, std::bit_cast<uint32_t>(scale), 4);
        for (size_t i = begin; i < end; ++i)
            dst[payload + i] = Quantize(Load(src, i, desc.dtype), scale);
    }
    Write(dst, 24, Checksum(dst), 4);
    return size;
}

void ScaledFp8Codec::Decode(std::span<const uint8_t> src,
                            const TensorDesc& desc,
                            std::span<uint8_t> dst) const {
    const size_t expected_size = EncodedSize(desc);
    const size_t n = Numel(desc);
    if (dst.size() < n * 2)
        throw std::invalid_argument("Output capacity is too small");
    CheckOverlap(src, dst);
    if (src.size() < kHeader ||
        !std::equal(kMagic.begin(), kMagic.end(), src.begin()) ||
        Read(src, 8, 2) != 1 || Read(src, 28, 4) != 0)
        throw InvalidRecord("Invalid record header or unsupported version");
    if (Read(src, 24, 4) != Checksum(src))
        throw InvalidRecord("Record checksum mismatch");
    if (src[10] != uint8_t(desc.dtype) || src[11] != desc.shape.size() ||
        Read(src, 12, 4) != group_size_ || Read(src, 16, 8) != n)
        throw std::invalid_argument("Record dtype, shape or codec mismatch");
    if (src.size() != expected_size)
        throw InvalidRecord("Truncated record or trailing bytes");
    for (size_t d = 0; d < desc.shape.size(); ++d)
        if (Read(src, kHeader + d * 8, 8) != desc.shape[d])
            throw std::invalid_argument("Record shape mismatch");
    const size_t scales = kHeader + desc.shape.size() * 8;
    const size_t groups = (n - 1) / group_size_ + 1;
    const size_t payload = scales + groups * 4;
    // Validate all side data before modifying any destination bytes.
    for (size_t group = 0; group < groups; ++group) {
        const float scale =
            std::bit_cast<float>(uint32_t(Read(src, scales + group * 4, 4)));
        if (!std::isfinite(scale) || scale <= 0)
            throw InvalidRecord("Invalid FP8 scale");
    }
    for (size_t i = 0; i < n; ++i)
        if ((src[payload + i] & 127) == 127)
            throw InvalidRecord("Non-finite FP8 payload");
    const double max_value =
        desc.dtype == Dtype::Float16
            ? 65504.0
            : double(std::bit_cast<float>(uint32_t{0x7f7f0000}));
    for (size_t group = 0; group < groups; ++group) {
        const float scale =
            std::bit_cast<float>(uint32_t(Read(src, scales + group * 4, 4)));
        const size_t begin = group * size_t(group_size_);
        const size_t end = begin + std::min(size_t(group_size_), n - begin);
        for (size_t i = begin; i < end; ++i) {
            const uint8_t code = src[payload + i];
            const float value = float(
                std::min(double(Fp8Values()[code & 127]) * scale, max_value));
            Store(dst, i, code & 128 ? -value : value, desc.dtype);
        }
    }
}

}  // namespace mooncake::codec
