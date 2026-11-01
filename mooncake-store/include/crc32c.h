#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

// SYCL's device pass cannot query x86 CPU features.
#if defined(__x86_64__) && !defined(__SYCL_DEVICE_ONLY__) && \
    (defined(__GNUC__) || defined(__clang__))
#include <nmmintrin.h>
#define MOONCAKE_CRC32C_HAVE_SSE42 1
#endif

namespace mooncake {

namespace crc32c_detail {

inline constexpr uint32_t kPolynomial = 0x82F63B78u;

// Slice-by-8 tables. kTables[0] is the byte-at-a-time table; kTables[k]
// advances kTables[k - 1] by one more zero byte.
inline constexpr auto kTables = [] {
    std::array<std::array<uint32_t, 256>, 8> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? (kPolynomial ^ (c >> 1)) : (c >> 1);
        }
        t[0][i] = c;
    }
    for (size_t k = 1; k < t.size(); ++k) {
        for (uint32_t i = 0; i < 256; ++i) {
            t[k][i] = (t[k - 1][i] >> 8) ^ t[0][t[k - 1][i] & 0xFF];
        }
    }
    return t;
}();

// CRC-32C is reflected, so the first message byte goes into the least
// significant byte of the register. Written as explicit shifts to stay
// endian-independent; compilers fold this into one load on little-endian
// targets.
inline uint64_t LoadLittleEndian64(const uint8_t* p) {
    return static_cast<uint64_t>(p[0]) | (static_cast<uint64_t>(p[1]) << 8) |
           (static_cast<uint64_t>(p[2]) << 16) |
           (static_cast<uint64_t>(p[3]) << 24) |
           (static_cast<uint64_t>(p[4]) << 32) |
           (static_cast<uint64_t>(p[5]) << 40) |
           (static_cast<uint64_t>(p[6]) << 48) |
           (static_cast<uint64_t>(p[7]) << 56);
}

// Portable path. The eight table lookups of a step are independent, so the
// CPU can issue them in parallel.
inline uint32_t ExtendSoftware(uint32_t crc, const uint8_t* p, size_t n) {
    while (n >= 8) {
        const uint64_t w = LoadLittleEndian64(p) ^ crc;
        crc = kTables[7][w & 0xFF] ^ kTables[6][(w >> 8) & 0xFF] ^
              kTables[5][(w >> 16) & 0xFF] ^ kTables[4][(w >> 24) & 0xFF] ^
              kTables[3][(w >> 32) & 0xFF] ^ kTables[2][(w >> 40) & 0xFF] ^
              kTables[1][(w >> 48) & 0xFF] ^ kTables[0][w >> 56];
        p += 8;
        n -= 8;
    }
    while (n > 0) {
        crc = kTables[0][(crc ^ *p++) & 0xFF] ^ (crc >> 8);
        --n;
    }
    return crc;
}

#ifdef MOONCAKE_CRC32C_HAVE_SSE42
// SSE4.2 crc32 instruction. Compiled for any x86-64 target and selected at
// run time, so binaries built without -msse4.2 still use it.
__attribute__((target("sse4.2"))) inline uint32_t ExtendSse42(uint32_t crc,
                                                              const uint8_t* p,
                                                              size_t n) {
    uint64_t c = crc;
    while (n >= 8) {
        c = _mm_crc32_u64(c, LoadLittleEndian64(p));
        p += 8;
        n -= 8;
    }
    uint32_t c32 = static_cast<uint32_t>(c);
    while (n > 0) {
        c32 = _mm_crc32_u8(c32, *p++);
        --n;
    }
    return c32;
}
#endif

using ExtendFn = uint32_t (*)(uint32_t crc, const uint8_t* p, size_t n);

// The fastest implementation this CPU supports. __builtin_cpu_init makes
// the check safe from static initializers too.
inline ExtendFn SelectExtend() {
#ifdef MOONCAKE_CRC32C_HAVE_SSE42
    __builtin_cpu_init();
    if (__builtin_cpu_supports("sse4.2")) return ExtendSse42;
#endif
    return ExtendSoftware;
}

// The implementation Extend uses, chosen once.
inline ExtendFn ActiveExtend() {
    static const ExtendFn extend = SelectExtend();
    return extend;
}

inline uint32_t Extend(uint32_t crc, const void* data, size_t len) {
    return ActiveExtend()(crc, static_cast<const uint8_t*>(data), len);
}

}  // namespace crc32c_detail

// CRC-32C (Castagnoli), reflected polynomial 0x82F63B78.
// Uses the SSE4.2 crc32 instruction when the CPU has it, otherwise a
// slice-by-8 table. Incremental usage:
//   Crc32c crc;
//   crc.Extend(p1, n1);
//   crc.Extend(p2, n2);
//   uint32_t value = crc.Final();
class Crc32c {
   public:
    Crc32c() : value_(kInit) {}

    void Extend(const void* data, size_t len) {
        value_ = crc32c_detail::Extend(value_, data, len);
    }

    uint32_t Final() const { return value_ ^ kInit; }

   private:
    static constexpr uint32_t kInit = 0xFFFFFFFFu;

    uint32_t value_;
};

inline uint32_t Crc32cValue(const void* data, size_t len) {
    Crc32c crc;
    crc.Extend(data, len);
    return crc.Final();
}

}  // namespace mooncake
