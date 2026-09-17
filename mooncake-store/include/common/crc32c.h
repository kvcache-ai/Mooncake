#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace mooncake {

// CRC-32C (Castagnoli), reflected polynomial 0x82F63B78.
// Table-driven software implementation; incremental usage:
//   Crc32c crc;
//   crc.Extend(p1, n1);
//   crc.Extend(p2, n2);
//   uint32_t value = crc.Final();
class Crc32c {
   public:
    Crc32c() : value_(kInit) {}

    void Extend(const void* data, size_t len) {
        const auto* p = static_cast<const uint8_t*>(data);
        uint32_t v = value_;
        for (size_t i = 0; i < len; ++i) {
            v = kTable[(v ^ p[i]) & 0xFF] ^ (v >> 8);
        }
        value_ = v;
    }

    uint32_t Final() const { return value_ ^ kInit; }

   private:
    static constexpr uint32_t kInit = 0xFFFFFFFFu;
    static constexpr std::array<uint32_t, 256> kTable = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
            }
            t[i] = c;
        }
        return t;
    }();

    uint32_t value_;
};

inline uint32_t Crc32cValue(const void* data, size_t len) {
    Crc32c crc;
    crc.Extend(data, len);
    return crc.Final();
}

}  // namespace mooncake
