#include "utils/crc32c_util.h"

#include <array>

namespace mooncake {

namespace {
constexpr uint32_t kCrc32cPolynomial = 0x82F63B78u;

const uint32_t* GetCrc32cTable() {
    static const std::array<uint32_t, 256> table = []() {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t crc = i;
            for (int j = 0; j < 8; ++j) {
                if (crc & 1u) {
                    crc = (crc >> 1) ^ kCrc32cPolynomial;
                } else {
                    crc >>= 1;
                }
            }
            t[i] = crc;
        }
        return t;
    }();
    return table.data();
}
}  // namespace

uint32_t Crc32c(const uint8_t* data, size_t len) {
    const uint32_t* table = GetCrc32cTable();
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return ~crc;
}

uint32_t Crc32c(const std::vector<uint8_t>& data) {
    if (data.empty()) {
        return Crc32c(nullptr, 0);
    }
    return Crc32c(data.data(), data.size());
}

}  // namespace mooncake
