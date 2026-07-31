#include "crc_checksum.h"

#include <array>

namespace mooncake {

namespace {

constexpr uint64_t kCrc64EcmaPolynomial = 0x42F0E1EBA9EA3693ULL;

constexpr std::array<uint64_t, 256> MakeCrc64EcmaTable() {
    std::array<uint64_t, 256> table{};
    for (size_t i = 0; i < table.size(); ++i) {
        uint64_t crc = static_cast<uint64_t>(i) << 56;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & (1ULL << 63)) != 0 ? (crc << 1) ^ kCrc64EcmaPolynomial
                                            : crc << 1;
        }
        table[i] = crc;
    }
    return table;
}

constexpr auto kCrc64EcmaTable = MakeCrc64EcmaTable();

}  // namespace

void CrcChecksum::Update(const void* data, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < size; ++i) {
        const auto index = static_cast<uint8_t>((crc_ >> 56) ^ bytes[i]);
        crc_ = kCrc64EcmaTable[index] ^ (crc_ << 8);
    }
}

uint64_t ComputeCrcChecksum(const void* data, size_t size) {
    CrcChecksum checksum;
    checksum.Update(data, size);
    return checksum.Finalize();
}

}  // namespace mooncake
