#include "crc_checksum.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string_view>
#include <vector>

namespace mooncake {
namespace {

// Reference implementation: the textbook bit-at-a-time definition of
// CRC-64/ECMA-182. Deliberately independent of the production tables so that a
// mistake in the slicing-by-8 tables cannot be masked by a shared bug.
uint64_t ReferenceCrc64Ecma(const uint8_t* data, size_t size) {
    constexpr uint64_t kPolynomial = 0x42F0E1EBA9EA3693ULL;
    uint64_t crc = 0;
    for (size_t i = 0; i < size; ++i) {
        crc ^= static_cast<uint64_t>(data[i]) << 56;
        for (int bit = 0; bit < 8; ++bit) {
            crc =
                (crc & (1ULL << 63)) != 0 ? (crc << 1) ^ kPolynomial : crc << 1;
        }
    }
    return crc;
}

std::vector<uint8_t> MakePattern(size_t size) {
    std::vector<uint8_t> buffer(size);
    for (size_t i = 0; i < size; ++i) {
        buffer[i] = static_cast<uint8_t>(i * 31 + 7);
    }
    return buffer;
}

}  // namespace

TEST(CrcChecksumTest, MatchesCrc64EcmaKnownVector) {
    constexpr std::string_view value = "123456789";
    EXPECT_EQ(ComputeCrcChecksum(value.data(), value.size()),
              0x6C40DF5F0B497347ULL);
}

TEST(CrcChecksumTest, StreamingMatchesContiguousForArbitraryLengths) {
    const std::array<uint8_t, 17> value = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
                                           0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
                                           0x0C, 0x0D, 0x0E, 0x0F, 0x10};

    CrcChecksum streaming;
    streaming.Update(value.data(), 3);
    streaming.Update(value.data() + 3, 7);
    streaming.Update(value.data() + 10, value.size() - 10);

    EXPECT_EQ(streaming.Finalize(),
              ComputeCrcChecksum(value.data(), value.size()));
    EXPECT_EQ(ComputeCrcChecksum(nullptr, 0), 0);
}

// Slicing-by-8 processes whole 8-byte groups and falls back to the
// byte-at-a-time loop for the remainder, so every length modulo 8 needs
// coverage, including the sizes just below and above the group width.
TEST(CrcChecksumTest, MatchesBitwiseReferenceForEveryTailLength) {
    const std::vector<uint8_t> buffer = MakePattern(129);
    for (size_t size = 0; size <= buffer.size(); ++size) {
        EXPECT_EQ(ComputeCrcChecksum(buffer.data(), size),
                  ReferenceCrc64Ecma(buffer.data(), size))
            << "size=" << size;
    }
}

// A split can leave the register mid-group, which forces the next Update to
// start with the byte-at-a-time path. The result must still match the
// contiguous computation for every split point.
TEST(CrcChecksumTest, StreamingMatchesContiguousForEverySplitPoint) {
    const std::vector<uint8_t> buffer = MakePattern(64);
    const uint64_t expected = ReferenceCrc64Ecma(buffer.data(), buffer.size());
    for (size_t split = 0; split <= buffer.size(); ++split) {
        CrcChecksum streaming;
        streaming.Update(buffer.data(), split);
        streaming.Update(buffer.data() + split, buffer.size() - split);
        EXPECT_EQ(streaming.Finalize(), expected) << "split=" << split;
    }
}

// The 8-byte load must not assume any particular alignment of the input.
TEST(CrcChecksumTest, MatchesBitwiseReferenceForUnalignedInput) {
    const std::vector<uint8_t> buffer = MakePattern(80);
    for (size_t offset = 1; offset < 8; ++offset) {
        const size_t size = buffer.size() - offset;
        EXPECT_EQ(ComputeCrcChecksum(buffer.data() + offset, size),
                  ReferenceCrc64Ecma(buffer.data() + offset, size))
            << "offset=" << offset;
    }
}

}  // namespace mooncake
