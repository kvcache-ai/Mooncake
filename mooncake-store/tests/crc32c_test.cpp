#include "crc32c.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#ifdef MOONCAKE_CRC32C_HAVE_SSE42
#include <cpuid.h>
#endif

namespace mooncake {
namespace {

// Bit-at-a-time CRC-32C, independent of the tables under test.
uint32_t ReferenceExtend(uint32_t crc, const uint8_t* p, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        crc ^= p[i];
        for (int k = 0; k < 8; ++k) {
            crc = (crc >> 1) ^ (0x82F63B78u & (0u - (crc & 1u)));
        }
    }
    return crc;
}

uint32_t ReferenceValue(const uint8_t* p, size_t n) {
    return ReferenceExtend(0xFFFFFFFFu, p, n) ^ 0xFFFFFFFFu;
}

bool CpuHasSse42() {
#ifdef MOONCAKE_CRC32C_HAVE_SSE42
    unsigned int eax = 0, ebx = 0, ecx = 0, edx = 0;
    return __get_cpuid(1, &eax, &ebx, &ecx, &edx) && (ecx & bit_SSE4_2) != 0;
#else
    return false;
#endif
}

struct Impl {
    const char* name;
    crc32c_detail::ExtendFn extend;
};

// Every implementation this host can run, so the slice-by-8 path is
// checked on x86 hosts too.
std::vector<Impl> Implementations() {
    std::vector<Impl> impls = {
        {"software", crc32c_detail::ExtendSoftware},
        {"dispatch", [](uint32_t crc, const uint8_t* p,
                        size_t n) { return crc32c_detail::Extend(crc, p, n); }},
    };
#ifdef MOONCAKE_CRC32C_HAVE_SSE42
    if (CpuHasSse42()) {
        impls.push_back({"sse42", crc32c_detail::ExtendSse42});
    }
#endif
    return impls;
}

uint32_t ValueOf(const Impl& impl, const uint8_t* p, size_t n) {
    return impl.extend(0xFFFFFFFFu, p, n) ^ 0xFFFFFFFFu;
}

std::vector<uint8_t> RandomBytes(size_t n, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::vector<uint8_t> bytes(n);
    for (auto& b : bytes) b = static_cast<uint8_t>(rng());
    return bytes;
}

// Picking the table path on an SSE4.2 CPU changes no value, only the speed,
// so only this test sees it.
TEST(Crc32cTest, SelectsSse42ExactlyWhenTheCpuHasIt) {
    const bool has_sse42 = CpuHasSse42();
    std::cout << "CPU has SSE4.2: " << (has_sse42 ? "yes" : "no") << "\n";
#ifdef MOONCAKE_CRC32C_HAVE_SSE42
    EXPECT_EQ(
        crc32c_detail::SelectExtend(),
        has_sse42 ? crc32c_detail::ExtendSse42 : crc32c_detail::ExtendSoftware);
#else
    EXPECT_EQ(crc32c_detail::SelectExtend(), crc32c_detail::ExtendSoftware);
#endif
    // Extend must call what SelectExtend chose.
    EXPECT_EQ(crc32c_detail::ActiveExtend(), crc32c_detail::SelectExtend());
}

// Check values from RFC 3720 appendix B.4 and the common "123456789" check.
TEST(Crc32cTest, KnownAnswers) {
    const std::string digits = "123456789";
    std::vector<uint8_t> zeros(32, 0x00);
    std::vector<uint8_t> ones(32, 0xFF);
    std::vector<uint8_t> ascending(32);
    std::vector<uint8_t> descending(32);
    for (size_t i = 0; i < 32; ++i) {
        ascending[i] = static_cast<uint8_t>(i);
        descending[i] = static_cast<uint8_t>(31 - i);
    }

    for (const auto& impl : Implementations()) {
        SCOPED_TRACE(impl.name);
        EXPECT_EQ(ValueOf(impl, nullptr, 0), 0x00000000u);
        EXPECT_EQ(ValueOf(impl, reinterpret_cast<const uint8_t*>(digits.data()),
                          digits.size()),
                  0xE3069283u);
        EXPECT_EQ(ValueOf(impl, zeros.data(), zeros.size()), 0x8A9136AAu);
        EXPECT_EQ(ValueOf(impl, ones.data(), ones.size()), 0x62A8AB43u);
        EXPECT_EQ(ValueOf(impl, ascending.data(), ascending.size()),
                  0x46DD794Eu);
        EXPECT_EQ(ValueOf(impl, descending.data(), descending.size()),
                  0x113FDB5Cu);
    }
    EXPECT_EQ(Crc32cValue(digits.data(), digits.size()), 0xE3069283u);
}

// Every length up to a few words past a page, at every start offset within
// a word, so the 8-byte loop and the byte tail are both exercised.
TEST(Crc32cTest, MatchesReferenceAtAllLengthsAndOffsets) {
    const auto bytes = RandomBytes(4096 + 64, 1);
    const auto impls = Implementations();
    for (size_t offset = 0; offset < 8; ++offset) {
        for (size_t len = 0; len <= 4096 + 40; ++len) {
            const uint8_t* p = bytes.data() + offset;
            const uint32_t expected = ReferenceValue(p, len);
            for (const auto& impl : impls) {
                ASSERT_EQ(ValueOf(impl, p, len), expected)
                    << impl.name << ": offset " << offset << " len " << len;
            }
        }
    }
}

TEST(Crc32cTest, MatchesReferenceOnLargeBuffer) {
    const auto bytes = RandomBytes((1 << 20) + 3, 2);
    const uint32_t expected = ReferenceValue(bytes.data(), bytes.size());
    for (const auto& impl : Implementations()) {
        SCOPED_TRACE(impl.name);
        EXPECT_EQ(ValueOf(impl, bytes.data(), bytes.size()), expected);
    }
    EXPECT_EQ(Crc32cValue(bytes.data(), bytes.size()), expected);
}

// The offset backend feeds header, key and value slices one Extend at a
// time; any split must give the one-shot value, in every implementation.
TEST(Crc32cTest, IncrementalExtendMatchesOneShot) {
    const auto bytes = RandomBytes(10000, 3);
    const uint32_t expected = ReferenceValue(bytes.data(), bytes.size());
    std::mt19937_64 rng(4);
    for (int round = 0; round < 200; ++round) {
        std::vector<size_t> pieces;
        for (size_t pos = 0; pos < bytes.size();) {
            pieces.push_back(std::min<size_t>(rng() % 70, bytes.size() - pos));
            pos += pieces.back();
        }
        for (const auto& impl : Implementations()) {
            SCOPED_TRACE(impl.name);
            uint32_t state = 0xFFFFFFFFu;
            size_t pos = 0;
            for (size_t piece : pieces) {
                state = impl.extend(state, bytes.data() + pos, piece);
                pos += piece;
            }
            ASSERT_EQ(state ^ 0xFFFFFFFFu, expected) << "round " << round;
        }
        Crc32c crc;
        size_t pos = 0;
        for (size_t piece : pieces) {
            crc.Extend(bytes.data() + pos, piece);
            pos += piece;
        }
        ASSERT_EQ(crc.Final(), expected) << "round " << round;
    }
}

}  // namespace
}  // namespace mooncake
