// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace {

struct BufferRange {
    uint64_t addr;
    uint64_t length;
};

// Mirrors TcpTransport::validateAddress logic exactly.
bool validateAddress(uint64_t addr, uint64_t size,
                     const std::vector<BufferRange>& buffers) {
    if (size == 0) return false;
    if (addr + size < addr) return false;

    for (const auto& buffer : buffers) {
        if (buffer.addr + buffer.length < buffer.addr) continue;
        if (buffer.addr <= addr && addr + size <= buffer.addr + buffer.length)
            return true;
    }
    return false;
}

class TcpAddressValidationTest : public ::testing::Test {
   protected:
    std::vector<BufferRange> buffers_;

    void SetUp() override {
        buffers_ = {
            {0x1000, 0x2000},     // [0x1000, 0x3000)
            {0x10000, 0x100000},  // [0x10000, 0x110000)
        };
    }
};

TEST_F(TcpAddressValidationTest, ExactMatch) {
    EXPECT_TRUE(validateAddress(0x1000, 0x2000, buffers_));
    EXPECT_TRUE(validateAddress(0x10000, 0x100000, buffers_));
}

TEST_F(TcpAddressValidationTest, WithinBounds) {
    EXPECT_TRUE(validateAddress(0x1000, 0x100, buffers_));
    EXPECT_TRUE(validateAddress(0x1500, 0x500, buffers_));
    EXPECT_TRUE(validateAddress(0x2FFF, 1, buffers_));
    EXPECT_TRUE(validateAddress(0x50000, 0x1000, buffers_));
}

TEST_F(TcpAddressValidationTest, OutOfBounds) {
    EXPECT_FALSE(validateAddress(0x500, 0x100, buffers_));
    EXPECT_FALSE(validateAddress(0x3000, 0x100, buffers_));
    EXPECT_FALSE(validateAddress(0x5000, 0x1000, buffers_));
    EXPECT_FALSE(validateAddress(0x200000, 0x100, buffers_));
}

TEST_F(TcpAddressValidationTest, PartialOverlap) {
    EXPECT_FALSE(validateAddress(0x2F00, 0x200, buffers_));
    EXPECT_FALSE(validateAddress(0x0F00, 0x200, buffers_));
    EXPECT_FALSE(validateAddress(0x10F000, 0x2000, buffers_));
}

TEST_F(TcpAddressValidationTest, ZeroSize) {
    EXPECT_FALSE(validateAddress(0x1000, 0, buffers_));
    EXPECT_FALSE(validateAddress(0x0, 0, buffers_));
}

TEST_F(TcpAddressValidationTest, IntegerOverflow) {
    EXPECT_FALSE(validateAddress(UINT64_MAX, 1, buffers_));
    EXPECT_FALSE(validateAddress(UINT64_MAX - 10, 100, buffers_));
    EXPECT_FALSE(validateAddress(1, UINT64_MAX, buffers_));
}

TEST_F(TcpAddressValidationTest, EmptyBufferList) {
    std::vector<BufferRange> empty;
    EXPECT_FALSE(validateAddress(0x1000, 0x100, empty));
}

TEST_F(TcpAddressValidationTest, SingleByteAccess) {
    EXPECT_TRUE(validateAddress(0x1000, 1, buffers_));
    EXPECT_TRUE(validateAddress(0x2FFF, 1, buffers_));
    EXPECT_FALSE(validateAddress(0x3000, 1, buffers_));
    EXPECT_FALSE(validateAddress(0x0FFF, 1, buffers_));
}

TEST_F(TcpAddressValidationTest, AdjacentBuffers) {
    std::vector<BufferRange> adjacent = {
        {0x1000, 0x1000},  // [0x1000, 0x2000)
        {0x2000, 0x1000},  // [0x2000, 0x3000)
    };
    EXPECT_TRUE(validateAddress(0x1000, 0x1000, adjacent));
    EXPECT_TRUE(validateAddress(0x2000, 0x1000, adjacent));
    // Spanning two buffers should fail
    EXPECT_FALSE(validateAddress(0x1800, 0x1000, adjacent));
}

TEST_F(TcpAddressValidationTest, LargeBuffer) {
    std::vector<BufferRange> large = {
        {0, UINT64_MAX},
    };
    EXPECT_TRUE(validateAddress(0, 1, large));
    EXPECT_TRUE(validateAddress(0, UINT64_MAX, large));
    EXPECT_TRUE(validateAddress(UINT64_MAX - 1, 1, large));
}

}  // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
