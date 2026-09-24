// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "flagcx_transport_internal.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <limits>

namespace mooncake::flagcx_internal {
namespace {

TEST(FlagCxTransportInternalTest, ContainsRangeAcceptsContainedRanges) {
    EXPECT_TRUE(containsRange(100, 64, 100, 64));
    EXPECT_TRUE(containsRange(100, 64, 116, 32));
    EXPECT_TRUE(containsRange(100, 64, 164, 0));
}

TEST(FlagCxTransportInternalTest, ContainsRangeRejectsOutsideRanges) {
    EXPECT_FALSE(containsRange(100, 64, 99, 1));
    EXPECT_FALSE(containsRange(100, 64, 116, 49));
    EXPECT_FALSE(containsRange(100, 64, 165, 0));
}

TEST(FlagCxTransportInternalTest, ContainsRangeDoesNotOverflow) {
    constexpr uintptr_t kMax = std::numeric_limits<uintptr_t>::max();
    EXPECT_TRUE(containsRange(kMax - 7, 7, kMax - 3, 3));
    EXPECT_FALSE(containsRange(kMax - 7, 7, kMax - 3, 4));
}

TEST(FlagCxTransportInternalTest, DescriptorLengthCapsWithoutWrapping) {
    constexpr size_t kMaxDescriptor = std::numeric_limits<uint32_t>::max();
    EXPECT_EQ(descriptorLength(0), 0u);
    EXPECT_EQ(descriptorLength(1024), 1024u);
    EXPECT_EQ(descriptorLength(kMaxDescriptor),
              std::numeric_limits<uint32_t>::max());
    if constexpr (std::numeric_limits<size_t>::max() > kMaxDescriptor) {
        EXPECT_EQ(descriptorLength(kMaxDescriptor + 1),
                  std::numeric_limits<uint32_t>::max());
        EXPECT_EQ(descriptorLength(std::numeric_limits<size_t>::max()),
                  std::numeric_limits<uint32_t>::max());
    }
}

}  // namespace
}  // namespace mooncake::flagcx_internal
