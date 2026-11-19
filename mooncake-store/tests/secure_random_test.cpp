// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "random.h"

#include <array>
#include <algorithm>
#include <gtest/gtest.h>

namespace mooncake {
TEST(SecureRandom, FillsAcrossOsCallLimitsAndDoesNotUseSeededPrng) {
    std::array<std::byte, 1031> first{}, second{};
    threadLocalRandomEngine().seed(42);
    ASSERT_TRUE(secureRandomBytes(first));
    threadLocalRandomEngine().seed(42);
    ASSERT_TRUE(secureRandomBytes(second));
    EXPECT_NE(first, second);
    EXPECT_TRUE(std::any_of(first.begin(), first.begin() + 256,
                            [](auto b) { return b != std::byte{0}; }));
    EXPECT_TRUE(std::any_of(first.begin() + 1024, first.end(),
                            [](auto b) { return b != std::byte{0}; }));
}
TEST(SecureRandom, EmptyRequestSucceeds) { EXPECT_TRUE(secureRandomBytes({})); }
}  // namespace mooncake
