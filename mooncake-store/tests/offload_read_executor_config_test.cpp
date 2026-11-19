// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <string>

#include "offload_read_executor_config.h"

namespace mooncake::detail {

TEST(OffloadReadExecutorConfig, DefaultsTo64WhenUnsetOrEmpty) {
    EXPECT_EQ(kDefaultOffloadReadIoThreads, 64u);
    EXPECT_EQ(ParseOffloadReadIoThreads(nullptr).value(), 64u);
    EXPECT_EQ(ParseOffloadReadIoThreads("").value(), 64u);
}

TEST(OffloadReadExecutorConfig, AcceptsExplicitThreadCounts) {
    for (uint32_t count : {1u, 16u, 32u, 64u, 96u, 128u}) {
        const auto parsed =
            ParseOffloadReadIoThreads(std::to_string(count).c_str());
        ASSERT_TRUE(parsed.has_value());
        EXPECT_EQ(*parsed, count);
    }
}

TEST(OffloadReadExecutorConfig, PreservesWhitespaceAndPlusParsing) {
    EXPECT_EQ(ParseOffloadReadIoThreads(" +32 \t").value(), 32u);
    EXPECT_EQ(ParseOffloadReadIoThreads(" 64 ").value(), 64u);
}

TEST(OffloadReadExecutorConfig, RejectsInvalidValuesAndFallsBackTo64) {
    for (const char* value : {"0", "-1", "129", "abc", "1.5", "64threads", " ",
                              "+", "++64", "+-64", "4294967296"}) {
        const auto parsed = ParseOffloadReadIoThreads(value);
        EXPECT_FALSE(parsed.has_value()) << value;
        EXPECT_EQ(parsed.value_or(kDefaultOffloadReadIoThreads), 64u) << value;
    }
}

}  // namespace mooncake::detail
