// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_streaming_config.h"

#include <gtest/gtest.h>

namespace mooncake::ssd_offload {

TEST(SsdStreamingConfig, DefaultsToLegacyAndDoesNotValidateUnusedLimits) {
    StreamingReadConfig config;
    EXPECT_FALSE(config.enabled);
    config.slot_bytes = 0;
    config.slots = 0;
    EXPECT_TRUE(config.Validate(0));
}

TEST(SsdStreamingConfig, EnabledDefaultsFitExistingArena) {
    StreamingReadConfig config;
    config.enabled = true;
    EXPECT_TRUE(config.Validate(1280ULL * 1024 * 1024));
    EXPECT_FALSE(config.Validate(4096));
}

TEST(SsdStreamingConfig, PushIsIndependentOptInWithValidatedLimits) {
    StreamingReadConfig config;
    EXPECT_FALSE(config.push_enabled);
    config.push_enabled = true;
    EXPECT_FALSE(config.enabled);
    EXPECT_TRUE(config.Validate(1280ULL * 1024 * 1024));
    EXPECT_FALSE(config.Validate(4096));
    config.slot_bytes = 0;
    EXPECT_FALSE(config.Validate(UINT64_MAX));
}
TEST(SsdStreamingConfig, PushModeDefaultFits128PageMhaBatch) {
    StreamingReadConfig config;
    EXPECT_EQ(config.DefaultMaxOutstanding(), 64u);
    EXPECT_EQ(config.ProtocolLimits().max_objects, 64u);
    config.enabled = true;
    EXPECT_EQ(config.DefaultMaxOutstanding(), 64u);
    config.push_enabled = true;
    EXPECT_EQ(config.DefaultMaxOutstanding(), 512u);
    EXPECT_EQ(config.ProtocolLimits().max_objects, 512u);
    EXPECT_EQ(SsdReadProtocolLimits{}.max_objects, 64u);
}

template <typename Config>
bool OldStreamingAggregateWorks() {
    if constexpr (requires {
                      Config{true,      size_t(4096), size_t(2),   size_t(1),
                             size_t(1), size_t(8),    size_t(100), size_t(200)};
                  }) {
        const Config config{true,      size_t(4096), size_t(2),   size_t(1),
                            size_t(1), size_t(8),    size_t(100), size_t(200)};
        return config.enabled && !config.push_enabled &&
               config.slot_bytes == 4096 && config.slots == 2 &&
               config.read_workers == 1 && config.write_workers == 1 &&
               config.max_outstanding == 8 && config.max_chunks == 100 &&
               config.max_fragments == 200;
    }
    return false;
}
TEST(SsdStreamingConfig, PreservesExistingPositionalAggregateInitialization) {
    EXPECT_TRUE(OldStreamingAggregateWorks<StreamingReadConfig>());
}

TEST(SsdStreamingConfig, RejectsInvalidEnabledLimits) {
    StreamingReadConfig config;
    config.enabled = true;
    for (size_t field = 0; field < 7; ++field) {
        auto bad = config;
        if (field == 0) bad.slot_bytes = 4097;
        if (field == 1) bad.slots = 0;
        if (field == 2) bad.read_workers = 0;
        if (field == 3) bad.write_workers = 65;
        if (field == 4) bad.max_outstanding = 1;
        if (field == 5) bad.max_chunks = 0;
        if (field == 6) bad.max_fragments = 0;
        EXPECT_FALSE(bad.Validate(UINT64_MAX)) << field;
    }
}

}  // namespace mooncake::ssd_offload
