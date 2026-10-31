// Copyright 2026 KVCache.AI
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

#include "bench_config.h"

#include <limits>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace tent {
namespace {

class BenchConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Flag defaults from utils.cpp, as an initiator.
        XferBenchConfig::target_seg_name = "127.0.0.1:12345";
        XferBenchConfig::op_type = "read";
        XferBenchConfig::check_consistency = false;
        XferBenchConfig::target_access = "fixed";
        XferBenchConfig::target_window_size = 0;
        XferBenchConfig::total_buffer_size = 1UL << 30;
        XferBenchConfig::start_block_size = 4096;
        XferBenchConfig::max_block_size = 1UL << 26;
        XferBenchConfig::start_batch_size = 1;
        XferBenchConfig::max_batch_size = 1;
        XferBenchConfig::duration = 5;
        XferBenchConfig::start_num_threads = 1;
        XferBenchConfig::max_num_threads = 1;
        XferBenchConfig::qos_classes.clear();
        XferBenchConfig::qos_classes_json.clear();
        XferBenchConfig::workload_classes_json.clear();
        XferBenchConfig::qos_link_capacity_gbps = 0.0;
        XferBenchConfig::deadline_us = 0;
        XferBenchConfig::deadline_tight_threads = 0;
        XferBenchConfig::backend = "tent";
        XferBenchConfig::xport_type = "";
        XferBenchConfig::use_hugepage = false;
        XferBenchConfig::hugepage_size = 0;
        XferBenchConfig::tent_intent_type = "unspec";
    }

    std::string validateError() {
        BenchConfig config;
        std::string error;
        EXPECT_FALSE(validateConfig(&config, &error));
        return error;
    }

    BenchConfig validateOk() {
        BenchConfig config;
        std::string error;
        EXPECT_TRUE(validateConfig(&config, &error)) << error;
        return config;
    }
};

TEST_F(BenchConfigTest, DefaultsAreValid) {
    const auto config = validateOk();
    EXPECT_EQ(config.op_mode.opcode, READ);
    EXPECT_FALSE(config.op_mode.mixed_opcode);
    EXPECT_FALSE(config.op_mode.random_access);
    EXPECT_TRUE(config.qos_classes.empty());
    EXPECT_TRUE(config.workload_classes.empty());
}

TEST_F(BenchConfigTest, ParsesOpTypes) {
    XferBenchConfig::op_type = "write";
    EXPECT_EQ(validateOk().op_mode.opcode, WRITE);

    XferBenchConfig::op_type = "mix";
    EXPECT_TRUE(validateOk().op_mode.mixed_opcode);

    XferBenchConfig::op_type = "write_seed";
    auto mode = validateOk().op_mode;
    EXPECT_EQ(mode.opcode, WRITE);
    EXPECT_TRUE(mode.write_seed);

    XferBenchConfig::op_type = "read_verify";
    mode = validateOk().op_mode;
    EXPECT_EQ(mode.opcode, READ);
    EXPECT_TRUE(mode.read_verify);

    XferBenchConfig::op_type = "read";
    XferBenchConfig::check_consistency = true;
    EXPECT_TRUE(validateOk().op_mode.mixed_opcode);
}

TEST_F(BenchConfigTest, RejectsBadOpType) {
    XferBenchConfig::op_type = "copy";
    EXPECT_EQ(validateError(),
              "Invalid args: workload only support "
              "read|write|mix|write_seed|read_verify");

    XferBenchConfig::op_type = "read_verify";
    XferBenchConfig::check_consistency = true;
    EXPECT_EQ(validateError(),
              "--check_consistency cannot be combined with "
              "write_seed/read_verify");
}

TEST_F(BenchConfigTest, ValidatesTargetAccess) {
    XferBenchConfig::target_access = "stride";
    EXPECT_EQ(validateError(),
              "Invalid args: --target_access only supports fixed|random");

    XferBenchConfig::target_access = "fixed";
    XferBenchConfig::target_window_size = 8192;
    EXPECT_EQ(validateError(),
              "--target_window_size requires --target_access=random");

    XferBenchConfig::target_access = "random";
    XferBenchConfig::target_window_size = 4097;
    EXPECT_EQ(validateError(),
              "--target_window_size must be a multiple of 4096");

    XferBenchConfig::target_window_size = 8192;
    EXPECT_TRUE(validateOk().op_mode.random_access);

    XferBenchConfig::op_type = "write_seed";
    EXPECT_EQ(validateError(),
              "--target_access=random cannot be combined with "
              "write_seed/read_verify");
}

TEST_F(BenchConfigTest, RejectsZeroSweepStart) {
    XferBenchConfig::start_num_threads = 0;
    EXPECT_EQ(validateError(), "--start_num_threads must be positive");
    XferBenchConfig::start_num_threads = 1;

    XferBenchConfig::start_block_size = 0;
    EXPECT_EQ(validateError(), "--start_block_size must be positive");
    XferBenchConfig::start_block_size = 4096;

    XferBenchConfig::start_batch_size = 0;
    EXPECT_EQ(validateError(), "--start_batch_size must be positive");
}

TEST_F(BenchConfigTest, RejectsSweepStartAboveMax) {
    XferBenchConfig::start_num_threads = 4;
    XferBenchConfig::max_num_threads = 2;
    EXPECT_EQ(validateError(),
              "--start_num_threads (4) must not exceed --max_num_threads (2)");
    XferBenchConfig::start_num_threads = 1;

    XferBenchConfig::start_block_size = 8192;
    XferBenchConfig::max_block_size = 4096;
    EXPECT_EQ(validateError(),
              "--start_block_size (8192) must not exceed --max_block_size "
              "(4096)");
    XferBenchConfig::start_block_size = 4096;

    XferBenchConfig::start_batch_size = 2;
    XferBenchConfig::max_batch_size = 1;
    EXPECT_EQ(validateError(),
              "--start_batch_size (2) must not exceed --max_batch_size (1)");
}

TEST_F(BenchConfigTest, RejectsSweepMaxThatOverflowsDoubling) {
    constexpr int kThreadLimit = std::numeric_limits<int>::max() / 2;
    XferBenchConfig::max_num_threads = kThreadLimit;
    validateOk();
    XferBenchConfig::max_num_threads = kThreadLimit + 1;
    EXPECT_EQ(validateError(),
              "--max_num_threads (" + std::to_string(kThreadLimit + 1) +
                  ") must be at most " + std::to_string(kThreadLimit));
    XferBenchConfig::max_num_threads = 1;

    constexpr size_t kSizeLimit = std::numeric_limits<size_t>::max() / 2;
    XferBenchConfig::max_block_size = kSizeLimit;
    validateOk();
    XferBenchConfig::max_block_size = kSizeLimit + 1;
    EXPECT_EQ(validateError(),
              "--max_block_size (" + std::to_string(kSizeLimit + 1) +
                  ") must be at most " + std::to_string(kSizeLimit));
    XferBenchConfig::max_block_size = 1UL << 26;

    XferBenchConfig::max_batch_size = kSizeLimit;
    EXPECT_EQ(validateError(),
              "--max_block_size x --max_batch_size overflows "
              "(67108864 x " +
                  std::to_string(kSizeLimit) + ")");
    XferBenchConfig::max_block_size = 4096;
    XferBenchConfig::max_batch_size = kSizeLimit + 1;
    EXPECT_EQ(validateError(),
              "--max_batch_size (" + std::to_string(kSizeLimit + 1) +
                  ") must be at most " + std::to_string(kSizeLimit));
}

TEST_F(BenchConfigTest, RejectsBlockTimesBatchOverflow) {
    // Each max is within its doubling limit, but the product overflows.
    XferBenchConfig::max_block_size = 1UL << 32;
    XferBenchConfig::max_batch_size = 1UL << 32;
    EXPECT_EQ(validateError(),
              "--max_block_size x --max_batch_size overflows (4294967296 x "
              "4294967296)");

    // A product just below 2^64 is accepted.
    XferBenchConfig::max_block_size = 1UL << 31;
    XferBenchConfig::max_batch_size = (1UL << 33) - 1;
    validateOk();
}

TEST_F(BenchConfigTest, RejectsNonPositiveDuration) {
    XferBenchConfig::duration = 0;
    EXPECT_EQ(validateError(), "--duration must be positive");
    XferBenchConfig::duration = -1;
    EXPECT_EQ(validateError(), "--duration must be positive");
}

TEST_F(BenchConfigTest, TargetSkipsInitiatorFlags) {
    XferBenchConfig::target_seg_name.clear();
    XferBenchConfig::op_type = "copy";
    XferBenchConfig::start_block_size = 0;
    XferBenchConfig::duration = 0;
    validateOk();

    XferBenchConfig::deadline_us = 10;
    XferBenchConfig::backend = "classic";
    EXPECT_EQ(validateError(),
              "deadline tagging is supported only by the tent backend");
}

TEST_F(BenchConfigTest, ValidatesHugepage) {
    XferBenchConfig::use_hugepage = true;
    EXPECT_EQ(validateError(),
              "--use_hugepage requires --backend=classic and "
              "--xport_type=shm or rdma");

    XferBenchConfig::backend = "classic";
    XferBenchConfig::xport_type = "shm";
    validateOk();

    XferBenchConfig::hugepage_size = static_cast<size_t>(-1);
    EXPECT_EQ(validateError(),
              "--hugepage_size must be 2MB, 512MB, 1GB, or a byte count "
              "(2097152, 536870912, 1073741824)");

    XferBenchConfig::hugepage_size = 4096;
    EXPECT_EQ(validateError(),
              "--hugepage_size must be 2MB, 512MB, 1GB, or a byte count "
              "(2097152, 536870912, 1073741824)");

    XferBenchConfig::hugepage_size = 0;
    XferBenchConfig::total_buffer_size = (2UL << 20) + 4096;
    EXPECT_EQ(validateError(),
              "--total_buffer_size must be a multiple of hugepage_size");
}

TEST_F(BenchConfigTest, ParsesQosClasses) {
    XferBenchConfig::start_num_threads = 2;
    XferBenchConfig::max_num_threads = 2;
    XferBenchConfig::qos_classes = "hi:1:100:2,lo:1:1000:1";
    const auto config = validateOk();
    ASSERT_EQ(config.qos_classes.size(), 2u);
    EXPECT_EQ(config.qos_classes[0].name, "hi");

    XferBenchConfig::qos_classes_json = "[]";
    EXPECT_EQ(validateError(),
              "Use only one of --qos_classes or --qos_classes_json");

    XferBenchConfig::qos_classes_json.clear();
    XferBenchConfig::max_num_threads = 4;
    EXPECT_EQ(validateError(),
              "QoS metrics require start_num_threads == max_num_threads");
}

TEST_F(BenchConfigTest, RejectsWorkloadWithQosFlags) {
    XferBenchConfig::workload_classes_json = "[]";
    XferBenchConfig::qos_classes = "hi:1:100:1";
    EXPECT_EQ(validateError(),
              "Use --workload_classes_json or QoS class flags, not both");
}

TEST_F(BenchConfigTest, ValidatesDeadlineFlags) {
    XferBenchConfig::qos_link_capacity_gbps = -1.0;
    EXPECT_EQ(validateError(),
              "qos_link_capacity_gbps must be finite and non-negative");
    XferBenchConfig::qos_link_capacity_gbps = 0.0;

    XferBenchConfig::deadline_tight_threads = 2;
    EXPECT_EQ(validateError(),
              "deadline_tight_threads must be in [0, max_num_threads]");
}

TEST(PickTargetSlotTest, NeverRepeatsPreviousSlot) {
    std::mt19937_64 rng(1);
    constexpr uint64_t kSlots = 4;
    uint64_t prev = kSlots;
    std::vector<int> hits(kSlots, 0);
    for (int i = 0; i < 1000; ++i) {
        const uint64_t slot = pickTargetSlot(rng, kSlots, prev);
        ASSERT_LT(slot, kSlots);
        ASSERT_NE(slot, prev);
        ++hits[slot];
        prev = slot;
    }
    for (int count : hits) EXPECT_GT(count, 0);
}

TEST(PickTargetSlotTest, TwoSlotsAlternate) {
    std::mt19937_64 rng(7);
    uint64_t prev = pickTargetSlot(rng, 2, 2);
    for (int i = 0; i < 10; ++i) {
        const uint64_t slot = pickTargetSlot(rng, 2, prev);
        EXPECT_EQ(slot, 1 - prev);
        prev = slot;
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
