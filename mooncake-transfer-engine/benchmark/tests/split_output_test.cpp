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

#include "split_output.h"

#include <cstdlib>
#include <fstream>
#include <limits>
#include <string>
#include <thread>
#include <unistd.h>

#include <gtest/gtest.h>

namespace mooncake {
namespace tent {
namespace {

std::string makeTempPath() {
    std::string path = testing::TempDir() + "tebench-split-XXXXXX";
    const int fd = mkstemp(path.data());
    EXPECT_GE(fd, 0);
    if (fd >= 0) close(fd);
    return path;
}

TEST(SplitOutputTest, FormatsJsonLineInMicroseconds) {
    char buf[256];
    size_t n = 0;
    SplitXferSample sample{4096, 207360, false, 66874123, 1152760007, 201};
    ASSERT_TRUE(formatSplitXferLine(buf, sizeof(buf), sample, &n));
    EXPECT_EQ(std::string(buf, n),
              "{\"block_size\":4096,\"batch_size\":207360,\"warmup\":false,"
              "\"submit_us\":66874.123,\"wait_us\":1152760.007,\"polls\":201}"
              "\n");
}

TEST(SplitOutputTest, FormatsSubMicrosecondAndWarmup) {
    char buf[256];
    size_t n = 0;
    SplitXferSample sample{65536, 1, true, 950, 40, 1};
    ASSERT_TRUE(formatSplitXferLine(buf, sizeof(buf), sample, &n));
    EXPECT_EQ(std::string(buf, n),
              "{\"block_size\":65536,\"batch_size\":1,\"warmup\":true,"
              "\"submit_us\":0.950,\"wait_us\":0.040,\"polls\":1}\n");
}

TEST(SplitOutputTest, FitsLargestValues) {
    char buf[256];
    size_t n = 0;
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    SplitXferSample sample{max, max, false, max, max, max};
    EXPECT_TRUE(formatSplitXferLine(buf, sizeof(buf), sample, &n));
}

TEST(SplitOutputTest, RejectsTruncatedBuffer) {
    char buf[8];
    size_t n = 99;
    SplitXferSample sample{1, 2, false, 3, 4, 5};
    EXPECT_FALSE(formatSplitXferLine(buf, sizeof(buf), sample, &n));
}

TEST(SplitOutputTest, WarmupFlagIsPerThread) {
    EXPECT_FALSE(splitWarmup());
    setSplitWarmup(true);
    EXPECT_TRUE(splitWarmup());
    bool other_thread = true;
    std::thread([&] { other_thread = splitWarmup(); }).join();
    EXPECT_FALSE(other_thread);
    setSplitWarmup(false);
    EXPECT_FALSE(splitWarmup());
}

TEST(SplitOutputTest, DisabledLogIsNoOp) {
    EXPECT_FALSE(splitOutputEnabled());
    logSplitXfer({1, 2, false, 3, 4, 5});
    EXPECT_FALSE(splitOutputEnabled());
}

TEST(SplitOutputTest, AppendsOneLinePerTransfer) {
    const std::string path = makeTempPath();
    std::string error;
    ASSERT_TRUE(openSplitOutput(path, &error)) << error;
    EXPECT_TRUE(splitOutputEnabled());
    logSplitXfer({4096, 1, true, 10000, 20000, 3});
    logSplitXfer({4096, 1, false, 11500, 21250, 4});
    closeSplitOutput();
    EXPECT_FALSE(splitOutputEnabled());

    std::ifstream in(path);
    std::string line1, line2, extra;
    ASSERT_TRUE(std::getline(in, line1));
    ASSERT_TRUE(std::getline(in, line2));
    EXPECT_FALSE(std::getline(in, extra));
    EXPECT_EQ(line1,
              "{\"block_size\":4096,\"batch_size\":1,\"warmup\":true,"
              "\"submit_us\":10.000,\"wait_us\":20.000,\"polls\":3}");
    EXPECT_EQ(line2,
              "{\"block_size\":4096,\"batch_size\":1,\"warmup\":false,"
              "\"submit_us\":11.500,\"wait_us\":21.250,\"polls\":4}");
    std::remove(path.c_str());
}

TEST(SplitOutputTest, ReportsOpenFailure) {
    std::string error;
    EXPECT_FALSE(openSplitOutput(testing::TempDir(), &error));
    EXPECT_EQ(error,
              "failed to open split JSONL output: " + testing::TempDir());
    EXPECT_FALSE(splitOutputEnabled());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
