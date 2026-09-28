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
#include <string>
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

TEST(SplitOutputTest, FormatsIntegerJsonLine) {
    char buf[160];
    size_t n = 0;
    SplitXferSample sample{207360, 66874, 1152760, 201};
    ASSERT_TRUE(formatSplitXferLine(buf, sizeof(buf), sample, &n));
    EXPECT_EQ(std::string(buf, n),
              "{\"batch_size\":207360,\"submit_us\":66874,\"wait_us\":1152760,"
              "\"polls\":201}\n");
}

TEST(SplitOutputTest, RejectsTruncatedBuffer) {
    char buf[8];
    size_t n = 99;
    SplitXferSample sample{1, 2, 3, 4};
    EXPECT_FALSE(formatSplitXferLine(buf, sizeof(buf), sample, &n));
}

TEST(SplitOutputTest, DisabledLogIsNoOp) {
    EXPECT_FALSE(splitOutputEnabled());
    logSplitXfer({1, 2, 3, 4});
    EXPECT_FALSE(splitOutputEnabled());
}

TEST(SplitOutputTest, AppendsOneLinePerTransfer) {
    const std::string path = makeTempPath();
    std::string error;
    ASSERT_TRUE(openSplitOutput(path, &error)) << error;
    EXPECT_TRUE(splitOutputEnabled());
    logSplitXfer({4096, 10, 20, 3});
    logSplitXfer({4096, 11, 21, 4});
    closeSplitOutput();
    EXPECT_FALSE(splitOutputEnabled());

    std::ifstream in(path);
    std::string line1, line2, extra;
    ASSERT_TRUE(std::getline(in, line1));
    ASSERT_TRUE(std::getline(in, line2));
    EXPECT_FALSE(std::getline(in, extra));
    EXPECT_EQ(line1,
              "{\"batch_size\":4096,\"submit_us\":10,\"wait_us\":20,\"polls\":"
              "3}");
    EXPECT_EQ(line2,
              "{\"batch_size\":4096,\"submit_us\":11,\"wait_us\":21,\"polls\":"
              "4}");
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
