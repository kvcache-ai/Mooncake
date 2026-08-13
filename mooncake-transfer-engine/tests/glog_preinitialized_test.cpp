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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>

#include "config.h"

namespace mooncake {
namespace {

class GlogPreinitializedTest : public ::testing::Test {
   protected:
    void TearDown() override {
        ::unsetenv("MC_LOG_DIR");
        if (google::IsGoogleLoggingInitialized()) {
            google::ShutdownGoogleLogging();
        }
    }
};

TEST_F(GlogPreinitializedTest, LoadGlobalConfigReusesHostLogging) {
    google::InitGoogleLogging("glog-preinitialized-test");
    ASSERT_TRUE(google::IsGoogleLoggingInitialized());
    ASSERT_EQ(::setenv("MC_LOG_DIR", ".", 1), 0);

    GlobalConfig config;
    loadGlobalConfig(config);

    EXPECT_TRUE(google::IsGoogleLoggingInitialized());
    EXPECT_EQ(FLAGS_log_dir, ".");
}

}  // namespace
}  // namespace mooncake
