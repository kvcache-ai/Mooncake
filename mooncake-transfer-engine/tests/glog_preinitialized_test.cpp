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
#include "glog_compat.h"

// MOONCAKE_GLOG_HAS_IS_INITIALIZED is provided by FindGLOG.cmake through the
// glog::glog INTERFACE compile definitions. It is 1 only when glog >= 0.6.0,
// where google::IsGoogleLoggingInitialized() is publicly exported at the top
// level. On older glog versions the same function is available in the internal
// namespace google::glog_internal_namespace_, so we forward-declare it below to
// exercise the exact same behavior this test verifies in config.cpp.
#ifndef MOONCAKE_GLOG_HAS_IS_INITIALIZED
#define MOONCAKE_GLOG_HAS_IS_INITIALIZED 0
#endif

#if !MOONCAKE_GLOG_HAS_IS_INITIALIZED
// See config.cpp for the rationale: glog < 0.6.0 keeps this function in the
// internal namespace; the symbol is exported by libglog, so we forward-declare
// it here instead of depending on glog's private, non-installed utilities.h.
namespace google {
namespace glog_internal_namespace_ {
bool IsGoogleLoggingInitialized();
}  // namespace glog_internal_namespace_
}  // namespace google
#endif

namespace mooncake {
namespace {

// Version-agnostic wrapper resolving to the correct glog symbol.
inline bool testGlogInitialized() {
#if MOONCAKE_GLOG_HAS_IS_INITIALIZED
    return google::IsGoogleLoggingInitialized();
#else
    return google::glog_internal_namespace_::IsGoogleLoggingInitialized();
#endif
}

class GlogPreinitializedTest : public ::testing::Test {
   protected:
    void TearDown() override {
        ::unsetenv("MC_LOG_DIR");
        if (testGlogInitialized()) {
            google::ShutdownGoogleLogging();
        }
    }
};

TEST_F(GlogPreinitializedTest, LoadGlobalConfigReusesHostLogging) {
    // Simulate the caller (host program) having already initialized glog.
    google::InitGoogleLogging("glog-preinitialized-test");
    ASSERT_TRUE(testGlogInitialized());
    ASSERT_EQ(::setenv("MC_LOG_DIR", ".", 1), 0);

    GlobalConfig config;
    loadGlobalConfig(config);

    // loadGlobalConfig must detect the pre-initialized state and reuse it
    // rather than re-initializing glog.
    EXPECT_TRUE(testGlogInitialized());
    EXPECT_EQ(FLAGS_log_dir, ".");
}

TEST_F(GlogPreinitializedTest, MainPathInitNoOpAfterStaticConfigInit) {
    // Reproduces the standalone mooncake_client / mooncake_master startup
    // order: a static initializer (rdma_transport/worker_pool.cpp evaluates
    // globalConfig()) runs loadGlobalConfig() before main(), which
    // initializes glog from MC_LOG_DIR and sets FLAGS_log_dir. main()'s
    // !FLAGS_log_dir.empty() branch then used to call
    // google::InitGoogleLogging() a second time and CHECK-fail on
    // glog >= 0.6.0 ("You called InitGoogleLogging() twice!").
    ASSERT_EQ(::setenv("MC_LOG_DIR", ".", 1), 0);

    GlobalConfig config;
    loadGlobalConfig(config);  // as the static initializer does
    ASSERT_TRUE(testGlogInitialized());
    ASSERT_EQ(FLAGS_log_dir, ".");

    // The main()-path call must be a no-op instead of CHECK-failing.
    mooncake::InitGoogleLoggingOnce("standalone-main");
    EXPECT_TRUE(testGlogInitialized());
}

}  // namespace
}  // namespace mooncake
