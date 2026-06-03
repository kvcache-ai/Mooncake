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

#include "environ.h"

#include <gtest/gtest.h>

#include <climits>
#include <cstdlib>

using mooncake::Environ;

class EnvironTest : public ::testing::Test {
   protected:
    void SetUp() override { clearTestEnvVars(); }
    void TearDown() override { clearTestEnvVars(); }

    void clearTestEnvVars() {
        unsetenv("MC_TEST_INT");
        unsetenv("MC_TEST_SIZET");
        unsetenv("MC_TEST_BOOL");
        unsetenv("MC_TEST_STRING");
    }
};

// --- GetInt ---

TEST_F(EnvironTest, GetIntValidValue) {
    setenv("MC_TEST_INT", "42", 1);
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 0), 42);
}

TEST_F(EnvironTest, GetIntNegativeValue) {
    setenv("MC_TEST_INT", "-100", 1);
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 0), -100);
}

TEST_F(EnvironTest, GetIntZero) {
    setenv("MC_TEST_INT", "0", 1);
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 99), 0);
}

TEST_F(EnvironTest, GetIntMissing) {
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 77), 77);
}

TEST_F(EnvironTest, GetIntEmpty) {
    setenv("MC_TEST_INT", "", 1);
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 55), 55);
}

TEST_F(EnvironTest, GetIntNonNumeric) {
    setenv("MC_TEST_INT", "abc", 1);
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 55), 55);
}

TEST_F(EnvironTest, GetIntTrailingGarbage) {
    setenv("MC_TEST_INT", "123abc", 1);
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 55), 55);
}

TEST_F(EnvironTest, GetIntOverflow) {
    setenv("MC_TEST_INT", "99999999999999999999", 1);
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 55), 55);
}

TEST_F(EnvironTest, GetIntMaxValue) {
    setenv("MC_TEST_INT", std::to_string(INT_MAX).c_str(), 1);
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 0), INT_MAX);
}

TEST_F(EnvironTest, GetIntMinValue) {
    setenv("MC_TEST_INT", std::to_string(INT_MIN).c_str(), 1);
    EXPECT_EQ(Environ::GetInt("MC_TEST_INT", 0), INT_MIN);
}

// --- GetSizeT ---

TEST_F(EnvironTest, GetSizeTValidValue) {
    setenv("MC_TEST_SIZET", "65536", 1);
    EXPECT_EQ(Environ::GetSizeT("MC_TEST_SIZET", 0), 65536u);
}

TEST_F(EnvironTest, GetSizeTZero) {
    setenv("MC_TEST_SIZET", "0", 1);
    EXPECT_EQ(Environ::GetSizeT("MC_TEST_SIZET", 99), 0u);
}

TEST_F(EnvironTest, GetSizeTMissing) {
    EXPECT_EQ(Environ::GetSizeT("MC_TEST_SIZET", 4096), 4096u);
}

TEST_F(EnvironTest, GetSizeTNonNumeric) {
    setenv("MC_TEST_SIZET", "bogus", 1);
    EXPECT_EQ(Environ::GetSizeT("MC_TEST_SIZET", 4096), 4096u);
}

TEST_F(EnvironTest, GetSizeTTrailingGarbage) {
    setenv("MC_TEST_SIZET", "100MB", 1);
    EXPECT_EQ(Environ::GetSizeT("MC_TEST_SIZET", 4096), 4096u);
}

TEST_F(EnvironTest, GetSizeTLargeValue) {
    setenv("MC_TEST_SIZET", "1099511627776", 1);  // 1 TiB
    EXPECT_EQ(Environ::GetSizeT("MC_TEST_SIZET", 0), 1099511627776ull);
}

TEST_F(EnvironTest, GetSizeTNegativeValue) {
    setenv("MC_TEST_SIZET", "-1", 1);
    EXPECT_EQ(Environ::GetSizeT("MC_TEST_SIZET", 4096), 4096u);
}

TEST_F(EnvironTest, GetSizeTOverflow) {
    setenv("MC_TEST_SIZET", "99999999999999999999999999", 1);
    EXPECT_EQ(Environ::GetSizeT("MC_TEST_SIZET", 4096), 4096u);
}

// --- GetBool ---

TEST_F(EnvironTest, GetBoolTrue) {
    for (const char* v :
         {"1", "true", "TRUE", "True", "on", "ON", "yes", "YES"}) {
        setenv("MC_TEST_BOOL", v, 1);
        EXPECT_TRUE(Environ::GetBool("MC_TEST_BOOL", false)) << "for: " << v;
    }
}

TEST_F(EnvironTest, GetBoolFalse) {
    for (const char* v : {"0", "false", "FALSE", "off", "no", "whatever"}) {
        setenv("MC_TEST_BOOL", v, 1);
        EXPECT_FALSE(Environ::GetBool("MC_TEST_BOOL", false)) << "for: " << v;
    }
}

TEST_F(EnvironTest, GetBoolMissing) {
    EXPECT_TRUE(Environ::GetBool("MC_TEST_BOOL", true));
    EXPECT_FALSE(Environ::GetBool("MC_TEST_BOOL", false));
}

TEST_F(EnvironTest, GetBoolEmpty) {
    setenv("MC_TEST_BOOL", "", 1);
    EXPECT_FALSE(Environ::GetBool("MC_TEST_BOOL", true));
}

// --- GetString ---

TEST_F(EnvironTest, GetStringValidValue) {
    setenv("MC_TEST_STRING", "hello", 1);
    EXPECT_EQ(Environ::GetString("MC_TEST_STRING", "default"), "hello");
}

TEST_F(EnvironTest, GetStringMissing) {
    EXPECT_EQ(Environ::GetString("MC_TEST_STRING", "default"), "default");
}

TEST_F(EnvironTest, GetStringEmpty) {
    setenv("MC_TEST_STRING", "", 1);
    EXPECT_EQ(Environ::GetString("MC_TEST_STRING", "default"), "");
}

TEST_F(EnvironTest, GetStringWithSpaces) {
    setenv("MC_TEST_STRING", "hello world", 1);
    EXPECT_EQ(Environ::GetString("MC_TEST_STRING", ""), "hello world");
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
