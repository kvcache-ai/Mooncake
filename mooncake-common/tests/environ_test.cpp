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
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include "environment_variable.h"

namespace mooncake {
namespace {

constexpr EnvironmentVariable<int> kInt{"MC_TEST_INT"};
constexpr EnvironmentVariable<int64_t> kInt64{"MC_TEST_INT64"};
constexpr EnvironmentVariable<uint32_t> kUInt32{"MC_TEST_UINT32"};
constexpr EnvironmentVariable<size_t> kSizeT{"MC_TEST_SIZET"};
constexpr EnvironmentVariable<double> kDouble{"MC_TEST_DOUBLE"};
constexpr EnvironmentVariable<bool> kBool{"MC_TEST_BOOL"};
constexpr EnvironmentVariable<std::string> kString{"MC_TEST_STRING"};
constexpr EnvironmentVariable<std::vector<int>> kIntList{"MC_TEST_INT_LIST"};
constexpr EnvironmentVariable<std::vector<std::string>> kStringList{
    "MC_TEST_STRING_LIST"};

// --- Get ---

TEST(EnvironTest, GetReturnsRawValueOrNullopt) {
    MapEnvironSource source{{"MC_TEST_STRING", " raw value "}};
    const Environ env(source);

    EXPECT_EQ(env.Get("MC_TEST_STRING"), " raw value ");
    EXPECT_FALSE(env.Get("MC_TEST_MISSING").has_value());

    source.Set("MC_TEST_STRING", "");
    ASSERT_TRUE(env.Get("MC_TEST_STRING").has_value());
    EXPECT_TRUE(env.Get("MC_TEST_STRING")->empty());

    source.Unset("MC_TEST_STRING");
    EXPECT_FALSE(env.Get("MC_TEST_STRING").has_value());
}

TEST(EnvironTest, ProcessReadsProcessEnvironment) {
    unsetenv("MC_TEST_INT");
    EXPECT_FALSE(Environ::Process().Get("MC_TEST_INT").has_value());

    setenv("MC_TEST_INT", "42", 1);
    EXPECT_EQ(Environ::Process().GetTyped(kInt), 42);
    unsetenv("MC_TEST_INT");
}

// --- GetTyped ---

TEST(EnvironTest, GetTypedParsesValues) {
    const MapEnvironSource source{{"MC_TEST_INT", " \t+42\r\n"},
                                  {"MC_TEST_INT64", "123456789012"},
                                  {"MC_TEST_SIZET", "1099511627776"},
                                  {"MC_TEST_DOUBLE", " 0.75 "},
                                  {"MC_TEST_BOOL", "off"},
                                  {"MC_TEST_STRING", "hello world"}};
    const Environ env(source);

    EXPECT_EQ(env.GetTyped(kInt), 42);
    EXPECT_EQ(env.GetTyped(kInt64), 123456789012LL);
    EXPECT_EQ(env.GetTyped(kSizeT), 1099511627776ULL);
    EXPECT_DOUBLE_EQ(env.GetTyped(kDouble).value(), 0.75);
    EXPECT_EQ(env.GetTyped(kBool), false);
    EXPECT_EQ(env.GetTyped(kString), "hello world");
}

TEST(EnvironTest, GetTypedIntegerBounds) {
    MapEnvironSource source{{"MC_TEST_INT", std::to_string(INT_MAX)}};
    const Environ env(source);
    EXPECT_EQ(env.GetTyped(kInt), INT_MAX);

    source.Set("MC_TEST_INT", std::to_string(INT_MIN));
    EXPECT_EQ(env.GetTyped(kInt), INT_MIN);
}

TEST(EnvironTest, GetTypedReturnsNulloptForMissingOrInvalidValues) {
    MapEnvironSource source;
    const Environ env(source);
    EXPECT_FALSE(env.GetTyped(kInt).has_value());
    EXPECT_FALSE(env.GetTyped(kString).has_value());

    for (const char* value : {"", "abc", "123abc", "99999999999999999999"}) {
        source.Set("MC_TEST_INT", value);
        EXPECT_FALSE(env.GetTyped(kInt).has_value()) << "for: " << value;
    }
    for (const char* value : {"-1", " -1", "100MB"}) {
        source.Set("MC_TEST_SIZET", value);
        EXPECT_FALSE(env.GetTyped(kSizeT).has_value()) << "for: " << value;
    }
    source.Set("MC_TEST_UINT32", "4294967296");
    EXPECT_FALSE(env.GetTyped(kUInt32).has_value());
    for (const char* value : {"", "0.75garbage", "nan"}) {
        source.Set("MC_TEST_DOUBLE", value);
        EXPECT_FALSE(env.GetTyped(kDouble).has_value()) << "for: " << value;
    }
    for (const char* value : {"", "whatever"}) {
        source.Set("MC_TEST_BOOL", value);
        EXPECT_FALSE(env.GetTyped(kBool).has_value()) << "for: " << value;
    }
}

TEST(EnvironTest, GetTypedPreservesEmptyString) {
    const MapEnvironSource source{{"MC_TEST_STRING", ""}};
    const Environ env(source);

    ASSERT_TRUE(env.GetTyped(kString).has_value());
    EXPECT_TRUE(env.GetTyped(kString)->empty());
}

TEST(EnvironTest, GetTypedParsesBooleanSpellings) {
    MapEnvironSource source;
    const Environ env(source);
    for (const char* value : {"1", "true", "TRUE", "True", "on", "ON", "yes",
                              "YES", "enable", "EnAbLe", " true "}) {
        source.Set("MC_TEST_BOOL", value);
        EXPECT_EQ(env.GetTyped(kBool), true) << "for: " << value;
    }
    for (const char* value :
         {"0", "false", "FALSE", "off", "no", "disable", "DiSaBlE"}) {
        source.Set("MC_TEST_BOOL", value);
        EXPECT_EQ(env.GetTyped(kBool), false) << "for: " << value;
    }
}

// --- GetTypedOr ---

TEST(EnvironTest, GetTypedOrUsesDefaultForMissingValues) {
    const MapEnvironSource source;
    const Environ env(source);

    EXPECT_EQ(env.GetTypedOr(kInt64, int64_t{17}), 17);
    EXPECT_TRUE(env.GetTypedOr(kBool, true));
    EXPECT_EQ(env.GetTypedOr(kString, std::string{"default"}), "default");
}

TEST(EnvironTest, GetTypedOrReturnsParsedValue) {
    const MapEnvironSource source{{"MC_TEST_INT", "0"}, {"MC_TEST_STRING", ""}};
    const Environ env(source);

    EXPECT_EQ(env.GetTypedOr(kInt, 99), 0);
    EXPECT_EQ(env.GetTypedOr(kString, std::string{"default"}), "");
}

TEST(EnvironTest, GetTypedOrWarnsAndUsesDefaultForInvalidValues) {
    const MapEnvironSource source{{"MC_TEST_INT64", "invalid"}};
    const Environ env(source);

    testing::internal::CaptureStderr();
    EXPECT_EQ(env.GetTypedOr(kInt64, int64_t{17}), 17);
    const std::string logs = testing::internal::GetCapturedStderr();

    EXPECT_NE(logs.find("MC_TEST_INT64"), std::string::npos);
    EXPECT_NE(logs.find("using default 17"), std::string::npos);
}

// --- GetList ---

TEST(EnvironTest, GetListSplitsAndParsesItems) {
    const MapEnvironSource source{{"MC_TEST_INT_LIST", "1, 2 ,3"},
                                  {"MC_TEST_STRING_LIST", "a; b ;c"}};
    const Environ env(source);

    EXPECT_EQ(env.GetList(kIntList), (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(env.GetList(kStringList, ';'),
              (std::vector<std::string>{"a", "b", "c"}));
}

TEST(EnvironTest, GetListHandlesMissingEmptyAndInvalidValues) {
    MapEnvironSource source;
    const Environ env(source);
    EXPECT_FALSE(env.GetList(kIntList).has_value());

    source.Set("MC_TEST_INT_LIST", "");
    ASSERT_TRUE(env.GetList(kIntList).has_value());
    EXPECT_TRUE(env.GetList(kIntList)->empty());

    for (const char* value : {"1,x,3", "1,,3", "1,"}) {
        source.Set("MC_TEST_INT_LIST", value);
        EXPECT_FALSE(env.GetList(kIntList).has_value()) << "for: " << value;
    }

    source.Set("MC_TEST_STRING_LIST", "a,,b");
    EXPECT_EQ(env.GetList(kStringList),
              (std::vector<std::string>{"a", "", "b"}));
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
