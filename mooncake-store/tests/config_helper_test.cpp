#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

#include "config_helper.h"

namespace mooncake::test {
namespace {

void ExpectUnsetError(const RequiredParam<int>& param,
                      const std::string& expected_message) {
    try {
        (void)param.Get();
        FAIL() << "Expected an unset RequiredParam to throw";
    } catch (const std::runtime_error& error) {
        EXPECT_EQ(std::string(error.what()), expected_message);
    }
}

TEST(ConfigHelperTest, CopyAssignmentPreservesParameterName) {
    RequiredParam<int> source("metrics_port");
    RequiredParam<int> copy("old_name");
    source = 42;

    copy = source;

    EXPECT_EQ(copy.Get(), 42);
    copy.Clear();
    ExpectUnsetError(copy, "Required parameter metrics_port has not been set");
}

TEST(ConfigHelperTest, CopyConstructionPreservesParameterName) {
    RequiredParam<int> source("metrics_port");
    source = 42;
    RequiredParam<int> copy(source);
    RequiredParam<int> observable("old_name");

    EXPECT_EQ(copy.Get(), 42);
    observable = copy;

    EXPECT_EQ(observable.Get(), 42);
    observable.Clear();
    ExpectUnsetError(observable,
                     "Required parameter metrics_port has not been set");
}

TEST(ConfigHelperTest, CopyUnsetPreservesNullName) {
    RequiredParam<int> source;
    RequiredParam<int> copy(source);
    RequiredParam<int> assigned("old_name");

    assigned = source;

    ExpectUnsetError(copy, "Required parameter has not been set");
    ExpectUnsetError(assigned, "Required parameter has not been set");
}

}  // namespace
}  // namespace mooncake::test
