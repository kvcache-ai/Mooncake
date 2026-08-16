#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

#include "config_helper.h"

namespace mooncake::test {
namespace {

void ExpectUnsetErrorNames(const RequiredParam<int>& param,
                           const std::string& name) {
    try {
        (void)param.Get();
        FAIL() << "Expected an unset RequiredParam to throw";
    } catch (const std::runtime_error& error) {
        EXPECT_NE(std::string(error.what()).find(name), std::string::npos)
            << error.what();
    }
}

TEST(ConfigHelperTest, CopyAssignmentPreservesParameterName) {
    RequiredParam<int> source("metrics_port");
    RequiredParam<int> copy("old_name");
    source = 42;

    copy = source;

    EXPECT_EQ(copy.Get(), 42);
    copy.Clear();
    ExpectUnsetErrorNames(copy, "metrics_port");
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
    ExpectUnsetErrorNames(observable, "metrics_port");
}

}  // namespace
}  // namespace mooncake::test
