#pragma once

#include <gtest/gtest.h>

namespace mooncake {
namespace testing {

class HighAvailabilityTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite();
    static void TearDownTestSuite();
};

}  // namespace testing
}  // namespace mooncake
