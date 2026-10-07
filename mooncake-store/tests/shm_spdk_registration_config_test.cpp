#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "../src/config/shm_spdk_registration_config.h"

namespace mooncake {
namespace {

class ScopedRegistrationEnv {
   public:
    ScopedRegistrationEnv() {
        if (const char* value = std::getenv(kName)) original_ = value;
        EXPECT_EQ(unsetenv(kName), 0);
    }
    ~ScopedRegistrationEnv() {
        if (original_) {
            EXPECT_EQ(setenv(kName, original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(kName), 0);
        }
    }
    void Set(const char* value) { ASSERT_EQ(setenv(kName, value, 1), 0); }

   private:
    static constexpr const char* kName = "MC_STORE_REGISTER_SPDK";
    std::optional<std::string> original_;
};

TEST(ShmSpdkRegistrationConfigTest, OnlyExactOneEnablesRegistration) {
    ScopedRegistrationEnv registration;
    EXPECT_FALSE(ShmSpdkRegistrationConfig::FromEnvironment().enabled);
    for (const char* value : {"", "0", "true", "yes", " 1", "1 ", "01"}) {
        registration.Set(value);
        EXPECT_FALSE(ShmSpdkRegistrationConfig::FromEnvironment().enabled)
            << value;
    }
    registration.Set("1");
    EXPECT_TRUE(ShmSpdkRegistrationConfig::FromEnvironment().enabled);
}

}  // namespace
}  // namespace mooncake
