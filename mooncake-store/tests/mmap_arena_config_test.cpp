#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <string>

#include "../src/config/mmap_arena_config.h"

namespace mooncake {
namespace {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) {
            original_ = value;
        }
        unsetenv(name);
    }

    ~ScopedEnvVar() {
        if (original_.has_value()) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

    void Set(const char* value) { setenv(name_.c_str(), value, 1); }
    void Unset() { unsetenv(name_.c_str()); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

struct MmapArenaEnvironment {
    ScopedEnvVar pool_size{"MC_MMAP_ARENA_POOL_SIZE"};
    ScopedEnvVar disable{"MC_DISABLE_MMAP_ARENA"};
    ScopedEnvVar use_hugepage{"MC_STORE_USE_HUGEPAGE"};
    ScopedEnvVar hugepage_size{"MC_STORE_HUGEPAGE_SIZE"};
};

class MmapArenaConfigTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kFlagPoolSize = 8ULL * 1024 * 1024 * 1024;

    MmapArenaEnvironment env;
};

TEST_F(MmapArenaConfigTest, UnsetAndEmptyEnvironmentPreserveFlagInputs) {
    auto config = MmapArenaConfig::FromEnvironment(false, kFlagPoolSize);
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.pool_size, kFlagPoolSize);
    EXPECT_FALSE(config.hugepages_explicitly_requested);

    env.pool_size.Set("");
    env.disable.Set("");
    config = MmapArenaConfig::FromEnvironment(true, kFlagPoolSize);
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.pool_size, kFlagPoolSize);
    EXPECT_FALSE(config.hugepages_explicitly_requested);
}

TEST_F(MmapArenaConfigTest, PoolSizeEnvironmentOptsInAndOverridesFlag) {
    env.pool_size.Set(" 1.5 MB ");

    const auto config = MmapArenaConfig::FromEnvironment(false, kFlagPoolSize);

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.pool_size, 1572864);
    EXPECT_FALSE(config.hugepages_explicitly_requested);
}

TEST_F(MmapArenaConfigTest, PreservesAcceptedInfinitePoolSize) {
    env.pool_size.Set("infinite");

    const auto config = MmapArenaConfig::FromEnvironment(false, kFlagPoolSize);

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.pool_size, std::numeric_limits<uint64_t>::max());
}

TEST_F(MmapArenaConfigTest, InvalidAndZeroPoolSizesOptInWithFlagFallback) {
    for (const char* value : {"0", "-1", "invalid", "   ", "1e999"}) {
        env.pool_size.Set(value);
        ::testing::internal::CaptureStderr();

        const auto config =
            MmapArenaConfig::FromEnvironment(false, kFlagPoolSize);
        const std::string logs = ::testing::internal::GetCapturedStderr();

        EXPECT_TRUE(config.enabled) << value;
        EXPECT_EQ(config.pool_size, kFlagPoolSize) << value;
        EXPECT_NE(logs.find("Invalid MC_MMAP_ARENA_POOL_SIZE='"),
                  std::string::npos)
            << value;
    }
}

TEST_F(MmapArenaConfigTest, DisableEnvironmentUsesCanonicalBooleanTokens) {
    env.pool_size.Set("2mb");
    for (const char* value : {"1", " TRUE ", "yes", "on", "enable"}) {
        env.disable.Set(value);
        EXPECT_FALSE(
            MmapArenaConfig::FromEnvironment(false, kFlagPoolSize).enabled)
            << value;
    }

    for (const char* value : {"0", " false ", "no", "off", "disable"}) {
        env.disable.Set(value);
        EXPECT_TRUE(
            MmapArenaConfig::FromEnvironment(false, kFlagPoolSize).enabled)
            << value;
    }

    env.disable.Set("true");
    EXPECT_FALSE(MmapArenaConfig::FromEnvironment(true, kFlagPoolSize).enabled);
}

TEST_F(MmapArenaConfigTest, InvalidDisableValueWarnsAndFallsBackToFalse) {
    env.disable.Set("maybe");
    ::testing::internal::CaptureStderr();

    const auto config = MmapArenaConfig::FromEnvironment(true, kFlagPoolSize);
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enabled);
    EXPECT_NE(logs.find("Ignoring invalid MC_DISABLE_MMAP_ARENA='maybe'"),
              std::string::npos);
}

TEST_F(MmapArenaConfigTest, DisabledPathSkipsHugepageAndPoolValidation) {
    env.pool_size.Set("invalid");
    env.disable.Set("true");
    env.use_hugepage.Set("1");
    env.hugepage_size.Set("invalid");
    ::testing::internal::CaptureStderr();

    const auto config = MmapArenaConfig::FromEnvironment(false, kFlagPoolSize);
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(logs.find("Invalid MC_STORE_HUGEPAGE_SIZE"), std::string::npos);
    EXPECT_EQ(logs.find("Invalid MC_MMAP_ARENA_POOL_SIZE"), std::string::npos);
}

TEST_F(MmapArenaConfigTest, PreservesValidationDiagnosticOrder) {
    env.pool_size.Set("invalid");
    env.disable.Set("maybe");
    env.use_hugepage.Set("1");
    env.hugepage_size.Set("invalid");
    ::testing::internal::CaptureStderr();

    const auto config = MmapArenaConfig::FromEnvironment(false, kFlagPoolSize);
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enabled);
    const size_t disable_warning =
        logs.find("Ignoring invalid MC_DISABLE_MMAP_ARENA");
    const size_t hugepage_warning = logs.find("Invalid MC_STORE_HUGEPAGE_SIZE");
    const size_t pool_warning = logs.find("Invalid MC_MMAP_ARENA_POOL_SIZE");
    ASSERT_NE(disable_warning, std::string::npos);
    ASSERT_NE(hugepage_warning, std::string::npos);
    ASSERT_NE(pool_warning, std::string::npos);
    EXPECT_LT(disable_warning, hugepage_warning);
    EXPECT_LT(hugepage_warning, pool_warning);
}

TEST_F(MmapArenaConfigTest, AnyPresentHugepageFlagIsAnExplicitRequest) {
    env.use_hugepage.Set("0");

    const auto config = MmapArenaConfig::FromEnvironment(true, kFlagPoolSize);

    EXPECT_TRUE(config.enabled);
    EXPECT_TRUE(config.hugepages_explicitly_requested);
}

TEST_F(MmapArenaConfigTest, EachConstructionReadsCurrentEnvironment) {
    env.pool_size.Set("2mb");
    auto config = MmapArenaConfig::FromEnvironment(false, kFlagPoolSize);
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.pool_size, 2ULL * 1024 * 1024);

    env.pool_size.Unset();
    config = MmapArenaConfig::FromEnvironment(false, kFlagPoolSize);
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.pool_size, kFlagPoolSize);
}

}  // namespace
}  // namespace mooncake
