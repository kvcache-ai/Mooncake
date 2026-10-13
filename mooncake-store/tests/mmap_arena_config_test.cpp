#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

#include "../src/config/mmap_arena_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class MmapArenaConfigTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kFlagPoolSize = 8ULL * 1024 * 1024 * 1024;
    static constexpr const char* kPoolSize = "MC_MMAP_ARENA_POOL_SIZE";
    static constexpr const char* kDisable = "MC_DISABLE_MMAP_ARENA";
    static constexpr const char* kUseHugepage = "MC_STORE_USE_HUGEPAGE";
    static constexpr const char* kHugepageSize = "MC_STORE_HUGEPAGE_SIZE";

    MmapArenaConfig Load(bool enabled_by_flag) const {
        return MmapArenaConfig::FromEnvironment(Environ(source_),
                                                enabled_by_flag, kFlagPoolSize);
    }

    MapEnvironSource source_;
};

TEST_F(MmapArenaConfigTest, UnsetAndEmptyEnvironmentPreserveFlagInputs) {
    auto config = Load(false);
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.pool_size, kFlagPoolSize);
    EXPECT_FALSE(config.hugepages_explicitly_requested);

    source_.Set(kPoolSize, "");
    source_.Set(kDisable, "");
    config = Load(true);
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.pool_size, kFlagPoolSize);
    EXPECT_FALSE(config.hugepages_explicitly_requested);
}

TEST_F(MmapArenaConfigTest, PoolSizeEnvironmentOptsInAndOverridesFlag) {
    source_.Set(kPoolSize, " 1.5 MB ");

    const auto config = Load(false);

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.pool_size, 1572864);
    EXPECT_FALSE(config.hugepages_explicitly_requested);
}

TEST_F(MmapArenaConfigTest, PreservesAcceptedInfinitePoolSize) {
    source_.Set(kPoolSize, "infinite");

    const auto config = Load(false);

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.pool_size, std::numeric_limits<uint64_t>::max());
}

TEST_F(MmapArenaConfigTest, InvalidAndZeroPoolSizesOptInWithFlagFallback) {
    for (const char* value : {"0", "-1", "invalid", "   ", "1e999"}) {
        source_.Set(kPoolSize, value);
        ::testing::internal::CaptureStderr();

        const auto config = Load(false);
        const std::string logs = ::testing::internal::GetCapturedStderr();

        EXPECT_TRUE(config.enabled) << value;
        EXPECT_EQ(config.pool_size, kFlagPoolSize) << value;
        EXPECT_NE(logs.find("Invalid MC_MMAP_ARENA_POOL_SIZE='"),
                  std::string::npos)
            << value;
    }
}

TEST_F(MmapArenaConfigTest, DisableEnvironmentUsesCanonicalBooleanTokens) {
    source_.Set(kPoolSize, "2mb");
    for (const char* value : {"1", " TRUE ", "yes", "on", "enable"}) {
        source_.Set(kDisable, value);
        EXPECT_FALSE(Load(false).enabled) << value;
    }

    for (const char* value : {"0", " false ", "no", "off", "disable"}) {
        source_.Set(kDisable, value);
        EXPECT_TRUE(Load(false).enabled) << value;
    }

    source_.Set(kDisable, "true");
    EXPECT_FALSE(Load(true).enabled);
}

TEST_F(MmapArenaConfigTest, InvalidDisableValueWarnsAndFallsBackToFalse) {
    source_.Set(kDisable, "maybe");
    ::testing::internal::CaptureStderr();

    const auto config = Load(true);
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enabled);
    EXPECT_NE(logs.find("Ignoring invalid MC_DISABLE_MMAP_ARENA='maybe'"),
              std::string::npos);
}

TEST_F(MmapArenaConfigTest, DisabledPathSkipsHugepageAndPoolValidation) {
    source_.Set(kPoolSize, "invalid");
    source_.Set(kDisable, "true");
    source_.Set(kUseHugepage, "1");
    source_.Set(kHugepageSize, "invalid");
    ::testing::internal::CaptureStderr();

    const auto config = Load(false);
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(logs.find("Invalid MC_STORE_HUGEPAGE_SIZE"), std::string::npos);
    EXPECT_EQ(logs.find("Invalid MC_MMAP_ARENA_POOL_SIZE"), std::string::npos);
}

TEST_F(MmapArenaConfigTest, PreservesValidationDiagnosticOrder) {
    source_.Set(kPoolSize, "invalid");
    source_.Set(kDisable, "maybe");
    source_.Set(kUseHugepage, "1");
    source_.Set(kHugepageSize, "invalid");
    ::testing::internal::CaptureStderr();

    const auto config = Load(false);
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
    source_.Set(kUseHugepage, "0");

    const auto config = Load(true);

    EXPECT_TRUE(config.enabled);
    EXPECT_TRUE(config.hugepages_explicitly_requested);
}

TEST_F(MmapArenaConfigTest, EachConstructionReadsCurrentEnvironment) {
    source_.Set(kPoolSize, "2mb");
    auto config = Load(false);
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.pool_size, 2ULL * 1024 * 1024);

    source_.Unset(kPoolSize);
    config = Load(false);
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.pool_size, kFlagPoolSize);
}

}  // namespace
}  // namespace mooncake
