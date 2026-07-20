#include "ha/ha_types.h"

#include <gtest/gtest.h>
#include <ylt/util/tl/expected.hpp>

namespace mooncake {

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> ParseHABackendSpec(
    const std::string& master_server_entry);

namespace ha {
namespace {

TEST(HABackendAvailabilityTest, UnknownBackendIsInvalid) {
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              ValidateHABackendAvailability(HABackendType::UNKNOWN));
}

TEST(HABackendAvailabilityTest, EtcdAvailabilityMatchesBuildFlag) {
#ifdef STORE_USE_ETCD
    EXPECT_EQ(ErrorCode::OK,
              ValidateHABackendAvailability(HABackendType::ETCD));
#else
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHABackendAvailability(HABackendType::ETCD));
#endif
}

TEST(HABackendAvailabilityTest, RedisAvailabilityMatchesBuildFlag) {
#ifdef STORE_USE_REDIS
    EXPECT_EQ(ErrorCode::OK,
              ValidateHABackendAvailability(HABackendType::REDIS));
#else
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHABackendAvailability(HABackendType::REDIS));
#endif
}

TEST(HABackendAvailabilityTest, K8sLeaseIsRejectedUntilCoordinatorExists) {
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHABackendAvailability(HABackendType::K8S));
}

TEST(HABackendAvailabilityTest,
     ClientSpecParsingRejectsUnavailableBackendBeforeCoordinatorCreation) {
    auto spec = ParseHABackendSpec("k8s://default/master");
    ASSERT_FALSE(spec.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE, spec.error());
}

}  // namespace
}  // namespace ha
}  // namespace mooncake
