#include "ha/ha_types.h"

#include <gtest/gtest.h>

namespace mooncake {
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

}  // namespace
}  // namespace ha
}  // namespace mooncake
