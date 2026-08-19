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

TEST(HABackendAvailabilityTest, K8sAvailabilityMatchesBuildFlag) {
#ifdef STORE_USE_K8S_LEASE
    EXPECT_EQ(ErrorCode::OK, ValidateHABackendAvailability(HABackendType::K8S));
#else
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHABackendAvailability(HABackendType::K8S));
#endif
}

TEST(HABackendAvailabilityTest, ClientSpecParsingMatchesK8sBuildFlag) {
    auto spec = ParseHABackendSpec("k8s://default/master");
#ifdef STORE_USE_K8S_LEASE
    ASSERT_TRUE(spec.has_value());
    ASSERT_TRUE(spec->has_value());
    EXPECT_EQ(HABackendType::K8S, (*spec)->type);
    EXPECT_EQ("default/master", (*spec)->connstring);
#else
    ASSERT_FALSE(spec.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE, spec.error());
#endif
}

}  // namespace
}  // namespace ha
}  // namespace mooncake
