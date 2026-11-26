#include "ha/ha_types.h"

#include <gtest/gtest.h>
#include <ylt/util/tl/expected.hpp>

namespace mooncake {

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> ParseHABackendSpec(
    const std::string& master_server_entry);

namespace ha {
namespace {

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
