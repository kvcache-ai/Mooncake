#include "metadata/tenant.h"
#include "object_test_helpers.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// The route itself is covered by object_route_test; these cover what the
// tenant adds around it.

TEST(TenantTest, QuotaAccountIsTheOneTheTenantWasBuiltWith) {
    EXPECT_EQ(Tenant().QuotaAccount(), nullptr);

    TenantQuotaAccount account;
    EXPECT_EQ(Tenant(&account).QuotaAccount(), &account);
}

TEST(TenantTest, EmptyTracksObjectsAndGroups) {
    Tenant tenant;
    EXPECT_TRUE(tenant.Empty());

    ASSERT_NE(test::PublishObject(tenant.Route(), "k1", "g1"), 0u);
    EXPECT_FALSE(tenant.Empty());

    // Tearing the object down drops its membership with it.
    {
        auto guard = tenant.Route().Write("k1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
    }
    EXPECT_TRUE(tenant.Empty());
    EXPECT_TRUE(tenant.Route().GroupMembers("g1").empty());
}

TEST(TenantTest, EachTenantRoutesItsOwnKeys) {
    Tenant first;
    Tenant second;
    ASSERT_NE(test::PublishObject(first.Route(), "k1"), 0u);

    // The same key is free on another tenant's route.
    EXPECT_FALSE(second.Route().Contains("k1"));
    ASSERT_NE(test::PublishObject(second.Route(), "k1"), 0u);
    EXPECT_EQ(first.Route().ObjectCount(), 1u);
    EXPECT_EQ(second.Route().ObjectCount(), 1u);
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
