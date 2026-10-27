#include "tenant_id.h"

#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(TenantIdTest, NormalizesEmptyTenantToDefault) {
    const TenantId empty("");
    EXPECT_EQ(empty, TenantId::Default());
    EXPECT_EQ(empty.value(), "default");
    EXPECT_TRUE(empty.IsDefault());

    const TenantId named("tenant-a");
    EXPECT_EQ(named.value(), "tenant-a");
    EXPECT_FALSE(named.IsDefault());
}

TEST(TenantIdTest, ValidatesCanonicalTenantName) {
    EXPECT_TRUE(TenantId().IsValid());
    EXPECT_TRUE(TenantId("tenant-a").IsValid());
    EXPECT_FALSE(TenantId("_reserved").IsValid());
    EXPECT_FALSE(TenantId(std::string("bad\nname")).IsValid());
    EXPECT_FALSE(TenantId(std::string("bad\x7f", 4)).IsValid());
}

TEST(TenantIdTest, SupportsOrderedAndHashedKeys) {
    EXPECT_LT(TenantId("tenant-a"), TenantId("tenant-b"));

    std::unordered_map<TenantId, int, TenantIdHash> tenants;
    tenants.emplace(TenantId("tenant-a"), 1);
    EXPECT_EQ(tenants.at(TenantId("tenant-a")), 1);
}

TEST(TenantIdTest, ScopedKeyPreservesExistingEncoding) {
    const TenantId tenant("tenant:with:colon");
    const std::string scoped = tenant.MakeScopedKey("path/key:with:colon");
    const std::string expected =
        std::string("tenant:with:colon") + '\0' + "path/key:with:colon";
    EXPECT_EQ(scoped, expected);

    auto [parsed_tenant, parsed_key] = TenantId::ParseScopedKey(scoped);
    EXPECT_EQ(parsed_tenant, tenant);
    EXPECT_EQ(parsed_key, "path/key:with:colon");
}

TEST(TenantIdTest, ScopedKeyRoundTripPreservesEmbeddedNullInLocalKey) {
    const std::string local_key("part1\0part2", 11);
    const std::string scoped = TenantId("tenant-a").MakeScopedKey(local_key);
    auto [tenant, parsed_key] = TenantId::ParseScopedKey(scoped);
    EXPECT_EQ(tenant, TenantId("tenant-a"));
    EXPECT_EQ(parsed_key, local_key);
}

TEST(TenantIdTest, ParsesLegacyUnscopedKeyAsDefaultTenant) {
    auto [tenant, key] = TenantId::ParseScopedKey("legacy-key");
    EXPECT_EQ(tenant, TenantId::Default());
    EXPECT_EQ(key, "legacy-key");
}

}  // namespace
}  // namespace mooncake
