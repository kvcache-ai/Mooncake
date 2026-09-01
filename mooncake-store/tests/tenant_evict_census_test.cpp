#include "tenant/tenant_store.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tenant {
namespace {

TEST(TenantEvictCensusTest, CensusEnumeratesAllObjects) {
    TenantStore store;
    store.Insert("k1", std::make_shared<ObjectEntry>("k1", "g1"));
    store.Insert("k2", std::make_shared<ObjectEntry>("k2", "g1"));
    store.Insert("k3", std::make_shared<ObjectEntry>("k3", ""));

    size_t count = 0;
    store.VisitObjects([&](const std::shared_ptr<ObjectEntry>&) { ++count; });
    EXPECT_EQ(count, 3u);
}

TEST(TenantEvictCensusTest, GroupedMembersAggregateAsOneAllOrNoneCandidateByLease) {
    TenantStore store;
    auto g1_lease = store.LeaseFor("g1");

    auto e1 = std::make_shared<ObjectEntry>("obj1", "g1");
    e1->set_lease(g1_lease);
    auto e2 = std::make_shared<ObjectEntry>("obj2", "g1");
    e2->set_lease(g1_lease);
    auto e3 = std::make_shared<ObjectEntry>("obj3", "");  // singleton: own lease

    store.Insert("obj1", e1);
    store.Insert("obj2", e2);
    store.Insert("obj3", e3);
    store.AddMember("g1", "obj1");
    store.AddMember("g1", "obj2");

    // Census derives logical groups by grouping on the shared lease pointer.
    // All-or-none: the whole group is protected if the shared lease is live,
    // otherwise the whole group is a candidate.
    std::unordered_map<std::string, std::vector<std::string>> members_by_lease;
    size_t singleton_objects = 0;
    store.VisitObjects([&](const std::shared_ptr<ObjectEntry>& entry) {
        if (!entry->IsGrouped()) {
            ++singleton_objects;
            return;
        }
        const auto id = std::to_string(
            reinterpret_cast<uintptr_t>(entry->lease().get()));
        members_by_lease[id].push_back(entry->key());
    });

    EXPECT_EQ(singleton_objects, 1u);  // obj3
    EXPECT_EQ(members_by_lease.size(), 1u);  // exactly one grouped unit (g1)
    EXPECT_EQ(members_by_lease.begin()->second.size(), 2u);  // obj1, obj2

    // A live shared lease protects the whole group (all-or-none).
    g1_lease->GrantReadLease(std::chrono::milliseconds(10'000));
    EXPECT_FALSE(store.AllExpired("g1", std::chrono::system_clock::now()));

    // Forcing the shared deadline into the past expires the whole group at once.
    g1_lease->SetDeadline(std::chrono::system_clock::now());
    EXPECT_TRUE(store.AllExpired("g1", std::chrono::system_clock::now()));
}

}  // namespace
}  // namespace tenant
}  // namespace mooncake
