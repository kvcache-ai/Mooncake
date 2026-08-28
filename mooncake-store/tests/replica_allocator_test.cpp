#include "placement/replica_allocator.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

#include "local_ssd/manager.h"
#include "placement/index.h"
#include "test_buffer_allocator.h"

namespace mooncake::test {
namespace {

constexpr size_t kCapacity = 1U << 20;

class PlacementState {
   public:
    TestBufferAllocator* Add(
        std::string name, std::string endpoint, size_t used = 0,
        AllocationTargetKind kind = AllocationTargetKind::NATIVE,
        std::string cxl_binding = {}) {
        auto allocator = std::make_shared<TestBufferAllocator>(
            name, std::move(endpoint), kCapacity,
            kind == AllocationTargetKind::CXL ? DEFAULT_CXL_BASE : next_base_);
        next_base_ += kCapacity + 4096;
        allocator->SetUsed(used);
        auto target = std::make_unique<AllocationTarget>(
            allocator.get(), kind, std::move(cxl_binding));
        EXPECT_TRUE(index.AddTarget(name, target.get()));
        auto* result = allocator.get();
        allocators.push_back(std::move(allocator));
        targets.push_back(std::move(target));
        return result;
    }

    ScopedPlacementReadAccess Access() { return {index, hosts, owners, mutex}; }

    PlacementIndex index;
    HostRegionIndex hosts;
    OwnerClientByGroupName owners;
    LocalSsdManager local_ssd;
    std::vector<std::shared_ptr<TestBufferAllocator>> allocators;
    std::vector<std::unique_ptr<AllocationTarget>> targets;
    std::shared_mutex mutex;

   private:
    uintptr_t next_base_{0x100000000ULL};
};

ReplicaAllocationRequest Request(size_t replica_count = 1) {
    ReplicaAllocationRequest request;
    request.size = 4096;
    request.replica_count = replica_count;
    return request;
}

std::set<std::string> Endpoints(const std::vector<Replica>& replicas) {
    std::set<std::string> result;
    for (const auto& replica : replicas) {
        result.insert(ReplicaEndpoint(replica));
    }
    return result;
}

}  // namespace

TEST(PlacementIndexTest, KeepsPointersStableAndRemovesGroupsWithSwapPop) {
    PlacementState state;
    state.Add("a", "a");
    state.Add("b", "b");
    state.Add("c", "c");
    auto* a = state.index.GetView().Find("a");
    auto* c = state.index.GetView().Find("c");

    for (size_t i = 0; i < 64; ++i) {
        state.Add("extra-" + std::to_string(i),
                  "endpoint-" + std::to_string(i));
    }
    ASSERT_TRUE(state.index.RemoveTarget("b", state.targets[1].get()));
    EXPECT_EQ(state.index.GetView().Find("a"), a);
    EXPECT_EQ(state.index.GetView().Find("c"), c);
    EXPECT_EQ(state.index.GetView().Find("b"), nullptr);
}

TEST(ReplicaAllocatorTest, SameNameFallbackStaysWithinOneLogicalGroup) {
    PlacementState state;
    auto* failing = state.Add("shared", "bad");
    failing->SetAlwaysFail();
    state.Add("shared", "good");
    state.Add("other", "other");

    ReplicaAllocator allocator(PlacementPolicyType::RANDOM);
    for (size_t i = 0; i < 64; ++i) {
        auto access = state.Access();
        auto result = allocator.Allocate(access, Request(2));
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(Endpoints(*result), (std::set<std::string>{"good", "other"}));
    }
    EXPECT_GT(failing->allocation_calls(), 0U);
}

TEST(ReplicaAllocatorTest, FailedPreferencesFallBackBestEffort) {
    PlacementState state;
    state.Add("failed", "failed")->SetAlwaysFail();
    state.Add("preferred", "preferred");
    state.Add("fallback", "fallback");
    std::vector<std::string> preferred{"failed", "preferred"};
    std::vector<std::string> excluded{"unused"};

    ReplicaAllocator allocator(PlacementPolicyType::RANDOM);
    auto request = Request(3);
    request.preferred_groups = preferred;
    request.excluded_groups = excluded;
    auto access = state.Access();
    auto result = allocator.Allocate(access, request);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(Endpoints(*result),
              (std::set<std::string>{"fallback", "preferred"}));
}

TEST(ReplicaAllocatorTest, RankedPoliciesUseFreeCapacityFeedback) {
    PlacementState state;
    state.Add("full", "full", kCapacity - 4096);
    auto* best = state.Add("best", "best", 0);
    state.Add("second", "second", kCapacity / 2);
    best->SetAlwaysFail();

    ReplicaAllocator allocator(PlacementPolicyType::FREE_RATIO_FIRST);
    auto access = state.Access();
    auto result = allocator.Allocate(access, Request());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(ReplicaEndpoint(result->front()), "second");
    EXPECT_GT(best->allocation_calls(), 0U);
}

TEST(ReplicaAllocatorTest, SsdPolicyUsesLogicalGroupOwner) {
    PlacementState state;
    const UUID low{1, 1};
    const UUID high{2, 2};
    state.Add("low", "low");
    state.Add("high", "high");
    state.owners["low"] = low;
    state.owners["high"] = high;
    ASSERT_EQ(state.local_ssd.RegisterClient(low, true), ErrorCode::OK);
    ASSERT_EQ(state.local_ssd.RegisterClient(high, true), ErrorCode::OK);
    ASSERT_TRUE(state.local_ssd.ReportCapacity(low, 1000).has_value());
    ASSERT_TRUE(state.local_ssd.ReportCapacity(high, 1000).has_value());
    ASSERT_TRUE(state.local_ssd.AdjustUsedBytes(low, 900));
    ASSERT_TRUE(state.local_ssd.AdjustUsedBytes(high, 100));

    ReplicaAllocator allocator(PlacementPolicyType::SSD_FREE_RATIO_FIRST,
                               LocalSSDMetricsView(state.local_ssd));
    auto access = state.Access();
    auto result = allocator.Allocate(access, Request());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(ReplicaEndpoint(result->front()), "high");
}

TEST(ReplicaAllocatorTest, LocalPolicyConsumesHostOrdering) {
    PlacementState state;
    state.Add("remote", "remote");
    state.Add("local-a", "local-a");
    state.Add("local-b", "local-b");
    state.hosts["writer"]["local-a"].insert(generate_uuid());
    state.hosts["writer"]["local-b"].insert(generate_uuid());

    ReplicaAllocator allocator(PlacementPolicyType::LOCAL_FIRST);
    auto request = Request(2);
    request.writer_host_id = "writer";
    request.object_key = "key";
    auto access = state.Access();
    std::vector<PlacementGroup*> expected;
    access.GetHostOrderedGroups("writer", "key", expected);
    auto result = allocator.Allocate(access, request);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 2U);
    EXPECT_EQ(ReplicaEndpoint((*result)[0]), expected[0]->name);
    EXPECT_EQ(ReplicaEndpoint((*result)[1]), expected[1]->name);
}

TEST(ReplicaAllocatorTest, CxlRequiresPreferenceAndConvertsBuffer) {
    PlacementState state;
    state.Add("cxl", "global-cxl", 0, AllocationTargetKind::CXL,
              "client-binding");
    ReplicaAllocator allocator(PlacementPolicyType::CXL);
    {
        auto access = state.Access();
        EXPECT_EQ(allocator.Allocate(access, Request()).error(),
                  ErrorCode::INVALID_PARAMS);
    }

    auto request = Request();
    request.preferred_group = "cxl";
    auto access = state.Access();
    auto result = allocator.Allocate(access, request);
    ASSERT_TRUE(result.has_value());
    auto descriptor = result->front()
                          .get_descriptor()
                          .get_memory_descriptor()
                          .buffer_descriptor;
    EXPECT_EQ(descriptor.protocol_, "cxl");
    EXPECT_EQ(descriptor.transport_endpoint_, "client-binding");
}

TEST(ReplicaAllocatorTest, AllocateFromResolvesOneGroup) {
    PlacementState state;
    state.Add("logical", "bad")->SetAlwaysFail();
    state.Add("logical", "good");
    ReplicaAllocator allocator(PlacementPolicyType::RANDOM);
    auto access = state.Access();
    auto result = allocator.AllocateFrom(access, 4096, "logical");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(ReplicaEndpoint(*result), "good");
    EXPECT_EQ(allocator.AllocateFrom(access, 4096, "missing").error(),
              ErrorCode::SEGMENT_NOT_FOUND);
}

}  // namespace mooncake::test
