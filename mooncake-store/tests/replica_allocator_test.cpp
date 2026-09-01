#include "placement/domain.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <vector>

#include "local_ssd/manager.h"
#include "placement/index.h"
#include "test_buffer_allocator.h"

namespace mooncake::test {
namespace {

constexpr size_t kCapacity = 1U << 20;

class TestPlacementTarget final : public PlacementTarget {
   public:
    TestPlacementTarget(std::shared_ptr<TestBufferAllocator> allocator,
                        bool is_cxl, std::string cxl_binding)
        : PlacementTarget(std::move(allocator)),
          is_cxl_(is_cxl),
          cxl_binding_(std::move(cxl_binding)) {}

    std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const override {
        auto buffer = allocator().allocate(size);
        if (buffer && is_cxl_) {
            buffer->change_to_cxl(cxl_binding_);
        }
        return buffer;
    }

   private:
    bool is_cxl_;
    std::string cxl_binding_;
};

class PlacementState {
   public:
    TestBufferAllocator* Add(std::string name, std::string endpoint,
                             size_t used = 0, bool is_cxl = false,
                             std::string cxl_binding = {}) {
        auto allocator = std::make_shared<TestBufferAllocator>(
            name, std::move(endpoint), kCapacity,
            is_cxl ? DEFAULT_CXL_BASE : next_base_);
        next_base_ += kCapacity + 4096;
        allocator->SetUsed(used);
        auto target = std::make_unique<TestPlacementTarget>(
            allocator, is_cxl, std::move(cxl_binding));
        EXPECT_TRUE(index.AddTarget(name, target.get()));
        auto* result = allocator.get();
        allocators.push_back(std::move(allocator));
        targets.push_back(std::move(target));
        return result;
    }

    ScopedPlacementReadAccess Access() { return {index, hosts, owners, mutex}; }
    ScopedPlacementReadAccess AcquirePlacementAccess() { return Access(); }

    PlacementIndex index;
    HostRegionIndex hosts;
    OwnerClientByGroupName owners;
    LocalSsdManager local_ssd;
    std::vector<std::shared_ptr<TestBufferAllocator>> allocators;
    std::vector<std::unique_ptr<PlacementTarget>> targets;
    std::shared_mutex mutex;

   private:
    uintptr_t next_base_{0x100000000ULL};
};

ReplicaAllocationRequest Request(size_t replica_count = 1) {
    ReplicaAllocationRequest request;
    request.replicas.size = 4096;
    request.replicas.count = replica_count;
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
    auto* a = state.index.Find("a");
    auto* c = state.index.Find("c");

    for (size_t i = 0; i < 64; ++i) {
        state.Add("extra-" + std::to_string(i),
                  "endpoint-" + std::to_string(i));
    }
    ASSERT_TRUE(state.index.RemoveTarget("b", state.targets[1].get()));
    EXPECT_EQ(state.index.Find("a"), a);
    EXPECT_EQ(state.index.Find("c"), c);
    EXPECT_EQ(state.index.Find("b"), nullptr);
}

TEST(ReplicaAllocatorPolicyTest, NoFOnlyRemapsSsdRanking) {
    PlacementState state;
    const SsdFreeRatioFirstPlacementPolicy ssd_policy{
        LocalSSDMetricsView(state.local_ssd)};
    const LocalFirstPlacementPolicy local_policy;

    const auto nof_ssd_policy = MakeNoFPlacementPolicy(ssd_policy);
    const auto nof_local_policy = MakeNoFPlacementPolicy(local_policy);

    static_assert(
        std::is_same_v<decltype(nof_ssd_policy), const RandomPlacementPolicy>);
    static_assert(std::is_same_v<decltype(nof_local_policy),
                                 const LocalFirstPlacementPolicy>);
}

TEST(ReplicaPlacementTest, BindsSourceToPolicy) {
    PlacementState state;
    state.Add("group", "endpoint");
    ReplicaPlacement placement(state, RandomPlacementPolicy{});

    auto result = placement.Allocate(Request());

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(ReplicaEndpoint(result->front()), "endpoint");
}

TEST(ReplicaAllocatorTest, SameNameFallbackStaysWithinOneLogicalGroup) {
    PlacementState state;
    auto* failing = state.Add("shared", "bad");
    failing->SetAlwaysFail();
    state.Add("shared", "good");
    state.Add("other", "other");

    ReplicaAllocator allocator(RandomPlacementPolicy{});
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

    ReplicaAllocator allocator(RandomPlacementPolicy{});
    auto request = Request(3);
    request.placement.preferred_groups = preferred;
    request.placement.excluded_groups = excluded;
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

    ReplicaAllocator allocator(FreeRatioFirstPlacementPolicy{});
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

    ReplicaAllocator allocator(
        SsdFreeRatioFirstPlacementPolicy(LocalSSDMetricsView(state.local_ssd)));
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

    ReplicaAllocator allocator(LocalFirstPlacementPolicy{});
    auto request = Request(2);
    request.host_affinity.writer_host_id = "writer";
    request.host_affinity.object_key = "key";
    auto access = state.Access();
    std::vector<const PlacementGroup*> expected;
    access.GetHostOrderedGroups("writer", "key", expected);
    auto result = allocator.Allocate(access, request);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 2U);
    EXPECT_EQ(ReplicaEndpoint((*result)[0]), expected[0]->name);
    EXPECT_EQ(ReplicaEndpoint((*result)[1]), expected[1]->name);
}

TEST(ReplicaAllocatorTest, PreferredOnlyRequiresAGroup) {
    PlacementState state;
    ReplicaAllocator allocator(PreferredOnlyPlacementPolicy{});
    {
        auto access = state.Access();
        EXPECT_EQ(allocator.Allocate(access, Request()).error(),
                  ErrorCode::INVALID_PARAMS);
    }

    state.Add("cxl", "global-cxl", 0, true, "client-binding");
    {
        auto access = state.Access();
        EXPECT_EQ(allocator.Allocate(access, Request()).error(),
                  ErrorCode::INVALID_PARAMS);
    }

    auto request = Request();
    request.placement.preferred_group = "cxl";
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
    ReplicaAllocator allocator(RandomPlacementPolicy{});
    auto access = state.Access();
    auto result = allocator.AllocateFrom(access, 4096, "logical");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(ReplicaEndpoint(*result), "good");
    EXPECT_EQ(allocator.AllocateFrom(access, 4096, "missing").error(),
              ErrorCode::SEGMENT_NOT_FOUND);
}

}  // namespace mooncake::test
