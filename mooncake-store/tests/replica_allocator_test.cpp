#include "segment/catalog.h"
#include "placement/replica_allocator.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

#include "client_liveness.h"
#include "local_ssd/manager.h"
#include "placement/index.h"
#include "test_buffer_allocator.h"

namespace mooncake::test {
namespace {

constexpr size_t kCapacity = 1U << 20;

class TestAllocationCandidate final : public AllocationCandidate {
   public:
    TestAllocationCandidate(std::shared_ptr<TestBufferAllocator> allocator,
                            bool is_cxl, std::string cxl_binding,
                            ClientSessionSharedPtr owner_session = nullptr)
        : AllocationCandidate(std::move(allocator), std::move(owner_session)),
          is_cxl_(is_cxl),
          cxl_binding_(std::move(cxl_binding)) {}

    AllocationCandidateKind Kind() const noexcept override {
        return is_cxl_ ? AllocationCandidateKind::CXL
                       : AllocationCandidateKind::NATIVE;
    }

   protected:
    std::unique_ptr<AllocatedBuffer> DoAllocate(size_t size) const override {
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
    TestBufferAllocator* Add(
        std::string name, std::string endpoint, size_t used = 0,
        bool is_cxl = false, std::string cxl_binding = {},
        std::string host = {}, ClientSessionSharedPtr owner_session = nullptr,
        std::optional<SegmentLocality> locality = std::nullopt) {
        auto allocator = std::make_shared<TestBufferAllocator>(
            name, std::move(endpoint), kCapacity,
            is_cxl ? DEFAULT_CXL_BASE : next_base_);
        next_base_ += kCapacity + 4096;
        allocator->SetUsed(used);
        if (locality) {
            allocator->SetLocality(std::move(locality));
        }
        auto candidate = std::make_unique<TestAllocationCandidate>(
            allocator, is_cxl, std::move(cxl_binding),
            std::move(owner_session));
        EXPECT_TRUE(index.AddCandidate(name, *candidate, host));
        auto* result = allocator.get();
        allocators.push_back(std::move(allocator));
        candidates.push_back(std::move(candidate));
        return result;
    }

    ScopedPlacementReadAccess Access() { return {index, catalog, mutex}; }

    PlacementIndex index;
    void Metadata(std::string name, UUID client) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        ASSERT_EQ(catalog.Register({segment, client, SegmentStatus::OK,
                                    RegionKind::HOST_MEMORY}),
                  ErrorCode::OK);
    }
    RegionCatalog catalog;
    LocalSsdManager local_ssd;
    std::vector<std::shared_ptr<TestBufferAllocator>> allocators;
    std::vector<std::unique_ptr<AllocationCandidate>> candidates;
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

TEST(PlacementIndexTest,
     KeepsPointersStableAndRemovesCandidateSetsWithSwapPop) {
    PlacementState state;
    state.Add("a", "a");
    state.Add("b", "b");
    state.Add("c", "c");
    auto* a = state.index.Find("a", AllocationCandidateKind::NATIVE);
    auto* c = state.index.Find("c", AllocationCandidateKind::NATIVE);

    for (size_t i = 0; i < 64; ++i) {
        state.Add("extra-" + std::to_string(i),
                  "endpoint-" + std::to_string(i));
    }
    ASSERT_TRUE(state.index.RemoveCandidate("b", *state.candidates[1], {}));
    EXPECT_EQ(state.index.Find("a", AllocationCandidateKind::NATIVE), a);
    EXPECT_EQ(state.index.Find("c", AllocationCandidateKind::NATIVE), c);
    EXPECT_EQ(state.index.Find("b", AllocationCandidateKind::NATIVE), nullptr);
}

TEST(PlacementIndexTest, UpdatesCandidatesWithoutReplacingEntry) {
    PlacementState state;
    state.Add("shared", "first");
    state.Add("shared", "second");
    const auto* entry =
        state.index.Find("shared", AllocationCandidateKind::NATIVE);
    ASSERT_NE(entry, nullptr);
    EXPECT_FALSE(state.index.AddCandidate("shared", *state.candidates[0], {}));
    EXPECT_EQ(entry->candidates.size(), 2U);

    EXPECT_TRUE(state.index.ReplaceCandidate("shared", *state.candidates[0],
                                             *state.candidates[0], {}));
    EXPECT_FALSE(state.index.ReplaceCandidate("shared", *state.candidates[0],
                                              *state.candidates[1], {}));
    EXPECT_NE(entry->candidates.find(state.candidates[0].get()),
              entry->candidates.end());
    EXPECT_EQ(entry->candidates.size(), 2U);

    auto allocator = std::make_shared<TestBufferAllocator>(
        "shared", "replacement", kCapacity);
    TestAllocationCandidate replacement(allocator, false, "");
    ASSERT_TRUE(state.index.ReplaceCandidate("shared", *state.candidates[0],
                                             replacement, {}));
    EXPECT_EQ(state.index.Find("shared", AllocationCandidateKind::NATIVE),
              entry);
    EXPECT_FALSE(
        state.index.RemoveCandidate("shared", *state.candidates[0], {}));
    ASSERT_TRUE(
        state.index.RemoveCandidate("shared", *state.candidates[1], {}));
    ASSERT_EQ(entry->candidates.size(), 1U);
    {
        auto access = state.Access();
        auto result = ReplicaAllocator(PreferredOnlyPlacementPolicy(
                                           AllocationCandidateKind::NATIVE))
                          .AllocateFrom(access, 4096, "shared");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(ReplicaEndpoint(*result), "replacement");
    }
    ASSERT_TRUE(state.index.RemoveCandidate("shared", replacement, {}));
    EXPECT_EQ(state.index.Find("shared", AllocationCandidateKind::NATIVE),
              nullptr);
    EXPECT_TRUE(
        state.index.active_entries(AllocationCandidateKind::NATIVE).empty());
}

TEST(ReplicaAllocatorTest, SameNameFallbackStaysWithinOneCandidateSet) {
    PlacementState state;
    auto* failing = state.Add("shared", "bad");
    failing->SetAlwaysFail();
    state.Add("shared", "good");
    state.Add("other", "other");

    for (size_t i = 0; i < 64; ++i) {
        auto access = state.Access();
        auto result = ReplicaAllocator(RandomPlacementPolicy{})
                          .Allocate(access, Request(2));
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
    auto* excluded_allocator = state.Add("excluded", "excluded");
    std::vector<std::string> preferred{"failed", "excluded", "preferred",
                                       "preferred"};
    std::vector<std::string> excluded{"unused", "excluded", "excluded"};

    auto request = Request(3);
    request.placement.preferred_segment_names = preferred;
    request.placement.excluded_segment_names = excluded;
    auto access = state.Access();
    auto result =
        ReplicaAllocator(RandomPlacementPolicy{}).Allocate(access, request);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 2U);
    EXPECT_EQ(excluded_allocator->allocation_calls(), 0U);
    EXPECT_EQ(Endpoints(*result),
              (std::set<std::string>{"fallback", "preferred"}));
}

TEST(ReplicaAllocatorTest, MembershipIsResetBetweenOrderedRequests) {
    PlacementState state;
    state.Add("a", "a", 0, false, {}, "writer");
    state.Add("b", "b", 0, false, {}, "writer");
    auto access = state.Access();
    ReplicaAllocator allocator(RandomPlacementPolicy{});
    std::vector<std::string> preferred{"b", "b", "a"};
    std::vector<std::string> excluded{"a"};
    auto request = Request(2);
    request.placement.preferred_segment_names = preferred;
    request.placement.excluded_segment_names = excluded;
    {
        auto first = allocator.Allocate(access, request);
        ASSERT_TRUE(first.has_value());
        ASSERT_EQ(first->size(), 1U);
        EXPECT_EQ(ReplicaEndpoint(first->front()), "b");
    }
    preferred = {"a", "a", "b"};
    request.placement.preferred_segment_names = preferred;
    request.placement.excluded_segment_names = {};
    auto second = allocator.Allocate(access, request);
    ASSERT_TRUE(second.has_value());
    ASSERT_EQ(second->size(), 2U);
    EXPECT_EQ(ReplicaEndpoint((*second)[0]), "a");
    EXPECT_EQ(ReplicaEndpoint((*second)[1]), "b");
    preferred = {"b", "b"};
    request.placement.preferred_segment_names = preferred;
    request.host_affinity = {"writer", "key"};
    auto local =
        ReplicaAllocator(LocalFirstPlacementPolicy{}).Allocate(access, request);
    ASSERT_TRUE(local.has_value());
    ASSERT_EQ(local->size(), 2U);
    EXPECT_EQ(ReplicaEndpoint((*local)[0]), "b");
    EXPECT_EQ(ReplicaEndpoint((*local)[1]), "a");
}

TEST(ReplicaAllocatorTest, RankedPoliciesUseFreeCapacityFeedback) {
    PlacementState state;
    state.Add("full", "full", kCapacity - 4096);
    auto* best = state.Add("best", "best", 0);
    state.Add("second", "second", kCapacity / 2);
    best->SetAlwaysFail();

    auto access = state.Access();
    auto result = ReplicaAllocator(FreeRatioFirstPlacementPolicy{})
                      .Allocate(access, Request());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(ReplicaEndpoint(result->front()), "second");
    EXPECT_GT(best->allocation_calls(), 0U);
}

TEST(ReplicaAllocatorTest, SsdPolicyUsesSegmentNameOwner) {
    PlacementState state;
    const UUID low{1, 1};
    const UUID high{2, 2};
    state.Add("low", "low");
    state.Add("high", "high");
    state.Metadata("low", low);
    state.Metadata("high", high);
    ASSERT_EQ(state.local_ssd.RegisterClient(low, true), ErrorCode::OK);
    ASSERT_EQ(state.local_ssd.RegisterClient(high, true), ErrorCode::OK);
    ASSERT_TRUE(state.local_ssd.ReportCapacity(low, 1000).has_value());
    ASSERT_TRUE(state.local_ssd.ReportCapacity(high, 1000).has_value());
    ASSERT_TRUE(state.local_ssd.AdjustUsedBytes(low, 900));
    ASSERT_TRUE(state.local_ssd.AdjustUsedBytes(high, 100));

    auto access = state.Access();
    auto result = ReplicaAllocator(SsdFreeRatioFirstPlacementPolicy{
                                       LocalSSDMetricsView(state.local_ssd)})
                      .Allocate(access, Request());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(ReplicaEndpoint(result->front()), "high");
}

TEST(ReplicaAllocatorTest, LocalPolicyConsumesHostOrdering) {
    PlacementState state;
    state.Add("remote", "remote");
    state.Add("local-a", "local-a", 0, false, {}, "writer");
    state.Add("local-b", "local-b", 0, false, {}, "writer");

    auto request = Request(2);
    request.host_affinity.writer_host_id = "writer";
    request.host_affinity.object_key = "key";
    auto access = state.Access();
    std::vector<std::string_view> expected;
    access.GetView().VisitHostOrderedSegmentNames("writer", "key",
                                                  [&](auto name) {
                                                      expected.push_back(name);
                                                      return false;
                                                  });
    auto result =
        ReplicaAllocator(LocalFirstPlacementPolicy{}).Allocate(access, request);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 2U);
    EXPECT_EQ(ReplicaEndpoint((*result)[0]), expected[0]);
    EXPECT_EQ(ReplicaEndpoint((*result)[1]), expected[1]);
}

TEST(ReplicaAllocatorTest, LocalPlacementSkipsUnavailableKindsAndFallsBack) {
    PlacementState state;
    state.Add("cxl-only", "cxl-only", 0, true, {}, "writer");
    auto* full = state.Add("full", "full", 0, false, {}, "writer");
    full->SetAlwaysFail();
    state.Add("local", "local", 0, false, {}, "writer");
    state.Add("remote", "remote");
    auto access = state.Access();
    auto request = Request(2);
    request.host_affinity = {"writer", "key"};
    auto result =
        ReplicaAllocator(LocalFirstPlacementPolicy{}).Allocate(access, request);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 2U);
    EXPECT_EQ(Endpoints(*result), (std::set<std::string>{"local", "remote"}));
}

TEST(ReplicaAllocatorTest, NamedAllocationSelectsKindWithoutFallback) {
    PlacementState state;
    auto* native = state.Add("mixed", "native");
    auto* cxl = state.Add("mixed", "global-cxl", 0, true, "client-binding");
    cxl->SetAlwaysFail();
    auto access = state.Access();
    auto failed = ReplicaAllocator(PreferredOnlyPlacementPolicy(
                                       AllocationCandidateKind::CXL))
                      .AllocateFrom(access, 4096, "mixed");
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(native->allocation_calls(), 0U);
    EXPECT_GT(cxl->allocation_calls(), 0U);
    cxl->SetAlwaysFail(false);
    auto result = ReplicaAllocator(PreferredOnlyPlacementPolicy(
                                       AllocationCandidateKind::CXL))
                      .AllocateFrom(access, 4096, "mixed");
    ASSERT_TRUE(result.has_value());
    auto descriptor =
        result->get_descriptor().get_memory_descriptor().buffer_descriptor;
    EXPECT_EQ(descriptor.protocol_, "cxl");
    EXPECT_EQ(descriptor.transport_endpoint_, "client-binding");
    auto native_result = ReplicaAllocator(RandomPlacementPolicy{})
                             .AllocateFrom(access, 4096, "mixed");
    ASSERT_TRUE(native_result.has_value());
    EXPECT_EQ(ReplicaEndpoint(*native_result), "native");
}

TEST(ReplicaAllocatorTest,
     NamedAllocationValidatesRequestAndRetriesSameSegment) {
    PlacementState state;
    state.Add("logical", "bad")->SetAlwaysFail();
    state.Add("logical", "good");
    auto access = state.Access();
    auto result = ReplicaAllocator(PreferredOnlyPlacementPolicy(
                                       AllocationCandidateKind::NATIVE))
                      .AllocateFrom(access, 4096, "logical");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(ReplicaEndpoint(*result), "good");
    EXPECT_EQ(ReplicaAllocator(
                  PreferredOnlyPlacementPolicy(AllocationCandidateKind::NATIVE))
                  .AllocateFrom(access, 4096, "missing")
                  .error(),
              ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_EQ(ReplicaAllocator(
                  PreferredOnlyPlacementPolicy(AllocationCandidateKind::NATIVE))
                  .AllocateFrom(access, 4096, "")
                  .error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ReplicaAllocator(
                  PreferredOnlyPlacementPolicy(AllocationCandidateKind::NATIVE))
                  .AllocateFrom(access, 0, "logical")
                  .error(),
              ErrorCode::INVALID_PARAMS);
}

TEST(ReplicaAllocatorTest, PreferredOnlyRequiresSegmentName) {
    PlacementState state;
    ReplicaAllocator allocator(
        PreferredOnlyPlacementPolicy{AllocationCandidateKind::CXL});
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
    request.placement.preferred_segment_name = "cxl";
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

TEST(ReplicaAllocatorTest, PreferredOnlySelectsKindWithoutFallback) {
    PlacementState state;
    auto* native = state.Add("mixed", "native");
    auto* cxl = state.Add("mixed", "global-cxl", 0, true, "client-binding");
    cxl->SetAlwaysFail();
    EXPECT_NE(state.index.Find("mixed", AllocationCandidateKind::NATIVE),
              state.index.Find("mixed", AllocationCandidateKind::CXL));

    auto request = Request();
    request.placement.preferred_segment_name = "mixed";
    {
        ReplicaAllocator allocator(
            PreferredOnlyPlacementPolicy{AllocationCandidateKind::CXL});
        auto access = state.Access();
        auto result = allocator.Allocate(access, request);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
        EXPECT_EQ(native->allocation_calls(), 0U);
        EXPECT_GT(cxl->allocation_calls(), 0U);
    }
    {
        ReplicaAllocator allocator(
            PreferredOnlyPlacementPolicy{AllocationCandidateKind::NATIVE});
        auto access = state.Access();
        auto result = allocator.Allocate(access, request);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(ReplicaEndpoint(result->front()), "native");
    }
}

TEST(PlacementIndexTest, HostNamesCountAcrossKindsAndIgnoreFailedMutations) {
    PlacementState state;
    state.Add("shared", "native", 0, false, {}, "host");
    state.Add("shared", "cxl", 0, true, {}, "host");
    auto names = [&] {
        std::vector<std::string> result;
        state.index.VisitHostOrderedSegmentNames("host", "key", [&](auto name) {
            result.emplace_back(name);
            return false;
        });
        return result;
    };
    EXPECT_FALSE(
        state.index.AddCandidate("shared", *state.candidates[0], "host"));
    EXPECT_EQ(names(), (std::vector<std::string>{"shared"}));
    TestAllocationCandidate missing(state.allocators[0], false, "");
    TestAllocationCandidate replacement(state.allocators[1], true, "");
    EXPECT_FALSE(
        state.index.ReplaceCandidate("shared", missing, replacement, "host"));
    ASSERT_TRUE(state.index.ReplaceCandidate("shared", *state.candidates[0],
                                             replacement, "host"));
    ASSERT_TRUE(state.index.RemoveCandidate("shared", replacement, "host"));
    EXPECT_EQ(names(), (std::vector<std::string>{"shared"}));
    ASSERT_TRUE(
        state.index.RemoveCandidate("shared", *state.candidates[1], "host"));
    EXPECT_TRUE(names().empty());
    EXPECT_FALSE(
        state.index.RemoveCandidate("shared", *state.candidates[1], "host"));
    EXPECT_TRUE(names().empty());
}

TEST(ReplicaAllocatorTest, HostOrderingWrapsAndDeduplicatesCatalogRecords) {
    PlacementState state;
    state.Add("unbound", "unbound");
    state.Add("a", "a", 0, false, {}, "host-b");
    state.Add("a", "a-duplicate", 0, false, {}, "host-b");
    state.Add("b", "b", 0, false, {}, "host-b");
    state.Add("a", "a-other-host", 0, false, {}, "host-d");
    state.Add("c", "c", 0, false, {}, "host-d");
    std::string key;
    while (std::hash<std::string_view>{}(key) % 2 != 0) key += 'x';
    auto names = [&](std::string_view host) {
        auto access = state.Access();
        std::vector<std::string_view> names;
        access.GetView().VisitHostOrderedSegmentNames(host, key,
                                                      [&](auto name) {
                                                          names.push_back(name);
                                                          return false;
                                                      });
        return std::vector<std::string>(names.begin(), names.end());
    };
    EXPECT_EQ(names("host-c"), (std::vector<std::string>{"a", "c", "b"}));
    EXPECT_EQ(names("host-z"), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_TRUE(names("").empty());
    {
        auto access = state.Access();
        std::vector<std::string> visited;
        access.GetView().VisitHostOrderedSegmentNames(
            "host-c", key, [&](auto name) {
                visited.emplace_back(name);
                return true;
            });
        EXPECT_EQ(visited, (std::vector<std::string>{"a"}));
    }
    ASSERT_TRUE(
        state.index.RemoveCandidate("a", *state.candidates[1], "host-b"));
    EXPECT_EQ(names("host-z"), (std::vector<std::string>{"a", "b", "c"}));
    ASSERT_TRUE(
        state.index.RemoveCandidate("a", *state.candidates[2], "host-b"));
    EXPECT_EQ(names("host-z"), (std::vector<std::string>{"b", "a", "c"}));
    ASSERT_TRUE(
        state.index.RemoveCandidate("a", *state.candidates[4], "host-d"));
    EXPECT_EQ(names("host-c"), (std::vector<std::string>{"c", "b"}));
}

TEST(ReplicaAllocatorTest,
     SuspectedSessionSkipsAllocationAndReportsDiagnostics) {
    PlacementState state;
    const auto now = ClientLivenessRecord::Clock::now();
    auto session_a =
        std::make_shared<ClientLivenessRecord>(generate_uuid(), now);
    auto session_b =
        std::make_shared<ClientLivenessRecord>(generate_uuid(), now);
    auto* alloc_a =
        state.Add("seg-a", "ep-a", 0, false, {}, "host-a", session_a);
    state.Add("seg-b", "ep-b", 0, false, {}, "host-b", session_b);

    ASSERT_EQ(
        session_a->Evaluate(now + std::chrono::seconds(10),
                            std::chrono::seconds(5), std::chrono::seconds(20)),
        ClientLivenessTransition::BECAME_SUSPECTED);

    auto access = state.Access();
    PlacementDiagnostics diag;
    auto request = Request(2);
    auto result = ReplicaAllocator(FreeRatioFirstPlacementPolicy{})
                      .Allocate(access, request, &diag);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1U);
    EXPECT_EQ(ReplicaEndpoint(result->front()), "ep-b");
    EXPECT_EQ(alloc_a->allocation_calls(), 0U);
    EXPECT_FALSE(diag.has_sufficient_active_entry_count);

    // PreferredOnly and AllocateFrom also reject non-serving sessions.
    auto named = ReplicaAllocator(RandomPlacementPolicy{})
                     .AllocateFrom(access, 4096, "seg-a");
    ASSERT_FALSE(named.has_value());
    EXPECT_EQ(named.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Recovering session_a resumes serving and restores diagnostics.
    ASSERT_EQ(session_a->Observe(now + std::chrono::seconds(11)),
              ClientLivenessObservation::RECOVERED_ACTIVE);
    auto recovered = ReplicaAllocator(FreeRatioFirstPlacementPolicy{})
                         .Allocate(access, request, &diag);
    ASSERT_TRUE(recovered.has_value());
    ASSERT_EQ(recovered->size(), 2U);
    EXPECT_TRUE(diag.has_sufficient_active_entry_count);
}

TEST(ReplicaAllocatorTest, NonServingEntriesDoNotBurnSampleOrRetryBudget) {
    PlacementState state;
    const auto now = ClientLivenessRecord::Clock::now();
    auto suspected =
        std::make_shared<ClientLivenessRecord>(generate_uuid(), now);
    ASSERT_EQ(
        suspected->Evaluate(now + std::chrono::seconds(10),
                            std::chrono::seconds(5), std::chrono::seconds(20)),
        ClientLivenessTransition::BECAME_SUSPECTED);
    for (size_t i = 0; i < 120; ++i) {
        state.Add("suspected-" + std::to_string(i),
                  "suspected-ep-" + std::to_string(i), 0, false, {}, {},
                  suspected);
    }
    auto active = std::make_shared<ClientLivenessRecord>(generate_uuid(), now);
    state.Add("active", "active-ep", 0, false, {}, {}, active);

    auto access = state.Access();
    for (size_t i = 0; i < 32; ++i) {
        auto r1 = ReplicaAllocator(RandomPlacementPolicy{})
                      .Allocate(access, Request(1));
        ASSERT_TRUE(r1.has_value());
        EXPECT_EQ(ReplicaEndpoint(r1->front()), "active-ep");

        auto r2 = ReplicaAllocator(FreeRatioFirstPlacementPolicy{})
                      .Allocate(access, Request(1));
        ASSERT_TRUE(r2.has_value());
        EXPECT_EQ(ReplicaEndpoint(r2->front()), "active-ep");
    }
}

TEST(ReplicaAllocatorTest, HcaAffinityPrefersMatchThenUnknownThenMismatch) {
    PlacementState state;
    auto* match = state.Add("shared", "match-ep", 0, false, {}, {}, nullptr,
                            SegmentLocality{0, {"mlx5_0", "mlx5_1"}});
    auto* unknown = state.Add("shared", "unknown-ep");
    auto* mismatch = state.Add("shared", "mismatch-ep", 0, false, {}, {},
                               nullptr, SegmentLocality{1, {"mlx5_2"}});

    std::vector<std::string> preferred_hcas{"mlx5_0"};
    auto request = Request(1);
    request.placement.preferred_segment_name = "shared";
    request.placement.preferred_hca_names = preferred_hcas;

    auto access = state.Access();
    for (size_t i = 0; i < 16; ++i) {
        auto res =
            ReplicaAllocator(RandomPlacementPolicy{}).Allocate(access, request);
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(ReplicaEndpoint(res->front()), "match-ep");
    }

    match->SetAlwaysFail(true);
    for (size_t i = 0; i < 16; ++i) {
        auto res =
            ReplicaAllocator(RandomPlacementPolicy{}).Allocate(access, request);
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(ReplicaEndpoint(res->front()), "unknown-ep");
    }

    unknown->SetAlwaysFail(true);
    auto res =
        ReplicaAllocator(RandomPlacementPolicy{}).Allocate(access, request);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(ReplicaEndpoint(res->front()), "mismatch-ep");
    EXPECT_GT(mismatch->allocation_calls(), 0U);
}

TEST(ReplicaAllocatorTest, HostAffinityAppliesAcrossAllRankerPolicies) {
    PlacementState state;
    state.Add("local", "local-ep", kCapacity - 8192, false, {}, "writer-host");
    state.Add("remote", "remote-ep", 0, false, {}, "remote-host");

    auto request = Request(1);
    request.host_affinity = {"writer-host", "key"};
    auto access = state.Access();

    auto free_ratio = ReplicaAllocator(FreeRatioFirstPlacementPolicy{})
                          .Allocate(access, request);
    ASSERT_TRUE(free_ratio.has_value());
    EXPECT_EQ(ReplicaEndpoint(free_ratio->front()), "local-ep");

    auto random =
        ReplicaAllocator(RandomPlacementPolicy{}).Allocate(access, request);
    ASSERT_TRUE(random.has_value());
    EXPECT_EQ(ReplicaEndpoint(random->front()), "local-ep");
}

}  // namespace mooncake::test
