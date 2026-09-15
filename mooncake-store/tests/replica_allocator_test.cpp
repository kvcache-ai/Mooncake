#include "segment/catalog.h"
#include "placement/replica_allocator.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

#include "client_liveness.h"
#include "local_ssd/manager.h"
#include "placement/index.h"
#include "random.h"
#include "test_buffer_allocator.h"

namespace mooncake::test {
namespace {

constexpr size_t kCapacity = 1U << 20;

class TestAllocationCandidate final : public AllocationCandidate {
   public:
    TestAllocationCandidate(std::shared_ptr<TestBufferAllocator> allocator,
                            bool is_cxl, std::string cxl_binding)
        : AllocationCandidate(std::move(allocator)),
          is_cxl_(is_cxl),
          cxl_binding_(std::move(cxl_binding)) {}

    std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const override {
        ++allocation_attempts_;
        auto buffer = AllocateRegistered(size);
        if (buffer && is_cxl_) {
            buffer->change_to_cxl(cxl_binding_);
        }
        return buffer;
    }

    AllocationCandidateKind Kind() const noexcept override {
        return is_cxl_ ? AllocationCandidateKind::CXL
                       : AllocationCandidateKind::NATIVE;
    }

    size_t allocation_attempts() const { return allocation_attempts_; }

   private:
    bool is_cxl_;
    std::string cxl_binding_;
    mutable size_t allocation_attempts_{0};
};

class PlacementState {
   public:
    TestBufferAllocator* Add(std::string name, std::string endpoint,
                             size_t used = 0, bool is_cxl = false,
                             std::string cxl_binding = {},
                             std::string host = {}) {
        auto allocator = std::make_shared<TestBufferAllocator>(
            name, std::move(endpoint), kCapacity,
            is_cxl ? DEFAULT_CXL_BASE : next_base_);
        next_base_ += kCapacity + 4096;
        allocator->SetUsed(used);
        auto candidate = std::make_unique<TestAllocationCandidate>(
            allocator, is_cxl, std::move(cxl_binding));
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
    std::vector<std::unique_ptr<TestAllocationCandidate>> candidates;
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

class ScopedRandomSeed final {
   public:
    explicit ScopedRandomSeed(RandomEngine::result_type seed)
        : saved_engine_(threadLocalRandomEngine()) {
        threadLocalRandomEngine().seed(seed);
    }
    ~ScopedRandomSeed() { threadLocalRandomEngine() = saved_engine_; }

   private:
    RandomEngine saved_engine_;
};

std::shared_ptr<ClientLivenessRecord> SuspectedClient() {
    const auto initial = ClientLivenessRecord::TimePoint{};
    auto record = std::make_shared<ClientLivenessRecord>(initial);
    EXPECT_EQ(
        record->Evaluate(initial + std::chrono::seconds(1),
                         std::chrono::seconds(1), std::chrono::seconds(60)),
        ClientLivenessTransition::BECAME_SUSPECTED);
    return record;
}

template <typename Policy>
class ReplicaAllocatorLivenessTest : public ::testing::Test {
   protected:
    auto Allocate(const ReplicaAllocationRequest& request,
                  PlacementDiagnostics* diagnostics = nullptr) {
        auto access = state.Access();
        if constexpr (std::same_as<Policy, SsdFreeRatioFirstPlacementPolicy>) {
            return ReplicaAllocator(SsdFreeRatioFirstPlacementPolicy{
                                        LocalSSDMetricsView(state.local_ssd)})
                .Allocate(access, request, diagnostics);
        } else {
            return ReplicaAllocator(Policy{}).Allocate(access, request,
                                                       diagnostics);
        }
    }

    PlacementState state;
};

using LivenessPolicies =
    ::testing::Types<RandomPlacementPolicy, LocalFirstPlacementPolicy,
                     FreeRatioFirstPlacementPolicy,
                     SsdFreeRatioFirstPlacementPolicy>;
TYPED_TEST_SUITE(ReplicaAllocatorLivenessTest, LivenessPolicies);

}  // namespace

TYPED_TEST(ReplicaAllocatorLivenessTest,
           SuspectedEntriesDoNotConsumeFallbackRetries) {
    // Both unfiltered sample starts for this seed miss the two healthy entries
    // within the 100-entry retry budget.
    ScopedRandomSeed seed(1);
    auto suspected = SuspectedClient();
    for (size_t i = 0; i < 256; ++i) {
        auto name = "suspected-" + std::to_string(i);
        this->state.Add(name, name);
        this->state.candidates.back()->BindClientLiveness(suspected);
    }
    this->state.Add("full", "full", kCapacity);
    this->state.Add("healthy", "healthy");

    auto result = this->Allocate(Request());
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1U);
    EXPECT_EQ(ReplicaEndpoint(result->front()), "healthy");
    for (size_t i = 0; i < 256; ++i) {
        EXPECT_EQ(this->state.candidates[i]->allocation_attempts(), 0U);
    }
}

TYPED_TEST(ReplicaAllocatorLivenessTest,
           DiagnosticsCountServingEntriesAndTrackRecovery) {
    ScopedRandomSeed seed(1);
    auto suspected = SuspectedClient();
    this->state.Add("healthy", "healthy");
    this->state.Add("healthy", "duplicate");
    this->state.Add("suspected", "suspected");
    this->state.candidates.back()->BindClientLiveness(suspected);
    this->state.Add("unavailable", "unavailable");
    this->state.candidates.back()->SetAvailability(false, true);
    this->state.Add("cxl", "cxl", 0, true);

    PlacementDiagnostics diagnostics;
    {
        auto result = this->Allocate(Request(2), &diagnostics);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->size(), 1U);
        // Neither duplicate regions nor non-serving/wrong-kind entries can
        // justify eviction when strict replica count cannot be satisfied.
        EXPECT_FALSE(diagnostics.has_sufficient_active_entry_count);
    }
    const auto recovery =
        ClientLivenessRecord::TimePoint{} + std::chrono::seconds(2);
    ASSERT_EQ(suspected->Observe(recovery),
              ClientLivenessObservation::RECOVERED_ACTIVE);
    {
        auto result = this->Allocate(Request(2), &diagnostics);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->size(), 2U);
        EXPECT_TRUE(diagnostics.has_sufficient_active_entry_count);
    }
    ASSERT_EQ(
        suspected->Evaluate(recovery + std::chrono::seconds(1),
                            std::chrono::seconds(1), std::chrono::seconds(60)),
        ClientLivenessTransition::BECAME_SUSPECTED);
    this->state.allocators[0]->SetUsed(kCapacity);
    this->state.allocators[1]->SetUsed(kCapacity);
    auto result = this->Allocate(Request(2), &diagnostics);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_FALSE(diagnostics.has_sufficient_active_entry_count);
}

TYPED_TEST(ReplicaAllocatorLivenessTest,
           NoServingEntriesReportNoCapacityWithoutAllocationAttempts) {
    auto suspected = SuspectedClient();
    this->state.Add("suspected", "suspected");
    this->state.candidates.back()->BindClientLiveness(suspected);
    PlacementDiagnostics diagnostics;
    diagnostics.has_sufficient_active_entry_count = true;
    auto result = this->Allocate(Request(), &diagnostics);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_FALSE(diagnostics.has_sufficient_active_entry_count);
    EXPECT_EQ(this->state.candidates.front()->allocation_attempts(), 0U);
}

TEST(ReplicaAllocatorTest, RankedSamplingSkipsSuspectedEntries) {
    for (bool use_ssd_metrics : {false, true}) {
        SCOPED_TRACE(use_ssd_metrics);
        ScopedRandomSeed seed(1);
        PlacementState state;
        auto suspected = SuspectedClient();
        for (size_t i = 0; i < 256; ++i) {
            auto name = "suspected-" + std::to_string(i);
            state.Add(name, name);
            state.candidates.back()->BindClientLiveness(suspected);
        }
        state.Add("second", "second", kCapacity / 2);
        state.Add("best", "best");
        const UUID second{1, 1};
        const UUID best{2, 2};
        state.Metadata("second", second);
        state.Metadata("best", best);
        for (const auto& owner : {second, best}) {
            ASSERT_EQ(state.local_ssd.RegisterClient(owner, true),
                      ErrorCode::OK);
            ASSERT_TRUE(state.local_ssd.ReportCapacity(owner, 1000));
        }
        ASSERT_TRUE(state.local_ssd.AdjustUsedBytes(second, 500));
        auto access = state.Access();
        auto result =
            use_ssd_metrics
                ? ReplicaAllocator(SsdFreeRatioFirstPlacementPolicy{
                                       LocalSSDMetricsView(state.local_ssd)})
                      .Allocate(access, Request())
                : ReplicaAllocator(FreeRatioFirstPlacementPolicy{})
                      .Allocate(access, Request());
        ASSERT_TRUE(result.has_value());
        // Filtering only fallback retries would select "second" for this seed,
        // rather than sampling and ranking the serving entries.
        EXPECT_EQ(ReplicaEndpoint(result->front()), "best");
    }
}

TEST(ReplicaAllocatorTest, SharedEntrySamplesOnlyServingCandidates) {
    PlacementState state;
    for (size_t i = 0; i < 4; ++i) {
        state.Add("shared", "endpoint-" + std::to_string(i));
    }
    auto suspected = SuspectedClient();
    const auto* entry =
        state.index.Find("shared", AllocationCandidateKind::NATIVE);
    // Make the unfiltered starting candidate non-serving, independently of
    // pointer ordering in the entry's candidate set.
    RandomEngine probe(3);
    const auto* first = *entry->candidates.nth(randomIndex(4, probe));
    for (auto& candidate : state.candidates) {
        if (candidate.get() == first) {
            candidate->BindClientLiveness(suspected);
        }
    }
    ScopedRandomSeed seed(3);
    auto access = state.Access();
    auto result = ReplicaAllocator(RandomPlacementPolicy{})
                      .AllocateFrom(access, 4096, "shared");
    ASSERT_TRUE(result.has_value());
    for (auto& candidate : state.candidates) {
        if (!candidate->IsServing()) {
            EXPECT_EQ(candidate->allocation_attempts(), 0U);
        }
    }
}

TEST(ReplicaAllocatorTest, PreferredOnlyCountsServingEntriesOfRequiredKind) {
    for (auto kind :
         {AllocationCandidateKind::NATIVE, AllocationCandidateKind::CXL}) {
        SCOPED_TRACE(static_cast<int>(kind));
        PlacementState state;
        const bool is_cxl = kind == AllocationCandidateKind::CXL;
        auto suspected = SuspectedClient();
        state.Add("preferred", "preferred", 0, is_cxl);
        state.candidates.back()->BindClientLiveness(suspected);
        state.Add("healthy", "healthy", 0, is_cxl);
        state.Add("other-kind", "other-kind", 0, !is_cxl);
        auto access = state.Access();
        ReplicaAllocator allocator(PreferredOnlyPlacementPolicy{kind});
        PlacementDiagnostics diagnostics;
        auto request = Request(2);
        request.placement.preferred_segment_name = "preferred";
        auto result = allocator.Allocate(access, request, &diagnostics);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
        EXPECT_FALSE(diagnostics.has_sufficient_active_entry_count);
        EXPECT_EQ(state.candidates.front()->allocation_attempts(), 0U);

        request.replicas.count = 1;
        state.candidates[1]->SetAvailability(false, true);
        result = allocator.Allocate(access, request, &diagnostics);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
        EXPECT_FALSE(diagnostics.has_sufficient_active_entry_count);
        EXPECT_EQ(state.candidates.front()->allocation_attempts(), 0U);
    }
}

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

}  // namespace mooncake::test
