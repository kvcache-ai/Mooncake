#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

#include "egm_store_pool.h"
#include "egm_store_pool_orchestrator.h"
#include "real_client.h"

namespace mooncake {
namespace {

enum class Operation {
    kAllocate,
    kInstallAllocator,
    kRegister,
    kMount,
    kUnmount,
    kUnregister,
    kReleaseAllocator,
    kDestroy,
};

struct Event {
    Operation operation;
    size_t allocation_id = 0;
    size_t ordinal = 0;
    bool flag = false;
    std::string context;
};

class FakeAllocation final : public EgmStorePoolAllocation {
   public:
    FakeAllocation(size_t id, void* base, size_t length, size_t granularity)
        : id_(id), base_(base), length_(length), granularity_(granularity) {}

    void* base() const override { return base_; }
    size_t length() const override { return length_; }
    size_t granularity() const override { return granularity_; }
    size_t id() const { return id_; }

   private:
    size_t id_;
    void* base_;
    size_t length_;
    size_t granularity_;
};

class FakeOperations final : public EgmStorePoolOperations {
   public:
    void Fail(Operation operation, std::initializer_list<size_t> ordinals) {
        failure_ordinals_[operation] = std::set<size_t>(ordinals);
    }

    void ClearFailures() { failure_ordinals_.clear(); }

    void SetRegisterSideEffectOnFailure(bool enabled) {
        register_side_effect_on_failure_ = enabled;
    }

    tl::expected<std::unique_ptr<EgmStorePoolAllocation>, ErrorCode> Allocate(
        const EgmStorePoolAllocationRequest& request) override {
        const size_t id = next_allocation_id_++;
        const bool fail = Record(
            Operation::kAllocate, id,
            request.role == EgmStorePoolAllocationRole::kGlobal,
            request.role == EgmStorePoolAllocationRole::kLocal ? "local"
                                                               : "global");
        if (fail) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

        constexpr uintptr_t kBase = 0x10000000;
        constexpr uintptr_t kStride = 0x10000;
        void* base = reinterpret_cast<void*>(kBase + id * kStride);
        live_allocations_.insert(id);
        allocation_by_base_[base] = id;
        return std::unique_ptr<EgmStorePoolAllocation>(new FakeAllocation(
            id, base, request.requested_length, kAllocationGranularity));
    }

    tl::expected<void, ErrorCode> InstallAllocatorView(
        EgmStorePoolAllocation* local_allocation,
        size_t configured_local_length) override {
        const size_t id = local_allocation == nullptr
                              ? 0
                              : AllocationId(local_allocation->base());
        const bool fail = Record(Operation::kInstallAllocator, id,
                                 configured_local_length != 0, "allocator");
        if (fail) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        allocator_installed_ = true;
        return {};
    }

    tl::expected<void, ErrorCode> ReleaseAllocatorView() override {
        const bool fail =
            Record(Operation::kReleaseAllocator, 0, false, "allocator");
        if (fail) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        allocator_installed_ = false;
        return {};
    }

    tl::expected<void, ErrorCode> RegisterLocal(
        void* base, size_t, bool remote_accessible) override {
        const size_t id = AllocationId(base);
        const bool fail =
            Record(Operation::kRegister, id, remote_accessible, "local");
        if (!fail || register_side_effect_on_failure_) {
            local_registrations_.insert(id);
        }
        if (fail) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        return {};
    }

    tl::expected<UUID, ErrorCode> MountGlobal(void* base, size_t) override {
        const size_t id = AllocationId(base);
        const bool fail = Record(Operation::kMount, id, true, "global");

        // Client::MountSegmentAndGetId registers TE first. On Master failure it
        // owns the first unregister compensation attempt.
        global_registrations_.insert(id);
        if (fail) {
            (void)UnregisterImpl(base, true, "client-compensation");
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        const UUID segment_id{id, 0x4e564c48};
        mounted_segments_[segment_id] = id;
        return segment_id;
    }

    tl::expected<void, ErrorCode> UnmountGlobal(
        const UUID& segment_id) override {
        auto mounted = mounted_segments_.find(segment_id);
        const size_t id =
            mounted == mounted_segments_.end() ? 0 : mounted->second;
        const bool fail = Record(Operation::kUnmount, id, true, "global");
        if (fail) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        if (mounted == mounted_segments_.end()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        global_registrations_.erase(id);
        mounted_segments_.erase(mounted);
        return {};
    }

    tl::expected<void, ErrorCode> UnregisterIfPresent(
        void* base, bool update_metadata) override {
        return UnregisterImpl(base, update_metadata, "orchestrator-fallback");
    }

    tl::expected<void, ErrorCode> Destroy(
        std::unique_ptr<EgmStorePoolAllocation>& allocation) override {
        const size_t id = AllocationId(allocation->base());
        const bool fail = Record(Operation::kDestroy, id, false, "allocation");
        if (fail) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        live_allocations_.erase(id);
        allocation_by_base_.erase(allocation->base());
        allocation.reset();
        return {};
    }

    size_t Count(Operation operation) const {
        return static_cast<size_t>(std::count_if(
            events_.begin(), events_.end(),
            [&](const Event& event) { return event.operation == operation; }));
    }

    std::vector<Event> Events(Operation operation) const {
        std::vector<Event> selected;
        std::copy_if(
            events_.begin(), events_.end(), std::back_inserter(selected),
            [&](const Event& event) { return event.operation == operation; });
        return selected;
    }

    size_t EventIndex(Operation operation, size_t occurrence = 1) const {
        size_t seen = 0;
        for (size_t index = 0; index < events_.size(); ++index) {
            if (events_[index].operation != operation) continue;
            if (++seen == occurrence) return index;
        }
        return events_.size();
    }

    const std::vector<Event>& events() const { return events_; }
    const std::set<size_t>& live_allocations() const {
        return live_allocations_;
    }
    const std::set<size_t>& local_registrations() const {
        return local_registrations_;
    }
    const std::set<size_t>& global_registrations() const {
        return global_registrations_;
    }
    size_t mounted_segment_count() const { return mounted_segments_.size(); }
    bool allocator_installed() const { return allocator_installed_; }

   private:
    static constexpr size_t kAllocationGranularity = 4096;

    bool Record(Operation operation, size_t allocation_id, bool flag,
                std::string context) {
        const size_t ordinal = ++operation_calls_[operation];
        events_.push_back(Event{.operation = operation,
                                .allocation_id = allocation_id,
                                .ordinal = ordinal,
                                .flag = flag,
                                .context = std::move(context)});
        auto failures = failure_ordinals_.find(operation);
        return failures != failure_ordinals_.end() &&
               failures->second.contains(ordinal);
    }

    size_t AllocationId(void* base) const {
        auto allocation = allocation_by_base_.find(base);
        return allocation == allocation_by_base_.end() ? 0 : allocation->second;
    }

    tl::expected<void, ErrorCode> UnregisterImpl(void* base,
                                                 bool update_metadata,
                                                 const char* context) {
        const size_t id = AllocationId(base);
        const bool fail =
            Record(Operation::kUnregister, id, update_metadata, context);
        if (fail) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        if (update_metadata) {
            global_registrations_.erase(id);
        } else {
            local_registrations_.erase(id);
        }
        return {};
    }

    size_t next_allocation_id_ = 1;
    std::map<Operation, size_t> operation_calls_;
    std::map<Operation, std::set<size_t>> failure_ordinals_;
    std::vector<Event> events_;
    std::map<void*, size_t> allocation_by_base_;
    std::set<size_t> live_allocations_;
    std::set<size_t> local_registrations_;
    std::set<size_t> global_registrations_;
    std::map<UUID, size_t> mounted_segments_;
    bool allocator_installed_ = false;
    bool register_side_effect_on_failure_ = false;
};

struct State {
    std::vector<EgmStorePoolGlobalRecord> globals;
    std::optional<EgmStorePoolLocalRecord> local;
    bool allocator_installed = false;
};

EgmStorePoolPlan PlanWithChunks(size_t chunk_count) {
    EgmStorePoolPlan plan;
    plan.requested_total = chunk_count * 4096;
    plan.effective_total = plan.requested_total;
    plan.common_alignment = 4096;
    for (size_t index = 0; index < chunk_count; ++index) {
        plan.chunks.push_back(EgmStorePoolChunkPlan{
            .node_id = static_cast<int>(index % 2),
            .chunk_bytes = 4096,
            .plan_index = index,
        });
    }
    return plan;
}

auto RunSetup(FakeOperations& operations, State& state,
              const EgmStorePoolPlan& plan, size_t local_size) {
    return SetupEgmStorePoolOrchestration(operations, plan, 0, local_size,
                                          state.globals, state.local,
                                          state.allocator_installed);
}

bool Cleanup(FakeOperations& operations, State& state) {
    return CleanupEgmStorePoolOrchestration(
        operations, state.globals, state.local, state.allocator_installed);
}

void ExpectNoPublishedOrOwnedState(const FakeOperations& operations,
                                   const State& state) {
    EXPECT_TRUE(operations.live_allocations().empty());
    EXPECT_TRUE(operations.local_registrations().empty());
    EXPECT_TRUE(operations.global_registrations().empty());
    EXPECT_EQ(operations.mounted_segment_count(), 0U);
    EXPECT_FALSE(operations.allocator_installed());
    EXPECT_TRUE(state.globals.empty());
    EXPECT_FALSE(state.local.has_value());
    EXPECT_FALSE(state.allocator_installed);
}

TEST(EgmStorePoolOrchestratorTest,
     ProviderConsumerAndHybridSizesRetainTheirMeaning) {
    struct Scenario {
        const char* name;
        size_t global_chunks;
        size_t local_size;
        size_t expected_allocations;
        size_t expected_registers;
        size_t expected_mounts;
    };
    const Scenario scenarios[] = {
        {"provider-only", 2, 0, 2, 0, 2},
        {"consumer-only", 0, 4096, 1, 1, 0},
        {"hybrid", 2, 4096, 3, 1, 2},
    };

    for (const auto& scenario : scenarios) {
        SCOPED_TRACE(scenario.name);
        FakeOperations operations;
        State state;
        auto setup =
            RunSetup(operations, state, PlanWithChunks(scenario.global_chunks),
                     scenario.local_size);
        ASSERT_TRUE(setup);
        EXPECT_EQ(operations.Count(Operation::kAllocate),
                  scenario.expected_allocations);
        EXPECT_EQ(operations.Count(Operation::kRegister),
                  scenario.expected_registers);
        EXPECT_EQ(operations.Count(Operation::kMount),
                  scenario.expected_mounts);

        const size_t last_allocate = operations.EventIndex(
            Operation::kAllocate, scenario.expected_allocations);
        const size_t first_register =
            operations.EventIndex(Operation::kRegister);
        const size_t first_mount = operations.EventIndex(Operation::kMount);
        EXPECT_LT(last_allocate, std::min(first_register, first_mount));
        for (const auto& event : operations.Events(Operation::kRegister)) {
            EXPECT_FALSE(event.flag) << "local registration must not be remote";
        }
        auto mounts = operations.Events(Operation::kMount);
        EXPECT_TRUE(std::is_sorted(mounts.begin(), mounts.end(),
                                   [](const Event& left, const Event& right) {
                                       return left.allocation_id <
                                              right.allocation_id;
                                   }))
            << "global chunks must mount in plan/allocation order";

        ASSERT_TRUE(Cleanup(operations, state));
        const size_t events_after_first_cleanup = operations.events().size();
        EXPECT_TRUE(Cleanup(operations, state));
        EXPECT_EQ(operations.events().size(), events_after_first_cleanup)
            << "teardown must be idempotent";
        ExpectNoPublishedOrOwnedState(operations, state);
    }
}

TEST(EgmStorePoolOrchestratorTest,
     EveryAllocationFailureRollsBackWithoutPublication) {
    for (size_t fail_ordinal = 1; fail_ordinal <= 3; ++fail_ordinal) {
        SCOPED_TRACE(fail_ordinal);
        FakeOperations operations;
        operations.Fail(Operation::kAllocate, {fail_ordinal});
        State state;

        auto setup = RunSetup(operations, state, PlanWithChunks(2), 4096);
        ASSERT_FALSE(setup);
        EXPECT_EQ(setup.error().error, ErrorCode::INTERNAL_ERROR);
        EXPECT_EQ(setup.error().stage,
                  EgmStorePoolOrchestrationStage::kAllocation);
        EXPECT_EQ(operations.Count(Operation::kRegister), 0U);
        EXPECT_EQ(operations.Count(Operation::kMount), 0U);

        ASSERT_TRUE(Cleanup(operations, state));
        ExpectNoPublishedOrOwnedState(operations, state);
    }
}

TEST(EgmStorePoolOrchestratorTest,
     LocalRegisterSideEffectThenFailureIsCompensated) {
    FakeOperations operations;
    operations.Fail(Operation::kRegister, {1});
    operations.SetRegisterSideEffectOnFailure(true);
    State state;

    auto setup = RunSetup(operations, state, PlanWithChunks(0), 4096);
    ASSERT_FALSE(setup);
    EXPECT_EQ(setup.error().error, ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(setup.error().stage,
              EgmStorePoolOrchestrationStage::kRegistration);
    ASSERT_EQ(operations.local_registrations().size(), 1U);
    ASSERT_TRUE(state.local);
    EXPECT_TRUE(state.local->registration_attempted);

    ASSERT_TRUE(Cleanup(operations, state));
    const size_t unregister = operations.EventIndex(Operation::kUnregister);
    const size_t release = operations.EventIndex(Operation::kReleaseAllocator);
    const size_t destroy = operations.EventIndex(Operation::kDestroy);
    EXPECT_LT(unregister, release);
    EXPECT_LT(release, destroy);
    ExpectNoPublishedOrOwnedState(operations, state);
}

TEST(EgmStorePoolOrchestratorTest,
     AllocatorViewFailureDestroysOwnersWithoutPublishing) {
    FakeOperations operations;
    operations.Fail(Operation::kInstallAllocator, {1});
    State state;

    auto setup = RunSetup(operations, state, PlanWithChunks(2), 4096);
    ASSERT_FALSE(setup);
    EXPECT_EQ(setup.error().error, ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(setup.error().stage,
              EgmStorePoolOrchestrationStage::kRegistration);
    EXPECT_FALSE(operations.allocator_installed());
    EXPECT_FALSE(state.allocator_installed);
    EXPECT_EQ(operations.Count(Operation::kRegister), 0U);
    EXPECT_EQ(operations.Count(Operation::kMount), 0U);
    EXPECT_EQ(operations.live_allocations().size(), 3U);

    ASSERT_TRUE(Cleanup(operations, state));
    EXPECT_EQ(operations.Count(Operation::kReleaseAllocator), 0U);
    EXPECT_EQ(operations.Count(Operation::kDestroy), 3U);
    ExpectNoPublishedOrOwnedState(operations, state);
}

TEST(EgmStorePoolOrchestratorTest,
     LaterMountFailureUnwindsEarlierChunksInReverseOrder) {
    FakeOperations operations;
    operations.Fail(Operation::kMount, {3});
    State state;

    auto setup = RunSetup(operations, state, PlanWithChunks(3), 4096);
    ASSERT_FALSE(setup);
    EXPECT_EQ(setup.error().error, ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(setup.error().stage, EgmStorePoolOrchestrationStage::kMount);

    ASSERT_TRUE(Cleanup(operations, state));
    auto unmounts = operations.Events(Operation::kUnmount);
    ASSERT_EQ(unmounts.size(), 2U);
    EXPECT_GT(unmounts[0].allocation_id, unmounts[1].allocation_id)
        << "earlier mounted chunks must unwind in reverse order";

    auto unregisters = operations.Events(Operation::kUnregister);
    ASSERT_GE(unregisters.size(), 3U);
    EXPECT_EQ(unregisters[0].context, "client-compensation");
    EXPECT_EQ(unregisters[1].context, "orchestrator-fallback");
    EXPECT_EQ(unregisters[0].allocation_id, unregisters[1].allocation_id)
        << "already removed current registration must be harmless";
    ExpectNoPublishedOrOwnedState(operations, state);
}

TEST(EgmStorePoolOrchestratorTest,
     FailedClientCompensationIsRecoveredByOrchestratorFallback) {
    FakeOperations operations;
    operations.Fail(Operation::kMount, {1});
    operations.Fail(Operation::kUnregister, {1});
    State state;

    auto setup = RunSetup(operations, state, PlanWithChunks(1), 0);
    ASSERT_FALSE(setup);
    EXPECT_EQ(setup.error().error, ErrorCode::INTERNAL_ERROR);
    ASSERT_EQ(operations.global_registrations().size(), 1U);

    ASSERT_TRUE(Cleanup(operations, state));
    auto unregisters = operations.Events(Operation::kUnregister);
    ASSERT_EQ(unregisters.size(), 2U);
    EXPECT_EQ(unregisters[0].context, "client-compensation");
    EXPECT_EQ(unregisters[1].context, "orchestrator-fallback");
    ExpectNoPublishedOrOwnedState(operations, state);
}

TEST(EgmStorePoolOrchestratorTest,
     DoubleCompensationFailureRetainsOwnershipUntilRetry) {
    FakeOperations operations;
    operations.Fail(Operation::kMount, {1});
    operations.Fail(Operation::kUnregister, {1, 2});
    State state;

    auto setup = RunSetup(operations, state, PlanWithChunks(1), 0);
    ASSERT_FALSE(setup);
    const ErrorCode original_error = setup.error().error;
    EXPECT_EQ(original_error, ErrorCode::INTERNAL_ERROR);

    EXPECT_FALSE(Cleanup(operations, state));
    EXPECT_EQ(operations.global_registrations().size(), 1U);
    EXPECT_EQ(operations.live_allocations().size(), 1U);
    EXPECT_TRUE(operations.allocator_installed());
    EXPECT_EQ(operations.Count(Operation::kDestroy), 0U);

    operations.ClearFailures();
    ASSERT_TRUE(Cleanup(operations, state));
    EXPECT_EQ(original_error, ErrorCode::INTERNAL_ERROR)
        << "cleanup retries must not replace the original setup error";
    ExpectNoPublishedOrOwnedState(operations, state);
}

TEST(EgmStorePoolOrchestratorTest,
     UnmountFailureRetainsAllOwnershipAndRetryIsSafe) {
    FakeOperations operations;
    State state;
    ASSERT_TRUE(RunSetup(operations, state, PlanWithChunks(2), 0));
    operations.Fail(Operation::kUnmount, {1});

    EXPECT_FALSE(Cleanup(operations, state));
    EXPECT_EQ(operations.mounted_segment_count(), 1U);
    EXPECT_EQ(operations.live_allocations().size(), 2U);
    EXPECT_TRUE(operations.allocator_installed());
    EXPECT_EQ(operations.Count(Operation::kDestroy), 0U);

    operations.ClearFailures();
    ASSERT_TRUE(Cleanup(operations, state));
    ExpectNoPublishedOrOwnedState(operations, state);
}

TEST(EgmStorePoolOrchestratorTest,
     UnregisterFailureRetainsLocalOwnershipAndRetryIsSafe) {
    FakeOperations operations;
    State state;
    ASSERT_TRUE(RunSetup(operations, state, PlanWithChunks(0), 4096));
    operations.Fail(Operation::kUnregister, {1});

    EXPECT_FALSE(Cleanup(operations, state));
    EXPECT_EQ(operations.local_registrations().size(), 1U);
    EXPECT_EQ(operations.live_allocations().size(), 1U);
    EXPECT_TRUE(operations.allocator_installed());

    operations.ClearFailures();
    ASSERT_TRUE(Cleanup(operations, state));
    ExpectNoPublishedOrOwnedState(operations, state);
}

TEST(EgmStorePoolOrchestratorTest,
     DestroyFailureHappensAfterAllocatorReleaseAndRetriesIdempotently) {
    FakeOperations operations;
    State state;
    ASSERT_TRUE(RunSetup(operations, state, PlanWithChunks(2), 4096));
    operations.Fail(Operation::kDestroy, {2});

    EXPECT_FALSE(Cleanup(operations, state));
    EXPECT_EQ(operations.mounted_segment_count(), 0U);
    EXPECT_TRUE(operations.global_registrations().empty());
    EXPECT_TRUE(operations.local_registrations().empty());
    EXPECT_FALSE(operations.allocator_installed());
    EXPECT_EQ(operations.Count(Operation::kDestroy), 3U)
        << "cleanup must attempt every allocation destroy";
    EXPECT_EQ(operations.live_allocations().size(), 1U);
    EXPECT_LT(operations.EventIndex(Operation::kReleaseAllocator),
              operations.EventIndex(Operation::kDestroy));

    operations.ClearFailures();
    ASSERT_TRUE(Cleanup(operations, state));
    const size_t events_after_retry = operations.events().size();
    EXPECT_TRUE(Cleanup(operations, state));
    EXPECT_EQ(operations.events().size(), events_after_retry);
    ExpectNoPublishedOrOwnedState(operations, state);
}

TEST(EgmStorePoolOrchestratorTest,
     OutstandingAllocatorViewRetainsEveryVmmOwnerUntilRetry) {
    FakeOperations operations;
    State state;
    ASSERT_TRUE(RunSetup(operations, state, PlanWithChunks(2), 4096));
    operations.Fail(Operation::kReleaseAllocator, {1});

    EXPECT_FALSE(Cleanup(operations, state));
    EXPECT_EQ(operations.mounted_segment_count(), 0U);
    EXPECT_TRUE(operations.global_registrations().empty());
    EXPECT_TRUE(operations.local_registrations().empty());
    EXPECT_TRUE(operations.allocator_installed());
    EXPECT_TRUE(state.allocator_installed);
    EXPECT_EQ(operations.Count(Operation::kDestroy), 0U);
    EXPECT_EQ(operations.live_allocations().size(), 3U);

    operations.ClearFailures();
    ASSERT_TRUE(Cleanup(operations, state));
    EXPECT_EQ(operations.Count(Operation::kReleaseAllocator), 2U);
    ExpectNoPublishedOrOwnedState(operations, state);
}

TEST(EgmStorePoolOrchestratorTest,
     OwnershipStatsIncludeOnlyLiveGlobalAndLocalAllocations) {
    FakeOperations operations;
    State state;
    ASSERT_TRUE(RunSetup(operations, state, PlanWithChunks(2), 8192));

    auto stats = GetEgmStorePoolOwnershipStats(state.globals, state.local);
    EXPECT_EQ(stats.allocations, 3U);
    EXPECT_EQ(stats.bytes, 2U * 4096U + 8192U);

    ASSERT_TRUE(Cleanup(operations, state));
    stats = GetEgmStorePoolOwnershipStats(state.globals, state.local);
    EXPECT_EQ(stats.allocations, 0U);
    EXPECT_EQ(stats.bytes, 0U);
}

TEST(EgmStorePoolOrchestratorTest,
     ProcessQuarantineTakesOwnershipWithoutDestructorAllocation) {
    ASSERT_EXIT(
        {
            FakeOperations operations;
            State state;
            if (!RunSetup(operations, state, PlanWithChunks(2), 4096)) {
                _exit(10);
            }
            auto node = PrepareEgmStorePoolProcessQuarantineNode();
            if (!node) _exit(11);
            if (!QuarantineEgmStorePoolOwnership(std::move(node), state.globals,
                                                 state.local)) {
                _exit(12);
            }
            const auto stats = GetEgmStorePoolProcessQuarantineStats();
            if (stats.clients != 1 || stats.allocations != 3 ||
                stats.bytes != 3U * 4096U || !state.globals.empty() ||
                state.local.has_value()) {
                _exit(13);
            }
            RealClient client;
            if (client.health_check() != HC_PROCESS_QUARANTINED) _exit(14);
            _exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

}  // namespace
}  // namespace mooncake
