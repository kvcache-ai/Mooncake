#include "egm_store_pool_orchestrator.h"

#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <new>
#include <utility>

#include "egm_store_pool.h"

namespace mooncake {
namespace {

using Clock = std::chrono::steady_clock;

uint64_t ElapsedMicros(Clock::time_point start) {
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                             Clock::now() - start)
                             .count();
    return static_cast<uint64_t>(std::max<int64_t>(elapsed, 0));
}

tl::unexpected<EgmStorePoolOrchestrationFailure> Failure(
    ErrorCode error, EgmStorePoolOrchestrationStage stage) {
    return tl::make_unexpected(
        EgmStorePoolOrchestrationFailure{.error = error, .stage = stage});
}

struct ProcessQuarantineRegistry {
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
    EgmStorePoolProcessQuarantineNode* head = nullptr;
    EgmStorePoolProcessQuarantineStats stats;
};

ProcessQuarantineRegistry& GetProcessQuarantineRegistry() {
    // Quarantined VMM owners deliberately outlive ordinary static destruction.
    // Leaking the small registry object also keeps its lock valid for late
    // atexit/signal cleanup paths.
    static auto* registry = new ProcessQuarantineRegistry();
    return *registry;
}

class ProcessQuarantineGuard {
   public:
    explicit ProcessQuarantineGuard(ProcessQuarantineRegistry& registry)
        : registry_(registry) {
        while (registry_.lock.test_and_set(std::memory_order_acquire)) {
        }
    }

    ~ProcessQuarantineGuard() {
        registry_.lock.clear(std::memory_order_release);
    }

   private:
    ProcessQuarantineRegistry& registry_;
};

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
    if (left > std::numeric_limits<uint64_t>::max() - right) {
        return std::numeric_limits<uint64_t>::max();
    }
    return left + right;
}

}  // namespace

tl::expected<void, EgmStorePoolOrchestrationFailure>
SetupEgmStorePoolOrchestration(
    EgmStorePoolOperations& operations, const EgmStorePoolPlan& plan,
    int local_numa_node, size_t local_buffer_size,
    std::vector<EgmStorePoolGlobalRecord>& global_records,
    std::optional<EgmStorePoolLocalRecord>& local_record,
    bool& allocator_installed, const EgmStorePoolStageObserver& observer) {
    if (!global_records.empty() || local_record || allocator_installed) {
        LOG(ERROR) << "EGM Store Pool orchestration refused non-empty state";
        return Failure(ErrorCode::INVALID_PARAMS,
                       EgmStorePoolOrchestrationStage::kAllocation);
    }

    auto observe = [&](EgmStorePoolOrchestrationStage stage,
                       Clock::time_point start, bool success) {
        if (observer) observer(stage, ElapsedMicros(start), success);
    };

    // Complete all allocations before the first allocator/TE/Master
    // publication. Records take ownership immediately so every later failure
    // is handled by the same explicit destroy path.
    auto stage_start = Clock::now();
    global_records.reserve(plan.chunks.size());
    if (local_buffer_size > 0) {
        EgmStorePoolAllocationRequest request;
        request.role = EgmStorePoolAllocationRole::kLocal;
        request.numa_node = local_numa_node;
        request.requested_length = local_buffer_size;
        request.fabric_exportable = false;

        auto allocation = operations.Allocate(request);
        if (!allocation) {
            LOG(ERROR) << "EGM Store Pool local allocation failed";
            observe(EgmStorePoolOrchestrationStage::kAllocation, stage_start,
                    false);
            return Failure(allocation.error(),
                           EgmStorePoolOrchestrationStage::kAllocation);
        }
        EgmStorePoolLocalRecord record;
        record.allocation = std::move(*allocation);
        local_record.emplace(std::move(record));
        if (!local_record->allocation ||
            local_record->allocation->base() == nullptr ||
            local_record->allocation->length() < local_buffer_size) {
            LOG(ERROR) << "EGM Store Pool local allocation violated plan";
            observe(EgmStorePoolOrchestrationStage::kAllocation, stage_start,
                    false);
            return Failure(ErrorCode::INVALID_PARAMS,
                           EgmStorePoolOrchestrationStage::kAllocation);
        }
    }

    for (const auto& chunk : plan.chunks) {
        EgmStorePoolAllocationRequest request;
        request.role = EgmStorePoolAllocationRole::kGlobal;
        request.numa_node = chunk.node_id;
        request.requested_length = chunk.chunk_bytes;
        request.fabric_exportable = true;
        request.required_va_alignment = plan.common_alignment;
        request.plan_index = chunk.plan_index;

        auto allocation = operations.Allocate(request);
        if (!allocation) {
            LOG(ERROR) << "EGM Store Pool global allocation failed for node "
                       << chunk.node_id << ", chunk=" << chunk.plan_index;
            observe(EgmStorePoolOrchestrationStage::kAllocation, stage_start,
                    false);
            return Failure(allocation.error(),
                           EgmStorePoolOrchestrationStage::kAllocation);
        }

        EgmStorePoolGlobalRecord record;
        record.allocation = std::move(*allocation);
        record.numa_node = chunk.node_id;
        record.plan_index = chunk.plan_index;
        global_records.push_back(std::move(record));
        auto& stored = global_records.back();

        const bool invalid =
            !stored.allocation || stored.allocation->base() == nullptr ||
            stored.allocation->length() != chunk.chunk_bytes ||
            plan.common_alignment == 0 ||
            reinterpret_cast<uintptr_t>(stored.allocation->base()) %
                    plan.common_alignment !=
                0;
        if (invalid) {
            LOG(ERROR) << "EGM Store Pool global allocation violated plan "
                          "for node "
                       << chunk.node_id << ", chunk=" << chunk.plan_index;
            observe(EgmStorePoolOrchestrationStage::kAllocation, stage_start,
                    false);
            return Failure(ErrorCode::INVALID_PARAMS,
                           EgmStorePoolOrchestrationStage::kAllocation);
        }
    }
    observe(EgmStorePoolOrchestrationStage::kAllocation, stage_start, true);

    stage_start = Clock::now();
    auto installed = operations.InstallAllocatorView(
        local_record ? local_record->allocation.get() : nullptr,
        local_buffer_size);
    if (!installed) {
        LOG(ERROR) << "EGM Store Pool allocator view installation failed";
        observe(EgmStorePoolOrchestrationStage::kRegistration, stage_start,
                false);
        return Failure(installed.error(),
                       EgmStorePoolOrchestrationStage::kRegistration);
    }
    allocator_installed = true;

    if (local_record) {
        local_record->registration_attempted = true;
        auto registered = operations.RegisterLocal(
            local_record->allocation->base(), local_buffer_size, false);
        if (!registered) {
            LOG(ERROR) << "EGM Store Pool local registration failed";
            observe(EgmStorePoolOrchestrationStage::kRegistration, stage_start,
                    false);
            return Failure(registered.error(),
                           EgmStorePoolOrchestrationStage::kRegistration);
        }
        local_record->registered = true;
    }
    observe(EgmStorePoolOrchestrationStage::kRegistration, stage_start, true);

    stage_start = Clock::now();
    for (auto& record : global_records) {
        record.registration_attempted = true;
        auto mounted = operations.MountGlobal(record.allocation->base(),
                                              record.allocation->length());
        if (!mounted) {
            LOG(ERROR) << "EGM Store Pool mount failed for node "
                       << record.numa_node << ", chunk=" << record.plan_index
                       << ", bytes=" << record.allocation->length();
            observe(EgmStorePoolOrchestrationStage::kMount, stage_start, false);
            return Failure(mounted.error(),
                           EgmStorePoolOrchestrationStage::kMount);
        }
        record.mounted_segment_id = *mounted;
    }
    observe(EgmStorePoolOrchestrationStage::kMount, stage_start, true);
    return {};
}

bool CleanupEgmStorePoolOrchestration(
    EgmStorePoolOperations& operations,
    std::vector<EgmStorePoolGlobalRecord>& global_records,
    std::optional<EgmStorePoolLocalRecord>& local_record,
    bool& allocator_installed) {
    bool publication_cleanup_succeeded = true;
    auto record_cleanup = [&](bool success, const char* operation) {
        if (!success) {
            publication_cleanup_succeeded = false;
            LOG(ERROR) << "EGM Store Pool " << operation << " cleanup failed";
        }
    };

    // Successful Master publications are reversed strictly by UUID and in
    // reverse plan order. A repeated Provider name is never used as identity.
    for (auto it = global_records.rbegin(); it != global_records.rend(); ++it) {
        if (!it->mounted_segment_id) continue;
        auto unmounted = operations.UnmountGlobal(*it->mounted_segment_id);
        record_cleanup(unmounted.has_value(), "global unmount");
        if (unmounted) {
            it->mounted_segment_id.reset();
            it->registration_attempted = false;
        }
    }

    // MountGlobal owns the first current-chunk compensation attempt. This is
    // the orchestrator's idempotent fallback, including the harmless case in
    // which the inner attempt already removed the TE registration.
    for (auto it = global_records.rbegin(); it != global_records.rend(); ++it) {
        if (it->mounted_segment_id || !it->registration_attempted) continue;
        auto unregistered =
            operations.UnregisterIfPresent(it->allocation->base(), true);
        record_cleanup(unregistered.has_value(),
                       "current registration fallback");
        if (unregistered) it->registration_attempted = false;
    }

    if (local_record && local_record->registration_attempted) {
        auto unregistered = operations.UnregisterIfPresent(
            local_record->allocation->base(), false);
        record_cleanup(unregistered.has_value(), "local unregister");
        if (unregistered) {
            local_record->registration_attempted = false;
            local_record->registered = false;
        }
    }

    if (!publication_cleanup_succeeded) {
        LOG(ERROR) << "EGM Store Pool publication cleanup is incomplete; "
                      "retaining allocator view and VMM ownership for retry";
        return false;
    }

    // Drop all aliases before destroying any VMM allocation. This ordering is
    // also used when only a local workspace or only global Provider chunks
    // exist.
    if (allocator_installed) {
        auto released = operations.ReleaseAllocatorView();
        if (!released) {
            LOG(ERROR) << "EGM Store Pool allocator view release failed; "
                          "retaining VMM ownership for cleanup retry";
            return false;
        }
        allocator_installed = false;
    }

    bool destroy_succeeded = true;
    auto destroy = [&](std::unique_ptr<EgmStorePoolAllocation>& allocation,
                       const char* role) {
        if (!allocation) return;
        auto result = operations.Destroy(allocation);
        if (!result || allocation) {
            destroy_succeeded = false;
            LOG(ERROR) << "EGM Store Pool " << role
                       << " allocation destroy failed";
        }
    };

    for (auto it = global_records.rbegin(); it != global_records.rend(); ++it) {
        destroy(it->allocation, "global");
    }
    if (local_record) destroy(local_record->allocation, "local");

    if (!destroy_succeeded) {
        LOG(ERROR) << "EGM Store Pool allocation destruction is incomplete; "
                      "retaining ownership records for retry";
        return false;
    }

    local_record.reset();
    global_records.clear();
    return true;
}

EgmStorePoolOwnershipStats GetEgmStorePoolOwnershipStats(
    const std::vector<EgmStorePoolGlobalRecord>& global_records,
    const std::optional<EgmStorePoolLocalRecord>& local_record) {
    EgmStorePoolOwnershipStats stats;
    auto include = [&stats](const auto& allocation) {
        if (!allocation) return;
        ++stats.allocations;
        stats.bytes = SaturatingAdd(
            stats.bytes, static_cast<uint64_t>(allocation->length()));
    };
    for (const auto& record : global_records) include(record.allocation);
    if (local_record) include(local_record->allocation);
    return stats;
}

std::unique_ptr<EgmStorePoolProcessQuarantineNode>
PrepareEgmStorePoolProcessQuarantineNode() noexcept {
    return std::unique_ptr<EgmStorePoolProcessQuarantineNode>(
        new (std::nothrow) EgmStorePoolProcessQuarantineNode());
}

bool QuarantineEgmStorePoolOwnership(
    std::unique_ptr<EgmStorePoolProcessQuarantineNode> node,
    std::vector<EgmStorePoolGlobalRecord>& global_records,
    std::optional<EgmStorePoolLocalRecord>& local_record) noexcept {
    if (!node) return false;

    node->ownership =
        GetEgmStorePoolOwnershipStats(global_records, local_record);
    node->global_records = std::move(global_records);
    node->local_record = std::move(local_record);
    global_records.clear();
    local_record.reset();

    auto& registry = GetProcessQuarantineRegistry();
    ProcessQuarantineGuard guard(registry);
    node->next = registry.head;
    registry.head = node.release();
    ++registry.stats.clients;
    registry.stats.allocations += registry.head->ownership.allocations;
    registry.stats.bytes =
        SaturatingAdd(registry.stats.bytes, registry.head->ownership.bytes);
    return true;
}

EgmStorePoolProcessQuarantineStats
GetEgmStorePoolProcessQuarantineStats() noexcept {
    auto& registry = GetProcessQuarantineRegistry();
    ProcessQuarantineGuard guard(registry);
    return registry.stats;
}

}  // namespace mooncake
