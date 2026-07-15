#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct EgmStorePoolPlan;

// Type-erased allocation ownership lets the production CUDA VMM path and the
// ordinary CPU failure tests exercise the exact same orchestration state
// machine. Destroy is deliberately an operation rather than an implicit
// unique_ptr reset so failures retain ownership for an explicit cleanup retry.
class EgmStorePoolAllocation {
   public:
    virtual ~EgmStorePoolAllocation() = default;

    virtual void* base() const = 0;
    virtual size_t length() const = 0;
    virtual size_t granularity() const = 0;
};

enum class EgmStorePoolAllocationRole { kLocal, kGlobal };

struct EgmStorePoolAllocationRequest {
    EgmStorePoolAllocationRole role = EgmStorePoolAllocationRole::kGlobal;
    int numa_node = -1;
    size_t requested_length = 0;
    bool fabric_exportable = false;
    size_t required_va_alignment = 0;
    size_t plan_index = 0;
};

// Internal dependency boundary for the publication transaction. Production
// delegates to NvlinkVmmAllocation and Client; tests use a recording fake.
class EgmStorePoolOperations {
   public:
    virtual ~EgmStorePoolOperations() = default;

    virtual tl::expected<std::unique_ptr<EgmStorePoolAllocation>, ErrorCode>
    Allocate(const EgmStorePoolAllocationRequest& request) = 0;

    virtual tl::expected<void, ErrorCode> InstallAllocatorView(
        EgmStorePoolAllocation* local_allocation,
        size_t configured_local_length) = 0;
    // Releasing the non-owning allocator view can fail while caller-visible
    // BufferHandles still retain it. In that case cleanup must keep both the
    // allocator view and the backing VMM owner for an explicit retry.
    virtual tl::expected<void, ErrorCode> ReleaseAllocatorView() = 0;

    virtual tl::expected<void, ErrorCode> RegisterLocal(
        void* base, size_t length, bool remote_accessible) = 0;
    virtual tl::expected<UUID, ErrorCode> MountGlobal(void* base,
                                                      size_t length) = 0;
    virtual tl::expected<void, ErrorCode> UnmountGlobal(
        const UUID& segment_id) = 0;
    virtual tl::expected<void, ErrorCode> UnregisterIfPresent(
        void* base, bool update_metadata) = 0;

    virtual tl::expected<void, ErrorCode> Destroy(
        std::unique_ptr<EgmStorePoolAllocation>& allocation) = 0;
};

struct EgmStorePoolGlobalRecord {
    std::unique_ptr<EgmStorePoolAllocation> allocation;
    int numa_node = -1;
    size_t plan_index = 0;
    bool registration_attempted = false;
    std::optional<UUID> mounted_segment_id;
};

struct EgmStorePoolLocalRecord {
    std::unique_ptr<EgmStorePoolAllocation> allocation;
    // Set before RegisterLocal so a side-effect-then-fail implementation is
    // still compensated by the idempotent unregister path.
    bool registration_attempted = false;
    bool registered = false;
};

struct EgmStorePoolOwnershipStats {
    size_t allocations = 0;
    uint64_t bytes = 0;
};

// Destruction cannot safely recycle a VMM address while publication cleanup is
// still uncertain. RealClient preallocates one node before EGM Store Pool setup
// so its destructor can transfer ownership into a process-lifetime quarantine
// without allocating memory. New EGM Store Pool setup is refused while this
// quarantine is non-empty; explicit close() retry is the recovery path before
// object destruction.
struct EgmStorePoolProcessQuarantineNode {
    std::vector<EgmStorePoolGlobalRecord> global_records;
    std::optional<EgmStorePoolLocalRecord> local_record;
    EgmStorePoolOwnershipStats ownership;
    EgmStorePoolProcessQuarantineNode* next = nullptr;
};

struct EgmStorePoolProcessQuarantineStats {
    size_t clients = 0;
    size_t allocations = 0;
    uint64_t bytes = 0;
};

enum class EgmStorePoolOrchestrationStage {
    kAllocation,
    kRegistration,
    kMount,
};

struct EgmStorePoolOrchestrationFailure {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;
    EgmStorePoolOrchestrationStage stage =
        EgmStorePoolOrchestrationStage::kAllocation;
};

using EgmStorePoolStageObserver = std::function<void(
    EgmStorePoolOrchestrationStage stage, uint64_t duration_us, bool success)>;

// Allocate every local/global range before installing the allocator view or
// publishing anything. On failure, ownership remains in the supplied records;
// the caller must run CleanupEgmStorePoolOrchestration, as RealClient does
// through its setup rollback guard.
tl::expected<void, EgmStorePoolOrchestrationFailure>
SetupEgmStorePoolOrchestration(
    EgmStorePoolOperations& operations, const EgmStorePoolPlan& plan,
    int local_numa_node, size_t local_buffer_size,
    std::vector<EgmStorePoolGlobalRecord>& global_records,
    std::optional<EgmStorePoolLocalRecord>& local_record,
    bool& allocator_installed, const EgmStorePoolStageObserver& observer = {});

// Reverse publication first. Allocator views and VMM ownership are released
// only after every unmount/unregister succeeds. Failed cleanup retains the
// exact remaining state and is safe to retry.
bool CleanupEgmStorePoolOrchestration(
    EgmStorePoolOperations& operations,
    std::vector<EgmStorePoolGlobalRecord>& global_records,
    std::optional<EgmStorePoolLocalRecord>& local_record,
    bool& allocator_installed);

EgmStorePoolOwnershipStats GetEgmStorePoolOwnershipStats(
    const std::vector<EgmStorePoolGlobalRecord>& global_records,
    const std::optional<EgmStorePoolLocalRecord>& local_record);

std::unique_ptr<EgmStorePoolProcessQuarantineNode>
PrepareEgmStorePoolProcessQuarantineNode() noexcept;

// Last-resort fail-closed handoff used only after explicit cleanup can no
// longer be retried because the owning RealClient is being destroyed.
bool QuarantineEgmStorePoolOwnership(
    std::unique_ptr<EgmStorePoolProcessQuarantineNode> node,
    std::vector<EgmStorePoolGlobalRecord>& global_records,
    std::optional<EgmStorePoolLocalRecord>& local_record) noexcept;

EgmStorePoolProcessQuarantineStats
GetEgmStorePoolProcessQuarantineStats() noexcept;

}  // namespace mooncake
