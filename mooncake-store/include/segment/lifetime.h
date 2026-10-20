#pragma once

#include <atomic>
#include <memory>
#include <utility>

#include "client_liveness.h"
#include "segment/status.h"

namespace mooncake {

// Process-local identity of one mounted region, independent of its allocator.
// A placement candidate and every buffer it hands out share one instance, so a
// buffer stays bound to its own region even when several regions share a single
// allocator (CXL). Remounting a region inherits the lifetime of the previous
// mount; mounting a new region starts a fresh one.
//
// Writers are the mount, status, and unmount paths of a segment manager; both
// flags are atomic so readers on unrelated threads stay cheap.
class SegmentLifetime {
   public:
    SegmentLifetime() : state_(std::make_shared<State>()) {}

    // A null owner means the region has no client yet, as right after a
    // snapshot restore, and is treated as serving.
    void BindClientLiveness(std::shared_ptr<ClientLivenessRecord> owner) {
        std::atomic_store_explicit(&state_->owner, std::move(owner),
                                   std::memory_order_release);
    }

    [[nodiscard]] std::shared_ptr<ClientLivenessRecord> GetClientLiveness()
        const {
        return std::atomic_load_explicit(&state_->owner,
                                         std::memory_order_acquire);
    }

    // The single home of the status-to-availability rule: only an OK region
    // accepts new allocations, and only an unmounting one gives up the buffers
    // it already handed out.
    void SetStatus(SegmentStatus status) {
        state_->allocatable.store(status == SegmentStatus::OK,
                                  std::memory_order_release);
        state_->readable.store(status != SegmentStatus::UNMOUNTING,
                               std::memory_order_release);
    }

    // Legacy mount path, which tracks allocatability on its own.
    void SetAllocatable(bool allocatable) {
        state_->allocatable.store(allocatable, std::memory_order_release);
    }

    // The region is gone, so neither existing buffers nor new allocations may
    // use it again.
    void Invalidate() {
        state_->allocatable.store(false, std::memory_order_release);
        state_->readable.store(false, std::memory_order_release);
    }

    // Owner-aware access checks used by placement and replica visibility.
    [[nodiscard]] bool CanAllocate() const {
        return state_->allocatable.load(std::memory_order_acquire) &&
               OwnerIsServing();
    }

    [[nodiscard]] bool CanRead() const {
        return HasReadableRegion() && OwnerIsServing();
    }

    // Region-only access check, deliberately ignoring the owner: a suspected
    // owner stays readable so retaining guards can still release its buffers.
    [[nodiscard]] bool HasReadableRegion() const {
        return state_->readable.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool operator==(const SegmentLifetime& other) const {
        return state_ == other.state_;
    }

   private:
    struct State {
        std::atomic<bool> allocatable{true};
        std::atomic<bool> readable{true};
        std::shared_ptr<ClientLivenessRecord> owner;
    };

    [[nodiscard]] bool OwnerIsServing() const {
        const auto owner = GetClientLiveness();
        return !owner || owner->IsServing();
    }

    std::shared_ptr<State> state_;
};

}  // namespace mooncake
