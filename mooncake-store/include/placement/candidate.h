#pragma once

#include <cstddef>
#include <memory>
#include <utility>

#include "allocator.h"
#include "client_session.h"

namespace mooncake {

enum class AllocationCandidateKind {
    NATIVE = 0,
    CXL,
};

inline constexpr size_t kAllocationCandidateKindCount =
    static_cast<size_t>(AllocationCandidateKind::CXL) + 1;

// A stable allocation endpoint published to PlacementIndex. RegionResource
// owns the candidate and must outlive every placement reference to it.
class AllocationCandidate {
   public:
    virtual ~AllocationCandidate() = default;

    virtual std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const = 0;
    virtual AllocationCandidateKind Kind() const noexcept = 0;

    // Liveness and buffer identity belong to a mounted region, not its
    // allocator (CXL regions can share one allocator).
    bool IsServing() const {
        const auto session = client_session();
        return allocation_lifetime_.isAvailable() &&
               (session ? session->IsServing()
                        : !buffer_lifetime_.requiresClientSession());
    }

    void BindClientSession(ClientSessionPtr session) {
        buffer_lifetime_.bindClientSession(std::move(session));
    }
    ClientSessionPtr client_session() const {
        return buffer_lifetime_.clientSession();
    }
    void BindBuffer(AllocatedBuffer& buffer) const {
        buffer.bindSegmentLifetime(buffer_lifetime_);
    }
    bool OwnsBuffer(const AllocatedBuffer& buffer) const {
        return buffer.segment_lifetime_ == buffer_lifetime_;
    }
    void SetAvailability(bool allocatable, bool readable) {
        allocation_lifetime_.setAvailable(allocatable);
        buffer_lifetime_.setAvailable(readable);
    }
    // Allocator replacement preserves both the region identity and its owner.
    void InheritBinding(const AllocationCandidate& previous) {
        allocation_lifetime_ = previous.allocation_lifetime_;
        buffer_lifetime_ = previous.buffer_lifetime_;
    }

    size_t Capacity() const { return allocator_->capacity(); }
    size_t Used() const { return allocator_->size(); }

    const std::shared_ptr<BufferAllocatorBase>& allocator_handle()
        const noexcept {
        return allocator_;
    }

   protected:
    explicit AllocationCandidate(std::shared_ptr<BufferAllocatorBase> allocator,
                                 bool requires_client_session = false)
        : allocator_(std::move(allocator)),
          buffer_lifetime_(requires_client_session) {}

    BufferAllocatorBase& allocator() const noexcept { return *allocator_; }

    std::unique_ptr<AllocatedBuffer> AllocateRegistered(size_t size) const {
        if (!IsServing()) return nullptr;
        const auto session = client_session();
        auto buffer = allocator().allocate(size);
        if (!buffer) return nullptr;
        BindBuffer(*buffer);
        if (!IsServing() || session != client_session()) return nullptr;
        return buffer;
    }

   private:
    std::shared_ptr<BufferAllocatorBase> allocator_;
    SegmentLifetime allocation_lifetime_;
    SegmentLifetime buffer_lifetime_;
};

}  // namespace mooncake
