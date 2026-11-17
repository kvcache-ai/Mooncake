#include "segment_allocator_registration.h"

#include <utility>

namespace mooncake {

SegmentAllocatorRegistration::SegmentAllocatorRegistration(
    std::shared_ptr<BufferAllocatorBase> allocator, SegmentLifetime lifetime,
    bool allocatable)
    : allocator_(std::move(allocator)),
      allocatable_(allocatable),
      lifetime_(std::move(lifetime)) {}

bool SegmentAllocatorRegistration::IsAllocatable() const noexcept {
    return allocatable_.load(std::memory_order_acquire) &&
           lifetime_.isServing();
}

std::unique_ptr<AllocatedBuffer> SegmentAllocatorRegistration::Allocate(
    size_t size) const {
    if (!IsAllocatable()) {
        return nullptr;
    }
    auto buffer = allocator_->allocate(size);
    if (!buffer) {
        return nullptr;
    }
    if (!IsAllocatable()) {
        return nullptr;
    }
    buffer->bindSegmentLifetime(lifetime_);
    return buffer;
}

const ClientSessionSharedPtr& SegmentAllocatorRegistration::GetClientSession()
    const noexcept {
    return lifetime_.getClientSession();
}

std::shared_ptr<SegmentAllocatorRegistration>
SegmentAllocatorRegistration::WithAllocator(
    std::shared_ptr<BufferAllocatorBase> replacement) const {
    return std::shared_ptr<SegmentAllocatorRegistration>(
        new SegmentAllocatorRegistration(std::move(replacement), lifetime_));
}

std::shared_ptr<SegmentAllocatorRegistration>
SegmentAllocatorRegistration::ReplaceClientSession(
    ClientSessionSharedPtr session,
    std::shared_ptr<BufferAllocatorBase> allocator, bool allocatable) {
    SegmentLifetime next_lifetime(std::move(session));
    if (!lifetime_.isValid()) {
        next_lifetime.Invalidate();
    }
    // Old buffers and detached snapshots retain the old generation and become
    // invalid; they must not silently migrate to a new client incarnation with
    // the same UUID.
    Invalidate();
    return std::shared_ptr<SegmentAllocatorRegistration>(
        new SegmentAllocatorRegistration(
            std::move(allocator), std::move(next_lifetime), allocatable));
}

void SegmentAllocatorRegistration::BindBuffer(AllocatedBuffer& buffer) const {
    buffer.bindSegmentLifetime(lifetime_);
}

void SegmentAllocatorRegistration::SetAllocatable(bool allocatable) noexcept {
    allocatable_.store(allocatable, std::memory_order_release);
}

void SegmentAllocatorRegistration::Invalidate() noexcept {
    allocatable_.store(false, std::memory_order_release);
    lifetime_.Invalidate();
}

}  // namespace mooncake
