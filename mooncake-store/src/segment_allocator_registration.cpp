#include "segment_allocator_registration.h"

#include <utility>

namespace mooncake {

SegmentAllocatorRegistration::SegmentAllocatorRegistration(
    std::shared_ptr<BufferAllocatorBase> allocator,
    ClientSessionSharedPtr owner_session)
    : allocator_(std::move(allocator)),
      lifetime_(SegmentLifetime(std::move(owner_session)).state_) {}

SegmentLifetime SegmentAllocatorRegistration::SnapshotLifetime() const {
    return SegmentLifetime(lifetime_.load(std::memory_order_acquire));
}

bool SegmentAllocatorRegistration::IsAllocatable() const {
    return allocatable_.load(std::memory_order_acquire) &&
           SnapshotLifetime().isServing();
}

std::unique_ptr<AllocatedBuffer> SegmentAllocatorRegistration::Allocate(
    size_t size) const {
    // Capture one lifetime generation across allocation, even if the segment
    // is rebound concurrently through another allocator-manager snapshot.
    const auto lifetime = SnapshotLifetime();
    if (!allocatable_.load(std::memory_order_acquire) ||
        !lifetime.isServing()) {
        return nullptr;
    }
    auto buffer = GetAllocator()->allocate(size);
    if (!buffer) {
        return nullptr;
    }
    buffer->bindSegmentLifetime(lifetime);
    if (!allocatable_.load(std::memory_order_acquire) ||
        !lifetime.isServing() || !(lifetime == SnapshotLifetime())) {
        return nullptr;
    }
    return buffer;
}

std::shared_ptr<BufferAllocatorBase>
SegmentAllocatorRegistration::GetAllocator() const {
    return allocator_.load(std::memory_order_acquire);
}

void SegmentAllocatorRegistration::BindAllocator(
    std::shared_ptr<BufferAllocatorBase> replacement) {
    allocator_.store(std::move(replacement), std::memory_order_release);
}

void SegmentAllocatorRegistration::BindClientSession(
    ClientSessionSharedPtr session) {
    auto lifetime = SnapshotLifetime();
    lifetime.BindSession(std::move(session));
    lifetime_.store(std::move(lifetime.state_), std::memory_order_release);
}

void SegmentAllocatorRegistration::BindBuffer(AllocatedBuffer& buffer) const {
    buffer.bindSegmentLifetime(SnapshotLifetime());
}

bool SegmentAllocatorRegistration::OwnsBuffer(
    const AllocatedBuffer& buffer) const {
    return buffer.segment_lifetime_ == SnapshotLifetime();
}

void SegmentAllocatorRegistration::SetAllocatable(bool allocatable) {
    allocatable_.store(allocatable, std::memory_order_release);
}

void SegmentAllocatorRegistration::Invalidate() {
    allocatable_.store(false, std::memory_order_release);
    SnapshotLifetime().Invalidate();
}

}  // namespace mooncake
