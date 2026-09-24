#include "segment_allocator_registration.h"

#include <utility>

namespace mooncake {

SegmentAllocatorRegistration::SegmentAllocatorRegistration(
    std::shared_ptr<BufferAllocatorBase> allocator,
    ClientSessionSharedPtr owner_session)
    : allocator_(std::move(allocator)), lifetime_(std::move(owner_session)) {}

bool SegmentAllocatorRegistration::IsAllocatable() const {
    return allocatable_.load(std::memory_order_acquire) &&
           lifetime_.isServing();
}

std::unique_ptr<AllocatedBuffer> SegmentAllocatorRegistration::Allocate(
    size_t size) const {
    // Capture one lifetime generation across allocation, even if the segment
    // is rebound concurrently through another allocator-manager snapshot.
    const auto lifetime = lifetime_;
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
        !lifetime.isServing() || !(lifetime == lifetime_)) {
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
    lifetime_.BindSession(std::move(session));
}

void SegmentAllocatorRegistration::BindBuffer(AllocatedBuffer& buffer) const {
    buffer.bindSegmentLifetime(lifetime_);
}

bool SegmentAllocatorRegistration::OwnsBuffer(
    const AllocatedBuffer& buffer) const {
    return buffer.segment_lifetime_ == lifetime_;
}

void SegmentAllocatorRegistration::SetAllocatable(bool allocatable) {
    allocatable_.store(allocatable, std::memory_order_release);
}

void SegmentAllocatorRegistration::Invalidate() {
    allocatable_.store(false, std::memory_order_release);
    lifetime_.Invalidate();
}

}  // namespace mooncake
