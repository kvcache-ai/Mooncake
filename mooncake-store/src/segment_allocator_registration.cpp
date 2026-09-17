#include "segment_allocator_registration.h"

#include <utility>

namespace mooncake {

SegmentAllocatorRegistration::SegmentAllocatorRegistration(
    std::shared_ptr<BufferAllocatorBase> allocator,
    ClientSessionSharedPtr owner_session)
    : allocator_(std::move(allocator)), lifetime_(std::move(owner_session)) {}

bool SegmentAllocatorRegistration::IsServing() const {
    return lifetime_.isAllocatable();
}

std::unique_ptr<AllocatedBuffer> SegmentAllocatorRegistration::Allocate(
    size_t size) const {
    // Capture one lifetime generation across allocation, even if the segment
    // is rebound concurrently through another allocator-manager snapshot.
    const auto lifetime = lifetime_;
    if (!lifetime.isAllocatable()) {
        return nullptr;
    }
    auto buffer = GetAllocator()->allocate(size);
    if (!buffer) {
        return nullptr;
    }
    buffer->bindSegmentLifetime(lifetime);
    if (!lifetime.isAllocatable() || !(lifetime == lifetime_)) {
        return nullptr;
    }
    return buffer;
}

std::shared_ptr<BufferAllocatorBase>
SegmentAllocatorRegistration::GetAllocator() const {
    return std::atomic_load_explicit(&allocator_, std::memory_order_acquire);
}

void SegmentAllocatorRegistration::BindAllocator(
    std::shared_ptr<BufferAllocatorBase> replacement) {
    std::atomic_store_explicit(&allocator_, std::move(replacement),
                               std::memory_order_release);
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
    lifetime_.SetAllocatable(allocatable);
}

void SegmentAllocatorRegistration::Invalidate() { lifetime_.Invalidate(); }

}  // namespace mooncake
