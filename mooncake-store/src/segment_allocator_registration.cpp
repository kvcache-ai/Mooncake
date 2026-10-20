#include "segment_allocator_registration.h"

#include <utility>

namespace mooncake {

SegmentAllocatorRegistration::SegmentAllocatorRegistration(
    std::shared_ptr<BufferAllocatorBase> allocator,
    std::shared_ptr<ClientLivenessRecord> client_liveness)
    : allocator_(std::move(allocator)) {
    lifetime_.BindClientLiveness(std::move(client_liveness));
}

bool SegmentAllocatorRegistration::IsServing() const {
    return lifetime_.CanAllocate();
}

std::unique_ptr<AllocatedBuffer> SegmentAllocatorRegistration::Allocate(
    size_t size) const {
    return AllocateBoundTo(*GetAllocator(), lifetime_, size);
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

void SegmentAllocatorRegistration::BindClientLiveness(
    std::shared_ptr<ClientLivenessRecord> record) {
    lifetime_.BindClientLiveness(std::move(record));
}

void SegmentAllocatorRegistration::BindBuffer(AllocatedBuffer& buffer) const {
    buffer.bindSegmentLifetime(lifetime_);
}

bool SegmentAllocatorRegistration::OwnsBuffer(
    const AllocatedBuffer& buffer) const {
    return buffer.isBoundTo(lifetime_);
}

void SegmentAllocatorRegistration::SetAllocatable(bool allocatable) {
    lifetime_.SetAllocatable(allocatable);
}

void SegmentAllocatorRegistration::Invalidate() { lifetime_.Invalidate(); }

}  // namespace mooncake
