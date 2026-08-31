#include "segment_allocator_registration.h"

#include <utility>

namespace mooncake {

SegmentAllocatorRegistration::SegmentAllocatorRegistration(
    std::shared_ptr<BufferAllocatorBase> allocator,
    std::shared_ptr<ClientLivenessRecord> client_liveness)
    : allocator_(std::move(allocator)),
      client_liveness_(std::move(client_liveness)) {}

bool SegmentAllocatorRegistration::IsServing() const {
    if (!allocation_lifetime_.isAvailable()) {
        return false;
    }
    const auto record =
        std::atomic_load_explicit(&client_liveness_, std::memory_order_acquire);
    return !record || record->IsServing();
}

std::unique_ptr<AllocatedBuffer> SegmentAllocatorRegistration::Allocate(
    size_t size) const {
    if (!allocation_lifetime_.isAvailable()) {
        return nullptr;
    }
    const auto record =
        std::atomic_load_explicit(&client_liveness_, std::memory_order_acquire);
    if (record && !record->IsServing()) {
        return nullptr;
    }
    auto buffer = GetAllocator()->allocate(size);
    if (!buffer) {
        return nullptr;
    }
    buffer->bindSegmentLifetime(buffer_lifetime_);
    buffer->bindClientLiveness(record);
    if (!allocation_lifetime_.isAvailable() ||
        record != std::atomic_load_explicit(&client_liveness_,
                                             std::memory_order_acquire) ||
        (record && !record->IsServing())) {
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

void SegmentAllocatorRegistration::BindClientLiveness(
    std::shared_ptr<ClientLivenessRecord> record) {
    std::atomic_store_explicit(&client_liveness_, std::move(record),
                               std::memory_order_release);
}

void SegmentAllocatorRegistration::BindBuffer(AllocatedBuffer& buffer) const {
    buffer.bindSegmentLifetime(buffer_lifetime_);
    buffer.bindClientLiveness(std::atomic_load_explicit(
        &client_liveness_, std::memory_order_acquire));
}

bool SegmentAllocatorRegistration::OwnsBuffer(
    const AllocatedBuffer& buffer) const {
    return buffer.segment_lifetime_ == buffer_lifetime_;
}

void SegmentAllocatorRegistration::SetAllocatable(bool allocatable) {
    allocation_lifetime_.setAvailable(allocatable);
}

void SegmentAllocatorRegistration::Invalidate() {
    allocation_lifetime_.setAvailable(false);
    buffer_lifetime_.setAvailable(false);
}

}  // namespace mooncake
