#include "segment_allocator_registration.h"

#include <optional>
#include <utility>

namespace mooncake {

SegmentAllocatorRegistration::SegmentAllocatorRegistration(
    std::shared_ptr<BufferAllocatorBase> allocator,
    std::shared_ptr<ClientLivenessRecord> client_liveness)
    : allocator_(std::move(allocator)),
      client_liveness_(std::move(client_liveness)) {}

bool SegmentAllocatorRegistration::IsServing() const {
    const auto record = std::atomic_load_explicit(
        &client_liveness_, std::memory_order_acquire);
    return lifetime_.isAvailable() && (!record || record->IsServing());
}

std::unique_ptr<AllocatedBuffer> SegmentAllocatorRegistration::Allocate(
    size_t size) const {
    if (!lifetime_.isAvailable()) {
        return nullptr;
    }
    const auto record = std::atomic_load_explicit(
        &client_liveness_, std::memory_order_acquire);
    std::optional<ClientLivenessRecord::ServingGuard> serving_guard;
    if (record) {
        serving_guard = record->TryAcquireServingGuard();
        if (!serving_guard) {
            return nullptr;
        }
    }
    auto buffer = GetAllocator()->allocate(size);
    if (!buffer) {
        return nullptr;
    }
    buffer->bindSegmentLifetime(lifetime_);
    buffer->bindClientLiveness(record);
    if (!lifetime_.isAvailable()) {
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
    buffer.bindSegmentLifetime(lifetime_);
    buffer.bindClientLiveness(std::atomic_load_explicit(
        &client_liveness_, std::memory_order_acquire));
}

}  // namespace mooncake
