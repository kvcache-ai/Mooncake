#include "segment_allocator_registration.h"

#include <utility>

namespace mooncake {

SegmentAllocatorRegistration::SegmentAllocatorRegistration(
    std::shared_ptr<BufferAllocatorBase> allocator, SegmentLifetime lifetime,
    bool allocatable)
    : allocator_(std::move(allocator)),
      allocatable_(allocatable),
      lifetime_(std::move(lifetime)),
      standalone_origin_(BufferAllocatorBase::GetBufferOrigin(allocator_)) {
    auto origin = std::make_shared<BufferOrigin>(*standalone_origin_);
    origin->lifetime = lifetime_;
    bound_origin_ = std::move(origin);
}

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
    BindBuffer(*buffer);
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
SegmentAllocatorRegistration::WithClientSession(
    ClientSessionSharedPtr session,
    std::shared_ptr<BufferAllocatorBase> allocator, bool allocatable) const {
    SegmentLifetime next_lifetime(std::move(session));
    if (!lifetime_.isValid()) {
        next_lifetime.Invalidate();
    }
    return std::shared_ptr<SegmentAllocatorRegistration>(
        new SegmentAllocatorRegistration(
            std::move(allocator), std::move(next_lifetime), allocatable));
}

void SegmentAllocatorRegistration::BindBuffer(AllocatedBuffer& buffer) const {
    if (buffer.origin_ == standalone_origin_) {
        buffer.origin_ = bound_origin_;
    } else if (buffer.origin_ != bound_origin_) {
        const auto& source = *buffer.origin_;
        if (!source.release_size && buffer.getAllocator() == allocator_) {
            buffer.origin_ = BufferOrigin::ForTransport(
                bound_origin_, source.protocol, source.cxl_endpoint,
                source.transfer_address_base);
        } else {
            // A foreign buffer can have its own allocator or release token.
            buffer.bindSegmentLifetime(lifetime_);
        }
    }
}

void SegmentAllocatorRegistration::SetAllocatable(bool allocatable) noexcept {
    allocatable_.store(allocatable, std::memory_order_release);
}

void SegmentAllocatorRegistration::Invalidate() noexcept {
    allocatable_.store(false, std::memory_order_release);
    lifetime_.Invalidate();
}

}  // namespace mooncake
