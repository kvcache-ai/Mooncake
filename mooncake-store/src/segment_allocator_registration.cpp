#include "segment_allocator_registration.h"

#include <utility>
#include <cassert>

namespace mooncake {

void ServingNameCounter::Update(bool serving) {
    // Serialize a name's 0 <-> 1 transitions so a concurrent removal cannot
    // decrement the aggregate before the matching addition has incremented it.
    std::lock_guard lock(mutex_);
    if (serving) {
        if (serving_registrations_++ == 0) {
            total_->fetch_add(1, std::memory_order_relaxed);
        }
    } else {
        assert(serving_registrations_ > 0);
        if (--serving_registrations_ == 0) {
            total_->fetch_sub(1, std::memory_order_relaxed);
        }
    }
}

SegmentAllocatorRegistration::SegmentAllocatorRegistration(
    std::shared_ptr<BufferAllocatorBase> allocator,
    std::shared_ptr<ClientLivenessRecord> client_liveness)
    : allocator_(std::move(allocator)),
      client_liveness_(std::move(client_liveness)) {}

SegmentAllocatorRegistration::~SegmentAllocatorRegistration() {
    UntrackServingName();
}

void SegmentAllocatorRegistration::TrackServingName(
    std::shared_ptr<ServingNameCounter> counter) {
    assert(!serving_name_counter_);
    serving_name_counter_ = std::move(counter);
    SubscribeServingCount();
}

void SegmentAllocatorRegistration::UntrackServingName() {
    UnsubscribeServingCount();
    serving_name_counter_.reset();
}

void SegmentAllocatorRegistration::SubscribeServingCount() {
    if (!serving_name_counter_ || !allocation_lifetime_.isAvailable()) {
        return;
    }
    const auto record =
        std::atomic_load_explicit(&client_liveness_, std::memory_order_acquire);
    if (record) {
        record->AddResourceObserver(
            this, ClientLivenessRecord::ResourceRequirement::SERVING,
            [counter = serving_name_counter_](bool serving) {
                counter->Update(serving);
            });
    } else {
        serving_name_counter_->Update(true);
    }
}

void SegmentAllocatorRegistration::UnsubscribeServingCount() {
    if (!serving_name_counter_ || !allocation_lifetime_.isAvailable()) {
        return;
    }
    const auto record =
        std::atomic_load_explicit(&client_liveness_, std::memory_order_acquire);
    if (record) {
        record->RemoveResourceObserver(this);
    } else {
        serving_name_counter_->Update(false);
    }
}

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
    UnsubscribeServingCount();
    std::atomic_store_explicit(&client_liveness_, std::move(record),
                               std::memory_order_release);
    SubscribeServingCount();
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
    if (allocatable == allocation_lifetime_.isAvailable()) {
        return;
    }
    UnsubscribeServingCount();
    allocation_lifetime_.setAvailable(allocatable);
    SubscribeServingCount();
}

void SegmentAllocatorRegistration::Invalidate() {
    SetAllocatable(false);
    buffer_lifetime_.setAvailable(false);
}

}  // namespace mooncake
