#include "segment_allocator_registration.h"

#include <cassert>
#include <utility>

namespace mooncake {

SegmentWriteHold::SegmentWriteHold(
    std::shared_ptr<SegmentAllocatorRegistration> registration,
    std::shared_ptr<BufferAllocatorBase> allocator) noexcept
    : registration_(std::move(registration)),
      allocator_(std::move(allocator)) {}

SegmentWriteHold::~SegmentWriteHold() { Release(); }

SegmentWriteHold::SegmentWriteHold(SegmentWriteHold&& other) noexcept
    : registration_(std::move(other.registration_)),
      allocator_(std::move(other.allocator_)) {}

SegmentWriteHold& SegmentWriteHold::operator=(
    SegmentWriteHold&& other) noexcept {
    if (this != &other) {
        Release();
        registration_ = std::move(other.registration_);
        allocator_ = std::move(other.allocator_);
    }
    return *this;
}

void SegmentWriteHold::Release() noexcept {
    if (!registration_) return;
    const auto previous =
        registration_->write_holds_.fetch_sub(1, std::memory_order_acq_rel);
    assert(previous != 0);
    (void)previous;
    allocator_.reset();
    registration_.reset();
}

bool SegmentWriteHold::CanContinue(Clock::time_point now) const {
    // Graceful drain blocks new allocations but lets existing writes finish.
    return registration_ && !registration_->IsWriteClosed() &&
           registration_->lifetime_.isValid() &&
           now < registration_->GracefulDeadline() &&
           registration_->GetAllocator() == allocator_;
}

bool SegmentWriteHold::IsPhysicallyRetired() const {
    return registration_ && registration_->IsPhysicallyRetired();
}

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
    if (HasWriteHolds() || IsWriteClosed()) return nullptr;
    auto next = std::shared_ptr<SegmentAllocatorRegistration>(
        new SegmentAllocatorRegistration(
            std::move(replacement), lifetime_,
            allocatable_.load(std::memory_order_acquire)));
    next->tcp_instance_ = tcp_instance_;
    next->graceful_deadline_.store(
        graceful_deadline_.load(std::memory_order_acquire),
        std::memory_order_relaxed);
    return next;
}

std::shared_ptr<SegmentAllocatorRegistration>
SegmentAllocatorRegistration::ReplaceClientSession(
    ClientSessionSharedPtr session,
    std::shared_ptr<BufferAllocatorBase> allocator, bool allocatable) {
    // A new registration must not hide unsettled writes on the old backing.
    if (!tcp_instance_.empty() && (HasWriteHolds() || IsWriteClosed()))
        return nullptr;
    SegmentLifetime next_lifetime(std::move(session));
    if (!lifetime_.isValid()) next_lifetime.Invalidate();
    auto next = std::shared_ptr<SegmentAllocatorRegistration>(
        new SegmentAllocatorRegistration(
            std::move(allocator), std::move(next_lifetime), allocatable));
    next->tcp_instance_ = tcp_instance_;
    // Old buffers and detached snapshots retain the old incarnation.
    Invalidate();
    return next;
}

bool SegmentAllocatorRegistration::HasWriteHolds() const {
    return WriteHoldCount() != 0;
}

size_t SegmentAllocatorRegistration::WriteHoldCount() const {
    return write_holds_.load(std::memory_order_acquire);
}

bool SegmentAllocatorRegistration::IsWriteClosed() const {
    return write_state_.load(std::memory_order_acquire) != WriteState::OPEN;
}

bool SegmentAllocatorRegistration::IsPhysicallyRetired() const {
    return write_state_.load(std::memory_order_acquire) ==
           WriteState::PHYSICALLY_RETIRED;
}

SegmentAllocatorRegistration::Clock::time_point
SegmentAllocatorRegistration::GracefulDeadline() const {
    return Clock::time_point(
        Clock::duration(graceful_deadline_.load(std::memory_order_acquire)));
}

void SegmentAllocatorRegistration::BindBuffer(AllocatedBuffer& buffer) const {
    buffer.bindSegmentLifetime(lifetime_);
}

bool SegmentAllocatorRegistration::OwnsBuffer(
    const AllocatedBuffer& buffer) const {
    return buffer.segment_lifetime_.state_ == lifetime_.state_;
}

void SegmentAllocatorRegistration::SetAllocatable(bool allocatable) noexcept {
    if (allocatable && IsWriteClosed()) return;
    if (allocatable) {
        graceful_deadline_.store(
            Clock::time_point::max().time_since_epoch().count(),
            std::memory_order_release);
    }
    allocatable_.store(allocatable, std::memory_order_release);
}

void SegmentAllocatorRegistration::BeginGracefulUnmount(
    Clock::time_point deadline) {
    auto previous = graceful_deadline_.load(std::memory_order_relaxed);
    const auto requested = deadline.time_since_epoch().count();
    while (requested < previous &&
           !graceful_deadline_.compare_exchange_weak(
               previous, requested, std::memory_order_release,
               std::memory_order_relaxed)) {
    }
    SetAllocatable(false);
}

void SegmentAllocatorRegistration::CloseForWrites() {
    auto expected = WriteState::OPEN;
    write_state_.compare_exchange_strong(expected, WriteState::CLOSED,
                                         std::memory_order_acq_rel);
    allocatable_.store(false, std::memory_order_release);
}

void SegmentAllocatorRegistration::MarkPhysicallyRetired() {
    CloseForWrites();
    lifetime_.Invalidate();
    write_state_.store(WriteState::PHYSICALLY_RETIRED,
                       std::memory_order_release);
}

void SegmentAllocatorRegistration::Invalidate() noexcept {
    CloseForWrites();
    // Logical invalidity must follow the owning session, even while a hold
    // retains the actual allocation for physical safety.
    lifetime_.Invalidate();
}

}  // namespace mooncake
