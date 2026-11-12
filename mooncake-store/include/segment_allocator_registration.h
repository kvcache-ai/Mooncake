#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "allocator.h"

namespace mooncake {

class AllocatorManager;
class ScopedAllocatorAccess;
class ScopedNoFSegmentAccess;
class ScopedSegmentAccess;
class SegmentAllocatorRegistration;

// One hold preserves an exact allocator/registration binding for a primary
// write. The caller keeps its AllocatedBuffer until settlement; the target
// Client pins backing and separately proves I/O quiescence.
class SegmentWriteHold {
   public:
    using Clock = std::chrono::steady_clock;

    SegmentWriteHold() = default;
    ~SegmentWriteHold();
    SegmentWriteHold(const SegmentWriteHold&) = delete;
    SegmentWriteHold& operator=(const SegmentWriteHold&) = delete;
    SegmentWriteHold(SegmentWriteHold&& other) noexcept;
    SegmentWriteHold& operator=(SegmentWriteHold&& other) noexcept;

    explicit operator bool() const { return registration_ != nullptr; }
    [[nodiscard]] bool CanContinue(Clock::time_point now) const;
    [[nodiscard]] bool IsPhysicallyRetired() const;
    [[nodiscard]] const std::shared_ptr<SegmentAllocatorRegistration>&
    registration() const {
        return registration_;
    }
    [[nodiscard]] const std::shared_ptr<BufferAllocatorBase>& allocator()
        const {
        return allocator_;
    }

   private:
    SegmentWriteHold(std::shared_ptr<SegmentAllocatorRegistration> registration,
                     std::shared_ptr<BufferAllocatorBase> allocator) noexcept;
    void Release() noexcept;

    std::shared_ptr<SegmentAllocatorRegistration> registration_;
    std::shared_ptr<BufferAllocatorBase> allocator_;

    friend class ScopedAllocatorAccess;
};

// Immutable allocator and client-incarnation binding. Replacement publishes a
// new registration under segment access; existing buffers/holds keep the old
// binding. The write gate additionally protects exposed strict TCP grants.
class SegmentAllocatorRegistration {
   public:
    using Clock = std::chrono::steady_clock;

    [[nodiscard]] bool IsAllocatable() const noexcept;
    [[nodiscard]] std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const;
    [[nodiscard]] const std::shared_ptr<BufferAllocatorBase>& GetAllocator()
        const noexcept {
        return allocator_;
    }
    [[nodiscard]] const ClientSessionSharedPtr& GetClientSession()
        const noexcept;
    [[nodiscard]] bool HasWriteHolds() const;
    [[nodiscard]] size_t WriteHoldCount() const;
    [[nodiscard]] bool IsWriteClosed() const;
    [[nodiscard]] bool IsPhysicallyRetired() const;
    [[nodiscard]] Clock::time_point GracefulDeadline() const;

    // Read under segment access or the immutable binding of a WriteHold.
    [[nodiscard]] const std::string& TcpInstance() const {
        return tcp_instance_;
    }

   private:
    enum class WriteState : uint8_t { OPEN, CLOSED, PHYSICALLY_RETIRED };

    SegmentAllocatorRegistration(std::shared_ptr<BufferAllocatorBase> allocator,
                                 SegmentLifetime lifetime,
                                 bool allocatable = true);
    [[nodiscard]] std::shared_ptr<SegmentAllocatorRegistration> WithAllocator(
        std::shared_ptr<BufferAllocatorBase> replacement) const;
    [[nodiscard]] std::shared_ptr<SegmentAllocatorRegistration>
    ReplaceClientSession(ClientSessionSharedPtr session,
                         std::shared_ptr<BufferAllocatorBase> allocator,
                         bool allocatable);
    void BindBuffer(AllocatedBuffer& buffer) const;
    [[nodiscard]] bool OwnsBuffer(const AllocatedBuffer& buffer) const;
    void SetAllocatable(bool allocatable) noexcept;
    void BeginGracefulUnmount(Clock::time_point deadline);
    void CloseForWrites();
    void MarkPhysicallyRetired();
    void Invalidate() noexcept;

    const std::shared_ptr<BufferAllocatorBase> allocator_;
    // Draining stops allocation without invalidating existing buffers.
    std::atomic<bool> allocatable_{true};
    SegmentLifetime lifetime_;
    std::atomic<size_t> write_holds_{0};
    std::atomic<WriteState> write_state_{WriteState::OPEN};
    std::atomic<Clock::duration::rep> graceful_deadline_{
        Clock::time_point::max().time_since_epoch().count()};
    std::string tcp_instance_;

    friend class AllocatorManager;
    friend class ScopedAllocatorAccess;
    friend class ScopedNoFSegmentAccess;
    friend class ScopedSegmentAccess;
    friend class SegmentWriteHold;
};

}  // namespace mooncake
