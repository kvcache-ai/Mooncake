#ifndef MOONCAKE_PG_GPU_RUNTIME_H
#define MOONCAKE_PG_GPU_RUNTIME_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include <cuda_alike.h>

#include "device_comm/device_collective/device_collective_feature.h"
#include "error_types.h"

namespace mooncake {

using GpuUuid = std::array<uint8_t, 16>;

class GpuStream;
class GpuEvent;

// These declarations are currently used only by the new collective runtime and
// rely on APIs that may be unavailable on non-CUDA platforms. This temporary
// compatibility restriction will be fixed as the new collective runtime
// matures.
#if MOONCAKE_PG_HAS_COLLECTIVE_V2
[[nodiscard]] PGResult<GpuUuid> queryGpuUuid(int device_index);

// Whether native atomics from source to destination are known to be supported.
// Returns false when the capability cannot be determined.
[[nodiscard]] PGResult<bool> gpuPeerSupportsNativeAtomics(
    const GpuUuid& source, const GpuUuid& destination);

// Convert microseconds to clock64() ticks.
PGResult<uint64_t> gpuTimeoutTicks(int device_index, size_t timeout_us);

struct GpuCaptureInfo {
    bool active = false;
    cudaStream_t origin = nullptr;
    cudaGraph_t graph = nullptr;
    uint64_t graph_id = 0;
};

// Add `stream` to an active capture without making its first captured work
// depend on work already captured on the origin stream.
PGResult<void> joinCaptureWithoutDependencies(const GpuCaptureInfo& capture,
                                              const GpuStream& stream);

// A move-only reference that can be transferred to a CUDA Graph. CUDA owns the
// payload after create() succeeds and invokes `destructor` when the final graph
// or graph-exec reference is released.
class GpuGraphUserObject {
   public:
    static PGResult<GpuGraphUserObject> create(int device, void* payload,
                                               cudaHostFn_t destructor);

    ~GpuGraphUserObject() noexcept;

    GpuGraphUserObject(const GpuGraphUserObject&) = delete;
    GpuGraphUserObject& operator=(const GpuGraphUserObject&) = delete;

    GpuGraphUserObject(GpuGraphUserObject&& other) noexcept;
    GpuGraphUserObject& operator=(GpuGraphUserObject&& other) noexcept;

    PGResult<void> transferToGraph(const GpuCaptureInfo& capture);

   private:
    GpuGraphUserObject(cudaUserObject_t object, int device) noexcept
        : object_(object), device_index_(device) {}

    void reset() noexcept;
    void moveFrom(GpuGraphUserObject&& other) noexcept;

    cudaUserObject_t object_ = nullptr;
    int device_index_ = -1;
};
#endif

class GpuDeviceGuard {
   public:
    [[nodiscard]] static PGResult<GpuDeviceGuard> create(int device);

    ~GpuDeviceGuard() noexcept;

    GpuDeviceGuard(const GpuDeviceGuard&) = delete;
    GpuDeviceGuard& operator=(const GpuDeviceGuard&) = delete;

    GpuDeviceGuard(GpuDeviceGuard&& other) noexcept;
    GpuDeviceGuard& operator=(GpuDeviceGuard&& other) noexcept;

   private:
    GpuDeviceGuard(int previous_device, bool restore_device) noexcept
        : previous_device_(previous_device), restore_device_(restore_device) {}

    void reset() noexcept;
    void moveFrom(GpuDeviceGuard&& other) noexcept;

    int previous_device_ = -1;
    bool restore_device_ = false;
};

namespace detail {

PGResult<size_t> gpuBufferBytes(size_t count, size_t element_size);
PGResult<void*> allocateGpuBuffer(int device, size_t bytes);
PGResult<void> releaseGpuBuffer(int device, void* pointer);
void resetGpuBuffer(int device, void* pointer) noexcept;
PGResult<void*> allocatePinnedHostBuffer(size_t bytes, unsigned int flags);
PGResult<void> releasePinnedHostBuffer(void* pointer);
void resetPinnedHostBuffer(void* pointer) noexcept;
PGResult<void*> mappedHostPointer(void* pointer);

}  // namespace detail

// Owns device storage. The caller must finish GPU uses before releasing it.
// Allocation leaves bytes uninitialized; zeroed() and fromHost() initialize
// them explicitly. Device objects must be trivially copyable.
template <typename T>
class GpuBuffer {
    static_assert(std::is_trivially_copyable_v<T>);

   public:
    GpuBuffer() noexcept = default;
    ~GpuBuffer() noexcept { reset(); }

    GpuBuffer(const GpuBuffer&) = delete;
    GpuBuffer& operator=(const GpuBuffer&) = delete;

    GpuBuffer(GpuBuffer&& other) noexcept { moveFrom(std::move(other)); }
    GpuBuffer& operator=(GpuBuffer&& other) noexcept {
        if (this != &other) {
            reset();
            moveFrom(std::move(other));
        }
        return *this;
    }

    [[nodiscard]] static PGResult<GpuBuffer> allocate(int device,
                                                      size_t count = 1) {
        PG_TRY(auto bytes, detail::gpuBufferBytes(count, sizeof(T)));
        PG_TRY(auto pointer, detail::allocateGpuBuffer(device, bytes));
        return GpuBuffer(static_cast<T*>(pointer), count, device);
    }

    [[nodiscard]] static PGResult<GpuBuffer> zeroed(int device,
                                                    size_t count = 1) {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device));
        PG_TRY(auto buffer, allocate(device, count));
        if (count != 0) {
            PG_TRY_CUDA(cudaMemset(buffer.get(), 0, buffer.sizeBytes()));
        }
        return buffer;
    }

    [[nodiscard]] static PGResult<GpuBuffer> fromHost(int device,
                                                      const T& initial) {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device));
        PG_TRY(auto buffer, allocate(device));
        PG_TRY_CUDA(cudaMemcpy(buffer.get(), &initial, sizeof(T),
                               cudaMemcpyHostToDevice));
        return buffer;
    }

    [[nodiscard]] T* get() const noexcept { return pointer_; }
    [[nodiscard]] size_t size() const noexcept { return count_; }
    [[nodiscard]] size_t sizeBytes() const noexcept {
        return count_ * sizeof(T);
    }
    [[nodiscard]] int deviceIndex() const noexcept { return device_index_; }

    PGResult<void> release() {
        PG_TRY(detail::releaseGpuBuffer(device_index_, pointer_));
        pointer_ = nullptr;
        count_ = 0;
        device_index_ = -1;
        return {};
    }

    void reset() noexcept {
        detail::resetGpuBuffer(device_index_, std::exchange(pointer_, nullptr));
        count_ = 0;
        device_index_ = -1;
    }

   private:
    GpuBuffer(T* pointer, size_t count, int device) noexcept
        : pointer_(pointer), count_(count), device_index_(device) {}

    void moveFrom(GpuBuffer&& other) noexcept {
        pointer_ = std::exchange(other.pointer_, nullptr);
        count_ = std::exchange(other.count_, 0);
        device_index_ = std::exchange(other.device_index_, -1);
    }

    T* pointer_ = nullptr;
    size_t count_ = 0;
    int device_index_ = -1;
};

// Owns value-initialized host objects in page-locked memory. Finish transfers
// that use this storage before releasing it.
template <typename T>
class PinnedHostBuffer {
   public:
    PinnedHostBuffer() noexcept = default;
    ~PinnedHostBuffer() noexcept { reset(); }

    PinnedHostBuffer(const PinnedHostBuffer&) = delete;
    PinnedHostBuffer& operator=(const PinnedHostBuffer&) = delete;

    PinnedHostBuffer(PinnedHostBuffer&& other) noexcept {
        moveFrom(std::move(other));
    }
    PinnedHostBuffer& operator=(PinnedHostBuffer&& other) noexcept {
        if (this != &other) {
            reset();
            moveFrom(std::move(other));
        }
        return *this;
    }

    [[nodiscard]] static PGResult<PinnedHostBuffer> create(
        size_t count = 1, unsigned int flags = 0) {
        PG_TRY(auto bytes, detail::gpuBufferBytes(count, sizeof(T)));
        PG_TRY(auto pointer, detail::allocatePinnedHostBuffer(bytes, flags));
        PinnedHostBuffer buffer;
        buffer.pointer_ = static_cast<T*>(pointer);
        if (count != 0) {
            std::uninitialized_value_construct_n(buffer.pointer_, count);
        }
        buffer.count_ = count;
        return buffer;
    }

    [[nodiscard]] T* get() const noexcept { return pointer_; }
    [[nodiscard]] size_t size() const noexcept { return count_; }
    [[nodiscard]] size_t sizeBytes() const noexcept {
        return count_ * sizeof(T);
    }

    PGResult<void> release() {
        if (!pointer_) return {};
        // End object lifetimes only once if releasing the allocation fails.
        std::destroy_n(pointer_, std::exchange(count_, 0));
        PG_TRY(detail::releasePinnedHostBuffer(pointer_));
        pointer_ = nullptr;
        return {};
    }

    void reset() noexcept {
        if (!pointer_) return;
        std::destroy_n(pointer_, std::exchange(count_, 0));
        detail::resetPinnedHostBuffer(std::exchange(pointer_, nullptr));
    }

   private:
    void moveFrom(PinnedHostBuffer&& other) noexcept {
        pointer_ = std::exchange(other.pointer_, nullptr);
        count_ = std::exchange(other.count_, 0);
    }

    T* pointer_ = nullptr;
    size_t count_ = 0;
};

// Owns host objects mapped into the selected device. host() and device()
// address the same storage.
template <typename T>
class MappedHostBuffer {
   public:
    MappedHostBuffer() noexcept = default;
    ~MappedHostBuffer() noexcept = default;

    MappedHostBuffer(const MappedHostBuffer&) = delete;
    MappedHostBuffer& operator=(const MappedHostBuffer&) = delete;

    MappedHostBuffer(MappedHostBuffer&& other) noexcept {
        moveFrom(std::move(other));
    }
    MappedHostBuffer& operator=(MappedHostBuffer&& other) noexcept {
        if (this != &other) moveFrom(std::move(other));
        return *this;
    }

    [[nodiscard]] static PGResult<MappedHostBuffer> create(
        int device, size_t count = 1, unsigned int flags = 0) {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device));
        MappedHostBuffer buffer;
        PG_TRY(buffer.storage_,
               PinnedHostBuffer<T>::create(count, flags | cudaHostAllocMapped));
        PG_TRY(auto pointer, detail::mappedHostPointer(buffer.storage_.get()));
        buffer.device_pointer_ = static_cast<T*>(pointer);
        buffer.device_index_ = device;
        return buffer;
    }

    [[nodiscard]] T* host() const noexcept { return storage_.get(); }
    [[nodiscard]] T* device() const noexcept { return device_pointer_; }
    [[nodiscard]] size_t size() const noexcept { return storage_.size(); }
    [[nodiscard]] size_t sizeBytes() const noexcept {
        return storage_.sizeBytes();
    }
    [[nodiscard]] int deviceIndex() const noexcept { return device_index_; }

    PGResult<void> release() {
        device_pointer_ = nullptr;
        device_index_ = -1;
        return storage_.release();
    }

    void reset() noexcept {
        device_pointer_ = nullptr;
        device_index_ = -1;
        storage_.reset();
    }

   private:
    void moveFrom(MappedHostBuffer&& other) noexcept {
        storage_ = std::move(other.storage_);
        device_pointer_ = std::exchange(other.device_pointer_, nullptr);
        device_index_ = std::exchange(other.device_index_, -1);
    }

    PinnedHostBuffer<T> storage_;
    T* device_pointer_ = nullptr;
    int device_index_ = -1;
};

class GpuStream {
   public:
    GpuStream() = delete;
    ~GpuStream() noexcept;

    GpuStream(const GpuStream&) = delete;
    GpuStream& operator=(const GpuStream&) = delete;

    GpuStream(GpuStream&& other) noexcept;
    GpuStream& operator=(GpuStream&& other) noexcept;

    [[nodiscard]] static PGResult<GpuStream> createNonBlocking(int device);
    [[nodiscard]] static GpuStream borrow(cudaStream_t stream, int device);

    [[nodiscard]] cudaStream_t get() const noexcept { return stream_; }

    [[nodiscard]] int deviceIndex() const noexcept { return device_index_; }

    [[nodiscard]] PGResult<cudaStreamCaptureStatus> captureStatus() const;

    [[nodiscard]] PGResult<void> waitEvent(const GpuEvent& event) const;
    [[nodiscard]] PGResult<void> synchronize() const;

#if MOONCAKE_PG_HAS_COLLECTIVE_V2
    [[nodiscard]] PGResult<GpuCaptureInfo> captureInfo() const;
    [[nodiscard]] PGResult<void> waitExternalEvent(const GpuEvent& event) const;
#endif

   private:
    GpuStream(cudaStream_t stream, int device, bool owns_stream) noexcept;

    void reset() noexcept;
    void moveFrom(GpuStream&& other) noexcept;

    cudaStream_t stream_ = nullptr;
    int device_index_ = -1;
    bool owns_stream_ = false;
};

class GpuEvent {
   public:
    [[nodiscard]] static PGResult<GpuEvent> create(
        int device, unsigned int flags = cudaEventDisableTiming);

    ~GpuEvent() noexcept;

    GpuEvent(const GpuEvent&) = delete;
    GpuEvent& operator=(const GpuEvent&) = delete;

    GpuEvent(GpuEvent&& other) noexcept;
    GpuEvent& operator=(GpuEvent&& other) noexcept;

    [[nodiscard]] cudaEvent_t get() const noexcept { return event_; }

    [[nodiscard]] PGResult<void> record(const GpuStream& stream);
    [[nodiscard]] PGResult<bool> query() const;

#if MOONCAKE_PG_HAS_COLLECTIVE_V2
    [[nodiscard]] PGResult<void> recordExternal(const GpuStream& stream);
#endif

   private:
    friend class GpuStream;

    GpuEvent(cudaEvent_t event, int device) noexcept
        : event_(event), device_index_(device) {}

    [[nodiscard]] int deviceIndex() const noexcept { return device_index_; }

    void reset() noexcept;
    void moveFrom(GpuEvent&& other) noexcept;

    cudaEvent_t event_ = nullptr;
    int device_index_ = -1;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_GPU_RUNTIME_H
