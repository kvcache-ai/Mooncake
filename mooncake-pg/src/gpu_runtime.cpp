#include "gpu_runtime.h"

#include <algorithm>
#include <cstring>
#include <exception>
#include <limits>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "error_types.h"

namespace mooncake {
namespace {

void warnCleanupFailure(const char* operation, const char* error) noexcept {
    LOG(WARNING) << "Mooncake PG CUDA cleanup failed while " << operation
                 << ": " << (error ? error : "unknown error");
}

template <typename Release>
void cleanupGpuResource(const char* operation, Release release) noexcept {
    try {
        auto result = release();
        if (!result.has_value()) {
            warnCleanupFailure(operation, result.error().message.c_str());
        }
    } catch (const std::exception& error) {
        warnCleanupFailure(operation, error.what());
    } catch (...) {
        warnCleanupFailure(operation, "unknown error");
    }
}

}  // namespace

namespace detail {

PGResult<size_t> gpuBufferBytes(size_t count, size_t element_size) {
    PG_VALIDATE_ARG(count <= std::numeric_limits<size_t>::max() / element_size,
                    "GPU buffer size overflows");
    return count * element_size;
}

PGResult<void*> allocateGpuBuffer(int device, size_t bytes) {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device));
    void* pointer = nullptr;
    if (bytes != 0) PG_TRY_CUDA(cudaMalloc(&pointer, bytes));
    return pointer;
}

PGResult<void> releaseGpuBuffer(int device, void* pointer) {
    if (!pointer) return {};
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device));
    PG_TRY_CUDA(cudaFree(pointer));
    return {};
}

void resetGpuBuffer(int device, void* pointer) noexcept {
    if (!pointer) return;
    cleanupGpuResource("free device buffer",
                       [&] { return releaseGpuBuffer(device, pointer); });
}

PGResult<void*> allocatePinnedHostBuffer(size_t bytes, unsigned int flags) {
    void* pointer = nullptr;
    if (bytes != 0) PG_TRY_CUDA(cudaHostAlloc(&pointer, bytes, flags));
    return pointer;
}

PGResult<void> releasePinnedHostBuffer(void* pointer) {
    if (pointer) PG_TRY_CUDA(cudaFreeHost(pointer));
    return {};
}

void resetPinnedHostBuffer(void* pointer) noexcept {
    if (!pointer) return;
    cleanupGpuResource("free pinned host buffer",
                       [&] { return releasePinnedHostBuffer(pointer); });
}

PGResult<void*> mappedHostPointer(void* pointer) {
    void* device_pointer = nullptr;
    if (pointer)
        PG_TRY_CUDA(cudaHostGetDevicePointer(&device_pointer, pointer, 0));
    return device_pointer;
}

}  // namespace detail

#if MOONCAKE_PG_HAS_COLLECTIVE_V2
PGResult<GpuUuid> queryGpuUuid(int device_index) {
    cudaDeviceProp properties{};
    PG_TRY_CUDA(cudaGetDeviceProperties(&properties, device_index));
    GpuUuid uuid{};
    static_assert(sizeof(properties.uuid.bytes) == sizeof(uuid));
    std::memcpy(uuid.data(), properties.uuid.bytes, uuid.size());
    return uuid;
}

PGResult<bool> gpuPeerSupportsNativeAtomics(const GpuUuid& source,
                                            const GpuUuid& destination) {
    // Accesses within one GPU use local atomic semantics; CUDA's peer
    // query requires distinct GPUs.
    if (source == destination) return true;

    int source_device = -1;
    int destination_device = -1;
    int device_count = 0;
    PG_TRY_CUDA(cudaGetDeviceCount(&device_count));
    for (int candidate = 0; candidate < device_count; ++candidate) {
        PG_TRY(auto uuid, queryGpuUuid(candidate));
        if (uuid == source) source_device = candidate;
        if (uuid == destination) destination_device = candidate;
        if (source_device >= 0 && destination_device >= 0) break;
    }

    if (source_device < 0 || destination_device < 0) {
        // FIXME: Query native atomics for peers hidden by CUDA_VISIBLE_DEVICES
        // and for MNNVL peers, e.g. via NVML. Until then, report no support.
        return false;
    }

    int accessible = 0;
    PG_TRY_CUDA(cudaDeviceCanAccessPeer(&accessible, source_device,
                                        destination_device));
    if (!accessible) return false;
    int native_atomics = 0;
    PG_TRY_CUDA(cudaDeviceGetP2PAttribute(&native_atomics,
                                          cudaDevP2PAttrNativeAtomicSupported,
                                          source_device, destination_device));
    return native_atomics != 0;
}

PGResult<uint64_t> gpuTimeoutTicks(int device_index, size_t timeout_us) {
    if (timeout_us == 0) return uint64_t{0};
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    int clock_rate_khz_value = 0;
    PG_TRY_CUDA(cudaDeviceGetAttribute(&clock_rate_khz_value,
                                       cudaDevAttrClockRate, device_index));
    const uint64_t clock_rate_khz = static_cast<uint64_t>(clock_rate_khz_value);
    if (clock_rate_khz == 0) return uint64_t{0};
    if (timeout_us > std::numeric_limits<uint64_t>::max() / clock_rate_khz) {
        return uint64_t{std::numeric_limits<uint64_t>::max()};
    }
    return uint64_t{std::max<uint64_t>(1, timeout_us * clock_rate_khz / 1000)};
}
#endif

PGResult<GpuDeviceGuard> GpuDeviceGuard::create(int device) {
    PG_VALIDATE_ARG(device >= 0, "invalid CUDA device index");

    int previous_device = -1;
    PG_TRY_CUDA(cudaGetDevice(&previous_device));
    if (previous_device == device) {
        return GpuDeviceGuard(previous_device, false);
    }

    PG_TRY_CUDA(cudaSetDevice(device));
    return GpuDeviceGuard(previous_device, true);
}

GpuDeviceGuard::~GpuDeviceGuard() noexcept { reset(); }

GpuDeviceGuard::GpuDeviceGuard(GpuDeviceGuard&& other) noexcept {
    moveFrom(std::move(other));
}

GpuDeviceGuard& GpuDeviceGuard::operator=(GpuDeviceGuard&& other) noexcept {
    if (this != &other) {
        reset();
        moveFrom(std::move(other));
    }
    return *this;
}

void GpuDeviceGuard::reset() noexcept {
    if (!restore_device_) {
        previous_device_ = -1;
        return;
    }
    const auto error = cudaSetDevice(previous_device_);
    if (error != cudaSuccess) {
        warnCleanupFailure("restore previous CUDA device",
                           cudaGetErrorString(error));
    }
    previous_device_ = -1;
    restore_device_ = false;
}

void GpuDeviceGuard::moveFrom(GpuDeviceGuard&& other) noexcept {
    previous_device_ = std::exchange(other.previous_device_, -1);
    restore_device_ = std::exchange(other.restore_device_, false);
}

GpuStream::~GpuStream() noexcept { reset(); }

GpuStream::GpuStream(GpuStream&& other) noexcept { moveFrom(std::move(other)); }

GpuStream& GpuStream::operator=(GpuStream&& other) noexcept {
    if (this != &other) {
        reset();
        moveFrom(std::move(other));
    }
    return *this;
}

PGResult<GpuStream> GpuStream::createNonBlocking(int device) {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device));
    cudaStream_t stream = nullptr;
    PG_TRY_CUDA(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking));
    return GpuStream(stream, device, true);
}

GpuStream GpuStream::borrow(cudaStream_t stream, int device) {
    PG_ASSERT(device >= 0, "invalid CUDA device index");
    return GpuStream(stream, device, false);
}

PGResult<cudaStreamCaptureStatus> GpuStream::captureStatus() const {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    cudaStreamCaptureStatus status = cudaStreamCaptureStatusNone;
    PG_TRY_CUDA(cudaStreamIsCapturing(stream_, &status));
    return status;
}

PGResult<void> GpuStream::waitEvent(const GpuEvent& event) const {
    PG_VALIDATE_ARG(device_index_ == event.deviceIndex(),
                    "CUDA event device does not match waiting stream device");
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaStreamWaitEvent(stream_, event.event_, 0));
    return {};
}

PGResult<void> GpuStream::synchronize() const {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaStreamSynchronize(stream_));
    return {};
}

GpuStream::GpuStream(cudaStream_t stream, int device, bool owns_stream) noexcept
    : stream_(stream), device_index_(device), owns_stream_(owns_stream) {}

void GpuStream::reset() noexcept {
    if (owns_stream_ && stream_) {
        cleanupGpuResource("destroy stream", [&]() -> PGResult<void> {
            PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
            PG_TRY_CUDA(cudaStreamDestroy(stream_));
            return {};
        });
    }
    stream_ = nullptr;
    device_index_ = -1;
    owns_stream_ = false;
}

void GpuStream::moveFrom(GpuStream&& other) noexcept {
    stream_ = other.stream_;
    device_index_ = other.device_index_;
    owns_stream_ = other.owns_stream_;
    other.stream_ = nullptr;
    other.device_index_ = -1;
    other.owns_stream_ = false;
}

PGResult<GpuEvent> GpuEvent::create(int device, unsigned int flags) {
    PG_VALIDATE_ARG(device >= 0, "invalid CUDA device index");
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device));

    cudaEvent_t event = nullptr;
    PG_TRY_CUDA(cudaEventCreateWithFlags(&event, flags));
    return GpuEvent(event, device);
}

GpuEvent::~GpuEvent() noexcept { reset(); }

GpuEvent::GpuEvent(GpuEvent&& other) noexcept { moveFrom(std::move(other)); }

GpuEvent& GpuEvent::operator=(GpuEvent&& other) noexcept {
    if (this != &other) {
        reset();
        moveFrom(std::move(other));
    }
    return *this;
}

PGResult<void> GpuEvent::record(const GpuStream& stream) {
    PG_VALIDATE_ARG(device_index_ == stream.deviceIndex(),
                    "CUDA event device does not match recording stream device");

    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaEventRecord(event_, stream.get()));
    return {};
}

PGResult<bool> GpuEvent::query() const {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    const auto result = cudaEventQuery(event_);
    if (result == cudaSuccess) return true;
    if (result == cudaErrorNotReady) return false;
    return makePGError(
        PGErrorCode::SystemError,
        std::string("cudaEventQuery failed: ") + cudaGetErrorString(result));
}

void GpuEvent::reset() noexcept {
    if (event_) {
        cleanupGpuResource("destroy CUDA event", [&]() -> PGResult<void> {
            PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
            PG_TRY_CUDA(cudaEventDestroy(event_));
            return {};
        });
    }
    event_ = nullptr;
    device_index_ = -1;
}

void GpuEvent::moveFrom(GpuEvent&& other) noexcept {
    event_ = other.event_;
    device_index_ = other.device_index_;
    other.event_ = nullptr;
    other.device_index_ = -1;
}

// These CUDA Graph helpers are currently used only by the new collective
// runtime.
#if MOONCAKE_PG_HAS_COLLECTIVE_V2
PGResult<GpuCaptureInfo> GpuStream::captureInfo() const {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));

    cudaStreamCaptureStatus status = cudaStreamCaptureStatusNone;
    unsigned long long graph_id = 0;
    cudaGraph_t graph = nullptr;
#if CUDART_VERSION >= 13000
    PG_TRY_CUDA(cudaStreamGetCaptureInfo(stream_, &status, &graph_id, &graph,
                                         nullptr, nullptr, nullptr));
#else
    PG_TRY_CUDA(cudaStreamGetCaptureInfo_v2(stream_, &status, &graph_id, &graph,
                                            nullptr, nullptr));
#endif

    if (status == cudaStreamCaptureStatusNone) {
        return GpuCaptureInfo{.origin = stream_};
    }
    PG_VALIDATE_STATE(status == cudaStreamCaptureStatusActive,
                      "CUDA Graph capture is invalidated");
    return GpuCaptureInfo{
        .active = true,
        .origin = stream_,
        .graph = graph,
        .graph_id = static_cast<uint64_t>(graph_id),
    };
}

PGResult<void> GpuStream::waitExternalEvent(const GpuEvent& event) const {
    PG_VALIDATE_ARG(device_index_ == event.deviceIndex(),
                    "CUDA event device does not match waiting stream device");
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(
        cudaStreamWaitEvent(stream_, event.event_, cudaEventWaitExternal));
    return {};
}

PGResult<void> GpuEvent::recordExternal(const GpuStream& stream) {
    PG_VALIDATE_ARG(device_index_ == stream.deviceIndex(),
                    "CUDA event device does not match recording stream device");

    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaEventRecordWithFlags(event_, stream.get(),
                                         cudaEventRecordExternal));
    return {};
}

PGResult<void> joinCaptureWithoutDependencies(const GpuCaptureInfo& capture,
                                              const GpuStream& stream) {
    PG_VALIDATE_STATE(capture.active && capture.graph,
                      "cannot join an inactive CUDA Graph capture");

    // An event captured on the origin stream brings this additional stream
    // into the same multi-stream capture. The wait temporarily gives `stream`
    // the origin's current captured dependencies.
    auto origin = GpuStream::borrow(capture.origin, stream.deviceIndex());
    PG_TRY(auto scratch, GpuEvent::create(stream.deviceIndex()));
    PG_TRY(scratch.record(origin));
    PG_TRY(stream.waitEvent(scratch));

    // The scratch event only bootstraps capture membership. Clear the pending
    // next-operation dependencies while leaving `stream` in the capture; the
    // caller will install its real first dependency next.
    PG_TRY(auto device_guard, GpuDeviceGuard::create(stream.deviceIndex()));
#if CUDART_VERSION >= 13000
    PG_TRY_CUDA(cudaStreamUpdateCaptureDependencies(
        stream.get(), nullptr, nullptr, 0, cudaStreamSetCaptureDependencies));
#else
    PG_TRY_CUDA(cudaStreamUpdateCaptureDependencies(
        stream.get(), nullptr, 0, cudaStreamSetCaptureDependencies));
#endif
    return {};
}

PGResult<GpuGraphUserObject> GpuGraphUserObject::create(
    int device, void* payload, cudaHostFn_t destructor) {
    PG_VALIDATE_ARG(device >= 0, "invalid CUDA device index");
    PG_VALIDATE_ARG(payload, "CUDA Graph user object payload is null");
    PG_VALIDATE_ARG(destructor, "CUDA Graph user object destructor is null");

    PG_TRY(auto device_guard, GpuDeviceGuard::create(device));
    cudaUserObject_t object = nullptr;
    PG_TRY_CUDA(cudaUserObjectCreate(&object, payload, destructor, 1,
                                     cudaUserObjectNoDestructorSync));
    return GpuGraphUserObject(object, device);
}

GpuGraphUserObject::~GpuGraphUserObject() noexcept { reset(); }

GpuGraphUserObject::GpuGraphUserObject(GpuGraphUserObject&& other) noexcept {
    moveFrom(std::move(other));
}

GpuGraphUserObject& GpuGraphUserObject::operator=(
    GpuGraphUserObject&& other) noexcept {
    if (this != &other) {
        reset();
        moveFrom(std::move(other));
    }
    return *this;
}

PGResult<void> GpuGraphUserObject::transferToGraph(
    const GpuCaptureInfo& capture) {
    PG_VALIDATE_STATE(object_, "CUDA Graph user object was already moved");
    PG_VALIDATE_STATE(capture.active && capture.graph,
                      "cannot attach a user object to an inactive CUDA Graph");

    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaGraphRetainUserObject(capture.graph, object_, 1,
                                          cudaGraphUserObjectMove));
    object_ = nullptr;
    device_index_ = -1;
    return {};
}

void GpuGraphUserObject::reset() noexcept {
    if (!object_) return;
    cleanupGpuResource(
        "release CUDA Graph user object", [&]() -> PGResult<void> {
            PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
            PG_TRY_CUDA(cudaUserObjectRelease(object_, 1));
            return {};
        });
    object_ = nullptr;
    device_index_ = -1;
}

void GpuGraphUserObject::moveFrom(GpuGraphUserObject&& other) noexcept {
    object_ = std::exchange(other.object_, nullptr);
    device_index_ = std::exchange(other.device_index_, -1);
}
#endif

}  // namespace mooncake
