#include "gpu_runtime.h"

#include <exception>
#include <utility>

#include <glog/logging.h>

#include "error_types.h"

namespace mooncake {
namespace {

void warnCleanupFailure(const char* operation, const char* error) noexcept {
    LOG(WARNING) << "Mooncake PG CUDA cleanup failed while " << operation
                 << ": " << (error ? error : "unknown error");
}

}  // namespace

GpuDeviceGuard::GpuDeviceGuard(int device) {
    PG_ASSERT(device >= 0, "invalid CUDA device index");

    PG_ASSERT_CUDA(cudaGetDevice(&previous_device_));
    if (previous_device_ == device) return;

    PG_ASSERT_CUDA(cudaSetDevice(device));
    restore_device_ = true;
}

GpuDeviceGuard::~GpuDeviceGuard() noexcept {
    if (!restore_device_) return;
    const auto error = cudaSetDevice(previous_device_);
    if (error != cudaSuccess) {
        warnCleanupFailure("restore collective CUDA device",
                           cudaGetErrorString(error));
    }
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

GpuStream GpuStream::createNonBlocking(int device) {
    const GpuDeviceGuard device_guard(device);
    cudaStream_t stream = nullptr;
    PG_ASSERT_CUDA(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking));
    return GpuStream(stream, device, true);
}

GpuStream GpuStream::borrow(cudaStream_t stream, int device) {
    PG_ASSERT(device >= 0, "invalid CUDA device index");
    return GpuStream(stream, device, false);
}

bool GpuStream::isCapturing() const {
    cudaStreamCaptureStatus capture_status = cudaStreamCaptureStatusNone;
    PG_ASSERT_CUDA(cudaStreamIsCapturing(stream_, &capture_status));
    return capture_status != cudaStreamCaptureStatusNone;
}

void GpuStream::waitEvent(const GpuEvent& event) const {
    const GpuDeviceGuard device_guard(device_index_);
    PG_ASSERT_CUDA(cudaStreamWaitEvent(stream_, event.event_, 0));
}

GpuStream::GpuStream(cudaStream_t stream, int device, bool owns_stream) noexcept
    : stream_(stream), device_index_(device), owns_stream_(owns_stream) {}

void GpuStream::reset() noexcept {
    if (owns_stream_ && stream_) {
        try {
            const GpuDeviceGuard device_guard(device_index_);
            const auto error = cudaStreamDestroy(stream_);
            if (error != cudaSuccess) {
                warnCleanupFailure("destroy stream", cudaGetErrorString(error));
            }
        } catch (const std::exception& error) {
            warnCleanupFailure("destroy stream", error.what());
        } catch (...) {
            warnCleanupFailure("destroy stream", "unknown error");
        }
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

GpuEvent::GpuEvent(int device, unsigned int flags) : device_index_(device) {
    PG_ASSERT(device >= 0, "invalid CUDA device index");

    const GpuDeviceGuard device_guard(device);
    PG_ASSERT_CUDA(cudaEventCreateWithFlags(&event_, flags));
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

void GpuEvent::record(const GpuStream& stream) {
    PG_ASSERT(device_index_ == stream.deviceIndex(),
              "CUDA event device does not match recording stream device");

    const GpuDeviceGuard device_guard(device_index_);
    PG_ASSERT_CUDA(cudaEventRecord(event_, stream.get()));
}

void GpuEvent::reset() noexcept {
    if (event_) {
        try {
            const GpuDeviceGuard device_guard(device_index_);
            const auto error = cudaEventDestroy(event_);
            if (error != cudaSuccess) {
                warnCleanupFailure("destroy CUDA event",
                                   cudaGetErrorString(error));
            }
        } catch (const std::exception& error) {
            warnCleanupFailure("destroy CUDA event", error.what());
        } catch (...) {
            warnCleanupFailure("destroy CUDA event", "unknown error");
        }
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

}  // namespace mooncake
