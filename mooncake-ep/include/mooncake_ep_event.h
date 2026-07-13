#pragma once

#include <cuda_runtime.h>
#include <memory>
#include <mooncake_ep_exception.cuh>

namespace mooncake {

struct EventHandle {
    std::shared_ptr<cudaEvent_t> event;
    std::shared_ptr<void> keepalive;

    EventHandle() {
        event = std::make_shared<cudaEvent_t>();
        CUDA_CHECK(
            cudaEventCreateWithFlags(event.get(), cudaEventDisableTiming));
    }

    explicit EventHandle(uint64_t stream_ptr,
                         std::shared_ptr<void> keepalive = nullptr)
        : EventHandle() {
        this->keepalive = std::move(keepalive);
        auto stream = reinterpret_cast<cudaStream_t>(stream_ptr);
        CUDA_CHECK(cudaEventRecord(*event, stream));
    }

    EventHandle(const EventHandle& other) = default;

    ~EventHandle() {
        if (event && event.use_count() == 1 && *event != nullptr) {
            cudaEventDestroy(*event);
            *event = nullptr;
        }
    }

    void current_stream_wait(uint64_t stream_ptr) const {
        auto stream = reinterpret_cast<cudaStream_t>(stream_ptr);
        CUDA_CHECK(cudaStreamWaitEvent(stream, *event, 0));
    }

    void synchronize() const { CUDA_CHECK(cudaEventSynchronize(*event)); }
};

inline cudaEvent_t create_event(cudaStream_t stream) {
    cudaEvent_t event = nullptr;
    CUDA_CHECK(cudaEventCreateWithFlags(&event, cudaEventDisableTiming));
    CUDA_CHECK(cudaEventRecord(event, stream));
    return event;
}

inline void stream_wait(cudaStream_t dst_stream, cudaStream_t src_stream) {
    EP_HOST_ASSERT(dst_stream != src_stream);
    auto event = create_event(src_stream);
    CUDA_CHECK(cudaStreamWaitEvent(dst_stream, event, 0));
    CUDA_CHECK(cudaEventDestroy(event));
}

inline void stream_wait(cudaStream_t s, const EventHandle& event) {
    CUDA_CHECK(cudaStreamWaitEvent(s, *event.event, 0));
}

}  // namespace mooncake
