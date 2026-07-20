#pragma once

#include <ATen/cuda/CUDAContext.h>
#include <memory>
#include <mooncake_ep_exception.cuh>
#include <torch/torch.h>

namespace mooncake {

struct EventHandle {
    std::shared_ptr<torch::Event> event;

    EventHandle() {
        event = std::make_shared<torch::Event>(torch::kCUDA);
        event->record(at::cuda::getCurrentCUDAStream());
    }

    explicit EventHandle(const at::cuda::CUDAStream& stream) {
        event = std::make_shared<torch::Event>(torch::kCUDA);
        event->record(stream);
    }

    EventHandle(const EventHandle& other) = default;

    void current_stream_wait() const {
        at::cuda::getCurrentCUDAStream().unwrap().wait(*event);
    }
};

inline torch::Event create_event(const at::cuda::CUDAStream& s) {
    auto event = torch::Event(torch::kCUDA);
    event.record(s);
    return event;
}

inline void stream_wait(const at::cuda::CUDAStream& s_0,
                        const at::cuda::CUDAStream& s_1) {
    EP_HOST_ASSERT(s_0.id() != s_1.id());
    s_0.unwrap().wait(create_event(s_1));
}

inline void stream_wait(const at::cuda::CUDAStream& s,
                        const EventHandle& event) {
    s.unwrap().wait(*event.event);
}

}  // namespace mooncake
