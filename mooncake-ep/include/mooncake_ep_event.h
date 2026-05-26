#pragma once

#ifdef MOONCAKE_EP_USE_MUSA
#include <ATen/musa/MUSAContext.h>
#else
#include <ATen/cuda/CUDAContext.h>
#endif
#include <memory>
#include <mooncake_ep_exception.cuh>
#include <torch/torch.h>

namespace mooncake {

#ifdef MOONCAKE_EP_USE_MUSA
using DeviceStream = at::musa::MUSAStream;
using DeviceType = torch::DeviceType;
constexpr DeviceType kDeviceType = torch::kMUSA;
#else
using DeviceStream = at::cuda::CUDAStream;
using DeviceType = torch::DeviceType;
constexpr DeviceType kDeviceType = torch::kCUDA;
#endif

struct EventHandle {
    std::shared_ptr<torch::Event> event;

    EventHandle() {
        event = std::make_shared<torch::Event>(kDeviceType);
#ifdef MOONCAKE_EP_USE_MUSA
        event->record(at::musa::getCurrentMUSAStream());
#else
        event->record(at::cuda::getCurrentCUDAStream());
#endif
    }

    explicit EventHandle(const DeviceStream& stream) {
        event = std::make_shared<torch::Event>(kDeviceType);
        event->record(stream);
    }

    EventHandle(const EventHandle& other) = default;

    void current_stream_wait() const {
#ifdef MOONCAKE_EP_USE_MUSA
        at::musa::getCurrentMUSAStream().unwrap().wait(*event);
#else
        at::cuda::getCurrentCUDAStream().unwrap().wait(*event);
#endif
    }
};

inline torch::Event create_event(const DeviceStream& s) {
    auto event = torch::Event(kDeviceType);
    event.record(s);
    return event;
}

inline void stream_wait(const DeviceStream& s_0,
                        const DeviceStream& s_1) {
    EP_HOST_ASSERT(s_0.id() != s_1.id());
    s_0.unwrap().wait(create_event(s_1));
}

inline void stream_wait(const DeviceStream& s,
                        const EventHandle& event) {
    s.unwrap().wait(*event.event);
}

}  // namespace mooncake
