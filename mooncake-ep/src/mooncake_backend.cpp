#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>
#include <iostream>
#include <thread>
#include <future>

namespace mooncake {

MooncakeBackend::MooncakeBackend(c10::intrusive_ptr<::c10d::Store> store,
                                 int rank, int size,
                                 c10::intrusive_ptr<Options> options)
    : ::c10d::Backend(rank, size) {}

c10::intrusive_ptr<c10d::Backend> MooncakeBackend::createMooncakeBackend(
    const c10::intrusive_ptr<c10d::Store>& store, int rank, int size,
    const std::chrono::duration<float>& timeout) {
    auto options =
        c10::make_intrusive<::c10d::Backend::Options>("mooncake_backend");
    return c10::make_intrusive<MooncakeBackend>(store, rank, size, options);
}

}  // namespace mooncake
