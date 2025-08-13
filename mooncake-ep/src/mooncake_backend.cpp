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

const std::string MooncakeBackend::getBackendName() const { return "mooncake"; }

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allreduce(
    std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts) {
    auto future = c10::make_intrusive<c10::ivalue::Future>(
        c10::ListType::create(c10::TensorType::get()));
    future->markCompleted(c10::IValue(tensors));
    return c10::make_intrusive<MooncakeWork>(c10d::OpType::ALLGATHER,
                                             std::move(future));
}
}  // namespace mooncake
