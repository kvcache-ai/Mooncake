// file name: dummy.cpp
#include "dummy.hpp"

namespace c10d {

// This is a dummy allgather that sets all output tensors to zero
// Modify the implementation to conduct real communication asynchronously
c10::intrusive_ptr<Work> BackendDummy::allgather(
        std::vector<std::vector<at::Tensor>>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const AllgatherOptions& /* unused */) {
    for (auto& outputTensorVec : outputTensors) {
        for (auto& outputTensor : outputTensorVec) {
            outputTensor.zero_();
        }
    }

    auto future = c10::make_intrusive<c10::ivalue::Future>(
        c10::ListType::create(c10::ListType::create(c10::TensorType::get())));
    future->markCompleted(c10::IValue(outputTensors));
    return c10::make_intrusive<WorkDummy>(OpType::ALLGATHER, std::move(future));
}

// This is a dummy allreduce that sets all output tensors to zero
// Modify the implementation to conduct real communication asynchronously
c10::intrusive_ptr<Work> BackendDummy::allreduce(
        std::vector<at::Tensor>& tensors,
        const AllreduceOptions& opts) {
    for (auto& tensor : tensors) {
        tensor.zero_();
    }

    auto future = c10::make_intrusive<c10::ivalue::Future>(
        c10::ListType::create(c10::TensorType::get()));
    future->markCompleted(c10::IValue(tensors));
    return c10::make_intrusive<WorkDummy>(OpType::ALLGATHER, std::move(future));
}

// file name: dummy.cpp
c10::intrusive_ptr<Backend> BackendDummy::createBackendDummy(
        const c10::intrusive_ptr<::c10d::Store>& /* unused */,
        int rank,
        int size,
        const std::chrono::duration<float>& /* unused */) {
    return c10::make_intrusive<BackendDummy>(rank, size);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("createBackendDummy", &BackendDummy::createBackendDummy);
}
} // namespace c10d