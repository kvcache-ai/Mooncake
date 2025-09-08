// file name: dummy.hpp
#include <torch/python.h>

#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>
#include <torch/csrc/distributed/c10d/Store.hpp>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Utils.hpp>

#include <pybind11/chrono.h>

using namespace c10d;

class BackendDummy : public c10d::Backend {
public:
    BackendDummy(int rank, int size): c10d::Backend(rank, size) {}

    c10::intrusive_ptr<c10d::Work> allgather(
        std::vector<std::vector<at::Tensor>>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const AllgatherOptions& opts = AllgatherOptions()) override;

    c10::intrusive_ptr<c10d::Work> allreduce(
        std::vector<at::Tensor>& tensors,
        const AllreduceOptions& opts = AllreduceOptions()) override;

    // The collective communication APIs without a custom implementation
    // will error out if invoked by application code.
    static c10::intrusive_ptr<c10d::Backend> createBackendDummy(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<c10d::Backend::Options>
        backendOptions) {
        return c10::make_intrusive<BackendDummy>(distBackendOpts.group_rank, distBackendOpts.group_size);
    }

    static void BackendDummyConstructor() __attribute__((constructor)) {
        py::object module = py::module::import("torch.distributed");
        py::object register_backend =
            module.attr("Backend").attr("register_backend");
        // torch.distributed.Backend.register_backend will add `dummy` as a
        // new valid backend.
        register_backend("dummy", py::cpp_function(createBackendDummy), true);
    }

};

class WorkDummy : public c10d::Work {
public:
    WorkDummy(
      OpType opType,
      c10::intrusive_ptr<c10::ivalue::Future> future) // future of the output
      : c10d::Work(
          -1, // rank, only used by recvAnySource, irrelevant in this demo
          opType),
      future_(std::move(future)) {}

private:
    c10::intrusive_ptr<c10::ivalue::Future> future_;
};