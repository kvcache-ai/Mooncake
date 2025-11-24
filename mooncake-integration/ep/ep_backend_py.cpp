#include <mooncake_backend.h>
#include <pybind11/gil.h>  // For GIL management
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/python.h>
#include <torch/torch.h>

namespace py = pybind11;

namespace mooncake {

class MooncakeBackend final : public ::c10d::Backend {
   public:
    struct MooncakeBackendOptions final : ::c10d::Backend::Options {
        explicit MooncakeBackendOptions(at::Tensor activeRanks)
            : Options{"mooncake"}, activeRanks_{activeRanks} {}

        ~MooncakeBackendOptions() override = default;

        at::Tensor activeRanks_;
    };

    MooncakeBackend(c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
                    c10::intrusive_ptr<MooncakeBackendOptions> options,
                    bool isCpu = false)
        : ::c10d::Backend(rank, size),
          backend_impl_(
              store, rank, size,
              options ? options->activeRanks_
                      : at::ones({size}, torch::dtype(torch::kInt32)
                                             .device(isCpu ? torch::kCPU
                                                           : torch::kCUDA)),
              isCpu) {}

    ~MooncakeBackend() override = default;

    const std::string getBackendName() const override { return "mooncake"; }

    c10::intrusive_ptr<c10d::Work> broadcast(
        std::vector<at::Tensor>& tensors,
        const c10d::BroadcastOptions& opts) override {
        return backend_impl_.broadcast(tensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> allreduce(
        std::vector<at::Tensor>& tensors,
        const c10d::AllreduceOptions& opts) override {
        return backend_impl_.allreduce(tensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> allgather(
        std::vector<std::vector<at::Tensor>>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::AllgatherOptions& opts) override {
        return backend_impl_.allgather(outputTensors, inputTensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> _allgather_base(
        at::Tensor& outputBuffer, at::Tensor& inputBuffer,
        const c10d::AllgatherOptions& opts) override {
        return backend_impl_._allgather_base(outputBuffer, inputBuffer, opts);
    }

    c10::intrusive_ptr<c10d::Work> _reduce_scatter_base(
        at::Tensor& outputBuffer, at::Tensor& inputBuffer,
        const c10d::ReduceScatterOptions& opts) override {
        return backend_impl_._reduce_scatter_base(outputBuffer, inputBuffer, opts);
    }

    c10::intrusive_ptr<c10d::Work> alltoall(
        std::vector<at::Tensor>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::AllToAllOptions& opts) override {
        return backend_impl_.alltoall(outputTensors, inputTensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> barrier(
        const c10d::BarrierOptions& opts) override {
        return backend_impl_.barrier(opts);
    }

    void shutdown() override { backend_impl_.shutdown(); }

    static void setHostIp(const std::string& hostIp) {
        MooncakeBackendImpl::setHostIp(hostIp);
    }

    static void setDeviceFilter(std::vector<std::string> filters) {
        MooncakeBackendImpl::setDeviceFilter(std::move(filters));
    }

    std::string getPreferredHca(std::string location) {
        return backend_impl_.getPreferredHca(location);
    }

    at::Tensor getActiveRanksTensor() {
        return backend_impl_.getActiveRanksTensor();
    }

   private:
    MooncakeBackendImpl backend_impl_;
};

c10::intrusive_ptr<c10d::Backend> createMooncakeBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    return c10::make_intrusive<MooncakeBackend>(
        distBackendOpts.store, distBackendOpts.group_rank,
        distBackendOpts.group_size, backendOptions);
}

c10::intrusive_ptr<c10d::Backend> createMooncakeCpuBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    return c10::make_intrusive<MooncakeBackend>(
        distBackendOpts.store, distBackendOpts.group_rank,
        distBackendOpts.group_size, backendOptions, true);
}

__attribute__((constructor)) static void MooncakeBackendConstructor() {
    py::object module = py::module::import("torch.distributed");
    py::object register_backend =
        module.attr("Backend").attr("register_backend");
    py::dict kwargsCpu;
    kwargsCpu["devices"] = py::make_tuple("cpu");
    register_backend("mooncake-cpu", py::cpp_function(createMooncakeCpuBackend),
                     /* extended_api */ true, **kwargsCpu);
    py::dict kwargsCuda;
    kwargsCuda["devices"] = py::make_tuple("cuda");
    register_backend("mooncake", py::cpp_function(createMooncakeBackend),
                     /* extended_api */ true, **kwargsCuda);
}

std::string getPreferredHca(c10::intrusive_ptr<c10d::Backend> backend,
                            std::string location) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getPreferredHca(location);
}

at::Tensor getActiveRanks(c10::intrusive_ptr<c10d::Backend> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getActiveRanksTensor();
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("createMooncakeBackend", &createMooncakeBackend);
    m.def("createMooncakeCpuBackend", &createMooncakeCpuBackend);
    m.def("set_host_ip", &MooncakeBackend::setHostIp);
    m.def("set_device_filter", &MooncakeBackend::setDeviceFilter);
    m.def("get_preferred_hca", &getPreferredHca);
    m.def("get_active_ranks", &getActiveRanks);

    py::class_<MooncakeBackend::MooncakeBackendOptions,
               c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>>(
        m, "MooncakeBackendOptions")
        .def(py::init<at::Tensor>(), py::arg("active_ranks"));
}

}
