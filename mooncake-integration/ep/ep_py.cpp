
#include <mooncake_backend.h>
#include <mooncake_ep_buffer.h>
#include <pybind11/gil.h>  // For GIL management
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/python.h>
#include <torch/torch.h>

namespace py = pybind11;

namespace mooncake {

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
    auto version = py::module::import("torch")
                       .attr("__version__")
                       .attr("split")("+")
                       .cast<std::vector<std::string>>()[0];
    TORCH_CHECK(version == "2.8.0", "Mooncake Backend requires torch==2.8.0");
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

PYBIND11_MODULE(ep, m) {
    m.def("createMooncakeBackend", &createMooncakeBackend);
    m.def("createMooncakeCpuBackend", &createMooncakeCpuBackend);
    m.def("set_host_ip", &MooncakeBackend::setHostIp);
    m.def("set_device_filter", &MooncakeBackend::setDeviceFilter);
    m.def("get_preferred_hca", &getPreferredHca);

    py::class_<MooncakeBackend::MooncakeBackendOptions,
               c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>>(
        m, "MooncakeBackendOptions")
        .def(py::init<at::Tensor>(), py::arg("active_ranks"));

    m.def("get_ep_buffer_size_hint", &get_ep_buffer_size_hint);

    py::class_<EventHandle>(m, "EventHandle")
        .def(py::init<>())
        .def("current_stream_wait", &EventHandle::current_stream_wait);

    m.attr("MAX_QP_COUNT") = pybind11::int_(MAX_QP_COUNT);

    py::class_<MooncakeEpBuffer>(m, "Buffer")
        .def(py::init<int, int, int64_t, int>())
        .def("is_roce", &MooncakeEpBuffer::is_roce)
        .def("sync_ib", &MooncakeEpBuffer::sync_ib)
        .def("sync_roce", &MooncakeEpBuffer::sync_roce)
        .def("get_mr_info", &MooncakeEpBuffer::get_mr_info)
        .def("get_gid", &MooncakeEpBuffer::get_gid)
        .def("get_local_qpns", &MooncakeEpBuffer::get_local_qpns)
        .def("get_local_lids", &MooncakeEpBuffer::get_local_lids)
        .def("dispatch", &MooncakeEpBuffer::dispatch)
        .def("combine", &MooncakeEpBuffer::combine)
        .def("get_next_combine_buffer",
             &MooncakeEpBuffer::get_next_combine_buffer);
}

}  // namespace mooncake