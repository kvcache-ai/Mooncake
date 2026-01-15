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

int getNumSyncedRanks(c10::intrusive_ptr<c10d::Backend> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getNumSyncedRanks();
}

void extendGroupSizeTo(c10::intrusive_ptr<c10d::Backend> backend, int size) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    mooncakeBackend->extendGroupSizeTo(size);
}

std::vector<bool> getPeerState(c10::intrusive_ptr<c10d::Backend> backend,
                               const std::vector<int> &ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getPeerState(ranks);
}

void recoverRanks(c10::intrusive_ptr<c10d::Backend> backend,
                  const std::vector<int> &ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    mooncakeBackend->recoverRanks(ranks);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("createMooncakeBackend", &createMooncakeBackend);
    m.def("createMooncakeCpuBackend", &createMooncakeCpuBackend);
    m.def("set_host_ip", &MooncakeBackend::setHostIp);
    m.def("set_device_filter", &MooncakeBackend::setDeviceFilter);
    m.def("get_preferred_hca", &getPreferredHca);
    m.def("get_active_ranks", &getActiveRanks);
    m.def("get_num_synced_ranks", &getNumSyncedRanks);
    m.def("extend_group_size_to", &extendGroupSizeTo);
    m.def("get_peer_state", &getPeerState);
    m.def("recover_ranks", &recoverRanks);

    py::class_<MooncakeBackend::MooncakeBackendOptions,
               c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>>(
        m, "MooncakeBackendOptions")
        .def(py::init<at::Tensor>(), py::arg("active_ranks"))
        .def(py::init<at::Tensor, bool>(), py::arg("active_ranks"),
             py::arg("is_extension"));

    m.def("get_ep_buffer_size_hint", &get_ep_buffer_size_hint);

    py::class_<EventHandle>(m, "EventHandle")
        .def(py::init<>())
        .def("current_stream_wait", &EventHandle::current_stream_wait);

    m.attr("MAX_QP_COUNT") = pybind11::int_(MAX_QP_COUNT);

    py::class_<MooncakeEpBuffer>(m, "Buffer")
        .def(py::init<int, int, int64_t, std::string>())
        .def("ibgda_disabled", &MooncakeEpBuffer::ibgda_disabled)
        .def("is_roce", &MooncakeEpBuffer::is_roce)
        .def("sync_ib", &MooncakeEpBuffer::sync_ib)
        .def("sync_roce", &MooncakeEpBuffer::sync_roce)
        .def("get_mr_info", &MooncakeEpBuffer::get_mr_info)
        .def("get_gid", &MooncakeEpBuffer::get_gid)
        .def("get_local_qpns", &MooncakeEpBuffer::get_local_qpns)
        .def("get_local_lids", &MooncakeEpBuffer::get_local_lids)
        .def("get_ipc_handle", &MooncakeEpBuffer::get_ipc_handle)
        .def("sync_nvlink_ipc_handles",
             &MooncakeEpBuffer::sync_nvlink_ipc_handles)
        .def("dispatch", &MooncakeEpBuffer::dispatch)
        .def("combine", &MooncakeEpBuffer::combine)
        .def("get_next_combine_buffer",
             &MooncakeEpBuffer::get_next_combine_buffer);
}

}  // namespace mooncake