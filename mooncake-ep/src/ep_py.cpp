#include <mooncake_ep_buffer.h>
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/python.h>
#include <torch/torch.h>
#ifndef MOONCAKE_EP_STANDALONE
#include <transfer_engine.h>
#endif
#include <cstdint>
#include <stdexcept>

namespace py = pybind11;

namespace mooncake {

// Trampoline: Python passes engine pointer as uintptr_t (from
// engine.get_engine_ptr()), C++ receives it as TransferEngine*.
// This avoids cross-module pybind11 type registration issues.
MooncakeEpBuffer* make_buffer(int rank, int num_ranks, int64_t num_ep_buffer_bytes,
                              uint64_t engine_ptr) {
#ifdef MOONCAKE_EP_STANDALONE
    if (engine_ptr != 0) {
        throw std::invalid_argument(
            "MOONCAKE_EP_STANDALONE does not accept a TransferEngine pointer");
    }
    return new MooncakeEpBuffer(rank, num_ranks, num_ep_buffer_bytes, nullptr);
#else
    auto* engine = reinterpret_cast<TransferEngine*>(engine_ptr);
    return new MooncakeEpBuffer(rank, num_ranks, num_ep_buffer_bytes, engine);
#endif
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("get_ep_buffer_size_hint", &get_ep_buffer_size_hint);

    py::class_<EventHandle>(m, "EventHandle")
        .def(py::init<>())
        .def("current_stream_wait", &EventHandle::current_stream_wait);

    m.attr("MAX_QP_COUNT") = pybind11::int_(MAX_QP_COUNT);

    py::class_<MooncakeEpBuffer>(m, "Buffer")
        .def(py::init(&make_buffer),
             py::arg("rank"), py::arg("num_ranks"),
             py::arg("num_ep_buffer_bytes"),
             py::arg("engine_ptr") = uint64_t(0))
        .def("ibgda_disabled", &MooncakeEpBuffer::ibgda_disabled)
        .def("use_fast_path", &MooncakeEpBuffer::use_fast_path)
        .def("update_local_qpns", &MooncakeEpBuffer::update_local_qpns)
        .def("is_roce", &MooncakeEpBuffer::is_roce)
        .def("sync_ibgda_peers", &MooncakeEpBuffer::sync_ibgda_peers)
        .def("get_mr_info", &MooncakeEpBuffer::get_mr_info)
        .def("get_gid", &MooncakeEpBuffer::get_gid)
        .def("get_local_qpns", &MooncakeEpBuffer::get_local_qpns)
        .def("get_local_lids", &MooncakeEpBuffer::get_local_lids)
        .def("get_ipc_handle", &MooncakeEpBuffer::get_ipc_handle)
        .def("sync_nvlink_ipc_handles",
             &MooncakeEpBuffer::sync_nvlink_ipc_handles)
        .def("verify_peer_access", &MooncakeEpBuffer::verify_peer_access)
        .def("dispatch", &MooncakeEpBuffer::dispatch)
        .def("combine", &MooncakeEpBuffer::combine)
        .def("get_next_combine_buffer",
             &MooncakeEpBuffer::get_next_combine_buffer);
}

}  // namespace mooncake
