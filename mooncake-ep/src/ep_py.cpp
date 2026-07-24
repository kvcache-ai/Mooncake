#include <mooncake_ep_buffer.h>
#include <elastic/mooncake_ep_elastic_buffer.h>
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>

namespace py = pybind11;

namespace mooncake {

void bind_legacy_buffer_perf(py::module_& module);

PYBIND11_MODULE(_ep, m) {
    m.def("get_ep_buffer_size_hint", &get_ep_buffer_size_hint);
    m.def("calculate_elastic_buffer_size",
          &MooncakeElasticBuffer::calculate_buffer_size);

    py::class_<EventHandle>(m, "EventHandle")
        .def(py::init<uint64_t>(), py::arg("stream_ptr") = 0)
        .def("current_stream_wait", &EventHandle::current_stream_wait,
             py::arg("stream_ptr"))
        .def("synchronize", &EventHandle::synchronize);

    m.attr("MAX_QP_COUNT") = pybind11::int_(MAX_QP_COUNT);
    bind_legacy_buffer_perf(m);

    py::class_<MooncakeEpBuffer>(m, "Buffer")
        .def(py::init<int, int, int64_t>())
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
        .def("dispatch", &MooncakeEpBuffer::dispatch)
        .def("combine", &MooncakeEpBuffer::combine);

    py::class_<MooncakeElasticBuffer>(m, "ElasticBuffer")
        .def(py::init<int, int, int64_t, int64_t, int64_t, int64_t, bool, bool,
                      bool, bool, bool, int, int, int, int>(),
             py::arg("rank"), py::arg("num_ranks"), py::arg("num_buffer_bytes"),
             py::arg("num_max_tokens_per_rank"), py::arg("hidden"),
             py::arg("num_topk"), py::arg("use_fp8_dispatch"),
             py::arg("deterministic"), py::arg("allow_hybrid_mode"),
             py::arg("allow_multiple_reduction"),
             py::arg("prefer_overlap_with_compute"), py::arg("sl_idx"),
             py::arg("num_allocated_qps"), py::arg("num_cpu_timeout_secs"),
             py::arg("num_gpu_timeout_secs"))
        .def_static("calculate_buffer_size",
                    &MooncakeElasticBuffer::calculate_buffer_size)
        .def("get_physical_domain_size",
             &MooncakeElasticBuffer::get_physical_domain_size)
        .def("get_logical_domain_size",
             &MooncakeElasticBuffer::get_logical_domain_size)
        .def("get_theoretical_num_sms",
             &MooncakeElasticBuffer::get_theoretical_num_sms)
        .def("ibgda_disabled", &MooncakeElasticBuffer::ibgda_disabled)
        .def("use_fast_path", &MooncakeElasticBuffer::use_fast_path)
        .def("update_local_qpns", &MooncakeElasticBuffer::update_local_qpns)
        .def("is_roce", &MooncakeElasticBuffer::is_roce)
        .def("sync_ibgda_peers", &MooncakeElasticBuffer::sync_ibgda_peers)
        .def("get_mr_info", &MooncakeElasticBuffer::get_mr_info)
        .def("get_gid", &MooncakeElasticBuffer::get_gid)
        .def("get_local_qpns", &MooncakeElasticBuffer::get_local_qpns)
        .def("get_local_lids", &MooncakeElasticBuffer::get_local_lids)
        .def("get_ipc_handle", &MooncakeElasticBuffer::get_ipc_handle)
        .def("sync_nvlink_ipc_handles",
             &MooncakeElasticBuffer::sync_nvlink_ipc_handles)
        .def("dispatch", &MooncakeElasticBuffer::dispatch)
        .def("combine", &MooncakeElasticBuffer::combine);
}

}  // namespace mooncake
