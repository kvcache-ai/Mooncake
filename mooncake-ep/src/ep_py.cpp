#include <mooncake_ep_buffer.h>
#include <mooncake_ep_elastic_buffer.h>
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/python.h>
#include <torch/torch.h>

namespace py = pybind11;

namespace mooncake {

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("get_ep_buffer_size_hint", &get_ep_buffer_size_hint);
    m.def("calculate_elastic_buffer_size",
          &MooncakeElasticBuffer::calculate_buffer_size);

    py::class_<EventHandle>(m, "EventHandle")
        .def(py::init<>())
        .def("current_stream_wait", &EventHandle::current_stream_wait);

    py::class_<ElasticNativeHandle>(m, "ElasticNativeHandle")
        .def(py::init<>())
        .def_readwrite("do_expand", &ElasticNativeHandle::do_expand)
        .def_readwrite("num_experts", &ElasticNativeHandle::num_experts)
        .def_readwrite("expert_alignment", &ElasticNativeHandle::expert_alignment)
        .def_readwrite("num_max_tokens_per_rank",
                       &ElasticNativeHandle::num_max_tokens_per_rank)
        .def_readwrite("num_sms", &ElasticNativeHandle::num_sms)
        .def_readwrite("topk_idx", &ElasticNativeHandle::topk_idx)
        .def_readwrite("psum_num_recv_tokens_per_scaleup_rank",
                       &ElasticNativeHandle::psum_num_recv_tokens_per_scaleup_rank)
        .def_readwrite("psum_num_recv_tokens_per_expert",
                       &ElasticNativeHandle::psum_num_recv_tokens_per_expert)
        .def_readwrite("recv_src_metadata",
                       &ElasticNativeHandle::recv_src_metadata)
        .def_readwrite("recv_layout_range",
                       &ElasticNativeHandle::recv_layout_range)
        .def_readwrite("dst_buffer_slot_idx",
                       &ElasticNativeHandle::dst_buffer_slot_idx)
        .def_readwrite("token_metadata_at_forward",
                       &ElasticNativeHandle::token_metadata_at_forward)
        .def_readwrite("channel_linked_list",
                       &ElasticNativeHandle::channel_linked_list)
        .def_readwrite("num_recv_tokens_per_expert_list",
                       &ElasticNativeHandle::num_recv_tokens_per_expert_list);

    py::class_<ElasticDispatchOutput>(m, "ElasticDispatchOutput")
        .def_readonly("recv_x", &ElasticDispatchOutput::recv_x)
        .def_readonly("recv_x_scales", &ElasticDispatchOutput::recv_x_scales)
        .def_readonly("recv_topk_idx", &ElasticDispatchOutput::recv_topk_idx)
        .def_readonly("recv_topk_weights",
                      &ElasticDispatchOutput::recv_topk_weights)
        .def_readonly("handle", &ElasticDispatchOutput::handle)
        .def_readonly("event", &ElasticDispatchOutput::event);

    py::class_<ElasticCombineOutput>(m, "ElasticCombineOutput")
        .def_readonly("combined_x", &ElasticCombineOutput::combined_x)
        .def_readonly("combined_topk_weights",
                      &ElasticCombineOutput::combined_topk_weights)
        .def_readonly("event", &ElasticCombineOutput::event);

    m.attr("MAX_QP_COUNT") = pybind11::int_(MAX_QP_COUNT);

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
        .def("combine", &MooncakeEpBuffer::combine)
        .def("get_next_combine_buffer",
             &MooncakeEpBuffer::get_next_combine_buffer);

    py::class_<MooncakeElasticBuffer>(m, "ElasticBuffer")
        .def(py::init<int, int, int64_t, int64_t, int64_t, int64_t, bool,
                      bool, bool, bool, bool, int, int, int, int>(),
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
        .def("dispatch", &MooncakeElasticBuffer::dispatch,
             py::arg("x"), py::arg("sf"), py::arg("topk_idx"),
             py::arg("topk_weights"), py::arg("active_ranks"),
             py::arg("num_experts"), py::arg("num_max_tokens_per_rank"),
             py::arg("expert_alignment"), py::arg("num_sms"),
             py::arg("do_expand"), py::arg("do_cpu_sync"),
             py::arg("async_with_compute_stream"),
             py::arg("cached_handle") = std::nullopt)
        .def("combine", &MooncakeElasticBuffer::combine,
             py::arg("x"), py::arg("handle"), py::arg("topk_weights"),
             py::arg("active_ranks"), py::arg("num_sms"),
             py::arg("async_with_compute_stream"), py::arg("out") = std::nullopt);
}

}  // namespace mooncake
