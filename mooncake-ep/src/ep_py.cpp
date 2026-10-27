#include <mooncake_ep_buffer.h>
#if !defined(MOONCAKE_EP_USE_MACA)
#include <elastic/mooncake_ep_elastic_buffer.h>
#endif
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>

namespace py = pybind11;

namespace mooncake {

void bind_legacy_buffer_perf(py::module_& module);

PYBIND11_MODULE(_ep, m) {
    m.def("get_ep_buffer_size_hint", &get_ep_buffer_size_hint);
    m.def("get_ep_elastic_buffer_size_hint", &get_ep_elastic_buffer_size_hint);
#if !defined(MOONCAKE_EP_USE_MACA)
    m.def("calculate_elastic_buffer_size",
          &MooncakeElasticBuffer::calculate_buffer_size);
    m.def("create_nccl_unique_id", &create_elastic_nccl_unique_id);
#endif
    m.def("has_nccl_device_support", []() {
#ifdef USE_NCCL_DEVICE
        return true;
#else
        return false;
#endif
    });

    py::class_<EventHandle>(m, "EventHandle")
        .def(py::init<uint64_t>(), py::arg("stream_ptr") = 0)
        .def("current_stream_wait", &EventHandle::current_stream_wait,
             py::arg("stream_ptr"))
        .def("synchronize", &EventHandle::synchronize);

    m.attr("MAX_QP_COUNT") = pybind11::int_(MAX_QP_COUNT);
    bind_legacy_buffer_perf(m);

    py::class_<MooncakeEpBuffer>(m, "Buffer")
        .def(py::init<int, int, int64_t, bool>(), py::arg("rank"),
             py::arg("num_ranks"), py::arg("num_ep_buffer_bytes"),
             py::arg("disable_p2p") = false)
        .def("ibgda_disabled", &MooncakeEpBuffer::ibgda_disabled)
        .def("p2p_enabled", &MooncakeEpBuffer::p2p_enabled)
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
        .def("_configure_elastic", &MooncakeEpBuffer::configure_elastic)
        .def("_set_explicit_destroy", &MooncakeEpBuffer::set_explicit_destroy)
        .def("_validate_connect_ranks",
             &MooncakeEpBuffer::validate_connect_ranks)
        .def("_stage_peer",
             [](MooncakeEpBuffer& buffer, int peer_rank, int64_t raddr,
                int32_t rkey, int64_t subnet_prefix, int64_t interface_id,
                const std::vector<int32_t>& qpns,
                const std::vector<int32_t>& lids,
                const std::vector<int32_t>& ipc_handle) {
                 py::gil_scoped_release release;
                 buffer.stage_peer(peer_rank, raddr, rkey, subnet_prefix,
                                   interface_id, qpns, lids, ipc_handle);
             })
        .def("_disconnect_ranks", &MooncakeEpBuffer::disconnect_ranks)
        .def("_update_mask_buffer", &MooncakeEpBuffer::update_mask_buffer)
        .def("_query_mask_buffer", &MooncakeEpBuffer::query_mask_buffer)
        .def("_clean_mask_buffer", &MooncakeEpBuffer::clean_mask_buffer)
        .def("_barrier", &MooncakeEpBuffer::barrier)
        .def("_get_active_rank_bound", &MooncakeEpBuffer::get_active_rank_bound)
        .def("_get_comm_stream", &MooncakeEpBuffer::get_comm_stream)
        .def("_get_local_buffer_descriptor",
             &MooncakeEpBuffer::get_local_buffer_descriptor)
        .def("destroy", &MooncakeEpBuffer::destroy)
        .def("dispatch", &MooncakeEpBuffer::dispatch, py::arg("x_ptr"),
             py::arg("topk_idx_ptr"), py::arg("active_ranks_ptr"),
             py::arg("num_tokens"), py::arg("hidden"), py::arg("num_topk"),
             py::arg("num_max_dispatch_tokens_per_rank"),
             py::arg("num_experts"), py::arg("timeout_us"), py::arg("use_fp8"),
             py::arg("packed_recv_x_ptr"), py::arg("packed_recv_x_scales_ptr"),
             py::arg("packed_recv_count_ptr"),
             py::arg("packed_recv_src_info_ptr"),
             py::arg("packed_recv_layout_range_ptr"), py::arg("async"),
             py::arg("return_recv_hook"), py::arg("compute_stream_ptr"),
             py::arg("cumulative_local_expert_recv_stats_ptr") = 0)
        .def("combine", &MooncakeEpBuffer::combine);

#if !defined(MOONCAKE_EP_USE_MACA)
    py::class_<MooncakeElasticBuffer>(m, "ElasticBuffer")
        .def(py::init<int, int, int64_t, int64_t, int64_t, int64_t, bool, bool,
                      bool, bool, bool, int, int, int, int, std::string,
                      std::vector<int32_t>>(),
             py::arg("rank"), py::arg("num_ranks"), py::arg("num_buffer_bytes"),
             py::arg("num_max_tokens_per_rank"), py::arg("hidden"),
             py::arg("num_topk"), py::arg("use_fp8_dispatch"),
             py::arg("deterministic"), py::arg("allow_hybrid_mode"),
             py::arg("allow_multiple_reduction"),
             py::arg("prefer_overlap_with_compute"), py::arg("sl_idx"),
             py::arg("num_allocated_qps"), py::arg("num_cpu_timeout_secs"),
             py::arg("num_gpu_timeout_secs"), py::arg("transport") = "ibgda",
             py::arg("nccl_unique_id") = std::vector<int32_t>{})
        .def("destroy", &MooncakeElasticBuffer::destroy)
        .def("using_nccl", &MooncakeElasticBuffer::using_nccl)
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
        .def("reconfigure_nccl", &MooncakeElasticBuffer::reconfigure_nccl)
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
#endif
}

}  // namespace mooncake
