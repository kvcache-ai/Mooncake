#if defined(MOONCAKE_EP_USE_XPU)
// Intel XPU: the SYCL buffer in xpu/ has the methods mooncake_ep_buffer.Buffer
// uses, under the same Python names.
#include "xpu/mooncake_ep_xpu_buffer.h"
#else
#include <mooncake_ep_buffer.h>
#endif
// No elastic buffer on MACA or XPU.
#if !defined(MOONCAKE_EP_USE_MACA) && !defined(MOONCAKE_EP_USE_XPU)
#define MOONCAKE_EP_HAS_ELASTIC 1
#include <elastic/mooncake_ep_elastic_buffer.h>
#endif
#include <pybind11/pybind11.h>
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>

namespace py = pybind11;

namespace mooncake {

#if defined(MOONCAKE_EP_USE_XPU)
using EpBuffer = xpu_ep::MooncakeEpXpuBuffer;
using EpEventHandle = xpu_ep::XpuEventHandle;
using xpu_ep::get_ep_buffer_size_hint;
constexpr const char* kEpDeviceType = "xpu";
#else
void bind_legacy_buffer_perf(py::module_& module);
using EpBuffer = MooncakeEpBuffer;
using EpEventHandle = EventHandle;
// CUDA-dialect builds (CUDA, MUSA, MACA) are all driven through torch.cuda.
constexpr const char* kEpDeviceType = "cuda";
#endif

PYBIND11_MODULE(_ep, m) {
    m.def("get_ep_buffer_size_hint", &get_ep_buffer_size_hint);
#ifdef MOONCAKE_EP_HAS_ELASTIC
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
    m.attr("DEVICE_TYPE") = py::str(kEpDeviceType);

    py::class_<EpEventHandle>(m, "EventHandle")
        .def(py::init<uint64_t>(), py::arg("stream_ptr") = 0)
        .def("current_stream_wait", &EpEventHandle::current_stream_wait,
             py::arg("stream_ptr"))
        .def("synchronize", &EpEventHandle::synchronize);

#if !defined(MOONCAKE_EP_USE_XPU)
    m.attr("MAX_QP_COUNT") = pybind11::int_(MAX_QP_COUNT);
    bind_legacy_buffer_perf(m);
#endif

    py::class_<EpBuffer> buffer(m, "Buffer");
#if defined(MOONCAKE_EP_USE_XPU)
    // The XPU buffer allocates on, and launches into, the caller's queue.
    buffer.def(py::init<int, int, int64_t, bool, uint64_t>(), py::arg("rank"),
               py::arg("num_ranks"), py::arg("num_ep_buffer_bytes"),
               py::arg("disable_p2p") = false, py::arg("stream_ptr") = 0);
#else
    buffer.def(py::init<int, int, int64_t, bool>(), py::arg("rank"),
               py::arg("num_ranks"), py::arg("num_ep_buffer_bytes"),
               py::arg("disable_p2p") = false);
#endif
    buffer.def("ibgda_disabled", &EpBuffer::ibgda_disabled)
        .def("p2p_enabled", &EpBuffer::p2p_enabled)
        .def("use_fast_path", &EpBuffer::use_fast_path)
        .def("is_roce", &EpBuffer::is_roce)
        .def("get_ipc_handle", &EpBuffer::get_ipc_handle)
        .def("sync_nvlink_ipc_handles", &EpBuffer::sync_nvlink_ipc_handles)
        .def("dispatch", &EpBuffer::dispatch)
        .def("combine", &EpBuffer::combine);
#if !defined(MOONCAKE_EP_USE_XPU)
    // RDMA (IBGDA) bootstrap; XPU reaches peers over Level Zero IPC only.
    buffer.def("update_local_qpns", &EpBuffer::update_local_qpns)
        .def("sync_ibgda_peers", &EpBuffer::sync_ibgda_peers)
        .def("get_mr_info", &EpBuffer::get_mr_info)
        .def("get_gid", &EpBuffer::get_gid)
        .def("get_local_qpns", &EpBuffer::get_local_qpns)
        .def("get_local_lids", &EpBuffer::get_local_lids);
#endif

#ifdef MOONCAKE_EP_HAS_ELASTIC
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
