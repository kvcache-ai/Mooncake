// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The `_ep` module for USE_XPU builds. It exposes the subset of the CUDA
// module that mooncake_ep_buffer.Buffer uses, under the same names.

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "mooncake_ep_xpu_buffer.h"
#include "xpu_ep_common.h"

namespace py = pybind11;

namespace mooncake {
namespace xpu_ep {

PYBIND11_MODULE(_ep, m) {
    m.def("get_ep_buffer_size_hint", &get_ep_buffer_size_hint);
    m.def("has_nccl_device_support", []() { return false; });
    m.attr("MAX_QP_COUNT") = py::int_(0);
    m.attr("DEVICE_TYPE") = py::str("xpu");

    py::class_<XpuEventHandle>(m, "EventHandle")
        .def(py::init<uint64_t>(), py::arg("stream_ptr") = 0)
        .def("current_stream_wait", &XpuEventHandle::current_stream_wait,
             py::arg("stream_ptr"))
        .def("synchronize", &XpuEventHandle::synchronize);

    using B = MooncakeEpXpuBuffer;
    py::class_<B>(m, "Buffer")
        .def(py::init<int, int, int64_t, bool, uint64_t>(), py::arg("rank"),
             py::arg("num_ranks"), py::arg("num_ep_buffer_bytes"),
             py::arg("disable_p2p") = false, py::arg("stream_ptr") = 0)
        .def("ibgda_disabled", &B::ibgda_disabled)
        .def("p2p_enabled", &B::p2p_enabled)
        .def("use_fast_path", &B::use_fast_path)
        .def("is_roce", &B::is_roce)
        // RDMA bootstrap is never reached (ibgda_disabled() is true); these
        // keep the CUDA method set for callers that probe it.
        .def("get_mr_info",
             [](B&) { return std::tuple<int64_t, int32_t>{0, 0}; })
        .def("update_local_qpns", [](B&) {})
        .def("get_ipc_handle", &B::get_ipc_handle)
        .def("sync_nvlink_ipc_handles", &B::sync_nvlink_ipc_handles)
        .def("dispatch", &B::dispatch)
        .def("combine", &B::combine);
}

}  // namespace xpu_ep
}  // namespace mooncake
