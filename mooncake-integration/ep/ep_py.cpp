
#include <mooncake_backend.h>
#include <pybind11/gil.h>  // For GIL management
#include <pybind11/stl.h>
#include <torch/torch.h>

#include "pybind_client.h"

#include "integration_utils.h"

namespace py = pybind11;

namespace mooncake {

PYBIND11_MODULE(ep, m) {
    py::class_<MooncakeBackend>(m, "MooncakeBackend")
        .def(py::init<c10::intrusive_ptr<::c10d::Store>, int, int,
                      c10::intrusive_ptr<::c10d::Backend::Options>>())
        .def("broadcast", &MooncakeBackend::broadcast);
}

}  // namespace mooncake