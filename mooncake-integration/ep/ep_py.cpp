
#include <mooncake_backend.h>
#include <pybind11/gil.h>  // For GIL management
#include <pybind11/stl.h>
#include <torch/torch.h>

#include "pybind_client.h"

#include "integration_utils.h"

namespace py = pybind11;

namespace mooncake {

__attribute__((constructor)) static void MooncakeBackendConstructor() {
    py::object module = py::module::import("torch.distributed");
    py::object register_backend =
        module.attr("Backend").attr("register_backend");
    register_backend("mooncake",
                     py::cpp_function(MooncakeBackend::createMooncakeBackend));
}

PYBIND11_MODULE(ep, m) {
    m.def("createMooncakeBackend", &MooncakeBackend::createMooncakeBackend);
}

}  // namespace mooncake