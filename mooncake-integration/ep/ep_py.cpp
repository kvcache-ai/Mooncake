
#include <mooncake_backend.h>
#include <pybind11/gil.h>  // For GIL management
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/torch.h>

#include "pybind_client.h"

#include "integration_utils.h"

namespace py = pybind11;

namespace mooncake {

c10::intrusive_ptr<c10d::Backend> createMooncakeBackend(
    py::object store_like, int rank, int size,
    std::chrono::milliseconds timeout) {
    if (py::hasattr(store_like, "underlying_store")) {
        store_like = store_like.attr("underlying_store");
    }
    auto store = store_like.cast<c10::intrusive_ptr<c10d::Store>>();
    auto options = c10::make_intrusive<::c10d::Backend::Options>("mooncake");
    options->timeout = timeout;
    return c10::make_intrusive<MooncakeBackend>(store, rank, size, options);
}

__attribute__((constructor)) static void MooncakeBackendConstructor() {
    py::object module = py::module::import("torch.distributed");
    py::object register_backend =
        module.attr("Backend").attr("register_backend");
    py::dict kwargs;
    kwargs["devices"] = py::make_tuple("cuda");
    register_backend("mooncake", py::cpp_function(createMooncakeBackend),
                     **kwargs);
}

PYBIND11_MODULE(ep, m) {
    m.def("createMooncakeBackend", &createMooncakeBackend);
}

}  // namespace mooncake