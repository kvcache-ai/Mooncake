#include <torch/csrc/distributed/c10d/DistributedBackend.hpp>
#include <pybind11/pybind11.h>

namespace py = pybind11;

// Function to expose
void testBinding1(c10d::DistributedBackendOptions distBackendOpts) {
    // Do nothing, just to test the binding
}

PYBIND11_MODULE(minimal, m) {
    m.def("testBinding1", &testBinding1);
}
