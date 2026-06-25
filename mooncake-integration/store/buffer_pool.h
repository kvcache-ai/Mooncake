#pragma once

#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace mooncake {

void bind_buffer_pool(py::module &m);

}  // namespace mooncake
