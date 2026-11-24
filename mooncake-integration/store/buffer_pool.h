#pragma once

#include <nanobind/nanobind.h>

namespace py = nanobind;

namespace mooncake {

void bind_buffer_pool(py::module_ &m);

}  // namespace mooncake
