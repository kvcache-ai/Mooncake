#include <pybind11/pybind11.h>

#include <iostream>
#include <string>
#include <string_view>

namespace py = pybind11;

class caller {
 public:
  caller(py::function func) : callback_(func) {}

  void hello(py::buffer buf) {
    py::buffer_info info = buf.request(true);
    char* data = static_cast<char*>(info.ptr);
    if (info.size > 0) {
      data[0] = 'Z';  // modify buf
      data[1] = 'Y';
    }

    auto view =
        py::memoryview::from_buffer(data, {info.size}, {sizeof(uint8_t)});

    py::gil_scoped_acquire acquire;
    callback_(view);
  }

 private:
  py::function callback_;
};

PYBIND11_MODULE(zero_copy, m) {
  py::class_<caller>(m, "caller")
      .def(py::init<py::function>())
      .def("hello", &caller::hello);
}