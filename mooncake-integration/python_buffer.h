#pragma once

#include <nanobind/nanobind.h>

#include <memory>

namespace mooncake {

class PythonBuffer {
   public:
    explicit PythonBuffer(nanobind::handle buffer,
                          int flags = PyBUF_STRIDES | PyBUF_FORMAT)
        : view_(new Py_buffer{}) {
        if (PyObject_GetBuffer(buffer.ptr(), view_.get(), flags) < 0) {
            throw nanobind::python_error();
        }
    }

    const Py_buffer &view() const { return *view_; }

   private:
    struct ReleaseView {
        void operator()(Py_buffer *view) const {
            if (view->obj) PyBuffer_Release(view);
            delete view;
        }
    };

    std::unique_ptr<Py_buffer, ReleaseView> view_;
};

inline int fill_byte_buffer(PyObject *owner, Py_buffer *view, void *buffer,
                            size_t size, int flags) {
    if (size > PY_SSIZE_T_MAX) {
        PyErr_SetString(PyExc_OverflowError, "buffer size exceeds Py_ssize_t");
        return -1;
    }
    return PyBuffer_FillInfo(view, owner, buffer, static_cast<Py_ssize_t>(size),
                             0, flags);
}

}  // namespace mooncake
