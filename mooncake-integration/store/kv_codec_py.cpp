#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "codec/kv_codec.h"

namespace py = pybind11;
using namespace mooncake::codec;

namespace {
py::buffer_info ByteBuffer(py::buffer buffer, bool writable) {
    auto info = buffer.request(writable);
    if (info.ndim != 1 || info.itemsize != 1 || info.strides[0] != 1)
        throw std::invalid_argument(
            "Expected a contiguous one-dimensional byte buffer");
    return info;
}
}  // namespace

PYBIND11_MODULE(_kv_codec, module) {
    py::register_exception<InvalidRecord>(module, "InvalidRecord",
                                          PyExc_ValueError);
    py::enum_<Dtype>(module, "Dtype")
        .value("FLOAT16", Dtype::Float16)
        .value("BFLOAT16", Dtype::BFloat16);
    py::class_<ScaledFp8Codec>(module, "ScaledFp8Codec")
        .def(py::init<uint32_t>(), py::arg("group_size") = 128)
        .def_property_readonly("format_id", &ScaledFp8Codec::FormatId)
        .def("encoded_size",
             [](const ScaledFp8Codec& codec, Dtype dtype,
                const std::vector<uint64_t>& shape) {
                 return codec.EncodedSize({dtype, shape});
             })
        .def("encode",
             [](const ScaledFp8Codec& codec, py::buffer source, Dtype dtype,
                const std::vector<uint64_t>& shape) {
                 const TensorDesc desc{dtype, shape};
                 const auto input = ByteBuffer(source, false);
                 const size_t size = codec.EncodedSize(desc);
                 // EncodedSize validates shape overflow. Reject mismatched
                 // input before allocating a potentially large record.
                 size_t elements = 1;
                 for (auto dim : shape) elements *= dim;
                 if (size_t(input.size) != elements * 2)
                     throw std::invalid_argument("Invalid source size");
                 std::string output(size, '\0');
                 {
                     py::gil_scoped_release release;
                     codec.Encode(desc,
                                  {static_cast<const uint8_t*>(input.ptr),
                                   size_t(input.size)},
                                  {reinterpret_cast<uint8_t*>(output.data()),
                                   output.size()});
                 }
                 return py::bytes(output);
             })
        .def("decode_into", [](const ScaledFp8Codec& codec, py::bytes record,
                               py::buffer destination, Dtype dtype,
                               const std::vector<uint64_t>& shape) {
            const auto output = ByteBuffer(destination, true);
            // Keep immutable Python bytes and the exported destination alive
            // throughout the synchronous call, including while GIL is released.
            const auto input = record.cast<std::string_view>();
            py::gil_scoped_release release;
            codec.Decode(
                {reinterpret_cast<const uint8_t*>(input.data()), input.size()},
                {dtype, shape},
                {static_cast<uint8_t*>(output.ptr), size_t(output.size)});
        });
}
