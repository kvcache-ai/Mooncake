#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <cstring>
#include <limits>

#include "engram/engram_store.h"
#include "engram/engram_store_config.h"
#include "pyclient.h"

namespace py = pybind11;
using namespace mooncake;
using namespace mooncake::engram;

namespace {

constexpr char kPyClientCapsuleName[] = "mooncake.PyClient.shared_ptr";
constexpr char kPyClientCapsuleMethod[] = "_get_pyclient_capsule";

std::shared_ptr<PyClient> unwrap_pyclient_capsule(py::object capsule) {
    if (capsule.is_none()) {
        return nullptr;
    }

    if (!PyCapsule_CheckExact(capsule.ptr())) {
        throw std::runtime_error(
            "store wrapper returned a non-capsule PyClient handle");
    }

    py::capsule py_client_capsule(capsule);
    const char* capsule_name = py_client_capsule.name();
    if (capsule_name == nullptr ||
        std::strcmp(capsule_name, kPyClientCapsuleName) != 0) {
        throw std::runtime_error(
            "store wrapper returned an unexpected PyClient capsule type");
    }

    auto* ptr = static_cast<std::shared_ptr<PyClient>*>(
        py_client_capsule.get_pointer());
    if (ptr == nullptr) {
        throw std::runtime_error(
            "store wrapper returned an empty PyClient capsule");
    }
    return *ptr;
}

std::shared_ptr<PyClient> unwrap_store(py::object store_obj) {
    if (store_obj.is_none()) {
        return nullptr;
    }

    try {
        return store_obj.cast<std::shared_ptr<PyClient>>();
    } catch (const py::cast_error&) {
    }

    if (!py::hasattr(store_obj, kPyClientCapsuleMethod)) {
        throw std::runtime_error(
            "EngramStore store parameter must be a PyClient or store wrapper "
            "that "
            "implements _get_pyclient_capsule()");
    }

    try {
        py::object capsule = store_obj.attr(kPyClientCapsuleMethod)();
        return unwrap_pyclient_capsule(capsule);
    } catch (const py::error_already_set& e) {
        throw std::runtime_error(
            "Failed to unwrap store wrapper for EngramStore: " +
            std::string(e.what()));
    }
}

py::array require_embedding_buffer(py::handle buf, int64_t rows,
                                   int row_bytes) {
    if (!py::isinstance<py::array>(buf)) {
        throw std::runtime_error("embedding_buffers must be NumPy arrays");
    }
    auto arr = py::reinterpret_borrow<py::array>(buf);
    if (!arr.dtype().is(py::dtype::of<uint8_t>()) ||
        !(arr.flags() & py::array::c_style)) {
        throw std::runtime_error("rows must be contiguous uint8 arrays");
    }
    if (arr.ndim() != 2 || arr.shape(0) != rows || arr.shape(1) != row_bytes) {
        throw std::runtime_error(
            "embedding buffer must match per-head table shape");
    }
    return arr;
}

}  // namespace

namespace mooncake {
namespace engram {

void bind_engram_store(py::module& m) {
    py::class_<EngramStoreConfig>(m, "EngramStoreConfig")
        .def(py::init<>())
        .def_readwrite("table_vocab_sizes",
                       &EngramStoreConfig::table_vocab_sizes)
        .def_readwrite("row_bytes", &EngramStoreConfig::row_bytes);

    py::class_<EngramStore>(m, "EngramStore")
        .def(
            "bind_local",
            [](EngramStore& self, int layer_id, py::list buffers) {
                const auto rows = self.get_table_vocab_sizes(layer_id);
                if (py::len(buffers) != rows.size())
                    throw std::runtime_error(
                        "Local table count must match heads");
                std::vector<const void*> pointers;
                std::vector<size_t> sizes;
                for (size_t h = 0; h < rows.size(); ++h) {
                    auto arr = require_embedding_buffer(
                        buffers[h], rows[h], self.get_row_bytes(layer_id));
                    pointers.push_back(arr.data());
                    sizes.push_back(arr.nbytes());
                }
                if (self.bind_local(layer_id, pointers, sizes) != 0)
                    throw std::runtime_error(
                        "bind_local requires an unbound layer and no Store "
                        "client");
            },
            py::arg("layer_id"), py::arg("embedding_buffers"),
            py::keep_alive<1, 3>(),
            "Bind immutable uint8 tables before lookup. Retains the arrays "
            "without "
            "copying; do not modify, resize or unmap them while bound.")
        .def("get_layer_ids", &EngramStore::get_layer_ids)
        .def("get_table_vocab_sizes", &EngramStore::get_table_vocab_sizes)
        .def("get_store_keys", &EngramStore::get_store_keys)
        .def("get_num_heads", &EngramStore::get_num_heads)
        .def("get_row_bytes", &EngramStore::get_row_bytes)
        .def(
            "remove_from_store",
            [](EngramStore& self, int layer_id, bool force) {
                int ret = self.remove_from_store(layer_id, force);
                if (ret < 0) {
                    throw std::runtime_error("remove_from_store failed, rc=" +
                                             std::to_string(ret));
                }
                return ret;
            },
            py::arg("layer_id"), py::arg("force") = false,
            "Remove all Mooncake Store tables for the selected layer. "
            "Returns the number of removed head tables; missing keys are "
            "ignored.")
        .def(py::init([](const std::map<int, EngramStoreConfig>& layers,
                         py::object store_obj) {
                 std::shared_ptr<PyClient> store = unwrap_store(store_obj);
                 return new EngramStore(layers, store);
             }),
             py::arg("layers"), py::arg("store") = py::none())
        .def(
            "lookup_into",
            [](EngramStore& self, int layer_id,
               py::array_t<int64_t, py::array::c_style> ids, py::array output) {
                const int width = self.get_row_bytes(layer_id);
                if (ids.ndim() != 3 ||
                    ids.shape(2) != self.get_num_heads(layer_id) ||
                    ids.shape(0) > std::numeric_limits<int>::max() ||
                    ids.shape(1) > std::numeric_limits<int>::max() ||
                    output.ndim() != 4 || output.shape(0) != ids.shape(0) ||
                    output.shape(1) != ids.shape(1) ||
                    output.shape(2) != ids.shape(2) ||
                    output.shape(3) != width || !output.writeable() ||
                    !(output.flags() & py::array::c_style) ||
                    !output.dtype().is(py::dtype::of<uint8_t>())) {
                    throw std::runtime_error(
                        "lookup_into requires contiguous IDs [B,L,H] and "
                        "matching writable uint8 output [B,L,H,row_bytes]");
                }
                if (ids.size() == 0) return;
                auto ids_buf = ids.request();
                auto out_buf = output.request();
                const int B = static_cast<int>(ids_buf.shape[0]);
                const int L = static_cast<int>(ids_buf.shape[1]);
                int ret;
                {
                    py::gil_scoped_release release;
                    ret = self.lookup_into(
                        layer_id, static_cast<const int64_t*>(ids_buf.ptr), B,
                        L, out_buf.ptr, out_buf.size * out_buf.itemsize);
                }
                if (ret != 0)
                    throw std::runtime_error("EngramStore lookup_into failed");
            },
            py::arg("layer_id"), py::arg("row_ids").noconvert(),
            py::arg("output").noconvert(),
            "Read into caller-owned uint8 memory. The caller must keep the "
            "output registered with this Store for Store-backed reads. "
            "Local bound tables do not require output registration. "
            "This method does not allocate, register, or unregister output.")
        .def(
            "populate",
            [](EngramStore& self, int layer_id, py::list embedding_buffers,
               const ReplicateConfig& config) {
                const std::vector<int64_t> vocab_sizes =
                    self.get_table_vocab_sizes(layer_id);
                const int row_bytes = self.get_row_bytes(layer_id);
                if (static_cast<size_t>(py::len(embedding_buffers)) !=
                    vocab_sizes.size()) {
                    throw std::runtime_error(
                        "embedding_buffers size must match num_heads");
                }

                std::vector<py::array> arrays;
                std::vector<void*> bufs;
                std::vector<size_t> sizes;
                arrays.reserve(vocab_sizes.size());
                bufs.reserve(vocab_sizes.size());
                sizes.reserve(vocab_sizes.size());
                for (size_t i = 0; i < vocab_sizes.size(); ++i) {
                    auto arr = require_embedding_buffer(
                        embedding_buffers[i], vocab_sizes[i], row_bytes);
                    auto req = arr.request();
                    arrays.push_back(arr);
                    bufs.push_back(req.ptr);
                    sizes.push_back(arr.nbytes());
                }
                py::gil_scoped_release release;
                int ret = self.populate(layer_id, bufs, sizes, config);
                if (ret != 0) {
                    throw std::runtime_error("populate failed");
                }
            },
            py::arg("layer_id"), py::arg("embedding_buffers"),
            py::arg("config") = ReplicateConfig{});
}

}  // namespace engram
}  // namespace mooncake
