#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <cstring>

#include "engram/engram_store.h"
#include "engram/engram_store_config.h"
#include "pyclient.h"

namespace py = pybind11;
using namespace mooncake;
using namespace mooncake::engram;

namespace {

constexpr char kPyClientCapsuleName[] = "mooncake.PyClient.shared_ptr";
constexpr char kPyClientCapsuleMethod[] = "_get_pyclient_capsule";

std::vector<std::vector<std::vector<int64_t>>> py_obj_to_vec3d(py::object obj) {
    std::vector<std::vector<std::vector<int64_t>>> result;
    for (auto batch_item : obj) {
        std::vector<std::vector<int64_t>> batch_vec;
        for (auto token_item : batch_item) {
            std::vector<int64_t> token_vec;
            for (auto head_item : token_item) {
                token_vec.push_back(head_item.cast<int64_t>());
            }
            batch_vec.push_back(std::move(token_vec));
        }
        result.push_back(std::move(batch_vec));
    }
    return result;
}

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

py::array_t<float> require_embedding_buffer(py::handle buf,
                                            int64_t expected_rows,
                                            int expected_cols) {
    if (!py::isinstance<py::array>(buf)) {
        throw std::runtime_error("embedding_buffers must be NumPy arrays");
    }

    auto arr =
        py::array_t<float, py::array::c_style | py::array::forcecast>::ensure(
            buf);
    if (!arr) {
        throw std::runtime_error("embedding_buffers must be float32 arrays");
    }

    auto req = arr.request();
    if (req.ndim != 2) {
        throw std::runtime_error("each embedding buffer must be 2D [N_h, D]");
    }
    if (req.shape[0] != expected_rows || req.shape[1] != expected_cols) {
        throw std::runtime_error(
            "embedding buffer shape does not match "
            "get_table_vocab_sizes()/get_embedding_dim()");
    }
    return arr;
}

py::array_t<float> lookup_to_numpy(
    EngramStore& self,
    const std::vector<std::vector<std::vector<int64_t>>>& row_ids) {
    if (row_ids.empty() || row_ids[0].empty()) {
        throw std::runtime_error("row_ids must not be empty");
    }

    const int B = static_cast<int>(row_ids.size());
    const int L = static_cast<int>(row_ids[0].size());
    const int H = self.get_num_heads();
    const int D = self.get_embedding_dim();
    py::array_t<float> output({B, L, H, D});
    auto out_buf = output.request();

    int ret =
        self.lookup_rows(row_ids, out_buf.ptr, out_buf.size * sizeof(float));
    if (ret != 0) {
        throw std::runtime_error("EngramStore lookup failed");
    }
    return output;
}

py::array_t<float> lookup_array_to_numpy(
    EngramStore& self,
    const py::array_t<int64_t, py::array::c_style | py::array::forcecast>&
        row_ids) {
    auto req = row_ids.request();
    if (req.ndim != 3) {
        throw std::runtime_error("row_ids array must have shape [B, L, H]");
    }

    const int B = static_cast<int>(req.shape[0]);
    const int L = static_cast<int>(req.shape[1]);
    const int H = static_cast<int>(req.shape[2]);
    if (H != self.get_num_heads()) {
        throw std::runtime_error("row_ids last dimension must match num_heads");
    }

    const int D = self.get_embedding_dim();
    py::array_t<float> output({B, L, H, D});
    auto out_buf = output.request();

    int ret =
        self.lookup_rows_contiguous(static_cast<const int64_t*>(req.ptr), B, L,
                                    out_buf.ptr, out_buf.size * sizeof(float));
    if (ret != 0) {
        throw std::runtime_error("EngramStore lookup failed");
    }
    return output;
}

}  // namespace

namespace mooncake {
namespace engram {

void bind_engram_store(py::module& m) {
    py::class_<EngramStoreConfig>(m, "EngramStoreConfig")
        .def(py::init<>())
        .def_readwrite("table_vocab_sizes",
                       &EngramStoreConfig::table_vocab_sizes)
        .def_readwrite("embedding_dim", &EngramStoreConfig::embedding_dim);

    py::class_<EngramStore>(m, "EngramStore")
        .def("get_table_vocab_sizes", &EngramStore::get_table_vocab_sizes)
        .def("get_store_keys", &EngramStore::get_store_keys)
        .def("get_num_heads", &EngramStore::get_num_heads)
        .def("get_embedding_dim", &EngramStore::get_embedding_dim)
        .def(
            "remove_from_store",
            [](EngramStore& self, bool force) {
                int ret = self.remove_from_store(force);
                if (ret < 0) {
                    throw std::runtime_error("remove_from_store failed, rc=" +
                                             std::to_string(ret));
                }
                return ret;
            },
            py::arg("force") = false,
            "Remove all Mooncake Store tables owned by this EngramStore layer. "
            "Returns the number of removed head tables; missing keys are "
            "ignored.")
        .def(py::init([](int layer_id, const EngramStoreConfig& cfg,
                         py::object store_obj) {
                 std::shared_ptr<PyClient> store = unwrap_store(store_obj);
                 return new EngramStore(layer_id, cfg, store);
             }),
             py::arg("layer_id"), py::arg("config"),
             py::arg("store") = py::none())
        .def(
            "lookup",
            [](EngramStore& self, py::object row_ids_obj) {
                if (py::isinstance<py::array>(row_ids_obj)) {
                    auto row_ids_array = py::array_t<
                        int64_t, py::array::c_style |
                                     py::array::forcecast>::ensure(row_ids_obj);
                    if (!row_ids_array) {
                        throw std::runtime_error(
                            "row_ids array must be convertible to int64");
                    }
                    return lookup_array_to_numpy(self, row_ids_array);
                }
                return lookup_to_numpy(self, py_obj_to_vec3d(row_ids_obj));
            },
            py::arg("row_ids"),
            "Lookup embeddings by precomputed row IDs. Returns [B, L, H, D].")
        .def(
            "populate",
            [](EngramStore& self, py::list embedding_buffers) {
                const std::vector<int64_t> vocab_sizes =
                    self.get_table_vocab_sizes();
                const int embed_dim = self.get_embedding_dim();
                if (static_cast<size_t>(py::len(embedding_buffers)) !=
                    vocab_sizes.size()) {
                    throw std::runtime_error(
                        "embedding_buffers size must match num_heads");
                }

                std::vector<py::array_t<float>> arrays;
                std::vector<void*> bufs;
                std::vector<size_t> sizes;
                arrays.reserve(vocab_sizes.size());
                bufs.reserve(vocab_sizes.size());
                sizes.reserve(vocab_sizes.size());
                for (size_t i = 0; i < vocab_sizes.size(); ++i) {
                    auto arr = require_embedding_buffer(
                        embedding_buffers[i], vocab_sizes[i], embed_dim);
                    auto req = arr.request();
                    arrays.push_back(arr);
                    bufs.push_back(req.ptr);
                    sizes.push_back(req.size * sizeof(float));
                }
                int ret = self.populate(bufs, sizes);
                if (ret != 0) {
                    throw std::runtime_error("populate failed");
                }
            },
            py::arg("embedding_buffers"));
}

}  // namespace engram
}  // namespace mooncake
