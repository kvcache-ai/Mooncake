#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "engram/engram.h"
#include "engram/engram_config.h"
#include "pyclient.h"

namespace py = pybind11;
using namespace mooncake;
using namespace mooncake::engram;

namespace {

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

std::vector<std::vector<std::vector<int64_t>>> py_array_to_vec3d(
    const py::array_t<int64_t, py::array::c_style | py::array::forcecast>&
        arr) {
    auto req = arr.request();
    if (req.ndim != 3) {
        throw std::runtime_error("row_ids array must have shape [B, L, H]");
    }

    const auto B = static_cast<size_t>(req.shape[0]);
    const auto L = static_cast<size_t>(req.shape[1]);
    const auto H = static_cast<size_t>(req.shape[2]);
    auto* data = static_cast<const int64_t*>(req.ptr);

    std::vector<std::vector<std::vector<int64_t>>> result(
        B, std::vector<std::vector<int64_t>>(L, std::vector<int64_t>(H)));
    for (size_t b = 0; b < B; ++b) {
        for (size_t l = 0; l < L; ++l) {
            const size_t offset = (b * L + l) * H;
            std::copy_n(data + offset, H, result[b][l].begin());
        }
    }
    return result;
}

std::string py_type_name(const py::handle& obj) {
    return py::str(obj.get_type().attr("__name__"));
}

std::shared_ptr<PyClient> unwrap_store(py::object store_obj) {
    if (store_obj.is_none()) {
        return nullptr;
    }

    try {
        return store_obj.cast<std::shared_ptr<PyClient>>();
    } catch (const py::cast_error&) {
    }

    const std::string type_name = py_type_name(store_obj);
    if (type_name != "MooncakeDistributedStore") {
        throw std::runtime_error("Engram store parameter must be a PyClient or "
                                 "MooncakeDistributedStore. Got: " +
                                 type_name);
    }

    try {
        py::module store_mod = py::module::import("store");
        py::object helper_func = store_mod.attr("_get_pyclient_from_wrapper");
        py::object capsule = helper_func(store_obj);
        if (capsule.is_none()) {
            return nullptr;
        }

        auto* ptr = static_cast<std::shared_ptr<PyClient>*>(
            py::capsule(capsule).get_pointer());
        if (ptr == nullptr) {
            throw std::runtime_error(
                "store wrapper returned an empty PyClient capsule");
        }
        return *ptr;
    } catch (const py::error_already_set& e) {
        throw std::runtime_error(
            "Failed to unwrap MooncakeDistributedStore for Engram: " +
            std::string(e.what()));
    }
}

py::array_t<float> require_embedding_buffer(py::handle buf, int64_t expected_rows,
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
    Engram& self, const std::vector<std::vector<std::vector<int64_t>>>& row_ids) {
    if (row_ids.empty() || row_ids[0].empty()) {
        throw std::runtime_error("row_ids must not be empty");
    }

    const int B = static_cast<int>(row_ids.size());
    const int L = static_cast<int>(row_ids[0].size());
    const int H = self.get_num_heads();
    const int D = self.get_embedding_dim();
    py::array_t<float> output({B, L, H, D});
    auto out_buf = output.request();

    int ret = self.lookup_rows(row_ids, out_buf.ptr,
                               out_buf.size * sizeof(float));
    if (ret != 0) {
        throw std::runtime_error("Engram lookup failed");
    }
    return output;
}

}  // namespace

namespace mooncake {
namespace engram {

void bind_engram(py::module& m) {
    py::class_<EngramConfig>(m, "EngramConfig")
        .def(py::init<>())
        .def_readwrite("table_vocab_sizes", &EngramConfig::table_vocab_sizes)
        .def_readwrite("embedding_dim", &EngramConfig::embedding_dim);

    py::class_<Engram>(m, "Engram")
        .def("get_table_vocab_sizes", &Engram::get_table_vocab_sizes)
        .def("get_store_keys", &Engram::get_store_keys)
        .def("get_num_heads", &Engram::get_num_heads)
        .def("get_embedding_dim", &Engram::get_embedding_dim)
        .def(
            "remove_from_store",
            [](Engram& self, bool force) {
                int ret = self.remove_from_store(force);
                if (ret < 0) {
                    throw std::runtime_error("remove_from_store failed, rc=" +
                                             std::to_string(ret));
                }
                return ret;
            },
            py::arg("force") = false,
            "Remove all Mooncake Store tables owned by this Engram layer. "
            "Returns the number of removed head tables; missing keys are "
            "ignored.")
        .def(
            py::init([](int layer_id, const EngramConfig& cfg,
                        py::object store_obj) {
                std::shared_ptr<PyClient> store = unwrap_store(store_obj);
                return new Engram(layer_id, cfg, store);
            }),
            py::arg("layer_id"), py::arg("config"),
            py::arg("store") = py::none())
        .def(
            "lookup",
            [](Engram& self, py::object row_ids_obj) {
                if (py::isinstance<py::array>(row_ids_obj)) {
                    auto row_ids_array =
                        py::array_t<int64_t, py::array::c_style |
                                                 py::array::forcecast>::ensure(
                            row_ids_obj);
                    if (!row_ids_array) {
                        throw std::runtime_error(
                            "row_ids array must be convertible to int64");
                    }
                    return lookup_to_numpy(self,
                                           py_array_to_vec3d(row_ids_array));
                }
                return lookup_to_numpy(self, py_obj_to_vec3d(row_ids_obj));
            },
            py::arg("row_ids"),
            "Lookup embeddings by precomputed row IDs. Returns [B, L, H, D].")
        .def(
            "populate",
            [](Engram& self, py::list embedding_buffers) {
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
                    auto arr = require_embedding_buffer(embedding_buffers[i],
                                                        vocab_sizes[i],
                                                        embed_dim);
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
