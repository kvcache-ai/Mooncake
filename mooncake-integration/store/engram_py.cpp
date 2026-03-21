#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "engram/engram.h"
#include "engram/engram_config.h"
#include "pyclient.h"

namespace py = pybind11;
using namespace mooncake;
using namespace mooncake::engram;

/**
 * Pybind bindings for Engram C++ module.
 *
 * Provides Python interface to Engram with Mooncake Store backend.
 */

// Helper: convert Python list of lists to C++ vector of vectors
std::vector<std::vector<int64_t>> py_list_to_vec2d(py::object obj) {
    std::vector<std::vector<int64_t>> result;
    for (auto row : obj) {
        std::vector<int64_t> vec;
        for (auto item : row) {
            vec.push_back(item.cast<int64_t>());
        }
        result.push_back(vec);
    }
    return result;
}

namespace mooncake {
namespace engram {

void bind_engram(py::module& m) {
    py::class_<EngramConfig>(m, "EngramConfig")
        .def(py::init<>())
        .def_readwrite("tokenizer_name_or_path",
                       &EngramConfig::tokenizer_name_or_path)
        .def_readwrite("engram_vocab_size", &EngramConfig::engram_vocab_size)
        .def_readwrite("max_ngram_size", &EngramConfig::max_ngram_size)
        .def_readwrite("n_embed_per_ngram", &EngramConfig::n_embed_per_ngram)
        .def_readwrite("n_head_per_ngram", &EngramConfig::n_head_per_ngram)
        .def_readwrite("layer_ids", &EngramConfig::layer_ids)
        .def_readwrite("pad_id", &EngramConfig::pad_id)
        .def_readwrite("seed", &EngramConfig::seed)
        .def_readwrite("kernel_size", &EngramConfig::kernel_size);

    py::class_<BackboneConfig>(m, "BackboneConfig")
        .def(py::init<>())
        .def_readwrite("hidden_size", &BackboneConfig::hidden_size)
        .def_readwrite("hc_mult", &BackboneConfig::hc_mult)
        .def_readwrite("vocab_size", &BackboneConfig::vocab_size)
        .def_readwrite("num_layers", &BackboneConfig::num_layers);

    py::class_<Engram>(m, "Engram")
        .def("get_embedding_tables_workspace_size",
             &Engram::get_embedding_tables_workspace_size)
        .def("get_query_workspace_size", &Engram::get_query_workspace_size,
             py::arg("B"), py::arg("L"))
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
                        const BackboneConfig& bb, py::object store_obj) {
                std::shared_ptr<PyClient> store = nullptr;
                if (!store_obj.is_none()) {
                    try {
                        store = store_obj.cast<std::shared_ptr<PyClient>>();
                    } catch (py::cast_error&) {
                        try {
                            std::string type_name =
                                py::str(store_obj.get_type().attr("__name__"));
                            if (type_name == "MooncakeDistributedStore") {
                                py::module store_mod =
                                    py::module::import("store");
                                py::object helper_func = store_mod.attr(
                                    "_get_pyclient_from_wrapper");
                                py::object capsule = helper_func(store_obj);
                                if (!capsule.is_none()) {
                                    std::shared_ptr<PyClient>* ptr =
                                        static_cast<std::shared_ptr<PyClient>*>(
                                            py::capsule(capsule).get_pointer());
                                    if (ptr) {
                                        store = *ptr;
                                    }
                                }
                            } else {
                                throw std::runtime_error(
                                    "Unknown store type: " + type_name);
                            }
                        } catch (py::error_already_set& e) {
                            py::gil_scoped_acquire acquire;
                            throw std::runtime_error(
                                "Engram store parameter must be a PyClient or "
                                "MooncakeDistributedStore. "
                                "Got: " +
                                std::string(py::str(
                                    store_obj.get_type().attr("__name__"))) +
                                ". Error: " + std::string(e.what()));
                        }
                    }
                }
                return new Engram(layer_id, cfg, bb, store);
            }),
            py::arg("layer_id"), py::arg("config"), py::arg("backbone_cfg"),
            py::arg("store") = py::none())
        .def(
            "hash_input_ids",
            [](Engram& self, py::object input_ids) {
                std::vector<std::vector<int64_t>> input_ids_vec =
                    py_list_to_vec2d(input_ids);
                auto hash_ids = self.hash_input_ids(input_ids_vec);
                const ssize_t B = static_cast<ssize_t>(hash_ids.size());
                const ssize_t L =
                    B == 0 ? 0 : static_cast<ssize_t>(hash_ids[0].size());
                const ssize_t H =
                    (B == 0 || L == 0)
                        ? 0
                        : static_cast<ssize_t>(hash_ids[0][0].size());
                py::array_t<int64_t> output({B, L, H});
                auto out = output.mutable_unchecked<3>();
                for (ssize_t b = 0; b < B; ++b) {
                    for (ssize_t l = 0; l < L; ++l) {
                        for (ssize_t h = 0; h < H; ++h) {
                            out(b, l, h) = hash_ids[b][l][h];
                        }
                    }
                }
                return output;
            },
            py::arg("input_ids"),
            "Hash token IDs into per-head embedding row indices.")
        .def(
            "query",
            [](Engram& self, py::object input_ids, py::object workspace) {
                std::vector<std::vector<int64_t>> input_ids_vec =
                    py_list_to_vec2d(input_ids);
                if (input_ids_vec.empty()) {
                    throw std::runtime_error("input_ids must not be empty");
                }

                int B = static_cast<int>(input_ids_vec.size());
                int L = static_cast<int>(input_ids_vec[0].size());
                int H = self.get_num_heads();
                int D = self.get_embedding_dim();

                py::array_t<float> output = py::array_t<float>({B, L, H, D});
                auto out_buf = output.request();

                void* ws_ptr = nullptr;
                size_t ws_size = 0;
                if (!workspace.is_none()) {
                    auto ws_arr = py::array::ensure(workspace);
                    if (!ws_arr) {
                        throw std::runtime_error(
                            "workspace must be a NumPy array when provided");
                    }
                    auto ws_req = ws_arr.request();
                    if (ws_req.ptr && ws_req.size > 0) {
                        ws_ptr = ws_req.ptr;
                        ws_size =
                            static_cast<size_t>(ws_req.size) * ws_req.itemsize;
                    }
                }

                int ret = self.query_embeddings(input_ids_vec, out_buf.ptr,
                                                out_buf.size * sizeof(float),
                                                ws_ptr, ws_size);
                if (ret != 0) {
                    throw std::runtime_error("Engram query failed");
                }
                return output;
            },
            py::arg("input_ids"), py::arg("workspace") = py::none(),
            "Query embeddings from Mooncake Store. Returns [B, L, num_heads, "
            "embed_D].")
        .def(
            "populate_store_from_buffers",
            [](Engram& self, py::list embedding_buffers) {
                std::vector<int64_t> vocab_sizes = self.get_table_vocab_sizes();
                int embed_dim = self.get_embedding_dim();
                if (static_cast<size_t>(py::len(embedding_buffers)) !=
                    vocab_sizes.size()) {
                    throw std::runtime_error(
                        "embedding_buffers size must match num_heads");
                }

                std::vector<void*> bufs;
                std::vector<size_t> sizes;
                bufs.reserve(vocab_sizes.size());
                sizes.reserve(vocab_sizes.size());
                for (size_t i = 0; i < vocab_sizes.size(); ++i) {
                    py::handle buf = embedding_buffers[i];
                    if (!py::isinstance<py::array>(buf)) {
                        throw std::runtime_error(
                            "embedding_buffers must be NumPy arrays");
                    }
                    auto arr =
                        py::array_t<float,
                                    py::array::c_style |
                                        py::array::forcecast>::ensure(buf);
                    if (!arr) {
                        throw std::runtime_error(
                            "embedding_buffers must be float32 arrays");
                    }
                    auto req = arr.request();
                    if (req.ndim != 2) {
                        throw std::runtime_error(
                            "each embedding buffer must be 2D [N_h, D]");
                    }
                    if (req.shape[0] != vocab_sizes[i] ||
                        req.shape[1] != embed_dim) {
                        throw std::runtime_error(
                            "embedding buffer shape does not match "
                            "get_table_vocab_sizes()/get_embedding_dim()");
                    }
                    bufs.push_back(req.ptr);
                    sizes.push_back(req.size * sizeof(float));
                }
                int ret = self.populate_store_from_buffers(bufs, sizes);
                if (ret != 0) {
                    throw std::runtime_error(
                        "populate_store_from_buffers failed");
                }
            },
            py::arg("embedding_buffers"));
}

}  // namespace engram
}  // namespace mooncake
