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
        .def(
            py::init([](int layer_id, const EngramConfig& cfg,
                        const BackboneConfig& bb, py::object store_obj) {
                std::shared_ptr<PyClient> store = nullptr;
                if (!store_obj.is_none()) {
                    // Try to cast as PyClient first (direct C++ store)
                    try {
                        store = store_obj.cast<std::shared_ptr<PyClient>>();
                    } catch (py::cast_error&) {
                        // If that fails, try to extract from
                        // MooncakeDistributedStore wrapper
                        try {
                            // Get the type name to check if it's
                            // MooncakeDistributedStore
                            std::string type_name =
                                py::str(store_obj.get_type().attr("__name__"));
                            if (type_name == "MooncakeDistributedStore") {
                                // Use the helper function from store module to
                                // get PyClient shared_ptr
                                py::module store_mod =
                                    py::module::import("store");
                                py::object helper_func = store_mod.attr(
                                    "_get_pyclient_from_wrapper");
                                py::object capsule = helper_func(store_obj);
                                if (!capsule.is_none()) {
                                    // Extract shared_ptr from capsule
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
            "forward",
            [](Engram& self, py::array_t<float> hidden_states,
               py::object input_ids) {
                // Extract shape
                auto h_buf = hidden_states.request();
                if (h_buf.ndim != 4) {
                    throw std::runtime_error(
                        "hidden_states must be 4D [B, L, hc_mult, D]");
                }
                int B = static_cast<int>(h_buf.shape[0]);
                int L = static_cast<int>(h_buf.shape[1]);
                int hc_mult = static_cast<int>(h_buf.shape[2]);
                int D = static_cast<int>(h_buf.shape[3]);

                // Convert input_ids
                std::vector<std::vector<int64_t>> input_ids_vec =
                    py_list_to_vec2d(input_ids);

                // Allocate output
                py::array_t<float> output =
                    py::array_t<float>({B, L, hc_mult, D});
                auto out_buf = output.request();

                // Call forward
                int ret = self.forward(h_buf.ptr, h_buf.size * sizeof(float),
                                       input_ids_vec, out_buf.ptr,
                                       out_buf.size * sizeof(float));
                if (ret != 0) {
                    throw std::runtime_error("Engram forward failed");
                }
                return output;
            },
            py::arg("hidden_states"), py::arg("input_ids"))
        .def(
            "populate_store_from_buffers",
            [](Engram& self, py::list embedding_buffers) {
                std::vector<void*> bufs;
                std::vector<size_t> sizes;
                for (auto buf : embedding_buffers) {
                    if (py::isinstance<py::array>(buf)) {
                        auto arr = buf.cast<py::array_t<float>>();
                        auto req = arr.request();
                        bufs.push_back(req.ptr);
                        sizes.push_back(req.size * sizeof(float));
                    } else {
                        throw std::runtime_error(
                            "embedding_buffers must be numpy arrays");
                    }
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
