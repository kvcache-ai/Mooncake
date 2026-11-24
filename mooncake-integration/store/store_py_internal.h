#pragma once

#include <glog/logging.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "integration_utils.h"

namespace py = nanobind;

namespace mooncake {

struct PyTensorInfo {
    uintptr_t data_ptr;
    size_t tensor_size;
    TensorMetadata metadata;
    py::object owner;

    bool valid() const {
        // Basic size check
        if (data_ptr == 0 && tensor_size != 0) {
            return false;
        }

        // Validate metadata
        TensorMetadata validated = metadata;
        validated.header.data_bytes = tensor_size;

        // Check dtype is within valid range (0 to TensorDtype::NR_DTYPES,
        // excluding UNKNOWN=-1)
        if (validated.header.dtype >=
            static_cast<int32_t>(TensorDtype::NR_DTYPES)) {
            return false;
        }

        // Check ndim is within valid range (0 to shape array size)
        const int kMaxDims = kMaxTensorDims;
        if (validated.header.ndim < 0 || validated.header.ndim > kMaxDims) {
            return false;
        }

        // Validate shape array
        // For valid dimensions (0 to ndim-1), shape should be >= 0
        auto shape = TensorShapeToVector(validated.layout.local_shape,
                                         validated.header.ndim);
        for (int i = 0; i < validated.header.ndim; ++i) {
            if (shape[i] < 0) {
                return false;  // Invalid dimension size
            }
        }

        return ValidateTensorMetadata(
            validated, validated.header.data_offset + tensor_size);
    }
};

inline std::vector<int64_t> tensor_shape_to_vector(const py::handle &tensor) {
    py::tuple shape_tuple = py::cast<py::tuple>(tensor.attr("shape"));
    std::vector<int64_t> shape;
    shape.reserve(shape_tuple.size());
    for (const auto &dim : shape_tuple) {
        shape.push_back(py::cast<int64_t>(dim));
    }
    return shape;
}

inline TensorMetadata build_full_tensor_metadata(const py::handle &tensor,
                                                 TensorDtype dtype_enum,
                                                 size_t tensor_size) {
    auto shape = tensor_shape_to_vector(tensor);
    TensorMetadata metadata = BuildTensorMetadata(
        static_cast<int32_t>(dtype_enum), shape, shape, TensorLayoutKind::FULL);
    metadata.header.data_bytes = tensor_size;
    return metadata;
}

inline TensorMetadata build_tp_shard_metadata(const py::handle &full_tensor,
                                              const py::handle &shard_tensor,
                                              TensorDtype dtype_enum,
                                              int tp_rank, int tp_size,
                                              int split_dim,
                                              size_t tensor_size) {
    auto global_shape = tensor_shape_to_vector(full_tensor);
    auto local_shape = tensor_shape_to_vector(shard_tensor);
    if (global_shape.size() != local_shape.size() || split_dim < 0 ||
        split_dim >= static_cast<int>(local_shape.size())) {
        return TensorMetadata{};
    }

    LayoutAxis axis{};
    axis.kind = static_cast<int32_t>(LayoutAxisKind::TP);
    axis.axis_index = 0;
    axis.shard_rank = tp_rank;
    axis.shard_count = tp_size;
    axis.split_dim = split_dim;
    axis.reserved0 = 0;
    axis.reserved1 = local_shape[split_dim] >= 0 ? local_shape[split_dim] : 0;

    TensorMetadata metadata = BuildTensorMetadata(
        static_cast<int32_t>(dtype_enum), global_shape, local_shape,
        TensorLayoutKind::SHARD, std::span<const LayoutAxis>(&axis, 1));
    metadata.header.data_bytes = tensor_size;
    return metadata;
}

inline py::object materialize_contiguous_tensor(const py::handle &tensor) {
    py::object contiguous = tensor.attr("contiguous")();
#ifdef USE_MLU
    if (!contiguous.is(tensor) &&
        py::cast<std::string>(contiguous.attr("device").attr("type")) ==
            "mlu") {
        // CNRT copies do not wait for work on PyTorch's current MLU stream.
        // The caller can synchronize its input, but this new copy is ours.
        // Wait before exposing its pointer to Store, including for TP shards.
        py::module_::import_("torch")
            .attr("mlu")
            .attr("current_stream")(contiguous.attr("device"))
            .attr("synchronize")();
    }
#endif
    return contiguous;
}

inline PyTensorInfo extract_tensor_info_impl(const py::object &tensor,
                                             const std::string &key_name,
                                             bool require_contiguous,
                                             const char *role) {
    PyTensorInfo info = {0, 0, {}, py::none()};
    if (py::cast<std::string>(tensor.attr("__class__").attr("__name__"))
            .find("Tensor") == std::string::npos) {
        LOG(ERROR) << role << " " << (key_name.empty() ? "" : "for " + key_name)
                   << " is not a PyTorch tensor";
        return info;
    }

    try {
        if (require_contiguous &&
            !py::cast<bool>(tensor.attr("is_contiguous")())) {
            LOG(ERROR) << role << " "
                       << (key_name.empty() ? "" : "for " + key_name)
                       << " must be contiguous";
            return info;
        }

        py::object inspected =
            require_contiguous ? tensor : materialize_contiguous_tensor(tensor);
        info.owner = inspected;
        info.data_ptr = py::cast<uintptr_t>(inspected.attr("data_ptr")());
        size_t numel = py::cast<size_t>(inspected.attr("numel")());
        size_t element_size =
            py::cast<size_t>(inspected.attr("element_size")());
        info.tensor_size = numel * element_size;

        TensorDtype dtype_enum = get_tensor_dtype(inspected.attr("dtype"));
        if (dtype_enum == TensorDtype::UNKNOWN) {
            LOG(ERROR) << "Unsupported tensor dtype"
                       << (key_name.empty() ? "" : " for " + key_name);
            return {0, 0, {}, py::none()};
        }
        int32_t ndim =
            static_cast<int32_t>(py::len(py::object(inspected.attr("shape"))));
        if (ndim > static_cast<int32_t>(kMaxTensorDims)) {
            LOG(ERROR) << "Tensor has more than " << kMaxTensorDims
                       << " dimensions: " << ndim;
            return {0, 0, {}, py::none()};
        }
        info.metadata =
            build_full_tensor_metadata(inspected, dtype_enum, info.tensor_size);
    } catch (const std::exception &e) {
        LOG(ERROR) << "Error extracting tensor info: " << e.what();
        return {0, 0, {}, py::none()};
    }

    return info;
}

inline PyTensorInfo extract_tensor_info(const py::object &tensor,
                                        const std::string &key_name = "") {
    return extract_tensor_info_impl(tensor, key_name, false, "Input");
}

inline PyTensorInfo extract_tensor_destination_info(
    const py::object &tensor, const std::string &key_name = "") {
    return extract_tensor_info_impl(tensor, key_name, true,
                                    "Destination tensor");
}

inline std::pair<int64_t, int64_t> calculate_shard_range(int64_t dim_size,
                                                         int rank,
                                                         int shard_count) {
    if (dim_size < 0 || shard_count <= 0 || rank < 0 || rank >= shard_count) {
        return {0, 0};
    }

    const int64_t start = (dim_size * static_cast<int64_t>(rank)) / shard_count;
    const int64_t end =
        (dim_size * static_cast<int64_t>(rank + 1)) / shard_count;
    return {start, end - start};
}

inline bool is_uniform_shardable_dim(int64_t dim_size, int shard_count) {
    return dim_size >= 0 && shard_count > 0 && dim_size % shard_count == 0;
}

inline bool validate_uniform_shard_request(const std::vector<int64_t> &shape,
                                           int split_dim, int shard_count,
                                           const std::string &error_context) {
    if (split_dim < 0 || split_dim >= static_cast<int>(shape.size()) ||
        shard_count <= 0) {
        LOG(ERROR) << error_context << ": invalid shard parameters";
        return false;
    }
    if (!is_uniform_shardable_dim(shape[split_dim], shard_count)) {
        LOG(ERROR) << error_context << ": only uniform sharding is supported";
        return false;
    }
    return true;
}

inline std::optional<py::object> materialize_shard_tensor(
    const py::handle &tensor, int split_dim, int rank, int shard_count) {
    auto shape = tensor_shape_to_vector(tensor);
    if (rank < 0 || rank >= shard_count ||
        !validate_uniform_shard_request(shape, split_dim, shard_count,
                                        "materialize_shard_tensor")) {
        return std::nullopt;
    }

    py::object torch_tensor = py::borrow<py::object>(tensor);
    const auto [start, size] =
        calculate_shard_range(shape[split_dim], rank, shard_count);
    return materialize_contiguous_tensor(
        torch_tensor.attr("narrow")(split_dim, start, size));
}

inline std::optional<std::vector<PyTensorInfo>> build_tp_shard_infos(
    const py::handle &tensor, int tp_size, int split_dim,
    const std::function<std::string(int)> &key_for_rank) {
    TensorDtype dtype_enum = get_tensor_dtype(tensor.attr("dtype"));
    if (dtype_enum == TensorDtype::UNKNOWN) {
        return std::nullopt;
    }

    auto shape = tensor_shape_to_vector(tensor);
    if (!validate_uniform_shard_request(shape, split_dim, tp_size,
                                        "build_tp_shard_infos")) {
        return std::nullopt;
    }

    std::vector<PyTensorInfo> infos;
    infos.reserve(tp_size);

    for (int rank = 0; rank < tp_size; ++rank) {
        auto chunk = materialize_shard_tensor(tensor, split_dim, rank, tp_size);
        if (!chunk.has_value()) {
            return std::nullopt;
        }

        auto info = extract_tensor_info(*chunk, key_for_rank(rank));
        info.metadata =
            build_tp_shard_metadata(tensor, *chunk, dtype_enum, rank, tp_size,
                                    split_dim, info.tensor_size);
        if (!info.valid()) {
            return std::nullopt;
        }
        infos.push_back(info);
    }
    return infos;
}

inline std::optional<ParsedTensorMetadata> parse_tensor_metadata_from_prefix(
    const void *buffer_ptr, const std::string &context,
    const std::string &key) {
    if (buffer_ptr == nullptr) {
        LOG(ERROR) << context << ": invalid metadata prefix for key " << key;
        return std::nullopt;
    }

    TensorMetadata metadata{};
    std::memcpy(&metadata, buffer_ptr, sizeof(TensorMetadata));
    auto parsed = ParseTensorMetadata(
        static_cast<const char *>(buffer_ptr),
        static_cast<size_t>(metadata.header.data_offset) +
            static_cast<size_t>(metadata.header.data_bytes));
    if (!parsed.has_value()) {
        LOG(ERROR) << context << ": invalid tensor metadata for key " << key;
    }
    return parsed;
}

}  // namespace mooncake
