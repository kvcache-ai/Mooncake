#pragma once

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <span>
#include <vector>

namespace py = pybind11;

namespace mooncake {

// Avoid global py::module_ objects
inline py::module_ torch_module() { return py::module_::import("torch"); }

enum class TensorDtype : int32_t {
    FLOAT32 = 0,
    FLOAT64 = 1,
    INT8 = 2,
    UINT8 = 3,
    INT16 = 4,
    UINT16 = 5,
    INT32 = 6,
    UINT32 = 7,
    INT64 = 8,
    UINT64 = 9,
    BOOL = 10,
    FLOAT16 = 11,
    BFLOAT16 = 12,
    FLOAT8_E4M3 = 13,
    FLOAT8_E5M2 = 14,
    NR_DTYPES = 15,
    UNKNOWN = -1
};

template <typename T>
py::array create_typed_array(char *exported_data, size_t offset,
                             size_t total_length, bool take_ownership) {
    if (take_ownership) {
        py::capsule free_when_done(
            exported_data, [](void *p) { delete[] static_cast<char *>(p); });
        return py::array_t<T>({static_cast<ssize_t>(total_length / sizeof(T))},
                              (T *)(exported_data + offset), free_when_done);
    }

    return py::array_t<T>({static_cast<ssize_t>(total_length / sizeof(T))},
                          (T *)(exported_data + offset), py::none());
}

using ArrayCreatorFunc = std::function<py::array(char *, size_t, size_t, bool)>;

static const std::array<ArrayCreatorFunc, 15> array_creators = {{
    create_typed_array<float>,     // FLOAT32 = 0
    create_typed_array<double>,    // FLOAT64 = 1
    create_typed_array<int8_t>,    // INT8 = 2
    create_typed_array<uint8_t>,   // UINT8 = 3
    create_typed_array<int16_t>,   // INT16 = 4
    create_typed_array<uint16_t>,  // UINT16 = 5
    create_typed_array<int32_t>,   // INT32 = 6
    create_typed_array<uint32_t>,  // UINT32 = 7
    create_typed_array<int64_t>,   // INT64 = 8
    create_typed_array<uint64_t>,  // UINT64 = 9
    create_typed_array<bool>,      // BOOL = 10
    create_typed_array<uint16_t>,  // FLOAT16 = 11 (using uint16_t as storage)
    create_typed_array<uint16_t>,  // BFLOAT16 = 12 (using uint16_t as storage)
    create_typed_array<uint8_t>,  // FLOAT8_E4M3 = 13 (using uint8_t as storage)
    create_typed_array<uint8_t>,  // FLOAT8_E5M2 = 14 (using uint8_t as storage)
}};

inline TensorDtype get_tensor_dtype(py::object dtype_obj) {
    if (dtype_obj.is_none()) {
        return TensorDtype::UNKNOWN;
    }

    auto torch = torch_module();

    if (dtype_obj.equal(torch.attr("float32"))) return TensorDtype::FLOAT32;
    if (dtype_obj.equal(torch.attr("float64"))) return TensorDtype::FLOAT64;
    if (dtype_obj.equal(torch.attr("int8"))) return TensorDtype::INT8;
    if (dtype_obj.equal(torch.attr("uint8"))) return TensorDtype::UINT8;
    if (dtype_obj.equal(torch.attr("int16"))) return TensorDtype::INT16;
    if (dtype_obj.equal(torch.attr("uint16"))) return TensorDtype::UINT16;
    if (dtype_obj.equal(torch.attr("int32"))) return TensorDtype::INT32;
    if (dtype_obj.equal(torch.attr("uint32"))) return TensorDtype::UINT32;
    if (dtype_obj.equal(torch.attr("int64"))) return TensorDtype::INT64;
    if (dtype_obj.equal(torch.attr("uint64"))) return TensorDtype::UINT64;
    if (dtype_obj.equal(torch.attr("bool"))) return TensorDtype::BOOL;
    if (dtype_obj.equal(torch.attr("float16"))) return TensorDtype::FLOAT16;
    if (dtype_obj.equal(torch.attr("bfloat16"))) return TensorDtype::BFLOAT16;
    if (dtype_obj.equal(torch.attr("float8_e4m3fn")))
        return TensorDtype::FLOAT8_E4M3;
    if (dtype_obj.equal(torch.attr("float8_e5m2")))
        return TensorDtype::FLOAT8_E5M2;

    return TensorDtype::UNKNOWN;
}

constexpr uint32_t kTensorObjectMagic = 0x4d4f4f4e;
constexpr uint16_t kTensorObjectVersion = 1;
constexpr size_t kMaxTensorDims = 8;
constexpr size_t kMaxLayoutAxes = 4;

enum class TensorLayoutKind : uint32_t {
    FULL = 0,
    SHARD = 1,
};

enum class LayoutAxisKind : int32_t {
    DP = 0,
    TP = 1,
    EP = 2,
    PP = 3,
    RESERVED = 4,
};

struct TensorShape {
    int64_t dims[kMaxTensorDims];
};

struct LayoutAxis {
    int32_t kind;
    int32_t axis_index;
    int32_t shard_rank;
    int32_t shard_count;
    int32_t split_dim;
    int32_t reserved0;
    int64_t reserved1;
};

struct TensorLayoutMetadata {
    TensorShape global_shape;
    TensorShape local_shape;
    uint32_t axis_count;
    uint32_t reserved0;
    LayoutAxis axes[kMaxLayoutAxes];
};

struct TensorObjectHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t header_size;
    int32_t dtype;
    int32_t ndim;
    uint32_t layout_kind;
    uint32_t reserved_flags;
    uint64_t data_offset;
    uint64_t data_bytes;
};

struct TensorMetadata {
    TensorObjectHeader header;
    TensorLayoutMetadata layout;
};

struct ParsedTensorMetadata {
    TensorMetadata metadata;
    size_t data_offset;
    size_t data_bytes;
};

inline TensorShape MakeTensorShape(const std::vector<int64_t> &dims) {
    TensorShape shape{};
    std::fill(std::begin(shape.dims), std::end(shape.dims), -1);
    for (size_t i = 0; i < dims.size() && i < kMaxTensorDims; ++i) {
        shape.dims[i] = dims[i];
    }
    return shape;
}

inline std::vector<int64_t> TensorShapeToVector(const TensorShape &shape,
                                                int32_t ndim) {
    std::vector<int64_t> dims;
    dims.reserve(ndim);
    for (int32_t i = 0; i < ndim; ++i) {
        dims.push_back(shape.dims[i]);
    }
    return dims;
}

inline TensorMetadata BuildTensorMetadata(
    int32_t dtype, const std::vector<int64_t> &global_shape,
    const std::vector<int64_t> &local_shape,
    TensorLayoutKind layout_kind = TensorLayoutKind::FULL,
    std::span<const LayoutAxis> axes = {}) {
    TensorMetadata metadata{};
    metadata.header.magic = kTensorObjectMagic;
    metadata.header.version = kTensorObjectVersion;
    metadata.header.header_size = sizeof(TensorMetadata);
    metadata.header.dtype = dtype;
    metadata.header.ndim = static_cast<int32_t>(global_shape.size());
    metadata.header.layout_kind = static_cast<uint32_t>(layout_kind);
    metadata.header.reserved_flags = 0;
    metadata.header.data_offset = sizeof(TensorMetadata);
    metadata.header.data_bytes = 0;

    metadata.layout.global_shape = MakeTensorShape(global_shape);
    metadata.layout.local_shape = MakeTensorShape(local_shape);
    metadata.layout.axis_count =
        static_cast<uint32_t>(std::min(axes.size(), kMaxLayoutAxes));
    metadata.layout.reserved0 = 0;
    for (size_t i = 0; i < metadata.layout.axis_count; ++i) {
        metadata.layout.axes[i] = axes[i];
    }
    return metadata;
}

inline bool ValidateTensorMetadata(const TensorMetadata &metadata,
                                   size_t total_length) {
    if (metadata.header.magic != kTensorObjectMagic ||
        metadata.header.version != kTensorObjectVersion ||
        metadata.header.header_size != sizeof(TensorMetadata)) {
        return false;
    }

    if (metadata.header.dtype < 0 ||
        metadata.header.dtype >= static_cast<int32_t>(TensorDtype::NR_DTYPES)) {
        return false;
    }

    if (metadata.header.ndim < 0 ||
        metadata.header.ndim > static_cast<int32_t>(kMaxTensorDims)) {
        return false;
    }

    if (metadata.layout.axis_count > kMaxLayoutAxes) {
        return false;
    }

    if (metadata.header.layout_kind >
        static_cast<uint32_t>(TensorLayoutKind::SHARD)) {
        return false;
    }

    if (metadata.header.data_offset < sizeof(TensorMetadata) ||
        metadata.header.data_offset > total_length) {
        return false;
    }

    if (metadata.header.data_bytes !=
        total_length - metadata.header.data_offset) {
        return false;
    }

    for (int32_t i = 0; i < metadata.header.ndim; ++i) {
        if (metadata.layout.global_shape.dims[i] <= 0 ||
            metadata.layout.local_shape.dims[i] < 0) {
            return false;
        }
    }

    for (size_t i = metadata.header.ndim; i < kMaxTensorDims; ++i) {
        if (metadata.layout.global_shape.dims[i] != -1 ||
            metadata.layout.local_shape.dims[i] != -1) {
            return false;
        }
    }

    for (size_t i = 0; i < metadata.layout.axis_count; ++i) {
        const auto &axis = metadata.layout.axes[i];
        if (axis.kind < static_cast<int32_t>(LayoutAxisKind::DP) ||
            axis.kind > static_cast<int32_t>(LayoutAxisKind::RESERVED)) {
            return false;
        }
        if (axis.shard_count <= 0 || axis.shard_rank < 0 ||
            axis.shard_rank >= axis.shard_count) {
            return false;
        }
        if (axis.split_dim < -1 || axis.split_dim >= metadata.header.ndim) {
            return false;
        }
    }

    return true;
}

inline std::optional<ParsedTensorMetadata> ParseTensorMetadata(
    const char *data, size_t total_length) {
    if (!data || total_length < sizeof(TensorMetadata)) {
        return std::nullopt;
    }

    ParsedTensorMetadata parsed{};
    std::memcpy(&parsed.metadata, data, sizeof(TensorMetadata));
    if (!ValidateTensorMetadata(parsed.metadata, total_length)) {
        return std::nullopt;
    }

    parsed.data_offset =
        static_cast<size_t>(parsed.metadata.header.data_offset);
    parsed.data_bytes = static_cast<size_t>(parsed.metadata.header.data_bytes);
    return parsed;
}

}  // namespace mooncake
