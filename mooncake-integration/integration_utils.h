#pragma once

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <array>
#include <functional>
#include <stdexcept>

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

inline size_t get_element_size(int32_t dtype) {
    switch (static_cast<TensorDtype>(dtype)) {
        case TensorDtype::FLOAT32:
            return sizeof(float);
        case TensorDtype::FLOAT64:
            return sizeof(double);
        case TensorDtype::INT8:
        case TensorDtype::UINT8:
            return 1;
        case TensorDtype::INT16:
        case TensorDtype::UINT16:
            return 2;
        case TensorDtype::INT32:
        case TensorDtype::UINT32:
            return 4;
        case TensorDtype::INT64:
        case TensorDtype::UINT64:
            return 8;
        case TensorDtype::BOOL:
            return 1;
        case TensorDtype::FLOAT16:
        case TensorDtype::BFLOAT16:
            return 2;
        case TensorDtype::FLOAT8_E4M3:
        case TensorDtype::FLOAT8_E5M2:
            return 1;
        default:
            LOG(ERROR)
                << "Unknown or unsupported TensorDtype: " << dtype
                << ". This can lead to memory corruption. "
                << "Please add support for this dtype or fix the dtype value.";
            throw std::runtime_error("Unknown or unsupported TensorDtype: " +
                                     std::to_string(dtype));
    }
}

struct TensorMetadata {
    int32_t dtype;
    int32_t ndim;
    int64_t shape[4];
};

// Global metadata stored once per logical key
// Contains information about the full tensor
// Use packed struct to avoid alignment issues
#pragma pack(push, 1)
struct GlobalMetadata {
    int32_t dtype;
    int32_t ndim;
    int32_t split_dim;
    int64_t shape[4];  // Full tensor shape
};
#pragma pack(pop)

// Chunk metadata stored per chunk
// Contains information about a single chunk's position in the split dimension
#pragma pack(push, 1)
struct ChunkMetadata {
    int64_t start_idx;  // Starting index in split_dim
    int64_t size;       // Size in split_dim
};
#pragma pack(pop)

}  // namespace mooncake
