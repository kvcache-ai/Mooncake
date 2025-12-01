#include <pybind11/gil.h>  // For GIL management
#include <pybind11/stl.h>
#include <numa.h>

#include "pyclient.h"
#include "dummy_client.h"
#include "real_client.h"

#include <cstdlib>  // for atexit

#include "integration_utils.h"

namespace py = pybind11;

namespace mooncake {
namespace {
std::vector<std::vector<void *>> CastAddrs2Ptrs(
    const std::vector<std::vector<uintptr_t>> &all_buffer_ptrs) {
    std::vector<std::vector<void *>> all_buffers;
    all_buffers.reserve(all_buffer_ptrs.size());
    for (auto &buffer_ptrs : all_buffer_ptrs) {
        std::vector<void *> ptrs;
        ptrs.reserve(buffer_ptrs.size());
        for (uintptr_t ptr : buffer_ptrs) {
            ptrs.push_back(reinterpret_cast<void *>(ptr));
        }
        all_buffers.emplace_back(std::move(ptrs));
    }
    return all_buffers;
}

// Helper function to convert ErrorCode to Python return value
// ErrorCode values are already negative, so just cast to int
inline int to_py_ret(ErrorCode error_code) {
    return static_cast<int>(error_code);
}
}  // namespace
// Python-specific wrapper functions that handle GIL and return pybind11 types
class MooncakeStorePyWrapper {
   public:
    std::shared_ptr<PyClient> store_{nullptr};
    bool use_dummy_client_{false};

    MooncakeStorePyWrapper() = default;

    bool is_client_initialized() const {
        // Check if the store and client are initialized
        // Dummy client does not use client_ instance
        return (store_ && (use_dummy_client_ || store_->client_));
    }

    pybind11::bytes get(const std::string &key) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return pybind11::bytes("\\0", 0);
        }

        const auto kNullString = pybind11::bytes("\\0", 0);

        {
            py::gil_scoped_release release_gil;
            if (use_dummy_client_) {
                auto [buffer_base, buffer_size] = store_->get_buffer_info(key);
                if (buffer_size == 0) {
                    py::gil_scoped_acquire acquire_gil;
                    return kNullString;
                }
                py::gil_scoped_acquire acquire_gil;
                return pybind11::bytes(reinterpret_cast<char *>(buffer_base),
                                       buffer_size);
            } else {
                auto buffer_handle = store_->get_buffer(key);
                if (!buffer_handle) {
                    py::gil_scoped_acquire acquire_gil;
                    return kNullString;
                }

                py::gil_scoped_acquire acquire_gil;
                return pybind11::bytes((char *)buffer_handle->ptr(),
                                       buffer_handle->size());
            }
        }
    }

    std::vector<pybind11::bytes> get_batch(
        const std::vector<std::string> &keys) {
        const auto kNullString = pybind11::bytes("\\0", 0);
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            py::gil_scoped_acquire acquire_gil;
            return {kNullString};
        }

        {
            py::gil_scoped_release release_gil;
            auto batch_data = store_->batch_get_buffer(keys);
            if (batch_data.empty()) {
                py::gil_scoped_acquire acquire_gil;
                return {kNullString};
            }

            py::gil_scoped_acquire acquire_gil;
            std::vector<pybind11::bytes> results;
            results.reserve(batch_data.size());

            for (const auto &data : batch_data) {
                results.emplace_back(
                    data ? pybind11::bytes((char *)data->ptr(), data->size())
                         : kNullString);
            }

            return results;
        }
    }

    pybind11::object get_tensor(const std::string &key) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return pybind11::none();
        }

        if (use_dummy_client_) {
            LOG(ERROR) << "get_tensor is not supported for dummy client now";
            return pybind11::none();
        }

        try {
            // Section with GIL released
            py::gil_scoped_release release_gil;
            auto buffer_handle = store_->get_buffer(key);
            if (!buffer_handle) {
                py::gil_scoped_acquire acquire_gil;
                return pybind11::none();
            }
            // Create contiguous buffer and copy data
            auto total_length = buffer_handle->size();
            char *exported_data = new char[total_length];
            if (!exported_data) {
                py::gil_scoped_acquire acquire_gil;
                LOG(ERROR) << "Invalid data format: insufficient data for "
                              "metadata";
                return pybind11::none();
            }
            TensorMetadata metadata;
            // Copy data from buffer to contiguous memory
            memcpy(exported_data, buffer_handle->ptr(), total_length);
            memcpy(&metadata, exported_data, sizeof(TensorMetadata));

            if (metadata.ndim < 0 || metadata.ndim > 4) {
                delete[] exported_data;
                py::gil_scoped_acquire acquire_gil;
                LOG(ERROR) << "Invalid tensor metadata: ndim=" << metadata.ndim;
                return pybind11::none();
            }

            TensorDtype dtype_enum = static_cast<TensorDtype>(metadata.dtype);
            if (dtype_enum == TensorDtype::UNKNOWN) {
                delete[] exported_data;
                py::gil_scoped_acquire acquire_gil;
                LOG(ERROR) << "Unknown tensor dtype!";
                return pybind11::none();
            }

            size_t tensor_size = total_length - sizeof(TensorMetadata);
            if (tensor_size == 0) {
                delete[] exported_data;
                py::gil_scoped_acquire acquire_gil;
                LOG(ERROR) << "Invalid data format: no tensor data found";
                return pybind11::none();
            }

            py::gil_scoped_acquire acquire_gil;
            // Convert bytes to tensor using torch.from_numpy
            pybind11::object np_array;
            int dtype_index = static_cast<int>(dtype_enum);
            if (dtype_index >= 0 &&
                dtype_index < static_cast<int>(array_creators.size())) {
                np_array = array_creators[dtype_index](
                    exported_data, sizeof(TensorMetadata), tensor_size);
            } else {
                LOG(ERROR) << "Unsupported dtype enum: " << dtype_index;
                return pybind11::none();
            }

            if (metadata.ndim > 0) {
                std::vector<int> shape_vec;
                for (int i = 0; i < metadata.ndim; i++) {
                    shape_vec.push_back(metadata.shape[i]);
                }
                py::tuple shape_tuple = py::cast(shape_vec);
                np_array = np_array.attr("reshape")(shape_tuple);
            }
            pybind11::object tensor =
                torch_module().attr("from_numpy")(np_array);
            return tensor;

        } catch (const pybind11::error_already_set &e) {
            LOG(ERROR) << "Failed to get tensor data: " << e.what();
            return pybind11::none();
        }
    }

    pybind11::list batch_get_tensor(const std::vector<std::string> &keys) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            py::list empty_list;
            for (size_t i = 0; i < keys.size(); ++i) {
                empty_list.append(py::none());
            }
            return empty_list;
        }

        if (use_dummy_client_) {
            LOG(ERROR) << "batch_get_tensor is not supported for dummy client "
                          "now";
            py::list empty_list;
            for (size_t i = 0; i < keys.size(); ++i) {
                empty_list.append(py::none());
            }
            return empty_list;
        }

        // Phase 1: Batch Get Buffers (GIL Released)
        std::vector<std::shared_ptr<BufferHandle>> buffer_handles;
        {
            py::gil_scoped_release release_gil;
            // This internal call already handles logging for query failures
            buffer_handles = store_->batch_get_buffer(keys);
        }

        py::list results_list;

        try {
            py::gil_scoped_acquire acquire_gil;
            auto torch = torch_module();

            for (const auto &buffer_handle : buffer_handles) {
                if (!buffer_handle) {
                    results_list.append(py::none());
                    continue;
                }

                auto total_length = buffer_handle->size();
                if (total_length <= sizeof(TensorMetadata)) {
                    LOG(ERROR) << "Invalid data format: insufficient data for "
                                  "metadata";
                    results_list.append(py::none());
                    continue;
                }

                char *exported_data = new char[total_length];
                if (!exported_data) {
                    LOG(ERROR) << "Failed to allocate memory for tensor data";
                    results_list.append(py::none());
                    continue;
                }

                memcpy(exported_data, buffer_handle->ptr(), total_length);

                TensorMetadata metadata;
                memcpy(&metadata, exported_data, sizeof(TensorMetadata));

                if (metadata.ndim < 0 || metadata.ndim > 4) {
                    delete[] exported_data;
                    LOG(ERROR)
                        << "Invalid tensor metadata: ndim=" << metadata.ndim;
                    results_list.append(py::none());
                    continue;
                }

                TensorDtype dtype_enum =
                    static_cast<TensorDtype>(metadata.dtype);
                if (dtype_enum == TensorDtype::UNKNOWN) {
                    delete[] exported_data;
                    LOG(ERROR) << "Unknown tensor dtype!";
                    results_list.append(py::none());
                    continue;
                }

                size_t tensor_size = total_length - sizeof(TensorMetadata);
                if (tensor_size == 0) {
                    delete[] exported_data;
                    LOG(ERROR) << "Invalid data format: no tensor data found";
                    results_list.append(py::none());
                    continue;
                }

                pybind11::object np_array;
                int dtype_index = static_cast<int>(dtype_enum);
                if (dtype_index >= 0 &&
                    dtype_index < static_cast<int>(array_creators.size())) {
                    // This call MUST take ownership of exported_data
                    np_array = array_creators[dtype_index](
                        exported_data, sizeof(TensorMetadata), tensor_size);
                } else {
                    delete[] exported_data;  // Free memory on error
                    LOG(ERROR) << "Unsupported dtype enum: " << dtype_index;
                    results_list.append(py::none());
                    continue;
                }

                if (metadata.ndim > 0) {
                    std::vector<int> shape_vec;
                    for (int i = 0; i < metadata.ndim; i++) {
                        shape_vec.push_back(metadata.shape[i]);
                    }
                    py::tuple shape_tuple = py::cast(shape_vec);
                    np_array = np_array.attr("reshape")(shape_tuple);
                }
                pybind11::object tensor = torch.attr("from_numpy")(np_array);
                results_list.append(tensor);
            }
        } catch (const pybind11::error_already_set &e) {
            LOG(ERROR) << "Failed during batch tensor deserialization: "
                       << e.what();
        }
        return results_list;
    }

    int put_tensor(const std::string &key, pybind11::object tensor) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        if (use_dummy_client_) {
            LOG(ERROR) << "put_tensor is not supported for dummy client now";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        try {
            if (!(tensor.attr("__class__")
                      .attr("__name__")
                      .cast<std::string>()
                      .find("Tensor") != std::string::npos)) {
                LOG(ERROR) << "Input is not a PyTorch tensor";
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }

            uintptr_t data_ptr = tensor.attr("data_ptr")().cast<uintptr_t>();
            size_t numel = tensor.attr("numel")().cast<size_t>();
            size_t element_size = tensor.attr("element_size")().cast<size_t>();
            size_t tensor_size = numel * element_size;

            pybind11::object shape_obj = tensor.attr("shape");
            pybind11::object dtype_obj = tensor.attr("dtype");

            TensorDtype dtype_enum = get_tensor_dtype(dtype_obj);
            if (dtype_enum == TensorDtype::UNKNOWN) {
                LOG(ERROR) << "Unsupported tensor dtype!";
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }

            pybind11::tuple shape_tuple =
                pybind11::cast<pybind11::tuple>(shape_obj);
            int32_t ndim = static_cast<int32_t>(shape_tuple.size());
            if (ndim > 4) {
                LOG(ERROR) << "Tensor has more than 4 dimensions: " << ndim;
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }

            TensorMetadata metadata;
            metadata.dtype = static_cast<int32_t>(dtype_enum);
            metadata.ndim = ndim;

            for (int i = 0; i < 4; i++) {
                if (i < ndim) {
                    metadata.shape[i] = shape_tuple[i].cast<uint64_t>();
                } else {
                    metadata.shape[i] = -1;
                }
            }

            // Section with GIL released
            py::gil_scoped_release release_gil;
            char *buffer = reinterpret_cast<char *>(data_ptr);
            char *metadata_buffer = reinterpret_cast<char *>(&metadata);
            std::vector<std::span<const char>> values;
            values.emplace_back(
                std::span<const char>(metadata_buffer, sizeof(TensorMetadata)));
            values.emplace_back(std::span<const char>(buffer, tensor_size));

            // Use put_parts to put metadata and tensor together
            auto put_result = store_->put_parts(key, values);
            if (put_result != 0) return put_result;
            return 0;
        } catch (const pybind11::error_already_set &e) {
            LOG(ERROR) << "Failed to access tensor data: " << e.what();
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
    }

    int pub_tensor(const std::string &key, pybind11::object tensor,
                   const ReplicateConfig &config = ReplicateConfig{}) {
        if (!store_ || !store_->client_) {
            LOG(ERROR) << "Client is not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        // Validate segment preferences
        if (!config.preferred_segments.empty() &&
            config.preferred_segments.size() != config.replica_num) {
            LOG(ERROR) << "Preferred segments size ("
                       << config.preferred_segments.size()
                       << ") must match replica_num (" << config.replica_num
                       << ")";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        try {
            if (!(tensor.attr("__class__")
                      .attr("__name__")
                      .cast<std::string>()
                      .find("Tensor") != std::string::npos)) {
                LOG(ERROR) << "Input is not a PyTorch tensor";
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }

            uintptr_t data_ptr = tensor.attr("data_ptr")().cast<uintptr_t>();
            size_t numel = tensor.attr("numel")().cast<size_t>();
            size_t element_size = tensor.attr("element_size")().cast<size_t>();
            size_t tensor_size = numel * element_size;

            pybind11::object shape_obj = tensor.attr("shape");
            pybind11::object dtype_obj = tensor.attr("dtype");

            TensorDtype dtype_enum = get_tensor_dtype(dtype_obj);
            if (dtype_enum == TensorDtype::UNKNOWN) {
                LOG(ERROR) << "Unsupported tensor dtype!";
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }

            pybind11::tuple shape_tuple =
                pybind11::cast<pybind11::tuple>(shape_obj);
            int32_t ndim = static_cast<int32_t>(shape_tuple.size());
            if (ndim > 4) {
                LOG(ERROR) << "Tensor has more than 4 dimensions: " << ndim;
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }

            TensorMetadata metadata;
            metadata.dtype = static_cast<int32_t>(dtype_enum);
            metadata.ndim = ndim;

            for (int i = 0; i < 4; i++) {
                if (i < ndim) {
                    metadata.shape[i] = shape_tuple[i].cast<uint64_t>();
                } else {
                    metadata.shape[i] = -1;
                }
            }

            // Section with GIL released
            py::gil_scoped_release release_gil;
            char *buffer = reinterpret_cast<char *>(data_ptr);
            char *metadata_buffer = reinterpret_cast<char *>(&metadata);
            std::vector<std::span<const char>> values;
            values.emplace_back(
                std::span<const char>(metadata_buffer, sizeof(TensorMetadata)));
            values.emplace_back(std::span<const char>(buffer, tensor_size));

            // Use put_parts to put metadata and tensor together with custom
            // config
            auto put_result = store_->put_parts(key, values, config);
            if (!put_result) {
                return put_result;
            }

            return 0;
        } catch (const pybind11::error_already_set &e) {
            LOG(ERROR) << "Failed to access tensor data: " << e.what();
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
    }

    std::vector<int> batch_put_tensor(const std::vector<std::string> &keys,
                                      const pybind11::list &tensors_list) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        if (use_dummy_client_) {
            LOG(ERROR) << "batch_put_tensor is not supported for dummy client "
                          "now";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        if (keys.size() != tensors_list.size()) {
            LOG(ERROR) << "Keys and tensors list size mismatch. keys="
                       << keys.size() << ", tensors=" << tensors_list.size();
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        if (keys.empty()) {
            return std::vector<int>();
        }

        struct TensorInfo {
            uintptr_t data_ptr;
            size_t tensor_size;
            TensorMetadata metadata;
            bool valid = false;  // Mark if metadata extraction was successful
        };

        std::vector<TensorInfo> infos(keys.size());
        std::vector<int> results(keys.size(), 0);  // Default to success

        // Phase 1: Extract Metadata (GIL Held)
        try {
            for (size_t i = 0; i < keys.size(); ++i) {
                py::object tensor = tensors_list[i];

                if (!(tensor.attr("__class__")
                          .attr("__name__")
                          .cast<std::string>()
                          .find("Tensor") != std::string::npos)) {
                    LOG(ERROR)
                        << "Input at index " << i << " is not a PyTorch tensor";
                    results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                uintptr_t data_ptr =
                    tensor.attr("data_ptr")().cast<uintptr_t>();
                size_t numel = tensor.attr("numel")().cast<size_t>();
                size_t element_size =
                    tensor.attr("element_size")().cast<size_t>();
                size_t tensor_size = numel * element_size;

                pybind11::object shape_obj = tensor.attr("shape");
                pybind11::object dtype_obj = tensor.attr("dtype");

                TensorDtype dtype_enum = get_tensor_dtype(dtype_obj);
                if (dtype_enum == TensorDtype::UNKNOWN) {
                    LOG(ERROR)
                        << "Unsupported tensor dtype for key " << keys[i];
                    results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                pybind11::tuple shape_tuple =
                    pybind11::cast<pybind11::tuple>(shape_obj);
                int32_t ndim = static_cast<int32_t>(shape_tuple.size());
                if (ndim > 4) {
                    LOG(ERROR) << "Tensor " << keys[i]
                               << " has more than 4 dimensions: " << ndim;
                    results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                TensorMetadata metadata;
                metadata.dtype = static_cast<int32_t>(dtype_enum);
                metadata.ndim = ndim;

                for (int j = 0; j < 4; j++) {
                    metadata.shape[j] =
                        (j < ndim) ? shape_tuple[j].cast<uint64_t>() : -1;
                }

                infos[i] = TensorInfo{data_ptr, tensor_size, metadata, true};
            }
        } catch (const pybind11::error_already_set &e) {
            LOG(ERROR) << "Failed to access tensor data during batch put: "
                       << e.what();
            return results;
        }

        std::vector<std::string> valid_keys;
        std::vector<void *> buffer_ptrs;
        std::vector<size_t> buffer_sizes;
        std::vector<std::unique_ptr<BufferHandle>>
            temp_handles;  // Manages lifetime of allocated buffers
        std::vector<size_t> valid_indices;  // To map results back

        {
            py::gil_scoped_release release_gil;

            for (size_t i = 0; i < infos.size(); ++i) {
                if (!infos[i].valid) {
                    continue;  // Skip items that failed metadata extraction
                }

                const auto &info = infos[i];
                size_t total_size = sizeof(TensorMetadata) + info.tensor_size;

                // Allocate a contiguous buffer for this tensor (metadata +
                // data)
                auto alloc_result =
                    store_->client_buffer_allocator_->allocate(total_size);

                if (!alloc_result) {
                    LOG(ERROR)
                        << "Failed to allocate buffer for key: " << keys[i]
                        << "size is: " << total_size;
                    results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;  // Skip this item
                }

                auto &handle = *alloc_result;

                // Copy metadata
                memcpy(handle.ptr(), &info.metadata, sizeof(TensorMetadata));
                // Copy tensor data
                memcpy(
                    static_cast<char *>(handle.ptr()) + sizeof(TensorMetadata),
                    reinterpret_cast<void *>(info.data_ptr), info.tensor_size);

                // Add to the list for batch_put_from
                valid_keys.push_back(keys[i]);
                buffer_ptrs.push_back(handle.ptr());
                buffer_sizes.push_back(total_size);
                temp_handles.push_back(
                    std::make_unique<BufferHandle>(std::move(handle)));
                valid_indices.push_back(i);
            }

            if (valid_keys.empty()) {
                return results;
            }

            std::vector<int> batch_op_results =
                store_->batch_put_from(valid_keys, buffer_ptrs, buffer_sizes);

            for (size_t i = 0; i < batch_op_results.size(); ++i) {
                size_t original_index = valid_indices[i];
                results[original_index] = batch_op_results[i];
            }
        }

        return results;
    }
};

PYBIND11_MODULE(store, m) {
    // Define the ReplicateConfig class
    py::class_<ReplicateConfig>(m, "ReplicateConfig")
        .def(py::init<>())
        .def_readwrite("replica_num", &ReplicateConfig::replica_num)
        .def_readwrite("with_soft_pin", &ReplicateConfig::with_soft_pin)
        .def_readwrite("preferred_segments",
                       &ReplicateConfig::preferred_segments)
        .def_readwrite("preferred_segment", &ReplicateConfig::preferred_segment)
        .def_readwrite("prefer_alloc_in_same_node",
                       &ReplicateConfig::prefer_alloc_in_same_node)
        .def("__str__", [](const ReplicateConfig &config) {
            std::ostringstream oss;
            oss << config;
            return oss.str();
        });

    py::enum_<ReplicaStatus>(m, "ReplicaStatus")
        .value("UNDEFINED", ReplicaStatus::UNDEFINED)
        .value("INITIALIZED", ReplicaStatus::INITIALIZED)
        .value("PROCESSING", ReplicaStatus::PROCESSING)
        .value("COMPLETE", ReplicaStatus::COMPLETE)
        .value("REMOVED", ReplicaStatus::REMOVED)
        .value("FAILED", ReplicaStatus::FAILED)
        .export_values();

    py::class_<MemoryDescriptor>(m, "MemoryDescriptor")
        .def_readwrite("buffer_descriptor",
                       &MemoryDescriptor::buffer_descriptor);

    py::class_<DiskDescriptor>(m, "DiskDescriptor")
        .def_readwrite("file_path", &DiskDescriptor::file_path)
        .def_readwrite("object_size", &DiskDescriptor::object_size);

    py::class_<Replica::Descriptor>(m, "ReplicaDescriptor")
        .def_readonly("status", &Replica::Descriptor::status)
        .def("is_memory_replica",
             static_cast<bool (Replica::Descriptor::*)() const noexcept>(
                 &Replica::Descriptor::is_memory_replica))
        .def("is_disk_replica",
             static_cast<bool (Replica::Descriptor::*)() const noexcept>(
                 &Replica::Descriptor::is_disk_replica))
        .def(
            "get_memory_descriptor",
            static_cast<const MemoryDescriptor &(Replica::Descriptor::*)()
                            const>(&Replica::Descriptor::get_memory_descriptor),
            py::return_value_policy::reference_internal)
        .def(
            "get_disk_descriptor",
            static_cast<const DiskDescriptor &(Replica::Descriptor::*)() const>(
                &Replica::Descriptor::get_disk_descriptor),
            py::return_value_policy::reference_internal);

    py::class_<AllocatedBuffer::Descriptor>(
        m, "Descriptor",
        "Descriptor for allocated buffers. Only memory descriptors are "
        "supported.")
        .def(py::init<>())
        .def_readwrite("size", &AllocatedBuffer::Descriptor::size_)
        .def_readwrite("buffer_address",
                       &AllocatedBuffer::Descriptor::buffer_address_)
        .def_readwrite("transport_endpoint",
                       &AllocatedBuffer::Descriptor::transport_endpoint_)
        .def("__repr__", [](const AllocatedBuffer::Descriptor &desc) {
            return "<Descriptor size=" + std::to_string(desc.size_) +
                   " buffer_address=" + std::to_string(desc.buffer_address_) +
                   " transport_endpoint=" + desc.transport_endpoint_ + ">";
        });

    // Define the BufferHandle class
    py::class_<BufferHandle, std::shared_ptr<BufferHandle>>(
        m, "BufferHandle", py::buffer_protocol())
        .def("ptr",
             [](const BufferHandle &self) {
                 // Return the pointer as an integer for Python
                 return reinterpret_cast<uintptr_t>(self.ptr());
             })
        .def("size", &BufferHandle::size)
        .def("__len__", &BufferHandle::size)
        .def_buffer([](BufferHandle &self) -> py::buffer_info {
            // BufferHandle now always contains contiguous memory
            if (self.size() > 0) {
                return py::buffer_info(
                    self.ptr(),   /* Pointer to buffer */
                    sizeof(char), /* Size of one scalar */
                    py::format_descriptor<
                        char>::format(),   /* Python struct-style
                                              format descriptor */
                    1,                     /* Number of dimensions */
                    {(size_t)self.size()}, /* Buffer dimensions */
                    {sizeof(char)} /* Strides (in bytes) for each index */
                );
            } else {
                // Empty buffer
                return py::buffer_info(
                    nullptr,      /* Pointer to buffer */
                    sizeof(char), /* Size of one scalar */
                    py::format_descriptor<
                        char>::format(), /* Python struct-style
                                            format descriptor */
                    1,                   /* Number of dimensions */
                    {0},                 /* Buffer dimensions */
                    {sizeof(char)}       /* Strides (in bytes) for each index */
                );
            }
        });

    // Create a wrapper that exposes DistributedObjectStore with Python-specific
    // methods
    py::class_<MooncakeStorePyWrapper>(m, "MooncakeDistributedStore")
        .def(py::init<>())
        .def(
            "setup",
            [](MooncakeStorePyWrapper &self, const std::string &local_hostname,
               const std::string &metadata_server,
               size_t global_segment_size = 1024 * 1024 * 16,
               size_t local_buffer_size = 1024 * 1024 * 16,
               const std::string &protocol = "tcp",
               const std::string &rdma_devices = "",
               const std::string &master_server_addr = "127.0.0.1:50051",
               const py::object &engine = py::none()) {
                self.use_dummy_client_ = false;
                self.store_ = std::make_shared<RealClient>();
                if (!self.store_) {
                    self.store_ = PyClient::create();
                }
                ResourceTracker::getInstance().registerInstance(
                    std::dynamic_pointer_cast<PyClient>(self.store_));
                std::shared_ptr<mooncake::TransferEngine> transfer_engine =
                    nullptr;
                if (!engine.is_none()) {
                    transfer_engine =
                        engine.cast<std::shared_ptr<TransferEngine>>();
                }
                return self.store_->setup_real(
                    local_hostname, metadata_server, global_segment_size,
                    local_buffer_size, protocol, rdma_devices,
                    master_server_addr, transfer_engine, "");
            },
            py::arg("local_hostname"), py::arg("metadata_server"),
            py::arg("global_segment_size"), py::arg("local_buffer_size"),
            py::arg("protocol"), py::arg("rdma_devices"),
            py::arg("master_server_addr"), py::arg("engine") = py::none())
        .def(
            "setup_dummy",
            [](MooncakeStorePyWrapper &self, size_t mem_pool_size,
               size_t local_buffer_size, const std::string &server_address) {
                self.use_dummy_client_ = true;
                self.store_ = std::make_shared<DummyClient>();
                ResourceTracker::getInstance().registerInstance(
                    std::dynamic_pointer_cast<PyClient>(self.store_));
                auto [ip, port] = parseHostNameWithPort(server_address);
                return self.store_->setup_dummy(
                    mem_pool_size, local_buffer_size, server_address,
                    "@mooncake_client_" + std::to_string(port) + ".sock");
            },
            py::arg("mem_pool_size"), py::arg("local_buffer_size"),
            py::arg("server_address"))
        .def("init_all",
             [](MooncakeStorePyWrapper &self, const std::string &protocol,
                const std::string &device_name,
                size_t mount_segment_size = 1024 * 1024 * 16) {
                 return self.store_->initAll(protocol, device_name,
                                             mount_segment_size);
             })
        .def("alloc_from_mem_pool",
             [](MooncakeStorePyWrapper &self, size_t size) {
                 py::gil_scoped_release release;
                 return self.store_->alloc_from_mem_pool(size);
             })
        .def("get", &mooncake::MooncakeStorePyWrapper::get)
        .def("get_batch", &mooncake::MooncakeStorePyWrapper::get_batch)
        .def(
            "get_buffer",
            [](MooncakeStorePyWrapper &self, const std::string &key) {
                py::gil_scoped_release release;
                return self.store_->get_buffer(key);
            },
            py::return_value_policy::take_ownership)
        .def(
            "batch_get_buffer",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys) {
                py::gil_scoped_release release;
                if (self.use_dummy_client_) {
                    LOG(ERROR) << "batch_get_buffer is not supported for dummy "
                                  "client now";
                    return std::vector<std::shared_ptr<BufferHandle>>{};
                }
                return self.store_->batch_get_buffer(keys);
            },
            py::return_value_policy::take_ownership)
        .def("remove",
             [](MooncakeStorePyWrapper &self, const std::string &key) {
                 py::gil_scoped_release release;
                 return self.store_->remove(key);
             })
        .def(
            "remove_by_regex",
            [](MooncakeStorePyWrapper &self, const std::string &str) {
                py::gil_scoped_release release;
                return self.store_->removeByRegex(str);
            },
            py::arg("regex_pattern"),
            "Removes objects from the store whose keys match the given "
            "regular expression.")
        .def("remove_all",
             [](MooncakeStorePyWrapper &self) {
                 py::gil_scoped_release release;
                 return self.store_->removeAll();
             })
        .def("is_exist",
             [](MooncakeStorePyWrapper &self, const std::string &key) {
                 py::gil_scoped_release release;
                 return self.store_->isExist(key);
             })
        .def(
            "batch_is_exist",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys) {
                py::gil_scoped_release release;
                return self.store_->batchIsExist(keys);
            },
            py::arg("keys"),
            "Check if multiple objects exist. Returns list of results: 1 if "
            "exists, 0 if not exists, -1 if error")
        .def("close",
             [](MooncakeStorePyWrapper &self) {
                 if (!self.store_) return 0;
                 int rc = self.store_->tearDownAll();
                 self.store_.reset();
                 return rc;
             })
        .def("get_size",
             [](MooncakeStorePyWrapper &self, const std::string &key) {
                 py::gil_scoped_release release;
                 return self.store_->getSize(key);
             })
        .def("get_tensor", &MooncakeStorePyWrapper::get_tensor, py::arg("key"),
             "Get a PyTorch tensor from the store")
        .def("put_tensor", &MooncakeStorePyWrapper::put_tensor, py::arg("key"),
             py::arg("tensor"), "Put a PyTorch tensor into the store")
        .def("batch_get_tensor", &MooncakeStorePyWrapper::batch_get_tensor,
             py::arg("keys"), "Get a batch of PyTorch tensors from the store")
        .def("batch_put_tensor", &MooncakeStorePyWrapper::batch_put_tensor,
             py::arg("keys"), py::arg("tensors_list"),
             "Put a batch of PyTorch tensors into the store")
        .def("pub_tensor", &MooncakeStorePyWrapper::pub_tensor, py::arg("key"),
             py::arg("tensor"), py::arg("config") = ReplicateConfig{},
             "Publish a PyTorch tensor with configurable replication settings")
        .def(
            "register_buffer",
            [](MooncakeStorePyWrapper &self, uintptr_t buffer_ptr,
               size_t size) {
                // Register memory buffer for RDMA operations
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                return self.store_->register_buffer(buffer, size);
            },
            py::arg("buffer_ptr"), py::arg("size"),
            "Register a memory buffer for direct access operations")
        .def(
            "unregister_buffer",
            [](MooncakeStorePyWrapper &self, uintptr_t buffer_ptr) {
                // Unregister memory buffer
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                return self.store_->unregister_buffer(buffer);
            },
            py::arg("buffer_ptr"),
            "Unregister a previously registered memory "
            "buffer for direct access operations")
        .def(
            "get_into",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               uintptr_t buffer_ptr, size_t size) {
                // Get data directly into user-provided buffer
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                if (self.use_dummy_client_) {
                    LOG(ERROR) << "get_into is not supported for dummy client "
                                  "now";
                    return (int64_t)-1;
                }
                return self.store_->get_into(key, buffer, size);
            },
            py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
            "Get object data directly into a pre-allocated buffer")
        .def(
            "batch_get_into",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<uintptr_t> &buffer_ptrs,
               const std::vector<size_t> &sizes) {
                std::vector<void *> buffers;
                buffers.reserve(buffer_ptrs.size());
                for (uintptr_t ptr : buffer_ptrs) {
                    buffers.push_back(reinterpret_cast<void *>(ptr));
                }
                py::gil_scoped_release release;
                return self.store_->batch_get_into(keys, buffers, sizes);
            },
            py::arg("keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
            "Get object data directly into pre-allocated buffers for "
            "multiple "
            "keys")
        .def(
            "put_from",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               uintptr_t buffer_ptr, size_t size,
               const ReplicateConfig &config = ReplicateConfig{}) {
                // Put data directly from user-provided buffer
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                if (self.use_dummy_client_) {
                    LOG(ERROR) << "put_from is not supported for dummy client "
                                  "now";
                    return -1;
                }
                return self.store_->put_from(key, buffer, size, config);
            },
            py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
            py::arg("config") = ReplicateConfig{},
            "Put object data directly from a pre-allocated buffer")
        .def(
            "put_from_with_metadata",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               uintptr_t buffer_ptr, uintptr_t metadata_buffer_ptr, size_t size,
               size_t metadata_size,
               const ReplicateConfig &config = ReplicateConfig{}) {
                // Put data directly from user-provided buffer with
                // metadata
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                void *metadata_buffer =
                    reinterpret_cast<void *>(metadata_buffer_ptr);
                py::gil_scoped_release release;
                if (self.use_dummy_client_) {
                    LOG(ERROR)
                        << "put_from_with_metadata is not supported for dummy "
                           "client now";
                    return -1;
                }
                return self.store_->put_from_with_metadata(
                    key, buffer, metadata_buffer, size, metadata_size, config);
            },
            py::arg("key"), py::arg("buffer_ptr"),
            py::arg("metadata_buffer_ptr"), py::arg("size"),
            py::arg("metadata_size"), py::arg("config") = ReplicateConfig{},
            "Put object data directly from a pre-allocated buffer with "
            "metadata")
        .def(
            "batch_put_from",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<uintptr_t> &buffer_ptrs,
               const std::vector<size_t> &sizes,
               const ReplicateConfig &config = ReplicateConfig{}) {
                std::vector<void *> buffers;
                buffers.reserve(buffer_ptrs.size());
                for (uintptr_t ptr : buffer_ptrs) {
                    buffers.push_back(reinterpret_cast<void *>(ptr));
                }
                py::gil_scoped_release release;
                return self.store_->batch_put_from(keys, buffers, sizes,
                                                   config);
            },
            py::arg("keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
            py::arg("config") = ReplicateConfig{},
            "Put object data directly from pre-allocated buffers for "
            "multiple "
            "keys")
        .def(
            "put",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               py::buffer buf,
               const ReplicateConfig &config = ReplicateConfig{}) {
                py::buffer_info info = buf.request(/*writable=*/false);
                py::gil_scoped_release release;
                return self.store_->put(
                    key,
                    std::span<const char>(static_cast<char *>(info.ptr),
                                          static_cast<size_t>(info.size)),
                    config);
            },
            py::arg("key"), py::arg("value"),
            py::arg("config") = ReplicateConfig{})
        .def(
            "put_parts",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               py::args parts,
               const ReplicateConfig &config = ReplicateConfig{}) {
                // 1) Python buffer → span
                std::vector<py::buffer_info> infos;
                std::vector<std::span<const char>> spans;
                infos.reserve(parts.size());
                spans.reserve(parts.size());

                for (auto &obj : parts) {
                    py::buffer buf = py::reinterpret_borrow<py::buffer>(obj);
                    infos.emplace_back(buf.request(false));
                    const auto &info = infos.back();
                    if (info.ndim != 1 || info.itemsize != 1)
                        throw std::runtime_error(
                            "parts must be 1-D bytes-like");

                    spans.emplace_back(static_cast<const char *>(info.ptr),
                                       static_cast<size_t>(info.size));
                }

                // 2) Call C++ function
                py::gil_scoped_release unlock;
                return self.store_->put_parts(key, spans, config);
            },
            py::arg("key"), py::arg("config") = ReplicateConfig{})
        .def(
            "put_batch",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<py::buffer> &buffers,
               const ReplicateConfig &config = ReplicateConfig{}) {
                // Convert pybuffers to spans without copying
                std::vector<py::buffer_info> infos;
                std::vector<std::span<const char>> spans;
                infos.reserve(buffers.size());
                spans.reserve(buffers.size());

                for (const auto &buf : buffers) {
                    infos.emplace_back(buf.request(/*writable=*/false));
                    const auto &info = infos.back();
                    spans.emplace_back(static_cast<const char *>(info.ptr),
                                       static_cast<size_t>(info.size));
                }

                py::gil_scoped_release release;
                return self.store_->put_batch(keys, spans, config);
            },
            py::arg("keys"), py::arg("values"),
            py::arg("config") = ReplicateConfig{})
        .def("get_hostname",
             [](MooncakeStorePyWrapper &self) {
                 return self.store_->get_hostname();
             })
        .def(
            "batch_put_from_multi_buffers",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<std::vector<uintptr_t>> &all_buffer_ptrs,
               const std::vector<std::vector<size_t>> &all_sizes,
               const ReplicateConfig &config = ReplicateConfig{}) {
                py::gil_scoped_release release;
                if (self.use_dummy_client_) {
                    LOG(ERROR)
                        << "batch_put_from_multi_buffers is not supported for "
                           "dummy client now";
                    return std::vector<int>{};
                }
                return self.store_->batch_put_from_multi_buffers(
                    keys, CastAddrs2Ptrs(all_buffer_ptrs), all_sizes, config);
            },
            py::arg("keys"), py::arg("all_buffer_ptrs"), py::arg("all_sizes"),
            py::arg("config") = ReplicateConfig{},
            "Put object data directly from multiple pre-allocated buffers for "
            "multiple "
            "keys")
        .def(
            "batch_get_into_multi_buffers",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<std::vector<uintptr_t>> &all_buffer_ptrs,
               const std::vector<std::vector<size_t>> &all_sizes,
               bool prefer_alloc_in_same_node = false) {
                py::gil_scoped_release release;
                if (self.use_dummy_client_) {
                    LOG(ERROR)
                        << "batch_get_into_multi_buffers is not supported for "
                           "dummy client now";
                    return std::vector<int>{};
                }
                return self.store_->batch_get_into_multi_buffers(
                    keys, CastAddrs2Ptrs(all_buffer_ptrs), all_sizes,
                    prefer_alloc_in_same_node);
            },
            py::arg("keys"), py::arg("all_buffer_ptrs"), py::arg("all_sizes"),
            py::arg("prefer_alloc_in_same_node") = false,
            "Get object data directly into multiple pre-allocated buffers for "
            "multiple "
            "keys")
        .def(
            "get_replica_desc",
            [](MooncakeStorePyWrapper &self, const std::string &key) {
                py::gil_scoped_release release;
                return self.store_->get_replica_desc(key);
            },
            py::arg("key"))
        .def(
            "batch_get_replica_desc",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys) {
                py::gil_scoped_release release;
                return self.store_->batch_get_replica_desc(keys);
            },
            py::arg("keys"));

    // Expose NUMA binding as a module-level function (no self required)
    m.def(
        "bind_to_numa_node",
        [](int node) {
            if (numa_available() < 0) {
                LOG(WARNING)
                    << "NUMA is not available on this system; binding skipped";
                return;
            }

            int max_node = numa_max_node();
            if (node < 0 || node > max_node) {
                LOG(WARNING) << "Invalid NUMA node: " << node
                             << ". Valid range: 0-" << max_node;
            }

            if (numa_run_on_node(node) != 0) {
                LOG(WARNING) << "numa_run_on_node failed for node " << node;
            }

            // Prefer this NUMA node for future allocations but allow fallback
            numa_set_bind_policy(0);  // non-strict binding
            numa_set_preferred(node);
        },
        py::arg("node"),
        "Bind the current thread and memory allocation preference to the "
        "specified NUMA node");
}

}  // namespace mooncake
