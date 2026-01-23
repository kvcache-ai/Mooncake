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

struct PyTensorInfo {
    uintptr_t data_ptr;
    size_t tensor_size;
    TensorMetadata metadata;

    // Check validity
    bool valid() const { return tensor_size > 0; }
};

PyTensorInfo extract_tensor_info(const py::object &tensor,
                                 const std::string &key_name = "") {
    PyTensorInfo info = {
        0,
        0,
        {},
    };

    if (!(tensor.attr("__class__")
              .attr("__name__")
              .cast<std::string>()
              .find("Tensor") != std::string::npos)) {
        LOG(ERROR) << "Input " << (key_name.empty() ? "" : "for " + key_name)
                   << " is not a PyTorch tensor";
        return info;
    }

    try {
        info.data_ptr = tensor.attr("data_ptr")().cast<uintptr_t>();
        size_t numel = tensor.attr("numel")().cast<size_t>();
        size_t element_size = tensor.attr("element_size")().cast<size_t>();
        info.tensor_size = numel * element_size;

        pybind11::object shape_obj = tensor.attr("shape");
        pybind11::object dtype_obj = tensor.attr("dtype");

        TensorDtype dtype_enum = get_tensor_dtype(dtype_obj);
        if (dtype_enum == TensorDtype::UNKNOWN) {
            LOG(ERROR) << "Unsupported tensor dtype"
                       << (key_name.empty() ? "" : " for " + key_name);
            return {0, 0, {}};
        }

        pybind11::tuple shape_tuple =
            pybind11::cast<pybind11::tuple>(shape_obj);
        int32_t ndim = static_cast<int32_t>(shape_tuple.size());

        if (ndim > 4) {
            LOG(ERROR) << "Tensor has more than 4 dimensions: " << ndim;
            return {0, 0, {}};
        }

        info.metadata.dtype = static_cast<int32_t>(dtype_enum);
        info.metadata.ndim = ndim;

        for (int i = 0; i < 4; i++) {
            info.metadata.shape[i] =
                (i < ndim) ? shape_tuple[i].cast<int64_t>() : -1;
        }
    } catch (const std::exception &e) {
        LOG(ERROR) << "Error extracting tensor info: " << e.what();
        return {0, 0, {}};
    }

    return info;
}

pybind11::object buffer_to_tensor(BufferHandle *buffer_handle, char *usr_buffer,
                                  int64_t data_length) {
    if (!buffer_handle && !usr_buffer) return pybind11::none();
    if (buffer_handle && usr_buffer) return pybind11::none();

    bool take_ownership = !!buffer_handle;
    size_t total_length;
    char *exported_data;
    if (take_ownership) {
        total_length = buffer_handle->size();
        if (total_length <= sizeof(TensorMetadata)) {
            LOG(ERROR) << "Invalid data format: insufficient data for metadata";
            return pybind11::none();
        }

        // Allocate memory to copy data out (array_creators usually take
        // ownership or we need a clean buffer)
        exported_data = new char[total_length];
        if (!exported_data) return pybind11::none();

        memcpy(exported_data, buffer_handle->ptr(), total_length);
    } else {
        exported_data = usr_buffer;
        if (data_length < 0) {
            LOG(ERROR) << "Get tensor into failed with error code: "
                       << data_length;
            return pybind11::none();
        }
        total_length = static_cast<size_t>(data_length);
        if (total_length <= sizeof(TensorMetadata)) {
            LOG(ERROR) << "Invalid data format: insufficient data for metadata";
            return pybind11::none();
        }
    }
    TensorMetadata metadata;
    memcpy(&metadata, exported_data, sizeof(TensorMetadata));

    if (metadata.ndim < 0 || metadata.ndim > 4) {
        if (take_ownership) {
            delete[] exported_data;
        }
        LOG(ERROR) << "Invalid tensor metadata: ndim=" << metadata.ndim;
        return pybind11::none();
    }

    TensorDtype dtype_enum = static_cast<TensorDtype>(metadata.dtype);
    size_t tensor_size = total_length - sizeof(TensorMetadata);

    if (tensor_size == 0 || dtype_enum == TensorDtype::UNKNOWN) {
        if (take_ownership) {
            delete[] exported_data;
        }
        LOG(ERROR) << "Invalid tensor data or unknown dtype";
        return pybind11::none();
    }

    int dtype_index = static_cast<int>(dtype_enum);
    if (dtype_index < 0 ||
        dtype_index >= static_cast<int>(array_creators.size())) {
        if (take_ownership) {
            delete[] exported_data;
        }
        LOG(ERROR) << "Unsupported dtype enum: " << dtype_index;
        return pybind11::none();
    }

    try {
        // Construct numpy array
        bool take_ownership = !!buffer_handle;
        py::object np_array = array_creators[dtype_index](
            exported_data, sizeof(TensorMetadata), tensor_size, take_ownership);

        // Reshape
        if (metadata.ndim > 0) {
            std::vector<uint64_t> shape_vec;
            for (int i = 0; i < metadata.ndim; i++) {
                shape_vec.push_back(metadata.shape[i]);
            }
            py::tuple shape_tuple = py::cast(shape_vec);
            np_array = np_array.attr("reshape")(shape_tuple);
        }

        // Convert to torch tensor
        pybind11::object tensor = torch_module().attr("from_numpy")(np_array);

        // Handle BFloat16/Float16 view checks
        if (dtype_enum == TensorDtype::BFLOAT16) {
            tensor = tensor.attr("view")(torch_module().attr("bfloat16"));
        } else if (dtype_enum == TensorDtype::FLOAT16) {
            tensor = tensor.attr("view")(torch_module().attr("float16"));
        } else if (dtype_enum == TensorDtype::FLOAT8_E4M3) {
            tensor = tensor.attr("view")(torch_module().attr("float8_e4m3fn"));
        } else if (dtype_enum == TensorDtype::FLOAT8_E5M2) {
            tensor = tensor.attr("view")(torch_module().attr("float8_e5m2"));
        }
        return tensor;

    } catch (const std::exception &e) {
        LOG(ERROR) << "Failed to convert buffer to tensor: " << e.what();
        if (take_ownership) {
            delete[] exported_data;
        }
        return pybind11::none();
    }
}

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

    std::string get_tp_key_name(const std::string &base_key, int rank) {
        return base_key + "_tp_" + std::to_string(rank);
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

    pybind11::object get_tensor_with_tp(const std::string &key, int tp_rank = 0,
                                        int tp_size = 1, int split_dim = 0) {
        if (tp_size <= 1) return get_tensor(key);
        return get_tensor(get_tp_key_name(key, tp_rank));
    }

    pybind11::list batch_get_tensor_with_tp(
        const std::vector<std::string> &base_keys, int tp_rank = 0,
        int tp_size = 1) {
        if (tp_size <= 1) return batch_get_tensor(base_keys);

        std::vector<std::string> shard_keys;
        shard_keys.reserve(base_keys.size());
        for (const auto &key : base_keys) {
            shard_keys.push_back(get_tp_key_name(key, tp_rank));
        }
        return batch_get_tensor(shard_keys);
    }

    pybind11::object get_tensor(const std::string &key) {
        if (!is_client_initialized() || use_dummy_client_) {
            LOG(ERROR) << "Client not initialized or Dummy client not "
                          "supported for tensors";
            return pybind11::none();
        }

        std::shared_ptr<BufferHandle> buffer_handle;
        {
            py::gil_scoped_release release_gil;
            buffer_handle = store_->get_buffer(key);
        }
        // Metadata parsing must happen with GIL held
        return buffer_to_tensor(buffer_handle.get(), NULL, 0);
    }

    pybind11::list batch_get_tensor(const std::vector<std::string> &keys) {
        if (!is_client_initialized() || use_dummy_client_) {
            LOG(ERROR) << "Client not initialized or Dummy client not "
                          "supported for tensors";
            py::list empty;
            for (size_t i = 0; i < keys.size(); ++i) empty.append(py::none());
            return empty;
        }

        std::vector<std::shared_ptr<BufferHandle>> buffer_handles;
        {
            py::gil_scoped_release release_gil;
            buffer_handles = store_->batch_get_buffer(keys);
        }

        py::list results_list;
        for (const auto &handle : buffer_handles) {
            results_list.append(buffer_to_tensor(handle.get(), NULL, 0));
        }
        return results_list;
    }

    pybind11::object get_tensor_into(const std::string &key,
                                     uintptr_t buffer_ptr, size_t size) {
        char *buffer = reinterpret_cast<char *>(buffer_ptr);
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return pybind11::none();
        }

        if (use_dummy_client_) {
            LOG(ERROR) << "get_tensor is not supported for dummy client now";
            return pybind11::none();
        }

        int64_t total_length;
        {
            py::gil_scoped_release release_gil;
            total_length = store_->get_into(key, buffer, size);
        }

        return buffer_to_tensor(NULL, buffer, total_length);
    }

    pybind11::list batch_get_tensor_into(
        const std::vector<std::string> &keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes) {
        std::vector<void *> buffers;
        buffers.reserve(buffer_ptrs.size());
        for (uintptr_t ptr : buffer_ptrs) {
            buffers.push_back(reinterpret_cast<void *>(ptr));
        }

        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            py::list empty_list;
            for (size_t i = 0; i < keys.size(); ++i) {
                empty_list.append(to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            return empty_list;
        }

        if (use_dummy_client_) {
            LOG(ERROR) << "batch_get_tensor is not supported for dummy client "
                          "now";
            py::list empty_list;
            for (size_t i = 0; i < keys.size(); ++i) {
                empty_list.append(to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            return empty_list;
        }

        // Phase 1: Batch Get Buffers (GIL Released)
        std::vector<int64_t> total_lengths;
        {
            py::gil_scoped_release release_gil;
            // This internal call already handles logging for query failures
            total_lengths = store_->batch_get_into(keys, buffers, sizes);
        }

        if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
            LOG(ERROR) << "Size mismatch: keys, buffer_ptrs, and sizes must "
                          "have the same length";
            py::list empty_list;
            for (size_t i = 0; i < keys.size(); ++i) {
                empty_list.append(to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            return empty_list;
        }

        py::list results_list;
        for (size_t i = 0; i < total_lengths.size(); i++) {
            const auto &buffer = buffers[i];
            const auto total_length = total_lengths[i];
            results_list.append(buffer_to_tensor(
                NULL, static_cast<char *>(buffer), total_length));
        }
        return results_list;
    }

    pybind11::object get_tensor_with_tp_into(const std::string &key,
                                             uintptr_t buffer_ptr, size_t size,
                                             int tp_rank = 0, int tp_size = 1,
                                             int split_dim = 0) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return pybind11::none();
        }

        if (use_dummy_client_) {
            LOG(ERROR)
                << "get_tensor_into is not supported for dummy client now";
            return pybind11::none();
        }

        if (tp_size <= 1) {
            return get_tensor_into(key, buffer_ptr, size);
        }

        // Construct the specific key for this rank: e.g., "key_tp_0"
        std::string tp_key = get_tp_key_name(key, tp_rank);

        // Delegate to the standard get_tensor_into method
        return get_tensor_into(tp_key, buffer_ptr, size);
    }

    pybind11::list batch_get_tensor_with_tp_into(
        const std::vector<std::string> &base_keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes, int tp_rank = 0, int tp_size = 1) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            py::list empty_list;
            for (size_t i = 0; i < base_keys.size(); ++i) {
                empty_list.append(py::none());
            }
            return empty_list;
        }

        if (use_dummy_client_) {
            LOG(ERROR) << "batch_get_tensor_with_tp_into is not supported for "
                          "dummy client";
            py::list empty_list;
            for (size_t i = 0; i < base_keys.size(); ++i) {
                empty_list.append(py::none());
            }
            return empty_list;
        }

        // If tp_size is 1, it's just a normal batch_get_tensor_into
        if (tp_size <= 1) {
            return batch_get_tensor_into(base_keys, buffer_ptrs, sizes);
        }

        // Generate the specific shard keys for the given tp_rank
        std::vector<std::string> shard_keys;
        shard_keys.reserve(base_keys.size());
        for (const auto &key : base_keys) {
            shard_keys.push_back(get_tp_key_name(key, tp_rank));
        }

        // Use the existing batch_get_tensor_into to fetch all shards at once
        return batch_get_tensor_into(shard_keys, buffer_ptrs, sizes);
    }

    int put_tensor_impl(const std::string &key, pybind11::object tensor,
                        const ReplicateConfig &config) {
        // Validation & Metadata extraction (GIL Held)
        auto info = extract_tensor_info(tensor, key);
        if (!info.valid()) return to_py_ret(ErrorCode::INVALID_PARAMS);

        // Prepare spans
        std::vector<std::span<const char>> values;
        values.emplace_back(reinterpret_cast<const char *>(&info.metadata),
                            sizeof(TensorMetadata));
        values.emplace_back(reinterpret_cast<const char *>(info.data_ptr),
                            info.tensor_size);

        // Store (GIL Released)
        py::gil_scoped_release release_gil;
        int ret = store_->put_parts(key, values, config);
        if (ret != 0)
            LOG(ERROR) << "put_parts failed for key " << key << " with code "
                       << ret;
        return ret;
    }

    int put_tensor(const std::string &key, pybind11::object tensor) {
        if (!is_client_initialized() || use_dummy_client_) {
            LOG(ERROR) << "Client not initialized or Dummy client not "
                          "supported for tensors";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return put_tensor_impl(key, tensor,
                               ReplicateConfig{});  // Default config
    }

    int put_tensor_with_tp_impl(
        const std::string &key, pybind11::object tensor,
        const ReplicateConfig &config = ReplicateConfig{}, int tp_rank = 0,
        int tp_size = 1, int split_dim = 0) {
        try {
            py::tuple chunks =
                tensor.attr("chunk")(tp_size, split_dim).cast<py::tuple>();
            if (static_cast<int>(chunks.size()) != tp_size) {
                LOG(ERROR) << "Chunking failed: got " << chunks.size()
                           << " chunks, expected " << tp_size;
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }

            for (int rank = 0; rank < tp_size; ++rank) {
                pybind11::object chunk = chunks[rank].attr("contiguous")();
                std::string tp_key = get_tp_key_name(key, rank);

                int ret = put_tensor_impl(tp_key, chunk, config);
                if (ret != 0) return ret;
            }
            return 0;

        } catch (const std::exception &e) {
            LOG(ERROR) << "Failed to put tensor with tp: " << e.what();
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
    }

    int put_tensor_with_tp(const std::string &key, pybind11::object tensor,
                           int tp_rank = 0, int tp_size = 1,
                           int split_dim = 0) {
        if (!is_client_initialized() || use_dummy_client_) {
            LOG(ERROR) << "Client not initialized or Dummy client not "
                          "supported for tensors";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (tp_size <= 1) return put_tensor(key, tensor);

        return put_tensor_with_tp_impl(key, tensor, ReplicateConfig{}, tp_rank,
                                       tp_size, split_dim);
    }

    std::vector<int> batch_put_tensor_impl(
        const std::vector<std::string> &keys,
        const pybind11::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}) {
        std::vector<PyTensorInfo> infos(keys.size());
        std::vector<int> results(keys.size(), 0);

        // 1. Extract Metadata (GIL Held)
        for (size_t i = 0; i < keys.size(); ++i) {
            infos[i] = extract_tensor_info(tensors_list[i], keys[i]);
            if (!infos[i].valid())
                results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        // 2. Prepare Buffers and Execute (GIL Released)
        {
            py::gil_scoped_release release_gil;

            // Temporary containers for the batch operation
            std::vector<std::string> valid_keys;
            std::vector<void *> buffer_ptrs;
            std::vector<size_t> buffer_sizes;
            std::vector<size_t> original_indices;  // Map back to results

            // Note: In batch mode, we need contiguous memory for Metadata +
            // Data.
            std::vector<std::unique_ptr<BufferHandle>> temp_allocations;

            for (size_t i = 0; i < infos.size(); ++i) {
                if (!infos[i].valid()) continue;

                size_t total_size =
                    sizeof(TensorMetadata) + infos[i].tensor_size;
                auto alloc_result =
                    store_->client_buffer_allocator_->allocate(total_size);

                if (!alloc_result) {
                    LOG(ERROR)
                        << "Failed to allocate buffer for key: " << keys[i];
                    results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                // Copy Metadata & Data
                char *dst = static_cast<char *>(alloc_result->ptr());
                memcpy(dst, &infos[i].metadata, sizeof(TensorMetadata));
                memcpy(dst + sizeof(TensorMetadata),
                       reinterpret_cast<void *>(infos[i].data_ptr),
                       infos[i].tensor_size);

                valid_keys.push_back(keys[i]);
                buffer_ptrs.push_back(alloc_result->ptr());
                buffer_sizes.push_back(total_size);
                original_indices.push_back(i);

                // Transfer ownership to temp_allocations so it survives until
                // batch_put_from returns
                temp_allocations.push_back(
                    std::make_unique<BufferHandle>(std::move(*alloc_result)));
            }

            if (!valid_keys.empty()) {
                std::vector<int> op_results = store_->batch_put_from(
                    valid_keys, buffer_ptrs, buffer_sizes, config);
                for (size_t i = 0; i < op_results.size(); ++i) {
                    results[original_indices[i]] = op_results[i];
                }
            }
        }

        return results;
    }

    std::vector<int> batch_put_tensor(const std::vector<std::string> &keys,
                                      const pybind11::list &tensors_list) {
        if (!is_client_initialized() || use_dummy_client_)
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));

        if (keys.size() != tensors_list.size() || keys.empty()) {
            if (!keys.empty()) LOG(ERROR) << "Size mismatch in batch_put";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        return batch_put_tensor_impl(keys, tensors_list, ReplicateConfig{});
    }

    std::vector<int> batch_put_tensor_with_tp_impl(
        const std::vector<std::string> &base_keys,
        const pybind11::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}, int tp_rank = 0,
        int tp_size = 1, int split_dim = 0) {
        std::vector<std::string> all_chunk_keys;
        py::list all_chunks_list;
        std::vector<size_t> processed_indices;
        std::vector<int> final_results(base_keys.size(), 0);

        try {
            // Chunking phase (GIL Held)
            for (size_t i = 0; i < base_keys.size(); ++i) {
                py::object tensor = tensors_list[i];
                // Quick validation
                if (tensor.is_none() ||
                    !tensor.attr("shape").cast<py::tuple>()) {
                    final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                py::tuple chunks =
                    tensor.attr("chunk")(tp_size, split_dim).cast<py::tuple>();
                if (static_cast<int>(chunks.size()) != tp_size) {
                    final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                processed_indices.push_back(i);
                for (int rank = 0; rank < tp_size; ++rank) {
                    all_chunk_keys.push_back(
                        get_tp_key_name(base_keys[i], rank));
                    all_chunks_list.append(chunks[rank].attr(
                        "contiguous")());  // Ensure contiguous here
                }
            }

            if (all_chunk_keys.empty()) return final_results;

            // Reuse the standard batch_put implementation
            std::vector<int> chunk_results =
                batch_put_tensor_impl(all_chunk_keys, all_chunks_list, config);

            // Aggregate results
            for (size_t i = 0; i < processed_indices.size(); ++i) {
                size_t original_idx = processed_indices[i];
                for (int j = 0; j < tp_size; ++j) {
                    int res = chunk_results[i * tp_size + j];
                    if (res != 0) {
                        final_results[original_idx] = res;  // First error wins
                        break;
                    }
                }
            }

        } catch (const std::exception &e) {
            LOG(ERROR) << "Batch put with TP failed: " << e.what();
        }

        return final_results;
    }

    std::vector<int> batch_put_tensor_with_tp(
        const std::vector<std::string> &base_keys,
        const pybind11::list &tensors_list, int tp_rank = 0, int tp_size = 1,
        int split_dim = 0) {
        if (tp_size <= 1) return batch_put_tensor(base_keys, tensors_list);
        if (!is_client_initialized() || use_dummy_client_)
            return std::vector<int>(base_keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));

        if (base_keys.size() != tensors_list.size() || base_keys.empty()) {
            if (!base_keys.empty()) LOG(ERROR) << "Size mismatch in batch_put";
            return std::vector<int>(base_keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        return batch_put_tensor_with_tp_impl(base_keys, tensors_list,
                                             ReplicateConfig{}, tp_rank,
                                             tp_size, split_dim);
    }

    int validate_replicate_config(
        const ReplicateConfig &config = ReplicateConfig{}) {
        if (!config.preferred_segments.empty() &&
            config.preferred_segments.size() != config.replica_num) {
            LOG(ERROR) << "Preferred segments size ("
                       << config.preferred_segments.size()
                       << ") must match replica_num (" << config.replica_num
                       << ")";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return 0;
    }
    int pub_tensor(const std::string &key, pybind11::object tensor,
                   const ReplicateConfig &config = ReplicateConfig{}) {
        if (!is_client_initialized() || use_dummy_client_) {
            LOG(ERROR) << "Client not initialized or Dummy client not "
                          "supported for tensors";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        int validate_result = validate_replicate_config(config);
        if (validate_result) return validate_result;

        return put_tensor_impl(key, tensor, config);
    }

    int pub_tensor_with_tp(const std::string &key, pybind11::object tensor,
                           const ReplicateConfig &config = ReplicateConfig{},
                           int tp_rank = 0, int tp_size = 1,
                           int split_dim = 0) {
        if (!is_client_initialized() || use_dummy_client_) {
            LOG(ERROR) << "Client not initialized or Dummy client not "
                          "supported for tensors";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        int validate_result = validate_replicate_config(config);
        if (validate_result) return validate_result;

        if (tp_size <= 1) return pub_tensor(key, tensor, config);

        return put_tensor_with_tp_impl(key, tensor, config, tp_rank, tp_size,
                                       split_dim);
    }

    std::vector<int> batch_pub_tensor(
        const std::vector<std::string> &keys,
        const pybind11::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}) {
        if (!is_client_initialized() || use_dummy_client_)
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));

        if (keys.size() != tensors_list.size() || keys.empty()) {
            if (!keys.empty()) LOG(ERROR) << "Size mismatch in batch_put";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        int validate_result = validate_replicate_config(config);
        if (validate_result) {
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        return batch_put_tensor_impl(keys, tensors_list, config);
    }

    std::vector<int> batch_pub_tensor_with_tp(
        const std::vector<std::string> &base_keys,
        const pybind11::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}, int tp_rank = 0,
        int tp_size = 1, int split_dim = 0) {
        if (tp_size <= 1)
            return batch_pub_tensor(base_keys, tensors_list, config);
        if (!is_client_initialized() || use_dummy_client_)
            return std::vector<int>(base_keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));

        if (base_keys.size() != tensors_list.size() || base_keys.empty()) {
            if (!base_keys.empty()) LOG(ERROR) << "Size mismatch in batch_put";
            return std::vector<int>(base_keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        int validate_result = validate_replicate_config(config);
        if (validate_result) {
            return std::vector<int>(base_keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        return batch_put_tensor_with_tp_impl(base_keys, tensors_list, config,
                                             tp_rank, tp_size, split_dim);
    }
};

class MooncakeHostMemAllocatorPyWrapper {
   public:
    // Only support ShmHelper for now
    ShmHelper *shm_helper_ = nullptr;

    MooncakeHostMemAllocatorPyWrapper() {
        shm_helper_ = ShmHelper::getInstance();
    }
    ~MooncakeHostMemAllocatorPyWrapper() { shm_helper_ = nullptr; }
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

    py::enum_<TaskType>(m, "TaskType")
        .value("REPLICA_COPY", TaskType::REPLICA_COPY)
        .value("REPLICA_MOVE", TaskType::REPLICA_MOVE)
        .export_values();

    py::enum_<TaskStatus>(m, "TaskStatus")
        .value("PENDING", TaskStatus::PENDING)
        .value("PROCESSING", TaskStatus::PROCESSING)
        .value("SUCCESS", TaskStatus::SUCCESS)
        .value("FAILED", TaskStatus::FAILED)
        .export_values();

    py::class_<QueryTaskResponse>(m, "QueryTaskResponse",
                                  "Response structure for QueryTask operation.")
        .def(py::init<>())
        .def_readonly("id", &QueryTaskResponse::id)
        .def_readonly("type", &QueryTaskResponse::type)
        .def_readonly("status", &QueryTaskResponse::status)
        .def_readonly("created_at_ms_epoch",
                      &QueryTaskResponse::created_at_ms_epoch)
        .def_readonly("last_updated_at_ms_epoch",
                      &QueryTaskResponse::last_updated_at_ms_epoch)
        .def_readonly("assigned_client", &QueryTaskResponse::assigned_client)
        .def_readonly("message", &QueryTaskResponse::message)
        .def("__repr__", [](const QueryTaskResponse &self) {
            std::ostringstream oss;
            oss << "QueryTaskResponse(id=" << self.id
                << ", type=" << static_cast<int>(self.type)
                << ", status=" << static_cast<int>(self.status)
                << ", assigned_client=" << self.assigned_client << ")";
            return oss.str();
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

    py::class_<MooncakeHostMemAllocatorPyWrapper>(m, "MooncakeHostMemAllocator")
        .def(py::init<>())
        .def("alloc",
             [](MooncakeHostMemAllocatorPyWrapper &self, size_t size) {
                 py::gil_scoped_release release;
                 void *ptr = self.shm_helper_->allocate(size);
                 return reinterpret_cast<uintptr_t>(ptr);
             })
        .def("free",
             [](MooncakeHostMemAllocatorPyWrapper &self, uintptr_t ptr) {
                 py::gil_scoped_release release;
                 return self.shm_helper_->free(reinterpret_cast<void *>(ptr));
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
        .def(
            "remove",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               bool force) {
                py::gil_scoped_release release;
                return self.store_->remove(key, force);
            },
            py::arg("key"), py::arg("force") = false,
            "Remove an object from the store. If force=True, skip lease and "
            "replication task checks.")
        .def(
            "remove_by_regex",
            [](MooncakeStorePyWrapper &self, const std::string &str,
               bool force) {
                py::gil_scoped_release release;
                return self.store_->removeByRegex(str, force);
            },
            py::arg("regex_pattern"), py::arg("force") = false,
            "Removes objects from the store whose keys match the given "
            "regular expression. If force=True, skip lease and replication "
            "task checks.")
        .def(
            "remove_all",
            [](MooncakeStorePyWrapper &self, bool force) {
                py::gil_scoped_release release;
                return self.store_->removeAll(force);
            },
            py::arg("force") = false,
            "Remove all objects from the store. If force=True, skip lease "
            "and replication task checks.")
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
        .def(
            "get_tensor_with_tp", &MooncakeStorePyWrapper::get_tensor_with_tp,
            py::arg("key"), py::arg("tp_rank") = 0, py::arg("tp_size") = 1,
            py::arg("split_dim") = 0,
            "Get a PyTorch tensor from the store, optionally sliced for Tensor "
            "Parallelism.\n"
            "Args:\n"
            "  key: The key of the tensor.\n"
            "  tp_rank: The current tensor parallel rank (default 0).\n"
            "  tp_size: The total tensor parallel size (default 1).\n"
            "  split_dim: The dimension to split the tensor along (default 0).")
        .def("batch_get_tensor_with_tp",
             &MooncakeStorePyWrapper::batch_get_tensor_with_tp,
             py::arg("base_keys"), py::arg("tp_rank") = 0,
             py::arg("tp_size") = 1,
             "Get a batch of PyTorch tensor shards from the store for a given "
             "Tensor Parallel rank.")
        .def("get_tensor", &MooncakeStorePyWrapper::get_tensor, py::arg("key"),
             "Get a PyTorch tensor from the store")
        .def("put_tensor_with_tp", &MooncakeStorePyWrapper::put_tensor_with_tp,
             py::arg("key"), py::arg("tensor"), py::arg("tp_rank") = 0,
             py::arg("tp_size") = 1, py::arg("split_dim") = 0,
             "Put a PyTorch tensor into the store, split into shards for "
             "tensor parallelism.\n"
             "The tensor is chunked immediately and stored as separate keys "
             "(e.g., key_tp_0).")
        .def("batch_put_tensor_with_tp",
             &MooncakeStorePyWrapper::batch_put_tensor_with_tp,
             py::arg("base_keys"), py::arg("tensors_list"),
             py::arg("tp_rank") = 0, py::arg("tp_size") = 1,
             py::arg("split_dim") = 0,
             "Put a batch of PyTorch tensors into the store, splitting each "
             "into shards for tensor parallelism.")
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
        .def("batch_pub_tensor", &MooncakeStorePyWrapper::batch_pub_tensor,
             py::arg("keys"), py::arg("tensors_list"),
             py::arg("config") = ReplicateConfig{},
             "Publish a batch of PyTorch tensors into the store with "
             "configurable replication settings")
        .def("pub_tensor_with_tp", &MooncakeStorePyWrapper::pub_tensor_with_tp,
             py::arg("key"), py::arg("tensor"),
             py::arg("config") = ReplicateConfig{}, py::arg("tp_rank") = 0,
             py::arg("tp_size") = 1, py::arg("split_dim") = 0,
             "Publish a PyTorch tensor into the store with configurable "
             "replication settings, split into shards for "
             "tensor parallelism.\n"
             "The tensor is chunked immediately and stored as separate keys "
             "(e.g., key_tp_0).")
        .def("batch_pub_tensor_with_tp",
             &MooncakeStorePyWrapper::batch_pub_tensor_with_tp,
             py::arg("base_keys"), py::arg("tensors_list"),
             py::arg("config") = ReplicateConfig{}, py::arg("tp_rank") = 0,
             py::arg("tp_size") = 1, py::arg("split_dim") = 0,
             "Publish a batch of PyTorch tensors into the store with "
             "configurable replication settings, splitting each "
             "into shards for tensor parallelism.")
        .def("get_tensor_into", &MooncakeStorePyWrapper::get_tensor_into,
             py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
             "Get tensor directly into a pre-allocated buffer")
        .def("batch_get_tensor_into",
             &MooncakeStorePyWrapper::batch_get_tensor_into, py::arg("keys"),
             py::arg("buffer_ptrs"), py::arg("sizes"),
             "Get tensors directly into pre-allocated buffers for "
             "multiple "
             "keys")
        .def(
            "get_tensor_with_tp_into",
            &MooncakeStorePyWrapper::get_tensor_with_tp_into, py::arg("key"),
            py::arg("buffer_ptr"), py::arg("size"), py::arg("tp_rank") = 0,
            py::arg("tp_size") = 1, py::arg("split_dim") = 0,
            "Get a PyTorch tensor from the store directly into a pre-allocated "
            "buffer, optionally sliced for Tensor Parallelism.\n"
            "Args:\n"
            "  key: The key of the tensor.\n"
            "  buffer_ptr: The buffer pointer pre-allocated for tensor.\n"
            "  size: The size of buffer.\n"
            "  tp_rank: The current tensor parallel rank (default 0).\n"
            "  tp_size: The total tensor parallel size (default 1).\n"
            "  split_dim: The dimension to split the tensor along (default 0).")
        .def(
            "batch_get_tensor_with_tp_into",
            &MooncakeStorePyWrapper::batch_get_tensor_with_tp_into,
            py::arg("base_keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
            py::arg("tp_rank") = 0, py::arg("tp_size") = 1,
            "Get a batch of PyTorch tensor shards from the store directly into "
            "pre-allocated buffers for a given Tensor Parallel rank.")
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
            py::arg("keys"))
        .def(
            "create_copy_task",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               const std::vector<std::string> &targets) -> py::tuple {
                py::gil_scoped_release release;
                auto result = self.store_->create_copy_task(key, targets);
                py::gil_scoped_acquire acquire;
                if (!result.has_value()) {
                    LOG(ERROR)
                        << "Copy failed for key: " << key
                        << ", error: " << static_cast<int>(result.error());
                    return py::make_tuple(UUID{0, 0}, toInt(result.error()));
                }
                return py::make_tuple(result.value(), 0);
            },
            py::arg("key"), py::arg("targets"),
            "Copy an object to target segments.\n\n"
            "Args:\n"
            "    key: Object key to copy.\n"
            "    targets: List of target segment names.\n\n"
            "Returns:\n"
            "    tuple[UUID, int]: (UUID of the copy task, error code: 0 if "
            "success, non-zero if failure)")
        .def(
            "create_move_task",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               const std::string &source,
               const std::string &target) -> py::tuple {
                py::gil_scoped_release release;
                auto result =
                    self.store_->create_move_task(key, source, target);
                py::gil_scoped_acquire acquire;
                if (!result.has_value()) {
                    LOG(ERROR)
                        << "Move failed for key: " << key
                        << ", error: " << static_cast<int>(result.error());
                    return py::make_tuple(UUID{0, 0}, toInt(result.error()));
                }
                return py::make_tuple(result.value(), 0);
            },
            py::arg("key"), py::arg("source"), py::arg("target"),
            "Move an object from source segment to target segment.\n\n"
            "Args:\n"
            "    key: Object key to move.\n"
            "    source: Source segment name.\n"
            "    target: Target segment name.\n\n"
            "Returns:\n"
            "    tuple[UUID, int]: (UUID of the move task, error code: 0 if "
            "success, non-zero if failure)")
        .def(
            "query_task",
            [](MooncakeStorePyWrapper &self, const UUID &task_id) -> py::tuple {
                py::gil_scoped_release release;
                auto result = self.store_->query_task(task_id);
                py::gil_scoped_acquire acquire;
                if (!result.has_value()) {
                    LOG(ERROR)
                        << "QueryTask failed for task_id: " << task_id
                        << ", error: " << static_cast<int>(result.error());
                    return py::make_tuple(py::none(), toInt(result.error()));
                }
                return py::make_tuple(result.value(), 0);
            },
            py::arg("task_id"),
            "Query the status of a task.\n\n"
            "Args:\n"
            "    task_id: UUID of the task to query.\n\n"
            "Returns:\n"
            "    tuple[QueryTaskResponse | None, int]: (QueryTaskResponse if "
            "success, error code: 0 if success, non-zero if failure)");

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
