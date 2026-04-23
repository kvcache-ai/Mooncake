#include <pybind11/gil.h>  // For GIL management
#include <pybind11/stl.h>
#include <numa.h>

#include <functional>
#include <numeric>
#include <unordered_map>
#include <unordered_set>

#include "pyclient.h"
#include "dummy_client.h"
#include "real_client.h"
#include "types.h"

#include <cstdlib>  // for atexit

#include "integration_utils.h"

namespace py = pybind11;

namespace mooncake {
namespace {

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
            static_cast<uint32_t>(TensorDtype::NR_DTYPES)) {
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
            if (shape[i] <= 0) {
                return false;  // Invalid dimension size
            }
        }

        return ValidateTensorMetadata(
            validated, validated.header.data_offset + tensor_size);
    }
};

TensorMetadata build_full_tensor_metadata(const py::handle &tensor,
                                          TensorDtype dtype_enum,
                                          size_t tensor_size);
std::optional<ParsedTensorMetadata> parse_tensor_metadata_from_buffer(
    BufferHandle *buffer_handle, char *usr_buffer, int64_t data_length,
    bool *take_ownership, char **exported_data, size_t *total_length);
std::pair<int64_t, int64_t> calculate_shard_range(int64_t dim_size, int rank,
                                                  int shard_count);

PyTensorInfo extract_tensor_info(const py::object &tensor,
                                 const std::string &key_name = "") {
    PyTensorInfo info = {
        0,
        0,
        {},
        py::none(),
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
        py::object contiguous_tensor = tensor.attr("contiguous")();
        info.owner = contiguous_tensor;
        info.data_ptr = contiguous_tensor.attr("data_ptr")().cast<uintptr_t>();
        size_t numel = contiguous_tensor.attr("numel")().cast<size_t>();
        size_t element_size =
            contiguous_tensor.attr("element_size")().cast<size_t>();
        info.tensor_size = numel * element_size;

        pybind11::object shape_obj = tensor.attr("shape");
        pybind11::object dtype_obj = tensor.attr("dtype");

        TensorDtype dtype_enum = get_tensor_dtype(dtype_obj);
        if (dtype_enum == TensorDtype::UNKNOWN) {
            LOG(ERROR) << "Unsupported tensor dtype"
                       << (key_name.empty() ? "" : " for " + key_name);
            return {0, 0, {}, py::none()};
        }

        pybind11::tuple shape_tuple =
            pybind11::cast<pybind11::tuple>(shape_obj);
        int32_t ndim = static_cast<int32_t>(shape_tuple.size());

        if (ndim > static_cast<int32_t>(kMaxTensorDims)) {
            LOG(ERROR) << "Tensor has more than " << kMaxTensorDims
                       << " dimensions: " << ndim;
            return {0, 0, {}, py::none()};
        }

        info.metadata =
            build_full_tensor_metadata(tensor, dtype_enum, info.tensor_size);
    } catch (const std::exception &e) {
        LOG(ERROR) << "Error extracting tensor info: " << e.what();
        return {0, 0, {}, py::none()};
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

    auto parsed = ParseTensorMetadata(exported_data, total_length);
    if (!parsed.has_value()) {
        if (take_ownership) {
            delete[] exported_data;
        }
        LOG(ERROR) << "Invalid tensor metadata";
        return pybind11::none();
    }

    int ndim = metadata.header.ndim;
    if (ndim < 0 || ndim > 4) {
        if (take_ownership) {
            delete[] exported_data;
        }
        LOG(ERROR) << "Invalid tensor metadata: ndim=" << ndim;
        return pybind11::none();
    }

    TensorDtype dtype_enum = static_cast<TensorDtype>(metadata.header.dtype);
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
        if (ndim > 0) {
            std::vector<uint64_t> shape_vec;
            for (int i = 0; i < ndim; i++) {
                shape_vec.push_back(metadata.layout.local_shape.dims[i]);
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

#include "store_py_internal.inc"

}  // namespace
// Python-specific wrapper functions that handle GIL and return pybind11 types
class MooncakeStorePyWrapper {
   public:
    std::shared_ptr<PyClient> store_{nullptr};
    bool use_dummy_client_{false};

    MooncakeStorePyWrapper() = default;

    // Helper to initialize real client and register it
    std::shared_ptr<RealClient> init_real_client() {
        auto &resource_tracker = ResourceTracker::getInstance();
        auto real_client = RealClient::create();
        use_dummy_client_ = false;
        store_ = real_client;
        resource_tracker.registerInstance(
            std::static_pointer_cast<PyClient>(store_));
        return real_client;
    }

    bool is_client_initialized() const {
        // Check if the store and client are initialized
        // Dummy client does not use client_ instance
        return (store_ && (use_dummy_client_ || store_->client_));
    }

    int health_check() {
        if (!store_) return HC_NOT_INITIALIZED;
        return store_->health_check();
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
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
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
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
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
        int tp_size = 1, int split_dim = 0,
        const std::vector<ParallelAxisSpec> &axes = {}) {
        return execute_tp_tensor_write_impl(
            key, tensor, config, tp_size, split_dim, axes,
            "Failed to put tensor with tp",
            [this](const std::string &shard_key, const PyTensorInfo &info,
                   const ReplicateConfig &write_config) {
                return put_tensor_impl(shard_key, info, write_config);
            });
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

    int put_tensor_with_parallelism(
        const std::string &key, pybind11::object tensor,
        const py::object &parallelism = py::none(),
        const ReplicateConfig &config = ReplicateConfig{},
        const py::object &writer_partition = py::none()) {
        auto request = resolve_parallelism_write_request(
            parallelism, writer_partition, "put_tensor_with_parallelism");
        if (!request.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return execute_put_tensor_with_parallelism_route(key, tensor, *request,
                                                         config);
    }

    std::vector<int> batch_put_tensor_impl(
        const std::vector<std::string> &keys,
        const std::vector<PyTensorInfo> &infos,
        const ReplicateConfig &config = ReplicateConfig{}) {
        return batch_write_tensor_impl(
            keys, infos, config, "put",
            [this, &config](const std::vector<std::string> &write_keys,
                            const std::vector<void *> &buffer_ptrs,
                            const std::vector<size_t> &buffer_sizes) {
                return store_->batch_put_from(write_keys, buffer_ptrs,
                                              buffer_sizes, config);
            });
    }

    std::vector<int> batch_put_tensor_impl(
        const std::vector<std::string> &keys,
        const pybind11::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}) {
        std::vector<PyTensorInfo> infos(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            infos[i] = extract_tensor_info(tensors_list[i], keys[i]);
        }
        return batch_put_tensor_impl(keys, infos, config);
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
        std::vector<int> final_results(base_keys.size(),
                                       to_py_ret(ErrorCode::INVALID_PARAMS));
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
                bool all_ok = true;
                for (int j = 0; j < tp_size; ++j) {
                    int res = chunk_results[i * tp_size + j];
                    if (res != 0) {
                        final_results[original_idx] = res;  // First error wins
                        all_ok = false;
                        break;
                    }
                }
                if (all_ok) {
                    final_results[original_idx] = 0;
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

    // Zero-copy put from pre-allocated buffer (layout:
    // [TensorObjectHeader+layout metadata][tensor data])
    int put_tensor_from(const std::string &key, uintptr_t buffer_ptr,
                        size_t size) {
        if (buffer_ptr == 0) {
            LOG(ERROR) << "Buffer pointer cannot be null";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        void *buffer = reinterpret_cast<void *>(buffer_ptr);
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (use_dummy_client_) {
            LOG(ERROR) << "put_tensor_from is not supported for dummy client";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (size <= sizeof(TensorMetadata)) {
            LOG(ERROR) << "Buffer size too small for tensor metadata";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        py::gil_scoped_release release_gil;
        return store_->put_from(key, buffer, size, ReplicateConfig{});
    }

    std::vector<int> batch_put_tensor_from(
        const std::vector<std::string> &keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (use_dummy_client_) {
            LOG(ERROR)
                << "batch_put_tensor_from is not supported for dummy client";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (keys.empty()) {
            return std::vector<int>();
        }
        if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
            LOG(ERROR) << "Size mismatch: keys, buffer_ptrs, and sizes must "
                          "have the same length";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        for (size_t i = 0; i < sizes.size(); ++i) {
            if (buffer_ptrs[i] == 0) {
                LOG(ERROR) << "Buffer pointer at index " << i
                           << " cannot be null";
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            if (sizes[i] <= sizeof(TensorMetadata)) {
                LOG(ERROR) << "Buffer size at index " << i
                           << " too small for tensor metadata";
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }
        }
        std::vector<void *> buffers;
        buffers.reserve(buffer_ptrs.size());
        for (uintptr_t ptr : buffer_ptrs) {
            buffers.push_back(reinterpret_cast<void *>(ptr));
        }
        py::gil_scoped_release release_gil;
        return store_->batch_put_from(keys, buffers, sizes, ReplicateConfig{});
    }

    pybind11::object get_tensor_with_writer_shard_full(
        const std::string &key, const std::string &context) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return pybind11::none();
        }
        if (use_dummy_client_) {
            LOG(ERROR) << context << ": dummy client is not supported";
            return pybind11::none();
        }

        auto reconstruction = load_writer_shard_reconstruction(key, context);
        if (!reconstruction.has_value()) {
            return get_tensor(key);
        }

        size_t total_tensor_numel = 1;
        for (auto dim : reconstruction->global_shape) {
            total_tensor_numel *= static_cast<size_t>(dim);
        }

        size_t element_size = 0;
        for (const auto &source : reconstruction->sources) {
            if (source.metadata.data_bytes == 0) {
                continue;
            }
            int64_t shard_numel = 1;
            const auto local_shape =
                TensorShapeToVector(source.metadata.metadata.layout.local_shape,
                                    source.metadata.metadata.header.ndim);
            for (auto dim : local_shape) {
                shard_numel *= dim;
            }
            if (shard_numel <= 0 ||
                source.metadata.data_bytes % static_cast<size_t>(shard_numel) !=
                    0) {
                LOG(ERROR) << context
                           << ": invalid writer shard tensor byte size";
                return py::none();
            }
            element_size =
                source.metadata.data_bytes / static_cast<size_t>(shard_numel);
            break;
        }

        const size_t total_length =
            sizeof(TensorMetadata) + total_tensor_numel * element_size;
        char *owned_buffer = new char[total_length];
        if (store_->register_buffer(owned_buffer, total_length) != 0) {
            LOG(ERROR) << context
                       << ": failed to register reconstruction buffer";
            delete[] owned_buffer;
            return py::none();
        }

        auto plan = build_full_tensor_into_plan_from_sources(
            reinterpret_cast<uintptr_t>(owned_buffer), total_length,
            reconstruction->sources, reconstruction->global_shape,
            reconstruction->split_dim, reconstruction->dtype, context,
            reconstruction->allow_empty_fragments);
        if (!plan.has_value()) {
            store_->unregister_buffer(owned_buffer);
            delete[] owned_buffer;
            return py::none();
        }

        auto success = execute_tensor_into_plan_transfers({*plan});
        if (success.empty() || !success[0]) {
            store_->unregister_buffer(owned_buffer);
            delete[] owned_buffer;
            return py::none();
        }

        return buffer_to_tensor(
            new BufferHandle(owned_buffer, total_length,
                             [this, owned_buffer]() {
                                 store_->unregister_buffer(owned_buffer);
                                 delete[] owned_buffer;
                             }),
            nullptr, 0);
    }

    pybind11::object get_tensor_with_tp_full(
        const std::string &key, int tp_rank, int tp_size, int split_dim,
        const std::string &context,
        const std::optional<TensorParallelismSpec> &parallelism =
            std::nullopt) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return pybind11::none();
        }
        if (use_dummy_client_) {
            LOG(ERROR) << context << ": dummy client is not supported";
            return pybind11::none();
        }
        ParallelAxisSpec axis{
            .kind = "tp",
            .rank = tp_rank,
            .size = tp_size,
            .split_dim = split_dim,
            .expert_id = std::nullopt,
            .stage_id = std::nullopt,
        };

        auto reconstruction =
            parallelism.has_value()
                ? load_parallelism_full_reconstruction_sources(
                      key, *parallelism, context)
                : load_tp_full_reconstruction_sources(key, axis, context);
        if (!reconstruction.has_value()) {
            return pybind11::none();
        }

        const size_t total_tensor_numel =
            std::accumulate(reconstruction->global_shape.begin(),
                            reconstruction->global_shape.end(),
                            static_cast<size_t>(1), std::multiplies<size_t>());
        size_t element_size = 0;
        for (const auto &source : reconstruction->sources) {
            if (source.metadata.data_bytes == 0) {
                continue;
            }
            int64_t shard_numel = 1;
            const auto local_shape =
                TensorShapeToVector(source.metadata.metadata.layout.local_shape,
                                    source.metadata.metadata.header.ndim);
            for (auto dim : local_shape) {
                shard_numel *= dim;
            }
            if (shard_numel <= 0 ||
                source.metadata.data_bytes % static_cast<size_t>(shard_numel) !=
                    0) {
                LOG(ERROR) << context << ": invalid shard tensor byte size";
                return pybind11::none();
            }
            element_size =
                source.metadata.data_bytes / static_cast<size_t>(shard_numel);
            break;
        }
        const size_t total_length =
            sizeof(TensorMetadata) + total_tensor_numel * element_size;

        char *owned_buffer = new char[total_length];
        if (store_->register_buffer(owned_buffer, total_length) != 0) {
            LOG(ERROR) << context
                       << ": failed to register reconstruction buffer";
            delete[] owned_buffer;
            return pybind11::none();
        }

        auto plan = build_full_tensor_into_plan_from_sources(
            reinterpret_cast<uintptr_t>(owned_buffer), total_length,
            reconstruction->sources, reconstruction->global_shape,
            reconstruction->split_dim, reconstruction->dtype, context,
            reconstruction->allow_empty_fragments);
        if (!plan.has_value()) {
            store_->unregister_buffer(owned_buffer);
            delete[] owned_buffer;
            return pybind11::none();
        }

        auto success = execute_tensor_into_plan_transfers({*plan});
        if (success.empty() || !success[0]) {
            store_->unregister_buffer(owned_buffer);
            delete[] owned_buffer;
            return py::none();
        }

        return buffer_to_tensor(
            new BufferHandle(owned_buffer, total_length,
                             [this, owned_buffer]() {
                                 store_->unregister_buffer(owned_buffer);
                                 delete[] owned_buffer;
                             }),
            nullptr, 0);
    }

    int put_tensor_impl(const std::string &key, const PyTensorInfo &info,
                        const ReplicateConfig &config) {
        if (!info.valid()) return to_py_ret(ErrorCode::INVALID_PARAMS);

        std::vector<std::span<const char>> values;
        values.emplace_back(reinterpret_cast<const char *>(&info.metadata),
                            info.metadata.header.data_offset);
        values.emplace_back(reinterpret_cast<const char *>(info.data_ptr),
                            info.tensor_size);

        py::gil_scoped_release release_gil;
        int ret = store_->put_parts(key, values, config);
        if (ret != 0)
            LOG(ERROR) << "put_parts failed for key " << key << " with code "
                       << ret;
        return ret;
    }

    template <typename ManifestWriteFn>
    int write_manifest_impl(const std::string &key,
                            const WriterShardManifest &manifest,
                            const char *operation_name,
                            ManifestWriteFn &&write_manifest) {
        std::span<const char> bytes(reinterpret_cast<const char *>(&manifest),
                                    sizeof(WriterShardManifest));
        py::gil_scoped_release release_gil;
        int ret = write_manifest(bytes);
        if (ret != 0) {
            LOG(ERROR) << operation_name << " manifest failed for key " << key
                       << " with code " << ret;
        }
        return ret;
    }

    template <typename BatchWriteFromFn>
    std::vector<int> batch_write_tensor_impl(
        const std::vector<std::string> &keys,
        const std::vector<PyTensorInfo> &infos, const ReplicateConfig &config,
        const char *operation_name, BatchWriteFromFn &&batch_write_from) {
        std::vector<int> results(keys.size(), 0);

        {
            py::gil_scoped_release release_gil;

            std::vector<std::string> valid_keys;
            std::vector<void *> buffer_ptrs;
            std::vector<size_t> buffer_sizes;
            std::vector<size_t> original_indices;
            std::vector<std::unique_ptr<BufferHandle>> temp_allocations;

            for (size_t i = 0; i < infos.size(); ++i) {
                if (!infos[i].valid()) {
                    results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                size_t total_size =
                    infos[i].metadata.header.data_offset + infos[i].tensor_size;
                auto alloc_result =
                    store_->client_buffer_allocator_->allocate(total_size);

                if (!alloc_result) {
                    LOG(ERROR) << "Failed to allocate buffer for "
                               << operation_name << " key: " << keys[i];
                    results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                char *dst = static_cast<char *>(alloc_result->ptr());
                std::memcpy(dst, &infos[i].metadata,
                            infos[i].metadata.header.data_offset);
                std::memcpy(dst + infos[i].metadata.header.data_offset,
                            reinterpret_cast<void *>(infos[i].data_ptr),
                            infos[i].tensor_size);

                valid_keys.push_back(keys[i]);
                buffer_ptrs.push_back(alloc_result->ptr());
                buffer_sizes.push_back(total_size);
                original_indices.push_back(i);
                temp_allocations.push_back(
                    std::make_unique<BufferHandle>(std::move(*alloc_result)));
            }

            if (!valid_keys.empty()) {
                std::vector<int> op_results =
                    batch_write_from(valid_keys, buffer_ptrs, buffer_sizes);
                for (size_t i = 0; i < op_results.size(); ++i) {
                    results[original_indices[i]] = op_results[i];
                }
            }
        }

        return results;
    }

    bool ensure_tensor_write_supported(const char *operation_name) const {
        if (!is_client_initialized() || use_dummy_client_) {
            LOG(ERROR) << operation_name
                       << ": client not initialized or dummy client not "
                          "supported for tensors";
            return false;
        }
        return true;
    }

    template <typename WriteFn>
    int execute_single_tensor_write(const char *operation_name,
                                    const ReplicateConfig &config,
                                    WriteFn &&write_fn) {
        if (!ensure_tensor_write_supported(operation_name)) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        int validate_result = validate_replicate_config(config);
        if (validate_result) {
            return validate_result;
        }
        return write_fn();
    }

    template <typename WriteFn>
    std::vector<int> execute_batch_tensor_write(
        const char *operation_name, const char *size_error_context,
        const std::vector<std::string> &keys, size_t value_count,
        const ReplicateConfig &config, WriteFn &&write_fn) {
        if (!ensure_tensor_write_supported(operation_name)) {
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (keys.size() != value_count || keys.empty()) {
            if (!keys.empty()) {
                LOG(ERROR) << size_error_context;
            }
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        int validate_result = validate_replicate_config(config);
        if (validate_result) {
            return std::vector<int>(keys.size(), validate_result);
        }
        return write_fn();
    }

    bool validate_tensor_object_buffers(
        const std::vector<std::string> &keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes, const char *size_error_context,
        const char *buffer_error_context) {
        if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
            LOG(ERROR) << size_error_context;
            return false;
        }
        for (size_t i = 0; i < sizes.size(); ++i) {
            if (!is_valid_tensor_object_buffer(
                    buffer_ptrs[i], sizes[i],
                    std::string(buffer_error_context) + " at index " +
                        std::to_string(i))) {
                return false;
            }
        }
        return true;
    }

    template <typename WriteFromFn>
    int execute_single_tensor_write_from(const char *operation_name,
                                         const std::string &key,
                                         uintptr_t buffer_ptr, size_t size,
                                         const ReplicateConfig &config,
                                         WriteFromFn &&write_from_fn) {
        if (!ensure_tensor_write_supported(operation_name)) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (!is_valid_tensor_object_buffer(buffer_ptr, size, operation_name)) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        int validate_result = validate_replicate_config(config);
        if (validate_result) {
            return validate_result;
        }
        return write_from_fn(key, reinterpret_cast<void *>(buffer_ptr), size,
                             config);
    }

    template <typename BatchWriteFromFn>
    std::vector<int> execute_batch_tensor_write_from(
        const char *operation_name, const char *size_error_context,
        const std::vector<std::string> &keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes, const ReplicateConfig &config,
        BatchWriteFromFn &&batch_write_from_fn) {
        if (!ensure_tensor_write_supported(operation_name)) {
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (keys.empty()) {
            return std::vector<int>();
        }
        if (!validate_tensor_object_buffers(keys, buffer_ptrs, sizes,
                                            size_error_context,
                                            "tensor object buffer")) {
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        int validate_result = validate_replicate_config(config);
        if (validate_result) {
            return std::vector<int>(keys.size(), validate_result);
        }
        std::vector<void *> buffers;
        buffers.reserve(buffer_ptrs.size());
        for (uintptr_t ptr : buffer_ptrs) {
            buffers.push_back(reinterpret_cast<void *>(ptr));
        }
        return batch_write_from_fn(keys, buffers, sizes, config);
    }

    template <typename DirectWriteFn, typename ParallelismWriteFn,
              typename WriterPartitionWriteFn>
    std::vector<int> execute_batch_parallelism_write_requests(
        const std::vector<std::string> &keys, size_t value_count,
        const py::object &parallelisms, const py::object &writer_partitions,
        const char *error_context, DirectWriteFn &&direct_write,
        ParallelismWriteFn &&parallelism_write,
        WriterPartitionWriteFn &&writer_partition_write) {
        if (!parallelisms.is_none() && !writer_partitions.is_none()) {
            LOG(ERROR)
                << error_context
                << ": writer_partitions cannot be combined with parallelisms";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (value_count != keys.size()) {
            LOG(ERROR) << error_context
                       << ": values and keys must have the same length";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (parallelisms.is_none() && writer_partitions.is_none()) {
            return direct_write();
        }

        std::vector<int> results(keys.size(),
                                 to_py_ret(ErrorCode::INVALID_PARAMS));
        if (!parallelisms.is_none()) {
            auto parallelism_list = validate_batch_parallelism_list(
                parallelisms, keys.size(), error_context);
            if (!parallelism_list.has_value()) {
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                results[i] = parallelism_write(i, (*parallelism_list)[i]);
            }
            return results;
        }

        auto writer_partition_list = validate_batch_writer_partition_list(
            writer_partitions, keys.size(), error_context);
        if (!writer_partition_list.has_value()) {
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        for (size_t i = 0; i < keys.size(); ++i) {
            results[i] = writer_partition_write(i, (*writer_partition_list)[i]);
        }
        return results;
    }

    template <typename DirectWriteFn, typename WriterShardWriteFn,
              typename LegacyTpWriteFn, typename TpParallelWriteFn,
              typename GenericShardWriteFn>
    int execute_parallelism_tensor_write_route(
        const std::string &key, pybind11::object tensor,
        const ResolvedParallelismWriteRequest &request,
        const ReplicateConfig &config, const char *error_context,
        DirectWriteFn &&direct_write, WriterShardWriteFn &&writer_shard_write,
        LegacyTpWriteFn &&legacy_tp_write,
        TpParallelWriteFn &&tp_parallel_write,
        GenericShardWriteFn &&generic_shard_write) {
        switch (request.route) {
            case ParallelismWriteStorageRoute::DIRECT_FULL_OBJECT:
                if (!ensure_tensor_write_supported(error_context)) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                if (int validate_result = validate_replicate_config(config);
                    validate_result) {
                    return validate_result;
                }
                return direct_write(key, tensor, config);
            case ParallelismWriteStorageRoute::WRITER_PARTITION_SHARD:
                if (!request.writer_partition.has_value()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                return writer_shard_write(key, tensor,
                                          *request.writer_partition, config);
            case ParallelismWriteStorageRoute::LEGACY_SINGLE_TP:
            case ParallelismWriteStorageRoute::TP_SHARDED_PARALLELISM:
            case ParallelismWriteStorageRoute::GENERIC_PARALLELISM_SHARD: {
                if (!request.parallelism.has_value()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                switch (request.route) {
                    case ParallelismWriteStorageRoute::LEGACY_SINGLE_TP: {
                        const auto &axis = request.parallelism->axes[0];
                        return legacy_tp_write(key, tensor, config, axis.rank,
                                               axis.size,
                                               axis.split_dim.value_or(0));
                    }
                    case ParallelismWriteStorageRoute::TP_SHARDED_PARALLELISM: {
                        auto tp_axis_index =
                            find_tp_axis_index(request.parallelism->axes);
                        if (!tp_axis_index.has_value()) {
                            return to_py_ret(ErrorCode::INVALID_PARAMS);
                        }
                        const auto &axis =
                            request.parallelism->axes[*tp_axis_index];
                        return tp_parallel_write(key, tensor, config, axis.rank,
                                                 axis.size,
                                                 axis.split_dim.value_or(0),
                                                 request.parallelism->axes);
                    }
                    case ParallelismWriteStorageRoute::
                        GENERIC_PARALLELISM_SHARD:
                        return generic_shard_write(
                            key, tensor, *request.parallelism, config);
                    default:
                        break;
                }
                break;
            }
        }
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }

    template <typename WritePartsFn>
    int execute_tensor_parts_write(
        const std::string &key,
        const std::vector<std::span<const char>> &values,
        const ReplicateConfig &config, const TensorWriteStoreOps &ops,
        WritePartsFn &&write_parts) {
        py::gil_scoped_release release_gil;
        int ret = write_parts(key, values, config);
        if (ret != 0) {
            LOG(ERROR) << ops.parts_operation_name << " failed for key " << key
                       << " with code " << ret;
        }
        return ret;
    }

    template <typename DirectWriteFn>
    int execute_parallelism_direct_tensor_write_from(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const ReplicateConfig &config, const char *operation_name,
        DirectWriteFn &&direct_write) {
        if (is_default_replicate_config(config)) {
            return direct_write(key, buffer_ptr, size);
        }
        return execute_single_tensor_write_from(
            operation_name, key, buffer_ptr, size, config,
            [&](const std::string &write_key, void *buffer, size_t write_size,
                const ReplicateConfig &write_config) {
                return direct_write(write_key,
                                    reinterpret_cast<uintptr_t>(buffer),
                                    write_size, write_config);
            });
    }

    pybind11::object decode_tensor_object_buffer(uintptr_t buffer_ptr,
                                                 size_t size,
                                                 const char *operation_name) {
        if (!is_valid_tensor_object_buffer(buffer_ptr, size, operation_name)) {
            return py::none();
        }
        pybind11::object tensor =
            buffer_to_tensor(NULL, reinterpret_cast<char *>(buffer_ptr),
                             static_cast<int64_t>(size));
        if (tensor.is_none()) {
            LOG(ERROR) << "Failed to decode tensor buffer for "
                       << operation_name;
        }
        return tensor;
    }

    std::string get_tp_write_shard_key(
        const std::string &base_key, int rank,
        const std::vector<ParallelAxisSpec> &axes) {
        if (axes.empty()) {
            return get_tp_key_name(base_key, rank);
        }
        auto shard_axes = axes;
        auto tp_axis_index = find_tp_axis_index(shard_axes);
        if (!tp_axis_index.has_value()) {
            return std::string();
        }
        shard_axes[*tp_axis_index].rank = rank;
        return get_parallelism_key_name(base_key,
                                        TensorParallelismSpec{shard_axes});
    }

    template <typename WriteShardFn>
    int execute_tp_tensor_write_impl(const std::string &key,
                                     pybind11::object tensor,
                                     const ReplicateConfig &config, int tp_size,
                                     int split_dim,
                                     const std::vector<ParallelAxisSpec> &axes,
                                     const char *error_context,
                                     WriteShardFn &&write_shard) {
        try {
            auto shard_infos = build_tp_shard_infos(
                tensor, tp_size, split_dim,
                [&](int rank) {
                    return get_tp_write_shard_key(key, rank, axes);
                },
                axes);
            if (!shard_infos.has_value()) {
                LOG(ERROR) << error_context;
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }

            for (int rank = 0; rank < tp_size; ++rank) {
                std::string shard_key = get_tp_write_shard_key(key, rank, axes);
                if (shard_key.empty()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                int ret = write_shard(shard_key, (*shard_infos)[rank], config);
                if (ret != 0) return ret;
            }
            return 0;
        } catch (const std::exception &e) {
            LOG(ERROR) << error_context << ": " << e.what();
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
    }

    template <typename WritePartsFn>
    int execute_tp_tensor_write_from_impl(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const ReplicateConfig &config, int tp_size, int split_dim,
        const std::vector<ParallelAxisSpec> &axes, const char *error_context,
        WritePartsFn &&write_parts) {
        auto parsed = parse_tensor_metadata_from_raw_buffer(buffer_ptr, size,
                                                            error_context);
        if (!parsed.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        for (int rank = 0; rank < tp_size; ++rank) {
            std::string shard_key = get_tp_write_shard_key(key, rank, axes);
            if (shard_key.empty()) {
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }

            auto plan = build_raw_tensor_shard_write_plan(
                *parsed, split_dim, rank, tp_size, error_context);
            if (!plan.has_value()) {
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }
            const auto global_shape =
                TensorShapeToVector(parsed->metadata.layout.global_shape,
                                    parsed->metadata.header.ndim);
            const auto local_shape = TensorShapeToVector(
                plan->metadata.layout.local_shape, plan->metadata.header.ndim);
            if (axes.empty()) {
                ParallelAxisSpec axis_spec{
                    "tp", rank, tp_size, split_dim, std::nullopt, std::nullopt};
                auto metadata = build_shard_metadata_from_shapes(
                    parsed->metadata.header.dtype, global_shape, local_shape,
                    {axis_spec}, plan->metadata.header.data_bytes);
                if (!metadata.has_value()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                plan->metadata = *metadata;
            } else {
                auto shard_axes = axes;
                auto tp_axis_index = find_tp_axis_index(shard_axes);
                if (!tp_axis_index.has_value()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                shard_axes[*tp_axis_index].rank = rank;
                const auto local_shape =
                    TensorShapeToVector(plan->metadata.layout.local_shape,
                                        plan->metadata.header.ndim);
                auto metadata = build_shard_metadata_from_shapes(
                    parsed->metadata.header.dtype, global_shape, local_shape,
                    shard_axes, plan->metadata.header.data_bytes);
                if (!metadata.has_value()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                plan->metadata = *metadata;
            }

            std::vector<std::span<const char>> values;
            values.emplace_back(reinterpret_cast<const char *>(&plan->metadata),
                                plan->metadata.header.data_offset);
            for (const auto &[src_offset, part_size] : plan->data_ranges) {
                values.emplace_back(
                    reinterpret_cast<const char *>(buffer_ptr + src_offset),
                    part_size);
            }
            int ret = write_parts(shard_key, values, config);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }

    template <typename BatchWriteFn>
    std::vector<int> batch_execute_tp_tensor_write_impl(
        const std::vector<std::string> &base_keys,
        const pybind11::list &tensors_list, const ReplicateConfig &config,
        int tp_size, int split_dim, const char *error_context,
        BatchWriteFn &&batch_write) {
        std::vector<std::string> all_chunk_keys;
        std::vector<PyTensorInfo> all_chunk_infos;
        std::vector<size_t> processed_indices;
        std::vector<int> final_results(base_keys.size(),
                                       to_py_ret(ErrorCode::INVALID_PARAMS));
        try {
            for (size_t i = 0; i < base_keys.size(); ++i) {
                py::object tensor = tensors_list[i];
                if (tensor.is_none()) {
                    final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                auto shard_infos = build_tp_shard_infos(
                    tensor, tp_size, split_dim, [&](int rank) {
                        return get_tp_key_name(base_keys[i], rank);
                    });
                if (!shard_infos.has_value()) {
                    final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                processed_indices.push_back(i);
                for (int rank = 0; rank < tp_size; ++rank) {
                    all_chunk_keys.push_back(
                        get_tp_key_name(base_keys[i], rank));
                    all_chunk_infos.push_back((*shard_infos)[rank]);
                }
            }

            if (all_chunk_keys.empty()) return final_results;

            std::vector<int> chunk_results =
                batch_write(all_chunk_keys, all_chunk_infos, config);
            for (size_t i = 0; i < processed_indices.size(); ++i) {
                size_t original_idx = processed_indices[i];
                bool all_ok = true;
                for (int j = 0; j < tp_size; ++j) {
                    int res = chunk_results[i * tp_size + j];
                    if (res != 0) {
                        final_results[original_idx] = res;
                        all_ok = false;
                        break;
                    }
                }
                if (all_ok) {
                    final_results[original_idx] = 0;
                }
            }
        } catch (const std::exception &e) {
            LOG(ERROR) << error_context << ": " << e.what();
        }

        return final_results;
    }

    template <typename BatchTensorWriteFn>
    std::vector<int> batch_decode_tensor_buffers_and_write(
        const std::vector<std::string> &keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes, const char *decode_error_context,
        BatchTensorWriteFn &&batch_tensor_write_fn) {
        py::list tensors_list;
        std::vector<size_t> processed_indices;
        std::vector<int> final_results(keys.size(), 0);

        for (size_t i = 0; i < keys.size(); ++i) {
            if (!is_valid_tensor_object_buffer(
                    buffer_ptrs[i], sizes[i],
                    std::string("tensor object buffer at index ") +
                        std::to_string(i))) {
                final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                continue;
            }

            py::object tensor =
                buffer_to_tensor(NULL, reinterpret_cast<char *>(buffer_ptrs[i]),
                                 static_cast<int64_t>(sizes[i]));
            if (tensor.is_none()) {
                LOG(ERROR) << decode_error_context << " at index " << i;
                final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                continue;
            }
            tensors_list.append(tensor);
            processed_indices.push_back(i);
        }

        if (processed_indices.empty()) {
            return final_results;
        }

        std::vector<std::string> valid_keys;
        valid_keys.reserve(processed_indices.size());
        for (size_t idx : processed_indices) {
            valid_keys.push_back(keys[idx]);
        }

        std::vector<int> op_results =
            batch_tensor_write_fn(valid_keys, tensors_list);
        for (size_t i = 0; i < processed_indices.size(); ++i) {
            final_results[processed_indices[i]] = op_results[i];
        }
        return final_results;
    }

    template <typename WriteShardFn>
    int execute_direct_parallelism_shard_write_from(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const TensorParallelismSpec &parallelism, const ReplicateConfig &config,
        const char *error_context, const TensorWriteStoreOps &ops,
        WriteShardFn &&write_shard) {
        auto parsed = parse_tensor_metadata_from_raw_buffer(buffer_ptr, size,
                                                            error_context);
        if (!parsed.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        auto metadata = build_shard_metadata_from_shapes(
            parsed->metadata.header.dtype,
            TensorShapeToVector(parsed->metadata.layout.global_shape,
                                parsed->metadata.header.ndim),
            TensorShapeToVector(parsed->metadata.layout.local_shape,
                                parsed->metadata.header.ndim),
            parallelism.axes, parsed->data_bytes);
        if (!metadata.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        std::string shard_key = get_parallelism_key_name(key, parallelism);
        std::vector<std::span<const char>> values;
        values.emplace_back(reinterpret_cast<const char *>(&*metadata),
                            metadata->header.data_offset);
        values.emplace_back(
            reinterpret_cast<const char *>(buffer_ptr + parsed->data_offset),
            parsed->data_bytes);
        return execute_tensor_parts_write(
            shard_key, values, config, ops,
            std::forward<WriteShardFn>(write_shard));
    }

    template <typename WritePartsFn, typename WriteManifestFn>
    int execute_writer_partition_shard_write_from(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const WriterPartitionSpec &writer, const ReplicateConfig &config,
        const char *error_context, const TensorWriteStoreOps &ops,
        WritePartsFn &&write_parts, WriteManifestFn &&write_manifest) {
        auto parsed = parse_tensor_metadata_from_raw_buffer(buffer_ptr, size,
                                                            error_context);
        if (!parsed.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        auto plan = build_raw_tensor_shard_write_plan(
            *parsed, writer.split_dim, writer.rank, writer.size, error_context);
        if (!plan.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        ParallelAxisSpec axis_spec{"tp",         writer.rank,
                                   writer.size,  writer.split_dim,
                                   std::nullopt, std::nullopt};
        auto metadata = build_shard_metadata_from_shapes(
            parsed->metadata.header.dtype,
            TensorShapeToVector(parsed->metadata.layout.global_shape,
                                parsed->metadata.header.ndim),
            TensorShapeToVector(plan->metadata.layout.local_shape,
                                plan->metadata.header.ndim),
            {axis_spec}, plan->metadata.header.data_bytes);
        if (!metadata.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        plan->metadata = *metadata;

        std::vector<std::span<const char>> values;
        values.emplace_back(reinterpret_cast<const char *>(&plan->metadata),
                            plan->metadata.header.data_offset);
        for (const auto &[src_offset, part_size] : plan->data_ranges) {
            values.emplace_back(
                reinterpret_cast<const char *>(buffer_ptr + src_offset),
                part_size);
        }

        int ret = execute_tensor_parts_write(
            get_writer_shard_key_name(key, writer), values, config, ops,
            std::forward<WritePartsFn>(write_parts));
        if (ret != 0) {
            return ret;
        }
        return write_manifest(
            get_writer_manifest_key_name(key),
            build_writer_shard_manifest_from_shape(
                TensorShapeToVector(parsed->metadata.layout.global_shape,
                                    parsed->metadata.header.ndim),
                parsed->metadata.header.dtype, writer),
            config);
    }

    template <typename DirectFromWriteFn, typename WriterShardFromWriteFn,
              typename TpShardFromWriteFn, typename GenericShardFromWriteFn,
              typename TensorRouteWriteFn>
    int execute_parallelism_tensor_write_from_route(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const ResolvedParallelismWriteRequest &request,
        const ReplicateConfig &config, const char *error_context,
        DirectFromWriteFn &&direct_from_write,
        WriterShardFromWriteFn &&writer_shard_from_write,
        TpShardFromWriteFn &&tp_shard_from_write,
        GenericShardFromWriteFn &&generic_shard_from_write,
        TensorRouteWriteFn &&tensor_route_write) {
        switch (request.route) {
            case ParallelismWriteStorageRoute::DIRECT_FULL_OBJECT:
                return direct_from_write(key, buffer_ptr, size, config);
            case ParallelismWriteStorageRoute::WRITER_PARTITION_SHARD:
                if (!request.writer_partition.has_value()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                return writer_shard_from_write(
                    key, buffer_ptr, size, *request.writer_partition, config);
            case ParallelismWriteStorageRoute::LEGACY_SINGLE_TP:
            case ParallelismWriteStorageRoute::TP_SHARDED_PARALLELISM: {
                if (!request.parallelism.has_value()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                auto tp_axis_index =
                    find_tp_axis_index(request.parallelism->axes);
                if (!tp_axis_index.has_value()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                const auto &axis = request.parallelism->axes[*tp_axis_index];
                const auto axes =
                    request.route ==
                            ParallelismWriteStorageRoute::LEGACY_SINGLE_TP
                        ? std::vector<ParallelAxisSpec>{}
                        : request.parallelism->axes;
                return tp_shard_from_write(key, buffer_ptr, size, axis.size,
                                           axis.split_dim.value_or(0), axes,
                                           config);
            }
            case ParallelismWriteStorageRoute::GENERIC_PARALLELISM_SHARD:
                if (!request.parallelism.has_value()) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                return generic_shard_from_write(key, buffer_ptr, size,
                                                *request.parallelism, config);
        }

        pybind11::object tensor =
            decode_tensor_object_buffer(buffer_ptr, size, error_context);
        if (tensor.is_none()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return tensor_route_write(key, tensor, request, config);
    }

    int put_manifest_impl(const std::string &key,
                          const WriterShardManifest &manifest,
                          const ReplicateConfig &config) {
        return write_manifest_impl(
            key, manifest, "put",
            [this, &key, &config](std::span<const char> bytes) {
                return store_->put(key, bytes, config);
            });
    }

    int upsert_manifest_impl(const std::string &key,
                             const WriterShardManifest &manifest,
                             const ReplicateConfig &config) {
        return write_manifest_impl(
            key, manifest, "upsert",
            [this, &key, &config](std::span<const char> bytes) {
                return store_->upsert(key, bytes, config);
            });
    }

    int put_tensor_with_writer_shards(
        const std::string &key, pybind11::object tensor,
        const WriterPartitionSpec &writer,
        const ReplicateConfig &config = ReplicateConfig{}) {
        if (!ensure_tensor_write_supported("put_tensor_with_writer_shards")) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        auto writer_info = build_writer_shard_tensor_info(
            key, tensor, writer, "put_tensor_with_writer_shards");
        if (!writer_info.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        int ret =
            put_tensor_impl(writer_info->shard_key, writer_info->info, config);
        if (ret != 0) {
            return ret;
        }
        return put_manifest_impl(get_writer_manifest_key_name(key),
                                 writer_info->manifest, config);
    }

    int put_direct_parallelism_shard(const std::string &key,
                                     pybind11::object tensor,
                                     const TensorParallelismSpec &parallelism,
                                     const ReplicateConfig &config) {
        auto info =
            build_direct_parallelism_shard_info(tensor, parallelism.axes, key);
        if (!info.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return put_tensor_impl(get_parallelism_key_name(key, parallelism),
                               *info, config);
    }

    int put_tensor_with_writer_shards_from(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const WriterPartitionSpec &writer,
        const ReplicateConfig &config = ReplicateConfig{}) {
        return execute_writer_partition_shard_write_from(
            key, buffer_ptr, size, writer, config,
            "put_tensor_with_parallelism_from",
            TensorWriteStoreOps{"put_parts"},
            [this](const std::string &shard_key,
                   const std::vector<std::span<const char>> &values,
                   const ReplicateConfig &write_config) {
                return store_->put_parts(shard_key, values, write_config);
            },
            [this](const std::string &manifest_key,
                   const WriterShardManifest &manifest,
                   const ReplicateConfig &write_config) {
                return put_manifest_impl(manifest_key, manifest, write_config);
            });
    }

    int put_direct_parallelism_shard_from(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const TensorParallelismSpec &parallelism,
        const ReplicateConfig &config) {
        return execute_direct_parallelism_shard_write_from(
            key, buffer_ptr, size, parallelism, config,
            "put_tensor_with_parallelism_from",
            TensorWriteStoreOps{"put_parts"},
            [this](const std::string &shard_key,
                   const std::vector<std::span<const char>> &values,
                   const ReplicateConfig &write_config) {
                return store_->put_parts(shard_key, values, write_config);
            });
    }

    int execute_put_tensor_with_parallelism_route(
        const std::string &key, pybind11::object tensor,
        const ResolvedParallelismWriteRequest &request,
        const ReplicateConfig &config) {
        return execute_parallelism_tensor_write_route(
            key, tensor, request, config, "put_tensor_with_parallelism",
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const ReplicateConfig &write_config) {
                return put_tensor_impl(write_key, write_tensor, write_config);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const WriterPartitionSpec &writer,
                   const ReplicateConfig &write_config) {
                return put_tensor_with_writer_shards(write_key, write_tensor,
                                                     writer, write_config);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const ReplicateConfig &write_config, int rank, int size,
                   int split_dim) {
                return put_tensor_with_tp_impl(write_key, write_tensor,
                                               write_config, size, split_dim);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const ReplicateConfig &write_config, int rank, int size,
                   int split_dim, const std::vector<ParallelAxisSpec> &axes) {
                return put_tensor_with_tp_impl(write_key, write_tensor,
                                               write_config, size, split_dim,
                                               axes);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const TensorParallelismSpec &parallelism_spec,
                   const ReplicateConfig &write_config) {
                return put_direct_parallelism_shard(
                    write_key, write_tensor, parallelism_spec, write_config);
            });
    }

    std::vector<int> batch_put_tensor_with_parallelism(
        const std::vector<std::string> &keys,
        const pybind11::list &tensors_list,
        const py::object &parallelisms = py::none(),
        const ReplicateConfig &config = ReplicateConfig{},
        const py::object &writer_partitions = py::none()) {
        return execute_batch_parallelism_write_requests(
            keys, tensors_list.size(), parallelisms, writer_partitions,
            "batch_put_tensor_with_parallelism",
            [this, &keys, &tensors_list, &config]() {
                if (!is_client_initialized() || use_dummy_client_) {
                    return std::vector<int>(
                        keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                }
                int validate_result = validate_replicate_config(config);
                if (validate_result) {
                    return std::vector<int>(keys.size(), validate_result);
                }
                return batch_put_tensor_impl(keys, tensors_list, config);
            },
            [this, &keys, &tensors_list, &config](
                size_t i, const py::handle &parallelism) {
                return put_tensor_with_parallelism(
                    keys[i], tensors_list[i],
                    py::reinterpret_borrow<py::object>(parallelism), config);
            },
            [this, &keys, &tensors_list, &config](
                size_t i, const py::handle &writer_partition) {
                return put_tensor_with_parallelism(
                    keys[i], tensors_list[i], py::none(), config,
                    py::reinterpret_borrow<py::object>(writer_partition));
            });
    }

    bool is_valid_tensor_object_buffer(uintptr_t buffer_ptr, size_t size,
                                       const std::string &op_name) {
        return parse_tensor_metadata_from_raw_buffer(buffer_ptr, size,
                                                     op_name.c_str())
            .has_value();
    }

    std::shared_ptr<RealClient> get_real_client() const {
        if (use_dummy_client_) {
            return nullptr;
        }
        return std::dynamic_pointer_cast<RealClient>(store_);
    }

    std::optional<RealClient::RegisteredBufferRegion>
    resolve_registered_buffer_region(uintptr_t buffer_ptr, size_t size,
                                     const std::string &context) const {
        auto real_client = get_real_client();
        if (!real_client) {
            LOG(ERROR) << context << ": real client is not available";
            return std::nullopt;
        }

        auto region = real_client->resolve_registered_buffer(
            reinterpret_cast<void *>(buffer_ptr));
        if (!region.has_value()) {
            LOG(ERROR) << context << ": buffer is not registered";
            return std::nullopt;
        }

        if (region->offset + size > region->size) {
            LOG(ERROR) << context << ": buffer range exceeds registered region";
            return std::nullopt;
        }

        return region;
    }

    std::optional<ParsedTensorMetadata> get_tensor_metadata(
        const std::string &key,
        std::shared_ptr<BufferHandle> *buffer_handle_out = nullptr) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
            return std::nullopt;
        }

        std::shared_ptr<BufferHandle> buffer_handle;
        {
            py::gil_scoped_release release_gil;
            buffer_handle = store_->get_buffer(key);
        }
        if (!buffer_handle) {
            return std::nullopt;
        }

        if (buffer_handle_out) {
            *buffer_handle_out = buffer_handle;
        }
        return ParseTensorMetadata(
            static_cast<const char *>(buffer_handle->ptr()),
            buffer_handle->size());
    }

    std::optional<TensorIntoPlan> build_tensor_into_plan(
        const std::string &read_key, uintptr_t buffer_ptr, size_t size,
        const std::string &context,
        const std::optional<ParsedTensorMetadata> &metadata = std::nullopt) {
        std::optional<ParsedTensorMetadata> resolved_metadata = metadata;
        if (!resolved_metadata.has_value()) {
            resolved_metadata = get_tensor_metadata(read_key);
        }
        if (!resolved_metadata.has_value()) {
            return std::nullopt;
        }

        const auto total_length =
            resolved_metadata->data_offset + resolved_metadata->data_bytes;
        if (total_length > size) {
            LOG(ERROR) << context << ": buffer too small for key " << read_key;
            return std::nullopt;
        }

        auto region =
            resolve_registered_buffer_region(buffer_ptr, size, context);
        if (!region.has_value()) {
            return std::nullopt;
        }
        if (region->offset + total_length > region->size) {
            LOG(ERROR)
                << context
                << ": resolved destination range exceeds registered region";
            return std::nullopt;
        }

        TensorIntoPlan plan;
        plan.user_buffer_ptr = buffer_ptr;
        plan.registered_buffer_ptr = reinterpret_cast<uintptr_t>(region->base);
        plan.registered_buffer_size = region->size;
        plan.total_length = total_length;
        plan.fragments.push_back(TensorIntoFragment{
            .read_key = read_key,
            .dst_offset = region->offset,
            .src_offset = 0,
            .size = total_length,
        });
        return plan;
    }

    std::optional<TensorIntoPlan> build_full_tensor_into_plan_from_sources(
        uintptr_t buffer_ptr, size_t size,
        const std::vector<ReconstructedShardSource> &sources,
        const std::vector<int64_t> &global_shape, int split_dim, int32_t dtype,
        const std::string &context, bool allow_empty_fragments = false) {
        if (sources.empty()) {
            LOG(ERROR) << context << ": missing reconstruction shard sources";
            return std::nullopt;
        }

        auto region =
            resolve_registered_buffer_region(buffer_ptr, size, context);
        if (!region.has_value()) {
            return std::nullopt;
        }

        size_t representative_index = 0;
        for (size_t i = 0; i < sources.size(); ++i) {
            if (sources[i].metadata.data_bytes > 0) {
                representative_index = i;
                break;
            }
        }
        const auto &representative = sources[representative_index].metadata;
        const auto representative_local_shape =
            TensorShapeToVector(representative.metadata.layout.local_shape,
                                representative.metadata.header.ndim);
        if (global_shape.size() != representative_local_shape.size()) {
            LOG(ERROR) << context << ": invalid tensor metadata shape";
            return std::nullopt;
        }
        if (split_dim < 0 ||
            split_dim >= static_cast<int>(global_shape.size())) {
            LOG(ERROR) << context << ": invalid split_dim";
            return std::nullopt;
        }

        size_t element_size = 0;
        if (representative.data_bytes > 0) {
            int64_t shard_numel = 1;
            for (auto dim : representative_local_shape) {
                shard_numel *= dim;
            }
            if (shard_numel <= 0 ||
                representative.data_bytes % static_cast<size_t>(shard_numel) !=
                    0) {
                LOG(ERROR) << context << ": invalid shard tensor byte size";
                return std::nullopt;
            }
            element_size =
                representative.data_bytes / static_cast<size_t>(shard_numel);
        }

        size_t total_tensor_numel = 1;
        for (auto dim : global_shape) {
            total_tensor_numel *= static_cast<size_t>(dim);
        }
        const size_t total_tensor_bytes = total_tensor_numel * element_size;
        const size_t total_length = sizeof(TensorMetadata) + total_tensor_bytes;
        if (total_length > size ||
            region->offset + total_length > region->size) {
            LOG(ERROR) << context
                       << ": buffer too small for reconstructed tensor";
            return std::nullopt;
        }

        TensorMetadata full_metadata = BuildTensorMetadata(
            dtype, global_shape, global_shape, TensorLayoutKind::FULL);
        full_metadata.header.data_bytes = total_tensor_bytes;
        std::memcpy(reinterpret_cast<void *>(buffer_ptr), &full_metadata,
                    sizeof(TensorMetadata));

        TensorIntoPlan plan;
        plan.user_buffer_ptr = buffer_ptr;
        plan.registered_buffer_ptr = reinterpret_cast<uintptr_t>(region->base);
        plan.registered_buffer_size = region->size;
        plan.total_length = total_length;

        int64_t elements_before = 1;
        for (int i = 0; i < split_dim; ++i) {
            elements_before *= global_shape[i];
        }
        int64_t elements_after = 1;
        for (size_t i = split_dim + 1; i < global_shape.size(); ++i) {
            elements_after *= global_shape[i];
        }

        std::vector<size_t> global_strides(global_shape.size(), 1);
        for (int i = static_cast<int>(global_shape.size()) - 2; i >= 0; --i) {
            global_strides[i] = global_strides[i + 1] *
                                static_cast<size_t>(global_shape[i + 1]);
        }

        int64_t shard_offset = 0;
        for (size_t i = 0; i < sources.size(); ++i) {
            const auto &source = sources[i];
            const auto local_shape =
                TensorShapeToVector(source.metadata.metadata.layout.local_shape,
                                    source.metadata.metadata.header.ndim);
            if (local_shape.size() != global_shape.size()) {
                LOG(ERROR) << context << ": invalid shard shape for key "
                           << source.read_key;
                return std::nullopt;
            }
            for (size_t dim = 0; dim < global_shape.size(); ++dim) {
                if (static_cast<int>(dim) == split_dim) {
                    continue;
                }
                if (local_shape[dim] != global_shape[dim]) {
                    LOG(ERROR) << context << ": shard shape mismatch for key "
                               << source.read_key;
                    return std::nullopt;
                }
            }

            const int64_t shard_start = shard_offset;
            const int64_t shard_size = local_shape[split_dim];
            shard_offset += shard_size;
            if (shard_size == 0) {
                continue;
            }

            std::vector<size_t> local_strides(local_shape.size(), 1);
            for (int dim = static_cast<int>(local_shape.size()) - 2; dim >= 0;
                 --dim) {
                local_strides[dim] = local_strides[dim + 1] *
                                     static_cast<size_t>(local_shape[dim + 1]);
            }

            const size_t chunk_data_offset = source.metadata.data_offset;
            for (int64_t slice_idx = 0; slice_idx < elements_before;
                 ++slice_idx) {
                size_t dst_prefix_linear = 0;
                size_t src_prefix_linear = 0;
                int64_t remaining = slice_idx;
                for (int dim = split_dim - 1; dim >= 0; --dim) {
                    const int64_t dim_size = global_shape[dim];
                    const int64_t coord = remaining % dim_size;
                    remaining /= dim_size;
                    dst_prefix_linear +=
                        static_cast<size_t>(coord) * global_strides[dim];
                    src_prefix_linear +=
                        static_cast<size_t>(coord) * local_strides[dim];
                }

                const size_t dst_element_index =
                    dst_prefix_linear + static_cast<size_t>(shard_start) *
                                            global_strides[split_dim];
                const size_t src_element_index = src_prefix_linear;
                const size_t copy_elements =
                    static_cast<size_t>(shard_size) *
                    static_cast<size_t>(elements_after);

                plan.fragments.push_back(TensorIntoFragment{
                    .read_key = source.read_key,
                    .dst_offset = region->offset + sizeof(TensorMetadata) +
                                  dst_element_index * element_size,
                    .src_offset =
                        chunk_data_offset + src_element_index * element_size,
                    .size = copy_elements * element_size,
                });
            }
        }

        if (shard_offset != global_shape[split_dim]) {
            LOG(ERROR)
                << context
                << ": shard extents do not cover reconstructed dimension";
            return std::nullopt;
        }
        if (plan.fragments.empty() &&
            !(allow_empty_fragments && total_tensor_bytes == 0)) {
            LOG(ERROR) << context
                       << ": no fragments planned for reconstruction";
            return std::nullopt;
        }
        return plan;
    }

    std::optional<TensorIntoPlan> build_tp_full_tensor_into_plan(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const ParallelAxisSpec &axis, const std::string &context,
        const std::optional<TensorParallelismSpec> &parallelism =
            std::nullopt) {
        auto reconstruction = load_tp_full_reconstruction_sources(
            key, axis, context, parallelism);
        if (!reconstruction.has_value()) {
            return std::nullopt;
        }
        return build_full_tensor_into_plan_from_sources(
            buffer_ptr, size, reconstruction->sources,
            reconstruction->global_shape, reconstruction->split_dim,
            reconstruction->dtype, context,
            reconstruction->allow_empty_fragments);
    }

    std::optional<FullTensorReconstructionSources>
    load_tp_full_reconstruction_sources(
        const std::string &key, const ParallelAxisSpec &axis,
        const std::string &context,
        const std::optional<TensorParallelismSpec> &parallelism =
            std::nullopt) {
        if (axis.size <= 0) {
            LOG(ERROR) << context << ": tp_size must be positive";
            return std::nullopt;
        }

        FullTensorReconstructionSources reconstruction;
        reconstruction.sources.reserve(axis.size);
        for (int shard_rank = 0; shard_rank < axis.size; ++shard_rank) {
            std::string read_key;
            if (parallelism.has_value()) {
                auto shard_parallelism = *parallelism;
                auto tp_axis_index = find_tp_axis_index(shard_parallelism.axes);
                if (!tp_axis_index.has_value()) {
                    LOG(ERROR) << context
                               << ": missing TP axis in full reconstruction";
                    return std::nullopt;
                }
                shard_parallelism.axes[*tp_axis_index].rank = shard_rank;
                read_key = get_parallelism_key_name(key, shard_parallelism);
            } else {
                read_key = resolve_tp_read_key(key, shard_rank, axis.size);
            }
            auto metadata = get_tensor_metadata(read_key);
            if (!metadata.has_value()) {
                return std::nullopt;
            }
            const LayoutAxis *tp_axis =
                find_layout_axis(metadata->metadata, LayoutAxisKind::TP);
            if (!is_shard_tensor_metadata(metadata->metadata) || !tp_axis ||
                tp_axis->shard_rank != shard_rank ||
                tp_axis->shard_count != axis.size) {
                LOG(ERROR) << context << ": TP metadata mismatch for key "
                           << read_key;
                return std::nullopt;
            }
            if (parallelism.has_value()) {
                auto stored_parallelism =
                    resolve_tp_compatible_parallelism_from_metadata(
                        *parallelism, metadata->metadata, context);
                if (!stored_parallelism.has_value()) {
                    return std::nullopt;
                }
            }
            reconstruction.sources.push_back(
                ReconstructedShardSource{read_key, *metadata});
        }

        reconstruction.global_shape = TensorShapeToVector(
            reconstruction.sources.front()
                .metadata.metadata.layout.global_shape,
            reconstruction.sources.front().metadata.metadata.header.ndim);
        const LayoutAxis *stored_tp_axis =
            find_layout_axis(reconstruction.sources.front().metadata.metadata,
                             LayoutAxisKind::TP);
        if (!stored_tp_axis) {
            LOG(ERROR) << context << ": missing TP axis metadata";
            return std::nullopt;
        }
        reconstruction.split_dim = stored_tp_axis->split_dim;
        if (axis.split_dim.has_value() &&
            axis.split_dim.value() != reconstruction.split_dim) {
            LOG(ERROR) << context << ": split_dim mismatch";
            return std::nullopt;
        }
        reconstruction.dtype =
            reconstruction.sources.front().metadata.metadata.header.dtype;
        return reconstruction;
    }

    std::optional<FullTensorReconstructionSources>
    load_writer_shard_reconstruction(const std::string &key,
                                     const std::string &context) {
        if (!is_client_initialized()) {
            LOG(ERROR) << context << ": client is not initialized";
            return std::nullopt;
        }

        std::shared_ptr<BufferHandle> manifest_handle;
        {
            py::gil_scoped_release release_gil;
            manifest_handle =
                store_->get_buffer(get_writer_manifest_key_name(key));
        }
        auto parsed_manifest =
            parse_writer_shard_manifest(manifest_handle.get());
        if (!parsed_manifest.has_value()) {
            return std::nullopt;
        }

        const auto &manifest = *parsed_manifest;
        const auto &global_shape = manifest.global_shape;
        const int split_dim = manifest.manifest.header.split_dim;
        const int shard_count = manifest.manifest.header.shard_count;
        if (split_dim < 0 ||
            split_dim >= static_cast<int>(global_shape.size())) {
            LOG(ERROR) << context << ": invalid writer split_dim";
            return std::nullopt;
        }

        FullTensorReconstructionSources reconstruction;
        reconstruction.sources.reserve(shard_count);
        for (int shard_rank = 0; shard_rank < shard_count; ++shard_rank) {
            WriterPartitionSpec writer{
                .rank = shard_rank,
                .size = shard_count,
                .split_dim = split_dim,
            };
            const std::string shard_key =
                get_writer_shard_key_name(key, writer);
            auto metadata = get_tensor_metadata(shard_key);
            if (!metadata.has_value()) {
                LOG(ERROR) << context << ": missing writer shard key "
                           << shard_key;
                return std::nullopt;
            }
            auto writer_parallelism =
                writer_partition_parallelism_from_metadata(metadata->metadata);
            if (!writer_parallelism.has_value() ||
                writer_parallelism->axes[0].rank != shard_rank ||
                writer_parallelism->axes[0].size != shard_count ||
                writer_parallelism->axes[0].split_dim != split_dim) {
                LOG(ERROR) << context
                           << ": writer shard metadata mismatch for key "
                           << shard_key;
                return std::nullopt;
            }
            reconstruction.sources.push_back(
                ReconstructedShardSource{shard_key, *metadata});
        }
        reconstruction.global_shape = manifest.global_shape;
        reconstruction.split_dim = manifest.manifest.header.split_dim;
        reconstruction.dtype = manifest.manifest.header.dtype;
        reconstruction.allow_empty_fragments = true;
        return reconstruction;
    }

    std::optional<TensorIntoPlan> build_writer_shard_full_tensor_into_plan(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const std::string &context) {
        auto reconstruction = load_writer_shard_reconstruction(key, context);
        if (!reconstruction.has_value()) {
            return build_tensor_into_plan(key, buffer_ptr, size, context);
        }

        return build_full_tensor_into_plan_from_sources(
            buffer_ptr, size, reconstruction->sources,
            reconstruction->global_shape, reconstruction->split_dim,
            reconstruction->dtype, context,
            reconstruction->allow_empty_fragments);
    }

    std::optional<FullTensorReconstructionSources>
    load_parallelism_full_reconstruction_sources(
        const std::string &key, const TensorParallelismSpec &parallelism,
        const std::string &context) {
        const ParallelAxisSpec *request_tp_axis =
            find_axis_spec_by_kind(parallelism, LayoutAxisKind::TP);
        if (!request_tp_axis) {
            LOG(ERROR) << context << ": full reconstruction requires a TP axis";
            return std::nullopt;
        }

        auto canonical_parallelism = canonicalize_parallelism_spec(parallelism);
        if (!canonical_parallelism.has_value()) {
            LOG(ERROR) << context << ": failed to canonicalize parallelism";
            return std::nullopt;
        }

        auto tp_axis_index = find_tp_axis_index(canonical_parallelism->axes);
        if (!tp_axis_index.has_value()) {
            LOG(ERROR) << context << ": missing TP axis in request";
            return std::nullopt;
        }

        FullTensorReconstructionSources reconstruction;
        reconstruction.sources.reserve(request_tp_axis->size);
        for (int shard_rank = 0; shard_rank < request_tp_axis->size;
             ++shard_rank) {
            auto shard_parallelism = *canonical_parallelism;
            shard_parallelism.axes[*tp_axis_index].rank = shard_rank;
            const std::string shard_key =
                get_parallelism_key_name(key, shard_parallelism);
            auto metadata = get_tensor_metadata(shard_key);
            if (!metadata.has_value()) {
                LOG(ERROR) << context
                           << ": no shard matched requested layout for TP rank "
                           << shard_rank;
                return std::nullopt;
            }

            auto stored_parallelism =
                resolve_tp_compatible_parallelism_from_metadata(
                    shard_parallelism, metadata->metadata, context);
            if (!stored_parallelism.has_value() ||
                !parallelism_specs_equal_by_kind(
                    *canonical_parallelism, *stored_parallelism,
                    true /* allow_tp_rank_mismatch */)) {
                LOG(ERROR) << context
                           << ": shard metadata mismatch for TP rank "
                           << shard_rank;
                return std::nullopt;
            }
            reconstruction.sources.push_back(
                ReconstructedShardSource{shard_key, *metadata});
        }

        const LayoutAxis *stored_tp_axis =
            find_layout_axis(reconstruction.sources.front().metadata.metadata,
                             LayoutAxisKind::TP);
        if (!stored_tp_axis) {
            LOG(ERROR) << context << ": missing TP axis metadata";
            return std::nullopt;
        }
        reconstruction.split_dim = stored_tp_axis->split_dim;
        if (request_tp_axis->split_dim.has_value() &&
            request_tp_axis->split_dim.value() != reconstruction.split_dim) {
            LOG(ERROR) << context << ": split_dim mismatch";
            return std::nullopt;
        }

        reconstruction.global_shape = TensorShapeToVector(
            reconstruction.sources.front()
                .metadata.metadata.layout.global_shape,
            reconstruction.sources.front().metadata.metadata.header.ndim);
        reconstruction.dtype =
            reconstruction.sources.front().metadata.metadata.header.dtype;
        return reconstruction;
    }

    std::optional<TensorIntoPlan> build_parallelism_full_tensor_into_plan(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const TensorParallelismSpec &parallelism, const std::string &context) {
        auto reconstruction = load_parallelism_full_reconstruction_sources(
            key, parallelism, context);
        if (!reconstruction.has_value()) {
            return std::nullopt;
        }
        return build_full_tensor_into_plan_from_sources(
            buffer_ptr, size, reconstruction->sources,
            reconstruction->global_shape, reconstruction->split_dim,
            reconstruction->dtype, context,
            reconstruction->allow_empty_fragments);
    }

    std::vector<bool> execute_tensor_into_plan_transfers(
        const std::vector<TensorIntoPlan> &plans) {
        std::vector<bool> success(plans.size(), false);
        if (plans.empty()) {
            return success;
        }

        std::vector<void *> buffers;
        std::vector<std::vector<std::string>> all_keys;
        std::vector<std::vector<std::vector<size_t>>> all_dst_offsets;
        std::vector<std::vector<std::vector<size_t>>> all_src_offsets;
        std::vector<std::vector<std::vector<size_t>>> all_sizes;
        buffers.reserve(plans.size());
        all_keys.reserve(plans.size());
        all_dst_offsets.reserve(plans.size());
        all_src_offsets.reserve(plans.size());
        all_sizes.reserve(plans.size());

        for (const auto &plan : plans) {
            buffers.push_back(
                reinterpret_cast<void *>(plan.registered_buffer_ptr));

            std::unordered_map<std::string, size_t> key_to_index;
            std::vector<std::string> keys;
            std::vector<std::vector<size_t>> dst_offsets;
            std::vector<std::vector<size_t>> src_offsets;
            std::vector<std::vector<size_t>> sizes;

            for (const auto &fragment : plan.fragments) {
                if (fragment.read_key.empty() || fragment.size == 0) {
                    continue;
                }
                auto [it, inserted] =
                    key_to_index.emplace(fragment.read_key, keys.size());
                if (inserted) {
                    keys.push_back(fragment.read_key);
                    dst_offsets.push_back({});
                    src_offsets.push_back({});
                    sizes.push_back({});
                }
                const size_t key_index = it->second;
                dst_offsets[key_index].push_back(fragment.dst_offset);
                src_offsets[key_index].push_back(fragment.src_offset);
                sizes[key_index].push_back(fragment.size);
            }

            all_keys.push_back(std::move(keys));
            all_dst_offsets.push_back(std::move(dst_offsets));
            all_src_offsets.push_back(std::move(src_offsets));
            all_sizes.push_back(std::move(sizes));
        }

        std::vector<std::vector<std::vector<int64_t>>> range_results;
        {
            py::gil_scoped_release release_gil;
            range_results = store_->get_into_ranges(
                buffers, all_keys, all_dst_offsets, all_src_offsets, all_sizes);
        }

        for (size_t i = 0; i < plans.size(); ++i) {
            if (i >= range_results.size() ||
                range_results[i].size() != all_sizes[i].size()) {
                continue;
            }

            success[i] = true;
            for (size_t key_idx = 0;
                 key_idx < all_sizes[i].size() && success[i]; ++key_idx) {
                if (range_results[i][key_idx].size() !=
                    all_sizes[i][key_idx].size()) {
                    success[i] = false;
                    break;
                }
                for (size_t frag_idx = 0;
                     frag_idx < all_sizes[i][key_idx].size(); ++frag_idx) {
                    if (range_results[i][key_idx][frag_idx] !=
                        static_cast<int64_t>(all_sizes[i][key_idx][frag_idx])) {
                        success[i] = false;
                        break;
                    }
                }
            }
        }
        return success;
    }

    py::list execute_tensor_into_plans(
        const std::vector<TensorIntoPlan> &plans) {
        py::list results;
        for (size_t i = 0; i < plans.size(); ++i) {
            results.append(py::none());
        }
        auto success = execute_tensor_into_plan_transfers(plans);
        for (size_t i = 0; i < plans.size(); ++i) {
            if (!success[i]) {
                continue;
            }
            results[i] = buffer_to_tensor(
                NULL, reinterpret_cast<char *>(plans[i].user_buffer_ptr),
                static_cast<int64_t>(plans[i].total_length));
        }
        return results;
    }

    std::string resolve_tp_read_key(const std::string &key, int tp_rank,
                                    int tp_size) const {
        if (tp_size <= 1) return key;
        return get_tp_key_name(key, tp_rank);
    }

    std::vector<std::string> resolve_tp_read_keys(
        const std::vector<std::string> &base_keys, int tp_rank,
        int tp_size) const {
        if (tp_size <= 1) return base_keys;

        std::vector<std::string> shard_keys;
        shard_keys.reserve(base_keys.size());
        for (const auto &key : base_keys) {
            shard_keys.push_back(get_tp_key_name(key, tp_rank));
        }
        return shard_keys;
    }

    pybind11::object get_tensor_with_parallelism(
        const std::string &key, const py::object &target = py::none()) {
        auto parsed_target = parse_read_target_spec(target);
        if (!parsed_target.has_value()) {
            return pybind11::none();
        }
        if (parsed_target->mode == ReadTargetMode::AS_STORED &&
            !parsed_target->parallelism.has_value()) {
            return get_tensor(key);
        }
        if (parsed_target->mode == ReadTargetMode::FULL) {
            if (!parsed_target->parallelism.has_value()) {
                return get_tensor_with_writer_shard_full(
                    key, "get_tensor_with_parallelism");
            }
            auto parallelism = validate_parallelism_spec(
                parsed_target->parallelism, "ReadTarget(mode=full)", false);
            if (!parallelism.has_value()) {
                return pybind11::none();
            }
            auto tp_axis_index = find_tp_axis_index(parallelism->axes);
            if (!tp_axis_index.has_value()) {
                LOG(ERROR) << "ReadTarget(mode=full): full reconstruction "
                              "requires a TP axis";
                return pybind11::none();
            }
            const auto &axis = parallelism->axes[*tp_axis_index];
            return get_tensor_with_tp_full(
                key, axis.rank, axis.size, axis.split_dim.value_or(0),
                "get_tensor_with_parallelism", *parallelism);
        }
        if (parsed_target->mode == ReadTargetMode::SHARD) {
            auto parallelism = validate_parallelism_spec(
                parsed_target->parallelism, "ReadTarget(mode=shard)", false);
            if (!parallelism.has_value()) {
                return pybind11::none();
            }
            auto resolved = resolve_parallelism_shard_read(
                [this](const std::string &read_key,
                       std::shared_ptr<BufferHandle> *buffer_handle_out) {
                    return get_tensor_metadata(read_key, buffer_handle_out);
                },
                key, *parallelism);
            if (!resolved.has_value()) {
                LOG(ERROR) << "Parallelism metadata mismatch for key " << key;
                return pybind11::none();
            }
            return buffer_to_tensor(resolved->buffer_handle.get(), NULL, 0);
        }
        LOG(ERROR) << "Unsupported ReadTarget mode";
        return pybind11::none();
    }

    pybind11::list batch_get_tensor_with_parallelism(
        const std::vector<std::string> &keys,
        const py::object &targets = py::none()) {
        if (targets.is_none()) {
            return batch_get_tensor(keys);
        }
        if (!py::isinstance<py::list>(targets)) {
            LOG(ERROR) << "targets must be a list or None";
            py::list empty;
            for (size_t i = 0; i < keys.size(); ++i) empty.append(py::none());
            return empty;
        }

        py::list target_list = py::cast<py::list>(targets);
        if (target_list.size() != keys.size()) {
            LOG(ERROR) << "keys and targets must have the same length";
            py::list empty;
            for (size_t i = 0; i < keys.size(); ++i) empty.append(py::none());
            return empty;
        }

        py::list results;
        for (size_t i = 0; i < keys.size(); ++i) {
            results.append(
                get_tensor_with_parallelism(keys[i], target_list[i]));
        }
        return results;
    }

    std::optional<TensorIntoPlan> build_tensor_into_plan_for_target(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const py::object &target, const std::string &context) {
        auto parsed_target = parse_read_target_spec(target);
        if (!parsed_target.has_value()) {
            return std::nullopt;
        }
        if (parsed_target->mode == ReadTargetMode::AS_STORED &&
            !parsed_target->parallelism.has_value()) {
            return build_tensor_into_plan(key, buffer_ptr, size, context);
        }

        if (parsed_target->mode == ReadTargetMode::FULL &&
            !parsed_target->parallelism.has_value()) {
            return build_writer_shard_full_tensor_into_plan(key, buffer_ptr,
                                                            size, context);
        }

        auto parallelism = validate_parallelism_spec(parsed_target->parallelism,
                                                     context, false);
        if (!parallelism.has_value()) {
            return std::nullopt;
        }

        if (parsed_target->mode == ReadTargetMode::SHARD) {
            auto resolved = resolve_parallelism_shard_read(
                [this](const std::string &read_key,
                       std::shared_ptr<BufferHandle> *buffer_handle_out) {
                    return get_tensor_metadata(read_key, buffer_handle_out);
                },
                key, *parallelism);
            if (!resolved.has_value()) {
                LOG(ERROR) << context
                           << ": parallelism metadata mismatch for key " << key;
                return std::nullopt;
            }
            return build_tensor_into_plan(resolved->read_key, buffer_ptr, size,
                                          context, resolved->metadata);
        }

        if (parsed_target->mode == ReadTargetMode::FULL) {
            return build_parallelism_full_tensor_into_plan(
                key, buffer_ptr, size, *parallelism, context);
        }

        LOG(ERROR) << context << ": unsupported ReadTarget mode";
        return std::nullopt;
    }

    pybind11::object get_tensor_with_parallelism_into(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const py::object &target = py::none()) {
        auto plan = build_tensor_into_plan_for_target(
            key, buffer_ptr, size, target, "get_tensor_with_parallelism_into");
        if (!plan.has_value()) {
            return pybind11::none();
        }
        py::list results = execute_tensor_into_plans({*plan});
        if (results.empty()) {
            return py::none();
        }
        return py::reinterpret_borrow<py::object>(results[0]);
    }

    pybind11::list batch_get_tensor_with_parallelism_into(
        const std::vector<std::string> &keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes,
        const py::object &targets = py::none()) {
        if (targets.is_none()) {
            return batch_get_tensor_into(keys, buffer_ptrs, sizes);
        }
        if (!py::isinstance<py::list>(targets)) {
            LOG(ERROR) << "targets must be a list or None";
            py::list empty;
            for (size_t i = 0; i < keys.size(); ++i) empty.append(py::none());
            return empty;
        }

        py::list target_list = py::cast<py::list>(targets);
        if (target_list.size() != keys.size() ||
            buffer_ptrs.size() != keys.size() || sizes.size() != keys.size()) {
            LOG(ERROR) << "keys, buffer_ptrs, sizes, and targets must have the "
                          "same length";
            py::list empty;
            for (size_t i = 0; i < keys.size(); ++i) empty.append(py::none());
            return empty;
        }

        std::vector<TensorIntoPlan> valid_plans;
        std::vector<size_t> valid_indices;
        valid_plans.reserve(keys.size());
        valid_indices.reserve(keys.size());
        py::list results;
        for (size_t i = 0; i < keys.size(); ++i) {
            results.append(py::none());
            auto plan = build_tensor_into_plan_for_target(
                keys[i], buffer_ptrs[i], sizes[i], target_list[i],
                "batch_get_tensor_with_parallelism_into");
            if (!plan.has_value()) {
                continue;
            }
            valid_indices.push_back(i);
            valid_plans.push_back(*plan);
        }

        py::list valid_results = execute_tensor_into_plans(valid_plans);
        for (size_t i = 0; i < valid_indices.size() && i < valid_results.size();
             ++i) {
            results[valid_indices[i]] = valid_results[i];
        }
        return results;
    }

    int put_tensor_with_tp_from(const std::string &key, uintptr_t buffer_ptr,
                                size_t size, int tp_rank = 0, int tp_size = 1,
                                int split_dim = 0) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (use_dummy_client_) {
            LOG(ERROR)
                << "put_tensor_with_tp_from is not supported for dummy client";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (buffer_ptr == 0) {
            LOG(ERROR) << "Buffer pointer cannot be null";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (size <= sizeof(TensorMetadata)) {
            LOG(ERROR) << "Buffer size too small for tensor metadata";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (tp_size <= 1) {
            return put_tensor_from(key, buffer_ptr, size);
        }

        pybind11::object tensor =
            buffer_to_tensor(NULL, reinterpret_cast<char *>(buffer_ptr),
                             static_cast<int64_t>(size));
        if (tensor.is_none()) {
            LOG(ERROR) << "Failed to decode full tensor buffer for "
                          "put_tensor_with_tp_from";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return put_tensor_with_tp_impl(key, tensor, ReplicateConfig{}, tp_rank,
                                       tp_size, split_dim);
    }

    int execute_put_tensor_with_parallelism_from_route(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const ResolvedParallelismWriteRequest &request,
        const ReplicateConfig &config) {
        return execute_parallelism_tensor_write_from_route(
            key, buffer_ptr, size, request, config,
            "put_tensor_with_parallelism_from",
            [this](const std::string &write_key, uintptr_t write_buffer_ptr,
                   size_t write_size, const ReplicateConfig &write_config) {
                return execute_parallelism_direct_tensor_write_from(
                    write_key, write_buffer_ptr, write_size, write_config,
                    "put_tensor_with_parallelism_from",
                    [this](const std::string &direct_key,
                           uintptr_t direct_buffer_ptr, size_t direct_size,
                           const ReplicateConfig &direct_config =
                               ReplicateConfig{}) {
                        if (is_default_replicate_config(direct_config)) {
                            return put_tensor_from(
                                direct_key, direct_buffer_ptr, direct_size);
                        }
                        py::gil_scoped_release release_gil;
                        return store_->put_from(
                            direct_key,
                            reinterpret_cast<void *>(direct_buffer_ptr),
                            direct_size, direct_config);
                    });
            },
            [this](const std::string &write_key, uintptr_t write_buffer_ptr,
                   size_t write_size, const WriterPartitionSpec &writer,
                   const ReplicateConfig &write_config) {
                return put_tensor_with_writer_shards_from(
                    write_key, write_buffer_ptr, write_size, writer,
                    write_config);
            },
            [this](const std::string &write_key, uintptr_t write_buffer_ptr,
                   size_t write_size, int tp_size, int split_dim,
                   const std::vector<ParallelAxisSpec> &axes,
                   const ReplicateConfig &write_config) {
                return execute_tp_tensor_write_from_impl(
                    write_key, write_buffer_ptr, write_size, write_config,
                    tp_size, split_dim, axes,
                    "put_tensor_with_parallelism_from",
                    [this](const std::string &shard_key,
                           const std::vector<std::span<const char>> &values,
                           const ReplicateConfig &parts_config) {
                        return execute_tensor_parts_write(
                            shard_key, values, parts_config,
                            TensorWriteStoreOps{"put_parts"},
                            [this](const std::string &final_key,
                                   const std::vector<std::span<const char>>
                                       &final_values,
                                   const ReplicateConfig &final_config) {
                                return store_->put_parts(
                                    final_key, final_values, final_config);
                            });
                    });
            },
            [this](const std::string &write_key, uintptr_t write_buffer_ptr,
                   size_t write_size,
                   const TensorParallelismSpec &parallelism_spec,
                   const ReplicateConfig &write_config) {
                return put_direct_parallelism_shard_from(
                    write_key, write_buffer_ptr, write_size, parallelism_spec,
                    write_config);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const ResolvedParallelismWriteRequest &write_request,
                   const ReplicateConfig &write_config) {
                return execute_put_tensor_with_parallelism_route(
                    write_key, write_tensor, write_request, write_config);
            });
    }

    int put_tensor_with_parallelism_from(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const py::object &parallelism = py::none(),
        const ReplicateConfig &config = ReplicateConfig{},
        const py::object &writer_partition = py::none()) {
        auto request = resolve_parallelism_write_request(
            parallelism, writer_partition, "put_tensor_with_parallelism_from");
        if (!request.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return execute_put_tensor_with_parallelism_from_route(
            key, buffer_ptr, size, *request, config);
    }

    std::vector<int> batch_put_tensor_with_tp_from(
        const std::vector<std::string> &base_keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes, int tp_rank = 0, int tp_size = 1,
        int split_dim = 0) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return std::vector<int>(base_keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (use_dummy_client_) {
            LOG(ERROR) << "batch_put_tensor_with_tp_from is not supported for "
                          "dummy client";
            return std::vector<int>(base_keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (tp_size <= 1) {
            return batch_put_tensor_from(base_keys, buffer_ptrs, sizes);
        }
        if (base_keys.size() != buffer_ptrs.size() ||
            base_keys.size() != sizes.size() || base_keys.empty()) {
            if (!base_keys.empty()) {
                LOG(ERROR) << "Size mismatch: base_keys, buffer_ptrs, and "
                              "sizes must have the same length";
            }
            return std::vector<int>(base_keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        py::list tensors_list;
        std::vector<size_t> processed_indices;
        std::vector<int> final_results(base_keys.size(), 0);

        for (size_t i = 0; i < base_keys.size(); ++i) {
            if (buffer_ptrs[i] == 0) {
                LOG(ERROR) << "Buffer pointer at index " << i
                           << " cannot be null";
                final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                continue;
            }
            if (sizes[i] <= sizeof(TensorMetadata)) {
                LOG(ERROR) << "Buffer size at index " << i
                           << " too small for tensor metadata";
                final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                continue;
            }

            py::object tensor =
                buffer_to_tensor(NULL, reinterpret_cast<char *>(buffer_ptrs[i]),
                                 static_cast<int64_t>(sizes[i]));
            if (tensor.is_none()) {
                LOG(ERROR) << "Failed to decode full tensor buffer at index "
                           << i;
                final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                continue;
            }
            tensors_list.append(tensor);
            processed_indices.push_back(i);
        }

        if (processed_indices.empty()) {
            return final_results;
        }

        std::vector<std::string> valid_keys;
        valid_keys.reserve(processed_indices.size());
        for (size_t idx : processed_indices) {
            valid_keys.push_back(base_keys[idx]);
        }

        std::vector<int> op_results = batch_put_tensor_with_tp_impl(
            valid_keys, tensors_list, ReplicateConfig{}, tp_rank, tp_size,
            split_dim);
        for (size_t i = 0; i < processed_indices.size(); ++i) {
            final_results[processed_indices[i]] = op_results[i];
        }
        return final_results;
    }

    std::vector<int> batch_put_tensor_with_parallelism_from(
        const std::vector<std::string> &keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes,
        const py::object &parallelisms = py::none(),
        const ReplicateConfig &config = ReplicateConfig{},
        const py::object &writer_partitions = py::none()) {
        return execute_batch_parallelism_write_requests(
            keys, buffer_ptrs.size(), parallelisms, writer_partitions,
            "batch_put_tensor_with_parallelism_from",
            [this, &keys, &buffer_ptrs, &sizes, &config]() {
                if (is_default_replicate_config(config)) {
                    return batch_put_tensor_from(keys, buffer_ptrs, sizes);
                }
                return execute_batch_tensor_write_from(
                    "batch_put_tensor_with_parallelism_from",
                    "Size mismatch: keys, buffer_ptrs, and sizes must have the "
                    "same length",
                    keys, buffer_ptrs, sizes, config,
                    [this](const std::vector<std::string> &write_keys,
                           const std::vector<void *> &buffers,
                           const std::vector<size_t> &buffer_sizes,
                           const ReplicateConfig &write_config) {
                        py::gil_scoped_release release_gil;
                        return store_->batch_put_from(
                            write_keys, buffers, buffer_sizes, write_config);
                    });
            },
            [this, &keys, &buffer_ptrs, &sizes, &config](
                size_t i, const py::handle &parallelism) {
                return put_tensor_with_parallelism_from(
                    keys[i], buffer_ptrs[i], sizes[i],
                    py::reinterpret_borrow<py::object>(parallelism), config);
            },
            [this, &keys, &buffer_ptrs, &sizes, &config](
                size_t i, const py::handle &writer_partition) {
                return put_tensor_with_parallelism_from(
                    keys[i], buffer_ptrs[i], sizes[i], py::none(), config,
                    py::reinterpret_borrow<py::object>(writer_partition));
            });
    }

    // --- Upsert tensor methods ---

    int upsert_tensor_impl(const std::string &key, const PyTensorInfo &info,
                           const ReplicateConfig &config) {
        if (!info.valid()) return to_py_ret(ErrorCode::INVALID_PARAMS);

        std::vector<std::span<const char>> values;
        values.emplace_back(reinterpret_cast<const char *>(&info.metadata),
                            info.metadata.header.data_offset);
        values.emplace_back(reinterpret_cast<const char *>(info.data_ptr),
                            info.tensor_size);

        py::gil_scoped_release release_gil;
        int ret = store_->upsert_parts(key, values, config);
        if (ret != 0)
            LOG(ERROR) << "upsert_parts failed for key " << key << " with code "
                       << ret;
        return ret;
    }

    int upsert_tensor_impl(const std::string &key, pybind11::object tensor,
                           const ReplicateConfig &config) {
        auto info = extract_tensor_info(tensor, key);
        if (!info.valid()) return to_py_ret(ErrorCode::INVALID_PARAMS);

        std::vector<std::span<const char>> values;
        values.emplace_back(reinterpret_cast<const char *>(&info.metadata),
                            sizeof(TensorMetadata));
        values.emplace_back(reinterpret_cast<const char *>(info.data_ptr),
                            info.tensor_size);

        py::gil_scoped_release release_gil;
        int ret = store_->upsert_parts(key, values, config);
        if (ret != 0)
            LOG(ERROR) << "upsert_parts failed for key " << key << " with code "
                       << ret;
        return ret;
    }

    int upsert_tensor(const std::string &key, pybind11::object tensor) {
        if (!is_client_initialized() || use_dummy_client_) {
            LOG(ERROR) << "Client not initialized or Dummy client not "
                          "supported for tensors";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return upsert_tensor_impl(key, tensor, ReplicateConfig{});
    }

    int upsert_tensor_with_writer_shards(
        const std::string &key, pybind11::object tensor,
        const WriterPartitionSpec &writer,
        const ReplicateConfig &config = ReplicateConfig{}) {
        if (!ensure_tensor_write_supported(
                "upsert_tensor_with_writer_shards")) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        auto writer_info = build_writer_shard_tensor_info(
            key, tensor, writer, "upsert_tensor_with_parallelism");
        if (!writer_info.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        int ret = upsert_tensor_impl(writer_info->shard_key, writer_info->info,
                                     config);
        if (ret != 0) {
            return ret;
        }
        return upsert_manifest_impl(get_writer_manifest_key_name(key),
                                    writer_info->manifest, config);
    }

    int upsert_direct_parallelism_shard(
        const std::string &key, pybind11::object tensor,
        const TensorParallelismSpec &parallelism,
        const ReplicateConfig &config) {
        auto info =
            build_direct_parallelism_shard_info(tensor, parallelism.axes, key);
        if (!info.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return upsert_tensor_impl(get_parallelism_key_name(key, parallelism),
                                  *info, config);
    }

    int upsert_tensor_with_writer_shards_from(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const WriterPartitionSpec &writer,
        const ReplicateConfig &config = ReplicateConfig{}) {
        return execute_writer_partition_shard_write_from(
            key, buffer_ptr, size, writer, config,
            "upsert_tensor_with_parallelism_from",
            TensorWriteStoreOps{"upsert_parts"},
            [this](const std::string &shard_key,
                   const std::vector<std::span<const char>> &values,
                   const ReplicateConfig &write_config) {
                return store_->upsert_parts(shard_key, values, write_config);
            },
            [this](const std::string &manifest_key,
                   const WriterShardManifest &manifest,
                   const ReplicateConfig &write_config) {
                return upsert_manifest_impl(manifest_key, manifest,
                                            write_config);
            });
    }

    int upsert_direct_parallelism_shard_from(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const TensorParallelismSpec &parallelism,
        const ReplicateConfig &config) {
        return execute_direct_parallelism_shard_write_from(
            key, buffer_ptr, size, parallelism, config,
            "upsert_tensor_with_parallelism_from",
            TensorWriteStoreOps{"upsert_parts"},
            [this](const std::string &shard_key,
                   const std::vector<std::span<const char>> &values,
                   const ReplicateConfig &write_config) {
                return store_->upsert_parts(shard_key, values, write_config);
            });
    }

    int upsert_tensor_with_tp_impl(
        const std::string &key, pybind11::object tensor,
        const ReplicateConfig &config = ReplicateConfig{}, int tp_size = 1,
        int split_dim = 0, const std::vector<ParallelAxisSpec> &axes = {}) {
        return execute_tp_tensor_write_impl(
            key, tensor, config, tp_size, split_dim, axes,
            "Failed to upsert tensor with tp",
            [this](const std::string &shard_key, const PyTensorInfo &info,
                   const ReplicateConfig &write_config) {
                return upsert_tensor_impl(shard_key, info, write_config);
            });
    }

    int execute_upsert_tensor_with_parallelism_route(
        const std::string &key, pybind11::object tensor,
        const ResolvedParallelismWriteRequest &request,
        const ReplicateConfig &config) {
        return execute_parallelism_tensor_write_route(
            key, tensor, request, config, "upsert_tensor_with_parallelism",
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const ReplicateConfig &write_config) {
                return upsert_tensor_impl(write_key, write_tensor,
                                          write_config);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const WriterPartitionSpec &writer,
                   const ReplicateConfig &write_config) {
                return upsert_tensor_with_writer_shards(write_key, write_tensor,
                                                        writer, write_config);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const ReplicateConfig &write_config, int rank, int size,
                   int split_dim) {
                return upsert_tensor_with_tp_impl(
                    write_key, write_tensor, write_config, size, split_dim);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const ReplicateConfig &write_config, int rank, int size,
                   int split_dim, const std::vector<ParallelAxisSpec> &axes) {
                return upsert_tensor_with_tp_impl(write_key, write_tensor,
                                                  write_config, size, split_dim,
                                                  axes);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const TensorParallelismSpec &parallelism_spec,
                   const ReplicateConfig &write_config) {
                return upsert_direct_parallelism_shard(
                    write_key, write_tensor, parallelism_spec, write_config);
            });
    }

    int upsert_tensor_with_parallelism(
        const std::string &key, pybind11::object tensor,
        const py::object &parallelism = py::none(),
        const ReplicateConfig &config = ReplicateConfig{},
        const py::object &writer_partition = py::none()) {
        auto request = resolve_parallelism_write_request(
            parallelism, writer_partition, "upsert_tensor_with_parallelism");
        if (!request.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return execute_upsert_tensor_with_parallelism_route(key, tensor,
                                                            *request, config);
    }

    int execute_upsert_tensor_with_parallelism_from_route(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const ResolvedParallelismWriteRequest &request,
        const ReplicateConfig &config) {
        return execute_parallelism_tensor_write_from_route(
            key, buffer_ptr, size, request, config,
            "upsert_tensor_with_parallelism_from",
            [this](const std::string &write_key, uintptr_t write_buffer_ptr,
                   size_t write_size, const ReplicateConfig &write_config) {
                return execute_parallelism_direct_tensor_write_from(
                    write_key, write_buffer_ptr, write_size, write_config,
                    "upsert_tensor_with_parallelism_from",
                    [this](const std::string &direct_key,
                           uintptr_t direct_buffer_ptr, size_t direct_size,
                           const ReplicateConfig &direct_config =
                               ReplicateConfig{}) {
                        if (is_default_replicate_config(direct_config)) {
                            return upsert_tensor_from(
                                direct_key, direct_buffer_ptr, direct_size);
                        }
                        py::gil_scoped_release release_gil;
                        return store_->upsert_from(
                            direct_key,
                            reinterpret_cast<void *>(direct_buffer_ptr),
                            direct_size, direct_config);
                    });
            },
            [this](const std::string &write_key, uintptr_t write_buffer_ptr,
                   size_t write_size, const WriterPartitionSpec &writer,
                   const ReplicateConfig &write_config) {
                return upsert_tensor_with_writer_shards_from(
                    write_key, write_buffer_ptr, write_size, writer,
                    write_config);
            },
            [this](const std::string &write_key, uintptr_t write_buffer_ptr,
                   size_t write_size, int tp_size, int split_dim,
                   const std::vector<ParallelAxisSpec> &axes,
                   const ReplicateConfig &write_config) {
                return execute_tp_tensor_write_from_impl(
                    write_key, write_buffer_ptr, write_size, write_config,
                    tp_size, split_dim, axes,
                    "upsert_tensor_with_parallelism_from",
                    [this](const std::string &shard_key,
                           const std::vector<std::span<const char>> &values,
                           const ReplicateConfig &parts_config) {
                        return execute_tensor_parts_write(
                            shard_key, values, parts_config,
                            TensorWriteStoreOps{"upsert_parts"},
                            [this](const std::string &final_key,
                                   const std::vector<std::span<const char>>
                                       &final_values,
                                   const ReplicateConfig &final_config) {
                                return store_->upsert_parts(
                                    final_key, final_values, final_config);
                            });
                    });
            },
            [this](const std::string &write_key, uintptr_t write_buffer_ptr,
                   size_t write_size,
                   const TensorParallelismSpec &parallelism_spec,
                   const ReplicateConfig &write_config) {
                return upsert_direct_parallelism_shard_from(
                    write_key, write_buffer_ptr, write_size, parallelism_spec,
                    write_config);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const ResolvedParallelismWriteRequest &write_request,
                   const ReplicateConfig &write_config) {
                return execute_upsert_tensor_with_parallelism_route(
                    write_key, write_tensor, write_request, write_config);
            });
    }

    int upsert_tensor_with_parallelism_from(
        const std::string &key, uintptr_t buffer_ptr, size_t size,
        const py::object &parallelism = py::none(),
        const ReplicateConfig &config = ReplicateConfig{},
        const py::object &writer_partition = py::none()) {
        auto request = resolve_parallelism_write_request(
            parallelism, writer_partition,
            "upsert_tensor_with_parallelism_from");
        if (!request.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return execute_upsert_tensor_with_parallelism_from_route(
            key, buffer_ptr, size, *request, config);
    }

    std::vector<int> batch_upsert_tensor_from(
        const std::vector<std::string> &keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (use_dummy_client_) {
            LOG(ERROR)
                << "batch_upsert_tensor_from is not supported for dummy client";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (keys.empty()) {
            return std::vector<int>();
        }
        if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
            LOG(ERROR) << "Size mismatch: keys, buffer_ptrs, and sizes must "
                          "have the same length";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        for (size_t i = 0; i < sizes.size(); ++i) {
            if (buffer_ptrs[i] == 0) {
                LOG(ERROR) << "Buffer pointer at index " << i
                           << " cannot be null";
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            if (sizes[i] <= sizeof(TensorMetadata)) {
                LOG(ERROR) << "Buffer size at index " << i
                           << " too small for tensor metadata";
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }
        }
        std::vector<void *> buffers;
        buffers.reserve(buffer_ptrs.size());
        for (uintptr_t ptr : buffer_ptrs) {
            buffers.push_back(reinterpret_cast<void *>(ptr));
        }
        py::gil_scoped_release release_gil;
        return store_->batch_upsert_from(keys, buffers, sizes,
                                         ReplicateConfig{});
    }

    std::vector<int> batch_upsert_tensor_with_parallelism_from(
        const std::vector<std::string> &keys,
        const std::vector<uintptr_t> &buffer_ptrs,
        const std::vector<size_t> &sizes,
        const py::object &parallelisms = py::none(),
        const ReplicateConfig &config = ReplicateConfig{},
        const py::object &writer_partitions = py::none()) {
        return execute_batch_parallelism_write_requests(
            keys, buffer_ptrs.size(), parallelisms, writer_partitions,
            "batch_upsert_tensor_with_parallelism_from",
            [this, &keys, &buffer_ptrs, &sizes, &config]() {
                if (is_default_replicate_config(config)) {
                    return batch_upsert_tensor_from(keys, buffer_ptrs, sizes);
                }
                return execute_batch_tensor_write_from(
                    "batch_upsert_tensor_with_parallelism_from",
                    "Size mismatch: keys, buffer_ptrs, and sizes must have the "
                    "same length",
                    keys, buffer_ptrs, sizes, config,
                    [this](const std::vector<std::string> &write_keys,
                           const std::vector<void *> &buffers,
                           const std::vector<size_t> &buffer_sizes,
                           const ReplicateConfig &write_config) {
                        py::gil_scoped_release release_gil;
                        return store_->batch_upsert_from(
                            write_keys, buffers, buffer_sizes, write_config);
                    });
            },
            [this, &keys, &buffer_ptrs, &sizes, &config](
                size_t i, const py::handle &parallelism) {
                return upsert_tensor_with_parallelism_from(
                    keys[i], buffer_ptrs[i], sizes[i],
                    py::reinterpret_borrow<py::object>(parallelism), config);
            },
            [this, &keys, &buffer_ptrs, &sizes, &config](
                size_t i, const py::handle &writer_partition) {
                return upsert_tensor_with_parallelism_from(
                    keys[i], buffer_ptrs[i], sizes[i], py::none(), config,
                    py::reinterpret_borrow<py::object>(writer_partition));
            });
    }

    std::vector<int> batch_upsert_tensor_impl(
        const std::vector<std::string> &keys,
        const std::vector<PyTensorInfo> &infos,
        const ReplicateConfig &config = ReplicateConfig{}) {
        return batch_write_tensor_impl(
            keys, infos, config, "upsert",
            [this, &config](const std::vector<std::string> &write_keys,
                            const std::vector<void *> &buffer_ptrs,
                            const std::vector<size_t> &buffer_sizes) {
                return store_->batch_upsert_from(write_keys, buffer_ptrs,
                                                 buffer_sizes, config);
            });
    }

    std::vector<int> batch_upsert_tensor_impl(
        const std::vector<std::string> &keys,
        const pybind11::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}) {
        std::vector<PyTensorInfo> infos(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            infos[i] = extract_tensor_info(tensors_list[i], keys[i]);
        }
        return batch_upsert_tensor_impl(keys, infos, config);
    }

    std::vector<int> batch_upsert_tensor(const std::vector<std::string> &keys,
                                         const pybind11::list &tensors_list) {
        if (!is_client_initialized() || use_dummy_client_)
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));

        if (keys.size() != tensors_list.size() || keys.empty()) {
            if (!keys.empty()) LOG(ERROR) << "Size mismatch in batch_upsert";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        return batch_upsert_tensor_impl(keys, tensors_list, ReplicateConfig{});
    }

    int upsert_tensor_from(const std::string &key, uintptr_t buffer_ptr,
                           size_t size) {
        if (buffer_ptr == 0) {
            LOG(ERROR) << "Buffer pointer cannot be null";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        void *buffer = reinterpret_cast<void *>(buffer_ptr);
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (use_dummy_client_) {
            LOG(ERROR)
                << "upsert_tensor_from is not supported for dummy client";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (size <= sizeof(TensorMetadata)) {
            LOG(ERROR) << "Buffer size too small for tensor metadata";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        py::gil_scoped_release release_gil;
        return store_->upsert_from(key, buffer, size, ReplicateConfig{});
    }

    std::vector<int> batch_upsert_tensor_with_parallelism(
        const std::vector<std::string> &keys,
        const pybind11::list &tensors_list,
        const py::object &parallelisms = py::none(),
        const ReplicateConfig &config = ReplicateConfig{},
        const py::object &writer_partitions = py::none()) {
        return execute_batch_parallelism_write_requests(
            keys, tensors_list.size(), parallelisms, writer_partitions,
            "batch_upsert_tensor_with_parallelism",
            [this, &keys, &tensors_list, &config]() {
                if (!is_client_initialized() || use_dummy_client_) {
                    return std::vector<int>(
                        keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                }
                int validate_result = validate_replicate_config(config);
                if (validate_result) {
                    return std::vector<int>(keys.size(), validate_result);
                }
                return batch_upsert_tensor_impl(keys, tensors_list, config);
            },
            [this, &keys, &tensors_list, &config](
                size_t i, const py::handle &parallelism) {
                return upsert_tensor_with_parallelism(
                    keys[i], tensors_list[i],
                    py::reinterpret_borrow<py::object>(parallelism), config);
            },
            [this, &keys, &tensors_list, &config](
                size_t i, const py::handle &writer_partition) {
                return upsert_tensor_with_parallelism(
                    keys[i], tensors_list[i], py::none(), config,
                    py::reinterpret_borrow<py::object>(writer_partition));
            });
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

    int upsert_pub_tensor(const std::string &key, pybind11::object tensor,
                          const ReplicateConfig &config = ReplicateConfig{}) {
        if (!is_client_initialized() || use_dummy_client_) {
            LOG(ERROR) << "Client not initialized or Dummy client not "
                          "supported for tensors";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        int validate_result = validate_replicate_config(config);
        if (validate_result) return validate_result;

        return upsert_tensor_impl(key, tensor, config);
    }

    std::vector<int> batch_upsert_pub_tensor(
        const std::vector<std::string> &keys,
        const pybind11::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}) {
        if (!is_client_initialized() || use_dummy_client_)
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));

        if (keys.size() != tensors_list.size() || keys.empty()) {
            if (!keys.empty())
                LOG(ERROR) << "Size mismatch in batch_upsert_pub";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        int validate_result = validate_replicate_config(config);
        if (validate_result)
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));

        return batch_upsert_tensor_impl(keys, tensors_list, config);
    }

    // --- End Upsert tensor methods ---
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

    int save_tensor_to_safetensor(const std::string &key,
                                  py::object file_name_obj = py::none()) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        if (use_dummy_client_) {
            LOG(ERROR) << "save_tensor_to_safetensor is not supported for "
                       << "dummy client now";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        pybind11::object tensor = get_tensor(key);
        if (tensor.is_none()) {
            LOG(ERROR) << "Failed to fetch tensor for key: " << key;
            return to_py_ret(ErrorCode::FILE_NOT_FOUND);
        }

        try {
            std::string resolved_file_name =
                file_name_obj.is_none() ? key
                                        : file_name_obj.cast<std::string>();
            auto safetensors_torch = py::module_::import("safetensors.torch");
            py::dict payload;
            payload[py::str(key)] = tensor;
            safetensors_torch.attr("save_file")(payload, resolved_file_name);
            return 0;
        } catch (const pybind11::error_already_set &e) {
            LOG(ERROR) << "Failed to save tensor to safetensor file: "
                       << e.what();
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
    }

    pybind11::object load_tensor_from_safetensor(py::object key_obj,
                                                 const std::string &file_name) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return pybind11::none();
        }

        if (use_dummy_client_) {
            LOG(ERROR) << "load_tensor_from_safetensor is not supported for "
                       << "dummy client now";
            return pybind11::none();
        }

        try {
            auto safetensors_torch = py::module_::import("safetensors.torch");
            py::dict loaded = safetensors_torch.attr("load_file")(file_name);
            if (py::len(loaded) == 0) {
                LOG(ERROR) << "No tensors found in safetensor file: "
                           << file_name;
                return pybind11::none();
            }

            std::string target_store_key =
                key_obj.is_none() ? file_name : key_obj.cast<std::string>();

            py::object selected_dict_key;
            if (!key_obj.is_none()) {
                py::str desired_key(target_store_key);
                if (py::cast<bool>(loaded.attr("__contains__")(desired_key))) {
                    selected_dict_key = desired_key;
                } else {
                    py::list dict_keys(loaded.attr("keys")());
                    selected_dict_key = dict_keys[0];
                    LOG(WARNING)
                        << "Key " << target_store_key
                        << " not found in safetensor file; using first entry";
                }
            } else {
                py::list dict_keys(loaded.attr("keys")());
                selected_dict_key = dict_keys[0];
            }

            py::object tensor = loaded[selected_dict_key];
            int rc = put_tensor(target_store_key, tensor);
            if (rc != 0) {
                LOG(ERROR) << "Failed to store tensor for key "
                           << target_store_key << ", rc=" << rc;
                return pybind11::none();
            }

            return tensor;
        } catch (const pybind11::error_already_set &e) {
            LOG(ERROR) << "Failed to load tensor from safetensor file: "
                       << e.what();
            return pybind11::none();
        }
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
        .def_readwrite("with_hard_pin", &ReplicateConfig::with_hard_pin)
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
                << ", created_at=" << self.created_at_ms_epoch
                << ", last_updated_at=" << self.last_updated_at_ms_epoch
                << ", assigned_client=" << self.assigned_client
                << ", message=\"" << self.message << "\")";
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

    py::class_<ParallelAxisSpec>(m, "ParallelAxis")
        .def(py::init<>())
        .def_readwrite("kind", &ParallelAxisSpec::kind)
        .def_readwrite("rank", &ParallelAxisSpec::rank)
        .def_readwrite("size", &ParallelAxisSpec::size)
        .def_readwrite("split_dim", &ParallelAxisSpec::split_dim)
        .def_readwrite("expert_id", &ParallelAxisSpec::expert_id)
        .def_readwrite("stage_id", &ParallelAxisSpec::stage_id);

    py::class_<TensorParallelismSpec>(m, "TensorParallelism")
        .def(py::init<>())
        .def_readwrite("axes", &TensorParallelismSpec::axes);

    py::class_<ReadTargetSpec>(m, "ReadTarget")
        .def(py::init<>())
        .def_property(
            "mode",
            [](const ReadTargetSpec &self) {
                switch (self.mode) {
                    case ReadTargetMode::AS_STORED:
                        return std::string("as_stored");
                    case ReadTargetMode::SHARD:
                        return std::string("shard");
                    case ReadTargetMode::FULL:
                        return std::string("full");
                }
                return std::string("as_stored");
            },
            [](ReadTargetSpec &self, const std::string &mode) {
                auto parsed = parse_read_target_mode(py::str(mode));
                if (!parsed.has_value()) {
                    throw std::runtime_error("Unsupported ReadTarget mode");
                }
                self.mode = *parsed;
            })
        .def_readwrite("parallelism", &ReadTargetSpec::parallelism);

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
               const py::object &engine = py::none(),
               bool enable_ssd_offload = false,
               const std::string &ssd_offload_path = "") {
                auto real_client = self.init_real_client();
                std::shared_ptr<mooncake::TransferEngine> transfer_engine =
                    nullptr;
                if (!engine.is_none()) {
                    transfer_engine =
                        engine.cast<std::shared_ptr<TransferEngine>>();
                }
                return real_client->setup_real(
                    local_hostname, metadata_server, global_segment_size,
                    local_buffer_size, protocol, rdma_devices,
                    master_server_addr, transfer_engine, "", enable_ssd_offload,
                    ssd_offload_path);
            },
            py::arg("local_hostname"), py::arg("metadata_server"),
            py::arg("global_segment_size"), py::arg("local_buffer_size"),
            py::arg("protocol"), py::arg("rdma_devices"),
            py::arg("master_server_addr"), py::arg("engine") = py::none(),
            py::arg("enable_ssd_offload") = false,
            py::arg("ssd_offload_path") = "")
        .def(
            "setup",
            [](MooncakeStorePyWrapper &self, const py::dict &config_dict) {
                auto real_client = self.init_real_client();

                // Convert py::dict to ConfigDict (all values as strings)
                ConfigDict config;
                for (auto item : config_dict) {
                    std::string key = py::str(item.first);
                    std::string value = py::str(item.second);
                    config[key] = value;
                }

                auto result = real_client->setup_internal(config);
                return result.has_value() ? 0
                                          : static_cast<int>(result.error());
            },
            py::arg("config"),
            "Setup the store with a configuration dictionary.\n"
            "Supported keys:\n"
            "  local_hostname (required): Local hostname.\n"
            "  metadata_server (required): Metadata server address.\n"
            "  global_segment_size: Global segment size (default 16MB).\n"
            "  local_buffer_size: Local buffer size (default 16MB).\n"
            "  protocol: Transfer protocol (default 'tcp').\n"
            "  rdma_devices: RDMA device list.\n"
            "  master_server_addr: Master server address.\n"
            "  ipc_socket_path: IPC socket path.\n"
            "  enable_ssd_offload: Enable SSD offload (default false).\n"
            "  ssd_offload_path: SSD storage directory path (overrides env "
            "var).")
        .def(
            "setup_dummy",
            [](MooncakeStorePyWrapper &self, size_t mem_pool_size,
               size_t local_buffer_size, const std::string &server_address) {
                auto &resource_tracker = ResourceTracker::getInstance();
                self.use_dummy_client_ = true;
                self.store_ = std::make_shared<DummyClient>();
                resource_tracker.registerInstance(
                    std::static_pointer_cast<PyClient>(self.store_));
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
        .def(
            "batch_remove",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys, bool force) {
                py::gil_scoped_release release;
                return self.store_->batchRemove(keys, force);
            },
            py::arg("keys"), py::arg("force") = false,
            "Batch remove objects by keys. Returns a list of status codes "
            "(0=success, negative=error code) for each key.")
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
        .def("health_check", &MooncakeStorePyWrapper::health_check,
             "Health check for store connectivity. "
             "Returns 0 if healthy, 1 if not initialized/closed, "
             "2 if master unreachable.")
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
        .def("save_tensor_to_safetensor",
             &MooncakeStorePyWrapper::save_tensor_to_safetensor, py::arg("key"),
             py::arg("file_name") = py::none(),
             "Export a tensor stored under the given key into a safetensors "
             "file. "
             "If file_name is not provided, the key will be used as the "
             "filename.")
        .def("load_tensor_from_safetensor",
             &MooncakeStorePyWrapper::load_tensor_from_safetensor,
             py::arg("key") = py::none(), py::arg("file_name"),
             "Load a tensor from a safetensors file and store it back in "
             "Mooncake.\n"
             "If the 'key' parameter is not provided, the file_name is used as "
             "the store key.\n"
             "If the provided key is not found in the safetensor file, the "
             "first tensor in the file is used and a warning is logged.")
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
            py::arg("split_dim") = 0,
            "Get a batch of PyTorch tensor shards from the store directly into "
            "pre-allocated buffers for a given Tensor Parallel rank.")
        .def("put_tensor_from", &MooncakeStorePyWrapper::put_tensor_from,
             py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
             "Put a tensor directly from a pre-allocated buffer. Buffer layout "
             "must be [TensorMetadata][tensor data], same as get_tensor_into.")
        .def("batch_put_tensor_from",
             &MooncakeStorePyWrapper::batch_put_tensor_from, py::arg("keys"),
             py::arg("buffer_ptrs"), py::arg("sizes"),
             "Put tensors directly from pre-allocated buffers for multiple "
             "keys. Each buffer layout: [TensorMetadata][tensor data].")
        .def("put_tensor_with_tp_from",
             &MooncakeStorePyWrapper::put_tensor_with_tp_from, py::arg("key"),
             py::arg("buffer_ptr"), py::arg("size"), py::arg("tp_rank") = 0,
             py::arg("tp_size") = 1, py::arg("split_dim") = 0,
             "Put a full tensor directly from a pre-allocated buffer for "
             "Tensor Parallelism. The buffer is split internally and stored "
             "under key_tp_<rank> for all ranks.")
        .def("batch_put_tensor_with_tp_from",
             &MooncakeStorePyWrapper::batch_put_tensor_with_tp_from,
             py::arg("base_keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
             py::arg("tp_rank") = 0, py::arg("tp_size") = 1,
             py::arg("split_dim") = 0,
             "Put a batch of full tensors directly from pre-allocated "
             "buffers for Tensor Parallelism. Each buffer is internally split "
             "and stored under key_tp_<rank> for all ranks.")
        .def("upsert_tensor", &MooncakeStorePyWrapper::upsert_tensor,
             py::arg("key"), py::arg("tensor"),
             "Upsert a PyTorch tensor into the store (insert or update)")
        .def("upsert_tensor_from", &MooncakeStorePyWrapper::upsert_tensor_from,
             py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
             "Upsert a tensor directly from a pre-allocated buffer. Buffer "
             "layout must be [TensorMetadata][tensor data].")
        .def("batch_upsert_tensor_from",
             &MooncakeStorePyWrapper::batch_upsert_tensor_from, py::arg("keys"),
             py::arg("buffer_ptrs"), py::arg("sizes"),
             "Upsert tensors directly from pre-allocated buffers for "
             "multiple keys. Each buffer layout: [TensorMetadata][tensor "
             "data].")
        .def("batch_upsert_tensor",
             &MooncakeStorePyWrapper::batch_upsert_tensor, py::arg("keys"),
             py::arg("tensors_list"),
             "Upsert a batch of PyTorch tensors into the store (insert or "
             "update)")
        .def("upsert_pub_tensor", &MooncakeStorePyWrapper::upsert_pub_tensor,
             py::arg("key"), py::arg("tensor"),
             py::arg("config") = ReplicateConfig{},
             "Upsert a PyTorch tensor with configurable replication settings")
        .def("batch_upsert_pub_tensor",
             &MooncakeStorePyWrapper::batch_upsert_pub_tensor, py::arg("keys"),
             py::arg("tensors_list"), py::arg("config") = ReplicateConfig{},
             "Batch upsert PyTorch tensors with configurable replication "
             "settings")
        .def("get_tensor_with_parallelism",
             &MooncakeStorePyWrapper::get_tensor_with_parallelism,
             py::arg("key"), py::arg("target") = py::none(),
             "Get a PyTorch tensor from the store using a ReadTarget request.")
        .def("batch_get_tensor_with_parallelism",
             &MooncakeStorePyWrapper::batch_get_tensor_with_parallelism,
             py::arg("keys"), py::arg("targets") = py::none(),
             "Get a batch of PyTorch tensors from the store using ReadTarget "
             "requests.")
        .def("get_tensor_with_parallelism_into",
             &MooncakeStorePyWrapper::get_tensor_with_parallelism_into,
             py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
             py::arg("target") = py::none(),
             "Get a PyTorch tensor from the store directly into a "
             "pre-allocated buffer using a ReadTarget request.")
        .def("batch_get_tensor_with_parallelism_into",
             &MooncakeStorePyWrapper::batch_get_tensor_with_parallelism_into,
             py::arg("keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
             py::arg("targets") = py::none(),
             "Get a batch of PyTorch tensors into pre-allocated buffers using "
             "ReadTarget requests.")
        .def("put_tensor_with_parallelism",
             &MooncakeStorePyWrapper::put_tensor_with_parallelism,
             py::arg("key"), py::arg("tensor"),
             py::arg("parallelism") = py::none(),
             py::arg("config") = ReplicateConfig{},
             py::arg("writer_partition") = py::none(),
             "Put a PyTorch tensor into the store using a TensorParallelism "
             "request or a writer_partition request.")
        .def("batch_put_tensor_with_parallelism",
             &MooncakeStorePyWrapper::batch_put_tensor_with_parallelism,
             py::arg("keys"), py::arg("tensors_list"),
             py::arg("parallelisms") = py::none(),
             py::arg("config") = ReplicateConfig{},
             py::arg("writer_partitions") = py::none(),
             "Put a batch of PyTorch tensors into the store using "
             "TensorParallelism requests or writer_partition requests.")
        .def("put_tensor_with_parallelism_from",
             &MooncakeStorePyWrapper::put_tensor_with_parallelism_from,
             py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
             py::arg("parallelism") = py::none(),
             py::arg("config") = ReplicateConfig{},
             py::arg("writer_partition") = py::none(),
             "Put a tensor directly from a pre-allocated buffer using a "
             "TensorParallelism request or a writer_partition request.")
        .def("batch_put_tensor_with_parallelism_from",
             &MooncakeStorePyWrapper::batch_put_tensor_with_parallelism_from,
             py::arg("keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
             py::arg("parallelisms") = py::none(),
             py::arg("config") = ReplicateConfig{},
             py::arg("writer_partitions") = py::none(),
             "Put a batch of tensors directly from pre-allocated buffers using "
             "TensorParallelism requests or writer_partition requests.")
        .def("upsert_tensor_with_parallelism",
             &MooncakeStorePyWrapper::upsert_tensor_with_parallelism,
             py::arg("key"), py::arg("tensor"),
             py::arg("parallelism") = py::none(),
             py::arg("config") = ReplicateConfig{},
             py::arg("writer_partition") = py::none(),
             "Upsert a PyTorch tensor into the store using a TensorParallelism "
             "request or a writer_partition request.")
        .def("upsert_tensor_with_parallelism_from",
             &MooncakeStorePyWrapper::upsert_tensor_with_parallelism_from,
             py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
             py::arg("parallelism") = py::none(),
             py::arg("config") = ReplicateConfig{},
             py::arg("writer_partition") = py::none(),
             "Upsert a tensor directly from a pre-allocated buffer using a "
             "TensorParallelism request or a writer_partition request.")
        .def("batch_upsert_tensor_with_parallelism_from",
             &MooncakeStorePyWrapper::batch_upsert_tensor_with_parallelism_from,
             py::arg("keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
             py::arg("parallelisms") = py::none(),
             py::arg("config") = ReplicateConfig{},
             py::arg("writer_partitions") = py::none(),
             "Upsert a batch of tensors directly from pre-allocated buffers "
             "using TensorParallelism requests or writer_partition requests.")
        .def("batch_upsert_tensor_with_parallelism",
             &MooncakeStorePyWrapper::batch_upsert_tensor_with_parallelism,
             py::arg("keys"), py::arg("tensors_list"),
             py::arg("parallelisms") = py::none(),
             py::arg("config") = ReplicateConfig{},
             py::arg("writer_partitions") = py::none(),
             "Upsert a batch of PyTorch tensors into the store using "
             "TensorParallelism requests or writer_partition requests.")
        .def(
            "upsert_from",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               uintptr_t buffer_ptr, size_t size,
               const ReplicateConfig &config = ReplicateConfig{}) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
                return self.store_->upsert_from(key, buffer, size, config);
            },
            py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
            py::arg("config") = ReplicateConfig{},
            "Upsert object data directly from a pre-allocated buffer")
        .def(
            "batch_upsert_from",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<uintptr_t> &buffer_ptrs,
               const std::vector<size_t> &sizes,
               const ReplicateConfig &config = ReplicateConfig{}) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<int>(
                        keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                }
                std::vector<void *> buffers;
                buffers.reserve(buffer_ptrs.size());
                for (uintptr_t ptr : buffer_ptrs) {
                    buffers.push_back(reinterpret_cast<void *>(ptr));
                }
                py::gil_scoped_release release;
                return self.store_->batch_upsert_from(keys, buffers, sizes,
                                                      config);
            },
            py::arg("keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
            py::arg("config") = ReplicateConfig{},
            "Upsert object data directly from pre-allocated buffers for "
            "multiple keys")
        .def(
            "upsert",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               py::buffer buf,
               const ReplicateConfig &config = ReplicateConfig{}) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                py::buffer_info info = buf.request(/*writable=*/false);
                py::gil_scoped_release release;
                return self.store_->upsert(
                    key,
                    std::span<const char>(static_cast<char *>(info.ptr),
                                          static_cast<size_t>(info.size)),
                    config);
            },
            py::arg("key"), py::arg("value"),
            py::arg("config") = ReplicateConfig{},
            "Upsert raw bytes into the store (insert or update)")
        .def(
            "upsert_parts",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               py::args parts,
               const ReplicateConfig &config = ReplicateConfig{}) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
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

                py::gil_scoped_release unlock;
                return self.store_->upsert_parts(key, spans, config);
            },
            py::arg("key"), py::arg("config") = ReplicateConfig{},
            "Upsert multiple byte parts as a single object (insert or update)")
        .def(
            "upsert_batch",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<py::buffer> &buffers,
               const ReplicateConfig &config = ReplicateConfig{}) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
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
                return self.store_->upsert_batch(keys, spans, config);
            },
            py::arg("keys"), py::arg("values"),
            py::arg("config") = ReplicateConfig{},
            "Batch upsert raw bytes for multiple keys (insert or update)")
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
                return self.store_->get_into(key, buffer, size);
            },
            py::arg("key"), py::arg("buffer_ptr"), py::arg("size"),
            "Get object data directly into a pre-allocated buffer")
        .def(
            "get_into_ranges",
            [](MooncakeStorePyWrapper &self,
               const std::vector<uintptr_t> &buffer_ptrs,
               const std::vector<std::vector<std::string>> &all_keys,
               const std::vector<std::vector<std::vector<size_t>>>
                   &all_dst_offsets,
               const std::vector<std::vector<std::vector<size_t>>>
                   &all_src_offsets,
               const std::vector<std::vector<std::vector<size_t>>> &all_sizes) {
                std::vector<void *> buffers;
                buffers.reserve(buffer_ptrs.size());
                for (uintptr_t ptr : buffer_ptrs) {
                    buffers.push_back(reinterpret_cast<void *>(ptr));
                }
                py::gil_scoped_release release;
                return self.store_->get_into_ranges(buffers, all_keys,
                                                    all_dst_offsets,
                                                    all_src_offsets, all_sizes);
            },
            py::arg("buffer_ptrs"), py::arg("all_keys"),
            py::arg("all_dst_offsets"), py::arg("all_src_offsets"),
            py::arg("all_sizes"),
            "Get multiple byte ranges from multiple objects into multiple "
            "pre-allocated buffers")
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
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                py::gil_scoped_release release;
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
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                void *buffer = reinterpret_cast<void *>(buffer_ptr);
                void *metadata_buffer =
                    reinterpret_cast<void *>(metadata_buffer_ptr);
                py::gil_scoped_release release;
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
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<int>{};
                }
                py::gil_scoped_release release;
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
            "batch_replica_clear",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::string &segment_name) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<std::string>{};
                }
                py::gil_scoped_release release;
                return self.store_->batch_replica_clear(keys, segment_name);
            },
            py::arg("keys"), py::arg("segment_name") = "",
            "Clear replicas for the given keys. Requires lease to be expired.")
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
