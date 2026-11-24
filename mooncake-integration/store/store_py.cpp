#include <nanobind/stl/map.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/unordered_map.h>
#include <nanobind/stl/variant.h>
#include <nanobind/stl/vector.h>
#include <numa.h>

#include <limits>
#include <mutex>
#include <optional>
#include <unordered_set>

#include "pyclient.h"
#include "dummy_client.h"
#include "real_client.h"
#include "common.h"
#include "types.h"
#include "memory_alloc.h"
#include "ssd_register_client.h"
#include "device/accelerator_registry.h"
#include "device/cuda_ipc_buffer.h"

#include <cstdlib>  // for atexit
#include <memory>

#include "integration_utils.h"
#include "buffer_pool.h"
#include "python_buffer.h"
#include "store_py_internal.h"

namespace py = nanobind;

namespace mooncake {
namespace {

// A serialized tensor object is laid out as [metadata][payload].  The raw
// *_tensor_from APIs accept host, pinned-host, and device addresses.  Reading
// metadata through a C++ pointer is invalid for device addresses, and a
// DummyClient cannot dereference a caller's pointer in the RealClient process.
// Keep the original addresses in the resulting parts. DummyClient and
// RealClient each perform the appropriate staging/IPC for non-local buffers.
struct RawTensorObjectParts {
    uintptr_t metadata_ptr = 0;
    size_t metadata_size = 0;
    uintptr_t payload_ptr = 0;
    size_t payload_size = 0;
};

std::optional<RawTensorObjectParts> make_raw_tensor_object_parts(
    uintptr_t buffer_ptr, size_t size, const char *operation_name) {
    if (buffer_ptr == 0 || size < sizeof(TensorMetadata) ||
        size > std::numeric_limits<uintptr_t>::max() - buffer_ptr) {
        LOG(ERROR) << operation_name << ": invalid tensor object buffer";
        return std::nullopt;
    }

    const auto &runtime =
        mooncake::device::GetAcceleratorRegistry().RuntimeAccelerators();
    TensorMetadata metadata{};
    if (!runtime.CopyToHost(&metadata,
                            reinterpret_cast<const void *>(buffer_ptr),
                            sizeof(metadata))) {
        LOG(ERROR) << operation_name << ": failed to read tensor metadata";
        return std::nullopt;
    }
    if (metadata.header.data_offset > std::numeric_limits<size_t>::max() ||
        metadata.header.data_bytes > std::numeric_limits<size_t>::max()) {
        LOG(ERROR) << operation_name << ": tensor object size overflows";
        return std::nullopt;
    }
    if (!ValidateTensorMetadata(metadata, size)) {
        LOG(ERROR) << operation_name << ": invalid tensor object metadata";
        return std::nullopt;
    }
    const size_t metadata_size =
        static_cast<size_t>(metadata.header.data_offset);
    const size_t payload_size = static_cast<size_t>(metadata.header.data_bytes);
    if (metadata_size > size || payload_size > size - metadata_size ||
        metadata_size > std::numeric_limits<uintptr_t>::max() - buffer_ptr) {
        LOG(ERROR) << operation_name << ": invalid tensor object range";
        return std::nullopt;
    }

    RawTensorObjectParts parts;
    parts.metadata_size = metadata_size;
    parts.payload_ptr = buffer_ptr + metadata_size;
    parts.payload_size = payload_size;
    if (payload_size >
        std::numeric_limits<uintptr_t>::max() - parts.payload_ptr) {
        LOG(ERROR) << operation_name << ": tensor payload address overflows";
        return std::nullopt;
    }
    // Keep the caller's original address in the part list. DummyClient uses
    // the registration table to validate/stage external buffers; replacing a
    // device metadata pointer with a private host copy would make that
    // pointer appear unregistered.
    parts.metadata_ptr = buffer_ptr;
    return parts;
}

py::tuple tensor_shape_tuple(const TensorMetadata &metadata) {
    std::vector<int64_t> shape_vec;
    shape_vec.reserve(metadata.header.ndim);
    for (int i = 0; i < metadata.header.ndim; i++) {
        shape_vec.push_back(metadata.layout.local_shape.dims[i]);
    }
    return py::tuple(py::cast(shape_vec));
}

py::object buffer_to_tensor(BufferHandle *buffer_handle, char *usr_buffer,
                            int64_t data_length, bool own_user_buffer = false) {
    if (!buffer_handle && !usr_buffer) return py::none();
    if (buffer_handle && usr_buffer) return py::none();

    bool take_ownership = !!buffer_handle || own_user_buffer;
    size_t total_length;
    char *exported_data;
    if (buffer_handle) {
        total_length = buffer_handle->size();
        if (total_length < sizeof(TensorMetadata)) {
            LOG(ERROR) << "Invalid data format: insufficient data for metadata";
            return py::none();
        }

        // Allocate memory to copy data out (array_creators usually take
        // ownership or we need a clean buffer)
        exported_data = new char[total_length];
        if (!exported_data) return py::none();

        if (!mooncake::device::GetAcceleratorRegistry()
                 .RuntimeAccelerators()
                 .CopyToHost(exported_data, buffer_handle->ptr(),
                             total_length)) {
            LOG(ERROR) << "Failed to copy buffer to host memory";
            delete[] exported_data;
            return py::none();
        }
    } else {
        exported_data = usr_buffer;
        if (data_length < 0) {
            if (take_ownership) {
                delete[] exported_data;
            }
            LOG(ERROR) << "Get tensor into failed with error code: "
                       << data_length;
            return py::none();
        }
        total_length = static_cast<size_t>(data_length);
        if (total_length < sizeof(TensorMetadata)) {
            if (take_ownership) {
                delete[] exported_data;
            }
            LOG(ERROR) << "Invalid data format: insufficient data for metadata";
            return py::none();
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
        return py::none();
    }

    int ndim = metadata.header.ndim;
    if (ndim < 0 || ndim > static_cast<int>(kMaxTensorDims)) {
        if (take_ownership) {
            delete[] exported_data;
        }
        LOG(ERROR) << "Invalid tensor metadata: ndim=" << ndim
                   << ", max supported=" << kMaxTensorDims;
        return py::none();
    }

    TensorDtype dtype_enum = static_cast<TensorDtype>(metadata.header.dtype);
    size_t tensor_size = parsed->data_bytes;
    size_t data_offset = parsed->data_offset;

    if (dtype_enum == TensorDtype::UNKNOWN) {
        if (take_ownership) {
            delete[] exported_data;
        }
        LOG(ERROR) << "Invalid tensor data or unknown dtype";
        return py::none();
    }

    int dtype_index = static_cast<int>(dtype_enum);
    if (dtype_index < 0 ||
        dtype_index >= static_cast<int>(array_creators.size())) {
        if (take_ownership) {
            delete[] exported_data;
        }
        LOG(ERROR) << "Unsupported dtype enum: " << dtype_index;
        return py::none();
    }

    std::unique_ptr<char[]> exported_data_guard(take_ownership ? exported_data
                                                               : nullptr);
    try {
        if (tensor_size == 0) {
            py::object dtype = tensor_dtype_to_torch_dtype(dtype_enum);
            if (dtype.is_none()) {
                LOG(ERROR) << "Unsupported dtype enum: " << dtype_index;
                return py::none();
            }
            py::object tensor = torch_module().attr("empty")(
                tensor_shape_tuple(metadata), py::arg("dtype") = dtype);
            return tensor;
        }

        // Construct numpy array
        py::object np_array = array_creators[dtype_index](
            exported_data, data_offset, tensor_size, take_ownership);
        exported_data_guard.release();

        // Reshape
        if (ndim > 0) {
            np_array = np_array.attr("reshape")(tensor_shape_tuple(metadata));
        }

        // Convert to torch tensor
        py::object tensor = torch_module().attr("from_numpy")(np_array);

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
        return py::none();
    }
}

std::optional<py::object> decode_raw_tensor_object_for_host(
    uintptr_t buffer_ptr, size_t size, std::vector<char> &host_buffer,
    const char *operation_name) {
    if (!make_raw_tensor_object_parts(buffer_ptr, size, operation_name)) {
        return std::nullopt;
    }

    char *tensor_buffer = reinterpret_cast<char *>(buffer_ptr);
    const auto &runtime =
        mooncake::device::GetAcceleratorRegistry().RuntimeAccelerators();
    if (runtime.FindDeviceForPointer(reinterpret_cast<void *>(buffer_ptr))) {
        host_buffer.resize(size);
        if (!runtime.CopyToHost(host_buffer.data(),
                                reinterpret_cast<void *>(buffer_ptr), size)) {
            LOG(ERROR) << operation_name
                       << ": failed to copy device tensor object to host";
            return std::nullopt;
        }
        tensor_buffer = host_buffer.data();
    }

    py::object tensor =
        buffer_to_tensor(nullptr, tensor_buffer, static_cast<int64_t>(size));
    if (tensor.is_none()) {
        LOG(ERROR) << operation_name << ": failed to decode tensor object";
        return std::nullopt;
    }
    return tensor;
}

py::tuple serialize_tensor_metadata(py::object tensor) {
    PyTensorInfo info = extract_tensor_info(tensor);
    if (!info.valid()) {
        throw py::value_error(
            "unsupported tensor for Mooncake tensor serialization");
    }

    TensorMetadata metadata = info.metadata;
    metadata.header.data_bytes = info.tensor_size;
    return py::make_tuple(py::bytes(reinterpret_cast<const char *>(&metadata),
                                    sizeof(TensorMetadata)),
                          info.data_ptr, info.tensor_size, info.owner);
}

size_t tensor_metadata_size() { return sizeof(TensorMetadata); }

bool tensor_destination_matches_metadata(const PyTensorInfo &target,
                                         const ParsedTensorMetadata &stored,
                                         const std::string &key,
                                         const std::string &context) {
    if (target.metadata.header.dtype != stored.metadata.header.dtype ||
        target.metadata.header.ndim != stored.metadata.header.ndim ||
        target.tensor_size != stored.data_bytes) {
        LOG(ERROR) << context << ": destination tensor mismatch for key "
                   << key;
        return false;
    }
    return TensorShapeToVector(target.metadata.layout.local_shape,
                               target.metadata.header.ndim) ==
           TensorShapeToVector(stored.metadata.layout.local_shape,
                               stored.metadata.header.ndim);
}

// Read tensor payloads directly into caller-owned CUDA buffers.  Metadata is
// fetched separately so no complete-object host staging buffer is needed.
template <typename ResultType>
bool apply_indexed_results(const char *context,
                           const std::vector<ResultType> &op_results,
                           const std::vector<size_t> &original_indices,
                           std::vector<ResultType> &results) {
    if (op_results.size() != original_indices.size()) {
        LOG(ERROR) << context << ": unexpected result count "
                   << op_results.size() << ", expected "
                   << original_indices.size();
        for (size_t index : original_indices) {
            results[index] =
                static_cast<ResultType>(toInt(ErrorCode::INTERNAL_ERROR));
        }
        return false;
    }
    for (size_t i = 0; i < op_results.size(); ++i) {
        results[original_indices[i]] = op_results[i];
    }
    return true;
}

py::object deserialize_tensor_from_bytes(py::bytes payload) {
    std::string data(payload.c_str(), payload.size());
    if (data.size() >
        static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        throw py::value_error("serialized tensor payload is too large");
    }

    char *owned_data = new char[data.size()];
    memcpy(owned_data, data.data(), data.size());
    py::object tensor = buffer_to_tensor(
        nullptr, owned_data, static_cast<int64_t>(data.size()), true);
    if (tensor.is_none()) {
        throw py::value_error("invalid serialized Mooncake tensor payload");
    }
    return tensor;
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

// Hold the exporter until the native operation returns.
inline std::span<const char> contiguous_buffer_span(const Py_buffer &info) {
    if (info.len > 0 && !PyBuffer_IsContiguous(&info, 'C')) {
        throw py::value_error(
            "value must be a C-contiguous buffer (bytes, bytearray, "
            "or a contiguous array); got a strided view");
    }
    return {static_cast<const char *>(info.buf), static_cast<size_t>(info.len)};
}

void append_tensor_write_buffers(const PyTensorInfo &info,
                                 std::vector<void *> &buffers,
                                 std::vector<size_t> &sizes) {
    buffers.push_back(const_cast<TensorMetadata *>(&info.metadata));
    sizes.push_back(info.metadata.header.data_offset);
    if (info.tensor_size > 0) {
        buffers.push_back(reinterpret_cast<void *>(info.data_ptr));
        sizes.push_back(info.tensor_size);
    }
}

void apply_tensor_batch_results(std::vector<int> &results,
                                const std::vector<size_t> &original_indices,
                                const std::vector<int> &op_results,
                                const char *operation_name) {
    if (op_results.size() != original_indices.size()) {
        LOG(ERROR) << operation_name << " returned unexpected result count";
        for (size_t index : original_indices) {
            results[index] = to_py_ret(ErrorCode::INTERNAL_ERROR);
        }
        return;
    }
    for (size_t i = 0; i < op_results.size(); ++i)
        results[original_indices[i]] = op_results[i];
}

}  // namespace

class RangedReadSnapshotPy {
   public:
    RangedReadSnapshotPy(std::shared_ptr<PyClient> store,
                         std::vector<std::string> keys)
        : store_(store), keys_(std::move(keys)) {
        snapshot_ = store->prepare_get_into_ranges_snapshot(keys_);
    }

    bool belongs_to(const std::shared_ptr<PyClient> &store) const {
        const auto owner = store_.lock();
        return owner && owner.get() == store.get();
    }

    std::vector<std::vector<std::vector<int64_t>>> get_into_ranges(
        const std::vector<void *> &buffers,
        const std::vector<std::vector<std::string>> &all_keys,
        const std::vector<std::vector<std::vector<size_t>>> &all_dst_offsets,
        const std::vector<std::vector<std::vector<size_t>>> &all_src_offsets,
        const std::vector<std::vector<std::vector<size_t>>> &all_sizes) {
        std::lock_guard<std::mutex> lock(mutex_);
        // A snapshot must not keep a closed Store and its segments alive;
        // reject use after close instead of reading through a torn-down
        // client.
        auto store = store_.lock();
        if (!store) {
            throw std::runtime_error(
                "RangedReadSnapshot is no longer usable: the Store was "
                "closed");
        }
        if (snapshot_.should_refresh()) {
            store->refresh_get_into_ranges_snapshot(snapshot_, keys_);
        }
        return store->get_into_ranges_from_snapshot(
            buffers, all_keys, all_dst_offsets, all_src_offsets, all_sizes,
            snapshot_);
    }

   private:
    // Weak by design: an idle snapshot must not extend the native client's
    // lifetime beyond close(), otherwise its segments stay published and a
    // later reader can observe a stale MEMORY replica (issue #5464).
    std::weak_ptr<PyClient> store_;
    std::vector<std::string> keys_;
    PyClient::RangedReadSnapshot snapshot_;
    std::mutex mutex_;
};

// Python-specific wrapper functions that handle GIL and return Python objects
class MooncakeStorePyWrapper {
   public:
    std::shared_ptr<PyClient> store_{nullptr};
    std::shared_ptr<RealClient> real_client_{nullptr};
    bool use_dummy_client_{false};

    MooncakeStorePyWrapper() = default;

    // Helper to initialize real client and register it
    std::shared_ptr<RealClient> init_real_client() {
        auto &resource_tracker = ResourceTracker::getInstance();
        auto real_client = RealClient::create();
        use_dummy_client_ = false;
        store_ = real_client;
        real_client_ = real_client;
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

    py::dict mount_segment(const std::string &path, size_t size, size_t offset,
                           const std::string &protocol,
                           const std::string &location) {
        py::dict result;
        result["ret"] = -1;
        result["segment_ids"] = py::list();

        auto real_client = std::dynamic_pointer_cast<RealClient>(store_);
        if (!real_client) {
            LOG(ERROR) << "mount_segment requires RealClient";
            return result;
        }
        std::vector<std::string> segment_ids;
        int ret;
        {
            py::gil_scoped_release release;
            ret = real_client->mountSegment(path, offset, size, protocol,
                                            location, segment_ids);
        }
        result["ret"] = ret;
        py::list ids;
        for (const auto &id : segment_ids) {
            ids.append(id);
        }
        result["segment_ids"] = ids;
        return result;
    }

    int unmount_segment(const std::vector<std::string> &segment_ids,
                        uint64_t grace_period_seconds = 0) {
        auto real_client = std::dynamic_pointer_cast<RealClient>(store_);
        if (!real_client) {
            LOG(ERROR) << "unmount_segment requires RealClient";
            return -1;
        }
        py::gil_scoped_release release;
        return real_client->unmountSegment(segment_ids, grace_period_seconds);
    }

    py::dict allocate_and_mount_segment(size_t size,
                                        const std::string &protocol,
                                        const std::string &location) {
        py::dict result;
        result["ret"] = -1;
        result["segment_ids"] = py::list();
        result["allocated_size"] = 0;

        auto real_client = std::dynamic_pointer_cast<RealClient>(store_);
        if (!real_client) {
            LOG(ERROR) << "allocate_and_mount_segment requires RealClient";
            return result;
        }
        std::vector<std::string> segment_ids;
        size_t allocated_size = 0;
        int ret;
        {
            py::gil_scoped_release release;
            ret = real_client->allocateAndMountSegment(
                size, protocol, location, segment_ids, &allocated_size);
        }
        result["ret"] = ret;
        result["segment_ids"] = py::cast(segment_ids);
        result["allocated_size"] = allocated_size;
        return result;
    }

    int unmount_and_free_segment(const std::vector<std::string> &segment_ids,
                                 uint64_t grace_period_seconds = 0) {
        auto real_client = std::dynamic_pointer_cast<RealClient>(store_);
        if (!real_client) {
            LOG(ERROR) << "unmount_and_free_segment requires RealClient";
            return -1;
        }
        py::gil_scoped_release release;
        return real_client->unmountAndFreeSegment(segment_ids,
                                                  grace_period_seconds);
    }

    int unmount_local_disk_segment(uint64_t grace_period_seconds = 0) {
        auto real_client = std::dynamic_pointer_cast<RealClient>(store_);
        if (!real_client) {
            LOG(ERROR) << "unmount_local_disk_segment requires RealClient";
            return -1;
        }
        py::gil_scoped_release release;
        return real_client->drainLocalDiskSegment(grace_period_seconds);
    }

    std::string get_tp_key_name(const std::string &base_key, int rank) const {
        return base_key + "_tp_" + std::to_string(rank);
    }

    py::object get(const std::string &key) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return py::bytes("\\0", 0);
        }

        const auto kNullString = py::bytes("\\0", 0);

        {
            py::gil_scoped_release release_gil;
            auto buffer_handle = store_->get_buffer(key);
            if (!buffer_handle) {
                py::gil_scoped_acquire acquire_gil;
                return kNullString;
            }

            py::gil_scoped_acquire acquire_gil;
            auto runtime_accelerator =
                mooncake::device::GetAcceleratorRegistry()
                    .RuntimeAccelerators();
            if (runtime_accelerator.FindDeviceForPointer(
                    buffer_handle->ptr())) {
                std::string host_buf(buffer_handle->size(), '\0');
                if (!runtime_accelerator.CopyToHost(host_buf.data(),
                                                    buffer_handle->ptr(),
                                                    buffer_handle->size())) {
                    LOG(ERROR) << "Failed to copy buffer to host memory";
                    return py::none();
                }
                return py::bytes(host_buf.data(), host_buf.size());
            }
            return py::bytes((char *)buffer_handle->ptr(),
                             buffer_handle->size());
        }
    }

    std::vector<py::bytes> get_batch(const std::vector<std::string> &keys) {
        const auto kNullString = py::bytes("\\0", 0);
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
            std::vector<py::bytes> results;
            results.reserve(batch_data.size());

            auto runtime_accelerator =
                mooncake::device::GetAcceleratorRegistry()
                    .RuntimeAccelerators();

            for (const auto &data : batch_data) {
                if (!data) {
                    results.emplace_back(kNullString);
                    continue;
                }
                if (runtime_accelerator.FindDeviceForPointer(data->ptr())) {
                    std::string host_buf(data->size(), '\0');
                    if (!runtime_accelerator.CopyToHost(
                            host_buf.data(), data->ptr(), data->size())) {
                        LOG(ERROR) << "Failed to copy buffer to host memory";
                        results.emplace_back(kNullString);
                        continue;
                    }
                    results.emplace_back(
                        py::bytes(host_buf.data(), host_buf.size()));
                } else {
                    results.emplace_back(
                        py::bytes((char *)data->ptr(), data->size()));
                }
            }
            return results;
        }
    }

    py::object get_tensor_with_tp(const std::string &key, int tp_rank = 0,
                                  int tp_size = 1, int split_dim = 0) {
        if (tp_size <= 1) return get_tensor(key);
        return get_tensor(get_tp_key_name(key, tp_rank));
    }

    py::list batch_get_tensor_with_tp(const std::vector<std::string> &base_keys,
                                      int tp_rank = 0, int tp_size = 1) {
        if (tp_size <= 1) return batch_get_tensor(base_keys);

        std::vector<std::string> shard_keys;
        shard_keys.reserve(base_keys.size());
        for (const auto &key : base_keys) {
            shard_keys.push_back(get_tp_key_name(key, tp_rank));
        }
        return batch_get_tensor(shard_keys);
    }

    py::object get_tensor(const std::string &key) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
            return py::none();
        }

        std::shared_ptr<BufferHandle> buffer_handle;
        {
            py::gil_scoped_release release_gil;
            buffer_handle = store_->get_buffer(key);
        }
        // Metadata parsing must happen with GIL held
        return buffer_to_tensor(buffer_handle.get(), NULL, 0);
    }

    py::list batch_get_tensor(const std::vector<std::string> &keys) {
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

    py::object get_tensor_into(const std::string &key, uintptr_t buffer_ptr,
                               size_t size) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return py::none();
        }
        if (buffer_ptr == 0) {
            LOG(ERROR) << "Buffer pointer cannot be null";
            return py::none();
        }

        char *buffer = reinterpret_cast<char *>(buffer_ptr);

        int64_t total_length;
        {
            py::gil_scoped_release release_gil;
            total_length = store_->get_into(key, buffer, size);
        }

        return buffer_to_tensor(NULL, buffer, total_length);
    }

    py::list batch_get_tensor_into(const std::vector<std::string> &keys,
                                   const std::vector<uintptr_t> &buffer_ptrs,
                                   const std::vector<size_t> &sizes) {
        auto invalid_params = [&keys]() {
            py::list empty_list;
            for (size_t i = 0; i < keys.size(); ++i) {
                empty_list.append(to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            return empty_list;
        };

        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return invalid_params();
        }
        if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
            LOG(ERROR) << "Size mismatch: keys, buffer_ptrs, and sizes must "
                          "have the same length";
            return invalid_params();
        }
        for (uintptr_t ptr : buffer_ptrs) {
            if (ptr == 0) {
                LOG(ERROR) << "Buffer pointer cannot be null";
                return invalid_params();
            }
        }

        std::vector<void *> buffers;
        buffers.reserve(buffer_ptrs.size());
        for (uintptr_t ptr : buffer_ptrs) {
            buffers.push_back(reinterpret_cast<void *>(ptr));
        }

        // Phase 1: Batch Get Buffers (GIL Released)
        std::vector<int64_t> total_lengths;
        {
            py::gil_scoped_release release_gil;
            // This internal call already handles logging for query failures
            total_lengths = store_->batch_get_into(keys, buffers, sizes);
        }

        if (total_lengths.size() != keys.size()) {
            LOG(ERROR) << "Unexpected batch_get_into result count "
                       << total_lengths.size() << ", expected " << keys.size();
            return invalid_params();
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

    std::vector<std::optional<ParsedTensorMetadata>>
    batch_get_tensor_metadata_prefixes(
        const std::vector<std::string> &keys, const std::string &context,
        const PyClient::QueryResultCache *snapshot = nullptr,
        std::vector<int64_t> *errors = nullptr) {
        std::vector<std::optional<ParsedTensorMetadata>> metadata(keys.size());
        if (keys.empty()) return metadata;

        const size_t scratch_size = keys.size() * sizeof(TensorMetadata);
        auto scratch = store_->allocate_client_buffer(scratch_size);
        if (!scratch) {
            LOG(ERROR) << context << ": failed to allocate metadata buffer";
            if (errors) {
                std::fill(errors->begin(), errors->end(),
                          to_py_ret(ErrorCode::NO_AVAILABLE_HANDLE));
            }
            return metadata;
        }

        std::vector<void *> buffers{scratch->ptr()};
        std::vector<std::vector<std::string>> all_keys{keys};
        std::vector<std::vector<std::vector<size_t>>> all_dst_offsets(1);
        std::vector<std::vector<std::vector<size_t>>> all_src_offsets(
            1, std::vector<std::vector<size_t>>(keys.size(), {0}));
        std::vector<std::vector<std::vector<size_t>>> all_sizes(
            1, std::vector<std::vector<size_t>>(keys.size(),
                                                {sizeof(TensorMetadata)}));
        all_dst_offsets[0].reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            all_dst_offsets[0].push_back({i * sizeof(TensorMetadata)});
        }

        std::vector<std::vector<std::vector<int64_t>>> results;
        {
            py::gil_scoped_release release_gil;
            results = snapshot ? real_client_->get_into_ranges_from_snapshot(
                                     buffers, all_keys, all_dst_offsets,
                                     all_src_offsets, all_sizes, *snapshot)
                               : store_->get_into_ranges(
                                     buffers, all_keys, all_dst_offsets,
                                     all_src_offsets, all_sizes, nullptr);
        }
        if (results.size() != 1 || results[0].size() != keys.size()) {
            LOG(ERROR) << context << ": metadata read result size mismatch";
            if (errors) {
                std::fill(errors->begin(), errors->end(),
                          to_py_ret(ErrorCode::INTERNAL_ERROR));
            }
            return metadata;
        }

        const char *base = static_cast<const char *>(scratch->ptr());
        for (size_t i = 0; i < keys.size(); ++i) {
            if (results[0][i].size() != 1 ||
                results[0][i][0] !=
                    static_cast<int64_t>(sizeof(TensorMetadata))) {
                if (errors) {
                    (*errors)[i] =
                        results[0][i].size() == 1 && results[0][i][0] < 0
                            ? results[0][i][0]
                            : to_py_ret(ErrorCode::INTERNAL_ERROR);
                }
                continue;
            }
            metadata[i] = parse_tensor_metadata_from_prefix(
                base + i * sizeof(TensorMetadata), context, keys[i]);
        }
        return metadata;
    }

    int64_t get_tensor_into_cuda(const std::string &key, py::object tensor) {
        py::list tensors;
        tensors.append(tensor);
        auto results = batch_get_tensor_into_cuda({key}, tensors);
        return results.empty() ? to_py_ret(ErrorCode::INVALID_PARAMS)
                               : results[0];
    }

    std::vector<int64_t> batch_get_tensor_into_cuda(
        const std::vector<std::string> &keys, py::list tensors_list) {
        std::vector<int64_t> results(keys.size(),
                                     to_py_ret(ErrorCode::INVALID_PARAMS));
        const std::string context = "batch_get_tensor_into_cuda";
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return results;
        }
        if (keys.size() != tensors_list.size()) {
            LOG(ERROR) << context << ": keys and tensors size mismatch";
            return results;
        }

        if (!use_dummy_client_) {
            if (keys.empty()) return results;
            // Keep both reads on one snapshot. Re-querying between the prefix
            // and payload can select a different version after an upsert.
            PyClient::QueryResultCache snapshot;
            {
                py::gil_scoped_release release_gil;
                auto queries = store_->batch_query(keys);
                if (queries.size() != keys.size()) {
                    return std::vector<int64_t>(
                        keys.size(), to_py_ret(ErrorCode::INTERNAL_ERROR));
                }
                snapshot.reserve(keys.size());
                for (size_t i = 0; i < keys.size(); ++i) {
                    snapshot.emplace(keys[i], std::move(queries[i]));
                }
            }
            auto metadata = batch_get_tensor_metadata_prefixes(
                keys, context, &snapshot, &results);
            std::vector<void *> buffers;
            std::vector<std::vector<std::string>> all_keys;
            std::vector<std::vector<std::vector<size_t>>> all_dst_offsets;
            std::vector<std::vector<std::vector<size_t>>> all_src_offsets;
            std::vector<std::vector<std::vector<size_t>>> all_sizes;
            std::vector<size_t> original_indices;
            buffers.reserve(keys.size());
            all_keys.reserve(keys.size());
            all_dst_offsets.reserve(keys.size());
            all_src_offsets.reserve(keys.size());
            all_sizes.reserve(keys.size());
            original_indices.reserve(keys.size());

            for (size_t i = 0; i < keys.size(); ++i) {
                PyTensorInfo target = extract_tensor_destination_info(
                    py::cast<py::object>(tensors_list[i]), keys[i]);
                if (!target.valid() || !metadata[i].has_value() ||
                    !tensor_destination_matches_metadata(target, *metadata[i],
                                                         keys[i], context)) {
                    continue;
                }
                const auto &stored = *metadata[i];
                if (stored.data_offset >
                    std::numeric_limits<size_t>::max() - stored.data_bytes) {
                    LOG(ERROR)
                        << context << " : tensor range overflows for key "
                        << keys[i];
                    continue;
                }
                if (stored.data_bytes == 0) {
                    results[i] = 0;
                    continue;
                }

                buffers.push_back(reinterpret_cast<void *>(target.data_ptr));
                all_keys.push_back({keys[i]});
                all_dst_offsets.push_back({{0}});
                all_src_offsets.push_back({{stored.data_offset}});
                all_sizes.push_back({{stored.data_bytes}});
                original_indices.push_back(i);
            }

            if (buffers.empty()) return results;

            std::vector<std::vector<std::vector<int64_t>>> range_results;
            {
                py::gil_scoped_release release_gil;
                range_results = real_client_->get_into_ranges_from_snapshot(
                    buffers, all_keys, all_dst_offsets, all_src_offsets,
                    all_sizes, snapshot);
            }
            for (size_t i = 0; i < original_indices.size(); ++i) {
                const size_t original_index = original_indices[i];
                results[original_index] = to_py_ret(ErrorCode::INTERNAL_ERROR);
                if (range_results.size() != original_indices.size() ||
                    range_results[i].size() != 1 ||
                    range_results[i][0].size() != 1) {
                    continue;
                }
                const int64_t result = range_results[i][0][0];
                if (result < 0 || static_cast<size_t>(result) ==
                                      metadata[original_index]->data_bytes) {
                    results[original_index] = result;
                }
            }
            return results;
        }

        auto metadata = batch_get_tensor_metadata_prefixes(keys, context);
        std::vector<CudaIpcReadRequest> read_requests;
        std::vector<size_t> original_indices;
        read_requests.reserve(keys.size());
        original_indices.reserve(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            PyTensorInfo target = extract_tensor_destination_info(
                py::cast<py::object>(tensors_list[i]), keys[i]);
            if (!target.valid() || !metadata[i].has_value() ||
                !tensor_destination_matches_metadata(target, *metadata[i],
                                                     keys[i], context)) {
                continue;
            }
            if (target.tensor_size == 0) {
                results[i] = 0;
                continue;
            }
            auto dst_buffer = mooncake::device::ExportCudaIpcBuffer(
                reinterpret_cast<void *>(target.data_ptr), target.tensor_size);
            if (!dst_buffer) {
                LOG(ERROR) << context
                           << ": destination tensor is not CUDA IPC exportable "
                           << "for key " << keys[i];
                continue;
            }
            read_requests.push_back(CudaIpcReadRequest{
                .key = keys[i],
                .destination = *dst_buffer,
                .source_offset = metadata[i]->data_offset,
                .size = metadata[i]->data_bytes,
            });
            original_indices.push_back(i);
        }

        if (!read_requests.empty()) {
            std::vector<int64_t> op_results;
            {
                py::gil_scoped_release release_gil;
                auto dummy_client =
                    std::static_pointer_cast<DummyClient>(store_);
                op_results =
                    dummy_client->batch_get_into_cuda_ipc(read_requests);
            }
            apply_indexed_results(context.c_str(), op_results, original_indices,
                                  results);
        }
        return results;
    }

    py::object get_tensor_with_tp_into(const std::string &key,
                                       uintptr_t buffer_ptr, size_t size,
                                       int tp_rank = 0, int tp_size = 1,
                                       int split_dim = 0) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return py::none();
        }

        if (tp_size <= 1) {
            return get_tensor_into(key, buffer_ptr, size);
        }

        // Construct the specific key for this rank: e.g., "key_tp_0"
        std::string tp_key = get_tp_key_name(key, tp_rank);

        // Delegate to the standard get_tensor_into method
        return get_tensor_into(tp_key, buffer_ptr, size);
    }

    py::list batch_get_tensor_with_tp_into(
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

    int put_tensor_impl(const std::string &key, py::object tensor,
                        const ReplicateConfig &config) {
        // Validation & Metadata extraction (GIL Held)
        auto info = extract_tensor_info(tensor, key);
        if (!info.valid()) return to_py_ret(ErrorCode::INVALID_PARAMS);
        return put_tensor_info_impl(key, info, config);
    }

    int put_tensor(const std::string &key, py::object tensor) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return put_tensor_impl(key, tensor,
                               ReplicateConfig{});  // Default config
    }

    ReplicateConfig MakeIndexedConfig(
        const ReplicateConfig &config,
        const std::vector<size_t> &original_indices) const {
        return config.ForKeySubset(original_indices);
    }

    ReplicateConfig MakeRepeatedIndexedConfig(
        const ReplicateConfig &config,
        const std::vector<size_t> &original_indices, int repeat_count) const {
        if (!config.group_ids && !config.session_ids &&
            !config.residency_affinity_ids &&
            !config.per_key_preferred_hca_names) {
            return config;
        }
        std::vector<size_t> shard_indices;
        shard_indices.reserve(original_indices.size() *
                              static_cast<size_t>(repeat_count));
        for (size_t index : original_indices) {
            for (int i = 0; i < repeat_count; ++i) {
                shard_indices.push_back(index);
            }
        }
        return config.ForKeySubset(shard_indices);
    }

    std::vector<int> ValidateBatchKeyHints(const ReplicateConfig &config,
                                           size_t key_count,
                                           const char *operation_name) const {
        if (!config.ValidSessionIds(key_count)) {
            LOG(ERROR) << operation_name << ": invalid per-key session IDs";
            return std::vector<int>(key_count,
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        if (!config.HasValidKeyHints(key_count)) {
            LOG(ERROR)
                << operation_name
                << ": group, affinity and per-key HCA hint counts must match "
                   "keys size";
            return std::vector<int>(key_count,
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        return {};
    }

    int put_tensor_with_tp_impl(
        const std::string &key, py::object tensor,
        const ReplicateConfig &config = ReplicateConfig{}, int tp_rank = 0,
        int tp_size = 1, int split_dim = 0) {
        // Single-tensor TP keeps per-shard group/HCA hints. Session and
        // residency affinity IDs describe the source tensor, not each shard.
        if (!config.ValidSessionIds(1) ||
            (config.residency_affinity_ids &&
             config.residency_affinity_ids->size() != 1))
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        try {
            ReplicateConfig tp_config = config;
            if (config.residency_affinity_ids) {
                tp_config.residency_affinity_ids = std::vector<std::string>(
                    tp_size, config.residency_affinity_ids->front());
            }
            if (!tp_config.HasValidKeyHints(tp_size))
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            std::vector<std::string> tp_keys;
            tp_keys.reserve(tp_size);
            for (int rank = 0; rank < tp_size; ++rank) {
                tp_keys.push_back(get_tp_key_name(key, rank));
            }
            auto infos = build_tp_shard_infos(
                tensor, tp_size, split_dim,
                [this, &key](int rank) { return get_tp_key_name(key, rank); });
            if (!infos.has_value()) {
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }
            if (config.session_ids) {
                tp_config.session_ids =
                    std::vector<std::string>(tp_size, config.SessionId());
            }
            auto results =
                batch_put_tensor_infos_impl(tp_keys, *infos, tp_config);
            for (int ret : results) {
                if (ret != 0) return ret;
            }
            return 0;

        } catch (const std::exception &e) {
            LOG(ERROR) << "Failed to put tensor with tp: " << e.what();
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
    }

    int put_tensor_with_tp(const std::string &key, py::object tensor,
                           int tp_rank = 0, int tp_size = 1,
                           int split_dim = 0) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (tp_size <= 1) return put_tensor(key, tensor);

        return put_tensor_with_tp_impl(key, tensor, ReplicateConfig{}, tp_rank,
                                       tp_size, split_dim);
    }

    std::vector<int> batch_put_tensor_infos_impl(
        const std::vector<std::string> &keys,
        const std::vector<PyTensorInfo> &infos,
        const ReplicateConfig &config = ReplicateConfig{}) {
        auto config_error = ValidateBatchKeyHints(config, keys.size(), "put");
        if (!config_error.empty()) return config_error;
        if (auto cuda_ipc_results =
                try_dummy_cuda_ipc_batch_put_tensor_impl(keys, infos, config)) {
            return *cuda_ipc_results;
        }
        return batch_write_tensor_impl(
            keys, infos, config, "put",
            [this](const std::vector<std::string> &write_keys,
                   const std::vector<void *> &buffers,
                   const std::vector<size_t> &sizes,
                   const ReplicateConfig &write_config) {
                return store_->batch_put_from(write_keys, buffers, sizes,
                                              write_config);
            },
            [this](const std::vector<std::string> &write_keys,
                   const std::vector<std::vector<void *>> &buffers,
                   const std::vector<std::vector<size_t>> &sizes,
                   const ReplicateConfig &write_config) {
                return real_client_->batch_put_from_multi_buffers(
                    write_keys, buffers, sizes, write_config, true);
            });
    }

    std::vector<int> batch_upsert_tensor_infos_impl(
        const std::vector<std::string> &keys,
        const std::vector<PyTensorInfo> &infos,
        const ReplicateConfig &config = ReplicateConfig{}) {
        auto config_error =
            ValidateBatchKeyHints(config, keys.size(), "upsert");
        if (!config_error.empty()) return config_error;
        if (auto cuda_ipc_results = try_dummy_cuda_ipc_batch_upsert_tensor_impl(
                keys, infos, config)) {
            return *cuda_ipc_results;
        }
        return batch_write_tensor_impl(
            keys, infos, config, "upsert",
            [this](const std::vector<std::string> &write_keys,
                   const std::vector<void *> &buffers,
                   const std::vector<size_t> &sizes,
                   const ReplicateConfig &write_config) {
                return store_->batch_upsert_from(write_keys, buffers, sizes,
                                                 write_config);
            },
            [this](const std::vector<std::string> &write_keys,
                   const std::vector<std::vector<void *>> &buffers,
                   const std::vector<std::vector<size_t>> &sizes,
                   const ReplicateConfig &write_config) {
                return real_client_->batch_upsert_from_multi_buffers(
                    write_keys, buffers, sizes, write_config, true);
            });
    }

    std::vector<int> batch_put_tensor_impl(
        const std::vector<std::string> &keys, const py::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}) {
        if (py::len(tensors_list) != keys.size()) {
            LOG(ERROR) << "batch_put_tensor keys and tensors size mismatch";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        std::vector<PyTensorInfo> infos(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            infos[i] = extract_tensor_info(tensors_list[i], keys[i]);
        }
        return batch_put_tensor_infos_impl(keys, infos, config);
    }

    std::vector<int> batch_put_tensor(const std::vector<std::string> &keys,
                                      const py::list &tensors_list) {
        if (!is_client_initialized())
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
        const std::vector<std::string> &base_keys, const py::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}, int tp_rank = 0,
        int tp_size = 1, int split_dim = 0) {
        std::vector<std::string> all_chunk_keys;
        std::vector<PyTensorInfo> all_chunk_infos;
        std::vector<size_t> processed_indices;
        std::vector<int> final_results(base_keys.size(),
                                       to_py_ret(ErrorCode::INVALID_PARAMS));
        auto config_error = ValidateBatchKeyHints(config, base_keys.size(),
                                                  "batch_put_tensor_with_tp");
        if (!config_error.empty()) {
            return config_error;
        }
        try {
            for (size_t i = 0; i < base_keys.size(); ++i) {
                py::object tensor = tensors_list[i];
                if (tensor.is_none() ||
                    !py::cast<py::tuple>(tensor.attr("shape"))) {
                    final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                auto infos = build_tp_shard_infos(
                    tensor, tp_size, split_dim,
                    [this, &base_keys, i](int rank) {
                        return get_tp_key_name(base_keys[i], rank);
                    });
                if (!infos.has_value()) {
                    final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                processed_indices.push_back(i);
                for (int rank = 0; rank < tp_size; ++rank) {
                    all_chunk_keys.push_back(
                        get_tp_key_name(base_keys[i], rank));
                    all_chunk_infos.push_back((*infos)[rank]);
                }
            }

            if (all_chunk_keys.empty()) return final_results;

            ReplicateConfig chunk_config =
                MakeRepeatedIndexedConfig(config, processed_indices, tp_size);
            std::vector<int> chunk_results = batch_put_tensor_infos_impl(
                all_chunk_keys, all_chunk_infos, chunk_config);
            if (chunk_results.size() != all_chunk_keys.size()) {
                LOG(ERROR) << "Batch put with TP returned "
                           << chunk_results.size() << " results, expected "
                           << all_chunk_keys.size();
                for (size_t index : processed_indices) {
                    final_results[index] = to_py_ret(ErrorCode::INTERNAL_ERROR);
                }
                return final_results;
            }

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
            LOG(ERROR) << "Batch put with TP failed: " << e.what();
        }

        return final_results;
    }

    std::vector<int> batch_put_tensor_with_tp(
        const std::vector<std::string> &base_keys, const py::list &tensors_list,
        int tp_rank = 0, int tp_size = 1, int split_dim = 0) {
        if (tp_size <= 1) return batch_put_tensor(base_keys, tensors_list);
        if (!is_client_initialized())
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

    // Put from a serialized tensor object (layout: [metadata][payload]).
    // Split the parts so DummyClient can stage external addresses safely.
    int put_tensor_from(const std::string &key, uintptr_t buffer_ptr,
                        size_t size) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        auto parts =
            make_raw_tensor_object_parts(buffer_ptr, size, "put_tensor_from");
        if (!parts.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        py::gil_scoped_release release_gil;
        void *payload = parts->payload_size == 0
                            ? nullptr
                            : reinterpret_cast<void *>(parts->payload_ptr);
        return store_->put_from_with_metadata(
            key, payload, reinterpret_cast<void *>(parts->metadata_ptr),
            parts->payload_size, parts->metadata_size, ReplicateConfig{});
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
        if (keys.empty()) {
            return std::vector<int>();
        }
        if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
            LOG(ERROR) << "Size mismatch: keys, buffer_ptrs, and sizes must "
                          "have the same length";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        std::vector<RawTensorObjectParts> parts;
        parts.reserve(keys.size());
        for (size_t i = 0; i < sizes.size(); ++i) {
            auto parsed = make_raw_tensor_object_parts(buffer_ptrs[i], sizes[i],
                                                       "batch_put_tensor_from");
            if (!parsed.has_value()) {
                LOG(ERROR) << "Invalid tensor object at index " << i;
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            parts.emplace_back(std::move(*parsed));
        }
        std::vector<std::vector<void *>> buffers;
        std::vector<std::vector<size_t>> part_sizes;
        buffers.reserve(parts.size());
        part_sizes.reserve(parts.size());
        for (auto &part : parts) {
            buffers.push_back({reinterpret_cast<void *>(part.metadata_ptr)});
            part_sizes.push_back({part.metadata_size});
            if (part.payload_size > 0) {
                buffers.back().push_back(
                    reinterpret_cast<void *>(part.payload_ptr));
                part_sizes.back().push_back(part.payload_size);
            }
        }
        py::gil_scoped_release release_gil;
        return store_->batch_put_from_multi_buffers(keys, buffers, part_sizes,
                                                    ReplicateConfig{});
    }

    int put_tensor_info_impl(const std::string &key, const PyTensorInfo &info,
                             const ReplicateConfig &config) {
        if (!info.valid()) return to_py_ret(ErrorCode::INVALID_PARAMS);
        if (use_dummy_client_) {
            auto results = batch_put_tensor_infos_impl(
                {key}, std::vector<PyTensorInfo>{info}, config);
            return results.empty() ? to_py_ret(ErrorCode::INTERNAL_ERROR)
                                   : results[0];
        }

        auto results = batch_put_tensor_infos_impl(
            std::vector<std::string>{key}, std::vector<PyTensorInfo>{info},
            config);
        if (results.size() != 1) {
            LOG(ERROR) << "put returned unexpected result count for key "
                       << key;
            return to_py_ret(ErrorCode::RPC_FAIL);
        }
        return results[0];
    }

    int put_tensor_with_tp_from(const std::string &key, uintptr_t buffer_ptr,
                                size_t size, int tp_rank = 0, int tp_size = 1,
                                int split_dim = 0) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (buffer_ptr == 0) {
            LOG(ERROR) << "Buffer pointer cannot be null";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (size < sizeof(TensorMetadata)) {
            LOG(ERROR) << "Buffer size too small for tensor metadata";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (tp_size <= 1) {
            return put_tensor_from(key, buffer_ptr, size);
        }

        std::vector<char> host_buffer;
        auto tensor = decode_raw_tensor_object_for_host(
            buffer_ptr, size, host_buffer, "put_tensor_with_tp_from");
        if (!tensor.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return put_tensor_with_tp_impl(key, *tensor, ReplicateConfig{}, tp_rank,
                                       tp_size, split_dim);
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
        std::vector<std::vector<char>> host_buffers(base_keys.size());

        for (size_t i = 0; i < base_keys.size(); ++i) {
            auto tensor = decode_raw_tensor_object_for_host(
                buffer_ptrs[i], sizes[i], host_buffers[i],
                "batch_put_tensor_with_tp_from");
            if (!tensor.has_value()) {
                final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                continue;
            }
            tensors_list.append(*tensor);
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

    template <typename CudaIpcBatchWriteFn>
    std::optional<std::vector<int>> try_dummy_cuda_ipc_batch_write_tensor_impl(
        const std::vector<std::string> &keys,
        const std::vector<PyTensorInfo> &infos, const ReplicateConfig &config,
        const char *operation_name, CudaIpcBatchWriteFn &&write_cuda_ipc) {
        if (!use_dummy_client_ || keys.size() != infos.size())
            return std::nullopt;

        struct CudaStreamContext {
            int32_t device_id;
            uintptr_t stream_handle;
        };
        std::vector<std::optional<CudaStreamContext>> cuda_stream_contexts(
            infos.size());
        for (size_t i = 0; i < infos.size(); ++i) {
            const py::object &owner = infos[i].owner;
            if (!infos[i].valid() || !owner || owner.is_none()) continue;

            try {
                if (!py::cast<bool>(owner.attr("is_cuda"))) continue;
                cuda_stream_contexts[i] = CudaStreamContext{
                    .device_id = py::cast<int32_t>(owner.attr("get_device")()),
                    .stream_handle = py::cast<uintptr_t>(
                        torch_module()
                            .attr("cuda")
                            .attr("current_stream")(owner.attr("device"))
                            .attr("cuda_stream")),
                };
            } catch (const py::python_error &) {
                PyErr_Clear();
            } catch (const py::cast_error &) {
                PyErr_Clear();
            }
        }

        std::vector<int> results(keys.size(), 0);
        py::gil_scoped_release release_gil;

        std::vector<std::optional<CudaIpcBufferHandle>> ipc_payloads(
            infos.size());
        for (size_t i = 0; i < infos.size(); ++i) {
            if (!infos[i].valid()) {
                results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                continue;
            }
            if (infos[i].tensor_size == 0) return std::nullopt;

            auto payload = mooncake::device::ExportCudaIpcBuffer(
                reinterpret_cast<const void *>(infos[i].data_ptr),
                infos[i].tensor_size);
            if (!payload) return std::nullopt;
            ipc_payloads[i] = std::move(*payload);
        }

        bool stream_context_invalid = false;
        for (size_t i = 0; i < infos.size(); ++i) {
            if (!ipc_payloads[i].has_value()) continue;
            if (!cuda_stream_contexts[i].has_value() ||
                cuda_stream_contexts[i]->device_id !=
                    ipc_payloads[i]->device_id) {
                results[i] = to_py_ret(ErrorCode::INTERNAL_ERROR);
                stream_context_invalid = true;
            }
        }
        if (stream_context_invalid) {
            LOG(ERROR) << "CUDA IPC tensor stream context validation failed";
            for (size_t i = 0; i < infos.size(); ++i) {
                if (ipc_payloads[i].has_value()) {
                    results[i] = to_py_ret(ErrorCode::INTERNAL_ERROR);
                }
            }
            return results;
        }

        std::vector<CudaIpcWriteRequest> write_requests;
        std::vector<size_t> original_indices;
        std::vector<std::unique_ptr<BufferHandle>> metadata_allocations;
        std::vector<std::pair<int32_t, uintptr_t>> unique_streams;
        write_requests.reserve(infos.size());
        original_indices.reserve(infos.size());
        metadata_allocations.reserve(infos.size());
        unique_streams.reserve(infos.size());

        for (size_t i = 0; i < infos.size(); ++i) {
            if (!ipc_payloads[i].has_value()) continue;

            size_t metadata_size = infos[i].metadata.header.data_offset;
            auto metadata = store_->allocate_client_buffer(metadata_size);
            if (!metadata) {
                results[i] = to_py_ret(ErrorCode::NO_AVAILABLE_HANDLE);
                continue;
            }

            std::memcpy(metadata->ptr(), &infos[i].metadata, metadata_size);
            write_requests.push_back(CudaIpcWriteRequest{
                .key = keys[i],
                .metadata =
                    CudaIpcShmBufferRef{
                        .ptr = reinterpret_cast<uint64_t>(metadata->ptr()),
                        .size = static_cast<uint64_t>(metadata_size),
                    },
                .payload = *ipc_payloads[i],
            });
            original_indices.push_back(i);
            metadata_allocations.push_back(
                std::make_unique<BufferHandle>(std::move(*metadata)));

            const CudaStreamContext &stream_context = *cuda_stream_contexts[i];
            bool stream_is_unique = true;
            for (const auto &[device_id, stream_handle] : unique_streams) {
                if (device_id == stream_context.device_id &&
                    stream_handle == stream_context.stream_handle) {
                    stream_is_unique = false;
                    break;
                }
            }
            if (stream_is_unique) {
                unique_streams.emplace_back(stream_context.device_id,
                                            stream_context.stream_handle);
            }
        }

        if (!write_requests.empty()) {
            ReplicateConfig write_config =
                MakeIndexedConfig(config, original_indices);
            auto dummy_client = std::static_pointer_cast<DummyClient>(store_);
            for (const auto &[device_id, stream_handle] : unique_streams) {
                if (!mooncake::device::SynchronizeCudaStream(device_id,
                                                             stream_handle)) {
                    LOG(ERROR)
                        << "CUDA IPC tensor stream synchronization failed";
                    for (size_t index : original_indices) {
                        results[index] = to_py_ret(ErrorCode::INTERNAL_ERROR);
                    }
                    return results;
                }
            }
            std::vector<int> op_results =
                write_cuda_ipc(dummy_client, write_requests, write_config);
            if (!apply_indexed_results(operation_name, op_results,
                                       original_indices, results)) {
                return results;
            }
        }
        return results;
    }

    std::optional<std::vector<int>> try_dummy_cuda_ipc_batch_put_tensor_impl(
        const std::vector<std::string> &keys,
        const std::vector<PyTensorInfo> &infos, const ReplicateConfig &config) {
        return try_dummy_cuda_ipc_batch_write_tensor_impl(
            keys, infos, config, "put",
            [](auto dummy_client, const auto &requests,
               const auto &write_config) {
                return dummy_client->batch_put_from_cuda_ipc(requests,
                                                             write_config);
            });
    }

    std::optional<std::vector<int>> try_dummy_cuda_ipc_batch_upsert_tensor_impl(
        const std::vector<std::string> &keys,
        const std::vector<PyTensorInfo> &infos, const ReplicateConfig &config) {
        return try_dummy_cuda_ipc_batch_write_tensor_impl(
            keys, infos, config, "upsert",
            [](auto dummy_client, const auto &requests,
               const auto &write_config) {
                return dummy_client->batch_upsert_from_cuda_ipc(requests,
                                                                write_config);
            });
    }

    template <typename StagedBatchWriteFn, typename DirectBatchWriteFn>
    std::vector<int> batch_write_tensor_impl(
        const std::vector<std::string> &keys,
        const std::vector<PyTensorInfo> &infos, const ReplicateConfig &config,
        const char *operation_name, StagedBatchWriteFn &&staged_batch_write,
        DirectBatchWriteFn &&direct_batch_write) {
        if (keys.size() != infos.size()) {
            LOG(ERROR) << operation_name
                       << ": keys and tensor infos must have the same length";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }

        std::vector<int> results(keys.size(), 0);
        {
            py::gil_scoped_release release_gil;
            std::vector<std::string> valid_keys;
            std::vector<size_t> original_indices;
            valid_keys.reserve(infos.size());
            original_indices.reserve(infos.size());

            auto run_staged_write = [&]() -> std::vector<int> {
                auto runtime_accelerator =
                    mooncake::device::GetAcceleratorRegistry()
                        .RuntimeAccelerators();
                std::vector<void *> buffer_ptrs;
                std::vector<size_t> buffer_sizes;
                std::vector<std::unique_ptr<BufferHandle>> temp_allocations;
                buffer_ptrs.reserve(infos.size());
                buffer_sizes.reserve(infos.size());
                temp_allocations.reserve(infos.size());

                for (size_t i = 0; i < infos.size(); ++i) {
                    if (!infos[i].valid()) {
                        results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                        continue;
                    }

                    size_t total_size = infos[i].metadata.header.data_offset +
                                        infos[i].tensor_size;
                    auto alloc_result =
                        store_->allocate_client_buffer(total_size);
                    if (!alloc_result) {
                        LOG(ERROR) << "Failed to allocate buffer for "
                                   << operation_name << " key: " << keys[i];
                        results[i] = to_py_ret(ErrorCode::NO_AVAILABLE_HANDLE);
                        continue;
                    }

                    char *dst = static_cast<char *>(alloc_result->ptr());
                    std::memcpy(dst, &infos[i].metadata,
                                infos[i].metadata.header.data_offset);
                    if (infos[i].tensor_size > 0) {
                        if (!runtime_accelerator.CopyToHost(
                                dst + infos[i].metadata.header.data_offset,
                                reinterpret_cast<const void *>(
                                    infos[i].data_ptr),
                                infos[i].tensor_size)) {
                            LOG(ERROR) << "Failed to copy tensor payload for "
                                       << operation_name << " key: " << keys[i];
                            results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                            continue;
                        }
                    }

                    valid_keys.push_back(keys[i]);
                    buffer_ptrs.push_back(alloc_result->ptr());
                    buffer_sizes.push_back(total_size);
                    original_indices.push_back(i);
                    temp_allocations.push_back(std::make_unique<BufferHandle>(
                        std::move(*alloc_result)));
                }

                if (!valid_keys.empty()) {
                    ReplicateConfig write_config =
                        MakeIndexedConfig(config, original_indices);
                    std::vector<int> op_results = staged_batch_write(
                        valid_keys, buffer_ptrs, buffer_sizes, write_config);
                    apply_tensor_batch_results(results, original_indices,
                                               op_results, operation_name);
                }
                return results;
            };

            if (use_dummy_client_) return run_staged_write();

            if (!real_client_) {
                LOG(ERROR) << operation_name
                           << ": real client is not available";
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }

            std::vector<std::vector<void *>> all_buffers;
            std::vector<std::vector<size_t>> all_sizes;
            all_buffers.reserve(infos.size());
            all_sizes.reserve(infos.size());

            for (size_t i = 0; i < infos.size(); ++i) {
                if (!infos[i].valid()) {
                    results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                    continue;
                }

                all_buffers.emplace_back();
                all_sizes.emplace_back();
                append_tensor_write_buffers(infos[i], all_buffers.back(),
                                            all_sizes.back());
                valid_keys.push_back(keys[i]);
                original_indices.push_back(i);
            }

            if (!valid_keys.empty()) {
                ReplicateConfig write_config =
                    MakeIndexedConfig(config, original_indices);
                std::vector<int> op_results = direct_batch_write(
                    valid_keys, all_buffers, all_sizes, write_config);
                apply_tensor_batch_results(results, original_indices,
                                           op_results, operation_name);
            }
        }

        return results;
    }

    // --- Upsert tensor methods ---

    int upsert_tensor_info_impl(const std::string &key,
                                const PyTensorInfo &info,
                                const ReplicateConfig &config) {
        if (!info.valid()) return to_py_ret(ErrorCode::INVALID_PARAMS);
        if (use_dummy_client_) {
            auto results = batch_upsert_tensor_infos_impl(
                {key}, std::vector<PyTensorInfo>{info}, config);
            return results.empty() ? to_py_ret(ErrorCode::INTERNAL_ERROR)
                                   : results[0];
        }

        auto results = batch_upsert_tensor_infos_impl(
            std::vector<std::string>{key}, std::vector<PyTensorInfo>{info},
            config);
        if (results.size() != 1) {
            LOG(ERROR) << "upsert returned unexpected result count for key "
                       << key;
            return to_py_ret(ErrorCode::RPC_FAIL);
        }
        return results[0];
    }

    int upsert_tensor_impl(const std::string &key, py::object tensor,
                           const ReplicateConfig &config) {
        auto info = extract_tensor_info(tensor, key);
        if (!info.valid()) return to_py_ret(ErrorCode::INVALID_PARAMS);
        return upsert_tensor_info_impl(key, info, config);
    }

    int upsert_tensor(const std::string &key, py::object tensor) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        return upsert_tensor_impl(key, tensor, ReplicateConfig{});
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
        if (keys.empty()) {
            return std::vector<int>();
        }
        if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
            LOG(ERROR) << "Size mismatch: keys, buffer_ptrs, and sizes must "
                          "have the same length";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        std::vector<RawTensorObjectParts> parts;
        parts.reserve(keys.size());
        for (size_t i = 0; i < sizes.size(); ++i) {
            auto parsed = make_raw_tensor_object_parts(
                buffer_ptrs[i], sizes[i], "batch_upsert_tensor_from");
            if (!parsed.has_value()) {
                LOG(ERROR) << "Invalid tensor object at index " << i;
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            parts.emplace_back(std::move(*parsed));
        }
        std::vector<std::vector<void *>> buffers;
        std::vector<std::vector<size_t>> part_sizes;
        buffers.reserve(parts.size());
        part_sizes.reserve(parts.size());
        for (auto &part : parts) {
            buffers.push_back({reinterpret_cast<void *>(part.metadata_ptr)});
            part_sizes.push_back({part.metadata_size});
            if (part.payload_size > 0) {
                buffers.back().push_back(
                    reinterpret_cast<void *>(part.payload_ptr));
                part_sizes.back().push_back(part.payload_size);
            }
        }
        py::gil_scoped_release release_gil;
        return store_->batch_upsert_from_multi_buffers(
            keys, buffers, part_sizes, ReplicateConfig{});
    }

    std::vector<int> batch_upsert_tensor_impl(
        const std::vector<std::string> &keys, const py::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}) {
        if (py::len(tensors_list) != keys.size()) {
            LOG(ERROR) << "batch_upsert_tensor keys and tensors size mismatch";
            return std::vector<int>(keys.size(),
                                    to_py_ret(ErrorCode::INVALID_PARAMS));
        }
        std::vector<PyTensorInfo> infos(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            infos[i] = extract_tensor_info(tensors_list[i], keys[i]);
        }
        return batch_upsert_tensor_infos_impl(keys, infos, config);
    }

    std::vector<int> batch_upsert_tensor(const std::vector<std::string> &keys,
                                         const py::list &tensors_list) {
        if (!is_client_initialized())
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
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        auto results = batch_upsert_tensor_from({key}, {buffer_ptr}, {size});
        if (results.size() != 1) {
            LOG(ERROR) << "Tensor upsert returned unexpected result count";
            return to_py_ret(ErrorCode::INTERNAL_ERROR);
        }
        return results[0];
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

    int upsert_pub_tensor(const std::string &key, py::object tensor,
                          const ReplicateConfig &config = ReplicateConfig{}) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        int validate_result = validate_replicate_config(config);
        if (validate_result) return validate_result;

        return upsert_tensor_impl(key, tensor, config);
    }

    std::vector<int> batch_upsert_pub_tensor(
        const std::vector<std::string> &keys, const py::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}) {
        if (!is_client_initialized())
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
    int pub_tensor(const std::string &key, py::object tensor,
                   const ReplicateConfig &config = ReplicateConfig{}) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        int validate_result = validate_replicate_config(config);
        if (validate_result) return validate_result;

        return put_tensor_impl(key, tensor, config);
    }

    int pub_tensor_with_tp(const std::string &key, py::object tensor,
                           const ReplicateConfig &config = ReplicateConfig{},
                           int tp_rank = 0, int tp_size = 1,
                           int split_dim = 0) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client not initialized";
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        int validate_result = validate_replicate_config(config);
        if (validate_result) return validate_result;

        if (tp_size <= 1) return pub_tensor(key, tensor, config);

        return put_tensor_with_tp_impl(key, tensor, config, tp_rank, tp_size,
                                       split_dim);
    }

    std::vector<int> batch_pub_tensor(
        const std::vector<std::string> &keys, const py::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}) {
        if (!is_client_initialized())
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
        const std::vector<std::string> &base_keys, const py::list &tensors_list,
        const ReplicateConfig &config = ReplicateConfig{}, int tp_rank = 0,
        int tp_size = 1, int split_dim = 0) {
        if (tp_size <= 1)
            return batch_pub_tensor(base_keys, tensors_list, config);
        if (!is_client_initialized())
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

        py::object tensor = get_tensor(key);
        if (tensor.is_none()) {
            LOG(ERROR) << "Failed to fetch tensor for key: " << key;
            return to_py_ret(ErrorCode::FILE_NOT_FOUND);
        }

        try {
            std::string resolved_file_name =
                file_name_obj.is_none() ? key
                                        : py::cast<std::string>(file_name_obj);
            auto safetensors_torch = py::module_::import_("safetensors.torch");
            py::dict payload;
            payload[py::str(key.data(), key.size())] = tensor;
            safetensors_torch.attr("save_file")(payload, resolved_file_name);
            return 0;
        } catch (const py::python_error &e) {
            LOG(ERROR) << "Failed to save tensor to safetensor file: "
                       << e.what();
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
    }

    py::object load_tensor_from_safetensor(py::object key_obj,
                                           const std::string &file_name) {
        if (!is_client_initialized()) {
            LOG(ERROR) << "Client is not initialized";
            return py::none();
        }

        try {
            auto safetensors_torch = py::module_::import_("safetensors.torch");
            auto loaded = py::cast<py::dict>(
                safetensors_torch.attr("load_file")(file_name));
            if (py::len(loaded) == 0) {
                LOG(ERROR) << "No tensors found in safetensor file: "
                           << file_name;
                return py::none();
            }

            std::string target_store_key =
                key_obj.is_none() ? file_name : py::cast<std::string>(key_obj);

            py::object selected_dict_key;
            if (!key_obj.is_none()) {
                py::str desired_key(target_store_key.data(),
                                    target_store_key.size());
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
                return py::none();
            }

            return tensor;
        } catch (const py::python_error &e) {
            LOG(ERROR) << "Failed to load tensor from safetensor file: "
                       << e.what();
            return py::none();
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

uintptr_t get_alloc_func_addr() {
    return reinterpret_cast<uintptr_t>(&hugepage_memory_alloc);
}

uintptr_t get_free_func_addr() {
    return reinterpret_cast<uintptr_t>(&hugepage_memory_free);
}

class MooncakeDistributedNoFRegisterPyWrapper {
   public:
    std::shared_ptr<NoFRegisterClient> register_{nullptr};

    MooncakeDistributedNoFRegisterPyWrapper() = default;
};

NB_MODULE(_store, m) {
    m.def("_serialize_tensor", &serialize_tensor_metadata,
          "Inspect a torch tensor as Mooncake tensor metadata, data pointer, "
          "size, and owner.");
    m.def("_tensor_metadata_size", &tensor_metadata_size,
          "Return the serialized Mooncake tensor metadata size.");
    m.def("_deserialize_tensor", &deserialize_tensor_from_bytes,
          "Deserialize Mooncake tensor metadata plus payload bytes.");

    py::class_<RangedReadSnapshotPy>(
        m, "RangedReadSnapshot",
        "Reusable metadata snapshot for repeated ranged reads");

    // Object data type classification
    py::enum_<ObjectDataType>(m, "ObjectDataType")
        .value("UNKNOWN", ObjectDataType::UNKNOWN)
        .value("KVCACHE", ObjectDataType::KVCACHE)
        .value("TENSOR", ObjectDataType::TENSOR)
        .value("WEIGHT", ObjectDataType::WEIGHT)
        .value("SAMPLE", ObjectDataType::SAMPLE)
        .value("ACTIVATION", ObjectDataType::ACTIVATION)
        .value("GRADIENT", ObjectDataType::GRADIENT)
        .value("OPTIMIZER_STATE", ObjectDataType::OPTIMIZER_STATE)
        .value("METADATA", ObjectDataType::METADATA)
        .value("GENERAL", ObjectDataType::GENERAL)
        .export_values();

    py::enum_<SoftPinAction>(m, "SoftPinAction")
        .value("PRESERVE", SoftPinAction::PRESERVE)
        .value("ENABLE", SoftPinAction::ENABLE)
        .value("DISABLE", SoftPinAction::DISABLE);

    // Define the ReplicateConfig class
    py::class_<ReplicateConfig>(m, "ReplicateConfig")
        .def(py::init<>())
        .def_rw("replica_num", &ReplicateConfig::replica_num)
        .def_rw("nof_replica_num", &ReplicateConfig::nof_replica_num)
        .def_rw("dfs_replica_num", &ReplicateConfig::dfs_replica_num)
        .def_rw("soft_pin_action", &ReplicateConfig::soft_pin_action)
        .def_rw("soft_pin_ttl_ms", &ReplicateConfig::soft_pin_ttl_ms)
        .def_rw("with_hard_pin", &ReplicateConfig::with_hard_pin)
        .def_rw("preferred_segments", &ReplicateConfig::preferred_segments)
        .def_rw("preferred_nof_segments",
                &ReplicateConfig::preferred_nof_segments)
        .def_rw("preferred_segment", &ReplicateConfig::preferred_segment)
        .def_rw("prefer_alloc_in_same_node",
                &ReplicateConfig::prefer_alloc_in_same_node)
        .def_rw("data_type", &ReplicateConfig::data_type)
        .def_rw("group_ids", &ReplicateConfig::group_ids)
        .def_prop_rw(
            "session_ids",
            [](const ReplicateConfig &config)
                -> std::optional<std::vector<std::string>> {
                if (!config.session_ids) return std::nullopt;
                return *config.session_ids;
            },
            [](ReplicateConfig &config,
               std::optional<std::vector<
                   std::variant<std::string, py::bytes, py::bytearray>>>
                   ids) {
                if (!ids) {
                    config.session_ids.reset();
                    return;
                }
                // Keep the old binding's raw bytes support. The store
                // validates UTF-8 when the IDs are used in a write.
                std::vector<std::string> session_ids;
                session_ids.reserve(ids->size());
                for (const auto &id : *ids) {
                    std::visit(
                        [&session_ids](const auto &value) {
                            session_ids.emplace_back(value.c_str(),
                                                     value.size());
                        },
                        id);
                }
                config.session_ids = std::move(session_ids);
            })
        .def_prop_rw(
            "residency_affinity_ids",
            [](const ReplicateConfig &config)
                -> std::optional<std::vector<std::string>> {
                if (!config.residency_affinity_ids) return std::nullopt;
                return *config.residency_affinity_ids;
            },
            [](ReplicateConfig &config,
               std::optional<std::vector<std::string>> ids) {
                config.residency_affinity_ids.reset();
                if (ids) config.residency_affinity_ids = std::move(*ids);
            })
        .def_prop_rw(
            "preferred_hca_names",
            [](const ReplicateConfig &config)
                -> std::optional<std::vector<std::string>> {
                return config.preferred_hca_names;
            },
            [](ReplicateConfig &config,
               std::optional<std::vector<std::string>> value) {
                config.preferred_hca_names = std::move(value);
            })
        .def_prop_rw(
            "per_key_preferred_hca_names",
            [](const ReplicateConfig &config)
                -> std::optional<std::vector<std::vector<std::string>>> {
                return config.per_key_preferred_hca_names;
            },
            [](ReplicateConfig &config,
               std::optional<std::vector<std::vector<std::string>>> value) {
                config.per_key_preferred_hca_names = std::move(value);
            })
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

    py::class_<ExistOptions>(m, "ExistOptions")
        .def(py::init<>())
        .def_rw("prefetch_to_memory", &ExistOptions::prefetch_to_memory)
        .def("__str__", [](const ExistOptions &options) {
            std::ostringstream oss;
            oss << options;
            return oss.str();
        });

    py::class_<MemoryDescriptor>(m, "MemoryDescriptor")
        .def_rw("buffer_descriptor", &MemoryDescriptor::buffer_descriptor);

    py::class_<DiskDescriptor>(m, "DiskDescriptor")
        .def_rw("file_path", &DiskDescriptor::file_path)
        .def_rw("object_size", &DiskDescriptor::object_size);

    py::class_<Replica::Descriptor>(m, "ReplicaDescriptor")
        .def_ro("status", &Replica::Descriptor::status)
        .def("is_memory_replica",
             static_cast<bool (Replica::Descriptor::*)() const noexcept>(
                 &Replica::Descriptor::is_memory_replica))
        .def("is_disk_replica",
             static_cast<bool (Replica::Descriptor::*)() const noexcept>(
                 &Replica::Descriptor::is_disk_replica))
        .def("is_local_disk_replica",
             static_cast<bool (Replica::Descriptor::*)() const noexcept>(
                 &Replica::Descriptor::is_local_disk_replica))
        .def(
            "get_memory_descriptor",
            static_cast<const MemoryDescriptor &(Replica::Descriptor::*)()
                            const>(&Replica::Descriptor::get_memory_descriptor),
            py::rv_policy::reference_internal)
        .def(
            "get_disk_descriptor",
            static_cast<const DiskDescriptor &(Replica::Descriptor::*)() const>(
                &Replica::Descriptor::get_disk_descriptor),
            py::rv_policy::reference_internal);

    py::class_<AllocatedBuffer::Descriptor>(
        m, "Descriptor",
        "Descriptor for allocated buffers. Only memory descriptors are "
        "supported.")
        .def(py::init<>())
        .def_rw("size", &AllocatedBuffer::Descriptor::size_)
        .def_rw("buffer_address", &AllocatedBuffer::Descriptor::buffer_address_)
        .def_rw("transport_endpoint",
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
        .value("CANCELED", TaskStatus::CANCELED)
        .export_values();

    py::class_<QueryTaskResponse>(m, "QueryTaskResponse",
                                  "Response structure for QueryTask operation.")
        .def(py::init<>())
        .def_ro("id", &QueryTaskResponse::id)
        .def_ro("type", &QueryTaskResponse::type)
        .def_ro("status", &QueryTaskResponse::status)
        .def_ro("created_at_ms_epoch", &QueryTaskResponse::created_at_ms_epoch)
        .def_ro("last_updated_at_ms_epoch",
                &QueryTaskResponse::last_updated_at_ms_epoch)
        .def_ro("assigned_client", &QueryTaskResponse::assigned_client)
        .def_ro("message", &QueryTaskResponse::message)
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

    py::enum_<JobType>(m, "JobType").value("DRAIN", JobType::DRAIN);
    py::enum_<JobStatus>(m, "JobStatus")
        .value("CREATED", JobStatus::CREATED)
        .value("PLANNING", JobStatus::PLANNING)
        .value("RUNNING", JobStatus::RUNNING)
        .value("SUCCEEDED", JobStatus::SUCCEEDED)
        .value("FAILED", JobStatus::FAILED)
        .value("CANCELED", JobStatus::CANCELED)
        .value("CANCELING", JobStatus::CANCELING)
        .value("TIMED_OUT", JobStatus::TIMED_OUT);

    py::class_<QueryJobResponse>(m, "QueryJobResponse")
        .def_ro("id", &QueryJobResponse::id)
        .def_ro("type", &QueryJobResponse::type)
        .def_ro("status", &QueryJobResponse::status)
        .def_ro("created_at_ms_epoch", &QueryJobResponse::created_at_ms_epoch)
        .def_ro("last_updated_at_ms_epoch",
                &QueryJobResponse::last_updated_at_ms_epoch)
        .def_ro("segments", &QueryJobResponse::segments)
        .def_ro("succeeded_units", &QueryJobResponse::succeeded_units)
        .def_ro("failed_units", &QueryJobResponse::failed_units)
        .def_ro("blocked_units", &QueryJobResponse::blocked_units)
        .def_ro("active_units", &QueryJobResponse::active_units)
        .def_ro("migrated_bytes", &QueryJobResponse::migrated_bytes)
        .def_ro("message", &QueryJobResponse::message);

    py::class_<ListDrainJobsResponse>(m, "ListDrainJobsResponse")
        .def_ro("jobs", &ListDrainJobsResponse::jobs)
        .def_ro("next_page_token", &ListDrainJobsResponse::next_page_token);

    py::class_<ListTasksResponse>(m, "ListTasksResponse")
        .def_ro("tasks", &ListTasksResponse::tasks)
        .def_ro("next_page_token", &ListTasksResponse::next_page_token);

    static PyType_Slot buffer_handle_slots[] = {
        {Py_bf_getbuffer,
         reinterpret_cast<void *>(
             +[](PyObject *self, Py_buffer *view, int flags) {
                 auto *buffer = py::inst_ptr<BufferHandle>(self);
                 const int status = fill_byte_buffer(self, view, buffer->ptr(),
                                                     buffer->size(), flags);
                 if (status == 0 && view->format &&
                     std::numeric_limits<char>::is_signed) {
                     static char signed_byte_format[] = "b";
                     view->format = signed_byte_format;
                 }
                 return status;
             })},
        {0, nullptr},
    };
    py::class_<BufferHandle>(m, "BufferHandle",
                             py::type_slots(buffer_handle_slots))
        .def("ptr",
             [](const BufferHandle &self) {
                 return reinterpret_cast<uintptr_t>(self.ptr());
             })
        .def("size", &BufferHandle::size)
        .def("__len__", &BufferHandle::size);

    bind_buffer_pool(m);

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

    auto make_pyclient_capsule =
        [](const std::shared_ptr<PyClient> &store) -> py::object {
        if (!store) {
            return py::none();
        }
        auto ptr = std::make_unique<std::shared_ptr<PyClient>>(store);
        std::shared_ptr<PyClient> *raw_ptr = ptr.get();
        py::object cap = py::capsule(
            raw_ptr, "mooncake.PyClient.shared_ptr", [](void *p) noexcept {
                delete static_cast<std::shared_ptr<PyClient> *>(p);
            });
        (void)ptr.release();  // Transfer ownership to capsule
        return cap;
    };

    py::class_<MooncakeDistributedNoFRegisterPyWrapper>(
        m, "MooncakeDistributedNoFRegister")
        .def(py::init<>())
        .def("real_register",
             [](MooncakeDistributedNoFRegisterPyWrapper &self,
                const std::string &nqn = "", size_t nsid = 1,
                const std::string &traddr = "", size_t trsvcid = 4420,
                uintptr_t base = 0x0, size_t size = 1024,
                const std::string &master_server_addr = "127.0.0.1:50051") {
                 self.register_ = std::make_shared<NoFRegisterClient>();
                 return self.register_->set_register(nqn, nsid, traddr, trsvcid,
                                                     base, size,
                                                     master_server_addr);
             })
        .def("real_unregister_by_endpoint",
             [](MooncakeDistributedNoFRegisterPyWrapper &self,
                const std::string &nqn = "", size_t nsid = 1,
                const std::string &traddr = "", size_t trsvcid = 4420,
                const std::string &master_server_addr = "127.0.0.1:50051") {
                 self.register_ = std::make_shared<NoFRegisterClient>();
                 return self.register_->set_unregister_by_endpoint(
                     nqn, nsid, traddr, trsvcid, master_server_addr);
             });
    // Create a wrapper that exposes DistributedObjectStore with Python-specific
    // methods
    py::class_<MooncakeStorePyWrapper>(m, "MooncakeDistributedStore")
        .def(py::init<>())
        .def(
            "begin_weight_snapshot",
            [](MooncakeStorePyWrapper &self, py::object snapshot,
               py::object adapter) -> py::object {
                py::object raw_store =
                    py::cast(&self, py::rv_policy::reference);
                return py::module_::import_("mooncake.reshard.weight.store")
                    .attr("begin_weight_snapshot")(raw_store, snapshot,
                                                   adapter);
            },
            py::arg("snapshot"), py::arg("adapter"),
            "Open a model-weight snapshot writer backed by this Store")
        .def(
            "_get_pyclient_capsule",
            [make_pyclient_capsule](MooncakeStorePyWrapper &self)
                -> py::object { return make_pyclient_capsule(self.store_); },
            "Internal use: expose the underlying PyClient handle as a typed "
            "capsule")
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
               const std::string &ssd_offload_path = "",
               const std::string &tenant_id = "default",
               bool enable_client_http_server = false,
               int client_http_port = DEFAULT_CLIENT_HTTP_PORT,
               bool enable_embedded_master = false) {
                auto real_client = self.init_real_client();
                std::shared_ptr<mooncake::TransferEngine> transfer_engine =
                    nullptr;
                if (!engine.is_none()) {
                    transfer_engine =
                        py::cast<std::shared_ptr<TransferEngine>>(engine);
                }
                int ret;
                {
                    py::gil_scoped_release release;
                    auto result = real_client->setup_internal(
                        local_hostname, metadata_server, global_segment_size,
                        local_buffer_size, protocol, rdma_devices,
                        master_server_addr, transfer_engine, "", 50052,
                        enable_ssd_offload, true, ssd_offload_path, tenant_id,
                        enable_client_http_server, client_http_port,
                        enable_embedded_master);
                    ret = result.has_value() ? 0
                                             : static_cast<int>(result.error());
                }
                return ret;
            },
            py::arg("local_hostname"), py::arg("metadata_server"),
            py::arg("global_segment_size"), py::arg("local_buffer_size"),
            py::arg("protocol"), py::arg("rdma_devices"),
            py::arg("master_server_addr"),
            py::arg("engine").none() = py::none(),
            py::arg("enable_ssd_offload") = false,
            py::arg("ssd_offload_path") = "", py::arg("tenant_id") = "default",
            py::arg("enable_client_http_server") = false,
            py::arg("client_http_port") = DEFAULT_CLIENT_HTTP_PORT,
            py::arg("enable_embedded_master") = false)
        .def(
            "setup",
            [](MooncakeStorePyWrapper &self, const py::dict &config_dict) {
                auto real_client = self.init_real_client();

                // Convert py::dict to ConfigDict (all values as strings)
                ConfigDict config;
                for (auto item : config_dict) {
                    std::string key =
                        py::cast<std::string>(py::str(item.first));
                    std::string value =
                        py::cast<std::string>(py::str(item.second));
                    config[key] = value;
                }

                int ret;
                {
                    py::gil_scoped_release release;
                    auto result = real_client->setup_internal(config);
                    ret = result.has_value() ? 0
                                             : static_cast<int>(result.error());
                }
                return ret;
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
            "  enable_egm_store_pool: Use EGM DRAM for the global Store pool "
            "(default false; requires protocol 'nvlink', "
            "global_segment_size>0). Local buffers use the existing "
            "client-buffer allocator.\n"
            "  egm_numa_nodes: Provider NUMA nodes ('auto' or comma-separated "
            "IDs; default 'auto').\n"
            "  master_server_addr: Master server address.\n"
            "  ipc_socket_path: IPC socket path.\n"
            "  enable_ssd_offload: Enable SSD offload (default false).\n"
            "  ssd_offload_path: SSD storage directory path (overrides env "
            "var).\n"
            "  tenant_id: Tenant identifier (default 'default').\n"
            "  enable_client_http_server: Enable client HTTP endpoints "
            "(default false).\n"
            "  client_http_port: Client HTTP metrics port (default 9300).\n"
            "  enable_ssd_prefetch: Enable best-effort SSD-to-DRAM prefetch "
            "on exist probes (default false).\n"
            "  ssd_prefetch_cooldown_sec: Prefetch memory-pressure backoff "
            "in seconds (default 5, 0 disables).\n"
            "  ssd_prefetch_dedup_ttl_sec: Prefetch per-key dedup/rate-limit "
            "TTL in seconds (default 30, 0 disables).\n"
            "  enable_embedded_master: Start an in-process master so no "
            "external mooncake_master is required (default false). This is "
            "test/eval plumbing, not a documented deployment mode.")
        .def(
            "setup_dummy",
            [](MooncakeStorePyWrapper &self, size_t mem_pool_size,
               size_t local_buffer_size, const std::string &server_address) {
                auto &resource_tracker = ResourceTracker::getInstance();
                self.use_dummy_client_ = true;
                self.store_ = std::make_shared<DummyClient>();
                self.real_client_.reset();
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
        .def("mount_segment", &MooncakeStorePyWrapper::mount_segment,
             py::arg("path"), py::arg("size"), py::arg("offset") = 0,
             py::arg("protocol") = "tcp", py::arg("location") = "")
        .def("unmount_segment", &MooncakeStorePyWrapper::unmount_segment,
             py::arg("segment_ids"), py::arg("grace_period_seconds") = 0)
        .def("allocate_and_mount_segment",
             &MooncakeStorePyWrapper::allocate_and_mount_segment,
             py::arg("size"), py::arg("protocol") = "tcp",
             py::arg("location") = "")
        .def("unmount_and_free_segment",
             &MooncakeStorePyWrapper::unmount_and_free_segment,
             py::arg("segment_ids"), py::arg("grace_period_seconds") = 0)
        .def("unmount_local_disk_segment",
             &MooncakeStorePyWrapper::unmount_local_disk_segment,
             py::arg("grace_period_seconds") = 0)
        .def("alloc_from_mem_pool",
             [](MooncakeStorePyWrapper &self, size_t size) {
                 py::gil_scoped_release release;
                 return self.store_->alloc_from_mem_pool(size);
             })
        .def("get", &mooncake::MooncakeStorePyWrapper::get)
        .def("get_batch", &mooncake::MooncakeStorePyWrapper::get_batch)
        .def("get_offload_rpc_read_count",
             [](MooncakeStorePyWrapper &self) -> int64_t {
                 auto real_client =
                     std::dynamic_pointer_cast<RealClient>(self.store_);
                 return real_client ? real_client->get_offload_rpc_read_count()
                                    : 0;
             })
        .def(
            "get_buffer",
            [](MooncakeStorePyWrapper &self, const std::string &key) {
                py::gil_scoped_release release;
                return self.store_->get_buffer(key);
            },
            py::rv_policy::take_ownership)
        .def(
            "batch_get_buffer",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys) {
                py::gil_scoped_release release;
                return self.store_->batch_get_buffer(keys);
            },
            py::rv_policy::take_ownership)
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
        .def(
            "is_exist",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               const ExistOptions &options) {
                py::gil_scoped_release release;
                return self.store_->isExist(key, options);
            },
            py::arg("key"), py::arg("options") = ExistOptions{},
            "Check if an object exists. Returns 1 if exists, 0 if not "
            "exists, negative error code on failure. When "
            "options.prefetch_to_memory is true and the client is configured "
            "with enable_ssd_prefetch, SSD-only keys get a best-effort "
            "asynchronous SSD-to-DRAM promotion so a later get can hit DRAM.")
        .def(
            "batch_is_exist",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const ExistOptions &options) {
                py::gil_scoped_release release;
                return self.store_->batchIsExist(keys, options);
            },
            py::arg("keys"), py::arg("options") = ExistOptions{},
            "Check if multiple objects exist. When options.prefetch_to_memory "
            "is true (and enable_ssd_prefetch is configured), triggers "
            "best-effort SSD-to-DRAM promotion for keys that only have "
            "LOCAL_DISK replicas. Returns list of results: 1 if "
            "exists, 0 if not exists, -1 if error")
        .def(
            "probe_key",
            [](MooncakeStorePyWrapper &self, const std::string &key) {
                py::gil_scoped_release release;
                return self.store_->probeKey(key);
            },
            py::arg("key"),
            "Point-in-time existence check that grants no read lease. "
            "Returns 1 if the object existed at the time of the call, 0 if "
            "not exists, -1 if error. The object may still be evicted "
            "before a subsequent get.")
        .def(
            "batch_probe_key",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys, const std::string &policy,
               uint64_t candidate_size) {
                GrantLeasePolicy lease_policy;
                if (policy == "none") {
                    lease_policy.lease_mode = ProbeLeaseMode::None;
                } else if (policy == "LastHitOnly") {
                    lease_policy.lease_mode = ProbeLeaseMode::LastHitOnly;
                } else {
                    throw py::value_error(
                        ("Unknown batch_probe_key policy: " + policy).c_str());
                }
                lease_policy.candidate_size = candidate_size;
                if (!self.is_client_initialized()) {
                    return std::vector<int>(
                        keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                }
                py::gil_scoped_release release;
                return self.store_->batchProbeKey(keys, lease_policy);
            },
            py::arg("keys"), py::arg("policy") = "none",
            py::arg("candidate_size") = 1,
            "Per-key existence results: 1 if readable, 0 if missing, and "
            "negative values on error. none grants no leases. LastHitOnly "
            "leases the last all-1 group of candidate_size results; failed "
            "candidates may retain partial leases after a concurrent eviction.")
        .def(
            "close",
            [](MooncakeStorePyWrapper &self) {
                int rc = self.store_ ? self.store_->tearDownAll() : 0;
                self.store_.reset();
                self.real_client_.reset();
                self.use_dummy_client_ = false;
                return rc;
            },
            "Close the store and clear the native client, including on "
            "cleanup failure. A nonzero result is terminal, not retryable.")
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
             py::arg("file_name").none() = py::none(),
             "Export a tensor stored under the given key into a safetensors "
             "file. "
             "If file_name is not provided, the key will be used as the "
             "filename.")
        .def("load_tensor_from_safetensor",
             &MooncakeStorePyWrapper::load_tensor_from_safetensor,
             py::arg("key").none() = py::none(), py::arg("file_name"),
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
        .def("get_tensor_into_cuda",
             &MooncakeStorePyWrapper::get_tensor_into_cuda, py::arg("key"),
             py::arg("tensor"), "Get tensor payload into a CUDA tensor")
        .def("batch_get_tensor_into_cuda",
             &MooncakeStorePyWrapper::batch_get_tensor_into_cuda,
             py::arg("keys"), py::arg("tensors_list"),
             "Get tensor payloads into pre-allocated CUDA tensors")
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
            "  split_dim: The tensor dimension used for TP sharding "
            "(default 0).")
        .def(
            "batch_get_tensor_with_tp_into",
            &MooncakeStorePyWrapper::batch_get_tensor_with_tp_into,
            py::arg("base_keys"), py::arg("buffer_ptrs"), py::arg("sizes"),
            py::arg("tp_rank") = 0, py::arg("tp_size") = 1,
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
               py::handle buf,
               const ReplicateConfig &config = ReplicateConfig{}) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                PythonBuffer buffer(buf);
                const auto &info = buffer.view();
                const auto value = contiguous_buffer_span(info);
                py::gil_scoped_release release;
                return self.store_->upsert(key, value, config);
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
                std::vector<PythonBuffer> infos;
                std::vector<std::span<const char>> spans;
                infos.reserve(parts.size());
                spans.reserve(parts.size());

                for (auto obj : parts) {
                    infos.emplace_back(obj);
                    const auto &info = infos.back().view();
                    if (info.ndim != 1 || info.itemsize != 1)
                        throw std::runtime_error(
                            "parts must be 1-D bytes-like");

                    spans.emplace_back(contiguous_buffer_span(info));
                }

                py::gil_scoped_release unlock;
                return self.store_->upsert_parts(key, spans, config);
            },
            py::arg("key"), py::arg("parts"),
            py::arg("config") = ReplicateConfig{},
            "Upsert multiple byte parts as a single object (insert or update)")
        .def(
            "upsert_batch",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<py::object> &buffers,
               const ReplicateConfig &config = ReplicateConfig{}) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                std::vector<PythonBuffer> infos;
                std::vector<std::span<const char>> spans;
                infos.reserve(buffers.size());
                spans.reserve(buffers.size());

                for (const auto &buf : buffers) {
                    infos.emplace_back(buf);
                    spans.emplace_back(
                        contiguous_buffer_span(infos.back().view()));
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
            "buffer for direct access operations. The caller must ensure no "
            "transfer on this buffer is in flight: deregistration does not "
            "wait for in-flight RDMA/NoF operations, and freeing or remapping "
            "the buffer while a transfer is active is unsafe")
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
            "prepare_get_into_ranges_snapshot",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys) {
                if (!self.is_client_initialized()) {
                    throw std::runtime_error("Client is not initialized");
                }
                std::vector<std::string> unique_keys;
                unique_keys.reserve(keys.size());
                std::unordered_set<std::string> seen;
                seen.reserve(keys.size());
                for (const auto &key : keys) {
                    if (seen.insert(key).second) unique_keys.push_back(key);
                }
                py::gil_scoped_release release;
                return std::make_shared<RangedReadSnapshotPy>(
                    self.store_, std::move(unique_keys));
            },
            py::arg("keys"),
            "Prepare a reusable metadata snapshot for ranged reads")
        .def(
            "get_into_ranges_from_snapshot",
            [](MooncakeStorePyWrapper &self,
               const std::shared_ptr<RangedReadSnapshotPy> &snapshot,
               const std::vector<uintptr_t> &buffer_ptrs,
               const std::vector<std::vector<std::string>> &all_keys,
               const std::vector<std::vector<std::vector<size_t>>>
                   &all_dst_offsets,
               const std::vector<std::vector<std::vector<size_t>>>
                   &all_src_offsets,
               const std::vector<std::vector<std::vector<size_t>>> &all_sizes) {
                if (!self.is_client_initialized()) {
                    throw std::runtime_error("Client is not initialized");
                }
                if (!snapshot || !snapshot->belongs_to(self.store_)) {
                    throw std::invalid_argument(
                        "Ranged-read snapshot belongs to another Store");
                }
                std::vector<void *> buffers;
                buffers.reserve(buffer_ptrs.size());
                for (uintptr_t ptr : buffer_ptrs) {
                    buffers.push_back(reinterpret_cast<void *>(ptr));
                }
                py::gil_scoped_release release;
                return snapshot->get_into_ranges(buffers, all_keys,
                                                 all_dst_offsets,
                                                 all_src_offsets, all_sizes);
            },
            py::arg("snapshot"), py::arg("buffer_ptrs"), py::arg("all_keys"),
            py::arg("all_dst_offsets"), py::arg("all_src_offsets"),
            py::arg("all_sizes"),
            "Get byte ranges using a reusable metadata snapshot. The "
            "snapshot is refreshed at the read-lease midpoint before a new "
            "transfer is submitted.")
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
               py::handle buf,
               const ReplicateConfig &config = ReplicateConfig{}) {
                PythonBuffer buffer(buf);
                const auto &info = buffer.view();
                const auto value = contiguous_buffer_span(info);
                py::gil_scoped_release release;
                return self.store_->put(key, value, config);
            },
            py::arg("key"), py::arg("value"),
            py::arg("config") = ReplicateConfig{})
        .def(
            "put_parts",
            [](MooncakeStorePyWrapper &self, const std::string &key,
               py::args parts,
               const ReplicateConfig &config = ReplicateConfig{}) {
                // 1) Python buffer → span
                std::vector<PythonBuffer> infos;
                std::vector<std::span<const char>> spans;
                infos.reserve(parts.size());
                spans.reserve(parts.size());

                for (auto obj : parts) {
                    infos.emplace_back(obj);
                    const auto &info = infos.back().view();
                    if (info.ndim != 1 || info.itemsize != 1)
                        throw std::runtime_error(
                            "parts must be 1-D bytes-like");

                    spans.emplace_back(contiguous_buffer_span(info));
                }

                // 2) Call C++ function
                py::gil_scoped_release unlock;
                return self.store_->put_parts(key, spans, config);
            },
            py::arg("key"), py::arg("parts"),
            py::arg("config") = ReplicateConfig{})
        .def(
            "put_batch",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<py::object> &buffers,
               const ReplicateConfig &config = ReplicateConfig{}) {
                // Convert pybuffers to spans without copying
                std::vector<PythonBuffer> infos;
                std::vector<std::span<const char>> spans;
                infos.reserve(buffers.size());
                spans.reserve(buffers.size());

                for (const auto &buf : buffers) {
                    infos.emplace_back(buf);
                    spans.emplace_back(
                        contiguous_buffer_span(infos.back().view()));
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
            "batch_get_session_start",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<int>{};
                }
                py::gil_scoped_release release;
                return self.store_->batch_get_session_start(keys);
            },
            py::arg("keys"),
            "Start a get session: query replicas once and cache them")
        .def(
            "batch_get_into_multi_buffer_ranges",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<std::vector<uintptr_t>> &all_buffer_ptrs,
               const std::vector<std::vector<size_t>> &all_sizes,
               const std::vector<std::vector<size_t>> &all_src_offsets) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<int>{};
                }
                py::gil_scoped_release release;
                return self.store_->batch_get_into_multi_buffer_ranges(
                    keys, CastAddrs2Ptrs(all_buffer_ptrs), all_sizes,
                    all_src_offsets);
            },
            py::arg("keys"), py::arg("all_buffer_ptrs"), py::arg("all_sizes"),
            py::arg("all_src_offsets"),
            "Ranged get into multiple buffers using a get session")
        .def(
            "batch_get_session_end",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return -1;
                }
                py::gil_scoped_release release;
                return self.store_->batch_get_session_end(keys);
            },
            py::arg("keys"),
            "End a get session and drop cached replica metadata")
        .def(
            "batch_put_session_start",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<size_t> &sizes,
               const ReplicateConfig &config = ReplicateConfig{}) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<int>{};
                }
                py::gil_scoped_release release;
                return self.store_->batch_put_session_start(keys, sizes,
                                                            config);
            },
            py::arg("keys"), py::arg("sizes"),
            py::arg("config") = ReplicateConfig{},
            "Start a put session: reserve objects without transfer")
        .def(
            "batch_put_from_multi_buffer_ranges",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys,
               const std::vector<std::vector<uintptr_t>> &all_buffer_ptrs,
               const std::vector<std::vector<size_t>> &all_sizes,
               const std::vector<std::vector<size_t>> &all_dst_offsets) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<int>{};
                }
                py::gil_scoped_release release;
                return self.store_->batch_put_from_multi_buffer_ranges(
                    keys, CastAddrs2Ptrs(all_buffer_ptrs), all_sizes,
                    all_dst_offsets);
            },
            py::arg("keys"), py::arg("all_buffer_ptrs"), py::arg("all_sizes"),
            py::arg("all_dst_offsets"),
            "Ranged put from multiple buffers using a put session")
        .def(
            "batch_put_session_end",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<int>{};
                }
                py::gil_scoped_release release;
                return self.store_->batch_put_session_end(keys);
            },
            py::arg("keys"), "Complete a put session and make objects readable")
        .def(
            "batch_put_session_revoke",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &keys) {
                if (!self.is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<int>{};
                }
                py::gil_scoped_release release;
                return self.store_->batch_put_session_revoke(keys);
            },
            py::arg("keys"), "Revoke an incomplete put session")
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
            "success, error code: 0 if success, non-zero if failure)")
        .def(
            "create_drain_job",
            [](MooncakeStorePyWrapper &self,
               const std::vector<std::string> &segments,
               const std::vector<std::string> &target_segments,
               int64_t max_concurrency, int64_t timeout_ms) -> py::tuple {
                if (!self.is_client_initialized() || max_concurrency <= 0 ||
                    max_concurrency > std::numeric_limits<uint32_t>::max() ||
                    timeout_ms < 0) {
                    return py::make_tuple(UUID{},
                                          toInt(ErrorCode::INVALID_PARAMS));
                }
                py::gil_scoped_release release;
                CreateDrainJobRequest request{
                    segments, target_segments,
                    static_cast<uint32_t>(max_concurrency)};
                auto result =
                    timeout_ms == 0
                        ? self.store_->create_drain_job(request)
                        : self.store_->create_timed_drain_job(
                              request, static_cast<uint64_t>(timeout_ms));
                py::gil_scoped_acquire acquire;
                return py::make_tuple(result.value_or(UUID{}),
                                      result ? 0 : toInt(result.error()));
            },
            py::arg("segments"),
            py::arg("target_segments") = std::vector<std::string>{},
            py::arg("max_concurrency") = 4, py::arg("timeout_ms") = 0,
            "Start draining mounted memory segments.\n\n"
            "Args:\n"
            "    segments: Source segment names.\n"
            "    target_segments: Target segment names; an empty list lets "
            "the master choose.\n"
            "    max_concurrency: Maximum concurrent tasks (default: 4).\n"
            "    timeout_ms: Timeout in milliseconds from job creation. "
            "Defaults to 0, which means no timeout is set.\n\n"
            "On timeout, the job cancels in-flight tasks and keeps "
            "unfinished sources DRAINING.\n\n"
            "Returns:\n"
            "    tuple[UUID, int]: (job ID, error code; 0 on success).")
        .def(
            "query_drain_job",
            [](MooncakeStorePyWrapper &self, const UUID &job_id) -> py::tuple {
                if (!self.is_client_initialized()) {
                    return py::make_tuple(py::none(),
                                          toInt(ErrorCode::INVALID_PARAMS));
                }
                py::gil_scoped_release release;
                auto result = self.store_->query_drain_job(job_id);
                py::gil_scoped_acquire acquire;
                if (!result) {
                    return py::make_tuple(py::none(), toInt(result.error()));
                }
                return py::make_tuple(std::move(*result), 0);
            },
            py::arg("job_id"),
            "Return (job, error_code). The job is None on error.")
        .def(
            "cancel_drain_job",
            [](MooncakeStorePyWrapper &self, const UUID &job_id) {
                if (!self.is_client_initialized()) {
                    return toInt(ErrorCode::INVALID_PARAMS);
                }
                py::gil_scoped_release release;
                auto result = self.store_->cancel_drain_job(job_id);
                return result ? 0 : toInt(result.error());
            },
            py::arg("job_id"),
            "Request cancellation. In-flight moves may finish; query until "
            "the job reaches CANCELED, or TIMED_OUT if its deadline triggered "
            "first. Cancellation may wait for an executor or replication "
            "cleanup. Returns an error code.")
        .def(
            "list_drain_jobs",
            [](MooncakeStorePyWrapper &self, std::optional<JobStatus> status,
               const std::optional<std::string> &segment, int64_t limit,
               const std::string &page_token) -> py::tuple {
                if (!self.is_client_initialized() || limit < 1 ||
                    limit > 1000) {
                    return py::make_tuple(py::none(),
                                          toInt(ErrorCode::INVALID_PARAMS));
                }
                py::gil_scoped_release release;
                auto result = self.store_->list_drain_jobs(
                    {status, segment, page_token,
                     static_cast<uint32_t>(limit)});
                py::gil_scoped_acquire acquire;
                if (!result) {
                    return py::make_tuple(py::none(), toInt(result.error()));
                }
                return py::make_tuple(std::move(*result), 0);
            },
            py::arg("status").none() = py::none(),
            py::arg("segment").none() = py::none(), py::arg("limit") = 100,
            py::arg("page_token") = "",
            "Return (page, error_code) for cluster-wide drain jobs. Follow "
            "page.next_page_token with the same filters until it is empty.")
        .def(
            "list_tasks",
            [](MooncakeStorePyWrapper &self, std::optional<TaskStatus> status,
               std::optional<TaskType> type,
               std::optional<UUID> assigned_client,
               const std::optional<std::string> &key, int64_t limit,
               const std::string &page_token) -> py::tuple {
                if (!self.is_client_initialized() || limit < 1 ||
                    limit > 1000) {
                    return py::make_tuple(py::none(),
                                          toInt(ErrorCode::INVALID_PARAMS));
                }
                py::gil_scoped_release release;
                auto result = self.store_->list_tasks(
                    {.status = status,
                     .type = type,
                     .assigned_client = assigned_client,
                     .key = key,
                     .page_token = page_token,
                     .limit = static_cast<uint32_t>(limit)});
                py::gil_scoped_acquire acquire;
                if (!result) {
                    return py::make_tuple(py::none(), toInt(result.error()));
                }
                return py::make_tuple(std::move(*result), 0);
            },
            py::arg("status").none() = py::none(),
            py::arg("type").none() = py::none(),
            py::arg("assigned_client").none() = py::none(),
            py::arg("key").none() = py::none(), py::arg("limit") = 100,
            py::arg("page_token") = "",
            "Return (page, error_code) for tasks in this client's tenant. "
            "Follow "
            "page.next_page_token with the same filters until it is empty.")
        .def(
            "cancel_task",
            [](MooncakeStorePyWrapper &self, const UUID &task_id) {
                if (!self.is_client_initialized()) {
                    return toInt(ErrorCode::INVALID_PARAMS);
                }
                py::gil_scoped_release release;
                auto result = self.store_->cancel_task(task_id);
                return result ? 0 : toInt(result.error());
            },
            py::arg("task_id"),
            "Cancel a pending task immediately or request cancellation of a "
            "processing task. Returns an error code. Query the final status.");

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

            int rc;
            {
                // numa_run_on_node() fills libnuma's per-node cpumask cache
                // on first use; see numaNodeCpuCacheMutex().
                std::lock_guard<std::mutex> guard(
                    mooncake::numaNodeCpuCacheMutex());
                rc = numa_run_on_node(node);
            }
            if (rc != 0) {
                LOG(WARNING) << "numa_run_on_node failed for node " << node;
            }

            // Prefer this NUMA node for future allocations but allow fallback
            numa_set_bind_policy(0);  // non-strict binding
            numa_set_preferred(node);
        },
        py::arg("node"),
        "Bind the current thread and memory allocation preference to the "
        "specified NUMA node");

    m.def("get_alloc_func_addr", &get_alloc_func_addr);
    m.def("get_free_func_addr", &get_free_func_addr);
}

}  // namespace mooncake
