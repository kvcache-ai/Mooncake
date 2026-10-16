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

template <typename CudaIpcBatchWriteFn>
std::optional<std::vector<int>> try_dummy_cuda_ipc_batch_write_tensor_impl(
    const std::vector<std::string> &keys,
    const std::vector<PyTensorInfo> &infos, const ReplicateConfig &config,
    const char *operation_name, CudaIpcBatchWriteFn &&write_cuda_ipc) {
    if (!use_dummy_client_ || keys.size() != infos.size()) return std::nullopt;

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
            if (!owner.attr("is_cuda").cast<bool>()) continue;
            cuda_stream_contexts[i] = CudaStreamContext{
                .device_id = owner.attr("get_device")().cast<int32_t>(),
                .stream_handle =
                    torch_module()
                        .attr("cuda")
                        .attr("current_stream")(owner.attr("device"))
                        .attr("cuda_stream")
                        .cast<uintptr_t>(),
            };
        } catch (const py::error_already_set &) {
            PyErr_Clear();
        } catch (const py::cast_error &) {
            PyErr_Clear();
        }
    }

    std::vector<int> results(keys.size(), 0);
    py::gil_scoped_release release_gil;

    std::vector<std::optional<CudaIpcBufferHandle>> ipc_payloads(infos.size());
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
            cuda_stream_contexts[i]->device_id != ipc_payloads[i]->device_id) {
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
                LOG(ERROR) << "CUDA IPC tensor stream synchronization failed";
                for (size_t index : original_indices) {
                    results[index] = to_py_ret(ErrorCode::INTERNAL_ERROR);
                }
                return results;
            }
        }
        std::vector<int> op_results =
            write_cuda_ipc(dummy_client, write_requests, write_config);
        if (!apply_indexed_results(operation_name, op_results, original_indices,
                                   results)) {
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
        [](auto dummy_client, const auto &requests, const auto &write_config) {
            return dummy_client->batch_put_from_cuda_ipc(requests,
                                                         write_config);
        });
}

std::optional<std::vector<int>> try_dummy_cuda_ipc_batch_upsert_tensor_impl(
    const std::vector<std::string> &keys,
    const std::vector<PyTensorInfo> &infos, const ReplicateConfig &config) {
    return try_dummy_cuda_ipc_batch_write_tensor_impl(
        keys, infos, config, "upsert",
        [](auto dummy_client, const auto &requests, const auto &write_config) {
            return dummy_client->batch_upsert_from_cuda_ipc(requests,
                                                            write_config);
        });
}

inline void append_tensor_write_buffers(const PyTensorInfo &info,
                                        std::vector<void *> &buffers,
                                        std::vector<size_t> &sizes) {
    buffers.push_back(const_cast<TensorMetadata *>(&info.metadata));
    sizes.push_back(info.metadata.header.data_offset);
    if (info.tensor_size > 0) {
        buffers.push_back(reinterpret_cast<void *>(info.data_ptr));
        sizes.push_back(info.tensor_size);
    }
}

inline void apply_tensor_batch_results(
    std::vector<int> &results, const std::vector<size_t> &original_indices,
    const std::vector<int> &op_results, const char *operation_name) {
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

    auto group_ids_error =
        ValidateGroupIdsForBatchConfig(config, keys.size(), operation_name);
    if (!group_ids_error.empty()) {
        return group_ids_error;
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

                size_t total_size =
                    infos[i].metadata.header.data_offset + infos[i].tensor_size;
                auto alloc_result = store_->allocate_client_buffer(total_size);
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
                            reinterpret_cast<const void *>(infos[i].data_ptr),
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
                temp_allocations.push_back(
                    std::make_unique<BufferHandle>(std::move(*alloc_result)));
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
            LOG(ERROR) << operation_name << ": real client is not available";
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
            apply_tensor_batch_results(results, original_indices, op_results,
                                       operation_name);
        }
    }

    return results;
}

bool ensure_tensor_write_supported(const char *operation_name) const {
    if (!is_client_initialized()) {
        LOG(ERROR) << operation_name << ": client not initialized";
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

bool validate_tensor_object_buffers(const std::vector<std::string> &keys,
                                    const std::vector<uintptr_t> &buffer_ptrs,
                                    const std::vector<size_t> &sizes,
                                    const char *size_error_context,
                                    const char *buffer_error_context) {
    if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << size_error_context;
        return false;
    }
    for (size_t i = 0; i < sizes.size(); ++i) {
        if (!is_valid_tensor_object_buffer(buffer_ptrs[i], sizes[i],
                                           std::string(buffer_error_context) +
                                               " at index " +
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
    const std::vector<uintptr_t> &buffer_ptrs, const std::vector<size_t> &sizes,
    const ReplicateConfig &config, BatchWriteFromFn &&batch_write_from_fn) {
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

bool is_valid_tensor_object_buffer(uintptr_t buffer_ptr, size_t size,
                                   const std::string &op_name) {
    return parse_tensor_metadata_from_raw_buffer(buffer_ptr, size,
                                                 op_name.c_str())
        .has_value();
}
