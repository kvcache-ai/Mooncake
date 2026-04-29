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
std::vector<int> batch_write_tensor_impl(const std::vector<std::string> &keys,
                                         const std::vector<PyTensorInfo> &infos,
                                         const ReplicateConfig &config,
                                         const char *operation_name,
                                         BatchWriteFromFn &&batch_write_from) {
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
                LOG(ERROR) << "Failed to allocate buffer for " << operation_name
                           << " key: " << keys[i];
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

    std::vector<int> results(keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
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
    LegacyTpWriteFn &&legacy_tp_write, TpParallelWriteFn &&tp_parallel_write,
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
            return writer_shard_write(key, tensor, *request.writer_partition,
                                      config);
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
                    return tp_parallel_write(
                        key, tensor, config, axis.rank, axis.size,
                        axis.split_dim.value_or(0), request.parallelism->axes);
                }
                case ParallelismWriteStorageRoute::GENERIC_PARALLELISM_SHARD:
                    return generic_shard_write(key, tensor,
                                               *request.parallelism, config);
                default:
                    break;
            }
            break;
        }
    }
    return to_py_ret(ErrorCode::INVALID_PARAMS);
}

template <typename WritePartsFn>
int execute_tensor_parts_write(const std::string &key,
                               const std::vector<std::span<const char>> &values,
                               const ReplicateConfig &config,
                               const TensorWriteStoreOps &ops,
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
int execute_parallelism_direct_tensor_write_from(const std::string &key,
                                                 uintptr_t buffer_ptr,
                                                 size_t size,
                                                 const ReplicateConfig &config,
                                                 const char *operation_name,
                                                 DirectWriteFn &&direct_write) {
    if (is_default_replicate_config(config)) {
        return direct_write(key, buffer_ptr, size);
    }
    return execute_single_tensor_write_from(
        operation_name, key, buffer_ptr, size, config,
        [&](const std::string &write_key, void *buffer, size_t write_size,
            const ReplicateConfig &write_config) {
            return direct_write(write_key, reinterpret_cast<uintptr_t>(buffer),
                                write_size, write_config);
        });
}

pybind11::object decode_tensor_object_buffer(uintptr_t buffer_ptr, size_t size,
                                             const char *operation_name) {
    if (!is_valid_tensor_object_buffer(buffer_ptr, size, operation_name)) {
        return py::none();
    }
    pybind11::object tensor = buffer_to_tensor(
        NULL, reinterpret_cast<char *>(buffer_ptr), static_cast<int64_t>(size));
    if (tensor.is_none()) {
        LOG(ERROR) << "Failed to decode tensor buffer for " << operation_name;
    }
    return tensor;
}

std::string get_tp_write_shard_key(const std::string &base_key, int rank,
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
            [&](int rank) { return get_tp_write_shard_key(key, rank, axes); },
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
int execute_tp_tensor_write_from_impl(const std::string &key,
                                      uintptr_t buffer_ptr, size_t size,
                                      const ReplicateConfig &config,
                                      int tp_size, int split_dim,
                                      const std::vector<ParallelAxisSpec> &axes,
                                      const char *error_context,
                                      WritePartsFn &&write_parts) {
    auto parsed =
        parse_tensor_metadata_from_raw_buffer(buffer_ptr, size, error_context);
    if (!parsed.has_value()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }

    for (int rank = 0; rank < tp_size; ++rank) {
        std::string shard_key = get_tp_write_shard_key(key, rank, axes);
        if (shard_key.empty()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }

        auto plan = build_raw_tensor_shard_write_plan(*parsed, split_dim, rank,
                                                      tp_size, error_context);
        if (!plan.has_value()) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        const auto global_shape = TensorShapeToVector(
            parsed->metadata.layout.global_shape, parsed->metadata.header.ndim);
        const auto local_shape = TensorShapeToVector(
            plan->metadata.layout.local_shape, plan->metadata.header.ndim);
        if (axes.empty()) {
            ParallelAxisSpec axis_spec{"tp",      rank,         tp_size,
                                       split_dim, std::nullopt, std::nullopt};
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
            const auto local_shape = TensorShapeToVector(
                plan->metadata.layout.local_shape, plan->metadata.header.ndim);
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
                tensor, tp_size, split_dim,
                [&](int rank) { return get_tp_key_name(base_keys[i], rank); });
            if (!shard_infos.has_value()) {
                final_results[i] = to_py_ret(ErrorCode::INVALID_PARAMS);
                continue;
            }

            processed_indices.push_back(i);
            for (int rank = 0; rank < tp_size; ++rank) {
                all_chunk_keys.push_back(get_tp_key_name(base_keys[i], rank));
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
    const std::vector<uintptr_t> &buffer_ptrs, const std::vector<size_t> &sizes,
    const char *decode_error_context,
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
    auto parsed =
        parse_tensor_metadata_from_raw_buffer(buffer_ptr, size, error_context);
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
    return execute_tensor_parts_write(shard_key, values, config, ops,
                                      std::forward<WriteShardFn>(write_shard));
}

template <typename WritePartsFn, typename WriteManifestFn>
int execute_writer_partition_shard_write_from(
    const std::string &key, uintptr_t buffer_ptr, size_t size,
    const WriterPartitionSpec &writer, const ReplicateConfig &config,
    const char *error_context, const TensorWriteStoreOps &ops,
    WritePartsFn &&write_parts, WriteManifestFn &&write_manifest) {
    auto parsed =
        parse_tensor_metadata_from_raw_buffer(buffer_ptr, size, error_context);
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
            reinterpret_cast<const char *>(buffer_ptr + src_offset), part_size);
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
            return writer_shard_from_write(key, buffer_ptr, size,
                                           *request.writer_partition, config);
        case ParallelismWriteStorageRoute::LEGACY_SINGLE_TP:
        case ParallelismWriteStorageRoute::TP_SHARDED_PARALLELISM: {
            if (!request.parallelism.has_value()) {
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }
            auto tp_axis_index = find_tp_axis_index(request.parallelism->axes);
            if (!tp_axis_index.has_value()) {
                return to_py_ret(ErrorCode::INVALID_PARAMS);
            }
            const auto &axis = request.parallelism->axes[*tp_axis_index];
            const auto axes =
                request.route == ParallelismWriteStorageRoute::LEGACY_SINGLE_TP
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
        put_tensor_info_impl(writer_info->shard_key, writer_info->info, config);
    if (ret != 0) {
        return ret;
    }
    return put_manifest_impl(get_writer_manifest_key_name(key),
                             writer_info->manifest, config);
}

int put_tensor_parallelism_tp_impl(const std::string &key,
                                   pybind11::object tensor,
                                   const ReplicateConfig &config, int tp_rank,
                                   int tp_size, int split_dim,
                                   const std::vector<ParallelAxisSpec> &axes) {
    std::string shard_key = get_tp_write_shard_key(key, tp_rank, axes);
    if (shard_key.empty()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }
    auto shard_info = build_direct_parallelism_shard_info(
        tensor, axes, shard_key, true /* infer_global_shape */);
    if (!shard_info.has_value()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }
    int ret = put_tensor_info_impl(shard_key, shard_info->info, config);
    if (ret != 0) {
        return ret;
    }
    return put_manifest_impl(get_parallelism_manifest_key_name(key),
                             shard_info->manifest, config);
}

int put_requested_parallelism_shard(const std::string &key,
                                    pybind11::object tensor,
                                    const TensorParallelismSpec &parallelism,
                                    const ReplicateConfig &config) {
    std::string shard_key = get_parallelism_key_name(key, parallelism);
    auto shard_info = build_requested_parallelism_shard_info(
        tensor, parallelism, shard_key, "put_tensor_with_parallelism");
    if (!shard_info.has_value()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }
    int ret = put_tensor_info_impl(shard_key, shard_info->info, config);
    if (ret != 0) {
        return ret;
    }
    return put_manifest_impl(get_parallelism_manifest_key_name(key),
                             shard_info->manifest, config);
}

int put_direct_parallelism_shard(const std::string &key,
                                 pybind11::object tensor,
                                 const TensorParallelismSpec &parallelism,
                                 const ReplicateConfig &config) {
    auto tp_axis_index = find_tp_axis_index(parallelism.axes);
    if (tp_axis_index.has_value() && parallelism.axes.size() > 1) {
        return put_requested_parallelism_shard(key, tensor, parallelism,
                                               config);
    }
    auto info =
        build_direct_parallelism_shard_info(tensor, parallelism.axes, key);
    if (!info.has_value()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }
    return put_tensor_info_impl(get_parallelism_key_name(key, parallelism),
                                *info, config);
}

int put_tensor_with_writer_shards_from(
    const std::string &key, uintptr_t buffer_ptr, size_t size,
    const WriterPartitionSpec &writer,
    const ReplicateConfig &config = ReplicateConfig{}) {
    return execute_writer_partition_shard_write_from(
        key, buffer_ptr, size, writer, config,
        "put_tensor_with_parallelism_from", TensorWriteStoreOps{"put_parts"},
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

int put_direct_parallelism_shard_from(const std::string &key,
                                      uintptr_t buffer_ptr, size_t size,
                                      const TensorParallelismSpec &parallelism,
                                      const ReplicateConfig &config) {
    return execute_direct_parallelism_shard_write_from(
        key, buffer_ptr, size, parallelism, config,
        "put_tensor_with_parallelism_from", TensorWriteStoreOps{"put_parts"},
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
                                           write_config, rank, size, split_dim);
        },
        [this](const std::string &write_key, pybind11::object write_tensor,
               const ReplicateConfig &write_config, int rank, int size,
               int split_dim, const std::vector<ParallelAxisSpec> &axes) {
            if (axes.size() == 1) {
                return put_tensor_parallelism_tp_impl(write_key, write_tensor,
                                                      write_config, rank, size,
                                                      split_dim, axes);
            }
            return put_requested_parallelism_shard(write_key, write_tensor,
                                                   TensorParallelismSpec{axes},
                                                   write_config);
        },
        [this](const std::string &write_key, pybind11::object write_tensor,
               const TensorParallelismSpec &parallelism_spec,
               const ReplicateConfig &write_config) {
            return put_direct_parallelism_shard(write_key, write_tensor,
                                                parallelism_spec, write_config);
        });
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

std::vector<int> batch_put_tensor_with_parallelism(
    const std::vector<std::string> &keys, const pybind11::list &tensors_list,
    const py::object &parallelisms = py::none(),
    const ReplicateConfig &config = ReplicateConfig{},
    const py::object &writer_partitions = py::none()) {
    return execute_batch_parallelism_write_requests(
        keys, tensors_list.size(), parallelisms, writer_partitions,
        "batch_put_tensor_with_parallelism",
        [this, &keys, &tensors_list, &config]() {
            if (!is_client_initialized() || use_dummy_client_) {
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            int validate_result = validate_replicate_config(config);
            if (validate_result) {
                return std::vector<int>(keys.size(), validate_result);
            }
            return batch_put_tensor_impl(keys, tensors_list, config);
        },
        [this, &keys, &tensors_list, &config](size_t i,
                                              const py::handle &parallelism) {
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

int execute_put_tensor_with_parallelism_from_route(
    const std::string &key, uintptr_t buffer_ptr, size_t size,
    const ResolvedParallelismWriteRequest &request,
    const ReplicateConfig &config) {
    return execute_parallelism_tensor_write_from_route(
        key, buffer_ptr, size, request, config,
        "put_tensor_with_parallelism_from",
        [this](const std::string &write_key, uintptr_t write_buffer_ptr,
               size_t write_size, const ReplicateConfig &write_config) {
            if (!is_default_replicate_config(write_config)) {
                if (!is_valid_tensor_object_buffer(
                        write_buffer_ptr, write_size,
                        "put_tensor_with_parallelism_from")) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                if (!ensure_tensor_write_supported(
                        "put_tensor_with_parallelism_from")) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                int validate_result = validate_replicate_config(write_config);
                if (validate_result) return validate_result;
                py::gil_scoped_release release_gil;
                return store_->put_from(
                    write_key, reinterpret_cast<void *>(write_buffer_ptr),
                    write_size, write_config);
            }
            return put_tensor_from(write_key, write_buffer_ptr, write_size);
        },
        [this](const std::string &write_key, uintptr_t write_buffer_ptr,
               size_t write_size, const WriterPartitionSpec &writer,
               const ReplicateConfig &write_config) {
            return put_tensor_with_writer_shards_from(
                write_key, write_buffer_ptr, write_size, writer, write_config);
        },
        [this](const std::string &write_key, uintptr_t write_buffer_ptr,
               size_t write_size, int tp_size, int split_dim,
               const std::vector<ParallelAxisSpec> &axes,
               const ReplicateConfig &write_config) {
            return execute_tp_tensor_write_from_impl(
                write_key, write_buffer_ptr, write_size, write_config, tp_size,
                split_dim, axes, "put_tensor_with_parallelism_from",
                [this](const std::string &shard_key,
                       const std::vector<std::span<const char>> &values,
                       const ReplicateConfig &parts_config) {
                    py::gil_scoped_release release_gil;
                    int ret =
                        store_->put_parts(shard_key, values, parts_config);
                    if (ret != 0) {
                        LOG(ERROR) << "put_parts failed for key " << shard_key
                                   << " with code " << ret;
                    }
                    return ret;
                });
        },
        [this](const std::string &write_key, uintptr_t write_buffer_ptr,
               size_t write_size, const TensorParallelismSpec &parallelism_spec,
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
    return execute_put_tensor_with_parallelism_from_route(key, buffer_ptr, size,
                                                          *request, config);
}

std::vector<int> batch_put_tensor_with_parallelism_from(
    const std::vector<std::string> &keys,
    const std::vector<uintptr_t> &buffer_ptrs, const std::vector<size_t> &sizes,
    const py::object &parallelisms = py::none(),
    const ReplicateConfig &config = ReplicateConfig{},
    const py::object &writer_partitions = py::none()) {
    return execute_batch_parallelism_write_requests(
        keys, buffer_ptrs.size(), parallelisms, writer_partitions,
        "batch_put_tensor_with_parallelism_from",
        [this, &keys, &buffer_ptrs, &sizes, &config]() {
            if (!is_default_replicate_config(config)) {
                if (!is_client_initialized() || use_dummy_client_) {
                    return std::vector<int>(
                        keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                }
                int validate_result = validate_replicate_config(config);
                if (validate_result) {
                    return std::vector<int>(keys.size(), validate_result);
                }
                if (keys.size() != buffer_ptrs.size() ||
                    keys.size() != sizes.size()) {
                    LOG(ERROR) << "Size mismatch: keys, buffer_ptrs, and sizes "
                                  "must have the same length";
                    return std::vector<int>(
                        keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                }
                for (size_t i = 0; i < sizes.size(); ++i) {
                    if (!is_valid_tensor_object_buffer(
                            buffer_ptrs[i], sizes[i],
                            "tensor object buffer at index " +
                                std::to_string(i))) {
                        return std::vector<int>(
                            keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                    }
                }
                std::vector<void *> buffers;
                buffers.reserve(buffer_ptrs.size());
                for (uintptr_t ptr : buffer_ptrs) {
                    buffers.push_back(reinterpret_cast<void *>(ptr));
                }
                py::gil_scoped_release release_gil;
                return store_->batch_put_from(keys, buffers, sizes, config);
            }
            return batch_put_tensor_from(keys, buffer_ptrs, sizes);
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

int upsert_tensor_with_writer_shards(
    const std::string &key, pybind11::object tensor,
    const WriterPartitionSpec &writer,
    const ReplicateConfig &config = ReplicateConfig{}) {
    if (!ensure_tensor_write_supported("upsert_tensor_with_writer_shards")) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }

    auto writer_info = build_writer_shard_tensor_info(
        key, tensor, writer, "upsert_tensor_with_parallelism");
    if (!writer_info.has_value()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }

    int ret = upsert_tensor_info_impl(writer_info->shard_key, writer_info->info,
                                      config);
    if (ret != 0) {
        return ret;
    }
    return upsert_manifest_impl(get_writer_manifest_key_name(key),
                                writer_info->manifest, config);
}

int upsert_tensor_parallelism_tp_impl(
    const std::string &key, pybind11::object tensor,
    const ReplicateConfig &config, int tp_rank, int tp_size, int split_dim,
    const std::vector<ParallelAxisSpec> &axes) {
    std::string shard_key = get_tp_write_shard_key(key, tp_rank, axes);
    if (shard_key.empty()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }
    auto shard_info = build_direct_parallelism_shard_info(
        tensor, axes, shard_key, true /* infer_global_shape */);
    if (!shard_info.has_value()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }
    int ret = upsert_tensor_info_impl(shard_key, shard_info->info, config);
    if (ret != 0) {
        return ret;
    }
    return upsert_manifest_impl(get_parallelism_manifest_key_name(key),
                                shard_info->manifest, config);
}

int upsert_requested_parallelism_shard(const std::string &key,
                                       pybind11::object tensor,
                                       const TensorParallelismSpec &parallelism,
                                       const ReplicateConfig &config) {
    std::string shard_key = get_parallelism_key_name(key, parallelism);
    auto shard_info = build_requested_parallelism_shard_info(
        tensor, parallelism, shard_key, "upsert_tensor_with_parallelism");
    if (!shard_info.has_value()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }
    int ret = upsert_tensor_info_impl(shard_key, shard_info->info, config);
    if (ret != 0) {
        return ret;
    }
    return upsert_manifest_impl(get_parallelism_manifest_key_name(key),
                                shard_info->manifest, config);
}

int upsert_direct_parallelism_shard(const std::string &key,
                                    pybind11::object tensor,
                                    const TensorParallelismSpec &parallelism,
                                    const ReplicateConfig &config) {
    auto tp_axis_index = find_tp_axis_index(parallelism.axes);
    if (tp_axis_index.has_value() && parallelism.axes.size() > 1) {
        return upsert_requested_parallelism_shard(key, tensor, parallelism,
                                                  config);
    }
    auto info =
        build_direct_parallelism_shard_info(tensor, parallelism.axes, key);
    if (!info.has_value()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }
    return upsert_tensor_info_impl(get_parallelism_key_name(key, parallelism),
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
            return upsert_manifest_impl(manifest_key, manifest, write_config);
        });
}

int upsert_direct_parallelism_shard_from(
    const std::string &key, uintptr_t buffer_ptr, size_t size,
    const TensorParallelismSpec &parallelism, const ReplicateConfig &config) {
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

int execute_upsert_tensor_with_parallelism_route(
    const std::string &key, pybind11::object tensor,
    const ResolvedParallelismWriteRequest &request,
    const ReplicateConfig &config) {
    return execute_parallelism_tensor_write_route(
        key, tensor, request, config, "upsert_tensor_with_parallelism",
        [this](const std::string &write_key, pybind11::object write_tensor,
               const ReplicateConfig &write_config) {
            return upsert_tensor_impl(write_key, write_tensor, write_config);
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
            return upsert_tensor_with_tp_impl(write_key, write_tensor,
                                              write_config, size, split_dim);
        },
        [this](const std::string &write_key, pybind11::object write_tensor,
               const ReplicateConfig &write_config, int rank, int size,
               int split_dim, const std::vector<ParallelAxisSpec> &axes) {
            if (axes.size() == 1) {
                return upsert_tensor_parallelism_tp_impl(
                    write_key, write_tensor, write_config, rank, size,
                    split_dim, axes);
            }
            return upsert_requested_parallelism_shard(
                write_key, write_tensor, TensorParallelismSpec{axes},
                write_config);
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
    return execute_upsert_tensor_with_parallelism_route(key, tensor, *request,
                                                        config);
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
            if (!is_default_replicate_config(write_config)) {
                if (!is_valid_tensor_object_buffer(
                        write_buffer_ptr, write_size,
                        "upsert_tensor_with_parallelism_from")) {
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                void *buffer = reinterpret_cast<void *>(write_buffer_ptr);
                if (!is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                if (use_dummy_client_) {
                    LOG(ERROR) << "upsert_tensor_with_parallelism_from is not "
                                  "supported for dummy client";
                    return to_py_ret(ErrorCode::INVALID_PARAMS);
                }
                int validate_result = validate_replicate_config(write_config);
                if (validate_result) return validate_result;
                py::gil_scoped_release release_gil;
                return store_->upsert_from(write_key, buffer, write_size,
                                           write_config);
            }
            return upsert_tensor_from(write_key, write_buffer_ptr, write_size);
        },
        [this](const std::string &write_key, uintptr_t write_buffer_ptr,
               size_t write_size, const WriterPartitionSpec &writer,
               const ReplicateConfig &write_config) {
            return upsert_tensor_with_writer_shards_from(
                write_key, write_buffer_ptr, write_size, writer, write_config);
        },
        [this](const std::string &write_key, uintptr_t write_buffer_ptr,
               size_t write_size, int tp_size, int split_dim,
               const std::vector<ParallelAxisSpec> &axes,
               const ReplicateConfig &write_config) {
            return execute_tp_tensor_write_from_impl(
                write_key, write_buffer_ptr, write_size, write_config, tp_size,
                split_dim, axes, "upsert_tensor_with_parallelism_from",
                [this](const std::string &shard_key,
                       const std::vector<std::span<const char>> &values,
                       const ReplicateConfig &parts_config) {
                    py::gil_scoped_release release_gil;
                    int ret =
                        store_->upsert_parts(shard_key, values, parts_config);
                    if (ret != 0) {
                        LOG(ERROR) << "upsert_parts failed for key "
                                   << shard_key << " with code " << ret;
                    }
                    return ret;
                });
        },
        [this](const std::string &write_key, uintptr_t write_buffer_ptr,
               size_t write_size, const TensorParallelismSpec &parallelism_spec,
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
        parallelism, writer_partition, "upsert_tensor_with_parallelism_from");
    if (!request.has_value()) {
        return to_py_ret(ErrorCode::INVALID_PARAMS);
    }
    return execute_upsert_tensor_with_parallelism_from_route(
        key, buffer_ptr, size, *request, config);
}

std::vector<int> batch_upsert_tensor_with_parallelism(
    const std::vector<std::string> &keys, const pybind11::list &tensors_list,
    const py::object &parallelisms = py::none(),
    const ReplicateConfig &config = ReplicateConfig{},
    const py::object &writer_partitions = py::none()) {
    return execute_batch_parallelism_write_requests(
        keys, tensors_list.size(), parallelisms, writer_partitions,
        "batch_upsert_tensor_with_parallelism",
        [this, &keys, &tensors_list, &config]() {
            if (!is_client_initialized() || use_dummy_client_) {
                return std::vector<int>(keys.size(),
                                        to_py_ret(ErrorCode::INVALID_PARAMS));
            }
            int validate_result = validate_replicate_config(config);
            if (validate_result) {
                return std::vector<int>(keys.size(), validate_result);
            }
            return batch_upsert_tensor_impl(keys, tensors_list, config);
        },
        [this, &keys, &tensors_list, &config](size_t i,
                                              const py::handle &parallelism) {
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

std::vector<int> batch_upsert_tensor_with_parallelism_from(
    const std::vector<std::string> &keys,
    const std::vector<uintptr_t> &buffer_ptrs, const std::vector<size_t> &sizes,
    const py::object &parallelisms = py::none(),
    const ReplicateConfig &config = ReplicateConfig{},
    const py::object &writer_partitions = py::none()) {
    return execute_batch_parallelism_write_requests(
        keys, buffer_ptrs.size(), parallelisms, writer_partitions,
        "batch_upsert_tensor_with_parallelism_from",
        [this, &keys, &buffer_ptrs, &sizes, &config]() {
            if (!is_default_replicate_config(config)) {
                if (!is_client_initialized()) {
                    LOG(ERROR) << "Client is not initialized";
                    return std::vector<int>(
                        keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                }
                if (use_dummy_client_) {
                    LOG(ERROR) << "batch_upsert_tensor_with_parallelism_from "
                                  "is not supported for dummy client";
                    return std::vector<int>(
                        keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                }
                int validate_result = validate_replicate_config(config);
                if (validate_result) {
                    return std::vector<int>(keys.size(), validate_result);
                }
                if (keys.empty()) {
                    return std::vector<int>();
                }
                if (keys.size() != buffer_ptrs.size() ||
                    keys.size() != sizes.size()) {
                    LOG(ERROR) << "Size mismatch: keys, buffer_ptrs, and sizes "
                                  "must have the same length";
                    return std::vector<int>(
                        keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                }
                for (size_t i = 0; i < sizes.size(); ++i) {
                    if (!is_valid_tensor_object_buffer(
                            buffer_ptrs[i], sizes[i],
                            "tensor object buffer at index " +
                                std::to_string(i))) {
                        return std::vector<int>(
                            keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                    }
                }
                std::vector<void *> buffers;
                buffers.reserve(buffer_ptrs.size());
                for (uintptr_t ptr : buffer_ptrs) {
                    buffers.push_back(reinterpret_cast<void *>(ptr));
                }
                py::gil_scoped_release release_gil;
                return store_->batch_upsert_from(keys, buffers, sizes, config);
            }
            return batch_upsert_tensor_from(keys, buffer_ptrs, sizes);
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
