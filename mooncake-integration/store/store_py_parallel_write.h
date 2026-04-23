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

    int put_direct_parallelism_shard(
        const std::string &key, pybind11::object tensor,
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
                                               write_config, rank, size,
                                               split_dim, axes);
            },
            [this](const std::string &write_key, pybind11::object write_tensor,
                   const TensorParallelismSpec &parallelism_spec,
                   const ReplicateConfig &write_config) {
                return put_direct_parallelism_shard(
                    write_key, write_tensor, parallelism_spec, write_config);
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
        if (!is_valid_tensor_object_buffer(buffer_ptr, size,
                                           "put_tensor_with_tp_from")) {
            return to_py_ret(ErrorCode::INVALID_PARAMS);
        }
        if (tp_size <= 1) {
            return put_tensor_from(key, buffer_ptr, size);
        }

        return execute_tp_tensor_write_from_impl(
            key, buffer_ptr, size, ReplicateConfig{}, tp_size, split_dim, {},
            "put_tensor_with_tp_from",
            [this](const std::string &shard_key,
                   const std::vector<std::span<const char>> &values,
                   const ReplicateConfig &write_config) {
                py::gil_scoped_release release_gil;
                int ret = store_->put_parts(shard_key, values, write_config);
                if (ret != 0) {
                    LOG(ERROR) << "put_parts failed for key " << shard_key
                               << " with code " << ret;
                }
                return ret;
            });
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
                    int validate_result =
                        validate_replicate_config(write_config);
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
                        py::gil_scoped_release release_gil;
                        int ret =
                            store_->put_parts(shard_key, values, parts_config);
                        if (ret != 0) {
                            LOG(ERROR) << "put_parts failed for key "
                                       << shard_key << " with code " << ret;
                        }
                        return ret;
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
            if (!is_valid_tensor_object_buffer(
                    buffer_ptrs[i], sizes[i],
                    "tensor object buffer at index " + std::to_string(i))) {
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
                        LOG(ERROR)
                            << "Size mismatch: keys, buffer_ptrs, and sizes "
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
                                keys.size(),
                                to_py_ret(ErrorCode::INVALID_PARAMS));
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
                return upsert_tensor_with_tp_impl(write_key, write_tensor,
                                                  write_config, size, split_dim);
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
                        LOG(ERROR)
                            << "upsert_tensor_with_parallelism_from is not "
                               "supported for dummy client";
                        return to_py_ret(ErrorCode::INVALID_PARAMS);
                    }
                    int validate_result =
                        validate_replicate_config(write_config);
                    if (validate_result) return validate_result;
                    py::gil_scoped_release release_gil;
                    return store_->upsert_from(write_key, buffer, write_size,
                                               write_config);
                }
                return upsert_tensor_from(write_key, write_buffer_ptr,
                                          write_size);
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
                        py::gil_scoped_release release_gil;
                        int ret = store_->upsert_parts(shard_key, values,
                                                       parts_config);
                        if (ret != 0) {
                            LOG(ERROR) << "upsert_parts failed for key "
                                       << shard_key << " with code " << ret;
                        }
                        return ret;
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
                if (!is_default_replicate_config(config)) {
                    if (!is_client_initialized()) {
                        LOG(ERROR) << "Client is not initialized";
                        return std::vector<int>(
                            keys.size(), to_py_ret(ErrorCode::INVALID_PARAMS));
                    }
                    if (use_dummy_client_) {
                        LOG(ERROR)
                            << "batch_upsert_tensor_with_parallelism_from "
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
                        LOG(ERROR)
                            << "Size mismatch: keys, buffer_ptrs, and sizes "
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
                                keys.size(),
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
                                                     config);
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
