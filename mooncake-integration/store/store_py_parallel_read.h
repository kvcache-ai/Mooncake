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
    return ParseTensorMetadata(static_cast<const char *>(buffer_handle->ptr()),
                               buffer_handle->size());
}

std::optional<ParsedTensorMetadata> parse_tensor_metadata_from_prefix(
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

std::vector<CachedQueryResultResponse> batch_query_for_reuse(
    const std::vector<std::string> &keys) {
    if (auto real_client = get_real_client()) {
        return real_client->batch_get_query_results(keys);
    }

    auto query_results = store_->batch_query(keys);
    std::vector<CachedQueryResultResponse> cached_results;
    cached_results.reserve(query_results.size());
    auto now = std::chrono::steady_clock::now();
    for (const auto &query_result : query_results) {
        cached_results.push_back(
            to_cached_query_result_response(query_result, now));
    }
    return cached_results;
}

std::optional<std::vector<ReconstructedShardSource>>
load_reconstructed_shard_sources_batch(const std::vector<std::string> &keys,
                                       const std::string &context) {
    if (!is_client_initialized()) {
        LOG(ERROR) << context << ": client is not initialized";
        return std::nullopt;
    }
    if (keys.empty()) {
        return std::vector<ReconstructedShardSource>{};
    }

    std::vector<CachedQueryResultResponse> cached_query_results;
    {
        py::gil_scoped_release release_gil;
        cached_query_results = batch_query_for_reuse(keys);
    }
    if (cached_query_results.size() != keys.size()) {
        LOG(ERROR) << context << ": BatchQuery result size mismatch";
        return std::nullopt;
    }

    mooncake::PyClient::QueryResultCache query_result_cache;
    auto now = std::chrono::steady_clock::now();
    std::vector<size_t> metadata_key_indices;
    metadata_key_indices.reserve(keys.size());
    std::vector<std::optional<CachedQueryResultResponse>>
        reusable_query_results(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        auto query_result =
            from_cached_query_result_response(cached_query_results[i], now);
        if (!query_result || query_result->IsLeaseExpired(now)) {
            continue;
        }

        query_result_cache.emplace(keys[i], std::move(query_result));
        reusable_query_results[i] = cached_query_results[i];
        metadata_key_indices.push_back(i);
    }

    std::vector<std::optional<ParsedTensorMetadata>> prefix_metadata(
        keys.size());
    if (!metadata_key_indices.empty()) {
        const size_t scratch_size =
            std::max<size_t>(metadata_key_indices.size(), 1) *
            sizeof(TensorMetadata);
        auto scratch_buffer = std::make_unique<char[]>(scratch_size);
        if (store_->register_buffer(scratch_buffer.get(), scratch_size) != 0) {
            LOG(ERROR) << context
                       << ": failed to register metadata scratch buffer";
            return std::nullopt;
        }

        char *scratch_base = scratch_buffer.get();
        std::vector<void *> metadata_buffers;
        metadata_buffers.reserve(metadata_key_indices.size());
        std::vector<std::vector<std::string>> metadata_all_keys(
            metadata_key_indices.size());
        std::vector<std::vector<std::vector<size_t>>> metadata_all_dst_offsets(
            metadata_key_indices.size());
        std::vector<std::vector<std::vector<size_t>>> metadata_all_src_offsets(
            metadata_key_indices.size(), {{0}});
        std::vector<std::vector<std::vector<size_t>>> metadata_all_sizes(
            metadata_key_indices.size(), {{sizeof(TensorMetadata)}});
        for (size_t i = 0; i < metadata_key_indices.size(); ++i) {
            // Each metadata read is modeled as a separate request, but they all
            // write into non-overlapping offsets in the same registered scratch
            // buffer.
            metadata_buffers.push_back(scratch_base);
            metadata_all_keys[i] = {keys[metadata_key_indices[i]]};
            metadata_all_dst_offsets[i] = {{i * sizeof(TensorMetadata)}};
        }

        std::vector<std::vector<std::vector<int64_t>>> metadata_results;
        {
            py::gil_scoped_release release_gil;
            metadata_results = store_->get_into_ranges(
                metadata_buffers, metadata_all_keys, metadata_all_dst_offsets,
                metadata_all_src_offsets, metadata_all_sizes,
                &query_result_cache);
        }
        for (size_t i = 0;
             i < metadata_results.size() && i < metadata_key_indices.size();
             ++i) {
            const size_t key_index = metadata_key_indices[i];
            if (metadata_results[i].size() == 1 &&
                metadata_results[i][0].size() == 1 &&
                metadata_results[i][0][0] ==
                    static_cast<int64_t>(sizeof(TensorMetadata))) {
                prefix_metadata[key_index] = parse_tensor_metadata_from_prefix(
                    scratch_base + i * sizeof(TensorMetadata), context,
                    keys[key_index]);
            }
        }
        store_->unregister_buffer(scratch_buffer.get());
    }

    std::vector<ReconstructedShardSource> sources;
    sources.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto parsed = std::move(prefix_metadata[i]);
        auto reusable_query_result = reusable_query_results[i];
        if (!parsed.has_value()) {
            parsed = get_tensor_metadata(keys[i]);
            reusable_query_result.reset();
        }
        if (!parsed.has_value()) {
            LOG(ERROR) << context
                       << ": missing reconstructed shard source for key "
                       << keys[i];
            return std::nullopt;
        }
        sources.push_back(
            ReconstructedShardSource{keys[i], *parsed, reusable_query_result});
    }
    return sources;
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

    auto region = resolve_writable_buffer_region(buffer_ptr, size, context);
    if (!region.has_value()) {
        return std::nullopt;
    }
    if (region->offset + total_length > region->size) {
        LOG(ERROR) << context
                   << ": resolved destination range exceeds writable region";
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

std::optional<size_t> extract_reconstruction_element_size(
    const std::vector<ReconstructedShardSource> &sources,
    const std::string &context) {
    for (const auto &source : sources) {
        auto element_size =
            TensorDtypeElementSize(source.metadata.metadata.header.dtype);
        if (!element_size.has_value()) {
            LOG(ERROR) << context << ": invalid shard tensor dtype";
            return std::nullopt;
        }

        const auto expected_data_bytes =
            TensorMetadataExpectedDataBytes(source.metadata.metadata);
        if (!expected_data_bytes.has_value() ||
            source.metadata.data_bytes != *expected_data_bytes) {
            LOG(ERROR) << context << ": invalid shard tensor byte size";
            return std::nullopt;
        }
        return *element_size;
    }
    return size_t{0};
}

std::optional<std::pair<int64_t, int64_t>> get_source_shard_range(
    const ReconstructedShardSource &source,
    const std::vector<int64_t> &global_shape, int split_dim,
    const std::string &context) {
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

    const LayoutAxis *tp_axis =
        find_layout_axis(source.metadata.metadata, LayoutAxisKind::TP);
    if (!is_shard_tensor_metadata(source.metadata.metadata) || !tp_axis) {
        LOG(ERROR) << context << ": missing TP shard metadata for key "
                   << source.read_key;
        return std::nullopt;
    }
    if (tp_axis->split_dim != split_dim || tp_axis->shard_count <= 0 ||
        tp_axis->shard_rank < 0 ||
        tp_axis->shard_rank >= tp_axis->shard_count) {
        LOG(ERROR) << context << ": invalid TP shard metadata for key "
                   << source.read_key;
        return std::nullopt;
    }
    if (!is_uniform_shardable_dim(global_shape[split_dim],
                                  tp_axis->shard_count)) {
        LOG(ERROR) << context << ": only uniform sharding is supported";
        return std::nullopt;
    }

    const auto [shard_start, shard_extent] = calculate_shard_range(
        global_shape[split_dim], tp_axis->shard_rank, tp_axis->shard_count);
    if (local_shape[split_dim] != shard_extent) {
        LOG(ERROR) << context << ": shard extent mismatch for key "
                   << source.read_key;
        return std::nullopt;
    }
    return std::pair<int64_t, int64_t>{shard_start, shard_extent};
}

std::optional<TensorIntoPlan> build_reconstructed_tensor_into_plan_from_sources(
    uintptr_t buffer_ptr, size_t size,
    std::vector<ReconstructedShardSource> sources,
    const std::vector<int64_t> &global_shape, int split_dim,
    const TensorMetadata &target_metadata, int64_t target_start,
    int64_t target_extent, const std::string &context,
    bool allow_empty_fragments = false) {
    if (sources.empty()) {
        LOG(ERROR) << context << ": missing reconstruction shard sources";
        return std::nullopt;
    }
    if (split_dim < 0 || split_dim >= static_cast<int>(global_shape.size())) {
        LOG(ERROR) << context << ": invalid split_dim";
        return std::nullopt;
    }
    if (target_start < 0 || target_extent < 0 ||
        target_start + target_extent > global_shape[split_dim]) {
        LOG(ERROR) << context << ": invalid target shard range";
        return std::nullopt;
    }

    auto region = resolve_writable_buffer_region(buffer_ptr, size, context);
    if (!region.has_value()) {
        return std::nullopt;
    }

    auto element_size = extract_reconstruction_element_size(sources, context);
    if (!element_size.has_value()) {
        return std::nullopt;
    }

    size_t target_tensor_numel = 1;
    for (size_t dim = 0; dim < global_shape.size(); ++dim) {
        const int64_t dim_extent = static_cast<int>(dim) == split_dim
                                       ? target_extent
                                       : global_shape[dim];
        target_tensor_numel *= static_cast<size_t>(dim_extent);
    }
    const size_t target_tensor_bytes = target_tensor_numel * *element_size;
    const size_t total_length = sizeof(TensorMetadata) + target_tensor_bytes;

    if (total_length > size || region->offset + total_length > region->size) {
        LOG(ERROR) << context << ": buffer too small for reconstructed tensor";
        return std::nullopt;
    }

    TensorIntoPlan plan;
    plan.user_buffer_ptr = buffer_ptr;
    plan.registered_buffer_ptr = reinterpret_cast<uintptr_t>(region->base);
    plan.registered_buffer_size = region->size;
    plan.total_length = total_length;
    plan.materialized_metadata = target_metadata;
    plan.materialized_metadata->header.data_bytes = target_tensor_bytes;

    int64_t elements_before = 1;
    for (int i = 0; i < split_dim; ++i) {
        elements_before *= global_shape[i];
    }
    int64_t elements_after = 1;
    for (size_t i = split_dim + 1; i < global_shape.size(); ++i) {
        elements_after *= global_shape[i];
    }

    std::vector<bool> covered(
        static_cast<size_t>(target_extent > 0 ? target_extent : 0), false);
    for (auto &source : sources) {
        auto source_range =
            get_source_shard_range(source, global_shape, split_dim, context);
        if (!source_range.has_value()) {
            return std::nullopt;
        }
        const auto [source_start, source_extent] = *source_range;
        const int64_t overlap_start = std::max(source_start, target_start);
        const int64_t overlap_end = std::min(source_start + source_extent,
                                             target_start + target_extent);
        if (overlap_end <= overlap_start) {
            continue;
        }
        const int64_t overlap_extent = overlap_end - overlap_start;
        const int64_t src_inner_offset = overlap_start - source_start;
        const int64_t dst_inner_offset = overlap_start - target_start;
        const size_t row_bytes = static_cast<size_t>(overlap_extent) *
                                 static_cast<size_t>(elements_after) *
                                 *element_size;
        for (int64_t idx = dst_inner_offset;
             idx < dst_inner_offset + overlap_extent; ++idx) {
            covered[static_cast<size_t>(idx)] = true;
        }

        for (int64_t slice_idx = 0; slice_idx < elements_before; ++slice_idx) {
            const size_t dst_offset =
                region->offset + sizeof(TensorMetadata) +
                static_cast<size_t>(slice_idx * target_extent +
                                    dst_inner_offset) *
                    static_cast<size_t>(elements_after) * *element_size;
            const size_t src_offset =
                source.metadata.data_offset +
                static_cast<size_t>(slice_idx * source_extent +
                                    src_inner_offset) *
                    static_cast<size_t>(elements_after) * *element_size;
            plan.fragments.push_back(TensorIntoFragment{
                .read_key = source.read_key,
                .dst_offset = dst_offset,
                .src_offset = src_offset,
                .size = row_bytes,
            });
        }
    }

    for (bool is_covered : covered) {
        if (!is_covered) {
            LOG(ERROR)
                << context
                << ": shard extents do not cover reconstructed dimension";
            return std::nullopt;
        }
    }
    if (plan.fragments.empty() && target_tensor_bytes != 0) {
        LOG(ERROR) << context << ": no fragments planned for reconstruction";
        return std::nullopt;
    }

    plan.query_results.reserve(sources.size());
    for (auto &source : sources) {
        if (source.cached_query_result.has_value()) {
            plan.query_results.push_back(PlannedQueryResult{
                source.read_key, std::move(source.cached_query_result)});
        }
    }
    return plan;
}

std::optional<TensorIntoPlan> build_full_tensor_into_plan_from_sources(
    uintptr_t buffer_ptr, size_t size,
    std::vector<ReconstructedShardSource> sources,
    const std::vector<int64_t> &global_shape, int split_dim, int32_t dtype,
    const std::string &context, bool allow_empty_fragments = false) {
    TensorMetadata full_metadata = BuildTensorMetadata(
        dtype, global_shape, global_shape, TensorLayoutKind::FULL);
    return build_reconstructed_tensor_into_plan_from_sources(
        buffer_ptr, size, std::move(sources), global_shape, split_dim,
        full_metadata, 0, global_shape[split_dim], context,
        allow_empty_fragments);
}

std::string resolve_tp_read_key(const std::string &key, int tp_rank,
                                int tp_size) const {
    if (tp_size <= 1) return key;
    return get_tp_key_name(key, tp_rank);
}

pybind11::object get_tensor_with_writer_shard_full(const std::string &key,
                                                   const std::string &context) {
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

    auto element_size =
        extract_reconstruction_element_size(reconstruction->sources, context);
    if (!element_size.has_value()) {
        return py::none();
    }

    const size_t total_length =
        sizeof(TensorMetadata) + total_tensor_numel * *element_size;
    char *owned_buffer = new char[total_length];
    if (store_->register_buffer(owned_buffer, total_length) != 0) {
        LOG(ERROR) << context << ": failed to register reconstruction buffer";
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

    std::vector<TensorIntoPlan> plans;
    plans.push_back(std::move(*plan));
    auto success = execute_tensor_into_plan_transfers(plans);
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

pybind11::object get_tensor_with_tp_full(const std::string &key, int tp_rank,
                                         int tp_size, int split_dim,
                                         const std::string &context) {
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
        load_tp_full_reconstruction_sources(key, axis, context);
    if (!reconstruction.has_value()) {
        return pybind11::none();
    }

    const size_t total_tensor_numel =
        std::accumulate(reconstruction->global_shape.begin(),
                        reconstruction->global_shape.end(),
                        static_cast<size_t>(1), std::multiplies<size_t>());
    auto element_size =
        extract_reconstruction_element_size(reconstruction->sources, context);
    if (!element_size.has_value()) {
        return pybind11::none();
    }
    const size_t total_length =
        sizeof(TensorMetadata) + total_tensor_numel * *element_size;

    char *owned_buffer = new char[total_length];
    if (store_->register_buffer(owned_buffer, total_length) != 0) {
        LOG(ERROR) << context << ": failed to register reconstruction buffer";
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

    std::vector<TensorIntoPlan> plans;
    plans.push_back(std::move(*plan));
    auto success = execute_tensor_into_plan_transfers(plans);
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

std::optional<TensorIntoPlan> build_tp_full_tensor_into_plan(
    const std::string &key, uintptr_t buffer_ptr, size_t size,
    const ParallelAxisSpec &axis, const std::string &context,
    const std::optional<TensorParallelismSpec> &parallelism = std::nullopt) {
    auto reconstruction =
        load_tp_full_reconstruction_sources(key, axis, context, parallelism);
    if (!reconstruction.has_value()) {
        return std::nullopt;
    }
    return build_full_tensor_into_plan_from_sources(
        buffer_ptr, size, reconstruction->sources, reconstruction->global_shape,
        reconstruction->split_dim, reconstruction->dtype, context,
        reconstruction->allow_empty_fragments);
}

std::optional<FullTensorReconstructionSources>
load_tp_full_reconstruction_sources(
    const std::string &key, const ParallelAxisSpec &axis,
    const std::string &context,
    const std::optional<TensorParallelismSpec> &parallelism = std::nullopt) {
    if (axis.size <= 0) {
        LOG(ERROR) << context << ": tp_size must be positive";
        return std::nullopt;
    }

    std::vector<std::string> read_keys;
    read_keys.reserve(axis.size);
    for (int shard_rank = 0; shard_rank < axis.size; ++shard_rank) {
        if (parallelism.has_value()) {
            auto shard_parallelism = *parallelism;
            auto tp_axis_index = find_tp_axis_index(shard_parallelism.axes);
            if (!tp_axis_index.has_value()) {
                LOG(ERROR) << context
                           << ": missing TP axis in full reconstruction";
                return std::nullopt;
            }
            shard_parallelism.axes[*tp_axis_index].rank = shard_rank;
            read_keys.push_back(
                get_parallelism_key_name(key, shard_parallelism));
        } else {
            read_keys.push_back(
                resolve_tp_read_key(key, shard_rank, axis.size));
        }
    }

    auto ordered_sources =
        load_reconstructed_shard_sources_batch(read_keys, context);
    if (!ordered_sources.has_value()) {
        return std::nullopt;
    }

    FullTensorReconstructionSources reconstruction;
    reconstruction.sources.reserve(axis.size);
    for (int shard_rank = 0; shard_rank < axis.size; ++shard_rank) {
        auto &source = (*ordered_sources)[shard_rank];
        const auto &metadata = source.metadata;
        const LayoutAxis *tp_axis =
            find_layout_axis(metadata.metadata, LayoutAxisKind::TP);
        if (!is_shard_tensor_metadata(metadata.metadata) || !tp_axis ||
            tp_axis->shard_rank != shard_rank ||
            tp_axis->shard_count != axis.size) {
            LOG(ERROR) << context << ": TP metadata mismatch for key "
                       << source.read_key;
            return std::nullopt;
        }
        if (parallelism.has_value()) {
            auto stored_parallelism =
                resolve_tp_compatible_parallelism_from_metadata(
                    *parallelism, metadata.metadata, context);
            if (!stored_parallelism.has_value()) {
                return std::nullopt;
            }
        }
        reconstruction.sources.push_back(std::move(source));
    }

    reconstruction.global_shape = TensorShapeToVector(
        reconstruction.sources.front().metadata.metadata.layout.global_shape,
        reconstruction.sources.front().metadata.metadata.header.ndim);
    const LayoutAxis *stored_tp_axis = find_layout_axis(
        reconstruction.sources.front().metadata.metadata, LayoutAxisKind::TP);
    if (!stored_tp_axis) {
        LOG(ERROR) << context << ": missing TP axis metadata";
        return std::nullopt;
    }
    reconstruction.split_dim = stored_tp_axis->split_dim;
    if (!is_uniform_shardable_dim(
            reconstruction.global_shape[reconstruction.split_dim],
            stored_tp_axis->shard_count)) {
        LOG(ERROR) << context << ": only uniform sharding is supported";
        return std::nullopt;
    }
    if (axis.split_dim.has_value() &&
        axis.split_dim.value() != reconstruction.split_dim) {
        LOG(ERROR) << context << ": split_dim mismatch";
        return std::nullopt;
    }
    reconstruction.dtype =
        reconstruction.sources.front().metadata.metadata.header.dtype;
    return reconstruction;
}

std::optional<FullTensorReconstructionSources> load_writer_shard_reconstruction(
    const std::string &key, const std::string &context) {
    if (!is_client_initialized()) {
        LOG(ERROR) << context << ": client is not initialized";
        return std::nullopt;
    }

    std::shared_ptr<BufferHandle> manifest_handle;
    {
        py::gil_scoped_release release_gil;
        manifest_handle = store_->get_buffer(get_writer_manifest_key_name(key));
    }
    auto parsed_manifest = parse_writer_shard_manifest(manifest_handle.get());
    if (!parsed_manifest.has_value()) {
        return std::nullopt;
    }

    const auto &manifest = *parsed_manifest;
    const auto &global_shape = manifest.global_shape;
    const int split_dim = manifest.manifest.header.split_dim;
    const int shard_count = manifest.manifest.header.shard_count;
    if (split_dim < 0 || split_dim >= static_cast<int>(global_shape.size())) {
        LOG(ERROR) << context << ": invalid writer split_dim";
        return std::nullopt;
    }

    std::vector<std::string> shard_keys;
    shard_keys.reserve(shard_count);
    for (int shard_rank = 0; shard_rank < shard_count; ++shard_rank) {
        WriterPartitionSpec writer{
            .rank = shard_rank,
            .size = shard_count,
            .split_dim = split_dim,
        };
        shard_keys.push_back(get_writer_shard_key_name(key, writer));
    }

    auto ordered_sources =
        load_reconstructed_shard_sources_batch(shard_keys, context);
    if (!ordered_sources.has_value()) {
        return std::nullopt;
    }

    FullTensorReconstructionSources reconstruction;
    reconstruction.sources.reserve(shard_count);
    for (int shard_rank = 0; shard_rank < shard_count; ++shard_rank) {
        auto &source = (*ordered_sources)[shard_rank];
        auto writer_parallelism = writer_partition_parallelism_from_metadata(
            source.metadata.metadata);
        if (!writer_parallelism.has_value() ||
            writer_parallelism->axes[0].rank != shard_rank ||
            writer_parallelism->axes[0].size != shard_count ||
            writer_parallelism->axes[0].split_dim != split_dim) {
            LOG(ERROR) << context << ": writer shard metadata mismatch for key "
                       << source.read_key;
            return std::nullopt;
        }
        reconstruction.sources.push_back(std::move(source));
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
        buffer_ptr, size, reconstruction->sources, reconstruction->global_shape,
        reconstruction->split_dim, reconstruction->dtype, context,
        reconstruction->allow_empty_fragments);
}

std::vector<bool> execute_tensor_into_plan_transfers(
    std::vector<TensorIntoPlan> &plans) {
    std::vector<bool> success(plans.size(), false);
    if (plans.empty()) {
        return success;
    }

    std::vector<void *> buffers;
    std::vector<std::vector<std::string>> all_keys;
    std::vector<std::vector<std::vector<size_t>>> all_dst_offsets;
    std::vector<std::vector<std::vector<size_t>>> all_src_offsets;
    std::vector<std::vector<std::vector<size_t>>> all_sizes;
    std::vector<size_t> transfer_plan_indices;
    buffers.reserve(plans.size());
    all_keys.reserve(plans.size());
    all_dst_offsets.reserve(plans.size());
    all_src_offsets.reserve(plans.size());
    all_sizes.reserve(plans.size());
    transfer_plan_indices.reserve(plans.size());

    for (size_t plan_idx = 0; plan_idx < plans.size(); ++plan_idx) {
        const auto &plan = plans[plan_idx];
        std::unordered_map<std::string, size_t> key_to_index;
        std::vector<std::string> keys;
        std::vector<std::vector<size_t>> dst_offsets;
        std::vector<std::vector<size_t>> src_offsets;
        std::vector<std::vector<size_t>> sizes;
        key_to_index.reserve(plan.fragments.size());
        keys.reserve(plan.fragments.size());
        dst_offsets.reserve(plan.fragments.size());
        src_offsets.reserve(plan.fragments.size());
        sizes.reserve(plan.fragments.size());

        for (const auto &fragment : plan.fragments) {
            if (fragment.read_key.empty() || fragment.size == 0) {
                continue;
            }
            auto [it, inserted] =
                key_to_index.emplace(fragment.read_key, keys.size());
            if (inserted) {
                keys.push_back(fragment.read_key);
                dst_offsets.emplace_back();
                src_offsets.emplace_back();
                sizes.emplace_back();
            }
            const size_t key_index = it->second;
            dst_offsets[key_index].push_back(fragment.dst_offset);
            src_offsets[key_index].push_back(fragment.src_offset);
            sizes[key_index].push_back(fragment.size);
        }

        if (keys.empty()) {
            if (plan.materialized_metadata.has_value()) {
                std::memcpy(reinterpret_cast<void *>(plan.user_buffer_ptr),
                            &*plan.materialized_metadata,
                            sizeof(TensorMetadata));
                success[plan_idx] = true;
            }
            continue;
        }

        buffers.push_back(reinterpret_cast<void *>(plan.registered_buffer_ptr));
        transfer_plan_indices.push_back(plan_idx);
        all_keys.push_back(std::move(keys));
        all_dst_offsets.push_back(std::move(dst_offsets));
        all_src_offsets.push_back(std::move(src_offsets));
        all_sizes.push_back(std::move(sizes));
    }

    if (transfer_plan_indices.empty()) {
        return success;
    }

    std::vector<std::vector<std::vector<int64_t>>> range_results;
    {
        py::gil_scoped_release release_gil;
        mooncake::PyClient::QueryResultCache query_result_cache;
        auto now = std::chrono::steady_clock::now();
        for (auto &plan : plans) {
            for (auto &planned_query_result : plan.query_results) {
                if (!planned_query_result.cached_query_result.has_value()) {
                    continue;
                }
                auto query_result = from_cached_query_result_response(
                    *planned_query_result.cached_query_result, now);
                if (query_result && !query_result->IsLeaseExpired(now)) {
                    query_result_cache.emplace(planned_query_result.read_key,
                                               std::move(query_result));
                }
            }
        }
        range_results = store_->get_into_ranges(
            buffers, all_keys, all_dst_offsets, all_src_offsets, all_sizes,
            query_result_cache.empty() ? nullptr : &query_result_cache);
    }

    for (size_t i = 0; i < transfer_plan_indices.size(); ++i) {
        const size_t plan_idx = transfer_plan_indices[i];
        if (i >= range_results.size() ||
            range_results[i].size() != all_sizes[i].size()) {
            continue;
        }
        success[plan_idx] = true;
        for (size_t key_idx = 0;
             key_idx < all_sizes[i].size() && success[plan_idx]; ++key_idx) {
            if (range_results[i][key_idx].size() !=
                all_sizes[i][key_idx].size()) {
                success[plan_idx] = false;
                break;
            }
            for (size_t fragment_idx = 0;
                 fragment_idx < all_sizes[i][key_idx].size(); ++fragment_idx) {
                if (range_results[i][key_idx][fragment_idx] !=
                    static_cast<int64_t>(all_sizes[i][key_idx][fragment_idx])) {
                    success[plan_idx] = false;
                    break;
                }
            }
        }
        if (success[plan_idx] &&
            plans[plan_idx].materialized_metadata.has_value()) {
            std::memcpy(
                reinterpret_cast<void *>(plans[plan_idx].user_buffer_ptr),
                &*plans[plan_idx].materialized_metadata,
                sizeof(TensorMetadata));
        }
    }
    return success;
}
