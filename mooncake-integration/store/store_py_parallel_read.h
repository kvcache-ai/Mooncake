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

    auto region = resolve_registered_buffer_region(buffer_ptr, size, context);
    if (!region.has_value()) {
        return std::nullopt;
    }
    if (region->offset + total_length > region->size) {
        LOG(ERROR) << context
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

std::optional<size_t> extract_reconstruction_element_size(
    const std::vector<ReconstructedShardSource> &sources,
    const std::string &context) {
    for (const auto &source : sources) {
        const auto local_shape =
            TensorShapeToVector(source.metadata.metadata.layout.local_shape,
                                source.metadata.metadata.header.ndim);
        int64_t shard_numel = 1;
        for (auto dim : local_shape) {
            shard_numel *= dim;
        }
        if (shard_numel < 0) {
            LOG(ERROR) << context << ": invalid shard tensor numel";
            return std::nullopt;
        }
        if (shard_numel == 0) {
            if (source.metadata.data_bytes != 0) {
                LOG(ERROR) << context << ": invalid empty shard byte size";
                return std::nullopt;
            }
            continue;
        }
        if (source.metadata.data_bytes % static_cast<size_t>(shard_numel) !=
            0) {
            LOG(ERROR) << context << ": invalid shard tensor byte size";
            return std::nullopt;
        }
        return source.metadata.data_bytes / static_cast<size_t>(shard_numel);
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
    const std::vector<ReconstructedShardSource> &sources,
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

    auto region = resolve_registered_buffer_region(buffer_ptr, size, context);
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

    TensorMetadata materialized_metadata = target_metadata;
    materialized_metadata.header.data_bytes = target_tensor_bytes;

    TensorIntoPlan plan;
    plan.user_buffer_ptr = buffer_ptr;
    plan.registered_buffer_ptr = reinterpret_cast<uintptr_t>(region->base);
    plan.registered_buffer_size = region->size;
    plan.total_length = total_length;
    plan.materialized_metadata = materialized_metadata;

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
    for (const auto &source : sources) {
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
    if (plan.fragments.empty() &&
        !(allow_empty_fragments && target_tensor_bytes == 0)) {
        LOG(ERROR) << context << ": no fragments planned for reconstruction";
        return std::nullopt;
    }
    return plan;
}

std::optional<TensorIntoPlan> build_full_tensor_into_plan_from_sources(
    uintptr_t buffer_ptr, size_t size,
    const std::vector<ReconstructedShardSource> &sources,
    const std::vector<int64_t> &global_shape, int split_dim, int32_t dtype,
    const std::string &context, bool allow_empty_fragments = false) {
    TensorMetadata full_metadata = BuildTensorMetadata(
        dtype, global_shape, global_shape, TensorLayoutKind::FULL);
    return build_reconstructed_tensor_into_plan_from_sources(
        buffer_ptr, size, sources, global_shape, split_dim, full_metadata, 0,
        global_shape[split_dim], context, allow_empty_fragments);
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
            LOG(ERROR) << context << ": invalid writer shard tensor byte size";
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
    const std::optional<TensorParallelismSpec> &parallelism = std::nullopt) {
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
            ? load_parallelism_full_reconstruction_sources(key, *parallelism,
                                                           context)
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

    FullTensorReconstructionSources reconstruction;
    reconstruction.sources.reserve(shard_count);
    for (int shard_rank = 0; shard_rank < shard_count; ++shard_rank) {
        WriterPartitionSpec writer{
            .rank = shard_rank,
            .size = shard_count,
            .split_dim = split_dim,
        };
        const std::string shard_key = get_writer_shard_key_name(key, writer);
        auto metadata = get_tensor_metadata(shard_key);
        if (!metadata.has_value()) {
            LOG(ERROR) << context << ": missing writer shard key " << shard_key;
            return std::nullopt;
        }
        auto writer_parallelism =
            writer_partition_parallelism_from_metadata(metadata->metadata);
        if (!writer_parallelism.has_value() ||
            writer_parallelism->axes[0].rank != shard_rank ||
            writer_parallelism->axes[0].size != shard_count ||
            writer_parallelism->axes[0].split_dim != split_dim) {
            LOG(ERROR) << context << ": writer shard metadata mismatch for key "
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
        buffer_ptr, size, reconstruction->sources, reconstruction->global_shape,
        reconstruction->split_dim, reconstruction->dtype, context,
        reconstruction->allow_empty_fragments);
}

std::optional<FullTensorReconstructionSources>
load_parallelism_manifest_reconstruction(
    const std::string &key, const TensorParallelismSpec &parallelism,
    const std::string &context) {
    const ParallelAxisSpec *request_tp_axis =
        find_axis_spec_by_kind(parallelism, LayoutAxisKind::TP);
    if (!request_tp_axis) {
        LOG(ERROR) << context << ": reconstruction requires a TP axis";
        return std::nullopt;
    }

    std::shared_ptr<BufferHandle> manifest_handle;
    {
        py::gil_scoped_release release_gil;
        manifest_handle =
            store_->get_buffer(get_parallelism_manifest_key_name(key));
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
        LOG(ERROR) << context << ": invalid parallelism split_dim";
        return std::nullopt;
    }
    if (request_tp_axis->split_dim.has_value() &&
        request_tp_axis->split_dim.value() != split_dim) {
        LOG(ERROR) << context << ": split_dim mismatch";
        return std::nullopt;
    }
    if (!is_uniform_shardable_dim(global_shape[split_dim],
                                  request_tp_axis->size) ||
        !is_uniform_shardable_dim(global_shape[split_dim], shard_count)) {
        LOG(ERROR) << context << ": only uniform sharding is supported";
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
    reconstruction.global_shape = global_shape;
    reconstruction.split_dim = split_dim;
    reconstruction.dtype = manifest.manifest.header.dtype;
    reconstruction.sources.reserve(shard_count);
    for (int shard_rank = 0; shard_rank < shard_count; ++shard_rank) {
        auto shard_parallelism = *canonical_parallelism;
        shard_parallelism.axes[*tp_axis_index].rank = shard_rank;
        shard_parallelism.axes[*tp_axis_index].size = shard_count;
        const std::string shard_key =
            get_parallelism_key_name(key, shard_parallelism);
        auto metadata = get_tensor_metadata(shard_key);
        if (!metadata.has_value()) {
            LOG(ERROR) << context
                       << ": no shard matched stored layout for TP rank "
                       << shard_rank;
            return std::nullopt;
        }
        auto stored_parallelism =
            resolve_tp_compatible_parallelism_from_metadata(
                shard_parallelism, metadata->metadata, context);
        if (!stored_parallelism.has_value() ||
            !parallelism_specs_equal_by_kind(
                shard_parallelism, *stored_parallelism,
                true /* allow_tp_rank_mismatch */)) {
            LOG(ERROR) << context << ": shard metadata mismatch for TP rank "
                       << shard_rank;
            return std::nullopt;
        }
        reconstruction.sources.push_back(
            ReconstructedShardSource{shard_key, *metadata});
    }
    return reconstruction;
}

std::optional<FullTensorReconstructionSources>
load_parallelism_full_reconstruction_sources(
    const std::string &key, const TensorParallelismSpec &parallelism,
    const std::string &context) {
    if (auto manifest_reconstruction =
            load_parallelism_manifest_reconstruction(key, parallelism, context);
        manifest_reconstruction.has_value()) {
        return manifest_reconstruction;
    }

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
    if (uses_legacy_tp_storage_key(*canonical_parallelism)) {
        const std::string first_key =
            resolve_tp_read_key(key, 0, request_tp_axis->size);
        auto first_metadata = get_tensor_metadata(first_key);
        if (!first_metadata.has_value()) {
            LOG(ERROR) << context
                       << ": no shard matched requested layout for TP rank 0";
            return std::nullopt;
        }
        const LayoutAxis *stored_tp_axis =
            find_layout_axis(first_metadata->metadata, LayoutAxisKind::TP);
        if (!is_shard_tensor_metadata(first_metadata->metadata) ||
            !stored_tp_axis) {
            LOG(ERROR) << context << ": missing TP axis metadata";
            return std::nullopt;
        }
        reconstruction.split_dim = stored_tp_axis->split_dim;
        if (request_tp_axis->split_dim.has_value() &&
            request_tp_axis->split_dim.value() != reconstruction.split_dim) {
            LOG(ERROR) << context << ": split_dim mismatch";
            return std::nullopt;
        }
        reconstruction.global_shape =
            TensorShapeToVector(first_metadata->metadata.layout.global_shape,
                                first_metadata->metadata.header.ndim);
        if (!is_uniform_shardable_dim(
                reconstruction.global_shape[reconstruction.split_dim],
                request_tp_axis->size) ||
            !is_uniform_shardable_dim(
                reconstruction.global_shape[reconstruction.split_dim],
                stored_tp_axis->shard_count)) {
            LOG(ERROR) << context << ": only uniform sharding is supported";
            return std::nullopt;
        }
        reconstruction.dtype = first_metadata->metadata.header.dtype;
        reconstruction.sources.reserve(stored_tp_axis->shard_count);
        for (int shard_rank = 0; shard_rank < stored_tp_axis->shard_count;
             ++shard_rank) {
            const std::string shard_key = resolve_tp_read_key(
                key, shard_rank, stored_tp_axis->shard_count);
            auto metadata = get_tensor_metadata(shard_key);
            if (!metadata.has_value()) {
                LOG(ERROR) << context
                           << ": no shard matched stored layout for TP rank "
                           << shard_rank;
                return std::nullopt;
            }
            const LayoutAxis *source_tp_axis =
                find_layout_axis(metadata->metadata, LayoutAxisKind::TP);
            if (!is_shard_tensor_metadata(metadata->metadata) ||
                !source_tp_axis || source_tp_axis->shard_rank != shard_rank ||
                source_tp_axis->shard_count != stored_tp_axis->shard_count ||
                source_tp_axis->split_dim != reconstruction.split_dim) {
                LOG(ERROR) << context
                           << ": shard metadata mismatch for TP rank "
                           << shard_rank;
                return std::nullopt;
            }
            reconstruction.sources.push_back(
                ReconstructedShardSource{shard_key, *metadata});
        }
        return reconstruction;
    }

    reconstruction.sources.reserve(request_tp_axis->size);
    for (int shard_rank = 0; shard_rank < request_tp_axis->size; ++shard_rank) {
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
            LOG(ERROR) << context << ": shard metadata mismatch for TP rank "
                       << shard_rank;
            return std::nullopt;
        }
        reconstruction.sources.push_back(
            ReconstructedShardSource{shard_key, *metadata});
    }

    const LayoutAxis *stored_tp_axis = find_layout_axis(
        reconstruction.sources.front().metadata.metadata, LayoutAxisKind::TP);
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
        reconstruction.sources.front().metadata.metadata.layout.global_shape,
        reconstruction.sources.front().metadata.metadata.header.ndim);
    if (!is_uniform_shardable_dim(
            reconstruction.global_shape[reconstruction.split_dim],
            request_tp_axis->size)) {
        LOG(ERROR) << context << ": only uniform sharding is supported";
        return std::nullopt;
    }
    reconstruction.dtype =
        reconstruction.sources.front().metadata.metadata.header.dtype;
    return reconstruction;
}

std::optional<TensorIntoPlan> build_parallelism_full_tensor_into_plan(
    const std::string &key, uintptr_t buffer_ptr, size_t size,
    const TensorParallelismSpec &parallelism, const std::string &context) {
    auto reconstruction =
        load_parallelism_full_reconstruction_sources(key, parallelism, context);
    if (!reconstruction.has_value()) {
        return std::nullopt;
    }
    return build_full_tensor_into_plan_from_sources(
        buffer_ptr, size, reconstruction->sources, reconstruction->global_shape,
        reconstruction->split_dim, reconstruction->dtype, context,
        reconstruction->allow_empty_fragments);
}

std::optional<TensorIntoPlan> build_parallelism_shard_tensor_into_plan(
    const std::string &key, uintptr_t buffer_ptr, size_t size,
    const TensorParallelismSpec &parallelism, const std::string &context) {
    auto reconstruction =
        load_parallelism_full_reconstruction_sources(key, parallelism, context);
    if (!reconstruction.has_value()) {
        return std::nullopt;
    }

    const ParallelAxisSpec *tp_axis =
        find_axis_spec_by_kind(parallelism, LayoutAxisKind::TP);
    if (!tp_axis) {
        LOG(ERROR) << context << ": shard reconstruction requires a TP axis";
        return std::nullopt;
    }
    const auto [target_start, target_extent] = calculate_shard_range(
        reconstruction->global_shape[reconstruction->split_dim], tp_axis->rank,
        tp_axis->size);
    std::vector<int64_t> target_shape = reconstruction->global_shape;
    target_shape[reconstruction->split_dim] = target_extent;
    auto target_metadata = build_shard_metadata_from_shapes(
        reconstruction->dtype, reconstruction->global_shape, target_shape,
        parallelism.axes, 0);
    if (!target_metadata.has_value()) {
        LOG(ERROR) << context << ": failed to build target shard metadata";
        return std::nullopt;
    }
    return build_reconstructed_tensor_into_plan_from_sources(
        buffer_ptr, size, reconstruction->sources, reconstruction->global_shape,
        reconstruction->split_dim, *target_metadata, target_start,
        target_extent, context, reconstruction->allow_empty_fragments);
}

pybind11::object get_tensor_with_parallelism_shard_full_materialized(
    const std::string &key, const TensorParallelismSpec &parallelism,
    const std::string &context) {
    auto reconstruction =
        load_parallelism_full_reconstruction_sources(key, parallelism, context);
    if (!reconstruction.has_value()) {
        return py::none();
    }

    const ParallelAxisSpec *tp_axis =
        find_axis_spec_by_kind(parallelism, LayoutAxisKind::TP);
    if (!tp_axis) {
        LOG(ERROR) << context << ": shard reconstruction requires a TP axis";
        return py::none();
    }
    const auto [target_start, target_extent] = calculate_shard_range(
        reconstruction->global_shape[reconstruction->split_dim], tp_axis->rank,
        tp_axis->size);

    auto element_size =
        extract_reconstruction_element_size(reconstruction->sources, context);
    if (!element_size.has_value()) {
        return py::none();
    }

    size_t target_tensor_numel = 1;
    for (size_t dim = 0; dim < reconstruction->global_shape.size(); ++dim) {
        const int64_t dim_extent =
            static_cast<int>(dim) == reconstruction->split_dim
                ? target_extent
                : reconstruction->global_shape[dim];
        target_tensor_numel *= static_cast<size_t>(dim_extent);
    }
    const size_t total_length =
        sizeof(TensorMetadata) + target_tensor_numel * *element_size;

    char *owned_buffer = new char[total_length];
    if (store_->register_buffer(owned_buffer, total_length) != 0) {
        LOG(ERROR) << context << ": failed to register reconstruction buffer";
        delete[] owned_buffer;
        return py::none();
    }

    auto plan = build_parallelism_shard_tensor_into_plan(
        key, reinterpret_cast<uintptr_t>(owned_buffer), total_length,
        parallelism, context);
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
        buffers.push_back(reinterpret_cast<void *>(plan.registered_buffer_ptr));

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
        for (size_t key_idx = 0; key_idx < all_sizes[i].size() && success[i];
             ++key_idx) {
            if (range_results[i][key_idx].size() !=
                all_sizes[i][key_idx].size()) {
                success[i] = false;
                break;
            }
            for (size_t frag_idx = 0; frag_idx < all_sizes[i][key_idx].size();
                 ++frag_idx) {
                if (range_results[i][key_idx][frag_idx] !=
                    static_cast<int64_t>(all_sizes[i][key_idx][frag_idx])) {
                    success[i] = false;
                    break;
                }
            }
        }
        if (!success[i]) {
            continue;
        }
        if (plans[i].materialized_metadata.has_value()) {
            std::memcpy(reinterpret_cast<void *>(plans[i].user_buffer_ptr),
                        &*plans[i].materialized_metadata,
                        sizeof(TensorMetadata));
        }
    }
    return success;
}

py::list execute_tensor_into_plans(const std::vector<TensorIntoPlan> &plans) {
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
        return build_writer_shard_full_tensor_into_plan(key, buffer_ptr, size,
                                                        context);
    }

    auto parallelism =
        validate_parallelism_spec(parsed_target->parallelism, context, false);
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
        if (resolved.has_value()) {
            return build_tensor_into_plan(resolved->read_key, buffer_ptr, size,
                                          context, resolved->metadata);
        }
        if (find_axis_spec_by_kind(*parallelism, LayoutAxisKind::TP)) {
            return build_parallelism_shard_tensor_into_plan(
                key, buffer_ptr, size, *parallelism, context);
        }
        LOG(ERROR) << context << ": parallelism metadata mismatch for key "
                   << key;
        return std::nullopt;
    }

    if (parsed_target->mode == ReadTargetMode::FULL) {
        return build_parallelism_full_tensor_into_plan(key, buffer_ptr, size,
                                                       *parallelism, context);
    }

    LOG(ERROR) << context << ": unsupported ReadTarget mode";
    return std::nullopt;
}

pybind11::object get_tensor_with_parallelism(
    const std::string &key, const py::object &target = py::none()) {
    auto parsed_target = parse_read_target_spec(target);
    if (!parsed_target.has_value()) {
        return py::none();
    }
    if (parsed_target->mode == ReadTargetMode::AS_STORED &&
        !parsed_target->parallelism.has_value()) {
        return get_tensor(key);
    }
    if (parsed_target->mode == ReadTargetMode::FULL &&
        !parsed_target->parallelism.has_value()) {
        return get_tensor_with_writer_shard_full(key,
                                                 "get_tensor_with_parallelism");
    }

    auto parallelism = validate_parallelism_spec(
        parsed_target->parallelism, "get_tensor_with_parallelism", false);
    if (!parallelism.has_value()) {
        return py::none();
    }

    if (parsed_target->mode == ReadTargetMode::SHARD) {
        auto resolved = resolve_parallelism_shard_read(
            [this](const std::string &read_key,
                   std::shared_ptr<BufferHandle> *buffer_handle_out) {
                return get_tensor_metadata(read_key, buffer_handle_out);
            },
            key, *parallelism);
        if (resolved.has_value()) {
            return buffer_to_tensor(resolved->buffer_handle.get(), nullptr, 0);
        }
        if (find_axis_spec_by_kind(*parallelism, LayoutAxisKind::TP)) {
            auto reconstructed =
                get_tensor_with_parallelism_shard_full_materialized(
                    key, *parallelism, "get_tensor_with_parallelism");
            if (!reconstructed.is_none()) {
                return reconstructed;
            }
        }
        if (uses_legacy_tp_storage_key(*parallelism)) {
            const auto *tp_axis =
                find_axis_spec_by_kind(*parallelism, LayoutAxisKind::TP);
            if (tp_axis) {
                return get_tensor(
                    resolve_tp_read_key(key, tp_axis->rank, tp_axis->size));
            }
        }
        LOG(ERROR) << "get_tensor_with_parallelism"
                   << ": parallelism metadata mismatch for key " << key;
        return py::none();
    }

    if (parsed_target->mode == ReadTargetMode::FULL) {
        const auto *tp_axis =
            find_axis_spec_by_kind(*parallelism, LayoutAxisKind::TP);
        if (!tp_axis) {
            LOG(ERROR) << "get_tensor_with_parallelism"
                       << ": full reconstruction requires a TP axis";
            return py::none();
        }
        return get_tensor_with_tp_full(
            key, tp_axis->rank, tp_axis->size, tp_axis->split_dim.value_or(0),
            "get_tensor_with_parallelism", *parallelism);
    }

    LOG(ERROR) << "get_tensor_with_parallelism"
               << ": unsupported ReadTarget mode";
    return py::none();
}

pybind11::list batch_get_tensor_with_parallelism(
    const std::vector<std::string> &keys,
    const py::object &targets = py::none()) {
    if (targets.is_none()) {
        return batch_get_tensor(keys);
    }

    auto target_list = validate_batch_request_list(
        targets, keys.size(), "batch_get_tensor_with_parallelism", "targets");
    if (!target_list.has_value()) {
        py::list empty;
        for (size_t i = 0; i < keys.size(); ++i) {
            empty.append(py::none());
        }
        return empty;
    }

    py::list results;
    for (size_t i = 0; i < keys.size(); ++i) {
        results.append(get_tensor_with_parallelism(
            keys[i], py::reinterpret_borrow<py::object>((*target_list)[i])));
    }
    return results;
}

pybind11::object get_tensor_with_parallelism_into(
    const std::string &key, uintptr_t buffer_ptr, size_t size,
    const py::object &target = py::none()) {
    auto plan = build_tensor_into_plan_for_target(
        key, buffer_ptr, size, target, "get_tensor_with_parallelism_into");
    if (!plan.has_value()) {
        return py::none();
    }
    auto results = execute_tensor_into_plans({*plan});
    if (results.empty()) {
        return py::none();
    }
    return py::reinterpret_borrow<py::object>(results[0]);
}

pybind11::list batch_get_tensor_with_parallelism_into(
    const std::vector<std::string> &keys,
    const std::vector<uintptr_t> &buffer_ptrs, const std::vector<size_t> &sizes,
    const py::object &targets = py::none()) {
    py::list empty;
    for (size_t i = 0; i < keys.size(); ++i) {
        empty.append(py::none());
    }
    if (keys.size() != buffer_ptrs.size() || keys.size() != sizes.size()) {
        LOG(ERROR)
            << "batch_get_tensor_with_parallelism_into"
            << ": keys, buffer_ptrs, and sizes must have the same length";
        return empty;
    }

    std::optional<py::list> target_list = std::nullopt;
    if (!targets.is_none()) {
        target_list = validate_batch_request_list(
            targets, keys.size(), "batch_get_tensor_with_parallelism_into",
            "targets");
        if (!target_list.has_value()) {
            return empty;
        }
    }

    std::vector<TensorIntoPlan> plans;
    plans.reserve(keys.size());
    std::vector<size_t> plan_indices;
    plan_indices.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        py::object target =
            target_list.has_value()
                ? py::reinterpret_borrow<py::object>((*target_list)[i])
                : py::none();
        auto plan = build_tensor_into_plan_for_target(
            keys[i], buffer_ptrs[i], sizes[i], target,
            "batch_get_tensor_with_parallelism_into");
        if (!plan.has_value()) {
            continue;
        }
        plan_indices.push_back(i);
        plans.push_back(*plan);
    }

    auto results = execute_tensor_into_plans(plans);
    for (size_t i = 0; i < plan_indices.size() && i < results.size(); ++i) {
        empty[plan_indices[i]] = results[i];
    }
    return empty;
}
