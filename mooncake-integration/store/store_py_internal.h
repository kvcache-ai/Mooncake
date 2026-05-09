std::vector<int64_t> tensor_shape_to_vector(const py::handle &tensor) {
    py::tuple shape_tuple = py::cast<py::tuple>(tensor.attr("shape"));
    std::vector<int64_t> shape;
    shape.reserve(shape_tuple.size());
    for (const auto &dim : shape_tuple) {
        shape.push_back(dim.cast<int64_t>());
    }
    return shape;
}

enum class ReadTargetMode {
    AS_STORED,
    SHARD,
    FULL,
};

struct ParallelAxisSpec {
    std::string kind;
    int rank{0};
    int size{1};
    std::optional<int> split_dim;
    std::optional<int> expert_id;
    std::optional<int> stage_id;
};

struct TensorParallelismSpec {
    std::vector<ParallelAxisSpec> axes;
};

struct ReadTargetSpec {
    ReadTargetMode mode{ReadTargetMode::AS_STORED};
    std::optional<TensorParallelismSpec> parallelism;
};

struct WriterPartitionSpec {
    int rank{0};
    int size{1};
    int split_dim{0};
};

struct WriterShardManifestHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t header_size;
    int32_t dtype;
    int32_t ndim;
    int32_t split_dim;
    int32_t shard_count;
    int32_t reserved0;
};

struct WriterShardManifest {
    WriterShardManifestHeader header;
    TensorShape global_shape;
};

struct ParsedWriterShardManifest {
    WriterShardManifest manifest;
    std::vector<int64_t> global_shape;
};

using ParallelismManifest = WriterShardManifest;
using ParsedParallelismManifest = ParsedWriterShardManifest;

struct WriterShardTensorInfo {
    PyTensorInfo info;
    std::string shard_key;
    WriterShardManifest manifest;
};

struct ParallelismShardTensorInfo {
    PyTensorInfo info;
    ParallelismManifest manifest;
};

struct RawTensorShardWritePlan {
    TensorMetadata metadata;
    std::vector<std::pair<size_t, size_t>> data_ranges;
};

struct TensorIntoFragment {
    std::string read_key;
    size_t dst_offset{0};
    size_t src_offset{0};
    size_t size{0};
};

struct TensorIntoPlan {
    uintptr_t user_buffer_ptr{0};
    uintptr_t registered_buffer_ptr{0};
    size_t registered_buffer_size{0};
    size_t total_length{0};
    std::vector<TensorIntoFragment> fragments;
    std::optional<TensorMetadata> materialized_metadata;
};

struct ResolvedTensorRead {
    std::string read_key;
    ParsedTensorMetadata metadata;
    std::shared_ptr<BufferHandle> buffer_handle;
};

enum class ParallelismShardReadStorageRoute {
    PARALLELISM_SHARD_KEY,
    WRITER_PARTITION_COMPATIBLE_SHARD,
};

struct ReconstructedShardSource {
    std::string read_key;
    ParsedTensorMetadata metadata;
};

struct FullTensorReconstructionSources {
    std::vector<ReconstructedShardSource> sources;
    std::vector<int64_t> global_shape;
    int split_dim{0};
    int32_t dtype{0};
    bool allow_empty_fragments{false};
};

std::optional<LayoutAxisKind> parse_layout_axis_kind(const std::string &kind);
std::optional<TensorMetadata> build_shard_metadata_from_shapes(
    int32_t dtype, const std::vector<int64_t> &global_shape,
    const std::vector<int64_t> &local_shape,
    const std::vector<ParallelAxisSpec> &axes, size_t tensor_size);
std::optional<TensorMetadata> build_shard_metadata_from_parallelism(
    const py::handle &full_tensor, const py::handle &shard_tensor,
    TensorDtype dtype_enum, const std::vector<ParallelAxisSpec> &axes,
    size_t tensor_size);
std::optional<size_t> find_tp_axis_index(
    const std::vector<ParallelAxisSpec> &axes);
bool validate_uniform_shard_request(const std::vector<int64_t> &shape,
                                    int split_dim, int shard_count,
                                    const std::string &error_context);
std::optional<py::object> materialize_shard_tensor(const py::handle &tensor,
                                                   int split_dim, int rank,
                                                   int shard_count);
std::string get_parallelism_key_name(const std::string &base_key,
                                     const TensorParallelismSpec &parallelism);
bool parallelism_matches_metadata(const TensorParallelismSpec &parallelism,
                                  const TensorMetadata &metadata);
std::optional<TensorParallelismSpec> parallelism_from_metadata(
    const TensorMetadata &metadata);
std::optional<TensorParallelismSpec> parse_tensor_parallelism_spec(
    const py::object &parallelism_obj);
std::optional<TensorParallelismSpec> validate_parallelism_spec(
    const std::optional<TensorParallelismSpec> &parallelism,
    const std::string &error_context, bool allow_empty);
std::optional<TensorParallelismSpec> canonicalize_parallelism_spec(
    const TensorParallelismSpec &parallelism);
std::optional<WriterPartitionSpec> parse_writer_partition_spec(
    const py::object &obj, const std::string &error_context);

constexpr uint32_t kWriterShardManifestMagic = 0x574d414e;
constexpr uint16_t kWriterShardManifestVersion = 1;

std::string get_writer_manifest_key_name(const std::string &base_key) {
    return base_key + "__writer_manifest";
}

std::string get_parallelism_manifest_key_name(const std::string &base_key) {
    return base_key + "__parallelism_manifest";
}

std::string get_writer_shard_key_name(const std::string &base_key,
                                      const WriterPartitionSpec &writer) {
    return base_key + "__writer_" + std::to_string(writer.rank) + "of" +
           std::to_string(writer.size) + "_sd" +
           std::to_string(writer.split_dim);
}

std::optional<ParsedWriterShardManifest> parse_writer_shard_manifest(
    BufferHandle *buffer_handle) {
    if (!buffer_handle || buffer_handle->size() < sizeof(WriterShardManifest)) {
        return std::nullopt;
    }

    ParsedWriterShardManifest parsed{};
    std::memcpy(&parsed.manifest, buffer_handle->ptr(),
                sizeof(WriterShardManifest));
    if (parsed.manifest.header.magic != kWriterShardManifestMagic ||
        parsed.manifest.header.version != kWriterShardManifestVersion ||
        parsed.manifest.header.header_size != sizeof(WriterShardManifest) ||
        parsed.manifest.header.ndim < 0 ||
        parsed.manifest.header.ndim > static_cast<int32_t>(kMaxTensorDims) ||
        parsed.manifest.header.shard_count <= 0 ||
        parsed.manifest.header.split_dim < 0 ||
        parsed.manifest.header.split_dim >= parsed.manifest.header.ndim) {
        return std::nullopt;
    }

    parsed.global_shape = TensorShapeToVector(parsed.manifest.global_shape,
                                              parsed.manifest.header.ndim);
    for (auto dim : parsed.global_shape) {
        if (dim <= 0) {
            return std::nullopt;
        }
    }
    return parsed;
}

WriterShardManifest build_writer_shard_manifest_from_shape(
    const std::vector<int64_t> &shape, int32_t dtype,
    const WriterPartitionSpec &writer) {
    WriterShardManifest manifest{};
    manifest.header.magic = kWriterShardManifestMagic;
    manifest.header.version = kWriterShardManifestVersion;
    manifest.header.header_size = sizeof(WriterShardManifest);
    manifest.header.dtype = dtype;
    manifest.header.ndim = static_cast<int32_t>(shape.size());
    manifest.header.split_dim = writer.split_dim;
    manifest.header.shard_count = writer.size;
    manifest.header.reserved0 = 0;
    manifest.global_shape = MakeTensorShape(shape);
    return manifest;
}

ParallelismManifest build_parallelism_manifest_from_shape(
    const std::vector<int64_t> &shape, int32_t dtype, int split_dim,
    int shard_count) {
    ParallelismManifest manifest{};
    manifest.header.magic = kWriterShardManifestMagic;
    manifest.header.version = kWriterShardManifestVersion;
    manifest.header.header_size = sizeof(ParallelismManifest);
    manifest.header.dtype = dtype;
    manifest.header.ndim = static_cast<int32_t>(shape.size());
    manifest.header.split_dim = split_dim;
    manifest.header.shard_count = shard_count;
    manifest.header.reserved0 = 0;
    manifest.global_shape = MakeTensorShape(shape);
    return manifest;
}

WriterShardManifest build_writer_shard_manifest(
    const py::handle &tensor, TensorDtype dtype_enum,
    const WriterPartitionSpec &writer) {
    return build_writer_shard_manifest_from_shape(
        tensor_shape_to_vector(tensor), static_cast<int32_t>(dtype_enum),
        writer);
}

TensorMetadata build_writer_shard_tensor_metadata(
    const py::handle &full_tensor, const py::handle &shard_tensor,
    TensorDtype dtype_enum, const WriterPartitionSpec &writer,
    size_t tensor_size) {
    ParallelAxisSpec axis_spec{"tp",         writer.rank,
                               writer.size,  writer.split_dim,
                               std::nullopt, std::nullopt};
    auto metadata = build_shard_metadata_from_parallelism(
        full_tensor, shard_tensor, dtype_enum, {axis_spec}, tensor_size);
    return metadata.value_or(TensorMetadata{});
}

TensorMetadata build_full_tensor_metadata(const py::handle &tensor,
                                          TensorDtype dtype_enum,
                                          size_t tensor_size) {
    auto shape = tensor_shape_to_vector(tensor);
    TensorMetadata metadata = BuildTensorMetadata(
        static_cast<int32_t>(dtype_enum), shape, shape, TensorLayoutKind::FULL);
    metadata.header.data_bytes = tensor_size;
    return metadata;
}

std::optional<LayoutAxis> build_layout_axis_from_spec(
    const ParallelAxisSpec &axis_spec, size_t axis_index,
    int64_t local_split_extent = -1) {
    auto kind = parse_layout_axis_kind(axis_spec.kind);
    if (!kind.has_value()) {
        return std::nullopt;
    }

    LayoutAxis axis{};
    axis.kind = static_cast<int32_t>(*kind);
    axis.axis_index = static_cast<int32_t>(axis_index);
    axis.shard_rank = axis_spec.rank;
    axis.shard_count = axis_spec.size;
    axis.split_dim = axis_spec.split_dim.value_or(-1);
    axis.reserved0 = 0;
    axis.reserved1 = 0;

    if (*kind == LayoutAxisKind::EP && axis_spec.expert_id.has_value()) {
        axis.reserved0 = axis_spec.expert_id.value();
    }
    if (*kind == LayoutAxisKind::PP && axis_spec.stage_id.has_value()) {
        axis.reserved0 = axis_spec.stage_id.value();
    }
    if (local_split_extent >= 0) {
        axis.reserved1 = local_split_extent;
    }
    return axis;
}

std::optional<TensorMetadata> build_shard_metadata_from_shapes(
    int32_t dtype, const std::vector<int64_t> &global_shape,
    const std::vector<int64_t> &local_shape,
    const std::vector<ParallelAxisSpec> &axes, size_t tensor_size) {
    if (axes.empty() || axes.size() > kMaxLayoutAxes ||
        global_shape.size() != local_shape.size()) {
        return std::nullopt;
    }

    std::vector<LayoutAxis> layout_axes;
    layout_axes.reserve(axes.size());
    for (size_t i = 0; i < axes.size(); ++i) {
        int64_t local_split_extent = -1;
        if (axes[i].split_dim.has_value()) {
            int split_dim = axes[i].split_dim.value();
            if (split_dim < 0 ||
                split_dim >= static_cast<int>(local_shape.size())) {
                return std::nullopt;
            }
            local_split_extent = local_shape[split_dim];
        }
        auto layout_axis =
            build_layout_axis_from_spec(axes[i], i, local_split_extent);
        if (!layout_axis.has_value()) {
            return std::nullopt;
        }
        layout_axes.push_back(*layout_axis);
    }

    TensorMetadata metadata = BuildTensorMetadata(
        dtype, global_shape, local_shape, TensorLayoutKind::SHARD,
        std::span<const LayoutAxis>(layout_axes.data(), layout_axes.size()));
    metadata.header.data_bytes = tensor_size;
    return metadata;
}

std::optional<TensorMetadata> build_shard_metadata_from_parallelism(
    const py::handle &full_tensor, const py::handle &shard_tensor,
    TensorDtype dtype_enum, const std::vector<ParallelAxisSpec> &axes,
    size_t tensor_size) {
    return build_shard_metadata_from_shapes(
        static_cast<int32_t>(dtype_enum), tensor_shape_to_vector(full_tensor),
        tensor_shape_to_vector(shard_tensor), axes, tensor_size);
}

bool validate_uniform_shard_request(const std::vector<int64_t> &shape,
                                    int split_dim, int shard_count,
                                    const std::string &error_context);

TensorMetadata build_tp_shard_metadata(const py::handle &full_tensor,
                                       const py::handle &shard_tensor,
                                       TensorDtype dtype_enum, int tp_rank,
                                       int tp_size, int split_dim,
                                       size_t tensor_size) {
    ParallelAxisSpec axis_spec{"tp",      tp_rank,      tp_size,
                               split_dim, std::nullopt, std::nullopt};
    auto metadata = build_shard_metadata_from_parallelism(
        full_tensor, shard_tensor, dtype_enum, {axis_spec}, tensor_size);
    return metadata.value_or(TensorMetadata{});
}

std::optional<size_t> find_tp_axis_index(
    const std::vector<ParallelAxisSpec> &axes) {
    for (size_t i = 0; i < axes.size(); ++i) {
        auto kind = parse_layout_axis_kind(axes[i].kind);
        if (kind == std::optional<LayoutAxisKind>(LayoutAxisKind::TP)) {
            return i;
        }
    }
    return std::nullopt;
}

std::optional<PyTensorInfo> build_direct_parallelism_shard_info(
    const py::handle &tensor, const std::vector<ParallelAxisSpec> &axes,
    const std::string &key_name) {
    TensorDtype dtype_enum = get_tensor_dtype(tensor.attr("dtype"));
    if (dtype_enum == TensorDtype::UNKNOWN) {
        return std::nullopt;
    }

    auto info = extract_tensor_info(py::reinterpret_borrow<py::object>(tensor),
                                    key_name);
    auto metadata = build_shard_metadata_from_parallelism(
        tensor, tensor, dtype_enum, axes, info.tensor_size);
    if (!metadata.has_value()) {
        return std::nullopt;
    }
    info.metadata = *metadata;
    if (!info.valid()) {
        return std::nullopt;
    }
    return info;
}

std::optional<ParallelismShardTensorInfo> build_direct_parallelism_shard_info(
    const py::handle &shard_tensor, const std::vector<ParallelAxisSpec> &axes,
    const std::string &key_name, bool infer_global_shape) {
    auto tp_axis_index = find_tp_axis_index(axes);
    if (!tp_axis_index.has_value()) {
        return std::nullopt;
    }

    const auto &tp_axis = axes[*tp_axis_index];
    if (!tp_axis.split_dim.has_value()) {
        return std::nullopt;
    }

    auto local_shape = tensor_shape_to_vector(shard_tensor);
    int split_dim = tp_axis.split_dim.value();
    if (split_dim < 0 || split_dim >= static_cast<int>(local_shape.size())) {
        return std::nullopt;
    }

    auto global_shape = local_shape;
    if (infer_global_shape) {
        global_shape[split_dim] *= tp_axis.size;
    }
    if (!validate_uniform_shard_request(
            global_shape, split_dim, tp_axis.size,
            "build_direct_parallelism_shard_info")) {
        return std::nullopt;
    }

    TensorDtype dtype_enum = get_tensor_dtype(shard_tensor.attr("dtype"));
    if (dtype_enum == TensorDtype::UNKNOWN) {
        return std::nullopt;
    }

    ParallelismShardTensorInfo shard_info;
    shard_info.info = extract_tensor_info(
        py::reinterpret_borrow<py::object>(shard_tensor), key_name);
    auto metadata = build_shard_metadata_from_parallelism(
        shard_tensor, shard_tensor, dtype_enum, axes,
        shard_info.info.tensor_size);
    if (!metadata.has_value()) {
        return std::nullopt;
    }
    shard_info.info.metadata = *metadata;
    shard_info.info.metadata.layout.global_shape =
        MakeTensorShape(global_shape);
    if (!shard_info.info.valid()) {
        return std::nullopt;
    }
    shard_info.manifest = build_parallelism_manifest_from_shape(
        global_shape, static_cast<int32_t>(dtype_enum), split_dim,
        tp_axis.size);
    return shard_info;
}

std::optional<ParallelismShardTensorInfo>
build_requested_parallelism_shard_info(const py::handle &tensor,
                                       const TensorParallelismSpec &parallelism,
                                       const std::string &key_name,
                                       const std::string &error_context) {
    auto tp_axis_index = find_tp_axis_index(parallelism.axes);
    if (!tp_axis_index.has_value()) {
        return std::nullopt;
    }

    const auto &tp_axis = parallelism.axes[*tp_axis_index];
    if (!tp_axis.split_dim.has_value()) {
        return std::nullopt;
    }

    const int split_dim = tp_axis.split_dim.value();
    auto global_shape = tensor_shape_to_vector(tensor);
    if (!validate_uniform_shard_request(global_shape, split_dim, tp_axis.size,
                                        error_context)) {
        return std::nullopt;
    }

    TensorDtype dtype_enum = get_tensor_dtype(tensor.attr("dtype"));
    if (dtype_enum == TensorDtype::UNKNOWN) {
        return std::nullopt;
    }

    auto shard_tensor =
        materialize_shard_tensor(tensor, split_dim, tp_axis.rank, tp_axis.size);
    if (!shard_tensor.has_value()) {
        return std::nullopt;
    }

    ParallelismShardTensorInfo shard_info;
    shard_info.info = extract_tensor_info(*shard_tensor, key_name);
    auto metadata = build_shard_metadata_from_parallelism(
        tensor, *shard_tensor, dtype_enum, parallelism.axes,
        shard_info.info.tensor_size);
    if (!metadata.has_value()) {
        return std::nullopt;
    }
    shard_info.info.metadata = *metadata;
    if (!shard_info.info.valid()) {
        return std::nullopt;
    }
    shard_info.manifest = build_parallelism_manifest_from_shape(
        global_shape, static_cast<int32_t>(dtype_enum), split_dim,
        tp_axis.size);
    return shard_info;
}

bool is_uniform_shardable_dim(int64_t dim_size, int shard_count) {
    return dim_size >= 0 && shard_count > 0 && dim_size % shard_count == 0;
}

bool validate_uniform_shard_request(const std::vector<int64_t> &shape,
                                    int split_dim, int shard_count,
                                    const std::string &error_context) {
    if (split_dim < 0 || split_dim >= static_cast<int>(shape.size()) ||
        shard_count <= 0) {
        LOG(ERROR) << error_context << ": invalid shard parameters";
        return false;
    }
    if (!is_uniform_shardable_dim(shape[split_dim], shard_count)) {
        LOG(ERROR) << error_context << ": only uniform sharding is supported";
        return false;
    }
    return true;
}

std::optional<py::object> materialize_shard_tensor(const py::handle &tensor,
                                                   int split_dim, int rank,
                                                   int shard_count) {
    auto shape = tensor_shape_to_vector(tensor);
    if (rank < 0 || rank >= shard_count ||
        !validate_uniform_shard_request(shape, split_dim, shard_count,
                                        "materialize_shard_tensor")) {
        return std::nullopt;
    }

    py::object torch_tensor = py::reinterpret_borrow<py::object>(tensor);
    const auto [start, size] =
        calculate_shard_range(shape[split_dim], rank, shard_count);
    return torch_tensor.attr("narrow")(split_dim, start, size)
        .attr("contiguous")();
}

std::optional<std::vector<PyTensorInfo>> build_tp_shard_infos(
    const py::handle &tensor, int tp_size, int split_dim,
    const std::function<std::string(int)> &key_for_rank,
    const std::vector<ParallelAxisSpec> &axes = {}) {
    TensorDtype dtype_enum = get_tensor_dtype(tensor.attr("dtype"));
    if (dtype_enum == TensorDtype::UNKNOWN) {
        return std::nullopt;
    }

    auto shape = tensor_shape_to_vector(tensor);
    if (!validate_uniform_shard_request(shape, split_dim, tp_size,
                                        "build_tp_shard_infos")) {
        return std::nullopt;
    }

    auto tp_axis_index = find_tp_axis_index(axes);
    std::vector<PyTensorInfo> infos;
    infos.reserve(tp_size);

    for (int rank = 0; rank < tp_size; ++rank) {
        auto chunk = materialize_shard_tensor(tensor, split_dim, rank, tp_size);
        if (!chunk.has_value()) {
            return std::nullopt;
        }

        auto info = extract_tensor_info(*chunk, key_for_rank(rank));
        if (axes.empty()) {
            info.metadata =
                build_tp_shard_metadata(tensor, *chunk, dtype_enum, rank,
                                        tp_size, split_dim, info.tensor_size);
        } else {
            auto shard_axes = axes;
            if (!tp_axis_index.has_value()) {
                return std::nullopt;
            }
            shard_axes[*tp_axis_index].rank = rank;
            auto metadata = build_shard_metadata_from_parallelism(
                tensor, *chunk, dtype_enum, shard_axes, info.tensor_size);
            if (!metadata.has_value()) {
                return std::nullopt;
            }
            info.metadata = *metadata;
        }
        if (!info.valid()) {
            return std::nullopt;
        }
        infos.push_back(info);
    }
    return infos;
}

bool is_shard_tensor_metadata(const TensorMetadata &metadata) {
    return metadata.header.layout_kind ==
           static_cast<uint32_t>(TensorLayoutKind::SHARD);
}

const LayoutAxis *find_layout_axis(const TensorMetadata &metadata,
                                   LayoutAxisKind kind) {
    for (size_t i = 0; i < metadata.layout.axis_count; ++i) {
        if (metadata.layout.axes[i].kind == static_cast<int32_t>(kind)) {
            return &metadata.layout.axes[i];
        }
    }
    return nullptr;
}

std::optional<LayoutAxisKind> parse_layout_axis_kind(const std::string &kind) {
    std::string normalized = kind;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    if (normalized == "TP") return LayoutAxisKind::TP;
    if (normalized == "DP") return LayoutAxisKind::DP;
    if (normalized == "EP") return LayoutAxisKind::EP;
    if (normalized == "PP") return LayoutAxisKind::PP;
    LOG(ERROR) << "Unsupported parallel axis kind: " << kind;
    return std::nullopt;
}

bool is_single_axis_parallelism_kind(const TensorParallelismSpec &parallelism,
                                     LayoutAxisKind kind) {
    return parallelism.axes.size() == 1 &&
           parse_layout_axis_kind(parallelism.axes[0].kind) ==
               std::optional<LayoutAxisKind>(kind);
}

bool uses_legacy_tp_storage_key(const TensorParallelismSpec &parallelism) {
    return is_single_axis_parallelism_kind(parallelism, LayoutAxisKind::TP);
}

bool should_use_legacy_single_tp_write_route(
    const TensorParallelismSpec &parallelism) {
    return false;
}

bool can_read_single_tp_request_from_writer_partition_storage(
    const TensorParallelismSpec &parallelism) {
    return uses_legacy_tp_storage_key(parallelism);
}

std::vector<ParallelismShardReadStorageRoute>
resolve_parallelism_shard_read_routes(
    const TensorParallelismSpec &parallelism) {
    std::vector<ParallelismShardReadStorageRoute> routes{
        ParallelismShardReadStorageRoute::PARALLELISM_SHARD_KEY,
    };
    if (can_read_single_tp_request_from_writer_partition_storage(parallelism)) {
        routes.push_back(ParallelismShardReadStorageRoute::
                             WRITER_PARTITION_COMPATIBLE_SHARD);
    }
    return routes;
}

std::optional<TensorParallelismSpec> writer_partition_parallelism_from_metadata(
    const TensorMetadata &metadata) {
    auto parallelism = parallelism_from_metadata(metadata);
    if (!parallelism.has_value() || !uses_legacy_tp_storage_key(*parallelism)) {
        return std::nullopt;
    }
    return parallelism;
}

enum class ParallelismWriteStorageRoute {
    DIRECT_FULL_OBJECT,
    WRITER_PARTITION_SHARD,
    LEGACY_SINGLE_TP,
    TP_SHARDED_PARALLELISM,
    GENERIC_PARALLELISM_SHARD,
};

struct ResolvedParallelismWriteRequest {
    ParallelismWriteStorageRoute route;
    std::optional<TensorParallelismSpec> parallelism;
    std::optional<WriterPartitionSpec> writer_partition;
};

struct TensorWriteStoreOps {
    const char *parts_operation_name;
};

std::optional<ResolvedParallelismWriteRequest>
resolve_parallelism_write_request(const py::object &parallelism_obj,
                                  const py::object &writer_partition_obj,
                                  const std::string &error_context) {
    if (!writer_partition_obj.is_none()) {
        if (!parallelism_obj.is_none()) {
            LOG(ERROR)
                << error_context
                << ": writer_partition cannot be combined with parallelism";
            return std::nullopt;
        }
        auto writer_partition =
            parse_writer_partition_spec(writer_partition_obj, error_context);
        if (!writer_partition.has_value()) {
            return std::nullopt;
        }
        return ResolvedParallelismWriteRequest{
            ParallelismWriteStorageRoute::WRITER_PARTITION_SHARD,
            std::nullopt,
            *writer_partition,
        };
    }
    if (parallelism_obj.is_none()) {
        return ResolvedParallelismWriteRequest{
            ParallelismWriteStorageRoute::DIRECT_FULL_OBJECT,
            std::nullopt,
            std::nullopt,
        };
    }

    auto parallelism = validate_parallelism_spec(
        parse_tensor_parallelism_spec(parallelism_obj), error_context, false);
    if (!parallelism.has_value()) {
        return std::nullopt;
    }
    if (should_use_legacy_single_tp_write_route(*parallelism)) {
        return ResolvedParallelismWriteRequest{
            ParallelismWriteStorageRoute::LEGACY_SINGLE_TP,
            *parallelism,
            std::nullopt,
        };
    }
    for (const auto &axis : parallelism->axes) {
        if (parse_layout_axis_kind(axis.kind) ==
            std::optional<LayoutAxisKind>(LayoutAxisKind::TP)) {
            return ResolvedParallelismWriteRequest{
                ParallelismWriteStorageRoute::TP_SHARDED_PARALLELISM,
                *parallelism,
                std::nullopt,
            };
        }
    }
    return ResolvedParallelismWriteRequest{
        ParallelismWriteStorageRoute::GENERIC_PARALLELISM_SHARD,
        *parallelism,
        std::nullopt,
    };
}

std::optional<py::list> validate_batch_request_list(
    const py::object &request_list_obj, size_t expected_size,
    const std::string &error_context, const char *request_name) {
    if (!py::isinstance<py::list>(request_list_obj)) {
        LOG(ERROR) << error_context << ": " << request_name
                   << " must be a list or None";
        return std::nullopt;
    }

    py::list request_list = py::cast<py::list>(request_list_obj);
    if (request_list.size() != expected_size) {
        LOG(ERROR) << error_context << ": keys and " << request_name
                   << " must have the same length";
        return std::nullopt;
    }
    return request_list;
}

std::optional<py::list> validate_batch_parallelism_list(
    const py::object &parallelisms, size_t expected_size,
    const std::string &error_context) {
    return validate_batch_request_list(parallelisms, expected_size,
                                       error_context, "parallelisms");
}

std::optional<py::list> validate_batch_writer_partition_list(
    const py::object &writer_partitions, size_t expected_size,
    const std::string &error_context) {
    return validate_batch_request_list(writer_partitions, expected_size,
                                       error_context, "writer_partitions");
}

bool axis_specs_equal(const ParallelAxisSpec &lhs,
                      const ParallelAxisSpec &rhs) {
    auto lhs_kind = parse_layout_axis_kind(lhs.kind);
    auto rhs_kind = parse_layout_axis_kind(rhs.kind);
    return lhs_kind.has_value() && rhs_kind.has_value() &&
           lhs_kind == rhs_kind && lhs.rank == rhs.rank &&
           lhs.size == rhs.size && lhs.split_dim == rhs.split_dim &&
           lhs.expert_id == rhs.expert_id && lhs.stage_id == rhs.stage_id;
}

int canonical_axis_kind_order(LayoutAxisKind kind) {
    switch (kind) {
        case LayoutAxisKind::DP:
            return 0;
        case LayoutAxisKind::TP:
            return 1;
        case LayoutAxisKind::EP:
            return 2;
        case LayoutAxisKind::PP:
            return 3;
        default:
            return 4;
    }
}

std::optional<TensorParallelismSpec> canonicalize_parallelism_spec(
    const TensorParallelismSpec &parallelism) {
    TensorParallelismSpec canonical = parallelism;
    std::sort(canonical.axes.begin(), canonical.axes.end(),
              [](const ParallelAxisSpec &lhs, const ParallelAxisSpec &rhs) {
                  auto lhs_kind = parse_layout_axis_kind(lhs.kind);
                  auto rhs_kind = parse_layout_axis_kind(rhs.kind);
                  if (!lhs_kind.has_value() || !rhs_kind.has_value()) {
                      return lhs.kind < rhs.kind;
                  }
                  int lhs_order = canonical_axis_kind_order(*lhs_kind);
                  int rhs_order = canonical_axis_kind_order(*rhs_kind);
                  if (lhs_order != rhs_order) {
                      return lhs_order < rhs_order;
                  }
                  return lhs.kind < rhs.kind;
              });
    return canonical;
}

const ParallelAxisSpec *find_axis_spec_by_kind(
    const TensorParallelismSpec &parallelism, LayoutAxisKind kind) {
    for (const auto &axis : parallelism.axes) {
        auto axis_kind = parse_layout_axis_kind(axis.kind);
        if (axis_kind == std::optional<LayoutAxisKind>(kind)) {
            return &axis;
        }
    }
    return nullptr;
}

bool parallelism_specs_equal_by_kind(const TensorParallelismSpec &lhs,
                                     const TensorParallelismSpec &rhs,
                                     bool allow_tp_rank_mismatch = false) {
    if (lhs.axes.size() != rhs.axes.size()) {
        return false;
    }

    for (const auto &lhs_axis : lhs.axes) {
        auto lhs_kind = parse_layout_axis_kind(lhs_axis.kind);
        if (!lhs_kind.has_value()) {
            return false;
        }
        const auto *rhs_axis = find_axis_spec_by_kind(rhs, *lhs_kind);
        if (!rhs_axis) {
            return false;
        }
        if (*lhs_kind == LayoutAxisKind::TP && allow_tp_rank_mismatch) {
            if (lhs_axis.size != rhs_axis->size ||
                lhs_axis.split_dim != rhs_axis->split_dim ||
                lhs_axis.expert_id != rhs_axis->expert_id ||
                lhs_axis.stage_id != rhs_axis->stage_id) {
                return false;
            }
            continue;
        }
        if (!axis_specs_equal(lhs_axis, *rhs_axis)) {
            return false;
        }
    }
    return true;
}

bool is_default_replicate_config(const ReplicateConfig &config) {
    return config.replica_num == 1 && !config.with_soft_pin &&
           !config.with_hard_pin && config.preferred_segments.empty() &&
           config.preferred_segment.empty() &&
           !config.prefer_alloc_in_same_node;
}

std::optional<ParallelAxisSpec> parse_parallel_axis_spec(
    const py::handle &obj) {
    if (!py::hasattr(obj, "kind") || !py::hasattr(obj, "rank") ||
        !py::hasattr(obj, "size")) {
        LOG(ERROR) << "ParallelAxis must provide kind, rank, and size";
        return std::nullopt;
    }

    ParallelAxisSpec axis;
    axis.kind = py::cast<std::string>(obj.attr("kind"));
    if (!parse_layout_axis_kind(axis.kind).has_value()) {
        return std::nullopt;
    }
    axis.rank = py::cast<int>(obj.attr("rank"));
    axis.size = py::cast<int>(obj.attr("size"));
    py::object split_dim = obj.attr("split_dim");
    if (!split_dim.is_none()) axis.split_dim = py::cast<int>(split_dim);
    py::object expert_id = obj.attr("expert_id");
    if (!expert_id.is_none()) axis.expert_id = py::cast<int>(expert_id);
    py::object stage_id = obj.attr("stage_id");
    if (!stage_id.is_none()) axis.stage_id = py::cast<int>(stage_id);
    return axis;
}

std::optional<TensorParallelismSpec> parse_tensor_parallelism_spec(
    const py::object &obj) {
    if (obj.is_none()) {
        return std::nullopt;
    }
    if (!py::hasattr(obj, "axes")) {
        LOG(ERROR) << "TensorParallelism must provide axes";
        return std::nullopt;
    }

    TensorParallelismSpec parallelism;
    py::list axes = py::cast<py::list>(obj.attr("axes"));
    for (const auto &axis_obj : axes) {
        auto axis = parse_parallel_axis_spec(axis_obj);
        if (!axis.has_value()) {
            return std::nullopt;
        }
        parallelism.axes.push_back(*axis);
    }
    return parallelism;
}

std::optional<ReadTargetMode> parse_read_target_mode(const py::object &obj) {
    std::string mode = py::cast<std::string>(obj);
    std::transform(mode.begin(), mode.end(), mode.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    if (mode == "AS_STORED") return ReadTargetMode::AS_STORED;
    if (mode == "SHARD") return ReadTargetMode::SHARD;
    if (mode == "FULL") return ReadTargetMode::FULL;
    LOG(ERROR) << "Unsupported ReadTarget mode: " << mode;
    return std::nullopt;
}

std::optional<WriterPartitionSpec> parse_writer_partition_spec(
    const py::object &obj, const std::string &error_context) {
    if (obj.is_none()) {
        return std::nullopt;
    }
    if (!py::hasattr(obj, "rank") || !py::hasattr(obj, "size") ||
        !py::hasattr(obj, "split_dim")) {
        LOG(ERROR)
            << error_context
            << ": writer_partition must provide rank, size, and split_dim";
        return std::nullopt;
    }

    WriterPartitionSpec writer;
    writer.rank = py::cast<int>(obj.attr("rank"));
    writer.size = py::cast<int>(obj.attr("size"));
    writer.split_dim = py::cast<int>(obj.attr("split_dim"));
    if (writer.size <= 0 || writer.rank < 0 || writer.rank >= writer.size ||
        writer.split_dim < 0) {
        LOG(ERROR) << error_context << ": invalid writer partition";
        return std::nullopt;
    }
    return writer;
}

bool is_valid_writer_partition(const WriterPartitionSpec &writer,
                               const std::vector<int64_t> &shape,
                               const std::string &error_context) {
    if (writer.size <= 0 || writer.rank < 0 || writer.rank >= writer.size) {
        LOG(ERROR) << error_context << ": invalid writer rank/size";
        return false;
    }
    if (writer.split_dim < 0 ||
        writer.split_dim >= static_cast<int>(shape.size())) {
        LOG(ERROR) << error_context << ": split_dim out of range";
        return false;
    }
    if (!validate_uniform_shard_request(shape, writer.split_dim, writer.size,
                                        error_context)) {
        return false;
    }
    return true;
}

std::optional<WriterShardTensorInfo> build_writer_shard_tensor_info(
    const std::string &key, const py::object &tensor,
    const WriterPartitionSpec &writer, const std::string &error_context) {
    auto shape = tensor_shape_to_vector(tensor);
    if (!is_valid_writer_partition(writer, shape, error_context)) {
        return std::nullopt;
    }

    TensorDtype dtype_enum = get_tensor_dtype(tensor.attr("dtype"));
    if (dtype_enum == TensorDtype::UNKNOWN) {
        LOG(ERROR) << error_context << ": unsupported tensor dtype";
        return std::nullopt;
    }

    auto shard_tensor = materialize_shard_tensor(tensor, writer.split_dim,
                                                 writer.rank, writer.size);
    if (!shard_tensor.has_value()) {
        LOG(ERROR) << error_context << ": failed to materialize writer shard";
        return std::nullopt;
    }

    WriterShardTensorInfo writer_info;
    writer_info.shard_key = get_writer_shard_key_name(key, writer);
    writer_info.info =
        extract_tensor_info(*shard_tensor, writer_info.shard_key);
    writer_info.info.metadata = build_writer_shard_tensor_metadata(
        tensor, *shard_tensor, dtype_enum, writer,
        writer_info.info.tensor_size);
    if (!writer_info.info.valid()) {
        LOG(ERROR) << error_context << ": invalid writer shard tensor info";
        return std::nullopt;
    }
    writer_info.manifest =
        build_writer_shard_manifest(tensor, dtype_enum, writer);
    return writer_info;
}

std::optional<ReadTargetSpec> parse_read_target_spec(const py::object &obj) {
    if (obj.is_none()) {
        return ReadTargetSpec{};
    }
    if (!py::hasattr(obj, "mode") || !py::hasattr(obj, "parallelism")) {
        LOG(ERROR) << "ReadTarget must provide mode and parallelism";
        return std::nullopt;
    }

    ReadTargetSpec target;
    auto mode = parse_read_target_mode(obj.attr("mode"));
    if (!mode.has_value()) {
        return std::nullopt;
    }
    target.mode = *mode;
    auto parallelism = parse_tensor_parallelism_spec(obj.attr("parallelism"));
    if (obj.attr("parallelism").is_none()) {
        target.parallelism = std::nullopt;
    } else if (!parallelism.has_value()) {
        return std::nullopt;
    } else {
        target.parallelism = *parallelism;
    }
    return target;
}

std::optional<ResolvedTensorRead> probe_parallelism_shard_read(
    const std::function<std::optional<ParsedTensorMetadata>(
        const std::string &, std::shared_ptr<BufferHandle> *)> &load_metadata,
    const std::string &key, const TensorParallelismSpec &parallelism,
    ParallelismShardReadStorageRoute route) {
    switch (route) {
        case ParallelismShardReadStorageRoute::PARALLELISM_SHARD_KEY: {
            std::string read_key = get_parallelism_key_name(key, parallelism);
            std::shared_ptr<BufferHandle> buffer_handle;
            auto metadata = load_metadata(read_key, &buffer_handle);
            if (!metadata.has_value() || !parallelism_matches_metadata(
                                             parallelism, metadata->metadata)) {
                return std::nullopt;
            }
            return ResolvedTensorRead{read_key, *metadata,
                                      std::move(buffer_handle)};
        }
        case ParallelismShardReadStorageRoute::
            WRITER_PARTITION_COMPATIBLE_SHARD: {
            const auto &axis = parallelism.axes[0];
            WriterPartitionSpec writer{
                .rank = axis.rank,
                .size = axis.size,
                .split_dim = axis.split_dim.value_or(0),
            };
            std::string read_key = get_writer_shard_key_name(key, writer);
            std::shared_ptr<BufferHandle> buffer_handle;
            auto metadata = load_metadata(read_key, &buffer_handle);
            if (!metadata.has_value()) {
                return std::nullopt;
            }

            auto writer_parallelism =
                writer_partition_parallelism_from_metadata(metadata->metadata);
            if (!writer_parallelism.has_value() ||
                !parallelism_matches_metadata(parallelism,
                                              metadata->metadata)) {
                return std::nullopt;
            }
            return ResolvedTensorRead{read_key, *metadata,
                                      std::move(buffer_handle)};
        }
    }
    return std::nullopt;
}

std::optional<ResolvedTensorRead> resolve_parallelism_shard_read(
    const std::function<std::optional<ParsedTensorMetadata>(
        const std::string &, std::shared_ptr<BufferHandle> *)> &load_metadata,
    const std::string &key, const TensorParallelismSpec &parallelism) {
    for (auto route : resolve_parallelism_shard_read_routes(parallelism)) {
        auto resolved = probe_parallelism_shard_read(load_metadata, key,
                                                     parallelism, route);
        if (resolved.has_value()) {
            return resolved;
        }
    }
    return std::nullopt;
}

std::optional<TensorParallelismSpec> validate_parallelism_spec(
    const std::optional<TensorParallelismSpec> &parallelism,
    const std::string &error_context, bool allow_empty = false) {
    if (!parallelism.has_value()) {
        if (allow_empty) {
            return parallelism;
        }
        LOG(ERROR) << error_context << ": parallelism is required";
        return std::nullopt;
    }

    if (parallelism->axes.empty()) {
        if (allow_empty) {
            return parallelism;
        }
        LOG(ERROR) << error_context << ": parallelism axes cannot be empty";
        return std::nullopt;
    }
    if (parallelism->axes.size() > kMaxLayoutAxes) {
        LOG(ERROR) << error_context
                   << ": axis count exceeds max supported axes";
        return std::nullopt;
    }

    std::unordered_set<int> seen_kinds;
    for (const auto &axis : parallelism->axes) {
        auto kind = parse_layout_axis_kind(axis.kind);
        if (!kind.has_value()) {
            LOG(ERROR) << error_context << ": unsupported axis kind "
                       << axis.kind;
            return std::nullopt;
        }
        if (axis.size <= 0 || axis.rank < 0 || axis.rank >= axis.size) {
            LOG(ERROR) << error_context << ": invalid rank/size for axis "
                       << axis.kind;
            return std::nullopt;
        }
        if (!seen_kinds.insert(static_cast<int>(*kind)).second) {
            LOG(ERROR) << error_context
                       << ": duplicate axis kinds are not supported";
            return std::nullopt;
        }

        switch (*kind) {
            case LayoutAxisKind::TP:
                if (!axis.split_dim.has_value()) {
                    LOG(ERROR)
                        << error_context << ": TP axis requires split_dim";
                    return std::nullopt;
                }
                break;
            case LayoutAxisKind::DP:
                if (axis.split_dim.has_value()) {
                    LOG(ERROR) << error_context
                               << ": DP axis must not provide split_dim";
                    return std::nullopt;
                }
                break;
            case LayoutAxisKind::EP:
                if (!axis.expert_id.has_value()) {
                    LOG(ERROR)
                        << error_context << ": EP axis requires expert_id";
                    return std::nullopt;
                }
                if (axis.split_dim.has_value()) {
                    LOG(ERROR)
                        << error_context
                        << ": EP axis must not provide split_dim in this phase";
                    return std::nullopt;
                }
                break;
            case LayoutAxisKind::PP:
                if (!axis.stage_id.has_value()) {
                    LOG(ERROR)
                        << error_context << ": PP axis requires stage_id";
                    return std::nullopt;
                }
                if (axis.split_dim.has_value()) {
                    LOG(ERROR) << error_context
                               << ": PP axis must not provide split_dim";
                    return std::nullopt;
                }
                break;
            default:
                LOG(ERROR) << error_context << ": unsupported axis kind";
                return std::nullopt;
        }
    }

    return parallelism;
}

std::string normalize_axis_kind_string(LayoutAxisKind kind) {
    switch (kind) {
        case LayoutAxisKind::TP:
            return "tp";
        case LayoutAxisKind::DP:
            return "dp";
        case LayoutAxisKind::EP:
            return "ep";
        case LayoutAxisKind::PP:
            return "pp";
        default:
            return "unknown";
    }
}

std::string encode_axis_key_suffix(const ParallelAxisSpec &axis) {
    auto kind = parse_layout_axis_kind(axis.kind);
    if (!kind.has_value()) {
        return "invalid";
    }

    std::string suffix = normalize_axis_kind_string(*kind) + "_" +
                         std::to_string(axis.rank) + "of" +
                         std::to_string(axis.size);
    if (axis.split_dim.has_value()) {
        suffix += "_sd" + std::to_string(axis.split_dim.value());
    }
    if (axis.expert_id.has_value()) {
        suffix += "_eid" + std::to_string(axis.expert_id.value());
    }
    if (axis.stage_id.has_value()) {
        suffix += "_sid" + std::to_string(axis.stage_id.value());
    }
    return suffix;
}

std::string get_parallelism_key_name(const std::string &base_key,
                                     const TensorParallelismSpec &parallelism) {
    if (uses_legacy_tp_storage_key(parallelism)) {
        return base_key + "_tp_" + std::to_string(parallelism.axes[0].rank);
    }

    auto canonical = canonicalize_parallelism_spec(parallelism);
    if (!canonical.has_value()) {
        return base_key;
    }

    std::string key = base_key;
    for (const auto &axis : canonical->axes) {
        key += "__" + encode_axis_key_suffix(axis);
    }
    return key;
}

std::optional<TensorParallelismSpec> parallelism_from_metadata(
    const TensorMetadata &metadata) {
    if (!is_shard_tensor_metadata(metadata)) {
        return std::nullopt;
    }

    TensorParallelismSpec parallelism;
    parallelism.axes.reserve(metadata.layout.axis_count);
    for (size_t i = 0; i < metadata.layout.axis_count; ++i) {
        const auto &stored_axis = metadata.layout.axes[i];
        auto kind = static_cast<LayoutAxisKind>(stored_axis.kind);
        ParallelAxisSpec axis;
        axis.kind = normalize_axis_kind_string(kind);
        axis.rank = stored_axis.shard_rank;
        axis.size = stored_axis.shard_count;
        if (stored_axis.split_dim >= 0) {
            axis.split_dim = stored_axis.split_dim;
        }
        if (kind == LayoutAxisKind::EP) {
            axis.expert_id = stored_axis.reserved0;
        }
        if (kind == LayoutAxisKind::PP) {
            axis.stage_id = stored_axis.reserved0;
        }
        parallelism.axes.push_back(axis);
    }
    return canonicalize_parallelism_spec(parallelism);
}

bool parallelism_matches_metadata(const TensorParallelismSpec &parallelism,
                                  const TensorMetadata &metadata) {
    auto requested_parallelism = canonicalize_parallelism_spec(parallelism);
    auto stored_parallelism = parallelism_from_metadata(metadata);
    if (!requested_parallelism.has_value() || !stored_parallelism.has_value() ||
        stored_parallelism->axes.size() != requested_parallelism->axes.size()) {
        return false;
    }

    for (size_t i = 0; i < requested_parallelism->axes.size(); ++i) {
        if (!axis_specs_equal(requested_parallelism->axes[i],
                              stored_parallelism->axes[i])) {
            return false;
        }
    }
    return true;
}

std::optional<TensorParallelismSpec>
resolve_tp_compatible_parallelism_from_metadata(
    const TensorParallelismSpec &parallelism, const TensorMetadata &metadata,
    const std::string &error_context) {
    auto stored_parallelism = parallelism_from_metadata(metadata);
    if (!stored_parallelism.has_value()) {
        LOG(ERROR) << error_context << ": missing shard parallelism metadata";
        return std::nullopt;
    }
    if (stored_parallelism->axes.size() != parallelism.axes.size()) {
        LOG(ERROR) << error_context << ": axis count mismatch";
        return std::nullopt;
    }

    auto request_tp_axis_index = find_tp_axis_index(parallelism.axes);
    auto stored_tp_axis_index = find_tp_axis_index(stored_parallelism->axes);
    if (!request_tp_axis_index.has_value() ||
        !stored_tp_axis_index.has_value()) {
        LOG(ERROR) << error_context << ": reconstruction requires a TP axis";
        return std::nullopt;
    }
    if (*request_tp_axis_index != *stored_tp_axis_index) {
        LOG(ERROR) << error_context << ": TP axis position mismatch";
        return std::nullopt;
    }

    for (size_t i = 0; i < parallelism.axes.size(); ++i) {
        if (i == *request_tp_axis_index) {
            const auto request_kind =
                parse_layout_axis_kind(parallelism.axes[i].kind);
            const auto stored_kind =
                parse_layout_axis_kind(stored_parallelism->axes[i].kind);
            if (!request_kind.has_value() || !stored_kind.has_value() ||
                request_kind != stored_kind ||
                parallelism.axes[i].split_dim !=
                    stored_parallelism->axes[i].split_dim ||
                parallelism.axes[i].expert_id !=
                    stored_parallelism->axes[i].expert_id ||
                parallelism.axes[i].stage_id !=
                    stored_parallelism->axes[i].stage_id) {
                LOG(ERROR) << error_context << ": TP axis metadata mismatch";
                return std::nullopt;
            }
            continue;
        }
        if (!axis_specs_equal(parallelism.axes[i],
                              stored_parallelism->axes[i])) {
            LOG(ERROR) << error_context << ": non-TP axis metadata mismatch";
            return std::nullopt;
        }
    }
    return stored_parallelism;
}

std::pair<int64_t, int64_t> calculate_shard_range(int64_t dim_size, int rank,
                                                  int shard_count) {
    if (dim_size < 0 || shard_count <= 0 || rank < 0 || rank >= shard_count) {
        return {0, 0};
    }

    const int64_t start = (dim_size * static_cast<int64_t>(rank)) / shard_count;
    const int64_t end =
        (dim_size * static_cast<int64_t>(rank + 1)) / shard_count;
    return {start, end - start};
}

std::optional<ParsedTensorMetadata> parse_tensor_metadata_from_buffer(
    BufferHandle *buffer_handle, char *usr_buffer, int64_t data_length,
    bool *take_ownership, char **exported_data, size_t *total_length) {
    if (!buffer_handle && !usr_buffer) return std::nullopt;
    if (buffer_handle && usr_buffer) return std::nullopt;

    *take_ownership = !!buffer_handle;
    if (*take_ownership) {
        *total_length = buffer_handle->size();
        if (*total_length < sizeof(TensorMetadata)) {
            LOG(ERROR) << "Invalid data format: insufficient data for metadata";
            return std::nullopt;
        }

        *exported_data = new char[*total_length];
        if (!*exported_data) return std::nullopt;
        memcpy(*exported_data, buffer_handle->ptr(), *total_length);
    } else {
        *exported_data = usr_buffer;
        if (data_length < 0) {
            LOG(ERROR) << "Get tensor into failed with error code: "
                       << data_length;
            return std::nullopt;
        }
        *total_length = static_cast<size_t>(data_length);
        if (*total_length < sizeof(TensorMetadata)) {
            LOG(ERROR) << "Invalid data format: insufficient data for metadata";
            return std::nullopt;
        }
    }

    auto parsed = ParseTensorMetadata(*exported_data, *total_length);
    if (!parsed.has_value()) {
        if (*take_ownership) {
            delete[] *exported_data;
            *exported_data = nullptr;
        }
        LOG(ERROR) << "Invalid tensor metadata";
        return std::nullopt;
    }

    return parsed;
}

std::optional<ParsedTensorMetadata> parse_tensor_metadata_from_raw_buffer(
    uintptr_t buffer_ptr, size_t size, const char *operation_name) {
    if (buffer_ptr == 0) {
        LOG(ERROR) << operation_name << ": buffer pointer cannot be null";
        return std::nullopt;
    }
    auto parsed =
        ParseTensorMetadata(reinterpret_cast<const char *>(buffer_ptr), size);
    if (!parsed.has_value()) {
        LOG(ERROR) << operation_name << ": invalid tensor object metadata";
        return std::nullopt;
    }
    return parsed;
}

std::optional<RawTensorShardWritePlan> build_raw_tensor_shard_write_plan(
    const ParsedTensorMetadata &parsed, int split_dim, int rank,
    int shard_count, const char *operation_name) {
    const auto global_shape = TensorShapeToVector(
        parsed.metadata.layout.global_shape, parsed.metadata.header.ndim);
    const auto local_shape = TensorShapeToVector(
        parsed.metadata.layout.local_shape, parsed.metadata.header.ndim);
    if (global_shape.size() != local_shape.size() || rank < 0 ||
        rank >= shard_count ||
        !validate_uniform_shard_request(global_shape, split_dim, shard_count,
                                        operation_name)) {
        return std::nullopt;
    }

    int64_t global_numel = 1;
    int64_t local_numel = 1;
    for (size_t dim = 0; dim < global_shape.size(); ++dim) {
        if (global_shape[dim] <= 0 || local_shape[dim] < 0) {
            LOG(ERROR) << operation_name << ": invalid tensor shape";
            return std::nullopt;
        }
        global_numel *= global_shape[dim];
        local_numel *= local_shape[dim];
        if (static_cast<int>(dim) != split_dim &&
            global_shape[dim] != local_shape[dim]) {
            LOG(ERROR) << operation_name
                       << ": unsupported non-split local shape";
            return std::nullopt;
        }
    }
    if (local_numel < 0 || global_numel <= 0) {
        LOG(ERROR) << operation_name << ": invalid tensor numel";
        return std::nullopt;
    }
    if ((local_numel == 0 && parsed.data_bytes != 0) ||
        (local_numel > 0 &&
         parsed.data_bytes % static_cast<size_t>(local_numel) != 0)) {
        LOG(ERROR) << operation_name << ": invalid tensor byte size";
        return std::nullopt;
    }

    const size_t element_size =
        local_numel == 0 ? 0
                         : parsed.data_bytes / static_cast<size_t>(local_numel);
    const auto [shard_start, shard_extent] =
        calculate_shard_range(global_shape[split_dim], rank, shard_count);

    std::vector<int64_t> shard_shape = global_shape;
    shard_shape[split_dim] = shard_extent;
    int64_t shard_numel = 1;
    for (auto dim : shard_shape) {
        shard_numel *= dim;
    }
    const size_t shard_bytes = static_cast<size_t>(shard_numel) * element_size;

    TensorMetadata metadata =
        BuildTensorMetadata(parsed.metadata.header.dtype, global_shape,
                            shard_shape, TensorLayoutKind::SHARD);
    metadata.header.data_bytes = shard_bytes;

    RawTensorShardWritePlan plan;
    plan.metadata = metadata;
    if (shard_extent == 0 || element_size == 0) {
        return plan;
    }

    int64_t elements_before = 1;
    for (int i = 0; i < split_dim; ++i) {
        elements_before *= global_shape[i];
    }
    int64_t elements_after = 1;
    for (size_t i = split_dim + 1; i < global_shape.size(); ++i) {
        elements_after *= global_shape[i];
    }

    const size_t row_bytes = static_cast<size_t>(shard_extent) *
                             static_cast<size_t>(elements_after) * element_size;
    plan.data_ranges.reserve(static_cast<size_t>(elements_before));
    for (int64_t slice_idx = 0; slice_idx < elements_before; ++slice_idx) {
        const size_t src_offset =
            parsed.data_offset +
            static_cast<size_t>(slice_idx * global_shape[split_dim] +
                                shard_start) *
                static_cast<size_t>(elements_after) * element_size;
        plan.data_ranges.emplace_back(src_offset, row_bytes);
    }
    return plan;
}
