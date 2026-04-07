#include "ha/snapshot/catalog_backed_snapshot_provider.h"

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <msgpack.hpp>

#include "ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.h"
#include "ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.h"
#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "segment.h"
#include "serialize/serializer.hpp"
#include "utils/zstd_util.h"

namespace mooncake {

namespace {

constexpr char kSnapshotManifestFile[] = "manifest.txt";
constexpr char kSnapshotMetadataFile[] = "metadata";
constexpr char kSnapshotSegmentsFile[] = "segments";
constexpr char kSnapshotSerializerVersion[] = "1.0.0";
constexpr char kSnapshotSerializerType[] = "messagepack";

enum class SnapshotCatalogBackendKind {
    kEmbedded,
    kRedis,
};

std::optional<SnapshotCatalogBackendKind> ParseSnapshotCatalogBackendKind(
    std::string_view store_type) {
    if (store_type.empty() || store_type == "embedded" ||
        store_type == "payload") {
        return SnapshotCatalogBackendKind::kEmbedded;
    }
    if (store_type == "redis") {
        return SnapshotCatalogBackendKind::kRedis;
    }
    return std::nullopt;
}

const msgpack::object* FindMapField(const msgpack::object& object,
                                    std::string_view field_name) {
    if (object.type != msgpack::type::MAP) {
        return nullptr;
    }

    for (uint32_t i = 0; i < object.via.map.size; ++i) {
        const auto& key = object.via.map.ptr[i].key;
        if (key.type != msgpack::type::STR) {
            continue;
        }

        std::string_view candidate(key.via.str.ptr, key.via.str.size);
        if (candidate == field_name) {
            return &object.via.map.ptr[i].val;
        }
    }

    return nullptr;
}

ErrorCode ValidateManifest(std::string_view snapshot_id,
                           std::string_view manifest) {
    const auto first = manifest.find('|');
    const auto second = first == std::string_view::npos
                            ? std::string_view::npos
                            : manifest.find('|', first + 1);
    if (first == std::string_view::npos || second == std::string_view::npos) {
        LOG(ERROR) << "Invalid snapshot manifest format, snapshot_id="
                   << snapshot_id << ", manifest=" << manifest;
        return ErrorCode::DESERIALIZE_FAIL;
    }

    const auto protocol = manifest.substr(0, first);
    const auto version = manifest.substr(first + 1, second - first - 1);
    if (protocol != kSnapshotSerializerType) {
        LOG(ERROR) << "Unsupported snapshot manifest protocol, snapshot_id="
                   << snapshot_id << ", protocol=" << protocol
                   << ", expected=" << kSnapshotSerializerType;
        return ErrorCode::DESERIALIZE_FAIL;
    }
    if (version != kSnapshotSerializerVersion) {
        LOG(ERROR) << "Unsupported snapshot manifest version, snapshot_id="
                   << snapshot_id << ", version=" << version
                   << ", expected=" << kSnapshotSerializerVersion;
        return ErrorCode::DESERIALIZE_FAIL;
    }

    return ErrorCode::OK;
}

tl::expected<std::optional<StandbyObjectMetadata>, ErrorCode>
DeserializeStandbyObjectMetadata(
    const msgpack::object& object, const SegmentView& segment_view,
    uint64_t snapshot_sequence_id,
    const std::chrono::system_clock::time_point& now) {
    if (object.type != msgpack::type::ARRAY) {
        LOG(ERROR) << "Snapshot metadata entry is not an array";
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }
    if (object.via.array.size < 7) {
        LOG(ERROR) << "Snapshot metadata entry is too short, size="
                   << object.via.array.size;
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }

    try {
        msgpack::object* array = object.via.array.ptr;
        uint32_t index = 0;

        std::string client_id_string = array[index++].as<std::string>();
        UUID client_id;
        if (!StringToUuid(client_id_string, client_id)) {
            LOG(ERROR) << "Snapshot metadata entry has invalid client UUID: "
                       << client_id_string;
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }

        (void)array[index++].as<uint64_t>();  // put_start_time
        const auto size = static_cast<size_t>(array[index++].as<uint64_t>());
        const auto lease_timestamp_ms = array[index++].as<uint64_t>();
        const bool has_soft_pin_timeout = array[index++].as<bool>();
        const auto soft_pin_timestamp_ms = array[index++].as<uint64_t>();
        const auto replica_count = array[index++].as<uint32_t>();

        if (object.via.array.size != 7 + replica_count &&
            object.via.array.size != 8 + replica_count) {
            LOG(ERROR) << "Snapshot metadata entry replica count mismatch, "
                       << "replicas=" << replica_count
                       << ", total_fields=" << object.via.array.size;
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }

        const auto lease_timeout = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(lease_timestamp_ms));
        std::optional<std::chrono::system_clock::time_point> soft_pin_timeout;
        if (has_soft_pin_timeout) {
            soft_pin_timeout.emplace(
                std::chrono::milliseconds(soft_pin_timestamp_ms));
        }

        if (size == 0 ||
            (lease_timeout <= now && (!soft_pin_timeout.has_value() ||
                                      soft_pin_timeout.value() <= now))) {
            return std::optional<StandbyObjectMetadata>();
        }

        std::vector<Replica::Descriptor> replicas;
        replicas.reserve(replica_count);
        for (uint32_t i = 0; i < replica_count; ++i) {
            auto replica_result =
                Serializer<Replica>::deserialize(array[index++], segment_view);
            if (!replica_result) {
                LOG(ERROR) << "Failed to deserialize snapshot replica: "
                           << replica_result.error().message;
                return tl::make_unexpected(replica_result.error().code);
            }

            const Replica& replica = *replica_result.value();
            if (replica.status() != ReplicaStatus::COMPLETE ||
                (replica.is_memory_replica() &&
                 replica.has_invalid_mem_handle())) {
                return std::optional<StandbyObjectMetadata>();
            }

            replicas.emplace_back(replica.get_descriptor());
        }

        if (replicas.empty()) {
            return std::optional<StandbyObjectMetadata>();
        }

        StandbyObjectMetadata metadata;
        metadata.client_id = client_id;
        metadata.size = size;
        metadata.replicas = std::move(replicas);
        metadata.last_sequence_id = snapshot_sequence_id;
        return std::optional<StandbyObjectMetadata>(std::move(metadata));
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Failed to parse snapshot metadata entry: " << ex.what();
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }
}

tl::expected<std::vector<std::pair<std::string, StandbyObjectMetadata>>,
             ErrorCode>
DeserializeStandbySnapshotMetadata(const std::vector<uint8_t>& data,
                                   const SegmentView& segment_view,
                                   uint64_t snapshot_sequence_id) {
    msgpack::object_handle root_handle;
    try {
        root_handle = msgpack::unpack(
            reinterpret_cast<const char*>(data.data()), data.size());
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Failed to unpack snapshot metadata payload: "
                   << ex.what();
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }

    const auto& root = root_handle.get();
    const auto* shards = FindMapField(root, "shards");
    if (shards == nullptr || shards->type != msgpack::type::MAP) {
        LOG(ERROR) << "Snapshot metadata payload misses shards map";
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }

    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    const auto now = std::chrono::system_clock::now();
    for (uint32_t i = 0; i < shards->via.map.size; ++i) {
        const auto& shard_blob = shards->via.map.ptr[i].val;
        if (shard_blob.type != msgpack::type::BIN) {
            LOG(ERROR) << "Snapshot shard payload is not binary";
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }

        std::vector<uint8_t> decompressed_shard;
        try {
            decompressed_shard = zstd_decompress(
                reinterpret_cast<const uint8_t*>(shard_blob.via.bin.ptr),
                shard_blob.via.bin.size);
        } catch (const std::exception& ex) {
            LOG(ERROR) << "Failed to decompress snapshot shard payload: "
                       << ex.what();
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }

        msgpack::object_handle shard_handle;
        try {
            shard_handle = msgpack::unpack(
                reinterpret_cast<const char*>(decompressed_shard.data()),
                decompressed_shard.size());
        } catch (const std::exception& ex) {
            LOG(ERROR) << "Failed to unpack snapshot shard payload: "
                       << ex.what();
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }

        const auto& shard = shard_handle.get();
        const auto* metadata_entries = FindMapField(shard, "metadata");
        if (metadata_entries == nullptr ||
            metadata_entries->type != msgpack::type::ARRAY) {
            LOG(ERROR) << "Snapshot shard misses metadata array";
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }

        for (uint32_t j = 0; j < metadata_entries->via.array.size; ++j) {
            const auto& item = metadata_entries->via.array.ptr[j];
            if (item.type != msgpack::type::ARRAY || item.via.array.size != 2) {
                LOG(ERROR) << "Snapshot metadata item has invalid shape";
                return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
            }

            try {
                const std::string key = item.via.array.ptr[0].as<std::string>();
                auto metadata_result = DeserializeStandbyObjectMetadata(
                    item.via.array.ptr[1], segment_view, snapshot_sequence_id,
                    now);
                if (!metadata_result) {
                    return tl::make_unexpected(metadata_result.error());
                }
                if (!metadata_result->has_value()) {
                    continue;
                }
                snapshot.emplace_back(key, std::move(metadata_result->value()));
            } catch (const std::exception& ex) {
                LOG(ERROR) << "Failed to parse snapshot metadata item: "
                           << ex.what();
                return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
            }
        }
    }

    return snapshot;
}

class CatalogBackedSnapshotProvider final : public SnapshotProvider {
   public:
    explicit CatalogBackedSnapshotProvider(
        const MasterServiceSupervisorConfig& config)
        : cluster_id_(config.cluster_id) {
        auto catalog_kind =
            ParseSnapshotCatalogBackendKind(config.snapshot_catalog_store_type);
        if (!catalog_kind.has_value()) {
            throw std::invalid_argument(
                "unknown snapshot catalog store type: " +
                config.snapshot_catalog_store_type);
        }

        auto object_store_type =
            ParseSnapshotObjectStoreType(config.snapshot_object_store_type);
        object_store_ = SnapshotObjectStore::Create(object_store_type);

        switch (catalog_kind.value()) {
            case SnapshotCatalogBackendKind::kEmbedded:
                catalog_store_ = std::make_unique<
                    ha::backends::embedded::EmbeddedSnapshotCatalogStore>(
                    object_store_.get());
                break;
            case SnapshotCatalogBackendKind::kRedis: {
#ifndef STORE_USE_REDIS
                throw std::invalid_argument(
                    "redis snapshot catalog store is unavailable in the "
                    "current build");
#else
                const auto connstring =
                    !config.snapshot_catalog_store_connstring.empty()
                        ? config.snapshot_catalog_store_connstring
                        : config.ha_backend_connstring;
                if (connstring.empty()) {
                    throw std::invalid_argument(
                        "redis snapshot catalog store requires a "
                        "connection string");
                }
                catalog_store_ = std::make_unique<
                    ha::backends::redis::RedisSnapshotCatalogStore>(
                    object_store_.get(), connstring, cluster_id_);
                break;
#endif
            }
        }
    }

    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> LoadLatestSnapshot(
        const std::string& cluster_id) override {
        if (!cluster_id.empty() && cluster_id != cluster_id_) {
            LOG(ERROR) << "Snapshot provider cluster mismatch, requested="
                       << cluster_id << ", configured=" << cluster_id_;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        auto latest_result = catalog_store_->GetLatest();
        if (!latest_result) {
            LOG(ERROR) << "Failed to resolve latest snapshot from catalog, "
                       << "cluster_id=" << cluster_id_
                       << ", error=" << toString(latest_result.error());
            return tl::make_unexpected(latest_result.error());
        }
        if (!latest_result->has_value()) {
            return std::optional<LoadedSnapshot>();
        }

        const auto& descriptor = latest_result->value();
        std::string object_prefix = descriptor.object_prefix;
        if (object_prefix.empty()) {
            object_prefix =
                ha::snapshot_catalog_store_detail::BuildSnapshotPrefix(
                    descriptor.snapshot_id);
        }

        std::string manifest_path = descriptor.manifest_key;
        if (manifest_path.empty()) {
            manifest_path = object_prefix + kSnapshotManifestFile;
        }

        std::string manifest_content;
        auto manifest_result =
            object_store_->DownloadString(manifest_path, manifest_content);
        if (!manifest_result) {
            LOG(ERROR) << "Failed to download snapshot manifest, snapshot_id="
                       << descriptor.snapshot_id << ", path=" << manifest_path
                       << ", error=" << manifest_result.error();
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }

        auto manifest_err =
            ValidateManifest(descriptor.snapshot_id, manifest_content);
        if (manifest_err != ErrorCode::OK) {
            return tl::make_unexpected(manifest_err);
        }

        std::vector<uint8_t> segments_content;
        const auto segments_path = object_prefix + kSnapshotSegmentsFile;
        auto segments_result =
            object_store_->DownloadBuffer(segments_path, segments_content);
        if (!segments_result) {
            LOG(ERROR) << "Failed to download snapshot segments payload, "
                       << "snapshot_id=" << descriptor.snapshot_id
                       << ", path=" << segments_path
                       << ", error=" << segments_result.error();
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }

        std::vector<uint8_t> metadata_content;
        const auto metadata_path = object_prefix + kSnapshotMetadataFile;
        auto metadata_result =
            object_store_->DownloadBuffer(metadata_path, metadata_content);
        if (!metadata_result) {
            LOG(ERROR) << "Failed to download snapshot metadata payload, "
                       << "snapshot_id=" << descriptor.snapshot_id
                       << ", path=" << metadata_path
                       << ", error=" << metadata_result.error();
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }

        SegmentManager segment_manager(BufferAllocatorType::OFFSET);
        SegmentSerializer segment_serializer(&segment_manager);
        auto deserialize_segments =
            segment_serializer.Deserialize(segments_content);
        if (!deserialize_segments) {
            LOG(ERROR) << "Failed to deserialize snapshot segments payload, "
                       << "snapshot_id=" << descriptor.snapshot_id
                       << ", error=" << deserialize_segments.error().message;
            return tl::make_unexpected(deserialize_segments.error().code);
        }

        auto deserialize_metadata = DeserializeStandbySnapshotMetadata(
            metadata_content, segment_manager.getView(),
            descriptor.last_included_seq);
        if (!deserialize_metadata) {
            LOG(ERROR) << "Failed to deserialize snapshot metadata payload, "
                       << "snapshot_id=" << descriptor.snapshot_id
                       << ", error=" << toString(deserialize_metadata.error());
            return tl::make_unexpected(deserialize_metadata.error());
        }

        LoadedSnapshot snapshot;
        snapshot.snapshot_id = descriptor.snapshot_id;
        snapshot.snapshot_sequence_id = descriptor.last_included_seq;
        snapshot.metadata = std::move(deserialize_metadata.value());
        return std::optional<LoadedSnapshot>(std::move(snapshot));
    }

   private:
    std::string cluster_id_;
    std::unique_ptr<SnapshotObjectStore> object_store_;
    std::unique_ptr<ha::SnapshotCatalogStore> catalog_store_;
};

}  // namespace

tl::expected<std::unique_ptr<SnapshotProvider>, ErrorCode>
CreateCatalogBackedSnapshotProvider(
    const MasterServiceSupervisorConfig& config) {
    try {
        return std::unique_ptr<SnapshotProvider>(
            std::make_unique<CatalogBackedSnapshotProvider>(config));
    } catch (const std::invalid_argument& ex) {
        LOG(ERROR) << "Invalid snapshot provider configuration: " << ex.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Failed to create catalog-backed snapshot provider: "
                   << ex.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

}  // namespace mooncake
