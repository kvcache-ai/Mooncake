#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage_backend.h"
#include "storage_device_metadata.h"
#include "nvme_kv_connector.h"
#include "nvme_kv_key_codec.h"
#include "nvme_kv_object_layout.h"
#include "rpc_types.h"

namespace mooncake {

class NvmeKvStorageBackend : public StorageBackendInterface {
   public:
    friend class FileStorageTest;
    struct NvmeKvDeviceSnapshot {
        std::string device_id;
        std::string device_serial;
        std::string state;
        uint32_t consecutive_failures = 0;
        uint32_t failure_threshold = 0;
        int64_t capacity_total = 0;
        int64_t total_size = 0;
        int64_t total_keys = 0;
        uint32_t max_key_size = 0;
        uint32_t max_value_size = 0;
        uint32_t runtime_transfer_limit = 0;
        uint32_t effective_max_value_size = 0;
        uint32_t queue_depth = 0;
        std::string backend_type;
        std::string uuid;
        std::string device_path;
        bool readable = false;
        bool writable = false;
        std::string health_state;
        std::string last_error;
        std::string storage_path;
    };

    struct NvmeKvStatusSnapshot {
        bool initialized = false;
        int64_t total_size = 0;
        int64_t total_keys = 0;
        uint32_t effective_max_value_size = 0;
        std::string placement_policy;
        std::vector<NvmeKvDeviceSnapshot> devices;
    };

    struct NvmeKvObjectSnapshot {
        std::string key;
        std::string device_id;
        std::string physical_key_hex;
        uint32_t physical_key_slot = 0;
        uint32_t payload_size = 0;
        std::string state;
        std::string tenant_id;
        std::string domain_id;
        std::string namespace_id;
    };

    explicit NvmeKvStorageBackend(
        const FileStorageConfig& file_storage_config_);

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;

    void SetTestFailurePredicate(
        std::function<bool(const std::string& key)> predicate) override {
        test_failure_predicate_ = std::move(predicate);
    }

    void SetTestCorruptionPredicate(
        std::function<bool(const std::string& key)> predicate) {
        test_corruption_predicate_ = std::move(predicate);
    }

    void SetTestMaxValueSizeOverride(uint32_t max_value_size) {
        test_max_value_size_override_ = max_value_size;
    }

    NvmeKvStatusSnapshot GetStatusSnapshot() const;
    std::vector<NvmeKvDeviceSnapshot> GetDeviceSnapshots() const;
    std::vector<StorageDeviceMetadata> ListStorageDeviceMetadata()
        const override;
    tl::expected<StorageDeviceMetadata, ErrorCode>
    ApplyStorageDeviceMetadataUpdate(
        const StorageDeviceMetadataUpdate& update) override;
    tl::expected<StorageDeviceMetadata, ErrorCode> StartStorageDeviceRecovery(
        const UUID& device_id) override;
    tl::expected<StorageDeviceMetadata, ErrorCode>
    CompleteStorageDeviceRecovery(const UUID& device_id) override;
    tl::expected<StorageDeviceMetadata, ErrorCode> FailStorageDeviceRecovery(
        const UUID& device_id) override;
    tl::expected<StorageDeviceMetadata, ErrorCode>
    RecordStorageDeviceProbeSuccess(const UUID& device_id) override;
    tl::expected<StorageDeviceMetadata, ErrorCode>
    RecordStorageDeviceProbeFailure(const UUID& device_id,
                                    const std::string& reason) override;
    tl::expected<StorageDeviceMetadata, ErrorCode> ProbeStorageDevice(
        const UUID& device_id) override;
    std::optional<NvmeKvDeviceSnapshot> GetDeviceSnapshot(
        const std::string& device_id) const;
    std::optional<NvmeKvObjectSnapshot> GetObjectSnapshot(
        const std::string& key);
    bool SetDeviceEnabled(const std::string& device_id, bool enabled);
    tl::expected<void, ErrorCode> RefreshDevices();
    std::vector<bool> DeleteKeysFromDevice(
        const std::vector<std::string>& keys);
    std::vector<std::string> ListKeysForDevice(
        const std::string& device_id) const override;
    std::pair<int64_t, int64_t> GetDeviceCapacityUsage(
        const std::string& device_id) const override;

   private:
    enum class ObjectState {
        WRITING,
        COMMITTED,
        DELETING,
        DELETED,
        CORRUPTED,
    };

    using PhysicalKey = NvmeKvPhysicalKey;
    using ObjectHeader = NvmeKvObjectHeader;

    enum class DeviceState {
        ENABLED,
        DISABLED_BY_CONFIG,
        DISABLED_BY_FAILURE,
        DISABLED_BY_CAPACITY,
    };

    struct CatalogEntry {
        std::string device_id;
        PhysicalKey physical_key;
        uint32_t physical_key_slot = 0;
        uint32_t payload_size;
        ObjectState state;
    };

    struct NvmeKvDeviceRuntime {
        std::string device_id;
        std::shared_ptr<NvmeKvConnector> connector;
        DeviceState state;
        uint32_t consecutive_failures = 0;
        int64_t total_size = 0;
        int64_t total_keys = 0;
        std::string provider_last_error;
    };

    struct ManifestChunkRecord {
        PhysicalKey physical_key;
        uint32_t payload_size;
        uint32_t payload_checksum;
    };

    std::filesystem::path CatalogPath() const;
    tl::expected<void, ErrorCode> InitDevices();
    tl::expected<void, ErrorCode> ReconcileDevices();
    tl::expected<void, ErrorCode> LoadCatalog();
    tl::expected<void, ErrorCode> RefreshCatalogFromDisk();
    tl::expected<void, ErrorCode> RebuildCatalogFromDevices();
    tl::expected<void, ErrorCode> PersistCatalog() const;
    uint32_t EffectiveMaxValueSize() const;
    uint32_t InlinePayloadLimit() const;
    uint32_t ChunkPayloadLimit() const;
    bool ShouldStoreInline(size_t payload_size) const;
    void MarkCorrupted(const std::string& key);
    std::vector<std::string> EnabledDeviceIds() const;
    tl::expected<std::string, ErrorCode> SelectDeviceIdForKey(
        const std::string& key, size_t retry_count = 0) const;
    std::shared_ptr<NvmeKvConnector> GetConnectorForDeviceId(
        const std::string& device_id) const;
    std::optional<std::string> ResolveDeviceNamespaceId(
        const UUID& device_id) const;
    void RecordDeviceFailure(const std::string& device_id,
                             const std::string& reason = {});
    void RecordDeviceFull(const std::string& device_id);
    void RecordDeviceSuccess(const std::string& device_id, int64_t payload_size,
                             bool new_key_committed);

    std::atomic<bool> initialized_{false};
    mutable SharedMutex mutex_;
    std::unordered_map<std::string, CatalogEntry> catalog_ GUARDED_BY(mutex_);
    std::unordered_map<std::string, NvmeKvDeviceRuntime> devices_
        GUARDED_BY(mutex_);
    std::atomic<int64_t> total_size_{0};
    std::atomic<int64_t> total_keys_{0};
    std::function<bool(const std::string& key)> test_failure_predicate_;
    std::function<bool(const std::string& key)> test_corruption_predicate_;
    uint32_t test_max_value_size_override_ = 0;
};

}  // namespace mooncake
