#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage_backend.h"
#include "nvme_kv_connector.h"
#include "nvme_kv_key_codec.h"
#include "nvme_kv_object_layout.h"

namespace mooncake {

class NvmeKvStorageBackend : public StorageBackendInterface {
   public:
    explicit NvmeKvStorageBackend(
        const FileStorageConfig& file_storage_config_);

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        std::function<void(const std::vector<std::string>& evicted_keys)>
            eviction_handler = nullptr) override;

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

   private:
    enum class ObjectState {
        WRITING,
        COMMITTED,
        CORRUPTED,
    };

    using PhysicalKey = NvmeKvPhysicalKey;
    using ObjectHeader = NvmeKvObjectHeader;

    enum class DeviceState {
        ENABLED,
        DISABLED_BY_FAILURE,
    };

    struct CatalogEntry {
        std::string device_id;
        PhysicalKey physical_key;
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
    };

    std::filesystem::path CatalogPath() const;
    tl::expected<void, ErrorCode> InitDevices();
    tl::expected<void, ErrorCode> LoadCatalog();
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
    void RecordDeviceFailure(const std::string& device_id);
    void RecordDeviceSuccess(const std::string& device_id, int64_t payload_size,
                             bool new_key_committed);
    void ReleaseReservation(int64_t payload_size, int64_t key_count);

    std::atomic<bool> initialized_{false};
    mutable SharedMutex mutex_;
    std::unordered_map<std::string, CatalogEntry> catalog_ GUARDED_BY(mutex_);
    std::unordered_map<std::string, NvmeKvDeviceRuntime> devices_
        GUARDED_BY(mutex_);
    int64_t reserved_size_ GUARDED_BY(mutex_) = 0;
    int64_t reserved_keys_ GUARDED_BY(mutex_) = 0;
    std::atomic<int64_t> total_size_{0};
    std::atomic<int64_t> total_keys_{0};
    std::function<bool(const std::string& key)> test_failure_predicate_;
};

}  // namespace mooncake
