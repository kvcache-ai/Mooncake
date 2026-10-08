#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "storage/distributed/object_storage_adapter.h"
#include "storage/distributed/kvcs/kvcs_driver.h"
#include "storage/distributed/kvcs/kvcs_efc_topology.h"
#include "storage_backend.h"

namespace mooncake {

inline constexpr char kKvcsLowLevelAdapterName[] = "kvcs-lowlevel";

// Bridges the distributed object-storage contract to KVCS Low Level.
// The adapter owns tenant encoding and one explicit target; the driver owns
// I/O.
class KvcsObjectStorageAdapter final : public ObjectStorageAdapter {
   public:
    KvcsObjectStorageAdapter(const FileStorageConfig& config,
                             std::string efc_config_path = {});
    KvcsObjectStorageAdapter(const FileStorageConfig& config,
                             std::unique_ptr<KvcsDriver> driver);
    ~KvcsObjectStorageAdapter() override;

    tl::expected<void, ErrorCode> Put(const std::string& logical_key,
                                      std::span<const char> data) override;
    tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                       const iovec* iov, int iovcnt) override;
    std::vector<tl::expected<void, ErrorCode>> PutBatch(
        const std::vector<ObjectPutRequest>& requests) override;
    tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                        void* buf, size_t len) override;
    tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) override;
    tl::expected<void, ErrorCode> Delete(
        const std::string& logical_key) override;

    ObjectStorageIoResults BatchPutV(
        std::span<const ObjectStoragePutRequest> requests) override;
    ObjectStorageIoResults BatchGetInto(
        std::span<const ObjectStorageGetRequest> requests) override;
    ObjectStorageIoResults BatchDelete(
        std::span<const std::string> logical_keys) override;

    bool SupportsProviderQuery() const override { return true; }
    ObjectStorageQueryResults BatchQueryProvider(
        std::span<const std::string> logical_keys) override;
    ObjectStorageQueryResults BatchQueryProviderUntil(
        std::span<const std::string> logical_keys,
        std::chrono::steady_clock::time_point deadline) override;

    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override;

    tl::expected<void, ErrorCode> Init() override;
    tl::expected<void, ErrorCode> CheckHealth() override;
    const char* GetName() const override { return kKvcsLowLevelAdapterName; }

   private:
    tl::expected<ObjectKey, ErrorCode> EncodeKey(
        std::string_view logical_key) const;
    ObjectStorageQueryResults BatchQueryKvcs(
        std::span<const ObjectKey> logical_keys,
        std::optional<std::chrono::steady_clock::time_point> deadline =
            std::nullopt);

    FileStorageConfig config_;
    std::string efc_config_path_;
    std::string target_id_;
    uint32_t mountpoint_index_ = 0;
    std::unique_ptr<KvcsDriver> driver_;
    bool initialized_ = false;
};

}  // namespace mooncake
