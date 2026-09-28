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

enum class KvcsAccessMode {
    kStandard,
    kLowLevel,
};

inline constexpr char kKvcsStandardAdapterName[] = "kvcs-standard";
inline constexpr char kKvcsLowLevelAdapterName[] = "kvcs-lowlevel";

struct KvcsLowLevelTargetSpec {
    std::string id;
    uint32_t mountpoint_index = 0;
    KvcsEfcRouteKind route_kind = KvcsEfcRouteKind::kKvCacheStore;
    std::unique_ptr<KvcsDriver> driver;
};

// Bridges the distributed object-storage contract to KVCS Standard or
// Low-Level mode. The adapter owns tenant encoding and target routing; the
// selected driver owns provider-specific I/O and metadata.
class KvcsObjectStorageAdapter final : public ObjectStorageAdapter {
   public:
    KvcsObjectStorageAdapter(const FileStorageConfig& config,
                             KvcsAccessMode mode,
                             std::string efc_config_path = {});
    KvcsObjectStorageAdapter(const FileStorageConfig& config,
                             KvcsAccessMode mode,
                             std::unique_ptr<KvcsDriver> driver);
    KvcsObjectStorageAdapter(const FileStorageConfig& config,
                             std::vector<KvcsLowLevelTargetSpec> targets);
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
    bool SupportsProviderQueryInParallel() const override {
        return mode_ == KvcsAccessMode::kLowLevel;
    }
    ObjectStorageQueryResults BatchQueryProvider(
        std::span<const std::string> logical_keys) override;
    ObjectStorageQueryResults BatchQueryProviderUntil(
        std::span<const std::string> logical_keys,
        std::chrono::steady_clock::time_point deadline) override;
    ObjectStorageIoResults BatchGetIntoWithQueryContexts(
        std::span<const ObjectStorageGetRequest> requests,
        std::span<const tl::expected<ObjectStorageQueryContext, ErrorCode>>
            contexts) override;
    void SerializeMetrics(std::string& output) const override;

    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override;

    tl::expected<void, ErrorCode> Init() override;
    tl::expected<void, ErrorCode> CheckHealth() override;
    const char* GetName() const override {
        return mode_ == KvcsAccessMode::kStandard ? kKvcsStandardAdapterName
                                                  : kKvcsLowLevelAdapterName;
    }

   private:
    tl::expected<ObjectKey, ErrorCode> EncodeKey(
        std::string_view logical_key) const;
    KvcsManifestResults BatchQueryKvcs(
        std::span<const ObjectKey> logical_keys,
        std::optional<std::chrono::steady_clock::time_point> deadline =
            std::nullopt);
    KvcsGetResults BatchGetKvcsWithManifests(
        std::span<const KvcsGetRequest> requests,
        std::span<const tl::expected<KvcsManifest, ErrorCode>> manifests);

    struct Impl;

    FileStorageConfig config_;
    KvcsAccessMode mode_;
    std::string efc_config_path_;
    std::vector<KvcsLowLevelTargetSpec> pending_targets_;
    std::unique_ptr<Impl> impl_;
    bool initialized_ = false;
};

}  // namespace mooncake
